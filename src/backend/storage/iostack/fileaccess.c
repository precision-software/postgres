/**
 * fileaccess.c
 * Provides a uniform set of file access routines.
 *  - Matches existing Virtual File Descriptor routines  (FileRead, FileWrite, FileSync).
 *  - Provides additional sequential routines similer to fread/fwrite/fseek.
 *  - Uses I/O Stacks to provide raw, buffered, encrypted and compressed files.
 *  - Layered on top of existing Postgres file access routines.
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/resource.h>		/* for getrlimit */
#include <sys/types.h>
#include <limits.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/pg_prng.h"
#include "pgstat.h"
#include "portability/mem.h"
#include "postmaster/startup.h"
#include "storage/fd.h"
#include "utils/resowner_private.h"

#include "storage/iostack_internal.h"
#include "storage/vfd.h"

/* Forward References. */
static IoStack *selectIoStack(const char *path, int oflags, int mode);
IoStack *vfdNew();
void ioStackSetup(void);
bool saveFileError(File file, IoStack *ioStack);
static inline size_t checkIoStackError(File file, ssize_t retval);

/* Wrapper for backwards compatibility */
File PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return FileOpenPerm(fileName, fileFlags, fileMode);
}

/* Preferred procedure name for opening a file */
File FileOpen(const char *fileName, int fileFlags)
{
	return FileOpenPerm(fileName, fileFlags, pg_file_create_mode);
}

File FileOpenPerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	debug("FileOpenPerm: fileName=%s fileFlags=0x%x fileMode=0x%x\n", fileName, fileFlags, fileMode);
	/* Allocate an I/O stack for this file. */
	IoStack *proto = selectIoStack(fileName, fileFlags, fileMode);

	if (proto == NULL)
		return -1;

	/* I/O stacks don't implement O_APPEND, so position to FileSize instead */
	bool append = (fileFlags & O_APPEND) != 0;
	fileFlags &= ~O_APPEND;

	/* Open the  prototype I/O stack */
	IoStack *ioStack = stackOpen(proto, fileName, fileFlags, fileMode);
	File file = (File)ioStack->openVal;
	if (file < 0)
	{
		saveFileError(-1, ioStack);
		free(ioStack);
		return checkIoStackError(-1, -1);
	}

	/* Save the I/O stack in the vfd structure */
	Vfd *vfdP = getVfd(file);
	vfdP->ioStack = ioStack;

	/* Position at end of file if appending. This only impacts WriteSeq and ReadSeq. */
	vfdP->offset = append? FileSize(file): 0;
	if (vfdP->offset == -1) {
		saveFileError(-1, ioStack);
		FileClose(file);
		return checkIoStackError(-1, -1);
	}

	return file;
}

/*
 * Close a file.
 */
int FileClose(File file)
{
	debug("FileClose: name=%s, file=%d  iostack=%p\n", getName(file), file, getStack(file));
	/* Make sure we are dealing with an open file */
	if (file < 0 || getStack(file) == NULL)
	{
		errno = EBADF;
		return -1;
	}

	/* Close the file.  The low level routine will invalidate the "file" index */
	IoStack *stack = getStack(file);
	ssize_t retval = stackClose(stack);

	/* If failed, save the error info. In this case, it goes to the dummy "-1" io stack */
	if (retval < 0)
		saveFileError(-1, stack);
	else
		FileClearError(-1);

	/* No matter what, release the memory on Close */
	free(stack);  /* TODO: do we need to restore errno? */
	debug("FileClose(done): file=%d retval=%d\n", file, retval);

	return (int)checkIoStackError(-1, retval);
}


ssize_t FileRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileRead: name=%s file=%d  amount=%zd offset=%lld\n", getName(file), file, amount, offset);
	Assert(offset >= 0);
	Assert((ssize_t)amount > 0);

	IoStack *stack = getStack(file);

	/* Read the data as requested */
	pgstat_report_wait_start(wait_event_info);
	ssize_t actual = stackReadAll(stack, buffer, amount, offset);
	pgstat_report_wait_end();

	/* If successful, update the file offset */
	if (actual >= 0)
		getVfd(file)->offset = offset + actual;

	debug("FileRead(done): file=%d  name=%s  actual=%zd\n", file, getName(file), actual);
	return actual;
}


ssize_t FileWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileWrite: name=%s file=%d  amount=%zd offset=%lld\n", getName(file), file, amount, offset);
	Assert(offset >= 0 && (ssize_t)amount > 0);

	/* Write the data as requested */
	pgstat_report_wait_start(wait_event_info);
	ssize_t actual = stackWriteAll(getStack(file), buffer, amount, offset);
	pgstat_report_wait_end();

	/* If successful, update the file offset */
	if (actual >= 0)
		getVfd(file)->offset = offset + actual;

	debug("FileWrite(done): file=%d  name=%s  actual=%zd\n", file, getName(file), actual);
	return actual;
}

int FileSync(File file, uint32 wait_event_info)
{
	pgstat_report_wait_start(wait_event_info);
	ssize_t retval = stackSync(getStack(file));
	pgstat_report_wait_end();
	return (int)retval;
}


off_t FileSize(File file)
{
	off_t size = stackSize(getStack(file));
	debug("FileSize: name=%s file=%d  size=%lld\n", getName(file), file, size);
	return size;
}

int	FileTruncate(File file, off_t offset, uint32 wait_event_info)
{
	pgstat_report_wait_start(wait_event_info);
	ssize_t retval = stackTruncate(getStack(file), offset);
	pgstat_report_wait_end();
	return (int)retval;
}


/*========================================================================================
 * Routines to emuulate C library FILE routines (fgetc, fprintf, ...)
 */

/* Similar to fgetc */
int
FileGetc(File file)
{
	char c;
	int ret = FileReadSeq(file, &c, 1, WAIT_EVENT_NONE);
	if (ret <= 0)
		ret = EOF;
	return ret;
}

/* Similar to fputc */
int FilePutc(int c, File file)
{
	char cbuf;
	cbuf = c;
	if (FileWriteSeq(file, &cbuf, 1, WAIT_EVENT_NONE) <= 0)
		c = EOF;
	return c;
}

/*
 * A temporary equivalent of fprintf.
 * This version limits text to what fits in a local buffer.
 * Ultimately, we need to update the internal snprintf.c (dopr) to spill
 * to temporary files as well as to FILE*.
 */
int FilePrintf(File file, const char *format, ...)
{
	va_list args;
	char buffer[4*1024]; /* arbitrary size, big enough? */
	int size;

	va_start(args, format);
	size = vsnprintf(buffer, sizeof(buffer), format, args);
	va_end(args);

	if (size < 0 || size >= sizeof(buffer))
		ereport(ERROR,
				errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				errmsg("FilePrintf buffer overflow - %d characters exceeded %lu buffer", size, sizeof(buffer)));

	return FileWriteSeq(file, buffer, Min(size, sizeof(buffer)-1), WAIT_EVENT_NONE);
}

int FileScanf(File file, const char *format, ...)
{
	Assert(false); /* Not implemented */
	return -1;
}

int FilePuts(File file, const char *string)
{
	return FileWriteSeq(file, string, strlen(string), WAIT_EVENT_NONE);
}


/* If there was an I/O stack error, throw the error exception.  Regular file I/O errors will return normally */
static inline size_t checkIoStackError(File file, ssize_t retval)
{
	if (retval < 0 && FileErrorCode(file) == EIOSTACK)
		ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR), errmsg("I/O Error for %s: %s", FilePathName(file), FileErrorMsg(file)));

	return retval;
}

/*
 * Read sequentially from the file.
 */
size_t
FileReadSeq(File file, void *buffer, size_t amount,
			uint32 wait_event_info)
{
	ssize_t retval = FileRead(file, buffer, amount, getVfd(file)->offset, wait_event_info);
	return checkIoStackError(file, retval);
}

/*
 * Write sequentially to the file.
 */
size_t
FileWriteSeq(File file, const void *buffer, size_t amount,
			 uint32 wait_event_info)
{
	ssize_t retval = FileWrite(file, buffer, amount, getVfd(file)->offset, wait_event_info);
	return checkIoStackError(file, retval);
}

/*
 * Seek to an absolute position within the file.
 * Relative positions can be calculated using FileTell or FileSize.
 */
off_t
FileSeek(File file, off_t offset)
{
	getVfd(file)->offset = offset;
	return offset;
}

/*
 * Tell us the current file position
 */
off_t
FileTell(File file)
{
	return getVfd(file)->offset;
}

/*
 * Error handling code.
 * These functions are similar to ferror(), but can be accessed even when the file index is -1.
 * This added feature allows error info to be fetched after a failed "open" call.
 * TODO:; consider inlining some of these.
 */

/*
 * Helper function to get the I/O stack associated with a virtual file index.
 * Maintains a dummy I/O Stack for the specal case when the file index is -1.
 */
static inline IoStack *errStack(File file)
{
	/* Static, dummy I/O stack when file == -1 */
	static IoStack staticIoStack = {0};

	/* Return the dummy if index is -1, else return the real stack */
	if (file == -1)
		return &staticIoStack;
	else
		return getStack(file);
}


/* True if an error occurred on the file.  (EOF is not an error) */
bool FileError(File file)
{
	return stackError(errStack(file));
}

/* True if the last read generated an EOF */
int FileEof(File file)
{
	return stackEof(errStack(file));
}

/* Clears an error, and is true if an error had been encountered */
bool FileClearError(File file)
{
	return stackClearError(errStack(file));
}

/* Get a pointer to the error message */
const char *FileErrorMsg(File file)
{
	return stackErrorMsg(errStack(file));
}

int FileErrorCode(File file)
{
	return stackErrorCode(errStack(file));
}
/*
 * Helper function to save any errors from the I/O stack to the virtual file index.
 */
bool saveFileError(File file, IoStack *ioStack)
{
	/* Copy the error, or clear the error, depending on ioStack */
	copyError(errStack(file), -1, ioStack);
	return stackError(ioStack);
}


/* TODO: the following may belong in a different file ... */
/* These need to be set properly */
static Byte *tempKey = (Byte *)"0123456789ABCDEF0123456789ABCDEF";
static size_t tempKeyLen = 32;
static Byte *permKey = (Byte *)"abcdefghijklmnopqrstuvwxyzABCDEF";
static size_t permKeyLen = 32;

/* Function to create a test stack for unit testing */
IoStack *(*testStackNew)() = NULL;

/*
 * Construct the appropriate I/O Stack.
 * This function provides flexibility in now I/O stacks are created.
 * It can look at open flags, do GLOB matching on pathnames,
 * or create different stacks if reading vs writing.
 * The current version uses special "PG_*" open flags.
 */

/* Prototype stacks */
static bool ioStacksInitialized = false;
IoStack *ioStackPlain;				/* Buffered, unmodified text */
IoStack *ioStackEncrypt;			/* Buffered and encrypted with a session key */
IoStack *ioStackEncryptPerm;        /* Buffered and encrypted with a session key */
IoStack *ioStackTest;				/* Stack used for unit testing */
IoStack *ioStackRaw;				/* Unbuffered "raw" file access */
IoStack *ioStackCompress;			/* Buffered and compressed */
IoStack *ioStackCompressEncrypt;	/* Compressed and encrypted with session key */


/*
 * Select the appropriate I/O Stack for this particular file.
 * This function provides flexibility in how I/O stacks are created.
 * It can look at open flags, do GLOB matching on pathnames,
 * or create different stacks if reading vs writing.
 * The current version looks for special "PG_*" open flags.
 */
static IoStack *selectIoStack(const char *path, int oflags, int mode)
{
	debug("IoStackNew: name=%s  oflags=0x%x  mode=o%o\n", path, oflags, mode);

	/* Set up the I/O prototype stacks if not already done */
	if (!ioStacksInitialized)
		ioStackSetup();

	/* TODO: create prototypes at beginning. Here, we just select them */
	/* Look at oflags to determine which stack to use */
	switch (oflags & PG_STACK_MASK)
	{
		case PG_PLAIN:            return ioStackPlain;
		case PG_ENCRYPT:          return ioStackEncrypt;
		case PG_ENCRYPT_PERM:     return ioStackEncryptPerm;
		case PG_TESTSTACK:        return ioStackTest;
		case 0:                   debug("Raw mode: path=%s oflags=0x%x\n", path, oflags); return ioStackRaw;

		default: elog(FATAL, "Unrecognized I/O Stack oflag 0x%x", (oflags & PG_STACK_MASK));
	}
}


/*
 * Initialize the I/O stack infrastructure. Creates prototype stacks.
 */
void ioStackSetup(void)
{
	/* Set up the prototype stacks */
	ioStackRaw = vfdStackNew();
	ioStackEncrypt = bufferedNew(1, aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen, vfdStackNew()));
	ioStackEncryptPerm = bufferedNew(1, aeadNew("AES-256-GCM", 16 * 1024, permKey, permKeyLen, vfdStackNew()));
	ioStackPlain = bufferedNew(64*1024, vfdStackNew());
	ioStackCompress = bufferedNew(1, lz4CompressNew(64 * 1024, vfdStackNew(), vfdStackNew()));
	ioStackCompressEncrypt = bufferedNew(1,
										 lz4CompressNew(16*1024,
														bufferedNew(1,
																	aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen,
																			vfdStackNew())),
														bufferedNew(1,
																	aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen,
																			vfdStackNew()))));
	/* Note we are now initialized */
	ioStacksInitialized = true;
}


bool endsWith(const char * str, const char * suffix)
{
	size_t strLen = strlen(str);
	size_t suffixLen = strlen(suffix);

	return
		strLen >= suffixLen &&
		strcmp(str + strLen - suffixLen, suffix) == 0;
}

bool startsWith(const char *str, const char *prefix)
{
	return strncmp(prefix, str, strlen(prefix)) == 0;
}

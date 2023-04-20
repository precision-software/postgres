/**
 * fileaccess.c
 * Provides a uniform set of file access routines.
 *  - Matches existing Virtual File Descriptor routines  (FileRead, FileWrite, FileSync).
 *  - Invoke I/O Stacks to provide raw, buffered, encrypted and compressed files.
 *  - Eventually invoke the real FileRead, FilwWrite, FileSync routines.
 *
 *  If an I/O stack error occurs, the calling code probably won't know what to do with it.
 *  We throw an error exception and let the caller deal with it.
 *  Normal file errors are returned as -1. We set errno for compatibility,
 *  but the caller can use FileErrorCode(-1)
 *  to get the error code.
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
void ioStackSetup(void);
void saveFileError(File file, IoStack *ioStack);
void fileSetError(File file, int errnum, const char *fmt, ...);
void throwIoStackError(File file);
void fileSetError(File file, int errnum, const char *fmt, ...);

/* Wrapper for backwards compatibility */
File PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return FileOpenPerm(fileName, fileFlags, fileMode);
}

/* Preferred procedure for opening a file */
File FileOpen(const char *fileName, int fileFlags)
{
	return FileOpenPerm(fileName, fileFlags, pg_file_create_mode);
}

/*
 * Open a file using an I/O stack.
 * If an error occurs, returns -1 and sets up error information
 * so FileError(-1) will return true. Note errno is set for compatibility.
 *
 * Note we must be sure to release *all* resources if we fail to open the file.
 * It should be the same as though never opened.
 *
 * Note stackOpen has a triple return value
 *   - an ioStack
 *   - a virtual file descriptor  ioStack->openVal
 *   - a required block size      ioStack->blockSize
 *
 * The following coding style is an attempt at "monadic" style.  (Don't ask ...)
 * In this attempt, we do repeated tests for errors, so processing essentially stops
 * once the first error is encountered. But all those repeated tests obscure the
 * simplicity of monads, so this approach should be reconsidered.
 */
File FileOpenPerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	debug("FileOpenPerm: fileName=%s fileFlags=0x%x fileMode=0x%x\n", fileName, fileFlags, fileMode);

	/* We start with no resources allocated */
	IoStack *ioStack = NULL;
	File file = -1;

	/* I/O stacks don't implement O_APPEND. We will position to FileSize later */
	bool append = (fileFlags & O_APPEND) != 0;
	fileFlags &= ~O_APPEND;

	/* Clear any previous error information */
	FileClearError(-1);

	/* Get a prototype I/O stack for this file. */
	IoStack *proto = selectIoStack(fileName, fileFlags, fileMode);
	if (proto == NULL)
		fileSetError(-1, EIOSTACK, "No I/O stack for file %s", fileName);

	/* We do not support O_DIRECT */
	if (!FileError(-1) && (PG_O_DIRECT & fileFlags) != 0)
		fileSetError(-1, EIOSTACK, "O_DIRECT not supported for file %s", fileName);

	/* Open the  prototype I/O stack */
	if (!FileError(-1))
	{
		ioStack = stackOpen(proto, fileName, fileFlags, fileMode);
		file = (File) ioStack->openVal;
		if (file < 0)
			saveFileError(-1, ioStack);
	}

	/* Save the opened I/O stack in the vfd structure */
	if (!FileError(-1))
		getVfd(file)->ioStack = ioStack;

	/* Position at end of file if appending. This only impacts WriteSeq and ReadSeq. */
	if (!FileError(-1))
	{
		getVfd(file)->offset = append ? stackSize(ioStack) : 0;
		saveFileError(-1, ioStack);
	}

    /* If we failed, release resources */
	if (FileError(-1))
	{
		if (file >= 0)
		{
			FileClose(file);
			file = -1;
		}
		else if (ioStack != NULL)
			free(ioStack);
	}

	/* done */
	throwIoStackError(-1);
	return file;
}

/*
 * Close a file. Like FileOpen(), the error information is saved in the dummy "-1" file,
 * but it can also be accessed using the closed virtual file descriptor.
 *
 * Close has special error handling. If the vfd already has an error, we don't
 * overwrite it.  This is because the error may have been set by a previous
 * operation on the file, and we don't want to lose that information.
 * However, the return value will always be 0 if we closed the file successfully.
 */
int FileClose(File file)
{
	debug("FileClose: name=%s, file=%d  iostack=%p\n", getName(file), file, getStack(file));
	int retval = -1;
    FileClearError(-1);

	/* Place to save existing error information if any */
	IoStack saveError = {0};

	/* Point to the I/O stack, if any */
	IoStack *ioStack = (file < 0) ? NULL : getStack(file);
	if (ioStack == NULL)
	    fileSetError(-1, EBADF, "FileClose: invalid file descriptor %d", file);

	/* Save any existing error information */
	if (!FileError(-1))
		copyError(&saveError, -1, ioStack);

	/* Close the I/O stack. The low level routine will invalidate the "file" index */
	if (!FileError(-1))
	{
		retval = stackClose(ioStack);
		if (retval < 0)
			saveFileError(-1, ioStack);
	}

	/* However, if the file already had an error, we want to keep it instead */
	if (stackError(&saveError))
		saveFileError(-1, &saveError);

	/* No matter what, release the residual stack element */
	if (ioStack != NULL)
	{
		free(ioStack);
		getVfd(file)->ioStack = NULL;
	}

	debug("FileClose(done): file=%d retval=%d\n", file, retval);

	throwIoStackError(-1);
	return retval;
}


ssize_t FileRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileRead: name=%s file=%d  amount=%zd offset=%lld\n", getName(file), file, amount, offset);
	Assert(offset >= 0);
	Assert((ssize_t)amount > 0);

	FileClearError(file);
	IoStack *stack = getStack(file);

	/* Read the data as requested */
	pgstat_report_wait_start(wait_event_info);
	ssize_t actual = stackReadAll(stack, buffer, amount, offset);
	pgstat_report_wait_end();

	/* If successful, update the file offset */
	if (actual >= 0)
		getVfd(file)->offset = offset + actual;

	debug("FileRead(done): file=%d  name=%s  actual=%zd\n", file, getName(file), actual);
	throwIoStackError(file);
	return actual;
}


ssize_t FileWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileWrite: name=%s file=%d  amount=%zd offset=%lld\n", getName(file), file, amount, offset);
	Assert(offset >= 0 && (ssize_t)amount > 0);

	FileClearError(file);

	/* Write the data as requested */
	pgstat_report_wait_start(wait_event_info);
	ssize_t actual = stackWriteAll(getStack(file), buffer, amount, offset);
	pgstat_report_wait_end();

	/* If successful, update the file offset */
	if (actual >= 0)
		getVfd(file)->offset = offset + actual;

	debug("FileWrite(done): file=%d  name=%s  actual=%zd\n", file, getName(file), actual);
	throwIoStackError(file);
	return actual;
}

int FileSync(File file, uint32 wait_event_info)
{
	FileClearError(file);
	pgstat_report_wait_start(wait_event_info);
	int retval = stackSync(getStack(file));
	pgstat_report_wait_end();
	throwIoStackError(file);
	return retval;
}


off_t FileSize(File file)
{
	FileClearError(file);
	off_t size = stackSize(getStack(file));
	debug("FileSize: name=%s file=%d  size=%lld\n", getName(file), file, size);
	throwIoStackError(file);
	return size;
}

/*
 * Some I/O stacks require a specific blcok size. This routine returns the block size
 * in case a caller needs to know.  (eg. O_DIRECT files,  unbeffered encryption, ...)
 */
ssize_t FileBlockSize(File file)
{
	return getStack(file)->blockSize;
}

/*
 * Truncate a file to the specified length, returning -1 on failure.
 */
int	FileTruncate(File file, off_t offset, uint32 wait_event_info)
{
	FileClearError(file);
	pgstat_report_wait_start(wait_event_info);
	ssize_t retval = stackTruncate(getStack(file), offset);
	pgstat_report_wait_end();
	throwIoStackError(file);
	return (int)retval;
}


/*========================================================================================
 * Routines to emuulate C library FILE routines (fgetc, fprintf, ...)
 */

/*
 * Similar to fgetc. Probably best if used with buffered files.
 */
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

/*
 * Read sequentially from the file.
 */
ssize_t
FileReadSeq(File file, void *buffer, size_t amount, uint32 wait_event_info)
{
	return FileRead(file, buffer, amount, getVfd(file)->offset, wait_event_info);
}

/*
 * Write sequentially to the file.
 */
ssize_t
FileWriteSeq(File file, const void *buffer, size_t amount, uint32 wait_event_info)
{
	return FileWrite(file, buffer, amount, getVfd(file)->offset, wait_event_info);
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

/* ===================================================================
 * Error handling code.
 * These functions are similar to ferror(), but can be accessed even when the file is closed or -1.
 * This added feature allows error info to be fetched after a failed "open" or "close" call.
 */

/*
 * Helper function to get the I/O stack associated with a virtual file index.
 * Maintains a dummy I/O Stack for the specal case when the file index is -1.
 */
static inline IoStack *errStack(File file)
{
	/* dummy I/O stack when file == -1 */
	static IoStack staticIoStack = {0};

	/* Return the dummy if index is -1, else return the real stack */
	if (file < 0 || getVfd(file)->ioStack == NULL)
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

/*
 * Get the errno associated the last file operaton.
 */
int FileErrorCode(File file)
{
	return stackErrorCode(errStack(file));
}

/*
 * Helper function to save any errors from the I/O stack to the virtual file index.
 */
void saveFileError(File file, IoStack *ioStack)
{
	/* Copy the error, or clear the error, depending on ioStack */
	copyError(errStack(file), -1, ioStack);
}

/*
 * Throw an error if an I/O stack error occurred on the file.
 * Since callers don't know what to do with non-system errors,
 * this is probably the best way to handle them.
 * As a side effect, sets errno.
 */
void throwIoStackError(File file)
{
	if (FileErrorCode(file) == EIOSTACK)
	{
		const char *path = (file < 0 || getVfd(file)->fileName == NULL) ? "unknown" : FilePathName(file);
		ereport(ERROR,
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("I/O Error for %s: %s", path, FileErrorMsg(file)));
	}
	errno = FileErrorCode(file);
}

void fileSetError(File file, int errcode, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	setError(errStack(file), errcode, fmt, args);
	va_end(args);
}



/* TODO: the following keys may belong in a different file ... */
/* These need to be set properly */
static Byte *tempKey = (Byte *)"0123456789ABCDEF0123456789ABCDEF";
static size_t tempKeyLen = 32;
static Byte *permKey = (Byte *)"abcdefghijklmnopqrstuvwxyzABCDEF";
static size_t permKeyLen = 32;

/* Function to create a test stack for unit testing */
IoStack *(*testStackNew)() = NULL;

/* Prototype stacks.*/
static bool ioStacksInitialized = false;
IoStack *ioStackPlain;				/* Buffered, unmodified text */
IoStack *ioStackEncrypt;			/* Buffered and encrypted with a session key */
IoStack *ioStackEncryptPerm;        /* Buffered and encrypted with a session key */
IoStack *ioStackTest;				/* Stack used for unit testing */
IoStack *ioStackRaw;				/* Unbuffered "raw" file access */
IoStack *ioStackCompress;			/* Buffered and compressed, random reads only. */
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
 * Initialize the I/O stack infrastructure
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

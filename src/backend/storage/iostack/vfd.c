#include <unistd.h>
#include "postgres.h"

#include <dirent.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/resource.h>		/* for getrlimit */
#include <sys/stat.h>
#include <sys/types.h>

#include <limits.h>
#include <unistd.h>
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_tablespace.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/pg_prng.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/mem.h"
#include "postmaster/startup.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/resowner_private.h"

#include "storage/iostack_internal.h"
#include "storage/vfd.h"

/*
 * Hooks into internal fd.c routines.
 * We are the only users of these hooks, so externs are here rather than in fd.h
 * where anybody could access them.
 */
extern File PathNameOpenFilePerm_Private(const char *fileName, int fileFlags, mode_t fileMode);
extern int FileClose_Private(File file);
extern ssize_t FileRead_Private(File file, void *buffer, size_t amount, off_t offset);
extern ssize_t FileWrite_Private(File file, const void *buffer, size_t amount);
extern int FileSync_Private(File file);
extern off_t FileSize_Private(File file);
extern int	FileTruncate_Private(File file, off_t offset);

/* Forward References. */
static IoStack *selectIoStack(const char *path, int oflags, int mode);
IoStack *vfdNew();
void ioStackSetup(void);

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
		free(ioStack);
		return -1;
	}

	/* Save the I/O stack in the vfd structure */
	Vfd *vfdP = getVfd(file);
	vfdP->ioStack = ioStack;

	/* Position at end of file if appending. This only impacts WriteSeq and ReadSeq. */
	vfdP->offset = append? FileSize(file): 0;
	if (vfdP->offset == -1) {
		FileClose(file);
		return -1;
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
	free(stack); // TODO: ensure errno is preserved.
    debug("FileClose(done): file=%d retval=%d\n", file, retval);
	return (int)retval;
}


ssize_t FileRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileRead: name=%s file=%d  amount=%zu offset=%lld\n", getName(file), file, amount, offset);
	Assert(offset >= 0);
	Assert((ssize_t)amount > 0);

	/* Read the data as requested */
	pgstat_report_wait_start(wait_event_info);
	ssize_t actual = stackReadAll(getStack(file), buffer, amount, offset);
	pgstat_report_wait_end();

	/* If successful, update the file offset */
	if (actual >= 0)
		getVfd(file)->offset = offset + actual;

	debug("FileRead(done): file=%d  name=%s  actual=%zd\n", file, getName(file), actual);
	return actual;
}


ssize_t FileWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileWrite: name=%s file=%d  amount=%zu offset=%lld\n", getName(file), file, amount, offset);
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





/*
 * Bottom of an I/O stack using PostgreSql's Virtual File Descriptors.
 */
typedef struct VfdStack
{
	IoStack ioStack;   /* Header used for all I/O stack types */
	File file;         /* Virtual file descriptor of the underlying file. */
} VfdBottom;


/*
 * Open a file using a virtual file descriptor.
 */
static VfdBottom *vfdOpen(VfdBottom *proto, const char *path, int oflags, int mode)
{
	/* Clone the bottom prototype. */
	VfdBottom *this = vfdStackNew(); /* No parameters to copy */

	/* Clear the I/O Stack flags so we don't get confused */
	oflags &= ~PG_STACK_MASK;

	/* Open the file and get a VFD. */
	this->file = PathNameOpenFilePerm_Private(path, oflags, mode);
	checkSystemError(this, this->file, "Unable to open vfd file %s", path);

	/* We are byte oriented and can support all block sizes */
	thisStack(this)->blockSize = 1;
	thisStack(this)->openVal = this->file;

	/* Always return a new I/O stack structure. It contains error info if problems occurred. */
	debug("vfdOpen(done): file=%d  name=%s oflags=0x%x  mode=0x%x\n", this->file, path, oflags, mode);
	return this;
}

/*
 * Write a random block of data to a virtual file descriptor.
 */
static ssize_t vfdWrite(VfdBottom *this, const Byte *buf, size_t bufSize, off_t offset)
{
	ssize_t actual = FileWrite_Private(this->file, buf, bufSize);
	debug("vfdWrite: file=%d  name=%s  size=%zu  offset=%lld  actual=%zd\n", this->file, getName(this->file), bufSize, offset, actual);
	return checkSystemError(this, actual, "Unable to write to file");
}

/*
 * Read a random block of data from a virtual file descriptor.
 */
static ssize_t vfdRead(VfdBottom *this, Byte *buf, size_t bufSize, off_t offset, uint32 wait_event_info)
{
	Assert(bufSize > 0 && offset >= 0);

	ssize_t actual = FileRead_Private(this->file, buf, bufSize, offset);
	debug("vfdRead: file=%d  name=%s  size=%zu  offset=%lld  actual=%zd\n", this->file, getName(this->file), bufSize, offset, actual);
	return checkSystemError(this, actual, "Unable to read from file %s", getName(this->file));
}

/*
 * Close the virtual file descriptor.
 */
static ssize_t vfdClose(VfdBottom *this)
{
	debug("vfdClose: file=%d name=%s\n", this->file, getName(this->file));
	/* If the file is closed, mark it as a bad file descriptor */
	if (this->file < 0)
	{
		errno = EBADF;
		return checkSystemError(this, -1, "Closing a file which is alread closed");
	}

	/* Mark this stack as closed */
	File file = this->file;
	this->file = -1;
	getVfd(file)->ioStack = NULL;

	/* Close the file for real. */
	int retval = FileClose_Private(file);

	/* Note: We allocated ioStack in FileOpen, so we will free it in FileClose */
	debug("vfdClose(done): file=%d  retval=%d\n", file, retval);
	return checkSystemError(this, retval, "Unable to close file");
}


static ssize_t vfdSync(VfdBottom *this)
{
	int retval = FileSync_Private(this->file);
	return checkSystemError(this, retval, "Unable to sync file");
}

static off_t vfdSize(VfdBottom *this)
{
	off_t offset = FileSize_Private(this->file);
	return checkSystemError(this, offset, "Unable to get file size");
}


static bool vfdTruncate(VfdBottom *this, off_t offset)
{
	int retval = FileTruncate_Private(this->file, offset);
	return checkSystemError(this, retval, "Unable to truncate file") >= 0;
}


IoStackInterface vfdInterface = (IoStackInterface)
	{
		.fnOpen = (IoStackOpen)vfdOpen,
		.fnWrite = (IoStackWrite)vfdWrite,
		.fnRead = (IoStackRead)vfdRead,
		.fnClose = (IoStackClose)vfdClose,
		.fnSync = (IoStackSync)vfdSync,
		.fnTruncate = (IoStackTruncate)vfdTruncate,
		.fnSize = (IoStackSize)vfdSize,
	};


/**
 * Create a new Vfd I/O Stack.
 */
void *vfdStackNew()
{
	VfdBottom *this = malloc(sizeof(VfdBottom));
	*this = (VfdBottom)
		{
			.file = -1,
			.ioStack = (IoStack){
				.iface=&vfdInterface,
				.next=NULL,
			}
		};
	return this;
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
 * Initialize the I/O stack infrastructure. Creates prototype stacks.
 */
void ioStackSetup()
{
	/* Set up the prototype stacks */
	ioStackRaw = vfdStackNew();
	ioStackEncrypt = bufferedNew(1, aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen, vfdStackNew()));
	ioStackEncryptPerm = bufferedNew(1, aeadNew("AES-256-GCM", 16 * 1024, permKey, permKeyLen, vfdStackNew()));
	ioStackPlain = bufferedNew(64*1024, vfdStackNew());
	ioStackCompress = bufferedNew(1, lz4CompressNew(64 * 1024, vfdStackNew(), vfdStackNew()));
	ioStackCompressEncrypt = bufferedNew(1,
								 lz4CompressNew(1,
												bufferedNew(16*1024,
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


/*
 * Select the appropriate I/O Stack.
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

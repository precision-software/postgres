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
extern int FileRead_Private(File file, void *buffer, size_t amount, off_t offset, uint32 wait_event_info);
extern int FileWrite_Private(File file, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info);
extern int FileSync_Private(File file, uint32 wait_event_info);
extern off_t FileSize_Private(File file);
extern int	FileTruncate_Private(File file, off_t offset, uint32 wait_event_info);

/* Prototypes */
IoStack *IoStackNew(const char *path, int oflags, int mode);
IoStack *vfdNew();

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
	IoStack *ioStack = IoStackNew(fileName, fileFlags, fileMode);
	if (ioStack == NULL)
		return -1;

	/* I/O stacks don't implement O_APPEND, so position to FileSize instead */
	bool append = (fileFlags & O_APPEND) != 0;
	fileFlags &= ~O_APPEND;

	/* Open the I/O stack. If successful, save it in vfd structure. */
	File file = fileOpen(ioStack, fileName, fileFlags, fileMode);
	if (file < 0)
	{
		freeIoStack(ioStack);
		return -1;
	}
	Vfd *vfdP = getVfd(file);

	/* Save the I/O stack associated with this file */
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

	/* Close the file */
	int retval = fileClose(getStack(file));
	freeIoStack(getStack(file));
	getVfd(file)->ioStack = NULL;

	return retval;
}


int FileRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileRead: name=%s file=%d  amount=%zu offset=%lld\n", getName(file), file, amount, offset);

	/* Read the data as requested */
	ssize_t actual = fileReadAll(getStack(file), buffer, amount, offset, wait_event_info);

	/* If successful, update the file offset */
	if (actual >= 0)
		getVfd(file)->offset = offset + actual;

	debug("FileRead(done): actual=%zd\n", actual);
	return actual;
}


int FileWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	debug("FileWrite: name=%s file=%d  amount=%zu offset=%lld\n", getName(file), file, amount, offset);

	/* Write the data as requested */
	ssize_t actual = fileWriteAll(getStack(file), buffer, amount, offset, wait_event_info);

	/* If successful, update the file offset */
	if (actual >= 0)
		getVfd(file)->offset = offset + actual;

	debug("FileRead(done): actual=%zd\n", actual);
	return actual;
}

int FileSync(File file, uint32 wait_event_info)
{
	return fileSync(getStack(file), wait_event_info);
}


off_t FileSize(File file)
{
	off_t size = fileSize(getStack(file));
	debug("FileSize: name=%s, file=%d  size=%lld\n", getName(file), file, size);
	return size;
}

int	FileTruncate(File file, off_t offset, uint32 wait_event_info)
{
	return fileTruncate(getStack(file), offset, wait_event_info);
}


/*
 * Wrappers which fit the internal File* routines into the IoStack.
 */
typedef struct VfdStack
{
	IoStack ioStack;
	File file;
} VfdStack;


static int vfdOpen(VfdStack *this, const char *path, int oflags, int mode)
{
	/* Clear the I/O Stack flags so we don't get confused */
	oflags &= ~PG_STACK_MASK;
	this->file = PathNameOpenFilePerm_Private(path, oflags, mode);

	return checkSystemError(this, this->file, "Unable to open vfd file %s", path);
}


static ssize_t vfdWrite(VfdStack *this, const Byte *buf,size_t bufSize, off_t offset, uint32 wait_event_info)
{
	ssize_t actual = FileWrite_Private(this->file, buf, bufSize, offset, wait_event_info);
	return checkSystemError(this, actual, "Unable to write to file");
}

static ssize_t vfdRead(VfdStack *this, Byte *buf,size_t bufSize, off_t offset, uint32 wait_event_info)
{
	ssize_t actual = FileRead_Private(this->file, buf, bufSize, offset, wait_event_info);
	return checkSystemError(this, actual, "Unable to read from file");
}

static int vfdClose(VfdStack *this)
{
	int retval = FileClose_Private(this->file);
	return checkSystemError(this, retval, "Unable to close file");

}


static int vfdSync(VfdStack *this, uint32 wait_event_info)
{
	int retval = FileSync_Private(this->file, wait_event_info);
	return checkSystemError(this, retval, "Unable to sync file");
}

static off_t vfdSize(VfdStack *this)
{
	off_t offset = FileSize_Private(this->file);
	return checkSystemError(this, offset, "Unable to get file size");
}


static int vfdTruncate(VfdStack *this, off_t offset, uint32 wait_event_info)
{
	int retval = FileTruncate_Private(this->file, offset, wait_event_info);
	return checkSystemError(this, retval, "Unable to truncate file");
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
IoStack *vfdStackNew()
{
	VfdStack *this = malloc(sizeof(VfdStack));
	*this = (VfdStack)
		{
			.file = -1,
			.ioStack = (IoStack){
				.iface=&vfdInterface,
				.next=NULL,
				.blockSize = 1,
			}
		};
	return (IoStack *)this;
}

/* TODO: the following may belong in a different file ... */
/* These need to be set properly */
static Byte *tempKey = (Byte *)"0123456789ABCDEF0123456789ABCDEF";
static size_t tempKeyLen = 32;
static Byte *permKey = (Byte *)"abcdefghijklmnopqrstuvwxyzABCDEF";
static size_t permKeyLen = 32;

/* Function to create a test stack for unit testing */
IoStack *(*testStackFn)() = NULL;

/*
 * Construct the appropriate I/O Stack.
 * This function provides flexibility in now I/O stacks are created.
 * It can look at open flags, do GLOB matching on pathnames,
 * or create different stacks if reading vs writing.
 * The current version uses special "PG_*" open flags.
 */
IoStack *IoStackNew(const char *name, int oflags, int mode)
{

	/* Look at oflags to determine which stack to use */
	switch (oflags & PG_STACK_MASK)
	{
		case PG_NOCRYPT:
			return vfdStackNew();

		case PG_ENCRYPT:
			return bufferedNew(1024, vfdStackNew());
	/*		return bufferedNew(1,
				aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen,
					vfdStackNew())); */
		case PG_ENCRYPT_PERM:
			return bufferedNew(1,
							   aeadNew("AES-256-GCM", 16 * 1024, permKey, permKeyLen,
									   vfdStackNew()));

		case PG_TESTSTACK:
			Assert(testStackNew != NULL);
			return testStackFn();

		default:
			elog(FATAL, "Unrecognized encryption oflag 0x%x", (oflags & PG_STACK_MASK));
	}
}

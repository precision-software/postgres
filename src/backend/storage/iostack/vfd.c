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
		freeIoStack(ioStack);
	else
		getVfd(file)->ioStack = ioStack;

	/* Position at end of file if appending. This only impacts WriteSeq and ReadSeq. */
	if (append && file >= 0)
		FileSeek(file, FileSize(file));

	return file;
}

/*
 * Close a file.
 */
int FileClose(File file)
{
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
	int actual = fileReadAll(getStack(file), buffer, amount, offset, wait_event_info);
	return actual;
}


int FileWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
{
	return fileWriteAll(getStack(file), buffer, amount, offset, wait_event_info);
}

int FileSync(File file, uint32 wait_event_info)
{
	return fileSync(getStack(file), wait_event_info);
}


off_t FileSize(File file)
{
	return fileSize(getStack(file));
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


/* These need to be set properly */
static Byte *tempKey = (Byte *)"0123456789ABCDEF0123456789ABCDEF";
static size_t tempKeyLen = 32;
static Byte *permKey = (Byte *)"abcdefghijklmnopqrstuvwxyzABCDEF";
static size_t permKeyLen = 32;

/* Function to create a test stack for unit testing */
IoStack *(*testStackNew)() = NULL;

/*
 * Construct the appropriate I/O Stack, based on flags and file name.
 */
IoStack *IoStackNew(const char *name, int oflags, int mode)
{

	/* For normal work, look at oflags to determine which stack to use */
	switch (oflags & PG_STACK_MASK)
	{
		case PG_NOCRYPT:
			return vfdStackNew();

		case PG_ENCRYPT:
			return bufferedNew(1,
							   aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen,
									   vfdStackNew()));
		case PG_ENCRYPT_PERM:
			return bufferedNew(1,
							   aeadNew("AES-256-GCM", 16 * 1024, permKey, permKeyLen,
									   vfdStackNew()));

		case PG_TESTSTACK:
			Assert(testStackNew != NULL);
			return testStackNew();

		default:
			elog(FATAL, "Unrecognized encryption oflag 0x%x", (oflags & PG_STACK_MASK));
	}
}

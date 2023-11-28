
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

/*
 * Bottom of an I/O stack using PostgreSql's Virtual File Descriptors.
 */
typedef struct VfdStack
{
	IoStack ioStack;   /* Header used for all I/O stack types */
	File file;
	off_t fileSize;
} VfdBottom;

static ssize_t vfdClose(VfdBottom *this);


/*
 * Open a file using a virtual file descriptor.
 */
static VfdBottom *vfdOpen(VfdBottom *proto, const char *path, int oflags, mode_t mode)
{
	/* Clone the bottom prototype. */
	VfdBottom *this = vfdStackNew(); /* No parameters to copy */

	/* Clear the I/O Stack flags so we don't get confused */
	oflags &= ~PG_STACK_MASK;

	/* Open the file and get a VFD. */
	this->ioStack.openVal = PathNameOpenFilePerm_Internal(path, oflags, mode);
	if (this->ioStack.openVal < 0)
	{
		stackSetError(this, errno, "Unable to open vfd file %s", path);
		return this;
	}

	/* We are byte oriented and can support all block sizes TODO allow blockSize to support O_DIRECT */
	this->file = this->ioStack.openVal;
	this->ioStack.blockSize = 1;

	/* Fetch the size of the file */
	this->fileSize = FileSize_Internal(this->file);
	if (this->fileSize < 0)
	{
		int save_errno = errno;
		vfdClose(this);
		this->ioStack.openVal = -1;
		stackSetError(this, save_errno, "vfdOpen: Unable to get size of file %s", path);
		return this;
	}

	/* Always return a new I/O stack structure. It contains error info if problems occurred. */
	file_debug("(done): file=%d  name=%s oflags=0x%x  mode=0x%x", this->file, path, oflags, mode);
	return this;
}

/*
 * Write a random block of data to a virtual file descriptor.
 */
static ssize_t
vfdWrite(VfdBottom *this, const Byte *buf, ssize_t bufSize, off_t offset)
{
	ssize_t actual;
	Assert(offset <= this->fileSize);

	actual = FileWrite_Internal(this->file, buf, bufSize, offset);
	if (actual < 0)
		return stackCheckError(this, -1, "Unable to write to file %s", FilePathName(this->file));

	/* Update file size if it increased */
	this->fileSize = MAX(this->fileSize, offset + actual);

	file_debug("file=%d  name=%s  size=%zd  offset=%lld  actual=%zd", this->file, FilePathName(this->file), bufSize, offset, actual);
	return actual;
}

/*
 * Read a random block of data from a virtual file descriptor.
 */
static ssize_t
vfdRead(VfdBottom *this, Byte *buf, ssize_t bufSize, off_t offset)
{
	ssize_t actual;
	Assert(bufSize > 0 && offset >= 0);

	this->ioStack.eof = (offset >= this->fileSize);
	if (this->ioStack.eof)
		return 0;

	actual = FileRead_Internal(this->file, buf, bufSize, offset);
	if (actual < 0)
		return stackCheckError(this, actual, "Unable to read from file %s", FilePathName(this->file));

	file_debug("file=%d  name=%s  size=%zd  offset=%lld  actual=%zd", this->file, FilePathName(this->file), bufSize, offset, actual);
	return actual;
}

/*
 * Close the virtual file descriptor.
 */
static ssize_t
vfdClose(VfdBottom *this)
{
	ssize_t retval;
	file_debug("file=%d name=%s", this->file, FilePathName(this->file));

	/* Check if the file is already closed */
	if (badFile(this->file))
	    return -1;

	/* Close the file for real. */
	retval = FileClose_Internal(this->file);

	/* Note: We allocated ioStack in FileOpen, so we will free it in FileClose */
	file_debug("(done): file=%d  retval=%zd", this->file, retval);

	this->file = -1; /* Just to be sure */
	this->ioStack.openVal = -1;
	return stackCheckError(this, retval, "Unable to close file");
}


static ssize_t
vfdSync(VfdBottom *this)
{
	int retval = FileSync_Internal(this->file);
	return stackCheckError(this, retval, "Unable to sync file");
}


static off_t
vfdSize(VfdBottom *this)
{
	return this->fileSize;
}


static bool vfdResize(VfdBottom *this, off_t newSize)
{
	bool success;

	/* CASE: file is shrinking. Truncate it.*/
	if (newSize < this->fileSize)
	    success = FileTruncate_Internal(this->file, newSize) >= 0;

	/* CASE: file is growing a small amount (64K). Write out zeros. */
	else if (newSize < this->fileSize + 64*1024)
		success = FileZero_Internal(this->file, this->fileSize, newSize - this->fileSize) >= 0; /* todo: wait event info - drop it */

	/* OTHERWISE: larger allocation. Use fallocate */
	else
		success = FileFallocate_Internal(this->file, this->fileSize, newSize - this->fileSize) >= 0;

	if (!success)
		return stackCheckError(this, -1, "Unable to truncate file %s(%d). errno=%d", /* TODO: Include error message */
					  FilePathName(this->file), this->file, errno);

	/* Remember the new size */
	this->fileSize = newSize;

	return success;
}


IoStackInterface vfdInterface = (IoStackInterface)
	{
		.fnOpen = (IoStackOpen)vfdOpen,
		.fnWrite = (IoStackWrite)vfdWrite,
		.fnRead = (IoStackRead)vfdRead,
		.fnClose = (IoStackClose)vfdClose,
		.fnSync = (IoStackSync)vfdSync,
		.fnResize = (IoStackResize) vfdResize,
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

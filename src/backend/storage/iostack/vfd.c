
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
	File file;         /* Virtual file descriptor of the underlying file. */
	off_t fileSize;
} VfdBottom;


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

	/* We are byte oriented and can support all block sizes, unless O_DIRECT */
	this->ioStack.blockSize = (oflags & O_DIRECT) ? 4*1024 : 1;
	this->file = this->ioStack.openVal;

	/* Cache the file size */
	this->fileSize = FileSize_Internal(this->file);
	if (this->fileSize < 0)
	{
		stackSetError(this, errno, "Unable to get file size. file=%$d path=%s", this->file, path);
		FileClose_Internal(this->ioStack.openVal);
		this->ioStack.openVal = -1;
		return this;
	}

	/* We are byte oriented and can support all block sizes, unless O_DIRECT */
	this->ioStack.blockSize = (oflags & O_DIRECT) ? 4*1024 : 1;
	this->file = this->ioStack.openVal;

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

	if (offset % this->ioStack.blockSize != 0)
		return stackSetError(this, EINVAL,
				 "Offset %lld is not aligned with block size %zd", offset, this->ioStack.blockSize);

	actual = FileWrite_Internal(this->file, buf, bufSize, offset);
	file_debug("file=%d  name=%s  size=%zd  offset=%lld  actual=%zd", this->file, FilePathName(this->file), bufSize, offset, actual);
	if (actual < 0)
		return stackSetError(this, errno, "Unable to write to file");

	/* Update our file size if it grew */
	this->fileSize = MAX(this->fileSize, offset + actual);

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
	Assert(offset <= this->fileSize);

	/* Check for EOF.  TODO: add filesize to I/O stack and make part of stackWrite */
	this->ioStack.eof = (offset >= this->fileSize);
	if (this->ioStack.eof)
		return 0;

	actual = FileRead_Internal(this->file, buf, bufSize, offset);
	if (actual < 0)
		stackSetError(this, errno, "Unable to read from file %s", FilePathName(this->file));

	file_debug("file=%d  name=%s  size=%zd  offset=%lld  actual=%zd",
			   this->file, FilePathName(this->file), bufSize, offset, actual);

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


/*
 * Resize the file.
 */
static bool vfdTruncate(VfdBottom *this, off_t offset)
{
	bool success;

	/* If shrinking, truncate file, otherwise grow it. */
	if (offset < this->fileSize)
		success = FileTruncate_Internal(this->file, offset) >= 0;
	else
		success = FileFallocate_Internal(this->file, this->fileSize, offset-this->fileSize) >= 0;

	if (!success)
	{
		stackSetError(this, errno, "Unable to resize file %s(%d). from %lld to %lld errno=%d", /* TODO: Include error message */
					  FilePathName(this->file), this->file, this->fileSize, offset, errno);
		return false;
	}


	this->fileSize = offset;
	return true;
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

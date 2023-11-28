
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
	this->file = PathNameOpenFilePerm_Internal(path, oflags, mode);
	stackCheckError(this, this->file, "Unable to open vfd file %s", path);

	/* We are byte oriented and can support all block sizes TODO allow blockSize to support O_DIRECT */
	thisStack(this)->blockSize = 1;
	thisStack(this)->openVal = this->file;

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
	ssize_t actual = FileWrite_Internal(this->file, buf, bufSize, offset);
	file_debug("file=%d  name=%s  size=%zd  offset=%lld  actual=%zd", this->file, FilePathName(this->file), bufSize, offset, actual);
	return stackCheckError(this, actual, "Unable to write to file");
}

/*
 * Read a random block of data from a virtual file descriptor.
 */
static ssize_t
vfdRead(VfdBottom *this, Byte *buf, ssize_t bufSize, off_t offset)
{
	ssize_t actual;
	Assert(bufSize > 0 && offset >= 0);

	actual = FileRead_Internal(this->file, buf, bufSize, offset);
	file_debug("file=%d  name=%s  size=%zd  offset=%lld  actual=%zd", this->file, FilePathName(this->file), bufSize, offset, actual);
	return stackCheckError(this, actual, "Unable to read from file %s", FilePathName(this->file));
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
	off_t offset = FileSize_Internal(this->file);
	return stackCheckError(this, offset, "Unable to get file size");
}


static bool vfdResize(VfdBottom *this, off_t newSize)
{

	off_t fileSize;
	bool success;
	fileSize = vfdSize(this);
	if (fileSize < 0)
		return false;

	/* CASE: file is shrinking. Truncate it.*/
	if (newSize < fileSize)
	    success = FileTruncate_Internal(this->file, newSize) >= 0;

	/* CASE: file is growing a small amount (64K). Write out zeros. */
	else if (newSize < fileSize + 64*1024)
		success = FileZero(this->file, fileSize, newSize - fileSize, 0) >= 0; /* todo: wait event info - drop it */

	/* OTHERWISE: larger allocation. Use fallocate */
	else
		success = FileFallocate(this->file, fileSize, newSize - fileSize, 0) >= 0;

	if (!success)
		stackSetError(this, errno, "Unable to truncate file %s(%d). errno=%d", /* TODO: Include error message */
					  FilePathName(this->file), this->file, errno);
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

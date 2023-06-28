/*
 * vfd.c contains wrappers around the PostgreSql file access routines.
 * The wrappers implement the I/O stack interface, allowing the PostgreSql file
 * access routines to be used as the bottom of an I/O stack.
 *
 * Note we would prefer to return error messages rather than throwing errors
 * from the low level code.
 * TODO: convert errors to return messages in *_Internal routines. eg. FileSync_Internal.
 */
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
extern File PathNameOpenFilePerm_Internal(const char *fileName, int fileFlags, mode_t fileMode);
extern int FileClose_Internal(File file);
extern ssize_t FileRead_Internal(File file, void *buffer, size_t amount, off_t offset);
extern ssize_t FileWrite_Internal(File file, const void *buffer, size_t amount, off_t offset);
extern int FileSync_Internal(File file);
extern off_t FileSize_Internal(File file);
extern int	FileTruncate_Internal(File file, off_t offset);

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
	VfdBottom *this = vfdStackNew(thisStack(proto)->blockSize);

	/* Clear the I/O Stack flags so we don't get confused */
	oflags &= ~PG_STACK_MASK;

	/* Open the file and get a VFD. */
	this->file = PathNameOpenFilePerm_Internal(path, oflags, mode);
	checkSystemError(this, this->file, "Unable to open vfd file %s", path);

	/* We are byte oriented and can support all block sizes */
	thisStack(this)->openVal = this->file;

	/* Always return a new I/O stack structure even if we fail. It contains error info. */
	debug("vfdOpen(done): file=%d path=%s oflags=0x%x  mode=0x%x\n", this->file, path, oflags, mode);
	return this;
}

/*
 * Write a block of data to a virtual file descriptor.
 */
static ssize_t vfdWrite(VfdBottom *this, const Byte *buf, size_t bufSize, off_t offset)
{
	ssize_t actual = FileWrite_Internal(this->file, buf, bufSize, offset);
	debug("vfdWrite: file=%d  name=%s  size=%zd  offset=%lld  actual=%zd\n", this->file, getName(this->file), bufSize, offset, actual);
	return checkSystemError(this, actual, "Unable to write to file %s", FilePathName(this->file));
}

/*
 * Read a random block of data from a virtual file descriptor.
 */
static ssize_t vfdRead(VfdBottom *this, Byte *buf, size_t bufSize, off_t offset)
{
    ssize_t actual;
    Assert(bufSize > 0 && offset >= 0);

    actual = FileRead_Internal(this->file, buf, bufSize, offset);
	thisStack(this)->eof = (actual == 0);
	debug("vfdRead: file=%d  name=%s  size=%zd  offset=%lld  actual=%zd\n", this->file, getName(this->file), bufSize, offset, actual);
	return checkSystemError(this, actual, "Unable to read from file %s", FilePathName(this->file));
}

/*
 * Close the virtual file descriptor.
 */
static ssize_t vfdClose(VfdBottom *this)
{
    File file;
    ssize_t retval;

    debug("vfdClose: file=%d name=%s\n", this->file, FilePathName(this->file));

	/* Reset the file descriptor so we don't close it twice. */
	file = this->file;
	this->file = -1;

	/* Close the file */
	retval = FileClose_Internal(file);
	return checkSystemError(this, retval, "Unable to close file %d", file);
}


static ssize_t vfdSync(VfdBottom *this)
{
	ssize_t retval = FileSync_Internal(this->file);
	return checkSystemError(this, retval, "Unable to sync file %s", FilePathName(this->file));
}

static off_t vfdSize(VfdBottom *this)
{
	off_t offset = FileSize_Internal(this->file);
	return checkSystemError(this, offset, "Unable to get file size for %s", FilePathName(this->file));
}


static ssize_t vfdTruncate(VfdBottom *this, off_t offset)
{
	ssize_t retval = FileTruncate_Internal(this->file, offset);
	return checkSystemError(this, retval, "Unable to truncate file %s", FilePathName(this->file));
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


/*
 * Create a new Vfd I/O Stack.
 */
void *vfdStackNew(size_t blockSize)
{
	VfdBottom *this = malloc(sizeof(VfdBottom));
	*this = (VfdBottom)
		{
			.file = -1,
			.ioStack = (IoStack){
				.iface=&vfdInterface,
				.next=NULL,
				.blockSize=blockSize,
			}
		};
	return this;
}

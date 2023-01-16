//
// Created by John Morris on 1/13/23.
//
#include <fcntl.h>
#include <stdlib.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <stdbool.h>

#include "/usr/local/include/iostack.h"

#include "storage/pg_iostack.h"
#include "storage/fd.h"
#include "vfd_bottom.h"


/**
 * VfdBottom is the consumer of file system events, doing the actual
 * work of opening, closing, reading and writing files.
 * This particular sink works with a Posix file system, and it is
 * a straightforward wrapper around Posix system calls.
 */


/* A conventional POSIX file system for reading/writing a file. */
struct VfdBottom {
	Filter filter;   /* first in every Filter. */
	File vfd;        /* The file descriptor for the currrently open file. */
	bool writable;   /* Can we write to the file? */
	bool readable;   /* Can we read from the file? */
	bool eof;        /* Has the currently open file read past eof? */
	off_t position;  /* Track file position to use with vfd */
};

static Error errorCantWrite = (Error){.code=errorCodeFilter, .msg="Writing to file opened as readonly"};
static Error errorCantRead = (Error){.code=errorCodeFilter, .msg="Reading from file opened as writeonly"};
static Error errorReadTooSmall = (Error){.code=errorCodeFilter, .msg="unbuffered read was smaller than block size"};


/**
 * Open a VFD file.
 */
VfdBottom *vfdOpen(VfdBottom *sink, char *path, int oflags, int perm, Error *error)
{
	/* Clone ourself. */
	VfdBottom *this = vfdBottomNew();
	if (isError(*error))
		return this;

	/* Check the oflags we are opening the file in. TODO: move checks to ioStack. */
	this->writable = (oflags & O_ACCMODE) != O_RDONLY;
	this->readable = (oflags & O_ACCMODE) != O_WRONLY;
	this->eof = false;
	this->position = 0;

	/* Default file permission when creating a file. */
	if (perm == 0)
		perm = 0666;

	/* Open the file and check for errors. */
	this->vfd = PathNameFileOpen(path, oflags, perm);
	if (this->vfd == -1)
		setSystemError(error);

	return this;
}


/**
 * Write data to a file. For efficiency, we like larger buffers,
 * but in a pinch we can write individual bytes.
 */
size_t vfdWrite(VfdBottom *this, Byte *buf, size_t bufSize, Error *error)
{
	/* Check for errors. TODO: move to ioStack. */
	if (errorIsOK(*error) && !this->writable)
		*error = errorCantWrite;

	/* Write the data. */
	int actual = FileWrite(this->vfd, buf, bufSize, this->position);
	if (actual == -1)
		return setSystemError(error);

	/* On success, track the new position.
	this->position += actual;

	return actual;
}



/**
 * Read data from a file, checking for EOF. We like larger read buffers
 * for efficiency, but they are not required.
 */
size_t vfdRead(VfdBottom *this, Byte *buf, size_t size, Error *error)
{

	/* Check for errors. */
	if (isError(*error))                 ;
	else if (!this->readable)            *error = errorCantRead;
	else if (this->eof)                  *error = errorEOF;

	// Do the actual read.
	return sys_read(this->fd, buf, size, error);
}


/**
 * Close a Posix file.
 */
void vfdClose(VfdBottom *this, Error *error)
{
	/* Close the fd if it was opened earlier. */
	sys_close(this->fd, error);
	free(this);
}


/**
 * Push data which has been written out to persistent storage.
 */
void vfdSync(VfdBottom *this, Error *error)
{
	/* Error if file was readonly. */
	if (errorIsOK(*error) && !this->writable)
		*error = errorCantWrite;

	/* Go sync it. */
	if (this->writable)
		sys_datasync(this->fd, error);
}


/**
 * Negotiate the block size for reading and writing.
 * Since we are not supporting O_DIRECT yet, we simply
 * return 1 indicating we can deal with any size.
 */
size_t vfdBlockSize(VfdBottom *this, size_t prevSize, Error *error)
{
	return 1;
}


/**
 * Abort file access, removing any files we may have created.
 * This allows us to remove temporary files when a transaction aborts.
 * Not currently implemented.
 */
void vfdAbort(VfdBottom *this, Error *errorO)
{
	abort(); /* TODO: not implemented. */
}

off_t vfdSeek(VfdBottom *this, pos_t position, Error *error)
{
	return sys_lseek(this->fd, position, error);
}


void vfdDelete(VfdBottom *this, char *path, Error *error)
{
	/* Unlink the file, even if we've already had an error */
	Error tempError = errorOK;
	sys_unlink(path, &tempError);
	setError(error, tempError);

}

FilterInterface vfdInterface = (FilterInterface)
	{
		.fnOpen = (FilterOpen)vfdOpen,
		.fnWrite = (FilterWrite)vfdWrite,
		.fnRead = (FilterRead)vfdRead,
		.fnClose = (FilterClose)vfdClose,
		.fnSync = (FilterSync)vfdSync,
		.fnBlockSize = (FilterBlockSize)vfdBlockSize,
		.fnAbort = (FilterAbort)vfdAbort,
		.fnSeek = (FilterSeek)vfdSeek,
		.fnDelete = (FilterDelete)vfdDelete
	};


/**
 * Create a new Posix file system Sink.
 */
VfdBottom *vfdBottomNew()
{
	VfdBottom *this = malloc(sizeof(VfdBottom));
	*this = (VfdBottom)
		{
			.fd = -1,
			.filter = (Filter){
				.iface=&vfdInterface,
				.next=NULL}
		};
	return this;
}

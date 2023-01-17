//
// Created by John Morris on 1/13/23.
//
#include <fcntl.h>
#include <stdlib.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <stdbool.h>

#include "postgres.h"
#include "/usr/local/include/iostack/iostack.h"
#include "/usr/local/include/iostack/iostack_error.h"
#include "/usr/local/include/iostack/common/filter.h"

#include "storage/pg_iostack.h"
#include "storage/fd.h"
#include "vfd_bottom.h"

extern uint32 IoStackWaitEvent;

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
	off_t position;  /* Track file position to use with vfd */
};


/**
 * Open a VFD file.
 */
 static
 VfdBottom *vfdOpen(VfdBottom *sink, char *path, int oflags, int perm, Error *error)
{
	/* Clone ourself. */
	VfdBottom *this = vfdBottomNew();
	if (isError(*error))
		return this;

	/* We are opening real vfds, not I/O Stacks */
	oflags &= ~PG_O_IOSTACK;

	/* Default file permission when creating a file. */
	if (perm == 0)
		perm = 0666;

	/* Open the file and check for errors. */
	this->vfd = PathNameOpenFilePerm(path, oflags, perm);
	if (this->vfd == -1)
	{
		setSystemError(error);
		return NULL;
	}

	/* Set our file position, checking for O_APPEND */
	this->position = (oflags & O_APPEND)? FileSize(this->vfd): 0;

	return this;
}


/**
 * Write data to a file. For efficiency, we like larger buffers,
 * but in a pinch we can write individual bytes.
 */
static size_t
vfdWrite(VfdBottom *this, Byte *buf, size_t bufSize, Error *error)
{
	/* Check for errors. TODO: move to ioStack. */
	if (isError(*error)) return 0;

	/* Write the data. */
	int actual = FileWrite(this->vfd, buf, bufSize, this->position, IoStackWaitEvent);
	if (actual == -1)
		actual = setSystemError(error);

	/* On success, track the new position. */
	this->position += actual;

	return actual;
}



/**
 * Read data from a file, checking for EOF. We like larger read buffers
 * for efficiency, but they are not required.
 */
static size_t
vfdRead(VfdBottom *this, Byte *buf, size_t size, Error *error)
{

	/* Check for errors. */
	if (isError(*error)) return 0;

	/* Read the data. */
	int actual = FileRead(this->vfd, buf, size, this->position, IoStackWaitEvent);
	if (actual == -1)
		actual = setSystemError(error);

	/* On success, track the new position. */
	this->position += actual;

	return actual;
}


/**
 * Close a VFD file
 */
static void
vfdClose(VfdBottom *this, Error *error)
{
	/* Close the fd if it was opened earlier. */
	FileClose(this->vfd);
	free(this);
}


/**
 * Push data which has been written out to persistent storage.
 */
static void
vfdSync(VfdBottom *this, Error *error)
{
	/* Check for errors */
	if (isError(*error)) return;

	/* Go sync it. */
	int res = FileSync(this->vfd, IoStackWaitEvent);
	if (res == -1)
		res = setSystemError(error);
}


/**
 * Negotiate the block size for reading and writing.
 * Since we are not supporting O_DIRECT yet, we simply
 * return 1 indicating we can deal with any size.
 */
static size_t
vfdBlockSize(VfdBottom *this, size_t prevSize, Error *error)
{
	return 1;
}


/**
 * Abort file access, removing any files we may have created.
 * This allows us to remove temporary files when a transaction aborts.
 * Not currently implemented.
 */
static void
vfdAbort(VfdBottom *this, Error *errorO)
{
	abort(); /* TODO: not implemented. */
}

static off_t
vfdSeek(VfdBottom *this, off_t position, Error *error)
{
    if (position == FILE_END_POSITION)
		this->position = FileSize(this->vfd);
	else
	    this->position = position;

	return this->position;
}


static void
vfdDelete(VfdBottom *this, const char *path, Error *error)
{
	/* Unlink the file, even if we've already had an error */
	int res = durable_unlink(path, LOG);
	if (res == -1)
		setSystemError(error);
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
			.vfd = -1,
			.filter = (Filter){
				.iface=&vfdInterface,
				.next=NULL}
		};
	return this;
}

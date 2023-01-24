//
// Created by John Morris on 1/12/23.
//

#include <fcntl.h>

#include "postgres.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/pg_iostack.h"

/*
 * Our current wait timer, passed as a global to vfd_bottom.
 *
 * This global variable tells the lower routines which wait event to use.
 * TODO: Passing a global is a bit of a Kludge ...
 */
uint32 IoStackWaitEvent = 0;


int checkForError(int ret, Error error);


/* These are the pre-configured I/O stacks */
static IoStack *encryptStack = NULL;  /* Encryption only */
static IoStack *ecompressStack = NULL; /* Encryption plus compression */

/*
 * Open an I/O stack for the given file
 */
IoStack *IoStackOpen(const char *fileName, int fileFlags, mode_t fileMode)
{
	Error error = errorOK;
	debug("IoStackOpen: fileName=%s  fileFlags=0x%x  fileMode=0x%x\n", fileName, fileFlags, fileMode);

	/* Verify I/O stacks have been configured */
	if (encryptStack == NULL || ecompressStack == NULL)
		ioStackError(&error, "Opening a file before I/O stacks are configured");

	/* Based on the flags, choose which I/O stack to use */
	IoStack *prototype = NULL;
	if ( (fileFlags & PG_IOSTACK) == PG_ENCRYPT)
		prototype = encryptStack;
	else if ((fileFlags & PG_IOSTACK) == PG_ECOMPRESS)
		prototype = ecompressStack;
	else
		ioStackError(&error, "Unexpected flags for opening an I/O stack");


	/* Clone the prototype and open it */
	IoStack *iostack = fileClone(prototype);
	fileOpen(iostack, fileName, fileFlags, fileMode, &error);

    if (isError(error))
	{
		Error ignoreError = errorOK;
		fileClose(iostack, &ignoreError);
		fileFree(iostack);
		iostack = NULL;
	}

	debug("IoStackOpen(done): iostack=%p  msg=%s", iostack, error.msg);
	checkForError(-1, error);  /* set errno if a system error, abort otherwise */
	return iostack;
}


/*
 * Read data from an opened I/O stack
 */
int IoStackRead(IoStack *iostack,  void *buffer, size_t amount, off_t offset,
				   uint32 wait_event_info)
{
	Error error = errorOK;
	IoStackWaitEvent = wait_event_info;
	debug("IoStackRead: amount=%zu offset=%lld\n", amount, offset);

	pgstat_report_wait_start(wait_event_info);

	fileSeek(iostack, offset, &error);
	size_t actual = fileRead(iostack, buffer, amount, &error);

	pgstat_report_wait_end();
    return checkForError(actual, error);
}


/*
 * Write data to an opened I/O stack
 */
int IoStackWrite(IoStack *iostack, const void *buffer, size_t amount, off_t offset,
				   uint32 wait_event_info)
{
	Error error = errorOK;
	IoStackWaitEvent = wait_event_info;
	debug("IoStackWrite: amount=%zu offset=%lld\n", amount, offset);

	pgstat_report_wait_start(wait_event_info);

	fileSeek(iostack, offset, &error);
	size_t actual = fileWrite(iostack, buffer, amount, &error);

	pgstat_report_wait_end();
	return checkForError(actual, error);
}


/*
 * Close an opened I/O stack, deleting the file if requested.
 */
int IoStackClose(IoStack *iostack, char *deleteName)
{
	debug("IoStackClose: iostack=%p deleteName=%s\n", iostack, deleteName);

	/* Close the file */
	Error error = errorOK;
	fileClose(iostack, &error);

	/* Delete the file if it was temporary */
	if (deleteName)
		fileDelete(iostack, deleteName, &error);

	/* Free the cloned iostack */
	debug("IoStackClose(before fileFree): iostack=%p  next=%p", iostack, *(void **)iostack);
	//fileFree(iostack); TODO: debugging - create deliberate memory leak

	/* Check for errors, returning 0 on success */
	debug("IoStackClose(done): msg=%s", error.msg);
	return checkForError(0, error);
}

/*
 * The following are not implemented yet. Just dummy them out so things "seem to work"
 */
int IoStackSync(IoStack *iostack, uint32 wait_event_info)
{
	return 0;
}

int IoStackPrefetch(IoStack *iostack, off_t offset, off_t amount, uint32 wait_event_info)
{
	return 0;
}

void IoStackWriteback(IoStack *iostack, off_t offset, off_t nbytes, uint32 wait_event_info)
{
}

off_t IoStackSize(IoStack *iostack)
{
	Error error = errorOK;
	debug("IoStackSize: \n");
	return fileSeek(iostack, FILE_END_POSITION, &error);
}


int IoStackTruncate(IoStack *iostack, off_t offset, uint32 wait_event_info)
{
	debug("IoStackTruncate: offset=%lld\n", offset);
	errno = EINVAL;
	return -1;
}

/*
 * Check for errors, returning -1 and errno on system errors.
 */
int checkForError(int ret, Error error)
{
	/* EOFs are OK */
	if (errorIsEOF(error))
		return 0;

	/* System errors - set errno and return -1 */
	else if (errorIsSystem(error)) {
		debug("checkForError: errno=%d msg=%s\n", -error.code, error.msg);
		errno = -error.code;
		return -1;
	}

	/* Other errors, raise an exception */
	else if (isError(error))
		ereport(FATAL, errmsg("IoStack error: code=%d  msg=%s", error.code, error.msg));

	/* Otherwise, everything is just fine. */
	else
		return ret;
}


void IoStackSetup(Byte *key, size_t keySize)
{
	debug("IoStackSetup: key=%32s", key);
	encryptStack =
		ioStackNew(
			bufferedNew(1024,
				aeadFilterNew("AES-256-GCM", 1024, key, keySize,
					vfdBottomNew())));
	ecompressStack =
		ioStackNew(
			bufferedNew(1024,
				lz4CompressNew(1024,
					bufferedNew(1024*1024,
						aeadFilterNew("AES-256-GCM", 1024, key, keySize,
							vfdBottomNew())))));
}

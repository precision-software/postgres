//
// Created by John Morris on 1/12/23.
//

#include <fcntl.h>
#include "/usr/local/include/iostack.h"  //<iostack.h>

#include "postgres.h"
#include "pgstat.h"

#include "storage/pg_iostack.h"
#include "storage/fd.h"

/* TODO: configure system include directories */
#include "/usr/local/include/iostack/iostack.h"
#include "/usr/local/include/iostack/file/fileSystemBottom.h"



/* A prototype file pipeline to use when opening files */
void *IoStackPrototype = NULL;

/*
 * Our current wait timer.
 * When using VFDs, the Read/Write/Sync code sets the wait timer,
 * so we need to pass the desired wait timer to them.
 * But if not using VFDs, the wait timers would not be set.
 * The compromise is to set it at both the high level code
 * and the low level vfd routines.
 * This global variable tells the lower routines which wait event to use */
uint32 IoStackWaitEvent = 0;


int checkForError(int ret, Error error);

/*
 * Open an I/O stack for the given file
 */
IoStack *IoStackOpen(IoStack *prototype, const char *fileName, int fileFlags, mode_t fileMode)
{
	Error error = errorOK;

	/* Clear the flags so lower level "open" use Postgres Vfds */
	/*   During development, the flag must be set to use IoStacks.  Later, the flag will reverse meaning */
	fileFlags &= ~PG_O_IOSTACK;

	/* Open the file using the prototype I/O stack */
	IoStack *iostack = fileOpen(prototype, fileName, fileFlags, fileMode, &error);
	if (isError(error) && checkForError(0, error) == -1)
		return NULL;

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

	/* If we will delete the file, then clone the pipeline first */
	IoStack *clone = NULL;
	Error ignorableError = errorEOF;
	if (deleteName)
		clone = fileOpen(iostack, "", 0, 0, &ignorableError);

	/* Close the file, freeing the pipeline. */
	Error error = errorOK;
	fileClose(iostack, &error);

	/* Delete the file if it was temporary */
	if (deleteName)
	{
		/* Delete the file using the cloned pipeline, then free the clone */
		fileDelete(clone, deleteName, &error);
		fileClose(clone, &error);
	};

	/* Check for errors, returning 0 on success */
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
	return fileSeek(iostack, FILE_END_POSITION, &error);
}


int IoStackTruncate(IoStack *iostack, off_t offset, uint32 wait_event_info)
{
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
		errno = -error.code;
		ereport(LOG,
				(errcode_for_file_access(),
					errmsg("I/O Stack error: %s %m", error.msg)));
		return -1;
	}

	/* Other errors, raise an exception */
	else if (isError(error))
		ereport(ERROR, errmsg("IoStack error: %s", error.msg));

	/* Otherwise, everything is just fine. */
	else
		return ret;
}


void IoStackSetup(void)
{
    IoStackPrototype = ioStackNew( fileSystemBottomNew() );
}

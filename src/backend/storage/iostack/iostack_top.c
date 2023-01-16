//
// Created by John Morris on 1/12/23.
//

#include <fcntl.h>
#include "/usr/local/include/iostack.h"  //<iostack.h>

#include "postgres.h"
#include "pgstat.h"

#include "storage/pg_iostack.h"
#include "storage/fd.h"


int checkForError(int ret, Error error);

/* A prototype file pipeline to use when opening files */
void *IoStackPrototype = NULL;


IoStack *IoStackOpen(IoStack *prototype, const char *fileName, int fileFlags, mode_t fileMode)
{
	Error error = errorOK;

	/* Clear the flags so VfdBottom opens Postgres Vfds rather than opening new IoStacks */
	/*   During development, the flag must be set to use IoStacks.  Later, the flag will reverse meaning */
	fileFlags &= ~PG_O_VFD;

	/* Open the file using the prototype I/O stack */
	IoStack *iostack = fileOpen(prototype, fileName, fileFlags, fileMode, &error);
	if (isError(error) && checkForError(0, error) == -1)
		return NULL;


	return iostack;
}


int IoStackRead(IoStack *iostack,  *buffer, size_t amount, off_t offset,
				   uint32 wait_event_info)
{
	Error error = errorOK;

	pgstat_report_wait_start(wait_event_info);

	fileSeek(iostack, offset, &error);
	size_t actual = fileRead(iostack, buffer, amount, &error);
	pgstat_report_wait_end();

    return checkForError(actual, error);
}

int IoStackWrite(IoStack *iostack, const void *buffer, size_t amount, off_t offset,
				   uint32 wait_event_info)
{
	Error error = errorOK;
	pgstat_report_wait_start(wait_event_info);

	fileSeek(iostack, offset, &error);
	size_t actual = fileRead(istack, buffer, amount, &error);

	pgstat_report_wait_end();
	return checkForError(actual, error);
}

int IoStackClose(IoStack *iostack, char *deleteName)
{

	/* If we will delete the file, then clone the pipeline first */
	IoStack *clone = NULL;
	Error ignoreError = errorEOF;
	if (deleteName)
		clone = fileOpen(iostack, "", 0, 0, &ignoreError);

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


int checkForError(int ret, Error error)
{
	if (errorIsEOF(error))
		return 0;

	else if (errorIsSystem(error)) {
		errno = -error.code;
		return -1;
	}

	else if (isError(error))
	{
		ereport(ERROR,
				(errcode ..., error.msg));
	}

	else
		return ret;
}

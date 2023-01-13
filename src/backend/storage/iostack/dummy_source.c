//
// Created by John Morris on 1/12/23.
//
#include "storage/iostack.h"
#include "storage/fd.h"
#include <fcntl.h>


/* A prototype file pipeline to use when opening files */
void *IoStackPrototype = NULL;


File IoStackOpen(const char *fileName, int fileFlags, mode_t fileMode)
{
	Error error = errorOK;

	/* Open the file using the prototype file pipeline */
	IoStack *fs = fileOpen(IoStackPrototype, fileName, fileFlags, &error);
	if (isError(error))
		return checkForError(-1, error);

	/* Allocate a vfd to hold the newly opened file source */
	File file = AllocateVfd();
	VfdCache[file] = (Vfd)
	{
	.fileName = strdup(fileName),
	.fs = fs,
	.fd = VFD_CLOSED
	}

	return file;
}


int IoStackRead(File file, void *buffer, size_t amount, off_t offset,
				   uint32 wait_event_info)
{
	IoStack *fs = VfdCache[file].fs;
	Error error = errorOK;

	pgstat_report_wait_start(wait_event_info);

	fileSeek(fs, offset, &error);
	size_t actual = fileRead(fs, buffer, amount, &error);

	pgstat_report_wait_end();

    return checkForError(actual, error);
}

int IoStackWrite(File file, void *buffer, size_t amount, off_t offset,
				   uint32 wait_event_info)
{
	IoStack *fs = VfdCache[file].fs;
	Error error = errorOK;
	pgstat_report_wait_start(wait_event_info);

	fileSeek(fs, offset, &error);
	size_t actual = fileRead(fs, buffer, amount, &error);

	pgstat_report_wait_end();
	return checkForError(actual, error);
}

int IoStackClose(File file)
{
	Error error = errorOK;
	Vfd *vfdP = &VfdCache[file];

	if (vfdP->fs == NULL)
		return 0;

	/* If we will delete the file, then clone the pipeline first */
	IoStack *clone = NULL;
	if (vfdP->fdstat & FD_DELETE_AT_CLOSE)
		clone = IoStackOpen(vfpdP->fs, "", 0, 0, &errorEOF);

    /* Close the file, freeing the pipeline. */
	fileClose(vfdP->fs, &error);
	vfdP->fs = NULL;
	if (isError(error))
		elog()

	/* Delete the file if it was temporary */
	if (vfdP->fdstate & FD_DELETE_AT_CLOSE)
	{
		/*
		 * If we get an error, as could happen within the ereport/elog calls,
		 * we'll come right back here during transaction abort.  Reset the
		 * flag to ensure that we can't get into an infinite loop.  This code
		 * is arranged to ensure that the worst-case consequence is failing to
		 * emit log message(s), not failing to attempt the unlink.
		 */
		vfdP->fdstate &= ~FD_DELETE_AT_CLOSE;

		/* Delete the file using the cloned pipeline, then free the clone */
		fileDelete(clone, vfdP->name, &error);
		fileClose(clone, &error);in
	}

	/* Unregister it from the resource owner */
	if (vfdP->resowner)
		ResourceOwnerForgetFile(vfdP->resowner, file);

	FreeVfd(file);
}


int checkForError(ret, Error error)
{
	if (errorIsEOF(error))
		return 0;
	else if (isSystemError(error)) {
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

//
//
//

#include <fcntl.h>
#include <fnmatch.h>

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


/*
 * Open an I/O stack for the given file
 */
IoStack *IoStackOpen(IoStack *prototype, const char *fileName, int fileFlags, mode_t fileMode)
{
	Error error = errorOK;
	debug("IoStackOpen: fileName=%s  fileFlags=0x%x  fileMode=0x%x\n", fileName, fileFlags, fileMode);

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


/*
 * Look at the file path and lookup the correspoinding I/O Stack.
 */
IoStack *sessionStack;
IoStack *databaseStack;
IoStack *noStack;

static struct {const char *pattern; IoStack **iostack;} configs[] =
{
	{"pg_logical/**", &sessionStack},
	{"PGVERSION", &noStack},
	{"pg_replslot/*/xid*.spill", &sessionStack},
	//{"*/pg_filenode.map", &sessionStack},
	//{"pg_wal/*", &noStack},
};

#define countof(array) (sizeof(array)/sizeof(*array))
/*
 * Select which I/O stack to use for the given file path.
 */
IoStack *pickIoStack(const char *path)
{
    int idx;
    for (idx = 0;  idx < countof(configs); idx++)
		if (fnmatch(configs[idx].pattern, path, FNM_PATHNAME) == 0)
			break;

	/* Return the iostack if matched, noStack if not matched */
	debug("pickStack: path=%s  idx=%d", path, idx);
	return idx < countof(configs)? *configs[idx].iostack: noStack;
}


/*
 * Initialize I/O stacks.
 */
void IoStackSetup()
{
    /* Get the keys for both permanent and temporary files TODO: */
	Byte *permanentKey  = (Byte *)"12345678901234567890123456789012";
	Byte *temporaryKey  = (Byte *)"abcdefghijklmnopqrstuvwxyzABCDEF";
	size_t blockSize = 16*1024;

	/* Encrypted with a key generated every session */
	sessionStack =
		ioStackNew(
			bufferedNew(blockSize,
						aeadFilterNew("AES-256-GCM", blockSize, temporaryKey, 32,
									  vfdBottomNew())));

	/* Encrypted with the permanent database key so file is available in future */
	databaseStack =
		ioStackNew(
			bufferedNew(blockSize,
						aeadFilterNew("AES-256-GCM", blockSize, permanentKey, 32,
									  vfdBottomNew())));
    /* No encryption or buffering */
	noStack = NULL;
}

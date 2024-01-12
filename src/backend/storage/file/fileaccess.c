/*
 * Routines for accessing Temp and other non-relational files opened
 * with the "FOpen()" call.
 */
#include "postgres.h"
#include "common/file_perm.h"
#include "storage/fd.h"
#include "storage/fileaccess.h"
#include "storage/iostack_internal.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"

/* Forward references */
static IoStack *getStack(File file);
static IoStack *getErrStack(File file);
static bool badFile(File file);
static inline void updateFileState(File file, off_t offset, ssize_t actual);

/*========================================================================================
 * Routines to emuulate C library FILE routines (fgetc, fprintf,
 */

/*
 * Similar to fgetc. Best if used with buffered files.
 */
int
FGetc(File file, uint32 wait)
{
	unsigned char c;
	int ret = (int) FReadSeq(file, &c, 1, wait);
	if (ret <= 0)
		return EOF;
	return c;
}

/*
 * Similar to fputc
 */
int FPutc(File file, unsigned char c, uint32 wait)
{
	if (FWriteSeq(file, &c, 1, wait) <= 0)
		return EOF;
	return c;
}

/*
 * Read a line of text, dropping cr/lf and terminate with EOL.
 */
bool FReadLine(File file, char *line, ssize_t maxLen, uint32 wait)
{
    ssize_t len;
	file_debug("file=%d path=%s  maxLen=%zd", file, FilePathName(file), maxLen);

    for (len = 0; len < maxLen-1; len++)
    {
        /* Get the next character */
        line[len] = FGetc(file, wait);

        /* Skip C/R in case it came from Windows. */
        if (line[len] == '\r')
            line[len] = FGetc(file, wait);

        /* Check for end of line */
        if (line[len] == '\n' || line[len] == EOF)
            break;
    }

    /* Terminate the line we just read */
    line[len] = '\0';

	/* Successful if we read a valid line.  Failed if error or eof */
	file_debug("   line=%s  len=%zd  eof=%d  errno=%d", line, len, FEof(file), FErrorCode(file));
    return !FError(file) && (len > 0 || !FEof(file));
}

/*
 * A temporary equivalent of fprintf.
 * This version limits text to what fits in a local buffer.
 * Ultimately, we need to update the internal snprintf.c (dopr) to spill
 * to temporary files.
 */
ssize_t FPrint(File file, const char *format, ...)
{
	va_list args;
	char buffer[4*1024]; /* arbitrary size, big enough? */
	int size;

	va_start(args, format);
	size = vsnprintf(buffer, sizeof(buffer), format, args);
	va_end(args);

	if (size < 0 || size >= sizeof(buffer))
		ereport(ERROR,
				errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				errmsg("FilePrintf buffer overflow - %d characters exceeded %lu buffer", size, sizeof(buffer)));

	return FWriteSeq(file, buffer, Min(size, sizeof(buffer)-1), 0);
}

ssize_t FScan(File file, const char *format, ...)
{
	Assert(false); /* Not implemented */
	return -1;
}

ssize_t FPuts(File file, const char *string, uint32 wait)
{
	return FWriteSeq(file, string, strlen(string), wait);
}

/*
 * Read sequentially from the file.
 */
ssize_t
FReadSeq(File file, void *buffer, size_t amount, uint32 wait)
{
	if (badFile(file))
		return -1;

	return FRead(file, buffer, amount, getFState(file)->offset, wait);
}

/*
 * Write sequentially to the file.
 */
ssize_t
FWriteSeq(File file, const void *buffer, size_t amount, uint32 wait)
{
	if (badFile(file))
		return -1;
	return FWrite(file, buffer, amount, getFState(file)->offset, wait);
}

/*
 * Seek to an absolute position within the file.
 * Relative positions can be calculated using FileTell or FileSize.
 */
off_t
FSeek(File file, off_t offset)
{
	file_debug("file=%d  offset=%lld", file, offset);
	if (badFile(file))
		return -1;

	getFState(file)->offset = offset;
	return offset;
}

/*
 * Tell us the current file position
 */
off_t
FTell(File file)
{
	if (badFile(file))
		return -1;
	return getFState(file)->offset;
}

/*
 * Preferred procedure for opening a file
 */
File FOpen(const char *fileName, uint64 fileFlags)
{
	return FOpenPerm(fileName, fileFlags, pg_file_create_mode);
}

/*
 * Open a file, specifying permissions.
 */
File FOpenPerm(const char *fileName, uint64 fileFlags, mode_t fileMode)
{
	File file;
	IoStack *proto;
	IoStack *ioStack;
	bool append;
	FileState *fstate;

	file_debug("FileOpenPerm: fileName=%s fileFlags=0x%lx fileMode=0x%hx", fileName, fileFlags, fileMode);

	/* Reserve a resource owner slot if we need one */
	if (fileFlags & PG_XACT)
		ResourceOwnerEnlarge(CurrentResourceOwner);

	/* I/O stacks don't implement O_APPEND, so seek to end of file instead */
	append = (fileFlags & O_APPEND) != 0;
	fileFlags &= ~O_APPEND;

	/* Decide which I/O stack to use. */
	proto = selectIoStack(fileName, fileFlags, fileMode);

	/* Open the  prototype I/O stack */
	ioStack = stackOpen(proto, fileName, fileFlags, fileMode);
	if (ioStack == NULL)
		return setFileError(-1, errno, "Unable to allocate I/O stack for %s", fileName);

	/* Save error info if open failed */
	file = (File) ioStack->openVal;
	if (file < 0)
	{
		copyError(getErrStack(-1), -1, ioStack);
		free(ioStack);
		return -1;
	}

	Assert(file > 0);

	/* Success. Save the io stack (unless legacy) in the vfd */
	fstate = getFState(file);
	fstate->ioStack = ioStack;

	/* Close when resource owner is released */
	if (fileFlags & PG_XACT)
		RegisterTemporaryFile(file);

	/* Delete file when closing */
	if (fileFlags & PG_DELETE)
		setDeleteOnClose(file);

	/* Account for temp file growth */
	if (fileFlags & PG_TEMP_LIMIT)
		setTempFileLimit(file);

	/* Transient File associated with subtransaction */
	if (fileFlags & PG_TRANSIENT)
		setTransient(file);

	/* Position at end of file if appending */
	fstate->offset = append ? FSize(file) : 0;
	if (fstate->offset == -1)
	{
		FClose(file);
		return -1;
	}

	return file;
}

/*
 * Close a file.
 * Note we do not set errno if the file already has a pending error.
 * This is a convenience for error handling. We want to close
 * the file without obscuring the original error.
 *
 */
bool FClose(File file)
{

	bool success;
	bool previousError;
	IoStack *stack;
	file_debug("name=%s, file=%d  iostack=%p", FilePathName(file), file, getStack(file));

	/*
	 * If a legacy file, close it using the legacy FileClose().
	 * A convenience so we don't have to distinguish between
	 * legacy files and FOpen files when cleaning things up.
	 */
	if (FileIsLegacy(file))
		return (FileClose(file) != 0);

	if (badFile(file))
		return false;

    /* Point to the file's I/O stack */
	stack = getStack(file);

	/* If there was a previous error, save it in dummy slot */
	previousError = FError(file);
	if (previousError)
		stackCopyError(getErrStack(-1), stack);

	/* Close the file.  The low level routine will invalidate the "file" vfd index */
	success = stackClose(stack);

	/* If a new error occurred, copy it to dummy slot */
	if (!success && !previousError)
		stackCopyError(getErrStack(-1), stack);

	/* No matter what, release the I/O stack element after Close */
	free(stack);

	file_debug("(done): file=%d success=%d", file, success);

	/* Restore errno in case it was reset. */
	FErrorCode(-1);
	return success;
}


ssize_t FRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait_info)
{
	ssize_t actual;
	file_debug("FileRead: name=%s file=%d  amount=%zd offset=%lld", FilePathName(file), file, amount, offset);

	if (badFile(file))
		return -1;

	/* Read the data as requested */
	actual = stackReadAll(getStack(file), buffer, amount, offset, wait_info);

	/* If successful, update the file offset */
	updateFileState(file, offset, actual);

	file_debug("FileRead(done): file=%d  name=%s  actual=%zd", file, FilePathName(file), actual);
	return actual;
}


ssize_t FWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait)
{
	ssize_t actual;
	file_debug("name=%s file=%d  amount=%zd offset=%lld", FilePathName(file), file, amount, offset);

	if (badFile(file))
		return -1;

	/*
	 * If we think we are creating a hole, update the file size first.
	 */
	if (offset > getFState(file)->fileSize && FSize(file) < 0)
		return -1;

	/* If creating a hole, extend the file to fill in the hole first. */
	if (offset > getFState(file)->fileSize && !FResize(file, offset, wait))
		return -1;

	/* Write the data as requested */
	actual = stackWriteAll(getStack(file), buffer, amount, offset, wait);

	/* Update the file state */
	updateFileState(file, offset, actual);

	file_debug("(done): file=%d  name=%s  actual=%zd", file, FilePathName(file), actual);
	return actual;
}

/*
 * Flush a file's data to persistent storage.
 * TODO: add offset and amount as parameters to the I/O stack.
 */
bool FSync(File file, uint32 wait)
{
	if (badFile(file))
		return -1;

	return stackSync(getStack(file), wait);
}

/*
 * Get the size of the file.
 */
off_t FSize(File file)
{
	off_t size;

	if (badFile(file))
		return -1;

	/* Query the file's size */
	size = stackSize(getStack(file));
	getFState(file)->fileSize = size;

	file_debug("name=%s file=%d  size=%lld", FilePathName(file), file, size);
	return size;
}

/*
 * Get the block size of the file.
 * All seeks must be to block boundaries,
 * and all I/O must be complete blocks except
 * for the final block which may be partial.
 */
ssize_t FBlockSize(File file)
{
	if (badFile(file))
		return -1;
	return getStack(file)->blockSize;
}


/*
 * Change the size of a file, either by truncating or filling with zeros.
 * Note this is a new function and does not have a legacy version.
 */
bool	FResize(File file, off_t offset, uint32 wait)
{
	if (badFile(file))
		return false;

	return stackResize(getStack(file), offset, wait);
}


/*
 * Error handling code.
 * These functions are similar to ferror(), but can be accessed even when the file is closed or -1.
 * This added feature allows error info to be fetched after a failed "open" or "close" call.
 *
 * TODO: move error info from IoStack to FStat, and let IoStack point to FStat.
 */

/* True if an error occurred on the file.  (EOF is not an error) */
bool FError(File file)
{
	return stackError(getErrStack(file));
}

/* True if the last read generated an EOF */
bool FEof(File file)
{
	return stackEof(getErrStack(file));
}

/* Clears an error, and is true if an error had been encountered */
bool FClearError(File file)
{
	return stackClearError(getErrStack(file));
}

/* Get a pointer to the error message */
const char *FErrorMsg(File file)
{
	return stackErrorMsg(getErrStack(file));
}

/*
 * Get the errno associated the file.
 * As a side effect, restores errno to the value it had when the error occurred.
 */
int FErrorCode(File file)
{
	return stackErrorCode(getErrStack(file));
}


/*
 * Set error information for the current file.
 * For compatibility, sets errno as a side effect.
 */
int setFileError(File file, int errorCode, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	stackVSetError(getErrStack(file), errorCode, fmt, args);
	va_end(args);

	/* Return -1 to indicate an error */
	return -1;
}


/*
 * Points to the I/O stack for the file. NULL if bad.
 */
static inline IoStack *getStack(File file)
{
	FileState *fs = getFState(file);  // TODO: Inline if possible
	if (fs == NULL)
		return NULL;

	return fs->ioStack;
}


/*
 * Checks the file descriptor to see if file is not a valid temp file.
 */
static inline bool badFile(File file)
{
	bool isBad = (getStack(file) == NULL);
	if (isBad)
		errno = EBADF;
	return isBad;
}

/*
 * Points to an I/O stack element for reporting errors.
 * Returns a dummy stack when fd is not a proper
 * temp file.
 */
static inline IoStack *getErrStack(File file)
{
	IoStack *stack;
	static IoStack dummyStack = {0};

	/* If file doesn't have an I/O stack, use the dummy stack. */
	stack = getStack(file);
	if (stack == NULL)
		stack = &dummyStack;

	/* In all cases, return an I/O stack */
	return stack;
}


bool PathNameFSync(const char *path, uint32 wait)
{
	File file;
	bool success;

	file = FOpen(path, PG_RAW | O_RDWR);

	success =
		file >= 0 &&
		FSync(file, wait)  &&
		FClose(file);


	return success;
}


static inline void updateFileState(File file, off_t offset, ssize_t actual)
{
	FileState *fs = getFState(file);
	Assert(fs != NULL);
	if (actual > 0)
	{
		fs->offset = offset + actual;
		fs->fileSize = MAX(fs->fileSize, fs->offset);
	}
}

/**
 * Buffered reconciles a byte stream input with an output of fixed size blocks.
 * Because output blocks are fixed size, it is possible to do random Seeks
 * and Writes to the output file.
 *
 * Buffered replicates the functionality of fread/fwrite/fseek.
 * Seeks and O_APPEND are not compatible with subsequent streaming filters which create
 * variable size blocks. (eg. compression).
 *
 *  One goal is to ensure purely sequential reads/writes do not require Seek operations.
 */
#include <stdlib.h>
#include <sys/fcntl.h>
#include "storage/iostack_internal.h"
#include "storage/fd.h"

#define palloc malloc

/**
 * Structure containing the state of the stream, including its buffer.
 */
 typedef struct Buffered Buffered;

struct Buffered
{
    IoStack ioStack;        /* Common to all filters */
    ssize_t suggestedSize;   /* The suggested buffer size. We may make it a bit bigger */

    Byte *buf;             /* Local buffer, precisely one block in size. */
	size_t bufferSize;        /* the size of the local buffer. */
    bool dirty;            /* Does the buffer contain dirty data? */

    off_t currentBlock;     /* File position of the beginning of the buffer */
    ssize_t currentSize;      /* Nr of actual bytes in the buffer */

    off_t fileSize;        /* Highest byte position we've seen so far for the file. */

    bool readable;        /* Opened for reading */
    bool writeable;       /* Opened for writing */
};


/* Forward references */
static ssize_t copyOut(Buffered *this, Byte *buf, size_t size, off_t position);
static ssize_t copyIn(Buffered *this, const Byte *buf, size_t size, off_t position);
static bool purgeBuffer(Buffered *this);
static bool flushBuffer(Buffered *this);
static bool fillBuffer(Buffered *this);
static ssize_t directWrite(Buffered *this, const Byte *buf, size_t size, off_t offset);
static ssize_t directRead(Buffered *this, Byte *buf, size_t size, off_t offset);
static bool positionToBuffer(Buffered *this, off_t position);
static Buffered *bufferedCleanup(Buffered *this);
static bool bufferedSync(Buffered *this);

/**
 * Open a buffered file, reading, writing or both.
 */
static Buffered *
bufferedOpen(Buffered *proto, const char *path, int oflags, mode_t perm)
{
	IoStack *next;
	Buffered *this;
	file_debug("path=%s oflags=%x perm=%x", path, oflags, perm);

    /* Below us, we need to read/modify/write even if write only. */
    if ( (oflags & O_ACCMODE) == O_WRONLY)
        oflags = (oflags & ~O_ACCMODE) | O_RDWR;

    /* Open the downstream file and clone our prototype */
    next = stackOpen(nextStack(proto), path, oflags, perm);
	if (next == NULL)
		return NULL;

	this = bufferedNew(proto->suggestedSize, next);
	if (this == NULL)
	{
		int save_errno = errno;
		stackClose(next);
		free(next);
		errno = save_errno;
		return NULL;
	}

	/* Extra return parameters */
	this->ioStack.openVal = next->openVal;
	this->ioStack.blockSize = 1;

	/* If an open error occurred, save the error and free the downstream node */
	if (next->openVal < 0)
	    return bufferedCleanup(this);

	/* Are we read/writing or both? */
	this->readable = (oflags & O_ACCMODE) != O_WRONLY;
	this->writeable = (oflags & O_ACCMODE) != O_RDONLY;

    /* Position to the start of file with an empty buffer */
    this->currentBlock = 0;
    this->dirty = false;
    this->currentSize = 0;

    /* Get the size of the file */
    this->fileSize = stackSize(nextStack(this));
	if (this->fileSize < 0)
		return bufferedCleanup(this);

	/* Peek ahead and choose a buffer size which is a multiple of our successor's block size */
	this->bufferSize = ROUNDUP(this->suggestedSize, nextStack(this)->blockSize);
    this->buf = malloc(this->bufferSize);

	/* Close our successor if we couldn't allocate memory */
	if (this->buf == NULL)
	{
		stackSetError(this, errno, "failed to allocate %z bytes", this->bufferSize);
		return bufferedCleanup(this);
	}

	/* Success */
	file_debug("(done) file=%zd(%s)  fileSize=%lld", thisStack(this)->openVal, FilePathName(thisStack(this)->openVal), this->fileSize);
	return this;
}


/**
 * Write data to the buffered file.
 */
static ssize_t
bufferedWrite(Buffered *this, const Byte *buf, size_t size, off_t offset)
{
	ssize_t actual;
    file_debug("bufferedWrite: size=%zd  offset=%lld ", size, offset);
    Assert(size >= 0);

	/* Skip empty writes */
	if (size == 0)
		return 0;

	/* Position to the new block if it changed. */
	if (!positionToBuffer(this, offset))
		return -1;

    /* If buffer is empty, offset is aligned, and the data exceeds block size, write directly to next stage */
    if (this->currentSize == 0 && offset == this->currentBlock && size >= this->bufferSize)
        return directWrite(this, buf, size, offset);

    /* Fill the buffer if it is empty ... */
    if (!fillBuffer(this))
		return -1;

    /* Copy data into the current buffer */
    actual = copyIn(this, buf, size, offset);

	/* Update file size */
	if (actual > 0)
	    this->fileSize = MAX(this->fileSize, offset + actual);

	Assert(this->currentSize == this->bufferSize || this->currentSize == 0 || this->currentBlock + this->currentSize == this->fileSize);

    return actual;
}


/*
 * Optimize writes by going directly to the next file if we don't need buffering.
 */
static ssize_t directWrite(Buffered *this, const Byte *buf, size_t size, off_t offset)
{
	ssize_t alignedSize;
	ssize_t actual;

	file_debug("directWrite: size=%zd offset=%lld", size, offset);

    /* Write out multiple blocks, but no partials */
    alignedSize = ROUNDDOWN(size, this->bufferSize);
    actual = stackWrite(nextStack(this), buf, alignedSize, offset);
	if (actual < 0)
		return copyNextError(this, actual);

	this->fileSize = MAX(this->fileSize, offset + actual);

	Assert(this->currentSize == this->bufferSize || this->currentSize == 0 || this->currentBlock + this->currentSize == this->fileSize);
    return actual;
}

/**
 * Read bytes from the buffered stream.
 * Note it may take multiple reads to get all the data or to reach EOF.
 */
static ssize_t bufferedRead(Buffered *this, Byte *buf, size_t size, off_t offset)
{
	ssize_t actual;

	file_debug("bufferedRead: size=%zd  offset=%lld ", size, offset);
	Assert(size > 0);

	/* Position to the new block if it changed. */
	if (!positionToBuffer(this, offset))
		return -1;

	/* If buffer is empty, offset is aligned, and the data exceeds block size, write directly to next stage */
	if (this->currentSize == 0 && offset == this->currentBlock && size >= this->bufferSize)
		return directRead(this, buf, size, offset);

	/* Fill the buffer if it is empty */
	if (!fillBuffer(this))
		return -1;

	/* Copy data from the current buffer */
	actual = copyOut(this, buf, size, offset);

	this->ioStack.eof = (actual == 0);
	return actual;
}


static ssize_t directRead(Buffered *this, Byte *buf, size_t size, off_t offset)
{
	ssize_t actual;
	ssize_t alignedSize;
	file_debug("directRead: size=%zd offset=%lld", size, offset);

	/* Read multiple blocks, last one might be partial */
	alignedSize = ROUNDDOWN(size, this->bufferSize);
	actual = stackRead(nextStack(this), buf, alignedSize, offset);
	if (actual < 0)
		return copyNextError(this, -1);

	this->ioStack.eof = (actual == 0);

	return actual;
}


/**
 * Seek to a position
 */
static bool positionToBuffer(Buffered *this, off_t position)
{
    /* Do nothing if we are already position at the proper block */
    off_t newBlock = ROUNDDOWN(position, this->bufferSize);
    file_debug("positionToBuffer: position=%lld  newBlock=%lld bufPosition=%lld", position, newBlock, this->currentBlock);
    if (newBlock == this->currentBlock)
	    return true;

	/* flush current block. */
	if (!flushBuffer(this))
		return false;

	/* Update position */
	this->currentBlock = newBlock;
	this->currentSize = 0;

    return true;
}

/**
 * Close the buffered file.
 */
static bool bufferedClose(Buffered *this)
{
	file_debug("file=%zd", this->ioStack.openVal);

    /* Flush our buffers. */
    flushBuffer(this);

	/* Clean things up. Maintains error info if error occurred. */
    bufferedCleanup(this);

	file_debug("(done): msg=%s", this->ioStack.errMsg);

	return !stackError(this);
}


/*
 * Synchronize any written data to persistent storage.
 */
static bool bufferedSync(Buffered *this)
{
    /* Flush our buffers. */
    bool success = flushBuffer(this);

    /* Pass on the sync request, even if flushing fails. */
    success &= stackSync(nextStack(this));

	if (!success)
		copyNextError(this, success);

	return success;
}

/*
 * Truncate the file at the given offset
 */
static bool
bufferedResize(Buffered *this, off_t offset)
{
	file_debug("bufferedResize: offset=%lld file=%zd oldSize=%lld", offset, this->ioStack.openVal, this->fileSize);

	/* Empty our buffer so we don't have any data in it. */
	if (!purgeBuffer(this))
	    return false;

	/* Resize the underlying file */
	if (!stackResize(nextStack(this), offset))
		return copyNextError(this, false);

	/* Track the new file size */
	this->fileSize = offset;

	/* Done. The next read/write will refill the buffer */
	return true;
}

static off_t bufferedSize(Buffered *this)
{
	file_debug("size=%lld", this->fileSize);
	return this->fileSize;
}


IoStackInterface bufferedInterface = (IoStackInterface)
	{
		.fnOpen = (IoStackOpen) bufferedOpen,
		.fnWrite = (IoStackWrite) bufferedWrite,
		.fnClose = (IoStackClose) bufferedClose,
		.fnRead = (IoStackRead) bufferedRead,
		.fnSync = (IoStackSync) bufferedSync,
		.fnResize = (IoStackResize) bufferedResize,
		.fnSize = (IoStackSize) bufferedSize,
	};


/**
 Create a new buffer filter object.
 It converts input bytes to records expected by the next filter in the pipeline.
 */
void *bufferedNew(ssize_t suggestedSize, void *next)
{
    Buffered *this = malloc(sizeof(Buffered));
    *this = (Buffered)
		{
		.suggestedSize = (suggestedSize == 0)? 16*1024: suggestedSize,
		.ioStack = (IoStack)
			{
			.next = next,
			.iface = &bufferedInterface,
			}
		};

    return this;
}


/*
 * Clean buffer and empty it out.
 */
static bool purgeBuffer(Buffered *this)
{
	/* Flush the buffer */
	if (!flushBuffer(this))
		return false;

	/* Reset it to "empty" */
	this->currentSize = 0;
	this->currentBlock = 0;

	return true;
}

/*
 * Clean a dirty buffer by writing it to disk. Does not change the contents of the buffer.
 */
static bool flushBuffer(Buffered *this)
{
    file_debug("flushBuffer: bufPosition=%lld  bufActual=%zd  dirty=%d", this->currentBlock, this->currentSize, this->dirty);
	Assert(this->currentBlock % this->bufferSize == 0);

    /* Clean the buffer if dirty */
	if (this->dirty)
	{
		if (stackWriteAll(nextStack(this), this->buf, this->currentSize, this->currentBlock) < 0)
			return copyNextError(this, false);

		this->dirty = false;
	}

    return true;
}

/*
 * Read in a new buffer of data for the current position
 */
static bool fillBuffer(Buffered *this)
{
	file_debug("fillBuffer: bufActual=%zd  bufPosition=%lld  fileSize=%lld",
		  this->currentSize, this->currentBlock, this->fileSize);
	Assert(this->currentBlock % this->bufferSize == 0);

	/* Don't fill in if it is already filled in */
	if (this->currentSize > 0)
		return true;

	/* Read in the current buffer */
	this->currentSize = stackReadAll(nextStack(this), this->buf, this->bufferSize, this->currentBlock);
	if (this->currentSize < 0)
		return copyNextError(this, false);

	Assert(this->currentSize == this->bufferSize || this->currentBlock + this->currentSize == this->fileSize);

	return true;
}

/* Copy user data from the user, respecting boundaries */
static ssize_t copyIn(Buffered *this, const Byte *buf, size_t size, off_t position)
{
	off_t offset;
	ssize_t actual;

	file_debug("copyIn: position=%lld  size=%zd bufPosition=%lld bufActual=%zd", position, size, this->currentBlock, this->currentSize);
	Assert(this->currentBlock == ROUNDDOWN(position, this->bufferSize));

	if (this->currentSize == -1)
		return -1;

	Assert(this->currentSize == this->bufferSize || this->currentBlock + this->currentSize == this->fileSize);

    /* Check to see if we are creating holes. */
	if (position > this->currentBlock + this->currentSize)
	    return stackSetError(this, -1, "Buffered I/O stack would create a hole");

	/* Copy bytes into the buffer, up to end of data or end of buffer */
	offset = position - this->currentBlock;
    actual = MIN(this->bufferSize - offset, size);
    memcpy(this->buf + offset, buf, actual);
	this->dirty = true;

    /* We may have extended the total data held in the buffer */
    this->currentSize = MAX(this->currentSize, actual + offset);
	file_debug("copyin(end): actual=%zd  bufActual=%zd", actual, this->currentSize);

    Assert(this->currentSize <= this->bufferSize);
    return actual;
}


/* Copy data to the user, respecting boundaries */
static ssize_t copyOut(Buffered *this, Byte *buf, size_t size, off_t position)
{
    ssize_t actual;
	off_t   offset;

	if (this->currentSize == -1)
		return -1;

	/* Check to see if we are skipping over holes */
	offset = position - this->currentBlock;
	if (offset > this->currentSize)
	    return stackSetError(this, -1, "Buffered I/O stack hole");

	/* Copy bytes out of the buffer, up to end of data or end of buffer */
    actual = MIN(this->currentSize - offset, size);
    memcpy(buf, this->buf + offset, actual);
    file_debug("copyOut: size=%zd bufPosition=%lld bufActual=%zd offset=%lld  actual=%zd",
		  size, this->currentBlock, this->currentSize, offset, actual);
    return actual;
}


/*
 * Clean up when no longer needed. Does not free the top element.
 */
static Buffered *bufferedCleanup(Buffered *this)
{
	IoStack *next = this->ioStack.next;

	/* Close the next layer if opened */
	if (next != NULL && next->openVal >= 0)
		stackClose(next);
	this->ioStack.openVal = -1;

	/* If we have no errors, then use error info from successor */
	if (!stackError(this) && next != NULL && stackError(next))
		copyNextError(this, -1);

	/* Free the next layer if allocated */
	if (next != NULL)
		free(next);
	this->ioStack.next = NULL;

	/* Free the buffer if allocated */
	if (this->buf != NULL)
		free(this->buf);
	this->buf = NULL;

	return this;
}

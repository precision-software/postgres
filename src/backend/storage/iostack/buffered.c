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
#include <assert.h>
#include "storage/iostack_internal.h"

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
	size_t blockSize;        /* the size of the local buffer. */
    bool dirty;            /* Does the buffer contain dirty data? */

    off_t currentBlock;     /* File position of the beginning of the buffer */
    ssize_t currentSize;      /* Nr of actual bytes in the buffer */

    off_t fileSize;        /* Highest byte position we've seen so far for the file. */
    bool sizeConfirmed;    /* fileSize is confirmed as actual file size. */

    bool readable;        /* Opened for reading */
    bool writeable;       /* Opened for writing */
};


/* Forward references */
static ssize_t copyOut(Buffered *this, Byte *buf, size_t size, off_t position);
static ssize_t copyIn(Buffered *this, const Byte *buf, size_t size, off_t position);
static bool flushBuffer(Buffered *this);
static bool fillBuffer(Buffered *this);
static ssize_t directWrite(Buffered *this, const Byte *buf, size_t size, off_t offset);
static ssize_t directRead(Buffered *this, Byte *buf, size_t size, off_t offset);
static bool positionToBuffer(Buffered *this, off_t position);
static Buffered *bufferedCleanup(Buffered *this);

/**
 * Open a buffered file, reading, writing or both.
 */
static Buffered *bufferedOpen(Buffered *proto, const char *path, int oflags, int perm)
{
    /* Below us, we need to read/modify/write even if write only. */
    if ( (oflags & O_ACCMODE) == O_WRONLY)
        oflags = (oflags & ~O_ACCMODE) | O_RDWR;

    /* Open the downstream file and clone our prototype */
    IoStack *next = stackOpen(nextStack(proto), path, oflags, perm);
	Buffered *this = bufferedNew(proto->suggestedSize, next);

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

    /* We don't know the size of the file yet. */
    this->fileSize = 0;
	this->sizeConfirmed = (oflags & O_TRUNC) != 0;

	/* Peek ahead and choose a buffer size which is a multiple of our successor's block size */
	this->blockSize = ROUNDUP(this->suggestedSize, nextStack(this)->blockSize);
    this->buf = malloc(this->blockSize);

	/* Close our successor if we couldn't allocate memory */
	if (this->buf == NULL)
	{
		stackCheckError(this, -1, "bufferedOpen failed to allocate %z bytes", this->blockSize);
		return bufferedCleanup(this);
	}

	/* Success */
	return this;
}


/**
 * Write data to the buffered file.
 */
static ssize_t bufferedWrite(Buffered *this, const Byte *buf, size_t size, off_t offset)
{
    file_debug("bufferedWrite: size=%zd  offset=%lld \n", size, offset);
    assert(size > 0);

	/* Position to the new block if it changed. */
	if (!positionToBuffer(this, offset))
		return -1;

    /* If buffer is empty, offset is aligned, and the data exceeds block size, write directly to next stage */
    if (this->currentSize == 0 && offset == this->currentBlock && size >= this->blockSize)
        return directWrite(this, buf, size, offset);

    /* Fill the buffer if it is empty ... */
    if (!fillBuffer(this))
		return -1;

    /* Copy data into the current buffer */
    ssize_t actual = copyIn(this, buf, size, offset);

	/* Update file size */
	if (actual > 0)
	    this->fileSize = MAX(this->fileSize, offset + actual);

    assert(actual > 0);
    return actual;
}


/*
 * Optimize writes by going directly to the next file if we don't need buffering.
 */
static ssize_t directWrite(Buffered *this, const Byte *buf, size_t size, off_t offset)
{
	file_debug("directWrite: size=%zd offset=%lld\n", size, offset);

    /* Write out multiple blocks, but no partials */
    ssize_t alignedSize = ROUNDDOWN(size, this->blockSize);
    ssize_t actual = stackWrite(nextStack(this), buf, alignedSize, offset);
	if (actual < 0)
		return copyNextError(this, actual);

	this->fileSize = MAX(this->fileSize, offset + actual);

    return actual;
}

/**
 * Read bytes from the buffered stream.
 * Note it may take multiple reads to get all the data or to reach EOF.
 */
static ssize_t bufferedRead(Buffered *this, Byte *buf, size_t size, off_t offset)
{
	file_debug("bufferedRead: size=%zd  offset=%lld \n", size, offset);
	assert(size > 0);

	/* Position to the new block if it changed. */
	if (!positionToBuffer(this, offset))
		return -1;

	/* If buffer is empty, offset is aligned, and the data exceeds block size, write directly to next stage */
	if (this->currentSize == 0 && offset == this->currentBlock && size >= this->blockSize)
		return directRead(this, buf, size, offset);

	/* Fill the buffer if it is empty */
	if (!fillBuffer(this))
		return -1;

	/* Copy data from the current buffer */
	ssize_t actual = copyOut(this, buf, size, offset);

	this->ioStack.eof = (actual == 0);
	return actual;
}


static ssize_t directRead(Buffered *this, Byte *buf, size_t size, off_t offset)
{
	file_debug("directRead: size=%zd offset=%lld\n", size, offset);
	/* Read multiple blocks, last one might be partial */
	ssize_t alignedSize = ROUNDDOWN(size, this->blockSize);
	ssize_t actual = stackRead(nextStack(this), buf, alignedSize, offset);
	if (actual < 0)
		return copyNextError(this, -1);

	/* update fileSize */
	this->fileSize = MAX(this->fileSize, offset + actual);
	this->ioStack.eof = (actual == 0);

	return actual;
}


/**
 * Seek to a position
 */
static bool positionToBuffer(Buffered *this, off_t position)
{
    /* Do nothing if we are already position at the proper block */
    off_t newBlock = ROUNDDOWN(position, this->blockSize);
    file_debug("positionToBuffer: position=%lld  newBlock=%lld bufPosition=%lld\n", position, newBlock, this->currentBlock);
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
static ssize_t bufferedClose(Buffered *this)
{
	file_debug("bufferedClose: file=%zd\n", this->ioStack.openVal);
    /* Flush our buffers. */
    flushBuffer(this);

	/* Clean things up */
    bufferedCleanup(this);

	file_debug("bufferedClose(done): msg=%s\n", this->ioStack.errMsg);
	return stackError(this)? -1: 0;
}


/*
 * Synchronize any written data to persistent storage.
 */
static ssize_t bufferedSync(Buffered *this)
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
static ssize_t bufferedTruncate(Buffered *this, off_t offset, uint32 wait_event)
{
	file_debug("bufferedTruncate: offset=%lld file=%zd\n", offset, this->ioStack.openVal);
	/* Position our buffer with the given position */
	if (!positionToBuffer(this, offset))
	    return false;

	/* Truncate the underlying file */
	if (stackTruncate(nextStack(this), offset) < 0)
		return copyNextError(this, -1);

	/* Update our buffer so it ends at that position */
	if (this->currentSize > 0)
	    this->currentSize = offset - this->currentBlock;
	this->fileSize = offset;
	this->sizeConfirmed = true;

	if (this->currentSize == 0)
		this->dirty = false;

	return 0;
}

static off_t bufferedSize(Buffered *this)
{
	file_debug("bufferedSize: confirmed=%d  size=%lld\n", this->sizeConfirmed, this->fileSize);
	if (this->sizeConfirmed)
		return this->fileSize;

	if (!flushBuffer(this))
		return -1;

	off_t size = stackSize(nextStack(this));
	if (size < 0)
		return copyNextError(this, size);

	file_debug("bufferedSize(done): size=%lld\n", size);
	return size;
}


IoStackInterface bufferedInterface = (IoStackInterface)
	{
		.fnOpen = (IoStackOpen) bufferedOpen,
		.fnWrite = (IoStackWrite) bufferedWrite,
		.fnClose = (IoStackClose) bufferedClose,
		.fnRead = (IoStackRead) bufferedRead,
		.fnSync = (IoStackSync) bufferedSync,
		.fnTruncate = (IoStackTruncate) bufferedTruncate,
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
 * Clean a dirty buffer by writing it to disk. Does not change the contents of the buffer.
 */
static bool flushBuffer(Buffered *this)
{
    file_debug("flushBuffer: bufPosition=%lld  bufActual=%zd  dirty=%d\n", this->currentBlock, this->currentSize, this->dirty);
	Assert(this->currentBlock % this->blockSize == 0);

    /* Clean the buffer if dirty */
	if (this->dirty)
	{
		if (stackWriteAll(nextStack(this), this->buf, this->currentSize, this->currentBlock) < 0)
			return copyNextError(this, false);

		/* Update file size */
		this->fileSize = MAX(this->fileSize, this->currentBlock + this->currentSize);
		this->dirty = false;
	}

    return true;
}

/*
 * Read in a new buffer of data for the current position
 */
static bool fillBuffer (Buffered *this)
{
	file_debug("fillBuffer: bufActual=%zd  bufPosition=%lld sizeConfirmed=%d  fileSize=%lld\n",
		  this->currentSize, this->currentBlock, this->sizeConfirmed, this->fileSize);
	assert(this->currentBlock % this->blockSize == 0);

	/* Don't fill in if it is already filled in */
	if (this->currentSize > 0)
		return true;

	/* Quick check for EOF (without system calls) */
	if (this->sizeConfirmed && this->currentBlock == this->fileSize)
	{
		this->currentSize = 0;
		this->ioStack.eof = true;
		return true;
	}

	/* Check for holes */
	if (this->sizeConfirmed && this->currentBlock > this->fileSize)
		return stackSetError(this, -1, "buffereedStack: creating holes (offset=%lld, fileSize=%lld", this->currentBlock, this->fileSize);

	/* Read in the current buffer */
	this->currentSize = stackReadAll(nextStack(this), this->buf, this->blockSize, this->currentBlock);
	if (this->currentSize < 0)
		return copyNextError(this, false);

	/* if EOF or partial read, update the known file size */
	this->sizeConfirmed |= (this->currentSize < this->blockSize);
	this->fileSize = MAX(this->fileSize, this->currentBlock + this->currentSize);

	return true;
}

/* Copy user data from the user, respecting boundaries */
static ssize_t copyIn(Buffered *this, const Byte *buf, size_t size, off_t position)
{
	file_debug("copyIn: position=%lld  size=%zd bufPosition=%lld bufActual=%zd\n", position, size, this->currentBlock, this->currentSize);
	assert(this->currentBlock == ROUNDDOWN(position, this->blockSize));

	if (this->currentSize == -1)
		return -1;

    /* Check to see if we are creating holes. */
	if (position > this->currentBlock + this->currentSize)
	    return stackSetError(this, -1, "Buffered I/O stack would create a hole");

	/* Copy bytes into the buffer, up to end of data or end of buffer */
	off_t offset = position - this->currentBlock;
    ssize_t actual = MIN(this->blockSize - offset, size);
    memcpy(this->buf + offset, buf, actual);
	this->dirty = true;

    /* We may have extended the total data held in the buffer */
    this->currentSize = MAX(this->currentSize, actual + offset);
	file_debug("copyin(end): actual=%zd  bufActual=%zd\n", actual, this->currentSize);

    assert(this->currentSize <= this->blockSize);
    return actual;
}


/* Copy data to the user, respecting boundaries */
static ssize_t copyOut(Buffered *this, Byte *buf, size_t size, off_t position)
{

	if (this->currentSize == -1)
		return -1;

	/* Check to see if we are skipping over holes */
	size_t offset = position - this->currentBlock;
	if (offset > this->currentSize)
	    return stackSetError(this, -1, "Buffered I/O stack hole");

	/* Copy bytes out of the buffer, up to end of data or end of buffer */
    ssize_t actual = MIN(this->currentSize - offset, size);
    memcpy(buf, this->buf + offset, actual);
    file_debug("copyOut: size=%zd bufPosition=%lld bufActual=%zd offset=%zd  actual=%zd\n",
		  size, this->currentBlock, this->currentSize, offset, actual);
    return actual;
}


/*
 * Clean up when no longer needed.
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
	if (next!= NULL)
		free(next);
	this->ioStack.next = NULL;

	/* Free the buffer if allocated */
	if (this->buf != NULL)
		free(this->buf);
	this->buf = NULL;

	return this;
}

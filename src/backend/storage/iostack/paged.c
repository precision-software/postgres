/**
 * Paged reconciles a byte stream input with an output of buffer cache style pages.
 * Because pages are fixed size, it is possible to do random Seeks
 * and Writes to the output file.
 */
#include <stdlib.h>
#include <sys/fcntl.h>
#include <assert.h>
#include "postgres.h"
#include "storage/bufpage.h"
#include "storage/iostack_internal.h"

#define palloc malloc

/**
 * Structure containing the state of the stream, including its page.
 */
typedef struct Paged Paged;
struct Paged
{
    IoStack ioStack;        /* Common to all filters */
    ssize_t pageSize;       /* The expected page size. We only read and write full pages */

	Page page;             /* Pointer to current page data  */
    bool dirty;            /* Does the page contain dirty data? */

    off_t currentBlock;     /* Content offset corresponding to the beginning of the current page */
	ssize_t currentSize;    /* Size of content of the current page */

	/* These can be replaced by PageXXXContents(this->page) ???? */
	Byte *buf;             /* The contents portion of the page */
	size_t bufSize;        /* The maximum size of the contents of the page */

    off_t fileSize;        /* Highest byte position we've seen so far for the file. */

    bool readable;        /* Opened for reading */
    bool writeable;       /* Opened for writing */
};

/* Forward references */
static ssize_t copyOut(Paged *this, Byte *buf, size_t size, off_t position);
static ssize_t copyIn(Paged *this, const Byte *buf, size_t size, off_t position);
static bool flushPage(Paged *this);
static bool fillPage(Paged *this);
static bool positionToPage(Paged *this, off_t position);
static Paged *pagedCleanup(Paged *this);

/**
 * Open a Paged file, reading, writing or both.
 */
static Paged *pagedOpen(Paged *proto, const char *path, int oflags, int perm)
{
    off_t nextFileSize, lastPage;
    IoStack *next;
    Paged *this;

	debug("pagedOpen: path=%s  oflags=0x%x\n", path, oflags);
    /* Below us, we need to read/modify/write even if write only. */
    if ( (oflags & O_ACCMODE) == O_WRONLY)
        oflags = (oflags & ~O_ACCMODE) | O_RDWR;

    /* Open the downstream file and clone our prototype */
    next = stackOpen(nextStack(proto), path, oflags, perm);
	this = pagedNew(proto->pageSize, next);  /* TODO: who sets and checks pageSize? */

	/* Extra return parameters */
	this->ioStack.openVal = next->openVal;

	/* If an open error occurred, save the error and free the downstream node */
	if (next->openVal < 0)
	    return pagedCleanup(this);

	/* Are we read/writing or both? */
	this->readable = (oflags & O_ACCMODE) != O_WRONLY;
	this->writeable = (oflags & O_ACCMODE) != O_RDONLY;

	/* Allocate a new page for our data */
    this->page = malloc(this->pageSize);  /* TODO: new macro coming */

	/* Close our successor if we couldn't allocate memory */
	if (this->page == NULL)
	{
		checkSystemError(this, -1, "pagedOpen failed to allocate %zd bytes", (ssize_t)BLCKSZ);
		return pagedCleanup(this);
	}

	/* Initialize the page */
	PageInit(this->page, this->pageSize, 0); /* TODO: features being passed */

	/* Set up some handy values */
	this->buf = (Byte *)PageGetContents(this->page);
	this->bufSize = PageGetMaxContentSize(this->page);

	/* Get the content size, pretending the last page is full. */
	nextFileSize = stackSize(nextStack(this));
	if (nextFileSize == -1)
		return pagedCleanup(this);
	Assert(nextFileSize % PageGetPageSize(this->page) == 0);
	this->fileSize = nextFileSize / this->pageSize * this->bufSize;

	/* Read the last page of the file into our internal page buffer */
	lastPage = (this->fileSize == 0) ? 0 : (this->fileSize - this->bufSize);
	this->currentBlock = lastPage;
	this->currentSize = 0;
	if (!fillPage(this))
		return pagedCleanup(this);

	/* Ignore EOF if the file was empty */
	if (nextFileSize == 0)
	     stackClearError(this);

	/* Adjust content size of the file, allowing for a partial last page */
	this->fileSize = this->currentBlock + this->currentSize;

	/* Success */
	debug("pagedOpen: fileSize=%lld  bufSize=%zd nextFileSize=%lld\n", this->fileSize, this->bufSize, nextFileSize);
	return this;
}


/**
 * Write data to the paged file.
 */
static ssize_t pagedWrite(Paged *this, const Byte *buf, size_t size, off_t offset)
{
    ssize_t actual;
    debug("pagedWrite: size=%zd  offset=%lld fileSize=%lld\n", size, offset, this->fileSize);
    assert(size > 0);

	/* Position to the new block if it changed. */
	if (!positionToPage(this, offset))
		return -1;

    /* Fill the page if it is empty ... */
    if (!fillPage(this))
		return -1;

    /* Copy data into the current page */
    actual = copyIn(this, buf, size, offset);

	/* Update file size */
	if (actual > 0)
	    this->fileSize = MAX(this->fileSize, offset + actual);

    assert(actual > 0);
    return actual;
}

/**
 * Read bytes from the paged stream.
 * Note it may take multiple reads to get all the data or to reach EOF.
 */
static ssize_t pagedRead(Paged *this, Byte *buf, size_t size, off_t offset)
{
    ssize_t actual;
	debug("pagedRead: size=%zd  offset=%lld fileSize=%lld\n", size, offset, this->fileSize);
	assert(size > 0);

	/* Position to the new page if it changed. */
	if (!positionToPage(this, offset))
		return -1;

	/* Fill the page if it is empty */
	if (!fillPage(this))
		return -1;

	/* Copy data from the current page */
	actual = copyOut(this, buf, size, offset);

	this->ioStack.eof = (actual == 0);
	return actual;
}

/**
 * Close the paged file.
 */
static ssize_t pagedClose(Paged *this)
{
	debug("pagedClose: file=%zd fileSize=%lld\n", this->ioStack.openVal, this->fileSize);
    /* Flush our pages. */
    flushPage(this);

	/* Clean things up */
    pagedCleanup(this);

	debug("pagedClose(done): msg=%s\n", this->ioStack.errMsg);
	return stackError(this)? -1: 0;
}


/*
 * Synchronize any written data to persistent storage.
 */
static ssize_t pagedSync(Paged *this)
{
    /* Flush our pages. */
    bool success = flushPage(this);

    /* Pass on the sync request, even if flushing fails. */
    success &= stackSync(nextStack(this));

	if (!success)
		copyNextError(this, success);

	return success;
}

/*
 * Truncate the file at the given offset
 */
static ssize_t pagedTruncate(Paged *this, off_t offset)
{
	debug("pagedTruncate: offset=%lld file=%zd\n", offset, this->ioStack.openVal);
	/* Position our page with the given position */
	if (!positionToPage(this, offset))
	    return false;

	/* Truncate the underlying file */
	if (stackTruncate(nextStack(this), offset) < 0)
		return copyNextError(this, -1);

	/* Update our page so it ends at that position */
	if (this->currentSize > 0)
	    this->currentSize = offset - this->currentBlock;
	this->fileSize = offset;

	if (this->currentSize == 0)
		this->dirty = false;

	return 0;
}

static off_t pagedSize(Paged *this)
{
	debug("pagedSize: size=%lld\n", this->fileSize);
	return this->fileSize;
}


IoStackInterface pagedInterface = (IoStackInterface)
	{
		.fnOpen = (IoStackOpen) pagedOpen,
		.fnWrite = (IoStackWrite) pagedWrite,
		.fnClose = (IoStackClose) pagedClose,
		.fnRead = (IoStackRead) pagedRead,
		.fnSync = (IoStackSync) pagedSync,
		.fnTruncate = (IoStackTruncate) pagedTruncate,
		.fnSize = (IoStackSize) pagedSize,
	};


/**
 Create a new page filter object.
 It converts input bytes to records expected by the next filter in the pipeline.
 */
void *pagedNew(ssize_t pageSize, void *next)
{
    Paged *this;
	ssize_t nextSize = thisStack(next)->blockSize;
	if (pageSize == 0)
		pageSize = nextSize;
	/* TODO: Assert the page is big enough */
	debug("pagedNew: pageSize=%zd  nextSize=%zd\n", pageSize, nextSize);

    this = malloc(sizeof(Paged));
    *this = (Paged)
		{
		.pageSize =  pageSize,
		.ioStack = (IoStack)
			{
			.next = next,
			.iface = &pagedInterface,
			.blockSize = 1,
			}
		};

    return this;
}



/**
 * Seek to a position
 */
static bool positionToPage(Paged *this, off_t position)
{
	/* Do nothing if we are already position at the proper block */
	off_t newBlock = ROUNDDOWN(position, this->bufSize);
	debug("positionToPage: position=%lld  newBlock=%lld bufPosition=%lld\n", position, newBlock, this->currentBlock);
	if (newBlock == this->currentBlock)
		return true;

	/* flush current block. */
	if (!flushPage(this))
		return false;

	/* Update position */
	this->currentBlock = newBlock;
	this->currentSize = 0;

	return true;
}

/*
 * Clean a dirty page by writing it to disk. Does not change the contents of the page, other than clearing the dirty bit.
 */
static bool flushPage(Paged *this)
{
    debug("flushPage: bufPosition=%lld  bufActual=%zd  dirty=%d\n", this->currentBlock, this->currentSize, this->dirty);
	Assert(this->currentBlock % this->bufSize == 0);

    /* Clean the page if dirty */
	if (this->dirty)
	{

		/* Calcualate the full page offset of the current page */
		off_t pageOffset = this->currentBlock / this->bufSize * this->pageSize;

		if (stackWriteAll(nextStack(this), (Byte *)this->page, this->pageSize, pageOffset) < 0)
			return copyNextError(this, false);

		/* Update file's content size */
		this->fileSize = MAX(this->fileSize, this->currentBlock + this->currentSize);
		this->dirty = false;
	}

    return true;
}

/*
 * Read in a new page of data for the current position
 */
static bool fillPage (Paged *this)
{
    off_t currentPage;

	debug("fillPage: bufActual=%zd  bufPosition=%lld  fileSize=%lld\n",
		  this->currentSize, this->currentBlock, this->fileSize);
	assert(this->currentBlock % this->bufSize == 0);

	/* Don't fill in if it is already filled in */
	if (this->currentSize > 0)
		return true;

	/* Quick check for EOF (without system calls) */
	if (this->currentBlock == this->fileSize)
	{
		PageInit(this->page, this->pageSize, 0);
		this->currentSize = 0;
		this->ioStack.eof = true;
		return true;
	}

	/* Check for holes */
	if (this->currentBlock > this->fileSize)
		return setIoStackError(this, -1, "pageeedStack: creating holes (offset=%lld, fileSize=%lld",
                               (long long)this->currentBlock, (long long)this->fileSize);

	/* Read in the current page */
	currentPage = this->currentBlock / this->bufSize * this->pageSize;
	if (stackReadAll(nextStack(this), (Byte *)this->page, this->pageSize, currentPage) < 0)
		return copyNextError(this, false);

	/* Make note of the current page size */
	this->currentSize = PageGetContentSize(this->page);
	debug("fillPage: currentSize=%lld\n", this->currentSize);

	return true;
}

/* Copy user data from the user, respecting boundaries */
static ssize_t copyIn(Paged *this, const Byte *buf, size_t size, off_t position)
{
    off_t offset;
    ssize_t actual;

	debug("copyIn: position=%lld  size=%zd bufPosition=%lld bufActual=%zd\n", position, size, this->currentBlock, this->currentSize);
	assert(this->currentBlock == ROUNDDOWN(position, this->bufSize));

	if (this->currentSize == -1)
		return -1;

    /* Check to see if we are creating holes. */
	if (position > this->currentBlock + this->currentSize)
	    return setIoStackError(this, -1, "Paged I/O stack would create a hole");

	/* Copy bytes into the page, up to end of data or end of page */
	offset = position - this->currentBlock;
    actual = MIN(this->bufSize - offset, size);
    memcpy(this->buf + offset, buf, actual);
	this->dirty = true;

    /* We may have extended the total data held in the page */
	if (actual + offset > this->currentSize)
	{
		this->currentSize = actual + offset;
		PageSetContentSize(this->page, actual + offset);
	}

	debug("copyin(end): actual=%zd  bufActual=%zd\n", actual, this->currentSize);
    assert(this->currentSize <= this->bufSize);
    return actual;
}


/* Copy data to the user, respecting boundaries */
static ssize_t copyOut(Paged *this, Byte *buf, size_t size, off_t position)
{
    size_t offset, actual;

	if (this->currentSize == -1)
		return -1;

	/* Check to see if we are skipping over holes */
	offset = position - this->currentBlock;
	if (offset > this->currentSize)
	    return setIoStackError(this, -1, "Paged I/O stack hole");

	/* Copy bytes out of the page, up to end of data or end of page */
    actual = MIN(this->currentSize - offset, size);
    memcpy(buf, this->buf + offset, actual);
    debug("copyOut: size=%zd bufPosition=%lld bufActual=%zd offset=%zd  actual=%zd\n",
		  size, this->currentBlock, this->currentSize, offset, actual);
    return actual;
}


/*
 * Clean up when no longer needed.
 */
static Paged *pagedCleanup(Paged *this)
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

	/* Free the page if allocated */
	if (this->page != NULL)
		free(this->page);
	this->buf = NULL;

	return this;
}

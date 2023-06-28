/*
 * lz4 compression/decompression.
 * Implements a streaming interface for reading/writing compressed files,
 * along with a simple index for doing random access reads. Note it
 * does NOT support random access writes.
 *
 * Problems to solve:
 *    - when freeing, need to free the index file.
 *    - add a header to the encrypted file (algorithm, block size)
 *    - make random access optional, but still support O_APPEND.
 *    - configure blockSize as needed by prev filter
 */
//#define DEBUG
#include <stdlib.h>
#include <lz4.h>
#include <sys/fcntl.h>
#include "storage/iostack_internal.h"
#include "utils/wait_event.h"

typedef struct Lz4Compress Lz4Compress;


/* Forward references */
static ssize_t setLz4Error(Lz4Compress *this, ssize_t retval);
ssize_t lz4DecompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize);
ssize_t lz4CompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize);
size_t maxCompressedSize(size_t size);
static Lz4Compress *lz4Cleanup(Lz4Compress *this);

/* Structure holding the state of our compression/decompression filter. */
struct Lz4Compress
{
    IoStack ioStack;

    size_t defaultBlockSize;          /* Configured default size of uncompressed block. */

    size_t maxCompressed;             /* upper limit on compressed block size */
	size_t blockSize;                 /* Copy of thisStack(this)->blockSize to make code prettier */
    Byte *compressedBuf;              /* Buffer to hold compressed data */
	Byte *tempBuf;                    /* temporary buffer to hold decompressed data when probing for size */

    IoStack *indexFile;               /* Index file created "on the fly" to support block seeks. */
	off_t indexStarts;                /* File offset where the index starts in the index file */

    off_t  lastBlock;				  /* The offset of the last (non-compressed) block */
	size_t lastSize;                  /* The uncompressed size of the last block */
	off_t  compressedLastBlock;       /* The offset of the last block in the compressed file */
	size_t compressedLastSize;        /* the compressed size of the last block, including 4 byte size */

	bool writable;                    /* The file can be written to */
	char indexPath[MAXPGPATH];

};

/*
 * Given a file offset, find the equivalent offset in the compressed file.
 * Note the offsets must be block aligned.
 */
static
off_t getCompressedOffset(Lz4Compress *this, off_t offset)
{
	uint64_t compressedOffset;
	size_t blockNr = offset / this->blockSize;

	Assert(offset % this->blockSize == 0);

	/* Case: offset is end of file, compressed offset is at end of compressed file. */
	if (offset == this->lastBlock + this->lastSize)
		compressedOffset = this->compressedLastBlock + this->compressedLastSize;

	/* Case: offset is within current index, read it from the index. */
	else if (offset <= this->lastBlock)
	{
		if (!stackReadInt64(this->indexFile, &compressedOffset, this->indexStarts + blockNr * 8))
			compressedOffset = setIoStackError(this, -1, "Lz4: offset (%lld) beyond last block (%lld)\n",
                                               (long long)offset, (long long)this->lastBlock);
	}

	/* Otherwise, out of bounds */
	else
		compressedOffset = setIoStackError(this, -1, "lz4: Requested offset (%lld), but fileSize is (%lld)\n", (
                long long)offset, (long long)this->lastBlock+this->lastSize);

	debug("getCompressedOffset: offset=%lld  compressed=%lld\n", offset, compressedOffset);
	return (off_t)compressedOffset;
}

/*
 * Copy a slice of a file into another file.
 *
 *   @param src - the source I/O stack
 *   @param srcOffset - starting offset within the source file.
 *   @param size - the number of bytes to copy.
 *   @param dest - the destination I/O stack.
 *   @param destOffset - the starting offset in the destination file.
 *   @returns - true if all the bytes were copied
 */
static
bool fileCopySlice(IoStack *src, off_t srcOffset, size_t size, IoStack *dest, off_t destOffset)
{
    size_t bufSize;
    Byte *buf;
    size_t total, actual;
	debug("fileCopySlice: srcOffset=%lld  destOffsset=%lld size=%zd\n", srcOffset, destOffset, size);

	/* Pick a compatible buffer size for the copy. */
	Assert(src->blockSize % dest->blockSize == 0 || dest->blockSize % src->blockSize == 0);
	bufSize = MAX(src->blockSize, dest->blockSize);  /* Bigger than src or dest block sizes */
	bufSize = ROUNDUP(64*1024, bufSize);                    /* Big enough for efficient copies */
	bufSize = MAX(size, bufSize);                           /* No point in being bigger than the total copy */
	buf = malloc(bufSize);

	stackClearError(src);
	stackClearError(dest);

	/* Repeat until the slice is copied or end of file */
	for (total = 0; total < size; total += actual)
	{
		ssize_t desired = MIN(bufSize, size - total);

		/* Read some data. Exit if error or EOF */
		actual = stackReadAll(src, buf + total, desired, srcOffset + total);
		if (actual <= 0)
			break;

		/* Write the data. Exit if error */
		actual = stackWriteAll(dest, buf, actual, destOffset + total);
		if (actual < 0)
			break;
	}

	/* Free the buffer we allocated earlier */
	free(buf);

	/* Check for early EOF */
	if (total < size && actual == 0)
		setIoStackError(src, false, "Unexpected EOF copying compression index\n");

	return (total == size);
}

/*
 * Open a compressed file and its index.
 */
static
Lz4Compress *lz4CompressOpen(Lz4Compress *proto, const char *path, int oflags, int mode)
{
    off_t rawSize, compressedSize, fileSize, indexSize;
    IoStack *next;
    Lz4Compress *this;
    debug("lz4Open: path=%s  oflags=0x%x\n", path, oflags);

    /* Open the compressed file and clone ourself */
	next = stackOpen(nextStack(proto), path, oflags, mode);
	this = lz4CompressNew(proto->defaultBlockSize, NULL, next);
	this->ioStack.openVal = next->openVal;

	/* Done if can't open */
	if (next->openVal < 0)
		return lz4Cleanup(this);

	/* Get the total compressed file size, including index */
	rawSize = stackSize(next);
	if (rawSize < 0)
		return lz4Cleanup(this);

	/* Start by assuming a new or empty file */
	fileSize = 0;
	compressedSize = 0;
	indexSize = 0;

	/* If the file has data, then read the compressed and uncompressed file sizes at the end of the compressed file */
	if (rawSize > 0)
	{
		if (!stackReadInt64(next, &compressedSize, rawSize - 16))
			return lz4Cleanup(this);
		if (!stackReadInt64(next, &fileSize, rawSize - 8))
			return lz4Cleanup(this);
		indexSize = rawSize - 16 - compressedSize;
	}
	debug("lz4Open: fileSize=%lld  compressedSize=%lld  indexSize=%lld  rawSize=%lld\n", fileSize, compressedSize, indexSize, rawSize);

	/* If writable, ... */
	this->writable = (oflags & O_ACCMODE) != O_RDONLY;
	if (this->writable)
	{
		/* Get the index file name by adding ".idx" to the file name. */
		strlcpy(this->indexPath, path, sizeof(this->indexPath));
		strlcat(this->indexPath, ".idx", sizeof(this->indexPath));

		/*
		 * Be a bit liberal creating the index file.
		 * Truncate it if exists, and we need to read it as well as write it.
		 */
		oflags = O_RDWR | O_TRUNC | O_CREAT;

		/* Create a separate index file */
		this->indexFile = stackOpen(proto->indexFile, this->indexPath, oflags, mode);
		if (this->indexFile->openVal < 0)
			return lz4Cleanup(this);
		this->indexStarts = 0;

		/* Copy the index data to the index file. */
		if (!fileCopySlice(next, compressedSize, indexSize, this->indexFile, this->indexStarts))
			return lz4Cleanup(this);

		/* Remove index data from the compressed data file */
		if (stackTruncate(next, compressedSize) < 0)
			return lz4Cleanup(this);

		this->indexStarts = 0;
	}

	/* otherwise (read only) */
	else
	{
		/* Open the data file a second time (gives separate buffer) */
		this->indexFile = stackOpen(proto->indexFile, path, oflags, mode);
		if (this->indexFile->openVal < 0)
			return lz4Cleanup(this);

		/* Remember where the index starts (just after the compressed data ends */
		this->indexStarts = compressedSize;
	}

	/* Tell our caller which block size we are using. */
	this->ioStack.blockSize = this->blockSize = this->defaultBlockSize;

	/* Verify the data and index files have compatible block sizes */
	if (next->blockSize != 1 || sizeof(off_t) % this->indexFile->blockSize != 0)
	{
		setIoStackError(this, -1, "Compression block size conflict: next=%zd index=%zd\n", next->blockSize, this->indexFile->blockSize);
		return lz4Cleanup(this);
	}

	/* Allocate buffers */
	this->maxCompressed = maxCompressedSize(this->blockSize);
	this->compressedBuf = malloc(this->maxCompressed);
	this->tempBuf = malloc(this->blockSize);
	if (this->compressedBuf == NULL || this->tempBuf == NULL)
	{
		checkSystemError(this, -1, "Unable to allocate compressed buffer of size %zd", this->maxCompressed);
		return lz4Cleanup(this);
	}

    /* Track the size of the file (more accurately, the offset and size of the last black */
	this->lastBlock = ROUNDDOWN(fileSize, this->blockSize);
	if (this->lastBlock == fileSize && fileSize != 0)
		this->lastBlock -= this->blockSize;
	this->lastSize = fileSize - this->lastBlock;

	/* Get the offset and size of the last compressed block */
	this->compressedLastBlock = 0;
	this->compressedLastSize = 0;
	if (indexSize > 0)
	{
		/* Read the offset of the last block in the compressed file */
		this->compressedLastBlock = getCompressedOffset(this, this->lastBlock);
		if (this->compressedLastBlock < 0)
			return lz4Cleanup(this);

		/* The size of the last compressed block is whatever is left over */
		this->compressedLastSize = rawSize - 16 - indexSize - this->compressedLastBlock;
	}

    /* Do we want to write a file header containing the block size? */
    /* TODO: later. */

	debug("lz4Open: retval=%d  blockSize=%zd  lastBlock=%lld\n", this->ioStack.openVal, this->blockSize, this->lastBlock);
	return this;
}

/*
 * Return the size of the decompressed file.
 */
static
off_t lz4CompressSize(Lz4Compress *this)
{
	return this->lastBlock + (off_t)this->lastSize;
}

static ssize_t lz4CompressTruncate(Lz4Compress *this, off_t offset);

/*
 * Write a block to the compressed file and its index.
 * Allows rewriting of the last block, but not other blocks.
 */
static
ssize_t lz4CompressWrite(Lz4Compress *this, const Byte *buf, size_t size, off_t offset)
{
    ssize_t compressed, actual;
	debug("lz4Write: size=%zd offset=%lld lastBlock=%lld\n", size, (long long)offset, (long long)his->lastBlock);

	/* All writes must be aligned */
	if (offset % this->blockSize != 0)
		return setIoStackError(this, -1, "Compression: all writes must be aligned. offset=%lld  alignment=%zd", (long long)offset, this->blockSize);

	/* Gaps not allowed */
	if (offset > this->lastBlock + this->blockSize)
		return setIoStackError(this, -1, "Compression: holes not allowed.  offset=%lld  fileSize=%lld", (long long)offset, (long long)this->lastBlock + this->lastSize);

	/* With compression, we can only write (or overwrite) at the end of the file. */
	if (offset + size < this->lastBlock + this->lastSize)
        return setIoStackError(this, -1, "Can only write at end of compressed file. offset=%lld  fileSize=%lld", (long long)offset, (long long)this->lastBlock+this->lastSize);

	/* Adjust write size to be a single block */
	size = MIN(size, this->blockSize);

	/* If overwriting more than a single block, then truncate the file first. */
	if (offset + size < this->lastBlock + this->lastSize && lz4CompressTruncate(this, offset) < 0)
		return -1;

	/* if appending a new block, */
	if (offset == this->lastBlock + this->lastSize)
	{
		/* Calculate offsets in the compressed file and the index file for the new block. TODO: call getCompressedIndex?? */
		off_t indexOffset = offset / this->blockSize * 8;
		off_t compressedOffset = this->compressedLastBlock + this->compressedLastSize;

		/* Write the new index entry */
		if (!stackWriteInt64(this->indexFile, compressedOffset, indexOffset))
			return checkSystemError(this, -1, "Unable to write to lz4 index file");

		/* Update file positions to point to the new block we are about to write */
		this->lastBlock = offset;
		this->compressedLastBlock = compressedOffset;

		/* Just in case the write fails, don't update file size until after the write completes. */
		this->lastSize = 0;
		this->compressedLastSize = 0;
	}

    /* Compress the block and write it out as a variable sized record */
    compressed = lz4CompressBuffer(this, this->compressedBuf, this->maxCompressed, buf, size);
    actual = stackWriteSized(nextStack(this), this->compressedBuf, compressed, this->compressedLastBlock);
    if (actual < 0)
		return copyError(this, actual, nextStack(this));

    /* Remember the size of this new, last block */
	this->compressedLastSize = compressed + 4;  /* Include the prepended 4 byte frame size */
	this->lastSize = size;

    return size;
}

/*
 * Read a block from the compressed file.
 */
static
ssize_t lz4CompressRead(Lz4Compress *this, Byte *buf, size_t size, off_t offset)
{
    off_t compressedOffset, compressedActual;
    ssize_t actual;
    debug("lz4Read: size=%zd  offset=%lld\n", size, offset);

	/* If we are positioned at EOF, then return EOF */
	if (offset >= this->lastBlock + this->lastSize)
	{
		thisStack(this)->eof = true;
		return 0;
	}

	/* All reads must be aligned */
	if (offset % this->blockSize != 0)
		return setIoStackError(this, -1, "Compression: all reads must be aligned. offset=%lld  alignment=%zd",
                               (long long)offset, this->blockSize);

	/* Adjust size to be a single block */
	size = MIN(size, this->blockSize);

	/* Read the index to get the offset of the compressed block, checking for error */
	compressedOffset = getCompressedOffset(this, offset);
	if (compressedOffset < 0)
		return compressedOffset;

    /* Read the compressed record, checking for error */
    compressedActual = stackReadSized(nextStack(this), this->compressedBuf, this->maxCompressed, compressedOffset);
	if (compressedActual < 0)
		return -1;

    /* Decompress the record we just read. Note we could have a valid zero length record. */
    actual = lz4DecompressBuffer(this, buf, size, this->compressedBuf, compressedActual);
	this->ioStack.eof = (compressedActual == 0);

	debug("lz4Read(done): actual=%zd lastBlock=%lld  lastSize=%zd\n", actual, this->lastBlock, this->lastSize);
    return actual;
}

/*
 * Close the compressed file.
 */
static
ssize_t lz4CompressClose(Lz4Compress *this)
{
	debug("lz4CompressClose: blockSize=%zd\n", this->blockSize);

	/* if writable ... */
	if (this->writable)
	{

		/* Get the size of the data files */
		off_t fileSize = this->lastBlock + this->lastSize;
		off_t compressedSize = this->compressedLastBlock + this->compressedLastSize;
		off_t indexSize = ROUNDUP(fileSize, this->blockSize) / this->blockSize * 8;
		off_t rawSize = compressedSize + indexSize + 16;  /* What it will be when we're done */
		debug("lz4Close: fileSize=%lld  compressedSize=%lld  indexSize=%lld  rawSize=%lld\n", fileSize, compressedSize, indexSize, rawSize);

		/* Append the index, compressed size and uncompressed size to the compressed file */
        (void)(fileCopySlice(this->indexFile, this->indexStarts, indexSize, nextStack(this), compressedSize) &&
			stackWriteInt64(nextStack(this), compressedSize, rawSize - 16) &&
		    stackWriteInt64(nextStack(this), fileSize, rawSize - 8));
	}

	/* Free up resources */
	lz4Cleanup(this);

	return stackError(this)?-1: 0;
}


/* Ensure all data written so far is persistent. */
static
ssize_t lz4CompressSync(Lz4Compress *this)
{
	int ret1 = stackSync(nextStack(this));
	int ret2 = stackSync(this->indexFile);
	if (ret1 < 0)
		return (int)copyError(this, ret1, nextStack(this));
	if (ret2 < 0)
		return (int)copyError(this, ret2, this->indexFile);

	return 0;

}

static
ssize_t lz4CompressTruncate(Lz4Compress *this, off_t offset)
{
    ssize_t actual;
    ssize_t newSize;
    off_t indexSize;
	debug("lz4truncate: offset=%lld  lastBlock=%lld  lastSize=%zd", offset, this->lastBlock, this->lastSize);

	/* if readonly, then a permissions error */
	if (!this->writable) {
	    errno = EPERM;
	    return checkSystemError(this, -1, "Truncating a readonly compressed file");
	}

	/* If the offset is not aligned, then read the last block.  (TODO: only truncate on block boundary?) */
	actual = 0;
	if (offset % this->blockSize != 0)
		actual = stackReadAll(thisStack(this), this->tempBuf, this->blockSize, ROUNDDOWN(offset, this->blockSize));

	/* Update our internal pointers so it looks like we are truncated at the beginning of the block */
	this->lastBlock = ROUNDDOWN(offset, this->blockSize);
	this->lastSize = 0;
	this->compressedLastBlock = getCompressedOffset(this, this->lastBlock);
	this->compressedLastSize = 0;

	/* Truncate the compressed and data files so we are at the beginning of the last block */
	indexSize = this->lastBlock / this->blockSize * 8;
	if (stackTruncate(nextStack(this), this->compressedLastBlock) < 0)
		return copyNextError(this, -1);
	if (stackTruncate(this->indexFile, indexSize) < 0)
		return copyError(this, -1, this->indexFile);

	/* Write out what remains of the last block */
	newSize = MIN(offset - this->lastBlock, actual);
	if (newSize > 0 && stackWriteAll((IoStack *)this, this->tempBuf, newSize, this->lastBlock) < 0)
		return -1;

	return 0;
}


IoStackInterface lz4CompressInterface = (IoStackInterface) {
    .fnOpen = (IoStackOpen)lz4CompressOpen,
    .fnClose = (IoStackClose)lz4CompressClose,
    .fnRead = (IoStackRead)lz4CompressRead,
    .fnWrite = (IoStackWrite)lz4CompressWrite,
    .fnSize = (IoStackSize)lz4CompressSize,
	.fnTruncate = (IoStackTruncate)lz4CompressTruncate,
	.fnSync = (IoStackSync)lz4CompressSync,
};


/**
 * Create a ioStack for writing and reading compressed files.
 * @param blockSize - size of individually compressed records.
 */
void *lz4CompressNew(size_t blockSize, void *indexFile, void *next)
{
    Lz4Compress *this = malloc(sizeof(Lz4Compress));
    *this = (Lz4Compress){
		.ioStack = (IoStack){.next=next, .iface = &lz4CompressInterface, .blockSize = blockSize,},
		.defaultBlockSize = blockSize,
		.indexFile = indexFile,
		};

    return (IoStack *)this;
}

/*
 * Compress a block of data from the input buffer to the output buffer.
 * Note the output buffer must be large enough to hold Size(input) bytes.
 * @param toBuf - the buffer receiving compressed data
 * @param toSize - the size of the buffer
 * @param fromBuf - the buffer providing uncompressed data.
 * @param fromSize - the size of the uncompressed .
 * @param error - keeps track of the error status.
 * @return - the number of compressed bytes.
 */
ssize_t lz4CompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize)
{
    ssize_t actual;
	debug("lz4CompressBuffer: toSize=%zd fromSize=%zd data=%.*s\n", toSize, fromSize, (int)fromSize, (char*)fromBuf);
	actual = LZ4_compress_default((char*)fromBuf, (char*)toBuf, (int)fromSize, (int)toSize);
	if (actual < 0)
		actual = setLz4Error(this, actual);

	debug("lz4CompressBuffer: actual=%zd   compressed=%s\n", actual, asHex(this->compressedBuf, actual));
	return actual;
}


/*
 * Decompress a block of data from the input buffer to the output buffer.
 * Note the output buffer must be large enough to hold a full record of data.
 * @param toBuf - the buffer receiving decompressed data
 * @param toSize - the size of the buffer
 * @param fromBuf - the buffer providing compressed data.
 * @param fromSize - the size of the uncompressed .
 * @param error - keeps track of the error status.
 * @return - the number of decompressed bytes.
 */
ssize_t lz4DecompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize)
{
    ssize_t actual;

	/* Special case for empty buffer. (eg. EOF) */
	debug("lz4DeompressBuffer: fromSize=%zd toSize=%zd  compressed=%s\n", fromSize, toSize, asHex(this->compressedBuf, fromSize));
	if (fromSize == 0)
		return 0;

	/* Decompress the buffer */
	actual = LZ4_decompress_safe((char*)fromBuf, (char*)toBuf, (int)fromSize, (int)toSize);
	if (actual < 0)
		actual = setLz4Error(this, actual);

	debug("lz4DecompressBuffer: actual=%zd data=%.*s\n", actual, (int)actual, toBuf);
	return actual;
}

/*
 * Given a proposed buffer of uncompressed data, how large could the compressed data be?
 * For LZ4 compression, it is crucial the output buffer be at least as large as any actual output.
 * @param inSize - size of uncompressed data.
 * @return - maximum size of compressed data.
 */
size_t maxCompressedSize(size_t rawSize)
{
	return LZ4_compressBound((int)rawSize);
}


static Lz4Compress *lz4Cleanup(Lz4Compress *this)
{
	IoStack *next = this->ioStack.next;
	IoStack *index = this->indexFile;

	/* Close the next layer if opened */
	if (next != NULL && next->openVal >= 0)
		stackClose(next);

	/* Delete the index file if it was created. TODO: error handling */
	if (index != NULL && index->openVal >= 0)
	{
		if (this->writable)
		    stackTruncate(index, 0); /* Don't flush to disc when closing */

		stackClose(this->indexFile);

		if (this->writable && unlink(this->indexPath) < 0)
		    checkSystemError(this, -1, "lz4 Unable to delete index file %s\n", this->indexPath);
	}

	/* If we have no errors, then use error info from successor or index file. */
	if (!stackError(this) && next != NULL && stackError(next))
		copyNextError(this, -1);
	else if (!stackError(this) && index != NULL && stackError(index))
		copyError(this, -1, index);

	/* Free the compressed and index layers if allocated */
	if (next != NULL)
		free(next);
	this->ioStack.next = NULL;
	if (index != NULL)
		free(index);
	this->indexFile = NULL;

	/* Free the buffers if allocated */
	if (this->compressedBuf != NULL)
		free(this->compressedBuf);
	this->compressedBuf = NULL;
	if (this->tempBuf != NULL)
		free(this->tempBuf);
	this->tempBuf = NULL;

	/* Make note we are fully closed */
	this->ioStack.openVal = -1;

	return this;
}

ssize_t setLz4Error(Lz4Compress *this, ssize_t retval)
{
	return setIoStackError(this, -1, "Lz4 compression error: code=%zd", -retval);

}

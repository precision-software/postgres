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
static ssize_t setLz4Error(ssize_t retval, const char *fmt, ...);
ssize_t lz4DecompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize);
ssize_t lz4CompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize);
size_t compressedSize(size_t size);
static Lz4Compress *lz4Cleanup(Lz4Compress *this);

/* Structure holding the state of our compression/decompression filter. */
struct Lz4Compress
{
    IoStack ioStack;

    size_t defaultBlockSize;          /* Configured default size of uncompressed block. */

    size_t compressedSize;            /* upper limit on compressed block size */
	size_t blockSize;                 /* Copy of thisStack(this)->blockSize to make code prettier */
    Byte *compressedBuf;              /* Buffer to hold compressed data */
	Byte *tempBuf;                    /* temporary buffer to hold decompressed data when probing for size */

    IoStack *indexFile;               /* Index file created "on the fly" to support block seeks. */

    off_t  lastBlock;				  /* The offset of the last (non-compressed) block */
	size_t lastSize;                  /* The uncompressed size of the last block */
	off_t  compressedLastBlock;       /* The offset of the last block in the compressed file */
	size_t compressedLastSize;        /* the compressed size of the last block, including 4 byte size */

	bool modified;                    /* The file has been modified */
};


/*
 * Open a compressed file and its index.
 */
static
Lz4Compress *lz4CompressOpen(Lz4Compress *proto, const char *path, int oflags, int mode)
{
    debug("lz4Open: path=%s  oflags=0x%x\n", path, oflags);

    /* Open the compressed file using the given path */
	IoStack *next = fileOpen(nextStack(proto), path, oflags, mode);


    /* Get the index file name by adding ".idx" to the file name. */
    char indexPath[MAXPGPATH];
    strlcpy(indexPath, path, sizeof(indexPath));
    strlcat(indexPath, ".idx", sizeof(indexPath));

	/* Since we might not have deleted the index file, truncate it instead of raising error if they exist */
	if (oflags & O_EXCL)
		oflags = (oflags & ~O_EXCL) | O_TRUNC;
	IoStack *indexFile = fileOpen(proto->indexFile, indexPath, oflags, mode);

	/* Clone our prototype */
	Lz4Compress *this = lz4CompressNew(proto->defaultBlockSize, indexFile, next);
	this->ioStack.openVal = next->openVal;

	/* Tell our caller which block size we are using. TODO: do we ever want caller to tell us? */
	this->ioStack.blockSize = this->blockSize = this->defaultBlockSize;

	/* if any open errors occurred, save the error info and free the downstream files */
	if (next->openVal < 0 || indexFile->openVal < 0)
		return lz4Cleanup(this);

	/* Verify the data and index files have compatible block sizes */
	if (next->blockSize != 1 || sizeof(off_t) % this->indexFile->blockSize != 0)
	    return lz4Cleanup(this);

	/* Allocate buffers */
	this->compressedSize = compressedSize(this->blockSize);
	this->compressedBuf = malloc(this->compressedSize);
	if (this->compressedBuf == NULL)
		checkSystemError(this, -1, "Unable to allocate compressed buffer of size %zd", this->compressedSize);

	this->tempBuf = malloc(this->blockSize);
	if (this->tempBuf == NULL)
		checkSystemError(this, -1, "Unable to allocate uncompresseed buffer of size %ze", this->blockSize);

	if (stackHasError(this))
		return lz4Cleanup(this);

	this->lastBlock = 0;
	this->lastSize = 0;
	this->compressedLastBlock = 0;
	this->compressedLastSize = 0;

	/* If truncating the file, then we are done. No need to get existing file sizes */
	if (oflags & O_TRUNC)
		return this;

    /* Make note of the file's existing size TODO: */
	off_t indexSize = fileSize(this->indexFile);
	Assert(indexSize % 8 == 0);

	/* If we have an existing index file, ... TODO: only for random access or append */
	if (indexSize > 0)
	{
		/* Read the last entry in the index file, which is the offset of the last block */
		if (!fileReadInt64(this->indexFile, (uint64_t *)&this->compressedLastBlock, indexSize - 8, WAIT_EVENT_NONE))
		{
			copyError(this, -1, this->indexFile);
			return lz4Cleanup(this);
		}

		/* Read the last data block, just to determine its size */
		this->lastBlock = indexSize / 8 * this->blockSize - this->blockSize;
		this->lastSize = fileReadAll(thisStack(this), this->tempBuf, this->blockSize, this->lastBlock, WAIT_EVENT_NONE);
		if (this->lastSize < 0)
			return lz4Cleanup(this);

		/* TODO: compressedLastSize is not being set!!! */
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


/*
 * Write a block to the compressed file and its index.
 * Allows rewriting of the last block, but not other blocks.
 */
static
ssize_t lz4CompressWrite(Lz4Compress *this, const Byte *buf, size_t size, off_t offset, uint32 wait_info)
{
	/* if NOT writing (or about to overwrite) the last block, then we have an error */
	if (offset + size < this->lastBlock + this->lastSize)
		return setIoStackError(this, -1, "Can only write at end of compressed file. endWrite=%lld  fileSize=%lld", offset+size, this->lastBlock+this->lastSize);

    /* We do one block at a time */
	debug("lz4Write: size=%zd offset=%lld lastBlock=%lld\n", size, offset, this->lastBlock);
    size = MIN(size, this->blockSize);
	Assert(offset % this->blockSize == 0);  /* offset must be aligned on block boundary */
	Assert(this->lastBlock % this->blockSize == 0);

	/* if appending a new block, */
	if (offset == this->lastBlock + this->lastSize)
	{
		/* Verify the previous block was not a partial block */
		if (offset % this->blockSize != 0)
			return setIoStackError(this, -1, "lz4 - writing new block but previous block not full");

		/* Calculate offsets in the compressed file and the index file for the new block */
		off_t indexOffset = offset / this->blockSize * 8;
		off_t compressedOffset = this->compressedLastBlock + this->compressedLastSize;

		/* Write the new index entry */
		if (!fileWriteInt64(this->indexFile, compressedOffset, indexOffset, WAIT_EVENT_NONE))
			return checkSystemError(this, -1, "Unable to write to lz4 index file");

		/* Update file positions to point to the new block we are about to write */
		this->lastBlock = offset;
		this->compressedLastBlock = compressedOffset;

		/* For safety ... */
		this->compressedLastSize = 0;
		this->lastSize = 0;
	}

    /* Compress the block and write it out as a variable sized record */
    size_t compressed = lz4CompressBuffer(this, this->compressedBuf, this->compressedSize, buf, size);
    ssize_t actual = fileWriteSized(nextStack(this), this->compressedBuf, compressed, this->compressedLastBlock, wait_info);
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
ssize_t lz4CompressRead(Lz4Compress *this, Byte *buf, size_t size, off_t offset, uint32 wait_info)
{
    /* We do one record at a time */
    size = MIN(size, this->blockSize);

    debug("lz4Read: size=%zu  offset=%llu\n", size, offset);

	if (offset % this->blockSize != 0)
		return setIoStackError(this, -1, "lz4 writing an unaligned offset (%lld) (%lld)", offset, this->blockSize);

	/* Read the index to get the offset of the compressed block, checking for EOF or error */
	off_t indexOffset = offset / this->blockSize * 8;
	uint64_t compressOffset;
	if (!fileReadInt64(this->indexFile, &compressOffset, indexOffset, wait_info))
	    return copyError(this, fileEof(this->indexFile)? 0: -1, this->indexFile);
	Assert(compressOffset & this->blockSize == 0);

    /* Read the compressed record, */
    size_t compressedActual = fileReadSized(nextStack(this), this->compressedBuf, this->compressedSize, compressOffset, WAIT_EVENT_NONE);

    /* Decompress the record we just read. */
    size_t actual = lz4DecompressBuffer(this, buf, size, this->compressedBuf, compressedActual);

	/* Update file positions */
	if (offset == this->lastBlock)
	{
		this->lastSize = actual;
		this->compressedLastBlock = compressOffset;
		this->compressedLastSize = compressedActual + 4;
	}

	debug("lz4Read(done): actual=%zd lastBlock=%zd  lastSize=%zd\n", actual, this->lastBlock, this->lastSize);
    return actual;
}

/*
 * Close the compressed file.
 */
static
ssize_t lz4CompressClose(Lz4Compress *this)
{
	debug("lz4CompressClose: blockSize=%zu\n", this->blockSize);

	lz4Cleanup(this);

	return (stackHasError(this)? -1: 0);
}


/* Ensure all data written so far is persistent. */
static
ssize_t lz4CompressSync(Lz4Compress *this, uint32 wait_info)
{
	int ret1 = fileSync(nextStack(this), wait_info);
	int ret2 = fileSync(this->indexFile, wait_info);
	if (ret1 < 0)
		return (int)copyError(this, ret1, nextStack(this));
	if (ret2 < 0)
		return (int)copyError(this, ret2, this->indexFile);

	return 0;

}

static
off_t lz4CompressTruncate(Lz4Compress *this, off_t offse, uint32 wait_info)
{
	Assert(offset % this->blockSize == 0);
	return setIoStackError(this, -1, "lz4CompressTruncate Not yet implemented");
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


/*
 * Create a ioStack for writing and reading compressed files.
 * TODO: blocksize is set at Open, specified by caller.
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
	debug("lz4CompressBuffer: toSize=%zu fromSize=%zu data=%.*s\n", toSize, fromSize, (int)fromSize, (char*)fromBuf);
	ssize_t actual = LZ4_compress_default((char*)fromBuf, (char*)toBuf, (int)fromSize, (int)toSize);
	if (actual < 0)
		setIoStackError(this, actual, "lz4: unable to compress");

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
	debug("lz4DeompressBuffer: fromSize=%zu toSize=%zu  compressed=%s\n", fromSize, toSize, asHex(this->compressedBuf, fromSize));
	ssize_t actual = LZ4_decompress_safe((char*)fromBuf, (char*)toBuf, (int)fromSize, (int)toSize);
	if (actual < 0)
		setIoStackError(this, actual, "lz4 unable to decompress a buffer");

	debug("lz4DecompressBuffer: actual=%zd data=%.*s\n", actual, (int)actual, toBuf);
	return actual;
}

/*
 * Given a proposed buffer of uncompressed data, how large could the compressed data be?
 * For LZ4 compression, it is crucial the output buffer be at least as large as any actual output.
 * @param inSize - size of uncompressed data.
 * @return - maximum size of compressed data.
 */
size_t compressedSize(size_t rawSize)
{
	return LZ4_compressBound((int)rawSize);
}


static Lz4Compress *lz4Cleanup(Lz4Compress *this)
{
	IoStack *next = this->ioStack.next;
	IoStack *index = this->indexFile;

	/* Close the next layer if opened */
	if (next != NULL && next->openVal >= 0)
		fileClose(next);
	this->ioStack.openVal = -1;

	/* Close the index file if opened */
	if (index != NULL && index->openVal >= 0)
		fileClose(index);

	/* If we have no errors, then use error info from successor or index file. */
	if (!stackHasError(this) && next != NULL && stackHasError(next))
		copyNextError(this, -1);
	else if (!stackHasError(this) && index != NULL && stackHasError(index))
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

	return this;
}

/* */
/* Created by John Morris on 11/1/22. */
/* */
//#define DEBUG
#include <stdlib.h>
#include <lz4.h>
#include <sys/fcntl.h>
#include "debug.h"
#include "storage/iostack_internal.h"
#include "utils/wait_event.h"

typedef struct Lz4Compress Lz4Compress;


/* Forward references */
static ssize_t setLz4Error(ssize_t retval, const char *fmt, ...);
ssize_t lz4DecompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize);
ssize_t lz4CompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize);
size_t compressedSize(size_t size);

/* Structure holding the state of our compression/decompression filter. */
struct Lz4Compress
{
    IoStack ioStack;

    size_t blockSize;                /* Configured size of uncompressed block. */

    size_t compressedSize;            /* upper limit on compressed block size */
    Byte *compressedBuf;              /* Buffer to hold compressed data */
	Byte *tempBuf;                    /* temporary buffer to hold decompressed data when probing for size */

    IoStack *indexFile;               /* Index file created "on the fly" to support block seeks. */

    off_t  lastBlock;				  /* The offset of the last (non-compressed) block */
	size_t  lastSize;                 /* The uncompressed size of the the last block */
	off_t   compressedLastBlock;      /* The offset of the last block in the compressed file */
	size_t   compressedLastSize;      /* the compressed size of the last block */
};


ssize_t lz4CompressOpen(Lz4Compress *this, const char *path, int oflags, int mode)
{
    debug("lz4Open: path=%s  oflags=0x%x\n", path, oflags);

    /* Open the compressed file */
	int retval = fileOpen(nextStack(this), path, oflags, mode);
	if (retval < 0)
		return copyNextError(this, retval);

    /* Open the index file as well. */
    char indexPath[MAXPGPATH];
    strlcpy(indexPath, path, sizeof(indexPath));
    strlcat(indexPath, ".idx", sizeof(indexPath));
	if (fileOpen(this->indexFile, indexPath, oflags, mode) < 0)
	{
		fileClose(nextStack(this));
		fileErrorInfo(this->indexFile, &this->indexFile->errNo, this->indexFile->errMsg);
		return retval;
	}

	/* Verify the data and index files have compatible block sizes */
	if (nextStack(this)->blockSize != 1 || sizeof(off_t) % this->indexFile->blockSize != 0)
	{
	    fileClose(nextStack(this));
		fileClose(this->indexFile);
		return setIoStackError(this, -1, "Lz4 mismatched block sizes: dataSize=%d  indexSize=%d", nextStack(this)->blockSize, this->indexFile->blockSize);
	}

	/* Allocate buffers */
	this->compressedSize = compressedSize(this->blockSize);
	this->compressedBuf = malloc(this->compressedSize);
	this->tempBuf = malloc(this->blockSize);

	this->lastBlock = 0;
	this->lastSize = 0;
	this->compressedLastBlock = 0;
	this->compressedLastSize = 0;

	if (oflags & O_TRUNC)
		return retval;

    /* Make note of the file's existing size TODO: */
	off_t indexSize = fileSize(this->indexFile);
	Assert(indexSize % 8 == 0);
	// error check

	/* If we have an existing index file, ... */
	if (indexSize > 0)
	{
		/* Read the last entry in the index file, which is the offset of the last block */
		this->compressedLastBlock = fileGet8(this->indexFile, indexSize - 8);
		// TODO: error check.

		/* Read the last data block, just to determine its size */
		this->lastBlock = indexSize / 8 * this->blockSize - this->blockSize;
		this->lastSize = fileReadAll(this, this->tempBuf, this->blockSize, this->lastBlock, WAIT_EVENT_NONE);
		// TODO: error check.
	}

    /* Do we want to write a file header containing the block size? */
    /* TODO: later. */
}


off_t lz4CompressSize(Lz4Compress *this)
{
	return this->lastBlock + (off_t)this->lastSize;
}



size_t lz4CompressWrite(Lz4Compress *this, const Byte *buf, size_t size, off_t offset, uint32 wait_info)
{
    /* We do one block at a time */
    size = MIN(size, this->blockSize);
	Assert(offset % this->blockSize == 0);  /* offset must be aligned on block boundary */

	/* if appending a new block, */
	if (offset == this->lastBlock + this->blockSize)
	{
		/* Verify the previous block is full */
		if (this->lastSize != this->blockSize)
			return setIoStackError(this, -1, "lz4 - writing new block but previous block not full");

		/* Write a new offset at the end of the index file */
		off_t indexSize = this->lastBlock / this->blockSize * 8;
		if (filePut8(this->indexFile, indexSize) < 0)
			return checkSystemError(this, -1, "Unable to write to lz4 index file");

		/* Update file position to point to the new block we are about to write */
		this->lastBlock += this->lastSize;
		this->compressedLastBlock += this->compressedLastSize;

		/* For safety ... */
		this->compressedLastSize = 0;
		this->lastSize = 0;
	}

	/* if NOT writing (or overwriting) the last block, then we have an error */
	if (offset != this->lastBlock)
		return setIoStackError(this, -1, "Can only write at end of compressed file");

    /* Compress the block and write it out as a variable sized record */
    size_t compressed = lz4CompressBuffer(this, this->compressedBuf, this->compressedSize, buf, size);
    ssize_t actual = fileWriteSized(nextStack(this), this->compressedBuf, compressed, offset, wait_info);
    if (actual < 0)
		return copyError(this, actual, nextStack(this));

    /* Remember the size of this new, last block */
	this->compressedLastSize = actual + 4;
	this->lastSize = size;

    return size;
}

size_t lz4CompressRead(Lz4Compress *this, Byte *buf, size_t size, off_t offset, uint32 wait_info)
{
    /* We do one record at a time */
    size = MIN(size, this->blockSize);

    debug("lz4Read: size=%zu  compressedPosition=%llu\n", size, this->compressedPosition);

	/* Read the index */
	off_t indexOffset = offset / this->blockSize * 8;
	off_t compressOffset = fileGet8(this->indexFile, indexOffset);
	// TODO: error check

    /* Read the compressed record, */
    size_t compressedActual = fileReadSized(this, this->compressedBuf, this->compressedSize, compressOffset);

    /* Decompress the record we just read. */
    size_t actual = lz4DecompressBuffer(this, buf, size, this->compressedBuf, compressedActual);

    return actual;
}


void lz4CompressClose(Lz4Compress *this, Error *error)
{
	debug("lz4CompressClose: blockSize=%zu\n", this->blockSize);
    fileClose(this->indexFile);
    fileClose(nextStack(this));
    if (this->compressedBuf != NULL)
        free(this->compressedBuf);
	this->compressedBuf = NULL;
    if (this->tempBuf != NULL)
        free(this->tempBuf);
	this->tempBuf = NULL;
	debug("lz4CompressClose: msg=%s\n", error->msg);
}


int lz4CompressSync(Lz4Compress *this, uint32 wait_info)
{
	int ret1 = fileSync(nextStack(this), wait_info);
	int ret2 = fileSync(this->indexFile, wait_info);
	if (ret1 < 0)
		return (int)copyError(this, ret1, nextStack(this));
	if (ret2 < 0)
		return (int)copyError(this, ret2, this->indexFile);

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
 * TODO: blocksize is set at Open, specified by caller.
 * @param blockSize - size of individually compressed records.
 */
Lz4Compress *lz4CompressNew(size_t blockSize, void *next)
{
    Lz4Compress *this = malloc(sizeof(Lz4Compress));
    *this = (Lz4Compress){
		.blockSize = blockSize,
		.ioStack = (IoStack){.next=next, .blockSize = -1,},
		.indexFile = NULL,
		.compressedBuf = NULL,
		.tempBuf = NULL,
		};


    return this;
}



/**
 * Compress a block of data from the input buffer to the output buffer.
 * Note the output buffer must be large enough to hold Size(input) bytes.
 * @param toBuf - the buffer receiving compressed data
 * @param toSize - the size of the buffer
 * @param fromBuf - the buffer providing uncompressed data.
 * @param fromSize - the size of the uncompressed .
 * @param error - keeps track of the error status.
 * @return - the number of compressed bytes.
 */
size_t lz4CompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize)
{
	debug("lz4CompressBuffer: toSize=%zu fromSize=%zu data=%.*s\n", toSize, fromSize, (int)fromSize, (char*)fromBuf);
	int actual = LZ4_compress_default((char*)fromBuf, (char*)toBuf, (int)fromSize, (int)toSize);
	if (actual < 0)
		setIoStackError(this, actual, "lz4: unable to compress");

	debug("lz4CompressBuffer: actual=%d   cipherBuf=%s\n", actual, asHex(this->compressedBuf, actual));
	return actual;
}


/**
 * Decompress a block of data from the input buffer to the output buffer.
 * Note the output buffer must be large enough to hold a full record of data.
 * @param toBuf - the buffer receiving decompressed data
 * @param toSize - the size of the buffer
 * @param fromBuf - the buffer providing compressed data.
 * @param fromSize - the size of the uncompressed .
 * @param error - keeps track of the error status.
 * @return - the number of decompressed bytes.
 */
size_t lz4DecompressBuffer(Lz4Compress *this, Byte *toBuf, size_t toSize, const Byte *fromBuf, size_t fromSize)
{
	debug("lz4DeompressBuffer: fromSize=%zu toSize=%zu  cipherBuf=%s\n", fromSize, toSize, asHex(this->compressedBuf, fromSize));
	int actual = LZ4_decompress_safe((char*)fromBuf, (char*)toBuf, (int)fromSize, (int)toSize);
	if (actual < 0)
		setIoStackError(this, actual, "lz4 unable to decompress a buffer");

	debug("lz4DecompressBuffer: actual=%d cipherBuf=%.*s\n", actual, actual, toBuf);
	return actual;
}



/**
 * Given a proposed buffer of uncompressed data, how large could the compressed data be?
 * For LZ4 compression, it is crucial the output buffer be at least as large as any actual output.
 * @param inSize - size of uncompressed data.
 * @return - maximum size of compressed data.
 */
size_t compressedSize(size_t rawSize)
{
	return LZ4_compressBound((int)rawSize);
}

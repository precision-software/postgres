/*
 *
 * TODO: integrate with postgres encryption
 * TODO: consider allocating blockSize header to keep things aligned.
 * TODO: option to skip MAC so block sizes don't change. (aids alignment)
 */
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include "postgres.h"

#include <openssl/opensslv.h>
#include <openssl/ssl.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/rand.h>

#include "storage/fd.h"
#include "storage/iostack.h"
#include "utils/wait_event.h"
#include "packed.h"

/* Interface */
typedef struct Aead Aead;


/* Forward references for Encryption/Decryption primitives */
typedef struct CipherContext CipherContext;
bool cipherSetup(CipherContext *this, char *cipherName, Byte *key, ssize_t keySize);
ssize_t cipherEncrypt(CipherContext *this,
					  const Byte *plainText, size_t plainSize,
					  Byte *header, size_t headerSize,
					  Byte *cipherText, size_t cipherSize,
					  Byte *iv, Byte *tag);
ssize_t cipherDecrypt(CipherContext *this,
					  Byte *plainText, size_t plainSize,
					  Byte *header, size_t headerSize,
					  Byte *cipherText, size_t cipherSize,
					  Byte *iv, Byte *tag);
static void cipherCleanup(CipherContext *this);

const char *cipherGetMsg(CipherContext *this);
ssize_t cipherSetMsg(CipherContext *this, ssize_t retval, const char *msg, ...);
size_t cipherGetIVSize(CipherContext *this);
size_t cipherGetTagSize(CipherContext *this);
size_t cipherGetPadding(CipherContext *this, size_t plainTextSize);
ssize_t cipherSetMsg(CipherContext *this, ssize_t retval, const char *msg, ...) pg_attribute_printf(3,4);
ssize_t cypherSetSslError(CipherContext *this, ssize_t retval);

/* Cipher context - exposed for inlining and inclusion in other structures */
struct CipherContext
{
	size_t keySize;              /* The size of the key in bytes */
	size_t ivSize;               /* Size of the initialization vector  (12 bytes for AES) */
	size_t cipherBlockSize;      /* Size of the cipher block. (16 bytes for AES) */
	size_t tagSize;              /* Size of the MAC tag to authenticate the encrypted block. (16 bytes for AES-GCM) */
	EVP_CIPHER *cipher;          /* The libcrypto cipher structure */
	EVP_CIPHER_CTX *ctx;         /* libcrypto context. */
	Byte key[EVP_MAX_KEY_LENGTH]; /* The key used for encryption */
	char msg[121];               /* Error message buffer */
};


static off_t aeadSize(Aead *this);
void generateIV(Aead *this, Byte *iv, size_t blockNr, uint64 sequenceNr);
ssize_t aead_encrypt(Aead *this, const Byte *plainBlock, size_t plainSize, Byte *header,
					 size_t headerSize, Byte *cipherBlock, size_t cipherSize, Byte *tag, size_t blockNr);
ssize_t aead_decrypt(Aead *this, Byte *plainText, size_t plainSize, Byte *header,
					 size_t headerSize, Byte *cipherText, size_t cipherSize, Byte *tag, size_t blockNr);
bool aeadCipherSetup(Aead *this, char *cipherName);
bool aeadConfigure(Aead *this);
size_t paddingSize(Aead *this, size_t suggestedSize);
off_t plainToCryptOffset(Aead *this, off_t plainOffset);
size_t cryptSize(Aead *this, size_t plainSize);
static Aead *aeadCleanup(Aead *this);
static bool aeadTruncate(Aead *this, off_t offset, uint32 wait);
static bool aeadExpand(Aead *this, off_t newSize, uint32 wait);
static bool writeFinal(Aead *this);
static ssize_t aeadRead(Aead *this, Byte *buf, size_t size, off_t offset, uint32 wait);

/*
 * Converter structure for encrypting and decrypting Blocks.
 */
#define MAX_CIPHER_NAME 64
#define MAX_AEAD_HEADER_SIZE 1024
#define HEADER_SEQUENCE_NUMBER ((size_t)-1)
struct Aead
{
	/* Always at beginning of structure */
	IoStack ioStack;

	/* Configuration. */
	Byte key[EVP_MAX_KEY_LENGTH];       /* The key for encrypting/decrypting */
	size_t keySize;                     /* The size of the key */
	char cipherName[MAX_CIPHER_NAME];   /* The name of the cipher, if encrypting a new file. */
	size_t suggestedSize;               /* The plaintext block size, if encrypting a new file */
	uint64 (*getSequenceNr)(void);       /* Function to generate next sequence number */

	/* Cipher State */
	CipherContext cipher[1];

	/* Our state after we've opened the encrypted file */
	off_t plainFileSize;          /* Unencrypted size of the file */
	size_t blockSize;              /* The size of the decrypted blocks */
	Byte *plainBuf;               /* A buffer to temporarily hold a decrypted block. */

	off_t cryptFileSize;          /* size of encrypted file as written to disk */
	Byte *cryptBuf;               /* Buffer to hold the current encrypted block */
	size_t cryptOverhead;         /* The size increase when encrypting */

	Byte *zeros;                  /* A full block of zeros for extending the file with zeros */

	bool writable;
};


/*
 * Open an encrypted file.
 * @param path - the path or file name.
 * @param oflag - the open flags, say O_RDONLY or O_CREATE.
 * @param mode - if creating a file, the permissions.
 * @return - Error status.
 */
static void *
aeadOpen(Aead *proto, const char *path, uint64 oflags, mode_t mode)
{
	ssize_t lastCryptSize;
	ssize_t lastPlainSize;
	off_t lastPlainOffset;
	ssize_t cryptSize;

	Aead *this;
	IoStack *next;

	/* Our actual file will always be binary, so clear the text flag */
	oflags &= ~PG_TEXT;

	/* Open our successor */
	next = stackOpen(nextStack(proto), path, oflags, mode);
	if (next == NULL)
		return NULL;

	/* Clone our prototype and attach to successor */
	this = aeadNew(proto->cipherName, proto->suggestedSize, proto->key, proto->keySize, proto->getSequenceNr, next);
	if (this == NULL)
	{
		stackClose(next);
		free(next);
		return NULL;
	}

	/* Open the underlying file */
	this->ioStack.openVal = next->openVal;
	if (next->openVal < 0)
		return aeadCleanup(this);

	/* Are we open for writing? */
	this->writable = (oflags & O_ACCMODE) != O_RDONLY;

	/* Configure the encryption cipher */
	if (!cipherSetup(this->cipher, this->cipherName, this->key, this->keySize))
	{
		setIoStackError(this, "Unable to setup cipher %s: %s", this->cipherName, cipherGetMsg(this->cipher));
		return aeadCleanup(this);
	}

	/* Our encrypted buffer must include tag and sequence, and it must match our successor's block size */
	this->cryptOverhead = cipherGetTagSize(this->cipher) + sizeof(uint64);
	cryptSize = ROUNDOFF(this->suggestedSize + this->cryptOverhead, next->blockSize);
	cryptSize = MAX(cryptSize, next->blockSize);
	Assert((cryptSize / next->blockSize) > 0 && (cryptSize % next->blockSize) == 0);

	/* Our plaintext buffer must be same size as encrypted buffer, minus tag, minus sequence */
	this->blockSize = cryptSize - this->cryptOverhead;
	file_debug("suggested=%zd cryptSize=%zd, blockSize=%zd nextSize=%zd",
			   this->suggestedSize, cryptSize, this->blockSize, next->blockSize);
	thisStack(this)->blockSize = this->blockSize;

	/* Allocate our own buffers based on the encryption config */
	this->plainBuf = malloc(this->blockSize);
	this->cryptBuf = malloc(this->blockSize + this->cryptOverhead);
	this->zeros = malloc(this->blockSize);
	if (this->cryptBuf == NULL || this->plainBuf == NULL || this->zeros == NULL)
	{
		stackSetError(this, errno, "Unable to allocate encryption buffers of size %zd", this->blockSize);
		return aeadCleanup(this);
	}
	bzero(this->zeros, this->blockSize);

	/* Fetch the size of the plain and encrypted files */
	if (aeadSize(this) < 0)
		return aeadCleanup(this);

	/* A completely empty file must be writable */
	if (!this->writable && this->cryptFileSize == 0)
	{
		setIoStackError(this, -1, "Existing Encrypted file %s is empty", path);
		return aeadCleanup(this);
	}

	/* If encrypted file has data, verify it ends in a valid partial block */
	lastCryptSize = this->cryptFileSize % (this->blockSize + this->cryptOverhead);
	lastPlainSize = lastCryptSize - this->cryptOverhead;
	lastPlainOffset = this->plainFileSize - lastPlainSize;

	file_debug("cryptFileSize=%lld, blockSize=%zd, lastBlockSize=%zd", this->cryptFileSize, this->blockSize, lastCryptSize);
	if (this->cryptFileSize > 0)
	{
		if (lastCryptSize < this->cryptOverhead)
		    setIoStackError(this, EIOSTACK, "Existing encrypted file %s must end in a partial block", path);
	    else
		    aeadRead(this, this->plainBuf, lastPlainSize, lastPlainOffset, 0);

		if (stackError(this))
		    return aeadCleanup(this);
	}

	/* Done. Top of stack includes openval and block size */
	return this;
}


/*
 * Read a block of encrypted data into our internal buffer, placing plaintext into the caller's buffer.
 */
static ssize_t aeadRead(Aead *this, Byte *buf, size_t size, off_t offset, uint32 wait)
{
	off_t cipherOffset;
	ssize_t actual;
	Byte *sequenceBuf;
	Byte *tagBuf;
	uint64 sequenceNr;
	size_t blockNr;
	Byte iv[EVP_MAX_IV_LENGTH];

	file_debug("file=%s size=%zd  offset=%lld blockNr=%llu", FilePathName(thisStack(this)->openVal), size, offset, offset / this->blockSize);

	/* All reads must be aligned */
	if (offset < 0 || offset % this->blockSize != 0)
		return (setIoStackError(this, -1, "Encryption: read from offset (%lld) not aligned (%zd)", (long long)offset, this->blockSize), -1);

	/* Read one block at a time. */
	size = MIN(size, this->blockSize);

	/* Translate our plaintext offset to our successor's encrypted offset */
	cipherOffset = plainToCryptOffset(this, offset);

	/* Read a block of encrypted text into our buffer. */
	actual = stackReadAll(nextStack(this), this->cryptBuf, size + this->cryptOverhead, cipherOffset, wait);
	if (actual <= 0)
		return copyNextError(this, actual); /* Zero length read is EOF */
	if (actual < this->cryptOverhead)
		return setIoStackError(this, -1, "Encryption: file has corrupt block at end");

	/* We now work with the data size */
	size = actual - this->cryptOverhead;

	/* Point to the sequence number, encrypted text and tag in our buffer */
	sequenceBuf = this->cryptBuf + size;
	tagBuf = sequenceBuf + sizeof(sequenceNr);

	/* Generate an IV based on block nr and sequence nr */
	blockNr = (offset / this->blockSize);
	sequenceNr = unpackInt64(sequenceBuf);
	generateIV(this, iv, blockNr, sequenceNr);

	/* Decrypt the ciphertext into our plaintext buffer */
	actual = cipherDecrypt(this->cipher,
							  buf, size,
							  sequenceBuf, sizeof(uint64),
							  this->cryptBuf, size,
							  iv, tagBuf);
	if (actual < 0)
		return setIoStackError(this, -1, "Unable to decrypt: %s", cipherGetMsg(this->cipher));
	Assert(actual == size);

	/* Track file size for EOF handling */
	this->plainFileSize = MAX(this->plainFileSize, offset + size);
	this->cryptFileSize = MAX(this->cryptFileSize, cipherOffset + size + this->cryptOverhead);

	/* Return the number of plaintext bytes read. */
	this->ioStack.eof = (size == 0); /* Empty block means EOF */
	return size;
}

/*
 * Encrypt data into our internal buffer and write to the output file.
 * Note we *do* want to write out zero length blocks if asked.
 * They are used to add a partial block to the end of the file.
 *   @param buf - data to be converted.
 *   @param size - number of bytes to be converted.
 *   @param error - error status, both input and output.
 *   @returns - number of bytes actually used.
 */
static ssize_t aeadWrite(Aead *this, const Byte *buf, size_t size, off_t offset, uint32 wait)
{
	ssize_t cryptSize;
	Byte *sequenceBuf;
	Byte *tagBuf;
	uint64 sequenceNr;
	uint64 blockNr;
	Byte iv[EVP_MAX_IV_LENGTH];
	off_t cryptOffset;
	ssize_t actual;

	file_debug("file=%s size=%zd  offset=%lld blockNr=%llu", FilePathName(thisStack(this)->openVal), size, offset, offset / this->blockSize);
	Assert(offset >= 0);

	/* All writes must be aligned */
	if (offset % this->blockSize != 0)
		return (setIoStackError(this, -1, "Encryption: write to offset (%lld) not aligned (%zd)", (long long)offset, this->blockSize), -1);

	/* Writing a partial block before end of file would cause corruption in the file */
	if (size < this->blockSize && offset + size < this->plainFileSize)
		return (setIoStackError(this, -1, "Encryption: writing partial block before end of file causes corruption"), -1);

	/* Limit encryption to one block at a time */
	size = MIN(size, this->blockSize);

	/* Translate our plaintext size and offset to ciphertext size and offset */
	cryptOffset = plainToCryptOffset(this, offset);
	cryptSize = size + this->cryptOverhead;

	/* The sequence number and tag are appended after the encrypted data */
	sequenceBuf = this->cryptBuf + size;
	tagBuf = sequenceBuf + sizeof(sequenceNr);

	/* Fill in the sequence number in the encrypted buffer. */
	sequenceNr = this->getSequenceNr();
	packInt64(sequenceBuf, sequenceNr);

	/* Generate an IV based on block nr and sequence nr */
	blockNr = (offset / this->blockSize);
	generateIV(this, iv, blockNr, sequenceNr);

	/* Encrypt the block of data into our encryption buffer, generating a tag */
	actual = cipherEncrypt(this->cipher,
								buf, size,
								sequenceBuf, sizeof(sequenceNr), /* Do we benefit from including this? */
								this->cryptBuf, size,
								iv, tagBuf);

	if (actual < 0)
		return setIoStackError(this, -1, "Unable to encrypt: %s", cipherGetMsg(this->cipher));
	Assert(actual == size);

	/* Write the encrypted block out */
	if (stackWriteAll(nextStack(this), this->cryptBuf, cryptSize, cryptOffset, wait) != cryptSize)
		return copyNextError(this, -1);

	/* Keep track of file sizes */
	this->plainFileSize = MAX(this->plainFileSize, offset + size);
	this->cryptFileSize = MAX(this->cryptFileSize, cryptOffset + cryptSize);

	return size;
}

/*
 * Close this encryption stack, releasing resources.
 */
static bool aeadClose(Aead *this)
{
	file_debug("openVal=%zd plainFileSize=%lld cryptFileSize=%lld", this->ioStack.openVal, this->plainFileSize, this->cryptFileSize);

	/* If needed, add a partial (empty) block to mark the end of encrypted data. */
	writeFinal(this);

	/* Release resources, including closing the downstream file */
	aeadCleanup(this);
	file_debug("(done): code=%d msg=%s", stackErrorCode(this), stackErrorMsg(this));
	return !stackError(this);
}


/*
 * Return the size of the plaintext file.
 * Fetch the actual size as reported to the OS,
 * and update the values we are tracking.
 */
static off_t aeadSize(Aead *this)
{
	ssize_t lastCryptSize;
	ssize_t lastPlainSize;
	ssize_t nBlocks;

	/* Get the encrypted file size */
	this->cryptFileSize = stackSize(nextStack(this));
	if (this->cryptFileSize < 0)
		return copyNextError(this, -1);

	/* Get the size of the last block, pretending zero length block is there if needed */
	lastCryptSize = this->cryptFileSize % (this->blockSize + this->cryptOverhead);
	if (lastCryptSize == 0)
		lastCryptSize = this->cryptOverhead;

	/* Translate to a plaintext file size */
	nBlocks = this->cryptFileSize / (this->blockSize + this->cryptOverhead);
	lastPlainSize = lastCryptSize - this->cryptOverhead;
	this->plainFileSize = nBlocks * this->blockSize + lastPlainSize;

	file_debug("plainFileSize=%lld  cryptFileSiz=%lld", this->plainFileSize, this->cryptFileSize);

	return this->plainFileSize;
}

/*
 * Change the size of the file, truncating or filling with zeros.
 */
static bool aeadResize(Aead *this, off_t newSize, uint32 wait)
{
	bool success;
	off_t oldSize;
	file_debug("newSize=%lld", newSize);

	/* To be safe, update the known file size */
	oldSize = aeadSize(this);
	if (oldSize < 0)
		return false;

	/* Extend, truncate or leave alone as appropriate */
	if (newSize < oldSize)
		success = aeadTruncate(this, newSize, wait);
	else if (newSize > this->plainFileSize)
		success = aeadExpand(this, newSize, wait);
	else
		success = true;

	return success;
}

/*
 * Truncate an encrypted file.
 * TODO? Redefine to truncate at the beginning of a block??? Much simpler, and let buffering handle the rest.
 */
static bool aeadTruncate(Aead *this, off_t offset, uint32 wait)
{
	bool success;
	off_t blockOffset;
	file_debug("file=%s offset=%lld  blockSize=%zd", FilePathName(this->ioStack.openVal), offset, this->blockSize);

	/* If truncating in the middle of a block, read in the block being truncated */
	blockOffset = ROUNDDOWN(offset, this->blockSize);
	if (blockOffset != offset)
	{
		success = stackReadAll(this, this->plainBuf, this->blockSize, blockOffset, wait) >= 0;
		if (!success)
			return false;
	}

	/* Set the new file size to the block boundary */
	this->plainFileSize = blockOffset;
	this->cryptFileSize = plainToCryptOffset(this, this->plainFileSize);

	/* Truncate the downstream file to match the end of the last full block */
	success = stackResize(nextStack(this), this->cryptFileSize, wait);
	if (!success)
		return copyNextError(this, false);

	/* Write out a final partial block if needed. */
	if (blockOffset != offset && stackWriteAll(this, this->plainBuf, offset - blockOffset, blockOffset, wait) < 0)
		return false;

	/* Append an empty block if needed */
	if (!writeFinal(this))
		return false;

	return true;
}

static bool aeadExpand(Aead *this, off_t newSize, uint32 wait)
{
	off_t lastBlockOffset;  /* Offset of last block in file */
	ssize_t lastBlockSize;  /* Size of last partial block */
	ssize_t actual;
	ssize_t nZeros;    /* Nr bytes to fill with zeros */

	/*
	 * Fill in the last block if it is a partial block.
	 * Do a "Read, modify, Write" to fill in more zeros..
	 */

	/* If file currently ends on a partial block, */
	lastBlockSize = this->plainFileSize % this->blockSize;
	if (lastBlockSize > 0)
	{
		/* Point to the end of the last full block */
		lastBlockOffset = this->plainFileSize - lastBlockSize;

		/* Read in the partial block, if any. */
		actual = stackReadAll(this, this->plainBuf, this->blockSize, lastBlockOffset, wait);
		if (actual < 0)
			return false;
		Assert(actual == lastBlockSize);  /* Would suggest this->plainFileSize is incorrect */

		/* Extend the last block with zeros. nZeros is to end of block or end of file */
		nZeros = MIN(this->blockSize - lastBlockSize, newSize - this->plainFileSize);
		bzero(this->plainBuf + lastBlockSize, nZeros);

		/* Write out the formerly partial buffer with the zeros appended */
		actual = stackWriteAll(this, this->plainBuf, lastBlockSize + nZeros, lastBlockOffset, wait);
		if (actual < 0)
			return false;
	}

	/* Fill in remaining blocks up to the new file size. */
	while (this->plainFileSize < newSize)
		if (stackWriteAll(this, this->zeros, MIN(newSize - this->plainFileSize, this->blockSize), this->plainFileSize, wait) < 0)
			return false;

	/* Write a final empty block if needed */
	if (!writeFinal(this))
		return false;

	Assert(this->plainFileSize == newSize);

	/* Done */
	return true;
}


/*
 * Sync the encrypted file to persistent storage.
 * We might need to append a final null block
 * if the last block is full.
 */
static bool aeadSync(Aead *this, uint32 wait)
{
	bool success;

	/* Don't sync readonly, although not an error. */
	if (!this->writable)
		return true;

	/* Append a zero length block if it doesn't end on a partial block */
	if (!writeFinal(this))
		return false;

	/* Sync the downstream file */
	success = stackSync(nextStack(this), wait);
	copyNextError(this, 0);

	/* Done. */
	return success;
}

/**
 * Abstract interface for the encryption ioStack.
 */
IoStackInterface aeadInterface = {
	.fnOpen = (IoStackOpen) aeadOpen,
	.fnRead = (IoStackRead) aeadRead,
	.fnWrite = (IoStackWrite) aeadWrite,
	.fnClose = (IoStackClose) aeadClose,
	.fnResize = (IoStackResize) aeadResize,
	.fnSize = (IoStackSize) aeadSize,
	.fnSync = (IoStackSync) aeadSync,
};

/*
 * Create a new aead encryption/decryption filter
 */
void *aeadNew(char *cipherName, size_t suggestedSize, Byte *key, size_t keySize, uint64 (*getSequenceNr)(), void *next)
{
	Aead *this;

	/* Pick a default cipher name */
	if (cipherName == NULL)
		cipherName = "AES-256-GCM";

	/* Create the aead structure. TODO: test for NULL */
	this = malloc(sizeof(Aead));
	*this = (Aead) {
		.suggestedSize = suggestedSize,
		.keySize = keySize,
		.getSequenceNr = getSequenceNr,
		.ioStack = (IoStack) {
			.iface = &aeadInterface,
			.next = next,
		}
	};

	/* Copy in the key and cipher name without overwriting memory. We'll validate later. */
	memcpy(this->key, key, MIN(keySize, sizeof(this->key)));
	strlcpy(this->cipherName, cipherName, sizeof(this->cipherName));

	return thisStack(this);
}

/*
 * Write out an empty block if the encrypted file ends on a block bounary.
 */
static bool writeFinal(Aead *this)
{
	ssize_t  lastCryptSize = this->cryptFileSize % (this->blockSize + this->cryptOverhead);

	file_debug("file=%s  lastCryptSize=%zd writable=%d plainFileSize=%lld cryptFileSize=%lld",
			   FilePathName(this->ioStack.openVal), lastCryptSize, this->writable, this->plainFileSize, this->cryptFileSize);
	Assert (lastCryptSize != 0 || this->plainFileSize % this->blockSize == 0);

	/* Success if we don't need to write out a final block */
	if (!this->writable || lastCryptSize > 0)
		return true;

	/* Write out a null block if needed */
	return (aeadWrite(this, this->plainBuf, 0, this->plainFileSize, 0) == 0);
}


/*
 * Construct an Initialization Vector (IV).
 */
void generateIV(Aead *this, Byte *iv, uint64 blockNr, uint64 sequenceNr)
{
	Assert(blockNr < ((uint64)1) << 32);
	Assert(cipherGetIVSize(this->cipher) == 12);

	packInt32(iv, blockNr);
	packInt64(iv+4, sequenceNr);
	file_debug("blockNr=%zd  sequenceNr=%zd iv=%s", blockNr, sequenceNr, asHex(iv, 12));
}

/*
 * Calculate the file offset of an encrypted block, given the file offset of a plaintext block.
 * Note this only works for block boundaries. TODO: What about header?
 */
off_t plainToCryptOffset(Aead *this, off_t plainOffset)
{
	Assert(plainOffset % this->blockSize == 0);
	return plainOffset / this->blockSize * (this->blockSize + this->cryptOverhead);
}


static Aead *aeadCleanup(Aead *this)
{
	IoStack *next;

	if (this == NULL)
		return NULL;

	/* Close the downstream layer if opened */
	next = this->ioStack.next;
	if (next != NULL && next->openVal >= 0)
		stackClose(next);
	this->ioStack.openVal = -1;

	/* If we have no errors, then report error info from successor */
	if (!stackError(this) && next != NULL && stackError(next))
		copyNextError(this, -1);

	/* Free the next layer if allocated */
	if (next != NULL)
		free(next);
	this->ioStack.next = NULL;

	/* Free the buffers if allocated */
	if (this->cryptBuf != NULL)
		free(this->cryptBuf);
	this->cryptBuf = NULL;
	if (this->plainBuf != NULL)
		free(this->plainBuf);
	this->plainBuf = NULL;
	if (this->zeros != NULL)
		free(this->zeros);
	this->zeros = NULL;

	/* Free the cipher structures. It was allocated as part of "this" so doesn't need freeing */
	cipherCleanup(this->cipher);
	bzero(this->cipher, sizeof(CipherContext));

	return this;
}


/*
 * Helper function to get the cipher details.
 */
bool cipherSetup(CipherContext *this, char *cipherName, Byte *key, ssize_t keySize)
{
	/* Clear out the cipher context */
	*this = (CipherContext){0};

	/* Create an OpenSSL cipher context. */
	this->ctx = EVP_CIPHER_CTX_new();
	if (this->ctx == NULL)
		return cypherSetSslError(this, false);

	/* Lookup cipher by name. */
	this->cipher = EVP_CIPHER_fetch(NULL, cipherName, NULL);
	if (this->cipher == NULL)
		return cipherSetMsg(this, false, "Encryption problem - cipher name %s not recognized", cipherName);

	/* Initialize the cipher here, so we can get the tagsize. */
	if (!EVP_EncryptInit_ex2(this->ctx, this->cipher, NULL, NULL, NULL))
		return cipherSetMsg(this, false, "Encryption problem - can't fetch tag size for %s", cipherName);

	/* Get the properties of the selected cipher */
	this->ivSize = EVP_CIPHER_iv_length(this->cipher);
	if (keySize != EVP_CIPHER_key_length(this->cipher))
		return setIoStackError(this, false, "Cipher key is the wrong size.");
	this->cipherBlockSize = EVP_CIPHER_block_size(this->cipher);
	this->tagSize = EVP_CIPHER_CTX_tag_length(this->ctx);

	/* Save the key */
	Assert(keySize <= sizeof(this->key));
	memcpy(this->key, key, keySize);
	this->keySize = keySize;

	file_debug("ivSize=%zd  keySize=%zd  blockSize=%zd  tagSize=%zd",
		  this->ivSize, this->keySize, this->cipherBlockSize, this->tagSize);

	return true;
}

/*
 * Free any resources used by the encryption code.
 */
void cipherCleanup(CipherContext *this)
{
	if (this->ctx != NULL)
		EVP_CIPHER_CTX_free(this->ctx);
	this->ctx = NULL;
	if (this->cipher != NULL)
		EVP_CIPHER_free(this->cipher);
	this->cipher = NULL;
}



/*
 * Decrypt one buffer of ciphertext, generating one (slightly smaller?) buffer of plain text.
 * This routine implements a generic AEAD interface.
 *  @param this - cipher context
 *  @param plainText - the text to be encrypted.
 *  @param blockSize - size of the plain text buffer
 *  @param header - text to be authenticated but not encrypted.
 *  @param headerSize - size of header. If 0, then header can be NULL.
 *  @param cipherText - the output encrypted text
 *  @param cryptSize - the size of the encrypted data, same as blockSize if no padding.
 *  @param iv - the initialization vector
 *  @param tag - the output MAC tag of size this->tagSize
 *  @returns size of the decrpyted data, or -1 if error
 */
ssize_t
cipherDecrypt(CipherContext *this,
			  Byte *plainText, size_t plainSize,
			  Byte *header, size_t headerSize,
			  Byte *cipherText, size_t cipherSize,
			  Byte *iv, Byte *tag)
{
	int plainUpdateSize;
	int plainFinalSize;
	ssize_t actual;

	file_debug("cipherSize=%zd   cipherText=%.128s ", cipherSize,  asHex(cipherText, cipherSize));
	file_debug("    headerSize=%zd  header=%s  tagSize=%zd  tag=%s",
			   headerSize, asHex(header, headerSize), this->tagSize, asHex(tag, this->tagSize));
	file_debug("    iv=%s", asHex(iv, this->ivSize));

	/* Reinitialize the encryption context to start a new record */
	EVP_CIPHER_CTX_reset(this->ctx);

	/* Configure the cipher with key and initialization vector */
	if (!EVP_DecryptInit_ex2(this->ctx, this->cipher, this->key, iv, NULL))
		return cypherSetSslError(this, -1);

	/* Set the MAC tag we need to match */
	if (!EVP_CIPHER_CTX_ctrl(this->ctx, EVP_CTRL_AEAD_SET_TAG, (int)this->tagSize, tag))
		return cypherSetSslError(this, -1);

	/* Include the header, if any, in the digest */
	if (headerSize > 0)
	{
		int zero = 0;
		if (!EVP_DecryptUpdate(this->ctx, NULL, &zero, header, (int)headerSize))
			return cypherSetSslError(this, -1);
	}

	/* Decrypt the body if any. We have two pieces: update and final. */
	plainUpdateSize = 0;
	if (cipherSize > 0)
	{
		plainUpdateSize = (int)plainSize;
		if (!EVP_DecryptUpdate(this->ctx, plainText, &plainUpdateSize, cipherText, (int)cipherSize))
			return cypherSetSslError(this, -1);
	}

	/* Finalise the decryption. This can, but probably won't, generate plaintext. */
	plainFinalSize = (int)plainSize - plainUpdateSize; /* CipherFinal expects "int" */
	if (!EVP_DecryptFinal_ex(this->ctx, plainText + plainUpdateSize, &plainFinalSize))
		return cypherSetSslError(this, -1);

	/* Plaintext size combines the update part of the encryption and the finalization. */
	actual = plainUpdateSize + plainFinalSize;

	/* We do not support padded encryption, so the encrypted size should match the plaintext size. */
	if (cipherSize != actual)
		return cipherSetMsg(this, -1, "Decryption doesn't support padding  (%zd bytes)", actual - cipherSize);

	file_debug("plainActual=%zd plainText='%.*s'", actual, (int)actual, asHex(plainText, actual));
	return actual;
}


/*
 * Encrypt one buffer of plain text, generating one buffer of cipher text.
 *  @param this - aaed converter
 *  @param plainText - the text to be encrypted.
 *  @param blockSize - size of the text to be encrypted
 *  @param header - text to be authenticated but not encrypted.
 *  @param headerSize - size of header. If 0, then header can be NULL.
 *  @param cipherText - the output encrypted text
 *  @param cypberSize - the size of the buffer on output.
 *  @param tag - the output MAC tag of size this->tagSize
 *  @param iv - the initialization vector
 *  @return - the actual size of the encrypted ciphertext or -1 on error.
 */
ssize_t
cipherEncrypt(CipherContext *this,
			  const Byte *plainText, size_t plainSize,
			  Byte *header, size_t headerSize,
			  Byte *cipherText, size_t cipherSize,
			  Byte *iv, Byte *tag)
{
	int cipherUpdateSize;
	int cipherFinalSize;
	ssize_t actual;
	file_debug("blockSize=%zd   plainText='%s'", plainSize, asHex(plainText, plainSize));
	file_debug("    headerSize=%zd  header=%s", headerSize, asHex(header, headerSize));
	file_debug("    iv=%s", asHex(iv, this->ivSize));

	/* Reinitialize the encryption context to start a new block of data */
	EVP_CIPHER_CTX_reset(this->ctx);

	/* Configure the cipher with the key and IV */
	if (!EVP_EncryptInit_ex2(this->ctx, this->cipher, this->key, iv, NULL))
		return cypherSetSslError(this, -1);

	/* Include the header, if any, in the digest */
	if (headerSize > 0)
	{
		int zero = 0;
		if (!EVP_EncryptUpdate(this->ctx, NULL, &zero, header, (int)headerSize))
			return cypherSetSslError(this, -1);
	}

	/* Encrypt the plaintext if any. */
	cipherUpdateSize = 0; /* CipherUpdate expects "int" */
	if (plainSize > 0)
	{
		cipherUpdateSize = (int)cipherSize;
		if (!EVP_EncryptUpdate(this->ctx, (Byte *)cipherText, &cipherUpdateSize, plainText, (int)plainSize))
		    return cypherSetSslError(this, -1);
	}

	/* Finalise the plaintext encryption. This can generate data, usually padding, even if there is no plain text. */
	cipherFinalSize = (int)cipherSize - cipherUpdateSize;
	if (!EVP_EncryptFinal_ex(this->ctx, (Byte *)cipherText + cipherUpdateSize, &cipherFinalSize))
	    return cypherSetSslError(this, -1);

	/* Get the authentication tag  */
	if (!EVP_CIPHER_CTX_ctrl(this->ctx, EVP_CTRL_AEAD_GET_TAG, (int)this->tagSize, tag))
		return cypherSetSslError(this, -1);

	/* Output size combines both the encyption (update) and the finalization. */
	actual = cipherUpdateSize + cipherFinalSize;
	file_debug("tag=%s cryptSize=%zd cipherText=%s", asHex(tag, this->tagSize), actual, asHex(cipherText, actual));

	/* We do not support padded encryption, so the encrypted size should match the plaintext size. */
	if (actual != plainSize)
		return cipherSetMsg(this, -1, "Encryption doesn't support padding (%zd bytes)", actual - plainSize);

	return actual;
}

/*
 * Get the error message for the last encryption error.
 */
const char *cipherGetMsg(CipherContext *this)
{
	return this->msg;
}

/*
 * Get the size of the initialization vector for this cipher.
 */
size_t cipherGetIVSize(CipherContext *this)
{
	return this->ivSize;
}

size_t cipherGetTagSize(CipherContext *this)
{
	return this->tagSize;
}

/*
 * Set an encryption error message.
 */
ssize_t cipherSetMsg(CipherContext *this, ssize_t retval, const char *msg, ...)
{
	va_list args;
	va_start(args, msg);
	vsnprintf(this->msg, sizeof(this->msg), msg, args);
	va_end(args);
	file_debug("msg=%s", this->msg);
	return retval;
}

ssize_t cypherSetSslError(CipherContext *this, ssize_t retval)
{
	int sslCode;
	int lastCode;

	/* Get the latest error code */
	for (lastCode = 0; (sslCode = ERR_get_error()) != 0; lastCode = sslCode)
		;

	/* Some errors aren't reported properly by openssl. eg. mismatched MAC on decryption */
	if (lastCode == 0)
		return cipherSetMsg(this, -1, "OpenSSL error: Unrecognized error (corrupt decryption?)");

	return cipherSetMsg(this, retval, "OpenSSL error: %s", ERR_error_string(lastCode, NULL));
}

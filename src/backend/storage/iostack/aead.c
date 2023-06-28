/*
 *
 * TODO: integrate with postgres encryption
 * TODO: consider allocating blockSize header to keep things aligned.
 * TODO: option to skip MAC so block sizes don't change. (aids alignment)
 */
//#define DEBUG
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>

#include <openssl/opensslv.h>
#include <openssl/ssl.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/rand.h>

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
off_t cryptOffset(Aead *this, off_t plainOffset);
size_t cryptSize(Aead *this, size_t plainSize);
static Aead *aeadCleanup(Aead *this);


/**
 * Converter structure for encrypting and decrypting TLS Blocks.
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
    off_t fileSize;               /* Unencrypted size of the file */
	off_t cryptFileSize;          /* size of encrypted file */
	Byte *cryptBuf;               /* Buffer to hold the current encrypted block */
    size_t cryptSize;             /* The size of the encrypted blocks */
	Byte *plainBuf;               /* A buffer to temporarily hold a decrypted block. */
	size_t plainSize;             /* The size of the decrypted blocks */

	bool writable;
};


/**
 * Open an encrypted file.
 * @param path - the path or file name.
 * @param oflag - the open flags, say O_RDONLY or O_CREATE.
 * @param mode - if creating a file, the permissions.
 * @return - Error status.
 */
static Aead *aeadOpen(Aead *proto, const char *path, int oflags, int mode)
{
    size_t lastBlockSize;
    size_t lastPlainSize;
    off_t lastPlainBlock;

    /* Open our successor and clone ourself */
	IoStack *next = stackOpen(nextStack(proto), path, oflags, mode);
	Aead *this = aeadNew(proto->cipherName, proto->suggestedSize, proto->key, proto->keySize, proto->getSequenceNr, next);
	this->ioStack.openVal = next->openVal;
	if (next->openVal < 0)
		return aeadCleanup(this);

	/* Are we open for writing? */
	this->writable = (oflags & O_ACCMODE) != O_RDONLY;

	/* Configure the encryption cipher */
	if (!cipherSetup(this->cipher, this->cipherName, this->key, this->keySize))
	{
		setIoStackError(this, -1, "Unable to setup cipher %s: %s", this->cipherName, cipherGetMsg(this->cipher));
		return aeadCleanup(this);
	}

	/* Our encrypted buffer must include tag and sequence, and it must match our successor's block size */
	this->cryptSize = ROUNDOFF(this->suggestedSize + cipherGetTagSize(this->cipher) + sizeof(uint64), next->blockSize);

	/* Our plaintext buffer must be same size as encrypted buffer, minus tag, minus sequence */
	this->plainSize = this->cryptSize - cipherGetTagSize(this->cipher) - sizeof(uint64);
	debug("aeadOpen: suggested=%zd cryptSize=%zd, plainSize=%zd nextSize=%zd\n", this->suggestedSize, this->cryptSize, this->plainSize, next->blockSize);

	/* Allocate our own buffers based on the encryption config */
	this->cryptBuf = malloc(this->cryptSize);
	if (this->cryptBuf == NULL)
	{
		checkSystemError(this, -1, "Unable to allocate encryption buffer of size %zd", this->cryptSize);
		return aeadCleanup(this);
	}

	/* Allocate a plain text buffer. This is only used to truncate on non-block boundaries.  Will be eliminated */
	this->plainBuf = malloc(this->plainSize);
	if (this->plainBuf == NULL)
	{
		checkSystemError(this, -1, "Unable to allocate plaintext buffer of size %zd", this->plainSize);
		return aeadCleanup(this);
	}

	/* Get the size of the newly opened encrypted file. */
	this->cryptFileSize = stackSize(nextStack(this));
	if (this->cryptFileSize < 0)
	{
		copyError(this, -1, nextStack(this));
		return aeadCleanup(this);
	}

	/* A completely empty file must be writable */
	if (!this->writable && this->cryptFileSize == 0)
	{
		setIoStackError(this, -1, "Existing Encrypted file %s is empty", path);
		return aeadCleanup(this);
	}

	/* If encrypted file has data, verify it ends in a partial block */
	lastBlockSize = this->cryptFileSize % this->cryptSize;
	debug("aeadOpen: cryptFileSize=%lld, cryptSize=%zd, lastBlockSize=%zd\n", this->cryptFileSize, this->cryptSize, lastBlockSize);
	if (this->cryptFileSize > 0 && lastBlockSize < cipherGetTagSize(this->cipher) + sizeof(uint64))
	{
		setIoStackError(this, -1, "Existing Encrypted file %s must end in a valid partial block", path);
		return aeadCleanup(this);
	}

	/* Calculate the plaintext size of the file */
	lastPlainSize = lastBlockSize - cipherGetTagSize(this->cipher) - sizeof(uint64);
	lastPlainBlock = this->cryptFileSize / this->cryptSize * this->plainSize;
	this->fileSize = (this->cryptFileSize == 0)? 0: lastPlainBlock + lastPlainSize;

	/* Done. Return includes top of stack along with openval and block size */
	thisStack(this)->blockSize = this->plainSize;
	return this;
}


/**
 * Read a block of encrypted data into our internal buffer, placing plaintext into the caller's buffer.
 */
static ssize_t aeadRead(Aead *this, Byte *buf, size_t size, off_t offset)
{
    off_t cipherOffset;
    ssize_t actual;
    size_t cipherTextSize;
    Byte *sequenceBuf;
    Byte *tag;
    uint64_t sequenceNr;
    size_t blockNr;
    Byte iv[EVP_MAX_IV_LENGTH];
    ssize_t plainSize;

    debug("aeadRead: size=%zd  offset=%lld\n", size, offset);

	/* Check for EOF first */
	this->ioStack.eof = (offset >= this->fileSize);
	if (this->ioStack.eof)
		return 0;

	/* All reads must be aligned */
	if (offset < 0 || offset % this->plainSize != 0)
		return (setIoStackError(this, -1, "Encryption: read from offset (%lld) not aligned (%zd)", (long long)offset, this->plainSize), -1);

	/* Read one block at a time. */
	size = MIN(size, this->plainSize);

	Assert(offset % thisStack(this)->blockSize == 0);

	/* Translate our offset to our successor's offset */
	cipherOffset = cryptOffset(this, offset);

    /* Read a block of downstream encrypted text into our buffer. */
    actual = stackReadAll(nextStack(this), this->cryptBuf, this->cryptSize, cipherOffset);
	if (actual <= 0)
		return copyNextError(this, actual);
	if (actual < sizeof(uint64) + cipherGetTagSize(this->cipher))
		return setIoStackError(this, -1, "Encryption: file has corrupt block at end");

	/* Point to the sequence number, encrypted text and tag in our buffer */
	cipherTextSize = actual - cipherGetTagSize(this->cipher) - sizeof(uint64);
    sequenceBuf = this->cryptBuf + cipherTextSize;
	tag = sequenceBuf + cipherTextSize;

    /* Generate an IV based on block nr and sequence nr */
	blockNr = (offset / this->plainSize);
	sequenceNr = unpackInt64(sequenceBuf);
	generateIV(this, iv, blockNr, sequenceNr);

	/* Decrypt the ciphertext into our plaintext buffer */
	plainSize = cipherDecrypt(this->cipher,
									  buf, size,
									  sequenceBuf, sizeof(uint64),
									  this->cryptBuf, cipherTextSize,
									  iv, tag);
	if (plainSize < 0)
		return setIoStackError(this, -1, "Unable to decrypt: %s", cipherGetMsg(this->cipher));

	/* Track our position for EOF handling */
	this->fileSize = MAX(this->fileSize, offset + plainSize);
	this->cryptFileSize = MAX(this->cryptFileSize, cipherOffset + cipherTextSize);

    /* Return the number of plaintext bytes read. */
	this->ioStack.eof = (plainSize == 0);
    return plainSize;
}

/**
 * Encrypt data into our internal buffer and write to the output file.
 *   @param buf - data to be converted.
 *   @param size - number of bytes to be converted.
 *   @param error - error status, both input and output.
 *   @returns - number of bytes actually used.
 */
static size_t aeadWrite(Aead *this, const Byte *buf, size_t size, off_t offset)
{
    size_t cryptSize;
    Byte *sequenceBuf;
    Byte *tagBuf;
    uint64 sequenceNr;
    uint64 blockNr;
    Byte iv[EVP_MAX_IV_LENGTH];
    ssize_t encryptSize;
    off_t cipherOffset;

    debug("aeadWrite: size=%zd  offset=%lld\n", size, offset);
	Assert(offset >= 0);

	/* All writes must be aligned */
	if (offset % this->plainSize != 0)
		return (setIoStackError(this, -1, "Encryption: write to offset (%lld) not aligned (%zd)", (long long)offset, this->plainSize), -1);

	/* Writing a partial block before end of file would cause corruption in the file */
	if (size < this->plainSize && offset+size < this->fileSize)
		return (setIoStackError(this, -1, "Encryption: partial block before end of file causes corruption"), -1);

	/* Limit encrytion to one block at a time */
	size = MIN(size, this->plainSize);

	/* Point to where the sequence nr and the tag will reside in the encrypted buffer */
	cryptSize = size + cipherGetTagSize(this->cipher) + sizeof(uint64);
	sequenceBuf = this->cryptBuf + size;
	tagBuf = sequenceBuf + sizeof(uint64);

	/* Fill in the sequence number in the encrypted buffer. */
	sequenceNr = this->getSequenceNr();
	packInt64(sequenceBuf, sequenceNr);

	/* Generate an IV based on block nr and sequence nr */
	blockNr = (offset / this->plainSize);
	generateIV(this, iv, blockNr, sequenceNr);

	/* Encrypt the block of data into our encryption buffer, generating a tag */
    encryptSize = cipherEncrypt(this->cipher,
									   buf, size,
									   sequenceBuf, sizeof(uint64), /* Do we benefit from including this? */
									   this->cryptBuf, cryptSize,
									   iv, tagBuf);
	if (encryptSize < 0)
		return setIoStackError(this, -1, "Unable to encrypt: %s", cipherGetMsg(this->cipher));
	if (encryptSize != size)
		return setIoStackError(this, 1, "Encrypted size mismatch: got %zd expected %zd", encryptSize, size);

	/* Translate our plaintext offset to our successor's ciphertext offset */
	cipherOffset = cryptOffset(this, offset);

    /* Write the encrypted block out */
    if (stackWriteAll(nextStack(this), this->cryptBuf, cryptSize, cipherOffset) != cryptSize)
	    return copyNextError(this, -1);

	/* Track file sizes */
	this->fileSize = MAX(this->fileSize, offset + size);
	this->cryptFileSize = MAX(this->cryptFileSize, cipherOffset + cryptSize);

    return size;
}

/**
 * Close this encryption stack releasing resources.
 */
static ssize_t aeadClose(Aead *this)
{
	debug("aeadClose: openVal=%zd fileSize=%lld cryptFileSize=%lld\n", this->ioStack.openVal, this->fileSize, this->cryptFileSize);

	/*
	 * If not already done, add a partial (empty) block to mark the end of encrypted data.
	 */
	if (this->writable && this->fileSize % this->plainSize == 0 && this->cryptFileSize % this->cryptSize == 0)
		aeadWrite(this, NULL, 0, this->fileSize);  /* sets errno and msg */

	/* Release resources, including closing the downstream file */
	aeadCleanup(this);
	debug("aeadClose(done): code=%d msg=%s\n", stackErrorCode(this), stackErrorMsg(this));
	return stackError(this)? -1: 0;
}


/*
 * Return the size of the plaintext file.
 */
static off_t aeadSize(Aead *this)
{
	debug("aeadSize (done) fileSize=%lld  cryptFileSiz=%lld\n", this->fileSize, this->cryptFileSize);
	return this->fileSize;
}


/*
 * Redefine to truncate at the beginning of a block??? Much simpler, and let buffering handle the rest.
 */
static ssize_t aeadTruncate(Aead *this, off_t offset)
{
    ssize_t retval;
	/* TODO: only truncate on a block boundary */
	/* If truncating at a partial block, read in the block being truncated */
	off_t blockOffset = ROUNDDOWN(offset, this->plainSize);
	if (blockOffset != offset)
	{
		ssize_t blockSize = aeadRead(this, this->plainBuf, this->plainSize, blockOffset);
		if (blockSize < 0)
			return blockSize;

		/* Make sure the block is big enough to be truncated */
		if (blockSize < offset-blockOffset)
			return setIoStackError(this, -1, "AeadTruncate - file is smaller than truncate offset");
	}

	/* Set the new file size to the block boundary */
	this->fileSize = blockOffset;
	this->cryptFileSize = cryptOffset(this, this->fileSize);

	/* Truncate the downstream file to match the new sizes */
	retval = stackTruncate(nextStack(this), this->cryptFileSize);
	if (retval < 0)
		return copyNextError(this, retval);

	/* If we have a partial block, then write it out */
	if (offset != blockOffset)
	    if (aeadWrite(this, this->plainBuf, offset-blockOffset, blockOffset) < 0)
		    return -1;

	return 0;
}


static ssize_t aeadSync(Aead *this)
{
	/* Sync the downstream file */
	int retval = stackSync(nextStack(this));
	if (retval < 0)
	    copyNextError(this, retval);
	return retval;
}

/**
 * Abstract interface for the encryption ioStack.
 */
IoStackInterface aeadInterface = {
	.fnOpen = (IoStackOpen) aeadOpen,
	.fnRead = (IoStackRead) aeadRead,
	.fnWrite = (IoStackWrite) aeadWrite,
	.fnClose = (IoStackClose) aeadClose,
	.fnTruncate = (IoStackTruncate) aeadTruncate,
	.fnSize = (IoStackSize) aeadSize,
	.fnSync = (IoStackSync) aeadSync,
};

/*
 * Create a new aead encryption/decryption filter
 */
void *aeadNew(char *cipherName, size_t suggestedSize, Byte *key, size_t keySize, uint64 (*getSequenceNr)(), void *next)
{
    size_t cryptSize;
    size_t plainSize;
    Aead *this;

	/* Pick a default cipher name */
	if (cipherName == NULL)
		cipherName = "AES-256-GCM";

	/* Our cipher block size must be a multiple of the next layer's block size */
	/* TODO: We haven't initialized the cipher yet, so assume the tag is 16 bytes */
	cryptSize = ROUNDOFF(suggestedSize + 16 + sizeof(uint64), thisStack(next)->blockSize);
	plainSize = cryptSize - 16 - sizeof(uint64);

	/* Create the aead structure */
    this = malloc(sizeof(Aead));
	*this = (Aead) {
		.suggestedSize = suggestedSize,
		.keySize = keySize,
		.getSequenceNr = getSequenceNr,
		.ioStack = (IoStack) {
			.iface = &aeadInterface,
			.next = next,
			.blockSize = plainSize,
		}
	};

    /* Copy in the key and cipher name without overwriting memory. We'll validate later. */
    memcpy(this->key, key, MIN(keySize, sizeof(this->key)));
    strlcpy(this->cipherName, cipherName, sizeof(this->cipherName));

    return thisStack(this);
}

/*
 * Create an Initialization Vector (IV)
 */
void generateIV(Aead *this, Byte *iv, uint64 blockNr, uint64 sequenceNr)
{
	Assert(blockNr < ((uint64)1) << 32);
	Assert(cipherGetIVSize(this->cipher) == 12);

	packInt32(iv, blockNr);
	packInt64(iv+4, sequenceNr);
	debug("generateIV  blockNr=%zd  sequenceNr=%zd iv=%s\n", blockNr, sequenceNr, asHex(iv, 12));
}

/*
 * Calculate the size of an encrypted block given the size of the plaintext block.
 * Note our encryption algorithms do not use padding.
 */
size_t cryptSize(Aead *this, size_t plainSize)
{
	return plainSize + cipherGetTagSize(this->cipher) + sizeof(uint64);
}

/*
 * Calculate the file offset of an encrypted block, given the file offset of a plaintext block.
 * Note this only works for block boundaries.
 */
off_t cryptOffset(Aead *this, off_t plainOffset)
{
	return plainOffset / this->plainSize * this->cryptSize;
}


static Aead *aeadCleanup(Aead *this)
{
	IoStack *next = this->ioStack.next;

	/* Close the downstream layer if opened */
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

	/* Free the cipher structures */
	cipherCleanup(this->cipher);

	return this;
}




/**
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

	/* Get the properties of the selected cipher */
	this->ivSize = EVP_CIPHER_iv_length(this->cipher);
	if (keySize != EVP_CIPHER_key_length(this->cipher))
		return setIoStackError(this, false, "Cipher key is the wrong size");
	this->cipherBlockSize = EVP_CIPHER_block_size(this->cipher);
	//this->tagSize = EVP_CIPHER_CTX_tag_length(this->ctx);
	this->tagSize = 16;

	/* Save the key */
	memcpy(this->key, key, keySize);
	this->keySize = keySize;

	debug("cipherSetup ivSize=%zd  keySize=%zd  blockSize=%zd  tagSize=%zd\n",
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
 *  @param plainSize - size of the plain text buffer
 *  @param header - text to be authenticated but not encrypted.
 *  @param headerSize - size of header. If 0, then header can be NULL.
 *  @param cipherText - the output encrypted text
 *  @param cryptSize - the size of the encrypted data
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
	debug("Decrypt:  cipherSize=%zd   cipherText=%.128s \n", cipherSize,  asHex(cipherText, cipherSize));
	debug("    headerSize=%zd  header=%s\n", headerSize, asHex(header, headerSize));
	debug("    iv=%s\n", asHex(iv, this->ivSize));

	/* Reinitialize the encryption context to start a new record */
	EVP_CIPHER_CTX_reset(this->ctx);

	/* Configure the cipher with key and initialization vector */
	if (!EVP_CipherInit_ex2(this->ctx, this->cipher, this->key, iv, 0, NULL))
		return cypherSetSslError(this, -1);

	/* Set the MAC tag we need to match */
	if (!EVP_CIPHER_CTX_ctrl(this->ctx, EVP_CTRL_AEAD_SET_TAG, (int)this->tagSize, tag))
		return cypherSetSslError(this, -1);

	/* Include the header, if any, in the digest */
	if (headerSize > 0)
	{
		int zero = 0;
		if (!EVP_CipherUpdate(this->ctx, NULL, &zero, header, (int)headerSize))
			return cypherSetSslError(this, -1);
	}

	/* Decrypt the body if any. We have two pieces: update and final. */
	plainUpdateSize = 0;
	if (cipherSize > 0)
	{
		plainUpdateSize = (int)plainSize;
		if (!EVP_CipherUpdate(this->ctx, plainText, &plainUpdateSize, cipherText, (int)cipherSize))
			return cypherSetSslError(this, -1);
	}

	/* Finalise the decryption. This can, but probably won't, generate plaintext. */
	plainFinalSize = (int)plainSize - plainUpdateSize; /* CipherFinal expects "int" */
	if (!EVP_CipherFinal_ex(this->ctx, plainText + plainUpdateSize, &plainFinalSize) && ERR_get_error() != 0)
		return cypherSetSslError(this, -1);

	/* Output plaintext size combines the update part of the encryption and the finalization. */
	actual = plainUpdateSize + plainFinalSize;

	/* We do not support padded encryption, so the encrypted size should match the plaintext size. */
	if (cipherSize != actual)
		return setIoStackError(this, -1, "Decryption doesn't support padding  (%zd bytes)", actual - cipherSize);

	debug("Decrypt:  plainActual=%zd plainText='%.*s'\n", actual, (int)actual, plainText);
	return actual;
}


/*
 * Encrypt one buffer of plain text, generating one buffer of cipher text.
 *  @param this - aaed converter
 *  @param plainText - the text to be encrypted.
 *  @param plainSize - size of the text to be encrypted
 *  @param header - text to be authenticated but not encrypted.
 *  @param headerSize - size of header. If 0, then header can be NULL.
 *  @param cipherText - the output encrypted text
 *  @param cryptSize - the size of the buffer on input, actual size on output.
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
	debug("Encrypt: plainSize=%zd   plainText='%.*s'\n",
		  plainSize, (int)plainSize, plainText);
	debug("    headerSize=%zd  header=%s\n", headerSize, asHex(header, headerSize));
	debug("    iv=%s\n", asHex(iv, this->ivSize));

	/* Reinitialize the encryption context to start a new block of data */
	EVP_CIPHER_CTX_reset(this->ctx);

	/* Configure the cipher with the key and IV */
	if (!EVP_CipherInit_ex2(this->ctx, this->cipher, this->key, iv, 1, NULL))
		return cypherSetSslError(this, -1);

	/* Include the header, if any, in the digest */
	if (headerSize > 0)
	{
		int zero = 0;
		if (!EVP_CipherUpdate(this->ctx, NULL, &zero, header, (int)headerSize))
			return cypherSetSslError(this, -1);
	}

	/* Encrypt the plaintext if any. */
	cipherUpdateSize = 0; /* CipherUpdate expects "int" */
	if (plainSize > 0)
	{
		cipherUpdateSize = (int)cipherSize;
		if (!EVP_CipherUpdate(this->ctx, (Byte *)cipherText, &cipherUpdateSize, plainText, (int)plainSize))
			return cypherSetSslError(this, -1);
	}

	/* Finalise the plaintext encryption. This can generate data, usually padding, even if there is no plain text. */
	cipherFinalSize = (int)cipherSize - cipherUpdateSize;
	if (!EVP_CipherFinal_ex(this->ctx, (Byte *)cipherText + cipherUpdateSize, &cipherFinalSize))
		return cypherSetSslError(this, -1);

	/* Get the authentication tag  */
	if (!EVP_CIPHER_CTX_ctrl(this->ctx, EVP_CTRL_AEAD_GET_TAG, (int)this->tagSize, tag))
		return cypherSetSslError(this, -1);
	debug("Encrypt: tag=%s cryptSize=%d cipherText=%.128s \n", asHex(tag, this->tagSize), cipherUpdateSize+cipherFinalSize, asHex(cipherText, cipherUpdateSize + cipherFinalSize));

	/* Output size combines both the encyption (update) and the finalization. */
	actual = cipherUpdateSize + cipherFinalSize;

	/* We do not support padded encryption, so the encrypted size should match the plaintext size. */
	if (actual != plainSize)
		return setIoStackError(this, -1, "Encryption doesn't support padding (%zd bytes)", actual - plainSize);

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
	return retval;
}

ssize_t cypherSetSslError(CipherContext *this, ssize_t retval)
{
	return cipherSetMsg(this, retval, "OpenSSL error: %s", ERR_error_string(ERR_get_error(), NULL));
}

/**/
#include <stdlib.h>
#include "storage/iostack_internal.h"
#include "storage/fd.h"
#include "storage/fileaccess.h"
#include "packed.h"
#include "access/xlog.h"

/* TODO: the following may belong in a different file ... */
/* These need to be set properly at startup */
static Byte *tempKey = (Byte *)"0123456789ABCDEF0123456789ABCDEF";
static size_t tempKeyLen = 32;
static Byte *permKey = (Byte *)"abcdefghijklmnopqrstuvwxyzABCDEF";
static size_t permKeyLen = 32;

/*
 * Function to return a temp file sequence number.
 * The temp numbers are reset after a crash, so the
 * encryption key must also be regenerated after a crash.
 * Any leftover temp files are not valid after a crash and
 * should be deleted.
 */
static uint64 tempSeqNr(void)
{
	return GetFakeLSNForUnloggedRel();
}

/* Function to create a test stack for unit testing */
IoStack *(*testStackNew)() = NULL;

/* Prototype stacks */
static bool ioStacksInitialized = false;
IoStack *ioStackPlain;				/* Buffered, unmodified text */
IoStack *ioStackEncrypt;			/* Buffered and encrypted with a session key */
IoStack *ioStackEncryptPerm;        /* Buffered and encrypted with a session key */
IoStack *ioStackTest;				/* Stack used for unit testing */
IoStack *ioStackRaw;				/* Unbuffered "raw" file access */
IoStack *ioStackCompress;			/* Buffered and compressed */
IoStack *ioStackCompressEncrypt;	/* Compressed and encrypted with session key */

/*
 * Set the stack used for PG_TESTSTACK.
 */
void
setTestStack(IoStack *proto)
{
	ioStackTest = proto;
}

/*
 * Select the appropriate I/O Stack for this particular file.
 * This function provides flexibility in how I/O stacks are created.
 * It can look at open flags, do GLOB matching on pathnames,
 * or create different stacks if reading vs writing.
 * The current version looks for special "PG_*" open flags.
 */
IoStack *
selectIoStack(const char *path, uint64 oflags, mode_t mode)
{
	file_debug("IoStackNew: name=%s  oflags=0x%x  mode=o%o", path, oflags, mode);

	/* Set up the I/O prototype stacks if not already done */
	if (!ioStacksInitialized)
		ioStackSetup();

	/* Look at oflags to determine which stack to use */
	switch (oflags & PG_STACK_MASK)
	{
		case PG_PLAIN:            return ioStackPlain;
		case PG_ENCRYPT:          return ioStackEncrypt;
		case PG_ENCRYPT_PERM:     return ioStackEncryptPerm;
		case PG_TESTSTACK:        return ioStackTest;
		case PG_RAW:              return ioStackRaw;
		case 0:                   file_debug("Default I/O stack: path=%s oflags=0x%llx", path, oflags); return ioStackRaw;

		default: Assert(false); elog(FATAL, "Unrecognized I/O Stack oflag 0x%llx", (oflags & PG_STACK_MASK));
	}
}


/*
 * Initialize the I/O stack infrastructure. Creates prototype stacks.
 */
void ioStackSetup(void)
{
	/* Set up the prototype stacks */
	ioStackRaw = vfdStackNew();
	ioStackPlain = bufferedNew(8*1024, vfdStackNew());
	ioStackEncrypt = bufferedNew(1, aeadNew("AES-256-GCM", 8 * 1024, tempKey, tempKeyLen, tempSeqNr, vfdStackNew()));

#ifdef NOTYET
	ioStackEncryptPerm = bufferedNew(1, aeadNew("AES-256-GCM", 16 * 1024, permKey, permKeyLen, vfdStackNew()));
	ioStackCompress = bufferedNew(1, lz4CompressNew(64 * 1024, vfdStackNew(), vfdStackNew()));
	ioStackCompressEncrypt = bufferedNew(1,
										 lz4CompressNew(16*1024,
														bufferedNew(1,
																	aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen,
																			vfdStackNew())),
														bufferedNew(1,
																	aeadNew("AES-256-GCM", 16 * 1024, tempKey, tempKeyLen,
																	vfdStackNew()))));
#endif
	/* Note we are now initialized */
	ioStacksInitialized = true;
}

/*
 * Write an entire buffer, making multiple writes if necessary.
 */
ssize_t stackWriteAll(void *thisVoid, const Byte *buf, size_t size, off_t offset, uint32 wait)
{
	IoStack *this = thisVoid;

	/* Repeat until the entire buffer is written (or error) */
	ssize_t total, current;
	for (total = 0; total < size; total += current)
	{
		/* Do a partial write */
		current = stackWrite(this, buf + total, size - total, offset + total, wait);
		if (current <= 0)
			break;
	}

	/* Check for errors. Return the same error state as fileWrite. */
	if (current < 0)
		total = current;

	return total;
}


ssize_t stackReadAll(void *thisVoid, Byte *buf, size_t size, off_t offset, uint32 wait)
{
	IoStack *this = thisVoid;

	/* Repeat until the entire buffer is read (or EOF or error) */
	ssize_t total, current;
	for (total = 0; total < size; total += current)
	{
		/* if we read a partial block, then we are done */
		if (total % thisStack(this)->blockSize != 0)
			break;

		/* Do the next read. If eof or error, then done */
		current = stackRead(this, buf + total, size - total, offset + total, wait);
		if (current <= 0)
			break;
	}

	/* Check for errors.. */
	if (current < 0)
		total = current;

	this->eof = (total == 0);

	return total;
}


/*
 * Write a 4 byte int in network byte order (big endian).
 */
bool stackWriteInt32(IoStack *this, uint32_t data, off_t offset, uint32 wait)
{
	static Byte buf[4];

	file_debug("stackWriteInt32: data=%d  offset=%lld", data, offset);
	buf[0] = (Byte)(data >> 24);
	buf[1] = (Byte)(data >> 16);
	buf[2] = (Byte)(data >> 8);
	buf[3] = (Byte)data;

	return (stackWriteAll(this, buf, 4, offset, wait) == 4);
}

/*
 * Read a 4 byte int in network byte order (big endian)
 */
bool stackReadInt32(IoStack *this, uint32_t *data, off_t offset, uint32 wait)
{
	Byte buf[4];
	if (stackReadAll(this, buf, 4, offset, wait) != 4)
		return false;

	*data = buf[0] << 24 | buf[1] << 16 | buf[2] << 8 | buf[3];
	file_debug("stackReadInt32: data=%d  offset=%lld", *data, offset);
	return true;
}


ssize_t stackWriteSized(IoStack *this, const Byte *buf, size_t size, off_t offset, uint32 wait)
{
	Assert(size <= MAX_BLOCK_SIZE);

	/* Output the length field first */
	if (!stackWriteInt32(this, size, offset, wait))
		return -1;

	/* Write out the data */
	return stackWriteAll(this, buf, size, offset + 4, wait);
}


/*
 * Read data from a sized record (size folllowed by data).
 * A zero return can mean either a zero length record or EOF,
 * so it may be necessary to invoke stackEof() or FileEof()
 * to fully detect an EOF condition.
 */
ssize_t stackReadSized(IoStack *this, Byte *buf, size_t size, off_t offset, uint32 wait)
{
	uint32_t expected;
	ssize_t ret;
	ssize_t actual;

	/* Read the length. Return immediately if EOF or error */
	Assert(size <= MAX_BLOCK_SIZE);
	ret = stackReadInt32(this, &expected, offset, wait);
	if (ret <= 0)
		return ret;

	/* Validate the length */
	if (expected > MAX_BLOCK_SIZE)
		return stackSetError(this, -1, "IoStack record length of %x is larger than %z", expected, size);

	/* read the data, including the possiblility of a zero length record. */
	actual = stackReadAll(this, buf, expected, offset + 4, wait);
	if (actual >= 0 && actual != expected)
		return stackSetError(this, -1, "IoStack record corrupted. Expected %z bytes but read only %z byres", expected, actual);

	return actual;
}


bool stackWriteInt64(IoStack *this, uint64_t data, off_t offset, uint32 wait)
{
	static Byte buf[8];

	file_debug("stackWriteInt64: data=%lld  offset=%lld", data, offset);
	/* TODO: unpack into the buffer */
	buf[0] = (Byte)(data >> 56);
	buf[1] = (Byte)(data >> 48);
	buf[2] = (Byte)(data >> 40);
	buf[3] = (Byte)(data >> 32);
	buf[4] = (Byte)(data >> 24);
	buf[5] = (Byte)(data >> 16);
	buf[6] = (Byte)(data >> 8);
	buf[7] = (Byte)data;

	return (stackWriteAll(this, buf, 8, offset, wait) == 8);
}

/*
 * Read an 8 byte int in network byte order (big endian)
 */
bool stackReadInt64(IoStack *this, uint64_t *data, off_t offset, uint32 wait)
{
	Byte buf[8];
	if (stackReadAll(this, buf, 8, offset, wait) != 8)
		return false;

	*data = (uint64_t)buf[0] << 56 | (uint64_t)buf[1] << 48 | (uint64_t)buf[2] << 40 |
		(uint64_t)buf[3] << 32 | (uint64_t)buf[4] << 24 | (uint64_t)buf[5] << 16 |
		(uint64_t)buf[6] << 8 | (uint64_t)buf[7];
	file_debug("stackReadInt64: data=%lld  offset=%lld", *data, offset);
	return true;
}

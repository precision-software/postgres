/*
 * iostack.c implements helper functions for supporting I/O stacks.
 */
#include <stdlib.h>
#include "storage/iostack_internal.h"

/*
 * Write an entire buffer. Returns the number of bytes written or -1 on error.
 */
ssize_t stackWriteAll(IoStack *this, const Byte *buf, size_t size, off_t offset)
{
	/* Repeat until the entire buffer is written (or error) */
	ssize_t total, current;
	for (total = 0; total < size; total += current)
	{
		/* Do a partial write */
		current = stackWrite(this, buf + total, size - total, offset + total);
		if (current <= 0)
			break;
	}

	/* Check for errors. Return the same error state as fileWrite. */
	if (current < 0)
		total = current;

	return total;
}


/*
 * Read an entire buffer. Returns the number of bytes read or -1 on error.
 */
ssize_t stackReadAll(IoStack *this, Byte *buf, size_t size, off_t offset)
{
	/* Repeat until the entire buffer is read (or EOF or error) */
	ssize_t total, current;
	for (total = 0; total < size; total += current)
	{
		/* if we read a partial block, then we are done */
		if (total % thisStack(this)->blockSize != 0)
			break;

		/* Do the next read. If eof or error, then done */
		current = stackRead(this, buf + total, size - total, offset + total);
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
 * Write a 4 byte int in network byte order (big endian)
 */
bool stackWriteInt32(IoStack *this, uint32_t data, off_t offset)
{
    static Byte buf[4];
	debug("stackWriteInt32: data=%d  offset=%lld\n", data, offset);

	packInt32(buf, data);
	return (stackWriteAll(this, buf, 4, offset) == 4);
}

/*
 * Read a 4 byte int in network byte order (big endian)
 */
bool stackReadInt32(IoStack *this, void *data, off_t offset)
{
	Byte buf[4];
	if (stackReadAll(this, buf, 4, offset) != 4)
		return false;

	*(uint32_t *)data = unpackInt32(buf);
	debug("stackReadInt32: data=%d  offset=%lld\n", *(uint32*)data, offset);
	return true;
}

/*
 * Write a sized record (size followed by data).
 * Returns the number of data bytes written or -1 on error.
 */
ssize_t stackWriteSized(IoStack *this, const Byte *buf, size_t size, off_t offset)
{
	Assert(size <= MAX_BLOCK_SIZE);

	/* Output the length field first */
	if (!stackWriteInt32(this, size, offset))
		return -1;

	/* Write out the data */
	return stackWriteAll(this, buf, size, offset + 4);
}


/*
 * Read data from a sized record (size folllowed by data).
 * A zero return can mean either a zero length record or EOF,
 * so it may be necessary to invoke stackEof() or FileEof()
 * to fully detect an EOF condition.
 */
ssize_t stackReadSized(IoStack *this, Byte *buf, size_t size, off_t offset)
{
    uint32_t expected;
    ssize_t actual;
    ssize_t ret;
	Assert(size <= MAX_BLOCK_SIZE);

	/* Read the length. Return immediately if EOF or error */
	ret = stackReadInt32(this, &expected, offset);
	if (ret <= 0)
		return ret;

	/* Validate the length */
	if (expected > MAX_BLOCK_SIZE)
		return setIoStackError(this, -1, "IoStack record length of %u is larger than %zd", expected, size);

	/* read the data, including the possiblility of a zero length record. */
	actual = stackReadAll(this, buf, expected, offset + 4);
	if (actual >= 0 && actual != expected)
		return setIoStackError(this, -1, "IoStack record corrupted. Expected %u bytes but read only %zd bytes", expected, actual);

	return actual;
}

void packInt32(Byte *buf, uint32 data)
{
	buf[0] = (Byte)(data >> 24);
	buf[1] = (Byte)(data >> 16);
	buf[2] = (Byte)(data >> 8);
	buf[3] = (Byte)data;
}

void packInt64(Byte *buf, uint64 data)
{
	buf[0] = (Byte)(data >> 56);
	buf[1] = (Byte)(data >> 48);
	buf[2] = (Byte)(data >> 40);
	buf[3] = (Byte)(data >> 32);
	buf[4] = (Byte)(data >> 24);
	buf[5] = (Byte)(data >> 16);
	buf[6] = (Byte)(data >> 8);
	buf[7] = (Byte)data;
}

uint64 unpackInt64(Byte *buf)
{
	return (uint64_t)buf[0] << 56 | (uint64_t)buf[1] << 48 | (uint64_t)buf[2] << 40 |
	       (uint64_t)buf[3] << 32 | (uint64_t)buf[4] << 24 | (uint64_t)buf[5] << 16 |
		   (uint64_t)buf[6] << 8  | (uint64_t)buf[7];
}

uint32 unpackInt32(Byte *buf)
{
	return (uint64_t)buf[0] << 24 | (uint64_t)buf[1] << 16 | (uint64_t)buf[2] << 8 | (uint64_t)buf[3];
}

/*
 * Write a 8 byte int in network byte order (big endian)
 */
bool stackWriteInt64(IoStack *this, uint64_t data, off_t offset)
{
    static Byte buf[8];
	debug("stackWriteInt64: data=%lld  offset=%lld\n", data, (long long)offset);

    packInt64(buf, data);
	return (stackWriteAll(this, buf, 8, offset) == 8);
}

/*
 * Read an 8 byte int in network byte order (big endian)
 */
bool stackReadInt64(IoStack *this, void *data, off_t offset)
{
	Byte buf[8];
	if (stackReadAll(this, buf, 8, offset) != 8)
		return false;

	*(uint64_t *)data = unpackInt64(buf);
	debug("stackReadInt64: data=%lu  offset=%lld\n", *(uint64*)data, (long long)offset);
	return true;
}

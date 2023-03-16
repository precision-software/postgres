/**/
#include <stdlib.h>
#include "storage/iostack_internal.h"
#include "packed.h"


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
	debug("stackWriteInt32: data=%d  offset=%lld\n", data, offset);
	static Byte buf[4];
	buf[0] = (Byte)(data >> 24);
	buf[1] = (Byte)(data >> 16);
	buf[2] = (Byte)(data >> 8);
	buf[3] = (Byte)data;

	return (stackWriteAll(this, buf, 4, offset) == 4);
}

/*
 * Read a 4 byte int in network byte order (big endian)
 */
bool stackReadInt32(IoStack *this, uint32_t *data, off_t offset)
{
	Byte buf[4];
	if (stackReadAll(this, buf, 4, offset) != 4)
		return false;

	*data = buf[0] << 24 | buf[1] << 16 | buf[2] << 8 | buf[3];
	debug("stackReadInt32: data=%d  offset=%lld\n", *data, offset);
	return true;
}


ssize_t stackWriteSized(IoStack *this, const Byte *buf, size_t size, off_t offset)
{
	Assert(size <= MAX_BLOCK_SIZE);

	/* Output the length field first */
	if (!stackWriteInt32(this, size, offset))
		return -1;

	/* Write out the data */
	return stackWriteAll(this, buf, size, offset + 4);
}



ssize_t stackReadSized(IoStack *this, Byte *buf, size_t size, off_t offset)
{
	Assert(size <= MAX_BLOCK_SIZE);

	/* Read the length. Return immediately if EOF or error */
	uint32_t expected;
	ssize_t ret = stackReadInt32(this, &expected, offset);
	if (ret <= 0)
		return ret;

	/* Validate the length */
	if (expected > MAX_BLOCK_SIZE)
		return setIoStackError(this, -1, "IoStack record length of %x is larger than %z", expected, size);

	/* read the data, including the possiblility of a zero length record. */
	ssize_t actual = stackReadAll(this, buf, expected, offset + 4);
	if (actual >= 0 && actual != expected)
		return setIoStackError(this, -1, "IoStack record corrupted. Expected %z bytes but read only %z byres", expected, actual);

	return actual;
}


bool stackWriteInt64(IoStack *this, uint64_t data, off_t offset)
{
	debug("stackWriteInt64: data=%lld  offset=%lld\n", data, offset);
	static Byte buf[8];
	buf[0] = (Byte)(data >> 56);
	buf[1] = (Byte)(data >> 48);
	buf[2] = (Byte)(data >> 40);
	buf[3] = (Byte)(data >> 32);
	buf[4] = (Byte)(data >> 24);
	buf[5] = (Byte)(data >> 16);
	buf[6] = (Byte)(data >> 8);
	buf[7] = (Byte)data;

	return (stackWriteAll(this, buf, 8, offset) == 8);
}

/*
 * Read an 8 byte int in network byte order (big endian)
 */
bool stackReadInt64(IoStack *this, uint64_t *data, off_t offset)
{
	Byte buf[8];
	if (stackReadAll(this, buf, 8, offset) != 8)
		return false;

	*data = (uint64_t)buf[0] << 56 | (uint64_t)buf[1] << 48 | (uint64_t)buf[2] << 40 |
		(uint64_t)buf[3] << 32 | (uint64_t)buf[4] << 24 | (uint64_t)buf[5] << 16 |
		(uint64_t)buf[6] << 8 | (uint64_t)buf[7];
	debug("stackReadInt64: data=%lld  offset=%lld\n", *data, offset);
	return true;
}

void fileClearError(void *thisVoid)
{
	IoStack *this = thisVoid;
	this->errNo = 0;
	this->eof = false;
	strcpy(this->errMsg, "");
}

bool stackErrorInfo(void *thisVoid, int *errNo, char *errMsg)
{
	IoStack *this = thisVoid;
	*errNo = errno = this->errNo;
	strcpy(errMsg, this->errMsg);
	return stackError(this);
}


int stackErrorNo(void *thisVoid)
{
	IoStack *this = thisVoid;
	return this->errNo;
}

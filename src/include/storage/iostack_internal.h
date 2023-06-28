/***********************************************************************************************************************************
Header file for developers of I/O Stacks

As a quick prototype, we are NOT doing alloc/free of memory. Consequently,
  - Error messages are preallocated as part of the IoStack structure.
  - Nested errors are not supported yet. (but easily done with dynamic memory allocations)

This is a "header only" file.
***********************************************************************************************************************/
#ifndef FILTER_ERROR_H
#define FILTER_ERROR_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>


#include "storage/iostack.h"

/* A convenient place to enable debug output of I/O stacks. */
//#define DEBUG

/* Upper limit on the block sizes we suport. Basically 16Mb with some extra space, say for nonces. */
#define MAX_BLOCK_SIZE (16*1024*1024)

/* Helpers to access the generic IoStack functions */
#define thisStack(this) ( (IoStack *)this )
#define nextStack(this) ( thisStack(this)->next )

/* Forward references to Error handling functions */
inline static int setError(void *thisVoid, int errNo, const char *fmt, va_list ap) pg_attribute_printf(3, 0);
inline static ssize_t setIoStackError(void *this, size_t retval, const char *fmt, ...) pg_attribute_printf(3,4);
inline static ssize_t checkSystemError(void *thisVoid, ssize_t retval, const char *fmt, ...) pg_attribute_printf(3,4);
inline static ssize_t copyError(void *thisVoid, ssize_t retval, void *thatVoid);
inline static ssize_t copyNextError(void *this, ssize_t retval);

/* Pack and unpack integers in BE (network) byte order. TODO: inline? */
void packInt32(Byte *buf, uint32 data);
uint32 unpackInt32(Byte *buf);
void packInt64(Byte *buf, uint64 data);
uint64 unpackInt64(Byte *buf);

/*
 * Test retval for a system error, returning retval so it can be used in a return statement.
 */
inline static ssize_t checkSystemError(void *thisVoid, ssize_t retval, const char *fmt, ...)
{
	va_list ap;
	IoStack *this = thisVoid;

	/* If a system error occured ... */
	if (retval < 0)
	{
		/* Save the error code */
		int save_errno = errno;

		/* Build up a new format string by prepending the system error info */
		char newFmt[121]; /* TODO: allocate? */
		snprintf(newFmt, sizeof(newFmt), "(%d - %s) %s", save_errno, strerror(errno), fmt);

		/* Using the new format string, format the error info and save it */
		va_start(ap, fmt);
		setError(this, save_errno, newFmt, ap);
		va_end(ap);
	}

	return retval;
}

/*
 * Copy error information from the next lower stack level to the current level.
 */
inline static ssize_t copyNextError(void *this, ssize_t retval)
{
	return copyError(this, retval, nextStack(this));
}

/*
 * Report a new I/O Stack error.
 * Pass through a return value so it can be used in a return statement.
 */
inline static ssize_t setIoStackError(void *this, size_t retval, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
    setError(this, EIOSTACK, fmt, ap);
	va_end(ap);

	return retval;
}

/*
 * Copy error information from one I/O Stack frame to another.
 * Pass through a return value so it can be used in a return statement.
 */
inline static ssize_t copyError(void *thisVoid, ssize_t retval, void *thatVoid)
{
	IoStack *this = thisVoid;
	IoStack *that = thatVoid;
	Assert(this != NULL && that != NULL);
	this->errNo = that->errNo;
	this->eof = stackEof(that);
	strcpy(this->errMsg, that->errMsg);
	return retval;
}

/*
 * Varargs helper function to format an error message and save it in the IoStack.
 * Return -1 so it can be used in a return statement.
 */
inline static int setError(void *thisVoid, int errNo, const char *fmt, va_list ap)
{
	IoStack *this = thisVoid;

	this->errNo = errNo;
	vsnprintf(this->errMsg, sizeof(this->errMsg), fmt, ap);

	/* restore the errno so caller can still test it */
	errno = errNo;
	return -1;
}

/* Some convenient macros. (Subject to the usual problems with macros) */
#ifndef MAX
#define MAX(a,b) ( ((a)>(b))?(a):(b) )
#endif
#ifndef MIN
#define MIN(a,b) ( ((a)<(b))?(a):(b) )
#endif
#define ROUNDDOWN(a,b) ( (a) / (b) * (b))
#define ROUNDUP(a,b)    ROUNDDOWN(a + b - 1, b)

/* Round off to the nearest multiple of b, but never less than b */
#define ROUNDOFF(a,b) ( (a < b) ? b : ROUNDDOWN(a + b/2 , b))

/*
 * To assist debugging we provide a "debug" function that can be used in place of elog.
 */
#ifndef DEBUG
#define debug(...) ((void) 0)

#else
#define USE_ASSERT_CHECKING 1

#define debug(args...) do {    \
    int save_errno = errno;    \
	fprintf(stderr, args);       \
	/* elog(DEBUG2, args);    */     \
	errno = save_errno;        \
	} while (0)


/*
 * Quick and dirty debug function to display a buffer in hex.
 * Avoids memory allocation by reusing portions of static buffer,
 * allowing multiple calls in one debug statement.
 */
static inline char *asHex(uint8_t *buf, size_t size)
{
	/* Static buffer for formatting hex string. */
    static char hex[1024];
    static char *bp = hex;

    /* Truncate the output to 64 bytes */
    if (size > 128)
       size = 128;

    /* Wrap around - we can provide a limited number of hex conversions in one printf */
    if (bp + 2 * size + 1 > hex + sizeof(hex))
        bp = hex;

    /* Convert bytes to hex and add to static buffer */
    char *start = bp;
    for (int i = 0; i < size; i++)
        bp += sprintf(bp, "%.2x", buf[i]);
    bp++; /* final null char added by sprintf */

    /* point to where the hex string started */
    return start;
}
#endif // DEBUG
#endif /*FILTER_ERROR_H */

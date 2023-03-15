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


#include "./iostack.h"

//#define DEBUG
#ifndef DEBUG
#define debug(...) ((void) 0)
#else
#define USE_ASSERT_CHECKING 1
#define debug(args...) fprintf(stderr, args)

//#define debug(args...) elog(DEBUG2, args);

/*
 * Quick and dirty debug function to display a buffer in hex.
 * Avoids memory allocation by reusing portions of static buffer.
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


/* Upper limit on the block sizes we suport. Basically 16Mb with some extra space, say for nonces. */
#define MAX_BLOCK_SIZE (16*1024*1024)

/* Helpers to access the generic IoStack functions */
#define thisStack(this) ( (IoStack *)this )
#define nextStack(this) ( thisStack(this)->next )


/* TODO: not all are on performance path, so review which should be inlined.
 * Also, adopt the general form:   setXXXError(this, retval, msg); */

/* Set error information and return -1 */
inline static int setError(void *thisVoid, int errNo, const char *fmt, va_list ap)
{
	IoStack *this = thisVoid;

	this->errNo = errNo;
	vsnprintf(this->errMsg, sizeof(this->errMsg), fmt, ap);
	debug("setError  this=%p  errNo=%d  msg=%s eof=%d\n", this, this->errNo, this->errMsg, this->eof);

	/* restore the errno so caller can still test it */
	errno = errNo;
    return -1;
}


/* Report a new I/O Stack error */
inline static ssize_t setIoStackError(void *this, size_t retval, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
    setError(this, EIOSTACK, fmt, ap);
	va_end(ap);

	return retval;
}


/* Test retval for system error, returning the retval */
static ssize_t checkSystemError(void *thisVoid, ssize_t retval, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	IoStack *this = thisVoid;

	/* If a system error occured ... */
	if (retval < 0)
	{
		/* Save the error code */
		int save_errno = errno;

		/* Create a new format string with system error info prepended */
		char newFmt[121]; /* TODO: allocate? */
		snprintf(newFmt, sizeof(newFmt), "(%d - %s) %s", save_errno, strerror(errno), fmt);

		/* Save the error information so "fileError" can retrieve it later */
		setError(this, save_errno, newFmt, ap);
	}

	va_end(ap);
	return retval;
}

/*
 * Copy error information from the next lower stack level to the current level.
 */
inline static ssize_t copyError(void *thisVoid, ssize_t retval, void *thatVoid)
{
	IoStack *this = thisVoid;
	IoStack *that = thatVoid;
	Assert(this != NULL && that != NULL);
	fileErrorInfo(that, &this->errNo, this->errMsg);
	this->eof = stackEof(that);
	return retval;
}


inline static ssize_t copyNextError(void *this, ssize_t retval)
{
	return copyError(this, retval, nextStack(this));
}

/*
 * Does the stack have an error condition?
 */
inline static bool stackHasError(void *this)
{
	return thisStack(this)->errNo != 0;
}

/*
 * Clear the error condition. True if there was one.
 */
inline static bool stackClearError(void *thisVoid)
{
	IoStack *this = thisVoid;
	bool retVal = stackHasError(this);
	this->errNo = 0;
	this->errMsg[0] = 0;
	return retVal;
}



/* Some convenient macros */
#ifndef MAX
#define MAX(a,b) ( ((a)>(b))?(a):(b) )
#endif
#ifndef MIN
#define MIN(a,b) ( ((a)<(b))?(a):(b) )
#endif
#define ROUNDDOWN(a,b) ( (a) / (b) * (b))
#define ROUNDUP(a,b)    ROUNDDOWN(a + b - 1, b)


#endif /*FILTER_ERROR_H */

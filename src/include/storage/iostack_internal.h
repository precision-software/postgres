/***********************************************************************************************************************************
Header file for developers of I/O Stacks

As a quick prototype, we are NOT doing alloc/free of memory. Consequently,
  - Errors must contain static strings.
  - Nested errors are not supported yet.
Probably need to manage own memory since errors could occur on malloc/free failures.

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

#define DEBUG
#ifndef DEBUG
#define debug(...) ((void) 0)
#else
#include <assert.h>
//#define debug(args...) fprintf(stderr, args)

#define debug(args...) elog(DEBUG2, args);


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
inline static int setError(void *thisVoid, int errNo, const char *fmt, va_list args)
{
	IoStack *this = thisVoid;

	this->errNo = errNo;
	snprintf(this->errMsg, sizeof(this->errMsg), fmt, args);
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
inline static ssize_t checkSystemError(void *thisVoid, ssize_t retval, const char *fmt, ...)
{
	IoStack *this = thisVoid;

	/* If a system error occured ... */
	if (retval < 0)
	{
		/* Save the error code */
		int save_errno = errno;

		/* Create a new format string with system error info prepended */
		char newFmt[121];
		snprintf(newFmt, sizeof(newFmt), "(%d - %s) %s", save_errno, strerror(errno), fmt);

		/* Save the error information so "fileError" can retrieve it later */
		va_list ap;
		va_start(ap, fmt);
		setError(this, save_errno, newFmt, ap);
		va_end(ap);
	}

	return retval;
}

/*
 * Copy error information from the next lower stack level to the current level.
 */
inline static bool fileErrorNext(void *thisVoid)
{
	IoStack *this = thisVoid;
	Assert(this != NULL && this->next != NULL);
	fileErrorInfo(this->next, &this->errNo, this->errMsg);
	this->eof = fileEof(this->next);
	return fileError(this);
}



inline static ssize_t setNextError(void *thisVoid, ssize_t retval)
{
	IoStack *this = thisVoid;

	/* Get the EOF info from our successor */
	thisStack(this)->eof = fileEof(nextStack(this));

	/* Fetch the error info as well */
	fileErrorInfo(nextStack(this), &thisStack(this)->errNo, thisStack(this)->errMsg);

	/* return the passed in retval */
	return retval;
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

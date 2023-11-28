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

/* Initialize I/O stacks.*/
extern void ioStackSetup(void);

/* To assist testing */
extern void setTestStack(IoStack *proto);

/*
 * The following stack eleements.
 */
extern void *vfdStackNew(void);

/*
 * Quick and dirty debug function to display a buffer in hex.
 * Avoids memory allocation by reusing portions of static buffer.
 */
static inline char *asHex(uint8_t *buf, size_t size)
{
	/* Static buffer for formatting hex string. */
    static char hex[1024];
    static char *bp = hex;
	char *start;

    /* Truncate the output to 64 bytes */
    if (size > 128)
       size = 128;

    /* Wrap around - we can provide a limited number of hex conversions in one printf */
    if (bp + 2 * size + 1 > hex + sizeof(hex))
        bp = hex;

    /* Convert bytes to hex and add to static buffer */
    start = bp;
    for (int i = 0; i < size; i++)
        bp += sprintf(bp, "%.2x", buf[i]);
    bp++; /* final null char added by sprintf */

    /* point to where the hex string started */
    return start;
}

/* Upper limit on the block sizes we suport. Basically 16Mb with some extra space, say for nonces. */
#define MAX_BLOCK_SIZE (16*1024*1024)

/* Helpers to access the generic IoStack functions */
#define thisStack(this) ( (IoStack *)this )
#define nextStack(this) ( thisStack(this)->next )


/* TODO: not all are on performance path, so review which should be inlined. */

/*
 * Set (vararg) error info for the I/O stack,
 *   setting errno and returning -1 for convenience.
 */
inline static int
stackVSetError(IoStack *stack, int errorCode, const char *fmt, va_list args)
{
	stack->errNo = errorCode;
	vsnprintf(stack->errMsg, sizeof(stack->errMsg), fmt, args);
	errno = errorCode;
	file_debug("Error! code=%d msg=%s", errorCode, stack->errMsg);
	return -1;
}

/*
 * Format error info for the I/O stack, return -1 for convenience.
 * Sets errno as a side effect.
 */
inline static int
stackSetError(void *thisVoid, int errorCode, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	stackVSetError(thisVoid, errorCode, fmt, args);
	va_end(args);
	return -1;
}


/*
 * Copy the error info from one stack to another, setting errno as a side effect.
 */
inline static void
stackCopyError(IoStack *dest, IoStack *src)
{
	dest->errNo = src->errNo;
	strcpy(dest->errMsg, src->errMsg);
	errno = src->errNo;
}


/*
 * Check return value for -1, and if so set the error info. Expects errno to be set.
 */
inline static ssize_t
stackCheckError(void *thisVoid, ssize_t retval, const char *fmt, ...)
{
	va_list args;
	IoStack *this = thisVoid;

	/* If a system error occured ... */
	if (retval < 0)
	{
		va_start(args, fmt);
		stackVSetError(this, errno, fmt, args);
		va_end(args);
	}

	return retval;
}

/*
 * Copy error information from the next lower stack level to the current level.
 */
inline static ssize_t
copyError(void *this, ssize_t retval, void *that)
{
	stackCopyError(this, that);
	return retval;
}

/*
 * Copy error info from the next lower stack level.
 */
inline static ssize_t
copyNextError(void *this, ssize_t retval)
{
	return copyError(this, retval, nextStack(this));
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

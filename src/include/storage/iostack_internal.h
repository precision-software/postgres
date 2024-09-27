/*
Header file for developers of I/O Stacks

As a quick prototype, we are NOT doing alloc/free of memory. Consequently,
  - Error messages are preallocated as part of the IoStack structure.
  - Nested errors are not supported yet. (but easily done with dynamic memory allocations)

This is a "header only" file.
*/
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
static inline char *asHex(const uint8_t *buf, size_t size)
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

	if (errorCode == EIOSTACK)
	    elog(WARNING, "IoStack Error: %s", stack->errMsg);

#ifdef NOTNOW
	/* TODO: debugging only. This will cause aeadtest to fail since aeadtest deliberately creates corrupt files. */
	if (errorCode == EIOSTACK)
		abort();
#endif

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
 * Set the error information and return retval for convenience.
 * Implemented as a macro so the return value can have any type.
 */
#define stackErrorSet(this, retval, code, args...) \
    (stackSetError(this, code, args), retval)

/*
 * Set an I/O stack error (EIOSTACK).
 */
#define setIoStackError(this, retval, args...)  stackErrorSet(this, retval, EIOSTACK, args)

/*
 * Expects errno to be set.  TODO: DEFUNCT
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
#define ROUNDUP(a,b)    ROUNDDOWN((a) + (b) - 1, b)
#define ROUNDOFF(a,b) ROUNDDOWN((a) + (b)/2, b)

/*
 * A collection of routines for packing/unpacking ints
 * into network byte order.
 */
inline static void
packInt8(Byte *dest, Byte value)
{
	*dest = value;
}

inline static void
packInt16(Byte *dest, uint16 value)
{
	packInt8(dest, value>>8);
	packInt8(dest+1, value);
}

inline static void
packInt32(Byte *dest, uint32 value)
{
	packInt16(dest, value>>16);
	packInt16(dest+2, value);
}

inline static void
packInt64(Byte *dest, uint64 value)
{
	packInt32(dest, value>>32);
	packInt32(dest+4, value);
}

inline static Byte
unpackInt8(Byte *src)
{
	return  *src;
}

inline static uint16
unpackInt16(Byte *src)
{
	return (uint16)unpackInt8(src)<<8 | unpackInt8(src+1);
}

inline static uint32
unpackInt32(Byte *src)
{
	return (uint32)unpackInt16(src)<<16 | unpackInt16(src+2);
}

inline static uint64
unpackInt64(Byte *src)
{
	return (uint64)unpackInt32(src)<<32 | unpackInt32(src+4);
}


#endif /*FILTER_ERROR_H */

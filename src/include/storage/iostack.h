/*
 * Header file for users of I/O stacks.
*/

#ifndef FILTER_IoStack_H
#define FILTER_IoStack_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/syslimits.h>
#include <unistd.h>
#include "postgres.h"

typedef uint8_t Byte;
typedef struct IoStack IoStack;

/*
 * Universal helper functions - across all I/O stacks.
 * TODO: Should "this" be void* or IoStack* ?  Leaning towards IoStack ...
 */
extern ssize_t stackWriteAll(void *this, const Byte *buf, size_t size, off_t offset, uint32 wait);
extern ssize_t stackReadAll(void *this, Byte *buf, size_t size, off_t offset, uint32 wait);
extern ssize_t stackReadSized(IoStack *this, Byte *buf, size_t size, off_t offset, uint32 wait);
extern ssize_t stackWriteSized(IoStack *this, const Byte *buf, size_t size, off_t offset, uint32 wait);
extern bool stackWriteInt32(IoStack *this, uint32_t data, off_t offset, uint32 wait);
extern bool stackReadInt32(IoStack *this, uint32_t *data, off_t offset, uint32 wait);
extern bool stackWriteInt64(IoStack *this, uint64_t data, off_t offset, uint32 wait);
extern bool stackReadInt64(IoStack *this, uint64_t *data, off_t offset, uint32 wait);

extern IoStack *selectIoStack(const char *path, uint64 oflags, mode_t mode);


/*
 * Specific layers provided by IoStack. Mix and match.
 */
void *bufferedNew(ssize_t suggestedSize, void *next);
void *lz4CompressNew(size_t blockSize, void *indexFile, void *next);
void *aeadNew(char *cipherName, size_t suggestedSize, Byte *key, size_t keyLen, uint64 getSeqNr(), void *next);
void *vfdStackNew(void);

/* Filter for talking to Posix files. Not used by Postgres, but handy for unit tests. */
IoStack *fileSystemBottomNew();

/*
 *
 * We need an error value for existing code which only looks at errno.
 * In the long term, we may want to create special error codes for postgres.
 * For now, pick a single distinctive and nonsensical errno.
 */
#define EIOSTACK ENOTSUP


typedef struct IoStackInterface IoStackInterface;
struct IoStack
{
	IoStack *next;					/* Pointer to the next lower layer of the IoStack */
	IoStackInterface *iface;		/* The implementation of the IoStack functions, eg Read, Write, Open */
	ssize_t blockSize;				/* The block size this layer is expecting. Can be queried by the layer above */
	ssize_t openVal; 				/* The value returned by the bottom layer's open call */
	bool eof;						/* True if End of file.  Reset by next read */
	int errNo;                      /* System error number - cleared by each call (except close) */
	char errMsg[121];               /* Error message */
};

/*
 * A set of functions each IoStack must provide.
 *   IoStackOpen On error, returns a closed node with error info,
 *               or NULL if unable to allocate memory.
 */
typedef IoStack *(*IoStackOpen)(void *this, const char *path, uint64 oflags, mode_t perm);
typedef ssize_t (*IoStackRead)(void *this, Byte *buf, ssize_t size, off_t offset, uint32 wait);
typedef ssize_t (*IoStackWrite)(void *this, const Byte *buf, ssize_t size, off_t offset, uint32 wait);
typedef bool (*IoStackSync)(void *this, uint32 wait);
typedef bool (*IoStackClose)(void *this);
typedef off_t (*IoStackSize)(void *this);
typedef bool (*IoStackResize) (void *this, off_t offset, uint32 wait);

struct IoStackInterface {
	IoStackOpen fnOpen;
	IoStackWrite fnWrite;
	IoStackClose fnClose;
	IoStackRead fnRead;
	IoStackSync fnSync;
	IoStackSize fnSize;
	IoStackResize fnResize;
};

/*
 * Abstract functions required for each filter in an I/O Stack.
 * TODO: reimplement as inline functions
 */
#define stackOpen(this, path, oflags, mode)       		(IoStack *)(invoke(Open, this, path, oflags, mode))
#define stackWrite(this, buf, size, offset, wait)  		invoke(Write, this, buf, size, offset, wait)
#define stackRead(this, buf, size, offset, wait)   		invoke(Read,  this, buf, size, offset, wait)
#define stackSync(this, wait)                      		invoke(Sync, this, wait)
#define stackSize(this)                           		invokeNoParms(Size, this)
#define stackResize(this, offset, wait)       			invoke(Resize, this, offset, wait)
#define stackClose(this)      							invokeNoParms(Close, this)

typedef IoStack *(*IoStackCreateFunction)(void);

/* Helper macros used above */
#define invoke(call, stack, args...)   (stackClearError(stack),((IoStack *)(stack))->iface->fn##call((void*)(stack), args))
#define invokeNoParms(call, stack)     (stackClearError(stack),((IoStack *)(stack))->iface->fn##call((void*)(stack)))

/*
 * Error handling functions for I/O stacks.
 * Could be called frequently, so inline them.
 */

/*
 * Get the error code, setting errno as a side effect.
 */
inline static
int stackErrorCode(void *this)
{
	errno = ((IoStack *)this)->errNo;
	return errno;
}

/*
 * Does the stack have an error condition? Sets errno as a side effect.
 */
inline static
bool stackError(void *this)
{
	return stackErrorCode(this) != 0;
}

/* Did the last read encounter EOF? */
inline static
bool stackEof(void *this)
{
	return ((IoStack *)this)->eof;
}

/*
 * Clear the error condition. True if there was one.
 */
inline static
bool stackClearError(void *thisVoid)
{
	IoStack *this = thisVoid;
	errno = this->errNo;
	this->errNo = 0;
	this->errMsg[0] = 0;
	this->eof = false;
	return errno != 0;
}

/*
 * Get the error message, setting errno as a side effect.
 */
inline static
const char * stackErrorMsg(void *this)
{
	stackErrorCode(this);
	return ((IoStack *)this)->errMsg;
}

/*
 * Get the error code, setting errno as a side effect.
 */
inline static int
stackErrorNo(void *thisVoid)
{
	IoStack *this = thisVoid;
	errno = this->errNo;
	return this->errNo;
}

/* Declare a "debug" macro */
#define FILE_DEBUG
#ifdef FILE_DEBUG
#define file_debug(...) \
    do {  \
        int save_errno = errno; \
		setvbuf(stderr, NULL, _IOLBF, 256); \
		fprintf(stderr, "%s(%d): ", __func__, getpid());     \
		fprintf(stderr, __VA_ARGS__); \
		fprintf(stderr, "\n");\
        /* elog(DEBUG2, __VA_ARGS__);  */ \
        errno = save_errno;  \
    } while (0)

#else
#define file_debug(...) ((void)0)
#endif

#endif /*FILTER_IoStack_H */

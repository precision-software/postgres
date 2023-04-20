/*
 * Header file for users of I/O stacks.
 *
 * An I/O stack is a set of layers that provide a file-like interface to a file.
 * Layers are stacked on top of each other. The top layer is called directly by the user,
 * and it provides the expected FileRead, FileWrite, FileClose functions.
 * The bottom layer provides actual access to a file. Intermediate layers implement
 * additional functionality, such as encryption, compression, or buffering.
 *
 * This file defines the I/O Stack interfaces and provides functions to link individual stack layers together.
 *
 * I/O stacks require more complex error handling than a simple file interface. The error handling is based on an error code
 * and an explicit error message. Eventually, the errNo field could be expanded to include postgres specific error codes.
 * Currently, non-system errors are asigned the value EIOSTACK (ENOTSUP).
 *
 * Once a stack is built, it serves as a prototype to the "Open" function. The "Open" function clones the prototype,
 * allocates resources and opens the actual file.
 * If the open fails, the only resource allocated is the top layer, which contains error information. It must be explicitly freed.
 *
 * When a stack is closed, all resources in the stack are released except the top layer, which contains error information.
 * The top layer must be explicitly freed.
 *
 * Note a "file" consists of a number of fixed size blocks, possibly terminated by a partial block.
 * The block size is a configurable parameter for some layers, and there is limited communication
 * between layers to ensure compatible block sizes. Note for buffering and Posix files, the block size is 1
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
typedef struct IoStackInterface IoStackInterface;

/* Forward references */
inline static int stackErrorCode(void *this);


/*
 * Abstract functions required for each filter in an I/O Stack.
 * Currently implemented as macros, but could be changed to inline functions.
 */
#define stackOpen(this, path, oflags, mode)       				(IoStack *)(stackClearError(this), invoke(Open, this, path, oflags, mode))
#define stackWrite(this, buf, size, offset)  					invoke(Write, this, buf, size, offset)
#define stackRead(this, buf, size, offset)   					invoke(Read,  this, buf, size, offset)
#define stackSync(this)                      					invokeNoParms(Sync, this)
#define stackSize(this)                           				invokeNoParms(Size, this)
#define stackTruncate(this, offset)       						invoke(Truncate, this, offset)
#define stackClose(this)      									invokeNoParms(Close, this)

/* Helper macros used above. Call the corresponding routine on a specific layer */
#define invoke(call, stack, args...)   (((IoStack *)(stack))->iface->fn##call((void*)(stack), args))
#define invokeNoParms(call, stack)     (((IoStack *)(stack))->iface->fn##call((void*)(stack)))

/*
 * Signatures of the actual functions in the interface.
 */
typedef IoStack *(*IoStackOpen)(void *this, const char *path, int mode, int perm);
typedef ssize_t (*IoStackRead)(void *this, Byte *buf, size_t size, off_t offset);
typedef ssize_t (*IoStackWrite)(void *this, const Byte *buf, size_t size, off_t offset);
typedef ssize_t (*IoStackSync)(void *this);
typedef ssize_t (*IoStackClose)(void *this);
typedef off_t (*IoStackSize)(void *this);
typedef ssize_t (*IoStackTruncate) (void *this, off_t offset);


/* A interface structure pointing to the actual functions. */
struct IoStackInterface {
	IoStackOpen fnOpen;
	IoStackWrite fnWrite;
	IoStackClose fnClose;
	IoStackRead fnRead;
	IoStackSync fnSync;
	IoStackSize fnSize;
	IoStackTruncate fnTruncate;
};

/* Generic I/O stack data which must be the first element in each type of IoStack */
struct IoStack
{
	IoStack *next;					/* Pointer to the next lower layer of the IoStack */
	IoStackInterface *iface;		/* The implementation of the IoStack functions, eg Read, Write, Open */
	ssize_t blockSize;				/* The block size this layer is expecting. Can be queried by the layer above */
	ssize_t openVal; 				/* The value returned by the bottom layer's open call */
	bool eof;						/* True if End of file.  Reset by next read */
	int errNo;                      /* System error number - cleared by ??? */
	char errMsg[121];               /* Error message */
};


/*
 * Universal helper functions - used across all layers.
 */
ssize_t stackWriteAll(IoStack *this, const Byte *buf, size_t size, off_t offset);
ssize_t stackReadAll(IoStack *this, Byte *buf, size_t size, off_t offset);
ssize_t stackReadSized(IoStack *this, Byte *buf, size_t size, off_t offset);
ssize_t stackWriteSized(IoStack *this, const Byte *buf, size_t size, off_t offset);
bool stackWriteInt32(IoStack *this, uint32_t data, off_t offset);
bool stackReadInt32(IoStack *this, void *data, off_t offset);
bool stackWriteInt64(IoStack *this, uint64_t data, off_t offset);
bool stackReadInt64(IoStack *this, void *data, off_t offset);

/*
 * For error handling, we need an errno value which is not encountered in normal operation
 * In the long term, we may want to create special error codes for postgres.
 * For now, pick a single distinctive and nonsensical errno.
 */
#define EIOSTACK ENOTSUP

/* For unit testing */
typedef IoStack *(*IoStackCreateFunction)(void);


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
	bool retVal = stackError(this);
	this->errNo = 0;
	this->errMsg[0] = 0;
	this->eof = false;
	errno = 0;
	return retVal;
}
/*
 * Get the layer's error code, setting errno as a side effect.
 */
inline static
int stackErrorCode(void *this)
{
	errno = ((IoStack *)this)->errNo;
	return errno;
}

/*
 * Get the error message, setting errno as a side effect.
 */
inline static
const char * stackErrorMsg(void *this)
{
	errno = stackErrorCode(this);
	return ((IoStack *)this)->errMsg;
}


/*
 * Finally, constructors for the known layers of an I/O stack.
 * These constructors allow us to create a stack by "mixing and matching" layers.
 */

/* General buffering layer */
void *bufferedNew(ssize_t suggestedSize, void *next);

/* Experimental Lz4 compression. Supports random reads, but not random writes */
void *lz4CompressNew(size_t blockSize, void *indexFile, void *next);

/* Encryption layer, supporting both random reads and writes. */
void *aeadNew(char *cipherName, size_t suggestedSize, Byte *key, size_t keyLen, void *next);

/* Bottom layer for accessing PostgreSql virtual file descriptors */;
void *vfdStackNew(void);

/* Bottom layer for accessing Posix files. Not used by Postgres, but handy for unit tests. */
IoStack *fileSystemBottomNew();


#endif /*FILTER_IoStack_H */

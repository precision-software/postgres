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
ssize_t stackWriteAll(IoStack *this, const Byte *buf, size_t size, off_t offset);
ssize_t stackReadAll(IoStack *this, Byte *buf, size_t size, off_t offset);
ssize_t stackReadSized(IoStack *this, Byte *buf, size_t size, off_t offset);
ssize_t stackWriteSized(IoStack *this, const Byte *buf, size_t size, off_t offset);
bool stackWriteInt32(IoStack *this, uint32_t data, off_t offset);
bool stackReadInt32(IoStack *this, uint32_t *data, off_t offset);
bool stackWriteInt64(IoStack *this, uint64_t data, off_t offset);
bool stackReadInt64(IoStack *this, uint64_t *data, off_t offset);


bool stackError(void *thisVoid);
bool stackEof(void *thisVoid);
//bool stackClearError(void *thisVoid);
bool stackErrorInfo(void *thisVoid, int *errNo, char *errMsg);
char *stackErrorMsg(void *thisVoid);
int stackErrorNo(void *thisVoid);


/*
 * Specific layers provided by IoStack. Mix and match.
 */
void *bufferedNew(ssize_t suggestedSize, void *next);
void *lz4CompressNew(size_t blockSize, void *indexFile, void *next);
void *aeadNew(char *cipherName, size_t suggestedSize, Byte *key, size_t keyLen, void *next);
void *vfdStackNew(void);

/* Filter for talking to Posix files. Not used by Postgres, but handy for unit tests. */
IoStack *fileSystemBottomNew();

/* We need some error code to signal an I/O Stack error. Pick an unlikely one as a filler */
#define EIOSTACK EBADF

/*
 * Internals moved here so fileRead, fileWrite, etc can be inlined.
 *
 * First, some comments about block size.
 *  - a file consists of a sequence of blocks, where all blocks are the same
 *    size except the last, which might be smaller.
 *  - The size of the block expected by the stack layer is saved in ioStack->blockSize.
 *  - Different layers in the stack may translate the data, and in doing so, change the blockSize.
 *  - In some cases, it is necessary to negotiate block sizes between the layers.
 *    For example, an encryption layer may add fillers or checksums to the encrypted data.
 *  - The negotiation, if needed, is handled during the Open() call. TODO: OUT OF DATE.
 *       - At construction of the stack, all blockSizes are initialized to 0.
 *       - When calling Open(), a layer can request a blockSize by setting nextStack(this)->blockSize to the requested size.
 *       - During the Open(), the sublayer assigns a block size by filling in its own value for this->blockSize.
 *       - After Open(), the calling layer verifies nextStack(this)->blockSize is acceptable.
 *         Generally, the assigned block size must be a multiple of the requested size.
 *       - For layers where block size doesn't matter (buffering or Posix files), the block size will be assigned as "1".
 */
typedef struct IoStackInterface IoStackInterface;
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
 * A set of functions each IoStack must provide.
 */
typedef IoStack *(*IoStackOpen)(void *this, const char *path, int mode, int perm);
typedef ssize_t (*IoStackRead)(void *this, Byte *buf, size_t size, off_t offset);
typedef ssize_t (*IoStackWrite)(void *this, const Byte *buf, size_t size, off_t offset);
typedef ssize_t (*IoStackSync)(void *this);
typedef ssize_t (*IoStackClose)(void *this);
typedef off_t (*IoStackSize)(void *this);
typedef ssize_t (*IoStackTruncate) (void *this, off_t offset);

struct IoStackInterface {
	IoStackOpen fnOpen;
	IoStackWrite fnWrite;
	IoStackClose fnClose;
	IoStackRead fnRead;
	IoStackSync fnSync;
	IoStackSize fnSize;
	IoStackTruncate fnTruncate;
};

/*
 * Abstract functions required for each filter in an I/O Stack. TODO: declare as inline functions.
 * TODO: Rename, file-->stack, and Write-->WritePartial and WriteAll-->Write
 */
#define stackOpen(this, path, oflags, mode)       				(IoStack *)(stackClearError(this), invoke(Open, this, path, oflags, mode))
#define stackWrite(this, buf, size, offset)  					invoke(Write, this, buf, size, offset)
#define stackRead(this, buf, size, offset)   					invoke(Read,  this, buf, size, offset)
#define stackSync(this)                      					invokeNoParms(Sync, this)
#define stackSize(this)                           				invokeNoParms(Size, this)
#define stackTruncate(this, offset)       						invoke(Truncate, this, offset)
#define stackClose(this)      									invokeNoParms(Close, this)

typedef IoStack *(*IoStackCreateFunction)(void);

/* Helper macros used above */
#define invoke(call, stack, args...)   (((IoStack *)(stack))->iface->fn##call((void*)(stack), args))
#define invokeNoParms(call, stack)     (((IoStack *)(stack))->iface->fn##call((void*)(stack)))

#endif /*FILTER_IoStack_H */

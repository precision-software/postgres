/*
 * Functions for accessing files.
 */
#ifndef TEST_MEMTRACK_OUT_FILEACCESS_H
#define TEST_MEMTRACK_OUT_FILEACCESS_H

#include "storage/iostack.h"

/*
 * Additional information to support FReadSeq/FWriteSeq/Encryption interface.
 */
typedef struct FState
{
	IoStack *ioStack;
	off_t offset;
	//ssize_t fileSize;
} FState;

/*
 * New names for basic file functions.
 * Want to keep the original functions untouched
 * until they are replaced by these.
 */
extern File FOpen(const char *path, uint64 oflags);
extern File FOpenPerm(const char *path, uint64 oflags, mode_t mode);
extern ssize_t FReadSeq(File file, void *buffer, size_t amount, uint32 wait);
extern ssize_t FWriteSeq(File file, const void *buffer, size_t amount, uint32 wait);
extern ssize_t FRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait);
extern ssize_t FWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait);
extern bool FClose(File file);
extern off_t FSize(File file);
extern bool FSync(File file, off_t offset, size_t amount, uint32 wait);
extern bool FResize(File file, off_t offset, uint32 wait);

/* Additional Sequential I/O functions */
extern int FGetc(File file);
extern int FPutc(File file, unsigned char c);
extern ssize_t FPrint(File file, const char *format, ...);
extern ssize_t FScan(File file, const char *format, ...);
extern ssize_t FPuts(File file, const char *string);
extern off_t FSeek(File file, off_t offset);
extern off_t FTell(File file);

/* Error handling */
extern bool FError(File file);
extern bool FEof(File file);
extern bool FClearError(File file);
extern const char *FErrorMsg(File file);
extern int FErrorCode(File file);

/* Misc. */
extern int setFileError(File file, int errorCode, const char *fmt, ...);
extern ssize_t FBlockSize(File file);


#endif //TEST_MEMTRACK_OUT_FILEACCESS_H

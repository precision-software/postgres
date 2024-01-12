/*
 * Functions for accessing files.
 */
#ifndef TEST_MEMTRACK_OUT_FILEACCESS_H
#define TEST_MEMTRACK_OUT_FILEACCESS_H

#include "storage/iostack.h"

/*
 * Additional information to support FReadSeq/FWriteSeq/Encryption interface.
 */
typedef struct FileState
{
	IoStack *ioStack;
	off_t offset;
	off_t fileSize;
} FileState;

/*
 * Additional open flags, all in upper word so they don't interfere
 * with existing kernel flags.
 */
#define PG_XACT           (1ll << 36)       /* Close at end of transaction */
#define PG_DELETE         (1ll << 37)       /* Delete file when closing */
#define PG_TEMP_LIMIT     (1ll << 38)       /* Enable temp file accounting */
#define PG_TRANSIENT      (1ll << 39)       /* Transient file closed when subtran aborts */
#define PG_TEXT           (1ll << 40)       /* Open as text file */

#define PG_STACK_MASK     (15ll << 32)
#define PG_ENCRYPT        (1ll << 32)
#define PG_ECOMPRESS      (2ll << 32)
#define PG_ENCRYPT_PERM   (3ll << 32)
#define PG_TESTSTACK      (4ll << 32)
#define PG_PLAIN          (5ll << 32)
#define PG_RAW            (6ll << 32)



#define PG_TEXT           (1ll << 40)

/*
 * New names for basic file functions.
 * Want to keep the original FILE_* functions untouched.
 */
extern File FOpen(const char *path, uint64 oflags);
extern File FOpenPerm(const char *path, uint64 oflags, mode_t mode);
extern ssize_t FReadSeq(File file, void *buffer, size_t amount, uint32 wait);
extern ssize_t FWriteSeq(File file, const void *buffer, size_t amount, uint32 wait);
extern ssize_t FRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait);
extern ssize_t FWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait);
extern bool FClose(File file);
extern off_t FSize(File file);
extern bool FSync(File file, uint32 wait);  /* TODO: add offset and size */
extern bool FResize(File file, off_t offset, uint32 wait);

/* Additional Sequential I/O functions */
extern int FGetc(File file, uint32 wait);
extern int FPutc(File file, unsigned char c, uint32 wait);
extern ssize_t FPrint(File file, const char *format, ...);
extern ssize_t FScan(File file, const char *format, ...);
extern ssize_t FPuts(File file, const char *string, uint32 wait);
extern off_t FSeek(File file, off_t offset);
extern off_t FTell(File file);
extern bool FReadLine(File file, char *buf, ssize_t maxlen, uint32 wait);

/* Convenience */
static inline bool FTruncate(File file, off_t newSize, uint32 wait)
{
	Assert(newSize <= FSize(file));
	return FResize(file, newSize, wait);
}

/* Error handling */
extern bool FError(File file);
extern bool FEof(File file);
extern bool FClearError(File file);
extern const char *FErrorMsg(File file);
extern int FErrorCode(File file);

/* Misc. */
extern int setFileError(File file, int errorCode, const char *fmt, ...);
extern ssize_t FBlockSize(File file);
static bool FileIsLegacy(File file);
extern bool PathNameFSync(const char *name, uint32 wait);

/* Expermental - belongs elsewhere. Says the function doesn't modify globals. */
#ifdef _MSC_VER
#define PURE __declspec(noalias)
#else
#define PURE __attribute__((pure))
#endif

/* Copied from fd.h, which includes us */
extern FileState *getFState(File file) PURE;

#endif //TEST_MEMTRACK_OUT_FILEACCESS_H

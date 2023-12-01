/*
 * Functions for accessing files.
 */
#ifndef TEST_MEMTRACK_OUT_FILEACCESS_H
#define TEST_MEMTRACK_OUT_FILEACCESS_H

#include "fd.h"

typedef struct FileAccess
{
	IoStack stack;
	off_t offset;
	off_t fileSize;
};

/*
 * New names for functions. Want to keep current "File*" names untouched.
 */
#define FOpen FileOpen
#define FRead FileReadSeq
#define FWrite FileWriteSeq
#define FPread FileRead
#define FPwrite FileWrite
#define FClose FileClose
#define FSync FileSync
#define FSize fileSize
#define FResize FileResize


#endif //TEST_MEMTRACK_OUT_FILEACCESS_H

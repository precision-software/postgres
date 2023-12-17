/* */
#ifndef FILTER_FILEFRAMEWORK_H
#define FILTER_FILEFRAMEWORK_H

#include <stdbool.h>

/* Function type to create an IoStack with the given block size */
typedef void *(*CreateStackFn)(size_t blockSize);

void seekTest(CreateStackFn createStack, char *nameFmt);
void singleSeekTest(CreateStackFn createStack, char *nameFmt, off_t fileSize, size_t bufSize);

void streamTest(CreateStackFn createStack, char *nameFmt);
void singleStreamTest(CreateStackFn createStack, char *nameFmt, off_t fileSize, size_t bufSize);

void readSeekTest(CreateStackFn createStack, char *nameFmt);
void singleReadSeekTest(CreateStackFn createStack, char *nameFmt, off_t fileSize, size_t bufSize);

void generateFile(const char *name, off_t fileSize, size_t blockSize);
bool verifyFile(const char *name,  off_t fileSiew, size_t blockSize);

#include "unitTestInternal.h"


#endif //FILTER_FILEFRAMEWORK_H

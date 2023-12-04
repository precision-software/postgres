/**/

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <stdbool.h>

#include "postgres.h"
#include "storage/fd.h"
#include "storage/iostack_internal.h"
//#include "storage/vfd.h"n

#include "fileFramework.h"
#include "unitTestInternal.h"

typedef uint8_t Byte;
#define PATH_MAX 1024

#define countof(array) (sizeof(array)/sizeof(array[0]))

/* Matrix of file and block sizes for testing. */
off_t fileSize[] = {0, 1024, 1, 64, 1027, 7*1024, 32*1024 + 127, 6*1024*1024+153};
size_t blockSize[] = {1024, 4 * 1024, 3 * 1024 + 357, 1024 - 237, 64, 1};

/* Given the position in the seek, generate one byte of data for that position. */
static inline Byte generateByte(size_t position)
{
    static char data[] = "The cat in the hat jumped over the quick brown fox while the dog ran away with the spoon.\n";
    size_t idx = position % (sizeof(data)-1);    // Skip the nil character.
    return data[idx];
}

/* Fill a buffer with data appropriate to that position in the seek */
static void generateBuffer(size_t position, Byte *buf, size_t size)
{
    for (size_t i = 0; i < size; i++)
        buf[i] = generateByte(position+i);
}

/* Verify a buffer has appropriate data for that position in the test file. */
static bool verifyBuffer(size_t position, Byte *buf, size_t size)
{
    for (size_t i = 0; i < size; i++)
    {
        Byte expected = generateByte(position + i);
		if (expected != buf[i])
			file_debug("verifyBuffer: i=%zu position=%zu  buf[i]=%c expected=%c", i, position, buf[i], expected);
        PG_ASSERT_EQ(expected, buf[i]);
    }
    return true;
}

/*
 * Create a file and fill it with known data.
 * The file contains the same line of text repeated over and over, which
 *   - makes it easy to verify with a text editor,
 *   - doesn't align with typical block sizes, and
 *   - is compressible.
 */
static void generateFile(char *path, off_t size, size_t bufferSize)
{
    Byte *buf;
    off_t position;
    File file;

    file_debug("generateFile: path=%s", path);
    file = FileOpen( path, O_WRONLY|O_CREAT|O_TRUNC|PG_TESTSTACK);
	PG_ASSERT(file != -1);
    buf = malloc(bufferSize); /* TODO: make buf be at end of struct */

    for (position = 0; position < size; position += bufferSize)
    {
        ssize_t actual;
        off_t expected = MIN(bufferSize, size-position);
        generateBuffer(position, buf, expected);
        actual = FileWriteSeq(file, buf, expected, 0);
        PG_ASSERT_EQ(expected, actual);
    }

    free(buf);
    PG_ASSERT(FileClose(file) == 0);
}

/* Verify a iostack has the correct data */
static void verifyFile(char *path, off_t fileSize, ssize_t bufferSize)
{
    File file;
    Byte *buf;

    file_debug("verifyFile: path=%s", path);
    file = FileOpen(path, O_RDONLY|PG_TESTSTACK);
	PG_ASSERT(file >= 0);
	PG_ASSERT(!FileEof(file));
	PG_ASSERT(!FileError(file));
    buf = malloc(bufferSize);

    for (off_t actual, position = 0; position < fileSize; position += actual)
    {
        size_t expected = MIN(bufferSize, fileSize - position);
        actual = FileReadSeq(file, buf, bufferSize, 0);
        PG_ASSERT_EQ(expected, actual);
        PG_ASSERT(verifyBuffer(position, buf, actual));
		PG_ASSERT(!FileEof(file));
		PG_ASSERT(!FileError(file));
    }

    // Read a final EOF.
	PG_ASSERT(!FileEof(file));
    FileReadSeq(file, buf, 1, 0);
    PG_ASSERT(FileEof(file));

    PG_ASSERT(FileClose(file) == 0);
}

/*
 * Create a file and fill it with known data using random seeks.
 * The file contains the same line of text repeated over and over, which
 *   - makes it easy to verify output with a text editor,
 *   - doesn't align with typical block sizes, and
 *   - is compressible.
 */
static void allocateFile(char *path, off_t size, ssize_t bufferSize)
{
    File file;
    Byte *buf;
    off_t position;

    file_debug("allocateFile: path=%s", path);
    /* Start out by allocating space and filling the file with "X"s. */
    file = FileOpen(path, O_WRONLY|O_CREAT|O_TRUNC|PG_TESTSTACK);
    buf = malloc(bufferSize);
    memset(buf, 'X', bufferSize);

    for (position = 0; position < size; position += bufferSize)
    {
        size_t expected = (size_t)MIN(bufferSize, size-position);
        size_t actual = FileWrite(file, buf, expected, position, 0);
        PG_ASSERT_EQ(actual, expected);
    }

    PG_ASSERT(FileClose(file) == 0);
    free(buf);
}

static const int prime = 3197;

static void generateRandomFile(char *path, off_t size, size_t blockSize)
{
    size_t nrBlocks;
    File file;
    Byte *buf;

    file_debug("generateRandomFile: path=%s", path);
    /* The nr of blocks must be relatively prime to "prime", otherwise we won't visit all the blocks. */
    nrBlocks = (size + blockSize - 1) / blockSize;
    PG_ASSERT( nrBlocks == 0 || (nrBlocks % prime) != 0);

    file = FileOpen(path, O_RDWR|PG_TESTSTACK);
	PG_ASSERT(file >= 0);
    buf = malloc(blockSize);


    for (off_t idx = 0; idx < nrBlocks; idx++)
    {
        ssize_t actual, expected;
        /* Pick a pseudo-random block and seek to it */
        off_t position = ((idx * prime) % nrBlocks) * blockSize;
        //printf("fileSeek - idx = %u  blockNr=%u nrBlocks=%u\n", idx, (idx*prime)%nrBlocks, nrBlocks);

        /* Generate data appropriate for that block. */
        expected = (size_t)MIN(blockSize, size - position);
        generateBuffer(position, buf, expected);

        /* Write the block */
        actual = FileWrite(file, buf, expected, position, 0);
        PG_ASSERT_EQ(expected,actual);
    }

    PG_ASSERT(FileClose(file) == 0);
}

static void appendFile(char *path, off_t fileSize, size_t bufferSize)
{
    File file;
    Byte *buf;
    ssize_t blockSize;
    ssize_t lastSize;
    off_t lastBlock;
    ssize_t remaining, actual;

    file_debug("appendFile: path=%s", path);
    file = FileOpen(path, O_RDWR|O_APPEND|PG_TESTSTACK);
	PG_ASSERT(file >= 0);
    buf = malloc(bufferSize);

    /* Since we are appending, we are at the end of file - should match file size */
	PG_ASSERT_EQ(fileSize, FileTell(file));

	/* The requested buffer size should be a multiple of the underlying block size. (Property of unit test) */
	blockSize = 1; //FileBlockSize(file);
	PG_ASSERT_EQ(0, bufferSize % blockSize);

	/* If the last block is a partial block, ... */
	lastBlock = ROUNDDOWN(fileSize, blockSize);
	lastSize = fileSize - lastBlock;
	if (lastSize > 0)
	{
		/* Rewrite the last block, which is now full */
		generateBuffer(lastBlock, buf, blockSize);
		PG_ASSERT_EQ(blockSize, FileWrite(file, buf, blockSize, lastBlock, 0));

		/* Adjust the write parameters to write whatever is left. It is now block aligned. */
		lastBlock += blockSize;
	}

    /* Write whatever remains to the end of file */
	remaining = (fileSize + bufferSize) - lastBlock;
    generateBuffer(lastBlock, buf, remaining);
    actual = FileWriteSeq(file, buf, remaining, 0);
    PG_ASSERT_EQ(remaining, actual);

    /* Close the file and verify it is correct. */
    PG_ASSERT_EQ(0, FileClose(file));
    verifyFile(path, fileSize+bufferSize, bufferSize);
}

/*
 * Verify a file has the correct data through random seeks.
 * This should do a complete verification - examining every byte of the file.
 */
static void verifyRandomFile(char *path, off_t size, size_t blockSize)
{
    File file;
    Byte *buf;
    size_t nrBlocks;

    file_debug("verifyRandomFile: path=%s", path);
	file = FileOpen(path, O_RDONLY|PG_TESTSTACK);
	PG_ASSERT(file >= 0);
    buf = malloc(blockSize);

    nrBlocks = (size + blockSize -1) / blockSize;
    PG_ASSERT(nrBlocks == 0 || (nrBlocks % prime) != 0);
    for (size_t idx = 0;  idx < nrBlocks; idx++)
    {
        ssize_t actual, expected;

        /* Pick a pseudo-random block and read it */
        off_t position = ((idx * prime) % nrBlocks) * blockSize;

        actual = FileRead(file, buf, blockSize, position, 0);

        /* Verify we read the correct data */
        expected = MIN(blockSize, size-position);
        PG_ASSERT_EQ(expected, actual);
        PG_ASSERT(verifyBuffer(position, buf, actual));
    }

    PG_ASSERT_EQ(0, FileClose(file));
}


static void deleteFile(char *name)
{
	unlink(name);
}


static void regression(char *name, size_t blockSize)
{
    File file;
	int idx;
    Byte *buf = calloc(blockSize, 0xff);
    Byte *ones = calloc(blockSize, 1);
	Byte *zeros= calloc(blockSize, 0);

    deleteFile(name);

	/* Shouldn't open a non-existent file - various modes) */
	file = FileOpen(name, O_RDWR|PG_TESTSTACK);
	PG_ASSERT_EQ(-1, file);
	PG_ASSERT_EQ(ENOENT, errno);

	file = FileOpen(name, O_RDONLY|PG_TESTSTACK);
	PG_ASSERT(file == -1);
	PG_ASSERT_EQ(errno, ENOENT);

	/* OK to create a file and reopen readonly */
	file = FileOpen(name, O_CREAT | O_WRONLY | O_TRUNC|PG_TESTSTACK);
	PG_ASSERT(file >= 0);
	PG_ASSERT_EQ(FileClose(file), 0);

	file = FileOpen(name, O_CREAT | O_WRONLY | O_TRUNC | PG_TESTSTACK);
	PG_ASSERT(file >= 0);
	PG_ASSERT_EQ(FileClose(file), 0);

	/* EBADF if closing an already closed file */
	PG_ASSERT(FileClose(file) != 0 && errno == EBADF);

	/* Should read EOF on empty file */
	file = FileOpen(name, O_RDONLY|PG_TESTSTACK);
	PG_ASSERT(0 == FileRead(file, buf, blockSize, 0, 0));
	PG_ASSERT(FileEof(file));
	PG_ASSERT(!FileError(file));
	PG_ASSERT(FileClose(file) == 0);

	/* Should write a block and then read EOF */
	file = FileOpen(name, O_RDWR|O_TRUNC|PG_TESTSTACK);
	PG_ASSERT_EQ(blockSize, FileWriteSeq(file, ones, blockSize, 0));
	PG_ASSERT_EQ(blockSize, FileSize(file));
	PG_ASSERT_EQ(0, FileReadSeq(file, buf, blockSize,  0));
	PG_ASSERT(FileEof(file));
	PG_ASSERT(!FileError(file));
	PG_ASSERT(FileClose(file) == 0);

	/* Writing beyond EOF should extend file */
	file = FileOpen(name, O_RDWR|O_TRUNC|PG_TESTSTACK);
	PG_ASSERT_EQ(blockSize, FileWrite(file, ones, blockSize, blockSize, 0));
	PG_ASSERT_EQ(2*blockSize, FileSize(file));
	PG_ASSERT_EQ(blockSize, FileRead(file, buf, blockSize, 0, 0));
	for (idx = 0; idx<blockSize; idx++)
		PG_ASSERT_EQ(zeros[idx], buf[idx]);
	PG_ASSERT_EQ(blockSize, FileRead(file, buf, blockSize, blockSize, 0));
	for (idx = 0; idx < blockSize; idx++)
		PG_ASSERT_EQ(ones[idx], buf[idx]);
	PG_ASSERT_EQ(0, FileClose(file));


	deleteFile(name);
}


/*
 * Run a test on a single configuration determined by file size and buffer size
 */
void singleSeekTest(CreateStackFn createStack, char *nameFmt, off_t size, size_t bufSize)
{
    char fileName[PATH_MAX];
    snprintf(fileName, sizeof(fileName), nameFmt, size, bufSize);
    beginTest(fileName);

	/* Inject the procedure to create an I/O Stack */
	setTestStack(createStack(bufSize));

	/* Quick regression test */
	regression(fileName, bufSize);

    /* create and read back as a stream */
    generateFile(fileName, size, bufSize);
    verifyFile(fileName, size, bufSize);

    /* Fill in the file with garbage, then write it out as random writes */
    allocateFile(fileName, size, bufSize);
    generateRandomFile(fileName, size, bufSize);
    verifyFile(fileName, size, bufSize);

    /* append to the file */
    appendFile(fileName, size, bufSize);
    verifyFile(fileName, size + bufSize, ROUNDDOWN(16 * 1024, bufSize));  /* larger buffer */

    /* Read back as random reads */
    verifyRandomFile(fileName, size + bufSize, bufSize);

    /* Clean things up */
    deleteFile(fileName);
}

/* run a matrix of tests for various file sizes and I/O sizes.  All will use a 1K block size. */
void seekTest(CreateStackFn testStack, char *nameFmt)
{
    for (int fileIdx = 0; fileIdx<countof(fileSize); fileIdx++)
        for (int bufIdx = 0; bufIdx<countof(blockSize); bufIdx++)
            if  (fileSize[fileIdx] / blockSize[bufIdx] < 4 * 1024 * 1024)  // Keep nr blocks under 4M to complete in reasonable time.
                singleSeekTest(testStack, nameFmt, fileSize[fileIdx], blockSize[bufIdx]);
}



/* Run a test on a single configuration determined by file size and buffer size */
void singleStreamTest(CreateStackFn createStack, char *nameFmt, off_t size, size_t bufferSize)
{
    char fileName[PATH_MAX];
    snprintf(fileName, sizeof(fileName), nameFmt, size, bufferSize);

    beginTest(fileName);

	/* Create the prototype TEST I/O stack */
	setTestStack(createStack(bufferSize));

	/* Quick regression test */
	regression(fileName, bufferSize);

	generateFile(fileName, size, bufferSize);
    verifyFile(fileName, size, bufferSize);

    appendFile(fileName, size, bufferSize);
    verifyFile(fileName, size + bufferSize, 16 * 1024);


    /* Clean things up */
    deleteFile(fileName);
}


/* run a matrix of tests for various file sizes and buffer sizes */
void streamTest(CreateStackFn testStack, char *nameFmt)
{
    for (int fileIdx = 0; fileIdx<countof(fileSize); fileIdx++)
        for (int bufIdx = 0; bufIdx<countof(blockSize); bufIdx++)
			if  (fileSize[fileIdx] / blockSize[bufIdx] < 4 * 1024 * 1024)  // Keep nr blocks under 4M to complete in reasonable time.
                singleStreamTest(testStack, nameFmt, fileSize[fileIdx], blockSize[bufIdx]);
}



/* Run a test on a single configuration determined by file size and buffer size */
void singleReadSeekTest(CreateStackFn createStack, char *nameFmt, off_t fileSize, size_t bufferSize)
{
    char fileName[PATH_MAX];
    snprintf(fileName, sizeof(fileName), nameFmt, fileSize, bufferSize);

    beginTest(fileName);

	/* Set up the I/O stack we want to test */
	setTestStack(createStack(bufferSize));

	generateFile(fileName, fileSize, bufferSize);
    verifyFile(fileName, fileSize, bufferSize);

    verifyRandomFile(fileName, fileSize, bufferSize);

    appendFile(fileName, fileSize, bufferSize);
    verifyRandomFile(fileName, fileSize + bufferSize, bufferSize);

	regression(fileName, bufferSize);

    /* Clean things up */
    deleteFile(fileName);
}

/* run a matrix of tests for various file sizes and buffer sizes */
void readSeekTest(CreateStackFn testStack, char *nameFmt)
{
    for (int fileIdx = 0; fileIdx<countof(fileSize); fileIdx++)
        for (int bufIdx = 0; bufIdx<countof(blockSize); bufIdx++)
			if  (fileSize[fileIdx] / blockSize[bufIdx] < 4 * 1024 * 1024)  // Keep nr blocks under 4M to complete in reasonable time.
                singleReadSeekTest(testStack, nameFmt, fileSize[fileIdx], blockSize[bufIdx]);
}

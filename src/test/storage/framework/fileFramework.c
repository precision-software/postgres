/**/

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/fcntl.h>
#include <unistd.h>

#include "storage/iostack_internal.h"
#include "fileFramework.h"
#include "unitTestInternal.h"
#include "storage/fd.h"

#define countof(array) (sizeof(array)/sizeof(array[0]))

/* Matrix of file and block sizes for testing. */
off_t fileSize[] = {0, 1024, 1, 64, 1027, 7*1024, 32*1024 + 127};
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
			debug("verifyBuffer: i=%zu position=%zu  buf[i]=%c expected=%c\n", i, position, buf[i], expected);
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
    debug("generateFile: path=%s\n", path);
    File file = FileOpen( path, O_WRONLY|O_CREAT|O_TRUNC);
	PG_ASSERT(file != -1);
    Byte *buf = malloc(bufferSize); /* TODO: make buf be at end of struct */

    off_t position;
    for (position = 0; position < size; position += bufferSize)
    {
        off_t expected = MIN(bufferSize, size-position);
        generateBuffer(position, buf, expected);
        size_t actual = FileWrite(file, buf, expected, position, 0);
        PG_ASSERT_EQ(expected, actual);
    }

    free(buf);
    PG_ASSERT(FileClose(file) == 0);
}

/* Verify a iostack has the correct data */
static void verifyFile(char *path, off_t size, size_t bufferSize)
{
    debug("verifyFile: path=%s\n", path);
    File file = FileOpen(path, O_RDONLY);
	PG_ASSERT(file >= 0);
	PG_ASSERT(!FileEof(file));
	PG_ASSERT(!FileError(file));
    Byte *buf = malloc(bufferSize);

    for (off_t actual, position = 0; position < size; position += actual)
    {
        size_t expected = MIN(bufferSize, size - position);
        actual = FileRead(file, buf, bufferSize, position, 0);
        PG_ASSERT_EQ(expected, actual);
        PG_ASSERT(verifyBuffer(position, buf, actual));
		PG_ASSERT(!FileEof(file));
		PG_ASSERT(!FileError(file));
    }

    // Read a final EOF.
	PG_ASSERT(!FileEof(file));
    FileRead(file, buf, 1, size, 0);
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
static void allocateFile(char *path, off_t size, size_t bufferSize)
{
    debug("allocateFile: path=%s\n", path);
    /* Start out by allocating space and filling the file with "X"s. */
    File file = FileOpen(path, O_WRONLY|O_CREAT|O_TRUNC);
    Byte *buf = malloc(bufferSize);
    memset(buf, 'X', bufferSize);

    off_t position;
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
    debug("generateRandomFile: path=%s\n", path);
    /* The nr of blocks must be relatively prime to "prime", otherwise we won't visit all the blocks. */
    size_t nrBlocks = (size + blockSize - 1) / blockSize;
    PG_ASSERT( nrBlocks == 0 || (nrBlocks % prime) != 0);

    File file = FileOpen(path, O_RDWR);
	PG_ASSERT(file >= 0);
    Byte *buf = malloc(blockSize);


    for (off_t idx = 0; idx < nrBlocks; idx++)
    {
        /* Pick a pseudo-random block and seek to it */
        off_t position = ((idx * prime) % nrBlocks) * blockSize;
        //printf("fileSeek - idx = %u  blockNr=%u nrBlocks=%u\n", idx, (idx*prime)%nrBlocks, nrBlocks);

        /* Generate data appropriate for that block. */
        size_t expected = (size_t)MIN(blockSize, size - position);
        generateBuffer(position, buf, expected);

        /* Write the block */
        size_t actual = FileWrite(file, buf, expected, position, 0);
        PG_ASSERT_EQ(actual, expected);
    }

    PG_ASSERT(FileClose(file) == 0);
}

static void appendFile(char *path, off_t size, size_t blockSize)
{
    debug("appendFile: path=%s\n", path);
    File file = FileOpen(path, O_RDWR);
	PG_ASSERT(file >= 0);
    Byte *buf = malloc(blockSize);

    /* Seek to the end of the file - should match file size */
    off_t endPosition = FileSize(file);

    /* Write a new block at the end of file */
    generateBuffer(endPosition, buf, blockSize);

    /* Write the block */
    size_t actual = FileWrite(file, buf, blockSize, endPosition, 0);
    PG_ASSERT_EQ(actual, blockSize);

    /* Close the file and verify it is correct. */
    PG_ASSERT(FileClose(file) == 0);

    verifyFile(path, size+blockSize, blockSize);
}

/*
 * Verify a ioStack has the correct data through randomlike seeks.
 * This should do a complete verification - examining every byte of the ioStack.
 */
static void verifyRandomFile(char *path, off_t size, size_t blockSize)
{
    debug("verifyRandomFile: path=%s\n", path);
	File file = FileOpen(path, O_RDONLY);
	PG_ASSERT(file >= 0);
    Byte *buf = malloc(blockSize);

    size_t nrBlocks = (size + blockSize -1) / blockSize;
    PG_ASSERT(nrBlocks == 0 || (nrBlocks % prime) != 0);
    for (size_t idx = 0;  idx < nrBlocks; idx++)
    {
        /* Pick a pseudo-random block and read it */
        off_t position = ((idx * prime) % nrBlocks) * blockSize;

        size_t actual = FileRead(file, buf, blockSize, position, 0);

        /* Verify we read the correct data */
        size_t expected = MIN(blockSize, size-position);
        PG_ASSERT_EQ(actual, expected);
        PG_ASSERT(verifyBuffer(position, buf, actual));
    }

    PG_ASSERT(FileClose(file) == 0);
}


static void deleteFile(char *name)
{
	unlink(name);
}


static void regression(char *name)
{

    deleteFile(name);

	/* Shouldn't open a non-existent file - various modes) */
	File file = FileOpen(name, O_RDWR);
	PG_ASSERT(file == -1);
	PG_ASSERT_EQ(errno, ENOENT);

	file = FileOpen(name, O_RDONLY);
	PG_ASSERT(file == -1);
	PG_ASSERT_EQ(errno, ENOENT);

	/* OK to create a file and reopen readonly */
	file = FileOpen(name, O_CREAT | O_WRONLY | O_TRUNC);
	PG_ASSERT(file >= 0);
	PG_ASSERT(FileClose(file) == 0);

	file = FileOpen(name, O_CREAT | O_WRONLY | O_TRUNC);
	PG_ASSERT(file >= 0);
	PG_ASSERT(FileClose(file) == 0);

	/* EBADF if closing an already closed file */
	PG_ASSERT(FileClose(file) != 0 && errno == EBADF);

	/* Shouldn't open an already open file */

	/* Should read EOF on empty file */

	/* Should seek to end and read EOF */

	deleteFile(name);
}


/*
 * For testing, we create a stack based on block size
 * but there are no parameters in the running code.
 * In a functional language we would simply bind the stack size,
 * but here we kludge it by passing size as an external, static value.
 */
typedef IoStack *(*CreateTestStackFn)(size_t blockSize);

static size_t testBlockSize;
static CreateTestStackFn createTestStack;

static IoStack *createStack()
{
	return createTestStack(testBlockSize);
}

extern IoStackCreateFunction ioStackCreateFunction[4];


/* Run a test on a single configuration determined by file size and buffer size */
void singleSeekTest(CreateTestStackFn testStack, char *nameFmt, off_t size, size_t bufferSize)
{
    char fileName[PATH_MAX];
    snprintf(fileName, sizeof(fileName), nameFmt, size, bufferSize);
    beginTest(fileName);

	/* Inject the procedure to create an I/O Stack */
	createTestStack = testStack;
	ioStackCreateFunction[0] = createStack;

    /* create and read back as a stream */
    generateFile(fileName, size, bufferSize);
    verifyFile(fileName, size, bufferSize);

    /* Fill in the file with garbage, then write it out as random writes */
    allocateFile(fileName, size, bufferSize);
    generateRandomFile(fileName, size, bufferSize);
    verifyFile(fileName, size, bufferSize);

    /* append to the file */
    appendFile(fileName, size, bufferSize);
    verifyFile(fileName, size+bufferSize, 16*1024);

    /* Read back as random reads */
    verifyRandomFile(fileName, size+bufferSize, bufferSize);

	regression(fileName);

    /* Clean things up */
    deleteFile(fileName);
}

/* run a matrix of tests for various file sizes and I/O sizes.  All will use a 1K block size. */
void seekTest(CreateTestStackFn testStack, char *nameFmt)
{
    for (int fileIdx = 0; fileIdx<countof(fileSize); fileIdx++)
        for (int bufIdx = 0; bufIdx<countof(blockSize); bufIdx++)
            if  (fileSize[fileIdx] / blockSize[bufIdx] < 4 * 1024 * 1024)  // Keep nr blocks under 4M to complete in reasonable time.
                singleSeekTest(testStack, nameFmt, fileSize[fileIdx], blockSize[bufIdx]);
}



/* Run a test on a single configuration determined by file size and buffer size */
void singleStreamTest(CreateTestStackFn testStack, char *nameFmt, off_t size, size_t bufferSize)
{
    char fileName[PATH_MAX];
    snprintf(fileName, sizeof(fileName), nameFmt, size, bufferSize);

    beginTest(fileName);

	/* Inject the procedure to create an I/O Stack */
	createTestStack = testStack;
	ioStackCreateFunction[0] = createStack;

	generateFile(fileName, size, bufferSize);
    verifyFile(fileName, size, bufferSize);

    appendFile(fileName, size, bufferSize);
    verifyFile(fileName, size + bufferSize, 16 * 1024);

	regression(fileName);

    /* Clean things up */
    deleteFile(fileName);
}


/* run a matrix of tests for various file sizes and buffer sizes */
void streamTest(CreateTestStackFn testStack, char *nameFmt)
{
    for (int fileIdx = 0; fileIdx<countof(fileSize); fileIdx++)
        for (int bufIdx = 0; bufIdx<countof(blockSize); bufIdx++)
            singleStreamTest(testStack, nameFmt, fileSize[fileIdx], blockSize[bufIdx]);
}



/* Run a test on a single configuration determined by file size and buffer size */
void singleReadSeekTest(CreateTestStackFn testStack, char *nameFmt, off_t size, size_t bufferSize)
{
    char fileName[PATH_MAX];
    snprintf(fileName, sizeof(fileName), nameFmt, size, bufferSize);

    beginTest(fileName);

	/* Inject the procedure to create an I/O Stack */
	createTestStack = testStack;
	ioStackCreateFunction[0] = createStack;

	generateFile(fileName, size, bufferSize);
    verifyFile(fileName, size, bufferSize);

    verifyRandomFile(fileName, size, bufferSize);

    appendFile(fileName, size, bufferSize);
    verifyRandomFile(fileName, size + bufferSize, bufferSize);

	regression(fileName);

    /* Clean things up */
    deleteFile(fileName);

}


/* run a matrix of tests for various file sizes and buffer sizes */
void readSeekTest(CreateTestStackFn testStack, char *nameFmt)
{
    for (int fileIdx = 0; fileIdx<countof(fileSize); fileIdx++)
        for (int bufIdx = 0; bufIdx<countof(blockSize); bufIdx++)
            singleReadSeekTest(testStack, nameFmt, fileSize[fileIdx], blockSize[bufIdx]);
}

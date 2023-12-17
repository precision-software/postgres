/*  */
#include <stdlib.h>

#include "storage/fd.h"
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"
#include "storage/iostack_internal.h"

static void testCorruptedFile(const char *name, off_t fileSize, size_t blockSize);


static uint64 getSequenceNr(void)
{
	static uint64 sequenceNr = 1;
	return sequenceNr++;
}

static void *createStack(size_t blockSize)
{
	return bufferedNew(blockSize, aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32, getSequenceNr, vfdStackNew()));
}

void testMain()
{
	system("rm -rf " TEST_DIR "encryption; mkdir -p " TEST_DIR "encryption");

	beginTestGroup("AEAD Encrypted Files");
	beginTest("AEAD Encrypted Files");

	/* Generate encryption errors and verify failure */
	testCorruptedFile(TEST_DIR "encryption/testfile_corrupt", 64*1024, 8*1024);

	singleSeekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat", 0, 64);
	seekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat");
}

static void
testCorruptedFile(const char *name, off_t fileSize, size_t blockSize)
{
	int64 rawFileSize;
	File file;

	/* Inject the procedure to create an I/O Stack */
	setTestStack(createStack(blockSize));

	/* Create a test file */
	generateFile(name, fileSize, blockSize);
	PG_ASSERT(verifyFile(name, fileSize, blockSize));

	/* Extend the file with an extra byte and it should fail */
	file = FOpen(name, PG_RAW | O_RDWR);
	PG_ASSERT(file >= 0);
	rawFileSize = FSize(file);
	PG_ASSERT(rawFileSize >= fileSize && rawFileSize > 4);
	PG_ASSERT(FResize(file, rawFileSize+1, 0));
	PG_ASSERT(FClose(file));
	PG_ASSERT(!verifyFile(name, fileSize, blockSize));

	/* Truncate the original file by a byte and it should fail. */
	file = FOpen(name, PG_RAW | O_RDWR);
	PG_ASSERT(file >= 0);
	PG_ASSERT(FResize(file, rawFileSize - 1, 0));
	PG_ASSERT(FClose(file));
	PG_ASSERT(!verifyFile(name, fileSize, blockSize));

	/* Restore the original size, but zero out the last word. Should Fail */
	file = FOpen(name, PG_RAW | O_RDWR);
	PG_ASSERT(file >= 0);
	PG_ASSERT(FResize(file, rawFileSize-4, 0));
	PG_ASSERT(FResize(file, rawFileSize, 0));
	PG_ASSERT(FClose(file));
	PG_ASSERT(!verifyFile(name, fileSize, blockSize));

	unlink(name);
}

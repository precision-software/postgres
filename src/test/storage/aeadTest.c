/*  */
#include <stdlib.h>

#include "storage/iostack.h"
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"

uint64 getSequenceNr(void);


uint64 getSequenceNr()
{
	static uint64 sequenceNr = 1;
	return sequenceNr++;
}

static void *createStack(size_t blockSize)
{
	return aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32, getSequenceNr, vfdStackNew());
}

void testMain()
{
	system("rm -rf " TEST_DIR "encryption; mkdir -p " TEST_DIR "encryption");

	beginTestGroup("AEAD Encrypted Files");
	beginTest("AEAD Encrypted Files");

	singleSeekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat", 1024, 4096);
	seekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat");
}

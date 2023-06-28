/*  */
#include <stdio.h>
#include <stdlib.h>
#include <sys/fcntl.h>

#include "storage/iostack.h"
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"

uint64 getSequenceNr(void);


uint64 getSequenceNr()
{
	static uint64 sequenceNr = 1;
	return sequenceNr++;
}

static IoStack *createStack(size_t blockSize)
{
	return aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32, getSequenceNr, vfdStackNew(1));
}

void testMain()
{
    system("rm -rf " TEST_DIR "encryption; mkdir -p " TEST_DIR "encryption");

    beginTestGroup("AES Encrypted Files");
	beginTest("AES Encrypted Files");

    singleSeekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat", 1024, 4096);
    seekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat");
}

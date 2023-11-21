/*
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/fcntl.h>
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"

#include "storage/iostack.h"

static void *
createStack(size_t blockSize)
{
	return bufferedNew(0, aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32, vfdStackNew()));
}

void testMain()
{
    system("rm -rf " TEST_DIR "aead; mkdir -p " TEST_DIR "aead");

	beginTest("Storage");
    beginTestGroup("aead Stack");
	singleSeekTest(createStack, TEST_DIR "aead/testfile_%u_%u.dat", 1024, 4096);
    seekTest(createStack, TEST_DIR "aead/testfile_%u_%u.dat");
}

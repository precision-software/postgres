/*  */
#include <stdio.h>
#include <stdlib.h>
#include <sys/fcntl.h>

#include "storage/iostack.h"
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"

static IoStack *createStack(size_t blockSize)
{
	return bufferedNew(64,
			 aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32,
				   vfdStackNew()));
}

void testMain()
{
    system("rm -rf " TEST_DIR "encryption; mkdir -p " TEST_DIR "encryption");

    beginTestGroup("AES Encrypted Files");
	beginTest("AES Encrypted Files");

    //singleSeekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat", 1027, 1);
    seekTest(createStack, TEST_DIR "encryption/testfile_%u_%u.dat");
}

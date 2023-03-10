/*
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/fcntl.h>
#include "storage/iostack.h"
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"

static IoStack *createStack(size_t blockSize)
{
	return 	bufferedNew(1024,
				lz4CompressNew(1024,
					bufferedNew(1024,
						aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32,
							vfdStackNew())),
					 bufferedNew(1024,
						aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32,
							vfdStackNew()))));


}

void testMain()
{
	system("rm -rf " TEST_DIR "raw; mkdir -p " TEST_DIR "raw");

	beginTestGroup("Raw Files");
	singleReadSeekTest(createStack, TEST_DIR "buffered/testfile_%u_%u.dat", 1027, 1024);
	readSeekTest(createStack, TEST_DIR "buffered/testfile_%u_%u.dat");
}

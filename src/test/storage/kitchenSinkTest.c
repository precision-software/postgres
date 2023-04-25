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
	return 	bufferedNew(1,
				lz4CompressNew(16*1024,
					bufferedNew(16*1024,
						aeadNew(NULL, blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32,
							vfdStackNew())),
					 bufferedNew(16*1024,
						aeadNew("AES-256-GCM", blockSize, (Byte *) "0123456789ABCDEF0123456789ABCDEF", 32,
							vfdStackNew()))));

}

void testMain()
{
	system("rm -rf " TEST_DIR "kitchensink; mkdir -p " TEST_DIR "kitchensink");

	beginTestGroup("Raw Files");
	singleReadSeekTest(createStack, TEST_DIR "kitchensink/testfile_%u_%u.dat", 32895, 3429);
	readSeekTest(createStack, TEST_DIR "kitchensink/testfile_%u_%u.dat");
}

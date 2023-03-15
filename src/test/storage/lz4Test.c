/*  */
#include <stdio.h>
#include <sys/fcntl.h>
#include "storage/iostack.h"
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"

static IoStack *createStack(size_t blockSize)
{
	return bufferedNew(64, lz4CompressNew(blockSize, bufferedNew(64, vfdStackNew()), bufferedNew(64,vfdStackNew())));
}


void testMain()
{
    system("rm -rf " TEST_DIR "compressed; mkdir -p " TEST_DIR "compressed");


    beginTestGroup("LZ4 Compression");
	beginTest("LZ4 Compression");

    singleReadSeekTest(createStack, TEST_DIR "compressed/testfile_%u_%u.lz4", 1024, 1024);
    readSeekTest(createStack, TEST_DIR "compressed/testfile_%u_%u.lz4");

}

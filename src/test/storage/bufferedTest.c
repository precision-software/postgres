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
	return bufferedNew(1024, vfdStackNew());
}

void testMain()
{
	system("rm -rf " TEST_DIR "buffered; mkdir -p " TEST_DIR "buffered");

	beginTestGroup("Raw Files");
	singleSeekTest(createStack, TEST_DIR "buffered/testfile_%u_%u.dat", 0, 3096);
	seekTest(createStack, TEST_DIR "buffered/testfile_%u_%u.dat");
}

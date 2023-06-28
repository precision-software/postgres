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
	return pagedNew(8*1024, vfdStackNew(1));
}

void testMain()
{
	system("rm -rf " TEST_DIR "paged; mkdir -p " TEST_DIR "paged");

	beginTestGroup("Raw Files");
	singleSeekTest(createStack, TEST_DIR "paged/testfile_%u_%u.dat", 0, 3096);
	seekTest(createStack, TEST_DIR "paged/testfile_%u_%u.dat");
}

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
createStack(size_t bufsize)
{
	return bufferedNew(8*1024, vfdStackNew());
}

void testMain()
{
    system("rm -rf " TEST_DIR "buffered; mkdir -p " TEST_DIR "buffered");

	beginTest("Storage");
    beginTestGroup("Buffered Stack");
	singleSeekTest(createStack, TEST_DIR "buffered/testfile_%u_%u.dat", 1024, 4096);
    seekTest(createStack, TEST_DIR "buffered/testfile_%u_%u.dat");
}

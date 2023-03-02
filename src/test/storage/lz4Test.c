/*  */
#include <stdio.h>
#include <sys/fcntl.h>
#include "storage/iostack.h"
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"


void testMain()
{
    system("rm -rf " TEST_DIR "compressed; mkdir -p " TEST_DIR "compressed");

    beginTestGroup("LZ4 Compression");
    IoStack *lz4 =
                bufferedNew(1024,
                    lz4CompressNew(1024,
                        bufferedNew(1024, vfdStackNew()),
						bufferedNew(1024, vfdStackNew())));

    singleReadSeekTest(lz4, TEST_DIR "compressed/testfile_%u_%u.lz4", 1024, 64);
    readSeekTest(lz4, TEST_DIR "compressed/testfile_%u_%u.lz4");

}

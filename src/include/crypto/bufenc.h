/*-------------------------------------------------------------------------
 *
 * bufenc.h
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/crypto/bufenc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFENC_H
#define BUFENC_H

#include "storage/bufmgr.h"
#include "crypto/kmgr.h"

/* Cluster encryption encrypts only main forks */
#define PageNeedsToBeEncrypted(forknum) \
	(FileEncryptionEnabled && (forknum) == MAIN_FORKNUM)


extern void InitializeBufferEncryption(void);
extern void EncryptPage(Page page, bool relation_is_permanent,
						BlockNumber blkno, RelFileNumber fileno);
extern void DecryptPage(Page page, bool relation_is_permanent,
						BlockNumber blkno, RelFileNumber fileno);

#endif							/* BUFENC_H */

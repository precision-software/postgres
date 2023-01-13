/*
 * TODO: bring into postgres standards
 */
#ifndef POSTGRES_IOSTACK_H
#define POSTGRES_IOSTACK_H

#include <sys/types.h>
#include "storage/fd.h"
#include "c.h"

/* Contains a prototype FilePipeline for opening new files */
extern void *IoStackPrototype;

/* VFD equivalent routines which invoke IoStacks instead of VFDs */
File IoStackOpen(const char *fileName, int fileFlags, mode_t fileMode);
void IoStackClose(File file);
int IoStackPrefetch(File file, off_t offset, off_t amount, uint32 wait_event_info);
void IoStackWriteback(File file, off_t offset, off_t nbytes, uint32 wait_event_info);
int IoStackRead(File file, void *buffer, size_t amount, off_t offset, uint32 wait_event_info);
int IoStackWrite(File file, void *buffer, size_t amount, off_t offset, uint32 wait_event_info);
int IoStackSync(File file, uint32 wait_event_info);
off_t IoStackSize(File file);
int IoStackTruncate(File file, off_t offset, uint32 wait_event_info);

/* Open flag to force opening VFDs directly, avoiding use of IoStacks *
#define PG_O_VFD 0x200000


#endif /* POSTGRES_IOSTACK_H */

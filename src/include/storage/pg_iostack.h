/*
 * TODO: bring into postgres standards
 */

#ifndef STORAGE_IOSTACK_H
#define STORAGE_IOSTACK_H

#ifdef DEBUG
#define debug(...) elog(DEBUG2, __VA_ARGS__)
#define FDDEBUG
#else
#define debug(...) ((void)0)
#endif

/* Open flag to request opening with IO Stacks */
#define PG_IOSTACK (0x2000000)

/*
 * If we are NOT using I/O Stacks, then dummy out the procedures which use them
 */
#ifdef NOT_NOW
#define noop  ((void)0)
#define IoStackPrototype NULL
#define IoStackOpen(proto, fileName, fileFlags, fileMode) noop
#define IoStackClose(file) noop
#define IoStackPrefetch(file, offset, amount,wait_event_info) noop
#define IoStackWriteback(file, offset, nbytes, wait_event_info) noop
#define IoStackRead(file, offsset, buffer, amount, wait_event_info) noop
#define IoStackWrite(file, offsset, buffer, amount, wait_event_info) noop
#define IoStackSync(file, wait_event_info) noop
#define IoStackSize(file)  noop
#define IoStackTruncate(file, offset, wait_event_info) noop
#undef noop

/*
 * If we are using I/O Stacks, then use the following routines
 */
#else
#include <sys/types.h>
#include "/usr/local/include/iostack.h"  /* <iostack.h> */
#include "storage/fd.h"
#include "c.h"

/* Contains a prototype FilePipeline for opening new files */
void IoStackSetup(void);
extern void *IoStackPrototype;

typedef struct VfdBottom VfdBottom;
VfdBottom *vfdBottomNew(void);


/* VFD equivalent routines which invoke IoStacks instead of VFDs */
IoStack *IoStackOpen(IoStack *proto, const char *fileName, int fileFlags, mode_t fileMode);
int IoStackClose(IoStack *iostack, char *deleteName);
int IoStackPrefetch(IoStack *iostack, off_t offset, off_t amount, uint32 wait_event_info);
void IoStackWriteback(IoStack *iostack, off_t offset, off_t nbytes, uint32 wait_event_info);
int IoStackRead(IoStack *iostack, void *buffer, size_t amount, off_t offset, uint32 wait_event_info);
int IoStackWrite(IoStack *iostack, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info);
int IoStackSync(IoStack *iostack, uint32 wait_event_info);
off_t IoStackSize(IoStack *iostack);
int IoStackTruncate(IoStack *iostack, off_t offset, uint32 wait_event_info);



#endif
#endif /* STORAGE_IOSTACK_H */

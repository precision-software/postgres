

#ifndef VFD_H
#define VFD_H
#include "utils/resowner_private.h"

#define VFD_CLOSED (-1)

#define FileIsValid(file) \
	((file) > 0 && (file) < (int) SizeVfdCache && VfdCache[file].fileName != NULL)

/* Point to the corresponding VfdCache entry if the file index is valid */
#define getVfd(file) \
	(Assert(FileIsValid(file)), &VfdCache[file])

#define FileIsNotOpen(file) (VfdCache[file].fd == VFD_CLOSED)

/* these are the assigned bits in fdstate below: */
#define FD_DELETE_AT_CLOSE	(1 << 0)	/* T = delete when closed */
#define FD_CLOSE_AT_EOXACT	(1 << 1)	/* T = close at eoXact */
#define FD_TEMP_FILE_LIMIT	(1 << 2)	/* T = respect temp_file_limit */

typedef struct vfd
{
	int			fd;				/* current FD, or VFD_CLOSED if none */
	unsigned short fdstate;		/* bitflags for VFD's state */
	ResourceOwner resowner;		/* owner, for automatic cleanup */
	File		nextFree;		/* link to next free VFD, if in freelist */
	File		lruMoreRecently;	/* doubly linked recency-of-use list */
	File		lruLessRecently;
	off_t		fileSize;		/* current size of file (0 if not temporary) */
	char	   *fileName;		/* name of file, or NULL for unused VFD */
	/* NB: fileName is malloc'd, and must be free'd when closing the VFD */
	int			fileFlags;		/* open(2) flags for (re)opening the file */
	mode_t		fileMode;		/* mode to pass to open(2) */
	off_t 		offset;			/* current position for sequential reads/writes */
	int 		errNo;			/* error from last operation. (Note "errno" is a macro on some platforms) */
	bool		eof;			/* whether an EOF occurred */
} Vfd;

/*
 * Virtual File Descriptor array pointer and size.  This grows as
 * needed.  'File' values are indexes into this array.
 * Note that VfdCache[0] is not a usable VFD, just a list header.
 */
extern Vfd *VfdCache;
extern Size SizeVfdCache;



#endif //VFD_H

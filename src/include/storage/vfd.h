

#ifndef VFD_H
#define VFD_H
#include "utils/resowner_private.h"
#include "storage/iostack.h"
#include "storage/fd.h"

#define VFD_CLOSED (-1)


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
	IoStack     *ioStack;		/* The I/O stack which is managing this file */
} Vfd;

/*
 * Virtual File Descriptor array pointer and size.  This grows as
 * needed.  'File' values are indexes into this array.
 * Note that VfdCache[0] is not a usable VFD, just a list header.
 */
extern Vfd *VfdCache;
extern Size SizeVfdCache;

/* True if the file index is valid */
static inline bool FileIsValid(File file)
{
	return (file > 0 && file < SizeVfdCache && VfdCache[file].fileName != NULL);
}


/* Point to the corresponding VfdCache entry if the file index is valid */
static inline Vfd *getVfd(File file)
{
	//Assert(FileIsValid(file));
	return &VfdCache[file];
}

/* Point to the file's I/O Stack */
static inline IoStack *getStack(File file)
{
	//Assert(getVfd(file)->ioStack != NULL);
	return getVfd(file)->ioStack;
}

/* Get the file's name, mainly for debugging */
static inline const char *getName(File file)
{
	if (file < 0)
		return "FileClosed";
	return getVfd(file)->fileName;
}

#define FileIsNotOpen(file) (VfdCache[file].fd == VFD_CLOSED)

/* open flags to enable encryption. */
#define PG_NOCRYPT        (0 << 28)
#define PG_ENCRYPT        (1 << 28)
#define PG_ECOMPRESS      (2 << 28)
#define PG_ENCRYPT_PERM   (3 << 28)
#define PG_TESTSTACK      (4 << 28)

#define PG_STACK_MASK     (7 << 28)

/* Point to an I/O Stack create function for unit testing */
extern IoStack *(*testStackNew)();



#endif //VFD_H

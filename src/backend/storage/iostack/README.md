# I/O Stacks
I/O stacks provide a uniform interface for accessing PostgreSql files. 
By consolidating file I/O into a single set of procedures, it becomes possible to implement
encryption and compression on all files which could possibly contain user data, not just the 
database tables and logs.

The types of files managed by I/O stacks include:
 - Temporary files which are closed or deleted at the end of a transaction.
 - Internal files  shared between PostgreSql backends.
 - Configuration files
 - Statistics and status files.
 - BufPage files for temporarily storing relational data.

Note I/O stacks are NOT used for buffered relational files (tables) or for the Write Ahead Log (WAL).
Those files have special encryption requirements and are handled separately.

## Interface
I/O stacks present two consistent interfaces. 1) fread/fwrite/ferror equivalents for doing
buffered sequential I/O, and 2) pread/pwrite for doing random I/O. Many of the existing procedures
are unchanged.

The file access calls can be summarized as:
 - FileOpen() - Opens a file, basically an alias for the existing PathNameOpenFile().
 - FileWrite(...) - no change from the existing  FileWrite() call, similar to pwrite().
 - FileWriteSeq(...) - a sequenctial version of FileWrite, similar to fwrite().
 - FileRead(...) - no change from the existingFileRead() call, similar to pread.
 - FileReadSeq(...) - a sequential version of FileRead.
 - FileSync(), FileSize(), FileTrunc() - match the existing routines.
 - FileClose() - close a file and release all internal resources.
 - FileEof() - true if the last read was EOF.
 - FileError() - true if the last operation generated an error.

The error handling saves and restores *errno*, so existing code in PostGres may continue
to test for -1 and access errno directly.

## Features
I/O stacks are intended to support encryption, compression and buffering for a variety of PostgreSql
data files. The particular I/O stack is selected based on the *o_flag* parameter passed to *FileOpen()*.

The following stacks will be supported initially, corresponding to the following open flags.
  
<br> PG_ENCRYPT : Encrypted and compressed using a permenent encryption key.
<br>PG_TEMP: Encrypted and compressed using a session (temporary) encryption key.
<br> PG_BUFFERED : Buffered plaintext files.
<br> empty : Unbuffered "raw" files.

Note I/O stacks integrate with PostgreSql's existing file management, meaning temporary
files will be closed when a transaction ends, anonymous files will be deleted when closed, and
files will be allocated to tablespaces as they are currently.

## Architecture
I/O stacks consist of interchangable modules layered on top of another.
At the top, FileAccess proveds the calls listed earlier.
At the bottom, Vfd accesses PostgreSql's Virtual File Descriptors (VFDs). 
In-between layers provide services like buffering, encryption and compression. 
These in-between layers are interchangable and can stacked together to create the services
needed by PostgreSql.

I/O stacks include the following intermediate layers.
<br>Buffered : General purpose file buffering, similar to fread/fwrite/fseek
<br>Aead: Encrytion and authentication useing the OpenSSL library. (AES-256-GMC)
<br>lz4: Compression using lz4. Compressed files are written sequentially, and may
be read randomly.

<Note: here is a good place to include diagrams.>

## Implementation
### General
 - based on I/O stack prototypes.
 - Open clones the prototype and allocates resources.
 - If unsuccessful, the stack is still created, but in a "closed" configuration.
 - Read/write/sync requests are passed from top to bottom and implemented as simple procedure calls.

#### Open Details
Internally, opening an I/O stack involves cloning an existing "prototype" stack,
configuring it, allocating resources and opening the underlying files.

Each layer in an I/O stack recursively does the following when opened.
- Open its successor
- Clone self based on the prototype.
- Negotiate blockSize with adjoining layers.
- Allocate memory and open files.
- On error, place self in the "closed" state.

At the end of an open call, either 1) the entire stack and its files are successfully opened,
or 2) the I/O stack is created, but in a "closed" state.

The internal open call needs to return 3 distinct values.
1) The cloned IoStack structure
2) blockSize, the block size it is expecting.
3) retVal, the return value from the lowest level open call, in this case a VFD.

C only supports a single return value, so blockSize and retVal are packaged in the 
returned IoStack structure.

#### Close Details
Once closed, the layer's substack is recursively closed and freed, and all of the layer's 
own resources are released. A "bare" IoStack structure still exists, so it is 
possible for the caller to extract error information. It is up to the caller to explicitly "free"
the bare structure after the stack is closed.

### FileAccess

### Buffered

### Aead Encryption

### Compression

### Vfd

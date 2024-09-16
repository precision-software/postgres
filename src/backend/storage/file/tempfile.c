/*
 * Temporary files.
 */

#include "postgres.h"

#include <dirent.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/resource.h>		/* for getrlimit */
#include <sys/stat.h>
#include <sys/types.h>
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <limits.h>
#include <unistd.h>
#include <fcntl.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_tablespace.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/pg_prng.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/mem.h"
#include "postmaster/startup.h"
#include "storage/fd.h"
#include "storage/fileaccess.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"
#include "utils/resowner.h"
#include "utils/varlena.h"

#include "storage/tempfile.h"

/*
 * Number of temporary files opened during the current session;
 * this is used in generation of tempfile names.
 */
static long tempFileCounter = 0;

/*
 * Array of OIDs of temp tablespaces.  (Some entries may be InvalidOid,
 * indicating that the current database's default tablespace should be used.)
 * When numTempTableSpaces is -1, this has not been set in the current
 * transaction.
 *
 * We export the array so it can be reset in fd.c.
 * We should eventually move the reset to this file.
 */
PGDLLEXPORT Oid *tempTableSpaces = NULL;
PGDLLEXPORT int	numTempTableSpaces = -1;
PGDLLEXPORT int	nextTempTableSpace = 0;
extern PGDLLIMPORT bool temporary_files_allowed;

/* Forward references */
static File OpenTemporaryFileInTablespace(Oid tblspcOid, bool rejectError, bool interXact);

/*
* Open a temporary file that will disappear when we close it.
*
* This routine takes care of generating an appropriate tempfile name.
* There's no need to pass in fileFlags or fileMode either, since only
* one setting makes any sense for a temp file.
*
* Unless interXact is true, the file is remembered by CurrentResourceOwner
* to ensure it's closed and deleted when it's no longer needed, typically at
* the end-of-transaction. In most cases, you don't want temporary files to
* outlive the transaction that created them, so this should be false -- but
* if you need "somewhat" temporary storage, this might be useful. In either
* case, the file is removed when the File is explicitly closed.
*/
File
OpenTemporaryFile(bool interXact)
{
	File		file = 0;

	Assert(temporary_files_allowed);	/* check temp file access is up */

	/*
	 * If some temp tablespace(s) have been given to us, try to use the next
	 * one.  If a given tablespace can't be found, we silently fall back to
	 * the database's default tablespace.
	 *
	 * BUT: if the temp file is slated to outlive the current transaction,
	 * force it into the database's default tablespace, so that it will not
	 * pose a threat to possible tablespace drop attempts.
	 */
	if (numTempTableSpaces > 0 && !interXact)
	{
		Oid			tblspcOid = GetNextTempTableSpace();

		if (OidIsValid(tblspcOid))
			file = OpenTemporaryFileInTablespace(tblspcOid, false, interXact);
	}

	/*
	 * If not, or if tablespace is bad, create in database's default
	 * tablespace.  MyDatabaseTableSpace should normally be set before we get
	 * here, but just in case it isn't, fall back to pg_default tablespace.
	 */
	if (file <= 0)
		file = OpenTemporaryFileInTablespace(MyDatabaseTableSpace ?
											 MyDatabaseTableSpace :
											 DEFAULTTABLESPACE_OID,
											 true, interXact);

	return file;
}

/*
 * Return the path of the temp directory in a given tablespace.
 */
void
TempTablespacePath(char *path, Oid tablespace)
{
	/*
	 * Identify the tempfile directory for this tablespace.
	 *
	 * If someone tries to specify pg_global, use pg_default instead.
	 */
	if (tablespace == InvalidOid ||
		tablespace == DEFAULTTABLESPACE_OID ||
		tablespace == GLOBALTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "base/%s", PG_TEMP_FILES_DIR);
	else
	{
		/* All other tablespaces are accessed via symlinks */
		snprintf(path, MAXPGPATH, "pg_tblspc/%u/%s/%s",
				 tablespace, TABLESPACE_VERSION_DIRECTORY,
				 PG_TEMP_FILES_DIR);
	}
}

/*
 * Open a temporary file in a specific tablespace.
 * Subroutine for OpenTemporaryFile, which see for details.
 */
static File
OpenTemporaryFileInTablespace(Oid tblspcOid, bool rejectError, bool interXact)
{
	char		tempdirpath[MAXPGPATH];
	char		tempfilepath[MAXPGPATH];
	File		file;
	uint64      oflags;

	TempTablespacePath(tempdirpath, tblspcOid);

	/*
	 * Generate a tempfile name that should be unique within the current
	 * database instance.
	 */
	snprintf(tempfilepath, sizeof(tempfilepath), "%s/%s%d.%ld",
			 tempdirpath, PG_TEMP_FILE_PREFIX, MyProcPid, tempFileCounter++);

	/*
	 * Choose oflags. Note we don't use O_EXCL in case there is an orphaned
	 * temp file that can be reused.
     */
	oflags = PG_ENCRYPT | PG_DELETE | PG_TEMP_LIMIT |
			 O_RDWR | O_CREAT | O_TRUNC | PG_BINARY;
	if (!interXact)
		oflags |= PG_XACT;

	/* Open the file. */
	file = FOpen(tempfilepath, oflags);
	if (file <= 0)
	{
		/*
		 * We might need to create the tablespace's tempfile directory, if no
		 * one has yet done so.
		 *
		 * Don't check for an error from MakePGDirectory; it could fail if
		 * someone else just did the same thing.  If it doesn't work then
		 * we'll bomb out on the second create attempt, instead.
		 */
		(void) MakePGDirectory(tempdirpath);

		file = FOpen(tempfilepath, oflags);
		if (file <= 0 && rejectError)
			elog(ERROR, "could not create temporary file \"%s\": %m",
				 tempfilepath);
	}

	return file;
}


/*
 * Create a new file.  The directory containing it must already exist.  Files
 * created this way are subject to temp_file_limit and are automatically
 * closed at end of transaction, but are not automatically deleted on close
 * because they are intended to be shared between cooperating backends.
 *
 * If the file is inside the top-level temporary directory, its name should
 * begin with PG_TEMP_FILE_PREFIX so that it can be identified as temporary
 * and deleted at startup by RemovePgTempFiles().  Alternatively, it can be
 * inside a directory created with PathNameCreateTemporaryDir(), in which case
 * the prefix isn't needed.
 */
File
PathNameCreateTemporaryFile(const char *path, bool error_on_failure)
{
	File		file;
	uint64 		oflags;

	Assert(temporary_files_allowed);	/* check temp file access is up */

	/*
	 * Choose oflags. Note we don't use O_EXCL in case there is an orphaned
	 * temp file that can be reused.
	 */
	oflags = PG_ENCRYPT | PG_TEMP_LIMIT |
		     O_RDWR | O_CREAT | O_TRUNC | PG_BINARY;

	/* Open the file. */
	file = FOpen(path, oflags);
	if (file <= 0 && error_on_failure)
			ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("could not create temporary file \"%s\": %m",
							   path)));

	return file;
}

/*
 * Open a file that was created with PathNameCreateTemporaryFile, possibly in
 * another backend. Files opened this way don't count against the
 * temp_file_limit of the caller, are automatically closed at the end of the
 * transaction but are not deleted on close.
 */
File
PathNameOpenTemporaryFile(const char *path, int oflags)
{
	File		file;

	Assert(temporary_files_allowed);	/* check temp file access is up */

	/* Open the file, but don't throw error if no such file */
	file = FOpen(path, PG_ENCRYPT | PG_XACT | oflags);
	if (file <= 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("could not open temporary file \"%s\": %m",
						   path)));

	return file;
}


/*
 * SetTempTablespaces
 *
 * Define a list (actually an array) of OIDs of tablespaces to use for
 * temporary files.  This list will be used until end of transaction,
 * unless this function is called again before then.  It is caller's
 * responsibility that the passed-in array has adequate lifespan (typically
 * it'd be allocated in TopTransactionContext).
 *
 * Some entries of the array may be InvalidOid, indicating that the current
 * database's default tablespace should be used.
 */
void
SetTempTablespaces(Oid *tableSpaces, int numSpaces)
{
	Assert(numSpaces >= 0);
	tempTableSpaces = tableSpaces;
	numTempTableSpaces = numSpaces;

	/*
	 * Select a random starting point in the list.  This is to minimize
	 * conflicts between backends that are most likely sharing the same list
	 * of temp tablespaces.  Note that if we create multiple temp files in the
	 * same transaction, we'll advance circularly through the list --- this
	 * ensures that large temporary sort files are nicely spread across all
	 * available tablespaces.
	 */
	if (numSpaces > 1)
		nextTempTableSpace = pg_prng_uint64_range(&pg_global_prng_state,
												  0, numSpaces - 1);
	else
		nextTempTableSpace = 0;
}

/*
 * TempTablespacesAreSet
 *
 * Returns true if SetTempTablespaces has been called in current transaction.
 * (This is just so that tablespaces.c doesn't need its own per-transaction
 * state.)
 */
bool
TempTablespacesAreSet(void)
{
	return (numTempTableSpaces >= 0);
}

/*
 * GetTempTablespaces
 *
 * Populate an array with the OIDs of the tablespaces that should be used for
 * temporary files.  (Some entries may be InvalidOid, indicating that the
 * current database's default tablespace should be used.)  At most numSpaces
 * entries will be filled.
 * Returns the number of OIDs that were copied into the output array.
 */
int
GetTempTablespaces(Oid *tableSpaces, int numSpaces)
{
	int			i;

	Assert(TempTablespacesAreSet());
	for (i = 0; i < numTempTableSpaces && i < numSpaces; ++i)
		tableSpaces[i] = tempTableSpaces[i];

	return i;
}

/*
 * GetNextTempTableSpace
 *
 * Select the next temp tablespace to use.  A result of InvalidOid means
 * to use the current database's default tablespace.
 */
Oid
GetNextTempTableSpace(void)
{
	if (numTempTableSpaces > 0)
	{
		/* Advance nextTempTableSpace counter with wraparound */
		if (++nextTempTableSpace >= numTempTableSpaces)
			nextTempTableSpace = 0;
		return tempTableSpaces[nextTempTableSpace];
	}
	return InvalidOid;
}

/*
 * Temporary files.
 * TODO: fill in description.
 */
#ifndef STORAGE_TEMPFILE_H
#define STORAGE_TEMPFILE_H


#include "storage/fd.h"
#include "storage/fileaccess.h"

extern File OpenTemporaryFile(bool interXact);
extern void TempTablespacePath(char *path, Oid tablespace);
extern File PathNameCreateTemporaryFile(const char *path, bool error_on_failure);
extern File PathNameOpenTemporaryFile(const char *path, int mode);
extern void SetTempTablespaces(Oid *tableSpaces, int numSpaces);
extern bool TempTablespacesAreSet(void);
extern Oid GetNextTempTableSpace(void);

extern bool temporary_files_allowed;



#endif /* STORAGE_TEMPFILE_H */

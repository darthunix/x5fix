#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/elog.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbpersistentstore.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define FileSpaceRelationId  5009

Datum
add_entry_gp_persistent_filespace_node(PG_FUNCTION_ARGS);

typedef struct FilespaceDirEntryKey
{
	Oid	filespaceOid;
} FilespaceDirEntryKey;

typedef struct FilespaceDirEntryData
{
	FilespaceDirEntryKey	key;

	int16	dbId1;
	char	locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen];

	int16	dbId2;
	char	locationBlankPadded2[FilespaceLocationBlankPaddedWithNullTermLen];

	PersistentFileSysState	state;

	int64					persistentSerialNum;
	ItemPointerData 		persistentTid;

} FilespaceDirEntryData;
typedef FilespaceDirEntryData *FilespaceDirEntry;


PG_FUNCTION_INFO_V1(add_entry_gp_persistent_filespace_node);

Datum
add_entry_gp_persistent_filespace_node(PG_FUNCTION_ARGS)
{
    FilespaceDirEntry fde;
    PersistentFileSysObjName fsObjName;
    Oid filespace;
    int16 p_dbid;
    char *p_path;
    int16 m_dbid;
    char *m_path;
    ItemPointerData persistentTid;

    filespace = PG_GETARG_OID(0);
    p_dbid = PG_GETARG_INT16(1);
    p_path = TextDatumGetCString(PG_GETARG_DATUM(2));
    m_dbid = PG_GETARG_INT16(3);
    m_path = TextDatumGetCString(PG_GETARG_DATUM(4));

    WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	if (Persistent_BeforePersistenceWork())
		elog(ERROR, "persistent table changes forbidden");
	PersistentFilespace_VerifyInitScan();
	PersistentFileSysObjName_SetFilespaceDir(&fsObjName, filespace);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;
    LWLockAcquire(FilespaceHashLock, LW_SHARED);

    // set filespaceOid
    fde = PersistentFilespace_FindDirUnderLock(filespace);
    if (fde == NULL)
        elog(ERROR, "did not find persistent filespace entry %u", filespace);

    fde->dbId1 = p_dbid;
    PersistentFilespace_BlankPadCopyLocation(
                                    fde->locationBlankPadded1,
                                    p_path);
    fde->dbId2 = m_dbid;
    PersistentFilespace_BlankPadCopyLocation(
                                    fde->locationBlankPadded2,
                                    m_path);
    ItemPointerCopy(&fde->persistentTid, &persistentTid);

    LWLockRelease(FilespaceHashLock);
    WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	PG_RETURN_BOOL(true);
}
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

static HTAB *persistentFilespaceSharedHashTable = NULL;

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

static bool
PersistentFilespace_HashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(FilespaceDirEntryKey);
	info.entrysize = sizeof(FilespaceDirEntryData);
	info.hash = tag_hash;
	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	persistentFilespaceSharedHashTable =
						ShmemInitHash("Persistent Filespace Hash",
								   gp_max_filespaces,
								   gp_max_filespaces,
								   &info,
								   hash_flags);

	if (persistentFilespaceSharedHashTable == NULL)
		return false;

	return true;
}

static FilespaceDirEntry
PersistentFilespace_FindDirUnderLock(
	Oid			filespaceOid)
{
	bool			found;

	FilespaceDirEntry	filespaceDirEntry;

	FilespaceDirEntryKey key;

	Assert(LWLockHeldByMe(FilespaceHashLock));

	if (persistentFilespaceSharedHashTable == NULL)
		elog(PANIC, "Persistent filespace information shared-memory not setup");

	key.filespaceOid = filespaceOid;

	filespaceDirEntry =
			(FilespaceDirEntry)
					hash_search(persistentFilespaceSharedHashTable,
								(void *) &key,
								HASH_FIND,
								&found);
	if (!found)
		return NULL;

	return filespaceDirEntry;
}

static void PersistentFilespace_BlankPadCopyLocation(
	char locationBlankPadded[FilespaceLocationBlankPaddedWithNullTermLen],
	char *location)
{
	int len;
	int blankPadLen;

	if (location != NULL)
	{
		len = strlen(location);
		if (len > FilespaceLocationBlankPaddedWithNullTermLen - 1)
			elog(ERROR, "Location '%s' is too long (found %d characaters -- expected no more than %d characters)",
				 location,
			     len,
			     FilespaceLocationBlankPaddedWithNullTermLen - 1);
	}
	else
		len = 0;

	if (len > 0)
		memcpy(locationBlankPadded, location, len);

	blankPadLen = FilespaceLocationBlankPaddedWithNullTermLen - 1 - len;
	if (blankPadLen > 0)
		MemSet(&locationBlankPadded[len], ' ', blankPadLen);

	locationBlankPadded[FilespaceLocationBlankPaddedWithNullTermLen - 1] = '\0';
}

Datum
add_entry_gp_persistent_filespace_node(PG_FUNCTION_ARGS)
{
    FilespaceDirEntry fde;
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

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;
    LWLockAcquire(FilespaceHashLock, LW_SHARED);

    PersistentFilespace_HashTableInit();

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
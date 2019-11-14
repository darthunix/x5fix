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

typedef struct PersistentFilespaceSharedData
{

	PersistentFileSysObjSharedData		fileSysObjSharedData;

} PersistentFilespaceSharedData;

PersistentFilespaceSharedData	*persistentFilespaceSharedData = NULL;


typedef struct PersistentFilespaceData
{

	PersistentFileSysObjData		fileSysObjData;

} PersistentFilespaceData;

#define PersistentFilespaceData_StaticInit {PersistentFileSysObjData_StaticInit}

PersistentFilespaceData	persistentFilespaceData = PersistentFilespaceData_StaticInit;

typedef struct ReadTupleForUpdateInfo
{
	PersistentFsObjType 		fsObjType;
	PersistentFileSysObjName 	*fsObjName;
	ItemPointerData				persistentTid;
	int64						persistentSerialNum;
} ReadTupleForUpdateInfo;

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE \
	ErrorContextCallback readTupleForUpdateErrContext; \
	ReadTupleForUpdateInfo readTupleForUpdateInfo;

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_POP \
{ \
	/* Pop the error context stack */ \
	error_context_stack = readTupleForUpdateErrContext.previous; \
}

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(name, tid, serialNum) \
{ \
	readTupleForUpdateInfo.fsObjType = (name)->type; \
    readTupleForUpdateInfo.fsObjName = (name); \
	readTupleForUpdateInfo.persistentTid = *(tid); \
	readTupleForUpdateInfo.persistentSerialNum = serialNum; \
\
	/* Setup error traceback support for ereport() */ \
	readTupleForUpdateErrContext.callback = PersistentFileSysObj_ReadForUpdateErrContext; \
	readTupleForUpdateErrContext.arg = (void *) &readTupleForUpdateInfo; \
	readTupleForUpdateErrContext.previous = error_context_stack; \
	error_context_stack = &readTupleForUpdateErrContext; \
}

static void
PersistentFileSysObj_ReadForUpdateErrContext(void *arg)
{
	ReadTupleForUpdateInfo *info = (ReadTupleForUpdateInfo*) arg;

	if (info->fsObjName != NULL)
	{
		errcontext(
			 "Reading tuple TID %s for possible update to persistent file-system object %s, persistent serial number " INT64_FORMAT,
			 ItemPointerToString(&info->persistentTid),
			 PersistentFileSysObjName_TypeAndObjectName(info->fsObjName),
			 info->persistentSerialNum);
	}
	else
	{
		errcontext(
			 "Reading tuple TID %s for possible update to persistent file-system object type %s, persistent serial number " INT64_FORMAT,
			 ItemPointerToString(&info->persistentTid),
			 PersistentFileSysObjName_TypeName(info->fsObjType),
			 info->persistentSerialNum);
	}
}

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


static void fix_heap(PersistentFileSysObjName *fsObjName,
							   ItemPointer persistentTid,
							   int64 persistentSerialNum,
							   int16 pridbid,
							   int16 mirdbid,
							   void *priloc,
                               void *mirloc,
							   bool	flushToXLog)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysObjName		actualFsObjName;
	PersistentFileSysState			state;

	MirroredObjectExistenceState	mirrorExistenceState;

	TransactionId					parentXid;
	int64							serialNum;

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	if (tupleCopy != NULL)
	{
		GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&state,
							&mirrorExistenceState,
							&parentXid,
							&serialNum);

		Datum *newValues;
		bool *replaces;
		bool needs_update = false;

		newValues = (Datum*)palloc0(fileSysObjData->storeData.numAttributes *
									sizeof(Datum));
		replaces = (bool*)palloc0(fileSysObjData->storeData.numAttributes *
								  sizeof(bool));

		if (fsObjType == PersistentFsObjType_FilespaceDir)
		{
            replaces[1] = true;
            newValues[1] =  Int16GetDatum(pridbid);

			Assert(strlen((char *)priloc) == FilespaceLocationBlankPaddedWithNullTermLen - 1);
			replaces[2] = true;
			newValues[2] = CStringGetTextDatum((char *)priloc);

            replaces[3] = true;
            newValues[3] =  Int16GetDatum(mirdbid);

			Assert(strlen((char *)mirloc) == FilespaceLocationBlankPaddedWithNullTermLen - 1);
			replaces[4] = true;
			newValues[4] = CStringGetTextDatum((char *)priloc);

			needs_update = true;
		}

		/* only replace the tuple if we've changed it */
		if (needs_update)
		{
			PersistentFileSysObj_ReplaceTuple(fsObjType,
											  persistentTid,
											  tupleCopy,
											  newValues,
											  replaces,
											  flushToXLog);
		}
		else
		{
			heap_freetuple(tupleCopy);
		}

		pfree(newValues);
		pfree(replaces);
	}
	pfree(values);
}

static FilespaceDirEntry
PersistentFilespace_CreateDirUnderLock(
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
								HASH_ENTER_NULL,
								&found);

	if (filespaceDirEntry == NULL)
		elog(ERROR, "Out of shared-memory for persistent filespaces");

	filespaceDirEntry->state = 0;
	filespaceDirEntry->persistentSerialNum = 0;
	MemSet(&filespaceDirEntry->persistentTid, 0, sizeof(ItemPointerData));

	return filespaceDirEntry;
}

static bool PersistentFilespace_ScanTupleCallback(
	ItemPointer 			persistentTid,
	int64					persistentSerialNum,
	Datum					*values)
{
	Oid		filespaceOid;

	int16	dbId1;
	char	locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen];

	int16	dbId2;
	char	locationBlankPadded2[FilespaceLocationBlankPaddedWithNullTermLen];

	PersistentFileSysState	state;

	int64	createMirrorDataLossTrackingSessionNum;

	MirroredObjectExistenceState	mirrorExistenceState;

	int32					reserved;
	TransactionId			parentXid;
	int64					serialNum;

	FilespaceDirEntry filespaceDirEntry;

	GpPersistentFilespaceNode_GetValues(
									values,
									&filespaceOid,
									&dbId1,
									locationBlankPadded1,
									&dbId2,
									locationBlankPadded2,
									&state,
									&createMirrorDataLossTrackingSessionNum,
									&mirrorExistenceState,
									&reserved,
									&parentXid,
									&serialNum);

   /*
	* Normally we would acquire this lock with the WRITE_FILESPACE_HASH_LOCK
	* macro, however, this particular function can be called during startup.
	* During startup, which executes in a single threaded context, no
	* PersistentObjLock exists and we cannot assert that we're holding it.
	*/
	LWLockAcquire(FilespaceHashLock, LW_EXCLUSIVE);

	filespaceDirEntry =	PersistentFilespace_CreateDirUnderLock(filespaceOid);

	filespaceDirEntry->dbId1 = dbId1;
	memcpy(filespaceDirEntry->locationBlankPadded1, locationBlankPadded1, FilespaceLocationBlankPaddedWithNullTermLen);

	filespaceDirEntry->dbId2 = dbId2;
	memcpy(filespaceDirEntry->locationBlankPadded2, locationBlankPadded2, FilespaceLocationBlankPaddedWithNullTermLen);

	filespaceDirEntry->state = state;
	filespaceDirEntry->persistentSerialNum = serialNum;
	filespaceDirEntry->persistentTid = *persistentTid;

	LWLockRelease(FilespaceHashLock);

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
			 "PersistentFilespace_ScanTupleCallback: filespace %u, dbId1 %d, dbId2 %d, state '%s', mirror existence state '%s', TID %s, serial number " INT64_FORMAT,
			 filespaceOid,
			 dbId1,
			 dbId2,
			 PersistentFileSysObjState_Name(state),
			 MirroredObjectExistenceState_Name(mirrorExistenceState),
			 ItemPointerToString2(persistentTid),
			 persistentSerialNum);

	return true;	// Continue.
}

static Size PersistentFilespace_SharedDataSize(void)
{
	return MAXALIGN(sizeof(PersistentFilespaceSharedData));
}

void PersistentFilespace_ShmemInit(void)
{
	bool found;
	bool ok;

	/* Create the shared-memory structure. */
	persistentFilespaceSharedData =
		(PersistentFilespaceSharedData *)
						ShmemInitStruct("Persistent Filespace Data",
										PersistentFilespace_SharedDataSize(),
										&found);

	if (!found)
	{
		PersistentFileSysObj_InitShared(
						&persistentFilespaceSharedData->fileSysObjSharedData);
	}

	/* Create or find our shared-memory hash table. */
	ok = PersistentFilespace_HashTableInit();
	if (!ok)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Not enough shared memory for persistent filespace hash table")));

	PersistentFileSysObj_Init(
						&persistentFilespaceData.fileSysObjData,
						&persistentFilespaceSharedData->fileSysObjSharedData,
						PersistentFsObjType_FilespaceDir,
						PersistentFilespace_ScanTupleCallback);


	Assert(persistentFilespaceSharedData != NULL);
	Assert(persistentFilespaceSharedHashTable != NULL);
}

static void PersistentFilespace_VerifyInitScan(void)
{
	if (persistentFilespaceSharedData == NULL)
		elog(PANIC, "Persistent filespace information shared-memory not setup");

	PersistentFileSysObj_VerifyInitScan();
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
    PersistentFileSysObjName fsObjName;
    int64 persistentSerialNum;

    filespace = PG_GETARG_OID(0);
    p_dbid = PG_GETARG_INT16(1);
    p_path = TextDatumGetCString(PG_GETARG_DATUM(2));
    m_dbid = PG_GETARG_INT16(3);
    m_path = TextDatumGetCString(PG_GETARG_DATUM(4));

    WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

    PersistentFilespace_ShmemInit();
    PersistentFilespace_VerifyInitScan();
    PersistentFileSysObjName_SetFilespaceDir(&fsObjName, filespace);

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
    persistentSerialNum = fde->persistentSerialNum;

    LWLockRelease(FilespaceHashLock);

    fix_heap(&fsObjName, &persistentTid, persistentSerialNum,
            p_dbid, m_dbid, p_path, m_path, false);

    WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	PG_RETURN_BOOL(true);
}
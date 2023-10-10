/* -------------------------------------------------------------------------
 *
 * pgstat_memtrack.c
 *	  Implementation of memory tracking statistics other than backends.
 *
 * This file contains the implementation of memtrack statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_memtrack.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/memtrack.h"
#include "utils/tuplestore.h"
#include "funcapi.h"
#include "storage/pg_shmem.h"

inline static Size asMB(Size bytes);

/*
 * Report postmaster memory allocations to pgstat.
 * Note memory statistics are accumulated in my_memory.
 * This function copies them into pgstat shared memory.
 * Only the postmaster should call this function.
 */
void
pgstat_report_postmaster_memory(void)
{
	PgStatShared_Memtrack *global = &pgStatLocal.shmem->memtrack;
	Assert(pgStatLocal.shmem != NULL);
	Assert(MyProcPid == PostmasterPid);

	pgstat_begin_changecount_write(&global->postmasterChangeCount);
	global->postmasterMemory = my_memory;
	pgstat_end_changecount_write(&global->postmasterChangeCount);
}


/*
 * Report background memory allocations to pgstat.
 */
void
pgstat_report_backend_memory(void)
{
	Assert(MyBEEntry != NULL);
	PGSTAT_BEGIN_WRITE_ACTIVITY(MyBEEntry);
	MyBEEntry->st_memory = my_memory;
	PGSTAT_END_WRITE_ACTIVITY(MyBEEntry);
}


/*
 * Initialize the pgstat global memory counters,
 * Called once during server startup.
 */
void
pgstat_init_memtrack(PgStatShared_Memtrack *global)
{
	Size		shmem_bytes;
	Size		shmem_mb;

	/* Get the size of the shared memory */
	shmem_bytes = ShmemGetSize();
	shmem_mb = asMB(shmem_bytes);

	/* Initialize the global memory counters. Total memory includes shared memory */
	pg_atomic_init_u64(&global->total_memory_used, shmem_bytes);
	pg_atomic_init_u64(&global->total_dsm_used, 0);

	/*
	 * Validate the server's memory limit if one is set.
	 */
	if (max_total_memory_mb > 0)
	{
		Size		connection_mb;
		Size		required_mb;

		/* Error if backend memory limit is less than shared memory size */
		if (max_total_memory_mb < shmem_mb)
			ereport(ERROR,
					errmsg("configured max_total_memory %dMB is < shared_memory_size %zuMB",
						   max_total_memory_mb, shmem_mb),
					errhint("Disable or increase the configuration parameter \"max_total_memory\"."));

		/* Decide how much memory is needed to support the connections. */
		connection_mb = asMB(MaxConnections * (initial_allocation_allowance + allocation_allowance_refill_qty));
		required_mb = shmem_mb + connection_mb;

		/* Warning if there isn't anough memory to support the connections */
		if (max_total_memory_mb < required_mb)
			ereport(WARNING,
					errmsg("max_total_memory %dMB should be increased to at least %zuMB to support %d connections",
						   max_total_memory_mb, required_mb, MaxConnections));

		/* We prefer to use max_total_memory_mb as bytes rather than MB */
		max_total_memory_bytes = (int64) max_total_memory_mb * 1024 * 1024;
	}
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * a pointer to a snapshot of the global memtrack values.
 */
PgStat_Memtrack *
pgstat_fetch_stat_memtrack(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_MEMORYTRACK);
	return &pgStatLocal.snapshot.memtrack;
}


/*
 * Populate the memtrack globals snapshot with current values.
 */
void
pgstat_memtrack_snapshot_cb(void)
{
	PgStatShared_Memtrack *global = &pgStatLocal.shmem->memtrack;
	PgStat_Memtrack *snap = &pgStatLocal.snapshot.memtrack;

	/* Get a copy of the postmaster's memory allocations */
	pgstat_copy_changecounted_stats(&snap->postmasterMemory,
									&global->postmasterMemory,
									sizeof(snap->postmasterMemory),
									&global->postmasterChangeCount);

	/* Get a copy of the global atomic counters. */
	snap->total_dsm_used = (int64) pg_atomic_read_u64(&global->total_dsm_used);
	snap->total_memory_used = (int64) pg_atomic_read_u64(&global->total_memory_used);
}


/*
 * SQL callable function to get the memory allocation of PG backends.
 */
Datum
pg_stat_get_backend_memory(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_MEMORY_ALLOCATION_COLS	(3 + PG_ALLOC_TYPE_MAX)
	int			num_backends;
	int			curr_backend;
	int			pid = PG_ARGISNULL(0) ? -1 : PG_GETARG_INT32(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* Ensure we have a consistent snapshot of postmaster and globals */
	(void) pgstat_fetch_stat_memtrack();

	InitMaterializedSRF(fcinfo, 0);

	/* 1-based index */
	num_backends = pgstat_fetch_stat_numbackends();
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		/* for each row */
		Datum		values[PG_STAT_GET_MEMORY_ALLOCATION_COLS] = {0};
		bool		nulls[PG_STAT_GET_MEMORY_ALLOCATION_COLS] = {0};
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		pg_allocator_type type;

		/* Get the next one in the list */
		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		beentry = &local_beentry->backendStatus;

		/* If looking for specific PID, ignore all the others */
		if (pid != -1 && beentry->st_procpid != pid)
			continue;

		/* Database id */
		if (beentry->st_databaseid != InvalidOid)
			values[0] = ObjectIdGetDatum(beentry->st_databaseid);
		else
			nulls[0] = true;

		/* Process id */
		values[1] = Int32GetDatum(beentry->st_procpid);

		/* total memory allocated */
		values[2] = UInt64GetDatum(beentry->st_memory.total);

		/* Subtotals of memory */
		for (type=0; type < PG_ALLOC_TYPE_MAX; type++)
			values[3 + type] = UInt64GetDatum(beentry->st_memory.subTotal[type]);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

		/* If only a single backend was requested, and we found it, break. */
		if (pid != -1)
			break;
	}

	return (Datum) 0;
}


/*
 * SQL callable function to get the Postmaster's memory allocation.
 * Returns a single row similar to pg_stat_get_backend_memory();
 */
Datum
pg_stat_get_postmaster_memory(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PgStat_Memtrack *memtrack;
	pg_allocator_type type;

	/* A single row, similar to pg_stat_backend_memory */
	Datum values[PG_STAT_GET_MEMORY_ALLOCATION_COLS] = {0};
	bool nulls[PG_STAT_GET_MEMORY_ALLOCATION_COLS] = {0};

	/* Get access to the memtrack snapshot */
	InitMaterializedSRF(fcinfo, 0);
	memtrack = pgstat_fetch_stat_memtrack();

	/* database - postmaster is not attached to a database */
	nulls[0] = true;

	/*  postmaster pid */
	values[1] = PostmasterPid;

	/* Report total menory allocated */
	values[2] = UInt64GetDatum(memtrack->postmasterMemory.total);

	/* Report subtotals of memory allocated */
	for (type = 0; type < PG_ALLOC_TYPE_MAX; type++)
		values[3 + type] = UInt64GetDatum(memtrack->postmasterMemory.subTotal[type]);

	/* Return a single tuple */
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum) 0;
}


/*
* SQL callable function to get the global memory allocation statistics.
*/
Datum
pg_stat_get_global_memory_allocation(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_GLOBAL_MEMORY_ALLOCATION_COLS	4
	TupleDesc	tupdesc;
	int64		total_memory_used;
	Datum		values[PG_STAT_GET_GLOBAL_MEMORY_ALLOCATION_COLS] = {0};
	bool		nulls[PG_STAT_GET_GLOBAL_MEMORY_ALLOCATION_COLS] = {0};
	PgStat_Memtrack *snap;

	/* Get access to the snapshot */
	snap = pgstat_fetch_stat_memtrack();

	/* Initialise attributes information in the tuple descriptor. */
	tupdesc = CreateTemplateTupleDesc(PG_STAT_GET_GLOBAL_MEMORY_ALLOCATION_COLS);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "total_memory_allocated",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "dsm_memory_allocated",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "total_memory_available",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "static_shared_memory",
					   INT8OID, -1, 0);
	BlessTupleDesc(tupdesc);

	/* Get total_memory_used */
	total_memory_used = snap->total_memory_used;
	values[0] = Int64GetDatum(total_memory_used);

	/* Get dsm_memory_used */
	values[1] = Int64GetDatum(snap->total_dsm_used);

	/* Get total_memory_available */
	if (max_total_memory_bytes > 0)
		values[2] = Int64GetDatum(max_total_memory_bytes - total_memory_used);
	else
		nulls[2] = true;

	/* Get the static shared memory size in bytes. More precise than GUC value. */
	values[3] = ShmemGetSize();

	/* Return the single record */
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}


/*
 * Convert size in bytes to size in MB, rounding up.
 */
inline static Size
asMB(Size bytes)
{
	return ((bytes + 1024 * 1024 - 1) / (1024 * 1024));
}

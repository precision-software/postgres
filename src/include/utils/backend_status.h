/* ----------
 * backend_status.h
 *	  Definitions related to backend status reporting
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * src/include/utils/backend_status.h
 * ----------
 */
#ifndef BACKEND_STATUS_H
#define BACKEND_STATUS_H

#include <stdlib.h>
#include <unistd.h>
#include "common/int.h"
#include "datatype/timestamp.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"			/* for BackendType */
#include "storage/backendid.h"
#include "storage/proc.h"
#include "utils/backend_progress.h"
#include "elog.h"


/* ----------
 * Backend states
 * ----------
 */
typedef enum BackendState
{
	STATE_UNDEFINED,
	STATE_IDLE,
	STATE_RUNNING,
	STATE_IDLEINTRANSACTION,
	STATE_FASTPATH,
	STATE_IDLEINTRANSACTION_ABORTED,
	STATE_DISABLED
} BackendState;

/* Various types of memory allocators we are tracking. */
typedef enum pg_allocator_type
{
	PG_ALLOC_OTHER = 0,    /* Not tracked, but part of total memory */
	PG_ALLOC_ASET,         /* Allocation Set           */
	PG_ALLOC_DSM,          /* Dynamic shared memory    */
	PG_ALLOC_GENERATION,   /* Generation Context (all freed at once) */
	PG_ALLOC_SLAB,         /* Slab allocator 		 */
	PG_ALLOC_TYPE_MAX,     /* (Last, for array sizing) */
} pg_allocator_type;

/* ----------
 * Shared-memory data structures
 * ----------
 */

/*
 * PgBackendSSLStatus
 *
 * For each backend, we keep the SSL status in a separate struct, that
 * is only filled in if SSL is enabled.
 *
 * All char arrays must be null-terminated.
 */
typedef struct PgBackendSSLStatus
{
	/* Information about SSL connection */
	int			ssl_bits;
	char		ssl_version[NAMEDATALEN];
	char		ssl_cipher[NAMEDATALEN];
	char		ssl_client_dn[NAMEDATALEN];

	/*
	 * serial number is max "20 octets" per RFC 5280, so this size should be
	 * fine
	 */
	char		ssl_client_serial[NAMEDATALEN];

	char		ssl_issuer_dn[NAMEDATALEN];
} PgBackendSSLStatus;

/*
 * PgBackendGSSStatus
 *
 * For each backend, we keep the GSS status in a separate struct, that
 * is only filled in if GSS is enabled.
 *
 * All char arrays must be null-terminated.
 */
typedef struct PgBackendGSSStatus
{
	/* Information about GSSAPI connection */
	char		gss_princ[NAMEDATALEN]; /* GSSAPI Principal used to auth */
	bool		gss_auth;		/* If GSSAPI authentication was used */
	bool		gss_enc;		/* If encryption is being used */
	bool		gss_delegation; /* If credentials delegated */

} PgBackendGSSStatus;

/*
 * PgBackendMemoryStatus
 *
 * For each backend, track how much memory has been allocated.
 * Note may be possible to have negative values, say if one backend
 * creates DSM segments and another backend destroys them.
 */
typedef struct PgBackendMemoryStatus
{
	int64		allocated_bytes;
	int64       allocated_bytes_by_type[PG_ALLOC_TYPE_MAX];
} PgBackendMemoryStatus;

/* This is the corresponding initialization value */
static const PgBackendMemoryStatus INIT_BACKEND_MEMORY = {0, {0}};

/* ----------
 * PgBackendStatus
 *
 * Each live backend maintains a PgBackendStatus struct in shared memory
 * showing its current activity.  (The structs are allocated according to
 * BackendId, but that is not critical.)  Note that this is unrelated to the
 * cumulative stats system (i.e. pgstat.c et al).
 *
 * Each auxiliary process also maintains a PgBackendStatus struct in shared
 * memory.
 * ----------
 */
typedef struct PgBackendStatus
{
	/*
	 * To avoid locking overhead, we use the following protocol: a backend
	 * increments st_changecount before modifying its entry, and again after
	 * finishing a modification.  A would-be reader should note the value of
	 * st_changecount, copy the entry into private memory, then check
	 * st_changecount again.  If the value hasn't changed, and if it's even,
	 * the copy is valid; otherwise start over.  This makes updates cheap
	 * while reads are potentially expensive, but that's the tradeoff we want.
	 *
	 * The above protocol needs memory barriers to ensure that the apparent
	 * order of execution is as it desires.  Otherwise, for example, the CPU
	 * might rearrange the code so that st_changecount is incremented twice
	 * before the modification on a machine with weak memory ordering.  Hence,
	 * use the macros defined below for manipulating st_changecount, rather
	 * than touching it directly.
	 */
	int			st_changecount;

	/* The entry is valid iff st_procpid > 0, unused if st_procpid == 0 */
	int			st_procpid;

	/* Type of backends */
	BackendType st_backendType;

	/* Times when current backend, transaction, and activity started */
	TimestampTz st_proc_start_timestamp;
	TimestampTz st_xact_start_timestamp;
	TimestampTz st_activity_start_timestamp;
	TimestampTz st_state_start_timestamp;

	/* Database OID, owning user's OID, connection client address */
	Oid			st_databaseid;
	Oid			st_userid;
	SockAddr	st_clientaddr;
	char	   *st_clienthostname;	/* MUST be null-terminated */

	/* Information about SSL connection */
	bool		st_ssl;
	PgBackendSSLStatus *st_sslstatus;

	/* Information about GSSAPI connection */
	bool		st_gss;
	PgBackendGSSStatus *st_gssstatus;

	/* current state */
	BackendState st_state;

	/* application name; MUST be null-terminated */
	char	   *st_appname;

	/*
	 * Current command string; MUST be null-terminated. Note that this string
	 * possibly is truncated in the middle of a multi-byte character. As
	 * activity strings are stored more frequently than read, that allows to
	 * move the cost of correct truncation to the display side. Use
	 * pgstat_clip_activity() to truncate correctly.
	 */
	char	   *st_activity_raw;

	/*
	 * Command progress reporting.  Any command which wishes can advertise
	 * that it is running by setting st_progress_command,
	 * st_progress_command_target, and st_progress_param[].
	 * st_progress_command_target should be the OID of the relation which the
	 * command targets (we assume there's just one, as this is meant for
	 * utility commands), but the meaning of each element in the
	 * st_progress_param array is command-specific.
	 */
	ProgressCommandType st_progress_command;
	Oid			st_progress_command_target;
	int64		st_progress_param[PGSTAT_NUM_PROGRESS_PARAM];

	/* query identifier, optionally computed using post_parse_analyze_hook */
	uint64		st_query_id;

	/* Memory allocated to this backend, both total and subtotals by type. */
	PgBackendMemoryStatus st_memory;

} PgBackendStatus;


/*
 * Macros to load and store st_changecount with appropriate memory barriers.
 *
 * Use PGSTAT_BEGIN_WRITE_ACTIVITY() before, and PGSTAT_END_WRITE_ACTIVITY()
 * after, modifying the current process's PgBackendStatus data.  Note that,
 * since there is no mechanism for cleaning up st_changecount after an error,
 * THESE MACROS FORM A CRITICAL SECTION.  Any error between them will be
 * promoted to PANIC, causing a database restart to clean up shared memory!
 * Hence, keep the critical section as short and straight-line as possible.
 * Aside from being safer, that minimizes the window in which readers will
 * have to loop.
 *
 * Reader logic should follow this sketch:
 *
 *		for (;;)
 *		{
 *			int before_ct, after_ct;
 *
 *			pgstat_begin_read_activity(beentry, before_ct);
 *			... copy beentry data to local memory ...
 *			pgstat_end_read_activity(beentry, after_ct);
 *			if (pgstat_read_activity_complete(before_ct, after_ct))
 *				break;
 *			CHECK_FOR_INTERRUPTS();
 *		}
 *
 * For extra safety, we generally use volatile beentry pointers, although
 * the memory barriers should theoretically be sufficient.
 */
#define PGSTAT_BEGIN_WRITE_ACTIVITY(beentry) \
	do { \
		START_CRIT_SECTION(); \
		(beentry)->st_changecount++; \
		pg_write_barrier(); \
	} while (0)

#define PGSTAT_END_WRITE_ACTIVITY(beentry) \
	do { \
		pg_write_barrier(); \
		(beentry)->st_changecount++; \
		Assert(((beentry)->st_changecount & 1) == 0); \
		END_CRIT_SECTION(); \
	} while (0)

#define pgstat_begin_read_activity(beentry, before_changecount) \
	do { \
		(before_changecount) = (beentry)->st_changecount; \
		pg_read_barrier(); \
	} while (0)

#define pgstat_end_read_activity(beentry, after_changecount) \
	do { \
		pg_read_barrier(); \
		(after_changecount) = (beentry)->st_changecount; \
	} while (0)

#define pgstat_read_activity_complete(before_changecount, after_changecount) \
	((before_changecount) == (after_changecount) && \
	 ((before_changecount) & 1) == 0)


/* ----------
 * LocalPgBackendStatus
 *
 * When we build the backend status array, we use LocalPgBackendStatus to be
 * able to add new values to the struct when needed without adding new fields
 * to the shared memory. It contains the backend status as a first member.
 * ----------
 */
typedef struct LocalPgBackendStatus
{
	/*
	 * Local version of the backend status entry.
	 */
	PgBackendStatus backendStatus;

	/*
	 * The backend ID.  For auxiliary processes, this will be set to a value
	 * greater than MaxBackends (since auxiliary processes do not have proper
	 * backend IDs).
	 */
	BackendId	backend_id;

	/*
	 * The xid of the current transaction if available, InvalidTransactionId
	 * if not.
	 */
	TransactionId backend_xid;

	/*
	 * The xmin of the current session if available, InvalidTransactionId if
	 * not.
	 */
	TransactionId backend_xmin;

	/*
	 * Number of cached subtransactions in the current session.
	 */
	int			backend_subxact_count;

	/*
	 * The number of subtransactions in the current session which exceeded the
	 * cached subtransaction limit.
	 */
	bool		backend_subxact_overflowed;
} LocalPgBackendStatus;


/* ----------
 * GUC parameters
 * ----------
 */
extern PGDLLIMPORT bool pgstat_track_activities;
extern PGDLLIMPORT int pgstat_track_activity_query_size;
extern PGDLLIMPORT int max_total_bkend_mem;

/* ----------
 * Other global variables
 * ----------
 */
extern PGDLLIMPORT PgBackendStatus *MyBEEntry;

/* Manage memory allocation for backends. */
extern PGDLLIMPORT PgBackendMemoryStatus my_memory;
extern PGDLLIMPORT int64 allocation_allowance_refill_qty;
extern PGDLLIMPORT int64 initial_allocation_allowance;
extern PGDLLIMPORT int64 allocation_upper_bound;
extern PGDLLIMPORT int64 allocation_lower_bound;
extern PGDLLIMPORT int64 max_total_bkend_bytes;


/* ----------
 * Functions called from postmaster
 * ----------
 */
extern Size BackendStatusShmemSize(void);
extern void CreateSharedBackendStatus(void);


/* ----------
 * Functions called from backends
 * ----------
 */

/* Initialization functions */
extern void pgstat_beinit(void);
extern void pgstat_bestart(void);

extern void pgstat_clear_backend_activity_snapshot(void);

/* Activity reporting functions */
extern void pgstat_report_activity(BackendState state, const char *cmd_str);
extern void pgstat_report_query_id(uint64 query_id, bool force);
extern void pgstat_report_tempfile(size_t filesize);
extern void pgstat_report_appname(const char *appname);
extern void pgstat_report_xact_timestamp(TimestampTz tstamp);
extern const char *pgstat_get_backend_current_activity(int pid, bool checkUser);
extern const char *pgstat_get_crashed_backend_activity(int pid, char *buffer,
													   int buflen);
extern uint64 pgstat_get_my_query_id(void);

/* ----------
 * Support functions for the SQL-callable functions to
 * generate the pgstat* views.
 * ----------
 */
extern int	pgstat_fetch_stat_numbackends(void);
extern PgBackendStatus *pgstat_fetch_stat_beentry(BackendId beid);
extern LocalPgBackendStatus *pgstat_fetch_stat_local_beentry(int beid);
extern char *pgstat_clip_activity(const char *raw_activity);


/* ----------
 * Backend memory accounting functions.
 *   Track how much memory each backend is using
 *   and place a cluster-wide limit on the total amount of backend memory.
 *
 * The main functions are:
 *     reserve_backend_memory()
 *     release_backend_memory()
 *     init_backend_memory()
 *     exit_backend_memory()
 *
 * The functions implement a "fast path" and a "slow path".
 * The fast path is used for most allocations, and it only references
 * variables local to the backend. The slow path is used when the
 * fast path exceeds its bounds. It updates global shared memory
 * variables, reports the current state to pgstat and checks
 * the cluster-wide limits on backend memory.
 *
 * The following counters represent the "TRUTH" of this backend's memory allocations.
 *   my_memory.allocated_bytes:               total amount of memory allocated by this backend.
 *   my_memory.allocated_bytes_by_type[type]: subtotals by allocator type.

 * These counters are the values reported to pgstat. They are a snapshot of the above values.
 *   proc->st_memory.allocated_bytes:               last total reported to pgstat
 *   proc->st_memory.allocated_bytes_by_type[type]: last reported subtotals reported to pgstat
 *
 * When to update pgstat and check memory limits.
 *   allocation_upper_bound:          update when my_memory.allocated_bytes exceeds this
 *   allocation_lower_bound:          update when my_memory.allocated_bytes drops below this
 *   allocation_allowance_refill_qty  amount of memory to allocate or release before updating again.
 *
 * Bounds checking on backend memory. Limits how much memory the cluster can use.
 *   ProcGlobal->total_bkend_mem_bytes:       total amount of memory reserved by all backends, including shared memory
 *   ProcGlobal->global_dsm_allocated_bytes:  total amount of shared memory allocated by all backends.
 *   max_total_bkend_bytes:                   maximum amount of memory allowed to be reserved by all backends.
 *   initial_allocation_allowance:            each backend consumes this much memory simply by existing.
 * ----------
 */

/* These are the main entry points for backend memory accounting */
extern void init_backend_memory(void);
static inline bool reserve_backend_memory(int64 size, pg_allocator_type type);
static inline void release_backend_memory(int64 size, pg_allocator_type type);
extern void exit_backend_memory(void);

/* Helper functions for backend memory accounting */
static inline bool update_local_allocation(int64 size, pg_allocator_type type);
extern bool update_global_allocation(int64 size, pg_allocator_type type);

/* ----------
 * reserve_backend_memory() -
 *  Called to report a desired increase in memory for this backend.
 *  true if successful.
 */
static inline bool
reserve_backend_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* quick optimization */
	if (size == 0)
		 return true;

	/* CASE: the new allocation is within bounds. Take the fast path. */
	else if (my_memory.allocated_bytes + size <= allocation_upper_bound)
		return update_local_allocation(size, type);

	/* CASE: out of bounds. Update pgstat and check memory limits */
	else
		return update_global_allocation(size, type);
}

/* ----------
 * unreserve_memory() -
 *  Called to report decrease in memory allocated for this backend.
 *  Note we should have already called "reserve_backend_memory"
 *  so we should never end up with a negative total allocation.
 */
static inline void
release_backend_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* quick optimization */
	if (size == 0)
		 return;

	/* CASE: In bounds, take the fast path */
	else if (my_memory.allocated_bytes - size >= allocation_lower_bound)
		update_local_allocation(-size, type);

	/* CASE: Out of bounds. Update pgstat and memory totals */
	else
		update_global_allocation(-size, type);
}


/*
* Fast path for reserving and releasing memory.
* This version is used for most allocations, and it
* is stripped down to the bare minimum to reduce impact
* on performance. It only updates local variables.
*/
static inline bool
update_local_allocation(int64 size, pg_allocator_type type)
{
	/* Update our local memory counters. */
	my_memory.allocated_bytes += size;
	my_memory.allocated_bytes_by_type[type] += size;

	return true;
}


/*--------------------------------------------
 * Convenience functions based on malloc/free
 *------------------------------------------*/

/*
 * Reserve memory from malloc if we can.
 */
static inline void *
malloc_backend(int64 size, pg_allocator_type type)
{
    void *ptr;

    /* reserve the memory if able to */
    if (!reserve_backend_memory(size, type))
	    return NULL;

	/* Allocate the memory, returning the reservation if failed */
	ptr = malloc(size);
	if (ptr == NULL)
		release_backend_memory(size, type);

	return ptr;
}

/*
 * Free memory which was allocated with malloc_reserved.
 * In the future, we could embed size and type in the
 * memory itself, but for now we just use the arguments.
 */
static inline void
free_backend(void *ptr, int64 size, pg_allocator_type type)
{
	release_backend_memory(size, type);
	free(ptr);
}


/*
 * Realloc reserved memory.
 */
static inline void *
realloc_backend(void *block, int64 new_size, int64 old_size, pg_allocator_type type)
{
	void *ptr;
	bool success;

	/* Update the reservation to the new size */
	release_backend_memory(old_size, type);
	success = reserve_backend_memory(new_size, type);

	/* If unable, free the old memory and return NULL */
	if (!success)
	{
		free(block);
		return NULL;
	}

	/* Now, actually resize the memory */
	ptr = realloc(block, new_size);

	/*
	 * If unable to resize, release the allocation.
	 * The actual memory has already been freed.
	 */
	if (ptr == NULL)
		release_backend_memory(new_size, type);

	return ptr;
}


/*
 * Helper function to add to an atomic sum, as long as the result is within bounds.
 * TODO: consider moving to atomics.h and clarify signed vs unsigned.
 */
static inline bool
atomic_add_within_bounds_i64(pg_atomic_uint64 *ptr, int64 add, int64 lower_bound, int64 upper_bound)
{
	int64 oldval;
	int64 newval;

	for (;;)
	{
		oldval = (int64)pg_atomic_read_u64(ptr);
		newval = oldval + add;  /* TODO: check overflow */

		/* check if we are out of bounds */
		if (newval < lower_bound || newval > upper_bound)
			return false;

		if (pg_atomic_compare_exchange_u64(ptr, (uint64 *)&oldval, newval))
		    return true;
	}
}

#endif							/* BACKEND_STATUS_H */

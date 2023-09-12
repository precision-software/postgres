#ifndef MEMTRACK_H
#define MEMTRACK_H

/* ----------
 * Memory accounting functions.
 *   Track how much memory each process is using and place an
 *   overall limit on how much memory a database server can allocate.
 *
 * The main functions are:
 *     init_tracked_memory()
 *     reserve_tracked_memory()
 *     release_tracked_memory()
 *     exit_tracked_memory()
 *
 * All private variables are properly initialized at startup, so init_tracked_memory()
 * only needs to be called after a fork() system call.
 *
 * The reserve/release functions implement both a "fast path" and a "slow path".
 * The fast path is used for most allocations, and it only references
 * private (hon-shared) variables. The slow path is invoked periodically; it updates
 * shared memory and checks for limits on total server memory.
 *
 * The following private variables represent the "TRUTH" of how much memory the process allocated.
 *   my_memory.allocated_bytes:               total amount of memory allocated by this process
 *   my_memory.allocated_bytes_by_type[type]: subtotals by allocator type.
 *
 * The private values are periodically reported to pgstat.
 * The following variables hold the last reported values
 *    reported_memory.allocated_bytes
 *    reported_memory.allocated_bytes_by_type[type]:
 *
 * The "slow path" is invoked when my_memory.allocate_bytes exceeds these bounds.
 * Once invoked, it updates the reported values and sets new bounds.
 *   allocation_upper_bound:          update when my_memory.allocated_bytes exceeds this
 *   allocation_lower_bound:          update when my_memory.allocated_bytes drops below this
 *   allocation_allowance_refill_qty  amount of memory to allocate or release before updating again.
 *
 * These counters are the values seen  by pgstat. They are a copy of reported_memory.
 *   proc->st_memory.allocated_bytes:               last total reported to pgstat
 *   proc->st_memory.allocated_bytes_by_type[type]: last reported subtotals reported to pgstat
 *
 * Limits on total server memory. If max_total_memory_byes is zero, there is no limit.
 *   ProcGlobal->total_memory_bytes:       total amount of memory reserved by the server, including shared memory
 *   max_total_memory_bytes:               maximum memory the server can allocate
 *
 * And finally,
 *   initial_allocation_allowance:            each process consumes this much memory simply by existing.
 *   ProcGlobal->global_dsm_allocated_bytes:  total amount of shared memory allocated by the server
 * ----------
 */

#include <unistd.h>
#include "postgres.h"
#include "common/int.h"
#include "port/atomics.h"

/* Various types of memory allocators we are tracking. */
typedef enum pg_allocator_type
{
	PG_ALLOC_INIT = 0,			/* Not tracked, but part of total memory */
	PG_ALLOC_ASET,				/* Allocation Set           */
	PG_ALLOC_DSM,				/* Dynamic shared memory    */
	PG_ALLOC_GENERATION,		/* Generation Context (all freed at once) */
	PG_ALLOC_SLAB,				/* Slab allocator 		 */
	PG_ALLOC_TYPE_MAX,			/* (Last, for array sizing) */
}			pg_allocator_type;


/*
 * PgBackendMemoryStatus
 *
 * For each backend, track how much memory has been allocated.
 * Note it may be possible to have negative values, say if one process
 * creates DSM segments and another process destroys them.
 */
typedef struct PgBackendMemoryStatus
{
	int64		allocated_bytes;
	int64		allocated_bytes_by_type[PG_ALLOC_TYPE_MAX];
}			PgBackendMemoryStatus;


/* This value is a candidate to be a GUC variable.  We chose 1MB arbitrarily. */
static const int64 allocation_allowance_refill_qty = 1024 * 1024;	/* 1MB */

/* Compile time initialization constants */
#define initial_allocation_allowance (1024 * 1024)
#define INIT_BACKEND_MEMORY (PgBackendMemoryStatus) \
	{initial_allocation_allowance, {initial_allocation_allowance}}
#define NO_BACKEND_MEMORY (PgBackendMemoryStatus) \
	{0, {0}}

/* Manage memory allocation for backends. */
extern PGDLLIMPORT PgBackendMemoryStatus my_memory;
extern PGDLLIMPORT PgBackendMemoryStatus reported_memory;
extern PGDLLIMPORT int64 allocation_upper_bound;
extern PGDLLIMPORT int64 allocation_lower_bound;

extern PGDLLIMPORT int64 max_total_memory_bytes;
extern PGDLLIMPORT int32 max_total_memory_mb;

/* These are the main entry points for memory tracking */
extern void init_tracked_memory(void);
static inline bool reserve_tracked_memory(int64 size, pg_allocator_type type);
static inline void release_tracked_memory(int64 size, pg_allocator_type type);
extern void exit_tracked_memory(void);

/* Helper functions for memory tracking */
static inline bool update_local_allocation(int64 size, pg_allocator_type type);
extern bool update_global_allocation(int64 size, pg_allocator_type type);

/* ----------
 *  Report a desired increase in memory for this process.
 *  true if successful.
 */
static inline bool
reserve_tracked_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* CASE: no change in reserved memory. Do nothing. */
	if (size == 0)
		return true;

	/* CASE: the new allocation is within bounds. Take the fast path. */
	if (my_memory.allocated_bytes + size <= allocation_upper_bound)
		return update_local_allocation(size, type);

	/* CASE: out of bounds. Update pgstat and check memory limits */
	return update_global_allocation(size, type);
}

/* ----------
 *  Report a decrease in memory allocated for this process.
 *  Note we should have already called "reserve_tracked_memory"
 *  so we should never end up with a negative total allocation.
 */
static inline void
release_tracked_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* CASE: no change in reserved memory. Do nothing. */
	if (size == 0)
		return;

	/* CASE: In bounds, take the fast path */
	if (my_memory.allocated_bytes - size >= allocation_lower_bound)
		update_local_allocation(-size, type);

	/* CASE: Out of bounds. Update pgstat and memory totals */
	else
		update_global_allocation(-size, type);
}


/*
* Fast path for reserving and releasing memory.
* This version is used for most allocations, and it
* is stripped down to the bare minimum to reduce impact
* on performance. It only updates private (non-shared) variables.
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
 * Allocate tracked memory using malloc.
 */
static inline void *
malloc_tracked(int64 size, pg_allocator_type type)
{
	void	   *ptr;

	/* reserve the memory if able to */
	if (!reserve_tracked_memory(size, type))
		return NULL;

	/* Allocate the memory, releasing the reservation if failed */
	ptr = malloc(size);
	if (ptr == NULL)
		release_tracked_memory(size, type);

	return ptr;
}

/*
 * Free memory which was allocated with malloc_tracked.
 * Note: most mallocs have a non-portable method to
 * get the size of a block of memory. Dropping the "size" parameter
 * would greatly simplify the calling code.
 */
static inline void
free_tracked(void *ptr, int64 size, pg_allocator_type type)
{
	release_tracked_memory(size, type);
	free(ptr);
}


/*
 * Realloc tracked memory.
 */
static inline void *
realloc_tracked(void *block, int64 new_size, int64 old_size, pg_allocator_type type)
{
	void	   *ptr;
	bool		success;

	/* Update the reservation to the new size */
	release_tracked_memory(old_size, type);
	success = reserve_tracked_memory(new_size, type);

	/* If unable, free the old memory and return NULL */
	if (!success)
	{
		free(block);
		return NULL;
	}

	/* Now, actually resize the memory */
	ptr = realloc(block, new_size);

	/*
	 * If unable to resize, release the allocation. The actual memory has
	 * already been freed.
	 */
	if (ptr == NULL)
		release_tracked_memory(new_size, type);

	return ptr;
}

#endif							/* //POSTGRES_IDE_MEMTRACK_H */

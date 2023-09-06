
#include <unistd.h>

#include "postgres.h"
#include "miscadmin.h"
#include "utils/backend_status.h"
#include "utils/memtrack.h"
#include "storage/proc.h"

/* Forward references */
static inline bool atomic_add_with_limit(pg_atomic_uint64 *sum, uint64 add, uint64 limit);

/*
 * Max backend memory allocation allowed (MB). 0 = disabled.
 * Max backend bytes is the same but in bytes.
 * These default to "0", meaning don't check bounds for total memory.
 */
int			max_total_bkend_mem = 0;
int64       max_total_bkend_bytes = 0;

/*
 * Private variables for tracking memory use.
 * These values are preset so memory tracking is active on startup.
 * After a fork(), they must be reset using 'init_backend_memory()'.
 */
PgBackendMemoryStatus  my_memory = INIT_BACKEND_MEMORY;
PgBackendMemoryStatus  reported_memory = NO_BACKEND_MEMORY;
int64      allocation_lower_bound = 0;
int64      allocation_upper_bound = 0;

/* ---------
 * init_backend_memory() -
 *
 * Called immediately after a fork().
 * Resets private memory counters to their initial startup values
 */
void
init_backend_memory(void)
{
	/* Start with nothing allocated. */
	my_memory = INIT_BACKEND_MEMORY;
	reported_memory = NO_BACKEND_MEMORY;

	/* Force the next allocation to do global bounds checking. */
	allocation_lower_bound = 0;
	allocation_upper_bound = 0;
}

/*
 * Clean up memory counters as backend is exiting.
 *
 * DSM memory is not automatically returned, so it persists in the counters.
 * All other memory will disappear, so those counters are set to zero.
 *
 * Ideally, this function would be called last, but in practice there are some
 * late memory releases that happen after it is called.
 */
void
exit_backend_memory(void)
{
	/*
	 * Release non-dsm memory.
	 * We don't release dsm shared memory since it survives process exit.
	 */
	for (int type = 0; type < PG_ALLOC_TYPE_MAX; type++)
		if (type != PG_ALLOC_DSM)
			release_backend_memory(my_memory.allocated_bytes_by_type[type], type);

	/* Force the final values to be posted to shmem */
	update_global_allocation(0, PG_ALLOC_OTHER);

	/* If we get a late request, send it to the long path. */
	allocation_lower_bound = 0;
	allocation_upper_bound = 0;
}


/*
 * Update backend memory allocation for a new request.
 *
 * There are two versions of this function. This one, which updates
 * global values in shared memory, and an optimized update_local_allocation()
 * which only updates private values.
 *
 * This version is the "slow path". We invoke it periodically to update
 * global values and pgstat statistics.
 *
 * Note the use of pg_atomic_fetch_add_u64() to update counters
 * with a signed value assumes we are on a 2's complement machine.
 * Otherwise, we would need to introduce atomic s64 operators.
 */
bool update_global_allocation(int64 size, pg_allocator_type type)
{
	int64 delta;
	int64 dsm_delta;

	/* If we are still initializing, only update the private counters */
	if (ProcGlobal == NULL || MyProcPid == 0)
		return update_local_allocation(size, type);

	/* Quick check to verify totals are not negative. Should never happen. */
	Assert((int64)atomic_load_u64(&ProcGlobal->total_bkend_mem_bytes) >= 0);
	Assert((int64)atomic_load_u64(&ProcGlobal->global_dsm_allocation) >= 0);

	/* Calculate total bytes allocated or freed since last report */
	delta = my_memory.allocated_bytes + size - reported_memory.allocated_bytes;

	/* If reporting new memory allocated, and we are limited by max_total_bkend ... */
	if (delta > 0 && max_total_bkend_bytes > 0 && MyAuxProcType == NotAnAuxProcess && MyProcPid != PostmasterPid)
	{
		/* Update the global total memory counter subject to the upper limit. */
		if (!atomic_add_with_limit(&ProcGlobal->total_bkend_mem_bytes, delta, max_total_bkend_bytes))
			return false;
	}

	/*
	 * Otherwise, update the global counter with no limit checking.
	 */
	else
		pg_atomic_fetch_add_u64(&ProcGlobal->total_bkend_mem_bytes, delta);

	/* Update the private memory counters. This must happen after bounds checking */
	update_local_allocation(size, type);

	/* Update the global dsm memory counter to reflect changes since our last report */
	dsm_delta = my_memory.allocated_bytes_by_type[PG_ALLOC_DSM] -
		reported_memory.allocated_bytes_by_type[PG_ALLOC_DSM];
	pg_atomic_fetch_add_u64(&ProcGlobal->global_dsm_allocation, dsm_delta);

	/* Update pgstat statistics if we are initialized as a backend process. */
	if (MyBEEntry != NULL)
	{
		PGSTAT_BEGIN_WRITE_ACTIVITY(MyBEEntry);
		MyBEEntry->st_memory = my_memory;
		PGSTAT_END_WRITE_ACTIVITY(MyBEEntry);
	}

	/* Remember the values we just reported to pgstat */
	reported_memory = my_memory;

	/* Update bounds so they bracket our new allocation size. */
	allocation_upper_bound = my_memory.allocated_bytes + allocation_allowance_refill_qty;
	allocation_lower_bound = my_memory.allocated_bytes - allocation_allowance_refill_qty;

	return true;
}


/*
 * Add to an atomic sum as long as it doesn't exceed the limit.
 * We are assuming reasonable values which are not going to overflow,
 * but we include an assertion just in case.
 */
static inline bool
atomic_add_with_limit(pg_atomic_uint64 *sum, uint64 add, uint64 limit)
{
	uint64 old_sum;
	uint64 new_sum;

	/* CAS loop until successful or until new sum would be out of bounds */
	old_sum = pg_atomic_read_u64(sum);
	do
	{
		new_sum = old_sum + add;
		if (new_sum > limit || new_sum < old_sum) /* Includes overflow test */
			return false;

	} while (!pg_atomic_compare_exchange_u64(sum, &old_sum, new_sum));

	return true;
}

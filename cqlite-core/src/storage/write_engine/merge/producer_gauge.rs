//! Producer-thread gauge for the k-way merge (issue #2316).
//!
//! Backs the [`crate::observability::catalog::MERGE_PRODUCER_THREADS`] gauge with
//! a process-global live count of merge producer OS threads. The count is
//! incremented at producer spawn ([`spawned`], called from
//! `SSTableRowIteratorAdapter::open`) and decremented when the producer thread
//! exits (via the RAII [`ProducerThreadGuard`], created as the first act of
//! `producer_thread`). The gauge is re-recorded on each change, so it RISES to the
//! `O(M)` producer count during a merge and RETURNS to baseline once the producers
//! are joined/dropped — making the previously-invisible per-merge thread cost
//! observable on a loaded node.

use std::sync::atomic::{AtomicI64, Ordering};

use crate::observability;

static LIVE: AtomicI64 = AtomicI64::new(0);

fn record(live: i64) {
    observability::record_gauge(observability::catalog::MERGE_PRODUCER_THREADS, live, &[]);
}

/// Account a just-spawned producer thread on the gauge. Balanced by exactly one
/// [`ProducerThreadGuard`] drop when that thread exits.
pub(super) fn spawned() {
    record(LIVE.fetch_add(1, Ordering::SeqCst) + 1);
}

/// RAII guard that decrements the live producer-thread count (and re-records the
/// gauge) when a producer thread exits — even on panic.
pub(super) struct ProducerThreadGuard;

impl Drop for ProducerThreadGuard {
    fn drop(&mut self) {
        record(LIVE.fetch_sub(1, Ordering::SeqCst) - 1);
    }
}

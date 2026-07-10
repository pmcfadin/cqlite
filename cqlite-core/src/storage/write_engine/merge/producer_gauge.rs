//! Producer-thread gauge for the k-way merge (issue #2316).
//!
//! Backs the [`crate::observability::catalog::MERGE_PRODUCER_THREADS`] gauge with
//! a process-global live count of merge producer OS threads. The count is
//! incremented BEFORE the OS thread is spawned ([`spawned`], called from
//! `SSTableRowIteratorAdapter::open`) so the increment happens-before any possible
//! decrement (correct-by-construction: the decrementing `ProducerThreadGuard` is
//! created first thing inside the child thread, so a fast-exiting producer can
//! never race its own decrement against the increment). If the spawn itself then
//! fails ([`rollback`], called from the same `open` on a `Builder::spawn` `Err`),
//! the increment is rolled back so it never leaks for a thread that never
//! started. The guard decrements when the producer thread exits (even on panic).
//! The gauge is re-recorded on each change, so it RISES to the `O(M)` producer
//! count during a merge and RETURNS to baseline once the producers exit — making
//! the previously-invisible per-merge thread cost observable on a loaded node.

use std::sync::atomic::{AtomicI64, Ordering};

use crate::observability;

static LIVE: AtomicI64 = AtomicI64::new(0);

fn record(live: i64) {
    observability::record_gauge(observability::catalog::MERGE_PRODUCER_THREADS, live, &[]);
}

fn decrement() {
    record(LIVE.fetch_sub(1, Ordering::SeqCst) - 1);
}

/// Account a just-about-to-spawn producer thread on the gauge, BEFORE
/// `std::thread::Builder::spawn` is called. Balanced by exactly one of:
/// [`ProducerThreadGuard`]'s drop (the thread started and later exited), or
/// [`rollback`] (the thread never actually started).
pub(super) fn spawned() {
    record(LIVE.fetch_add(1, Ordering::SeqCst) + 1);
}

/// Undo a [`spawned`] increment for a producer that failed to actually start
/// (`Builder::spawn` returned `Err`) — the increment must not leak for a thread
/// that never ran (and thus never gets a `ProducerThreadGuard` decrement).
pub(super) fn rollback() {
    decrement();
}

/// RAII guard that decrements the live producer-thread count (and re-records the
/// gauge) when a producer thread exits — even on panic. Created as the FIRST act
/// inside the spawned producer thread, so it is guaranteed to run for every
/// thread that actually started (pairing exactly one decrement per [`spawned`]
/// increment that was not rolled back).
pub(super) struct ProducerThreadGuard;

impl Drop for ProducerThreadGuard {
    fn drop(&mut self) {
        decrement();
    }
}

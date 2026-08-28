//! Always-on READ-PATH ARM probes (issue #3058).
//!
//! The Flight `do_get` row route can be served by two structurally different
//! arms: the k-way compaction merge (`KWayMerger` + the compaction reconciler)
//! and — when exactly ONE post-prune source is involved — the single-generation
//! query scan. "Which arm ran" is not observable from the RESULT (both arms are
//! required to return the same rows), so a test that wants to pin the routing
//! must observe an explicit MARKER. These counters are that marker.
//!
//! # Why an explicit marker, not a timing/throughput inference
//!
//! Spec `flight-single-sstable-bypass` (issue AC #1) requires the path-taken
//! assertion to FAIL when the merge path is entered, and forbids inferring the
//! arm from elapsed time, throughput, or CPU share (a timing assertion is both
//! host-dependent and, per issue #2877, capable of passing while the work it
//! claims to have removed is still being done). A counter incremented at the
//! merge's own construction / reconcile / cell-metadata-allocation sites is a
//! direct observation of the work, so `== 0` is a proof, not a correlation.
//!
//! # Always-on, like `SCAN_FOR_KEY_CALLS`
//!
//! These are plain `Relaxed` adds on the MERGE arm only (never on the scan
//! arm), following the always-on `SCAN_FOR_KEY_CALLS` precedent
//! (`data_access/model.rs`) rather than the `cfg`-gated `read_work_counters`
//! pattern: the consumers are integration tests in a DIFFERENT crate
//! (`cqlite-flight/tests/`), which cannot see `cfg(test)` counters and must not
//! be forced to rebuild `cqlite-core` under a non-default feature. The merge arm
//! costs microseconds per row, so one uncontended relaxed increment per merged
//! row is unmeasurable there; the fast arm pays literally nothing because it
//! never reaches these sites.
//!
//! # Reading them
//!
//! They are PROCESS-GLOBAL. A test observes a DELTA around the operation under
//! test ([`ReadPathProbe::snapshot`] → run → [`ReadPathProbe::delta_since`]) and
//! must serialize against any sibling test in the same binary that also merges
//! (one file = one process is the strongest form; a mutex is the minimum).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

static MERGERS_BUILT: AtomicU64 = AtomicU64::new(0);
static RECONCILE_ENTRIES: AtomicU64 = AtomicU64::new(0);
static CELL_METADATA_MAPS: AtomicU64 = AtomicU64::new(0);

/// Record one k-way merger construction over an already-open reader set
/// (`KWayMerger::new_from_readers` — the Flight warm row route's merge arm).
#[inline]
pub fn record_merger_built() {
    MERGERS_BUILT.fetch_add(1, Ordering::Relaxed);
}

/// Record one entry into the compaction reconciler
/// (`KWayMerger::reconcile_cluster_with_overlap_counted`).
#[inline]
pub fn record_reconcile_entry() {
    RECONCILE_ENTRIES.fetch_add(1, Ordering::Relaxed);
}

/// Record one per-row `HashMap<String, CellWriteMetadata>` allocation in the row
/// decoder (i.e. one row decoded with `want_cell_metadata == true`).
#[inline]
pub fn record_cell_metadata_map() {
    CELL_METADATA_MAPS.fetch_add(1, Ordering::Relaxed);
}

/// A point-in-time reading of the three arm counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadPathProbe {
    /// k-way mergers constructed over a warm reader set.
    pub mergers_built: u64,
    /// Entries into the compaction reconciler.
    pub reconcile_entries: u64,
    /// Per-row cell-write-metadata maps allocated by the row decoder.
    pub cell_metadata_maps: u64,
}

impl ReadPathProbe {
    /// Read the current process-global counter values.
    pub fn snapshot() -> Self {
        Self {
            mergers_built: MERGERS_BUILT.load(Ordering::Relaxed),
            reconcile_entries: RECONCILE_ENTRIES.load(Ordering::Relaxed),
            cell_metadata_maps: CELL_METADATA_MAPS.load(Ordering::Relaxed),
        }
    }

    /// The work recorded between `earlier` and `self`, saturating (a counter can
    /// only climb, so a saturating subtraction is exact here and can never
    /// underflow-panic on a racing reader).
    pub fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            mergers_built: self.mergers_built.saturating_sub(earlier.mergers_built),
            reconcile_entries: self
                .reconcile_entries
                .saturating_sub(earlier.reconcile_entries),
            cell_metadata_maps: self
                .cell_metadata_maps
                .saturating_sub(earlier.cell_metadata_maps),
        }
    }

    /// Whether ANY merge-arm work was recorded in this (delta) reading.
    pub fn any_merge_work(&self) -> bool {
        self.mergers_built > 0 || self.reconcile_entries > 0 || self.cell_metadata_maps > 0
    }
}

// ---------------------------------------------------------------------------
// Query-row producer completion (issue #3384)
// ---------------------------------------------------------------------------

/// Producers that have finished decoding, published with RELEASE ordering.
///
/// Deliberately NOT a field of [`ReadPathProbe`]: every field there is loaded
/// `Relaxed`, which is right for counters read after the work is known to be over,
/// and wrong for this one — its whole job is to TELL you the work is over.
static QUERY_ROW_PRODUCERS_FINISHED: AtomicU64 = AtomicU64::new(0);

/// Publish that one `QueryRowStream` producer has finished decoding.
///
/// Call BEFORE the terminal message is sent, never after (roborev, issue #3384):
/// a consumer holding the terminal message must be able to conclude that this
/// producer can no longer publish, or a producer from a PRIOR test case can
/// increment into a LATER case's freshly-reset counter and the later case will
/// observe a completion that was never its own.
pub fn mark_query_row_producer_finished() {
    QUERY_ROW_PRODUCERS_FINISHED.fetch_add(1, Ordering::Release);
}

/// `QueryRowStream` producers that have finished decoding since the last
/// [`reset_query_row_producers_finished`] (issue #3384).
///
/// The CAUSAL completion signal for an abandoned walk. A test that instead waits
/// for a work counter to "stop growing" is sampling another thread's progress: a
/// producer merely descheduled, or paused between two increments, is
/// indistinguishable from one that has stopped, so the walk can resume and drain
/// the table right after the assertion passed.
///
/// # What it proves, and what it does NOT
///
/// It proves the OUTER query-row producer stopped. It does NOT prove the INNER
/// batched scan task has (roborev, issue #3384): `drive_full_scan_rows` drops
/// `BatchedScanStream` on its way out and that task is never joined. That trail is
/// BOUNDED — the inner loop consults its consumer only when a batch fills, so after
/// the receiver is dropped it decodes at most one more emit batch before its `send`
/// fails and it returns — so it cannot run away, but a reader wanting a FINAL work
/// count should let the trail land rather than read the instant this signal moves.
/// Joining the inner task on shutdown is tracked on #3428.
///
/// The `Acquire` load is load-bearing, not decoration. With `Relaxed` on both
/// sides, observing a non-zero count would establish NO happens-before edge with
/// the producer's earlier work-counter increments, so a reader could see
/// "finished" and then read a STALE row count — precisely the guarantee this
/// signal exists to give (roborev, issue #3384).
pub fn query_row_producers_finished() -> u64 {
    QUERY_ROW_PRODUCERS_FINISHED.load(Ordering::Acquire)
}

/// Blocking scan tasks currently running (issue #3384). A GAUGE, not a counter.
static BLOCKING_SCAN_TASKS_INFLIGHT: AtomicI64 = AtomicI64::new(0);

/// RAII marker for one blocking scan task: increments on construction, decrements on
/// drop (issue #3384).
///
/// The stitching path's parse/feed halves run under `spawn_blocking`, and blocking
/// tasks are NOT cancellable — dropping their `JoinHandle` DETACHES them. So neither
/// the outer query-row producer's completion nor the scan future's drop guard proves
/// decoding has stopped: both can fire while a detached blocking half is still
/// draining. This gauge is the THIRD and last of the completion signals — the other two
/// are `query_row_producers_finished` (outer thread) and the scan future's own drop
/// guard, and neither covers detached blocking work. A gauge rather than a completion counter
/// because the number of blocking halves is an implementation detail — a reader waits
/// for it to reach ZERO instead of knowing how many to expect.
///
/// Decrement is a `Drop` impl so an unwinding task still clears its slot; a leaked
/// increment would hang every future reader.
/// The private field is load-bearing (roborev, issue #3384): as a UNIT struct this
/// was constructible by name, so a caller could make one WITHOUT the increment and
/// then decrement the gauge below zero on drop — silently breaking every completion
/// check that reads it. `pub(crate)` for the same reason, narrowed further: nothing
/// outside this crate needs to register a blocking task, and the observers
/// ([`blocking_scan_tasks_inflight`]) stay public.
pub(crate) struct BlockingScanTaskGuard(());

impl BlockingScanTaskGuard {
    /// Register one running blocking scan task. The ONLY way to make one.
    pub(crate) fn new() -> Self {
        // `AcqRel` for the same chaining reason as the decrement below.
        BLOCKING_SCAN_TASKS_INFLIGHT.fetch_add(1, Ordering::AcqRel);
        Self(())
    }
}

impl Drop for BlockingScanTaskGuard {
    fn drop(&mut self) {
        // `AcqRel`, not `Release` (roborev, issue #3384). A reader's `Acquire` load of
        // ZERO synchronizes-with the FINAL decrement only. With plain `Release` stores
        // that leaves the EARLIER tasks' work unordered: if the feed half decrements
        // last, the reader's subsequent (Relaxed) work-counter load is not guaranteed to
        // observe the parse half's increments — so the "final count" would not be final,
        // which is the entire property this gauge exists to provide. `AcqRel` makes each
        // decrement ACQUIRE the ones before it, chaining happens-before through every
        // task so the last decrement publishes all of their work.
        BLOCKING_SCAN_TASKS_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Blocking scan tasks still running (issue #3384). ZERO means every detached
/// blocking half has stopped, so `stream_walk_partitions_parsed` is final.
pub fn blocking_scan_tasks_inflight() -> i64 {
    BLOCKING_SCAN_TASKS_INFLIGHT.load(Ordering::Acquire)
}

/// Times a batched scan took the STITCHING branch (issue #3384).
static BATCHED_SCAN_STITCHING_PATHS: AtomicU64 = AtomicU64::new(0);

/// Record that a batched scan routed to `run_scan_stream_windowed` — the
/// STITCHING branch (issue #3384).
///
/// Exists so a test can AFFIRMATIVELY measure which branch its fixture took, rather
/// than infer it. The completion signal below covers only the non-stitching loop, and
/// the obvious proxies for "am I on that loop" are all wrong: absence of
/// `CompressionInfo.db` does not imply it (roborev), and neither does the fixture being
/// uncompressed in the colloquial sense — `requires_chunk_stitching()` is
/// `data_format() == V5CompressedLegacy && is_nb_format()`, and `V5_0Uncompressed` maps
/// to the FORMER but not the LATTER. Two proxies, two different wrong answers; the
/// branch itself is the only thing worth asserting on.
pub fn mark_batched_scan_stitching_path() {
    BATCHED_SCAN_STITCHING_PATHS.fetch_add(1, Ordering::Release);
}

/// Batched scans that took the stitching branch since
/// [`reset_batched_scan_stitching_paths`] (issue #3384).
pub fn batched_scan_stitching_paths() -> u64 {
    BATCHED_SCAN_STITCHING_PATHS.load(Ordering::Acquire)
}

/// Zero the stitching-branch count (issue #3384).
pub fn reset_batched_scan_stitching_paths() {
    BATCHED_SCAN_STITCHING_PATHS.store(0, Ordering::Release);
}

/// Detached `BatchedScanStream` tasks that have terminated, published with
/// RELEASE ordering (issue #3384).
static BATCHED_SCANS_FINISHED: AtomicU64 = AtomicU64::new(0);

/// Publish that one detached batched-scan task has terminated.
///
/// The SECOND of three abandoned-walk completion signals. `mark_query_row_producer_finished`
/// proves the outer query-row thread stopped pulling; this proves the detached task it
/// dropped without joining has stopped DECODING. Only all three together make
/// `stream_walk_partitions_parsed` final — waiting a fixed interval instead merely
/// makes the race less likely, which is the defect this issue exists to remove.
///
/// # Scope, and it is narrower than "the scan has finished" (roborev, issue #3384)
///
/// This is published from a DROP guard on the spawned future, so it covers everything
/// that decodes INSIDE that future — which is the whole of the block-by-block loop
/// taken by non-stitching (uncompressed) readers.
///
/// It does NOT cover the STITCHING path. `requires_chunk_stitching()` (compressed `nb`)
/// routes to `run_scan_stream_windowed`, which dispatches `spawn_blocking` parse/feed
/// tasks; blocking tasks are not cancellable, so they can still be decoding and
/// advancing `stream_walk_partitions_parsed` when this signal fires. On that path the
/// signal is PREMATURE and must not be read as finality. Joining those tasks on
/// cancellation is the real fix and is tracked on #3428.
pub fn mark_batched_scan_finished() {
    BATCHED_SCANS_FINISHED.fetch_add(1, Ordering::Release);
}

/// Detached batched-scan tasks that have terminated since
/// [`reset_batched_scans_finished`] (issue #3384). `Acquire`, for the same
/// happens-before reason as [`query_row_producers_finished`].
pub fn batched_scans_finished() -> u64 {
    BATCHED_SCANS_FINISHED.load(Ordering::Acquire)
}

/// Zero the detached batched-scan completion count (issue #3384).
pub fn reset_batched_scans_finished() {
    BATCHED_SCANS_FINISHED.store(0, Ordering::Release);
}

/// Zero the completion count. A test that will wait on
/// [`query_row_producers_finished`] must call this first, while holding whatever
/// lock serializes producers in its binary.
pub fn reset_query_row_producers_finished() {
    QUERY_ROW_PRODUCERS_FINISHED.store(0, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A delta must count exactly the work recorded between the two snapshots,
    /// independent of whatever a sibling test already accumulated.
    #[test]
    fn delta_counts_only_work_between_snapshots() {
        let before = ReadPathProbe::snapshot();
        record_merger_built();
        record_reconcile_entry();
        record_reconcile_entry();
        record_cell_metadata_map();
        let delta = ReadPathProbe::snapshot().delta_since(&before);
        assert!(delta.mergers_built >= 1, "the merger build was recorded");
        assert!(delta.reconcile_entries >= 2, "both reconciles recorded");
        assert!(delta.cell_metadata_maps >= 1, "the map alloc was recorded");
        assert!(delta.any_merge_work());
    }

    /// A zero delta reports no merge work — the shape the bypass assertion uses.
    #[test]
    fn zero_delta_reports_no_merge_work() {
        let a = ReadPathProbe {
            mergers_built: 7,
            reconcile_entries: 9,
            cell_metadata_maps: 11,
        };
        let delta = a.delta_since(&a);
        assert_eq!(delta, ReadPathProbe::default());
        assert!(!delta.any_merge_work());
    }
}

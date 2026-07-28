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

use std::sync::atomic::{AtomicU64, Ordering};

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

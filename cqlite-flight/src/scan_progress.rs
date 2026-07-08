//! Incremental scan-progress metering for the Flight merge/scan loop (issue #2162).
//!
//! Extracted from `producer.rs` (which is already over the campsite file-size
//! threshold, epic #1116) so the new incremental-emission machinery lives in its
//! own small module. [`ScanProgressMeter`] flushes `cqlite.query.rows_scanned` /
//! `cqlite.read.rows` / `cqlite.read.partitions` counter deltas at the
//! [`SCAN_PROGRESS_ROWS`] threshold — plus a final remainder flush on `Drop` that
//! covers every exit path — so the counters climb during a long scan while the
//! total stays byte-identical to the pre-#2162 single emission. [`ScanProgress`]
//! is the feature-independent observation seam (analogous to
//! `crate::streaming::StreamProbe`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cqlite_core::observability::{self as obs, catalog, AttrValue};

/// Row threshold at which the merge/scan loop flushes an incremental
/// `cqlite.query.rows_scanned` (and `cqlite.read.rows` / `cqlite.read.partitions`)
/// counter delta while a long scan is in progress (issue #2162). Batch-scale, so
/// emission is per-batch-scale, NEVER per row — the counter climbs before the
/// scan returns without a per-row `add_counter` on the hot path.
pub(crate) const SCAN_PROGRESS_ROWS: u64 = 8192;

/// Feature-independent progress-observation seam for the merge/scan loop (issue
/// #2162), analogous to [`crate::streaming::StreamProbe`]: cheap `Relaxed` atomics
/// always maintained, so a test can assert the number of incremental delta
/// flushes and their summed total WITHOUT depending on the `observability` OTel
/// exporter (which is a genuine no-op when the feature is off). It also carries
/// the flush `threshold` so a test can lower it to exercise multiple flushes over
/// a small fixture; production uses [`SCAN_PROGRESS_ROWS`].
#[derive(Clone)]
pub(crate) struct ScanProgress {
    /// Count of `cqlite.query.rows_scanned` delta flushes emitted (threshold
    /// crossings + the final remainder). A completed scan that exceeds the
    /// threshold records ≥ 2; one that does not records exactly 1.
    flushes: Arc<AtomicU64>,
    /// Sum of the emitted deltas — equals the scan's total examined-row count on
    /// completion (byte-identical to the pre-#2162 single emission).
    flushed_rows: Arc<AtomicU64>,
    /// Rows examined between flushes before a delta is emitted.
    threshold: u64,
}

impl Default for ScanProgress {
    fn default() -> Self {
        Self::with_threshold(SCAN_PROGRESS_ROWS)
    }
}

impl ScanProgress {
    /// Build a seam with an explicit flush threshold (tests lower it; production
    /// uses [`ScanProgress::default`] == [`SCAN_PROGRESS_ROWS`]).
    pub(crate) fn with_threshold(threshold: u64) -> Self {
        Self {
            flushes: Arc::new(AtomicU64::new(0)),
            flushed_rows: Arc::new(AtomicU64::new(0)),
            threshold: threshold.max(1),
        }
    }

    /// Number of incremental delta flushes emitted so far.
    #[cfg(test)]
    pub(crate) fn flush_count(&self) -> u64 {
        self.flushes.load(Ordering::Relaxed)
    }

    /// Summed emitted deltas so far (== total examined rows on completion).
    #[cfg(test)]
    pub(crate) fn flushed_rows(&self) -> u64 {
        self.flushed_rows.load(Ordering::Relaxed)
    }
}

/// Accumulates examined rows + scanned partitions during one merge/scan run and
/// flushes `cqlite.query.rows_scanned` / `cqlite.read.rows` / `cqlite.read.partitions`
/// counter deltas at the [`ScanProgress`] threshold, plus a final remainder flush
/// on [`Drop`] (issue #2162).
///
/// `Drop` guarantees the remainder is emitted on EVERY exit — a completed scan, a
/// `LIMIT` early break, a cooperative cancel, a merge error, or a panic — so the
/// monotonic total always equals the scan's total examined-row count and no
/// in-flight progress is ever lost. Emission is at most once per threshold of
/// examined rows, never per row.
pub(crate) struct ScanProgressMeter<'a> {
    progress: &'a ScanProgress,
    access_path: &'static str,
    examined: u64,
    partitions: u64,
    flushed_rows: u64,
    flushed_partitions: u64,
}

impl<'a> ScanProgressMeter<'a> {
    pub(crate) fn new(progress: &'a ScanProgress, access_path: &'static str) -> Self {
        Self {
            progress,
            access_path,
            examined: 0,
            partitions: 0,
            flushed_rows: 0,
            flushed_partitions: 0,
        }
    }

    /// Count one partition actually scanned (post token-range filter).
    pub(crate) fn record_partition(&mut self) {
        self.partitions += 1;
    }

    /// Count one row materialised/examined by the scan (BEFORE predicate
    /// filtering — the `rows_scanned` semantic) and flush a delta if the threshold
    /// is crossed.
    pub(crate) fn record_row(&mut self) {
        self.examined += 1;
        if self.examined - self.flushed_rows >= self.progress.threshold {
            self.flush();
        }
    }

    /// Emit the accumulated (unflushed) deltas, if any, and advance the markers.
    ///
    /// The [`ScanProgress`] observation seam (`flushes`/`flushed_rows`) tracks
    /// `cqlite.query.rows_scanned` flushes specifically (the spec's Scenario 3.2
    /// contract), so it is only bumped when `rows_delta > 0` — a partitions-only
    /// bump (a partition whose every row was a tombstone / skipped, so zero rows
    /// were examined but one partition was) still emits `cqlite.read.partitions`
    /// but does NOT count as a `rows_scanned` flush.
    fn flush(&mut self) {
        let rows_delta = self.examined - self.flushed_rows;
        let partitions_delta = self.partitions - self.flushed_partitions;
        if rows_delta == 0 && partitions_delta == 0 {
            return;
        }
        if rows_delta > 0 {
            obs::add_counter(
                catalog::QUERY_ROWS_SCANNED,
                rows_delta,
                &[(
                    catalog::attr::ACCESS_PATH,
                    AttrValue::StaticStr(self.access_path),
                )],
            );
            // Format-agnostic (issue #2162, roborev): the k-way merge reconciles
            // rows across potentially several input SSTables of possibly mixed
            // BIG/BTI format before this counter's grain, so no single format
            // label is honest here without per-input-file tallies threaded
            // through reconciliation (see catalog::READ_ROWS doc). A future
            // extension could add per-format splitting if a consumer needs it.
            obs::add_counter(catalog::READ_ROWS, rows_delta, &[]);
            self.progress.flushes.fetch_add(1, Ordering::Relaxed);
            self.progress
                .flushed_rows
                .fetch_add(rows_delta, Ordering::Relaxed);
        }
        if partitions_delta > 0 {
            // Format-agnostic for the same reason as READ_ROWS above.
            obs::add_counter(catalog::READ_PARTITIONS, partitions_delta, &[]);
        }
        self.flushed_rows = self.examined;
        self.flushed_partitions = self.partitions;
    }
}

impl Drop for ScanProgressMeter<'_> {
    fn drop(&mut self) {
        // Final remainder flush — covers a completed scan AND every early exit
        // (LIMIT break, cancel, error, panic), so the incremental total is always
        // the scan's total examined-row count.
        self.flush();
    }
}

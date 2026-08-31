//! Row-granular streaming read-path merge drive loop (issue #2230).
//!
//! The buffered [`MergeProducer::drive_merge`] loop steps the merge one WHOLE
//! PARTITION at a time (`MergeStep::Partition { rows: Vec<MergeEntry> }`), so a
//! `SELECT ... LIMIT 1` over a multi-million-row wide partition materialises the
//! ENTIRE partition into RAM before emitting a single row (peak memory =
//! O(largest partition), independent of `LIMIT`/`batch_size`), and a cancel can
//! only take effect at a partition boundary.
//!
//! This module wires the read path through issue #1668's within-partition
//! [`StreamingMerger`] instead: [`MergeProducer::drive_merge_streaming`] consumes
//! [`StreamingStep`] increments (one reconciled row per `ClusterGroup`), so peak
//! memory is bounded to a single clustering-key group and cancellation is polled
//! at ROW granularity — a huge single-partition merge can now be interrupted
//! mid-partition.
//!
//! Output rows and their order are byte-identical to `drive_merge`: both paths
//! use the SAME reconciliation primitives and `StreamingStep::ClusterGroup`
//! yields entries in the SAME relative order `MergeStep::Partition { rows }`
//! would (proven row-for-row by the `step_streaming_matches_step_for_*` oracle
//! tests in `cqlite-core`'s `merge/streaming.rs`). The token filter,
//! `ScanProgressMeter` accounting, `entry_to_row` carrier suppression, the
//! predicate filter, `batch_size` buffering, and the post-filter `LIMIT` early
//! break are preserved exactly.
//!
//! Lives in its own module (not `producer.rs`) because that file is over the
//! campsite file-size threshold (epic #1116).

use std::sync::Arc;
use std::time::Instant;

use cqlite_core::export::ArrowRowAccumulator;
use cqlite_core::observability::stream_subphase;
use cqlite_core::observability::{StreamSubPhase, StreamSubPhaseTimings};
use cqlite_core::query::PartitionKeyCache;
use cqlite_core::storage::write_engine::merge::{StreamingMerger, StreamingStep};
use cqlite_core::storage::write_engine::{DecoratedKey, KWayMerger};

use crate::batch_bytes::BatchByteCap;
use crate::cancel::CancelFlag;
use crate::egress_flush::StageEncodeAccum;
use crate::producer::{BatchSink, MergeProducer, ProducerError};
use crate::row_source::{MergeRowSource, RowSource, SourceStep};
use crate::scan_progress::{ScanProgress, ScanProgressMeter};
use crate::statics::StaticMergeSource;

/// Per-request in-`stream` sub-phase accumulator for the row-drive loop (issue
/// #2819 B2/B3).
///
/// Resolves the flight per-request sub-phase sink ONCE before the loop (a single
/// thread-local read + `Arc` clone, NOT per row — B3) and folds `stream_merge`
/// CPU nanos into a plain `u64` local as the loop runs. It is written into the
/// shared `AtomicU64` counter EXACTLY ONCE, on [`Drop`] — so an early return (a
/// cooperative cancel, a `LIMIT` break, or a `?`-propagated error) still records
/// the work already done, and a full scan makes ONE `fetch_add` regardless of
/// row count.
///
/// `stream_merge` is merge CPU only: each iteration's BLOCKING merge-input recv
/// wait (producer starvation / cold-IO, timed by the recv site into the
/// thread-local `pull_wait_nanos` accumulator) is subtracted from the iteration's
/// wall time before it is added here (B2) — so a slow producer inflates neither
/// `stream_merge` nor cold-fault falsely. (`stream_encode` is timed separately
/// in `MergeProducer::flush_credited`, around the Arrow build only — see
/// `egress_flush.rs` — so it excludes the egress-credit reserve park.)
///
/// On-path overhead (issue #2819 Medium): with a meter installed the loop pays
/// ~2 `Instant::now()` + a couple of thread-local reads PER ROW (one iteration
/// snapshot + one record). The reconcile and materialize are folded into ONE
/// timed region so no per-scope boundary clock is taken. On-path instrumentation
/// overhead; throughput microbenchmark tracked in #2980.
///
/// When no sink is installed (every non-flight caller) the accumulator is inert:
/// [`Self::active`] is `false`, so the hot loop skips `Instant::now()` entirely
/// and `Drop` records nothing.
struct RowSubPhaseAccum {
    sink: Option<Arc<StreamSubPhaseTimings>>,
    merge_nanos: u64,
}

impl RowSubPhaseAccum {
    fn new() -> Self {
        Self {
            sink: stream_subphase::current(),
            merge_nanos: 0,
        }
    }

    #[inline]
    fn active(&self) -> bool {
        self.sink.is_some()
    }

    #[inline]
    fn add_merge(&mut self, nanos: u64) {
        self.merge_nanos = self.merge_nanos.saturating_add(nanos);
    }

    /// Snapshot the start of one merge iteration: `Some(Instant)` + the current
    /// recv-wait total when a sink is installed, else `(None, 0)` — NO clock read
    /// on the non-flight path. Paired with exactly one [`Self::record_merge_iter`]
    /// per iteration.
    #[inline]
    fn iter_start(&self) -> (Option<Instant>, u64) {
        if self.active() {
            (Some(Instant::now()), stream_subphase::pull_wait_nanos())
        } else {
            (None, 0)
        }
    }

    /// Fold the whole merge region `[t0, now]` MINUS the recv-wait accrued since
    /// `wait_before` into the `stream_merge` bucket — the reconcile (`step_row`)
    /// AND the materialize (`entry_to_row`) both land here, so no per-scope
    /// boundary clock is needed (issue #2819 Medium: ~2 `Instant::now()`/row —
    /// `t0` in [`Self::iter_start`] + `now` here — not ~4). Called EXACTLY ONCE
    /// per iteration, at the point the merge region ends (before the flush/emit
    /// block, whose encode/grpc-write are separate buckets). The recv-wait is all
    /// inside `step_row` ⊂ `[t0, now]`, so subtracting the delta leaves merge CPU.
    #[inline]
    fn record_merge_iter(&mut self, t0: Option<Instant>, wait_before: u64) {
        if let Some(t0) = t0 {
            let wait = stream_subphase::pull_wait_nanos().saturating_sub(wait_before);
            self.add_merge(stream_subphase::elapsed_nanos(t0).saturating_sub(wait));
        }
    }
}

impl Drop for RowSubPhaseAccum {
    fn drop(&mut self) {
        if let Some(sink) = &self.sink {
            sink.add_nanos(StreamSubPhase::Merge, self.merge_nanos);
        }
    }
}

/// Abstraction over the ROW-granular streaming stepper — the streaming analogue
/// of `producer::PartitionStepper` (issue #2230).
///
/// Exists so the cooperative-cancellation ordering (cancel is polled BEFORE each
/// `step_row`) and the bounded-materialisation guarantee can be proven by a
/// counting test double: a pre-cancel must yield ZERO `step_row` calls (no row
/// reconciled), and a `LIMIT`/mid-partition-cancel must stop after a BOUNDED
/// number of `step_row` calls — never partition-width.
pub(crate) trait RowStepper {
    /// Advance the merge by one streaming increment (or report completion).
    fn step_row(&mut self) -> Result<StreamingStep, cqlite_core::Error>;
}

impl RowStepper for StreamingMerger<'_> {
    fn step_row(&mut self) -> Result<StreamingStep, cqlite_core::Error> {
        self.step_streaming()
    }
}

/// Forward through a mutable reference so a test double can wrap a borrowed
/// stepper (`&mut StreamingMerger`) without moving it.
impl<T: RowStepper + ?Sized> RowStepper for &mut T {
    fn step_row(&mut self) -> Result<StreamingStep, cqlite_core::Error> {
        (**self).step_row()
    }
}

impl MergeProducer {
    /// Wrap `merger` in a [`StreamingMerger`] and drive the streaming read-path
    /// loop over it (issue #2230). Called by `produce_streaming`/`merge_paths` in
    /// place of the buffered `drive_merge`, so those sites stay a one-line rename
    /// and `producer.rs` does not grow (campsite rule, epic #1116).
    pub(crate) fn drive_merge_over(
        &self,
        merger: &mut KWayMerger,
        cancel: &CancelFlag,
        sink: &mut dyn BatchSink,
        progress: &ScanProgress,
        access_path: &'static str,
    ) -> Result<(), ProducerError> {
        let mut stream = StreamingMerger::new(merger);
        self.drive_merge_streaming(&mut stream, cancel, sink, progress, access_path)
    }

    /// Drive the row-merge loop over `stepper` one reconciled row at a time,
    /// appending full-row batches (issue #2230).
    ///
    /// Semantics are byte-identical to [`MergeProducer::drive_merge`] — same
    /// token filter, `record_partition`/`record_row` accounting, carrier
    /// suppression, predicate filter, `batch_size` buffering, and post-filter
    /// `LIMIT` early break — but peak memory is bounded to one clustering-key
    /// group instead of a whole partition, and cancellation is polled at ROW
    /// granularity (before EACH `step_row`) rather than only at a partition
    /// boundary. A cancel set BEFORE the first `step_row` yields ZERO reconciled
    /// rows (mirrors `drive_merge`'s pre-`step` cancel check, issue #1473).
    pub(crate) fn drive_merge_streaming(
        &self,
        stepper: &mut dyn RowStepper,
        cancel: &CancelFlag,
        sink: &mut dyn BatchSink,
        progress: &ScanProgress,
        access_path: &'static str,
    ) -> Result<(), ProducerError> {
        let mut source = MergeRowSource::new(stepper);
        // Issue #3095: on a STATIC-bearing table the merger streams the reconciled
        // static row as an ordinary `clustering_key: None` entry, which the drive
        // loop would emit as a phantom `ck = null` row while the real rows kept a
        // null static column. `StaticMergeSource` adapts that input shape to
        // Cassandra's `processPartition()` semantics; it declines to wrap (and this
        // is a plain re-borrow) for every table without both a static and a
        // clustering column, so the non-static path is unchanged.
        match StaticMergeSource::wrap(self, &mut source) {
            Some(mut adapted) => {
                self.drive_row_source(&mut adapted, cancel, sink, progress, access_path)
            }
            None => self.drive_row_source(&mut source, cancel, sink, progress, access_path),
        }
    }

    /// The arm-independent row drive loop (issue #3058): identical batching,
    /// byte budget, cancellation, progress accounting, predicate/projection and
    /// `LIMIT` handling for BOTH the k-way merge source and the single-source
    /// scan source. See [`Self::drive_merge_streaming`] for the semantics; the
    /// only difference between the arms is where `next_step` gets its rows.
    pub(crate) fn drive_row_source(
        &self,
        source: &mut dyn RowSource,
        cancel: &CancelFlag,
        sink: &mut dyn BatchSink,
        progress: &ScanProgress,
        access_path: &'static str,
    ) -> Result<(), ProducerError> {
        let limit = self.spec.limit;
        // A zero cap produces no rows without touching the merge.
        if limit == Some(0) {
            return Ok(());
        }
        let mut meter = ScanProgressMeter::new(progress, access_path);
        // Issue #3552: the build pass's transpose, run at PUSH time and SPLIT across two
        // calls — `stage` resolves each projected cell ONCE per row and charges the
        // width from that same visit, then `commit` moves the staged cells into
        // column-major storage. Both are timed (roborev round 7); attributing the
        // whole transpose to `stage` alone is what made `stream_encode` under-report.
        // row's payload width. Reused across batches: `clear` retains capacity up to a
        // BOUND and releases the excess, so a store far over that bound may shrink and
        // later reallocate (issue #3552, roborev rounds 6-7).
        let mut buffer = ArrowRowAccumulator::with_capacity(&self.columns, self.batch_size);
        // Issue #3552: `stage` AND `commit` together perform the transpose that used to run inside
        // `flush_buffer`'s `stream_encode` region, so its wall time is folded back into
        // that SAME bucket from here — locally, one atomic on Drop, never per row. Without
        // this the transpose would have left the measured window without leaving the
        // program and `stream_encode` would read lower with no work removed.
        let mut stage_encode = StageEncodeAccum::new();
        let mut emitted: u64 = 0;
        // Issue #2819 (B2/B3): resolve the flight sub-phase sink ONCE here and
        // accumulate `stream_merge` CPU into a local, flushed to the shared atomic
        // on this accumulator's Drop (covers every early return below). Inert
        // (no `Instant::now`) when no flight sink is installed. (`stream_encode` is
        // timed in `flush_credited`.)
        let mut accum = RowSubPhaseAccum::new();
        // Issue #2825: the SAME dual-boundary accumulator `drive_merge` uses — a
        // cap wired into only one egress path would leave the other unbounded.
        let mut byte_cap = BatchByteCap::new(self.max_batch_bytes);
        // Issue #2821: the SAME per-merge array-node count `drive_merge` uses —
        // a governor wired into only one loop would leave the other unbounded.
        let n_array_nodes = self.egress_array_nodes()?;
        // Issue #1817: one partition-key decode cache for the whole merge; each
        // partition's rows arrive consecutively, so its key decodes once.
        let mut pk_cache = PartitionKeyCache::default();
        // Issue #2324 (roborev 1633): projection-aware assembly set, computed once.
        let assemble_cols = self.assemble_columns();

        // Per-partition token / record-once state. `drive_merge` calls
        // `record_partition()` exactly once per token-passing partition BEFORE
        // iterating its rows; here we (re)evaluate the token filter and record on
        // the first increment of each new partition (whether a `ClusterGroup` or,
        // for an empty-but-token-passing partition, its `PartitionEnd`).
        let mut partition_key: Option<DecoratedKey> = None;
        let mut partition_active = false;
        let mut partition_recorded = false;

        loop {
            // Cooperative cancellation at ROW granularity (issue #2230): polled
            // BEFORE each pull, so a client disconnect stops the merge mid-partition
            // — not only at a partition boundary.
            if cancel.is_cancelled() {
                return Err(ProducerError::Cancelled);
            }
            // Map by VARIANT, not by racing the cancel flag (issue #2264): a real
            // I/O/corruption error that happens to race a client disconnect is
            // NEVER masked as a clean `Cancelled` abort — only an actual
            // cancellation maps to `ProducerError::Cancelled`.
            //
            // Issue #2819 (B2/Medium): snapshot ONE iteration start here; the whole
            // merge region — `step_row` reconcile (k-way merge + LWW/tombstone/TTL,
            // which also does the BLOCKING merge-input recv) PLUS the `entry_to_row`
            // materialize — is folded into `stream_merge` at the region's end via a
            // SINGLE `record_merge_iter`, with the recv-wait delta subtracted (the
            // recv-wait is producer starvation / cold-IO, not merge CPU). ~2
            // `Instant::now()`/row (this snapshot + the record), not ~4.
            let (iter_t0, wait_before) = accum.iter_start();
            // Errors already arrive in the producer taxonomy (mapped by VARIANT
            // inside each source, issue #2264 — never by racing the cancel flag).
            let step = source.next_step()?;

            let (key, entry) = match step {
                SourceStep::Row(key, row) => (key, row),
                SourceStep::PartitionEnd(key) => {
                    // An empty (all-purged) but token-passing partition still
                    // counts as scanned in `drive_merge` (its empty
                    // `MergeStep::Partition` fires `record_partition` before its
                    // zero rows) — mirror that here.
                    self.begin_partition(
                        &key,
                        &mut partition_key,
                        &mut partition_active,
                        &mut partition_recorded,
                        &mut meter,
                    );
                    accum.record_merge_iter(iter_t0, wait_before);
                    continue;
                }
                SourceStep::Complete => {
                    accum.record_merge_iter(iter_t0, wait_before);
                    break;
                }
            };

            self.begin_partition(
                &key,
                &mut partition_key,
                &mut partition_active,
                &mut partition_recorded,
                &mut meter,
            );
            // Token-range filter: drop whole partitions outside the split's range.
            if !partition_active {
                accum.record_merge_iter(iter_t0, wait_before);
                continue;
            }

            // Build the row so predicates can reference any projected-out column
            // too (`assemble_cols` includes filter-referenced columns); carriers
            // (`entry_to_row` → None) are skipped without counting a row. Its
            // materialize CPU is part of the `stream_merge` region timed above.
            let Some(row) =
                self.materialize_pending(&key, entry, &mut pk_cache, assemble_cols.as_ref())?
            else {
                accum.record_merge_iter(iter_t0, wait_before);
                continue;
            };
            // Count a row materialised/examined by the scan (BEFORE the predicate
            // filter — the `rows_scanned` semantic).
            meter.record_row();
            // Predicate pushdown: keep the row only when it is definitely True.
            if let Some(filter) = &self.spec.filter {
                if !filter.keeps(&row) {
                    accum.record_merge_iter(iter_t0, wait_before);
                    continue;
                }
            }
            // Merge region (reconcile + materialize + predicate) ends here — record
            // it ONCE, BEFORE the flush/emit block below (encode + grpc-write are
            // separate buckets).
            accum.record_merge_iter(iter_t0, wait_before);
            // Dual row-cap / byte-cap boundary (issue #2825): the row's Arrow
            // payload width is charged BEFORE it joins the batch and the cut fires
            // on the row that WOULD cross the cap. Test-then-push, so a row wider
            // than the whole cap still leaves as a one-row batch instead of
            // flushing an empty buffer.
            //
            // Issue #3552: `stage` resolves the row's cells into the columnar
            // store's staging slot and charges the width from THOSE cells, so the
            // width no longer costs a second, independent resolution pass. The
            // staged row joins the batch at `commit`, after any cut — the same
            // order (and the same numbers) as the estimate/push it replaces.
            let width = stage_encode.timed(|| buffer.stage(row));
            if byte_cap.cut_before(width).is_yes() {
                self.flush_credited(sink, &mut buffer, &mut byte_cap, n_array_nodes)?;
            }
            // Timed for the same reason as the drive loop's: `commit` walks every
            // projected slot to move staged cells into column-major storage, and that is
            // work the fold MOVED here from the formerly-timed transpose (roborev round 7).
            // The flush above stays outside both windows — it is already Encode's region.
            stage_encode.timed(|| buffer.commit());
            emitted += 1;
            byte_cap.accumulate(width);
            if buffer.len() >= self.batch_size {
                self.flush_credited(sink, &mut buffer, &mut byte_cap, n_array_nodes)?;
            }
            // LIMIT reached (counted post-filter): stop the merge early.
            if let Some(cap) = limit {
                if emitted >= cap {
                    break;
                }
            }
        }

        if !buffer.is_empty() {
            self.flush_credited(sink, &mut buffer, &mut byte_cap, n_array_nodes)?;
        }
        Ok(())
    }

    /// Establish per-partition token/record state on the first increment of a
    /// partition, recording it exactly once (post token filter) — the streaming
    /// equivalent of `drive_merge`'s per-partition `record_partition()`.
    fn begin_partition(
        &self,
        key: &DecoratedKey,
        partition_key: &mut Option<DecoratedKey>,
        partition_active: &mut bool,
        partition_recorded: &mut bool,
        meter: &mut ScanProgressMeter,
    ) {
        if partition_key.as_ref() != Some(key) {
            *partition_key = Some(key.clone());
            *partition_active = match &self.spec.token {
                Some(token) => token.contains(key.token),
                None => true,
            };
            *partition_recorded = false;
        }
        if *partition_active && !*partition_recorded {
            meter.record_partition();
            *partition_recorded = true;
        }
    }
}

#[cfg(test)]
#[path = "producer_stream_tests.rs"]
mod tests;

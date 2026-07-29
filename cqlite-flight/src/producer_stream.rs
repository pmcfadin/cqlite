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

use cqlite_core::export::estimate_arrow_row_bytes;
use cqlite_core::observability::stream_subphase;
use cqlite_core::observability::{StreamSubPhase, StreamSubPhaseTimings};
use cqlite_core::query::{PartitionKeyCache, QueryRow};
use cqlite_core::storage::write_engine::merge::{StreamingMerger, StreamingStep};
use cqlite_core::storage::write_engine::{DecoratedKey, KWayMerger};

use crate::batch_bytes::BatchByteCap;
use crate::cancel::CancelFlag;
use crate::producer::{BatchSink, MergeProducer, ProducerError};
use crate::row_source::{MergeRowSource, RowSource, SourceStep};
use crate::scan_progress::{ScanProgress, ScanProgressMeter};

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
        self.drive_row_source(&mut source, cancel, sink, progress, access_path)
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
        let mut buffer: Vec<QueryRow> = Vec::with_capacity(self.batch_size);
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
            let step = source.next_step().map_err(|e| match e {
                cqlite_core::Error::Cancelled => ProducerError::Cancelled,
                other => ProducerError::Merge(other),
            })?;

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
            // Dual row-cap / byte-cap boundary (issue #2825): estimate the row's
            // Arrow payload width BEFORE it moves into the buffer and cut on the
            // row that WOULD cross the cap. Test-then-push, so a row wider than
            // the whole cap still leaves as a one-row batch instead of flushing
            // an empty buffer.
            let width = estimate_arrow_row_bytes(&self.columns, &row);
            if byte_cap.cut_before(width).is_yes() {
                self.flush_credited(sink, &mut buffer, &mut byte_cap, n_array_nodes)?;
            }
            buffer.push(row);
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
mod tests {
    use super::{MergeProducer, RowStepper};
    use crate::cancel::CancelFlag;
    use crate::filter::ScanSpec;
    use crate::producer::{CollectSink, DirSource, ProducerError, SstableSource};
    use crate::scan_progress::ScanProgress;
    use crate::testutil::{build_sstables, clustering_schema, total_rows, write_clustered};
    use cqlite_core::query::AccessPath;
    use cqlite_core::storage::write_engine::merge::{StreamingMerger, StreamingStep};
    use cqlite_core::storage::write_engine::KWayMerger;

    /// Number of clustering rows in a single WIDE partition. Large enough that
    /// materialising it whole (the pre-#2230 behaviour) is obviously distinct
    /// from the bounded pull the fix performs, small enough to stay fast.
    const WIDTH: usize = 500;

    /// A [`RowStepper`] that counts `step_row` calls, so a test can prove the
    /// drive loop pulls only a BOUNDED number of reconciled rows before a
    /// `LIMIT`/cancel stops it — never the whole partition.
    struct CountingRowStepper<S> {
        inner: S,
        count: usize,
    }

    impl<S: RowStepper> RowStepper for CountingRowStepper<S> {
        fn step_row(&mut self) -> Result<StreamingStep, cqlite_core::Error> {
            self.count += 1;
            self.inner.step_row()
        }
    }

    /// A [`RowStepper`] that counts `step_row` calls AND sets a [`CancelFlag`]
    /// once it has yielded `cancel_after` increments (simulating a client
    /// disconnect landing mid-partition).
    struct CancellingRowStepper<'a, S> {
        inner: S,
        cancel: &'a CancelFlag,
        cancel_after: usize,
        count: usize,
    }

    impl<S: RowStepper> RowStepper for CancellingRowStepper<'_, S> {
        fn step_row(&mut self) -> Result<StreamingStep, cqlite_core::Error> {
            self.count += 1;
            let step = self.inner.step_row()?;
            if self.count >= self.cancel_after {
                self.cancel.cancel();
            }
            Ok(step)
        }
    }

    /// A [`RowStepper`] that sleeps in `step_row` and attributes 3/4 of the
    /// MEASURED sleep to the pull-wait accumulator (simulating a BLOCKING
    /// merge-input recv) before completing — so a test can prove the drive loop
    /// SUBTRACTS recv-wait from the `stream_merge` bucket (issue #2819 B2). The
    /// injected wait is a fraction of the ACTUAL elapsed sleep (recorded in
    /// `actual_nanos`), not a hardcoded constant, so the assertion is
    /// host-independent (no #2642 wall-clock race).
    struct RecvWaitStepper {
        sleep: std::time::Duration,
        actual_nanos: u64,
        done: bool,
    }

    impl RowStepper for RecvWaitStepper {
        fn step_row(&mut self) -> Result<StreamingStep, cqlite_core::Error> {
            if self.done {
                return Ok(StreamingStep::Complete);
            }
            let t = std::time::Instant::now();
            std::thread::sleep(self.sleep);
            let actual = cqlite_core::observability::stream_subphase::elapsed_nanos(t);
            self.actual_nanos = actual;
            // Attribute 3/4 of the MEASURED sleep to recv-wait, as the real recv
            // site would — a fraction of what actually elapsed, never a constant.
            cqlite_core::observability::stream_subphase::add_pull_wait_nanos(actual * 3 / 4);
            self.done = true;
            Ok(StreamingStep::Complete)
        }
    }

    /// Build one wide partition (`pk = 1`, `WIDTH` clustering rows) in a single
    /// SSTable and return its table dir (temp dir kept alive by the caller).
    fn wide_partition() -> (tempfile::TempDir, std::path::PathBuf) {
        let schema = clustering_schema();
        let rows: Vec<_> = (0..WIDTH)
            .map(|i| write_clustered(1, &format!("ck{i:04}"), i as i32, 100))
            .collect();
        let (temp, _data, dir) = build_sstables(&schema, vec![rows]);
        (temp, dir)
    }

    /// AC1 (bounded intra-partition memory): a `LIMIT 1` scan over a wide
    /// partition must reconcile only a BOUNDED number of rows (here: one
    /// `ClusterGroup`) before it emits and stops — NOT the whole partition. The
    /// counting stepper proves `drive_merge_streaming` pulls row-granular
    /// increments and breaks at the `LIMIT`, rather than draining the partition
    /// as the buffered `drive_merge` (one `step()` = whole partition) would.
    #[test]
    fn streaming_limit_one_reconciles_bounded_rows_not_whole_partition() {
        let schema = clustering_schema();
        let (_temp, dir) = wide_partition();
        let spec = ScanSpec {
            limit: Some(1),
            ..Default::default()
        };
        let producer = MergeProducer::with_spec(schema.clone(), 8192, spec).unwrap();

        let paths = DirSource::new(&dir).data_paths().unwrap();
        let mut merger = KWayMerger::new(paths, &schema).unwrap();
        let mut stream = StreamingMerger::new(&mut merger);
        let mut counting = CountingRowStepper {
            inner: &mut stream,
            count: 0,
        };

        let mut batches = Vec::new();
        {
            let mut sink = CollectSink(&mut batches);
            producer
                .drive_merge_streaming(
                    &mut counting,
                    &CancelFlag::new(),
                    &mut sink,
                    &ScanProgress::default(),
                    AccessPath::FullScan.label(),
                )
                .expect("streaming drive succeeds");
        }

        assert_eq!(total_rows(&batches), 1, "LIMIT 1 emits exactly one row");
        // A live-row wide partition with no tombstones yields the first row on
        // the first `step_row`; allow a tiny margin for any leading carrier, but
        // it must be WAY below partition width.
        assert!(
            counting.count <= 4,
            "LIMIT 1 pulled {} increments — must be bounded, not partition-width ({WIDTH})",
            counting.count
        );
    }

    /// AC1 (allow `&mut S` to be used as a stepper via the trait): a wrapper over
    /// a borrowed stepper must forward correctly. (Compile-and-behaviour guard
    /// for the generic bound used above.)
    #[test]
    fn counting_stepper_forwards_borrowed_inner() {
        let schema = clustering_schema();
        let (_temp, dir) = wide_partition();
        let paths = DirSource::new(&dir).data_paths().unwrap();
        let mut merger = KWayMerger::new(paths, &schema).unwrap();
        let mut stream = StreamingMerger::new(&mut merger);
        let mut counting = CountingRowStepper {
            inner: &mut stream,
            count: 0,
        };
        // One manual pull must yield a ClusterGroup and bump the count.
        let step = counting.step_row().expect("step");
        assert!(matches!(step, StreamingStep::ClusterGroup { .. }));
        assert_eq!(counting.count, 1);
    }

    /// AC2 (mid-partition cancellation): a cancel set mid-partition must stop the
    /// merge within a bounded number of rows — NOT at partition end — and return
    /// `ProducerError::Cancelled`. The cancelling stepper sets the flag after
    /// `CANCEL_AFTER` increments; because the drive loop polls the cancel BEFORE
    /// each pull, exactly `CANCEL_AFTER` increments are pulled, well below
    /// partition width.
    #[test]
    fn streaming_cancel_mid_partition_stops_within_bounded_rows() {
        const CANCEL_AFTER: usize = 5;
        let schema = clustering_schema();
        let (_temp, dir) = wide_partition();
        // No LIMIT: without the fix the whole partition would drain before any
        // cancel could be observed.
        let producer = MergeProducer::new(schema.clone(), 8192).unwrap();

        let paths = DirSource::new(&dir).data_paths().unwrap();
        let mut merger = KWayMerger::new(paths, &schema).unwrap();
        let mut stream = StreamingMerger::new(&mut merger);
        let cancel = CancelFlag::new();
        let mut cancelling = CancellingRowStepper {
            inner: &mut stream,
            cancel: &cancel,
            cancel_after: CANCEL_AFTER,
            count: 0,
        };

        let mut batches = Vec::new();
        let err = {
            let mut sink = CollectSink(&mut batches);
            producer
                .drive_merge_streaming(
                    &mut cancelling,
                    &cancel,
                    &mut sink,
                    &ScanProgress::default(),
                    AccessPath::FullScan.label(),
                )
                .expect_err("mid-partition cancel aborts")
        };

        assert!(
            matches!(err, ProducerError::Cancelled),
            "expected ProducerError::Cancelled, got {err:?}"
        );
        assert_eq!(
            cancelling.count, CANCEL_AFTER,
            "cancel is polled BEFORE each pull, so exactly {CANCEL_AFTER} increments \
             are pulled — mid-partition, not partition-width ({WIDTH})"
        );
        assert!(
            cancelling.count < WIDTH,
            "the merge stopped mid-partition, not at partition end"
        );
    }

    /// A cancel set BEFORE the first `step_row` must abort having reconciled ZERO
    /// rows (mirrors `drive_merge`'s pre-`step` cancel check, issue #1473) — the
    /// streaming path's cancel is checked BEFORE the first pull.
    #[test]
    fn streaming_pre_cancel_reconciles_zero_rows() {
        let schema = clustering_schema();
        let (_temp, dir) = wide_partition();
        let producer = MergeProducer::new(schema.clone(), 8192).unwrap();

        let paths = DirSource::new(&dir).data_paths().unwrap();
        let mut merger = KWayMerger::new(paths, &schema).unwrap();
        let mut stream = StreamingMerger::new(&mut merger);
        let mut counting = CountingRowStepper {
            inner: &mut stream,
            count: 0,
        };

        let cancelled = CancelFlag::new();
        cancelled.cancel();
        let mut batches = Vec::new();
        let err = {
            let mut sink = CollectSink(&mut batches);
            producer
                .drive_merge_streaming(
                    &mut counting,
                    &cancelled,
                    &mut sink,
                    &ScanProgress::default(),
                    AccessPath::FullScan.label(),
                )
                .expect_err("pre-cancelled streaming merge aborts")
        };

        assert!(matches!(err, ProducerError::Cancelled));
        assert_eq!(
            counting.count, 0,
            "cancel must be observed BEFORE any step_row — zero rows reconciled"
        );
        assert!(batches.is_empty(), "no batch produced when pre-cancelled");
    }

    /// End-to-end wiring + byte-identity: the streaming `produce_streaming` path
    /// (now `drive_merge_streaming`) must return exactly the SAME batches as the
    /// buffered collect path (`produce` → `merge_paths` → `drive_merge`) over the
    /// same data — proving `produce_streaming` is actually wired to the streaming
    /// drive AND that its output is unchanged.
    #[test]
    fn produce_streaming_matches_buffered_collect_path() {
        let schema = clustering_schema();
        // Two SSTables, a wide partition plus a couple of narrow ones, so the
        // merge really interleaves runs and crosses partition boundaries.
        let batch_a: Vec<_> = (0..WIDTH)
            .map(|i| write_clustered(1, &format!("ck{i:04}"), i as i32, 100))
            .collect();
        let batch_b = vec![
            write_clustered(1, "ck0000", 999, 200), // newer wins for ck0000
            write_clustered(2, "z", 7, 100),
            write_clustered(3, "y", 8, 100),
        ];
        let (_temp, _data, dir) = build_sstables(&schema, vec![batch_a, batch_b]);

        let producer = MergeProducer::new(schema, 64).unwrap();
        let source = DirSource::new(&dir);
        let buffered = producer.produce(&source).expect("buffered collect");
        let paths = source.data_paths().unwrap();
        let streamed = producer
            .produce_streaming_to_vec(paths, &CancelFlag::new())
            .expect("streaming path");

        assert_eq!(
            total_rows(&buffered),
            total_rows(&streamed),
            "streaming and buffered paths emit the same row count"
        );
        assert_eq!(
            buffered.len(),
            streamed.len(),
            "same batch count (identical batch_size chunking)"
        );
        for (b, s) in buffered.iter().zip(streamed.iter()) {
            assert_eq!(b, s, "streaming batch must be byte-identical to buffered");
        }
    }

    /// Spec R3 (issue #3058): a row materialized from the SINGLE-GENERATION scan
    /// arm carries `cell_metadata: None`, exactly as the merge arm's rows do — no
    /// consumer can observe a difference in the emitted `QueryRow`, and no
    /// per-cell write-metadata map is attached to it.
    #[test]
    fn a_scanned_row_carries_no_cell_metadata() {
        use crate::row_source::PendingRow;
        use cqlite_core::query::PartitionKeyCache;
        use cqlite_core::storage::write_engine::DecoratedKey;
        use cqlite_core::types::{ScanRow, Value};
        use cqlite_core::RowKey;
        use std::sync::Arc as StdArc;

        let schema = crate::testutil::simple_schema();
        let producer = MergeProducer::new(schema, 8192).unwrap();
        // `id` is the partition key (4-byte big-endian int); the decoded cells
        // carry only the regular columns, as the single-generation reader emits.
        let key_bytes = 7_i32.to_be_bytes().to_vec();
        let scan_row = ScanRow::Row(vec![
            (StdArc::from("name"), Value::text("n7")),
            (StdArc::from("score"), Value::Integer(70)),
        ]);
        let mut pk_cache = PartitionKeyCache::default();
        let row = producer
            .materialize_pending(
                &DecoratedKey::new(0, key_bytes.clone()),
                PendingRow::Scanned(RowKey::new(key_bytes), scan_row),
                &mut pk_cache,
                None,
            )
            .expect("materialize succeeds")
            .expect("a live scan row is emitted");

        assert!(
            row.cell_metadata.is_none(),
            "the fast arm's emitted QueryRow must carry NO cell metadata (identical \
             to the merge arm's rows — `filter.rs`/`agg.rs` never read it)"
        );
        assert_eq!(row.values.get("name"), Some(&Value::text("n7")));
        assert_eq!(row.values.get("score"), Some(&Value::Integer(70)));
        assert_eq!(
            row.values.get("id"),
            Some(&Value::Integer(7)),
            "the partition-key column is reconstructed from the row key"
        );
    }

    /// B2 (recv-wait exclusion): the drive loop must SUBTRACT the blocking
    /// merge-input recv-wait from the `stream_merge` bucket, so `stream_merge` is
    /// merge CPU only. A stub `step_row` sleeps, attributes 3/4 of the MEASURED
    /// sleep to the pull-wait accumulator (as the real recv site would), then
    /// completes. `stream_merge` must land BELOW that injected 3/4 (leaving ≈1/4)
    /// — a CORRECTNESS metric-vs-metric check (recorded merge bucket vs the
    /// recorded recv-wait, both derived from the SAME measured sleep), NOT a
    /// host-latency threshold (no #2642 wall-clock race — neither side is a
    /// constant). Without the subtraction `stream_merge` ≈ the full measured sleep,
    /// i.e. ABOVE the injected 3/4, so it fails closed at any host speed.
    #[test]
    fn stream_merge_excludes_recv_wait() {
        use cqlite_core::observability::{stream_subphase, StreamSubPhase, StreamSubPhaseTimings};
        use std::sync::Arc;
        use std::time::Duration;

        let schema = clustering_schema();
        let producer = MergeProducer::new(schema, 8192).unwrap();

        // Install a sink on THIS (drive) thread so the accumulator records into it.
        let timings = Arc::new(StreamSubPhaseTimings::default());
        let _install = stream_subphase::install(Some(timings.clone()));

        let mut stepper = RecvWaitStepper {
            sleep: Duration::from_millis(40),
            actual_nanos: 0,
            done: false,
        };

        let mut batches = Vec::new();
        {
            let mut sink = CollectSink(&mut batches);
            producer
                .drive_merge_streaming(
                    &mut stepper,
                    &CancelFlag::new(),
                    &mut sink,
                    &ScanProgress::default(),
                    AccessPath::FullScan.label(),
                )
                .expect("drive succeeds");
        }

        // Injected recv-wait = 3/4 of the MEASURED sleep; merge must be the
        // remaining ≈1/4, strictly below the injected wait regardless of host speed.
        let injected_wait = stepper.actual_nanos * 3 / 4;
        let merge = timings.nanos(StreamSubPhase::Merge);
        assert!(
            merge < injected_wait,
            "stream_merge ({merge} ns) must EXCLUDE the injected recv-wait \
             ({injected_wait} ns, 3/4 of the {} ns measured sleep) — the B2 \
             subtraction regressed",
            stepper.actual_nanos
        );
    }
}

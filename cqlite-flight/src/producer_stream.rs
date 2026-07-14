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

use arrow::record_batch::RecordBatch;
use cqlite_core::export::rows_to_record_batch;
use cqlite_core::query::{PartitionKeyCache, QueryRow};
use cqlite_core::storage::write_engine::merge::{StreamingMerger, StreamingStep};
use cqlite_core::storage::write_engine::{DecoratedKey, KWayMerger};

use crate::cancel::CancelFlag;
use crate::producer::{BatchSink, MergeProducer, ProducerError};
use crate::scan_progress::{ScanProgress, ScanProgressMeter};

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

    /// Convert `buffer`'s rows into an Arrow batch and clear it — the streaming
    /// equivalent of `MergeProducer::flush_buffer` (kept here so that private
    /// helper need not widen its visibility and force a signature rewrap in the
    /// over-threshold `producer.rs`).
    fn flush(&self, buffer: &mut Vec<QueryRow>) -> Result<RecordBatch, ProducerError> {
        let batch = rows_to_record_batch(&self.columns, buffer)?;
        buffer.clear();
        Ok(batch)
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
        let limit = self.spec.limit;
        // A zero cap produces no rows without touching the merge.
        if limit == Some(0) {
            return Ok(());
        }
        let mut meter = ScanProgressMeter::new(progress, access_path);
        let mut buffer: Vec<QueryRow> = Vec::with_capacity(self.batch_size);
        let mut emitted: u64 = 0;
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
            let step = stepper.step_row().map_err(|e| match e {
                cqlite_core::Error::Cancelled => ProducerError::Cancelled,
                other => ProducerError::Merge(other),
            })?;

            let (key, entry) = match step {
                StreamingStep::ClusterGroup { key, row } => (key, row),
                StreamingStep::PartitionEnd { key } => {
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
                    continue;
                }
                StreamingStep::Complete => break,
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
                continue;
            }

            // Build the row so predicates can reference any projected-out column
            // too (`assemble_cols` includes filter-referenced columns); carriers
            // (`entry_to_row` → None) are skipped without counting a row.
            let Some(row) = self.entry_to_row(
                &key.key,
                entry.row_data,
                &mut pk_cache,
                assemble_cols.as_ref(),
            )?
            else {
                continue;
            };
            // Count a row materialised/examined by the scan (BEFORE the predicate
            // filter — the `rows_scanned` semantic).
            meter.record_row();
            // Predicate pushdown: keep the row only when it is definitely True.
            if let Some(filter) = &self.spec.filter {
                if !filter.keeps(&row) {
                    continue;
                }
            }
            buffer.push(row);
            emitted += 1;
            if buffer.len() >= self.batch_size {
                sink.emit(self.flush(&mut buffer)?)?;
            }
            // LIMIT reached (counted post-filter): stop the merge early.
            if let Some(cap) = limit {
                if emitted >= cap {
                    break;
                }
            }
        }

        if !buffer.is_empty() {
            sink.emit(self.flush(&mut buffer)?)?;
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
}

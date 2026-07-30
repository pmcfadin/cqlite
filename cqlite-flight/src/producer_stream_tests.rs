//! Unit tests for the row-granular streaming drive loop (issue #2230), split out
//! of `producer_stream.rs` to keep that file under the campsite file-size target
//! (epic #1116).

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

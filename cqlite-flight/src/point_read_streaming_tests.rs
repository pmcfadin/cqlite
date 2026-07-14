//! Row-granular streaming for the point-read and cache-warm merge paths
//! (issue #2423).
//!
//! #2230 routed the full-scan `do_get` path through the within-partition
//! `StreamingMerger` (`drive_merge_over` / `drive_merge_streaming`) so a
//! `LIMIT`/small-batch scan over a huge wide partition materialises only one
//! clustering-key group at a time and a cancel takes effect mid-partition. But
//! two sibling paths still drove the BUFFERED `drive_merge`
//! (`KWayMerger::step()` = a whole partition per step): the partition
//! point-read (`WHERE pk = ?`, `produce_point`) and the cache-warm reader set
//! (`produce_streaming_from_readers`). Over a multi-million-row wide partition
//! — the AWS field team's most common read shape — those still buffered the
//! ENTIRE target partition before emitting a row and cancelled only at a
//! partition boundary.
//!
//! These pins go through the REAL producer paths (not a hand-built drive) and
//! prove the fix by OBSERVABLE behaviour: a sink that trips the shared
//! `CancelFlag` after the first batch (WITHOUT returning an error, so it does
//! not short-circuit via `?`) forces the drive to stop at ITS OWN cancel-poll
//! granularity. The buffered drive polls cancel only at a partition boundary,
//! so over ONE wide partition it emits every row before observing the cancel;
//! the streaming drive polls before each row, so it stops within ~`batch_size`
//! rows. That difference is RED on the unfixed buffered code and GREEN after the
//! `drive_merge` → `drive_merge_over` rename.
//!
//! In-crate (not `tests/`) so they can use the `testutil` write-engine fixture
//! builders and the `pub(crate)` producer seams (`produce_streaming`,
//! `produce_streaming_from_readers`, `drive_merge` / `drive_merge_over`).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::cancel::CancelFlag;
use crate::filter::ScanSpec;
use crate::producer::{BatchSink, CollectSink, DirSource, MergeProducer, ProducerError};
use crate::scan_progress::ScanProgress;
use crate::testutil::{build_sstables, clustering_schema, total_rows, write_clustered};
use crate::ticket::{FlightTicket, Predicate, PredicateOp};

use cqlite_core::query::AccessPath;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::{build_single_partition_merger, PartitionKey};
use cqlite_core::types::Value;
use cqlite_core::{Config, Platform};
use serde_json::json;

/// Clustering rows in the single WIDE target partition. Large enough that
/// materialising it whole (the buffered pre-fix behaviour) is obviously
/// distinct from the bounded `batch_size` pull the streaming fix performs, small
/// enough to stay fast.
const WIDTH: usize = 500;

/// Small merge/emit batch so the streaming drive emits its first batch after a
/// handful of rows — well below `WIDTH`.
const BATCH: usize = 4;

/// A `BatchSink` that counts rows across all batches AND, after each batch,
/// trips a shared [`CancelFlag`] (simulating a client disconnect landing
/// mid-partition). It does NOT return an error: returning `Err` from `emit`
/// would short-circuit the drive via `sink.emit(..)?` on BOTH paths and hide the
/// buffered/streaming difference. Setting the flag instead forces each drive to
/// observe the cancel at its own poll granularity — a partition boundary
/// (buffered) vs before each row (streaming).
struct CancelAfterFirstBatchSink<'a> {
    cancel: &'a CancelFlag,
    rows: usize,
}

impl BatchSink for CancelAfterFirstBatchSink<'_> {
    fn emit(&mut self, batch: RecordBatch) -> Result<(), ProducerError> {
        self.rows += batch.num_rows();
        self.cancel.cancel();
        Ok(())
    }
}

/// Build one WIDE partition (`pk = 1`, `WIDTH` clustering rows) in a single
/// SSTable; return the temp dir (kept alive by the caller) and its table dir.
fn wide_partition() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = clustering_schema();
    let rows: Vec<_> = (0..WIDTH)
        .map(|i| write_clustered(1, &format!("ck{i:04}"), i as i32, 100))
        .collect();
    let (temp, _data, dir) = build_sstables(&schema, vec![rows]);
    (temp, dir)
}

/// A point-read spec (`WHERE pk = 1`, no LIMIT) over the clustering schema.
fn point_spec(schema: &TableSchema) -> ScanSpec {
    let ticket = FlightTicket {
        keyspace: "flight_ks".into(),
        table: "wide".into(),
        predicates: vec![Predicate {
            column: "pk".into(),
            op: PredicateOp::Equal,
            value: json!(1),
        }],
        ..Default::default()
    };
    ScanSpec::from_ticket(&ticket, schema).expect("point spec")
}

/// Open every `-Data.db` under `dir` as a warm `Arc<SSTableReader>` (mirrors the
/// warm-registry hand-off in `producer_warm.rs`'s own test).
fn warm_readers(dir: &std::path::Path) -> Vec<Arc<SSTableReader>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let mut readers = Vec::new();
        for entry in std::fs::read_dir(dir).expect("read table dir").flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
            {
                let reader = SSTableReader::open(&path, &config, Arc::clone(&platform))
                    .await
                    .expect("open reader");
                readers.push(Arc::new(reader));
            }
        }
        readers
    })
}

// ---- AC1: point-read materialises only O(batch_size) rows, not the whole partition ----

/// AC1. A `WHERE pk = 1` point read over a wide partition, with a sink that
/// trips the cancel after the first batch, must materialise/emit only
/// O(`batch_size`) rows before the drive observes the cancel and stops — NOT the
/// whole partition. RED pre-fix: the buffered `produce_point` → `drive_merge`
/// steps the whole partition in one `step()` and emits EVERY row before its next
/// (partition-boundary) cancel poll, so the sink sees all `WIDTH` rows.
#[test]
fn point_read_materialises_bounded_rows_not_whole_partition() {
    let (_temp, dir) = wide_partition();
    let schema = clustering_schema();
    let producer = MergeProducer::with_spec(schema, BATCH, point_spec(&clustering_schema())).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();

    let cancel = CancelFlag::new();
    let mut sink = CancelAfterFirstBatchSink {
        cancel: &cancel,
        rows: 0,
    };
    // The point route drives `produce_point`; after the fix that is the streaming
    // drive, so it stops within ~BATCH rows of the first emit. The drive returns
    // `Cancelled` once it observes the tripped flag at a row/partition boundary.
    let _ = producer.produce_streaming(paths, &cancel, &mut sink, &ScanProgress::default(), || {});

    assert!(
        sink.rows <= BATCH * 4,
        "point read materialised {} rows before the cancel took effect — must be \
         bounded to ~batch_size, not the whole partition ({WIDTH})",
        sink.rows
    );
}

// ---- AC2: cancellation takes effect mid-partition on the point-read path ----

/// AC2. A cancel tripped mid-partition on the point-read path must stop the merge
/// within a BOUNDED number of rows (not at partition end) and surface as
/// `ProducerError::Cancelled`. RED pre-fix: the buffered drive drains the whole
/// partition before its next cancel poll, so the sink sees all `WIDTH` rows even
/// though the flag was tripped after the first batch.
#[test]
fn point_read_cancel_takes_effect_mid_partition() {
    let (_temp, dir) = wide_partition();
    let schema = clustering_schema();
    let producer = MergeProducer::with_spec(schema, BATCH, point_spec(&clustering_schema())).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();

    let cancel = CancelFlag::new();
    let mut sink = CancelAfterFirstBatchSink {
        cancel: &cancel,
        rows: 0,
    };
    let err = producer
        .produce_streaming(paths, &cancel, &mut sink, &ScanProgress::default(), || {})
        .expect_err("a mid-partition cancel aborts the point read");

    assert!(
        matches!(err, ProducerError::Cancelled),
        "mid-partition cancel must surface as Cancelled, got {err:?}"
    );
    assert!(
        sink.rows < WIDTH,
        "the point read stopped mid-partition ({} rows), not at partition end ({WIDTH})",
        sink.rows
    );
}

// ---- AC3: the cache-warm path is likewise bounded ----

/// AC3 (warm point-read, `producer_warm.rs` line 92). The warm reader-set point
/// read must be bounded exactly as the cold path: a cancel tripped after the
/// first batch stops the merge within ~`batch_size` rows, not the whole
/// partition. RED pre-fix: `produce_streaming_from_readers`'s point branch drove
/// the buffered `drive_merge`.
#[test]
fn warm_point_read_materialises_bounded_rows_not_whole_partition() {
    let (_temp, dir) = wide_partition();
    let schema = clustering_schema();
    let readers = warm_readers(&dir);
    assert!(!readers.is_empty(), "the fixture must ship a warm reader");
    let producer = MergeProducer::with_spec(schema, BATCH, point_spec(&clustering_schema())).unwrap();

    let cancel = CancelFlag::new();
    let mut sink = CancelAfterFirstBatchSink {
        cancel: &cancel,
        rows: 0,
    };
    let _ = producer.produce_streaming_from_readers(
        readers,
        &cancel,
        &mut sink,
        &ScanProgress::default(),
        || {},
    );

    assert!(
        sink.rows <= BATCH * 4,
        "warm point read materialised {} rows before the cancel took effect — must \
         be bounded to ~batch_size, not the whole partition ({WIDTH})",
        sink.rows
    );
}

/// AC3 (warm full-scan, `producer_warm.rs` line 99). The warm reader-set
/// FULL-scan branch (no point predicate) must also be bounded: a cancel tripped
/// after the first batch stops the merge within ~`batch_size` rows. RED pre-fix:
/// the full-scan branch drove the buffered `drive_merge`.
#[test]
fn warm_full_scan_materialises_bounded_rows_not_whole_partition() {
    let (_temp, dir) = wide_partition();
    let schema = clustering_schema();
    let readers = warm_readers(&dir);
    assert!(!readers.is_empty(), "the fixture must ship a warm reader");
    // Default spec → no point route → the full-scan warm branch (line 99).
    let producer = MergeProducer::with_spec(schema, BATCH, ScanSpec::default()).unwrap();

    let cancel = CancelFlag::new();
    let mut sink = CancelAfterFirstBatchSink {
        cancel: &cancel,
        rows: 0,
    };
    let _ = producer.produce_streaming_from_readers(
        readers,
        &cancel,
        &mut sink,
        &ScanProgress::default(),
        || {},
    );

    assert!(
        sink.rows <= BATCH * 4,
        "warm full scan materialised {} rows before the cancel took effect — must \
         be bounded to ~batch_size, not the whole partition ({WIDTH})",
        sink.rows
    );
}

// ---- AC4: byte-identity — streaming vs buffered drive over the SAME point merger ----

/// AC4. Driving the SAME point merger through the buffered `drive_merge` and the
/// streaming `drive_merge_over` must yield byte-identical batches (same row
/// count, same batch chunking, same contents). This guards that the fix changes
/// only WHEN rows are materialised, never the output.
#[test]
fn point_read_streaming_is_byte_identical_to_buffered() {
    let (_temp, dir) = wide_partition();
    let schema = clustering_schema();
    let producer = MergeProducer::with_spec(schema.clone(), BATCH, point_spec(&schema)).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();

    let key = PartitionKey::single("pk", Value::Integer(1))
        .to_bytes(&schema)
        .expect("serialize pk");
    let label = AccessPath::StreamingPartitionLookup.label();

    let buffered = {
        let mut merger =
            build_single_partition_merger(paths.clone(), &[key.clone()], &schema, ScanCancel::default())
                .expect("build merger")
                .expect("a candidate holds the key");
        let mut batches = Vec::new();
        let mut sink = CollectSink(&mut batches);
        producer
            .drive_merge(&mut merger, &CancelFlag::new(), &mut sink, &ScanProgress::default(), label)
            .expect("buffered drive");
        batches
    };
    let streamed = {
        let mut merger =
            build_single_partition_merger(paths, &[key], &schema, ScanCancel::default())
                .expect("build merger")
                .expect("a candidate holds the key");
        let mut batches = Vec::new();
        let mut sink = CollectSink(&mut batches);
        producer
            .drive_merge_over(&mut merger, &CancelFlag::new(), &mut sink, &ScanProgress::default(), label)
            .expect("streaming drive");
        batches
    };

    assert_eq!(total_rows(&buffered), WIDTH, "the whole partition survives");
    assert_eq!(
        total_rows(&buffered),
        total_rows(&streamed),
        "streaming and buffered emit the same row count"
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

//! Parity + regression tests for Issue #827 (streaming compaction read path).
//!
//! **Problem**: the k-way merge producer (`merge::producer_thread`) read each
//! source via `iterate_all_partitions_for_compaction`, which fully materialised
//! the decompressed data section and parsed every entry into a `Vec` before any
//! entry entered the bounded channel. End-to-end peak memory therefore scaled
//! with total input size, not with a bounded read-ahead window.
//!
//! **Fix**: a sliding-window incremental stitch+parse
//! (`SSTableReader::stream_all_partitions_for_compaction`) driven by the bounded
//! one-partition parser
//! (`V5CompressedLegacyParser::parse_one_partition_with_timestamps`), plus a
//! streaming producer that forwards one entry at a time.
//!
//! **These tests** guard *correctness* of that refactor:
//!
//! 1. `parse_block_with_timestamps_emit` (always-on, via the streaming reader
//!    which uses the one-partition parser) returns exactly the same entries as
//!    the materialising `parse_block_with_timestamps` (via the Vec reader API),
//!    byte-for-byte (keys, values, timestamps, tombstones), across a
//!    multi-partition / multi-chunk SSTable.
//! 2. **chunk-straddle**: a partition larger than one 16 KiB compression chunk
//!    still parses identically through the sliding window (proves the window
//!    refills across chunk boundaries / the NeedMore logic).
//!
//! The fixtures are written in-test via the `WriteEngine` (V5CompressedLegacy
//! "nb" format — exactly what compaction reads), so the tests need no external
//! dataset corpus.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use tempfile::TempDir;

const KEYSPACE: &str = "issue827_ks";
const TABLE: &str = "items";

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, name: &str, payload: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Text(payload.to_string()),
        },
    ];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Find the single `-Data.db` file produced by a flush under `dir`.
fn find_data_db(dir: &std::path::Path) -> std::path::PathBuf {
    let table_dir = dir.join(KEYSPACE).join(TABLE);
    std::fs::read_dir(&table_dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .expect("a Data.db file should exist after flush")
}

/// Write `mutations` into one SSTable and return its Data.db path (kept alive by
/// the returned `TempDir`).
async fn write_sstable(mutations: Vec<Mutation>) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");
    for m in mutations {
        engine.write(m).expect("write row");
    }
    engine
        .flush()
        .await
        .expect("flush sstable")
        .expect("non-empty sstable");
    engine.close().await.expect("close engine");

    let path = find_data_db(&data_dir);
    (temp_dir, path)
}

/// A comparable snapshot of a compaction entry. Epic #899 widened the compaction
/// read contract from `(RowKey, Value, ts)` to per-element `CompactionRow`; the
/// streaming↔materialising parity gate compares whole `CompactionRow`s.
type EntrySnapshot = cqlite_core::storage::sstable::reader::CompactionRow;

async fn collect_vec(reader: &SSTableReader, schema: &TableSchema) -> Vec<EntrySnapshot> {
    reader
        .iterate_all_partitions_for_compaction(Some(schema))
        .await
        .expect("iterate_all_partitions_for_compaction")
}

async fn collect_stream(reader: &SSTableReader, schema: &TableSchema) -> Vec<EntrySnapshot> {
    let mut out: Vec<EntrySnapshot> = Vec::new();
    reader
        .stream_all_partitions_for_compaction(Some(schema), &ScanCancel::default(), None, |row| {
            out.push(row);
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .expect("stream_all_partitions_for_compaction");
    out
}

async fn open_reader(path: &std::path::Path) -> SSTableReader {
    let mut config = Config::default();
    // #591: never mmap (mirrors the producer thread).
    config.storage.use_mmap = false;
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("open reader")
}

/// Assert the streaming compaction read returns byte-for-byte the same entries
/// (order, keys, values, timestamps) as the materialising Vec read.
fn assert_parity(vec_entries: &[EntrySnapshot], stream_entries: &[EntrySnapshot], ctx: &str) {
    assert_eq!(
        stream_entries.len(),
        vec_entries.len(),
        "Issue #827 [{ctx}]: streaming read returned {} entries, materialising read returned {}",
        stream_entries.len(),
        vec_entries.len()
    );
    for (i, (got, want)) in stream_entries.iter().zip(vec_entries.iter()).enumerate() {
        assert_eq!(
            got.key, want.key,
            "Issue #827 [{ctx}]: entry {i} key mismatch"
        );
        assert_eq!(
            got.row_timestamp, want.row_timestamp,
            "Issue #827 [{ctx}]: entry {i} timestamp mismatch"
        );
        assert_eq!(
            got.row_data, want.row_data,
            "Issue #827 [{ctx}]: entry {i} row data mismatch"
        );
    }
}

/// Many small partitions spanning MULTIPLE 16 KiB compression chunks: the
/// streaming sliding-window read must equal the materialising Vec read exactly.
#[tokio::test]
async fn test_streaming_parity_multi_chunk() {
    let schema = make_schema();
    // ~600 partitions × (a ~64-byte payload) comfortably exceeds several 16 KiB
    // chunks once serialised, forcing the window to refill across chunk
    // boundaries.
    let mut mutations = Vec::new();
    for id in 0..600i32 {
        let payload = format!("payload-{id:08}-{}", "x".repeat(48));
        mutations.push(write_row(
            id,
            &format!("name-{id}"),
            &payload,
            1_000 + id as i64,
        ));
    }
    let (_tmp, data_path) = write_sstable(mutations).await;

    let reader = open_reader(&data_path).await;
    let vec_entries = collect_vec(&reader, &schema).await;
    assert!(
        vec_entries.len() >= 500,
        "Issue #827 precondition: expected many partitions, got {}",
        vec_entries.len()
    );
    let stream_entries = collect_stream(&reader, &schema).await;
    assert_parity(&vec_entries, &stream_entries, "multi_chunk");
}

/// A SINGLE partition whose serialised size exceeds one 16 KiB chunk: parity
/// must still hold, proving the bounded one-partition parser returns NeedMore
/// (not a premature end) until the whole partition has been stitched together.
#[tokio::test]
async fn test_streaming_parity_chunk_straddle_wide_partition() {
    let schema = make_schema();
    // One ~48 KiB text value (> 2 chunks) plus a few normal partitions around it.
    let big_payload = "Z".repeat(48 * 1024);
    let mutations = vec![
        write_row(1, "small-before", "p1", 2_001),
        write_row(2, "the-big-one", &big_payload, 2_002),
        write_row(3, "small-after", "p3", 2_003),
    ];
    let (_tmp, data_path) = write_sstable(mutations).await;

    let reader = open_reader(&data_path).await;
    let vec_entries = collect_vec(&reader, &schema).await;
    assert!(
        !vec_entries.is_empty(),
        "Issue #827 precondition: wide-partition fixture should yield rows"
    );
    // Sanity: the big partition's value really is large (so it straddles chunks).
    use cqlite_core::storage::sstable::reader::CompactionRowData;
    let has_big = vec_entries.iter().any(|row| match &row.row_data {
        CompactionRowData::Live { simple, .. } => simple
            .iter()
            .any(|c| matches!(&c.value, Value::Text(s) if s.len() >= 48 * 1024)),
        CompactionRowData::Tombstone { .. } => false,
        // Issue #933: range-tombstone markers carry no inline cell values.
        CompactionRowData::RangeMarker { .. } => false,
        // Issue #1072: partition tombstones carry no inline cell values.
        CompactionRowData::PartitionDelete { .. } => false,
    });
    assert!(
        has_big,
        "Issue #827 precondition: expected a partition with a >=48 KiB text value"
    );

    let stream_entries = collect_stream(&reader, &schema).await;
    assert_parity(&vec_entries, &stream_entries, "chunk_straddle");
}

/// Tombstone parity (#505/#533 invariant): a partition carrying a row tombstone
/// must stream byte-identically to the materialising read — same
/// `Value::Tombstone` and same deletion timestamp. This guards the tombstone-
/// shadowing semantics the merger relies on.
#[tokio::test]
async fn test_streaming_parity_with_row_tombstone() {
    let schema = make_schema();

    // A live partition, a row tombstone, and another live partition.
    let mutations = vec![
        write_row(10, "alive-1", "p", 4_001),
        // Row tombstone for id=11.
        Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(11)),
            None,
            vec![CellOperation::DeleteRow],
            4_002,
            None,
        ),
        write_row(12, "alive-2", "p", 4_003),
    ];
    let (_tmp, data_path) = write_sstable(mutations).await;

    let reader = open_reader(&data_path).await;
    let vec_entries = collect_vec(&reader, &schema).await;
    let stream_entries = collect_stream(&reader, &schema).await;

    // At least one tombstone must be present, so the parity is non-trivial.
    use cqlite_core::storage::sstable::reader::CompactionRowData;
    let tombstone_count = vec_entries
        .iter()
        .filter(|row| matches!(row.row_data, CompactionRowData::Tombstone { .. }))
        .count();
    assert!(
        tombstone_count >= 1,
        "Issue #827 precondition: expected a row tombstone in the fixture"
    );

    assert_parity(&vec_entries, &stream_entries, "row_tombstone");
}

// ===========================================================================
// Finding 1 (#827): multi-row partition straddling a chunk boundary.
//
// The bounded one-partition parser must NOT re-emit rows it already emitted
// when a partition is truncated mid-parse (NeedMore) after several rows. The
// fixtures above only use single-row partitions, so they cannot catch the
// duplicate-row regression. This uses a CLUSTERED schema: one partition with
// many clustering rows whose serialised size exceeds one 16 KiB compression
// chunk, forcing a mid-partition straddle AFTER several rows were parsed.
// ===========================================================================

const CLUSTERED_TABLE: &str = "events";

/// A partition-key + clustering-key schema, so a single partition can hold many
/// rows (one per clustering value).
fn make_clustered_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: CLUSTERED_TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_clustered_row(pk: i32, ck: i32, payload: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, CLUSTERED_TABLE);
    let partition = PartitionKey::single("pk", Value::Integer(pk));
    let clustering = ClusteringKey::single("ck", Value::Integer(ck));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload.to_string()),
    }];
    Mutation::new(table_id, partition, Some(clustering), ops, timestamp, None)
}

/// Write `mutations` into one SSTable under `CLUSTERED_TABLE` and return its
/// Data.db path (kept alive by the returned `TempDir`).
async fn write_clustered_sstable(mutations: Vec<Mutation>) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_clustered_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");
    for m in mutations {
        engine.write(m).expect("write row");
    }
    engine
        .flush()
        .await
        .expect("flush sstable")
        .expect("non-empty sstable");
    engine.close().await.expect("close engine");

    let table_dir = data_dir.join(KEYSPACE).join(CLUSTERED_TABLE);
    let path = std::fs::read_dir(&table_dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .expect("a Data.db file should exist after flush");
    (temp_dir, path)
}

/// Finding 1 regression: a SINGLE partition with MANY clustering rows whose
/// total serialised size exceeds one 16 KiB compression chunk. The window will
/// be truncated mid-partition AFTER several rows have already been parsed,
/// returning `NeedMore`. The streaming read MUST yield exactly the same entries
/// as the materialising Vec read — no row duplicated (the bug) and none dropped.
#[tokio::test]
async fn test_streaming_parity_multi_row_partition_chunk_straddle() {
    let schema = make_clustered_schema();

    // One partition, 200 clustering rows. Each row carries a ~512-byte payload,
    // so the partition serialises to well over 16 KiB (multiple chunks) and the
    // straddle happens after many rows were already emitted by the parser.
    const ROWS: i32 = 200;
    let payload = "Q".repeat(512);
    let mut mutations = Vec::new();
    for ck in 0..ROWS {
        mutations.push(write_clustered_row(7, ck, &payload, 5_000 + ck as i64));
    }
    let (_tmp, data_path) = write_clustered_sstable(mutations).await;

    let reader = open_reader(&data_path).await;
    let vec_entries = collect_vec(&reader, &schema).await;
    // Precondition: every clustering row is a distinct compaction entry, so the
    // single partition produced many entries.
    assert!(
        vec_entries.len() >= ROWS as usize,
        "Finding 1 precondition: expected >= {ROWS} entries from a multi-row \
         partition, got {} (did clustering rows collapse?)",
        vec_entries.len()
    );

    let stream_entries = collect_stream(&reader, &schema).await;

    // The core assertion: no duplicates and none dropped — exact parity.
    assert_eq!(
        stream_entries.len(),
        vec_entries.len(),
        "Finding 1: streaming read returned {} entries, materialising read \
         returned {} — a multi-row partition straddling a chunk boundary \
         re-emitted (duplicated) or dropped rows",
        stream_entries.len(),
        vec_entries.len()
    );

    // Stronger guard: the multiset of (key,value,ts) must match exactly. Sort
    // both so any duplicate/drop shows up as a mismatch regardless of ordering.
    let mut vec_sorted = vec_entries.clone();
    let mut stream_sorted = stream_entries.clone();
    let sort_key = |e: &EntrySnapshot| (e.key.0.clone(), e.row_timestamp);
    vec_sorted.sort_by_key(sort_key);
    stream_sorted.sort_by_key(sort_key);
    assert_eq!(
        stream_sorted, vec_sorted,
        "Finding 1: streaming entries differ from materialising entries for a \
         multi-row partition straddling a chunk boundary (duplicate/dropped rows)"
    );

    // Belt-and-suspenders: assert no exact-duplicate entry appears in the stream
    // that is not also duplicated in the Vec read (catches re-emission directly).
    let mut seen: HashMap<(Vec<u8>, i64), usize> = HashMap::new();
    for e in &stream_entries {
        *seen
            .entry((e.key.as_bytes().to_vec(), e.row_timestamp))
            .or_insert(0) += 1;
    }
    let mut want: HashMap<(Vec<u8>, i64), usize> = HashMap::new();
    for e in &vec_entries {
        *want
            .entry((e.key.as_bytes().to_vec(), e.row_timestamp))
            .or_insert(0) += 1;
    }
    assert_eq!(
        seen, want,
        "Finding 1: per-(key,ts) entry counts differ — duplicate rows emitted \
         across a chunk-straddling multi-row partition"
    );
}

/// Early-Break: returning `ControlFlow::Break` from the emit callback must stop
/// the streaming read promptly and yield exactly the prefix produced so far.
#[tokio::test]
async fn test_streaming_break_stops_early() {
    let schema = make_schema();
    let mut mutations = Vec::new();
    for id in 0..200i32 {
        mutations.push(write_row(id, &format!("n-{id}"), "p", 3_000 + id as i64));
    }
    let (_tmp, data_path) = write_sstable(mutations).await;

    let reader = open_reader(&data_path).await;

    let mut collected: Vec<Vec<u8>> = Vec::new();
    reader
        .stream_all_partitions_for_compaction(Some(&schema), &ScanCancel::default(), None, |row| {
            collected.push(row.key.as_bytes().to_vec());
            if collected.len() >= 5 {
                Ok(std::ops::ControlFlow::Break(()))
            } else {
                Ok(std::ops::ControlFlow::Continue(()))
            }
        })
        .await
        .expect("stream with early break");

    assert_eq!(
        collected.len(),
        5,
        "Issue #827: Break must stop the stream at exactly 5 entries, got {}",
        collected.len()
    );
}

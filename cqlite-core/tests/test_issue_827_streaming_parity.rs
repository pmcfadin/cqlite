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
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, RowKey};
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

/// A comparable snapshot of a compaction entry: (key bytes, value, timestamp).
type EntrySnapshot = (Vec<u8>, Value, i64);

async fn collect_vec(reader: &SSTableReader, schema: &TableSchema) -> Vec<EntrySnapshot> {
    reader
        .iterate_all_partitions_for_compaction(Some(schema))
        .await
        .expect("iterate_all_partitions_for_compaction")
        .into_iter()
        .map(|(k, v, ts)| (k.0, v, ts))
        .collect()
}

async fn collect_stream(reader: &SSTableReader, schema: &TableSchema) -> Vec<EntrySnapshot> {
    let mut out: Vec<EntrySnapshot> = Vec::new();
    reader
        .stream_all_partitions_for_compaction(Some(schema), |key: RowKey, value, ts| {
            out.push((key.0, value, ts));
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
        assert_eq!(got.0, want.0, "Issue #827 [{ctx}]: entry {i} key mismatch");
        assert_eq!(
            got.1, want.1,
            "Issue #827 [{ctx}]: entry {i} value mismatch"
        );
        assert_eq!(
            got.2, want.2,
            "Issue #827 [{ctx}]: entry {i} timestamp mismatch"
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
    let has_big = vec_entries.iter().any(|(_, v, _)| {
        if let Value::Map(entries) = v {
            entries
                .iter()
                .any(|(_, val)| matches!(val, Value::Text(s) if s.len() >= 48 * 1024))
        } else {
            false
        }
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
    let tombstone_count = vec_entries
        .iter()
        .filter(|(_, v, _)| matches!(v, Value::Tombstone(_)))
        .count();
    assert!(
        tombstone_count >= 1,
        "Issue #827 precondition: expected a row tombstone in the fixture"
    );

    assert_parity(&vec_entries, &stream_entries, "row_tombstone");
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
        .stream_all_partitions_for_compaction(Some(&schema), |key: RowKey, _v, _ts| {
            collected.push(key.0);
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

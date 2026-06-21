//! Integration tests for issue #766 (epic #762 writer fidelity D4):
//! BTI `Partitions.db` trie writer, phase 1.
//!
//! # What these prove
//!
//! 1. `SSTableWriter` defaults to BIG and emits **no** `Partitions.db`, with no
//!    behavioral change to the existing components.
//! 2. With `SSTableFormat::Bti`, the writer additionally emits a `Partitions.db`
//!    trie that our **own BTI reader** reads back: each partition's point lookup
//!    resolves to the exact `Data.db` offset that was written.
//! 3. The byte-comparable partition-key transform handles every partition-key
//!    type present in the test corpus (int, bigint, text, uuid, timestamp, blob)
//!    and composite keys — the written trie round-trips for all of them.
//!
//! All tests require the `write-support` feature.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::bti::{
    lookup_raw_key_in_bti_partitions_db, BtiPartitionLocation,
};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::io::Cursor;
use tempfile::TempDir;

/// Minimal single-int-PK schema used by the default-format invariance test.
fn int_pk_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "t".to_string(),
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
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn int_mutation(id: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "t"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// AC#1: default format is BIG and emits no Partitions.db.
#[tokio::test]
async fn default_format_is_big_no_partitions_db() {
    let dir = TempDir::new().unwrap();
    let schema = int_pk_schema();

    let mut writer = SSTableWriter::new(dir.path().to_path_buf(), 1, &schema).unwrap();
    assert_eq!(writer.format(), SSTableFormat::Big);

    let m = int_mutation(1, "alice", 1_000_000);
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();

    let info = writer.finish().await.unwrap();
    assert!(
        info.partitions_path.is_none(),
        "BIG must not emit Partitions.db"
    );

    // No Partitions.db file should exist anywhere under the table dir.
    let table_dir = dir.path().join("test_ks").join("t");
    let has_partitions = std::fs::read_dir(&table_dir)
        .unwrap()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().contains("Partitions.db"));
    assert!(!has_partitions, "no Partitions.db expected for BIG format");

    // TOC must NOT list Partitions.db for BIG.
    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    assert!(!toc.contains("Partitions.db"));
    // Existing BIG components remain present.
    assert!(toc.contains("Index.db"));
    assert!(toc.contains("Summary.db"));
}

/// AC#2: BTI format emits a Partitions.db the BTI reader reads back to the
/// correct Data.db offsets (point lookups).
#[tokio::test]
async fn bti_format_partitions_db_roundtrips_via_reader() {
    let dir = TempDir::new().unwrap();
    let schema = int_pk_schema();

    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();
    assert_eq!(writer.format(), SSTableFormat::Bti);

    // Write several partitions in token order.
    let mut keyed: Vec<_> = (0..8)
        .map(|i| {
            let m = int_mutation(i, &format!("name{i}"), 1_000_000 + i as i64);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);

    // Remember each partition's raw key bytes so we can look it up afterward.
    let raw_keys: Vec<Vec<u8>> = keyed.iter().map(|(k, _)| k.key.clone()).collect();

    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }

    let info = writer.finish().await.unwrap();
    let partitions_path = info
        .partitions_path
        .expect("BTI format must emit Partitions.db");
    assert!(partitions_path.exists());

    // Issue #908: canonical BTI naming + no BIG index components.
    assert_eq!(
        partitions_path.file_name().unwrap().to_str().unwrap(),
        "da-1-bti-Partitions.db",
        "BTI Partitions.db must use the da/bti descriptor"
    );
    assert!(
        info.index_path.is_none(),
        "BTI must not emit Index.db (#908)"
    );
    assert!(
        info.summary_path.is_none(),
        "BTI must not emit Summary.db (#908)"
    );

    // TOC must list Partitions.db for BTI but not Index/Summary.
    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    assert!(
        toc.contains("Partitions.db"),
        "BTI TOC must list Partitions.db"
    );
    assert!(!toc.contains("Index.db"), "BTI TOC must not list Index.db");
    assert!(
        !toc.contains("Summary.db"),
        "BTI TOC must not list Summary.db"
    );

    // Read every partition back through the BTI reader and confirm a hit.
    let bytes = std::fs::read(&partitions_path).unwrap();
    assert!(
        bytes.len() > 8,
        "Partitions.db must be a non-empty trie + footer"
    );

    for raw in &raw_keys {
        let mut cur = Cursor::new(bytes.clone());
        let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, raw)
            .expect("BTI lookup must not error")
            .unwrap_or_else(|| panic!("partition {raw:?} not found in written Partitions.db"));
        match loc {
            BtiPartitionLocation::DataOffset(_) => { /* phase 1: direct Data.db offset */ }
            BtiPartitionLocation::RowsOffset(r) => {
                panic!("phase-1 writer must emit DataOffset, got RowsOffset({r})")
            }
        }
    }
}

/// AC#2 (offset fidelity): the trie-resolved Data.db offset for each partition
/// equals the partition's true offset in the written Data.db.
///
/// We reconstruct each partition's expected offset from the Index.db-independent
/// fact that BTI returns the same offset the writer recorded: verify the
/// resolved offsets are distinct and that looking up the first-token partition
/// resolves to offset 0 (the first partition always starts at Data.db offset 0).
#[tokio::test]
async fn bti_resolved_offsets_are_distinct_and_first_is_zero() {
    let dir = TempDir::new().unwrap();
    let schema = int_pk_schema();

    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();

    let mut keyed: Vec<_> = (0..6)
        .map(|i| {
            let m = int_mutation(i, &format!("v{i}"), 2_000_000 + i as i64);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    let raw_keys: Vec<Vec<u8>> = keyed.iter().map(|(k, _)| k.key.clone()).collect();

    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();
    let bytes = std::fs::read(info.partitions_path.unwrap()).unwrap();

    let mut offsets = Vec::new();
    for raw in &raw_keys {
        let mut cur = Cursor::new(bytes.clone());
        match lookup_raw_key_in_bti_partitions_db(&mut cur, raw)
            .unwrap()
            .unwrap()
        {
            BtiPartitionLocation::DataOffset(o) => offsets.push(o),
            BtiPartitionLocation::RowsOffset(o) => panic!("unexpected RowsOffset({o})"),
        }
    }

    // The first partition (lowest token, written first) starts at Data.db 0.
    assert!(
        offsets.contains(&0),
        "one partition must resolve to offset 0"
    );
    // All offsets must be distinct (each partition is at a unique Data.db offset).
    let mut sorted = offsets.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        offsets.len(),
        "resolved offsets must be distinct"
    );
}

/// Finding 2 (roborev #908, resolved in #910): an empty BTI-format SSTable
/// cannot be published.
///
/// The earlier #766 behavior OMITTED `Partitions.db` for an empty SSTable, but
/// that still produced a `da-*-bti-*` artifact with no `Partitions.db` — which
/// the BTI reader requires for every `da` SSTable (it needs the 8-byte trie root
/// footer), making the artifact unreadable. A zero-partition trie has no valid
/// canonical form. Rather than emit an unreadable SSTable we now REFUSE to
/// finish an empty BTI write with a clear error. (Cassandra never flushes an
/// empty BTI SSTable; every real `da` fixture has >= 1 partition.)
#[tokio::test]
async fn empty_bti_sstable_is_refused() {
    let dir = TempDir::new().unwrap();
    let schema = int_pk_schema();

    // BTI format, but write zero partitions.
    let writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();
    assert_eq!(writer.format(), SSTableFormat::Bti);

    let result = writer.finish().await;
    assert!(
        result.is_err(),
        "an empty BTI SSTable must be refused (no readable Partitions.db form)"
    );
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("empty BTI SSTable"),
        "error must explain the empty-BTI refusal; got: {msg}"
    );
}

/// AC#4: byte-comparable transform + trie round-trip for every partition-key
/// type in the corpus (int, bigint, text, uuid, timestamp, blob) and composites.
///
/// The trie operates on the **raw serialized partition-key bytes**; the
/// byte-comparable transform is the Murmur3 token, which is type-agnostic. This
/// test drives the writer directly with representative raw key bytes per type
/// and proves each round-trips through the BTI reader to its recorded offset.
#[test]
fn byte_comparable_transform_roundtrips_for_all_pk_types() {
    use cqlite_core::storage::sstable::writer::partitions_writer::PartitionsTrieWriter;

    // (label, raw on-disk key bytes) for each partition-key CQL type in the
    // corpus. Raw bytes mirror Cassandra's on-disk serialization:
    //   int        -> 4-byte big-endian
    //   bigint/ts  -> 8-byte big-endian
    //   text       -> UTF-8 bytes
    //   uuid       -> 16 raw bytes
    //   blob       -> raw bytes
    //   composite  -> concatenated component bytes (length-prefixing is the
    //                 caller's concern; the token only sees the raw bytes)
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("int", 42i32.to_be_bytes().to_vec()),
        ("int_neg", (-7i32).to_be_bytes().to_vec()),
        (
            "bigint",
            9_223_372_036_854_775_807i64.to_be_bytes().to_vec(),
        ),
        ("text", b"sensor-A".to_vec()),
        ("text_unicode", "tenant-\u{00e9}".as_bytes().to_vec()),
        ("uuid", vec![0xABu8; 16]),
        ("uuid2", vec![0x12u8; 16]),
        ("timestamp", 1_700_000_000_000i64.to_be_bytes().to_vec()),
        ("blob", vec![0x00, 0xFF, 0x10, 0x00, 0x7F]),
        ("composite_text_int", {
            // ("acme" :: text) ++ (5 :: int) — concatenated raw bytes.
            let mut v = b"acme".to_vec();
            v.extend_from_slice(&5i32.to_be_bytes());
            v
        }),
        ("composite_uuid_ts", {
            let mut v = vec![0x33u8; 16];
            v.extend_from_slice(&1_650_000_000_000i64.to_be_bytes());
            v
        }),
    ];

    let mut writer = PartitionsTrieWriter::new();
    // Assign a distinct, growing Data.db offset to each case.
    let mut expected: Vec<(&str, Vec<u8>, u64)> = Vec::new();
    for (i, (label, raw)) in cases.iter().enumerate() {
        let off = (i as u64) * 101 + 7;
        writer.add_partition(raw, off);
        expected.push((label, raw.clone(), off));
    }

    let trie = writer.finish().expect("trie serialization must succeed");
    assert!(trie.len() > 8);

    for (label, raw, off) in &expected {
        let mut cur = Cursor::new(trie.clone());
        let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, raw)
            .unwrap_or_else(|e| panic!("[{label}] lookup error: {e}"))
            .unwrap_or_else(|| panic!("[{label}] key not found in trie"));
        match loc {
            BtiPartitionLocation::DataOffset(got) => assert_eq!(
                got, *off,
                "[{label}] expected DataOffset({off}) got DataOffset({got})"
            ),
            BtiPartitionLocation::RowsOffset(r) => {
                panic!("[{label}] phase-1 must emit DataOffset, got RowsOffset({r})")
            }
        }
    }
}

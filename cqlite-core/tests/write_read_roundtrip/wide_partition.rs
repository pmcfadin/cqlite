//! Wide Partition Promoted Index E2E Tests (Issue #752)
//!
//! Verifies that:
//! - Wide partitions (data > 64 KiB) produce a non-zero `promoted_index_len` in Index.db.
//! - Small partitions (data < 64 KiB) still produce `promoted_index_len = 0` (no regression).
//! - The written SSTable is readable back through SSTableManager.

#![cfg(feature = "write-support")]

use super::{create_clustered_mutation, create_clustering_schema};
use cqlite_core::platform::Platform;
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use std::sync::Arc;
use tempfile::TempDir;

// ─── helpers ────────────────────────────────────────────────────────────────

/// Decode one Cassandra unsigned VInt from the start of `bytes`.
/// Returns (value, bytes_consumed).
fn read_vuint(bytes: &[u8]) -> (u64, usize) {
    assert!(!bytes.is_empty(), "empty slice passed to read_vuint");
    let first = bytes[0];
    if first < 0x80 {
        return (first as u64, 1);
    }
    // Count leading ones in the first byte to determine extra bytes.
    let extra = first.leading_ones() as usize; // 1..=8
    assert!(extra <= 8, "malformed VUInt: too many leading ones");
    let mask = 0xFF_u8 >> extra; // bits in first byte that carry value
    let mut value = (first & mask) as u64;
    for &b in bytes[1..=extra].iter() {
        value = (value << 8) | b as u64;
    }
    (value, 1 + extra)
}

/// Read the promoted_index_len field for the **first** entry in a raw Index.db buffer.
///
/// Layout per entry: `[key_len: u16 BE][raw key bytes][data_offset: vuint][promoted_len: vuint]`.
fn first_entry_promoted_len(index_bytes: &[u8]) -> u64 {
    assert!(
        index_bytes.len() >= 2,
        "Index.db too short to contain key_len"
    );

    // key_len (u16 BE)
    let key_len = u16::from_be_bytes([index_bytes[0], index_bytes[1]]) as usize;
    let mut pos = 2 + key_len; // skip key_len + raw key bytes

    // data_offset (vuint)
    let (_, consumed) = read_vuint(&index_bytes[pos..]);
    pos += consumed;

    // promoted_len (vuint)
    let (promoted_len, _) = read_vuint(&index_bytes[pos..]);
    promoted_len
}

// ─── tests ───────────────────────────────────────────────────────────────────

/// Small partition (1 row, data << 64 KiB): promoted_index_len must be 0.
#[tokio::test]
async fn test_small_partition_no_promoted_index() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_clustering_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write a single row — nowhere near 64 KiB.
    let mutation = create_clustered_mutation(1, "ck_only", "a small value", 1_000_000);
    engine
        .write_async(mutation)
        .await
        .expect("Write should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    let index_bytes =
        std::fs::read(info.index_path.as_ref().unwrap()).expect("Should read Index.db");
    let promoted_len = first_entry_promoted_len(&index_bytes);
    assert_eq!(
        promoted_len, 0,
        "Small partition must have promoted_index_len=0, got {}",
        promoted_len
    );
}

/// Wide partition: write one partition key with enough clustered rows so that
/// the serialized uncompressed data exceeds 2 × 64 KiB = 128 KiB.  That
/// triggers at least 2 IndexInfo blocks, which is the gate for emitting the
/// promoted index in `IndexWriter::add_partition_with_promoted`.
///
/// Assertion: the first (and only) Index.db entry has `promoted_index_len > 0`.
#[tokio::test]
async fn test_wide_partition_emits_promoted_index() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_clustering_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Each row: ck ~8 bytes + data ~200 bytes → ~210 bytes/row.
    // 1 000 rows ≈ 210 KB >> 2 × 64 KiB (128 KiB).
    // All rows share the same partition key (pk=42) → wide single partition.
    const N_ROWS: usize = 1_000;
    // 190-byte padding to make each row's data field clearly over 200 bytes total.
    let padding: String = "x".repeat(190);
    for i in 0..N_ROWS {
        let ck = format!("ck_{:06}", i);
        let data = format!("data_{}_{}", i, padding);
        let mutation = create_clustered_mutation(42, &ck, &data, 1_000_000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // ── 1. Index.db must have non-zero promoted_index_len ──────────────────
    let index_bytes =
        std::fs::read(info.index_path.as_ref().unwrap()).expect("Should read Index.db");
    let promoted_len = first_entry_promoted_len(&index_bytes);
    assert!(
        promoted_len > 0,
        "Wide partition ({}B data file) must have promoted_index_len > 0, got 0. \
         Check that DataWriter::write_partition_with_index_blocks is crossing the 64 KiB boundary.",
        std::fs::metadata(&info.data_path)
            .map(|m| m.len())
            .unwrap_or(0)
    );

    // ── 2. Our own reader must be able to open the Index.db ────────────────
    let cqlite_config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&cqlite_config)
            .await
            .expect("Platform creation"),
    );
    let reader = cqlite_core::storage::sstable::index_reader::IndexReader::open(
        info.index_path.as_ref().unwrap(),
        platform,
    )
    .await
    .expect("IndexReader must open wide-partition Index.db without error");

    let entries = reader.get_partition_entries();
    assert_eq!(
        entries.len(),
        1,
        "Should have exactly 1 partition entry (pk=42), got {}",
        entries.len()
    );

    // ── 3. SSTableManager must scan back all rows ──────────────────────────
    let platform2 = Arc::new(
        Platform::new(&cqlite_config)
            .await
            .expect("Platform creation"),
    );
    let manager = cqlite_core::storage::sstable::SSTableManager::new(
        &temp_dir.path().join("data"),
        &cqlite_config,
        platform2,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager must load wide-partition SSTable");

    let table_id = cqlite_core::types::TableId::from("test_roundtrip.clustered");
    let results = manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("Scan must succeed on wide-partition SSTable");

    assert_eq!(
        results.len(),
        N_ROWS,
        "SSTableManager must read back all {} rows from the wide partition, got {}",
        N_ROWS,
        results.len()
    );
}

//! Issue #824 (Epic #817): lower-priority finding verification.
//!
//! Two independent findings are exercised here:
//!
//! **#12 — column subset (sparse rows).** When a row does not contain every
//! regular column, the `HAS_ALL_COLUMNS` (0x20) flag is cleared and a
//! columns-subset field is serialized. Cassandra's
//! `Columns.Serializer.serializeSubset` (`Columns.java:503-531`) selects the
//! encoding by superset size:
//!
//!   * `< 64` regular columns → a *single unsigned VInt* whose bit `i` is SET
//!     when column `i` is MISSING (value `0` is reserved and is only produced
//!     when every column is present — i.e. `HAS_ALL_COLUMNS` would normally be
//!     used instead).
//!   * `>= 64` regular columns → the *large-subset* encoding: an unsigned
//!     VInt count of missing columns, followed by the smaller of {present
//!     indices, missing indices} as unsigned VInt deltas.
//!
//! The `< 64` vs `>= 64` MODE SELECTION is decode-critical: a reader that
//! always treats the field as a single VInt mis-parses every `>= 64`-column
//! table and corrupts the row stream.  These tests pin the WRITTEN BYTES at
//! and around the boundary (63 / 64 / 65 columns), including tail-column
//! inclusion, and they decode the field exactly as Cassandra's deserializer
//! would.
//!
//! These are byte-level assertions (per Issue #719 / the Issue #717 lesson): a
//! pure CQLite round-trip can pass even when reader and writer are wrong in the
//! same way, so the canonical wire encoding is asserted directly.
//!
//! **#23 — AlwaysPresentFilter.** A table created with
//! `bloom_filter_fp_chance = 1.0` is backed by Cassandra's
//! `AlwaysPresentFilter`, which serializes NOTHING — the SSTable simply has no
//! `Filter.db` component. CQLite must read such a table without crashing and
//! must return correct rows (the bloom filter only ever skips lookups; absence
//! means "consult the data"). Both the scan path AND the point-lookup (`get`)
//! path must resolve the partition: an absent filter must never short-circuit a
//! lookup to `None`. The tests write a normal SSTable, remove its `Filter.db`
//! (and its TOC entry, reproducing the faithful always-present / absent-filter
//! case), then confirm the partition reads back correctly via both `scan` and
//! `get`.

#![cfg(feature = "write-support")]

use std::collections::HashMap;

use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{TableId as CqlTableId, Value};
use cqlite_core::{Config, RowKey};
use tempfile::TempDir;

const T0: i64 = 1_704_067_200_000_000; // 2024-01-01T00:00:00Z in µs

// Row flag bits (Cassandra UnfilteredSerializer).
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const END_OF_PARTITION: u8 = 0x01;

/// Build a table with `n` regular columns named `c00`, `c01`, ... (zero-padded
/// to 2 digits so lexicographic order == numeric index order). All columns are
/// simple `text` so the writer's column ordering (simple-before-complex, then by
/// name) is exactly the c00..c{n-1} index order used by the subset bitmap.
fn schema_with_n_columns(n: usize) -> TableSchema {
    let columns = (0..n)
        .map(|i| Column {
            name: format!("c{:02}", i),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();

    TableSchema {
        keyspace: "issue824".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![], // no clustering: row body starts right after flags
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

async fn flush_mutations(schema: &TableSchema, mutations: Vec<Mutation>) -> TempDir {
    let temp = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in mutations {
        engine.write_async(m).await.expect("write");
    }
    engine
        .flush()
        .await
        .expect("flush")
        .expect("flush produced an sstable");
    temp
}

/// Flush a single-partition mutation and return the raw Data.db bytes.
async fn flush_data_bytes(schema: &TableSchema, mutations: Vec<Mutation>) -> Vec<u8> {
    let temp = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in mutations {
        engine.write_async(m).await.expect("write");
    }
    let info = engine
        .flush()
        .await
        .expect("flush")
        .expect("flush produced an sstable");
    std::fs::read(&info.data_path).expect("read Data.db")
}

/// Read a Cassandra unsigned VInt; returns (value, new_pos).
///
/// The leading-ones count of the first byte gives the number of extension
/// bytes (0..=8). For `extra == 8` the first byte (0xFF) contributes no value
/// bits and the full 64-bit value lives in the 8 trailing bytes — this case is
/// reached by a bit-62 missing-column bitmap (value 1<<62).
fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
    let first = data[pos];
    let extra = first.leading_ones() as usize;
    assert!(extra <= 8, "unsigned vint has at most 8 extension bytes");
    let mut value = if extra >= 8 {
        0u64
    } else {
        (first as u64) & (0xFFu64 >> (extra + 1))
    };
    for i in 0..extra {
        value = (value << 8) | data[pos + 1 + i] as u64;
    }
    (value, pos + 1 + extra)
}

/// Build a mutation that writes a subset of the n columns. `present` is the set
/// of column indices (into c00..c{n-1}) to include.
fn write_subset(table_id: &TableId, pk: &PartitionKey, present: &[usize]) -> Mutation {
    let ops = present
        .iter()
        .map(|&i| CellOperation::Write {
            column: format!("c{:02}", i),
            value: Value::Text(format!("v{}", i)),
        })
        .collect();
    Mutation::new(table_id.clone(), pk.clone(), None, ops, T0, None)
}

/// Navigate a single-partition, single-row Data.db to the columns-subset field
/// and return (subset_field_start, pos_after_row_size, row_size, body_start).
///
/// Layout for a no-clustering regular row that lacks HAS_ALL_COLUMNS:
///   partition header: keylen(u8=4? no — VInt) ... we reuse the issue_717
///   constant layout: keylen(2 bytes total: u16) + int key(4) + LIVE del(12).
///
/// NOTE: partition key length in this writer is a 2-byte big-endian u16 prefix,
/// followed by the key bytes, then the partition-level LIVE deletion (1 byte
/// 0x80 is NOT used here; CQLite writes the 12-byte legacy deletion). We do not
/// assume the exact partition header size — instead we locate the row by
/// scanning for the row flags byte we expect.
struct RowNav {
    /// Position of the row flags byte.
    flags_pos: usize,
    /// Position of the columns-subset field (first byte).
    subset_pos: usize,
    /// row_size value (body length, includes prev_size VInt).
    row_size: u64,
    /// Position of the first body byte (the prev_size VInt).
    body_start: usize,
}

/// Locate the single regular row in a freshly written single-partition Data.db.
///
/// We know the writer emits, for an int PK with no clustering and a sparse row:
///   [2-byte keylen][4-byte int key][12-byte LIVE partition deletion]
///   [row flags: HAS_TIMESTAMP (0x04)]
///   [row_size VInt][prev_size VInt][ts_delta VInt][columns subset ...][cells]
///   [END_OF_PARTITION 0x01]
fn locate_sparse_row(data: &[u8]) -> RowNav {
    // Partition header: keylen(2) + int key(4) + LIVE deletion(12) — matches the
    // layout asserted in issue_717_row_tombstone_columns_subset.rs.
    let mut pos = 2 + 4 + 12;

    let flags_pos = pos;
    let flags = data[pos];
    pos += 1;
    let _ = flags;

    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "sparse live row must carry HAS_TIMESTAMP"
    );
    assert_eq!(
        flags & ROW_HAS_ALL_COLUMNS,
        0,
        "sparse row (a column missing) must NOT set HAS_ALL_COLUMNS"
    );

    // No clustering prefix (no clustering keys), so row_size is next.
    let (row_size, p) = read_vuint(data, pos);
    pos = p;
    let body_start = pos;

    // prev_size VInt
    let (_prev, p) = read_vuint(data, pos);
    pos = p;

    // timestamp delta VInt (HAS_TIMESTAMP)
    let (_ts_delta, p) = read_vuint(data, pos);
    pos = p;

    // columns subset begins here
    RowNav {
        flags_pos,
        subset_pos: pos,
        row_size,
        body_start,
    }
}

// ---------------------------------------------------------------------------
// #12 — column subset (sparse rows)
// ---------------------------------------------------------------------------

/// 63-column superset (< 64) → single-VInt bitmap, bit = MISSING.
/// One column (c62, the tail column) is omitted; its bit must be set.
#[tokio::test]
async fn subset_63_columns_uses_single_vint_bitmap_tail_missing() {
    let n = 63;
    let schema = schema_with_n_columns(n);
    let table_id = TableId::new("issue824", "t");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // Present: every column EXCEPT the tail (c62 missing).
    let present: Vec<usize> = (0..n - 1).collect();
    let data = flush_data_bytes(&schema, vec![write_subset(&table_id, &pk, &present)]).await;

    let nav = locate_sparse_row(&data);

    // < 64 columns: the subset is a single unsigned VInt bitmap. Exactly one bit
    // (index 62, the tail) is set → value 1<<62.
    let (subset, after) = read_vuint(&data, nav.subset_pos);
    assert_eq!(
        subset,
        1u64 << (n - 1),
        "63-col superset must encode missing tail column as single-VInt bitmap with bit 62 set"
    );

    // The row body must end exactly after the subset (no cell data for the one
    // missing column; the 62 present columns' cells follow). We don't assert the
    // cell bytes, only that the subset is one VInt and the stream is consistent.
    assert!(
        after <= nav.body_start + nav.row_size as usize,
        "subset must fit inside the declared row body"
    );

    // The whole partition must terminate with END_OF_PARTITION.
    let part_end = nav.body_start + nav.row_size as usize;
    assert_eq!(
        data[part_end], END_OF_PARTITION,
        "partition must end after the single declared row body"
    );
    assert_eq!(part_end + 1, data.len());
}

/// 64-column superset (== boundary) → LARGE-subset encoding: a VInt count of
/// missing columns, then index deltas (NOT a single bitmap VInt).
#[tokio::test]
async fn subset_64_columns_uses_large_subset_encoding() {
    let n = 64;
    let schema = schema_with_n_columns(n);
    let table_id = TableId::new("issue824", "t");
    let pk = PartitionKey::single("pk", Value::Integer(2));

    // Present: every column EXCEPT the tail (c63 missing) → 1 missing column.
    let present: Vec<usize> = (0..n - 1).collect();
    let data = flush_data_bytes(&schema, vec![write_subset(&table_id, &pk, &present)]).await;

    let nav = locate_sparse_row(&data);

    // Large subset: first a VInt = count of missing columns.
    let (missing_count, p) = read_vuint(&data, nav.subset_pos);
    assert_eq!(
        missing_count, 1,
        "64-col superset (>=64) must write a VInt count of missing columns first"
    );

    // present (63) >= columns/2 (32), so the writer serializes the *missing*
    // indices (the smaller set): a single VInt = 63 (index of tail c63).
    let (idx, _p2) = read_vuint(&data, p);
    assert_eq!(
        idx,
        (n - 1) as u64,
        "the missing tail column index (63) must follow the count as a VInt delta"
    );
}

/// 65-column superset (> 64) → large-subset encoding; here MOST columns are
/// missing, so the writer serializes the smaller PRESENT-index set instead.
#[tokio::test]
async fn subset_65_columns_large_subset_serializes_smaller_set() {
    let n = 65;
    let schema = schema_with_n_columns(n);
    let table_id = TableId::new("issue824", "t");
    let pk = PartitionKey::single("pk", Value::Integer(3));

    // Present: only the tail column c64. So 64 columns are MISSING, 1 present.
    // present (1) < columns/2 (32) → writer serializes the PRESENT indices.
    let present = vec![n - 1];
    let data = flush_data_bytes(&schema, vec![write_subset(&table_id, &pk, &present)]).await;

    let nav = locate_sparse_row(&data);

    // Large subset: VInt count of missing columns = 64.
    let (missing_count, p) = read_vuint(&data, nav.subset_pos);
    assert_eq!(
        missing_count,
        (n - 1) as u64,
        "65-col superset with one present column must report 64 missing"
    );

    // Then the smaller set (present indices): a single VInt = 64 (tail index).
    let (present_idx, _p2) = read_vuint(&data, p);
    assert_eq!(
        present_idx,
        (n - 1) as u64,
        "present tail index (64) must be serialized as the smaller index set"
    );
}

/// Round-trip control: a <64-column row with a missing MIDDLE column and a
/// missing TAIL column must encode both bits in the single-VInt bitmap.
#[tokio::test]
async fn subset_below_boundary_multiple_missing_including_tail() {
    let n = 10;
    let schema = schema_with_n_columns(n);
    let table_id = TableId::new("issue824", "t");
    let pk = PartitionKey::single("pk", Value::Integer(4));

    // Missing: c03 (middle) and c09 (tail). Present: everything else.
    let present: Vec<usize> = (0..n).filter(|&i| i != 3 && i != 9).collect();
    let data = flush_data_bytes(&schema, vec![write_subset(&table_id, &pk, &present)]).await;

    let nav = locate_sparse_row(&data);
    let (subset, _p) = read_vuint(&data, nav.subset_pos);

    let expected = (1u64 << 3) | (1u64 << 9);
    assert_eq!(
        subset, expected,
        "bitmap must set bit 3 (middle) and bit 9 (tail) for the two missing columns"
    );
    assert_ne!(subset & (1 << 9), 0, "tail column bit must be included");
    assert_eq!(subset & (1 << 0), 0, "present column 0 bit must be clear");
    let _ = nav.flags_pos;
}

/// DECODE-SIDE divergence guard for #12: CQLite's V5 reader parses the subset
/// field as a single unsigned VInt regardless of superset size (see
/// `reader/parsing/v5_compressed_legacy.rs::parse_row_metadata`, which calls
/// `parse_vuint` once and stores a `u64` bitmap). For a >=64-column superset the
/// on-disk field is `count + index-deltas`, so a single-VInt read consumes ONLY
/// the count and then mis-aligns on the following index VInt(s)/cells.
///
/// This test demonstrates the mismatch at the byte level: the value a single
/// VInt read would return (the missing-count) is NOT a valid missing-bitmap for
/// the >=64-column row, and there are additional subset bytes the single-VInt
/// reader would leave unconsumed (which it would then mis-read as cell data).
#[tokio::test]
async fn subset_ge64_single_vint_reader_would_misalign() {
    let n = 64;
    let schema = schema_with_n_columns(n);
    let table_id = TableId::new("issue824", "t");
    let pk = PartitionKey::single("pk", Value::Integer(5));

    // Two missing columns (c10 and c63) so the large subset is count=2 followed
    // by two index VInts — unambiguously more than one VInt of subset payload.
    let present: Vec<usize> = (0..n).filter(|&i| i != 10 && i != 63).collect();
    let data = flush_data_bytes(&schema, vec![write_subset(&table_id, &pk, &present)]).await;

    let nav = locate_sparse_row(&data);

    // Correct (large-subset) decode:
    let (missing_count, p_after_count) = read_vuint(&data, nav.subset_pos);
    assert_eq!(missing_count, 2, "expected 2 missing columns in the count");
    let (idx0, p1) = read_vuint(&data, p_after_count);
    let (idx1, p_after_indices) = read_vuint(&data, p1);
    assert_eq!(idx0, 10, "first missing index");
    assert_eq!(idx1, 63, "second missing index (tail)");

    // The full subset field spans from subset_pos to p_after_indices and is MORE
    // than one VInt long: a single-VInt reader (current CQLite reader) stops at
    // p_after_count, leaving the index VInts to be mis-read as cell data.
    assert!(
        p_after_indices > p_after_count,
        "the >=64 subset field has trailing index bytes a single-VInt reader would mis-parse \
         (current reader limitation; bit-only u64 bitmap cannot represent the large-subset form)"
    );
}

// ---------------------------------------------------------------------------
// #23 — AlwaysPresentFilter / absent Filter.db
// ---------------------------------------------------------------------------

/// An always-present bloom filter (`bloom_filter_fp_chance = 1.0`) is encoded by
/// Cassandra as NO Filter.db component at all. Removing Filter.db from an
/// otherwise valid SSTable reproduces that on-disk state. Reading the table must
/// not crash and must still return the written partition.
#[tokio::test]
async fn absent_filter_db_reads_without_crash() {
    let schema = schema_with_n_columns(3);
    let table_id = TableId::new("issue824", "t");
    let pk = PartitionKey::single("pk", Value::Integer(7));

    let temp = flush_mutations(&schema, vec![write_subset(&table_id, &pk, &[0, 1, 2])]).await;

    // Find the written SSTable directory and delete every Filter.db component,
    // simulating bloom_filter_fp_chance = 1.0 (AlwaysPresentFilter → no Filter.db).
    let data_root = temp.path().join("data");
    let mut filter_files = Vec::new();
    let mut data_files = Vec::new();
    for entry in walkdir(&data_root) {
        let name = entry.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with("-Filter.db") {
            filter_files.push(entry.clone());
        }
        if name.ends_with("-Data.db") {
            data_files.push(entry.clone());
        }
    }
    assert!(
        !filter_files.is_empty(),
        "writer should have produced at least one Filter.db to remove"
    );
    assert!(!data_files.is_empty(), "expected a Data.db to read back");

    for f in &filter_files {
        std::fs::remove_file(f).expect("remove Filter.db");
    }
    // Confirm the absent-filter state.
    for f in &filter_files {
        assert!(!f.exists(), "Filter.db must be gone");
    }
    // Faithfully simulate AlwaysPresentFilter (bloom_filter_fp_chance = 1.0): a
    // real Cassandra SSTable in that mode has NO Filter.db AND does not list it in
    // TOC.txt. Leaving a dangling TOC entry would be an inconsistency Cassandra
    // never produces, so strip the Filter.db line from every TOC.txt too.
    for entry in walkdir(&data_root) {
        let name = entry.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with("-TOC.txt") {
            let toc = std::fs::read_to_string(&entry).expect("read TOC.txt");
            let pruned: String = toc
                .lines()
                .filter(|l| l.trim() != "Filter.db")
                .map(|l| format!("{l}\n"))
                .collect();
            std::fs::write(&entry, pruned).expect("rewrite TOC.txt without Filter.db");
        }
    }

    // Read back through the REAL reader path (SSTableManager → scan), which runs
    // load_bloom_filter internally. With Filter.db absent it must return Ok(None)
    // for the filter, open without error, and the scan must still return the
    // written partition — exactly the AlwaysPresentFilter (bloom_filter_fp_chance
    // = 1.0) read scenario. A regression that errored on a missing Filter.db, or
    // failed scans without a bloom filter, would fail here.
    let cqlite_config = Config::default();
    let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
    let manager = SSTableManager::new(
        &data_root,
        &cqlite_config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager must open without error despite the missing Filter.db");

    let scan_table = CqlTableId::from("issue824.t");
    let results = manager
        .scan(&scan_table, None, None, None, Some(&schema))
        .await
        .expect("scan must succeed with Filter.db absent");

    let want_pk: Vec<u8> = 7_i32.to_be_bytes().into();
    assert!(
        results.iter().any(|(k, _)| k.as_bytes() == want_pk),
        "partition pk=7 must be returned by a scan even with Filter.db absent; \
         got {} row(s)",
        results.len()
    );

    // Point lookups must agree with scan when Filter.db is absent. An absent
    // Filter.db is the AlwaysPresentFilter case ("filter always says maybe"), so
    // `get` must NOT short-circuit to None: it consults the data exactly as the
    // scan does. The per-reader bloom gate (`data_access.rs`) correctly skips when
    // the filter is `None`, and the size=0 Index.db fallback routes `get` through
    // the same stitched-chunk scan that `scan` uses (#517), so both resolve pk=7.
    let got = manager
        .get(&scan_table, &RowKey::new(want_pk.clone()))
        .await
        .expect("get must succeed with Filter.db absent");
    assert!(
        got.is_some(),
        "get(pk=7) must return Some even with Filter.db absent (AlwaysPresentFilter)"
    );
}

/// **Point-lookup read robustness with an absent `Filter.db` (finding #23).**
///
/// `SSTableManager::scan` and `SSTableManager::get` must AGREE on which
/// partitions exist when `Filter.db` is absent (AlwaysPresentFilter,
/// `bloom_filter_fp_chance = 1.0`). An absent filter means "always maybe", so a
/// point lookup must consult the data rather than short-circuit to `None`.
///
/// Mechanically, three things make the `get` path resolve pk=7 here:
///   1. The per-reader bloom gate (`data_access.rs`) only runs `might_contain`
///      when `self.bloom_filter` is `Some`; an absent filter skips the gate
///      entirely, so the lookup is never falsely rejected.
///   2. The freshly written SSTable's `Index.db` reports `size = 0` for the
///      partition (Cassandra 5.0 style), so `get` falls back to `scan_for_key`
///      instead of seeking a bogus offset (`data_access.rs`).
///   3. `scan_for_key` reuses the same stitched-chunk parse that the full scan
///      uses for V5CompressedLegacy, giving `get`/`scan` a consistent view of
///      the data (#517). The match is therefore found regardless of Filter.db.
///
/// This is a positive regression test: a future change that re-introduced a
/// filter-absent short-circuit, or that broke get/scan parity on the size=0
/// fallback, would fail here.
#[tokio::test]
async fn point_lookup_without_filter_db_returns_row() {
    let schema = schema_with_n_columns(3);
    let table_id = TableId::new("issue824", "t");
    let pk = PartitionKey::single("pk", Value::Integer(7));
    let temp = flush_mutations(&schema, vec![write_subset(&table_id, &pk, &[0, 1, 2])]).await;
    let data_root = temp.path().join("data");

    // Remove Filter.db and its TOC entries (faithful AlwaysPresentFilter).
    for entry in walkdir(&data_root) {
        let name = entry.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with("-Filter.db") {
            std::fs::remove_file(&entry).expect("remove Filter.db");
        }
    }
    for entry in walkdir(&data_root) {
        let name = entry.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with("-TOC.txt") {
            let toc = std::fs::read_to_string(&entry).expect("read TOC.txt");
            let pruned: String = toc
                .lines()
                .filter(|l| l.trim() != "Filter.db")
                .map(|l| format!("{l}\n"))
                .collect();
            std::fs::write(&entry, pruned).expect("rewrite TOC.txt");
        }
    }

    let cfg = Config::default();
    let platform = Arc::new(Platform::new(&cfg).await.expect("platform"));
    let manager = SSTableManager::new(
        &data_root,
        &cfg,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("manager opens without Filter.db");

    let scan_table = CqlTableId::from("issue824.t");
    let want_pk: Vec<u8> = 7_i32.to_be_bytes().into();
    let got = manager
        .get(&scan_table, &RowKey::new(want_pk))
        .await
        .expect("get must not error");
    assert!(
        got.is_some(),
        "get(pk=7) must return Some even with Filter.db absent (AlwaysPresentFilter)"
    );
}

/// Minimal recursive directory walk (avoids adding a dev-dependency).
fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

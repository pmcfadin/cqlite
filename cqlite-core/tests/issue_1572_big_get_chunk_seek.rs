//! Issue #1572 (Epic C / C1): BIG ("nb") `get()` must resolve a partition via
//! the raw-key `Index.db` map and seek ONLY the covering chunk(s) — it must NOT
//! read + decompress the whole `Data.db` on every lookup via `scan_for_key`.
//!
//! # What these prove (red on `main`, green with the fix)
//! 1. `present_key_get_does_not_sequential_scan` — a `get()` for a present key on
//!    a MULTI-CHUNK BIG fixture leaves `SCAN_FOR_KEY_CALLS` unchanged (delta == 0).
//!    On `main` every BIG `get()` falls through to `scan_for_key` (delta == 1).
//! 2. `get_reads_bounded_chunks_not_whole_file` — a `get()` decompresses a BOUNDED
//!    number of chunks (the covering chunk, 1..=2), NOT O(file). Identical bounded
//!    work for a head-of-file and a deep (high-chunk) partition proves the read is
//!    chunk-targeted, not a whole-file stitch. On `main` the wired decompress site
//!    never runs (delta == 0), so the `>= 1` lower bound is red there too.
//! 3. `absent_key_returns_none_without_scan` — a key absent from the complete
//!    `Index.db` map returns `None` definitively, with no sequential scan.
//! 4. `get_value_matches_full_scan_oracle_for_all_keys` — for EVERY key in the
//!    fixture, the fixed `get()` returns byte-identical rows to the whole-file scan
//!    (the slow-but-correct oracle). This is the correctness guard on the new path.
//!
//! The `SCAN_FOR_KEY_CALLS` / decompress counters are process-global, so every
//! counter-observing test serializes on the `serial_test` mutex (the existing
//! counter-test convention). Tests SKIP (never fail) when the binary fixture is
//! absent; a present fixture that yields 0 rows stays a hard failure.
//!
//! ```bash
//! CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --test issue_1572_big_get_chunk_seek
//! ```

use cqlite_core::{
    storage::sstable::reader::SSTableReader, types::TableId, Config, RowKey, ScanRow,
};
use serial_test::serial;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const KEYSPACE: &str = "test_basic";
const TABLE: &str = "simple_table";

/// Locate the multi-chunk BIG `Data.db` for `test_basic/simple_table`, or `None`
/// when the binary fixture is absent (skip, not fail).
fn big_data_db() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = PathBuf::from(root).join("sstables").join(KEYSPACE);
    let table_dir = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .find(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(&format!("{TABLE}-"))
        })
        .map(|e| e.path())?;
    std::fs::read_dir(&table_dir).ok()?.flatten().find_map(|e| {
        let s = e.file_name().to_string_lossy().to_string();
        // `-Data.db` (never the `-Data.db.jsonl` golden).
        if s.ends_with("-Data.db") {
            Some(e.path())
        } else {
            None
        }
    })
}

async fn open_reader(data_db: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform::new"),
    );
    SSTableReader::open(data_db, &config, platform)
        .await
        .expect("SSTableReader::open must succeed for the BIG fixture")
}

fn table_id() -> TableId {
    TableId::from(format!("{KEYSPACE}.{TABLE}"))
}

/// Enumerate the fixture's partitions via a full scan (the slow-but-correct
/// oracle) keyed by raw partition-key bytes → first row for that key.
async fn scan_oracle(reader: &SSTableReader) -> HashMap<Vec<u8>, ScanRow> {
    let rows = reader
        .scan(&table_id(), None, None, None, None)
        .await
        .expect("full scan (oracle) must succeed");
    assert!(
        !rows.is_empty(),
        "fixture is present but the full scan returned 0 rows"
    );
    let mut map: HashMap<Vec<u8>, ScanRow> = HashMap::new();
    for (k, v) in rows {
        map.entry(k.as_bytes().to_vec()).or_insert(v);
    }
    map
}

/// The fixture must be genuinely MULTI-CHUNK for these tests to be meaningful.
fn assert_multi_chunk(reader: &SSTableReader) -> u64 {
    let comp = reader
        .compression_info
        .as_ref()
        .expect("BIG fixture is chunk-compressed (CompressionInfo.db present)");
    let n = comp.chunk_offsets.len();
    assert!(
        n > 1,
        "this test requires a multi-chunk fixture; {KEYSPACE}/{TABLE} has {n} chunk(s)"
    );
    comp.chunk_length as u64
}

// -------------------------------------------------------------------------
// Test 1: present-key get() does not sequential scan
// -------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn present_key_get_does_not_sequential_scan() {
    let Some(dd) = big_data_db() else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} BIG fixture not available");
        return;
    };
    let reader = open_reader(&dd).await;
    assert_multi_chunk(&reader);
    let oracle = scan_oracle(&reader).await;
    let present_key = oracle.keys().next().expect("at least one key").clone();

    // Sanity: the raw-key Index.db map resolves this present key (fast path arms).
    assert!(
        reader
            .lookup_partition_with_index(&present_key)
            .await
            .expect("index lookup must not error")
            .is_some(),
        "index_reader must resolve a present key for the fast path"
    );

    let before = SSTableReader::scan_for_key_call_count();
    let got = reader
        .get(&table_id(), &RowKey::new(present_key.clone()))
        .await
        .expect("get() must not error");
    let after = SSTableReader::scan_for_key_call_count();

    assert!(got.is_some(), "get() for a present key must return a row");
    assert_eq!(
        before, after,
        "BIG get() for a present key must NOT invoke scan_for_key \
         (whole-file decompress); count went {before} -> {after}"
    );
    eprintln!("present_key_get_does_not_sequential_scan PASSED (scan_for_key delta 0)");
}

// -------------------------------------------------------------------------
// Test 2: get() reads a bounded number of chunks, independent of file size
// -------------------------------------------------------------------------

/// Open a FRESH reader (cold chunk cache) and return the number of chunk
/// decompressions a single `get()` for `key` performs.
async fn decompress_cost_for(dd: &Path, key: &[u8]) -> u64 {
    let reader = open_reader(dd).await;
    SSTableReader::reset_decompress_calls();
    let got = reader
        .get(&table_id(), &RowKey::new(key.to_vec()))
        .await
        .expect("get() must not error");
    assert!(got.is_some(), "get() for a present key must return a row");
    SSTableReader::decompress_call_count()
}

#[tokio::test]
#[serial]
async fn get_reads_bounded_chunks_not_whole_file() {
    let Some(dd) = big_data_db() else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} BIG fixture not available");
        return;
    };
    let reader = open_reader(&dd).await;
    let chunk_len = assert_multi_chunk(&reader);
    let n_chunks = reader
        .compression_info
        .as_ref()
        .map(|c| c.chunk_offsets.len())
        .unwrap_or(0);
    let oracle = scan_oracle(&reader).await;

    // Pick the partition with the SMALLEST and the LARGEST uncompressed offset so
    // the two probes land in different (and, for the max, a deep) chunk.
    let mut min_key: Option<(u64, Vec<u8>)> = None;
    let mut max_key: Option<(u64, Vec<u8>)> = None;
    for k in oracle.keys() {
        if let Some((off, _)) = reader
            .lookup_partition_with_index(k)
            .await
            .expect("index lookup must not error")
        {
            if min_key.as_ref().is_none_or(|(m, _)| off < *m) {
                min_key = Some((off, k.clone()));
            }
            if max_key.as_ref().is_none_or(|(m, _)| off > *m) {
                max_key = Some((off, k.clone()));
            }
        }
    }
    let (min_off, head_key) = min_key.expect("a min-offset key");
    let (max_off, tail_key) = max_key.expect("a max-offset key");
    let tail_chunk = max_off / chunk_len;
    assert!(
        tail_chunk >= 1,
        "the deepest partition (offset {max_off}, chunk_len {chunk_len}) should be past chunk 0 \
         on a {n_chunks}-chunk fixture"
    );
    // The two probes must land in DIFFERENT chunks so neither reuses the other's
    // decompressed chunk (a same-chunk pair would just be a cache hit = 0).
    assert_ne!(
        min_off / chunk_len,
        max_off / chunk_len,
        "head/tail probes must be in different chunks"
    );

    let head_cost = decompress_cost_for(&dd, &head_key).await;
    let tail_cost = decompress_cost_for(&dd, &tail_key).await;

    // Bounded: one covering chunk (up to 2 if the partition straddles a boundary),
    // NEVER O(file) = n_chunks. The `>= 1` lower bound is what fails on `main`
    // (the whole-file stitch never touches the wired decompress site).
    for (label, cost) in [("head", head_cost), ("tail", tail_cost)] {
        assert!(
            (1..=2).contains(&cost),
            "{label} get() must decompress a bounded 1..=2 chunks (chunk-targeted), \
             got {cost} on a {n_chunks}-chunk fixture"
        );
        assert!(
            cost < n_chunks as u64,
            "{label} get() decompressed {cost} chunks — must be far below the \
             whole-file count {n_chunks}"
        );
    }
    assert_eq!(
        head_cost, tail_cost,
        "chunk-targeted work must be independent of the partition's file position \
         (head {head_cost} vs deep-tail {tail_cost})"
    );
    eprintln!(
        "get_reads_bounded_chunks_not_whole_file PASSED \
         (head={head_cost} tail={tail_cost} of {n_chunks} chunks)"
    );
}

// -------------------------------------------------------------------------
// Test 3: absent key returns None without a full scan
// -------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn absent_key_returns_none_without_scan() {
    let Some(dd) = big_data_db() else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} BIG fixture not available");
        return;
    };
    let reader = open_reader(&dd).await;
    assert_multi_chunk(&reader);
    let oracle = scan_oracle(&reader).await;

    // Synthesize a 16-byte key that is definitely NOT in the fixture.
    let absent = RowKey::new(vec![0xEEu8; 16]);
    assert!(
        !oracle.contains_key(absent.as_bytes()),
        "chosen absent key unexpectedly present"
    );
    // The complete Index.db map must not resolve it.
    assert!(
        reader
            .lookup_partition_with_index(absent.as_bytes())
            .await
            .expect("index lookup must not error")
            .is_none(),
        "absent key must miss the complete Index.db map"
    );

    let before = SSTableReader::scan_for_key_call_count();
    let got = reader
        .get(&table_id(), &absent)
        .await
        .expect("get() for an absent key must not error");
    let after = SSTableReader::scan_for_key_call_count();

    assert!(got.is_none(), "get() for an absent key must return None");
    assert_eq!(
        before, after,
        "an Index.db-definitive absent must not trigger a sequential scan; \
         count went {before} -> {after}"
    );
    eprintln!("absent_key_returns_none_without_scan PASSED");
}

// -------------------------------------------------------------------------
// Test 4: value parity vs the whole-file scan oracle for EVERY key
// -------------------------------------------------------------------------

#[tokio::test]
async fn get_value_matches_full_scan_oracle_for_all_keys() {
    let Some(dd) = big_data_db() else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} BIG fixture not available");
        return;
    };
    let reader = open_reader(&dd).await;
    assert_multi_chunk(&reader);
    let oracle = scan_oracle(&reader).await;

    let mut checked = 0usize;
    for (raw_key, expected) in &oracle {
        let got = reader
            .get(&table_id(), &RowKey::new(raw_key.clone()))
            .await
            .unwrap_or_else(|e| panic!("get() errored for key {raw_key:02x?}: {e}"));
        let got = got.unwrap_or_else(|| {
            panic!("get() returned None for a key the scan found: {raw_key:02x?}")
        });
        assert_eq!(
            &got, expected,
            "fixed get() row differs from the whole-file scan oracle for key {raw_key:02x?}"
        );
        checked += 1;
    }
    assert_eq!(
        checked,
        oracle.len(),
        "must have checked every partition key"
    );
    eprintln!("get_value_matches_full_scan_oracle_for_all_keys PASSED ({checked} keys)");
}

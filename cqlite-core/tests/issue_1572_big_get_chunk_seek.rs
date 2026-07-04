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

/// Return the fixture's chunk length when it is genuinely MULTI-CHUNK (so the
/// chunk-targeting test is meaningful), or `None` when the fixture is present
/// but single-chunk / uncompressed (no `CompressionInfo.db` or a single chunk).
///
/// Only `get_reads_bounded_chunks_not_whole_file` (the bytes-bounded micro-proof)
/// consults this — a present-but-single-chunk fixture is a LOUD warn-skip there,
/// NOT a hard failure. The scan-counter and oracle-parity proofs are decoupled
/// from chunk count and run on any present fixture (roborev #1572 Medium).
fn multi_chunk_len(reader: &SSTableReader) -> Option<u64> {
    let comp = reader.compression_info.as_ref()?;
    if comp.chunk_offsets.len() <= 1 {
        return None;
    }
    Some(comp.chunk_length as u64)
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
    // This proof (SCAN_FOR_KEY delta == 0 on a present key) is valid on a
    // single-chunk fixture too, so it must run whenever the fixture is PRESENT
    // regardless of chunk count — it may only SKIP when Data.db is ABSENT
    // (handled above). Decoupling the multi-chunk requirement from the core
    // correctness proof means a fixture regression to single-chunk can never
    // silently neuter the scan-counter guard (roborev #1572 Medium).
    let reader = open_reader(&dd).await;
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
    // This is the ONLY test that genuinely requires a MULTI-CHUNK fixture (it
    // proves a get() decompresses a bounded number of chunks, not O(file)). If
    // the fixture is PRESENT but single-chunk, the chunk-bound micro-proof cannot
    // run — but that non-execution must be LOUD so a fixture regression to
    // single-chunk can't quietly neuter the perf proof (roborev #1572 Medium).
    // (A fixture-ABSENT run above is a quiet skip; this is a present-but-degraded
    // warn-skip.)
    let Some(chunk_len) = multi_chunk_len(&reader) else {
        eprintln!(
            "WARNING: #1572 chunk-bound proof did not run — fixture present but single-chunk \
             ({KEYSPACE}/{TABLE}); the bytes-bounded chunk-targeting proof was SKIPPED"
        );
        return;
    };
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
    // The `(1..=2).contains(&cost)` bound alone proves chunk-targeting for ANY
    // `n_chunks >= 2` (a whole-file stitch would decompress `n_chunks`). We do NOT
    // additionally assert `cost < n_chunks`: on an exactly-2-chunk fixture a
    // partition straddling the boundary reads both chunks (`cost == 2`), a correct
    // chunk-targeted read that `2 < 2` would false-fail (roborev #1572 Low).
    for (label, cost) in [("head", head_cost), ("tail", tail_cost)] {
        assert!(
            (1..=2).contains(&cost),
            "{label} get() must decompress a bounded 1..=2 chunks (chunk-targeted), \
             got {cost} on a {n_chunks}-chunk fixture"
        );
    }
    // NOTE: we do NOT assert `head_cost == tail_cost`. Chunk-targeting is already
    // proven by the bounded `(1..=2).contains(&cost)` check above; a strict
    // equality would flake if exactly one probed partition straddles a chunk
    // boundary (cost 2) while the other does not (cost 1) — both are correct
    // chunk-targeted reads, just off by one covering chunk.
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
    // An Index.db-definitive absent (no sequential scan) holds on a single-chunk
    // fixture too, so this runs whenever the fixture is PRESENT; it only SKIPs
    // when Data.db is ABSENT (handled above) (roborev #1572 Medium).
    let reader = open_reader(&dd).await;
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
#[serial]
async fn get_value_matches_full_scan_oracle_for_all_keys() {
    let Some(dd) = big_data_db() else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} BIG fixture not available");
        return;
    };
    // The correctness guard: get() byte-parity vs the whole-file scan oracle for
    // EVERY key. Valid on a single-chunk fixture too, so it must run whenever the
    // fixture is PRESENT — decoupling it from the multi-chunk requirement means a
    // regression to a single-chunk fixture can never silently pass this proof
    // (roborev #1572 Medium). It SKIPs only when Data.db is ABSENT (handled above).
    let reader = open_reader(&dd).await;
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

// -------------------------------------------------------------------------
// Test 5 (roborev correctness regression): a TRUNCATED / partial Index.db
// must not turn an index MISS into a silent wrong-answer.
//
// `IndexReader::open` does NOT guarantee a complete partition map: entry
// parsing `break`s on the first unparseable entry and the leftover bytes are
// discarded, so a truncated Index.db opens successfully with a PARTIAL prefix
// map. The #1572 fast path treated an index miss as a definitive absent →
// a partition whose entry lies past the parse-stop point returned `None` from
// `get()` even though `scan()` still finds it in Data.db (get/scan divergence).
//
// The fix makes EVERY `index_reader` miss fall back to the whole-file scan
// oracle (the bloom pre-check remains the fast definitive-absent path for the
// common case). This test exercises the MID-ENTRY truncation case; the
// boundary-aligned case (whole trailing entries dropped) is covered by
// `boundary_truncated_index_get_falls_back_to_scan` below. Both build a degraded
// fixture by truncating a COPY of a real Index.db (never mutating the shared
// dataset) and assert every scan-visible key is still found by `get()`.
// -------------------------------------------------------------------------

/// Copy the full SSTable component set (every sibling of `data_db`, e.g.
/// `nb-1-big-*`) into `dst_dir`, returning the copied Data.db path.
fn copy_component_set(data_db: &Path, dst_dir: &Path) -> PathBuf {
    let src_dir = data_db.parent().expect("Data.db has a parent dir");
    let data_name = data_db
        .file_name()
        .expect("Data.db file name")
        .to_string_lossy()
        .to_string();
    // Component stem is everything up to `-Data.db` (e.g. `nb-1-big`).
    let stem = data_name
        .strip_suffix("-Data.db")
        .expect("Data.db name ends with -Data.db");
    for entry in std::fs::read_dir(src_dir)
        .expect("read source SSTable dir")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().to_string();
        // Copy this generation's real components, but NOT the `.jsonl` golden.
        if name.starts_with(&format!("{stem}-")) && !name.ends_with(".jsonl") {
            std::fs::copy(entry.path(), dst_dir.join(&name))
                .unwrap_or_else(|e| panic!("copy {name}: {e}"));
        }
    }
    dst_dir.join(&data_name)
}

#[tokio::test]
async fn truncated_index_get_falls_back_to_scan() {
    let Some(dd) = big_data_db() else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} BIG fixture not available");
        return;
    };

    // 1. Copy the component set to a temp dir so we never mutate the shared dataset.
    let tmp = tempfile::tempdir().expect("tempdir");
    let copied_data = copy_component_set(&dd, tmp.path());
    let index_copy = tmp.path().join(
        copied_data
            .file_name()
            .unwrap()
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );

    // 2. Truncate the Index.db copy MID-ENTRY: drop the tail so the final entry
    //    fails to parse. The parser stops early (partial prefix map, leftover
    //    bytes discarded); the token-tail partition(s) are lost from the map but
    //    still present in Data.db.
    let orig_len = std::fs::metadata(&index_copy)
        .expect("Index.db metadata")
        .len();
    assert!(
        orig_len > 32,
        "Index.db unexpectedly tiny ({orig_len} bytes)"
    );
    // Remove the last 8 bytes — cuts into the final entry's key so `take` fails,
    // and only the final entry is affected (earlier entries parse intact).
    let truncated_len = orig_len - 8;
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&index_copy)
        .expect("open Index.db copy for truncation");
    f.set_len(truncated_len).expect("truncate Index.db copy");
    drop(f);

    // 3. Open the reader over the DEGRADED copy and enumerate all keys via scan
    //    (the whole-file oracle is unaffected by the Index.db truncation).
    let reader = open_reader(&copied_data).await;
    let oracle = scan_oracle(&reader).await;

    // 4. Partition oracle keys by whether the truncated Index.db still resolves
    //    them. A partial map means at least one scan-visible key now MISSES the
    //    index — that is the key the #1572 regression would silently drop.
    let mut missing: Vec<Vec<u8>> = Vec::new();
    let mut resolvable = 0usize;
    for k in oracle.keys() {
        if reader
            .lookup_partition_with_index(k)
            .await
            .expect("index lookup must not error")
            .is_some()
        {
            resolvable += 1;
        } else {
            missing.push(k.clone());
        }
    }
    assert!(
        resolvable > 0,
        "truncation should leave a parsed prefix (some keys still resolvable)"
    );
    assert!(
        !missing.is_empty(),
        "truncation must drop at least one scan-visible key from the Index.db map \
         (else the degraded-input scenario is not exercised)"
    );

    // 5. Correctness: every scan-visible key whose Index.db entry was lost must
    //    STILL be found by get() (via the scan fallback), byte-identical to scan.
    //    Without the fix, get() returns None here → get/scan divergence.
    for raw_key in &missing {
        let got = reader
            .get(&table_id(), &RowKey::new(raw_key.clone()))
            .await
            .unwrap_or_else(|e| {
                panic!("get() errored for post-truncation key {raw_key:02x?}: {e}")
            });
        let got = got.unwrap_or_else(|| {
            panic!(
                "REGRESSION: get() returned None for key {raw_key:02x?} that scan() finds \
                 (partial Index.db miss treated as definitive absent)"
            )
        });
        let expected = oracle.get(raw_key).expect("oracle has the key");
        assert_eq!(
            &got, expected,
            "get() (scan fallback) row differs from the scan oracle for key {raw_key:02x?}"
        );
    }
    eprintln!(
        "truncated_index_get_falls_back_to_scan PASSED \
         ({} dropped keys recovered via scan fallback, {resolvable} still indexed)",
        missing.len()
    );
}

// -------------------------------------------------------------------------
// Test 6 (roborev correctness regression, boundary-aligned): the specific case
// the previous parse-consumption completeness signal could NOT detect.
//
// The pre-fix code marked the Index.db map "known-complete" whenever the entry
// parser consumed all bytes to EOF (empty leftover). That detects MID-ENTRY
// truncation (test 5) but NOT truncation aligned to an EXACT entry boundary:
// dropping WHOLE trailing entries leaves the surviving prefix parsing cleanly
// with NO leftover bytes, so the map looked "complete" while partitions were
// silently missing → an index miss for a dropped-but-scan-visible key returned
// `None` from `get()` (get/scan divergence).
//
// The fix removes the unreliable completeness signal entirely: EVERY index miss
// falls back to the whole-file scan oracle. This test truncates a COPY of a real
// Index.db at an EXACT entry boundary (drop the last whole entry, computed by
// walking the on-disk entry layout) and asserts every dropped-but-scan-visible
// key is still returned by `get()` byte-identical to `scan()`.
// FAILS without the fix (get() returns None for the dropped key), PASSES with it.
// -------------------------------------------------------------------------

/// Decode a Cassandra unsigned vint at `buf[pos..]`, returning
/// `(value, total_bytes_consumed)`, or `None` if the vint is truncated.
///
/// Encoding: the first byte's leading 1-bits give the number of EXTRA bytes;
/// the remaining low bits of the first byte are the value's high bits, followed
/// by the extra bytes big-endian (Cassandra `VIntCoding`).
fn read_unsigned_vint(buf: &[u8], pos: usize) -> Option<(u64, usize)> {
    let first = *buf.get(pos)?;
    let extra = first.leading_ones() as usize;
    if extra == 0 {
        return Some((first as u64, 1));
    }
    if pos + 1 + extra > buf.len() {
        return None;
    }
    // For extra >= 7 the first byte carries no value bits (all marker bits).
    let mask = if extra >= 7 {
        0u8
    } else {
        0xffu8 >> (extra + 1)
    };
    let mut val = (first & mask) as u64;
    for i in 0..extra {
        val = (val << 8) | buf[pos + 1 + i] as u64;
    }
    Some((val, 1 + extra))
}

/// Walk the headerless BIG Index.db entry layout from `start`, returning the byte
/// offset AFTER each complete entry (i.e. every exact entry boundary). Each entry
/// is `[key_len: u16 BE][key][data_offset: uvint][promoted_len: uvint][promoted]`.
///
/// Returns `None` when the bytes don't look like the expected headerless layout
/// (implausible key length) so the caller can SKIP rather than truncate blindly.
/// Real Cassandra 5.0 `nb` Index.db is headerless (starts with the u16 key_len).
fn entry_end_offsets(buf: &[u8]) -> Option<Vec<usize>> {
    let mut pos = 0usize;
    let mut ends = Vec::new();
    while pos + 2 <= buf.len() {
        let key_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        // Plausibility guard: a real partition key is 1..=4096 bytes. An
        // implausible length means this isn't the headerless entry layout.
        if key_len == 0 || key_len > 4096 {
            return None;
        }
        let mut p = pos + 2;
        if p + key_len > buf.len() {
            break; // partial trailing entry (key truncated)
        }
        p += key_len;
        // data_offset uvint
        let (_data_off, n1) = read_unsigned_vint(buf, p)?;
        p += n1;
        // promoted_index_len uvint + promoted_index_data
        let (promoted_len, n2) = read_unsigned_vint(buf, p)?;
        p += n2;
        let promoted_len = promoted_len as usize;
        if p + promoted_len > buf.len() {
            break; // partial trailing entry (promoted data truncated)
        }
        p += promoted_len;
        ends.push(p);
        pos = p;
    }
    Some(ends)
}

#[tokio::test]
async fn boundary_truncated_index_get_falls_back_to_scan() {
    let Some(dd) = big_data_db() else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} BIG fixture not available");
        return;
    };

    // 1. Copy the component set to a temp dir so we never mutate the shared dataset.
    let tmp = tempfile::tempdir().expect("tempdir");
    let copied_data = copy_component_set(&dd, tmp.path());
    let index_copy = tmp.path().join(
        copied_data
            .file_name()
            .unwrap()
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );

    // 2. Compute the EXACT entry boundaries of the real Index.db, then truncate
    //    at the boundary that drops the LAST WHOLE entry. The surviving prefix
    //    parses cleanly with NO leftover bytes — the boundary-aligned case the
    //    old parse-consumption signal could not detect.
    let index_bytes = std::fs::read(&index_copy).expect("read Index.db copy");
    let Some(ends) = entry_end_offsets(&index_bytes) else {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} Index.db is not the headerless BIG layout");
        return;
    };
    if ends.len() < 2 {
        eprintln!("SKIP: {KEYSPACE}/{TABLE} Index.db has <2 entries; cannot drop a whole entry");
        return;
    }
    // Drop the last whole entry: truncate at the boundary after the penultimate
    // entry. This is an EXACT boundary ⇒ the prefix parses cleanly to EOF.
    let truncated_len = ends[ends.len() - 2] as u64;
    let full_len = index_bytes.len() as u64;
    assert!(
        truncated_len < full_len,
        "boundary truncation must actually shorten the file ({truncated_len} < {full_len})"
    );
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&index_copy)
        .expect("open Index.db copy for truncation");
    f.set_len(truncated_len).expect("truncate Index.db copy");
    drop(f);

    // 3. Open the reader over the DEGRADED copy and enumerate all keys via scan
    //    (the whole-file oracle is unaffected by the Index.db truncation).
    let reader = open_reader(&copied_data).await;
    let oracle = scan_oracle(&reader).await;

    // 4. Partition oracle keys by whether the boundary-truncated Index.db still
    //    resolves them. Dropping whole trailing entries must leave at least one
    //    scan-visible key that now MISSES the index — the key the boundary-aligned
    //    #1572 regression would silently drop.
    let mut missing: Vec<Vec<u8>> = Vec::new();
    let mut resolvable = 0usize;
    for k in oracle.keys() {
        if reader
            .lookup_partition_with_index(k)
            .await
            .expect("index lookup must not error")
            .is_some()
        {
            resolvable += 1;
        } else {
            missing.push(k.clone());
        }
    }
    // Self-check that the boundary walk was correct: the surviving prefix must
    // still resolve a non-empty set of keys (a bad walk / wrong header assumption
    // would corrupt the prefix and resolve none).
    assert!(
        resolvable > 0,
        "boundary truncation should leave a clean parsed prefix (some keys still resolvable)"
    );
    assert!(
        !missing.is_empty(),
        "dropping a whole trailing entry must remove at least one scan-visible key \
         from the Index.db map (else the boundary-aligned scenario is not exercised)"
    );

    // 5. Correctness: every scan-visible key whose Index.db entry was dropped must
    //    STILL be found by get() (via the scan fallback), byte-identical to scan.
    //    Without the fix, the boundary-aligned truncation looked "complete" so the
    //    index miss returned None here → get/scan divergence.
    for raw_key in &missing {
        let got = reader
            .get(&table_id(), &RowKey::new(raw_key.clone()))
            .await
            .unwrap_or_else(|e| panic!("get() errored for dropped key {raw_key:02x?}: {e}"));
        let got = got.unwrap_or_else(|| {
            panic!(
                "REGRESSION: get() returned None for key {raw_key:02x?} that scan() finds \
                 (boundary-aligned Index.db miss treated as definitive absent)"
            )
        });
        let expected = oracle.get(raw_key).expect("oracle has the key");
        assert_eq!(
            &got, expected,
            "get() (scan fallback) row differs from the scan oracle for key {raw_key:02x?}"
        );
    }
    eprintln!(
        "boundary_truncated_index_get_falls_back_to_scan PASSED \
         ({} dropped keys recovered via scan fallback, {resolvable} still indexed)",
        missing.len()
    );
}

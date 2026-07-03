//! Integration tests for the shared decompressed-chunk cache (issue #1567,
//! Epic B/B1) wired into the production read path.
//!
//! These drive the two cache sites reachable through the public reader API — the
//! windowed streaming scan (`scan_stream`) and the BTI target-chunk point read
//! (`get`) — over REAL Cassandra 5.0 fixtures. The third site
//! (`get_cached_data`, BIG offset read) is reachable only via `read_value_at_offset`
//! with `pub(crate)` offset math, so it is proven in-crate
//! (`data_access::chunk_cache_wiring_tests`).
//!
//! Oracles (both asserted):
//!  * the reader's OWN cache hit/miss counters (`chunk_cache()`) — per-instance,
//!    so immune to test parallelism, and
//!  * the process-global `SSTableReader::decompress_call_count()` — reliable here
//!    because each `tests/` file is its own process and these tests are
//!    `#[serial]`, so no other test moves the counter during a measured window.
//!
//! Dataset tests SKIP (not fail) when the fixture is absent, but NEVER pass with
//! 0 rows when present (an explicit `rows > 0` guard).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::cache::DecompressedChunkCache;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::{RowKey, TableId};
use cqlite_core::{Config, Platform};

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

fn datasets_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = PathBuf::from(root);
        if p.is_dir() {
            return Some(p);
        }
    }
    None
}

fn data_db(ks: &str, tbl: &str) -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables").join(ks);
    let rd = std::fs::read_dir(&base).ok()?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?;
        if name.starts_with(&format!("{tbl}-")) {
            if let Ok(files) = std::fs::read_dir(entry.path()) {
                for f in files.flatten() {
                    let p = f.path();
                    if p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                    {
                        return Some(p);
                    }
                }
            }
        }
    }
    None
}

/// SKIP-or-fail on an absent fixture, honoring `CQLITE_REQUIRE_FIXTURES`.
fn resolve_or_skip(ks: &str, tbl: &str) -> Option<PathBuf> {
    match data_db(ks, tbl) {
        Some(p) => Some(p),
        None => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but {ks}.{tbl} Data.db is absent"
            );
            eprintln!("SKIP: {ks}.{tbl} fixture absent");
            None
        }
    }
}

async fn open(path: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("open fixture")
}

async fn open_with_cache(path: &Path, cache: Arc<DecompressedChunkCache>) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open_with_cache(path, &config, platform, cache)
        .await
        .expect("open fixture with shared cache")
}

async fn scan_stream_count(reader: &Arc<SSTableReader>, tid: &TableId) -> usize {
    let mut rx = Arc::clone(reader).scan_stream(tid.clone(), None, None, None, 64);
    let mut n = 0usize;
    while let Some(item) = rx.recv().await {
        item.expect("scan_stream item must be Ok");
        n += 1;
    }
    n
}

/// Task 1.6 (windowed-scan wiring) + 1.5 (zero-work repeat, decompress side): a
/// second full streaming scan of a compressed BIG (`nb`) table serves every chunk
/// from the shared cache — zero decompressions, all rows identical.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn windowed_scan_repeat_is_cached() {
    let Some(data_db) = resolve_or_skip("test_basic", "simple_table") else {
        return;
    };
    let reader = Arc::new(open(&data_db).await);
    let tid = TableId::new("test_basic.simple_table");

    // Cold scan: decompresses every chunk, populating the cache.
    let (h0, m0) = (reader.chunk_cache().hit_count(), reader.chunk_cache().miss_count());
    SSTableReader::reset_decompress_calls();
    let cold_rows = scan_stream_count(&reader, &tid).await;
    let cold_decompress = SSTableReader::decompress_call_count();
    let (h1, m1) = (reader.chunk_cache().hit_count(), reader.chunk_cache().miss_count());

    assert!(cold_rows > 0, "fixture present but scan returned 0 rows");
    assert!(
        cold_decompress > 0,
        "cold scan must decompress >=1 chunk (site wired?) — got {cold_decompress}"
    );
    assert!(m1 - m0 > 0, "cold scan must populate the cache (misses)");
    assert_eq!(h1 - h0, 0, "cold scan must not hit the cache");

    // Warm scan: every chunk resident → zero decompress, all hits.
    SSTableReader::reset_decompress_calls();
    let warm_rows = scan_stream_count(&reader, &tid).await;
    let warm_decompress = SSTableReader::decompress_call_count();
    let (h2, m2) = (reader.chunk_cache().hit_count(), reader.chunk_cache().miss_count());

    assert_eq!(warm_rows, cold_rows, "cache MUST NOT change the row set");
    assert_eq!(warm_decompress, 0, "warm scan must perform ZERO decompressions");
    assert_eq!(m2 - m1, 0, "warm scan must not miss the cache");
    assert!(h2 - h1 > 0, "warm scan must hit the cache");
}

/// Task 1.6 (BTI point-read wiring): the same BTI (`da`) partition point-read
/// twice serves the target chunk from the shared cache the second time (zero
/// decompress), identical result.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn bti_point_read_repeat_is_cached() {
    let Some(data_db) = resolve_or_skip("test_da", "simple_table") else {
        return;
    };
    let reader = open(&data_db).await;
    let tid = TableId::new("test_da.simple_table");

    // Learn a present key from a small scan.
    let rows = reader
        .scan(&tid, None, None, Some(1), None)
        .await
        .expect("scan for a key");
    let Some((k, _)) = rows.first() else {
        panic!("test_da.simple_table present but scan returned 0 rows");
    };
    let key = RowKey::from(k.clone());

    // Cold point read: BTI target chunk decompressed + cached.
    let (h0, m0) = (reader.chunk_cache().hit_count(), reader.chunk_cache().miss_count());
    SSTableReader::reset_decompress_calls();
    let cold = reader.get(&tid, &key).await.expect("cold BTI point read");
    let cold_decompress = SSTableReader::decompress_call_count();
    let (h1, m1) = (reader.chunk_cache().hit_count(), reader.chunk_cache().miss_count());

    assert!(cold.is_some(), "cold BTI point read must find the key");
    assert!(
        cold_decompress > 0,
        "cold BTI read must decompress the target chunk (site wired?)"
    );
    assert!(m1 - m0 > 0, "cold BTI read must populate the cache");
    assert_eq!(h1 - h0, 0, "cold BTI read must not hit the cache");

    // Warm point read: target chunk resident → zero decompress, cache hit.
    SSTableReader::reset_decompress_calls();
    let warm = reader.get(&tid, &key).await.expect("warm BTI point read");
    let warm_decompress = SSTableReader::decompress_call_count();
    let (h2, m2) = (reader.chunk_cache().hit_count(), reader.chunk_cache().miss_count());

    assert_eq!(
        format!("{cold:?}"),
        format!("{warm:?}"),
        "cache MUST NOT change the point-read result"
    );
    assert_eq!(warm_decompress, 0, "warm BTI read must perform ZERO decompressions");
    assert_eq!(m2 - m1, 0, "warm BTI read must not miss the cache");
    assert!(h2 - h1 > 0, "warm BTI read must hit the cache");
}

/// Task 1.7 (scan larger than cache): a table whose decompressed size exceeds a
/// small configured budget scans fully and correctly, with the cache's resident
/// bytes held within the budget throughout (eviction under scan pressure).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn scan_larger_than_cache_stays_bounded() {
    let Some(data_db) = resolve_or_skip("test_basic", "simple_table") else {
        return;
    };
    let tid = TableId::new("test_basic.simple_table");

    // Learn the full decompressed footprint with a generous cache.
    let big = Arc::new(open_with_cache(&data_db, Arc::new(DecompressedChunkCache::with_budget_bytes(256 * 1024 * 1024))).await);
    let full_rows = scan_stream_count(&big, &tid).await;
    let total_resident = big.chunk_cache().resident_bytes();
    assert!(full_rows > 0, "fixture present but scan returned 0 rows");
    assert!(
        total_resident > 0,
        "expected a compressed table with resident decompressed chunks"
    );

    // Budget = a quarter of the footprint: comfortably larger than one chunk
    // (so the single-oversized-entry exception never triggers) yet far smaller
    // than the whole table, forcing eviction during the scan.
    let budget = (total_resident / 4).max(1);
    let small_cache = Arc::new(DecompressedChunkCache::with_budget_and_shards(budget, 1));
    let bounded = Arc::new(open_with_cache(&data_db, small_cache.clone()).await);

    let bounded_rows = scan_stream_count(&bounded, &tid).await;
    assert_eq!(
        bounded_rows, full_rows,
        "scan larger than cache must still return ALL rows"
    );
    assert!(
        small_cache.resident_bytes() <= budget,
        "resident bytes {} must stay within budget {} under scan pressure",
        small_cache.resident_bytes(),
        budget
    );
    // Eviction actually ran (working set exceeded the budget).
    assert!(
        small_cache.miss_count() > small_cache.len() as u64,
        "eviction must have occurred (more misses than resident entries)"
    );
}

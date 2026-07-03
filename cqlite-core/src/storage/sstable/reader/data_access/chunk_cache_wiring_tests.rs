//! In-crate proof that the BIG point-read chunk fetch (`get_cached_data`) is
//! wired to the shared [`DecompressedChunkCache`] (issue #1567, Epic B/B1).
//!
//! `get_cached_data` is reached only through the `pub` offset-read wrapper
//! [`SSTableReader::read_value_at_offset`], whose valid `(offset, size)` is
//! derived here from the reader's private `actual_header_size` + `stats.file_size`
//! — so this proof lives in-crate rather than in `tests/` (the offset math needs
//! `pub(crate)` reader state). The other two wired sites (windowed scan, BTI) are
//! reachable through the public query API and are proven in
//! `tests/decompressed_chunk_cache_tests.rs`.
//!
//! The oracle is the reader's OWN cache hit/miss counters (per-instance →
//! immune to test parallelism) plus the process-global `CHUNK_READ_CALLS`
//! (`get_cached_data` is the only site that increments it, and no corpus fixture
//! reaches it via the public API, so its delta is reliable under `#[serial]`).
//! This exercises the R2 "zero-work repeat read" scenario: the warm read returns
//! the identical result with ZERO underlying reads and ZERO decompressions.

use crate::storage::sstable::reader::SSTableReader;
use crate::{Config, Platform};
use std::path::PathBuf;
use std::sync::Arc;

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
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"))?;
    fallback.is_dir().then_some(fallback)
}

/// Locate `test_basic.uncompressed_table`'s Data.db — an UNCOMPRESSED BIG (`nb`)
/// table, so `get_cached_data` reads raw bytes (no decompress) and the wiring
/// evidence is the `CHUNK_READ_CALLS` counter (reads skipped on a hit).
fn uncompressed_data_db() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables/test_basic");
    let rd = std::fs::read_dir(&base).ok()?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?;
        if name.starts_with("uncompressed_table-") {
            let dir = entry.path();
            if let Ok(files) = std::fs::read_dir(&dir) {
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

async fn open(path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("open uncompressed_table fixture")
}

/// Task 1.6 (BIG point-read wiring) + R2 (zero-work repeat read): calling
/// `read_value_at_offset` — which funnels through `get_cached_data` — twice at
/// the same offset serves the second read from the shared cache with ZERO
/// underlying reads and an identical result.
#[tokio::test]
#[serial_test::serial]
async fn big_point_read_get_cached_data_is_wired() {
    let Some(data_db) = uncompressed_data_db() else {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but test_basic.uncompressed_table is absent"
        );
        eprintln!(
            "SKIP: test_basic.uncompressed_table absent — cannot prove get_cached_data wiring"
        );
        return;
    };

    let reader = open(&data_db).await;
    // Valid offset/size for the first partition region: the whole data section.
    // `read_value_at_offset` verifies CRC over the covered chunks, reads the
    // bytes via `get_cached_data`, caches them by `block_offset`, then parses the
    // first partition. Deterministic across runs (immutable SSTable).
    let header = reader.actual_header_size as u64;
    let file_size = reader.stats.file_size;
    assert!(
        file_size > header,
        "fixture must have a non-empty data section (file_size={file_size}, header={header})"
    );
    let size = (file_size - header) as u32;
    assert!(size > 0);

    // Cold read: populates the cache (one miss, one underlying read).
    let (h0, m0) = (
        reader.chunk_cache().hit_count(),
        reader.chunk_cache().miss_count(),
    );
    SSTableReader::reset_chunk_read_calls();
    let cold = reader
        .read_value_at_offset(header, size)
        .await
        .expect("cold offset read");
    let cold_reads = SSTableReader::chunk_read_call_count();
    let (h1, m1) = (
        reader.chunk_cache().hit_count(),
        reader.chunk_cache().miss_count(),
    );
    assert_eq!(m1 - m0, 1, "cold read must be a cache MISS");
    assert_eq!(h1 - h0, 0, "cold read must not hit the cache");
    assert!(
        cold_reads >= 1,
        "cold read must perform >=1 underlying read"
    );

    // Warm read: identical offset → cache hit, ZERO underlying reads, ZERO
    // decompress (uncompressed table), identical result.
    SSTableReader::reset_chunk_read_calls();
    SSTableReader::reset_decompress_calls();
    let warm = reader
        .read_value_at_offset(header, size)
        .await
        .expect("warm offset read");
    let warm_reads = SSTableReader::chunk_read_call_count();
    let warm_decompress = SSTableReader::decompress_call_count();
    let (h2, m2) = (
        reader.chunk_cache().hit_count(),
        reader.chunk_cache().miss_count(),
    );

    assert_eq!(m2 - m1, 0, "warm read must NOT miss the cache");
    assert_eq!(h2 - h1, 1, "warm read must be a cache HIT");
    assert_eq!(
        warm_reads, 0,
        "warm read must perform ZERO underlying reads"
    );
    assert_eq!(
        warm_decompress, 0,
        "warm read must perform ZERO decompressions"
    );
    assert_eq!(
        format!("{cold:?}"),
        format!("{warm:?}"),
        "cache MUST NOT change the read result"
    );
}

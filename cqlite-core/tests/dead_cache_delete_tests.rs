//! Public-surface tests for issue #1568 (Epic B / B2 — dead-cache-delete).
//!
//! These prove the SPEC scenarios of `openspec/changes/dead-cache-delete`:
//!  * the retained `MemoryConfig.block_cache.max_size` knob is wired to the real
//!    B1 [`DecompressedChunkCache`] byte budget (capacity + eviction);
//!  * `Database::stats().memory_stats` reports the B1 cache's REAL hit/miss and
//!    occupancy numbers (a repeated cached read makes `block_cache_hit_rate()`
//!    non-zero instead of the pre-change structural `0.0`);
//!  * the `MemoryStats` semver shape (field names/types + `block_cache_hit_rate()`)
//!    is preserved.
//!
//! Dataset tests SKIP (not fail) when the fixture is absent — honoring
//! `CQLITE_REQUIRE_FIXTURES` / `CQLITE_PARITY_REQUIRE_DATASETS` — but NEVER pass
//! with 0 rows when the fixture is present.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::cache::DecompressedChunkCache;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::TableId;
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
    for entry in std::fs::read_dir(&base).ok()?.flatten() {
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

async fn open_reader_with_budget(path: &Path, budget_bytes: u64) -> SSTableReader {
    let mut config = Config::default();
    config.memory.block_cache.max_size = budget_bytes;
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("open fixture")
}

/// Open a reader sharing an explicitly-constructed cache, so a test can control
/// the shard count (eviction determinism), not just the byte budget.
async fn open_reader_with_cache(path: &Path, cache: Arc<DecompressedChunkCache>) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    SSTableReader::open_with_cache(path, &config, platform, cache)
        .await
        .expect("open fixture")
}

async fn scan_count(reader: &Arc<SSTableReader>, tid: &TableId) -> usize {
    let mut rx = Arc::clone(reader).scan_stream(tid.clone(), None, None, None, 64);
    let mut n = 0usize;
    while let Some(item) = rx.recv().await {
        item.expect("scan_stream item must be Ok");
        n += 1;
    }
    n
}

/// Spec: "A default-budget open uses the configured budget as the B1 capacity"
/// and the first clause of "Setting the budget knob changes B1 cache capacity" —
/// the live B1 cache's `budget_bytes()` equals the configured
/// `block_cache.max_size`, not an unrelated hard-coded constant.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn config_block_cache_max_size_is_the_b1_budget() {
    let Some(db) = resolve_or_skip("test_basic", "simple_table") else {
        return;
    };

    // Default open: the B1 capacity is the configured default budget (256 MiB),
    // not a hard-coded constant.
    let default_budget = Config::default().memory.block_cache.max_size;
    let reader = open_reader_with_budget(&db, default_budget).await;
    assert_eq!(
        reader.chunk_cache().budget_bytes() as u64,
        default_budget,
        "default open: B1 budget_bytes() must equal configured block_cache.max_size"
    );

    // A custom budget flows through to the B1 capacity. Both values are multiples
    // of DEFAULT_SHARDS (16), so the per-shard split reconstructs them exactly.
    for budget in [1u64 << 20, 8u64 << 20] {
        let reader = open_reader_with_budget(&db, budget).await;
        assert_eq!(
            reader.chunk_cache().budget_bytes() as u64,
            budget,
            "B1 budget_bytes() must equal the configured block_cache.max_size ({budget})"
        );
    }
}

/// Spec: "Setting the budget knob changes B1 cache capacity" — under a small
/// configured budget the cache evicts while a fixture is scanned, its
/// `resident_bytes()` stays within the configured budget, and the scan still
/// returns every row.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn small_config_budget_forces_eviction_and_bounds_residency() {
    let Some(db) = resolve_or_skip("test_basic", "simple_table") else {
        return;
    };
    let tid = TableId::new("test_basic.simple_table");

    // Learn the full decompressed footprint with a generous (default) budget.
    let big =
        Arc::new(open_reader_with_budget(&db, Config::default().memory.block_cache.max_size).await);
    let big_rows = scan_count(&big, &tid).await;
    let footprint = big.chunk_cache().resident_bytes();
    assert!(big_rows > 0, "fixture present but scan returned 0 rows");
    assert!(
        footprint > 0,
        "expected a compressed table with resident decompressed chunks"
    );
    let loaded_chunks = big.chunk_cache().len();
    assert!(loaded_chunks > 1, "fixture must span multiple chunks");

    // Budget = 3/5 of the footprint. Use a SINGLE-SHARD cache
    // (`with_budget_and_shards(_, 1)`) so the residency bound is exact and
    // deterministic. With the production 16-shard cache, each shard independently
    // retains up to one oversized entry (`insert`'s documented `len() > 1` guard),
    // so `resident_bytes()` can legitimately exceed the total `budget` — the old
    // `budget/16 > one chunk` reasoning did not hold for every fixture and made
    // the `<= budget` assertion flaky. One shard makes `budget_per_shard == budget`;
    // since `budget` (3/5 of a multi-chunk footprint) far exceeds any single chunk,
    // the single-oversized retention never triggers, so `resident_bytes() <= budget`
    // is a real, deterministic eviction invariant.
    let budget = (footprint as u64) * 3 / 5;
    assert!(budget > 0 && (budget as usize) < footprint);

    let cache = Arc::new(DecompressedChunkCache::with_budget_and_shards(
        budget as usize,
        1,
    ));
    let bounded = Arc::new(open_reader_with_cache(&db, cache).await);
    assert_eq!(bounded.chunk_cache().budget_bytes() as u64, budget);
    let bounded_rows = scan_count(&bounded, &tid).await;

    assert_eq!(
        bounded_rows, big_rows,
        "scan under a small budget must still return ALL rows"
    );
    assert!(
        bounded.chunk_cache().resident_bytes() as u64 <= budget,
        "resident bytes {} must stay within the configured budget {}",
        bounded.chunk_cache().resident_bytes(),
        budget
    );
    // Eviction actually ran: more chunks were loaded (misses) than remain resident.
    assert!(
        bounded.chunk_cache().miss_count() > bounded.chunk_cache().len() as u64,
        "eviction must have occurred (misses {} > resident {})",
        bounded.chunk_cache().miss_count(),
        bounded.chunk_cache().len()
    );
}

/// Spec: "Repeated cached read yields a non-zero reported hit rate" +
/// "Reported occupancy tracks real resident bytes" — open a multi-chunk fixture
/// through the PUBLIC `Database` API and issue the IDENTICAL point-lookup read
/// twice so the second is served from the shared B1 decompressed-chunk cache,
/// then assert `Database::stats().memory_stats` reflects the REAL B1 cache (hit
/// rate > 0.0, occupancy > 0). On pre-change code the hit rate is a structural
/// `0.0`.
///
/// A point lookup (`WHERE id = <uuid>`) is used because that read path routes
/// through the cache-consulting `get_cached_data` site; it is a public
/// `Database::execute` query end to end.
///
/// Gated on `cli-helpers`: the fixture is loaded through the public one-shot
/// ingestion API (`cqlite_core::ingestion::ingest`), which is `cli-helpers`-
/// gated. The full agent gate runs the `cli-helpers` tier.
#[cfg(feature = "cli-helpers")]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn stats_block_cache_hit_rate_and_occupancy_are_real() {
    let Some(_) = resolve_or_skip("test_basic", "simple_table") else {
        return;
    };
    let Some(db) = open_fixture_db(
        "test_basic",
        "simple_table",
        "basic-types.cql",
        Config::default(),
    )
    .await
    else {
        return;
    };

    // Learn a present partition key from a full scan (never a 0-row pass).
    let scan = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .expect("scan for a key");
    assert!(
        !scan.rows.is_empty(),
        "fixture present but scan returned 0 rows"
    );
    let id = scan
        .rows
        .iter()
        .find_map(|r| r.get("id").and_then(uuid_literal))
        .expect("a row with a UUID id");

    let q = format!("SELECT * FROM test_basic.simple_table WHERE id = {id}");
    let first = db.execute(&q).await.expect("cold point read");
    assert_eq!(first.rows.len(), 1, "point read must find exactly one row");
    // Identical read again → served from the shared B1 decompressed-chunk cache.
    let second = db.execute(&q).await.expect("warm point read");
    assert_eq!(second.rows.len(), 1, "repeat point read must be identical");

    let stats = db.stats().await.expect("stats");
    assert!(
        stats.memory_stats.block_cache_hit_rate() > 0.0,
        "repeat cached read must yield a real, non-zero block-cache hit rate \
         (pre-change code reports a structural 0.0); got {}",
        stats.memory_stats.block_cache_hit_rate()
    );
    assert!(
        stats.memory_stats.total_memory_used > 0,
        "reported occupancy must track the B1 cache's real resident bytes"
    );
}

/// Issue #1568 (roborev F1): `block_cache.enabled == false` must GENUINELY
/// disable caching, not be a decorative toggle. The contrast to
/// `stats_block_cache_hit_rate_and_occupancy_are_real` (which opens with the
/// default `enabled == true` and asserts hit rate > 0 / occupancy > 0): with
/// caching disabled the SAME repeated point read is served straight from disk,
/// so `Database::stats().memory_stats` reports a structural zero — hit rate
/// `== 0.0` AND `total_memory_used == 0` (the cache never populates). This is the
/// exact behavior Node's `cacheEnabled: false` maps to (→ `block_cache.enabled`).
#[cfg(feature = "cli-helpers")]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn stats_block_cache_disabled_yields_no_caching() {
    let Some(_) = resolve_or_skip("test_basic", "simple_table") else {
        return;
    };
    let mut config = Config::default();
    config.memory.block_cache.enabled = false;
    let Some(db) = open_fixture_db("test_basic", "simple_table", "basic-types.cql", config).await
    else {
        return;
    };

    // Learn a present partition key from a full scan (never a 0-row pass).
    let scan = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .expect("scan for a key");
    assert!(
        !scan.rows.is_empty(),
        "fixture present but scan returned 0 rows"
    );
    let id = scan
        .rows
        .iter()
        .find_map(|r| r.get("id").and_then(uuid_literal))
        .expect("a row with a UUID id");

    // The identical point read twice: with caching disabled the second read is
    // NOT served from a warm cache — it re-reads from disk. Reads still succeed.
    let q = format!("SELECT * FROM test_basic.simple_table WHERE id = {id}");
    let first = db.execute(&q).await.expect("first point read");
    assert_eq!(first.rows.len(), 1, "point read must find exactly one row");
    let second = db.execute(&q).await.expect("second point read");
    assert_eq!(second.rows.len(), 1, "repeat point read must be identical");

    let stats = db.stats().await.expect("stats");
    assert_eq!(
        stats.memory_stats.block_cache_hit_rate(),
        0.0,
        "disabled block cache must report a structural 0.0 hit rate (no caching), got {}",
        stats.memory_stats.block_cache_hit_rate()
    );
    assert_eq!(
        stats.memory_stats.total_memory_used, 0,
        "disabled block cache must never populate (reported occupancy stays 0)"
    );
}

/// Spec: "stats() surface shape is unchanged" — `MemoryStats` keeps its public
/// field names/types and the `block_cache_hit_rate()` accessor (semver). A
/// compile-time shape assertion: renaming/removing any field or the accessor
/// breaks this test.
#[test]
fn memory_stats_semver_shape_preserved() {
    let ms = cqlite_core::memory::MemoryStats::default();
    let _: u64 = ms.block_cache_hits;
    let _: u64 = ms.block_cache_misses;
    let _: u64 = ms.row_cache_hits;
    let _: u64 = ms.row_cache_misses;
    let _: usize = ms.total_memory_used;
    let _: u64 = ms.buffer_allocations;
    let _: u64 = ms.buffer_deallocations;
    let _: f64 = ms.block_cache_hit_rate();
    let _: f64 = ms.row_cache_hit_rate();
}

/// Open a queryable `Database` over one fixture table, isolated in a temp dir so
/// the shared corpus is never mutated. Uses the public one-shot ingestion API
/// (`cqlite_core::ingestion::ingest`) with the table's schema.
#[cfg(feature = "cli-helpers")]
async fn open_fixture_db(
    ks: &str,
    tbl: &str,
    schema_file: &str,
    core_config: Config,
) -> Option<cqlite_core::Database> {
    use cqlite_core::ingestion::{ingest, IngestionConfig};

    let root = datasets_root()?;
    let src = data_db(ks, tbl)?.parent()?.to_path_buf();
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let dst = tmp
        .path()
        .join(ks)
        .join(src.file_name().expect("fixture dir final component"));
    copy_dir(&src, &dst);
    // Leak the temp dir for the process lifetime: the Database keeps live file
    // handles into the copy and each scan opens its own handle (issue #815), so
    // reaping the dir mid-test would break reads.
    let _persisted = tmp.keep();

    let schema_path = root.join("../schemas").join(schema_file);
    let cfg = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: dst
            .parent()
            .and_then(|p| p.parent())
            .expect("temp/<ks>/<dir>")
            .to_path_buf(),
        version_hint: Some("5.0".to_string()),
        core_config,
        table_directory_filter: Some(format!("/{ks}/{tbl}")),
    };
    Some(ingest(cfg).await.expect("ingest fixture").database)
}

#[cfg(feature = "cli-helpers")]
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create dst dir");
    for entry in std::fs::read_dir(src).expect("read src dir").flatten() {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).expect("copy file");
        }
    }
}

#[cfg(feature = "cli-helpers")]
fn uuid_literal(v: &cqlite_core::types::Value) -> Option<String> {
    if let cqlite_core::types::Value::Uuid(b) = v {
        let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
        Some(format!(
            "{}-{}-{}-{}-{}",
            &h[0..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..32]
        ))
    } else {
        None
    }
}

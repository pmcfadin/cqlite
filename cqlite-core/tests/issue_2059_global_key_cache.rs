//! Issue #2059 — the process-global key→partition-offset cache work-probe.
//!
//! Spec `global-key-offset-cache` Requirement "A cache hit skips the Summary-guided
//! Index.db interval parse (post-#2412 work-probe)", scenario "A repeated present-key
//! point read hits and touches zero interval parses":
//!
//! Driven through the PUBLIC `SSTableReader::get` path on a real BIG fixture served
//! through the lazy Summary-guided path (#2412):
//! - The COLD (first) fetch of a present key is a cache MISS → reads exactly ONE
//!   bounded `Index.db` interval (`cqlite.sstable.index_interval_parses_total += 1`)
//!   and populates the global cache.
//! - The WARM (second) fetch of the SAME key is a cache HIT → resolves the partition
//!   location WITHOUT reading any interval (`index_interval_parses_total += 0`).
//! - Both fetches return the byte-identical partition (correctness preserved).
//!
//! Separate integration-test process (its OWN, fresh, process-global cache — no
//! cross-test pollution) + the OTel capture harness installs a process-global meter
//! provider, so this must not share the parallel `--lib` unit-test binary
//! (roborev #2163 / #2385 precedent).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test issue_2059_global_key_cache
//! ```

#![cfg(feature = "observability-testing")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::{RowKey, TableId};
use cqlite_core::Config;
use serial_test::serial;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate a BIG `*-Data.db` with sibling `*-Index.db` AND `*-Summary.db` (the lazy
/// Summary-guided path prerequisite).
fn find_big_data_file_with_summary(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let entries = std::fs::read_dir(root.join("sstables").join(keyspace)).ok()?;
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        let dir = e.path();
        let files = std::fs::read_dir(&dir).ok()?;
        let mut data_file: Option<PathBuf> = None;
        let (mut has_index, mut has_summary) = (false, false);
        for f in files.flatten() {
            let name = f.file_name().to_string_lossy().into_owned();
            if name.ends_with("-Data.db") {
                data_file = Some(f.path());
            } else if name.ends_with("-Index.db") {
                has_index = true;
            } else if name.ends_with("-Summary.db") {
                has_summary = true;
            }
        }
        if has_index && has_summary {
            if let Some(df) = data_file {
                return Some(df);
            }
        }
    }
    None
}

fn sibling(data_file: &Path, suffix: &str) -> PathBuf {
    let name = data_file.file_name().unwrap().to_string_lossy();
    let base = name.strip_suffix("-Data.db").unwrap();
    data_file.with_file_name(format!("{base}{suffix}"))
}

/// Recover the fixture's present raw partition keys via a HELPER eager Index.db
/// reader (distinct from the lazy reader under test).
fn present_raw_keys(
    data_file: &Path,
    platform: Arc<Platform>,
    rt: &tokio::runtime::Runtime,
) -> Vec<Vec<u8>> {
    let index_path = sibling(data_file, "-Index.db");
    let ir = rt
        .block_on(IndexReader::open(&index_path, platform))
        .expect("eager Index.db open for present-key harvest");
    ir.get_partition_entries()
        .iter()
        .map(|e| e.key_digest.to_vec())
        .collect()
}

/// Cold miss populates the global cache; warm hit resolves the partition with ZERO
/// interval parses; both fetches return the same present partition.
#[test]
#[serial]
fn cold_miss_populates_warm_hit_skips_interval_parse() {
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!("Skipping (#2059 work-probe): BIG test_basic/simple_table absent");
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    let keys = present_raw_keys(&data_file, platform.clone(), &rt);
    assert!(
        !keys.is_empty(),
        "fixture must expose present Index.db entries (0-rows-when-present is a failure)"
    );
    let present = keys[0].clone();

    let reader = rt
        .block_on(SSTableReader::open(&data_file, &config, platform))
        .expect("lazy BIG open");
    let table_id = TableId::from("test_basic.simple_table");

    // The key cache is PROCESS-GLOBAL, so a sibling serial test in this binary may
    // have populated this generation's entry already. Clear this generation's
    // entries so the "cold" read below is a genuine miss (fresh-slate for THIS
    // generation only — the identity is the same file across tests).
    reader.invalidate_key_cache_entries();

    // COLD read: miss → exactly one bounded interval parse → populate the cache.
    mc.reset();
    let cold = rt
        .block_on(reader.get(&table_id, &RowKey::new(present.clone())))
        .expect("cold point read must not error");
    let m_cold = mc.flush_and_collect();
    assert!(cold.is_some(), "a known-present key must resolve on the cold read");
    assert_eq!(
        m_cold.counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL),
        1.0,
        "the cold read (cache miss) must read EXACTLY ONE bounded Index.db interval"
    );

    // WARM read: hit → the interval parse is skipped entirely.
    mc.reset();
    let warm = rt
        .block_on(reader.get(&table_id, &RowKey::new(present.clone())))
        .expect("warm point read must not error");
    let m_warm = mc.flush_and_collect();
    assert!(warm.is_some(), "the same key must resolve on the warm read");
    assert_eq!(
        m_warm.counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL),
        0.0,
        "the warm read (cache HIT) must skip the Index.db interval parse entirely (0 parses)"
    );
    assert_eq!(
        m_warm.counter_sum(catalog::INDEX_PARSES_TOTAL),
        0.0,
        "the warm read must perform ZERO full Index.db parses"
    );

    // Correctness: cold and warm return the byte-identical partition.
    assert_eq!(
        cold, warm,
        "the warm cache-served read must return the byte-identical partition the cold read did"
    );
}

/// Issue #2059 §C — invalidating a generation drops its cached locations, so a
/// subsequent read of the SAME key must re-read the bounded `Index.db` interval
/// (proving the invalidation hook actually reclaims the entry end-to-end, not just
/// the counter). Spec Requirement "Entries are invalidated on generation removal".
#[test]
#[serial]
fn invalidation_forces_a_fresh_interval_parse() {
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!("Skipping (#2059 invalidation): BIG test_basic/simple_table absent");
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    let keys = present_raw_keys(&data_file, platform.clone(), &rt);
    assert!(!keys.is_empty(), "fixture must expose present Index.db entries");
    let present = keys[0].clone();

    let reader = rt
        .block_on(SSTableReader::open(&data_file, &config, platform))
        .expect("lazy BIG open");
    let table_id = TableId::from("test_basic.simple_table");

    // Fresh-slate this generation (process-global cache; see the sibling test).
    reader.invalidate_key_cache_entries();

    // Populate the cache (cold read).
    let _ = rt
        .block_on(reader.get(&table_id, &RowKey::new(present.clone())))
        .expect("cold read");

    // Warm read confirms the entry is resident (0 interval parses).
    mc.reset();
    let _ = rt
        .block_on(reader.get(&table_id, &RowKey::new(present.clone())))
        .expect("warm read");
    assert_eq!(
        mc.flush_and_collect()
            .counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL),
        0.0,
        "before invalidation the warm read is a cache hit (0 interval parses)"
    );

    // Invalidate this generation's entries (the removal/compaction/warm-evict hook).
    let dropped = reader.invalidate_key_cache_entries();
    assert!(dropped >= 1, "invalidation must drop the cached location");

    // The next read of the same key MISSES → must re-read exactly one interval.
    mc.reset();
    let after = rt
        .block_on(reader.get(&table_id, &RowKey::new(present.clone())))
        .expect("post-invalidation read");
    assert!(after.is_some(), "the key is still present on disk — it must still resolve");
    assert_eq!(
        mc.flush_and_collect()
            .counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL),
        1.0,
        "after invalidation the entry is gone, so the read re-parses one interval"
    );
}

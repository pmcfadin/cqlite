//! Issue #1585 (Epic E / E3): the compressed-chunk read path performs EXACTLY
//! one logical `read_exact` per chunk.
//!
//! Before E3 each compression chunk cost TWO `read_exact` calls — one for the
//! compressed payload, then a second for the trailing 4-byte CRC32. E3 folds
//! them into a single `read_exact` into one `payload+CRC` buffer and splits the
//! CRC off afterwards (the CRC is still verified BEFORE the payload is handed to
//! the decompressor — guardrail: unchanged CRC ordering). This test pins that
//! win via the `READ_CALLS` read-work counter (consumer E3).
//!
//! Oracle: on a cold single-generation full scan of a FULLY-COMPRESSIBLE fixture
//! every Data.db chunk is read once (`READ_CALLS`) and decompressed once
//! (`DECOMPRESS_CALLS`) — the cache is cold so there are no hits, and small text
//! rows never trip the incompressible raw-passthrough path — so
//! `read_calls == decompress_calls`. On the pre-E3 two-read path this same scan
//! recorded `read_calls == 2 * decompress_calls`, so the equality is a tight
//! RED→GREEN discriminator (RED = 2×).
//!
//! Compiled only with `--features work-counters` (the getters/`reset` and the
//! counter bodies live behind that feature; see `read_work_counters`). Requires
//! `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when the
//! fixture is absent, but NEVER passes with 0 rows when present. Excluded under
//! `tombstones` (that build serves reads via a full-scan filter rather than the
//! targeted chunk path this evidences).
//!
//! The counters are a shared process-global, so this test serializes on the
//! `serial_test` mutex (the existing counter-test convention) — a stale value
//! from a parallel test can never satisfy an assertion after a `reset`.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::Database;
use serial_test::serial;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(dir) = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        }) {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

/// True if `<datasets>/sstables/<keyspace>/<table>-*/` holds a `*-Data.db` file.
/// Skip keys off fixture presence (not a 0-row result), so a present fixture that
/// yields 0 rows stays a hard failure.
fn fixture_data_present(keyspace: &str, table: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(root.join("sstables").join(keyspace)) else {
        return false;
    };
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Data.db") {
                    return true;
                }
            }
        }
    }
    false
}

/// Ingest a single fixture table and return a fresh `Database`.
async fn setup(keyspace: &str, schema_file: &str) -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join(schema_file);
    if !schema_path.exists() {
        return None;
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return None;
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

/// Scenario: a cold full scan reads each compressed chunk exactly once. With the
/// cache cold (no hits) and the fixture fully compressible (no raw passthrough),
/// `READ_CALLS == DECOMPRESS_CALLS` — one read and one decompress per chunk. The
/// pre-E3 two-read path recorded `READ_CALLS == 2 * DECOMPRESS_CALLS`, so this
/// equality fails on the old path (RED) and holds after E3 (GREEN).
#[tokio::test]
#[serial]
async fn full_scan_reads_each_chunk_once() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (E3 read-op): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (E3 read-op): could not ingest test_basic");
        return;
    };

    // Reset BEFORE the scan so the entire chunk-read path is measured from zero.
    rwc::reset();
    assert_eq!(rwc::read_calls(), 0, "reset must zero READ_CALLS");
    assert_eq!(
        rwc::decompress_calls(),
        0,
        "reset must zero DECOMPRESS_CALLS"
    );

    let scan = db
        .execute("SELECT id FROM test_basic.simple_table")
        .await
        .expect("cold scan of test_basic.simple_table");
    assert!(
        !scan.rows.is_empty(),
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );

    let reads = rwc::read_calls();
    let decompresses = rwc::decompress_calls();
    assert!(
        reads >= 1 && decompresses >= 1,
        "E3: a compressed scan must record at least one read and one decompress; \
         reads={reads}, decompresses={decompresses}"
    );
    // The tight A5/E3 discriminator: exactly one read per decompressed chunk.
    // Pre-E3 this was `reads == 2 * decompresses`.
    assert_eq!(
        reads, decompresses,
        "E3: the chunk read path must issue exactly ONE read per chunk \
         (reads={reads}) — equal to the per-chunk decompress count \
         (decompresses={decompresses}); reads == 2*decompresses would mean the \
         pre-E3 separate payload+CRC reads regressed back in"
    );
}

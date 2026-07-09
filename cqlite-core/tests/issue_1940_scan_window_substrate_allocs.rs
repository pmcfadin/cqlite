//! Issue #1940 (Stage 2 / D2): the steady-state windowed scan allocates AT MOST
//! ONE buffer per decompressed chunk.
//!
//! ## What this guards
//!
//! Before D2 the windowed-scan hot seam allocated at least TWO buffers per chunk:
//! the IO half read the compressed `payload+CRC` into an owned `Vec<u8>` and moved
//! it onto the chunk channel (so it could never be reused as a scratch), the parse
//! half decompressed into a fresh `Vec<u8>`, and the B1 cache converted that `Vec`
//! into an `Arc<[u8]>` with `Arc::from(boxed_slice)` — a THIRD allocate + memcpy.
//!
//! D2 collapses the copy chain to one allocation per chunk:
//! - the B1 cache stores `bytes::Bytes`, so `insert` is a zero-copy `Bytes::from(Vec)`
//!   (it reuses the decompress-output `Vec`'s heap allocation, no third alloc);
//! - decompression happens in the IO half, so the compressed read buffer is a
//!   REUSED per-cursor scratch (`clear()` + refill, no per-chunk realloc);
//! - the ONE surviving allocation is the decompress-output buffer that becomes the
//!   refcounted `Bytes` substrate the window borrows.
//!
//! ## The measured invariant (RED on main ≥2, GREEN after D2 ≤1)
//!
//! `CHUNK_PATH_ALLOCS` (the `work-counters` per-chunk copy-chain allocation counter,
//! purpose-built for #1940) is incremented at EACH surviving copy-chain heap
//! allocation on the windowed-scan path. A steady-state (warmed) scan over a
//! multi-chunk compressed fixture must record `chunk_path_allocs <= decompress_calls`
//! (≤1 alloc per decompressed chunk). On `main` the compressed-read buffer + the
//! `Arc::from` cache copy made this ≥2 per chunk.
//!
//! We also assert the E3/A5 invariant is not regressed: exactly ONE read per chunk
//! (`read_calls == decompress_calls`), so the substrate change did not add a read.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` and
//! bodies live behind that feature). Requires `CQLITE_DATASETS_ROOT` + fetched
//! binaries; skips (never fails) when the fixture is absent, but NEVER passes with
//! 0 rows / 0 chunks when present. Excluded under `tombstones` (that build serves
//! reads via a full-scan filter rather than the windowed chunk path).
//!
//! The counters are a shared process-global, so this test serializes on the
//! `serial_test` mutex (the counter-test convention).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
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
    // Disable the B1 chunk cache so EVERY chunk read routes through the full
    // read → decompress copy chain deterministically. A warm cache serves hits as
    // refcount bumps (decompress_calls == 0), which would make the per-chunk decode
    // alloc bound vacuous; disabling it forces a real decompress per chunk on every
    // scan, so the ≤1-alloc-per-decompressed-chunk bound is measured against the
    // actual copy chain (read scratch + decompress output), the exact path D2 fixes.
    let mut core_config = cqlite_core::Config::default();
    core_config.memory.block_cache.enabled = false;
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: Some("5.0".to_string()),
        core_config,
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

/// Drain a full streaming scan to completion, returning the row count. A tiny
/// `buffer_size` keeps the window bounded (does not affect the per-chunk alloc
/// count, which is what we measure).
async fn drain_scan(db: &Database, sql: &str) -> usize {
    let config = StreamingConfig {
        buffer_size: 1,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(sql, config)
        .await
        .expect("execute_streaming should succeed");
    let mut n = 0usize;
    while let Some(row) = iter.next_async().await {
        row.expect("streamed row should be Ok");
        n += 1;
    }
    n
}

/// A multi-chunk (>1 CompressionInfo chunk), fully-compressed `nb` fixture, so the
/// windowed chunk-stitching scan path runs and decompresses more than one chunk.
const KEYSPACE: &str = "test_timeseries";
const TABLE: &str = "sensor_data";
const SCHEMA_FILE: &str = "time-series.cql";

#[tokio::test]
#[serial]
async fn steady_state_windowed_scan_allocs_at_most_one_per_chunk() {
    if !fixture_data_present(KEYSPACE, TABLE) {
        eprintln!(
            "Skipping (#1940 substrate allocs): {KEYSPACE}/{TABLE} Data.db not present \
             (run fetch-datasets.sh)"
        );
        return;
    }
    let Some(db) = setup(KEYSPACE, SCHEMA_FILE).await else {
        eprintln!("Skipping (#1940 substrate allocs): could not ingest {KEYSPACE}");
        return;
    };
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    // First scan warms the per-cursor read scratch high-water mark (the reused
    // compressed-read buffer grows to its peak once); the B1 cache is disabled, so
    // the MEASURED scan below still decompresses every chunk. This proves the
    // STEADY-STATE per-chunk allocation: the read scratch is reused (0 allocs after
    // warmup) and the single decompress-output buffer is the ≤1/chunk survivor.
    let warm = drain_scan(&db, &sql).await;
    assert!(
        warm > 0,
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );

    // Measured scan.
    rwc::reset();
    assert_eq!(rwc::chunk_path_allocs(), 0, "reset must zero CHUNK_PATH_ALLOCS");
    let rows = drain_scan(&db, &sql).await;
    assert_eq!(rows, warm, "steady-state scan must return the same rows");

    let allocs = rwc::chunk_path_allocs();
    let decompresses = rwc::decompress_calls();
    let reads = rwc::read_calls();
    eprintln!(
        "#1940 substrate: {rows} rows, decompresses={decompresses}, reads={reads}, \
         chunk_path_allocs={allocs}"
    );

    // Non-vacuous: the windowed chunk path must have run over MORE THAN ONE chunk
    // (otherwise a 1-chunk fixture makes ≤1/chunk trivially satisfiable).
    assert!(
        decompresses >= 2,
        "#1940: fixture must decompress >=2 chunks for the per-chunk alloc bound to \
         be non-vacuous (got {decompresses}); is {KEYSPACE}/{TABLE} still multi-chunk?"
    );

    // E3/A5 invariant not regressed: exactly one read per decompressed chunk.
    assert_eq!(
        reads, decompresses,
        "#1940: the substrate change must not regress the E3 one-read-per-chunk \
         invariant (reads={reads} decompresses={decompresses})"
    );

    // The load-bearing D2 assertion: at most ONE copy-chain heap allocation per
    // decompressed chunk. On `main` the compressed-read buffer + the `Arc::from`
    // cache copy made this >= 2 * decompresses.
    assert!(
        allocs <= decompresses,
        "#1940 REGRESSION: the windowed scan allocated {allocs} copy-chain buffers over \
         {decompresses} decompressed chunks (> 1 per chunk). The substrate must reuse the \
         compressed read scratch and flow the single decompress-output Vec into the B1 \
         cache as zero-copy Bytes — see design.md D2."
    );
}

//! Memory-bound regression test for Issue #790 (dhat-gated).
//!
//! **Problem**: `Database::execute_streaming` materialized the entire result set
//! in a `Vec` before the bounded channel saw a row, so peak live heap scaled
//! with row count (≈194 MB / 100k rows in the dhat profile), not with the
//! `buffer_size` read-ahead window.
//!
//! **Fix**: A lazy `scan_stream` (reader → manager → storage → executor) that
//! parses one entry at a time into a bounded channel. Live heap is now bounded
//! by `buffer_size`, independent of total rows.
//!
//! **This test** profiles a full streaming scan under the dhat heap profiler and
//! asserts the peak stays within the project's 128 MiB budget. It is gated on
//! the `dhat-heap` feature and runs in the profiling job (`scripts/profile.sh`),
//! not normal `cargo test`. On the small in-repo corpus the peak is a few MiB;
//! the discriminating 194 MB → bounded result is measured against the external
//! 100k-row perf corpus (cqlite-perf #5). The companion parity test
//! (`test_issue_790_streaming_parity.rs`) — which streams through a
//! `buffer_size = 1` channel — provides the always-on correctness guard.
//!
//! Run via:
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --package cqlite-core --features cli-helpers,dhat-heap \
//!   --test test_issue_790_streaming_memory --profile bench
//! ```
//!
//! **Requirements**: `CQLITE_DATASETS_ROOT`, real Data.db files.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "dhat-heap"
))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::Database;

// The dhat allocator must be the global allocator to observe every allocation.
// This test binary is separate from all others, so installing it here does not
// affect normal builds or other test binaries.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const HEAP_BUDGET_BYTES: usize = 128 * 1024 * 1024;

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        let schemas_dir = datasets_root.parent()?.join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
    }
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    schemas_dir.exists().then_some(schemas_dir)
}

fn data_files_present(keyspace: &str) -> bool {
    let Some(root) = get_datasets_root() else {
        return false;
    };
    let dir = root.join("sstables").join(keyspace);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return false;
    };
    for entry in entries.flatten() {
        if let Ok(files) = std::fs::read_dir(entry.path()) {
            for file in files.flatten() {
                if file
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with("-Data.db"))
                {
                    return true;
                }
            }
        }
    }
    false
}

async fn setup_db(schema_file: &str, keyspace: &str) -> Result<Database, String> {
    let datasets_root =
        get_datasets_root().ok_or_else(|| "CQLITE_DATASETS_ROOT not set".to_string())?;
    let schemas_dir = get_schemas_dir().ok_or_else(|| "schemas dir not found".to_string())?;
    let schema_path = schemas_dir.join(schema_file);
    if !schema_path.exists() {
        return Err(format!("schema not found at {:?}", schema_path));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{}/", keyspace)),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;
    Ok(result.database)
}

#[tokio::test]
async fn test_streaming_full_scan_stays_within_heap_budget() {
    if !data_files_present("test_collections") {
        eprintln!("Skipping: no Data.db files (run fetch-datasets.sh)");
        return;
    }

    // Start the profiler before the workload so all allocation is attributed.
    let _profiler = dhat::Profiler::builder().testing().build();

    let db = match setup_db("collections.cql", "test_collections").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: setup failed: {e}");
            return;
        }
    };

    // Stream the largest table with a small read-ahead window. A non-incremental
    // producer would materialize the whole result set here, so the peak would
    // scale with row count rather than `buffer_size`.
    let config = StreamingConfig {
        buffer_size: 8,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(
            "SELECT * FROM test_collections.large_collections_table",
            config,
        )
        .await
        .expect("execute_streaming should succeed");

    let mut rows = 0u64;
    while let Some(row) = iter.next_async().await {
        row.expect("streamed row should be Ok");
        rows += 1;
    }
    assert!(rows > 0, "expected rows from large_collections_table");

    let stats = dhat::HeapStats::get();
    eprintln!(
        "Issue #790 streaming full scan: {rows} rows, peak heap {} bytes ({:.2} MiB)",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0)
    );
    assert!(
        stats.max_bytes <= HEAP_BUDGET_BYTES,
        "Issue #790: streaming full-scan peak heap {} bytes exceeds the {} byte budget — \
         the read path may have regressed to materializing the whole result set",
        stats.max_bytes,
        HEAP_BUDGET_BYTES
    );
}

//! Issue #1593 (Epic F, F3): moving the mmap/`O_DIRECT` scan read off the async
//! worker pool is a SCHEDULING change, not a data change — it must not perturb a
//! single row.
//!
//! This parity guard scans the same real multi-chunk fixture twice — once via a
//! reader opened with the memory-mapped backend (`use_mmap = true`, the faulting
//! backend F3 now feeds on a `spawn_blocking` thread) and once via the default
//! buffered backend (fed inline on the async runtime) — and asserts the two full
//! streaming scans return the identical row set in the identical order.
//!
//! Skip-not-fail when the fixture Data.db is absent; a present fixture returning
//! zero rows is a FAILURE (never a vacuous pass).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::{Database, Value};

const KEYSPACE: &str = "test_wide_rows";
const TABLE: &str = "wide_partition_table";
const SCHEMA_FILE: &str = "wide-rows.cql";

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        if let Some(parent) = datasets_root.parent() {
            let schemas_dir = parent.join("schemas");
            if schemas_dir.exists() {
                return Some(schemas_dir);
            }
        }
    }
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    schemas_dir.exists().then_some(schemas_dir)
}

fn fixture_present() -> bool {
    let Some(root) = get_datasets_root() else {
        return false;
    };
    let table_root = root.join("sstables").join(KEYSPACE);
    let Ok(entries) = std::fs::read_dir(&table_root) else {
        return false;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{TABLE}-"))
            && entry.path().is_dir()
            && std::fs::read_dir(entry.path())
                .ok()
                .map(|mut d| {
                    d.any(|f| {
                        f.ok()
                            .and_then(|f| f.file_name().to_str().map(|n| n.ends_with("-Data.db")))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

async fn open_db(use_mmap: bool) -> Database {
    let datasets_root = get_datasets_root().expect("CQLITE_DATASETS_ROOT");
    let schemas_dir = get_schemas_dir().expect("schemas dir");
    let schema_path = schemas_dir.join(SCHEMA_FILE);
    assert!(schema_path.exists(), "schema not found: {schema_path:?}");

    let mut core_config = cqlite_core::Config::default();
    core_config.storage.use_mmap = use_mmap;

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    ingest(config)
        .await
        .expect("ingest wide_partition_table")
        .database
}

/// Collect the full streaming scan as an ordered list of `(partition_key, name)`
/// where `name` is the first text/uuid-ish identity column present. We compare
/// the *entire ordered projection dictionary* rather than a single column so the
/// parity is over the whole row, not a proxy.
async fn scan_rows(db: &Database, sql: &str) -> Vec<Vec<(String, String)>> {
    let config = StreamingConfig {
        buffer_size: 4,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(sql, config)
        .await
        .expect("execute_streaming");
    let mut rows = Vec::new();
    while let Some(row) = iter.next_async().await {
        let row = row.expect("streamed row Ok");
        let mut cols: Vec<(String, String)> = row
            .values
            .iter()
            .map(|(k, v)| (k.to_string(), format_value(v)))
            .collect();
        cols.sort_by(|a, b| a.0.cmp(&b.0));
        rows.push(cols);
    }
    rows
}

fn format_value(v: &Value) -> String {
    // A stable, total textual rendering sufficient for equality comparison.
    format!("{v:?}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mmap_and_buffered_scans_return_identical_rows() {
    if !fixture_present() {
        eprintln!("Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh).");
        return;
    }

    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    let buffered_db = open_db(false).await;
    let buffered_rows = scan_rows(&buffered_db, &sql).await;

    let mmap_db = open_db(true).await;
    let mmap_rows = scan_rows(&mmap_db, &sql).await;

    assert!(
        !buffered_rows.is_empty(),
        "Issue #1593: {KEYSPACE}.{TABLE} present but buffered scan returned 0 rows — vacuous"
    );
    assert_eq!(
        buffered_rows.len(),
        mmap_rows.len(),
        "Issue #1593: mmap scan row count {} != buffered {} — the F3 scheduling change altered \
         the emitted row set",
        mmap_rows.len(),
        buffered_rows.len()
    );
    assert_eq!(
        buffered_rows, mmap_rows,
        "Issue #1593: mmap scan produced a different ordered row set than the buffered scan — \
         the F3 scheduling change must be data-transparent"
    );
}

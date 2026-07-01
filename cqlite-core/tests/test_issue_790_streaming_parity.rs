//! Parity + regression test for Issue #790
//!
//! **Problem**: `Database::execute_streaming` was not incremental on the read
//! path. `execute_streaming_background` called `StorageEngine::scan`, which
//! materialized the *entire* result set into a `Vec` before the bounded channel
//! ever saw a row. Peak live heap therefore scaled with the number of result
//! rows (≈194 MB / 100k rows in the dhat profile), not with a bounded
//! read-ahead window — defeating the purpose of a streaming API.
//!
//! **Fix**: A lazy `scan_stream` threaded through reader → manager → storage →
//! executor. The reader parses one entry at a time into a bounded channel
//! (`blocking_send` backpressure), the manager k-way-merges per-SSTable streams,
//! and the executor forwards rows into the existing result channel. Live heap is
//! now bounded by `buffer_size`, independent of total rows.
//!
//! **This test** guards *correctness* of that refactor: the streaming path must
//! return exactly the same rows (keys + values), in the same order, as the
//! materializing `execute` path — across simple-type and collection-heavy
//! tables, and under several `buffer_size` values (including a tiny buffer that
//! forces the backpressure path). The companion dhat test
//! (`test_issue_790_streaming_memory.rs`) covers the memory bound.
//!
//! **Requirements**:
//! - `CQLITE_DATASETS_ROOT` pointing to `test-data/datasets`
//! - Real SSTable Data.db files (run `bash test-data/scripts/fetch-datasets.sh`)

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::HashMap;
use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::types::Value;
use cqlite_core::{Database, RowKey};

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
    if schemas_dir.exists() {
        return Some(schemas_dir);
    }
    None
}

/// True if at least one Data.db file exists under `sstables/<keyspace>/`.
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

/// Ingest a single keyspace using the given schema file.
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

/// A comparable, order-independent-within-a-row snapshot of a result row.
type RowSnapshot = (Vec<u8>, HashMap<std::sync::Arc<str>, Value>);

fn snapshot_key(key: &RowKey) -> Vec<u8> {
    key.as_bytes().to_vec()
}

/// Collect the materializing `execute` path into key-sorted snapshots.
async fn collect_execute(db: &Database, sql: &str) -> Vec<RowSnapshot> {
    let result = db.execute(sql).await.expect("execute should succeed");
    let mut rows: Vec<RowSnapshot> = result
        .rows
        .into_iter()
        .map(|r| (snapshot_key(&r.key), r.values))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Collect the streaming path into key-sorted snapshots.
async fn collect_streaming(db: &Database, sql: &str, buffer_size: usize) -> Vec<RowSnapshot> {
    let config = StreamingConfig {
        buffer_size,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(sql, config)
        .await
        .expect("execute_streaming should succeed");

    let mut rows = Vec::new();
    while let Some(row) = iter.next_async().await {
        let row = row.expect("streamed row should be Ok");
        rows.push((snapshot_key(&row.key), row.values));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Assert the streaming path returns exactly the materializing path's rows.
async fn assert_parity(schema_file: &str, keyspace: &str, table: &str) {
    if !data_files_present(keyspace) {
        eprintln!("Skipping {keyspace}.{table}: no Data.db files (run fetch-datasets.sh)");
        return;
    }
    let db = match setup_db(schema_file, keyspace).await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping {keyspace}.{table}: setup failed: {e}");
            return;
        }
    };

    let sql = format!("SELECT * FROM {keyspace}.{table}");
    let expected = collect_execute(&db, &sql).await;

    // Precondition: a meaningful number of rows, so parity is not trivially true.
    assert!(
        !expected.is_empty(),
        "Issue #790 precondition: {keyspace}.{table} should return rows"
    );

    // Compare across several buffer sizes, including buffer_size = 1 which forces
    // the per-row backpressure path (parser blocked between every entry).
    for buffer_size in [1usize, 8, 1024] {
        let streamed = collect_streaming(&db, &sql, buffer_size).await;

        assert_eq!(
            streamed.len(),
            expected.len(),
            "Issue #790: streaming '{sql}' (buffer_size={buffer_size}) returned {} rows, \
             materializing execute returned {}",
            streamed.len(),
            expected.len()
        );

        for (i, (got, want)) in streamed.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                got.0, want.0,
                "Issue #790: row {i} key mismatch (buffer_size={buffer_size}) for {keyspace}.{table}"
            );
            assert_eq!(
                got.1, want.1,
                "Issue #790: row {i} value mismatch (buffer_size={buffer_size}) for {keyspace}.{table}"
            );
        }
    }
}

/// Simple primitive types — exercises the per-row partition-value path.
#[tokio::test]
async fn test_streaming_parity_basic_simple_table() {
    assert_parity("basic-types.cql", "test_basic", "simple_table").await;
}

/// Collection-heavy table — exercises the cell-value parse path that dominated
/// the dhat profile (300k cell allocations live at peak before the fix).
#[tokio::test]
async fn test_streaming_parity_collection_table() {
    assert_parity("collections.cql", "test_collections", "collection_table").await;
}

/// Largest collection table available in the corpus — the closest in-repo proxy
/// for the wide/large result set that motivated the memory fix.
#[tokio::test]
async fn test_streaming_parity_large_collections_table() {
    assert_parity(
        "collections.cql",
        "test_collections",
        "large_collections_table",
    )
    .await;
}

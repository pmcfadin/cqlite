//! Regression test for Issue #581
//!
//! **Problem**: `Database::execute_streaming` ignored `LIMIT`. The streaming
//! background task (`execute_streaming_background`) handled the
//! `ExecutionStep::Limit` arm by only `log::debug!`-ing
//! "will be applied by consumer" and never enforcing it; the consumer iterator
//! did not apply it either. As a result `SELECT ... LIMIT N` streamed the entire
//! result set instead of `N` rows — a silent correctness bug (wrong row count,
//! no error).
//!
//! **Fix**: Enforce LIMIT/OFFSET in the producer (`execute_streaming_background`):
//! extract the limit bound up front, skip OFFSET matches, and stop sending /
//! return once `count` rows have gone through the channel so the producer stops
//! scanning early. This mirrors the non-streaming `execute_limit`
//! (drain OFFSET, then truncate to `count`).
//!
//! **Before the fix**: streaming `SELECT * ... LIMIT 10` against the ~1000-row
//! `test_basic.simple_table` yielded the full result set.
//! **After the fix**: it yields exactly 10.
//!
//! **OFFSET note**: CQL (like Cassandra) has no `OFFSET` surface syntax, so
//! `ExecutionStep::Limit.offset` is never populated from a query string. The
//! OFFSET branch of the fix is exercised by code review against `execute_limit`
//! rather than a SQL-level test; this file covers the reachable LIMIT path
//! through the public streaming API.
//!
//! **Requirements**:
//! - `CQLITE_DATASETS_ROOT` pointing to `test-data/datasets`
//! - Real SSTable Data.db files (run `bash test-data/scripts/fetch-datasets.sh`)

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::Database;

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

/// Returns true if at least one Data.db file exists for test_basic.
fn data_files_present() -> bool {
    let Some(root) = get_datasets_root() else {
        return false;
    };
    let dir = root.join("sstables").join("test_basic");
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

async fn setup_simple_table_db() -> Result<Database, String> {
    let datasets_root =
        get_datasets_root().ok_or_else(|| "CQLITE_DATASETS_ROOT not set".to_string())?;
    let schemas_dir = get_schemas_dir().ok_or_else(|| "schemas dir not found".to_string())?;

    let schema_path = schemas_dir.join("basic-types.cql");
    if !schema_path.exists() {
        return Err(format!("schema not found at {:?}", schema_path));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_basic/".to_string()),
    };

    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;
    Ok(result.database)
}

/// Stream a query and return the number of rows yielded.
async fn count_streamed(db: &Database, sql: &str) -> usize {
    let mut iter = db
        .execute_streaming(sql, StreamingConfig::default())
        .await
        .expect("execute_streaming should succeed");

    let mut count = 0usize;
    while let Some(row) = iter.next_async().await {
        row.expect("streamed row should be Ok");
        count += 1;
    }
    count
}

/// Core regression: streaming `LIMIT N` must yield exactly N rows, not the full
/// result set. Before the fix this returned all 999 rows.
#[tokio::test]
async fn test_issue_581_streaming_limit_is_enforced() {
    if !data_files_present() {
        eprintln!("Skipping: no Data.db files (run fetch-datasets.sh)");
        return;
    }

    let db = match setup_simple_table_db().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: setup failed: {}", e);
            return;
        }
    };

    // Sanity: the unbounded scan really does return many rows, so the LIMIT
    // assertions below are meaningful (not coincidentally equal to the total).
    // Derived dynamically so the test is robust to dataset row-count changes.
    let total = count_streamed(&db, "SELECT * FROM test_basic.simple_table").await;
    assert!(
        total > 50,
        "Issue #581 precondition: full streaming scan should return many rows \
         (need > 50 to make LIMIT meaningful), got {}",
        total
    );

    for limit in [1usize, 10, 50] {
        let sql = format!("SELECT * FROM test_basic.simple_table LIMIT {}", limit);
        let got = count_streamed(&db, &sql).await;
        assert_eq!(
            got, limit,
            "Issue #581: streaming '{}' must yield exactly {} rows, got {} \
             (before the fix the producer ignored LIMIT and streamed all {} rows)",
            sql, limit, got, total
        );
    }
}

/// A LIMIT larger than the result set must yield the whole set (no over-truncation).
#[tokio::test]
async fn test_issue_581_streaming_limit_above_total() {
    if !data_files_present() {
        eprintln!("Skipping: no Data.db files (run fetch-datasets.sh)");
        return;
    }

    let db = match setup_simple_table_db().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: setup failed: {}", e);
            return;
        }
    };

    let total = count_streamed(&db, "SELECT * FROM test_basic.simple_table").await;
    let got = count_streamed(&db, "SELECT * FROM test_basic.simple_table LIMIT 100000").await;
    assert_eq!(
        got, total,
        "Issue #581: LIMIT above the row count should yield all {} rows, got {}",
        total, got
    );
}

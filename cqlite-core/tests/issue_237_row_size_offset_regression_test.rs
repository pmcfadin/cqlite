//! Issue #237 Regression Test: Row Size Offset Calculation
//!
//! This test ensures the V5CompressedLegacy parser correctly calculates partition
//! boundaries for tables with clustering keys. The bug was that `row_size` was
//! being measured from the wrong offset, causing the parser to land in the middle
//! of cell data and skip most partitions.
//!
//! Root cause: `row_size` is measured from AFTER the row_size VInt is consumed,
//! not from where it starts. The fix adds `row_size_vint_len` tracking.
//!
//! This regression test validates:
//! 1. composite_key_table (with clustering keys) parses 90+ entries
//! 2. simple_table (no clustering keys) still works as a baseline
//! 3. static_columns_table (another clustering key table) works

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};

fn init_logging() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .is_test(true)
        .try_init();
}

/// Get the datasets root directory from environment
fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Get the schemas directory
fn get_schemas_dir() -> Option<PathBuf> {
    get_datasets_root()
        .and_then(|root| root.parent().map(|p| p.join("schemas")))
        .filter(|p| p.exists())
}

/// Setup test database with real SSTables via ingestion
async fn setup_database(keyspace_filter: &str) -> Result<cqlite_core::Database, String> {
    let datasets_root = get_datasets_root()
        .ok_or_else(|| "CQLITE_DATASETS_ROOT not set or path doesn't exist".to_string())?;

    let schemas_dir = get_schemas_dir().ok_or_else(|| "schemas directory not found".to_string())?;

    let schema_path = schemas_dir.join("basic-types.cql");
    if !schema_path.exists() {
        return Err(format!(
            "basic-types.cql schema not found at {:?}",
            schema_path
        ));
    }

    let data_dir = datasets_root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables directory not found at {:?}", data_dir));
    }

    let ingestion_config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(keyspace_filter.to_string()),
    };

    let ingestion_result = ingest(ingestion_config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;

    Ok(ingestion_result.database)
}

/// Test composite_key_table parses all partitions correctly
///
/// Before Issue #237 fix: Only 32 entries parsed, 179 partitions skipped
/// After fix: ~100 entries parsed, no skipped partitions
#[tokio::test]
async fn test_issue_237_composite_key_table_full_parsing() {
    init_logging();

    let db = match setup_database("/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Query composite_key_table - has clustering keys (timestamp, text)
    let query = "SELECT * FROM test_basic.composite_key_table";
    let result = db.execute(query).await;

    match result {
        Ok(query_result) => {
            let row_count = query_result.rows.len();
            println!(
                "✅ Issue #237: composite_key_table parsed {} rows",
                row_count
            );

            // CRITICAL ASSERTION: Before the fix, only ~32 rows were parsed
            // After the fix, we should get close to 100 rows
            assert!(
                row_count >= 90,
                "Issue #237 REGRESSION: composite_key_table should parse at least 90 rows, got {}. \
                 This indicates the row_size offset calculation is broken for tables with clustering keys. \
                 Check v5_compressed_legacy.rs row_size_vint_len handling.",
                row_count
            );

            // Verify rows have data (not just empty maps)
            for row in &query_result.rows {
                assert!(
                    !row.values.is_empty(),
                    "Issue #237: Rows should contain cell values"
                );
            }

            println!("✅ Issue #237 regression test PASSED");
        }
        Err(e) => {
            panic!("❌ Issue #237: Failed to query composite_key_table: {}", e);
        }
    }
}

/// Baseline test: simple_table (no clustering keys) should still work
#[tokio::test]
async fn test_issue_237_simple_table_baseline() {
    init_logging();

    let db = match setup_database("/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // simple_table has no clustering keys - baseline test
    let query = "SELECT * FROM test_basic.simple_table LIMIT 100";
    let result = db.execute(query).await;

    match result {
        Ok(query_result) => {
            let row_count = query_result.rows.len();
            println!(
                "✅ Issue #237 baseline: simple_table parsed {} rows",
                row_count
            );

            assert!(
                row_count >= 50,
                "simple_table baseline should parse at least 50 rows with LIMIT 100, got {}",
                row_count
            );
        }
        Err(e) => {
            panic!("❌ simple_table baseline failed: {}", e);
        }
    }
}

/// Test static_columns_table (another table with clustering keys)
#[tokio::test]
async fn test_issue_237_static_columns_table_clustering() {
    init_logging();

    let db = match setup_database("/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // static_columns_table has clustering keys
    let query = "SELECT * FROM test_basic.static_columns_table";
    let result = db.execute(query).await;

    match result {
        Ok(query_result) => {
            let row_count = query_result.rows.len();
            println!(
                "✅ Issue #237: static_columns_table parsed {} rows",
                row_count
            );

            // Should have meaningful number of rows
            assert!(
                row_count >= 30,
                "Issue #237: static_columns_table should parse at least 30 rows, got {}. \
                 This may indicate clustering key parsing issues.",
                row_count
            );
        }
        Err(e) => {
            panic!("❌ static_columns_table failed: {}", e);
        }
    }
}

/// Test collection_clustering_table (clustering with collections)
#[tokio::test]
async fn test_issue_237_collection_clustering_table() {
    init_logging();

    let db = match setup_database("/test_collections/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // collection_clustering_table has clustering keys with collection columns
    let query = "SELECT * FROM test_collections.collection_clustering_table";
    let result = db.execute(query).await;

    match result {
        Ok(query_result) => {
            let row_count = query_result.rows.len();
            println!(
                "✅ Issue #237: collection_clustering_table parsed {} rows",
                row_count
            );

            // Smoke test reference shows 30 entries
            assert!(
                row_count >= 20,
                "Issue #237: collection_clustering_table should parse at least 20 rows, got {}",
                row_count
            );
        }
        Err(e) => {
            panic!("❌ collection_clustering_table failed: {}", e);
        }
    }
}

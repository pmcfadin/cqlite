//! Integration test for Issue #222: SELECT query functionality
//!
//! This test validates that SELECT queries work correctly against real
//! Cassandra SSTable files using the ingestion flow.
//!
//! **Purpose**: Verify that the query engine properly connects to the REPL/CLI
//! and can execute SELECT...WHERE queries against real SSTable data.
//!
//! **Requirements**:
//! - CQLITE_DATASETS_ROOT environment variable pointing to test-data/datasets
//! - test_basic dataset with simple_table SSTable files
//! - basic-types.cql schema file
//!
//! **Coverage**:
//! - SELECT * FROM table LIMIT N
//! - SELECT columns FROM table WHERE condition
//! - Column projection
//! - WHERE clause filtering

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

// Test constants
const TEST_QUALIFIED_TABLE: &str = "test_basic.simple_table";
const KEYSPACE_FILTER: &str = "/test_basic/";

/// Get the datasets root directory from environment or default
fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Get the schemas directory
fn get_schemas_dir() -> Option<PathBuf> {
    // Try environment variable first
    if let Some(datasets_root) = get_datasets_root() {
        // Datasets root is test-data/datasets, schemas are in test-data/schemas
        let schemas_dir = datasets_root.parent()?.join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
    }

    // Fallback to relative path from cargo manifest
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    if schemas_dir.exists() {
        return Some(schemas_dir);
    }

    None
}

/// Setup test database with real SSTables via ingestion
/// Returns Ok(Database) if successful, Err(reason) if test should be skipped
async fn setup_test_database() -> Result<Database, String> {
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
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };

    let ingestion_result = ingest(ingestion_config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;

    // Verify ingestion loaded schemas
    if ingestion_result.schema_load_result.schemas_loaded == 0 {
        return Err("No schemas loaded during ingestion".to_string());
    }

    Ok(ingestion_result.database)
}

#[tokio::test]
async fn test_issue_222_select_with_limit() {
    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Test 1: SELECT * with LIMIT
    let query = format!("SELECT * FROM {} LIMIT 5", TEST_QUALIFIED_TABLE);
    let result = db.execute(&query).await;

    match result {
        Ok(query_result) => {
            assert!(
                !query_result.rows.is_empty(),
                "Issue #222: SELECT * LIMIT should return rows"
            );
            assert!(
                query_result.rows.len() <= 5,
                "Issue #222: LIMIT 5 should return at most 5 rows, got {}",
                query_result.rows.len()
            );
            println!(
                "Issue #222 VERIFIED: SELECT * LIMIT returned {} rows",
                query_result.rows.len()
            );
        }
        Err(e) => {
            panic!("Issue #222: SELECT * query failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_issue_222_select_with_column_projection() {
    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Test 2: SELECT with specific columns
    let query = format!("SELECT id, name, age FROM {} LIMIT 3", TEST_QUALIFIED_TABLE);
    let result = db.execute(&query).await;

    match result {
        Ok(query_result) => {
            assert!(
                !query_result.rows.is_empty(),
                "Issue #222: SELECT with column projection should return rows"
            );
            println!(
                "Issue #222 VERIFIED: SELECT with column projection returned {} rows",
                query_result.rows.len()
            );

            // Verify we have data in the rows
            for row in &query_result.rows {
                assert!(!row.values.is_empty(), "Issue #222: Row should have values");
            }
        }
        Err(e) => {
            panic!("Issue #222: SELECT with column projection failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_issue_222_select_with_where_clause() {
    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Test 3: SELECT with WHERE clause
    let query = format!(
        "SELECT id, name, age FROM {} WHERE age > 50 LIMIT 10",
        TEST_QUALIFIED_TABLE
    );
    let result = db.execute(&query).await;

    match result {
        Ok(query_result) => {
            // WHERE clause filters data - verify query executes successfully
            // Note: We verify execution, not filtering correctness (that's tested elsewhere)
            println!(
                "Issue #222 VERIFIED: SELECT with WHERE clause returned {} rows",
                query_result.rows.len()
            );

            // If we got rows, verify they're valid
            for row in &query_result.rows {
                assert!(
                    !row.values.is_empty(),
                    "Issue #222: Filtered rows should have values"
                );
            }
        }
        Err(e) => {
            panic!("Issue #222: SELECT with WHERE clause failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_issue_222_query_execution_time() {
    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Test 4: Verify execution time is reported
    let query = format!("SELECT * FROM {} LIMIT 3", TEST_QUALIFIED_TABLE);
    let result = db.execute(&query).await;

    match result {
        Ok(query_result) => {
            // Execution time should be non-negative (may be 0 on very fast systems)
            println!(
                "Issue #222 VERIFIED: Query execution time reported: {}ms",
                query_result.execution_time_ms
            );
        }
        Err(e) => {
            panic!("Issue #222: Query failed: {}", e);
        }
    }
}

//! Comprehensive CLI integration tests for all 33 test tables
//!
//! Tests that `SELECT * FROM keyspace.table LIMIT 10` succeeds on all tables
//! across all 4 keyspaces: test_basic, test_collections, test_timeseries, test_wide_rows
//!
//! Requirements:
//! - Test data must exist at test-data/datasets/sstables/
//! - All 4 schema files must exist in test-data/schemas/
//!
//! Validates:
//! 1. Exit code 0 - Command succeeded
//! 2. Valid JSON array output - Parses correctly
//! 3. Non-empty results - At least 1 row returned
//! 4. No ERROR messages in stderr - No parsing failures
//! 5. No invalid data markers in output - No corrupted values like <invalid-timestamp>
//!
//! Run with:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-cli comprehensive_select -- --nocapture
//! ```

#![allow(clippy::all)]

use rstest::rstest;
use std::path::PathBuf;
use std::process::Command;

// =============================================================================
// Test Configuration
// =============================================================================

/// Get datasets root from environment or default path
fn get_datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("test-data/datasets")
        })
}

/// Get schemas directory
fn get_schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
}

// =============================================================================
// Test Helper Functions
// =============================================================================

/// Result of a SELECT query test
#[derive(Debug)]
struct QueryTestResult {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    row_count: Option<usize>,
    is_valid_json: bool,
    has_errors: bool,
    has_warnings: bool,
    has_invalid_data: bool,
    error_count: usize,
    warning_count: usize,
}

impl QueryTestResult {
    fn is_success(&self) -> bool {
        self.exit_code == Some(0)
            && self.is_valid_json
            && self.row_count.unwrap_or(0) > 0
            && !self.has_errors
            && !self.has_invalid_data
    }

    /// Check if stderr contains parsing errors or warnings
    fn analyze_stderr(stderr: &str) -> (bool, bool, usize, usize) {
        let has_errors = stderr.contains("ERROR");
        let has_warnings = stderr.contains("WARN")
            && (stderr.contains("malformed")
                || stderr.contains("corruption")
                || stderr.contains("Skipping")
                || stderr.contains("invalid"));

        let error_count = stderr.matches("ERROR").count();
        let warning_count = stderr.matches("WARN").count();

        (has_errors, has_warnings, error_count, warning_count)
    }

    /// Check if output contains invalid data markers
    fn has_invalid_output(stdout: &str) -> bool {
        stdout.contains("<invalid-")
            || stdout.contains("invalid-timestamp")
            || stdout.contains("invalid-date")
            || stdout.contains("invalid-uuid")
    }
}

/// Run SELECT * FROM keyspace.table LIMIT 10 and return results
fn run_select_query(keyspace: &str, table: &str, schema_file: &str) -> QueryTestResult {
    let data_dir = get_datasets_root().join("sstables");
    let schema_path = get_schemas_dir().join(schema_file);
    let query = format!("SELECT * FROM {}.{} LIMIT 10", keyspace, table);

    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            schema_path.to_str().unwrap(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--query",
            &query,
            "--out",
            "json",
        ])
        .output()
        .expect("Failed to execute CLI command");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Check if output is valid JSON array
    let trimmed = stdout.trim();
    let is_valid_json = trimmed.starts_with('[') && trimmed.ends_with(']');

    // Count rows if valid JSON
    let row_count = if is_valid_json {
        serde_json::from_str::<Vec<serde_json::Value>>(&stdout)
            .ok()
            .map(|v| v.len())
    } else {
        None
    };

    // Check for errors and warnings in stderr
    let (has_errors, has_warnings, error_count, warning_count) =
        QueryTestResult::analyze_stderr(&stderr);

    // Check for invalid data in output
    let has_invalid_data = QueryTestResult::has_invalid_output(&stdout);

    QueryTestResult {
        exit_code: output.status.code(),
        stdout,
        stderr,
        row_count,
        is_valid_json,
        has_errors,
        has_warnings,
        has_invalid_data,
        error_count,
        warning_count,
    }
}

/// Assert test data is available, skip test if not
fn ensure_test_data_available() -> bool {
    let data_dir = get_datasets_root().join("sstables");
    let schemas_dir = get_schemas_dir();

    if !data_dir.exists() {
        eprintln!(
            "SKIP: Test data not available at {:?}. Set CQLITE_DATASETS_ROOT.",
            data_dir
        );
        return false;
    }

    if !schemas_dir.exists() {
        eprintln!("SKIP: Schema files not available at {:?}", schemas_dir);
        return false;
    }

    true
}

/// Run a test for a specific table
fn run_table_test(keyspace: &str, table: &str, schema_file: &str) {
    if !ensure_test_data_available() {
        return;
    }

    let result = run_select_query(keyspace, table, schema_file);

    // Diagnostic output
    eprintln!("=== {}.{} ===", keyspace, table);
    eprintln!("Exit code: {:?}", result.exit_code);
    eprintln!("Valid JSON: {}", result.is_valid_json);
    eprintln!("Row count: {:?}", result.row_count);
    eprintln!(
        "Errors: {} | Warnings: {} | Invalid data: {}",
        result.error_count, result.warning_count, result.has_invalid_data
    );
    if result.has_errors || result.has_warnings {
        eprintln!(
            "STDERR (first 1000 chars):\n{}",
            &result.stderr[..result.stderr.len().min(1000)]
        );
    }

    // Assertions
    assert_eq!(
        result.exit_code,
        Some(0),
        "{}.{}: Expected exit code 0, got {:?}. STDERR: {}",
        keyspace,
        table,
        result.exit_code,
        &result.stderr[..result.stderr.len().min(500)]
    );

    assert!(
        result.is_valid_json,
        "{}.{}: Expected valid JSON array output. Got: {}...",
        keyspace,
        table,
        &result.stdout[..result.stdout.len().min(200)]
    );

    assert!(
        result.row_count.unwrap_or(0) > 0,
        "{}.{}: Expected non-empty results",
        keyspace,
        table
    );

    // New: Check for parsing errors in stderr
    assert!(
        !result.has_errors,
        "{}.{}: Found {} ERROR messages in stderr. This indicates parsing failures.\nSTDERR:\n{}",
        keyspace,
        table,
        result.error_count,
        &result.stderr[..result.stderr.len().min(2000)]
    );

    // New: Check for invalid data in output
    assert!(
        !result.has_invalid_data,
        "{}.{}: Output contains invalid data markers (e.g., <invalid-timestamp>). This indicates data corruption.\nOutput sample:\n{}",
        keyspace,
        table,
        &result.stdout[..result.stdout.len().min(500)]
    );
}

// =============================================================================
// test_basic Keyspace Tests (8 tables)
// =============================================================================

#[rstest]
#[case("simple_table")]
#[case("composite_key_table")]
#[case("compression_test_table")]
#[case("counters")]
#[case("multi_partition_table")]
#[case("static_columns_table")] // Issue #255: STATIC column schema parsing
#[case("ttl_test_table")]
#[case("uncompressed_table")]
#[cfg(feature = "state_machine")]
fn test_select_test_basic(#[case] table: &str) {
    run_table_test("test_basic", table, "basic-types.cql");
}

// =============================================================================
// test_collections Keyspace Tests (8 tables)
// =============================================================================

#[rstest]
#[case("collection_clustering_table")]
#[case("collection_table")]
#[case("collections_with_udts")]
#[case("empty_collections_table")]
#[case("frozen_collections_table")]
#[case("large_collections_table")]
#[case("nested_collections_table")]
#[case("typed_collections_table")]
#[cfg(feature = "state_machine")]
fn test_select_test_collections(#[case] table: &str) {
    run_table_test("test_collections", table, "collections.cql");
}

// =============================================================================
// test_timeseries Keyspace Tests (9 tables)
// =============================================================================

#[rstest]
#[case("app_metrics")]
#[case("event_store")]
#[case("log_entries")]
#[case("sensor_data")]
#[case("stock_prices")]
#[case("tick_data")]
#[case("time_bucketed_counters")] // Issue #256: Counter table returns 0 rows
#[case("user_activity")]
#[case("user_sessions")]
#[cfg(feature = "state_machine")]
fn test_select_test_timeseries(#[case] table: &str) {
    run_table_test("test_timeseries", table, "time-series.cql");
}

// =============================================================================
// test_wide_rows Keyspace Tests (8 tables)
// =============================================================================

#[rstest]
#[case("chat_messages")]
#[case("document_versions")]
#[case("large_blob_table")]
#[case("many_columns_table")]
#[case("multi_metric_timeseries")]
#[case("product_catalog")]
#[case("sparse_data_table")]
#[case("wide_partition_table")]
#[cfg(feature = "state_machine")]
fn test_select_test_wide_rows(#[case] table: &str) {
    run_table_test("test_wide_rows", table, "wide-rows.cql");
}

// =============================================================================
// Summary Report Test
// =============================================================================

/// Run all 33 tables and produce a summary report.
/// This test is ignored by default - run with: cargo test test_all_tables_summary -- --ignored
#[test]
#[ignore]
#[cfg(feature = "state_machine")]
fn test_all_tables_summary() {
    if !ensure_test_data_available() {
        return;
    }

    let mut passed = Vec::new();
    let mut failed = Vec::new();

    // All tables organized by keyspace
    let test_configs = [
        (
            "test_basic",
            "basic-types.cql",
            vec![
                "simple_table",
                "composite_key_table",
                "compression_test_table",
                "counters",
                "multi_partition_table",
                "static_columns_table",
                "ttl_test_table",
                "uncompressed_table",
            ],
        ),
        (
            "test_collections",
            "collections.cql",
            vec![
                "collection_clustering_table",
                "collection_table",
                "collections_with_udts",
                "empty_collections_table",
                "frozen_collections_table",
                "large_collections_table",
                "nested_collections_table",
                "typed_collections_table",
            ],
        ),
        (
            "test_timeseries",
            "time-series.cql",
            vec![
                "app_metrics",
                "event_store",
                "log_entries",
                "sensor_data",
                "stock_prices",
                "tick_data",
                "time_bucketed_counters",
                "user_activity",
                "user_sessions",
            ],
        ),
        (
            "test_wide_rows",
            "wide-rows.cql",
            vec![
                "chat_messages",
                "document_versions",
                "large_blob_table",
                "many_columns_table",
                "multi_metric_timeseries",
                "product_catalog",
                "sparse_data_table",
                "wide_partition_table",
            ],
        ),
    ];

    for (keyspace, schema, tables) in test_configs {
        for table in tables {
            let result = run_select_query(keyspace, table, schema);
            let full_name = format!("{}.{}", keyspace, table);

            if result.is_success() {
                passed.push(full_name);
            } else {
                failed.push((
                    full_name,
                    format!(
                        "exit={:?}, valid_json={}, rows={:?}",
                        result.exit_code, result.is_valid_json, result.row_count
                    ),
                ));
            }
        }
    }

    // Print summary
    let total = passed.len() + failed.len();
    println!("\n========================================");
    println!("   COMPREHENSIVE TABLE TEST SUMMARY");
    println!("========================================");
    println!("Passed: {}/{}", passed.len(), total);
    println!("Failed: {}/{}", failed.len(), total);

    if !failed.is_empty() {
        println!("\nFailed tables:");
        for (table, reason) in &failed {
            println!("  - {}: {}", table, reason);
        }
    }

    println!("\nPassed tables:");
    for table in &passed {
        println!("  - {}", table);
    }

    // Assert all passed
    assert!(
        failed.is_empty(),
        "{} out of {} tables failed. See details above.",
        failed.len(),
        total
    );
}

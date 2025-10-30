//! Integration tests for CLI schema validation (Issue #199)
//!
//! Tests that the CLI fails fast when schema/data mismatches are detected,
//! providing clear error messages with troubleshooting steps.
//!
//! Test Coverage:
//! - Valid table queries succeed
//! - Nonexistent table queries fail-fast with schema errors
//! - Error messages include troubleshooting steps
//! - All statement types (SELECT/INSERT/UPDATE/DELETE) are validated
//! - Experimental fallback bypasses schema validation

#![cfg(all(test, feature = "state_machine"))]
#![allow(clippy::all)]

use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;

// ============================================================================
// Helper Functions
// ============================================================================

/// Helper to get test data directory
fn test_data_dir() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("Failed to get parent directory")
                .join("test-data/datasets/sstables")
        })
}

/// Helper to get schema file
fn test_schema_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("Failed to get parent directory")
        .join("test-data/schemas/basic-types.cql")
}

/// Helper to get simple_table path for fallback tests
fn get_simple_table_path() -> PathBuf {
    let root = test_data_dir();
    let table_dir = root
        .join("test_basic")
        .join("simple_table-6aa08200a25111f0a3fef1a551383fb9");

    // Return the Data.db file path
    table_dir.join("nb-1-big-Data.db")
}

// ============================================================================
// Test Cases - Valid Query Success
// ============================================================================

#[test]
#[ignore = "Requires working ingestion system - table discovery not yet stable"]
fn test_valid_table_query_succeeds() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("SELECT * FROM test_basic.simple_table LIMIT 1")
        .arg("--format")
        .arg("json");

    cmd.assert().success();
}

// ============================================================================
// Test Cases - Nonexistent Table Fail-Fast
// ============================================================================

#[test]
fn test_nonexistent_table_fails_fast() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("SELECT * FROM test_basic.nonexistent_table");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Schema not found for table"))
        .stderr(predicate::str::contains("nonexistent_table"))
        .stderr(predicate::str::contains("Troubleshooting"));
}

#[test]
fn test_error_message_includes_troubleshooting() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("SELECT * FROM test_basic.missing_table");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains(
            "Verify table name matches schema definition",
        ))
        .stderr(predicate::str::contains(
            "Check that schema file was loaded correctly",
        ));
}

// ============================================================================
// Test Cases - Statement Type Validation
// ============================================================================

#[test]
fn test_insert_statement_validation() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg(
            "INSERT INTO test_basic.nonexistent (id) VALUES (550e8400-e29b-41d4-a716-446655440000)",
        );

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Schema not found"));
}

#[test]
fn test_update_statement_validation() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("UPDATE test_basic.nonexistent SET name = 'test' WHERE id = 550e8400-e29b-41d4-a716-446655440000");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Schema not found"));
}

#[test]
fn test_delete_statement_validation() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("DELETE FROM test_basic.nonexistent WHERE id = 550e8400-e29b-41d4-a716-446655440000");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Schema not found"));
}

// ============================================================================
// Test Cases - Fallback Feature Bypass
// ============================================================================

#[test]
fn test_fallback_feature_bypasses_validation() {
    // This test verifies that the experimental fallback feature
    // doesn't trigger schema validation (Issue #142)

    let table_path = get_simple_table_path();

    // We don't assert table_path exists here because we're testing
    // that the error is NOT a schema validation error

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--enable-select-fallback")
        .arg("-e")
        .arg(format!("SELECT * FROM {}", table_path.display()));

    let output = cmd.output().expect("Failed to execute command");
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should NOT contain schema validation error
    // (may fail with file not found or other errors, but not schema validation)
    assert!(
        !stderr.contains("Schema not found for table"),
        "Fallback should bypass schema validation. Stderr: {}",
        stderr
    );
}

// ============================================================================
// Test Cases - Keyspace Qualification
// ============================================================================

#[test]
fn test_unqualified_table_name_fails() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("SELECT * FROM nonexistent_table");

    // Should fail - either due to schema validation or missing keyspace context
    cmd.assert().failure();
}

#[test]
#[ignore = "Requires working ingestion system - table discovery not yet stable"]
fn test_qualified_table_name_with_valid_keyspace_succeeds() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("SELECT * FROM test_basic.simple_table LIMIT 1")
        .arg("--format")
        .arg("json");

    cmd.assert().success();
}

// ============================================================================
// Test Cases - Exit Codes
// ============================================================================

#[test]
fn test_schema_validation_failure_exit_code() {
    let schema = test_schema_file();
    let data_dir = test_data_dir();

    // Assert test data is available
    assert!(
        schema.exists() && data_dir.exists(),
        "Test requires schema at {:?} and data at {:?}",
        schema,
        data_dir
    );

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");
    cmd.arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("-e")
        .arg("SELECT * FROM test_basic.nonexistent_table");

    let output = cmd.output().expect("Failed to execute command");

    // Should fail with a non-zero exit code
    assert!(
        !output.status.success(),
        "Schema validation failure should return non-zero exit code"
    );

    // Exit code should be consistent (not 0)
    let exit_code = output.status.code().unwrap_or(1);
    assert_ne!(exit_code, 0, "Exit code should be non-zero for errors");
}

//! Golden snapshot tests for table output validation
//!
//! Tests Issue #139: Golden snapshot infrastructure for table output validation
//!
//! These tests capture expected output formats to prevent formatting regressions.
//!
//! # Running Tests
//!
//! Normal run (validates against golden snapshots):
//! ```bash
//! cargo test --test table_snapshot_tests
//! ```
//!
//! Update snapshots when output format changes intentionally:
//! ```bash
//! UPDATE_SNAPSHOTS=1 cargo test --test table_snapshot_tests
//! ```

use anyhow::Result;
use std::path::PathBuf;
use std::process::Command;

// Import golden snapshot helper from common module
#[path = "common/golden_snapshots.rs"]
mod golden_snapshots;

const CLI_BINARY: &str = "cqlite";

/// Test helper to run CLI commands and capture output
fn run_cli_command(args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .args(["run", "--quiet", "--bin", CLI_BINARY, "--"])
        .args(args)
        .output()
        .expect("Failed to execute CLI command")
}

/// Get the test data root directory from environment or default path
fn get_test_data_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("test-data/datasets")
        })
}

/// Get the schemas directory
fn get_schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
}

#[test]
#[cfg(feature = "state_machine")]
fn test_table_output_snapshot() -> Result<()> {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found at {:?}",
        data_dir
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 3",
        "--format",
        "table",
    ]);

    // Verify command succeeded
    assert_eq!(
        output.status.code(),
        Some(0),
        "CLI command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;

    // Assert against golden snapshot
    golden_snapshots::assert_golden_snapshot("table_output.txt", &stdout)?;

    Ok(())
}

#[test]
#[cfg(feature = "state_machine")]
fn test_json_output_deterministic() -> Result<()> {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found at {:?}",
        data_dir
    );

    // Run the same query twice to ensure deterministic output
    let run_query = || {
        run_cli_command(&[
            "--schema",
            schema_file.to_str().unwrap(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "-e",
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            "--format",
            "json",
        ])
    };

    let output1 = run_query();
    let output2 = run_query();

    // Both should succeed
    assert!(
        output1.status.success(),
        "First run should succeed. stderr: {}",
        String::from_utf8_lossy(&output1.stderr)
    );
    assert!(
        output2.status.success(),
        "Second run should succeed. stderr: {}",
        String::from_utf8_lossy(&output2.stderr)
    );

    let stdout1 = String::from_utf8(output1.stdout)?;
    let stdout2 = String::from_utf8(output2.stdout)?;

    // Outputs should be identical (deterministic)
    assert_eq!(
        stdout1.trim(),
        stdout2.trim(),
        "JSON output should be deterministic across runs"
    );

    // Also save as golden snapshot
    golden_snapshots::assert_golden_snapshot("json_output.json", &stdout1)?;

    Ok(())
}

#[test]
#[cfg(feature = "state_machine")]
fn test_csv_output_snapshot() -> Result<()> {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found at {:?}",
        data_dir
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 3",
        "--format",
        "csv",
    ]);

    // Verify command succeeded
    assert!(
        output.status.success(),
        "CLI command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;

    // Assert against golden snapshot
    golden_snapshots::assert_golden_snapshot("csv_output.csv", &stdout)?;

    Ok(())
}

#[test]
#[cfg(feature = "state_machine")]
fn test_collections_table_output_snapshot() -> Result<()> {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("collections.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found at {:?}",
        data_dir
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_collections.collection_table LIMIT 2",
        "--format",
        "table",
    ]);

    // Verify command succeeded
    assert!(
        output.status.success(),
        "CLI command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;

    // Assert against golden snapshot for collections
    golden_snapshots::assert_golden_snapshot("collections_table_output.txt", &stdout)?;

    Ok(())
}

#[test]
#[cfg(feature = "state_machine")]
fn test_wide_rows_output_snapshot() -> Result<()> {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("wide-rows.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found at {:?}",
        data_dir
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_wide_rows.wide_partition_table LIMIT 3",
        "--format",
        "table",
    ]);

    // Verify command succeeded
    assert!(
        output.status.success(),
        "CLI command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;

    // Assert against golden snapshot for wide rows
    golden_snapshots::assert_golden_snapshot("wide_rows_table_output.txt", &stdout)?;

    Ok(())
}

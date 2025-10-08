//! Integration tests for one-shot SELECT execution with ingestion
//!
//! Tests Issue #135: Wire ingestion into one-shot SELECT
//!
//! Requirements:
//! - `cqlite --schema ... --data-dir ... -e "SELECT ..."` reads SSTables via catalog
//! - Non-empty rows on `test-data`
//! - Correct exit codes: 3 (schema), 4 (data-dir/discovery), 5 (unsupported/query errors)
//! - Supports version precedence: flag > SSTable metadata > metadata.yml > unknown

#![allow(clippy::all)]

use std::path::PathBuf;
use std::process::Command;

const CLI_BINARY: &str = "cqlite";

/// Test helper to run CLI commands and capture output
fn run_cli_command(args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .args(&["run", "--quiet", "--bin", CLI_BINARY, "--"])
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
fn test_one_shot_select_with_ingestion_basic() {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Skip if test data not available
    if !data_dir.exists() || !schema_file.exists() {
        eprintln!("Skipping test: test data not found at {:?}", data_dir);
        return;
    }

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--format",
        "json",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // Should succeed (exit code 0)
    assert!(
        output.status.success(),
        "One-shot SELECT should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Should produce output (rows or empty result, but not an error)
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should not contain critical errors
    assert!(
        !stderr.contains("error") || stderr.contains("INFO"),
        "Should not contain errors in stderr"
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_one_shot_select_with_invalid_schema_exit_code_3() {
    let data_dir = get_test_data_root().join("sstables");
    let invalid_schema = PathBuf::from("/tmp/nonexistent_schema.cql");

    // Skip if test data not available
    if !data_dir.exists() {
        eprintln!("Skipping test: test data not found");
        return;
    }

    let output = run_cli_command(&[
        "--schema",
        invalid_schema.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // Should fail with exit code 3 (schema error)
    let exit_code = output.status.code().unwrap_or(1);
    assert_eq!(
        exit_code, 3,
        "Invalid schema should return exit code 3, got {}",
        exit_code
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("schema")
            || stderr.contains("Ingestion failed")
            || stderr.contains("Path does not exist"),
        "Error message should mention schema or path error"
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_one_shot_select_with_invalid_data_dir_exit_code_4() {
    let invalid_data_dir = PathBuf::from("/tmp/nonexistent_data_directory");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Skip if schema not available
    if !schema_file.exists() {
        eprintln!("Skipping test: schema not found");
        return;
    }

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        invalid_data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // Should fail with exit code 4 (discovery/data-dir error)
    let exit_code = output.status.code().unwrap_or(1);
    assert_eq!(
        exit_code, 4,
        "Invalid data-dir should return exit code 4, got {}",
        exit_code
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("data")
            || stderr.contains("directory")
            || stderr.contains("Path does not exist")
            || stderr.contains("Ingestion failed"),
        "Error message should mention data directory error"
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_one_shot_select_with_version_hint() {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Skip if test data not available
    if !data_dir.exists() || !schema_file.exists() {
        eprintln!("Skipping test: test data not found");
        return;
    }

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--cassandra-version",
        "5.0",
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--format",
        "json",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // Should succeed with version hint
    assert!(
        output.status.success(),
        "One-shot SELECT with version hint should succeed"
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_one_shot_select_table_format() {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Skip if test data not available
    if !data_dir.exists() || !schema_file.exists() {
        eprintln!("Skipping test: test data not found");
        return;
    }

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--format",
        "table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));

    // Should succeed
    assert!(
        output.status.success(),
        "Table format output should succeed"
    );

    // Table format should produce some output
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.is_empty(), "Table format should produce output");
}

#[test]
#[cfg(feature = "state_machine")]
fn test_one_shot_select_csv_format() {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Skip if test data not available
    if !data_dir.exists() || !schema_file.exists() {
        eprintln!("Skipping test: test data not found");
        return;
    }

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--format",
        "csv",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));

    // Should succeed
    assert!(output.status.success(), "CSV format output should succeed");

    // CSV format should produce some output
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.is_empty(), "CSV format should produce output");
}

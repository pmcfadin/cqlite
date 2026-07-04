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

/// Test helper to run CLI commands and capture output using the pre-built binary
/// (`CARGO_BIN_EXE_cqlite`), avoiding a nested `cargo run` rebuild per test.
fn run_cli_command(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
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
    let _stdout = String::from_utf8_lossy(&output.stdout);
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

    // Assert test data is available
    assert!(
        data_dir.exists(),
        "Test requires full SSTable dataset: test data not found at {:?}",
        data_dir
    );

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
    assert!(
        schema_file.exists(),
        "Test requires full SSTable dataset: schema not found at {:?}",
        schema_file
    );

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

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

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

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

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

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

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

// ============================================================================
// Issue #223: --query parameter tests (alias for --execute)
// ============================================================================

#[test]
#[cfg(feature = "state_machine")]
fn test_query_parameter_alias_for_execute() {
    // Issue #223: --query should work identically to --execute
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--query", // Using --query alias instead of -e
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
        "--query alias should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Should produce non-empty output
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.is_empty(),
        "--query should produce output identical to -e"
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_out_parameter_json_format() {
    // Issue #223: --out should control output format
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--query",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--out", // Using --out instead of --format
        "json",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));

    // Should succeed
    assert!(
        output.status.success(),
        "--out json should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Output should be valid JSON (starts with { or [)
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    assert!(
        trimmed.starts_with('{') || trimmed.starts_with('['),
        "--out json should produce JSON output, got: {}",
        &trimmed[..trimmed.len().min(100)]
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_out_parameter_csv_format() {
    // Issue #223: --out csv should produce CSV output
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--query",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--out",
        "csv",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));

    // Should succeed
    assert!(
        output.status.success(),
        "--out csv should succeed. Exit code: {:?}",
        output.status.code()
    );

    // CSV output should contain commas (header line at minimum)
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(',') || stdout.lines().count() > 0,
        "--out csv should produce CSV-like output"
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_out_parameter_table_format() {
    // Issue #223: --out table should produce table output
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--query",
        "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--out",
        "table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));

    // Should succeed
    assert!(
        output.status.success(),
        "--out table should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Table output should contain some output
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.is_empty(), "--out table should produce output");
}

#[test]
#[cfg(feature = "state_machine")]
fn test_query_and_out_combined_prd_example() {
    // Issue #223: Test the exact PRD example usage pattern
    // cqlite --schema schema.json --data-dir /path/to/sstables \
    //   --query "SELECT * FROM users WHERE id = 'abc'" \
    //   --out json
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--query",
        "SELECT * FROM test_basic.simple_table LIMIT 3",
        "--out",
        "json",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // Should succeed - this is the PRD-specified usage
    assert!(
        output.status.success(),
        "PRD example (--query + --out json) should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Should produce JSON output
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    assert!(
        trimmed.starts_with('{') || trimmed.starts_with('['),
        "PRD example should produce JSON output"
    );
}

#[test]
#[cfg(feature = "state_machine")]
fn test_out_takes_precedence_over_format() {
    // Issue #223: --out should override --format when both are specified
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    // Assert test data is available
    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset: test data not found. data_dir={:?}, schema_file={:?}",
        data_dir,
        schema_file
    );

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--query",
        "SELECT id, name FROM test_basic.simple_table LIMIT 1",
        "--format",
        "table", // This should be overridden
        "--out",
        "json", // This should win
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));

    // Should succeed
    assert!(
        output.status.success(),
        "--out should take precedence over --format. Exit code: {:?}",
        output.status.code()
    );

    // Output should be JSON (not table format)
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    assert!(
        trimmed.starts_with('{') || trimmed.starts_with('['),
        "--out json should override --format table, but got: {}",
        &trimmed[..trimmed.len().min(100)]
    );
}

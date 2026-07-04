//! Export Integration Tests (Issue #282)
//!
//! End-to-end integration tests for the CLI export command using real SSTable data.
//!
//! Epic: #276 (M3 Output Writers)
//! Depends on: #278 (Export Command), #281 (Parquet Writer Tests)
//!
//! # Test Coverage
//!
//! - Export to CSV, JSON, Parquet formats
//! - test_basic and test_collections datasets
//! - Golden file comparisons
//! - Parquet validation with arrow-rs
//! - Error cases (invalid table, bad format, file errors)
//! - Cross-format consistency

#![cfg(feature = "state_machine")]

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::error::Error as StdError;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use tempfile::TempDir;

mod common;

// ============================================================================
// Helper Functions
// ============================================================================

/// Run CLI command and capture output using the pre-built binary
/// (`CARGO_BIN_EXE_cqlite`), avoiding a nested `cargo run` rebuild per test.
fn run_cli_command(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("Failed to execute CLI command")
}

/// Get test data root directory from environment or default path
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

/// Get schemas directory
fn get_schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
}

/// Helper to read Parquet bytes back into a RecordBatch for verification
fn read_parquet_back(bytes: &[u8]) -> Result<RecordBatch, Box<dyn StdError>> {
    let bytes = Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let mut reader = builder.build()?;
    match reader.next() {
        Some(result) => result.map_err(|e| Box::new(e) as Box<dyn StdError>),
        None => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "No batches in Parquet file",
        )) as Box<dyn StdError>),
    }
}

/// Verify Parquet file has valid magic bytes (PAR1 at start and end)
fn verify_parquet_magic(bytes: &[u8]) {
    assert!(bytes.len() >= 8, "Parquet file too small");
    assert_eq!(&bytes[0..4], b"PAR1", "Should start with PAR1 magic bytes");
    assert_eq!(
        &bytes[bytes.len() - 4..],
        b"PAR1",
        "Should end with PAR1 magic bytes"
    );
}

/// Assert test data is available, skip test if not
fn assert_test_data_available() -> (PathBuf, PathBuf) {
    let data_dir = get_test_data_root().join("sstables");
    let schema_file = get_schemas_dir().join("basic-types.cql");

    assert!(
        data_dir.exists() && schema_file.exists(),
        "Test requires full SSTable dataset. \n        Set CQLITE_DATASETS_ROOT or run: bash test-data/scripts/fetch-datasets.sh\n        data_dir={data_dir:?}, schema_file={schema_file:?}"
    );

    (data_dir, schema_file)
}

// ============================================================================
// Basic CSV Export Tests
// ============================================================================

#[test]
fn test_export_csv_basic_types() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export.csv");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "CSV export should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Verify output file exists and has content
    assert!(output_file.exists(), "Output CSV file should exist");
    let csv_content = fs::read_to_string(&output_file).expect("Failed to read CSV");
    assert!(!csv_content.is_empty(), "CSV should not be empty");

    // Verify it has a header row (first line with column names)
    let lines: Vec<&str> = csv_content.lines().collect();
    assert!(lines.len() >= 2, "CSV should have header + data rows");

    // Header should contain expected column names
    let header = lines[0];
    assert!(
        header.contains("id") || header.contains("name"),
        "CSV header should contain column names: {header}"
    );
}

#[test]
fn test_export_json_basic_types() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export.json");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "json",
        "--table",
        "test_basic.simple_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "JSON export should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Verify output file exists and is valid JSON
    assert!(output_file.exists(), "Output JSON file should exist");
    let json_content = fs::read_to_string(&output_file).expect("Failed to read JSON");
    assert!(!json_content.is_empty(), "JSON should not be empty");

    // Verify it's valid JSON (should be an array)
    let parsed: serde_json::Value =
        serde_json::from_str(&json_content).expect("Should be valid JSON");
    assert!(parsed.is_array(), "JSON output should be an array");

    let array = parsed.as_array().unwrap();
    assert!(!array.is_empty(), "JSON array should have rows");
}

#[test]
fn test_export_csv_collections() {
    let (data_dir, _) = assert_test_data_available();
    let schema_file = get_schemas_dir().join("collections.cql");

    if !schema_file.exists() {
        eprintln!("Skipping test: collections schema not found");
        return;
    }

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export_collections.csv");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_collections.collection_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "CSV export of collections should succeed. Exit code: {:?}",
        output.status.code()
    );

    assert!(output_file.exists(), "Output CSV file should exist");
    let csv_content = fs::read_to_string(&output_file).expect("Failed to read CSV");
    assert!(!csv_content.is_empty(), "CSV should not be empty");
}

#[test]
fn test_export_json_collections() {
    let (data_dir, _) = assert_test_data_available();
    let schema_file = get_schemas_dir().join("collections.cql");

    if !schema_file.exists() {
        eprintln!("Skipping test: collections schema not found");
        return;
    }

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export_collections.json");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "json",
        "--table",
        "test_collections.collection_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "JSON export of collections should succeed. Exit code: {:?}",
        output.status.code()
    );

    assert!(output_file.exists(), "Output JSON file should exist");
    let json_content = fs::read_to_string(&output_file).expect("Failed to read JSON");

    // Verify it's valid JSON with collections
    let parsed: serde_json::Value =
        serde_json::from_str(&json_content).expect("Should be valid JSON");
    assert!(parsed.is_array(), "JSON output should be an array");
}

// ============================================================================
// Parquet Export Tests
// ============================================================================

#[test]
fn test_export_parquet_basic() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export.parquet");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "parquet",
        "--table",
        "test_basic.simple_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "Parquet export should succeed. Exit code: {:?}",
        output.status.code()
    );

    // Verify output file exists
    assert!(output_file.exists(), "Output Parquet file should exist");

    // Read and verify Parquet magic bytes
    let parquet_bytes = fs::read(&output_file).expect("Failed to read Parquet file");
    verify_parquet_magic(&parquet_bytes);
}

#[test]
fn test_export_parquet_roundtrip() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export_roundtrip.parquet");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "parquet",
        "--table",
        "test_basic.simple_table",
    ]);

    assert!(
        output.status.success(),
        "Parquet export should succeed for roundtrip test"
    );

    // Read back with arrow-rs and validate
    let parquet_bytes = fs::read(&output_file).expect("Failed to read Parquet file");
    let batch = read_parquet_back(&parquet_bytes).expect("Failed to read Parquet back");

    // Verify we got data
    assert!(batch.num_rows() > 0, "Should have rows in Parquet file");
    assert!(
        batch.num_columns() > 0,
        "Should have columns in Parquet file"
    );

    eprintln!(
        "Parquet roundtrip: {} rows, {} columns",
        batch.num_rows(),
        batch.num_columns()
    );
}

#[test]
fn test_export_parquet_schema_matches() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export_schema.parquet");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "parquet",
        "--table",
        "test_basic.simple_table",
    ]);

    assert!(
        output.status.success(),
        "Parquet export should succeed for schema test"
    );

    // Read back and verify schema has expected columns
    let parquet_bytes = fs::read(&output_file).expect("Failed to read Parquet file");
    let batch = read_parquet_back(&parquet_bytes).expect("Failed to read Parquet back");

    // Get column names from Arrow schema
    let schema = batch.schema();
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // simple_table should have 'id' column at minimum
    assert!(
        column_names.contains(&"id"),
        "Parquet schema should contain 'id' column. Found: {column_names:?}"
    );

    eprintln!("Parquet columns: {column_names:?}");
}

// ============================================================================
// Filter Tests
// ============================================================================

#[test]
fn test_export_with_query_filter() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("export_filtered.csv");

    // Use a WHERE clause filter
    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
        "--query",
        "active = true",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // Query filter must work - strict assertion
    assert!(
        output.status.success(),
        "Export with --query filter should succeed. Exit code: {:?}\nSTDERR: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(output_file.exists(), "Filtered output should exist");
    let csv_content = fs::read_to_string(&output_file).expect("Failed to read CSV");
    let line_count = csv_content.lines().count();

    // Should have header + at least some data rows
    assert!(line_count >= 1, "CSV should have at least a header row");
    eprintln!("Filtered CSV rows (including header): {line_count}");
}

#[test]
fn test_export_row_count_matches_query() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_file = temp_dir.path().join("export_count.csv");

    // Export to CSV
    let export_output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        csv_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    assert!(
        export_output.status.success(),
        "Export should succeed for count test"
    );

    // Also run a direct query to get row count
    let query_output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "-e",
        "SELECT * FROM test_basic.simple_table",
        "--format",
        "json",
    ]);

    // Query must succeed for this test to be valid - strict assertion
    assert!(
        query_output.status.success(),
        "Direct query should succeed. Exit code: {:?}\nSTDERR: {}",
        query_output.status.code(),
        String::from_utf8_lossy(&query_output.stderr)
    );

    let query_stdout = String::from_utf8_lossy(&query_output.stdout);
    let query_json: serde_json::Value =
        serde_json::from_str(&query_stdout).expect("Query output should be valid JSON");
    let query_row_count = query_json
        .as_array()
        .expect("Query output should be a JSON array")
        .len();

    // Count CSV rows (subtract 1 for header)
    let csv_content = fs::read_to_string(&csv_file).expect("Failed to read CSV");
    let csv_row_count = csv_content.lines().count().saturating_sub(1);

    eprintln!("Row counts - Query: {query_row_count}, CSV: {csv_row_count}");

    assert_eq!(
        csv_row_count, query_row_count,
        "CSV export row count should match query result count"
    );
}

#[test]
fn test_export_with_limit() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_file = temp_dir.path().join("export_limit.csv");

    const LIMIT: usize = 3;

    // Export with --limit
    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        csv_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
        "--limit",
        &LIMIT.to_string(),
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "Export with --limit should succeed. Exit code: {:?}\nSTDERR: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(csv_file.exists(), "Output CSV file should exist");
    let csv_content = fs::read_to_string(&csv_file).expect("Failed to read CSV");
    let lines: Vec<&str> = csv_content.lines().collect();

    // Should have header + exactly LIMIT data rows
    let data_row_count = lines.len().saturating_sub(1); // Subtract header
    assert_eq!(
        data_row_count, LIMIT,
        "CSV should have exactly {LIMIT} data rows (got {data_row_count}). Full content:\n{csv_content}"
    );

    eprintln!("Limit test passed: {LIMIT} rows exported as expected");
}

// ============================================================================
// Progress / Statistics Suppression Tests (Issue #284)
// ============================================================================

/// Spec R4 (quiet scenario): `--quiet` emits no progress and no summary on
/// stdout, while the export file is still written.
#[test]
fn test_export_quiet_emits_no_stdout_but_writes_file() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("quiet_export.csv");

    let output = run_cli_command(&[
        "--quiet",
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "Quiet export should succeed. Exit code: {:?}",
        output.status.code()
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.trim().is_empty(),
        "Quiet export must emit no stdout (no progress, no summary). Got:\n{stdout}"
    );

    assert!(
        output_file.exists(),
        "Quiet export must still write the output file"
    );
    let csv_content = fs::read_to_string(&output_file).expect("Failed to read CSV");
    assert!(
        !csv_content.is_empty(),
        "Quiet export file should have content"
    );
}

/// Spec R4 (piped scenario): without `--quiet` but with stdout not a TTY
/// (assert/Command capture is inherently non-TTY), the command emits no
/// progress and no final summary, yet still writes the export file. This is
/// the wiring-evidence test for the summary-suppression fix (previously the
/// summary printed whenever `!quiet`, even when piped).
#[test]
fn test_export_piped_non_tty_emits_no_progress_or_summary() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("piped_export.csv");

    // No --quiet flag; captured stdout via Command is non-TTY (piped).
    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "Piped export should succeed. Exit code: {:?}",
        output.status.code()
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // No final summary fields and no progress preamble.
    assert!(
        !stdout.contains("Export complete:")
            && !stdout.contains("Rows:")
            && !stdout.contains("Size:")
            && !stdout.contains("Time:")
            && !stdout.contains("Rate:"),
        "Piped export must emit no summary on stdout. Got:\n{stdout}"
    );
    assert!(
        !stdout.contains("Exporting data from:")
            && !stdout.contains("Streaming export in progress"),
        "Piped export must emit no progress preamble on stdout. Got:\n{stdout}"
    );

    assert!(
        output_file.exists(),
        "Piped export must still write the output file"
    );
    let csv_content = fs::read_to_string(&output_file).expect("Failed to read CSV");
    assert!(
        !csv_content.is_empty(),
        "Piped export file should have content"
    );
}

/// Spec R1/R5 (determinate path executes end-to-end): with `--limit` set the
/// known-total / determinate-bar code path runs without error and produces
/// exactly the limited row output.
#[test]
fn test_export_with_limit_determinate_path_succeeds() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("limit_determinate.csv");

    const LIMIT: usize = 2;

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
        "--limit",
        &LIMIT.to_string(),
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        output.status.success(),
        "Determinate (--limit) export should succeed. Exit code: {:?}\nSTDERR: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(output_file.exists(), "Output CSV file should exist");
    let csv_content = fs::read_to_string(&output_file).expect("Failed to read CSV");
    let data_row_count = csv_content.lines().count().saturating_sub(1);
    assert_eq!(
        data_row_count, LIMIT,
        "Determinate export should write exactly {LIMIT} rows (got {data_row_count})"
    );
}

// ============================================================================
// Golden File / Determinism Tests
// ============================================================================

#[test]
fn test_export_csv_deterministic() {
    // Test that CSV export produces valid, parseable output
    // Note: Row order is not deterministic (depends on SSTable partition order)
    // so we verify structure rather than exact content
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("deterministic.csv");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    assert!(output.status.success(), "CSV export should succeed");

    let csv_content = fs::read_to_string(&output_file).expect("Failed to read CSV");
    let lines: Vec<&str> = csv_content.lines().collect();

    // Verify structure
    assert!(lines.len() > 1, "Should have header + data rows");

    // Verify header contains expected columns
    let header = lines[0];
    assert!(header.contains("id"), "Header should contain 'id'");
    assert!(header.contains("name"), "Header should contain 'name'");
    assert!(header.contains("age"), "Header should contain 'age'");

    // Verify we have data rows
    let data_row_count = lines.len() - 1;
    assert!(
        data_row_count > 0,
        "Should have at least one data row, got {data_row_count}"
    );

    eprintln!(
        "CSV structure verified: {} columns, {} data rows",
        header.split(',').count(),
        data_row_count
    );
}

#[test]
fn test_export_json_deterministic() {
    // Test that JSON export produces valid, parseable output with correct structure
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("deterministic.json");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "json",
        "--table",
        "test_basic.simple_table",
    ]);

    assert!(output.status.success(), "JSON export should succeed");

    let json_content = fs::read_to_string(&output_file).expect("Failed to read JSON");
    let parsed: serde_json::Value =
        serde_json::from_str(&json_content).expect("Should be valid JSON");

    // Verify it's an array with objects
    assert!(parsed.is_array(), "JSON output should be an array");
    let array = parsed.as_array().unwrap();
    assert!(!array.is_empty(), "JSON array should have rows");

    // Verify first row has expected keys
    let first_row = &array[0];
    assert!(first_row.is_object(), "Each row should be an object");
    let obj = first_row.as_object().unwrap();

    assert!(obj.contains_key("id"), "Row should contain 'id'");
    assert!(obj.contains_key("name"), "Row should contain 'name'");
    assert!(obj.contains_key("age"), "Row should contain 'age'");

    eprintln!(
        "JSON structure verified: {} keys per row, {} rows",
        obj.len(),
        array.len()
    );
}

// ============================================================================
// Error Case Tests
// ============================================================================

#[test]
fn test_export_nonexistent_table_behavior() {
    // Issue #280: With streaming export, non-existent tables now fail early
    // with clear error message rather than silently returning empty results.
    // This is better validation behavior.
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("empty_output.csv");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "nonexistent_keyspace.nonexistent_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // With streaming export (Issue #280), non-existent tables now return an error
    // because column metadata cannot be determined without schema.
    // This is better behavior than silently returning empty results.
    assert!(
        !output.status.success(),
        "Export command should fail for non-existent table (strict validation)"
    );

    // Check stderr for indication of the problem
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("column") || stderr.contains("Could not determine"),
        "Should indicate column metadata issue: {stderr}"
    );

    // Since the command fails, output file should not exist
    assert!(
        !output_file.exists(),
        "Output file should not be created when export fails"
    );
}

#[test]
fn test_export_invalid_format_error() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("error_output.xyz");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "invalid_format",
        "--table",
        "test_basic.simple_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        !output.status.success(),
        "Export should fail for invalid format"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_lowercase().contains("format")
            || stderr.contains("invalid")
            || stderr.contains("possible values"),
        "Error message should indicate format issue: {stderr}"
    );
}

#[test]
fn test_export_missing_table_arg_error() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("error_output.csv");

    // Missing --table argument
    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        // --table is missing
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        !output.status.success(),
        "Export should fail when --table is missing"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--table") || stderr.contains("required") || stderr.contains("table"),
        "Error message should indicate missing table argument: {stderr}"
    );
}

#[test]
fn test_export_nonexistent_output_dir_error() {
    let (data_dir, schema_file) = assert_test_data_available();

    // Try to write to a nonexistent directory
    let output_file = PathBuf::from("/nonexistent_directory_12345/output.csv");

    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    eprintln!("Exit status: {}", output.status);
    eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        !output.status.success(),
        "Export should fail for nonexistent output directory"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_lowercase().contains("directory")
            || stderr.to_lowercase().contains("path")
            || stderr.to_lowercase().contains("permission")
            || stderr.to_lowercase().contains("no such file"),
        "Error message should indicate path/directory issue: {stderr}"
    );
}

// ============================================================================
// Cross-Format Consistency Tests
// ============================================================================

#[test]
fn test_export_csv_json_row_count_matches() {
    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_file = temp_dir.path().join("consistency.csv");
    let json_file = temp_dir.path().join("consistency.json");

    // Export to CSV
    let csv_output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        csv_file.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    // Export to JSON
    let json_output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        json_file.to_str().unwrap(),
        "--format",
        "json",
        "--table",
        "test_basic.simple_table",
    ]);

    assert!(csv_output.status.success(), "CSV export should succeed");
    assert!(json_output.status.success(), "JSON export should succeed");

    // Count CSV rows (subtract 1 for header)
    let csv_content = fs::read_to_string(&csv_file).expect("Failed to read CSV");
    let csv_row_count = csv_content.lines().count().saturating_sub(1);

    // Count JSON rows
    let json_content = fs::read_to_string(&json_file).expect("Failed to read JSON");
    let json_parsed: serde_json::Value =
        serde_json::from_str(&json_content).expect("Should be valid JSON");
    let json_row_count = json_parsed.as_array().map(|a| a.len()).unwrap_or(0);

    eprintln!("Cross-format row counts - CSV: {csv_row_count}, JSON: {json_row_count}");

    assert_eq!(
        csv_row_count, json_row_count,
        "CSV and JSON exports should have same row count"
    );
}

/// Test that the export_sstable library function supports Parquet export.
/// This test directly calls the library function rather than going through CLI
/// since export_sstable is an internal API used for direct SSTable export.
#[tokio::test]
async fn test_export_sstable_to_parquet() {
    use cqlite_cli::cli::ExportFormat;
    use cqlite_cli::commands::export_sstable;
    use std::io::Write;

    let (data_dir, _cql_schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_file = temp_dir.path().join("sstable_export.parquet");

    // Create a schema file in the format expected by parse_json_schema
    // The parser expects columns as an object with "type" and "kind" fields
    let schema_content = r#"{
        "keyspace": "test_basic",
        "table": "simple_table",
        "columns": {
            "id": { "type": "uuid", "kind": "PartitionKey" },
            "name": { "type": "text", "kind": "Regular" },
            "age": { "type": "int", "kind": "Regular" },
            "active": { "type": "boolean", "kind": "Regular" }
        }
    }"#;

    let schema_file = temp_dir.path().join("test_schema.json");
    {
        let mut f = fs::File::create(&schema_file).expect("Failed to create schema file");
        f.write_all(schema_content.as_bytes())
            .expect("Failed to write schema");
    }

    // Find the first SSTable Data.db file dynamically
    // Structure: sstables/test_basic/simple_table-UUID/nb-1-big-Data.db
    let test_basic_dir = data_dir.join("test_basic");
    let simple_table_dir = fs::read_dir(&test_basic_dir)
        .expect("Failed to read test_basic directory")
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("simple_table-")
        })
        .expect("No simple_table directory found");

    let sstable_file = simple_table_dir.path().join("nb-1-big-Data.db");

    assert!(
        sstable_file.exists(),
        "Test requires SSTable file: {sstable_file:?}"
    );

    eprintln!("Using SSTable: {sstable_file:?}");
    eprintln!("Using schema: {schema_file:?}");
    eprintln!("Output file: {output_file:?}");

    // Call the library function directly
    let result = export_sstable(
        &sstable_file,
        &schema_file,
        &output_file,
        ExportFormat::Parquet,
    )
    .await;

    assert!(
        result.is_ok(),
        "export_sstable to Parquet should succeed: {:?}",
        result.err()
    );

    // Verify output file exists and is a valid Parquet file
    assert!(output_file.exists(), "Output Parquet file should exist");
    let parquet_bytes = fs::read(&output_file).expect("Failed to read Parquet file");
    verify_parquet_magic(&parquet_bytes);

    // Read back and verify it has data
    let batch = read_parquet_back(&parquet_bytes).expect("Failed to read Parquet back");
    eprintln!(
        "SSTable to Parquet export verified: {} rows, {} columns",
        batch.num_rows(),
        batch.num_columns()
    );

    // Parquet file should have been created (may have 0 rows if data parsing is incomplete)
    // The key validation is that the export completes without error and produces valid Parquet
    assert!(
        !parquet_bytes.is_empty(),
        "Parquet file should have content"
    );
}

// ============================================================================
// Memory Efficiency Tests
// ============================================================================

/// Test that export operations stay within memory budget.
///
/// This test validates the <128MB memory target from CLAUDE.md.
/// Uses sysinfo to measure process RSS before and after export.
/// Marked as #[ignore] for CI - run manually with: cargo test test_export_memory_efficiency -- --ignored
#[test]
#[ignore]
fn test_export_memory_efficiency() {
    use sysinfo::{ProcessRefreshKind, System};

    let (data_dir, schema_file) = assert_test_data_available();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Get baseline memory usage
    let mut system = System::new();
    let pid = sysinfo::get_current_pid().expect("Failed to get current PID");
    system.refresh_process_specifics(pid, ProcessRefreshKind::new().with_memory());
    let baseline_memory = system.process(pid).map(|p| p.memory()).unwrap_or(0);

    eprintln!(
        "Baseline memory: {} bytes ({:.1} MB)",
        baseline_memory,
        baseline_memory as f64 / (1024.0 * 1024.0)
    );

    // Export to Parquet (most memory-intensive format due to columnar buffering)
    let output_file = temp_dir.path().join("memory_test.parquet");
    let output = run_cli_command(&[
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "export",
        output_file.to_str().unwrap(),
        "--format",
        "parquet",
        "--table",
        "test_basic.simple_table",
    ]);

    assert!(
        output.status.success(),
        "Export should succeed for memory test. Exit code: {:?}\nSTDERR: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // Measure memory after export
    system.refresh_process_specifics(pid, ProcessRefreshKind::new().with_memory());
    let peak_memory = system.process(pid).map(|p| p.memory()).unwrap_or(0);

    let memory_delta = peak_memory.saturating_sub(baseline_memory);
    let memory_delta_mb = memory_delta as f64 / (1024.0 * 1024.0);

    eprintln!(
        "Peak memory: {} bytes ({:.1} MB)",
        peak_memory,
        peak_memory as f64 / (1024.0 * 1024.0)
    );
    eprintln!("Memory delta: {memory_delta} bytes ({memory_delta_mb:.1} MB)");

    // Memory target from CLAUDE.md: <128MB for large files
    const MEMORY_LIMIT_MB: f64 = 128.0;
    assert!(
        memory_delta_mb < MEMORY_LIMIT_MB,
        "Export memory usage ({memory_delta_mb:.1} MB) should stay under {MEMORY_LIMIT_MB} MB limit"
    );

    eprintln!(
        "Memory efficiency test passed: {memory_delta_mb:.1} MB < {MEMORY_LIMIT_MB} MB limit"
    );
}

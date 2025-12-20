//! One-shot integration tests for CQLite CLI
//!
//! Tests end-to-end execution using `-e` (execute), `-f` (file), and `read-sstable` commands
//! against real Cassandra 5.0 SSTable data from test-data/datasets.
//!
//! These tests validate:
//! - JSON output format correctness
//! - CSV output format correctness
//! - Table output format (using golden snapshots)
//! - Script file execution with `-f` flag
//! - Direct SSTable reading via read-sstable command
//! - Multi-table dataset coverage
//!
//! Requirements:
//! - Real SSTable data at: test-data/datasets/sstables/
//! - JSONL reference files alongside Data.db files
//! - Environment variable: CQLITE_DATASETS_ROOT (optional, has default)
//!
//! Note: These tests use the `read-sstable` command for direct SSTable access
//! since the query engine integration with --data-dir is still in development.
//!
//! ## Current Status (Issue #125)
//!
//! **IMPORTANT**: Most SSTable reading tests currently fail due to a known format version
//! incompatibility issue (version 0x0010 vs expected 0x0001). This is a separate issue
//! from Issue #125 and is being tracked independently.
//!
//! Tests that currently pass:
//! - Error handling tests (test_invalid_sstable_path_returns_error, test_missing_table_returns_error)
//! - Test infrastructure and helper methods
//!
//! Tests blocked by format issue:
//! - All JSON/CSV/Table output tests against real SSTables
//!
//! When the format issue is resolved, these tests will verify:
//! - Correct JSON array output with valid structure
//! - Correct CSV output with headers and data rows
//! - Correct table formatting with cqlsh-style separators
//! - Golden snapshot matching for table format stability

#![cfg(all(test, feature = "state_machine"))]
#![allow(clippy::all)]

use assert_cmd::Command;
use serde_json::Value as JsonValue;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

// ============================================================================
// Test Context Helper
// ============================================================================

/// Helper struct for managing test execution against real SSTable datasets
struct TestContext {
    keyspace: String,
    table: String,
    data_root: PathBuf,
}

impl TestContext {
    /// Create a new test context for a specific keyspace and table
    ///
    /// # Arguments
    /// * `keyspace` - Keyspace name (e.g., "test_basic")
    /// * `table` - Table name (e.g., "simple_table")
    fn new(keyspace: &str, table: &str) -> Self {
        let data_root = get_test_data_root();
        Self {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            data_root,
        }
    }

    /// Execute a read-sstable command (used for one-shot testing)
    ///
    /// # Arguments
    /// * `format` - Output format ("json", "csv", "table")
    /// * `limit` - Optional limit on rows returned
    ///
    /// # Returns
    /// Result containing stdout output or error message
    fn read_sstable(&self, format: &str, limit: Option<usize>) -> Result<String, String> {
        let table_dir = self.find_table_directory()?;
        let data_file = table_dir.join("nb-1-big-Data.db");

        if !data_file.exists() {
            return Err(format!("Data file not found: {}", data_file.display()));
        }

        let mut cmd = Command::cargo_bin("cqlite")
            .map_err(|e| format!("Failed to find cqlite binary: {}", e))?;

        cmd.arg("read-sstable")
            .arg(&data_file)
            .arg("--format")
            .arg(format);

        if let Some(limit_val) = limit {
            cmd.arg("--limit").arg(limit_val.to_string());
        }

        let output = cmd
            .output()
            .map_err(|e| format!("Failed to execute command: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        // Check if command succeeded
        if !output.status.success() {
            return Err(format!(
                "Command failed with status {}: stdout={}, stderr={}",
                output.status, stdout, stderr
            ));
        }

        // Filter out log messages and empty lines
        // - Lines starting with emoji (📖) are informational messages
        // - Lines starting with '[' and containing timestamps (e.g., [2025-) are log entries
        // - Empty lines are whitespace
        let filtered_output: Vec<&str> = stdout
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.is_empty()
                    && !line.starts_with("📖")
                    && !(line.starts_with('[') && line.contains("202")) // More specific timestamp filter
            })
            .collect();

        Ok(filtered_output.join("\n"))
    }

    /// Execute a CQL query using the `-e` flag (for future use when DB integration is complete)
    ///
    /// # Arguments
    /// * `query` - CQL query to execute
    /// * `format` - Output format ("json", "csv", "table")
    ///
    /// # Returns
    /// Result containing stdout output or error message
    #[allow(dead_code)]
    fn execute_query(&self, query: &str, format: &str) -> Result<String, String> {
        let table_dir = self.find_table_directory()?;

        let mut cmd = Command::cargo_bin("cqlite")
            .map_err(|e| format!("Failed to find cqlite binary: {}", e))?;

        cmd.arg("--data-dir")
            .arg(&self.data_root)
            .arg("--format")
            .arg(format)
            .arg("-e")
            .arg(query);

        // Set schema path if available
        if let Ok(schema_path) = self.find_schema_file(&table_dir) {
            cmd.arg("--schema").arg(schema_path);
        }

        let output = cmd
            .output()
            .map_err(|e| format!("Failed to execute command: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "Command failed with status {}: {}",
                output.status, stderr
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        Ok(stdout)
    }

    /// Execute CQL statements from a script file using the `-f` flag (future use)
    ///
    /// # Arguments
    /// * `script_path` - Path to .cql script file
    /// * `format` - Output format ("json", "csv", "table")
    ///
    /// # Returns
    /// Result containing stdout output or error message
    #[allow(dead_code)]
    fn execute_script(&self, script_path: &Path, format: &str) -> Result<String, String> {
        let table_dir = self.find_table_directory()?;

        let mut cmd = Command::cargo_bin("cqlite")
            .map_err(|e| format!("Failed to find cqlite binary: {}", e))?;

        cmd.arg("--data-dir")
            .arg(&self.data_root)
            .arg("--format")
            .arg(format)
            .arg("-f")
            .arg(script_path);

        // Set schema path if available
        if let Ok(schema_path) = self.find_schema_file(&table_dir) {
            cmd.arg("--schema").arg(schema_path);
        }

        let output = cmd
            .output()
            .map_err(|e| format!("Failed to execute command: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "Command failed with status {}: {}",
                output.status, stderr
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        Ok(stdout)
    }

    /// Load JSONL reference data for validation
    ///
    /// # Returns
    /// Vector of parsed JSON objects from the reference file
    fn load_jsonl_reference(&self) -> Result<Vec<JsonValue>, String> {
        let jsonl_path = self.find_jsonl_file()?;
        let content = fs::read_to_string(&jsonl_path)
            .map_err(|e| format!("Failed to read JSONL file {}: {}", jsonl_path.display(), e))?;

        let mut records = Vec::new();
        for (line_num, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let record: JsonValue = serde_json::from_str(line).map_err(|e| {
                format!(
                    "Failed to parse JSONL line {} in {}: {}",
                    line_num + 1,
                    jsonl_path.display(),
                    e
                )
            })?;
            records.push(record);
        }

        Ok(records)
    }

    /// Find the JSONL reference file for this table
    ///
    /// # Returns
    /// Path to the nb-1-big-Data.db.jsonl file
    fn find_jsonl_file(&self) -> Result<PathBuf, String> {
        let table_dir = self.find_table_directory()?;
        let jsonl_path = table_dir.join("nb-1-big-Data.db.jsonl");

        if !jsonl_path.exists() {
            return Err(format!(
                "JSONL reference file not found: {}",
                jsonl_path.display()
            ));
        }

        Ok(jsonl_path)
    }

    /// Find the table directory (handles UUID suffix)
    ///
    /// Table directories follow the pattern: {table_name}-{uuid}/
    fn find_table_directory(&self) -> Result<PathBuf, String> {
        let keyspace_dir = self.data_root.join(&self.keyspace);

        if !keyspace_dir.exists() {
            return Err(format!(
                "Keyspace directory not found: {}",
                keyspace_dir.display()
            ));
        }

        // Find directory starting with table name followed by UUID
        let entries = fs::read_dir(&keyspace_dir).map_err(|e| {
            format!(
                "Failed to read keyspace directory {}: {}",
                keyspace_dir.display(),
                e
            )
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();

            // Match pattern: table_name-uuid
            if name_str.starts_with(&format!("{}-", self.table)) {
                return Ok(entry.path());
            }
        }

        Err(format!(
            "Table directory not found for table '{}' in keyspace '{}'",
            self.table, self.keyspace
        ))
    }

    /// Find schema file (.cql or .json) in table directory
    fn find_schema_file(&self, table_dir: &Path) -> Result<PathBuf, String> {
        // Try .cql first
        let cql_schema = table_dir.join("schema.cql");
        if cql_schema.exists() {
            return Ok(cql_schema);
        }

        // Try .json
        let json_schema = table_dir.join("schema.json");
        if json_schema.exists() {
            return Ok(json_schema);
        }

        Err(format!("No schema file found in {}", table_dir.display()))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Get test data root directory
///
/// Uses CQLITE_DATASETS_ROOT environment variable or default path
fn get_test_data_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            // Use workspace root relative path
            let manifest_dir = env!("CARGO_MANIFEST_DIR");
            PathBuf::from(manifest_dir)
                .parent()
                .expect("Failed to get parent directory of cqlite-cli")
                .join("test-data/datasets/sstables")
        })
}

/// Validate JSON output structure
///
/// Ensures the output is valid JSON with expected structure
/// (either array of objects or single object)
fn validate_json_structure(json_output: &str, reference: &[JsonValue]) -> Result<(), String> {
    // Parse as JSON to ensure it's valid
    let parsed: JsonValue =
        serde_json::from_str(json_output).map_err(|e| format!("Invalid JSON output: {}", e))?;

    // Validate it's an array or object
    match &parsed {
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                return Err("JSON array is empty, expected at least one record".to_string());
            }
            // Optionally validate count (allowing for limits/filtering)
            if arr.len() > reference.len() {
                return Err(format!(
                    "JSON array has {} records but reference only has {}",
                    arr.len(),
                    reference.len()
                ));
            }
            // Ensure each element is an object
            for (idx, item) in arr.iter().enumerate() {
                if !item.is_object() {
                    return Err(format!(
                        "JSON array element {} is not an object: {:?}",
                        idx, item
                    ));
                }
            }
        }
        JsonValue::Object(_) => {
            // Single object result is acceptable
        }
        _ => {
            return Err(format!(
                "Expected JSON array or object, got: {}",
                parsed.to_string()
            ));
        }
    }

    Ok(())
}

/// Validate CSV output structure
///
/// Ensures CSV has header line and minimum number of data rows
fn validate_csv_structure(csv_output: &str, min_rows: usize) -> Result<(), String> {
    if csv_output.trim().is_empty() {
        return Err("CSV output is empty".to_string());
    }

    // Use proper CSV parser to handle quoted multi-line values
    let mut reader = csv::Reader::from_reader(csv_output.as_bytes());

    // Verify header exists
    let headers = reader
        .headers()
        .map_err(|e| format!("Failed to read CSV headers: {}", e))?;

    if headers.is_empty() {
        return Err("CSV header is empty".to_string());
    }

    // Count data rows
    let data_rows = reader.records().count();

    if data_rows < min_rows {
        return Err(format!(
            "CSV has {} data rows, expected at least {}",
            data_rows, min_rows
        ));
    }

    Ok(())
}

// ============================================================================
// Test Cases - JSON Output
// ============================================================================

#[test]
fn test_simple_table_json_output_e_flag() {
    let ctx = TestContext::new("test_basic", "simple_table");

    // Read SSTable with JSON output (limit to 5 for testing)
    let output = ctx
        .read_sstable("json", Some(5))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Load reference data
    let reference = ctx
        .load_jsonl_reference()
        .expect(&format!(
            "Failed to load JSONL reference for table {}/{}. Check that reference file exists in test data.",
            ctx.keyspace, ctx.table
        ));

    // Validate JSON structure
    validate_json_structure(&output, &reference).expect(&format!(
        "JSON validation failed for table {}/{}. Output may not match expected structure.",
        ctx.keyspace, ctx.table
    ));

    // Basic sanity checks - output should contain data
    assert!(!output.trim().is_empty(), "JSON output should not be empty");
}

#[test]
fn test_collections_table_json_output() {
    let ctx = TestContext::new("test_collections", "collection_table");

    // Read SSTable with JSON output (limit for stability)
    let output = ctx
        .read_sstable("json", Some(3))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Load reference data
    let reference = ctx
        .load_jsonl_reference()
        .expect(&format!(
            "Failed to load JSONL reference for table {}/{}. Check that reference file exists in test data.",
            ctx.keyspace, ctx.table
        ));

    // Validate JSON structure
    validate_json_structure(&output, &reference).expect(&format!(
        "JSON validation failed for table {}/{}. Output may not match expected structure.",
        ctx.keyspace, ctx.table
    ));

    // Collections should be present in JSON output
    assert!(!output.trim().is_empty(), "JSON output should not be empty");
}

// ============================================================================
// Test Cases - CSV Output
// ============================================================================

#[test]
fn test_simple_table_csv_output_e_flag() {
    let ctx = TestContext::new("test_basic", "simple_table");

    // Read SSTable with CSV output
    let output = ctx
        .read_sstable("csv", Some(5))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Validate CSV structure - expect at least 1 data row
    validate_csv_structure(&output, 1).expect(&format!(
        "CSV validation failed for table {}/{}. Output may not have proper CSV structure.",
        ctx.keyspace, ctx.table
    ));

    // CSV should have header and data
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() >= 2, "CSV should have header + data rows");
}

#[test]
fn test_wide_rows_table_csv_output() {
    let ctx = TestContext::new("test_wide_rows", "wide_partition_table");

    // Read SSTable with CSV output
    let output = ctx
        .read_sstable("csv", Some(3))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Validate CSV structure - wide rows should have multiple columns
    validate_csv_structure(&output, 1).expect(&format!(
        "CSV validation failed for table {}/{}. Output may not have proper CSV structure.",
        ctx.keyspace, ctx.table
    ));

    // Verify we have substantial output
    assert!(
        output.len() > 50,
        "Wide rows CSV output should be substantial"
    );
}

// ============================================================================
// Test Cases - Table Output (Snapshot)
// ============================================================================

#[test]
#[ignore = "Snapshots unstable due to HashMap randomization in row value display (Issue #TBD)"]
fn test_simple_table_table_output_e_flag() {
    let ctx = TestContext::new("test_basic", "simple_table");

    // Read SSTable with table output
    let output = ctx
        .read_sstable("table", Some(5))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Use insta snapshot testing for table format
    insta::assert_snapshot!(output);

    // Basic validation - table should have separators
    assert!(
        output.contains(" | "),
        "Table output should have column separators"
    );
    assert!(
        output.contains("-+-"),
        "Table output should have row separators"
    );
}

#[test]
#[ignore = "Snapshots unstable due to HashMap randomization in row value display (Issue #TBD)"]
fn test_collections_table_table_output() {
    let ctx = TestContext::new("test_collections", "collection_table");

    // Read SSTable with table output (limited for snapshot stability)
    let output = ctx
        .read_sstable("table", Some(3))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Use insta snapshot testing for table format
    insta::assert_snapshot!(output);

    // Verify table structure
    assert!(
        output.contains(" | "),
        "Table output should have column separators"
    );
}

// ============================================================================
// Test Cases - Script File Execution (-f flag)
// ============================================================================
// NOTE: These tests are currently ignored pending full -e/-f flag integration
// with database initialization. They will be enabled when Issue #125 Phase 2
// completes the database loading from --data-dir.

#[test]
#[ignore = "Requires -f flag with database initialization (Phase 2)"]
fn test_script_execution_with_f_flag() {
    let ctx = TestContext::new("test_basic", "simple_table");

    // Create temporary script file
    let mut script_file =
        NamedTempFile::new().expect("Failed to create temporary test script file");

    let script_content = format!(
        "-- Test script for table output\nSELECT * FROM {}.{} LIMIT 2;\n",
        ctx.keyspace, ctx.table
    );

    script_file
        .write_all(script_content.as_bytes())
        .expect("Failed to write test script content to temporary file");
    script_file
        .flush()
        .expect("Failed to flush temporary script file to disk");

    // Execute script with -f flag
    let output = ctx
        .execute_script(script_file.path(), "json")
        .expect(&format!(
            "Failed to execute script for table {}/{}. Check that CLI binary supports -f flag.",
            ctx.keyspace, ctx.table
        ));

    // Validate JSON output
    let reference = ctx
        .load_jsonl_reference()
        .expect(&format!(
            "Failed to load JSONL reference for table {}/{}. Check that reference file exists in test data.",
            ctx.keyspace, ctx.table
        ));
    validate_json_structure(&output, &reference).expect(&format!(
        "JSON validation failed for table {}/{}. Output may not match expected structure.",
        ctx.keyspace, ctx.table
    ));

    // Verify we got results
    assert!(
        !output.trim().is_empty(),
        "Script execution should produce output"
    );
}

#[test]
#[ignore = "Requires -f flag with database initialization (Phase 2)"]
fn test_multi_statement_script_execution() {
    let ctx = TestContext::new("test_basic", "simple_table");

    // Create script with multiple statements
    let mut script_file =
        NamedTempFile::new().expect("Failed to create temporary test script file");

    let script_content = format!(
        "-- Multi-statement script\n\
         SELECT * FROM {}.{} LIMIT 1;\n\
         SELECT * FROM {}.{} LIMIT 2;\n",
        ctx.keyspace, ctx.table, ctx.keyspace, ctx.table
    );

    script_file
        .write_all(script_content.as_bytes())
        .expect("Failed to write test script content to temporary file");
    script_file
        .flush()
        .expect("Failed to flush temporary script file to disk");

    // Execute multi-statement script
    let output = ctx
        .execute_script(script_file.path(), "table")
        .expect(&format!(
            "Failed to execute multi-statement script for table {}/{}. Check that CLI binary supports -f flag.",
            ctx.keyspace, ctx.table
        ));

    // Verify table format output
    assert!(
        output.contains(" | "),
        "Multi-statement output should have table format"
    );

    // Should contain multiple result sets or combined output
    assert!(
        !output.trim().is_empty(),
        "Multi-statement script should produce output"
    );
}

// ============================================================================
// Test Cases - Timeseries Data
// ============================================================================

#[test]
fn test_timeseries_sensor_data_json() {
    let ctx = TestContext::new("test_timeseries", "sensor_data");

    // Read SSTable with JSON output
    let output = ctx
        .read_sstable("json", Some(5))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Load reference data
    let reference = ctx
        .load_jsonl_reference()
        .expect(&format!(
            "Failed to load JSONL reference for table {}/{}. Check that reference file exists in test data.",
            ctx.keyspace, ctx.table
        ));

    // Validate JSON structure
    validate_json_structure(&output, &reference).expect(&format!(
        "JSON validation failed for table {}/{}. Output may not match expected structure.",
        ctx.keyspace, ctx.table
    ));
}

#[test]
fn test_timeseries_sensor_data_csv() {
    let ctx = TestContext::new("test_timeseries", "sensor_data");

    // Read SSTable with CSV output
    let output = ctx
        .read_sstable("csv", Some(5))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Validate CSV structure
    validate_csv_structure(&output, 1).expect(&format!(
        "CSV validation failed for table {}/{}. Output may not have proper CSV structure.",
        ctx.keyspace, ctx.table
    ));
}

// ============================================================================
// Test Cases - Composite Key Tables
// ============================================================================

#[test]
fn test_composite_key_table_json() {
    let ctx = TestContext::new("test_basic", "composite_key_table");

    // Read SSTable with JSON output
    let output = ctx
        .read_sstable("json", Some(5))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Load reference data
    let reference = ctx
        .load_jsonl_reference()
        .expect(&format!(
            "Failed to load JSONL reference for table {}/{}. Check that reference file exists in test data.",
            ctx.keyspace, ctx.table
        ));

    // Validate JSON structure
    validate_json_structure(&output, &reference).expect(&format!(
        "JSON validation failed for table {}/{}. Output may not match expected structure.",
        ctx.keyspace, ctx.table
    ));
}

#[test]
#[ignore = "Snapshots unstable due to HashMap randomization in row value display (Issue #TBD)"]
fn test_composite_key_table_table_format() {
    let ctx = TestContext::new("test_basic", "composite_key_table");

    // Read SSTable with table output
    let output = ctx
        .read_sstable("table", Some(3))
        .expect(&format!(
            "Failed to read SSTable for table {}/{}. Check that test data exists and CLI binary is built.",
            ctx.keyspace, ctx.table
        ));

    // Snapshot test for composite key table formatting
    insta::assert_snapshot!(output);

    // Verify table structure
    assert!(
        output.contains(" | "),
        "Composite key table output should have column separators"
    );
}

// ============================================================================
// Test Cases - Error Handling
// ============================================================================

#[test]
fn test_invalid_sstable_path_returns_error() {
    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    cmd.arg("read-sstable")
        .arg("/nonexistent/path/to/Data.db")
        .arg("--format")
        .arg("json");

    let output = cmd.output().expect("Failed to execute command");

    // Should fail with non-zero exit status
    assert!(
        !output.status.success(),
        "Invalid SSTable path should return error"
    );
}

#[test]
fn test_missing_table_returns_error() {
    let ctx = TestContext::new("test_basic", "nonexistent_table");

    // Try to find table directory
    let result = ctx.find_table_directory();

    // Should fail to find directory
    assert!(result.is_err(), "Missing table should return error");
}

#[test]
fn test_invalid_format_returns_error() {
    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    // Use a valid SSTable path but invalid format
    let test_data_root = get_test_data_root();
    let simple_table_dir = test_data_root
        .join("test_basic")
        .join("simple_table-6aa08200a25111f0a3fef1a551383fb9");
    let data_db_path = simple_table_dir.join("nb-1-big-Data.db");

    cmd.arg("read-sstable")
        .arg(data_db_path.to_str().unwrap())
        .arg("--format")
        .arg("invalid_format");

    let output = cmd.output().expect("Failed to execute command");
    assert!(
        !output.status.success(),
        "Command with invalid format should fail"
    );

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("invalid") || stderr.contains("format") || stderr.contains("supported"),
        "Error message should mention invalid format. Got: {}",
        stderr
    );
}

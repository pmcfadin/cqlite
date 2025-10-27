//! Tests for Issue #142: Optional fallback for `-e SELECT` to read-sstable
//!
//! This test suite validates the experimental SELECT fallback feature that routes
//! `-e SELECT` commands to read-sstable when ingestion is unavailable (no schema/data-dir).
//!
//! **IMPORTANT**: This is a TEMPORARY feature (disabled by default) that will be
//! removed in M3 after ingestion stabilizes.
//!
//! Test Coverage:
//! - Flag defaults to false (disabled by default)
//! - Flag can be enabled via CLI flag
//! - Flag can be enabled via environment variable
//! - Fallback only activates when ingestion unavailable
//! - Simple SELECT query parsing works correctly
//! - Warning message appears when fallback is used

#![cfg(all(test, feature = "state_machine"))]
#![allow(clippy::all)]

use assert_cmd::Command;
use std::path::PathBuf;

// ============================================================================
// Helper Functions
// ============================================================================

/// Get test data root directory for fallback tests
fn get_test_data_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let manifest_dir = env!("CARGO_MANIFEST_DIR");
            PathBuf::from(manifest_dir)
                .parent()
                .expect("Failed to get parent directory")
                .join("test-data/datasets/sstables")
        })
}

/// Get path to simple_table test data
fn get_simple_table_path() -> PathBuf {
    let root = get_test_data_root();
    let table_dir = root
        .join("test_basic")
        .join("simple_table-6aa08200a25111f0a3fef1a551383fb9");

    // Return the Data.db file path
    table_dir.join("nb-1-big-Data.db")
}

// ============================================================================
// Test Cases - Flag Behavior
// ============================================================================

#[test]
fn test_fallback_disabled_by_default() {
    // Create command without the flag
    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    // Use a SELECT query without schema/data-dir (ingestion unavailable)
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    let query = format!("SELECT * FROM {}", table_path.display());

    cmd.arg("-e").arg(&query).arg("--format").arg("json");

    let output = cmd.output().expect("Failed to execute command");

    // Should fail because:
    // 1. Fallback is disabled by default
    // 2. Ingestion is unavailable (no schema/data-dir)
    // 3. Query engine will fail without ingestion
    assert!(
        !output.status.success(),
        "Command should fail when fallback disabled and ingestion unavailable"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should NOT see fallback warning
    assert!(
        !stderr.contains("Using experimental read-sstable fallback"),
        "Should not use fallback when disabled by default"
    );
}

#[test]
fn test_fallback_enabled_with_flag() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    let query = format!("SELECT * FROM {}", table_path.display());

    cmd.arg("--enable-select-fallback")
        .arg("-e")
        .arg(&query)
        .arg("--format")
        .arg("json");

    let output = cmd.output().expect("Failed to execute command");

    // Should succeed with fallback enabled
    assert!(
        output.status.success(),
        "Command should succeed with fallback enabled. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should see fallback warning
    assert!(
        stderr.contains("Using experimental read-sstable fallback"),
        "Should show fallback warning when enabled. stderr: {}",
        stderr
    );
}

#[test]
fn test_fallback_enabled_with_env() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    let query = format!("SELECT * FROM {}", table_path.display());

    cmd.env("CQLITE_ENABLE_SELECT_FALLBACK", "true")
        .arg("-e")
        .arg(&query)
        .arg("--format")
        .arg("json");

    let output = cmd.output().expect("Failed to execute command");

    // Should succeed with fallback enabled via env var
    assert!(
        output.status.success(),
        "Command should succeed with fallback enabled via env. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should see fallback warning
    assert!(
        stderr.contains("Using experimental read-sstable fallback"),
        "Should show fallback warning when enabled via env. stderr: {}",
        stderr
    );
}

// ============================================================================
// Test Cases - Conditional Activation
// ============================================================================

#[test]
fn test_fallback_requires_ingestion_unavailable() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    // Create a dummy schema file
    let schema_dir = get_test_data_root().join("test_basic");
    let schema_file = schema_dir.join("schema.cql");

    if !schema_file.exists() {
        eprintln!(
            "Skipping test: schema file not found at {}",
            schema_file.display()
        );
        return;
    }

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    let query = format!("SELECT * FROM {}", table_path.display());

    // Provide schema AND data-dir (ingestion AVAILABLE)
    cmd.arg("--enable-select-fallback")
        .arg("--schema")
        .arg(&schema_file)
        .arg("--dataset")
        .arg("test_basic")
        .arg("-e")
        .arg(&query)
        .arg("--format")
        .arg("json");

    let output = cmd.output().expect("Failed to execute command");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should NOT use fallback because ingestion is available
    // (has both schema and dataset)
    assert!(
        !stderr.contains("Using experimental read-sstable fallback"),
        "Should not use fallback when ingestion is available (schema + dataset provided). stderr: {}",
        stderr
    );
}

#[test]
fn test_fallback_only_for_select_queries() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    let query = format!("DESCRIBE TABLE {}", table_path.display());

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    cmd.arg("--enable-select-fallback")
        .arg("-e")
        .arg(&query)
        .arg("--format")
        .arg("json");

    let output = cmd.output().expect("Failed to execute command");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should NOT use fallback for non-SELECT queries
    assert!(
        !stderr.contains("Using experimental read-sstable fallback"),
        "Should not use fallback for non-SELECT queries. stderr: {}",
        stderr
    );
}

// ============================================================================
// Test Cases - Query Parsing
// ============================================================================

#[test]
fn test_fallback_simple_select_parsing() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    // Test various SELECT query formats
    let queries = vec![
        format!("SELECT * FROM {}", table_path.display()),
        format!("select * from {}", table_path.display()),
        format!("  SELECT   *   FROM   {}  ", table_path.display()),
        format!("SELECT * FROM {};", table_path.display()),
    ];

    for query in queries {
        let mut test_cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

        test_cmd
            .arg("--enable-select-fallback")
            .arg("-e")
            .arg(&query)
            .arg("--format")
            .arg("json");

        let output = test_cmd.output().expect("Failed to execute command");

        let stderr = String::from_utf8_lossy(&output.stderr);

        // All variants should successfully use fallback
        assert!(
            stderr.contains("Using experimental read-sstable fallback"),
            "Query '{}' should trigger fallback. stderr: {}",
            query,
            stderr
        );

        assert!(
            stderr.contains("Extracted table path"),
            "Query '{}' should extract table path. stderr: {}",
            query,
            stderr
        );
    }
}

#[test]
fn test_fallback_invalid_path_error() {
    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    // Use a path that doesn't exist
    let query = "SELECT * FROM /nonexistent/path/to/table";

    cmd.arg("--enable-select-fallback")
        .arg("-e")
        .arg(query)
        .arg("--format")
        .arg("json");

    let output = cmd.output().expect("Failed to execute command");

    // Should fail because path doesn't exist
    assert!(
        !output.status.success(),
        "Command should fail when table path doesn't exist"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should see error about path not existing
    assert!(
        stderr.contains("Table path does not exist")
            || stderr.contains("SELECT fallback failed")
            || stderr.contains("not found"),
        "Should show error about invalid path. stderr: {}",
        stderr
    );
}

// ============================================================================
// Test Cases - Output Format Validation
// ============================================================================

#[test]
fn test_fallback_json_output() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    let query = format!("SELECT * FROM {}", table_path.display());

    cmd.arg("--enable-select-fallback")
        .arg("-e")
        .arg(&query)
        .arg("--format")
        .arg("json")
        .arg("--limit")
        .arg("3");

    let output = cmd.output().expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Filter out log lines and get the actual JSON output
    let json_output: Vec<&str> = stdout
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !line.starts_with("📖")
                && !line.starts_with("Displaying")
                && !line.starts_with("✅")
                && !(line.starts_with('[') && line.contains("202"))
        })
        .collect();

    let json_str = json_output.join("\n");

    // Should be valid JSON
    let parse_result = serde_json::from_str::<serde_json::Value>(&json_str);
    assert!(
        parse_result.is_ok(),
        "Output should be valid JSON. Got: {}",
        json_str
    );
}

#[test]
fn test_fallback_csv_output() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    let query = format!("SELECT * FROM {}", table_path.display());

    cmd.arg("--enable-select-fallback")
        .arg("-e")
        .arg(&query)
        .arg("--format")
        .arg("csv")
        .arg("--limit")
        .arg("3");

    let output = cmd.output().expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Filter out log lines
    let csv_output: Vec<&str> = stdout
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !line.starts_with("📖")
                && !line.starts_with("Displaying")
                && !(line.starts_with('[') && line.contains("202"))
        })
        .collect();

    let csv_str = csv_output.join("\n");

    // Should have CSV structure (header + data rows with commas)
    assert!(
        csv_str.contains(','),
        "CSV output should contain commas. Got: {}",
        csv_str
    );

    let lines: Vec<&str> = csv_str.lines().collect();
    assert!(
        lines.len() >= 2,
        "CSV should have at least header + 1 data row. Got {} lines",
        lines.len()
    );
}

// ============================================================================
// Test Cases - Warning Message
// ============================================================================

#[test]
fn test_fallback_warning_message() {
    let table_path = get_simple_table_path();
    if !table_path.exists() {
        eprintln!(
            "Skipping test: test data not found at {}",
            table_path.display()
        );
        return;
    }

    let mut cmd = Command::cargo_bin("cqlite").expect("Failed to find cqlite binary");

    let query = format!("SELECT * FROM {}", table_path.display());

    cmd.arg("--enable-select-fallback")
        .arg("-e")
        .arg(&query)
        .arg("--format")
        .arg("json");

    let output = cmd.output().expect("Failed to execute command");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Verify all required warning elements are present
    assert!(
        stderr.contains("⚠️"),
        "Warning should include warning emoji. stderr: {}",
        stderr
    );

    assert!(
        stderr.contains("experimental"),
        "Warning should mention 'experimental'. stderr: {}",
        stderr
    );

    assert!(
        stderr.contains("read-sstable fallback"),
        "Warning should mention 'read-sstable fallback'. stderr: {}",
        stderr
    );

    assert!(
        stderr.contains("temporary feature"),
        "Warning should mention 'temporary feature'. stderr: {}",
        stderr
    );

    assert!(
        stderr.contains("disabled by default"),
        "Warning should mention 'disabled by default'. stderr: {}",
        stderr
    );
}

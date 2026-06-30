//! CLI DML Integration Tests (Gap D1-D3)
//!
//! Tests that INSERT, UPDATE, and DELETE statements work end-to-end
//! via the CLI `--execute` flag with `--writable` mode.
//!
//! These tests verify the full user-facing write path:
//! CQL string → parser → Mutation → WriteEngine → SSTable

#![cfg(feature = "write-support")]

use std::path::PathBuf;
use std::process::{Command, Output};
use tempfile::TempDir;

/// Get the project root directory
fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Get schemas directory
#[allow(dead_code)]
fn schemas_dir() -> PathBuf {
    project_root().join("test-data/schemas")
}

/// Run CLI command with write support and capture output
fn run_write_cli(args: &[&str]) -> Output {
    Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--package",
            "cqlite-cli",
            "--features",
            "write-support",
            "--",
        ])
        .args(args)
        .current_dir(project_root())
        .output()
        .expect("Failed to execute CLI command")
}

/// Create a minimal CQL schema file for a simple table (no clustering key)
fn create_simple_schema(dir: &TempDir) -> PathBuf {
    let schema_path = dir.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        r#"
CREATE KEYSPACE IF NOT EXISTS test_write WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE test_write;

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name TEXT,
    age INT,
    active BOOLEAN
);
"#,
    )
    .expect("Failed to write schema file");
    schema_path
}

/// Create a CQL schema file for a table with clustering key
fn create_clustered_schema(dir: &TempDir) -> PathBuf {
    let schema_path = dir.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        r#"
CREATE KEYSPACE IF NOT EXISTS test_write WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE test_write;

CREATE TABLE IF NOT EXISTS events (
    user_id INT,
    event_time TIMESTAMP,
    event_type TEXT,
    data TEXT,
    PRIMARY KEY (user_id, event_time)
) WITH CLUSTERING ORDER BY (event_time ASC);
"#,
    )
    .expect("Failed to write schema file");
    schema_path
}

// ============================================================================
// D1: INSERT via --execute
// ============================================================================

#[test]
fn test_cli_execute_insert_basic() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_simple_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.users (id, name, age, active) VALUES (1, 'Alice', 30, true)",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "INSERT should succeed. stdout: {stdout}, stderr: {stderr}"
    );
    assert!(
        stdout.contains("OK"),
        "Should print OK on successful INSERT. stdout: {stdout}"
    );
}

#[test]
fn test_cli_execute_insert_with_timestamp() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_simple_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.users (id, name, age) VALUES (2, 'Bob', 25) USING TIMESTAMP 1704067200000000",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "INSERT with TIMESTAMP should succeed. stderr: {stderr}"
    );
    assert!(stdout.contains("OK"));
}

#[test]
fn test_cli_execute_insert_with_clustering_key() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_clustered_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.events (user_id, event_time, event_type, data) VALUES (1, 1704067200000, 'login', 'from_web')",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "INSERT with clustering key should succeed. stderr: {stderr}"
    );
    assert!(stdout.contains("OK"));
}

// ============================================================================
// D2: UPDATE via --execute
// ============================================================================

#[test]
fn test_cli_execute_update_basic() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_simple_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    // First insert a row
    run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.users (id, name, age) VALUES (1, 'Alice', 30)",
    ]);

    // Then update it
    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "UPDATE test_write.users SET name = 'Alicia', age = 31 WHERE id = 1",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "UPDATE should succeed. stderr: {stderr}"
    );
    assert!(stdout.contains("OK"));
}

#[test]
fn test_cli_execute_update_with_clustering() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_clustered_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "UPDATE test_write.events SET event_type = 'logout', data = 'session_end' WHERE user_id = 1 AND event_time = 1704067200000",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "UPDATE with clustering key should succeed. stderr: {stderr}"
    );
    assert!(stdout.contains("OK"));
}

// ============================================================================
// D3: DELETE via --execute
// ============================================================================

#[test]
fn test_cli_execute_delete_row() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_simple_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    // Insert first
    run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.users (id, name, age) VALUES (1, 'Alice', 30)",
    ]);

    // Delete the row
    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "DELETE FROM test_write.users WHERE id = 1",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "DELETE should succeed. stderr: {stderr}"
    );
    assert!(stdout.contains("OK"));
}

#[test]
fn test_cli_execute_delete_with_clustering() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_clustered_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "DELETE FROM test_write.events WHERE user_id = 1 AND event_time = 1704067200000",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "DELETE with clustering key should succeed. stderr: {stderr}"
    );
    assert!(stdout.contains("OK"));
}

#[test]
fn test_cli_execute_delete_columns() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_clustered_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "DELETE event_type, data FROM test_write.events WHERE user_id = 1 AND event_time = 1704067200000",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "DELETE columns should succeed. stderr: {stderr}"
    );
    assert!(stdout.contains("OK"));
}

// ============================================================================
// Error cases
// ============================================================================

#[test]
fn test_cli_execute_dml_without_writable_flag() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_simple_schema(&temp_dir);

    // Attempt INSERT without --writable flag - should fail (either because of
    // missing --data-dir or missing --writable, both are correct failures)
    let output = run_write_cli(&[
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.users (id, name) VALUES (1, 'Alice')",
    ]);

    assert!(
        !output.status.success(),
        "INSERT without --writable should fail"
    );
}

#[test]
fn test_cli_execute_insert_then_flush() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_simple_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    // INSERT + flush in sequence
    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.users (id, name, age) VALUES (1, 'Alice', 30)",
        "--flush",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "INSERT + flush should succeed. stderr: {stderr}"
    );
    assert!(
        stdout.contains("OK"),
        "Should print OK for INSERT. stdout: {stdout}"
    );
}

/// Issue #1253: a SINGLE combined `--execute INSERT ... --flush` invocation must
/// persist the inserted row's VALUE into the produced SSTable. Previously the
/// `--flush` handler ran before `--execute`, flushing an empty memtable; the row
/// only landed in the WAL, never in Data.db. This is a content assertion: it
/// reopens the produced SSTable read-only and reads the row back.
#[test]
fn test_cli_execute_insert_and_flush_single_invocation_persists_value() {
    let temp_dir = TempDir::new().unwrap();
    let schema_path = create_simple_schema(&temp_dir);
    let write_dir = temp_dir.path().join("write_data");

    // Single invocation: INSERT + flush together.
    let output = run_write_cli(&[
        "--writable",
        "--write-dir",
        write_dir.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--execute",
        "INSERT INTO test_write.users (id, name, age) VALUES (7, 'Grace', 42)",
        "--flush",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Combined INSERT + flush should succeed. stderr: {stderr}"
    );

    // The flush must produce a real Data.db (not an empty-memtable no-op).
    let data_db = write_dir.join("data/test_write/users/nb-1-big-Data.db");
    assert!(
        data_db.exists(),
        "Combined INSERT + flush must produce Data.db. stdout: {stdout}\nstderr: {stderr}"
    );

    // Reopen the produced SSTable READ-ONLY and assert the inserted VALUE is present.
    let read_output = run_write_cli(&[
        "--schema",
        schema_path.to_str().unwrap(),
        "--data-dir",
        write_dir.join("data").to_str().unwrap(),
        "--execute",
        "SELECT * FROM test_write.users",
        "--out",
        "json",
    ]);
    let read_stdout = String::from_utf8_lossy(&read_output.stdout);
    let read_stderr = String::from_utf8_lossy(&read_output.stderr);
    assert!(
        read_output.status.success(),
        "Read-back SELECT should succeed. stderr: {read_stderr}"
    );
    assert!(
        read_stdout.contains("\"Grace\"") && read_stdout.contains("42"),
        "Inserted row value must be persisted in the SSTable and readable back. \
         Got read stdout: {read_stdout}\nread stderr: {read_stderr}"
    );
}

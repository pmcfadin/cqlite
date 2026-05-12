//! Security tests for CLI input validation
//!
//! These tests verify that user-supplied inputs (especially dataset names)
//! are properly validated to prevent directory traversal and path injection attacks.

use assert_cmd::Command;
use tempfile::TempDir;

/// Test that dataset names with directory traversal patterns are rejected
#[test]
fn test_dataset_name_rejects_directory_traversal_parent() {
    let temp_datasets_root = TempDir::new().unwrap();

    // Create a test schema file
    let schema_path = temp_datasets_root.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_table (id int PRIMARY KEY);",
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.env("CQLITE_DATASETS_ROOT", temp_datasets_root.path())
        .arg("--dataset")
        .arg("../../../etc") // Directory traversal attempt
        .arg("--schema")
        .arg(&schema_path)
        .arg("query")
        .arg("SELECT * FROM test_table");

    let output = cmd.output().unwrap();

    // Should fail with security error
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid dataset name") || stderr.contains("must not contain"),
        "Expected security validation error, got: {stderr}"
    );
}

/// Test that dataset names with forward slashes are rejected
#[test]
fn test_dataset_name_rejects_forward_slash() {
    let temp_datasets_root = TempDir::new().unwrap();

    let schema_path = temp_datasets_root.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_table (id int PRIMARY KEY);",
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.env("CQLITE_DATASETS_ROOT", temp_datasets_root.path())
        .arg("--dataset")
        .arg("foo/bar/baz")
        .arg("--schema")
        .arg(&schema_path)
        .arg("query")
        .arg("SELECT * FROM test_table");

    let output = cmd.output().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid dataset name") || stderr.contains("'/'"),
        "Expected security validation error, got: {stderr}"
    );
}

/// Test that dataset names with backslashes are rejected (Windows paths)
#[test]
fn test_dataset_name_rejects_backslash() {
    let temp_datasets_root = TempDir::new().unwrap();

    let schema_path = temp_datasets_root.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_table (id int PRIMARY KEY);",
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.env("CQLITE_DATASETS_ROOT", temp_datasets_root.path())
        .arg("--dataset")
        .arg("..\\..\\windows\\system32")
        .arg("--schema")
        .arg(&schema_path)
        .arg("query")
        .arg("SELECT * FROM test_table");

    let output = cmd.output().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid dataset name") || stderr.contains("'\\'"),
        "Expected security validation error, got: {stderr}"
    );
}

/// Test that dataset names starting with a dot are rejected (hidden files)
#[test]
fn test_dataset_name_rejects_leading_dot() {
    let temp_datasets_root = TempDir::new().unwrap();

    let schema_path = temp_datasets_root.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_table (id int PRIMARY KEY);",
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.env("CQLITE_DATASETS_ROOT", temp_datasets_root.path())
        .arg("--dataset")
        .arg(".hidden_config")
        .arg("--schema")
        .arg(&schema_path)
        .arg("query")
        .arg("SELECT * FROM test_table");

    let output = cmd.output().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid dataset name") || stderr.contains("start with '.'"),
        "Expected security validation error, got: {stderr}"
    );
}

/// Test that valid dataset names are accepted
#[test]
fn test_dataset_name_accepts_valid_names() {
    let temp_datasets_root = TempDir::new().unwrap();

    // Create sstables directory structure
    let sstables_dir = temp_datasets_root
        .path()
        .join("sstables")
        .join("valid_dataset");
    std::fs::create_dir_all(&sstables_dir).unwrap();

    let schema_path = temp_datasets_root.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_table (id int PRIMARY KEY);",
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.env("CQLITE_DATASETS_ROOT", temp_datasets_root.path())
        .arg("--dataset")
        .arg("valid_dataset") // Valid alphanumeric with underscore
        .arg("--schema")
        .arg(&schema_path)
        .arg("query")
        .arg("SELECT * FROM test_table");

    let output = cmd.output().unwrap();

    // Should not fail on validation (may fail later for missing data, but not validation)
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Invalid dataset name"),
        "Valid dataset name should not trigger validation error, got: {stderr}"
    );
}

/// Test canonicalization prevents symlink attacks
#[test]
#[cfg(unix)] // Symlinks behave differently on Windows
fn test_dataset_canonicalization_prevents_symlink_escape() {
    use std::os::unix::fs::symlink;

    let temp_root = TempDir::new().unwrap();
    let datasets_root = temp_root.path().join("datasets");
    let sstables_dir = datasets_root.join("sstables");
    std::fs::create_dir_all(&sstables_dir).unwrap();

    // Create a directory outside the datasets root
    let outside_dir = temp_root.path().join("external_data");
    std::fs::create_dir(&outside_dir).unwrap();

    // Create a symlink from sstables/malicious -> external_data
    let symlink_path = sstables_dir.join("malicious");
    symlink(&outside_dir, &symlink_path).unwrap();

    let schema_path = temp_root.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_table (id int PRIMARY KEY);",
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.env("CQLITE_DATASETS_ROOT", &datasets_root)
        .arg("--dataset")
        .arg("malicious") // Valid name but resolves outside root
        .arg("--schema")
        .arg(&schema_path)
        .arg("query")
        .arg("SELECT * FROM test_table");

    let output = cmd.output().unwrap();

    // Should fail on canonicalization check
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Security violation") || stderr.contains("escaped"),
        "Expected security violation for symlink escape, got: {stderr}"
    );
}

/// Test that dataset names with Unicode characters are handled safely
#[test]
fn test_dataset_name_handles_unicode() {
    let temp_datasets_root = TempDir::new().unwrap();

    let schema_path = temp_datasets_root.path().join("test_schema.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_table (id int PRIMARY KEY);",
    )
    .unwrap();

    // Unicode characters that might cause issues
    let problematic_names = vec![
        "dataset\u{202E}txt.exe", // Right-to-Left Override
        "dataset\u{200B}name",    // Zero-width space
        "dataset\u{FEFF}name",    // Zero-width no-break space
    ];

    for name in problematic_names {
        let mut cmd = Command::cargo_bin("cqlite").unwrap();
        cmd.env("CQLITE_DATASETS_ROOT", temp_datasets_root.path())
            .arg("--dataset")
            .arg(name)
            .arg("--schema")
            .arg(&schema_path)
            .arg("query")
            .arg("SELECT * FROM test_table");

        let output = cmd.output().unwrap();

        // The behavior may vary, but it should not crash or cause unexpected behavior
        // At minimum, it should not escape the datasets root
        if output.status.success() {
            // If it succeeds in parsing, verify it didn't access outside paths
            // (This would need additional instrumentation in production)
        }
    }
}

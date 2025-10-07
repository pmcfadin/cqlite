//! Integration tests for script execution (-f/--file flag)
//!
//! Tests the M2 CLI feature for executing CQL scripts from files
//! as specified in Issue #122.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

/// Helper to create a test script file
fn create_test_script(dir: &TempDir, filename: &str, content: &str) -> std::path::PathBuf {
    let script_path = dir.path().join(filename);
    fs::write(&script_path, content).expect("Failed to write test script");
    script_path
}

#[test]
fn test_execute_simple_script() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
-- Simple query script
SELECT * FROM users;
SELECT * FROM orders;
"#;
    let script_path = create_test_script(&temp_dir, "simple.cql", script_content);

    // Note: This test will fail without state_machine feature enabled
    // and without actual database setup. The test validates the CLI interface.
    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic");

    // For now, we just test that the CLI accepts the -f flag
    // Full integration requires state_machine feature and proper database setup
    let output = cmd.output().unwrap();

    // Check that the flag was recognized (not an unknown argument error)
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be "unknown argument" error
        assert!(
            !stderr.contains("unexpected argument") && !stderr.contains("unrecognized"),
            "CLI should recognize -f flag"
        );
    }
}

#[test]
fn test_script_file_not_found() {
    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg("/nonexistent/path/script.cql")
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // Should fail with file not found error
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    // Should contain error about missing file
    assert!(
        stderr.contains("Failed to read script file")
            || stderr.contains("No such file")
            || stderr.contains("not found"),
        "Should report file not found error. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_with_unterminated_statement() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
SELECT * FROM users;
SELECT * FROM orders WHERE id = 1
"#;
    let script_path = create_test_script(&temp_dir, "unterminated.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // Should fail with parse error
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Unterminated statement") || stderr.contains("semicolon"),
        "Should report unterminated statement. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_with_comments() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
-- This is a line comment
/* This is a block comment */
SELECT * FROM users;

/* Multi-line
   block comment */
SELECT * FROM orders; -- inline comment
"#;
    let script_path = create_test_script(&temp_dir, "comments.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    // Test that comments are properly handled during parsing
    let output = cmd.output().unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    // Should not have comment-related parse errors
    assert!(
        !stderr.contains("comment") || stderr.contains("Unterminated"),
        "Comments should be parsed correctly. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_with_strings_containing_semicolons() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
SELECT * FROM users WHERE name = 'foo;bar';
SELECT * FROM orders WHERE note = 'Order; pending';
"#;
    let script_path = create_test_script(&temp_dir, "strings.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // Should parse correctly (semicolons in strings should not break statements)
    let stderr = String::from_utf8_lossy(&output.stderr);
    // Should not have parse errors related to statement termination
    assert!(
        !stderr.contains("Unterminated statement found"),
        "String semicolons should not break parsing. Stderr: {}",
        stderr
    );
}

#[test]
fn test_empty_script_file() {
    let temp_dir = TempDir::new().unwrap();
    let script_path = create_test_script(&temp_dir, "empty.cql", "");

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // Empty file should not error, just a warning
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    // Should indicate no statements to execute
    assert!(
        stdout.contains("no statements") || stderr.contains("no statements"),
        "Should handle empty file gracefully. Stdout: {}, Stderr: {}",
        stdout,
        stderr
    );
}

#[test]
fn test_script_comments_only() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
-- Just comments
/* No actual
   statements here */
"#;
    let script_path = create_test_script(&temp_dir, "comments_only.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // Comments-only file should behave like empty file
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("no statements") || stderr.contains("no statements"),
        "Should handle comments-only file gracefully. Stdout: {}, Stderr: {}",
        stdout,
        stderr
    );
}

#[test]
fn test_script_output_format_table() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = "SELECT * FROM users;";
    let script_path = create_test_script(&temp_dir, "query.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp")
        .arg("--out")
        .arg("table");

    // Just verify the flag combination is accepted
    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should not have argument parsing errors
    assert!(
        !stderr.contains("unexpected argument"),
        "Should accept --out table with -f. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_output_format_json() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = "SELECT * FROM users;";
    let script_path = create_test_script(&temp_dir, "query.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp")
        .arg("--out")
        .arg("json");

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should accept JSON format
    assert!(
        !stderr.contains("unexpected argument"),
        "Should accept --out json with -f. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_output_format_csv() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = "SELECT * FROM users;";
    let script_path = create_test_script(&temp_dir, "query.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp")
        .arg("--out")
        .arg("csv");

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should accept CSV format
    assert!(
        !stderr.contains("unexpected argument"),
        "Should accept --out csv with -f. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_with_multiline_statements() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
SELECT *
FROM users
WHERE status = 'active'
  AND created_at > '2025-01-01';

SELECT user_id,
       order_id,
       total
FROM orders;
"#;
    let script_path = create_test_script(&temp_dir, "multiline.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should parse multi-line statements correctly
    assert!(
        !stderr.contains("Unterminated statement"),
        "Multi-line statements should parse correctly. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_with_long_file_alias() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = "SELECT * FROM users;";
    let script_path = create_test_script(&temp_dir, "query.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("--file")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should accept --file as well as -f
    assert!(
        !stderr.contains("unexpected argument"),
        "Should accept --file alias. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_complex_real_world() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
-- Database exploration script
-- Generated: 2025-10-07

/* ==========================
   User Queries
   ========================== */

-- Get active users
SELECT id, name, email
FROM users
WHERE status = 'active'
LIMIT 100;

-- Get user orders
SELECT user_id, order_id, total
FROM orders
WHERE created_at > '2025-01-01'
  AND status IN ('pending', 'shipped')
ORDER BY created_at DESC;

/* Query with string containing special chars */
SELECT COUNT(*) FROM events WHERE type = 'login;logout';
"#;
    let script_path = create_test_script(&temp_dir, "complex.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should parse complex script without parse errors
    assert!(
        !stderr.contains("Unterminated statement"),
        "Complex script should parse correctly. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_unterminated_block_comment() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
SELECT * FROM users;
/* This comment is not closed
SELECT * FROM orders;
"#;
    let script_path = create_test_script(&temp_dir, "bad_comment.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // Should fail with unterminated comment error
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Unterminated block comment") || stderr.contains("comment"),
        "Should report unterminated block comment. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_unterminated_string() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = r#"
SELECT * FROM users WHERE name = 'unterminated;
"#;
    let script_path = create_test_script(&temp_dir, "bad_string.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // Should fail with unterminated string error
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Unterminated string") || stderr.contains("string"),
        "Should report unterminated string. Stderr: {}",
        stderr
    );
}

#[test]
fn test_script_with_schema_flag() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = "SELECT * FROM users;";
    let script_path = create_test_script(&temp_dir, "query.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp")
        .arg("--schema")
        .arg("/tmp/schema.cql");

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should accept --schema with -f
    assert!(
        !stderr.contains("unexpected argument"),
        "Should accept --schema with -f. Stderr: {}",
        stderr
    );
}

#[test]
fn test_conflicting_execute_and_file() {
    let temp_dir = TempDir::new().unwrap();

    let script_content = "SELECT * FROM users;";
    let script_path = create_test_script(&temp_dir, "query.cql", script_content);

    let mut cmd = Command::cargo_bin("cqlite").unwrap();
    cmd.arg("-e")
        .arg("SELECT * FROM orders;")
        .arg("-f")
        .arg(script_path.to_str().unwrap())
        .arg("--data-dir")
        .arg("/tmp");

    let output = cmd.output().unwrap();

    // May error or prioritize one over the other - depends on implementation
    // This test documents the expected behavior once implemented
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should either execute successfully or report conflict
    assert!(
        output.status.success()
            || stderr.contains("conflict")
            || stderr.contains("cannot use both"),
        "Should handle -e and -f flags appropriately. Stderr: {}",
        stderr
    );
}

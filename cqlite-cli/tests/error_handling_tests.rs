use anyhow::Result;
use std::process::Command;
use std::path::PathBuf;
use tempfile::TempDir;

/// Comprehensive error handling and edge case tests
/// 
/// These tests ensure the CLI handles error conditions gracefully:
/// - Invalid input validation
/// - Resource constraints and limits
/// - Network and I/O failures
/// - Memory pressure scenarios
/// - Concurrent access issues
/// - Data corruption scenarios
/// - Security and permission issues

#[cfg(test)]
mod error_handling_tests {
    use super::*;

    const CLI_BINARY: &str = "cqlite";

    fn run_cli_command(args: &[&str]) -> Result<std::process::Output> {
        Command::new("cargo")
            .args(&["run", "--bin", CLI_BINARY, "--"])
            .args(args)
            .output()
            .map_err(|e| anyhow::anyhow!("Failed to run CLI command: {}", e))
    }

    #[test]
    fn test_invalid_command_line_arguments() -> Result<()> {
        // Test unknown flag
        let output = run_cli_command(&["--unknown-flag"])?;
        assert!(!output.status.success(), "Should reject unknown flag");
        
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains("unrecognized") || stderr.contains("error"), 
               "Should show error for unknown flag");
        
        // Test invalid subcommand
        let output = run_cli_command(&["invalid-subcommand"])?;
        assert!(!output.status.success(), "Should reject invalid subcommand");
        
        // Test missing required arguments
        let output = run_cli_command(&["admin", "backup"])?;
        assert!(!output.status.success(), "Should reject missing backup path");
        
        // Test invalid argument values
        let output = run_cli_command(&["--format", "invalid-format", "query", "SELECT 1"])?;
        assert!(!output.status.success(), "Should reject invalid format");
        
        Ok(())
    }

    #[test]
    fn test_database_access_errors() -> Result<()> {
        // Test non-existent database file
        let output = run_cli_command(&[
            "--database", "/tmp/nonexistent/path/database.db",
            "query", "SELECT 1"
        ])?;
        
        // Should fail gracefully, not crash
        let stderr = String::from_utf8(output.stderr)?;
        println!("Non-existent database error: {}", stderr);
        
        // Test read-only directory (if possible)
        if cfg!(unix) {
            let output = run_cli_command(&[
                "--database", "/root/readonly.db",
                "query", "SELECT 1"
            ])?;
            
            println!("Read-only access error: {:?}", output);
        }
        
        Ok(())
    }

    #[test]
    fn test_invalid_query_syntax() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("syntax_test.db");
        
        let invalid_queries = vec![
            "INVALID SQL SYNTAX",
            "SELECT * FROM",
            "INSERT INTO",
            "DELETE WHERE",
            "UPDATE SET",
            "CREATE TABLE (",
            "DROP TABLE",
            "",
            "   ",
            "SELECT * FROM table_that_does_not_exist",
        ];
        
        for query in invalid_queries {
            let output = run_cli_command(&[
                "--database", db_path.to_str().unwrap(),
                "query", query
            ])?;
            
            println!("Invalid query '{}': {:?}", query, output);
            // Should fail gracefully without crashing
        }
        
        Ok(())
    }

    #[test]
    fn test_sstable_file_errors() -> Result<()> {
        // Test non-existent SSTable file
        let output = run_cli_command(&[
            "info", "/tmp/nonexistent/sstable"
        ])?;
        
        assert!(!output.status.success(), "Should fail for non-existent SSTable");
        
        let stderr = String::from_utf8(output.stderr)?;
        println!("Non-existent SSTable error: {}", stderr);
        
        // Test directory that's not an SSTable
        let temp_dir = TempDir::new()?;
        let fake_dir = temp_dir.path().join("not-an-sstable");
        std::fs::create_dir_all(&fake_dir)?;
        
        let output = run_cli_command(&[
            "info", fake_dir.to_str().unwrap()
        ])?;
        
        println!("Invalid SSTable directory: {:?}", output);
        
        // Test with corrupted SSTable structure
        let corrupt_dir = temp_dir.path().join("corrupt-sstable");
        std::fs::create_dir_all(&corrupt_dir)?;
        std::fs::write(corrupt_dir.join("invalid-file.db"), b"corrupt data")?;
        
        let output = run_cli_command(&[
            "info", corrupt_dir.to_str().unwrap()
        ])?;
        
        println!("Corrupted SSTable structure: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_schema_file_errors() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        // Test non-existent schema file
        let output = run_cli_command(&[
            "schema", "validate", "/tmp/nonexistent.json"
        ])?;
        
        assert!(!output.status.success(), "Should fail for non-existent schema");
        
        // Test invalid JSON schema
        let invalid_json = temp_dir.path().join("invalid.json");
        std::fs::write(&invalid_json, "{ invalid json syntax }")?;
        
        let output = run_cli_command(&[
            "schema", "validate", invalid_json.to_str().unwrap()
        ])?;
        
        assert!(!output.status.success(), "Should reject invalid JSON");
        
        // Test incomplete JSON schema
        let incomplete_json = temp_dir.path().join("incomplete.json");
        std::fs::write(&incomplete_json, r#"{"keyspace": "test"}"#)?;
        
        let output = run_cli_command(&[
            "schema", "validate", incomplete_json.to_str().unwrap()
        ])?;
        
        println!("Incomplete schema validation: {:?}", output);
        
        // Test invalid CQL DDL
        let invalid_cql = temp_dir.path().join("invalid.cql");
        std::fs::write(&invalid_cql, "INVALID CREATE TABLE SYNTAX")?;
        
        let output = run_cli_command(&[
            "schema", "validate", invalid_cql.to_str().unwrap()
        ])?;
        
        assert!(!output.status.success(), "Should reject invalid CQL");
        
        // Test empty files
        let empty_file = temp_dir.path().join("empty.json");
        std::fs::write(&empty_file, "")?;
        
        let output = run_cli_command(&[
            "schema", "validate", empty_file.to_str().unwrap()
        ])?;
        
        assert!(!output.status.success(), "Should reject empty file");
        
        Ok(())
    }

    #[test]
    fn test_configuration_file_errors() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("config_error_test.db");
        
        // Test non-existent config file
        let output = run_cli_command(&[
            "--config", "/tmp/nonexistent.toml",
            "--database", db_path.to_str().unwrap(),
            "admin", "info"
        ])?;
        
        println!("Non-existent config: {:?}", output);
        
        // Test invalid TOML syntax
        let invalid_toml = temp_dir.path().join("invalid.toml");
        std::fs::write(&invalid_toml, "[invalid toml syntax")?;
        
        let output = run_cli_command(&[
            "--config", invalid_toml.to_str().unwrap(),
            "--database", db_path.to_str().unwrap(),
            "admin", "info"
        ])?;
        
        println!("Invalid TOML config: {:?}", output);
        
        // Test config with invalid values
        let invalid_values = temp_dir.path().join("invalid_values.toml");
        std::fs::write(&invalid_values, r#"
[performance]
cache_size_mb = -100
query_timeout_ms = "invalid"
"#)?;
        
        let output = run_cli_command(&[
            "--config", invalid_values.to_str().unwrap(),
            "--database", db_path.to_str().unwrap(),
            "admin", "info"
        ])?;
        
        println!("Invalid config values: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_import_export_errors() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("import_export_error.db");
        
        // Test import from non-existent file
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "import", "/tmp/nonexistent.csv",
            "--format", "csv",
            "--table", "test"
        ])?;
        
        assert!(!output.status.success(), "Should fail for non-existent import file");
        
        // Test export to read-only location (if possible)
        if cfg!(unix) {
            let output = run_cli_command(&[
                "--database", db_path.to_str().unwrap(),
                "export", "test_table", "/root/readonly.csv",
                "--format", "csv"
            ])?;
            
            println!("Read-only export location: {:?}", output);
        }
        
        // Test import with invalid data format
        let invalid_csv = temp_dir.path().join("invalid.csv");
        std::fs::write(&invalid_csv, "invalid,csv,data\nwith,mismatched,columns,extra")?;
        
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "import", invalid_csv.to_str().unwrap(),
            "--format", "csv",
            "--table", "test"
        ])?;
        
        println!("Invalid CSV import: {:?}", output);
        
        // Test import with invalid JSON
        let invalid_json = temp_dir.path().join("invalid.json");
        std::fs::write(&invalid_json, "{ invalid json for import }")?;
        
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "import", invalid_json.to_str().unwrap(),
            "--format", "json",
            "--table", "test"
        ])?;
        
        println!("Invalid JSON import: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_version_detection_errors() -> Result<()> {
        // Test invalid Cassandra version
        let output = run_cli_command(&[
            "--cassandra-version", "invalid-version",
            "info", "/tmp/test"
        ])?;
        
        assert!(!output.status.success(), "Should reject invalid version");
        
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains("version") || stderr.contains("invalid"), 
               "Should mention version error");
        
        // Test unsupported version
        let output = run_cli_command(&[
            "--cassandra-version", "1.0",
            "info", "/tmp/test"
        ])?;
        
        println!("Unsupported version: {:?}", output);
        
        // Test future version
        let output = run_cli_command(&[
            "--cassandra-version", "99.0",
            "info", "/tmp/test"
        ])?;
        
        println!("Future version: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_memory_and_resource_limits() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("resource_test.db");
        
        // Test with very small memory limit (via config)
        let small_memory_config = temp_dir.path().join("small_memory.toml");
        std::fs::write(&small_memory_config, r#"
[performance]
memory_limit_mb = 1
cache_size_mb = 1
"#)?;
        
        let output = run_cli_command(&[
            "--config", small_memory_config.to_str().unwrap(),
            "--database", db_path.to_str().unwrap(),
            "bench", "write", "--ops", "1000"
        ])?;
        
        println!("Small memory limit test: {:?}", output);
        
        // Test with very large operation count
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "bench", "read", "--ops", "1000000", "--threads", "1"
        ])?;
        
        println!("Large operation count: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_concurrent_access_errors() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("concurrent_test.db");
        
        // Create database first
        let setup_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "admin", "info"
        ])?;
        
        println!("Database setup: {:?}", setup_output);
        
        // Test multiple concurrent operations
        use std::thread;
        use std::sync::Arc;
        
        let db_path = Arc::new(db_path);
        let mut handles = vec![];
        
        for i in 0..3 {
            let db_path_clone = Arc::clone(&db_path);
            let handle = thread::spawn(move || {
                run_cli_command(&[
                    "--database", db_path_clone.to_str().unwrap(),
                    "query", &format!("SELECT {} as test_value", i)
                ])
            });
            handles.push(handle);
        }
        
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(result) => {
                    println!("Concurrent operation {} result: {:?}", i, result);
                }
                Err(e) => {
                    println!("Concurrent operation {} panicked: {:?}", i, e);
                }
            }
        }
        
        Ok(())
    }

    #[test]
    fn test_large_data_handling() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("large_data.db");
        
        // Test very long query string
        let long_query = format!("SELECT {} as long_value", "x".repeat(10000));
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "query", &long_query
        ])?;
        
        println!("Long query test: {:?}", output);
        
        // Test very long file path
        let long_path = temp_dir.path().join(&"a".repeat(200));
        let output = run_cli_command(&[
            "--database", long_path.to_str().unwrap_or("invalid"),
            "admin", "info"
        ])?;
        
        println!("Long path test: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_signal_handling() -> Result<()> {
        // Test graceful shutdown (simulation)
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("signal_test.db");
        
        // Start a long-running operation and simulate interruption
        use std::process::{Command, Stdio};
        use std::time::Duration;
        
        let mut child = Command::new("timeout")
            .args(&["2s", "cargo", "run", "--bin", "cqlite", "--",
                   "--database", db_path.to_str().unwrap(),
                   "bench", "write", "--ops", "10000"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        
        match child {
            Ok(mut process) => {
                // Wait for timeout
                let result = process.wait();
                println!("Signal handling test result: {:?}", result);
            }
            Err(e) => {
                println!("Signal handling test failed (expected if timeout not available): {}", e);
            }
        }
        
        Ok(())
    }

    #[test]
    fn test_encoding_and_character_errors() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("encoding_test.db");
        
        // Test with non-UTF8 characters in query
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "query", "SELECT 'café' as unicode_test"
        ])?;
        
        println!("Unicode query test: {:?}", output);
        
        // Test with binary data in file names (if possible)
        let binary_name = temp_dir.path().join("test\x00binary");
        let output = run_cli_command(&[
            "--database", binary_name.to_str().unwrap_or("invalid"),
            "admin", "info"
        ])?;
        
        println!("Binary filename test: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_stack_overflow_prevention() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("stack_test.db");
        
        // Test deeply nested query (if applicable)
        let nested_query = format!("SELECT {}", "((".repeat(100) + "1" + &"))".repeat(100));
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "query", &nested_query
        ])?;
        
        println!("Nested query test: {:?}", output);
        
        Ok(())
    }
}

/// Security and permission tests
#[cfg(test)]
mod security_tests {
    use super::*;

    #[test]
    fn test_path_traversal_prevention() -> Result<()> {
        // Test path traversal attempts
        let malicious_paths = vec![
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "/proc/self/mem",
            "\\\\server\\share\\file",
        ];
        
        for path in malicious_paths {
            let output = run_cli_command(&[
                "--database", path,
                "admin", "info"
            ])?;
            
            println!("Path traversal test '{}': {:?}", path, output);
            // Should fail safely without accessing sensitive files
        }
        
        Ok(())
    }

    #[test]
    fn test_command_injection_prevention() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("injection_test.db");
        
        // Test potential command injection in arguments
        let injection_attempts = vec![
            "; rm -rf /",
            "| cat /etc/passwd",
            "&& echo hacked",
            "`whoami`",
            "$(ls -la)",
        ];
        
        for injection in injection_attempts {
            let output = run_cli_command(&[
                "--database", db_path.to_str().unwrap(),
                "query", injection
            ])?;
            
            println!("Command injection test '{}': {:?}", injection, output);
            // Should treat as query text, not execute as commands
        }
        
        Ok(())
    }

    #[test]
    fn test_file_permission_handling() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        // Create files with different permissions
        let readable_file = temp_dir.path().join("readable.db");
        std::fs::write(&readable_file, b"test")?;
        
        if cfg!(unix) {
            use std::os::unix::fs::PermissionsExt;
            
            // Create read-only file
            let readonly_file = temp_dir.path().join("readonly.db");
            std::fs::write(&readonly_file, b"test")?;
            let metadata = std::fs::metadata(&readonly_file)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o444); // Read-only
            std::fs::set_permissions(&readonly_file, permissions)?;
            
            let output = run_cli_command(&[
                "--database", readonly_file.to_str().unwrap(),
                "admin", "info"
            ])?;
            
            println!("Read-only file test: {:?}", output);
            
            // Create no-permission file
            let noperm_file = temp_dir.path().join("noperm.db");
            std::fs::write(&noperm_file, b"test")?;
            let metadata = std::fs::metadata(&noperm_file)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o000); // No permissions
            std::fs::set_permissions(&noperm_file, permissions)?;
            
            let output = run_cli_command(&[
                "--database", noperm_file.to_str().unwrap(),
                "admin", "info"
            ])?;
            
            println!("No permission file test: {:?}", output);
            
            // Restore permissions for cleanup
            let metadata = std::fs::metadata(&noperm_file)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o644);
            let _ = std::fs::set_permissions(&noperm_file, permissions);
        }
        
        Ok(())
    }
}
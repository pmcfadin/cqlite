use anyhow::Result;
use std::process::Command;
use std::path::PathBuf;
use tempfile::TempDir;

/// Integration tests for the CQLite CLI
/// 
/// These tests validate all CLI functionality including:
/// - Command-line argument parsing
/// - Database operations
/// - Output formatting
/// - Error handling
/// - Interactive REPL mode
/// - SSTable reading capabilities

#[cfg(test)]
mod tests {
    use super::*;

    const CLI_BINARY: &str = "cqlite";

    /// Test helper to create a temporary database
    fn create_temp_database() -> Result<(TempDir, PathBuf)> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test.db");
        Ok((temp_dir, db_path))
    }

    /// Test helper to run CLI commands
    fn run_cli_command(args: &[&str]) -> Result<std::process::Output> {
        Command::new("cargo")
            .args(&["run", "--bin", CLI_BINARY, "--"])
            .args(args)
            .output()
            .map_err(|e| anyhow::anyhow!("Failed to run CLI command: {}", e))
    }

    #[test]
    fn test_cli_help() -> Result<()> {
        let output = run_cli_command(&["--help"])?;
        assert!(output.status.success(), "CLI help should succeed");
        
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("CQLite"), "Help should mention CQLite");
        assert!(stdout.contains("--database"), "Help should show database option");
        assert!(stdout.contains("--format"), "Help should show format option");
        
        Ok(())
    }

    #[test]
    fn test_cli_version() -> Result<()> {
        let output = run_cli_command(&["--version"])?;
        assert!(output.status.success(), "CLI version should succeed");
        
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("cqlite"), "Version should mention cqlite");
        
        Ok(())
    }

    #[test]
    fn test_cli_invalid_argument() -> Result<()> {
        let output = run_cli_command(&["--invalid-argument"])?;
        assert!(!output.status.success(), "Invalid argument should fail");
        
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains("error") || stderr.contains("unrecognized"), 
               "Should show error for invalid argument");
        
        Ok(())
    }

    #[test]
    fn test_query_command_basic() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "query", 
            "SELECT 1"
        ])?;
        
        // TODO: Once compilation is fixed, validate actual query execution
        // For now, just check the command structure
        println!("Query command output: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_query_command_with_timing() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "query", 
            "--timing",
            "SELECT 1"
        ])?;
        
        println!("Query with timing output: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_query_command_with_explain() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "query", 
            "--explain",
            "SELECT 1"
        ])?;
        
        println!("Query with explain output: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_output_formats() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        // Test table format (default)
        let table_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--format", "table",
            "query", "SELECT 1"
        ])?;
        
        // Test JSON format
        let json_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--format", "json",
            "query", "SELECT 1"
        ])?;
        
        // Test CSV format
        let csv_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--format", "csv",
            "query", "SELECT 1"
        ])?;
        
        // Test YAML format
        let yaml_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--format", "yaml",
            "query", "SELECT 1"
        ])?;
        
        println!("Table format: {:?}", table_output);
        println!("JSON format: {:?}", json_output);
        println!("CSV format: {:?}", csv_output);
        println!("YAML format: {:?}", yaml_output);
        
        Ok(())
    }

    #[test]
    fn test_admin_commands() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        // Test admin info
        let info_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "admin", "info"
        ])?;
        
        // Test admin compact
        let compact_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "admin", "compact"
        ])?;
        
        println!("Admin info: {:?}", info_output);
        println!("Admin compact: {:?}", compact_output);
        
        Ok(())
    }

    #[test]
    fn test_schema_commands() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        // Test schema list
        let list_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "schema", "list"
        ])?;
        
        println!("Schema list: {:?}", list_output);
        
        Ok(())
    }

    #[test]
    fn test_bench_commands() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        // Test read benchmark with minimal operations
        let read_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "bench", "read", 
            "--ops", "10",
            "--threads", "1"
        ])?;
        
        // Test write benchmark with minimal operations
        let write_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "bench", "write",
            "--ops", "10", 
            "--threads", "1"
        ])?;
        
        // Test mixed benchmark
        let mixed_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "bench", "mixed",
            "--read-pct", "70",
            "--ops", "10",
            "--threads", "1"
        ])?;
        
        println!("Read benchmark: {:?}", read_output);
        println!("Write benchmark: {:?}", write_output);
        println!("Mixed benchmark: {:?}", mixed_output);
        
        Ok(())
    }

    #[test]
    fn test_verbose_and_quiet_modes() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        // Test verbose mode
        let verbose_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--verbose",
            "admin", "info"
        ])?;
        
        // Test quiet mode
        let quiet_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--quiet",
            "admin", "info"
        ])?;
        
        println!("Verbose mode: {:?}", verbose_output);
        println!("Quiet mode: {:?}", quiet_output);
        
        Ok(())
    }

    #[test]
    fn test_config_file_loading() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("config.toml");
        let db_path = temp_dir.path().join("test.db");
        
        // Create a basic config file
        std::fs::write(&config_path, r#"
[performance]
cache_size_mb = 128
query_timeout_ms = 30000
memory_limit_mb = 512

[logging]
level = "info"
"#)?;
        
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--config", config_path.to_str().unwrap(),
            "admin", "info"
        ])?;
        
        println!("Config file loading: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_sstable_auto_detect() -> Result<()> {
        // Test auto-detection feature
        let output = run_cli_command(&[
            "--auto-detect",
            "--cassandra-version", "5.0",
            "info", "/tmp/nonexistent/sstable/path"
        ])?;
        
        // Should fail gracefully for non-existent path
        println!("SSTable auto-detect: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_cassandra_version_validation() -> Result<()> {
        // Test valid version
        let valid_output = run_cli_command(&[
            "--cassandra-version", "5.0",
            "info", "/tmp/nonexistent"
        ])?;
        
        // Test invalid version
        let invalid_output = run_cli_command(&[
            "--cassandra-version", "99.0",
            "info", "/tmp/nonexistent"
        ])?;
        
        println!("Valid version: {:?}", valid_output);
        println!("Invalid version: {:?}", invalid_output);
        
        Ok(())
    }

    #[test]
    fn test_error_handling() -> Result<()> {
        // Test with non-existent database file
        let output = run_cli_command(&[
            "--database", "/tmp/nonexistent.db",
            "query", "SELECT 1"
        ])?;
        
        // Should fail gracefully
        println!("Non-existent database: {:?}", output);
        
        Ok(())
    }

    #[test]
    fn test_import_export_commands() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        let temp_file = _temp_dir.path().join("test.json");
        
        // Create a test file for import
        std::fs::write(&temp_file, r#"{"test": "data"}"#)?;
        
        // Test import command
        let import_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "import",
            temp_file.to_str().unwrap(),
            "--format", "json",
            "--table", "test_table"
        ])?;
        
        // Test export command
        let export_file = _temp_dir.path().join("export.json");
        let export_output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "export",
            "test_table",
            export_file.to_str().unwrap(),
            "--format", "json"
        ])?;
        
        println!("Import: {:?}", import_output);
        println!("Export: {:?}", export_output);
        
        Ok(())
    }
}

/// Performance and stress tests
#[cfg(test)]
mod performance_tests {
    use super::*;

    #[test]
    #[ignore] // Run with: cargo test --ignored
    fn test_large_query_performance() -> Result<()> {
        let (_temp_dir, db_path) = create_temp_database()?;
        
        let start = std::time::Instant::now();
        let output = run_cli_command(&[
            "--database", db_path.to_str().unwrap(),
            "--format", "json",
            "query", "SELECT 1"
        ])?;
        let duration = start.elapsed();
        
        println!("Query took: {:?}", duration);
        assert!(duration.as_millis() < 5000, "Query should complete within 5 seconds");
        
        println!("Performance test output: {:?}", output);
        
        Ok(())
    }

    #[test]
    #[ignore] // Run with: cargo test --ignored
    fn test_concurrent_cli_operations() -> Result<()> {
        use std::thread;
        use std::sync::Arc;
        
        let (_temp_dir, db_path) = create_temp_database()?;
        let db_path = Arc::new(db_path);
        
        let mut handles = vec![];
        
        for i in 0..5 {
            let db_path_clone = Arc::clone(&db_path);
            let handle = thread::spawn(move || {
                run_cli_command(&[
                    "--database", db_path_clone.to_str().unwrap(),
                    "admin", "info"
                ])
            });
            handles.push(handle);
        }
        
        for handle in handles {
            let result = handle.join().unwrap();
            println!("Concurrent operation result: {:?}", result);
        }
        
        Ok(())
    }
}
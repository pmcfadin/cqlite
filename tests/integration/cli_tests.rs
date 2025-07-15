use std::process::Command;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use assert_cmd::prelude::*;

/// Integration tests for the CQLite CLI
#[cfg(test)]
mod cli_integration_tests {
    use super::*;

    fn get_cli_binary() -> Command {
        Command::cargo_bin("cqlite").unwrap()
    }

    fn create_temp_db() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        (temp_dir, db_path)
    }

    #[test]
    fn test_cli_help() {
        let mut cmd = get_cli_binary();
        cmd.arg("--help");
        
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("CQLite"))
            .stdout(predicates::str::contains("Usage:"));
    }

    #[test]
    fn test_cli_version() {
        let mut cmd = get_cli_binary();
        cmd.arg("--version");
        
        cmd.assert()
            .success()
            .stdout(predicates::str::contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn test_basic_query_execution() {
        let (_temp_dir, db_path) = create_temp_db();
        
        let mut cmd = get_cli_binary();
        cmd.args(&[
            "--database", db_path.to_str().unwrap(),
            "query",
            "CREATE KEYSPACE test_ks"
        ]);
        
        cmd.assert().success();
    }

    #[test]
    fn test_interactive_mode_help() {
        let (_temp_dir, db_path) = create_temp_db();
        
        // Test that help command works in interactive mode
        let mut cmd = get_cli_binary();
        cmd.args(&[
            "--database", db_path.to_str().unwrap(),
            "--quiet"
        ]);
        
        // This would require more complex stdin handling for full testing
        // For now, just ensure the binary can start
        let output = cmd.output().unwrap();
        assert!(output.status.success() || output.status.code() == Some(0));
    }

    #[test]
    fn test_import_export_functionality() {
        let (_temp_dir, db_path) = create_temp_db();
        
        // Create a test CSV file
        let csv_content = "id,name,email\n1,John,john@test.com\n2,Jane,jane@test.com";
        let csv_path = _temp_dir.path().join("test_data.csv");
        std::fs::write(&csv_path, csv_content).unwrap();
        
        // Test import
        let mut import_cmd = get_cli_binary();
        import_cmd.args(&[
            "--database", db_path.to_str().unwrap(),
            "import",
            csv_path.to_str().unwrap(),
            "--format", "csv",
            "--table", "users"
        ]);
        
        import_cmd.assert().success();
    }

    #[test]
    fn test_admin_commands() {
        let (_temp_dir, db_path) = create_temp_db();
        
        // Test database info
        let mut cmd = get_cli_binary();
        cmd.args(&[
            "--database", db_path.to_str().unwrap(),
            "admin",
            "info"
        ]);
        
        cmd.assert().success();
    }

    #[test]
    fn test_schema_commands() {
        let (_temp_dir, db_path) = create_temp_db();
        
        // Test table listing
        let mut cmd = get_cli_binary();
        cmd.args(&[
            "--database", db_path.to_str().unwrap(),
            "schema",
            "list"
        ]);
        
        cmd.assert().success();
    }

    #[test]
    fn test_benchmark_commands() {
        let (_temp_dir, db_path) = create_temp_db();
        
        // Test read benchmark with minimal operations
        let mut cmd = get_cli_binary();
        cmd.args(&[
            "--database", db_path.to_str().unwrap(),
            "bench",
            "read",
            "--ops", "10",
            "--threads", "1"
        ]);
        
        cmd.assert().success();
    }

    #[test]
    fn test_output_formats() {
        let (_temp_dir, db_path) = create_temp_db();
        
        // Test different output formats
        for format in &["json", "csv", "table", "yaml"] {
            let mut cmd = get_cli_binary();
            cmd.args(&[
                "--database", db_path.to_str().unwrap(),
                "--format", format,
                "query",
                "SELECT 1 as test"
            ]);
            
            cmd.assert().success();
        }
    }

    #[test]
    fn test_verbose_modes() {
        let (_temp_dir, db_path) = create_temp_db();
        
        // Test different verbosity levels
        for verbosity in &["-v", "-vv", "-vvv"] {
            let mut cmd = get_cli_binary();
            cmd.args(&[
                "--database", db_path.to_str().unwrap(),
                verbosity,
                "admin",
                "info"
            ]);
            
            cmd.assert().success();
        }
    }

    #[test]
    fn test_quiet_mode() {
        let (_temp_dir, db_path) = create_temp_db();
        
        let mut cmd = get_cli_binary();
        cmd.args(&[
            "--database", db_path.to_str().unwrap(),
            "--quiet",
            "admin",
            "info"
        ]);
        
        cmd.assert().success();
    }
}
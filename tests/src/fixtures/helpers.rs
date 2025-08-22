//! Test helper utilities

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

/// Test environment helper
pub struct TestEnvironment {
    pub temp_dir: TempDir,
    pub db_path: PathBuf,
}

impl TestEnvironment {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db_path = temp_dir.path().join("test.db");

        Self { temp_dir, db_path }
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn temp_path(&self) -> &Path {
        self.temp_dir.path()
    }
}

/// Execute a CLI command and return the output
pub fn execute_cli_command(args: &[&str]) -> Result<Output, std::io::Error> {
    Command::new("cargo")
        .args(&["run", "--package", "cqlite-cli", "--"])
        .args(args)
        .output()
}

/// Check if a command executed successfully
pub fn command_success(output: &Output) -> bool {
    output.status.success()
}

/// Get stdout as string
pub fn stdout_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Get stderr as string
pub fn stderr_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// Create a test database with sample schema
pub async fn create_test_database(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Implement actual database creation using cqlite-core
    // For now, just create the file
    std::fs::write(db_path, b"")?;
    Ok(())
}

/// Helper for creating test configuration
pub fn create_test_config() -> serde_json::Value {
    serde_json::json!({
        "connection": {
            "timeout_ms": 5000,
            "retry_attempts": 1,
            "pool_size": 1
        },
        "output": {
            "max_rows": 100,
            "colors": false
        },
        "performance": {
            "query_timeout_ms": 30000,
            "cache_size_mb": 64
        }
    })
}

//! CLI Integration Tests

use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;
use std::time::Duration;
use serde::{Serialize, Deserialize};

/// Configuration for CLI integration tests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CLITestConfig {
    pub timeout: Duration,
    pub verbose: bool,
    pub test_data_path: Option<PathBuf>,
}

impl Default for CLITestConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            verbose: false,
            test_data_path: None,
        }
    }
}

/// Result of a CLI integration test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CLITestResult {
    pub test_name: String,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub execution_time: Duration,
    pub exit_code: Option<i32>,
}

/// CLI Integration Test Suite
#[derive(Debug)]
pub struct CLIIntegrationTestSuite {
    config: CLITestConfig,
    results: Vec<CLITestResult>,
}

impl CLIIntegrationTestSuite {
    pub fn new(config: CLITestConfig) -> Self {
        Self {
            config,
            results: Vec::new(),
        }
    }

    pub async fn run_all_tests(&mut self) -> Result<Vec<CLITestResult>, Box<dyn std::error::Error>> {
        // Run basic CLI tests
        self.test_help_command().await?;
        self.test_version_command().await?;
        
        Ok(self.results.clone())
    }

    async fn test_help_command(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        let output = Command::new("cargo")
            .args(&["run", "--bin", "cqlite", "--", "--help"])
            .output()?;
        
        let result = CLITestResult {
            test_name: "help_command".to_string(),
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            execution_time: start.elapsed(),
            exit_code: output.status.code(),
        };
        
        self.results.push(result);
        Ok(())
    }

    async fn test_version_command(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        let output = Command::new("cargo")
            .args(&["run", "--bin", "cqlite", "--", "--version"])
            .output()?;
        
        let result = CLITestResult {
            test_name: "version_command".to_string(),
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            execution_time: start.elapsed(),
            exit_code: output.status.code(),
        };
        
        self.results.push(result);
        Ok(())
    }
}

/// Create a CLI command instance
pub fn get_cli_binary() -> Command {
    Command::cargo_bin("cqlite").unwrap()
}

/// Create a temporary database for testing
pub fn create_temp_db() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    (temp_dir, db_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_help() {
        let mut cmd = get_cli_binary();
        cmd.arg("--help");

        cmd.assert()
            .success()
            .stdout(predicate::str::contains("CQLite"))
            .stdout(predicate::str::contains("Usage:"));
    }

    #[test]
    fn test_cli_version() {
        let mut cmd = get_cli_binary();
        cmd.arg("--version");

        cmd.assert()
            .success()
            .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
    }
}

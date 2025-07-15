//! CLI Integration Tests

use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

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

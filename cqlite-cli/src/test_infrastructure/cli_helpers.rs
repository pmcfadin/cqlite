//! CLI testing utilities and helpers
//!
//! This module provides utilities for testing CLI commands, including
//! command builders, assertion helpers, and output validation.

use super::{TestContainer, TestResult};
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::path::PathBuf;
use std::process::Command;

/// CLI test runner for executing and validating CLI commands
#[derive(Debug)]
pub struct CliTestRunner {
    container: TestContainer,
    binary_name: String,
    timeout: std::time::Duration,
}

/// Builder for constructing CLI test commands
#[derive(Debug)]
pub struct CliTestBuilder {
    command: String,
    args: Vec<String>,
    env_vars: Vec<(String, String)>,
    working_dir: Option<PathBuf>,
    stdin_data: Option<String>,
    timeout: Option<std::time::Duration>,
}

/// Assertion helper for CLI command results
#[derive(Debug)]
pub struct CommandAssertion {
    cmd: assert_cmd::Command,
}

impl CliTestRunner {
    /// Create a new CLI test runner
    pub fn new(container: TestContainer) -> Self {
        Self {
            container,
            binary_name: "cqlite".to_string(),
            timeout: std::time::Duration::from_secs(30),
        }
    }

    /// Set the binary name to test
    pub fn with_binary<S: Into<String>>(mut self, binary_name: S) -> Self {
        self.binary_name = binary_name.into();
        self
    }

    /// Set command timeout
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Create a new command builder
    pub fn command<S: Into<String>>(&self, command: S) -> CliTestBuilder {
        CliTestBuilder::new(command)
            .with_timeout(self.timeout)
            .with_working_dir(self.container.environment().temp_dir.clone())
    }

    /// Execute a simple command with arguments
    pub fn run(&self, args: &[&str]) -> TestResult<CommandAssertion> {
        let mut cmd = Command::cargo_bin(&self.binary_name)?;
        cmd.args(args);
        cmd.current_dir(&self.container.environment().temp_dir);

        // Set up test environment variables
        let env = self.container.environment();
        cmd.env("CQLITE_CONFIG", env.config_path);
        cmd.env("CQLITE_DB", env.db_path);

        Ok(CommandAssertion {
            cmd: assert_cmd::Command::from(cmd),
        })
    }

    /// Test basic CLI functionality
    pub fn test_help(&self) -> TestResult<()> {
        self.run(&["--help"])?
            .assert_success()?
            .stdout_contains("CQLite - High-performance embedded database")?;
        Ok(())
    }

    /// Test version command
    pub fn test_version(&self) -> TestResult<()> {
        self.run(&["--version"])?
            .assert_success()?
            .stdout_contains(env!("CARGO_PKG_VERSION"))?;
        Ok(())
    }

    /// Test database info command
    pub fn test_info(&self) -> TestResult<()> {
        let sstable_fixture = self
            .container
            .environment()
            .fixtures_dir
            .join("test.sstable");
        let schema_fixture = self
            .container
            .environment()
            .fixtures_dir
            .join("test.schema");

        // Create dummy fixtures for testing
        std::fs::write(&sstable_fixture, "dummy sstable data")?;
        std::fs::write(&schema_fixture, "{\"tables\": []}")?;

        self.run(&["info", sstable_fixture.to_str().unwrap()])?
            .assert_success()?;

        Ok(())
    }
}

impl CliTestBuilder {
    /// Create a new command builder
    pub fn new<S: Into<String>>(command: S) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env_vars: Vec::new(),
            working_dir: None,
            stdin_data: None,
            timeout: None,
        }
    }

    /// Add command argument
    pub fn arg<S: Into<String>>(mut self, arg: S) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add multiple arguments
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(|s| s.into()));
        self
    }

    /// Set environment variable
    pub fn env<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.env_vars.push((key.into(), value.into()));
        self
    }

    /// Set working directory
    pub fn with_working_dir<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        self.working_dir = Some(dir.into());
        self
    }

    /// Set stdin data
    pub fn with_stdin<S: Into<String>>(mut self, data: S) -> Self {
        self.stdin_data = Some(data.into());
        self
    }

    /// Set command timeout
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Execute the command and return assertion helper
    pub fn execute(self) -> TestResult<CommandAssertion> {
        let mut cmd = Command::cargo_bin("cqlite")?;

        // Add command as first argument
        cmd.arg(&self.command);

        // Add all arguments
        cmd.args(&self.args);

        // Set environment variables
        for (key, value) in &self.env_vars {
            cmd.env(key, value);
        }

        // Set working directory
        if let Some(ref dir) = self.working_dir {
            cmd.current_dir(dir);
        }

        let mut assert_cmd = assert_cmd::Command::from(cmd);

        // Set stdin data if provided
        if let Some(ref stdin) = self.stdin_data {
            assert_cmd.write_stdin(stdin.as_str());
        }

        // Set timeout if provided
        if let Some(timeout) = self.timeout {
            assert_cmd.timeout(timeout);
        }

        Ok(CommandAssertion { cmd: assert_cmd })
    }
}

impl CommandAssertion {
    /// Assert command succeeds
    pub fn assert_success(mut self) -> TestResult<Self> {
        self.cmd.assert().success();
        Ok(self)
    }

    /// Assert command fails
    pub fn assert_failure(mut self) -> TestResult<Self> {
        self.cmd.assert().failure();
        Ok(self)
    }

    /// Assert stdout contains text
    pub fn stdout_contains<S: AsRef<str>>(mut self, expected: S) -> TestResult<Self> {
        self.cmd
            .assert()
            .stdout(predicate::str::contains(expected.as_ref()));
        Ok(self)
    }

    /// Assert stderr contains text
    pub fn stderr_contains<S: AsRef<str>>(mut self, expected: S) -> TestResult<Self> {
        self.cmd
            .assert()
            .stderr(predicate::str::contains(expected.as_ref()));
        Ok(self)
    }

    /// Assert stdout matches exactly
    pub fn stdout_equals<S: AsRef<str>>(mut self, expected: S) -> TestResult<Self> {
        let expected_string = expected.as_ref().to_string();
        self.cmd.assert().stdout(expected_string);
        Ok(self)
    }

    /// Assert stderr matches exactly
    pub fn stderr_equals<S: AsRef<str>>(mut self, expected: S) -> TestResult<Self> {
        let expected_string = expected.as_ref().to_string();
        self.cmd.assert().stderr(expected_string);
        Ok(self)
    }

    /// Assert stdout is empty
    pub fn stdout_empty(mut self) -> TestResult<Self> {
        self.cmd.assert().stdout(predicate::str::is_empty());
        Ok(self)
    }

    /// Assert stderr is empty
    pub fn stderr_empty(mut self) -> TestResult<Self> {
        self.cmd.assert().stderr(predicate::str::is_empty());
        Ok(self)
    }

    /// Assert exit code
    pub fn exit_code(mut self, code: i32) -> TestResult<Self> {
        self.cmd.assert().code(code);
        Ok(self)
    }

    /// Get the raw assert_cmd::Command for custom assertions
    pub fn raw_command(self) -> assert_cmd::Command {
        self.cmd
    }
}

/// Common CLI test scenarios
pub struct CliTestScenarios;

impl CliTestScenarios {
    /// Test all basic CLI commands
    pub fn test_basic_commands(runner: &CliTestRunner) -> TestResult<()> {
        // Test help command
        runner.test_help()?;

        // Test version command
        runner.test_version()?;

        println!("✅ Basic CLI commands test passed");
        Ok(())
    }

    /// Test query execution commands
    pub fn test_query_commands(runner: &CliTestRunner) -> TestResult<()> {
        let env = runner.container.environment();
        let schema_file = env.fixtures_dir.join("test_schema.json");
        let sstable_file = env.fixtures_dir.join("test_sstable");

        // Create test fixtures
        std::fs::write(
            &schema_file,
            r#"{"tables": [{"name": "users", "columns": []}]}"#,
        )?;
        std::fs::write(&sstable_file, "test sstable data")?;

        // Test read command
        runner
            .run(&[
                "read",
                sstable_file.to_str().unwrap(),
                "--schema",
                schema_file.to_str().unwrap(),
            ])?
            .assert_success()?;

        // Test info command
        runner
            .run(&["info", sstable_file.to_str().unwrap()])?
            .assert_success()?;

        println!("✅ Query commands test passed");
        Ok(())
    }

    /// Test error handling
    pub fn test_error_handling(runner: &CliTestRunner) -> TestResult<()> {
        // Test invalid command
        runner
            .run(&["invalid_command"])?
            .assert_failure()?
            .stderr_contains("error:")?;

        // Test missing required argument
        runner
            .run(&["read"])?
            .assert_failure()?
            .stderr_contains("required")?;

        // Test non-existent file
        runner
            .run(&[
                "read",
                "/non/existent/file",
                "--schema",
                "/non/existent/schema",
            ])?
            .assert_failure()?;

        println!("✅ Error handling test passed");
        Ok(())
    }

    /// Test configuration handling
    pub fn test_config_handling(runner: &CliTestRunner) -> TestResult<()> {
        let env = runner.container.environment();

        // Test with custom config file
        runner
            .run(&["--config", env.config_path.to_str().unwrap(), "--help"])?
            .assert_success()?;

        // Test verbose flag
        runner.run(&["-v", "--help"])?.assert_success()?;

        // Test quiet flag
        runner.run(&["-q", "--help"])?.assert_success()?;

        println!("✅ Configuration handling test passed");
        Ok(())
    }

    /// Run all CLI test scenarios
    pub fn run_all(runner: &CliTestRunner) -> TestResult<()> {
        Self::test_basic_commands(runner)?;
        Self::test_query_commands(runner)?;
        Self::test_error_handling(runner)?;
        Self::test_config_handling(runner)?;

        println!("🎉 All CLI test scenarios passed!");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_infrastructure::TestContainer;

    #[tokio::test]
    async fn test_cli_runner_creation() {
        let container = TestContainer::new().unwrap();
        let runner = CliTestRunner::new(container)
            .with_binary("cqlite")
            .with_timeout(std::time::Duration::from_secs(10));

        assert_eq!(runner.binary_name, "cqlite");
        assert_eq!(runner.timeout, std::time::Duration::from_secs(10));
    }

    #[test]
    fn test_command_builder() {
        let builder = CliTestBuilder::new("read")
            .arg("test.sstable")
            .arg("--schema")
            .arg("test.schema")
            .env("TEST_VAR", "test_value")
            .with_timeout(std::time::Duration::from_secs(30));

        assert_eq!(builder.command, "read");
        assert_eq!(builder.args.len(), 3);
        assert_eq!(builder.env_vars.len(), 1);
        assert_eq!(builder.timeout, Some(std::time::Duration::from_secs(30)));
    }
}

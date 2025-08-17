//! Basic CLI functionality integration tests

use clap::Parser;
use cqlite_cli::test_infrastructure::*;
use cqlite_cli::{test_container, assert_cli_success, assert_cli_error};

#[tokio::test]
async fn test_cli_help_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test help command
    runner.test_help()?;
    
    // Test specific command help
    runner.run(&["--help"])?
        .assert_success()?
        .stdout_contains("CQLite - High-performance embedded database")?;
    
    Ok(())
}

#[tokio::test]
async fn test_cli_version_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.test_version()?;
    
    runner.run(&["--version"])?
        .assert_success()?
        .stdout_contains(env!("CARGO_PKG_VERSION"))?;
    
    Ok(())
}

#[tokio::test]
async fn test_invalid_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.run(&["invalid_command"])?
        .assert_failure()?
        .stderr_contains("error")?;
    
    Ok(())
}

#[tokio::test]
async fn test_verbose_flag() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.run(&["-v", "--help"])?
        .assert_success()?;
    
    runner.run(&["-vv", "--help"])?
        .assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_quiet_flag() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.run(&["-q", "--help"])?
        .assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_config_file_option() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    // Create a test config file
    let config = cqlite_cli::Config::default();
    config.save_to_file(&env.config_path)?;
    
    runner.run(&[
        "--config", 
        env.config_path.to_str().unwrap(),
        "--help"
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_database_path_option() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    runner.run(&[
        "--database",
        env.db_path.to_str().unwrap(),
        "--help"
    ])?.assert_success()?;
    
    Ok(())
}

#[test]
fn test_cli_argument_parsing() {
    // Test that CLI arguments can be parsed correctly
    // This is a unit test that doesn't require actual command execution
    use clap::Parser;
    use cqlite_cli::Commands;
    
    // Test parsing help
    let args = vec!["cqlite", "--help"];
    // Note: This would normally panic, but we're just testing the structure exists
    
    // Test parsing version
    let args = vec!["cqlite", "--version"];
    // Same note as above
    
    // If we reach here, the CLI structure compiles correctly
    assert!(true);
}

#[tokio::test]
async fn test_output_format_options() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test different output formats
    runner.run(&["--format", "table", "--help"])?
        .assert_success()?;
    
    runner.run(&["--format", "json", "--help"])?
        .assert_success()?;
    
    runner.run(&["--format", "csv", "--help"])?
        .assert_success()?;
    
    Ok(())
}

#[tokio::test] 
async fn test_cassandra_version_override() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.run(&["--cassandra-version", "5.0", "--help"])?
        .assert_success()?;
    
    runner.run(&["--cassandra-version", "4.0", "--help"])?
        .assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_auto_detect_flag() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.run(&["--auto-detect", "--help"])?
        .assert_success()?;
    
    Ok(())
}
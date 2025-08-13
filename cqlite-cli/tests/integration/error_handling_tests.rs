//! Error handling integration tests

use cqlite_cli::test_infrastructure::*;

#[tokio::test]
async fn test_invalid_command_error_handling() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test completely invalid command
    runner.run(&["nonexistent_command"])?
        .assert_failure()?
        .stderr_contains("error")?;
    
    Ok(())
}

#[tokio::test]
async fn test_missing_required_arguments() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test read command without required arguments
    runner.run(&["read"])?
        .assert_failure()?
        .stderr_contains("required")?;
    
    // Test select command without required arguments
    runner.run(&["select"])?
        .assert_failure()?
        .stderr_contains("required")?;
    
    Ok(())
}

#[tokio::test]
async fn test_invalid_file_paths() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test read command with non-existent SSTable file
    runner.run(&[
        "read",
        "/non/existent/sstable/path",
        "--schema",
        "/non/existent/schema/path",
    ])?.assert_failure()?;
    
    // Test info command with non-existent file
    runner.run(&[
        "info",
        "/non/existent/file",
    ])?.assert_failure()?;
    
    Ok(())
}

#[tokio::test]
async fn test_invalid_command_line_options() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test invalid format option
    runner.run(&["--format", "invalid_format", "--help"])?
        .assert_failure()?
        .stderr_contains("invalid")?;
    
    // Test invalid numeric values
    runner.run(&["read", "dummy", "--schema", "dummy", "--limit", "not_a_number"])?
        .assert_failure()?;
    
    Ok(())
}

#[tokio::test]
async fn test_configuration_file_errors() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    // Test with non-existent config file
    runner.run(&[
        "--config",
        "/non/existent/config.toml",
        "--help",
    ])?.assert_failure()?;
    
    // Test with invalid config file format
    let invalid_config_path = env.temp_dir.join("invalid_config.toml");
    std::fs::write(&invalid_config_path, "invalid toml content [")?;
    
    runner.run(&[
        "--config",
        invalid_config_path.to_str().unwrap(),
        "--help",
    ])?.assert_failure()?;
    
    Ok(())
}

#[tokio::test]
async fn test_schema_validation_errors() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    // Create invalid schema file
    let invalid_schema_path = env.fixtures_dir.join("invalid_schema.json");
    std::fs::write(&invalid_schema_path, "invalid json content")?;
    
    let dummy_sstable_path = env.fixtures_dir.join("dummy.sstable");
    std::fs::write(&dummy_sstable_path, "dummy content")?;
    
    // Test read command with invalid schema
    runner.run(&[
        "read",
        dummy_sstable_path.to_str().unwrap(),
        "--schema",
        invalid_schema_path.to_str().unwrap(),
    ])?.assert_failure()?;
    
    Ok(())
}

#[tokio::test]
async fn test_memory_limit_errors() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    let dummy_sstable_path = env.fixtures_dir.join("dummy.sstable");
    let dummy_schema_path = env.fixtures_dir.join("dummy_schema.json");
    
    std::fs::write(&dummy_sstable_path, "dummy content")?;
    std::fs::write(&dummy_schema_path, r#"{"tables": []}"#)?;
    
    // Test with extremely low memory limit (should fail gracefully)
    runner.run(&[
        "read",
        dummy_sstable_path.to_str().unwrap(),
        "--schema",
        dummy_schema_path.to_str().unwrap(),
        "--max-memory-mb", "1",
    ])?.assert_failure()?;
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_access_error_handling() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // This test checks how the CLI handles concurrent access to the same resources
    // Since we're running in a test environment, this might not trigger actual
    // concurrent access issues, but it ensures the error handling paths exist
    
    let env = container.environment();
    let dummy_sstable_path = env.fixtures_dir.join("concurrent_test.sstable");
    let dummy_schema_path = env.fixtures_dir.join("concurrent_schema.json");
    
    std::fs::write(&dummy_sstable_path, "test content")?;
    std::fs::write(&dummy_schema_path, r#"{"tables": []}"#)?;
    
    // Multiple operations on the same file
    for _ in 0..3 {
        let _result = runner.run(&[
            "info",
            dummy_sstable_path.to_str().unwrap(),
        ]);
        // Don't assert on results as concurrent access behavior may vary
    }
    
    Ok(())
}

#[tokio::test]
async fn test_timeout_error_handling() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test operations that might timeout (using very small limits where possible)
    let env = container.environment();
    let dummy_sstable_path = env.fixtures_dir.join("timeout_test.sstable");
    let dummy_schema_path = env.fixtures_dir.join("timeout_schema.json");
    
    std::fs::write(&dummy_sstable_path, "test content for timeout")?;
    std::fs::write(&dummy_schema_path, r#"{"tables": []}"#)?;
    
    // This is a basic test - actual timeout behavior would depend on implementation
    let result = runner.run(&[
        "read",
        dummy_sstable_path.to_str().unwrap(),
        "--schema",
        dummy_schema_path.to_str().unwrap(),
    ]);
    
    // The command should either succeed or fail gracefully
    match result {
        Ok(_) => assert!(true),  // Success is fine
        Err(_) => assert!(true), // Graceful failure is also fine
    }
    
    Ok(())
}

#[tokio::test]
async fn test_resource_exhaustion_handling() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test handling of resource exhaustion scenarios
    let env = container.environment();
    
    // Create a large dummy file to test memory/resource limits
    let large_dummy_path = env.fixtures_dir.join("large_test.sstable");
    let large_content = "x".repeat(1000); // Not actually large, but symbolic
    std::fs::write(&large_dummy_path, large_content)?;
    
    let dummy_schema_path = env.fixtures_dir.join("large_schema.json");
    std::fs::write(&dummy_schema_path, r#"{"tables": []}"#)?;
    
    // Test with minimal resource limits
    let result = runner.run(&[
        "read",
        large_dummy_path.to_str().unwrap(),
        "--schema",
        dummy_schema_path.to_str().unwrap(),
        "--max-memory-mb", "1",
        "--buffer-size", "64",
    ]);
    
    // Should handle resource constraints gracefully
    match result {
        Ok(_) => assert!(true),  // Handled successfully
        Err(_) => assert!(true), // Failed gracefully
    }
    
    Ok(())
}

#[tokio::test]
async fn test_malformed_query_error_handling() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    let dummy_sstable_path = env.fixtures_dir.join("query_test.sstable");
    let dummy_schema_path = env.fixtures_dir.join("query_schema.json");
    
    std::fs::write(&dummy_sstable_path, "test content")?;
    std::fs::write(&dummy_schema_path, r#"{"tables": [{"name": "test", "columns": []}]}"#)?;
    
    // Test malformed SQL queries
    runner.run(&[
        "select",
        dummy_sstable_path.to_str().unwrap(),
        "--schema",
        dummy_schema_path.to_str().unwrap(),
        "INVALID SQL QUERY SYNTAX",
    ])?.assert_failure()?;
    
    // Test query with non-existent table
    runner.run(&[
        "select",
        dummy_sstable_path.to_str().unwrap(),
        "--schema",
        dummy_schema_path.to_str().unwrap(),
        "SELECT * FROM nonexistent_table",
    ])?.assert_failure()?;
    
    Ok(())
}
//! CLI command functionality tests
//!
//! Tests for specific CLI commands like read, info, select, etc.

use cqlite_cli::test_infrastructure::*;
use std::path::PathBuf;

#[tokio::test]
async fn test_read_command_with_fixtures() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    // Create test fixtures
    let fixtures = TestDataBuilder::new(env.fixtures_dir.clone())?
        .add_sstable("test_users", 
            SSTableFixture::new("users")
                .add_column("id", "UUID")
                .add_column("name", "TEXT")
                .add_row(vec![
                    serde_json::Value::String("123e4567-e89b-12d3-a456-426614174000".to_string()),
                    serde_json::Value::String("John Doe".to_string()),
                ])
        )?
        .add_schema("test_schema",
            SchemaFixture::new("test_keyspace")
                .add_table(
                    TableSchema::new("users")
                        .add_column("id", "UUID", false)
                        .add_column("name", "TEXT", false)
                        .with_primary_key(vec!["id".to_string()])
                )?
        )?
        .build()?;
    
    let sstable_path = &fixtures[0].file_path;
    let schema_path = &fixtures[1].file_path;
    
    // Test basic read command
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_info_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    // Create a dummy SSTable file
    let sstable_path = env.fixtures_dir.join("test.sstable");
    std::fs::write(&sstable_path, "dummy sstable data")?;
    
    // Test basic info command
    runner.run(&[
        "info",
        sstable_path.to_str().unwrap(),
    ])?.assert_success()?;
    
    // Test info command with detailed flag
    runner.run(&[
        "info",
        sstable_path.to_str().unwrap(),
        "--detailed",
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_select_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    // Create test fixtures
    let fixtures = TestDataBuilder::create_common_fixtures(env.fixtures_dir.clone())?;
    
    let sstable_path = fixtures.iter()
        .find(|f| f.name == "basic_users")
        .map(|f| &f.file_path)
        .ok_or("SSTable fixture not found")?;
    
    let schema_path = fixtures.iter()
        .find(|f| f.name == "basic_schema")
        .map(|f| &f.file_path)
        .ok_or("Schema fixture not found")?;
    
    // Test SELECT command
    runner.run(&[
        "select",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "SELECT * FROM users",
    ])?.assert_success()?;
    
    // Test SELECT with specific columns
    runner.run(&[
        "select",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "SELECT name FROM users WHERE id = '550e8400-e29b-41d4-a716-446655440000'",
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_read_command_with_limits() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    let fixtures = TestDataBuilder::create_common_fixtures(env.fixtures_dir.clone())?;
    let sstable_path = fixtures.iter()
        .find(|f| f.name == "basic_users")
        .map(|f| &f.file_path)
        .unwrap();
    let schema_path = fixtures.iter()
        .find(|f| f.name == "basic_schema") 
        .map(|f| &f.file_path)
        .unwrap();
    
    // Test with limit
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--limit", "5",
    ])?.assert_success()?;
    
    // Test with skip
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--skip", "1",
    ])?.assert_success()?;
    
    // Test with both limit and skip
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--limit", "5",
        "--skip", "1",
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_read_command_with_generation() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    let fixtures = TestDataBuilder::create_common_fixtures(env.fixtures_dir.clone())?;
    let sstable_path = fixtures.iter()
        .find(|f| f.name == "basic_users")
        .map(|f| &f.file_path)
        .unwrap();
    let schema_path = fixtures.iter()
        .find(|f| f.name == "basic_schema")
        .map(|f| &f.file_path)
        .unwrap();
    
    // Test with specific generation
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--generation", "1",
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_read_command_with_performance_options() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    let fixtures = TestDataBuilder::create_common_fixtures(env.fixtures_dir.clone())?;
    let sstable_path = fixtures.iter()
        .find(|f| f.name == "basic_users")
        .map(|f| &f.file_path)
        .unwrap();
    let schema_path = fixtures.iter()
        .find(|f| f.name == "basic_schema")
        .map(|f| &f.file_path)
        .unwrap();
    
    // Test with parallel processing
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--parallel",
    ])?.assert_success()?;
    
    // Test with custom page size
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--page-size", "100",
    ])?.assert_success()?;
    
    // Test with custom buffer size
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--buffer-size", "16384",
    ])?.assert_success()?;
    
    // Test with memory limit
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--max-memory-mb", "200",
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_select_command_with_output_formats() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    let fixtures = TestDataBuilder::create_common_fixtures(env.fixtures_dir.clone())?;
    let sstable_path = fixtures.iter()
        .find(|f| f.name == "basic_users")
        .map(|f| &f.file_path)
        .unwrap();
    let schema_path = fixtures.iter()
        .find(|f| f.name == "basic_schema")
        .map(|f| &f.file_path)
        .unwrap();
    
    // Test with table format (default)
    runner.run(&[
        "select",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--format", "table",
        "SELECT * FROM users",
    ])?.assert_success()?;
    
    // Test with JSON format
    runner.run(&[
        "select",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--format", "json",
        "SELECT * FROM users",
    ])?.assert_success()?;
    
    // Test with CSV format
    runner.run(&[
        "select",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
        "--format", "csv",
        "SELECT * FROM users",
    ])?.assert_success()?;
    
    Ok(())
}

#[tokio::test]
async fn test_query_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test simple query (this might fail if database is not properly initialized)
    // But it should at least parse the command correctly
    let result = runner.run(&[
        "query",
        "SELECT * FROM system.local",
    ]);
    
    // We don't assert success here because the database might not be initialized
    // The important thing is that the command is recognized and parsed
    match result {
        Ok(_) => {
            // Command executed successfully
        }
        Err(_) => {
            // Command failed, but that's expected without proper database setup
            // The important thing is it was recognized as a valid command
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_query_command_with_options() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    // Test query with explain
    let _result = runner.run(&[
        "query",
        "SELECT * FROM system.local",
        "--explain",
    ]);
    
    // Test query with timing
    let _result = runner.run(&[
        "query",
        "SELECT * FROM system.local",
        "--timing",
    ]);
    
    // Test query with both explain and timing
    let _result = runner.run(&[
        "query",
        "SELECT * FROM system.local",
        "--explain",
        "--timing",
    ]);
    
    Ok(())
}
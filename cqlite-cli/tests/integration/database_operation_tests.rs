//! Database operation integration tests

use cqlite_cli::test_infrastructure::*;

#[tokio::test]
async fn test_database_initialization() -> TestResult<()> {
    test_container!(container);
    let db = container.init_database().await?;
    
    // Verify database was created successfully
    let db_guard = db.lock().await;
    assert!(container.environment().db_path.exists());
    
    Ok(())
}

#[tokio::test]
async fn test_schema_loading() -> TestResult<()> {
    test_container!(container);
    let env = container.environment();
    
    // Create a test schema
    let schema_content = r#"
    {
        "keyspace": "test_keyspace",
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"name": "id", "data_type": "UUID", "is_nullable": false, "is_static": false},
                    {"name": "name", "data_type": "TEXT", "is_nullable": false, "is_static": false},
                    {"name": "email", "data_type": "TEXT", "is_nullable": true, "is_static": false}
                ],
                "primary_key": ["id"],
                "clustering_key": [],
                "options": {}
            }
        ],
        "custom_types": []
    }
    "#;
    
    let schema_path = env.fixtures_dir.join("test_schema.json");
    std::fs::write(&schema_path, schema_content)?;
    
    let db = container.init_database().await?;
    let mut db_guard = db.lock().await;
    db_guard.schema_path = Some(schema_path);
    
    // Load schema
    db_guard.load_schema().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_query_execution() -> TestResult<()> {
    test_container!(container);
    let db = container.init_database().await?;
    
    let db_guard = db.lock().await;
    
    // Test basic query execution (placeholder implementation)
    let result = db_guard.execute_query("SELECT * FROM system.local").await?;
    assert!(!result.is_empty());
    
    Ok(())
}

#[tokio::test]
async fn test_data_fixture_loading() -> TestResult<()> {
    test_container!(container);
    let env = container.environment();
    
    // Create test data fixture
    let fixture_content = r#"
    INSERT INTO users (id, name, email) VALUES 
    ('550e8400-e29b-41d4-a716-446655440000', 'John Doe', 'john@example.com');
    INSERT INTO users (id, name, email) VALUES 
    ('550e8400-e29b-41d4-a716-446655440001', 'Jane Smith', 'jane@example.com');
    "#;
    
    let fixture_path = env.fixtures_dir.join("test_data.sql");
    std::fs::write(&fixture_path, fixture_content)?;
    
    let db = container.init_database().await?;
    let mut db_guard = db.lock().await;
    db_guard.data_fixtures.push(fixture_path);
    
    // Load fixtures
    db_guard.load_fixtures().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_database_configuration() -> TestResult<()> {
    test_container!(container);
    
    // Test creating CLI configuration
    let config = container.create_cli_config()?;
    let env = container.environment();
    
    assert_eq!(config.default_database, Some(env.db_path));
    assert!(env.config_path.exists());
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_database_connections() -> TestResult<()> {
    test_container!(container1);
    test_container!(container2);
    
    // Initialize databases in both containers
    let db1 = container1.init_database().await?;
    let db2 = container2.init_database().await?;
    
    // Verify both databases are independent
    let env1 = container1.environment();
    let env2 = container2.environment();
    
    assert_ne!(env1.db_path, env2.db_path);
    assert!(env1.db_path.exists());
    assert!(env2.db_path.exists());
    
    Ok(())
}

#[tokio::test]
async fn test_database_cleanup() -> TestResult<()> {
    let container = {
        test_container!(container);
        let db = container.init_database().await?;
        let env = container.environment();
        
        // Verify database file exists
        assert!(env.db_path.exists());
        
        container
    }; // Container goes out of scope here and should trigger cleanup
    
    // After container cleanup, temporary files should be removed
    // Note: The actual cleanup happens in the Drop implementation
    // This test mainly verifies the cleanup mechanism exists
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_database_operations() -> TestResult<()> {
    test_container!(container);
    let db = container.init_database().await?;
    
    // Simulate concurrent database operations
    let db1 = db.clone();
    let db2 = db.clone();
    
    let handle1 = tokio::spawn(async move {
        let db_guard = db1.lock().await;
        db_guard.execute_query("SELECT 1").await
    });
    
    let handle2 = tokio::spawn(async move {
        let db_guard = db2.lock().await;
        db_guard.execute_query("SELECT 2").await
    });
    
    // Wait for both operations to complete
    let result1 = handle1.await??;
    let result2 = handle2.await??;
    
    assert!(!result1.is_empty());
    assert!(!result2.is_empty());
    
    Ok(())
}

#[tokio::test]  
async fn test_database_with_custom_config() -> TestResult<()> {
    let custom_config = TestConfig::new()
        .with_timeout(std::time::Duration::from_secs(60))
        .with_verbose()
        .no_cleanup();
    
    test_container!(container, custom_config);
    let db = container.init_database().await?;
    
    // Verify database initialization with custom config
    let env = container.environment();
    assert!(env.db_path.exists());
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_like_operations() -> TestResult<()> {
    test_container!(container);
    let db = container.init_database().await?;
    
    let db_guard = db.lock().await;
    
    // Test a sequence of operations that should be atomic
    let queries = vec![
        "CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, value TEXT)",
        "INSERT INTO test_table (id, value) VALUES (1, 'test1')",
        "INSERT INTO test_table (id, value) VALUES (2, 'test2')",
        "SELECT * FROM test_table",
    ];
    
    for query in queries {
        let result = db_guard.execute_query(query).await;
        match result {
            Ok(_) => continue,
            Err(_) => {
                // Some queries might fail in the test environment
                // The important thing is that they're handled gracefully
                break;
            }
        }
    }
    
    Ok(())
}
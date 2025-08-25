//! QA Database Integration Tests
//!
//! This module validates that the database query execution pipeline
//! properly handles INSERT and SELECT operations.

use cqlite_core::{Config, Database};
use std::collections::HashMap;
use tempfile::TempDir;

/// Test actual database INSERT and SELECT operations
#[tokio::test]
async fn test_database_insert_and_select_integration() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config = Config::test_config();
    
    let db = Database::open(temp_dir.path(), config)
        .await
        .expect("Failed to open database");

    // Create table
    let create_result = db
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
        .await
        .expect("Failed to create table");
        
    println!("Create table result: rows_affected={}", create_result.rows_affected);

    // Insert test data
    let insert_result = db
        .execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
        .await
        .expect("Failed to insert data");
        
    println!("Insert result: rows_affected={}", insert_result.rows_affected);
    assert_eq!(insert_result.rows_affected, 1, "INSERT should affect 1 row");

    // Insert more test data  
    let insert_result2 = db
        .execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")
        .await
        .expect("Failed to insert second row");
        
    println!("Insert 2 result: rows_affected={}", insert_result2.rows_affected);
    assert_eq!(insert_result2.rows_affected, 1, "Second INSERT should affect 1 row");

    // Query data - this is where the bug was happening
    let select_result = db
        .execute("SELECT * FROM users")
        .await
        .expect("Failed to execute SELECT");
        
    println!("Select result: rows.len()={}, execution_time_ms={}", 
             select_result.rows.len(), select_result.execution_time_ms);

    // Validate results
    assert_eq!(select_result.rows.len(), 2, "SELECT should return 2 rows");
    
    // Verify row data
    let mut found_alice = false;
    let mut found_bob = false;
    
    for row in &select_result.rows {
        if let Some(name_value) = row.values.get("name") {
            if let cqlite_core::Value::Text(name) = name_value {
                if name == "Alice" {
                    found_alice = true;
                    // Verify Alice's email
                    if let Some(email_value) = row.values.get("email") {
                        if let cqlite_core::Value::Text(email) = email_value {
                            assert_eq!(email, "alice@example.com", "Alice's email should match");
                        }
                    }
                } else if name == "Bob" {
                    found_bob = true;
                    // Verify Bob's email  
                    if let Some(email_value) = row.values.get("email") {
                        if let cqlite_core::Value::Text(email) = email_value {
                            assert_eq!(email, "bob@example.com", "Bob's email should match");
                        }
                    }
                }
            }
        }
    }
    
    assert!(found_alice, "Should find Alice in results");
    assert!(found_bob, "Should find Bob in results");

    // Test specific WHERE clause
    let where_result = db
        .execute("SELECT * FROM users WHERE name = 'Alice'")
        .await
        .expect("Failed to execute WHERE query");
        
    println!("WHERE query result: rows.len()={}", where_result.rows.len());
    assert_eq!(where_result.rows.len(), 1, "WHERE query should return 1 row");

    db.close().await.expect("Failed to close database");
}

/// Test performance scenario with multiple rows
#[tokio::test]
async fn test_database_performance_with_multiple_rows() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config = Config::test_config();
    
    let db = Database::open(temp_dir.path(), config)
        .await
        .expect("Failed to open database");

    // Create table
    db.execute("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT)")
        .await
        .expect("Failed to create table");

    // Insert 10 rows (this is what the performance tests expect)
    for i in 1..=10 {
        let insert_query = format!("INSERT INTO test_data (id, value) VALUES ({}, 'test_value_{}')", i, i);
        let result = db.execute(&insert_query).await.expect("Failed to insert test data");
        assert_eq!(result.rows_affected, 1, "Each INSERT should affect 1 row");
    }

    // Query all data
    let select_result = db
        .execute("SELECT * FROM test_data")
        .await
        .expect("Failed to select test data");
        
    println!("Performance test: found {} rows", select_result.rows.len());
    assert_eq!(select_result.rows.len(), 10, "Should find exactly 10 rows for performance test");

    // Validate each row has expected data
    let mut found_ids = std::collections::HashSet::new();
    for row in &select_result.rows {
        if let Some(id_value) = row.values.get("id") {
            if let cqlite_core::Value::Integer(id) = id_value {
                found_ids.insert(*id);
            } else if let cqlite_core::Value::Text(id_str) = id_value {
                if let Ok(id) = id_str.parse::<i64>() {
                    found_ids.insert(id);
                }
            }
        }
    }
    
    // Should have IDs 1 through 10
    for i in 1..=10 {
        assert!(found_ids.contains(&i), "Should find ID {}", i);
    }

    db.close().await.expect("Failed to close database");
}

/// Test empty result scenarios
#[tokio::test]
async fn test_empty_query_results() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config = Config::test_config();
    
    let db = Database::open(temp_dir.path(), config)
        .await
        .expect("Failed to open database");

    // Create table
    db.execute("CREATE TABLE empty_test (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("Failed to create table");

    // Query empty table
    let empty_result = db
        .execute("SELECT * FROM empty_test")
        .await
        .expect("Failed to query empty table");
        
    println!("Empty query result: rows.len()={}", empty_result.rows.len());
    assert_eq!(empty_result.rows.len(), 0, "Empty table should return 0 rows");
    assert!(empty_result.is_empty(), "Empty result should report as empty");

    // Query with WHERE clause on empty table
    let where_empty_result = db
        .execute("SELECT * FROM empty_test WHERE id = 1")
        .await
        .expect("Failed to query empty table with WHERE");
        
    assert_eq!(where_empty_result.rows.len(), 0, "WHERE on empty table should return 0 rows");

    db.close().await.expect("Failed to close database");
}
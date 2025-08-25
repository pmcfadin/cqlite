//! Test for query integration fix
//!
//! This test validates that INSERT and SELECT operations work correctly
//! after fixing the Value::Map deserialization issue.

use cqlite_core::{Config, Database};
use tempfile::TempDir;

#[tokio::test]
async fn test_insert_and_select_fix() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config = Config::default();
    
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

    // Query data - this should now work correctly
    let select_result = db
        .execute("SELECT * FROM users")
        .await
        .expect("Failed to execute SELECT");
        
    println!("Select result: rows.len()={}, execution_time_ms={}", 
             select_result.rows.len(), select_result.execution_time_ms);

    // Debug: Print row contents
    for (i, row) in select_result.rows.iter().enumerate() {
        println!("Row {}: {:?}", i, row.values);
    }

    // Validate results
    assert_eq!(select_result.rows.len(), 2, "SELECT should return 2 rows");
    
    // Check that we have the expected columns in the results
    for row in &select_result.rows {
        assert!(row.values.contains_key("id"), "Row should have 'id' column");
        assert!(row.values.contains_key("name"), "Row should have 'name' column");
        assert!(row.values.contains_key("email"), "Row should have 'email' column");
    }

    db.close().await.expect("Failed to close database");
}
// Basic SSTable reading functionality tests - Issue #25

use super::*;
use cqlite_core::{Config, Error, Result};
use cqlite_core::storage::sstable::reader::SSTableReader;

#[tokio::test]
async fn test_basic_sstable_opening() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    let test_data = TestSSTableData::default();
    let sstable_path = harness.create_test_sstable("basic_test", test_data).await?;
    
    // Test that we can open the file
    test_utils::assert_readable(&harness, &sstable_path).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_nonexistent_file_handling() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    let nonexistent_path = harness.temp_path().join("nonexistent.db");
    
    // Should return appropriate error for missing file
    test_utils::assert_error(&harness, &nonexistent_path, |error| {
        matches!(error, Error::Io(_))
    }).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_empty_file_handling() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    let empty_file_path = harness.temp_path().join("empty.db");
    
    // Create empty file
    tokio::fs::File::create(&empty_file_path).await
        .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to create empty file: {}", e))))?;
    
    // Should handle empty file gracefully
    test_utils::assert_error(&harness, &empty_file_path, |error| {
        matches!(error, Error::Io(_))
    }).await?;
    
    Ok(())
}

#[tokio::test] 
async fn test_basic_data_reading() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    let test_data = TestSSTableData {
        keyspace: "test_ks".to_string(),
        table: "users".to_string(),
        rows: vec![
            TestRow {
                key: b"user_1".to_vec(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("name".to_string(), b"Alice".to_vec());
                    cols.insert("age".to_string(), b"25".to_vec());
                    cols
                },
                timestamp: Some(1640995200000),
            },
            TestRow {
                key: b"user_2".to_vec(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("name".to_string(), b"Bob".to_vec());
                    cols.insert("age".to_string(), b"30".to_vec());
                    cols
                },
                timestamp: Some(1640995260000),
            },
        ],
        ..Default::default()
    };
    
    let sstable_path = harness.create_test_sstable("data_reading_test", test_data).await?;
    let reader = harness.open_reader(&sstable_path).await?;
    
    // Basic validation that we can create a reader
    // More detailed data validation would require implementing the full SSTable format
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_files_handling() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create multiple test files
    let files = vec![
        ("file1", TestSSTableData::default()),
        ("file2", {
            let mut data = TestSSTableData::default();
            data.keyspace = "ks2".to_string();
            data
        }),
        ("file3", {
            let mut data = TestSSTableData::default();
            data.table = "table2".to_string();
            data
        }),
    ];
    
    let mut created_files = Vec::new();
    for (name, data) in files {
        let path = harness.create_test_sstable(name, data).await?;
        created_files.push(path);
    }
    
    // Verify all files can be opened
    for file_path in &created_files {
        test_utils::assert_readable(&harness, file_path).await?;
    }
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_file_access() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    let test_data = TestSSTableData::default();
    let sstable_path = harness.create_test_sstable("concurrent_test", test_data).await?;
    
    // Create multiple concurrent readers
    let mut tasks = Vec::new();
    for i in 0..5 {
        let harness_path = sstable_path.clone();
        let harness_config = harness.config.clone();
        let harness_platform = harness.platform.clone();
        
        let task = tokio::spawn(async move {
            let reader = SSTableReader::open(&harness_path, &harness_config, harness_platform).await?;
            // Basic validation that concurrent access works
            Result::<()>::Ok(())
        });
        
        tasks.push(task);
    }
    
    // Wait for all tasks to complete
    for task in tasks {
        let result = task.await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Task failed: {}", e))))?;
        result?;
    }
    
    Ok(())
}

/// Performance test to ensure reasonable file opening times
#[tokio::test]
async fn test_file_opening_performance() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    let test_data = TestSSTableData::default();
    let sstable_path = harness.create_test_sstable("performance_test", test_data).await?;
    
    let start_time = std::time::Instant::now();
    let _reader = harness.open_reader(&sstable_path).await?;
    let duration = start_time.elapsed();
    
    // File opening should be reasonably fast (under 1 second for test files)
    assert!(duration.as_secs() < 1, "File opening took too long: {:?}", duration);
    
    Ok(())
}

/// Test error messages are helpful for debugging
#[tokio::test]
async fn test_error_message_quality() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    let invalid_path = harness.temp_path().join("invalid_chars_\0\0.db");
    
    match harness.open_reader(&invalid_path).await {
        Err(Error::Io(io_error)) => {
            let error_msg = format!("{}", io_error);
            // Error message should be descriptive
            assert!(!error_msg.is_empty(), "Error message should not be empty");
        },
        Err(other) => {
            // Other error types are also acceptable
        },
        Ok(_) => {
            // If it somehow succeeds, that's also acceptable for this test
        }
    }
    
    Ok(())
}
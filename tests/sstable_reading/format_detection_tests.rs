// SSTable format detection and version compatibility tests - Issue #25

use super::*;
use cqlite_core::{Config, Error, Result};

/// Test different Cassandra SSTable format versions
#[tokio::test]
async fn test_cassandra_version_detection() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test different Cassandra versions
    let versions = vec!["3.11", "4.0", "5.0"];
    
    for version in versions {
        println!("Testing Cassandra version {}", version);
        
        let test_data = TestSSTableData {
            version: version.to_string(),
            keyspace: format!("test_ks_{}", version.replace(".", "_")),
            table: "version_test".to_string(),
            rows: vec![TestRow {
                key: format!("version_{}_key", version).into_bytes(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("version".to_string(), version.as_bytes().to_vec());
                    cols.insert("data".to_string(), b"test data".to_vec());
                    cols
                },
                timestamp: Some(1640995200000),
            }],
            compression: None,
        };
        
        let sstable_path = harness.create_test_sstable(&format!("version_{}", version), test_data).await?;
        
        // Verify file was created
        assert!(sstable_path.exists(), "SSTable for version {} should be created", version);
        
        // Test that reader can handle version (may fail due to format, but shouldn't crash)
        let _result = harness.open_reader(&sstable_path).await;
        // We don't assert success since our test files aren't real SSTables
    }
    
    Ok(())
}

/// Test SSTable format magic number detection
#[tokio::test]
async fn test_magic_number_validation() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test with different magic numbers
    let magic_tests = vec![
        ("valid", b"SSTable"),
        ("invalid_short", b"SST"),
        ("invalid_wrong", b"BADMAG"),
        ("empty", b""),
    ];
    
    for (test_name, magic) in magic_tests {
        let test_file = harness.temp_path().join(format!("{}.db", test_name));
        
        // Create file with specific magic number
        use tokio::fs::File;
        use tokio::io::AsyncWriteExt;
        
        let mut file = File::create(&test_file).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to create {}: {}", test_name, e))))?;
        
        file.write_all(magic).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write magic: {}", e))))?;
        
        file.write_all(b"5.0test_data").await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write data: {}", e))))?;
        
        file.flush().await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to flush: {}", e))))?;
        
        // Test reading (should handle gracefully)
        let _result = harness.open_reader(&test_file).await;
        println!("Magic number test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test file size and structure validation
#[tokio::test]
async fn test_file_structure_validation() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test different file structures
    let structure_tests = vec![
        ("minimal", 10),   // Very small file
        ("small", 100),    // Small file
        ("medium", 1000),  // Medium file
        ("large", 10000),  // Larger file
    ];
    
    for (test_name, size) in structure_tests {
        let test_file = harness.temp_path().join(format!("structure_{}.db", test_name));
        
        // Create file with specific size
        use tokio::fs::File;
        use tokio::io::AsyncWriteExt;
        
        let mut file = File::create(&test_file).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to create {}: {}", test_name, e))))?;
        
        // Write header
        file.write_all(b"SSTable5.0").await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write header: {}", e))))?;
        
        // Write data to reach desired size
        let remaining_size = size.saturating_sub(10); // Account for header
        let data_chunk = vec![b'x'; std::cmp::min(remaining_size, 1000)];
        let chunks = remaining_size / data_chunk.len();
        
        for _ in 0..chunks {
            file.write_all(&data_chunk).await
                .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write chunk: {}", e))))?;
        }
        
        let final_size = remaining_size % data_chunk.len();
        if final_size > 0 {
            file.write_all(&vec![b'x'; final_size]).await
                .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write final: {}", e))))?;
        }
        
        file.flush().await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to flush: {}", e))))?;
        
        // Verify file size
        let metadata = tokio::fs::metadata(&test_file).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to get metadata: {}", e))))?;
        
        assert!(metadata.len() >= size as u64, "File {} should be at least {} bytes", test_name, size);
        
        // Test reading (should handle various sizes gracefully)
        let _result = harness.open_reader(&test_file).await;
        println!("Structure test '{}' completed (size: {} bytes)", test_name, metadata.len());
    }
    
    Ok(())
}

/// Test metadata and header parsing
#[tokio::test]
async fn test_metadata_parsing() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test with different metadata combinations
    let metadata_tests = vec![
        ("basic", "test_ks", "test_table", None),
        ("with_compression", "compressed_ks", "compressed_table", Some("LZ4".to_string())),
        ("long_names", "very_long_keyspace_name_for_testing", "very_long_table_name_for_testing", None),
        ("special_chars", "test-ks.v1", "test_table_2023", Some("Snappy".to_string())),
    ];
    
    for (test_name, keyspace, table, compression) in metadata_tests {
        let test_data = TestSSTableData {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            compression,
            version: "5.0".to_string(),
            rows: vec![TestRow {
                key: format!("{}_key", test_name).into_bytes(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("ks".to_string(), keyspace.as_bytes().to_vec());
                    cols.insert("table".to_string(), table.as_bytes().to_vec());
                    cols
                },
                timestamp: Some(1640995200000),
            }],
        };
        
        let sstable_path = harness.create_test_sstable(&format!("metadata_{}", test_name), test_data).await?;
        
        // Test reading metadata (may fail due to format, but shouldn't crash)
        let _result = harness.open_reader(&sstable_path).await;
        println!("Metadata test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test corrupted file handling
#[tokio::test]
async fn test_corrupted_file_handling() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create a valid test file first
    let test_data = TestSSTableData::default();
    let original_path = harness.create_test_sstable("original", test_data).await?;
    
    // Create various corrupted versions
    let corruption_tests = vec![
        ("truncated_header", 5),    // Cut off in header
        ("truncated_middle", 50),   // Cut off in middle
        ("truncated_end", 90),      // Cut off near end
    ];
    
    for (test_name, truncate_percent) in corruption_tests {
        // Read original file
        let original_data = tokio::fs::read(&original_path).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to read original: {}", e))))?;
        
        // Create truncated version
        let truncate_size = (original_data.len() * truncate_percent) / 100;
        let truncated_data = &original_data[..truncate_size];
        
        let corrupted_path = harness.temp_path().join(format!("corrupted_{}.db", test_name));
        tokio::fs::write(&corrupted_path, truncated_data).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write corrupted: {}", e))))?;
        
        // Test reading corrupted file (should handle gracefully)
        test_utils::assert_error(&harness, &corrupted_path, |error| {
            matches!(error, Error::Io(_))
        }).await?;
        
        println!("Corruption test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test concurrent format detection
#[tokio::test]
async fn test_concurrent_format_detection() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create multiple test files with different formats
    let mut file_paths = Vec::new();
    
    for i in 0..5 {
        let test_data = TestSSTableData {
            keyspace: format!("concurrent_ks_{}", i),
            version: match i % 3 {
                0 => "3.11".to_string(),
                1 => "4.0".to_string(),
                _ => "5.0".to_string(),
            },
            ..Default::default()
        };
        
        let path = harness.create_test_sstable(&format!("concurrent_{}", i), test_data).await?;
        file_paths.push(path);
    }
    
    // Test concurrent format detection
    let mut tasks = Vec::new();
    
    for (i, path) in file_paths.iter().enumerate() {
        let harness_config = harness.config.clone();
        let harness_platform = harness.platform.clone();
        let test_path = path.clone();
        
        let task = tokio::spawn(async move {
            let _result = cqlite_core::storage::sstable::reader::SSTableReader::open(
                &test_path, &harness_config, harness_platform
            ).await;
            
            println!("Concurrent format detection {} completed", i);
            Result::<()>::Ok(())
        });
        
        tasks.push(task);
    }
    
    // Wait for all tasks
    for (i, task) in tasks.into_iter().enumerate() {
        let result = task.await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Task {} failed: {}", i, e))))?;
        result?;
    }
    
    Ok(())
}
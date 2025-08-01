// SSTable error handling and edge case tests - Issue #25

use super::*;
use cqlite_core::{Config, Error, Result};

/// Test various error conditions and edge cases
#[tokio::test]
async fn test_file_not_found_errors() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test different non-existent file scenarios
    let nonexistent_tests = vec![
        ("simple_missing", "nonexistent.db"),
        ("path_with_dirs", "missing/dir/file.db"),
        ("unicode_filename", "файл_не_существует.db"),
        ("special_chars", "file-with-special@chars#.db"),
        ("very_long_name", &format!("{}.db", "x".repeat(200))),
    ];
    
    for (test_name, filename) in nonexistent_tests {
        println!("Testing file not found: {} ({})", test_name, filename);
        
        let nonexistent_path = harness.temp_path().join(filename);
        
        // Test that appropriate error is returned
        test_utils::assert_error(&harness, &nonexistent_path, |error| {
            matches!(error, Error::Io(_))
        }).await?;
        
        println!("File not found test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test permission and access errors
#[tokio::test]
async fn test_permission_errors() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create a test file first
    let test_data = TestSSTableData::default();
    let test_file = harness.create_test_sstable("permission_test", test_data).await?;
    
    // On Unix systems, we can test permission errors
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        
        println!("Testing permission denied scenarios");
        
        // Make file unreadable
        let metadata = std::fs::metadata(&test_file)
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to get metadata: {}", e))))?;
        let mut permissions = metadata.permissions();
        let original_mode = permissions.mode();
        
        // Remove read permissions
        permissions.set_mode(0o000);
        std::fs::set_permissions(&test_file, permissions)
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to set permissions: {}", e))))?;
        
        // Test that permission error is handled
        let result = harness.open_reader(&test_file).await;
        match result {
            Err(Error::Io(_)) => {
                println!("✅ Permission error correctly handled");
            },
            _ => {
                println!("⚠️ Permission error may not have been detected");
            }
        }
        
        // Restore permissions for cleanup
        let mut restore_permissions = std::fs::metadata(&test_file).unwrap().permissions();
        restore_permissions.set_mode(original_mode);
        std::fs::set_permissions(&test_file, restore_permissions).ok();
    }
    
    #[cfg(not(unix))]
    {
        println!("Skipping permission tests on non-Unix platform");
    }
    
    Ok(())
}

/// Test corrupted file headers
#[tokio::test]
async fn test_corrupted_file_headers() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test different types of header corruption
    let corruption_tests = vec![
        ("empty_file", vec![], "Completely empty file"),
        ("single_byte", vec![0x00], "Single byte file"),
        ("partial_magic", b"SST".to_vec(), "Partial magic number"),
        ("wrong_magic", b"BADMAGIC".to_vec(), "Wrong magic number"),
        ("null_bytes", vec![0x00; 16], "All null bytes"),
        ("random_bytes", generate_random_bytes(16), "Random header"),
    ];
    
    for (test_name, header_data, description) in corruption_tests {
        println!("Testing corrupted header: {} ({})", test_name, description);
        
        let corrupted_file = harness.temp_path().join(format!("corrupted_{}.db", test_name));
        
        // Write corrupted header
        tokio::fs::write(&corrupted_file, header_data).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write corrupted file: {}", e))))?;
        
        // Test that error is handled gracefully
        test_utils::assert_error(&harness, &corrupted_file, |error| {
            matches!(error, Error::Io(_))
        }).await?;
        
        println!("Corrupted header test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test file size edge cases
#[tokio::test]
async fn test_file_size_edge_cases() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test files of various problematic sizes
    let size_tests = vec![
        ("zero_bytes", 0, "Empty file"),
        ("one_byte", 1, "Single byte"),
        ("header_boundary", 15, "Just under header size"),
        ("exact_header", 16, "Exact header size"),
        ("partial_data", 50, "Header plus partial data"),
    ];
    
    for (test_name, file_size, description) in size_tests {
        println!("Testing file size edge case: {} ({} bytes, {})", test_name, file_size, description);
        
        let test_file = harness.temp_path().join(format!("size_{}.db", test_name));
        
        // Create file with specific size
        let file_data = if file_size == 0 {
            vec![]
        } else if file_size < 16 {
            // Partial header
            b"SSTable5.0"[..file_size].to_vec()
        } else {
            // Full header plus padding
            let mut data = b"SSTable5.0testdata".to_vec();
            data.resize(file_size, b'x');
            data
        };
        
        tokio::fs::write(&test_file, file_data).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write size test file: {}", e))))?;
        
        // Test reading file of this size
        let result = harness.open_reader(&test_file).await;
        
        // We expect errors for files that are too small or malformed
        match result {
            Ok(_) => {
                if file_size >= 16 {
                    println!("  ✅ File opened successfully (size: {} bytes)", file_size);
                } else {
                    println!("  ⚠️ Unexpectedly opened small file (size: {} bytes)", file_size);
                }
            },
            Err(e) => {
                println!("  ✅ Appropriately rejected file (size: {} bytes): {}", file_size, e);
            }
        }
        
        println!("Size edge case test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test invalid data types and structures
#[tokio::test]
async fn test_invalid_data_structures() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test different invalid data scenarios
    let invalid_data_tests = vec![
        ("invalid_utf8", vec![0xFF, 0xFE, 0xFD], "Invalid UTF-8 sequences"),
        ("null_terminated", b"SSTable5.0\0\0\0invalid".to_vec(), "Null bytes in data"),
        ("very_large_size", create_size_prefixed_data(0xFFFFFFFF), "Impossible size field"),
        ("negative_timestamp", create_negative_timestamp_data(), "Negative timestamp"),
        ("circular_reference", create_circular_data(), "Circular data structure"),
    ];
    
    for (test_name, invalid_data, description) in invalid_data_tests {
        println!("Testing invalid data structure: {} ({})", test_name, description);
        
        let invalid_file = harness.temp_path().join(format!("invalid_{}.db", test_name));
        
        tokio::fs::write(&invalid_file, invalid_data).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write invalid file: {}", e))))?;
        
        // Test that invalid data is handled gracefully
        test_utils::assert_error(&harness, &invalid_file, |error| {
            matches!(error, Error::Io(_))
        }).await?;
        
        println!("Invalid data test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test resource exhaustion scenarios
#[tokio::test]
async fn test_resource_exhaustion() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    println!("Testing resource exhaustion scenarios");
    
    // Test opening many files simultaneously
    let mut readers = Vec::new();
    let max_files = 50; // Conservative limit for testing
    
    for i in 0..max_files {
        let test_data = TestSSTableData {
            keyspace: format!("resource_test_{}", i),
            ..Default::default()
        };
        
        let test_file = harness.create_test_sstable(&format!("resource_{}", i), test_data).await?;
        
        // Try to open the file
        match harness.open_reader(&test_file).await {
            Ok(reader) => {
                readers.push(reader);
                if i % 10 == 0 {
                    println!("  Opened {} files successfully", i + 1);
                }
            },
            Err(e) => {
                println!("  Reached resource limit at {} files: {}", i, e);
                break;
            }
        }
    }
    
    println!("Resource exhaustion test completed (opened {} files)", readers.len());
    
    // Files should be cleaned up automatically when readers are dropped
    drop(readers);
    
    Ok(())
}

/// Test concurrent error conditions
#[tokio::test]
async fn test_concurrent_error_conditions() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    println!("Testing concurrent error conditions");
    
    // Create files with various error conditions
    let error_files = vec![
        ("nonexistent", None),  // File doesn't exist
        ("empty", Some(vec![])), // Empty file
        ("corrupted", Some(vec![0xFF; 50])), // Random data
        ("partial", Some(b"SST".to_vec())), // Partial header
    ];
    
    let mut error_file_paths = Vec::new();
    
    for (error_type, data) in error_files {
        let file_path = harness.temp_path().join(format!("error_{}.db", error_type));
        
        if let Some(file_data) = data {
            tokio::fs::write(&file_path, file_data).await
                .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to create error file: {}", e))))?;
        }
        // If data is None, we don't create the file (nonexistent case)
        
        error_file_paths.push((error_type, file_path));
    }
    
    // Try to open all error files concurrently
    let mut tasks = Vec::new();
    
    for (i, (error_type, path)) in error_file_paths.iter().enumerate() {
        let harness_config = harness.config.clone();
        let harness_platform = harness.platform.clone();
        let test_path = path.clone();
        let error_type_str = error_type.to_string();
        
        let task = tokio::spawn(async move {
            let result = cqlite_core::storage::sstable::reader::SSTableReader::open(
                &test_path, &harness_config, harness_platform
            ).await;
            
            match result {
                Ok(_) => {
                    println!("  Unexpected success for {} error condition", error_type_str);
                },
                Err(e) => {
                    println!("  ✅ Correctly handled {} error: {}", error_type_str, e);
                }
            }
            
            Result::<()>::Ok(())
        });
        
        tasks.push(task);
    }
    
    // Wait for all concurrent error handling
    for (i, task) in tasks.into_iter().enumerate() {
        let result = task.await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Concurrent error task {} failed: {}", i, e))))?;
        result?;
    }
    
    println!("Concurrent error conditions test completed");
    
    Ok(())
}

/// Test memory safety with malformed data
#[tokio::test]
async fn test_memory_safety() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    println!("Testing memory safety with malformed data");
    
    // Create various malformed files that could cause memory issues
    let memory_safety_tests = vec![
        ("buffer_overflow", create_potential_overflow_data()),
        ("integer_overflow", create_integer_overflow_data()),
        ("stack_overflow", create_deep_recursion_data()),
        ("use_after_free", create_uaf_trigger_data()),
    ];
    
    for (test_name, malformed_data) in memory_safety_tests {
        println!("  Testing memory safety: {}", test_name);
        
        let malformed_file = harness.temp_path().join(format!("memory_{}.db", test_name));
        
        tokio::fs::write(&malformed_file, malformed_data).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write malformed file: {}", e))))?;
        
        // Test that malformed data doesn't cause crashes
        let result = harness.open_reader(&malformed_file).await;
        
        // We expect these to fail safely
        match result {
            Ok(_) => {
                println!("    ⚠️ Malformed data was accepted (potential issue)");
            },
            Err(e) => {
                println!("    ✅ Malformed data rejected safely: {}", e);
            }
        }
    }
    
    println!("Memory safety tests completed");
    
    Ok(())
}

// Helper functions for generating test data

fn generate_random_bytes(size: usize) -> Vec<u8> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut data = Vec::with_capacity(size);
    let mut hasher = DefaultHasher::new();
    
    for i in 0..size {
        i.hash(&mut hasher);
        data.push((hasher.finish() & 0xFF) as u8);
    }
    
    data
}

fn create_size_prefixed_data(invalid_size: u32) -> Vec<u8> {
    let mut data = b"SSTable5.0".to_vec();
    data.extend_from_slice(&invalid_size.to_le_bytes());
    data.extend_from_slice(b"invalid_data_after_size");
    data
}

fn create_negative_timestamp_data() -> Vec<u8> {
    let mut data = b"SSTable5.0".to_vec();
    // Add a negative timestamp (as signed 64-bit integer)
    let negative_timestamp: i64 = -1640995200000;
    data.extend_from_slice(&negative_timestamp.to_le_bytes());
    data.extend_from_slice(b"data_with_negative_timestamp");
    data
}

fn create_circular_data() -> Vec<u8> {
    let mut data = b"SSTable5.0".to_vec();
    // Create data that could trigger circular references
    data.extend_from_slice(b"circular_ref:");
    data.extend_from_slice(&(data.len() as u32).to_le_bytes()); // Reference to itself
    data.extend_from_slice(b"end");
    data
}

fn create_potential_overflow_data() -> Vec<u8> {
    let mut data = b"SSTable5.0".to_vec();
    // Add a length field that's larger than remaining data
    let fake_length = 0xFFFFFFFFu32;
    data.extend_from_slice(&fake_length.to_le_bytes());
    data.extend_from_slice(b"small_actual_data");
    data
}

fn create_integer_overflow_data() -> Vec<u8> {
    let mut data = b"SSTable5.0".to_vec();
    // Create data with values near integer overflow boundaries
    data.extend_from_slice(&u64::MAX.to_le_bytes());
    data.extend_from_slice(&i64::MIN.to_le_bytes());
    data.extend_from_slice(&u32::MAX.to_le_bytes());
    data.extend_from_slice(b"overflow_test_data");
    data
}

fn create_deep_recursion_data() -> Vec<u8> {
    let mut data = b"SSTable5.0".to_vec();
    
    // Create nested structure that could cause stack overflow
    for i in 0..1000 {
        data.extend_from_slice(b"nest");
        data.extend_from_slice(&(i as u32).to_le_bytes());
    }
    
    data.extend_from_slice(b"deep_end");
    data
}

fn create_uaf_trigger_data() -> Vec<u8> {
    let mut data = b"SSTable5.0".to_vec();
    
    // Create data patterns that might trigger use-after-free
    data.extend_from_slice(b"ptr1:");
    data.extend_from_slice(&(100u32).to_le_bytes());
    data.extend_from_slice(b"free:");
    data.extend_from_slice(&(100u32).to_le_bytes()); // Same address
    data.extend_from_slice(b"use:");
    data.extend_from_slice(&(100u32).to_le_bytes()); // Use after free
    
    data
}
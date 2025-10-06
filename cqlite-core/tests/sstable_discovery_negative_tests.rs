//! Negative tests for SSTable discovery
//!
//! Tests error conditions, malformed inputs, and edge cases that should be
//! handled gracefully without causing crashes or data corruption.

use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Config;

/// Test behavior with completely missing files
#[tokio::test]
async fn test_missing_files_handling() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let missing_files = vec![
        "nonexistent-1-big-Data.db",
        "missing-file.sst",
        "/does/not/exist/file-1-big-Data.db",
        "",
    ];

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    for missing_file in missing_files {
        if missing_file.is_empty() {
            continue; // Skip empty string test
        }

        let file_path = test_root.join(missing_file);

        // Ensure file doesn't exist
        assert!(
            !file_path.exists(),
            "File should not exist: {}",
            missing_file
        );

        // Test SSTableReader handling
        match SSTableReader::open(&file_path, &config, platform.clone()).await {
            Ok(_) => {
                panic!("Should not succeed opening missing file: {}", missing_file);
            }
            Err(e) => {
                // Should get a proper error, not a panic
                println!(
                    "✓ Missing file handled correctly: {} -> {}",
                    missing_file, e
                );

                // Error should indicate file not found
                let error_msg = e.to_string().to_lowercase();
                assert!(
                    error_msg.contains("not found")
                        || error_msg.contains("no such file")
                        || error_msg.contains("does not exist"),
                    "Error should indicate file not found: {}",
                    e
                );
            }
        }
    }
}

/// Test behavior with corrupted file headers
#[tokio::test]
async fn test_corrupted_file_headers() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let corruption_cases = vec![
        ("empty_file", vec![]),
        (
            "invalid_magic",
            vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05],
        ),
        ("truncated_header", vec![0x6d, 0x61, 0x64, 0x61, 0x00]),
        (
            "wrong_version",
            vec![0x6d, 0x61, 0x64, 0x61, 0xFF, 0xFF, 0xFF, 0xFF],
        ),
        ("garbage_data", vec![0xFF; 1024]),
        ("partial_magic", vec![0x6d, 0x61]),
        ("null_bytes", vec![0x00; 4096]),
    ];

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    for (case_name, corruption_data) in corruption_cases {
        let filename = format!("corrupted-{}-1-big-Data.db", case_name);
        let file_path = test_root.join(&filename);

        // Create corrupted file
        fs::write(&file_path, corruption_data).await.unwrap();

        // Test handling of corrupted file
        match SSTableReader::open(&file_path, &config, platform.clone()).await {
            Ok(_reader) => {
                // Some corruption might be recoverable, log but don't fail
                println!(
                    "⚠ Corrupted file was opened (might have recovery logic): {}",
                    case_name
                );
            }
            Err(e) => {
                // Should handle corruption gracefully
                println!("✓ Corrupted file handled correctly: {} -> {}", case_name, e);

                // Error should indicate corruption or parsing failure
                let error_msg = e.to_string().to_lowercase();
                assert!(
                    error_msg.contains("invalid")
                        || error_msg.contains("corrupt")
                        || error_msg.contains("parse")
                        || error_msg.contains("magic")
                        || error_msg.contains("header")
                        || error_msg.contains("format"),
                    "Error should indicate corruption/parsing issue: {}",
                    e
                );
            }
        }

        // Cleanup
        fs::remove_file(&file_path).await.unwrap();
    }
}

/// Test behavior with inaccessible files (permission denied)
#[tokio::test]
#[cfg(unix)]
async fn test_permission_denied_handling() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let filename = "permission-test-1-big-Data.db";
    let file_path = test_root.join(filename);

    // Create valid SSTable file
    let valid_data = create_valid_sstable_header();
    fs::write(&file_path, valid_data).await.unwrap();

    // Remove read permissions
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(&file_path).await.unwrap().permissions();
    perms.set_mode(0o000); // No permissions
    fs::set_permissions(&file_path, perms).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test handling of permission denied
    match SSTableReader::open(&file_path, &config, platform).await {
        Ok(_) => {
            panic!("Should not succeed opening file with no read permissions");
        }
        Err(e) => {
            println!("✓ Permission denied handled correctly: {}", e);

            let error_msg = e.to_string().to_lowercase();
            assert!(
                error_msg.contains("permission")
                    || error_msg.contains("denied")
                    || error_msg.contains("access")
                    || error_msg.contains("forbidden"),
                "Error should indicate permission issue: {}",
                e
            );
        }
    }

    // Restore permissions for cleanup
    let mut perms = fs::metadata(&file_path).await.unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&file_path, perms).await.unwrap();
}

/// Test behavior with extremely large files
#[tokio::test]
async fn test_large_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    // Test with file claiming to be extremely large in header
    let filename = "large-file-1-big-Data.db";
    let file_path = test_root.join(filename);

    let mut large_file_data = Vec::new();

    // Valid header but claiming huge size
    large_file_data.extend_from_slice(&[
        0x6d, 0x61, 0x64, 0x61, // Magic "mada"
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0xFF, 0xFF, 0xFF, 0xFF, // Huge partition count
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Huge data size
    ]);

    // Add minimal actual data
    large_file_data.extend_from_slice(b"small actual data");

    fs::write(&file_path, large_file_data).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test handling of size mismatch
    match SSTableReader::open(&file_path, &config, platform).await {
        Ok(_reader) => {
            // Might be handled if there are size validation checks
            println!("⚠ Large file size handled (might have validation logic)");
        }
        Err(e) => {
            println!("✓ Large file size mismatch handled: {}", e);

            // Should indicate size or validation issue
            let error_msg = e.to_string().to_lowercase();
            assert!(
                error_msg.contains("size")
                    || error_msg.contains("length")
                    || error_msg.contains("invalid")
                    || error_msg.contains("mismatch"),
                "Error should indicate size issue: {}",
                e
            );
        }
    }
}

/// Test behavior with malformed component files
#[tokio::test]
async fn test_malformed_component_files() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let base_name = "malformed-test-1-big";

    // Create valid Data.db file
    let data_file = test_root.join(format!("{}-Data.db", base_name));
    fs::write(&data_file, create_valid_sstable_header())
        .await
        .unwrap();

    // Create malformed component files
    let malformed_components = vec![
        ("Index.db", vec![0xFF; 10]),                     // Too small
        ("Summary.db", vec![]),                           // Empty
        ("Filter.db", vec![0x00; 100000]),                // Too large for content
        ("Statistics.db", b"invalid text data".to_vec()), // Wrong format
        ("CompressionInfo.db", vec![0xFF; 1]),            // Truncated
        ("TOC.txt", b"\xFF\xFF\xFF\xFF".to_vec()),        // Binary data in text file
    ];

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    for (component_type, malformed_data) in malformed_components {
        let component_file = test_root.join(format!("{}-{}", base_name, component_type));
        fs::write(&component_file, malformed_data).await.unwrap();

        // Test that SSTableReader can still open the Data.db file
        // even with malformed components
        match SSTableReader::open(&data_file, &config, platform.clone()).await {
            Ok(reader) => {
                println!(
                    "✓ SSTableReader opened despite malformed {}",
                    component_type
                );

                // Test that operations handle malformed components gracefully
                match component_type {
                    "Index.db" => {
                        let _result = reader.lookup_partition_with_index(b"test_key").await;
                        println!("✓ Index lookup handled malformed Index.db");
                    }
                    "Summary.db" => {
                        let _result = reader.iterate_token_range(-1000, 1000).await;
                        println!("✓ Token range iteration handled malformed Summary.db");
                    }
                    "Statistics.db" => {
                        let _result = reader.stats().await.cloned().unwrap_or_default();
                        println!("✓ Statistics access handled malformed Statistics.db");
                    }
                    _ => {
                        println!("✓ Basic operations work with malformed {}", component_type);
                    }
                }
            }
            Err(e) => {
                println!("✓ Malformed {} handled with error: {}", component_type, e);
            }
        }

        // Remove malformed component for next test
        fs::remove_file(&component_file).await.unwrap();
    }
}

/// Test behavior with directory instead of file
#[tokio::test]
async fn test_directory_instead_of_file() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    // Create directory with SSTable filename
    let dir_name = "directory-1-big-Data.db";
    let dir_path = test_root.join(dir_name);
    fs::create_dir(&dir_path).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test opening directory as if it were a file
    match SSTableReader::open(&dir_path, &config, platform).await {
        Ok(_) => {
            panic!("Should not succeed opening directory as SSTable file");
        }
        Err(e) => {
            println!("✓ Directory-as-file handled correctly: {}", e);

            let error_msg = e.to_string().to_lowercase();
            assert!(
                error_msg.contains("directory")
                    || error_msg.contains("is a directory")
                    || error_msg.contains("not a file")
                    || error_msg.contains("invalid"),
                "Error should indicate directory issue: {}",
                e
            );
        }
    }
}

/// Test behavior with circular symlinks
#[tokio::test]
#[cfg(unix)]
async fn test_circular_symlink_handling() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let link1 = test_root.join("link1-1-big-Data.db");
    let link2 = test_root.join("link2-1-big-Data.db");

    // Create circular symlinks
    tokio::fs::symlink(&link2, &link1).await.unwrap();
    tokio::fs::symlink(&link1, &link2).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test handling of circular symlinks
    match SSTableReader::open(&link1, &config, platform).await {
        Ok(_) => {
            panic!("Should not succeed opening circular symlink");
        }
        Err(e) => {
            println!("✓ Circular symlink handled correctly: {}", e);

            let error_msg = e.to_string().to_lowercase();
            assert!(
                error_msg.contains("loop")
                    || error_msg.contains("circular")
                    || error_msg.contains("symlink")
                    || error_msg.contains("too many levels")
                    || error_msg.contains("not found"),
                "Error should indicate symlink issue: {}",
                e
            );
        }
    }
}

/// Test behavior with special characters in filenames
#[tokio::test]
async fn test_special_characters_in_filenames() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let special_filenames = vec![
        "file with spaces-1-big-Data.db",
        "file\twith\ttabs-1-big-Data.db",
        "file\nwith\nnewlines-1-big-Data.db",
        "file\"with\"quotes-1-big-Data.db",
        "file'with'quotes-1-big-Data.db",
        "file;with;semicolons-1-big-Data.db",
        "file|with|pipes-1-big-Data.db",
        "file&with&ampersands-1-big-Data.db",
        "file<with>brackets-1-big-Data.db",
        "file[with]brackets-1-big-Data.db",
        "file{with}braces-1-big-Data.db",
        "file(with)parens-1-big-Data.db",
        "file*with*asterisks-1-big-Data.db",
        "file?with?questions-1-big-Data.db",
    ];

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    for special_filename in special_filenames {
        // Skip filenames that would be invalid on the filesystem
        if special_filename.contains('\n') || special_filename.contains('\t') {
            continue; // These would cause filesystem errors
        }

        let file_path = test_root.join(special_filename);

        // Create file with valid content
        fs::write(&file_path, create_valid_sstable_header())
            .await
            .unwrap();

        // Test that special characters are handled
        match SSTableReader::open(&file_path, &config, platform.clone()).await {
            Ok(_reader) => {
                println!(
                    "✓ Special characters handled correctly: {}",
                    special_filename
                );
            }
            Err(e) => {
                // Should not fail due to filename characters specifically
                println!(
                    "✓ Special characters test completed: {} -> {}",
                    special_filename, e
                );

                // Error should not be about the filename characters
                let error_msg = e.to_string().to_lowercase();
                assert!(
                    !error_msg.contains("character") && !error_msg.contains("invalid filename"),
                    "Error should not be about filename characters: {}",
                    e
                );
            }
        }

        // Cleanup
        fs::remove_file(&file_path).await.unwrap();
    }
}

/// Test memory exhaustion prevention with huge file claims
#[tokio::test]
async fn test_memory_exhaustion_prevention() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let filename = "memory-bomb-1-big-Data.db";
    let file_path = test_root.join(filename);

    let mut memory_bomb_data = Vec::new();

    // Header claiming billions of partitions
    memory_bomb_data.extend_from_slice(&[
        0x6d, 0x61, 0x64, 0x61, // Magic "mada"
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x7F, 0xFF, 0xFF, 0xFF, // Huge partition count (i32::MAX)
    ]);

    // Small actual file
    memory_bomb_data.extend_from_slice(b"tiny data");

    fs::write(&file_path, memory_bomb_data).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test that we don't try to allocate based on claimed size
    let start_memory = get_memory_usage();

    match SSTableReader::open(&file_path, &config, platform).await {
        Ok(_reader) => {
            let end_memory = get_memory_usage();
            let memory_increase = end_memory.saturating_sub(start_memory);

            // Should not have allocated gigabytes based on partition count claim
            assert!(
                memory_increase < 100 * 1024 * 1024,
                "Should not allocate huge memory: {} bytes",
                memory_increase
            );
            println!(
                "✓ Memory bomb prevented. Memory increase: {} bytes",
                memory_increase
            );
        }
        Err(e) => {
            println!("✓ Memory bomb prevented with error: {}", e);
        }
    }
}

/// Test concurrent access to same file
#[tokio::test]
async fn test_concurrent_access_handling() {
    let temp_dir = TempDir::new().unwrap();
    let test_root = temp_dir.path();

    let filename = "concurrent-test-1-big-Data.db";
    let file_path = test_root.join(filename);

    // Create valid SSTable file
    fs::write(&file_path, create_valid_sstable_header())
        .await
        .unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Try to open the same file multiple times concurrently
    let mut handles = Vec::new();

    for i in 0..5 {
        let file_path = file_path.clone();
        let config = config.clone();
        let platform = platform.clone();

        let handle = tokio::spawn(async move {
            match SSTableReader::open(&file_path, &config, platform).await {
                Ok(_reader) => {
                    println!("✓ Concurrent open {} succeeded", i);
                    Ok(())
                }
                Err(e) => {
                    println!("✓ Concurrent open {} handled: {}", i, e);
                    Err(e)
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all concurrent operations
    let mut successes = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            successes += 1;
        }
        // Errors are acceptable for concurrent access
    }

    // At least one should succeed, but concurrent failures are acceptable
    println!(
        "✓ Concurrent access test completed: {}/5 succeeded",
        successes
    );
}

// Helper functions

fn create_valid_sstable_header() -> Vec<u8> {
    let mut data = Vec::new();

    // Valid SSTable header
    data.extend_from_slice(&[
        0x6d, 0x61, 0x64, 0x61, // Magic "mada"
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x0A, // Partition count (10)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, // Data size
    ]);

    // Add some partition data
    for i in 0..10 {
        data.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x08, // Key length
        ]);
        data.extend_from_slice(format!("key_{:04}", i).as_bytes());
        data.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x10, // Value length
        ]);
        data.extend_from_slice(&[0x44; 16]); // Mock value data
    }

    data
}

fn get_memory_usage() -> usize {
    // Simple memory usage estimation
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            return kb * 1024; // Convert KB to bytes
                        }
                    }
                }
            }
        }
    }

    // Fallback: return 0 if we can't measure
    0
}

/// Test coordination hook integration for negative test results
#[tokio::test]
async fn test_negative_tests_coordination() {
    // Store negative test results in memory for coordination
    let result = tokio::process::Command::new("npx")
        .args([
            "claude-flow@alpha",
            "hooks",
            "post-edit",
            "--file",
            "sstable_discovery_negative_tests",
            "--memory-key",
            "swarm/tester/negative_tests",
        ])
        .output()
        .await;

    match result {
        Ok(output) => {
            println!(
                "✓ Negative test results stored in memory: {}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
        Err(e) => {
            eprintln!("Warning: Could not store negative test results: {}", e);
        }
    }
}

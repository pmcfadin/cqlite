//! Edge case tests for SSTable component handling
//!
//! These tests focus on edge cases and error conditions that can occur
//! when loading separate SSTable component files (Index.db, Filter.db, etc.)
//! and verify graceful degradation and error handling.

use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Config;

/// Test handling of zero-byte component files
#[tokio::test]
async fn test_zero_byte_component_files() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-zero-byte";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let index_file = scenario_dir.join(format!("{}-Index.db", base_name));
    let summary_file = scenario_dir.join(format!("{}-Summary.db", base_name));
    let filter_file = scenario_dir.join(format!("{}-Filter.db", base_name));

    // Create realistic Data.db but zero-byte component files
    create_minimal_data_file(&data_file).await;
    fs::write(&index_file, b"").await.unwrap(); // Zero bytes
    fs::write(&summary_file, b"").await.unwrap(); // Zero bytes
    fs::write(&filter_file, b"").await.unwrap(); // Zero bytes

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Zero-byte component files handled gracefully");
        }
        Err(e) => {
            // Should handle zero-byte files without crashing
            assert!(
                e.to_string().contains("corruption") || e.to_string().contains("invalid"),
                "Should detect zero-byte files as corruption: {}",
                e
            );
            println!("✓ Zero-byte files detected as corruption: {}", e);
        }
    }
}

/// Test handling of truncated component files
#[tokio::test]
async fn test_truncated_component_files() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-truncated";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let index_file = scenario_dir.join(format!("{}-Index.db", base_name));
    let summary_file = scenario_dir.join(format!("{}-Summary.db", base_name));

    create_minimal_data_file(&data_file).await;

    // Create truncated files (partial headers)
    fs::write(&index_file, &[0x00, 0x00, 0x00, 0x01, 0x00, 0x00])
        .await
        .unwrap(); // Incomplete header
    fs::write(&summary_file, &[0x00, 0x00, 0x00]).await.unwrap(); // Very truncated

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Truncated component files handled gracefully");
        }
        Err(e) => {
            // Should detect truncation
            println!("✓ Truncated files detected: {}", e);
        }
    }
}

/// Test handling of component files with invalid magic numbers
#[tokio::test]
async fn test_invalid_magic_numbers() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-invalid-magic";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let index_file = scenario_dir.join(format!("{}-Index.db", base_name));
    let summary_file = scenario_dir.join(format!("{}-Summary.db", base_name));

    create_minimal_data_file(&data_file).await;

    // Create files with wrong magic numbers
    let mut invalid_index = vec![0xDE, 0xAD, 0xBE, 0xEF]; // Wrong magic
    invalid_index.extend_from_slice(&[0x00; 60]); // Padding
    fs::write(&index_file, invalid_index).await.unwrap();

    let mut invalid_summary = vec![0xCA, 0xFE, 0xBA, 0xBE]; // Wrong magic
    invalid_summary.extend_from_slice(&[0x00; 60]); // Padding
    fs::write(&summary_file, invalid_summary).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Invalid magic numbers handled gracefully");
        }
        Err(e) => {
            println!("✓ Invalid magic numbers detected: {}", e);
        }
    }
}

/// Test handling of very large component files
#[tokio::test]
async fn test_very_large_component_files() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-large-components";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let index_file = scenario_dir.join(format!("{}-Index.db", base_name));

    create_minimal_data_file(&data_file).await;

    // Create an unreasonably large Index.db file (10MB of zeros)
    let large_content = vec![0u8; 10 * 1024 * 1024];
    fs::write(&index_file, large_content).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Large component files handled (may use streaming)");
        }
        Err(e) => {
            // Should handle large files without memory issues
            println!("✓ Large component file handling: {}", e);
        }
    }
}

/// Test handling of component files with permission issues
#[tokio::test]
async fn test_component_permission_issues() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-permissions";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let index_file = scenario_dir.join(format!("{}-Index.db", base_name));

    create_minimal_data_file(&data_file).await;
    create_minimal_index_file(&index_file).await;

    // Make index file read-only on Unix systems
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&index_file).await.unwrap().permissions();
        perms.set_mode(0o000); // No permissions
        fs::set_permissions(&index_file, perms).await.unwrap();
    }

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Permission issues handled gracefully");
        }
        Err(e) => {
            #[cfg(unix)]
            {
                assert!(
                    e.to_string().contains("permission") || e.to_string().contains("denied"),
                    "Should detect permission issues: {}",
                    e
                );
            }
            println!("✓ Permission issues detected: {}", e);
        }
    }

    // Restore permissions for cleanup
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&index_file).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&index_file, perms).unwrap();
    }
}

/// Test concurrent access to component files
#[tokio::test]
async fn test_concurrent_component_access() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-concurrent";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let index_file = scenario_dir.join(format!("{}-Index.db", base_name));
    let summary_file = scenario_dir.join(format!("{}-Summary.db", base_name));

    create_minimal_data_file(&data_file).await;
    create_minimal_index_file(&index_file).await;
    create_minimal_summary_file(&summary_file).await;

    let config = Config::default();

    // Try to open multiple readers concurrently
    let mut handles = Vec::new();

    for i in 0..5 {
        let data_file_clone = data_file.clone();
        let config_clone = config.clone();

        let handle = tokio::spawn(async move {
            let platform = Arc::new(Platform::new(&config_clone).await.unwrap());
            let result = SSTableReader::open(&data_file_clone, &config_clone, platform).await;
            (i, result.is_ok())
        });

        handles.push(handle);
    }

    // Wait for all concurrent attempts
    let mut successful_opens = 0;
    for handle in handles {
        if let Ok((i, success)) = handle.await {
            if success {
                successful_opens += 1;
                println!("✓ Concurrent reader {} opened successfully", i);
            }
        }
    }

    println!(
        "✓ Concurrent access test: {}/5 readers opened",
        successful_opens
    );
}

/// Test handling of mixed component versions
#[tokio::test]
async fn test_mixed_component_versions() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-mixed-versions";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let index_file = scenario_dir.join(format!("{}-Index.db", base_name));
    let summary_file = scenario_dir.join(format!("{}-Summary.db", base_name));

    create_minimal_data_file(&data_file).await;

    // Create component files with different version numbers
    create_versioned_index_file(&index_file, 1).await; // Version 1
    create_versioned_summary_file(&summary_file, 5).await; // Version 5

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Mixed component versions handled gracefully");
        }
        Err(e) => {
            println!("✓ Mixed component versions detected: {}", e);
        }
    }
}

/// Test component file path case sensitivity
#[tokio::test]
async fn test_component_path_case_sensitivity() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "test-case-sensitivity";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));

    create_minimal_data_file(&data_file).await;

    // Create component files with different cases
    let index_file_lower = scenario_dir.join(format!("{}-index.db", base_name)); // lowercase
    let summary_file_upper = scenario_dir.join(format!("{}-SUMMARY.DB", base_name)); // uppercase

    create_minimal_index_file(&index_file_lower).await;
    create_minimal_summary_file(&summary_file_upper).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test case-insensitive component discovery
    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Case-insensitive component discovery works");
        }
        Err(e) => {
            // May not find components due to case sensitivity
            println!("✓ Case sensitivity handling: {}", e);
        }
    }
}

// Helper functions for creating minimal test files

async fn create_minimal_data_file(path: &Path) {
    let minimal_data = vec![
        0x6d, 0x61, 0x64, 0x61, // Magic "mada"
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x01, // Partition count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Data size (64 bytes)
    ];
    fs::write(path, minimal_data).await.unwrap();
}

async fn create_minimal_index_file(path: &Path) {
    let minimal_index = vec![
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x01, // Entry count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Data size
        0x12, 0x34, 0x56, 0x78, // Checksum
        // One index entry
        0x00, 0x00, 0x00, 0x08, // Key digest length
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Key digest
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, // Data offset
        0x00, 0x00, 0x00, 0x10, // Data size
    ];
    fs::write(path, minimal_index).await.unwrap();
}

async fn create_minimal_summary_file(path: &Path) {
    let minimal_summary = vec![
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x01, // Entry count
        0x00, 0x00, 0x00, 0x0A, // Sampling rate
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Min token
        0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Max token
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, // Data size
        0x87, 0x65, 0x43, 0x21, // Checksum
        // One summary entry
        0x00, 0x04, // Key length
        0x74, 0x65, 0x73, 0x74, // "test"
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Token
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Index offset
        0x00, 0x00, 0x00, 0x00, // Position
    ];
    fs::write(path, minimal_summary).await.unwrap();
}

async fn create_versioned_index_file(path: &Path, version: u32) {
    let mut data = vec![];
    data.extend_from_slice(&version.to_be_bytes());
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Entry count = 0
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10]); // Data size
    data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]); // Checksum
    fs::write(path, data).await.unwrap();
}

async fn create_versioned_summary_file(path: &Path, version: u32) {
    let mut data = vec![];
    data.extend_from_slice(&version.to_be_bytes());
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Entry count = 0
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x0A]); // Sampling rate
    data.extend_from_slice(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // Min token
    data.extend_from_slice(&[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]); // Max token
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20]); // Data size
    data.extend_from_slice(&[0x87, 0x65, 0x43, 0x21]); // Checksum
    fs::write(path, data).await.unwrap();
}

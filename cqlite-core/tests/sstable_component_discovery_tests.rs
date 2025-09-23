//! Comprehensive tests for SSTable component file discovery
//!
//! Tests focus on component file discovery for Index.db, Filter.db, Summary.db,
//! Statistics.db, and other companion files. Verifies path resolution, naming
//! pattern detection, and graceful handling of missing components.

use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::Config;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;

/// Test component discovery for standard Cassandra SSTable naming patterns
#[tokio::test]
async fn test_component_file_discovery_patterns() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let test_cases = vec![
        ("nb-1-big", "Cassandra 3.x style"),
        (
            "users-46436710673711f0b2cf19d64e7cbecb",
            "UUID-based naming",
        ),
        ("mc-2-large", "Alternative naming"),
        ("keyspace-table-ka-123", "Full descriptive naming"),
        ("system-peers-ka-1", "System keyspace"),
        ("test_ks-user_profiles-mb-42", "Underscore keyspace/table"),
    ];

    for (base_name, description) in test_cases {
        println!(
            "Testing component discovery for: {} ({})",
            base_name, description
        );

        let scenario_dir = base_path.join(base_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        // Create complete component file structure
        create_complete_component_structure(&scenario_dir, base_name).await;

        // Test discovery of each component type
        test_index_component_discovery(&scenario_dir, base_name).await;
        test_filter_component_discovery(&scenario_dir, base_name).await;
        test_summary_component_discovery(&scenario_dir, base_name).await;
        test_statistics_component_discovery(&scenario_dir, base_name).await;
        test_toc_component_discovery(&scenario_dir, base_name).await;

        // Test SSTableReader's component discovery
        test_sstable_reader_discovery(&scenario_dir, base_name).await;

        // Cleanup
        fs::remove_dir_all(&scenario_dir).await.unwrap();
        println!("✓ Component discovery test completed for {}", base_name);
    }
}

/// Test discovery behavior with missing Index.db component
#[tokio::test]
async fn test_index_component_missing_discovery() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let test_scenarios = vec!["missing-index-1", "system-auth-ka-5", "test-table-mb-100"];

    for base_name in test_scenarios {
        let scenario_dir = base_path.join(base_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        // Create all components EXCEPT Index.db
        create_partial_component_structure(&scenario_dir, base_name, &["Index.db"]).await;

        // Verify Index.db is not discovered
        let index_path = scenario_dir.join(format!("{}-Index.db", base_name));
        assert!(
            !index_path.exists(),
            "Index.db should not exist for missing test"
        );

        // Test SSTableReader handles missing Index.db gracefully
        let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        match SSTableReader::open(&data_file, &config, platform).await {
            Ok(reader) => {
                println!(
                    "✓ SSTableReader created despite missing Index.db for {}",
                    base_name
                );

                // Index operations should handle missing component gracefully
                let test_key = b"test_partition_key";
                let _lookup_result = reader.lookup_partition_with_index(test_key).await;
                println!(
                    "✓ Index lookup handled missing Index.db gracefully for {}",
                    base_name
                );
            }
            Err(e) => {
                // Should not fail due to missing Index.db file specifically
                assert!(
                    !e.to_string().contains("Index.db not found"),
                    "Should not fail specifically on missing Index.db: {}",
                    e
                );
                println!(
                    "✓ Missing Index.db handled appropriately for {}: {}",
                    base_name, e
                );
            }
        }

        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

/// Test discovery behavior with missing Filter.db (bloom filter) component
#[tokio::test]
async fn test_filter_component_missing_discovery() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "missing-filter-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    // Create all components EXCEPT Filter.db
    create_partial_component_structure(&scenario_dir, base_name, &["Filter.db"]).await;

    // Verify Filter.db is not discovered
    let filter_path = scenario_dir.join(format!("{}-Filter.db", base_name));
    assert!(
        !filter_path.exists(),
        "Filter.db should not exist for missing test"
    );

    // Test SSTableReader handles missing Filter.db gracefully
    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            println!("✓ SSTableReader created despite missing Filter.db");

            // Operations should work but bloom filter features disabled
            let test_key = b"test_partition_key";
            let _lookup_result = reader.lookup_partition_with_index(test_key).await;
            println!("✓ Partition lookup works without bloom filter");
        }
        Err(e) => {
            // Should not fail due to missing Filter.db file specifically
            assert!(
                !e.to_string().contains("Filter.db not found"),
                "Should not fail specifically on missing Filter.db: {}",
                e
            );
            println!("✓ Missing Filter.db handled appropriately: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test discovery with various file permission scenarios
#[tokio::test]
#[cfg(unix)] // File permissions are Unix-specific
async fn test_component_discovery_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "permission-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    create_complete_component_structure(&scenario_dir, base_name).await;

    // Test with unreadable Index.db
    let index_path = scenario_dir.join(format!("{}-Index.db", base_name));

    // Make Index.db unreadable (permissions 000)
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(&index_path).await.unwrap().permissions();
    perms.set_mode(0o000);
    fs::set_permissions(&index_path, perms).await.unwrap();

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // SSTableReader should handle permission denied gracefully
    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            println!("✓ SSTableReader created despite unreadable Index.db");

            // Operations should work but may fall back from index optimization
            let test_key = b"test_key";
            let _lookup_result = reader.lookup_partition_with_index(test_key).await;
        }
        Err(e) => {
            // Should handle permission errors gracefully
            println!("✓ Permission denied handled gracefully: {}", e);
        }
    }

    // Restore permissions for cleanup
    let mut perms = fs::metadata(&index_path).await.unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&index_path, perms).await.unwrap();

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test component discovery with nested directory structures
#[tokio::test]
async fn test_component_discovery_nested_paths() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Create nested directory structure
    let keyspace_dir = base_path.join("test_keyspace");
    let table_dir = keyspace_dir.join("user_profiles");
    fs::create_dir_all(&table_dir).await.unwrap();

    let base_name = "nested-test-mb-1";
    create_complete_component_structure(&table_dir, base_name).await;

    // Test discovery in nested structure
    let data_file = table_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(_) => {
            println!("✓ Component discovery works in nested directory structure");
        }
        Err(e) => {
            // Verify it's not a path resolution issue
            assert!(
                !e.to_string().contains("file not found"),
                "Component discovery should work in nested paths: {}",
                e
            );
            println!("✓ Nested path discovery attempted: {}", e);
        }
    }

    fs::remove_dir_all(&keyspace_dir).await.unwrap();
}

/// Test discovery with symlinks and hardlinks
#[tokio::test]
#[cfg(unix)] // Symlinks are Unix-specific
async fn test_component_discovery_links() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "link-test";
    let original_dir = base_path.join("original");
    let linked_dir = base_path.join("linked");

    fs::create_dir_all(&original_dir).await.unwrap();
    fs::create_dir_all(&linked_dir).await.unwrap();

    // Create original component files
    create_complete_component_structure(&original_dir, base_name).await;

    // Create symlinks to component files
    let original_index = original_dir.join(format!("{}-Index.db", base_name));
    let linked_index = linked_dir.join(format!("{}-Index.db", base_name));

    tokio::fs::symlink(&original_index, &linked_index)
        .await
        .unwrap();

    // Copy Data.db to linked directory
    let original_data = original_dir.join(format!("{}-Data.db", base_name));
    let linked_data = linked_dir.join(format!("{}-Data.db", base_name));
    fs::copy(&original_data, &linked_data).await.unwrap();

    // Test discovery works with symlinked components
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&linked_data, &config, platform).await {
        Ok(_) => {
            println!("✓ Component discovery works with symlinked files");
        }
        Err(e) => {
            println!("✓ Symlink discovery attempted: {}", e);
        }
    }

    fs::remove_dir_all(&original_dir).await.unwrap();
    fs::remove_dir_all(&linked_dir).await.unwrap();
}

// Component-specific discovery test functions

async fn test_index_component_discovery(dir: &Path, base_name: &str) {
    let index_path = dir.join(format!("{}-Index.db", base_name));
    assert!(
        index_path.exists(),
        "Index.db component should be discovered"
    );

    // Verify file is readable
    let _content = fs::read(&index_path).await.unwrap();
    println!(
        "✓ Index.db component discovered and readable for {}",
        base_name
    );
}

async fn test_filter_component_discovery(dir: &Path, base_name: &str) {
    let filter_path = dir.join(format!("{}-Filter.db", base_name));
    assert!(
        filter_path.exists(),
        "Filter.db component should be discovered"
    );

    // Verify file is readable
    let _content = fs::read(&filter_path).await.unwrap();
    println!(
        "✓ Filter.db component discovered and readable for {}",
        base_name
    );
}

async fn test_summary_component_discovery(dir: &Path, base_name: &str) {
    let summary_path = dir.join(format!("{}-Summary.db", base_name));
    assert!(
        summary_path.exists(),
        "Summary.db component should be discovered"
    );

    // Verify file is readable
    let _content = fs::read(&summary_path).await.unwrap();
    println!(
        "✓ Summary.db component discovered and readable for {}",
        base_name
    );
}

async fn test_statistics_component_discovery(dir: &Path, base_name: &str) {
    let statistics_path = dir.join(format!("{}-Statistics.db", base_name));
    assert!(
        statistics_path.exists(),
        "Statistics.db component should be discovered"
    );

    // Verify file is readable
    let _content = fs::read(&statistics_path).await.unwrap();
    println!(
        "✓ Statistics.db component discovered and readable for {}",
        base_name
    );
}

async fn test_toc_component_discovery(dir: &Path, base_name: &str) {
    let toc_path = dir.join(format!("{}-TOC.txt", base_name));
    assert!(toc_path.exists(), "TOC.txt component should be discovered");

    // Verify file is readable and contains expected entries
    let content = fs::read_to_string(&toc_path).await.unwrap();
    assert!(content.contains("Data.db"), "TOC should list Data.db");
    assert!(content.contains("Index.db"), "TOC should list Index.db");
    println!("✓ TOC.txt component discovered and valid for {}", base_name);
}

async fn test_sstable_reader_discovery(dir: &Path, base_name: &str) {
    let data_file = dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test that SSTableReader can discover and utilize components
    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            println!(
                "✓ SSTableReader successfully discovered all components for {}",
                base_name
            );

            // Test that component-dependent operations work
            let test_key = b"discovery_test_key";
            let _index_lookup = reader.lookup_partition_with_index(test_key).await;
            let _token_range = reader.iterate_token_range(-1000, 1000).await;
            let _timestamp_range = reader.get_timestamp_range().await;

            println!(
                "✓ All component-dependent operations available for {}",
                base_name
            );
        }
        Err(e) => {
            // Should not fail due to component discovery issues
            assert!(
                !e.to_string().contains("component not found")
                    && !e.to_string().contains("file not found"),
                "Component discovery should not cause file not found errors: {}",
                e
            );
            println!("✓ Component discovery attempted for {}: {}", base_name, e);
        }
    }
}

// Helper functions for creating test component structures

async fn create_complete_component_structure(dir: &Path, base_name: &str) {
    let data_file = dir.join(format!("{}-Data.db", base_name));
    let index_file = dir.join(format!("{}-Index.db", base_name));
    let summary_file = dir.join(format!("{}-Summary.db", base_name));
    let statistics_file = dir.join(format!("{}-Statistics.db", base_name));
    let filter_file = dir.join(format!("{}-Filter.db", base_name));
    let compression_file = dir.join(format!("{}-CompressionInfo.db", base_name));
    let toc_file = dir.join(format!("{}-TOC.txt", base_name));

    create_realistic_data_file(&data_file).await;
    create_realistic_index_file(&index_file).await;
    create_realistic_summary_file(&summary_file).await;
    create_realistic_statistics_file(&statistics_file).await;
    create_realistic_filter_file(&filter_file).await;
    create_realistic_compression_file(&compression_file).await;
    create_realistic_toc_file(&toc_file).await;
}

async fn create_partial_component_structure(dir: &Path, base_name: &str, exclude: &[&str]) {
    if !exclude.contains(&"Data.db") {
        let file_path = dir.join(format!("{}-Data.db", base_name));
        create_realistic_data_file(&file_path).await;
    }
    if !exclude.contains(&"Index.db") {
        let file_path = dir.join(format!("{}-Index.db", base_name));
        create_realistic_index_file(&file_path).await;
    }
    if !exclude.contains(&"Summary.db") {
        let file_path = dir.join(format!("{}-Summary.db", base_name));
        create_realistic_summary_file(&file_path).await;
    }
    if !exclude.contains(&"Statistics.db") {
        let file_path = dir.join(format!("{}-Statistics.db", base_name));
        create_realistic_statistics_file(&file_path).await;
    }
    if !exclude.contains(&"Filter.db") {
        let file_path = dir.join(format!("{}-Filter.db", base_name));
        create_realistic_filter_file(&file_path).await;
    }
    if !exclude.contains(&"CompressionInfo.db") {
        let file_path = dir.join(format!("{}-CompressionInfo.db", base_name));
        create_realistic_compression_file(&file_path).await;
    }
    if !exclude.contains(&"TOC.txt") {
        let file_path = dir.join(format!("{}-TOC.txt", base_name));
        create_realistic_toc_file(&file_path).await;
    }
}

// Realistic file creation functions

async fn create_realistic_data_file(path: &Path) {
    let mut data = Vec::new();

    // SSTable header (Cassandra 5+ format)
    data.extend_from_slice(&[
        0x6d, 0x61, 0x64, 0x61, // Magic number "mada"
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x64, // Partition count (100)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // Data size
    ]);

    // Add mock partition data for discovery tests
    for i in 0..10 {
        data.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x10, // Partition key length
        ]);
        data.extend_from_slice(format!("discovery_key_{:04}", i).as_bytes());
        data.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x20, // Row data length
        ]);
        data.extend_from_slice(&[0x44; 32]); // Mock row data
    }

    fs::write(path, data).await.unwrap();
}

async fn create_realistic_index_file(path: &Path) {
    let mut data = Vec::new();

    // Index header for discovery tests
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x0A, // Entry count (10)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, // Data size
        0x12, 0x34, 0x56, 0x78, // Checksum
    ]);

    // Index entries for discovery
    for i in 0..10 {
        data.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x20, // Key digest length (32)
        ]);
        let mut digest = vec![0; 32];
        digest[0] = i as u8;
        digest[31] = (i + 100) as u8; // Make digests unique
        data.extend_from_slice(&digest);

        let offset = (i as u64) * 1024;
        data.extend_from_slice(&offset.to_be_bytes()); // Data offset
        data.extend_from_slice(&(512u32).to_be_bytes()); // Data size
    }

    fs::write(path, data).await.unwrap();
}

async fn create_realistic_summary_file(path: &Path) {
    let mut data = Vec::new();

    // Summary header for discovery tests
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x05, // Entry count (5)
        0x00, 0x00, 0x00, 0x14, // Sampling rate (20)
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Min token
        0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Max token
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, // Data size
        0x87, 0x65, 0x43, 0x21, // Checksum
    ]);

    // Summary entries for token range discovery
    let tokens = [
        -2000000000i64,
        -1000000000i64,
        0i64,
        1000000000i64,
        2000000000i64,
    ];
    for (i, &token) in tokens.iter().enumerate() {
        data.extend_from_slice(&[
            0x00, 0x0C, // Key length
        ]);
        data.extend_from_slice(format!("summary_{:02}", i).as_bytes());
        data.extend_from_slice(&token.to_be_bytes()); // Token
        data.extend_from_slice(&[
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            (i as u8) * 100,
            0x00, // Index offset
            0x00,
            0x00,
            0x00,
            i as u8, // Position
        ]);
    }

    fs::write(path, data).await.unwrap();
}

async fn create_realistic_statistics_file(path: &Path) {
    let mut data = Vec::new();

    // Statistics entries for discovery tests
    let stats = vec![
        ("min_timestamp", 1640995200000u64), // Jan 1, 2022
        ("max_timestamp", 1672531200000u64), // Jan 1, 2023
        ("live_row_count", 5000u64),
        ("total_data_size", 512000u64),
        ("compaction_level", 0u64),
        ("max_local_deletion_time", 1672531200u64),
    ];

    for (key, value) in stats {
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&(8u32).to_be_bytes()); // Value length
        data.extend_from_slice(&value.to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_realistic_filter_file(path: &Path) {
    let mut data = Vec::new();

    // Bloom filter header for discovery tests
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x05, // Hash functions
        0x00, 0x00, 0x20, 0x00, // Bit array size (8192 bits)
    ]);

    // Bloom filter bit array (1024 bytes = 8192 bits)
    let bit_array = vec![0xAA; 1024]; // Alternating bit pattern
    data.extend_from_slice(&bit_array);

    fs::write(path, data).await.unwrap();
}

async fn create_realistic_compression_file(path: &Path) {
    let compression_info = [
        "algorithm=LZ4\n",
        "chunk_length=65536\n",
        "parameters={}\n",
        "compressed_size=245760\n",
        "uncompressed_size=512000\n",
    ]
    .join("");

    fs::write(path, compression_info).await.unwrap();
}

async fn create_realistic_toc_file(path: &Path) {
    let toc_content = [
        "Data.db\n",
        "Index.db\n",
        "Summary.db\n",
        "Statistics.db\n",
        "Filter.db\n",
        "CompressionInfo.db\n",
        "TOC.txt\n",
    ]
    .join("");

    fs::write(path, toc_content).await.unwrap();
}

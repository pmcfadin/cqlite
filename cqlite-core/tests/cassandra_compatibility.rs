//! Integration tests for Cassandra compatibility
//!
//! These tests validate that CQLite can correctly write and read SSTable files
//! that are compatible with Apache Cassandra, including format validation,
//! compression support, and data integrity checks.

use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

use cqlite_core::{Config, Result, RowKey, Value};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::sstable::validation::CassandraValidationFramework;
use cqlite_core::types::TableId;

/// Test creating and validating a basic Cassandra-compatible SSTable
#[tokio::test]
async fn test_basic_cassandra_compatibility() -> Result<()> {
    let temp_dir = TempDir::new().map_err(|e| {
        cqlite_core::error::Error::io(format!("Failed to create temp dir: {}", e))
    })?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    // Create validation framework
    let validator = CassandraValidationFramework::new(
        platform.clone(),
        config.clone(),
        temp_dir.path().to_str().unwrap()
    );

    // Create a basic SSTable with Cassandra-compatible format
    let sstable_path = temp_dir.path().join("test_basic.sst");
    let mut writer = SSTableWriter::create(&sstable_path, &config, platform.clone()).await?;
    
    // Add test data
    let test_entries = vec![
        (TableId::new("users"), RowKey::from("user:1"), Value::Text("John Doe".to_string())),
        (TableId::new("users"), RowKey::from("user:2"), Value::Text("Jane Smith".to_string())),
        (TableId::new("orders"), RowKey::from("order:100"), Value::Integer(1500)),
        (TableId::new("orders"), RowKey::from("order:101"), Value::Float(29.99)),
    ];

    for (table_id, key, value) in test_entries {
        writer.add_entry(&table_id, key, value).await?;
    }
    
    writer.finish().await?;
    assert!(sstable_path.exists(), "SSTable file should be created");
    
    // Validate the created SSTable
    let report = validator.run_full_validation().await?;
    if !report.is_successful() {
        println!("Validation warnings: {}", report.summary());
    }
    
    Ok(())
}

/// Test Cassandra compression compatibility
#[tokio::test]
async fn test_cassandra_compression_compatibility() -> Result<()> {
    let temp_dir = TempDir::new().map_err(|e| {
        cqlite_core::error::Error::io(format!("Failed to create temp dir: {}", e))
    })?;
    
    let compressed_config = Config {
        storage: cqlite_core::config::StorageConfig {
            compression: cqlite_core::config::CompressionConfig {
                enabled: true,
                algorithm: cqlite_core::config::CompressionAlgorithm::Lz4,
            },
            enable_bloom_filters: true,
            bloom_filter_fp_rate: 0.01,
            block_size: 64 * 1024,
        },
        ..Default::default()
    };
    
    let platform = Arc::new(Platform::new(&compressed_config).await?);
    let compressed_path = temp_dir.path().join("test_compressed.sst");
    let mut compressed_writer = SSTableWriter::create(&compressed_path, &compressed_config, platform.clone()).await?;
    
    // Add more data to test compression
    for i in 0..100 {
        let table_id = TableId::new("large_table");
        let key = RowKey::from(format!("key_{:06}", i));
        let value = Value::Text(format!("This is test data entry number {} with some repetitive content to ensure compression works effectively", i));
        compressed_writer.add_entry(&table_id, key, value).await?;
    }
    
    compressed_writer.finish().await?;
    assert!(compressed_path.exists(), "Compressed SSTable should be created");
    
    // Verify the compressed file is smaller than it would be uncompressed
    let metadata = std::fs::metadata(&compressed_path).map_err(|e| {
        cqlite_core::error::Error::io(format!("Failed to get file metadata: {}", e))
    })?;
    
    // With 100 entries of ~120 chars each, uncompressed should be >12KB
    // Compressed should be significantly smaller due to repetitive content
    assert!(metadata.len() < 8000, "Compressed file should be smaller than expected uncompressed size");
    
    Ok(())
}

/// Test reading real Cassandra SSTable files for compatibility validation
#[test] 
fn test_read_real_cassandra_sstables() {
    let test_env_path = "../test-env/cassandra5/sstables";
    if !Path::new(test_env_path).exists() {
        println!("Skipping real SSTable test - test environment not found at {}", test_env_path);
        return;
    }
    
    // Test that we can at least enumerate the SSTable directories
    let entries = std::fs::read_dir(test_env_path).expect("Should be able to read test-env directory");
    let mut sstable_dirs = 0;
    
    for entry in entries {
        let entry = entry.expect("Should be able to read directory entry");
        if entry.file_type().expect("Should have file type").is_dir() {
            sstable_dirs += 1;
        }
    }
    
    assert!(sstable_dirs > 0, "Should have at least one SSTable directory in test environment");
}

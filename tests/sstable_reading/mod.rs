// Integration tests for SSTable reading functionality - Issue #25

pub mod basic_reading_tests;
pub mod format_detection_tests;
pub mod compression_tests;
pub mod error_handling_tests;
pub mod performance_tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use cqlite_core::{Config, Error, Result};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::platform::Platform;

/// Test harness for SSTable reading validation
pub struct SSTableTestHarness {
    temp_dir: TempDir,
    config: Config,
    platform: Arc<Platform>,
}

impl SSTableTestHarness {
    pub async fn new() -> Result<Self> {
        let temp_dir = TempDir::new().map_err(|e| Error::Io(std::io::Error::other(format!("Failed to create temp dir: {}", e))))?;
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        
        Ok(Self {
            temp_dir,
            config,
            platform,
        })
    }
    
    pub fn temp_path(&self) -> &Path {
        self.temp_dir.path()
    }
    
    pub async fn create_test_sstable(&self, name: &str, data: TestSSTableData) -> Result<PathBuf> {
        let sstable_path = self.temp_path().join(format!("{}.db", name));
        // Create minimal valid SSTable file for testing
        create_test_sstable_file(&sstable_path, data).await?;
        Ok(sstable_path)
    }
    
    pub async fn open_reader(&self, path: &Path) -> Result<SSTableReader> {
        SSTableReader::open(path, &self.config, self.platform.clone()).await
    }
}

#[derive(Debug, Clone)]
pub struct TestSSTableData {
    pub keyspace: String,
    pub table: String,
    pub rows: Vec<TestRow>,
    pub compression: Option<String>,
    pub version: String,
}

#[derive(Debug, Clone)]
pub struct TestRow {
    pub key: Vec<u8>,
    pub columns: std::collections::HashMap<String, Vec<u8>>,
    pub timestamp: Option<i64>,
}

impl Default for TestSSTableData {
    fn default() -> Self {
        Self {
            keyspace: "test_keyspace".to_string(),
            table: "test_table".to_string(),
            rows: vec![
                TestRow {
                    key: b"test_key_1".to_vec(),
                    columns: {
                        let mut cols = std::collections::HashMap::new();
                        cols.insert("name".to_string(), b"Test User 1".to_vec());
                        cols.insert("email".to_string(), b"test1@example.com".to_vec());
                        cols
                    },
                    timestamp: Some(1640995200000), // 2022-01-01 00:00:00 UTC
                },
                TestRow {
                    key: b"test_key_2".to_vec(),
                    columns: {
                        let mut cols = std::collections::HashMap::new();
                        cols.insert("name".to_string(), b"Test User 2".to_vec());
                        cols.insert("email".to_string(), b"test2@example.com".to_vec());
                        cols
                    },
                    timestamp: Some(1640995260000), // 2022-01-01 00:01:00 UTC
                },
            ],
            compression: None,
            version: "5.0".to_string(),
        }
    }
}

async fn create_test_sstable_file(path: &Path, data: TestSSTableData) -> Result<()> {
    // For now, create a minimal file structure that the reader can recognize
    // This would be replaced with actual SSTable format generation
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    
    let mut file = File::create(path).await
        .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to create test file: {}", e))))?;
    
    // Write minimal SSTable header for testing
    // Magic number for SSTable format
    file.write_all(b"SSTable").await
        .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write header: {}", e))))?;
    
    // Version info
    file.write_all(data.version.as_bytes()).await
        .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write version: {}", e))))?;
    
    // Minimal data section
    for row in &data.rows {
        file.write_all(&row.key).await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write row key: {}", e))))?;
        file.write_all(b"\n").await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to write separator: {}", e))))?;
    }
    
    file.flush().await
        .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to flush file: {}", e))))?;
    
    Ok(())
}

/// Common test utilities
pub mod test_utils {
    use super::*;
    
    /// Assert that an SSTable file can be opened and read
    pub async fn assert_readable(harness: &SSTableTestHarness, path: &Path) -> Result<()> {
        let reader = harness.open_reader(path).await?;
        // Basic validation - if we can create a reader, the file is readable
        Ok(())
    }
    
    /// Assert that a file produces the expected error
    pub async fn assert_error<F>(harness: &SSTableTestHarness, path: &Path, error_check: F) -> Result<()> 
    where
        F: Fn(&Error) -> bool,
    {
        match harness.open_reader(path).await {
            Err(e) if error_check(&e) => Ok(()),
            Err(e) => Err(Error::Io(std::io::Error::other(format!("Wrong error type: {:?}", e)))),
            Ok(_) => Err(Error::Io(std::io::Error::other("Expected error but got success"))),
        }
    }
}
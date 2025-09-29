//! Comprehensive test utilities for loading real SSTable data
//!
//! This module provides reusable utilities for integration tests that work with
//! real Cassandra SSTable files from the test dataset directory.
//!
//! ## Basic Usage
//!
//! ```rust
//! use cqlite_core::tests::common::{TestContext, AssertionHelpers};
//!
//! #[tokio::test]
//! async fn test_sstable_preparation() {
//!     let mut context = TestContext::new("test_basic").await.unwrap();
//!     let sstable_path = context.prepare_sstable("simple_table").await.unwrap();
//!
//!     // Test partition lookup
//!     let partition_key = b"test_key";
//!     AssertionHelpers::verify_partition_lookup(&sstable_path, partition_key, true).await.unwrap();
//!
//!     // Verify metrics
//!     let metrics = context.cleanup().unwrap();
//!     assert!(!metrics.load_times.is_empty());
//! }
//! ```
//!
//! ## Performance Testing
//!
//! ```rust
//! use cqlite_core::tests::common::PerformanceTestUtils;
//!
//! #[tokio::test]
//! async fn test_concurrent_access() {
//!     let (result, duration) = PerformanceTestUtils::time_operation(|| async {
//!         // Your async operation here
//!         Ok::<(), Error>(())
//!     }).await;
//!
//!     println!("Operation took: {:?}", duration);
//! }
//! ```
//!
//! ## Dataset Discovery
//!
//! ```rust
//! use cqlite_core::tests::common::DatasetUtils;
//!
//! #[tokio::test]
//! async fn test_dataset_discovery() {
//!     let datasets = DatasetUtils::get_available_datasets().unwrap();
//!     let descriptor = DatasetUtils::create_dataset_descriptor("test_basic").await.unwrap();
//!
//!     println!("Available tables: {:?}", descriptor.tables);
//! }
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

use cqlite_core::{Error, Result};

/// Test context for managing SSTable test environments
#[derive(Debug)]
pub struct TestContext {
    /// Temporary directory for test files
    pub temp_dir: TempDir,
    /// Path to the test dataset directory
    pub dataset_path: PathBuf,
    /// Performance metrics collector
    pub metrics: TestMetrics,
}

/// Performance metrics for test operations
#[derive(Debug, Default, Clone)]
pub struct TestMetrics {
    /// SSTable load times
    pub load_times: Vec<Duration>,
    /// Cache hit rates
    pub cache_hits: u64,
    /// Cache miss counts
    pub cache_misses: u64,
    /// Total bytes read
    #[allow(dead_code)]
    pub bytes_read: u64,
}

/// SSTable dataset descriptor for different test scenarios
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DatasetDescriptor {
    /// Dataset name (e.g., "test_basic", "system")
    pub name: String,
    /// Available tables in this dataset
    pub tables: Vec<TableDescriptor>,
    /// Expected Cassandra version
    pub cassandra_version: String,
}

/// Table descriptor within a dataset
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TableDescriptor {
    /// Table name
    pub name: String,
    /// Table UUID directory name
    pub uuid_dir: String,
    /// Expected row count
    pub row_count: Option<u64>,
    /// Expected SSTable components
    pub expected_components: Vec<SSTableComponent>,
}

/// SSTable component types for validation
#[derive(Debug, Clone, PartialEq)]
pub enum SSTableComponent {
    Data,
    Index,
    Summary,
    Filter,
    CompressionInfo,
    Statistics,
    Toc,
    Digest,
}

impl TestContext {
    /// Create a new test context for a specific dataset
    pub async fn new(dataset_name: &str) -> Result<Self> {
        let temp_dir = TempDir::new()
            .map_err(|e| Error::InvalidState(format!("Failed to create temp dir: {}", e)))?;

        // Use environment-relative path calculation instead of hardcoded paths
        let dataset_path = if let Ok(datasets_root) = std::env::var("CQLITE_DATASETS_ROOT") {
            Path::new(&datasets_root)
                .join("sstables")
                .join(dataset_name)
        } else {
            // Default to relative path from cargo manifest
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .ok_or_else(|| Error::InvalidPath("Cannot find project root".to_string()))?
                .join("test-data")
                .join("datasets")
                .join("sstables")
                .join(dataset_name)
        };

        if !dataset_path.exists() {
            return Err(Error::InvalidPath(format!(
                "Dataset not found: {}",
                dataset_path.display()
            )));
        }

        Ok(TestContext {
            temp_dir,
            dataset_path,
            metrics: TestMetrics::default(),
        })
    }

    /// Prepare SSTable files for testing by copying them to temp directory
    pub async fn prepare_sstable(&mut self, table_name: &str) -> Result<PathBuf> {
        let start_time = Instant::now();

        // Find the table directory
        let table_dir = self.find_table_directory(table_name)?;

        // Copy SSTable files to temp directory for isolation
        let temp_table_dir = self.copy_sstable_files(&table_dir, table_name).await?;

        // Record performance metrics
        let load_time = start_time.elapsed();
        self.metrics.load_times.push(load_time);

        Ok(temp_table_dir)
    }

    /// Get all available tables in the current dataset
    pub fn get_available_tables(&self) -> Result<Vec<TableDescriptor>> {
        let mut tables = Vec::new();

        let entries = fs::read_dir(&self.dataset_path).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    let (table_name, _uuid) = self.parse_table_directory_name(file_name)?;
                    let components = self.discover_sstable_components(&path)?;

                    tables.push(TableDescriptor {
                        name: table_name,
                        uuid_dir: file_name.to_string(), // Use the full directory name
                        row_count: None,                 // Could be parsed from metadata
                        expected_components: components,
                    });
                }
            }
        }

        Ok(tables)
    }

    /// Copy SSTable files to temporary directory for test isolation
    async fn copy_sstable_files(&self, source_dir: &Path, table_name: &str) -> Result<PathBuf> {
        let dest_dir = self.temp_dir.path().join(table_name);
        fs::create_dir_all(&dest_dir).map_err(Error::Io)?;

        // Copy all SSTable component files
        let entries = fs::read_dir(source_dir).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let source_path = entry.path();

            if source_path.is_file() {
                let file_name = source_path
                    .file_name()
                    .ok_or_else(|| Error::InvalidPath("Invalid file name".to_string()))?;
                let dest_path = dest_dir.join(file_name);

                fs::copy(&source_path, &dest_path).map_err(Error::Io)?;
            }
        }

        Ok(dest_dir)
    }

    /// Find the table directory by name
    fn find_table_directory(&self, table_name: &str) -> Result<PathBuf> {
        let entries = fs::read_dir(&self.dataset_path).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if dir_name.starts_with(&format!("{}-", table_name)) {
                        return Ok(path);
                    }
                }
            }
        }

        Err(Error::InvalidPath(format!(
            "Table not found: {}",
            table_name
        )))
    }

    /// Parse table directory name to extract table name and UUID
    fn parse_table_directory_name(&self, dir_name: &str) -> Result<(String, String)> {
        if let Some(dash_pos) = dir_name.rfind('-') {
            let table_name = dir_name[..dash_pos].to_string();
            let uuid = dir_name[dash_pos + 1..].to_string();
            Ok((table_name, uuid))
        } else {
            Err(Error::InvalidPath(format!(
                "Invalid table directory name: {}",
                dir_name
            )))
        }
    }

    /// Discover SSTable components in a directory
    fn discover_sstable_components(&self, table_dir: &Path) -> Result<Vec<SSTableComponent>> {
        let mut components = Vec::new();

        let entries = fs::read_dir(table_dir).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();

            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.contains("-Data.db") {
                    components.push(SSTableComponent::Data);
                } else if file_name.contains("-Index.db") {
                    components.push(SSTableComponent::Index);
                } else if file_name.contains("-Summary.db") {
                    components.push(SSTableComponent::Summary);
                } else if file_name.contains("-Filter.db") {
                    components.push(SSTableComponent::Filter);
                } else if file_name.contains("-CompressionInfo.db") {
                    components.push(SSTableComponent::CompressionInfo);
                } else if file_name.contains("-Statistics.db") {
                    components.push(SSTableComponent::Statistics);
                } else if file_name.contains("-TOC.txt") {
                    components.push(SSTableComponent::Toc);
                } else if file_name.contains("-Digest.crc32") {
                    components.push(SSTableComponent::Digest);
                }
            }
        }

        Ok(components)
    }

    /// Record bytes read for metrics
    #[allow(dead_code)]
    pub fn record_bytes_read(&mut self, bytes: u64) {
        self.metrics.bytes_read += bytes;
    }

    /// Clean up test context and return metrics
    #[allow(dead_code)]
    pub fn cleanup(self) -> Result<TestMetrics> {
        // TempDir automatically cleans up when dropped
        Ok(self.metrics)
    }
}

/// Utilities for performance testing
#[allow(dead_code)]
pub struct PerformanceTestUtils;

impl PerformanceTestUtils {
    /// Time an async operation and return the result with duration
    #[allow(dead_code)]
    pub async fn time_operation<F, Fut, T>(operation: F) -> (T, Duration)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = Instant::now();
        let result = operation().await;
        let duration = start.elapsed();
        (result, duration)
    }

    /// Run concurrent access test with multiple readers
    #[allow(dead_code)]
    pub async fn concurrent_access_test<F, Fut>(
        operation_factory: F,
        num_concurrent: usize,
    ) -> Vec<Duration>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        use std::sync::Arc;
        let operation_factory = Arc::new(operation_factory);
        let mut handles = Vec::new();

        for _ in 0..num_concurrent {
            let factory = operation_factory.clone();
            let handle = tokio::spawn(async move {
                let start = Instant::now();
                let _ = factory().await;
                start.elapsed()
            });
            handles.push(handle);
        }

        let mut durations = Vec::new();
        for handle in handles {
            if let Ok(duration) = handle.await {
                durations.push(duration);
            }
        }

        durations
    }
}

/// Common assertion helpers for SSTable testing
pub struct AssertionHelpers;

impl AssertionHelpers {
    /// Discover actual components in a directory (public version)
    #[allow(dead_code)]
    pub fn discover_components(table_dir: &Path) -> Result<Vec<SSTableComponent>> {
        Self::discover_components_internal(table_dir)
    }

    /// Validate SSTable component file offsets
    #[allow(dead_code)]
    pub fn validate_offsets(
        data_file_size: u64,
        index_offsets: &[(u64, u64)], // (start, end) pairs
        component_name: &str,
    ) -> Result<()> {
        for (i, (start, end)) in index_offsets.iter().enumerate() {
            if start >= end {
                return Err(Error::InvalidFormat(format!(
                    "Invalid offset range in {}: entry {} has start >= end ({} >= {})",
                    component_name, i, start, end
                )));
            }

            if *end > data_file_size {
                return Err(Error::InvalidFormat(format!(
                    "Offset out of bounds in {}: entry {} end offset {} exceeds file size {}",
                    component_name, i, end, data_file_size
                )));
            }
        }

        Ok(())
    }

    /// Verify SSTable component integrity
    #[allow(dead_code)]
    pub async fn verify_component_integrity(
        table_dir: &Path,
        expected_components: &[SSTableComponent],
    ) -> Result<()> {
        let discovered_components = Self::discover_components_internal(table_dir)?;

        for expected in expected_components {
            if !discovered_components.contains(expected) {
                return Err(Error::InvalidFormat(format!(
                    "Missing expected component: {:?}",
                    expected
                )));
            }
        }

        Ok(())
    }

    /// Discover actual components in a directory (internal)
    fn discover_components_internal(table_dir: &Path) -> Result<Vec<SSTableComponent>> {
        let mut components = Vec::new();

        let entries = fs::read_dir(table_dir).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();

            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.contains("-Data.db") {
                    components.push(SSTableComponent::Data);
                } else if file_name.contains("-Index.db") {
                    components.push(SSTableComponent::Index);
                } else if file_name.contains("-Summary.db") {
                    components.push(SSTableComponent::Summary);
                } else if file_name.contains("-Filter.db") {
                    components.push(SSTableComponent::Filter);
                } else if file_name.contains("-CompressionInfo.db") {
                    components.push(SSTableComponent::CompressionInfo);
                } else if file_name.contains("-Statistics.db") {
                    components.push(SSTableComponent::Statistics);
                } else if file_name.contains("-TOC.txt") {
                    components.push(SSTableComponent::Toc);
                } else if file_name.contains("-Digest.crc32") {
                    components.push(SSTableComponent::Digest);
                }
            }
        }

        Ok(components)
    }
}

/// Dataset discovery utilities
pub struct DatasetUtils;

impl DatasetUtils {
    /// Get all available datasets
    #[allow(dead_code)]
    pub fn get_available_datasets() -> Result<Vec<String>> {
        let datasets_path = if let Ok(datasets_root) = std::env::var("CQLITE_DATASETS_ROOT") {
            Path::new(&datasets_root).join("sstables")
        } else {
            // Default to relative path from cargo manifest
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .ok_or_else(|| Error::InvalidPath("Cannot find project root".to_string()))?
                .join("test-data")
                .join("datasets")
                .join("sstables")
        };

        let mut datasets = Vec::new();

        let entries = fs::read_dir(&datasets_path).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    datasets.push(name.to_string());
                }
            }
        }

        Ok(datasets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_context_creation() {
        let result = TestContext::new("test_basic").await;
        assert!(
            result.is_ok(),
            "Failed to create test context: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_dataset_discovery() {
        let datasets = DatasetUtils::get_available_datasets();
        assert!(
            datasets.is_ok(),
            "Failed to discover datasets: {:?}",
            datasets.err()
        );

        let datasets = datasets.unwrap();
        assert!(!datasets.is_empty(), "No datasets found");
        assert!(
            datasets.contains(&"test_basic".to_string()),
            "test_basic dataset not found"
        );
    }

    #[tokio::test]
    async fn test_table_discovery() {
        let context = TestContext::new("test_basic").await.unwrap();
        let tables = context.get_available_tables();
        assert!(
            tables.is_ok(),
            "Failed to discover tables: {:?}",
            tables.err()
        );

        let tables = tables.unwrap();
        assert!(!tables.is_empty(), "No tables found in test_basic dataset");
    }

    #[test]
    fn test_metrics_collection() {
        let mut metrics = TestMetrics::default();
        metrics.cache_hits = 80;
        metrics.cache_misses = 20;

        assert_eq!(metrics.cache_hits + metrics.cache_misses, 100);
    }

    #[test]
    fn test_offset_validation() {
        let offsets = vec![(0, 100), (100, 200), (200, 300)];
        let result = AssertionHelpers::validate_offsets(300, &offsets, "test");
        assert!(result.is_ok(), "Valid offsets should pass validation");

        let invalid_offsets = vec![(0, 100), (100, 200), (200, 400)];
        let result = AssertionHelpers::validate_offsets(300, &invalid_offsets, "test");
        assert!(result.is_err(), "Invalid offsets should fail validation");
    }
}

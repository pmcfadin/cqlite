//! Test Data Organization and Artifact Management
//!
//! This module defines the organization structure for real Cassandra 5 SSTable
//! test artifacts and provides utilities for managing test data.

use std::path::{Path, PathBuf};
use std::collections::HashMap;

use cqlite_core::{Result, RowKey, Value, schema::TableSchema};
use serde::{Deserialize, Serialize};

/// Test artifact organization structure
#[derive(Debug, Clone)]
pub struct ArtifactOrganization {
    /// Base artifacts directory
    pub base_dir: PathBuf,
    /// Available test data sets
    pub datasets: HashMap<String, DatasetInfo>,
}

/// Information about a test dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetInfo {
    /// Dataset name
    pub name: String,
    /// Description of the dataset
    pub description: String,
    /// Tables included in this dataset
    pub tables: HashMap<String, TableInfo>,
    /// Cassandra version used to generate this data
    pub cassandra_version: String,
    /// Creation timestamp
    pub created_at: String,
    /// Expected test scenarios this dataset supports
    pub supported_scenarios: Vec<String>,
}

/// Information about a table in a dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Keyspace name
    pub keyspace: String,
    /// Number of partitions
    pub partition_count: usize,
    /// Average partition size in bytes
    pub avg_partition_size: usize,
    /// Total data size in bytes
    pub total_size: usize,
    /// SSTable files for this table
    pub sstable_files: SSTableFiles,
    /// Schema definition
    pub schema_file: Option<PathBuf>,
    /// Known test data file
    pub test_data_file: Option<PathBuf>,
    /// Compression algorithm used
    pub compression: Option<String>,
}

/// SSTable file triplet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableFiles {
    /// Path to Data.db file
    pub data_file: PathBuf,
    /// Path to Index.db file
    pub index_file: PathBuf,
    /// Path to Summary.db file
    pub summary_file: PathBuf,
    /// Optional Statistics.db file
    pub statistics_file: Option<PathBuf>,
    /// Optional Filter.db file (bloom filter)
    pub filter_file: Option<PathBuf>,
}

/// Known test data for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestDataManifest {
    /// Known existing partition keys
    pub existing_keys: Vec<KnownKey>,
    /// Known non-existing keys for negative testing
    pub nonexistent_keys: Vec<String>,
    /// Scan test cases
    pub scan_cases: Vec<ScanTestCase>,
    /// Token range test cases
    pub token_ranges: Vec<TokenRangeTestCase>,
    /// Performance expectations
    pub performance_baseline: Option<PerformanceBaseline>,
}

/// Known key with expected value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownKey {
    /// Partition key
    pub key: String,
    /// Expected value (serialized)
    pub expected_value: String,
    /// Value type for deserialization
    pub value_type: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Scan test case definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanTestCase {
    /// Test case name
    pub name: String,
    /// Start key (optional)
    pub start_key: Option<String>,
    /// End key (optional)
    pub end_key: Option<String>,
    /// Limit (optional)
    pub limit: Option<usize>,
    /// Expected result count
    pub expected_count: usize,
    /// Sample of expected results
    pub expected_sample: Vec<KnownKey>,
}

/// Token range test case
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRangeTestCase {
    /// Test case name
    pub name: String,
    /// Start token
    pub start_token: i64,
    /// End token
    pub end_token: i64,
    /// Expected partition count in range
    pub expected_partitions: usize,
    /// Sample keys expected in range
    pub sample_keys: Vec<String>,
}

/// Performance baseline for regression testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    /// Baseline creation date
    pub created_at: String,
    /// Cassandra version used
    pub cassandra_version: String,
    /// Environment info
    pub environment: String,
    /// Operation baselines
    pub operations: HashMap<String, OperationBaseline>,
}

/// Performance baseline for a specific operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationBaseline {
    /// Operation name
    pub operation: String,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
    /// P95 latency in milliseconds
    pub p95_latency_ms: f64,
    /// P99 latency in milliseconds
    pub p99_latency_ms: f64,
    /// Throughput in operations per second
    pub throughput_ops_sec: f64,
    /// Memory usage in KB
    pub memory_usage_kb: usize,
    /// Number of samples
    pub sample_count: usize,
}

impl ArtifactOrganization {
    /// Create new artifact organization from base directory
    pub async fn new(base_dir: PathBuf) -> Result<Self> {
        let mut organization = Self {
            base_dir,
            datasets: HashMap::new(),
        };

        organization.discover_datasets().await?;
        Ok(organization)
    }

    /// Discover available datasets in the artifacts directory
    async fn discover_datasets(&mut self) -> Result<()> {
        use cqlite_core::platform::Platform;
        use cqlite_core::Config;
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        if !platform.fs().exists(&self.base_dir).await? {
            return Ok(());
        }

        let mut dir_entries = platform.fs().read_dir(&self.base_dir).await?;

        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(dataset_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Ok(dataset_info) = self.load_dataset_info(&path, dataset_name).await {
                        self.datasets.insert(dataset_name.to_string(), dataset_info);
                    }
                }
            }
        }

        Ok(())
    }

    /// Load dataset information from directory
    async fn load_dataset_info(&self, dataset_path: &Path, dataset_name: &str) -> Result<DatasetInfo> {
        let manifest_path = dataset_path.join("dataset.json");

        use cqlite_core::platform::Platform;
        use cqlite_core::Config;
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        let dataset_info = if platform.fs().exists(&manifest_path).await? {
            // Load from manifest file
            let manifest_content = platform.fs().read_to_string(&manifest_path).await?;
            serde_json::from_str(&manifest_content)
                .map_err(|e| crate::error::Error::parsing_error(format!("Invalid dataset manifest: {}", e)))?
        } else {
            // Create default dataset info by discovering tables
            self.create_default_dataset_info(dataset_path, dataset_name).await?
        };

        Ok(dataset_info)
    }

    /// Create default dataset info by discovering table directories
    async fn create_default_dataset_info(&self, dataset_path: &Path, dataset_name: &str) -> Result<DatasetInfo> {
        use cqlite_core::platform::Platform;
        use cqlite_core::Config;
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        let mut tables = HashMap::new();
        let mut dir_entries = platform.fs().read_dir(dataset_path).await?;

        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(table_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Ok(table_info) = self.discover_table_info(&path, table_name).await {
                        tables.insert(table_name.to_string(), table_info);
                    }
                }
            }
        }

        Ok(DatasetInfo {
            name: dataset_name.to_string(),
            description: format!("Auto-discovered dataset: {}", dataset_name),
            tables,
            cassandra_version: "5.0.x".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            supported_scenarios: vec![
                "get_single_key".to_string(),
                "scan_full_table".to_string(),
                "lookup_partition_basic".to_string(),
            ],
        })
    }

    /// Discover table information from directory
    async fn discover_table_info(&self, table_path: &Path, table_name: &str) -> Result<TableInfo> {
        use cqlite_core::platform::Platform;
        use cqlite_core::Config;
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        // Find SSTable files
        let mut data_file = None;
        let mut index_file = None;
        let mut summary_file = None;
        let mut statistics_file = None;
        let mut filter_file = None;

        let mut dir_entries = platform.fs().read_dir(table_path).await?;
        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.ends_with("-Data.db") {
                    data_file = Some(path.clone());
                } else if filename.ends_with("-Index.db") {
                    index_file = Some(path.clone());
                } else if filename.ends_with("-Summary.db") {
                    summary_file = Some(path.clone());
                } else if filename.ends_with("-Statistics.db") {
                    statistics_file = Some(path.clone());
                } else if filename.ends_with("-Filter.db") {
                    filter_file = Some(path.clone());
                }
            }
        }

        let data_file = data_file.ok_or_else(|| {
            crate::error::Error::file_not_found(&format!("Data.db not found in {}", table_path.display()))
        })?;
        let index_file = index_file.ok_or_else(|| {
            crate::error::Error::file_not_found(&format!("Index.db not found in {}", table_path.display()))
        })?;
        let summary_file = summary_file.ok_or_else(|| {
            crate::error::Error::file_not_found(&format!("Summary.db not found in {}", table_path.display()))
        })?;

        // Get file sizes for metadata
        let data_size = platform.fs().metadata(&data_file).await?.len() as usize;

        let sstable_files = SSTableFiles {
            data_file,
            index_file,
            summary_file,
            statistics_file,
            filter_file,
        };

        // Look for schema and test data files
        let schema_file = {
            let schema_path = table_path.join("schema.cql");
            if platform.fs().exists(&schema_path).await? {
                Some(schema_path)
            } else {
                None
            }
        };

        let test_data_file = {
            let test_data_path = table_path.join("test_data.json");
            if platform.fs().exists(&test_data_path).await? {
                Some(test_data_path)
            } else {
                None
            }
        };

        Ok(TableInfo {
            name: table_name.to_string(),
            keyspace: "test".to_string(), // Default keyspace
            partition_count: 0, // Would be discovered by parsing
            avg_partition_size: 0,
            total_size: data_size,
            sstable_files,
            schema_file,
            test_data_file,
            compression: None, // Would be detected from SSTable metadata
        })
    }

    /// Get dataset by name
    pub fn get_dataset(&self, name: &str) -> Option<&DatasetInfo> {
        self.datasets.get(name)
    }

    /// List all available datasets
    pub fn list_datasets(&self) -> Vec<&DatasetInfo> {
        self.datasets.values().collect()
    }

    /// Create the recommended directory structure
    pub async fn create_recommended_structure(&self) -> Result<()> {
        use cqlite_core::platform::Platform;
        use cqlite_core::Config;
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        // Create base directory
        platform.fs().create_dir_all(&self.base_dir).await?;

        // Create dataset directories
        let datasets = vec![
            ("simple_table", "Basic single-partition tables for fundamental testing"),
            ("multi_partition", "Multiple partitions for range queries and scanning"),
            ("wide_partitions", "Large partitions to test index efficiency"),
            ("complex_types", "Tables with collections, UDTs, and complex data types"),
        ];

        for (dataset_name, description) in datasets {
            let dataset_dir = self.base_dir.join(dataset_name);
            platform.fs().create_dir_all(&dataset_dir).await?;

            // Create README.md
            let readme_content = format!(
                "# {} Dataset\n\n{}\n\n## Structure\n\nEach table should have:\n- `*-Data.db` - Main data file\n- `*-Index.db` - Index file\n- `*-Summary.db` - Summary file\n- `schema.cql` - CQL table definition (optional)\n- `test_data.json` - Known test data manifest (optional)\n\n## Usage\n\nPlace real Cassandra 5.x SSTable files here for golden-path testing.\n",
                dataset_name.replace('_', " ").to_title_case(),
                description
            );
            platform.fs().write(&dataset_dir.join("README.md"), readme_content.as_bytes()).await?;

            // Create sample dataset.json
            let sample_dataset = DatasetInfo {
                name: dataset_name.to_string(),
                description: description.to_string(),
                tables: HashMap::new(),
                cassandra_version: "5.0.x".to_string(),
                created_at: chrono::Utc::now().to_rfc3339(),
                supported_scenarios: vec![
                    "get_single_key".to_string(),
                    "scan_full_table".to_string(),
                ],
            };

            let dataset_json = serde_json::to_string_pretty(&sample_dataset)?;
            platform.fs().write(&dataset_dir.join("dataset.json.example"), dataset_json.as_bytes()).await?;

            // Create sample test_data.json
            let sample_test_data = TestDataManifest {
                existing_keys: vec![
                    KnownKey {
                        key: "test_key_1".to_string(),
                        expected_value: "test_value_1".to_string(),
                        value_type: "text".to_string(),
                        metadata: HashMap::new(),
                    }
                ],
                nonexistent_keys: vec!["nonexistent_key".to_string()],
                scan_cases: vec![
                    ScanTestCase {
                        name: "full_scan".to_string(),
                        start_key: None,
                        end_key: None,
                        limit: None,
                        expected_count: 10,
                        expected_sample: vec![],
                    }
                ],
                token_ranges: vec![],
                performance_baseline: None,
            };

            let test_data_json = serde_json::to_string_pretty(&sample_test_data)?;
            platform.fs().write(&dataset_dir.join("test_data.json.example"), test_data_json.as_bytes()).await?;
        }

        Ok(())
    }
}

trait ToTitleCase {
    fn to_title_case(&self) -> String;
}

impl ToTitleCase for str {
    fn to_title_case(&self) -> String {
        self.split_whitespace()
            .map(|word| {
                let mut chars = word.chars();
                match chars.next() {
                    None => String::new(),
                    Some(first) => first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Load test data manifest from file
pub async fn load_test_data_manifest(path: &Path) -> Result<TestDataManifest> {
    use cqlite_core::platform::Platform;
    use cqlite_core::Config;
    use std::sync::Arc;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let content = platform.fs().read_to_string(path).await?;
    let manifest: TestDataManifest = serde_json::from_str(&content)
        .map_err(|e| crate::error::Error::parsing_error(format!("Invalid test data manifest: {}", e)))?;

    Ok(manifest)
}

/// Save test data manifest to file
pub async fn save_test_data_manifest(path: &Path, manifest: &TestDataManifest) -> Result<()> {
    use cqlite_core::platform::Platform;
    use cqlite_core::Config;
    use std::sync::Arc;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let content = serde_json::to_string_pretty(manifest)?;
    platform.fs().write(path, content.as_bytes()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_artifact_organization_creation() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        let organization = ArtifactOrganization::new(base_dir).await.unwrap();
        assert_eq!(organization.datasets.len(), 0); // No datasets initially
    }

    #[tokio::test]
    async fn test_recommended_structure_creation() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        let organization = ArtifactOrganization::new(base_dir.clone()).await.unwrap();
        organization.create_recommended_structure().await.unwrap();

        // Check that directories were created
        assert!(base_dir.join("simple_table").exists());
        assert!(base_dir.join("multi_partition").exists());
        assert!(base_dir.join("wide_partitions").exists());
        assert!(base_dir.join("complex_types").exists());

        // Check that README files were created
        assert!(base_dir.join("simple_table").join("README.md").exists());
    }

    #[test]
    fn test_test_data_manifest_serialization() {
        let manifest = TestDataManifest {
            existing_keys: vec![
                KnownKey {
                    key: "test".to_string(),
                    expected_value: "value".to_string(),
                    value_type: "text".to_string(),
                    metadata: HashMap::new(),
                }
            ],
            nonexistent_keys: vec!["nonexistent".to_string()],
            scan_cases: vec![],
            token_ranges: vec![],
            performance_baseline: None,
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: TestDataManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(manifest.existing_keys.len(), deserialized.existing_keys.len());
        assert_eq!(manifest.existing_keys[0].key, deserialized.existing_keys[0].key);
    }
}
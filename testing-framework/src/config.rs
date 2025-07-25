//! Configuration management for the testing framework

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfig {
    pub environment: EnvironmentConfig,
    pub docker: DockerConfig,
    pub cqlite: CQLiteConfig,
    pub comparison: ComparisonConfig,
    pub reporting: ReportingConfig,
    pub test_data: TestDataConfig,
}

/// Environment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentConfig {
    pub test_timeout_seconds: u64,
    pub parallel_execution: bool,
    pub max_concurrent_tests: usize,
    pub retry_failed_tests: bool,
    pub max_retries: usize,
    pub log_level: String,
}

/// Docker configuration for Cassandra
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerConfig {
    pub container_name: String,
    pub image: String,
    pub network_name: String,
    pub cassandra_port: u16,
    pub startup_timeout_seconds: u64,
    pub health_check_interval_seconds: u64,
    pub data_volume: String,
    pub scripts_path: PathBuf,
}

/// CQLite configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CQLiteConfig {
    pub binary_path: PathBuf,
    pub database_path: PathBuf,
    pub default_keyspace: String,
    pub connection_timeout_seconds: u64,
    pub query_timeout_seconds: u64,
    pub output_format: String,
}

/// Output comparison configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonConfig {
    pub ignore_whitespace: bool,
    pub ignore_case: bool,
    pub ignore_column_order: bool,
    pub ignore_row_order: bool,
    pub ignore_timestamps: bool,
    pub timestamp_tolerance_ms: u64,
    pub numeric_precision_tolerance: f64,
    pub normalize_uuids: bool,
    pub custom_normalizers: Vec<String>,
}

/// Reporting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportingConfig {
    pub output_directory: PathBuf,
    pub generate_html: bool,
    pub generate_json: bool,
    pub generate_csv: bool,
    pub generate_diff_files: bool,
    pub include_success_details: bool,
    pub max_diff_lines: usize,
    pub compress_large_outputs: bool,
}

/// Test data configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestDataConfig {
    pub schema_files: Vec<PathBuf>,
    pub data_files: Vec<PathBuf>,
    pub test_queries: Vec<PathBuf>,
    pub custom_test_cases: Vec<PathBuf>,
    pub generate_random_data: bool,
    pub random_data_seed: u64,
    pub max_random_records: usize,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            environment: EnvironmentConfig {
                test_timeout_seconds: 300,
                parallel_execution: true,
                max_concurrent_tests: 4,
                retry_failed_tests: true,
                max_retries: 2,
                log_level: "info".to_string(),
            },
            docker: DockerConfig {
                container_name: "cassandra-test".to_string(),
                image: "cassandra:5.0".to_string(),
                network_name: "cassandra-network".to_string(),
                cassandra_port: 9042,
                startup_timeout_seconds: 120,
                health_check_interval_seconds: 10,
                data_volume: "cassandra-test-data".to_string(),
                scripts_path: PathBuf::from("../test-env/cassandra5/scripts"),
            },
            cqlite: CQLiteConfig {
                binary_path: PathBuf::from("../target/release/cqlite"),
                database_path: PathBuf::from("./test_data/cqlite.db"),
                default_keyspace: "test_keyspace".to_string(),
                connection_timeout_seconds: 30,
                query_timeout_seconds: 60,
                output_format: "table".to_string(),
            },
            comparison: ComparisonConfig {
                ignore_whitespace: true,
                ignore_case: false,
                ignore_column_order: false,
                ignore_row_order: false,
                ignore_timestamps: false,
                timestamp_tolerance_ms: 1000,
                numeric_precision_tolerance: 0.0001,
                normalize_uuids: true,
                custom_normalizers: vec![],
            },
            reporting: ReportingConfig {
                output_directory: PathBuf::from("./test_results"),
                generate_html: true,
                generate_json: true,
                generate_csv: true,
                generate_diff_files: true,
                include_success_details: false,
                max_diff_lines: 1000,
                compress_large_outputs: true,
            },
            test_data: TestDataConfig {
                schema_files: vec![
                    PathBuf::from("../test-env/cassandra5/scripts/create-keyspaces.cql")
                ],
                data_files: vec![
                    PathBuf::from("../test-env/cassandra5/docker/generate-test-data-fixed.cql")
                ],
                test_queries: vec![
                    PathBuf::from("./test_queries")
                ],
                custom_test_cases: vec![],
                generate_random_data: false,
                random_data_seed: 42,
                max_random_records: 1000,
            },
        }
    }
}

impl TestConfig {
    /// Load configuration from file
    pub fn load_from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        
        if path.extension().and_then(|s| s.to_str()) == Some("yaml") 
           || path.extension().and_then(|s| s.to_str()) == Some("yml") {
            Ok(serde_yaml::from_str(&content)?)
        } else {
            Ok(serde_json::from_str(&content)?)
        }
    }

    /// Save configuration to file
    pub fn save_to_file(&self, path: &PathBuf) -> Result<()> {
        let content = if path.extension().and_then(|s| s.to_str()) == Some("yaml")
                         || path.extension().and_then(|s| s.to_str()) == Some("yml") {
            serde_yaml::to_string(self)?
        } else {
            serde_json::to_string_pretty(self)?
        };

        std::fs::write(path, content)?;
        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Check binary paths exist
        if !self.cqlite.binary_path.exists() {
            anyhow::bail!("CQLite binary not found at: {:?}", self.cqlite.binary_path);
        }

        // Check schema files exist
        for schema_file in &self.test_data.schema_files {
            if !schema_file.exists() {
                anyhow::bail!("Schema file not found: {:?}", schema_file);
            }
        }

        // Validate timeout values
        if self.environment.test_timeout_seconds == 0 {
            anyhow::bail!("Test timeout must be greater than 0");
        }

        if self.docker.startup_timeout_seconds == 0 {
            anyhow::bail!("Docker startup timeout must be greater than 0");
        }

        // Validate output directory can be created
        if let Some(parent) = self.reporting.output_directory.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        Ok(())
    }

    /// Create a minimal test configuration for CI/CD
    pub fn minimal() -> Self {
        let mut config = Self::default();
        config.environment.max_concurrent_tests = 1;
        config.environment.parallel_execution = false;
        config.docker.startup_timeout_seconds = 60;
        config.reporting.generate_html = false;
        config.reporting.include_success_details = false;
        config
    }

    /// Create a comprehensive test configuration for full validation
    pub fn comprehensive() -> Self {
        let mut config = Self::default();
        config.environment.max_concurrent_tests = 8;
        config.environment.parallel_execution = true;
        config.comparison.ignore_whitespace = false;
        config.reporting.include_success_details = true;
        config.test_data.generate_random_data = true;
        config.test_data.max_random_records = 10000;
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = TestConfig::default();
        assert_eq!(config.environment.test_timeout_seconds, 300);
        assert_eq!(config.docker.cassandra_port, 9042);
        assert!(config.comparison.ignore_whitespace);
    }

    #[test]
    fn test_config_serialization() {
        let config = TestConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TestConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.environment.test_timeout_seconds, deserialized.environment.test_timeout_seconds);
    }

    #[test]
    fn test_minimal_config() {
        let config = TestConfig::minimal();
        assert_eq!(config.environment.max_concurrent_tests, 1);
        assert!(!config.environment.parallel_execution);
    }

    #[test]
    fn test_comprehensive_config() {
        let config = TestConfig::comprehensive();
        assert_eq!(config.environment.max_concurrent_tests, 8);
        assert!(config.environment.parallel_execution);
        assert!(config.test_data.generate_random_data);
    }
}
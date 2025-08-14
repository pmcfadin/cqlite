//! Wide partition test data generator for promoted index validation
//!
//! This module creates test datasets specifically designed to force the creation
//! of promoted index entries, allowing validation of Index.db parsing for wide partitions.
//!
//! ## Promoted Index Creation Thresholds
//!
//! Cassandra 5+ creates promoted index entries for wide partitions based on these criteria:
//!
//! - **Partition Size Threshold**: Partitions exceeding 64KB (~65,536 bytes) trigger promoted index creation
//! - **Clustering Key Count**: High clustering key counts contribute to partition size
//! - **Row Size Impact**: Larger individual rows accelerate threshold crossing
//!
//! ### Configuration Guidelines
//!
//! To guarantee promoted index creation, ensure:
//! ```
//! clustering_keys_per_partition * row_size_bytes >= 64KB (65,536 bytes)
//! ```
//!
//! **Examples:**
//! - 1,000 clustering keys × 100 bytes/row = 100KB ✅ (promotes)
//! - 10,000 clustering keys × 10 bytes/row = 100KB ✅ (promotes) 
//! - 100 clustering keys × 100 bytes/row = 10KB ❌ (no promotion)
//!
//! **Default Configuration:**
//! - `clustering_keys_per_partition: 10,000` 
//! - `row_size_bytes: 1,024` (1KB)
//! - **Result**: 10,000 × 1,024 = ~10MB per partition → **Guaranteed promoted index**
//!
//! The validation framework verifies that generated partitions actually trigger
//! promoted index creation by analyzing the resulting Index.db files.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Configuration for generating wide partition test data
#[derive(Debug, Clone)]
pub struct WidePartitionConfig {
    /// Number of clustering keys per partition (to force wide partitions)
    pub clustering_keys_per_partition: usize,
    /// Size of each row in bytes (to exceed promoted index threshold)
    pub row_size_bytes: usize,
    /// Number of partitions to create
    pub partition_count: usize,
    /// Table name for the test
    pub table_name: String,
}

impl Default for WidePartitionConfig {
    fn default() -> Self {
        Self {
            clustering_keys_per_partition: 10_000, // Force promoted index
            row_size_bytes: 1024,                  // 1KB per row
            partition_count: 5,
            table_name: "wide_partition_test".to_string(),
        }
    }
}

/// Wide partition test generator
pub struct WidePartitionTestGenerator {
    config: WidePartitionConfig,
    output_dir: PathBuf,
}

impl WidePartitionTestGenerator {
    /// Create a new test generator
    pub fn new(config: WidePartitionConfig, output_dir: PathBuf) -> Self {
        Self { config, output_dir }
    }

    /// Generate CQL schema for wide partition table
    pub fn generate_schema(&self) -> String {
        format!(
            r#"
CREATE KEYSPACE IF NOT EXISTS test_keyspace 
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};

USE test_keyspace;

CREATE TABLE IF NOT EXISTS {table_name} (
    partition_key text,
    clustering_key text,
    data_column text,
    timestamp_column timestamp,
    large_data blob,
    PRIMARY KEY (partition_key, clustering_key)
) WITH compression = {{'class': 'LZ4Compressor', 'chunk_length_in_kb': 64}};
"#,
            table_name = self.config.table_name
        )
    }

    /// Generate CQL insert statements for wide partitions
    pub fn generate_inserts(&self) -> Result<Vec<String>> {
        let mut inserts = Vec::new();

        for partition_id in 0..self.config.partition_count {
            for clustering_id in 0..self.config.clustering_keys_per_partition {
                let large_data =
                    "0x".to_string() + &"deadbeef".repeat(self.config.row_size_bytes / 4);

                let insert = format!(
                    "INSERT INTO {table_name} (partition_key, clustering_key, data_column, timestamp_column, large_data) VALUES ('partition_{partition_id}', 'clustering_{clustering_id}', 'data_{partition_id}_{clustering_id}', toTimestamp(now()), {large_data});",
                    table_name = self.config.table_name,
                    partition_id = partition_id,
                    clustering_id = clustering_id,
                    large_data = large_data
                );
                inserts.push(insert);

                // Log progress for large datasets
                if (partition_id * self.config.clustering_keys_per_partition + clustering_id) % 1000
                    == 0
                {
                    println!(
                        "Generated {} inserts...",
                        partition_id * self.config.clustering_keys_per_partition + clustering_id
                    );
                }
            }
        }

        Ok(inserts)
    }

    /// Generate validation metrics for promoted index testing
    pub fn generate_validation_metrics(&self) -> WidePartitionMetrics {
        let total_rows = self.config.partition_count * self.config.clustering_keys_per_partition;
        let estimated_partition_size =
            self.config.clustering_keys_per_partition * self.config.row_size_bytes;

        // Cassandra typically creates promoted index when partition exceeds 64KB
        let promoted_index_threshold = 64 * 1024;
        let expected_promoted_partitions = if estimated_partition_size > promoted_index_threshold {
            self.config.partition_count
        } else {
            0
        };

        WidePartitionMetrics {
            config: self.config.clone(),
            total_rows,
            estimated_partition_size,
            expected_promoted_partitions,
            promoted_index_threshold,
            force_promoted_index: estimated_partition_size > promoted_index_threshold,
        }
    }

    /// Write test data files to output directory
    pub async fn write_test_files(&self) -> Result<WidePartitionTestFiles> {
        tokio::fs::create_dir_all(&self.output_dir)
            .await
            .context("Failed to create output directory")?;

        // Write schema file
        let schema_path = self.output_dir.join("schema.cql");
        tokio::fs::write(&schema_path, self.generate_schema())
            .await
            .context("Failed to write schema file")?;

        // Write insert statements
        let inserts_path = self.output_dir.join("inserts.cql");
        let inserts = self.generate_inserts()?;
        let inserts_content = inserts.join("\n");
        tokio::fs::write(&inserts_path, inserts_content)
            .await
            .context("Failed to write inserts file")?;

        // Write validation metrics
        let metrics_path = self.output_dir.join("validation_metrics.json");
        let metrics = self.generate_validation_metrics();
        let metrics_json = serde_json::to_string_pretty(&metrics)?;
        tokio::fs::write(&metrics_path, metrics_json)
            .await
            .context("Failed to write metrics file")?;

        // Write Docker compose for data generation
        let docker_compose_path = self.output_dir.join("docker-compose.yml");
        let docker_compose_content = self.generate_docker_compose();
        tokio::fs::write(&docker_compose_path, docker_compose_content)
            .await
            .context("Failed to write docker-compose file")?;

        Ok(WidePartitionTestFiles {
            schema_file: schema_path,
            inserts_file: inserts_path,
            metrics_file: metrics_path,
            docker_compose_file: docker_compose_path,
            output_directory: self.output_dir.clone(),
        })
    }

    /// Generate Docker Compose configuration for test data creation
    fn generate_docker_compose(&self) -> String {
        format!(
            r#"version: '3.8'

services:
  cassandra:
    image: cassandra:5.0
    environment:
      - CASSANDRA_CLUSTER_NAME=TestCluster
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_RACK=rack1
    ports:
      - "9042:9042"
    volumes:
      - ./schema.cql:/schema.cql
      - ./inserts.cql:/inserts.cql
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe cluster'"]
      interval: 30s
      timeout: 10s
      retries: 5

  data-generator:
    image: cassandra:5.0
    depends_on:
      cassandra:
        condition: service_healthy
    volumes:
      - ./schema.cql:/schema.cql
      - ./inserts.cql:/inserts.cql
      - ./output:/output
    command: |
      bash -c "
        echo 'Setting up wide partition test data...'
        cqlsh cassandra -f /schema.cql
        echo 'Schema created, inserting test data...'
        cqlsh cassandra -f /inserts.cql
        echo 'Data inserted, generating SSTable files...'
        nodetool -h cassandra flush
        echo 'Copying SSTable files...'
        docker cp cassandra:/var/lib/cassandra/data/test_keyspace/{table_name}*/ /output/
        echo 'Wide partition test data generation complete!'
      "
"#,
            table_name = self.config.table_name
        )
    }
}

/// Metrics for validating wide partition test generation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WidePartitionMetrics {
    pub config: WidePartitionConfig,
    pub total_rows: usize,
    pub estimated_partition_size: usize,
    pub expected_promoted_partitions: usize,
    pub promoted_index_threshold: usize,
    pub force_promoted_index: bool,
}

impl WidePartitionMetrics {
    /// Get human-readable summary
    pub fn summary(&self) -> String {
        format!(
            r#"Wide Partition Test Metrics:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Configuration:
  • Table: {}
  • Partitions: {}
  • Clustering keys per partition: {}
  • Row size: {} bytes
  • Total rows: {}

Promoted Index Analysis:
  • Estimated partition size: {} bytes ({:.1} KB)
  • Promoted index threshold: {} bytes ({} KB)
  • Expected promoted partitions: {}
  • Will force promoted index: {}

Validation Expectations:
  • Index.db should contain {} partition entries
  • {} partitions should have promoted index entries
  • Each promoted partition should have ~{} index entries
  • Total data size: ~{:.1} MB

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"#,
            self.config.table_name,
            self.config.partition_count,
            self.config.clustering_keys_per_partition,
            self.config.row_size_bytes,
            self.total_rows,
            self.estimated_partition_size,
            self.estimated_partition_size as f64 / 1024.0,
            self.promoted_index_threshold,
            self.promoted_index_threshold / 1024,
            self.expected_promoted_partitions,
            if self.force_promoted_index {
                "✅ YES"
            } else {
                "❌ NO"
            },
            self.config.partition_count,
            self.expected_promoted_partitions,
            self.config.clustering_keys_per_partition / 100, // Estimated promoted index entries
            (self.total_rows * self.config.row_size_bytes) as f64 / (1024.0 * 1024.0)
        )
    }
}

/// Generated test files for wide partition validation
#[derive(Debug, Clone)]
pub struct WidePartitionTestFiles {
    pub schema_file: PathBuf,
    pub inserts_file: PathBuf,
    pub metrics_file: PathBuf,
    pub docker_compose_file: PathBuf,
    pub output_directory: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wide_partition_generator() {
        let temp_dir = TempDir::new().unwrap();
        let config = WidePartitionConfig {
            clustering_keys_per_partition: 100, // Small for test
            row_size_bytes: 1024,
            partition_count: 2,
            table_name: "test_wide".to_string(),
        };

        let generator = WidePartitionTestGenerator::new(config, temp_dir.path().to_path_buf());
        let metrics = generator.generate_validation_metrics();

        assert_eq!(metrics.total_rows, 200);
        assert_eq!(metrics.estimated_partition_size, 102400); // 100 * 1024

        let test_files = generator.write_test_files().await.unwrap();
        assert!(test_files.schema_file.exists());
        assert!(test_files.inserts_file.exists());
        assert!(test_files.metrics_file.exists());
    }

    #[test]
    fn test_promoted_index_threshold_calculation() {
        let config = WidePartitionConfig {
            clustering_keys_per_partition: 1000,
            row_size_bytes: 100, // 1000 * 100 = 100KB > 64KB threshold
            partition_count: 3,
            table_name: "threshold_test".to_string(),
        };

        let generator = WidePartitionTestGenerator::new(config, PathBuf::from("/tmp"));
        let metrics = generator.generate_validation_metrics();

        assert!(metrics.force_promoted_index);
        assert_eq!(metrics.expected_promoted_partitions, 3);
    }
}

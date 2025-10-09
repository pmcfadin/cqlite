//! Ingestion Module for M2-CLI One-Shot Execution
//!
//! This module orchestrates schema loading and SSTable discovery to build
//! a fully-configured Database instance for one-shot query execution.

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::discovery::DiscoveryService;
use crate::error::{Error, Result};
use crate::schema::{
    aggregator::{AggregatorConfig, LoadResult, SchemaAggregator},
    registry::{SchemaRegistry, SchemaRegistryConfig},
    UdtRegistry,
};
use crate::Config;
use crate::Database;
use crate::Platform;

/// Configuration for one-shot ingestion
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Schema file paths (.cql or .json) to load
    pub schema_paths: Vec<PathBuf>,

    /// Root data directory containing SSTables (e.g., /var/lib/cassandra/data)
    pub data_dir: PathBuf,

    /// Optional Cassandra version hint (e.g., "5.0")
    pub version_hint: Option<String>,

    /// Core database configuration
    pub core_config: Config,
}

/// Result of ingestion operation
#[derive(Debug)]
pub struct IngestionResult {
    /// The initialized Database instance
    pub database: Database,

    /// Schema loading summary
    pub schema_load_result: LoadResult,

    /// SSTable discovery summary
    pub discovery_summary: DiscoverySummary,

    /// Schema registry for coverage reporting
    pub schema_registry: Arc<RwLock<SchemaRegistry>>,
}

/// SSTable discovery summary
#[derive(Debug, Clone)]
pub struct DiscoverySummary {
    /// Total number of SSTables discovered
    pub sstables_found: usize,
    /// Keyspaces discovered
    pub keyspaces: Vec<String>,
    /// Tables discovered per keyspace
    pub tables: Vec<String>,
    /// Full directory paths for each discovered table
    pub table_directories: Vec<PathBuf>,
    /// Resolved Cassandra version (from precedence)
    pub resolved_version: Option<String>,
}

/// Main ingestion function for one-shot execution
///
/// This function orchestrates:
/// 1. Schema loading from provided paths (CQL/JSON files)
/// 2. SSTable discovery from data directory
/// 3. Database construction with QueryEngine
///
/// # Errors
///
/// Returns Error::Schema for schema loading failures (exit code 3)
/// Returns Error::Io for discovery/data-dir failures (exit code 4)
/// Returns Error::QueryExecution for query engine setup failures (exit code 5)
pub async fn ingest(config: IngestionConfig) -> Result<IngestionResult> {
    // Step 1: Validate data directory exists
    if !config.data_dir.exists() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Data directory does not exist: {}",
                config.data_dir.display()
            ),
        )));
    }

    if !config.data_dir.is_dir() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Data directory path is not a directory: {}",
                config.data_dir.display()
            ),
        )));
    }

    // Step 2: Initialize Platform
    let platform = Arc::new(Platform::new(&config.core_config).await?);

    // Step 3: Load schemas using SchemaAggregator
    let registry_config = SchemaRegistryConfig::default();
    let schema_registry = Arc::new(RwLock::new(
        SchemaRegistry::new(
            registry_config,
            platform.clone(),
            config.core_config.clone(),
        )
        .await
        .map_err(|e| Error::Schema(format!("Failed to create schema registry: {}", e)))?,
    ));

    let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

    let aggregator_config = AggregatorConfig {
        graceful_degradation: false, // Fail fast for one-shot execution
        validate_udt_dependencies: true,
    };

    let mut aggregator = SchemaAggregator::new(
        schema_registry.clone(),
        udt_registry.clone(),
        aggregator_config,
    );

    let schema_load_result = if !config.schema_paths.is_empty() {
        aggregator
            .load_from_paths(&config.schema_paths)
            .await
            .map_err(|e| Error::Schema(format!("Schema loading failed: {}", e)))?
    } else {
        // No schema paths provided - return empty result
        LoadResult {
            schemas_loaded: 0,
            udts_loaded: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    };

    // Check for schema loading errors (fail fast)
    if !schema_load_result.errors.is_empty() {
        let error_messages: Vec<String> = schema_load_result
            .errors
            .iter()
            .map(|e| format!("{:?}: {}", e.error_type, e.message))
            .collect();
        return Err(Error::Schema(format!(
            "Schema loading failed with {} error(s): {}",
            schema_load_result.errors.len(),
            error_messages.join("; ")
        )));
    }

    // Step 4: Discover SSTables using DiscoveryService
    let discovery_service = DiscoveryService::with_schema_registry(
        config.data_dir.clone(),
        config.version_hint.clone(),
        schema_registry.clone(),
    );

    let service_summary = discovery_service.scan().await.map_err(|e| {
        // Map discovery errors to appropriate error types
        match e {
            Error::Io(_) => e,
            _ => Error::Io(std::io::Error::other(format!(
                "SSTable discovery failed: {}",
                e
            ))),
        }
    })?;

    // Step 5: Build Database with discovered SSTables
    // Use open_with_discovered_sstables() to avoid duplicate SSTable scanning
    // Storage path is data_dir (for runtime storage), discovered directories from DiscoveryService
    let database = Database::open_with_discovered_sstables(
        &config.data_dir,
        service_summary.table_directories.clone(),
        config.core_config.clone(),
    )
    .await
    .map_err(|e| {
        // Map database creation errors appropriately
        match e {
            Error::Schema(_) => e,
            Error::Io(_) => e,
            #[cfg(feature = "state_machine")]
            Error::QueryExecution(_) => e,
            _ => Error::QueryExecution(format!("Database initialization failed: {}", e)),
        }
    })?;

    // Convert from discovery module's DiscoverySummary to ingestion's DiscoverySummary
    let discovery_summary = DiscoverySummary {
        sstables_found: service_summary.sstables_found,
        keyspaces: service_summary.keyspaces,
        tables: service_summary.tables,
        table_directories: service_summary.table_directories,
        resolved_version: service_summary.resolved_version,
    };

    Ok(IngestionResult {
        database,
        schema_load_result,
        discovery_summary,
        schema_registry: schema_registry.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Note: Tests for discover_sstables() and resolve_cassandra_version()
    // have been removed as these functions are now in the discovery module
    // and are tested there.

    #[tokio::test]
    async fn test_ingest_invalid_data_dir() {
        let config = IngestionConfig {
            schema_paths: vec![],
            data_dir: PathBuf::from("/nonexistent/path"),
            version_hint: None,
            core_config: Config::default(),
        };

        let result = ingest(config).await;
        assert!(result.is_err());

        if let Err(Error::Io(io_err)) = result {
            assert_eq!(io_err.kind(), std::io::ErrorKind::NotFound);
        } else {
            panic!("Expected Io error for nonexistent directory");
        }
    }

    #[tokio::test]
    async fn test_ingest_with_empty_schema_paths() {
        let temp_dir = TempDir::new().unwrap();

        let config = IngestionConfig {
            schema_paths: vec![],
            data_dir: temp_dir.path().to_path_buf(),
            version_hint: Some("5.0".to_string()),
            core_config: Config::default(),
        };

        let result = ingest(config).await;
        assert!(result.is_ok());

        let ingestion_result = result.unwrap();
        assert_eq!(ingestion_result.schema_load_result.schemas_loaded, 0);
        assert_eq!(ingestion_result.schema_load_result.udts_loaded, 0);
    }
}

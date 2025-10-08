//! Ingestion Module for M2-CLI One-Shot Execution
//!
//! This module orchestrates schema loading and SSTable discovery to build
//! a fully-configured Database instance for one-shot query execution.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

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

    // Step 4: Discover SSTables from data directory
    let discovery_summary = discover_sstables(&config.data_dir, config.version_hint.as_deref())
        .await
        .map_err(|e| {
            // Map discovery errors to appropriate error types
            match e {
                Error::Io(_) => e,
                _ => Error::Io(std::io::Error::other(format!(
                    "SSTable discovery failed: {}",
                    e
                ))),
            }
        })?;

    // Step 5: Build Database using Database::open()
    // The Database::open() already handles StorageEngine and QueryEngine initialization
    let database = Database::open(&config.data_dir, config.core_config.clone())
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

    Ok(IngestionResult {
        database,
        schema_load_result,
        discovery_summary,
    })
}

/// Discover SSTables in the data directory
///
/// Scans the data directory for SSTable files and resolves the Cassandra version
/// using the precedence: version_hint > SSTable metadata > metadata.yml > unknown
async fn discover_sstables(
    data_dir: &Path,
    version_hint: Option<&str>,
) -> Result<DiscoverySummary> {
    // Step 1: Scan directory structure for keyspaces and tables
    let mut keyspaces = Vec::new();
    let mut tables = Vec::new();
    let mut sstables_found = 0;

    // Cassandra data directory structure is:
    // data_dir/keyspace_name/table_name-table_id/sstable_files
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let keyspace_name = entry.file_name().to_string_lossy().to_string();

                // Skip system keyspaces for discovery summary
                if !keyspace_name.starts_with("system") {
                    keyspaces.push(keyspace_name.clone());
                }

                // Scan for tables in this keyspace
                if let Ok(table_entries) = std::fs::read_dir(entry.path()) {
                    for table_entry in table_entries.flatten() {
                        if table_entry.path().is_dir() {
                            let table_dir_name =
                                table_entry.file_name().to_string_lossy().to_string();

                            // Extract table name (format: table_name-table_id)
                            let table_name = table_dir_name
                                .split('-')
                                .next()
                                .unwrap_or(&table_dir_name)
                                .to_string();

                            let qualified_name = format!("{}.{}", keyspace_name, table_name);
                            if !qualified_name.starts_with("system") {
                                tables.push(qualified_name);
                            }

                            // Count SSTable files (Data.db files)
                            if let Ok(sstable_files) = std::fs::read_dir(table_entry.path()) {
                                for sstable_file in sstable_files.flatten() {
                                    let file_name =
                                        sstable_file.file_name().to_string_lossy().to_string();
                                    if file_name.ends_with("-Data.db")
                                        || file_name.ends_with("Data.db")
                                    {
                                        sstables_found += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Step 2: Resolve Cassandra version using precedence
    let resolved_version = resolve_cassandra_version(data_dir, version_hint).await?;

    Ok(DiscoverySummary {
        sstables_found,
        keyspaces,
        tables,
        resolved_version,
    })
}

/// Resolve Cassandra version using precedence:
/// 1. version_hint (if provided)
/// 2. SSTable metadata (from Data.db headers)
/// 3. metadata.yml (cluster metadata)
/// 4. "unknown" (fallback)
async fn resolve_cassandra_version(
    data_dir: &Path,
    version_hint: Option<&str>,
) -> Result<Option<String>> {
    // Precedence 1: Use version hint if provided
    if let Some(hint) = version_hint {
        return Ok(Some(hint.to_string()));
    }

    // Precedence 2: Try to read version from SSTable metadata
    // This would require reading the first few bytes of a Data.db file
    // For now, we'll skip this and move to metadata.yml
    // TODO: Implement SSTable header version detection

    // Precedence 3: Try to read metadata.yml
    let metadata_path = data_dir.join("metadata.yml");
    if metadata_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&metadata_path) {
            // Parse YAML for version field
            // Simple string search (not full YAML parsing for now)
            for line in content.lines() {
                if line.trim().starts_with("version:") {
                    let version = line
                        .trim()
                        .strip_prefix("version:")
                        .unwrap_or("")
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_string();
                    if !version.is_empty() {
                        return Ok(Some(version));
                    }
                }
            }
        }
    }

    // Precedence 4: Unknown
    Ok(Some("unknown".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_resolve_cassandra_version_with_hint() {
        let temp_dir = TempDir::new().unwrap();
        let version = resolve_cassandra_version(temp_dir.path(), Some("5.0"))
            .await
            .unwrap();
        assert_eq!(version, Some("5.0".to_string()));
    }

    #[tokio::test]
    async fn test_resolve_cassandra_version_from_metadata_yml() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_content = "version: 5.0.1\nother: field\n";
        fs::write(temp_dir.path().join("metadata.yml"), metadata_content).unwrap();

        let version = resolve_cassandra_version(temp_dir.path(), None)
            .await
            .unwrap();
        assert_eq!(version, Some("5.0.1".to_string()));
    }

    #[tokio::test]
    async fn test_resolve_cassandra_version_unknown() {
        let temp_dir = TempDir::new().unwrap();
        let version = resolve_cassandra_version(temp_dir.path(), None)
            .await
            .unwrap();
        assert_eq!(version, Some("unknown".to_string()));
    }

    #[tokio::test]
    async fn test_discover_sstables_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let summary = discover_sstables(temp_dir.path(), None).await.unwrap();

        assert_eq!(summary.sstables_found, 0);
        assert!(summary.keyspaces.is_empty());
        assert!(summary.tables.is_empty());
    }

    #[tokio::test]
    async fn test_discover_sstables_with_structure() {
        let temp_dir = TempDir::new().unwrap();

        // Create keyspace/table directory structure
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();

        let table_dir = keyspace_dir.join("users-abc123");
        fs::create_dir(&table_dir).unwrap();

        // Create a mock SSTable file
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock data").unwrap();

        let summary = discover_sstables(temp_dir.path(), None).await.unwrap();

        assert_eq!(summary.sstables_found, 1);
        assert!(summary.keyspaces.contains(&"test_ks".to_string()));
        assert!(summary
            .tables
            .iter()
            .any(|t| t.starts_with("test_ks.users")));
    }

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

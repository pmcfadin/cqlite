//! Discovery service for orchestrating SSTable scanning and coverage analysis
//!
//! This module provides the main DiscoveryService that coordinates filesystem scanning,
//! version resolution, and schema coverage calculation.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::schema::registry::SchemaRegistry;

use super::coverage::{CoverageBadge, CoverageCalculator, CoverageInfo};
use super::scanner::Scanner;

/// Service for discovering SSTables and computing schema coverage
pub struct DiscoveryService {
    data_dir: PathBuf,
    version_hint: Option<String>,
    schema_registry: Option<Arc<RwLock<SchemaRegistry>>>,
}

impl DiscoveryService {
    /// Create service without schema coverage
    ///
    /// This creates a discovery service that will scan for SSTables and resolve
    /// the Cassandra version, but will not compute schema coverage.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path to the Cassandra data directory
    /// * `version_hint` - Optional Cassandra version hint (e.g., "5.0")
    pub fn new(data_dir: PathBuf, version_hint: Option<String>) -> Self {
        Self {
            data_dir,
            version_hint,
            schema_registry: None,
        }
    }

    /// Create service with schema coverage calculation
    ///
    /// This creates a discovery service that will scan for SSTables, resolve
    /// the Cassandra version, and compute schema coverage by comparing discovered
    /// tables with the schema registry.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path to the Cassandra data directory
    /// * `version_hint` - Optional Cassandra version hint (e.g., "5.0")
    /// * `schema_registry` - Schema registry for coverage calculation
    pub fn with_schema_registry(
        data_dir: PathBuf,
        version_hint: Option<String>,
        schema_registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            data_dir,
            version_hint,
            schema_registry: Some(schema_registry),
        }
    }

    /// Scan data directory and compute summary
    ///
    /// This method performs the following operations:
    /// 1. Scans the data directory for SSTables (skipping system keyspaces)
    /// 2. Resolves the Cassandra version using precedence rules
    /// 3. Computes schema coverage (if schema registry is available)
    /// 4. Generates a coverage badge
    ///
    /// # Returns
    ///
    /// A `DiscoverySummary` containing all discovered information
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data directory cannot be read
    /// - Version resolution fails
    /// - Coverage calculation fails (if schema registry is provided)
    pub async fn scan(&self) -> Result<DiscoverySummary> {
        let scanner = Scanner::new(&self.data_dir, self.version_hint.clone());
        let scan_result = scanner.scan()?;
        let resolved_version = scanner.resolve_version(&scan_result)?;

        // Extract table directory paths from keyspace_info
        let table_directories: Vec<PathBuf> = scan_result
            .keyspace_info
            .iter()
            .flat_map(|ks_info| {
                ks_info
                    .tables
                    .iter()
                    .map(|table_info| table_info.path.clone())
            })
            .collect();

        let (coverage, badge) = if let Some(registry) = &self.schema_registry {
            let calculator = CoverageCalculator::new(registry.clone());
            let coverage = calculator.calculate(&scan_result.tables).await?;
            let badge = calculator.compute_badge(&coverage);
            (Some(coverage), badge)
        } else {
            // No schema registry - cannot compute coverage
            (None, CoverageBadge::Unknown)
        };

        Ok(DiscoverySummary {
            timestamp: SystemTime::now(),
            data_dir: self.data_dir.clone(),
            keyspaces: scan_result.keyspaces,
            tables: scan_result.tables,
            table_directories,
            sstables_found: scan_result.sstable_count,
            resolved_version,
            coverage,
            badge,
        })
    }
}

/// Summary of discovery scan
///
/// This structure contains all information discovered during a scan operation,
/// including keyspaces, tables, SSTable counts, version information, and
/// schema coverage metrics.
#[derive(Debug, Clone)]
pub struct DiscoverySummary {
    /// Timestamp when the scan was performed
    pub timestamp: SystemTime,
    /// Data directory that was scanned
    pub data_dir: PathBuf,
    /// Keyspaces discovered (excluding system keyspaces)
    pub keyspaces: Vec<String>,
    /// Fully qualified table names discovered (excluding system tables)
    pub tables: Vec<String>,
    /// Full directory paths for each discovered table
    pub table_directories: Vec<PathBuf>,
    /// Total number of SSTables found
    pub sstables_found: usize,
    /// Resolved Cassandra version
    pub resolved_version: Option<String>,
    /// Schema coverage information (if schema registry was provided)
    pub coverage: Option<CoverageInfo>,
    /// Coverage badge indicating overall status
    pub badge: CoverageBadge,
}

impl DiscoverySummary {
    /// Get a human-readable summary string
    pub fn summary_text(&self) -> String {
        let mut text = String::new();

        text.push_str(&format!(
            "Discovered {} keyspaces, {} tables, {} SSTables\n",
            self.keyspaces.len(),
            self.tables.len(),
            self.sstables_found
        ));

        if let Some(version) = &self.resolved_version {
            text.push_str(&format!("Cassandra version: {}\n", version));
        }

        if let Some(coverage) = &self.coverage {
            text.push_str(&format!(
                "Schema coverage: {}/{} tables ({:.1}%) - Badge: {:?}\n",
                coverage.tables_with_schema,
                coverage.total_tables,
                coverage.coverage_percentage(),
                self.badge
            ));

            if !coverage.tables_missing_schema.is_empty() {
                text.push_str(&format!(
                    "  Missing schema: {}\n",
                    coverage.tables_missing_schema.join(", ")
                ));
            }

            if !coverage.schemas_without_data.is_empty() {
                text.push_str(&format!(
                    "  Schema without data: {}\n",
                    coverage.schemas_without_data.join(", ")
                ));
            }
        } else {
            text.push_str("Schema coverage: Not available (no schema registry)\n");
        }

        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::registry::{SchemaRegistry, SchemaRegistryConfig};
    use crate::schema::TableSchema;
    use crate::{Config, Platform};
    use std::fs;
    use tempfile::TempDir;

    async fn create_test_registry() -> Arc<RwLock<SchemaRegistry>> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let registry_config = SchemaRegistryConfig::default();

        Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ))
    }

    async fn register_test_schema(
        registry: Arc<RwLock<SchemaRegistry>>,
        keyspace: &str,
        table: &str,
    ) {
        use crate::schema::registry::SchemaSource;

        let schema = TableSchema::new_for_testing(keyspace, table);

        let reg = registry.write().await;
        reg.register_schema(schema, SchemaSource::Manual)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_discovery_service_without_schema_registry() {
        let temp_dir = TempDir::new().unwrap();

        // Create test data structure
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();
        let table_dir = keyspace_dir.join("users-abc123");
        fs::create_dir(&table_dir).unwrap();
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock").unwrap();

        let service = DiscoveryService::new(temp_dir.path().to_path_buf(), Some("5.0".to_string()));
        let summary = service.scan().await.unwrap();

        assert_eq!(summary.keyspaces.len(), 1);
        assert_eq!(summary.tables.len(), 1);
        assert_eq!(summary.table_directories.len(), 1);
        assert_eq!(summary.table_directories[0], table_dir);
        assert_eq!(summary.sstables_found, 1);
        assert_eq!(summary.resolved_version, Some("5.0".to_string()));
        assert!(summary.coverage.is_none());
        assert_eq!(summary.badge, CoverageBadge::Unknown);
    }

    #[tokio::test]
    async fn test_discovery_service_with_schema_registry() {
        let temp_dir = TempDir::new().unwrap();

        // Create test data structure
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();
        let table_dir = keyspace_dir.join("users-abc123");
        fs::create_dir(&table_dir).unwrap();
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock").unwrap();

        // Create schema registry and register matching schema
        let registry = create_test_registry().await;
        register_test_schema(registry.clone(), "test_ks", "users").await;

        let service = DiscoveryService::with_schema_registry(
            temp_dir.path().to_path_buf(),
            Some("5.0".to_string()),
            registry,
        );

        let summary = service.scan().await.unwrap();

        assert_eq!(summary.keyspaces.len(), 1);
        assert_eq!(summary.tables.len(), 1);
        assert_eq!(summary.table_directories.len(), 1);
        assert_eq!(summary.table_directories[0], table_dir);
        assert_eq!(summary.sstables_found, 1);
        assert_eq!(summary.resolved_version, Some("5.0".to_string()));

        // Check coverage
        assert!(summary.coverage.is_some());
        let coverage = summary.coverage.as_ref().unwrap();
        assert_eq!(coverage.total_tables, 1);
        assert_eq!(coverage.tables_with_schema, 1);
        assert_eq!(coverage.tables_missing_schema.len(), 0);

        // Should be green badge (100% coverage)
        assert_eq!(summary.badge, CoverageBadge::Green);
    }

    #[tokio::test]
    async fn test_discovery_service_partial_coverage() {
        let temp_dir = TempDir::new().unwrap();

        // Create test data structure with multiple tables
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();

        let mut expected_dirs = Vec::new();
        for table_name in &["users", "posts", "comments"] {
            let table_dir = keyspace_dir.join(format!("{}-abc123", table_name));
            fs::create_dir(&table_dir).unwrap();
            fs::write(table_dir.join("na-1-big-Data.db"), b"mock").unwrap();
            expected_dirs.push(table_dir);
        }

        // Create schema registry and register only one schema
        let registry = create_test_registry().await;
        register_test_schema(registry.clone(), "test_ks", "users").await;

        let service =
            DiscoveryService::with_schema_registry(temp_dir.path().to_path_buf(), None, registry);

        let summary = service.scan().await.unwrap();

        assert_eq!(summary.tables.len(), 3);
        assert_eq!(summary.table_directories.len(), 3);
        // Verify all expected directories are present (order may vary)
        for expected_dir in &expected_dirs {
            assert!(summary.table_directories.contains(expected_dir));
        }
        assert_eq!(summary.sstables_found, 3);

        // Check coverage
        let coverage = summary.coverage.as_ref().unwrap();
        assert_eq!(coverage.total_tables, 3);
        assert_eq!(coverage.tables_with_schema, 1);
        assert_eq!(coverage.tables_missing_schema.len(), 2);

        // Should be red badge (33% coverage < 50%)
        assert_eq!(summary.badge, CoverageBadge::Red);
    }

    #[tokio::test]
    async fn test_discovery_service_skips_system_keyspaces() {
        let temp_dir = TempDir::new().unwrap();

        // Create system keyspace (should be skipped)
        let system_dir = temp_dir.path().join("system");
        fs::create_dir(&system_dir).unwrap();
        let system_table_dir = system_dir.join("local-123");
        fs::create_dir(&system_table_dir).unwrap();
        fs::write(system_table_dir.join("Data.db"), b"mock").unwrap();

        // Create user keyspace
        let user_dir = temp_dir.path().join("user_ks");
        fs::create_dir(&user_dir).unwrap();
        let user_table_dir = user_dir.join("table-456");
        fs::create_dir(&user_table_dir).unwrap();
        fs::write(user_table_dir.join("na-1-big-Data.db"), b"mock").unwrap();

        let service = DiscoveryService::new(temp_dir.path().to_path_buf(), None);
        let summary = service.scan().await.unwrap();

        assert_eq!(summary.keyspaces.len(), 1);
        assert!(summary.keyspaces.contains(&"user_ks".to_string()));
        assert!(!summary.keyspaces.iter().any(|k| k.starts_with("system")));
    }

    #[tokio::test]
    async fn test_discovery_summary_text() {
        let temp_dir = TempDir::new().unwrap();

        // Create test data
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();
        let table_dir = keyspace_dir.join("users-abc123");
        fs::create_dir(&table_dir).unwrap();
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock").unwrap();

        let service = DiscoveryService::new(temp_dir.path().to_path_buf(), Some("5.0".to_string()));
        let summary = service.scan().await.unwrap();

        let text = summary.summary_text();
        assert!(text.contains("Discovered 1 keyspaces, 1 tables, 1 SSTables"));
        assert!(text.contains("Cassandra version: 5.0"));
        assert!(text.contains("Schema coverage: Not available"));
    }

    #[tokio::test]
    async fn test_discovery_service_empty_directory() {
        let temp_dir = TempDir::new().unwrap();

        let service = DiscoveryService::new(temp_dir.path().to_path_buf(), None);
        let summary = service.scan().await.unwrap();

        assert_eq!(summary.keyspaces.len(), 0);
        assert_eq!(summary.tables.len(), 0);
        assert_eq!(summary.table_directories.len(), 0);
        assert_eq!(summary.sstables_found, 0);
        assert_eq!(summary.resolved_version, Some("unknown".to_string()));
    }

    #[tokio::test]
    async fn test_discovery_service_version_resolution_precedence() {
        let temp_dir = TempDir::new().unwrap();

        // Create metadata.yml with version
        fs::write(temp_dir.path().join("metadata.yml"), "version: 4.0\n").unwrap();

        // Version hint should take precedence
        let service = DiscoveryService::new(temp_dir.path().to_path_buf(), Some("5.0".to_string()));
        let summary = service.scan().await.unwrap();
        assert_eq!(summary.resolved_version, Some("5.0".to_string()));

        // Without hint, should use metadata.yml
        let service2 = DiscoveryService::new(temp_dir.path().to_path_buf(), None);
        let summary2 = service2.scan().await.unwrap();
        assert_eq!(summary2.resolved_version, Some("4.0".to_string()));
    }
}

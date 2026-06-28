//! Schema Aggregator for M2-CLI
//!
//! This module implements schema loading and merging from multiple sources (CQL and JSON files/directories).
//! It handles two-pass loading (UDTs first, then tables) and implements last-wins merging strategy.
//!
//! Responsibilities are split across submodules:
//! - [`json`] — JSON schema parsing/conversion (minimal + full formats).
//! - [`cql`] — CQL file parsing (`CREATE TYPE`/`CREATE TABLE`, keyspace context).
//! - this module — the shared model plus orchestration (file discovery,
//!   per-file dispatch, two-pass registry application).

mod cql;
mod json;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::schema::{TableSchema, UdtRegistry};
use crate::types::UdtTypeDef;

#[allow(unused_imports)]
use crate::schema::cql_parser;

/// Configuration for schema aggregator behavior
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// Whether to continue loading after encountering errors
    pub graceful_degradation: bool,
    /// Whether to validate UDT dependencies
    pub validate_udt_dependencies: bool,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            graceful_degradation: true,
            validate_udt_dependencies: true,
        }
    }
}

/// Schema aggregator for loading and merging schemas from multiple sources
pub struct SchemaAggregator {
    /// Schema registry for storing table schemas
    registry: Arc<RwLock<crate::schema::registry::SchemaRegistry>>,
    /// UDT registry for managing user-defined types
    udt_registry: Arc<RwLock<UdtRegistry>>,
    /// Configuration
    config: AggregatorConfig,
    /// Collected errors during loading
    errors: Vec<SchemaLoadError>,
    /// Collected warnings during loading
    warnings: Vec<SchemaLoadWarning>,
}

/// Result of schema loading operation
#[derive(Debug, Clone)]
pub struct LoadResult {
    /// Number of table schemas successfully loaded
    pub schemas_loaded: usize,
    /// Number of UDTs successfully loaded
    pub udts_loaded: usize,
    /// Errors encountered during loading
    pub errors: Vec<SchemaLoadError>,
    /// Warnings encountered during loading
    pub warnings: Vec<SchemaLoadWarning>,
}

/// Error encountered during schema loading
#[derive(Debug, Clone)]
pub struct SchemaLoadError {
    /// File path where the error occurred
    pub file_path: Option<PathBuf>,
    /// Type of error
    pub error_type: LoadErrorType,
    /// Error message
    pub message: String,
}

/// Types of schema load errors
#[derive(Debug, Clone)]
pub enum LoadErrorType {
    /// Failed to read file
    FileRead,
    /// Invalid JSON format
    InvalidJson,
    /// Invalid CQL syntax
    InvalidCql,
    /// Missing UDT dependency
    MissingUdtDependency,
    /// Circular UDT dependency
    CircularUdtDependency,
    /// Schema validation failed
    ValidationFailed,
    /// Invalid file format (neither .cql nor .json)
    InvalidFileFormat,
}

/// Warning encountered during schema loading
#[derive(Debug, Clone)]
pub struct SchemaLoadWarning {
    /// File path where the warning occurred
    pub file_path: Option<PathBuf>,
    /// Warning message
    pub message: String,
}

/// Intermediate parsed schema data before registry insertion
#[derive(Debug, Clone)]
struct ParsedSchema {
    /// Keyspace name (for context only; tables/udts are now keyed by qualified names)
    #[allow(dead_code)]
    keyspace: String,
    /// Table schemas (keyed by qualified name: "keyspace.table")
    tables: HashMap<String, TableSchema>,
    /// UDT definitions (keyed by qualified name: "keyspace.typename")
    udts: HashMap<String, UdtTypeDef>,
}

impl SchemaAggregator {
    /// Create a new schema aggregator
    pub fn new(
        registry: Arc<RwLock<crate::schema::registry::SchemaRegistry>>,
        udt_registry: Arc<RwLock<UdtRegistry>>,
        config: AggregatorConfig,
    ) -> Self {
        Self {
            registry,
            udt_registry,
            config,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Load schemas from multiple paths (files or directories)
    pub async fn load_from_paths(&mut self, paths: &[PathBuf]) -> Result<LoadResult> {
        self.errors.clear();
        self.warnings.clear();

        // Step 1: Discover all files from paths in order
        let mut all_files = Vec::new();
        for path in paths {
            if let Err(e) = self.discover_files(path, &mut all_files) {
                self.errors.push(SchemaLoadError {
                    file_path: Some(path.clone()),
                    error_type: LoadErrorType::FileRead,
                    message: format!("Failed to discover files: {}", e),
                });
            }
        }

        if all_files.is_empty() && !self.errors.is_empty() {
            return Ok(self.build_result(0, 0));
        }

        // Step 2: Parse all files into intermediate format
        let mut parsed_schemas = Vec::new();
        for file_path in &all_files {
            match self.parse_file(file_path).await {
                Ok(Some(schema)) => parsed_schemas.push(schema),
                Ok(None) => {} // Skipped file
                Err(e) => {
                    // Map error type based on the actual error variant
                    let error_type = match &e {
                        Error::Io(_) => LoadErrorType::FileRead,
                        Error::CqlParse(_) => LoadErrorType::InvalidCql,
                        Error::Schema(_) => {
                            // Schema errors are structural validation failures
                            // Check message for JSON vs general validation
                            let msg = e.to_string();
                            if msg.contains("Invalid JSON")
                                || msg.contains("JSON")
                                || msg.contains("json")
                            {
                                LoadErrorType::InvalidJson
                            } else {
                                // Missing partition_keys, bad clustering config, etc.
                                LoadErrorType::ValidationFailed
                            }
                        }
                        _ => {
                            // Fallback: check error message for clues
                            let msg = e.to_string();
                            if msg.contains("JSON") || msg.contains("json") {
                                LoadErrorType::InvalidJson
                            } else if msg.contains("CQL") || msg.contains("parse") {
                                LoadErrorType::InvalidCql
                            } else {
                                // Unknown error types default to validation failure, not I/O
                                LoadErrorType::ValidationFailed
                            }
                        }
                    };
                    self.errors.push(SchemaLoadError {
                        file_path: Some(file_path.clone()),
                        error_type,
                        message: format!("Failed to parse file: {}", e),
                    });
                    // Check graceful_degradation after parse failure
                    if !self.config.graceful_degradation {
                        return Ok(self.build_result(0, 0));
                    }
                }
            }
        }

        // Early return if parsing failed and strict mode is enabled
        if !self.config.graceful_degradation && !self.errors.is_empty() {
            return Ok(self.build_result(0, 0));
        }

        // Step 3: Two-pass loading - UDTs first, then tables
        let (udts_loaded, tables_loaded) = self.apply_schemas(parsed_schemas).await;

        Ok(self.build_result(tables_loaded, udts_loaded))
    }

    /// Discover files from a path (file or directory)
    fn discover_files(&mut self, path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        if !path.exists() {
            return Err(Error::InvalidPath(format!(
                "Path does not exist: {}",
                path.display()
            )));
        }

        if path.is_file() {
            // Single file - validate extension
            if let Some(ext) = path.extension() {
                let ext_str = ext.to_string_lossy().to_lowercase();
                if ext_str == "cql" || ext_str == "json" {
                    files.push(path.to_path_buf());
                } else {
                    self.warnings.push(SchemaLoadWarning {
                        file_path: Some(path.to_path_buf()),
                        message: format!("Skipping file with unsupported extension: {}", ext_str),
                    });
                }
            }
        } else if path.is_dir() {
            // Directory - scan recursively in lexical order
            self.scan_directory_recursive(path, files)?;
        }

        Ok(())
    }

    /// Recursively scan directory for schema files in lexical order
    #[allow(clippy::only_used_in_recursion)]
    fn scan_directory_recursive(&mut self, dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
            .map_err(Error::Io)?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .collect();

        // Sort lexically for deterministic ordering
        entries.sort();

        for entry in entries {
            if entry.is_file() {
                if let Some(ext) = entry.extension() {
                    let ext_str = ext.to_string_lossy().to_lowercase();
                    if ext_str == "cql" || ext_str == "json" {
                        files.push(entry);
                    }
                }
            } else if entry.is_dir() {
                self.scan_directory_recursive(&entry, files)?;
            }
        }

        Ok(())
    }

    /// Parse a single file into intermediate schema format
    async fn parse_file(&self, path: &Path) -> Result<Option<ParsedSchema>> {
        let ext = path
            .extension()
            .ok_or_else(|| Error::InvalidPath("File has no extension".to_string()))?;

        let ext_str = ext.to_string_lossy().to_lowercase();

        match ext_str.as_str() {
            "cql" => self.parse_cql_file(path).await,
            "json" => self.parse_json_file(path).await,
            _ => Err(Error::InvalidPath(format!(
                "Unsupported file extension: {}",
                ext_str
            ))),
        }
    }

    /// Apply parsed schemas to registries (two-pass: UDTs first, then tables)
    async fn apply_schemas(&mut self, parsed_schemas: Vec<ParsedSchema>) -> (usize, usize) {
        // Pass 1: Register all UDTs with last-wins strategy
        let mut udt_map: HashMap<String, (String, UdtTypeDef)> = HashMap::new(); // key: keyspace.udt_name -> (keyspace, UdtTypeDef)

        for parsed in &parsed_schemas {
            for (qualified_name, udt_def) in &parsed.udts {
                // qualified_name is already "keyspace.typename" from parse_cql_file
                udt_map.insert(
                    qualified_name.clone(),
                    (udt_def.keyspace.clone(), udt_def.clone()),
                );
            }
        }

        // Register UDTs in registry
        let mut udts_loaded = 0;
        {
            let mut udt_registry = self.udt_registry.write().await;
            for (_key, (_keyspace, udt_def)) in udt_map {
                if self.config.validate_udt_dependencies {
                    // Validate dependencies exist
                    if let Err(e) = udt_registry.register_udt_with_validation(udt_def.clone()) {
                        self.errors.push(SchemaLoadError {
                            file_path: None,
                            error_type: LoadErrorType::CircularUdtDependency,
                            message: format!("UDT validation failed: {}", e),
                        });
                        // Check graceful_degradation after UDT validation failure
                        if !self.config.graceful_degradation {
                            // Return early with UDTs loaded so far, skip tables
                            return (udts_loaded, 0);
                        }
                        continue;
                    }
                } else {
                    udt_registry.register_udt(udt_def);
                }
                udts_loaded += 1;
            }
        }

        // Early return after UDT phase if strict mode and errors exist
        if !self.config.graceful_degradation && !self.errors.is_empty() {
            return (udts_loaded, 0);
        }

        // Pass 2: Register all tables with last-wins strategy
        let mut table_map: HashMap<String, TableSchema> = HashMap::new();

        for parsed in &parsed_schemas {
            for (qualified_name, table_schema) in &parsed.tables {
                // qualified_name is already "keyspace.table" from parse_cql_file
                table_map.insert(qualified_name.clone(), table_schema.clone());
            }
        }

        // Register tables in registry
        let mut tables_loaded = 0;
        {
            let registry = self.registry.write().await;
            for (_key, table_schema) in table_map {
                // Fail fast on undefined UDT references (issue #761): a column
                // referencing a UDT not in the registry surfaces here with the
                // missing type named, rather than later as a confusing
                // parse/deserialization error. Gated by the same flag that
                // controls UDT dependency validation.
                if self.config.validate_udt_dependencies {
                    let udt_registry = self.udt_registry.read().await;
                    if let Err(e) = table_schema.validate_udt_references(&udt_registry) {
                        self.errors.push(SchemaLoadError {
                            file_path: None,
                            error_type: LoadErrorType::ValidationFailed,
                            message: format!(
                                "Failed to register table '{}.{}': {}",
                                table_schema.keyspace, table_schema.table, e
                            ),
                        });
                        if !self.config.graceful_degradation {
                            return (udts_loaded, tables_loaded);
                        }
                        continue;
                    }
                }
                match registry
                    .register_schema(
                        table_schema.clone(),
                        crate::schema::registry::SchemaSource::Manual,
                    )
                    .await
                {
                    Ok(_) => tables_loaded += 1,
                    Err(e) => {
                        self.errors.push(SchemaLoadError {
                            file_path: None,
                            error_type: LoadErrorType::ValidationFailed,
                            message: format!(
                                "Failed to register table '{}.{}': {}",
                                table_schema.keyspace, table_schema.table, e
                            ),
                        });
                        // Check graceful_degradation after table registration failure
                        if !self.config.graceful_degradation {
                            // Return early with counts so far
                            return (udts_loaded, tables_loaded);
                        }
                    }
                }
            }
        }

        (udts_loaded, tables_loaded)
    }

    /// Build load result from current state
    fn build_result(&self, schemas_loaded: usize, udts_loaded: usize) -> LoadResult {
        LoadResult {
            schemas_loaded,
            udts_loaded,
            errors: self.errors.clone(),
            warnings: self.warnings.clone(),
        }
    }
}

/// Shared test fixtures for aggregator submodules.
#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use crate::platform::Platform;
    use crate::schema::registry::{SchemaRegistry, SchemaRegistryConfig};
    use crate::Config;
    use std::io::Write;
    pub(crate) use tempfile::TempDir;

    pub(crate) async fn setup_test_aggregator() -> (SchemaAggregator, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let registry_config = SchemaRegistryConfig::default();
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ));
        let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

        let aggregator = SchemaAggregator::new(registry, udt_registry, AggregatorConfig::default());

        (aggregator, temp_dir)
    }

    pub(crate) fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn test_error_type_mapping_io_error() {
        let (mut aggregator, _temp_dir) = setup_test_aggregator().await;

        // Test with a non-existent file to trigger IO error
        let non_existent_path = PathBuf::from("/nonexistent/path/schema.json");
        let result = aggregator
            .load_from_paths(std::slice::from_ref(&non_existent_path))
            .await
            .unwrap();

        assert_eq!(result.schemas_loaded, 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::FileRead
        ));
        assert!(result.errors[0]
            .message
            .contains("Failed to discover files"));
    }

    #[tokio::test]
    async fn test_error_type_mapping_invalid_json() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Test with malformed JSON
        let invalid_json = r#"{"keyspace": "ks", "table": "broken", invalid}"#;
        let path = write_file(temp_dir.path(), "invalid.json", invalid_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidJson
        ));
        assert!(result.errors[0].message.contains("Failed to parse file"));
        assert!(result.errors[0].message.contains("Invalid JSON"));
    }

    #[tokio::test]
    async fn test_error_type_mapping_invalid_cql() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Test with invalid CQL syntax
        let invalid_cql = r#"
        CREATE INVALID SYNTAX HERE
        id uuid PRIMARY KEY
        "#;
        let path = write_file(temp_dir.path(), "invalid.cql", invalid_cql);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidCql
        ));
        assert!(result.errors[0].message.contains("Failed to parse file"));
    }

    #[tokio::test]
    async fn test_error_message_preservation() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Test that original error messages are preserved
        let invalid_json = r#"{"keyspace": "ks""#; // Missing closing brace
        let path = write_file(temp_dir.path(), "broken.json", invalid_json);
        let result = aggregator
            .load_from_paths(std::slice::from_ref(&path))
            .await
            .unwrap();

        assert_eq!(result.errors.len(), 1);
        // Error message should contain both "Failed to parse file" and the original error
        assert!(result.errors[0].message.contains("Failed to parse file"));
        assert!(result.errors[0].message.contains("Invalid JSON"));
        // File path should be preserved
        assert_eq!(result.errors[0].file_path, Some(path));
    }

    #[tokio::test]
    async fn test_multiple_error_types_in_batch() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Create multiple files with different error types
        let invalid_json = r#"{"invalid json"#;
        let invalid_cql = r#"INVALID CQL SYNTAX"#;

        let json_path = write_file(temp_dir.path(), "bad.json", invalid_json);
        let cql_path = write_file(temp_dir.path(), "bad.cql", invalid_cql);

        let result = aggregator
            .load_from_paths(&[json_path, cql_path])
            .await
            .unwrap();

        assert_eq!(result.schemas_loaded, 0);
        assert_eq!(result.errors.len(), 2);

        // Find the JSON and CQL errors
        let json_error = result
            .errors
            .iter()
            .find(|e| {
                e.file_path
                    .as_ref()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .ends_with(".json")
            })
            .unwrap();
        let cql_error = result
            .errors
            .iter()
            .find(|e| {
                e.file_path
                    .as_ref()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .ends_with(".cql")
            })
            .unwrap();

        // Verify correct error types
        assert!(matches!(json_error.error_type, LoadErrorType::InvalidJson));
        assert!(matches!(cql_error.error_type, LoadErrorType::InvalidCql));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_file_read_error_from_parse_file() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Create a file and make it unreadable (Unix-only test)
        let json_content =
            r#"{"keyspace": "ks", "table": "test", "columns": [], "partition_keys": ["id"]}"#;
        let path = write_file(temp_dir.path(), "unreadable.json", json_content);

        // Make file unreadable
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&path, perms).unwrap();

        // Privileged users (e.g. uid 0 in containerized CI) bypass file
        // permissions, so the read-error precondition cannot be created.
        if fs::File::open(&path).is_ok() {
            return;
        }

        let result = aggregator
            .load_from_paths(std::slice::from_ref(&path))
            .await
            .unwrap();

        // Restore permissions for cleanup
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        let _ = fs::set_permissions(&path, perms);

        // Should have an IO error
        assert_eq!(result.schemas_loaded, 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::FileRead
        ));
    }
}

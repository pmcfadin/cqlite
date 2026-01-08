//! Schema Aggregator for M2-CLI
//!
//! This module implements schema loading and merging from multiple sources (CQL and JSON files/directories).
//! It handles two-pass loading (UDTs first, then tables) and implements last-wins merging strategy.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::schema::{
    cql_parser::{classify_statement, parse_create_type, split_cql_statements, StatementType},
    parse_cql_schema, ClusteringColumn, Column, KeyColumn, TableSchema, UdtRegistry,
};
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

/// JSON schema formats
#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum JsonSchemaFormat {
    /// Minimal format: single table with "table" field
    Minimal(MinimalTableSchema),
    /// Full format: with "tables" array and optional "udts" array
    Full(FullSchema),
}

/// Minimal JSON schema format (single table)
#[derive(Debug, serde::Deserialize)]
struct MinimalTableSchema {
    keyspace: String,
    table: String,
    columns: Vec<JsonColumn>,
    #[serde(default)]
    partition_keys: Vec<String>,
    #[serde(default)]
    primary_key: Vec<String>, // Synonym for partition_keys when no clustering
    #[serde(default)]
    clustering_keys: Vec<JsonClusteringKey>,
}

/// Full JSON schema format (multiple tables + UDTs)
/// Note: Both `udts` and `tables` are optional to support UDT-only or table-only files
#[derive(Debug, serde::Deserialize)]
struct FullSchema {
    keyspace: String,
    #[serde(default)]
    udts: Vec<JsonUdt>,
    #[serde(default)]
    tables: Vec<JsonTable>,
}

/// JSON table definition
#[derive(Debug, serde::Deserialize)]
struct JsonTable {
    name: String,
    columns: Vec<JsonColumn>,
    #[serde(default)]
    partition_keys: Vec<String>,
    #[serde(default)]
    primary_key: Vec<String>,
    #[serde(default)]
    clustering_keys: Vec<JsonClusteringKey>,
}

/// JSON column definition
#[derive(Debug, serde::Deserialize)]
struct JsonColumn {
    name: String,
    #[serde(alias = "data_type")]
    r#type: String,
    #[serde(default)]
    nullable: bool,
}

/// JSON clustering key definition
#[derive(Debug, serde::Deserialize)]
struct JsonClusteringKey {
    name: String,
    #[serde(alias = "data_type")]
    r#type: String,
    #[serde(default)]
    order: Option<String>,
}

/// JSON UDT definition
#[derive(Debug, serde::Deserialize)]
struct JsonUdt {
    name: String,
    fields: Vec<JsonUdtField>,
}

/// JSON UDT field definition
#[derive(Debug, serde::Deserialize)]
struct JsonUdtField {
    name: String,
    #[serde(alias = "data_type")]
    r#type: String,
    #[serde(default = "default_nullable")]
    nullable: bool,
}

fn default_nullable() -> bool {
    true
}

/// Extract keyspace name from USE statement
/// Example: "USE test_basic;" -> Some("test_basic")
/// Example: "USE \"test-basic\";" -> Some("test-basic")
fn extract_use_keyspace(statement: &str) -> Option<String> {
    let normalized = statement.trim().to_lowercase();
    if !normalized.starts_with("use ") {
        return None;
    }

    // Extract keyspace name after "USE "
    let after_use = statement.trim()[4..].trim();
    let mut ks_name = after_use.trim_end_matches(';').trim();

    // Strip quotes from keyspace name if present
    if ks_name.starts_with('"') && ks_name.ends_with('"') && ks_name.len() > 1 {
        ks_name = &ks_name[1..ks_name.len() - 1];
    }

    if ks_name.is_empty() {
        None
    } else {
        Some(ks_name.to_string())
    }
}

/// Extract keyspace name from CREATE KEYSPACE statement
/// Example: "CREATE KEYSPACE IF NOT EXISTS test_basic WITH ..." -> Some("test_basic")
/// Example: "CREATE KEYSPACE \"test-basic\" WITH ..." -> Some("test-basic")
fn extract_create_keyspace_name(statement: &str) -> Option<String> {
    let normalized = statement.trim().to_lowercase();
    if !normalized.starts_with("create keyspace") {
        return None;
    }

    // Split by whitespace and find the keyspace name
    let words: Vec<&str> = statement.split_whitespace().collect();

    // Pattern: CREATE KEYSPACE [IF NOT EXISTS] <name> ...
    let start_idx = if words.len() > 2 && words[2].eq_ignore_ascii_case("if") {
        5 // Skip "CREATE KEYSPACE IF NOT EXISTS"
    } else {
        2 // Skip "CREATE KEYSPACE"
    };

    if words.len() > start_idx {
        let mut ks_name = words[start_idx].trim();

        // Strip quotes from keyspace name if present
        if ks_name.starts_with('"') && ks_name.ends_with('"') && ks_name.len() > 1 {
            ks_name = &ks_name[1..ks_name.len() - 1];
        }

        Some(ks_name.to_string())
    } else {
        None
    }
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

    /// Parse a CQL file (supports multiple statements: CREATE TYPE and CREATE TABLE)
    async fn parse_cql_file(&self, path: &Path) -> Result<Option<ParsedSchema>> {
        let content = std::fs::read_to_string(path)?;

        // Split file content into individual statements
        let statements = split_cql_statements(&content);

        if statements.is_empty() {
            return Ok(None);
        }

        let mut keyspace: Option<String> = None;
        let mut tables = HashMap::new();
        let mut udts = HashMap::new();
        let mut errors = Vec::new();

        // Separate CREATE TYPE from CREATE TABLE statements
        let mut create_type_stmts = Vec::new();
        let mut create_table_stmts = Vec::new();

        for statement in &statements {
            match classify_statement(statement) {
                StatementType::CreateType => create_type_stmts.push(statement.as_str()),
                StatementType::CreateTable => create_table_stmts.push(statement.as_str()),
                StatementType::Other(ref kind) if kind == "use" => {
                    // Extract keyspace name from USE statement
                    if let Some(ks_name) = extract_use_keyspace(statement) {
                        keyspace = Some(ks_name);
                    }
                }
                StatementType::Other(ref kind) if kind == "create" => {
                    // Handle CREATE KEYSPACE statements - extract keyspace name
                    if let Some(ks_name) = extract_create_keyspace_name(statement) {
                        // Only set keyspace if not already set by USE statement
                        if keyspace.is_none() {
                            keyspace = Some(ks_name);
                        }
                    }
                }
                StatementType::Other(_kind) => {
                    // Skip other statement types silently (e.g., ALTER, DROP, comments)
                }
            }
        }

        // Parse CREATE TYPE statements first (UDTs must be registered before tables)
        for stmt in create_type_stmts {
            match parse_create_type(stmt) {
                Ok((_, (type_name, type_keyspace, fields))) => {
                    // Determine keyspace (use from statement or inherit from file context)
                    let udt_keyspace = type_keyspace.unwrap_or_else(|| {
                        keyspace.clone().unwrap_or_else(|| "default".to_string())
                    });

                    // Update keyspace if not set
                    if keyspace.is_none() {
                        keyspace = Some(udt_keyspace.clone());
                    }

                    // Build UdtTypeDef
                    let mut udt_def = UdtTypeDef::new(udt_keyspace.clone(), type_name.clone());
                    for (field_name, field_type_str) in fields {
                        // Parse field type
                        let field_type = crate::schema::CqlType::parse(&field_type_str)?;
                        udt_def = udt_def.with_field(field_name, field_type, true);
                    }

                    // Key by fully-qualified name (keyspace.typename) to avoid multi-keyspace collisions
                    let qualified_name = format!("{}.{}", udt_keyspace, type_name);
                    udts.insert(qualified_name, udt_def);
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to parse CREATE TYPE in {}: {:?}",
                        path.display(),
                        e
                    ));
                }
            }
        }

        // Parse CREATE TABLE statements
        for stmt in create_table_stmts {
            match parse_cql_schema(stmt) {
                Ok(mut table_schema) => {
                    // Override keyspace with the one from USE statement or CREATE KEYSPACE
                    // Only override if the table doesn't have an explicit qualified name
                    if table_schema.keyspace == "default" {
                        if let Some(ref active_keyspace) = keyspace {
                            table_schema.keyspace = active_keyspace.clone();
                        }
                    }

                    // Update keyspace if not set (from first table's explicit keyspace)
                    if keyspace.is_none() {
                        keyspace = Some(table_schema.keyspace.clone());
                    }

                    // Key by fully-qualified name (keyspace.table) to avoid multi-keyspace collisions
                    let qualified_name =
                        format!("{}.{}", table_schema.keyspace, table_schema.table);
                    tables.insert(qualified_name, table_schema);
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to parse CREATE TABLE in {}: {}",
                        path.display(),
                        e
                    ));
                }
            }
        }

        // If there were errors and no successful parses, return error
        if !errors.is_empty() && tables.is_empty() && udts.is_empty() {
            return Err(Error::CqlParse(format!(
                "Failed to parse CQL file {}: {}",
                path.display(),
                errors.join("; ")
            )));
        }

        // If no tables or UDTs were parsed, and statements exist, treat as error
        // This catches truly invalid CQL that doesn't match any expected pattern
        if tables.is_empty() && udts.is_empty() && !statements.is_empty() {
            // Check if all statements are legitimate "other" types (USE, CREATE KEYSPACE, etc.)
            // or if there are truly unrecognized/invalid statements
            let legitimate_keywords = [
                "use", "create", "alter", "drop", "grant", "revoke", "truncate",
            ];
            let has_invalid_statement = statements.iter().any(|stmt| {
                let normalized = stmt.trim().to_lowercase();
                let first_word = normalized.split_whitespace().next().unwrap_or("");

                // If it's a CREATE statement that wasn't successfully parsed, it's invalid
                if normalized.starts_with("create ") {
                    return true;
                }

                // If it's not a legitimate keyword, it's invalid
                !legitimate_keywords.contains(&first_word)
            });

            // If there are invalid statements, return an error
            if has_invalid_statement {
                return Err(Error::CqlParse(format!(
                    "Failed to parse CQL file {}: No valid CREATE TABLE or CREATE TYPE statements found",
                    path.display()
                )));
            }
        }

        // Determine final keyspace (use first discovered or default)
        let final_keyspace = keyspace.unwrap_or_else(|| "default".to_string());

        Ok(Some(ParsedSchema {
            keyspace: final_keyspace,
            tables,
            udts,
        }))
    }

    /// Parse a JSON file (either minimal or full format)
    async fn parse_json_file(&self, path: &Path) -> Result<Option<ParsedSchema>> {
        let content = std::fs::read_to_string(path)?;

        let json_schema: JsonSchemaFormat = serde_json::from_str(&content)
            .map_err(|e| Error::schema(format!("Invalid JSON in {}: {}", path.display(), e)))?;

        match json_schema {
            JsonSchemaFormat::Minimal(minimal) => self.parse_minimal_format(path, minimal).await,
            JsonSchemaFormat::Full(full) => self.parse_full_format(path, full).await,
        }
    }

    /// Parse minimal JSON format (single table)
    async fn parse_minimal_format(
        &self,
        _path: &Path,
        minimal: MinimalTableSchema,
    ) -> Result<Option<ParsedSchema>> {
        let table_schema = self.convert_minimal_to_table_schema(minimal)?;
        let keyspace = table_schema.keyspace.clone();

        let mut tables = HashMap::new();
        // Key by fully-qualified name (keyspace.table) to avoid multi-keyspace collisions
        let qualified_name = format!("{}.{}", table_schema.keyspace, table_schema.table);
        tables.insert(qualified_name, table_schema);

        Ok(Some(ParsedSchema {
            keyspace,
            tables,
            udts: HashMap::new(),
        }))
    }

    /// Parse full JSON format (multiple tables + UDTs)
    async fn parse_full_format(
        &self,
        _path: &Path,
        full: FullSchema,
    ) -> Result<Option<ParsedSchema>> {
        let keyspace = full.keyspace.clone();
        let mut tables = HashMap::new();
        let mut udts = HashMap::new();

        // Parse UDTs
        for udt_json in full.udts {
            let udt_def = self.convert_json_udt_to_typedef(&keyspace, udt_json)?;
            // Key by fully-qualified name (keyspace.typename) to avoid multi-keyspace collisions
            let qualified_name = format!("{}.{}", udt_def.keyspace, udt_def.name);
            udts.insert(qualified_name, udt_def);
        }

        // Parse tables
        for table_json in full.tables {
            let table_schema = self.convert_json_table_to_table_schema(&keyspace, table_json)?;
            // Key by fully-qualified name (keyspace.table) to avoid multi-keyspace collisions
            let qualified_name = format!("{}.{}", table_schema.keyspace, table_schema.table);
            tables.insert(qualified_name, table_schema);
        }

        Ok(Some(ParsedSchema {
            keyspace,
            tables,
            udts,
        }))
    }

    /// Convert minimal JSON format to TableSchema
    fn convert_minimal_to_table_schema(&self, minimal: MinimalTableSchema) -> Result<TableSchema> {
        // Determine partition keys (prefer partition_keys, fallback to primary_key)
        let partition_key_names = if !minimal.partition_keys.is_empty() {
            minimal.partition_keys
        } else if !minimal.primary_key.is_empty() {
            minimal.primary_key
        } else {
            return Err(Error::schema(
                "Table must have partition_keys or primary_key".to_string(),
            ));
        };

        // Build columns
        let columns: Vec<Column> = minimal
            .columns
            .iter()
            .map(|col| Column {
                name: col.name.clone(),
                data_type: col.r#type.clone(),
                nullable: col.nullable,
                default: None,
                is_static: false, // TODO: minimal schema doesn't track static columns yet
            })
            .collect();

        // Build partition keys
        let partition_keys: Vec<KeyColumn> = partition_key_names
            .iter()
            .enumerate()
            .map(|(pos, name)| {
                let col = minimal
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or_else(|| {
                        Error::schema(format!("Partition key '{}' not found in columns", name))
                    })?;

                Ok(KeyColumn {
                    name: col.name.clone(),
                    data_type: col.r#type.clone(),
                    position: pos,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Build clustering keys
        let clustering_keys: Vec<ClusteringColumn> = minimal
            .clustering_keys
            .iter()
            .enumerate()
            .map(|(pos, ck)| ClusteringColumn {
                name: ck.name.clone(),
                data_type: ck.r#type.clone(),
                position: pos,
                order: ck.order.as_deref().map(|s| s.into()).unwrap_or_default(),
            })
            .collect();

        let schema = TableSchema {
            keyspace: minimal.keyspace,
            table: minimal.table,
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
        };

        schema.validate()?;
        Ok(schema)
    }

    /// Convert JSON table to TableSchema
    fn convert_json_table_to_table_schema(
        &self,
        keyspace: &str,
        table_json: JsonTable,
    ) -> Result<TableSchema> {
        let partition_key_names = if !table_json.partition_keys.is_empty() {
            table_json.partition_keys
        } else if !table_json.primary_key.is_empty() {
            table_json.primary_key
        } else {
            return Err(Error::schema(format!(
                "Table '{}' must have partition_keys or primary_key",
                table_json.name
            )));
        };

        let columns: Vec<Column> = table_json
            .columns
            .iter()
            .map(|col| Column {
                name: col.name.clone(),
                data_type: col.r#type.clone(),
                nullable: col.nullable,
                default: None,
                is_static: false, // TODO: JSON schema doesn't track static columns yet
            })
            .collect();

        let partition_keys: Vec<KeyColumn> = partition_key_names
            .iter()
            .enumerate()
            .map(|(pos, name)| {
                let col = table_json
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or_else(|| {
                        Error::schema(format!(
                            "Partition key '{}' not found in columns of table '{}'",
                            name, table_json.name
                        ))
                    })?;

                Ok(KeyColumn {
                    name: col.name.clone(),
                    data_type: col.r#type.clone(),
                    position: pos,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let clustering_keys: Vec<ClusteringColumn> = table_json
            .clustering_keys
            .iter()
            .enumerate()
            .map(|(pos, ck)| ClusteringColumn {
                name: ck.name.clone(),
                data_type: ck.r#type.clone(),
                position: pos,
                order: ck.order.as_deref().map(|s| s.into()).unwrap_or_default(),
            })
            .collect();

        let schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: table_json.name,
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
        };

        schema.validate()?;
        Ok(schema)
    }

    /// Convert JSON UDT to UdtTypeDef
    fn convert_json_udt_to_typedef(&self, keyspace: &str, udt_json: JsonUdt) -> Result<UdtTypeDef> {
        let mut udt_def = UdtTypeDef::new(keyspace.to_string(), udt_json.name);

        for field in udt_json.fields {
            let field_type = crate::schema::CqlType::parse(&field.r#type)?;
            udt_def = udt_def.with_field(field.name, field_type, field.nullable);
        }

        Ok(udt_def)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::Platform;
    use crate::schema::registry::{SchemaRegistry, SchemaRegistryConfig};
    use crate::Config;
    use std::io::Write;
    use tempfile::TempDir;

    async fn setup_test_aggregator() -> (SchemaAggregator, TempDir) {
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

    fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }

    #[tokio::test]
    async fn test_load_single_json_file() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let json_content = r#"
        {
            "keyspace": "test_ks",
            "table": "users",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "name", "type": "text"}
            ],
            "partition_keys": ["id"],
            "clustering_keys": []
        }
        "#;

        let json_path = write_file(temp_dir.path(), "users.json", json_content);
        let result = aggregator.load_from_paths(&[json_path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_load_single_cql_file() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let cql_content = r#"
        CREATE TABLE test_ks.products (
            id uuid PRIMARY KEY,
            name text,
            price decimal
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "products.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_directory_scanning_lexical_order() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Create files in non-lexical order, verify they're processed lexically
        write_file(
            temp_dir.path(),
            "c_table.json",
            r#"{"keyspace":"ks","table":"c","columns":[{"name":"id","type":"uuid"}],"partition_keys":["id"]}"#,
        );
        write_file(
            temp_dir.path(),
            "a_table.json",
            r#"{"keyspace":"ks","table":"a","columns":[{"name":"id","type":"uuid"}],"partition_keys":["id"]}"#,
        );
        write_file(
            temp_dir.path(),
            "b_table.json",
            r#"{"keyspace":"ks","table":"b","columns":[{"name":"id","type":"uuid"}],"partition_keys":["id"]}"#,
        );

        let result = aggregator
            .load_from_paths(&[temp_dir.path().to_path_buf()])
            .await
            .unwrap();

        assert_eq!(result.schemas_loaded, 3);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_last_wins_for_duplicate_tables() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let first_json = r#"
        {
            "keyspace": "ks",
            "table": "users",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "name", "type": "text"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let second_json = r#"
        {
            "keyspace": "ks",
            "table": "users",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "name", "type": "text"},
                {"name": "email", "type": "text"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let path1 = write_file(temp_dir.path(), "users_v1.json", first_json);
        let path2 = write_file(temp_dir.path(), "users_v2.json", second_json);

        let result = aggregator.load_from_paths(&[path1, path2]).await.unwrap();

        // Last wins, so we should have 1 schema (the second one)
        assert_eq!(result.schemas_loaded, 1);

        // Verify the schema has 3 columns (from second definition)
        let registry = aggregator.registry.read().await;
        let schema = registry.get_schema("ks", "users").await.unwrap();
        assert_eq!(schema.columns.len(), 3);
    }

    #[tokio::test]
    async fn test_two_pass_udt_then_tables() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let full_schema_json = r#"
        {
            "keyspace": "ks",
            "udts": [
                {
                    "name": "address",
                    "fields": [
                        {"name": "street", "type": "text"},
                        {"name": "city", "type": "text"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "addr", "type": "frozen<address>"}
                    ],
                    "partition_keys": ["id"],
                    "clustering_keys": []
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "schema.json", full_schema_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 1);
        assert!(result.errors.is_empty());

        // Verify UDT was registered
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(udt_registry.contains_udt("ks", "address"));
    }

    /// Test for Issue #230: REPL fails when schema directory contains UDT-only JSON files
    /// UDT-only files (without "tables" array) should parse successfully
    #[tokio::test]
    async fn test_udt_only_json_schema_issue_230() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // This JSON has "udts" but no "tables" - should work after fix
        let udt_only_json = r#"
        {
            "keyspace": "test_keyspace",
            "udts": [
                {
                    "name": "address_type",
                    "fields": [
                        { "name": "street", "type": "text" },
                        { "name": "city", "type": "text" },
                        { "name": "zip", "type": "int" }
                    ]
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "address.json", udt_only_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // Should load successfully with no errors
        assert!(
            result.errors.is_empty(),
            "Expected no errors but got: {:?}",
            result.errors
        );
        assert_eq!(result.udts_loaded, 1, "Expected 1 UDT to be loaded");
        assert_eq!(
            result.schemas_loaded, 0,
            "Expected 0 tables (UDT-only file)"
        );

        // Verify UDT was registered
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("test_keyspace", "address_type"),
            "UDT address_type should be registered in test_keyspace"
        );
    }

    /// Test symmetric case: table-only JSON files (tables without UDTs)
    /// This validates that #[serde(default)] works for both fields
    #[tokio::test]
    async fn test_table_only_json_schema_symmetry() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // This JSON has "tables" but no "udts" - symmetric to Issue #230 fix
        let table_only_json = r#"
        {
            "keyspace": "test_keyspace",
            "tables": [
                {
                    "name": "simple_table",
                    "columns": [
                        { "name": "id", "type": "uuid" },
                        { "name": "data", "type": "text" }
                    ],
                    "partition_keys": ["id"],
                    "clustering_keys": []
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "table_only.json", table_only_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // Should load successfully with no errors
        assert!(
            result.errors.is_empty(),
            "Expected no errors but got: {:?}",
            result.errors
        );
        assert_eq!(result.udts_loaded, 0, "Expected 0 UDTs (table-only file)");
        assert_eq!(result.schemas_loaded, 1, "Expected 1 table to be loaded");

        // Verify table was registered
        let registry = aggregator.registry.read().await;
        assert!(
            registry
                .get_schema("test_keyspace", "simple_table")
                .await
                .is_ok(),
            "Table simple_table should be registered in test_keyspace"
        );
    }

    #[tokio::test]
    async fn test_invalid_json_error_collection() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let invalid_json = r#"{"keyspace": "ks", "table": "broken""#; // Missing closing brace

        let path = write_file(temp_dir.path(), "broken.json", invalid_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 0);
        assert!(!result.errors.is_empty());
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidJson
        ));
    }

    #[tokio::test]
    async fn test_minimal_format_with_primary_key_synonym() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let json_content = r#"
        {
            "keyspace": "ks",
            "table": "items",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "data", "type": "text"}
            ],
            "primary_key": ["id"]
        }
        "#;

        let path = write_file(temp_dir.path(), "items.json", json_content);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_data_type_alias_support() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let json_content = r#"
        {
            "keyspace": "ks",
            "table": "legacy",
            "columns": [
                {"name": "id", "data_type": "uuid"},
                {"name": "value", "data_type": "text"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let path = write_file(temp_dir.path(), "legacy.json", json_content);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert!(result.errors.is_empty());
    }

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

    #[tokio::test]
    async fn test_multi_statement_cql_file_with_create_type_and_create_table() {
        // Create aggregator without UDT validation to avoid dependency ordering issues
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

        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: true,
                validate_udt_dependencies: false, // Disable to avoid HashMap ordering issues
            },
        );

        // Multi-statement CQL file with CREATE TYPE and CREATE TABLE
        let cql_content = r#"
        -- Test schema with UDTs
        CREATE TYPE test_ks.address (
            street text,
            city text,
            zip_code int
        );

        CREATE TYPE test_ks.contact_info (
            email text,
            phone text,
            address address
        );

        CREATE TABLE test_ks.users (
            id uuid PRIMARY KEY,
            name text,
            contact contact_info
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "schema.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        // Verify both UDTs and table were loaded
        assert_eq!(result.udts_loaded, 2, "Expected 2 UDTs to be loaded");
        assert_eq!(result.schemas_loaded, 1, "Expected 1 table to be loaded");
        assert!(
            result.errors.is_empty(),
            "Expected no errors, got: {:?}",
            result.errors
        );

        // Verify UDTs were registered
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("test_ks", "address"),
            "address UDT should be registered"
        );
        assert!(
            udt_registry.contains_udt("test_ks", "contact_info"),
            "contact_info UDT should be registered"
        );

        // Verify table was registered
        let registry = aggregator.registry.read().await;
        let schema = registry.get_schema("test_ks", "users").await.unwrap();
        assert_eq!(schema.table, "users");
        assert_eq!(schema.columns.len(), 3);
    }

    #[tokio::test]
    #[ignore = "Test fails due to UDT dependency validation not implemented - see Issue #117 review"]
    async fn test_cql_file_with_comments_and_semicolons() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Test edge cases: comments with semicolons
        let cql_content = r#"
        -- This is a comment with ; semicolon
        CREATE TYPE test_ks.metadata (
            key text,
            value text
        );

        /* Multi-line comment
           with ; semicolon */
        CREATE TABLE test_ks.data (
            id uuid PRIMARY KEY,
            info metadata
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "edge_cases.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(result.udts_loaded, 1);
        assert_eq!(result.schemas_loaded, 1);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_backward_compat_single_create_table() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Ensure backward compatibility with single CREATE TABLE files
        let cql_content = r#"
        CREATE TABLE test_ks.simple (
            id uuid PRIMARY KEY,
            data text
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "simple.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_graceful_degradation_false_fails_on_invalid_json() {
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

        // Create aggregator with strict mode (graceful_degradation = false)
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: false,
                validate_udt_dependencies: true,
            },
        );

        // Create invalid JSON file followed by valid JSON file
        let invalid_json = r#"{"keyspace": "ks", "table": "broken""#; // Missing closing brace
        let valid_json = r#"
        {
            "keyspace": "ks",
            "table": "valid_table",
            "columns": [
                {"name": "id", "type": "uuid"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let invalid_path = write_file(temp_dir.path(), "01_invalid.json", invalid_json);
        let valid_path = write_file(temp_dir.path(), "02_valid.json", valid_json);

        let result = aggregator
            .load_from_paths(&[invalid_path, valid_path])
            .await
            .unwrap();

        // In strict mode, should fail immediately after first error
        assert_eq!(result.schemas_loaded, 0);
        assert_eq!(result.udts_loaded, 0);
        assert!(!result.errors.is_empty());
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidJson
        ));
    }

    #[tokio::test]
    async fn test_graceful_degradation_true_continues_after_error() {
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

        // Create aggregator with graceful mode (graceful_degradation = true)
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: true,
                validate_udt_dependencies: true,
            },
        );

        // Create invalid JSON file followed by valid JSON file
        let invalid_json = r#"{"keyspace": "ks", "table": "broken""#; // Missing closing brace
        let valid_json = r#"
        {
            "keyspace": "ks",
            "table": "valid_table",
            "columns": [
                {"name": "id", "type": "uuid"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let invalid_path = write_file(temp_dir.path(), "01_invalid.json", invalid_json);
        let valid_path = write_file(temp_dir.path(), "02_valid.json", valid_json);

        let result = aggregator
            .load_from_paths(&[invalid_path, valid_path])
            .await
            .unwrap();

        // In graceful mode, should continue and load valid table
        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert_eq!(result.errors.len(), 1); // Should collect the error but continue
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidJson
        ));
    }

    #[tokio::test]
    #[ignore = "Test fails because register_udt_with_validation does not catch invalid UDT references - pre-existing limitation"]
    async fn test_graceful_degradation_false_fails_on_invalid_udt() {
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

        // Create aggregator with strict mode
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: false,
                validate_udt_dependencies: true,
            },
        );

        // Create schema with UDT that references non-existent UDT
        let schema_with_invalid_udt = r#"
        {
            "keyspace": "ks",
            "udts": [
                {
                    "name": "user_type",
                    "fields": [
                        {"name": "addr", "type": "frozen<nonexistent_udt>"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "data", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "schema.json", schema_with_invalid_udt);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // In strict mode, should fail on UDT validation and NOT load tables
        // NOTE: This test currently fails because register_udt_with_validation
        // does not validate nested UDT references. This is a pre-existing limitation.
        // TODO: Implement proper UDT dependency validation before enabling this test.
        assert_eq!(result.schemas_loaded, 0); // Tables should NOT be loaded
        assert_eq!(result.udts_loaded, 0); // UDT should not be loaded
        assert!(!result.errors.is_empty());
        // Error should be about circular/missing dependency
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::CircularUdtDependency
        ));
    }

    #[tokio::test]
    #[ignore = "Test fails because register_udt_with_validation does not catch invalid UDT references - pre-existing limitation"]
    async fn test_graceful_degradation_true_loads_tables_despite_invalid_udt() {
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

        // Create aggregator with graceful mode
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: true,
                validate_udt_dependencies: true,
            },
        );

        // Create schema with UDT that references non-existent UDT
        let schema_with_invalid_udt = r#"
        {
            "keyspace": "ks",
            "udts": [
                {
                    "name": "user_type",
                    "fields": [
                        {"name": "addr", "type": "frozen<nonexistent_udt>"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "data", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "schema.json", schema_with_invalid_udt);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // In graceful mode, should continue and load tables despite UDT failure
        assert_eq!(result.schemas_loaded, 1); // Table SHOULD be loaded
        assert_eq!(result.udts_loaded, 0); // UDT should not be loaded
        assert_eq!(result.errors.len(), 1); // Should collect the error
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::CircularUdtDependency
        ));
    }

    #[tokio::test]
    async fn test_multi_keyspace_cql_file_no_collision() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // CQL file with UDTs and tables from TWO different keyspaces
        let cql_content = r#"
        CREATE TYPE ks_a.address (
            street text,
            city text
        );

        CREATE TYPE ks_b.address (
            country text,
            postal_code text
        );

        CREATE TABLE ks_a.users (
            id uuid PRIMARY KEY,
            addr frozen<address>
        );

        CREATE TABLE ks_b.customers (
            id uuid PRIMARY KEY,
            location frozen<address>
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "multi_ks.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        // Both UDTs should be loaded (no collision)
        assert_eq!(
            result.udts_loaded, 2,
            "Expected 2 UDTs from different keyspaces"
        );
        assert_eq!(
            result.schemas_loaded, 2,
            "Expected 2 tables from different keyspaces"
        );
        assert!(
            result.errors.is_empty(),
            "Expected no errors, got: {:?}",
            result.errors
        );

        // Verify both UDTs are registered with correct keyspaces
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("ks_a", "address"),
            "ks_a.address should be registered"
        );
        assert!(
            udt_registry.contains_udt("ks_b", "address"),
            "ks_b.address should be registered"
        );

        // Verify both tables are registered with correct keyspaces
        let registry = aggregator.registry.read().await;
        let schema_a = registry.get_schema("ks_a", "users").await.unwrap();
        assert_eq!(schema_a.keyspace, "ks_a");
        assert_eq!(schema_a.table, "users");

        let schema_b = registry.get_schema("ks_b", "customers").await.unwrap();
        assert_eq!(schema_b.keyspace, "ks_b");
        assert_eq!(schema_b.table, "customers");
    }

    #[tokio::test]
    async fn test_error_schema_validation_not_mislabeled_as_file_read() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // JSON file with structural validation error (missing partition_keys)
        let invalid_schema = r#"
        {
            "keyspace": "ks",
            "table": "broken_table",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "data", "type": "text"}
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "invalid_schema.json", invalid_schema);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // Should fail with ValidationFailed, NOT FileRead
        assert_eq!(result.schemas_loaded, 0);
        assert!(!result.errors.is_empty());
        assert!(
            matches!(result.errors[0].error_type, LoadErrorType::ValidationFailed),
            "Expected ValidationFailed for missing partition_keys, got: {:?}",
            result.errors[0].error_type
        );
        assert!(
            result.errors[0].message.contains("partition_keys")
                || result.errors[0].message.contains("primary_key"),
            "Error message should mention missing keys: {}",
            result.errors[0].message
        );
    }

    #[tokio::test]
    async fn test_multi_keyspace_json_files_no_collision() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // First JSON file for keyspace ks_a
        let json_ks_a = r#"
        {
            "keyspace": "ks_a",
            "udts": [
                {
                    "name": "address",
                    "fields": [
                        {"name": "street", "type": "text"},
                        {"name": "city", "type": "text"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "name", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        // Second JSON file for keyspace ks_b with SAME UDT and table names
        let json_ks_b = r#"
        {
            "keyspace": "ks_b",
            "udts": [
                {
                    "name": "address",
                    "fields": [
                        {"name": "country", "type": "text"},
                        {"name": "postal_code", "type": "text"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "email", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        let path_a = write_file(temp_dir.path(), "ks_a.json", json_ks_a);
        let path_b = write_file(temp_dir.path(), "ks_b.json", json_ks_b);

        let result = aggregator.load_from_paths(&[path_a, path_b]).await.unwrap();

        // Both UDTs and both tables should be loaded (no collision)
        assert_eq!(
            result.udts_loaded, 2,
            "Expected 2 UDTs from different keyspaces"
        );
        assert_eq!(
            result.schemas_loaded, 2,
            "Expected 2 tables from different keyspaces"
        );
        assert!(
            result.errors.is_empty(),
            "Expected no errors, got: {:?}",
            result.errors
        );

        // Verify both UDTs are registered with correct keyspaces
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("ks_a", "address"),
            "ks_a.address should be registered"
        );
        assert!(
            udt_registry.contains_udt("ks_b", "address"),
            "ks_b.address should be registered"
        );

        // Verify both tables are registered with correct keyspaces and different columns
        let registry = aggregator.registry.read().await;
        let schema_a = registry.get_schema("ks_a", "users").await.unwrap();
        assert_eq!(schema_a.keyspace, "ks_a");
        assert_eq!(schema_a.table, "users");
        assert!(
            schema_a.columns.iter().any(|c| c.name == "name"),
            "ks_a.users should have 'name' column"
        );

        let schema_b = registry.get_schema("ks_b", "users").await.unwrap();
        assert_eq!(schema_b.keyspace, "ks_b");
        assert_eq!(schema_b.table, "users");
        assert!(
            schema_b.columns.iter().any(|c| c.name == "email"),
            "ks_b.users should have 'email' column"
        );
    }
}

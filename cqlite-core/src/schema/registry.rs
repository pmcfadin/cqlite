//! Schema Registry for Centralized Schema Management
//!
//! This module provides a centralized registry for managing table schemas, UDTs,
//! and other schema-related information with support for schema discovery,
//! validation, caching, and version management.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    Config, Error, Result,
    platform::Platform,
    schema::{
        CqlType, TableSchema, UdtRegistry,
        discovery::{SchemaDiscoveryConfig, SchemaDiscoveryEngine, SchemaInfo},
    },
    types::{ComparatorType, UdtTypeDef},
};

/// Configuration for schema registry
#[derive(Debug, Clone)]
pub struct SchemaRegistryConfig {
    /// Enable automatic schema discovery
    pub enable_auto_discovery: bool,
    /// Enable schema caching
    pub enable_caching: bool,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Enable schema versioning
    pub enable_versioning: bool,
    /// Maximum versions to keep per schema
    pub max_versions_per_schema: usize,
    /// Enable schema validation
    pub enable_validation: bool,
    /// Auto-refresh schemas on SSTable changes
    pub auto_refresh_on_changes: bool,
    /// Discovery configuration
    pub discovery_config: SchemaDiscoveryConfig,
}

impl Default for SchemaRegistryConfig {
    fn default() -> Self {
        Self {
            enable_auto_discovery: true,
            enable_caching: true,
            cache_ttl_seconds: 3600, // 1 hour
            enable_versioning: true,
            max_versions_per_schema: 5,
            enable_validation: true,
            auto_refresh_on_changes: false, // Disabled by default for performance
            discovery_config: SchemaDiscoveryConfig::default(),
        }
    }
}

/// Centralized schema registry
pub struct SchemaRegistry {
    /// Configuration
    config: SchemaRegistryConfig,
    /// Platform abstraction
    _platform: Arc<Platform>,
    /// Core configuration
    _core_config: Config,
    /// Registered table schemas by keyspace.table
    schemas: Arc<RwLock<HashMap<String, SchemaEntry>>>,
    /// UDT registry for managing user-defined types
    udt_registry: Arc<RwLock<UdtRegistry>>,
    /// Schema discovery engine
    discovery_engine: Arc<SchemaDiscoveryEngine>,
    /// Schema validator
    validator: Arc<SchemaValidator>,
    /// Schema version history
    version_history: Arc<RwLock<HashMap<String, Vec<SchemaVersion>>>>,
}

/// Schema entry in the registry
#[derive(Debug, Clone)]
struct SchemaEntry {
    /// The table schema
    schema: TableSchema,
    /// Extended schema information if available
    extended_info: Option<SchemaInfo>,
    /// When the schema was registered/updated
    registered_at: SystemTime,
    /// Source of the schema
    source: SchemaSource,
    /// Validation status
    validation_status: SchemaValidationStatus,
    /// Associated SSTable files
    _associated_files: Vec<PathBuf>,
}

/// Source of schema information
#[derive(Debug, Clone)]
pub enum SchemaSource {
    /// Discovered from SSTable files
    Discovered(Vec<PathBuf>),
    /// Loaded from external definition
    External(PathBuf),
    /// Parsed from CQL DDL
    Cql(String),
    /// Manually registered
    Manual,
}

/// Schema validation status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaValidationStatus {
    /// Schema is valid
    Valid,
    /// Schema has warnings but is usable
    ValidWithWarnings,
    /// Schema is invalid
    Invalid,
    /// Not yet validated
    NotValidated,
}

/// Schema version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Version number
    pub version: u32,
    /// When this version was created
    pub created_at: SystemTime,
    /// Schema at this version
    pub schema: TableSchema,
    /// Changes from previous version
    pub changes: Vec<SchemaChange>,
    /// Source of this version
    pub source: String,
}

/// Schema change description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    /// Type of change
    pub change_type: SchemaChangeType,
    /// Component affected
    pub component: String,
    /// Description of the change
    pub description: String,
    /// Old value (if applicable)
    pub old_value: Option<String>,
    /// New value (if applicable)
    pub new_value: Option<String>,
}

/// Types of schema changes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchemaChangeType {
    /// Column added
    ColumnAdded,
    /// Column removed
    ColumnRemoved,
    /// Column type changed
    ColumnTypeChanged,
    /// Column renamed
    ColumnRenamed,
    /// Index added
    IndexAdded,
    /// Index removed
    IndexRemoved,
    /// UDT added
    UdtAdded,
    /// UDT modified
    UdtModified,
    /// UDT removed
    UdtRemoved,
    /// Table option changed
    TableOptionChanged,
}

/// Schema validation report
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Table identifier
    pub table_id: String,
    /// Overall validation status
    pub status: SchemaValidationStatus,
    /// Validation errors
    pub errors: Vec<ValidationError>,
    /// Validation warnings
    pub warnings: Vec<ValidationWarning>,
    /// Recommendations
    pub recommendations: Vec<String>,
    /// Validation timestamp
    pub validated_at: SystemTime,
}

/// Validation error details
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error code
    pub code: String,
    /// Error message
    pub message: String,
    /// Affected component
    pub component: Option<String>,
    /// Severity level
    pub severity: ErrorSeverity,
}

/// Error severity levels
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorSeverity {
    Critical,
    High,
    Medium,
    Low,
}

/// Validation warning details
#[derive(Debug, Clone)]
pub struct ValidationWarning {
    /// Warning code
    pub code: String,
    /// Warning message
    pub message: String,
    /// Affected component
    pub component: Option<String>,
}

/// Schema search query
#[derive(Debug, Clone)]
pub struct SchemaQuery {
    /// Keyspace filter (optional)
    pub keyspace: Option<String>,
    /// Table name pattern (supports wildcards)
    pub table_pattern: Option<String>,
    /// Include schemas with specific source types
    pub source_types: Option<Vec<SchemaSource>>,
    /// Include only validated schemas
    pub validated_only: bool,
    /// Include version history
    pub include_history: bool,
}

impl SchemaRegistry {
    /// Create a new schema registry
    pub async fn new(
        config: SchemaRegistryConfig,
        platform: Arc<Platform>,
        core_config: Config,
    ) -> Result<Self> {
        let discovery_engine = Arc::new(
            SchemaDiscoveryEngine::new(
                config.discovery_config.clone(),
                platform.clone(),
                core_config.clone(),
            )
            .await?,
        );

        let validator = Arc::new(SchemaValidator::new());
        let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

        Ok(Self {
            config,
            _platform: platform,
            _core_config: core_config,
            schemas: Arc::new(RwLock::new(HashMap::new())),
            udt_registry,
            discovery_engine,
            validator,
            version_history: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Discover and register schema from SSTable files
    pub async fn discover_schema(
        &self,
        keyspace: &str,
        table: &str,
        sstable_files: &[PathBuf],
    ) -> Result<TableSchema> {
        if !self.config.enable_auto_discovery {
            return Err(Error::Schema("Auto-discovery is disabled".to_string()));
        }

        // Use discovery engine to analyze SSTable files
        let schema_info = self
            .discovery_engine
            .discover_schema(keyspace, table, sstable_files)
            .await?;

        // Convert to TableSchema format for compatibility
        let table_schema = self.convert_schema_info_to_table_schema(&schema_info)?;

        // Register the discovered schema
        self.register_discovered_schema(
            table_schema.clone(),
            Some(schema_info),
            sstable_files.to_vec(),
        )
        .await?;

        Ok(table_schema)
    }

    /// Register a schema from external source
    pub async fn register_schema(&self, schema: TableSchema, source: SchemaSource) -> Result<()> {
        let table_id = format!("{}.{}", schema.keyspace, schema.table);

        // Validate schema if validation is enabled
        let validation_status = if self.config.enable_validation {
            match self.validator.validate_table_schema(&schema).await {
                Ok(_) => SchemaValidationStatus::Valid,
                Err(_) => SchemaValidationStatus::Invalid,
            }
        } else {
            SchemaValidationStatus::NotValidated
        };

        // Create schema entry
        let entry = SchemaEntry {
            schema: schema.clone(),
            extended_info: None,
            registered_at: SystemTime::now(),
            source,
            validation_status,
            _associated_files: Vec::new(),
        };

        // Store in registry
        {
            let mut schemas = self.schemas.write().await;

            // Check if we need to create a new version
            if self.config.enable_versioning && schemas.contains_key(&table_id) {
                self.create_schema_version(&table_id, &schema).await?;
            }

            schemas.insert(table_id, entry);
        }

        Ok(())
    }

    /// Get schema by keyspace and table name
    pub async fn get_schema(&self, keyspace: &str, table: &str) -> Result<TableSchema> {
        let table_id = format!("{}.{}", keyspace, table);
        let schemas = self.schemas.read().await;

        match schemas.get(&table_id) {
            Some(entry) => {
                // Check if schema is still valid (cache TTL)
                if self.is_entry_expired(&entry) {
                    drop(schemas); // Release read lock
                    return self.refresh_schema(keyspace, table).await;
                }
                Ok(entry.schema.clone())
            }
            None => {
                drop(schemas); // Release read lock
                // Try to discover schema if auto-discovery is enabled
                if self.config.enable_auto_discovery {
                    self.auto_discover_schema(keyspace, table).await
                } else {
                    Err(Error::Schema(format!(
                        "Schema not found: {}.{}",
                        keyspace, table
                    )))
                }
            }
        }
    }

    /// Get extended schema information
    pub async fn get_schema_info(&self, keyspace: &str, table: &str) -> Result<Option<SchemaInfo>> {
        let table_id = format!("{}.{}", keyspace, table);
        let schemas = self.schemas.read().await;

        match schemas.get(&table_id) {
            Some(entry) => Ok(entry.extended_info.clone()),
            None => Ok(None),
        }
    }

    /// List all registered schemas
    pub async fn list_schemas(&self, query: Option<SchemaQuery>) -> Result<Vec<TableSchema>> {
        let schemas = self.schemas.read().await;
        let mut results = Vec::new();

        for (_table_id, entry) in schemas.iter() {
            // Apply query filters if provided
            if let Some(ref q) = query {
                if !self.matches_query(&entry.schema, q) {
                    continue;
                }
            }

            results.push(entry.schema.clone());
        }

        // Sort by keyspace, then table name
        results.sort_by(|a, b| {
            a.keyspace
                .cmp(&b.keyspace)
                .then_with(|| a.table.cmp(&b.table))
        });

        Ok(results)
    }

    /// Validate a schema
    pub async fn validate_schema(&self, keyspace: &str, table: &str) -> Result<ValidationReport> {
        let schema = self.get_schema(keyspace, table).await?;
        let table_id = format!("{}.{}", keyspace, table);

        // Perform comprehensive validation
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();

        // Basic schema structure validation
        if let Err(e) = schema.validate() {
            errors.push(ValidationError {
                code: "SCHEMA_INVALID".to_string(),
                message: e.to_string(),
                component: None,
                severity: ErrorSeverity::Critical,
            });
        }

        // UDT validation
        self.validate_schema_udts(&schema, &mut errors, &mut warnings)
            .await;

        // Column type validation
        self.validate_column_types(&schema, &mut errors, &mut warnings)
            .await;

        // Performance recommendations
        self.generate_performance_recommendations(&schema, &mut recommendations)
            .await;

        // Determine overall status
        let status = if !errors.is_empty() {
            SchemaValidationStatus::Invalid
        } else if !warnings.is_empty() {
            SchemaValidationStatus::ValidWithWarnings
        } else {
            SchemaValidationStatus::Valid
        };

        // Update validation status in registry
        {
            let mut schemas = self.schemas.write().await;
            if let Some(entry) = schemas.get_mut(&table_id) {
                entry.validation_status = status.clone();
            }
        }

        Ok(ValidationReport {
            table_id,
            status,
            errors,
            warnings,
            recommendations,
            validated_at: SystemTime::now(),
        })
    }

    /// Get schema version history
    pub async fn get_schema_history(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Vec<SchemaVersion>> {
        if !self.config.enable_versioning {
            return Err(Error::Schema("Schema versioning is disabled".to_string()));
        }

        let table_id = format!("{}.{}", keyspace, table);
        let history = self.version_history.read().await;

        Ok(history.get(&table_id).cloned().unwrap_or_default())
    }

    /// Remove schema from registry
    pub async fn remove_schema(&self, keyspace: &str, table: &str) -> Result<()> {
        let table_id = format!("{}.{}", keyspace, table);

        {
            let mut schemas = self.schemas.write().await;
            schemas.remove(&table_id);
        }

        // Also remove from version history if versioning is enabled
        if self.config.enable_versioning {
            let mut history = self.version_history.write().await;
            history.remove(&table_id);
        }

        Ok(())
    }

    /// Generate CQL CREATE statement for schema
    pub async fn generate_cql(&self, keyspace: &str, table: &str) -> Result<String> {
        // First try to get extended schema info for better CQL generation
        if let Some(schema_info) = self.get_schema_info(keyspace, table).await? {
            return self.discovery_engine.generate_cql(&schema_info).await;
        }

        // Fallback to basic TableSchema CQL generation
        let schema = self.get_schema(keyspace, table).await?;
        Ok(self.generate_basic_cql(&schema))
    }

    /// Export schema as JSON
    pub async fn export_schema_json(&self, keyspace: &str, table: &str) -> Result<String> {
        self.export_schema_json_with_config(
            keyspace,
            table,
            &crate::schema::json_exporter::JsonExportConfig::default(),
        )
        .await
    }

    /// Export schema as JSON with custom configuration
    pub async fn export_schema_json_with_config(
        &self,
        keyspace: &str,
        table: &str,
        config: &crate::schema::json_exporter::JsonExportConfig,
    ) -> Result<String> {
        // Try extended schema info first
        if let Some(schema_info) = self.get_schema_info(keyspace, table).await? {
            return self
                .discovery_engine
                .export_json_with_config(&schema_info, config)
                .await;
        }

        // Fallback to basic TableSchema JSON
        let schema = self.get_schema(keyspace, table).await?;
        let exporter = crate::schema::json_exporter::JsonExporter::with_config(config.clone());
        exporter.export_table_schema(&schema)
    }

    /// Export schema as compact JSON (minimal format)
    pub async fn export_schema_json_compact(&self, keyspace: &str, table: &str) -> Result<String> {
        let config = crate::schema::json_exporter::JsonExportConfig {
            format_variant: crate::schema::json_exporter::JsonFormat::Compact,
            include_metadata: false,
            include_performance_metrics: false,
            include_type_details: false,
            pretty_format: false,
            ..Default::default()
        };
        self.export_schema_json_with_config(keyspace, table, &config)
            .await
    }

    /// Export schema for API documentation (OpenAPI-compatible format)
    pub async fn export_schema_json_openapi(&self, keyspace: &str, table: &str) -> Result<String> {
        let config = crate::schema::json_exporter::JsonExportConfig {
            format_variant: crate::schema::json_exporter::JsonFormat::OpenApi,
            include_documentation: true,
            include_type_details: true,
            include_metadata: false,
            ..Default::default()
        };
        self.export_schema_json_with_config(keyspace, table, &config)
            .await
    }

    /// Export schema for data pipeline tools
    pub async fn export_schema_json_pipeline(&self, keyspace: &str, table: &str) -> Result<String> {
        let config = crate::schema::json_exporter::JsonExportConfig {
            format_variant: crate::schema::json_exporter::JsonFormat::DataPipeline,
            include_type_details: true,
            include_table_options: false,
            include_performance_metrics: true,
            ..Default::default()
        };
        self.export_schema_json_with_config(keyspace, table, &config)
            .await
    }

    /// Export multiple schemas as a JSON collection
    pub async fn export_multiple_schemas_json(
        &self,
        schema_infos: &[SchemaInfo],
    ) -> Result<String> {
        let exporter = crate::schema::json_exporter::JsonExporter::new();
        exporter.export_multiple_schemas(schema_infos)
    }

    /// Export all schemas in a keyspace as JSON collection
    pub async fn export_keyspace_schemas_json(&self, keyspace: &str) -> Result<String> {
        let mut schema_infos = Vec::new();

        // Get all schemas in the keyspace
        for (_table_id, entry) in self.schemas.read().await.iter() {
            if entry.schema.keyspace == keyspace {
                // Try to get extended schema info
                if let Ok(Some(schema_info)) = self
                    .get_schema_info(&entry.schema.keyspace, &entry.schema.table)
                    .await
                {
                    schema_infos.push(schema_info);
                }
            }
        }

        if schema_infos.is_empty() {
            return Err(Error::NotFound(format!(
                "No schemas found in keyspace '{}'",
                keyspace
            )));
        }

        self.export_multiple_schemas_json(&schema_infos).await
    }

    /// Register UDT in the registry
    pub async fn register_udt(&self, udt_def: UdtTypeDef) -> Result<()> {
        let mut registry = self.udt_registry.write().await;
        registry.register_udt(udt_def);
        Ok(())
    }

    /// Get UDT definition
    pub async fn get_udt(&self, keyspace: &str, name: &str) -> Result<Option<UdtTypeDef>> {
        let registry = self.udt_registry.read().await;
        Ok(registry.get_udt(keyspace, name).cloned())
    }

    /// Get ComparatorType for a specific column in a table
    pub async fn get_column_comparator(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<ComparatorType> {
        let schema = self.get_schema(keyspace, table).await?;

        // Find the column
        let column_def = schema
            .columns
            .iter()
            .find(|c| c.name == column)
            .ok_or_else(|| {
                Error::Schema(format!(
                    "Column '{}' not found in table '{}.{}'",
                    column, keyspace, table
                ))
            })?;

        // Parse the column type and create comparator
        let cql_type = CqlType::parse(&column_def.data_type)?;
        ComparatorType::from_cql_type(&cql_type)
    }

    /// Get ComparatorType for all columns in a table
    pub async fn get_table_comparators(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<HashMap<String, ComparatorType>> {
        let schema = self.get_schema(keyspace, table).await?;
        let mut comparators = HashMap::new();

        for column in &schema.columns {
            let cql_type = CqlType::parse(&column.data_type)?;
            let comparator = ComparatorType::from_cql_type(&cql_type)?;
            comparators.insert(column.name.clone(), comparator);
        }

        Ok(comparators)
    }

    /// Get ComparatorType for partition key columns (for key comparison)
    pub async fn get_partition_key_comparator(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Vec<ComparatorType>> {
        let schema = self.get_schema(keyspace, table).await?;
        let mut comparators = Vec::new();

        // Get partition keys in order
        let ordered_keys = schema.ordered_partition_keys();
        for key_column in ordered_keys {
            let cql_type = CqlType::parse(&key_column.data_type)?;
            let comparator = ComparatorType::from_cql_type(&cql_type)?;
            comparators.push(comparator);
        }

        Ok(comparators)
    }

    /// Get the complete schema context for parsing operations
    pub async fn get_parsing_context(&self, keyspace: &str, table: &str) -> Result<ParsingContext> {
        let schema = self.get_schema(keyspace, table).await?;
        let partition_comparators = self.get_partition_key_comparator(keyspace, table).await?;
        let clustering_comparators = self.get_clustering_key_comparator(keyspace, table).await?;
        let column_comparators = self.get_table_comparators(keyspace, table).await?;

        Ok(ParsingContext {
            schema,
            partition_comparators,
            clustering_comparators,
            column_comparators,
        })
    }

    /// Get ComparatorType for clustering key columns (for clustering comparison)
    pub async fn get_clustering_key_comparator(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Vec<ComparatorType>> {
        let schema = self.get_schema(keyspace, table).await?;
        let mut comparators = Vec::new();

        // Get clustering keys in order
        let ordered_keys = schema.ordered_clustering_keys();
        for key_column in ordered_keys {
            let cql_type = CqlType::parse(&key_column.data_type)?;
            let comparator = ComparatorType::from_cql_type(&cql_type)?;
            comparators.push(comparator);
        }

        Ok(comparators)
    }

    /// Validate column type compatibility using ComparatorType
    pub async fn validate_column_type_compatibility(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
        expected_type: &str,
    ) -> Result<bool> {
        let column_comparator = self.get_column_comparator(keyspace, table, column).await?;
        let expected_cql_type = CqlType::parse(expected_type)?;
        let expected_comparator = ComparatorType::from_cql_type(&expected_cql_type)?;

        // Check if comparators are compatible (same type structure)
        Ok(self.comparators_are_compatible(&column_comparator, &expected_comparator))
    }

    /// Check if two ComparatorTypes are compatible
    fn comparators_are_compatible(&self, left: &ComparatorType, right: &ComparatorType) -> bool {
        match (left, right) {
            // Exact matches
            (ComparatorType::Boolean, ComparatorType::Boolean) => true,
            (ComparatorType::TinyInt, ComparatorType::TinyInt) => true,
            (ComparatorType::SmallInt, ComparatorType::SmallInt) => true,
            (ComparatorType::Int, ComparatorType::Int) => true,
            (ComparatorType::BigInt, ComparatorType::BigInt) => true,
            (ComparatorType::Float32, ComparatorType::Float32) => true,
            (ComparatorType::Float, ComparatorType::Float) => true,
            (ComparatorType::Text, ComparatorType::Text) => true,
            (ComparatorType::Blob, ComparatorType::Blob) => true,
            (ComparatorType::Timestamp, ComparatorType::Timestamp) => true,
            (ComparatorType::Uuid, ComparatorType::Uuid) => true,
            (ComparatorType::Json, ComparatorType::Json) => true,

            // Collection types
            (ComparatorType::List(l_elem), ComparatorType::List(r_elem)) => {
                self.comparators_are_compatible(l_elem, r_elem)
            }
            (ComparatorType::Set(l_elem), ComparatorType::Set(r_elem)) => {
                self.comparators_are_compatible(l_elem, r_elem)
            }
            (ComparatorType::Map(l_key, l_val), ComparatorType::Map(r_key, r_val)) => {
                self.comparators_are_compatible(l_key, r_key)
                    && self.comparators_are_compatible(l_val, r_val)
            }

            // Tuple types
            (ComparatorType::Tuple(l_fields), ComparatorType::Tuple(r_fields)) => {
                l_fields.len() == r_fields.len()
                    && l_fields
                        .iter()
                        .zip(r_fields.iter())
                        .all(|(l, r)| self.comparators_are_compatible(l, r))
            }

            // UDT types
            (
                ComparatorType::Udt {
                    type_name: l_name,
                    keyspace: l_ks,
                    ..
                },
                ComparatorType::Udt {
                    type_name: r_name,
                    keyspace: r_ks,
                    ..
                },
            ) => l_name == r_name && l_ks == r_ks,

            // Frozen types
            (ComparatorType::Frozen(l_inner), ComparatorType::Frozen(r_inner)) => {
                self.comparators_are_compatible(l_inner, r_inner)
            }

            // Custom types
            (ComparatorType::Custom(l_name), ComparatorType::Custom(r_name)) => l_name == r_name,

            // No other combinations are compatible
            _ => false,
        }
    }

    /// Get registry statistics
    pub async fn get_statistics(&self) -> Result<RegistryStatistics> {
        let schemas = self.schemas.read().await;
        let udt_registry = self.udt_registry.read().await;
        let version_history = self.version_history.read().await;

        let mut stats = RegistryStatistics {
            total_schemas: schemas.len(),
            schemas_by_keyspace: HashMap::new(),
            validated_schemas: 0,
            schemas_with_warnings: 0,
            invalid_schemas: 0,
            total_udts: udt_registry.total_udts(),
            total_versions: version_history.values().map(|v| v.len()).sum(),
            auto_discovered_schemas: 0,
            manually_registered_schemas: 0,
            cache_hit_rate: 0.0, // TODO: Implement cache metrics
        };

        // Analyze schema distribution and status
        for entry in schemas.values() {
            let keyspace = &entry.schema.keyspace;
            *stats
                .schemas_by_keyspace
                .entry(keyspace.clone())
                .or_insert(0) += 1;

            match entry.validation_status {
                SchemaValidationStatus::Valid => stats.validated_schemas += 1,
                SchemaValidationStatus::ValidWithWarnings => stats.schemas_with_warnings += 1,
                SchemaValidationStatus::Invalid => stats.invalid_schemas += 1,
                SchemaValidationStatus::NotValidated => {}
            }

            match entry.source {
                SchemaSource::Discovered(_) => stats.auto_discovered_schemas += 1,
                _ => stats.manually_registered_schemas += 1,
            }
        }

        Ok(stats)
    }

    // Private helper methods

    async fn register_discovered_schema(
        &self,
        schema: TableSchema,
        schema_info: Option<SchemaInfo>,
        sstable_files: Vec<PathBuf>,
    ) -> Result<()> {
        let table_id = format!("{}.{}", schema.keyspace, schema.table);
        let source = SchemaSource::Discovered(sstable_files.clone());

        let entry = SchemaEntry {
            schema,
            extended_info: schema_info,
            registered_at: SystemTime::now(),
            source,
            validation_status: SchemaValidationStatus::Valid, // Discovery implies validation
            _associated_files: sstable_files,
        };

        let mut schemas = self.schemas.write().await;
        schemas.insert(table_id, entry);

        Ok(())
    }

    fn convert_schema_info_to_table_schema(&self, schema_info: &SchemaInfo) -> Result<TableSchema> {
        let mut columns = Vec::new();
        let mut partition_keys = Vec::new();
        let mut clustering_keys = Vec::new();

        // Convert partition keys
        for (pos, pk) in schema_info.partition_key.iter().enumerate() {
            partition_keys.push(crate::schema::KeyColumn {
                name: pk.name.clone(),
                data_type: pk.data_type.clone(),
                position: pos,
            });
        }

        // Convert clustering keys
        for ck in &schema_info.clustering_keys {
            clustering_keys.push(ck.clone());
        }

        // Convert all columns
        for col in &schema_info.regular_columns {
            columns.push(crate::schema::Column {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
                default: None, // ColumnDefinition doesn't have default_value
            });
        }

        // Add static columns
        for col in &schema_info.static_columns {
            columns.push(crate::schema::Column {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
                default: None, // ColumnDefinition doesn't have default_value
            });
        }

        Ok(TableSchema {
            keyspace: schema_info.keyspace.clone(),
            table: schema_info.table.clone(),
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
        })
    }

    fn is_entry_expired(&self, entry: &SchemaEntry) -> bool {
        if !self.config.enable_caching {
            return false;
        }

        let ttl = std::time::Duration::from_secs(self.config.cache_ttl_seconds);
        entry
            .registered_at
            .elapsed()
            .unwrap_or(std::time::Duration::ZERO)
            > ttl
    }

    async fn refresh_schema(&self, keyspace: &str, table: &str) -> Result<TableSchema> {
        // Implementation for refreshing expired schema
        // For now, just try auto-discovery
        self.auto_discover_schema(keyspace, table).await
    }

    async fn auto_discover_schema(&self, keyspace: &str, table: &str) -> Result<TableSchema> {
        // Try to find SSTable files for this table
        // This is a placeholder - in practice, you'd scan the data directory
        let sstable_files = self.find_sstable_files(keyspace, table).await?;

        if sstable_files.is_empty() {
            return Err(Error::Schema(format!(
                "No SSTables found for {}.{}",
                keyspace, table
            )));
        }

        self.discover_schema(keyspace, table, &sstable_files).await
    }

    async fn find_sstable_files(&self, _keyspace: &str, _table: &str) -> Result<Vec<PathBuf>> {
        // Placeholder implementation
        // In practice, this would scan the data directory structure
        Ok(Vec::new())
    }

    fn matches_query(&self, schema: &TableSchema, query: &SchemaQuery) -> bool {
        // Apply keyspace filter
        if let Some(ref ks) = query.keyspace {
            if &schema.keyspace != ks {
                return false;
            }
        }

        // Apply table pattern filter
        if let Some(ref pattern) = query.table_pattern {
            if !self.matches_pattern(&schema.table, pattern) {
                return false;
            }
        }

        // Other filters would be applied here
        true
    }

    fn matches_pattern(&self, text: &str, pattern: &str) -> bool {
        // Simple wildcard matching (can be enhanced)
        if pattern == "*" {
            return true;
        }

        // For now, just exact match or contains
        text == pattern || text.contains(pattern)
    }

    async fn create_schema_version(&self, table_id: &str, new_schema: &TableSchema) -> Result<()> {
        let mut version_history = self.version_history.write().await;
        let versions = version_history
            .entry(table_id.to_string())
            .or_insert_with(Vec::new);

        let version_number = versions.len() as u32 + 1;
        let changes = if versions.is_empty() {
            vec![SchemaChange {
                change_type: SchemaChangeType::ColumnAdded,
                component: "initial".to_string(),
                description: "Initial schema version".to_string(),
                old_value: None,
                new_value: None,
            }]
        } else {
            // Compare with previous version to detect changes
            self.detect_schema_changes(&versions.last().unwrap().schema, new_schema)
        };

        let new_version = SchemaVersion {
            version: version_number,
            created_at: SystemTime::now(),
            schema: new_schema.clone(),
            changes,
            source: "registry".to_string(),
        };

        versions.push(new_version);

        // Limit version history size
        if versions.len() > self.config.max_versions_per_schema {
            versions.remove(0);
        }

        Ok(())
    }

    fn detect_schema_changes(
        &self,
        old_schema: &TableSchema,
        new_schema: &TableSchema,
    ) -> Vec<SchemaChange> {
        let mut changes = Vec::new();

        // Compare columns
        let old_columns: HashMap<_, _> = old_schema.columns.iter().map(|c| (&c.name, c)).collect();
        let new_columns: HashMap<_, _> = new_schema.columns.iter().map(|c| (&c.name, c)).collect();

        // Find added columns
        for (name, column) in &new_columns {
            if !old_columns.contains_key(name) {
                changes.push(SchemaChange {
                    change_type: SchemaChangeType::ColumnAdded,
                    component: name.to_string(),
                    description: format!(
                        "Column '{}' added with type '{}'",
                        name, column.data_type
                    ),
                    old_value: None,
                    new_value: Some(column.data_type.clone()),
                });
            }
        }

        // Find removed columns
        for (name, _) in &old_columns {
            if !new_columns.contains_key(name) {
                changes.push(SchemaChange {
                    change_type: SchemaChangeType::ColumnRemoved,
                    component: name.to_string(),
                    description: format!("Column '{}' removed", name),
                    old_value: None,
                    new_value: None,
                });
            }
        }

        // Find type changes
        for (name, new_column) in &new_columns {
            if let Some(old_column) = old_columns.get(name) {
                if old_column.data_type != new_column.data_type {
                    changes.push(SchemaChange {
                        change_type: SchemaChangeType::ColumnTypeChanged,
                        component: name.to_string(),
                        description: format!("Column '{}' type changed", name),
                        old_value: Some(old_column.data_type.clone()),
                        new_value: Some(new_column.data_type.clone()),
                    });
                }
            }
        }

        changes
    }

    async fn validate_schema_udts(
        &self,
        schema: &TableSchema,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        let udt_registry = self.udt_registry.read().await;

        for column in &schema.columns {
            // Check if column type references a UDT
            if let Ok(cql_type) = CqlType::parse(&column.data_type) {
                self.validate_cql_type_udts(
                    &cql_type,
                    &schema.keyspace,
                    &udt_registry,
                    errors,
                    warnings,
                );
            }
        }
    }

    fn validate_cql_type_udts(
        &self,
        cql_type: &CqlType,
        keyspace: &str,
        udt_registry: &UdtRegistry,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        match cql_type {
            CqlType::Udt(udt_name, _) => {
                if !udt_registry.contains_udt(keyspace, udt_name) {
                    errors.push(ValidationError {
                        code: "UDT_NOT_FOUND".to_string(),
                        message: format!("UDT '{}' not found in keyspace '{}'", udt_name, keyspace),
                        component: Some(udt_name.clone()),
                        severity: ErrorSeverity::High,
                    });
                }
            }
            CqlType::List(inner) | CqlType::Set(inner) | CqlType::Frozen(inner) => {
                self.validate_cql_type_udts(inner, keyspace, udt_registry, errors, warnings);
            }
            CqlType::Map(key_type, value_type) => {
                self.validate_cql_type_udts(key_type, keyspace, udt_registry, errors, warnings);
                self.validate_cql_type_udts(value_type, keyspace, udt_registry, errors, warnings);
            }
            CqlType::Tuple(types) => {
                for t in types {
                    self.validate_cql_type_udts(t, keyspace, udt_registry, errors, warnings);
                }
            }
            _ => {} // Primitive types don't need UDT validation
        }
    }

    async fn validate_column_types(
        &self,
        schema: &TableSchema,
        errors: &mut Vec<ValidationError>,
        _warnings: &mut Vec<ValidationWarning>,
    ) {
        for column in &schema.columns {
            if let Err(e) = CqlType::parse(&column.data_type) {
                errors.push(ValidationError {
                    code: "INVALID_COLUMN_TYPE".to_string(),
                    message: format!("Invalid column type '{}': {}", column.data_type, e),
                    component: Some(column.name.clone()),
                    severity: ErrorSeverity::High,
                });
            }
        }
    }

    async fn generate_performance_recommendations(
        &self,
        schema: &TableSchema,
        recommendations: &mut Vec<String>,
    ) {
        // Check for potential performance issues

        // Large partition keys
        if schema.partition_keys.len() > 3 {
            recommendations.push(
                "Consider reducing the number of partition key columns for better performance"
                    .to_string(),
            );
        }

        // Many clustering keys
        if schema.clustering_keys.len() > 5 {
            recommendations
                .push("Large number of clustering keys may impact query performance".to_string());
        }

        // Column count
        if schema.columns.len() > 50 {
            recommendations.push(
                "Consider using UDTs or denormalizing wide tables for better performance"
                    .to_string(),
            );
        }
    }

    fn generate_basic_cql(&self, schema: &TableSchema) -> String {
        let mut cql = format!("CREATE TABLE {}.{} (\n", schema.keyspace, schema.table);

        // Add columns
        for (i, column) in schema.columns.iter().enumerate() {
            if i > 0 {
                cql.push_str(",\n");
            }
            cql.push_str(&format!("  {} {}", column.name, column.data_type));
        }

        // Add primary key
        if !schema.partition_keys.is_empty() {
            cql.push_str(",\n  PRIMARY KEY (");

            if schema.partition_keys.len() == 1 && schema.clustering_keys.is_empty() {
                cql.push_str(&schema.partition_keys[0].name);
            } else {
                // Composite primary key
                cql.push('(');
                for (i, pk) in schema.partition_keys.iter().enumerate() {
                    if i > 0 {
                        cql.push_str(", ");
                    }
                    cql.push_str(&pk.name);
                }
                cql.push(')');

                if !schema.clustering_keys.is_empty() {
                    for ck in &schema.clustering_keys {
                        cql.push_str(", ");
                        cql.push_str(&ck.name);
                    }
                }
            }

            cql.push(')');
        }

        cql.push_str("\n);");
        cql
    }
}

/// Registry statistics
#[derive(Debug, Clone)]
pub struct RegistryStatistics {
    /// Total number of registered schemas
    pub total_schemas: usize,
    /// Schemas grouped by keyspace
    pub schemas_by_keyspace: HashMap<String, usize>,
    /// Number of validated schemas
    pub validated_schemas: usize,
    /// Schemas with validation warnings
    pub schemas_with_warnings: usize,
    /// Invalid schemas
    pub invalid_schemas: usize,
    /// Total UDTs registered
    pub total_udts: usize,
    /// Total schema versions stored
    pub total_versions: usize,
    /// Auto-discovered schemas
    pub auto_discovered_schemas: usize,
    /// Manually registered schemas
    pub manually_registered_schemas: usize,
    /// Cache hit rate
    pub cache_hit_rate: f64,
}

/// Schema-driven parsing context containing all necessary type information
#[derive(Debug, Clone)]
pub struct ParsingContext {
    /// The complete table schema
    pub schema: TableSchema,
    /// Comparators for partition key components
    pub partition_comparators: Vec<ComparatorType>,
    /// Comparators for clustering key components
    pub clustering_comparators: Vec<ComparatorType>,
    /// Comparators for all columns by name
    pub column_comparators: HashMap<String, ComparatorType>,
}

impl ParsingContext {
    /// Get comparator for a specific column
    pub fn get_column_comparator(&self, column_name: &str) -> Option<&ComparatorType> {
        self.column_comparators.get(column_name)
    }

    /// Check if schema-driven parsing is fully configured
    pub fn is_complete(&self) -> bool {
        !self.partition_comparators.is_empty() || !self.schema.partition_keys.is_empty()
    }

    /// Get all key columns (partition + clustering) names in order
    pub fn get_all_key_column_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        names.extend(
            self.schema
                .ordered_partition_keys()
                .iter()
                .map(|k| k.name.clone()),
        );
        names.extend(
            self.schema
                .ordered_clustering_keys()
                .iter()
                .map(|k| k.name.clone()),
        );
        names
    }
}

/// Schema validator for comprehensive validation
pub struct SchemaValidator;

impl SchemaValidator {
    pub fn new() -> Self {
        Self
    }

    pub async fn validate_table_schema(&self, schema: &TableSchema) -> Result<()> {
        schema.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_schema_registry_creation() {
        let config = SchemaRegistryConfig::default();
        let core_config = Config::default();
        let platform = Arc::new(Platform::new(&core_config).await.unwrap());

        let registry = SchemaRegistry::new(config, platform, core_config)
            .await
            .unwrap();
        let stats = registry.get_statistics().await.unwrap();

        assert_eq!(stats.total_schemas, 0);
    }

    #[test]
    fn test_schema_query_creation() {
        let query = SchemaQuery {
            keyspace: Some("test_ks".to_string()),
            table_pattern: Some("user_*".to_string()),
            source_types: None,
            validated_only: false,
            include_history: false,
        };

        assert_eq!(query.keyspace.as_ref().unwrap(), "test_ks");
        assert_eq!(query.table_pattern.as_ref().unwrap(), "user_*");
    }
}

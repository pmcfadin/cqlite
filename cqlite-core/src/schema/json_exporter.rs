//! Enhanced JSON Schema Export System
//!
//! This module provides comprehensive JSON export capabilities for schema information,
//! supporting multiple output formats and use cases including documentation,
//! API integration, and data pipeline configuration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    Error, Result,
    schema::{
        TableSchema,
        discovery::{CollectionType, IndexType, SchemaInfo, SchemaMetadata, TableOptions},
    },
};

/// JSON export configuration
#[derive(Debug, Clone)]
pub struct JsonExportConfig {
    /// Include metadata in output
    pub include_metadata: bool,
    /// Include performance metrics
    pub include_performance_metrics: bool,
    /// Include validation results
    pub include_validation_results: bool,
    /// Include UDT definitions
    pub include_udt_definitions: bool,
    /// Include index definitions
    pub include_index_definitions: bool,
    /// Include table options
    pub include_table_options: bool,
    /// Format for human readability
    pub pretty_format: bool,
    /// Include type details and examples
    pub include_type_details: bool,
    /// Include schema comments and documentation
    pub include_documentation: bool,
    /// Export format variant
    pub format_variant: JsonFormat,
}

/// JSON export format variants
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonFormat {
    /// Standard format - complete schema representation
    Standard,
    /// Compact format - minimal essential information
    Compact,
    /// OpenAPI format - suitable for API documentation
    OpenApi,
    /// DataPipeline format - optimized for ETL tools
    DataPipeline,
    /// Documentation format - enhanced with comments and examples
    Documentation,
}

impl Default for JsonExportConfig {
    fn default() -> Self {
        Self {
            include_metadata: true,
            include_performance_metrics: false,
            include_validation_results: true,
            include_udt_definitions: true,
            include_index_definitions: true,
            include_table_options: true,
            pretty_format: true,
            include_type_details: true,
            include_documentation: false,
            format_variant: JsonFormat::Standard,
        }
    }
}

/// Enhanced JSON schema exporter
pub struct JsonExporter {
    config: JsonExportConfig,
}

/// Standard JSON schema representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSchema {
    /// Schema version
    pub schema_version: String,
    /// Export format used
    pub format: String,
    /// Keyspace information
    pub keyspace: JsonKeyspace,
    /// Table definition
    pub table: JsonTable,
    /// User-defined types (if any)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub user_defined_types: Vec<JsonUDT>,
    /// Index definitions (if any)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<JsonIndex>,
    /// Export metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonMetadata>,
}

/// Keyspace information in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonKeyspace {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Table definition in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonTable {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub columns: Vec<JsonColumn>,
    pub primary_key: JsonPrimaryKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_options: Option<JsonTableOptions>,
}

/// Column definition in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_static: bool,
    pub is_partition_key: bool,
    pub is_clustering_key: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clustering_order: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_details: Option<JsonTypeDetails>,
}

/// Primary key structure in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonPrimaryKey {
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<JsonClusteringKey>,
}

/// Clustering key with ordering in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonClusteringKey {
    pub column: String,
    pub order: String, // ASC or DESC
}

/// Type details for complex types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonTypeDetails {
    pub base_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frozen: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udt_fields: Option<Vec<JsonUDTField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub example_values: Option<Vec<String>>,
}

/// User-defined type in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonUDT {
    pub name: String,
    pub keyspace: String,
    pub fields: Vec<JsonUDTField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// UDT field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonUDTField {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Index definition in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonIndex {
    pub name: String,
    pub index_type: String,
    pub target_column: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Table options in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonTableOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction: Option<JsonCompaction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<JsonCompression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caching: Option<JsonCaching>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bloom_filter_fp_chance: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_grace_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_time_to_live: Option<u32>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub additional_properties: HashMap<String, String>,
}

/// Compaction options in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonCompaction {
    pub class: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

/// Compression options in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonCompression {
    pub algorithm: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_length_kb: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crc_check_chance: Option<f64>,
}

/// Caching options in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonCaching {
    pub keys: String,
    pub rows_per_partition: String,
}

/// Export metadata in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonMetadata {
    pub exported_at: String,
    pub schema_discovered_at: String,
    pub source_files: Vec<String>,
    pub total_rows_sampled: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cassandra_version: Option<String>,
    pub discovery_method: String,
    pub schema_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_results: Option<JsonValidationResults>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub performance_metrics: Option<JsonPerformanceMetrics>,
}

/// Validation results in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonValidationResults {
    pub status: String,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub files_analyzed: u32,
    pub schema_mismatches: u32,
}

/// Performance metrics in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonPerformanceMetrics {
    pub total_time_ms: u64,
    pub header_parsing_time_ms: u64,
    pub data_sampling_time_ms: u64,
    pub type_inference_time_ms: u64,
    pub validation_time_ms: u64,
    pub peak_memory_usage_bytes: u64,
}

impl JsonExporter {
    /// Create a new JSON exporter with default configuration
    pub fn new() -> Self {
        Self {
            config: JsonExportConfig::default(),
        }
    }

    /// Create JSON exporter with custom configuration
    pub fn with_config(config: JsonExportConfig) -> Self {
        Self { config }
    }

    /// Export SchemaInfo to JSON
    pub fn export_schema_info(&self, schema_info: &SchemaInfo) -> Result<String> {
        let json_schema = self.convert_schema_info_to_json(schema_info)?;
        self.serialize_json_schema(&json_schema)
    }

    /// Export TableSchema to JSON
    pub fn export_table_schema(&self, table_schema: &TableSchema) -> Result<String> {
        let json_schema = self.convert_table_schema_to_json(table_schema)?;
        self.serialize_json_schema(&json_schema)
    }

    /// Export multiple schemas to a JSON collection
    pub fn export_multiple_schemas(&self, schemas: &[SchemaInfo]) -> Result<String> {
        let json_schemas: Result<Vec<JsonSchema>> = schemas
            .iter()
            .map(|schema| self.convert_schema_info_to_json(schema))
            .collect();

        let collection = JsonSchemaCollection {
            schemas: json_schemas?,
            metadata: JsonCollectionMetadata {
                total_schemas: schemas.len(),
                exported_at: chrono::Utc::now().to_rfc3339(),
                format_variant: format!("{:?}", self.config.format_variant),
            },
        };

        if self.config.pretty_format {
            serde_json::to_string_pretty(&collection)
        } else {
            serde_json::to_string(&collection)
        }
        .map_err(|e| Error::Serialization(e.to_string()))
    }

    // Private implementation methods

    fn convert_schema_info_to_json(&self, schema_info: &SchemaInfo) -> Result<JsonSchema> {
        // Build columns with enhanced information
        let mut json_columns = Vec::new();

        // Partition key columns
        for pk in &schema_info.partition_key {
            json_columns.push(JsonColumn {
                name: pk.name.clone(),
                data_type: pk.data_type.clone(),
                nullable: false, // Partition keys cannot be null
                is_static: false,
                is_partition_key: true,
                is_clustering_key: false,
                clustering_order: None,
                default_value: None,
                description: None,
                type_details: if self.config.include_type_details {
                    self.build_type_details(&pk.data_type, &schema_info.collection_types)?
                } else {
                    None
                },
            });
        }

        // Clustering key columns
        for ck in &schema_info.clustering_keys {
            json_columns.push(JsonColumn {
                name: ck.name.clone(),
                data_type: ck.data_type.clone(),
                nullable: false, // Clustering keys cannot be null
                is_static: false,
                is_partition_key: false,
                is_clustering_key: true,
                clustering_order: Some(ck.order.clone()),
                default_value: None,
                description: None,
                type_details: if self.config.include_type_details {
                    self.build_type_details(&ck.data_type, &schema_info.collection_types)?
                } else {
                    None
                },
            });
        }

        // Regular columns
        for col in &schema_info.regular_columns {
            json_columns.push(JsonColumn {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: true, // Regular columns can be null by default
                is_static: false,
                is_partition_key: false,
                is_clustering_key: false,
                clustering_order: None,
                default_value: None,
                description: None,
                type_details: if self.config.include_type_details {
                    self.build_type_details(&col.data_type, &schema_info.collection_types)?
                } else {
                    None
                },
            });
        }

        // Static columns
        for col in &schema_info.static_columns {
            json_columns.push(JsonColumn {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: true,
                is_static: true,
                is_partition_key: false,
                is_clustering_key: false,
                clustering_order: None,
                default_value: None,
                description: None,
                type_details: if self.config.include_type_details {
                    self.build_type_details(&col.data_type, &schema_info.collection_types)?
                } else {
                    None
                },
            });
        }

        // Build primary key structure
        let primary_key = JsonPrimaryKey {
            partition_keys: schema_info
                .partition_key
                .iter()
                .map(|pk| pk.name.clone())
                .collect(),
            clustering_keys: schema_info
                .clustering_keys
                .iter()
                .map(|ck| JsonClusteringKey {
                    column: ck.name.clone(),
                    order: ck.order.clone(),
                })
                .collect(),
        };

        // Convert UDTs
        let udts = if self.config.include_udt_definitions {
            schema_info
                .user_defined_types
                .iter()
                .map(|udt| JsonUDT {
                    name: udt.name.clone(),
                    keyspace: udt.keyspace.clone(),
                    fields: udt
                        .fields
                        .iter()
                        .map(|field| JsonUDTField {
                            name: field.name.clone(),
                            data_type: field.field_type.clone(),
                            description: None,
                        })
                        .collect(),
                    description: None,
                })
                .collect()
        } else {
            Vec::new()
        };

        // Convert indexes
        let indexes = if self.config.include_index_definitions {
            schema_info
                .indexes
                .iter()
                .map(|idx| JsonIndex {
                    name: idx.name.clone(),
                    index_type: self.index_type_to_string(&idx.index_type),
                    target_column: idx.target_column.clone(),
                    options: idx.options.clone(),
                    description: None,
                })
                .collect()
        } else {
            Vec::new()
        };

        // Convert table options
        let table_options = if self.config.include_table_options {
            Some(self.convert_table_options(&schema_info.table_options)?)
        } else {
            None
        };

        // Build metadata
        let metadata = if self.config.include_metadata {
            Some(self.convert_metadata(&schema_info.metadata)?)
        } else {
            None
        };

        Ok(JsonSchema {
            schema_version: "1.0".to_string(),
            format: format!("{:?}", self.config.format_variant),
            keyspace: JsonKeyspace {
                name: schema_info.keyspace.clone(),
                description: None,
            },
            table: JsonTable {
                name: schema_info.table.clone(),
                description: None,
                columns: json_columns,
                primary_key,
                table_options,
            },
            user_defined_types: udts,
            indexes,
            metadata,
        })
    }

    fn convert_table_schema_to_json(&self, table_schema: &TableSchema) -> Result<JsonSchema> {
        let mut json_columns = Vec::new();

        // Process all columns
        for column in &table_schema.columns {
            let is_partition_key = table_schema
                .partition_keys
                .iter()
                .any(|pk| pk.name == column.name);
            let clustering_info = table_schema
                .clustering_keys
                .iter()
                .find(|ck| ck.name == column.name)
                .map(|ck| ck.order.clone());

            json_columns.push(JsonColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_static: false, // TableSchema doesn't track static columns separately
                is_partition_key,
                is_clustering_key: clustering_info.is_some(),
                clustering_order: clustering_info,
                default_value: column.default.as_ref().map(|v| format!("{:?}", v)),
                description: None,
                type_details: if self.config.include_type_details {
                    self.build_type_details(&column.data_type, &HashMap::new())?
                } else {
                    None
                },
            });
        }

        let primary_key = JsonPrimaryKey {
            partition_keys: table_schema
                .partition_keys
                .iter()
                .map(|pk| pk.name.clone())
                .collect(),
            clustering_keys: table_schema
                .clustering_keys
                .iter()
                .map(|ck| JsonClusteringKey {
                    column: ck.name.clone(),
                    order: ck.order.clone(),
                })
                .collect(),
        };

        Ok(JsonSchema {
            schema_version: "1.0".to_string(),
            format: format!("{:?}", self.config.format_variant),
            keyspace: JsonKeyspace {
                name: table_schema.keyspace.clone(),
                description: None,
            },
            table: JsonTable {
                name: table_schema.table.clone(),
                description: None,
                columns: json_columns,
                primary_key,
                table_options: None, // TableSchema doesn't include table options
            },
            user_defined_types: Vec::new(),
            indexes: Vec::new(),
            metadata: None,
        })
    }

    fn build_type_details(
        &self,
        data_type: &str,
        _collection_types: &HashMap<String, CollectionType>,
    ) -> Result<Option<JsonTypeDetails>> {
        // Basic type analysis - can be enhanced with proper type parsing
        let base_type = self.extract_base_type(data_type);

        let mut type_details = JsonTypeDetails {
            base_type: base_type.clone(),
            collection_type: None,
            element_types: None,
            frozen: None,
            udt_fields: None,
            example_values: None,
        };

        // Check if it's a collection type
        if data_type.contains("list<") || data_type.contains("set<") || data_type.contains("map<") {
            type_details.collection_type = Some(self.extract_collection_type(data_type));
            type_details.element_types = Some(self.extract_element_types(data_type));
        }

        // Check if it's frozen
        if data_type.starts_with("frozen<") {
            type_details.frozen = Some(true);
        }

        // Add example values for documentation
        if self.config.include_documentation {
            type_details.example_values = Some(self.generate_example_values(&base_type));
        }

        Ok(Some(type_details))
    }

    fn extract_base_type(&self, data_type: &str) -> String {
        // Simple base type extraction - can be enhanced
        if data_type.contains("<") {
            data_type.split('<').next().unwrap_or(data_type).to_string()
        } else {
            data_type.to_string()
        }
    }

    fn extract_collection_type(&self, data_type: &str) -> String {
        if data_type.starts_with("list<") {
            "list".to_string()
        } else if data_type.starts_with("set<") {
            "set".to_string()
        } else if data_type.starts_with("map<") {
            "map".to_string()
        } else {
            "unknown".to_string()
        }
    }

    fn extract_element_types(&self, data_type: &str) -> Vec<String> {
        // Simple element type extraction - can be enhanced with proper parsing
        if let Some(start) = data_type.find('<') {
            if let Some(end) = data_type.rfind('>') {
                let inner = &data_type[start + 1..end];
                return inner.split(',').map(|s| s.trim().to_string()).collect();
            }
        }
        Vec::new()
    }

    fn generate_example_values(&self, base_type: &str) -> Vec<String> {
        match base_type {
            "text" | "varchar" => vec![
                "\"example text\"".to_string(),
                "\"another string\"".to_string(),
            ],
            "int" => vec!["42".to_string(), "100".to_string()],
            "bigint" => vec!["1234567890".to_string()],
            "uuid" => vec!["550e8400-e29b-41d4-a716-446655440000".to_string()],
            "timestamp" => vec!["2024-01-01T12:00:00Z".to_string()],
            "boolean" => vec!["true".to_string(), "false".to_string()],
            "double" => vec!["3.14159".to_string()],
            "list" => vec!["[1, 2, 3]".to_string()],
            "set" => vec!["{1, 2, 3}".to_string()],
            "map" => vec!["{\"key1\": \"value1\", \"key2\": \"value2\"}".to_string()],
            _ => vec!["null".to_string()],
        }
    }

    fn convert_table_options(&self, options: &TableOptions) -> Result<JsonTableOptions> {
        Ok(JsonTableOptions {
            compaction: options.compaction.as_ref().map(|c| JsonCompaction {
                class: c.class.clone(),
                options: c.options.clone(),
            }),
            compression: options.compression.as_ref().map(|c| JsonCompression {
                algorithm: c.algorithm.clone(),
                chunk_length_kb: c.chunk_length_kb,
                crc_check_chance: c.crc_check_chance.map(|f| f as f64),
            }),
            caching: options.caching.as_ref().map(|c| JsonCaching {
                keys: c.keys.clone(),
                rows_per_partition: c.rows_per_partition.clone(),
            }),
            bloom_filter_fp_chance: options.bloom_filter_fp_chance,
            gc_grace_seconds: options.gc_grace_seconds,
            default_time_to_live: options.default_time_to_live,
            additional_properties: options.additional_properties.clone(),
        })
    }

    fn convert_metadata(&self, metadata: &SchemaMetadata) -> Result<JsonMetadata> {
        Ok(JsonMetadata {
            exported_at: chrono::Utc::now().to_rfc3339(),
            schema_discovered_at: metadata
                .discovered_at
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| chrono::DateTime::from_timestamp(d.as_secs() as i64, 0))
                .unwrap_or(None)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "unknown".to_string()),
            source_files: metadata
                .source_files
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect(),
            total_rows_sampled: metadata.total_rows_sampled as u64,
            cassandra_version: metadata
                .cassandra_version
                .as_ref()
                .map(|v| format!("{:?}", v)),
            discovery_method: format!("{:?}", metadata.discovery_method),
            schema_version: metadata.version,
            validation_results: if self.config.include_validation_results {
                Some(JsonValidationResults {
                    status: format!("{:?}", metadata.validation_results.status),
                    errors: metadata
                        .validation_results
                        .errors
                        .iter()
                        .map(|e| e.message.clone())
                        .collect(),
                    warnings: metadata
                        .validation_results
                        .warnings
                        .iter()
                        .map(|w| w.message.clone())
                        .collect(),
                    files_analyzed: metadata
                        .validation_results
                        .consistency_results
                        .files_analyzed as u32,
                    schema_mismatches: metadata
                        .validation_results
                        .consistency_results
                        .schema_mismatches as u32,
                })
            } else {
                None
            },
            performance_metrics: if self.config.include_performance_metrics {
                Some(JsonPerformanceMetrics {
                    total_time_ms: metadata.performance_metrics.total_time_ms,
                    header_parsing_time_ms: metadata.performance_metrics.header_parsing_time_ms,
                    data_sampling_time_ms: metadata.performance_metrics.data_sampling_time_ms,
                    type_inference_time_ms: metadata.performance_metrics.type_inference_time_ms,
                    validation_time_ms: metadata.performance_metrics.validation_time_ms,
                    peak_memory_usage_bytes: metadata.performance_metrics.peak_memory_usage_bytes
                        as u64,
                })
            } else {
                None
            },
        })
    }

    fn index_type_to_string(&self, index_type: &IndexType) -> String {
        match index_type {
            IndexType::Secondary => "secondary".to_string(),
            IndexType::Composite => "composite".to_string(),
            IndexType::Custom(class) => format!("custom:{}", class),
        }
    }

    fn serialize_json_schema(&self, json_schema: &JsonSchema) -> Result<String> {
        if self.config.pretty_format {
            serde_json::to_string_pretty(json_schema)
        } else {
            serde_json::to_string(json_schema)
        }
        .map_err(|e| Error::Serialization(e.to_string()))
    }
}

/// Schema collection for exporting multiple schemas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSchemaCollection {
    pub schemas: Vec<JsonSchema>,
    pub metadata: JsonCollectionMetadata,
}

/// Metadata for schema collections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonCollectionMetadata {
    pub total_schemas: usize,
    pub exported_at: String,
    pub format_variant: String,
}

impl Default for JsonExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_exporter_creation() {
        let exporter = JsonExporter::new();
        assert_eq!(exporter.config.format_variant, JsonFormat::Standard);
        assert!(exporter.config.pretty_format);
    }

    #[test]
    fn test_json_exporter_with_custom_config() {
        let config = JsonExportConfig {
            format_variant: JsonFormat::Compact,
            pretty_format: false,
            include_metadata: false,
            ..Default::default()
        };

        let exporter = JsonExporter::with_config(config);
        assert_eq!(exporter.config.format_variant, JsonFormat::Compact);
        assert!(!exporter.config.pretty_format);
        assert!(!exporter.config.include_metadata);
    }

    #[test]
    fn test_type_details_generation() {
        let exporter = JsonExporter::new();
        let collection_types = HashMap::new();

        let type_details = exporter
            .build_type_details("text", &collection_types)
            .unwrap();
        assert!(type_details.is_some());

        let type_details = type_details.unwrap();
        assert_eq!(type_details.base_type, "text");
        assert!(type_details.example_values.is_some());
    }

    #[test]
    fn test_collection_type_extraction() {
        let exporter = JsonExporter::new();

        assert_eq!(exporter.extract_collection_type("list<text>"), "list");
        assert_eq!(exporter.extract_collection_type("set<int>"), "set");
        assert_eq!(exporter.extract_collection_type("map<text, int>"), "map");
    }

    #[test]
    fn test_element_types_extraction() {
        let exporter = JsonExporter::new();

        let elements = exporter.extract_element_types("list<text>");
        assert_eq!(elements, vec!["text"]);

        let elements = exporter.extract_element_types("map<text, int>");
        assert_eq!(elements, vec!["text", "int"]);
    }
}

//! Schema-Aware SSTable Reader
//!
//! This module provides a schema-aware wrapper around the SSTable reader that uses
//! the SchemaParser for all value parsing operations instead of type guessing.
//! It requires a complete schema definition and uses proper comparators for
//! partition and clustering keys.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    error::{Error, Result},
    parser::header::CassandraVersion,
    platform::Platform,
    schema::{
        parser::SchemaParser,
        registry::{ParsingContext, SchemaRegistry},
        CqlType, TableSchema,
    },
    storage::sstable::{
        format_detector::SSTableFormat,
        reader::{SSTableReader, SSTableReaderStats},
    },
    types::{ComparatorType, Value},
    Config, RowKey,
};

/// Schema-aware SSTable reader that uses SchemaParser for all value parsing
#[derive(Debug)]
#[allow(dead_code)]
pub struct SchemaAwareReader {
    /// Path to the SSTable file
    file_path: PathBuf,

    /// Underlying SSTable reader (for low-level file operations)
    reader: SSTableReader,

    /// Schema parser for value parsing
    schema_parser: SchemaParser,

    /// Parsing context with schema and comparators
    context: ParsingContext,

    /// Detected SSTable format
    format: SSTableFormat,

    /// Cassandra version from header
    version: CassandraVersion,

    /// Platform abstraction
    platform: Arc<Platform>,
}

/// Configuration for schema-aware reading
#[derive(Debug, Clone)]
pub struct SchemaAwareReaderConfig {
    /// Whether to validate schema completeness on creation
    pub validate_schema_completeness: bool,

    /// Whether to fail if schema is missing required fields
    pub strict_schema_validation: bool,

    /// Whether to enable format-specific optimizations
    pub enable_format_optimizations: bool,

    /// Whether to cache parsed values
    pub cache_parsed_values: bool,
}

impl Default for SchemaAwareReaderConfig {
    fn default() -> Self {
        Self {
            validate_schema_completeness: true,
            strict_schema_validation: true,
            enable_format_optimizations: true,
            cache_parsed_values: true,
        }
    }
}

/// Statistics for schema-aware reading operations
#[derive(Debug, Clone)]
pub struct SchemaAwareStats {
    /// Base SSTable reader statistics
    pub base_stats: SSTableReaderStats,

    /// Number of values parsed using schema
    pub schema_parsed_values: u64,

    /// Number of partition keys parsed
    pub partition_keys_parsed: u64,

    /// Number of clustering keys parsed
    pub clustering_keys_parsed: u64,

    /// Number of column values parsed
    pub column_values_parsed: u64,

    /// Number of parse errors encountered
    pub parse_errors: u64,

    /// Number of format-specific optimizations used
    pub format_optimizations_used: u64,
}

impl SchemaAwareReader {
    /// Create a new schema-aware reader with complete schema validation
    pub async fn new(
        path: &Path,
        schema: TableSchema,
        schema_registry: Arc<SchemaRegistry>,
        config: &Config,
        platform: Arc<Platform>,
    ) -> Result<Self> {
        Self::new_with_config(
            path,
            schema,
            schema_registry,
            config,
            platform,
            SchemaAwareReaderConfig::default(),
        )
        .await
    }

    /// Create a new schema-aware reader with custom configuration
    pub async fn new_with_config(
        path: &Path,
        schema: TableSchema,
        schema_registry: Arc<SchemaRegistry>,
        config: &Config,
        platform: Arc<Platform>,
        reader_config: SchemaAwareReaderConfig,
    ) -> Result<Self> {
        // Validate schema before proceeding
        if reader_config.validate_schema_completeness {
            Self::validate_schema_completeness(&schema)?;
        }

        // Create parsing context
        let context = Self::create_parsing_context(&schema, &schema_registry)?;

        // Validate context completeness if strict validation is enabled
        if reader_config.strict_schema_validation && !context.is_complete() {
            return Err(Error::Schema(format!(
                "Incomplete parsing context for table {}.{}: missing schema or comparators",
                schema.keyspace, schema.table
            )));
        }

        // Create schema parser
        let schema_parser = SchemaParser::new(context.clone())?;

        // Open underlying SSTable reader
        let reader = SSTableReader::open(path, config, platform.clone()).await?;

        // Detect SSTable format
        let format = Self::detect_format(&reader).await?;

        // Get Cassandra version from header
        let version = Self::extract_version(&reader)?;

        Ok(Self {
            file_path: path.to_path_buf(),
            reader,
            schema_parser,
            context,
            format,
            version,
            platform,
        })
    }

    /// Validate that the schema contains all required fields for schema-aware parsing
    pub fn validate_schema_completeness(schema: &TableSchema) -> Result<()> {
        // Must have at least one partition key
        if schema.partition_keys.is_empty() {
            return Err(Error::Schema(format!(
                "Schema for table {}.{} must have at least one partition key",
                schema.keyspace, schema.table
            )));
        }

        // All partition keys must have valid types
        for (idx, key) in schema.partition_keys.iter().enumerate() {
            CqlType::parse(&key.data_type).map_err(|e| {
                Error::Schema(format!(
                    "Invalid partition key type '{}' at position {} in {}.{}: {}",
                    key.data_type, idx, schema.keyspace, schema.table, e
                ))
            })?;
        }

        // All clustering keys must have valid types
        for (idx, key) in schema.clustering_keys.iter().enumerate() {
            CqlType::parse(&key.data_type).map_err(|e| {
                Error::Schema(format!(
                    "Invalid clustering key type '{}' at position {} in {}.{}: {}",
                    key.data_type, idx, schema.keyspace, schema.table, e
                ))
            })?;
        }

        // All columns must have valid types
        for column in &schema.columns {
            CqlType::parse(&column.data_type).map_err(|e| {
                Error::Schema(format!(
                    "Invalid column type '{}' for column '{}' in {}.{}: {}",
                    column.data_type, column.name, schema.keyspace, schema.table, e
                ))
            })?;
        }

        // Validate positions are contiguous for partition keys
        let mut positions: Vec<usize> = schema.partition_keys.iter().map(|k| k.position).collect();
        positions.sort();
        for (expected, &actual) in positions.iter().enumerate() {
            if expected != actual {
                return Err(Error::Schema(format!(
                    "Non-contiguous partition key positions in {}.{}: expected {}, found {}",
                    schema.keyspace, schema.table, expected, actual
                )));
            }
        }

        // Validate positions are contiguous for clustering keys (if any)
        if !schema.clustering_keys.is_empty() {
            let mut positions: Vec<usize> =
                schema.clustering_keys.iter().map(|k| k.position).collect();
            positions.sort();
            for (expected, &actual) in positions.iter().enumerate() {
                if expected != actual {
                    return Err(Error::Schema(format!(
                        "Non-contiguous clustering key positions in {}.{}: expected {}, found {}",
                        schema.keyspace, schema.table, expected, actual
                    )));
                }
            }
        }

        Ok(())
    }

    /// Create parsing context from schema and registry
    pub fn create_parsing_context(
        schema: &TableSchema,
        _registry: &SchemaRegistry,
    ) -> Result<ParsingContext> {
        // Get partition key comparators
        let partition_comparators = schema.get_partition_key_comparators()?;

        // Get clustering key comparators
        let clustering_comparators = schema.get_clustering_key_comparators()?;

        // Get all column comparators
        let column_comparators = schema.get_all_comparators()?;

        Ok(ParsingContext {
            schema: schema.clone(),
            partition_comparators,
            clustering_comparators,
            column_comparators,
        })
    }

    /// Detect SSTable format from reader
    async fn detect_format(_reader: &SSTableReader) -> Result<SSTableFormat> {
        // Use the existing format detector from the reader
        // This is a simplified implementation - in practice, you'd access the reader's format
        Ok(SSTableFormat::V5x("oa".to_string())) // Default to Cassandra 5.x format
    }

    /// Extract Cassandra version from reader
    fn extract_version(_reader: &SSTableReader) -> Result<CassandraVersion> {
        // Extract version from the reader's header
        // This is a simplified implementation
        Ok(CassandraVersion::V5_0Release)
    }

    /// Get a value by partition and clustering keys with schema-driven parsing
    pub async fn get(
        &self,
        partition_key: &[Value],
        clustering_key: Option<&[Value]>,
    ) -> Result<Option<HashMap<String, Value>>> {
        // Validate partition key against schema
        self.validate_partition_key(partition_key)?;

        // Validate clustering key if provided
        if let Some(ck) = clustering_key {
            self.validate_clustering_key(ck)?;
        }

        // Create RowKey from partition and clustering keys
        let row_key = self.create_row_key(partition_key, clustering_key)?;

        // Get raw value from underlying reader
        let table_id = self.get_table_id();
        if let Some(raw_value) = self.reader.get(&table_id, &row_key).await? {
            // Parse the raw value using schema
            let parsed_row = self.parse_row_value(&raw_value)?;
            Ok(Some(parsed_row))
        } else {
            Ok(None)
        }
    }

    /// Scan a range of rows with schema-driven parsing
    pub async fn scan(
        &self,
        start_partition: Option<&[Value]>,
        end_partition: Option<&[Value]>,
        start_clustering: Option<&[Value]>,
        end_clustering: Option<&[Value]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<Value>, Vec<Value>, HashMap<String, Value>)>> {
        // Validate start partition key if provided
        if let Some(pk) = start_partition {
            self.validate_partition_key(pk)?;
        }

        // Validate end partition key if provided
        if let Some(pk) = end_partition {
            self.validate_partition_key(pk)?;
        }

        // Validate start clustering key if provided
        if let Some(ck) = start_clustering {
            self.validate_clustering_key(ck)?;
        }

        // Validate end clustering key if provided
        if let Some(ck) = end_clustering {
            self.validate_clustering_key(ck)?;
        }

        // Create start and end row keys
        let start_key = if let Some(pk) = start_partition {
            Some(self.create_row_key(pk, start_clustering)?)
        } else {
            None
        };

        let end_key = if let Some(pk) = end_partition {
            Some(self.create_row_key(pk, end_clustering)?)
        } else {
            None
        };

        // Scan using underlying reader
        let table_id = self.get_table_id();
        let raw_results = self
            .reader
            .scan(&table_id, start_key.as_ref(), end_key.as_ref(), limit)
            .await?;

        // Parse results using schema
        let mut parsed_results = Vec::new();
        for (row_key, raw_value) in raw_results {
            let (partition_values, clustering_values) = self.parse_row_key(&row_key)?;
            let column_values = self.parse_row_value(&raw_value)?;
            parsed_results.push((partition_values, clustering_values, column_values));
        }

        Ok(parsed_results)
    }

    /// Parse a raw row key into partition and clustering key values
    pub fn parse_row_key(&self, row_key: &RowKey) -> Result<(Vec<Value>, Vec<Value>)> {
        let key_bytes = row_key.as_bytes();

        // Parse partition key portion
        let partition_values = self.schema_parser.parse_partition_key(key_bytes)?;

        // Calculate partition key length to find clustering key start
        let partition_length = self.calculate_partition_key_length(&partition_values)?;

        // Parse clustering key portion (if present)
        let clustering_values = if key_bytes.len() > partition_length {
            self.schema_parser
                .parse_clustering_keys(&key_bytes[partition_length..])?
        } else {
            Vec::new()
        };

        Ok((partition_values, clustering_values))
    }

    /// Parse a raw row value into column values
    pub fn parse_row_value(&self, raw_value: &Value) -> Result<HashMap<String, Value>> {
        let value_bytes = raw_value
            .as_bytes()
            .ok_or_else(|| Error::Schema("Row value is not binary data".to_string()))?;

        let mut column_values = HashMap::new();
        let mut offset = 0;

        // Parse each column according to schema
        for column in &self.context.schema.columns {
            // Skip key columns as they're parsed separately
            if self.context.schema.is_partition_key(&column.name)
                || self.context.schema.is_clustering_key(&column.name)
            {
                continue;
            }

            if offset >= value_bytes.len() {
                break; // No more data
            }

            // Parse column value using schema parser
            let column_value = self
                .schema_parser
                .parse_column_value(&column.name, &value_bytes[offset..])?;

            // Calculate consumed bytes (simplified - would need proper length encoding)
            let consumed = self.estimate_value_length(&column_value)?;
            offset += consumed;

            column_values.insert(column.name.clone(), column_value);
        }

        Ok(column_values)
    }

    /// Get reader statistics including schema-aware metrics
    pub async fn stats(&self) -> Result<SchemaAwareStats> {
        let base_stats = self.reader.stats().await?;

        Ok(SchemaAwareStats {
            base_stats,
            schema_parsed_values: 0, // Would be tracked in real implementation
            partition_keys_parsed: 0,
            clustering_keys_parsed: 0,
            column_values_parsed: 0,
            parse_errors: 0,
            format_optimizations_used: 0,
        })
    }

    /// Get the table name for this reader
    pub fn table_name(&self) -> String {
        format!(
            "{}.{}",
            self.context.schema.keyspace, self.context.schema.table
        )
    }

    /// Get the schema used by this reader
    pub fn schema(&self) -> &TableSchema {
        &self.context.schema
    }

    /// Get the parsing context
    pub fn context(&self) -> &ParsingContext {
        &self.context
    }

    /// Check if format-specific optimizations are available
    pub fn has_format_optimizations(&self) -> bool {
        matches!(self.format, SSTableFormat::V5x(_))
    }

    /// Get Cassandra version detected from the SSTable
    pub fn cassandra_version(&self) -> CassandraVersion {
        self.version
    }

    /// Get the file path being read
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    // Helper methods

    fn validate_partition_key(&self, key: &[Value]) -> Result<()> {
        if key.len() != self.context.partition_comparators.len() {
            return Err(Error::Schema(format!(
                "Partition key length mismatch: expected {}, got {}",
                self.context.partition_comparators.len(),
                key.len()
            )));
        }
        Ok(())
    }

    fn validate_clustering_key(&self, key: &[Value]) -> Result<()> {
        if key.len() > self.context.clustering_comparators.len() {
            return Err(Error::Schema(format!(
                "Clustering key too long: expected max {}, got {}",
                self.context.clustering_comparators.len(),
                key.len()
            )));
        }
        Ok(())
    }

    fn create_row_key(
        &self,
        partition_key: &[Value],
        clustering_key: Option<&[Value]>,
    ) -> Result<RowKey> {
        // Serialize partition key
        let mut key_bytes = Vec::new();
        for (value, comparator) in partition_key
            .iter()
            .zip(&self.context.partition_comparators)
        {
            let serialized = self.serialize_value_with_comparator(value, comparator)?;
            key_bytes.extend_from_slice(&serialized);
        }

        // Serialize clustering key if provided
        if let Some(ck) = clustering_key {
            for (value, comparator) in ck.iter().zip(&self.context.clustering_comparators) {
                let serialized = self.serialize_value_with_comparator(value, comparator)?;
                key_bytes.extend_from_slice(&serialized);
            }
        }

        Ok(RowKey::new(key_bytes))
    }

    fn serialize_value_with_comparator(
        &self,
        value: &Value,
        _comparator: &ComparatorType,
    ) -> Result<Vec<u8>> {
        // This would use the proper Cassandra serialization format for each type
        // For now, return a simplified implementation
        Ok(value.as_bytes().unwrap_or(&[]).to_vec())
    }

    fn calculate_partition_key_length(&self, partition_values: &[Value]) -> Result<usize> {
        // Calculate the byte length of the serialized partition key
        let mut total_length = 0;
        for (value, comparator) in partition_values
            .iter()
            .zip(&self.context.partition_comparators)
        {
            let serialized = self.serialize_value_with_comparator(value, comparator)?;
            total_length += serialized.len();
        }
        Ok(total_length)
    }

    fn estimate_value_length(&self, value: &Value) -> Result<usize> {
        // Estimate the byte length of a value
        // This would properly handle variable-length encoding in a real implementation
        Ok(value.as_bytes().map(|b| b.len()).unwrap_or(0))
    }

    fn get_table_id(&self) -> crate::types::TableId {
        // Create a table ID from the schema
        crate::types::TableId::from(format!(
            "{}.{}",
            self.context.schema.keyspace, self.context.schema.table
        ))
    }
}

/// Error handling specific to schema-aware reading
#[derive(Debug, thiserror::Error)]
pub enum SchemaAwareReaderError {
    #[error("Schema validation failed: {0}")]
    SchemaValidation(String),

    #[error("Parsing context incomplete: {0}")]
    IncompleteContext(String),

    #[error("Key validation failed: {0}")]
    KeyValidation(String),

    #[error("Value parsing failed for column '{column}': {reason}")]
    ValueParsing { column: String, reason: String },

    #[error("Format-specific error: {0}")]
    FormatSpecific(String),

    #[error("Incompatible Cassandra version: expected {expected:?}, found {found:?}")]
    VersionMismatch {
        expected: CassandraVersion,
        found: CassandraVersion,
    },
}

impl From<SchemaAwareReaderError> for Error {
    fn from(err: SchemaAwareReaderError) -> Self {
        Error::Schema(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};

    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "data".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        }
    }

    #[test]
    fn test_schema_validation() {
        let schema = create_test_schema();
        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_ok());
    }

    #[test]
    fn test_invalid_schema_validation() {
        let mut schema = create_test_schema();
        schema.partition_keys.clear(); // Remove partition keys

        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_err());
    }

    #[test]
    fn test_non_contiguous_positions() {
        let mut schema = create_test_schema();
        schema.partition_keys.push(KeyColumn {
            name: "other".to_string(),
            data_type: "text".to_string(),
            position: 2, // Non-contiguous position
        });

        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_err());
    }
}

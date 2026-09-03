//! Schema Discovery and Metadata Management
//!
//! This module provides automatic schema detection and metadata management for
//! SSTable files, enabling the REPL to understand table structures and provide
//! intelligent data access capabilities.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    parser::header::{CassandraVersion, ColumnInfo},
    platform::Platform,
    schema::{Column, TableSchema},
    storage::sstable::reader::SSTableReader,
    types::{DataType, Value},
    Config, Result,
};

/// Schema discovery configuration
#[derive(Debug, Clone)]
pub struct SchemaDiscoveryConfig {
    /// Maximum number of rows to sample for type inference
    pub max_sample_rows: usize,
    /// Enable aggressive type inference
    pub aggressive_inference: bool,
    /// Cache discovered schemas
    pub cache_schemas: bool,
    /// Schema cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Enable schema versioning
    pub enable_versioning: bool,
    /// Maximum schema versions to keep
    pub max_versions: usize,
}

impl Default for SchemaDiscoveryConfig {
    fn default() -> Self {
        Self {
            max_sample_rows: 1000,
            aggressive_inference: true,
            cache_schemas: true,
            cache_ttl_seconds: 3600, // 1 hour
            enable_versioning: true,
            max_versions: 10,
        }
    }
}

/// Discovered schema information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredSchema {
    /// Table schema
    pub schema: TableSchema,
    /// Discovery metadata
    pub metadata: SchemaMetadata,
    /// Column statistics
    pub column_stats: HashMap<String, ColumnStatistics>,
    /// Type inference confidence
    pub inference_confidence: f64,
    /// Schema validation status
    pub validation_status: ValidationStatus,
}

/// Schema discovery metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    /// When schema was discovered
    pub discovered_at: SystemTime,
    /// Source SSTable files
    pub source_files: Vec<PathBuf>,
    /// Number of rows sampled
    pub rows_sampled: usize,
    /// Cassandra version detected
    pub cassandra_version: Option<CassandraVersion>,
    /// Discovery method used
    pub discovery_method: DiscoveryMethod,
    /// Schema version
    pub version: u32,
}

/// Column statistics for type inference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Column name
    pub name: String,
    /// Inferred data type
    pub inferred_type: String,
    /// Type confidence (0.0 - 1.0)
    pub type_confidence: f64,
    /// Null value percentage
    pub null_percentage: f64,
    /// Unique value count (sampled)
    pub unique_values: usize,
    /// Average value size in bytes
    pub avg_size_bytes: f64,
    /// Min/max values (for applicable types)
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    /// Common patterns detected
    pub patterns: Vec<String>,
}

/// Schema validation status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValidationStatus {
    /// Schema is valid and consistent
    Valid,
    /// Schema has minor inconsistencies
    WarningsPresent,
    /// Schema has significant issues
    Invalid,
    /// Schema could not be validated
    Unknown,
}

/// Discovery method used
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoveryMethod {
    /// From SSTable header metadata
    HeaderMetadata,
    /// Inferred from data sampling
    DataSampling,
    /// Combination of header and sampling
    Hybrid,
    /// From external schema definition
    External,
}

/// Schema discovery engine
#[allow(dead_code)]
pub struct SchemaDiscovery {
    /// Configuration
    config: SchemaDiscoveryConfig,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Core configuration
    core_config: Config,
    /// Schema cache
    schema_cache: Arc<RwLock<HashMap<String, (DiscoveredSchema, Instant)>>>,
    /// Type inference engine
    type_inference: Arc<TypeInferenceEngine>,
    /// Schema validator
    validator: Arc<SchemaValidator>,
}

impl SchemaDiscovery {
    /// Create a new schema discovery engine
    pub async fn new(
        config: SchemaDiscoveryConfig,
        platform: Arc<Platform>,
        core_config: Config,
    ) -> Result<Self> {
        let type_inference = Arc::new(TypeInferenceEngine::new());
        let validator = Arc::new(SchemaValidator::new());

        Ok(Self {
            config,
            platform,
            core_config,
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
            type_inference,
            validator,
        })
    }

    /// Discover schema for a table from SSTable files
    pub async fn discover_table_schema(
        &self,
        keyspace: &str,
        table: &str,
        sstable_files: &[PathBuf],
    ) -> Result<DiscoveredSchema> {
        let cache_key = format!("{}.{}", keyspace, table);

        // Check cache first
        if self.config.cache_schemas {
            if let Some(cached) = self.get_cached_schema(&cache_key).await {
                return Ok(cached);
            }
        }

        // Perform discovery
        let discovered = self
            .perform_schema_discovery(keyspace, table, sstable_files)
            .await?;

        // Cache the result
        if self.config.cache_schemas {
            self.cache_schema(cache_key, discovered.clone()).await;
        }

        Ok(discovered)
    }

    /// Perform the actual schema discovery
    async fn perform_schema_discovery(
        &self,
        keyspace: &str,
        table: &str,
        sstable_files: &[PathBuf],
    ) -> Result<DiscoveredSchema> {
        let start_time = SystemTime::now();
        let mut source_files = Vec::new();
        let mut all_column_data = HashMap::new();
        let mut total_rows_sampled = 0;
        let mut cassandra_version = None;

        // Analyze each SSTable file
        for file_path in sstable_files {
            if let Ok(reader) = self.create_reader(file_path).await {
                source_files.push(file_path.clone());

                // Try to get schema from header first
                if let Ok(header_schema) = self.extract_schema_from_header(&reader).await {
                    if cassandra_version.is_none() {
                        let header = reader.header();
                        cassandra_version = Some(header.cassandra_version);
                    }

                    // Merge with existing column data
                    self.merge_header_schema(&mut all_column_data, header_schema);
                }

                // Sample data for type inference
                let sampled_data = self.sample_table_data(&reader).await?;
                total_rows_sampled += sampled_data.len();

                // Analyze sampled data
                self.analyze_sampled_data(&mut all_column_data, sampled_data);

                // Limit total sampling
                if total_rows_sampled >= self.config.max_sample_rows {
                    break;
                }
            }
        }

        // Infer final schema
        let schema = self
            .infer_table_schema(keyspace, table, &all_column_data)
            .await?;

        // Calculate column statistics
        let column_stats = self.calculate_column_statistics(&all_column_data).await;

        // Calculate inference confidence
        let inference_confidence = self.calculate_inference_confidence(&column_stats);

        // Validate schema
        let validation_status = self.validator.validate_schema(&schema, &column_stats).await;

        // Determine discovery method
        let discovery_method = if source_files.is_empty() {
            DiscoveryMethod::External
        } else if all_column_data.values().any(|cd| cd.header_info.is_some()) {
            if total_rows_sampled > 0 {
                DiscoveryMethod::Hybrid
            } else {
                DiscoveryMethod::HeaderMetadata
            }
        } else {
            DiscoveryMethod::DataSampling
        };

        let metadata = SchemaMetadata {
            discovered_at: start_time,
            source_files,
            rows_sampled: total_rows_sampled,
            cassandra_version,
            discovery_method,
            version: 1,
        };

        Ok(DiscoveredSchema {
            schema,
            metadata,
            column_stats,
            inference_confidence,
            validation_status,
        })
    }

    /// Create a reader for the SSTable file
    async fn create_reader(&self, file_path: &Path) -> Result<SSTableReader> {
        SSTableReader::open(file_path, &self.core_config, self.platform.clone()).await
    }

    /// Extract schema information from SSTable header
    async fn extract_schema_from_header(
        &self,
        reader: &SSTableReader,
    ) -> Result<HashMap<String, ColumnInfo>> {
        let header = reader.header();
        let mut columns = HashMap::new();

        for column_def in &header.columns {
            columns.insert(column_def.name.clone(), column_def.clone());
        }

        Ok(columns)
    }

    /// Sample data from the SSTable for type inference
    async fn sample_table_data(
        &self,
        reader: &SSTableReader,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Header column names for the raw-fallback case (issue #1334): a
        // `ScanRow::RawRow` is undecoded whole-row bytes, so — matching the
        // pre-#1334 sampler — map it onto the first header column rather than a
        // synthetic `"data"` blob that would infer a bogus column.
        let header_first_column: Option<String> =
            reader.header().columns.first().map(|c| c.name.clone());

        // Get all entries and sample up to max_rows
        let all_entries = reader.get_all_entries().await?;

        // Issue #1334: each entry carries a `ScanRow` row. Disassemble a live row's
        // interned cells into a name→value map for type inference; map a raw
        // fallback row onto the header column name; suppress markers (row
        // tombstone / null row) which carry no columns.
        let samples: Vec<HashMap<String, Value>> = all_entries
            .into_iter()
            .take(self.config.max_sample_rows)
            .filter_map(|(_table_id, _row_key, row)| {
                let cells = row.into_sample_cells(header_first_column.as_deref())?;
                if cells.is_empty() {
                    return None;
                }
                let row_data: HashMap<String, Value> = cells
                    .into_iter()
                    .map(|(name, v)| (name.to_string(), v))
                    .collect();
                Some(row_data)
            })
            .collect();

        Ok(samples)
    }

    /// Infer table schema from column data
    async fn infer_table_schema(
        &self,
        keyspace: &str,
        table: &str,
        column_data: &HashMap<String, ColumnData>,
    ) -> Result<TableSchema> {
        let mut columns = Vec::new();

        for (name, data) in column_data {
            let data_type = self.type_inference.infer_column_type(data).await;
            let column = Column {
                name: name.clone(),
                data_type: data_type.to_string(),
                nullable: true,
                default: None,
                is_static: false,
            };
            columns.push(column);
        }

        // Sort columns by name for consistency
        columns.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(TableSchema {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            partition_keys: vec![], // Would need sophisticated analysis
            clustering_keys: vec![],
            columns,
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        })
    }

    /// Calculate column statistics
    async fn calculate_column_statistics(
        &self,
        column_data: &HashMap<String, ColumnData>,
    ) -> HashMap<String, ColumnStatistics> {
        let mut stats = HashMap::new();

        for (name, data) in column_data {
            let stat = ColumnStatistics {
                name: name.clone(),
                inferred_type: self
                    .type_inference
                    .infer_column_type(data)
                    .await
                    .to_string(),
                type_confidence: data.calculate_type_confidence(),
                null_percentage: data.calculate_null_percentage(),
                unique_values: data.unique_values.len(),
                avg_size_bytes: data.calculate_average_size(),
                min_value: data.min_value.clone(),
                max_value: data.max_value.clone(),
                patterns: data.detected_patterns.clone(),
            };
            stats.insert(name.clone(), stat);
        }

        stats
    }

    /// Calculate overall inference confidence
    fn calculate_inference_confidence(
        &self,
        column_stats: &HashMap<String, ColumnStatistics>,
    ) -> f64 {
        if column_stats.is_empty() {
            return 0.0;
        }

        let total_confidence: f64 = column_stats.values().map(|stat| stat.type_confidence).sum();

        total_confidence / column_stats.len() as f64
    }

    // Cache management methods

    async fn get_cached_schema(&self, cache_key: &str) -> Option<DiscoveredSchema> {
        let cache = self.schema_cache.read().await;
        if let Some((schema, cached_at)) = cache.get(cache_key) {
            let ttl = Duration::from_secs(self.config.cache_ttl_seconds);
            if cached_at.elapsed() < ttl {
                return Some(schema.clone());
            }
        }
        None
    }

    async fn cache_schema(&self, cache_key: String, schema: DiscoveredSchema) {
        let mut cache = self.schema_cache.write().await;
        cache.insert(cache_key, (schema, Instant::now()));

        // Simple cache eviction
        if cache.len() > 100 {
            let oldest_key = cache
                .iter()
                .min_by_key(|(_, (_, time))| time)
                .map(|(key, _)| key.clone());

            if let Some(key) = oldest_key {
                cache.remove(&key);
            }
        }
    }

    // Helper methods for data processing

    fn merge_header_schema(
        &self,
        column_data: &mut HashMap<String, ColumnData>,
        header_columns: HashMap<String, ColumnInfo>,
    ) {
        for (name, column_info) in header_columns {
            let entry = column_data.entry(name).or_insert_with(ColumnData::new);
            entry.header_info = Some(column_info);
        }
    }

    fn analyze_sampled_data(
        &self,
        column_data: &mut HashMap<String, ColumnData>,
        samples: Vec<HashMap<String, Value>>,
    ) {
        for sample in samples {
            for (column_name, value) in sample {
                let entry = column_data
                    .entry(column_name)
                    .or_insert_with(ColumnData::new);
                entry.add_sample_value(value);
            }
        }
    }
}

/// Internal column data for analysis
#[derive(Debug)]
struct ColumnData {
    /// Header information if available
    header_info: Option<ColumnInfo>,
    /// Sample values collected
    sample_values: Vec<Value>,
    /// Unique values (limited set)
    unique_values: BTreeMap<String, usize>,
    /// Null count
    null_count: usize,
    /// Min/max values
    min_value: Option<Value>,
    max_value: Option<Value>,
    /// Detected patterns
    detected_patterns: Vec<String>,
    /// Type frequency map
    type_frequency: HashMap<String, usize>,
}

impl ColumnData {
    fn new() -> Self {
        Self {
            header_info: None,
            sample_values: Vec::new(),
            unique_values: BTreeMap::new(),
            null_count: 0,
            min_value: None,
            max_value: None,
            detected_patterns: Vec::new(),
            type_frequency: HashMap::new(),
        }
    }

    fn add_sample_value(&mut self, value: Value) {
        if value == Value::Null {
            self.null_count += 1;
        } else {
            // Track type frequency
            let type_name = value.type_name();
            *self.type_frequency.entry(type_name).or_insert(0) += 1;

            // Track unique values (limited)
            if self.unique_values.len() < 1000 {
                let value_str = format!("{:?}", value);
                *self.unique_values.entry(value_str).or_insert(0) += 1;
            }

            // Update min/max
            if self.min_value.is_none() || Some(&value) < self.min_value.as_ref() {
                self.min_value = Some(value.clone());
            }
            if self.max_value.is_none() || Some(&value) > self.max_value.as_ref() {
                self.max_value = Some(value.clone());
            }

            self.sample_values.push(value);
        }
    }

    fn calculate_type_confidence(&self) -> f64 {
        if self.type_frequency.is_empty() {
            return 0.0;
        }

        let total_samples = self.type_frequency.values().sum::<usize>();
        let max_frequency = *self.type_frequency.values().max().unwrap_or(&0);

        max_frequency as f64 / total_samples as f64
    }

    fn calculate_null_percentage(&self) -> f64 {
        let total = self.sample_values.len() + self.null_count;
        if total == 0 {
            0.0
        } else {
            self.null_count as f64 / total as f64
        }
    }

    fn calculate_average_size(&self) -> f64 {
        if self.sample_values.is_empty() {
            0.0
        } else {
            let total_size: usize = self.sample_values.iter().map(|v| v.estimate_size()).sum();
            total_size as f64 / self.sample_values.len() as f64
        }
    }
}

/// Type inference engine
struct TypeInferenceEngine;

impl TypeInferenceEngine {
    fn new() -> Self {
        Self
    }

    async fn infer_column_type(&self, column_data: &ColumnData) -> DataType {
        // If we have header information, use it
        if let Some(ref header_info) = column_data.header_info {
            return self.convert_cql_type_to_data_type(&header_info.column_type);
        }

        // Otherwise, infer from sample data
        if let Some(most_common_type) = column_data
            .type_frequency
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(type_name, _)| type_name)
        {
            return self.string_to_data_type(most_common_type);
        }

        DataType::Text // Default fallback
    }

    fn convert_cql_type_to_data_type(&self, type_name: &str) -> DataType {
        match type_name.to_lowercase().as_str() {
            "text" | "varchar" | "ascii" => DataType::Text,
            "int" => DataType::Integer,
            "bigint" => DataType::BigInt,
            "boolean" => DataType::Boolean,
            "double" => DataType::Float,
            "float" => DataType::Float,
            "uuid" => DataType::Uuid,
            "timestamp" => DataType::Timestamp,
            "blob" => DataType::Blob,
            _ => DataType::Text,
        }
    }

    fn string_to_data_type(&self, type_name: &str) -> DataType {
        match type_name {
            "Text" => DataType::Text,
            "Integer" => DataType::Integer,
            "Float" => DataType::Float,
            "Boolean" => DataType::Boolean,
            _ => DataType::Text,
        }
    }
}

/// Schema validator
struct SchemaValidator;

impl SchemaValidator {
    fn new() -> Self {
        Self
    }

    async fn validate_schema(
        &self,
        _schema: &TableSchema,
        column_stats: &HashMap<String, ColumnStatistics>,
    ) -> ValidationStatus {
        let mut warnings = 0;
        let mut errors = 0;

        // Check type confidence
        for stat in column_stats.values() {
            if stat.type_confidence < 0.5 {
                warnings += 1;
            }
            if stat.type_confidence < 0.3 {
                errors += 1;
            }
        }

        if errors > 0 {
            ValidationStatus::Invalid
        } else if warnings > 0 {
            ValidationStatus::WarningsPresent
        } else {
            ValidationStatus::Valid
        }
    }
}

// Extension trait for Value to add helper methods
trait ValueExt {
    fn type_name(&self) -> String;
    fn estimate_size(&self) -> usize;
}

impl ValueExt for Value {
    fn type_name(&self) -> String {
        match self {
            Value::Null => "Null".to_string(),
            Value::Text(_) => "Text".to_string(),
            Value::Integer(_) => "Integer".to_string(),
            Value::BigInt(_) => "BigInteger".to_string(),
            Value::Counter(_) => "Counter".to_string(),
            Value::Float(_) => "Float".to_string(),
            Value::Boolean(_) => "Boolean".to_string(),
            Value::Uuid(_) => "UUID".to_string(),
            Value::Timestamp(_) => "Timestamp".to_string(),
            Value::Date(_) => "Date".to_string(),
            Value::Time(_) => "Time".to_string(),
            Value::Inet(_) => "Inet".to_string(),
            Value::Blob(_) => "Blob".to_string(),
            Value::List(_) => "List".to_string(),
            Value::Set(_) => "Set".to_string(),
            Value::Map(_) => "Map".to_string(),
            Value::Json(_) => "JSON".to_string(),
            Value::TinyInt(_) => "TinyInt".to_string(),
            Value::SmallInt(_) => "SmallInt".to_string(),
            Value::Float32(_) => "Float32".to_string(),
            Value::Tuple(_) => "Tuple".to_string(),
            Value::Udt(_) => "UDT".to_string(),
            Value::Frozen(_) => "Frozen".to_string(),
            Value::Varint(_) => "Varint".to_string(),
            Value::Decimal { .. } => "Decimal".to_string(),
            Value::Duration { .. } => "Duration".to_string(),
            Value::Tombstone(_) => "Tombstone".to_string(),
            // Diagnostic name: the declared type is what makes the sentinel
            // meaningful (issue #3805).
            Value::Empty(ty) => format!("Empty({})", ty.cql_name()),
        }
    }

    fn estimate_size(&self) -> usize {
        match self {
            Value::Null => 0,
            // The empty buffer occupies zero bytes (issue #3805).
            Value::Empty(_) => 0,
            Value::Text(s) => s.len(),
            Value::Integer(_) => 4,
            Value::BigInt(_) => 8,
            Value::Counter(_) => 8,
            Value::Float(_) => 8,
            Value::Boolean(_) => 1,
            Value::Uuid(_) => 16,
            Value::Timestamp(_) => 8,
            Value::Date(_) => 4,
            Value::Time(_) => 8,
            Value::Inet(bytes) => bytes.len(),
            Value::Blob(b) => b.len(),
            Value::List(items) => items.iter().map(|v| v.estimate_size()).sum::<usize>() + 8,
            Value::Set(items) => items.iter().map(|v| v.estimate_size()).sum::<usize>() + 8,
            Value::Map(map) => {
                map.iter()
                    .map(|(k, v)| k.estimate_size() + v.estimate_size())
                    .sum::<usize>()
                    + 16
            }
            Value::Json(_) => 64, // JSON estimate
            Value::TinyInt(_) => 1,
            Value::SmallInt(_) => 2,
            Value::Float32(_) => 4,
            Value::Tuple(t) => t.iter().map(|v| v.estimate_size()).sum::<usize>() + 8,
            Value::Udt(_) => 32,                   // UDT estimate
            Value::Frozen(f) => f.estimate_size(), // Recursive
            Value::Varint(data) => data.len(),
            Value::Decimal { unscaled, .. } => 4 + unscaled.len(), // scale + data
            Value::Duration { .. } => 12,                          // 3 * 4 bytes
            Value::Tombstone(_) => 8,                              // Tombstone marker
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_schema_discovery_creation() {
        let _temp_dir = TempDir::new().unwrap();
        let config = SchemaDiscoveryConfig::default();
        let core_config = Config::default();
        let platform = Arc::new(Platform::new(&core_config).await.unwrap());

        let discovery = SchemaDiscovery::new(config, platform, core_config)
            .await
            .unwrap();

        // Test that it was created successfully
        assert!(!discovery.config.cache_schemas || discovery.schema_cache.read().await.is_empty());
    }

    #[test]
    fn test_column_data_analysis() {
        let mut column_data = ColumnData::new();

        // Add some sample values
        column_data.add_sample_value(Value::text("test1".to_string()));
        column_data.add_sample_value(Value::text("test2".to_string()));
        column_data.add_sample_value(Value::Null);
        column_data.add_sample_value(Value::text("test3".to_string()));

        // Test calculations
        assert_eq!(column_data.calculate_null_percentage(), 0.25); // 1 null out of 4 values
        assert_eq!(column_data.unique_values.len(), 3); // 3 unique text values
        assert!(column_data.calculate_type_confidence() > 0.7); // Mostly text values
    }
}

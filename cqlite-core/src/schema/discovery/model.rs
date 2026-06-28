//! Shared data model for schema discovery.
//!
//! Configuration, the discovered-schema representation, type metadata, and
//! validation/metric result types used across the discovery submodules.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{parser::header::CassandraVersion, schema::ClusteringColumn, types::Value};

/// Enhanced schema discovery configuration
#[derive(Debug, Clone)]
pub struct SchemaDiscoveryConfig {
    /// Maximum number of rows to sample for type inference
    pub max_sample_rows: usize,
    /// Enable aggressive type inference
    pub aggressive_inference: bool,
    /// Cache discovered schemas
    pub enable_schema_cache: bool,
    /// Schema cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Enable schema versioning
    pub enable_versioning: bool,
    /// Maximum schema versions to keep
    pub max_versions: usize,
    /// Enable UDT discovery
    pub enable_udt_discovery: bool,
    /// Enable collection type analysis
    pub enable_collection_analysis: bool,
    /// Enable index discovery
    pub enable_index_discovery: bool,
    /// Enable cross-file validation
    pub enable_cross_file_validation: bool,
    /// Minimum confidence threshold for type inference
    pub min_confidence_threshold: f64,
}

impl Default for SchemaDiscoveryConfig {
    fn default() -> Self {
        Self {
            max_sample_rows: 2000,
            aggressive_inference: true,
            enable_schema_cache: true,
            cache_ttl_seconds: 3600, // 1 hour
            enable_versioning: true,
            max_versions: 10,
            enable_udt_discovery: true,
            enable_collection_analysis: true,
            enable_index_discovery: true,
            enable_cross_file_validation: true,
            min_confidence_threshold: 0.7,
        }
    }
}

/// Comprehensive schema information extracted from SSTables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Keyspace name
    pub keyspace: String,
    /// Table name
    pub table: String,
    /// Partition key columns with ordering
    pub partition_key: Vec<ColumnDefinition>,
    /// Clustering key columns with ordering
    pub clustering_keys: Vec<ClusteringColumn>,
    /// Regular data columns
    pub regular_columns: Vec<ColumnDefinition>,
    /// Static columns (if any)
    pub static_columns: Vec<ColumnDefinition>,
    /// Collection type definitions
    pub collection_types: HashMap<String, CollectionType>,
    /// User-defined type definitions
    pub user_defined_types: Vec<UDTDefinition>,
    /// Secondary index definitions
    pub indexes: Vec<IndexDefinition>,
    /// Table configuration options
    pub table_options: TableOptions,
    /// Schema discovery metadata
    pub metadata: SchemaMetadata,
}

/// Enhanced column definition with Cassandra-specific details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// CQL data type
    pub data_type: String,
    /// Parsed type information
    pub type_info: TypeInfo,
    /// Whether column accepts null values
    pub nullable: bool,
    /// Whether column is static (for clustering tables)
    pub is_static: bool,
    /// Default value if specified
    pub default_value: Option<Value>,
    /// Column position in table definition
    pub position: usize,
    /// Type inference confidence (0.0 - 1.0)
    pub confidence: f64,
}

/// Detailed type information for complex types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInfo {
    /// Base type ID as string representation
    pub type_id: String,
    /// Type parameters for generic types
    pub type_params: Vec<String>,
    /// Whether type is frozen
    pub is_frozen: bool,
    /// Element type for collections
    pub element_type: Option<Box<TypeInfo>>,
    /// Key type for maps
    pub key_type: Option<Box<TypeInfo>>,
    /// Value type for maps
    pub value_type: Option<Box<TypeInfo>>,
    /// UDT field definitions if this is a UDT
    pub udt_fields: Option<Vec<UdtFieldInfo>>,
    /// Tuple element types if this is a tuple
    pub tuple_elements: Option<Vec<TypeInfo>>,
}

/// UDT field information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdtFieldInfo {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: TypeInfo,
    /// Whether field is nullable
    pub nullable: bool,
}

/// Collection type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionType {
    /// Collection kind (list, set, map)
    pub kind: CollectionKind,
    /// Element type for lists and sets
    pub element_type: Option<String>,
    /// Key type for maps
    pub key_type: Option<String>,
    /// Value type for maps
    pub value_type: Option<String>,
    /// Whether the collection is frozen
    pub is_frozen: bool,
}

/// Collection kind enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CollectionKind {
    List,
    Set,
    Map,
    Tuple,
}

/// User-defined type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UDTDefinition {
    /// UDT name
    pub name: String,
    /// Keyspace where UDT is defined
    pub keyspace: String,
    /// Field definitions
    pub fields: Vec<UdtFieldDefinition>,
    /// Version when UDT was created
    pub version: Option<u32>,
}

/// UDT field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdtFieldDefinition {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: String,
    /// Field position
    pub position: usize,
    /// Whether field is nullable
    pub nullable: bool,
}

/// Secondary index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name
    pub name: String,
    /// Target column
    pub target_column: String,
    /// Index type
    pub index_type: IndexType,
    /// Index options
    pub options: HashMap<String, String>,
}

/// Index type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    /// Regular secondary index
    Secondary,
    /// Composite index
    Composite,
    /// Custom index
    Custom(String),
}

/// Table configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOptions {
    /// Compaction strategy
    pub compaction: Option<CompactionStrategy>,
    /// Compression options
    pub compression: Option<CompressionOptions>,
    /// Cache settings
    pub caching: Option<CachingOptions>,
    /// Bloom filter settings
    pub bloom_filter_fp_chance: Option<f64>,
    /// GC grace seconds
    pub gc_grace_seconds: Option<u32>,
    /// Default time to live
    pub default_time_to_live: Option<u32>,
    /// Memtable flush period
    pub memtable_flush_period_in_ms: Option<u32>,
    /// Additional properties
    pub additional_properties: HashMap<String, String>,
}

/// Compaction strategy information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionStrategy {
    /// Strategy class name
    pub class: String,
    /// Strategy options
    pub options: HashMap<String, String>,
}

/// Compression options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionOptions {
    /// Compression algorithm
    pub algorithm: String,
    /// Chunk length
    pub chunk_length_kb: Option<u32>,
    /// CRC check chance
    pub crc_check_chance: Option<f32>,
}

/// Caching options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachingOptions {
    /// Keys cache setting
    pub keys: String,
    /// Rows cache setting
    pub rows_per_partition: String,
}

/// Schema discovery metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    /// When schema was discovered
    pub discovered_at: SystemTime,
    /// Source SSTable files
    pub source_files: Vec<PathBuf>,
    /// Total rows sampled across all files
    pub total_rows_sampled: usize,
    /// Cassandra version detected
    pub cassandra_version: Option<CassandraVersion>,
    /// Discovery method used
    pub discovery_method: DiscoveryMethod,
    /// Schema version
    pub version: u32,
    /// Validation results
    pub validation_results: ValidationResults,
    /// Discovery performance metrics
    pub performance_metrics: DiscoveryMetrics,
}

/// Schema discovery method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoveryMethod {
    /// Extracted from SSTable header metadata
    HeaderMetadata,
    /// Inferred from data sampling and analysis
    DataSampling,
    /// Combination of header metadata and data sampling
    Hybrid,
    /// From external schema definition file
    External,
}

/// Schema validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResults {
    /// Overall validation status
    pub status: ValidationStatus,
    /// Validation errors
    pub errors: Vec<ValidationError>,
    /// Validation warnings
    pub warnings: Vec<ValidationWarning>,
    /// Cross-file consistency results
    pub consistency_results: ConsistencyResults,
}

/// Validation status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValidationStatus {
    /// Schema is valid and consistent
    Valid,
    /// Schema has minor issues but is usable
    ValidWithWarnings,
    /// Schema has significant issues
    Invalid,
    /// Schema validation failed
    ValidationFailed,
}

/// Validation error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Error type
    pub error_type: ValidationErrorType,
    /// Error message
    pub message: String,
    /// Affected column or component
    pub component: Option<String>,
    /// Source file where error was found
    pub source_file: Option<PathBuf>,
}

/// Validation error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationErrorType {
    /// Type mismatch between files
    TypeMismatch,
    /// Missing required component
    MissingComponent,
    /// Invalid type definition
    InvalidTypeDefinition,
    /// Constraint violation
    ConstraintViolation,
    /// UDT definition inconsistency
    UdtInconsistency,
    /// Index definition error
    IndexError,
}

/// Validation warning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    /// Warning type
    pub warning_type: ValidationWarningType,
    /// Warning message
    pub message: String,
    /// Affected component
    pub component: Option<String>,
}

/// Validation warning types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationWarningType {
    /// Low confidence type inference
    LowConfidence,
    /// Deprecated feature usage
    DeprecatedFeature,
    /// Version compatibility issue
    VersionCompatibility,
    /// Performance recommendation
    PerformanceRecommendation,
}

/// Cross-file consistency results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistencyResults {
    /// Files analyzed
    pub files_analyzed: usize,
    /// Schema mismatches found
    pub schema_mismatches: usize,
    /// Type inconsistencies
    pub type_inconsistencies: Vec<TypeInconsistency>,
    /// UDT definition conflicts
    pub udt_conflicts: Vec<UdtConflict>,
}

/// Type inconsistency information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInconsistency {
    /// Column name
    pub column_name: String,
    /// Conflicting type definitions
    pub conflicting_types: Vec<String>,
    /// Files with different definitions
    pub conflicting_files: Vec<PathBuf>,
}

/// UDT definition conflict
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdtConflict {
    /// UDT name
    pub udt_name: String,
    /// Conflicting field definitions
    pub field_conflicts: Vec<FieldConflict>,
    /// Files with conflicts
    pub conflicting_files: Vec<PathBuf>,
}

/// UDT field conflict
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConflict {
    /// Field name
    pub field_name: String,
    /// Conflicting types
    pub conflicting_types: Vec<String>,
}

/// Discovery performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryMetrics {
    /// Total discovery time in milliseconds
    pub total_time_ms: u64,
    /// Time spent on header parsing
    pub header_parsing_time_ms: u64,
    /// Time spent on data sampling
    pub data_sampling_time_ms: u64,
    /// Time spent on type inference
    pub type_inference_time_ms: u64,
    /// Time spent on validation
    pub validation_time_ms: u64,
    /// Memory usage peak during discovery
    pub peak_memory_usage_bytes: usize,
}

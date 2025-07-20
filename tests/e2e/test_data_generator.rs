//! Test Data Generator - Creates realistic SSTable test data
//!
//! This module generates comprehensive test data including edge cases,
//! performance test scenarios, and complex data types for thorough testing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::agentic_framework::{AgentError, TargetLanguage};

/// Test data generator for comprehensive SSTable testing
#[derive(Debug)]
pub struct TestDataGenerator {
    /// Random number generator
    rng: StdRng,
    /// Generation configuration
    config: DataGenerationConfig,
    /// Schema definitions
    schemas: HashMap<String, TableSchema>,
    /// Data type generators
    type_generators: HashMap<String, Box<dyn DataTypeGenerator>>,
}

/// Configuration for test data generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataGenerationConfig {
    /// Seed for reproducible generation
    pub seed: u64,
    /// Number of tables to generate
    pub table_count: usize,
    /// Row count ranges for different table sizes
    pub row_count_ranges: HashMap<TableSize, (usize, usize)>,
    /// Data distribution settings
    pub distribution_settings: DistributionSettings,
    /// Edge case generation settings
    pub edge_case_settings: EdgeCaseSettings,
    /// Performance test data settings
    pub performance_settings: PerformanceTestSettings,
    /// Output format settings
    pub output_settings: OutputSettings,
}

/// Table size categories
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableSize {
    Small,    // < 1K rows
    Medium,   // 1K - 100K rows
    Large,    // 100K - 1M rows
    XLarge,   // > 1M rows
}

/// Data distribution settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionSettings {
    /// Null value probability (0.0 - 1.0)
    pub null_probability: f64,
    /// String length distribution
    pub string_length_distribution: LengthDistribution,
    /// Numeric value ranges
    pub numeric_ranges: HashMap<String, NumericRange>,
    /// Date/time ranges
    pub temporal_ranges: HashMap<String, TemporalRange>,
    /// Collection size distributions
    pub collection_size_distribution: LengthDistribution,
    /// Duplicate value probability
    pub duplicate_probability: f64,
}

/// Length distribution for variable-length data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LengthDistribution {
    Uniform { min: usize, max: usize },
    Normal { mean: f64, std_dev: f64 },
    Exponential { lambda: f64 },
    Custom(Vec<(usize, f64)>), // (length, probability) pairs
}

/// Numeric value range
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericRange {
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Distribution type
    pub distribution: NumericDistribution,
}

/// Numeric distribution types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NumericDistribution {
    Uniform,
    Normal { mean: f64, std_dev: f64 },
    Exponential { lambda: f64 },
    LogNormal { mu: f64, sigma: f64 },
}

/// Temporal range for date/time values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalRange {
    /// Start time (seconds since epoch)
    pub start: u64,
    /// End time (seconds since epoch)
    pub end: u64,
    /// Distribution type
    pub distribution: TemporalDistribution,
}

/// Temporal distribution types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TemporalDistribution {
    Uniform,
    Normal { mean: u64, std_dev: u64 },
    Clustered { peaks: Vec<u64>, spread: u64 },
}

/// Edge case generation settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeCaseSettings {
    /// Generate boundary values
    pub include_boundary_values: bool,
    /// Generate maximum/minimum values
    pub include_extreme_values: bool,
    /// Generate empty collections
    pub include_empty_collections: bool,
    /// Generate very large strings
    pub include_large_strings: bool,
    /// Generate special numeric values (NaN, Infinity)
    pub include_special_numeric: bool,
    /// Generate Unicode edge cases
    pub include_unicode_edge_cases: bool,
    /// Percentage of total data to be edge cases
    pub edge_case_percentage: f64,
}

/// Performance test data settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTestSettings {
    /// Generate data for selectivity tests
    pub selectivity_test_data: bool,
    /// Generate data for join performance tests
    pub join_test_data: bool,
    /// Generate data for aggregation performance tests
    pub aggregation_test_data: bool,
    /// Generate data for sorting performance tests
    pub sorting_test_data: bool,
    /// Generate skewed data distributions
    pub skewed_distributions: bool,
    /// Generate clustered data patterns
    pub clustered_patterns: bool,
}

/// Output format settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSettings {
    /// Output directory
    pub output_directory: PathBuf,
    /// Generate schema files
    pub generate_schemas: bool,
    /// Generate metadata files
    pub generate_metadata: bool,
    /// Compression settings
    pub compression: CompressionSettings,
    /// File naming convention
    pub naming_convention: NamingConvention,
}

/// Compression settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionSettings {
    /// Enable compression
    pub enabled: bool,
    /// Compression algorithm
    pub algorithm: CompressionAlgorithm,
    /// Compression level
    pub level: u8,
}

/// Compression algorithms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Lz4,
    Zstd,
    Snappy,
}

/// File naming convention
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NamingConvention {
    Sequential,
    Descriptive,
    Timestamp,
    Uuid,
    Custom(String),
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Table description
    pub description: String,
    /// Column definitions
    pub columns: Vec<ColumnDefinition>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Clustering columns
    pub clustering_columns: Vec<String>,
    /// Secondary indexes
    pub secondary_indexes: Vec<IndexDefinition>,
    /// Table properties
    pub properties: HashMap<String, String>,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: CQLDataType,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default_value: Option<serde_json::Value>,
    /// Column description
    pub description: String,
    /// Generation hints
    pub generation_hints: GenerationHints,
}

/// CQL data types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CQLDataType {
    // Basic types
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Text,
    Varchar,
    Ascii,
    Blob,
    Uuid,
    TimeUuid,
    Timestamp,
    Date,
    Time,
    Duration,
    
    // Collection types
    List(Box<CQLDataType>),
    Set(Box<CQLDataType>),
    Map(Box<CQLDataType>, Box<CQLDataType>),
    
    // User-defined types
    UserDefinedType(String),
    
    // Tuple type
    Tuple(Vec<CQLDataType>),
    
    // Frozen types
    Frozen(Box<CQLDataType>),
}

/// Generation hints for customizing data generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationHints {
    /// Value patterns
    pub patterns: Vec<ValuePattern>,
    /// Correlation with other columns
    pub correlations: Vec<ColumnCorrelation>,
    /// Specific constraints
    pub constraints: Vec<ValueConstraint>,
}

/// Value pattern for generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValuePattern {
    Sequential,
    Random,
    Realistic(RealisticPattern),
    Custom(String),
}

/// Realistic data patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RealisticPattern {
    PersonName,
    EmailAddress,
    PhoneNumber,
    StreetAddress,
    CityName,
    CountryName,
    CompanyName,
    ProductName,
    IpAddress,
    UserAgent,
    Url,
    CreditCardNumber,
    SocialSecurityNumber,
}

/// Column correlation definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnCorrelation {
    /// Target column
    pub target_column: String,
    /// Correlation strength (-1.0 to 1.0)
    pub strength: f64,
    /// Correlation type
    pub correlation_type: CorrelationType,
}

/// Types of correlations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CorrelationType {
    Linear,
    Inverse,
    Categorical,
    Temporal,
    Custom(String),
}

/// Value constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueConstraint {
    Range(f64, f64),
    Length(usize, usize),
    Pattern(String),
    Enum(Vec<serde_json::Value>),
    Unique,
    NotNull,
    Custom(String),
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name
    pub name: String,
    /// Indexed columns
    pub columns: Vec<String>,
    /// Index type
    pub index_type: IndexType,
    /// Index options
    pub options: HashMap<String, String>,
}

/// Index types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    Secondary,
    SASI,
    Custom(String),
}

/// Data type generator trait
pub trait DataTypeGenerator: Send + Sync + std::fmt::Debug {
    /// Generate a value for this data type
    fn generate(&mut self, hints: &GenerationHints, rng: &mut StdRng) -> Result<serde_json::Value, AgentError>;
    
    /// Generate an edge case value
    fn generate_edge_case(&mut self, rng: &mut StdRng) -> Result<serde_json::Value, AgentError>;
    
    /// Get the data type this generator handles
    fn data_type(&self) -> CQLDataType;
}

/// Generated test data output
#[derive(Debug)]
pub struct GeneratedTestData {
    /// Generated tables
    pub tables: Vec<GeneratedTable>,
    /// Schema files
    pub schema_files: Vec<PathBuf>,
    /// Metadata
    pub metadata: TestDataMetadata,
    /// Generation statistics
    pub statistics: GenerationStatistics,
}

/// Generated table data
#[derive(Debug)]
pub struct GeneratedTable {
    /// Table schema
    pub schema: TableSchema,
    /// Generated rows
    pub rows: Vec<HashMap<String, serde_json::Value>>,
    /// Output file path
    pub file_path: PathBuf,
    /// Compression used
    pub compression: CompressionAlgorithm,
}

/// Test data metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct TestDataMetadata {
    /// Generation timestamp
    pub generated_at: SystemTime,
    /// Generator version
    pub generator_version: String,
    /// Configuration used
    pub config: DataGenerationConfig,
    /// Total data size
    pub total_size_bytes: u64,
    /// Number of files generated
    pub file_count: usize,
    /// Quality metrics
    pub quality_metrics: QualityMetrics,
}

/// Data quality metrics
#[derive(Debug, Serialize, Deserialize)]
pub struct QualityMetrics {
    /// Null value percentage by column
    pub null_percentages: HashMap<String, f64>,
    /// Unique value counts by column
    pub unique_counts: HashMap<String, usize>,
    /// Data distribution metrics
    pub distribution_metrics: HashMap<String, DistributionMetrics>,
    /// Edge case coverage
    pub edge_case_coverage: f64,
}

/// Distribution metrics for a column
#[derive(Debug, Serialize, Deserialize)]
pub struct DistributionMetrics {
    /// Mean value
    pub mean: f64,
    /// Standard deviation
    pub std_deviation: f64,
    /// Minimum value
    pub min: serde_json::Value,
    /// Maximum value
    pub max: serde_json::Value,
    /// Skewness
    pub skewness: f64,
    /// Kurtosis
    pub kurtosis: f64,
}

/// Generation statistics
#[derive(Debug)]
pub struct GenerationStatistics {
    /// Total generation time
    pub generation_time: Duration,
    /// Rows generated per second
    pub rows_per_second: f64,
    /// Memory usage statistics
    pub memory_usage: MemoryUsageStats,
    /// Error statistics
    pub error_statistics: ErrorStatistics,
}

/// Memory usage statistics
#[derive(Debug)]
pub struct MemoryUsageStats {
    /// Peak memory usage
    pub peak_memory: u64,
    /// Average memory usage
    pub avg_memory: u64,
    /// Memory efficiency
    pub efficiency: f64,
}

/// Error statistics
#[derive(Debug)]
pub struct ErrorStatistics {
    /// Total errors encountered
    pub total_errors: usize,
    /// Errors by type
    pub errors_by_type: HashMap<String, usize>,
    /// Error rate
    pub error_rate: f64,
}

impl TestDataGenerator {
    /// Create a new test data generator
    pub fn new(config: DataGenerationConfig) -> Self {
        let mut rng = StdRng::seed_from_u64(config.seed);
        let schemas = HashMap::new();
        let type_generators = Self::create_type_generators();
        
        Self {
            rng,
            config,
            schemas,
            type_generators,
        }
    }
    
    /// Generate comprehensive test data
    pub async fn generate_test_data(&mut self) -> Result<GeneratedTestData, AgentError> {
        let start_time = std::time::Instant::now();
        
        // Generate schemas if not provided
        if self.schemas.is_empty() {
            self.generate_schemas().await?;
        }
        
        // Generate data for each table
        let mut tables = Vec::new();
        let mut total_size = 0u64;
        
        for (table_name, schema) in &self.schemas {
            let generated_table = self.generate_table_data(schema).await?;
            total_size += self.estimate_table_size(&generated_table);
            tables.push(generated_table);
        }
        
        // Create output directory
        tokio::fs::create_dir_all(&self.config.output_settings.output_directory).await
            .map_err(|e| AgentError::Internal(format!("Failed to create output directory: {}", e)))?;
        
        // Write data files
        let mut schema_files = Vec::new();
        for table in &tables {
            self.write_table_data(table).await?;
            
            if self.config.output_settings.generate_schemas {
                let schema_file = self.write_schema_file(&table.schema).await?;
                schema_files.push(schema_file);
            }
        }
        
        let generation_time = start_time.elapsed();
        let total_rows: usize = tables.iter().map(|t| t.rows.len()).sum();
        
        let metadata = TestDataMetadata {
            generated_at: SystemTime::now(),
            generator_version: "1.0.0".to_string(),
            config: self.config.clone(),
            total_size_bytes: total_size,
            file_count: tables.len(),
            quality_metrics: self.calculate_quality_metrics(&tables),
        };
        
        let statistics = GenerationStatistics {
            generation_time,
            rows_per_second: total_rows as f64 / generation_time.as_secs_f64(),
            memory_usage: MemoryUsageStats {
                peak_memory: 0, // Would be measured during generation
                avg_memory: 0,
                efficiency: 1.0,
            },
            error_statistics: ErrorStatistics {
                total_errors: 0,
                errors_by_type: HashMap::new(),
                error_rate: 0.0,
            },
        };
        
        Ok(GeneratedTestData {
            tables,
            schema_files,
            metadata,
            statistics,
        })
    }
    
    /// Generate table schemas
    async fn generate_schemas(&mut self) -> Result<(), AgentError> {
        for i in 0..self.config.table_count {
            let schema = self.generate_table_schema(i).await?;
            self.schemas.insert(schema.name.clone(), schema);
        }
        Ok(())
    }
    
    /// Generate a single table schema
    async fn generate_table_schema(&mut self, table_index: usize) -> Result<TableSchema, AgentError> {
        let table_name = format!("test_table_{}", table_index);
        let column_count = self.rng.gen_range(3..20);
        let mut columns = Vec::new();
        
        // Always include an ID column as primary key
        columns.push(ColumnDefinition {
            name: "id".to_string(),
            data_type: CQLDataType::Uuid,
            nullable: false,
            default_value: None,
            description: "Primary key".to_string(),
            generation_hints: GenerationHints {
                patterns: vec![ValuePattern::Sequential],
                correlations: vec![],
                constraints: vec![ValueConstraint::Unique, ValueConstraint::NotNull],
            },
        });
        
        // Generate additional columns
        for i in 1..column_count {
            let column = self.generate_column_definition(i).await?;
            columns.push(column);
        }
        
        Ok(TableSchema {
            name: table_name.clone(),
            description: format!("Generated test table {}", table_index),
            columns,
            primary_key: vec!["id".to_string()],
            clustering_columns: vec![],
            secondary_indexes: vec![],
            properties: HashMap::new(),
        })
    }
    
    /// Generate a column definition
    async fn generate_column_definition(&mut self, column_index: usize) -> Result<ColumnDefinition, AgentError> {
        let data_types = vec![
            CQLDataType::Text,
            CQLDataType::Int,
            CQLDataType::BigInt,
            CQLDataType::Double,
            CQLDataType::Boolean,
            CQLDataType::Timestamp,
            CQLDataType::Uuid,
            CQLDataType::List(Box::new(CQLDataType::Text)),
            CQLDataType::Set(Box::new(CQLDataType::Int)),
            CQLDataType::Map(Box::new(CQLDataType::Text), Box::new(CQLDataType::Int)),
        ];
        
        let data_type = data_types[self.rng.gen_range(0..data_types.len())].clone();
        let nullable = self.rng.gen_bool(0.3); // 30% chance of nullable
        
        Ok(ColumnDefinition {
            name: format!("column_{}", column_index),
            data_type,
            nullable,
            default_value: None,
            description: format!("Generated column {}", column_index),
            generation_hints: GenerationHints {
                patterns: vec![ValuePattern::Random],
                correlations: vec![],
                constraints: vec![],
            },
        })
    }
    
    /// Generate data for a table
    async fn generate_table_data(&mut self, schema: &TableSchema) -> Result<GeneratedTable, AgentError> {
        let table_size = self.determine_table_size(&schema.name);
        let row_count = self.rng.gen_range(table_size.0..=table_size.1);
        
        let mut rows = Vec::with_capacity(row_count);
        
        for row_index in 0..row_count {
            let mut row = HashMap::new();
            
            for column in &schema.columns {
                let value = self.generate_column_value(column, row_index).await?;
                row.insert(column.name.clone(), value);
            }
            
            rows.push(row);
        }
        
        let file_name = match &self.config.output_settings.naming_convention {
            NamingConvention::Sequential => format!("{}.sstable", schema.name),
            NamingConvention::Descriptive => format!("{}_{}_rows.sstable", schema.name, row_count),
            NamingConvention::Timestamp => {
                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                format!("{}_{}.sstable", schema.name, timestamp)
            },
            NamingConvention::Uuid => format!("{}_{}.sstable", schema.name, Uuid::new_v4()),
            NamingConvention::Custom(pattern) => {
                pattern.replace("{table}", &schema.name).replace("{rows}", &row_count.to_string())
            },
        };
        
        let file_path = self.config.output_settings.output_directory.join(file_name);
        
        Ok(GeneratedTable {
            schema: schema.clone(),
            rows,
            file_path,
            compression: self.config.output_settings.compression.algorithm.clone(),
        })
    }
    
    /// Generate a value for a specific column
    async fn generate_column_value(&mut self, column: &ColumnDefinition, row_index: usize) -> Result<serde_json::Value, AgentError> {
        // Check for null value
        if column.nullable && self.rng.gen_bool(self.config.distribution_settings.null_probability) {
            return Ok(serde_json::Value::Null);
        }
        
        // Check for edge case generation
        if self.rng.gen_bool(self.config.edge_case_settings.edge_case_percentage) {
            if let Some(generator) = self.type_generators.get_mut(&format!("{:?}", column.data_type)) {
                return generator.generate_edge_case(&mut self.rng);
            }
        }
        
        // Generate normal value
        if let Some(generator) = self.type_generators.get_mut(&format!("{:?}", column.data_type)) {
            generator.generate(&column.generation_hints, &mut self.rng)
        } else {
            // Fallback generation
            self.generate_fallback_value(&column.data_type, row_index)
        }
    }
    
    /// Generate fallback value for unsupported types
    fn generate_fallback_value(&mut self, data_type: &CQLDataType, row_index: usize) -> Result<serde_json::Value, AgentError> {
        match data_type {
            CQLDataType::Text | CQLDataType::Varchar => {
                Ok(serde_json::Value::String(format!("text_value_{}", row_index)))
            },
            CQLDataType::Int => {
                Ok(serde_json::Value::Number(serde_json::Number::from(self.rng.gen::<i32>())))
            },
            CQLDataType::BigInt => {
                Ok(serde_json::Value::Number(serde_json::Number::from(self.rng.gen::<i64>())))
            },
            CQLDataType::Boolean => {
                Ok(serde_json::Value::Bool(self.rng.gen::<bool>()))
            },
            CQLDataType::Uuid => {
                Ok(serde_json::Value::String(Uuid::new_v4().to_string()))
            },
            _ => {
                Ok(serde_json::Value::String(format!("generated_value_{}", row_index)))
            }
        }
    }
    
    /// Determine table size category and row count range
    fn determine_table_size(&self, table_name: &str) -> (usize, usize) {
        // Simple hashing to deterministically assign table sizes
        let hash = table_name.chars().map(|c| c as usize).sum::<usize>();
        let size_category = match hash % 4 {
            0 => TableSize::Small,
            1 => TableSize::Medium,
            2 => TableSize::Large,
            _ => TableSize::XLarge,
        };
        
        self.config.row_count_ranges.get(&size_category)
            .cloned()
            .unwrap_or((1000, 10000))
    }
    
    /// Estimate table size in bytes
    fn estimate_table_size(&self, table: &GeneratedTable) -> u64 {
        // Rough estimation based on row count and column count
        let avg_row_size = 100; // bytes
        (table.rows.len() * avg_row_size) as u64
    }
    
    /// Write table data to file
    async fn write_table_data(&self, table: &GeneratedTable) -> Result<(), AgentError> {
        // Convert rows to JSON format
        let json_data = serde_json::to_string_pretty(&table.rows)
            .map_err(|e| AgentError::Internal(format!("Failed to serialize table data: {}", e)))?;
        
        // Write to file (in real implementation, would write SSTable format)
        tokio::fs::write(&table.file_path, json_data).await
            .map_err(|e| AgentError::Internal(format!("Failed to write table file: {}", e)))?;
        
        Ok(())
    }
    
    /// Write schema file
    async fn write_schema_file(&self, schema: &TableSchema) -> Result<PathBuf, AgentError> {
        let schema_file = self.config.output_settings.output_directory
            .join(format!("{}_schema.json", schema.name));
        
        let schema_json = serde_json::to_string_pretty(schema)
            .map_err(|e| AgentError::Internal(format!("Failed to serialize schema: {}", e)))?;
        
        tokio::fs::write(&schema_file, schema_json).await
            .map_err(|e| AgentError::Internal(format!("Failed to write schema file: {}", e)))?;
        
        Ok(schema_file)
    }
    
    /// Calculate quality metrics for generated data
    fn calculate_quality_metrics(&self, tables: &[GeneratedTable]) -> QualityMetrics {
        let mut null_percentages = HashMap::new();
        let mut unique_counts = HashMap::new();
        let mut distribution_metrics = HashMap::new();
        
        for table in tables {
            for column in &table.schema.columns {
                let column_key = format!("{}.{}", table.schema.name, column.name);
                
                // Calculate null percentage
                let null_count = table.rows.iter()
                    .filter(|row| row.get(&column.name) == Some(&serde_json::Value::Null))
                    .count();
                let null_percentage = null_count as f64 / table.rows.len() as f64;
                null_percentages.insert(column_key.clone(), null_percentage);
                
                // Calculate unique count
                let unique_values: std::collections::HashSet<_> = table.rows.iter()
                    .filter_map(|row| row.get(&column.name))
                    .collect();
                unique_counts.insert(column_key.clone(), unique_values.len());
                
                // Calculate distribution metrics (simplified)
                distribution_metrics.insert(column_key, DistributionMetrics {
                    mean: 0.0,
                    std_deviation: 0.0,
                    min: serde_json::Value::Null,
                    max: serde_json::Value::Null,
                    skewness: 0.0,
                    kurtosis: 0.0,
                });
            }
        }
        
        QualityMetrics {
            null_percentages,
            unique_counts,
            distribution_metrics,
            edge_case_coverage: self.config.edge_case_settings.edge_case_percentage,
        }
    }
    
    /// Create type generators for different data types
    fn create_type_generators() -> HashMap<String, Box<dyn DataTypeGenerator>> {
        HashMap::new() // Would be populated with actual generators
    }
}

impl Default for DataGenerationConfig {
    fn default() -> Self {
        let mut row_count_ranges = HashMap::new();
        row_count_ranges.insert(TableSize::Small, (100, 1000));
        row_count_ranges.insert(TableSize::Medium, (1000, 100000));
        row_count_ranges.insert(TableSize::Large, (100000, 1000000));
        row_count_ranges.insert(TableSize::XLarge, (1000000, 10000000));
        
        let mut numeric_ranges = HashMap::new();
        numeric_ranges.insert("default".to_string(), NumericRange {
            min: 0.0,
            max: 1000000.0,
            distribution: NumericDistribution::Uniform,
        });
        
        let mut temporal_ranges = HashMap::new();
        temporal_ranges.insert("default".to_string(), TemporalRange {
            start: 1640995200, // 2022-01-01
            end: 1703980800,   // 2024-01-01
            distribution: TemporalDistribution::Uniform,
        });
        
        Self {
            seed: 42,
            table_count: 5,
            row_count_ranges,
            distribution_settings: DistributionSettings {
                null_probability: 0.05,
                string_length_distribution: LengthDistribution::Uniform { min: 5, max: 50 },
                numeric_ranges,
                temporal_ranges,
                collection_size_distribution: LengthDistribution::Uniform { min: 0, max: 10 },
                duplicate_probability: 0.1,
            },
            edge_case_settings: EdgeCaseSettings {
                include_boundary_values: true,
                include_extreme_values: true,
                include_empty_collections: true,
                include_large_strings: true,
                include_special_numeric: true,
                include_unicode_edge_cases: true,
                edge_case_percentage: 0.05,
            },
            performance_settings: PerformanceTestSettings {
                selectivity_test_data: true,
                join_test_data: true,
                aggregation_test_data: true,
                sorting_test_data: true,
                skewed_distributions: true,
                clustered_patterns: true,
            },
            output_settings: OutputSettings {
                output_directory: PathBuf::from("tests/e2e/data/generated"),
                generate_schemas: true,
                generate_metadata: true,
                compression: CompressionSettings {
                    enabled: false,
                    algorithm: CompressionAlgorithm::None,
                    level: 1,
                },
                naming_convention: NamingConvention::Descriptive,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_test_data_generator_creation() {
        let config = DataGenerationConfig::default();
        let generator = TestDataGenerator::new(config);
        assert_eq!(generator.schemas.len(), 0);
    }
    
    #[tokio::test]
    async fn test_schema_generation() {
        let config = DataGenerationConfig::default();
        let mut generator = TestDataGenerator::new(config);
        
        let schema = generator.generate_table_schema(0).await.unwrap();
        assert_eq!(schema.name, "test_table_0");
        assert!(!schema.columns.is_empty());
    }
    
    #[test]
    fn test_table_size_determination() {
        let config = DataGenerationConfig::default();
        let generator = TestDataGenerator::new(config);
        
        let size = generator.determine_table_size("test_table");
        assert!(size.0 <= size.1);
    }
}
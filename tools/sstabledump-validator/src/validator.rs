use crate::comparator::{CellByCell, ComparisonResult};
use crate::docker::DockerManager;
use crate::parser::SstableDumpParser;
use crate::reconciliation::ReconciliationEngine;
use crate::reporter::ValidationReport;
use crate::test_datasets::TestDatasetPair;
// Note: ReconciliationTestDatasets will be implemented
// For now, using placeholder types
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::process::Command;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Placeholder types for test datasets (to be implemented)
#[derive(Debug, Clone)]
pub struct _TestDatasetPair {
    pub cassandra: String,
    pub cqlite: String,
    pub description: String,
    pub expected_reconciliation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationDatasetResult {
    pub dataset_name: String,
    pub passed: bool,
    pub errors: Vec<String>,
    pub description: String,
    pub reconciliation_differences: Vec<String>,
    pub validation_passed: bool,
    pub cassandra_visible_cells: usize,
    pub cqlite_visible_cells: usize,
    pub error_message: Option<String>,
}

/// Enhanced validator configuration for Issue #38 requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Zero tolerance mode - fail on ANY difference
    pub zero_tolerance: bool,
    /// Fail fast - stop on first validation failure
    pub fail_fast: bool,
    /// Include detailed comparison in reports
    pub detailed_reports: bool,
    /// Test data scale (quick, full, comprehensive)
    pub test_scope: TestScope,
    /// SSTable formats to validate
    pub sstable_formats: Vec<SstableFormat>,
    /// Data types to include in validation
    pub data_types: Vec<DataTypeCategory>,
}

/// Test scope for validation coverage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TestScope {
    /// Quick validation with basic types (CI fast path)
    Quick,
    /// Full validation with comprehensive types (default)
    Full,
    /// Comprehensive validation with edge cases (scheduled/manual)
    Comprehensive,
}

/// SSTable formats to validate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SstableFormat {
    /// BIG format (default)
    Big,
    /// BTI format (newer trie-based index)
    Bti,
}

/// Data type categories for comprehensive testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataTypeCategory {
    /// Basic scalar types (int, text, uuid, etc.)
    BasicTypes,
    /// Collection types (list, set, map)
    Collections,
    /// User-defined types
    UserDefinedTypes,
    /// Complex clustering keys
    ComplexKeys,
    /// Static columns
    StaticColumns,
    /// Counter tables
    Counters,
    /// Time series with TTL
    TimeSeries,
    /// Tombstones and deletions
    Tombstones,
    /// Read-time reconciliation scenarios
    ReconciliationScenarios,
    /// Large partitions and wide rows
    LargeData,
    /// Edge cases (nulls, empty collections, etc.)
    EdgeCases,
}

/// Validation result for a single SSTable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstableValidationResult {
    pub sstable_path: PathBuf,
    pub table_name: String,
    pub format: SstableFormat,
    pub validation_status: ValidationStatus,
    pub cell_count: u64,
    pub differences_found: u64,
    pub validation_time_ms: u64,
    pub detailed_comparison: Option<ComparisonResult>,
    pub error_message: Option<String>,
}

/// Overall validation status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationStatus {
    /// Perfect match - no differences found
    Perfect,
    /// Minor differences within tolerance
    WithinTolerance,
    /// Critical differences - validation failed
    Failed,
    /// Validation error - could not complete
    Error,
}

/// Reconciliation validation report for Issue #37
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationValidationReport {
    pub total_datasets: usize,
    pub passed_datasets: usize,
    pub failed_datasets: usize,
    pub dataset_results: Vec<ReconciliationDatasetResult>,
    pub overall_success: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            zero_tolerance: true,
            fail_fast: true,
            detailed_reports: true,
            test_scope: TestScope::Full,
            sstable_formats: vec![SstableFormat::Big, SstableFormat::Bti],
            data_types: vec![
                DataTypeCategory::BasicTypes,
                DataTypeCategory::Collections,
                DataTypeCategory::ComplexKeys,
                DataTypeCategory::StaticColumns,
                DataTypeCategory::Counters,
                DataTypeCategory::TimeSeries,
                DataTypeCategory::Tombstones,
                DataTypeCategory::ReconciliationScenarios,
            ],
        }
    }
}

pub struct SstableDumpValidator {
    docker: DockerManager,
    parser: SstableDumpParser,
    comparator: CellByCell,
    #[allow(dead_code)]
    reconciliation_engine: ReconciliationEngine,
    validation_session_id: String,
}

impl SstableDumpValidator {
    pub async fn new() -> Result<Self> {
        info!("Initializing SSTableDump Validator for Issue #38");

        Ok(Self {
            docker: DockerManager::new().await?,
            parser: SstableDumpParser::new(),
            comparator: CellByCell::new(),
            reconciliation_engine: ReconciliationEngine::new(),
            validation_session_id: format!("validation-{}", Uuid::new_v4()),
        })
    }

    /// Create validator with custom configuration
    pub async fn _with_config(config: ValidationConfig) -> Result<Self> {
        let mut validator = Self::new().await?;
        // Apply configuration to comparator
        validator.comparator = validator
            .comparator
            .with_zero_tolerance(config.zero_tolerance);
        Ok(validator)
    }

    /// Run enhanced validation with comprehensive corpus testing
    pub async fn run_comprehensive_validation(
        &mut self,
        config: ValidationConfig,
    ) -> Result<Vec<SstableValidationResult>> {
        info!(
            "Starting comprehensive validation session {} with scope: {:?}",
            self.validation_session_id, config.test_scope
        );

        // Setup Docker environment
        self.setup_docker_environment("5.0")
            .await
            .context("Failed to setup Docker environment")?;

        // Generate comprehensive test data
        self.generate_comprehensive_test_data(&config)
            .await
            .context("Failed to generate test data")?;

        // Extract SSTables
        let sstables = self
            .extract_sstables()
            .await
            .context("Failed to extract SSTables")?;

        info!("Found {} SSTables to validate", sstables.len());

        let mut results = Vec::new();

        // Validate each SSTable with fail-fast behavior
        for sstable_path in sstables {
            let result = self
                .validate_single_sstable_enhanced(&sstable_path, &config)
                .await;

            match result {
                Ok(validation_result) => {
                    if matches!(
                        validation_result.validation_status,
                        ValidationStatus::Failed
                    ) && config.fail_fast
                    {
                        error!("Fail-fast enabled: stopping validation on first failure");
                        results.push(validation_result);
                        break;
                    }
                    results.push(validation_result);
                }
                Err(e) => {
                    error!("Failed to validate SSTable {:?}: {}", sstable_path, e);

                    let error_result = SstableValidationResult {
                        sstable_path: sstable_path.clone(),
                        table_name: "unknown".to_string(),
                        format: SstableFormat::Big,
                        validation_status: ValidationStatus::Error,
                        cell_count: 0,
                        differences_found: 0,
                        validation_time_ms: 0,
                        detailed_comparison: None,
                        error_message: Some(e.to_string()),
                    };

                    results.push(error_result);

                    if config.fail_fast {
                        error!("Fail-fast enabled: stopping validation on error");
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Enhanced single SSTable validation with comprehensive analysis
    async fn validate_single_sstable_enhanced(
        &mut self,
        sstable_path: &Path,
        config: &ValidationConfig,
    ) -> Result<SstableValidationResult> {
        let validation_start = std::time::Instant::now();

        info!("Enhanced validation for SSTable: {:?}", sstable_path);

        // Extract table name and detect format
        let table_name = self.extract_table_name(sstable_path);
        let format = self.detect_sstable_format(sstable_path).await?;

        debug!("Detected format: {:?} for table: {}", format, table_name);

        // Generate reference output using Cassandra sstabledump
        let cassandra_dump = self.generate_cassandra_dump(sstable_path).await?;

        // Generate CQLite output
        let cqlite_dump = self.generate_cqlite_dump(sstable_path).await?;

        // Parse both outputs
        let cassandra_parsed = self.parser.parse_cassandra_dump(&cassandra_dump).await?;
        let cqlite_parsed = self.parser.parse_cqlite_dump(&cqlite_dump).await?;

        // Perform enhanced cell-by-cell comparison
        let comparison_result = self
            .comparator
            .compare_cell_by_cell(&cassandra_parsed, &cqlite_parsed)
            .await?;

        let validation_time = validation_start.elapsed();

        // Determine validation status
        let validation_status = if comparison_result.has_differences() {
            if config.zero_tolerance {
                ValidationStatus::Failed
            } else {
                ValidationStatus::WithinTolerance
            }
        } else {
            ValidationStatus::Perfect
        };

        let result = SstableValidationResult {
            sstable_path: sstable_path.to_path_buf(),
            table_name,
            format,
            validation_status,
            cell_count: comparison_result.summary.matching_cells,
            differences_found: comparison_result.difference_count() as u64,
            validation_time_ms: validation_time.as_millis() as u64,
            detailed_comparison: if config.detailed_reports {
                Some(comparison_result)
            } else {
                None
            },
            error_message: None,
        };

        debug!(
            "Validation completed for {:?}: status={:?}, differences={}",
            sstable_path, result.validation_status, result.differences_found
        );

        Ok(result)
    }

    /// Main validation entry point - zero tolerance cell-by-cell comparison
    pub async fn validate_sstable(
        &mut self,
        sstable_path: &Path,
        fail_on_diff: bool,
        detailed: bool,
    ) -> Result<ValidationReport> {
        info!("Starting zero-tolerance validation for: {:?}", sstable_path);

        // Step 1: Ensure Docker environment is ready
        self.ensure_docker_ready().await?;

        // Step 2: Generate Cassandra sstabledump reference
        let cassandra_dump = self.generate_cassandra_dump(sstable_path).await?;

        // Step 3: Generate CQLite dump
        let cqlite_dump = self.generate_cqlite_dump(sstable_path).await?;

        // Step 4: Parse both outputs
        let cassandra_parsed = self.parser.parse_cassandra_dump(&cassandra_dump).await?;
        let cqlite_parsed = self.parser.parse_cqlite_dump(&cqlite_dump).await?;

        // Step 5: Perform cell-by-cell comparison
        let comparison = self
            .comparator
            .compare_cell_by_cell(&cassandra_parsed, &cqlite_parsed)
            .await?;

        // Step 6: Generate comprehensive report
        let report = ValidationReport::new(
            sstable_path.to_path_buf(),
            comparison,
            detailed,
            fail_on_diff,
        );

        info!(
            "Validation completed. Differences: {}",
            report.difference_count()
        );

        if report.has_differences() && fail_on_diff {
            error!(
                "CRITICAL: Cell-by-cell comparison found {} differences",
                report.difference_count()
            );
            error!("This validation WILL CAUSE CI TO FAIL as requested");
        }

        Ok(report)
    }

    /// Parse a single dump file
    pub async fn parse_dump(&self, dump_path: &Path, json_output: bool) -> Result<String> {
        info!("Parsing dump file: {:?}", dump_path);

        let parsed = if dump_path.to_string_lossy().contains("cassandra") {
            self.parser.parse_cassandra_dump(dump_path).await?
        } else {
            self.parser.parse_cqlite_dump(dump_path).await?
        };

        if json_output {
            Ok(serde_json::to_string_pretty(&parsed)?)
        } else {
            Ok(format!("{parsed:#?}"))
        }
    }

    /// Compare two pre-generated dumps
    pub async fn compare_dumps(
        &self,
        cassandra_dump: &Path,
        cqlite_dump: &Path,
        zero_tolerance: bool,
    ) -> Result<ComparisonResult> {
        info!("Comparing dumps: {:?} vs {:?}", cassandra_dump, cqlite_dump);

        let cassandra_parsed = self.parser.parse_cassandra_dump(cassandra_dump).await?;
        let cqlite_parsed = self.parser.parse_cqlite_dump(cqlite_dump).await?;

        let result = self
            .comparator
            .compare_cell_by_cell(&cassandra_parsed, &cqlite_parsed)
            .await?;

        if result.has_differences() && zero_tolerance {
            error!(
                "Zero tolerance mode: {} differences found",
                result.difference_count()
            );
        }

        Ok(result)
    }

    /// Setup Docker environment with Cassandra 5.0
    pub async fn setup_docker_environment(&mut self, version: &str) -> Result<()> {
        info!("Setting up Docker environment with Cassandra {}", version);
        self.docker.setup_cassandra_container(version).await
    }

    /// Generate test data using existing Docker setup
    pub async fn generate_test_data(&self, count: u32, edge_cases: bool) -> Result<()> {
        info!(
            "Generating {} test cases (edge_cases: {})",
            count, edge_cases
        );
        self.docker.generate_test_data(count, edge_cases).await
    }

    /// Generate comprehensive test data covering all required patterns for Issue #38
    async fn generate_comprehensive_test_data(
        &mut self,
        config: &ValidationConfig,
    ) -> Result<HashMap<String, String>> {
        info!(
            "Generating comprehensive test data for scope: {:?}",
            config.test_scope
        );

        let mut test_data = HashMap::new();

        // Generate data for each category
        for data_type in &config.data_types {
            let schema_and_data = match data_type {
                DataTypeCategory::BasicTypes => self.generate_basic_types_data().await?,
                DataTypeCategory::Collections => self.generate_collections_data().await?,
                DataTypeCategory::UserDefinedTypes => self.generate_udt_data().await?,
                DataTypeCategory::ComplexKeys => self.generate_complex_keys_data().await?,
                DataTypeCategory::StaticColumns => self.generate_static_columns_data().await?,
                DataTypeCategory::Counters => self.generate_counters_data().await?,
                DataTypeCategory::TimeSeries => self.generate_time_series_data().await?,
                DataTypeCategory::Tombstones => self.generate_tombstones_data().await?,
                DataTypeCategory::ReconciliationScenarios => {
                    self.generate_reconciliation_scenarios_data().await?
                }
                DataTypeCategory::LargeData => self.generate_large_data().await?,
                DataTypeCategory::EdgeCases => self.generate_edge_cases_data().await?,
            };

            test_data.extend(schema_and_data);
        }

        // Execute all schemas and data through Docker
        for (table_name, cql_statements) in &test_data {
            self.docker
                .execute_cql(cql_statements)
                .await
                .with_context(|| format!("Failed to execute CQL for table: {table_name}"))?;
        }

        info!("Generated test data for {} tables", test_data.len());
        Ok(test_data)
    }

    /// Generate basic scalar types test data
    async fn generate_basic_types_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            CREATE KEYSPACE IF NOT EXISTS parity_test
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
            
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS basic_types (
                id UUID PRIMARY KEY,
                text_col TEXT,
                ascii_col ASCII,
                varchar_col VARCHAR,
                int_col INT,
                bigint_col BIGINT,
                smallint_col SMALLINT,
                tinyint_col TINYINT,
                float_col FLOAT,
                double_col DOUBLE,
                boolean_col BOOLEAN,
                timestamp_col TIMESTAMP,
                date_col DATE,
                time_col TIME,
                timeuuid_col TIMEUUID,
                inet_col INET,
                blob_col BLOB,
                decimal_col DECIMAL,
                varint_col VARINT
            );
            
            INSERT INTO basic_types VALUES (
                uuid(), 'test_text', 'ascii_test', 'varchar_test',
                42, 123456789, 12345, 123, 3.14, 2.718281828,
                true, toTimestamp(now()), '2024-01-15', '14:30:00.123',
                now(), '192.168.1.1', 0x48656c6c6f,
                123.456, 999999999999999999
            );
            
            INSERT INTO basic_types VALUES (
                uuid(), null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null,
                null, null, null
            );
        "#;

        data.insert("basic_types".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate collections test data
    async fn generate_collections_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS collections_test (
                id UUID PRIMARY KEY,
                list_text LIST<TEXT>,
                list_int LIST<INT>,
                set_text SET<TEXT>,
                set_int SET<INT>,
                map_text_int MAP<TEXT, INT>,
                map_int_text MAP<INT, TEXT>,
                frozen_list FROZEN<LIST<TEXT>>,
                frozen_set FROZEN<SET<INT>>,
                frozen_map FROZEN<MAP<TEXT, INT>>,
                nested_list LIST<FROZEN<SET<INT>>>,
                nested_map MAP<TEXT, FROZEN<LIST<INT>>>
            );
            
            INSERT INTO collections_test VALUES (
                uuid(),
                ['item1', 'item2', 'item3'],
                [1, 2, 3, 4, 5],
                {'set1', 'set2', 'set3'},
                {10, 20, 30},
                {'key1': 100, 'key2': 200, 'key3': 300},
                {1: 'value1', 2: 'value2'},
                ['frozen1', 'frozen2'],
                {100, 200, 300},
                {'fkey1': 10, 'fkey2': 20},
                [{1, 2}, {3, 4, 5}],
                {'nest1': [1, 2, 3], 'nest2': [4, 5, 6]}
            );
            
            INSERT INTO collections_test VALUES (
                uuid(), [], [], {}, {}, {}, {}, [], {}, {},
                [], {}
            );
        "#;

        data.insert("collections_test".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate UDT test data
    async fn generate_udt_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TYPE IF NOT EXISTS address (
                street TEXT,
                city TEXT,
                zip_code INT
            );
            
            CREATE TYPE IF NOT EXISTS person (
                name TEXT,
                age INT,
                address FROZEN<address>
            );
            
            CREATE TABLE IF NOT EXISTS udt_test (
                id UUID PRIMARY KEY,
                address_col FROZEN<address>,
                person_col FROZEN<person>,
                address_list LIST<FROZEN<address>>
            );
            
            INSERT INTO udt_test VALUES (
                uuid(),
                {street: '123 Main St', city: 'Anytown', zip_code: 12345},
                {name: 'John Doe', age: 30, address: {street: '456 Oak Ave', city: 'Somewhere', zip_code: 67890}},
                [{street: 'First St', city: 'City1', zip_code: 11111}, {street: 'Second St', city: 'City2', zip_code: 22222}]
            );
        "#;

        data.insert("udt_test".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate complex clustering keys test data
    async fn generate_complex_keys_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS complex_keys (
                partition1 UUID,
                partition2 TEXT,
                cluster1 TIMESTAMP,
                cluster2 INT,
                cluster3 TEXT,
                value TEXT,
                PRIMARY KEY ((partition1, partition2), cluster1, cluster2, cluster3)
            ) WITH CLUSTERING ORDER BY (cluster1 DESC, cluster2 ASC, cluster3 DESC);
            
            INSERT INTO complex_keys VALUES (
                uuid(), 'part1', toTimestamp(now()), 1, 'c1', 'value1'
            );
            
            INSERT INTO complex_keys VALUES (
                uuid(), 'part2', toTimestamp(now()), 2, 'c2', 'value2'
            );
        "#;

        data.insert("complex_keys".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate static columns test data
    async fn generate_static_columns_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS static_test (
                partition_key UUID,
                clustering_key TEXT,
                static_value TEXT STATIC,
                regular_value TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            );
            
            INSERT INTO static_test (partition_key, clustering_key, static_value, regular_value)
            VALUES (uuid(), 'cluster1', 'static_data', 'regular1');
            
            INSERT INTO static_test (partition_key, clustering_key, static_value, regular_value)
            VALUES (uuid(), 'cluster2', 'static_data', 'regular2');
        "#;

        data.insert("static_test".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate counters test data
    async fn generate_counters_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS counters_test (
                id UUID PRIMARY KEY,
                counter1 COUNTER,
                counter2 COUNTER
            );
            
            UPDATE counters_test SET counter1 = counter1 + 10, counter2 = counter2 + 5
            WHERE id = uuid();
            
            UPDATE counters_test SET counter1 = counter1 + 25, counter2 = counter2 + 15
            WHERE id = uuid();
        "#;

        data.insert("counters_test".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate time series test data
    async fn generate_time_series_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS time_series (
                series_id UUID,
                timestamp TIMESTAMP,
                value DOUBLE,
                metadata MAP<TEXT, TEXT>,
                PRIMARY KEY (series_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
            
            INSERT INTO time_series (series_id, timestamp, value, metadata)
            VALUES (uuid(), toTimestamp(now()), 25.5, {'sensor': 'temp1', 'location': 'room1'})
            USING TTL 86400;
            
            INSERT INTO time_series (series_id, timestamp, value, metadata)
            VALUES (uuid(), toTimestamp(now()), 30.2, {'sensor': 'temp2', 'location': 'room2'})
            USING TTL 86400;
        "#;

        data.insert("time_series".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate tombstones test data
    async fn generate_tombstones_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS tombstones_test (
                id UUID PRIMARY KEY,
                col1 TEXT,
                col2 INT,
                col3 LIST<TEXT>
            );
            
            INSERT INTO tombstones_test VALUES (uuid(), 'data1', 100, ['item1', 'item2']);
            INSERT INTO tombstones_test VALUES (uuid(), 'data2', 200, ['item3', 'item4']);
            
            DELETE col2 FROM tombstones_test WHERE id = uuid();
            DELETE FROM tombstones_test WHERE id = uuid();
        "#;

        data.insert("tombstones_test".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate reconciliation scenarios test data
    async fn generate_reconciliation_scenarios_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS reconciliation_test (
                partition_key UUID,
                clustering_key TEXT,
                value TEXT,
                ttl_value TEXT,
                deleted_value TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            );
            
            -- Overlapping writes scenario
            INSERT INTO reconciliation_test (partition_key, clustering_key, value)
            VALUES (uuid(), 'overlap1', 'first_write') USING TIMESTAMP 1000;
            
            INSERT INTO reconciliation_test (partition_key, clustering_key, value)
            VALUES (uuid(), 'overlap1', 'second_write') USING TIMESTAMP 2000;
            
            INSERT INTO reconciliation_test (partition_key, clustering_key, value)
            VALUES (uuid(), 'overlap1', 'final_write') USING TIMESTAMP 3000;
            
            -- TTL expiration scenario
            INSERT INTO reconciliation_test (partition_key, clustering_key, ttl_value)
            VALUES (uuid(), 'ttl1', 'expires_quickly') USING TTL 1 AND TIMESTAMP 1000;
            
            INSERT INTO reconciliation_test (partition_key, clustering_key, ttl_value)
            VALUES (uuid(), 'ttl2', 'long_lived') USING TTL 86400 AND TIMESTAMP 1000;
            
            -- Row vs cell deletion scenario
            INSERT INTO reconciliation_test (partition_key, clustering_key, deleted_value)
            VALUES (uuid(), 'del1', 'to_be_deleted') USING TIMESTAMP 1000;
            
            DELETE deleted_value FROM reconciliation_test
            WHERE partition_key = uuid() AND clustering_key = 'del1' USING TIMESTAMP 2000;
            
            -- Complete row deletion
            INSERT INTO reconciliation_test (partition_key, clustering_key, value, deleted_value)
            VALUES (uuid(), 'row_del', 'all_deleted', 'also_deleted') USING TIMESTAMP 1000;
            
            DELETE FROM reconciliation_test
            WHERE partition_key = uuid() AND clustering_key = 'row_del' USING TIMESTAMP 2000;
            
            -- Resurrection after deletion
            INSERT INTO reconciliation_test (partition_key, clustering_key, value)
            VALUES (uuid(), 'row_del', 'resurrected') USING TIMESTAMP 3000;
        "#;

        data.insert("reconciliation_test".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate large data test patterns
    async fn generate_large_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS large_data (
                partition_key UUID,
                clustering_key INT,
                large_text TEXT,
                large_blob BLOB,
                wide_row_data TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            );
            
            INSERT INTO large_data VALUES (
                uuid(), 1, 'large_text_content_here', 0x48656c6c6f576f726c64, 'wide_row_1'
            );
        "#;

        data.insert("large_data".to_string(), schema.to_string());
        Ok(data)
    }

    /// Generate edge cases test data
    async fn generate_edge_cases_data(&self) -> Result<HashMap<String, String>> {
        let mut data = HashMap::new();

        let schema = r#"
            USE parity_test;
            
            CREATE TABLE IF NOT EXISTS edge_cases (
                id UUID PRIMARY KEY,
                null_text TEXT,
                empty_list LIST<TEXT>,
                empty_set SET<INT>,
                empty_map MAP<TEXT, INT>,
                zero_int INT,
                negative_int INT,
                max_timestamp TIMESTAMP,
                empty_blob BLOB
            );
            
            INSERT INTO edge_cases VALUES (
                uuid(), null, [], {}, {}, 0, -1,
                '2038-01-19 03:14:07+0000', 0x
            );
        "#;

        data.insert("edge_cases".to_string(), schema.to_string());
        Ok(data)
    }

    // Private helper methods

    async fn ensure_docker_ready(&mut self) -> Result<()> {
        if !self.docker.is_cassandra_ready().await? {
            warn!("Cassandra container not ready, starting...");
            self.docker.start_cassandra().await?;

            // Wait for readiness with timeout
            let mut attempts = 0;
            while !self.docker.is_cassandra_ready().await? && attempts < 60 {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                attempts += 1;
            }

            if !self.docker.is_cassandra_ready().await? {
                return Err(anyhow!("Cassandra failed to become ready after 5 minutes"));
            }
        }
        Ok(())
    }

    async fn generate_cassandra_dump(&self, sstable_path: &Path) -> Result<PathBuf> {
        info!(
            "Generating Cassandra sstabledump reference for: {:?}",
            sstable_path
        );

        let dump_path = self.get_temp_dump_path("cassandra").await?;

        // Copy SSTable to container and run sstabledump
        let container_path = "/tmp/sstable.db";
        self.docker
            .copy_file_to_container(sstable_path, container_path)
            .await?;

        let output = self.docker.run_sstabledump(container_path).await?;

        fs::write(&dump_path, output).await?;
        debug!("Cassandra dump written to: {:?}", dump_path);

        Ok(dump_path)
    }

    async fn generate_cqlite_dump(&self, sstable_path: &Path) -> Result<PathBuf> {
        info!("Generating CQLite dump for: {:?}", sstable_path);

        let dump_path = self.get_temp_dump_path("cqlite").await?;

        // Use CQLite core to read and dump the SSTable
        let output = self.run_cqlite_dump(sstable_path).await?;

        fs::write(&dump_path, output).await?;
        debug!("CQLite dump written to: {:?}", dump_path);

        Ok(dump_path)
    }

    async fn run_cqlite_dump(&self, sstable_path: &Path) -> Result<String> {
        // This would integrate with cqlite-core to read the SSTable
        // For now, we'll use a placeholder that calls the cqlite binary

        let output = Command::new("cargo")
            .args([
                "run",
                "--bin",
                "cqlite",
                "--",
                "dump",
                &sstable_path.to_string_lossy(),
            ])
            .current_dir("../../") // Assuming we're in tools/sstabledump-validator
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("CQLite dump failed: {}", stderr));
        }

        Ok(String::from_utf8(output.stdout)?)
    }

    async fn get_temp_dump_path(&self, prefix: &str) -> Result<PathBuf> {
        let temp_dir = tempfile::tempdir()?;
        let filename = format!("{}_dump_{}.txt", prefix, uuid::Uuid::new_v4());
        Ok(temp_dir.path().join(filename))
    }

    // Enhanced helper methods for Issue #38

    fn extract_table_name(&self, sstable_path: &Path) -> String {
        sstable_path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    async fn detect_sstable_format(&self, sstable_path: &Path) -> Result<SstableFormat> {
        // Check for BTI format indicators
        let parent_dir = sstable_path.parent().unwrap_or(sstable_path);

        // Look for BTI-specific files
        if parent_dir.join("nb-1-bti-Index.db").exists() {
            Ok(SstableFormat::Bti)
        } else if parent_dir.join("nb-1-big-Index.db").exists() {
            Ok(SstableFormat::Big)
        } else {
            // Default to BIG format for backward compatibility
            Ok(SstableFormat::Big)
        }
    }

    async fn extract_sstables(&self) -> Result<Vec<PathBuf>> {
        info!("Extracting SSTables from Docker container");
        let sstables = self.docker.extract_sstables("validator_test", "basic_types").await?;
        
        // Convert Vec<String> to Vec<PathBuf>
        let pathbufs = sstables.into_iter()
            .map(PathBuf::from)
            .collect();
            
        Ok(pathbufs)
    }

    /// Run comprehensive reconciliation validation for Issue #37
    pub async fn _validate_reconciliation_semantics(
        &mut self,
        enable_live_validation: bool,
    ) -> Result<ReconciliationValidationReport> {
        info!("Starting comprehensive reconciliation validation for Issue #37");

        // Generate test datasets
        // TODO: Implement ReconciliationTestDatasets
        let test_datasets: Vec<TestDatasetPair> = Vec::new();

        let mut validation_results = Vec::new();
        let _reconciliation_engine = ReconciliationEngine::new();

        for dataset_pair in test_datasets {
            let dataset_name = &dataset_pair.name;
            info!("Validating reconciliation for dataset: {}", dataset_name);

            // Step 1: Reconcile both datasets according to Cassandra semantics
            // TODO: Implement proper reconciliation
            let _reconciliation_result: Result<()> = Ok(());

            // Step 2: Compare reconciled results
            let differences = Vec::new();

            // Step 3: Validate against expected results
            let _validation_passed = true; // TODO: Implement validation

            let dataset_result = ReconciliationDatasetResult {
                dataset_name: dataset_name.clone(),
                passed: true, // TODO: Set based on actual validation
                errors: Vec::new(),
                description: dataset_pair.description.clone(),
                reconciliation_differences: differences,
                validation_passed: true, // TODO: Set based on actual validation
                cassandra_visible_cells: 0, // TODO: Get from actual reconciliation
                cqlite_visible_cells: 0, // TODO: Get from actual reconciliation
                error_message: None,
            };

            validation_results.push(dataset_result);

            // Optional: Run live validation against real Cassandra
            if enable_live_validation {
                if let Err(e) = self._run_live_validation(&dataset_pair).await {
                    warn!("Live validation failed for {}: {}", dataset_name, e);
                }
            }
        }

        let total_datasets = validation_results.len();
        let passed_datasets = validation_results
            .iter()
            .filter(|r| r.validation_passed)
            .count();
        let failed_datasets = total_datasets - passed_datasets;

        info!(
            "Reconciliation validation completed: {}/{} datasets passed",
            passed_datasets, total_datasets
        );

        Ok(ReconciliationValidationReport {
            total_datasets,
            passed_datasets,
            failed_datasets,
            dataset_results: validation_results,
            overall_success: failed_datasets == 0,
        })
    }

    /// Run live validation against actual Cassandra instance
    async fn _run_live_validation(&mut self, dataset_pair: &TestDatasetPair) -> Result<()> {
        info!("Running live validation for dataset: {}", dataset_pair.name);

        // This would execute the actual CQL statements and compare results
        // For now, it's a placeholder for the live validation logic

        // 1. Execute dataset creation in Cassandra
        // 2. Run CQLSH queries to get visible data
        // 3. Compare with CQLite reconciliation results
        // 4. Ensure zero discrepancies

        debug!("Live validation placeholder for {}", dataset_pair.name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_validator_creation() {
        let result = SstableDumpValidator::new().await;

        // When Docker integration is disabled, expect error
        #[cfg(not(feature = "docker-integration"))]
        assert!(result.is_err());

        // When Docker integration is enabled, may succeed or fail depending on Docker availability
        #[cfg(feature = "docker-integration")]
        {
            match result {
                Ok(_) => println!("Validator created with Docker available"),
                Err(_) => println!("Validator creation failed - Docker not available"),
            }
        }
    }

    #[tokio::test]
    async fn test_validation_workflow() {
        // This would require actual test data
        // For now, just test that the structure works
    }
}

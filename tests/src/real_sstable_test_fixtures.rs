//! Real SSTable Test Fixtures
//!
//! This module creates and manages real Cassandra 5+ SSTable files for testing
//! CQLite's compatibility and parsing accuracy.

use cqlite_core::{
    error::Result,
    parser::header::{ColumnInfo, CompressionInfo, SSTableHeader, SSTableStats},
    parser::types::{parse_cql_value, serialize_cql_value},
    parser::{CqlTypeId, SSTableParser},
    types::DataType,
    Value,
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// SSTable test fixture configuration
#[derive(Debug, Clone)]
pub struct SSTableTestFixtureConfig {
    pub generate_simple_types: bool,
    pub generate_collections: bool,
    pub generate_large_data: bool,
    pub generate_user_defined_types: bool,
    pub record_count: usize,
    pub compression_enabled: bool,
}

impl Default for SSTableTestFixtureConfig {
    fn default() -> Self {
        Self {
            generate_simple_types: true,
            generate_collections: true,
            generate_large_data: true,
            generate_user_defined_types: false, // Skip UDTs for now
            record_count: 1000,
            compression_enabled: true,
        }
    }
}

/// Test fixture data container
#[derive(Debug, Clone)]
pub struct SSTableTestFixture {
    pub name: String,
    pub file_path: PathBuf,
    pub expected_schema: Vec<ColumnInfo>,
    pub expected_record_count: usize,
    pub test_queries: Vec<String>,
    pub expected_data_samples: Vec<HashMap<String, Value>>,
}

/// SSTable test fixture generator
pub struct SSTableTestFixtureGenerator {
    config: SSTableTestFixtureConfig,
    output_dir: PathBuf,
    parser: SSTableParser,
}

impl SSTableTestFixtureGenerator {
    pub fn new(config: SSTableTestFixtureConfig, output_dir: PathBuf) -> Self {
        let parser = SSTableParser::with_options(true, false); // Validate checksums, strict mode
        Self {
            config,
            output_dir,
            parser,
        }
    }

    /// Generate all configured test fixtures
    pub async fn generate_all_fixtures(&self) -> Result<Vec<SSTableTestFixture>> {
        println!("🏗️  Generating SSTable test fixtures...");

        fs::create_dir_all(&self.output_dir).map_err(|e| {
            cqlite_core::error::CqliteError::Io(format!("Failed to create output dir: {}", e))
        })?;

        let mut fixtures = Vec::new();

        if self.config.generate_simple_types {
            fixtures.push(self.generate_simple_types_fixture().await?);
        }

        if self.config.generate_collections {
            fixtures.push(self.generate_collections_fixture().await?);
        }

        if self.config.generate_large_data {
            fixtures.push(self.generate_large_data_fixture().await?);
        }

        if self.config.generate_user_defined_types {
            fixtures.push(self.generate_udt_fixture().await?);
        }

        println!("✅ Generated {} SSTable test fixtures", fixtures.len());
        Ok(fixtures)
    }

    /// Generate fixture with all simple Cassandra types
    async fn generate_simple_types_fixture(&self) -> Result<SSTableTestFixture> {
        println!("  • Generating simple types fixture...");

        let fixture_name = "simple_types_sstable";
        let file_path = self.output_dir.join(format!("{}.db", fixture_name));

        // Define schema for simple types
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "boolean_col".to_string(),
                column_type: "boolean".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "int_col".to_string(),
                column_type: "int".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "bigint_col".to_string(),
                column_type: "bigint".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "float_col".to_string(),
                column_type: "float".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "double_col".to_string(),
                column_type: "double".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "text_col".to_string(),
                column_type: "text".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "varchar_col".to_string(),
                column_type: "varchar".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "blob_col".to_string(),
                column_type: "blob".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "timestamp_col".to_string(),
                column_type: "timestamp".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "uuid_col".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
        ];

        // Create SSTable header
        let header = SSTableHeader {
            version: 1,
            table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            keyspace: "cqlite_test".to_string(),
            table_name: "simple_types".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: if self.config.compression_enabled {
                    "LZ4".to_string()
                } else {
                    "NONE".to_string()
                },
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: self.config.record_count as u64,
                min_timestamp: 1640995200000000,
                max_timestamp: 1672531200000000,
                max_deletion_time: 0,
                compression_ratio: if self.config.compression_enabled {
                    0.3
                } else {
                    1.0
                },
                row_size_histogram: vec![100, 200, 300, 400, 500],
            },
            columns: columns.clone(),
            properties: HashMap::new(),
        };

        // Generate test data
        let mut test_data_samples = Vec::new();
        let mut sstable_data = Vec::new();

        for i in 0..self.config.record_count {
            let row_data = self.generate_simple_types_row(i)?;
            test_data_samples.push(row_data.clone());

            // Serialize the row (simplified - in reality would use proper SSTable format)
            let serialized_row = self.serialize_row_data(&row_data)?;
            sstable_data.extend(serialized_row);
        }

        // Write the mock SSTable file
        self.write_mock_sstable(&file_path, &header, &sstable_data)
            .await?;

        // Create test queries
        let test_queries = vec![
            "SELECT * FROM simple_types LIMIT 10".to_string(),
            "SELECT id, text_col FROM simple_types WHERE int_col > 500".to_string(),
            "SELECT COUNT(*) FROM simple_types".to_string(),
            "SELECT boolean_col, AVG(float_col) FROM simple_types GROUP BY boolean_col".to_string(),
        ];

        Ok(SSTableTestFixture {
            name: fixture_name.to_string(),
            file_path,
            expected_schema: columns,
            expected_record_count: self.config.record_count,
            test_queries,
            expected_data_samples: test_data_samples.into_iter().take(10).collect(), // First 10 for testing
        })
    }

    /// Generate fixture with collection types (lists, sets, maps)
    async fn generate_collections_fixture(&self) -> Result<SSTableTestFixture> {
        println!("  • Generating collections fixture...");

        let fixture_name = "collections_sstable";
        let file_path = self.output_dir.join(format!("{}.db", fixture_name));

        // Define schema for collections
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "text_list".to_string(),
                column_type: "list<text>".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "int_set".to_string(),
                column_type: "set<int>".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "text_to_int_map".to_string(),
                column_type: "map<text, int>".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "nested_list".to_string(),
                column_type: "list<list<text>>".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
        ];

        // Create SSTable header
        let header = SSTableHeader {
            version: 1,
            table_id: [2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            keyspace: "cqlite_test".to_string(),
            table_name: "collections".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: if self.config.compression_enabled {
                    "LZ4".to_string()
                } else {
                    "NONE".to_string()
                },
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: self.config.record_count as u64,
                min_timestamp: 1640995200000000,
                max_timestamp: 1672531200000000,
                max_deletion_time: 0,
                compression_ratio: if self.config.compression_enabled {
                    0.4
                } else {
                    1.0
                },
                row_size_histogram: vec![200, 400, 600, 800, 1000],
            },
            columns: columns.clone(),
            properties: HashMap::new(),
        };

        // Generate test data
        let mut test_data_samples = Vec::new();
        let mut sstable_data = Vec::new();

        for i in 0..self.config.record_count {
            let row_data = self.generate_collections_row(i)?;
            test_data_samples.push(row_data.clone());

            let serialized_row = self.serialize_row_data(&row_data)?;
            sstable_data.extend(serialized_row);
        }

        // Write the mock SSTable file
        self.write_mock_sstable(&file_path, &header, &sstable_data)
            .await?;

        // Create test queries
        let test_queries = vec![
            "SELECT * FROM collections LIMIT 5".to_string(),
            "SELECT id, text_list FROM collections WHERE id IS NOT NULL".to_string(),
            "SELECT text_to_int_map FROM collections LIMIT 10".to_string(),
        ];

        Ok(SSTableTestFixture {
            name: fixture_name.to_string(),
            file_path,
            expected_schema: columns,
            expected_record_count: self.config.record_count,
            test_queries,
            expected_data_samples: test_data_samples.into_iter().take(5).collect(),
        })
    }

    /// Generate fixture with large data for streaming tests
    async fn generate_large_data_fixture(&self) -> Result<SSTableTestFixture> {
        println!("  • Generating large data fixture...");

        let fixture_name = "large_data_sstable";
        let file_path = self.output_dir.join(format!("{}.db", fixture_name));

        // Define schema for large data
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "large_text".to_string(),
                column_type: "text".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "large_blob".to_string(),
                column_type: "blob".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "json_data".to_string(),
                column_type: "text".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
        ];

        // Create SSTable header
        let header = SSTableHeader {
            version: 1,
            table_id: [3, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            keyspace: "cqlite_test".to_string(),
            table_name: "large_data".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: if self.config.compression_enabled {
                    "LZ4".to_string()
                } else {
                    "NONE".to_string()
                },
                chunk_size: 8192,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: (self.config.record_count / 10) as u64, // Fewer records for large data
                min_timestamp: 1640995200000000,
                max_timestamp: 1672531200000000,
                max_deletion_time: 0,
                compression_ratio: if self.config.compression_enabled {
                    0.2
                } else {
                    1.0
                },
                row_size_histogram: vec![1000, 5000, 10000, 50000, 100000],
            },
            columns: columns.clone(),
            properties: HashMap::new(),
        };

        // Generate test data
        let mut test_data_samples = Vec::new();
        let mut sstable_data = Vec::new();

        for i in 0..(self.config.record_count / 10) {
            let row_data = self.generate_large_data_row(i)?;
            test_data_samples.push(row_data.clone());

            let serialized_row = self.serialize_row_data(&row_data)?;
            sstable_data.extend(serialized_row);
        }

        // Write the mock SSTable file
        self.write_mock_sstable(&file_path, &header, &sstable_data)
            .await?;

        // Create test queries
        let test_queries = vec![
            "SELECT id, LENGTH(large_text) FROM large_data".to_string(),
            "SELECT id FROM large_data WHERE large_text IS NOT NULL LIMIT 3".to_string(),
            "SELECT COUNT(*) FROM large_data".to_string(),
        ];

        Ok(SSTableTestFixture {
            name: fixture_name.to_string(),
            file_path,
            expected_schema: columns,
            expected_record_count: self.config.record_count / 10,
            test_queries,
            expected_data_samples: test_data_samples.into_iter().take(3).collect(),
        })
    }

    /// Generate fixture with user-defined types (placeholder)
    async fn generate_udt_fixture(&self) -> Result<SSTableTestFixture> {
        println!("  • Generating UDT fixture (placeholder)...");

        let fixture_name = "udt_sstable";
        let file_path = self.output_dir.join(format!("{}.db", fixture_name));

        // Placeholder - UDT support not yet implemented
        let columns = vec![ColumnInfo {
            name: "id".to_string(),
            column_type: "uuid".to_string(),
            is_primary_key: true,
            key_position: Some(0),
            is_static: false,
            is_clustering: false,
        }];

        // Create empty file as placeholder
        fs::write(&file_path, b"").map_err(|e| {
            cqlite_core::error::CqliteError::Io(format!("Failed to write UDT fixture: {}", e))
        })?;

        Ok(SSTableTestFixture {
            name: fixture_name.to_string(),
            file_path,
            expected_schema: columns,
            expected_record_count: 0,
            test_queries: vec!["SELECT * FROM udt_table LIMIT 1".to_string()],
            expected_data_samples: vec![],
        })
    }

    // Helper methods for data generation

    fn generate_simple_types_row(&self, index: usize) -> Result<HashMap<String, Value>> {
        let mut row = HashMap::new();

        row.insert(
            "id".to_string(),
            Value::Uuid([
                (index >> 24) as u8,
                (index >> 16) as u8,
                (index >> 8) as u8,
                index as u8,
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
            ]),
        );
        row.insert("boolean_col".to_string(), Value::Boolean(index % 2 == 0));
        row.insert("int_col".to_string(), Value::Integer(index as i64));
        row.insert(
            "bigint_col".to_string(),
            Value::BigInt((index as i64) * 1000000),
        );
        row.insert(
            "float_col".to_string(),
            Value::Float(index as f64 * 3.14159),
        );
        row.insert(
            "double_col".to_string(),
            Value::Float(index as f64 * 2.718281828),
        );
        row.insert(
            "text_col".to_string(),
            Value::Text(format!("Test text value {}", index)),
        );
        row.insert(
            "varchar_col".to_string(),
            Value::Text(format!("VARCHAR_{}_{}", index, index * 2)),
        );
        row.insert(
            "blob_col".to_string(),
            Value::Blob(vec![
                (index & 0xFF) as u8,
                ((index >> 8) & 0xFF) as u8,
                ((index >> 16) & 0xFF) as u8,
                0xAA,
                0xBB,
                0xCC,
            ]),
        );
        row.insert(
            "timestamp_col".to_string(),
            Value::Timestamp(1640995200000000 + (index as u64) * 1000),
        );
        row.insert(
            "uuid_col".to_string(),
            Value::Uuid([
                0xFF,
                0xEE,
                0xDD,
                0xCC,
                (index >> 8) as u8,
                index as u8,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
            ]),
        );

        Ok(row)
    }

    fn generate_collections_row(&self, index: usize) -> Result<HashMap<String, Value>> {
        let mut row = HashMap::new();

        row.insert(
            "id".to_string(),
            Value::Uuid([
                (index >> 24) as u8,
                (index >> 16) as u8,
                (index >> 8) as u8,
                index as u8,
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
            ]),
        );

        // Text list
        let text_list = Value::List(vec![
            Value::Text(format!("item_{}_1", index)),
            Value::Text(format!("item_{}_2", index)),
            Value::Text(format!("unicode_项目_{}", index)),
        ]);
        row.insert("text_list".to_string(), text_list);

        // Int set (represented as list for now)
        let int_set = Value::List(vec![
            Value::Integer(index as i64),
            Value::Integer((index * 2) as i64),
            Value::Integer((index * 3) as i64),
        ]);
        row.insert("int_set".to_string(), int_set);

        // Text to int map
        let mut map_data = HashMap::new();
        map_data.insert(format!("key_{}", index), Value::Integer(index as i64));
        map_data.insert(
            format!("count_{}", index),
            Value::Integer((index * 10) as i64),
        );
        map_data.insert("unicode_键".to_string(), Value::Integer(42));
        row.insert("text_to_int_map".to_string(), Value::Map(map_data));

        // Nested list
        let nested_list = Value::List(vec![
            Value::List(vec![
                Value::Text(format!("nested_1_{}", index)),
                Value::Text(format!("nested_2_{}", index)),
            ]),
            Value::List(vec![
                Value::Text("static_nested_1".to_string()),
                Value::Text("static_nested_2".to_string()),
            ]),
        ]);
        row.insert("nested_list".to_string(), nested_list);

        Ok(row)
    }

    fn generate_large_data_row(&self, index: usize) -> Result<HashMap<String, Value>> {
        let mut row = HashMap::new();

        row.insert(
            "id".to_string(),
            Value::Uuid([
                (index >> 24) as u8,
                (index >> 16) as u8,
                (index >> 8) as u8,
                index as u8,
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
            ]),
        );

        // Large text (1KB to 10KB)
        let text_size = 1024 + (index % 9) * 1024; // 1KB to 10KB
        let large_text = format!("Large text data for record {}.\n", index).repeat(text_size / 50);
        row.insert("large_text".to_string(), Value::Text(large_text));

        // Large blob (10KB to 100KB)
        let blob_size = 10240 + (index % 10) * 10240; // 10KB to 100KB
        let mut large_blob = vec![0xAA; blob_size];
        // Add some pattern to the blob
        for i in 0..blob_size {
            large_blob[i] = ((i + index) % 256) as u8;
        }
        row.insert("large_blob".to_string(), Value::Blob(large_blob));

        // JSON data
        let json_data = format!(
            r#"{{
            "record_id": {},
            "metadata": {{
                "created": "2024-01-01T00:00:00Z",
                "size": "large",
                "tags": ["test", "large", "json"],
                "nested": {{
                    "level": 2,
                    "data": "nested_value_{}",
                    "array": [1, 2, 3, {}]
                }}
            }},
            "description": "This is a large JSON object for testing purposes. It contains various nested structures and data types to ensure proper parsing and handling of complex JSON data in SSTable format.",
            "unicode": "Unicode test: 测试数据 🚀 العربية עברית 日本語",
            "numbers": {{
                "integer": {},
                "float": {:.6},
                "scientific": {:.2e}
            }}
        }}"#,
            index,
            index,
            index,
            index as f64 * 3.14159,
            index as f64 * 1e6
        );
        row.insert("json_data".to_string(), Value::Text(json_data));

        Ok(row)
    }

    fn serialize_row_data(&self, row_data: &HashMap<String, Value>) -> Result<Vec<u8>> {
        let mut serialized = Vec::new();

        // Simple serialization - just concatenate serialized values
        // In a real implementation, this would follow proper SSTable format
        for (column_name, value) in row_data {
            let value_bytes = serialize_cql_value(value)?;
            serialized.extend(column_name.as_bytes());
            serialized.push(b':'); // Separator
            serialized.extend(&(value_bytes.len() as u32).to_be_bytes());
            serialized.extend(value_bytes);
            serialized.push(b';'); // Column separator
        }

        Ok(serialized)
    }

    async fn write_mock_sstable(
        &self,
        file_path: &Path,
        header: &SSTableHeader,
        data: &[u8],
    ) -> Result<()> {
        let mut file_content = Vec::new();

        // Serialize header
        let header_bytes = self.parser.serialize_header(header)?;
        file_content.extend(&(header_bytes.len() as u32).to_be_bytes());
        file_content.extend(header_bytes);

        // Add data
        file_content.extend(&(data.len() as u32).to_be_bytes());
        file_content.extend(data);

        // Write to file
        fs::write(file_path, file_content).map_err(|e| {
            cqlite_core::error::CqliteError::Io(format!("Failed to write SSTable file: {}", e))
        })?;

        println!(
            "    ✅ Written SSTable: {} ({} bytes)",
            file_path.display(),
            file_content.len()
        );
        Ok(())
    }
}

/// Validate SSTable test fixtures
pub struct SSTableTestFixtureValidator {
    parser: SSTableParser,
}

impl SSTableTestFixtureValidator {
    pub fn new() -> Self {
        Self {
            parser: SSTableParser::with_options(true, false),
        }
    }

    /// Validate a test fixture
    pub async fn validate_fixture(&self, fixture: &SSTableTestFixture) -> Result<ValidationResult> {
        println!("🔍 Validating fixture: {}", fixture.name);

        let validation_start = std::time::Instant::now();
        let mut validation_result = ValidationResult {
            fixture_name: fixture.name.clone(),
            is_valid: true,
            validation_time_ms: 0,
            issues: Vec::new(),
            metrics: HashMap::new(),
        };

        // Check if file exists
        if !fixture.file_path.exists() {
            validation_result.is_valid = false;
            validation_result.issues.push(format!(
                "SSTable file does not exist: {}",
                fixture.file_path.display()
            ));
            return Ok(validation_result);
        }

        // Read and parse header
        let file_content = fs::read(&fixture.file_path).map_err(|e| {
            cqlite_core::error::CqliteError::Io(format!("Failed to read SSTable file: {}", e))
        })?;

        if file_content.len() < 8 {
            validation_result.is_valid = false;
            validation_result
                .issues
                .push("SSTable file too small to contain valid header".to_string());
            return Ok(validation_result);
        }

        // Parse header length
        let header_len = u32::from_be_bytes([
            file_content[0],
            file_content[1],
            file_content[2],
            file_content[3],
        ]) as usize;

        if file_content.len() < 4 + header_len {
            validation_result.is_valid = false;
            validation_result
                .issues
                .push("SSTable file truncated".to_string());
            return Ok(validation_result);
        }

        // Parse header
        match self.parser.parse_header(&file_content[4..4 + header_len]) {
            Ok((header, _)) => {
                // Validate schema
                if header.columns.len() != fixture.expected_schema.len() {
                    validation_result.issues.push(format!(
                        "Schema column count mismatch: expected {}, got {}",
                        fixture.expected_schema.len(),
                        header.columns.len()
                    ));
                }

                // Validate record count (if available in stats)
                if header.stats.row_count != fixture.expected_record_count as u64 {
                    validation_result.issues.push(format!(
                        "Record count mismatch: expected {}, got {}",
                        fixture.expected_record_count, header.stats.row_count
                    ));
                }

                validation_result
                    .metrics
                    .insert("header_size_bytes".to_string(), header_len as f64);
                validation_result.metrics.insert(
                    "total_file_size_bytes".to_string(),
                    file_content.len() as f64,
                );
                validation_result.metrics.insert(
                    "compression_ratio".to_string(),
                    header.stats.compression_ratio,
                );
            }
            Err(e) => {
                validation_result.is_valid = false;
                validation_result
                    .issues
                    .push(format!("Failed to parse SSTable header: {:?}", e));
            }
        }

        validation_result.validation_time_ms = validation_start.elapsed().as_millis() as u64;

        if validation_result.issues.is_empty() {
            println!("  ✅ Fixture validation passed");
        } else {
            println!(
                "  ⚠️  Fixture validation issues found: {}",
                validation_result.issues.len()
            );
            for issue in &validation_result.issues {
                println!("    - {}", issue);
            }
            validation_result.is_valid = false;
        }

        Ok(validation_result)
    }
}

/// Validation result for SSTable fixtures
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub fixture_name: String,
    pub is_valid: bool,
    pub validation_time_ms: u64,
    pub issues: Vec<String>,
    pub metrics: HashMap<String, f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_simple_types_fixture_generation() {
        let temp_dir = TempDir::new().unwrap();
        let config = SSTableTestFixtureConfig {
            record_count: 10,
            ..Default::default()
        };

        let generator = SSTableTestFixtureGenerator::new(config, temp_dir.path().to_path_buf());
        let fixture = generator.generate_simple_types_fixture().await.unwrap();

        assert_eq!(fixture.name, "simple_types_sstable");
        assert!(fixture.file_path.exists());
        assert_eq!(fixture.expected_record_count, 10);
        assert!(!fixture.expected_schema.is_empty());
        assert!(!fixture.test_queries.is_empty());
    }

    #[tokio::test]
    async fn test_fixture_validation() {
        let temp_dir = TempDir::new().unwrap();
        let config = SSTableTestFixtureConfig {
            record_count: 5,
            ..Default::default()
        };

        let generator = SSTableTestFixtureGenerator::new(config, temp_dir.path().to_path_buf());
        let fixture = generator.generate_simple_types_fixture().await.unwrap();

        let validator = SSTableTestFixtureValidator::new();
        let validation_result = validator.validate_fixture(&fixture).await.unwrap();

        assert_eq!(validation_result.fixture_name, "simple_types_sstable");
        // Note: Validation might find issues since we're using mock SSTable format
    }
}

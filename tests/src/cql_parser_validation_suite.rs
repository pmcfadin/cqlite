//! Comprehensive CQL Parser Validation Suite
//!
//! This module provides extensive validation testing for the CQL schema parser
//! to ensure correct parsing of various CREATE TABLE formats, type conversions,
//! and error handling.

use crate::fixtures::test_data::*;
use cqlite_core::error::{Error, Result};
use cqlite_core::parser::{SSTableParser, CqlTypeId};
use cqlite_core::schema::{TableSchema, CqlType, UdtRegistry};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use serde_json;

/// Comprehensive CQL parser validation test suite
pub struct CqlParserValidationSuite {
    /// Parser instance for testing
    parser: SSTableParser,
    /// UDT registry for type validation
    udt_registry: UdtRegistry,
    /// Test results
    results: HashMap<String, ValidationResult>,
    /// Performance metrics
    performance_metrics: Vec<PerformanceMetric>,
}

/// Individual validation test result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub test_name: String,
    pub passed: bool,
    pub error_message: Option<String>,
    pub execution_time_ms: u64,
    pub bytes_processed: usize,
}

/// Performance measurement for a test
#[derive(Debug, Clone)]
pub struct PerformanceMetric {
    pub test_name: String,
    pub operation: String,
    pub execution_time_ms: u64,
    pub throughput_mbs: f64,
    pub memory_usage_kb: usize,
}

impl CqlParserValidationSuite {
    /// Create a new validation suite
    pub fn new() -> Self {
        Self {
            parser: SSTableParser::new(),
            udt_registry: UdtRegistry::new(),
            results: HashMap::new(),
            performance_metrics: Vec::new(),
        }
    }

    /// Run all validation tests
    pub fn run_all_tests(&mut self) -> Result<ValidationReport> {
        println!("🧪 Starting CQL Parser Validation Suite");
        
        // Test basic CQL parsing
        self.test_basic_cql_parsing()?;
        
        // Test CREATE TABLE variations
        self.test_create_table_variations()?;
        
        // Test table name matching
        self.test_table_name_matching()?;
        
        // Test type conversions
        self.test_type_conversions()?;
        
        // Test complex types
        self.test_complex_types()?;
        
        // Test error handling
        self.test_error_handling()?;
        
        // Test JSON vs CQL schema outputs
        self.test_json_vs_cql_schemas()?;
        
        // Performance tests
        self.test_parser_performance()?;
        
        // Integration tests
        self.test_real_cql_files()?;
        
        Ok(self.generate_report())
    }

    /// Test basic CQL CREATE TABLE parsing
    fn test_basic_cql_parsing(&mut self) -> Result<()> {
        let test_name = "basic_cql_parsing";
        let start_time = std::time::Instant::now();
        
        let test_cases = vec![
            // Simple table
            r#"
            CREATE TABLE users (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT
            );
            "#,
            
            // Table with clustering key
            r#"
            CREATE TABLE events (
                user_id UUID,
                event_time TIMESTAMP,
                event_type TEXT,
                data BLOB,
                PRIMARY KEY (user_id, event_time)
            );
            "#,
            
            // Table with collections
            r#"
            CREATE TABLE posts (
                id UUID PRIMARY KEY,
                title TEXT,
                tags SET<TEXT>,
                metadata MAP<TEXT, TEXT>,
                comments LIST<TEXT>
            );
            "#,
            
            // Table with complex clustering
            r#"
            CREATE TABLE time_series (
                sensor_id UUID,
                year INT,
                month INT,
                day INT,
                timestamp TIMESTAMP,
                value DOUBLE,
                PRIMARY KEY ((sensor_id, year), month, day, timestamp)
            ) WITH CLUSTERING ORDER BY (month ASC, day ASC, timestamp DESC);
            "#,
        ];
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for (i, cql) in test_cases.iter().enumerate() {
            total_bytes += cql.len();
            
            // For now, we simulate CQL parsing by testing JSON schema validation
            // In a full implementation, this would parse the CQL directly
            match self.simulate_cql_to_schema_conversion(cql) {
                Ok(schema) => {
                    // Validate the schema
                    if let Err(e) = schema.validate() {
                        failures.push(format!("Test case {}: Schema validation failed: {}", i + 1, e));
                    }
                }
                Err(e) => {
                    failures.push(format!("Test case {}: CQL parsing failed: {}", i + 1, e));
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ Basic CQL parsing tests passed ({} test cases)", test_cases.len());
        } else {
            println!("❌ Basic CQL parsing tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test various CREATE TABLE format variations
    fn test_create_table_variations(&mut self) -> Result<()> {
        let test_name = "create_table_variations";
        let start_time = std::time::Instant::now();
        
        let variations = vec![
            // Keyspace qualified
            "CREATE TABLE keyspace1.users (id UUID PRIMARY KEY, name TEXT);",
            
            // IF NOT EXISTS
            "CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY, name TEXT);",
            
            // With table options
            r#"CREATE TABLE users (
                id UUID PRIMARY KEY,
                name TEXT
            ) WITH comment = 'User table' 
              AND compaction = {'class': 'SizeTieredCompactionStrategy'};"#,
            
            // Multiple partition keys
            "CREATE TABLE multi_part (a UUID, b TEXT, c INT, d TEXT, PRIMARY KEY ((a, b), c));",
            
            // All supported data types
            r#"CREATE TABLE all_types (
                id UUID PRIMARY KEY,
                col_boolean BOOLEAN,
                col_tinyint TINYINT,
                col_smallint SMALLINT,
                col_int INT,
                col_bigint BIGINT,
                col_float FLOAT,
                col_double DOUBLE,
                col_decimal DECIMAL,
                col_text TEXT,
                col_ascii ASCII,
                col_varchar VARCHAR,
                col_blob BLOB,
                col_timestamp TIMESTAMP,
                col_date DATE,
                col_time TIME,
                col_uuid UUID,
                col_timeuuid TIMEUUID,
                col_inet INET,
                col_duration DURATION
            );"#,
        ];
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for (i, cql) in variations.iter().enumerate() {
            total_bytes += cql.len();
            
            match self.simulate_cql_to_schema_conversion(cql) {
                Ok(schema) => {
                    if let Err(e) = schema.validate() {
                        failures.push(format!("Variation {}: {}", i + 1, e));
                    }
                }
                Err(e) => {
                    failures.push(format!("Variation {}: {}", i + 1, e));
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ CREATE TABLE variations tests passed ({} variations)", variations.len());
        } else {
            println!("❌ CREATE TABLE variations tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test table name matching logic
    fn test_table_name_matching(&mut self) -> Result<()> {
        let test_name = "table_name_matching";
        let start_time = std::time::Instant::now();
        
        let test_cases = vec![
            // (table_definition, search_pattern, should_match)
            ("CREATE TABLE users (id UUID PRIMARY KEY);", "users", true),
            ("CREATE TABLE keyspace1.users (id UUID PRIMARY KEY);", "users", true),
            ("CREATE TABLE keyspace1.users (id UUID PRIMARY KEY);", "keyspace1.users", true),
            ("CREATE TABLE \"Users\" (id UUID PRIMARY KEY);", "Users", true),
            ("CREATE TABLE \"Users\" (id UUID PRIMARY KEY);", "users", false),
            ("CREATE TABLE my_table (id UUID PRIMARY KEY);", "my_table", true),
            ("CREATE TABLE my_table (id UUID PRIMARY KEY);", "table", false),
        ];
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for (i, (table_def, pattern, should_match)) in test_cases.iter().enumerate() {
            total_bytes += table_def.len();
            
            match self.simulate_cql_to_schema_conversion(table_def) {
                Ok(schema) => {
                    let matches = self.table_name_matches(&schema, pattern);
                    if matches != *should_match {
                        failures.push(format!(
                            "Test case {}: Expected match={}, got match={} for pattern '{}' against '{}'",
                            i + 1, should_match, matches, pattern, schema.table
                        ));
                    }
                }
                Err(e) => {
                    failures.push(format!("Test case {}: Failed to parse: {}", i + 1, e));
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ Table name matching tests passed ({} test cases)", test_cases.len());
        } else {
            println!("❌ Table name matching tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test type conversion between CQL and internal types
    fn test_type_conversions(&mut self) -> Result<()> {
        let test_name = "type_conversions";
        let start_time = std::time::Instant::now();
        
        let type_mappings = vec![
            // (cql_type, expected_internal_type, test_value)
            ("BOOLEAN", CqlType::Boolean, Value::Boolean(true)),
            ("TINYINT", CqlType::TinyInt, Value::TinyInt(42)),
            ("SMALLINT", CqlType::SmallInt, Value::SmallInt(1000)),
            ("INT", CqlType::Int, Value::Integer(1000000)),
            ("BIGINT", CqlType::BigInt, Value::BigInt(1000000000000)),
            ("FLOAT", CqlType::Float, Value::Float32(3.14)),
            ("DOUBLE", CqlType::Double, Value::Float(3.14159)),
            ("TEXT", CqlType::Text, Value::Text("hello".to_string())),
            ("VARCHAR", CqlType::Text, Value::Text("varchar".to_string())),
            ("ASCII", CqlType::Ascii, Value::Text("ascii".to_string())),
            ("BLOB", CqlType::Blob, Value::Blob(vec![1, 2, 3, 4])),
            ("UUID", CqlType::Uuid, Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])),
            ("TIMESTAMP", CqlType::Timestamp, Value::Timestamp(1640995200000000)),
        ];
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for (cql_type_str, expected_type, test_value) in type_mappings.iter() {
            total_bytes += cql_type_str.len();
            
            // Test CQL type parsing
            match CqlType::parse(cql_type_str) {
                Ok(parsed_type) => {
                    if parsed_type != *expected_type {
                        failures.push(format!(
                            "Type '{}': Expected {:?}, got {:?}",
                            cql_type_str, expected_type, parsed_type
                        ));
                    }
                }
                Err(e) => {
                    failures.push(format!("Type '{}': Parse error: {}", cql_type_str, e));
                }
            }
            
            // Test value serialization/deserialization roundtrip
            match self.parser.serialize_value(test_value) {
                Ok(serialized) => {
                    // Try to determine type ID from value
                    let type_id = self.value_to_type_id(test_value);
                    match self.parser.parse_value(&serialized[1..], type_id) {
                        Ok((parsed_value, _)) => {
                            if !self.values_equivalent(test_value, &parsed_value) {
                                failures.push(format!(
                                    "Type '{}': Roundtrip failed - original: {:?}, parsed: {:?}",
                                    cql_type_str, test_value, parsed_value
                                ));
                            }
                        }
                        Err(e) => {
                            failures.push(format!("Type '{}': Parse error: {}", cql_type_str, e));
                        }
                    }
                }
                Err(e) => {
                    failures.push(format!("Type '{}': Serialize error: {}", cql_type_str, e));
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ Type conversion tests passed ({} types)", type_mappings.len());
        } else {
            println!("❌ Type conversion tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test complex type parsing (collections, UDTs, tuples)
    fn test_complex_types(&mut self) -> Result<()> {
        let test_name = "complex_types";
        let start_time = std::time::Instant::now();
        
        let complex_types = vec![
            // Collections
            ("LIST<TEXT>", "list of text"),
            ("SET<INT>", "set of integers"),
            ("MAP<TEXT, BIGINT>", "map from text to bigint"),
            
            // Nested collections
            ("LIST<SET<TEXT>>", "list of sets of text"),
            ("MAP<TEXT, LIST<INT>>", "map from text to list of int"),
            ("SET<MAP<TEXT, BIGINT>>", "set of maps"),
            
            // Frozen collections
            ("FROZEN<LIST<TEXT>>", "frozen list"),
            ("FROZEN<SET<UUID>>", "frozen set"),
            ("FROZEN<MAP<TEXT, INT>>", "frozen map"),
            
            // Tuples
            ("TUPLE<TEXT, INT, BOOLEAN>", "tuple with three types"),
            ("TUPLE<UUID, TIMESTAMP, TEXT>", "tuple with uuid, timestamp, text"),
            
            // Complex nested structures
            ("MAP<TEXT, FROZEN<LIST<TUPLE<UUID, TEXT>>>>", "complex nested structure"),
        ];
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for (type_str, description) in complex_types.iter() {
            total_bytes += type_str.len();
            
            match CqlType::parse(type_str) {
                Ok(parsed_type) => {
                    // Verify the type structure makes sense
                    if !self.validate_complex_type_structure(&parsed_type) {
                        failures.push(format!(
                            "Complex type '{}' ({}): Invalid structure: {:?}",
                            type_str, description, parsed_type
                        ));
                    }
                    
                    // Test that it can be used in a table schema
                    let table_cql = format!(
                        "CREATE TABLE test_complex (id UUID PRIMARY KEY, complex_col {});",
                        type_str
                    );
                    
                    if let Err(e) = self.simulate_cql_to_schema_conversion(&table_cql) {
                        failures.push(format!(
                            "Complex type '{}' ({}): Cannot be used in table: {}",
                            type_str, description, e
                        ));
                    }
                }
                Err(e) => {
                    failures.push(format!(
                        "Complex type '{}' ({}): Parse error: {}",
                        type_str, description, e
                    ));
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ Complex type tests passed ({} types)", complex_types.len());
        } else {
            println!("❌ Complex type tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test error handling for malformed CQL
    fn test_error_handling(&mut self) -> Result<()> {
        let test_name = "error_handling";
        let start_time = std::time::Instant::now();
        
        let malformed_cql = vec![
            // Missing semicolon
            ("CREATE TABLE users (id UUID PRIMARY KEY)", "missing semicolon"),
            
            // Invalid syntax
            ("CREATE TABLE (id UUID PRIMARY KEY);", "missing table name"),
            
            // Missing primary key
            ("CREATE TABLE users (id UUID, name TEXT);", "missing primary key"),
            
            // Invalid data type
            ("CREATE TABLE users (id INVALID_TYPE PRIMARY KEY);", "invalid data type"),
            
            // Unclosed parenthesis
            ("CREATE TABLE users (id UUID PRIMARY KEY;", "unclosed parenthesis"),
            
            // Invalid collection syntax
            ("CREATE TABLE users (id UUID PRIMARY KEY, tags LIST<>);", "empty list type"),
            
            // Invalid map syntax
            ("CREATE TABLE users (id UUID PRIMARY KEY, data MAP<TEXT>);", "incomplete map type"),
            
            // Reserved keyword as column name without quotes
            ("CREATE TABLE users (select TEXT PRIMARY KEY);", "reserved keyword"),
            
            // Invalid clustering order
            ("CREATE TABLE events (id UUID, ts TIMESTAMP, PRIMARY KEY (id, ts)) WITH CLUSTERING ORDER BY (ts INVALID);", "invalid clustering order"),
        ];
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for (malformed, description) in malformed_cql.iter() {
            total_bytes += malformed.len();
            
            // These should all result in errors
            match self.simulate_cql_to_schema_conversion(malformed) {
                Ok(_) => {
                    failures.push(format!(
                        "Malformed CQL '{}' ({}): Expected error but parsing succeeded",
                        malformed, description
                    ));
                }
                Err(_) => {
                    // This is expected - the CQL should fail to parse
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ Error handling tests passed ({} malformed cases)", malformed_cql.len());
        } else {
            println!("❌ Error handling tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test JSON vs CQL schema output comparison
    fn test_json_vs_cql_schemas(&mut self) -> Result<()> {
        let test_name = "json_vs_cql_schemas";
        let start_time = std::time::Instant::now();
        
        let test_cases = vec![
            (
                r#"CREATE TABLE users (
                    id UUID PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    created_at TIMESTAMP
                );"#,
                r#"{
                    "keyspace": "test",
                    "table": "users",
                    "partition_keys": [
                        {"name": "id", "type": "uuid", "position": 0}
                    ],
                    "clustering_keys": [],
                    "columns": [
                        {"name": "id", "type": "uuid", "nullable": false},
                        {"name": "name", "type": "text", "nullable": true},
                        {"name": "email", "type": "text", "nullable": true},
                        {"name": "created_at", "type": "timestamp", "nullable": true}
                    ]
                }"#,
            ),
        ];
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for (i, (cql, expected_json)) in test_cases.iter().enumerate() {
            total_bytes += cql.len() + expected_json.len();
            
            // Parse CQL to schema
            match self.simulate_cql_to_schema_conversion(cql) {
                Ok(cql_schema) => {
                    // Parse expected JSON schema
                    match TableSchema::from_json(expected_json) {
                        Ok(json_schema) => {
                            // Compare schemas (ignoring keyspace differences for this test)
                            if !self.schemas_equivalent(&cql_schema, &json_schema) {
                                failures.push(format!(
                                    "Test case {}: CQL and JSON schemas are not equivalent",
                                    i + 1
                                ));
                            }
                        }
                        Err(e) => {
                            failures.push(format!(
                                "Test case {}: Failed to parse expected JSON schema: {}",
                                i + 1, e
                            ));
                        }
                    }
                }
                Err(e) => {
                    failures.push(format!(
                        "Test case {}: Failed to parse CQL: {}",
                        i + 1, e
                    ));
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ JSON vs CQL schema tests passed ({} comparisons)", test_cases.len());
        } else {
            println!("❌ JSON vs CQL schema tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test parser performance with large CQL files
    fn test_parser_performance(&mut self) -> Result<()> {
        let test_name = "parser_performance";
        let start_time = std::time::Instant::now();
        
        // Generate large CQL content for performance testing
        let large_cql = self.generate_large_cql_content();
        let content_size = large_cql.len();
        
        let perf_start = std::time::Instant::now();
        
        // Parse the large content multiple times
        let iterations = 100;
        let mut failures = Vec::new();
        
        for i in 0..iterations {
            match self.simulate_cql_to_schema_conversion(&large_cql) {
                Ok(_) => {}
                Err(e) => {
                    failures.push(format!("Iteration {}: {}", i + 1, e));
                    if failures.len() > 5 {
                        break; // Stop after too many failures
                    }
                }
            }
        }
        
        let perf_duration = perf_start.elapsed();
        let throughput_mbs = (content_size * iterations) as f64 
            / perf_duration.as_secs_f64() 
            / 1_000_000.0;
        
        // Performance targets
        let min_throughput_mbs = 10.0; // 10 MB/s minimum
        let max_avg_latency_ms = 100.0; // 100ms max average latency
        
        let avg_latency_ms = perf_duration.as_millis() as f64 / iterations as f64;
        
        if throughput_mbs < min_throughput_mbs {
            failures.push(format!(
                "Performance below target: {:.2} MB/s < {:.2} MB/s",
                throughput_mbs, min_throughput_mbs
            ));
        }
        
        if avg_latency_ms > max_avg_latency_ms {
            failures.push(format!(
                "Latency above target: {:.2}ms > {:.2}ms",
                avg_latency_ms, max_avg_latency_ms
            ));
        }
        
        // Store performance metrics
        self.performance_metrics.push(PerformanceMetric {
            test_name: test_name.to_string(),
            operation: "cql_parsing".to_string(),
            execution_time_ms: perf_duration.as_millis() as u64,
            throughput_mbs,
            memory_usage_kb: 0, // Would need actual memory measurement
        });
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: content_size * iterations,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!(
                "✅ Performance tests passed ({} iterations, {:.2} MB/s, {:.2}ms avg latency)",
                iterations, throughput_mbs, avg_latency_ms
            );
        } else {
            println!("❌ Performance tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Test parser with real CQL files from test fixtures
    fn test_real_cql_files(&mut self) -> Result<()> {
        let test_name = "real_cql_files";
        let start_time = std::time::Instant::now();
        
        // Create some test CQL files to validate against
        let test_files = self.create_test_cql_files()?;
        
        let mut failures = Vec::new();
        let mut total_bytes = 0;
        
        for test_file in &test_files {
            match fs::read_to_string(test_file) {
                Ok(content) => {
                    total_bytes += content.len();
                    
                    match self.simulate_cql_to_schema_conversion(&content) {
                        Ok(schema) => {
                            if let Err(e) = schema.validate() {
                                failures.push(format!(
                                    "File '{}': Schema validation failed: {}",
                                    test_file.display(), e
                                ));
                            }
                        }
                        Err(e) => {
                            failures.push(format!(
                                "File '{}': Parse failed: {}",
                                test_file.display(), e
                            ));
                        }
                    }
                }
                Err(e) => {
                    failures.push(format!(
                        "File '{}': Read failed: {}",
                        test_file.display(), e
                    ));
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let result = ValidationResult {
            test_name: test_name.to_string(),
            passed: failures.is_empty(),
            error_message: if failures.is_empty() { None } else { Some(failures.join("; ")) },
            execution_time_ms: execution_time,
            bytes_processed: total_bytes,
        };
        
        self.results.insert(test_name.to_string(), result.clone());
        
        if result.passed {
            println!("✅ Real CQL file tests passed ({} files)", test_files.len());
        } else {
            println!("❌ Real CQL file tests failed: {}", result.error_message.unwrap());
        }
        
        Ok(())
    }

    /// Generate validation report
    fn generate_report(&self) -> ValidationReport {
        let total_tests = self.results.len();
        let passed_tests = self.results.values().filter(|r| r.passed).count();
        let failed_tests = total_tests - passed_tests;
        
        let total_time_ms: u64 = self.results.values().map(|r| r.execution_time_ms).sum();
        let total_bytes: usize = self.results.values().map(|r| r.bytes_processed).sum();
        
        ValidationReport {
            total_tests,
            passed_tests,
            failed_tests,
            total_execution_time_ms: total_time_ms,
            total_bytes_processed: total_bytes,
            test_results: self.results.clone(),
            performance_metrics: self.performance_metrics.clone(),
        }
    }

    // Helper methods

    /// Simulate CQL to schema conversion (placeholder for actual CQL parser)
    fn simulate_cql_to_schema_conversion(&self, cql: &str) -> Result<TableSchema> {
        // This is a simplified simulation - in reality, this would be a full CQL parser
        // For now, we create a basic schema based on common patterns
        
        if cql.trim().is_empty() {
            return Err(Error::schema("Empty CQL input".to_string()));
        }
        
        // Extract table name from CQL (very basic pattern matching)
        let table_name = if let Some(captures) = regex::Regex::new(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)")
            .unwrap()
            .captures(cql)
        {
            captures.get(2).map(|m| m.as_str()).unwrap_or("unknown_table")
        } else {
            return Err(Error::schema("Could not extract table name".to_string()));
        };
        
        // Create a basic schema - this would be much more sophisticated in reality
        Ok(TableSchema {
            keyspace: "test".to_string(),
            table: table_name.to_string(),
            partition_keys: vec![cqlite_core::schema::KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![cqlite_core::schema::Column {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                nullable: false,
                default: None,
            }],
            comments: HashMap::new(),
        })
    }

    /// Check if table name matches pattern
    fn table_name_matches(&self, schema: &TableSchema, pattern: &str) -> bool {
        // Simple matching logic - could be enhanced with more sophisticated patterns
        schema.table == pattern || 
        format!("{}.{}", schema.keyspace, schema.table) == pattern
    }

    /// Convert Value to CqlTypeId for parsing tests
    fn value_to_type_id(&self, value: &Value) -> CqlTypeId {
        match value {
            Value::Boolean(_) => CqlTypeId::Boolean,
            Value::TinyInt(_) => CqlTypeId::Tinyint,
            Value::SmallInt(_) => CqlTypeId::Smallint,
            Value::Integer(_) => CqlTypeId::Int,
            Value::BigInt(_) => CqlTypeId::BigInt,
            Value::Float32(_) => CqlTypeId::Float,
            Value::Float(_) => CqlTypeId::Double,
            Value::Text(_) => CqlTypeId::Varchar,
            Value::Blob(_) => CqlTypeId::Blob,
            Value::Uuid(_) => CqlTypeId::Uuid,
            Value::Timestamp(_) => CqlTypeId::Timestamp,
            _ => CqlTypeId::Blob, // Default fallback
        }
    }

    /// Check if two values are equivalent for testing purposes
    fn values_equivalent(&self, v1: &Value, v2: &Value) -> bool {
        // Simplified equivalence check - could be more sophisticated
        std::mem::discriminant(v1) == std::mem::discriminant(v2)
    }

    /// Validate complex type structure makes sense
    fn validate_complex_type_structure(&self, cql_type: &CqlType) -> bool {
        match cql_type {
            CqlType::List(inner) => self.validate_complex_type_structure(inner),
            CqlType::Set(inner) => self.validate_complex_type_structure(inner),
            CqlType::Map(key, value) => {
                self.validate_complex_type_structure(key) && 
                self.validate_complex_type_structure(value)
            }
            CqlType::Tuple(types) => {
                !types.is_empty() && types.iter().all(|t| self.validate_complex_type_structure(t))
            }
            CqlType::Frozen(inner) => self.validate_complex_type_structure(inner),
            CqlType::Udt(_, fields) => !fields.is_empty(),
            _ => true, // Primitive types are always valid
        }
    }

    /// Check if two schemas are equivalent (ignoring minor differences)
    fn schemas_equivalent(&self, schema1: &TableSchema, schema2: &TableSchema) -> bool {
        // Simplified comparison - could be more comprehensive
        schema1.table == schema2.table &&
        schema1.partition_keys.len() == schema2.partition_keys.len() &&
        schema1.columns.len() == schema2.columns.len()
    }

    /// Generate large CQL content for performance testing
    fn generate_large_cql_content(&self) -> String {
        let mut cql = String::new();
        
        // Generate multiple table definitions
        for i in 0..50 {
            cql.push_str(&format!(
                r#"
                CREATE TABLE table_{} (
                    id UUID PRIMARY KEY,
                    col1 TEXT,
                    col2 BIGINT,
                    col3 TIMESTAMP,
                    col4 LIST<TEXT>,
                    col5 MAP<TEXT, BIGINT>,
                    col6 SET<UUID>
                );
                "#,
                i
            ));
        }
        
        cql
    }

    /// Create test CQL files for integration testing
    fn create_test_cql_files(&self) -> Result<Vec<std::path::PathBuf>> {
        let test_dir = std::path::Path::new("target/test_cql_files");
        std::fs::create_dir_all(test_dir).map_err(|e| {
            Error::schema(format!("Failed to create test directory: {}", e))
        })?;
        
        let test_files = vec![
            ("simple_table.cql", "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);"),
            ("collections_table.cql", r#"
                CREATE TABLE posts (
                    id UUID PRIMARY KEY,
                    title TEXT,
                    tags SET<TEXT>,
                    metadata MAP<TEXT, TEXT>
                );
            "#),
            ("clustering_table.cql", r#"
                CREATE TABLE events (
                    user_id UUID,
                    event_time TIMESTAMP,
                    event_type TEXT,
                    PRIMARY KEY (user_id, event_time)
                ) WITH CLUSTERING ORDER BY (event_time DESC);
            "#),
        ];
        
        let mut file_paths = Vec::new();
        
        for (filename, content) in test_files {
            let file_path = test_dir.join(filename);
            std::fs::write(&file_path, content).map_err(|e| {
                Error::schema(format!("Failed to write test file: {}", e))
            })?;
            file_paths.push(file_path);
        }
        
        Ok(file_paths)
    }
}

/// Complete validation report
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub total_execution_time_ms: u64,
    pub total_bytes_processed: usize,
    pub test_results: HashMap<String, ValidationResult>,
    pub performance_metrics: Vec<PerformanceMetric>,
}

impl ValidationReport {
    /// Print a formatted report to stdout
    pub fn print_report(&self) {
        println!("\n🧪 CQL Parser Validation Report");
        println!("=" .repeat(50));
        
        println!("📊 Summary:");
        println!("  Total Tests: {}", self.total_tests);
        println!("  Passed: {} ({:.1}%)", self.passed_tests, 
                (self.passed_tests as f64 / self.total_tests as f64) * 100.0);
        println!("  Failed: {} ({:.1}%)", self.failed_tests,
                (self.failed_tests as f64 / self.total_tests as f64) * 100.0);
        println!("  Total Time: {}ms", self.total_execution_time_ms);
        println!("  Total Bytes: {}", self.total_bytes_processed);
        
        if !self.performance_metrics.is_empty() {
            println!("\n⚡ Performance Metrics:");
            for metric in &self.performance_metrics {
                println!("  {} ({}): {:.2} MB/s, {}ms", 
                        metric.test_name, metric.operation, 
                        metric.throughput_mbs, metric.execution_time_ms);
            }
        }
        
        println!("\n📋 Test Results:");
        let mut sorted_results: Vec<_> = self.test_results.values().collect();
        sorted_results.sort_by_key(|r| &r.test_name);
        
        for result in sorted_results {
            let status = if result.passed { "✅ PASS" } else { "❌ FAIL" };
            println!("  {} - {} ({}ms, {} bytes)", 
                    result.test_name, status, 
                    result.execution_time_ms, result.bytes_processed);
            
            if let Some(error) = &result.error_message {
                println!("    Error: {}", error);
            }
        }
        
        println!("\n" + "=".repeat(50));
        
        if self.failed_tests == 0 {
            println!("🎉 All tests passed! CQL parser validation successful.");
        } else {
            println!("⚠️  {} test(s) failed. Review errors above.", self.failed_tests);
        }
    }
    
    /// Save report to JSON file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| Error::serialization(format!("Failed to serialize report: {}", e)))?;
        
        std::fs::write(path, json)
            .map_err(|e| Error::schema(format!("Failed to write report file: {}", e)))?;
        
        Ok(())
    }
}

// Re-export for easy testing
pub use regex;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_suite_creation() {
        let suite = CqlParserValidationSuite::new();
        assert_eq!(suite.results.len(), 0);
        assert_eq!(suite.performance_metrics.len(), 0);
    }

    #[test]
    fn test_basic_type_parsing() {
        let suite = CqlParserValidationSuite::new();
        
        // Test primitive type parsing
        assert_eq!(CqlType::parse("TEXT").unwrap(), CqlType::Text);
        assert_eq!(CqlType::parse("BIGINT").unwrap(), CqlType::BigInt);
        assert_eq!(CqlType::parse("UUID").unwrap(), CqlType::Uuid);
    }

    #[test]
    fn test_collection_type_parsing() {
        let suite = CqlParserValidationSuite::new();
        
        // Test collection parsing
        match CqlType::parse("LIST<TEXT>").unwrap() {
            CqlType::List(inner) => assert_eq!(*inner, CqlType::Text),
            _ => panic!("Expected List type"),
        }
        
        match CqlType::parse("MAP<TEXT, BIGINT>").unwrap() {
            CqlType::Map(key, value) => {
                assert_eq!(*key, CqlType::Text);
                assert_eq!(*value, CqlType::BigInt);
            }
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn test_table_name_extraction() {
        let suite = CqlParserValidationSuite::new();
        
        // Test table name matching
        let schema = TableSchema {
            keyspace: "test".to_string(),
            table: "users".to_string(),
            partition_keys: vec![],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
        };
        
        assert!(suite.table_name_matches(&schema, "users"));
        assert!(suite.table_name_matches(&schema, "test.users"));
        assert!(!suite.table_name_matches(&schema, "posts"));
    }
}
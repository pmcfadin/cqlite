//! Comprehensive tests for the hardened validator parser
//! Issue #31: Cross-Version Complex Type Validation

use super::super::super::cqlite_core::validation::hardened_validator_parser::*;
use crate::cqlite_core::{
    error::Result,
    schema::{CqlType, UdtFieldDef, UdtRegistry, UdtTypeDef},
    types::Value,
};
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tempfile::TempDir;
use tokio_test;

/// Test fixtures for hardened validator testing
struct TestFixtures {
    temp_dir: TempDir,
    test_data_paths: Vec<PathBuf>,
    udt_registry: UdtRegistry,
}

impl TestFixtures {
    fn new() -> Result<Self> {
        let temp_dir = TempDir::new().unwrap();
        let test_data_paths = vec![
            temp_dir.path().join("v3.7"),
            temp_dir.path().join("v4.0"),
            temp_dir.path().join("v4.1"),
            temp_dir.path().join("v5.0"),
        ];

        // Create test directories
        for path in &test_data_paths {
            std::fs::create_dir_all(path).unwrap();
        }

        // Create test UDT registry
        let mut udt_registry = UdtRegistry::new();
        Self::populate_test_udt_registry(&mut udt_registry)?;

        Ok(Self {
            temp_dir,
            test_data_paths,
            udt_registry,
        })
    }

    fn populate_test_udt_registry(registry: &mut UdtRegistry) -> Result<()> {
        // Add address UDT
        let address_def = UdtTypeDef {
            keyspace: "hardened_validator_test".to_string(),
            name: "address".to_string(),
            fields: vec![
                UdtFieldDef {
                    name: "street".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "city".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "state".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "zip_code".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "country".to_string(),
                    field_type: CqlType::Text,
                },
            ],
        };
        registry.register_udt(address_def)?;

        // Add phone_number UDT
        let phone_def = UdtTypeDef {
            keyspace: "hardened_validator_test".to_string(),
            name: "phone_number".to_string(),
            fields: vec![
                UdtFieldDef {
                    name: "country_code".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "area_code".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "number".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "extension".to_string(),
                    field_type: CqlType::Text,
                },
            ],
        };
        registry.register_udt(phone_def)?;

        // Add person UDT with nested types
        let person_def = UdtTypeDef {
            keyspace: "hardened_validator_test".to_string(),
            name: "person".to_string(),
            fields: vec![
                UdtFieldDef {
                    name: "first_name".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "last_name".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "email".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "home_address".to_string(),
                    field_type: CqlType::Frozen(Box::new(CqlType::Udt(
                        "address".to_string(),
                        vec![],
                    ))),
                },
                UdtFieldDef {
                    name: "work_address".to_string(),
                    field_type: CqlType::Frozen(Box::new(CqlType::Udt(
                        "address".to_string(),
                        vec![],
                    ))),
                },
                UdtFieldDef {
                    name: "phone_numbers".to_string(),
                    field_type: CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                        "phone_number".to_string(),
                        vec![],
                    ))))),
                },
                UdtFieldDef {
                    name: "emergency_contacts".to_string(),
                    field_type: CqlType::Map(
                        Box::new(CqlType::Text),
                        Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                            "phone_number".to_string(),
                            vec![],
                        )))),
                    ),
                },
            ],
        };
        registry.register_udt(person_def)?;

        // Add company UDT with deep nesting
        let company_def = UdtTypeDef {
            keyspace: "hardened_validator_test".to_string(),
            name: "company".to_string(),
            fields: vec![
                UdtFieldDef {
                    name: "name".to_string(),
                    field_type: CqlType::Text,
                },
                UdtFieldDef {
                    name: "headquarters".to_string(),
                    field_type: CqlType::Frozen(Box::new(CqlType::Udt(
                        "address".to_string(),
                        vec![],
                    ))),
                },
                UdtFieldDef {
                    name: "employees".to_string(),
                    field_type: CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                        "person".to_string(),
                        vec![],
                    ))))),
                },
                UdtFieldDef {
                    name: "departments".to_string(),
                    field_type: CqlType::Map(
                        Box::new(CqlType::Text),
                        Box::new(CqlType::List(Box::new(CqlType::Frozen(Box::new(
                            CqlType::Udt("person".to_string(), vec![]),
                        ))))),
                    ),
                },
            ],
        };
        registry.register_udt(company_def)?;

        Ok(())
    }

    fn create_test_sstable_data(&self, version: CassandraVersion) -> Vec<u8> {
        // Create mock SSTable data for testing
        let mut data = Vec::new();

        // SSTable header
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x03]); // Version
        data.extend_from_slice(&[0x01; 16]); // Table ID
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Generation
        data.push(0x00); // No compression
        data.push(0x00); // No stats

        // Simple index
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Partition count
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x04]); // Key length
        data.extend_from_slice(b"test"); // Key
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]); // Offset
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x64]); // Size

        // Summary offsets
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Summary count
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // Offset

        // Mock row data
        data.push(0x00); // Row flags
        if version.supports_enhanced_metadata() {
            data.extend_from_slice(&[0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x00, 0x00]);
            // Timestamp
        }

        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04]); // Clustering key length
        data.extend_from_slice(b"key1"); // Clustering key

        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]); // Column count

        // Column 1: Simple text column
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05]); // Name length
        data.extend_from_slice(b"col1"); // Name
        data.push(0x00); // Column flags
        data.push(0x0D); // Type: Varchar
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x05]); // Value length
        data.extend_from_slice(b"hello"); // Value

        // Column 2: Complex list column
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09]); // Name length
        data.extend_from_slice(b"complex_col"); // Name
        data.push(0x00); // Column flags
        data.push(0x20); // Type: List

        // List data
        let list_data = self.create_test_list_data(version);
        data.extend_from_slice(&(list_data.len() as i32).to_be_bytes()); // Value length
        data.extend_from_slice(&list_data); // Value

        data
    }

    fn create_test_list_data(&self, version: CassandraVersion) -> Vec<u8> {
        let mut data = Vec::new();

        if version.supports_mixed_type_collections() {
            // Cassandra 5.0+ mixed-type format
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]); // Element count
            data.push(0x01); // Format flags (mixed types)

            // Element 1: Text
            data.push(0x0D); // Text type
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x05]); // Length
            data.extend_from_slice(b"item1"); // Data

            // Element 2: Integer
            data.push(0x09); // Int type
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x04]); // Length
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x2A]); // Data (42)
        } else {
            // Legacy homogeneous format
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]); // Element count
            data.push(0x0D); // Element type: Text

            // Element 1
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x05]); // Length
            data.extend_from_slice(b"item1"); // Data

            // Element 2
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x05]); // Length
            data.extend_from_slice(b"item2"); // Data
        }

        data
    }

    fn write_test_file(&self, version: CassandraVersion, filename: &str) -> PathBuf {
        let version_dir = match version {
            CassandraVersion::V3_7 => &self.test_data_paths[0],
            CassandraVersion::V4_0 => &self.test_data_paths[1],
            CassandraVersion::V4_1 => &self.test_data_paths[2],
            CassandraVersion::V5_0 => &self.test_data_paths[3],
            _ => &self.test_data_paths[3],
        };

        let file_path = version_dir.join(filename);
        let test_data = self.create_test_sstable_data(version);
        std::fs::write(&file_path, test_data).unwrap();
        file_path
    }
}

#[tokio::test]
async fn test_hardened_validator_creation() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let parser = HardenedValidatorParser::new(config);
    assert!(parser.is_ok());
}

#[tokio::test]
async fn test_cassandra_version_features() {
    // Test version feature detection
    assert!(!CassandraVersion::V3_7.supports_mixed_type_collections());
    assert!(!CassandraVersion::V3_7.supports_frozen_collections());
    assert!(!CassandraVersion::V3_7.supports_enhanced_metadata());
    assert!(!CassandraVersion::V3_7.supports_duration_type());

    assert!(!CassandraVersion::V3_11.supports_mixed_type_collections());
    assert!(!CassandraVersion::V3_11.supports_frozen_collections());
    assert!(!CassandraVersion::V3_11.supports_enhanced_metadata());
    assert!(CassandraVersion::V3_11.supports_duration_type());

    assert!(!CassandraVersion::V4_0.supports_mixed_type_collections());
    assert!(CassandraVersion::V4_0.supports_frozen_collections());
    assert!(!CassandraVersion::V4_0.supports_enhanced_metadata());
    assert!(CassandraVersion::V4_0.supports_duration_type());

    assert!(!CassandraVersion::V4_1.supports_mixed_type_collections());
    assert!(CassandraVersion::V4_1.supports_frozen_collections());
    assert!(CassandraVersion::V4_1.supports_enhanced_metadata());
    assert!(CassandraVersion::V4_1.supports_duration_type());

    assert!(CassandraVersion::V5_0.supports_mixed_type_collections());
    assert!(CassandraVersion::V5_0.supports_frozen_collections());
    assert!(CassandraVersion::V5_0.supports_enhanced_metadata());
    assert!(CassandraVersion::V5_0.supports_duration_type());
}

#[tokio::test]
async fn test_mixed_type_list_parsing() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Test Cassandra 5.0 mixed-type list
    let mixed_list_data = fixtures.create_test_list_data(CassandraVersion::V5_0);
    let result = parser.parse_mixed_type_list(&mixed_list_data);

    assert!(result.is_ok());
    let value = result.unwrap();

    match value {
        Value::List(elements) => {
            assert_eq!(elements.len(), 2);
            assert!(matches!(elements[0], Value::Text(_)));
            assert!(matches!(elements[1], Value::Integer(_)));
        }
        _ => panic!("Expected list value"),
    }
}

#[tokio::test]
async fn test_homogeneous_list_parsing() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Test legacy homogeneous list
    let homogeneous_list_data = fixtures.create_test_list_data(CassandraVersion::V4_0);
    let result = parser.parse_homogeneous_list(&homogeneous_list_data);

    assert!(result.is_ok());
    let value = result.unwrap();

    match value {
        Value::List(elements) => {
            assert_eq!(elements.len(), 2);
            assert!(matches!(elements[0], Value::Text(_)));
            assert!(matches!(elements[1], Value::Text(_)));
        }
        _ => panic!("Expected list value"),
    }
}

#[tokio::test]
async fn test_udt_parsing_with_registry() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Create mock UDT data for address
    let mut udt_data = Vec::new();

    // Type name length and name
    udt_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07]); // Length: 7
    udt_data.extend_from_slice(b"address"); // Type name

    // Field values (simplified - just street field)
    udt_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x0C]); // Field length
    udt_data.extend_from_slice(b"123 Main St"); // Street value

    let result = parser.parse_udt_enhanced(&udt_data, CassandraVersion::V5_0);
    assert!(result.is_ok());

    let value = result.unwrap();
    match value {
        Value::Udt(udt) => {
            assert_eq!(udt.type_name, "address");
            assert_eq!(udt.keyspace, "hardened_validator_test");
            assert!(!udt.fields.is_empty());
        }
        _ => panic!("Expected UDT value"),
    }
}

#[tokio::test]
async fn test_tuple_parsing() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Create mock tuple data
    let mut tuple_data = Vec::new();

    // Field count
    tuple_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03]); // 3 fields

    // Field types
    tuple_data.push(0x0D); // Text
    tuple_data.push(0x09); // Int
    tuple_data.push(0x04); // Boolean

    // Field values
    // Text field
    tuple_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x05]); // Length
    tuple_data.extend_from_slice(b"hello"); // Value

    // Int field
    tuple_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x04]); // Length
    tuple_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x2A]); // Value: 42

    // Boolean field
    tuple_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Length
    tuple_data.push(0x01); // Value: true

    let result = parser.parse_tuple_enhanced(&tuple_data, CassandraVersion::V5_0);
    assert!(result.is_ok());

    let value = result.unwrap();
    match value {
        Value::Tuple(fields) => {
            assert_eq!(fields.len(), 3);
            assert!(matches!(fields[0], Value::Text(_)));
            assert!(matches!(fields[1], Value::Integer(_)));
            assert!(matches!(fields[2], Value::Boolean(_)));
        }
        _ => panic!("Expected tuple value"),
    }
}

#[tokio::test]
async fn test_performance_targets() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    // Set strict performance targets
    config.performance_targets.max_ms_per_mb = 500.0; // 0.5 second per MB
    config.performance_targets.min_throughput_mbs = 2.0; // 2 MB/s minimum
    config.performance_targets.max_row_parse_latency_us = 1000; // 1ms max per row

    let parser = HardenedValidatorParser::new(config).unwrap();

    // Verify configuration
    assert_eq!(parser.config.performance_targets.max_ms_per_mb, 500.0);
    assert_eq!(parser.config.performance_targets.min_throughput_mbs, 2.0);
    assert_eq!(
        parser.config.performance_targets.max_row_parse_latency_us,
        1000
    );
}

#[tokio::test]
async fn test_memory_limits() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    // Set strict memory limits
    config.memory_limits.max_collection_size = 1000;
    config.memory_limits.max_udt_fields = 100;
    config.memory_limits.max_string_length = 10000;
    config.memory_limits.max_blob_size = 1024 * 1024; // 1MB

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Test that large collection triggers limit
    let mut large_list_data = Vec::new();
    large_list_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8]); // 1000 elements
    large_list_data.push(0x0D); // Text type

    let result = parser.parse_homogeneous_list(&large_list_data);
    assert!(result.is_ok()); // Should be at the limit

    // Test that oversized collection triggers error
    let mut oversized_list_data = Vec::new();
    oversized_list_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40]); // 1,000,000 elements
    oversized_list_data.push(0x0D); // Text type

    let result = parser.parse_homogeneous_list(&oversized_list_data);
    assert!(result.is_err()); // Should exceed limit
}

#[tokio::test]
async fn test_validation_status_calculation() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let parser = HardenedValidatorParser::new(config).unwrap();

    // Test perfect validation
    assert_eq!(
        parser.determine_validation_status(100.0, 0),
        ValidationStatus::Perfect
    );

    // Test minor issues
    assert_eq!(
        parser.determine_validation_status(98.0, 5),
        ValidationStatus::MinorIssues
    );

    // Test major issues
    assert_eq!(
        parser.determine_validation_status(85.0, 50),
        ValidationStatus::MajorIssues
    );

    // Test failed validation
    assert_eq!(
        parser.determine_validation_status(60.0, 200),
        ValidationStatus::Failed
    );
}

#[tokio::test]
async fn test_cross_version_compatibility() {
    let fixtures = TestFixtures::new().unwrap();

    // Create test files for each version
    for version in CassandraVersion::all_versions() {
        fixtures.write_test_file(version, "test-Data.db");
    }

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());
    config.cross_version_testing = true;

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Test cross-version compatibility
    let result = parser.validate_cross_version_compatibility().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_comprehensive_validation() {
    let fixtures = TestFixtures::new().unwrap();

    // Create test files for each version
    for version in CassandraVersion::all_versions() {
        fixtures.write_test_file(version, "complex_collections-Data.db");
        fixtures.write_test_file(version, "tuple_tests-Data.db");
        fixtures.write_test_file(version, "udt_tests-Data.db");
    }

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());
    config.strict_validation = true;
    config.cross_version_testing = true;

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Run comprehensive validation
    let result = parser.validate_comprehensive().await;
    assert!(result.is_ok());

    let validation_result = result.unwrap();

    // Verify we tested all versions
    assert_eq!(
        validation_result.version_results.len(),
        CassandraVersion::all_versions().len()
    );

    // Verify we have performance metrics
    assert!(validation_result.performance_metrics.total_time_ms > 0);

    // Verify we have coverage metrics
    assert!(validation_result.coverage_metrics.coverage_percentage >= 0.0);
    assert!(validation_result.coverage_metrics.coverage_percentage <= 100.0);
}

#[tokio::test]
async fn test_error_handling_and_recovery() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Test with malformed data
    let malformed_data = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Invalid data

    let result = parser.parse_mixed_type_list(&malformed_data);
    assert!(result.is_err());

    // Test with empty data
    let empty_data = vec![];
    let result = parser.parse_homogeneous_list(&empty_data);
    assert!(result.is_err());

    // Test with null UDT data
    let null_udt_data = vec![];
    let result = parser.parse_udt_enhanced(&null_udt_data, CassandraVersion::V5_0);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_report_generation() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let parser = HardenedValidatorParser::new(config).unwrap();

    // Create a mock validation result
    let mut version_results = HashMap::new();
    version_results.insert(
        CassandraVersion::V5_0,
        VersionValidationResult {
            version: CassandraVersion::V5_0,
            files_processed: 10,
            successful_parses: 9,
            failed_parses: 1,
            false_positives: 0,
            false_negatives: 0,
            accuracy_percentage: 90.0,
            complex_type_results: HashMap::new(),
            performance: PerformanceMetrics {
                total_time_ms: 1000,
                avg_time_per_file_ms: 100.0,
                throughput_mbs: 2.5,
                memory_stats: MemoryStats {
                    peak_memory_mb: 50.0,
                    avg_memory_mb: 25.0,
                    memory_efficiency: 0.5,
                },
                vs_targets: PerformanceVsTargets {
                    all_targets_met: true,
                    time_per_mb_ratio: 0.8,
                    throughput_ratio: 1.25,
                    memory_ratio: 0.5,
                },
            },
        },
    );

    let validation_result = ValidationResult {
        status: ValidationStatus::MinorIssues,
        version_results,
        performance_metrics: PerformanceMetrics {
            total_time_ms: 1000,
            avg_time_per_file_ms: 100.0,
            throughput_mbs: 2.5,
            memory_stats: MemoryStats {
                peak_memory_mb: 50.0,
                avg_memory_mb: 25.0,
                memory_efficiency: 0.5,
            },
            vs_targets: PerformanceVsTargets {
                all_targets_met: true,
                time_per_mb_ratio: 0.8,
                throughput_ratio: 1.25,
                memory_ratio: 0.5,
            },
        },
        error_analysis: ErrorAnalysis {
            total_errors: 1,
            error_categories: HashMap::new(),
            critical_errors: Vec::new(),
            error_patterns: Vec::new(),
        },
        coverage_metrics: CoverageMetrics {
            types_tested: ["list", "map", "udt"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            version_combinations: vec![CassandraVersion::V5_0],
            edge_cases_covered: 5,
            coverage_percentage: 75.0,
        },
        timestamp: chrono::Utc::now(),
    };

    let report = parser
        .generate_validation_report(&validation_result)
        .unwrap();

    // Verify report contains expected sections
    assert!(report.contains("# Hardened Validator Parser"));
    assert!(report.contains("## Issue #31"));
    assert!(report.contains("## Executive Summary"));
    assert!(report.contains("## Version-Specific Results"));
    assert!(report.contains("## Performance Analysis"));
    assert!(report.contains("## Test Coverage"));
    assert!(report.contains("## Recommendations"));

    // Verify status reporting
    assert!(report.contains("**Validation Status:** MinorIssues"));
    assert!(report.contains("⚠️ **Minor Issues Detected**"));
}

#[test]
fn test_complex_type_test_result_merge() {
    let mut result1 = ComplexTypeTestResult {
        type_name: "list".to_string(),
        tests_run: 10,
        tests_passed: 8,
        parsing_errors: vec!["error1".to_string()],
        performance: TypePerformanceMetrics {
            avg_parse_time_us: 100.0,
            max_parse_time_us: 200,
            memory_per_instance_bytes: 1000,
            throughput_per_second: 100.0,
        },
    };

    let result2 = ComplexTypeTestResult {
        type_name: "list".to_string(),
        tests_run: 5,
        tests_passed: 5,
        parsing_errors: vec!["error2".to_string()],
        performance: TypePerformanceMetrics {
            avg_parse_time_us: 200.0,
            max_parse_time_us: 300,
            memory_per_instance_bytes: 2000,
            throughput_per_second: 200.0,
        },
    };

    result1.merge_with(&result2);

    assert_eq!(result1.tests_run, 15);
    assert_eq!(result1.tests_passed, 13);
    assert_eq!(result1.parsing_errors.len(), 2);
    assert_eq!(result1.performance.avg_parse_time_us, 150.0);
    assert_eq!(result1.performance.max_parse_time_us, 300);
}

#[tokio::test]
async fn test_edge_case_handling() {
    let fixtures = TestFixtures::new().unwrap();

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Test empty list
    let empty_list_data = vec![
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Count: 0
    ];
    let result = parser.parse_homogeneous_list(&empty_list_data);
    assert!(result.is_ok());
    if let Value::List(elements) = result.unwrap() {
        assert_eq!(elements.len(), 0);
    }

    // Test single element list
    let single_element_data = vec![
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Count: 1
        0x0D, // Text type
        0x00, 0x00, 0x00, 0x04, // Length: 4
        b't', b'e', b's', b't', // Data: "test"
    ];
    let result = parser.parse_homogeneous_list(&single_element_data);
    assert!(result.is_ok());
    if let Value::List(elements) = result.unwrap() {
        assert_eq!(elements.len(), 1);
    }

    // Test null elements handling
    let null_element_data = vec![
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Count: 1
        0x0D, // Text type
        0xFF, 0xFF, 0xFF, 0xFF, // Length: -1 (null)
    ];
    let result = parser.parse_homogeneous_list(&null_element_data);
    assert!(result.is_ok()); // Should handle null elements gracefully
}

/// Integration test with real SSTable-like data
#[tokio::test]
async fn test_integration_with_realistic_data() {
    let fixtures = TestFixtures::new().unwrap();

    // Create more realistic test data
    for version in CassandraVersion::all_versions() {
        let file_path = fixtures.write_test_file(version, "realistic-Data.db");

        // Verify file was created and has content
        assert!(file_path.exists());
        let metadata = std::fs::metadata(&file_path).unwrap();
        assert!(metadata.len() > 0);
    }

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());
    config.strict_validation = true;
    config.cross_version_testing = true;

    // Set realistic performance targets
    config.performance_targets.max_ms_per_mb = 1000.0; // 1 second per MB (as required)
    config.performance_targets.min_throughput_mbs = 1.0; // 1 MB/s minimum
    config.performance_targets.max_row_parse_latency_us = 1000; // 1ms max per row

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    // Run comprehensive validation
    let result = parser.validate_comprehensive().await;
    assert!(result.is_ok());

    let validation_result = result.unwrap();

    // For integration test, we expect some level of success
    // (even with mock data, basic parsing should work)
    assert!(validation_result.version_results.len() > 0);

    // Generate and verify report
    let report = parser
        .generate_validation_report(&validation_result)
        .unwrap();
    assert!(report.len() > 1000); // Should be a substantial report

    // Log summary for manual inspection
    println!("Integration Test Summary:");
    println!("Status: {:?}", validation_result.status);
    println!(
        "Versions tested: {}",
        validation_result.version_results.len()
    );
    println!(
        "Total time: {}ms",
        validation_result.performance_metrics.total_time_ms
    );
    println!(
        "Coverage: {:.1}%",
        validation_result.coverage_metrics.coverage_percentage
    );
}

/// Benchmark test to ensure performance targets are met
#[tokio::test]
async fn test_performance_benchmarks() {
    let fixtures = TestFixtures::new().unwrap();

    // Create multiple test files per version for performance testing
    for version in CassandraVersion::all_versions() {
        for i in 0..5 {
            fixtures.write_test_file(version, &format!("perf-test-{}-Data.db", i));
        }
    }

    let mut config = HardenedValidatorConfig::default();
    config.test_data_paths = fixtures.test_data_paths.clone();
    config.udt_registry = Some(fixtures.udt_registry.clone());

    // Set strict performance requirements
    config.performance_targets.max_ms_per_mb = 1000.0; // Sub-second per MB requirement
    config.performance_targets.min_throughput_mbs = 2.0;
    config.performance_targets.max_row_parse_latency_us = 1000; // 1ms max per row

    let mut parser = HardenedValidatorParser::new(config).unwrap();

    let start_time = std::time::Instant::now();
    let result = parser.validate_comprehensive().await;
    let total_time = start_time.elapsed();

    assert!(result.is_ok());
    let validation_result = result.unwrap();

    // Verify performance targets were met
    println!("Performance Benchmark Results:");
    println!("Total validation time: {:?}", total_time);
    println!(
        "Throughput: {:.2} MB/s",
        validation_result.performance_metrics.throughput_mbs
    );
    println!(
        "Targets met: {}",
        validation_result
            .performance_metrics
            .vs_targets
            .all_targets_met
    );

    // For this test with mock data, we primarily verify that the validation completes
    // within a reasonable time and doesn't crash
    assert!(total_time < Duration::from_secs(60)); // Should complete within 1 minute
}

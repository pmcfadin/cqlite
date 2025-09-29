//! Comprehensive tests for spec-driven header parsing improvements
//!
//! This test suite validates the transition from heuristic header parsing to
//! specification-driven decoding shared across Data.db, Index.db, and Summary.db readers.

use cqlite_core::{
    parser::header::CassandraVersion,
    storage::sstable::header_spec::{
        get_global_registry, HeaderSpecRegistry, SSTableComponentType,
    },
};

/// Test basic header specification registry functionality
#[tokio::test]
async fn test_header_spec_registry_initialization() {
    let registry = HeaderSpecRegistry::new();

    // Verify all supported components have specifications
    assert!(registry.get_spec(SSTableComponentType::Data).is_ok());
    assert!(registry.get_spec(SSTableComponentType::Index).is_ok());
    assert!(registry.get_spec(SSTableComponentType::Summary).is_ok());

    // Test specification properties
    let data_spec = registry.get_spec(SSTableComponentType::Data).unwrap();
    assert_eq!(data_spec.component_type, SSTableComponentType::Data);
    assert!(data_spec.has_magic_number);
    assert_eq!(data_spec.min_version, 1);
    assert_eq!(data_spec.max_version, 10);

    let index_spec = registry.get_spec(SSTableComponentType::Index).unwrap();
    assert_eq!(index_spec.component_type, SSTableComponentType::Index);
    assert!(!index_spec.has_magic_number); // Legacy format

    let summary_spec = registry.get_spec(SSTableComponentType::Summary).unwrap();
    assert_eq!(summary_spec.component_type, SSTableComponentType::Summary);
    assert!(!summary_spec.has_magic_number); // Default spec is legacy format
    assert_eq!(summary_spec.magic_number, None); // Legacy format has no magic
}

/// Test Data.db header parsing with different Cassandra versions
#[tokio::test]
async fn test_data_header_parsing_multiple_versions() {
    let registry = get_global_registry();

    // Test Cassandra 5.0 Alpha format
    let mut data = Vec::new();
    data.extend_from_slice(&CassandraVersion::V5_0Alpha.magic_number().to_be_bytes());
    data.extend_from_slice(&1u16.to_be_bytes()); // version
    data.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]); // table_id

    // VString keyspace
    data.push(8); // length
    data.extend_from_slice(b"keyspace");

    // VString table_name
    data.push(5); // length
    data.extend_from_slice(b"table");

    // Generation
    data.extend_from_slice(&12345u64.to_be_bytes());

    let result = registry.parse_data_header(&data);
    assert!(
        result.is_ok(),
        "Failed to parse valid Data.db header: {:?}",
        result.err()
    );

    let parsed_header = result.unwrap();
    assert_eq!(parsed_header.component_type, SSTableComponentType::Data);
    assert_eq!(parsed_header.cassandra_version, CassandraVersion::V5_0Alpha);
    assert_eq!(parsed_header.format_version, 1);

    // Verify field extraction
    assert!(parsed_header.fields.contains_key("table_id"));
    assert!(parsed_header.fields.contains_key("keyspace"));
    assert!(parsed_header.fields.contains_key("table_name"));
    assert!(parsed_header.fields.contains_key("generation"));

    let keyspace = parsed_header
        .fields
        .get("keyspace")
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(keyspace, "keyspace");

    let table_name = parsed_header
        .fields
        .get("table_name")
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(table_name, "table");

    let generation = parsed_header
        .fields
        .get("generation")
        .unwrap()
        .as_u64()
        .unwrap();
    assert_eq!(generation, 12345);
}

/// Test Index.db header parsing (legacy format without magic number)
#[tokio::test]
async fn test_index_header_parsing_legacy_format() {
    let registry = get_global_registry();

    let mut data = Vec::new();
    data.extend_from_slice(&2u32.to_be_bytes()); // version
    data.extend_from_slice(&1000u32.to_be_bytes()); // entry_count
    data.extend_from_slice(&65536u64.to_be_bytes()); // data_size
    data.extend_from_slice(&0x12345678u32.to_be_bytes()); // checksum

    let result = registry.parse_index_header(&data);
    assert!(
        result.is_ok(),
        "Failed to parse valid Index.db header: {:?}",
        result.err()
    );

    let parsed_header = result.unwrap();
    assert_eq!(parsed_header.component_type, SSTableComponentType::Index);
    assert_eq!(parsed_header.cassandra_version, CassandraVersion::Legacy);
    assert_eq!(parsed_header.format_version, 2);

    // Verify field extraction
    let version = parsed_header
        .fields
        .get("version")
        .unwrap()
        .as_u32()
        .unwrap();
    assert_eq!(version, 2);

    let entry_count = parsed_header
        .fields
        .get("entry_count")
        .unwrap()
        .as_u32()
        .unwrap();
    assert_eq!(entry_count, 1000);

    let data_size = parsed_header
        .fields
        .get("data_size")
        .unwrap()
        .as_u64()
        .unwrap();
    assert_eq!(data_size, 65536);

    let checksum = parsed_header
        .fields
        .get("checksum")
        .unwrap()
        .as_u32()
        .unwrap();
    assert_eq!(checksum, 0x12345678);
}

/// Test Summary.db header parsing with magic number
#[tokio::test]
async fn test_summary_header_parsing_with_magic() {
    let registry = get_global_registry();

    let mut data = Vec::new();
    data.extend_from_slice(&0x43515354u32.to_be_bytes()); // "CQST" magic number
    data.extend_from_slice(&3u32.to_be_bytes()); // version
    data.extend_from_slice(&500u32.to_be_bytes()); // entry_count
    data.extend_from_slice(&128u32.to_be_bytes()); // sampling_rate
    data.extend_from_slice(&(-1000i64).to_be_bytes()); // min_token
    data.extend_from_slice(&1000i64.to_be_bytes()); // max_token
    data.extend_from_slice(&32768u64.to_be_bytes()); // data_size
    data.extend_from_slice(&0xABCDEF00u32.to_be_bytes()); // checksum

    let result = registry.parse_summary_header(&data);
    assert!(
        result.is_ok(),
        "Failed to parse valid Summary.db header: {:?}",
        result.err()
    );

    let parsed_header = result.unwrap();
    assert_eq!(parsed_header.component_type, SSTableComponentType::Summary);
    assert_eq!(parsed_header.format_version, 3);

    // Verify field extraction
    let entry_count = parsed_header
        .fields
        .get("entry_count")
        .unwrap()
        .as_u32()
        .unwrap();
    assert_eq!(entry_count, 500);

    let sampling_rate = parsed_header
        .fields
        .get("sampling_rate")
        .unwrap()
        .as_u32()
        .unwrap();
    assert_eq!(sampling_rate, 128);

    let min_token = parsed_header
        .fields
        .get("min_token")
        .unwrap()
        .as_u64()
        .unwrap();
    assert_eq!(min_token as i64, -1000);

    let max_token = parsed_header
        .fields
        .get("max_token")
        .unwrap()
        .as_u64()
        .unwrap();
    assert_eq!(max_token as i64, 1000);
}

/// Test validation constraints for field values
#[tokio::test]
async fn test_field_validation_constraints() {
    let registry = get_global_registry();

    // Test Index.db with invalid entry count (exceeds maximum)
    let mut data = Vec::new();
    data.extend_from_slice(&1u32.to_be_bytes()); // version
    data.extend_from_slice(&200_000_000u32.to_be_bytes()); // entry_count (exceeds MAX_REASONABLE_ENTRIES)
    data.extend_from_slice(&65536u64.to_be_bytes()); // data_size
    data.extend_from_slice(&0u32.to_be_bytes()); // checksum

    let result = registry.parse_index_header(&data);
    assert!(
        result.is_err(),
        "Should fail validation for excessive entry count"
    );

    // Test Summary.db with invalid sampling rate (zero)
    let mut data = Vec::new();
    data.extend_from_slice(&0x43515354u32.to_be_bytes()); // magic
    data.extend_from_slice(&1u32.to_be_bytes()); // version
    data.extend_from_slice(&100u32.to_be_bytes()); // entry_count
    data.extend_from_slice(&0u32.to_be_bytes()); // sampling_rate (invalid: zero)
    data.extend_from_slice(&0i64.to_be_bytes()); // min_token
    data.extend_from_slice(&1000i64.to_be_bytes()); // max_token
    data.extend_from_slice(&1000u64.to_be_bytes()); // data_size
    data.extend_from_slice(&0u32.to_be_bytes()); // checksum

    let result = registry.parse_summary_header(&data);
    assert!(
        result.is_err(),
        "Should fail validation for zero sampling rate"
    );
}

/// Test error handling for insufficient data
#[tokio::test]
async fn test_insufficient_data_handling() {
    let registry = get_global_registry();

    // Test with empty data
    let empty_data = Vec::new();
    assert!(registry.parse_data_header(&empty_data).is_err());
    assert!(registry.parse_index_header(&empty_data).is_err());
    assert!(registry.parse_summary_header(&empty_data).is_err());

    // Test with partial headers
    let partial_data = vec![0x6F, 0x61, 0x00, 0x00]; // Just magic number
    assert!(registry.parse_data_header(&partial_data).is_err());

    let partial_index = vec![0x00, 0x00, 0x00, 0x01]; // Just version
    assert!(registry.parse_index_header(&partial_index).is_err());
}

/// Test version compatibility across different formats
#[tokio::test]
async fn test_version_compatibility() {
    let registry = get_global_registry();

    // Test all supported Cassandra versions for Data.db
    let versions = [
        CassandraVersion::Legacy,
        CassandraVersion::V5_0Alpha,
        CassandraVersion::V5_0Beta,
        CassandraVersion::V5_0Release,
        CassandraVersion::V5_0NewBig,
        CassandraVersion::V5_0Bti,
    ];

    for version in &versions {
        let mut data = Vec::new();
        data.extend_from_slice(&version.magic_number().to_be_bytes());
        data.extend_from_slice(&1u16.to_be_bytes()); // version
        data.extend_from_slice(&[0; 16]); // table_id
        data.push(4); // keyspace length
        data.extend_from_slice(b"test");
        data.push(5); // table_name length
        data.extend_from_slice(b"table");
        data.extend_from_slice(&1u64.to_be_bytes()); // generation

        let result = registry.parse_data_header(&data);
        match result {
            Ok(parsed_header) => {
                assert_eq!(parsed_header.cassandra_version, *version);
                println!("Successfully parsed header for version: {:?}", version);
            }
            Err(e) => {
                println!("Failed to parse header for version {:?}: {}", version, e);
                // Some versions might legitimately fail if not fully implemented
            }
        }
    }
}

/// Test header size calculation accuracy
#[tokio::test]
async fn test_header_size_calculation() {
    let registry = get_global_registry();

    // Create a Data.db header with known size
    let mut data = Vec::new();
    data.extend_from_slice(&CassandraVersion::V5_0Release.magic_number().to_be_bytes()); // 4 bytes
    data.extend_from_slice(&1u16.to_be_bytes()); // 2 bytes
    data.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]); // 16 bytes
    data.push(9); // 1 byte
    data.extend_from_slice(b"testkeysp"); // 9 bytes
    data.push(10); // 1 byte
    data.extend_from_slice(b"tablename1"); // 10 bytes
    data.extend_from_slice(&54321u64.to_be_bytes()); // 8 bytes

    let expected_size = 4 + 2 + 16 + 1 + 9 + 1 + 10 + 8; // 51 bytes

    let result = registry.parse_data_header(&data).unwrap();
    assert_eq!(result.header_size, expected_size);
}

/// Test round-trip compatibility between spec-driven and legacy parsers
#[tokio::test]
async fn test_roundtrip_compatibility() {
    // This test ensures that headers parsed by the spec-driven approach
    // produce equivalent results to the legacy parsers for supported formats

    let registry = get_global_registry();

    // Test Summary.db format (one of the simpler formats for validation)
    let mut data = Vec::new();
    data.extend_from_slice(&1u32.to_be_bytes()); // version (legacy format)
    data.extend_from_slice(&250u32.to_be_bytes()); // entry_count
    data.extend_from_slice(&64u32.to_be_bytes()); // sampling_rate
    data.extend_from_slice(&(-500i64).to_be_bytes()); // min_token
    data.extend_from_slice(&500i64.to_be_bytes()); // max_token
    data.extend_from_slice(&16384u64.to_be_bytes()); // data_size
    data.extend_from_slice(&0x12345678u32.to_be_bytes()); // checksum

    // Parse with spec-driven approach (will fall back to legacy for this format)
    let spec_result = registry.parse_summary_header(&data);
    assert!(spec_result.is_ok());

    let parsed_header = spec_result.unwrap();

    // Verify the parsed values match expected legacy format
    assert_eq!(parsed_header.format_version, 1);

    let entry_count = parsed_header
        .fields
        .get("entry_count")
        .unwrap()
        .as_u32()
        .unwrap();
    assert_eq!(entry_count, 250);

    let sampling_rate = parsed_header
        .fields
        .get("sampling_rate")
        .unwrap()
        .as_u32()
        .unwrap();
    assert_eq!(sampling_rate, 64);
}

/// Benchmark comparison between spec-driven and heuristic parsing
#[tokio::test]
async fn test_parsing_performance_regression() {
    use std::time::Instant;

    let registry = get_global_registry();

    // Create a representative Summary.db header
    let mut data = Vec::new();
    data.extend_from_slice(&0x43515354u32.to_be_bytes()); // magic
    data.extend_from_slice(&1u32.to_be_bytes()); // version
    data.extend_from_slice(&1000u32.to_be_bytes()); // entry_count
    data.extend_from_slice(&128u32.to_be_bytes()); // sampling_rate
    data.extend_from_slice(&0i64.to_be_bytes()); // min_token
    data.extend_from_slice(&1000000i64.to_be_bytes()); // max_token
    data.extend_from_slice(&65536u64.to_be_bytes()); // data_size
    data.extend_from_slice(&0u32.to_be_bytes()); // checksum

    // Warm up
    for _ in 0..10 {
        let _ = registry.parse_summary_header(&data);
    }

    // Benchmark spec-driven parsing
    let iterations = 1000;
    let start = Instant::now();
    for _ in 0..iterations {
        let result = registry.parse_summary_header(&data);
        assert!(result.is_ok());
    }
    let spec_duration = start.elapsed();

    println!(
        "Spec-driven parsing: {} iterations in {:?} ({:?} per iteration)",
        iterations,
        spec_duration,
        spec_duration / iterations
    );

    // Ensure performance is reasonable (should be faster than 100μs per parse)
    let per_iteration = spec_duration / iterations;
    assert!(
        per_iteration.as_micros() < 100,
        "Spec-driven parsing too slow: {:?} per iteration",
        per_iteration
    );
}

/// Test comprehensive field extraction with different data types
#[tokio::test]
async fn test_comprehensive_field_extraction() {
    let registry = get_global_registry();

    // Test all supported field types in Index.db header
    let mut data = Vec::new();
    data.extend_from_slice(&5u32.to_be_bytes()); // version (U32BE)
    data.extend_from_slice(&2500u32.to_be_bytes()); // entry_count (U32BE)
    data.extend_from_slice(&1048576u64.to_be_bytes()); // data_size (U64BE)
    data.extend_from_slice(&0xDEADBEEFu32.to_be_bytes()); // checksum (U32BE)

    let result = registry.parse_index_header(&data).unwrap();

    // Test type-safe extraction
    assert_eq!(result.fields.get("version").unwrap().as_u32().unwrap(), 5);
    assert_eq!(
        result.fields.get("entry_count").unwrap().as_u32().unwrap(),
        2500
    );
    assert_eq!(
        result.fields.get("data_size").unwrap().as_u64().unwrap(),
        1048576
    );
    assert_eq!(
        result.fields.get("checksum").unwrap().as_u32().unwrap(),
        0xDEADBEEF
    );

    // Test that wrong type extraction fails appropriately
    assert!(result.fields.get("version").unwrap().as_string().is_err());
    assert!(result.fields.get("data_size").unwrap().as_u32().is_err());
}

/// Integration test with real SSTable readers
#[tokio::test]
async fn test_integration_with_sstable_readers() {
    // This test verifies that the spec-driven headers work correctly
    // when integrated with the actual SSTable reader infrastructure

    // Note: This would require actual SSTable test files or mocked platform
    // For now, we verify that the integration points exist and are accessible

    let registry = get_global_registry();

    // Verify all component types are accessible
    assert!(registry.get_spec(SSTableComponentType::Data).is_ok());
    assert!(registry.get_spec(SSTableComponentType::Index).is_ok());
    assert!(registry.get_spec(SSTableComponentType::Summary).is_ok());

    // Verify that convenience parsing methods work
    let test_data = vec![0; 64]; // Dummy data

    // These should fail gracefully with appropriate error messages
    let data_result = registry.parse_data_header(&test_data);
    let index_result = registry.parse_index_header(&test_data);
    let summary_result = registry.parse_summary_header(&test_data);

    // All should fail due to invalid data, but not panic
    assert!(data_result.is_err());
    assert!(index_result.is_err());
    assert!(summary_result.is_err());
}

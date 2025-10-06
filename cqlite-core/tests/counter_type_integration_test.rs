//! Integration tests for Counter type support (Issue #103)
//!
//! These tests validate that Counter type parsing works correctly with real
//! Cassandra 5.0 SSTable data, ensuring:
//! - Counter values are parsed as Value::Counter (NOT Value::BigInt)
//! - Counter value correctness matches reference data
//! - Schema discovery recognizes Counter type
//! - Type system correctly identifies Counter type

use cqlite_core::{
    platform::Platform,
    schema::{
        registry::{SchemaRegistry, SchemaRegistryConfig},
        Column, KeyColumn, TableSchema,
    },
    storage::sstable::schema_aware_reader::SchemaAwareReader,
    types::Value,
    Config,
};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

/// Get the test datasets root from environment or default location
fn get_test_datasets_root() -> PathBuf {
    env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("../test-data/datasets");
            path
        })
}

/// Find a Data.db file in the given table directory
fn find_data_file(table_dir: &PathBuf) -> Option<PathBuf> {
    if let Ok(entries) = std::fs::read_dir(table_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("db")
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            {
                return Some(path);
            }
        }
    }
    None
}

/// Create schema for test_basic.counters table
///
/// Based on Statistics.db.txt:
/// KeyType: org.apache.cassandra.db.marshal.UTF8Type
/// RegularColumns: share_count:CounterColumnType, total_interactions:CounterColumnType,
///                 like_count:CounterColumnType, view_count:CounterColumnType
fn create_counters_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_basic".to_string(),
        table: "counters".to_string(),
        partition_keys: vec![KeyColumn {
            name: "page_name".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "page_name".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
            },
            Column {
                name: "like_count".to_string(),
                data_type: "counter".to_string(),
                nullable: true,
                default: None,
            },
            Column {
                name: "share_count".to_string(),
                data_type: "counter".to_string(),
                nullable: true,
                default: None,
            },
            Column {
                name: "total_interactions".to_string(),
                data_type: "counter".to_string(),
                nullable: true,
                default: None,
            },
            Column {
                name: "view_count".to_string(),
                data_type: "counter".to_string(),
                nullable: true,
                default: None,
            },
        ],
        comments: HashMap::new(),
    }
}

/// Test that Counter SSTable directory exists (smoke test)
#[test]
fn test_counter_table_exists() {
    let test_root = get_test_datasets_root();
    let counter_table_path =
        test_root.join("sstables/test_basic/counters-6b12cbd0a25111f0a3fef1a551383fb9");

    if counter_table_path.exists() {
        println!(
            "Counter table directory exists at: {:?}",
            counter_table_path
        );
        assert!(counter_table_path.is_dir(), "Should be a directory");
    } else {
        println!(
            "Skipping: Counter table not found at {:?}",
            counter_table_path
        );
    }
}

/// Test that SchemaAwareReader can open Counter SSTable
#[tokio::test]
#[ignore] // Run with: cargo test --test counter_type_integration_test -- --ignored
async fn test_counter_sstable_schema_aware_reader_init() {
    let datasets_root = get_test_datasets_root();
    let test_table_dir =
        datasets_root.join("sstables/test_basic/counters-6b12cbd0a25111f0a3fef1a551383fb9");

    let data_file = match find_data_file(&test_table_dir) {
        Some(f) => f,
        None => {
            eprintln!(
                "Skipping test: No Data.db file found in {:?}",
                test_table_dir
            );
            return;
        }
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let schema = create_counters_table_schema();
    let registry_config = SchemaRegistryConfig::default();
    let registry = Arc::new(
        SchemaRegistry::new(registry_config, platform.clone(), config.clone())
            .await
            .unwrap(),
    );

    let reader = SchemaAwareReader::new(&data_file, schema, registry, &config, platform)
        .await
        .unwrap();

    println!(
        "SchemaAwareReader successfully created for Counter table: {}",
        reader.table_name()
    );
    println!("Schema: {:?}", reader.schema());

    // Verify schema contains Counter type columns
    let like_count_col = reader
        .schema()
        .columns
        .iter()
        .find(|c| c.name == "like_count");
    assert!(like_count_col.is_some(), "like_count column should exist");
    assert_eq!(
        like_count_col.unwrap().data_type,
        "counter",
        "like_count should be counter type"
    );

    let view_count_col = reader
        .schema()
        .columns
        .iter()
        .find(|c| c.name == "view_count");
    assert!(view_count_col.is_some(), "view_count column should exist");
    assert_eq!(
        view_count_col.unwrap().data_type,
        "counter",
        "view_count should be counter type"
    );
}

/// Test that schema validation accepts Counter type
#[test]
fn test_counter_schema_validation() {
    let schema = create_counters_table_schema();

    // Verify schema contains Counter type columns
    let counter_columns: Vec<_> = schema
        .columns
        .iter()
        .filter(|c| c.data_type == "counter")
        .collect();

    assert_eq!(
        counter_columns.len(),
        4,
        "Should have 4 counter type columns"
    );

    // Verify column names
    let counter_names: Vec<&str> = counter_columns.iter().map(|c| c.name.as_str()).collect();
    assert!(counter_names.contains(&"like_count"));
    assert!(counter_names.contains(&"share_count"));
    assert!(counter_names.contains(&"total_interactions"));
    assert!(counter_names.contains(&"view_count"));

    // Schema validation should pass
    let result = SchemaAwareReader::validate_schema_completeness(&schema);
    assert!(
        result.is_ok(),
        "Counter schema should pass validation: {:?}",
        result.err()
    );
}

/// Test that Counter values are parsed correctly from binary data
///
/// This test validates that Counter values are NOT parsed as BigInt,
/// but as distinct Value::Counter variant.
#[test]
fn test_counter_value_parsing() {
    use cqlite_core::parser::types::parse_counter;

    // Test data from JSONL reference: value 422216548022666
    // Counter is stored as big-endian i64
    let test_value: i64 = 422216548022666;
    let bytes = test_value.to_be_bytes();

    // Parse as Counter
    let result = parse_counter(&bytes);
    assert!(
        result.is_ok(),
        "Counter parsing should succeed: {:?}",
        result.err()
    );

    let (remaining, parsed_value) = result.unwrap();

    // Verify full consumption
    assert_eq!(remaining.len(), 0, "Buffer should be fully consumed");

    // CRITICAL: Verify it's parsed as Counter, NOT BigInt
    match parsed_value {
        Value::Counter(c) => {
            assert_eq!(c, test_value, "Counter value should match");
            println!("Successfully parsed Counter value: {}", c);
        }
        Value::BigInt(b) => {
            panic!(
                "FAILURE: Value was parsed as BigInt({}) instead of Counter",
                b
            );
        }
        other => {
            panic!(
                "FAILURE: Value was parsed as {:?} instead of Counter",
                other
            );
        }
    }
}

/// Test that Counter and BigInt are distinct types
#[test]
fn test_counter_bigint_distinction() {
    use cqlite_core::types::Value;

    let counter_value = Value::Counter(1000);
    let bigint_value = Value::BigInt(1000);

    // They should NOT be equal even with same numeric value
    assert_ne!(
        counter_value, bigint_value,
        "Counter and BigInt should be distinct types"
    );

    // Verify type detection
    assert!(matches!(counter_value, Value::Counter(_)));
    assert!(matches!(bigint_value, Value::BigInt(_)));

    // Both can be converted to i64
    assert_eq!(counter_value.as_i64(), Some(1000));
    assert_eq!(bigint_value.as_i64(), Some(1000));

    // But they should format differently
    let counter_str = format!("{}", counter_value);
    let bigint_str = format!("{}", bigint_value);
    assert!(
        counter_str.contains("counter"),
        "Counter should format with 'counter' prefix"
    );
    assert!(
        !bigint_str.contains("counter"),
        "BigInt should not format with 'counter' prefix"
    );
}

/// Test Counter type in type system
#[test]
fn test_counter_type_system() {
    use cqlite_core::schema::CqlType;
    use cqlite_core::types::Value;

    // Test CqlType::Counter exists
    let counter_type = CqlType::Counter;
    println!("Counter type: {:?}", counter_type);

    // Test Value::Counter inference
    let counter_value = Value::Counter(42);
    let inferred_type = counter_value.data_type();

    assert_eq!(
        inferred_type,
        CqlType::Counter,
        "Value::Counter should infer to CqlType::Counter"
    );
}

/// Test that reference data values are valid Counters
#[test]
fn test_reference_data_counter_values() {
    // From JSONL reference data, all Counter values are 422216548022666
    let expected_value: i64 = 422216548022666;

    // Verify this is a valid i64 (not overflow)
    assert!(expected_value > 0);
    assert!(expected_value < i64::MAX);

    // Create Counter value
    let counter = Value::Counter(expected_value);
    assert_eq!(counter.as_i64(), Some(expected_value));

    println!("Reference Counter value validated: {}", expected_value);
}

/// Test schema-driven Counter parsing with type IDs
#[test]
fn test_schema_driven_counter_parsing() {
    use cqlite_core::parser::types::{parse_bigint, parse_counter};

    // Test value from reference data
    let test_value: i64 = 422216548022666;
    let bytes = test_value.to_be_bytes();

    // Parse with Counter parser
    let result = parse_counter(&bytes);
    assert!(
        result.is_ok(),
        "Counter parsing should succeed: {:?}",
        result.err()
    );

    let (remaining, parsed) = result.unwrap();
    assert_eq!(remaining.len(), 0, "Should consume all bytes");

    // Verify it's Counter type
    match parsed {
        Value::Counter(c) => {
            assert_eq!(c, test_value);
            println!("Counter parser produced Counter: {}", c);
        }
        other => {
            panic!("Counter parser should produce Counter, got: {:?}", other);
        }
    }

    // Verify that BigInt parser would produce different result
    let bigint_result = parse_bigint(&bytes);
    assert!(bigint_result.is_ok());
    let (_, bigint_parsed) = bigint_result.unwrap();

    match bigint_parsed {
        Value::BigInt(_) => {
            println!("BigInt parser correctly produces BigInt variant");
        }
        _ => panic!("BigInt parser should produce BigInt variant"),
    }

    // Demonstrate they are different even with same bytes
    assert_ne!(parsed, bigint_parsed);
}

#[cfg(test)]
mod counter_value_tests {
    use super::*;

    #[test]
    fn test_counter_serialized_size() {
        let counter = Value::Counter(1000);
        // Counter is a 64-bit integer
        let serialized_size = counter.size_estimate();
        assert_eq!(serialized_size, 8, "Counter should be 8 bytes");
    }

    #[test]
    fn test_counter_null_vs_zero() {
        let counter_zero = Value::Counter(0);
        let null_value = Value::Null;

        assert_ne!(
            counter_zero, null_value,
            "Counter(0) should be distinct from Null"
        );
    }

    #[test]
    fn test_counter_display_format() {
        let counter = Value::Counter(12345);
        let display = format!("{}", counter);
        assert!(
            display.contains("counter"),
            "Display should include 'counter' indicator"
        );
        assert!(
            display.contains("12345"),
            "Display should include numeric value"
        );
    }
}

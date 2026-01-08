//! Property-based tests for SchemaParser consumed-byte tracking
//!
//! Tests Issue #94 requirement: property tests for nesting/nulls

use cqlite_core::{
    schema::{
        parser::SchemaParser, registry::ParsingContext, Column, CqlType, KeyColumn, TableSchema,
    },
    types::{ComparatorType, Value},
};
use std::collections::HashMap;

/// Helper to create a parsing context for testing
fn create_test_context(columns: Vec<Column>) -> ParsingContext {
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
    };

    ParsingContext {
        partition_comparators: vec![ComparatorType::Int],
        clustering_comparators: vec![],
        column_comparators: HashMap::new(),
        schema,
    }
}

/// Property: Parsing a value and re-encoding it should consume exactly the right number of bytes
#[test]
fn test_primitive_types_consumed_bytes() {
    let test_cases = vec![
        ("boolean", vec![1], 1),
        ("tinyint", vec![42], 1),
        ("smallint", vec![0, 42], 2),
        ("int", vec![0, 0, 0, 42], 4),
        ("bigint", vec![0, 0, 0, 0, 0, 0, 0, 42], 8),
        ("float", vec![64, 72, 243, 215], 4), // 3.14 as f32
        ("double", vec![64, 9, 30, 184, 81, 235, 133, 31], 8), // 3.14 as f64
        ("uuid", vec![0; 16], 16),
        ("timestamp", vec![0, 0, 0, 0, 0, 0, 0, 42], 8),
    ];

    for (type_name, data, expected_consumed) in test_cases {
        let columns = vec![Column {
            name: "test_col".to_string(),
            data_type: type_name.to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }];

        let mut context = create_test_context(columns);
        context.column_comparators.insert(
            "test_col".to_string(),
            ComparatorType::from_cql_type(&CqlType::parse(type_name).unwrap()).unwrap(),
        );

        let parser = SchemaParser::new(context).unwrap();
        let result = parser.parse_column_value("test_col", &data);

        match result {
            Ok((value, consumed)) => {
                assert_eq!(
                    consumed, expected_consumed,
                    "Type {} should consume {} bytes, but consumed {}",
                    type_name, expected_consumed, consumed
                );
                assert!(
                    !matches!(value, Value::Null),
                    "Should not return Null for valid {} data",
                    type_name
                );
            }
            Err(e) => panic!("Failed to parse {}: {:?}", type_name, e),
        }
    }
}

/// Property: Variable-length types must report exact consumed bytes including length prefix
#[test]
fn test_variable_length_types_consumed_bytes() {
    // Text with length prefix
    let text_data = {
        let text = b"hello";
        let mut data = Vec::new();
        data.extend_from_slice(&(text.len() as i32).to_be_bytes());
        data.extend_from_slice(text);
        data
    };

    let columns = vec![Column {
        name: "text_col".to_string(),
        data_type: "text".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }];

    let mut context = create_test_context(columns);
    context
        .column_comparators
        .insert("text_col".to_string(), ComparatorType::Text);

    let parser = SchemaParser::new(context).unwrap();
    let (value, consumed) = parser.parse_column_value("text_col", &text_data).unwrap();

    assert_eq!(
        consumed,
        4 + 5,
        "Text should consume length prefix (4) + content (5)"
    );
    match value {
        Value::Text(s) => assert_eq!(s, "hello"),
        _ => panic!("Expected Text value"),
    }
}

/// Property: Collections must report exact consumed bytes for all nested elements
#[test]
fn test_list_consumed_bytes() {
    // List<int> with 3 elements
    let list_data = {
        let mut data = Vec::new();
        // Count: 3
        data.extend_from_slice(&3i32.to_be_bytes());
        // Element 1: 4 bytes (int) = 1
        data.extend_from_slice(&1i32.to_be_bytes());
        // Element 2: 4 bytes (int) = 2
        data.extend_from_slice(&2i32.to_be_bytes());
        // Element 3: 4 bytes (int) = 3
        data.extend_from_slice(&3i32.to_be_bytes());
        data
    };

    let columns = vec![Column {
        name: "list_col".to_string(),
        data_type: "list<int>".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }];

    let mut context = create_test_context(columns);
    context.column_comparators.insert(
        "list_col".to_string(),
        ComparatorType::from_cql_type(&CqlType::parse("list<int>").unwrap()).unwrap(),
    );

    let parser = SchemaParser::new(context).unwrap();
    let (value, consumed) = parser.parse_column_value("list_col", &list_data).unwrap();

    // Expected: 4 (count) + 4*3 (three ints) = 16 bytes
    assert_eq!(
        consumed, 16,
        "List<int> with 3 elements should consume 16 bytes"
    );

    match value {
        Value::List(elements) => {
            assert_eq!(elements.len(), 3);
        }
        _ => panic!("Expected List value"),
    }
}

/// Property: Maps must track consumed bytes for all key-value pairs
#[test]
fn test_map_consumed_bytes() {
    // Map<int, int> with 2 entries
    let map_data = {
        let mut data = Vec::new();
        // Count: 2
        data.extend_from_slice(&2i32.to_be_bytes());
        // Entry 1: key=1, value=10
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&10i32.to_be_bytes());
        // Entry 2: key=2, value=20
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(&20i32.to_be_bytes());
        data
    };

    let columns = vec![Column {
        name: "map_col".to_string(),
        data_type: "map<int, int>".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }];

    let mut context = create_test_context(columns);
    context.column_comparators.insert(
        "map_col".to_string(),
        ComparatorType::from_cql_type(&CqlType::parse("map<int, int>").unwrap()).unwrap(),
    );

    let parser = SchemaParser::new(context).unwrap();
    let (value, consumed) = parser.parse_column_value("map_col", &map_data).unwrap();

    // Expected: 4 (count) + 2 * (4 + 4) (two key-value pairs) = 20 bytes
    assert_eq!(
        consumed, 20,
        "Map<int, int> with 2 entries should consume 20 bytes"
    );

    match value {
        Value::Map(entries) => {
            assert_eq!(entries.len(), 2);
        }
        _ => panic!("Expected Map value"),
    }
}

/// Property: Nested collections must accurately track all levels of nesting
#[test]
fn test_nested_list_consumed_bytes() {
    // List<List<int>> with structure: [[1, 2], [3]]
    let nested_list_data = {
        let mut data = Vec::new();
        // Outer list count: 2
        data.extend_from_slice(&2i32.to_be_bytes());

        // Inner list 1: [1, 2]
        data.extend_from_slice(&2i32.to_be_bytes()); // count
        data.extend_from_slice(&1i32.to_be_bytes()); // element 1
        data.extend_from_slice(&2i32.to_be_bytes()); // element 2

        // Inner list 2: [3]
        data.extend_from_slice(&1i32.to_be_bytes()); // count
        data.extend_from_slice(&3i32.to_be_bytes()); // element 1

        data
    };

    let columns = vec![Column {
        name: "nested_col".to_string(),
        data_type: "list<list<int>>".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }];

    let mut context = create_test_context(columns);
    context.column_comparators.insert(
        "nested_col".to_string(),
        ComparatorType::from_cql_type(&CqlType::parse("list<list<int>>").unwrap()).unwrap(),
    );

    let parser = SchemaParser::new(context).unwrap();
    let (value, consumed) = parser
        .parse_column_value("nested_col", &nested_list_data)
        .unwrap();

    // Expected: 4 (outer count) + 4 (inner1 count) + 8 (two ints) + 4 (inner2 count) + 4 (one int) = 24 bytes
    assert_eq!(
        consumed, 24,
        "Nested List<List<int>> [[1,2],[3]] should consume 24 bytes"
    );

    match value {
        Value::List(outer_elements) => {
            assert_eq!(outer_elements.len(), 2);
            // Verify nested structure
            if let Value::List(inner1) = &outer_elements[0] {
                assert_eq!(inner1.len(), 2);
            } else {
                panic!("Expected inner list");
            }
        }
        _ => panic!("Expected List value"),
    }
}

/// Property: Null values must be handled correctly with length = -1
#[test]
fn test_null_handling_in_row() {
    // Test data with null value (length = -1)
    let null_marker = (-1i32).to_be_bytes();

    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        },
        Column {
            name: "nullable_col".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        },
    ];

    let context = ParsingContext {
        schema: TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: columns.clone(),
            comments: HashMap::new(),
        },
        partition_comparators: vec![ComparatorType::Int],
        clustering_comparators: vec![],
        column_comparators: {
            let mut map = HashMap::new();
            map.insert("id".to_string(), ComparatorType::Int);
            map.insert("nullable_col".to_string(), ComparatorType::Text);
            map
        },
    };

    let parser = SchemaParser::new(context).unwrap();

    // Row data: id=42 (4 bytes), nullable_col=null (4 bytes for -1)
    let mut row_data = Vec::new();
    row_data.extend_from_slice(&42i32.to_be_bytes());
    row_data.extend_from_slice(&null_marker);

    let row = parser.parse_row(&row_data).unwrap();

    assert_eq!(row.len(), 2);
    assert!(matches!(row.get("id"), Some(Value::Integer(42))));
    assert!(matches!(row.get("nullable_col"), Some(Value::Null)));
}

/// Property: Tuple types must track consumed bytes for all fields
#[test]
fn test_tuple_consumed_bytes() {
    // Tuple<int, text> = (42, "test")
    let tuple_data = {
        let mut data = Vec::new();
        // Field 1: int = 42
        data.extend_from_slice(&42i32.to_be_bytes());
        // Field 2: text = "test" (length-prefixed)
        let text = b"test";
        data.extend_from_slice(&(text.len() as i32).to_be_bytes());
        data.extend_from_slice(text);
        data
    };

    let columns = vec![Column {
        name: "tuple_col".to_string(),
        data_type: "tuple<int, text>".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }];

    let mut context = create_test_context(columns);
    context.column_comparators.insert(
        "tuple_col".to_string(),
        ComparatorType::from_cql_type(&CqlType::parse("tuple<int, text>").unwrap()).unwrap(),
    );

    let parser = SchemaParser::new(context).unwrap();
    let (value, consumed) = parser.parse_column_value("tuple_col", &tuple_data).unwrap();

    // Expected: 4 (int) + 4 (text length) + 4 (text content) = 12 bytes
    assert_eq!(
        consumed, 12,
        "Tuple<int, text> (42, 'test') should consume 12 bytes"
    );

    match value {
        Value::Tuple(fields) => {
            assert_eq!(fields.len(), 2);
        }
        _ => panic!("Expected Tuple value"),
    }
}

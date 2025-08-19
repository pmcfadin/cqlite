//! Comprehensive test suite for RowCellStateMachine implementation
//! Tests all state transitions, VInt parsing, and error handling

#[cfg(test)]
mod tests {
    use super::super::row_cell_state_machine::*;
    use crate::schema::{Column, ClusteringColumn, TableSchema};
    use crate::types::{ComparatorType, Value};
    use std::collections::HashMap;

    /// Helper to create a test schema
    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "tags".to_string(),
                    data_type: "list<text>".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        }
    }

    /// Create a valid row header with no optional fields
    fn create_simple_row_header() -> Vec<u8> {
        let mut data = Vec::new();
        // Flags: 0x00 (no TTL, no deletion)
        data.push(0x00);
        // Timestamp: 42 (8 bytes, big-endian)
        data.extend_from_slice(&42i64.to_be_bytes());
        data
    }

    /// Create a row header with TTL
    fn create_row_header_with_ttl() -> Vec<u8> {
        let mut data = Vec::new();
        // Flags: 0x01 (has TTL)
        data.push(0x01);
        // Timestamp: 42
        data.extend_from_slice(&42i64.to_be_bytes());
        // TTL: 3600 seconds (4 bytes)
        data.extend_from_slice(&3600u32.to_be_bytes());
        data
    }

    /// Create a row header with deletion info
    fn create_row_header_with_deletion() -> Vec<u8> {
        let mut data = Vec::new();
        // Flags: 0x02 (has local deletion time)
        data.push(0x02);
        // Timestamp: 42
        data.extend_from_slice(&42i64.to_be_bytes());
        // Local deletion time: 1234567890
        data.extend_from_slice(&1234567890u32.to_be_bytes());
        data
    }

    /// Create a simple partition key
    fn create_simple_partition_key() -> Vec<u8> {
        let mut data = Vec::new();
        // Component count: 1 (vint encoded as 0x02 - zigzag encoding)
        data.push(0x02);
        // Component length: 3 bytes (vint encoded as 0x06)
        data.push(0x06);
        // Component data: "key"
        data.extend_from_slice(b"key");
        data
    }

    /// Create a composite partition key
    fn create_composite_partition_key() -> Vec<u8> {
        let mut data = Vec::new();
        // Component count: 2 (vint encoded as 0x04)
        data.push(0x04);
        // First component length: 3 (vint encoded as 0x06)
        data.push(0x06);
        // First component: "abc"
        data.extend_from_slice(b"abc");
        // Second component length: 3 (vint encoded as 0x06)
        data.push(0x06);
        // Second component: "xyz"
        data.extend_from_slice(b"xyz");
        data
    }

    /// Create static row data
    fn create_static_row() -> Vec<u8> {
        let mut data = Vec::new();
        // Static row flag: 0x40
        data.push(0x40);
        // Column count: 2 (vint)
        data.push(0x04);

        // Column 1: name="static1", value="value1"
        data.push(0x10); // name length: 8 (vint)
        data.extend_from_slice(b"static1");
        data.push(0x0C); // value length: 6 (vint)
        data.extend_from_slice(b"value1");

        // Column 2: name="static2", value="value2"
        data.push(0x10); // name length: 8 (vint)
        data.extend_from_slice(b"static2");
        data.push(0x0C); // value length: 6 (vint)
        data.extend_from_slice(b"value2");

        data
    }

    /// Create clustering rows with varying column masks
    fn create_clustering_rows_dense() -> Vec<u8> {
        let mut data = Vec::new();
        // Row count: 1 (vint)
        data.push(0x02);

        // Row 1:
        // Clustering key length: 4 (vint)
        data.push(0x08);
        // Clustering key: "row1"
        data.extend_from_slice(b"row1");
        // Timestamp
        data.extend_from_slice(&100i64.to_be_bytes());
        // Column count: 2
        data.push(0x04);

        // Column 1
        data.push(0x08); // name length: 4
        data.extend_from_slice(b"col1");
        data.push(0x0A); // value length: 5
        data.extend_from_slice(b"data1");

        // Column 2
        data.push(0x08); // name length: 4
        data.extend_from_slice(b"col2");
        data.push(0x0A); // value length: 5
        data.extend_from_slice(b"data2");

        data
    }

    /// Create clustering rows with sparse columns
    fn create_clustering_rows_sparse() -> Vec<u8> {
        let mut data = Vec::new();
        // Row count: 2 (vint)
        data.push(0x04);

        // Row 1: Has 1 column
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // column count: 1
        data.push(0x08); // name length: 4
        data.extend_from_slice(b"col1");
        data.push(0x0A); // value length: 5
        data.extend_from_slice(b"data1");

        // Row 2: Has 0 columns (null row)
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row2");
        data.extend_from_slice(&200i64.to_be_bytes());
        data.push(0x00); // column count: 0

        data
    }

    #[test]
    fn test_state_machine_creation() {
        let state_machine = RowCellStateMachine::new();
        assert_eq!(state_machine.current_state(), &State::Header);
        assert!(!state_machine.is_complete());
        assert!(!state_machine.has_error());
    }

    #[test]
    fn test_state_machine_with_schema() {
        let schema = create_test_schema();
        let comparator = ComparatorType::Blob;
        let state_machine = RowCellStateMachine::with_schema(schema, comparator);
        assert_eq!(state_machine.current_state(), &State::Header);
    }

    #[test]
    fn test_parse_simple_header() {
        let mut state_machine = RowCellStateMachine::new();
        let header_data = create_simple_row_header();

        let result = state_machine.process(&header_data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), header_data.len());
        assert_eq!(state_machine.current_state(), &State::PartitionKey);
    }

    #[test]
    fn test_parse_header_with_ttl() {
        let mut state_machine = RowCellStateMachine::new();
        let header_data = create_row_header_with_ttl();

        let result = state_machine.process(&header_data);
        assert!(result.is_ok());
        assert_eq!(state_machine.current_state(), &State::PartitionKey);
    }

    #[test]
    fn test_parse_header_with_deletion() {
        let mut state_machine = RowCellStateMachine::new();
        let header_data = create_row_header_with_deletion();

        let result = state_machine.process(&header_data);
        assert!(result.is_ok());
        assert_eq!(state_machine.current_state(), &State::PartitionKey);
    }

    #[test]
    fn test_parse_simple_partition_key() {
        let mut state_machine = RowCellStateMachine::new();

        // First parse header
        let header = create_simple_row_header();
        state_machine.process(&header).unwrap();

        // Then parse partition key
        let partition_key = create_simple_partition_key();
        let result = state_machine.process(&partition_key);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_composite_partition_key() {
        let mut state_machine = RowCellStateMachine::new();

        // Parse header first
        let header = create_simple_row_header();
        state_machine.process(&header).unwrap();

        // Parse composite partition key
        let partition_key = create_composite_partition_key();
        let result = state_machine.process(&partition_key);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_static_row() {
        let mut state_machine = RowCellStateMachine::new();

        // Setup: parse header and partition key
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());
        data.extend(create_static_row());

        // Add empty clustering rows to complete
        data.push(0x00); // 0 clustering rows

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert!(row.static_row.is_some());
        assert_eq!(row.static_row.unwrap().column_count, 2);
    }

    #[test]
    fn test_parse_dense_clustering_rows() {
        let mut state_machine = RowCellStateMachine::new();

        // Build complete row data
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());
        data.extend(create_clustering_rows_dense());

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert_eq!(row.clustering_rows[0].columns.len(), 2);
    }

    #[test]
    fn test_parse_sparse_clustering_rows() {
        let mut state_machine = RowCellStateMachine::new();

        // Build complete row data
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());
        data.extend(create_clustering_rows_sparse());

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 2);
        assert_eq!(row.clustering_rows[0].columns.len(), 1);
        assert_eq!(row.clustering_rows[1].columns.len(), 0);
    }

    #[test]
    fn test_complete_row_with_all_sections() {
        let mut state_machine = RowCellStateMachine::new();

        // Build complete row with all sections
        let mut data = Vec::new();
        data.extend(create_row_header_with_ttl());
        data.extend(create_composite_partition_key());
        data.extend(create_static_row());
        data.extend(create_clustering_rows_dense());

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert!(row.header.ttl.is_some());
        assert_eq!(row.partition_key.component_count, 2);
        assert!(row.static_row.is_some());
        assert_eq!(row.clustering_rows.len(), 1);
    }

    #[test]
    fn test_insufficient_data_error() {
        let mut state_machine = RowCellStateMachine::new();

        // Try to process with insufficient data for header
        let data = vec![0x00]; // Only 1 byte, need at least 9
        let result = state_machine.process(&data);

        assert!(result.is_ok()); // Process succeeds but state machine should have error
        assert!(state_machine.has_error());
    }

    #[test]
    fn test_state_machine_reset() {
        let mut state_machine = RowCellStateMachine::new();

        // Process some data
        let header = create_simple_row_header();
        state_machine.process(&header).unwrap();
        assert_ne!(state_machine.current_state(), &State::Header);

        // Reset
        state_machine.reset();
        assert_eq!(state_machine.current_state(), &State::Header);
        assert!(!state_machine.has_error());
    }

    #[test]
    fn test_schema_aware_parsing() {
        let schema = create_test_schema();
        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row with columns matching schema
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with schema columns
        data.push(0x02); // 1 clustering row
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // 1 column

        // Column "name" (text type in schema)
        data.push(0x08); // name length: 4
        data.extend_from_slice(b"name");
        data.push(0x0A); // value length: 5
        data.extend_from_slice(b"Alice");

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());
    }

    #[test]
    fn test_ttl_and_deletion_coexistence() {
        let mut state_machine = RowCellStateMachine::new();

        // Create header with both TTL and deletion
        let mut header_data = Vec::new();
        header_data.push(0x03); // Flags: 0x03 (has both TTL and deletion)
        header_data.extend_from_slice(&42i64.to_be_bytes());
        header_data.extend_from_slice(&3600u32.to_be_bytes()); // TTL
        header_data.extend_from_slice(&1234567890u32.to_be_bytes()); // Local deletion

        let result = state_machine.process(&header_data);
        assert!(result.is_ok());
        assert_eq!(state_machine.current_state(), &State::PartitionKey);
    }

    #[test]
    fn test_empty_clustering_rows() {
        let mut state_machine = RowCellStateMachine::new();

        // Build row with no clustering rows
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());
        data.push(0x00); // 0 clustering rows

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());
        assert_eq!(parsed_row.unwrap().clustering_rows.len(), 0);
    }

    #[test]
    fn test_multiple_component_partition_keys() {
        let mut state_machine = RowCellStateMachine::new();

        // Create partition key with 5 components
        let mut partition_key = Vec::new();
        partition_key.push(0x0A); // 5 components (vint)

        for i in 0..5 {
            let component = format!("part{}", i);
            partition_key.push((component.len() * 2) as u8); // length (vint)
            partition_key.extend_from_slice(component.as_bytes());
        }

        // Build complete data
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(partition_key);
        data.push(0x00); // No clustering rows

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());
        assert_eq!(parsed_row.unwrap().partition_key.component_count, 5);
    }

    /// Test schema-driven decoding of nested collections (list<set<text>>)
    #[test]
    fn test_schema_driven_nested_collections() {
        // Create schema with nested collection column
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "nested_data".to_string(),
            data_type: "list<set<text>>".to_string(),
            nullable: true,
            default: None,
        });

        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row data with nested collection column
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with nested collection
        data.push(0x02); // 1 clustering row
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // 1 column

        // Column "nested_data" with complex nested structure
        data.push(0x16); // name length: 11 (nested_data)
        data.extend_from_slice(b"nested_data");
        data.push(0x14); // value length: 10 (mock binary data for list<set<text>>)
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]);

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert!(row.clustering_rows[0].columns.contains_key("nested_data"));

        // Should be parsed as blob since we preserve type-aware parsing for schema integration
        match &row.clustering_rows[0].columns["nested_data"] {
            Value::Blob(_) => {} // Expected for complex types without full parser
            _ => panic!("Expected blob value for complex nested type"),
        }
    }

    /// Test schema-driven decoding of User Defined Types (UDT)
    #[test]
    #[cfg(feature = "state_machine")]
    fn test_schema_driven_udt_parsing() {
        // Create schema with UDT column
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "address".to_string(),
            data_type: "frozen<address_type>".to_string(),
            nullable: true,
            default: None,
        });

        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row data with UDT column
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with UDT
        data.push(0x02); // 1 clustering row
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // 1 column

        // Column "address" (frozen UDT)
        data.push(0x0E); // name length: 7
        data.extend_from_slice(b"address");
        data.push(0x20); // value length: 16 (mock UDT binary data)
        // Mock UDT binary data representing {street: "Main St", city: "NYC", zip: 10001}
        data.extend_from_slice(&[
            0x00, 0x07, 0x4D, 0x61, 0x69, 0x6E, 0x20, 0x53, 0x74, // street: "Main St"
            0x00, 0x03, 0x4E, 0x59, 0x43, // city: "NYC"
            0x00, 0x02, 0x27, 0x11, // zip: 10001
        ]);

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert!(row.clustering_rows[0].columns.contains_key("address"));

        // Should be parsed as blob for frozen UDT until full UDT parser is implemented
        match &row.clustering_rows[0].columns["address"] {
            Value::Blob(data) => assert_eq!(data.len(), 16),
            _ => panic!("Expected blob value for frozen UDT"),
        }
    }

    /// Test schema-driven tuple decoding with multiple types
    #[test]
    fn test_schema_driven_tuple_parsing() {
        // Create schema with tuple column
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "coordinates".to_string(),
            data_type: "tuple<double, double, text>".to_string(),
            nullable: true,
            default: None,
        });

        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row data with tuple column
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with tuple
        data.push(0x02); // 1 clustering row
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // 1 column

        // Column "coordinates" (tuple<double, double, text>)
        data.push(0x16); // name length: 11
        data.extend_from_slice(b"coordinates");
        data.push(0x1C); // value length: 14 (mock tuple binary data)
        // Mock tuple binary: (40.7128, -74.0060, "NYC")
        data.extend_from_slice(&[
            0x40, 0x44, 0x5B, 0x3F, 0xDB, 0x8B, 0x44, 0x61, // 40.7128 (double)
            0xC0, 0x52, 0x80, 0x7E, 0xA9, 0x86, 0xB8, 0x6A, // -74.0060 (double)
            0x4E, 0x59, 0x43, // "NYC" (text)
        ]);

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert!(row.clustering_rows[0].columns.contains_key("coordinates"));

        // Should be parsed as blob for complex tuple until full tuple parser is implemented
        match &row.clustering_rows[0].columns["coordinates"] {
            Value::Blob(data) => assert_eq!(data.len(), 14),
            _ => panic!("Expected blob value for tuple"),
        }
    }

    /// Test multi-component clustering key with schema awareness
    #[test]
    fn test_multi_component_clustering_keys() {
        // Create schema with multi-component clustering
        let mut schema = create_test_schema();
        schema.clustering_keys = vec![
            ClusteringColumn {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
                order: "ASC".to_string(),
            },
            ClusteringColumn {
                name: "sequence".to_string(),
                data_type: "int".to_string(),
                position: 1,
                order: "ASC".to_string(),
            },
        ];

        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row data with multi-component clustering key
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with composite clustering key
        data.push(0x02); // 1 clustering row
        data.push(0x10); // key length: 8 (timestamp) + 4 (int) = 12, but encoded as 16 for vint
        // Mock composite clustering key: timestamp + sequence
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0x83, 0x8F, 0xA4, 0x32, 0x00, // timestamp (8 bytes)
            0x00, 0x00, 0x00, 0x01, // sequence = 1 (4 bytes)
        ]);
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // 1 column

        // Simple column
        data.push(0x08); // name length: 4
        data.extend_from_slice(b"data");
        data.push(0x0A); // value length: 5
        data.extend_from_slice(b"value");

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert_eq!(row.clustering_rows[0].clustering_key.len(), 12); // 8 + 4 bytes
        assert!(row.clustering_rows[0].columns.contains_key("data"));
    }

    /// Test frozen vs non-frozen collections with schema
    #[test]
    fn test_frozen_vs_non_frozen_collections() {
        // Create schema with both frozen and non-frozen collections
        let mut schema = create_test_schema();
        schema.columns.extend(vec![
            Column {
                name: "frozen_list".to_string(),
                data_type: "frozen<list<text>>".to_string(),
                nullable: true,
                default: None,
            },
            Column {
                name: "regular_list".to_string(),
                data_type: "list<text>".to_string(),
                nullable: true,
                default: None,
            },
        ]);

        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row data with both types
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with both frozen and regular collections
        data.push(0x02); // 1 clustering row
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x04); // 2 columns

        // Frozen list column
        data.push(0x16); // name length: 11
        data.extend_from_slice(b"frozen_list");
        data.push(0x0C); // value length: 6
        data.extend_from_slice(&[0x00, 0x02, 0x41, 0x42, 0x43, 0x44]); // Mock frozen list data

        // Regular list column
        data.push(0x18); // name length: 12
        data.extend_from_slice(b"regular_list");
        data.push(0x0C); // value length: 6
        data.extend_from_slice(&[0x01, 0x02, 0x45, 0x46, 0x47, 0x48]); // Mock regular list data

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert!(row.clustering_rows[0].columns.contains_key("frozen_list"));
        assert!(row.clustering_rows[0].columns.contains_key("regular_list"));

        // Both should be parsed as blobs until full collection parsers are implemented
        // but the schema information is used to determine the approach
        match (
            &row.clustering_rows[0].columns["frozen_list"],
            &row.clustering_rows[0].columns["regular_list"],
        ) {
            (Value::Blob(frozen_data), Value::Blob(regular_data)) => {
                assert_eq!(frozen_data.len(), 6);
                assert_eq!(regular_data.len(), 6);
                // Different binary representations indicate different handling
                assert_ne!(frozen_data, regular_data);
            }
            _ => panic!("Expected blob values for both collection types"),
        }
    }

    /// Test schema-driven map decoding with complex key-value types
    #[test]
    fn test_schema_driven_complex_map() {
        // Create schema with complex map column
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "metadata".to_string(),
            data_type: "map<text, frozen<list<int>>>".to_string(),
            nullable: true,
            default: None,
        });

        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row data with complex map
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with complex map
        data.push(0x02); // 1 clustering row
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // 1 column

        // Column "metadata" (map<text, frozen<list<int>>>)
        data.push(0x10); // name length: 8
        data.extend_from_slice(b"metadata");
        data.push(0x18); // value length: 12 (mock map binary data)
        // Mock map data: {"key1": [1, 2, 3], "key2": [4, 5, 6]}
        data.extend_from_slice(&[
            0x02, // 2 entries
            0x04, 0x6B, 0x65, 0x79, 0x31, // "key1"
            0x0C, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, // [1, 2, 3]
            0x04, 0x6B, 0x65, 0x79, 0x32, // "key2"
            0x0C, 0x00, 0x04, 0x00, 0x05, 0x00, 0x06, // [4, 5, 6]
        ]);

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert!(row.clustering_rows[0].columns.contains_key("metadata"));

        // Should be parsed as blob for complex map until full map parser is implemented
        match &row.clustering_rows[0].columns["metadata"] {
            Value::Blob(data) => assert_eq!(data.len(), 12),
            _ => panic!("Expected blob value for complex map"),
        }
    }

    /// Test error handling with schema mismatch
    #[test]
    #[cfg(feature = "state_machine")]
    fn test_schema_mismatch_handling() {
        // Create schema expecting specific columns
        let schema = create_test_schema();
        let comparator = ComparatorType::Blob;
        let mut state_machine = RowCellStateMachine::with_schema(schema, comparator);

        // Build row data with column not in schema
        let mut data = Vec::new();
        data.extend(create_simple_row_header());
        data.extend(create_simple_partition_key());

        // Add clustering row with unknown column
        data.push(0x02); // 1 clustering row
        data.push(0x08); // key length: 4
        data.extend_from_slice(b"row1");
        data.extend_from_slice(&100i64.to_be_bytes());
        data.push(0x02); // 1 column

        // Column "unknown" (not in schema)
        data.push(0x0E); // name length: 7
        data.extend_from_slice(b"unknown");
        data.push(0x0A); // value length: 5
        data.extend_from_slice(b"value");

        let result = state_machine.process(&data);
        assert!(result.is_ok());
        assert!(state_machine.is_complete());

        let parsed_row = state_machine.take_parsed_row();
        assert!(parsed_row.is_some());

        let row = parsed_row.unwrap();
        assert_eq!(row.clustering_rows.len(), 1);
        assert!(row.clustering_rows[0].columns.contains_key("unknown"));

        // Unknown columns should be preserved as blobs (graceful fallback)
        match &row.clustering_rows[0].columns["unknown"] {
            Value::Blob(data) => assert_eq!(data, b"value"),
            _ => panic!("Expected blob value for unknown column"),
        }
    }
}

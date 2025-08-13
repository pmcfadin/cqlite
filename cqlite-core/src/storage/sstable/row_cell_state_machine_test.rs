//! Comprehensive test suite for RowCellStateMachine implementation
//! Tests all state transitions, VInt parsing, and error handling

#[cfg(test)]
mod tests {
    use super::super::row_cell_state_machine::*;
    use crate::schema::{Column, TableSchema};
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
}
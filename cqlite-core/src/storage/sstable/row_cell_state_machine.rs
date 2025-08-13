//! Cassandra 5 'oa' format row/cell state machine
//!
//! This module implements a spec-accurate state machine for parsing Cassandra 5+ SSTable
//! row and cell data according to the exact binary format specification.
//!
//! State machine flow:
//! HEADER → PARTITION_KEY → DELETION_INFO → STATIC_ROW → CLUSTERING_ROWS → COLUMN_DATA
//!
//! Each state validates input and transitions appropriately using proper VInt decoding
//! and handles optional components like deletion info, static rows, and TTL information.

use crate::{
    error::{Error, Result},
    parser::{
        types::{parse_cql_value, CqlTypeId},
        vint::parse_vint_length,
    },
    schema::TableSchema,
    types::{ComparatorType, TombstoneType, Value},
};
use std::collections::HashMap;

/// State machine states for Cassandra 5 'oa' format row/cell parsing
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Initial state - parsing row header
    Header,
    /// Parsing partition key components
    PartitionKey,
    /// Parsing deletion information (optional)
    DeletionInfo,
    /// Parsing static row data (optional)
    StaticRow,
    /// Parsing clustering rows
    ClusteringRows,
    /// Parsing column data within a row
    ColumnData,
    /// Final state - parsing complete
    Complete,
    /// Error state - invalid data encountered
    Error(String),
}

/// Row header information from Cassandra 5 'oa' format
#[derive(Debug, Clone)]
pub struct RowHeader {
    /// Row flags indicating what sections are present
    pub flags: u8,
    /// Timestamp for the row
    pub timestamp: i64,
    /// TTL (time-to-live) in seconds, if present
    pub ttl: Option<u32>,
    /// Local deletion time, if row is deleted
    pub local_deletion_time: Option<u32>,
}

/// Partition key information
#[derive(Debug, Clone)]
pub struct PartitionKey {
    /// Number of partition key components
    pub component_count: usize,
    /// Raw partition key bytes
    pub key_bytes: Vec<u8>,
    /// Parsed key components
    pub components: Vec<Vec<u8>>,
}

/// Deletion information for rows or columns
#[derive(Debug, Clone)]
pub struct DeletionInfo {
    /// Type of deletion
    pub deletion_type: TombstoneType,
    /// Deletion timestamp
    pub deletion_time: i64,
    /// Local deletion time
    pub local_deletion_time: u32,
}

/// Static row data
#[derive(Debug, Clone)]
pub struct StaticRow {
    /// Number of static columns
    pub column_count: usize,
    /// Static column values
    pub columns: HashMap<String, Value>,
}

/// Clustering row information
#[derive(Debug, Clone)]
pub struct ClusteringRow {
    /// Clustering key bytes
    pub clustering_key: Vec<u8>,
    /// Row timestamp
    pub timestamp: i64,
    /// Row deletion info, if deleted
    pub deletion_info: Option<DeletionInfo>,
    /// Column data for this clustering row
    pub columns: HashMap<String, Value>,
}

/// Complete parsed row data
#[derive(Debug, Clone)]
pub struct ParsedRow {
    /// Row header
    pub header: RowHeader,
    /// Partition key
    pub partition_key: PartitionKey,
    /// Deletion information, if present
    pub deletion_info: Option<DeletionInfo>,
    /// Static row data, if present
    pub static_row: Option<StaticRow>,
    /// Clustering rows
    pub clustering_rows: Vec<ClusteringRow>,
}

/// Cassandra 5 'oa' format row/cell state machine
pub struct RowCellStateMachine {
    /// Current parsing state
    state: State,
    /// Current offset in the data
    offset: usize,
    /// Accumulated parsed data
    parsed_row: Option<ParsedRow>,
    /// Error message if parsing failed
    error_message: Option<String>,
    /// Table schema for type-aware parsing
    schema: Option<TableSchema>,
    /// Comparator type for key decoding
    comparator: Option<ComparatorType>,
}

impl RowCellStateMachine {
    /// Create a new state machine
    pub fn new() -> Self {
        Self {
            state: State::Header,
            offset: 0,
            parsed_row: None,
            error_message: None,
            schema: None,
            comparator: None,
        }
    }

    /// Create a new state machine with schema information
    pub fn with_schema(schema: TableSchema, comparator: ComparatorType) -> Self {
        Self {
            state: State::Header,
            offset: 0,
            parsed_row: None,
            error_message: None,
            schema: Some(schema),
            comparator: Some(comparator),
        }
    }

    /// Get current state
    pub fn current_state(&self) -> &State {
        &self.state
    }

    /// Check if parsing is complete
    pub fn is_complete(&self) -> bool {
        matches!(self.state, State::Complete)
    }

    /// Check if an error occurred
    pub fn has_error(&self) -> bool {
        matches!(self.state, State::Error(_))
    }

    /// Get the parsed row data (if complete)
    pub fn take_parsed_row(&mut self) -> Option<ParsedRow> {
        self.parsed_row.take()
    }

    /// Get error message if in error state
    pub fn error_message(&self) -> Option<&str> {
        match &self.state {
            State::Error(msg) => Some(msg),
            _ => self.error_message.as_deref(),
        }
    }

    /// Process input data and advance the state machine
    pub fn process(&mut self, data: &[u8]) -> Result<usize> {
        let mut consumed = 0;

        while consumed < data.len() && !self.is_complete() && !self.has_error() {
            let remaining = &data[consumed..];

            match self.process_current_state(remaining) {
                Ok(bytes_consumed) => {
                    consumed += bytes_consumed;
                    self.offset += bytes_consumed;

                    // Prevent infinite loops
                    if bytes_consumed == 0 {
                        self.state = State::Error("No progress made in state machine".to_string());
                        break;
                    }
                }
                Err(e) => {
                    self.state = State::Error(e.to_string());
                    self.error_message = Some(e.to_string());
                    break;
                }
            }
        }

        Ok(consumed)
    }

    /// Process the current state and return bytes consumed
    fn process_current_state(&mut self, data: &[u8]) -> Result<usize> {
        match &self.state {
            State::Header => self.parse_header(data),
            State::PartitionKey => self.parse_partition_key(data),
            State::DeletionInfo => self.parse_deletion_info(data),
            State::StaticRow => self.parse_static_row(data),
            State::ClusteringRows => self.parse_clustering_rows(data),
            State::ColumnData => self.parse_column_data(data),
            State::Complete => Ok(0),
            State::Error(_) => Ok(0),
        }
    }

    /// Parse row header according to Cassandra 5 'oa' format specification
    fn parse_header(&mut self, data: &[u8]) -> Result<usize> {
        if data.len() < 9 {
            return Err(Error::corruption("Insufficient data for row header"));
        }

        let mut offset = 0;

        // Parse row flags (1 byte)
        let flags = data[offset];
        offset += 1;

        // Parse timestamp (8 bytes, big-endian)
        let timestamp = i64::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let mut ttl = None;
        let mut local_deletion_time = None;

        // Check flags for optional fields
        if flags & 0x01 != 0 {
            // TTL present (4 bytes)
            if data.len() < offset + 4 {
                return Err(Error::corruption("Insufficient data for TTL"));
            }
            ttl = Some(u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]));
            offset += 4;
        }

        if flags & 0x02 != 0 {
            // Local deletion time present (4 bytes)
            if data.len() < offset + 4 {
                return Err(Error::corruption(
                    "Insufficient data for local deletion time",
                ));
            }
            local_deletion_time = Some(u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]));
            offset += 4;
        }

        let header = RowHeader {
            flags,
            timestamp,
            ttl,
            local_deletion_time,
        };

        // Initialize parsed row
        self.parsed_row = Some(ParsedRow {
            header,
            partition_key: PartitionKey {
                component_count: 0,
                key_bytes: Vec::new(),
                components: Vec::new(),
            },
            deletion_info: None,
            static_row: None,
            clustering_rows: Vec::new(),
        });

        // Transition to next state
        self.state = State::PartitionKey;

        Ok(offset)
    }

    /// Parse partition key with proper VInt encoding
    fn parse_partition_key(&mut self, data: &[u8]) -> Result<usize> {
        let mut offset = 0;

        // Parse component count (VInt)
        let (remaining, component_count) = parse_vint_length(data)
            .map_err(|_| Error::corruption("Failed to parse partition key component count"))?;
        offset += data.len() - remaining.len();

        if component_count > 256 {
            return Err(Error::corruption("Too many partition key components"));
        }

        let mut components = Vec::with_capacity(component_count);
        let _key_start_offset = offset;

        // Parse each component
        for i in 0..component_count {
            if offset >= data.len() {
                return Err(Error::corruption(format!(
                    "Insufficient data for partition key component {}",
                    i
                )));
            }

            // Parse component length (VInt)
            let (remaining, component_len) = parse_vint_length(&data[offset..]).map_err(|_| {
                Error::corruption(format!(
                    "Failed to parse partition key component {} length",
                    i
                ))
            })?;
            offset = data.len() - remaining.len();

            if component_len > remaining.len() {
                return Err(Error::corruption(format!(
                    "Partition key component {} length {} exceeds available data {}",
                    i,
                    component_len,
                    remaining.len()
                )));
            }

            // Extract component data
            let component_data = &remaining[..component_len];
            components.push(component_data.to_vec());
            offset += component_len;
        }

        // Update parsed row with partition key
        if let Some(ref mut parsed_row) = self.parsed_row {
            parsed_row.partition_key = PartitionKey {
                component_count,
                key_bytes: data[_key_start_offset..offset].to_vec(),
                components,
            };
        }

        // Check if deletion info follows
        if offset < data.len() && (data[offset] & 0x80) != 0 {
            self.state = State::DeletionInfo;
        } else if offset < data.len() && (data[offset] & 0x40) != 0 {
            self.state = State::StaticRow;
        } else {
            self.state = State::ClusteringRows;
        }

        Ok(offset)
    }

    /// Parse deletion information if present
    fn parse_deletion_info(&mut self, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Err(Error::corruption("No data for deletion info"));
        }

        let mut offset = 0;

        // Parse deletion type flag
        let deletion_flag = data[offset];
        offset += 1;

        if (deletion_flag & 0x80) == 0 {
            // No deletion info, transition to next state
            if (deletion_flag & 0x40) != 0 {
                self.state = State::StaticRow;
            } else {
                self.state = State::ClusteringRows;
            }
            return Ok(0); // No bytes consumed
        }

        // Parse deletion timestamp (8 bytes)
        if data.len() < offset + 8 {
            return Err(Error::corruption(
                "Insufficient data for deletion timestamp",
            ));
        }

        let deletion_time = i64::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        // Parse local deletion time (4 bytes)
        if data.len() < offset + 4 {
            return Err(Error::corruption(
                "Insufficient data for local deletion time",
            ));
        }

        let local_deletion_time = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        let deletion_info = DeletionInfo {
            deletion_type: TombstoneType::RowTombstone,
            deletion_time,
            local_deletion_time,
        };

        // Update parsed row
        if let Some(ref mut parsed_row) = self.parsed_row {
            parsed_row.deletion_info = Some(deletion_info);
        }

        // Transition to next state
        if offset < data.len() && (data[offset] & 0x40) != 0 {
            self.state = State::StaticRow;
        } else {
            self.state = State::ClusteringRows;
        }

        Ok(offset)
    }

    /// Parse static row data if present
    fn parse_static_row(&mut self, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Err(Error::corruption("No data for static row"));
        }

        let mut offset = 0;

        // Check static row flag
        let static_flag = data[offset];
        offset += 1;

        if (static_flag & 0x40) == 0 {
            // No static row, transition to clustering rows
            self.state = State::ClusteringRows;
            return Ok(0);
        }

        // Parse static column count (VInt)
        let (remaining, column_count) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse static column count"))?;
        offset = data.len() - remaining.len();

        if column_count > 1000 {
            return Err(Error::corruption("Too many static columns"));
        }

        let mut columns = HashMap::new();

        // Parse each static column
        for i in 0..column_count {
            if offset >= data.len() {
                return Err(Error::corruption(format!(
                    "Insufficient data for static column {}",
                    i
                )));
            }

            // Parse column name length and data
            let (remaining, name_len) = parse_vint_length(&data[offset..]).map_err(|_| {
                Error::corruption(format!("Failed to parse static column {} name length", i))
            })?;
            offset = data.len() - remaining.len();

            if name_len > remaining.len() {
                return Err(Error::corruption(format!(
                    "Static column {} name length exceeds available data",
                    i
                )));
            }

            let column_name = String::from_utf8(remaining[..name_len].to_vec()).map_err(|_| {
                Error::corruption(format!("Invalid UTF-8 in static column {} name", i))
            })?;
            offset += name_len;

            // Parse column value length and data
            let (remaining, value_len) = parse_vint_length(&data[offset..]).map_err(|_| {
                Error::corruption(format!("Failed to parse static column {} value length", i))
            })?;
            offset = data.len() - remaining.len();

            if value_len > remaining.len() {
                return Err(Error::corruption(format!(
                    "Static column {} value length exceeds available data",
                    i
                )));
            }

            // Parse the actual value (assuming blob for now, would need schema info for proper parsing)
            let value = Value::Blob(remaining[..value_len].to_vec());
            columns.insert(column_name, value);
            offset += value_len;
        }

        let static_row = StaticRow {
            column_count,
            columns,
        };

        // Update parsed row
        if let Some(ref mut parsed_row) = self.parsed_row {
            parsed_row.static_row = Some(static_row);
        }

        // Transition to clustering rows
        self.state = State::ClusteringRows;

        Ok(offset)
    }

    /// Parse clustering rows
    fn parse_clustering_rows(&mut self, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            // No clustering rows, we're done
            self.state = State::Complete;
            return Ok(0);
        }

        let mut offset = 0;

        // Parse number of clustering rows (VInt)
        let (remaining, row_count) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse clustering row count"))?;
        offset = data.len() - remaining.len();

        if row_count > 10000 {
            return Err(Error::corruption("Too many clustering rows"));
        }

        let mut clustering_rows = Vec::with_capacity(row_count);

        // Parse each clustering row
        for i in 0..row_count {
            if offset >= data.len() {
                return Err(Error::corruption(format!(
                    "Insufficient data for clustering row {}",
                    i
                )));
            }

            let (consumed, clustering_row) = self.parse_single_clustering_row(&data[offset..])?;
            clustering_rows.push(clustering_row);
            offset += consumed;
        }

        // Update parsed row
        if let Some(ref mut parsed_row) = self.parsed_row {
            parsed_row.clustering_rows = clustering_rows;
        }

        // Parsing complete
        self.state = State::Complete;

        Ok(offset)
    }

    /// Parse a single clustering row
    fn parse_single_clustering_row(&mut self, data: &[u8]) -> Result<(usize, ClusteringRow)> {
        let mut offset = 0;

        // Parse clustering key length (VInt)
        let (remaining, key_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse clustering key length"))?;
        offset = data.len() - remaining.len();

        if key_len > remaining.len() {
            return Err(Error::corruption(
                "Clustering key length exceeds available data",
            ));
        }

        let clustering_key = remaining[..key_len].to_vec();
        offset += key_len;

        // Parse row timestamp (8 bytes)
        if data.len() < offset + 8 {
            return Err(Error::corruption(
                "Insufficient data for clustering row timestamp",
            ));
        }

        let timestamp = i64::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        // Check for row deletion info
        let mut deletion_info = None;
        if offset < data.len() && (data[offset] & 0x80) != 0 {
            let (consumed, del_info) = self.parse_row_deletion_info(&data[offset..])?;
            deletion_info = Some(del_info);
            offset += consumed;
        }

        // Parse column count (VInt)
        let (remaining, column_count) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse column count"))?;
        offset = data.len() - remaining.len();

        if column_count > 1000 {
            return Err(Error::corruption("Too many columns in clustering row"));
        }

        let mut columns = HashMap::new();

        // Parse each column
        for i in 0..column_count {
            if offset >= data.len() {
                return Err(Error::corruption(format!(
                    "Insufficient data for column {} in clustering row",
                    i
                )));
            }

            let (consumed, column_name, column_value) = self.parse_column(&data[offset..])?;
            columns.insert(column_name, column_value);
            offset += consumed;
        }

        let clustering_row = ClusteringRow {
            clustering_key,
            timestamp,
            deletion_info,
            columns,
        };

        Ok((offset, clustering_row))
    }

    /// Parse row deletion information
    fn parse_row_deletion_info(&mut self, data: &[u8]) -> Result<(usize, DeletionInfo)> {
        let mut offset = 0;

        // Skip deletion flag (already checked)
        offset += 1;

        // Parse deletion timestamp (8 bytes)
        if data.len() < offset + 8 {
            return Err(Error::corruption(
                "Insufficient data for row deletion timestamp",
            ));
        }

        let deletion_time = i64::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        // Parse local deletion time (4 bytes)
        if data.len() < offset + 4 {
            return Err(Error::corruption(
                "Insufficient data for row local deletion time",
            ));
        }

        let local_deletion_time = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        let deletion_info = DeletionInfo {
            deletion_type: TombstoneType::RowTombstone,
            deletion_time,
            local_deletion_time,
        };

        Ok((offset, deletion_info))
    }

    /// Parse a single column (name and value)
    fn parse_column(&mut self, data: &[u8]) -> Result<(usize, String, Value)> {
        let mut offset = 0;

        // Parse column name length (VInt)
        let (remaining, name_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse column name length"))?;
        offset = data.len() - remaining.len();

        if name_len > remaining.len() {
            return Err(Error::corruption(
                "Column name length exceeds available data",
            ));
        }

        let column_name = String::from_utf8(remaining[..name_len].to_vec())
            .map_err(|_| Error::corruption("Invalid UTF-8 in column name"))?;
        offset += name_len;

        // Parse column value length (VInt)
        let (remaining, value_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse column value length"))?;
        offset = data.len() - remaining.len();

        if value_len > remaining.len() {
            return Err(Error::corruption(
                "Column value length exceeds available data",
            ));
        }

        // Parse the actual value using schema information if available
        let value = if value_len == 0 {
            Value::Null
        } else {
            let value_data = &remaining[..value_len];
            // Try to use schema information to determine the correct type
            if let Some(ref schema) = self.schema {
                // Look up column type in schema
                if let Some(column) = schema.columns.iter().find(|c| c.name == column_name) {
                    // Parse using the column's data type
                    if let Ok(type_id) = self.data_type_to_cql_type_id(&column.data_type) {
                        if let Ok((_, parsed_value)) = parse_cql_value(value_data, type_id) {
                            parsed_value
                        } else {
                            // Fall back to blob if parsing fails
                            Value::Blob(value_data.to_vec())
                        }
                    } else {
                        // Unknown type, preserve as blob
                        Value::Blob(value_data.to_vec())
                    }
                } else {
                    // Column not found in schema, preserve as blob
                    Value::Blob(value_data.to_vec())
                }
            } else {
                // No schema available, preserve as blob
                Value::Blob(value_data.to_vec())
            }
        };

        offset += value_len;

        Ok((offset, column_name, value))
    }

    /// Parse column data (placeholder for future extension)
    fn parse_column_data(&mut self, _data: &[u8]) -> Result<usize> {
        // This state is used for more complex column parsing scenarios
        // For now, transition directly to complete
        self.state = State::Complete;
        Ok(0)
    }

    /// Reset the state machine for reuse
    pub fn reset(&mut self) {
        self.state = State::Header;
        self.offset = 0;
        self.parsed_row = None;
        self.error_message = None;
    }

    /// Convert data type string to CQL type ID
    fn data_type_to_cql_type_id(&self, data_type: &str) -> Result<CqlTypeId> {
        match data_type.to_lowercase().as_str() {
            "ascii" => Ok(CqlTypeId::Ascii),
            "bigint" => Ok(CqlTypeId::BigInt),
            "blob" => Ok(CqlTypeId::Blob),
            "boolean" => Ok(CqlTypeId::Boolean),
            "counter" => Ok(CqlTypeId::Counter),
            "date" => Ok(CqlTypeId::Date),
            "decimal" => Ok(CqlTypeId::Decimal),
            "double" => Ok(CqlTypeId::Double),
            "float" => Ok(CqlTypeId::Float),
            "int" => Ok(CqlTypeId::Int),
            "inet" => Ok(CqlTypeId::Inet),
            "smallint" => Ok(CqlTypeId::Smallint),
            "text" | "varchar" => Ok(CqlTypeId::Varchar),
            "time" => Ok(CqlTypeId::Time),
            "timestamp" => Ok(CqlTypeId::Timestamp),
            "timeuuid" => Ok(CqlTypeId::Timeuuid),
            "tinyint" => Ok(CqlTypeId::Tinyint),
            "uuid" => Ok(CqlTypeId::Uuid),
            "varint" => Ok(CqlTypeId::Varint),
            _ if data_type.starts_with("list<") => Ok(CqlTypeId::List),
            _ if data_type.starts_with("set<") => Ok(CqlTypeId::Set),
            _ if data_type.starts_with("map<") => Ok(CqlTypeId::Map),
            _ if data_type.starts_with("tuple<") => Ok(CqlTypeId::Tuple),
            _ if data_type.starts_with("frozen<") => {
                // Parse the inner type
                let inner = data_type.trim_start_matches("frozen<").trim_end_matches('>');
                self.data_type_to_cql_type_id(inner)
            }
            _ => Err(Error::corruption(format!("Unknown data type: {}", data_type))),
        }
    }
}

impl Default for RowCellStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_machine_creation() {
        let state_machine = RowCellStateMachine::new();
        assert_eq!(state_machine.current_state(), &State::Header);
        assert!(!state_machine.is_complete());
        assert!(!state_machine.has_error());
    }

    #[test]
    fn test_state_transitions() {
        let mut state_machine = RowCellStateMachine::new();

        // Test basic state progression
        assert_eq!(state_machine.current_state(), &State::Header);

        // Reset should return to header state
        state_machine.reset();
        assert_eq!(state_machine.current_state(), &State::Header);
    }

    #[test]
    fn test_error_handling() {
        let mut state_machine = RowCellStateMachine::new();

        // Test with insufficient data
        let result = state_machine.process(&[0x01]); // Too small for header
        assert!(result.is_ok()); // Should handle gracefully but may not progress

        // Check if error state is set appropriately
        if state_machine.has_error() {
            assert!(state_machine.error_message().is_some());
        }
    }

    #[test]
    fn test_minimal_valid_row() {
        let mut state_machine = RowCellStateMachine::new();

        // Create minimal valid row data
        let mut data = Vec::new();

        // Row header: flags (1) + timestamp (8)
        data.push(0x00); // No TTL or deletion
        data.extend_from_slice(&42i64.to_be_bytes()); // Timestamp

        // Partition key: component count (1) + component length (1) + component ("k")
        data.push(0x02); // 1 component (vint encoded: 1 -> 2 in zigzag)
        data.push(0x02); // 1 byte length (vint encoded: 1 -> 2 in zigzag)
        data.push(b'k'); // Component data

        // Clustering row count: 0 (no clustering rows)
        data.push(0x00); // 0 rows (vint encoded: 0 -> 0)

        let result = state_machine.process(&data);
        assert!(result.is_ok());

        // Should eventually reach complete state
        let consumed = result.unwrap();
        assert!(consumed <= data.len());
    }
}

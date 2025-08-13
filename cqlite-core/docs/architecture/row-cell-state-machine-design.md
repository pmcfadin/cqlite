# Cassandra 5 'oa' Format Row/Cell State Machine Design

## Overview

This document describes the design and implementation of a spec-accurate state machine for parsing Cassandra 5+ SSTable row and cell data according to the exact binary format specification.

## Architecture

### State Machine Design

The `RowCellStateMachine` implements a finite state machine that processes Cassandra 5 'oa' format data through the following states:

```
HEADER → PARTITION_KEY → DELETION_INFO → STATIC_ROW → CLUSTERING_ROWS → COLUMN_DATA → COMPLETE
```

### State Transitions

1. **HEADER**: Parses row flags, timestamp, optional TTL, and local deletion time
2. **PARTITION_KEY**: Parses partition key components using VInt encoding
3. **DELETION_INFO**: Optionally parses deletion information if present
4. **STATIC_ROW**: Optionally parses static row data if present
5. **CLUSTERING_ROWS**: Parses clustering rows with their column data
6. **COLUMN_DATA**: Reserved for complex column parsing scenarios
7. **COMPLETE**: Final state indicating successful parsing

### Key Features

#### VInt Decoding
- Uses proper Cassandra VInt decoding throughout the state machine
- Handles variable-length integers for component counts, lengths, and sizes
- Compatible with Cassandra's ZigZag encoding for signed integers

#### Optional Components Handling
- **TTL Information**: Parsed when flags indicate presence
- **Deletion Info**: Handles row-level and cell-level tombstones
- **Static Rows**: Processes static column data when present

#### Error Handling
- Comprehensive validation at each state transition
- Prevents buffer overruns with length validation
- Graceful error reporting with detailed context

#### Byte-Level Accuracy
- Maintains exact compatibility with Cassandra format specification
- Proper handling of big-endian encoded timestamps and integers
- Accurate parsing of compound/composite keys

## Data Structures

### RowHeader
```rust
pub struct RowHeader {
    pub flags: u8,                           // Row flags
    pub timestamp: i64,                      // Row timestamp
    pub ttl: Option<u32>,                    // TTL if present
    pub local_deletion_time: Option<u32>,    // Local deletion time if present
}
```

### PartitionKey
```rust
pub struct PartitionKey {
    pub component_count: usize,              // Number of key components
    pub key_bytes: Vec<u8>,                  // Raw key bytes
    pub components: Vec<Vec<u8>>,            // Parsed components
}
```

### DeletionInfo
```rust
pub struct DeletionInfo {
    pub deletion_type: TombstoneType,        // Type of deletion
    pub deletion_time: i64,                  // Deletion timestamp
    pub local_deletion_time: u32,            // Local deletion time
}
```

### ParsedRow
```rust
pub struct ParsedRow {
    pub header: RowHeader,                   // Row header
    pub partition_key: PartitionKey,         // Partition key
    pub deletion_info: Option<DeletionInfo>, // Deletion info if present
    pub static_row: Option<StaticRow>,       // Static row if present
    pub clustering_rows: Vec<ClusteringRow>, // Clustering rows
}
```

## Integration with SSTableReader

### State Machine Integration
The state machine is integrated into the existing `SSTableReader` through:

1. **Format Detection**: Automatically uses state machine for Cassandra 5+ formats
2. **Fallback Support**: Falls back to legacy parsing for older formats
3. **Entry Conversion**: Converts parsed rows to standard entry format
4. **Error Recovery**: Graceful handling of parsing failures

### Usage Pattern
```rust
// Create state machine
let mut state_machine = RowCellStateMachine::new();

// Process data
let consumed = state_machine.process(data)?;

// Check completion
if state_machine.is_complete() {
    if let Some(parsed_row) = state_machine.take_parsed_row() {
        // Convert to entries and use
        let entries = convert_parsed_row_to_entries(&parsed_row)?;
    }
}
```

## Performance Characteristics

### Memory Efficiency
- Streaming parsing without full data buffering
- Minimal memory allocation during parsing
- Reusable state machine instances

### Error Recovery
- Graceful degradation to legacy parsing
- Detailed error messages for debugging
- Prevention of infinite loops and crashes

### Scalability
- Handles large rows and clustering data efficiently
- Supports thousands of columns per row
- Efficient parsing of complex partition keys

## Specification Compliance

### Cassandra 5 'oa' Format
- **Header Format**: Exact flag parsing and optional field handling
- **VInt Encoding**: Full compatibility with Cassandra's variable-length integers
- **Timestamp Handling**: Proper big-endian 64-bit timestamp parsing
- **Key Components**: Accurate parsing of multi-component partition keys
- **Column Data**: Proper handling of column names and values

### Binary Format Accuracy
- Byte-level compatibility with Cassandra output
- Proper handling of empty values and null data
- Accurate parsing of deletion markers and TTL information

## Testing Strategy

### Unit Tests
- State transition validation
- Error condition handling
- Edge case processing (empty data, malformed input)
- VInt encoding/decoding verification

### Integration Tests
- Real Cassandra SSTable parsing
- Format compatibility verification
- Performance benchmarking
- Memory usage validation

### Compatibility Tests
- Cross-version compatibility testing
- Format migration validation
- Error recovery testing

## Future Enhancements

### Schema Integration
- Use schema metadata for proper type parsing
- Enhanced collection type handling
- UDT (User Defined Type) support

### Performance Optimizations
- Zero-copy parsing where possible
- SIMD acceleration for bulk operations
- Parallel processing of clustering rows

### Extended Format Support
- Cassandra 6+ format preparation
- Custom format extensions
- Advanced compression handling

## Implementation Files

- `row_cell_state_machine.rs`: Core state machine implementation
- `reader.rs`: Integration with SSTableReader
- `types.rs`: Supporting data structures
- `vint.rs`: VInt encoding/decoding utilities

## Conclusion

The Row/Cell State Machine provides a robust, spec-accurate foundation for parsing Cassandra 5+ SSTable data. Its design emphasizes correctness, performance, and maintainability while providing seamless integration with existing codebase infrastructure.
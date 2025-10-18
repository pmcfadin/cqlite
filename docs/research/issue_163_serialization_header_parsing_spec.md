# Issue #163: SerializationHeader Parsing Specification

**Date**: 2025-10-17
**Status**: Implementation Ready
**Cassandra Version**: 5.0 (nb-format)

## Executive Summary

This document specifies the complete binary format and parsing algorithm for the SerializationHeader section embedded in Cassandra 5.0 Statistics.db files. The SerializationHeader contains partition key, clustering key, and regular column metadata required for schema extraction.

**Critical Insight**: The SerializationHeader is NOT contiguous with EncodingStats. There is a ~4900-byte intermediate section (bloom filter/histogram data) between EncodingStats and SerializationHeader that must be skipped or searched through.

## Problem Statement

The current parser (Issue #163) successfully extracts regular column definitions from SerializationHeader but marks all columns as `is_primary_key: false`, causing schema extraction to fail with "No partition keys found". The parser must:

1. Extract partition key type(s) and mark them as primary keys
2. Extract clustering key type(s) and mark them as clustering keys
3. Extract regular column definitions
4. Extract static column definitions (if present)
5. Assign correct `key_position` values to maintain ordering

## Binary Format Layout

### Overall Structure

```
=== Statistics.db File Structure ===

[0x00-0x1F]      Header (32 bytes)
[0x20-~0x70]     EncodingStats section (~80 bytes)
[~0x70-~0x1399]  Intermediate Data (~4900 bytes)
                 - Bloom filter metadata
                 - Histogram data
                 - Other statistics
[~0x1399+]       SerializationHeader section
```

### SerializationHeader Internal Structure

Based on Cassandra's `SerializationHeader.Serializer.deserialize()` (line 96), the format is:

```
=== SerializationHeader Binary Format ===

1. Partition Key Types Section:
   [VInt: type_descriptor_length]
   [UTF-8: type_descriptor_string]  <- Starts with '(' character (0x28)

2. Clustering Key Types Section (if clustering keys exist):
   [VInt: clustering_key_count]
   For each clustering key (0 to N):
     [complex type encoding]

3. Section Marker:
   [0x00 0x00]  <- Fixed 2-byte marker

4. Regular Columns Section:
   [VInt: regular_column_count]
   For each regular column:
     [byte: name_length]
     [UTF-8: column_name]
     [byte: type_separator]  <- Category indicator ('+', ')', '(', etc.)
     [UTF-8: type_string]    <- "org.apache.cassandra.db.marshal.XxxType"

5. Static Columns Section (if static columns exist):
   [VInt: static_column_count]
   For each static column:
     [same format as regular columns]
```

## Detailed Format Specification

### 1. Partition Key Type Descriptor

**Location**: First field in SerializationHeader
**Format**: VInt-length-prefixed UTF-8 string

**Single Partition Key** (most common):
```
Bytes: [0x80 0x28] "(org.apache.cassandra.db.marshal.UUIDType"
       ^^^^^^^^^^ VInt encoding of 40 (string length)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Type string (40 bytes)
```

**Composite Partition Key** (multiple columns):
```
Bytes: [VInt: length] "(org.apache.cassandra.db.marshal.CompositeType(..."
                      ^^^ Starts with '(' (0x28)
```

**Key Characteristics**:
- Always starts with `'('` character (0x28) for single keys
- May start with `'('` followed by `org.apache.cassandra.db.marshal.CompositeType` for composite keys
- VInt length includes the opening parenthesis
- String does NOT have closing parenthesis

**VInt Encoding Rules**:
- Single byte (< 0x80): value is the byte itself
- Multi-byte (>= 0x80): `0x80 0xNN` means value is `0xNN` (for values 0-127)
- For longer values, see Cassandra VInt specification

### 2. Clustering Key Section

**Location**: After partition key type, before 0x00 0x00 marker
**Format**: Variable, only present if clustering keys exist

**Detection Strategy**:
- If next bytes after partition key type are `0x00 0x00`, no clustering keys
- If next bytes form a VInt count > 0, clustering keys section follows

**Clustering Key Encoding** (from composite_key_table analysis):
```
[VInt: count]  <- Number of clustering columns (e.g., 0x02 for 2 keys)
For each clustering key:
  [byte: flags/encoding_type]
  [variable: type_encoding]
  [variable: name_encoding or position]
```

**Type Prefix Markers**:
- `'['` (0x5b): Indicates composite/reversed clustering type
- Example: `[org.apache.cassandra.db.marshal.ReversedType(TimestampType)`

**Parsing Complexity**: HIGH
- Format varies by clustering key configuration
- ReversedType wraps the actual type
- Multiple clustering keys use composite encoding
- **Recommendation**: Initial implementation can skip clustering key section by searching for `0x00 0x00` marker

### 3. Section Marker

**Location**: After clustering key section (or after partition key if no clustering)
**Format**: Fixed 2 bytes

```
0x00 0x00
```

**Purpose**: Delimits primary key metadata from column metadata

### 4. Regular Columns Section

**Location**: Immediately after `0x00 0x00` marker
**Format**: VInt count followed by column definitions

**Column Definition Format**:
```
[byte: name_length]           <- Single byte (0x00-0xFF)
[UTF-8: column_name]          <- N bytes
[byte: type_separator]        <- Single byte (category indicator)
[UTF-8: type_string]          <- Variable length, ends at next separator or section end
```

**Example** (from simple_table at 0x1d5d):
```
Offset  Hex Bytes                               Decoded
0x1d5d: 00 00                                   Section marker
0x1d5f: 12                                      Column count = 0x12 (18 decimal)
0x1d60: 0f                                      Name length = 15
0x1d61: 61 63 63 6f 75 6e 74 5f 62 61 6c...   "account_balance"
0x1d70: 2b                                      Type separator '+' (DecimalType category)
0x1d71: 6f 72 67 2e 61 70 61 63 68 65...       "org.apache.cassandra.db.marshal.DecimalType"
```

**Type Separator Categories** (observed patterns):
- `'+'` (0x2b): DecimalType
- `')'` (0x29): Int32Type, ByteType, ShortType (numeric)
- `'('` (0x28): UTF8Type (text)
- `','` (0x2c): TimeUUIDType, DurationType (time-based)
- `'.'` (0x2e): SimpleDateType
- `'-'` (0x2d): TimestampType
- `'*'` (0x2a): DoubleType, FloatType
- `'/'` (0x2f): InetAddressType

**Note**: The separator is part of the type string encoding, not a field delimiter. Parse until the next name_length byte or end of section.

### 5. Static Columns Section

**Location**: After regular columns section (if present)
**Format**: Same as regular columns

**Detection**:
- After parsing all regular columns, check if more data remains
- If next VInt > 0, static column section follows
- Static columns use same encoding as regular columns

## Parsing Algorithm

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Parse Statistics.db Header (32 bytes)                    │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Parse EncodingStats Section                              │
│    - metadata_type (u32)                                    │
│    - partitioner string                                     │
│    - min_timestamp, min_deletion_time, min_ttl (VInts)     │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. SKIP Intermediate Data (~4900 bytes)                     │
│    Strategy: Search for partition key type pattern          │
│    Pattern: VInt followed by "(org.apache.cassandra..."     │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Parse SerializationHeader                                │
│    ├─ Parse partition key type descriptor                   │
│    ├─ Parse clustering key section (if present)             │
│    ├─ Expect 0x00 0x00 marker                              │
│    ├─ Parse regular columns                                 │
│    └─ Parse static columns (if present)                     │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Populate ColumnInfo Structures                           │
│    - Set is_primary_key = true for partition keys           │
│    - Set is_clustering = true for clustering keys           │
│    - Set key_position for all keys                          │
└─────────────────────────────────────────────────────────────┘
```

### Detailed Parsing Steps

#### Step 1: Locate SerializationHeader Start

**Problem**: SerializationHeader is ~4900 bytes after EncodingStats
**Solution**: Pattern-based search

```rust
fn find_serialization_header_start(input: &[u8]) -> IResult<&[u8], usize> {
    // Search for pattern: VInt + "(org.apache.cassandra.db.marshal."
    // Max search window: 8KB from current position

    let search_pattern = b"(org.apache.cassandra.db.marshal.";
    let max_search = 8192;

    for offset in 0..max_search.min(input.len() - 50) {
        // Check for VInt (single or multi-byte) followed by pattern
        if input[offset..].starts_with(search_pattern) {
            // Found it! Now find the VInt that precedes it
            // Work backwards to find the length VInt
            return Ok(offset - vint_length);
        }

        // Alternative: Check for 0x80 0xNN pattern (common VInt encoding)
        if offset >= 2 && input[offset-2] == 0x80 {
            let length = input[offset-1] as usize;
            if offset + length <= input.len() {
                if &input[offset..offset+length] == search_pattern {
                    return Ok(offset - 2); // VInt starts 2 bytes before
                }
            }
        }
    }

    Err(nom::Err::Error(...)) // Not found
}
```

**Validation**:
- Verify VInt value matches string length
- Ensure string is valid UTF-8
- Check string starts with expected pattern

#### Step 2: Parse Partition Key Type

```rust
fn parse_partition_key_type(input: &[u8]) -> IResult<&[u8], PartitionKeyInfo> {
    // Parse VInt length
    let (input, type_len) = parse_vuint(input)?;

    // Validate length is reasonable (typically 30-100 bytes)
    if type_len == 0 || type_len > 500 {
        return Err(nom::Err::Error(...));
    }

    // Extract type string
    let (input, type_bytes) = take(type_len as usize)(input)?;
    let type_string = std::str::from_utf8(type_bytes)?;

    // Validate format
    if !type_string.starts_with("(org.apache.cassandra.db.marshal.") {
        return Err(nom::Err::Error(...));
    }

    // Determine if composite (multiple partition keys) or simple
    let is_composite = type_string.contains("CompositeType");

    let partition_key_info = if is_composite {
        parse_composite_partition_key_type(type_string)?
    } else {
        PartitionKeyInfo {
            keys: vec![PartitionKey {
                position: 0,
                data_type: extract_type_name(type_string),
            }],
        }
    };

    Ok((input, partition_key_info))
}
```

**Type Name Extraction**:
```rust
fn extract_type_name(type_string: &str) -> String {
    // Input: "(org.apache.cassandra.db.marshal.UUIDType"
    // Output: "uuid"

    // Remove leading '('
    let s = type_string.trim_start_matches('(');

    // Extract class name (last component)
    let class_name = s.split('.').last().unwrap_or(s);

    // Remove "Type" suffix
    let type_base = class_name.trim_end_matches("Type");

    // Convert to CQL type name
    convert_marshal_type_to_cql(type_base)
}
```

#### Step 3: Parse Clustering Key Section

**Detection**:
```rust
fn parse_clustering_keys_section(input: &[u8]) -> IResult<&[u8], Vec<ClusteringKeyInfo>> {
    // Check for 0x00 0x00 marker (no clustering keys)
    if input.len() >= 2 && input[0] == 0x00 && input[1] == 0x00 {
        return Ok((input, vec![])); // No clustering keys
    }

    // Parse clustering key count
    let (input, count) = parse_vuint(input)?;

    if count == 0 {
        // Expect 0x00 0x00 marker next
        return Ok((input, vec![]));
    }

    // Parse clustering key metadata (COMPLEX - varies by configuration)
    let (input, clustering_keys) = parse_clustering_key_list(input, count)?;

    Ok((input, clustering_keys))
}
```

**Simplified Approach** (Phase 1 Implementation):
```rust
fn skip_to_column_section_marker(input: &[u8]) -> IResult<&[u8], ()> {
    // Search for 0x00 0x00 marker within next 500 bytes
    for offset in 0..500.min(input.len() - 2) {
        if input[offset] == 0x00 && input[offset + 1] == 0x00 {
            // Check if followed by reasonable column count (1-200)
            if offset + 2 < input.len() {
                let potential_count = input[offset + 2];
                if potential_count > 0 && potential_count < 200 {
                    return Ok((&input[offset..], ()));
                }
            }
        }
    }
    Err(nom::Err::Error(...))
}
```

**Phase 2 Enhancement** (Full Clustering Key Parsing):
- Parse clustering key count
- For each clustering key, parse type encoding
- Extract position and ordering information
- Handle ReversedType wrapping
- Map to column names (if available in another section)

#### Step 4: Parse Section Marker

```rust
fn expect_section_marker(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, marker) = take(2usize)(input)?;

    if marker != [0x00, 0x00] {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((input, ()))
}
```

#### Step 5: Parse Regular Columns

```rust
fn parse_regular_columns(input: &[u8]) -> IResult<&[u8], Vec<ColumnDefinition>> {
    // Parse column count
    let (input, column_count) = parse_vuint(input)?;

    if column_count == 0 || column_count > 500 {
        return Err(nom::Err::Error(...)); // Sanity check
    }

    let mut columns = Vec::with_capacity(column_count as usize);
    let mut remaining = input;

    for _ in 0..column_count {
        let (rest, column) = parse_column_definition(remaining)?;
        columns.push(column);
        remaining = rest;
    }

    Ok((remaining, columns))
}

fn parse_column_definition(input: &[u8]) -> IResult<&[u8], ColumnDefinition> {
    // Parse name length (single byte)
    let (input, name_len) = be_u8(input)?;

    // Validate name length
    if name_len == 0 || name_len > 200 {
        return Err(nom::Err::Error(...));
    }

    // Parse column name
    let (input, name_bytes) = take(name_len as usize)(input)?;
    let name = std::str::from_utf8(name_bytes)?.to_string();

    // Parse type separator (single byte)
    let (input, separator) = be_u8(input)?;

    // Parse type string
    // Strategy: Read until we hit a single-byte value that could be
    // the next column's name_length (1-200) OR until we've read a
    // reasonable type string length (30-100 bytes)
    let (input, type_string) = parse_type_string(input)?;

    let data_type = convert_marshal_type_to_cql(&type_string);

    Ok((input, ColumnDefinition { name, data_type }))
}
```

**Type String Parsing Strategy**:
```rust
fn parse_type_string(input: &[u8]) -> IResult<&[u8], String> {
    // Type strings are variable length, ending when we hit:
    // 1. Next column's name_length byte (1-200 range)
    // 2. End of section

    // Look for "org.apache.cassandra.db.marshal." pattern
    // Type string typically 30-100 bytes

    for len in 30..200 {
        if len > input.len() {
            break;
        }

        let candidate = &input[0..len];
        if let Ok(s) = std::str::from_utf8(candidate) {
            if s.starts_with("org.apache.cassandra.db.marshal.")
               && s.ends_with("Type") {
                // Check if next byte is a reasonable name_length
                if len < input.len() {
                    let next_byte = input[len];
                    if next_byte > 0 && next_byte < 200 {
                        return Ok((&input[len..], s.to_string()));
                    }
                }
            }
        }
    }

    Err(nom::Err::Error(...))
}
```

**Alternative Approach** (More Robust):
```rust
fn parse_type_string_robust(input: &[u8]) -> IResult<&[u8], String> {
    // Type strings follow pattern: "org.apache.cassandra.db.marshal.XxxType"
    // Next byte after "Type" is either:
    // - Name length of next column (1-200)
    // - Start of static section
    // - End of buffer

    let prefix = b"org.apache.cassandra.db.marshal.";
    if !input.starts_with(prefix) {
        return Err(nom::Err::Error(...));
    }

    // Find "Type" suffix
    for offset in prefix.len()..input.len() {
        if input[offset..].starts_with(b"Type") {
            let type_end = offset + 4; // "Type".len()
            let type_bytes = &input[0..type_end];
            let type_string = std::str::from_utf8(type_bytes)?.to_string();
            return Ok((&input[type_end..], type_string));
        }
    }

    Err(nom::Err::Error(...))
}
```

#### Step 6: Parse Static Columns (Optional)

```rust
fn parse_static_columns_optional(input: &[u8]) -> IResult<&[u8], Vec<ColumnDefinition>> {
    // If no more data, no static columns
    if input.is_empty() {
        return Ok((input, vec![]));
    }

    // Try to parse static column count
    let result = parse_vuint(input);
    match result {
        Ok((rest, count)) if count > 0 && count < 100 => {
            // Parse static columns (same format as regular columns)
            parse_column_list(rest, count)
        }
        _ => {
            // No static columns
            Ok((input, vec![]))
        }
    }
}
```

### Step 7: Populate ColumnInfo Structures

```rust
fn build_column_info_list(
    partition_keys: &[PartitionKey],
    clustering_keys: &[ClusteringKeyInfo],
    regular_columns: &[ColumnDefinition],
    static_columns: &[ColumnDefinition],
) -> Vec<ColumnInfo> {
    let mut columns = Vec::new();

    // Add partition keys
    for (pos, pk) in partition_keys.iter().enumerate() {
        columns.push(ColumnInfo {
            name: pk.name.clone(), // May need to derive from type or lookup
            column_type: pk.data_type.clone(),
            is_primary_key: true,
            key_position: Some(pos as u16),
            is_static: false,
            is_clustering: false,
        });
    }

    // Add clustering keys
    for (pos, ck) in clustering_keys.iter().enumerate() {
        columns.push(ColumnInfo {
            name: ck.name.clone(),
            column_type: ck.data_type.clone(),
            is_primary_key: true,
            key_position: Some(pos as u16),
            is_static: false,
            is_clustering: true,
        });
    }

    // Add regular columns
    for col in regular_columns {
        columns.push(ColumnInfo {
            name: col.name.clone(),
            column_type: col.data_type.clone(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
        });
    }

    // Add static columns
    for col in static_columns {
        columns.push(ColumnInfo {
            name: col.name.clone(),
            column_type: col.data_type.clone(),
            is_primary_key: false,
            key_position: None,
            is_static: true,
            is_clustering: false,
        });
    }

    columns
}
```

## Edge Cases and Validation

### Edge Case 1: Composite Partition Keys

**Scenario**: Table with multiple partition key columns

**Format**:
```
"(org.apache.cassandra.db.marshal.CompositeType(
    org.apache.cassandra.db.marshal.UUIDType,
    org.apache.cassandra.db.marshal.UTF8Type
)"
```

**Handling**:
- Parse CompositeType content
- Extract individual component types
- Assign sequential positions
- **Challenge**: Column names not embedded in type string

**Solution**:
- Phase 1: Mark as single composite partition key
- Phase 2: Parse component types and correlate with column names from another source (TOC, schema.cql)

### Edge Case 2: Zero Clustering Keys

**Scenario**: Simple table with only partition key

**Format**:
```
[VInt: partition_key_type_len]
[UTF-8: partition_key_type]
[0x00 0x00]  <- Immediate marker
[VInt: regular_column_count]
...
```

**Handling**: Detection works naturally (0x00 0x00 immediately after partition key type)

### Edge Case 3: Reversed Clustering Keys

**Scenario**: Clustering key with DESC ordering

**Format**:
```
"[org.apache.cassandra.db.marshal.ReversedType(
    org.apache.cassandra.db.marshal.TimestampType
)"
```

**Handling**:
- Detect `ReversedType` wrapper
- Extract inner type
- Set clustering order to DESC
- **Note**: Requires full clustering key parsing (Phase 2)

### Edge Case 4: No Regular Columns

**Scenario**: Table with only primary key columns

**Validation**:
- Column count = 0 is valid
- Proceed to static column section or end

### Edge Case 5: Collection Types

**Format**:
```
"org.apache.cassandra.db.marshal.ListType(UTF8Type)"
"org.apache.cassandra.db.marshal.MapType(Int32Type,UTF8Type)"
"org.apache.cassandra.db.marshal.SetType(UUIDType)"
```

**Handling**:
- Parse full type string including nested types
- Convert to CQL syntax: `list<text>`, `map<int, text>`, `set<uuid>`

## Validation Rules

### Required Validations

1. **Partition Key Type**:
   - VInt length must be > 0 and < 500
   - String must start with `"(org.apache.cassandra.db.marshal."`
   - Must be valid UTF-8

2. **Section Marker**:
   - Must be exactly `0x00 0x00`
   - Must appear after partition/clustering key section

3. **Column Count**:
   - Must be >= 0 and <= 500 (sanity check)
   - Must match actual number of columns parsed

4. **Column Names**:
   - Length must be > 0 and < 200
   - Must be valid UTF-8
   - Should match regex `[a-z][a-z0-9_]*` (lowercase identifiers)

5. **Column Types**:
   - Must start with `"org.apache.cassandra.db.marshal."`
   - Must end with `"Type"`
   - Must be valid UTF-8

### Error Handling

**Fail Fast**:
- Invalid VInt encoding
- Buffer overflow (reading past end)
- Invalid UTF-8 sequences
- Section marker not found

**Graceful Degradation**:
- Unknown column types → map to "unknown" or "blob"
- Missing static columns → empty list
- Cannot parse clustering keys → skip to regular columns (Phase 1)

## Test Cases

### Test Case 1: Simple Table (simple_table)

**Input**: `/test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Statistics.db`

**Expected Output**:
```rust
PartitionKeys: [
    ColumnInfo { name: "id", type: "uuid", is_primary_key: true, key_position: Some(0), is_clustering: false }
]
ClusteringKeys: []
RegularColumns: [
    ColumnInfo { name: "account_balance", type: "decimal", ... },
    ColumnInfo { name: "active", type: "boolean", ... },
    // ... 16 more columns
]
StaticColumns: []
```

**Validation**:
- 1 partition key (id: UUID)
- 0 clustering keys
- 18 regular columns
- Column count in binary = 0x12 (18)

### Test Case 2: Composite Key Table (composite_key_table)

**Input**: `/test-data/datasets/sstables/test_basic/composite_key_table-*/nb-1-big-Statistics.db`

**Expected Output**:
```rust
PartitionKeys: [
    ColumnInfo { name: "id", type: "uuid", is_primary_key: true, key_position: Some(0), is_clustering: false }
]
ClusteringKeys: [
    ColumnInfo { name: "timestamp", type: "timestamp", is_primary_key: true, key_position: Some(0), is_clustering: true },
    ColumnInfo { name: "category", type: "text", is_primary_key: true, key_position: Some(1), is_clustering: true }
]
RegularColumns: [
    ColumnInfo { name: "data", type: "text", ... },
    ColumnInfo { name: "value", type: "int", ... }
]
StaticColumns: []
```

**Validation**:
- 1 partition key (id: UUID)
- 2 clustering keys (timestamp: ReversedType(TimestampType), category: UTF8Type)
- 2 regular columns
- Clustering key section detected before 0x00 0x00 marker

### Test Case 3: TTL Test Table (ttl_test_table)

**Input**: `/test-data/datasets/sstables/test_basic/ttl_test_table-*/nb-1-big-Statistics.db`

**Expected Output**:
```rust
PartitionKeys: [
    ColumnInfo { name: "id", type: "uuid", is_primary_key: true, key_position: Some(0), is_clustering: false }
]
ClusteringKeys: []
RegularColumns: [
    ColumnInfo { name: "expiring_value", type: "text", ... },
    ColumnInfo { name: "session_info", type: "text", ... },
    ColumnInfo { name: "temporary_data", type: "text", ... }
]
StaticColumns: []
```

**Validation**:
- 1 partition key (id: UUID)
- 0 clustering keys
- 3 regular columns
- Column count in binary = 0x03 (3)
- SerializationHeader starts at offset 0x1399

### Test Case 4: Static Columns Table

**Input**: `/test-data/datasets/sstables/test_basic/static_columns_table-*/nb-1-big-Statistics.db`

**Expected Output**:
```rust
PartitionKeys: [ ... ]
ClusteringKeys: [ ... ]
RegularColumns: [ ... ]
StaticColumns: [
    ColumnInfo { name: "static_col1", type: "text", is_static: true, ... }
]
```

**Validation**:
- Static columns parsed after regular columns
- `is_static` flag set correctly

## Implementation Phases

### Phase 1: Minimal Viable Implementation

**Goal**: Extract partition keys and regular columns for simple tables

**Scope**:
- Parse partition key type (single partition key only)
- Skip clustering key section using 0x00 0x00 marker search
- Parse regular columns
- Skip static columns
- Mark partition key as `is_primary_key: true`

**Limitations**:
- Composite partition keys not supported
- Clustering keys not extracted (skipped)
- Static columns ignored

**Deliverables**:
- Fixes "No partition keys found" error
- Enables schema extraction for simple tables
- Passes simple_table and ttl_test_table tests

### Phase 2: Full Implementation

**Goal**: Support all SerializationHeader features

**Scope**:
- Parse composite partition keys
- Parse clustering key section fully
- Extract clustering key names and types
- Parse static columns
- Handle ReversedType wrapping
- Support collection types

**Deliverables**:
- Passes composite_key_table test
- Passes static_columns_table test
- Full schema extraction for all table types

### Phase 3: Validation and Hardening

**Goal**: Production-ready parser

**Scope**:
- Comprehensive error handling
- Fuzzing and edge case testing
- Performance optimization
- Hex dump validation against Cassandra source
- Cross-validation with sstabledump output

## Pseudo-Code Summary

```rust
// Main entry point
pub fn parse_serialization_header(input: &[u8]) -> IResult<&[u8], SerializationHeader> {
    // Step 1: Find SerializationHeader start (skip intermediate data)
    let (input, sh_start) = find_serialization_header_start(input)?;

    // Step 2: Parse partition key type
    let (input, partition_keys) = parse_partition_key_type(input)?;

    // Step 3: Parse clustering keys (or skip to marker)
    let (input, clustering_keys) = parse_clustering_keys_section(input)?;

    // Step 4: Expect section marker
    let (input, _) = expect_section_marker(input)?;

    // Step 5: Parse regular columns
    let (input, regular_columns) = parse_regular_columns(input)?;

    // Step 6: Parse static columns (optional)
    let (input, static_columns) = parse_static_columns_optional(input)?;

    // Step 7: Build ColumnInfo list
    let columns = build_column_info_list(
        &partition_keys,
        &clustering_keys,
        &regular_columns,
        &static_columns,
    );

    Ok((input, SerializationHeader { columns }))
}
```

## References

### Cassandra Source Code

1. **SerializationHeader.java** (line 96)
   - `SerializationHeader.Serializer.deserialize()`
   - Reads partition keys → clustering keys → regular columns → static columns

2. **StatsComponent.java** (line 142)
   - Passes remaining buffer slice to SerializationHeader deserializer

3. **CompositeType.java**
   - Composite partition key encoding

4. **ReversedType.java**
   - Clustering key ordering

### CQLite Internal Docs

1. `/Users/patrick/local_projects/cqlite/docs/research/issue_163_serialization_header_format.md`
   - Binary format discovery and validation

2. `/Users/patrick/local_projects/cqlite/docs/research/issue_163_serialization_header_location.md`
   - Location and offset analysis

3. `/Users/patrick/local_projects/cqlite/docs/research/issue_163_ttl_test_hex_analysis.md`
   - Detailed hex dump walkthrough

### Test Data

1. `test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Statistics.db`
   - 18 regular columns, 1 partition key, 0 clustering keys

2. `test-data/datasets/sstables/test_basic/composite_key_table-*/nb-1-big-Statistics.db`
   - 2 regular columns, 1 partition key, 2 clustering keys

3. `test-data/datasets/sstables/test_basic/ttl_test_table-*/nb-1-big-Statistics.db`
   - 3 regular columns, 1 partition key, 0 clustering keys

## Appendix A: VInt Encoding Reference

Cassandra VInt (Variable-length Integer) encoding:

```
Value Range         | Encoding
--------------------|----------------------------------
0-127              | Single byte: 0x00-0x7F
128-255            | 0x80 <value>
256-65535          | 0x81 <high_byte> <low_byte>
...                | ... (longer multi-byte encoding)
```

**Common Patterns**:
- `0x80 0x28` = 40 (partition key type length)
- `0x12` = 18 (regular column count for simple_table)
- `0x03` = 3 (regular column count for ttl_test_table)

## Appendix B: Type Conversion Map

Marshal Type → CQL Type:

```rust
"UTF8Type"        -> "text"
"Int32Type"       -> "int"
"LongType"        -> "bigint"
"UUIDType"        -> "uuid"
"TimestampType"   -> "timestamp"
"DecimalType"     -> "decimal"
"BooleanType"     -> "boolean"
"FloatType"       -> "float"
"DoubleType"      -> "double"
"BytesType"       -> "blob"
"AsciiType"       -> "ascii"
"InetAddressType" -> "inet"
"TimeUUIDType"    -> "timeuuid"
"DurationType"    -> "duration"
"SimpleDateType"  -> "date"
"TimeType"        -> "time"

// Collections
"ListType(T)"     -> "list<T>"
"SetType(T)"      -> "set<T>"
"MapType(K,V)"    -> "map<K,V>"

// Special
"CompositeType(...)" -> Parse components
"ReversedType(T)"    -> T (with DESC order flag)
```

## Appendix C: Hex Dump Reference

### simple_table SerializationHeader (offset 0x1d2d)

```
Offset  Hex Bytes                               Decoded
0x1d2d  45 19 00 00                             EncodingStats end, VInt=25
0x1d31  (or g.a pa ch e.c as sa nd ra .d b.    Partition key type string
        ma rs ha l.U UI DT yp e
0x1d5d  00 00                                   Section marker
0x1d5f  12                                      Column count = 18
0x1d60  0f                                      Name len = 15
0x1d61  ac co un t_ ba la nc e                  "account_balance"
0x1d70  2b                                      Type separator
0x1d71  or g.a pa ch e.c as sa nd ra .d b.     Type string
        ma rs ha l.D ec im al Ty pe
...     [17 more columns]
```

### ttl_test_table SerializationHeader (offset 0x1399)

```
Offset  Hex Bytes                               Decoded
0x1399  80 28                                   VInt = 40
0x139b  (or g.a pa ch e.c as sa nd ra .d b.    Partition key type string
        ma rs ha l.U UI DT yp e
0x13c3  00 00                                   Section marker
0x13c5  03                                      Column count = 3
0x13c6  0e                                      Name len = 14
0x13c7  ex pi ri ng _v al ue                    "expiring_value"
...     [2 more columns]
```

---

**End of Specification**

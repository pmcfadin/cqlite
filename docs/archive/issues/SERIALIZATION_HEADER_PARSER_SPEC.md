# SerializationHeader Parser Implementation Specification

**Version:** 1.0
**Date:** 2025-10-29
**Based On:** Reverse engineering analysis of Cassandra 5.0 Statistics.db files

---

## Quick Reference

### Binary Format (Simplified)

```
[VInt prefix?] [0x00 0x00]                    -- marker/alignment
[u8 len] [partition_key_type_string]          -- partition key type
[u8 count]                                     -- clustering key count
  [u8 len] [clustering_type_string]            -- for each clustering key
[0x00]                                         -- separator
[u8 count]                                     -- regular column count
  [u8 name_len] [name] [u8 type_len] [type]    -- for each column
```

---

## Parser Pseudocode

```rust
pub struct SerializationHeader {
    pub partition_key_type: String,
    pub clustering_types: Vec<String>,
    pub regular_columns: Vec<ColumnDefinition>,
}

pub struct ColumnDefinition {
    pub name: String,
    pub type_string: String,
}

impl SerializationHeader {
    pub fn parse(cursor: &mut Cursor<&[u8]>) -> Result<Self> {
        // 1. Find the 0x00 0x00 marker (or parse EncodingStats to get here)
        //    For now, assume cursor is already positioned after EncodingStats

        // Skip unknown VInt prefix (bytes before 0x00 0x00)
        // This might be tail of EncodingStats - need to parse EncodingStats properly
        // OR search for 0x00 0x00 marker

        // 2. Read partition key type
        let marker1 = cursor.read_u8()?;
        let marker2 = cursor.read_u8()?;
        if marker1 != 0x00 || marker2 != 0x00 {
            return Err(ParseError::MissingMarker);
        }

        let partition_key_type = read_type_string(cursor)?;

        // 3. Read clustering types
        let clustering_count = cursor.read_u8()?;
        let mut clustering_types = Vec::with_capacity(clustering_count as usize);

        for _ in 0..clustering_count {
            clustering_types.push(read_type_string(cursor)?);
        }

        // 4. Verify separator after clustering types
        let separator = cursor.read_u8()?;
        if separator != 0x00 {
            return Err(ParseError::InvalidSeparator);
        }

        // 5. Read regular columns
        let column_count = cursor.read_u8()?;
        let mut regular_columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let name = read_string(cursor)?;
            let type_string = read_type_string(cursor)?;
            regular_columns.push(ColumnDefinition { name, type_string });
        }

        Ok(SerializationHeader {
            partition_key_type,
            clustering_types,
            regular_columns,
        })
    }
}

fn read_type_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let length = cursor.read_u8()?;  // TODO: VInt support for length >= 128
    read_string_with_length(cursor, length as usize)
}

fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let length = cursor.read_u8()?;  // TODO: VInt support for length >= 128
    read_string_with_length(cursor, length as usize)
}

fn read_string_with_length(cursor: &mut Cursor<&[u8]>, length: usize) -> Result<String> {
    let mut buffer = vec![0u8; length];
    cursor.read_exact(&mut buffer)?;
    String::from_utf8(buffer).map_err(|_| ParseError::InvalidUtf8)
}
```

---

## Key Implementation Notes

### 1. Cursor Positioning

**Challenge:** The parser needs to be positioned at the start of SerializationHeader, but it follows EncodingStats which has variable length.

**Solutions:**

**Option A:** Parse EncodingStats first, consume it completely, then parse SerializationHeader:
```rust
let encoding_stats = EncodingStats::parse(cursor)?;
let serialization_header = SerializationHeader::parse(cursor)?;
```

**Option B:** Search for `0x00 0x00` marker backwards from end of file or forwards from known position:
```rust
fn find_serialization_header(data: &[u8]) -> Option<usize> {
    // Search for 0x00 0x00 pattern
    // WARNING: May have false positives in data
    data.windows(2)
        .position(|window| window == [0x00, 0x00])
}
```

**Recommendation:** Use Option A (parse EncodingStats first) for correctness.

### 2. VInt Support

**Current Observation:** All observed length prefixes are single bytes (< 128).

**Future-Proofing:**
```rust
fn read_length_prefix(cursor: &mut Cursor<&[u8]>) -> Result<usize> {
    let first_byte = cursor.read_u8()?;

    if first_byte < 128 {
        // Simple single-byte length
        Ok(first_byte as usize)
    } else {
        // VInt decoding (if needed in future)
        decode_vint(cursor, first_byte)
    }
}
```

**VInt Decoding (Cassandra format):**
```rust
fn decode_vint(cursor: &mut Cursor<&[u8]>, first_byte: u8) -> Result<usize> {
    // Cassandra VInt: MSB indicates continuation
    // Implementation depends on Cassandra's exact VInt encoding
    // (may differ from Protocol Buffers VarInt)
    todo!("Implement Cassandra VInt decoding")
}
```

### 3. Nested Type Handling

**Example:** `ReversedType(TimestampType)` is stored as one concatenated string.

**Parser Behavior:**
```rust
// NO special handling needed - just store the full string
let clustering_type = read_type_string(cursor)?;
// clustering_type = "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
```

**Type Parsing (if needed later):**
```rust
fn parse_type_string(type_str: &str) -> CqlType {
    if type_str.contains("ReversedType(") {
        // Extract nested type
        let inner = extract_nested_type(type_str);
        CqlType::Reversed(Box::new(parse_type_string(inner)))
    } else if type_str.ends_with("UUIDType") {
        CqlType::Uuid
    }
    // ... etc
}
```

### 4. Error Handling

**Define Parse Errors:**
```rust
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Missing 0x00 0x00 marker before partition key type")]
    MissingMarker,

    #[error("Invalid separator after clustering types (expected 0x00)")]
    InvalidSeparator,

    #[error("Invalid UTF-8 in string")]
    InvalidUtf8,

    #[error("Unexpected EOF")]
    UnexpectedEof,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

### 5. Validation Against Schema

**After Parsing:**
```rust
impl SerializationHeader {
    pub fn validate(&self, expected_schema: &TableSchema) -> Result<()> {
        // Check partition key type matches
        if self.partition_key_type != expected_schema.partition_key_type {
            return Err(ValidationError::PartitionKeyMismatch);
        }

        // Check clustering key count
        if self.clustering_types.len() != expected_schema.clustering_keys.len() {
            return Err(ValidationError::ClusteringCountMismatch);
        }

        // Check each clustering type
        for (i, (parsed, expected)) in self.clustering_types.iter()
            .zip(&expected_schema.clustering_keys)
            .enumerate() {
            if parsed != &expected.type_string {
                return Err(ValidationError::ClusteringTypeMismatch { index: i });
            }
        }

        // Check regular column count and types
        // ...

        Ok(())
    }
}
```

---

## Test Cases

### Test Data Locations

```
/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/
├── composite_key_table-*/nb-1-big-Statistics.db
├── simple_table-*/nb-1-big-Statistics.db
└── ttl_test_table-*/nb-1-big-Statistics.db
```

### Test Case 1: composite_key_table

**Input:** Statistics.db at offset 0x1390-0x14b4

**Expected Output:**
```rust
SerializationHeader {
    partition_key_type: "org.apache.cassandra.db.marshal.UUIDType",
    clustering_types: vec![
        "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)",
        "org.apache.cassandra.db.marshal.UTF8Type",
    ],
    regular_columns: vec![
        ColumnDefinition {
            name: "data",
            type_string: "org.apache.cassandra.db.marshal.UTF8Type",
        },
        ColumnDefinition {
            name: "value",
            type_string: "org.apache.cassandra.db.marshal.Int32Type",
        },
    ],
}
```

**Validation:**
```bash
# Compare against sstabledump output
cat nb-1-big-Statistics.db.txt | grep -E "(KeyType|ClusteringTypes|RegularColumns)"
```

Expected:
```
KeyType: org.apache.cassandra.db.marshal.UUIDType
ClusteringTypes: [org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType), org.apache.cassandra.db.marshal.UTF8Type]
RegularColumns: data:org.apache.cassandra.db.marshal.UTF8Type, value:org.apache.cassandra.db.marshal.Int32Type
```

### Test Case 2: ttl_test_table

**Input:** Statistics.db at offset 0x1390+

**Expected Output:**
```rust
SerializationHeader {
    partition_key_type: "org.apache.cassandra.db.marshal.UUIDType",
    clustering_types: vec![],  // EMPTY - no clustering
    regular_columns: vec![
        ColumnDefinition {
            name: "expiring_value",
            type_string: "org.apache.cassandra.db.marshal.Int32Type",
        },
        ColumnDefinition {
            name: "session_info",
            type_string: "org.apache.cassandra.db.marshal.UTF8Type",
        },
        ColumnDefinition {
            name: "temporary_data",
            type_string: "org.apache.cassandra.db.marshal.UTF8Type",
        },
    ],
}
```

**Validation:**
```
KeyType: org.apache.cassandra.db.marshal.UUIDType
ClusteringTypes: []
RegularColumns: session_info:org.apache.cassandra.db.marshal.UTF8Type, temporary_data:org.apache.cassandra.db.marshal.UTF8Type, expiring_value:org.apache.cassandra.db.marshal.Int32Type
```

**Note:** Column order may differ from schema (alphabetical in parsed output).

### Test Case 3: simple_table

**Expected:** 18 regular columns with various primitive types (see schema).

---

## Integration Points

### Where SerializationHeader is Used

1. **Schema Extraction:**
   ```rust
   // Extract schema from Statistics.db without accessing system tables
   let schema = TableSchema::from_serialization_header(&header);
   ```

2. **Data.db Parsing:**
   ```rust
   // Use types to deserialize partition/clustering keys and column values
   let partition_key = deserialize_partition_key(data, &header.partition_key_type)?;
   let clustering_keys = deserialize_clustering_keys(data, &header.clustering_types)?;
   ```

3. **Validation:**
   ```rust
   // Ensure Statistics.db matches expected schema from CQL
   header.validate(&expected_schema)?;
   ```

---

## Performance Considerations

### Memory Allocation

**Optimization:** Pre-allocate vectors based on counts:
```rust
let mut clustering_types = Vec::with_capacity(clustering_count as usize);
let mut regular_columns = Vec::with_capacity(column_count as usize);
```

### String Interning

**For Repeated Type Strings:**
```rust
// Consider string interning for common types like "org.apache.cassandra.db.marshal.UTF8Type"
use string_cache::DefaultAtom;

pub struct SerializationHeader {
    pub partition_key_type: DefaultAtom,
    pub clustering_types: Vec<DefaultAtom>,
    // ...
}
```

**Trade-off:** Memory savings vs. complexity. Evaluate after profiling.

### Zero-Copy Parsing

**Current:** Allocates `String` for each type/name.

**Alternative:** Store byte slices with lifetime bounds:
```rust
pub struct SerializationHeader<'a> {
    pub partition_key_type: &'a str,
    pub clustering_types: Vec<&'a str>,
    pub regular_columns: Vec<ColumnDefinition<'a>>,
}
```

**Trade-off:** Complexity in lifetime management. Use if memory profiling shows bottleneck.

---

## Debugging Tools

### Hex Dump Utility

```rust
fn dump_serialization_header_region(stats_db: &Path) -> Result<()> {
    let data = std::fs::read(stats_db)?;

    // Find 0x00 0x00 marker
    let start = data.windows(2)
        .position(|w| w == [0x00, 0x00])
        .ok_or(ParseError::MissingMarker)?;

    // Dump next 256 bytes
    println!("SerializationHeader region (offset {:#x}):", start);
    for (i, chunk) in data[start..].chunks(16).take(16).enumerate() {
        print!("{:08x}  ", start + i * 16);
        for byte in chunk {
            print!("{:02x} ", byte);
        }
        println!();
    }

    Ok(())
}
```

### Comparison Tool

```rust
fn compare_with_sstabledump(header: &SerializationHeader, stats_txt: &Path) -> Result<()> {
    let content = std::fs::read_to_string(stats_txt)?;

    // Parse sstabledump output
    let key_type_line = content.lines()
        .find(|l| l.starts_with("KeyType:"))
        .ok_or(ParseError::MissingKeyType)?;

    let expected_key_type = key_type_line.strip_prefix("KeyType: ")
        .unwrap()
        .trim();

    assert_eq!(header.partition_key_type, expected_key_type);

    // Compare clustering types...
    // Compare columns...

    Ok(())
}
```

---

## Known Limitations

### 1. VInt Encoding

**Current:** Only single-byte lengths supported (< 128).

**Impact:** Will fail on type strings >= 128 bytes or column names >= 128 bytes.

**Likelihood:** Low (longest observed: 91 bytes for `ReversedType(...)`).

**Mitigation:** Add VInt support when first encountered in testing.

### 2. Marker Detection

**Current:** Assumes `0x00 0x00` marker uniquely identifies SerializationHeader start.

**Risk:** False positives if data contains `0x00 0x00` bytes.

**Mitigation:** Parse EncodingStats properly to calculate exact offset.

### 3. Column Order

**Observation:** Parsed column order may differ from schema definition order.

**Cause:** Cassandra may reorder columns (alphabetically? by internal ID?).

**Impact:** Column lookups must use name-based matching, not index-based.

**Mitigation:** Store columns in `HashMap<String, ColumnDefinition>` instead of `Vec`.

---

## Implementation Checklist

- [ ] Implement `SerializationHeader::parse()`
- [ ] Add support for `0x00 0x00` marker detection
- [ ] Implement single-byte length prefix reading
- [ ] Add separator validation (0x00 after clustering types)
- [ ] Implement column definition parsing
- [ ] Write unit tests for composite_key_table
- [ ] Write unit tests for ttl_test_table
- [ ] Write unit tests for simple_table
- [ ] Add validation against sstabledump output
- [ ] Add VInt support (future-proofing)
- [ ] Implement schema validation
- [ ] Add integration with Data.db parsing
- [ ] Performance profiling and optimization
- [ ] Documentation and examples

---

## References

- **Reverse Engineering Analysis:** `/Users/patrick/local_projects/cqlite/SERIALIZATION_HEADER_REVERSE_ENGINEERING.md`
- **Test Data:** `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/`
- **Schema Definitions:** `/Users/patrick/local_projects/cqlite/test-data/schemas/basic-types.cql`
- **Validation Artifacts:** `nb-1-big-Statistics.db.txt` (sstabledump output)

---

## Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-10-29 | Initial specification based on reverse engineering |

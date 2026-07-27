---
name: CQL Type System & Schema Handling
description: Implement and deserialize all CQL types including primitives (int, text, timestamp, uuid, varint, decimal), collections (list, set, map), tuples, UDTs (user-defined types), and frozen types. Use when working with CQL type deserialization, schema validation, collection parsing, UDT handling, or type-correct data generation.
allowed-tools: Read, Grep, Glob
---

# CQL Type System & Schema Handling

This skill provides guidance on implementing Cassandra CQL type system with schema-provided deserialization.

## When to Use This Skill

- Implementing CQL type deserializers
- Parsing collection types (list, set, map)
- Handling User-Defined Types (UDTs)
- Working with frozen vs non-frozen types
- Tuple deserialization
- Schema validation
- Type-correct data generation

## Core Principles

### Schema-Provided Deserialization
Per PRD: **schema passed in, not inferred**

```rust
// Schema provides type information
fn deserialize_cell(
    data: &[u8],
    column_type: &CqlType,  // From schema
) -> Result<CqlValue>
```

Never try to infer type from data alone - always use schema.

## CQL Type Categories

### 1. Primitive Types

#### Fixed-Size Primitives
- `boolean` - 1 byte (0x00 or 0x01)
- `tinyint` - 1 byte signed
- `smallint` - 2 bytes signed, big-endian
- `int` - 4 bytes signed, big-endian
- `bigint` - 8 bytes signed, big-endian
- `float` - 4 bytes IEEE 754
- `double` - 8 bytes IEEE 754
- `date` - 4 bytes BE unsigned, days since epoch **biased by `Integer.MIN_VALUE`**
  (epoch day 0 is stored as `0x80000000`) — un-bias with
  `stored.wrapping_add(i32::MIN as u32) as i32`; a plain `u32` read is wrong by 2^31 days.
  Citations: `row_decoder/cell_value_scalar.rs:329-333`; cassandra-5.0.8
  `serializers/SimpleDateSerializer.java:36-37,110,113-115`.
- `time` - 8 bytes signed BE (nanoseconds since midnight; no bias)

#### Variable-Size Primitives
- `text`/`varchar` - UTF-8 encoded string
- `blob` - raw bytes
- `ascii` - ASCII-only string

#### Special Primitives
- `uuid`/`timeuuid` - 16 bytes
- `inet` - 4 bytes (IPv4) or 16 bytes (IPv6)
- `varint` - variable-length big integer
- `decimal` - scale (4 bytes) + unscaled varint
- `duration` - months, days, nanoseconds as 3 **ZigZag-SIGNED** VInts — the ONLY signed
  VInts in `Data.db`, wherever a duration payload occurs (including nested in a
  collection/tuple/UDT). Decoding them unsigned makes every negative duration wrong.
  Citations: `appendix-b-encodings-cheat-sheet.md:63-65,92-97`;
  `DurationSerializer.java:49-51`.
- `timestamp` - 8 bytes (milliseconds since Unix epoch)

### 2. Collection Types

See [collections-and-udts.md](collections-and-udts.md) for detailed format.

> ### ⚠️ There are TWO collection encodings — pick by frozen-ness, never by byte pattern
> Frozen and non-frozen collections do NOT share a wire format. Assuming the frozen layout
> for a non-frozen column (or vice versa) mis-frames every element.

**FROZEN collection** (`frozen<list<int>>`, and any collection nested inside another
collection/tuple/UDT) — one opaque cell, **fixed 4-byte big-endian `i32`** count and
element lengths (NOT VInts):
```
[i32 BE: element_count]
[for each element:]
    [i32 BE: element_len]   // -1 = null
    [bytes: element_data]
```
> **Citation**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/frozen.rs:21`
> (count), `:98,279,370` (element/key lengths). Guide:
> `appendix-b-encodings-cheat-sheet.md:537-539`. Cassandra:
> `CollectionSerializer.java:67-92`, `TupleType.java:341-364`.

**NON-FROZEN collection** (the default `list<int>`, `set<T>`, `map<K,V>`) — a *set of
cells*, framed with **unsigned VInts**:
```
[if the row sets ROW_HAS_COMPLEX_DELETION (0x40): complex deletion, 2 unsigned VInt deltas]
[unsigned VInt: cell_count]
[for each cell:]
    [1 byte: cell flags]
    [conditional timestamp / local_deletion_time / ttl unsigned-VInt deltas]
    [unsigned VInt: path_len][path bytes]     // element key / path
    [unsigned VInt: value_len][value bytes]   // omitted when IS_DELETED or HAS_EMPTY_VALUE
```
> **Citation**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs:332`
> (`cell_count` via `parse_vuint`), `:1089` (path length), `:1136` (value length),
> `:279-300` (complex deletion). Guide: `appendix-b-encodings-cheat-sheet.md:530-536` — a
> non-frozen collection cell length-prefixes BOTH path and value with an unsigned VInt,
> **fixed-width element types included** (the `valueLengthIfFixed()` raw-bytes shortcut is
> a simple-cell rule only, `AbstractType.java:538-543`).

**Types:**
- `list<T>` - Ordered, allows duplicates
- `set<T>` - Unordered, no duplicates
- `map<K,V>` - Key-value pairs

### 3. Tuple Types

**Format:**
```
[element_1_data]
[element_2_data]
...
```

No size prefix - elements serialized back-to-back.
Each element uses its type's serialization.

### 4. User-Defined Types (UDTs)

**Format:**
```
[for each field in schema order:]
    [4 bytes: field_size (-1 for null, 0 for empty, >0 for data)]
    [if size > 0:]
        [bytes: field_data]
```

UDT schema defines field names and types.

### 5. Frozen vs Non-Frozen

**Frozen types:**
- Serialized as single blob
- Cannot update individual elements
- Used in primary keys
- Nested collections must be frozen

**Non-frozen collections:**
- Can update individual elements
- Only allowed at top level (not nested)
- Uses tombstones for deletions

## Type Deserialization Patterns

### Zero-Copy Pattern
```rust
use bytes::Bytes;

fn deserialize_text(data: Bytes) -> Result<String> {
    // Zero-copy: validate UTF-8 then wrap
    let s = std::str::from_utf8(&data)?;
    Ok(s.to_string())  // Only copy if needed
}

fn deserialize_blob(data: Bytes) -> Result<Bytes> {
    // Zero-copy: just return the slice
    Ok(data)
}
```

### Length-Prefixed Pattern
```rust
fn deserialize_length_prefixed(data: &[u8]) -> Result<(Bytes, &[u8])> {
    if data.len() < 4 {
        return Err(Error::NotEnoughBytes);
    }
    
    let size = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    
    if size < 0 {
        return Ok((Bytes::new(), &data[4..]));  // Null
    }
    
    let size = size as usize;
    if data.len() < 4 + size {
        return Err(Error::NotEnoughBytes);
    }
    
    let value = Bytes::copy_from_slice(&data[4..4 + size]);
    let remaining = &data[4 + size..];
    
    Ok((value, remaining))
}
```

### Collection Pattern
```rust
fn deserialize_list(
    data: &[u8],
    element_type: &CqlType,
) -> Result<Vec<CqlValue>> {
    let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut offset = 4;
    let mut elements = Vec::with_capacity(count);
    
    for _ in 0..count {
        let (element_data, remaining) = deserialize_length_prefixed(&data[offset..])?;
        let element = deserialize_value(&element_data, element_type)?;
        elements.push(element);
        offset = data.len() - remaining.len();
    }
    
    Ok(elements)
}
```

## Schema Handling

### Schema Sources
1. **Statistics.db**: Serialization header with column definitions
2. **System tables**: `system_schema.tables`, `system_schema.columns`
3. **CQL schema file**: For test data generation

### Schema Representation
```rust
struct TableSchema {
    keyspace: String,
    table: String,
    partition_keys: Vec<ColumnDef>,
    clustering_keys: Vec<ColumnDef>,
    regular_columns: Vec<ColumnDef>,
    static_columns: Vec<ColumnDef>,
}

struct ColumnDef {
    name: String,
    cql_type: CqlType,
}

enum CqlType {
    // Primitives
    Boolean,
    Int,
    BigInt,
    Text,
    Uuid,
    Timestamp,
    // ... more primitives
    
    // Collections
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
    
    // Complex
    Tuple(Vec<CqlType>),
    Udt(UdtDef),
    
    // Modifiers
    Frozen(Box<CqlType>),
}
```

## PRD Alignment

**Supports Milestone M1** (Core Reading Library):
- All CQL types including collections & UDTs
- Schema-provided deserialization (not inferred)
- Zero-copy patterns where possible

**Supports Milestone M5** (Write Support):
- Type-correct serialization
- Schema validation

## Common Pitfalls

### 1. Inferring Types
❌ **Wrong:** Look at data to guess type
✅ **Right:** Use schema to know type

### 2. Copying Unnecessarily
❌ **Wrong:** `Vec<u8>` for every field
✅ **Right:** `Bytes` with zero-copy slicing

### 3. Ignoring Null Handling
❌ **Wrong:** Assume all fields present
✅ **Right:** Check for null (-1 size prefix)

### 4. Frozen Semantics
❌ **Wrong:** Try to update frozen collection elements
✅ **Right:** Replace entire frozen value

### 5. Nested Collections
❌ **Wrong:** Allow non-frozen nested collections
✅ **Right:** Nested collections must be frozen

## Type System References

Detailed specifications in:
- [cql-types-reference.md](cql-types-reference.md) - Complete type catalog
- [collections-and-udts.md](collections-and-udts.md) - Collection and UDT formats

## Testing

Generate type-correct test data:
```bash
# Use test-data-management skill for Docker-based generation
cd test-data
./scripts/start-clean.sh
./scripts/generate.sh
```

Validate parsing against sstabledump:
```bash
sstabledump test-data/datasets/sstables/keyspace/table/*.db
```

## Next Steps

When adding new type support:
1. Add to `CqlType` enum
2. Implement deserializer with zero-copy where possible
3. Add serializer (for M5 write support)
4. Create property tests with edge cases
5. Generate test data with type
6. Validate against sstabledump


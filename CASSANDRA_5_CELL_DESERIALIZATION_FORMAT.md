# Cassandra 5.0 Cell Value Deserialization Format

**Date**: 2025-10-16
**Purpose**: Document precise byte-level format for `Cell.Serializer.deserialize()` to fix V5CompressedLegacy parser
**Sources**: Apache Cassandra Java source code (cassandra-5.0 branch)

---

## 1. Cell Flag Constants

From `Cell.Serializer` class (Cell.java lines 263-267):

| Flag Name              | Hex Value | Bit Position | Meaning |
|------------------------|-----------|--------------|---------|
| `IS_DELETED_MASK`      | `0x01`    | Bit 0        | Cell is a tombstone (deleted) |
| `IS_EXPIRING_MASK`     | `0x02`    | Bit 1        | Cell has TTL/expiration |
| `HAS_EMPTY_VALUE_MASK` | `0x04`    | Bit 2        | Cell value is empty (no bytes to read) |
| `USE_ROW_TIMESTAMP_MASK` | `0x08`  | Bit 3        | Use row-level timestamp (don't read cell timestamp) |
| `USE_ROW_TTL_MASK`     | `0x10`    | Bit 4        | Use row-level TTL (don't read cell TTL) |

**Source Code:**
```java
// From org/apache/cassandra/db/rows/Cell.java
private final static int IS_DELETED_MASK = 0x01;
private final static int IS_EXPIRING_MASK = 0x02;
private final static int HAS_EMPTY_VALUE_MASK = 0x04;
private final static int USE_ROW_TIMESTAMP_MASK = 0x08;
private final static int USE_ROW_TTL_MASK = 0x10;
```

---

## 2. Cell Deserialization Algorithm

### Byte-Level Deserialization Flow

Based on `Cell.Serializer.deserialize()` (Cell.java lines 308-350):

```rust
fn deserialize_cell(
    flags: u8,
    data: &[u8],
    row_liveness: &LivenessInfo,
    column: &ColumnMetadata,
    header: &SerializationHeader,
) -> Result<Cell> {
    let mut offset = 0;

    // Step 1: Decode flags
    let has_value = (flags & HAS_EMPTY_VALUE_MASK) == 0;  // ✅ 0x04 NOT set = has value
    let is_deleted = (flags & IS_DELETED_MASK) != 0;      // 0x01 set = tombstone
    let is_expiring = (flags & IS_EXPIRING_MASK) != 0;    // 0x02 set = has TTL
    let use_row_timestamp = (flags & USE_ROW_TIMESTAMP_MASK) != 0;  // 0x08 set
    let use_row_ttl = (flags & USE_ROW_TTL_MASK) != 0;    // 0x10 set

    // Step 2: Read timestamp (conditional)
    let timestamp = if use_row_timestamp {
        row_liveness.timestamp()  // Use row timestamp
    } else {
        // Read VInt delta from stats.minTimestamp
        let delta = read_unsigned_vint(&data[offset..])?;
        offset += vint_size(delta);
        header.stats.minTimestamp + delta
    };

    // Step 3: Read localDeletionTime (conditional)
    let local_deletion_time = if use_row_ttl {
        row_liveness.localExpirationTime()  // Use row TTL
    } else if is_deleted || is_expiring {
        // Read VInt delta from stats.minLocalDeletionTime
        let delta = read_unsigned_vint32(&data[offset..])?;
        offset += vint_size(delta);
        header.stats.minLocalDeletionTime + delta
    } else {
        NO_DELETION_TIME  // -2^31 (0x80000000)
    };

    // Step 4: Read TTL (conditional)
    let ttl = if use_row_ttl {
        row_liveness.ttl()  // Use row TTL
    } else if is_expiring {
        // Read VInt delta from stats.minTTL
        let delta = read_unsigned_vint32(&data[offset..])?;
        offset += vint_size(delta);
        header.stats.minTTL + delta
    } else {
        NO_TTL  // 0
    };

    // Step 5: Read cell path (if complex column)
    let path = if column.is_complex() {
        // Read cell path (collection key, UDT field, etc.)
        let path_size = read_unsigned_vint32(&data[offset..])?;
        offset += vint_size(path_size);
        let path_bytes = &data[offset..offset + path_size];
        offset += path_size;
        Some(CellPath::new(path_bytes))
    } else {
        None
    };

    // Step 6: Read value (conditional)
    let value = if has_value {
        // Read value using AbstractType.read()
        // This reads: VInt length + actual bytes
        let value_length = read_unsigned_vint32(&data[offset..])?;
        offset += vint_size(value_length);

        if value_length > 0 {
            let value_bytes = &data[offset..offset + value_length];
            offset += value_length;
            Some(value_bytes.to_vec())
        } else {
            Some(Vec::new())  // Empty value (0 bytes)
        }
    } else {
        // HAS_EMPTY_VALUE_MASK set = no value to read
        None
    };

    Ok(Cell {
        timestamp,
        ttl,
        local_deletion_time,
        path,
        value,
    })
}
```

---

## 3. Critical Implementation Details

### 3.1 VInt Encoding Format

From `VIntCoding.java`:

**Cassandra uses a custom VInt format** (NOT standard LEB128):

- **Single byte (0-127)**: Value stored directly in one byte
- **Multi-byte**: First byte's leading set bits indicate continuation length
  - Number of extra bytes = `Integer.numberOfLeadingZeros(~firstByte) - 24`
  - Value mask for first byte = `0xff >> extraBytesToRead`

**Examples:**
```
Value 5:    0x05 (single byte)
Value 128:  0x81 0x80 (two bytes)
Value 255:  0x81 0xFF (two bytes)
```

### 3.2 SerializationHeader Delta Encoding

From `SerializationHeader.java`:

All timestamps, TTLs, and deletion times are stored as **delta-encoded VInts**:

```java
// Reading timestamp
public long readTimestamp(DataInputPlus in) throws IOException {
    return in.readUnsignedVInt() + stats.minTimestamp;
}

// Reading local deletion time
public long readLocalDeletionTime(DataInputPlus in) throws IOException {
    return in.readUnsignedVInt32() + stats.minLocalDeletionTime;
}

// Reading TTL
public int readTTL(DataInputPlus in) throws IOException {
    return in.readUnsignedVInt32() + stats.minTTL;
}
```

**Implication**: You MUST have the `SerializationHeader.stats` (min values) to decode these fields correctly.

### 3.3 Value Length Encoding

From `AbstractType.java`:

Cell values are encoded with **VInt-prefixed length**:

```java
public <V> V read(ValueAccessor<V> accessor, DataInputPlus in, int maxValueSize)
    throws IOException {
    int length = in.readUnsignedVInt32();  // ✅ VInt length prefix

    if (length < 0)
        throw new IOException("Corrupt (negative) value length encountered");

    if (length > maxValueSize)
        throw new IOException("Value too large: " + length);

    return accessor.read(in, length);  // Read exactly 'length' bytes
}
```

**Key Point**: Even for fixed-size types (int, boolean, etc.), Cassandra 5.0 uses VInt length prefix when stored in cells.

---

## 4. Edge Cases and Semantics

### 4.1 `HAS_EMPTY_VALUE_MASK` (0x04) Behavior

**CRITICAL FINDING**: The flag name is **misleading**!

```java
boolean hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0;  // ✅ INVERTED LOGIC!
```

- **Flag NOT set (0x04 absent)**: Cell HAS a value → read VInt length + bytes
- **Flag SET (0x04 present)**: Cell has NO value → skip value reading entirely

**Example:**
```
Flags = 0x00 → hasValue = true  → Read value bytes
Flags = 0x04 → hasValue = false → Skip value reading (empty cell)
Flags = 0x08 → hasValue = true  → Read value bytes (uses row timestamp)
```

### 4.2 Deleted Cells

When `IS_DELETED_MASK` (0x01) is set:

1. Cell is a tombstone
2. `localDeletionTime` is read (VInt delta)
3. Value is NOT read (even if `HAS_EMPTY_VALUE_MASK` not set)
4. Result: `Value::Null` with deletion metadata

### 4.3 Expiring Cells

When `IS_EXPIRING_MASK` (0x02) is set:

1. Cell has TTL and will expire
2. Both `ttl` and `localDeletionTime` must be read
3. Value IS read normally (unless also deleted)

### 4.4 Row-Level Timestamp/TTL Sharing

**Optimization**: Cells can share row-level metadata:

- `USE_ROW_TIMESTAMP_MASK` (0x08): Skip reading cell timestamp, use row timestamp
- `USE_ROW_TTL_MASK` (0x10): Skip reading cell TTL, use row TTL

**Requires**: Access to row-level `LivenessInfo` structure.

---

## 5. Hex Example Walkthrough

### Example 1: Simple Text Cell with Value

**Hex bytes**: `08 05 61 73 63 69 69`

**Decoding:**
```
Offset 0: 0x08 = flags
  - 0x08 = USE_ROW_TIMESTAMP_MASK set
  - has_value = (0x08 & 0x04) == 0 → TRUE ✅
  - is_deleted = (0x08 & 0x01) != 0 → FALSE
  - is_expiring = (0x08 & 0x02) != 0 → FALSE
  - use_row_timestamp = (0x08 & 0x08) != 0 → TRUE
  - use_row_ttl = (0x08 & 0x10) != 0 → FALSE

Offset 1: Skip timestamp (using row timestamp)

Offset 1: Skip localDeletionTime (not deleted/expiring)

Offset 1: Skip TTL (not expiring)

Offset 1: 0x05 = VInt value length = 5 bytes

Offset 2-6: 0x61 0x73 0x63 0x69 0x69 = "ascii"

Result: Text("ascii") with row timestamp
```

### Example 2: Empty Cell

**Hex bytes**: `04`

**Decoding:**
```
Offset 0: 0x04 = flags
  - 0x04 = HAS_EMPTY_VALUE_MASK set
  - has_value = (0x04 & 0x04) == 0 → FALSE ❌
  - (other flags not set)

Result: Empty cell, no value bytes read
```

### Example 3: Deleted Cell

**Hex bytes**: `01 2A 10`

**Decoding:**
```
Offset 0: 0x01 = flags
  - 0x01 = IS_DELETED_MASK set
  - is_deleted = TRUE

Offset 1: 0x2A = VInt timestamp delta = 42
  timestamp = stats.minTimestamp + 42

Offset 2: 0x10 = VInt localDeletionTime delta = 16
  localDeletionTime = stats.minLocalDeletionTime + 16

Result: Tombstone (deleted cell) with deletion metadata
```

### Example 4: Expiring Cell with TTL

**Hex bytes**: `02 15 08 14 05 68 65 6C 6C 6F`

**Decoding:**
```
Offset 0: 0x02 = flags
  - 0x02 = IS_EXPIRING_MASK set
  - has_value = TRUE (0x04 not set)
  - is_expiring = TRUE

Offset 1: 0x15 = VInt timestamp delta = 21
  timestamp = stats.minTimestamp + 21

Offset 2: 0x08 = VInt localDeletionTime delta = 8
  localDeletionTime = stats.minLocalDeletionTime + 8

Offset 3: 0x14 = VInt TTL delta = 20
  ttl = stats.minTTL + 20

Offset 4: 0x05 = VInt value length = 5

Offset 5-9: 0x68 0x65 0x6C 0x6C 0x6F = "hello"

Result: Text("hello") with TTL = 20 seconds
```

---

## 6. Rust Implementation Checklist

### Flag Interpretation
- [ ] Define all 5 flag constants correctly
- [ ] Implement **inverted logic** for `HAS_EMPTY_VALUE_MASK` (0x04 NOT set = has value)
- [ ] Handle flag combinations (deleted + expiring, etc.)

### Conditional Field Reading
- [ ] Read timestamp only if `USE_ROW_TIMESTAMP_MASK` not set
- [ ] Read localDeletionTime only if deleted OR expiring (and not using row TTL)
- [ ] Read TTL only if expiring (and not using row TTL)
- [ ] Read value only if `has_value == true` (0x04 NOT set)

### VInt Decoding
- [ ] Implement Cassandra VInt format (NOT LEB128)
- [ ] Handle single-byte values (0-127) efficiently
- [ ] Decode multi-byte values with leading-zero-based continuation

### Delta Decoding
- [ ] Access `SerializationHeader.stats` for min values
- [ ] Add delta to min value for timestamp, TTL, localDeletionTime
- [ ] Handle missing header (fallback to raw values?)

### Value Parsing
- [ ] Read VInt length prefix for value
- [ ] Read exactly `length` bytes for value data
- [ ] Handle zero-length values (empty but present)
- [ ] Handle missing values (HAS_EMPTY_VALUE_MASK set)

### Type-Specific Decoding
- [ ] Text types: UTF-8 decode value bytes
- [ ] Integer types: Big-endian decode
- [ ] Boolean: Single byte (0 or 1)
- [ ] UUID: 16 bytes
- [ ] Collections: Nested cell path + value

---

## 7. Key Differences from Current Implementation

### Current V5CompressedLegacy Parser Issues

From `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`:

**Problem 1**: Missing `HAS_EMPTY_VALUE_MASK` handling
```rust
// Current code (line ~400-428) doesn't check HAS_EMPTY_VALUE_MASK before parsing value
// It always tries to read value bytes, even when flag 0x04 is set
```

**Problem 2**: Incorrect flag interpretation
```rust
// Line 467-481: Text parsing always reads VInt length
// Should SKIP value reading if (flags & 0x04) != 0
if flags & 0x04 != 0 {
    return Ok((Value::Text(String::new()), offset));  // Empty, no bytes to read
}
```

**Problem 3**: Missing delta decoding
```rust
// Lines 401-414: Reads VInt for timestamp/TTL but doesn't add to minTimestamp/minTTL
// Should be:
let timestamp = read_vint(&data[offset..])? + header.stats.minTimestamp;
```

### Recommended Fix

```rust
const CELL_IS_DELETED: u8 = 0x01;
const CELL_IS_EXPIRING: u8 = 0x02;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;  // ✅ Flag SET = NO value
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
const CELL_USE_ROW_TTL: u8 = 0x10;

fn parse_cell_with_flags(
    &self,
    data: &[u8],
    mut offset: usize,
    column: &Column,
    header: &SerializationHeader,
) -> Result<(Value, usize)> {
    let flags = data[offset];
    offset += 1;

    // ✅ CRITICAL: Inverted logic for HAS_EMPTY_VALUE_MASK
    let has_value = (flags & CELL_HAS_EMPTY_VALUE) == 0;
    let is_deleted = (flags & CELL_IS_DELETED) != 0;
    let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
    let use_row_timestamp = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
    let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

    // Read timestamp (conditional)
    if !use_row_timestamp {
        let (remaining, delta) = parse_vint(&data[offset..])?;
        let _timestamp = header.stats.minTimestamp + delta;  // ✅ Add delta
        offset = data.len() - remaining.len();
    }

    // Read localDeletionTime (conditional)
    if !use_row_ttl && (is_deleted || is_expiring) {
        let (remaining, delta) = parse_vint(&data[offset..])?;
        let _local_deletion = header.stats.minLocalDeletionTime + delta;  // ✅ Add delta
        offset = data.len() - remaining.len();
    }

    // Read TTL (conditional)
    if !use_row_ttl && is_expiring {
        let (remaining, delta) = parse_vint(&data[offset..])?;
        let _ttl = header.stats.minTTL + delta;  // ✅ Add delta
        offset = data.len() - remaining.len();
    }

    // Return early for deleted cells
    if is_deleted {
        return Ok((Value::Null, offset));
    }

    // ✅ CRITICAL: Only read value if has_value is true
    if !has_value {
        return Ok((Value::Text(String::new()), offset));  // Empty cell
    }

    // Read value with VInt length prefix
    self.parse_value_bytes(data, offset, column)
}
```

---

## 8. Summary of Critical Findings

### 1. Flag 0x08 Does NOT Mean Empty Value

**The confusion in Issue #160**:
- Hex `08 05 61 73 63 69 69` was interpreted as "empty value" because flag = 0x08
- **CORRECT interpretation**: 0x08 = `USE_ROW_TIMESTAMP_MASK`, NOT empty value
- `HAS_EMPTY_VALUE_MASK` is 0x04, and it's **NOT set**, so cell HAS value
- Result should be: `Text("ascii")` with row timestamp

### 2. Inverted Logic for HAS_EMPTY_VALUE_MASK

```rust
// ❌ WRONG
if flags & 0x04 != 0 {
    // Has value
}

// ✅ CORRECT
if flags & 0x04 == 0 {
    // Has value
}
```

### 3. Delta Encoding is Mandatory

All timestamp/TTL/deletion values are deltas from `SerializationHeader.stats` min values.
**Cannot decode correctly without the header.**

### 4. Value Length is Always VInt-Prefixed

Even for fixed-size types (int, boolean), Cassandra 5.0 cells use VInt length prefix.

---

## 9. Testing Strategy

### Unit Tests to Create

```rust
#[test]
fn test_cell_flag_0x08_with_value() {
    // Hex: 08 05 61 73 63 69 69
    let data = vec![0x08, 0x05, 0x61, 0x73, 0x63, 0x69, 0x69];
    let result = parse_cell(&data, column_ascii);
    assert_eq!(result, Value::Text("ascii"));
}

#[test]
fn test_cell_flag_0x04_empty() {
    // Hex: 04
    let data = vec![0x04];
    let result = parse_cell(&data, column_ascii);
    assert_eq!(result, Value::Text(""));  // Empty, no bytes read
}

#[test]
fn test_cell_flag_0x01_deleted() {
    // Hex: 01 2A 10
    let data = vec![0x01, 0x2A, 0x10];
    let result = parse_cell(&data, column_ascii);
    assert_eq!(result, Value::Null);  // Tombstone
}

#[test]
fn test_cell_flag_0x02_expiring() {
    // Hex: 02 15 08 14 05 68 65 6C 6C 6F
    let data = vec![0x02, 0x15, 0x08, 0x14, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F];
    let result = parse_cell(&data, column_ascii);
    assert_eq!(result, Value::Text("hello"));  // With TTL
}
```

---

## 10. References

### Java Source Files (cassandra-5.0 branch)

1. **Cell.java** (`org/apache/cassandra/db/rows/Cell.java`)
   - Lines 263-267: Flag constants
   - Lines 308-350: `Cell.Serializer.deserialize()` method

2. **SerializationHeader.java** (`org/apache/cassandra/db/SerializationHeader.java`)
   - `readTimestamp()`, `readTTL()`, `readLocalDeletionTime()` methods
   - Delta encoding with `EncodingStats.min*` values

3. **AbstractType.java** (`org/apache/cassandra/db/marshal/AbstractType.java`)
   - `read()` method: VInt length prefix + value bytes

4. **VIntCoding.java** (`org/apache/cassandra/utils/vint/VIntCoding.java`)
   - Custom VInt encoding format (not LEB128)

### Key Cassandra Concepts

- **LivenessInfo**: Row-level timestamp/TTL metadata
- **EncodingStats**: Min values for delta encoding (in SerializationHeader)
- **CellPath**: Identifies cells within complex columns (collections, UDTs)
- **NO_DELETION_TIME**: -2^31 (0x80000000) constant for non-deleted cells
- **NO_TTL**: 0 constant for non-expiring cells

---

**End of Document**

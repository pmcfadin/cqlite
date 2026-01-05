# BTI Partition Index Payload Format Research

**Date**: 2026-01-05
**Status**: Complete - Ready for Implementation
**Related Issue**: Direct partition lookup optimization

## Executive Summary

BTI (Block-based Trie Index) partition indexes in Cassandra 5.0 use a variable-length encoding for storing partition offsets. The 8-byte metadata pattern observed in Index.db files is **NOT** a fixed format, but rather a trie node payload with:

1. **Hash byte** (1 byte): Lower 8 bits of partition key filter hash
2. **Position** (1-7 bytes): Data.db file offset using SizedInts encoding

The actual payload size is determined by the `payloadBits` field in the trie node header.

## Format Specification

### Trie Node Header

Each BTI trie node starts with a 1-byte header:

```
Byte 0: [node_type: 4 bits][payload_flags: 4 bits]
        ^^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^^^^^^
        Upper nibble        Lower nibble
```

- **Node type** (bits 7-4): 0=PayloadOnly, 1=Single, 2=Sparse, 3=Dense
- **Payload flags** (bits 3-0): Contains `payloadBits` when node has payload

### Payload Structure

When a node has a payload (`payloadBits > 0`), it's stored after the node data:

```
[hash_byte: 1 byte][position: N bytes]
```

Where:
- `hash_byte` = `key.filterHashLowerBits()` (lower 8 bits of partition key hash)
- `position` = Data.db file offset (or ~offset for direct-to-data, Row index offset for indexed partitions)
- `N` = payload size in bytes

### Determining Payload Size

From `PartitionIndex.java` line 130-136:

```java
int size = SizedInts.nonZeroSize(payload.position);
int payloadBits = FLAG_HAS_HASH_BYTE + (size - 1);
// FLAG_HAS_HASH_BYTE = 8
```

Therefore:
- `payloadBits = 8` → size = 1 byte position
- `payloadBits = 9` → size = 2 bytes position
- `payloadBits = 10` → size = 3 bytes position
- `payloadBits = 11` → size = 4 bytes position
- `payloadBits = 12` → size = 5 bytes position
- `payloadBits = 13` → size = 6 bytes position
- `payloadBits = 14` → size = 7 bytes position
- `payloadBits = 15` → size = 8 bytes position

To decode:
```
size = payloadBits - FLAG_HAS_HASH_BYTE + 1
     = payloadBits - 8 + 1
     = payloadBits - 7
```

### SizedInts Encoding

`SizedInts.java` implements variable-length big-endian integer encoding:

```java
public static int nonZeroSize(long value) {
    if (value < 0)
        value = ~value;
    int lz = Long.numberOfLeadingZeros(value);
    return (64 - lz + 1 + 7) / 8;  // At least 1, at most 8
}
```

This determines the minimum number of bytes needed to store a value (1-8 bytes).

Reading:
```java
public static long read(ByteBuffer src, int startPos, int bytes) {
    switch (bytes) {
        case 1: return src.get(startPos);
        case 2: return src.getShort(startPos);
        case 3: return (src.get(startPos) << 16L) | (src.getShort(startPos + 1) & 0xFFFFL);
        case 4: return src.getInt(startPos);
        // ... up to 8 bytes
    }
}
```

All encodings are **big-endian**.

## Example: Decoding Real Hex Data

Given the hex pattern from Index.db:
```
00 0e 00 04 41 4d 5a 4e  00 00 04 80 00 4f 88 00
^---^ ^---^ ^---------^  ^-----------------------^
len   klen  "AMZN"       payload (8 bytes total)
```

Assuming we read the trie node and found `payloadBits = 11`:

```python
payloadBits = 11
size = payloadBits - 7 = 4 bytes

payload_data = [0x00, 0x00, 0x04, 0x80, 0x00, 0x4f, 0x88, 0x00]

hash_byte = payload_data[0] = 0x00
position_bytes = payload_data[1:1+4] = [0x00, 0x04, 0x80, 0x00]

# Big-endian 4-byte read
position = (0x00 << 24) | (0x04 << 16) | (0x80 << 8) | 0x00
         = 0x00048000
         = 294,912 bytes
         = ~295 KB
```

This is the **Data.db offset** where the partition data for "AMZN" begins!

## Position Sign Encoding

From `PartitionIndex.java` comments:

```java
/**
 * To avoid having to create an object to carry the result, the two are
 * distinguished by sign. Direct-to-dfile entries are recorded as ~position
 * (~ instead of - to differentiate 0 in ifile from 0 in dfile).
 */
```

- **Positive position**: Points to row index file (Rows.db) entry
- **Negative position (`~pos`)**: Points directly to Data.db offset (no row index)

Small partitions that don't need row-level indexing use the direct-to-data encoding.

## Implementation Requirements

### 1. Read Node Header to Get payloadBits

```rust
// Read 1-byte node header
let node_header = reader.read_u8()?;
let node_type = (node_header >> 4) & 0x0F;
let payload_bits = node_header & 0x0F;

if payload_bits < 8 {
    // No hash byte, different encoding (not for partition index)
    return Err(Error::Parse("Expected hash byte in payload"));
}
```

### 2. Read Payload Data

```rust
let size = payload_bits - 7;  // Convert payloadBits to byte count
let hash_byte = reader.read_u8()?;

// Read position using SizedInts encoding
let position = read_sized_int(&mut reader, size as usize)?;

// Check if direct-to-data or row-indexed
let (is_direct, data_offset) = if position < 0 {
    (true, !position as u64)  // Bitwise NOT
} else {
    (false, position as u64)  // Row index offset
};
```

### 3. SizedInts Reader

```rust
fn read_sized_int<R: Read>(reader: &mut R, bytes: usize) -> Result<i64, Error> {
    match bytes {
        1 => Ok(reader.read_i8()? as i64),
        2 => Ok(reader.read_i16::<BigEndian>()? as i64),
        3 => {
            let high = reader.read_i8()? as i64;
            let low = reader.read_u16::<BigEndian>()? as i64;
            Ok((high << 16) | low)
        },
        4 => Ok(reader.read_i32::<BigEndian>()? as i64),
        5 => {
            let high = reader.read_i8()? as i64;
            let low = reader.read_u32::<BigEndian>()? as i64;
            Ok((high << 32) | low)
        },
        6 => {
            let high = reader.read_i16::<BigEndian>()? as i64;
            let low = reader.read_u32::<BigEndian>()? as i64;
            Ok((high << 32) | low)
        },
        7 => {
            let high1 = reader.read_i8()? as i64;
            let high2 = reader.read_u16::<BigEndian>()? as i64;
            let low = reader.read_u32::<BigEndian>()? as i64;
            Ok((high1 << 48) | (high2 << 32) | low)
        },
        8 => Ok(reader.read_i64::<BigEndian>()?),
        _ => Err(Error::Parse(format!("Invalid SizedInts byte count: {}", bytes))),
    }
}
```

## Cassandra Source References

Key files in `~/local_projects/cassandra`:

1. **`src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndex.java`**
   - Line 79: `FLAG_HAS_HASH_BYTE = 8`
   - Line 250-260: `getIndexPos()` - Decodes payload
   - Line 110-141: `PartitionIndexSerializer` - Encodes payload

2. **`src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java`**
   - Line 34-45: Index entry structure
   - Line 90-97: Serialization format

3. **`src/java/org/apache/cassandra/io/util/SizedInts.java`**
   - Line 36-42: `nonZeroSize()` - Calculate byte count
   - Line 54-92: `read()` - Decode variable-length int
   - Line 102-105: `write()` - Encode variable-length int

4. **`src/java/org/apache/cassandra/io/tries/TrieNode.java`**
   - Line 13-23: Node header format
   - Line 27-30: `payloadFlags()` extraction

5. **`src/java/org/apache/cassandra/io/tries/Walker.java`**
   - Line 89-92: `payloadFlags()` usage
   - Line 98-101: `payloadPosition()` calculation

## Testing Strategy

1. **Unit tests**: Verify SizedInts encoding/decoding for all byte sizes (1-8)
2. **Integration tests**: Parse real BTI Partitions.db files from test datasets
3. **Validation**: Compare extracted offsets against sstabledump output
4. **Edge cases**:
   - Position = 0 (start of file)
   - Large positions (> 4GB, requiring 6+ bytes)
   - Negative positions (direct-to-data via `~pos`)

## Performance Implications

### Benefits of Direct Offset Extraction

Current approach (sequential scan):
```
For each partition lookup:
  1. Scan Data.db from start
  2. Parse partition headers until key match
  3. Read partition data
```

With BTI offset extraction:
```
For each partition lookup:
  1. Navigate BTI trie to find key (O(key length))
  2. Extract Data.db offset from payload
  3. Seek directly to offset in Data.db
  4. Read partition data
```

**Expected improvement**:
- Sequential scan: O(n) where n = file size
- BTI direct lookup: O(log n) trie navigation + O(1) seek

For a 1GB SSTable with 10,000 partitions:
- Sequential: ~500MB average read
- BTI lookup: ~10KB trie navigation + direct seek

## Next Steps

1. **Implement SizedInts decoder** in `cqlite-core/src/storage/sstable/bti/sized_ints.rs`
2. **Update BTI parser** to decode payloads correctly
3. **Add integration test** using real test data
4. **Benchmark** performance improvement vs sequential scan
5. **Update documentation** with findings

## References

- Cassandra 5.0.0 source: `https://github.com/apache/cassandra/tree/cassandra-5.0.0`
- Local source: `~/local_projects/cassandra/src/java/org/apache/cassandra/io/sstable/format/bti/`
- CQLite docs: `docs/sstables-definitive-guide/chapters/17-bti-formats.md`

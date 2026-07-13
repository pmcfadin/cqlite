## Cassandra 5.0 SSTable Row Format Reference

### Source
Extracted from Apache Cassandra 5.0 source code (UnfilteredSerializer.java, Cell.java, Clustering.java)

---

## Complete Row Deserialization Sequence

### 1. Row Header (Flags)
```
[1 byte: flags]
[0-1 bytes: extended flags if EXTENSION_FLAG (0x80) set]
```

**Flags Breakdown:**
- `0x01`: IS_MARKER (range tombstone marker)
- `0x02`: (reserved)
- `0x04`: HAS_TIMESTAMP (row has liveness timestamp)
- `0x08`: HAS_TTL (row has time-to-live)
- `0x10`: HAS_DELETION (row has deletion tombstone)
- `0x20`: HAS_ALL_COLUMNS (all schema columns present, no bitmap needed)
- `0x40`: IS_STATIC (static row)
- `0x80`: EXTENSION_FLAG (extended flags byte follows)

### 2. Clustering Prefix
For tables with clustering columns:
```
[VInt: header with 2 bits per column (batches of 32)]
[bytes: column values for non-null/non-empty columns]
```

For tables **without** clustering columns (empty `clustering_types`):
- **No bytes read** - parser returns immediately

**2-bit encoding per column:**
- `00`: Present (value bytes follow)
- `01`: Empty (no bytes, empty array)
- `11`: Null (no bytes, null value)

### 3. Row Body

**SSTable format always includes:**
```
[VInt: row_size] - total bytes in row body
[VInt: prev_unfiltered_size] - size of previous unfiltered
```

**If HAS_TIMESTAMP (0x04) flag set:**
```
[VInt: timestamp_delta] - delta from encoding stats minTimestamp
[if HAS_TTL (0x08) also set:]
    [VInt: ttl_delta] - delta from encoding stats minTTL
    [VInt: local_deletion_time_delta] - delta from encoding stats minLocalDeletionTime
```

**If HAS_DELETION (0x10) flag set:**
```
[VInt: deletion_timestamp_delta] - delta from encoding stats minTimestamp
[VInt: deletion_local_time_delta] - delta from encoding stats minLocalDeletionTime
```

**Column Selection:**
```
[if NOT HAS_ALL_COLUMNS (flag 0x20 not set):]
    [VInt-encoded bitmap: which columns are present]
[if HAS_ALL_COLUMNS (flag 0x20 set):]
    (no bitmap, all schema columns present)
```

**Cell Data:**
For each present column (simple or complex):
```
[cell data - see Cell Format below]
```

---

## Cell Format

### Simple Cell
```
[1 byte: flags]
[0-1 byte: extended_flags if EXTENDED_FLAG (0x40) set]
[VInt: timestamp_delta if NOT USE_ROW_TIMESTAMP (0x08)]
[VInt: local_deletion_time_delta if IS_DELETED or IS_EXPIRING]
[VInt: ttl_delta if IS_EXPIRING and NOT USE_ROW_TTL]
[bytes: value if NOT IS_DELETED and NOT HAS_EMPTY_VALUE]
```

**Cell Flags:**
- `0x01`: IS_DELETED (tombstone)
- `0x02`: IS_EXPIRING (has TTL)
- `0x04`: HAS_EMPTY_VALUE (INVERTED: flag=0 means has value, flag=1 means empty)
- `0x08`: USE_ROW_TIMESTAMP (use row timestamp, don't read separate)
- `0x10`: USE_ROW_TTL (use row TTL)
- `0x20`: HAS_NULL_VALUE (value is null)
- `0x40`: EXTENDED_FLAG (extended flags follow)

### Complex Cell (Collections, UDTs)
Collections have additional wrapping:
```
[VInt: element_count]
[for each element:]
    [cell format as above]
```

---

## VInt Encoding

Variable-length integer used throughout format:
- First byte encodes both sign and length
- Subsequent bytes contain actual value
- Can represent both signed (VInt) and unsigned (Unsigned VInt) values

**Unsigned VInt** (used for sizes, counts):
- First byte: `0xxxxxxx` = 7-bit value
- First byte: `1xxxxxxx` = multi-byte, continuation follows

---

## Delta Encoding

Many values are delta-encoded against base values from Statistics.db:

**SerializationHeader provides:**
- `minTimestamp` - base for all timestamp deltas
- `minTTL` - base for all TTL deltas  
- `minLocalDeletionTime` - base for local deletion time deltas

**To decode:**
```rust
actual_timestamp = header.min_timestamp + timestamp_delta
actual_ttl = header.min_ttl + ttl_delta
actual_local_deletion_time = header.min_local_deletion_time + ldt_delta
```

---

## Example: Simple Row with All Columns

Schema: `simple_table` with no clustering, 18 regular columns

```
[0x24] - flags (HAS_TIMESTAMP | HAS_ALL_COLUMNS)
[VInt: row_size]
[VInt: prev_size]
[VInt: timestamp_delta]
[cell 1: account_balance]
[cell 2: created]
...
[cell 18: user_name]
```

No clustering bytes (0 clustering columns)
No column bitmap (HAS_ALL_COLUMNS set)
All 18 cells present in schema order

---

## Critical Parsing Rules

1. **Clustering Prefix**: MUST check if `clustering_types.is_empty()` before reading
2. **Column Bitmap**: Only read if HAS_ALL_COLUMNS (0x20) **not** set
3. **Delta Encoding**: All timestamps/TTLs are deltas, not absolute values
4. **Cell Empty Flag**: Logic is **inverted** - flag set = empty, flag clear = has value
5. **Row Sizes**: Always present in SSTable format (not in internal messages)

---

## Compression

Cassandra 5.0 supports three compression algorithms:

### Block Structure
```
[compressed_block_1]
[compressed_block_2]
...
```

Each block:
- Fixed maximum size (typically 64KB uncompressed)
- Compressed independently
- CRC checksum for validation
- May contain multiple rows or partial rows

### Decompression
1. Read compressed size from block header
2. Decompress entire block
3. Parse rows from decompressed buffer
4. Track offsets within decompressed data

---

## Reference Implementation

See `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` for full Rust implementation following this specification.


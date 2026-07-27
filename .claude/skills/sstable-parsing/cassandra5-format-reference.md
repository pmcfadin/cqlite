## Cassandra 5.0 SSTable Row Format Reference

### Source
Extracted from Apache Cassandra 5.0 source code (UnfilteredSerializer.java, Cell.java, Clustering.java)

---

## Complete Row Deserialization Sequence

### 1. Row Header (Flags)
```
[1 byte: flags]
[0-1 bytes: extended flags — present iff ROW_HAS_EXTENDED_FLAGS (0x80) is set]
```

**Main flag byte:**

| Value | Name | Meaning |
|-------|------|---------|
| `0x01` | `END_OF_PARTITION` | End-of-partition marker — **nothing follows this flag byte** |
| `0x02` | `IS_MARKER` | Unfiltered is a RangeTombstoneMarker, not a Row |
| `0x04` | `ROW_HAS_TIMESTAMP` | Row has a liveness timestamp (delta-encoded) |
| `0x08` | `ROW_HAS_TTL` | Row has a TTL (delta-encoded) |
| `0x10` | `ROW_HAS_DELETION` | Row has a deletion tombstone |
| `0x20` | `ROW_HAS_ALL_COLUMNS` | All schema columns present — no bitmap needed |
| `0x40` | `ROW_HAS_COMPLEX_DELETION` | Row carries a non-frozen collection column with deletion info |
| `0x80` | `ROW_HAS_EXTENDED_FLAGS` | Extended flags byte follows |

**EXTENDED flag byte** (present only when `ROW_HAS_EXTENDED_FLAGS = 0x80` is set):

| Value | Name | Meaning |
|-------|------|---------|
| `0x01` | `EXTENDED_IS_STATIC` | Static row — carries **NO** clustering prefix |

> **Citations**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/mod.rs:709-715`
> (`ROW_HAS_TIMESTAMP` `0x04`, `ROW_HAS_TTL` `0x08`, `ROW_HAS_DELETION` `0x10`,
> `ROW_HAS_ALL_COLUMNS` `0x20`, `ROW_HAS_COMPLEX_DELETION` `0x40`,
> `ROW_HAS_EXTENDED_FLAGS` `0x80`), `:820` (`END_OF_PARTITION = 0x01`), `:821`
> (`IS_MARKER = 0x02`), `:825` (`EXTENDED_IS_STATIC = 0x01`). Guide:
> `docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md:206-212`.
> Cassandra: `UnfilteredSerializer.java:102-109` and `:114-122`.

**⚠️ `0x01` is the partition boundary, not a static/marker bit.** Treating `0x01` as
`IS_STATIC` (or as the marker flag) means **mis-detecting partition boundaries** — the
highest-consequence single bit in the row format. `IS_STATIC` lives at `0x01` of the
**EXTENDED** byte. `HAS_ALL_COLUMNS` has exactly one value, `0x20`.

**Common flag combinations** (`appendix-b-encodings-cheat-sheet.md:215-219`):
- `0x24`: simple write (`HAS_TIMESTAMP | HAS_ALL_COLUMNS`)
- `0x2C`: TTL write (`HAS_TIMESTAMP | HAS_TTL | HAS_ALL_COLUMNS`)
- `0x04`: partial update (timestamp, no `HAS_ALL_COLUMNS` → bitmap follows)
- `0x14`: row deletion (`HAS_TIMESTAMP | HAS_DELETION`)

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

The V5 row/partition decoder lives in the
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/` **directory** (~30 files).
Entry points:

- `row_decoder/mod.rs` — flag constants (`END_OF_PARTITION`, `IS_MARKER`,
  `ROW_HAS_*`, `EXTENDED_IS_STATIC`) and the parser struct.
- `row_decoder/row_framing.rs` — row/partition framing and boundary detection.
- `row_decoder/row_data.rs`, `cell_value_scalar.rs`, `cell_value_complex.rs` — cell decode.
- `row_decoder/complex_column.rs` — non-frozen collections; `frozen.rs` — frozen
  collections; `udt.rs` — UDTs; `partition_driver.rs` — partition iteration.

The former single-file V5-compressed-legacy parser module was deleted by epic #1116
(source splits), commit `cb049f7a8`; any pointer to a single `.rs` file for this parser is
stale. Format authority for a genuinely disputed on-disk question is Apache Cassandra 5.0.8
(`UnfilteredSerializer.java`, `Cell.java`, `ClusteringPrefix.java`) plus
`docs/sstables-definitive-guide/`.


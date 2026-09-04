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

> **Citations**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/row_flags.rs:12-18`
> (`ROW_HAS_TIMESTAMP` `0x04`, `ROW_HAS_TTL` `0x08`, `ROW_HAS_DELETION` `0x10`,
> `ROW_HAS_ALL_COLUMNS` `0x20`, `ROW_HAS_COMPLEX_DELETION` `0x40`,
> `ROW_HAS_EXTENDED_FLAGS` `0x80`), `:24` (`END_OF_PARTITION = 0x01`), `:26`
> (`IS_MARKER = 0x02`), `:31` (`EXTENDED_IS_STATIC = 0x01`). Guide:
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
[VInt: timestamp_delta if NOT USE_ROW_TIMESTAMP (0x08)]
[VInt: local_deletion_time_delta if IS_DELETED, or if IS_EXPIRING and NOT USE_ROW_TTL]
[VInt: ttl_delta if IS_EXPIRING and NOT USE_ROW_TTL]
[bytes: value if NOT HAS_EMPTY_VALUE (length-prefixed unless the type is fixed-width)]
```

There is **no extended flag byte for a cell** — extended flags are a *row*-header concept
only.

**Cell Flags** — the complete set; there are exactly five:

| Value | Name | Meaning |
|-------|------|---------|
| `0x01` | `IS_DELETED` | Cell is a tombstone (no value) |
| `0x02` | `IS_EXPIRING` | Cell has a TTL (TTL/local-deletion fields follow) |
| `0x04` | `HAS_EMPTY_VALUE` | Zero-length value — flag SET means empty (not NULL) |
| `0x08` | `USE_ROW_TIMESTAMP` | Reuse the row timestamp; no cell timestamp is written |
| `0x10` | `USE_ROW_TTL` | Reuse the row TTL **and** local_deletion_time; neither is written |

> **Citations**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/cell_value.rs:49-53`
> — all five constants, in `parse_cell_value_schema_order`, the **production** cell
> decoder (`CELL_IS_DELETED` `0x01`, `CELL_IS_EXPIRING` `0x02`, `CELL_HAS_EMPTY_VALUE`
> `0x04`, `CELL_USE_ROW_TIMESTAMP` `0x08`, `CELL_USE_ROW_TTL` `0x10`). Cite this file, not
> `row_data.rs:860-863` — that is a `#[cfg(test)]` mirror (`parse_cell_header_end_offset`)
> carrying only four of the five. Guide:
> `appendix-b-encodings-cheat-sheet.md:231-238`. Cassandra 5.0.8:
> `db/rows/Cell.java:262-266` — `Cell.Serializer` declares these five `*_MASK` constants
> and **no others**, so `0x20` and `0x40` are NOT cell flags (an earlier revision of this
> file invented `HAS_NULL_VALUE`/`EXTENDED_FLAG`; both are fabrications).

**Critical distinction** (`appendix-b-encodings-cheat-sheet.md:247-250`): a tombstone
(`IS_DELETED`) MUST NOT set `USE_ROW_TIMESTAMP` — tombstones require an explicit timestamp
and local_deletion_time.

### Complex Cell — NON-FROZEN collections and non-frozen UDTs
A non-frozen collection column is a *set of cells*, wrapped as:
```
[if ROW_HAS_COMPLEX_DELETION (0x40) on the row: complex deletion time — 2 unsigned VInt deltas]
[unsigned VInt: cell_count]
[for each cell:]
    [1 byte: cell flags (table above)]
    [conditional timestamp / local_deletion_time / ttl deltas]
    [unsigned VInt: path_len][path bytes]      // the collection key/element path
    [unsigned VInt: value_len][value bytes]    // omitted when IS_DELETED or HAS_EMPTY_VALUE
```

> **Citation**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs:279-300`
> (complex deletion deltas), `:332` (`cell_count` via `parse_vuint`), `:1089` (path length
> unsigned VInt), `:1136` (value length unsigned VInt). Guide:
> `appendix-b-encodings-cheat-sheet.md:530-536` — a non-frozen collection cell
> length-prefixes BOTH path and value with an **unsigned VInt**, fixed-width element types
> included.

### FROZEN collections / tuples / UDTs — a DIFFERENT encoding
A frozen value is a single opaque cell whose bytes use **fixed 4-byte big-endian `i32`**
counts and element lengths — **not** VInts:
```
[i32 BE: element_count]
[for each element:]
    [i32 BE: element_len]   // -1 = null
    [element bytes]
```

> **Citation**: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/frozen.rs:21`
> (`i32::from_be_bytes` count), `:98`/`:279`/`:370` (element/key lengths). Guide:
> `appendix-b-encodings-cheat-sheet.md:537-539`. Cassandra:
> `CollectionSerializer.java:67-92`, `TupleType.java:341-364`.

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

1. **Partition boundary FIRST**: a flag byte of `0x01` (`END_OF_PARTITION`) ends the
   partition — nothing follows it. Check it before interpreting any other bit
   (`row_decoder/row_flags.rs:24`).
2. **Markers are not rows**: `0x02` (`IS_MARKER`) means a RangeTombstoneMarker, not a Row
   (`row_decoder/row_flags.rs:26`).
3. **Clustering Prefix**: MUST check if `clustering_types.is_empty()` before reading; a
   static row (`EXTENDED_IS_STATIC` = `0x01` of the **extended** byte) has no clustering
   prefix at all (`row_decoder/row_flags.rs:31`).
4. **Column Bitmap**: Only read if HAS_ALL_COLUMNS (0x20) **not** set
5. **Delta Encoding**: All timestamps/TTLs are deltas, not absolute values
6. **Cell Empty Flag**: Logic is **inverted** - flag set = empty, flag clear = has value
7. **Row Sizes**: Always present in SSTable format (not in internal messages)
8. **Never guess**: take signedness/framing from the field's serializer, never from byte
   patterns (no-heuristics mandate, #28).

---

## Compression

Cassandra 5.0 supports **four compression algorithms plus Noop**: LZ4, Snappy, Deflate,
Zstd, and Noop (stored raw) — `cqlite-core/src/storage/sstable/compression_info.rs:43-48`.
See `compression-formats.md` in this skill for the full `CompressionInfo.db` layout.

### Chunk Structure
```
[compressed_chunk_1][crc32: 4 bytes BE]
[compressed_chunk_2][crc32: 4 bytes BE]
...
```

Each chunk:
- Fixed maximum **uncompressed** size = `chunk_length` from `CompressionInfo.db`;
  Cassandra 5.0 default **16 KiB** (`CompressionParams.DEFAULT_CHUNK_LENGTH`,
  cassandra-5.0.8 `schema/CompressionParams.java:47`)
- Compressed independently
- Followed by an **unconditional** 4-byte big-endian CRC32 over the **compressed** bytes
  (`chunk_decompressor.rs:275-292`)
- May contain multiple rows or partial rows

### Decompression
1. Read the chunk offsets from `CompressionInfo.db` (offsets **only** — there is no stored
   per-chunk length; the payload length is `next_offset - this_offset - 4`)
2. Read the chunk record, validate the trailing big-endian CRC32 over the compressed bytes
3. Decompress the chunk with the algorithm named in `CompressionInfo.db`
4. Parse rows from the decompressed buffer; offset within the chunk is
   `logical_offset % chunk_length`

---

## Reference Implementation

The V5 row/partition decoder lives in the
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/` **directory** (~30 files).
Entry points:

- `row_decoder/row_flags.rs` — the row + extended flag constants
  (`END_OF_PARTITION`, `IS_MARKER`, `ROW_HAS_*`, `EXTENDED_IS_STATIC`); they left
  `mod.rs` in a campsite-rule split (epic #1116). `row_decoder/mod.rs` — the parser
  struct and entry point.
- `row_decoder/row_framing.rs` — row/partition framing and boundary detection.
- `row_decoder/cell_value.rs` — the production cell decoder + the five `CELL_*` flag
  constants (`:49-53`). `row_data.rs`, `cell_value_scalar.rs`, `cell_value_complex.rs` —
  the rest of the cell-decode ladder.
- `row_decoder/complex_column.rs` — non-frozen collections; `frozen.rs` — frozen
  collections; `udt.rs` — UDTs; `partition_driver.rs` — partition iteration.

The former single-file V5-compressed-legacy parser module was deleted by epic #1116
(source splits), commit `cb049f7a8`; any pointer to a single `.rs` file for this parser is
stale. Format authority for a genuinely disputed on-disk question is Apache Cassandra 5.0.8
(`UnfilteredSerializer.java`, `Cell.java`, `ClusteringPrefix.java`) plus
`docs/sstables-definitive-guide/`.


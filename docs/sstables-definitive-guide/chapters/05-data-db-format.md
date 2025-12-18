## Data.db Format

This chapter describes the on-disk layout of partitions, rows, and cells in `Data.db`: how headers reference schema, how unfiltered rows, range tombstones, and markers are encoded, and how encodings like vints and cell flags are interpreted.

### In this chapter you will learn
- Partition headers and row/cluster layout basics
- Cell value encodings, varints/vints, collections/UDTs
- Deletions, range tombstones, TTLs and expiring cells
- How readers interpret flags and headers during parsing

## Partition and Row Layout

Minimal annotated example from `test_basic/simple_table` (trimmed and formatted):

```text
partition key = 4d4321e2-662b-4ba1-b75f-48e080727a52
row liveness ts = 2025-09-16T22:14:23.739Z
cells: account_balance=21088.5, active=false, age=75, name=(utf8) ...
```

Underlying file shows a partition stream with a serialization header followed by unfiltered rows and optional tombstone markers.

![Data.db row layout](diagrams/data-db-row-layout)
- Alt text: Annotated Data.db partition/row/cell structure
- Caption: Serialization header → unfiltered rows/markers → cells with flags and vints

## Encodings and Flags

VInt parsing (Cassandra-compatible), used across headers and lengths. For a concise implementation walkthrough, see Appendix C.

Readers interpret row/cell flags to distinguish live cells, TTLs, and tombstones; see Chapter 11 for tombstone semantics. Cross-link to Appendix B for a compact encoding summary.

Common cell flags (high level):
- live cell vs tombstone
- presence of timestamp, ttl, local deletion time
- empty/expiring cells

Bit-level flags (Cassandra 5.0, authoritative references):

| Bit | Meaning                    | When present                                |
|-----|----------------------------|---------------------------------------------|
| 0   | isDeleted                  | Cell is a tombstone                         |
| 1   | isExpiring                 | TTL fields follow                           |
| 2   | hasEmptyValue              | Zero-length value                           |
| 3   | hasTimestamp               | Timestamp present in cell header            |
| 4   | hasLocalDeletionTime       | Local deletion time present                 |
| 5+  | format extensions/reserved | Format-specific                             |

Authoritative classes to consult in Cassandra 5.0:
- `org.apache.cassandra.db.rows.*` (e.g., `Unfiltered`, `Cell`, `BufferCell`)
- `org.apache.cassandra.db.SerializationHeader`
- `org.apache.cassandra.db.rows.SerializationHelper`

Endianness:
- Integers in SSTable payloads are big-endian unless otherwise specified; varints are MSB-first variable-length.
- Network/binary compatibility relies on consistent big-endian parsing for fixed-width fields.

## Deletions and TTL Semantics

- Partition tombstone: marks entire partition deleted at a timestamp
- Row tombstone: targets a specific clustering row
- Range tombstone: spans clustering ranges
- TTL/expiring: cells carry ttl and local deletion time; expired cells are omitted at read

### Collections and UDTs

**Collections** (list/set/map) have two storage modes:
- **Frozen** (`frozen<list<...>>`): Single-cell storage, entire collection serialized as one blob
- **Non-frozen** (`list<...>`): Multi-cell storage, each element stored as separate cell

**Non-frozen collection cell format** (complex columns):
```
[flags: u8]
[timestamp: VInt if not USE_ROW_TIMESTAMP_MASK]
[local_deletion_time: VInt if deleted/expiring]
[ttl: VInt if expiring]
[cell_path: VInt length + bytes]  ← List: UUID, Set: element value, Map: key
[value: VInt length + bytes]      ← Element/value data
```

**UDTs** (User-Defined Types) serialize fields in schema order with 4-byte BE length prefixes:
```
[field_1_length: 4-byte BE i32][field_1_data]
[field_2_length: 4-byte BE i32][field_2_data]
...
```

**UDT field length semantics** (confirmed via Issue #220):
- `-1` (0xFFFFFFFF): Field is NULL
- `0` (0x00000000): Field is empty (zero-length but present)
- `>0`: Number of bytes of field data following
- Trailing omitted fields are implicitly NULL

**Critical distinction**: The **outer** type determines storage:
- `list<frozen<udt>>` = multi-cell (each UDT element is separate cell)
- `frozen<list<udt>>` = single-cell (entire list is one blob)

See `tables/type-mapping-complex.md` for detailed format specifications.

### Key Takeaways
- `Data.db` is schema-driven and encodes partitions as unfiltered row streams.
- VInts and bit flags compactly encode sizes, timestamps, and cell metadata.
- Tombstones and TTLs are first-class and affect reconciliation.

### Troubleshooting
- If parsed sizes seem inconsistent, verify VInt decoding and endian assumptions.
- For collections with unexpected nulls, check for element tombstones and TTL expiration handling.

### References
- Cassandra 5.0.0:
  - Rows and tombstones: `org.apache.cassandra.db.rows.*` (`Unfiltered`, `RangeTombstoneMarker`)
  - Serialization header: [org.apache.cassandra.db.SerializationHeader](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java)
  
For implementation details, see Appendix C.

## V5CompressedLegacy Row Header Format (Cassandra 5.0)

The V5CompressedLegacy format (BigFormat with compression, "nb" file prefix) uses a structured row header with delta-encoded metadata fields. This format is used by Cassandra 5.0 SSTables with the legacy "big" format and compression enabled.

### Row Structure (Corrected - Issue #213)

The complete row format, confirmed via Cassandra's `UnfilteredSerializer.java`:

```
[row_flags: u8]
[extended_flags: u8 if 0x80 set]
[clustering_prefix: variable]          ← For tables with clustering keys
[row_size: VInt]
[prev_size: VInt]
[timestamp: VInt if 0x04 set]          ← Delta from min_timestamp
[ttl: VInt if 0x08 set]                ← Delta from min_ttl
[deletion: 2 VInts if 0x10 set]        ← local_deletion_time delta + deletion timestamp
[column_bitmap: VInt + bytes if NOT 0x20]
[cell_data...]
```

**Critical Note**: For tables WITH clustering keys, the clustering prefix comes IMMEDIATELY after flags and BEFORE `row_size`. This differs from initial documentation which placed `row_size` immediately after flags.

### Clustering Prefix Format

For tables with clustering keys, values are encoded between flags and row_size:

```
[header: VInt]                         ← 2 bits per clustering column
[value_1: type-specific]               ← Only if state indicates PRESENT
[value_2: type-specific]
...
```

The header VInt uses 2 bits per column to indicate state:
- `00` (0): Value PRESENT - followed by type-specific bytes
- `01` (1): Value EMPTY - zero-length (no bytes follow)
- `10` (2): Value NULL - no bytes follow
- `11` (3): Reserved

Type-specific encoding:
- **Fixed-width types** (timestamp, int, bigint, UUID): Raw bytes, no length prefix
- **Variable-width types** (text, varchar, blob): VInt length prefix + bytes

### Row Flags

| Flag | Hex  | Meaning            | Details |
|------|------|--------------------|---------|
| 0x04 | HAS_TIMESTAMP      | Timestamp delta present | Delta-encoded from Statistics.db min_timestamp |
| 0x08 | HAS_TTL           | TTL delta present | Delta-encoded from Statistics.db min_ttl |
| 0x10 | HAS_DELETION      | Deletion time present | Two VInts: local_deletion_time delta and deletion timestamp |
| 0x20 | HAS_ALL_COLUMNS   | All columns present (no bitmap) | When set, all schema columns have values (no NULLs) |
| 0x80 | HAS_EXTENDED_FLAGS | Extended flags byte follows | Reserved for future format extensions |

### Delta Decoding

All metadata fields use delta encoding against minimum values from Statistics.db:

```
absolute_timestamp = min_timestamp + timestamp_delta
absolute_ttl = min_ttl + ttl_delta
absolute_deletion_time = min_local_deletion_time + deletion_time_delta
```

**Example**: If Statistics.db shows `min_timestamp = 1759713125983682` and row header contains `timestamp_delta = 1000`, the absolute timestamp is `1759713125984682` (microseconds since epoch).

### Column Bitmap

When `HAS_ALL_COLUMNS` (0x20) is **NOT** set, a column bitmap follows the metadata fields:

```
[column_count: VInt]
[bitmap_bytes: (column_count + 7) / 8 bytes]
```

Each bit indicates column presence:
- Bit = 1: Column has a value in this row
- Bit = 0: Column is NULL (not present)

**Example**: For a table with 10 columns, if only columns 0, 2, and 9 have values:
- `column_count = 10` (VInt: 0x0a)
- `bitmap_bytes = 2` bytes: `0b00000101` (columns 0,2) and `0b00000010` (column 9)

### Validation

This format specification is confirmed through:
- Implementation: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
- Cassandra Source: `org.apache.cassandra.db.rows.UnfilteredSerializer.java` (lines 151-210)
- Integration tests: All 26/33 test tables pass (tables with clustering keys now work)
- Test data: Real Cassandra 5.0 SSTables including sensor_data, wide_partition_table, app_metrics

### References

- Cassandra 5.0.0 Source: `org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer`
- SerializationHeader: Delta encoding semantics for Statistics.db integration
- Implementation research: See `docs/sstables-definitive-guide/ISSUE_162_LEARNINGS.md` for detailed findings



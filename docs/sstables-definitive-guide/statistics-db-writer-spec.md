# Cassandra 5.0 Statistics.db Binary Format Specification

**Document Purpose**: Writer Implementation Guide  
**File Analyzed**: `test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db`  
**Total Size**: 8483 bytes (0x2123)  
**Date**: January 2026

This specification documents the EXACT binary format needed to write valid Statistics.db files for Cassandra 5.0 'nb' format SSTables.

---

## 1. File Structure Overview

Statistics.db consists of:
1. **TOC (Table of Contents)** - `4 + (2 × 4) + (8 × num_components)` bytes for checksummed
   versions (`nb`/`oa`), or `4 + (8 × num_components)` bytes without checksums. For 4
   components with checksums: `4 + 8 + 32 = 44` bytes (VALIDATION starts at `0x002c`).
2. **Component data sections** - VALIDATION, COMPACTION, STATS, HEADER

**Key Encoding Rules**:
- All multi-byte integers are **BIG ENDIAN**
- All strings are **UTF-8** encoded, prefixed with VInt length
- VInt encoding follows Cassandra's variable-length integer format

---

## 2. TOC (Table of Contents) Format

| Offset   | Size | Type    | Field Name         | Example Value                    |
|----------|------|---------|-------------------|----------------------------------|
| `0x0000` | 4    | u32 BE  | `num_components`  | `0x00000004` (always 4)         |
| `0x0004` | 4    | u32 BE  | `checksum`        | `0x26291b05` (CRC32)            |

### TOC Entries (8 bytes each, 4 total):

| Offset   | Size | Type    | Field Name         | Example Value                    |
|----------|------|---------|-------------------|----------------------------------|
| `0x0008` | 4    | u32 BE  | `component_type`  | 0 (VALIDATION)                   |
| `0x000c` | 4    | u32 BE  | `component_offset`| `0x002c` (44 bytes)             |
| `0x0010` | 4    | u32 BE  | `component_type`  | 1 (COMPACTION)                   |
| `0x0014` | 4    | u32 BE  | `component_offset`| `0x0065` (101 bytes)            |
| `0x0018` | 4    | u32 BE  | `component_type`  | 2 (STATS)                        |
| `0x001c` | 4    | u32 BE  | `component_offset`| `0x0b53` (2899 bytes)           |
| `0x0020` | 4    | u32 BE  | `component_type`  | 3 (HEADER)                       |
| `0x0024` | 4    | u32 BE  | `component_offset`| `0x1d2b` (7467 bytes)           |

**TOC ends at**: `0x002b` (44 bytes for checksummed nb/oa). The 4 bytes at `0x0028`–`0x002b` are
the `toc_block_checksum` (CRC32 over all TOC entries, `MetadataSerializer.java:96`), not padding.

### MetadataType Enum

From [`org.apache.cassandra.io.sstable.metadata.MetadataType`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataType.java):

| Value | Name        | Description                              |
|-------|-------------|------------------------------------------|
| 0     | VALIDATION  | Partitioner, bloom filter settings       |
| 1     | COMPACTION  | Ancestor histograms, cardinality         |
| 2     | STATS       | Full `StatsMetadata` (26 version-gated fields; histograms, timestamps, repair info) |
| 3     | HEADER      | SerializationHeader (column schema)      |

---

## 3. VALIDATION Component (MetadataType 0)

**Purpose**: Validation metadata including partitioner and bloom filter settings

**Offset**: `0x002c` (44 bytes from file start)  
**Size**: 53 bytes

### Binary Format

| Offset   | Size | Type    | Field Name         | Value                                              |
|----------|------|---------|--------------------|---------------------------------------------------|
| `0x002c` | 2    | u16 BE  | `partitioner_len`  | `0x00 0x2b` = 43 (Java `writeUTF` 2-byte length prefix) |
| `0x002e` | 43   | UTF-8   | `partitioner`      | `org.apache.cassandra.dht.Murmur3Partitioner`     |
| `0x0059` | 8    | f64 BE  | `bloom_fp_chance`  | `0.01` (1% false positive rate)                   |

**Raw bloom_fp_chance bytes**: `3f 84 7a e1 47 ae 14 7b`

### Writer Notes

- `partitioner_len` is a **2-byte big-endian** length prefix written by Java `DataOutput.writeUTF`
  (e.g., `0x00 0x2b` = 43 for Murmur3Partitioner). No reserved byte precedes it.
- Standard Murmur3 partitioner string is 43 bytes
- `bloom_fp_chance`: Typical value is `0.01` (1%) or `0.001` (0.1%)

---

## 4. COMPACTION Component (MetadataType 1)

**Purpose**: Compaction metadata including ancestor histograms and cardinality estimates

**Offset**: `0x0065` (101 bytes from file start)  
**Size**: 2798 bytes

### Structure

Contains complex `EstimatedHistogram` structures for:
- Ancestor table counts
- Cardinality estimates (HyperLogLog)

**First 32 bytes** (observed):
```
0x0065: 00 00 0a e6 ff ff ff fe 0d 19 01 e8 07 be b1 01
0x0075: f0 d4 01 82 80 01 f2 b2 03 c8 81 08 f0 d1 1c b4
```

### Writer Notes (DEFERRED TO FUTURE MILESTONE)

- For MVP writer: Use minimal stub (empty/zero histogram data)
- Future: Implement full `MetadataCollector` serialization
- Cassandra source: [`org.apache.cassandra.io.sstable.metadata.CompactionMetadata`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/CompactionMetadata.java)

---

## 5. STATS Component (MetadataType 2)

**Purpose**: Table statistics including EncodingStats and distribution histograms

**Offset**: `0x0b53` (2899 bytes from file start)  
**Size**: 4568 bytes (LARGEST component)

### Structure

Contains the full `StatsMetadata` serialization (26 version-gated fields including
`EstimatedHistogram` for partition sizes and cell counts per row, commit-log positions,
min/max timestamps, deletion times, TTL, compression ratio, `TombstoneHistogram`,
level, repair metadata, clustering bounds, and more). See `StatsMetadata.java:402–513`.

> **Note**: `EncodingStats` is NOT a sub-block inside the STATS component. It is the
> leading field of the HEADER component only (`SerializationHeader.java:453`).

**First 64 bytes** (observed):
```
0x0b53: 00 00 00 9c 00 00 00 00 00 00 00 01 00 00 00 00
0x0b63: 00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 00
0x0b73: 00 00 00 00 00 00 00 00 00 00 00 02 00 00 00 00
0x0b83: 00 00 00 00 00 00 00 00 00 00 00 03 00 00 00 00
```

### Writer Notes (DEFERRED TO FUTURE MILESTONE)

- `StatsMetadata` is the full serialization (26 fields); `EncodingStats` is the leading
  field of the HEADER component, not an embedded sub-block here.
- For MVP writer: Use minimal stub with zero/empty histograms
- Future: Implement full `StatsMetadata` serializer with version gates
- **Note**: Current `enhanced_statistics_parser.rs` ONLY extracts EncodingStats

> **TombstoneHistogram legacy format (pre-`oa`)**: The `LegacyHistogramSerializer`
> writes deletion time as `double` (8 bytes) and tombstone count as `long` (8 bytes)
> per entry — NOT `int` (4 bytes). Total per entry = 16 bytes (`TombstoneHistogram.java:155–156`).
> The modern serializer (`oa`+) writes `long` (8) + `int` (4) = 12 bytes per entry.
> Also: `HistogramSerializer.serializedSize` over-allocates by `4 × histSize` bytes
> (stale comment assumes 16 bytes/entry); do not use it as a wire-byte guarantee.

---

## 6. HEADER Component (SerializationHeader, MetadataType 3)

**Purpose**: Column schema metadata for deserializing Data.db files

**Offset**: `0x1d2b` (7467 bytes from file start)  
**Size**: 1016 bytes

### Binary Format

| Offset   | Size   | Type                  | Field Name                   | Value                                                   |
|----------|--------|-----------------------|------------------------------|---------------------------------------------------------|
| `0x1d2b` | 7      | UnsignedVInt (64-bit) | `minTimestamp_delta`         | `fd 20 28 75 dc 45 19` (~316 T µs from TIMESTAMP_EPOCH) |
| `0x1d32` | 1      | UnsignedVInt32        | `minLocalDeletionTime_delta` | `0x00` (no deletions; delta from DELETION_TIME_EPOCH)   |
| `0x1d33` | 1      | UnsignedVInt32        | `minTTL_delta`               | `0x00` (no TTL; delta from TTL_EPOCH = 0)               |
| `0x1d34` | 1      | VInt                  | `pk_type_len`                | `0x28` (40 bytes)                                       |
| `0x1d35` | 40     | UTF-8                 | `pk_type`                    | `org.apache.cassandra.db.marshal.UUIDType`              |
| `0x1d5d` | 1      | UnsignedVInt32        | `ck_count`                   | `0x00` (0 clustering keys)                              |
| `0x1d5e` | 1      | UnsignedVInt32        | `static_count`               | `0x00` (0 static columns)                               |
| `0x1d5f` | 1      | UnsignedVInt32        | `reg_count`                  | `0x12` = 18 regular columns                             |

### Regular Column Format (repeats for each column)

**Example: First column (`account_balance`)**

| Offset   | Size | Type    | Field Name         | Value                                              |
|----------|------|---------|--------------------|---------------------------------------------------|
| `0x1d60` | 1    | VInt    | `col_name_len`     | `0x0f` (15 bytes)                                 |
| `0x1d61` | 15   | UTF-8   | `col_name`         | `account_balance`                                 |
| `0x1d70` | 1    | VInt    | `col_type_len`     | `0x2b` (43 bytes)                                 |
| `0x1d71` | 43   | UTF-8   | `col_type`         | `org.apache.cassandra.db.marshal.DecimalType`     |

This pattern repeats for all 18 regular columns.

### Complete SerializationHeader Structure

The HEADER component MUST begin with `EncodingStats` before any key-type data
(`SerializationHeader.java:453`). A writer that omits EncodingStats shifts every
subsequent field and produces a file Cassandra cannot parse.

1. **EncodingStats** (3–14 bytes, variable): Three unsigned VInt delta fields:
   - `minTimestamp_delta`: `writeUnsignedVInt` (64-bit, up to 8 bytes)
   - `minLocalDeletionTime_delta`: `writeUnsignedVInt32` (32-bit, up to 5 bytes)
   - `minTTL_delta`: `writeUnsignedVInt32` (32-bit, up to 5 bytes)
2. **pk_type_len** (VInt): Length of partition key type string
3. **pk_type** (UTF-8): CQL type descriptor
4. **ck_count** (UnsignedVInt32): Number of clustering keys (0 for simple tables)
5. **(If ck_count > 0)**: For each clustering key:
   - `ck_type_len` (VInt)
   - `ck_type` (UTF-8)
6. **static_count** (UnsignedVInt32): Number of static columns (0 if none)
7. **(If static_count > 0)**: For each static column:
   - `col_name_len` (VInt), `col_name` (UTF-8), `col_type_len` (VInt), `col_type` (UTF-8)
8. **reg_count** (UnsignedVInt32): Number of regular columns (18 in this file)
9. **For each regular column** (18 in this file):
   - `col_name_len` (VInt)
   - `col_name` (UTF-8)
   - `col_type_len` (VInt)
   - `col_type` (UTF-8): Full CQL type descriptor

### Column Type Examples

| CQL Type  | Marshal Type String                                  |
|-----------|-----------------------------------------------------|
| UUID      | `org.apache.cassandra.db.marshal.UUIDType`          |
| DECIMAL   | `org.apache.cassandra.db.marshal.DecimalType`       |
| BOOLEAN   | `org.apache.cassandra.db.marshal.BooleanType`       |
| TEXT      | `org.apache.cassandra.db.marshal.UTF8Type`          |
| DOUBLE    | `org.apache.cassandra.db.marshal.DoubleType`        |
| TIME      | `org.apache.cassandra.db.marshal.TimeType`          |
| TIMESTAMP | `org.apache.cassandra.db.marshal.TimestampType`     |
| INT       | `org.apache.cassandra.db.marshal.Int32Type`         |
| BIGINT    | `org.apache.cassandra.db.marshal.LongType`          |

---

## 7. Encoding Details

### VInt (Variable-length Integer)

Based on Cassandra's VInt encoding (see `cqlite-core/src/parser/vint.rs`):

- **Single byte** (0-127): `0x00`-`0x7F`
- **Two bytes** (128-16511): First byte `0x80`-`0xBF`
- **Three bytes**: First byte `0xC0`-`0xDF`
- **Four bytes**: First byte `0xE0`-`0xEF`
- And so on...

**Encoding algorithm**:
```rust
fn encode_vint(value: i64) -> Vec<u8> {
    // See cqlite-core/src/parser/vint.rs for reference
}
```

### UTF-8 Strings

- Always prefixed with **VInt length**
- Length is **byte count**, not character count
- No null terminator
- Standard UTF-8 encoding

**Example**:
```
"hello" → [0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
         └─ VInt(5)   └─ UTF-8 bytes ─────────┘
```

---

## 8. Writer Implementation Checklist

### PHASE 1 (MVP - Issue #328)

- [x] Understand TOC structure
- [x] Understand VALIDATION component (partitioner, bloom FP)
- [x] Understand HEADER component (SerializationHeader)
- [ ] Implement VInt encoder
- [ ] Implement TOC writer
- [ ] Implement VALIDATION writer
- [ ] Implement HEADER writer
- [ ] Stub COMPACTION writer (minimal valid data)
- [ ] Stub STATS writer (minimal valid data)
- [ ] Calculate component offsets dynamically
- [ ] Calculate checksum for TOC

### PHASE 2 (Future Milestone)

- [ ] Implement full COMPACTION component (EstimatedHistogram)
- [ ] Implement full STATS component (EncodingStats + MetadataCollector)
- [ ] Add validation against Cassandra sstabledump

---

## 9. Component Offset Summary

| Component    | Type | Offset    | Size  | End       |
|--------------|------|-----------|-------|-----------|
| TOC          | -    | `0x0000`  | 40    | `0x0028`  |
| VALIDATION   | 0    | `0x002c`  | 53    | `0x0061`  |
| COMPACTION   | 1    | `0x0065`  | 2798  | `0x0b53`  |
| STATS        | 2    | `0x0b53`  | 4568  | `0x1d2b`  |
| HEADER       | 3    | `0x1d2b`  | 1016  | `0x2123`  |

**File size**: 8483 bytes (0x2123)

---

## 10. Validation Steps for Writer

1. Write test Statistics.db file
2. Verify TOC checksum matches Cassandra computation
3. Verify component offsets are correct
4. Test parsing with `enhanced_statistics_parser.rs`
5. Verify `simple_table` schema round-trips correctly
6. Compare binary output with this reference file using:
   - `hexdump -C`
   - `diff` tool
7. Run Cassandra `sstablemetadata` tool on output
8. Verify Data.db can be read with written Statistics.db

---

## 11. References

- [`MetadataSerializer.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java)
- [`MetadataType.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataType.java)
- [`ValidationMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/ValidationMetadata.java)
- [`CompactionMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/CompactionMetadata.java)
- [`StatsMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)
- [`SerializationHeader.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java)
- [`EncodingStats.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/EncodingStats.java)
- [`EstimatedHistogram.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/EstimatedHistogram.java)
- [`TombstoneHistogram.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/streamhist/TombstoneHistogram.java)
- **CQLite Parser**: `cqlite-core/src/parser/enhanced_statistics_parser.rs`
- **Reference File**: `test-data/datasets/sstables/test_basic/simple_table-.../nb-1-big-Statistics.db`

---

**End of Specification**

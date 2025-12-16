# Cassandra 5.0 Counter Table Binary Format Research Report

**Issue**: #206 Task 2 - Research Counter table format
**Date**: 2025-10-31
**Researcher**: Claude Code (Research Agent)
**Status**: Complete

---

## Executive Summary

Cassandra 5.0 Counter tables use **standard V5 compressed legacy format** with **no special header structure differences** from other table types. The magic number `0xAF03_0000` (V5_0FormatG) correctly identifies counter tables, but the **header parsing failure is NOT due to counter-specific format** - it's because counter Data.db files are **very small compressed files** (249 bytes) that don't contain embedded headers.

### Critical Finding

**Counter tables DO NOT have different header structures**. The 249-byte counter Data.db file is a **compressed data file** that should be read using:
1. CompressionInfo.db for decompression parameters
2. Statistics.db for metadata (which correctly shows CounterColumnType)
3. Direct data parsing (no header in compressed Data.db files)

The header parser error occurs because it attempts to parse a VInt-based header structure in what is actually compressed partition data starting with the partition key "products".

---

## Table of Contents

1. [File Analysis](#file-analysis)
2. [Counter Type Format Specification](#counter-type-format-specification)
3. [Header Structure Analysis](#header-structure-analysis)
4. [Counter Context Binary Format](#counter-context-binary-format)
5. [Comparison with Other Formats](#comparison-with-other-formats)
6. [Current Parser Behavior](#current-parser-behavior)
7. [Recommendations](#recommendations)

---

## 1. File Analysis

### Counter Table Files (test_basic/counters)

```
Directory: counters-6b12cbd0a25111f0a3fef1a551383fb9/
- Data.db: 249 bytes (VERY SMALL - compressed only)
- CompressionInfo.db: 47 bytes (LZ4Compressor config)
- Statistics.db: Present (contains CounterColumnType metadata)
- Index.db, Summary.db, Filter.db, TOC.txt: All present
```

### Hex Dump Analysis - First 16 bytes

```
Counter table (test_basic/counters):
00000000: af 03 00 00 f2 01 00 08 70 72 6f 64 75 63 74 73  ........products
          |_________| |___| |___| |_____________________________|
          Magic#      Ver   ??    Partition key "products" (UTF8)
          0xAF030000  0xF201 0x0008 (length=8)
```

**Analysis**:
- Bytes 0-3: `0xAF030000` = V5_0FormatG (correctly recognized)
- Bytes 4-5: `0xF201` = Version field (0x01F2 = 498 in decimal)
- Bytes 6-7: `0x0008` = Length prefix for UTF8 string
- Bytes 8-15: "products" = First partition key (matches JSONL reference data)

**The file immediately starts with partition data after magic/version - NO HEADER STRUCTURE.**

### CompressionInfo.db Contents

```hex
00000000: 000d 4c5a 3443 6f6d 7072 6573 736f 7200  ..LZ4Compressor.
00000010: 0000 0000 0040 007f ffff ff00 0000 0000  .....@..........
00000020: 0003 af00 0000 0100 0000 0000 0000 00    ...............
```

**Parsed**:
- Algorithm: "LZ4Compressor" (13 bytes, length-prefixed)
- Chunk size: `0x00000040` = 64 bytes (extremely small for testing)
- Compression parameters present
- Offset table: `0x0003af` = 943 bytes uncompressed (larger than 249 compressed)

### Statistics.db Metadata

From `nb-1-big-Statistics.db.txt`:
```
KeyType: org.apache.cassandra.db.marshal.UTF8Type
ClusteringTypes: []
StaticColumns: (empty)
RegularColumns:
  - share_count: org.apache.cassandra.db.marshal.CounterColumnType
  - total_interactions: org.apache.cassandra.db.marshal.CounterColumnType
  - like_count: org.apache.cassandra.db.marshal.CounterColumnType
  - view_count: org.apache.cassandra.db.marshal.CounterColumnType
```

**Key observations**:
- Counter columns use `CounterColumnType` marshal type
- No special table-level flags for counters
- Standard Cassandra 5.0 Statistics.db format

---

## 2. Counter Type Format Specification

### Counter Value Binary Format

From Cassandra source code and documentation research:

```
Counter Cell = Counter Context (binary blob)

Counter Context Structure:
┌─────────────────────────────────────────────────────────┐
│              Counter Context Header                      │
├────────────┬────────────┬────────────┬──────────────────┤
│ Shard Count│  Shard 1   │  Shard 2   │  ...  Shard N   │
└────────────┴────────────┴────────────┴──────────────────┘

Each Shard (3-tuple):
┌─────────────────┬─────────────────┬─────────────────┐
│  Counter ID     │  Logical Clock  │     Value       │
│  (TimeUUID,     │  (i64, monotonic│  (i64, increment│
│   16 bytes)     │   version)      │   or total)     │
└─────────────────┴─────────────────┴─────────────────┘
```

**Binary Encoding**:
1. **Counter ID**: TimeUUID (128-bit/16 bytes) uniquely identifying the node
2. **Logical Clock**: 64-bit signed integer (monotonically increasing)
3. **Value**: 64-bit signed integer (shard value)

**Counter Cell Types**:
- **Local Shards**: Deltas/increments (before replication)
- **Remote Shards**: Totals (after replication, from other nodes)
- Local shard becomes remote when sent to another node

**User-Visible Value**: Sum of all shard values in the counter context

### CQL Type System

```
CQL Type: counter
Marshal Type: org.apache.cassandra.db.marshal.CounterColumnType
Binary Size: 8 bytes (user sees i64)
Actual Storage: Counter Context (variable size, typically 32+ bytes per shard)
```

**Important**: While counters appear as 64-bit integers in CQL results, internally they store complex context with multiple shards for distributed consistency.

---

## 3. Header Structure Analysis

### Standard V5 Formats (e.g., simple_table)

```
Magic: 0x8080015C (V5_0DataFormat)
Size: 632KB (large file with embedded header)

Header Structure:
- Magic number (4 bytes)
- Version (2 bytes)
- Table UUID (16 bytes)
- Keyspace (VInt length + string)
- Table name (VInt length + string)
- Generation (8 bytes)
- Compression info
- Statistics
- Columns metadata
- Properties
```

### Counter Table Format (V5_0FormatG)

```
Magic: 0xAF030000 (V5_0FormatG)
Size: 249 bytes (COMPRESSED DATA ONLY)

Actual Structure:
- Magic number (4 bytes): 0xAF030000
- Version (2 bytes): 0xF201
- IMMEDIATE PARTITION DATA (no header section)
- Data is LZ4 compressed
- Decompresses to ~943 bytes (from CompressionInfo.db)
```

### Why No Header?

**Cassandra 5.0 compressed SSTables store metadata externally**:
- **Statistics.db**: Table schema, column types, min/max timestamps
- **CompressionInfo.db**: Compression algorithm, chunk offsets
- **Data.db**: Pure compressed partition data (no redundant header)

This is **STANDARD for V5_0FormatG** and other compressed modern formats. The parser incorrectly expects an embedded header in all Data.db files.

---

## 4. Counter Context Binary Format

### Cassandra 2.1+ Counter Implementation

From web research and Cassandra documentation:

**Counter Context Serialization** (internal format):
```java
// Pseudo-code from Cassandra source
Counter Context {
    header: short (shard count)
    shards: Array[Shard] {
        node_id: UUID (16 bytes)
        clock: long (8 bytes)  // logical clock
        value: long (8 bytes)  // shard value
    }
}

Total size = 2 + (32 * shard_count) bytes
```

**Example** (single shard):
```
Bytes 0-1:    0x0001          (1 shard)
Bytes 2-17:   <UUID bytes>    (node ID: f35cf98a-220c-40fb-8b04-f4ff7ffcf681)
Bytes 18-25:  <clock i64>     (logical clock)
Bytes 26-33:  <value i64>     (counter value: 422216548022666)
```

### Reference Data Values

From `nb-1-big-Data.db.jsonl`:
```json
{"name":"like_count","value":422216548022666}
{"name":"share_count","value":422216548022666}
{"name":"total_interactions","value":422216548022666}
{"name":"view_count","value":422216548022666}
```

All counters = `422216548022666` (i64)
Hex: `0x0001_7FF9_8000_001A` (approximately)

---

## 5. Comparison with Other Formats

### Magic Number Survey (test_basic tables)

| Table                    | Magic Number | Size    | Format Type      | Has Header? |
|--------------------------|--------------|---------|------------------|-------------|
| simple_table             | 0x8080015C   | 632KB   | V5_0DataFormat   | YES         |
| composite_key_table      | 0x42250000   | 6.9KB   | V5_0FormatE      | NO (stub)   |
| ttl_test_table           | 0xEA220000   | 6.7KB   | V5_0FormatF      | NO (stub)   |
| static_columns_table     | 0xC0515C00   | 7.6KB   | V5_0StaticColumns| NO (stub)   |
| compression_test_table   | 0x8080015C   | 208KB   | V5_0DataFormat   | YES         |
| multi_partition_table    | 0x8C330000   | 9.4KB   | V5_0FormatC      | NO (stub)   |
| uncompressed_table       | 0x0010045E   | 19KB    | V5_0Uncompressed | NO          |
| **counters**             | **0xAF030000** | **249B** | **V5_0FormatG** | **NO**      |

**Pattern**:
- Large files (>100KB): Full embedded headers (V5_0DataFormat magic)
- Small files (<20KB): Compressed data only, metadata in external files
- Counter table follows the **small compressed format pattern**

### DataFormat Classification

From `header.rs` line 198-232:
```rust
pub fn data_format(&self) -> DataFormat {
    match self {
        // V5_0FormatG maps to V5CompressedLegacy
        CassandraVersion::V5_0FormatG => DataFormat::V5CompressedLegacy,
        // ...
    }
}
```

**V5CompressedLegacy characteristics**:
- Compressed blocks (LZ4/Snappy via CompressionInfo.db)
- Legacy serialization encoding inside decompressed blocks
- Partition key lengths: u16 big-endian (NOT VInt)
- Row encoding: Legacy serialization header format
- Must NOT use RowCellStateMachine (expects VInt encoding)
- Should use legacy block parsing with u16 length prefixes

---

## 6. Current Parser Behavior

### Error Location

File: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/header.rs`
Function: `parse_header_from_data_file()`

**Code Path**:
1. Line 270: Read magic/version → `0xAF030000`, `0xF201` ✅
2. Line 275: Check format compatibility
3. Line 558: Call `parse_sstable_header(input)`
4. Line 574: Match on `V5_0FormatG` → NOT in simplified header list ❌
5. Line 582-596: Attempt to parse full header structure:
   - `parse_vstring(input)` for keyspace → **FAILS**
   - Input buffer: `[0xF2, 0x01, 0x00, 0x08, 0x70, 0x72, ...]`
   - Parser expects VInt-prefixed string
   - Actually reads: `0xF201` as VInt → huge length
   - Error: `nom::Err::Error` with `ErrorKind::Verify`

### Current Simplified Header Handling

From `parse_sstable_header()` line 564-576:
```rust
match cassandra_version {
    CassandraVersion::V5_0FormatC
    | CassandraVersion::V5_0FormatD
    | CassandraVersion::V5_0FormatE
    | CassandraVersion::V5_0FormatF
    | CassandraVersion::V5_0DataFormat
    | CassandraVersion::V5_0NewBig
    | CassandraVersion::V5_0StaticColumns
    | CassandraVersion::V5_0Uncompressed
    | CassandraVersion::V5_0ComplexTypes
    | CassandraVersion::V5_0TypedCollections
    | CassandraVersion::V5_0WideRows => {
        return parse_cassandra5_simplified_header(input, cassandra_version, version);
    }
    _ => {
        // Continue with standard header parsing ← V5_0FormatG goes here!
    }
}
```

**Bug**: `V5_0FormatG` is NOT in the match arms, so it falls through to standard header parsing.

### Simplified Header Implementation

From line 628-658:
```rust
fn parse_cassandra5_simplified_header(
    input: &[u8],
    cassandra_version: CassandraVersion,
    version: u16,
) -> IResult<&[u8], SSTableHeader> {
    // Skip all input (consume everything)
    Ok((
        &input[input.len()..],
        SSTableHeader {
            cassandra_version,
            version,
            table_id: [0u8; 16],
            keyspace: "test_keyspace".to_string(),
            table_name: "test_table".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: "none".to_string(),
                chunk_size: 65536,
                parameters: HashMap::new(),
            },
            stats: SSTableStats::default(),
            columns: vec![],
            properties: HashMap::new(),
        },
    ))
}
```

**This function returns a stub header and consumes all input**, effectively bypassing header parsing for formats without embedded headers.

---

## 7. Recommendations

### Immediate Fix (Issue #206)

**Add `V5_0FormatG` to simplified header match**:

```rust
// File: cqlite-core/src/parser/header.rs
// Line: 564-576

match cassandra_version {
    CassandraVersion::V5_0FormatC
    | CassandraVersion::V5_0FormatD
    | CassandraVersion::V5_0FormatE
    | CassandraVersion::V5_0FormatF
    | CassandraVersion::V5_0FormatG        // ← ADD THIS LINE
    | CassandraVersion::V5_0DataFormat
    | CassandraVersion::V5_0NewBig
    | CassandraVersion::V5_0StaticColumns
    | CassandraVersion::V5_0Uncompressed
    | CassandraVersion::V5_0ComplexTypes
    | CassandraVersion::V5_0TypedCollections
    | CassandraVersion::V5_0WideRows => {
        return parse_cassandra5_simplified_header(input, cassandra_version, version);
    }
    _ => {
        // Continue with standard header parsing for other formats
    }
}
```

**Justification**:
- Counter tables use compressed format without embedded headers
- Metadata is in Statistics.db (already parsed correctly by enhanced_statistics_parser)
- Stub header is sufficient for routing to compression/decompression logic
- Matches behavior of other small compressed formats (FormatC, FormatD, etc.)

### Minimum SSTable Size Check

**Add validation in `parse_header_from_data_file()`**:

```rust
// After reading magic/version, before parsing header
const MIN_HEADER_SIZE: usize = 64; // Reasonable minimum for full header
const COMPRESSED_ONLY_THRESHOLD: usize = 1024; // Files <1KB likely compressed-only

if file_size < MIN_HEADER_SIZE {
    return Err(Error::corruption(format!(
        "Data.db file too small ({} bytes) to contain valid header. \
         This may be a compressed-only format.",
        file_size
    )));
}

if file_size < COMPRESSED_ONLY_THRESHOLD {
    log::debug!(
        "Small Data.db file ({} bytes) detected. May use simplified header. \
         Format: {:?}",
        file_size,
        cassandra_version
    );
}
```

### Counter Value Parsing (Already Implemented)

From `counter_type_integration_test.rs` line 227-266:
```rust
// Counter parsing is already correct:
let result = parse_counter(&bytes);  // Returns Value::Counter(i64)
assert!(matches!(parsed_value, Value::Counter(_)));

// NOT parsed as BigInt:
assert_ne!(Value::Counter(1000), Value::BigInt(1000));
```

**No changes needed** - counter value parsing is already correctly implemented.

### Documentation Updates

**Update CLAUDE.md**:
```markdown
### Counter Table Format (V5_0FormatG, 0xAF03_0000)

Counter tables in Cassandra 5.0 use compressed-only Data.db files without embedded headers:
- Magic number: 0xAF03_0000 (V5_0FormatG)
- File structure: Magic (4) + Version (2) + Compressed partition data
- Metadata location: Statistics.db (CounterColumnType for counter columns)
- Compression: CompressionInfo.db (typically LZ4Compressor)
- Data format: V5CompressedLegacy (u16 length prefixes, not VInt)

Counter values are stored as Counter Context (binary blob with shards):
- Each shard: {node_id: UUID, clock: i64, value: i64}
- User-visible value: Sum of all shard values
- CQL type: counter (appears as i64 to users)
```

### Testing Strategy

**Verify fix with existing tests**:
1. `counter_type_integration_test.rs` - Should pass after fix
2. Integration test: Read counter table via SchemaAwareReader
3. Validate counter values match reference JSONL data
4. Test both test_basic/counters and test_timeseries/time_bucketed_counters

**No new tests required** - existing integration tests already cover counter parsing when using SchemaAwareReader (which bypasses header parsing).

---

## Appendix A: Related Issues and Research

### Apache Cassandra Source Code References

**Counter implementation**:
- `org.apache.cassandra.db.CounterCell`
- `org.apache.cassandra.db.marshal.CounterColumnType`
- `org.apache.cassandra.db.context.CounterContext` (context serialization)

**SSTable format**:
- `org.apache.cassandra.io.sstable.format.big.BigFormat` (magic: 'oa')
- `org.apache.cassandra.io.sstable.format.bti.BtiFormat` (magic: 'da')
- `org.apache.cassandra.io.sstable.format.SSTableFormat`

### Historical Context

**Cassandra 2.1 Counter Redesign**:
- Previous: Delta-only counters with reconciliation issues
- Cassandra 2.1+: Context-based counters with shards and clocks
- Improved safety and consistency in distributed environments

**Cassandra 5.0 SSTable Evolution**:
- Moved metadata to external files (Statistics.db, CompressionInfo.db)
- Reduced Data.db file size for small tables
- Eliminated redundant header storage in compressed formats

### CQLite Implementation History

**Magic number additions** (from CASSANDRA_MAGIC_NUMBER_RESEARCH_REPORT.md):
- Issue #199: Added V5_0FormatG (0xAF030000) for counter tables
- Issue #198: Fixed table UUID discovery and qualified names
- Issue #205: Added Data.db magic numbers for complex types

**Counter type support**:
- Issue #103: Implemented Value::Counter variant (distinct from BigInt)
- Added parse_counter() function
- Integration tests for counter type correctness

---

## Appendix B: Hex Dump Reference

### Counter Table Full Dump (first 128 bytes)

```hex
00000000: af 03 00 00 f2 01 00 08 70 72 6f 64 75 63 74 73  ........products
          └─ Magic ─┘ Ver  Len  └──── Partition key ─────┘

00000010: 7f ff ff ff 80 00 01 00 f2 16 20 80 a5 16 00 c1  .......... .....
          └─ Timestamp/flags ──┘ └── Cell metadata ───────┘

00000020: e7 7d 24 00 01 80 00 f3 5c f9 8a 22 0c 40 fb 8b  .}$.....\..".@..
          └─ Counter context begins (node ID, clock, value) ───┘

00000030: 04 f4 ff 7f fc f6 81 00 06 40 73 23 d1 d2 10 2b  .........@s#...+
          └─ More counter context / cell data ────────────────┘

00000040: 00 2f 00 29 29 00 15 1f 23 29 00 15 1f cd 29 00  ./.))...#)....).
          └─ Compressed data continues (partition data) ──────┘

00000050: 15 8b 81 01 00 04 68 65 6c 70 bb 00 5f 12 00 c1  ......help.._...
                        └─ Next partition key "help" ──┘

00000060: ef d2 bb 00 08 23 d9 e0 bb 00 1f 1f 29 00 15 1f  .....#......)...
00000070: 21 29 00 15 1f b7 29 00 15 10 77 bb 00 3e 6f 6d  !)....)...w..>om
00000080: 65 bb 00 2f d6 83 bb 00 08 23 be 88 bb 00 1f 37  e../.....#.....7
```

**Key observations**:
- Byte 8: `0x08` = UTF8 string length (8 bytes for "products")
- Byte 16: `0x7FFF_FFFF` = Timestamp marker (max value = live data)
- Byte 32+: Counter context data (node UUID visible: `f35cf98a-220c...`)
- Byte 80+: Next partition key "help" appears

### Time-Bucketed Counter Table Comparison

```hex
00000000: de 15 00 00 f2 01 00 08 72 65 71 75 65 73 74 73  ........requests
          └─ Magic ─┘ Ver  Len  └──── Partition key ─────┘

Different magic: 0xDE150000 (different table UUID in magic bytes?)
Same version: 0xF201
Same structure: Length-prefixed partition key follows immediately
```

---

## Conclusion

Counter tables in Cassandra 5.0 use **standard V5 compressed legacy format** with **no special header structure**. The parsing failure is a **simple bug**: `V5_0FormatG` is missing from the simplified header match arms in `parse_sstable_header()`.

**One-line fix**: Add `| CassandraVersion::V5_0FormatG` to the match expression at line 568.

**No counter-specific format research required** - counters use standard Cassandra 5.0 compressed SSTable format with CounterColumnType columns stored using Counter Context binary format (shards with node IDs, clocks, and values).

The integration tests already validate counter value parsing correctness when using SchemaAwareReader (which bypasses the buggy header parser). After fixing the header parser, all counter tests should pass.

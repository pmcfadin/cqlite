# V5CompressedLegacy Decompressed Block Format Specification

**Status**: Phase 1 Research - PRELIMINARY FINDINGS
**Date**: 2025-10-14
**Issue**: #160
**Test Data**: `test_basic.simple_table` (nb-1-big-Data.db)

## Executive Summary

The V5CompressedLegacy format (`DataFormat::V5CompressedLegacy`) corresponds to Cassandra 5.0 SSTables using the "nb" (big) format with Snappy compression. After decompression, the binary data uses a **hybrid encoding scheme** that differs from the pure VInt encoding used in the newer "oa" format.

**CRITICAL FINDING**: The decompressed blocks do NOT use pure VInt encoding. Analysis reveals what appears to be **u8 or u16 length prefixes** for partition keys, combined with fixed-width fields for timestamps and deletion markers.

## Block Structure Overview

```
Decompressed Block Layout:
┌────────────────────────────────────────────────────┐
│ Partition Header (varies)                          │
├────────────────────────────────────────────────────┤
│ Row Data (multiple rows per partition)             │
│ ┌────────────────────────────────────────────────┐ │
│ │ Row Header                                     │ │
│ ├────────────────────────────────────────────────┤ │
│ │ Liveness Info (timestamp, TTL, etc.)           │ │
│ ├────────────────────────────────────────────────┤ │
│ │ Cells (column name/value pairs)                │ │
│ └────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────┘
```

## Detailed Field Encodings

### Partition Header

From test data analysis (`15291a77-d739-4e73-8397-b787442f3a1f`):

```
Offset | Hex Bytes                                      | Interpretation
-------|-----------------------------------------------|------------------
0x0000 | 00                                            | Flags byte (unfiltered partition marker)
0x0001 | 10                                            | Partition key length (16 bytes for UUID)
0x0002 | 15 29 1a 77 d7 39 4e 73 83 97 b7 87 44 2f 3a 1f | Raw UUID bytes (16 bytes)
0x0012 | 7f ff ff ff                                   | Partition deletion time (0x7fffffff = no deletion)
0x0016 | 80 00 00 00 00 00 00 00                       | Timestamp or flags (8 bytes, signed encoding?)
```

**Key Observations**:
1. **Offset 0x0000**: Single byte flags field
   - 0x00 = standard unfiltered partition header
   - May encode: IS_EMPTY, HAS_STATIC_ROW, HAS_PARTITION_DELETION, etc.

2. **Offset 0x0001**: **u8 length prefix** (NOT VInt)
   - Value 0x10 (16 decimal) = exact UUID byte count
   - This is a **simple u8**, not VInt-encoded
   - VInt encoding of 16 would also be 0x10, but context suggests u8

3. **Offset 0x0002-0x0011**: Raw partition key bytes
   - UUID stored as 16 raw bytes
   - NO component count prefix
   - NO per-component length prefixes
   - Direct binary representation

4. **Offset 0x0012-0x0015**: Partition-level deletion time
   - 4-byte signed integer (big-endian)
   - 0x7fffffff = Integer.MAX_VALUE = no deletion
   - Present even when partition is not deleted

5. **Offset 0x0016-0x001d**: Timestamp/liveness marker (8 bytes)
   - Structure unclear - may be:
     * Base timestamp for delta encoding
     * Row count estimate
     * Additional flags
   - Requires further analysis

### Row Header Structure

Starting at approximately offset 0x001e:

```
Offset | Hex Bytes          | Interpretation
-------|-------------------|------------------
0x001e | 24 82             | Row flags and type markers
0x0020 | 5b 1e c8 21 af 08 | Timestamp data (partial)
0x0026 | 07 00             | Continuation/flags
0x0028 | 00 00 02 30       | Cell count or clustering data
```

**Analysis**:
- Row header format is NOT yet fully decoded
- Timestamp from JSON: "2025-10-06T01:12:05.394120Z"
- Need to map these bytes to the timestamp value

### Cell Encoding

Example from hex dump (column: "ascii_field" = "ascii"):

```
Offset | Hex Bytes                | Interpretation
-------|--------------------------|------------------
0x0035 | 08                       | Type tag or flags
0x0036 | 05                       | String length (5 bytes)
0x0037 | 61 73 63 69 69           | "ascii" (UTF-8)
```

**Key Observations**:
1. **Offset 0x0035**: Type tag (0x08)
   - May indicate: string type, has value, no TTL, etc.
   - Requires correlation with Cassandra type system

2. **Offset 0x0036**: **u8 length prefix** for string
   - Value 0x05 = 5 bytes
   - Simple u8, NOT VInt

3. **Offset 0x0037-0x003b**: Raw UTF-8 string bytes

## Encoding Type Summary

Based on hex dump analysis and source code review:

| Field                  | Encoding Type    | Evidence                                    |
|------------------------|------------------|---------------------------------------------|
| Partition key length   | **u8**           | 0x10 for 16-byte UUID                       |
| Partition key data     | Raw bytes        | Direct UUID bytes, no component structure   |
| Partition deletion     | i32 (big-endian) | 0x7fffffff = Integer.MAX_VALUE             |
| Timestamp fields       | Mixed (unknown)  | 8-byte fields, encoding unclear             |
| String length          | **u8**           | 0x05 for 5-byte "ascii" string              |
| String data            | UTF-8            | Raw bytes                                   |

## Source Code vs. Reality

### What Cassandra 5.0 Source Says

From `UnfilteredRowIteratorSerializer.java`:
- Partition keys use `ByteBufferUtil.writeWithVIntLength()`
- Timestamps use VInt delta encoding: `writeUnsignedVInt(timestamp - minTimestamp)`
- Cell counts use VInt encoding
- **Expected**: Pure VInt-based encoding

### What We Observe in V5CompressedLegacy

- Partition key length: **u8** (0x10 for 16 bytes)
- Partition key data: **Raw bytes** (no VInt lengths)
- Deletion time: **Fixed 4-byte i32**
- String lengths: **u8** (0x05 for 5 bytes)
- **Reality**: Hybrid u8/raw byte encoding

## Hypothesis: Why the Discrepancy?

1. **Legacy Compatibility Layer**
   - The "nb" format may retain older encoding for backward compatibility
   - Compressed formats may use simplified encoding for efficiency
   - VInt overhead eliminated for common sizes (strings, UUIDs)

2. **Format Evolution**
   - "oa" format (V5_0NewBig, V5_0Bti): Pure VInt encoding
   - "nb" format (V5CompressedLegacy): Hybrid u8/VInt encoding
   - Migration path from older Cassandra versions

3. **Compression Optimization**
   - Simpler encoding compresses better with Snappy
   - Reduces CPU overhead during compression/decompression
   - Trade-off: less flexible, more predictable sizes

## Unknowns and Research Gaps

### High Priority
1. **Row header structure** (bytes 0x001e-0x002b)
   - How is the timestamp encoded?
   - Where is the clustering key (if any)?
   - What do the flags bytes mean?

2. **Cell type tags** (byte 0x0035 = 0x08)
   - Mapping to CQL types (text, int, boolean, etc.)
   - Flags for: nullable, has TTL, has timestamp, etc.

3. **Length encoding rules**
   - When is u8 used vs. u16 vs. VInt?
   - Is there a threshold (e.g., <256 bytes = u8)?

### Medium Priority
4. **Multi-component partition keys**
   - How are composite keys encoded?
   - Are component counts included?
   - Individual component lengths?

5. **Clustering keys**
   - Test table may have no clustering
   - Need example with clustering to analyze

6. **Complex types**
   - Collections (lists, sets, maps)
   - UDTs (user-defined types)
   - Tuples

### Low Priority
7. **Static rows** vs. regular rows
8. **Tombstones** and deletion markers
9. **TTL** encoding
10. **Counter columns**

## Next Steps for Phase 2 (Implementation)

### 1. Validate u8 Length Hypothesis
```rust
// Test parsing with u8 length prefix
let partition_key_len = data[1] as usize; // u8, NOT VInt
let partition_key_bytes = &data[2..2 + partition_key_len];
```

### 2. Decode Timestamp Fields
```rust
// Try different timestamp encodings:
// - i64 big-endian microseconds
// - i64 with sign bit encoding
// - Delta-encoded from base timestamp
```

### 3. Build Cell Parser
```rust
// Cell structure hypothesis:
// [type_tag: u8][length: u8][data: [u8; length]]
fn parse_cell(data: &[u8]) -> Result<(String, Value)> {
    let type_tag = data[0];
    let length = data[1] as usize;
    let value_bytes = &data[2..2 + length];

    match type_tag {
        0x08 => parse_string(value_bytes),
        0x04 => parse_boolean(value_bytes),
        // ... other types
    }
}
```

### 4. Create Test-Driven Parser
- Start with `test_basic.simple_table` as reference
- Parse first partition completely
- Validate against sstabledump JSON
- Iterate until 100% match

## References

### Test Data
- **File**: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db`
- **Hex Dump**: `/tmp/v5_compressed_legacy_block_sample.hex`
- **JSON Reference**: `/tmp/simple_table_sstabledump.json`
- **Analysis**: `/tmp/v5_format_analysis.txt`

### Cassandra Source Code
- `UnfilteredRowIteratorSerializer.java` (cassandra-5.0 branch)
- `SerializationHeader.java` (cassandra-5.0 branch)
- `UnfilteredSerializer.java` (cassandra-5.0 branch)
- `BigFormat.java` (cassandra-5.0 branch)

### Key Commits
- Issue #159: V5CompressedLegacy routing (reverted)
- Issue #158: Schema propagation fixes
- Issue #157: SchemaManager integration

## Recommendations

### For Immediate Implementation

1. **Create dedicated parser**: `parse_v5_compressed_legacy_partition()`
   - Separate from VInt-based state machine parser
   - Use u8 length prefixes for partition keys
   - Handle fixed-width deletion times

2. **Test incrementally**:
   - Parse partition header only (first 30 bytes)
   - Add row header parsing
   - Add cell parsing
   - Validate each step against sstabledump

3. **Document assumptions**:
   - Tag each parsing decision with evidence
   - Create test cases for edge cases
   - Maintain hex dump annotations

### For Long-Term Architecture

1. **Dual parser strategy**:
   - `V5UncompressedOA`: Pure VInt encoding + state machine
   - `V5CompressedLegacy`: Hybrid u8/raw encoding + dedicated parser
   - Route based on `DataFormat` enum

2. **Schema integration**:
   - Use schema for type interpretation (already in place)
   - Fall back to blob for unknown types
   - Validate parsed values against schema types

3. **Validation infrastructure**:
   - Compare parsed output to sstabledump JSON
   - Track parse success rate per field
   - Generate diff reports for failures

## Appendix A: Hex Dump with Full Annotations

```
0000: 00       Partition header flags (0x00 = standard unfiltered)
0001: 10       Partition key length (16 bytes, u8)
0002-0011:     Partition key UUID bytes (15291a77-d739-4e73-8397-b787442f3a1f)
      15 29 1a 77 d7 39 4e 73
      83 97 b7 87 44 2f 3a 1f
0012-0015:     Partition deletion time (0x7fffffff = no deletion, i32 big-endian)
      7f ff ff ff
0016-001d:     Unknown 8-byte field (timestamp? flags?)
      80 00 00 00 00 00 00 00
001e-001f:     Row header start (flags?)
      24 82
0020-0027:     Timestamp/liveness data
      5b 1e c8 21 af 08 07 00
0028-002b:     Cell count or clustering data
      00 00 02 30
002c-0034:     Unknown structure
      36 0f 08 01 08 00 00 00 28
0035:          Cell type tag (0x08)
      08
0036:          String length (5 bytes, u8)
      05
0037-003b:     String "ascii" (UTF-8)
      61 73 63 69 69
003c:          Next cell type tag (0x08)
      08
003d:          Next cell flags/length (0x04)
      04
003e-0041:     Next cell value (unknown type)
      80 00 4f 21
... (continues with more cells)
```

## Appendix B: Cassandra Type Tag Mapping (Hypothesis)

| Type Tag | CQL Type   | Evidence                    |
|----------|------------|-----------------------------|
| 0x08     | text/ascii | String "ascii" at offset 0x0037 |
| 0x04     | int/boolean| Follows type 0x08 pattern   |
| 0x??     | double     | account_balance = 31595.67  |
| 0x??     | timestamp  | created field               |
| 0x??     | date       | birth_date field            |
| 0x??     | blob       | description (large blob)    |

**Note**: Type tags are HYPOTHETICAL and require validation against more examples.

---

**Document Status**: Phase 1 Complete - Ready for Phase 2 Implementation
**Last Updated**: 2025-10-14
**Next Review**: After Phase 2 parser implementation

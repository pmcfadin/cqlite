# Issue #163: SerializationHeader Location in Statistics.db

## Executive Summary

**Problem**: The current parser in `parse_minimal_encoding_stats()` attempts to parse SerializationHeader immediately after parsing EncodingStats (minTimestamp, minLocalDeletionTime, minTTL), but the SerializationHeader is NOT immediately adjacent to these fields in the binary format.

**Root Cause**: There is a large (~4900 byte) section between EncodingStats and SerializationHeader that contains histogram/bloom filter data. The current parser doesn't skip this intermediate section.

## Hex Dump Analysis

File: `ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db`
Total size: 5235 bytes (0x1473)

### Section Boundaries

1. **Header** (offsets 0x00-0x1F, 32 bytes)
2. **EncodingStats** (offsets 0x20-~0x70, approximately 80 bytes)
   - metadata_type: 0x00000003 (u32 BE)
   - Partitioner string (43 bytes): "org.apache.cassandra.dht.Murmur3Partitioner"
   - minTimestamp, minLocalDeletionTime, minTTL (VInts)
3. **Histogram/Bloom Filter Data** (offsets ~0x70-0x1398, approximately 4900 bytes)
   - This is the MISSING piece the current parser doesn't account for
4. **SerializationHeader** (offsets 0x1399+)
   - Starts at offset **0x1399** (5017 bytes from file start, 4985 bytes after header)

### Confirmed SerializationHeader Location

At offset **0x1399**:
```
00001390  75 ed 65 c2 f0 12 e3 ce  e5 c1 51 80 28 6f 72 67  |u.e.......Q.(org|
000013a0  2e 61 70 61 63 68 65 2e  63 61 73 73 61 6e 64 72  |.apache.cassandr|
000013b0  61 2e 64 62 2e 6d 61 72  73 68 61 6c 2e 55 55 49  |a.db.marshal.UUI|
000013c0  44 54 79 70 65 00 00 03  0e 65 78 70 69 72 69 6e  |DType....expirin|
```

Decoded:
- **0x1398**: `0x51` (81 decimal - but this is actually part of a larger VInt)
- **0x1399-0x139A**: `0x80 0x28` - VInt encoding for 40 (partition key type length)
- **0x139B-0x13C2**: `(org.apache.cassandra.db.marshal.UUIDType` (40 bytes)
- **0x13C3-0x13C4**: `0x00 0x00` - section marker
- **0x13C5**: `0x03` - regular column count (3 columns)
- **0x13C6+**: Column definitions follow

## Current Parser Behavior

The `parse_minimal_encoding_stats()` function:

1. Parses header (32 bytes) ✓
2. Skips metadata_type (u32) ✓
3. Parses/skips data_length VInt ✓
4. Parses partitioner_len VInt ✓
5. Skips partitioner string (43 bytes) ✓
6. Skips 2 metadata VInts ✓
7. Parses minTimestamp VInt ✓
8. Parses minLocalDeletionTime VInt ✓
9. Parses minTTL VInt ✓
10. **Calls `parse_serialization_header_columns(input)` directly** ✗

At step 10, the `input` pointer is approximately at offset 0x70-0x80, but SerializationHeader doesn't start until offset 0x1399. The parser is off by approximately **4900 bytes**.

## Solution

The current implementation in `parse_serialization_header_columns()` **already has the correct solution** - it searches for the column section marker pattern (`0x00 0x00 [count]`) within the first 8KB of remaining data.

However, there may be an issue with the search logic or with how the parser is being called. To debug this:

### Recommended Debug Steps

1. **Add logging before calling parse_serialization_header_columns** to show:
   - Exact byte position in file after parsing minTTL
   - Next 64-128 bytes in hex
   - Remaining buffer size

2. **Add logging in parse_serialization_header_columns** to show:
   - Input buffer size at entry
   - Search progress (offsets where potential markers are found)
   - Whether it finds the pattern at offset ~4900

3. **Verify the search window**:
   - Current max search is 8KB (8192 bytes)
   - Gap to SerializationHeader is ~4900 bytes
   - This should be sufficient, but verify search is working correctly

## Expected Fix

Based on analysis, the search-based approach in `parse_serialization_header_columns()` is correct. The issue is likely one of:

A. **Search not finding the pattern** - The pattern `0x00 0x00 0x03` at offset 0x13C3 should be detected
B. **Validation failing** - Column parsing might be failing validation and continuing search
C. **Pattern variation** - The actual pattern might be slightly different (e.g., VInt encoding of count)

### Pattern to Search For

At offset 0x13C3 in file:
```
0x00 0x00 0x03 0x0e 0x65 0x78 0x70...
[section marker] [count=3] [name_len=14] [name="expiring_value"...]
```

This matches the expected pattern exactly.

## Conclusion

The SerializationHeader starts at offset 0x1399 (4985 bytes after the 32-byte header). The current parser's search-based approach should work, but may have a bug in the search logic or validation. Adding debug logging as outlined above will identify the exact failure point.


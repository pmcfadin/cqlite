# Issue #160: Java Source Code Research Findings

**Date**: 2025-10-15
**Status**: ❌ **BLOCKED** - Need clarification on actual format

---

## Summary

Researched Cassandra 5.0 Java source code to understand V5CompressedLegacy format. Found discrepancies between:
1. Patrick's description (row header u16 with flags + column_count)
2. Modern Cassandra code (separate flag bytes, VInt column bitmap)
3. Test data behavior (nonsensical column counts like 549, 891 for 4-column table)

**Root cause**: Parser looking for row headers at wrong offsets (5521, 5789, 6050...) - heuristic search failing.

---

## What I Found in Cassandra Java Source

### 1. SerializationHeader.java (trunk)

**Format**:
```java
// SerializationHeader.Serializer.deserialize()
EncodingStats stats = EncodingStats.serializer.deserialize(in);
AbstractType<?> keyType = typeSerializer.deserialize(in);
List<AbstractType<?>> clusteringTypes = typeSerializer.deserializeList(in);
Map<ByteBuffer, AbstractType<?>> staticColumns = readColumnsWithType(in);
Map<ByteBuffer, AbstractType<?>> regularColumns = readColumnsWithType(in);
```

**Key insight**: SerializationHeader is in Statistics.db metadata, NOT in Data.db stream (for modern formats).

### 2. Columns.java - Column Subset Bitmap (trunk)

**For < 64 columns**:
```java
public Columns deserializeSubset(Columns superset, DataInputPlus in) {
    long encoded = in.readUnsignedVInt();  // Single VInt, not chunk_count!
    if (encoded == 0L) return superset;     // 0 = all columns present

    // Each bit: 0 = present, 1 = missing
    for (ColumnMetadata column : superset) {
        if ((encoded & 1) == 0) builder.add(column);
        encoded >>>= 1;
    }
}
```

**This contradicts Patrick's description** of "unsigned short chunk_count + bitmask bytes". Modern Cassandra uses simple VInt bitmap.

### 3. UnfilteredSerializer.java (trunk) - Modern Row Format

**Modern format** (NOT LegacyLayout):
```java
// Flags in first byte
int flags = 0;
if (!pkLiveness.isEmpty()) flags |= HAS_TIMESTAMP;  // 0x04
if (pkLiveness.isExpiring()) flags |= HAS_TTL;      // 0x08
if (!deletion.isLive()) flags |= HAS_DELETION;      // 0x10
if (hasAllColumns) flags |= HAS_ALL_COLUMNS;        // 0x20
```

**Key insight**: Modern format uses separate flag bytes, NOT a combined u16 with `flags = header & 0x3F; column_count = header >> 6`.

### 4. LegacyLayout.java (cassandra-3.0)

Found in cassandra-3.0 branch, but does NOT contain RowHeader constants Patrick described. The file has:
```java
public final static int DELETION_MASK        = 0x01;
public final static int EXPIRATION_MASK      = 0x02;
public final static int COUNTER_MASK         = 0x04;
public final static int COUNTER_UPDATE_MASK  = 0x08;
private final static int RANGE_TOMBSTONE_MASK = 0x10;
```

**Could not find** the RowHeader class with `HAS_TIMESTAMP=0x01, HAS_TTL=0x02, HAS_DELETION=0x04` that Patrick described.

---

## Current Implementation Status

### What's Implemented ✅

1. **SerializationHeader parsing** (lines 100-237):
   - VInt header length
   - EncodingStats (5 VInts)
   - Column sets parsing

2. **Row header parsing** (lines 594-724):
   - `flags = row_header & 0x3F`
   - `column_count = row_header >> 6`
   - Optional fields based on flags

3. **Column bitmap parsing** (lines 730-810):
   - Chunk-based format (Patrick's description)
   - NOT the VInt format from Columns.java

### What's Broken ❌

**Test output** (simple_table with 4 columns):
```
Row header = 0x897f, flags = 0x3f, column_count = 549
Row header = 0x4a17, flags = 0x17, column_count = 296
Row header = 0xdecf, flags = 0x0f, column_count = 891
Row header = 0x63a7, flags = 0x27, column_count = 398
Row header = 0xd691, flags = 0x11, column_count = 858
```

**Problem**: Column counts are nonsensical (549-891 for 4-column table).

**Root cause**: Heuristic search finding wrong offsets (5521, 5789, 6050, 6198, 6502). These aren't actually row headers - just random bytes being misinterpreted.

---

## Key Questions for Patrick

### 1. Where does SerializationHeader live?

**Patrick's guidance**: "SerializationHeader at very start of Data.db stream"

**Cassandra source**: SerializationHeader.Serializer is for Statistics.db metadata, not Data.db stream.

**Question**: Does V5CompressedLegacy actually have SerializationHeader in each decompressed block, or is it only in Statistics.db?

### 2. What is the actual row format?

**Patrick said**:
- u16 row header = flags (lower 6 bits) + column_count (upper 10 bits)
- Row header 0x0224: flags=0x24, column_count=8

**Cassandra source**:
- Modern UnfilteredSerializer uses separate flag bytes
- Couldn't find RowHeader class in LegacyLayout.java

**Question**: Is there a specific Cassandra source file that shows the u16 row header format Patrick described? Or is this a CQLite-specific interpretation?

### 3. Column bitmap format?

**Patrick said**: "unsigned short chunkCount, then for each chunk: unsigned short offset + 8 bytes bitmask"

**Cassandra source**: `Columns.deserializeSubset()` uses single VInt where each bit = column presence

**Question**: Which format does V5CompressedLegacy actually use?

### 4. Where do rows actually start?

**Current blocker**: Heuristic search finds wrong offsets.

**Question**: After partition header (ends at offset ~30), what comes next?
- Option A: SerializationHeader → then rows
- Option B: Column bitmap → then rows
- Option C: Rows start immediately (no header/bitmap per partition)
- Option D: Something else?

---

## Hex Dump Analysis

From test_basic/simple_table Data.db (first partition, decompressed block):

```
Offset  Bytes                            Interpretation
------  -----                            --------------
0x00    00                               Partition flags ✓
0x01    10                               Partition key length = 16 ✓
0x02    15 29 1a 77 ... 2f 3a 1f        UUID partition key (16 bytes) ✓
0x12    7f ff ff ff                      Deletion time (none) ✓
0x16    80 00 09 01 f4 97 02 24          8 bytes unknown
0x1e    82 5b 1e c8 21 af                6 bytes unknown
0x24    08 07 00 00 00 02 30 36          ???
0x2c    0f 08 01 08 00 00 00 28          ???
0x34    08 05 61 73 63 69 69             0x08 + text "ascii"
0x3b    08 04 80 00 4f 21                0x08 + 4 bytes
```

**Patrick's analysis**: "At offset 0x1e, row header is 0x0224 (bytes `02 24`)"

**But**: I see bytes `82 5b` at offset 0x1e, not `02 24`.

**Question**: Can Patrick provide exact byte offsets for:
1. Where partition header ends
2. Where row header starts
3. Where column bitmap starts (if present)
4. Where first cell data starts

---

## Possible Paths Forward

### Option A: Get actual Cassandra 5.0 format spec

**Need**:
- Exact file/class name for V5CompressedLegacy row format
- Byte-level layout documentation
- Maybe: sstabledump source code that reads this format

### Option B: Reverse-engineer from test data

**Approach**:
1. Patrick provides annotated hex dump showing:
   - Partition header end offset
   - Row header location and value
   - Column bitmap location (if present)
   - First cell location
2. Implement exact byte-level parsing matching that structure

### Option C: Use sstabledump as reference

**Approach**:
1. Run Cassandra sstabledump on test data with verbose/debug mode
2. Compare sstabledump's byte offsets with our parser
3. Implement matching logic

### Option D: Simplified fallback

**If format is too complex**:
1. Skip SerializationHeader/row header parsing
2. Use simple heuristic: after partition header, look for cell markers (0x08, 0x20, etc.)
3. Iterate schema columns, parse each cell
4. Accept limitations (won't handle NULL columns, clustering keys, etc.)

---

## Recommendation

**Immediate action**: Need Patrick to clarify which Cassandra source file/class shows the exact V5CompressedLegacy format, OR provide annotated hex dump showing byte-by-byte structure.

**Without this**: Cannot proceed - current implementation is parsing random bytes as row headers.

---

## Files Modified

- `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` (592 lines)
  - SerializationHeader parsing: Implemented (may not match actual format)
  - Row header parsing: Implemented (wrong offsets)
  - Column bitmap: Implemented (wrong format?)
  - Cell parsing: Implemented (never reached due to wrong offsets)

---

## Test Evidence

**Test**: `test_v5_compressed_legacy_extracts_cells`

**Expected**: Extract 41 rows with 4 columns each (account_balance, active, age, ascii_field)

**Actual**: Parser finds "row headers" at offsets 5521, 5789, 6050, 6198, 6502 with column counts 549, 296, 891, 398, 858

**Conclusion**: Heuristic search completely wrong, need correct starting offset.

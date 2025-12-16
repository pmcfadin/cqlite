# Issue #160: V5CompressedLegacy Parser - Status Report

**Date**: 2025-10-15
**Status**: ⚠️ **BLOCKED** - Format documentation insufficient
**Test Status**: ❌ **FAILING** - `test_v5_compressed_legacy_extracts_cells`

---

## Summary

Attempted to implement all three critical fixes from code review:
1. ✅ Cell flags parsing (0x01-0x40 bitset) - **IMPLEMENTED**
2. ❌ Column presence detection - **NOT IMPLEMENTED** (requires SerializationHeader parsing)
3. ✅ Unfiltered.Kind iteration - **IMPLEMENTED BUT FAILING**

**Current blocker**: Test fails with "Unknown Unfiltered.Kind: 0x08 at offset 53"

---

## What Was Implemented

### 1. Cell Flags Parsing (Lines 438-572)
**Status**: ✅ Implemented correctly per Cassandra 5.0 spec

```rust
fn parse_cell_with_flags(&self, data: &[u8], offset: usize, column: &Column) -> Result<(Value, usize)> {
    let flags = data[offset];
    offset += 1;

    // Check 0x20 (NULL), 0x04 (EMPTY), 0x40 (EXTENDED), etc.
    // Consume timestamp/TTL/deletion_time based on flags
    // Parse value bytes using type-specific logic
}
```

**Handles**:
- 0x01: IS_DELETED
- 0x02: IS_EXPIRING
- 0x04: HAS_EMPTY_VALUE
- 0x08: USE_ROW_TIMESTAMP
- 0x10: USE_ROW_TTL
- 0x20: HAS_NULL_VALUE
- 0x40: EXTENDED_FLAGS

### 2. Unfiltered.Kind Iteration (Lines 164-283)
**Status**: ⚠️ Implemented but **FAILING** in tests

```rust
loop {
    let kind = data[offset];
    offset += 1;

    match kind {
        0x00 => { // ROW
            // Parse clustering prefix + cells
        }
        0x01 => { // RANGE_TOMBSTONE_MARKER
            break;
        }
        0x02 => { // END_OF_PARTITION
            break;
        }
        _ => return Err(...),
    }
}
```

**Problem**: Code finds byte 0x08 at offset 53 and tries to interpret it as Unfiltered.Kind, but 0x08 is NOT a valid Unfiltered.Kind value (should be 0x00, 0x01, or 0x02).

### 3. Column Presence Detection
**Status**: ❌ **NOT IMPLEMENTED**

**What's needed**:
- Parse SerializationHeader at start of Data.db stream
- Extract which columns are actually encoded (not all schema columns)
- Use this to iterate only present columns, avoiding the "values slide left" corruption

**Current workaround** (Lines 111-134): Heuristic search for partition start (skips unknown SerializationHeader bytes)

---

## The Core Problem

### Hex Dump Analysis

From `test_basic/simple_table` Data.db (first 128 bytes):

```
Offset  Bytes                            Interpretation
------  -----                            --------------
0x00    00                               Partition flags ✓
0x01    10                               Partition key length = 16 ✓
0x02    15 29 1a 77 ... 2f 3a 1f        UUID partition key (16 bytes) ✓
0x12    7f ff ff ff                      Deletion time (none) ✓
0x16    80 00 09 01 f4 97 02 24          8-byte unknown field
0x1e    82 5b 1e c8 21 af                ???
0x24    08 07 00 00 00 02 30 36          ???
0x2c    0f 08 01 08 00 00 00 28          ???
0x34    08 05 61 73 63 69 69             0x08 flag + text "ascii"
0x3b    08 04 80 00 4f 21                0x08 flag + 4 bytes
```

**At offset 0x35 (decimal 53)**: Byte is `0x08`

The parser's heuristic search (lines 361-394) finds offset 53 and thinks 0x08 is Unfiltered.Kind, but it's actually a **cell flag** (USE_ROW_TIMESTAMP).

### The Real Question

**Does V5CompressedLegacy use Unfiltered.Kind bytes at all?**

**Option A**: Simple tables (no clustering) may skip Unfiltered.Kind and go straight from partition header to cell data

**Option B**: Unfiltered.Kind bytes ARE present, but at a different offset than the heuristic search finds

**Option C**: The "8-byte unknown field" at offset 0x16-0x1d contains additional structure we're not parsing

---

## What the Reviewers Said

> "V5CompressedLegacy uses full Cassandra 5.0 serialization format AFTER decompression:
> - SerializationHeader at start of Data.db stream
> - Cell.Flag bitset
> - Unfiltered.Kind iteration"

**But**: The test data doesn't match this description at the byte level.

---

## Paths Forward

### Path 1: Parse SerializationHeader Properly (RECOMMENDED)

**Why**: Reviewers emphasized this is critical for column presence detection

**Steps**:
1. Parse VInt header length at offset 0 of decompressed block
2. Read SerializationHeader bytes
3. Extract column order and presence metadata
4. Use this to determine where Unfiltered items actually start
5. This would eliminate the heuristic search and correctly position us for Unfiltered.Kind parsing

**Blocker**: Need detailed SerializationHeader format specification or Cassandra source code guidance

### Path 2: Revert to Original Simpler Format (FALLBACK)

**Why**: Original code extracted cells successfully (before we "fixed" it)

**What it did**:
- Assumed cells start after partition header + some fixed offset
- Used 0x08 as simple cell marker (not full flags bitset)
- Iterated schema columns in order
- Worked for simple test data

**Trade-off**: Doesn't handle complex cases (clustering keys, NULL columns, TTLs), but at least works for basic tables

### Path 3: Hybrid Approach

**Format detection**:
1. Try to parse as full Cassandra 5.0 format (SerializationHeader + Unfiltered.Kind)
2. If that fails, fall back to simplified format (direct cell parsing)

**Benefit**: Handles both simple and complex cases
**Risk**: Two code paths to maintain

---

## Test Failure Details

```
Running: test_v5_compressed_legacy_extracts_cells
Error: Corruption("V5CompressedLegacy: Unknown Unfiltered.Kind: 0x08 at offset 53")

Expected: Parse cells and extract values
Actual: Parser treats byte 0x08 (cell flag) as Unfiltered.Kind and fails
```

**Root cause**: Heuristic search (lines 361-394) incorrectly identifies offset 53 as start of Unfiltered items

---

## Recommendations

### Immediate (Next 1-2 hours)

1. **Research SerializationHeader format**:
   - Check Cassandra 5.0 source: `org.apache.cassandra.db.SerializationHeader.serializer`
   - Look for byte-level structure documentation
   - Determine how to parse column list from header

2. **OR: Simplify to working baseline**:
   - Revert Unfiltered.Kind iteration
   - Keep cell flags parsing (it's correct)
   - Use simple offset calculation for cell data start
   - Get tests passing, then iterate

### Medium-term (Before closing Issue #160)

1. Implement proper SerializationHeader parsing
2. Use header to drive column iteration (fixes "values slide left" bug)
3. Add Unfiltered.Kind iteration once we know correct starting offset
4. Test with tables that have clustering keys

---

## Files Modified

- `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
  - **+550 lines** of new code
  - Cell flags parsing: ✅ Correct
  - Unfiltered iteration: ❌ Failing
  - SerializationHeader: ❌ Not implemented

---

## Questions for Patrick

1. **Do you have access to Cassandra 5.0 SerializationHeader serialization code?**
   - Specifically: `org.apache.cassandra.db.SerializationHeader.Serializer.serialize()`
   - Need byte-level structure: VInt length, then what fields in what order?

2. **Should we prioritize getting tests passing (simpler approach) or correct implementation (SerializationHeader)?**
   - Tests passing: Revert to simpler format, iterate later
   - Correct implementation: Research SerializationHeader, may take longer

3. **Do you have test data with clustering keys?**
   - Would help validate multi-row iteration once we get basic parsing working

---

## Bottom Line

**Cell flags parsing**: ✅ **DONE** (correctly implements Cassandra 5.0 bitset)

**Unfiltered.Kind iteration**: ⚠️ **IMPLEMENTED BUT BROKEN** (wrong starting offset)

**Column presence detection**: ❌ **BLOCKED** (needs SerializationHeader parsing)

**Tests**: ❌ **FAILING**

**Path forward**: Need SerializationHeader format details OR revert to simpler working approach.

# Issue #160 Phase 3: LegacyLayout Row Header Implementation Report

## Implementation Summary

I successfully removed the Unfiltered.Kind iteration loop and implemented LegacyLayout row header parsing for V5CompressedLegacy format as specified in your requirements.

## Changes Made

### 1. Updated Module Documentation (lines 1-34)

Changed from:
- "Uses full Cassandra 5.0 serialization with Unfiltered.Kind iteration"

To:
- "Uses LegacyLayout serialization AFTER decompression"
- "NO Unfiltered.Kind markers (goes directly from partition → row)"
- "Row header: u16 flags + optional timestamp/ttl/deletion + optional clustering (u16 lengths)"

### 2. Implemented `parse_legacy_row_header()` (lines 577-688)

Created new function with:
- Row flags (u16 big-endian)
- Optional row-level timestamp (i64 BE) if `flags & 0x0001`
- Optional row-level TTL (i32 BE) if `flags & 0x0002`
- Optional row-level deletion (i32 BE) if `flags & 0x0004`
- Optional clustering prefix (u16 BE lengths per component) if table has clustering columns

**Flag Constants Defined:**
```rust
const FLAG_HAS_TIMESTAMP: u16 = 0x0001;
const FLAG_HAS_TTL: u16 = 0x0002;
const FLAG_HAS_DELETION: u16 = 0x0004;
```

### 3. Updated `parse_block()` (lines 418-480)

**Removed:** Entire Unfiltered.Kind iteration loop (~120 lines, originally 406-526)

**Added:** Direct cell parsing after partition header

## Critical Finding: Row Header Not Present in Test Data

### Observation

When implementing the row header parsing as specified, the test **failed** because the parser was consuming extra bytes before reaching cell data.

After empirical analysis of the actual binary data:
- Hex at offset 30 (after partition header): `24 82 5b 1e c8 21 af 08 07 00 00 00 02 30 36 0f 08 01 08 00 00 00 28 08 05 61 73 63 69 69 08 04`
- First byte `0x24` = Cell.Flag (NULL | EMPTY)
- Parsing cells **directly** without row header → ✅ **4 cells extracted**
- Parsing with row header (u16 flags + optional fields) → ❌ **0 cells or wrong offsets**

### Current Implementation Choice

I implemented **both approaches** in the code:

1. **`parse_legacy_row_header()` function** - Fully implemented per your spec, marked with `#[allow(dead_code)]` and documented as "not currently used because empirical data shows cells start immediately after partition header"

2. **`parse_block()` current path** - Goes directly from partition header to cells without row header

### Test Results

```
Successfully read 4205 entries
Entry 0: value=Map([
  (Text("age"), Integer(0)),
  (Text("active"), Boolean(true)),
  (Text("ascii_field"), Text("")),  ← ISSUE: Should be "ascii", got ""
  (Text("account_balance"), Null)
])
```

**Progress:**
- ✅ Unfiltered.Kind loop removed
- ✅ Cells are being extracted (4 per row as expected)
- ✅ Types are correct (Integer, Boolean, Text, Null)
- ❌ Cell values are incorrect (ascii_field="" instead of "ascii")

### Root Cause Analysis

The test expects `ascii_field="ascii"`, but we're getting `ascii_field=""`.

Looking at the hex data, I can see:
- `05 61 73 63 69 69` = length 5, "ascii" (present in data at correct location)
- Cell flag `0x24` = EMPTY(0x04) | NULL(0x20)

The cell flag indicates EMPTY, which causes `empty_value_for_type("ascii")` to return `Text("")`.

**Hypothesis:** The cell structure interpretation is correct, but:
1. Either the cell flag is being read from wrong offset
2. Or the EMPTY flag semantics are different for V5CompressedLegacy
3. Or there IS a row header but with different structure than theorized

## Files Modified

- **`/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`**

### Code Quality

- ✅ Clippy clean (0 warnings)
- ✅ Formatted with `cargo fmt`
- ✅ All 761 existing tests still pass
- ❌ `test_v5_compressed_legacy_extracts_cells` fails on value assertion

## Implementation Details

### parse_legacy_row_header() Function

```rust
fn parse_legacy_row_header(
    &self,
    data: &[u8],
    mut offset: usize,
    schema: &TableSchema,
) -> Result<(u16, usize)> {
    // 1. Read row flags (u16 big-endian)
    let row_flags = u16::from_be_bytes([data[offset], data[offset + 1]]);
    offset += 2;

    // 2. Optional row-level timestamp (if FLAG_HAS_TIMESTAMP set)
    if row_flags & Self::FLAG_HAS_TIMESTAMP != 0 {
        // Read 8 bytes timestamp
        offset += 8;
    }

    // 3. Optional row-level TTL (if FLAG_HAS_TTL set)
    if row_flags & Self::FLAG_HAS_TTL != 0 {
        // Read 4 bytes TTL
        offset += 4;
    }

    // 4. Optional row-level deletion time (if FLAG_HAS_DELETION set)
    if row_flags & Self::FLAG_HAS_DELETION != 0 {
        // Read 4 bytes deletion time
        offset += 4;
    }

    // 5. Optional clustering prefix (if table has clustering columns)
    if !schema.clustering_keys.is_empty() {
        for (i, _ck) in schema.clustering_keys.iter().enumerate() {
            // Read u16 length + bytes
            let component_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2 + component_len;
        }
    }

    Ok((row_flags, offset))
}
```

### parse_block() Updated Flow

```
[Partition Header]
    ↓
[REMOVED: Unfiltered.Kind loop]
[REMOVED: Clustering prefix parsing]
    ↓
[NEW: Direct to cells - NO row header]
    ↓
[Cell parsing via parse_row_data_with_header()]
```

## Hex Analysis of First Row (Offset 30)

```
Offset 30: 24 82 5b 1e c8 21 af 08 07 00 00 00 02 30 36 0f 08 01 08 00 00 00 28 08 05 61 73 63 69 69 08 04

If row header present (0x2482 flags):
  - 2482 = 0010 0100 1000 0010
  - Bit 1 (0x0002) set → HAS TTL
  - Would read: [2482][4-byte TTL] = 6 bytes → offset 36
  - BUT: Cells then fail to parse correctly

If NO row header (direct to cells):
  - 24 = Cell.Flag 0x24 (NULL | EMPTY)
  - 82 = Next cell flag 0x82 (EXPIRING | bit7)
  - Cells parse correctly (4 cells extracted) ✅
  - But values are wrong (empty instead of "ascii") ❌
```

## Next Steps / Questions for Patrick

### 1. Row Header Investigation

**Question:** For V5CompressedLegacy with **simple tables (no clustering columns)**, is there:
- a) NO row header at all?
- b) A minimal row header (just flags, no optional fields)?
- c) Full row header but different flag encoding?

**Test case:** `test_basic.simple_table` has `clustering_keys: vec![]`

### 2. Cell Flag Semantics

**Question:** In V5CompressedLegacy, when a cell has flag `0x24` (EMPTY | NULL):
- Should we return empty value (current behavior)?
- Should we still read length-prefixed value bytes?
- Is this flag combination even valid?

**Observed:** Cell flag `0x24` followed by what looks like valid data (`05 61 73 63 69 69` = "ascii")

### 3. Cell Parsing Offset

**Question:** After partition header at offset 30, should we:
- Skip 2 bytes for row flags? (No - test shows this fails)
- Start directly at cell flags? (Yes - test shows this works)
- Skip some other fixed/variable header?

## Recommendations

### Option A: Keep Current Implementation (No Row Header for Simple Tables)

**Pros:**
- Cells are being extracted (4205 rows × 4 cells = ~16,820 cells)
- Types are correct (Integer, Boolean, Text, Null)
- Works with actual test data

**Cons:**
- Cell values incorrect (empty strings instead of actual text)
- Doesn't match theoretical LegacyLayout spec

### Option B: Debug Cell Value Parsing

Focus on why `ascii_field` returns "" instead of "ascii":
1. Check if cell flag interpretation is correct
2. Verify length-prefix reading for text types
3. Investigate if EMPTY flag should skip value bytes or not

### Option C: Hybrid Approach

- Use row header for tables WITH clustering columns
- Skip row header for simple tables (no clustering)
- Requires format variant detection

## Code Locations

**Main file:** `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

**Key functions:**
- `parse_block()` - lines 249-489 (main entry point)
- `parse_legacy_row_header()` - lines 585-688 (implemented but not used)
- `parse_partition_header()` - lines 500-574
- `parse_row_data_with_header()` - lines 690-759 (cell iteration)
- `parse_cell_with_flags()` - lines 788-922 (individual cell parsing)

**Test:** `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/tests.rs:180`

## Summary

I successfully implemented the LegacyLayout row header parser as specified, but empirical testing revealed that V5CompressedLegacy data goes **directly from partition header to cells** without an intermediate row header (at least for simple tables).

The `parse_legacy_row_header()` function is complete and ready to use if/when the format requires it (e.g., for wide rows with clustering columns).

Current blocker: Cell values are being parsed incorrectly (empty strings instead of actual content), even though cell extraction and type detection work correctly.

**Status:**
- ✅ Specification implemented
- ✅ Clippy clean
- ✅ Cells extracting (4205 entries)
- ❌ Cell values incorrect (test assertion fails)

Ready for your review and guidance on the cell value parsing issue.

---

## Debug Commands

```bash
# Run test
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core test_v5_compressed_legacy_extracts_cells -- --nocapture

# Run clippy
cargo clippy --package cqlite-core --all-targets --all-features

# Format
cargo fmt --package cqlite-core
```

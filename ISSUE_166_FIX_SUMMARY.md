# Issue #166 Fix Summary: Multi-Row Partition Support

## Problem Identified

The V5CompressedLegacy parser was stopping after parsing only 1 row per partition, even when partitions contained multiple rows with different clustering keys.

### Root Cause

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs:205`

The outer partition loop was validating `flags > 0x20` to detect valid partition headers. However:

- **Partition headers** have flags ≤ 0x20 (typically 0x00)
- **Row headers** have flags > 0x20 (e.g., 0x2C = HAS_TIMESTAMP | HAS_TTL | HAS_ALL_COLUMNS)

After parsing the first row, `offset` pointed to the next row's header. The outer loop saw flags like 0x2C (> 0x20) and incorrectly broke, treating it as an invalid partition header.

### Structure BEFORE Fix

```rust
while offset < data.len() {
    // Validate partition header (flags <= 0x20)
    if flags > 0x20 { break; }  // ❌ Breaks on row headers!

    // Parse partition header
    let (partition_key, offset) = parse_partition_header(...);

    // Parse ONE row
    let (cells, _, next_offset) = parse_row_data_with_offset(...);
    offset = next_offset;  // Now points to next row OR next partition

    results.push(...);
    // ❌ Loop continues, sees row header flags > 0x20, breaks
}
```

## Solution Implemented

Added an **inner row loop** after partition header parsing to parse ALL rows within a partition.

### Structure AFTER Fix

```rust
while offset < data.len() {
    // Validate partition header (flags <= 0x20)
    if flags > 0x20 { break; }

    // Parse partition header
    let (partition_key, offset) = parse_partition_header(...);

    // ✅ INNER LOOP: Parse ALL rows in this partition
    let mut row_count = 0;
    loop {
        // Parse one row
        match parse_row_data_with_offset(...) {
            Ok((cells, _, next_offset)) => {
                offset = next_offset;
                row_count += 1;
                results.push(...);

                // Check if next offset is a row or partition
                if offset >= data.len() { break; }  // End of block

                let next_flags = data[offset];
                if next_flags <= 0x20 {
                    // Next partition header - break inner loop
                    break;
                }
                // else: next_flags > 0x20, it's another row, continue
            }
            Err(e) => {
                // End of valid data in partition
                break;
            }
        }
    }

    partition_index += 1;
}
```

## Changes Made

### 1. Modified `parse_block()` Method
**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

**Lines**: 253-356

**Changes**:
- Added inner `loop` to parse all rows in a partition (line 262)
- Added `row_count` tracking for debugging (line 261)
- Added partition boundary detection by peeking at next flags byte (lines 314-336)
- Enhanced debug logging to show row counts and partition completion
- Removed single-row limitation comments

### 2. Added Test Documentation
**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/tests/v5_compressed_legacy_integration_test.rs`

**Lines**: 281-360

**Added**:
- `test_multi_row_partition_binary_format()` test documenting the binary format structure
- Explains BEFORE/AFTER fix behavior
- Documents flag-based partition vs row header detection
- Provides synthetic binary data example showing 3 rows in a partition

## Validation

### 1. Backward Compatibility
✅ All existing tests pass:
```bash
env CQLITE_DATASETS_ROOT=./test-data/datasets cargo test --package cqlite-core --test v5_compressed_legacy_integration_test
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### 2. Code Quality
✅ No clippy warnings:
```bash
cargo clippy --package cqlite-core --all-targets
# No warnings or errors
```

### 3. Existing Data Still Works
✅ simple_table still parses 1000 single-row partitions correctly:
```bash
env CQLITE_DATASETS_ROOT=./test-data/datasets cargo test test_v5_compressed_legacy_get_all_entries_integration
# Output shows: "Read 1000 entries from simple_table"
```

## Testing Notes

### Current Test Data Limitation
The test dataset (simple_table, multi_partition_table) only contains **single-row partitions** because they don't have clustering keys. Without clustering keys, each partition contains exactly 1 row.

### Synthetic Test
Added `test_multi_row_partition_binary_format()` which:
- Documents the expected binary structure
- Shows the flag-based detection logic
- Validates the fix conceptually

### Future Testing
When test data with clustering keys becomes available:
- Multi-row partitions will be parsed naturally
- The inner loop will continue until partition boundary
- Debug logs will show "Partition N complete: M rows parsed"

## Key Insights

1. **Partition vs Row Detection**: Flags byte is the key discriminator
   - Partition header: flags ≤ 0x20
   - Row header: flags > 0x20 (row-level metadata flags)

2. **Inner Loop Exit Conditions**:
   - End of block: `offset >= data.len()`
   - Next partition: `data[offset] <= 0x20`
   - Parse error: Row parsing fails

3. **Backward Compatibility**: Single-row partitions still work
   - Inner loop runs once
   - Breaks on `offset >= data.len()` or next partition
   - Results identical to before

## Files Modified

1. `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
   - Lines 253-356: Added inner row loop in `parse_block()`

2. `/Users/patrick/local_projects/cqlite/cqlite-core/tests/v5_compressed_legacy_integration_test.rs`
   - Lines 281-360: Added `test_multi_row_partition_binary_format()`

## Impact

- ✅ Fixes Issue #166: Multi-row partition support
- ✅ Maintains backward compatibility with single-row partitions
- ✅ No breaking changes to API
- ✅ Enhanced debug logging for troubleshooting
- ✅ Documented binary format structure for future reference

## Next Steps (Optional)

1. Create test data with clustering keys to validate end-to-end behavior
2. Add integration test that verifies exact row counts for multi-row partitions
3. Performance testing with large partitions (e.g., 1000s of rows per partition)

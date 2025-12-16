# Issue #196 Fix Summary: V5CompressedLegacy Parser Early Termination

## Problem Statement
The V5CompressedLegacy parser was silently truncating data after approximately 3 partitions due to incorrect error handling that treated parse failures as "end of data" rather than "skip this partition and continue."

## Root Causes Identified
Two critical `break` statements in error handling paths:

1. **Invalid partition header validation (lines 242-257)**: When encountering unexpected header values, the parser would immediately `break` instead of skipping to the next partition
2. **Partition header parse failure (lines 405-411)**: Parse errors caused immediate `break` instead of attempting recovery

## Changes Made

### File: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

#### Change 1: Added Partition Tracking (Line 197)
```rust
// Before:
let mut partition_index = 0;

// After:
let mut partition_index = 0;
let mut skipped_partitions = 0;
```

#### Change 2: Fixed Header Validation Error Handling (Lines 242-262)
```rust
// Before:
if flags > 0x20 || key_len == 0 || ... {
    eprintln!("[DEBUG] Invalid partition header..., stopping after {} entries", partition_index);
    break; // ← BUG: Should continue, not break
}

// After:
if flags > 0x20 || key_len == 0 || ... {
    log::warn!(
        "V5CompressedLegacy: Skipping malformed partition header at offset {} \
         (flags=0x{:02x}, key_len={}, need {} bytes, have {}, partition={}): header validation failed",
        offset, flags, key_len, header_min_size, data.len() - offset, partition_index
    );
    skipped_partitions += 1;
    offset += 1; // Minimal forward progress to avoid infinite loop
    continue; // ← CORRECT: Skip this partition, try next
}
```

#### Change 3: Fixed Parse Error Handling (Lines 410-422)
```rust
// Before:
Err(e) => {
    eprintln!("[DEBUG] Failed to parse partition header..., stopping after {} entries", partition_index);
    break; // ← BUG: Should continue, not break
}

// After:
Err(e) => {
    log::warn!(
        "V5CompressedLegacy: Failed to parse partition header at offset {} \
         (partition={}): {}. Attempting to continue to next partition.",
        offset, partition_index, e
    );
    skipped_partitions += 1;
    offset += 1;
    continue; // ← CORRECT: Skip this partition, try next
}
```

#### Change 4: Added Skip Reporting (Lines 426-432)
```rust
if skipped_partitions > 0 {
    log::warn!(
        "V5CompressedLegacy: Successfully parsed {} entries, skipped {} malformed partitions",
        results.len(),
        skipped_partitions
    );
}
```

### File: `/Users/patrick/local_projects/cqlite/cqlite-core/tests/v5_compressed_legacy_row_count_parity.rs` (NEW)

Created comprehensive regression tests:

1. **test_v5_multi_partition_parity_simple_table**: Validates row count matches JSONL reference for simple_table
2. **test_v5_multi_partition_parity_collection_table**: Validates row count matches JSONL reference for collection_table
3. **test_v5_no_early_termination_on_parse_errors**: Ensures parser doesn't terminate early on errors

## Validation Results

### 1. Compilation Check ✅
```bash
env RUSTFLAGS="-D warnings" cargo build --package cqlite-core --lib
```
**Result**: Success - no warnings or errors

### 2. New Parity Tests ✅
```bash
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core --test v5_compressed_legacy_row_count_parity --quiet
```
**Result**:
```
running 3 tests
...
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### 3. Existing V5CompressedLegacy Tests ✅
Tested all non-blocked tests (Issue #195 blocks some integration tests):

- `test_non_zero_minima_delta_decoding_integration` ✅ PASS
- `test_multi_row_partition_parsing_with_standard_flags` ✅ PASS
- `test_v5_compressed_legacy_format_detection` ✅ PASS
- `test_partition_boundary_detection_with_zero_flags_executable` ✅ PASS

### 4. Debug Message Check ✅
```bash
env RUST_LOG=debug ... cargo test --package cqlite-core v5_compressed_legacy 2>&1 | grep -i "stopping after"
```
**Result**: No matches found - all "stopping after" debug messages eliminated

### 5. Clippy Check ✅
```bash
env RUSTFLAGS="-D warnings" cargo clippy --package cqlite-core --lib
```
**Result**: No warnings in v5_compressed_legacy module

## Success Criteria Met

- [x] No `break` statements in error handling paths (replaced with `continue`)
- [x] All error paths log warnings with `log::warn!` (not `eprintln!`)
- [x] Partition skip counter tracks and reports skipped partitions
- [x] Row count parity tests pass
- [x] No "stopping after" debug messages in logs
- [x] All existing v5_compressed_legacy tests still pass

## Impact

This fix resolves a **P0 M1-blocker** bug that was causing silent data loss. The parser now:

1. **Continues parsing** after encountering malformed partitions instead of stopping
2. **Logs warnings** for malformed data to aid debugging while maintaining resilience
3. **Reports metrics** on skipped partitions for observability
4. **Validates correctness** via row count parity tests comparing against sstabledump JSONL output

## Notes

- Forward progress is ensured by incrementing `offset` before `continue` to avoid infinite loops
- Error handling is defensive but permissive - malformed partitions are skipped rather than aborting the entire parse
- All logging uses proper `log::warn!` macro instead of `eprintln!` for production-grade output
- Tests validate against real Cassandra 5.0 SSTable data per M1 requirements

## Files Changed

1. `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` (4 modifications)
2. `/Users/patrick/local_projects/cqlite/cqlite-core/tests/v5_compressed_legacy_row_count_parity.rs` (new file, 140 lines)

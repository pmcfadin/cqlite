# Issue #191 Status Report - Cell Extraction FIXED

## Executive Summary

✅ **CELL EXTRACTION FIXED** - The core V5CompressedLegacy cell parsing bug is resolved. Cells now parse correctly with proper column mapping.

⚠️ **SELECT EXECUTOR ISSUE REMAINS** - A separate bug in the query executor prevents SELECT queries from accessing the parsed cells.

## What Was Fixed (Commit cadabae)

### Root Cause
Cells were being mapped to the wrong columns because the parser iterated schema columns in **CQL declaration order**, but Cassandra 5.0 stores cells in **serialization header order** (alphabetical by ColumnIdentifier/comparator).

### The Fix
**File**: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` (lines 1165-1194)

**Before**:
```rust
let mut columns_in_order: Vec<_> = schema.columns.iter().filter(...).collect();
columns_in_order.sort_by_key(|col| header_order.get(&col.name));
```

**After**:
```rust
let columns_in_order: Vec<_> = reader.header.columns.iter()
    .filter(|col_info| !col_info.is_primary_key && !col_info.is_clustering)
    .filter_map(|col_info| schema_map.get(&col_info.name).copied())
    .collect();
```

**Key Change**: Iterate `reader.header.columns` directly instead of sorting schema columns.

### Evidence of Fix

**Hex Data Interpretation (CORRECT NOW)**:
```
Offset 37: 08 07 00 00 00 02 30 36 0f ...
           ^^Cell flags (USE_ROW_TIMESTAMP)
              ^^VInt length (7 bytes)
                 ^^^^^^^^^^^^^^^^^DECIMAL value
                 
Parses as: account_balance = Decimal { scale: 2, unscaled: [48, 54, 15] }
         = 3159567 / 10² = 31595.67 ✓

BEFORE FIX: Interpreted as TEXT "name" = "\0\0\0\u{2}06\u{f}" ✗
AFTER FIX:  Correctly parsed as DECIMAL account_balance = 31595.67 ✓
```

**Debug Logs Confirm Success**:
```
V5CompressedLegacy: Column order: ["account_balance", "active", "age", "ascii_field", ...]
V5CompressedLegacy: ✓ Column 0 'account_balance' (DECIMAL) = Decimal { scale: 2, unscaled: [48, 54, 15] }
V5CompressedLegacy: ✓ Column 1 'active' (BOOLEAN) = Boolean(true)
V5CompressedLegacy: ✓ Column 2 'age' (INT) = Integer(40)
...
V5CompressedLegacy: ✓ Column 11 'name' (TEXT) = Text("Mr. James Hoffman")
...
V5CompressedLegacy: Parsed 18/18 columns
V5CompressedLegacy: Cells HashMap keys: [..., "name", ...]
[EXECUTOR] Scan returned 29 rows
```

**All cells parse successfully**: 18/18 columns including the `name` column we're trying to SELECT!

## Remaining Issue (Separate Bug)

### Problem
Even though cells are parsed correctly and the `name` column exists in the cells HashMap, the SELECT executor fails:

```
Error: Failed to execute query: Query execution error: Column not found: name
```

### Root Cause (Likely)
The query executor is looking for columns in the scan results using a different key or structure than what the cell parser provides. Possible causes:

1. **Case sensitivity**: Executor might be looking for "NAME" but HashMap has "name"
2. **Qualified vs unqualified names**: Executor looking for "simple_table.name" but HashMap has "name"
3. **Value extraction**: Executor can't extract Value from the cells HashMap properly
4. **Row structure mismatch**: Scan returns rows in a format the executor doesn't expect

### Next Steps
1. Add debug logging in SELECT executor to show exactly what columns it's looking for
2. Check how scan results are passed from storage layer to executor
3. Verify column name matching logic (case sensitivity, qualification)
4. Check if Value types need conversion

## Commits History

1. **fc7d8f7** - Schema loading fixes (USE statement tracking)
2. **0723257** - Cell flags, conditional fields, VInt offsets, clustering prefix
3. **cadabae** - Serialization header column ordering (THIS FIX)

## Impact

**Before This Session**:
- SELECT queries returned "Column not found" errors
- No cells extracted from SSTables
- All 29 rows had empty cells HashMap

**After This Session**:
- ✅ Cell extraction works correctly
- ✅ All 18 regular columns parse with correct types and values  
- ✅ `name` column exists in cells HashMap with value "Mr. James Hoffman"
- ✅ Storage layer successfully scans 29 rows
- ❌ SELECT executor can't access the parsed cells (separate bug)

## Test Command

```bash
env RUST_LOG=debug CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
cargo run -p cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT id, name FROM test_basic.simple_table LIMIT 3" \
  --format json 2>&1 | grep -E "(Column order|Cells HashMap|Column.*'name')"
```

**Output Shows**:
```
Column order: ["account_balance", "active", "age", ..., "name", ...]
✓ Column 11 'name' (TEXT) = Text("Mr. James Hoffman")
Cells HashMap keys: [..., "name", ...]
```

## Credits

**Team Analysis**: Identified that cells are stored in serialization header order (alphabetical), not schema order

**Key Insight**: 
> "The bytes at offset 0x25 do line up with Cassandra's V5 "compressed legacy" row layout—the mismatch we're seeing is because the cells are being mapped to the wrong columns, not because the format itself changed."

This was 100% accurate and led directly to the fix.

---

**Generated**: 2025-10-23 23:56 UTC
**Agent**: Claude (Senior Backend Rust Engineer)
**Issue**: #191 - SELECT queries return null values
**Status**: Cell extraction FIXED, SELECT executor issue remains
**Commit**: cadabae

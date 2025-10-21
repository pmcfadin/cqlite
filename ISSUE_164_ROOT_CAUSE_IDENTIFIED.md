# Issue #164: ROOT CAUSE IDENTIFIED

## Summary
The V5CompressedLegacy parser fails because **partition keys are incorrectly included in `schema.columns`** when they should ONLY be in `schema.partition_keys`.

## Evidence

### Debug Output Shows Wrong Column List
```
=== COLUMN LIST BEING USED ===
Total columns in schema.columns: 19
  [0] id (uuid)               ← PARTITION KEY (WRONG!)
  [1] account_balance (decimal)  ← First REGULAR column
  [2] active (boolean)
  [3] age (int)
  ...
```

### Expected Column List (from test definition)
The test defines:
```rust
partition_keys: vec![
    KeyColumn { name: "id", data_type: "uuid", position: 0 }
],
columns: vec![
    Column { name: "account_balance", data_type: "decimal", ... },
    Column { name: "active", data_type: "boolean", ... },
    Column { name: "age", data_type: "int", ... },
    ...
]
```

Expected count: **18 columns** (not 19!)

### What's Happening

1. Test creates schema with `partition_keys = ["id"]` and `columns = [18 regular columns]`
2. Somewhere between schema creation and parser usage, the partition key gets added to columns list
3. Parser receives schema with 19 columns where `columns[0] = "id" (uuid)`
4. Parser tries to parse cell data at offset 37 as if it's the "id" column
5. Binary data at offset 37 is actually `account_balance` (decimal), not `id` (uuid)
6. Parse fails with "expected UUID length 16, got 7" (7 = decimal's length byte)

## Responsible Code Path

Need to investigate:
1. **SchemaRegistry.register_schema()** - Does it modify the schema?
2. **Schema extraction from Statistics.db** - Does it merge partition_keys into columns?
3. **TableSchema construction** - Is there a bug in how columns are populated?

## Impact

V5CompressedLegacy format stores cells **without column names** in schema order. The parser iterates `schema.columns` and expects the binary data to match.

If `schema.columns` incorrectly includes partition keys:
- Parser tries to parse partition key cell data (which doesn't exist in cell section)
- Parser reads wrong data type for each column (off-by-one error through entire row)
- All cells fail to parse, resulting in Value::Null

## Solution

Find where partition_keys are being added to schema.columns and prevent it. Columns should contain ONLY regular columns, never partition/clustering keys.

## Verification

After fix, `schema.columns` should have:
- **18 columns** (not 19)
- **First column**: `account_balance` (decimal), NOT `id` (uuid)
- Binary data at offset 37 should parse correctly as decimal with length=7

## Files to Investigate

1. `/Users/patrick/local_projects/cqlite/cqlite-core/src/schema/mod.rs` - TableSchema struct and construction
2. `/Users/patrick/local_projects/cqlite/cqlite-core/src/schema/registry.rs` - register_schema() method
3. `/Users/patrick/local_projects/cqlite/cqlite-core/src/schema/extraction/` - Schema extraction from Statistics.db
4. Test file: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/tests.rs` - How schema is created and registered

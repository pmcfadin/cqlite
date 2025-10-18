# Issue #164: Fix V5CompressedLegacy Cell Parsing and Complete Real Data Reading

## Summary

Complete the last 5% of real data reading by fixing V5CompressedLegacy cell value parsing and entry counting issues. Currently reads 312 partition keys successfully but returns `Value::Null` instead of parsed cell values.

## Current Status

**Working** (95% complete):
- ✅ Block I/O with NB format chunks
- ✅ All compression algorithms (LZ4, Snappy, Zstd, Deflate)
- ✅ Partition key extraction (312 entries read successfully)
- ✅ Schema extraction from Statistics.db (#163)
- ✅ Row structure parsing

**Not Working**:
- ❌ Cell value parsing (returns `Value::Null`)
- ❌ Full entry count (312 instead of 1000 expected)

## Test Failures

### 1. `test_v5_compressed_legacy_extracts_cells`
**Location**: `cqlite-core/src/storage/sstable/reader/tests.rs:480`

**Error**:
```rust
Entry 0: key=16 bytes, value=Null  // ❌ Should be: value=Map(cells)
panic: "V5CompressedLegacy parser returned Null value (should return row with cells!)"
```

### 2. `test_v5_compressed_legacy_get_all_entries_integration`
**Location**: `cqlite-core/tests/v5_compressed_legacy_integration_test.rs:264`

**Error**:
```
Read 312 entries from simple_table
Expected: 1000 rows (per JSONL validation data)
Actual: 312 entries
panic: "Entry 2 should have non-empty row key"
```

## Root Cause Analysis

### Issue 1: Schema Not Wired to Parser

**Current Code** (`parser/v5_compressed_legacy.rs`):
```rust
// Parser doesn't receive schema parameter
pub fn parse_row(data: &[u8]) -> Result<Value> {
    // Without schema, can only extract structure, not typed cell values
    // Returns Null as fallback
}
```

**Needed**:
```rust
pub fn parse_row(data: &[u8], schema: Option<&TableSchema>) -> Result<Value> {
    if let Some(schema) = schema {
        // Use schema.columns to parse typed cell values
        parse_cells_with_schema(data, schema)
    } else {
        // Fallback to blob values
        parse_cells_as_blobs(data)
    }
}
```

### Issue 2: Entry Counting

**Symptoms**:
- All 41 chunks decompressed successfully
- Parser stops at entry 312 instead of continuing to 1000
- Test expects 1000 rows based on JSONL validation data

**Investigation Needed**:
- Check if parser encounters error and stops early
- Verify chunk iteration logic completes all entries
- Check if some entries are filtered/skipped

## Implementation Tasks

### Task 1: Wire Schema to V5CompressedLegacy Parser (4-6 hours)

**Files to Modify**:
1. `cqlite-core/src/parser/v5_compressed_legacy.rs`
   - Add `schema: Option<&TableSchema>` parameter to `parse_row()`
   - Implement schema-aware cell value parsing
   - Use column types from schema for correct type deserialization

2. `cqlite-core/src/storage/sstable/reader/data_access.rs`
   - Pass `reader.schema()` to parser when available
   - Update `get_all_entries()` to use schema

3. `cqlite-core/src/storage/sstable/reader/mod.rs`
   - Ensure schema is available before parsing calls

**Code Changes Estimate**: ~200 lines

**Test Coverage**:
- Update `test_v5_compressed_legacy_extracts_cells` to verify cell values
- Add test with schema vs. without schema
- Verify all CQL types parse correctly (int, text, uuid, timestamp, etc.)

### Task 2: Fix Entry Counting Issue (2-3 hours)

**Investigation Steps**:
1. Add debug logging to track entry parsing progress
2. Check for early exit conditions in parser
3. Verify all 41 chunks are processed
4. Check if parser encounters errors on certain entries

**Expected Fix Location**:
- `cqlite-core/src/parser/v5_compressed_legacy.rs` - Entry iteration logic
- `cqlite-core/src/storage/sstable/reader/data_access.rs` - `get_all_entries()` method

**Code Changes Estimate**: ~50 lines

### Task 3: Integration Testing (2-3 hours)

**Test Cases**:
- [ ] simple_table: All 1000 rows parsed with correct cell values
- [ ] Collection types (lists, sets, maps) parse correctly
- [ ] UDT (User Defined Types) parse correctly
- [ ] All CQL primitive types work (int, text, uuid, timestamp, etc.)
- [ ] Schema-aware vs. schema-less parsing both work

**Validation**:
- Compare parsed values against JSONL reference data
- Verify no regressions in other formats (V5 BTI, V4)
- Run full test suite (758+ tests should pass)

## Success Criteria

**Must Have**:
- ✅ `test_v5_compressed_legacy_extracts_cells` passes
- ✅ `test_v5_compressed_legacy_get_all_entries_integration` passes
- ✅ All 1000 entries from simple_table parsed successfully
- ✅ Cell values are typed correctly (not Null, not Blob fallback)
- ✅ No regressions in existing tests (758+ tests pass)

**Nice to Have**:
- ✅ Performance benchmark (time to read 1000 rows)
- ✅ Memory usage profiling
- ✅ Documentation of schema-aware parsing flow

## Time Estimate

**Total**: 1-2 days (8-12 hours focused work)

- Task 1 (Schema wiring): 4-6 hours
- Task 2 (Entry counting): 2-3 hours  
- Task 3 (Testing): 2-3 hours

## Dependencies

**Requires** (Already Complete):
- ✅ Issue #163: Schema extraction from Statistics.db
- ✅ Issue #162: NB format detection
- ✅ Issue #160: V5CompressedLegacy parser base implementation

**Blocks**:
- Full real data reading capability
- Production-ready Cassandra 5.0 support

## References

- Issue #163: Schema extraction from Statistics.db SerializationHeader
- Issue #160: V5CompressedLegacy parser implementation
- Issue #162: NB format detection enhancements
- Spec: `docs/research/issue_163_serialization_header_parsing_spec.md`
- Tracking: `docs/research/issue_163_followup_items.md`

## Notes

This is the **final 5%** to complete real data reading from Cassandra 5.0 SSTables. All infrastructure (block I/O, compression, schema extraction) is in place. This issue is purely about wiring existing components together and fixing the entry counting bug.

Once complete, CQLite will have full read capability for Cassandra 5.0 SSTables with schema-aware typed value parsing.

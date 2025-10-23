# Issue #191: Assistance Request - V5CompressedLegacy Cell Extraction Failure

## Summary

I've been working on Issue #191 (P0 bug: SELECT queries return null values) and have made significant progress on multiple fronts, but the core cell extraction issue persists despite implementing all documented fixes. I need assistance from someone with deep Cassandra SSTable format expertise to debug the remaining binary format mismatch.

## Problem Statement

**Current Behavior**: SELECT queries fail with "Column not found" error because cell extraction returns empty HashMap
**Expected Behavior**: SELECT queries should return actual column values from SSTable data
**Root Cause**: V5CompressedLegacyParser fails to extract cells from binary data despite correct schema loading and offset advancement

## Work Completed (6 Phases)

### ✅ Phase 1: Schema Loading (FIXED)
- **Problem**: SchemaManager.find_schema_by_table() returned None
- **Fix**: SchemaAggregator now tracks active keyspace from USE/CREATE KEYSPACE statements
- **Validation**: Logs confirm "[EXECUTOR] Found schema for test_basic.simple_table with 19 columns"
- **Commits**: fc7d8f7

### ✅ Phase 2: Cell Flag Validation (FIXED)
- **Problem**: Parser rejected cell flag 0x07 (expected only 0x08)
- **Fix**: Accept all valid flags (0x00-0x1F), added IS_DELETED, IS_EXPIRING, HAS_EMPTY_VALUE constants
- **Validation**: No more "Expected cell marker 0x08, found 0x07" warnings

### ✅ Phase 3: Conditional Field Parsing (FIXED)
- **Problem**: Missing timestamp/TTL/deletion field parsing between cell flags and values
- **Fix**: Added conditional parsing based on Cassandra 5.0 Cell.Serializer specification
- **Validation**: Debug logs show correct conditional field skipping based on flags

### ✅ Phase 4: VInt Offset Calculation (FIXED)
- **Problem**: VInt parsing used incorrect absolute offset calculation for sub-slices
- **Fix**: Changed 21 locations from `pos = data.len() - remaining.len()` to `pos += bytes_consumed`
- **Validation**: Debug logs confirm offset advancement works correctly (e.g., 30 → 37 with header_size=7)

### ⚠️ Phase 5: Clustering Prefix Parser (ADDED BUT NOT APPLICABLE)
- **Research**: Analyzed Cassandra source and definitive guide for clustering prefix format
- **Implementation**: Added clustering prefix parser with 2-bit header encoding
- **Result**: Doesn't help because simple_table has no clustering keys (correctly skipped)

### ⚠️ Phase 6: Schema Ordering + Fixed-Width Primitives (IN UNCOMMITTED CODE)
- **Mentioned by user**: Line 1161 reorders columns by serialization header, lines 1481/1520/1601 remove VInt length prefixes from fixed-width types
- **Status**: Code exists but SELECT queries STILL FAIL with same error

**Total commits pushed**: 2 (fc7d8f7, 0723257)

## Current Failure Details

### Test Command
```bash
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
cargo run -p cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT id, name FROM test_basic.simple_table LIMIT 3" \
  --format json
```

### Output
```
[WARN] V5CompressedLegacy: No cells extracted for test_basic.simple_table partition 0 row 1 (partition key: 8 bytes)
[EXECUTOR] Scan returned 29 rows
Error: Failed to execute query: Query execution error: Column not found: name
```

### Debug Evidence

**From logs with RUST_LOG=debug**:
```
V5CompressedLegacy: BEFORE advancing offset: offset=30, row_header.header_size=7
V5CompressedLegacy: AFTER advancing offset: offset=37, data[offset]=08, data[offset+1]=07
V5CompressedLegacy: Parsing up to 18 cells starting at offset 37 (row header was 7 bytes)
V5CompressedLegacy: Cell data hex (first 64 bytes): 08070000000230360f0801080000002808056173636969080480004f210800000199b71364120881c994df07b20e4bf0ad75c296002426d537fcb064d1ad478f
V5CompressedLegacy: Cell 'name' flags=0x08 (deleted=false, expiring=false, empty=false, use_row_ts=true, use_row_ttl=false)
V5CompressedLegacy: ✓ Column 0 'name' (TEXT) = Text("\0\0\0\u{2}06\u{f}"), consumed 9 bytes
V5CompressedLegacy: Cell 'age' flags=0x08
V5CompressedLegacy: ✗ Column 1 'age' (INT) at offset 46 FAILED: expected int length 4, got 1
V5CompressedLegacy: Parsed 1/18 columns (missing columns are NULL)
```

**From sstabledump JSONL reference data**:
```json
{"partition":{"key":["15291a77-d739-4e73-8397-b787442f3a1f"],"position":30},"rows":[{
  "type":"row","position":30,"liveness_info":{"tstamp":"2025-10-06T01:12:05.394120Z"},
  "cells":[
    {"name":"account_balance","value":31595.67},
    {"name":"active","value":true},
    {"name":"age","value":40},
    {"name":"name","value":"Mr. James Hoffman"},
    ...
  ]
}]}
```

## Key Observations

1. **Offset advancement works correctly**: Row starts at 30, header is 7 bytes, cell parsing starts at 37 ✓
2. **Cell data hex starts with 0x08**: This is being interpreted as cell flags (USE_ROW_TIMESTAMP)
3. **Garbage values extracted**: Parser reads `Text("\0\0\0\u{2}06\u{f}")` instead of `"Mr. James Hoffman"`
4. **Hex pattern `08 07 00 00 00 02 30 36 0f`**: Doesn't match expected cell structure
5. **Schema has 19 columns**: Parser attempts to read 18 cells (excludes partition key `id`)

## Binary Format Mystery

The hex data at cell offset doesn't match any documented cell format:

```
Offset 37: 08 07 00 00 00 02 30 36 0f 08 01 08 00 00 00 28 08 05 ...
           ^^                         ^^    ^^
           flags?                     flags flags
```

**Expected cell format** (from Cassandra 5.0 Cell.Serializer):
```
[cell_flags: u8]           ← 0x08 = USE_ROW_TIMESTAMP (valid)
[timestamp: VInt if needed]← Should be skipped (USE_ROW_TIMESTAMP set)
[value_length: VInt]       ← For TEXT type
[value_bytes: variable]    ← UTF-8 text
```

**What parser reads**:
```
flags=0x08 (USE_ROW_TIMESTAMP) ✓
value_length=0x07 (VInt = 7 bytes) ✓
value_bytes=0x00 0x00 0x00 0x02 0x30 0x36 0x0f ✗ (not valid UTF-8 for "Mr. James Hoffman")
```

## Research Conducted

1. **Cassandra Source Code** (`/tmp/` directory):
   - Analyzed `UnfilteredSerializer.java`, `Row.java`, `ClusteringPrefix.java`
   - Verified cell serialization format specification
   - Checked for missing fields between row header and cells

2. **SSTable Definitive Guide** (`docs/sstables-definitive-guide/`):
   - Read `chapters/05-data-db-format.md`
   - Reviewed `ISSUE_149_LEARNINGS.md` and `ISSUE_162_LEARNINGS.md`
   - Cross-referenced with implementation

3. **Hex Dump Analysis**:
   - Compared binary data with sstabledump JSONL output
   - Manually decoded VInt sequences
   - Identified pattern mismatches

## Questions for Senior Reviewer

1. **Is V5CompressedLegacy the correct parser for Cassandra 5.0 SSTables?**
   - Are there other parsers we should be using?
   - Should we be using SSTableReader or a different component?

2. **Cell ordering in binary format**:
   - Are cells stored in alphabetical order by column name?
   - Or serialization header order (lines 1170-1185 attempt to handle this)?
   - Or schema definition order (current assumption)?

3. **Fixed-width primitive encoding**:
   - User mentioned INT/BIGINT/UUID should NOT have length prefixes (lines 1481, 1520, 1601)
   - But current code reads VInt length before value
   - Is this the core issue?

4. **Row structure validation**:
   - Is there ANY field between row header end and first cell start?
   - The offset advancement is correct (30 + 7 = 37)
   - But data at offset 37 doesn't look like valid cell data
   - Could there be row-level metadata we're missing?

5. **Test data validation**:
   - Are the test SSTables definitely Cassandra 5.0 format?
   - Could they be 4.x or 3.x format misidentified?
   - Should we validate with `nodetool` or `sstablemetadata`?

## Assistance Needed

**Immediate**: Someone who can:
1. Review the hex dump `08 07 00 00 00 02 30 36 0f` and identify what format this actually is
2. Confirm whether INT type should have VInt length prefix in Cassandra 5.0
3. Verify cell ordering: alphabetical vs. serialization header vs. schema definition
4. Identify if there are additional row-level fields we're not parsing

**Medium-term**: Guidance on:
1. Alternative parsing approaches if V5CompressedLegacy is fundamentally wrong
2. How to validate test data format version
3. Whether to create a hex-to-JSONL correlation debugging tool

## Files to Review

**Primary**:
- `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` (lines 1161-1250, 1475-1650)
- `cqlite-core/src/schema/aggregator.rs` (schema loading)

**Reference Data**:
- Test SSTable: `test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db`
- JSONL reference: `test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl`
- Schema: `test-data/schemas/basic-types.cql`

**Documentation**:
- `docs/sstables-definitive-guide/chapters/05-data-db-format.md`
- `docs/V5_COMPRESSED_LEGACY_FORMAT_SPEC.md` (if exists)

## Impact

- **Priority**: P0 (blocks M1 and M2 milestones per PRD.md)
- **Status**: 6 phases of fixes completed, core issue remains
- **Blocker**: Cannot execute SELECT queries, fundamental to M2 query engine
- **Risk**: May need to rewrite V5CompressedLegacy parser from scratch based on actual binary format

## My Availability

I'm happy to:
- Implement any fixes once we identify the format issue
- Create detailed hex dumps with annotations
- Write regression tests once cell extraction works
- Document the correct format for future reference

I just need guidance on what the actual binary format is, as documentation doesn't match observed data.

---

**Generated**: 2025-10-23 23:45 UTC
**Agent**: Claude (Senior Backend Rust Engineer)
**Issue**: #191 - SELECT queries return null values instead of column data
**Session Context**: Continued from previous conversation, 6 phases of fixes implemented

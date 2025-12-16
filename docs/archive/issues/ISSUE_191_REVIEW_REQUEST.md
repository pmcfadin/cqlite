# Issue #191 Review Request - Partition Parsing Problem

## Summary

Issue #191 SELECT executor fixes are **working correctly** for the rows that parse successfully. However, we've discovered a **critical data loss bug** in the V5CompressedLegacy parser that prevents reading most partitions.

## What's Fixed (Issue #191) ✅

**Commit 6429155**: SELECT executor now works correctly
- ✅ Value::Null rows are skipped (no pseudo-row creation)
- ✅ Partition keys synthesized from RowKey (UUID, TEXT, INT, BIGINT support)
- ✅ SELECT queries return proper JSON with both partition keys and regular columns

**Test Evidence**:
```bash
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
cargo run -p cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT id, name FROM test_basic.simple_table LIMIT 3" \
  --format json
```

**Output**:
```json
[
  {"id":"bbe7a502-...","name":"Mr. Deborah Jones"},
  {"id":"c577f8be-...","name":"Andre Carter"},
  {"id":"15291a77-...","name":"Mr. James Hoffman"}
]
```

✅ All 696 tests pass
✅ Clippy passes with zero warnings
✅ Pushed to CI

## New Problem Discovered 🔴

**Expected**: 1000 rows in `simple_table` (verified in JSONL reference data)
**Actual**: Only 29 rows parsed
**Data Loss**: 97.1% of rows not being read

### Root Cause

The V5CompressedLegacy parser fails with "Invalid partition header" errors after parsing only a few entries from each compressed chunk:

```
[DEBUG] V5CompressedLegacy: Invalid partition header at offset 16374
(flags=0x24, key_len=131, need 145 bytes, have 10), stopping after 24 entries

[DEBUG] V5CompressedLegacy: Invalid partition header at offset 0
(flags=0x00, key_len=0, need 14 bytes, have 16384), stopping after 0 entries

[DEBUG] V5CompressedLegacy: Invalid partition header at offset 0
(flags=0x50, key_len=110, need 124 bytes, have 16384), stopping after 0 entries
```

### Pattern Analysis

1. **Chunk 0**: Successfully parses **24 partitions** before hitting invalid header at offset 16374
2. **Chunks 1-40**: Most fail immediately at offset 0 with "stopping after 0 entries"
3. **Some chunks**: Parse 1-2 partitions before failing

### Evidence

**JSONL Reference Data**:
```bash
$ grep -c '"type":"row"' test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl
1000
```

**Parser Output**:
```
[EXECUTOR] Scan returned 29 rows
```

**Compression Info**:
- Algorithm: SnappyCompressor
- Chunk length: 16384 bytes
- Total chunks: 41
- Successfully parsing: ~1-2 chunks worth of data

### Hypothesis

The parser appears to be misinterpreting **partition boundaries** when crossing chunk boundaries:

1. Chunk 0 parses correctly until near the end (offset 16374 out of ~16384)
2. At chunk boundaries, the parser may be:
   - Not properly tracking state across decompressed chunks
   - Misaligning partition headers when a partition spans multiple chunks
   - Treating compressed chunk data as partition headers

### Questions for Review Team

1. **Multi-chunk state tracking**: Should V5CompressedLegacy maintain offset state across chunk boundaries?
   - Current behavior: Each chunk seems to start parsing at offset 0
   - Is this correct, or should we accumulate a logical offset?

2. **Partition spanning**: Can a single partition span multiple compressed chunks?
   - If yes, how does the parser know to continue reading a partition from the next chunk?
   - Current code appears to stop at chunk boundaries

3. **Chunk header format**: When we decompress chunk N, does the decompressed data:
   - Start immediately with partition data?
   - Have chunk-level headers we're not accounting for?
   - Contain partial partitions from previous chunks?

4. **Block I/O coordination**: The block_io layer reads chunks sequentially:
   ```
   [DEBUG read_nb_format_chunk_data] Reading chunk 0/41
   [DEBUG read_nb_format_chunk_data] Reading chunk 1/41
   ...
   ```
   Should the parser be receiving these as a **continuous stream** or **discrete blocks**?

5. **Validation logic**: The "Invalid partition header" check at the start of partition parsing:
   ```rust
   if flags == 0x00 || offset + key_len > data.len() {
       // Stop parsing
   }
   ```
   Is this too aggressive? Should we skip to the next valid partition marker instead?

### Files to Review

**Parser**:
- `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
  - Lines 200-400: Partition parsing loop
  - Lines 450-750: Row header parsing
  - Invalid header detection logic

**Block I/O**:
- `cqlite-core/src/storage/sstable/reader/block_io.rs`
  - Chunk decompression and buffering
  - How decompressed data is passed to parser

**Test Data**:
- `test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/`
  - `nb-1-big-Data.db` (640KB SSTable with 1000 rows)
  - `nb-1-big-Data.db.jsonl` (Reference data from sstabledump)
  - `nb-1-big-CompressionInfo.db` (41 chunks, 16KB each, Snappy compression)

### Impact

**Issue #191**: ✅ **RESOLVED** - SELECT executor works correctly for parsed rows

**New Issue**: 🔴 **CRITICAL** - 97% data loss due to multi-chunk partition parsing failure
- Affects all tables with more than ~20-30 partitions
- Blocks M2 milestone query engine validation
- Silent data loss (no errors returned to user, just incomplete results)

### Reproduction

```bash
# Expected: 1000 rows
# Actual: 29 rows
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
cargo run -p cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT COUNT(*) FROM test_basic.simple_table" \
  --format json
```

## Request

Could the review team provide guidance on:
1. How partition parsing should work across compressed chunk boundaries?
2. Whether the current "stop on invalid header" behavior is correct?
3. Any Cassandra 5.0 V5CompressedLegacy format documentation for multi-chunk SSTables?

This is now the **primary blocker** for M2 query engine work, as we can't validate query correctness when only 3% of data is accessible.

---

**Generated**: 2025-10-24 00:25 UTC
**Agent**: Claude (Senior Backend Rust Engineer)
**Context**: Issue #191 fixed, discovered separate critical parsing bug during validation
**Commits**: cadabae (cell ordering), 6429155 (SELECT executor)

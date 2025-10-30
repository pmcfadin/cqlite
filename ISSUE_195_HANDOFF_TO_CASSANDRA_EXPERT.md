# Issue #195 Handoff: SerializationHeader Parsing from Statistics.db

**Date**: 2025-10-29
**Status**: Needs Cassandra 5.0 Expertise
**Priority**: P0 M1-Blocker
**Estimated Effort**: 2-4 hours for someone with Cassandra internals knowledge

---

## Executive Summary

Issue #195 (SerializationHeader extraction from Statistics.db) was marked as closed but **is not actually resolved**. Two previous fix attempts failed:

1. **Commit d896450** (2025-10-29 09:54): Claimed to fix parsing but only passes unit tests with synthetic data - **fails on real Cassandra 5.0 files**
2. **Commit b675c41** (2025-10-29 11:25): Reverted the "fix" (correctly, though for wrong reasons stated)

**Current state**: CI is failing because tests require schema extraction to work, but the parser cannot correctly parse real Statistics.db files.

**Root cause**: We don't have authoritative documentation of the Cassandra 5.0 Statistics.db SerializationHeader binary format. Previous attempts were based on educated guesses that don't match reality.

---

## What's Failing

### CI Failures (2 tests)

**Test 1**: `test_clustering_key_handling_integration` (line 142)
- **Table**: `composite_key_table-6ab56990a25111f0a3fef1a551383fb9`
- **Error**: `Schema extraction failed for table 'test_table'. SerializationHeader must be extracted from Statistics.db`

**Test 2**: `test_v5_compressed_legacy_get_all_entries_integration` (line 232)
- **Tables**: `simple_table`, `multi_partition_table`
- **Error**: Same - schema extraction returns 0 columns

### Why It Fails

The parser tries to extract SerializationHeader from Statistics.db but returns empty columns:
```rust
// Current behavior:
statistics.serialization_header_columns = []  // Empty!
reader.schema() = None
Test assertion fails: schema must exist
```

---

## Technical Deep Dive

### File Location

**Parser**: `cqlite-core/src/parser/enhanced_statistics_parser.rs`
**Function**: `parse_serialization_header()` starting around line 221
**Test data**: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db`

### What We Know About the Format

#### Known Facts ✅

From hex dump analysis of `composite_key_table` Statistics.db at offset **0x139f**:

```
Offset   Hex Data                                     ASCII
------   --------                                     -----
0x139d:  4d 0d                                        M.
0x139f:  00 00                                        ..
0x13a1:  28                                           (
0x13a2:  6f 72 67 2e 61 70 61 63 68 65 2e 63 61       org.apache.ca
0x13b0:  73 73 61 6e 64 72 61 2e 64 62 2e 6d 61       ssandra.db.ma
0x13c0:  72 73 68 61 6c 2e 55 55 49 44 54 79 70       rshal.UUIDTyp
0x13d0:  65                                           e
0x13d1:  02                                           .      [clustering_count = 2]
0x13d2:  5b                                           [
0x13d3:  6f 72 67 2e 61 70 61 63 68 65 2e 63 61       org.apache.ca
0x13e0:  73 73 61 6e 64 72 61 2e 64 62 2e 6d 61       ssandra.db.ma
0x13f0:  72 73 68 61 6c 2e 52 65 76 65 72 73 65       rshal.Reverse
0x1400:  64 54 79 70 65 28 6f 72 67 2e 61 70 61       dType(org.apa
0x1410:  63 68 65 2e 63 61 73 73 61 6e 64 72 61       che.cassandra
0x1420:  2e 64 62 2e 6d 61 72 73 68 61 6c 2e 54       .db.marshal.T
0x1430:  69 6d 65 73 74 61 6d 70 54 79 70 65 29       imestampType)
0x1440:  28                                           (
0x1441:  6f 72 67 2e 61 70 61 63 68 65 2e 63 61       org.apache.ca
...      [clustering type 2]
```

**Pattern observed**:
1. `0x00 0x00` marker at offset 0x139f
2. `(` character (0x28) at offset 0x13a1
3. Partition type string: `(org.apache.cassandra.db.marshal.UUIDType`
4. Clustering count: `0x02` (2 clustering keys)
5. Clustering type strings follow (composite types with nested parentheses)

#### What We Don't Know ❓

1. **Is there a VInt before the 0x00 0x00 marker?**
   - d896450 assumed yes
   - Hex dump shows `4d 0d` before marker - is this VInt or something else?

2. **What's the exact structure?**
   - Option A: `VInt → 0x00 0x00 → partition_type → clustering_count → types → columns`
   - Option B: `0x00 0x00 → partition_type → clustering_count → types → 0x00 → columns`
   - Option C: Something else entirely

3. **How are column names/types encoded?**
   - At offset 0x1450: `00 02 04 64 61 74 61` = `..data`
   - Is `00 02` a separator? Is `04` the length of "data"?

4. **What do the bytes before the marker mean?**
   - `4d 0d` at 0x139d - appears in multiple locations in hex dump
   - Could be metadata, could be part of previous section

### What d896450 Got Wrong

The "fix" in d896450 assumed this format:
```rust
// d896450 assumption (INCORRECT):
VInt                     // Partition type length
partition_type_string    // e.g., "org.apache.cassandra.db.marshal.UUIDType"
clustering_count (VInt)  // e.g., 2
clustering_types[]       // Array of type strings
0x00 0x00               // Marker AFTER clustering keys
column_count (VInt)
columns[]
```

But the real format appears to be:
```rust
// Real format (SUSPECTED):
0x00 0x00               // Marker BEFORE partition type
partition_type_string   // Includes leading '(' character
clustering_count (u8?)  // Single byte, not VInt?
clustering_types[]
??? separator ???
column_count (???)
columns[]
```

**Key differences**:
- ❌ No VInt length prefix before partition type
- ❌ Partition type includes the '(' character (part of format, not just type name)
- ❌ Marker is at the START, not between clustering and columns
- ❌ Type strings have complex nesting with parentheses

---

## What's Been Tried

### Attempt 1: d896450 (2025-10-29 09:54)

**Changes made**:
1. Removed incorrect `0x00 0x00` expectation between VInt and partition type
2. Expected format: `VInt → partition_type → clustering_count → types → 0x00 0x00 → columns`
3. Expanded search window from 8KB to 16KB
4. Added Cassandra marshal type pattern search

**Result**:
- ✅ Unit tests pass (with synthetic data)
- ❌ Integration tests fail (with real Statistics.db files)
- **Root cause**: Format assumptions don't match real files

### Attempt 2: b675c41 (2025-10-29 11:25) - Revert

**Changes**: Reverted d896450 back to original broken parser

**Rationale given**: "broke composite_key_table integration tests"

**Actual situation**:
- ✅ Correctly reverted non-working code
- ❌ Didn't actually fix the problem (just removed the failed fix attempt)
- Result: Back to square one

### Attempt 3: Current Investigation (2025-10-29 19:00+)

**What we did**:
1. Analyzed hex dumps of real Statistics.db files
2. Traced parser execution with detailed logging
3. Compared unit test synthetic data vs real file formats
4. Documented the format discrepancies

**Conclusion**: Need someone with Cassandra 5.0 source code access and internals knowledge

---

## Recommended Approach

### Step 1: Get Ground Truth (30 minutes)

**Option A - Run sstabledump**:
```bash
# Use Cassandra's own tool to see what IT thinks the schema is
cd test-data/datasets/sstables/test_basic/composite_key_table-*/
sstabledump nb-1-big-Data.db | head -50

# Look for schema section in JSON output
# This tells us the EXPECTED partition keys, clustering keys, and columns
```

**Option B - Check Cassandra Source Code**:
- File: `src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java`
- File: `src/java/org/apache/cassandra/db/SerializationHeader.java`
- Look for serialization format specification
- Cassandra 5.0 specific (not 4.x or 3.x)

### Step 2: Document Actual Format (30 minutes)

Create a format specification document with:
1. Byte-by-byte layout with field names
2. VInt vs fixed-size integers
3. String encoding (length prefix? null-terminated?)
4. Nested composite type handling
5. Examples from 2-3 different tables

### Step 3: Implement Parser (1-2 hours)

Update `enhanced_statistics_parser.rs`:

```rust
fn parse_serialization_header_at_offset(
    input: &[u8],
    start_offset: usize,
) -> IResult<&[u8], (Vec<String>, Vec<String>, Vec<Column>)> {

    // Start with the 0x00 0x00 marker we know exists
    let (input, _) = tag(b"\x00\x00")(input)?;

    // Parse partition type (includes leading '(' for composite types)
    let (input, partition_type) = parse_cassandra_type_string(input)?;

    // Parse clustering count (appears to be single byte, not VInt)
    let (input, clustering_count) = be_u8(input)?;

    // Parse clustering types
    let mut clustering_types = Vec::new();
    let mut input = input;
    for _ in 0..clustering_count {
        let (remaining, clustering_type) = parse_cassandra_type_string(input)?;
        clustering_types.push(clustering_type);
        input = remaining;
    }

    // TODO: Figure out separator between clustering types and columns
    // Look for pattern in hex dump

    // TODO: Parse column count and columns
    // Format TBD based on hex analysis

    Ok((input, (vec![partition_type], clustering_types, columns)))
}

fn parse_cassandra_type_string(input: &[u8]) -> IResult<&[u8], String> {
    // TODO: Handle nested types like:
    // ReversedType(TimestampType)
    // CompositeType(UUIDType, TimestampType)
    //
    // May need recursive parser or parentheses counting
}
```

### Step 4: Validate (30 minutes)

```bash
# Run integration tests
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core --test v5_compressed_legacy_integration_test \
  test_clustering_key_handling_integration -- --nocapture

# Should see:
# ✅ Successfully parsed SerializationHeader
# ✅ Schema extraction succeeded
# ✅ Test passes
```

---

## Test Data Available

### Tables with Statistics.db Files

All in `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/`:

**Simple cases** (good for initial testing):
- `test_basic/simple_table-*` - Single partition key, no clustering
- `test_basic/ttl_test_table-*` - Single partition key, simple columns

**Complex case** (the failing test):
- `test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/`
  - Partition key: UUID
  - Clustering keys: 2 (ReversedType(TimestampType), UTF8Type)
  - Regular columns: data (UTF8Type), value (Int32Type)

**Reference files**:
- Each SSTable directory has a `schema.cql` file with the CREATE TABLE statement
- Each has a `.jsonl` file with expected row data from sstabledump

### Quick Validation Command

```bash
# Test against multiple tables at once
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core --lib enhanced_statistics_parser -- --nocapture

# Then integration tests
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core --test v5_compressed_legacy_integration_test -- --nocapture
```

---

## Success Criteria

### Must Have ✅

1. **Integration tests pass**:
   - `test_clustering_key_handling_integration` - PASS
   - `test_v5_compressed_legacy_get_all_entries_integration` - PASS

2. **Schema extraction works**:
   - `reader.schema()` returns `Some(schema)` for all test tables
   - Partition keys correctly identified
   - Clustering keys correctly identified
   - Regular columns with correct names and types

3. **CI goes green**:
   - "CI" workflow (full test suite) passes
   - No "Schema extraction failed" errors

### Nice to Have 🎯

1. **Format documentation**: Create `STATISTICS_DB_FORMAT.md` with byte-by-byte specification
2. **Multiple table validation**: Test parser on all 32 tables in test suite
3. **Error messages**: Clear diagnostics when parsing fails
4. **Unit tests**: Add tests with real Statistics.db bytes (not just synthetic)

---

## Resources

### Files to Focus On

**Primary**:
- `cqlite-core/src/parser/enhanced_statistics_parser.rs` (the parser)
- `cqlite-core/tests/v5_compressed_legacy_integration_test.rs` (failing tests)

**Reference**:
- `test-data/datasets/sstables/test_basic/composite_key_table-*/nb-1-big-Statistics.db` (hex dump this)
- `test-data/datasets/sstables/test_basic/composite_key_table-*/schema.cql` (expected schema)

### Useful Commands

```bash
# Hex dump specific offset range
hexdump -C path/to/nb-1-big-Statistics.db | sed -n '300,350p'

# Run parser with debug logging
env RUST_LOG=debug \
  env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core --test v5_compressed_legacy_integration_test \
  test_clustering_key_handling_integration -- --nocapture 2>&1 | less

# Quick unit test iteration
cargo test --package cqlite-core --lib enhanced_statistics_parser::tests::test_serialization_header_with_clustering_keys

# CI validation (once fixed)
env RUSTFLAGS="-D warnings" cargo clippy --package cqlite-core --lib
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core --quiet
```

### External Resources

**Cassandra Documentation**:
- [Cassandra 5.0 Storage Format](https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html)
- [SSTable Format Specification](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec)

**Cassandra Source Code** (authoritative):
- `src/java/org/apache/cassandra/db/SerializationHeader.java`
- `src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java`
- `src/java/org/apache/cassandra/io/util/DataOutputPlus.java` (for VInt encoding)

**sstabledump Tool**:
```bash
# If Cassandra is installed
sstabledump --help
sstabledump path/to/nb-1-big-Data.db
```

---

## Related Work Completed

### Issue #196 (RESOLVED ✅)

While investigating Issue #195, we successfully fixed **Issue #196** (V5CompressedLegacy parser stopping after 3 partitions):

**Changes**:
- Fixed error handling in `v5_compressed_legacy.rs` to continue parsing instead of breaking early
- Added row count parity regression tests
- All CI checks pass except the full test suite (blocked by Issue #195)

**Status**:
- Commit: `626f84b`
- CI: 3/4 workflows passing (SSTableDump Parity ✅, Core Library ✅, Minimal Features ✅, Full Suite ❌ due to Issue #195)

This means once Issue #195 is fixed, we'll have a fully green CI.

---

## Questions for the Expert

When you start working on this, consider these questions:

1. **Does Cassandra 5.0 use VInt encoding for type string lengths?**
   - Or is it fixed-size integers (u8, u16, etc.)?
   - Are type strings null-terminated or length-prefixed?

2. **What does the `0x00 0x00` marker actually signify?**
   - Start of SerializationHeader section?
   - Separator between sections?
   - Magic number?

3. **How are nested composite types encoded?**
   - `ReversedType(TimestampType)` - Is this a string or structured binary?
   - Do we parse the string or decode the structure?

4. **Is there a version field or format indicator?**
   - Statistics.db may have evolved across Cassandra versions
   - Is there a way to detect the format version?

5. **Are there any Cassandra unit tests we can reference?**
   - Java tests that serialize/deserialize SerializationHeader
   - Example byte sequences with known expected output

---

## Contact

**Previous work by**: Claude Code (AI agent)
**Investigation docs**:
- This file (ISSUE_195_HANDOFF_TO_CASSANDRA_EXPERT.md)
- `SERIALIZATION_HEADER_PARSER_ANALYSIS.md` (if present)

**For questions about the investigation**:
- Review git history: commits d896450, b675c41, 626f84b
- Check debug logs in failed CI runs
- Examine hex dumps provided above

---

## Appendix: Hex Dump Reference

### composite_key_table Statistics.db (Offset 0x1390-0x1450)

```
00001390  ff 7f fc f6 81 45 02 b7  8f fd 20 28 75 ed 4d 0d  |.....E.... (u.M.|
000013a0  00 00 28 6f 72 67 2e 61  70 61 63 68 65 2e 63 61  |..(org.apache.ca|
000013b0  73 73 61 6e 64 72 61 2e  64 62 2e 6d 61 72 73 68  |ssandra.db.marsh|
000013c0  61 6c 2e 55 55 49 44 54  79 70 65 02 5b 6f 72 67  |al.UUIDType.[org|
000013d0  2e 61 70 61 63 68 65 2e  63 61 73 73 61 6e 64 72  |.apache.cassandr|
000013e0  61 2e 64 62 2e 6d 61 72  73 68 61 6c 2e 52 65 76  |a.db.marshal.Rev|
000013f0  65 72 73 65 64 54 79 70  65 28 6f 72 67 2e 61 70  |ersedType(org.ap|
00001400  61 63 68 65 2e 63 61 73  73 61 6e 64 72 61 2e 64  |ache.cassandra.d|
00001410  62 2e 6d 61 72 73 68 61  6c 2e 54 69 6d 65 73 74  |b.marshal.Timest|
00001420  61 6d 70 54 79 70 65 29  28 6f 72 67 2e 61 70 61  |ampType)(org.apa|
00001430  63 68 65 2e 63 61 73 73  61 6e 64 72 61 2e 64 62  |che.cassandra.db|
00001440  2e 6d 61 72 73 68 61 6c  2e 55 54 46 38 54 79 70  |.marshal.UTF8Typ|
00001450  65 00 02 04 64 61 74 61  28 6f 72 67 2e 61 70 61  |e...data(org.apa|
```

**Key observations**:
- Offset `0x139f-0x13a0`: `00 00` (marker)
- Offset `0x13a1`: `28` = '(' (start of partition type)
- Offset `0x13a2-0x13cf`: `org.apache.cassandra.db.marshal.UUIDType`
- Offset `0x13d0`: `02` (clustering count = 2)
- Offset `0x13d1`: `5b` = '[' (start of clustering type 1?)
- Offset `0x1450`: `00 02 04 64 61 74 61` = `..data` (column name "data"?)

Good luck! This is a critical M1 blocker, so getting it right is important. Feel free to reach out if you need clarification on anything in this handoff.

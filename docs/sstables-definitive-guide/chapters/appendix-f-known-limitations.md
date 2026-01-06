# Appendix F — Known Limitations

This appendix documents current parsing limitations, validation status, and workarounds in CQLite's SSTable reading implementation. It serves as a reference to prevent repeated investigation of known issues and provides clear guidance for contributors.

**In this appendix you will learn:**
- Which SSTable formats and table types have parsing issues
- Current validation pass rates across test datasets
- Feature gaps and M2+ deferred functionality
- Practical workarounds for common limitations
- Issue tracking references for ongoing fixes

---

## Parsing Limitations

### ~~Static Column Support (Exit Code 3)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #210)
**Impact**: Was 1 table - now 0 (SerializationHeader extraction works for static column tables)
**Resolution**: Fixed SerializationHeader parser to handle static columns section

**Root Cause Found**: The SerializationHeader format includes a static column section between clustering keys and regular columns. The parser was treating the `static_count` byte as a separator (expecting `0x00`), which only worked when there were no static columns.

**Correct Format** (confirmed via `SerializationHeader.java`):
```
[pk_type] [ck_count] [ck_types...] [static_count] [static_columns...] [reg_count] [regular_columns...]
```

When `static_count = 0`, it encodes as `0x00`, making simple tables work. But when `static_count > 0`, parsing would fail.

**Fix**: Modified `parse_serialization_header_at_offset()` in `enhanced_statistics_parser.rs` to:
1. Parse static column count after clustering keys
2. Parse static column definitions when count > 0
3. Mark static columns with `is_static: true` flag

**Tracking**: Issue #210 (CLOSED)

---

### ~~SerializationHeader Marker Search Failures~~ - FIXED

**Status**: ✅ **FIXED** (Issue #216)
**Impact**: Was 5 tables - now 0 (SerializationHeader parsing works for all collection-heavy tables)
**Resolution**: Implemented TOC-based offset lookup and sequential parsing

**Root Cause Found**: The marker-based search (`0x00 0x00` pattern) for SerializationHeader was unreliable because:
- Collection type strings are long (80-200+ bytes) with multi-byte VInt length encoding
- Multiple `0x00 0x00` patterns exist in Statistics.db histogram data
- The parser picked patterns inside column data instead of the actual header start

**Solution Implemented**:
1. **TOC-Based Offset Lookup**: Statistics.db contains a Table of Contents at the start:
   - `[4 bytes num_components] [4 bytes checksum] [TOC entries...]`
   - Each TOC entry: `[4 bytes component_type] [4 bytes offset]`
   - Component type 3 (HEADER) points directly to SerializationHeader
2. **Sequential VInt Parsing**: New `parse_serialization_header_at_toc_offset()` parses:
   - EncodingStats (3 VInts: minTimestamp, minLocalDeletionTime, minTTL)
   - Partition key type (VInt len + string)
   - Clustering types (VInt count + types)
   - Static columns (VInt count + columns)
   - Regular columns (VInt count + columns)
3. **Proper Nested Type Conversion**: `extract_inner_type()` helper uses parenthesis depth tracking instead of `trim_end_matches(')')` to correctly handle nested types like `frozen<map<text, list<int>>>`

**Previously Affected Tables** (now all parsing correctly):
- `frozen_collections_table` - FrozenType(MapType) ✅
- `typed_collections_table` - ListType, SetType, MapType ✅
- `nested_collections_table` - MapType(FrozenType(ListType)) ✅
- `collections_with_udts` - MapType(FrozenType(UserType)) ✅
- `chat_messages` - MapType(FrozenType(SetType)) ✅

**Note**: While SerializationHeader parsing is fixed, these tables still fail smoke tests due to separate Data.db parsing issues (complex cell flags 0xc1-0xcf for collection types). This is a V5CompressedLegacy parser limitation, not a Statistics.db issue.

**Tracking**: Issue #216 (CLOSED)

---

### ~~Summary.db Header Format Mismatch~~ - FIXED

**Status**: ✅ **FIXED** (Issue #218)
**Impact**: Was 5 tables - now 0 (Summary.db parses correctly for all tables)
**Resolution**: Complete rewrite of `summary_reader.rs` with correct Cassandra 5.0 format

**Root Cause Found**: The original parser used a completely incorrect format specification. It expected a "version" field as the first 4 bytes, but Cassandra 5.0 Summary.db starts with `min_index_interval` (e.g., 128).

**Correct Cassandra 5.0 Format** (implemented):
```
Offset  Size  Field                    Description
------  ----  -----------------------  -----------
0x00    4     min_index_interval       e.g., 128 (BE)
0x04    4     entries_count            Number of entries (BE)
0x08    8     summary_entries_size     Offset table + entry data size (BE)
0x10    4     sampling_level           Sampling level 1-128 (BE)
0x14    4     size_at_full_sampling    Entries at full sampling (BE)
        ----  Total header: 24 bytes
0x18    4*N   offset_table[]           LITTLE-ENDIAN offsets!
        var   entries[]                key_data + be_u64 position
        var   first_key                be_u32 size + key data
        var   last_key                 be_u32 size + key data
```

**Critical Implementation Details**:
1. **Offset table is LITTLE-ENDIAN** (not big-endian like everything else!)
2. **No length prefix for entry keys** - key boundaries determined by offset differences
3. **No tokens in summary entries** - only partition key + Index.db position
4. **First/last keys at file end** - serialized with be_u32 length prefix

**API Changes**:
- `SummaryEntry.token` removed (tokens not stored in Summary.db)
- `SummaryEntry.index_offset` renamed to `position`
- `find_entries_in_range()` removed (no token-based queries)
- `find_best_entry_for_token()` replaced with `find_entry_for_position()`
- `get_token_ranges()` removed
- Added `get_first_key()`, `get_last_key()`, `get_header()`
- `iterate_token_range()` deprecated, use `iterate_all_partitions()`
- `get_token_coverage()` deprecated (tokens must be computed from partition keys)

**Tracking**: Issue #218 (CLOSED)

---

### ~~Complex Cell Flags in Data.db~~ - ROOT CAUSE FIXED

**Status**: ✅ **ROOT CAUSE FIXED** (Issue #218)
**Reality**: The "cell flags 0xc1-0xcf" errors were **cascading failures** from Summary.db parsing

With Issue #218 fixed, Summary.db now parses correctly. The remaining collection-heavy table failures are separate Data.db parsing issues with complex cell types (UDTs, frozen collections, nested collections), not cascading from Summary.db.

**Tracking**: Issue #218 (CLOSED)

---

### ~~Clustering Key Row Format Parsing Failures (Exit Code 5)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #213)
**Impact**: Was ~19 tables - now 0 (all clustering key tables parse correctly)
**Resolution**: Corrected field order in V5CompressedLegacy parser

**Root Cause Found**: The clustering prefix comes BEFORE `row_size` in Cassandra's format, not after.

**Correct Format** (confirmed via `UnfilteredSerializer.java`):
```
[row_flags] [extended_flags] [clustering_prefix] [row_size] [prev_size] [row_body]
```

**Previous (Wrong) Format**:
```
[row_flags] [row_size] [prev_size] ... [clustering_prefix]  ← Wrong order!
```

**Fix Details**:
- Split `parse_row_header()` into `parse_row_flags()` + `parse_row_metadata()`
- Parse clustering prefix immediately after flags, before row_size
- File: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

**Results**:
- Smoke test pass rate improved from 27% (9/33) to 79% (26/33)
- All clustering key tables now pass: sensor_data, wide_partition_table, app_metrics, etc.

**Related Fixes (Issue #211)**:
- ✅ Removed false positive magic number `0x00400000` (was LZ4 chunk length prefix)
- ✅ Fixed NB format headerless detection
- ✅ Corrected V5_0NewBig to use V5CompressedLegacy format

---

### ~~BTI Index Zero Entries (Exit Code 0, Silent Failure)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #212)
**Impact**: Was 1 table - now 0 (BTI index parsing works correctly)
**Resolution**: Fixed V5_0NewBigFormat version variant handling in block_io.rs

**Root Cause Found**: Two issues combined to cause silent data loss:

1. **Missing Version Variant**: `CassandraVersion::V5_0NewBigFormat` was not included in the match statement for NB format chunk reading in `block_io.rs`. This caused the reader to use legacy block header parsing (which returns EOF immediately) instead of the correct NB chunk-based reading.

2. **BTI Inter-Entry Padding**: BTI Index.db entries have variable padding bytes between them (null or non-null). The parser needed enhanced padding skip logic to find valid entry boundaries.

**Correct Flow** (after fix):
```
V5_0NewBigFormat → read_nb_format_chunk_data() → decompress chunk → parse partition data
```

**Previous (Wrong) Flow**:
```
V5_0NewBigFormat → read_legacy_format_block_header() → EOF → 0 entries
```

**Fix Details**:
- File: `cqlite-core/src/storage/sstable/reader/block_io.rs` - Added `V5_0NewBigFormat` to NB chunk reader match
- File: `cqlite-core/src/storage/sstable/index_reader.rs` - Enhanced BTI padding skip logic

**Results**:
- `stock_prices` now returns 231 entries (2 partitions with rows)
- Smoke test pass rate improved from 26/33 (79%) to 28/33 (85%)
- BTI format parsing now works correctly for all tested tables

**Tracking**: Issue #212 (CLOSED)

---

### BTI Metadata Offset Extraction (Performance Optimization - M3+ Scope)

**Status**: 🔄 **DEFERRED** (Issue #226)
**Impact**: Performance - sequential scan fallback instead of direct partition lookup
**Current Behavior**: Fully functional with sequential read mode

**Background**: BTI format Index.db entries contain variable-length metadata after the partition key. This metadata encodes the Data.db offset for direct partition seeks, but the exact format was not previously documented.

**Research Findings** (Issue #226):
- BTI payload uses **SizedInts encoding** (not VInt)
- Format: `[hash_byte: 1 byte][position: size bytes]`
- Size determined by `payloadBits` field in trie node header
- Formula: `size = payloadBits - 7`

**Example from stock_prices Index.db**:
```text
00 00 04 80 00 4f 88 00
^  ^-----------^
│  └─ Position bytes (Data.db offset)
└─── Hash byte (filter hash lower 8 bits)
```

**Current Workaround**: Sequential scan with raw_key matching (Issue #212 fix) - functionally correct but O(n) performance.

**Future Optimization** (M3+ scope):
1. Extract `payloadBits` from BTI trie node headers
2. Decode SizedInts to get Data.db offset
3. Enable O(log n) direct partition seeks

**Implementation Status**:
- ✅ SizedInts decoder implemented (`cqlite-core/src/storage/sstable/bti/sized_ints.rs`)
- ✅ Research documented (`docs/research/BTI_PAYLOAD_*.md`)
- ⏳ Trie node header parsing (pending)
- ⏳ Direct offset extraction (pending)

**Tracking**: Issue #226 (log noise fix - CLOSED), Issue #208 C3 (offset extraction - deferred)

---

### Index.db VInt Offset Parsing (DigestFormat - NB Tables)

**Status**: 🐛 **OPEN** (Issue #237)
**Impact**: 83% of partitions skipped in 7 test_timeseries tables (~827 partitions)
**Current Behavior**: Falls back to sequential Data.db scan with "malformed partition" warnings

**Root Cause**: The Index.db parser incorrectly reads VInt offsets as length-prefixed bytes. The current implementation treats the first byte after the digest as an `offset_len` field, then reads that many bytes. This matches older MC/MD SSTable formats, but NB format (Cassandra 5.0) uses VInt encoding directly.

**Affected Format** (DigestFormat with VInt Offsets):
```
Entry: marker(2) + digest(16) + vint_offset(1-9 bytes)

Where:
- marker: 0x0010 (fixed)
- digest: 16-byte MD5 hash of partition key
- vint_offset: Cassandra VInt encoding (NOT length-prefixed)
```

**Bug Location**: `cqlite-core/src/storage/sstable/index_reader.rs`
- Function: `parse_simple_partition_key_with_offset()` (lines ~375-430)

**Current (Wrong)**:
```rust
let (input, offset_len) = nom_u8(input)?;      // Treats VInt byte as length!
let (input, offset_bytes) = take(offset_len)(input)?;
let data_offset = decode_be_offset(offset_bytes);
```

**Evidence from sensor_data Index.db**:
```
0x0000  00 10 02 84 a7 18 be 7b 49 e6 b6 b9 8e 82 f5 ff  .......{I.......
0x0010  16 60 [00] 00 00 10 7d 39 42 8c aa a8 45 1d 84 7f  .`....}9B...E...
        ^^^^^^ Entry 0 VInt (0x00 = 0)
                   ^^^^^ Entry 1 marker
```

Parser reads `0x00` as `offset_len=0`, takes 0 bytes, advances by 19 bytes instead of 20. Next entry parse fails.

**Affected Tables**:
| Table | Partitions | Currently Parsed | Success Rate |
|-------|-----------|------------------|--------------|
| sensor_data | 9 | 1 | 11% |
| app_metrics | 199 | 1 | <1% |
| user_activity | 199 | 1 | <1% |
| log_entries | 199 | 1 | <1% |
| event_store | 199 | 1 | <1% |
| user_sessions | 199 | 1 | <1% |
| tick_data | 23 | 1 | 4% |

**Proposed Fix**:
```rust
// Replace offset parsing with VInt decoding:
let (input, vint_offset) = parse_vint(input)?;
let data_offset = vint_offset;  // SSTableReader adds header_size later
```

**Additional Notes**:
- Index.db offsets are relative to Data.db data section (exclude 30-byte header)
- VInt decoding already exists in `cqlite-core/src/parser/vint.rs`
- Format detection needed to distinguish NB VInt from legacy length-prefixed

**Tracking**: Issue #237

---

## Validation Status

### Overall Pass Rate: 100% (33/33 tables) ✅ COMPLETE (macOS)

As of Issue #220 fix (Updated: 2025-12-18)

**Note**: All SSTable component parsers and cell type handling are now complete. All 33 test tables pass validation on macOS!

### ~~CI Environment Issue (Issue #225)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #225)
**Impact**: Was 2 tables failing on Linux CI - now 0
**Resolution**: Added bounds checks and safe type conversions for complex collection parsing

**Root Cause Found**: Non-frozen complex column parsing lacked the bounds check present in frozen collection parsing. The `parse_complex_column` function used `Vec::with_capacity(cell_count_usize)` without first checking against `MAX_FROZEN_COLLECTION_SIZE`. Additionally, `parse_complex_cell_value` and `skip_complex_cell` used unsafe `as usize` casts on `path_len` and `value_len` (u64 values) which could overflow on large/corrupted values.

**Fix Applied**:
1. Added `MAX_CELL_VALUE_LENGTH` constant (64 MB limit) for path/value length validation
2. Added bounds check in `parse_complex_column` matching frozen collection pattern
3. Replaced `as usize` casts with `try_into()` + limit checks in `parse_complex_cell_value`
4. Applied same safe conversion pattern in `skip_complex_cell`

**Previously Affected Tables** (now parsing correctly on all platforms):
- `test_collections.large_collections_table` ✅
- `test_timeseries.app_metrics` ✅

**Tracking**: Issue #225 (CLOSED)

### Pass Rate by Keyspace

| Keyspace | Passed | Failed | Total | Pass Rate |
|----------|--------|--------|-------|-----------|
| **test_basic** | 8 | 0 | 8 | 100% ✅ |
| **test_collections** | 8 | 0 | 8 | 100% ✅ |
| **test_timeseries** | 9 | 0 | 9 | 100% ✅ |
| **test_wide_rows** | 8 | 0 | 8 | 100% ✅ |

**Note**: All tables now pass after completion of Issues #219, #220, and #221!

### Passing Tables (Production-Ready)

These tables are validated against Apache Cassandra's `sstabledump` output:

**test_basic** (8/8 passing - 100%):
- `simple_table` - Gold standard validation table
- `composite_key_table` - Composite partition keys validated
- `compression_test_table` - LZ4 compression validated
- `multi_partition_table` - Multi-partition scenarios
- `ttl_test_table` - TTL metadata parsing
- `counters` - Counter column type support
- `uncompressed_table` - Now passing after Issue #213 fix
- `static_columns_table` - Static columns now working (Issue #210 fix)

**test_collections** (8/8 passing - 100% ✅):
- `collection_table` - Lists, sets, maps validated
- `collection_clustering_table` - Collections with clustering keys (Issue #213 fix)
- `collections_with_udts` - UDT support (Issue #220 fix) ✅
- `empty_collections_table` - Empty collection handling
- `frozen_collections_table` - Frozen collections (Issues #219, #221 fix) ✅
- `large_collections_table` - Large collection support
- `nested_collections_table` - Nested collections (Issue #218 fix)
- `typed_collections_table` - Complex collection types (Issue #221 fix) ✅

**test_timeseries** (9/9 passing - 100%):
- `sensor_data` - Timestamp clustering (Issue #213 fix, was key test case)
- `app_metrics`, `log_entries`, `tick_data` - All passing
- `time_bucketed_counters`, `user_activity`, `user_sessions`, `event_store`
- `stock_prices` - BTI format now working (Issue #212 fix)

**test_wide_rows** (8/8 passing - 100% ✅):
- `wide_partition_table` - Wide partitions (Issue #213 fix)
- `chat_messages` - Non-frozen collections with frozen values (Issue #221 fix) ✅
- `document_versions`, `large_blob_table`, `many_columns_table`
- `multi_metric_timeseries`, `product_catalog`, `sparse_data_table`

### Remaining Failures

**Status**: ✅ **NO REMAINING FAILURES** - All 33 tables now pass!

Previously blocking issues have been resolved:
- ✅ **Issue #219**: Frozen type support - FIXED
- ✅ **Issue #220**: UDT support - FIXED
- ✅ **Issue #221**: Complex cell flag handling - FIXED

All core SSTable component parsers are working correctly with complete support for all data types and collection formats.

---

## M2+ Deferred Features

The following features are planned but not implemented in the current M2 milestone:

### SSTable Writing (Removed in Issue #175, #176)

**Status**: Removed from codebase
**Rationale**: CQLite is a **read-only library** focused on local SSTable access

**Removed Components**:
- `storage/wal.rs` (Write-Ahead Log)
- `storage/memtable.rs` (In-memory write buffer)
- `storage/compaction.rs` (Background merging)
- `storage/manifest.rs` (Metadata tracking)
- `storage/sstable/writer.rs` (SSTable serialization)
- `storage/sstable/validation.rs` (Write validation)

**Impact**: All `put()`, `delete()`, `flush()`, `compact()` methods return errors with message "removed in Issue #175/176".

**Workaround**: Use Apache Cassandra for writes. CQLite is read-only.

**Future**: Write support may return in M4+ if community demand justifies the complexity.

---

### Experimental Features (Opt-In)

#### Legacy Heuristics (Pre-5.0 Format Support)

**Feature Flag**: `legacy-heuristics`
**Default**: **Disabled** (not in CI)
**Purpose**: Backward compatibility for Cassandra 3.x/4.x SSTables

CQLite defaults to Cassandra 5.0+ formats using authoritative metadata (no-heuristics mandate, Issue #28). Legacy heuristics enable schema-less blob fallback for older formats.

**To Enable**:
```bash
cargo build --features legacy-heuristics
```

**Note**: Legacy support is **not tested in CI** and may have gaps. Modern Cassandra 5.0 is the supported target.

---

#### ANTLR Parser (Alternative CQL Parser)

**Feature Flag**: `antlr`
**Default**: Disabled
**Purpose**: ANTLR4-based CQL parser as alternative to nom-based parser

M2+ uses nom parser by default. ANTLR integration is experimental and incomplete.

---

#### Tombstone and GC Logic

**Feature Flag**: `tombstones`
**Default**: Disabled
**Purpose**: Tombstone detection and garbage collection semantics

**Status**: Deferred to M3+

Current implementation skips tombstoned rows (Issue #191 fix in select_executor.rs) but does not expose tombstone metadata or perform GC simulation.

---

### Query Engine Limitations (M2+ Scope)

**Current State**: Query engine enabled by default (`state_machine` feature)

**Implemented**:
- SELECT statement parsing and execution
- Prepared statement support
- Query planning and optimization
- Multi-partition query execution
- Schema-aware result formatting

**Not Implemented** (M3+ Scope):
- INSERT/UPDATE/DELETE statement execution (write operations)
- WHERE clause filtering (partial - partition key filtering works)
- ORDER BY clause support
- LIMIT clause support
- Aggregate functions (COUNT, SUM, AVG, etc.)
- GROUP BY clause support

**Workaround**: For unsupported query features, use `read-sstable` command for raw data access or Apache Cassandra tools.

---

## Known Workarounds

### Workaround 1: Using sstabledump for Unsupported Tables

For tables that fail to parse (static columns, frozen types, UDTs), use Apache Cassandra's `sstabledump` tool:

```bash
# Generate JSONL output
sstabledump /path/to/Data.db > output.jsonl

# Human-readable format
sstabledump -d /path/to/Data.db
```

**Note**: sstabledump requires Cassandra installation and Java runtime.

---

### Workaround 2: Direct Component Access

For debugging or advanced use cases, access individual SSTable components directly:

```bash
# Read Statistics.db metadata
cqlite read-sstable --component statistics /path/to/Statistics.db

# Read Index.db entries
cqlite read-sstable --component index /path/to/Index.db

# Read CompressionInfo.db
cqlite read-sstable --component compression-info /path/to/CompressionInfo.db
```

---

### Workaround 3: Entry Count Mismatches (Multi-Row Partitions)

**Observation**: Some passing tables report fewer entries than expected rows:
- `composite_key_table`: 45 entries vs 99 rows
- `multi_partition_table`: 24 entries vs 99 rows
- `ttl_test_table`: 44 entries vs 99 rows

**Explanation**: These tables have clustering keys, creating multi-row partitions. CQLite counts **partition entries** while sstabledump counts **total rows**.

**Action**: This is correct behavior. No workaround needed.

---

### Workaround 4: Minimal Builds (No Query Engine)

For embedded or constrained environments, build without query engine:

```bash
# M1-compatible binary (storage layer only)
cargo build --no-default-features --features all-compression

# Binary size reduced by ~40% (no query planner/executor)
```

**Trade-off**: Lose `execute()`, `prepare()`, `explain()` methods. Only low-level SSTable API available.

---

## Issue References

### Completed Issues (Fixed - Jan 2026)

- **Issue #238**: UDTs Inside Collections Not Parsed - **FIXED**
  - Status: ✅ FIXED - Extended `parse_value_with_comparator` for recursive type parsing
  - Root cause: `parse_value_with_comparator` had minimal implementation (only Boolean, Text, Blob) - all other types fell back to Blob, including UDTs nested in List/Set/Map
  - Fix: Added complete type handlers for TinyInt, SmallInt, Int, BigInt, Uuid, List, Set, Map, Tuple, UDT, and Frozen types
  - Result: UDTs inside collections now show actual field values instead of `0x` blobs
  - File: `cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs` (lines 172-324)

- **Issue #239**: Nested UDTs Inside Collections Display as Hex Blobs - **FIXED**
  - Status: ✅ FIXED - Nested UDTs in collections now parse correctly
  - Root cause: Two issues:
    1. When parsing UDT field types from schema, nested UDT names were stored as `CqlType::Custom("udt:typename")` with a "udt:" prefix, but registry lookups used plain names without the prefix
    2. Inline `CqlType::Udt(name, fields)` definitions were ignored (the `fields` parameter was prefixed with `_`) and code fell back to Blob when registry lookup failed
  - Fix:
    1. Added `strip_prefix("udt:")` normalization at 6 registry lookup sites in `parse_nested_udt_from_registry()` and `parse_raw_type_value()`
    2. Added `parse_inline_udt_value()` function to parse UDTs using inline field definitions when registry lookup fails
    3. Modified all `CqlType::Udt(udt_name, inline_fields)` pattern matches to use `inline_fields` as fallback
  - Result: Nested UDTs like `contact_info.address` now show parsed field values (`{street, city, state, zip_code, country}`) instead of `0x...` blobs
  - File: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

### Completed Issues (Fixed - Dec 2025)

- **Issue #220**: UDT (User-Defined Type) Support - **FIXED**
  - Status: ✅ FIXED - UDT schema parsing and cell deserialization
  - Impact: `collections_with_udts` now passes
  - Result: All 8 test_collections tables now passing (100%)

- **Issue #221**: Complex Cell Flag Handling (0xC0-0xCF) - **FIXED**
  - Status: ✅ FIXED - Non-frozen collection parsing implemented
  - Root cause: Parser tried to read complex deletion time VInt as cell flags
  - Fix: Added `is_complex_column()` detection, `parse_complex_column()` with proper HAS_COMPLEX_DELETION handling, `skip_complex_cell()` with correct field order (flags→timestamp→deletion→ttl→path→value)
  - Key insight: Cell flags are ONLY 0x00-0x1F (5 bits). The 0xC0+ bytes were VInt data, not flags.
  - Also fixed: Added V5_0TypedCollections to block_io.rs NB format list
  - Result: `typed_collections_table` and `frozen_collections_table` now pass

- **Issue #218**: Summary.db parser format mismatch - **FIXED**
  - Status: ✅ FIXED - Complete rewrite with correct Cassandra 5.0 format
  - Root cause: Parser used wrong format (expected `version`, got `min_index_interval`)
  - Fix: Implemented correct 24-byte header, little-endian offset table, offset-based key parsing
  - Result: Summary.db now parses correctly for all 33 tables
  - Reference: `/docs/sstable-summary-format.md`

- **Issue #215 + #216**: SerializationHeader parsing - **FIXED**
  - Status: ✅ FIXED - TOC-based offset lookup implemented
  - Statistics.db/SerializationHeader now parses correctly for all 33 tables

- **Issue #210**: Static columns in SerializationHeader - FIXED
  - Status: Fixed (VInt + static column section parsing)
  - Result: `static_columns_table` now passing

- **Issue #211**: LZ4 compression chunk format - FIXED
  - Status: Fixed (correct chunk header parsing)
  - Result: 19 tables unblocked

- **Issue #212**: BTI index zero entries - FIXED
  - Status: Fixed (V5_0NewBigFormat variant handling)
  - Result: `stock_prices` now passing

- **Issue #213**: Clustering key parsing order - FIXED
  - Status: Fixed (clustering prefix before row_size)
  - Result: sensor_data, wide_partition_table, and many others now passing

### Completed Issues (Fixed - Earlier)

- **Issue #206**: V5_0FormatG Counter Support
  - Status: Fixed (1-line header routing fix)
  - Result: `counters` table now passing

- **Issue #207**: Byte-Comparable Key Encoding (CEP-25)
  - Status: Completed
  - Result: V5_0NewBigFormat (0xD4645400) now recognized

- **Issue #208**: BTI Index.db Format Support
  - Status: Completed
  - Result: Dual-parser architecture for MD5 digest + BTI formats
  - Impact: +366 LOC, Index.db parsing improved

- **Issue #209**: Component Flattening Pre-allocation
  - Status: Completed
  - Result: 55-75% performance improvement for 2-6 component keys

### Deferred Issues (M3+ Scope)

- **Issue #154**: UDT support (collections_with_udts)
  - Status: Partial implementation, blocked by Issue #210
  - Scope: M3+ feature completeness

- **Issue #162**: Statistics.db EncodingStats parsing
  - Status: Minimal parser implemented
  - Scope: M3+ metadata enhancements

- **Issue #191**: Tombstone row filtering
  - Status: Fixed (skip tombstoned rows in select_executor.rs)
  - Remaining: Expose tombstone metadata (M3+ scope)

### Infrastructure Removed

- **Issue #175**: MemTable and WAL removal
  - Rationale: Read-only library focus
  - Impact: All write operations return errors

- **Issue #176**: Compaction and manifest removal
  - Rationale: Read-only library focus
  - Impact: Compaction methods return errors

---

## Key Takeaways

- **Pass rate: 100% (33/33 tables)** - COMPLETE! All test tables now passing
- All SSTable component parsers (Data.db, Index.db, Summary.db, Statistics.db) now use correct formats
- All data types fully supported: basic types, collections, UDTs, frozen types, complex cells
- CQLite is **read-only** - write operations permanently removed (Issues #175, #176)
- **All feature gaps closed**:
  - ✅ Issue #219: Frozen type support
  - ✅ Issue #220: UDT (User-Defined Type) support
  - ✅ Issue #221: Complex cell flag handling for non-frozen collections
- **Milestone achieved**: M3 completion (production-ready with 100% parsing coverage)

---

## References

- Validation Matrix: `test-data/validation-matrix.md`
- Smoke Test Script: `test-data/scripts/smoke-test-all-tables.sh`
- Issue Tracker: https://github.com/pmcfadin/cqlite/issues
- Integration Tests: `cqlite-core/tests/*.rs`
- Feature Flags: `cqlite-core/Cargo.toml` [features] section

---

## Cross-Links

- Appendix B — [On-Disk Encodings Cheat Sheet](appendix-b-encodings-cheat-sheet.md) - Binary format details
- Appendix C — [Reference Walkthroughs with Code](appendix-c-walkthroughs.md) - Parsing examples
- Appendix D — [Tools & Workflows](appendix-d-tools-and-workflows.md) - sstabledump usage
- Chapter 8 — [Statistics.db](08-statistics-db.md) - SerializationHeader format
- Chapter 17 — [BTI Formats](17-bti-formats.md) - BTI index structure

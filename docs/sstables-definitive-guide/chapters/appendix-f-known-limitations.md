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

### Frozen Collection and Type Support (Exit Code 3)

**Status**: Partially Implemented
**Impact**: 3 tables
**Root Cause**: SerializationHeader extraction fails; frozen types use different serialization format

Frozen collections and frozen UDTs are serialized as single binary blobs rather than multi-cell structures. Current parser cannot extract their metadata from Statistics.db.

**Tables Affected**:
- `frozen_collections_table` (test_collections)
- `typed_collections_table` (test_collections)
- `chat_messages` (test_wide_rows - includes frozen types)

**Workaround**: None for frozen collections. Regular (non-frozen) collections work correctly.

**Note**: Regular collection parsing (lists, sets, maps) works correctly as demonstrated by `collection_table` (12 integration tests, 499 rows validated).

**Tracking**: Issue #210

---

### Nested Collection Parsing (Exit Code 3)

**Status**: Not Implemented
**Impact**: 1 table (`test_collections.nested_collections_table`)
**Root Cause**: Requires recursive parser; currently blocked by SerializationHeader extraction failure

Collections containing other collections (e.g., `map<text, list<int>>`, `list<set<text>>`) require recursive type parsing. Implementation is feasible but low priority.

**Tables Affected**:
- `nested_collections_table` (test_collections keyspace)

**Workaround**: None. Avoid nested collections or use Cassandra tools.

**Tracking**: Issue #210 (SerializationHeader), deferred nested parsing to M3

---

### User-Defined Type (UDT) Parsing

**Status**: Minimal Implementation (Issue #154)
**Impact**: 1 table (`test_collections.collections_with_udts`)
**Root Cause**: UDT schema extraction incomplete; SerializationHeader parsing fails

UDTs require schema registry access to deserialize field-by-field. Current implementation may have gaps in UDT field parsing.

**Tables Affected**:
- `collections_with_udts` (test_collections keyspace)

**Workaround**: None for UDT-containing tables.

**Tracking**: Issue #154, Issue #210

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

## Validation Status

### Overall Pass Rate: 84.8% (28/33 tables)

As of Issue #212 fix (Updated: 2025-12-16)

### Pass Rate by Keyspace

| Keyspace | Passed | Failed | Total | Pass Rate |
|----------|--------|--------|-------|-----------|
| **test_basic** | 8 | 0 | 8 | 100% |
| **test_collections** | 5 | 3 | 8 | 62.5% |
| **test_timeseries** | 9 | 0 | 9 | 100% |
| **test_wide_rows** | 7 | 1 | 8 | 87.5% |

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

**test_collections** (5/8 passing):
- `collection_table` - Lists, sets, maps validated
- `collection_clustering_table` - Collections with clustering keys (Issue #213 fix)
- `empty_collections_table` - Empty collection handling
- `large_collections_table` - Large collection support

**test_timeseries** (9/9 passing - 100%):
- `sensor_data` - Timestamp clustering (Issue #213 fix, was key test case)
- `app_metrics`, `log_entries`, `tick_data` - All passing
- `time_bucketed_counters`, `user_activity`, `user_sessions`, `event_store`
- `stock_prices` - BTI format now working (Issue #212 fix)

**test_wide_rows** (7/8 passing):
- `wide_partition_table` - Now passing (Issue #213 fix)
- `document_versions`, `large_blob_table`, `many_columns_table`
- `multi_metric_timeseries`, `product_catalog`, `sparse_data_table`

### Remaining Failures (5 tables)

| Table | Exit Code | Root Cause |
|-------|-----------|------------|
| `collections_with_udts` | 3 | UDT schema parsing incomplete |
| `frozen_collections_table` | - | Frozen type serialization not implemented |
| `nested_collections_table` | 3 | Recursive collection parsing needed |
| `typed_collections_table` | 5 | Complex type handling issues |
| `chat_messages` | 5 | Contains frozen types |

**Note**: These 5 remaining failures are all related to advanced collection and type features: UDTs, frozen collections, and nested collections. Basic SSTable parsing for all standard table types now works correctly.

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

### Active Issues (P0 - Blocking)

- **Issue #211**: Partition key parsing failures (19 tables, 57.6% of failures)
  - Root cause: Byte offset miscalculation in compressed blocks
  - Priority: P0 - Largest blocker
  - ETA: Under investigation

- **Issue #210**: SerializationHeader extraction failures (4 tables)
  - Root cause: enhanced_statistics_parser.rs cannot locate header for complex schemas
  - Priority: P0 - Blocks static columns, frozen types
  - ETA: Under investigation

- **Issue #212**: BTI index zero entries (1 table)
  - Root cause: BTI offset extraction fails, silent data loss
  - Priority: P1 - Silent failure, but limited scope
  - ETA: Under investigation

### Completed Issues (Fixed)

- **Issue #206**: V5_0FormatG Counter Support
  - Status: Fixed (1-line header routing fix)
  - Result: `counters` table now passing

- **Issue #207**: Byte-Comparable Key Encoding (CEP-25)
  - Status: Completed
  - Result: V5_0NewBigFormat (0xD4645400) now recognized
  - Note: May have introduced regression (Issue #211)

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

- **27.3% pass rate** (9/33 tables) as of 2025-11-02 - significant room for improvement
- **Issue #211 (partition key parsing)** is the largest blocker affecting 19 tables
- **test_wide_rows keyspace** has 0% pass rate and zero integration tests - critical gap
- **Tier 1 tables** (`wide_partition_table`, `sensor_data`, `uncompressed_table`) must be fixed for M1 completion
- CQLite is **read-only** - write operations permanently removed (Issues #175, #176)
- Use `sstabledump` workaround for unsupported table types (static columns, frozen types, UDTs)

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

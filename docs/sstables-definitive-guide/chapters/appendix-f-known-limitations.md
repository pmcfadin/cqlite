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

### Static Column Support (Exit Code 3)

**Status**: Not Implemented
**Impact**: 1 table (`test_basic.static_columns_table`)
**Root Cause**: SerializationHeader extraction fails for tables with static columns

Static columns (partition-level data shared across rows) require different parsing logic than regular columns. The current enhanced_statistics_parser.rs cannot locate the SerializationHeader in Statistics.db for these tables.

**Tables Affected**:
- `static_columns_table` (test_basic keyspace)

**Workaround**: None. Tables with static columns cannot be read. Use Cassandra's `sstabledump` tool for access.

**Tracking**: Issue #210

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

### Partition Key Parsing Failures (Exit Code 5)

**Status**: Active Investigation
**Impact**: 19 tables (57.6% of failures)
**Root Cause**: Byte offset miscalculation in compressed blocks; possible VInt decoding errors

This is the **largest blocker** affecting tables across all keyspaces. The parser fails to extract partition key component lengths correctly, leading to read failures.

**Symptom**: "Failed to parse partition key component length" or similar VInt parsing errors

**Tables Affected** (Tier 1 Priority - Core Validation Tables):
- `wide_partition_table` (test_wide_rows - **14 integration tests**)
- `sensor_data` (test_timeseries - **12 integration tests**)
- `uncompressed_table` (test_basic - **5 integration tests**)

**Tables Affected** (All):
- test_basic: `uncompressed_table`
- test_collections: `collection_clustering_table`, `empty_collections_table`, `large_collections_table`
- test_timeseries: `sensor_data`, `app_metrics`, `log_entries`, `tick_data`, `time_bucketed_counters`, `user_activity`
- test_wide_rows: `wide_partition_table`, `document_versions`, `large_blob_table`, `many_columns_table`, `multi_metric_timeseries`, `product_catalog`, `sparse_data_table`

**Recommended Action**: Fix Tier 1 tables first (`wide_partition_table`, `sensor_data`, `uncompressed_table`). These are heavily tested and represent core functionality.

**Tracking**: Issue #211

**Note**: Recent byte-comparable key encoding changes (Issue #207, CEP-25) may have introduced regression. Git bisect recommended.

---

### BTI Index Zero Entries (Exit Code 0, Silent Failure)

**Status**: Active Investigation
**Impact**: 1 table (`test_timeseries.stock_prices`)
**Root Cause**: BTI offset extraction fails; sequential scan fallback returns 0 entries

The BTI (Big Table Index) parser successfully opens the Index.db but returns zero entries. This is a **silent data loss scenario** - no error reported, but all data is inaccessible.

**Tables Affected**:
- `stock_prices` (test_timeseries - 2 rows expected, 0 returned)

**Workaround**: None. Table appears empty when queried.

**Tracking**: Issue #212

**Note**: BTI dual-parser architecture (Issue #208) works for other tables but fails on this specific BTI format variant.

---

## Validation Status

### Overall Pass Rate: 27.3% (9/33 tables)

As of validation-matrix.md (Last Updated: 2025-11-02)

### Pass Rate by Keyspace

| Keyspace | Passed | Failed | Total | Pass Rate |
|----------|--------|--------|-------|-----------|
| **test_basic** | 5 | 3 | 8 | 62.5% |
| **test_collections** | 1 | 7 | 8 | 12.5% |
| **test_timeseries** | 3 | 6 | 9 | 33.3% |
| **test_wide_rows** | 0 | 8 | 8 | 0.0% |

### Passing Tables (Production-Ready)

These tables are validated against Apache Cassandra's `sstabledump` output and have extensive integration test coverage:

**test_basic** (5/8 passing):
- `simple_table` (999 rows, 23 integration tests) - Gold standard validation table
- `composite_key_table` (99 rows, 9 tests) - Composite partition keys validated
- `compression_test_table` (99 rows, 11 tests) - LZ4 compression validated
- `multi_partition_table` (99 rows, 7 tests) - Multi-partition scenarios
- `ttl_test_table` (99 rows, 5 tests) - TTL metadata parsing
- `counters` (4 rows, 2 tests) - Counter column type support (Issue #206 fix)

**test_collections** (1/8 passing):
- `collection_table` (499 rows, 12 tests) - Lists, sets, maps validated

**test_timeseries** (3/9 passing):
- `event_store` (199 rows, 1 test)
- `user_sessions` (199 rows, 1 test)

Note: Entry count mismatches exist for some passing tables (composite_key_table shows 45 entries vs 99 rows). This indicates multi-row partitions (clustering keys) are correctly parsed but counted differently than sstabledump's partition-level output.

### Critical Failures (Tier 1 Tables with Heavy Test Coverage)

**These tables SHOULD pass but currently fail**:

- `wide_partition_table` (test_wide_rows) - **14 integration tests**, EXIT CODE 5
- `sensor_data` (test_timeseries) - **12 integration tests**, EXIT CODE 5
- `uncompressed_table` (test_basic) - **5 integration tests**, EXIT CODE 5

**Action Required**: Fix these tables immediately. Their failure indicates systemic parser issues affecting core functionality.

### Test Coverage Gaps

**test_wide_rows keyspace**: 0% pass rate, **0 integration tests** for all 8 tables. This is a critical blind spot.

**Tier 3 Tables**: 16 tables have 0-1 integration tests. Low coverage increases regression risk.

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

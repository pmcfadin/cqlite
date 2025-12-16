# CQLite Test Tables Validation Matrix - Issue #200

**Comprehensive validation tracking for all test tables across the CQLite test suite**

**Last Updated**: 2025-11-02 (After Issues #207, #208, #209 fixes)
**Issue Reference**: [#200](https://github.com/pmcfadin/cqlite/issues/200) - Validate all 33 test tables can be loaded successfully
**Current Status**: 9/33 PASS (27.3% pass rate) - **NEEDS ATTENTION**

---

## Summary Statistics

| Metric | Value | Notes |
|--------|-------|-------|
| **Total Tables** | 33 | Across 4 keyspaces (test_basic, test_collections, test_timeseries, test_wide_rows) |
| **Tables with JSONL** | 33 | 100% coverage - all tables have sstabledump reference files |
| **Smoke Test Pass** | 9/33 | 27.3% pass rate (+1 from #206 counter fix) |
| **Smoke Test Fail** | 24/33 | 72.7% failure rate - 3 critical issues identified |
| **Exit Code 3 Failures** | 4 | SerializationHeader extraction issues (Issue #210) |
| **Exit Code 5 Failures** | 19 | Partition key parsing failures (Issue #211) |
| **Exit Code 0 Empty** | 1 | BTI zero entries (Issue #212) |

### Pass Rate by Keyspace

| Keyspace | Passed | Failed | Total | Pass Rate |
|----------|--------|--------|-------|-----------|
| **test_basic** | 5 | 3 | 8 | 62.5% |
| **test_collections** | 1 | 7 | 8 | 12.5% ⚠️ |
| **test_timeseries** | 3 | 6 | 9 | 33.3% (↑ from 22.2%) |
| **test_wide_rows** | 0 | 8 | 8 | 0.0% 🔴 |

---

## Main Validation Table

### test_basic (8 tables - 5 PASS / 3 FAIL)

| Table | Rows | Load | Parse | Count | Int Test | Status | Notes |
|-------|------|------|-------|-------|----------|--------|-------|
| simple_table | 999 | ✅ | ✅ | ✅ | ✅ (23 tests) | **PASS** | Heavily tested, core validation table |
| composite_key_table | 99 | ✅ | ✅ | ⚠️ | ✅ (9 tests) | **PASS** | Entry count mismatch (45 vs 99) - investigate |
| compression_test_table | 99 | ✅ | ✅ | ✅ | ✅ (11 tests) | **PASS** | LZ4 compression validated |
| multi_partition_table | 99 | ✅ | ✅ | ⚠️ | ✅ (7 tests) | **PASS** | Entry count mismatch (24 vs 99) - investigate |
| ttl_test_table | 99 | ✅ | ✅ | ⚠️ | ✅ (5 tests) | **PASS** | Entry count mismatch (44 vs 99) - investigate |
| counters | 4 | ✅ | ✅ | ✅ | ✅ (2 tests) | **PASS** | Fixed by Issue #206 (V5_0FormatG support) |
| static_columns_table | 99 | ❌ | ❌ | ❌ | ⚠️ (1 test) | **FAIL** | Exit code 3 - Static column parsing unsupported |
| uncompressed_table | 99 | ❌ | ❌ | ❌ | ✅ (5 tests) | **FAIL** | Exit code 5 - Unexpected failure (should pass) 🔴 |

### test_collections (8 tables - 1 PASS / 7 FAIL)

| Table | Rows | Load | Parse | Count | Int Test | Status | Notes |
|-------|------|------|-------|-------|----------|--------|-------|
| collection_table | 499 | ✅ | ✅ | ✅ | ✅ (12 tests) | **PASS** | Core collection validation table |
| collection_clustering_table | 49 | ❌ | ❌ | ❌ | ⚠️ (3 tests) | **FAIL** | Exit code 5 - Collection clustering key handling |
| collections_with_udts | 49 | ❌ | ❌ | ❌ | ⚠️ (1 test) | **FAIL** | Exit code 3 - UDT support incomplete (Issue #154) |
| empty_collections_table | 49 | ❌ | ❌ | ❌ | ✅ (1 test) | **FAIL** | Exit code 5 - Empty collection handling |
| frozen_collections_table | 49 | ❌ | ❌ | ❌ | ⚠️ (1 test) | **FAIL** | Exit code 3 - Frozen type support missing |
| large_collections_table | 49 | ❌ | ❌ | ❌ | ⚠️ (2 tests) | **FAIL** | Exit code 5 - Large collection parsing |
| nested_collections_table | 49 | ❌ | ❌ | ❌ | ⚠️ (4 tests) | **FAIL** | Exit code 3 - Nested collection parsing |
| typed_collections_table | 49 | ❌ | ❌ | ❌ | ⚠️ (1 test) | **FAIL** | Exit code 3 - Typed collection support |

### test_timeseries (9 tables - 2 PASS / 7 FAIL)

| Table | Rows | Load | Parse | Count | Int Test | Status | Notes |
|-------|------|------|-------|-------|----------|--------|-------|
| event_store | 199 | ✅ | ✅ | ⚠️ | ⚠️ (1 test) | **PASS** | Entry count mismatch (53 vs 199) - investigate |
| user_sessions | 199 | ✅ | ✅ | ⚠️ | ⚠️ (1 test) | **PASS** | Entry count mismatch (74 vs 199) - investigate |
| sensor_data | 9 | ❌ | ❌ | ❌ | ✅ (12 tests) | **FAIL** | Exit code 5 - Unexpected (heavily tested!) 🔴 |
| app_metrics | 199 | ❌ | ❌ | ❌ | ⚠️ (1 test) | **FAIL** | Exit code 5 - Metrics schema parsing |
| log_entries | 199 | ❌ | ❌ | ❌ | ⚠️ (1 test) | **FAIL** | Exit code 5 - Log schema parsing |
| stock_prices | 2 | ❌ | ⚠️ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 0 - Zero entries (Issue #212: BTI offset) |
| tick_data | 23 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Tick data schema |
| time_bucketed_counters | 0 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Counter + empty data |
| user_activity | 199 | ❌ | ❌ | ❌ | ✅ (3 tests) | **FAIL** | Exit code 5 - Activity schema parsing |

### test_wide_rows (8 tables - 0 PASS / 8 FAIL) 🔴

| Table | Rows | Load | Parse | Count | Int Test | Status | Notes |
|-------|------|------|-------|-------|----------|--------|-------|
| wide_partition_table | 99 | ❌ | ❌ | ❌ | ✅ (14 tests) | **FAIL** | Exit code 5 - **CRITICAL** (core test table!) 🔴 |
| chat_messages | 49 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 3 - Chat schema parsing |
| document_versions | 49 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Document versioning schema |
| large_blob_table | 49 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Blob handling |
| many_columns_table | 49 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Wide schema (100 columns!) |
| multi_metric_timeseries | 49 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Multi-metric schema (30 metrics) |
| product_catalog | 49 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Catalog schema |
| sparse_data_table | 49 | ❌ | ❌ | ❌ | ❌ (0 tests) | **FAIL** | Exit code 5 - Sparse data handling |

---

## Recent Fixes (Issues #207, #208, #209)

### ✅ Issue #207: Byte-Comparable Key Encoding Support
**Status**: COMPLETED
**Impact**: Added CEP-25 byte-comparable key decoding for Cassandra 5.0 'newbig' format
**Files**: `cqlite-core/src/storage/sstable/reader/parsing/byte_comparable.rs` (NEW)
**Result**: V5_0NewBigFormat magic number (0xD4645400) now recognized

### ✅ Issue #208: BTI Index.db Format Support
**Status**: COMPLETED
**Impact**: Added dual-parser architecture for BTI partition key index format
**Files**: `cqlite-core/src/storage/sstable/index_reader.rs` (+366 LOC)
**Result**: Index.db now parses both MD5 digest format and BTI format partition keys

### ✅ Issue #209: Component Flattening Pre-allocation
**Status**: COMPLETED
**Impact**: Performance optimization reducing allocations from O(n) to O(1)
**Files**: `key_parsing.rs`, `row_cell_state_machine.rs`, `benches/component_flattening.rs` (NEW)
**Result**: 55-75% faster for 2-6 component keys (most common case)

### ✅ Issue #206: V5_0FormatG Counter Support
**Status**: COMPLETED
**Impact**: Added V5_0FormatG (0xAF030000) to header parsing routing
**Files**: `cqlite-core/src/parser/header.rs` (1-line fix)
**Result**: `counters` table now passing (9/33 total passing, +1)

---

## Known Issues and Blockers

### Critical Blockers (Impacting Multiple Tables)

#### 1. Issue #211: Partition Key Component Length Parsing Failures (16 tables) - P0
**Impact**: 48.5% of all tables (LARGEST BLOCKER)
**Status**: OPEN - Newly created
**Root Causes**:
- Byte offset miscalculation in compressed blocks
- VInt decoding consuming wrong number of bytes
- Possible regression from Issue #207 byte-comparable changes

**Affected Tables**:
- test_basic: `uncompressed_table` 🔴
- test_collections: `collection_clustering_table`, `empty_collections_table`, `large_collections_table`, `nested_collections_table` (4 tables)
- test_timeseries: `sensor_data` 🔴, `app_metrics`, `log_entries`, `tick_data`, `time_bucketed_counters`, `user_activity` (6 tables)
- test_wide_rows: `document_versions`, `large_blob_table`, `many_columns_table`, `multi_metric_timeseries`, `product_catalog`, `sparse_data_table`, `wide_partition_table` 🔴 (7 tables)

**Recommended Actions**:
1. Git bisect to confirm if Issue #207 introduced regression
2. Hex dump analysis comparing failing vs passing tables
3. Add debug logging for offset/length tracking
4. Fix `wide_partition_table`, `sensor_data`, `uncompressed_table` first (Tier 1 tables)

#### 2. Issue #210: SerializationHeader Extraction Failures (4 tables) - P0
**Impact**: 12.1% of all tables
**Status**: OPEN - Newly created
**Root Cause**: enhanced_statistics_parser.rs cannot locate SerializationHeader in Statistics.db for tables with static columns, frozen types, or complex clustering

**Affected Tables**:
- test_basic: `static_columns_table`
- test_collections: `frozen_collections_table`, `typed_collections_table`
- test_wide_rows: `chat_messages`

**Recommended Actions**:
1. Hex dump Statistics.db for affected tables
2. Compare with working tables (simple_table, basic_int_table)
3. Research Cassandra 5.0 Statistics.db SerializationHeader location patterns
4. Validate parser logic in `enhanced_statistics_parser.rs`

#### 3. Issue #212: BTI Index Zero Entries (1 table) - P1
**Impact**: 1 table (`stock_prices`) - Silent data loss
**Status**: OPEN - Newly created
**Root Cause**: BTI offset extraction fails, sequential scan fallback returns 0 entries

**Recommended Actions**:
1. Verify BTI offset extraction logic in `index_reader.rs`
2. Validate sequential scan fallback path
3. Hex dump Index.db to examine raw BTI entry structure
4. Compare with working BTI tables

#### 4. Nested Collections Parsing (2 tables) - P1
**Impact**: 2 tables (`nested_collections_table`, `collections_with_udts`)
**Status**: Feature gap (already failing via Issue #210 SerializationHeader)
**Complexity**: High (recursive parser required)

#### 5. Entry Count Mismatches (6 tables) - P2
**Impact**: 6 passing tables show entry count != partition count
**Tables**: `composite_key_table`, `multi_partition_table`, `ttl_test_table`, `event_store`, `user_sessions`
**Status**: Not blocking (tables pass), but indicates potential issues:
- Multi-row partitions (clustering keys)
- TTL/tombstone data affecting counts
- Schema-aware parsing differences from sstabledump

**Recommended Action**: Investigate count differences to ensure correct partition/row parsing

---

## Integration Test Coverage Analysis

### Coverage Tiers

#### Tier 1: Heavy Coverage (5+ test file references)
**8 tables with extensive test coverage**

| Table | Test References | Status | Notes |
|-------|----------------|--------|-------|
| simple_table | 23 | ✅ PASS | Gold standard validation table |
| wide_partition_table | 14 | ❌ FAIL | **CRITICAL** - core test table failing! 🔴 |
| sensor_data | 12 | ❌ FAIL | **CRITICAL** - heavily tested table failing! 🔴 |
| collection_table | 12 | ✅ PASS | Primary collection validation |
| compression_test_table | 11 | ✅ PASS | LZ4 compression validation |
| composite_key_table | 9 | ✅ PASS | Composite key validation |
| multi_partition_table | 7 | ✅ PASS | Multi-partition testing |
| uncompressed_table | 5 | ❌ FAIL | **CRITICAL** - should be simplest case! 🔴 |

**Action Required**: Fix failing Tier 1 tables immediately - these are core validation tables with extensive test coverage.

#### Tier 2: Moderate Coverage (2-4 test file references)
**9 tables with moderate testing**

| Table | Test References | Status | Keyspace |
|-------|----------------|--------|----------|
| ttl_test_table | 5 | ✅ PASS | test_basic |
| nested_collections_table | 4 | ❌ FAIL | test_collections |
| collection_clustering_table | 3 | ❌ FAIL | test_collections |
| user_activity | 3 | ❌ FAIL | test_timeseries |
| counters | 2 | ❌ FAIL | test_basic |
| large_collections_table | 2 | ❌ FAIL | test_collections |
| empty_collections_table | 1 | ❌ FAIL | test_collections |
| frozen_collections_table | 1 | ❌ FAIL | test_collections |
| typed_collections_table | 1 | ❌ FAIL | test_collections |

**Status**: 1/9 passing (11.1%)

#### Tier 3: Minimal/No Coverage (0-1 test file references)
**16 tables with minimal testing**

**test_collections**:
- collections_with_udts (1 test) - ❌ FAIL

**test_timeseries**:
- app_metrics (1 test) - ❌ FAIL
- event_store (1 test) - ✅ PASS
- log_entries (1 test) - ❌ FAIL
- user_sessions (1 test) - ✅ PASS
- stock_prices (0 tests) - ❌ FAIL
- tick_data (0 tests) - ❌ FAIL
- time_bucketed_counters (0 tests) - ❌ FAIL

**test_wide_rows** (ALL with 0 tests):
- chat_messages - ❌ FAIL
- document_versions - ❌ FAIL
- large_blob_table - ❌ FAIL
- many_columns_table - ❌ FAIL
- multi_metric_timeseries - ❌ FAIL
- product_catalog - ❌ FAIL
- sparse_data_table - ❌ FAIL

**Status**: 2/16 passing (12.5%)
**Critical Gap**: All 8 test_wide_rows tables have ZERO integration test coverage and ALL are failing 🔴

---

## Test Infrastructure Coverage

### Integration Test Files Referencing Test Tables

**Core Integration Tests** (23 test files reference test tables):
- `common/sstable_test_utils.rs` - Shared utilities for all tables
- `counter_type_integration_test.rs` - Counter type specific
- `collection_sstable_integration_test.rs` - Collection tables
- `crc32_header_checksum_test.rs` - Header validation
- `debug_schema_extraction.rs` - Schema debugging
- `documentation_examples_validation_test.rs` - Doc examples
- `enhanced_index_operation_tests.rs` - Index operations
- `index_db_offset_calculation_tests.rs` - Index offset math
- `index_db_parsing_regression_tests.rs` - Index regression tests
- `index_size_zero_integration_test.rs` - Zero-size index edge cases
- `index_summary_correlation_test.rs` - Summary validation
- `issue_154_test.rs` - UDT support (Issue #154)
- `m1_memory_validation.rs` - M1 memory constraints
- `nb_format_integration_test.rs` - NB format validation
- `reference_data_parity.rs` - JSONL parity validation
- `schema_aggregator_integration_test.rs` - Schema aggregation
- `schema_aware_reader_integration_test.rs` - Schema-aware reading
- `sstable_component_discovery_tests.rs` - Component discovery
- `sstable_discovery_comprehensive_tests.rs` - Table discovery
- `sstabledump_parity_index.rs` - sstabledump validation
- `statistics_db_real_file_test.rs` - Statistics.db parsing
- `v5_compressed_legacy_integration_test.rs` - V5 compressed legacy format
- `v5_compressed_legacy_parity_test.rs` - V5 parity checks

**Coverage Gap**: No dedicated integration tests for test_wide_rows tables (explains 0% pass rate)

---

## Recommendations

### Immediate Priorities (P0 - M1 Completion Blockers)

#### 1. Fix Tier 1 Failing Tables (CRITICAL)
**Tables**: `wide_partition_table`, `sensor_data`, `uncompressed_table`
**Why**: These are core validation tables with 5+ integration tests each. Their failure indicates systemic issues.
**Action**: Debug with verbose logging to identify exact failure points.

#### 2. Create test_wide_rows Integration Tests (CRITICAL)
**Why**: 0% pass rate + 0 integration test coverage = blind spot
**Action**: Create `wide_rows_integration_test.rs` covering at least:
- `wide_partition_table` (core M1 table)
- `many_columns_table` (stress test)
- `sparse_data_table` (edge case)

#### 3. Categorize Exit Code 5 Failures
**Why**: 18 tables (54.5%) failing with generic "internal error"
**Action**: Run each failing table with `RUST_LOG=debug` and categorize by root cause:
- Schema extraction errors
- Type system gaps
- Index/metadata parsing
- Row unmarshalling failures

### Short-Term Priorities (P1 - M2 Foundation)

#### 4. Implement Static Column Support
**Impact**: Fixes `static_columns_table`
**Complexity**: Medium (new parsing path required)
**Benefit**: Unlocks partition-level data feature

#### 5. Complete Frozen Type Support
**Impact**: Fixes `frozen_collections_table`
**Complexity**: Medium (different serialization format)
**Benefit**: Unlocks frozen collections feature

#### 6. Verify and Fix Issue #154 (UDT Support)
**Impact**: Fixes `collections_with_udts`
**Status**: May already be fixed in recent commits
**Action**: Re-run `issue_154_test.rs` and smoke test

#### 7. Add Nested Collection Parsing
**Impact**: Fixes `nested_collections_table`
**Complexity**: High (recursive parser required)
**Benefit**: Enables complex collection types

### Medium-Term Priorities (P2 - Feature Completeness)

#### 8. Implement Counter Type Support
**Impact**: Fixes `counters`, `time_bucketed_counters`
**Complexity**: Medium (special counter semantics)
**Benefit**: Unlocks counter column feature

#### 9. Investigate Entry Count Mismatches
**Impact**: 6 tables showing count != partition count
**Complexity**: Low (debugging exercise)
**Benefit**: Ensures correct multi-row partition handling

#### 10. Add Integration Tests for Tier 3 Tables
**Impact**: 16 tables with minimal coverage
**Benefit**: Catch regressions early, improve confidence

---

## Success Metrics

### M1 Completion Criteria (Storage Layer)
- [ ] **80%+ pass rate** (26/33 tables passing)
- [ ] All Tier 1 tables passing (8/8)
- [ ] test_basic: 7/8 passing (counters excepted)
- [ ] test_collections: 5/8 passing (frozen/nested excepted)
- [ ] test_timeseries: 6/9 passing (counters/advanced excepted)
- [ ] test_wide_rows: 4/8 passing (basic coverage)

### M2 Completion Criteria (Query Engine)
- [ ] **90%+ pass rate** (30/33 tables passing)
- [ ] All keyspaces at 75%+ pass rate
- [ ] All Tier 1 tables passing (8/8)
- [ ] Counter support implemented
- [ ] Static column support implemented
- [ ] Frozen type support implemented

### M3 Completion Criteria (Production Ready)
- [ ] **100% pass rate** (33/33 tables passing)
- [ ] All feature gaps closed
- [ ] All integration test coverage at Tier 2+
- [ ] Entry count parity with sstabledump

---

## Validation Artifacts

### Smoke Test Results
- **Script**: `test-data/scripts/smoke-test-all-tables.sh`
- **Output Directory**: `test-data/scripts/smoke-test-all-tables-results/`
- **Format**: JSON output per table (from `read-sstable --format json`)
- **Last Run**: 2025-10-30

### Reference Data
- **JSONL Files**: `test-data/datasets/sstables/{keyspace}/{table}/*.jsonl`
- **Source**: Generated via `sstabledump` from Cassandra 5.0
- **Coverage**: 33/33 tables (100%)
- **Format**: One line per partition (nested JSON)

### Integration Tests
- **Location**: `cqlite-core/tests/*.rs`
- **Count**: 23 test files reference test tables
- **Coverage**: Heavy (8 tables), Moderate (9 tables), Minimal (16 tables)

---

## Appendix: Quick Reference

### By Status
- **PASSING (8)**: simple_table, composite_key_table, compression_test_table, multi_partition_table, ttl_test_table, collection_table, event_store, user_sessions
- **CRITICAL FAILURES (3)**: wide_partition_table, sensor_data, uncompressed_table (Tier 1 tables)
- **FEATURE GAPS (7)**: Exit code 3 failures (static columns, frozen types, UDTs, nested collections)
- **SCHEMA ISSUES (18)**: Exit code 5 failures (schema extraction, type system, parsing)

### By Keyspace
- **test_basic**: 5/8 PASS (62.5%)
- **test_collections**: 1/8 PASS (12.5%)
- **test_timeseries**: 2/9 PASS (22.2%)
- **test_wide_rows**: 0/8 PASS (0.0%) 🔴

### By Row Count
- **Large (500+)**: simple_table (999), collection_table (499)
- **Medium (100-499)**: 7 tables in test_basic, 6 tables in test_timeseries
- **Small (1-99)**: 20 tables (mostly test_collections and test_wide_rows)
- **Empty/Minimal**: time_bucketed_counters (0), stock_prices (2), sensor_data (9)

---

**Next Steps**:
1. Debug Tier 1 failures with verbose logging
2. Create test_wide_rows integration tests
3. Categorize exit code 5 failures by root cause
4. Implement static column and frozen type support

**Owner**: CQLite Core Team
**Tracking**: Issue #200
**Validation Command**: `bash test-data/scripts/smoke-test-all-tables.sh`

# CQLite Issue Archive Index

This directory contains historical issue investigation files, code reviews, and research documents that were previously stored at the project root. These documents provide valuable context on how various features were implemented and bugs were resolved during the development of CQLite.

**Archive Created**: 2025-12-15
**Total Documents**: 100+ files spanning Issues #119 through #253

---

## Table of Contents

- [Issue-Specific Documentation](#issue-specific-documentation)
  - [Issue #119: QueryEngine Handoff](#issue-119-queryengine-handoff)
  - [Issue #140: SELECT * Empty JSON Bug](#issue-140-select--empty-json-bug)
  - [Issue #143: Implementation Summary](#issue-143-implementation-summary)
  - [Issue #158: Schema Wiring & Value Extraction](#issue-158-schema-wiring--value-extraction)
  - [Issue #159: Critical Analysis](#issue-159-critical-analysis)
  - [Issue #160: V5CompressedLegacy Parser Development](#issue-160-v5compressedlegacy-parser-development)
  - [Issue #162: Collection Parsing](#issue-162-collection-parsing)
  - [Issue #164: V5CompressedLegacy Cell Parsing Fix](#issue-164-v5compressedlegacy-cell-parsing-fix)
  - [Issue #165: Multi-Row Parsing](#issue-165-multi-row-parsing)
  - [Issue #166: Multi-Partition Support](#issue-166-multi-partition-support)
  - [Issue #168: Review Summary](#issue-168-review-summary)
  - [Issue #191: Schema Loading from CQL Files](#issue-191-schema-loading-from-cql-files)
  - [Issue #192: Fix Summary](#issue-192-fix-summary)
  - [Issue #195: Cassandra Expert Handoff](#issue-195-cassandra-expert-handoff)
  - [Issue #196: Fix Summary](#issue-196-fix-summary)
  - [Issue #198: Test Coverage Audit](#issue-198-test-coverage-audit)
  - [Issue #253: Execution Path Divergence Investigation + SQL→CQL Terminology](#issue-253-execution-path-divergence-investigation--sqlcql-terminology)
- [Format Research & Reverse Engineering](#format-research--reverse-engineering)
  - [Cassandra 5.0 Format Specifications](#cassandra-50-format-specifications)
  - [Magic Number Analysis](#magic-number-analysis)
  - [Serialization Header Research](#serialization-header-research)
- [Implementation & Analysis Reports](#implementation--analysis-reports)
- [Code Reviews & Guidelines](#code-reviews--guidelines)
- [Planning & Strategy Documents](#planning--strategy-documents)

---

## Issue-Specific Documentation

### Issue #119: QueryEngine Handoff
**File**: `ISSUE_119_HANDOFF.md`
**Topic**: QueryEngine component handoff documentation
**Status**: Completed handoff
**Key Content**: Documentation for transitioning QueryEngine ownership between development teams

---

### Issue #140: SELECT * Empty JSON Bug
**Files**:
- `ISSUE_140_QUICK_REFERENCE.md` - Quick reference card for testing and validation
- `ISSUE_140_TEST_EXECUTION_PLAN.md` - Test execution strategy
- `ISSUE_140_VALIDATION_GUIDE.md` - Comprehensive validation procedures
- `TESTING_SUMMARY_ISSUE_140.md` - Final testing summary

**Topic**: SELECT * queries returning empty JSON objects instead of column data
**Status**: ✅ RESOLVED
**Root Cause**: Column value extraction bug in query executor
**Solution**: Fixed value extraction logic to properly populate JSON results

**Key Content**:
- Pre-push testing checklist
- Three-tier testing strategy (minimal, quick, comprehensive)
- Test data validation procedures
- CI/CD integration guidelines

**Before**: `[{}, {}, {}]`
**After**: `[{"id": "123", "name": "test", ...}, ...]`

---

### Issue #143: Implementation Summary
**File**: `IMPLEMENTATION_SUMMARY_ISSUE_143.md`
**Topic**: Feature implementation summary (specific feature TBD from file content)
**Status**: Completed

---

### Issue #158: Schema Wiring & Value Extraction
**Files**:
- `ISSUE_158_CODE_REVIEW.md` - Initial code review
- `ISSUE_158_CODE_REVIEW_SUMMARY.md` - Review summary
- `ISSUE_158_COMPLETE_CODE_REVIEW_REPORT.md` - Complete review report
- `ISSUE_158_DEV_HANDOFF.md` - Development team handoff
- `ISSUE_158_FINAL_CODE_REVIEW.md` - Final review
- `ISSUE_158_FINAL_REVIEW_REPORT.md` - Final review report
- `ISSUE_158_REVIEW_FOR_PATRICK.md` - Executive summary for project owner

**Topic**: Schema-aware parsing integration for Cassandra 5.0 SSTable data
**Status**: ⚠️ INCOMPLETE - Good code but missing critical components
**Date**: October 14, 2025

**What Was Completed**:
- ✅ Phase 3: Schema wiring through parser (commit 32ddd19)
- ✅ Phase 4: Value extraction logic
- ✅ Excellent code quality (5-star rating)

**Gaps Identified**:
- ❌ Phase 1-2 changes (format detection/routing) uncommitted
- ⛔ Release binary is stale (built before changes)
- ❌ Schema lookup broken (wrong table name format)
- ❌ No integration tests

**Estimated Fix Time**: 5-9 hours
1. Commit Phase 1-2 changes (15 min)
2. Rebuild binary (1 min)
3. Fix schema lookup (2-4 hours)
4. Add integration tests (2-4 hours)

---

### Issue #160: V5CompressedLegacy Parser Development
**Files** (15 documents):
- `ISSUE_160_BYTE_ANALYSIS.md` - Binary format byte-level analysis
- `ISSUE_160_CODE_REVIEW_CRITICAL_FINDING.md` - Critical bugs identified
- `ISSUE_160_CRITICAL_FINDINGS.md` - Additional critical issues
- `ISSUE_160_FINAL_CODE_REVIEW_REPORT.md` - Final comprehensive review
- `ISSUE_160_HANDOFF_TO_IMPLEMENTATION_TEAM.md` - Implementation team handoff
- `ISSUE_160_HANDOFF_TO_PATRICK.md` - Executive handoff
- `ISSUE_160_JAVA_SOURCE_FINDINGS.md` - Cassandra Java source code analysis
- `ISSUE_160_OFFSET_FIX_SUMMARY.md` - Offset calculation bug fixes
- `ISSUE_160_PHASE_2_SUMMARY.md` - Phase 2 completion summary
- `ISSUE_160_PHASE_3_IMPLEMENTATION_REPORT.md` - Phase 3 implementation
- `ISSUE_160_RECOMMENDED_FIXES.md` - Bug fix recommendations
- `ISSUE_160_REVIEW_FOR_PATRICK.md` - Executive summary
- `ISSUE_160_REVIEW_SUMMARY_FOR_PATRICK.md` - Review summary
- `ISSUE_160_ROW_FORMAT_RESEARCH_SUMMARY.md` - Row format research
- `ISSUE_160_STATUS_REPORT.md` - Overall status report
- `PHASE_1_COMPLETE_ISSUE_160.md` - Phase 1 completion marker

**Topic**: Complete implementation of V5CompressedLegacy SSTable format parser for Cassandra 5.0
**Status**: ✅ PARSER COMPLETE - Trivial scanner bug found
**Date**: October 15, 2025
**Timeline**: <1 hour to fix scanner bug

**Achievement**:
- ✅ 592 lines of production code
- ✅ Format research completed
- ✅ All 744 tests passing
- ✅ Extracts rows from all 41 blocks
- ✅ No parsing errors
- ✅ Code Quality: ⭐⭐⭐⭐⭐ Excellent

**Bug Found**:
- Parser returns: `table_id='test_basic.simple_table'` (with keyspace)
- Scanner expects: `table_id='simple_table'` (without keyspace)
- Result: All 41 rows filtered out (mismatch causes 0 results)

**Fix Options**:
1. **Option A**: Fix scanner matching logic (30 min) - Accept partial table ID matches
2. **Option B**: Strip keyspace from parser output (30 min) - Normalize table IDs

**Development Phases**:
- Phase 1: Format research and specification
- Phase 2: Basic parser implementation
- Phase 3: Full feature implementation and testing

---

### Issue #162: Collection Parsing
**File**: `ISSUE_162_TASK_3_COLLECTION_PARSING_HANDOFF.md`
**Topic**: Collection type parsing (lists, sets, maps) implementation handoff
**Status**: Handoff to implementation team

---

### Issue #164: V5CompressedLegacy Cell Parsing Fix
**Files**:
- `ISSUE_164_CURRENT_STATUS.md` - Current implementation status
- `ISSUE_164_DEBUG_FINDINGS.md` - Debugging session findings
- `ISSUE_164_IMPLEMENTATION_SUMMARY.md` - Complete implementation summary
- `ISSUE_164_REVIEW_CHECKLIST.md` - Review checklist
- `ISSUE_164_ROOT_CAUSE_IDENTIFIED.md` - Root cause analysis
- `ISSUE_164_TEAM_FEEDBACK_RESPONSE.md` - Team feedback response
- `ISSUE_164_V5_COMPRESSED_LEGACY_CELL_FIX.md` - Cell parsing fix details

**Topic**: V5CompressedLegacy cell parsing returning null values instead of actual data
**Status**: ✅ COMPLETE - All tests passing, ready for CI

**Problems Solved**:

**1. Null Cell Values (Primary Issue)**
- **Root Cause**: Partition keys incorrectly included in `schema.columns` list
- **Fix**: Added filtering logic to exclude partition/clustering keys from cell parsing loop
- **Lines**: 647-670 in row_decoder

**2. Incomplete Cell Type Encodings**
- **Root Cause**: Mixed assumptions about length prefixes in V5CompressedLegacy format
- **Fix**: Implemented comprehensive type encoding (lines 769-1409)
  - **Fixed-Size Types (NO length prefix)**: boolean, int, bigint, counter, double, float, timestamp, time, timeuuid
  - **Variable-Size Types (VInt length prefix)**: text, blob, uuid, decimal, varint, duration, date, smallint, tinyint, inet

**Test Results**: 18/18 column types now parse correctly with proper type-aware deserialization

---

### Issue #165: Multi-Row Parsing
**File**: `ISSUE_165_V5_COMPRESSED_LEGACY_MULTI_ROW_PARSING.md`
**Topic**: Support for parsing multiple rows within V5CompressedLegacy partitions
**Status**: Implementation complete (related to Issue #166)

---

### Issue #166: Multi-Partition Support
**Files**:
- `ISSUE_166_CODE_REVIEW_FINAL.md` - Final code review
- `ISSUE_166_COMPLETION_SUMMARY.md` - Completion summary
- `ISSUE_166_FIX_SUMMARY.md` - Fix summary
- `ISSUE_166_VERIFICATION_REPORT.md` - Verification report

**Topic**: Support multi-partition blocks in V5CompressedLegacy SSTable reader
**Status**: ✅ RESOLVED - CI Green
**Commit**: `a832b9b`
**Date**: 2025-10-19

**The Problem**: Parser stopped after reading only 1 partition instead of all 1000 partitions in decompressed block

**Root Cause**: V5CompressedLegacy format includes a mandatory 4-byte trailing field after each row's cell data that is NOT included in the `row_size` field from row header. Parser wasn't accounting for this, causing incorrect offset calculations.

**The Solution**:
- Added `ROW_TRAILING_FIELD_SIZE = 4` constant
- Updated partition boundary calculation: `next_offset = input_offset + row_size + 4`
- Enhanced documentation with complete format structure
- Added debug logging for trailing field bytes

**Test Results**:
- **Before Fix**: Parsed 1/1000 partitions (0.1% success rate)
- **After Fix**: Parsed 1000/1000 partitions (100% success rate) ✅

**Impact**: Full support for multi-partition blocks, large SSTables (1000+ partitions, ~663KB decompressed), and all 18+ Cassandra 5.0 CQL types

---

### Issue #168: Review Summary
**File**: `ISSUE_168_REVIEW_SUMMARY.md`
**Topic**: Code review summary (specific scope TBD from file content)
**Status**: Review completed

---

### Issue #191: Schema Loading from CQL Files
**Files**:
- `ISSUE_191_ASSISTANCE_REQUEST.md` - Request for assistance
- `ISSUE_191_FIX_REPORT.md` - Fix implementation report
- `ISSUE_191_REVIEW_REQUEST.md` - Review request
- `ISSUE_191_STATUS_REPORT.md` - Status report

**Topic**: SELECT queries returning `Value::Null` due to schema loading issues
**Status**: ✅ RESOLVED

**Problem**: Schemas from `--schema` CQL file weren't being loaded into SchemaManager with correct keyspace

**Root Cause**: SchemaAggregator in `cqlite-core/src/schema/aggregator.rs` was **ignoring `USE` statements**, causing all tables to be registered with default keyspace `"default"` instead of the actual keyspace (e.g., `"test_basic"`).

**Evidence from CI**:
```
[EXECUTOR] No schema found for test_basic.simple_table, proceeding without schema-aware parsing
[EXECUTOR] Scan returned 29 rows
Error: Column not found: name
```

**Implementation**:
1. Added `extract_use_keyspace()` helper to parse `USE <keyspace>` statements
2. Added `extract_create_keyspace_name()` helper for `CREATE KEYSPACE` statements
3. Modified `parse_cql_file()` to track active keyspace
4. Modified CREATE TABLE parsing to override default keyspace with active keyspace

**Files Modified**: `cqlite-core/src/schema/aggregator.rs` (lines 206-224)

---

### Issue #192: Fix Summary
**File**: `ISSUE_192_FIX_SUMMARY.md`
**Topic**: Bug fix summary (specific bug TBD from file content)
**Status**: Fix implemented

---

### Issue #195: Cassandra Expert Handoff
**File**: `ISSUE_195_HANDOFF_TO_CASSANDRA_EXPERT.md`
**Topic**: Handoff to Cassandra format expert for complex format issues
**Status**: Handoff completed

---

### Issue #196: Fix Summary
**File**: `ISSUE_196_FIX_SUMMARY.md`
**Topic**: Bug fix summary (specific bug TBD from file content)
**Status**: Fix implemented

---

### Issue #198: Test Coverage Audit
**Files**:
- `ISSUE_198_COMPLETE_HANDOFF.md` - Complete handoff documentation
- `ISSUE_198_COMPLETE_SUMMARY.md` - Complete summary
- `ISSUE_198_SKIP_PATTERN_REMOVAL_SUMMARY.md` - Skip pattern removal details
- `SKIP_PATTERN_REMOVAL_SUMMARY.md` - Additional skip pattern documentation

**Topic**: Address discrepancy between claimed 95% coverage and actual 33.7% coverage
**Status**: ✅ PHASE 1 COMPLETE - Test Skip Pattern Removal

**Objective**: Fix false positive test coverage from silent test skips

**Completed Work**:
- ✅ Removed ~55 test skip patterns creating false positives
- ✅ Fixed all P0 blocking issues from code review
- ✅ CI is GREEN (fmt, clippy, build, tests all pass)
- ✅ Tests now fail loudly when prerequisites missing

**Coverage Metrics**:
- **Current**: 33.7% (7,159 / 21,223 lines) - ACCURATE measurement
- **PRD Target**: 95% unit-test coverage (M1 exit criterion)
- **Gap**: 61.3 percentage points

**Problem Identified**: 14 test files contained silent skip patterns:
```rust
if !path.exists() {
    println!("⏭️  Skipping test...");
    return; // Test passes without validation - FALSE POSITIVE
}
```

**Solution**: Converted to fail-fast assertions:
```rust
let path = find_file(...).unwrap_or_else(|| {
    panic!("Test requires full SSTable dataset: clear error message")
});
```

**Files Modified**: 14 test files including:
- `index_summary_correlation_test.rs`
- `index_db_parsing_regression_tests.rs`
- `schema_aware_reader_integration_test.rs`
- `counter_type_integration_test.rs`
- And 10 more integration test files

---

### Issue #253: Execution Path Divergence Investigation + SQL→CQL Terminology

**Files**:
- `docs/research/issue-253-advanced-execution-path-research.md` - ADVANCED path research
- `cqlite-core/tests/execution_path_parity_tests.rs` - Parity test suite with root cause validation

**Topic**: Root cause analysis for query execution path divergence and SQL→CQL terminology fix
**Status**: ✅ INVESTIGATION COMPLETE
**Date**: 2026-01-07

**Root Cause Analysis**:

The two execution paths serve fundamentally different purposes:

| Aspect | LEGACY Path | ADVANCED Path |
|--------|-------------|---------------|
| **Purpose** | Synthetic INSERT/SELECT testing | Real SSTable reading |
| **Location** | `executor.rs:794-805` | `select_executor.rs:1095-1189` |
| **Key format** | `format!("user_key_{}", id)` | Schema-aware binary decoding |
| **Limitation** | Only "id" column + Integer type | Supports all CQL key types |
| **Correct for** | In-memory testing only | Production SSTable data |

**The Bug**: LEGACY key generation violates the No-Heuristics Mandate (Issue #28) by:
1. Hardcoding column name "id" instead of using schema metadata
2. Using synthetic text keys that don't match Cassandra partition key format
3. Only supporting Integer type, ignoring uuid/text/composite keys

**Why the 8-Token Heuristic Exists** (`engine.rs:132-142`):

`SELECT * FROM ks.table WHERE id = 1` = exactly 8 whitespace-separated tokens.

The routing hack sends ≤8 token queries with "WHERE id =" to LEGACY path to maintain
compatibility with the synthetic INSERT feature (also uses `user_key_{}` format).
This is a workaround for the INSERT bug, not a feature.

**Deliverables Completed**:
- ✅ Root cause identified: LEGACY uses synthetic keys, ADVANCED uses real Cassandra keys
- ✅ 8-token heuristic explained: maintains INSERT compatibility
- ✅ Parity test suite validates divergence exists in codebase
- ✅ SQL→CQL terminology fixed (262 occurrences across 8 files)

**Unification Strategy Design Proposal**:

**Phase 1: Fix LEGACY INSERT** (Recommended first step)
- Update `executor.rs` INSERT to use schema-aware key generation
- Match the `decode_partition_key_value()` approach from ADVANCED path
- Code location: `executor.rs:857-863` (INSERT key generation)

**Phase 2: Remove Routing Hack**
- Once INSERT generates correct keys, remove `engine.rs:132-142` routing logic
- All SELECT queries go through ADVANCED path
- Test: Verify INSERT → SELECT roundtrip with real key formats

**Phase 3: Deprecate LEGACY Path**
- Move LEGACY executor behind feature flag `legacy_executor`
- Default to ADVANCED-only execution
- Eventually remove LEGACY code entirely

**Alternative: Deprecate INSERT Feature**
- The INSERT feature is behind `experimental` flag and primarily for testing
- Could remove entirely rather than fix, simplifying architecture
- SSTable files are read-only in production anyway

**Future Work** (separate issues):
- [ ] Fix LEGACY INSERT key generation (Phase 1)
- [ ] Remove routing hack after INSERT fix (Phase 2)
- [ ] Rename `state_machine` feature flag (see Issue #252)

---

## Format Research & Reverse Engineering

### Cassandra 5.0 Format Specifications

**Files**:
- `CASSANDRA_5_CELL_DESERIALIZATION_FORMAT.md` - Cell deserialization format specification
- `CASSANDRA_5_INDEX_DB_FORMAT_RESEARCH.md` - Index.db binary format reverse engineering
- `CASSANDRA_50_FORMAT_SPECIFICATION.md` - Comprehensive Cassandra 5.0 format spec
- `CASSANDRA_50_ROW_FORMAT_RESEARCH.md` - Row format detailed research
- `CASSANDRA_VALUE_READING_RESEARCH.md` - Value reading mechanisms
- `CASSANDRA5_PARSING_EXECUTIVE_SUMMARY.md` - Executive summary of parsing work
- `INDEX_DB_FORMAT_VALIDATION_REPORT.md` - Index.db format validation

**Topic**: Comprehensive reverse engineering of Cassandra 5.0 SSTable binary formats
**Date**: October 2025

**Key Achievements**:

**Index.db Format Discovery**:
- ✅ FORMAT COMPLETELY REVERSE-ENGINEERED
- Validated against 1,000 real partition entries
- **Critical Finding**: NO HEADER in nb format (contrary to header_spec.rs)
- Documented complete binary structure for partition index entries

**Research Scope**:
- Cell deserialization algorithms
- Row data layout and encoding
- Value extraction mechanisms
- Partition index structures
- Test data: `test_basic/simple_table` (nb-1 format)

**Impact**: Enabled accurate parsing of Cassandra 5.0 SSTable files without relying on incomplete documentation

---

### Magic Number Analysis

**Files**:
- `CASSANDRA_MAGIC_NUMBER_RESEARCH_REPORT.md` - Initial research
- `MAGIC_NUMBER_COMPARISON_REPORT.md` - Comparison analysis
- `MAGIC_NUMBER_FIX_RECOMMENDATIONS.md` - Fix recommendations
- `MAGIC_NUMBER_REVERSE_ENGINEERING_REPORT.md` - Reverse engineering results
- `MAGIC_NUMBER_VISUAL_ANALYSIS.md` - Visual analysis of magic numbers

**Topic**: Analysis of Cassandra SSTable magic numbers and version identification

**Key Finding**: Magic numbers are **opaque identifiers**, NOT structured bit-encoded formats. They serve as version/format signatures (like PNG's `0x89504E47`), not bitwise-encoded metadata containers.

**Supported Magic Numbers** (from header.rs, lines 138-155):
```rust
0x6F61_0000, // Legacy 'oa' format
0xAD01_0000, // Cassandra 5.0 Alpha
0xA007_0000, // Cassandra 5.0 Beta
0x4316_0000, // Cassandra 5.0 Release
0x0040_0000, // Cassandra 5.0 'nb' (new big) format
0x6461_0000, // Cassandra 5.0 BTI (Big Trie-Indexed) format
```

**Analysis Approach**: Pattern analysis, version correlation, byte position analysis

**Outcome**: Established authoritative magic number registry for format detection

---

### Serialization Header Research

**Files**:
- `SERIALIZATION_HEADER_ANALYSIS_SUMMARY.md` - Analysis summary
- `SERIALIZATION_HEADER_PARSER_ANALYSIS.md` - Parser analysis
- `SERIALIZATION_HEADER_PARSER_SPEC.md` - Parser specification
- `SERIALIZATION_HEADER_REVERSE_ENGINEERING.md` - Reverse engineering process
- `SERIALIZATION_HEADER_VISUAL_MAP.md` - Visual format map

**Topic**: Reverse engineering of Cassandra 5.0 Statistics.db SerializationHeader format
**Version**: 1.0
**Date**: October 29, 2025

**Binary Format Discovered**:
```
[VInt prefix?] [0x00 0x00]                    -- marker/alignment
[u8 len] [partition_key_type_string]          -- partition key type
[u8 count]                                     -- clustering key count
  [u8 len] [clustering_type_string]            -- for each clustering key
[0x00]                                         -- separator
[u8 count]                                     -- regular column count
  [u8 name_len] [name] [u8 type_len] [type]    -- for each column
```

**Research Method**: Binary analysis of Statistics.db files from test data

**Impact**: Enabled schema extraction from SSTable metadata without requiring external schema files

---

## Implementation & Analysis Reports

**General Implementation Files**:
- `AGGREGATOR_IMPLEMENTATION_SUMMARY.md` - SchemaAggregator implementation
- `ANALYSIS_REPORT.md` - General analysis report
- `CLI_ONE_SHOT_DEBUG_FINDINGS.md` - CLI one-shot mode debugging
- `FINAL_DECISION_SUMMARY.md` - Final decision documentation
- `FINAL_SUMMARY_FOR_PATRICK.md` - Executive summary for project owner
- `FLAG_0x24_INVESTIGATION_FINDINGS.md` - Investigation of 0x24 flag in binary format
- `HANDOFF_COMPLETE.md` - Component handoff completion
- `HANDOFF_TO_ISSUE_159.md` - Handoff to Issue #159
- `OPTIMIZATION_CODE_AUDIT.md` - Code optimization audit
- `PARTITION_KEY_LIMIT_FIX_SUMMARY.md` - Partition key limit fix
- `REPL_INGESTION_FIX_SUMMARY.md` - REPL ingestion bug fix
- `SESSION_COMPLETE_SUMMARY.md` - Development session summary
- `TYPES_OPTIMIZATION_SUMMARY.md` - Type system optimization
- `V5_COMPRESSED_LEGACY_FORMAT_RESEARCH_PLAN.md` - Research plan for V5CompressedLegacy
- `WRITER_VIABILITY_ASSESSMENT.md` - SSTable writer viability analysis

**Issue-Specific Lowercase Files**:
- `issue_126_review_summary.md` - Issue #126 review
- `issue_128_code_review.md` - Issue #128 code review
- `issue_128_review_summary.md` - Issue #128 review summary
- `review_findings_issue_126.md` - Issue #126 review findings

**Topics Covered**:
- SchemaAggregator implementation for CQL schema parsing
- CLI debugging and one-shot query mode fixes
- Flag 0x24 binary format investigation
- Type system performance optimization
- SSTable writer implementation assessment
- Partition key limit handling
- REPL ingestion pipeline fixes

---

## Code Reviews & Guidelines

**Files**:
- `CODE_REVIEW_GUIDELINES.md` - Code review standards and procedures
- `CODE_REVIEW_SUMMARY.md` - General code review summary
- `CQLITE_CORE_M1_M2_CODE_REVIEW.md` - M1/M2 milestone code review
- `PR_180_REVIEW.md` - Pull Request #180 review

**Topics**:
- Code review standards for CQLite project
- M1 (Core Reading Library) code review findings
- M2 (Query Engine) code review findings
- Pull request review procedures
- Quality gates and acceptance criteria

---

## Planning & Strategy Documents

**Files**:
- `cassandra5-parsing-fix-FINAL.plan.md` - Final parsing fix plan
- `cassandra5-parsing-fix-REVISED.plan.md` - Revised parsing fix plan
- `LOCAL_CI_TESTING.md` - Local CI testing procedures
- `PATRICK_DECISION_NEEDED.md` - Decision points requiring owner input
- `PROJECT_STRUCTURE.md` - Project structure documentation
- `RELEASE_NOTES_M1.md` - M1 milestone release notes

**Topics**:
- Strategic planning for Cassandra 5.0 parsing fixes
- Multi-phase implementation plans
- Local CI testing and validation procedures
- Project structure and organization
- M1 milestone achievements and release notes
- Decision points for project direction

---

## Document Organization

### Files by Category

**Issue Documentation**: 52 files covering Issues #119-#198
**Format Research**: 17 files on Cassandra 5.0 binary formats
**Implementation Reports**: 16 files on feature implementations
**Code Reviews**: 4+ files on code review processes
**Planning Documents**: 6 files on project planning and strategy

### Archive Conventions

1. **File Naming**: Original filenames preserved for traceability
2. **Issue Grouping**: Related documents grouped by issue number
3. **Cross-References**: Issues reference related research documents
4. **Status Markers**: ✅ Resolved, ⚠️ Incomplete, 🔄 In Progress

### Searching This Archive

**By Issue Number**: Search for "Issue #XXX" or "ISSUE_XXX"
**By Topic**: Search for keywords like "parser", "schema", "V5CompressedLegacy", "magic number"
**By Status**: Search for "✅ RESOLVED", "⚠️ INCOMPLETE", "COMPLETE"
**By Date**: Most documents include dates from October 2025

### Related Documentation

- **Current Issues**: See GitHub Issues at https://github.com/pmcfadin/cqlite/issues
- **Technical Docs**: See `/docs/technical/` for current specifications
- **User Guides**: See `/docs/user-guides/` for usage documentation
- **Development**: See `/docs/development/` for development guides

---

## Historical Significance

This archive represents the detailed development history of CQLite's M1 (Core Reading Library) and M2 (Query Engine) milestones, including:

- Complete reverse engineering of Cassandra 5.0 SSTable formats
- Implementation of V5CompressedLegacy parser (15+ documents)
- Schema-aware parsing integration
- Multi-partition block support
- Type system implementation for 18+ CQL types
- Test coverage improvements from false 95% to accurate 33.7%
- Code quality and review processes

These documents provide invaluable context for understanding design decisions, bug fixes, and format specifications that shaped the current codebase.

---

**Last Updated**: 2026-01-07
**Maintainer**: CQLite Development Team
**Archive Location**: `/docs/archive/issues/`

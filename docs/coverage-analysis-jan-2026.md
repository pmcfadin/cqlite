# CQLite Test Coverage Analysis Report
**Generated**: January 9, 2026
**Tool**: cargo tarpaulin with LLVM instrumentation

## Executive Summary

Current test coverage for cqlite-core is **30.54%** (8,457 / 27,690 lines), significantly below targets across all tiers. This reflects the early stage of test development for the core parsing infrastructure.

**Status**: All tiers FAILING targets
- Critical Tier: 33.31% (Target: 90%) - FAIL by 56.69 pp
- Important Tier: 43.10% (Target: 80%) - FAIL by 36.90 pp
- Supporting Tier: 51.36% (Target: 70%) - FAIL by 18.64 pp
- Utilities Tier: 14.63% (Target: 50%) - FAIL by 35.37 pp

---

## Coverage by Tier

### Critical Tier (Target: 90%+)
**Status**: FAIL - 33.31% average coverage

Critical infrastructure for SSTable parsing and row/column access:

- **Average Coverage**: 33.31% (2,046 / 7,379 lines)
- **Files**: 30 total
- **Pass Rate**: 0/30 files meet 90% target

**Files with 0% Coverage** (5 files, 425 lines):
1. `storage/sstable/reader/cache.rs` - 29 lines (unused cache layer)
2. `storage/sstable/reader/integrity.rs` - 53 lines (integrity checking)
3. `storage/sstable/reader/key_digest.rs` - 40 lines (key digest computation)
4. `storage/sstable/reader/parsing/block_entries.rs` - 218 lines (BTI parsing)
5. `storage/sstable/reader/partition_lookup.rs` - 85 lines (partition index lookup)

**Bottom 10 Files**:
| File | Coverage | Lines |
|------|----------|-------|
| cache.rs | 0.00% | 0/29 |
| integrity.rs | 0.00% | 0/53 |
| key_digest.rs | 0.00% | 0/40 |
| block_entries.rs | 0.00% | 0/218 |
| partition_lookup.rs | 0.00% | 0/85 |
| data_access.rs | 3.88% | 12/309 |
| block_io.rs | 4.29% | 9/210 |
| parsing/mod.rs | 6.48% | 16/247 |
| reader/mod.rs | 10.19% | 16/157 |
| component_loading.rs | 11.11% | 19/171 |

**Top 5 Files**:
- `parser/mod.rs`: 100.00% (2/2 lines)
- `parser/statistics.rs`: 89.35% (151/169 lines)
- `parser/header.rs`: 80.17% (186/232 lines)
- `byte_comparable.rs`: 76.27% (45/59 lines)
- `reader/types.rs`: 75.00% (3/4 lines)

**Key Parsing Files**:
- `parsing/key_parsing.rs`: 44.30% (66/149 lines) - COVERED but needs expansion
- `parsing/value_parsing.rs`: 33.60% (84/250 lines) - COVERED but needs expansion
- `parsing/block_entries.rs`: 0.00% (0/218 lines) - NOT COVERED ⚠️

---

### Important Tier (Target: 80%+)
**Status**: FAIL - 43.10% average coverage

Query engine and schema management:

- **Average Coverage**: 43.10% (2,626 / 7,588 lines)
- **Files**: 32 total
- **Pass Rate**: 0/32 files meet 80% target

**Files with <5% Coverage**:
- `cql/mod.rs`: 0.00% (0 lines)
- `query/select_optimizer.rs`: 0.89% (1/112 lines)
- `query/select_executor.rs`: 1.23% (2/163 lines)
- `schema/discovery.rs`: 4.84% (5/103 lines)

**Coverage Breakdown**:
- Query execution: 14.24% (executor.rs)
- Schema management: 4.84% (discovery.rs)
- Type system: Varies by module

---

### Supporting Tier (Target: 70%+)
**Status**: FAIL - 51.36% average coverage

Index and directory infrastructure:

- **Average Coverage**: 51.36% (671 / 1,614 lines)
- **Files**: 17 total
- **Pass Rate**: 0/17 files meet 70% target

**Lowest Coverage**:
- BTI parser: 17.72% (18/102 lines)
- BTI nodes: 21.20% (22/104 lines)
- Directory validation: 25.53% (21/82 lines)
- Directory TOC: 30.95% (25/81 lines)
- Directory mod: 32.12% (33/103 lines)

---

### Utilities Tier (Target: 50%+)
**Status**: FAIL - 14.63% average coverage

Benchmarks and testing infrastructure:

- **Average Coverage**: 14.63% (43 / 148 lines)
- **Files**: 2 total
- **Pass Rate**: 0/2 files meet 50% target

**Breakdown**:
- Benchmarks (zerocopy_benchmarks.rs): 0.00% (0/119 lines) - Not exercised in tests
- Testing helpers (dataset_helpers.rs): 29.25% (29/99 lines)

---

## Key Findings

### Positive Progress
1. **Key parsing infrastructure partially covered**:
   - `key_parsing.rs`: 44.30% coverage - demonstrates parsing logic is being tested
   - `value_parsing.rs`: 33.60% coverage - basic value extraction paths exercised

2. **Parser header and statistics**:
   - `parser/header.rs`: 80.17% coverage (nearly reaches target!)
   - `parser/statistics.rs`: 89.35% coverage (meets target!)

3. **High-level module coverage**:
   - `parser/mod.rs`: 100% coverage
   - Basic binary parsing working

### Critical Gaps

1. **Block Entry Indexing (BTI) - 0% Coverage** (218 lines)
   - `block_entries.rs` - completely untested
   - Affects ability to parse trie-based SSTables
   - Related: BTI parser at 17.72%, BTI nodes at 21.20%

2. **Reader Infrastructure - 0% Coverage** (290 lines across 5 files)
   - `cache.rs`: Not exercised
   - `integrity.rs`: Validation not tested
   - `key_digest.rs`: MD5/digest logic untested
   - `partition_lookup.rs`: Index lookup untested

3. **Query Engine - <15% Coverage**
   - Executor: 14.24%
   - Select optimizer: 0.89%
   - Select executor: 1.23%
   - CQL parser: 0.00%

4. **Schema Discovery - <5% Coverage** (4.84%)
   - Discovery infrastructure underdeveloped
   - Affects dataset introspection

---

## Coverage Trends

### Previous Milestones
- **M1 Completion (Dec 2025)**: Expected ~40-50% critical tier coverage
- **Current (Jan 2026)**: 33.31% critical tier coverage
- **Trend**: Regression likely due to new test gating (state_machine feature)

### Test Execution Notes
Tarpaulin detected several test binaries with zero coverage:
- `issue_222_select_integration_test`: No coverage recorded
- `issue_237_row_size_offset_regression_test`: No coverage recorded
- `execution_path_parity_tests`: No coverage recorded
- `query_correctness_tests`: No coverage recorded

These may be feature-gated or requiring specific flags to run.

---

## Recommended Coverage Improvements

### Phase 1: Critical (Must Fix)
1. **Block Entry Parsing** - Add 15-20 unit tests for BTI parsing
   - Target: 50% coverage in block_entries.rs
   - Effort: Medium (requires BTI format understanding)

2. **Reader Cache and Integrity** - Add functional tests
   - Target: 50%+ coverage in cache.rs, integrity.rs
   - Effort: Low (wrapping/validation code)

3. **Key Parsing Expansion** - Increase from 44.30% to 90%
   - Target: +83 additional lines covered in key_parsing.rs
   - Effort: Medium (edge cases, error conditions)

### Phase 2: Important (Should Fix)
1. **Query Executor Tests** - From 14.24% to 60%+
   - Add execution path coverage for all query types
   - Effort: High (requires query engine implementation)

2. **CQL Parser Tests** - From 0.00% to 30%+
   - Parse diverse CQL statement types
   - Effort: Medium (parser implementation + test data)

### Phase 3: Supporting (Nice to Have)
1. **BTI Index Coverage** - From 17.72% to 50%+
2. **Directory Infrastructure** - From 32.12% to 50%+
3. **Statistics Parsing** - Already strong at 89.35%, maintain

---

## Test Execution Observations

**Test Run Summary** (from tarpaulin):
- 6 tests passed (collection_sstable_integration_test)
- 7 tests passed (test_issue_188_select_limit_query)
- 12 tests passed (spec_driven_header_parsing_tests)
- 10 tests passed (parsing_improvements_test)
- 10 tests passed (parser_factory_tests)
- 7 tests passed (reader_compression_tests)
- Total successful: ~60+ tests
- Total in binaries reporting no coverage: 0 tests

**Coverage Issues**:
- Some test binaries compile but report zero coverage (LLVM profiling issue?)
- Need to verify feature-gated tests are being compiled with `--all-features`

---

## Reproducibility

To regenerate this report:

```bash
cd /Users/patrick/local_projects/cqlite
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo tarpaulin \
  --package cqlite-core \
  --out Json \
  --output-dir /tmp \
  --timeout 300
```

Output files:
- `/tmp/tarpaulin-report.json` - Machine-readable coverage data
- `/tmp/tarpaulin-report.html` - Visual HTML report

---

## Next Steps

1. **Enable feature-gated tests** in tarpaulin runs
   ```bash
   cargo tarpaulin --all-features
   ```

2. **Focus on Critical Tier** - Get to 60%+ before moving to Important tier

3. **Implement missing tests** for block entries and reader infrastructure

4. **Track regression** - Establish baseline and monitor on each commit


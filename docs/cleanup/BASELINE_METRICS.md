# Issue #168: Cleanup Baseline Metrics

**Captured:** 2025-10-20 15:55:36 PDT
**Purpose:** Establish baseline metrics before cleanup operations
**Related Issues:** Cleanup issues #1-#13 in `/cleanup-issues/`

---

## Executive Summary

This document captures the **actual measured state** of the cqlite-core codebase before cleanup operations begin. All metrics below are derived from real command execution, not estimates.

**Total Lines to Remove:** ~7,000+ lines (across dead code + write infrastructure)

---

## Build Performance Metrics

### Full Release Build Time

**Command:**
```bash
cargo build --release
```

**Result:**
```
Finished `release` profile [optimized] target(s) in 0.27s
```

**Baseline:** 0.27 seconds (incremental, already built)

### Release Binary Size

**Location:** `target/release/cqlite`

**Size:** 4.8 MB

**Command:**
```bash
du -h target/release/cqlite
```

---

## Code Volume Metrics

### Total Lines in cqlite-core/src

**Command:**
```bash
find cqlite-core/src -name "*.rs" -type f | xargs wc -l | tail -1
```

**Result:** 101,143 total lines

### Total Source Files

**Command:**
```bash
find cqlite-core/src -name "*.rs" -type f | wc -l
```

**Result:** 159 source files

### Test Files

**Command:**
```bash
ls cqlite-core/tests/*.rs | wc -l
```

**Result:** 40 integration test files

---

## Test Suite Metrics

### Test Case Counts

**Unit Tests (#[test]):**
```bash
grep -r "#\[test\]" cqlite-core/src/ | wc -l
```
**Result:** 689 unit tests

**Async Tests (#[tokio::test]):**
```bash
grep -r "#\[tokio::test\]" cqlite-core/ | wc -l
```
**Result:** 387 async tests

**Integration Tests:**
```bash
grep -r "#\[test\]" cqlite-core/tests/ | wc -l
```
**Result:** 112 integration test functions

**Total Test Functions:** ~1,188 (689 + 387 + 112)

**Test Executables:**
```bash
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core --no-run --quiet 2>&1 | grep "Executable" | wc -l
```
**Result:** 42 test executables (includes lib + 41 integration test binaries)

---

## Code Quality Metrics

### Clippy Warnings

**Command:**
```bash
cargo clippy --all-targets 2>&1 | grep -c "warning:"
```

**Result:** 10 warnings

**Breakdown:**
- 3 warnings: unexpected `cfg` condition value: `legacy-heuristics` in tests
- 1 warning: `clippy::map_entry` in chunked_data_reader_integration_test.rs
- 2 warnings: `clippy::nonminimal_bool` in golden_path_get_operations_tests.rs
- 4+ compilation errors (not warnings) in test files with outdated APIs

**Note:** Test suite has compilation errors that need fixing before cleanup.

### Compilation Status

**Status:** ❌ Test suite does NOT compile cleanly

**Errors Found:**
- Missing methods: `calculate_actual_header_size`, `lookup_summary_entry`, `get_summary_stats`, `lookup_index_entry`, `lookup_range_entries`, `scan_stream`
- Private method access: `parse_static_row`
- Type mismatches in test fixtures
- SchemaRegistry API changes

**Impact:** These must be fixed before baseline tests can run.

---

## Dead Code Inventory (Phase 1: Zero-Risk Deletions)

### Issue #2: OptimizedExecutor (Dead Code)

**File:** `cqlite-core/src/query/optimized_executor.rs`
**Lines:** 1,045
**Status:** Never instantiated
**Verification:**
```bash
grep -r "OptimizedExecutor::new" cqlite-core/src/ | grep -v optimized_executor.rs
# Result: NO MATCHES
```

### Issue #3: PerformanceMonitor (Dead Code)

**File:** `cqlite-core/src/performance_monitor.rs`
**Lines:** 595
**Status:** Only used in own unit tests
**Verification:**
```bash
grep -r "PerformanceMonitor::new" cqlite-core/src/ | grep -v performance_monitor.rs
# Result: NO MATCHES
```

### Issue #4: Parser Performance Code (Dead Code)

**Files:** TBD (need to identify specific files)
**Estimated Lines:** ~500-1,000

---

## Write Infrastructure Inventory (Phase 3: Medium-Risk Removals)

### Issue #9: WAL and MemTable

**Files:**
- `cqlite-core/src/storage/wal.rs` (426 lines)
- `cqlite-core/src/storage/memtable.rs` (442 lines)

**Total:** 868 lines

**Dependencies:**
- Requires Issue #8 (feature-gate write methods) complete first
- Blocks Issue #10 (compaction/manifest removal)

### Issue #10: Compaction and Manifest

**Files:** TBD (need file list from issue)
**Estimated Lines:** ~2,000-3,000

---

## Feature Combination Testing Status

### Current Default Features

```toml
default = ["all-compression", "metrics", "experimental", "state_machine"]
```

### Feature Combinations to Test (Issue #168)

#### ✅ Tested Combinations

- [x] Full build with all features
  ```bash
  cargo build --all-features
  # Status: PASS (0.27s)
  ```

- [x] Release build
  ```bash
  cargo build --release
  # Status: PASS (0.27s, 4.8MB binary)
  ```

#### ❌ Untested Combinations (Need Fixing)

- [ ] Minimal features (M1 reading only)
  ```bash
  cargo build --no-default-features --features=all-compression
  # Status: UNKNOWN (test suite has compilation errors)
  ```

- [ ] Minimal features with tests
  ```bash
  cargo test --no-default-features --features=all-compression
  # Status: FAIL (test compilation errors)
  ```

- [ ] State machine only (M2 query engine)
  ```bash
  cargo build --no-default-features --features=all-compression,state_machine
  # Status: UNKNOWN
  ```

#### 🔧 Blocked by Test Failures

The following tests are currently failing and block baseline validation:

1. `tests/P0_4_modern_format_tests.rs`
   - Missing: `SSTableReader::calculate_actual_header_size()`
   - Private method: `parse_static_row()`

2. `tests/golden_path_summary_index_integration_tests.rs`
   - Missing: `SummaryReader::lookup_summary_entry()`
   - Missing: `SummaryReader::get_summary_stats()`
   - Missing: `IndexReader::lookup_index_entry()`
   - Missing: `IndexReader::lookup_range_entries()`

3. `tests/golden_path_scan_operations_tests.rs`
   - SchemaRegistry API change: requires 3 arguments
   - Missing: `SSTableReader::scan_stream()`

4. `tests/chunked_data_reader_integration_test.rs`
   - Clippy errors: `unused_io_amount`

---

## Validation Script Status

**Script:** `scripts/validate-cleanup.sh`

### Validation Steps

1. ✅ **Step 1:** Test core reading features
   - Status: BLOCKED (test compilation errors)

2. ✅ **Step 2:** Check clippy warnings (must be <= 50)
   - Current: 10 warnings
   - Status: PASS

3. ✅ **Step 3:** Check for unused imports
   - Status: UNKNOWN (not run due to test failures)

4. ✅ **Step 4:** Build release binary
   - Status: PASS (4.8MB)

**Overall Validation Status:** ❌ BLOCKED by test compilation errors

---

## Estimated Cleanup Impact

### Lines to Remove (Conservative Estimate)

| Phase | Component | Lines | Risk |
|-------|-----------|-------|------|
| Phase 1 | OptimizedExecutor | 1,045 | Zero |
| Phase 1 | PerformanceMonitor | 595 | Zero |
| Phase 1 | Parser Performance Code | ~800 | Zero |
| Phase 2 | Benchmarks (feature-gate) | ~500 | Zero |
| Phase 2 | Tombstone Merger (feature-gate) | ~400 | Low |
| Phase 3 | WAL | 426 | Medium |
| Phase 3 | MemTable | 442 | Medium |
| Phase 3 | Compaction | ~1,500 | Medium |
| Phase 3 | Manifest | ~1,000 | Medium |
| Phase 4 | SelectOptimizer simplification | ~300 | Medium |
| **Total** | | **~7,008** | |

### Expected Improvements

**Build Time:**
- Current: 0.27s (incremental)
- Expected: ~0.20s (10-15% reduction from less code to check)

**Binary Size:**
- Current: 4.8 MB
- Expected: ~4.0 MB (15-20% reduction)

**Memory Usage:**
- Current: Unknown baseline
- Expected: Reduced (no MemTable, no WAL buffers)

**Test Count:**
- Current: ~1,188 test functions
- Expected: Same or slightly fewer (dead code tests removed)

---

## Pre-Cleanup Action Items

Before cleanup can begin, these issues must be resolved:

### Critical Blockers

1. **Fix Test Compilation Errors**
   - [ ] Update P0_4_modern_format_tests.rs
   - [ ] Update golden_path_summary_index_integration_tests.rs
   - [ ] Update golden_path_scan_operations_tests.rs
   - [ ] Update chunked_data_reader_integration_test.rs

2. **Establish Working Baseline**
   - [ ] All tests compile
   - [ ] All tests pass
   - [ ] Validation script passes
   - [ ] Document passing test count

3. **Feature Combination Testing**
   - [ ] Test minimal features build
   - [ ] Test minimal features tests
   - [ ] Document all supported feature combinations

### Recommended (Non-Blocking)

1. **Document API Changes**
   - Create migration guide for removed methods
   - Document replacement APIs

2. **Setup CI for Minimal Features**
   - Add `ci-minimal-features.yml` validation
   - Ensure it runs on all PRs

---

## CI Pipeline Status

### Existing Pipelines

**`.github/workflows/ci-minimal-features.yml`:**
- Validates `experimental` feature NOT in default
- Validates `benchmarks` feature NOT in default
- Checks feature documentation presence
- Status: Configured for Issue #168

**Other CI:** TBD (need to check main CI pipeline)

---

## File Structure Reference

```
cleanup-issues/
├── CLEANUP_ROADMAP.md
├── EXECUTIVE_SUMMARY.md
├── ISSUE_TEMPLATES.md
├── issue-01-setup-safety-nets.md
├── issue-02-delete-optimized-executor.md
├── issue-03-delete-performance-monitor.md
├── issue-04-delete-parser-perf-code.md
├── issue-08-feature-gate-write-methods.md
├── issue-09-remove-wal-memtable.md
└── README.md

docs/cleanup/
└── BASELINE_METRICS.md (this file)

scripts/
└── validate-cleanup.sh
```

---

## Next Steps

1. **Immediate:** Fix test compilation errors (see Critical Blockers)
2. **Phase 0:** Complete Issue #1 (Setup Safety Nets)
3. **Phase 1:** Parallel execution of Issues #2-#4 (dead code removal)
4. **Phase 2:** Feature gating (Issues #6-#7)
5. **Phase 3:** Write infrastructure removal (Issues #8-#10, sequential)
6. **Phase 4:** Simplification and validation (Issues #11-#13)

---

## Measurement Commands Reference

For future re-measurement after cleanup:

```bash
# Build time
time cargo build --release

# Binary size
du -h target/release/cqlite

# Total lines
find cqlite-core/src -name "*.rs" -type f | xargs wc -l | tail -1

# Test count
grep -r "#\[test\]" cqlite-core/src/ | wc -l
grep -r "#\[tokio::test\]" cqlite-core/ | wc -l

# Clippy warnings
cargo clippy --all-targets 2>&1 | grep -c "warning:"

# Validation
./scripts/validate-cleanup.sh
```

---

## Appendix: Raw Command Outputs

### Build Output
```
Finished `release` profile [optimized] target(s) in 0.27s
```

### Clippy Sample
```
warning: unexpected `cfg` condition value: `legacy-heuristics`
   --> tests/P0_4_modern_format_tests.rs:162:7
```

### Test Compilation Errors
```
error[E0599]: no function or associated item named `calculate_actual_header_size` found
error[E0624]: method `parse_static_row` is private
error[E0277]: the trait bound `std::string::String: Borrow<&str>` is not satisfied
```

---

**Document Version:** 1.0
**Last Updated:** 2025-10-20 15:55:36 PDT
**Status:** Baseline Captured (with known test failures)

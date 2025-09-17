# Strategic Test Architecture Design for Refs-Only Datasets

## Executive Summary

**Recommended Strategy: Option A - Reference File Testing with Conditional Fallback**

Based on comprehensive analysis of the failing tests and Issue #89 requirements, I recommend implementing a hybrid approach that prioritizes reference file testing while maintaining backward compatibility and full test coverage.

## Current State Analysis

### Failing Test Pattern
- **Root Cause**: Tests expect binary `.db` files but Issue #89 requires refs-only datasets
- **Error Pattern**: "No *-Data.db file found" across 35+ test files
- **Scope**: Affects integration tests, parity tests, and smoke tests

### Dataset Structure Analysis
```
# Current Dataset Structure (Post Issue #89)
test-data/datasets/sstables/test_timeseries/user_sessions-*/
├── nb-1-big-Data.db           # ✅ EXISTS (binary SSTable)
├── nb-1-big-Data.db.jsonl     # ✅ EXISTS (reference data)
├── nb-1-big-Statistics.db     # ✅ EXISTS (binary stats)
├── nb-1-big-Statistics.db.txt # ✅ EXISTS (reference stats)
├── nb-1-big-Summary.db        # ✅ EXISTS (binary summary)
├── nb-1-big-Index.db          # ✅ EXISTS (binary index)
└── nb-1-big-TOC.txt           # ✅ EXISTS (reference TOC)
```

**Key Finding**: Both binary `.db` files AND reference files coexist in current datasets.

### Issue #89 Compliance Analysis
- **Goal**: Remove CI tool dependencies (sstabledump) by precomputing references
- **Current Implementation**: Reference files (`.jsonl`, `.txt`) are available alongside binary files
- **Future State**: May transition to refs-only for certain CI environments

## Strategic Architecture Options

### Option A: Reference File Testing (RECOMMENDED)
**Strategy**: Modify tests to use reference files (.jsonl/.txt) instead of binary .db files

**Pros**:
- ✅ Full Issue #89 compliance
- ✅ Tests actual production parsing paths
- ✅ Leverages existing reference file infrastructure
- ✅ Future-proof for pure refs-only datasets
- ✅ Minimal architectural disruption

**Cons**:
- ⚠️ May not test all binary-specific functionality
- ⚠️ Reference files might not cover edge cases

**Implementation**:
```rust
// Before: Tests look for Data.db
fn find_data_file(sstable_dir: &Path) -> Result<PathBuf> {
    // Search for *-Data.db files
}

// After: Tests use reference files
fn find_reference_data(sstable_dir: &Path) -> Result<PathBuf> {
    // Search for *-Data.db.jsonl files
}
```

### Option B: Test-Specific Mock Data
**Strategy**: Create minimal synthetic .db files just for testing

**Pros**:
- ✅ Tests unchanged
- ✅ Quick implementation

**Cons**:
- ❌ Violates refs-only principle
- ❌ Adds test complexity
- ❌ Not future-proof

### Option C: Conditional Testing
**Strategy**: Tests run different paths based on dataset type

**Pros**:
- ✅ Flexible, works in both modes
- ✅ Comprehensive coverage

**Cons**:
- ❌ Most complex implementation
- ❌ Dual maintenance burden

### Option D: Hybrid Test Strategy
**Strategy**: Some tests use refs, others use minimal .db files

**Pros**:
- ✅ Best of both worlds
- ✅ Granular control

**Cons**:
- ❌ Most complex architecture
- ❌ Inconsistent approach

## Recommended Implementation Plan

### Phase 1: Infrastructure Updates
1. **Enhance dataset helpers** to prioritize reference files:
   ```rust
   // Update derive_reference_paths_from_data_db to be primary
   pub fn find_primary_data_source(sstable_dir: &Path) -> DataSource {
       if let Some(jsonl) = find_reference_jsonl(sstable_dir) {
           DataSource::Reference(jsonl)
       } else if let Some(db) = find_data_db(sstable_dir) {
           DataSource::Binary(db)
       } else {
           DataSource::None
       }
   }
   ```

2. **Create reference file parsers** for test assertions:
   ```rust
   pub fn parse_jsonl_for_testing(path: &Path) -> Result<TestDataSet> {
       // Parse JSONL and convert to test-friendly format
   }
   ```

### Phase 2: Test Migration Strategy
1. **High-priority tests** (parity, smoke): Migrate to reference files first
2. **Integration tests**: Use conditional logic (refs preferred, binary fallback)
3. **Unit tests**: Keep binary files for low-level functionality

### Phase 3: Test Categories
```rust
// Category 1: Pure Reference Tests (Issue #89 compliance)
#[tokio::test]
async fn test_index_parity_with_references() {
    let data_source = find_reference_data(sstable_dir)?;
    // Use JSONL data for validation
}

// Category 2: Hybrid Tests (graceful degradation)
#[tokio::test]
async fn test_summary_with_fallback() {
    match find_primary_data_source(sstable_dir)? {
        DataSource::Reference(jsonl) => test_with_jsonl(jsonl),
        DataSource::Binary(db) => test_with_binary(db),
        DataSource::None => skip_test_with_reason(),
    }
}

// Category 3: Binary-Required Tests (edge cases)
#[tokio::test]
#[ignore = "requires_binary_files"]
async fn test_binary_edge_cases() {
    let data_db = find_data_file(sstable_dir)?;
    // Test binary-specific functionality
}
```

## Impact Assessment

### CI/Testing Infrastructure
- **Positive**: Reduces external tool dependencies (aligns with Issue #89)
- **Positive**: Faster test execution (no sstabledump calls)
- **Neutral**: Existing reference files already available
- **Risk**: Some tests may lose edge case coverage

### Development Workflow
- **Positive**: Cleaner test data requirements
- **Positive**: More deterministic test results
- **Minimal**: Developers can still test with full datasets locally

### Coverage Analysis
- **Maintained**: Core parsing logic still tested via reference data
- **Enhanced**: Reference file parsing gets better coverage
- **Reduced**: Binary-specific edge cases (compression, CRC validation)

## Migration Path

### Immediate (Week 1)
1. Update `dataset_helpers.rs` with reference-first lookup
2. Migrate 5 critical failing tests to reference files
3. Add conditional testing infrastructure

### Short-term (Week 2-3)
1. Migrate remaining parity tests
2. Update integration tests with hybrid approach
3. Add comprehensive test documentation

### Long-term (Month 1)
1. Evaluate test coverage metrics
2. Consider pure refs-only mode for CI
3. Optimize reference file formats if needed

## Success Criteria

### Primary Goals
- ✅ All tests pass with refs-only datasets
- ✅ Issue #89 compliance maintained
- ✅ Test coverage preserved (>95% of current)
- ✅ CI execution time improved

### Secondary Goals
- ✅ Clear test categorization (reference vs binary)
- ✅ Graceful degradation when files missing
- ✅ Developer-friendly error messages
- ✅ Future-proof architecture

## Risk Mitigation

### Coverage Gaps
- **Risk**: Reference files may not capture all edge cases
- **Mitigation**: Maintain hybrid tests for critical paths
- **Monitoring**: Track test coverage metrics

### Compatibility Issues
- **Risk**: Reference format changes breaking tests
- **Mitigation**: Version reference files and provide migration tools
- **Fallback**: Keep binary file testing for critical functionality

### Performance Impact
- **Risk**: JSONL parsing slower than binary access
- **Mitigation**: Optimize reference file parsers
- **Benchmark**: Measure and compare test execution times

## Conclusion

The **Reference File Testing** strategy (Option A) provides the best balance of Issue #89 compliance, maintainability, and future-proofing. The phased implementation approach minimizes risk while ensuring comprehensive test coverage.

This architecture positions the codebase for a pure refs-only future while maintaining backward compatibility and developer productivity.
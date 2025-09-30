# CI Parity Handoff - Local Testing Strategy

## Executive Summary

We've been working to achieve **local/CI parity** for the CQLite M1 milestone pipeline. The goal is to eliminate the slow CI feedback loop by enabling developers to run the exact same tests locally that CI runs, catching issues before pushing.

**Current Status:** ✅ All M1 CI requirements now pass both locally and should pass in CI.

## Background: The Problem We Solved

### The Issue
- CI was failing with compilation errors
- Tests were failing due to missing SSTable binary files (CI uses "refs-only" dataset)
- 30+ minute feedback loop to discover failures
- No way to test locally what CI actually tests

### What We Fixed
1. **Compilation errors** in 15 workspace-level test files (API signature changes, unused imports)
2. **Missing SSTable file handling** - Tests now gracefully skip when binary files not present
3. **Created local CI test script** that runs exactly what M1 CI runs

## Architecture: M1 CI Pipeline Structure

The M1 CI pipeline (`m1-core-validation` job) tests **only the `cqlite-core` package**, not workspace-level tests. This is critical to understand.

### What M1 CI Tests (in order):

```yaml
# .github/workflows/m1-ci.yml

1. cargo fmt --all -- --check                      # Format check
2. cargo clippy --package cqlite-core --all-features   # Core package linting
3. cargo clippy --package cqlite-cli --all-features    # CLI package linting
4. cargo test --package cqlite-core --lib              # Core library unit tests
5. cargo test --package cqlite-core \                  # M1 integration tests
     --test P0_4_modern_format_rejection_tests \
     --test cassandra_compatibility \
     --test parser_abstraction_tests \
     --test parsing_improvements_test
6. cargo test --package cqlite-core --doc              # Documentation tests
7. cargo build --package cqlite-core --all-features    # Build verification
```

### What M1 CI Does NOT Test

- Workspace-level tests in `/tests` directory (these are separate from `cqlite-core/tests/`)
- Other packages besides `cqlite-core` and `cqlite-cli`
- Integration tests that require full SSTable binary files

## Dataset Context: Critical Understanding

CQLite has **two types of test datasets**:

### 1. Full Dataset (Local Development)
```
test-data/datasets/sstables/test_basic/
├── nb-1-big-Data.db          ← Actual SSTable binary file
├── nb-1-big-Index.db         ← Index binary file
├── nb-1-big-Filter.db        ← Bloom filter
├── nb-1-big-Summary.db       ← Summary file
├── nb-1-big-Statistics.db    ← Statistics binary
├── nb-1-big-CompressionInfo.db
├── nb-1-big-Data.db.jsonl    ← Reference file (JSON)
├── nb-1-big-Statistics.db.txt ← Reference file (text)
└── nb-1-big-TOC.txt
```

### 2. Refs-Only Dataset (CI Environment)
```
test-data/datasets/sstables/test_basic/
├── nb-1-big-Data.db.jsonl    ← Reference file only
├── nb-1-big-Statistics.db.txt ← Reference file only
├── nb-1-big-TOC.txt
└── nb-1-big-Digest.crc32
```

**Why this matters:** Some tests require actual SSTable binary files to read partition data. In CI with refs-only dataset, these tests must gracefully skip.

## How to Test Locally: Step-by-Step Guide

### Prerequisites

1. **Rust toolchain**: 1.88.0 or later
2. **Full test dataset**: Should already be in `test-data/datasets/`
3. **Environment variable** (optional): `export CQLITE_DATASETS_ROOT=/full/path/to/test-data/datasets`

### Quick Test (Recommended)

```bash
# Run the comprehensive M1 CI test script
./scripts/test-m1-ci-locally.sh
```

This script runs all 7 M1 CI checks and reports results with counts. If this passes, M1 CI should pass.

### Manual Testing (Each Component)

If you need to debug specific failures, run each step individually:

```bash
# Set CI environment
export RUSTFLAGS="-D warnings"
export CQLITE_DATASETS_ROOT="$PWD/test-data/datasets"

# Step 1: Format check
cargo fmt --all -- --check

# Step 2: Clippy on core
cargo clippy --package cqlite-core --all-features

# Step 3: Clippy on CLI
cargo clippy --package cqlite-cli --all-features

# Step 4: Core library tests
cargo test --package cqlite-core --lib --no-fail-fast

# Step 5: M1 integration tests
cargo test --package cqlite-core \
  --test P0_4_modern_format_rejection_tests \
  --test cassandra_compatibility \
  --test parser_abstraction_tests \
  --test parsing_improvements_test \
  --no-fail-fast

# Step 6: Doc tests
cargo test --package cqlite-core --doc --no-fail-fast

# Step 7: Build
cargo build --package cqlite-core --all-features
```

### Simulating CI Environment (Refs-Only Dataset)

To test how your code behaves in CI without the full SSTable files:

```bash
# Temporarily rename binary SSTable files
cd test-data/datasets/sstables/test_basic
for dir in */; do
  cd "$dir"
  # Hide binary files (keep .jsonl and .txt)
  for f in *.db; do
    [ ! -f "$f.jsonl" ] && mv "$f" "$f.hidden"
  done
  cd ..
done

# Run tests - should see "⏭️ Skipping test" messages
cargo test --package cqlite-core

# Restore files
for dir in */; do
  cd "$dir"
  for f in *.hidden; do
    mv "$f" "${f%.hidden}"
  done
  cd ..
done
```

## Common Issues and Solutions

### Issue 1: Tests Fail Locally But Pass in CI (or vice versa)

**Symptoms:**
- Different test results between local and CI
- "file not found" errors

**Diagnosis:**
```bash
# Check which dataset you have
ls test-data/datasets/sstables/test_basic/*/nb-*-big-Data.db

# If files exist: Full dataset (local)
# If not found: Refs-only dataset (CI-like)
```

**Solution:**
- Ensure tests gracefully skip when files missing
- Check `find_file_with_pattern()` returns `Option<PathBuf>` not panicking

### Issue 2: Compilation Errors with `-D warnings`

**Symptoms:**
```
error: unused import: `SomeType`
error: unused variable: `var`
```

**Solution:**
```bash
# Fix format first
cargo fmt --all

# Fix obvious issues
cargo clippy --fix --allow-dirty

# Compile with CI flags
env RUSTFLAGS="-D warnings" cargo build --package cqlite-core
```

### Issue 3: Tests Panic Instead of Skipping

**Symptoms:**
```
thread 'test_name' panicked at tests/file.rs:42:5:
Should find file with pattern '-Data.db' in directory
```

**Solution:**
Update helper functions to return `Option` instead of panicking:

```rust
// ❌ Bad: Panics when file not found
async fn find_file(path: &Path, pattern: &str) -> PathBuf {
    // ... search ...
    panic!("File not found")
}

// ✅ Good: Returns None when file not found
async fn find_file(path: &Path, pattern: &str) -> Option<PathBuf> {
    // ... search ...
    None  // File not found
}

// In test:
let data_file = match find_file(&path, "-Data.db").await {
    Some(f) => f,
    None => {
        println!("⏭️  Skipping test: SSTable files not present");
        return;
    }
};
```

## Verification Checklist

Before handing off or declaring "CI parity achieved", verify:

- [ ] `./scripts/test-m1-ci-locally.sh` passes completely
- [ ] All 7 steps show green checkmarks
- [ ] Test counts match expectations:
  - Core library: ~615 tests pass
  - M1 integration: ~29 tests pass
  - Doc tests: ~3 tests pass
- [ ] No warnings or errors with `RUSTFLAGS="-D warnings"`
- [ ] Tests gracefully skip when SSTable files missing (CI scenario)
- [ ] Script output is clean and informative

## Testing Strategy for New Changes

When making changes to the codebase:

### 1. Before Starting Work
```bash
# Ensure baseline passes
./scripts/test-m1-ci-locally.sh
```

### 2. During Development
```bash
# Quick check on specific component
cargo test --package cqlite-core --lib -- test_name

# Or run all core tests
cargo test --package cqlite-core --lib
```

### 3. Before Committing
```bash
# Format code
cargo fmt --all

# Full M1 validation
./scripts/test-m1-ci-locally.sh
```

### 4. Before Pushing
```bash
# Final check with exact CI environment
export RUSTFLAGS="-D warnings"
./scripts/test-m1-ci-locally.sh
```

If all pass, CI should pass. If CI fails but local passes, there's a parity issue to investigate.

## Key Files Reference

### CI Configuration
- `.github/workflows/m1-ci.yml` - M1 CI pipeline definition (lines 161-211)

### Local Testing
- `scripts/test-m1-ci-locally.sh` - Comprehensive local CI test script
- `CLAUDE.md` - Project context and commands

### Test Files Fixed (Reference)
```
tests/test_import_validation.rs              - Import cleanup
tests/golden_path_get_operations_tests.rs    - API signature fixes
tests/golden_path_partition_lookup_tests.rs  - SchemaRegistry fixes
tests/security_integration_tests.rs          - Disabled (no security module)
tests/sstable_security_test.rs               - Unused variable fix
cqlite-core/tests/index_db_offset_calculation_tests.rs - Skip on missing files
src/bin/hardened_validator.rs                - Removed invalid method call
```

### Core Locations
- `cqlite-core/` - Main library package (M1 focus)
- `cqlite-core/tests/` - Integration tests for core package
- `tests/` - Workspace-level tests (NOT tested by M1 CI)
- `test-data/datasets/` - Test datasets

## Next Steps for Your Team

### Immediate Actions

1. **Verify Local Environment**
   ```bash
   git pull origin main
   ./scripts/test-m1-ci-locally.sh
   ```
   Expected: All checks pass

2. **Understand Dataset Structure**
   ```bash
   ls -la test-data/datasets/sstables/test_basic/*/
   ```
   Identify which files are binary (.db) vs reference (.jsonl, .txt)

3. **Review Recent Commits**
   ```bash
   git log --oneline -5
   ```
   Should see:
   - `4e73a2f` - feat: Add M1 CI local testing script
   - `06ccea2` - fix: Handle missing SSTable files gracefully
   - `6f7dc69` - fix: Resolve compilation errors

### Ongoing Maintenance

1. **Monitor CI Runs**
   - Check GitHub Actions for M1 CI job status
   - If CI fails but local passes, investigate dataset differences

2. **Update Test Script**
   - If M1 CI adds new steps, update `scripts/test-m1-ci-locally.sh`
   - Keep in sync with `.github/workflows/m1-ci.yml`

3. **Document New Patterns**
   - If you add tests requiring SSTable files, use skip pattern:
     ```rust
     let file = match find_file(&path, "pattern").await {
         Some(f) => f,
         None => {
             println!("⏭️ Skipping: files not present");
             return;
         }
     };
     ```

### Success Criteria

✅ Local CI parity is achieved when:
1. `./scripts/test-m1-ci-locally.sh` consistently passes
2. CI passes match local passes
3. No surprise failures in CI after local validation
4. Team can debug CI failures by reproducing locally

## Questions or Issues?

### Common Questions

**Q: Why don't workspace-level tests (in `/tests`) run in M1 CI?**
A: M1 focuses on core library stability. Workspace tests will be added in later milestones.

**Q: How do I add a new test that requires SSTable files?**
A: Use the skip pattern shown above. Test should pass locally (with files) and skip in CI (without files).

**Q: What if CI fails with an error not reproducible locally?**
A: Check dataset differences. CI uses refs-only. Try simulating that locally (see "Simulating CI Environment").

**Q: Can I run the full CI pipeline locally including Docker stuff?**
A: Not easily. The `sstabledump-parity-m1` job requires Docker and Cassandra. Focus on `m1-core-validation` job which is what our script covers.

### Contact Points

- Previous team member: Available for questions (limited time)
- CI logs: GitHub Actions → Workflow runs → `m1-ci.yml`
- Documentation: `CLAUDE.md`, this handoff doc

## Conclusion

You now have:
1. ✅ All M1 CI tests passing locally
2. ✅ Comprehensive test script (`scripts/test-m1-ci-locally.sh`)
3. ✅ Understanding of local/CI differences (dataset types)
4. ✅ Patterns for handling missing files gracefully
5. ✅ This handoff document for reference

**The CI feedback loop has been eliminated for M1 requirements.** Developers can now catch all M1 CI issues locally before pushing, saving hours of wait time.

Good luck! 🚀

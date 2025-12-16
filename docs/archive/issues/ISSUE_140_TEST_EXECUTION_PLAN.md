# Issue #140 Test Execution Plan

## Overview

This document provides the exact sequence of commands to validate the Issue #140 fix before pushing to CI.

## Executive Summary

**What was fixed**: SELECT * queries now properly populate column metadata, preventing empty JSON objects in output.

**Testing approach**: Two-tier validation strategy
1. **Quick Pre-Push Check** (~2 minutes) - Essential checks before any push
2. **Comprehensive Validation** (~5-10 minutes) - Full test suite for thorough validation

## Option 1: Quick Pre-Push Check (Recommended First Step)

**Time**: ~2 minutes
**Purpose**: Fast validation of essential functionality

```bash
cd /Users/patrick/local_projects/cqlite
./test-data/scripts/quick-pre-push-check.sh
```

**What it checks**:
- ✓ CLI builds successfully
- ✓ All 9 CI smoke tests pass
- ✓ Core unit tests pass
- ✓ No clippy warnings
- ✓ Code is properly formatted
- ✓ Quick determinism check (3 runs)

**When to use**: Run this before EVERY push to CI.

## Option 2: Comprehensive Validation (Recommended for Major Changes)

**Time**: ~5-10 minutes
**Purpose**: Exhaustive testing across all dimensions

```bash
cd /Users/patrick/local_projects/cqlite
./test-data/scripts/validate-issue-140-fix.sh
```

**What it checks**:
- ✓ Debug build tests (all categories)
- ✓ Release build tests (all categories)
- ✓ Deterministic output (5 runs each build)
- ✓ Multiple table schemas (simple, collections)
- ✓ Column projection (SELECT specific columns)
- ✓ Edge cases (empty, single row, many rows)
- ✓ All output formats (JSON, CSV, Table)
- ✓ Full unit test suite
- ✓ CI smoke test simulation
- ✓ Clippy and formatting checks

**When to use**:
- Before pushing major fixes
- After modifying query execution logic
- When you need 100% confidence
- Before creating a PR

## Manual Testing Commands

If you prefer to run individual tests:

### Test 1: Basic Smoke Tests
```bash
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas/basic-types.cql
export CQLITE_DATASET=test_basic

cargo build --package cqlite-cli --bin cqlite
./test-data/scripts/ci-one-shot-smoke.sh
```

**Expected**: `9/9 tests passed`

### Test 2: Verify Non-Empty JSON Output
```bash
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --format json
```

**Expected output should include**:
```json
[
  {
    "id": 1,
    "name": "Alice"
  },
  {
    "id": 2,
    "name": "Bob"
  },
  ...
]
```

**NOT** empty objects like `{}` or `[{}, {}, {}]`

### Test 3: Determinism Check
```bash
# Run query 5 times
for i in {1..5}; do
  cargo run --bin cqlite -- \
    --schema test-data/schemas/basic-types.cql \
    --dataset test_basic \
    --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
    --format json > /tmp/run_${i}.json
done

# Compare all runs (should show no differences)
for i in {2..5}; do
  diff /tmp/run_1.json /tmp/run_${i}.json && echo "Run $i matches run 1"
done
```

**Expected**: All diffs should be empty (no output), confirming identical results

### Test 4: Unit Tests
```bash
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core --quiet
```

**Expected**: `test result: ok`

### Test 5: Code Quality
```bash
# Clippy
cargo clippy --package cqlite-core --quiet

# Formatting
cargo fmt --check
```

**Expected**: No warnings or errors

## Understanding Test Results

### Success Indicators

**Quick Pre-Push Check**:
```
========================================
✓ ALL CHECKS PASSED
========================================

Your changes are ready to push to CI!
```

**Comprehensive Validation**:
```
========================================
✓ ALL VALIDATION TESTS PASSED ✓
========================================

The Issue #140 fix is validated and ready for CI!
```

### Failure Indicators

If any check fails, you'll see:
```
✗ <CHECK_NAME> FAILED
```

Review the error output and logs to identify the issue.

### Common Failure Modes

1. **Empty JSON objects in output**
   - Symptom: `[{}, {}, {}]` instead of actual data
   - Cause: Column metadata not populated
   - Fix: Verify lines 166-186 in select_executor.rs

2. **Non-deterministic output**
   - Symptom: Repeated runs produce different JSON
   - Cause: Column ordering not alphabetical
   - Fix: Verify `col_names.sort()` is present

3. **Test data not found**
   - Symptom: "No SSTable files found"
   - Cause: CQLITE_DATASETS_ROOT not set or incorrect
   - Fix: Verify test data at `test-data/datasets/sstables/test_basic/`

4. **Build failures**
   - Symptom: Compilation errors
   - Cause: Syntax errors or dependency issues
   - Fix: Review build output, fix compilation errors

## Test Results Location

All test artifacts are stored in:
```
test-data/scripts/issue-140-validation/
```

Review logs in this directory for detailed diagnostics.

## Recommended Testing Workflow

### Before First Push (Comprehensive)

```bash
# Step 1: Run comprehensive validation
./test-data/scripts/validate-issue-140-fix.sh

# Step 2: Review results
cat test-data/scripts/issue-140-validation/ci-smoke-tests.log

# Step 3: If all pass, commit and push
git add -u
git commit -m "fix(issue-140): Populate column metadata for SELECT * queries"
git push origin main
```

### For Subsequent Pushes (Quick Check)

```bash
# Step 1: Quick check
./test-data/scripts/quick-pre-push-check.sh

# Step 2: If pass, push
git push origin main

# Step 3: Monitor CI
# Watch GitHub Actions for final validation
```

### After Any Query Engine Changes (Comprehensive)

```bash
# Always run comprehensive validation after modifying:
# - select_executor.rs
# - query engine logic
# - result serialization
./test-data/scripts/validate-issue-140-fix.sh
```

## Time Estimates

| Task | Time | When to Use |
|------|------|-------------|
| Quick Pre-Push Check | ~2 min | Before every push |
| Comprehensive Validation | ~5-10 min | Before major PRs, after query engine changes |
| Manual Smoke Tests | ~30 sec | Quick sanity check |
| Full Unit Test Suite | ~1-2 min | After core changes |

## CI Equivalence

The comprehensive validation script (`validate-issue-140-fix.sh`) runs the EXACT same checks as CI:

| CI Check | Local Equivalent |
|----------|-----------------|
| Build (debug) | ✓ Included |
| Build (release) | ✓ Included |
| Smoke tests | ✓ Identical script |
| Unit tests | ✓ Same command |
| Clippy warnings | ✓ Same flags |
| Format check | ✓ Same command |

Running comprehensive validation locally = 100% CI confidence

## Troubleshooting

### Tests Hang or Timeout

```bash
# Add timeout to prevent hangs
timeout 300 ./test-data/scripts/validate-issue-140-fix.sh
```

### Need More Verbose Output

```bash
# Edit the script to remove --quiet flags
# Or run tests manually with --nocapture
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core -- --nocapture
```

### Test Data Issues

```bash
# Verify test data integrity
./test-data/scripts/validate-magic-numbers.sh

# Refresh test data if needed
./test-data/scripts/fetch-datasets.sh
```

## Final Pre-Push Checklist

Before pushing to CI, verify:

- [ ] Quick pre-push check passes: `./test-data/scripts/quick-pre-push-check.sh`
- [ ] OR comprehensive validation passes: `./test-data/scripts/validate-issue-140-fix.sh`
- [ ] SELECT * queries return non-empty JSON (manual verification)
- [ ] Determinism verified (3+ identical runs)
- [ ] No uncommitted changes: `git status`
- [ ] Changes are staged: `git diff --cached`

Then push:
```bash
git push origin main
```

## After Pushing

Monitor CI results:
1. Go to GitHub Actions: https://github.com/YOUR_USERNAME/cqlite/actions
2. Watch the latest workflow run
3. All checks should be ✓ green
4. If any fail, review CI logs and re-run validation locally

## Success Criteria

The fix is successful when:

1. ✓ All smoke tests pass (9/9)
2. ✓ SELECT * queries produce non-empty JSON objects with actual column data
3. ✓ Output is deterministic (alphabetically ordered columns)
4. ✓ SELECT specific columns still works
5. ✓ All output formats work (JSON, CSV, Table)
6. ✓ Unit tests pass
7. ✓ No clippy warnings
8. ✓ Code is properly formatted
9. ✓ CI pipeline completes successfully

## Additional Resources

- **Detailed validation guide**: `ISSUE_140_VALIDATION_GUIDE.md`
- **CI smoke test documentation**: `test-data/scripts/CI_SMOKE_TEST_USAGE.md`
- **Issue tracking**: GitHub Issue #140
- **Related issue**: GitHub Issue #129 (deterministic output)

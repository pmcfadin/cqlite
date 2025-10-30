# Issue #140 Quick Reference Card

## TL;DR - What to Run Before Pushing

```bash
# Quick check (2 min) - Run this before EVERY push
./test-data/scripts/quick-pre-push-check.sh

# If all pass, you're good to push
git push origin main
```

## Three Testing Scripts

### 1. Minimal Verification (30 sec)
```bash
./test-data/scripts/verify-issue-140-fix.sh
```
✓ Quick sanity check that core fix works

### 2. Quick Pre-Push Check (2 min) ⭐ RECOMMENDED
```bash
./test-data/scripts/quick-pre-push-check.sh
```
✓ All essential checks before push
✓ Same as CI tests

### 3. Comprehensive Validation (5-10 min)
```bash
./test-data/scripts/validate-issue-140-fix.sh
```
✓ Exhaustive testing across all dimensions
✓ Run before major PRs

## What Was Fixed

**Before**: SELECT * queries produced empty JSON: `[{}, {}, {}]`

**After**: SELECT * queries produce proper JSON with columns:
```json
[
  {"id": 1, "name": "Alice", ...},
  {"id": 2, "name": "Bob", ...}
]
```

## Testing Workflow

```bash
# 1. Verify fix works
./test-data/scripts/verify-issue-140-fix.sh

# 2. Run pre-push check
./test-data/scripts/quick-pre-push-check.sh

# 3. If pass, push
git push origin main
```

## Manual Commands

### Run Smoke Tests
```bash
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas/basic-types.cql
export CQLITE_DATASET=test_basic

./test-data/scripts/ci-one-shot-smoke.sh
```

### Run Unit Tests
```bash
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core --quiet
```

### Test a Query
```bash
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --format json
```

## Success Indicators

### ✓ Working Fix
```json
[
  {
    "id": 1,
    "name": "Alice",
    ...
  }
]
```

### ✗ Broken Fix
```json
[
  {},
  {},
  {}
]
```

## Quick Checklist

Before pushing, verify:
- [ ] Quick pre-push check passes
- [ ] No empty JSON objects in output
- [ ] All smoke tests pass (9/9)
- [ ] Unit tests pass
- [ ] No clippy warnings
- [ ] Code formatted

## Time Budget

| Script | Time |
|--------|------|
| verify-issue-140-fix.sh | 30 sec |
| quick-pre-push-check.sh | 2 min |
| validate-issue-140-fix.sh | 5-10 min |

## Full Documentation

- **Detailed Guide**: `ISSUE_140_VALIDATION_GUIDE.md`
- **Execution Plan**: `ISSUE_140_TEST_EXECUTION_PLAN.md`
- **Summary**: `TESTING_SUMMARY_ISSUE_140.md`

## One-Liner for Push

```bash
./test-data/scripts/quick-pre-push-check.sh && git push origin main
```

## Troubleshooting

**Empty objects still appear?**
```bash
# Rebuild CLI
cargo build --package cqlite-cli
```

**Tests fail?**
```bash
# Run comprehensive validation for diagnostics
./test-data/scripts/validate-issue-140-fix.sh
```

**Need verbose output?**
```bash
cargo test --package cqlite-core -- --nocapture
```

## Files Created

**Scripts** (in `test-data/scripts/`):
- ✓ `verify-issue-140-fix.sh` - Minimal check
- ✓ `quick-pre-push-check.sh` - Quick validation
- ✓ `validate-issue-140-fix.sh` - Comprehensive testing

**Docs** (in workspace root):
- ✓ `ISSUE_140_VALIDATION_GUIDE.md` - Full guide
- ✓ `ISSUE_140_TEST_EXECUTION_PLAN.md` - Execution plan
- ✓ `TESTING_SUMMARY_ISSUE_140.md` - Summary
- ✓ `ISSUE_140_QUICK_REFERENCE.md` - This card

## Bottom Line

**Run before EVERY push:**
```bash
./test-data/scripts/quick-pre-push-check.sh
```

**If it passes, you're safe to push to CI. Period.**

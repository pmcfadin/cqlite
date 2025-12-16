# Local Testing Guideline for CI Parity

## Quick Pre-Push Check
```bash
# Run the comprehensive local CI validation script
./scripts/test-all-ci-locally.sh
```

This script validates all 3 active workflows:
1. **M1 Minimal CI Pipeline** (clippy + formatting)
2. **Main CI** (full `cqlite-core` tests)
3. **SSTableDump Parity Gate** (build validation + parity tests)

---

## ⚠️ Known CI Parity Issues

### SSTableDump Parity Tests May Fail in CI
The `test_statistics_parity_validator_with_deterministic_tables` test may pass locally but fail in CI due to **stale dataset directories with old UUIDs**.

**Why this happens:**
- CI has both old UUID directories (e.g., `simple_table-5428f520902711f0a7e2a33d4c609114`)
- And new UUID directories (e.g., `simple_table-6de93b70934a11f08d448925b7a9e804`)
- The test uses `resolve_table_to_sstable_path()` which may pick old directories in CI
- Local environment only has new UUID directories

**Detection:**
```bash
# Check if you have duplicate table directories locally
find test-data/datasets/sstables/test_basic -type d -name "simple_table-*"
```

If you see multiple directories for the same table, you have the same problem CI has.

**Workaround:**
```bash
# Test with specific UUID paths that match CI
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core --test sstabledump_parity_statistics \
  -- --nocapture
```

---

## Individual Workflow Commands

### M1 Minimal CI (fastest, catches most issues)
```bash
# Clippy with warnings as errors
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features

# Format check
cargo fmt --check
```

### Main CI (comprehensive test suite)
```bash
# Run all cqlite-core tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core --all-features \
  -- --skip test_legacy_format_allows_blob_fallback_with_feature
```

### SSTableDump Parity (build validation + parity tests)
```bash
# Build specific tests
env RUSTFLAGS="-D warnings" cargo clippy --package cqlite-core \
  --test sstabledump_parity_statistics \
  --test sstabledump_parity_index \
  --test sstabledump_parity_summary

# Run parity tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --release --package cqlite-core \
  --test sstabledump_parity_statistics \
  --test sstabledump_parity_index \
  --test sstabledump_parity_summary \
  -- --nocapture
```

---

## Key Environment Variables
- `CQLITE_DATASETS_ROOT=$PWD/test-data/datasets` - Required for tests
- `RUSTFLAGS="-D warnings"` - Convert warnings to errors (CI mode)

---

## Before Every Push
```bash
./scripts/test-all-ci-locally.sh && git push
```

This ensures CI will pass! 🎯

---

## Debugging CI Failures

### Step 1: Check which workflow failed
```bash
gh run list --limit 3
```

### Step 2: View failure logs
```bash
# Get run ID from step 1
gh run view <RUN_ID> --log-failed
```

### Step 3: Reproduce locally
```bash
# Run the specific failing test
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core --test <TEST_FILE_NAME> -- --nocapture
```

### Step 4: Check for dataset differences
```bash
# Compare local vs CI dataset structure
find test-data/datasets/sstables -type d -name "*-*" | sort
```

---

## Common Issues

### Issue: Test passes locally but fails in CI
**Cause**: Dataset UUID mismatch or stale directories in CI
**Fix**: Clean up old UUID directories or update test to handle multiple UUIDs

### Issue: Clippy passes locally but fails in CI
**Cause**: Missing `RUSTFLAGS="-D warnings"` locally
**Fix**: Always run clippy with `env RUSTFLAGS="-D warnings"`

### Issue: Format check fails in CI
**Cause**: Not running `cargo fmt` before commit
**Fix**: Run `cargo fmt` then `cargo fmt --check` before pushing

---

## Quick Reference

| Task | Command |
|------|---------|
| **Full CI validation** | `./scripts/test-all-ci-locally.sh` |
| **Clippy (strict)** | `env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features` |
| **Format** | `cargo fmt` |
| **Format check** | `cargo fmt --check` |
| **All tests** | `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core --all-features` |
| **Parity tests** | `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core --test sstabledump_parity_statistics -- --nocapture` |

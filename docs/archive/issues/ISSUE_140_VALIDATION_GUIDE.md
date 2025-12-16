# Issue #140 Fix Validation Guide

This guide provides comprehensive testing instructions for validating the Issue #140 fix (dynamic column metadata population for SELECT * queries) before pushing to CI.

## Quick Start

### Run Full Validation (Recommended)
```bash
cd /Users/patrick/local_projects/cqlite
./test-data/scripts/validate-issue-140-fix.sh
```

This comprehensive script runs ALL validation tests (~5-10 minutes):
- ✓ Debug and release builds
- ✓ Deterministic output verification (5 runs each)
- ✓ Multiple table schemas (simple, collections)
- ✓ Column projection tests
- ✓ Edge cases (empty, single row, many rows)
- ✓ All output formats (JSON, CSV, Table)
- ✓ Unit tests
- ✓ CI smoke tests
- ✓ Clippy and formatting checks

### Quick Pre-Push Check (~2 minutes)
```bash
cd /Users/patrick/local_projects/cqlite

# 1. Build and run smoke tests
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas/basic-types.cql
export CQLITE_DATASET=test_basic

cargo build --package cqlite-cli --bin cqlite --quiet
./test-data/scripts/ci-one-shot-smoke.sh

# 2. Run core unit tests
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core --quiet

# 3. Check code quality
cargo clippy --package cqlite-core --quiet
cargo fmt --check
```

## Individual Test Categories

### 1. Test Deterministic Output (Issue #129)

Run the same query multiple times and verify identical output:

```bash
# Debug build
for i in {1..5}; do
  cargo run --bin cqlite -- \
    --schema test-data/schemas/basic-types.cql \
    --dataset test_basic \
    --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
    --format json > /tmp/run_${i}.json
done

# Compare all runs
diff /tmp/run_1.json /tmp/run_2.json
diff /tmp/run_1.json /tmp/run_3.json
diff /tmp/run_1.json /tmp/run_4.json
diff /tmp/run_1.json /tmp/run_5.json
```

### 2. Test Different Table Schemas

```bash
# Simple table
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --format json

# Collections table
cargo run --bin cqlite -- \
  --schema test-data/schemas/collections.cql \
  --dataset test_collections \
  --execute "SELECT * FROM test_collections.collection_table LIMIT 2" \
  --format json
```

### 3. Test Column Projection

```bash
# SELECT specific columns (should still work)
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT id, name FROM test_basic.simple_table LIMIT 3" \
  --format json
```

### 4. Test Edge Cases

```bash
# Empty or minimal results
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table WHERE id = 99999" \
  --format json

# Single row
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 1" \
  --format json

# Many rows
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 100" \
  --format json
```

### 5. Test All Output Formats

```bash
# JSON format
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --format json

# CSV format
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --format csv

# Table format
cargo run --bin cqlite -- \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --format table
```

### 6. Test Debug vs Release Builds

```bash
# Debug build
cargo build --package cqlite-cli --bin cqlite
time ./target/debug/cqlite \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 100" \
  --format json > /tmp/debug_output.json

# Release build
cargo build --package cqlite-cli --bin cqlite --release
time ./target/release/cqlite \
  --schema test-data/schemas/basic-types.cql \
  --dataset test_basic \
  --execute "SELECT * FROM test_basic.simple_table LIMIT 100" \
  --format json > /tmp/release_output.json

# Compare outputs (should be identical)
diff /tmp/debug_output.json /tmp/release_output.json
```

### 7. Run Unit Tests

```bash
# All core tests
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core

# With verbose output
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core -- --nocapture

# Specific test
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core test_name_here
```

### 8. Run CI Smoke Tests

```bash
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas/basic-types.cql
export CQLITE_DATASET=test_basic

./test-data/scripts/ci-one-shot-smoke.sh
```

### 9. Code Quality Checks

```bash
# Clippy (as CI does it)
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features

# Format check
cargo fmt --check

# If formatting is wrong, fix it
cargo fmt
```

## Validation Checklist

Before pushing to CI, ensure:

- [ ] Comprehensive validation script passes: `./test-data/scripts/validate-issue-140-fix.sh`
- [ ] Debug build smoke tests pass (9/9)
- [ ] Release build smoke tests pass (9/9)
- [ ] Deterministic output verified (5+ identical runs)
- [ ] All unit tests pass
- [ ] Clippy shows no warnings
- [ ] Code is properly formatted
- [ ] SELECT * queries return non-empty JSON objects
- [ ] SELECT specific columns still works
- [ ] All output formats work (JSON, CSV, Table)

## Understanding the Fix

The fix addresses Issue #140 where SELECT * queries produced empty JSON objects (`{}`) in golden snapshots.

**Root Cause**: When `SELECT *` is used, the query parser doesn't populate `context.columns` (it's intentionally empty to signal "all columns"). However, the `QueryResult` metadata needs column information for proper JSON serialization.

**Solution** (in `cqlite-core/src/query/select_executor.rs`, lines 166-186):
1. After collecting query results, check if `context.columns` is empty
2. If empty AND results exist, infer columns from the first row's HashMap keys
3. Sort column names alphabetically for deterministic output (Issue #129)
4. Populate `QueryMetadata.columns` with inferred column information

**Key Code**:
```rust
// CRITICAL FIX (Issue #129/#140): Populate metadata.columns for SELECT *
let mut columns = context.columns;
if columns.is_empty() && !intermediate_results.is_empty() {
    // Infer columns from first row
    let first_row = &intermediate_results[0];
    let mut col_names: Vec<_> = first_row.values.keys().collect();
    col_names.sort(); // Sort alphabetically for deterministic ordering

    for (idx, col_name) in col_names.iter().enumerate() {
        columns.push(ColumnInfo {
            name: (*col_name).clone(),
            data_type: crate::types::DataType::Text,
            nullable: true,
            position: idx,
            table_name: None,
        });
    }
}
```

## Troubleshooting

### Test Failures

**"No SSTable files found"**:
```bash
# Verify test data exists
ls -la test-data/datasets/sstables/test_basic/
# Should see files like: na-1-big-Data.db, na-1-big-Index.db, etc.
```

**"Unit tests failed"**:
```bash
# Run with verbose output to see which test failed
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core -- --nocapture
```

**"Outputs are not deterministic"**:
- This indicates Issue #129 regression (column ordering not alphabetical)
- Check that `col_names.sort()` is present in select_executor.rs
- Verify no other code paths add columns in non-deterministic order

### Performance Issues

If tests are slow:
```bash
# Use release build for faster execution
cargo build --release --package cqlite-cli

# Use --quiet flag to reduce output
cargo test --package cqlite-core --quiet
```

## CI Environment Simulation

The validation script (`validate-issue-140-fix.sh`) simulates the exact CI environment:

1. Clean build from scratch
2. Both debug and release builds
3. All smoke tests with golden snapshot comparison
4. Full unit test suite
5. Clippy with `-D warnings` (warnings as errors)
6. Format checking

Running this script locally gives you 100% confidence that CI will pass.

## Results Location

All test results are stored in:
```
test-data/scripts/issue-140-validation/
├── build-debug.log          # Debug build output
├── build-release.log         # Release build output
├── determinism/              # Deterministic output test results
│   ├── debug_run_1.json
│   ├── debug_run_2.json
│   ├── ...
│   ├── release_run_1.json
│   └── ...
├── schema-tests/             # Different schema test results
│   ├── debug_simple_table.json
│   ├── debug_collections.json
│   └── ...
├── edge-cases/               # Edge case test results
│   ├── debug_empty.json
│   ├── debug_single_row.json
│   └── ...
├── unit-tests.log            # Unit test output
├── ci-smoke-tests.log        # CI smoke test output
├── clippy.log                # Clippy output
└── fmt.log                   # Format check output
```

## Next Steps After Validation

Once all validation passes:

```bash
# 1. Stage your changes
git add -u

# 2. Commit with descriptive message
git commit -m "fix(issue-140): Populate column metadata for SELECT * queries

Resolves empty JSON objects in golden snapshots by inferring columns
from first result row when SELECT * is used. Maintains alphabetical
column ordering for deterministic output (issue #129).

All 9 smoke tests pass. Validated with comprehensive test suite."

# 3. Push to CI
git push origin main
```

CI should now pass with:
- ✓ Build successful
- ✓ All tests pass
- ✓ No clippy warnings
- ✓ Proper formatting
- ✓ Golden snapshots match

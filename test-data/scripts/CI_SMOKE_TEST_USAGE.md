# CI Smoke Test Usage Guide

## Overview

The `ci-one-shot-smoke.sh` script provides automated smoke testing for CQLite's one-shot query execution mode. It validates exit codes, compares outputs against golden snapshots, and supports multiple output formats (JSON, CSV, table).

**Related Issue**: #140

## Quick Start

```bash
# Set required environment variables
export CQLITE_DATA_DIR=/path/to/test-data/datasets/sstables
export CQLITE_SCHEMA=/path/to/test-data/schemas/basic-types.cql

# Run the smoke test
./test-data/scripts/ci-one-shot-smoke.sh
```

## Environment Variables

### Required

- `CQLITE_DATA_DIR` - Path to SSTable data directory (e.g., `/path/to/test-data/datasets/sstables`)
- `CQLITE_SCHEMA` - Path to schema file (e.g., `/path/to/test-data/schemas/basic-types.cql`)

### Optional

- `CQLITE_CLI` - Path to cqlite binary (if not set, script will build or find existing binary)
- `OUTPUT_DIR` - Directory for test results (default: `./smoke-test-results`)

## Test Coverage

The smoke test suite includes:

1. **Basic SELECT with JSON output** - Tests JSON formatting with `simple_table`
2. **Basic SELECT with CSV output** - Tests CSV formatting with `simple_table`
3. **Basic SELECT with table output** - Tests ASCII table formatting
4. **Column projection** - Tests `SELECT id, name FROM ...`
5. **Collections query** - Tests collections table with JSON output
6. **Invalid query syntax** - Validates error handling for malformed queries
7. **Missing schema file** - Validates error handling when schema doesn't exist
8. **Missing data directory** - Validates error handling when data directory doesn't exist
9. **Non-existent table query** - Validates graceful handling (currently returns exit 0)

## Exit Codes

The script returns:
- `0` - All tests passed
- `1` - One or more tests failed

Individual test exit code expectations:
- **Success tests**: Exit code `0`
- **Error tests**: Non-zero exit code (typically `3-5` depending on error type)

## Output Structure

After running, the script generates:

```
smoke-test-results/
├── test_select_json_simple.actual
├── test_select_csv_simple.actual
├── test_select_table_simple.actual
├── test_select_columns.actual
├── test_select_collections.actual
├── test_error_invalid_query.actual
├── test_error_missing_schema.actual
├── test_error_missing_data_dir.actual
└── test_query_nonexistent_table.actual
```

## Snapshot Comparison

If golden snapshot files exist in `smoke-test-snapshots/`, the script will:
1. Compare actual output against the golden file using `diff -u`
2. Report any differences
3. Fail the test if output doesn't match

**Note**: Snapshots are optional. Tests will pass based on exit codes alone if no snapshots exist.

## CI Integration

### GitHub Actions Example

```yaml
- name: Run one-shot smoke tests
  env:
    CQLITE_DATA_DIR: ${{ github.workspace }}/test-data/datasets/sstables
    CQLITE_SCHEMA: ${{ github.workspace }}/test-data/schemas/basic-types.cql
  run: ./test-data/scripts/ci-one-shot-smoke.sh
```

### Local Development

```bash
# From workspace root
export CQLITE_DATA_DIR=$(pwd)/test-data/datasets/sstables
export CQLITE_SCHEMA=$(pwd)/test-data/schemas/basic-types.cql

./test-data/scripts/ci-one-shot-smoke.sh
```

## Interpreting Results

### Success

```
=========================================
         SMOKE TEST SUMMARY
=========================================

  Tests Run:    9
  Tests Passed: 9
  Tests Failed: 0

  Output Directory: ./smoke-test-results

=========================================
        ALL TESTS PASSED ✓
=========================================
```

### Failure

```
[FAIL] test_select_json_simple: Output does not match snapshot
--- smoke-test-snapshots/select_simple_json.golden
+++ smoke-test-results/test_select_json_simple.actual
@@ -1,5 +1,5 @@
-{"rows": [...]}
+{"rows": [changed data]}

=========================================
         SMOKE TEST SUMMARY
=========================================

  Tests Run:    9
  Tests Passed: 8
  Tests Failed: 1
```

## Troubleshooting

### "CQLITE_DATA_DIR environment variable not set"

Set the required environment variable:
```bash
export CQLITE_DATA_DIR=/Users/patrick/local_projects/cqlite/test-data/datasets/sstables
```

### "CLI binary not found"

The script will automatically build the binary. If this fails:
```bash
cargo build --package cqlite-cli --bin cqlite
```

### Test fails with snapshot mismatch

1. Check if the output change is intentional
2. If intentional, update the golden snapshot:
   ```bash
   cp smoke-test-results/test_name.actual smoke-test-snapshots/snapshot_name.golden
   ```
3. If unintentional, investigate the regression

### Warnings in output

Warnings (e.g., "Could not load SSTable file") are normal and expected. The CLI loads all tables in the data directory, and some may be incompatible or corrupt. As long as the target tables load successfully, tests will pass.

## Generating Golden Snapshots

See `smoke-test-snapshots/README.md` for detailed instructions on generating and updating golden snapshot files.

## Extending the Test Suite

To add a new test case:

1. **Add a success test**:
   ```bash
   run_test \
       "test_name" \
       "SELECT query here" \
       "json" \
       0 \
       "${SNAPSHOTS_DIR}/snapshot_name.golden"
   ```

2. **Add an error test**:
   ```bash
   run_error_test \
       "test_error_name" \
       "expected_pattern" \
       --schema "${CQLITE_SCHEMA}" \
       --data-dir "${CQLITE_DATA_DIR}" \
       --execute "invalid query" \
       --format "json"
   ```

3. **Generate snapshot** (if using snapshot comparison):
   ```bash
   # Run test to generate .actual file
   ./ci-one-shot-smoke.sh

   # Inspect output
   cat smoke-test-results/test_name.actual

   # Copy to snapshots if correct
   cp smoke-test-results/test_name.actual smoke-test-snapshots/snapshot_name.golden
   ```

## Performance

Typical run time: **15-30 seconds** (includes CLI execution for 9 test cases)

Factors affecting performance:
- Number of SSTables in data directory
- CLI binary optimization level (debug vs release)
- System I/O performance

## Related Files

- `ci-one-shot-smoke.sh` - Main smoke test script
- `smoke-test-snapshots/` - Golden snapshot files
- `smoke-test-snapshots/README.md` - Snapshot generation guide
- `../schemas/basic-types.cql` - Test schema for basic types
- `../schemas/collections.cql` - Test schema for collections
- `../datasets/sstables/` - Test SSTable data

## Known Issues

- **Test 9** (non-existent table query) currently expects exit code `0`, but this may change when error handling improves
- Some test data warnings are expected due to incompatible SSTables in the data directory
- Snapshot comparison requires stable CLI output format (not suitable during rapid development)

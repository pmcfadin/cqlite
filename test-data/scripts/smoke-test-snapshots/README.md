# Smoke Test Golden Snapshots

This directory contains golden snapshot files for the CI smoke test suite.

## Snapshot Files

These files represent the expected output for various one-shot query tests:

- `select_simple_json.golden` - JSON output for basic SELECT query on simple_table
- `select_simple_csv.golden` - CSV output for basic SELECT query on simple_table
- `select_simple_table.golden` - Table output for basic SELECT query on simple_table
- `select_columns_json.golden` - JSON output for column projection query
- `select_collections_json.golden` - JSON output for collections query

## Generating Snapshots

**IMPORTANT**: Snapshots should only be generated when the CLI output format is stable and produces correct results.

To generate snapshots:

1. Ensure the CLI is built and working correctly
2. Set environment variables:
   ```bash
   export CQLITE_DATA_DIR=/path/to/test-data/datasets/sstables
   export CQLITE_SCHEMA=/path/to/test-data/schemas/basic-types.cql
   ```

3. Run the smoke test to generate `.actual` files:
   ```bash
   ./ci-one-shot-smoke.sh
   ```

4. Manually inspect the `.actual` files in `smoke-test-results/`

5. If the output is correct, copy them to this directory as `.golden` files:
   ```bash
   cp smoke-test-results/test_select_json_simple.actual smoke-test-snapshots/select_simple_json.golden
   cp smoke-test-results/test_select_csv_simple.actual smoke-test-snapshots/select_simple_csv.golden
   cp smoke-test-results/test_select_table_simple.actual smoke-test-snapshots/select_simple_table.golden
   cp smoke-test-results/test_select_columns.actual smoke-test-snapshots/select_columns_json.golden
   cp smoke-test-results/test_select_collections.actual smoke-test-snapshots/select_collections_json.golden
   ```

## Updating Snapshots

When the CLI output format changes intentionally (e.g., improved formatting, bug fixes):

1. Review the changes carefully
2. Run the smoke test and verify the new output is correct
3. Update the corresponding `.golden` files
4. Commit the updated snapshots with a clear explanation in the commit message

## Snapshot Format Notes

- **JSON snapshots**: Should contain valid JSON (array or object)
- **CSV snapshots**: Should contain comma-separated values with headers
- **Table snapshots**: Should contain ASCII table formatting with borders

## CI Integration

The smoke test script:
- Compares actual output against these golden files using `diff -u`
- Fails if any test output doesn't match
- Allows tests to run without snapshots (only validates exit codes)
- Reports detailed diff output on mismatch

## First Run Without Snapshots

If this is the first run and no `.golden` files exist, the script will:
- Still run all tests
- Validate exit codes
- Generate `.actual` files for manual inspection
- Warn that snapshots are missing
- Pass tests that have correct exit codes (even without snapshots)

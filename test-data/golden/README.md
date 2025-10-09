# Golden Snapshot Files

This directory contains golden snapshot files for CLI integration tests (Issue #140).

## Purpose

Golden snapshots serve as reference outputs for regression testing. They capture the expected output of CLI commands against known test data. Tests compare actual CLI output against these golden snapshots to detect unintended changes in behavior or output formatting.

## Current State

The golden snapshots in this directory reflect the current CLI output format, including:
- Debug messages (e.g., `DEBUG: Database::execute(...)`)
- Structured JSON/CSV output with metadata
- Empty result sets (if test data doesn't contain actual rows yet)

This is expected behavior and serves as a baseline for regression testing. As the CLI implementation evolves and test data is populated, these snapshots should be regenerated to reflect the new expected behavior.

## Files

The following golden snapshot files are generated:

### Basic Types (test_basic.simple_table)

| File | Format | Query | Description |
|------|--------|-------|-------------|
| `basic_select_json.json` | JSON | `SELECT * FROM test_basic.simple_table LIMIT 5` | All columns, JSON output |
| `basic_select_csv.csv` | CSV | `SELECT * FROM test_basic.simple_table LIMIT 5` | All columns, CSV output |
| `basic_select_table.table` | Table | `SELECT * FROM test_basic.simple_table LIMIT 5` | All columns, table output (reference) |
| `basic_select_columns_json.json` | JSON | `SELECT id, name, age FROM test_basic.simple_table LIMIT 3` | Subset of columns, JSON output |
| `basic_select_columns_csv.csv` | CSV | `SELECT id, name, age FROM test_basic.simple_table LIMIT 3` | Subset of columns, CSV output |

### Collections (test_collections.collection_table)

| File | Format | Query | Description |
|------|--------|-------|-------------|
| `collections_select.json` | JSON | `SELECT * FROM test_collections.collection_table LIMIT 3` | Collections, JSON output |
| `collections_select_csv.csv` | CSV | `SELECT * FROM test_collections.collection_table LIMIT 3` | Collections, CSV output |
| `collections_select_table.table` | Table | `SELECT * FROM test_collections.collection_table LIMIT 3` | Collections, table output (reference) |

## Generating Golden Snapshots

To regenerate all golden snapshot files:

```bash
# From test-data/scripts directory
./generate-golden-snapshots.sh

# Or use release build for better performance
./generate-golden-snapshots.sh --release
```

The script will:
1. Build the CLI (debug or release mode)
2. Create/verify the golden directory
3. Verify required test data exists
4. Run queries and save outputs
5. Report summary of generated files

## Using Golden Snapshots in Tests

To use golden snapshots in integration tests:

```rust
use std::path::PathBuf;

fn get_golden_path(filename: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../test-data/golden")
        .join(filename)
}

#[test]
fn test_basic_select_json() {
    let output = run_cli_command(&[
        "--schema", "test-data/schemas/basic-types.json",
        "--data-dir", "test-data/datasets",
        "-e", "SELECT * FROM test_basic.simple_table LIMIT 5",
        "--format", "json",
    ]);

    let expected = std::fs::read_to_string(get_golden_path("basic_select_json.json"))
        .expect("Failed to read golden snapshot");

    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
}
```

## When to Regenerate

Regenerate golden snapshots when:

1. **Intentional output format changes**: When you deliberately change JSON/CSV/table formatting
2. **Query behavior changes**: When you modify query execution that affects output
3. **Test data changes**: When underlying test datasets are updated
4. **New features**: When adding new output formats or query capabilities

## Verification Checklist

Before committing regenerated golden snapshots:

- [ ] Review diff to understand what changed
- [ ] Verify changes are intentional and expected
- [ ] Ensure all queries still execute successfully
- [ ] Run integration tests to confirm they pass
- [ ] Check that output formats are valid (JSON parses, CSV is well-formed, etc.)
- [ ] Update this README if adding new snapshot files

## Test Data Dependencies

Golden snapshots depend on:

- **Schemas**: `test-data/schemas/basic-types.json`, `collections.json`
- **Datasets**: `test-data/datasets/sstables/test_basic/`, `test_collections/`

Ensure these are present and up-to-date before regenerating snapshots.

## Troubleshooting

### Script fails with "test data not found"

Ensure test datasets are present:
```bash
ls -la test-data/datasets/sstables/test_basic/
ls -la test-data/datasets/sstables/test_collections/
```

If missing, regenerate test data (see `test-data/scripts/` for data generation scripts).

### CLI binary not found

Build the CLI first:
```bash
cargo build --package cqlite-cli --bin cqlite
```

### Output differs from golden snapshot

This could indicate:
1. A regression (unintended behavior change)
2. Intentional change requiring snapshot update
3. Non-deterministic output (e.g., timestamps, UUIDs)

For non-deterministic data, consider:
- Using fixed test data with known values
- Normalizing output before comparison
- Testing specific fields rather than full output

## Related Issues

- Issue #140: Golden snapshots for CLI integration tests

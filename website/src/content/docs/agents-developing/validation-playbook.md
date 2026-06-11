---
title: sstabledump Validation Playbook
description: JSONL golden files, parity tests, and smoke-test-all-tables — how CQLite validates parsing correctness against Cassandra's reference output.
sidebar:
  label: Validation playbook
  order: 5
---

CQLite validates parsing correctness by comparing output against `sstabledump` — the
Cassandra tool that produces authoritative JSON from SSTable files. Golden JSONL files
are committed alongside the binary SSTables so CI can run parity checks without a live
Cassandra cluster.

## Golden JSONL files

Every Data.db in the dataset has a companion `.jsonl` file containing
`sstabledump -l` output (one JSON object per line, one line per row):

```
test-data/datasets/sstables/
└── test_basic/
    └── simple_table-<hash>/
        ├── nb-1-big-Data.db         ← binary (not in git; fetch separately)
        └── nb-1-big-Data.db.jsonl   ← sstabledump golden (committed to git)
```

The JSONL files ARE in git. You can run parity tests without fetching binary SSTables,
but you need the binaries to run the parser itself.

## Smoke test: all tables

```bash
# Runs CLI against every table, checks exit codes and row counts
bash test-data/scripts/smoke-test-all-tables.sh
```

The gate runs this against a freshly built debug binary:

```bash
cargo build --package cqlite-cli --bin cqlite
CQLITE_CLI="$PWD/target/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh
```

Using a freshly built binary prevents the failure mode where a stale release binary
(from a previous run) passes smoke while current code is broken. This was an actual
failure caught in the first full gate run.

**Expected output**: 33/33 tables PASS; the 3 da/BTI tables are SKIP-PENDING.

## Integration parity tests (Rust)

The seven CI-enforced integration tests compare parsed rows against the JSONL goldens:

```bash
cargo test --package cqlite-integration-tests \
  --test golden_path_scan_operations_tests \
  --test golden_path_get_operations_tests \
  --test golden_path_partition_lookup_tests \
  --test golden_path_summary_index_integration_tests \
  --test chunked_data_reader_direct_test \
  --test comprehensive_component_integration_tests \
  --test fixture_specific_integration_tests
```

These are the same seven targets the gate's `integration-tests` component runs. Run
them individually when a specific area fails.

## Python parity tests

```bash
# All 33 tables: row count + value-level parity
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  pytest bindings/python/tests/test_parity.py -v

# Python vs CLI output equivalence
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  pytest bindings/python/tests/test_cli_parity.py -v
```

`test_parity.py` has three test classes:
- `TestRowCountParity` — row count per keyspace (33/33 must pass)
- `TestValueParity` — cell-level comparison for representative tables
- `TestE2ESummary` — asserts all 33 tables pass (explicit failure if count drops)

**Known xfail**: none as of Dec 2025. Prior xfails (`static_columns_table` #480,
`typed_collections_table` #481) are resolved. Issue #493 (set element tombstones)
is tracked as out-of-scope for v0.9.1.

## Node.js parity tests

```bash
# Requires CQLITE_DATASETS_ROOT
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets npm run test:parity --prefix bindings/node
```

39 parity tests in `bindings/node/__test__/parity.test.js`. Uses JSONL utilities in
`parity-utils.js` for parsing and type normalization.

## Manual parity check workflow

When investigating a single table:

```bash
# 1. Parse with cqlite CLI
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables/test_basic/simple_table-<hash> \
  --query "SELECT * FROM test_basic.simple_table" \
  --out json > /tmp/cqlite.json

# 2. Reference is the JSONL golden
cat test-data/datasets/sstables/test_basic/simple_table-<hash>/nb-1-big-Data.db.jsonl \
  | jq -s '.' > /tmp/reference.json

# 3. Normalize and diff
jq -S '.' /tmp/cqlite.json > /tmp/cqlite-sorted.json
jq -S '.' /tmp/reference.json > /tmp/ref-sorted.json
diff /tmp/ref-sorted.json /tmp/cqlite-sorted.json
```

Type differences between sstabledump JSON and CQLite JSON are expected and documented:
Python uses native types (datetime, UUID, bytes); CLI uses JSON strings. Normalization
is needed for comparison — see `bindings/python/tests/test_cli_parity.py` for the
normalization logic.

## Adding a new table to parity coverage

1. Add schema to `test-data/schemas/` and `schemas/core.list`
2. Regenerate: `bash test-data/scripts/regenerate-datasets.sh`
3. JSONL golden is generated automatically during regeneration
4. Add table to the relevant parity test file
5. Run gate: `scripts/agent-gate.sh`

## Row count 0 — silent-pass trap

If parity tests pass but show 0 rows, `CQLITE_DATASETS_ROOT` is unset or points to a
directory without binary Data.db files. The tests return empty results (not an error)
when no files are found. This is the failure mode the gate's dataset preflight prevents.

```bash
# Verify data is present
find "$CQLITE_DATASETS_ROOT/sstables" -name "*-Data.db" | wc -l
# Must be > 0 (should be 33+ for the full corpus)
```

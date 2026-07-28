---
name: Test Data Generation & Validation
description: Generate real Cassandra 5.0 test data using Docker containers, export SSTables with proper directory structure, validate parsing against sstabledump, and manage test datasets. Use when working with test data generation, dataset creation, SSTable export, validation, fixture management, or sstabledump comparison.
---

# Test Data Generation & Validation

This skill provides guidance on generating real Cassandra 5.0 test data and validating parsing correctness.

## When to Use This Skill

- Generating test data with specific schemas
- Creating test fixtures for property tests
- Exporting SSTables from Cassandra
- Validating parsed data against sstabledump
- Managing test datasets
- Creating reproducible test scenarios

## Overview

CQLite uses real Cassandra 5.0 instances to generate test data, ensuring:
- Format correctness (real Cassandra writes)
- Edge case coverage (nulls, empty values, large values)
- Compression validation (actual compressed SSTables)
- Schema variety (all CQL types)

## A fixture proves nothing about a property it does not VARY (issue #3042)

Before choosing or generating a fixture, name the property under test and ask whether this
fixture **varies** it. If it does not, the fixture cannot validate it — the property is
**UNTESTED**, not proven, and the resulting green is meaningless. This is a *selection-time*
check: once the fixture is in place the test passes and nothing flags the gap.

- **A single-component clustering key cannot validate a multi-component invariant.** Component
  separators, `NEXT_COMPONENT` framing between components, and mixed ASC/DESC inversion all
  need arity ≥ 2 to be exercised at all.
- **Values that happen to share a first byte cannot validate byte-discriminated trie/index
  logic.** BTI partition/row tries branch on transition bytes; keys with an identical leading
  byte never force the discriminating branch.
- **Correctness resting on an incidental property of one fixture is untested.** Real instance:
  the pre-fix BTI root-delta base was 2 bytes low, and the wrong root pointed *straight at the
  root's only child* purely because that fixture's child node happened to be 2 bytes wide —
  the defect was invisible until a fixture varied the node width (issue #3002, pinned in
  `cqlite-core/tests/issue_3002_bti_rows_root_base.rs`).

So: vary the arity, vary the leading bytes, vary the widths and the orders. A corpus of one
shape is a corpus of one datapoint. See also the self-round-trip blind spot in `CLAUDE.md`
§Testing — a fixture you generated with CQLite cannot be the oracle for CQLite.

## Test Data Workflow

See [dataset-generation.md](dataset-generation.md) for complete workflow details.

### Quick Start

**Full corpus regeneration (recommended):**
```bash
# Regenerates nb + oa + da keyspaces in one command
bash test-data/scripts/regenerate-datasets.sh

# With custom row count
bash test-data/scripts/regenerate-datasets.sh --rows 200
```

**Compose-stack workflow (interactive / partial):**
All paths below are repo-root-relative; run via `bash` (`regenerate-datasets.sh` is not
committed executable, so `./` invocation fails on it):

```bash
# 1. Start clean Cassandra 5 with schemas (container: cqlite-cassandra-5-0)
bash test-data/scripts/start-clean.sh

# 2. (Optional) run CQL inserts manually via cqlsh
docker exec -it cqlite-cassandra-5-0 cqlsh

# 3. Export SSTables + generate JSONL goldens
bash test-data/scripts/export.sh

# 4. Shutdown and clean volumes
bash test-data/scripts/shutdown-clean.sh
```

## Generation Scripts

### regenerate-datasets.sh (primary entrypoint)
Full corpus regeneration: nb + oa + da keyspaces, single Docker container.

**What it does:**
1. Starts `cassandra:5.0.2` container (`cqlite-regen`)
2. Applies schemas and inserts rows for nb keyspaces (CASSANDRA_4 compat mode)
3. Restarts with `storage_compatibility_mode: NONE`, creates oa keyspace
4. Restarts with `sstable.selected_format: bti`, creates da keyspace
5. Exports SSTables via tar stream + generates sstabledump JSONL goldens
6. Writes `metadata.yml`

**Options:**
- `--rows N` - Rows per nb table (default: 50)
- `--out <dir>` - Output directory (default: test-data/datasets)
- `--dry-run` - Print commands without executing

**Example:**
```bash
bash test-data/scripts/regenerate-datasets.sh
bash test-data/scripts/regenerate-datasets.sh --rows 200 --out /tmp/ds
bash test-data/scripts/regenerate-datasets.sh --dry-run
```

### start-clean.sh
Starts Cassandra 5.0 container (via compose) and applies schemas.

**What it does:**
1. Starts the `cqlite-cassandra-5-0` container via `test-data/docker/docker-compose-cassandra5.yml`
2. Waits for Cassandra to be healthy
3. Applies schemas from `schemas/core.list`

**Environment variables:**
- `SCHEMA_SET=core` - Use curated schema list (default)
- `SCHEMA_SET=all` - Use all *.cql files

**Example:**
```bash
bash test-data/scripts/start-clean.sh
SCHEMA_SET=all bash test-data/scripts/start-clean.sh
```

### export.sh
Exports SSTables from the running Cassandra container and generates reference files.

**What it does:**
1. Runs `nodetool flush` to write all memtables to disk
2. Streams SSTables from container to `datasets/sstables/` via tar
3. Generates sstabledump JSONL golden files and sstablemetadata statistics
4. Writes `metadata.yml` using cqlsh (no external generator container needed)
5. Writes `references.yml` manifest

**Output structure:**
```
test-data/datasets/
├── metadata.yml
├── references.yml
└── sstables/
    ├── test_basic/
    │   └── simple_table-<hash>/
    │       ├── *-Data.db
    │       ├── *-Data.db.jsonl   # sstabledump golden
    │       ├── *-Index.db
    │       ├── *-Statistics.db
    │       └── *-TOC.txt
    ├── test_collections/
    └── test_timeseries/
```

### shutdown-clean.sh
Stops Cassandra and removes Docker volumes.

**What it does:**
1. Stops all containers
2. Removes Docker volumes (clean slate)
3. Prepares for next generation cycle

**Use when:**
- Done with current dataset
- Want to regenerate from scratch
- Cleaning up after tests

## Test Schemas

Schemas in `test-data/schemas/`:

`schemas/core.list` names the curated set `start-clean.sh` applies by default:
`basic-types.cql`, `collections.cql`, `time-series.cql`, `wide-rows.cql` (→ keyspaces
`test_basic`, `test_collections`, `test_timeseries`, `test_wide_rows`).

Beyond the core set the directory holds the format-specific and parity fixtures — `oa-test.cql`,
`da-test.cql` (BTI), `cql-type-parity.cql`, `compaction-parity*.cql`, `tombstone-parity.cql`,
`compression-parity.cql`, `write-load-parity.cql`, `deltas.cql`, `wide-table-bti.cql`, plus
`udts/` and `legacy/`.

**Read the `.cql` for the actual DDL** — column sets and clustering shapes change with the fixtures,
and row counts come from the generator (`--rows N`, default 50), not from the schema. Which tables
are enforced vs skip-pending lives in `test-data/validation-matrix.md` +
`test-data/corpus-coverage-policy.md`, and the corpus is enumerated from disk per run (#1229) —
never hard-code a table count.

### Custom Schemas
Add your own:
```bash
# Create schema
echo "CREATE TABLE test_keyspace.my_table (...);" > schemas/my-schema.cql

# Add to core.list
echo "my-schema.cql" >> schemas/core.list

# Generate
bash test-data/scripts/regenerate-datasets.sh
```

## Validation Workflow

See [validation-workflow.md](validation-workflow.md) for complete validation process.

### Validate Against sstabledump

Table directories carry a UUID suffix (`<table>-<uuid>/`), so glob rather than hard-coding a path;
`<schema>.cql` is one of the files in `test-data/schemas/` (e.g. `basic-types.cql`).

```bash
TABLE_DIR=$(echo test-data/datasets/sstables/<keyspace>/<table>-*)

# 1. Generate sstabledump reference
#    (a committed `*-Data.db.jsonl` golden already sits beside every Data.db —
#     prefer comparing against that unless you are refreshing the golden)
sstabledump "$TABLE_DIR"/*-Data.db > reference.json

# 2. Parse with cqlite
cargo run --bin cqlite -- \
    --data-dir "$TABLE_DIR" \
    --schema test-data/schemas/<schema>.cql \
    --out json > cqlite.json

# 3. Compare (ignoring formatting)
jq -S '.' reference.json > ref-sorted.json
jq -S '.' cqlite.json > cql-sorted.json
diff ref-sorted.json cql-sorted.json
```

### Automated Validation

```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets

# All enumerated tables (smoke)
bash test-data/scripts/smoke-test-all-tables.sh

# Query-semantics parity oracle (post-reconciliation SELECT at a pinned `now`)
cargo test --package cqlite-core --test query_semantics_oracle_parity

# Point-vs-full differential lane
cargo test --package cqlite-core --test point_vs_full_differential
```

There is no `--test sstable_validation` target — the three entrypoints above are the real ones.

## Property Testing

Generate random data for property tests:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_row_parsing_roundtrip(
        partition_key in any::<i32>(),
        text_value in "\\PC*",  // Any valid unicode
        int_value in any::<i32>(),
    ) {
        // Generate test data in Cassandra
        insert_test_row(partition_key, &text_value, int_value)?;
        flush_memtable()?;
        
        // Parse with cqlite
        let parsed = parse_sstable()?;
        
        // Validate roundtrip
        assert_eq!(parsed.get_int("partition_key"), partition_key);
        assert_eq!(parsed.get_text("text_col"), text_value);
        assert_eq!(parsed.get_int("int_col"), int_value);
    }
}
```

## Dataset Packaging

Package datasets for CI or distribution:

```bash
# Package current dataset (the script decides the archive name — read its output)
bash test-data/scripts/package_datasets.sh
```

**Contents:**
- All SSTables
- metadata.yml
- Schema files
- README with generation parameters

## CI Integration

### Smoke Test
Quick validation in CI:

```bash
# Fetch the pinned dataset release (the script carries tag + asset + sha256)
bash test-data/scripts/fetch-datasets.sh

# Run core tests
bash test-data/scripts/ci-one-shot-smoke.sh

# Validates:
# - Basic parsing
# - All CQL types
# - Compression
# - Collections
```

See `test-data/scripts/CI_SMOKE_TEST_USAGE.md` for details.

## Common Scenarios

### Scenario 1: Test New CQL Type
```bash
# 1. Add column to schema
echo "ALTER TABLE test_basic.simple_table ADD duration_col duration;" \
    >> schemas/basic-types.cql

# 2. Regenerate data
bash test-data/scripts/regenerate-datasets.sh

# 3. Validate parsing
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
bash test-data/scripts/smoke-test-all-tables.sh
cargo test --package cqlite-core --test query_semantics_oracle_parity
```

### Scenario 2: Test Large Values
```bash
# Generate with higher row count
bash test-data/scripts/regenerate-datasets.sh --rows 10000
```

### Scenario 3: Test Edge Cases
Add a new schema `.cql` file and CQL INSERT statements, then run
`regenerate-datasets.sh`.  For small one-off tests, use the compose stack:

```bash
bash test-data/scripts/start-clean.sh
docker exec -it cqlite-cassandra-5-0 cqlsh -e "INSERT INTO test_basic.simple_table ..."
bash test-data/scripts/export.sh
```

## PRD Alignment

**Supports Milestone M1** (Core Reading Library):
- 95% test coverage goal
- All CQL types validated
- Real Cassandra data ensures format correctness

**Supports All Milestones:**
- Regression testing with frozen datasets
- Property-based testing for edge cases
- CI integration for PR validation

## Troubleshooting

The compose stack's container is named **`cqlite-cassandra-5-0`** (see `container_name:` in
`test-data/docker/docker-compose-cassandra5.yml`). Every `docker exec`/`docker logs` below assumes
you brought it up first (`bash test-data/scripts/start-clean.sh`) — check `docker ps` before
concluding a command "failed". Note `regenerate-datasets.sh` uses its own separate container
(`cqlite-regen`), not this one.

### Cassandra Won't Start
```bash
docker ps -a | grep cqlite-cassandra-5-0
docker logs cqlite-cassandra-5-0

# Common issue: Port 9042 in use
lsof -i :9042
# Kill process or change port in test-data/docker/docker-compose-cassandra5.yml
```

### Generation Fails
```bash
# Read the script's own stdout/stderr (capture it; there is no committed log directory)
bash test-data/scripts/regenerate-datasets.sh > /tmp/regen.log 2>&1

# Verify schema applied
docker exec cqlite-cassandra-5-0 cqlsh -e "DESCRIBE KEYSPACES;"
```

### Export Produces No Files
```bash
# Verify data exists in container
docker exec cqlite-cassandra-5-0 ls -la /var/lib/cassandra/data/

# Check if flush happened
docker logs cqlite-cassandra-5-0 | grep flush
```

## Dataset Repository

Packaged datasets live in GitHub releases on this repo, but **never hard-code the tag or asset
name** — the pin moves. Fetch via the script, which carries the current pinned
tag/asset/sha256 and verifies the download:

```bash
bash test-data/scripts/fetch-datasets.sh
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
```

Overrides exist for one-off pins (`DATASET_TAG`, `DATASET_ASSET`, `DATASET_SHA256`); bumping the
committed pin is `test-data/scripts/bump-dataset-pin.sh`.

Download for:
- CI without Docker
- Reproducible benchmarks
- Offline development

## Next Steps

When creating new tests:
1. Design schema in `schemas/`
2. Regenerate data with `regenerate-datasets.sh`
3. Write parser test
4. Validate with sstabledump
5. Add to CI smoke test suite

See documentation:
- [dataset-generation.md](dataset-generation.md) - Full workflow
- [validation-workflow.md](validation-workflow.md) - Validation process


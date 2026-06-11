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
```bash
cd test-data

# 1. Start clean Cassandra 5 with schemas
./scripts/start-clean.sh

# 2. (Optional) run CQL inserts manually via cqlsh
docker exec -it cqlite-cassandra-5-0 cqlsh

# 3. Export SSTables + generate JSONL goldens
./scripts/export.sh

# 4. Shutdown and clean volumes
./scripts/shutdown-clean.sh
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
1. Starts `cassandra-5-0` container via docker-compose
2. Waits for Cassandra to be healthy
3. Applies schemas from `schemas/core.list`

**Environment variables:**
- `SCHEMA_SET=core` - Use curated schema list (default)
- `SCHEMA_SET=all` - Use all *.cql files

**Example:**
```bash
./scripts/start-clean.sh
SCHEMA_SET=all ./scripts/start-clean.sh
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

### basic-types.cql
Simple table with all primitive types:
- Partition key: uuid
- No clustering
- Columns: int, text, timestamp, boolean, etc.

### collections.cql
Collection types:
- list<int>
- set<text>
- map<text, int>
- Nested frozen collections

### time-series.cql
Time-series pattern:
- Partition key: sensor_id
- Clustering: timestamp (DESC)
- Columns: temperature, humidity, pressure

### wide-rows.cql
Wide partition testing:
- Single partition key
- Many clustering rows (1000+)
- Tests pagination and offset handling

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

```bash
# 1. Generate sstabledump reference
sstabledump test-data/datasets/sstables/keyspace/table/*-Data.db \
    > reference.json

# 2. Parse with cqlite
cargo run --bin cqlite -- \
    --data-dir test-data/datasets/sstables/keyspace/table \
    --schema test-data/schemas/schema.cql \
    --out json > cqlite.json

# 3. Compare (ignoring formatting)
jq -S '.' reference.json > ref-sorted.json
jq -S '.' cqlite.json > cql-sorted.json
diff ref-sorted.json cql-sorted.json
```

### Automated Validation

Run validation script:
```bash
# Validate all test tables
cargo test --test sstable_validation

# Validate specific table
cargo test --test sstable_validation -- simple_table
```

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
# Package current dataset
./scripts/package_datasets.sh

# Output: test-data/cqlite-test-data-v5.0-<date>.tar.gz
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
# Use packaged dataset
tar xzf cqlite-test-data-v5.0.tar.gz

# Run core tests
./scripts/ci-one-shot-smoke.sh

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
cargo test --test sstable_validation
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

### Cassandra Won't Start
```bash
# Check logs
docker logs cassandra-5-0

# Common issue: Port 9042 in use
lsof -i :9042
# Kill process or change port in docker-compose-cassandra5.yml
```

### Generation Fails
```bash
# Check generator logs
cat test-data/logs/data_generation.log

# Verify schema applied
docker exec cassandra-5-0 cqlsh -e "DESCRIBE KEYSPACES;"
```

### Export Produces No Files
```bash
# Verify data exists in container
docker exec cassandra-5-0 ls -la /var/lib/cassandra/data/

# Check if flush happened
docker logs cassandra-5-0 | grep flush
```

## Dataset Repository

Packaged datasets available at:
```
https://github.com/pmcfadin/cqlite/releases/tag/test-data-v5.0
```

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


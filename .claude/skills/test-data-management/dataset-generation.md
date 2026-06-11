# Dataset Generation Workflow

Complete guide to generating the CQLite test corpus (datasets-v3: nb + oa + da).

## Current corpus: datasets-v3

Three SSTable version/format tiers produced by Cassandra 5.0.2:

| Tier | Version | Format | Keyspaces | Tables |
|------|---------|--------|-----------|--------|
| Primary | `nb` | `big` | test_basic, test_collections, test_timeseries, test_wide_rows | 33 |
| OA extended | `oa` | `big` | test_oa | 6 |
| BTI extended | `da` | `bti` | test_da | 3 |

Golden JSONL files (sstabledump output) are committed alongside each table's
binary SSTables.

## Full corpus regeneration

Use `regenerate-datasets.sh` to reproduce all tiers in one command:

```bash
# Full regeneration (~50 rows/table, default output to test-data/datasets)
bash test-data/scripts/regenerate-datasets.sh

# Custom row count and output directory
bash test-data/scripts/regenerate-datasets.sh --rows 200 --out /tmp/new-datasets

# Dry-run (prints all docker commands without executing)
bash test-data/scripts/regenerate-datasets.sh --dry-run
```

### How it works

The script runs a **single `cassandra:5.0.2` container** (`cqlite-regen`)
through three phases, restarting the container between phases to change format:

1. **nb phase** (default `storage_compatibility_mode: CASSANDRA_4`)
   - Applies `basic-types.cql`, `collections.cql`, `time-series.cql`, `wide-rows.cql`
   - Inserts rows via inline Python (cassandra-driver installed in container)
   - `nodetool flush` + `nodetool compact` for each keyspace

2. **oa phase** (`storage_compatibility_mode: NONE`)
   - Edits `/etc/cassandra/cassandra.yaml` in container + restart
   - Applies `oa-test.cql`, inserts rows including tombstones
   - `nodetool flush` + `nodetool compact test_oa`

3. **da/BTI phase** (`sstable.selected_format: bti`)
   - Edits `cassandra.yaml` + restart
   - Applies `da-test.cql`, inserts rows
   - `nodetool flush` + `nodetool compact test_da`

4. **Export**
   - Streams `/var/lib/cassandra/data` out via `tar | tar`
   - Runs `sstabledump -l` inside container for every `*-Data.db`
   - Writes `datasets/metadata.yml`

After regeneration:

```bash
# Smoke-test 39 tables (nb=33 + oa=6; da=3 currently SKIP-PENDING BTI)
bash test-data/scripts/smoke-test-all-tables.sh

# Package as release tarball
bash test-data/scripts/package_datasets.sh

# Publish to GitHub releases
bash test-data/scripts/publish_datasets.sh
```

## Compose-stack workflow (interactive / partial regeneration)

For interactive work, schema iteration, or single-keyspace exports, use the
compose stack and manual export:

```bash
# 1. Start Cassandra 5 + apply schemas
bash test-data/scripts/start-clean.sh

# 2. Run CQL inserts manually or via cqlsh
docker exec -it cqlite-cassandra-5-0 cqlsh
# > SOURCE '/opt/schemas/basic-types.cql';
# > INSERT INTO test_basic.simple_table ...;

# 3. Export SSTables + generate JSONL goldens
#    (uses cqlsh for metadata — no external generator container)
bash test-data/scripts/export.sh

# 4. Shutdown + remove volumes
bash test-data/scripts/shutdown-clean.sh
```

## Step-by-step reference

### start-clean.sh

Starts `cassandra-5-0` via compose, waits for health, applies schemas from
`schemas/core.list`.

```bash
# Use default core schemas (basic, collections, timeseries, wide-rows)
bash test-data/scripts/start-clean.sh

# Use all *.cql files
SCHEMA_SET=all bash test-data/scripts/start-clean.sh
```

### export.sh

Flushes memtables, streams the Cassandra data directory to
`test-data/datasets/sstables/`, then runs `sstabledump` and `sstablemetadata`
for each Data.db to produce JSONL golden files and statistics.

Metadata (row counts) is now generated via `cqlsh` directly in the Cassandra
container — no separate generator container needed.

```bash
bash test-data/scripts/export.sh
```

### shutdown-clean.sh

Stops the compose stack and removes volumes.

```bash
bash test-data/scripts/shutdown-clean.sh
```

## Schema files

| File | Keyspace | Tables | Description |
|------|----------|--------|-------------|
| `basic-types.cql` | test_basic | 8 | Primitive types, composite keys, compression variants |
| `collections.cql` | test_collections | 8 | SET/LIST/MAP, nested, frozen, UDT collections |
| `time-series.cql` | test_timeseries | 9 | TWCS, TTL, time-bucketed counters |
| `wide-rows.cql` | test_wide_rows | 8 | Wide partitions, many columns, large blobs |
| `oa-test.cql` | test_oa | 6 | oa format: simple types, collections, UDT, TTL, static, tombstones |
| `da-test.cql` | test_da | 3 | da/BTI format: simple types, collections, TTL |

## CI integration

CI fetches binary SSTables from the `datasets-v3` GitHub release:

```bash
bash test-data/scripts/fetch-datasets.sh
```

See `.github/workflows/` for the full CI configuration.

## Known limitations

`regenerate-datasets.sh` **skips tables with UDT columns** (e.g.
`collections_with_udts` in `test_collections`) because the inline Python
inserter cannot construct UDT values; smoke tests on a freshly regenerated
corpus will show those tables as missing.

## Adding new schema types

1. Create `test-data/schemas/my-schema.cql`
2. Add `my-schema.cql` to `test-data/schemas/core.list`
3. Add insert logic to `regenerate-datasets.sh` (or run manually via cqlsh)
4. Regenerate: `bash test-data/scripts/regenerate-datasets.sh`
5. Package and publish: `bash test-data/scripts/package_datasets.sh`

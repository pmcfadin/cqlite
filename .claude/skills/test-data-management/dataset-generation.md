# Dataset Generation Workflow

Complete guide to generating the CQLite test corpus (datasets-v3: nb + oa + da).

## Current corpus: datasets-v3

Three SSTable version/format tiers produced by Cassandra 5.0.2 by
`regenerate-datasets.sh`:

| Tier | Version | Format | Keyspaces |
|------|---------|--------|-----------|
| Primary | `nb` | `big` | test_basic, test_collections, test_timeseries, test_wide_rows |
| OA extended | `oa` | `big` | test_oa |
| BTI extended | `da` | `bti` | test_da |

> This is what **this script** regenerates, not the full enforced corpus — other keyspaces
> (e.g. `test_big`, `test_comp`) come from their own `generate-*.sh` fixture scripts and are
> also enforced. The authoritative, disk-derived scope is
> `test-data/validation-matrix.md` + `test-data/corpus-coverage-policy.md`; counts are
> re-derived per run (#1229), never hard-coded.

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
# Smoke-test the whole corpus. The table set is ENUMERATED FROM DISK per run
# (issue #1229 retired the hand-typed allowlist) — never hard-code a count.
# Enforced scope + skip-pending keyspaces: test-data/validation-matrix.md
#                                          test-data/corpus-coverage-policy.md
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

Starts the `cqlite-cassandra-5-0` container via
`test-data/docker/docker-compose-cassandra5.yml`, waits for health, applies schemas from
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

The table counts above describe what these schemas create; they are **not** the coverage
contract. Enforced scope (which keyspaces run vs are skip-pending) lives in
`test-data/corpus-coverage-policy.md` + `test-data/validation-matrix.md`, and both the smoke
script and the binding parity suites enumerate the corpus **from disk** per run (#1229) — a
newly-committed keyspace is automatically in scope. Never assert a hard-coded table total.
`test-data/schemas/` also holds parity fixtures not listed here (compaction, tombstone,
compression, write-load, deltas, cql-type).

## CI integration

CI fetches the binary SSTables from the pinned GitHub dataset release. The script carries the
pin (tag + asset + sha256) — read it there, don't transcribe it:

```bash
bash test-data/scripts/fetch-datasets.sh
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
```

See `.github/workflows/` for the full CI configuration.

## Adding new schema types

1. Create `test-data/schemas/my-schema.cql`
2. Add `my-schema.cql` to `test-data/schemas/core.list`
3. Add insert logic to `regenerate-datasets.sh` (or run manually via cqlsh)
4. Regenerate: `bash test-data/scripts/regenerate-datasets.sh`
5. Package and publish: `bash test-data/scripts/package_datasets.sh`

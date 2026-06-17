# CQLite Test Data (Cassandra 5 — datasets-v3)

Test data for CQLite SSTable compatibility testing. The corpus covers three
SSTable version/format combinations (nb, oa, da) produced by Cassandra 5.0.2.

## Current corpus: datasets-v3

| Version | Format | Keyspace   | Tables | Notes                           |
|---------|--------|-----------|--------|---------------------------------|
| `nb`    | `big`  | test_basic, test_collections, test_timeseries, test_wide_rows | 33 | Default C* 5.0 compat mode |
| `oa`    | `big`  | test_oa   | 6      | `storage_compatibility_mode: NONE` |
| `da`    | `bti`  | test_da   | 3      | `sstable.selected_format: bti`  |

JSONL golden files (sstabledump output) are committed to git alongside the
SSTable binaries in each table directory.

## Fetching test data (CI / local development)

The git repository contains only JSONL reference files. Binary SSTables are
fetched separately:

```bash
bash test-data/scripts/fetch-datasets.sh
```

Without binary Data.db files, query tests will pass but return 0 rows.

## Directory structure

```
test-data/
├── docker/
│   └── docker-compose-cassandra5.yml   # Cassandra 5.0.2 service
├── schemas/
│   ├── basic-types.cql                 # test_basic keyspace (8 tables)
│   ├── collections.cql                 # test_collections keyspace (8 tables)
│   ├── time-series.cql                 # test_timeseries keyspace (9 tables)
│   ├── wide-rows.cql                   # test_wide_rows keyspace (8 tables)
│   ├── oa-test.cql                     # test_oa keyspace (6 tables, oa format)
│   ├── da-test.cql                     # test_da keyspace (3 tables, da/BTI format)
│   └── core.list                       # curated schema list for start-clean.sh
├── scripts/
│   ├── regenerate-datasets.sh          # Full corpus regeneration (see below)
│   ├── start-clean.sh                  # Start Cassandra + apply schemas
│   ├── export.sh                       # Flush + export SSTables + sstabledump goldens
│   ├── shutdown-clean.sh               # Stop and remove volumes
│   ├── fetch-datasets.sh               # Download release tarball
│   ├── package_datasets.sh             # Package for GitHub release
│   ├── publish_datasets.sh             # Publish to GitHub releases
│   ├── smoke-test-all-tables.sh        # Smoke-test all 39 tables
│   ├── compose-guard.sh                # Start + health-check helper
│   ├── container_env.sh                # Docker/Podman detection helpers
│   └── e2e-cassandra-readback.sh       # E2E readback acceptance gate
└── datasets/                           # .gitignored (binary SSTables)
    ├── metadata.yml
    ├── references.yml
    └── sstables/
        ├── test_basic/
        ├── test_collections/
        ├── test_timeseries/
        ├── test_wide_rows/
        ├── test_oa/
        └── test_da/
```

## Regenerating the corpus

Use `regenerate-datasets.sh` to reproduce all three format tiers from scratch
in a single Docker container (no compose stack required):

```bash
# Full regeneration with default row counts (~50 rows/table)
bash test-data/scripts/regenerate-datasets.sh

# Custom output directory and row count
bash test-data/scripts/regenerate-datasets.sh \
  --out /tmp/my-datasets \
  --rows 200

# Dry-run: print all docker commands without executing
bash test-data/scripts/regenerate-datasets.sh --dry-run
```

### What it does

1. Starts a `cassandra:5.0.2` container named `cqlite-regen`
2. **Phase 1 — nb corpus**: applies core schemas, inserts rows via inline
   Python (using the cassandra-driver installed inside the container), then
   flushes and compacts all nb keyspaces
3. **Phase 2 — oa corpus**: edits `cassandra.yaml`
   (`storage_compatibility_mode: NONE`), restarts the container, applies
   `oa-test.cql`, inserts rows (including tombstones), flushes and compacts
4. **Phase 3 — da corpus**: edits `cassandra.yaml`
   (`sstable.selected_format: bti`), restarts, applies `da-test.cql`, inserts
   rows, flushes and compacts
5. Streams the Cassandra data directory out via `tar | tar` into
   `datasets/sstables/`
6. Generates JSONL golden files for every Data.db using `sstabledump -l`
   (inside the container)
7. Writes `datasets/metadata.yml`

### Prerequisites

- Docker (or Podman) in PATH
- ~10 GB free disk space
- ~4 GB RAM available for Cassandra

### After regeneration

```bash
# Smoke-test all 39 tables (nb=33 + oa=6)
bash test-data/scripts/smoke-test-all-tables.sh

# Package as a tarball
bash test-data/scripts/package_datasets.sh

# Publish to GitHub Releases
bash test-data/scripts/publish_datasets.sh
```

## Using the compose stack for interactive work

For ad-hoc schema changes, manual CQL sessions, or testing against a live
cluster, use the compose stack directly:

```bash
# Start Cassandra and apply schemas
bash test-data/scripts/start-clean.sh

# Interactive CQL session
docker exec -it cqlite-cassandra-5-0 cqlsh

# Export SSTables and generate sstabledump golden files
bash test-data/scripts/export.sh

# Stop and remove volumes
bash test-data/scripts/shutdown-clean.sh
```

`export.sh` generates metadata.yml using `cqlsh` (no external generator
container required).

## Schema files

| File | Keyspace | Tables | Purpose |
|------|----------|--------|---------|
| `basic-types.cql` | test_basic | 8 | All primitive CQL types, compression variants, static columns, counters |
| `collections.cql` | test_collections | 8 | SET, LIST, MAP, nested, frozen, UDT-in-collections |
| `time-series.cql` | test_timeseries | 9 | TWCS, TTL, time-bucketed counters, sensor/event data |
| `wide-rows.cql` | test_wide_rows | 8 | Wide partitions, many-columns, large blobs, chat patterns |
| `oa-test.cql` | test_oa | 6 | oa-format simple types, collections, UDT >=128 bytes, TTL, static, tombstones |
| `da-test.cql` | test_da | 3 | da/BTI-format simple types, collections, TTL |

## Test data in CI

CI fetches binary SSTables from the `datasets-v3` GitHub Release asset:

```yaml
- name: Fetch test data
  run: bash test-data/scripts/fetch-datasets.sh
```

See `.github/workflows/` for the full CI configuration.

## Troubleshooting

**Port 9042 already in use:**
```bash
lsof -i :9042
```
The compose stack maps host port 9046 → container port 9042, so this only
conflicts if another service uses 9046.

**Cassandra OOM:** Increase Docker memory limit or reduce `--rows`.

**regenerate-datasets.sh container already exists:**
```bash
docker rm -f cqlite-regen
```

**sstabledump produces empty output:** Ensure the flush + compact steps ran
before the export. Check container logs: `docker logs cqlite-regen`.

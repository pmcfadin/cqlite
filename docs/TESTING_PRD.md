# CQLite Testing Strategy

> **Note**: The original phased PRD (Weeks 1–6, referencing `test-env/cassandra5/`) has been
> archived at [`docs/archive/TESTING_PRD.md`](archive/TESTING_PRD.md). That directory no longer
> exists. This file describes the current architecture.

## Current Architecture

CQLite's test data pipeline is fully implemented. The current approach does not use an on-demand
container pipeline; instead, pre-generated Cassandra 5.0 SSTables are fetched from a hosted
archive and validated with `sstabledump`.

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Fetch script | `test-data/scripts/fetch-datasets.sh` | Downloads pre-generated SSTable tarballs |
| Docker compose | `test-data/docker/docker-compose-cassandra5.yml` | Regenerates datasets from scratch when needed |
| Data generator | `test-data/docker/Dockerfile.data-generator` | Cassandra 5.0.2 data-generator image |
| Schemas | `test-data/schemas/` | CQL schema definitions for all keyspaces |
| JSONL references | `test-data/datasets/sstables/**/*.jsonl` | Golden files from `sstabledump` for parity checks |
| Validation matrix | `test-data/validation-matrix.md` | Current per-table pass/fail status |

### Quick Start

```bash
# Fetch pre-generated binary SSTable files (required for integration tests)
bash test-data/scripts/fetch-datasets.sh

# Run integration tests against real SSTable data
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core

# Smoke-test all 33 tables
bash test-data/scripts/smoke-test-all-tables.sh
```

### CI Integration

Tests run automatically via GitHub Actions on every pull request:
- `rust-ci.yml` — Rust unit and integration tests
- `python-ci.yml` — Python binding tests (pytest, 360+ tests)
- `node-ci.yml` — Node.js binding tests (Jest, 255+ tests)

Integration tests pass `CQLITE_DATASETS_ROOT` pointing at the fetched dataset directory.
The 33 tables across four keyspaces (`test_basic`, `test_collections`, `test_timeseries`,
`test_wide_rows`) are validated for row-count and cell-level parity against the JSONL golden
files. Current pass rate: **100% (33/33 tables)**.

### Regenerating Test Data

To regenerate the SSTable dataset from scratch (requires Docker):

```bash
cd test-data/docker
docker compose -f docker-compose-cassandra5.yml up --build
```

This starts Cassandra 5.0.2, runs the data-generator service, and exports the resulting
SSTables. See `test-data/docker/docker-compose-cassandra5.yml` for details.

---
title: Test Data
description: Fetching the dataset, dataset pins, CQLITE_DATASETS_ROOT, and what happens without Data.db files.
sidebar:
  label: Test data
  order: 4
---

The git repository contains only JSONL reference files (sstabledump output, used for
parity validation). Binary `*-Data.db` files are NOT in git — they are fetched from a
GitHub release asset.

## Fetching the dataset

```bash
bash test-data/scripts/fetch-datasets.sh
```

This downloads, verifies (SHA256), and extracts to `test-data/datasets/`. It also
removes macOS AppleDouble (`._*`) and `.DS_Store` files that macOS `tar` embeds in
archives and that break file-suffix scanners like `*-Data.db`.

## Dataset pins

The fetch script uses these defaults (override with environment variables):

```bash
DATASET_TAG=datasets-v3
DATASET_ASSET=cassandra5-small-full-v3.2.tar.gz
DATASET_SHA256=bebc763752c8d68c7fb0483a1b31294b4d1d21343d3f7d124da069e5073202fa
```

CI reads the same pins from `.github/workflows/sstabledump-parity-gate.yml`:

```yaml
DATASET_TAG: datasets-v3
DATASET_ASSET: cassandra5-small-full-v3.2.tar.gz
DATASET_SHA256: bebc763752c8d68c7fb0483a1b31294b4d1d21343d3f7d124da069e5073202fa
```

**Why SHA256 is the cache key**: CI caches the extracted dataset by SHA256. Using the
tag name as the cache key would allow a re-published asset (same tag, different
content) to serve stale data. The SHA256 is content-addressed — cache hit means the
exact bytes are correct.

**Why v3.2 (asset history)**: The v3 tarball was produced on macOS and contained
AppleDouble entries; v3.1 was repackaged with `--exclude='*/._*'` to strip them.
v3.2 (issue #1099) republished the corpus to include the Epic #970 `test_comp` +
`corruption/test_comp_corrupt` fixtures, which `issue_1000_verifier` requires —
the v3.1 asset predated those fixtures, so the `test` lane could only clean-skip
that coverage. All assets share the `datasets-v3` tag (it holds multiple versions;
the SHA256 pin selects the exact bytes). When inspecting or repackaging archives,
use Python's `tarfile` module (platform-neutral) rather than `tar -tf` (macOS
bsdtar hides `._*` on listing and re-embeds them on repack).

## CQLITE_DATASETS_ROOT

Set this environment variable to point at the extracted dataset directory:

```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
```

The gate script sets it automatically to `$REPO_ROOT/test-data/datasets` if not
already set. Integration test runners require it; CI sets it in the workflow env.

**Without it**, tests that scan for SSTables will find no files and return results as
if no data exists — not an error, just empty. This is the silent-pass failure mode
the gate's dataset preflight check was added to catch.

## What happens without Data.db files

- `cargo test --package cqlite-core` — unit tests pass; integration tests that scan
  `CQLITE_DATASETS_ROOT` return 0 rows and count as passing
- `scripts/agent-gate.sh` — **aborts with exit code 1** before running any component;
  you must fetch the data first
- Smoke tests — fail immediately; the smoke script expects at least one Data.db per table

The gate's dataset preflight check:
```bash
DATA_COUNT=$(find "$CQLITE_DATASETS_ROOT/sstables" -name "*-Data.db" 2>/dev/null | wc -l | tr -d ' ')
if [ "$DATA_COUNT" -eq 0 ]; then
  echo "agent-gate: no Data.db files under $CQLITE_DATASETS_ROOT/sstables" >&2
  echo "agent-gate: fetch them first: bash test-data/scripts/fetch-datasets.sh" >&2
  exit 1
fi
```

## Dataset layout

After extraction:

```
test-data/datasets/
├── metadata.yml
├── references.yml
└── sstables/
    ├── test_basic/
    │   └── simple_table-<hash>/
    │       ├── nb-1-big-Data.db        ← binary (not in git)
    │       ├── nb-1-big-Data.db.jsonl  ← sstabledump golden (in git)
    │       ├── nb-1-big-Index.db
    │       ├── nb-1-big-Statistics.db
    │       └── nb-1-big-TOC.txt
    ├── test_collections/
    ├── test_timeseries/
    └── test_wide_rows/
```

## Dataset tiers

| Tier | SSTable version | Format | Keyspaces | Tables |
|------|-----------------|--------|-----------|--------|
| Primary | `nb` | `big` | test_basic, test_collections, test_timeseries, test_wide_rows | 33 |
| OA extended | `oa` | `big` | test_oa | 6 |
| BTI | `da` | `bti` | test_da | 3 (smoke SKIP-PENDING BTI parser) |

**Current pass rate**: 100% (33/33 nb tables as of Dec 2025). The da/BTI tables are
excluded from the default smoke run pending full BTI parser support.

## Keyspace overview

| Keyspace | Tables | Purpose |
|----------|--------|---------|
| test_basic | 8 | Simple types |
| test_collections | 8 | Lists, sets, maps |
| test_timeseries | 9 | Time-series patterns |
| test_wide_rows | 8 | Wide partitions |

## Regenerating the corpus

If you need to regenerate the test data (e.g., after schema changes):

```bash
# Full regeneration — three SSTable tiers, ~50 rows/table
bash test-data/scripts/regenerate-datasets.sh

# Custom row count
bash test-data/scripts/regenerate-datasets.sh --rows 200

# Dry-run
bash test-data/scripts/regenerate-datasets.sh --dry-run
```

Requires Docker. The script runs a `cassandra:5.0.2` container through three phases
to produce nb, oa, and da/BTI SSTables. After regeneration, package and publish:

```bash
bash test-data/scripts/package_datasets.sh
bash test-data/scripts/publish_datasets.sh
```

See `.claude/skills/test-data-management/dataset-generation.md` in the repo for the
full workflow including the compose-stack interactive path.

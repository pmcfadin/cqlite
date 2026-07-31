---
title: Test Data
description: Fetching the dataset, dataset pins, CQLITE_DATASETS_ROOT, the checkout-relative schemas root, and what happens without Data.db files.
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

This downloads, verifies (SHA256), and extracts to `$CQLITE_DATASETS_ROOT` (default
`test-data/datasets/`). It also removes macOS AppleDouble (`._*`) and `.DS_Store` files
that macOS `tar` embeds in archives and that break file-suffix scanners like `*-Data.db`.

**Use the export line the script prints (issue #3131).** Both exit paths — fresh
extraction *and* warm cache — now re-verify the content at the extraction target and print
the one root that run guarantees:

```
Dataset root VERIFIED (warm cache, download skipped): /data/datasets — 155 *-Data.db present
Use EXACTLY this root (the only one this run guarantees):

  export CQLITE_DATASETS_ROOT=/data/datasets
```

**That printed line takes precedence over any root remembered from CLAUDE.md or from this
page.** If `CQLITE_DATASETS_ROOT` was already set in the environment, the fetch extracts
*there* and never populates the checkout's `test-data/datasets` — so exporting
`$PWD/test-data/datasets` afterwards gives you a corpus-less root. The script prints an
explicit `NOTE:` when the two differ. Before #3131 the warm path exited 0 having named no
root at all, which is how the documented remedy silently failed to remedy.

Two flags:

```bash
bash test-data/scripts/fetch-datasets.sh --verify-only   # is this root usable? mutates NOTHING
bash test-data/scripts/fetch-datasets.sh --help
```

`--verify-only` downloads, extracts, removes and creates nothing (it will not even `mkdir`
the parent of the root it is probing), so it is safe to run against a live corpus. **Every
unrecognized argument is rejected with exit 2**, deliberately: the script's default path is
destructive (`rm -rf` on the dataset root), so a typo like `-verify-only` must never
silently select it.

## Dataset pins

The fetch script uses these defaults (override with environment variables):

```bash
DATASET_TAG=datasets-v3
DATASET_ASSET=cassandra5-small-full-v3.4.tar.gz
DATASET_SHA256=3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33
```

CI reads the same pins from `.github/workflows/sstabledump-parity-gate.yml`:

```yaml
DATASET_TAG: datasets-v3
DATASET_ASSET: cassandra5-small-full-v3.4.tar.gz
DATASET_SHA256: 3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33
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

This environment variable points at the extracted dataset directory — use the absolute
path `fetch-datasets.sh` printed, e.g.:

```bash
export CQLITE_DATASETS_ROOT=/data/datasets
```

The gate script defaults it to `$REPO_ROOT/test-data/datasets` if not already set.
Integration test runners require it; CI sets it in the workflow env.

**`CQLITE_DATASETS_ROOT` alone is sufficient, on every layout (issues #3131 / #3148).**
The corpus root does **not** need a `schemas` sibling next to it, and no composite/symlinked
root has to be assembled to make one appear. Any usable corpus root works, including one
entirely outside the checkout.

**Without it**, tests that scan for SSTables will find no files and return results as
if no data exists — not an error, just empty. This is the silent-pass failure mode
the gate's dataset preflight check was added to catch.

## The schemas root is checkout-relative, not a sibling of the corpus

`test-data/schemas/` (23 **committed** files, including the `legacy/` and `udts/`
subdirectories) is source, not fetched data. It is never part of the dataset archive.

Tests and benches resolve it **checkout-relative** through the single shared helper
`test-data/support/fixture_roots.rs`, anchored on the enclosing checkout's workspace-root
`Cargo.toml` (the nearest ancestor manifest declaring `[workspace]`). Nothing climbs `..`
from the datasets root any more.

The retired idiom was `datasets_root().join("../schemas")`, and it failed two ways:

1. It made **committed source's** location depend on an env var whose entire purpose is to
   point at *relocatable fetched data*. A machine caching its corpus at `/data/datasets`
   then needed a `/data/schemas` that no `git checkout` ever creates — the #3131 report,
   "no single `CQLITE_DATASETS_ROOT` works."
2. `join("..")` is **not** a lexical parent at the syscall level: the kernel resolves
   `datasets/..` against the *symlink target's* parent. So a symlinked `/data/datasets`
   resolved `../schemas` into the checkout while a real directory resolved it to
   `/data/schemas` — two visually identical layouts, opposite outcomes, no error explaining
   why (#3148's "symlink trap"). Anchoring on the checkout removes the `..` component
   entirely, so there is nothing left to mis-resolve.

Consequence for operators: **do not create a `schemas` symlink next to your corpus.** If a
runbook or an older report told you to, that instruction is retired.

### CQLITE_SCHEMAS_ROOT (optional, and MUST be absolute)

`CQLITE_SCHEMAS_ROOT` overrides the checkout default. It exists only for a genuinely
out-of-tree run (a packaged corpus plus schemas shipped together, no checkout) — the normal
case needs no environment variable at all.

| Value | Result |
|-------|--------|
| unset / blank | checkout-relative default (an exported-but-empty var is a scripting accident, never a root) |
| **relative** | **REJECTED fail-closed** — the gate FAILs, the test helper panics |
| absolute + readable directory | used, and reported as an override |
| absolute but not a directory | falls back to the checkout default, so a stale export degrades instead of breaking every fixture load |

**Why a relative value is rejected rather than resolved**: it cannot mean the same thing on
both sides of the contract. `scripts/agent-gate.sh` evaluates it with CWD = repository root,
while cargo runs each test binary with CWD = the *package* directory. A relative override
would let the gate stamp `schemas: 6/6 … under packaged/schemas (override)` while every test
binary silently read the checkout's schemas instead — a SUMMARY certifying root A for a run
that used root B, which is exactly the misleading-`STATUS: OK` defect #3148 was filed for.
Rejecting it makes the two sides agree by construction.

## What happens without Data.db files

- `cargo test --package cqlite-core` — unit tests pass; integration tests that scan
  `CQLITE_DATASETS_ROOT` return 0 rows and count as passing
- `scripts/agent-gate.sh` — **aborts with exit code 1** before running any component;
  you must fetch the data first
- Smoke tests — fail immediately; the smoke script expects at least one Data.db per table

The FULL gate's fixture preflight has **two** fail-closed causes, one per half of the fixture
contract, and both are textually distinct in the SUMMARY:

| Marker | Cause | Opt-out |
|--------|-------|---------|
| `missing-fixtures: FAIL-CLOSED (#2078)` | the fetched `test_basic` corpus is absent | `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` (stamps a visible `missing-fixtures: OPT-OUT (…)`) |
| `missing-schemas: FAIL-CLOSED (#3148)` | a canonical `.cql` under the resolved schemas root is not a readable regular file, **or** `CQLITE_SCHEMAS_ROOT` was relative and got rejected | **none, by design** |

The corpus guard runs first, so a run missing both reports the #2078 cause — the fetched half
is the one an operator must act on. On success the SUMMARY carries a positive
`schemas: N/N canonical .cql readable under <root> (<source>)` line, so a pasted block shows
the check ran rather than merely that nothing complained. `--lite` and `--only` stay lenient;
only the FULL gate is strict. Details: [Gate contract](/cqlite/agents-developing/gate-contract/).

**Why `missing-schemas:` has no opt-out.** The fetched corpus is legitimately absent on a
fresh box, so #2078 needs an escape hatch. Committed source in a checkout never is — an
unreachable schemas root means a broken checkout or a stale override, and neither may certify
a run. An opt-out could only ever buy a vacuous green.

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

`test-data/schemas/` is **not** shown above and is not part of this tree: it is committed
source living beside `datasets/` in the checkout, and it is resolved from the checkout, not
from `$CQLITE_DATASETS_ROOT`. When the corpus is relocated outside the checkout there is no
`schemas` sibling next to it, and none is needed.

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

# cqlite-core benchmarks

Single-threaded [Criterion](https://github.com/bheisler/criterion.rs)
micro-benchmarks for the `cqlite-core` public API. This is **Phase 1** of the
performance plan (Epic #541): the in-repo regression wall. It answers one
question — "did this PR get slower?" — against small, fixed, deterministic
fixtures. Headline numbers, volume tiers, concurrency sweeps, and codec sweeps
are Phase 2 and live in the external `cqlite-perf` harness, not here.

## Running

```bash
# Full measurement run (writes HTML to target/criterion/)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo bench -p cqlite-core

# Single bench
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --bench partition_lookup

# Compile + smoke run only (one iteration per bench — used in CI gating)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --bench partition_lookup -- --test
```

Benches read real Cassandra 5.0 SSTables from `test-data/datasets`. If the
binaries are missing, fetch them first:

```bash
bash test-data/scripts/fetch-datasets.sh
```

`CQLITE_DATASETS_ROOT` is honored by every bench. When unset, benches fall back
to the workspace-relative `test-data/datasets` path (derived from
`CARGO_MANIFEST_DIR`), so a checkout with fetched datasets runs with no
environment setup.

## Fixtures (Issue #537)

`fixtures/mod.rs` is the shared, deterministic fixture loader every bench draws
from. It is included per-bench via `#[path = "fixtures/mod.rs"] mod fixtures;`
and provides:

- `datasets_root()` / `table_dir(keyspace, table)` — locate the vendored
  SSTables (hash-independent), honoring `CQLITE_DATASETS_ROOT`.
- `seeded_rng()` + `BENCH_SEED` — a fixed-seed RNG so any "random" key/partition
  selection is identical on every run and machine.
- `ReadFixture` descriptors (`SIMPLE`, `CLUSTERING`, `TYPE_HEAVY`) and
  `open_read_db()` — open a queryable `Database` over one fixture table,
  **isolated in a temp dir** so a bench run never mutates the shared corpus
  (requires `--features cli-helpers`).
- `open_write_engine()` — build a `WriteEngine` against a temp dir (requires
  `--features write-support`).

No network, no Docker, no live Cassandra — the fixtures are exactly the SSTables
`fetch-datasets.sh` provides. `fixtures_smoke` is the acceptance bench: it
asserts the seeded RNG is reproducible and that a fixture scan returns a stable
row count run-to-run.

```bash
# Exercise the read + write loaders too (needs the optional features)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --features cli-helpers,write-support \
  --bench fixtures_smoke
```

## Benches

| Bench | Status | What it measures |
|-------|--------|------------------|
| `partition_lookup` | kept | Index.db partition-key lookup (`IndexReader::lookup_partition`) — cold/warm cache, throughput, access-pattern distribution. The latency-sensitive read path. |
| `m1_performance` | kept | M1 baseline targets: partition-lookup latency plus multi-SSTable read throughput (MB/s). |
| `fixtures_smoke` | added (#537) | Smoke/acceptance bench proving the fixture loaders are deterministic (seeded RNG + stable scan row count). Read/write portions activate under `cli-helpers` / `write-support`. |
| `read` | added (#538) | Read suite (needs `--features cli-helpers`): `point_lookup`, `clustering_slice`, `full_scan`, `type_heavy` over the fixtures via the public query API. |
| `write` | added (#539) | Write suite (needs `--features write-support`): `ingest_wal_on` (sustained ingest) and `flush` (memtable→SSTable flush latency) over the M5 `WriteEngine`. |

## Performance regression gate (Issue #540)

CI runs the `read` + `write` benches on **both the PR and `main`, on the same
runner**, and fails the PR if any tracked bench's Criterion **median** is more
than the threshold slower than on main. Comparing PR-vs-main on one runner means
the gate measures *relative* change only, so it is immune to the cross-machine
(and cross-OS) variance that makes committed absolute-time baselines unreliable.

- **Workflow:** `.github/workflows/perf-regression.yml` (runs on PRs touching
  `cqlite-core/**` and on manual `workflow_dispatch`).
- **Policy / baseline:** `cqlite-core/benches/perf-gate.json` — the tracked bench
  IDs and `threshold_pct` (default **10%**). This file *is* the committed,
  version-controlled baseline policy; the measured baseline is `main` itself,
  re-measured on every run.
- **Comparison:** `scripts/ci/check_perf_regression.py` reads the Criterion
  median (`estimates.json`) for each tracked bench from the `pr` and `base`
  baselines and exits non-zero on any regression past the threshold. Benches
  absent from `main` (e.g. a brand-new bench) are reported `SKIP`, never failed.

### Adjusting / refreshing the gate

There is no committed absolute-time baseline to refresh — **`main` is the living
baseline**, so merging a legitimate change automatically updates what future PRs
are compared against. To change *what* the gate enforces, edit
`cqlite-core/benches/perf-gate.json`:

- raise/lower `threshold_pct` (micro-benchmarks on shared CI runners are noisy;
  if the gate flaps, raise the threshold or increase `--sample-size` in the
  workflow's `BENCH_ARGS`);
- add/remove bench IDs from `benches`.

If a PR intentionally trades performance for another goal, justify the regression
in the PR description; a reviewer can raise the threshold or merge with the
explanation on record.

## Audit (Issue #536)

The three benches wired here dated to Aug 2025 (pre-format-maturity). Issue #536
revived them against current code and decided keep/fix/delete per bench:

- **`partition_lookup` — kept (fixed).** Compiles and runs against the current
  public API. Fixed a stale hard-coded fallback dataset path
  (`/Users/patrick/...`) to a portable `CARGO_MANIFEST_DIR`-relative path.
- **`m1_performance` — kept.** Compiles and runs unmodified; provides the
  partition-lookup and read-throughput baselines the new read suite (Issue #538)
  builds on.
- **`component_flattening` — deleted.** It benchmarked `Vec::with_capacity` vs
  `Vec::new` from the Rust standard library and never called any `cqlite-core`
  code, so it could not catch a regression in CQLite. Removed here and from
  `Cargo.toml`; the optimization it once illustrated (Issue #209) is already in
  the codebase and covered by unit tests.

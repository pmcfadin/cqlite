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

## Profiling

Every bench target has an in-process sampling CPU profiler attached
(`profiling/mod.rs`, [pprof](https://crates.io/crates/pprof) on unix). It is
inert during normal measurement runs — including the CI gate — and activates
only when criterion is invoked with `--profile-time`:

```bash
# 10 s of sampling per selected bench →
# target/criterion/<group>/<bench>/profile/flamegraph.svg
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --features cli-helpers --bench read -- --profile-time 10
```

See [docs/profiling.md](../../docs/profiling.md) for the full workflow:
flamegraphs, dhat heap profiling against the 128 MiB budget
(`examples/heap_profile.rs`), and the `scripts/profile.sh`
profile → fix → re-measure loop.

## Fixtures (Issue #537)

`fixtures/mod.rs` is the shared, deterministic fixture loader every bench draws
from. It is included per-bench via `#[path = "fixtures/mod.rs"] mod fixtures;`
and provides:

- `datasets_root()` / `table_dir(keyspace, table)` — locate the vendored
  SSTables (hash-independent), honoring `CQLITE_DATASETS_ROOT`.
- `seeded_rng()` + `BENCH_SEED` — a fixed-seed RNG so any "random" key/partition
  selection is identical on every run and machine.
- `ReadFixture` descriptors (`SIMPLE`, `SIMPLE_BTI`, `CLUSTERING`, `TYPE_HEAVY`)
  and `open_read_db()` — open a queryable `Database` over one fixture table,
  **isolated in a temp dir** so a bench run never mutates the shared corpus
  (requires `--features cli-helpers`). `SIMPLE_BTI` is optional (the `test_da`
  corpus is absent in some checkouts); guard with `fixture_present()` and
  skip-register when missing.
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
| `read` | added (#538), point-read reworked (#1562) | Read suite (needs `--features cli-helpers`): `get_partition_big`, `get_partition_bti`, `clustering_slice`, `full_scan`, `type_heavy` over the fixtures via the public query API. `get_partition_*` are **real** partition-targeted point reads (`WHERE id = <unquoted-uuid>`, #949/#956), asserted at setup to report a targeted `AccessPath` (not the old `SELECT * … LIMIT 1` scan proxy). `_bti` skip-registers when the optional `test_da` corpus is absent. |
| `write` | added (#539, #574) | Write suite (needs `--features write-support`): `ingest_wal_on`, `ingest_wal_off`, and `flush` — see below. |
| `observability_overhead` | added (#1043) | Zero-overhead-when-disabled gate: `read_scan` (needs `cli-helpers`) and `write_merge` (needs `write-support`). The SAME bench source runs under the default build vs `--features observability` with export disabled; the two arms are compared by `scripts/ci/observability_overhead.sh` — see below. |

### `observability_overhead` two-build comparison (Issue #1043)

`benches/observability_overhead.rs` runs an identical read/scan (and write/merge)
workload that carries the production `#[tracing::instrument]` spans and catalog
metric calls. It proves the observability contract: when the feature is OFF — or
ON but export disabled (`init` never called) — instrumentation costs effectively
nothing.

A single `cargo bench` process compiles one feature set, so the two builds run as
two invocations of the **same** bench, compared on the same runner (immune to
cross-machine variance, like the perf-regression gate):

```bash
# Runs both arms and fails if export-disabled overhead exceeds the threshold.
OVERHEAD_THRESHOLD_PCT=2.0 \
  CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  scripts/ci/observability_overhead.sh
```

- **Arm 1 (default):** `--features cli-helpers,write-support` → baseline `obs_default_off`.
- **Arm 2 (export disabled):** `--features cli-helpers,write-support,observability`
  → baseline `obs_export_disabled`. OTel is linked but no provider/exporter exists.
- **Threshold:** `OVERHEAD_THRESHOLD_PCT` (default **2%**), documented as
  `OVERHEAD_THRESHOLD_PCT` in the bench source. The comparison reads each arm's
  Criterion median from `target/criterion/<id>/<baseline>/estimates.json`.

CI wiring lives in `.github/workflows/observability-gate.yml`, which also runs the
`cargo-tree` no-OTel guard (`scripts/ci/observability_no_otel_default.sh`) and the
in-memory-OTLP correctness/sampling tests.

### `write` bench breakdown

| Bench name | Gate policy | What it measures |
|------------|-------------|------------------|
| `write/ingest_wal_off` | **Strictly gated** — strict pass/fail | 256-row ingest with `Durability::Disabled`: WAL append and fsync are skipped. Pure CPU + memtable cost. Stable enough for reliable regression detection. |
| `write/ingest_wal_on` | **Advisory** — reported, never fails CI | Identical 256-row ingest with `Durability::SyncEachWrite` (default): every row calls `wal.append()` + `wal.sync()` (fsync). I/O-dominated; fsync latency on shared CI runners makes this too noisy for strict gating, but it documents durability cost. |
| `write/flush` | **Strictly gated** — strict pass/fail | Pre-filled memtable flushed once per iteration. Throughput reported in MB/s. |

## Performance regression gate (Issues #540, #572)

CI runs the `read` + `write` benches on **both the PR and `main`, on the same
runner**, and fails the PR if any **strictly gated** bench's Criterion **median**
is more than its configured threshold slower than on main. Comparing PR-vs-main
on one runner means the gate measures *relative* change only, so it is immune to
the cross-machine (and cross-OS) variance that makes committed absolute-time
baselines unreliable.

- **Workflow:** `.github/workflows/perf-regression.yml` (triggers on PRs touching
  `cqlite-core/**`, `Cargo.{toml,lock}`, the workflow itself, the gate script,
  and `perf-gate.json`). Docs-only, examples-only, and non-perf `.github/**`
  changes do **not** trigger the gate — they cannot affect benchmark results.
- **Policy:** `cqlite-core/benches/perf-gate.json` — the tracked bench IDs,
  per-bench `threshold_pct`, and the `advisory_benches` list. This file *is* the
  committed, version-controlled baseline policy; the measured baseline is `main`
  itself, re-measured on every run.
- **Comparison:** `scripts/ci/check_perf_regression.py` reads the Criterion
  median (`estimates.json`) for each tracked bench from the `pr` and `base`
  baselines. Benches absent from `main` (a brand-new bench) are reported `SKIP`,
  never failed.

### Strict vs advisory benches

The gate distinguishes two classes of benches (configured via `perf-gate.json`):

| Class | Behavior | When to use |
|-------|----------|-------------|
| **Strict** | Non-zero exit if delta > `threshold_pct`. Blocks merging. | CPU-bound, stable timings: `read/*`, `write/ingest_wal_off`, `write/flush`. |
| **Advisory** | Delta reported in CI output, but **never causes a non-zero exit**, regardless of size. | I/O-dominated by fsync: `write/ingest_wal_on`. Variance on shared runners exceeds any useful threshold. |

To mark a bench advisory, add its ID to the `advisory_benches` list in
`perf-gate.json`. The per-bench `threshold_pct` for advisory benches still
controls what is highlighted in the output (the "elevated delta" warning level);
it never triggers a failure.

### Path-ignore behavior (Issue #572)

The gate workflow uses a `paths` allowlist. It activates only when a PR changes
files that can plausibly affect benchmark results:

- `cqlite-core/**` — library source and bench source
- `Cargo.toml` / `Cargo.lock` — dependency changes
- `.github/workflows/perf-regression.yml` — the gate workflow itself
- `scripts/ci/check_perf_regression.py` — the comparison script
- `cqlite-core/benches/perf-gate.json` — gate policy

PRs that touch **only** `docs/**`, `**/*.md`, `examples/**`, or other
`.github/workflows/` files are silently skipped by GitHub Actions' path filter
and will not trigger the gate, preventing false-positive failure alerts from
fsync noise on docs-only changes.

### Tolerance model

Each bench entry in `perf-gate.json` has its own `threshold_pct`. The default is
`10%` for CPU-bound benches and `25%` for the advisory `write/ingest_wal_on`
(which only affects the "highlight" level in output, not pass/fail). Raise a
bench's threshold if the gate flaps on a stable CI environment, or increase
`--sample-size` in the workflow's `BENCH_ARGS` for stabler medians.

### Adjusting / refreshing the gate

There is no committed absolute-time baseline to refresh — **`main` is the living
baseline**, so merging a legitimate change automatically updates what future PRs
are compared against. To change *what* the gate enforces, edit
`cqlite-core/benches/perf-gate.json`:

- raise/lower `threshold_pct` per bench;
- add/remove bench IDs from `benches`;
- move a bench between strict and advisory by adding/removing its ID from
  `advisory_benches`.

If a PR intentionally trades performance for another goal, justify the regression
in the PR description; a reviewer can raise the threshold or merge with the
explanation on record.

### Tests

`scripts/ci/tests/test_check_perf_regression.py` validates the strict-vs-advisory
logic against fixture Criterion estimate directories. Run with:

```bash
# From repo root — uses the venv pytest
bindings/python/.venv/bin/pytest scripts/ci/tests/test_check_perf_regression.py -v
```

The tests prove: (a) a CPU-bench regression above threshold exits non-zero;
(b) a large `write/ingest_wal_on` swing exits zero (advisory reported only).

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

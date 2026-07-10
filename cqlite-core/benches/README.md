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
| `read` | added (#538), point-read reworked (#1562), chunk cache (#1567) | Read suite (needs `--features cli-helpers`): `get_partition_big`, `get_partition_bti`, `point_lookup_repeated`, `clustering_slice`, `full_scan`, `type_heavy` over the fixtures via the public query API. `get_partition_*` are **real** partition-targeted point reads (`WHERE id = <unquoted-uuid>`, #949/#956), asserted at setup to report a targeted `AccessPath` (not the old `SELECT * … LIMIT 1` scan proxy). `point_lookup_repeated` (#1567) measures the steady-state **cached** repeat point read — Criterion warms up, so the target chunk is decompressed once and served from the shared decompressed-chunk cache thereafter (`Arc::clone`, no re-read/re-decompress). `_bti` / `point_lookup_repeated` skip-register when the optional `test_da` corpus is absent. |
| `write` | added (#539, #574) | Write suite (needs `--features write-support`): `ingest_wal_on`, `ingest_wal_off`, and `flush` — see below. |
| `compaction` | added (#1646) | Compaction / k-way-merge suite (needs `--features write-support`): `narrow`, `wide`, `tombstone_heavy` — full multi-generation STCS compaction over flushed L0 SSTables — see below. |
| `observability_overhead` | added (#1043) | Zero-overhead-when-disabled gate: `read_scan` (needs `cli-helpers`) and `write_merge` (needs `write-support`). The SAME bench source runs under the default build vs `--features observability` with export disabled; the two arms are compared by `scripts/ci/observability_overhead.sh` — see below. |
| `concurrent_scan` | added (#917), **gated** (#1564) | Aggregate throughput of N ∈ {1,2,4,8} concurrent `get_all_entries()` scans against one shared `Arc<SSTableReader>`, for the buffered and mmap backends (needs `--features cli-helpers`). Gated via a **concurrency scaling floor** (not absolute time) — see below. |
| `read_while_write` | added (#1143), **gated** (#1564) | Reader-side scan latency with ~6 full-scan readers running concurrently with ~2 sustained-ingest writers (needs `--features cli-helpers,write-support`). Gated on the Criterion **median** (strict, 25% threshold); the p99 tail is printed to stderr for local diagnosis and owned by the A2 tail-latency harness (#1563). |
| `tail_latency` | added (#1563) | `harness = false` mixed-load tail harness (needs `--features cli-helpers`): p50/p99/p999 + intra-run ratios for point reads under a background scan — see below. Appends per-metric rows to the unified ledger. |
| `open` | added (#1566) | `harness = false` cold-open + memory bench (needs `--features cli-helpers`): `open/cold_big` and `open/cold_bti` (fresh `SSTableReader::open` component-load cost; `_bti` skip-registers when `test_da` is absent) and `mem/open_n_readers` (per-reader RSS gauge). Skip-on-absent, panic-on-present-but-broken; appends its medians + the memory metric to the unified ledger — see below. |

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

### `compaction` bench breakdown (Issue #1646, Epic O finding O1)

Each iteration flushes `min_threshold` (4) L0 SSTables in the **untimed** setup,
installs an `STCSPolicy` explicitly via `set_merge_policy` (so O1 measures
compaction regardless of whether the default-on STCS wiring, N1, has landed),
then drives `WriteEngine::maintenance_step` to completion in the **timed**
routine. Throughput is reported as compacted rows/second (`Throughput::Elements`).

| Bench name | Gate policy | What it measures |
|------------|-------------|------------------|
| `compaction/narrow` | **Strictly gated** — strict pass/fail | Many small single-row partitions (`UUID` PK, no clustering) across the L0 SSTables. The CPU-bound merge-core probe; stable enough for strict regression detection. |
| `compaction/wide` | **Advisory** — reported, never fails CI | A few fat partitions, each SSTable contributing a disjoint clustering slice so the merged partition is the union of all of them. Memory/data-shaped by design — **O2's dhat budget is its guard, not this wall clock** — so it is advisory. |
| `compaction/tombstone_heavy` | **Strictly gated** — strict pass/fail | Live rows shadowed by row/range/cell tombstones in a later generation, exercising the reconcile + range-shadowing path. CPU-bound; strictly gated. |

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
| **Strict** | Non-zero exit if delta > `threshold_pct`. Blocks merging. | CPU-bound, stable timings: `read/*`, `write/ingest_wal_off`, `write/flush`, `compaction/narrow`, `compaction/tombstone_heavy`. |
| **Advisory** | Delta reported in CI output, but **never causes a non-zero exit**, regardless of size. | I/O-dominated by fsync (`write/ingest_wal_on`) or memory/data-shaped (`compaction/wide`, whose dhat budget is owned by O2, not this wall clock). Variance on shared runners exceeds any useful threshold. |

To mark a bench advisory, add its ID to the `advisory_benches` list in
`perf-gate.json`. The per-bench `threshold_pct` for advisory benches still
controls what is highlighted in the output (the "elevated delta" warning level);
it never triggers a failure.

### Export / Flight perf net (Issue #1494, AD5)

The export/Flight lane (epic #1469) is measured by a **tiered** suite plus
**dhat budget guards**. This is the measurement net the AB/AE optimization
children (AB1/AB3/AB7 Flight memory + streaming, AE1–AE5 per-cell conversion)
assert their wins against — "benches FIRST, baseline before wins."

**Tier 1 — STRICT conversion + export micro-benches** (`export_throughput` in
`cqlite-core/benches`, ids `export/*`): CPU-bound and stable, so they gate as
same-runner PR-vs-`main` median ratios like the read/write benches.

**Tier 2 — ADVISORY end-to-end Flight `do_get`** (`flight_do_get` in
`cqlite-flight/benches`, id `flight/do_get`): drives the **public tonic
`FlightService::do_get` RPC over a real loopback transport** (wiring evidence for
the RPC path). Its wall time is Tokio-runtime + tonic-transport + I/O dominated
(the `write/ingest_wal_on` precedent), so it is **reported but never fails CI**.

**Hard signal — dhat budget guards** (run in the mandatory `agent-gate.sh`
`memory-budget` component, not the CI perf lane): allocation **counts/bytes** are
machine-independent, so they are the load-deterministic per-gate signal for this
path. The converter guard
(`cqlite-core/tests/issue_1494_converter_alloc_budget.rs`) pins per-row CQL→Arrow
allocations; the producer guard
(`cqlite-flight/tests/issue_1494_producer_mem_budget.rs`) pins the Flight
producer's total + peak bytes. Both are **non-vacuous** (fail on 0 rows / 0
allocations) and land **passing** as baseline locks; AB/AE own tightening them.

#### Current-`main` baseline (post-#1495)

> **Provenance:** these figures already include the merged **#1495** (PR #2312)
> arrow-convert accessor-once win — they are the **post-#1495 `main` floor**, the
> reference #1496 and the AB/AE children measure ratios against, **not** a
> pre-optimization number. The wall-clock rows below are **local reference
> figures** (macOS, `--sample-size 20`), recorded for orientation only — the gate
> compares same-runner ratios and commits **no** absolute-time baseline. The dhat
> figures ARE machine-independent (deterministic allocation counts/bytes).

| Bench | Class | Fixture | Median (local ref) | Throughput |
|-------|-------|---------|--------------------|------------|
| `export/rows_to_record_batch` | STRICT | `test_collections.collection_table` (500 rows × 7 cols) | 0.51 ms | 0.98 Melem/s |
| `export/json` | STRICT | same | 4.32 ms | 0.12 Melem/s |
| `export/parquet` | STRICT | same | 2.52 ms | 0.20 Melem/s |
| `export/delta` | STRICT | 5,000 synthetic upserts | 2.98 ms | 1.68 Melem/s |
| `flight/do_get` | ADVISORY | 2,000-row `keyvalue` (self-contained flush) | 6.15 ms | 0.33 Melem/s |

| dhat budget guard | Measured (post-#1495) | Committed ceiling |
|-------------------|-----------------------|-------------------|
| converter allocs/row (`issue_1494_converter_alloc_budget`) | 0.91 allocs/row (453 / 500 rows) | 3.0 allocs/row |
| producer total bytes (`issue_1494_producer_mem_budget`) | 20,908,845 B (~20.9 MB, 2,000 rows) | 32 MiB |
| producer peak bytes | 3,149,012 B (~3.1 MB) | 8 MiB |

Reproduce (from the repo root, datasets fetched):

```bash
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --features cli-helpers,write-support,parquet,delta-scan \
  --bench export_throughput
cargo bench -p cqlite-flight --bench flight_do_get
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test -p cqlite-core --features cli-helpers,dhat-heap,arrow \
  --test issue_1494_converter_alloc_budget -- --test-threads=1 --nocapture
cargo test -p cqlite-flight --features dhat-heap \
  --test issue_1494_producer_mem_budget -- --test-threads=1 --nocapture
```

**Refresh procedure (drift-free):** the wall-clock `base` is re-measured on
`main` every CI run — there is no committed absolute-time number to drift. To
retune, edit `perf-gate.json` (thresholds / advisory list) **and** update the
numbers in this table in the same PR. To ratchet a dhat ceiling down (an AB/AE
child), edit the `const` in the guard test and update its row here.

### Concurrency scaling floor (Issue #1564)

`concurrent_scan` is gated by a **machine-independent scaling floor**, not an
absolute time, because its absolute curve is runner-noisy but its *intra-run
ratio* is not. The gate policy carries this in a `scaling_floors` array in
`perf-gate.json` (evaluated by `check_perf_regression.py` after the median loop):

```jsonc
"scaling_floors": [
  { "id": "concurrent_scan/buffered/n4", "baseline_id": "concurrent_scan/buffered/n1",
    "degree_ratio": 4, "min_scaling": 1.8 },
  { "id": "concurrent_scan/mmap/n4",     "baseline_id": "concurrent_scan/mmap/n1",
    "degree_ratio": 4, "min_scaling": 1.8 }
]
```

Each entry checks, **on the PR (`pr`) baseline alone** (no `main` comparison):

```
scaling = degree_ratio · median(n1) / median(n4)
```

Both medians come from the same run on the same runner, so the machine's absolute
speed cancels — this is exactly the property absolute-time gating lacks.

- **Healthy parallel scans:** `median(n4) ≈ median(n1)` → `scaling ≈ 4`
  (measured ≈ **2.98** buffered, ≈ **3.19** mmap).
- **Re-serialized read path** (e.g. a reintroduced shared `Mutex`, exactly what
  #815 removed): `median(n4) ≈ 4·median(n1)` → `scaling ≈ 1.0`.

The **floor is `1.8`**: below observed healthy (≈2.98/3.19) with ~40% headroom for
CI-runner variance (fewer cores, shared scheduler; ubuntu-latest is ~4 vCPU),
while still failing decisively against the ≈1.0 serialization signature. `n2`/`n8`
are still measured for the local scaling curve but are not floored. Because a
scaling floor is **intra-run** (its data is always present on any run that benches
`concurrent_scan`), a configured floor whose `n1`/`n4` median is **missing** is a
gate **failure** (`MISSING DATA`), not a silent `SKIP` — so a typo'd id, an omitted
`--bench`, or a no-data bench cannot quietly disable the gate. A floor may opt into
skip-on-absent with `"optional": true` (for a genuinely optional fixture, like the
`test_da` BTI corpus convention).

`read_while_write` is gated as a standard **strict median** entry
(`read_while_write/readers6_writers2`, 25% threshold) — the reader-side aggregate
latency under write load, which is machine-readable and stable (tracks p50 ≈ 5 ms).
The bench uses a writer-readiness barrier so the readers' timed scans begin only
once every writer is actively ingesting, guaranteeing the median reflects latency
under live write contention (not an uncontended window). Its p99 tail is not gated
here (it is stderr-only and runner-noisy); the tail is owned by the A2 tail-latency
harness (#1563).

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

## Tail-latency harness (Issue #1563)

The perf-regression gate above compares Criterion **medians**. But the July 2026
read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic A)
found the three biggest read-path defects — C2 cursor convoy, F1 reader-map FIFO
stall, F3 blocking I/O on async workers — are all **tail** pathologies: they
barely move the median but inflate p99/p999 under a mixed load (a background scan
running while point reads arrive). A median gate is structurally blind to them.

`benches/tail_latency.rs` is a `harness = false` custom-main bench (not Criterion,
which reports only a median) whose measurement core lives in the shared
`benches/tail_latency/mod.rs` module (also exercised by
`cqlite-core/tests/tail_latency_harness.rs`). Over one shared `Database` on the
BIG `test_basic.simple_table` fixture it:

1. runs a fixed stream of real partition-targeted point reads
   (`SELECT id, name … WHERE id = <uuid-literal>`, the #949/#956 path) with **no**
   background scan — the *scan-free baseline*; then
2. runs the identical stream while **one continuous background full-table scan**
   (`SELECT *` looped on its own thread) hammers the same reader set — the
   *mixed* load.

Setup asserts the point read returns ≥1 row and reports a **targeted**
`AccessPath` (`PartitionLookup`) or panics — the same honesty guard as the `read`
benches. Sample counts are fixed consts (`WARMUP` + `MEASURED_N`), never
wall-clock-bounded.

### Running

```bash
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --features cli-helpers --bench tail_latency
```

Under default features (no `cli-helpers`) it prints a note and exits 0 without
measuring. When the fixture binary is absent it skips (no measurement).

### JSON output

It prints a machine-readable report to stdout:

```json
{
  "mixed":     { "p50": <ns>, "p99": <ns>, "p999": <ns> },
  "scan_free": { "p50": <ns>, "p99": <ns>, "p999": <ns> },
  "p99_over_p50": <mixed.p99 / mixed.p50>,
  "p99_mixed_over_scan_free": <mixed.p99 / scan_free.p99>
}
```

`p99_over_p50` is the tail spread within the mixed load; `p99_mixed_over_scan_free`
is the convoy inflation — the headline number the C2/F1/F3 fixes must drive down.
All gate thresholds are these intra-run **ratios**, never wall-clock absolutes, so
shared-runner noise cannot flap the gate.

### Unified history ledger (Issue #1566, Epic A / A5)

All harness benches persist their metrics to **one** append-only ledger,
`target/profiling/history.jsonl`, written through the shared Rust module
`benches/bench_ledger`. Every line is one JSON object **per metric** in the schema:

```json
{"ts": 1783103681, "commit": "<full-sha|unknown>", "bench": "tail_latency", "metric": "p99_over_p50", "value": 2.31, "unit": "ratio"}
```

- The `tail_latency` bench writes `mixed_p50/p99/p999` + `scan_free_p50/p99/p999`
  (`ns`) and the two derived ratios (`ratio`), one line each.
- The `open` bench writes `cold_big_median_ns` / `cold_bti_median_ns` (`ns`) and the
  per-reader memory gauges `rss_after_n_readers_bytes` / `rss_per_reader_bytes`
  (`bytes`).
- `scripts/profile_report.py` writes each criterion bench's `median_ns` (`ns`) and
  `peak_heap_bytes` (`bytes`) in the same schema.

`./scripts/profile.sh report` reads the whole ledger back into a longitudinal
per-metric table (latest value + delta vs the previous distinct commit). The ledger
is generated run data (machine/run-specific): it lives under `target/` (gitignored)
and is **never committed**; CI may upload it as an artifact. Override the path for a
bench with the `CQLITE_BENCH_LEDGER` env var (else
`<crate>/../target/profiling/history.jsonl`). Append is best-effort — a ledger write
failure logs to stderr and never fails a measurement run.

This replaced the A2 bespoke `benches/tail-latency-history.jsonl` (retired when A5
landed). See `docs/profiling.md` for the full ledger contract.

### Advisory-first tail gate

`benches/tail-latency-gate.json` holds per-ratio `max` thresholds plus an
`advisory` flag; `scripts/ci/check_tail_latency.py <harness_json> <gate_json>`
reports each ratio against its threshold:

- **Advisory** (`advisory: true`, the default): breaches are reported with an
  advisory status but the checker **always exits 0**. This records today's convoy
  so the C2/F1/F3 fixes can be shown red-then-green without redding the gate now.
- **Enforcing** (`advisory: false`, or pass `--enforce`): any ratio over its `max`
  exits non-zero.

**Flip to enforcing:** once C2/F1/F3 land and `p99_mixed_over_scan_free` drops,
tighten the `max` values in `tail-latency-gate.json` to the new floor and set
`advisory: false` (or pass `--enforce` in CI).

The checker logic is proven by `scripts/ci/tests/test_check_tail_latency.py`
(advisory breach → exit 0; enforce breach → exit 1; within-threshold → exit 0):

```bash
bindings/python/.venv/bin/pytest scripts/ci/tests/test_check_tail_latency.py -v
```

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

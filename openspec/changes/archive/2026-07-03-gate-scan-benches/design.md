# Design — gate-scan-benches

## Context

The perf gate (`scripts/ci/check_perf_regression.py`, policy `cqlite-core/benches/perf-gate.json`, driver
`.github/workflows/perf-regression.yml`) compares each tracked bench's Criterion **median** between a PR
baseline (`pr`) and a main baseline (`base`) measured **on the same runner**, and fails if a strict
bench is more than `threshold_pct` slower on the PR. This "relative on one runner" model is immune to
cross-machine variance and is why there are no committed absolute-time baselines.

`concurrent_scan` and `read_while_write` were deliberately kept out of this gate because their *absolute*
timings are runner-noisy. But that leaves concurrency-scaling and read-under-write regressions ungated.

## Key decision — gate an intra-run ratio, not an absolute time

The dominant `concurrent_scan` regression is **re-serialization** (a shared `Mutex` on the read path),
which #815 explicitly removed. Its signature is unambiguous in a *ratio within a single run*:

```
scaling(n) = throughput(n) / throughput(1)
           = (elements(n)/median(n)) / (elements(1)/median(1))
           = (degree · rows_per_scan / median(n)) / (rows_per_scan / median(1))
           = degree · median(1) / median(n)
```

- Healthy parallel scans: `median(n4) ≈ median(n1)` → `scaling ≈ 4` (observed: 2.98 buffered, 3.19 mmap).
- Fully serialized (mutex): `median(n4) ≈ 4·median(n1)` → `scaling ≈ 1.0`.

Because both medians come from the **same baseline on the same runner**, the machine's absolute speed
cancels. This is exactly the property the bench doc said absolute-time gating lacked, so the "not a CI
gate" objection does not apply to a scaling floor. The floor is evaluated on the PR (`new`) baseline
alone — it needs no `main` baseline.

### Floor value: 1.8

Observed healthy scaling is ≈2.98 (buffered) / ≈3.19 (mmap); serialized collapses to ≈1.0. `1.8` sits
below healthy with ~40% headroom for CI-runner variance (fewer cores, shared scheduler; ubuntu-latest is
~4 vCPU) while still failing decisively against the ≈1.0 serialization signature. The floor and this
derivation are recorded in `perf-gate.json` (`_note`) and README so a future editor understands the
margin. The issue's specified ratio is `n4/n1`; `n2`/`n8` remain measured (for the local scaling curve)
but are not floored.

## perf-gate.json schema extension (additive)

A new optional top-level array `scaling_floors`, evaluated independently of `benches`:

```jsonc
"scaling_floors": [
  {
    "id": "concurrent_scan/buffered/n4",       // the scaled bench (numerator degree)
    "baseline_id": "concurrent_scan/buffered/n1", // the n=1 baseline
    "degree_ratio": 4,                          // elements(id)/elements(baseline_id)
    "min_scaling": 1.8,
    "_note": "throughput(n4)/throughput(n1)=4·median(n1)/median(n4); healthy≈2.98, serialized≈1.0"
  },
  { "id": "concurrent_scan/mmap/n4", "baseline_id": "concurrent_scan/mmap/n1",
    "degree_ratio": 4, "min_scaling": 1.8, "_note": "healthy≈3.19, serialized≈1.0" }
]
```

Legacy configs without `scaling_floors` behave exactly as before (`cfg.get("scaling_floors", [])`).

## check_perf_regression.py extension (additive)

After the existing median-regression loop, evaluate each `scaling_floors` entry against the **`new`**
baseline:

- `m_base = _median_ns(dir, baseline_id, new)`, `m_scaled = _median_ns(dir, id, new)`.
- If either is `None` → **SKIP** (report, never fail) — mirrors the missing-data rule so an absent
  optional fixture (or a bench not yet on this branch) cannot fail the gate.
- Else `scaling = degree_ratio · m_base / m_scaled`. If `scaling < min_scaling` → **FAIL** (append to
  failures → non-zero exit). Else `ok`.
- Print each in a clearly-labeled section (`baseline (ns)`, `scaled (ns)`, `scaling`, `floor`, status).
- The "nothing was compared" guard is widened: it fails only when **no** median bench AND **no** scaling
  floor could be evaluated, so a fixture that provides only scaling data still passes/fails correctly.

No `unwrap`-equivalent silent paths; missing keys use `.get` with documented defaults. Pure Python,
stdlib only (matches the existing script).

## read_while_write bit-rot fix (bench code only)

The writer loop `while !stop.load(...) { ingest }` lets a late-scheduled `spawn_blocking` writer observe
`stop == true` on its first check and return `written = 0`; if both writers do, `total_written == 0`
panics the correctness floor. Fix: make the loop do-while — **ingest once, then check `stop`** — so
`total_written ≥ WRITERS` always. Measurement semantics are unchanged: writers still sustain-ingest for
the whole reader window in the common case; the fix only removes the zero-ingest race. No library code
changes.

## read_while_write gated metric

Criterion's median for `read_while_write/readers6_writers2` is the per-iteration aggregate reader latency
under write load — machine-readable and stable (tracks p50 ≈ 5 ms). Gated as a standard strict median
entry with a **wide 25% threshold** (contention bench) and a note that p99 tails belong to A2 (#1563).
The bench keeps printing p50/p99 to stderr for local diagnosis.

## Workflow wiring

`.github/workflows/perf-regression.yml` currently runs `--bench read --bench write`. Add `--bench
concurrent_scan --bench read_while_write` to **both** the PR and main invocations (features already
include `cli-helpers,write-support`, which both benches need). concurrent_scan on `main` is unused by the
scaling floor but is harmless and keeps the two invocations symmetric.

## Alternatives considered

- **Gate concurrent_scan absolute n4 time pr-vs-main** — rejected: reintroduces exactly the runner-noise
  flakiness the bench doc (and the project's flaky-perf-gate history) warned against; a scaling ratio is
  machine-independent.
- **Gate read_while_write p99** — rejected: p99 is not a machine-readable Criterion metric here (stderr
  only) and is runner-noisy; A2's harness (#1563) owns tails. Median is the honest, stable choice.
- **Signal writer-readiness via a barrier instead of do-while** — rejected as heavier; do-while is the
  minimal change that guarantees the correctness floor without altering measurement.

## Test strategy (TDD)

Extend `scripts/ci/tests/test_check_perf_regression.py` with fixture Criterion trees:
1. **Scaling PASS**: `concurrent_scan/*/n1` & `/n4` medians giving scaling ≈3.0 → exit 0.
2. **Scaling FAIL (red-run proof)**: `median(n4) ≈ 4·median(n1)` → scaling ≈1.0 < 1.8 → non-zero exit,
   output names the floored bench. This is the committed, deterministic serialization-regression proof.
3. **Scaling SKIP**: missing `n4` data → reported SKIP, never fails.
Plus a real scratch-branch red-run (wrap the scan hot path in a `Mutex`, re-measure, show the gate reds)
pasted in the PR for narrative.

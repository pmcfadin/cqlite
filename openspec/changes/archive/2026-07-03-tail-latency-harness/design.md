# Design — tail-latency-harness

## Context

Source of truth: `docs/reports/read-path-performance-audit-2026-07-01.md` §Epic A (row A2); issue #1563
(child of Epic A #1513). Builds on A1 (#1562, merged): the real point-read surface, the
`benches/fixtures/mod.rs` loader, and the `perf-gate.json` + `scripts/ci/check_perf_regression.py`
conventions. Guardrail: **no read-path production code changes** — additive harness/gate/test only.

## Decision 1 — harness form: `harness = false` custom-main bench, not a Criterion bench

Criterion reports a single **median** per bench and the A1 gate compares that median PR-vs-`main`. The
tail harness must report **percentiles** (p50/p99/p999) and gate **intra-run ratios**, which Criterion's
model does not express. Options considered:

| Form | Verdict |
|------|---------|
| Criterion bench (`harness = true`) | **Rejected.** Emits a median, not percentiles; the gate mechanism is a PR-vs-main median compare, not an intra-run ratio. |
| Plain `scripts/` binary invoking the CLI | **Rejected.** Would drive the process boundary, not the in-process reader set the audit convoy is about; can't share one `Database`/reader set between the scan and the point reads. |
| `harness = false` bench under `benches/` with a custom `fn main()` | **Chosen.** Compiles against `cqlite-core` + the shared `fixtures` loader, drives one in-process shared `Database`, and is free to compute/emit percentiles + ratios as JSON and append the ledger. Lives with the other benches. |

**Chosen: a `harness = false` bench** `cqlite-core/benches/tail_latency.rs` whose measurement logic
lives in a shared module `cqlite-core/benches/tail_latency/mod.rs` so the TDD test can include and
exercise it (see Decision 5).

## Decision 2 — load model: background scan + foreground point-read stream, vs a scan-free baseline

Per the issue step 1: start **one continuous background full-table scan** (`SELECT *` looped over the
shared `Database`), then issue a fixed-length stream of foreground **point reads** against the *same*
reader set, timing each point read. This is exactly the C2/F1/F3 tail pathology (a scan cursor + reader
map held while point reads arrive). The **scan-free baseline** runs the identical point-read stream with
no background scan. The gate compares the two.

- **Point-read surface** = A1's proven real path: `SELECT id, name FROM test_basic.simple_table WHERE
  id = <uuid-literal>` (projected → routes through `SelectExecutor`/#949, not the legacy scan). Setup
  asserts `rows.len() ≥ 1` and `access_path.is_targeted()` (`PartitionLookup`) or panics — wiring-evidence.
- **Sharing** = `Arc<Database>` (the read handle is `&self`/`Send + Sync`; concurrent execute is safe,
  per #815). The background scan runs on a spawned thread (its own current-thread Tokio runtime) driven
  by an `AtomicBool` stop flag; the foreground stream runs on the main thread. The scan thread is
  started before and joined after the point-read stream so the tail is measured under live contention.
- **Fixed iteration counts** (documented consts: warmup + measured N point reads, and the scan loops
  until stopped) so the measured set is identical run-to-run; no wall-clock-bounded loops.

## Decision 3 — gate: a self-contained intra-run ratio checker, ADVISORY first

The A1 gate compares medians PR-vs-main on one runner. Tail ratios are computed **within a single run**,
so a separate checker is cleaner than overloading `check_perf_regression.py`:

- `cqlite-core/benches/tail-latency-gate.json` — policy: `advisory: true`, and per-ratio thresholds
  `p99_over_p50` and `p99_mixed_over_scan_free` (`max`). Mirrors `perf-gate.json`'s "policy file is the
  committed baseline" model.
- `scripts/ci/check_tail_latency.py <harness_json> <gate_json>` — reads the harness JSON, prints each
  ratio vs its threshold with a status, and **while `advisory` is true exits 0 regardless of any
  breach** (reports only). A `--enforce` flag (or setting `advisory: false`) makes a breach exit
  non-zero. `scripts/ci/tests/test_check_tail_latency.py` proves: advisory breach → exit 0 (reported);
  enforce breach → exit 1; within-threshold → exit 0.

**Flip-to-enforcing** (documented in the gate JSON `_comment`, the bench README, and the checker
`--help`): once C2/F1/F3 land and the `p99_mixed_over_scan_free` ratio drops, tighten the thresholds to
the new floor and set `advisory: false`.

## Decision 4 — persisted ledger, pending A5 consolidation

Each harness run appends one JSON record — `{ts, commit, mixed:{p50,p99,p999},
scan_free:{p50,p99,p999}, ratios:{...}}` — to `cqlite-core/benches/tail-latency-history.jsonl`. This is
**generated run data** (machine/run-specific), so it is **gitignored**, not committed. A5 (cold-open
bench + persisted ledger) will introduce the unified `history.jsonl`; the harness ledger path is a
single const documented to fold into A5's ledger then.

## Decision 5 — TDD tests robust to shared-runner noise (ratios, wide tolerances)

`cqlite-core/tests/tail_latency_harness.rs` includes the shared harness module and the fixtures loader
via `#[path]` (both resolve to `crate::…`, the same pattern the benches use). Under the gate's
`core-tests` (`--features cli-helpers`) component:

- **Self-assertion (mixed vs baseline):** `p99_mixed ≤ k × p99_scan_free`, where `k` is chosen from the
  first measured run on `main` (the convoy inflates the ratio) and documented as a const with the
  measured "before" number. `k` is set generously above the observed convoy ratio so the test is green
  on today's `main`; it tightens when the tail fixes land. Never asserts absolute times.
- **Determinism (smoke):** two consecutive scan-free runs agree within a wide, documented tolerance
  (structural invariants always: `p50 ≤ p99 ≤ p999`, counts match, JSON round-trips). Deliberately loose
  — the issue guardrail forbids gating on wall-clock noise.
- **Pure unit tests** (no datasets): percentile computation (nearest-rank on a known vector) and the
  gate-ratio/advisory logic.
- **Skip-not-fail** when the fixture binary is absent (reuse `fixtures::fixture_present`), and
  **fail-loud** (panic) when the fixture is present but yields 0 rows or a non-targeted path — never a
  0-row measurement (parity-is-truth doctrine).

## Alternatives rejected

- **Gating absolute p99 latency** — rejected; cross-runner variance makes any committed absolute time
  unreliable (the same rationale A1/#572 give). Ratios only.
- **Reusing Criterion's sample vector for percentiles** — rejected; Criterion owns its sampling/warmup
  loop and reports a median, not the raw per-op latencies under a *concurrent background scan*, which is
  the whole point.

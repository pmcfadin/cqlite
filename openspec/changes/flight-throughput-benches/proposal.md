## Why

The export/Flight read path has **no** performance instrumentation of any kind: no throughput
bench, no allocation-budget coverage for the producer/conversion hot paths, and no perf-gate entry.
`cqlite-core/benches/perf-gate.json` tracks the read/write/decode/compaction paths but has zero
export or Flight entries, and the only export-adjacent bench in the tree is
`observability_overhead.rs` (which measures span cost, not conversion throughput). This is finding
**AD5** of the July 2026 read-path performance audit and part of epic **#1469** (parity + perf net).

This is the "measurement first" issue for the export/Flight lane: **benches FIRST — baseline before
wins**. Its deliverable is the measurement net that the AB/AE optimization children assert against —
AB1 (Flight memory bound), AB3 (one-shot streaming), AB7 (batch row-group), and AE1–AE5 (per-cell
conversion work). Until the net exists, no export/Flight optimization claim is trustworthy, because a
regression on the producer or the CQL→Arrow converter merges silently.

Constraints from the current tree that shape the design:

- **The perf gate is already load-robust and SKIP-aware.** `.github/workflows/perf-regression.yml`
  measures each tracked Criterion bench on **both** the PR merge-ref **and** `main` on the **same
  runner**, and `scripts/ci/check_perf_regression.py` fails only on a per-bench *median-regression
  ratio* past `threshold_pct`. A bench absent from either baseline is reported **SKIP** (first-landing
  green). This design — a same-runner ratio, never a committed absolute number — is exactly the
  load-tolerant, non-flaky mechanism #1494 must reuse rather than reinvent. It also carries an
  **advisory** class (`advisory_benches`) for I/O/runtime-dominated benches that are reported but never
  fail CI (e.g. `write/ingest_wal_on`).

- **The deterministic budget signal already has a home in the mandatory gate.** `scripts/agent-gate.sh`
  runs `work-counters-guard`, `byte-budget-guard`, `arrow-parity-guard`, and `memory-budget`
  (dhat-heap) as components on **every** full gate. dhat allocation *counts and bytes* are
  machine-independent, so they give a hard per-gate signal that does **not** flake under load — unlike
  wall-clock. The epic-H allocation-observation machinery
  (`cqlite-core/tests/test_issue_1046_scan_alloc_scaling.rs`, the `#[global_allocator] dhat::Alloc`
  pattern, also used by #1668/#827/#1660) is directly reusable for the producer + converter and is
  currently unused on this path — the issue mandates reuse, not duplication.

- **#1495 has already merged (PR #2312).** The arrow-conversion win it delivered is already in `main`.
  The first baseline this change captures therefore *already contains the #1495 improvement*. That is
  correct and honest: the baseline is "current `main` tip," not a pre-#1495 number. #1496 (queued) and
  the AB/AE children are measured as ratios against **this** captured baseline, and the change states
  so explicitly so no one later mistakes the post-#1495 floor for a pre-optimization one.

## What Changes

- **Add a tiered export/Flight bench suite.** Criterion throughput benches over pinned datasets for:
  (a) CQL→Arrow conversion (`cqlite-core::export::arrow_convert::rows_to_record_batch`) — the per-cell
  data plane shared by Flight and Parquet; (b) json/csv/parquet export writers and delta export; and
  (c) an **end-to-end Flight `do_get` streaming-throughput** harness that drives the **public** Flight
  RPC surface over the existing in-process transport (`cqlite-flight/tests/do_get_transport_test.rs`
  harness) — not an internal helper. The Flight bench requires adding a `criterion` dev-dependency and
  a `[[bench]]` target to `cqlite-flight` (crate is `publish = false`).

- **Add allocation / peak-memory budget guards** for the conversion hot path and the Flight producer,
  built on the **reused** epic-H dhat machinery. Each guard asserts a measured bound against the
  **current-main** figure (with documented headroom) and is **non-vacuous**: it fails loudly if the
  fixture yields zero rows or if zero allocations were observed (a vacuous "0 allocs" can never pass).
  These are the observation points AB1/AB3/AB7/AE1–AE5 tighten; #1494 lands them **passing** as
  baseline locks (they encode "do not regress below today"), and the AB/AE children own the aggressive
  target-bound assertions that fail-on-today by construction.

- **Register the export/Flight perf-gate entry.** Add the conversion + export micro-benches to
  `perf-gate.json` as **STRICT** median-regression entries, and the end-to-end Flight `do_get`
  throughput bench as an **ADVISORY** entry (its wall time is async-runtime + transport dominated, like
  `write/ingest_wal_on`). Wire the new benches into both `cargo bench` invocations in
  `perf-regression.yml`, guarded by the established "target may not exist on `main` yet" pattern so the
  first landing SKIPs green.

- **Commit the baseline artifact + refresh procedure.** `perf-gate.json` (tracked-bench list +
  thresholds + advisory classification) is the committed policy artifact; `cqlite-core/benches/README.md`
  records the human-readable current-main baseline numbers (post-#1495) as a durable "baseline before
  wins" record, and documents the refresh procedure (base is re-measured every CI run — never a stale
  committed number; retuning a threshold edits `perf-gate.json` + updates the README numbers in the PR).

- **Wiring evidence + red-run.** Each bench asserts at setup that it drove the public surface and
  returned ≥ 1 row (a full-scan/zero-row fallback panics, never masquerades as a measurement); the PR
  demonstrates a red-run (artificially slow the converter → the STRICT gate entry FAILs; inflate a
  producer allocation → the budget guard FAILs).

## Non-goals

- **No export/Flight production-code changes.** This is additive bench + budget-test + gate-wiring +
  docs work only. The AB/AE children make the code changes and tighten the budgets this change seeds.
- **No new perf-gate *mechanism*.** Reuse `check_perf_regression.py` (STRICT/ADVISORY/SKIP + the
  `scaling_floors` kind) and `perf-regression.yml`; only the tracked-bench list and workflow bench
  targets grow. No committed absolute-timing baseline (drift/flake); the same-runner ratio stands.
- **No wall-clock perf gating inside the local `agent-gate.sh`.** The mandatory gate's export/Flight
  signal is the load-deterministic dhat budget guard; wall-clock throughput lives in the CI perf lane
  (`ci:perf` label / nightly), so a loaded box never reds the gate of record.
- **No tail-latency / p99 gating** for the Flight path (a later child if needed).
- **Not** setting the aggressive AB/AE target bounds — those are the consumer issues' TDD red tests.

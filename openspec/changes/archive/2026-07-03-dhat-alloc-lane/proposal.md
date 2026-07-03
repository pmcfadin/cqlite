## Why

The project's <128 MiB memory target (CLAUDE.md "Memory target") is checked only by a **manual**
dhat step (`./scripts/profile.sh heap`) that no CI or gate run ever executes. There is no
allocation-budget regression net and no compile-time pin on `size_of::<Value>()` (measured today;
three rare `Value` variants are still inlined — Epic E #1517 E1 will shrink it). A read-path
allocation regression, or a `Value` that silently grows a hot per-cell type, merges green today.

This is child **A4** of Epic A (#1513, "measurement first"; issue #1565). Source of truth:
`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic A, Wave 1. It is design-driven,
additive **measurement machinery** — Seam-1 pre-approved for the Epic A batch. It reuses A1/A2
conventions: the same `benches/fixtures/mod.rs` real-SSTable loader and the existing
`cqlite-core/examples/heap_profile.rs` dhat wiring (dhat is already a `cqlite-core` dependency,
installed only under the opt-in `dhat-heap` feature).

**Ratchet, not improvement.** The deliverable is the *machinery + honest pins* set to **today's
measured numbers** (measured-first, ceiling = measured + documented slack). The optimization epics
(E1 `Value` shrink; E2/E3 read-path alloc reductions) then *tighten* these ceilings. Nothing here
"fixes" allocations — no production-code changes.

Guardrail (from the issue): **no production-code changes** — additive test/gate machinery only, plus
one compile-time `size_of` assertion in `types.rs` (a zero-cost pin, not a behavior change). Never
let a dataset-dependent budget test pass on an empty dataset.

## What Changes

- **A dhat allocation/memory-budget test lane** — a new integration test target
  `cqlite-core/tests/memory_budget.rs`, compiled only under the `dhat-heap` feature (so normal builds
  and the default `core-tests` run are unaffected; the file installs the `dhat::Alloc` global
  allocator, so it must be its own binary). It opens a real fixture via the shared
  `benches/fixtures/mod.rs` loader and asserts allocation/peak-byte budgets from `dhat::HeapStats`.
- **`select_full_scan_alloc_budget`** — runs a full-table `SELECT *` over the largest available real
  fixture, asserts total bytes allocated ≤ a pinned ceiling. (The issue's "10k-row" intent: real
  corpus fixtures cap below 10k rows, so we drive the largest real fixture and, to reach a
  meaningful row count, repeat the scan a fixed number of iterations — the workload is real SSTable
  data, not synthetic; the ceiling is the honestly-measured total.)
- **`materialized_select_byte_ceiling`** — runs a materializing `SELECT *` over the type-heavy
  fixture, asserts **peak** heap bytes ≤ a pinned ceiling (and ≤ the 128 MiB project budget).
- **A compile-time `Value` layout pin** in `cqlite-core/src/types.rs`:
  `const _: () = assert!(std::mem::size_of::<Value>() <= N);` where `N` is today's measured
  `size_of::<Value>()`, with a comment naming the measured value and Epic E #1517 E1's smaller
  target. Fails the build (any feature set) if `Value` grows past the pin.
- **Gate wiring**: a new `memory-budget` component in `scripts/agent-gate.sh` that runs the lane with
  `--features cli-helpers,dhat-heap` and `--test-threads=1` (dhat's global profiler is a singleton;
  concurrent test threads would double-install it). It is dataset-dependent — added to
  `DATASET_COMPONENTS` so the existing preflight FAILs loudly on a missing/empty dataset (never a
  silent skip). The gate header documents the component and its budgets.
- **TDD honesty**: each budget test is written so that setting its ceiling below today's measured
  value makes it FAIL red — demonstrated once in the PR, then landed at the honest ceiling.

## Non-goals

- **No read-path production code changes.** Additive test/gate machinery + one zero-cost compile-time
  `size_of` assertion only (issue guardrail). The actual `Value` shrink and allocation reductions are
  later Epic E children; this only makes them measurable and ratchet-able.
- **No tightening of the ceilings here.** They ship at today's measured values (+ documented slack);
  E1/E2/E3 tighten them. Deliberately-red "too-tight" runs are shown in the PR only, not committed.
- **No new fixture corpus.** Reuses the vendored real SSTables via `benches/fixtures/mod.rs`; adds no
  Docker/network/synthetic-data dependency.
- **No change to A1/A2/A3 gates** (`perf-gate.json`, `tail-latency-gate.json`) — the memory-budget
  lane is a separate, self-contained dhat test target.
- **Not** a cross-machine absolute-memory SLO; peak-byte ceilings are pinned to the measured local
  workload with documented slack and are ratchet targets, not portable guarantees.

# Proposal: Feature-matrix gate lanes — cqlite-flight tests, legacy-heuristics execution, isolated parquet/delta-scan (issue #1699)

**Milestone:** 0.17 hygiene · **Priority:** P1 · **Routing:** design-driven — this changes **what the gate of
record certifies**, the cost every worker pays on every full run, and it must be reconciled against three
coverage layers that landed *after* the audit that filed it (the scoped clippy matrix #1844, the
`flight-query-semantics-oracle` component, and the CI Flight tier under `required` #2910). No external
oracle exists; the deliverable is a gate contract plus recorded observation. · **Issue:** #1699 ·
**Epic:** #1685 (config honesty) · **Source:** `docs/reports/platform-observability-audit-2026-07-01.md`
finding AH6 · **Wave-1 measurement train** with parser H #1601, read A #1513.

## Why

The gate's feature coverage has holes of exactly the class that caused the Minimal-Features CI incident
(#1978: a `write-support`-gated item referenced from an ungated `#[cfg(test)]` module broke a CI build that
no local lane compiled). Three specific holes, **re-verified against the tree at `2bde26a7c` rather than
taken from the audit**:

1. **`cqlite-flight`'s test suite is never EXECUTED by the gate.** The crate ships **41 integration test
   files** plus `--lib` unit tests. The full gate runs exactly **three** of those tests, by name:
   `query_semantics_flight_parity` and `issue_3095_flight_static_columns` (`flight-query-semantics-oracle`,
   `agent-gate.sh:5198`) and one dhat test (`memory-budget`, `agent-gate.sh:8081`). Everything else is
   compiled — by clippy's per-package pass (`agent-gate.sh:4722`, `--all-targets`) — and never run.
2. **`legacy-heuristics`' 95 cfg sites are never EXECUTED, and never built in isolation.** The feature is
   *test-compiled* today (it is in clippy's cqlite-core feature list, `agent-gate.sh:4700`), so the audit's
   "never test-compiled" is stale — but compiling is not running. Five `cqlite-core/tests/*.rs` files carry
   `#[cfg(feature = "legacy-heuristics")]` test bodies that **no gate component and no CI job has ever
   executed**. And because that clippy pass enables ~30 features at once, the feature is never exercised at
   its own minimal feature set.
3. **`parquet` and `delta-scan` are never built in ISOLATION.** They appear only inside clippy's combined
   feature list, which is the shape that *masks* cross-feature coupling: a `parquet`-gated item that
   accidentally references a `delta-scan`-gated item compiles fine whenever both are on.

The gate is THE gate (local-first doctrine). Two of these holes are covered *downstream* — CI's Flight tier
runs `cargo test -p cqlite-flight --lib` and, in its full tier, `cargo test -p cqlite-flight`
(`flight-ci.yml:193,229`), and `required` fails closed on that tier's absence (#2910). That is the wrong
way round: a worker discovers a Flight regression **after** pushing, on a tier that costs a CI round-trip,
instead of in the gate that is supposed to be the verdict. The isolation hole is covered nowhere.

**Measured, not assumed** (this box, warm shared target dir, `cargo` 1.97.1): the two isolation lanes cost
**18 s** (parquet) and **10 s** (delta-scan) and both **pass today** — so they are regression guards, not
latent-bug finders, and that is stated rather than oversold. `cargo build -p cqlite-core --features
legacy-heuristics` costs **26 s**. Flight and legacy-execution costs are recorded in `design.md` D6 with the
lane-scope decision they drive.

## What Changes

1. **Four new full-gate components**, registered in `COMPONENTS`, `--list`, the dispatch table and the
   SUMMARY block:
   - `flight-tests` — executes cqlite-flight's suite (scope decided in `design.md` D4).
   - `legacy-heuristics` — `RUSTFLAGS=-D warnings cargo build -p cqlite-core --features legacy-heuristics`,
     then **runs** the legacy-heuristics-gated cqlite-core tests, with the target set **derived
     mechanically** from the committed source and fail-closed when the derivation finds no subject.
   - `feature-iso-parquet` — `cqlite-core --no-default-features --features all-compression,parquet`,
     **without** `delta-scan`.
   - `feature-iso-delta-scan` — the mirror, **without** `parquet`.
2. **The isolation lanes test-compile** (`--all-targets`) under `RUSTFLAGS=-D warnings`, not a bare
   `cargo check`. The issue's literal `cargo check` would build a lane blind to the very incident class it
   cites — #1978 was a `#[cfg(test)]` module, and a plain check never compiles test targets. This is a
   deliberate strengthening, recorded in `design.md` D2.
3. **Every new lane is OBSERVED to fire, not merely present** (#3272 doctrine). A committed, re-runnable
   planted-break harness plants the minimal cross-feature / missing-execution break for each lane in a
   throwaway `git worktree` and asserts that lane exits non-zero — and that the unbroken lane still passes,
   so the harness cannot pass by failing everything. Opt-in (it compiles), with the observation recorded.
4. **Cheap structural asserts in the fast loop**: the components are registered, appear in `--list`, and
   appear in the SUMMARY — pinned in `scripts/tests/test_agent_gate_summary.sh`, which `--lite` already runs.
5. **Wall-time accounting posted**: per-component durations from the SUMMARY plus a baseline-vs-after full
   gate total measured sequentially on one box (one gate at a time, #2640).
6. **Doctrine updated in the same change**: the CLAUDE.md gate table's description of what the full gate
   covers, and the website `agents-developing/gate-contract/` page.

## Do NOT

- **Not CI-only.** These are gate components; CI mirrors the gate, never the reverse.
- **Not `--all-features` for the isolation lanes** — isolation is the entire point.
- **Do not restructure `flight-query-semantics-oracle`.** Its per-lane fixture SKIP predicates are
  load-bearing (#3095) and a small overlap with `flight-tests` is cheaper than re-deriving them.
- **Do not add an opt-out env var for a new lane.** Committed source and a committed crate are never
  legitimately absent; an escape hatch could only buy a vacuous green.

## Risks / open decisions (Seam 1)

- **The legacy-heuristics-gated tests have never been executed.** They may not pass. `design.md` D3 states
  the three exits (fix / `#[ignore]` + filed follow-up / narrow the lane to compile-only) and the
  recommendation; the measured answer is recorded before implementation begins.
- **`flight-tests` is the only expensive new lane.** D4 decides whole-package vs `--lib`+bounded set on the
  measured number, and places it in the SIDE lane with its own `CARGO_TARGET_DIR` so it does not thrash
  MAIN's shared target dir (#2657). **RESOLVED during implementation: `--lib --bins`** — the measurement was
  not cost but CORRECTNESS. The package's integration suite is ~50% non-deterministic under intra-package
  parallelism (4 runs PASS/FAIL/PASS/FAIL, 2 distinct victims; box load, `nice`, `--test-threads=2` and
  concurrent MAIN compilation each ruled out by measurement), so the integration half is descoped as #3384
  and the lane DECLARES that gap in a derived census it prints on every run. See D4's second correction.

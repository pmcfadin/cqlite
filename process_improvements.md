# Delivery Process Improvements — throughput tracker

Living tracker for CQLite delivery-pipeline throughput work. Owner-facing: what we're
changing, why (data), and current status. Source of truth for *failures* is the delivery
telemetry ledger (`docs/reports/delivery-telemetry.jsonl`) + `scripts/delivery-telemetry.py retro`.

Last updated: 2026-07-03.

## Where the time actually goes (telemetry, n=91 issues)

| Phase | Median | Mean | Notes |
|---|---|---|---|
| created → PR | 19.6h | 87.7h | dominated by backlog wait, not active work |
| PR → merge (review/gate/roborev tail) | **0.8h** | 2.3h | active pipeline is fast once claimed |
| total cycle | 24.3h | 90.0h | |

Retro weighted failure ranking:

| category | count | weight | score |
|---|---|---|---|
| rework | 220 | 4 | **880** |
| roborev_findings | 430 | 2 | 860 |
| gate_failures | 69 | 5 | 345 |
| rebase_events | 107 | 2 | 214 |
| claim_collisions | 0 | 3 | 0 |

**Key reads:**
- The pipeline tail is *fine*. The controllable cost is **iteration churn**: roborev findings
  (4.7/issue, max 40) → rework (2.4/issue) → full gate re-runs (`gate_runs` 1.8/issue, max 9;
  each run ~45–60 min wall-clock ≈ ~120 gate-hours total across the program).
- Findings inflate the tail directly: 0 findings → 0.4h to merge; 4+ findings → 1.3h (3.3×).
  Worst offenders are design-routed parity work (#1028: 22 findings/11 rework; #1027: 17/9).
- **`claim_collisions = 0`** — the branch-lock claim protocol is working; no throughput lost to
  duplicate work. Do not touch it.
- **~330G of duplicated `target/` dirs** across 7 worktrees — every fresh worktree cold-compiles
  the whole workspace before its first gate run. No shared compiler cache today.

## The levers (in flight)

| Lever | Issue | What | Status |
|---|---|---|---|
| Reduce recurring `rework` (retro #1) | #1793 | push recurring finding classes left so they never trigger a fix→re-gate round | open |
| Reduce recurring `roborev_findings` | #1736 | same family — pre-empt the recurring finding classes | open |
| Tiered gate (`--lite`) + review-first | #1821 | fast inner-loop gate subset for iteration + full gate once pre-merge; conditional internal review before roborev | open |
| **Shared compiler cache (sccache)** | **#1822** | per-worktree `target/` + shared object cache to delete cross-worktree cold-compile duplication; rejected shared `CARGO_TARGET_DIR` (build-lock serializes parallel gates) | ✅ **DONE (PR #1833)** — 562s / 25.6% saved on fresh-worktree case, 100% hit rate |
| Machine-wide gate concurrency cap | #1825 | bound simultaneous full-gate runs so higher session concurrency stays safe (also: concurrent gates skew wall-clock measurements) | open |
| Gate perf: nextest + shared build cache | #1737 | ~15–20min sequential gate bottleneck; nextest + build cache (overlaps #1822) | open |

Three orthogonal families: **(1) cut the churn at the source** (#1793/#1736/#1821), **(2) delete
duplicated compile work** (#1822/#1737), **(3) make higher concurrency safe** (#1825). Do all three;
they compound.

## Activity log

- **2026-07-03** — Ran telemetry retro over 91 records; produced the time-suck analysis above.
  Identified sccache (over shared `target/`) as a root-cost lever independent of gate tiering.
- **2026-07-03** — Filed **#1822** (sccache spike, scoped, measure-first) and dispatched a
  `test-validator` subagent to run the cold-vs-warm gate measurement in an isolated worktree; wire
  into `agent-gate.sh` (auto-detect, graceful no-op) only if the measured delta justifies it.
  Merge-on-green authorized by owner. Awaiting the cold/warm measurement table.
- **2026-07-03** — **#1822 landed (PR #1833, `1547fea6`).** Spike measured 3 scenarios with
  sccache 0.16.0: COLD (empty cache) 36.6 min / 10% hit → FRESH_WITH_CACHE (new worktree, warm
  cache) **27.3 min / 100% hit** → WARM (incremental `target/`) 17.3 min. **562s (25.6%) saved on
  the fresh-worktree case** — the cross-worktree scenario sccache targets. Compile-bound components
  24–91% faster (format-compat 91%, smoke/minimal-build 76%, cli/write/integration 53–56%);
  test-execution-bound (core-tests) <5%, as expected. **Decision: WIRED IN** — auto-detect in
  `agent-gate.sh` (opt-out `CQLITE_DISABLE_SCCACHE=1`), `CARGO_INCREMENTAL=0`, `CARGO_TARGET_DIR`
  rejected. Final gate with wiring: RESULT PASS (99.9% hit). roborev clean.
  Insight: incremental `target/` state still beats sccache for *repeated local edits* in one
  worktree — the two are complementary (sccache for fresh worktrees, incremental for local
  iteration). #1737 (nextest + build cache) now partially subsumed by this; flag for dedup.

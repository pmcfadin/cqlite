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
| Tiered gate (`--lite`) + review-first | #1821 | fast inner-loop gate subset for iteration + full gate once pre-merge; conditional internal review before roborev | 🔜 almost done (PR #1828) |
| **Shared compiler cache (sccache)** | **#1822** | per-worktree `target/` + shared object cache to delete cross-worktree cold-compile duplication; rejected shared `CARGO_TARGET_DIR` (build-lock serializes parallel gates) | ✅ **DONE (PR #1833)** — 562s / 25.6% saved on fresh-worktree case, 100% hit rate |
| Machine-wide gate concurrency cap | #1825 | bound simultaneous full-gate runs so higher session concurrency stays safe (also: concurrent gates skew wall-clock measurements) | open |
| **Gate perf: nextest + parallel components** | **#1737** | `cargo-nextest` for the core-tests floor + capped 2-lane parallel components + live-Docker parity tests skipped by default (kept in nightly lanes) + fail-closed result collection | ✅ **DONE (PR #1841)** — **258s vs 697s same-machine (63% off) / 75% vs 1036s ref**; nextest 2917 passing, no tests dropped |

Three orthogonal families: **(1) cut the churn at the source** (#1793/#1736/#1821), **(2) delete
duplicated compile work** (#1822/#1737), **(3) make higher concurrency safe** (#1825). Do all three;
they compound.

## Testing principle: focus the iteration loop, keep the merge gate complete

Speed comes from running tests **faster/in parallel** (nextest #1737), **not recompiling** (sccache
#1822), and **focusing the inner loop** (`--lite` tiering + path→component scoping, #1821) — NOT from
permanently skipping tests before merge. CQLite's whole value is byte-for-byte Cassandra parity; the
merge gate's job is to catch the regression you didn't predict. So:
- **Per-test change-based selection (test-impact analysis) is deliberately kept OUT of the merge gate.**
  Rust has no trustworthy per-test dependency graph, so any selection is itself a heuristic — and
  skipping a byte-parity test on a bad guess is the exact silent-failure class this project guards
  against. Fine as an optional inner-loop accelerator only.
- **Path→component scoping** (touch `bindings/python/**` → skip node-bindings, etc.) is a safe, coarse
  win for the `--lite` inner loop — folds into #1821. The full gate still runs everything pre-merge.

### Goldens-in-gate, Docker-in-nightly (the parity boundary)

- **The agent gate runs against STATIC GOLDEN datasets** (`CQLITE_DATASETS_ROOT`), never live
  containers. Fast + deterministic + complete.
- **Live Docker (Cassandra 5.0) is for fixture *generation/regeneration* only** — nightly /
  `workflow_dispatch` parity lanes (`cassandra-parity.yml`, `tombstone-ttl-parity.yml`,
  `cql-type-parity.yml`, `nightly-docker-parity`, `exhaustive-regeneration.yml`) and
  `test-data/scripts/*.sh`. Intentionally off the gate + PRs.
- **⚠️ Leak found 2026-07-03:** a few parity tests (`cqlite-core/tests/issue_911_bti_*.rs`,
  `cqlite-cli/tests/compatibility/**`) probe `docker info` and run containers *if Docker + a Cassandra
  image are present*. This gate machine HAS Docker + `cassandra:5.0/5.0.2/4.1` cached, so those tests
  **fire during `core-tests`**, adding real wall-clock + non-determinism to the 694s floor. Folded into
  **#1737**: measure their cost, and skip the Docker-spawning tests in the gate/`--lite` path (env
  guard or nextest filter) so the gate stays on goldens — without dropping the coverage (it stays in
  the nightly Docker lanes).

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
- **2026-07-03** — Post-sccache gate breakdown revealed the new floor: **core-tests is 67% of the
  17.3-min warm gate (694s)** and it's test *execution*, not compile — sccache can't touch it. The
  other 15 components combined are ~37s; the gate is still strictly sequential. **Re-scoped #1737**
  (owner-directed) to the 2 remaining levers — `cargo-nextest` for core-tests (2–4× typical) +
  capped parallelism of independent components (~32% alone, collapses to the core-tests long pole;
  concurrency-capped per #1825). Moved out of #1737: build cache → #1822 (done), two-tier → #1821.
  Claimed #1737 and dispatched a `test-validator` subagent to implement + measure (≥40% target off
  the 1036s baseline).
- **2026-07-03** — **#1737 landed (PR #1841, `0c6aeee6`).** cargo-nextest for core-tests + capped
  2-lane parallel components → **258s vs 697s same-machine baseline (63% off) / 75% vs the 1036s
  reference** — clears ≥40% and sub-6-min stretch. nextest ran **2917 tests passing**, doctests
  preserved (separate `--doc` pass), no tests dropped. Also **skipped live-Docker parity tests in the
  gate by default** (`CQLITE_SKIP_DOCKER_TESTS=1`; they spin up Cassandra + add non-determinism —
  coverage kept in the nightly Docker lanes), and added **fail-closed result collection**
  (roborev round-1 caught a fail-OPEN hole: a side-lane component dying before writing its `.result`
  was silently omitted while the gate still reported PASS → now any missing result or nonzero
  side-lane exit forces RESULT: FAIL). Delivery cost: 1 roborev finding (fixed), 2 rebases (main
  moved + #1693 graceful-shutdown cli test conflict, resolved preserving both).
  **Caveat:** the gate's final RESULT was FAIL — but *solely* the 3 `issue_1020` UDT compaction-parity
  tests, a **pre-existing main-red from committed duplicate fixtures** (commit e51bf879, tracked by
  **#1840**), which fail on `main` under any runner and are unrelated to #1737. Owner-authorized merge
  over that red; #1840 (which fixture generation is canonical) stays a separate fix.
- **Gate wall-clock arc (this session):** compile-dedup (sccache, #1822) took the fresh-worktree gate
  36.6→27.3 min; then nextest + parallelism (#1737) took the warm gate **17.3 → ~4.3 min**. The gate
  went from a ~15–20 min sequential bottleneck to sub-5-min, with compile cost erased and the
  test-execution floor parallelized.

## Change detail — measurable process changes

Each entry below states what changed, the problem it targets, a falsifiable hypothesis, and
exactly how we will measure whether it worked — so we can evaluate (and revert) changes with
data instead of vibes. The primary measurement source is the append-only delivery-telemetry
ledger `docs/reports/delivery-telemetry.jsonl` (schema
`docs/reports/delivery-telemetry.schema.json`), stamped once per completed issue by
`flow-finalize` via `scripts/delivery-telemetry.py`.

**How to add an entry:** append a new `### YYYY-MM-DD — <short title>` subsection at the TOP
of this section (newest first) with: **Change** (what concretely changed), **Problem it
targets** (observed pain + issue numbers), **Hypothesis** (a falsifiable prediction),
**How to measure** (exact ledger fields / query + before/after windows; name the baseline
data point(s)), and optionally **Status / result** (filled in later once enough post-change
issues have landed). Keep entries short so a future reader can re-run the measurement.

### 2026-07-03 — Tiered gate (`--lite`) + conditional review-first + full-gate-once-before-merge

- **Change** — Added `scripts/agent-gate.sh --lite`, a fast iteration gate that
  runs ONLY `file-size` + `fmt` + FULL-workspace `clippy` (`-D warnings`) +
  blast-radius-scoped tests (the touched package's `--lib` + the diff's new
  `--test` targets), emitting a distinct `==== AGENT-GATE LITE SUMMARY ====`
  block (`MODE: lite`) that can never be pasted as the full gate's SUMMARY. The
  default (no-flag) full gate is byte-for-byte unchanged. Doctrine (CLAUDE.md,
  `flow-implement`, `worker`) now prescribes the loop
  `implement → lite (each fix round) → conditional internal rust-reviewer review
  → lite → FULL gate ONCE before merge → roborev → CI → merge`, with an internal
  `rust-reviewer` review-first pass before the first full gate for diffs that
  change a `pub` item, touch >1 call site of a changed symbol, or add a new
  surface (skipped for mechanical/localized diffs). **`--lite` never replaces the
  full gate**: the full `scripts/agent-gate.sh` runs exactly once before merge and
  its `==== AGENT-GATE SUMMARY ====` block is the only run that counts.

- **Problem it targets** (session retro 2026-07-03):
  1. The full gate is the bottleneck — `core-tests` (~440–697s) plus python/node
     bindings (~70–220s each) push a run to 12–25 min, and it was being run on
     **every roborev round**.
  2. Multi-round roborev churn — each convergence round forced another full-gate
     cycle.
  3. Machine-saturation SIGKILLs — under load 30–60 with ~15 concurrent gates,
     gates got SIGKILLed mid-`core-tests` and retried (one implementer wedged
     ~1h22m purely waiting on the gate).

- **Hypothesis** — Iterating on `--lite` and running the FULL gate only once
  before merge (plus catching structural findings via review-first) reduces
  **full-gate runs per issue** and **roborev rounds per issue**, which lowers
  **cycle time** and machine saturation. Directionally: full-gate-runs/issue and
  roborev-rounds/issue should both fall vs the pre-change baseline.

- **How to measure** — Using `docs/reports/delivery-telemetry.jsonl`, compare
  before vs after this change, per issue:
  - full-gate runs per issue (gate pass/fail counters),
  - roborev rounds per issue (roborev findings / rework passes),
  - rework passes, and
  - cycle time + phase durations (from GitHub timestamps).
  Aggregate a window of issues before this entry's date vs a window after, and
  look for a downward shift in full-gate-runs/issue and roborev-rounds/issue with
  cycle time flat-or-down.
  - **Baseline (this session's issues):** #1589 — one-and-done roborev; #1692 —
    three roborev rounds (three full-gate cycles); gate SIGKILLs observed under
    load. These are the pre-change reference points.

- **Status / result** — TBD (revisit after enough post-change issues have landed
  in the ledger).

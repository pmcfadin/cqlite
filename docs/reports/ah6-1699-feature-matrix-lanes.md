# AH6 — feature-matrix gate lanes: observed to fire (issue #1699)

Issue #1699 added four gate components — `flight-tests`, `legacy-heuristics`,
`feature-iso-parquet`, `feature-iso-delta-scan`. This report is the issue's **AC2**
deliverable: affirmative evidence that each lane **fires on a planted break** and
**stays silent on a clean tree**.

## Why a green lane is not evidence

Presence in `scripts/agent-gate.sh --list` proves a lane is *registered*. A green
SUMMARY line proves it *ran and found nothing*. Neither proves it **can fail**.
`feature-iso-parquet` reports `PASS (0s)` on a warm tree, and from the SUMMARY block
alone that is indistinguishable from a lane that compiles nothing and always exits 0.
Design decision **D5** therefore requires each lane to be *observed* in both
directions, and the delta spec states it as a binding requirement: *"Every new lane is
observed to fire on a planted break and not to fire on a clean tree."*

A planted-break harness that only ever plants breaks is the vacuous-guard shape of
#3229 — it passes just as happily against a lane that fails unconditionally. So the
harness asserts **both** directions per lane, and reports a lane that is red in both
as a **HARNESS FAILURE**, never as a successful observation.

## The harness

`scripts/tests/test_agent_gate_feature_matrix_lanes.sh` — committed, re-runnable,
and **opt-in**: it is deliberately absent from `COMPONENTS`, `LITE_COMPONENTS` and
`DELTA_COMPONENTS`, because it performs real compiles and taxing every full gate to
re-prove a static property is disproportionate (D5). Nightly `gate.yml` enrollment is
out of scope — a workflow change needs #2910 registry enrollment.

```bash
export CQLITE_DATASETS_ROOT=/data/datasets   # the absolute root fetch-datasets.sh prints
bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh              # all four lanes
bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh flight-tests # one lane (exits 3: PARTIAL)
```

Properties that make the observation mean something:

- **It runs the real component**, `bash scripts/agent-gate.sh --only <lane>`, never a
  retyped cargo command. A retyped command would prove that a cargo invocation works;
  the subject here is the gate component.
- **`--only` exit codes are load-bearing and are not the usual 0/1.** A PARTIAL run
  that found nothing exits **3** (the gate refuses to let a partial run be scripted
  into a green claim); a PARTIAL run with a failed component exits **1**. The harness
  additionally parses the component's own SUMMARY line and requires exit code and
  status to agree, so a gate that mis-reported one of them could not be mistaken for
  an observation.
- **All mutation happens in a throwaway `git worktree add --detach` copy.** #2926
  makes a mid-run tree mutation a gate FAIL, so a harness that edited the tree its own
  gate was running in would be the very defect it exists to catch. Plants are applied
  and reverted **between** runs, never during one; the copy is removed on an `EXIT`
  trap including on failure.
- The copy gets its **own `CARGO_TARGET_DIR`** (outside the copy, so the revert cannot
  sweep it), so a lane's clean and planted runs share compilation.
- Reverts are uniform (`git checkout -- . && git clean -fd`) and **verified** —
  a tree that will not revert is reported as a harness error rather than silently
  contaminating the next lane.

## Observed results

Run at `94833d510` on the worker box, `CQLITE_DATASETS_ROOT=/data/datasets`.
Harness elapsed: **581 s** (8 runs — one clean and one planted per lane). All four
lanes fired.

| lane | planted break | clean tree | planted tree | attributed to |
|------|---------------|-----------|--------------|---------------|
| `feature-iso-parquet` | a `#[cfg(feature = "parquet")]` fn at the root of `cqlite-core/src/lib.rs` calling a `#[cfg(feature = "delta-scan")]` fn — compiles with both features on (clippy's ~30-feature cqlite-core arm), unresolved with parquet alone. #1978's class. | **PASS** (exit 3, 112 s) | **FAIL** (exit 1, 20 s) | `ah6_planted_delta_scan_marker` |
| `feature-iso-delta-scan` | the mirror: a `#[cfg(feature = "delta-scan")]` fn calling a `#[cfg(feature = "parquet")]` fn. | **PASS** (exit 3, 46 s) | **FAIL** (exit 1, 16 s) | `ah6_planted_parquet_marker` |
| `legacy-heuristics` | a **new** `cqlite-core/tests/ah6_planted_legacy.rs` holding a `#[cfg(feature = "legacy-heuristics")] #[test]` with an inverted assertion. | **PASS** (exit 3, 176 s) | **FAIL** (exit 1, 5 s) | `ah6_planted_legacy_heuristics_break` |
| `flight-tests` | a **new** `cqlite-flight/tests/ah6_planted_flight.rs` with a failing `#[test]`. | **PASS** (exit 3, 178 s) | **FAIL** (exit 1, 27 s) | `ah6_planted_flight_break` |

Two of the plants do extra duty beyond "the lane can fail":

- The `legacy-heuristics` plant is a **new file**, so the lane's red also proves its
  `--test` target set is genuinely **derived** from the committed source (it picked up
  a sixth gated file with no gate edit; a hard-coded list would have ignored it and
  stayed green) and that the lane **executes** rather than merely compiles — a
  compile-only lane stays green on a failing assertion, which is exactly D3's premise.
- The `flight-tests` plant is a target the gate names **nowhere**, so its red proves
  the lane reaches past the three cqlite-flight targets already covered
  (`query_semantics_flight_parity`, `issue_3095_flight_static_columns`, and
  `memory-budget`'s dhat target).

**Attribution.** A bare red is not evidence: a lane that broke for an unrelated reason
produces the same exit code and the same SUMMARY line. The harness therefore requires
each planted run's output to **name the planted symbol** (the right-hand column above);
a red that does not is reported as `FIRED-UNATTRIBUTED` and fails the harness.

**Exit codes.** `--only` on a component that found nothing exits **3** (`PARTIAL` — the
gate refuses to let a partial run be scripted into a green claim); with a failed
component it exits **1**. The harness checks the exit code and the SUMMARY status line
and requires them to agree.

The durations in the table are the harness's own `--only` runs from a fresh throwaway
worktree against a shared, partly-warm `CARGO_TARGET_DIR`. They are neither the cold
figures nor the gate's warm figures below; they are recorded for reproducibility, not
as the lanes' cost.


## Cost

Two different numbers, and the second **cannot be derived from the first**.

### Per-component durations

| lane | measurement | secs | note |
|------|-------------|------|------|
| `feature-iso-parquet` | `cargo check --no-default-features --features all-compression,parquet` | 18 | **cold**; lib-only `cargo check` — the *superseded* shape (D2 replaced it with `cargo test --lib --no-run`) |
| `feature-iso-delta-scan` | `cargo check --no-default-features --features all-compression,delta-scan` | 10 | **cold**; same superseded shape |
| `legacy-heuristics` (build half) | `cargo build -p cqlite-core --features legacy-heuristics` | 26 | **cold** |
| `flight-tests` | `cargo test -p cqlite-flight` (whole package) | 128 | **cold** |
| `legacy-heuristics` (component) | first green run of the component as shipped | 37 | first green run |
| `flight-tests` (component) | SUMMARY line | 27 | **warm cache** |
| `legacy-heuristics` (component) | SUMMARY line | 7 | **warm cache** |
| `feature-iso-parquet` (component) | SUMMARY line | 0 | **warm cache** — *not* the lane's cost |
| `feature-iso-delta-scan` (component) | SUMMARY line | 1 | **warm cache** — *not* the lane's cost |

The warm numbers are labelled as warm on purpose. A warm `0s` is the cost of cargo
deciding there is nothing to rebuild; it is not what the lane costs on a tree that
actually changed. The cold `cargo check` figures for the two isolation lanes measure
the **superseded** instrument (lib-only `cargo check`); the shipped lanes run
`cargo test --lib --no-run`, which additionally compiles the lib's `#[cfg(test)]`
modules — the #1978 incident class a bare `cargo check` is blind to.

### Added full-gate wall time

**METHOD CHANGED, AND WHY — a baseline-vs-after subtraction is not measurable on this
fleet, so the question is answered a better way.**

The plan was a baseline full gate at `origin/main` versus the gate of record on this
branch, run sequentially. It was attempted and **abandoned mid-run, deliberately**: this
worker box hosts five lane worktrees, and while the baseline ran the 16-core box sustained
a load average of **52–86** from co-scheduled gates in lanes 1697/1701/1705. A four-component
delta cannot be recovered from two totals taken under load that varies by more than the
delta itself, and the "after" run would sit under different load again. Publishing the
subtraction would have been a number with no measurement behind it. (The same load is the
prime suspect in #3380, an intermittent guard failure observed during this work.)

**The load-independent answer, which is also the one that matters: is the SIDE lane the
critical path?** All four lanes are dispatched to the concurrent SIDE lane by
`_component_lane`, each in its own `CARGO_TARGET_DIR`, because each builds cqlite-core at a
feature set diverging from MAIN's and would otherwise thrash MAIN's shared target dir
(#2657). Concurrent work adds wall time **only insofar as it outlasts MAIN**. So the added
wall time is read off a SINGLE run by comparing the SIDE lane's total against MAIN's:

- if MAIN still finishes last, the four lanes cost **zero** added wall time — they hid
  entirely inside MAIN's long pole;
- if SIDE now finishes last, the added wall time is `SIDE_total - MAIN_total`, and only
  that excess.

This needs no baseline, is immune to whole-box load (both lanes are inside the same run,
under the same load), and is computed from the gate of record's own per-component
durations. The figure is reported in the PR from that SUMMARY block.

**These are different numbers, and summing the per-component durations does not yield
the second.** The lanes run in the concurrent **SIDE** lane, each in its own
`CARGO_TARGET_DIR` — `_component_lane` (`scripts/agent-gate.sh:2217`) dispatches all
four there, because each builds cqlite-core at a feature set that diverges from MAIN's
and would otherwise thrash MAIN's shared target dir (#2657). Concurrent work adds wall
time only to the extent it outlasts MAIN, so the naive sum of component seconds
overstates the added wall time, possibly to ≈0. Both numbers are reported; neither is
presented as the other.

## References

- Design: `openspec/changes/feature-matrix-gate-lanes/design.md` (D2, D3, D4, D5, D6)
- Harness: `scripts/tests/test_agent_gate_feature_matrix_lanes.sh`
- Registration pin: `scripts/tests/test_agent_gate_summary.sh` (runs in `--lite` via `tooling-tests`)

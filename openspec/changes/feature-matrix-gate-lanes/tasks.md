# Tasks: feature-matrix-gate-lanes (issue #1699)

> Design decided in `design.md`. In one line: add four full-gate components — `flight-tests`,
> `legacy-heuristics`, `feature-iso-parquet`, `feature-iso-delta-scan` — that **execute** what the gate
> currently only compiles (or does not touch at all), hold each to the #3272 observed-to-fire standard via a
> committed opt-in planted-break harness, pin registration with a `--lite`-reachable structural self-test,
> and post both cost numbers. AC→requirement map at the top of
> `specs/gate-feature-matrix-lanes/spec.md`.
>
> **Two premises in the issue are STALE at `2bde26a7c` and the design corrects them rather than implementing
> them literally**: `legacy-heuristics` IS test-compiled today (clippy's cqlite-core feature list,
> `agent-gate.sh:4700`) — so the lane's subject is EXECUTION (D3); and `cqlite-flight` is not wholly
> untested — three of its targets run by name and CI's Flight tier runs the package under `required` — so
> the gap is LOCAL pre-push execution (D4). Do not re-derive these.

## 1. `feature-iso-parquet` + `feature-iso-delta-scan` (surface: `scripts/agent-gate.sh`)
- [x] Register both names in `COMPONENTS` (`:2166`) and add dispatch arms. Do **not** add them to
      `LITE_COMPONENTS` or `DELTA_COMPONENTS`.
- [x] Each lane: `-D warnings` + `cargo test -p cqlite-core --no-default-features --features
      all-compression,<parquet|delta-scan> --lib --no-run` — the other feature absent.
      **SUPERSEDED INSTRUCTION:** this task originally mandated `cargo check --all-targets`. That instrument
      was measured WRONG — `--all-targets` compiles ~100 integration test files written against the default
      feature set (3 named failures = noise, not leakage), while `cargo check` does not compile the lib's
      `#[cfg(test)]` modules at all and is therefore blind to the #1978 class these lanes exist to catch.
      `--lib --no-run` is the shipped instrument, pinned by `1699-iso-instrument`/`1699-iso-forbidden`.
- [x] Place both in the SIDE lane with their own `CARGO_TARGET_DIR` (divergent feature set ⇒ MAIN
      target-dir thrash, #2657). Neither needs fixtures ⇒ **not** in `DATASET_COMPONENTS`.
- [x] No opt-out env var.

## 2. `legacy-heuristics` (surface: `scripts/agent-gate.sh`)
- [x] Register in `COMPONENTS` + dispatch. Build half: `RUSTFLAGS="-D warnings" cargo build -p cqlite-core
      --features legacy-heuristics`.
- [x] Execute half: derive the `--test` target set from **cargo metadata**, including a target when a
      `legacy-heuristics` cfg site appears anywhere in its **module closure** or when `required-features`
      names the feature; include `--lib`. **Fail closed on zero derived targets** (and on an unresolvable
      module tree), naming the derivation — never PASS, never SKIP.
      **SUPERSEDED INSTRUCTION:** originally "grep committed `cqlite-core/tests/*.rs`". That glob cannot see
      a manifest-gated target or a directory-style `tests/foo/main.rs`, so it silently understated the set
      (roborev rounds 11-13).
- [x] Run under the existing `check_no_unexpected_zero_tests` guard.
- [x] Add to `DATASET_COMPONENTS` (several derived targets consume fixtures) so the existing preflight
      applies.
- [x] SIDE lane, own `CARGO_TARGET_DIR` (feature set diverges from MAIN's).
- [x] **Record the first-ever execution result.** If positively-gated tests fail, apply the D3.RISK ruling
      from Seam 1 — default recommendation (b): `#[ignore]` with a filed follow-up issue, lane lands green
      over the remainder. Never (c) compile-only.

## 3. `flight-tests` (surface: `scripts/agent-gate.sh`)
- [x] Register in `COMPONENTS` + dispatch, SIDE lane, own `CARGO_TARGET_DIR`, in `DATASET_COMPONENTS`.
- [x] Scope per D4/D6 on the measured number — RESOLVED as `--lib --bins` (D4 second correction, #3384);
      no opt-out env var.
- [x] Run under a zero-tests guard THAT HAS A SUBJECT at this scope: `check_no_unexpected_zero_tests`
      disclaims `--lib`, so use its `--lib` analogue (each selected unittest target OBSERVED and NON-ZERO).
- [x] PRINT the coverage census on every run, to BOTH stdout (`>>>`) and the component log: declared
      integration-target count derived from `cargo metadata`, that this lane runs none of them, CI's Flight
      tier as what does, and #3384/#3383. Retire the flake-quarantine plumbing (no subject left); RETAIN
      `_package_test_targets` (feeds the census) and `check_declared_test_targets_observed` (uncalled, with a
      comment naming what will call it again).
- [x] Leave `flight-query-semantics-oracle` functionally untouched — including its per-lane #3095 fixture
      SKIP predicates. Overlap is accepted deliberately (D4).

## 4. Structural registration self-test (surface: `scripts/tests/test_agent_gate_summary.sh`)
- [x] For each of the four names assert: present in `COMPONENTS`, reachable in the dispatch table, printed
      by `--list`. Hermetic, no cargo, sub-second — it must stay affordable in `--lite`.
- [x] Verify the self-test is REACHED and that removing a name reds it. Demonstrated in a throwaway
      `git worktree`: `rc=1`, `FAIL - 1699-dispatch: feature-iso-delta-scan has NO dispatch_component arm`.
      **SUPERSEDED INSTRUCTION:** originally "reached by `--lite`". That is FALSE and was withdrawn —
      `test_agent_gate_summary.sh` runs in `tooling-tests`, which is **not** in `LITE_COMPONENTS`, so the
      enforcement is the **gate of record**. Deliberately not forced into `--lite`: 257 asserts / ~14s
      against `roborev-lints`' sub-second hermetic charter.

## 5. Planted-break harness (surface: `scripts/tests/test_agent_gate_feature_matrix_lanes.sh`, new)
- [x] Create a throwaway `git worktree add --detach` copy; **never** mutate the live checkout. Assert
      `git status --porcelain` in the live worktree is unchanged by a run.
- [x] Per lane, plant the minimal *incident-class* break from D5 and assert the lane exits non-zero.
- [x] Per lane, assert the lane PASSes on the unbroken copy. A lane failing in both directions is a
      harness FAILURE, reported as such.
      **CORRECTION to this task's literal wording ("exits zero"): the clean direction exits `3`, not `0`.**
      The harness runs the REAL component via `agent-gate.sh --only <lane>`, and a PARTIAL run that found
      nothing exits **3** by design — the gate refuses to let a partial run be scripted into a green claim
      (`OVERALL=PARTIAL` ⇒ `exit 3`); a PARTIAL run with a failed component exits `1`. Asserting `0` would
      have been unsatisfiable and would have pressured the harness into retyping the lane's cargo command,
      which proves a command works, not that the lane fires. So the harness asserts exit `3` + SUMMARY
      status `PASS` for clean, and exit `1` + SUMMARY status `FAIL` for planted, requiring the two signals
      to agree.
- [x] Print, per lane, what was planted and what fired. Clean up the worktree on exit, including on failure.
- [x] **Beyond the task list: attribute each planted red to its plant.** A lane that broke for an unrelated
      reason yields an identical exit code and an identical SUMMARY line, so a bare red is not evidence.
      Each planted run's output must NAME the planted symbol; a red that does not is reported as
      `FIRED-UNATTRIBUTED` and fails the harness.
- [x] Opt-in only — **not** in `COMPONENTS`. Nightly `gate.yml` enrollment is deliberately out of scope
      (workflow change ⇒ #2910 registry enrollment); file it as a follow-up instead.
- [x] **Residual test gaps DECLARED, not left silent** — #3409 names every scenario the spec states and
      the code implements but no test drives (the FAIL halves of three fail-closed branches, the
      behavioural form of the manifest-gated case, the #1978-shaped isolation plant that would make the
      observation discriminating, the un-measured "clippy still records PASS" clause, the `--delta`
      registry assert, and the one clause of the round-18 jq port this corpus cannot discriminate). Same
      standard the `flight-tests` census applies to itself: a lane that omits coverage says so.

## 6. Observation + cost record (surface: `docs/reports/`)
- [x] Record the harness run: all four lanes observed firing, per-lane planted break, and the negative
      direction. This is the AC2 deliverable —
      `docs/reports/ah6-1699-feature-matrix-lanes.md` (observation RE-TAKEN at `3fbe5d2dd`, i.e. the shipped
      tree — the earlier `94833d510` run predated the flight-tests plant moving and was no longer
      reproducible with the committed harness; all four FIRED, each attributed to its planted symbol).
- [ ] Record per-component durations from the full-gate SUMMARY.
- [ ] Report the added full-gate wall time as `max(0, SIDE_total − MAIN_total)` from the gate of record's
      own per-component durations, and state that it was taken with build caches pruned SYMMETRICALLY.
      **SUPERSEDED INSTRUCTION:** this task originally mandated a baseline full run at the merge base versus
      the gate of record, run sequentially on one machine. It was attempted and ABANDONED mid-run — this box
      hosts five lane worktrees and sustained load 52–86 on 16 cores, so a four-component delta is not
      recoverable from two totals whose noise exceeds the delta. The replacement is load-independent because
      both lanes sit inside the same run under the same load. R7 in the spec now prescribes it.
- [x] Update the Full-gate row to name the added feature-matrix coverage; update the website page to match.
      (Done, and CORRECTED after the C audit: both artifacts had been left describing the SUPERSEDED
      behaviour — "whole suite"/"WHOLE package", `tests/*.rs`, and a deleted module-level `#![cfg]`
      derivation — while only the spec was rewritten for the two descopes.)
- [ ] Verify publication by grepping the **served** page for a phrase this change introduces (#3042). A zero
      count is not-yet-published, never "done".

## 7. Delivery
- [x] `--lite` green each fix round (summary-file redirect; `PASS|FAIL` probe only). 14 rounds run.
- [~] `rust-reviewer` + sanctioned `roborev-review.sh --agent … --model …` on the lite-green diff BEFORE the
      first full gate. This diff is code-bearing (bash + a report) — roborev certification is required, and
      the docs-only substitute does not apply.
      **PARTIAL, and deliberately not ticked.** roborev: **17 rounds** run through the sanctioned wrapper,
      every finding fixed, none waived. `rust-reviewer`: spawned and it never returned findings, including
      after a direct request — one of SIX subagents in this session that did work and went idle silently. So
      the Rust-review half of this task has NOT been satisfied and is recorded as a residual rather than
      quietly counted as done. Mitigating, but not a substitute: this diff is ~99% bash + markdown (the only
      Rust touched is three `#[cfg]`/`#[ignore]` attribute edits in test files), which is outside
      `rust-reviewer`'s subject matter, and those three edits were each reviewed by roborev.
      **Residual for the closer/lead: decide whether a Rust review is required for a diff with no
      meaningful Rust in it.** I am not making that call by ticking a box.
- [ ] PR body: both cost numbers, the harness observation, the two corrected premises, and the full SUMMARY
      block showing the four new components.
- [ ] `flow-closer` endgame: ONE full gate of record → C (spec-auditor) → final roborev → `premerge-assert`
      → `gh pr merge --auto --squash --delete-branch`.

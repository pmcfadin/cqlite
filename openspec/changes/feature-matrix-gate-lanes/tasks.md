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
- [ ] Register both names in `COMPONENTS` (`:2166`) and add dispatch arms. Do **not** add them to
      `LITE_COMPONENTS` or `DELTA_COMPONENTS`.
- [ ] Each lane: `RUSTFLAGS="-D warnings" cargo check --all-targets -p cqlite-core --no-default-features
      --features all-compression,<parquet|delta-scan>` — the other feature absent. `--all-targets` is
      load-bearing (D2); record why at the call site, citing #1978.
- [ ] Place both in the SIDE lane with their own `CARGO_TARGET_DIR` (divergent feature set ⇒ MAIN
      target-dir thrash, #2657). Neither needs fixtures ⇒ **not** in `DATASET_COMPONENTS`.
- [ ] No opt-out env var.

## 2. `legacy-heuristics` (surface: `scripts/agent-gate.sh`)
- [ ] Register in `COMPONENTS` + dispatch. Build half: `RUSTFLAGS="-D warnings" cargo build -p cqlite-core
      --features legacy-heuristics`.
- [ ] Execute half: derive the `--test` target set by grepping committed `cqlite-core/tests/*.rs` for
      `legacy-heuristics`; include `--lib`. **Fail closed on zero derived targets**, naming the derivation —
      never PASS, never SKIP.
- [ ] Run under the existing `check_no_unexpected_zero_tests` guard.
- [ ] Add to `DATASET_COMPONENTS` (several derived targets consume fixtures) so the existing preflight
      applies.
- [ ] SIDE lane, own `CARGO_TARGET_DIR` (feature set diverges from MAIN's).
- [ ] **Record the first-ever execution result.** If positively-gated tests fail, apply the D3.RISK ruling
      from Seam 1 — default recommendation (b): `#[ignore]` with a filed follow-up issue, lane lands green
      over the remainder. Never (c) compile-only.

## 3. `flight-tests` (surface: `scripts/agent-gate.sh`)
- [ ] Register in `COMPONENTS` + dispatch, SIDE lane, own `CARGO_TARGET_DIR`, in `DATASET_COMPONENTS`.
- [ ] Scope per D4/D6 on the measured number; run under the zero-tests guard; no opt-out env var.
- [ ] Leave `flight-query-semantics-oracle` functionally untouched — including its per-lane #3095 fixture
      SKIP predicates. Overlap is accepted deliberately (D4).

## 4. Structural registration self-test (surface: `scripts/tests/test_agent_gate_summary.sh`)
- [ ] For each of the four names assert: present in `COMPONENTS`, reachable in the dispatch table, printed
      by `--list`. Hermetic, no cargo, sub-second — it must stay affordable in `--lite`.
- [ ] Verify it is reached by `--lite` (via `tooling-tests`/`roborev-lints` as that script is currently
      wired) and that removing a name reds `--lite`. Demonstrate the red, do not assume it.

## 5. Planted-break harness (surface: `scripts/tests/test_agent_gate_feature_matrix_lanes.sh`, new)
- [ ] Create a throwaway `git worktree add --detach` copy; **never** mutate the live checkout. Assert
      `git status --porcelain` in the live worktree is unchanged by a run.
- [ ] Per lane, plant the minimal *incident-class* break from D5 and assert the lane exits non-zero.
- [ ] Per lane, assert the lane exits **zero** on the unbroken copy. A lane failing in both directions is a
      harness FAILURE, reported as such.
- [ ] Print, per lane, what was planted and what fired. Clean up the worktree on exit, including on failure.
- [ ] Opt-in only — **not** in `COMPONENTS`. Nightly `gate.yml` enrollment is deliberately out of scope
      (workflow change ⇒ #2910 registry enrollment); file it as a follow-up instead.

## 6. Observation + cost record (surface: `docs/reports/`)
- [ ] Record the harness run: all four lanes observed firing, per-lane planted break, and the negative
      direction. This is the AC2 deliverable.
- [ ] Record per-component durations from the full-gate SUMMARY.
- [ ] Measure the baseline full gate at the merge base and the gate of record on this branch, **sequentially,
      one gate at a time** (#2640), and post both totals. Do not sum per-component seconds to claim added
      wall time (D6).

## 7. Doctrine (surface: `CLAUDE.md`, website `agents-developing/gate-contract/`)
- [ ] Update the Full-gate row to name the added feature-matrix coverage; update the website page to match.
- [ ] Verify publication by grepping the **served** page for a phrase this change introduces (#3042). A zero
      count is not-yet-published, never "done".

## 8. Delivery
- [ ] `--lite` green each fix round (summary-file redirect; `PASS|FAIL` probe only).
- [ ] `rust-reviewer` + sanctioned `roborev-review.sh --agent … --model …` on the lite-green diff BEFORE the
      first full gate. This diff is code-bearing (bash + a report) — roborev certification is required, and
      the docs-only substitute does not apply.
- [ ] PR body: both cost numbers, the harness observation, the two corrected premises, and the full SUMMARY
      block showing the four new components.
- [ ] `flow-closer` endgame: ONE full gate of record → C (spec-auditor) → final roborev → `premerge-assert`
      → `gh pr merge --auto --squash --delete-branch`.

# Tasks: certified-tree-vs-merged-tree (issue #3650, SLICE 1 — base-staleness advisory)

Surface exercised by each task is named, per `openspec/config.yaml` rules.

## 1. The advisory script

- [ ] 1.1 Create `scripts/flow/base-staleness.sh` (executable, `set -uo pipefail`).
      **Surface:** the command itself — usable by hand for the standing triage rule
      ("is the fix for this red already on main and merely absent from my base?").
      - Resolve the merge-base (`git merge-base origin/main HEAD`), NOT `origin/main`'s tip (D4/#3392).
      - `N` = `git rev-list --count <merge-base>..origin/main`.
      - Diff paths = `git diff --name-only -z <merge-base>...HEAD` (**`-z`**, per #3229's path-normalisation
        invariant — no path-reading `git diff` without `-z`).
      - `M` = commits in that range touching (diff paths ∪ gate-global set).
      - Print the merge-base, the `origin/main` sha AND its commit date (D5).
      - Never fetch; never write; never mutate a ref.
- [ ] 1.2 Hard-code the gate-global list in ONE place, no env override (D1/#3312).
- [ ] 1.3 Output vocabulary (D2): `BASE-STALENESS:` prefix; verdicts
      `STALE-RECOGNISED` / `NO-STALENESS-RECOGNISED` / `UNMEASURED`; zero prints `0 RECOGNISED`;
      `NON-EXHAUSTIVE` lines on **every** run. No `PASS`, no `OK`, no `RESULT:` anywhere.
- [ ] 1.4 Exit codes (D3): `0` no-staleness, `4` stale, `5` unmeasured, `3` usage. State the
      **`UNMEASURED` MUST be treated as stale** consumer contract in the header.

## 2. Tests — `scripts/tests/test_base_staleness.sh`

- [ ] 2.1 Harness modelled on `scripts/tests/test_premerge_assert.sh`: `ok()`/`bad()` counters, one
      `mktemp -d` with a cleanup trap, `# --- Case N: <claim> ---` banners, a `=== base-staleness: N passed,
      M failed ===` tail that exits non-zero on any failure.
      **Surface:** the script's CLI, driven against synthetic git repos built in the temp dir.
- [ ] 2.2 Case: stale base with blast-radius churn → `STALE-RECOGNISED`, exit 4.
- [ ] 2.3 Case: up-to-date base → `behind 0 commits`, `NO-STALENESS-RECOGNISED`, exit 0.
- [ ] 2.4 Case: merge-base is used, not the base ref's tip (branch whose main advanced past its point).
- [ ] 2.5 **Case (motivating, pinned):** diff sharing NO path with a commit behind that touches
      `.config/nextest.toml` → `STALE-RECOGNISED`. This is PR #3362's shape and the case the narrow
      definition fails.
- [ ] 2.6 Case: unrelated churn only → counted in `N`, not in `M`, verdict `NO-STALENESS-RECOGNISED`.
- [ ] 2.7 Case (AC5, vocabulary): no run's output contains `PASS`, `OK`, or `RESULT:`.
- [ ] 2.8 Case (AC5): a zero blast radius prints `0 RECOGNISED`, never a bare `0`; `NON-EXHAUSTIVE` present.
- [ ] 2.9 Case: missing `origin/main` → `UNMEASURED`, exit 5, and output contains neither
      `NO-STALENESS-RECOGNISED` nor a bare blast-radius `0`.
- [ ] 2.10 Case: no merge-base → `UNMEASURED`, exit 5.
- [ ] 2.11 **Planted-mutant case** (D8, AC6) following `scripts/tests/test_ws0_perf_invocation_lint.sh:812-830`:
      copy the script, empty the gate-global set, assert case 2.5 reds against the copy — and assert the
      planted defect is genuinely the one described, so a bare red is not accepted as evidence.
- [ ] 2.12 Non-vacuity: assert the synthetic fixtures actually have the shape the cases claim (the
      self-consistency-assert idiom at `test_premerge_assert.sh:525-530`).

## 3. `premerge-assert.sh` integration — advisory only, NO verdict change

- [ ] 3.1 Resolve `base-staleness.sh` from the script's OWN directory, no env override (D7/#3312).
      **Surface:** `scripts/flow/premerge-assert.sh` stdout.
- [ ] 3.2 Print its finding on `PREMERGE: ADVISORY` lines. Never alter the exit code; an absent, failing,
      or `UNMEASURED` advisory is reported and non-fatal (D6).
- [ ] 3.3 **Retain** the three `PREMERGE: SCOPE` lines and the literal `#3650`; extend by one line pointing
      at the advisory.
- [ ] 3.4 Extend `scripts/tests/test_premerge_assert.sh`: advisory-printed case; broken-advisory-non-fatal
      case; extend Case 39 (`:842-867`) so the retained SCOPE wording stays pinned.

## 4. Gate wiring

- [ ] 4.1 Register `scripts/tests/test_base_staleness.sh` in `run_tooling_tests`
      (`scripts/agent-gate.sh:10385`), including the echoed command list at `:11935`.
      **Surface:** the full gate's `tooling-tests` component.
- [ ] 4.2 Confirm a failing assertion in the new suite makes `tooling-tests` — and the full gate — FAIL.
      Do NOT add it to `--lite` or `DELTA_COMPONENTS`.

## 5. Doctrine (same change, per CLAUDE.md)

- [ ] 5.1 `CLAUDE.md:1052-1062` — describe the advisory, its non-blocking slice-1 status, the
      `UNMEASURED`-is-stale contract, and the declared non-exhaustiveness. Keep the merge-result gap open
      and name slice 2's issue.
- [ ] 5.2 `scripts/flow/premerge-assert.sh:99-116` header residual 3 — same, and keep the residual.
- [ ] 5.3 `.claude/agents/flow-closer.md:210-216` and `.claude/skills/flow-address/SKILL.md:76` — same.
- [ ] 5.4 Do NOT edit `openspec/changes/archive/**` (historical).

## 6. Certification

- [ ] 6.1 `scripts/agent-gate.sh --lite` green each fix round, summary-file redirect (#2079).
- [ ] 6.2 `rust-reviewer` + sanctioned roborev (`scripts/flow/roborev-review.sh --agent codex --model
      gpt-5.6-sol`) on the lite-green diff, BEFORE any full gate (#2086). Push first.
- [ ] 6.3 Open PR with `Refs #3650` — **NOT `Closes`**: the issue stays open for slice 2.
- [ ] 6.4 `flow-closer`: ONE full gate of record → `spec-auditor` C → final roborev → `premerge-assert`
      → `gh pr merge --auto --squash --delete-branch`.
- [ ] 6.5 File slice 2 (merge-result gate mode + fail-closed enforcement + disclaimer update) as its own
      issue, and the dependency-closure blast radius as another. Reference both in the PR body.
- [ ] 6.6 Telemetry stamped with `--slice` (issue stays OPEN, `closed_at: null`) per #3550/#3559.

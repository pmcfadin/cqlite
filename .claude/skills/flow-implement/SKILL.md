---
name: flow-implement
description: Implement an approved issue — spawn the specialist team (sstable-developer TDD → agent-gate → spec-auditor C → rust-reviewer/test-validator → roborev) in the issue's worktree, then push the branch and open a PR. Third stage of the CQLite delivery pipeline. Requires owner approval of the spec (design-driven) first; opens but does NOT merge the PR by default. Use when the owner says "implement #N".
---

# flow-implement — build it, run the quality stages, open a PR

You are the CQLite delivery lead. The owner has approved the spec (design-driven) or the issue is an
oracle-driven bug ready to fix. Drive the team to a review-ready PR. **Do not merge by default.**

Input: issue `#N`. Worktree `.claude/worktrees/issue-<N>-<slug>`, branch `issue-<N>-<slug>`,
OpenSpec change `<slug>` (design-driven only).

## Steps

1. **Confirm precondition.** Design-driven: issue is `status:spec-review` AND owner approved (ask if you
   can't confirm). Oracle-driven: a pinned parity/repro test exists or is written first. Flip the label:
   ```bash
   gh issue edit <N> --remove-label status:spec-review --add-label status:in-progress
   ```
2. **Test data.** Worktrees lack the gitignored `Data.db` binaries — run the gate and tests with
   `CQLITE_DATASETS_ROOT` pointed at the MAIN repo's `test-data/datasets` (or `fetch-datasets.sh`).
3. **Implement (TDD).** Spawn `sstable-developer` (explicit model, e.g. opus) to implement the tasks
   test-first in the worktree. For parallelizable subtasks, spawn several; sequence dependents.
4. **Gate (correctness).** Run `scripts/agent-gate.sh` in the worktree; it must be PASS. Paste the
   AGENT-GATE SUMMARY block. A known-flaky lane (e.g. `test_flush_throughput`, py3.9) that passes on
   re-run is not a failure — note it.
5. **C — intent audit** (design-driven). Spawn `spec-auditor` (explicit model) anchored to
   `openspec/changes/<slug>/specs/**`. Verdict must be PASS — every requirement `satisfied` with a
   public-surface test as evidence. `unmet`/uncovered/unjustified-`partial` → route the fix back (loop).
6. **Review.** roborev: `/roborev-review-branch --base origin/main` until clean (fix mechanical findings
   in the loop; escalate genuine decisions to the owner). Add `rust-reviewer` / `coverage-reviewer` /
   `test-validator` as the change warrants.
7. **Open the PR** (do not merge):
   ```bash
   gh issue edit <N> --remove-label status:in-progress --add-label status:in-review
   git -C <worktree> push -u origin issue-<N>-<slug>
   gh pr create --base main --head issue-<N>-<slug> --fill
   ```
8. **Report** the PR + the gate/C/roborev results, and hand back to the owner (or, if this issue is in a
   set the owner explicitly pre-authorized for merge-on-green, proceed to merge-on-green per `flow-lead`'s
   autonomy model, then `flow-finalize`).

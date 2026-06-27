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
   can't confirm). Oracle-driven: a pinned parity/repro test exists or is written first. Set exactly one
   lifecycle label — clear ALL `status:*` first so the issue never carries two (oracle issues arrive at
   `status:ready`, design issues at `status:spec-review`):
   ```bash
   gh issue edit <N> --remove-label status:ready --remove-label status:spec-review \
     --remove-label status:addressing --remove-label status:in-review --add-label status:in-progress
   ```
   (`--remove-label` is a no-op for labels not present, so this is safe regardless of the starting state.)
   Set the Project `Status=In Progress` too. **Run the `flow-board` detection snippet FIRST** — it does
   `gh auth switch --user "$project_account"` so the project-capable account is active (the EMU flip
   otherwise makes `gh project item-edit` fail and the board write degrade to labels SILENTLY). If
   `have_project=1`, `gh project item-edit ... Status=In Progress`; if `have_project=0`, the label above
   is the fallback AND you MUST print the loud `⚠️ board unavailable …` warning so the owner knows the
   board will not reflect this claim.
2. **Ensure the worktree exists — and that you hold the claim.** Design-driven issues already have a
   pushed claim branch (established + re-read in `flow-activate`); reuse it. Oracle-driven issues skip
   `flow-activate`, so they run the claim protocol (D2) HERE: eligibility = `Ready` AND **no**
   `issue-<N>-*` branch on origin, then create + **push** the branch as the cross-machine lock, then
   re-read and proceed only as holder:
   ```bash
   wt=".claude/worktrees/issue-<N>-<slug>"
   git -C <repo-root> fetch origin -q
   if git -C <repo-root> worktree list | grep -q "$wt"; then
     :  # design-driven: worktree + pushed claim already exist (from flow-activate)
   else
     # oracle-driven: claim now. Refuse if another machine already holds the lock.
     if git -C <repo-root> ls-remote --heads origin "issue-<N>-*" | grep -q .; then
       echo "Already claimed on origin — do not work it; take the next item (or fetch to RESUME)."; exit 0
     fi
     git -C <repo-root> worktree add "$wt" -b "issue-<N>-<slug>" origin/main
     # UNIQUE claim commit so a same-base race gets distinct SHAs (a bare identical-SHA
     # push is a no-op success → both would win). Non-force push: colliding SHA is rejected.
     git -C "$wt" commit --allow-empty -m "claim issue-<N> $(hostname -s)-${RANDOM}-$$"
     git -C "$wt" push -u origin "issue-<N>-<slug>" || { echo "Push rejected — another holds the claim; back off."; exit 0; }
     gh issue edit <N> --add-assignee @me
     # re-read: proceed only if origin's branch tip is YOUR claim commit (you won the race)
     git -C <repo-root> fetch origin -q
     [ "$(git -C <repo-root> ls-remote --heads origin "issue-<N>-<slug>" | awk '{print $1}')" \
       = "$(git -C "$wt" rev-parse HEAD)" ] || { echo "Lost the race — back off."; exit 0; }
   fi
   ```
3. **Test data.** Worktrees lack the gitignored `Data.db` binaries — run the gate and tests with
   `CQLITE_DATASETS_ROOT` pointed at the MAIN repo's `test-data/datasets` (or `fetch-datasets.sh`).
4. **Implement (TDD).** Spawn `sstable-developer` (explicit model, e.g. opus) to implement the tasks
   test-first in the worktree. For parallelizable subtasks, spawn several; sequence dependents.
5. **Gate (correctness).** Run `scripts/agent-gate.sh` in the worktree; it must be PASS. Paste the
   AGENT-GATE SUMMARY block. A known-flaky lane (e.g. `test_flush_throughput`, py3.9) that passes on
   re-run is not a failure — note it.
6. **C — intent audit** (design-driven). Spawn `spec-auditor` (explicit model) anchored to
   `openspec/changes/<slug>/specs/**`. Verdict must be PASS — every requirement `satisfied` with a
   public-surface test as evidence. `unmet`/uncovered/unjustified-`partial` → route the fix back (loop).
7. **Review.** roborev: `/roborev-review-branch --base origin/main` until clean (fix mechanical findings
   in the loop; escalate genuine decisions to the owner). Add `rust-reviewer` / `coverage-reviewer` /
   `test-validator` as the change warrants.
8. **Open the PR** (do not merge). The claim branch is already on origin (pushed in step 2); this push
   just sends the implementation commits. Move the board to `In Review`:
   ```bash
   gh issue edit <N> --remove-label status:in-progress --add-label status:in-review
   git -C <worktree> push -u origin issue-<N>-<slug>
   gh pr create --base main --head issue-<N>-<slug> --fill
   # Board → In Review: GitHub's "Pull request linked to issue" built-in normally does this on PR open.
   # Also set it directly as a belt-and-suspenders: run the flow-board detection snippet first (it
   # switches to the project-capable account), then gh project item-edit ... Status=In Review when
   # have_project=1; on have_project=0 the label above suffices but print the loud ⚠️ board-unavailable warning.
   ```
9. **Report** the PR + the gate/C/roborev results, and hand back to the owner (or, if this issue is in a
   set the owner explicitly pre-authorized for merge-on-green, proceed to merge-on-green per `flow-lead`'s
   autonomy model, then `flow-finalize`).

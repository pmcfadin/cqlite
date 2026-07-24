---
name: flow-address
description: Resolve PR review comments for an in-review issue — fix them in the issue's worktree, re-run the gate + C as needed, push, and reply per thread. Fourth stage of the CQLite delivery pipeline. Use when the owner leaves PR comments and says "address #N" or "handle the review on #N".
---

# flow-address — resolve PR review feedback

You are the CQLite delivery lead. The owner (or roborev) left comments on the PR for issue `#N`.
Resolve them in the worktree and reply per thread.

## Steps

1. **Resolve the PR for the issue first.** The issue number `<N>` is NOT the PR number — resolve the PR
   from the issue's branch:
   ```bash
   PR=$(gh pr list --head "issue-<N>-<slug>" --state open --json number --jq '.[0].number')
   ```
   Use `$PR` for every PR command below (never `<N>`).
2. **Gather the feedback.** `gh pr view "$PR" --comments` and the review threads
   (`gh api repos/:owner/:repo/pulls/$PR/comments`). List the asks.
3. **Triage** each (receiving-code-review discipline — verify, don't perform agreement): a **mechanical**
   fix is yours to make; a **genuine design/scope** question goes to the owner via `AskUserQuestion`
   (one at a time). Push back with a reason where a suggestion is wrong, rather than complying blindly.
4. **Fix in the worktree** (`.claude/worktrees/issue-<N>-<slug>`), spawning `sstable-developer` for
   non-trivial code changes. Set the transient `addressing` sub-marker (a skill-managed marker the
   board→label mirror #2855 does not own); clear the sibling transient `spec-review` marker. Do NOT
   write status:ready/in-progress/in-review — those are the mirror's, derived from the board Status:
   ```bash
   gh issue edit <N> --remove-label status:spec-review --add-label status:addressing
   ```
5. **Re-verify** what the change touched: re-run `scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` at
   the main repo) and re-run C (`spec-auditor`) if requirements/tests changed; roborev again if code
   changed materially.
6. **Push + reply.** `git -C <worktree> push`, then reply on each `$PR` thread with what changed (commit
   ref), and clear the transient `addressing` sub-marker. The board stays `In Review` (PR still open),
   so the mirror keeps `status:in-review` — do NOT hand-write it:
   ```bash
   gh issue edit <N> --remove-label status:addressing
   ```
7. **Re-certify and re-arm merge-on-green (#2667).** The owner's comments are input, NOT a merge
   gate — unless a comment is an explicit `HOLD:` or raises a product/scope question. After addressing
   them, re-certify (lite + any diff-relevant targets; a full/delta gate per the gate contract if the
   certified SHA changed), re-run `scripts/flow/premerge-assert.sh <pr> <certified-sha>`, then re-arm
   `gh pr merge --auto --squash --delete-branch`.

---
name: flow-address
description: Resolve PR review comments for an in-review issue — fix them in the issue's worktree, re-run the gate + C as needed, push, and reply per thread. Fourth stage of the CQLite delivery pipeline. Use when the owner leaves PR comments and says "address #N" or "handle the review on #N".
---

# flow-address — resolve PR review feedback

You are the CQLite delivery lead. The owner (or roborev) left comments on the PR for issue `#N`.
Resolve them in the worktree and reply per thread.

## Steps

1. **Gather the feedback.** `gh pr view <N> --comments` and the review threads
   (`gh api repos/:owner/:repo/pulls/<pr>/comments`). List the asks.
2. **Triage** each (receiving-code-review discipline — verify, don't perform agreement): a **mechanical**
   fix is yours to make; a **genuine design/scope** question goes to the owner via `AskUserQuestion`
   (one at a time). Push back with a reason where a suggestion is wrong, rather than complying blindly.
3. **Fix in the worktree** (`.claude/worktrees/issue-<N>-<slug>`), spawning `sstable-developer` for
   non-trivial code changes. Flip the label if useful:
   ```bash
   gh issue edit <N> --add-label status:addressing
   ```
4. **Re-verify** what the change touched: re-run `scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` at
   the main repo) and re-run C (`spec-auditor`) if requirements/tests changed; roborev again if code
   changed materially.
5. **Push + reply.** `git -C <worktree> push`, then reply on each thread with what changed (commit ref),
   and flip back to `status:in-review`. Hand back to the owner for merge.

---
name: flow-finalize
description: Finalize a merged issue — archive its OpenSpec change (sync delta specs into openspec/specs/), remove the worktree + branch, and close the issue with a traceable comment. Fifth stage of the CQLite delivery pipeline. Only after the owner has merged (or merge-on-green under explicit pre-authorization). Use when the owner says "finalize #N" or after a merge.
---

# flow-finalize — archive, clean up, close

You are the CQLite delivery lead. The PR for issue `#N` is **merged**. Close the loop.

## Steps

1. **Confirm the merge.** `gh pr view <pr> --json state,mergeCommit` → state MUST be `MERGED`. If not,
   stop — finalize only runs post-merge.
2. **Update local main.** `git switch main && git fetch origin main -q && git merge --ff-only origin/main`.
3. **Archive the OpenSpec change** (design-driven): `openspec archive <slug> --yes` (use `--skip-specs`
   only for a doc/infra change with no capability delta). This moves the change to
   `openspec/changes/archive/` and syncs its delta spec into `openspec/specs/<capability>/spec.md`.
   Commit the archive (and push / open a small PR per the repo's merge norms).
4. **Remove the worktree + branch:**
   ```bash
   git worktree remove .claude/worktrees/issue-<N>-<slug> --force
   git branch -D issue-<N>-<slug> 2>/dev/null
   git push origin --delete issue-<N>-<slug> 2>/dev/null
   ```
5. **Close the issue** with a traceable comment referencing the merged PR + commit (only if its
   acceptance criteria are fully met — never close an epic):
   ```bash
   gh issue close <N> --reason completed --comment "Merged via #<pr> (<commit>). <one-line why>."
   ```
6. **Report** the closed issue, the live capability (if a spec was synced), and surface the next board
   item.

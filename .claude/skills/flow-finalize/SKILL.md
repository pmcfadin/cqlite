---
name: flow-finalize
description: Finalize a merged issue — archive its OpenSpec change (sync delta specs into openspec/specs/), remove the worktree + branch, and close the issue with a traceable comment. Fifth stage of the CQLite delivery pipeline. Only after the owner has merged (or merge-on-green under explicit pre-authorization). Use when the owner says "finalize #N" or after a merge.
---

# flow-finalize — archive, clean up, close

You are the CQLite delivery lead. The PR for issue `#N` is **merged**. Close the loop.

## Steps

1. **Confirm the merge.** `gh pr view <pr> --json state,mergeCommit` → state MUST be `MERGED`. If not,
   stop — finalize only runs post-merge.
2. **Update the root checkout's main.** Do NOT `git switch main` from a worktree — `main` is checked out
   in the repo root, so the switch is rejected. Operate on the root explicitly:
   ```bash
   git -C <repo-root> fetch origin main -q
   git -C <repo-root> merge --ff-only origin/main   # only if the root is on main; else just the fetch
   ```
   (Archiving + cleanup below run from the worktree / repo root as noted; they don't require local main.)
3. **Archive the OpenSpec change** (design-driven): `openspec archive <slug> --yes` (use `--skip-specs`
   only for a doc/infra change with no capability delta). This moves the change to
   `openspec/changes/archive/` and syncs its delta spec into `openspec/specs/<capability>/spec.md`.
   Commit the archive (and push / open a small PR per the repo's merge norms).
4. **Set the board to Done + release the claim.** The PR-merged / issue-closed server-side automation
   should already have moved the Project item to `Status=Done` (it fires even when you merge from the
   phone/web — no `flow-*` run needed); if it hasn't, set it yourself, else flip the `status:*` label in
   the fallback (the Project-vs-labels detection snippet is in `flow-board`):
   ```bash
   # gh project item-edit <item-id> --field Status --single-select-option-id <Done>   # when board present
   gh issue edit <N> --remove-label status:in-review --add-label status:done 2>/dev/null || true
   ```
   Releasing the claim = removing the `issue-<N>-<slug>` branch from origin (the cross-machine lock); the
   cleanup below does exactly that. After finalize, nothing for this issue may remain `In Progress`/`In
   Review` and no `issue-<N>-*` branch may remain on origin.
5. **Remove the worktree + branch (releases the claim lock):**
   ```bash
   git worktree remove .claude/worktrees/issue-<N>-<slug> --force
   git branch -D issue-<N>-<slug> 2>/dev/null
   git push origin --delete issue-<N>-<slug> 2>/dev/null   # deletes the origin claim lock
   ```
   Confirm the lock is gone: `git ls-remote --heads origin "issue-<N>-*"` returns nothing.
6. **Close the issue** with a traceable comment referencing the merged PR + commit (only if its
   acceptance criteria are fully met — never close an epic):
   ```bash
   gh issue close <N> --reason completed --comment "Merged via #<pr> (<commit>). <one-line why>."
   ```
7. **Report** the closed issue, the live capability (if a spec was synced), and surface the next board
   item.

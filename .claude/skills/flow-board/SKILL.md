---
name: flow-board
description: Report status across all in-flight CQLite delivery work — every open issue by lifecycle label with priority, stage, worktree, and PR/CI state — THEN surface and drive the single furthest-along item waiting on the owner (a green PR to merge, a spec to approve), or offer a pick-list. Use when the owner asks "where do things stand", "what's next", "unblock me", or "what needs me".
---

# flow-board — status + the one next thing

You are the CQLite delivery lead. Give the owner one view of the pipeline (from GitHub labels + PR/CI
state + worktrees), then surface and drive the **single** item waiting on them. Read-only render; the
unblock step acts only through the owner unless a set is pre-authorized for merge-on-green.

## Steps

1. **Issues by lifecycle:**
   ```bash
   gh issue list --state open --json number,title,labels,url --limit 100
   ```
   Bucket by `status:*`; read the `P?` label as priority. Flag drift (approved spec still
   `status:spec-review`; merged PR whose issue is still `status:in-review`).
2. **PRs + CI:**
   ```bash
   gh pr list --state open --json number,headRefName,title,reviewDecision,url --limit 100
   ```
   For each `issue-*` PR, check required CI; an `in-review` PR is only owner-actionable once CI is green.
3. **Worktrees:** `git worktree list` — confirm each in-flight issue maps 1:1:1:1 (issue ↔ worktree ↔
   change ↔ PR); flag orphans.
4. **Render the board** compactly: per issue → `#N (slug)  P?  status  PR/CI  worktree`.
5. **Surface ONE next thing.** Pick the furthest-along item waiting on the owner — in order: a
   green-CI PR to merge (Seam 2), a committed spec to approve (Seam 1), an addressing PR with replies.
   Drive that one (render the spec inline / show the PR), or — if nothing waits — offer a short
   pick-list of `status:ready` issues to `flow-activate`, highest priority first. Don't dump the whole
   backlog; show the one, mention the rest.

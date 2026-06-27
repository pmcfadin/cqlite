---
name: flow-board
description: Report status across all in-flight CQLite delivery work — render the shared GitHub Project claim board (item, status, assignee, priority) with drift + abandoned-claim reconciliation, then surface and drive the single furthest-along item waiting on the owner (a green PR to merge, a spec to approve), or offer a claim-aware pick-list. Use when the owner asks "where do things stand", "what's next", "unblock me", or "what needs me".
---

# flow-board — claim board + the one next thing

You are the CQLite delivery lead. Give the owner one view of the pipeline (from the shared GitHub
Project claim board + PR/CI state + worktrees), reconcile drift and reap abandoned claims, then surface
and drive the **single** item waiting on them. Read-only render; the unblock step acts only through the
owner unless a set is pre-authorized for merge-on-green.

## Project-or-labels detection (shared by all flow-* skills)

The board is a **GitHub Project (v2)** with a `Status` single-select
(`Backlog/Ready/In Progress/In Review/Done`). Reading/writing it needs the `project` token scope. Detect
once and degrade gracefully to the `status:*` label model (D6) — never block:

```bash
# project_owner / project_number identify the CQLite Delivery board (see setup-project-board.sh output).
project_owner="${CQLITE_PROJECT_OWNER:-pmcfadin}"
project_number="${CQLITE_PROJECT_NUMBER:-}"
have_project=0
if gh auth status 2>&1 | grep -q "'project'" \
   && [ -n "$project_number" ] \
   && gh project view "$project_number" --owner "$project_owner" >/dev/null 2>&1; then
  have_project=1
fi
# have_project=1 → use `gh project item-list/item-edit`; have_project=0 → use `status:*` labels + assignee.
```

When `have_project=0`, every Project read below is replaced by `gh issue list --label "status:*"` and
every Project write (`Status=...`) by the equivalent `status:*` label flip — the pipeline keeps working
on labels alone. The one-time fix is the owner's: `gh auth refresh -s project` + run
`test-data/scripts/setup-project-board.sh`.

## Steps

1. **Render the board.** If `have_project=1`, render from the Project:
   ```bash
   gh project item-list "$project_number" --owner "$project_owner" --format json --limit 200
   ```
   Show, per item: `#N (slug)  P?  Status  assignee  PR/CI  worktree`. Group by `Status`
   (`Backlog → Ready → In Progress → In Review → Done`); each `In Progress` item MUST show its assignee
   (the claiming session/owner). If `have_project=0`, fall back:
   ```bash
   gh issue list --state open --json number,title,labels,assignees,url --limit 100
   ```
   Bucket by `status:*`; read the `P?` label as priority; show assignee from `assignees`.
2. **PRs + CI:**
   ```bash
   gh pr list --state open --json number,headRefName,title,reviewDecision,url --limit 100
   ```
   For each `issue-*` PR, check required CI; an `In Review` PR is only owner-actionable once CI is green.
3. **Worktrees + claim branches:** `git worktree list` — confirm each in-flight issue maps 1:1:1:1
   (issue ↔ worktree ↔ change ↔ PR); flag orphans. List the claim locks on origin:
   ```bash
   git ls-remote --heads origin "issue-*"
   ```
   Each `issue-<N>-<slug>` branch on origin is an active claim.
4. **Reconcile + reap.** Cross-check the board against GitHub-side state:
   - **Drift:** a PR that is **merged** (or its issue closed) while the item is still `In Progress`
     (or `In Review`) → flag for transition to `Done` (the server-side automation should do this; if it
     hasn't, set it: `gh project item-edit ... --field Status --single-select-option-id <Done>` or flip
     the `status:*` label). Also flag an approved spec still `Ready`/`status:spec-review`.
   - **Abandoned claim (reaper):** an item that is `In Progress` whose `issue-<N>-*` origin branch has
     **no recent commits** — the claiming session likely died and leaked a stuck item. Check freshness:
     ```bash
     # newest commit date on the claim branch (origin); compare to "now - 24h" or your stale window
     git log -1 --format=%cI "origin/issue-<N>-<slug>" 2>/dev/null
     ```
     Surface each stale `In Progress` claim as **STALLED — reclaim or finish** (another machine can
     `git fetch` the branch to resume; or `flow-finalize`/abandon to release it). Do not silently steal a
     claim — surface it for the owner.
5. **Surface ONE next thing.** Pick the furthest-along item waiting on the owner — in order: a
   green-CI PR to merge (Seam 2), a committed spec to approve (Seam 1), an addressing PR with replies,
   then a STALLED claim to reclaim. Drive that one (render the spec inline / show the PR), or — if
   nothing waits — offer a short **claim-aware** pick-list: only items that are `Ready` AND have **no**
   `issue-<N>-*` branch on origin (already-claimed items are not offered) to `flow-activate`, highest
   priority first. Don't dump the whole backlog; show the one, mention the rest.

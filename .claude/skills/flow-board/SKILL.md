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
(`Backlog/Ready/In Progress/In Review/Done`). **The board `Status` field is the SOLE dispatch authority
(Path A, issue #1886).** `status:*` labels are decorative/non-authoritative — they are NOT a dispatch
fallback and MUST NOT be used to select or claim work. Reading/writing the board needs the `project`
token scope. Detect it once:

```bash
# project_owner / project_number identify the CQLite Delivery board (see setup-project-board.sh output).
project_owner="${CQLITE_PROJECT_OWNER:-pmcfadin}"
project_number="${CQLITE_PROJECT_NUMBER:-}"
project_account="${CQLITE_PROJECT_ACCOUNT:-pmcfadin}"
# CRITICAL: gh's active account silently flips to an EMU account (pmcfadin_sfemu) that lacks the
# `project` scope. A grep of `gh auth status` is NOT enough — it matches ANY logged-in account, so it
# reads true while the *active* account can't touch Projects, and every board write then degrades to
# labels SILENTLY. Force the project-capable account active before any board op (idempotent):
gh auth switch --user "$project_account" >/dev/null 2>&1 || true
gh auth setup-git >/dev/null 2>&1 || true
have_project=0
# The real gate is whether the now-active account can actually read the board — not a scope grep.
if [ -n "$project_number" ] \
   && gh project view "$project_number" --owner "$project_owner" >/dev/null 2>&1; then
  have_project=1
fi
# have_project=1 → use `gh project item-list/item-edit`. have_project=0 → board is UNREACHABLE: STOP.
```

**Path A: the board is the only authority — there is NO label dispatch fallback.** When `have_project=0`
the board is unreachable, and because `status:*` labels are decorative they are NOT a safe substitute for
selecting or claiming work (stale labels are exactly what caused the wrong-grabs). So on `have_project=0`:
**do not dispatch.** Print
`🛑 board unreachable (active gh account lacks 'project' scope) — CANNOT dispatch; status:* labels are decorative, not the queue. Fix auth first.`
and STOP. You MAY still render a read-only status view from labels for the owner, but no claim, no
selection, no "next thing" happens without the board. The one-time fix is the owner's:
`gh auth refresh -s project` on the `$project_account` + run `test-data/scripts/setup-project-board.sh`.

## Steps

1. **Render the board.** If `have_project=1`, render from the Project:
   ```bash
   gh project item-list "$project_number" --owner "$project_owner" --format json --limit 200
   ```
   Show, per item: `#N (slug)  P?  Status  assignee  PR/CI  worktree`. Group by `Status`
   (`Backlog → Ready → In Progress → In Review → Done`); each `In Progress` item MUST show its assignee
   (the claiming session/owner). If `have_project=0`, you may render a **read-only** status view from
   labels (NOT a dispatch source — see Path A note above):
   ```bash
   gh issue list --state open --json number,title,labels,assignees,url --limit 100
   ```
   Bucket by `status:*`; read the `P?` label as priority; show assignee from `assignees`. This view is
   informational only — no claim/selection happens without the board.
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
   nothing waits — offer a short **claim-aware** pick-list: only items whose **board `Status=Ready`** AND
   have **no** `issue-<N>-*` branch on origin (already-claimed items are not offered) to `flow-activate`,
   highest priority first. Selection is by **board `Status` only** — never by `status:ready` label.
   **An empty board Ready column means no work is ready → say so and STOP.** Do NOT fall back to the
   `status:*` label set to find more (near a release, Ready is *supposed* to drain to zero; dredging
   labels is the exact wrong-grab bug). Don't dump the whole backlog; show the one, mention the rest.

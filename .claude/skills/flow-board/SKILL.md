---
name: flow-board
description: Report status across all in-flight CQLite delivery work — render the shared GitHub Project claim board (item, status, assignee, priority) with drift + abandoned-claim reconciliation, then surface and drive the single furthest-along item waiting on the owner (a green PR to merge, a spec to approve), or offer a claim-aware pick-list. Use when the owner asks "where do things stand", "what's next", "unblock me", or "what needs me".
---

# flow-board — claim board + the one next thing

You are the CQLite delivery lead. Give the owner one view of the pipeline (from the shared GitHub
Project claim board + PR/CI state + worktrees), reconcile drift and reap abandoned claims, then surface
and drive the **single** item waiting on them. Read-only render; the unblock step acts only through the
owner unless a set is pre-authorized for merge-on-green.

> **GitHub API resilience:** `gh issue`/`gh pr`/`gh project` writes ride the **GraphQL** bucket, which
> throttles **separately** from REST (each 5k pts/hr, independent per-bucket windows). GraphQL exhaustion
> is **routine, not exotic** — fleet polling burns it (observed machine-wide on 2026-07-27). For a
> **comment or a PR create, SWAP TO REST immediately; do NOT "retry" a hard-exhausted bucket** — retrying a
> limit that resets on the hour is a hang, not a backoff:
> ```bash
> gh api -X POST repos/OWNER/REPO/issues/N/comments -F body=@/tmp/comment.md   # comment
> gh api -X POST repos/OWNER/REPO/pulls -f title=… -f head=… -f base=main      # PR create
> ```
> Never stall the board sweep on one exhausted bucket. This is an **API-endpoint swap for the identical operation only** — it is
> NOT a dispatch fallback: Path A (#1886) still holds, and selecting/claiming work from `status:*` labels
> remains forbidden regardless of which API bucket is throttled.
>
> **MERGE HAS NO REST FALLBACK — `PUT repos/OWNER/REPO/pulls/N/merge` is FORBIDDEN.** That endpoint merges
> **immediately**, bypassing the required-check wait that branch protection exists to enforce (#2433 —
> GitHub-enforced merge gate, `enforce_admins=true`). The only sanctioned merge is
> `gh pr merge --auto --squash --delete-branch`. **Merge is the ONE operation with no REST swap** — the
> REST fallback above is for *comments and PR creates*, never merges. `--auto` is
> **set-once/idempotent**, so on a GraphQL throttle **sleep past the reset window and re-arm** (a re-arm on
> an already-armed PR is a safe no-op) — and unlike a comment, a merge is never urgent enough to justify
> bypassing the check wait. Never substitute REST for a merge, and never merge to "unblock" a throttled
> bucket.

## Project-or-labels detection (shared by all flow-* skills)

The board is a **GitHub Project (v2)** with a `Status` single-select
(`Backlog/Ready/In Progress/In Review/Done`). **The board `Status` field is the SOLE dispatch authority
(Path A, issue #1886).** `status:*` labels are an **ENFORCED board-derived read-mirror** (#2855 —
`project-board-sync.yml` is their sole writer, and a drift detector FAILs the run on disagreement): they
are trustworthy for **cheap server-side candidate discovery** (narrowing), but they are **eventually
consistent (≤30-min lag) and are NEVER the dispatch/claim authority** — MUST NOT be used to select or
claim work. Reading/writing the board needs the `project` token scope. Detect it once:

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
the board is unreachable, and because `status:*` labels are only a lagging board-derived mirror (#2855)
they are NOT a safe substitute for selecting or claiming work (a stale mirror read is exactly what caused
the wrong-grabs). So on `have_project=0`:
**do not dispatch.** Print
`🛑 board unreachable (active gh account lacks 'project' scope) — CANNOT dispatch; status:* labels are a lagging read-mirror, not the queue. Fix auth first.`
and STOP. You MAY still render a read-only status view from labels for the owner, but no claim, no
selection, no "next thing" happens without the board. The one-time fix is the owner's:
`gh auth refresh -s project` on the `$project_account` + run `test-data/scripts/setup-project-board.sh`.

## Steps

1. **Cheap candidate discovery (#2855).** `status:*` is now an ENFORCED read-mirror of board Status
   (written only by `project-board-sync.yml`), so it is server-side filterable and cheap for
   *narrowing* the candidate set without pulling issue bodies or paginating every board item:
   ```bash
   gh issue list --state open --label status:ready --json number,title
   ```
   This is discovery only — it narrows candidates. It is **eventually consistent** (≤30-min mirror lag)
   and is **NEVER the dispatch/claim authority**: you MUST still confirm each candidate against the
   live board `Status` in step 6 and acquire the claim ref before working it.

   **The rule, plainly: labels NARROW candidates; the filtered board read + the claim ref DECIDE.** The lag
   is real and bites both ways — an owner-captured snapshot from 2026-07-27 had #3054 (skill format tables)
   at board `In Progress` and #3055 (flow doctrine) at `In Review` while **both** still carried
   `status:ready`, and two freshly-promoted `Ready` P0s (#3058, #3068) carried **no** label yet. So a lead
   who trusts the label can report an issue as available when an agent is already three stages into it,
   **and** miss a just-promoted P0 entirely. This is #2855 behaving as designed, not a bug.
2. **Render the board — FILTER SERVER-SIDE, never page the whole board.** If `have_project=1`, render
   from the Project (the authoritative view). On this 900+ item board an **unfiltered** `item-list`
   truncates and silently under-reports a column (it cost the owner a mis-reported Ready column on
   2026-07-27). The fix is a **server-side Projects filter**, not a different API: `--query` takes
   Projects filter syntax (`assignee:octocat`, `-status:Done`; **quote multi-word option names**), and the
   filtered read is exact, faster, and cheaper than the GraphQL `projectItems` path. Owner-verified on the
   live board: `status:Ready` returned 10 items in ~1.6s.
   ```bash
   gh project item-list "$project_number" --owner "$project_owner" --query 'status:Ready'         --format json -L 100
   gh project item-list "$project_number" --owner "$project_owner" --query 'status:"In Progress"' --format json -L 100
   gh project item-list "$project_number" --owner "$project_owner" --query 'status:"In Review"'   --format json -L 100
   ```
   Read each lifecycle column with its own filtered call. Do NOT switch to GraphQL `projectItems` to
   "avoid truncation" — that was a wrong diagnosis; and do NOT raise `-L` on an unfiltered list instead of
   filtering.
   **Pick order (owner ruling 2026-09-01, #3893): release-milestoned product items FIRST.** Offer or
   claim a delivery-tooling `Ready` item only when no product item is `Ready`, or the tooling item's body
   cites a blocking cause (false PASS / lane blocked > 1 h / recurred twice). Render the two groups
   separately so the split is visible.
   Show, per item: `#N (slug)  P?  Status  assignee  PR/CI  worktree`. Group by `Status`
   (`Backlog → Ready → In Progress → In Review → Done`); each `In Progress` item MUST show its assignee
   (the claiming session/owner). If `have_project=0`, you may render a **read-only** status view from
   the mirrored labels (NOT a dispatch source — see Path A note above):
   ```bash
   gh issue list --state open --json number,title,labels,assignees,url --limit 100
   ```
   Bucket by `status:*` (the #2855 board-derived mirror — accurate to within its ≤30-min sweep lag, never
   an authority); read the `P?` label as priority; show assignee from `assignees`. This view is
   informational only — no claim/selection happens without the board.
3. **PRs + CI:**
   ```bash
   gh pr list --state open --json number,headRefName,title,reviewDecision,url --limit 100
   ```
   For each `issue-*` PR, check required CI; an `In Review` PR is only owner-actionable once CI is green.
4. **Worktrees + claim refs:** `git worktree list` — confirm each in-flight issue maps 1:1:1:1
   (issue ↔ worktree ↔ change ↔ PR); flag orphans. List the claim locks on origin — the slugless
   fixed-name **claim refs** are now THE lock (#2665); the `issue-<N>-<slug>` branch is only PR plumbing:
   ```bash
   bash scripts/flow/claim.sh status               # active claim refs: refs/claims/issue-<N> + holder + age
   git ls-remote origin 'refs/claims/*'            # raw view of the per-issue lock namespace
   git ls-remote --heads origin "issue-*"          # legacy branch-locks (older workers) + PR heads
   ```
   **Two DISTINCT ref namespaces — never conflate them, or you will double-claim:**
   - `refs/claims/issue-<N>` — the **per-issue lock** (`claim.sh`, #2665). THE arbiter of who owns an issue.
   - `refs/lane-claims/<machine>/<issue>` — the **supervisor-authored LANE claim** (#2655/#2499, per-lane
     since #3393's ruling; the legacy `refs/machine-claims/<machine>` is still read only so a
     pre-ruling ref gets drained), stamped by
     `worker-supervisor.sh` via `claim-heartbeat.sh stamp`. It records which *machine* is busy and feeds the
     CI reaper's `should-reap` predicate. It is **NOT** an issue lock, and its presence/absence says nothing
     about whether a given issue is claimable.
   A `refs/lane-claims/` entry read as a per-issue claim (or vice versa) produces exactly the
   double-claim the ref lock exists to prevent.
   Each `CLAIM: STATUS issue=<N>` line is an active claim (holder machine/actor + age); a matching
   legacy `issue-<N>-<slug>` branch, if any, is that claim's PR head (or an old-fleet branch-lock).
4a. **Fleet view (issue #2089).** For each `In Progress` item, join the claim against the shared
   heartbeat refs — a cheap origin git ref, never a GitHub API call — to show which machine holds it and
   whether it is alive. (`scripts/flow/*.sh` blobs are mode `100644` — **always `bash`-prefixed**, never
   executed directly.)
   ```bash
   bash scripts/flow/claim-heartbeat.sh list
   ```
   This renders one line per machine: `machine  issue  ts  age` (e.g. `mbp-2  #2083  2026-07-06T18:03:11Z
   12m`). Join on `issue` against the board's `In Progress` rows and render alongside worktrees/claims:
   `#N (slug)  machine  heartbeat-age`. An `In Progress` item with **no** heartbeat row at all (never
   beat, or its ref was already cleared) is itself a signal — treat its claim-branch commit freshness as
   the only evidence until a beat appears. Ref layout + age-bucket semantics are documented in
   `scripts/flow/claim-heartbeat.sh`'s header — this skill only consumes `list`, never reimplements the
   parsing.
5. **Reconcile + reap.** Cross-check the board against GitHub-side state:
   - **Drift:** a PR that is **merged** (or its issue closed) while the item is still `In Progress`
     (or `In Review`) → flag for transition to `Done` (the server-side automation should do this; if it
     hasn't, set the **board `Status` only** — never hand-flip a `status:*` label, which the #2855 mirror
     owns and will revert):
     ```bash
     # `--field` is NOT a gh flag (verified gh 2.87.3 offers only --field-id). All four IDs are required:
     gh project item-edit --id <item-id> --project-id <project-id> \
       --field-id <status-field-id> --single-select-option-id <Done-option-id>
     ```
     The mirror follows on its next pass (Done → no board-derived label). Also flag an approved spec still
     `Ready`/`status:spec-review`.
   - **Abandoned claim (reaper) — deterministic rule (issue #2089).** This REPLACES the old "no recent
     commits" guesswork, which false-positived on long no-commit implementation phases and
     false-negatived on a push-then-idle machine. The rule now has two conditions, both required:
     ```bash
     # 1. heartbeat age for the claiming machine, from the fleet view (step 4a above):
     bash scripts/flow/claim-heartbeat.sh list   # age column for the issue's machine
     # 2. no open PR for the issue:
     gh pr list --state open --search "issue-<N>" --json number --jq 'length'
     ```
     Reap **only when**: heartbeat age > **4 hours** (the documented threshold —
     `scripts/flow/claim-heartbeat.sh`'s header is the single source of truth for this number; do not
     hardcode a different value here) **AND** there is no open PR for the issue. A fresh heartbeat with no
     PR yet is normal mid-implementation, not abandoned; a stale heartbeat with an open PR is a
     review-wait, not abandoned — neither alone triggers a reap.
     - **Reap = comment + clear, never delete work.** When both conditions hold:
       1. Post a traceable comment on the issue: which machine's claim is being reaped, the observed
          heartbeat age, and that no PR was open.
       2. Clear the assignee.
       3. Set board `Status` → `Ready`.
       4. Clear the dead machine's heartbeat ref: `bash scripts/flow/claim-heartbeat.sh clear <machine>`.
       5. **NEVER delete the `issue-<N>-*` branch if it carries commits.** The branch (PR plumbing) is
          preserved on origin exactly as-is; picking the issue back up means resuming that branch
          (`git fetch` + continue), not starting a fresh `flow-activate`. Only a branch with zero commits
          beyond its base (never actually started) is a candidate for removal, and even then prefer
          leaving it for the owner to clear explicitly.
       6. **The reaped claim ref is `claim.sh adopt`-eligible (#2665).** Leave the `refs/claims/issue-<N>`
          ref in place and note its current SHA (from `claim.sh status <N>`); the next worker adopts it
          via compare-and-swap — `bash scripts/flow/claim.sh adopt <N> --expect <that-sha>` — so a
          resurrected original holder loses the lease and detects the loss immediately (#2467/#2499). Do
          not `claim.sh release` a reaped-but-non-dead claim.
   - **Orphaned-endgame reaper — second deterministic rule (issue #2667/#2499).** The rule above
     protects **every** open-PR item as a review-wait, which makes the exact #2499 orphaned-endgame
     state (a closer that armed/parked a PR then vanished) permanently un-reapable. Close that blind
     spot deterministically: an open PR is a *review-wait* (protected) **only when it is still moving** —
     its head SHA advanced OR it has review activity **newer** than the staleness window. A **stalled**
     open PR is not protected. Check both ages:
     ```bash
     # head-SHA age: when the PR's tip commit last changed
     gh pr view <pr> --json commits --jq '.commits[-1].committedDate'
     # review activity age: newest review or PR comment
     gh pr view <pr> --json reviews,comments \
       --jq '[(.reviews[].submittedAt), (.comments[].createdAt)] | max // "none"'
     ```
     Trigger **only when**: claiming machine's heartbeat age > **4 hours** (same single-source threshold
     as above) **AND** there is an open PR whose **head SHA is unchanged > 4 hours** **AND** no review
     activity newer than 4 hours. This is the orphaned endgame: a certified PR sitting completed-but-unowned.
     - **Do NOT auto-adopt — surface it (owner-attention, not silent steal).** When all three hold:
       1. **Page the owner** via the repo-owned notify contract (#3119): e.g.
          `bash scripts/lib/gate-notify.sh --publish FAIL "orphaned endgame #<N>" "PR #<pr> head+review idle >4h; claim heartbeat stale — adopt-eligible"`.
          Best-effort — with no ntfy target configured it is a silent no-op; rely on the comment + surfacing.
          Never call `agent-notify` with a `--category` flag: upstream v1.1.0 has no such arm and swallows
          it, publishing the flag NAME as the title and a red page as a green priority-3 success.
       2. Post a traceable comment on the **PR**: the machine whose claim is stale, the head-SHA age,
          the review-activity age, and that the PR is now adopt-eligible.
       3. Mark the issue **adopt-eligible** exactly as the first reaper does — leave the
          `refs/claims/issue-<N>` ref in place, note its SHA from `claim.sh status <N>`; the next worker
          takes it via `bash scripts/flow/claim.sh adopt <N> --expect <that-sha>` (compare-and-swap, so
          a resurrected holder detects the loss). Do **not** clear the assignee, flip `Status`, or delete
          the branch here — the endgame may still be genuinely resumable; you are *surfacing* the orphan
          for owner/worker adoption, not reaping the work.
   - **Advertised collision window — a DETECTOR, not a reaper (issue #3436).** The two reapers above
     ask "is a claim abandoned". This asks the opposite question: **is an issue being worked with no
     claim at all, while the board advertises it as available?** Three facts, no heuristics — board
     `Status=Ready` AND a pushed `issue-<N>-*` branch on origin AND no `refs/claims/issue-<N>`:
     ```bash
     bash scripts/flow/advertised-collision-scan.sh          # exit 3 = at least one row reported; rows are prefixed `COLLISION-WINDOW:` because the three facts are a SIGNATURE of a window, not proof a collision is occurring
     ```
     Measured instance: #3393 ran 20+ commits in exactly this state after a legitimate
     release-on-finalize, with the board inviting a second claimant the whole time. **Report it, never
     act on it**: it deletes no ref, moves no board item and touches no branch. The remedy is the
     worker's — `claim.sh verify <N>`, then the documented `adopt` path — because only the session on
     that box knows whether it owns the branch. Surface every row in the render with the issue, the
     branch and the lane-lock state; **exit 1 is "none found OR not measurable", never a clean bill of
     health** (positive-detection only, per #3393's split ruling).
   - An item with a fresh heartbeat (age ≤ 4h) is **not** touched by either rule. An item with an open PR
     that is **still moving** (head advanced or review activity within 4h) is a live review-wait — surface
     it as in-review as normal. Do not silently steal a live claim.
6. **Surface ONE next thing.** Pick the furthest-along item waiting on the owner — in order: a
   green-CI PR to merge (Seam 2), a committed spec to approve (Seam 1), an addressing PR with replies,
   an **orphaned endgame** just flagged adopt-eligible by step 5 (a stalled certified PR the owner should
   adopt/merge), then an item just reaped by step 5 (now `Status=Ready`, ready to reclaim). Drive that one (render the
   spec inline / show the PR), or — if nothing waits — offer a short **claim-aware** pick-list: only items
   whose **board `Status=Ready`** AND
   have **no** `refs/claims/issue-<N>` claim ref and **no** legacy `issue-<N>-*` branch on origin
   (already-claimed items are not offered) to `flow-activate`, highest priority first. Step 1's
   `status:ready` mirror read may have **narrowed** the candidate set, but the final selection is by
   **board `Status` only** — the label is never the authority.
   **An empty board Ready column means no work is ready → say so and STOP.** Do NOT fall back to the
   `status:*` label set to find more — a mirror row the board does not confirm as `Ready` is stale by
   definition (near a release, Ready is *supposed* to drain to zero; dredging labels is the exact
   wrong-grab bug). Don't dump the whole backlog; show the one, mention the rest.

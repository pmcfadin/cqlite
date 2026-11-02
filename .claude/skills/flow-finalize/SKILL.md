---
name: flow-finalize
description: Finalize a merged issue — archive its OpenSpec change (sync delta specs into openspec/specs/), remove the worktree + branch, and close the issue with a traceable comment. Fifth stage of the CQLite delivery pipeline. Runs after the PR is merged — workers merge autonomously on green (gate PASS + C PASS + roborev clean), so finalize normally follows the worker's own merge. Use right after a merge (or when the owner says "finalize #N").
---

# flow-finalize — archive, clean up, close

You are the CQLite delivery lead. The PR for issue `#N` is **merged**. Close the loop.

## Steps

> **FIRST — is this a COMPLETED delivery or a SLICE? (issue #3550)** A **slice** is a merged PR that
> shipped part of an issue which **deliberately stays OPEN** (the shape the lead ruled correct on
> #3393). This skill's default path ends by **archiving the change, setting the board to Done, and
> CLOSING the issue** — every one of which is **WRONG for a slice**, and closing the issue is the
> named FORBIDDEN workaround #3550 exists to prevent. Do not run steps 3, 5 and 7 unmodified for a
> slice; see **"Slice deliveries"** below, which says exactly what to do instead. Deciding is
> mechanical, not a judgement call: if the merged PR declared `Closes #<N>` it is a **completed
> delivery**; if it deliberately did not, it is a **slice**.
> Compare by **URL**, never by number — an issue NUMBER is repository-scoped, so a PR closing
> `other-repo#<N>` would read as closing THIS repo's `#<N>` and you would close a deliberately-open
> issue. `delivery-telemetry.py` makes the same comparison the same way (issue #3550).
> ```bash
> # gh's built-in --jq takes NO --arg, so the issue url is passed through the ENVIRONMENT
> # and read with jq's env.U. (`gh pr view --jq --arg u "$u" '<expr>'` is a usage error:
> # --jq consumes "--arg" and the rest become surplus positionals.)
> export U=$(gh issue view <N> --json url --jq .url)
> gh pr view <pr> --json closingIssuesReferences \
>   --jq 'if [.closingIssuesReferences[].url] | index(env.U) then "COMPLETED" else "SLICE" end'
> # COMPLETED -> run every step as written
> # SLICE     -> follow "Slice deliveries" below
> ```

1. **Confirm the merge + capture the merged branch.** state MUST be `MERGED`; the cleanup in step 6 keys
   off the merged PR's **`headRefName`** (NOT a `issue-<N>-*` glob — see the #1162 guardrails below):
   ```bash
   gh pr view <pr> --json state,mergeCommit,headRefName
   # state MUST be MERGED; record headRefName as <merged-branch> for step 6.
   ```
   If not merged, stop — finalize only runs post-merge.
2. **Update the root checkout's main — but FIRST guard against a commandeered root.** A Codex/other
   session may have switched the shared root checkout onto its own branch. Do NOT `git switch main` /
   `git reset` it back (that yanks it from whoever owns it), and NEVER commit to it while it is off-main:
   ```bash
   repo_root=~/projects/cqlite
   root_branch=$(git -C "$repo_root" rev-parse --abbrev-ref HEAD)
   git -C "$repo_root" fetch origin main -q
   if [ "$root_branch" = "main" ]; then
     git -C "$repo_root" merge --ff-only origin/main
   else
     echo "⚠️ root checkout is on $root_branch, not main — will stamp main-only artifacts via a throwaway worktree (step 4), NOT the root."
   fi
   ```
   (Archiving + cleanup below run from the worktree; they don't require local main.)
3. **Archive the OpenSpec change** (design-driven) — **NOT for a SLICE delivery** (see the gate above:
   the change is unfinished and archiving strands the remaining slices): `openspec archive <slug> --yes` (use `--skip-specs`
   only for a doc/infra change with no capability delta). This moves the change to
   `openspec/changes/archive/` and syncs its delta spec into `openspec/specs/<capability>/spec.md`.
   Commit the archive (and push / open a small PR per the repo's merge norms).
4. **(Retired) Telemetry stamping.** Delivery telemetry was retired on 2026-11-02 (#4406): do not run
   `scripts/delivery-telemetry.py record` and do not open a ledger PR. `docs/reports/delivery-telemetry.jsonl`
   stays in the tree as history only. Step numbers are kept so existing references still resolve.
5. **Set the board to Done + release the claim.** — **NOT Done for a SLICE delivery** (see the gate
   above: use `Ready`/`In Progress`; Done hides an unfinished issue from dispatch). The PR-merged / issue-closed server-side automation
   should already have moved the Project item to `Status=Done` (it fires even when you merge from the
   phone/web — no `flow-*` run needed); if it hasn't, set it yourself:
   ```bash
   # Run the flow-board detection snippet first (it switches to the project-capable account — the EMU
   # flip otherwise makes this write fail silently), then set the board Status only.
   # `--field` is NOT a gh flag (verified gh 2.87.3 offers only --field-id); all four IDs are required:
   gh project item-edit --id <item-id> --project-id <project-id> \
     --field-id <status-field-id> --single-select-option-id <Done-option-id>   # when have_project=1
   ```
   Do NOT write any `status:*` label here. Done → no board-derived label (board→label mirror #2855:
   Backlog/Done carry none), and the mirror only reconciles OPEN issues, so a leftover label on the
   now-closed issue is irrelevant to discovery (`--state open`) and to the detector; a reopen fires
   the mirror's `issues: reopened` trigger and re-derives from board Status.
   Releasing the claim = deleting the CLAIM REF `refs/claims/issue-<N>` via `claim.sh release` (#2665 — the
   ref is THE cross-machine lock); deleting the `issue-<N>-<slug>` branch is plumbing cleanup of the merged
   PR head, not the lock. Both happen in step 6 below. After finalize, nothing for this issue may remain
   `In Progress`/`In Review`, and neither the claim ref nor an `issue-<N>-*` branch may remain on origin.
6. **Release the locks, THEN remove the worktree + branch via the guarded cleanup (plumbing).** Do NOT hand-glob
   `issue-<N>-*` or blindly `--force` — that destroyed an unrelated active claim on 2026-06-27 (the #1143
   incident: PR merged from `issue-1143-read-p99-regression`, glob also matched + deleted the separate
   active `issue-1143-scan-window-offload`). Use the guardrailed script instead — it targets ONLY the
   merged PR's branch, refuses on >1 lock for the issue (1:1:1:1 violation), and refuses to remove a
   dirty/unpushed worktree.
   **Release the locks FIRST, while the lane directory still exists** — the claim ref, the actual
   cross-machine lock (#2665), and the machine-local lane lock. The PR is merged, so the open-PR
   guard passes; do NOT use `--force` here (that is the reaper's path in `flow-board`, not finalize).
   **The ORDER is load-bearing and was wrong until #3436 roborev round 15**: the cleanup below deletes
   the worktree, and `lane-lock.sh release` proves you are the holder by walking up from your cwd
   INSIDE the lane — so releasing afterwards is refused as not-holder and leaks the record. Round 12
   wrote that warning into the comment below and left the steps in the old order, which is why a
   later review found it again: an instruction is not a sequence.
   ```bash
   bash scripts/flow/claim.sh release <N>   # deletes refs/claims/issue-<N> → CLAIM: RELEASED
   # ...and drop the MACHINE-LOCAL lane lock (#3436). THIS IS NOW REQUIRED, not cosmetic:
   # the lock files live in a lock root OUTSIDE the worktree (`$LANE_ROOT/.lane-locks`),
   # so removing the worktree does NOT delete the record. An earlier version of this note
   # said it did, which made a real release read as optional.
   # KEYED BY ISSUE FIXES THE SUBJECT, NOT THE IDENTITY (#3436, roborev round 12). It needs
   # no --lane-dir, so it cannot miss a lane locked under a non-default path -- but proving
   # you are the HOLDER resolves an identity by walking up from your cwd INSIDE the lane, and
   # the cleanup BELOW removes the worktree. Run from a deleted lane, release is
   # refused as not-holder, and the `|| true` below turned that into a silent leak: the record
   # outlives the lane and the next session for this issue meets a lock nobody can clear.
   # SO THIS BLOCK RUNS FIRST, from inside the lane. If the lane is already gone,
   # the sanctioned fallbacks are `--pid <durable-holder>` or `--force` (which only DELETES and
   # needs no identity). Do not suppress the failure, and verify the record is actually gone.
   # (#3436 FIX 8 --
   # measured before the lock root existed: `release 9600` printed
   # `RELEASED (already free) ... record=absent` rc=0 while the record sat in
   # `.claude/worktrees/issue-9600-slug/`, i.e. a finalize reported a successful release
   # having released nothing).
   # A lock left behind by a killed session is still not a leak: its holder reads DEAD-*
   # and the next acquire reclaims it automatically.
   # PASS THE LEASE (#3436, roborev round 24). `release --expect` exists precisely so a DELAYED
   # or DUPLICATED release cannot delete a NEWER lock: the five-component token is IDENTICAL
   # across a release-and-reacquire by the same process, so only the lease (`<token>#<nonce>`,
   # the record INCARNATION) tells the two apart. Adding the option and leaving the documented
   # caller without it fixes nothing in the flow that actually runs — a second finalize, or a
   # retried one, still deletes a live lock. Read the lease immediately before releasing; if the
   # record changed in between, the CAS refuses, which is the point.
   LEASE="$(bash scripts/flow/lane-lock.sh status <N> 2>/dev/null | grep -oE 'lease=[^ ]*' | head -1 | cut -d= -f2-)"
   if [ -n "$LEASE" ]; then
     bash scripts/flow/lane-lock.sh release <N> --expect "$LEASE"
   else
     # No lease means no record to protect — release plainly so a genuinely absent lock is still
     # cleaned up. `--expect ''` is refused by design, so it must not be passed through.
     bash scripts/flow/lane-lock.sh release <N>
   fi
   # NO `|| true` on either branch: a refused release is a LEAK, not noise
   # VERIFY, do not assume — `status` must report no record for this issue:
   bash scripts/flow/lane-lock.sh status <N>
   # confirm gone: `claim.sh status <N>` prints `CLAIM: STATUS none`.
   ```

   **Now remove the worktree + branch via the guarded cleanup (plumbing).** Do NOT hand-glob
   `issue-<N>-*` or blindly `--force`.
   ```bash
   # --confirm-unmerged: a squash-merge leaves the branch tip out of `main`; step 1
   # already verified PR state=MERGED, which IS the authority the flag stands for.
   # `scripts/flow/*.sh` blobs are mode 100644 (no +x) — ALWAYS invoke them via `bash`.
   bash scripts/flow/finalize-cleanup.sh --issue <N> --merged-branch <merged-branch> --confirm-unmerged
   # Add --dry-run first to preview. Exit codes: 0 ok · 2 multi-lock · 3 dirty/unpushed · 4 unmerged tip.
   ```
   On a non-zero exit the script changed nothing and surfaced why — resolve the 1:1:1:1 violation or the
   dirty worktree by hand; never force past it. Confirm the lock is gone afterward:
   `git ls-remote --heads origin "issue-<N>-*"` returns nothing.
   (Regression coverage: `scripts/flow/tests/finalize-cleanup.test.sh` encodes the #1143 scenario.)
   Then clear this machine's claim heartbeat so it doesn't linger on origin until `flow-board`'s 4h reap
   window (issue #2089):
   ```bash
   bash scripts/flow/claim-heartbeat.sh clear "$(hostname -s)"
   ```
7. **Close the issue** with a traceable comment referencing the merged PR + commit — **only** if its
   acceptance criteria are fully met, **never** for a SLICE delivery (see the gate at the top and
   "Slice deliveries" above: a slice's issue stays OPEN by design, and closing it here is the
   FORBIDDEN workaround of #3550), and never an epic:
   ```bash
   gh issue close <N> --reason completed --comment "Merged via #<pr> (<commit>). <one-line why>."
   ```
   **GitHub API resilience:** `gh issue close`/`gh issue comment` ride the **GraphQL** bucket, which
   throttles **separately** from REST (each 5k pts/hr, independent per-bucket windows). If GraphQL is
   exhausted, fall back to `gh api` REST (comment → `repos/OWNER/REPO/issues/N/comments`,
   close → `PATCH repos/OWNER/REPO/issues/N -f state=closed`). Never stall finalize on one exhausted bucket.
### Slice deliveries — what changes (issue #3550)

A slice finalizes the **delivery**, never the **issue**. Run steps 1, 2, 4 and 6 as written; the other
three are wrong for a slice, in the direction that destroys the thing the issue is protecting:

| step | completed delivery | **slice** |
|---|---|---|
| 3 — archive the OpenSpec change | archive it | **DO NOT.** The change is not finished; archiving it strands the remaining slices with no spec. |
| 4 — telemetry | retired (#4406) | retired (#4406) — nothing to stamp. |
| 5 — board `Status=Done` | set Done | **DO NOT.** Set `Status=Ready` (more slices to claim) or leave `In Progress` if you are continuing. Done on an unfinished issue hides it from dispatch. |
| 7 — **close the issue** | close it | **DO NOT CLOSE.** Comment instead, naming the slice shipped and what remains: `gh issue comment <N> --body "Slice shipped: #<pr> (<commit>) — <what landed>. Remaining: <what does not>. Issue stays OPEN by design (#3393 ruling)."` |

**Closing a deliberately-open issue because this skill's default path says to is exactly the
substitution #3550 forbids** — a tool's or a checklist's shape must never decide whether a problem is
recorded as solved. If you are unsure which kind you have, ASK the lead rather than closing.

8. **Report** the closed issue, the live capability (if a spec was synced), and surface the next board
   item.
9. **Reset before the next item (issue #2085).** This is the inter-issue compaction point. The ledger stamp
   (step 4) is the durable record — carry **zero prior-issue history** forward. Drop the retained board
   renders, gate summaries, roborev findings, PR body, and any Seam-1 spec render for this issue; route any
   durable cross-issue lesson to `MEMORY.md` / `process_improvements.md`, never the live window. Re-hydrate
   the **next** item from the **board alone** — the lead must be re-runnable from board + disk state at any
   point, so a session that clears N issues stays O(1 issue), not O(N).

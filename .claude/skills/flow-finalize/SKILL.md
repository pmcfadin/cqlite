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
4. **Stamp the telemetry ledger.** Write one record for this completed issue so the pipeline's
   self-improvement loop has data (schema + doctrine: `docs/reports/delivery-telemetry.schema.json`;
   tool: `scripts/delivery-telemetry.py`). GitHub-derived fields (issue/PR timestamps, priority,
   routing) are pulled live; the run counters are what YOU observed during this issue — supply honest
   values (a counter you did not observe is an error, never a fabricated `0`):
   ```bash
   python3 scripts/delivery-telemetry.py record \
     --issue <N> --pr <pr> --slug <slug> --routing design|oracle \
     --gate pass|fail|not-run --gate-runs <runs through the first PASS; don't re-run after a pass> \
     # `--gate not-run --gate-runs 0` (#3448) is the ONLY honest record when NO full gate of
     # record ran; the two are coupled both ways (not-run <=> 0) and neither flag is optional.
     # Add --slice (#3550) when the ISSUE deliberately stays OPEN and this PR shipped a SLICE of it:
     # it records closed_at: null and bounds cycle_time_s on the PR's mergedAt. The kind is decided by
     # replaying the issue TIMELINE to the PR's mergedAt (#3559): slice <=> the issue was OPEN at
     # mergedAt AND this PR closes NOTHING. So a slice STAYS stampable after its issue is closed or
     # reopened, and --slice is refused when the LAST `closed`/`reopened` event STRICTLY BEFORE
     # mergedAt is a `closed` -- the LAST one decides, so a close FOLLOWED by a reopen before the
     # merge leaves the issue open at mergedAt and is ACCEPTED (a deciding close COMPLETED the issue), when a state event falls in the SAME SECOND as
     # mergedAt (one-second resolution, so the tie is unmeasurable and is refused as that), or when
     # this PR declares it closes the issue (an ordinary completed delivery whose auto-close lands
     # after the merge — retry WITHOUT --slice once GitHub records the close). An issue open at
     # mergedAt stamped WITHOUT --slice is refused too. Where the tool CANNOT disprove your flag it
     # accepts it and says on stderr that the kind rests on YOUR assertion, not a measurement — read
     # that note, it is not noise. NEVER close the issue to satisfy the tool, and NEVER hand-append
     # to the JSONL: both are FORBIDDEN.
     --claim-collisions <rejected claim pushes> --rebase-events <rebases/conflict resolutions> \
     --roborev-findings <roborev findings raised> --rework <re-open / re-review rounds>
   # This skill NEVER invokes roborev (the closer owns the final pass, via the only sanctioned invocation
   # `scripts/flow/roborev-review.sh` — #2964). "roborev clean" in this ledger means that wrapper's
   # terminal `RESULT: PASS`; `NOTHING-TO-REVIEW` or FAIL is not clean and is not finalizable.
   # Land the ledger via a PR — `main` blocks direct pushes (#2433 branch protection: PR required,
   # enforce_admins=true). NEVER `git push`/`push origin HEAD:main` (rejected), and NEVER `git checkout`
   # in the shared root (a closer that switched root to a telemetry branch stranded it off main).
   # ALWAYS a dedicated telemetry-<N> worktree branched off origin/main:
   git -C ~/projects/cqlite fetch origin main -q
   git -C ~/projects/cqlite worktree add /tmp/cqlite-ledger-<N> -b telemetry-<N> origin/main -q
   cd /tmp/cqlite-ledger-<N>
   # IMPORTANT: `record` writes to the SCRIPT's repo ledger (the root checkout), NOT $PWD. After running
   # it, verify the new line landed in THIS worktree's docs/reports/delivery-telemetry.jsonl (move it here
   # if the tool wrote it to root) and leave the root checkout CLEAN.
   git add docs/reports/delivery-telemetry.jsonl
   git commit -m "chore(telemetry): record #<N> delivery (PR #<pr>)"
   git push -u origin telemetry-<N>
   gh api repos/pmcfadin/cqlite/pulls -f title="chore(telemetry): record #<N> delivery" \
     -f head="telemetry-<N>" -f base="main" \
     -f body="Telemetry-only ledger stamp for #<N> (PR #<pr>). Routed via PR — main blocks direct pushes." --jq '.html_url'
   cd ~/projects/cqlite && git worktree remove /tmp/cqlite-ledger-<N> --force
   # The telemetry PR merges once its own `required` check goes green. The ledger is a HOT append-only
   # file: on a rebase conflict, KEEP ALL lines (main's ledger + your new record) — never drop a peer's.
   # Do NOT block the code merge on the telemetry PR; if its CI is pending, hand its number back and
   # merge it centrally when green.
   ```
   `--routing` is required (it is never inferred); `--priority` defaults from the issue's `P?` label
   (pass it to override). `record` refuses a second stamp for the same issue (pass `--allow-duplicate` to
   override). The live ledger lives on `main`, reachable only via a PR (never a direct push).
   Confirm with `python3 scripts/delivery-telemetry.py lint`.
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
6. **Release the claim ref, then remove the worktree + branch via the guarded cleanup (plumbing).** Do NOT hand-glob
   `issue-<N>-*` or blindly `--force` — that destroyed an unrelated active claim on 2026-06-27 (the #1143
   incident: PR merged from `issue-1143-read-p99-regression`, glob also matched + deleted the separate
   active `issue-1143-scan-window-offload`). Use the guardrailed script instead — it targets ONLY the
   merged PR's branch, refuses on >1 lock for the issue (1:1:1:1 violation), and refuses to remove a
   dirty/unpushed worktree:
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
   Then **release the claim ref itself** — the actual cross-machine lock (#2665). The PR is merged, so the
   open-PR guard passes; do NOT use `--force` here (that is the reaper's path in `flow-board`, not finalize):
   ```bash
   bash scripts/flow/claim.sh release <N>   # deletes refs/claims/issue-<N> → CLAIM: RELEASED
   # ...and drop the MACHINE-LOCAL lane lock (#3436). Removing the worktree already
   # deletes the lock FILE, so this is for the audit line rather than correctness —
   # run it BEFORE the worktree removal above if you want the release recorded. A lock
   # left behind by a killed session is not a leak either: its holder reads DEAD-* and
   # the next acquire reclaims it automatically.
   bash scripts/flow/lane-lock.sh release <N> || true
   # confirm gone: `claim.sh status <N>` prints `CLAIM: STATUS none`.
   ```
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
| 4 — stamp telemetry | as written | add **`--slice`**. It records `closed_at: null` and bounds `cycle_time_s` on the PR's `mergedAt`. Since **#3559** the tool decides this by replaying the issue's **timeline** to the PR's `mergedAt`, so it now **ACCEPTS** `--slice` for an issue that is closed or reopened NOW but was open when your PR merged — the case that used to be unstampable. It REFUSES when the **last** `closed`/`reopened` event **strictly before** `mergedAt` is a `closed` (that delivery COMPLETED the issue; a later reopen does not change it — but a close FOLLOWED by a reopen *before* the merge leaves the issue open at `mergedAt` and IS accepted), when a state event falls in the **same second** as `mergedAt` (one-second resolution, so the tie is unmeasurable), and when **this PR declares it closes the issue** (`Closes #N` ⇒ completed delivery, whatever the issue's current state). A refusal therefore still means you classified wrong — but re-read *which* of those it names, and **never** close the issue to make the stamp succeed. |
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

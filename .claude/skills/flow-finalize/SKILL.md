---
name: flow-finalize
description: Finalize a merged issue — archive its OpenSpec change (sync delta specs into openspec/specs/), remove the worktree + branch, and close the issue with a traceable comment. Fifth stage of the CQLite delivery pipeline. Runs after the PR is merged — workers merge autonomously on green (gate PASS + C PASS + roborev clean), so finalize normally follows the worker's own merge. Use right after a merge (or when the owner says "finalize #N").
---

# flow-finalize — archive, clean up, close

You are the CQLite delivery lead. The PR for issue `#N` is **merged**. Close the loop.

## Steps

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
3. **Archive the OpenSpec change** (design-driven): `openspec archive <slug> --yes` (use `--skip-specs`
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
     --gate pass --gate-runs <runs through the first PASS; don't re-run after a pass> \
     --claim-collisions <rejected claim pushes> --rebase-events <rebases/conflict resolutions> \
     --roborev-findings <roborev findings raised> --rework <re-open / re-review rounds>
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
5. **Set the board to Done + release the claim.** The PR-merged / issue-closed server-side automation
   should already have moved the Project item to `Status=Done` (it fires even when you merge from the
   phone/web — no `flow-*` run needed); if it hasn't, set it yourself, else flip the `status:*` label in
   the fallback (the Project-vs-labels detection snippet is in `flow-board`):
   ```bash
   # If you must set it yourself, run the flow-board detection snippet first (it switches to the
   # project-capable account — the EMU flip otherwise makes this write fail silently):
   # gh project item-edit <item-id> --field Status --single-select-option-id <Done>   # when have_project=1
   gh issue edit <N> --remove-label status:in-review --add-label status:done 2>/dev/null || true
   ```
   Releasing the claim = removing the `issue-<N>-<slug>` branch from origin (the cross-machine lock); the
   cleanup below does exactly that. After finalize, nothing for this issue may remain `In Progress`/`In
   Review` and no `issue-<N>-*` branch may remain on origin.
6. **Remove the worktree + branch via the guarded cleanup (releases the claim lock).** Do NOT hand-glob
   `issue-<N>-*` or blindly `--force` — that destroyed an unrelated active claim on 2026-06-27 (the #1143
   incident: PR merged from `issue-1143-read-p99-regression`, glob also matched + deleted the separate
   active `issue-1143-scan-window-offload`). Use the guardrailed script instead — it targets ONLY the
   merged PR's branch, refuses on >1 lock for the issue (1:1:1:1 violation), and refuses to remove a
   dirty/unpushed worktree:
   ```bash
   # --confirm-unmerged: a squash-merge leaves the branch tip out of `main`; step 1
   # already verified PR state=MERGED, which IS the authority the flag stands for.
   scripts/flow/finalize-cleanup.sh --issue <N> --merged-branch <merged-branch> --confirm-unmerged
   # Add --dry-run first to preview. Exit codes: 0 ok · 2 multi-lock · 3 dirty/unpushed · 4 unmerged tip.
   ```
   On a non-zero exit the script changed nothing and surfaced why — resolve the 1:1:1:1 violation or the
   dirty worktree by hand; never force past it. Confirm the lock is gone afterward:
   `git ls-remote --heads origin "issue-<N>-*"` returns nothing.
   (Regression coverage: `scripts/flow/tests/finalize-cleanup.test.sh` encodes the #1143 scenario.)
   Then clear this machine's claim heartbeat so it doesn't linger on origin until `flow-board`'s 4h reap
   window (issue #2089):
   ```bash
   scripts/flow/claim-heartbeat.sh clear "$(hostname -s)"
   ```
7. **Close the issue** with a traceable comment referencing the merged PR + commit (only if its
   acceptance criteria are fully met — never close an epic):
   ```bash
   gh issue close <N> --reason completed --comment "Merged via #<pr> (<commit>). <one-line why>."
   ```
   **GitHub API resilience:** `gh issue close`/`gh issue comment` ride the **GraphQL** bucket, which
   throttles **separately** from REST (each 5k pts/hr, independent per-bucket windows). If GraphQL is
   exhausted, fall back to `gh api` REST (comment → `repos/OWNER/REPO/issues/N/comments`,
   close → `PATCH repos/OWNER/REPO/issues/N -f state=closed`). Never stall finalize on one exhausted bucket.
8. **Report** the closed issue, the live capability (if a spec was synced), and surface the next board
   item.
9. **Reset before the next item (issue #2085).** This is the inter-issue compaction point. The ledger stamp
   (step 4) is the durable record — carry **zero prior-issue history** forward. Drop the retained board
   renders, gate summaries, roborev findings, PR body, and any Seam-1 spec render for this issue; route any
   durable cross-issue lesson to `MEMORY.md` / `process_improvements.md`, never the live window. Re-hydrate
   the **next** item from the **board alone** — the lead must be re-runnable from board + disk state at any
   point, so a session that clears N issues stays O(1 issue), not O(N).

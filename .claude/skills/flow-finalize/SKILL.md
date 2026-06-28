---
name: flow-finalize
description: Finalize a merged issue — archive its OpenSpec change (sync delta specs into openspec/specs/), remove the worktree + branch, and close the issue with a traceable comment. Fifth stage of the CQLite delivery pipeline. Only after the owner has merged (or merge-on-green under explicit pre-authorization). Use when the owner says "finalize #N" or after a merge.
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
   git -C <repo-root> add docs/reports/delivery-telemetry.jsonl
   git -C <repo-root> commit -m "telemetry(#<N>): stamp delivery ledger" && git -C <repo-root> push
   ```
   `--routing` is required (it is never inferred); `--priority` defaults from the issue's `P?` label
   (pass it to override). `record` refuses a second stamp for the same issue (pass `--allow-duplicate` to
   override). The live ledger lives on `main`, so stamp it on the root checkout / via a tiny follow-up
   commit. Confirm with `python3 scripts/delivery-telemetry.py lint`.
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
7. **Close the issue** with a traceable comment referencing the merged PR + commit (only if its
   acceptance criteria are fully met — never close an epic):
   ```bash
   gh issue close <N> --reason completed --comment "Merged via #<pr> (<commit>). <one-line why>."
   ```
8. **Report** the closed issue, the live capability (if a spec was synced), and surface the next board
   item.

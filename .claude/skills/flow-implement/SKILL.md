---
name: flow-implement
description: Implement an approved issue — spawn the specialist team (sstable-developer TDD → agent-gate → spec-auditor C → rust-reviewer/test-validator → roborev) in the issue's worktree, then push the branch and open a PR. Third stage of the CQLite delivery pipeline. Requires owner approval of the spec (design-driven) first; opens but does NOT merge the PR by default. Use when the owner says "implement #N".
---

# flow-implement — build it, run the quality stages, open a PR

You are the CQLite delivery lead. The owner has approved the spec (design-driven) or the issue is an
oracle-driven bug ready to fix. Drive the team to a review-ready PR. **Do not merge by default.**

Input: issue `#N`. Worktree `.claude/worktrees/issue-<N>-<slug>`, branch `issue-<N>-<slug>`,
OpenSpec change `<slug>` (design-driven only).

## Steps

1. **Confirm precondition.** Design-driven: issue is `status:spec-review` AND owner approved (ask if you
   can't confirm). Oracle-driven: a pinned parity/repro test exists or is written first. Set exactly one
   lifecycle label — clear ALL `status:*` first so the issue never carries two (oracle issues arrive at
   `status:ready`, design issues at `status:spec-review`):
   ```bash
   gh issue edit <N> --remove-label status:ready --remove-label status:spec-review \
     --remove-label status:addressing --remove-label status:in-review --add-label status:in-progress
   ```
   (`--remove-label` is a no-op for labels not present, so this is safe regardless of the starting state.)
   Set the Project `Status=In Progress` too. **Run the `flow-board` detection snippet FIRST** — it does
   `gh auth switch --user "$project_account"` so the project-capable account is active (the EMU flip
   otherwise makes `gh project item-edit` fail and the board write degrade to labels SILENTLY). If
   `have_project=1`, `gh project item-edit ... Status=In Progress`; if `have_project=0`, the label above
   is the fallback AND you MUST print the loud `⚠️ board unavailable …` warning so the owner knows the
   board will not reflect this claim.
2. **Ensure the worktree exists — and that you hold the claim.** Design-driven issues already have a
   pushed claim branch (established + re-read in `flow-activate`); reuse it. Oracle-driven issues skip
   `flow-activate`, so they run the claim protocol (D2) HERE: eligibility = `Ready` AND **no**
   `issue-<N>-*` branch on origin, then create + **push** the branch as the cross-machine lock, then
   re-read and proceed only as holder:
   ```bash
   wt=".claude/worktrees/issue-<N>-<slug>"
   git -C <repo-root> fetch origin -q
   if git -C <repo-root> worktree list | grep -q "$wt"; then
     :  # design-driven: worktree + pushed claim already exist (from flow-activate)
   else
     # oracle-driven: claim now. Refuse if another machine already holds the lock.
     if git -C <repo-root> ls-remote --heads origin "issue-<N>-*" | grep -q .; then
       echo "Already claimed on origin — do not work it; take the next item (or fetch to RESUME)."; exit 0
     fi
     git -C <repo-root> worktree add "$wt" -b "issue-<N>-<slug>" origin/main
     # UNIQUE claim commit so a same-base race gets distinct SHAs (a bare identical-SHA
     # push is a no-op success → both would win). Non-force push: colliding SHA is rejected.
     git -C "$wt" commit --allow-empty -m "claim issue-<N> $(hostname -s)-${RANDOM}-$$"
     git -C "$wt" push -u origin "issue-<N>-<slug>" || { echo "Push rejected — another holds the claim; back off."; exit 0; }
     gh issue edit <N> --add-assignee @me
     # re-read: proceed only if origin's branch tip is YOUR claim commit (you won the race)
     git -C <repo-root> fetch origin -q
     [ "$(git -C <repo-root> ls-remote --heads origin "issue-<N>-<slug>" | awk '{print $1}')" \
       = "$(git -C "$wt" rev-parse HEAD)" ] || { echo "Lost the race — back off."; exit 0; }
   fi
   ```
3. **Test data.** Worktrees lack the gitignored `Data.db` binaries — run the gate and tests with
   `CQLITE_DATASETS_ROOT` pointed at the MAIN repo's `test-data/datasets` (or `fetch-datasets.sh`).
4. **Implement (TDD) — via subagents, NOT inline; tiered-gate loop (issue #1821).** You orchestrate; you
   do not read source, write code, or run the gate in your own context (that's what fills it up). Spawn
   `sstable-developer` (explicit `model: opus` — pinned models are inaccessible) to implement test-first in
   the worktree. For parallelizable subtasks spawn several; sequence dependents. Use `test-validator` for
   gate/failure triage and `Explore` for code search — keep raw file contents out of your context. The
   implementer runs the **fix-round loop below, in order**, and returns only a short summary + the LITE
   block (NOT the full SUMMARY) each round:
   1. Make the next test-first change.
   2. **Run `scripts/agent-gate.sh --lite`** (fmt + file-size + FULL-workspace clippy + blast-radius-scoped
      tests, ~1-5 min). It is the FAST ITERATION gate, NOT the gate of record; it emits a distinct
      `==== AGENT-GATE LITE SUMMARY ====` block that must NEVER be pasted as the full SUMMARY.
   3. If lite FAILs, fix and go to step 2. Repeat until lite is PASS and the change is complete.
   Do NOT run the full `scripts/agent-gate.sh` during the fix-round loop — that is step 6.
5. **Conditional internal review-first (issue #1821) — BEFORE the first full gate.** If the diff changes a
   `pub` item, touches >1 call site of a changed symbol, or adds a new surface, run an internal
   `rust-reviewer` pass (explicit `model: opus`) now and address its findings — this catches structural
   findings before a 12-25 min full-gate cycle is spent. **Skip this step** for mechanical/localized diffs.
   Re-run `scripts/agent-gate.sh --lite` after any review-driven change.
6. **Gate (correctness) — run the FULL `scripts/agent-gate.sh` EXACTLY ONCE before merge.** After the
   fix-round loop converges (and step 5, if applicable), run the FULL gate in the worktree; it must be
   PASS. **`--lite` NEVER replaces this** — the full `==== AGENT-GATE SUMMARY ====` block is the ONLY run
   that counts. Loop shape: `implement → lite (each round) → conditional review-first → lite → FULL gate
   ONCE → roborev → CI → merge`. Paste the
   AGENT-GATE SUMMARY block. A known-flaky lane (e.g. `test_flush_throughput`, py3.9) that passes on
   re-run is not a failure — note it.
   - **Gate PASS ≠ CI green** (L2, flow-meta #1310). The local gate does NOT run every CI lane (it uses
     pre-existing datasets and a subset of `--test` targets). When the change touches a **regenerate path,
     a fixture parser, or a fail-closed CI guard**, reproduce the **actual CI lane** locally before relying
     on the gate — regenerate sources from the live container → corpus gen → the lane's exact target (e.g.
     `compression-corruption-parity` = regenerate + require-fixtures; `parity-manifest` =
     `cargo test -p cassandra-parity --test corpus_audit_tests`). #1236 and #1199 both passed the gate then
     failed CI on lanes the gate never ran. (#1269 reconciles the gate's component set with the CI lanes.)
   - **Never gate a non-deterministically-regenerated source on a whole-file byte identity** — the BTI trie
     (`Partitions.db`/`Rows.db`) and `Statistics.db` are not byte-reproducible across regen runs. Gate the
     **semantic verdict** (the parity test), keep the empty/missing-verdict authoring check fail-closed
     (validation playbook, L1). Per-component binding is tracked in #1294.
7. **C — intent audit** (design-driven). Spawn `spec-auditor` (explicit model) anchored to
   `openspec/changes/<slug>/specs/**`. Verdict must be PASS — every requirement `satisfied` with a
   public-surface test as evidence. `unmet`/uncovered/unjustified-`partial` → route the fix back (loop).
8. **Review.** roborev: `/roborev-review-branch --base origin/main` until clean (fix mechanical findings
   in the loop; escalate genuine decisions to the owner). Add `rust-reviewer` / `coverage-reviewer` /
   `test-validator` as the change warrants. (If a roborev round drives a code change, re-run
   `scripts/agent-gate.sh --lite` to iterate, then the FULL gate once more before merge.)
9. **Open the PR.** The claim branch is already on origin (pushed in step 2); this push sends the
   implementation commits. Use a closing keyword (`Closes #<N>`) so merge auto-closes the issue:
   ```bash
   gh issue edit <N> --remove-label status:in-progress --add-label status:in-review
   git -C <worktree> push -u origin issue-<N>-<slug>
   gh pr create --base main --head issue-<N>-<slug> --fill   # ensure body has "Closes #<N>"
   # Board → In Review fires via GitHub's "Pull request linked to issue" built-in. Belt-and-suspenders:
   # run the flow-board detection snippet first (switches to the project-capable account), then
   # gh project item-edit ... Status=In Review when have_project=1; else the label above + loud ⚠️ warning.
   ```
10. **Terminal state — arm merge-on-green, then STOP.** The worker's terminal state for an issue is
   **PR-open + `agent-gate.sh` PASS + (design-driven) spec-auditor C PASS + roborev clean**. At that point
   you arm the merge-on-green mechanism and **end your turn** — there is no human merge click, and you do
   NOT poll the PR's own external CI. Steps:
   - **Check the manager's orders**: read the issue's `🧭 MANAGER <!-- MGR:... -->` comments. If the
     latest order is `HOLD: merge after #N`, keep merge-on-green **gated behind #N** (the manager sequences
     it); obey `ORDER`.
   - Rebase on current `origin/main`; resolve any conflict in your own worktree.
   - **Arm merge-on-green and END your turn — do NOT busy-poll CI.** Do not schedule repeated
     `ScheduleWakeup` cycles to watch the PR's cross-platform CI matrix after the work is done; that is the
     token bleed this doctrine forbids. Landing on green is delegated:
     - **Primary today — the manager-owned poller.** `main` has no required status checks (`contexts=[]`),
       so `gh pr merge --auto` would merge instantly against an empty check set (forbidden). Hand the PR
       off to the manager-owned poller/merge-engine, which gates on an explicit lane set and lands it on
       green. Log that you armed the poller path.
     - **Once required status checks are configured on `main`** — arm `gh pr merge --auto --squash
       --delete-branch` (GitHub lands it natively when the required checks pass, zero tokens). Log that path.
   - **`flow-finalize <N>`** runs on the merge event (archive any OpenSpec change, stamp the telemetry
     ledger, remove the worktree, delete the origin claim lock, close the issue with a traceable comment) —
     triggered by the merge, not by a CI busy-wait.
   Escalate to the owner (do NOT arm merge-on-green) only for: an unresolved roborev finding that's a
   genuine design call, a scope/product question, or anything outside this issue. Report the terminal state
   + gate/C/roborev summary + which merge-on-green path you armed.
   (`ScheduleWakeup` remains valid for genuinely external, harness-untracked state — just not for polling a
   PR's own CI after the work is complete.)

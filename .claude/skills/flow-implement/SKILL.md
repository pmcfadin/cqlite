---
name: flow-implement
description: Implement an approved issue — spawn sstable-developer (TDD) in the issue's worktree, run rust-reviewer + roborev on the lite-green diff BEFORE any full gate (review-first is default), open the PR, then hand the endgame (the ONE full gate → C → final roborev → merge-on-green → finalize) to a disposable flow-closer agent so gate/review churn never accretes in the lead session. Third stage of the CQLite delivery pipeline. Requires owner approval of the spec (design-driven) first. Use when the owner says "implement #N".
---

# flow-implement — build it, review it, open a PR, hand off the endgame

You are the CQLite delivery lead. The owner has approved the spec (design-driven) or the issue is an
oracle-driven bug ready to fix. Drive the team to a **reviewed, PR-open** state, then spawn `flow-closer`
to run the terminal stages in its own disposable context.

Input: issue `#N`. Worktree `.claude/worktrees/issue-<N>-<slug>`, branch `issue-<N>-<slug>`,
OpenSpec change `<slug>` (design-driven only).

## The loop (one design, read it end-to-end)

`implement (TDD) → lite (each round) → rust-reviewer + roborev on the lite-green diff (review-first,
DEFAULT) → fix (lite re-cert, scoped targets — never a full gate) → open PR → flow-closer {FULL gate
ONCE → C → final roborev → merge-on-green → finalize}`

Review runs **before** the first full gate (issue #2086), so the ONE full gate of record certifies
already-reviewed code exactly once, immediately pre-merge (issue #2087). Roborev fix rounds re-certify with
`--lite` + diff-scoped targets, never a full gate. The full gate, the C audit, the final roborev pass, and
the merge all run inside `flow-closer` (issue #2084) — the lead's context receives only its terminal packet,
never gate stdout or review churn.

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
     # design-driven: worktree + pushed claim already exist (from flow-activate).
     # Implementation starting IS a stage transition — refresh the heartbeat (#2089).
     scripts/flow/claim-heartbeat.sh beat <N>
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
     scripts/flow/claim-heartbeat.sh beat <N>   # FIRST beat — establishes the claim heartbeat (#2089)
   fi
   ```
3. **Test data.** Worktrees lack the gitignored `Data.db` binaries — run the gate and tests with
   `CQLITE_DATASETS_ROOT` pointed at the MAIN repo's `test-data/datasets` (or `fetch-datasets.sh`).
4. **Implement (TDD) — via subagents, NOT inline; tiered-gate loop (issue #1821).** You orchestrate; you
   do not read source, write code, or run the gate in your own context (that's what fills it up). Spawn
   `sstable-developer` (explicit `model: opus` — pinned models are inaccessible) to implement test-first in
   the worktree. For parallelizable subtasks spawn several; sequence dependents. Use `test-validator` for
   gate/failure triage and `Explore` for code search — keep raw file contents out of your context. The
   implementer runs the **fix-round loop below, in order**. **Capped return contract (issue #2080):** each
   round it returns EXACTLY the `==== AGENT-GATE LITE SUMMARY ====` block (~15 lines) + **≤5 lines of prose**
   (what changed, what's next) — never raw lite/gate output, full test logs, or diffs; it references file
   paths instead.
   1. Make the next test-first change.
   2. **Run `scripts/agent-gate.sh --lite` with the summary-file redirect** (issue #2079 — never stream raw
      gate stdout into a persistent context):
      ```bash
      AGENT_GATE_SUMMARY_FILE=/tmp/lite-<N>.txt \
        scripts/agent-gate.sh --lite > lite-<N>.log 2>&1 < /dev/null
      cat /tmp/lite-<N>.txt   # the complete LITE block (default recovery: .agent-gate-lite-summary.txt)
      ```
      Lite runs fmt + file-size + FULL-workspace clippy + blast-radius-scoped tests (~1-5 min). It is the
      FAST ITERATION gate, NOT the gate of record; its distinct `MODE: lite` block must NEVER be pasted as
      the full SUMMARY.
   3. If lite FAILs, fix and go to step 2. Repeat until lite is PASS and the change is complete.
   Do NOT run the full `scripts/agent-gate.sh` during the fix-round loop — that is the `flow-closer`'s single
   gate of record (step 7).
5. **Review-first — DEFAULT, BEFORE the first full gate (issues #2086/#2087/#2088).** On the **lite-green**
   diff, run `rust-reviewer` (explicit `model: opus`) **and** roborev
   (`/roborev-review-branch --base origin/main`, machine-configured agent) NOW — before any full gate — so
   the ONE full gate certifies already-reviewed code. **Skip review-first ONLY for a genuinely mechanical
   diff:** no `pub`-item change AND a single call site AND no new surface (the narrow inverse of the old
   conditional). When in doubt, review.
   - **Triage every finding per `docs/development/roborev-severity.md`.** **Blockers** (correctness,
     data-parity, no-heuristics violations, safety/unwrap-panic paths, wiring-evidence gaps, security, any
     stated acceptance criterion) are fixed now — each re-triggers `fix → --lite re-cert → re-review`.
     **Nits** (style/naming, comment/doc polish, test-robustness suggestions with no failing scenario) are
     **batched into ONE linked follow-up issue** (labeled, referencing the PR) opened at merge time and NEVER
     trigger a re-verify round. When in doubt, blocker.
   - **Scoped re-cert, never a full gate here (issue #2087).** A blocker fix that touches src re-certifies
     with `scripts/agent-gate.sh --lite` (blast-radius-scoped tests) + any diff-relevant parity/integration
     `--test` target — NOT a full gate. The single full gate of record runs once, immediately pre-merge, in
     the `flow-closer` (step 7). Lite re-certs are never the gate of record (their `MODE: lite` marker
     enforces that).
   - Add `coverage-reviewer` / `test-validator` as the change warrants. Escalate a genuine **design-call**
     finding to the owner (NEEDS-YOU) rather than deciding it.
6. **Open the PR** (reviewed, lite-green code). The claim branch is already on origin (pushed in step 2);
   this push sends the implementation commits. Use a closing keyword (`Closes #<N>`) so merge auto-closes
   the issue, then refresh the heartbeat (#2089):
   ```bash
   gh issue edit <N> --remove-label status:in-progress --add-label status:in-review
   git -C <worktree> push -u origin issue-<N>-<slug>
   gh pr create --base main --head issue-<N>-<slug> --fill   # ensure body has "Closes #<N>"
   scripts/flow/claim-heartbeat.sh beat <N>                  # PR-open stage transition
   # Board → In Review fires via GitHub's "Pull request linked to issue" built-in. Belt-and-suspenders:
   # run the flow-board detection snippet first (switches to the project-capable account), then
   # gh project item-edit ... Status=In Review when have_project=1; else the label above + loud ⚠️ warning.
   ```
   **GitHub API resilience:** `gh pr create` / `gh issue comment` ride the **GraphQL** bucket, which
   throttles **separately** from REST (each 5k pts/hr, independent per-bucket windows). If GraphQL is
   exhausted, fall back to `gh api` REST: PR create → `repos/OWNER/REPO/pulls`, comment →
   `repos/OWNER/REPO/issues/N/comments`, merge → `repos/OWNER/REPO/pulls/N/merge`. Never stall a pipeline
   step on a single exhausted bucket.
7. **Hand the endgame to `flow-closer` — the disposable per-issue closer (issue #2084).** Spawn
   `flow-closer` (explicit `model: opus`) once, passing `#N`, the worktree/branch, routing, the OpenSpec
   `<slug>` (design only), the open PR number, and `CQLITE_DATASETS_ROOT`. It owns the terminal stages in
   its **own** context so gate stdout, the C audit, and roborev churn never accrete in yours:
   1. Runs THE full `scripts/agent-gate.sh` **exactly once** — the ONLY gate of record — via
      `Bash run_in_background` with the summary-file pattern (issue #2079). **It NEVER idle-waits** on the
      gate: a subagent that idle-waits on a 12-25 min gate is killed by the 600s stall watchdog and orphans
      the gate process (#1855). The harness re-invokes it on gate exit; it reads the SUMMARY from the file.
   2. Spawns `spec-auditor` for **C** PASS (design-routed): every requirement `satisfied` with a
      public-surface test; `unmet`/uncovered/unjustified-`partial` blocks merge.
   3. Runs a **final roborev confirmation pass** (should be clean-on-arrival after step 5's review-first).
      Triage per `docs/development/roborev-severity.md`: mechanical blockers fixed inline by the closer;
      src-design blockers respawn a fresh `sstable-developer`; nits batched into the follow-up issue.
      **Any src change after the full gate INVALIDATES it** — the gate of record must postdate the final
      src change AND the final rebase, so the closer re-runs the full gate if either happened.
   4. Merges on green (`gh pr merge --squash --delete-branch`, worker-merges-own-PR model) — obeying any
      open `HOLD: merge after #N` — then runs `flow-finalize`.
   5. Returns ONLY a terminal packet: `{verdict, PR URL, summary-file path, C, roborev, ≤10 lines
      residual}`. Escalations (design-call finding, unmet requirement, scope/product question, work outside
      the issue) come back as `verdict: blocked` for the owner's NEEDS-YOU list — the closer holds the merge.
8. **Report.** Relay the closer's terminal packet — verdict, PR URL, the gate-of-record summary-file path,
   the C/roborev line, and any residual — to the owner. **Never read the raw gate log or roborev transcript
   into your context**; the terminal packet is all you retain (that is the whole reason the closer exists).

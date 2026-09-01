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

1. **Confirm precondition.** Design-driven: issue is `status:spec-review` AND (owner approved OR the
   `resume-dont-ask` label is present — a durable Seam-1 seal that stands in for a per-session approval, #2666).
   In an **attended** session you may ask when you can't confirm approval; in an **unattended** session NEVER
   ask — **park** per the #2666 park-and-resume protocol (post ONE structured question comment + add the
   `needs-decision` label + write a `blocked`/`reason: seam1-approval` marker + EXIT), never `AskUserQuestion`.
   Oracle-driven: a pinned parity/repro test exists or is written first. Clear the transient
   skill-managed sub-markers (`spec-review`/`addressing`) so the issue does not carry a stale one — do
   NOT write a `status:in-progress` label: the board→label mirror (#2855) derives it from the board
   Status you set, and reverts any hand-written board-derived label on its next pass:
   ```bash
   gh issue edit <N> --remove-label status:spec-review --remove-label status:addressing
   ```
   (`--remove-label` is a no-op for labels not present, so this is safe regardless of the starting state.)
   Set the Project `Status=In Progress` too. **Run the `flow-board` detection snippet FIRST** — it does
   `gh auth switch --user "$project_account"` so the project-capable account is active (the EMU flip
   otherwise makes `gh project item-edit` fail and the board write degrade to labels SILENTLY). If
   `have_project=1`, set the board Status (`--field` is NOT a gh flag — only `--field-id`):
   ```bash
   gh project item-edit --id <item-id> --project-id <project-id> \
     --field-id <status-field-id> --single-select-option-id <In-Progress-option-id>
   ```
   If `have_project=0`, the `--remove-label` cleanup above is all you can do AND you MUST print the loud
   `⚠️ board unavailable …` warning so the owner knows the board will not reflect this claim.
2. **Ensure the worktree exists — and that you hold the claim.** Design-driven issues already hold the
   claim ref + pushed branch (acquired in `flow-activate`); reuse them. Oracle-driven issues skip
   `flow-activate`, so they run the claim protocol (D2) HERE: `claim.sh` is the lock (the slugless
   fixed-name ref `refs/claims/issue-<N>`, #2665 — a slug-named branch is only PR plumbing). Acquire the
   ref FIRST, then create the worktree/branch:
   ```bash
   wt=".claude/worktrees/issue-<N>-<slug>"
   git -C <repo-root> fetch origin -q
   if git -C <repo-root> worktree list | grep -q "$wt"; then
     # design-driven: claim ref + worktree already exist (from flow-activate).
     # Implementation starting IS a stage transition — refresh the heartbeat (#2089).
     bash scripts/flow/claim-heartbeat.sh beat <N>
   else
     # oracle-driven: acquire the claim ref now. claim.sh does the atomic push +
     # re-read; a UNIQUE root commit means a different-slug or identical-base
     # competitor can no longer double-claim. Adopting a reaped claim instead?
     # Use: bash scripts/flow/claim.sh adopt <N> --expect <old-sha>.
     # RESUMING an issue whose issue-<N>-* branch outlived its claim ref (the
     # `reason=legacy-branch-lock ... claim-ref=free` refusal)? The sanctioned command:
     #   bash scripts/flow/claim.sh adopt <N> --expect none --reason resume-legacy-branch-lock:branch-outlived-claim
     # (#2945 — git's empty lease: still server-arbitrated, records who + why. The
     # --reason above is CONCRETE on purpose: a placeholder reason, or one still
     # carrying an unsubstituted <…>, is a usage error (exit 64)). The refusal
     # DIAGNOSES (branch +
     # claim-ref=free) but deliberately prints NO runnable command: an older-fleet
     # worker locks with the BRANCH only, so this adopt WOULD succeed against a lane
     # somebody is actively working. CONFIRM abandonment first — `claim-heartbeat.sh
     # should-reap <machine>` (age > 4h AND no open PR AND pid-dead if local), the board
     # Status, the branch/PR author. Never resume blind.
     # NEVER hand-craft a claim commit to get past the guard.
     if ! bash scripts/flow/claim.sh claim <N>; then
       echo "CLAIM LOST — another machine holds refs/claims/issue-<N>. Take the next item (or fetch to RESUME)."; exit 0
     fi
     # CLAIM HELD → worktree + branch (naming/PR plumbing, NOT the lock).
     git -C <repo-root> worktree add "$wt" -b "issue-<N>-<slug>" origin/main
     git -C "$wt" push -u origin "issue-<N>-<slug>"   # PR head — NOT the lock
     gh issue edit <N> --add-assignee @me
     bash scripts/flow/claim-heartbeat.sh beat <N>   # FIRST beat — establishes the claim heartbeat (#2089)
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
        bash scripts/agent-gate.sh --lite > lite-<N>.log 2>&1 < /dev/null
      cat /tmp/lite-<N>.txt   # the complete LITE block (default recovery: .agent-gate-lite-summary.txt)
      ```
      **Reader contract (#2874):** the exit code is primary, and before trusting the block's
      `RESULT:` confirm its `run-id:` line is the run you launched — the no-clobber guard can leave a
      foreign peer's block on a shared pinned path (unreachable with a unique path, but verify). On a
      `run-id` mismatch, read the sibling `/tmp/lite-<N>.txt.integrity-fail.*` / `logs:` bundle instead.
      **And only `PASS`/`FAIL` is a verdict (#3041):** the gate stamps
      `RESULT: INCOMPLETE (gate did not finish)` at launch, so if you poll rather than wait for exit,
      use the **RECORD grammar** `grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)'` — a bare `grep -q` on the
      bare `RESULT:` token matches that **liveness placeholder** and would read a just-launched run as a
      finished one, and an UNANCHORED form matches `RESULT: PASSENGER`. That grammar is for full/`--lite`/
      `--delta` ONLY: an **`--only <component>`** run demotes success to `RESULT: PARTIAL`, so polling it with
      the record grammar SPINS ON GREEN (#3750) — there the exit status (**3**) is primary, the fallback is
      `grep -qE '^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'`, and the component's VERDICT is a SEPARATE read
      (`bash scripts/gate-component-verdict.sh "$SUM" --mode only --component <name>`), because a completed run
      whose component SKIPped is not a pass. And **`--delta` is a THIRD mode with a THIRD set** — it
      alone can terminate `ERROR` or `REFUSED`, so polling a `--delta` re-cert with the record grammar
      hangs on a terminal outcome:
      `grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)([[:space:]]|$)'` (#3750).
      Lite's components are exactly `file-size fmt clippy roborev-lints scoped-tests` (the
      `scripts/agent-gate.sh` `LITE_COMPONENTS` array), where clippy is **per-package scoped** (#1844) and
      `scoped-tests` is blast-radius (touched package `--lib` + the diff's new `--test` targets). It is the
      FAST ITERATION gate, NOT the gate of record; its distinct `MODE: lite` block must NEVER be pasted as
      the full SUMMARY.
   3. If lite FAILs, fix and go to step 2. Repeat until lite is PASS and the change is complete.
   Do NOT run the full `scripts/agent-gate.sh` during the fix-round loop — that is the `flow-closer`'s single
   gate of record (step 7).
5. **Review-first — DEFAULT, BEFORE the first full gate (issues #2086/#2087/#2088).** On the **lite-green**
   diff, run `rust-reviewer` (explicit `model: opus`) **and** roborev NOW. **`scripts/flow/roborev-review.sh`
   is the ONLY sanctioned roborev invocation (#2964)** — there is no `/roborev-review-branch` slash
   command, and a bare `roborev review --branch --base origin/main` is **NON-SANCTIONED** (from a worktree
   it resolves against the ROOT checkout and enqueues `origin/main`, reporting clean having reviewed
   NOTHING), as is the two-positional commit-range form. **PUSH the implementation commit first** — the
   wrapper asserts the push and FAILs an unpushed branch, since unpushed work is itself an empty-diff
   cause. Pass **BOTH** `--agent` and `--model`; the wrapper requires them (#2433 — one alone inherits the
   `.roborev.toml`-pinned model and hard-400s as a silent-looking review outage):
   ```bash
   bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol
   ```
   Retain ONLY the `==== ROBOREV REVIEW SUMMARY ====` block, never the raw transcript (it goes to the
   `log:` path in the block). Exit `0` PASS / `1` FAIL / `3` NOTHING-TO-REVIEW / `2` usage error, and
   **any** non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a FAILED review round, never
   "roborev clean": fix the cause the block names (unpushed branch, mismatched `reviewed-sha`, vacuous
   verdict) and re-run. A **docs-only diff cannot be roborev-certified at all** — record primary-source
   verification in the PR instead. "docs-only" means a **code-free CENSUS**, never a `docs/` path prefix:
   `docs/reports/*-artifacts/` measurement harnesses are executable code that IS reviewed. Nothing
   predicts roborev's exclusion set pre-enqueue (deferred, #3283), so a swallowed path FAILs AFTER the
   round under `prompt-content:` — **if `prompt-content:` FAILs, suspect `.roborev.toml` first** (#3229).
   Four rules + evidence:
   https://pmcfadin.github.io/cqlite/agents-developing/roborev-findings/.
   Run review-first before any full gate — so
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
   git -C <worktree> push -u origin issue-<N>-<slug>
   gh pr create --base main --head issue-<N>-<slug> --fill   # ensure body has "Closes #<N>"
   bash scripts/flow/claim-heartbeat.sh beat <N>             # PR-open stage transition
   # Board → In Review fires via GitHub's "Pull request linked to issue" built-in. Belt-and-suspenders:
   # run the flow-board detection snippet first (switches to the project-capable account), then set the
   # board Status when have_project=1 (`--field` is NOT a gh flag — only --field-id):
   #   gh project item-edit --id <item-id> --project-id <project-id> \
   #     --field-id <status-field-id> --single-select-option-id <In-Review-option-id>
   # Do NOT write a status:in-review
   # label — the board→label mirror (#2855) derives it from the board Status; if have_project=0 print
   # the loud ⚠️ board-unavailable warning so the owner knows the board (and thus the mirror) is stale.
   ```
   **GitHub API resilience:** `gh pr create` / `gh issue comment` ride the **GraphQL** bucket, which
   throttles **separately** from REST (each 5k pts/hr, independent per-bucket windows). If GraphQL is
   exhausted, fall back to `gh api` REST: PR create → `repos/OWNER/REPO/pulls`, comment →
   `repos/OWNER/REPO/issues/N/comments`. Never stall a pipeline step on a single exhausted bucket.
   **MERGE HAS NO REST FALLBACK — `PUT repos/OWNER/REPO/pulls/N/merge` is FORBIDDEN**: it merges
   immediately, bypassing the required-check wait branch protection exists to enforce (#2433,
   `enforce_admins=true`). `gh pr merge --auto` is set-once/idempotent — on a throttle, sleep and retry it.
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
   4. After the pre-merge SHA assert + `HOLD` re-read, **arms auto-merge
      (`gh pr merge --auto --squash --delete-branch`) so GitHub owns the CI-green wait** (#2667; safe
      because #2433's `required` check + `enforce_admins` are live) — obeying any open `HOLD: merge
      after #N` — then runs `flow-finalize` (in-session when the required check is already green at arm
      time, else on a later wake confirming `state=MERGED`).
   5. Returns ONLY a terminal packet: `{verdict, PR URL, summary-file path, C, roborev, ≤10 lines
      residual}` (`verdict: auto-armed` when the merge is still pending GitHub's green). Escalations
      (design-call finding, unmet requirement, scope/product question, work outside the issue) come back
      as `verdict: blocked` for the owner's NEEDS-YOU list — the closer holds the merge.
8. **Report.** Relay the closer's terminal packet — verdict, PR URL, the gate-of-record summary-file path,
   the C/roborev line, and any residual — to the owner. **Never read the raw gate log or roborev transcript
   into your context**; the terminal packet is all you retain (that is the whole reason the closer exists).

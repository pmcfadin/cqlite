---
name: flow-address
description: Resolve PR review comments for an in-review issue — fix them in the issue's worktree, re-run the gate + C as needed, push, and reply per thread. Fourth stage of the CQLite delivery pipeline. Use when the owner leaves PR comments and says "address #N" or "handle the review on #N".
---

# flow-address — resolve PR review feedback

You are the CQLite delivery lead. The owner (or roborev) left comments on the PR for issue `#N`.
Resolve them in the worktree and reply per thread.

## Steps

1. **Resolve the PR for the issue first.** The issue number `<N>` is NOT the PR number — resolve the PR
   from the issue's branch:
   ```bash
   PR=$(gh pr list --head "issue-<N>-<slug>" --state open --json number --jq '.[0].number')
   ```
   Use `$PR` for every PR command below (never `<N>`).
2. **Gather the feedback.** `gh pr view "$PR" --comments` and the review threads
   (`gh api repos/:owner/:repo/pulls/$PR/comments`). List the asks.
3. **Triage** each (receiving-code-review discipline — verify, don't perform agreement): a **mechanical**
   fix is yours to make; a **genuine design/scope** question goes to the owner. In an **attended** session
   ask ONE at a time via `AskUserQuestion`. **`AskUserQuestion` is attended-sessions-ONLY (#2666)** — in an
   **unattended** session it is FORBIDDEN (the worker would hang until the log-tail watchdog pages it):
   instead **park** — post ONE structured question comment (options + recommendation + default), add the
   `needs-decision` label, write a `blocked` marker with `reason: needs-decision`, and **EXIT**, releasing
   the machine. Push back with a reason where a suggestion is wrong, rather than complying blindly.
4. **Re-acquire before the first edit — this stage RESUMES work, which is where the collision window
   reopens (#3436).** Addressing review comments is by definition restarting work on an existing branch,
   so the #3436 trigger applies verbatim: *"I am about to commit to a branch for an issue I do not
   currently hold"*. Do BOTH, before any fix:
   ```bash
   bash scripts/flow/claim.sh verify <N>       # on failure read reason=; see flow-implement step 2
   # From the SESSION's own cwd, never a subshell. `$(cd … && pwd)` computes only a path
   # and is REFUSED (reason=unresolved-identity, #3436 FIX 5); `( cd … && acquire )` is
   # worse — it SUCCEEDS while recording the subshell, which exits on return, so the
   # record reads DEAD-NO-PROCESS and a peer is granted the lane by auto-reclaim
   # (#3436 FIX 14). The cwd test finds a process working in the lane; it does not prove
   # the process outlives the command.
   cd <worktree> || exit 1
   bash scripts/flow/lane-lock.sh acquire <N> --lane-dir "$PWD" || exit 0
   ```
   **AND TAKE THE BOARD OFF `Ready` IN THE SAME BREATH (#3436 AC6).** Re-acquiring the claim
   closes the window for a session that reads the *ref*; the board is what a session reads
   *first*. #3393 ran 20+ commits with no claim ref **while the board advertised the issue as
   available**, so a well-behaved peer doing exactly what doctrine says — read the board, take a
   `Ready` item — would have collided, and the claim ref could not stop it because no ref existed.
   ```bash
   gh project item-list 1 --owner pmcfadin --query 'status:Ready' --format json -L 100 \
     --jq '.items[]|select(.content.number==<N>)|.id'      # empty => already off Ready
   # still Ready? set board Status=In Progress -- board Status ONLY, never a status:* label,
   # which the #2855 mirror owns and will revert.
   ```
   `advertised-collision-scan.sh` reports exactly this shape (Ready + a pushed branch + no claim
   ref), so leaving it unfixed on resume is a row someone else has to chase.
   A `released-then-resumed` refusal is NOT a stale lock and NOT an abandoned peer lane — it means the
   lane lock holds THIS SESSION's own token; take the documented `adopt --expect none --reason <why>`
   path. A `lane-occupied-by-live-peer` refusal is its opposite: a DIFFERENT live process on this box is
   in that lane — adopt nothing, reap nothing, stop. Same for a `lane-lock` `OCCUPIED` with
   `liveness=ALIVE`: do not edit. Only a verifiably DEAD holder is auto-reclaimed.
   `lane-lock=occupied-alive-unattributed` (or a `lane-evidence=…unattributed…`) means a live holder
   exists and the run could NOT establish whether it is you — run `lane-lock.sh verify <N>` from
   INSIDE the lane before writing anything.
5. **Fix in the worktree** (`.claude/worktrees/issue-<N>-<slug>`), spawning `sstable-developer` for
   non-trivial code changes. Set the transient `addressing` sub-marker (a skill-managed marker the
   board→label mirror #2855 does not own); clear the sibling transient `spec-review` marker. Do NOT
   write status:ready/in-progress/in-review — those are the mirror's, derived from the board Status:
   ```bash
   gh issue edit <N> --remove-label status:spec-review --add-label status:addressing
   ```
6. **Re-verify what the change touched — `--lite` per address round, NEVER a full gate here.** The tiered
   loop (#1821/#2087) gives each fix round `--lite`; the ONE full gate of record runs inside `flow-closer`
   (#2084). Always use the mandatory summary-file redirect (#1175/#2079) — never stream raw gate stdout
   into a persistent context:
   ```bash
   AGENT_GATE_SUMMARY_FILE=/tmp/lite-<N>.txt \
     bash scripts/agent-gate.sh --lite > /tmp/lite-<N>.log 2>&1 < /dev/null
   cat /tmp/lite-<N>.txt   # the LITE block (MODE: lite) is the ONLY gate text you retain
   ```
   Add any diff-relevant parity/integration `--test` target (run with `CQLITE_DATASETS_ROOT` pointed at
   the main repo). Re-run C (`spec-auditor`) if requirements/tests changed; if code changed materially,
   **push first**, then re-run roborev through the ONLY sanctioned invocation (#2964) —
   `bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol` (both flags always; never a bare
   `roborev review --branch`, which from a worktree reviews `origin/main` and reports clean having reviewed
   nothing). Retain only its `==== ROBOREV REVIEW SUMMARY ====` block; any non-PASS terminal `RESULT`,
   `NOTHING-TO-REVIEW` included, is a failed round, not clean.
   If the certified SHA moved, re-certification is the closer's full (or `--delta`) gate per
   the gate contract — not a full gate in this skill.
7. **Push + reply.** `git -C <worktree> push`, then reply on each `$PR` thread with what changed (commit
   ref), and clear the transient `addressing` sub-marker. The board stays `In Review` (PR still open),
   so the mirror keeps `status:in-review` — do NOT hand-write it:
   ```bash
   gh issue edit <N> --remove-label status:addressing
   ```
8. **Re-certify and re-arm merge-on-green (#2667).** The owner's comments are input, NOT a merge
   gate — unless a comment is an explicit `HOLD:` or raises a product/scope question. After addressing
   them, re-certify (lite + any diff-relevant targets), then re-run premerge-assert in the shape that
   matches what you changed (#3465) and re-arm `gh pr merge --auto --squash --delete-branch`:
   ```bash
   # A CODE fix moved the certified SHA -> it needs a NEW full gate at the new head:
   bash scripts/flow/premerge-assert.sh <pr> <certified-sha> <full-gate-summary>
   # A TEST/DOCS-ONLY fix on top of a full PASS at anchor X -> #1892's --delta re-cert,
   # never a repeat full gate; pass BOTH the anchor's full summary AND the delta summary:
   bash scripts/flow/premerge-assert.sh <pr> <certified-sha> <anchor-full-summary> <delta-summary>
   ```
   The third argument is REQUIRED and must always be a FULL-gate `RESULT: PASS` block; `--lite` is
   refused by name everywhere, and a `--delta` block is accepted ONLY as the optional fourth argument
   (where the script checks that its `delta-anchor:` names the third argument's block and that its own
   `commit:`/`tree-start:` are at the certified SHA). `--lite` is never the gate of record.
   Exit 0 proves the diff is unchanged since certification and that a full gate PASSed on that exact
   tree — **not** that it was certified against current `main` (a squash-merge composes the diff with
   main's tip; the merge-result gate is #3650 **slice 2**). Report it that way.
   Its `PREMERGE: ADVISORY` lines (#3650 slice 1) report `N` commits behind the merge-base and `M`
   of those in this diff's blast radius, measured at the **CERTIFIED SHA** rather than the local
   `HEAD` (paths the diff touches + a hard-coded gate-global set; every run declares TWO gaps — it is
   not a dependency closure, and the gate-global list is itself curated and NON-CLOSED). They are **information, never a verdict**: the advisory cannot change the exit
   code, an absent/failing/`UNMEASURED` advisory is non-fatal, and a consumer that ever acts on it
   must treat `UNMEASURED` as STALE rather than fresh.

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
4. **Fix in the worktree** (`.claude/worktrees/issue-<N>-<slug>`), spawning `sstable-developer` for
   non-trivial code changes. Set the transient `addressing` sub-marker (a skill-managed marker the
   board→label mirror #2855 does not own); clear the sibling transient `spec-review` marker. Do NOT
   write status:ready/in-progress/in-review — those are the mirror's, derived from the board Status:
   ```bash
   gh issue edit <N> --remove-label status:spec-review --add-label status:addressing
   ```
5. **Re-verify what the change touched — `--lite` per address round, NEVER a full gate here.** The tiered
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
6. **Push + reply.** `git -C <worktree> push`, then reply on each `$PR` thread with what changed (commit
   ref), and clear the transient `addressing` sub-marker. The board stays `In Review` (PR still open),
   so the mirror keeps `status:in-review` — do NOT hand-write it:
   ```bash
   gh issue edit <N> --remove-label status:addressing
   ```
7. **Re-certify and re-arm merge-on-green (#2667).** The owner's comments are input, NOT a merge
   gate — unless a comment is an explicit `HOLD:` or raises a product/scope question. After addressing
   them, re-certify (lite + any diff-relevant targets), then re-run premerge-assert in the shape that
   matches what you changed (#3465) and re-arm `gh pr merge --auto --squash --delete-branch`:
   ```bash
   # A CODE fix moved the certified SHA -> it needs a NEW full gate at the new head:
   bash scripts/flow/premerge-assert.sh <pr> <certified-sha> <full-gate-summary> --c-verdict AUTO
   # A TEST/DOCS-ONLY fix on top of a full PASS at anchor X -> #1892's --delta re-cert,
   # never a repeat full gate; pass BOTH the anchor's full summary AND the delta summary:
   bash scripts/flow/premerge-assert.sh <pr> <certified-sha> <anchor-full-summary> <delta-summary> \
     --c-verdict AUTO
   ```
   `--c-verdict` is REQUIRED and has no default (#3751): omitting it is exit 3, never a silent
   "C is not required". `AUTO` MEASURES whether C is required from the certified tree and reads
   the `c` stage's verdict; a design-routed branch whose C stage is absent or `NOT-RUN` is
   REFUSED. If addressing comments changed the SPEC deltas, C has to be re-run — re-open the
   stage (`review-stage.sh open c --issue <N> --agent spec-auditor --force`, which KEEPS the
   original clock) and read its `verdict` again.
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

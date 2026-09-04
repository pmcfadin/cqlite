# Delivery operating model — manager + flow-lead workers

Two roles. One board. The manager orchestrates; the workers do everything else.

## Roles

| | **Manager** (one window, `/manager`) | **flow-lead workers** (**one per machine**; N machines) |
|---|---|---|
| Writes code / claims / merges? | **Never by hand** (workers arm `gh pr merge --auto`; GitHub lands the fleet's PRs on green) | Yes — owns the issue end-to-end |
| Board | Controls **Ready** (what + order); reconciles; reaps | Reads Ready; claims the oldest unlocked item |
| Lifecycle | none | full **1:1:1:1**: claim → implement → lite → **review-first** (rust-reviewer + roborev) → open PR → **`flow-closer`** {FULL gate ONCE → C → final roborev → **merge-on-green**} → cleanup |
| Communication | signed **issue comments** (work orders) + Ready ordering | reads manager comments before acting; obeys the latest order |
| Tempo | sets it via Ready throughput, WIP cap, and ordering | runs flat-out on its claimed issue |

**Ready = the dispatch queue. A signed comment = a work order.** Those are the manager's only channels.

## Manager → worker comment protocol

Every manager order begins with a marker so workers parse orders, not human chatter:

```
🧭 **MANAGER** <!-- MGR:<id> -->
GO                      # cleared to run to completion
HOLD: merge after #N    # build + reach green, then block the merge until #N is merged
ORDER: k                # queue rank when several are Ready at once
<free-text / dependency notes>
```

`<id>` = a stable manager-session tag (host + short id). Workers obey the **latest** manager order.

## Worker lifecycle (flow-lead)

1. **Pick up**: take the oldest issue whose **board `Status=Ready`** with **no** `issue-N-*` lock on origin.
   For a cheap first pass you MAY *narrow* candidates with the enforced status mirror
   (`gh issue list --state open --label status:ready --json number,title` — server-side, no issue
   bodies, no board pagination; #2855), but the **selection decision is by board `Status` ONLY** (Path A,
   #1886: the board is the sole dispatch authority). The `status:ready` label is an enforced read-mirror
   of board Status written solely by `project-board-sync.yml`; it is eventually-consistent (≤30-min lag),
   so it only narrows — confirm the candidate's live board `Status=Ready` before working it, never treat
   the label as proof it is Ready/unclaimed. **Empty Ready → stop** (no work is ready; near a release
   Ready is meant to drain to zero). Board unreachable → STOP and fix auth, do not dispatch from labels.
   Claim it (`bash scripts/flow/claim.sh claim <N>` = the cross-machine lock, the slugless
   `refs/claims/issue-<N>` ref, #2665); `CLAIM HELD` wins, `CLAIM LOST` takes the next item.
2. **Read orders**: read the issue's manager comments. Note any `HOLD` / `ORDER` / instructions.
3. **Route — spec-first for new work**: design-driven / any new feature → run **`flow-activate` FIRST**
   (produces the OpenSpec proposal/design/specs/tasks, STOPS at Seam 1 for owner spec approval); no code
   until the spec is approved. Oracle-driven bug (Cassandra/sstabledump truth + pinned test) → straight to implement.
4. **Implement + review-first, then open the PR** (`flow-implement`) via subagents (worker orchestrates;
   `sstable-developer` model:opus implements TDD, iterating on `scripts/agent-gate.sh --lite` — it NEVER
   runs the full gate, #1855). On the lite-green diff, run **review-first by default** (rust-reviewer +
   roborev BEFORE any full gate, #2086); triage findings blocker/nit per `docs/development/roborev-severity.md`
   (#2088) — blockers fixed with `--lite` re-cert (#2087), nits batched into one follow-up issue — then open
   the PR. **Out-of-scope bug found** → a subagent files a new detailed issue (never fix it inline / never
   grow the diff); if it **blocks** completion, comment "blocked on #<new>" on your issue, pause, and surface
   to the manager (it sequences via `HOLD`/Ready) — fix it only as its own 1:1:1:1 claim.
5. **`flow-closer` runs the endgame (#2084) and merges on green.** `flow-implement` spawns a disposable
   per-issue `flow-closer` that runs the ONE full `scripts/agent-gate.sh` of record (via `run_in_background`
   + the summary-file pattern — it **never idle-waits**, which would trip the #1855 stall watchdog), the
   **C** intent audit (design), and the final roborev pass, then — after the pre-merge SHA assert + any
   `HOLD: merge after #N` obeyed — **arms auto-merge (`gh pr merge --auto --squash --delete-branch`) so
   GitHub owns the CI-green wait** (#2667), and returns only a terminal packet. Any src change after the
   full gate INVALIDATES it — the closer re-runs the gate if a fix or rebase postdates it.
   No worker CI busy-wait (`ScheduleWakeup`-polling a PR's own CI is prohibited).
   **Re-read the issue LIVE immediately before spawning the closer** (owner, 2026-08-03) — not from the
   last status tick. A manager order can land between a poll and the spawn (it has), and the closer is
   the one irreversible step in the pipeline, so it is the one step that must never run on stale
   instructions. See `process_improvements.md` (2026-08-03 / #3249, entry 4).
6. **Finalize follows the merge.** The merge event triggers `flow-finalize` (archive any OpenSpec change,
   **stamp the telemetry ledger** with the roborev blocker/nit split, remove the worktree, delete the origin
   claim branch + clear the heartbeat, close the issue with a traceable comment). Board → Done (built-in).
   The lead then **resets** — zero prior-issue carryover, next item re-hydrated from the board alone (#2085).

## Merge-on-green (how a green PR lands — `--auto`, no worker CI busy-wait)

A worker/closer never busy-polls its PR's own CI. After **local certification** (the gate of record PASS +
**C** PASS + roborev clean) and the pre-merge SHA assert + `HOLD` re-read, it **arms auto-merge and stops** —
GitHub owns the CI-green wait:

```bash
gh pr merge <pr> --auto --squash --delete-branch
```

GitHub lands the PR the instant the branch's **`required`** status check passes and auto-closes the issue
via `Closes #N`. This is the single default path — there is no manager-owned poller/merge-engine (that
mechanism was never built; it is gone).

**Why `--auto` is safe (#2433):** `main` has a real `required` status check + `enforce_admins=true` — **not**
an empty `contexts=[]` set. `--auto` therefore can never land a PR against an unchecked head, and there is no
admin bypass. Branch protection is the green-signal guard, enforced by GitHub itself.

**Finalize follows the merge across a possible session boundary (#2667).** Because `--auto` can complete
after the arming session exits, finalize (telemetry, board, claim release) runs on whichever wake observes
the merge:
- **Fast path (DEFAULT when the `required` check is already green at arm time):** the closer briefly confirms
  `gh pr view <pr> --json state -q .state` == `MERGED` (hard-deadline poll, not a tight loop) and finalizes
  in-session.
- **Deferred path (required check still pending at arm time):** the closer returns `verdict: auto-armed` and
  a **later wake / next session** confirms `state=MERGED` before running `flow-finalize`. The #2667 gate
  completion push-signal and GitHub's own auto-merge notification are the callbacks — the summary file is a
  push signal now, not a poll target.

**`ScheduleWakeup` is still valid** for genuinely external, harness-untracked state; what is forbidden is
using it to busy-poll a PR's own external CI after the work is complete.

## Post-gate delta re-certification (test/docs-only rounds, issue #1892)

The "full gate exactly once" rule (issue #1821) held on **zero** non-trivial issues in the #1889 retro:
every roborev/address round — usually a Low test-robustness or docs finding — re-triggered a full gate at
15–25 min each, adding zero signal for the delta (the scoped tests often don't even run the changed test
file). #1853 burned ~3 full-gate cycles and #1921 ~2 on test/docs-only polish rounds. The fix closes that
loophole **without** weakening the gate of record:

- **After a full-gate PASS at commit `X`**, if the subsequent diff `X..Y` touches **ONLY** what the
  re-cert can **EXECUTE** — rust cargo test code (`.rs` under `tests/` dirs, `*_test(s).rs`), python
  binding tests (`bindings/python/tests/`, run by the #1893 python tier), and/or docs (`*.md` anywhere;
  TOP-LEVEL `docs/`, `website/`) — re-certify with
  `scripts/agent-gate.sh --delta X --anchor-run-id <X's full-gate run-id>`
  (or `--anchor-summary-file <path to X's full SUMMARY>`, which reads the run-id and refuses if that file
  is not a full-gate PASS block). It runs file-size + fmt + the diff's changed test targets and emits a
  DISTINCT `==== AGENT-GATE DELTA SUMMARY ====` block (MODE: delta).
- **Fail-closed, executable-only scope:** anything else in `X..Y` (src, scripts, workflows, `Cargo.*`,
  config, test-data) makes `--delta` **REFUSE** and name the offending files — a production change always
  requires a fresh full `scripts/agent-gate.sh`. That refusal deliberately includes two *test* classes the
  delta components cannot execute (roborev job 1452): node `__test__/` files (scoped-tests only
  compile-checks `cqlite-node`, it never runs jest) and shell self-tests (`scripts/tests/*.sh`, run only by
  the full gate's tooling-tests) — an ALLOW there would mint a PASS DELTA block for an untested change.
  The delta is NOT the gate of record and can never substitute for the full gate on a production change.
- **PR evidence:** record BOTH artifacts — the anchor's full SUMMARY (the gate of record) AND the `X..Y`
  DELTA block. The DELTA block's markers and `gate-of-record:` line make it impossible to paste a delta run
  as a full SUMMARY.
- **Standing backstop (owner condition, 2026-07-04):** long-term quality is backstopped by the nightly
  full run on `main` — `.github/workflows/gate.yml` (deep-check) re-runs the FULL gate with
  `CQLITE_CLIPPY_FULL=1`, deeper than the local gate. The `--delta` doctrine references this backstop; a
  red nightly is the safety net for anything a delta round scoped past.

## Pipelining independent lanes (don't serialize on waits, retro #1889)

The lead pipelines near-independent issues instead of serializing on long waits (full gate 15-25 min, CI,
roborev round-trips):

- **(a)** While one lane's full gate / CI / roborev runs, the lead launches or advances other independent
  lanes — implementation + review stages overlap freely.
- **(b)** Merge-on-green is **armed per PR** (it lands when green) rather than blocking the queue on each
  PR's CI; the lead advances to the next lane after arming.
- **(c)** Full gates for different lanes are run **serially** by the lead — enforced mechanically
  (#2640): `CQLITE_GATE_MAX_CONCURRENCY=1` (pinned by `bootstrap-agent-machine.sh`) makes the #1825
  cap admit one full gate and the per-gate core budget give it full cores, and each gate derives
  `CARGO_BUILD_JOBS`/`--test-threads` from its slot count and runs under `taskpolicy`/`nice` so an
  overlap no longer oversubscribes the CPU. Only the full-gate step serializes; everything else
  overlaps. No manual `pgrep`-serialization needed.
- **(d)** Long waits use **scheduled wakeups**, never idle polling.

## Self-improvement loop (telemetry + retro)

The pipeline measures itself so improvement is data-driven, not anecdotal:
- **Sense** — at finalize, the worker stamps one record per delivery cycle (issue, pr) into the
  append-only ledger `docs/reports/delivery-telemetry.jsonl` (schema:
  `docs/reports/delivery-telemetry.schema.json`) via `scripts/delivery-telemetry.py record`. A
  reopened issue that ships more than once legitimately gets one record per shipped PR (issue #2314)
  — retro aggregation by issue treats such multi-cycle issues as multiple deliveries, not one. So does
  an issue that ships one or more **slices** while DELIBERATELY remaining open (issue #3550): stamp each
  with `--slice`, which records `closed_at: null` (the marker) and bounds `cycle_time_s` on the PR's
  `mergedAt` — the authoritative terminal timestamp of a slice — and `retro` reports those records as
  their own SLICE class, never as completed issues. `--slice` states what was true at DELIVERY time,
  which the issue's CURRENT state cannot decide (GitHub records an auto-close AFTER the merge, so an
  ordinary completed delivery and a late-stamped slice look alike). Since issue #3559 the tool decides it
  by replaying the issue's own TIMELINE to the PR's `mergedAt`, and the rule is a CONJUNCTION:
  **slice ⟺ the issue was OPEN at `mergedAt` AND this PR closes NOTHING.** Both halves are permanent —
  every auto-closing PR's issue was also open at `mergedAt`, because the close is recorded afterwards, so
  only the PR's own `closingIssuesReferences` separates "open because it is never closing" from "open
  because the close lands five seconds later" (a slice PR closes NOTHING). A slice is therefore stampable
  after its issue has been closed or reopened (this is what unblocked the three owed #3393 records
  #3407/#3429/#3467), and is REFUSED when the LAST `closed`/`reopened` event STRICTLY BEFORE `mergedAt` is a `closed` — the last one decides, so a close FOLLOWED by a reopen before the merge is ACCEPTED (an
  event in the SAME SECOND as the merge is unmeasurable at one-second resolution and is refused as that) —
  that delivery COMPLETED the issue, and a later reopen does not change it. `--slice` is an operator
  ASSERTION: the tool refuses it wherever it can be DISPROVED, and where it cannot be, the assertion
  stands. One residual is UNDECIDABLE and is not claimed: a completed delivery whose PR omits `Closes #N`
  and whose issue is closed BY HAND later is observationally identical to a genuine slice whose issue is
  completed later by another PR — both are open-at-`mergedAt`, close-nothing, closed-later — so the
  difference is intent, and doctrine (not mechanism) bounds it, since `flow-implement` mandates
  `Closes #<N>` in every PR body. Closing the issue to satisfy the tool (a tool's data
  model must never decide whether a problem is recorded as solved) and hand-appending a line past the
  validator are both FORBIDDEN. Records
  hold authoritative data only — GitHub-derived timestamps (cycle time + coarse phase durations) plus
  run-observed counters (claim collisions, rebase events, agent-gate pass/fail + run count, roborev
  findings, rework). A counter that was not observed is an error, never a fabricated `0`. A delivery
  where NO full gate of record ran is recorded as `gate: not-run` with `gate_runs: 0` (issue #3448) —
  a legal value, never a default, coupled both ways so the pair cannot tell two stories; `retro`
  reports those records as their own UNGATED class rather than folding them into gated passes.
- **Diagnose** — on a cadence (per-epic or weekly) the **manager** runs `delivery-telemetry.py retro`,
  which ranks the recorded failure categories by a documented weighted tally (deterministic, not an
  inferred model) and reports the single highest-cost recurring failure. `--file` files a `flow-meta`
  improvement issue, deduped against open `flow-meta` issues by a stable category marker.
- **Improve** — that `flow-meta` issue enters Ready and runs through the normal pipeline like any other.

The `delivery-telemetry` agent-gate component (SKIP-aware on `python3`) covers the tool: schema
round-trip, lint-rejects-malformed, fixture-ledger → expected top failure, and dedupe.

## Concurrency: MULTIPLE LANES PER MACHINE (#3393 owner ruling 2026-08-28, retracting #1930)

- **~~Exactly one flow-lead worker per machine~~ — RETRACTED.** #1930 (owner decision 2026-07-04) made
  one-worker-per-machine the doctrine default; **#3393 retracted it** because the fleet had not followed it
  all day (up to 4 lanes per box on standing instruction) and *leaving it written is what caused the defect
  it was meant to prevent*: the per-machine claim ref was designed one-ref-per-machine **because of this
  text**, so several lanes on one box overwrote each other's claim and a monitor could see at most one —
  which is why two of #3393's three silent lane deaths, both on one host, were structurally invisible.
  **Design for N lanes per box.** Each lane is claim-protocol-gated (`refs/claims/issue-<N>` per issue,
  `refs/lane-claims/<machine>/<lane-id>` for liveness); N *bare* sessions with no claim protocol remains
  the discouraged path.
- **What DOES still hold — one full gate at a time per machine.** That is a RESOURCE bound, not a
  worker-count invariant, and it is enforced mechanically (`CQLITE_GATE_MAX_CONCURRENCY=1`, #2640) rather
  than by counting workers. See the next bullet.
- **Full-gate concurrency = 1, always.** Never run 2+ full gates at once, regardless of the #1825 cap.
  The cap prevents SIGKILL under load but NOT timing flakes: two concurrent full gates flaked
  `mixed_p99_bounded_by_k_times_baseline` (`cqlite-core/tests/tail_latency_harness.rs`) under CPU
  oversubscription (#1625 core-tests: 693s solo → 87s + FAIL alongside a peer gate).
- **Slugless fixed-name claim ref (#2665).** Two sessions once claimed #1632 with *different slugs*
  (`issue-1632-parser-hardening` vs `issue-1632-parser-hardening-bundle`), so the exact-slug push never
  collided and the lock didn't fire; two sessions on an identical `origin/main` SHA likewise both saw an
  "up-to-date" success. The lock is now the slugless ref `refs/claims/issue-<N>` acquired via
  `scripts/flow/claim.sh` — a unique root-commit push git arbitrates server-side, slug- and base-independent.
  Pre-claim, check both the ref (`claim.sh status <N>`) AND the legacy `issue-<N>-*` branch glob
  (`git ls-remote --heads origin "issue-<N>-*"`) → skip if either is present.
- **Cross-machine coordination is unchanged:** one-worker-*per-machine* composes with multiple machines —
  each machine runs one worker; the origin `refs/claims/issue-<N>` ref coordinates across machines (the
  `issue-<N>-<slug>` branch is now PR plumbing). The claim lock is not load-bearing *within* a machine.

## Merge sequencing (why HOLD exists)

The claim-lock prevents two agents on one file; it does nothing for cross-cutting `mod.rs`/`lib.rs`
re-export conflicts (e.g. 18 concurrent #1116 splits). The manager sequences by **Ready ordering** and
**`HOLD: merge after #N`** so dependent or conflict-prone work lands in a safe order. Workers rebase on
the current `origin/main` before merging; if a rebase conflicts, the worker resolves it in its own
worktree (the manager never rebases someone else's branch).

## Human seams (unchanged)
- **Seam 1 — spec approval**: design-driven issues stop after `flow-activate` for owner approval.
- **Exceptions / product calls**: scope, epic close, conflicting requirements → manager surfaces a
  **NEEDS-YOU** list; never decided autonomously.
- Workers otherwise **arm `gh pr merge --auto` and stop**; GitHub lands the PR when the `required` check
  goes green (#2433/#2667). There is no human merge click for worker-owned issues — and no worker CI busy-wait.

## Hard rules
- The gate is the only run that counts; paste its summary block.
- Worktrees only; the claim ref (`claim.sh`, `refs/claims/issue-<N>`) is the lock — the branch push is PR plumbing; stage explicit paths.
- EMU guard every board op: `gh auth switch --user pmcfadin && gh auth setup-git`.
- roborev runs ONLY through `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>
  [--repo <abs-path>] [--base <ref>]` (#2964; fleet form `--agent codex --model gpt-5.6-sol`). **Both flags
  ALWAYS** — the wrapper rejects a missing one as a usage error. Push the branch first. NON-SANCTIONED
  direct forms: `roborev review --branch` **without an explicit `--repo`** (from a worktree it resolves
  against the ROOT checkout), the two-positional commit-range form (range base = git's empty tree), and a
  single-SHA review (reviews ONE COMMIT, not the branch — a partial review whose sha equals HEAD). `--repo`
  is what makes `--branch` correct, so the wrapper reviews the RANGE `<base>..HEAD` and asserts both
  endpoints from the job record; a docs-only diff cannot be roborev-certified at all, because roborev
  drops exactly what its configured `exclude_patterns` pathspecs match (it makes no code/non-code
  judgement) and `*.md` is configured. "docs-only" is a code-free CENSUS, never a `docs/` path prefix —
  `docs/reports/*-artifacts/` harness executables ARE reviewed code. Nothing predicts that exclusion set
  pre-enqueue (deferred, #3283), so a swallowed path FAILs AFTER the round under `prompt-content:` — if
  `prompt-content:` FAILs, suspect `.roborev.toml` first (#3229). Any non-PASS terminal `RESULT`, `NOTHING-TO-REVIEW`
  included, is a failed round and a blocked merge — never "roborev clean". See CLAUDE.md +
  `docs/development/agent-machine-setup.md`.
- **Coverage is not equivalence — a per-slice review ledger is an audit trail, never a certification**
  (owner, 2026-08-03). The only certifying review artifact is ONE genuine full-range `<base>...HEAD`
  round with `prompt-content: PASS`; never slice the base to get a green. On a non-PASS round, raise
  roborev's `default_max_prompt_size` (#3257/#3263) and re-run the FULL range — past the assembled-prompt
  ceiling roborev spills the diff to a file the sandbox cannot read, so the model answers "No issues
  found" having read zero lines, a vacuous PASS textually identical to a real one. On #3249 a complete
  per-slice ledger over a `+4216` diff still missed 4 findings (1 High, 2 Medium) that the full range
  found. See `process_improvements.md` (2026-08-03 / #3249, entry 3).
- Every GitHub write gets a short traceable comment.

## Field round validation (separate from the agent gate, issue #2399)

The live 3-node field validation cycle (round tracker channel, e.g. #2367) reports
against its own standard: `docs/development/round-validation-metrics.md` — 14 metrics,
A/B baked into a pass/fail round gate, C/D tracked as numbers, pre-filled with the
round-9 baseline. That round gate is a **live-cluster field-validation verdict**, not
`scripts/agent-gate.sh` — a delivery cycle's agent-gate PASS says nothing about whether
that build survives a live round, and vice versa. New round trackers seed from
`.github/ISSUE_TEMPLATE/round-tracker.yml`.

## Implement-loop depth moved verbatim from CLAUDE.md (#4092)

- **ROBOREV LAST, and a later rebase VOIDS the roborev round (#3752).** The endgame order is
  **rebase → gate of record → C → roborev → `premerge-assert` → arm**, and the reason is a
  **BYTE ASYMMETRY** that decides it by itself: **a roborev round changes no bytes, so reviewing
  after gating costs nothing and cannot invalidate a gate PASS; a rebase changes bytes, so gating
  or reviewing before it certifies the wrong tree.** Review-after-gate is free; gate-after-review
  is not. A rebase REWRITES the reviewed commit, so a PR can truthfully record "roborev: PASS"
  about a commit that no longer exists on the branch being merged — measured on PR #3735, whose
  genuine job 304 at `d3812f59` (`findings: NONE`, 1.07M input tokens) became, after the lane's
  correct rebase, a `git cat-file -t` that reports no such object, with TWO unreviewed commits
  after the reviewed content, one of them the semantic rebase-conflict fix in the only file that
  overlapped `main` — i.e. the most review-worthy commit on the branch. So **if you rebase, you
  are back at the gate**: re-gate, re-review, re-assert, in that order.
  **POST THE BLOCK ON THE PR.** `premerge-assert.sh`'s `review-binding` leg reads the roborev job
  id from a `==== ROBOREV REVIEW SUMMARY ====` block recorded in the PR body or a top-level
  comment, so recording it is what lets the merge gate know a review happened at all.
  **AND A NON-EMPTY SEMANTIC OVERLAP MEANS GIT CAN MERGE CLEANLY AND STILL BE WRONG.** After a
  rebase, compute the overlap over `merge-base..origin/main` — **never `HEAD..origin/main`**,
  which includes reverting your own work (measured 16 files vs the correct 3) — re-run the tests
  touching every overlapping file, and EXPECT a fix. Any such fix is new code, so it invalidates
  the gate AND the review.
- **How a lead actually stops a merge (#3752 AC7).** The sanctioned stop is **converting the PR
  to draft** (`gh pr ready --undo <pr>`), which GitHub enforces against merging, or a per-tier
  `ci:` state. **`gh pr merge --disable-auto` alone is NOT a stop** — it removes the auto-merge
  REQUEST, and a plain `gh pr merge --squash` succeeds immediately afterward (measured: #3735
  merged three minutes after the lead disarmed it). A column-zero `HOLD:` **COMMENT** on the PR or on
  the issue it closes is now mechanical too, because `premerge-assert`'s `hold-check` leg reads
  it (30-minute disarm window, a named committed constant with no env override); a lead clears
  one with a column-zero `GO:` or `RELEASE:` line. **A COMMENT, not the PR DESCRIPTION** — the
  leg scans comment bodies and never the description, deliberately: a PR body is editable at any
  time by anyone with write access **with no per-edit attribution**, so it is the weaker artifact
  and must never be an authorization channel (#3312), whereas a comment is permanent and
  attributable. Do not "helpfully" add body scanning; a `HOLD:` typed into the description is
  silently unenforced, which is why this sentence names the artifact. But a draft is the only stop that holds
  without the lane's cooperation.
- **Review-first (#2086)**: review BEFORE the first full gate so the ONE gate certifies
  already-reviewed code. Skip ONLY for a genuinely mechanical diff (no `pub`-item change AND single
  call site AND no new surface). When in doubt, review.

- **flow-closer (#2084/#2668)**: the full gate, the final roborev pass, and the merge run inside the
  disposable `flow-closer` subagent — the lead retains only its terminal packet (verdict, PR URL,
  summary-file path, ≤10 lines residual), never gate stdout or review churn. The closer has **no
  `Agent` tool**, so **C is spawned by the lead at the closer's `NEEDS-SPAWN` request** (the closer
  stops, emits a `NEEDS-SPAWN {role: spec-auditor, …}` packet, and the lead spawns `spec-auditor`
  then re-invokes with the verdict; a src-design fix respawns `sstable-developer` the same way).
  Before arming `gh pr merge --auto` the closer runs the scripted pre-merge assert
  `scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-of-record-summary> [<delta-summary>]`
  (#2456/#3465) — refusing to merge unless the PR head still equals the certified SHA **AND** a gate
  of record exists for it. Since #3752 it also runs two legs BEFORE its head check, both fail-closed
  and both refusing on `UNMEASURED` (a positive verdict requires a positive measurement):
  **`PREMERGE: REVIEW-BINDING`** — the roborev job recorded on the PR must have a reviewed head that
  is an ANCESTOR of the certified sha (`git merge-base --is-ancestor` is the load-bearing test and
  runs FIRST; `git cat-file -t` is a DIAGNOSTIC ONLY, because a rebase leaves the old commit dangling
  and reflog-reachable so it still answers `commit`) with no reviewable code after it by
  `classify-docs-only.sh`, the reviewed head being derived from the JOB RECORD's `git_ref`
  (`<base40>..<head40>`), never from the `Enqueued job <N> for <sha>` line, which for a range review
  names only the BASE; a **code-free PR diff** is a loudly DECLARED `NOT-APPLICABLE`, since a
  code-free diff cannot be roborev-certified at all. **BOTH HALVES of `git_ref` bind, and the base
  half is the T4 vacuity class one level down**: a `<head~1>..<head>` record has a head EQUAL to the
  certified sha, so it passes every head test there is while leaving every earlier commit on the
  branch unreviewed — the leg therefore requires the reviewed base, PROJECTED onto the branch as
  `merge-base(recorded-base, certified)`, to be at or before the PR's **merge-base** (never the base
  ref's tip, #3392), or the skipped prefix to be code-free. That projection is the difference between
  a check and a false FAIL: a base recorded OFF the branch skips none of the PR's own commits, so the
  skipped prefix is a COMMIT SET and never a path diff against the recorded base. **AMONG THE ROUNDS THAT COVER,
  THE LATEST DECIDES, AND IT MUST ITSELF BIND (#3752, roborev job 78).** The first draft said "ANY
  recorded round that covers suffices" and stopped the scan at the first bindable record — so an
  earlier CLEAN round stayed sufficient even when a LATER recorded round at the same certified head
  reported findings or failure, i.e. a **known, newer, adverse review result was ignored because an
  older favourable one was encountered first**. With exactly ONE covering round there is no ordering question and no chronology is
  required — F2's defect needs two covering rounds by construction, and demanding an order key to
  sort a set of one reds correct input the moment a real record lacks the field. With more than one,
  chronology comes from the record's own `started_at`,
  never from PR-comment order (a comment can be posted out of order or edited) and never from the job
  id (nothing guarantees ids are monotonic across agents); ordering is lexicographic, so the
  fixed-width ISO-8601 UTC form is CHECKED and anything else is `UNMEASURED` rather than sorted
  wrongly, as is a covering round with no readable stamp — **the order is never guessed, because
  guessing it is what lets an older favourable round win again**. **AND A VALIDATED STAMP IS STILL
  NOT AN ORDER WHEN TWO ROUNDS SHARE IT (roborev job 82).** The selection comparison is strict, so on
  EQUAL `started_at` the first-encountered index survived — PR-record order deciding a merge, which
  is the very thing the sentence above forbids, one level down. There is **no finer key to break it
  with**: measured on live records, every chronology field the job record carries
  (`enqueued_at`/`started_at`/`finished_at`/`created_at`) is **second**-resolution and the record's
  own `uuid` is v4 (random, not time-ordered). So a tie at the maximum refuses as `UNMEASURED`,
  naming both tied jobs, **unless EVERY round tied there is independently bindable** — with no
  disagreement there is nothing for an ordering to resolve, and refusing would red the correct input
  of two reviewers legitimately starting inside one second. Still true, and orthogonal: every
  job on the PR is examined and one unretrievable record cannot end the scan (an unresolved record
  decides only when no covering round decided the question, as `UNMEASURED`). **Declared residual**:
  an unretrievable record could in principle BE a newer adverse round, and that cannot be
  distinguished from an early round aged out of `roborev list --limit`, so demanding retrievability
  of every historical record would red a correct multi-round PR — what is closed is the finding's
  subject, known newer results being ignored. **AND A RANGE MATCH ALONE DOES NOT BIND** — the leg's first draft REPORTED the
  recorded verdict and derived nothing from it, declaring that a residual, which was a false-green
  route in a merge gate: a block naming an in-progress, FAILED or findings-bearing job whose range
  happened to match bound the merge, and it is an ACCIDENT route before a hostile one (a lane
  pasting its own first FAILING round certifies itself). A job now binds only when the **JOB
  RECORD's structured verdict** — never the PR block's self-reported one, which is untrusted text —
  says `clean`. The verdict is THREE-VALUED and an unreadable one is `UNMEASURED`: a range
  match is not a review.
  **AND A `findings` RECORD CANNOT BIND AT THE MERGE POINT AT ALL — NOT EVEN WITH A PERFECT
  AUTHORIZATION (roborev job 103). DECLARING THE GAP WAS NOT ENOUGH.** The deferral route exists at
  REVIEW time because roborev **re-reports** a lead-deferred finding on every later round (#3626), so
  a record stays `findings` forever and requiring `clean` there with no way out would make such a
  merge UNOBTAINABLE. At the MERGE point the authorization is still re-verified through the SAME
  scanner the wrapper uses (`roborev-waiver-scan.py findings-deferral-authorization`, a narrow kind
  returning the DISTINCT state `granted-authorization`) — but a grant now yields **`UNMEASURED`
  (exit 5), never `BOUND`**. The reason is the one half that kind cannot judge: the marker's
  **`count=`**, the field that ties a deferral to the findings it defers, is matched against the
  count OBSERVED BY THE REVIEW, and **no trusted count exists at merge time** — measured, on
  findings-bearing jobs 78 and 102, `roborev show --json` exposes only `verdict_bool`/`verdict`, a
  letter and no count, and `--recheck-job` enqueues nothing so it writes no record either. The
  earlier design DECLARED that gap in the leg's output and bound anyway, which let the merge gate
  honour an authorization **the review-time path would REJECT**: an allowlisted human can post a
  fresh marker after the review carrying any count at all, and nothing at the merge point compared
  it to anything. The actor is a NON-INVOKER and the shape is an ACCIDENT, so by #3312's triage rule
  it is a defect and not an out-of-model bypass — and a declaration is not a control. Fabricating a
  count would be an affirmative assert over an unmeasured value; comparing the marker's count with
  itself would be a tautology. **So the remedy at merge time is a CLEAN covering round, never a
  marker** — the leg says exactly that, and the call is kept only to separate "no authorization
  exists" (a measured refusal, exit 4) from "the authorization is good but unverifiable here"
  (exit 5), which are different operator actions. **AND THE DEFERRAL PATH IS THREE-VALUED, NOT TWO (roborev job 102): "the
  authorization was evaluated and REFUSED" and "the authorization COULD NOT BE EVALUATED" are
  different states with different REMEDIES, so they get different exits.** A CLOSED or non-existent
  tracking issue is an answer GitHub GAVE ⇒ `UNBOUND` (exit 4); an issue whose state could not be
  ASKED, an absent or failing `roborev-waiver-scan.py`, an unreadable author allowlist, or a scanner
  payload carrying no readable state ⇒ `UNMEASURED` (exit 5). Both refuse the merge — `premerge-assert`
  maps 4 and 5 alike to its loud exit-2 refusal — so this is the DIAGNOSIS and never a softening:
  reporting "no authorized deferral covers this job" for an unreachable `gh` sends a lead to re-post a
  marker that was already fine, when the fix is restoring access. **The disarm half AND BOTH COMMENT THREADS are read with `gh api --paginate`,
  with EVERY page decoded before any verdict**: one page of 100 events is not the timeline, and
  `--json comments` is a BOUNDED connection — so a persistent `HOLD:` outside the first page
  produced a false `NO-HOLD-RECOGNISED` on the artifact a lead actually posts a stop order in. ONE
  normalised stream feeds both job discovery and the hold scan, and the REST-vs-GraphQL spelling
  difference (`user.login`/`created_at` vs `author.login`/`createdAt`) is reconciled ONCE at the
  fetch boundary: read the wrong one and every author is EMPTY, which silently stops granting
  deferrals and stops honouring an allowlisted release — fail-closed, and wrong on correct input.
  An unrecognised payload shape REFUSES rather than yielding a shorter comment list, because a
  short thread is indistinguishable from a quiet one. A `clear` derived from a partially
  read signal is a false clearance on exactly the scenario this leg exists for. **`PREMERGE: HOLD-CHECK`** — the machine-readable
  half of the `HOLD:` re-read (below). **Resolved PER THREAD, and it refuses while ANY thread is
  held (#3752, roborev job 78):** every marker used to land in one global timeline, so an authorized
  `GO:` on one closing issue cleared an unrelated, NEWER `HOLD:` on another thread purely by being
  later — a release nobody wrote for the thread that was held. A release now clears only the thread
  it is posted on, the report NAMES each held thread so the operator knows where to post one, and
  there is deliberately **no cross-thread release**: if one is ever wanted it needs its own explicit
  design, and the conservative direction is to refuse. **Markers are ordered by `updatedAt`, not `createdAt`** —
  what a reader SEES is the current text, so an OLD comment EDITED to carry `HOLD:` must not lose
  to a `GO:` posted before that edit; a marker-bearing comment whose edit timestamp is unreadable
  cannot be ordered against its siblings and is `UNMEASURED`. **The third argument is
  REQUIRED, and that is the #3465 mechanism**: verifying the head against a *claimed* certified sha never verified that a
  certified sha EXISTS. **Two distinct escapes, one mechanism.** #3408 = **no gate at all** (merged on
  22 `--lite` PASSes and not one full `scripts/agent-gate.sh` run, because nothing in the merge path
  ever asked for the block). #3616 = **a real gate, someone else's** — a closer located its run dir by
  RECENCY (`ls -t /tmp/agent-gate.*`), read a PEER LANE's dir, saw 33 of 37 components PASS and was
  about to merge #3616 on PR #3580's verdict; the count, the dir and the timestamps were all real, and
  only the `run-id:` line exposed it, read by a human. With 14000-27000 stale run dirs per box and up
  to 4 concurrent gates, recency picks a peer ROUTINELY. **That second class is what the
  `commit:`+`tree-start:` binding refuses**: a peer's summary carries the OTHER PR's branch head, so
  requiring both abbreviations to match the certified sha converts "a human might notice the `run-id:`
  line" into a mechanical refusal at the merge point — the sha compare is the guard, not bookkeeping.
  The script now requires the summary file to hold exactly ONE
  `==== AGENT-GATE SUMMARY ====` block (whole-line-anchored; `--lite`/`--delta` headers are distinct
  and refused by name, as is a second or unterminated block) with `RESULT: PASS` and
  `tree-integrity: PASS` compared **token-exactly** (`INCOMPLETE` is the launch sentinel, not a
  verdict — #3041; a mutated-mid-run tree is not a certification — #2926), and with BOTH `commit:`
  (7 hex) and `tree-start:` (12 hex) prefix-matching the certified sha **at each value's own width**
  — a non-hex placeholder REFUSES rather than being skipped. It cannot verify `run-id:` (it did not
  launch the gate — #2874's reader contract needs the launcher) and it cannot prove the summary came
  from a real run rather than a hand-written file: a **hostile invoker is out of the threat model**;
  what this closes is **accident and drift** — a diligent worker with no step in its path telling it
  the gate of record was never run. `dirty:` is REPORTED in the success line **and ENFORCED** (#3648): the gate of record's
  block — and, in Case B, the delta block's too — must read `dirty: no`, matched AFFIRMATIVELY, so an
  absent or unrecognised value REFUSES rather than being read as clean. A `dirty: yes` run certified the
  sha PLUS uncommitted tracked edits (the capture is `--exclude-standard`, so never a gitignored log) and
  `commit:`/`tree-start:` cannot see the difference. There is deliberately NO opt-out — a dirty tree is
  always re-gateable, so an override could only buy a vacuous green. **The FOURTH argument is optional and is the ONLY way a `--delta`
  re-cert can certify a merge** — because #1892 *mandates* `--delta`, "never a repeat full gate", for a
  test/docs-only diff on top of a full PASS at anchor `X`, and mandates that the PR record BOTH blocks.
  A 3-arg-only guard therefore red on correct, doctrine-mandated input, which is the guard agents learn
  to waive. So: **Case A (3 args)** the full block's `commit:`/`tree-start:` must cover the certified
  sha; **Case B (4 args)** the third argument is the ANCHOR's full PASS (its sha need NOT be the
  certified sha) and the fourth must be one `==== AGENT-GATE DELTA SUMMARY ====` block with
  `MODE: delta` (asserted affirmatively — the inverse of Case A's belt), `RESULT: PASS`,
  `tree-integrity: PASS`, a `delta-anchor:` naming exactly that anchor (an `(UNRESOLVED)` anchor
  refuses), and its OWN `commit:`/`tree-start:` at the certified sha. Either way a full-gate PASS must
  EXIST and the merged tree is covered — directly, or by an anchored delta re-cert on top of it. A
  block carrying `nested-under:` (#2874) is refused outright: a nested sub-gate runs at the SAME tree,
  so the sha binding provably cannot see it.
  **AND IN CASE B THE ANCHOR MUST BE ON THE CERTIFIED SHA'S HISTORY (#3653).** Everything above proves
  the two blocks AGREE about a sha, never that the sha is on THIS PR — Case B's anchor identity rested
  entirely on the delta run's SELF-DECLARED `delta-anchor:` line, so any full-gate PASS plus a delta
  naming it satisfied the chain: the #3616 cross-lane class surviving in the one path Case A's sha
  binding does not cover. (The accident route was narrowed by `agent-gate.sh --delta`'s own
  fail-closed diff classification, i.e. **by another script** — a real constraint stated nowhere the
  guard is read.) So `git merge-base --is-ancestor <anchor> <certified>`, **three-valued** because
  `--is-ancestor`'s rc 1 is itself three-valued (#3544 — in a SHALLOW clone it also means "the
  connecting history is absent", so rc 1 is a verdict only in a repository proven complete):
  **BOUND** (rc 0) proceeds and is RECORDED as `anchor-ancestry: BOUND` on the `PREMERGE: DELTA-RECERT`
  line, because a silent pass is indistinguishable from a check that never ran; **NOT-ANCESTOR**
  (rc 1, both objects present, `--is-shallow-repository` = `false`) is exit 2 naming both shas;
  everything UNMEASURABLE — no git, not a work tree, either object absent, shallow or shallowness
  unknown, `--is-ancestor` exiting ≥ 2 — is exit 3 under its own `PREMERGE: ANCHOR-UNVERIFIABLE`
  marker, each cause carrying its own remedy, because an unmeasurable result is UNKNOWN and "fix the
  box" is a different operator action from "your chain is wrong". **The walk does NOT run in the lane**
  (roborev job 355): `$GIT_DIR/info/grafts` rewrites parentage and SURVIVES `--no-replace-objects` (the
  job-285 measurement above, re-measured for this check: `no` → `YES` → `YES`), so a graft alone turns a
  FOREIGN anchor into `BOUND` — and grafts live in the COMMON git dir that every lane on this fleet
  shares, making the planter a PEER LANE as well as an accident. The ruling there was to MOVE the walk,
  and it is applied here: the object reads and `merge-base` run in a throwaway `git init` scratch whose
  only view of the lane is `GIT_ALTERNATE_OBJECT_DIRECTORIES` — pure object storage, no config, hence no
  grafts, no replace refs, no promisor; a failure to build it is UNVERIFIABLE, **never** a fall-back to
  the live repository. Two reads stay in the lane on purpose: resolving its object directory
  (`rev-parse --git-path objects`, no object read, no network) and `--is-shallow-repository`, because a
  fresh scratch is NEVER shallow and probing it there would answer `false` unconditionally, making the
  shallow guard a vacuous pass. **A SCRATCH'S ENVIRONMENT IS LOAD-BEARING IN A WAY A LANE READ'S IS NOT**
  (roborev job 358): the old rationale — "these reads are addressed BY A SHA, so no environment can bend
  them" — is right about a read in the lane and WRONG here, because an environment variable does not bend
  the OBJECT, it bends WHICH REPOSITORY ANSWERS. Measured on git 2.43.0: `GIT_DIR` **overrides `-C`**, and
  both `git init --template=<dir>` and `GIT_TEMPLATE_DIR` seed a planted `info/grafts` INTO the new
  scratch. So every git call — the lane DISCOVERY reads included (job 276: the allowlist has to reach the
  sites a later change adds) — runs under `env -i` + an allowlist ADMITting only `PATH` and `TMPDIR`
  (tighter than the pre-flight's: no network here, so no `HOME`, no `SSH_*`, no proxy), with
  `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM=/dev/null` plus an explicit empty `--template=`. The reads are
  also **bounded** by the runner this script already resolves for the advisory — but **only the EXTERNAL
  commands are** (every git call and `mktemp -d`), and the token says exactly that:
  `anchor-reads: bounded-<n>s+<g>s(external:git,mktemp,sh;UNBOUNDED:command-v+pwd-builtins)`. **The scratch
  directory is deliberately NOT DELETED** — a delete through a peer-mutable pathname cannot be made
  race-free in shell (no `openat`/`unlinkat`) and every lane here runs as the SAME USER, so after three
  rounds of narrowing it was REMOVED: one object-less `git init` is left under TMPDIR per Case B merge and
  the OS reaps it. **The
  token was corrected TWICE — once for overclaiming, once for UNDERclaiming**, and an
  **ANCHOR-PATH OPERATION AUDIT** in the script header now lists every operation with two facts, *is it
  bounded* and *is its target validated*, in the `workspace-test-disposition.txt` idiom (a new operation
  must join the table; it records completeness and labelling, not truth). That audit exists because seven
  shipped-script findings on #3653 were the same two questions asked of different operations, one per
  review round. It made canonicalisation runner-bounded (`sh -c 'cd … && pwd -P'` is external, so no
  builtin-bounding was invented), DELETED both `[ -d … ]` builtin stats as redundant with it, and bounded
  + target-revalidated the scratch `rm -rf`. What is left unbounded is `command -v` PATH lookups and one
  `$(pwd)` diagnostic — builtins over local state, unreachable from the shared object store or TMPDIR. And **where no `timeout`/`gtimeout`
  supporting `--kill-after` exists the check REFUSES** (`ANCHOR-UNVERIFIABLE`, naming a one-command
  remedy). **That REVERSES the first ruling here, which said run unbounded and declare it, on the ground
  that a hang is a LIVENESS failure yielding no verdict rather than a false pass.** What that missed: **a
  hang in this guard BLOCKS THE MERGE ANYWAY**, so the real comparison is never merge-vs-refuse but
  *hang forever with no diagnosis* vs *refuse now with a named cause and remedy* — same outcome for the
  merge, and the refusal strictly dominates. "It cannot produce a false pass" was true and IRRELEVANT,
  because the alternative was never a pass. Hand-rolling a portable bounded runner is ruled OUT: that is
  new process-lifetime code, the family that produced three defects in this issue's own test scaffolding,
  and a fourth inside a merge guard costs more than one named install command.
  **THE COMMIT-GRAPH IS TRUSTED METADATA, SO IT IS DISABLED — AND THE MEASUREMENT IS NOT THE ONE THE
  FINDING PREDICTED** (roborev job 361). `objects/info/commit-graph` reaches the scratch through the
  alternate, is NOT content-addressed, and git trusts its parent edges. Measured on git 2.43.0 against a
  graph whose CDAT parent slot was patched and whose checksum recomputed: `rev-list --parents` reports the
  FORGED parent and `-c core.commitGraph=false` the real one (so the graph IS consulted and IS trusted) —
  but `merge-base --is-ancestor`, the call this guard makes, answered `no` **both** ways, so the exploit
  against THIS call did not reproduce. The flag ships as defence in depth (one git version or one refactor
  from mattering) and the test says so, pinning it STRUCTURALLY where no behavioural arm can. Measured and
  deliberately NOT disabled: `core.multiPackIndex` (with the pack `.idx` removed the object was
  unreadable, so no evidence it supplies lookups or edges) and reachability bitmaps (`GIT_TRACE2_PERF=1`
  showed ZERO bitmap mentions during the call) — widening past a measurement is guessing.
  **AND AT THAT POINT THE BOUNDARY IS DECLARED RATHER THAN ENUMERATED AGAIN.** Three review rounds found
  three routes into one mechanism (graft → environment/template → commit-graph), which is #3544 job 264's
  *"one axis closed, space declared done"* shape, and #3746 / job 311 already ruled on the unclosable
  version: DECLARE it in the emitted line and hand the subject to the issue that owns it. So every Case B
  success line ends with one constant — `ancestry over this box's SHARED object store: objects+metadata
  and SCRATCH namespace: objects, metadata and scratch TRUSTED, not verified (#3746) — closes
  accident/drift, NOT a same-UID peer` — folded into the ONE renderer, never per-arm. **THAT IS THE
  TERMINUS OF THIS HARDENING LINE (job 390).** Every lane on this fleet runs as the same user, so a peer
  can write the shared object store AND our scratch — it can drop `.git/info/grafts` into the scratch
  between `git init` and the walk, reproducing the graft attack inside the thing built to prevent it — and
  no mode or ownership can admit this process while excluding a peer. So the CLAIM is narrowed rather than
  the hole patched, and the hazard is assigned to **#3746**, which already owns "lanes share an object
  store". **A later same-UID-peer instance is that declared boundary, not a new defect** — which is what
  #3653 asked for, since its own text says the hostile route is largely closed elsewhere and the defect was
  the constraint not being stated where the guard is read. What the binding proves:
  ancestry over the objects and commit metadata this box's shared store presents, isolated (each with a
  positive control) from grafts, replace refs, an inherited `GIT_DIR`, an ambient template, and the
  commit-graph. What it does not: that the anchor is on the PR **as GitHub sees it**, and anything against
  a peer that can WRITE that shared store. A fifth route in this family is a residual under that
  declaration, not a false claim — a check that claims nothing false is worth more than one claiming a
  closure it does not deliver.
  **What a `PREMERGE: OK` does NOT prove (#3650) — it says so itself, on a `PREMERGE: SCOPE` line.**
  It proves the diff is unchanged since certification and that a full gate PASSed on **that exact
  tree**. It does NOT prove the change was certified against the `main` it will join: a squash-merge
  composes the diff with main's CURRENT tip, so for any PR whose base is behind main **the certified
  tree and the merged tree are different objects**. Measured on #3358/PR #3362: base `2bde26a7c` with
  main 10 commits ahead, whose head gate FAILed `core-tests` only because a known flake's fix
  (`5e08db201`, #3514) was on main and absent from that base — the benign direction; the malign one is
  a PASS at a stale head hiding an interaction with something that landed in between. A gate on the
  MERGE RESULT is **#3650 SLICE 2** and is still not implemented here. Report the verdict as "gate of
  record verified at `<sha>`", never "certified against main".
  **What #3650 SLICE 1 DID add — a non-blocking BASE-STALENESS ADVISORY, which is information and
  not a verdict.** `scripts/flow/base-staleness.sh` (runnable by hand — it is the mechanization of
  the standing triage question *"is the fix for this red already on main and merely absent from my
  base?"*) reports `N` commits behind the **merge-base** with `origin/main` (never the base ref's
  tip — #3392) and `M` of those touching the diff's **blast radius**, which is
  *(paths the diff touches) + (a hard-coded gate-global set)* — content that can change ANY gate's
  verdict regardless of the diff (`.config/nextest.toml`, the toolchain pin, the Cargo manifests,
  `scripts/agent-gate.sh`, `scripts/ci/**`, **`scripts/tests/**`**, `cqlite-core/tests/support/**`,
  `test-data/**`, `.github/workflows/**`). That set is **one NAMED, COMMITTED list
  (`GATE_GLOBAL_PATTERNS`) with no env override**, never an inline glob: an override is
  settable by the party it constrains, *"which paths stale my certification"* is exactly what a
  lane wanting to skip a re-gate would widen, and the next person adding a shared test-support
  directory has to be able to FIND the list. **Membership asserts ONE predicate** — *content here can
  change a gate's verdict INDEPENDENTLY OF THE DIFF* — not "is important" or "is shared"; to add an
  entry, state which gate COMPONENT it can flip and how you MEASURED its selectivity.
  `scripts/tests/**` is in the set because the gate does not merely READ that roster, it EXECUTES it
  (`tooling-tests` runs ~16 of them), so one commit touching one of those files reds EVERY lane's
  full gate — the predicate verbatim — and it was measured before being added (28 → 37 of 107, 9
  commits staling only because of it), while `deny.toml` and the loose `scripts/*.sh` helpers were
  measured and NOT added because they fire zero times. **And the list is DECLARED NON-CLOSED in the
  output**: it is a curated, measured list of RECOGNISED gate-global content, so a gate-global path
  absent from it is a false negative — declared as gap 2 of 2 beside the dependency-closure gap,
  because declaring one gap while having two affirms a completeness the list does not have.
  **The two path sources are RENAME-SYMMETRIC by construction, and that is a FAIL-OPEN if broken.**
  The diff side is porcelain (`git diff`), which honours `diff.renames` (git default TRUE since 2.9)
  and reports a rename's DESTINATION ONLY; the commit side is plumbing (`git diff-tree`), which
  rename-detects only under an explicit `-M`. Unpinned, a PR that renames a path — routine here, the
  campsite rule makes splits normal — loses the OLD path, a commit behind editing it matches NEITHER
  half, and the scan reports `blast-radius 0 RECOGNISED` on a genuinely stale base. `diff.relative`
  is the same class and is worse because the INVOKER controls it: set, porcelain run from a
  subdirectory strips the prefix, making the count a function of cwd. Both are pinned off on the
  porcelain call; **do NOT add `-M` to the `diff-tree` call**, which would reintroduce the asymmetry
  from the other direction. `premerge-assert.sh` prints the finding on
  `PREMERGE: ADVISORY` lines and **can never fail on it** — an absent, failing or `UNMEASURED`
  advisory is REPORTED and is not fatal in slice 1 — and the three `PREMERGE: SCOPE` lines are
  RETAINED, because slice 1 does not close the gap they disclose. Three properties to carry:
  **(1)** the output is ANCHORED so it cannot be pasted or grepped as a certification. **The
  absolute form of this property was FALSIFIED BY REVIEW and the correction is recorded rather than
  softened**: it read *"no `PASS`, no `OK`, no `RESULT:` in any run"*, which is impossible because the
  advisory prints repository-controlled paths VERBATIM — `test-data/**` is gate-global and the tracked
  path `test-data/scripts/CI_SMOKE_TEST_USAGE.md` contains `OK`; three tracked paths do today, and the
  test asserting the absolute form passed only because the sampled run's matched set happened to
  exclude them, a test passing for the wrong reason. What holds instead: **every** output line, stdout
  AND stderr, begins with `BASE-STALENESS: `; every dynamic field is CONTROL-CHARACTER SANITIZED
  (git PERMITS NEWLINES IN PATHS, and unsanitized such a path emits a line with NO prefix, breaking the
  anchor everything rests on) while otherwise printing the path verbatim, because masking it would
  mangle it for the reader — #3312's rule is to anchor or remove the channel, never to pick a rarer
  delimiter; the verdict appears ONLY on a `verdict ` line carrying a token from the closed set
  {`STALE-RECOGNISED`, `NO-STALENESS-RECOGNISED`, `UNMEASURED`}, prose going on `verdict-detail` lines;
  and the script's own STATIC TEMPLATE TEXT carries none of the three tokens, asserted STRUCTURALLY
  over the source file, which is provable where a claim about one sample run is not. **Declared
  residual: a repository path CAN contain a reserved substring and the advisory prints it — the anchor
  is what makes that harmless.** The no-finding verdict is `NO-STALENESS-RECOGNISED` (a *scan result*,
  never `FRESH`/`CLEAN`); **(2)** `M = 0` prints
  `0 RECOGNISED`, never a bare `0`, and every run prints its own `NON-EXHAUSTIVE` lines, because the
  blast radius is **not a dependency closure** — a commit changing an item the diff CALLS while
  touching neither the diff's paths nor a gate-global path is reported as NOT staling, a real
  false-negative class that is declared, filed, and not closed; **(3)** exit `4` is
  `STALE-RECOGNISED`, `5` is `UNMEASURED`, and **a consumer MUST treat `5`/`UNMEASURED` as STALE,
  never as fresh** — the standing rule against deriving a pass from the absence of a bad signal.
  The definition was chosen BY MEASUREMENT against the case that produced the issue
  (`docs/round-artifacts/issue-3650-blast-radius-measurements.md`): on PR #3362 the culprit commit
  and the diff share **no path**, so path intersection alone would call that certification fresh
  exactly when it was not, while intersection + gate-global fires on 37 of 107 commits behind (35%)
  — measured at `origin/main` `b1e8598a2`, subject `4bc6b913a`, the sha quoted because `behind` is a
  function of where main was — leaving 65% of the churn non-staling. The run NAMES the culprit
  (`matched 5e08db201 gate-global .config/nextest.toml`), so the detection is attributable rather
  than a coincidence on a count — and the count is reported BY THE SCRIPT, which is the authority
  for it; a number quoted in prose here decays exactly like a comment. With
  `--auto` armed, GitHub lands the PR on the `required` check going green (#2667); no CI busy-wait.
- **Severity triage (#2088, rubric `docs/development/roborev-severity.md`)**: roborev **blockers**
  are fixed pre-merge — each re-triggers `fix → --lite (+ any diff-relevant parity/integration
  target) → re-review` (#2087). **Nits** never trigger
  a re-verify round: batch all of a PR's nits into ONE linked follow-up issue at merge time. When in
  doubt, blocker. Every pre-roborev self-check class below is BLOCKER by definition.
  **Scripts get a capped loop (#3893):** roborev on `scripts/**`, `.claude/**`, `.github/**` and
  measurement-harness code (`docs/reports/*-artifacts/**`) is capped at **TWO rounds**; round-3 findings
  are DISPOSED — one linked follow-up issue per PR, `roborev-defer` marker on the merits — not fixed,
  UNLESS a finding is a **hang** or a **false verdict** (those two classes are exempt from every
  convergence rule). Bash has no compiler, so each fix round seeds the next; measured 22/25/32 findings
  over 7–12 rounds on three harness PRs in one day, most in the prior round's own fix. Tests and the full
  gate still apply; only the review loop is capped.
  **A DEFERRED finding still has to get PAST roborev, and since #3626 that is mechanical rather than a
  matter of lead memory**: roborev re-reports a deferred finding on every later round, so batching nits
  into a follow-up issue does not by itself make `findings:` read `NONE`. The lead records the deferral
  with a `roborev-defer: findings` PR comment naming the filed issue numbers and the observed count, and
  applies it with `--recheck-job`; the run then reports `findings: DEFERRED (…)` (never `NONE`) and may
  reach `PASS`. See the roborev-invocation bullet above for the marker and its constraints.
- **Post-gate polish (#1892)**: after a full PASS at `X`, a test/docs-only diff `X..Y` re-certifies
  with `--delta` (fail-closed; see gate table above), never a repeat full gate. The nightly
  `gate.yml` deep-check re-runs the FULL gate on `main` as the standing backstop.
- `--lite` NEVER replaces the full gate — the full `AGENT-GATE SUMMARY` is the only run that counts.


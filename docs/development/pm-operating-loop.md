# Delivery operating model — manager + flow-lead workers

Two roles. One board. The manager orchestrates; the workers do everything else.

## Roles

| | **Manager** (one window, `/manager`) | **flow-lead workers** (**one per machine**; N machines) |
|---|---|---|
| Writes code / claims / merges? | **Never by hand** (runs the merge-on-green poller for the fleet) | Yes — owns the issue end-to-end |
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
   **Select by board `Status` ONLY — never by the `status:ready` label** (Path A, #1886: the board is the
   sole dispatch authority; labels are decorative). **Empty Ready → stop** (no work is ready; near a release
   Ready is meant to drain to zero — do NOT fall back to labels). Board unreachable → STOP and fix auth, do
   not dispatch from labels. Claim it (branch push = the cross-machine lock); first push wins, losers take the next item.
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
   **C** intent audit (design), and the final roborev pass, then — with any `HOLD: merge after #N` obeyed —
   merges on green (`gh pr merge --squash --delete-branch`) and returns only a terminal packet. Any src
   change after the full gate INVALIDATES it — the closer re-runs the gate if a fix or rebase postdates it.
   No worker CI busy-wait (`ScheduleWakeup`-polling a PR's own CI is prohibited).
6. **Finalize follows the merge.** The merge event triggers `flow-finalize` (archive any OpenSpec change,
   **stamp the telemetry ledger** with the roborev blocker/nit split, remove the worktree, delete the origin
   claim branch + clear the heartbeat, close the issue with a traceable comment). Board → Done (built-in).
   The lead then **resets** — zero prior-issue carryover, next item re-hydrated from the board alone (#2085).

## Merge-on-green (how a green PR lands — no worker CI busy-wait)

A worker never busy-polls its PR's own CI. When it reaches its terminal state it **arms** one of two
merge-on-green paths and stops; the mechanism watches the green signal for it:

- **Primary today — the manager-owned poller.** `main` currently has **no required status checks**
  (`contexts=[]`), so a naive `gh pr merge --auto` would merge the instant it is set, against an empty
  check set (forbidden — see the green-signal guard below). So the worker hands the PR off to the
  manager-owned poller/merge-engine, which gates on an explicit lane set and lands the PR on green. The
  poller runs **once at the manager level for the whole fleet**, not N times per worker — that concentration
  is the efficiency win.
- **`gh pr merge --auto --squash --delete-branch` — primary once required checks are configured on `main`.**
  When real required status checks exist for the PR's branch, `--auto` is the zero-token native path:
  GitHub lands the PR the moment the required checks pass and auto-closes the issue via `Closes #N`. Until
  then it is **not** used as the primary path.

The worker **logs which path it armed**. **Green-signal guard:** merge-on-green SHALL only land a PR once a
*defined* green signal exists — configured required checks, or the manager-poller's explicit lane set. It
must never auto-land against an empty required-check set.

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
- **(c)** Full gates for different lanes are run **serially** by the lead (respecting the #1825 cap +
  measured ~2-gate contention) — only the full-gate step serializes; everything else overlaps.
- **(d)** Long waits use **scheduled wakeups**, never idle polling.

## Self-improvement loop (telemetry + retro)

The pipeline measures itself so improvement is data-driven, not anecdotal:
- **Sense** — at finalize, the worker stamps one record per completed issue into the append-only ledger
  `docs/reports/delivery-telemetry.jsonl` (schema: `docs/reports/delivery-telemetry.schema.json`) via
  `scripts/delivery-telemetry.py record`. Records hold authoritative data only — GitHub-derived
  timestamps (cycle time + coarse phase durations) plus run-observed counters (claim collisions, rebase
  events, agent-gate pass/fail + run count, roborev findings, rework). A counter that was not observed is
  an error, never a fabricated `0`.
- **Diagnose** — on a cadence (per-epic or weekly) the **manager** runs `delivery-telemetry.py retro`,
  which ranks the recorded failure categories by a documented weighted tally (deterministic, not an
  inferred model) and reports the single highest-cost recurring failure. `--file` files a `flow-meta`
  improvement issue, deduped against open `flow-meta` issues by a stable category marker.
- **Improve** — that `flow-meta` issue enters Ready and runs through the normal pipeline like any other.

The `delivery-telemetry` agent-gate component (SKIP-aware on `python3`) covers the tool: schema
round-trip, lint-rejects-malformed, fixture-ledger → expected top failure, and dedupe.

## Concurrency: one worker per machine (#1930, owner decision 2026-07-04)

- **Exactly one flow-lead worker per machine.** That worker OWNS the machine's Ready-queue throughput and
  is the SOLE authority on machine load. It fans out *implementation* to many subagents (cheap: edits +
  `--lite` gates) and lets read-only reviews (rust-reviewer/spec-auditor) overlap, but it **serializes the
  full `agent-gate.sh` (concurrency = 1)** and caps heavy fan-out. This is the doctrine DEFAULT
  ("one lead → subagents, zero dup by construction"); N bare independent leads on one box is the
  discouraged path.
- **Full-gate concurrency = 1, always.** Never run 2+ full gates at once, regardless of the #1825 cap.
  The cap prevents SIGKILL under load but NOT timing flakes: two concurrent full gates flaked
  `mixed_p99_bounded_by_k_times_baseline` (`cqlite-core/tests/tail_latency_harness.rs`) under CPU
  oversubscription (#1625 core-tests: 693s solo → 87s + FAIL alongside a peer gate).
- **Any-slug pre-claim check.** Two same-machine sessions once claimed #1632 with *different slugs*
  (`issue-1632-parser-hardening` vs `issue-1632-parser-hardening-bundle`), so the exact-slug push never
  collided and the lock didn't fire. Before claiming, check for **ANY** `issue-<N>-*` branch:
  `git ls-remote --heads origin "issue-<N>-*"` → skip if present.
- **Cross-machine coordination is unchanged:** one-worker-*per-machine* composes with multiple machines —
  each machine runs one worker; the origin `issue-<N>-<slug>` branch lock coordinates across machines. The
  branch lock is no longer load-bearing *within* a machine.

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
- Workers otherwise **arm merge-on-green and stop**; the mechanism lands the PR on green. There is no human
  merge click for worker-owned issues — and no worker CI busy-wait.

## Hard rules
- The gate is the only run that counts; paste its summary block.
- Worktrees only; the branch push is the lock; stage explicit paths.
- EMU guard every board op: `gh auth switch --user pmcfadin && gh auth setup-git`.
- roborev follows **this machine's configured agent** (`.roborev.toml`; commonly `codex` — run with no
  flags). Pass explicit `--agent`/`--model` ONLY as a per-machine troubleshooting override when the local
  config is broken; never pin a specific agent as doctrine. See `docs/development/agent-machine-setup.md`.
- Every GitHub write gets a short traceable comment.

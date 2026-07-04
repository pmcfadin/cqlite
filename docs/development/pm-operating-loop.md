# Delivery operating model — manager + flow-lead workers

Two roles. One board. The manager orchestrates; the workers do everything else.

## Roles

| | **Manager** (one window, `/manager`) | **flow-lead workers** (N windows / machines) |
|---|---|---|
| Writes code / claims / merges? | **Never by hand** (runs the merge-on-green poller for the fleet) | Yes — owns the issue end-to-end |
| Board | Controls **Ready** (what + order); reconciles; reaps | Reads Ready; claims the oldest unlocked item |
| Lifecycle | none | full **1:1:1:1**: claim → implement → gate → C → roborev → PR → **arm merge-on-green → cleanup** |
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
4. **Run to completion** (`flow-implement`) via subagents (worker orchestrates; `sstable-developer` model:opus
   implements + runs the gate). **Out-of-scope bug found** → a subagent files a new detailed issue (never fix
   it inline / never grow the diff); if it **blocks** completion, comment "blocked on #<new>" on your issue,
   pause, and surface to the manager (it sequences via `HOLD`/Ready) — fix it only as its own 1:1:1:1 claim.
5. **Terminal state — arm merge-on-green, then STOP.** The worker's terminal state for an issue is
   **PR-open + `agent-gate.sh` PASS + spec-auditor C PASS (design) + roborev clean** (with any `HOLD`
   cleared). At that point re-check for an open `HOLD` (if `HOLD: merge after #N`, the merge-on-green
   mechanism stays gated behind #N — the manager sequences it), rebase on current `origin/main` and resolve
   any conflict in your own worktree, then **arm the merge-on-green mechanism (below) and end your turn.**
   Do **NOT** poll the PR's own external CI in a yield/wake loop (repeated `ScheduleWakeup` cycles) waiting
   for the cross-platform matrix — once the work is done that is pure token bleed, and it is prohibited.
6. **Merge-on-green lands it; finalize follows the merge.** The armed mechanism lands the PR when its
   defined green signal passes; the merge event triggers `flow-finalize` (archive any OpenSpec change,
   **stamp the telemetry ledger**, remove the worktree, delete the origin claim branch, close the issue
   with a traceable comment). Board → Done (built-in). Finalize is driven by the merge event, not by a CI
   busy-wait.

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
- roborev in this env: `--agent claude-code --model opus`.
- Every GitHub write gets a short traceable comment.

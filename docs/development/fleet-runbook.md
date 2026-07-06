# Fleet Runbook — Running the Agentic Delivery Pipeline as the Owner

The exact, user-level version of how to run CQLite delivery across one or more machines.
This is the *operator's* doc — what you type, what you'll see, when the system needs you.
Doctrine and internals live elsewhere ([delivery pipeline](https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/),
`docs/development/pm-operating-loop.md`, `CLAUDE.md`); this page is the driver's seat.

> **Status notes:** items marked **[after #2084]** etc. describe behavior that lands with the
> Tier-2 epic #2083 (context-economy restructure, audit `docs/reports/agentic-workflow-audit-2026-07-06.md`).
> Everything unmarked works today.

---

## The model in one paragraph

**One Claude Code session per machine. N machines = N issues in flight.** Each session claims work
by pushing an `issue-<N>-<slug>` branch to origin (the cross-machine lock — collisions are
impossible by construction), works it in an isolated worktree, and merges its own PR when the
quality bar is met (gate PASS + roborev clean, + spec-audit PASS for design work). You touch the
system in exactly **two places**: approving specs (Seam 1) and making product calls (the NEEDS-YOU
list). Everything else runs itself.

The machine you're sitting at runs the **lead** (your conversation partner, which also works
issues itself). Other machines run pure **workers**. A lead is a worker with a human attached.

---

## Machine setup (once per machine)

```bash
git clone https://github.com/pmcfadin/cqlite && cd cqlite
bash scripts/bootstrap-agent-machine.sh        # or manually: sccache, cargo-nextest, bash>=4.3
bash test-data/scripts/fetch-datasets.sh       # real SSTable binaries — REQUIRED (see below)
gh auth status                                  # must include the 'project' scope (board access)
```

Sanity check: `bash scripts/agent-gate.sh --lite` should pass in ~1–5 min, and the SUMMARY's
`accelerators:` line should read `sccache=on nextest=on lanes=parallel`. If anything says
`absent`, the gate prints the one-line install fix — do it; a degraded machine is ~3× slower.

**Datasets matter:** without them, parity components skip. Today that still yields an overall
PASS (the dangerous silent green — being fixed as **#2078**, after which the full gate FAILS
loudly instead). Fetch them on every machine, day one.

---

## Laptop A — your lead session (lead + worker)

```bash
claude --agent flow-lead
```

It orients from the board automatically and opens with something like:

> 3 Ready · 1 In Progress (laptop-B, #2081, heartbeat 4m) · NEEDS-YOU: spec approval #2084, product call on binding-parity compression.

### Your morning routine (~15 minutes, highest-leverage thing you do all day)

1. **Approve queued specs.** Say `activate <N>` for anything groomed, or just approve what's
   rendered. The lead shows the full spec + design inline — read it, then say **"approved"** (or
   redline it). *Why this is the routine:* design work waits a median **91 hours** for this
   moment; batching approvals here removes more wall-clock than any machinery change.
2. **Clear the NEEDS-YOU list.** Product calls, scope questions, epic closes. One at a time,
   recommendations first.
3. **Promote work.** Groomed issues land at board `Backlog` (groomed ≠ scheduled). Say
   **"promote #2078 #2079 #2081"** to fill the Ready column. An empty Ready column = the fleet
   idles, by design.

### Then let it work

Say **"work the queue"** (or just `implement <N>`). The lead claims an issue exactly like any
worker — branch push, assignee, board `In Progress` — and drives it through subagents. You can
interrupt at any time to groom an idea, ask "where do things stand", or approve a spec; the heavy
lifting is in subagents and background gates, not your conversation. **[after #2084/#2085]** the
gate → review → merge endgame runs in a disposable "closer" agent and the lead resets between
issues, so the session stays crisp all day — restarting it is also always free (state lives on
the board and disk, never in the window).

If you want Laptop A undistracted for a strategy session: **"hold claims — B has the queue."**

---

## Laptop B (and C, D…) — pure workers

```bash
cd cqlite && claude
```

then type:

```
/worker
```

That's it. The worker claims the top Ready item via the branch protocol, runs the full loop
(implement → lite gate each round → review → full gate ONCE → roborev → PR → **merges its own PR
on green** → finalize → telemetry stamp), then claims the next. It never needs your eyes
mid-issue. Leave it running.

**Hard rule: one session per machine.** Never start a second lead/worker session on a box that
has one — two sessions collide on worktrees and oversubscribe the CPU (SIGKILLed gates, flaked
perf tests, corrupted sccache under load — all field-observed). More throughput = another
*machine*, not another session.

### Overnight / unattended operation **[after #2090]**

Never leave one session grinding all night — context accretes across issues until the worker
degrades, and a session can't judge its own degradation from the inside. Instead, run the
**worker supervisor**: every issue gets a brand-new process (context hard-bounded at one issue),
and the supervisor — not a bare loop — guards the machine:

```bash
bash scripts/local/worker-supervisor.sh          # defaults: MAX_ISSUES=4, 8h ceiling
```

What it guarantees:

- **One issue per session**: each iteration rehydrates from the board, resumes this machine's own
  claim branch first (crash recovery), else claims the next Ready item, works it to merged +
  finalized, and exits. Empty Ready = cheap no-op + backoff.
- **It cannot overload the box**: preflight holds the next iteration while load is high, a dead
  iteration's cargo/gate processes linger, or disk is low — it waits, it never spins. A flock
  makes a second supervisor on the same machine refuse to start.
- **It cannot fail silently**: a push notification (ntfy) on every merge (info) and on any
  stop/hold/breaker-trip (alert). 2–3 consecutive abnormal exits trip the breaker → stop + alert,
  never hot-respawn. One journal line per iteration (issue, verdict, duration, PR).
- **It stops on its own**: at the issue budget or wall-clock ceiling — overnight is "clear a few
  issues safely," not "run unbounded."
- **Stop it yourself:** `touch .worker-stop` (finishes the current issue, then exits).

**Morning check:** your phone already told you the headline. On the lead: `what needs me` —
merged PRs, anything held/reaped, the NEEDS-YOU list. A stale heartbeat *plus* no alert received
= the supervisor itself died — the one unambiguous alarm.

Safe by construction: a worker session holds zero irreplaceable state (claim = origin branch,
code = worktree commits, criteria = issue body, verdict = summary file, next = board).

---

## What you'll be asked, and what you never will be

**You WILL be asked (the only interrupts):**
- **Seam 1:** "Here's the spec + design for #N — approve?" (design-driven work only)
- **NEEDS-YOU:** product decisions, scope/title changes, epic closes, genuine design-call review
  findings, `HOLD` conflicts. Always as a list with a recommendation.

**You will NEVER be asked to:** merge a green PR (workers merge their own), re-run a gate, read a
gate log (the ~15-line SUMMARY block is all anyone sees), or arbitrate a claim conflict (the
branch lock prevents them — 0 collisions in 174 issues).

## Phrasebook

| You say | What happens |
|---|---|
| `groom <idea>` | One scoped issue, oracle/design routed, lands at Backlog |
| `promote #N #M` | Board Status → Ready; the fleet may now claim them |
| `activate <N>` | Worktree + OpenSpec; spec rendered for your approval (Seam 1) |
| `approved` | Implementation begins |
| `implement <N>` / `work the queue` | This session claims and drives it |
| `where do things stand` / `what needs me` | Board render + the single furthest-along item |
| `hold claims` | This session stops picking up new issues |
| `HOLD: merge #X after #Y` | Ordering constraint workers must obey |
| `finalize <N>` | (Rarely needed — workers self-finalize after merging) |

---

## Reading the board

`what needs me` on any lead shows: item · Status · assignee · priority · claim (origin branch) ·
**[after #2089]** machine + heartbeat age. Interpretation:

- **Ready, no claim branch** → next thing a worker will grab
- **In Progress, heartbeat fresh** → leave it alone
- **In Progress, heartbeat stale** → **[after #2089]** auto-reaped (Status → Ready, work
  preserved on the branch); today: the flow-board reaper flags it and asks you
- **Ready but branch already on origin** → parked-by-design (e.g. spec approved, awaiting a
  team) — pickup is *resume that branch*, never a fresh claim

## Recovery scenarios (all safe by construction)

| Scenario | What to do |
|---|---|
| Laptop lid closed mid-issue | Nothing. Commits are on the origin branch. Reopen and say `implement <N>` — it resumes from the worktree. |
| Session feels degraded / bloated | Kill it, start fresh. Board + disk are the state; the new session rehydrates in one board read. |
| Board unreachable (auth/scope error) | The session STOPS by design (labels are decorative, never a dispatch source). Fix `gh auth refresh -s project` and restart. |
| Gate seems hung | It's probably queued: look for `waiting for gate slot (N in use)…`. Queued ≠ hung. |
| Green SUMMARY but parity lines say SKIP | Datasets missing on that machine — `fetch-datasets.sh`, re-run. (#2078 makes this a hard FAIL so it can't slip through.) |
| Two machines want the same issue | Impossible past the claim: the second branch push fails; the loser re-reads and picks the next Ready item. |

---

## The two dials you own

1. **Approval cadence** (Seam 1). The system's real rate limiter — median 29.4h backlog vs
   16-minute merges. Batch approvals at session start; keep the Ready column non-empty.
2. **Fleet size.** Each additional machine = `git clone` + bootstrap + `/worker` = one more
   concurrent issue. Coordination cost of machine N+1: one branch push per claim.

*Written 2026-07-06 from the agentic-workflow audit. Update this page in the same change whenever
flow-* doctrine changes (doctrine-current rule).*

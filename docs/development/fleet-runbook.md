# Fleet Runbook — Running the Agentic Delivery Pipeline as the Owner

The exact, user-level version of how to run CQLite delivery across one or more machines.
This is the *operator's* doc — what you type, what you'll see, when the system needs you.
Doctrine and internals live elsewhere ([delivery pipeline](https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/),
`docs/development/pm-operating-loop.md`, `CLAUDE.md`); this page is the driver's seat.

> **Status:** the context-economy restructure (Tier-2 epic #2083, audit
> `docs/reports/agentic-workflow-audit-2026-07-06.md`) has landed — the disposable per-issue closer,
> inter-issue lead reset, claim heartbeats + deterministic reap, and the worker supervisor all work today.

---

## The model in one paragraph

**One Claude Code session per machine. N machines = N issues in flight.** Each session claims work
by acquiring the slugless fixed-name ref `refs/claims/issue-<N>` on origin (`scripts/flow/claim.sh`,
the cross-machine lock — #2665), works it in an isolated worktree, and merges its own PR when the
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
bash scripts/flow/claim.sh smoke               # preflight: prove origin accepts refs/claims/* (see below)
```

**Claim-ref preflight (#2665):** the cross-machine lock is a push to the `refs/claims/*` ref
namespace on origin — `claim.sh smoke` creates, `ls-remote`s, and deletes a throwaway
`refs/claims/smoke-<nonce>` ref to confirm the remote permits it (`SMOKE-OK` = good). This is
**verified working on github.com/pmcfadin/cqlite** (2026-07-17). Run it **once when adopting a new
remote or host** — a managed Git host that restricts custom ref namespaces would make the whole
claim mechanism unusable, and that must be caught before the fleet relies on it. **Non-unique
hostnames:** the claim holder identity is `hostname -s`; on a fleet of cloud images/containers/cloned
VMs that report the *same* short hostname, export a UNIQUE `CLAIM_MACHINE` per box (else two machines
share one identity and each treats the other's claim as its own).

Sanity check: `bash scripts/agent-gate.sh --lite` should pass in ~1–5 min, and the SUMMARY's
`accelerators:` line should read `sccache=on nextest=on lanes=parallel`. If anything says
`absent`, the gate prints the one-line install fix — do it; a degraded machine is ~3× slower.

**Datasets matter:** without them, parity components skip. The FULL gate FAILs CLOSED when the
fetched validation corpus is absent (**#2078**), stamping `missing-fixtures: FAIL-CLOSED (#2078)`;
`AGENT_GATE_ALLOW_MISSING_FIXTURES=1` opts out visibly, and `--lite`/`--only` stay lenient. Fetch
them on every machine, day one.

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
lifting is in subagents and background gates, not your conversation. The gate → review → merge
endgame runs in a disposable "closer" agent and the lead resets between issues (issues #2084/#2085),
so the session stays crisp all day — restarting it is also always free (state lives on the board and
disk, never in the window).

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

That's it. The worker claims the top Ready item via the claim protocol, runs the full loop
(implement → lite gate each round → review → full gate ONCE → roborev → PR → **merges its own PR
on green** → finalize → telemetry stamp), then claims the next. It never needs your eyes
mid-issue. Leave it running.

**Hard rule: one session per machine.** Never start a second lead/worker session on a box that
has one — two sessions collide on worktrees and oversubscribe the CPU (SIGKILLed gates, flaked
perf tests, corrupted sccache under load — all field-observed). More throughput = another
*machine*, not another session.

### Overnight / unattended operation

Never leave one session grinding all night — context accretes across issues until the worker
degrades, and a session can't judge its own degradation from the inside. Instead, run the
**worker supervisor**: every issue gets a brand-new process (context hard-bounded at one issue),
and the supervisor — not a bare loop — guards the machine:

```bash
bash scripts/local/worker-supervisor.sh          # defaults: MAX_ISSUES=4, 8h ceiling
```

Each iteration spawns a headless worker with the validated invocation (issue #2841):

```bash
claude -p --output-format stream-json --verbose --dangerously-skip-permissions --agent flow-lead '/worker'
```

`-p` runs the prompt to completion and exits (no interactive TUI to block on); `--agent flow-lead`
is the registered orchestrator (`worker` is a `/`-command/skill, **not** an agent — `--agent worker`
exits 1); `--dangerously-skip-permissions` lets an unattended session run `gh`/`git` without a human
approving each prompt; `--output-format stream-json --verbose` streams the worker's live activity to
stdout so the supervisor's per-iteration redirect captures it (see monitoring, below). Override the
whole command with `WORKER_CMD` if needed; the default is what the supervisor uses when you don't.

What it guarantees:

- **One issue per session**: each iteration rehydrates from the board, resumes this machine's own
  claim branch first (crash recovery), else claims the next Ready item, works it to merged +
  finalized, and exits. Empty Ready = cheap no-op + backoff.
- **It cannot overload the box**: preflight holds the next iteration while load is high, a dead
  iteration's cargo/gate processes **or an orphaned worker Claude CLI** (the unattended
  `claude -p … --agent flow-lead` spawn shape, #2670/#2841)
  linger, or disk is low — it waits, it never spins. A flock makes a second supervisor on the same
  machine refuse to start. (The Claude probe keys on the supervisor's own `-p … --agent flow-lead`
  spawn shape, so a legitimate interactive `claude` REPL or an interactive `claude --agent flow-lead`
  lead session — neither carries `-p` — is not matched.) A
  hold cannot latch it silently: every hold pass re-checks the stop-file and the wall-clock budget,
  and a leftover hold that never clears stops the loop loudly, paging the surviving PIDs (#2670).
  The two leftover families are bounded **separately** (#2670): a non-self-clearing orphaned worker
  CLI (`leftover-worker`) trips the tight `LEFTOVER_HOLD_MAX` (default 3 ≈ 15 min), while a
  self-clearing build/gate process (`leftover-build`: cargo/nextest/gate-slot-daemon) gets the loose
  `BUILD_HOLD_MAX` (default 12 ≈ 1 h, `<=0` disables) so a legitimate concurrent full gate (15–25 min)
  is waited out, never mistaken for a stuck orphan.
- **It cannot be fooled by a false finalize (#2670)**: a `finalized` marker is trusted only after
  the claimed PR gh-verifies as MERGED (via `state,mergedAt,autoMergeRequest`). A worker that parked
  its endgame yet wrote `finalized` is caught (`verified: mismatch:<state>`, confirmed across grace
  re-reads that absorb read-after-merge lag), paged high, judged abnormal, and never credited; a
  forged PR reference — non-numeric, a non-pmcfadin/cqlite URL, or one gh *resolves as absent* (gh's
  `could not resolve to a PullRequest` signature only — a transport `not found` like DNS/proxy 404 is
  **not** forgery) — is `mismatch:UNRESOLVED` (same escalation). An OPEN PR with **auto-merge armed**
  is the closer's legitimate path, judged `finalized-pending-automerge` (uncounted, breaker-neutral),
  not a false finalize. Such PRs are tracked **per-PR** (#2670): each is re-verified on later
  iterations and, once it reaches MERGED, **retroactively credited** toward `MAX_ISSUES`
  (`pending-credited`) — so a fast fleet with several *distinct* PRs pending at once is never mistaken
  for a stuck one. Only when the **same** PR is observed still-unmerged across `PENDING_AUTOMERGE_MAX`
  consecutive iterations **and** has been pending at least `PENDING_AUTOMERGE_MIN_SECS` (a wall-clock
  floor above CI time, so a burst of fast no-progress iterations can't burn the budget) is it
  auto-merge-stuck and the loop stops (`automerge-stuck`); a tracked PR that instead ends
  CLOSED-unmerged pages high (`pending-dropped`), never silently swallowed. A GitHub
  *outage* — or a missing JSON
  parser, a tooling gap that must never read as forgery — yields a neutral `finalized-unverified`
  (paged, uncounted, breaker untouched); a **persistent** outage is bounded: `UNVERIFIED_MAX`
  consecutive unverifiable finalizes stop the loop (`verify-unavailable`), so the `MAX_ISSUES` ceiling
  can't drift.
- **It cannot fail silently**: a push notification (ntfy) on every merge (info) and on any
  stop/hold/breaker-trip (alert). 2–3 consecutive abnormal exits trip the breaker → stop + alert,
  never hot-respawn. One journal line per iteration (issue, verdict, duration, PR, `verified`).
- **It never wedges on a question (#2666)**: a worker that hits Seam 1 or a genuine owner decision
  **parks** (posts a `needs-decision` question comment + EXITs) rather than waiting — the supervisor
  judges it `parked-on-owner` and pages the owner once. A worker that nonetheless gets stuck on an
  interactive prompt is caught mid-iteration by a log-tail watchdog and paged as `stuck-on-question`.
  **Neither counts toward the crash breaker.** The watchdog reads the per-iteration capture at
  `$LOG_DIR/iter-<N>.log` — under `-p` a worker writes its narrative to the session transcript, not
  stdout, so the supervisor's default `WORKER_CMD` adds `--output-format stream-json --verbose`
  precisely so the redirect captures a live event stream; the watchdog's "prompt signature in the
  tail AND log size frozen across two scans" logic then works, and the log stays useful to a human
  (a wedged worker's byte size freezes exactly when the stream stops). **Watch a live worker** with
  `tail -f "$LOG_DIR/iter-$(ls -1 "$LOG_DIR" | grep -oE 'iter-[0-9]+' | sort -t- -k2 -n | tail -1 | cut -d- -f2).log"` (or simply `tail -f "$LOG_DIR"/iter-*.log`).

**Per-iteration verdicts** (one journal line each):

| Verdict | Meaning | Breaker |
|---------|---------|---------|
| `finalized` | claimed → gate/review → merge-on-green → finalized (`issue`+`pr` set) **and the PR gh-verifies as MERGED** (#2670); journal `verified: merged` | resets |
| `finalized-unverified` | well-formed finalize, but gh could not confirm the merge — gh missing / network / rate limit, **or no JSON parser present** (a tooling gap is never read as forgery, #2670); journal `verified: unverified`, default-priority page, **not counted** toward the issue budget | **neutral** (neither trips nor resets) |
| `finalized-pending-automerge` | PR is OPEN with auto-merge armed (the closer's auto-merge path, #2670) — it will land; journal `verified: pending-automerge`, default-priority page, **not counted yet**, tracked per-PR for retroactive credit; the **same** PR still-unmerged `PENDING_AUTOMERGE_MAX` iterations in a row ⇒ `automerge-stuck` stop | **neutral** |
| `pending-credited` | a previously `finalized-pending-automerge` PR re-verified as MERGED on a later iteration (#2670) — **retroactively counted** toward `MAX_ISSUES`; journal `verified: merged` | **neutral** |
| `pending-dropped` | a tracked armed PR that ended **CLOSED-unmerged** (auto-merge dropped / PR closed) on re-verification (#2670) — HIGH "armed PR did not land" page, dropped uncredited (never silently swallowed) | **neutral** |
| `no-work` | nothing Ready / nothing to resume — backoff, then retry | resets |
| `blocked` | stopped short of merge for an owner escalation; same issue twice ⇒ head-blocked stop | resets |
| `parked-on-owner` | clean park (#2666): `blocked` marker with `reason: seam1-approval\|needs-decision`; high page, loop advances | **never** |
| `stuck-on-question` | worker wedged on a prompt, detected mid-iteration; high page with the captured text | **never** |
| `abnormal` | nonzero exit / missing / malformed marker / unknown outcome / **finalized marker whose PR is a stable non-merged state** (`verified: mismatch:<state>`, after grace re-reads) **or a forged PR ref** (`verified: mismatch:UNRESOLVED` — non-numeric, foreign-host URL, or gh-unresolvable); high page naming the discrepancy (#2670) | **+1** |
- **It stops on its own**: at the issue budget or wall-clock ceiling — overnight is "clear a few
  issues safely," not "run unbounded."
- **Stop it yourself:** `touch .worker-stop` (finishes the current issue, then exits).

**Morning check:** your phone already told you the headline. On the lead: `what needs me` —
merged PRs, anything held/reaped, the NEEDS-YOU list. A stale heartbeat *plus* no alert received
= the supervisor itself died — the one unambiguous alarm.

Safe by construction: a worker session holds zero irreplaceable state (claim = origin ref,
code = worktree commits, criteria = issue body, verdict = summary file, next = board).

---

## What you'll be asked, and what you never will be

**You WILL be asked (the only interrupts):**
- **Seam 1:** "Here's the spec + design for #N — approve?" (design-driven work only)
- **NEEDS-YOU:** product decisions, scope/title changes, epic closes, genuine design-call review
  findings, `HOLD` conflicts. Always as a list with a recommendation.

**You will NEVER be asked to:** merge a green PR (workers merge their own), re-run a gate, read a
gate log (the ~15-line SUMMARY block is all anyone sees), or arbitrate a claim conflict (git's
server-side ref arbitration on `refs/claims/issue-<N>` decides every race — #2665). **History note:**
the earlier slug-named branch lock guaranteed only *same-name* atomicity — two sessions on
*different* slugs, or on an identical `origin/main` SHA, could both "win" (the #1632 slug pair; the
identical-SHA no-op "up-to-date" push). The "collisions impossible by construction / 0 in 174 issues"
claim overstated that: same-slug collisions were prevented, slug/SHA races were not. The fixed-name
claim ref (#2665) is what actually closes the class.

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

`what needs me` on any lead shows: item · Status · assignee · priority · claim (`refs/claims/issue-<N>`) ·
machine + heartbeat age (issue #2089). Interpretation:

- **Ready, no claim ref** → next thing a worker will grab
- **In Progress, heartbeat fresh** → leave it alone
- **In Progress, heartbeat stale** → deterministically reaped by flow-board (heartbeat age > 4h AND no
  open PR → Status → Ready, work preserved on the branch, traceable comment; issue #2089). The
  supervisor also stamps a machine-scoped claim ref `refs/machine-claims/<machine>` that the
  `project-board-sync` 30-min cron's `reap-claims` job reaps on the SAME predicate server-side (age >
  4h AND no open PR AND, for a local claim, PID-dead) — so a supervisor that dies overnight gets its
  claim reaped by CI without waiting for a human to run flow-board (issue #2655)
- **Ready but branch already on origin** → parked-by-design (e.g. spec approved, awaiting a
  team) — pickup is *resume that branch*, never a fresh claim

## Recovery scenarios (all safe by construction)

| Scenario | What to do |
|---|---|
| Laptop lid closed mid-issue | Nothing. Commits are on the origin branch. Reopen and say `implement <N>` — it resumes from the worktree. |
| Session feels degraded / bloated | Kill it, start fresh. Board + disk are the state; the new session rehydrates in one board read. |
| Board unreachable (auth/scope error) | The session STOPS by design (labels are decorative, never a dispatch source). Fix `gh auth refresh -s project` and restart. |
| Gate seems hung | It's probably queued: look for `waiting for gate slot (N in use)…`. Queued ≠ hung. |
| Green SUMMARY but parity lines say SKIP | Datasets missing on that machine — `fetch-datasets.sh`, re-run. The FULL gate FAILs CLOSED here (`missing-fixtures: FAIL-CLOSED (#2078)`) so it can't slip through; `--lite`/`--only` stay lenient. |
| Two machines want the same issue | Impossible past the claim: the second claim-ref push is rejected server-side (non-fast-forward on the fixed-name ref, #2665); the loser sees `CLAIM LOST` and picks the next Ready item. |

---

## The two dials you own

1. **Approval cadence** (Seam 1). The system's real rate limiter — median 29.4h backlog vs
   16-minute merges. Batch approvals at session start; keep the Ready column non-empty.
2. **Fleet size.** Each additional machine = `git clone` + bootstrap + `/worker` = one more
   concurrent issue. Coordination cost of machine N+1: one claim-ref push per claim.

*Written 2026-07-06 from the agentic-workflow audit. Update this page in the same change whenever
flow-* doctrine changes (doctrine-current rule).*

---
title: Delivery pipeline (flow-lead)
description: The manager-orchestrated delivery workflow — the flow-lead agent, the flow-* pipeline, the specialist roster, and the two human seams.
sidebar:
  label: Delivery pipeline
---

CQLite delivery is driven by a **manager agent, `flow-lead`**, that orchestrates a team of specialist
agents through a defined pipeline. Start it as your session driver — `claude --agent flow-lead` (it is
the repo's default agent) — and it orients from the board. It orchestrates; the specialists do the
middle; you sit in two seats.

## The pipeline

```
  flow-groom ─▶ flow-activate ─▶ flow-implement ─▶ flow-address ─▶ flow-finalize
   idea→issue    Seam 1:           team builds,      resolve PR       archive +
   (oracle vs    spec+design,      gate→C→roborev,   comments         cleanup +
    design)      STOP for you      open PR (no merge)                 close issue
                       ▲                                    ▲
                       │                                    │
                  flow-board surfaces the single next thing waiting on you
```

| Verb | What it does |
|------|--------------|
| `flow-groom` | rough idea → one scoped issue (one `P0`–`P3`, `status:ready`, testable criteria); decides oracle vs design |
| `flow-activate` | worktree + branch + `opsx:propose`; renders spec + design inline; **STOPS at Seam 1** |
| `flow-implement` | implement (TDD) → review-first (`rust-reviewer` + roborev on the lite-green diff) → open PR → spawn `flow-closer` for the endgame (full gate → **C** → final roborev → merge → finalize) |
| `flow-address` | resolves PR review comments; re-verifies; pushes; replies |
| `flow-finalize` | `opsx:archive` + **stamp the telemetry ledger** + remove worktree/branch + close issue (post-merge) |
| `flow-board` | status across in-flight work + drives the one item waiting on you |

## Oracle vs design (the routing decision)

- **Oracle-driven** (SSTable parsing, compaction/tombstone parity, type decode) — a Cassandra/sstabledump
  source of truth exists. Issue + a pinned parity test; **skip OpenSpec**; groom → implement.
- **Design-driven** (bindings/M6, query-engine surface, CLI/REPL UX, perf/M7, process) — no oracle.
  Goes through `flow-activate` (OpenSpec proposal/design/specs/tasks).

## The two human seams

1. **Spec approval** (Seam 1, in `flow-activate`) — you approve the OpenSpec spec + design before any
   implementation. The lead renders it inline and stops.
2. **Merge** (Seam 2) — **merge-on-green** (no human merge click for worker-owned issues):
   - A worker's **terminal state** for an issue is PR-open + `agent-gate.sh` PASS + C PASS (design-driven)
     + roborev clean. At that point it **arms the merge-on-green mechanism and ends its turn** — it does
     **not** poll the PR's own external CI in a yield/wake loop (see [Merge-on-green](#merge-on-green-no-ci-busy-wait)).
   - *Always escalated, never decided by the lead:* product decisions, scope/title changes, epic closes.

## Merge-on-green (no CI busy-wait)

Once a worker reaches its terminal state (PR-open + gate PASS + C PASS + roborev clean) it **arms** a
merge-on-green mechanism and **stops**. It must **not** busy-poll its PR's own external CI — repeatedly
waking (`ScheduleWakeup`) to watch the cross-platform matrix after the work is done is pure token bleed and
is prohibited. Landing on green is delegated:

- **Primary today — the manager-owned poller.** `main` currently has **no required status checks**
  (`contexts=[]`), so a naive `gh pr merge --auto` would merge instantly against an empty check set — which
  the green-signal guard forbids. The worker hands the PR to the manager-owned poller/merge-engine, which
  gates on an explicit lane set and lands it on green. The poller runs **once for the whole fleet** at the
  manager level, not per worker.
- **`gh pr merge --auto --squash --delete-branch` — primary once required checks are configured on `main`.**
  With real required checks in place, `--auto` is the zero-token native path: GitHub lands the PR when the
  required checks pass and auto-closes the issue via `Closes #N`.

The worker **logs which path it armed**. **Green-signal guard:** merge-on-green lands only once a *defined*
green signal exists (configured required checks or the poller's explicit lane set) — never against an empty
required-check set. `flow-finalize` runs on the merge event (not a CI busy-wait). `ScheduleWakeup` remains
valid for genuinely external, harness-untracked state — just not for polling a PR's own CI after the work
is complete.

### GitHub-enforced merge gate (#2433)

`main` now carries **full branch protection**: the `required` status check (the "Required PR Gate" CI
workflow) is a required context, with `enforce_admins` on. Merge-on-green is therefore
**local gate PASS + C (design) + roborev clean *and* the GitHub `required` check green** — the last term
is machine-enforced, not honor-system. Because `enforce_admins` is enabled, even `gh pr merge --admin`
is refused while the check is pending or red (proven on probe PR #2441: plain and `--admin` merges both
rejected with `mergeStateStatus: BLOCKED`), so **there is no bypass**. A red that is a known flake gets
`gh run rerun --failed` — never an admin override. This is load-bearing: if branch-protection settings
ever regress (contexts emptied, `enforce_admins` disabled), this doctrine governs catching it.

### Closer merge protocol (#2456)

The `flow-closer` certifies a **specific SHA** — the tree the full gate of record and the final
roborev pass actually ran on. Three mechanical rules keep the merge honest:

- **Pre-merge SHA assertion (#2456, hard precondition).** Immediately before `gh pr merge`, the closer
  does `git push`, then asserts `gh pr view <N> --json headRefOid` **equals the locally-certified
  tip** — and **refuses to merge on mismatch**. Motivated by the 2026-07-14 stale-merge escape on
  #2299/PR #2421: the closer certified a rebased-and-fixed tip locally but never pushed it, so
  `gh pr merge` squashed the PR's *stale* pre-fix head and transiently landed a known data-loss
  blocker on `main` (remediated by PR #2455). The GitHub required check re-runs on push but cannot
  catch a "merge of an old green head" — the SHA assertion is the real guard.
- **Unique gate-summary paths.** Each gate writes its `AGENT_GATE_SUMMARY_FILE` to a `mktemp`-unique
  path (e.g. `$(mktemp /tmp/gate-<issue>-XXXXXX.txt)`) — shared `/tmp` names get contended under
  multi-lane load, so one lane's summary can clobber or be misread as another's.
- **Single full gate per machine.** The lead enforces one full gate at a time on a box; the closer
  `pgrep`-checks for a running gate before launching its own so concurrent gates never corrupt a
  shared `target/`.

## The specialist roster

| Role | Agent / tool |
|------|--------------|
| implement / format debug (TDD) | `sstable-developer` |
| review-first (Rust review) | `rust-reviewer` — on the lite-green diff, BEFORE the full gate |
| endgame owner (full gate → C → final roborev → merge → finalize) | `flow-closer` — per issue, disposable context |
| intent audit (C) | `spec-auditor` (anchored to `openspec/changes/<name>/specs/**`) — see [Spec-driven audit](/cqlite/agents-developing/spec-driven-audit/) |
| parity / test execution | `test-validator` |
| test quality | `coverage-reviewer` |
| code review | roborev (review-first + the closer's final pass) |
| correctness | `scripts/agent-gate.sh` — the ONE gate of record, inside `flow-closer` |

## State model

- **Backlog** = GitHub issues; the Project `Status` field is the authoritative lifecycle
  (`Backlog → Ready → In Progress → In Review → Done`). Each issue carries one `P0`–`P3`; `status:*`
  labels are decorative only (Path A, #1886 — see [the claim board](#the-shared-claim-board)).
- **1:1:1:1** — one issue ↔ one worktree/branch `issue-<N>-<slug>` ↔ one OpenSpec change `<slug>` ↔ one
  PR. Worktrees branch from `origin/main` and lack the gitignored `Data.db` binaries — run the gate with
  `CQLITE_DATASETS_ROOT` pointed at the main repo's `test-data/datasets`.
- The **definition of done** is the [spec-driven audit](/cqlite/agents-developing/spec-driven-audit/)
  one: `agent-gate.sh` PASS + C PASS + roborev clean.

## The shared claim board

In-flight work is tracked on a shared **GitHub Project (v2)** with a single-select `Status` field
(`Backlog → Ready → In Progress → In Review → Done`). It is the cross-session, cross-machine view — and
the thing a human can also drive from mobile. `flow-board` renders it (`gh project item-list`) showing
each item's status, assignee, and priority; built-in server-side Project automations move items on
GitHub-side events (PR merged / issue closed → `Done`, assigned → `In Progress`), so the board stays
fresh even when an action came from the phone or web with no `flow-*` run.

**One-time setup (the owner's action):** Projects v2 needs the `project` token scope —
`gh auth refresh -s project` — then run `test-data/scripts/setup-project-board.sh` to create + link the
board and normalize the `Status` options. The built-in workflow automations (merge/close → `Done`,
assigned → `In Progress`) cannot be set via CLI; the script prints the manual web-UI step for them.

**Path A — the board is the sole dispatch authority (issue #1886):** work is selected and claimed by
the Project `Status` field ONLY. `status:*` labels are **decorative and non-authoritative** — never use
them to select or claim work. If the `project` scope or the board is **unreachable, STOP and fix the auth**
(`gh auth refresh -s project`) — do **not** fall back to labels to find work. An empty `Ready` column means
no work is ready (near a release it is *meant* to drain to zero), not a cue to dredge labels.

## The claim protocol (no duplicate work)

Before working an item, a session claims it so no two sessions — **including two sessions authenticated
as the same GitHub user on different machines** — work the same item. Because assignee `@me` is identical
for the same user on two machines, the assignee is *not* the lock; the deciding lock is the
**`issue-<N>-<slug>` branch pushed to origin** (server-side, per-machine-distinct, the natural 1:1:1:1
artifact).

1. **Eligibility** — the item is `Ready` AND has **no** existing `issue-<N>-*` branch on origin
   (`git ls-remote --heads origin "issue-<N>-*"`).
2. **Claim** — push the `issue-<N>-<slug>` branch to origin (the cross-machine lock), then set assignee
   `@me` + `Status=In Progress` for board visibility. `flow-activate` pushes the branch immediately —
   before any spec work — as the claim; oracle-driven issues claim in `flow-implement`.
3. **Re-read** — confirm origin's branch is your commit (you won the race) and proceed only if you hold
   it; otherwise back off and take the next eligible item.

Another machine that finds an existing claim branch can `git fetch` it to **resume** that work instead of
colliding. The claiming session also maintains a liveness **heartbeat** (`scripts/flow/claim-heartbeat.sh
beat <N>` — a cheap origin git ref under `refs/heartbeats/<machine>`, never a GitHub API call — refreshed at
claim time and on every stage transition: activate/implement/gate/PR). `flow-board` reaps **abandoned
claims deterministically** (issue #2089): an `In Progress` item is reaped only when its heartbeat age
exceeds the documented threshold (4h — the `claim-heartbeat.sh` header is the single source of truth) **AND**
it has no open PR — reap = a traceable comment + assignee clear + `Status → Ready` + a claim-branch note
(never deleting a branch that carries commits). This replaces the old "no recent commits" guess.
`flow-finalize` releases the claim by deleting the origin branch and clearing the heartbeat on cleanup.

For unattended/overnight runs a **worker supervisor** (`scripts/local/worker-supervisor.sh`, issue #2090)
recycles one worker process per issue — the hard context bound is process exit: the worker rehydrates from
the board, resumes this machine's own claim branch first (crash recovery) else claims the next Ready item,
runs it to merged + finalized, writes a `.worker-last-iteration.json` marker, and **exits** (never a second
issue per session). The supervisor adds a flock single-instance (mechanizing one-worker-per-machine),
fail-closed preflight (load/disk/leftover-process/stop-file), a crash-loop breaker, budgets, and ntfy
notifications. See the [fleet runbook](https://github.com/pmcfadin/cqlite/blob/main/docs/development/fleet-runbook.md).

## Concurrency model

- **One active worker per machine; the worker paces the machine's load (#1930).** A single lead/worker
  session owns a machine at a time — the load + worktree-isolation rule that sits *above* the claim
  protocol. Two efforts on one box collide on the shared worktree and oversubscribe the CPU, which flakes
  scheduling-sensitive tests (write-throughput, the streaming GIL-release test) and can SIGKILL gates. The
  owning worker is responsible for load: **serialize your own full-gate runs — never two full
  `scripts/agent-gate.sh` at once on one box** (the machine-wide gate cap is a backstop, not a license to
  overlap). **Subagents are exempt:** a worker fanning out `sstable-developer`/reviewers is not "multiple
  workers" — they never launch competing full gates. The rule targets independent lead/worker *sessions*.
- **Default (recommended): one lead → subagents.** A single `flow-lead` spawns subagents and assigns each
  **disjoint** work — zero duplicate work by construction.
- **Multiple independent sessions: the claim protocol is mandatory.** Each acquires work only through the
  claim protocol above — and, per the rule above, independent sessions belong on *separate* machines
  (one-per-machine handles a single box; different machines coordinate via the pushed branch lock).
- **Agent Teams is optional, desktop-only.** `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` gives a built-in
  file-locked shared task list for coordinated parallel sessions, but it is experimental and desktop/tmux
  -only (no `/resume`, one team per session). Use it if you want; it is not required.
- **Never run N bare `flow-lead`s without the claim protocol** — independent leads with no claim each pick
  the same top `Ready` item and collide.

## Driving from mobile / remote

The Claude Code mobile app cannot run the local pipeline itself (no local bash, skills, worktrees, or the
dataset binaries). Two supported ways to still drive work from the phone:

- **Remote Control (primary).** Run `claude remote-control` on the laptop and connect from the mobile
  app; the phone drives the **full local `flow-*` pipeline** (worktrees, `agent-gate.sh`, `gh`,
  `openspec`) in that local session. The laptop must stay online.
- **Claude Code on the web (secondary, cloud).** A cloud session uses the repo-committed `.claude/`
  (skills/agents/hooks) but not user-scoped config or local data. Run the **cloud setup script**
  `test-data/scripts/cloud-setup.sh` first — it installs `openspec` + `gh` and fetches the dataset
  (`fetch-datasets.sh`) so `flow-implement` can run the gate in the cloud.

**Spec approval is the only standing human seam, and it is GitHub-mobile-native** regardless of how you
drive: approve the OpenSpec spec + design in the session (Seam 1). For worker-owned issues **merge is no
longer a hand-merge step** — the PR auto-lands via [merge-on-green](#merge-on-green-no-ci-busy-wait)
(the manager-owned poller today; `gh pr merge --auto` once required checks are configured on `main`), and
the merge event moves the board item to `Done`. The owner intervenes on merge (from the mobile app / web
UI) **only on escalation** — a genuine design-call roborev finding, a scope/product question, or work
outside the issue.

## The implement loop: review before gate, gate once at the end (issues #1821, #2084, #2086, #2087, #2088)

Inside `flow-implement` the loop is ONE coherent design, not three patches:

```
implement (TDD) → lite (each fix round) → rust-reviewer + roborev on the lite-green diff
  (review-first, DEFAULT) → fix (lite re-cert + diff-scoped targets, NEVER a full gate)
  → open PR → flow-closer { FULL gate ONCE → C → final roborev → merge-on-green → finalize }
```

- **Review-first is the default (issue #2086).** `rust-reviewer` + roborev run on the **lite-green** diff
  **before** the first full gate, so review discovers fixable problems before we pay for the 12–25 min gate.
  Skip only for a genuinely mechanical diff (no `pub`-item change AND single call site AND no new surface).
- **Scoped re-cert, one full gate (issue #2087).** A roborev blocker that touches src re-certifies with
  `scripts/agent-gate.sh --lite` (blast-radius-scoped) + any diff-relevant parity/integration target — NOT
  a full gate. The single full gate of record runs **once**, immediately pre-merge; lite re-certs (their
  `MODE: lite` marker) are never the gate of record.
- **Severity-triaged findings (issue #2088).** Findings are classified per the
  [roborev severity rubric](https://github.com/pmcfadin/cqlite/blob/main/docs/development/roborev-severity.md):
  **blockers** (correctness, data-parity, no-heuristics, safety, wiring-evidence, security, any acceptance
  criterion) are fixed pre-merge; **nits** (style, naming, comment/doc polish, no-repro test suggestions)
  are batched into ONE linked follow-up issue at merge time and never trigger a re-verify round. When in
  doubt, blocker.
- **The disposable `flow-closer` owns the endgame (issue #2084).** `flow-implement` opens the PR, then
  spawns a per-issue `flow-closer` that runs the ONE full `scripts/agent-gate.sh` of record (via
  `run_in_background` + the summary-file pattern — it **never idle-waits**, which would trip the #1855 stall
  watchdog and orphan the gate), the **C** intent audit, the final roborev pass, then merges on green and
  `flow-finalize`s. It returns only a terminal packet (verdict, PR URL, summary-file path, ≤10 lines
  residual), so gate stdout and review churn die with its context instead of accreting in the lead session.
  Any src change after the full gate INVALIDATES it — the gate of record must postdate the final src change
  and rebase.
- **Division of labor.** An implementer subagent (`sstable-developer`) edits/commits/pushes and verifies
  with `--lite`/targeted tests **only** — it must **never** invoke the full gate.

Every gate invocation — full and `--lite` — uses the **summary-file redirect** by default
(`AGENT_GATE_SUMMARY_FILE=<path> … > gate.log 2>&1 < /dev/null`, then `cat <path>`); raw gate stdout is
never read into a persistent agent context (issue #2079). See the
[gate contract](/cqlite/agents-developing/gate-contract/) for the summary-file default and the
`accelerators:` line.

## Inter-issue reset for the lead (issue #2085)

The `flow-lead` is the only long-lived agent, so it compacts between issues: after each `flow-finalize` it
carries **zero prior-issue history** (board renders, gate summaries, roborev findings, PR bodies, and
Seam-1 spec renders are dropped — `spec-auditor` re-reads specs from `openspec/changes/<slug>/` anyway),
re-hydrates the **next** item from the **board alone**, and stays re-runnable from board + disk state at any
point (worktree, origin claim branch, issue/PR bodies, OpenSpec files, summary files, telemetry ledger).
Durable cross-issue lessons route to `MEMORY.md` / `process_improvements.md`, never the live window. The
same board-only rehydration rule applies to worker sessions (see the supervisor below).

## Machine setup + accelerators

A fresh machine that will run the pipeline should first run
`bash scripts/bootstrap-agent-machine.sh` (details in `docs/development/agent-machine-setup.md`): it
verifies the gate accelerators (`sccache`, `cargo-nextest`, modern bash — issue #1848), the datasets +
`CQLITE_DATASETS_ROOT`, `gh` auth + the `project` scope, and roborev's local config. **roborev follows the
machine's configured agent** (commonly `codex` via `.roborev.toml`; no flags) — explicit `--agent`/`--model`
is a per-machine troubleshooting override only, never doctrine.

## Pipelining independent lanes (retro #1889)

The lead **pipelines** near-independent issues rather than serializing on long waits (a full gate is
15–25 min, plus CI and roborev round-trips):

- While one lane's full gate / CI / roborev runs, the lead advances **other independent lanes** —
  implementation and review stages overlap freely.
- Merge-on-green is **armed per PR** (it lands when green) rather than blocking the queue on each PR's CI.
- **Only the full-gate step serializes** across lanes (respecting the #1825 machine-wide cap and measured
  ~2-gate contention); everything else overlaps.
- Long waits use **scheduled wakeups**, never idle polling.

## Operational caveats

- **Subagent model pin.** The `model:` pinned in a subagent's frontmatter is not always accessible — always
  pass an explicit, accessible `model` (e.g. `opus`) when spawning, or the spawn fails.
- **GitHub REST resilience.** Board / `gh` operations run in bursts and can hit GitHub's secondary rate
  limits. Batch reads (one `gh project item-list` over per-item polls) and, on a `403`/secondary-limit
  response, back off and retry rather than failing the run.

## Self-improvement loop (telemetry + retro)

The pipeline measures itself so improvement is data-driven, not anecdotal — **sense → diagnose → improve**:

- **Sense.** `flow-finalize` stamps one record per delivery cycle (issue, pr) into the append-only
  ledger `docs/reports/delivery-telemetry.jsonl` (governed by
  `docs/reports/delivery-telemetry.schema.json`) using `scripts/delivery-telemetry.py record`. A
  reopened issue that ships more than once legitimately gets one record per shipped PR — retro
  aggregation by issue treats such multi-cycle issues as multiple deliveries, not one (issue #2314).
  Records carry **authoritative data only**: GitHub-derived
  timestamps (issue/PR open + merge + close → cycle time and coarse phase durations) plus run-observed
  counters — claim collisions, rebase/conflict events, agent-gate pass/fail + run count, roborev findings,
  and rework. A counter that was not observed is an **error**, never a fabricated `0` (no-heuristics
  mandate). `delivery-telemetry.py lint` schema-validates every line.
- **Diagnose.** On a cadence (per-epic or weekly) the manager runs `delivery-telemetry.py retro`, which
  ranks the recorded failure categories by a **documented weighted tally** (`Σ count × weight` — a
  deterministic policy table, not an inferred or learned model) and reports the single highest-cost
  recurring failure. Default is a dry-run print; `--file` files a `flow-meta` improvement issue, **deduped**
  against open `flow-meta` issues by a stable category marker.
- **Improve.** That `flow-meta` issue enters Ready and flows through the normal pipeline.

The `delivery-telemetry` agent-gate component (SKIP-aware on `python3`) covers the tool: schema
round-trip, lint-rejects-malformed, fixture-ledger → expected top failure, and dedupe.

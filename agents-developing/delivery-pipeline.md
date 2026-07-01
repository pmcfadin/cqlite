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
| `flow-implement` | spawns the team in the worktree; runs `agent-gate` → **C** → roborev; opens the PR |
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

## The specialist roster

| Role | Agent / tool |
|------|--------------|
| implement / format debug (TDD) | `sstable-developer` |
| intent audit (C) | `spec-auditor` (anchored to `openspec/changes/<name>/specs/**`) — see [Spec-driven audit](/cqlite/agents-developing/spec-driven-audit/) |
| Rust review | `rust-reviewer` |
| parity / test execution | `test-validator` |
| test quality | `coverage-reviewer` |
| code review | roborev |
| correctness | `scripts/agent-gate.sh` |

## State model

- **Backlog** = GitHub issues + labels: one `P0`–`P3`, lifecycle
  `status:{ready, spec-review, in-progress, in-review, addressing}`.
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

**Graceful degradation:** if the `project` scope or the board is absent, every `flow-*` skill falls back
to the existing `status:*` label model (+ assignee) and never blocks — the Project is purely additive.

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
colliding. `flow-board` reaps **abandoned claims** — an `In Progress` item whose branch has had no recent
commits (the claiming session died) is flagged as STALLED for reclaim or finish, so a crash never leaks a
stuck item. `flow-finalize` releases the claim by deleting the origin branch on cleanup.

## Concurrency model

- **Default (recommended): one lead → subagents.** A single `flow-lead` spawns subagents and assigns each
  **disjoint** work — zero duplicate work by construction.
- **Multiple independent sessions: the claim protocol is mandatory.** Each acquires work only through the
  claim protocol above.
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

## Self-improvement loop (telemetry + retro)

The pipeline measures itself so improvement is data-driven, not anecdotal — **sense → diagnose → improve**:

- **Sense.** `flow-finalize` stamps one record per completed issue into the append-only ledger
  `docs/reports/delivery-telemetry.jsonl` (governed by `docs/reports/delivery-telemetry.schema.json`)
  using `scripts/delivery-telemetry.py record`. Records carry **authoritative data only**: GitHub-derived
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

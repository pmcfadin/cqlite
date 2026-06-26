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
| `flow-finalize` | `opsx:archive` + remove worktree/branch + close issue (post-merge) |
| `flow-board` | status across in-flight work + drives the one item waiting on you |

## Oracle vs design (the routing decision)

- **Oracle-driven** (SSTable parsing, compaction/tombstone parity, type decode) — a Cassandra/sstabledump
  source of truth exists. Issue + a pinned parity test; **skip OpenSpec**; groom → implement.
- **Design-driven** (bindings/M6, query-engine surface, CLI/REPL UX, perf/M7, process) — no oracle.
  Goes through `flow-activate` (OpenSpec proposal/design/specs/tasks).

## The two human seams

1. **Spec approval** (Seam 1, in `flow-activate`) — you approve the OpenSpec spec + design before any
   implementation. The lead renders it inline and stops.
2. **Merge** (Seam 2) — **pre-authorized merge-on-green**:
   - *Default:* the lead opens the PR but does **not** merge or close it — merge is yours.
   - *Exception:* for a set you **explicitly pre-authorize** ("merge #X, #Y on green"), the lead may
     squash-merge + finalize, **only** when `agent-gate.sh` PASS + C PASS + roborev clean.
   - *Always escalated, never decided by the lead:* product decisions, scope/title changes, epic closes.

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

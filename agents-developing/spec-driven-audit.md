---
title: Spec-driven audit
description: How design-driven work is captured as OpenSpec changes and audited against its specs before merge.
sidebar:
  label: Spec-driven audit
---

CQLite uses **OpenSpec** as the front door for *design-driven* new work, and audits
each change against its own specs before merge. This page defines that loop, the audit
layers, and how OpenSpec relates to the `superpowers` skill family.

## When OpenSpec applies (oracle vs design)

Decide this per piece of work and state it in the proposal:

- **Oracle-driven** work — SSTable parsing/decode, compaction byte-parity, tombstone/TTL,
  type system — has an external source of truth (the Cassandra format + `sstabledump`
  goldens). These do **not** get an OpenSpec change; a GitHub issue + a pinned parity
  test is the right container.
- **Design-driven** work — bindings (M6 WASM), the query-engine surface, CLI/REPL UX,
  perf targets (M7), the agent-team process itself — has real latitude and **no** oracle.
  This enters via OpenSpec `explore` → `propose`.

## The merge flow

```
apply ─▶ gate (correctness) ─▶ C (intent) ─▶ roborev (code) ─▶ merge ─▶ archive
                                   │
                                   └── B: optional roborev design-review (escalation)
```

Each layer answers a different question, and they do not overlap:

| Layer | Question | Tool |
|-------|----------|------|
| correctness | does it work / match Cassandra? | `scripts/agent-gate.sh` + parity goldens |
| surface | does the public API exercise it? | wiring-evidence test (see [Wiring evidence](/cqlite/agents-developing/wiring-evidence/)) |
| **intent (C)** | does the impl satisfy each spec requirement? | `spec-auditor` subagent, anchored to `openspec/changes/<name>/specs/**` |
| code | is the code sound? | roborev |

### C — spec-anchored intent audit

The `spec-auditor` subagent reads the change's requirements + `#### Scenario:` blocks and
emits a per-requirement verdict — `satisfied` / `partial` / `unmet` — with evidence (the
test + public-surface call chain that exercises each scenario). It is read-only and runs
**only after the gate is green** (auditing intent on broken code is wasted). Any `unmet`
requirement, any requirement whose scenario has no exercising test from the public
surface, or any `partial` without written justification **blocks merge**.

### B — optional roborev escalation

`B` reuses the `roborev-design-review-branch` skill, invoked with the change's
proposal/design/specs as review criteria for an independent semantic second opinion. It
is on-demand — invoke when C reports `partial`, the change is high-stakes, or it touches
doctrine (no-heuristics / cross-binding parity) — not required for every change.

## Relationship to superpowers

The two frameworks are **complementary**: `superpowers` are the *techniques/discipline*;
OpenSpec is the *artifact system + lifecycle*. They nest, they do not compete. Per the
superpowers instruction-priority (user/project doctrine > superpowers > default), this
relationship wins where they would overlap.

| Lifecycle stage | OpenSpec (system of record) | superpowers (technique) |
|-----------------|-----------------------------|-------------------------|
| think / clarify | `explore` | `brainstorming` (the method inside explore) |
| capture intent | `propose` → proposal/design/specs/tasks | `writing-plans` (OpenSpec is the durable output) |
| implement | `apply` | `test-driven-development`, `using-git-worktrees` |
| verify correctness | gate (C's precondition) | `verification-before-completion` |
| audit intent | **C** (this loop) | `requesting-code-review` framing |
| review code | roborev | `receiving-code-review` |
| integrate | `archive` | `finishing-a-development-branch` |

Three rules:

1. **Brainstorming is satisfied by `explore`** — opening design-driven work in explore
   *is* the brainstorming step. (You may still invoke `brainstorming` standalone as a
   technique; it just needs no second home.)
2. **One system of record** — capture the plan's thinking into OpenSpec artifacts; do not
   keep a parallel `plan.md` alongside the proposal.
3. **Front door is conditional** — design-driven new work → OpenSpec; oracle-driven bug
   fixes → GitHub issue + pinned parity test.

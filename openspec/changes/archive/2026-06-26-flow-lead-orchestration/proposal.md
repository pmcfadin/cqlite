## Why

Delivery on CQLite is currently driven by a human acting as lead who spawns
subagents ad-hoc (as happened across the #1081 / #1071 / #1099 sprint). The
pattern works but lives only in conversation — there is no durable manager
persona, no named pipeline, and the autonomy rules are folklore. We want a
**manager agent (`flow-lead`)** that orchestrates a team of specialist agents
through a defined pipeline, with the owner in a small, explicit set of seats.

Reference pattern: `poc-arrow-cass-graph/.claude` (a `flow-lead` default agent +
a `flow-*` skill pipeline over OpenSpec). This change adapts that pattern to
CQLite's reality — its existing specialists, the `agent-gate`, roborev, and the
`change-audit` (C) intent layer from the `spec-driven-audit` change.

- **Milestone:** maintenance / process (the delivery harness for M6+).
- **Oracle vs design:** design-driven — a workflow has no Cassandra oracle.
- Builds on and reuses the `spec-driven-audit` change (gate → C → roborev).

## What Changes

- Add a **`flow-lead` manager agent** (`.claude/agents/flow-lead.md`) — the
  PM/lead persona that orients from the board, drives the pipeline, spawns and
  sequences specialists, holds the through-line, and never writes production
  code itself. Set it as the session default agent in `.claude/settings.json`.
- Add a **`flow-*` skill suite** wrapping what already exists:
  `flow-groom` (idea → one scoped issue), `flow-activate` (worktree + OpenSpec
  propose + design, STOP at Seam 1), `flow-implement` (background team →
  gate → C → roborev → PR), `flow-address` (resolve PR comments),
  `flow-finalize` (archive + cleanup + close), and `flow-board` (status +
  surface the single next item for the owner).
- **Remap the specialist roster** to CQLite's agents: `sstable-developer`
  (impl/TDD), `rust-reviewer` + `spec-auditor` (C, intent), `test-validator`
  (sstabledump parity), `coverage-reviewer` (test quality); roborev for code
  review; `agent-gate.sh` for correctness.
- **Autonomy model — pre-authorized merge-on-green** (owner's decision): by
  default the lead opens a PR but does NOT merge or close (merge is the owner's
  seam); the lead MAY squash-merge + finalize a set the owner has EXPLICITLY
  pre-authorized ("merge #X,#Y on green"), only when gate + C + roborev are all
  green; product decisions, scope/title changes, and epic-closes ALWAYS escalate
  to a NEEDS-YOU list.

## Capabilities

### New Capabilities
- `delivery-pipeline`: the manager-orchestrated delivery workflow — the
  `flow-lead` agent, the `flow-*` pipeline verbs, the specialist roster, the
  1:1:1:1 state model, and the autonomy/human-seam policy.

### Modified Capabilities
- `change-audit`: unchanged in requirements; the pipeline INVOKES it as the
  intent stage (no spec edit here — listed for traceability only).

## Impact

- **Agents/config:** new `.claude/agents/flow-lead.md`; new `.claude/skills/flow-*`;
  `.claude/settings.json` gains `"agent": "flow-lead"`.
- **Doctrine:** CLAUDE.md "agent-team conventions" + the website
  `agents-developing/` (a delivery-pipeline page); reconcile the existing
  "Product-manager behavior (lead)" section with the chosen autonomy model.
- **No cqlite-core / binding code changes.** No impact on no-heuristics, the
  binding surfaces, or the memory budget.

## Non-goals

- Not a fully autonomous loop — the owner keeps the spec-approval seam and the
  default merge seam (see autonomy model).
- Not replacing OpenSpec, the gate, C, or roborev — `flow-*` orchestrates them.
- Not a CI/GitHub-Actions orchestrator — `flow-lead` runs in the attended
  session (a CI integration can follow).
- Not changing how oracle-driven bug fixes are handled (issue + pinned test;
  they may still flow through groom/implement but skip OpenSpec).

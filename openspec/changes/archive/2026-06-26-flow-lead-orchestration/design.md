## Context

This sprint a human lead spawned subagents ad-hoc and ran gate + roborev + a
spec-auditor by hand. It worked but is undurable. The `poc-arrow-cass-graph`
repo formalizes the same idea: a `flow-lead` default agent + a `flow-*` skill
pipeline over OpenSpec, spawning specialist agents for the middle. This change
ports that pattern onto CQLite's existing assets (specialists, `agent-gate.sh`,
roborev, and the `change-audit` C layer) rather than inventing new ones.

## Goals / Non-Goals

**Goals:**
- A durable manager persona + named pipeline that reproduces the sprint's
  workflow without re-deriving it each time.
- Owner in exactly the seats they chose: spec approval (Seam 1) + merge
  (Seam 2, with a pre-authorized merge-on-green exception).
- Reuse, not reinvent: OpenSpec + gate + C + roborev are the stages.

**Non-Goals:**
- Fully autonomous merge/close; CI-side orchestration; replacing any existing
  stage; changing oracle-driven bug handling.

## Decisions

### D1 — `flow-lead` is the session default agent, orchestrator-only
`.claude/settings.json` sets `"agent": "flow-lead"`. The persona orients from
the board on start and NEVER writes production code — it spawns specialists and
holds the through-line. Rationale: matches the poc and the role the human lead
actually played.

### D2 — `flow-*` verbs wrap existing tools (thin, not new machinery)
`flow-groom / activate / implement / address / finalize / board`. `activate`
runs `opsx:propose` + design agents and STOPS at Seam 1; `implement` runs the
team and the quality stages; `finalize` runs `opsx:archive` + cleanup. Rationale:
the value is sequencing + the human seams, not new build logic.

### D3 — Specialist roster = CQLite's agents (remap, don't clone the poc's)
impl/TDD → `sstable-developer`; intent audit → `spec-auditor` (C, from
`spec-driven-audit`); code review → roborev; Rust review → `rust-reviewer`;
parity/tests → `test-validator`; test quality → `coverage-reviewer`;
correctness → `scripts/agent-gate.sh`. The poc's `tdd-developer` / six-lens
panel / `build-engineer` map onto these. Rationale: CQLite already has stronger,
domain-specific lanes (parity, no-heuristics) than the poc's generic ones.

### D4 — Autonomy: pre-authorized merge-on-green (owner's decision)
Default: open the PR, do NOT merge/close (merge = owner's seam). Exception: for
a set the owner EXPLICITLY pre-authorizes, the lead MAY squash-merge + finalize
when `agent-gate` PASS + C PASS + roborev clean. ALWAYS escalate (NEEDS-YOU):
product decisions, scope/title changes, epic closes. Rationale: the chosen
model; it matches how the #1081/#1071/#1099 sprint actually ran (the owner
pre-authorized the set, the lead merged on green).

### D5 — 1:1:1:1 state model on long-lived worktrees
One issue ↔ one branch/worktree `issue-<N>-<slug>` (under a stable worktrees
dir, NOT the Agent tool's throwaway isolation) ↔ one OpenSpec change `<slug>` ↔
one PR. Backlog = GitHub issues + labels (one `P0–P3`, lifecycle
`status:{ready,spec-review,in-progress,in-review,addressing}`). Rationale:
parallel changes stay isolated; the board is derivable from labels + PR state.

### D6 — Reconcile with the existing "Product-manager behavior (lead)" doctrine
CLAUDE.md already grants the lead autonomous comment/label/assign + close-when-
criteria-met. D4 NARROWS the merge/close part to the pre-authorized-set model
and supersedes the looser reading; the comment/label/assign autonomy stays.
Rationale: one stated policy, no folklore drift.

### D7 — Spawn with explicit model overrides
When `flow-implement` spawns specialists, it passes an explicit accessible model
(e.g. opus) — the repo's subagents carry a pinned model in frontmatter that is
not always accessible, so relying on the default silently fails. Rationale:
observed failure mode this sprint (a resumed agent reverted to an inaccessible
pinned model).

## Risks / Trade-offs

- **Over-process for small/oracle work.** Mitigation: oracle-driven bugs skip
  OpenSpec (issue + pinned test); `flow-groom` can route them straight to
  `flow-implement` without `activate`.
- **Default-agent lock-in.** Setting a session default agent changes every new
  session's behavior; mitigation: it's a single settings line, trivially
  reverted, and the owner can still start a plain session.
- **Drift between the persona file and the published doctrine.** Mitigation: the
  website delivery-pipeline page is the canonical description; the agent file is
  the role's operating manual (same split the poc uses).
- **Autonomy mis-set.** Mitigation: D4 defaults to the SAFE side (no merge)
  unless explicitly authorized per set.

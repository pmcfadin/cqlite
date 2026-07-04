---
name: start-epic
description: DEPRECATED — superseded by the flow-* delivery pipeline. Kept as a pointer so old references don't 404. Use flow-groom / flow-implement / flow-board instead.
disable-model-invocation: true
argument-hint: <epic-issue-number>
---

# start-epic — DEPRECATED (issue #1855)

Superseded by the **flow-\*** delivery pipeline. The old body encoded stale doctrine (full-gate-per-round,
non-claim-aware boards, a retired `TaskCompleted` hook) that can regress a session. Do not follow it.

Use instead:
- **`flow-board`** — render the shared claim board and surface the single next thing.
- **`flow-groom` → `flow-activate` → `flow-implement` → `flow-address` → `flow-finalize`** — the per-issue
  pipeline (1:1:1:1; tiered gate; spec-auditor **C**; merge-on-green).
- **`manager` / `worker`** commands — run the lead-and-workers model across a backlog.

Doctrine: `docs/development/pm-operating-loop.md`.

---
name: CI/CD Validation & Merge Workflow
description: Pre-push validation checklist (cargo fmt, clippy with zero warnings, feature flag testing, test suite), CI monitoring, merge process, and release quality gates. Use when preparing to push code, validating changes before PR, running CI checks, merging PRs, or preparing releases.
---

# CI/CD Validation & Merge Workflow

Validation, CI monitoring, and merge procedures for cqlite. There is **one gate** —
`scripts/agent-gate.sh` — run in a **tiered loop**. The old manual `cargo` checklist, coverage/tarpaulin
gate, and human-merge steps were M1-era fossils and are gone (issue #1855); see the pointer files in this
dir and `docs/development/pm-operating-loop.md`.

## When to Use This Skill

Pre-push validation · preparing PRs · monitoring CI · merging · releases · troubleshooting CI failures.

## Quick Validation — the tiered gate loop (issue #1821)

`scripts/agent-gate.sh` is THE canonical pre-PR gate (issue #719): it mirrors the enforced CI gates plus
the local smoke suite and emits a machine-checkable summary block. A claim that "the gate passed" must come
from this script's summary (between the `AGENT-GATE SUMMARY` markers, ending in `RESULT: PASS`) — ad-hoc
`cargo` runs do not count.

```bash
# ITERATE — every fix round (fmt + file-size + workspace clippy + blast-radius-scoped tests, ~1-5 min).
# Emits a DISTINCT ==== AGENT-GATE LITE SUMMARY ==== block that must NEVER be pasted as the full SUMMARY.
scripts/agent-gate.sh --lite

# BEFORE MERGE — the FULL gate, run EXACTLY ONCE by the lead. Its ==== AGENT-GATE SUMMARY ====
# (ending RESULT: PASS) is the only run that counts. --lite never replaces it.
scripts/agent-gate.sh

# Debugging only (output marked PARTIAL, never counts as the gate):
scripts/agent-gate.sh --only fmt,clippy
```

- **Division of labor (issue #1855):** subagents iterate on `--lite` / targeted tests and end at
  commit + push + report. The **lead** runs the full gate + roborev — a subagent idle-waiting on a 12-20 min
  full gate gets killed by the 600s stall watchdog and takes its child gate process down with it.
- **Queued gate ≠ hung gate:** under load the full gate may **queue for a #1825 slot** (prints
  `waiting for gate slot (N in use)…` once) then run 15-20 min — total wall time can exceed 20 min. Use a
  long Bash `timeout` or `run_in_background` and check for that line before assuming a hang (the default
  2-min timeout truncates a queued gate).
- **Gate PASS ≠ CI green:** the local gate uses pre-existing datasets and a subset of `--test` targets.
  When a change touches a **regenerate path, a fixture parser, or a fail-closed CI guard**, reproduce the
  actual CI lane locally before relying on the gate.

## CI Monitoring

```bash
gh run list --limit 10            # recent runs
gh run view <run-id>              # a specific run
gh run watch                      # watch live
gh run view <run-id> --log-failed # failed-job logs only
```

## Merge

Merge is **autonomous on green** — gate PASS + (design-driven) spec-auditor **C** PASS + roborev clean →
`gh pr merge --squash --delete-branch`, then `flow-finalize <N>`. Merge is not a human gate; hold only for a
genuine design call, a scope/product question, an unmet requirement, or a `HOLD: merge after #N` order. See
[merge-process.md](merge-process.md) and `docs/development/pm-operating-loop.md`.

## References

- `docs/development/pm-operating-loop.md` — delivery model (tiered gate, merge-on-green, telemetry)
- The `flow-*` skills — pipeline stages (groom → activate → implement → address → finalize; board)
- [validation-checklist.md](validation-checklist.md) · [merge-process.md](merge-process.md) — pointers
- Gate contract: https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/

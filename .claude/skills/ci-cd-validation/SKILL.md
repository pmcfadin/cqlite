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

**Only `RESULT: PASS`/`RESULT: FAIL` is a verdict (#3041).** The gate writes
`RESULT: INCOMPLETE (gate did not finish)` into the summary file **at launch** and overwrites it on
completion, so `INCOMPLETE` is a **liveness placeholder, not a verdict**. If you poll a summary file
instead of waiting for the process to exit, the predicate is
the **RECORD grammar** `grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)' "$AGENT_GATE_SUMMARY_FILE"` — a bare
`grep -q` on the bare `RESULT:` token fires the instant the gate starts and would accept a just-launched (or
still-queued) gate as certified, and an unanchored form matches `RESULT: PASSENGER`.

**COMPLETION AND VERDICT ARE TWO ASSERTIONS (#3750).** The record grammar above is for full/`--lite`/`--delta`
and must keep **REFUSING** `PARTIAL`. An **`--only <component>`** run demotes success to `RESULT: PARTIAL`, so
that grammar spins on green there. Poll `--only` by **EXIT STATUS** (`3` = completed PARTIAL) where you can
observe it, else by the **ONLY grammar** `grep -qE '^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'`; then read the
component's verdict SEPARATELY, from its own line:
`bash scripts/gate-component-verdict.sh "$SUM" --mode only --component <name> --run-id <id>` (exit 0 PASS /
1 NOT-PASS / 4 COULD-NOT-MEASURE, no verdict available whatever the reason / 64 USAGE). It is **NOT a
completion probe and has no opinion about liveness — never call it in a loop**: establish completion
first with the grammars above or `gate-liveness.sh`, which is the three-valued liveness authority and
the only one of the two that may be polled (#3750 descoped a retryability taxonomy here, because it
told a lane a LIVE gate was permanently unmeasurable and an obedient lane relaunches it). A
completed run whose component **SKIPped or is absent is NOT a pass** —
and a SKIPping component still exits 3, so exit 3 is completion and never a green.

**Every invocation — full, lite, and `--only` — MUST use the summary-file redirect** (#1175/#2079). The
summary block is the only gate text an agent retains; never stream raw gate stdout into a persistent
context, and never read `gate.log`.

```bash
# ITERATE — every fix round. --lite components (exactly, per scripts/agent-gate.sh LITE_COMPONENTS):
#   file-size · fmt · clippy (PER-PACKAGE scoped, #1844 — not whole-workspace) · roborev-lints ·
#   scoped-tests (blast-radius: touched package --lib + the diff's new --test targets). ~1-5 min.
# Emits a DISTINCT ==== AGENT-GATE LITE SUMMARY ==== block that must NEVER be pasted as the full SUMMARY.
AGENT_GATE_SUMMARY_FILE=/tmp/lite-<N>.txt \
  bash scripts/agent-gate.sh --lite > /tmp/lite-<N>.log 2>&1 < /dev/null
cat /tmp/lite-<N>.txt

# BEFORE MERGE — the FULL gate, run EXACTLY ONCE, inside the flow-closer subagent (#2084).
# Its ==== AGENT-GATE SUMMARY ==== (ending RESULT: PASS) is the only run that counts; --lite never
# replaces it.
AGENT_GATE_SUMMARY_FILE=/tmp/gate-<N>.txt \
  bash scripts/agent-gate.sh > /tmp/gate-<N>.log 2>&1 < /dev/null
cat /tmp/gate-<N>.txt

# Debugging only (output marked PARTIAL, never counts as the gate):
AGENT_GATE_SUMMARY_FILE=/tmp/partial-<N>.txt \
  bash scripts/agent-gate.sh --only fmt,clippy > /tmp/partial-<N>.log 2>&1 < /dev/null
```

- **Division of labor (#1855/#2084/#2079):** the implementing subagent iterates on `--lite` / targeted
  tests and ends at commit + push + report. **The ONE full gate of record and the final roborev pass run
  inside the disposable `flow-closer` subagent, NOT in the lead** — that roborev pass goes through the ONLY
  sanctioned invocation, `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>` (#2964; both
  flags required, never a bare `roborev review --branch`), and the closer retains only its
  `==== ROBOREV REVIEW SUMMARY ====` block, whose header is deliberately distinct from every
  `AGENT-GATE *SUMMARY` so neither can be pasted as the other — the lead never runs the full gate and
  never reads its stdout; it retains only the closer's terminal packet (verdict, PR URL, summary-file
  path). The closer launches the gate via `Bash run_in_background` and reads the SUMMARY **from the file**:
  a subagent that idle-waits on a 12-25 min full gate is killed by the 600s stall watchdog and orphans its
  child gate process. The closer has no `Agent` tool, so it requests **C** (`spec-auditor`) from the lead
  via a `NEEDS-SPAWN` packet (#2668).
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

Merge is **autonomous on green** — once **local certification** holds (gate PASS + (design-driven)
spec-auditor **C** PASS + roborev clean — a terminal `RESULT: PASS` from
`scripts/flow/roborev-review.sh`, never `NOTHING-TO-REVIEW`), and after the pre-merge SHA assert + `HOLD`
re-read, **arm
`gh pr merge --auto --squash --delete-branch`** and stop; GitHub lands the PR when the #2433 `required`
check goes green (#2667), then `flow-finalize <N>`. **Never `ScheduleWakeup`-poll a PR's own CI** — `--auto`
replaces the busy-wait. Merge is not a human gate; hold only for a genuine design call, a scope/product
question, an unmet requirement, or a `HOLD: merge after #N` order. See
[merge-process.md](merge-process.md) and `docs/development/pm-operating-loop.md`.

## References

- `docs/development/pm-operating-loop.md` — delivery model (tiered gate, merge-on-green, telemetry)
- The `flow-*` skills — pipeline stages (groom → activate → implement → address → finalize; board)
- [validation-checklist.md](validation-checklist.md) · [merge-process.md](merge-process.md) — pointers
- Gate contract: https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/

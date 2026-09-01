# Pre-Push Validation Checklist

> **Superseded (issue #1855).** The old 11-step manual `cargo` checklist (and its nonexistent
> tarpaulin/90%-coverage gate) is gone — it duplicated and drifted from the real gate. There is one gate.

Validation is `scripts/agent-gate.sh`, run in the tiered loop:

**Every invocation carries the mandatory summary-file redirect** (#1175/#2079):
`AGENT_GATE_SUMMARY_FILE=<path> bash scripts/agent-gate.sh [--lite] > gate.log 2>&1 < /dev/null`, then
read only the summary file — never `gate.log`.

- **Iterate:** `--lite` on every fix round (~1-5 min). Its components are exactly
  `file-size fmt clippy roborev-lints scoped-tests` (the `scripts/agent-gate.sh` `LITE_COMPONENTS` array),
  where lite clippy is **per-package scoped** (#1844), not whole-workspace, and `scoped-tests` is
  blast-radius (touched package `--lib` + the diff's new `--test` targets). It emits a distinct
  `==== AGENT-GATE LITE SUMMARY ====` block that must NEVER be pasted as the full SUMMARY.
- **Before merge:** the FULL `scripts/agent-gate.sh` runs **exactly once, inside the `flow-closer`
  subagent** (#2084) — not in the lead, which never runs the full gate nor reads its stdout. Its
  `==== AGENT-GATE SUMMARY ====` (ending `RESULT: PASS`) is the only run that counts. `--lite` never
  replaces it. Under load the full gate may **queue for a #1825 slot** (prints `waiting for gate slot
  (N in use)…` once) then run 15-25 min — launch it with `run_in_background`/a long timeout; queued ≠ hung.
- **`INCOMPLETE` is a liveness placeholder, not a verdict (#3041).** The startup sentinel puts
  `RESULT: INCOMPLETE (gate did not finish)` in the summary file before any component runs (a queued
  gate already has one), so any completion poll must be
  the **RECORD grammar** `grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)' "$AGENT_GATE_SUMMARY_FILE"`, never a
  bare `grep -q` on the bare `RESULT:` token and never an unanchored form (which matches `RESULT: PASSENGER`).
- **An `--only <component>` run needs the OTHER grammar, and its verdict is a SEPARATE read (#3750).** `--only`
  demotes success to `RESULT: PARTIAL`, so the record grammar above spins on green. Completion: **exit status
  `3`** where observable, else `grep -qE '^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'`. Verdict:
  `bash scripts/gate-component-verdict.sh "$SUM" --mode only --component <name>` — a completed run whose
  component SKIPped or is absent is NOT a pass.
- **`--delta` is a THIRD mode with a THIRD completion set (#3750).** It alone can terminate `ERROR` or
  `REFUSED`, so polling it with the record grammar hangs on a terminal outcome:
  `grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)([[:space:]]|$)'` — `gate-liveness.sh`'s own
  enumerated terminal set, token for token.

See `SKILL.md` (this dir) for the loop and `docs/development/pm-operating-loop.md` for the delivery model.

# Pre-Push Validation Checklist

> **Superseded (issue #1855).** The old 11-step manual `cargo` checklist (and its nonexistent
> tarpaulin/90%-coverage gate) is gone — it duplicated and drifted from the real gate. There is one gate.

Validation is `scripts/agent-gate.sh`, run in the tiered loop:

- **Iterate:** `scripts/agent-gate.sh --lite` (fmt + file-size + workspace clippy + blast-radius-scoped
  tests, ~1-5 min) on every fix round. It emits a distinct `==== AGENT-GATE LITE SUMMARY ====` block that
  must NEVER be pasted as the full SUMMARY.
- **Before merge:** the **lead** runs the FULL `scripts/agent-gate.sh` **exactly once**; its
  `==== AGENT-GATE SUMMARY ====` (ending `RESULT: PASS`) is the only run that counts. `--lite` never
  replaces it. Under load the full gate may **queue for a #1825 slot** (prints `waiting for gate slot
  (N in use)…` once) then run 15-20 min — use a long timeout; queued ≠ hung.

See `SKILL.md` (this dir) for the loop and `docs/development/pm-operating-loop.md` for the delivery model.

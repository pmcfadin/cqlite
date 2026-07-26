# gate-nested-isolation — make the gate of record immune to nested/concurrent gate activity

## Why

The nested-gate state-sharing family keeps killing full gates of record (issue #2874, residual of
#2751/#2849). The env-inheritance vector is already closed (`agent-gate.sh` de-exports
`AGENT_GATE_SUMMARY_FILE` before running components), but field evidence 2026-07-24 (#2856 endgame)
shows a full gate run *without* the env var still died INCOMPLETE in `tooling-tests` while a second
lane ran the same gate self-test files concurrently on the box. The residual kill surface:

1. **Same-checkout default summary paths** — a nested or concurrent gate invocation that does not pin
   its own `AGENT_GATE_SUMMARY_FILE` writes the checkout default (`.agent-gate-summary.txt`), the same
   file the parent gate of record is using.
2. **Fixed-name self-test fixtures** — `test_agent_gate_parity_report.sh` uses a fixed fixture path
   `test-data/.tmp-parity-manifest-mutated.yml` with an EXIT trap that `rm`s it; two concurrent lanes
   race, and one lane's trap deletes the other's live fixture.
3. **Detection is end-of-run only** — a mid-run clobber manifests as a bare INCOMPLETE death with no
   named cause, costing a full ~1h gate re-run to even diagnose.

Cost on 2026-07-24: ~2 wasted full gate runs (~1h wall-clock) plus a standing serialize-everything
ops rule that taxes every endgame. This change retires that ops rule.

Routing: **design-driven** (harness/process) — milestone: unmilestoned harness work, owner-directed
promotion 2026-07-24.

## What changes

- `scripts/agent-gate.sh`: structural nested-run isolation — the parent gate exports a run marker;
  any gate invocation that detects it defaults its summary file to a private per-invocation path
  (inside its own mktemp log dir), never the enclosing checkout's default. Per-self-test `unset`/pin
  discipline stays as belt-and-suspenders but is no longer load-bearing.
- `scripts/agent-gate.sh`: mid-run summary-integrity check at component boundaries — a foreign
  run-id in the summary file produces a named `summary-integrity: FAIL` line and a loud FAIL, never
  a bare INCOMPLETE death.
- `scripts/tests/*.sh` gate self-tests: every fixture/tmp path becomes a per-run mktemp namespace
  (terminal `XXXXXX`, macOS-safe); the parity-report mutated-manifest fixture gets a unique per-run
  name; EXIT traps remove only paths the run created.
- New regression self-tests (run inside `tooling-tests`): nested-clobber immunity + same-checkout
  concurrency.
- Docs: `docs/development/gate-ops.md` — replace the "serialize everything / box-exclusive" guidance
  with the new guarantees.

## Non-goals

- No change to the machine slot semantics, `CQLITE_GATE_MAX_CONCURRENCY`, or the cap self-exemptions
  for `--lite`/`--delta`/`--only` (#2849's territory — already fixed).
- No change to the summary block format/contract beyond adding the named integrity FAIL line.
- Not the nightly `gate.yml` CI lane (that is #2662, a sibling lane).
- No attempt to make two *full* gates in ONE checkout mutually safe — that remains out of scope
  (separate worktrees / unique summary paths remain the rule for peer full gates).

## Doctrine impact

- Retires the 2026-07-24 "box-exclusive full gate / serialize all self-test lanes" ops rule.
- `docs/development/gate-ops.md` updated in the same change. CLAUDE.md gate wording unchanged
  (the summary-file redirect invocation stays the documented default).

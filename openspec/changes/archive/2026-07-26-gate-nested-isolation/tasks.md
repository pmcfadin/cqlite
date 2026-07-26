# Tasks — gate-nested-isolation

## 1. Structural nested-run isolation (surface: `scripts/agent-gate.sh` summary-path resolution)
- [x] 1.1 Export `AGENT_GATE_PARENT_RUN_ID="$RUN_ID"` alongside the existing
      `AGENT_GATE_SUMMARY_FILE` de-export, before any component runs.
- [x] 1.2 In summary-path resolution: if `AGENT_GATE_PARENT_RUN_ID` is set and no explicit
      `AGENT_GATE_SUMMARY_FILE`, default to `$LOG_DIR/summary.txt`; stamp `nested-under:
      <parent-run-id>` in the summary block. Explicit env still wins.
- [x] 1.3 Regression self-test (new, wired into `tooling-tests`): parent gate mid-run + nested
      no-env invocation in the same checkout → parent summary byte-identical (hash before/after),
      nested summary landed in its own log dir. TDD: write the test first, watch it fail on the
      current default-path behavior.

## 2. Mid-run summary-integrity check (surface: `agent-gate.sh` component loop)
- [x] 2.1 Component-boundary guard: `grep -q "run-id: $RUN_ID" "$SUMMARY_FILE"` (when the file
      exists); on mismatch write `summary-integrity: FAIL (foreign run-id detected mid-run;
      expected $RUN_ID)` + `RESULT: FAIL`, exit non-zero.
- [x] 2.2 Self-test: overwrite the summary with a foreign-run-id block mid-run (fast selftest
      mode) → named FAIL line present, exit non-zero, never bare INCOMPLETE.

## 3. Hermetic self-test fixtures (surface: `scripts/tests/*.sh`)
- [x] 3.1 `test_agent_gate_parity_report.sh`: per-run `mktemp` mutated-manifest fixture under
      `test-data/` (terminal `XXXXXX`); trap removes only this run's file. Verify no write to
      shared repo files (reads/diffs OK).
- [x] 3.2 Sweep all gate self-tests for other fixed shared paths → per-run mktemp. (Slots dir
      `/tmp/cqlite-gate-slots` is the semaphore by design — out of scope.)
- [x] 3.3 Structural self-check: FAIL `tooling-tests` on any fixed `.tmp-*` fixture name or
      non-terminal-`XXXXXX` mktemp template in `scripts/tests/*.sh`.

## 4. Concurrency self-test (surface: `tooling-tests` component)
- [x] 4.1 Bounded test: two concurrent instances of the fast parity-report/summary self-tests in
      one checkout, both pass.
- [x] 4.2 Record `tooling-tests` wall-clock before/after on the PR (±10% budget).

## 5. Docs (same change)
- [x] 5.1 `docs/development/gate-ops.md`: replace box-exclusive/serialize-everything guidance with
      the new guarantees (nested auto-isolation, named integrity FAIL, hermetic self-tests).

## 6. Quality stages
- [ ] 6.1 `--lite` each fix round (summary-file redirect, unique path).
- [ ] 6.2 Review-first: `rust-reviewer` (script/shell scope) + roborev on the lite-green diff.
- [ ] 6.3 Open PR; flow-closer: ONE full gate of record → C intent audit (`spec-auditor` vs this
      change's `specs/**`) → final roborev → merge-on-green → finalize.
- [ ] 6.4 Field verification (acceptance #3): full gate on this box with a concurrent self-test
      loop lane; record PASS on issue #2874 — retires the box-exclusive ops rule.

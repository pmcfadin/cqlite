# Design — gate-nested-isolation

## Context (facts from the current tree)

- `agent-gate.sh` resolves `SUMMARY_FILE` at lines ~1540-1549: `AGENT_GATE_SUMMARY_FILE` if set, else
  per-checkout defaults `.agent-gate-summary.txt` / `-lite-` / `-delta-` under `$REPO_ROOT`.
- Every run already has a unique nonce: `RUN_ID="$LOG_DIR"` where `LOG_DIR=$(mktemp -d
  ".../agent-gate.XXXXXX")`; every SUMMARY block carries `run-id: $RUN_ID`, and the startup
  INCOMPLETE placeholder stamps it too.
- The parent already de-exports `AGENT_GATE_SUMMARY_FILE` before components run (#2751 fix), and
  most self-tests both `unset` it and pin nested `--only` invocations to private mktemp summaries.
- Detection today is end-of-run only: `emit_summary` re-greps for its own `run-id` after writing.
  A mid-run clobber (nested gate writing the same default path, or a foreign INCOMPLETE placeholder)
  kills the run as a bare INCOMPLETE.
- The one fixed-name fixture: `test_agent_gate_parity_report.sh` sets
  `MUT="$REPO_ROOT/test-data/.tmp-parity-manifest-mutated.yml"` (must live under the real repo root
  for the tool's `repo_root()` resolution) with `trap 'rm -f "$MUT"' EXIT` — the cross-lane
  delete race.
- All existing mktemp templates are already macOS-safe (terminal `XXXXXX`); the guard here is a
  lint-style check so a regression cannot re-enter.

## Options considered

**(a) Discipline-only (status quo + more per-test pins).** Keep relying on each self-test to unset
and pin. Rejected: this is the regime that already failed in the field on 2026-07-24 — one missed
pin anywhere re-opens the vector, and the failure mode (silent INCOMPLETE an hour later) is the most
expensive kind.

**(b) Structural isolation + hermetic fixtures + mid-run integrity check (CHOSEN).** Make the
*script itself* incapable of the collision: nested runs are detected and auto-redirected to private
paths; the one fixed fixture becomes per-run; a cheap component-boundary check names the failure if
anything foreign still touches the summary.

**(c) flock the summary file.** Advisory locks on the summary path. Rejected: doesn't fix the
fixture delete race at all, adds cross-platform lock semantics (macOS flock quirks), and serializes
writers instead of separating them — treats the symptom.

## Chosen design

### 1. Structural nested-run summary isolation (`agent-gate.sh`)

- After resolving its own `SUMMARY_FILE` and `RUN_ID`, the parent gate **exports
  `AGENT_GATE_PARENT_RUN_ID="$RUN_ID"`** (alongside the existing de-export of
  `AGENT_GATE_SUMMARY_FILE`).
- At summary-path resolution time, any invocation that sees `AGENT_GATE_PARENT_RUN_ID` set (and no
  explicit `AGENT_GATE_SUMMARY_FILE` of its own) is a **nested run**: it defaults its summary to
  `$LOG_DIR/summary.txt` — its own private mktemp dir — never the checkout default. An explicit
  `AGENT_GATE_SUMMARY_FILE` from the nested caller still wins (self-tests keep pinning to assert on
  content).
- The nested run's summary notes `nested-under: <parent-run-id>` for traceability.
- This closes the vector for EVERY nested invocation, present and future, without requiring any
  self-test discipline. Mirrors the #2849 pattern of making the script self-exempting rather than
  documenting a rule.

### 2. Mid-run summary-integrity check at component boundaries (`agent-gate.sh`)

- Between components (the existing per-component loop), a cheap guard: if `$SUMMARY_FILE` exists and
  does NOT contain `run-id: $RUN_ID`, the run stops with a **named cause**: it rewrites the summary
  with `summary-integrity: FAIL (foreign run-id detected mid-run; expected <RUN_ID>)`,
  `RESULT: FAIL`, and exits non-zero. Cost: one `grep -q` per component (~20 greps/run, negligible).
- `emit_summary`'s existing end-of-run re-grep stays as the final backstop.
- Result: acceptance #4 — a clobber is a diagnosed FAIL in seconds, never a bare INCOMPLETE death
  discovered an hour later.

### 3. Hermetic self-test fixtures (`scripts/tests/*.sh`)

- `test_agent_gate_parity_report.sh`: replace the fixed `MUT` path with
  `MUT=$(mktemp "$REPO_ROOT/test-data/.tmp-parity-manifest-mutated.XXXXXX")` — still under the real
  repo root (the tool's `repo_root()` constraint), but per-run unique; the EXIT trap now removes only
  this run's file. Audit the same script's use of the real `docs/reports/cassandra-test-parity.md`:
  reads/diffs are fine; any *write* to a shared repo file moves to a temp copy.
- Sweep all gate self-tests for any other fixed shared path (fixtures, sentinel files, tmp names) →
  per-run mktemp. The machine-wide slots dir `/tmp/cqlite-gate-slots` is the semaphore BY DESIGN and
  is explicitly out of scope.
- Add a self-check (inside the existing `test_agent_gate_summary.sh` family) that greps
  `scripts/tests/*.sh` for non-terminal-`XXXXXX` mktemp templates and for a denylist of fixed
  `.tmp-*` fixture names, so the class cannot silently return.

### 4. Regression + concurrency self-tests (new, run in `tooling-tests`)

- **Nested-clobber immunity**: launch a parent-shaped gate stub (real `agent-gate.sh
  --emit-summary-selftest`-class invocation with a pinned summary), then from inside its env run a
  nested gate WITHOUT an explicit summary path in the same checkout; assert the parent summary is
  byte-identical (hash before/after) and the nested run wrote into its own log dir.
- **Same-checkout concurrency**: run two instances of the (fast) parity-report/summary self-tests
  concurrently in one checkout; both must pass. Bounded runtime — uses the fast self-test paths,
  not full gates.

### 5. Wall-clock guard

- `tooling-tests` component wall-clock must stay within ±10% of pre-change (acceptance #5). The new
  concurrency self-test uses the fast paths and runs the two instances in parallel, so added cost is
  ~one extra fast self-test, not a serial doubling. Timing recorded on the PR.

## Field verification (acceptance #3)

After merge-candidate state: run one full gate on this box while a second lane loops the gate
self-test files concurrently; record the PASS on issue #2874. This is the evidence that retires the
box-exclusive ops rule.

## Risks

- The nested-detection env var propagates through any subprocess the gate spawns — intended; a
  false-positive "nested" run only means a private summary path, which is always safe. A user who
  genuinely wants a nested run to write a specific path still can (explicit env wins).
- The parity-report fixture rename must keep the file under `test-data/` in the REAL repo root or
  the tool's `repo_root()` resolution breaks — covered by the test itself.

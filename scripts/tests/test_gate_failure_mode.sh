#!/usr/bin/env bash
#
# Regression tests for scripts/ci/gate-failure-mode.sh (issue #2662).
#
# The alert automation (gate-failure-issue.yml) only ever executes on a real
# `workflow_run` event, so its routing decision is untestable in the workflow.
# That logic lives in gate-failure-mode.sh (pure, offline); this test pins its
# decision table so a regression is caught by the gate's `tooling-tests`
# component instead of merging undetected.
#
# Fast + hermetic: no network, no `gh`, no datasets, no scratch files.
# AGENT_GATE_SUMMARY_FILE is unset so a nested invocation can never clobber a
# parent gate's summary (#2751/#2874).
#
# Run standalone:   bash scripts/tests/test_gate_failure_mode.sh
set -uo pipefail
unset AGENT_GATE_SUMMARY_FILE 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE_SH="$SCRIPT_DIR/../ci/gate-failure-mode.sh"
GATE_NAME="Nightly agent-gate (deep check)"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# expect <wanted-mode> <description> <args...> — run the mode script with the
# given flags and assert stdout equals <wanted-mode>.
expect() {
  local want="$1" desc="$2"
  shift 2
  local got
  got="$(bash "$MODE_SH" "$@" 2>/dev/null)" || true
  if [ "$got" = "$want" ]; then
    ok "$desc"
  else
    bad "$desc (got '$got', wanted '$want')"
  fi
}

# expect_reason <stderr-substring> <description> <args...> — assert the decision
# is `none` AND the stderr reason contains <stderr-substring>, so distinct
# rejection branches (e.g. pull_request vs allowlist) are distinguishable, not
# just uniformly "none".
expect_reason() {
  local needle="$1" desc="$2"
  shift 2
  local out err
  err="$(bash "$MODE_SH" "$@" 2>&1 >/dev/null)" || true
  out="$(bash "$MODE_SH" "$@" 2>/dev/null)" || true
  if [ "$out" != "none" ]; then
    bad "$desc (mode '$out', wanted 'none')"
    return
  fi
  case "$err" in
    *"$needle"*) ok "$desc" ;;
    *) bad "$desc (reason '$err' lacks '$needle')" ;;
  esac
}

# --- auto workflow_run path: conclusion -> mode ------------------------------
for c in failure cancelled timed_out; do
  expect file "workflow_run schedule $c -> file" \
    --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
    --event schedule --branch main --conclusion "$c"
done
expect resolve "workflow_run schedule success -> resolve" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion success
expect file "workflow_run dispatch-on-main failure -> file" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event workflow_dispatch --branch main --conclusion failure
expect none "workflow_run schedule startup_failure -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion startup_failure

# --- auto path rejections, each with a DISTINCT stderr reason -----------------
expect_reason "is not the gate lane" "wrong workflow name -> none (name reason)" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "Some Other Lane" \
  --event schedule --branch main --conclusion failure
expect_reason "pull_request" "pull_request origin -> none (pr reason)" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event pull_request --branch main --conclusion failure
expect_reason "not schedule/workflow_dispatch" "push origin -> none (allowlist reason)" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event push --branch main --conclusion failure
expect_reason "non-main branch" "dispatch on non-main branch -> none (branch reason)" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event workflow_dispatch --branch issue-2662-x --conclusion failure

# --- manual replay path ------------------------------------------------------
expect file "replay valid numeric run_id red -> file" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion cancelled --run-id 12345
expect resolve "replay valid run_id green -> resolve" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion success --run-id 12345
expect_reason "requires a run_id" "replay absent run_id -> none (missing-run-id reason)" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure
expect_reason "is not numeric" "replay non-numeric run_id -> none (numeric reason)" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure --run-id "12; rm -rf /"
expect none "replay wrong workflow name -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "Another Workflow" \
  --event schedule --branch main --conclusion failure --run-id 999
expect none "replay pull_request-origin target -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event pull_request --branch main --conclusion failure --run-id 999
expect none "replay non-main dispatch target -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event workflow_dispatch --branch feature --conclusion failure --run-id 999

# --- argument robustness -----------------------------------------------------
expect_reason "gate-name is required" "missing gate-name -> none (required reason)" \
  --trigger workflow_run --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure
expect_reason "missing value for --conclusion" "trailing flag with no value -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion
expect_reason "unknown argument" "unknown argument -> none (unknown reason)" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure --bogus x

printf '\n%s\n' "gate-failure-mode: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]

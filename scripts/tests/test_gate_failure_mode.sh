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
# Fast + hermetic: no network, no `gh`, no datasets. Per-run mktemp scratch with
# a TERMINAL XXXXXX (macOS-safe); AGENT_GATE_SUMMARY_FILE is unset so a nested
# invocation can never clobber a parent gate's summary (#2751/#2874).
#
# Run standalone:   bash scripts/tests/test_gate_failure_mode.sh
set -uo pipefail
unset AGENT_GATE_SUMMARY_FILE 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE_SH="$SCRIPT_DIR/../ci/gate-failure-mode.sh"
GATE_NAME="Nightly agent-gate (deep check)"

T=$(mktemp -d "${TMPDIR:-/tmp}/gate-failure-mode-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

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

# --- auto workflow_run path --------------------------------------------------
# Scheduled red conclusions -> file.
for c in failure cancelled timed_out; do
  expect file "workflow_run schedule $c -> file" \
    --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
    --event schedule --branch main --conclusion "$c"
done

# Scheduled green -> resolve.
expect resolve "workflow_run schedule success -> resolve" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion success

# workflow_dispatch origin on main -> honored (red -> file).
expect file "workflow_run dispatch-on-main failure -> file" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event workflow_dispatch --branch main --conclusion failure

# workflow_dispatch origin on a NON-main branch -> none (feature-branch test).
expect none "workflow_run dispatch on non-main branch -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event workflow_dispatch --branch issue-2662-x --conclusion failure

# pull_request origin -> none (never file for a PR run).
expect none "workflow_run pull_request origin -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event pull_request --branch main --conclusion failure

# Wrong workflow name -> none (belt-and-suspenders even on the auto path).
expect none "workflow_run wrong workflow name -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "Some Other Lane" \
  --event schedule --branch main --conclusion failure

# Unknown/non-terminal conclusion on an allowlisted origin -> none.
expect none "workflow_run schedule startup_failure -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion startup_failure

# Unknown origin event (e.g. push) -> none.
expect none "workflow_run push origin -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event push --branch main --conclusion failure

# --- manual replay (workflow_dispatch of the alert workflow) ------------------
# Valid numeric run_id, gate lane, schedule/main, red -> file.
expect file "replay valid numeric run_id red -> file" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion cancelled --run-id 12345

# Valid replay of a green run -> resolve.
expect resolve "replay valid run_id green -> resolve" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion success --run-id 12345

# Absent run_id on replay -> none.
expect none "replay absent run_id -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure

# Non-numeric run_id on replay -> none.
expect none "replay non-numeric run_id -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure --run-id "12; rm -rf /"

# Replay whose target is a DIFFERENT workflow -> none.
expect none "replay wrong workflow name -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "Another Workflow" \
  --event schedule --branch main --conclusion failure --run-id 999

# Replay of a pull_request-origin run -> none.
expect none "replay pull_request-origin target -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event pull_request --branch main --conclusion failure --run-id 999

# Replay of a non-main dispatch target -> none.
expect none "replay non-main dispatch target -> none" \
  --trigger workflow_dispatch --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event workflow_dispatch --branch feature --conclusion failure --run-id 999

# --- misc robustness ---------------------------------------------------------
# Missing --gate-name -> none (fail-closed).
expect none "missing gate-name -> none" \
  --trigger workflow_run --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure

# Unknown argument -> none (fail-closed).
expect none "unknown argument -> none" \
  --trigger workflow_run --gate-name "$GATE_NAME" --name "$GATE_NAME" \
  --event schedule --branch main --conclusion failure --bogus x

printf '\n%s\n' "gate-failure-mode: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]

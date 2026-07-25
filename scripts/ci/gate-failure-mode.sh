#!/usr/bin/env bash
# gate-failure-mode.sh — the pure routing decision for gate-failure-issue.yml
# (issue #2662). Given an already-resolved gate-run identity, decide what the
# alert automation should do:
#
#   file    — the run is RED (failure/cancelled/timed_out): file/update the
#             deduplicated `gate-nightly-failure` tracking issue.
#   resolve — the run is GREEN (success): comment "resolved" on the open issue.
#   none    — do nothing (origin not allowlisted, wrong workflow, bad replay
#             input, or a non-terminal/unknown conclusion).
#
# WHY A SEPARATE SCRIPT: the routing logic (dispatch-vs-auto validation, the
# allowlist, the conclusion→mode mapping) is the only non-trivial part of the
# workflow, and a workflow only executes on a real `workflow_run` event — so it
# cannot be unit-tested in-repo. Extracting the decision here makes it callable
# from `scripts/tests/test_gate_failure_mode.sh` and keeps the workflow a thin
# shell that fetches metadata + performs the `gh` side effects.
#
# PURE + OFFLINE: this script performs NO network calls. The caller resolves the
# run's metadata (for the manual-replay path via `gh run view`) and passes it in;
# the decision is a function of the inputs only. That is what makes it testable.
#
# All inputs are passed as flag values (never positionally interpolated into a
# shell), so the workflow can hand it untrusted `workflow_run` payload fields
# safely. Emits EXACTLY one token (`file`|`resolve`|`none`) on stdout and a
# one-line human reason on stderr; always exits 0 (the stdout token is the
# decision — a non-zero exit under the workflow's `set -e` would be noise).
set -euo pipefail
unset AGENT_GATE_SUMMARY_FILE 2>/dev/null || true

TRIGGER=""      # workflow_run | workflow_dispatch
GATE_NAME=""    # the expected gate workflow name (constant, from the workflow)
RUN_NAME=""     # the run's workflow name
EVENT=""        # the run's origin event (schedule|workflow_dispatch|push|pull_request|…)
BRANCH=""       # the run's head branch
CONCLUSION=""   # the run's conclusion (failure|cancelled|timed_out|success|…)
RUN_ID=""       # the run id (required numeric on the workflow_dispatch replay path)

while [ "$#" -gt 0 ]; do
  case "$1" in
    --trigger)    TRIGGER="${2:-}"; shift 2 ;;
    --gate-name)  GATE_NAME="${2:-}"; shift 2 ;;
    --name)       RUN_NAME="${2:-}"; shift 2 ;;
    --event)      EVENT="${2:-}"; shift 2 ;;
    --branch)     BRANCH="${2:-}"; shift 2 ;;
    --conclusion) CONCLUSION="${2:-}"; shift 2 ;;
    --run-id)     RUN_ID="${2:-}"; shift 2 ;;
    *) echo "gate-failure-mode: unknown argument '$1'" >&2; echo none; exit 0 ;;
  esac
done

decide() { echo "$1"; }

# Map a terminal conclusion to the side effect: RED → file, GREEN → resolve.
mode_for_conclusion() {
  case "$1" in
    failure|cancelled|timed_out) echo file ;;
    success) echo resolve ;;
    *) echo none ;;
  esac
}

# The gate workflow must be identified. On the auto workflow_run path the trigger
# already filters by workflow name, but validating it here too is cheap defense
# in depth and keeps both paths uniform + testable.
if [ -z "${GATE_NAME}" ]; then
  echo "gate-failure-mode: --gate-name is required" >&2
  decide none; exit 0
fi
if [ "${RUN_NAME}" != "${GATE_NAME}" ]; then
  echo "run workflow '${RUN_NAME:-<none>}' is not the gate lane '${GATE_NAME}' — none" >&2
  decide none; exit 0
fi

# Never act on a pull_request-origin run.
if [ "${EVENT}" = "pull_request" ]; then
  echo "origin event is pull_request — none" >&2
  decide none; exit 0
fi

# Manual replay (workflow_dispatch of the alert workflow): the caller supplied an
# arbitrary run_id. Require it present + numeric (fail-closed) before trusting the
# fetched metadata.
if [ "${TRIGGER}" = "workflow_dispatch" ]; then
  if [ -z "${RUN_ID}" ]; then
    echo "workflow_dispatch replay requires a run_id — none given — none" >&2
    decide none; exit 0
  fi
  if ! printf '%s' "${RUN_ID}" | grep -Eq '^[0-9]+$'; then
    echo "run_id '${RUN_ID}' is not numeric — none" >&2
    decide none; exit 0
  fi
fi

# Origin-event allowlist. The gate lane runs only on schedule (always the default
# branch = main) or workflow_dispatch; a dispatch on a non-main branch (e.g. a
# maintainer testing on a feature branch) must NOT touch the backstop issue.
case "${EVENT}" in
  schedule)
    : ;;
  workflow_dispatch)
    if [ "${BRANCH}" != "main" ]; then
      echo "gate dispatch on non-main branch '${BRANCH:-<none>}' — none" >&2
      decide none; exit 0
    fi ;;
  *)
    echo "origin event '${EVENT:-<none>}' is not schedule/workflow_dispatch — none" >&2
    decide none; exit 0 ;;
esac

MODE="$(mode_for_conclusion "${CONCLUSION}")"
if [ "${MODE}" = "none" ]; then
  echo "conclusion '${CONCLUSION:-<none>}' is not terminal red/green — none" >&2
fi
decide "${MODE}"

#!/usr/bin/env bash
# .claude/hooks/issue-gate.sh
#
# TaskCompleted advisory gate. Runs a FAST sanity check when a task is marked
# complete. It is deliberately NOT the gate of record — that is the flow-closer's
# ONE full `scripts/agent-gate.sh` run, immediately pre-merge (see CLAUDE.md).
#
# DEFUSED for issue #2671 (epic #2664 harness audit). The prior version pointed
# ISSUE_GATE_TEST_CMD at the FULL gate (`scripts/agent-gate.sh`, 12-25 min) under
# this hook's 600s timeout, ran it from whatever cwd the session happened to have
# (field-observed firing full gates from the MAIN checkout), and blocked on a
# synchronous `roborev --wait`. It could not succeed and contended for gate slots
# invisibly. The defusal:
#   * ISSUE_GATE_TEST_CMD now points at the LITE gate (`scripts/agent-gate.sh --lite`).
#   * The test command runs from the REPO ROOT of THIS hook file (derived from
#     BASH_SOURCE via git rev-parse), never the session cwd, with a UNIQUE mktemp
#     AGENT_GATE_SUMMARY_FILE so concurrent gates never contend on one summary path.
#   * A wall-clock budget (ISSUE_GATE_LITE_BUDGET_SECS, default 480s) fits inside the
#     600s hook timeout with margin; exceeding it FAILS OPEN with a visible warning
#     (task completion is NEVER blocked on a timeout — this hook is advisory).
#   * No hook path runs roborev anymore — the flow-* review pipeline owns reviews.
#   * If the repo has no Rust diff vs origin/main the hook skips entirely (cheap
#     early-exit, matching --lite's blast-radius philosophy).
#
# Exit 2 -> blocks completion (a genuine lite FAIL); stderr is fed back as the
# reason. Exit 0 -> allowed (PASS, no-diff skip, or a fail-open timeout).
#
# Configure via env (set in .claude/settings.json "env" or your shell):
#   ISSUE_GATE_TEST_CMD           the fast check, e.g. "scripts/agent-gate.sh --lite"
#   ISSUE_GATE_COVERAGE_CMD       exits non-zero when coverage is under threshold
#   ISSUE_GATE_LITE_BUDGET_SECS   wall-clock budget for the test cmd (default 480)

EVENT_JSON="$(cat || true)"   # TaskCompleted event JSON on stdin (context only)
: "${EVENT_JSON:=}"           # referenced only for context; keep shellcheck quiet

TEST_CMD="${ISSUE_GATE_TEST_CMD:-}"
COVERAGE_CMD="${ISSUE_GATE_COVERAGE_CMD:-}"
LITE_BUDGET_SECS="${ISSUE_GATE_LITE_BUDGET_SECS:-480}"

fail() {
  echo "Issue gate blocked task completion:" 1>&2
  echo "  $1" 1>&2
  exit 2
}

warn_open() {
  # Advisory-only fail-open: never block task completion. The gate of record is
  # the flow-closer's full gate.
  echo "issue-gate: WARNING — $1" 1>&2
  echo "issue-gate: FAILING OPEN (advisory hook only; the gate of record is the flow-closer's full gate)." 1>&2
  exit 0
}

# --- Resolve the repo root from THIS hook file, never the session cwd (#2671) ---
HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(git -C "$HOOK_DIR" rev-parse --show-toplevel 2>/dev/null || echo "")"
if [ -z "$REPO_ROOT" ]; then
  warn_open "could not resolve the repo root from ${HOOK_DIR:-<unknown>}; skipping the advisory check."
fi

# --- Cheap early-exit: no Rust diff vs origin/main -> nothing for --lite to do ---
# Only skip when we can POSITIVELY determine there is no Rust change; if the base
# ref is unresolvable we fall through and run the check (conservative).
if git -C "$REPO_ROOT" rev-parse --verify --quiet origin/main >/dev/null 2>&1; then
  RUST_DIFF="$(git -C "$REPO_ROOT" diff --name-only origin/main...HEAD -- '*.rs' 2>/dev/null || echo "")"
  if [ -z "$RUST_DIFF" ]; then
    echo "issue-gate: no Rust diff vs origin/main — skipping the advisory --lite check (#2671)." 1>&2
    exit 0
  fi
fi

# --- Run the test command under a wall-clock budget, from the repo root ---
# run_budgeted <budget-secs> <shell-command-string>
#   returns 0 on success, the child's exit code on genuine failure, 124 on timeout.
# Portable (no GNU `timeout` dependency — stock macOS lacks it): backgrounds the
# command and polls, TERM/KILL-ing it if the budget is exceeded.
run_budgeted() {
  local budget="$1" cmd="$2" pid waited=0
  ( cd "$REPO_ROOT" && eval "$cmd" ) </dev/null 1>&2 &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$waited" -ge "$budget" ]; then
      kill -TERM "$pid" 2>/dev/null
      sleep 2
      kill -KILL "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
      return 124
    fi
    sleep 2
    waited=$((waited + 2))
  done
  wait "$pid"
  return $?
}

if [ -n "$TEST_CMD" ]; then
  # Unique summary path so concurrent/foreign gates never contend on one file (#2671/#2079).
  AGENT_GATE_SUMMARY_FILE="$(mktemp -t issue-gate-lite-summary.XXXXXX 2>/dev/null || echo "${TMPDIR:-/tmp}/issue-gate-lite-summary.$$")"
  export AGENT_GATE_SUMMARY_FILE
  run_budgeted "$LITE_BUDGET_SECS" "$TEST_CMD"
  rc=$?
  rm -f "$AGENT_GATE_SUMMARY_FILE" 2>/dev/null
  if [ "$rc" -eq 124 ]; then
    warn_open "the lite check ('$TEST_CMD') exceeded its ${LITE_BUDGET_SECS}s budget."
  elif [ "$rc" -ne 0 ]; then
    fail "Lite check failed ('$TEST_CMD'). Fix the failures before marking this issue done (the full gate of record is the flow-closer's)."
  fi
else
  echo "issue-gate: ISSUE_GATE_TEST_CMD is unset — skipping the test check." 1>&2
fi

if [ -n "$COVERAGE_CMD" ]; then
  if ! ( cd "$REPO_ROOT" && eval "$COVERAGE_CMD" ) 1>&2; then
    fail "Coverage check failed ('$COVERAGE_CMD'). Add tests until coverage meets the threshold."
  fi
fi

# roborev: intentionally NOT run from this hook (#2671). A TaskCompleted hook must
# not launch a review — the flow-* delivery pipeline (rust-reviewer + roborev on the
# lite-green diff, then the flow-closer's final roborev pass) owns all reviews. The
# prior synchronous `roborev review --wait` here double-enqueued jobs and blocked
# task completion for minutes. Do not reintroduce it.

exit 0

#!/usr/bin/env bash
# .claude/hooks/issue-gate.sh
#
# TaskCompleted gate. An issue's task cannot be marked complete until:
#   1. the project's tests pass,
#   2. (optional) a coverage command passes its own threshold,
#   3. roborev has no failing reviews for the current branch.
#
# Exit 2 -> blocks completion; stderr is fed back as the reason. Exit 0 -> allowed.
#
# Configure via env (set in .claude/settings.json "env" or your shell):
#   ISSUE_GATE_TEST_CMD       e.g. "make test"
#   ISSUE_GATE_COVERAGE_CMD   exits non-zero when coverage is under threshold
#   ISSUE_GATE_ROBOREV        "1" (default) to run the roborev branch gate, "0" to skip

EVENT_JSON="$(cat || true)"   # TaskCompleted event JSON on stdin (context only)

TEST_CMD="${ISSUE_GATE_TEST_CMD:-}"
COVERAGE_CMD="${ISSUE_GATE_COVERAGE_CMD:-}"
ROBOREV_ENABLED="${ISSUE_GATE_ROBOREV:-1}"

fail() {
  echo "Issue gate blocked task completion:" 1>&2
  echo "  $1" 1>&2
  exit 2
}

if [ -n "$TEST_CMD" ]; then
  if ! eval "$TEST_CMD" 1>&2; then
    fail "Tests failed ('$TEST_CMD'). Fix the failures before marking this issue done."
  fi
else
  echo "issue-gate: ISSUE_GATE_TEST_CMD is unset — skipping the test check." 1>&2
fi

if [ -n "$COVERAGE_CMD" ]; then
  if ! eval "$COVERAGE_CMD" 1>&2; then
    fail "Coverage check failed ('$COVERAGE_CMD'). Add tests until coverage meets the threshold."
  fi
fi

# roborev: no unresolved findings on the current branch. --wait gives a synchronous
# verdict (exit 0 = PASS, 1 = FAIL). Use --branch= with the equals sign. If you rely on
# roborev's post-commit hook, you can swap the line below for `roborev wait` to avoid
# enqueuing a duplicate review job.
if [ "$ROBOREV_ENABLED" = "1" ] && command -v roborev >/dev/null 2>&1; then
  BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")"
  if [ -n "$BRANCH" ] && [ "$BRANCH" != "HEAD" ]; then
    if ! roborev review --branch="$BRANCH" --wait --quiet 1>&2; then
      fail "roborev found unresolved issues on branch '$BRANCH'. Open 'roborev tui' or run '/roborev-fix', resolve them, then retry."
    fi
  fi
fi

exit 0

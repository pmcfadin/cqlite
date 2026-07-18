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
#     (task completion is NEVER blocked on a timeout — this hook is advisory). On a
#     timeout the ENTIRE process group is killed (TERM, 2s grace, then KILL), so a
#     backgrounded cargo/gate grandchild can never keep contending for gate slots
#     invisibly — the exact failure mode this defusal exists to close. The poll
#     interval ramps 0.2s -> 2s, so the effective ceiling is budget + ~2s poll slop
#     + 2s TERM grace (still far under the 600s hook timeout).
#   * No hook path runs roborev anymore — the flow-* review pipeline owns reviews.
#   * If a FULL gate currently holds a machine-wide concurrency slot, the advisory
#     check is SKIPPED — yielding to the gate of record is always correct (#1930/#2640).
#   * If the repo has no Rust diff vs origin/main the advisory --lite test is
#     intentionally SKIPPED (matching --lite's blast-radius philosophy) — a non-Rust
#     diff is left uncovered by this test. A wired coverage command still runs.
#   * The gate's own verbose output goes to a temp log; on a genuine FAIL the block
#     reason is ONLY the lite SUMMARY block + the log path — never the raw log
#     (mirroring the doctrine invocation: read the SUMMARY, never the gate log). The
#     summary file is removed via an EXIT trap; the output log is RETAINED on a FAIL
#     (so the block reason's path resolves) and removed on PASS/timeout.
#   * The FAILING-OPEN notice is emitted on BOTH stdout and stderr so it reaches the
#     session (hook stdout is surfaced on a success/exit-0 completion).
#
# Exit 2 -> blocks completion (a genuine lite FAIL); stderr is fed back as the
# reason. Exit 0 -> allowed (PASS, slot/no-diff skip, or a fail-open timeout).
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

# Validate the budget numerically before any arithmetic uses it (a non-integer would
# break the `[ ... -ge ]` test and could hang the poll). Fall back to the default.
case "$LITE_BUDGET_SECS" in
  ''|*[!0-9]*)
    echo "issue-gate: WARNING — ISSUE_GATE_LITE_BUDGET_SECS='$LITE_BUDGET_SECS' is not a non-negative integer; using 480." 1>&2
    LITE_BUDGET_SECS=480 ;;
esac

fail() {
  echo "Issue gate blocked task completion:" 1>&2
  echo "  $1" 1>&2
  exit 2
}

warn_line() {
  # Advisory-only warning helper. It NEVER exits — the caller decides whether to
  # continue (e.g. so a wired coverage command still runs after a fail-open timeout).
  # Emitted on BOTH stdout (surfaced to the session on an exit-0 completion) and
  # stderr (always visible), so a FAILING-OPEN notice can never be swallowed (#2671).
  printf 'issue-gate: WARNING — %s\nissue-gate: (advisory hook only; the gate of record is the flow-closer'"'"'s full gate).\n' "$1"
  printf 'issue-gate: WARNING — %s\nissue-gate: (advisory hook only; the gate of record is the flow-closer'"'"'s full gate).\n' "$1" 1>&2
}

# full_gate_active: true (0) when ANY gate slot holder currently holds one of the
# machine-wide concurrency slots (issue #1825). In practice only FULL gates take a
# slot (--lite self-exempts), so a held slot means the gate of record is running;
# distinguishing full-vs-lite holders is not worth a new slot-file protocol here.
# Slots live under $CQLITE_GATE_SLOTS_DIR (default ${TMPDIR:-/tmp}/cqlite-gate-slots)
# as slot.N files held by a live daemon via a non-blocking fcntl.flock (LOCK_EX).
# Existence alone is stale-prone (the file survives the gate), so we TEST the lock:
# python3 tries a non-blocking flock on each slot; a slot it cannot lock (or cannot
# even open, because another UID's live gate owns it) is held -> a gate is active.
# No python3 (or no dir) -> we cannot tell -> return false (fail toward running).
full_gate_active() {
  local dir="${CQLITE_GATE_SLOTS_DIR:-${TMPDIR:-/tmp}/cqlite-gate-slots}"
  [ -d "$dir" ] || return 1
  command -v python3 >/dev/null 2>&1 || return 1
  python3 - "$dir" <<'PY'
import sys, os, fcntl, glob
d = sys.argv[1]
for p in glob.glob(os.path.join(d, "slot.*")):
    try:
        fd = os.open(p, os.O_RDWR)
    except PermissionError:
        sys.exit(0)   # another UID owns the slot file -> its live gate holds it
    except OSError:
        continue
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        sys.exit(0)   # locked -> a gate holds this slot
    finally:
        os.close(fd)
sys.exit(1)           # no held slot found
PY
}

# --- Resolve the repo root from THIS hook file, never the session cwd (#2671) ---
HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(git -C "$HOOK_DIR" rev-parse --show-toplevel 2>/dev/null || echo "")"
if [ -z "$REPO_ROOT" ]; then
  # No repo root -> nothing this advisory hook can run; exit clean (never block).
  warn_line "could not resolve the repo root from ${HOOK_DIR:-<unknown>}; skipping the advisory check."
  exit 0
fi

# --- Slot-aware skip: yield to the gate of record (#1930/#2640) ---
# If a FULL gate holds a slot, an advisory --lite run would contend for CPU/slots
# invisibly — the exact hazard this defusal closes. Skip the whole advisory check.
if full_gate_active; then
  echo "issue-gate: full gate active — advisory check skipped" 1>&2
  exit 0
fi

# --- Cheap skip: no Rust change (committed OR working-tree) -> nothing for --lite ---
# This short-circuits ONLY the TEST_CMD block (a wired coverage command still runs).
# Skip only when BOTH the committed diff vs origin/main AND the working tree are free
# of *.rs changes. `|| echo x` on either query FAILS TOWARD RUNNING (a git error makes
# the var non-empty, so we do NOT skip). If the base ref is unresolvable we run too.
SKIP_TEST=0
if git -C "$REPO_ROOT" rev-parse --verify --quiet origin/main >/dev/null 2>&1; then
  RUST_DIFF="$(git -C "$REPO_ROOT" diff --name-only origin/main...HEAD -- '*.rs' 2>/dev/null || echo x)"
  RUST_DIRTY="$(git -C "$REPO_ROOT" status --porcelain -- '*.rs' 2>/dev/null || echo x)"
  if [ -z "$RUST_DIFF" ] && [ -z "$RUST_DIRTY" ]; then
    SKIP_TEST=1
  fi
fi

# --- Run the test command under a wall-clock budget, from the repo root ---
# run_budgeted <budget-secs> <shell-command-string> <output-log>
#   Returns the child's real exit code; sets the GLOBAL BUDGET_TIMED_OUT=1 (never a
#   124 sentinel — a child that genuinely exits 124 must still BLOCK) when it killed
#   the child for exceeding the budget. Command output goes to <output-log> (never to
#   this hook's stderr) so a FAIL reason can be ONLY the SUMMARY + the log path.
# Portable (no GNU `timeout` dependency — stock macOS lacks it): `set -m` puts the
# backgrounded command in its OWN process group (group id == $!), so on a timeout we
# kill the NEGATIVE pid (the whole group: the subshell, the gate, and any cargo/gate
# grandchildren) — a direct-child-only kill would orphan the grandchild to keep
# contending for gate slots, the very failure this defusal closes. `set +m` right
# after the fork silences async job-completion notices without moving the child back
# out of its group. Elapsed time is real integer wall-clock (date +%s) — no float
# accounting, no drift. On timeout we group-kill with a direct-pid fallback and the
# final wait can never block. The poll interval ramps 0.2s -> 2s to stay cheap, so the
# effective ceiling is budget + ~2s poll slop + 2s TERM grace.
BUDGET_TIMED_OUT=0
run_budgeted() {
  local budget="$1" cmd="$2" log="$3" pid t0 interval_tenths=2 child_pgid
  BUDGET_TIMED_OUT=0
  set -m
  ( cd "$REPO_ROOT" && eval "$cmd" ) >"$log" 2>&1 </dev/null &
  pid=$!
  set +m
  t0=$(date +%s)
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$(( $(date +%s) - t0 ))" -ge "$budget" ]; then
      # SAFETY: only signal the process GROUP when the child actually leads its own
      # group (pgid == pid) — i.e. `set -m` really gave it one. If `set -m` silently
      # failed, the child shares OUR group, and a negative-pid kill would signal this
      # hook's own session; fall back to a direct single-process kill in that case.
      child_pgid=$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')
      if [ -n "$child_pgid" ] && [ "$child_pgid" = "$pid" ]; then
        kill -TERM "-$child_pgid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
        sleep 2
        kill -KILL "-$child_pgid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null
      else
        kill -TERM "$pid" 2>/dev/null
        sleep 2
        kill -KILL "$pid" 2>/dev/null
      fi
      wait "$pid" 2>/dev/null || true
      BUDGET_TIMED_OUT=1
      return 124
    fi
    sleep "$(printf '%d.%d' "$((interval_tenths / 10))" "$((interval_tenths % 10))")"
    interval_tenths=$((interval_tenths * 2))
    [ "$interval_tenths" -gt 20 ] && interval_tenths=20
  done
  wait "$pid"
  return $?
}

if [ "$SKIP_TEST" = "1" ]; then
  echo "issue-gate: no Rust diff vs origin/main — the advisory --lite test is intentionally skipped (#2671)." 1>&2
elif [ -n "$TEST_CMD" ]; then
  # Unique summary + log paths so concurrent/foreign gates never contend (#2671/#2079).
  AGENT_GATE_SUMMARY_FILE="$(mktemp -t issue-gate-lite-summary.XXXXXX 2>/dev/null || echo "${TMPDIR:-/tmp}/issue-gate-lite-summary.$$")"
  GATE_OUTPUT_LOG="$(mktemp -t issue-gate-lite.XXXXXX.log 2>/dev/null || echo "${TMPDIR:-/tmp}/issue-gate-lite.$$.log")"
  # Always remove the summary file on exit, even if fail()/a signal short-circuits the
  # explicit rm below. The output log is intentionally NOT cleaned here — it is retained
  # on a FAIL so the block reason's log path stays valid (see header).
  trap 'rm -f "${AGENT_GATE_SUMMARY_FILE:-}"' EXIT
  export AGENT_GATE_SUMMARY_FILE
  run_budgeted "$LITE_BUDGET_SECS" "$TEST_CMD" "$GATE_OUTPUT_LOG"
  rc=$?
  if [ "$BUDGET_TIMED_OUT" = "1" ]; then
    rm -f "$AGENT_GATE_SUMMARY_FILE" "$GATE_OUTPUT_LOG" 2>/dev/null
    # FAIL OPEN on a budget overrun: warn and fall through (never block; a wired
    # coverage command still runs, then the hook exits 0). A genuine child exit 124
    # is NOT a timeout (BUDGET_TIMED_OUT stays 0) and takes the block path below.
    warn_line "the lite check ('$TEST_CMD') exceeded its ${LITE_BUDGET_SECS}s budget — FAILING OPEN."
  elif [ "$rc" -ne 0 ]; then
    # Block reason = ONLY the SUMMARY block (the only retainable text, per doctrine)
    # plus the log path — never the raw gate log.
    if [ -s "$AGENT_GATE_SUMMARY_FILE" ]; then
      echo "issue-gate: --- lite gate SUMMARY (retainable text) ---" 1>&2
      cat "$AGENT_GATE_SUMMARY_FILE" 1>&2
      echo "issue-gate: --- end lite gate SUMMARY ---" 1>&2
    fi
    rm -f "$AGENT_GATE_SUMMARY_FILE" 2>/dev/null
    fail "lite check failed — see the SUMMARY above; full output: $GATE_OUTPUT_LOG"
  fi
  rm -f "$AGENT_GATE_SUMMARY_FILE" "$GATE_OUTPUT_LOG" 2>/dev/null
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

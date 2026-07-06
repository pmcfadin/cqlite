#!/usr/bin/env bash
# scripts/local/worker-supervisor.sh — unattended worker recycle loop (issue #2090).
#
# One issue, one worker session, one process exit (context economy). This
# supervisor is what turns that into a safe *overnight loop*: single-instance
# lock, fail-closed preflight (wait, never spin), a crash-loop breaker, hard
# issue/wall-clock budgets, and one journal line + notification per iteration.
# The worker itself is opaque to this script — it is judged ONLY by its exit
# code and the ITERATION MARKER it leaves behind.
#
# ============================================================================
# ITERATION MARKER CONTRACT (mirrored in
# docs/scratch/agentic-workflow-audit/doc-deltas-supervisor.md — the worker
# skill MUST implement exactly this contract)
# ============================================================================
# The worker's LAST act, whatever else happened, is to write JSON to
# $MARKER_FILE (default: <repo-root>/.worker-last-iteration.json):
#
#   {"outcome":"finalized|no-work|blocked","issue":<int|null>,
#    "pr":"<url>|null","duration_s":<int>,"reason":"<string, required if blocked>"}
#
#   finalized — claimed an issue, drove it through gate/review/merge-on-green,
#               flow-finalized it. issue + pr MUST be set. Counts toward
#               MAX_ISSUES. Resets the crash-loop breaker.
#   no-work   — rehydrated from the board, nothing Ready (or nothing to
#               resume). issue/pr may be null. Does NOT count toward
#               MAX_ISSUES; triggers a BACKOFF_NOWORK_SECS sleep so the loop
#               doesn't hot-poll an empty board. Resets the breaker.
#   blocked   — made progress but stopped short of merge for a reason needing
#               the owner (design-call finding, scope question, HOLD order,
#               unmet requirement). "reason" MUST be set. Does NOT count
#               toward MAX_ISSUES. Resets the breaker; the issue is
#               remembered (LAST_BLOCKED_ISSUE) but never auto-retried.
#
# Any other outcome value, a marker missing required fields, a nonzero worker
# exit code, OR no marker file present when the worker process exits => the
# iteration is judged ABNORMAL and counts toward BREAKER_N. The supervisor
# removes any pre-existing marker before every spawn — a marker left behind
# by a prior (possibly crashed) invocation must never be re-judged as this
# iteration's outcome.
# ============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Config (env-overridable; all defaults per issue #2090)
# ---------------------------------------------------------------------------
MAX_ISSUES="${MAX_ISSUES:-4}"
MAX_HOURS="${MAX_HOURS:-8}"
DISK_FLOOR_GB="${DISK_FLOOR_GB:-40}"
BREAKER_N="${BREAKER_N:-3}"
BACKOFF_NOWORK_SECS="${BACKOFF_NOWORK_SECS:-900}"
HOLD_POLL_SECS="${HOLD_POLL_SECS:-300}"
MAX_ITER_SECS="${MAX_ITER_SECS:-7200}"

SUPERVISOR_LOCK="${SUPERVISOR_LOCK:-${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock}"
STOP_FILE="${STOP_FILE:-$REPO_ROOT/.worker-stop}"
MARKER_FILE="${MARKER_FILE:-$REPO_ROOT/.worker-last-iteration.json}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs/worker-supervisor}"

detect_ncpu() {
  if command -v nproc >/dev/null 2>&1; then nproc
  elif command -v getconf >/dev/null 2>&1 && getconf _NPROCESSORS_ONLN >/dev/null 2>&1; then getconf _NPROCESSORS_ONLN
  elif command -v sysctl >/dev/null 2>&1; then sysctl -n hw.ncpu
  else echo 4
  fi
}
LOAD_MAX="${LOAD_MAX:-$(detect_ncpu)}"

# Real invocation (documented default; every real fleet run should pin this
# explicitly rather than rely on the default prompt text drifting).
if [[ -z "${WORKER_CMD:-}" ]]; then
  WORKER_CMD="claude --agent worker 'Resume the existing issue-<N>-* claim branch on this machine if one exists; otherwise claim the next Ready issue from the board. Run it to completion (implement, gate, review, merge on green, finalize, telemetry stamp). Write the iteration marker as your last act, then exit.'"
fi

# NOTE: default probe commands are assigned via explicit if/then blocks, not a
# compact "${VAR:-default}" one-liner — the deeply nested/escaped quoting that
# form requires corrupts the ALREADY-SET value at bash's parse stage (verified
# empirically: a real override got mangled even though the default clause is
# never evaluated when the var is set). Plain assignment has no such hazard.
if [[ -z "${LOAD_PROBE_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # literal $1 for the later `bash -c` eval, not expanded now.
  LOAD_PROBE_CMD='if [ -r /proc/loadavg ]; then cut -d" " -f1 /proc/loadavg; else sysctl -n vm.loadavg | tr -d "{}" | awk "{print \$1}"; fi'
fi
if [[ -z "${DISK_PROBE_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # single-quoted $4 is intentional: literal text for the later `bash -c` eval, not expanded now.
  DISK_PROBE_CMD="df -Pk \"$REPO_ROOT\" | awk 'NR==2{print int(\$4/1024/1024)}'"
fi
if [[ -z "${PROC_PROBE_CMD:-}" ]]; then
  PROC_PROBE_CMD="pgrep -f 'cargo |nextest|gate_slot_daemon' | wc -l | tr -d ' '"
fi

NOOP_NOTIFY_MARKER="__noop_notify__"
if [[ -z "${NOTIFY_CMD:-}" ]]; then
  if command -v agent-notify >/dev/null 2>&1; then NOTIFY_CMD="agent-notify"; else NOTIFY_CMD="$NOOP_NOTIFY_MARKER"; fi
fi

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
WARNED_NOOP_NOTIFY=0
log() { printf '[worker-supervisor] %s\n' "$*" >&2; }

notify() {
  local priority="$1" title="$2" message="$3" category="completion"
  [[ "$priority" == "high" ]] && category="error"
  if [[ "$NOTIFY_CMD" == "$NOOP_NOTIFY_MARKER" ]]; then
    if [[ "$WARNED_NOOP_NOTIFY" -eq 0 ]]; then
      log "WARN: agent-notify not on PATH; notifications are no-ops for this run"
      WARNED_NOOP_NOTIFY=1
    fi
    return 0
  fi
  "$NOTIFY_CMD" --category "$category" "$title" "$message" || log "WARN: notify command failed (non-fatal)"
}

is_gt() { awk -v a="$1" -v b="$2" 'BEGIN{ if ((a+0)>(b+0)) exit 0; exit 1 }'; }
is_lt() { awk -v a="$1" -v b="$2" 'BEGIN{ if ((a+0)<(b+0)) exit 0; exit 1 }'; }

marker_field() {
  local field="$1"
  [[ -f "$MARKER_FILE" ]] || return 0
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg f "$field" '.[$f] // empty' "$MARKER_FILE" 2>/dev/null
  else
    python3 -c '
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
v = d.get(sys.argv[2])
print(v if v is not None else "")
' "$MARKER_FILE" "$field" 2>/dev/null || true
  fi
}

json_or_null() { [[ -n "$1" ]] && printf '"%s"' "$1" || printf 'null'; }
num_or_null() { [[ "$1" =~ ^[0-9]+$ ]] && printf '%s' "$1" || printf 'null'; }

journal_line() {
  mkdir -p "$LOG_DIR"
  local jf="${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  printf '{"ts":"%s","iter":%d,"outcome":"%s","issue":%s,"pr":%s,"duration_s":%d,"exit_code":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" "$(num_or_null "$3")" "$(json_or_null "$4")" "$5" "$6" >>"$jf"
}

# ---------------------------------------------------------------------------
# Single-instance lock. macOS ships no flock(1); an atomic mkdir + pid-liveness
# check gives the same "only one supervisor per machine" guarantee portably.
# ---------------------------------------------------------------------------
acquire_lock() {
  if mkdir "$SUPERVISOR_LOCK" 2>/dev/null; then
    echo $$ >"$SUPERVISOR_LOCK/pid"
    trap 'rm -rf "$SUPERVISOR_LOCK" 2>/dev/null || true' EXIT
    return 0
  fi
  local holder_pid=""
  [[ -f "$SUPERVISOR_LOCK/pid" ]] && holder_pid="$(cat "$SUPERVISOR_LOCK/pid" 2>/dev/null || true)"
  if [[ -n "$holder_pid" ]] && kill -0 "$holder_pid" 2>/dev/null; then
    echo "worker-supervisor: another instance is already running (pid $holder_pid, lock $SUPERVISOR_LOCK)" >&2
    exit 1
  fi
  log "reclaiming stale lock $SUPERVISOR_LOCK (holder pid $holder_pid not alive)"
  rm -rf "$SUPERVISOR_LOCK"
  if mkdir "$SUPERVISOR_LOCK" 2>/dev/null; then
    echo $$ >"$SUPERVISOR_LOCK/pid"
    trap 'rm -rf "$SUPERVISOR_LOCK" 2>/dev/null || true' EXIT
    return 0
  fi
  echo "worker-supervisor: failed to acquire lock $SUPERVISOR_LOCK" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# Portable timeout wrapper (GNU coreutils `timeout` is not guaranteed on macOS).
# ---------------------------------------------------------------------------
run_with_timeout() {
  local secs="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$secs" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$secs" "$@"
  else
    "$@" &
    local pid=$!
    (sleep "$secs" && kill -TERM "$pid" 2>/dev/null) &
    local watcher=$!
    local rc=0
    wait "$pid" || rc=$?
    kill "$watcher" 2>/dev/null || true
    wait "$watcher" 2>/dev/null || true
    return "$rc"
  fi
}

# ---------------------------------------------------------------------------
# Preflight: fail-closed, wait-don't-spin. Returns a hold reason on stdout, or
# empty when clear. stop-file / budgets are handled by the caller (clean exit,
# not a hold).
# ---------------------------------------------------------------------------
preflight_reason() {
  local load procs disk
  load="$(bash -c "$LOAD_PROBE_CMD" 2>/dev/null || echo 0)"
  if is_gt "$load" "$LOAD_MAX"; then
    echo "load"
    return 0
  fi
  procs="$(bash -c "$PROC_PROBE_CMD" 2>/dev/null || echo 0)"
  if [[ "$procs" =~ ^[0-9]+$ ]] && [[ "$procs" -gt 0 ]]; then
    echo "leftover-processes"
    return 0
  fi
  disk="$(bash -c "$DISK_PROBE_CMD" 2>/dev/null || echo 999999)"
  if is_lt "$disk" "$DISK_FLOOR_GB"; then
    echo "disk"
    return 0
  fi
  echo ""
}

LAST_HOLD_REASON=""
preflight_wait() {
  while true; do
    [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
    local reason
    reason="$(preflight_reason)"
    if [[ -z "$reason" ]]; then
      LAST_HOLD_REASON=""
      return 0
    fi
    if [[ "$reason" != "$LAST_HOLD_REASON" ]]; then
      notify "high" "worker-supervisor HOLD" "HOLD: $reason (repolling every ${HOLD_POLL_SECS}s, no spawn)"
      LAST_HOLD_REASON="$reason"
    fi
    log "HOLD: $reason; sleeping ${HOLD_POLL_SECS}s"
    sleep "$HOLD_POLL_SECS"
  done
}

# ---------------------------------------------------------------------------
# State + exit paths
# ---------------------------------------------------------------------------
ITER=0
ISSUES_DONE=0
CONSECUTIVE_ABNORMAL=0
LAST_BLOCKED_ISSUE=""
START_TS=$(date +%s)
MAX_HOURS_SECS=$((MAX_HOURS * 3600))

finalize_exit() {
  local reason="$1" code="$2"
  local elapsed=$(($(date +%s) - START_TS))
  mkdir -p "$LOG_DIR"
  printf '{"ts":"%s","iter":%d,"outcome":"summary","reason":"%s","issues_done":%d,"elapsed_s":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ITER" "$reason" "$ISSUES_DONE" "$elapsed" \
    >>"${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  local prio="info"
  [[ "$reason" == "breaker" ]] && prio="high"
  notify "$prio" "worker-supervisor stopped" "reason=$reason issues_done=$ISSUES_DONE elapsed_s=$elapsed"
  exit "$code"
}

# ---------------------------------------------------------------------------
# One iteration: spawn, judge, journal.
# ---------------------------------------------------------------------------
run_iteration() {
  ITER=$((ITER + 1))
  rm -f "$MARKER_FILE"
  mkdir -p "$LOG_DIR"
  local logfile="$LOG_DIR/iter-${ITER}.log"
  local t0 t1 rc=0
  t0=$(date +%s)
  set +e
  run_with_timeout "$MAX_ITER_SECS" bash -c "$WORKER_CMD" >"$logfile" 2>&1
  rc=$?
  set -e
  t1=$(date +%s)
  local duration=$((t1 - t0))

  if [[ "$rc" -ne 0 ]] || [[ ! -f "$MARKER_FILE" ]]; then
    journal_line "$ITER" "abnormal" "" "" "$duration" "$rc"
    log "iteration $ITER abnormal (exit=$rc marker_present=$([[ -f "$MARKER_FILE" ]] && echo yes || echo no))"
    trip_breaker_or_continue
    return 0
  fi

  local outcome issue pr reason
  outcome="$(marker_field outcome)"
  issue="$(marker_field issue)"
  pr="$(marker_field pr)"
  reason="$(marker_field reason)"

  case "$outcome" in
    finalized)
      CONSECUTIVE_ABNORMAL=0
      ISSUES_DONE=$((ISSUES_DONE + 1))
      journal_line "$ITER" "finalized" "$issue" "$pr" "$duration" "$rc"
      notify "info" "worker-supervisor: finalized issue $issue" "pr=$pr duration_s=$duration"
      ;;
    no-work)
      CONSECUTIVE_ABNORMAL=0
      journal_line "$ITER" "no-work" "$issue" "$pr" "$duration" "$rc"
      log "no work available; backing off ${BACKOFF_NOWORK_SECS}s"
      sleep "$BACKOFF_NOWORK_SECS"
      ;;
    blocked)
      CONSECUTIVE_ABNORMAL=0
      LAST_BLOCKED_ISSUE="$issue"
      journal_line "$ITER" "blocked" "$issue" "$pr" "$duration" "$rc"
      notify "info" "worker-supervisor: blocked on issue $issue" "${reason:-no reason given}"
      log "remembered blocked issue $LAST_BLOCKED_ISSUE (not auto-retried)"
      ;;
    *)
      journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc"
      log "iteration $ITER abnormal (unrecognized marker outcome '$outcome')"
      trip_breaker_or_continue
      ;;
  esac
}

trip_breaker_or_continue() {
  CONSECUTIVE_ABNORMAL=$((CONSECUTIVE_ABNORMAL + 1))
  if [[ "$CONSECUTIVE_ABNORMAL" -ge "$BREAKER_N" ]]; then
    notify "high" "worker-supervisor BREAKER" "$BREAKER_N consecutive abnormal iterations — stopping, no hot respawn"
    finalize_exit "breaker" 1
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  acquire_lock
  log "started: MAX_ISSUES=$MAX_ISSUES MAX_HOURS=$MAX_HOURS LOAD_MAX=$LOAD_MAX DISK_FLOOR_GB=$DISK_FLOOR_GB BREAKER_N=$BREAKER_N"
  while true; do
    [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
    [[ $(($(date +%s) - START_TS)) -ge "$MAX_HOURS_SECS" ]] && finalize_exit "budget-wallclock" 0
    [[ "$ISSUES_DONE" -ge "$MAX_ISSUES" ]] && finalize_exit "budget-issues" 0

    preflight_wait

    run_iteration
  done
}

main "$@"

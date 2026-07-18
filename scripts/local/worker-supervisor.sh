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
#               flow-finalized it. issue + pr MUST be set (both non-null,
#               non-missing) — a "finalized" marker with a null/missing issue
#               or pr is judged ABNORMAL (counts toward BREAKER_N, does NOT
#               count toward MAX_ISSUES) rather than trusted at face value.
#               Otherwise counts toward MAX_ISSUES. Resets the breaker.
#   no-work   — rehydrated from the board, nothing Ready (or nothing to
#               resume). issue/pr may be null. Does NOT count toward
#               MAX_ISSUES; triggers a BACKOFF_NOWORK_SECS sleep so the loop
#               doesn't hot-poll an empty board. Resets the breaker.
#   blocked   — made progress but stopped short of merge for a reason needing
#               the owner (design-call finding, scope question, HOLD order,
#               unmet requirement). "reason" MUST be set. Does NOT count
#               toward MAX_ISSUES. Resets the breaker; the issue is
#               remembered (LAST_BLOCKED_ISSUE). If the SAME issue reports
#               "blocked" on two consecutive iterations, the supervisor treats
#               the queue as head-blocked and STOPS cleanly (high-priority
#               notify, exit 0) rather than looping until MAX_HOURS — it is
#               retried exactly once, never auto-retried indefinitely.
#
#               Two "blocked" reasons are a distinct CLEAN PARK (issue #2666,
#               park-and-resume) rather than an owner-escalation:
#                 reason=seam1-approval  — the worker hit Seam 1 (an unapproved
#                                          design spec) in an unattended session.
#                 reason=needs-decision  — a genuine mid-run owner decision.
#               For either, the worker's contract is: post ONE structured
#               question comment on the issue (rendered options + recommendation
#               + default), add the `needs-decision` label, write this marker
#               (optionally an extra "question" field = one-line question
#               summary for the page), and EXIT — releasing the machine. NEVER
#               wait, NEVER call AskUserQuestion unattended. The supervisor
#               judges these NORMAL: verdict `parked-on-owner`, never toward the
#               breaker, does NOT head-block the queue (the labeled issue is
#               excluded from the worker's next pickup until the owner answers
#               and the label clears), fires ONE high-priority page, and moves
#               on to the next Ready issue.
#
# stuck-on-question (mid-iteration, no marker) — a worker WEDGED on an
#               interactive prompt (AskUserQuestion / permission prompt / menu)
#               in an unattended session never writes a marker; it just burns
#               MAX_ITER_SECS and would look "abnormal". A lightweight watchdog
#               tails the live iter-N.log every STUCK_POLL_SECS for prompt
#               signatures; on a hit it pages the owner immediately and records
#               verdict `stuck-on-question` when the worker later exits without
#               a clean marker. This verdict is NOT abnormal and never counts
#               toward BREAKER_N.
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
# Mid-iteration stuck-on-question watchdog (issue #2666): how often to tail the
# live worker log for an interactive-prompt signature, and the signatures to
# match. Both env-overridable (tests tighten the poll interval).
STUCK_POLL_SECS="${STUCK_POLL_SECS:-30}"
PROMPT_SIGNATURE_RE="${PROMPT_SIGNATURE_RE:-AskUserQuestion|Do you want to|waiting for input|❯}"

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

# detect_prompt_signature <logfile>: true (exit 0) when the tail of the worker's
# live log shows an interactive-prompt signature — a wedged AskUserQuestion, a
# permission prompt, a `❯` menu block, or a "waiting for input" line (issue
# #2666). Best-effort sensor, tuned to the unattended-session invariant that the
# worker NEVER legitimately prompts (it parks instead — see the marker contract).
detect_prompt_signature() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 1
  tail -n 80 "$logfile" 2>/dev/null | grep -qE "$PROMPT_SIGNATURE_RE"
}

# captured_question <logfile>: the matching prompt line(s), collapsed to a single
# ≤300-char line for the ntfy body. Empty when nothing matched.
captured_question() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 0
  tail -n 80 "$logfile" 2>/dev/null | grep -E "$PROMPT_SIGNATURE_RE" 2>/dev/null \
    | head -n 3 | tr '\n' ' ' | cut -c1-300
}

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

# json_or_null <value>: quotes+escapes a string field for embedding into the
# JSONL journal, or emits a bare `null` when empty. A quote/backslash/newline
# in an untrusted field (pr URL, blocked "reason" text) must never be allowed
# to corrupt journal JSON — printf '"%s"' alone does not escape those chars.
# Preferred path: python3 json.dumps (already a project dependency, same as
# marker_field's jq-absent fallback). Degraded fallback (python3 somehow
# absent): strip to a conservative safe charset so the line still parses,
# rather than emitting broken JSON.
json_or_null() {
  local v="$1"
  [[ -n "$v" ]] || { printf 'null'; return 0; }
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$v" 2>/dev/null && return 0
  fi
  printf '"%s"' "$(printf '%s' "$v" | tr -cd 'A-Za-z0-9:/._#?=&-')"
}
num_or_null() { [[ "$1" =~ ^[0-9]+$ ]] && printf '%s' "$1" || printf 'null'; }

# journal_line <iter> <outcome> <issue> <pr> <duration_s> <exit_code> [reason]
# `reason` is optional (only "blocked" iterations pass one); both `pr` and
# `reason` are free-form worker-controlled text and go through json_or_null so
# a stray quote/backslash/newline can never corrupt the JSONL line.
journal_line() {
  mkdir -p "$LOG_DIR"
  local jf="${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  printf '{"ts":"%s","iter":%d,"outcome":"%s","issue":%s,"pr":%s,"duration_s":%d,"exit_code":%d,"reason":%s}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" "$(num_or_null "$3")" "$(json_or_null "$4")" "$5" "$6" "$(json_or_null "${7:-}")" >>"$jf"
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
  # Atomic reclaim (rename-then-remove), not rm-then-mkdir: two supervisors
  # racing a dead-pid lock both taking the rm-then-mkdir path could both end up
  # believing they won. `mv` on the same filesystem is atomic, so only ONE
  # racer's mv can succeed against a given stale directory name; that racer
  # removes the renamed-aside copy and falls through to its own mkdir below.
  # The loser's mv fails (the name is already gone), so it falls through to the
  # normal mkdir-fails path and exits loudly instead of silently co-running.
  if mv "$SUPERVISOR_LOCK" "$SUPERVISOR_LOCK.stale.$$" 2>/dev/null; then
    rm -rf "$SUPERVISOR_LOCK.stale.$$"
  fi
  if mkdir "$SUPERVISOR_LOCK" 2>/dev/null; then
    echo $$ >"$SUPERVISOR_LOCK/pid"
    trap 'rm -rf "$SUPERVISOR_LOCK" 2>/dev/null || true' EXIT
    return 0
  fi
  echo "worker-supervisor: failed to acquire lock $SUPERVISOR_LOCK" >&2
  exit 1
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
  local stuck_flag="$LOG_DIR/.iter-${ITER}.stuck"
  rm -f "$stuck_flag"
  local t0 t1 rc=0
  t0=$(date +%s)
  # Spawn the worker in the background and supervise it with ONE poll loop that
  # does two jobs at once (issue #2666): enforce MAX_ITER_SECS (portably — no
  # coreutils `timeout` on macOS), and tail the live log for interactive-prompt
  # signatures. A worker wedged on a prompt in an unattended session is invisible
  # to an exit-only judge — it just burns MAX_ITER_SECS and looks "abnormal".
  # The watchdog pages the owner the instant a signature appears and records a
  # `stuck-on-question` verdict so the wedge never counts toward the breaker.
  set +e
  bash -c "$WORKER_CMD" >"$logfile" 2>&1 &
  local wpid=$!
  local deadline=$((t0 + MAX_ITER_SECS))
  local stuck_notified=0 g
  while kill -0 "$wpid" 2>/dev/null; do
    if [[ "$(date +%s)" -ge "$deadline" ]]; then
      log "iteration $ITER exceeded MAX_ITER_SECS=${MAX_ITER_SECS}s; terminating worker"
      kill -TERM "$wpid" 2>/dev/null
      g=0
      while kill -0 "$wpid" 2>/dev/null && [[ "$g" -lt 5 ]]; do sleep 1; g=$((g + 1)); done
      kill -KILL "$wpid" 2>/dev/null
      break
    fi
    if [[ "$stuck_notified" -eq 0 ]] && detect_prompt_signature "$logfile"; then
      local qtext
      qtext="$(captured_question "$logfile")"
      printf '%s' "$qtext" >"$stuck_flag"
      notify "high" "worker-supervisor: stuck-on-question (iter $ITER)" \
        "worker appears blocked on an interactive prompt: ${qtext:-<no text captured>}"
      log "iteration $ITER: interactive-prompt signature detected; paged owner (stuck-on-question)"
      stuck_notified=1
    fi
    sleep "$STUCK_POLL_SECS"
  done
  wait "$wpid"
  rc=$?
  set -e
  t1=$(date +%s)
  local duration=$((t1 - t0))

  # A live-but-prompt-blocked worker (detected mid-iteration) that then exits
  # without a trustworthy clean marker is a PARK-shaped stall, not a crash:
  # verdict `stuck-on-question`, owner already paged, NOT counted toward the
  # breaker (issue #2666). Guarded on "no clean marker" so a late false-positive
  # signature can never mask a real finalized/park outcome.
  if [[ -f "$stuck_flag" ]] && { [[ "$rc" -ne 0 ]] || [[ ! -f "$MARKER_FILE" ]]; }; then
    journal_line "$ITER" "stuck-on-question" "" "" "$duration" "$rc" "$(cat "$stuck_flag" 2>/dev/null)"
    log "iteration $ITER stuck-on-question (owner paged; not counted toward breaker)"
    return 0
  fi

  if [[ "$rc" -ne 0 ]] || [[ ! -f "$MARKER_FILE" ]]; then
    journal_line "$ITER" "abnormal" "" "" "$duration" "$rc"
    log "iteration $ITER abnormal (exit=$rc marker_present=$([[ -f "$MARKER_FILE" ]] && echo yes || echo no))"
    trip_breaker_or_continue
    return 0
  fi

  local outcome issue pr reason question
  outcome="$(marker_field outcome)"
  issue="$(marker_field issue)"
  pr="$(marker_field pr)"
  reason="$(marker_field reason)"
  question="$(marker_field question)"

  case "$outcome" in
    finalized)
      if [[ -z "$issue" || -z "$pr" ]]; then
        # F5: contract requires BOTH issue and pr on "finalized" — a marker
        # claiming success with either missing/null is untrustworthy and must
        # not be counted as a done issue nor reset the breaker.
        journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc"
        log "iteration $ITER abnormal (finalized marker missing issue/pr: issue='$issue' pr='$pr')"
        trip_breaker_or_continue
      else
        CONSECUTIVE_ABNORMAL=0
        ISSUES_DONE=$((ISSUES_DONE + 1))
        journal_line "$ITER" "finalized" "$issue" "$pr" "$duration" "$rc"
        notify "info" "worker-supervisor: finalized issue $issue" "pr=$pr duration_s=$duration"
      fi
      ;;
    no-work)
      CONSECUTIVE_ABNORMAL=0
      journal_line "$ITER" "no-work" "$issue" "$pr" "$duration" "$rc"
      log "no work available; backing off ${BACKOFF_NOWORK_SECS}s"
      sleep "$BACKOFF_NOWORK_SECS"
      ;;
    blocked)
      CONSECUTIVE_ABNORMAL=0
      case "$reason" in
        seam1-approval | needs-decision)
          # CLEAN PARK (issue #2666, park-and-resume). The worker hit Seam 1 (an
          # unapproved design spec) or a genuine mid-run owner decision: it posted
          # ONE structured question comment, added the `needs-decision` label, and
          # released the machine. Judged NORMAL — never abnormal, never toward the
          # breaker — and it does NOT head-block the queue: the labeled issue is
          # excluded from the worker's next pickup until the owner answers and the
          # label clears, so the loop moves straight to the next Ready issue. Fire
          # ONE high-priority page whose title carries the issue # and the first
          # line of the question (the marker's optional "question" field).
          journal_line "$ITER" "parked-on-owner" "$issue" "$pr" "$duration" "$rc" "$reason"
          local qline="${question:-$reason}"
          notify "high" "worker-supervisor: parked issue $issue — ${qline}" \
            "issue #$issue parked awaiting owner (${reason}). Answer the needs-decision question comment on the issue; the worker resumes on a newer owner reply."
          log "issue $issue parked-on-owner ($reason); moving to next Ready issue"
          ;;
        *)
          journal_line "$ITER" "blocked" "$issue" "$pr" "$duration" "$rc" "$reason"
          if [[ -n "$issue" && "$issue" == "$LAST_BLOCKED_ISSUE" ]]; then
            # F2: the SAME issue blocked on two consecutive iterations means the
            # queue is head-blocked — looping would just reset the breaker every
            # time and burn wall-clock budget until MAX_HOURS with no progress.
            # Stop cleanly and page the owner instead.
            notify "high" "worker-supervisor: issue $issue persistently blocked" "issue #$issue persistently blocked — queue is head-blocked, needs owner"
            log "issue $issue blocked on two consecutive iterations; queue is head-blocked, stopping"
            finalize_exit "head-blocked" 0
          fi
          LAST_BLOCKED_ISSUE="$issue"
          notify "info" "worker-supervisor: blocked on issue $issue" "${reason:-no reason given}"
          log "remembered blocked issue $LAST_BLOCKED_ISSUE (not auto-retried this run)"
          ;;
      esac
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

# Run the loop only when executed directly; when sourced (e.g. by the tooling
# tests to exercise detect_prompt_signature/captured_question in isolation) the
# functions are defined but the machine-guarding loop never starts.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi

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
#               breaker, and normally does NOT head-block the queue (the labeled
#               issue is excluded from the worker's next pickup until the owner
#               answers and the label clears), fires ONE high-priority page, and
#               moves on to the next Ready issue. Safety valve (mirrors the
#               blocked-path F2 guard): if the SAME issue parks on two
#               consecutive iterations — the label evidently never applied, so
#               the pickup exclusion is not holding — the supervisor pages the
#               owner (head-blocked-on-decision) and STOPS cleanly rather than
#               re-asking one question until MAX_ISSUES.
#
# stuck-on-question (mid-iteration, no marker) — a worker WEDGED on an
#               interactive prompt (AskUserQuestion / permission prompt / menu)
#               in an unattended session never writes a marker; it just burns
#               MAX_ITER_SECS and would look "abnormal". A watchdog classifies
#               this on POSITIVE WEDGE EVIDENCE, not a bare substring match (the
#               Claude CLI routinely prints tool names like `AskUserQuestion` in
#               normal trace, so a whole-log substring match would misclassify
#               ordinary crashes as stuck and permanently defeat the breaker —
#               roborev 1773). Every STUCK_POLL_SECS the watchdog scans, and
#               declares `stuck-on-question` ONLY when ALL hold across TWO
#               consecutive scans: (a) the process is alive, (b) a prompt
#               signature is in the LAST ~20 log lines (tail, not whole file),
#               and (c) the log has not grown between the scans (a wedged prompt
#               emits nothing). It then pages the owner and records the verdict
#               when the worker later exits without a clean marker. NOT abnormal,
#               never toward BREAKER_N. A marker-less exit whose only signature is
#               a stray scrollback match (fails tail-locality or no-growth) stays
#               ABNORMAL and counts toward the breaker.
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

# ---------------------------------------------------------------------------
# Supervisor-authored git-ref claim (issue #2655 / #2499 design).
#
# The supervisor — a long-lived, machine-scoped process — stamps a
# refs/machine-claims/<machine> liveness ref (issue+PID+ts) at every worker spawn and
# clears it when it exits cleanly, so claim liveness is MECHANISM-driven and no
# longer depends on the worker LLM remembering to `beat`. The PID recorded is
# the SUPERVISOR's own ($$): it is the stable per-machine anchor that outlives
# each recycled worker, so a same-machine reaper's process-liveness check tracks
# "is this machine's supervisor still running" rather than a transient worker
# subprocess. When the supervisor dies, nothing refreshes the ref and it goes
# stale within the reap threshold.
#
# CLAIM_CMD is the claim-heartbeat.sh entrypoint; overridable so the tests can
# substitute a hermetic stub (no origin/network). Set CLAIM_CMD="" to disable
# claim stamping entirely (e.g. a machine with no origin push rights).
CLAIM_CMD="${CLAIM_CMD:-bash $REPO_ROOT/scripts/flow/claim-heartbeat.sh}"
# The machine identity the claim ref is scoped to — must match what the reaper
# clears. Defaults to claim-heartbeat.sh's own default (`hostname -s`), honoring
# HEARTBEAT_MACHINE when the fleet overrides it.
CLAIM_MACHINE="${HEARTBEAT_MACHINE:-$(hostname -s 2>/dev/null || echo unknown)}"
# Best-known issue for the CURRENT iteration's spawn stamp. Empty/unknown on the
# first iteration (the worker picks from the board); learned from each marker and
# reused as the next spawn's stamp. "0" is stamped when still unknown — the
# reaper treats a non-issue as "no open-PR guard applicable" and governs on age.
CLAIM_ISSUE="${CLAIM_ISSUE:-}"

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

# stamp_claim <issue>: refresh refs/machine-claims/<machine> with issue+PID+ts (issue
# #2655). PID stamped is the SUPERVISOR's ($$) — the stable per-machine anchor,
# not a transient worker subprocess. A non-numeric/empty issue is stamped as "0"
# (unknown): the reaper then governs purely on age + open-PR (a "0" issue can
# have no open PR). Non-fatal on failure (a claim-push hiccup must never crash
# the supervisor) — logs a WARN and continues; the ref simply won't refresh this
# iteration and ages toward the reap threshold, which is fail-safe.
stamp_claim() {
  local issue="$1"
  [[ -n "$CLAIM_CMD" ]] || return 0
  case "$issue" in '' | *[!0-9]*) issue=0 ;; esac
  if HEARTBEAT_MACHINE="$CLAIM_MACHINE" $CLAIM_CMD stamp "$issue" "$$" >/dev/null 2>&1; then
    log "claim stamped: machine=$CLAIM_MACHINE issue=$issue pid=$$"
  else
    log "WARN: claim stamp failed (machine=$CLAIM_MACHINE issue=$issue) — ref not refreshed this iteration (non-fatal)"
  fi
}

# clear_claim: delete refs/machine-claims/<machine> on a CLEAN supervisor exit (issue
# #2655). `reap` refuses to delete a ref whose issue still has an open PR, so a
# supervisor that stops with an unfinished endgame leaves the claim in place for
# adoption rather than orphaning it. Non-fatal on failure.
clear_claim() {
  [[ -n "$CLAIM_CMD" ]] || return 0
  if HEARTBEAT_MACHINE="$CLAIM_MACHINE" $CLAIM_CMD reap "$CLAIM_MACHINE" >/dev/null 2>&1; then
    log "claim cleared on clean exit: machine=$CLAIM_MACHINE"
  else
    log "WARN: claim clear declined/failed for machine=$CLAIM_MACHINE (open PR or push error) — left for adoption (non-fatal)"
  fi
}

# detect_prompt_signature <logfile>: true (exit 0) when the LAST ~20 lines of the
# worker's live log show an interactive-prompt signature — a wedged
# AskUserQuestion, a permission prompt, a `❯` menu block, or a "waiting for input"
# line (issue #2666). Deliberately a TAIL scan, not a whole-log scan: the Claude
# CLI routinely prints tool names like `AskUserQuestion` in its normal trace, so
# a bare whole-file substring match would misclassify ordinary crashes as stuck
# and permanently defeat the breaker (roborev 1773). A stray match in old
# scrollback is not evidence of a live wedge; only a signature still resident in
# the last frames (paired with no-growth — see the supervise loop) is.
STUCK_TAIL_LINES="${STUCK_TAIL_LINES:-20}"
detect_prompt_signature() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 1
  tail -n "$STUCK_TAIL_LINES" "$logfile" 2>/dev/null | grep -qE "$PROMPT_SIGNATURE_RE"
}

# captured_question <logfile>: the matching prompt line(s) from the same tail
# window, collapsed to a single ≤300-char line for the ntfy body. Empty when
# nothing matched.
captured_question() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 0
  tail -n "$STUCK_TAIL_LINES" "$logfile" 2>/dev/null | grep -E "$PROMPT_SIGNATURE_RE" 2>/dev/null \
    | head -n 3 | tr '\n' ' ' | cut -c1-300
}

# log_size <logfile>: byte size of the file, or 0 when absent. A wedged
# interactive prompt produces NO further output, so a frozen byte size across two
# consecutive scans is the positive evidence that distinguishes a genuine wedge
# from a busy worker that merely printed a tool name and kept writing.
log_size() {
  local f="$1"
  [[ -f "$f" ]] || { printf '0'; return 0; }
  wc -c <"$f" 2>/dev/null | tr -d ' '
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
LAST_PARKED_ISSUE=""
START_TS=$(date +%s)
MAX_HOURS_SECS=$((MAX_HOURS * 3600))

finalize_exit() {
  local reason="$1" code="$2"
  # Release this machine's claim ref on a clean stop (issue #2655). `reap`
  # refuses when the claim's issue still has an open PR, so an unfinished endgame
  # is preserved for adoption rather than orphaned.
  clear_claim
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
  # Stamp/refresh the machine's claim ref BEFORE spawning (issue #2655): the
  # supervisor authors liveness mechanically, so a beat-then-crash worker can no
  # longer look alive for the whole threshold window. Uses the best-known issue
  # from the previous iteration's marker (empty/unknown -> "0").
  stamp_claim "$CLAIM_ISSUE"
  local logfile="$LOG_DIR/iter-${ITER}.log"
  local stuck_flag="$LOG_DIR/.iter-${ITER}.stuck"
  rm -f "$stuck_flag"
  local t0 t1 rc=0
  t0=$(date +%s)
  # Spawn the worker in the background and supervise it with a split-cadence poll
  # loop (issue #2666, hardened per roborev 1773):
  #   * exit/deadline check every 1s — near-blocking completion latency and
  #     MAX_ITER_SECS enforced at 1s granularity (portably — no coreutils
  #     `timeout` on macOS).
  #   * WEDGE scan every STUCK_POLL_SECS — classify `stuck-on-question` ONLY on
  #     positive wedge evidence that holds across TWO consecutive scans: (a) the
  #     process is still alive, (b) a prompt signature is in the LAST ~20 log
  #     lines (tail scan, not whole-log), AND (c) the log has NOT GROWN between
  #     the two scans (byte size unchanged). A wedged prompt emits no further
  #     output, so its tail + size freeze; a busy worker that merely printed the
  #     tool name in its trace keeps writing (size grows) and/or scrolls the match
  #     out of the tail — either fails the test and is ignored. Tradeoff: a
  #     genuine wedge takes up to 2*STUCK_POLL_SECS to confirm (vs an instant but
  #     false-positive-prone substring match), and a marker-less abnormal exit
  #     with a stray scrollback match stays ABNORMAL (counts toward the breaker) —
  #     exactly the guarantee the substring approach would have defeated.
  set +e
  bash -c "$WORKER_CMD" >"$logfile" 2>&1 &
  local wpid=$!
  local deadline=$((t0 + MAX_ITER_SECS))
  local stuck_notified=0 g now
  local last_scan_ts=$t0 prev_size=-1 prev_sig=0 cur_size cur_sig
  while kill -0 "$wpid" 2>/dev/null; do
    now=$(date +%s)
    if [[ "$now" -ge "$deadline" ]]; then
      log "iteration $ITER exceeded MAX_ITER_SECS=${MAX_ITER_SECS}s; terminating worker"
      kill -TERM "$wpid" 2>/dev/null
      g=0
      while kill -0 "$wpid" 2>/dev/null && [[ "$g" -lt 5 ]]; do sleep 1; g=$((g + 1)); done
      kill -KILL "$wpid" 2>/dev/null
      break
    fi
    if [[ "$stuck_notified" -eq 0 ]] && [[ $((now - last_scan_ts)) -ge "$STUCK_POLL_SECS" ]]; then
      last_scan_ts="$now"
      cur_size="$(log_size "$logfile")"
      if detect_prompt_signature "$logfile"; then cur_sig=1; else cur_sig=0; fi
      # Wedge confirmed only when the signature is present in the tail at BOTH
      # this scan and the prior one AND the log did not grow between them (and
      # the process is still alive — the loop condition). prev_size<0 = no prior
      # scan yet, so the very first scan can never confirm.
      if [[ "$prev_sig" -eq 1 && "$cur_sig" -eq 1 && "$prev_size" -ge 0 && "$cur_size" -eq "$prev_size" ]]; then
        local qtext
        qtext="$(captured_question "$logfile")"
        printf '%s' "$qtext" >"$stuck_flag"
        notify "high" "worker-supervisor: stuck-on-question (iter $ITER)" \
          "worker appears wedged on an interactive prompt (frozen log + signature across 2 polls): ${qtext:-<no text captured>}"
        log "iteration $ITER: wedge confirmed (frozen log + tail signature x2 polls); paged owner (stuck-on-question)"
        stuck_notified=1
      fi
      prev_size="$cur_size"
      prev_sig="$cur_sig"
    fi
    sleep 1
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
    # NEUTRAL, not transparent: like every other non-abnormal verdict this
    # resets the consecutive-abnormal counter, so a real prior crash chain is
    # BROKEN by a stuck iteration rather than silently continuing across it
    # (roborev 1769: `abnormal → stuck → abnormal → abnormal` must not trip
    # BREAKER_N=3). The owner has already been paged.
    CONSECUTIVE_ABNORMAL=0
    journal_line "$ITER" "stuck-on-question" "" "" "$duration" "$rc" "$(cat "$stuck_flag" 2>/dev/null)"
    log "iteration $ITER stuck-on-question (owner paged; breaker chain reset, not counted)"
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

  # Learn the issue this iteration worked so the NEXT spawn's claim stamp names
  # it (issue #2655). A recycled worker that resumes the same machine's claim
  # branch stays on the same issue, so carrying it forward keeps the claim ref's
  # open-PR reap guard accurate. A finalized issue is released (its endgame is
  # done); anything else keeps the issue for the next stamp.
  case "$outcome" in
    finalized) CLAIM_ISSUE="" ;;
    *) [[ -n "$issue" ]] && CLAIM_ISSUE="$issue" ;;
  esac

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
          if [[ -n "$issue" && "$issue" == "$LAST_PARKED_ISSUE" ]]; then
            # Head-block-on-decision guard (mirrors the F2 blocked-path guard,
            # roborev 1769): the SAME issue parked on two consecutive iterations
            # means the worker keeps re-parking it — typically because the
            # `needs-decision` label never applied, so the pickup exclusion is not
            # holding and the loop would burn to MAX_ISSUES re-asking one question.
            # Page the owner and STOP cleanly instead of looping.
            notify "high" "worker-supervisor: issue $issue head-blocked on decision" "issue #$issue parked twice in a row (needs-decision) — queue head-blocked on an owner decision, needs owner"
            log "issue $issue parked on two consecutive iterations; head-blocked on decision, stopping"
            finalize_exit "head-blocked-decision" 0
          fi
          LAST_PARKED_ISSUE="$issue"
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

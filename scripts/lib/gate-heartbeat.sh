#!/usr/bin/env bash
# gate-heartbeat.sh — the liveness beater for scripts/agent-gate.sh (issue #3473).
#
# WHY THIS EXISTS
# ---------------
# The gate's startup sentinel writes `RESULT: INCOMPLETE (gate did not finish)`
# into the summary file at launch and only overwrites it at the terminal emit
# (#3041). That single placeholder is therefore the artifact of THREE different
# states — queued, running, and killed — so a lane whose gate was reaped reads a
# summary textually IDENTICAL to a lane whose gate is 30 minutes from finishing.
# #3473 measured the cost: background work launched from inside a lane session is
# terminated at a hard ceiling, and the only way anyone could tell a reaped gate
# from a live one was a human running `ps` on the box.
#
# `INCOMPLETE` provably cannot carry that distinction: it is written ONCE, so its
# mere presence says nothing about when the writer last drew breath. Liveness needs
# a signal that DECAYS. This beater is that signal — it rewrites a heartbeat file
# every $interval seconds for as long as the gate process is alive, so a reader can
# distinguish "recently beaten" (running) from "stale" (reaped) with no access to
# the box's process table.
#
# WHY A SEPARATE PROCESS, not a `( … ) &` subshell of the gate:
#   - a backgrounded subshell inherits the gate's EXIT trap (the #1825 slot-release
#     trap is already guarded against exactly this hazard with a BASHPID check), and
#   - its std fds would be a copy of the gate's stdout pipe, which is what #1175
#     forbids for any long-lived background child (a leaked descriptor truncates a
#     streamed SUMMARY under an until-EOF reader). The gate launches us with fds
#     detached to /dev/null for that reason, same as gate_slot_daemon.py.
#
# WHY IT VERIFIES THE GATE PID EVERY BEAT
# ---------------------------------------
# If the gate is SIGKILLed (or its cgroup/process group is torn down) this process
# may briefly outlive it. A beater that kept beating after its gate died would
# report a dead gate as RUNNING FOREVER — the precise false-negative #3473 is about,
# reintroduced one level down. So every beat is preceded by an AFFIRMATIVE check that
# the gate pid is still the same process, and a failed check ends this process
# WITHOUT writing (leaving the last good beat to go stale, which is the truth).
#
# Reuse-proofing: a bare `kill -0` answers "some process holds this pid", which a
# recycled pid also satisfies. Where /proc is available we additionally pin field 22
# of /proc/<pid>/stat (starttime, in clock ticks since boot — immutable for the life
# of a process), so a recycled pid reads as DEAD rather than alive. Where it is not,
# we fall back to `kill -0` and SAY SO in the artifact (`parent-check: kill0`) rather
# than let the reader assume a guarantee it is not getting.
#
# Usage:
#   gate-heartbeat.sh --file <path> --run-id <id> --gate-pid <pid>
#                     [--mode <full|lite|delta>] [--interval <secs>] [--logs <dir>]
set -uo pipefail

FILE=""; RUN_ID=""; GATE_PID=""; MODE=""; INTERVAL=20; LOGS=""; LAUNCH_NONCE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --file)     FILE="${2:?--file needs a path}"; shift 2 ;;
    --run-id)   RUN_ID="${2:?--run-id needs a value}"; shift 2 ;;
    --gate-pid) GATE_PID="${2:?--gate-pid needs a pid}"; shift 2 ;;
    --mode)     MODE="${2:?--mode needs a value}"; shift 2 ;;
    --interval) INTERVAL="${2:?--interval needs seconds}"; shift 2 ;;
    --logs)     LOGS="${2:?--logs needs a dir}"; shift 2 ;;
    # An opaque token from a LAUNCHER, echoed into every beat so that launcher can prove this
    # beat is its run's and not a concurrent peer's on the same path (#3473). Never interpreted.
    --launch-nonce) LAUNCH_NONCE="${2:?--launch-nonce needs a value}"; shift 2 ;;
    *) echo "gate-heartbeat: unknown argument '$1'" >&2; exit 64 ;;
  esac
done
# bash 3.2-compatible required-argument check (no ${var,,}: this script must run
# on the same shell floor as agent-gate.sh, which still guards for bash 3.2).
[ -n "$FILE" ]     || { echo "gate-heartbeat: --file is required" >&2; exit 64; }
[ -n "$RUN_ID" ]   || { echo "gate-heartbeat: --run-id is required" >&2; exit 64; }
[ -n "$GATE_PID" ] || { echo "gate-heartbeat: --gate-pid is required" >&2; exit 64; }
case "$GATE_PID" in ''|*[!0-9]*) echo "gate-heartbeat: --gate-pid must be numeric (got '$GATE_PID')" >&2; exit 64 ;; esac
case "$INTERVAL" in ''|*[!0-9]*) echo "gate-heartbeat: --interval must be numeric (got '$INTERVAL')" >&2; exit 64 ;; esac
[ "$INTERVAL" -ge 1 ] || { echo "gate-heartbeat: --interval must be >= 1" >&2; exit 64; }

# _starttime <pid> -> field 22 of /proc/<pid>/stat, or empty when unobtainable.
# Field 2 (comm) may contain spaces AND parentheses, so the fields are counted from
# after the LAST ')' — the standard way to parse this file safely.
# TIERED identity (roborev job 185). /proc does not exist on macOS/BSD, and falling back to a
# bare `kill -0` there meant the beater could not tell its gate from a RECYCLED pid — so after a
# reap it would keep publishing for a stranger and a reader would report RUNNING for a gate that
# is gone. `ps -o lstart=` is portable, stable, and empty for an absent pid; its one-second
# granularity is immaterial for reuse detection, which requires cycling the whole pid space.
_starttime() {
  local pid="$1" raw rest ls
  raw=$(cat "/proc/$pid/stat" 2>/dev/null)
  if [ -n "$raw" ]; then
    rest="${raw##*) }"
    # rest begins at field 3 (state); starttime is field 22 => the 20th of rest.
    # shellcheck disable=SC2086  # deliberate word-split into positional params
    set -- $rest
    if [ $# -ge 20 ]; then printf 'proc:%s' "${20}"; return 0; fi
  fi
  ls=$(ps -o lstart= -p "$pid" 2>/dev/null | tr -s ' ')
  if [ -n "$ls" ]; then printf 'ps:%s' "$ls"; return 0; fi
  return 1
}

GATE_STARTTIME=$(_starttime "$GATE_PID") || GATE_STARTTIME=""
case "$GATE_STARTTIME" in
  proc:*) PARENT_CHECK=starttime ;;   # tick granularity, fully reuse-proof
  ps:*)   PARENT_CHECK=lstart ;;      # second granularity, reuse-proof in practice
  *)      PARENT_CHECK=kill0 ;;       # NEITHER available: existence only, not identity
esac

# Published purely so a HUMAN reading a heartbeat knows which box wrote it. No verdict
# depends on it: a reader cannot inspect a pid across machines, and rather than model that,
# #3473 descoped the death claim entirely (see gate-liveness.sh).
# OMIT the field rather than publish a placeholder (roborev job 221). `|| echo unknown` made a FAILED
# lookup indistinguishable from a real hostname, and because the reader used the same fallback, two
# failures compared EQUAL and were accepted as proof of a shared clock domain. A reader already treats
# an ABSENT host as unproven, so absence is the honest encoding of "could not determine".
HOST_NAME=$(uname -n 2>/dev/null || true)
case "$HOST_NAME" in unknown) HOST_NAME="" ;; esac

# _gate_alive: AFFIRMATIVE liveness. Returns 0 only on a positive answer.
# The identity COMPARISON must cover every tier that HAS an identity (roborev job 188). The
# previous version branched on `starttime` alone, so the `lstart` tier added for macOS was
# LABELLED in the beat and then never actually compared — it fell through to a bare `kill -0`,
# which a recycled pid satisfies. The beat would still advertise `parent-check: lstart`, so a
# reader would trust it and report a dead gate as RUNNING. Adding a tier without wiring its
# comparison is worse than not adding it: it buys the appearance of a guarantee.
#
# Only `kill0` — no identity available at all — may fall back to bare existence, and the reader
# already refuses to grant an epoch-based RUNNING from such a beat.
# A ZOMBIE gate is GONE. Its pid entry — and therefore its /proc start time — survive until its
# parent reaps it, so identity comparison alone says "still ours" about a process that has already
# exited. After a SIGKILL under a stopped or non-reaping parent the beater would publish forever and
# the reader would report a dead gate as RUNNING: a false liveness claim, which is the one direction
# this whole mechanism must never fail in (roborev job 203).
#
# AFFIRMATIVE reading only: 0 solely on a confirmed zombie. Unmeasurable => 1 (not a zombie), so an
# unreadable state never fabricates a death.
_proc_is_zombie() {  # <pid> -> 0 = provably a zombie, 1 = not, or unmeasurable
  local pid=$1 _st _state
  if _st=$(cat "/proc/$pid/stat" 2>/dev/null) && [ -n "$_st" ]; then
    # `comm` is parenthesised and may contain ')' and spaces: read state after the LAST ')'.
    _state=${_st##*)}
    set -- $_state
    [ "${1:-}" = "Z" ] && return 0
    return 1
  fi
  if _state=$(ps -o state= -p "$pid" 2>/dev/null) && [ -n "$_state" ]; then
    case "$_state" in Z*) return 0 ;; *) return 1 ;; esac
  fi
  return 1
}

_gate_alive() {
  # Checked on EVERY tier, before identity: a zombie's identity still matches, so identity alone
  # cannot see this. The kill0 tier needs it most — `kill -0` succeeds on a zombie outright.
  _proc_is_zombie "$GATE_PID" && return 1
  case "$PARENT_CHECK" in
    starttime|lstart)
      local now
      now=$(_starttime "$GATE_PID") || return 1
      [ -n "$now" ] || return 1
      [ "$now" = "$GATE_STARTTIME" ] || return 1   # pid recycled => the gate is gone
      return 0 ;;
  esac
  kill -0 "$GATE_PID" 2>/dev/null
}

# Write the block to a sibling temp then rename, so a reader never sees a partial
# heartbeat (rename within a directory is atomic). A failed write is not fatal — the
# beat simply goes stale, which a reader reports as STALLED/UNKNOWN, never as RUNNING.
# TMP_PATH is module-scope so the signal handler can remove whatever mktemp actually chose.
TMP_PATH=""
_beat() {
  local seq="$1" epoch tmp
  epoch=$(date +%s)
  # SECURE, EXCLUSIVE creation (roborev job 162, Medium). This used to be the PREDICTABLE
  # `$FILE.tmp.$$`, opened with `>` (O_TRUNC, follows symlinks). In a shared writable
  # directory another local user can pre-create that exact path as a symlink and have EVERY
  # beat — once every 20s, for the whole run — truncate an arbitrary file writable by the
  # gate user. `mktemp` creates with O_EXCL|O_CREAT and mode 0600, so it cannot follow a
  # planted symlink and cannot collide.
  # `mv -f "$tmp" "$FILE"` treats a DIRECTORY — or a symlink to one — as a destination DIRECTORY, so
  # it would SUCCEED while depositing a new random temp file inside it every interval (roborev job
  # 213). Liveness stays unreadable, every poll answers UNKNOWN, and the accumulating files can fail
  # the gate's own tree-integrity check. Checked before EVERY publish, not once at startup, because a
  # directory can appear at that path mid-run.
  if [ -d "$FILE" ]; then
    echo "gate-heartbeat: the heartbeat destination '$FILE' is a directory (or a symlink to one)," >&2
    echo "                so publishing by rename would drop a temp file INSIDE it every beat and" >&2
    echo "                liveness would never be readable. Refusing to publish (#3473)." >&2
    return 1
  fi
  tmp=$(mktemp "$FILE.tmp.XXXXXX" 2>/dev/null) || return 1
  TMP_PATH="$tmp"
  {
    echo "==== AGENT-GATE HEARTBEAT ===="
    echo "run-id: $RUN_ID"
    [ -n "$MODE" ] && echo "mode: $MODE"
    echo "gate-pid: $GATE_PID"
    # `host` IS AN INPUT TO A VERDICT, and the comment here previously said the opposite — which is
    # exactly the belief that made `|| echo unknown` look harmless (roborev job 221). The reader uses
    # this field to decide whether it shares a CLOCK DOMAIN with the beater, and that decision gates
    # whether freshness may be judged from `beat-epoch` at all. Publish a real hostname or NOTHING;
    # never a placeholder, which a reader with the same failed lookup would match against.
    # `gate-starttime` and `boot-id` were published so a reader could infer the gate's
    # DEATH from them; that inference is descoped (see gate-liveness.sh), so they are no
    # longer published — an unused field is surface that invites the inference back.
    # OMITTED entirely when undeterminable, so "absent" and "unverified" are the SAME state to a
    # reader rather than two spellings of it (roborev job 221).
    [ -n "$HOST_NAME" ] && echo "host: $HOST_NAME"
    [ -n "$LAUNCH_NONCE" ] && echo "launch-nonce: $LAUNCH_NONCE"
    echo "beater-pid: $$"
    echo "parent-check: $PARENT_CHECK"
    echo "interval: $INTERVAL"
    echo "beat-seq: $seq"
    echo "beat-epoch: $epoch"
    echo "beat-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    [ -n "$LOGS" ] && echo "logs: $LOGS"
    echo "==== END AGENT-GATE HEARTBEAT ===="
  } > "$tmp" 2>/dev/null || { rm -f "$tmp" 2>/dev/null; TMP_PATH=""; return 1; }
  mv -f "$tmp" "$FILE" 2>/dev/null || { rm -f "$tmp" 2>/dev/null; TMP_PATH=""; return 1; }
  TMP_PATH=""
  return 0
}

# Exit quietly on a signal WITHOUT beating: a terminating beater must never leave a
# beat newer than its own death, or the reader would date the gate's liveness to the
# moment the beater was killed. Also drop the in-flight temp file, so a beater killed
# mid-write leaves no `<file>.tmp.<pid>` litter beside the caller's summary.
SLEEP_PID=""
# shellcheck disable=SC2317  # reached only via the trap below
_shutdown() {
  [ -n "$SLEEP_PID" ] && kill "$SLEEP_PID" 2>/dev/null
  # Remove the temp mktemp ACTUALLY chose, not a name we guessed: the old handler deleted
  # `$FILE.tmp.$$`, which after the mktemp change would leave the real temp behind.
  [ -n "$TMP_PATH" ] && rm -f "$TMP_PATH" 2>/dev/null
  exit 0
}
trap _shutdown HUP TERM INT QUIT PIPE

seq=0
while :; do
  _gate_alive || { [ -n "$TMP_PATH" ] && rm -f "$TMP_PATH" 2>/dev/null; exit 0; }
  seq=$((seq + 1))
  _beat "$seq"
  # `sleep &` + `wait`, NOT a foreground `sleep`. bash does not run a trap handler
  # while a FOREGROUND child is running — it defers until the child reaps — so a
  # foreground `sleep $INTERVAL` made this process ignore the gate's SIGTERM for up to
  # a full interval, and a beater still alive after its gate exited is exactly the
  # "orphaned beater" this script's pid check exists to prevent (measured: the #3473
  # end-to-end case caught a 20s straggler). `wait` IS interruptible by a trap, so the
  # handler runs immediately and kills the pending sleep on its way out.
  sleep "$INTERVAL" &
  SLEEP_PID=$!
  wait "$SLEEP_PID" 2>/dev/null
  SLEEP_PID=""
done

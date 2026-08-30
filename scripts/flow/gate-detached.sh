#!/usr/bin/env bash
# gate-detached.sh — run scripts/agent-gate.sh in its OWN cgroup, so a lane session's
# teardown cannot kill it (issue #3473 AC3).
#
# THE PROBLEM, MEASURED
# ---------------------
# Every process a lane session spawns — including one launched with `nohup`, `setsid`,
# closed fds and a ppid of 1 — inherits the lane pane's systemd scope
# (`tmux-spawn-<uuid>.scope`). That scope carries the systemd defaults
# `KillMode=control-group`, `SendSIGKILL=yes`, `TimeoutStopUSec=90s`. When it is torn
# down, systemd signals EVERY task in the cgroup.
#
# Cgroup membership is inherited across fork and CANNOT be shed by detaching from the
# controlling terminal, the process group or the session. That is why `nohup`/`setsid`
# do not help, and why the symptom was indiscriminate: #3473 recorded a passive `sleep`
# loop dying alongside the gate, which correctly ruled out CPU, memory, disk and the
# #1825 slot cap — a cgroup kill does not care what the work is.
#
# It also explains the control the issue reported: the coordination lead's gate,
# launched over `ssh` + `nohup`, completed on the same box, same sha, same slot cap. An
# ssh login gets its own `session-N.scope`, a DIFFERENT cgroup, so a lane pane's
# teardown never reaches it. The variable was never the work — it was the cgroup.
#
# THE FIX
# -------
# `systemd-run --user` starts the gate as a transient unit under `app.slice`, i.e. in a
# cgroup of its own, parented by the systemd user manager rather than by this session.
# The lane can then exit, crash, be recycled or have its pane killed, and the gate runs
# to its verdict. Measured on this fleet: two identical tickers, one inheriting a
# `KillMode=control-group` cgroup (with setsid+nohup+closed fds) and one in its own
# scope; on teardown the first died leaving NO trace, the second was untouched.
#
# WHAT THIS DOES NOT DO
# ---------------------
# It does not make the gate faster, and it does not remove the #1825 slot cap — a
# detached gate still queues. It also does not let you walk away without a plan for
# reading the verdict: use scripts/gate-liveness.sh, which distinguishes RUNNING from
# REAPED (this script prints the exact command).
#
# Usage:
#   bash scripts/flow/gate-detached.sh [--summary <path>] [--log <path>] [--] [gate args...]
#
# Prints the unit name, summary path, heartbeat path and the poll command, then exits
# 0 immediately — the gate keeps running. Any remaining arguments go to agent-gate.sh.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SUMMARY=""; LOGFILE=""
GATE_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --summary) SUMMARY="${2:?--summary needs a path}"; shift 2 ;;
    --log)     LOGFILE="${2:?--log needs a path}"; shift 2 ;;
    -h|--help) sed -n '2,45p' "$0"; exit 0 ;;
    --)        shift; GATE_ARGS+=("$@"); break ;;
    *)         GATE_ARGS+=("$1"); shift ;;
  esac
done

if ! command -v systemd-run >/dev/null 2>&1; then
  # NAMED refusal, never a silent fallback to an in-session launch. A caller who asked
  # for a detached gate and got a session-scoped one would believe it was protected —
  # the false-assurance direction. Say what is missing and what the consequence is.
  echo "gate-detached: systemd-run is not available on this host." >&2
  echo "gate-detached: this host cannot run a cgroup-detached gate, so a gate started" >&2
  echo "               from this session WILL die with the session (#3473). Run the gate" >&2
  echo "               from a separate login (ssh + nohup), which gets its own scope." >&2
  exit 69   # EX_UNAVAILABLE — the capability is absent, the request was well-formed
fi
if ! systemd-run --user --scope --quiet true >/dev/null 2>&1; then
  echo "gate-detached: 'systemd-run --user' does not work here (no user systemd manager?)." >&2
  echo "gate-detached: refusing to launch a gate that would inherit this session's cgroup" >&2
  echo "               and die with it (#3473). Use ssh + nohup from a separate login." >&2
  exit 69
fi

RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"
UNIT="cqlite-gate-$RUN_TAG"
# DEFAULT artifact paths go in a PRIVATE mkdtemp directory, never a predictable name in
# shared /tmp (roborev job 157, Medium). A name derived from the timestamp and pid is
# guessable, and this script TRUNCATES the log with `>` — so on a multi-user box another
# local user could pre-create a symlink at the predicted path and have us clobber any file
# the gate user can write. `mktemp -d` yields an unguessable directory created with 0700 by
# the C library, which closes both the prediction and the symlink step.
#
# A caller-SUPPLIED path is used as given: that is their explicit choice of location, and
# silently relocating it would break the contract that they know the path in advance.
if [ -z "$SUMMARY" ] || [ -z "$LOGFILE" ]; then
  PRIVDIR=$(mktemp -d "${TMPDIR:-/tmp}/cqlite-gate-XXXXXX") || {
    echo "gate-detached: cannot create a private directory for the default artifacts" >&2
    exit 1
  }
  [ -n "$SUMMARY" ] || SUMMARY="$PRIVDIR/summary.txt"
  [ -n "$LOGFILE" ] || LOGFILE="$PRIVDIR/gate.log"
fi
case "$SUMMARY" in /*) ;; *) SUMMARY="$PWD/$SUMMARY" ;; esac
case "$LOGFILE" in /*) ;; *) LOGFILE="$PWD/$LOGFILE" ;; esac

# ---------------------------------------------------------------------------
# Environment forwarding. THIS IS NOT OPTIONAL PLUMBING.
#
# A transient unit does NOT inherit the caller's environment — it starts from the
# systemd user manager's. Silently dropping the caller's environment would hand the
# gate a different PATH (no cargo/rustup), no CQLITE_DATASETS_ROOT and no sccache
# wiring, and the gate would then fail or SKIP for reasons that have nothing to do with
# the branch under test. Worse, it could look like a real red.
#
# So we forward EVERY exported variable, minus a deny-list, rather than an allowlist of
# the ones someone remembered. An allowlist here fails silently and asymmetrically: a
# new gate-relevant variable is simply absent, and nothing says so. The deny-list holds
# only variables that MUST be re-derived by the new unit (systemd's own invocation
# identity) or that name this session's own bookkeeping.
#
# Two shapes are refused rather than mangled, and both are REPORTED, not swallowed: a
# name that is not a portable shell identifier, and a value containing a newline
# (systemd-run's --setenv is a single line). A dropped variable is printed by name so a
# gate that then behaves oddly has a paper trail.
# ---------------------------------------------------------------------------
SETENV_ARGS=()
FORWARDED=0
SKIPPED=""
while IFS= read -r -d '' kv; do
  name="${kv%%=*}"
  value="${kv#*=}"
  case "$name" in
    # systemd re-derives these for the new unit; carrying ours over is wrong.
    INVOCATION_ID|JOURNAL_STREAM|MAINPID|LISTEN_PID|LISTEN_FDS|NOTIFY_SOCKET) continue ;;
    # session bookkeeping that must not follow the gate.
    _|SHLVL|OLDPWD|PWD) continue ;;
    # The gate's summary path is set explicitly below; an inherited one would be
    # de-exported by the gate anyway (#2751) but must not compete with our --setenv.
    AGENT_GATE_SUMMARY_FILE) continue ;;
  esac
  case "$name" in
    ''|*[!A-Za-z0-9_]*|[0-9]*) SKIPPED="${SKIPPED:+$SKIPPED }$name(non-identifier)"; continue ;;
  esac
  case "$value" in
    *$'\n'*) SKIPPED="${SKIPPED:+$SKIPPED }$name(newline-in-value)"; continue ;;
  esac
  SETENV_ARGS+=("--setenv=$name=$value")
  FORWARDED=$((FORWARDED + 1))
done < <(env -0)
SETENV_ARGS+=("--setenv=AGENT_GATE_SUMMARY_FILE=$SUMMARY")

# Pre-create the log so the caller can tail it immediately even before the unit starts.
: > "$LOGFILE" 2>/dev/null || { echo "gate-detached: cannot write log $LOGFILE" >&2; exit 1; }

# VERIFY THE SUMMARY LOCATION BEFORE LAUNCHING (roborev job 160, Medium). Only the log was
# checked, so a bad summary directory produced a running-but-UNMONITORABLE gate: the gate
# cannot publish its verdict, the beater cannot publish liveness, and every poll answers
# UNKNOWN forever. The gate would burn 30-50 minutes and certify nothing — and the caller
# would have no way to tell that from a slow queue.
#
# Both capabilities are probed, because they are DISTINCT permissions and the second is the
# one nobody thinks of: publishing the summary needs write access to the FILE, while the
# heartbeat needs to CREATE and RENAME a sibling temp in the DIRECTORY. A directory that
# permits rewriting an existing summary but not creating new entries (e.g. sticky, or
# write-denied with a pre-existing file) satisfies the first and fails the second.
#
# Probed by DOING it, not by inspecting mode bits: permissions are the resultant of owner,
# group, ACLs, mount flags and SELinux, and `[ -w ]` answers about none of those reliably.
_sumdir=$(dirname -- "$SUMMARY")
if [ ! -d "$_sumdir" ]; then
  echo "gate-detached: summary directory '$_sumdir' does not exist — refusing to launch a gate" >&2
  echo "               that could not publish its verdict (#3473)." >&2
  exit 1
fi
# If something is ALREADY at the summary path, directory permissions are not the whole story
# (roborev job 162, Medium): a pre-existing file the gate cannot rewrite means it can publish
# neither its startup sentinel nor its verdict, even though creating NEW entries in that
# directory works fine. So the existing file is checked too — and by DOING it, since `[ -w ]`
# does not account for ACLs, mount flags or SELinux.
#
# `: >> "$SUMMARY"` opens for APPEND and writes zero bytes: it cannot truncate, cannot alter
# the contents, and does not even update mtime. That matters because under #2874 the path may
# hold a LIVE PEER's block — the check must not disturb it.
if [ -e "$SUMMARY" ] || [ -L "$SUMMARY" ]; then
  if [ -L "$SUMMARY" ] || [ ! -f "$SUMMARY" ]; then
    echo "gate-detached: '$SUMMARY' exists but is not a regular file (symlink, directory, fifo or" >&2
    echo "               device). Refusing: the gate's verdict would go somewhere unintended (#3473)." >&2
    exit 1
  fi
  if ! : >> "$SUMMARY" 2>/dev/null; then
    echo "gate-detached: '$SUMMARY' already exists and is NOT writable, so the gate could not" >&2
    echo "               publish its sentinel or its verdict there — every poll would answer" >&2
    echo "               UNKNOWN. Refusing to launch an unmonitorable gate (#3473)." >&2
    exit 1
  fi
fi
# The probe deliberately NEVER touches $SUMMARY itself. Truncating it to test writability
# would destroy whatever is at that path — and under #2874 that could be a LIVE PEER's
# summary block, i.e. the probe would cause the very data loss the no-clobber contract
# exists to prevent, before the gate's own foreign-run-id detection ever got a chance to
# see it. Directory capability is what both publishers actually need, and it is testable
# without writing to any path the caller owns.
#
# Both probe names sit under the `.heartbeat.tmp.` prefix so they fall inside the gate's
# existing tree-integrity carve-out, in case a concurrent gate captures the tree between
# their creation and removal.
_hbprobe="$SUMMARY.heartbeat.tmp.launchprobe.$$"
_hbprobe2="$SUMMARY.heartbeat.tmp.launchprobe-renamed.$$"
if ! : > "$_hbprobe" 2>/dev/null; then
  rm -f "$_hbprobe" 2>/dev/null || true
  echo "gate-detached: cannot create a file in '$_sumdir', so neither the gate's summary nor" >&2
  echo "               the liveness heartbeat could be published there — every poll of this" >&2
  echo "               gate would answer UNKNOWN. Refusing to launch an unmonitorable gate," >&2
  echo "               rather than burn 30-50 minutes certifying nothing (#3473)." >&2
  exit 1
fi
if ! mv -f "$_hbprobe" "$_hbprobe2" 2>/dev/null; then
  rm -f "$_hbprobe" "$_hbprobe2" 2>/dev/null || true
  echo "gate-detached: cannot RENAME within '$_sumdir', which the heartbeat's atomic publish" >&2
  echo "               requires — every poll of this gate would answer UNKNOWN. Refusing (#3473)." >&2
  exit 1
fi
rm -f "$_hbprobe" "$_hbprobe2" 2>/dev/null || true

# --collect reaps the unit record on exit; the SUMMARY FILE is the verdict artifact, so
# nothing of record is lost with the unit. --same-dir keeps the gate in this worktree
# (it derives its scope from git in $PWD). stdin is closed: a gate must never wait on a
# terminal that is not there.
if ! systemd-run --user --unit="$UNIT" --collect --same-dir --quiet \
     --property=StandardInput=null \
     --property="StandardOutput=append:$LOGFILE" \
     --property="StandardError=append:$LOGFILE" \
     "${SETENV_ARGS[@]}" \
     bash "$REPO_ROOT/scripts/agent-gate.sh" "${GATE_ARGS[@]}"; then
  echo "gate-detached: systemd-run failed to start unit $UNIT (see $LOGFILE)" >&2
  exit 1
fi

CG=$(systemctl --user show "$UNIT" -p ControlGroup --value 2>/dev/null)
cat <<EOF
==== GATE DETACHED (#3473) ====
unit:        $UNIT
cgroup:      ${CG:-<unavailable>}
args:        agent-gate.sh ${GATE_ARGS[*]:-<full gate>}
summary:     $SUMMARY
heartbeat:   $SUMMARY.heartbeat
log:         $LOGFILE
env:         forwarded $FORWARDED variables${SKIPPED:+; DROPPED: $SKIPPED}
this gate is in its OWN cgroup — it survives this session exiting, crashing or being
recycled. It does NOT skip the #1825 slot queue, so it may sit in 'waiting for gate
slot' first.
poll it with (never read the gate log):
  bash scripts/gate-liveness.sh $SUMMARY
stop it with:
  systemctl --user stop $UNIT
==== END GATE DETACHED ====
EOF
exit 0

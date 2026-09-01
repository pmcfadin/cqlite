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
# STALLED (this script prints the exact command, bound to this run).
#
# THREAT MODEL — STATED, so findings in this area get TRIAGED rather than patched (#3473)
# ---------------------------------------------------------------------------------------
# Seven review rounds landed 25 findings in this change, and from round 5 on the launcher was
# the dominant source (2, 2, 3) while the reader settled at one per round. Every one of those
# launcher findings was a HOSTILE LOCAL USER scenario: a planted symlink, a sticky directory
# owned by someone else, values readable in another process's argv. That list does not close on
# its own, so the boundary is written down here rather than rediscovered each round.
#
# CLAUDE.md already rules on this exact class, for roborev's wrapper:
#     "the INVOKER can bypass this" => out of model - record it, do not patch it;
#     "a NON-INVOKER can bypass this" or "this can be bypassed BY ACCIDENT" => defect.
#     Same-host actors able to write these scripts are INVOKER-CLASS, not third parties.
#
# Applied here:
#
#   OUT OF MODEL — an attacker who can WRITE the directory holding a caller-supplied summary or
#   log path. On this fleet lanes run as ONE user on dedicated boxes, so such an actor can also
#   edit this script, shadow `systemd-run` on PATH, or simply run their own gate. Defending the
#   probe while leaving those open is the false-assurance shape #3312 exists to remove. The
#   DEFAULT paths are not exposed to this at all: they live in a 0700 `mktemp -d` with an
#   unguessable name.
#
#   IN MODEL, and fixed — exposure that needs NO write access anywhere: `/proc/<pid>/cmdline` is
#   WORLD-READABLE, so forwarding the environment through `systemd-run --setenv` handed every
#   token to any user on the box. That is a non-invoker read, so it is a defect, and it is why
#   the environment now travels in a mode-0600 file instead.
#
#   IN MODEL, and fixed — ACCIDENT AND DRIFT, which is the larger category in practice: a stale
#   heartbeat standing in for a real one, a leftover summary from an earlier run, a predictable
#   temp name colliding with a concurrent gate, a caller pointing two gates at one path.
#
# The cheap hardening already here (mktemp everywhere, symlink refusal, non-regular-file
# refusal, non-destructive probes) is KEPT: CLAUDE.md's ruling explicitly says cheap hardening
# stays even where an invoker could reach the same end another way. What this section licenses is
# not removing it, but declining to ADD more of it — a further "a local user could plant X"
# finding here should be recorded against the threat model, not fixed by a ninth round.
#
# Usage:
#   bash scripts/flow/gate-detached.sh [--summary <path>] [--log <path>] [--] [gate args...]
#
# Prints the unit name, summary path, heartbeat path and the poll command, then exits
# 0 immediately — the gate keeps running. Any remaining arguments go to agent-gate.sh.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

# A DEDICATED usage function, not a `sed -n '2,45p'` over the header (roborev job 188, Low). The
# line range drifted as the header grew: by the time the threat-model section was added, --help
# ended mid-sentence inside it and omitted the invocation syntax entirely. A range that must be
# re-tuned whenever a comment is edited is a latent defect; this cannot drift.
_usage() {
  # delimiter deliberately NOT 'USAGE': that word is a section heading at column zero inside
  # this text, and it terminated the heredoc early.
  cat <<'HELPTEXT'
gate-detached.sh — run scripts/agent-gate.sh in its OWN cgroup, so a lane session's teardown
cannot kill it (issue #3473).

USAGE
  bash scripts/flow/gate-detached.sh [--summary <path>] [--log <path>] [--] [gate args...]

OPTIONS
  --summary <path>   where the gate writes its AGENT-GATE SUMMARY block. Default: a file inside
                     a private 0700 mktemp directory (printed on launch).
  --log <path>       where the gate's stdout/stderr go. Default: alongside the summary.
                     Must not be the summary or the heartbeat path.
  --                 end of options; everything after it is passed to agent-gate.sh.
  -h, --help         this text.

ON SUCCESS it prints the unit name, cgroup, summary, heartbeat, log, and the exact
gate-liveness.sh command to poll with (shell-escaped, bound to this run's run-id), then exits 0
immediately — the gate keeps running.

EXIT CODES
  0   the gate was launched and proved monitorable
  1   refused (unusable summary/log path, or the gate published no heartbeat)
  69  this host lacks a capability this script needs: no working `systemd-run --user`, no
      0700 per-user runtime directory, or no `flock`

WHY, and the threat model it does and does not cover: see the comment header of this file and
docs/development/lane-gate-execution.md.
HELPTEXT
}

SUMMARY=""; LOGFILE=""
GATE_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --summary) SUMMARY="${2:?--summary needs a path}"; shift 2 ;;
    --log)     LOGFILE="${2:?--log needs a path}"; shift 2 ;;
    -h|--help) _usage; exit 0 ;;
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

# LINGERING IS A SEPARATE, EQUALLY LOAD-BEARING PRECONDITION (roborev job 206, High). Escaping the
# pane's cgroup is necessary but NOT sufficient: without lingering, the USER MANAGER itself is
# stopped when the user's last session ends, and stopping `user@<uid>.service` tears down the
# transient units it manages — including this gate. `systemd-run --user` succeeding says the manager
# is running NOW; it says nothing about whether it survives a logout. systemd's own documentation is
# the authority: lingering is what keeps a user manager "around after logouts".
#
# `KillUserProcesses=no` does NOT substitute for it — that governs whether a SESSION's processes are
# killed at session end, not whether the user manager and its units are stopped.
#
# This refuses for the same reason the cgroup check does: the caller would otherwise believe a
# 30-50 minute gate is protected when it is not, which is the exact false assurance this script
# exists to remove. An UNMEASURABLE answer refuses too — a positive verdict requires an affirmative
# measurement, and "I could not ask" is not one.
_linger=$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null || true)
case "$_linger" in
  yes) ;;
  no)
    echo "gate-detached: user lingering is DISABLED for '$(id -un)', so the user systemd manager" >&2
    echo "               is stopped when your last session ends — and stopping it tears down the" >&2
    echo "               transient unit holding the gate. Escaping the pane cgroup does not help:" >&2
    echo "               the gate would still die at logout (#3473)." >&2
    echo "               Remedy (one command, persists across reboots):" >&2
    echo "                   loginctl enable-linger $(id -un)" >&2
    echo "               Then re-run. Refusing rather than claiming a protection this host cannot" >&2
    echo "               currently deliver." >&2
    exit 69 ;;
  *)
    echo "gate-detached: could NOT determine whether user lingering is enabled" >&2
    echo "               ('loginctl show-user -p Linger' gave '${_linger:-<no answer>}')." >&2
    echo "               Without lingering the user manager stops at logout and takes the gate" >&2
    echo "               with it, so this cannot be assumed (#3473). Refusing: a claim that the" >&2
    echo "               gate survives session teardown needs an affirmative measurement." >&2
    echo "               If lingering IS enabled, check 'loginctl' is on PATH and working." >&2
    exit 69 ;;
esac

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
# (the generated export line is a single line). A dropped variable is printed by name so a
# gate that then behaves oddly has a paper trail.
# ---------------------------------------------------------------------------
# WHY A WRAPPER SCRIPT AND NOT `--setenv` (roborev job 169, Medium). `--setenv=NAME=VALUE`
# places every value on systemd-run's COMMAND LINE, and `/proc/<pid>/cmdline` is
# WORLD-READABLE — whereas `/proc/<pid>/environ` is readable only by the owner. So forwarding
# the environment through argv is a real downgrade in exposure, and this fleet's environment
# routinely holds `GH_TOKEN`, `PROJECTS_TOKEN` and `PARITY_HEAL_TOKEN`.
#
# Instead the values are written into a mode-0600 wrapper script inside the private directory,
# quoted with `printf %q` (shell-exact, so no systemd quoting semantics to get wrong — an
# `EnvironmentFile` attempt was measured returning EMPTY values, so it is not used), and only
# the SCRIPT PATH appears in argv.
ENV_SCRIPT=""
if [ -z "${PRIVDIR:-}" ]; then
  PRIVDIR=$(mktemp -d "${TMPDIR:-/tmp}/cqlite-gate-XXXXXX") || {
    echo "gate-detached: cannot create a private directory for the environment script" >&2
    exit 1
  }
fi
ENV_SCRIPT="$PRIVDIR/gate-env.sh"
# gate-env.sh holds the WHOLE exported environment, tokens included (roborev job 172, Medium).
# The first version never deleted it, so every launch left a persistent 0600 copy of this
# session's credentials in an undisclosed directory — on success AND on every failure path.
# Measured while fixing this: 51 such files had accumulated in /tmp during development. It
# needs no attacker to write anything; it is a credential-at-rest leak of our own making.
#
# Removed unconditionally at exit. By then either the unit is proven running (so the wrapper
# already `exec`d and no longer reads the file) or we are failing and have stopped the unit.
# The private directory goes too, but only when it holds nothing else: with DEFAULT paths it
# also holds the summary and log the caller needs, and deleting those would be worse than the
# leak. `rmdir` gives exactly that semantics for free — it only succeeds on an empty directory.
# shellcheck disable=SC2317,SC2329  # invoked indirectly, by the EXIT trap below
_cleanup_env() {
  [ -n "${ENV_SCRIPT:-}" ] && rm -f "$ENV_SCRIPT" 2>/dev/null
  [ -n "${PRIVDIR:-}" ] && rmdir "$PRIVDIR" 2>/dev/null
  return 0
}
trap _cleanup_env EXIT
( umask 077; : > "$ENV_SCRIPT" ) || {
  echo "gate-detached: cannot create $ENV_SCRIPT" >&2; exit 1
}
FORWARDED=0
SKIPPED=""
{
  echo '#!/usr/bin/env bash'
  echo '# Generated by gate-detached.sh (#3473). Mode 0600: it carries this session'"'"'s'
  echo '# environment, which argv must never carry (see the note in the launcher).'
} >> "$ENV_SCRIPT"
while IFS= read -r -d '' kv; do
  name="${kv%%=*}"
  value="${kv#*=}"
  case "$name" in
    # systemd re-derives these for the new unit; carrying ours over is wrong.
    INVOCATION_ID|JOURNAL_STREAM|MAINPID|LISTEN_PID|LISTEN_FDS|NOTIFY_SOCKET) continue ;;
    # session bookkeeping that must not follow the gate.
    _|SHLVL|OLDPWD|PWD) continue ;;
    # agent-gate.sh's OWN re-exec markers (roborev job 166, Low). It sets AGENT_GATE_WRAPPED=1
    # and AGENT_GATE_WRAPPER=<cmd> after re-execing itself under nice/taskpolicy. Forwarding
    # them into a fresh systemd unit carries the CLAIM of being wrapped without the actual
    # nice state, so the detached gate would skip its own wrapping and then REPORT itself as
    # wrapped in the SUMMARY's cpu-budget line — a false accelerator claim, and unwrapped
    # scheduling for a 30-50 minute job. Dropped so the new process decides and records its
    # own wrapper state.
    AGENT_GATE_WRAPPED|AGENT_GATE_WRAPPER) continue ;;
    # BUILD-FLAG CONTAMINATION (#3740). agent-gate.sh's own header says "never export global
    # RUSTFLAGS on a worker": a non-empty RUSTFLAGS in the environment SUPPRESSES cargo's managed
    # block, and the gate then APPENDS its own -- yielding a doubled `-D warnings -D warnings` that
    # applies deny-warnings to components the gate deliberately scopes it AWAY from. Measured: that
    # contamination made binding-rust-tests FAIL on a CLEAN tree, was diagnosed as a source defect,
    # and halted the fleet for about an hour on a P0 that did not exist.
    #
    # THIS LAUNCHER IS THE PROPAGATION VECTOR, which is why the drop belongs here: it forwarded the
    # caller's WHOLE environment, so a lane that exported RUSTFLAGS once poisoned every detached gate
    # it starts -- invisibly, because the flag arrives through systemd-run rather than a command line
    # anyone can read. Verified by capturing the generated wrapper: before this arm it carried
    # RUSTFLAGS twice and CARGO_ENCODED_RUSTFLAGS once.
    #
    # Dropped, and NAMED in SKIPPED so the launch banner discloses it -- a silent drop would be its
    # own version of the same problem. CARGO_ENCODED_RUSTFLAGS and RUSTDOCFLAGS are the same channel
    # under other names; the gate sets what it needs per-component via a scoped prefix.
    RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|RUSTDOCFLAGS)
      SKIPPED="${SKIPPED:+$SKIPPED }$name(build-flag-contamination:#3740)"; continue ;;
    # The gate's summary path is set explicitly below.
    AGENT_GATE_SUMMARY_FILE) continue ;;
  esac
  case "$name" in
    ''|*[!A-Za-z0-9_]*|[0-9]*) SKIPPED="${SKIPPED:+$SKIPPED }$name(non-identifier)"; continue ;;
  esac
  case "$value" in
    *$'\n'*) SKIPPED="${SKIPPED:+$SKIPPED }$name(newline-in-value)"; continue ;;
  esac
  printf 'export %s=%q\n' "$name" "$value" >> "$ENV_SCRIPT"
  FORWARDED=$((FORWARDED + 1))
done < <(env -0)
# A LAUNCH NONCE (roborev job 190, Medium). Verification used to accept "the first summary run-id
# that differs from the pre-launch value" as ours — but a CONCURRENT gate on the same summary path
# can publish first, and then this launcher would report success and print a poll command bound to
# the PEER's run. A run-id we cannot predict is no basis for the claim; a token we generate is.
# The gate echoes it into the summary and hands it to the beater, and both artifacts must carry it.
LAUNCH_NONCE="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM$RANDOM"
printf 'export AGENT_GATE_LAUNCH_NONCE=%q\n' "$LAUNCH_NONCE" >> "$ENV_SCRIPT"
printf 'export AGENT_GATE_SUMMARY_FILE=%q\n' "$SUMMARY" >> "$ENV_SCRIPT"
# SELF-UNLINK before exec (roborev job 178, Medium). The launcher's EXIT trap cannot run if the
# launcher is SIGKILLed, or its session torn down, after the unit has started — and then this
# 0600 file, holding every forwarded secret, survives indefinitely. Deleting it from INSIDE the
# wrapper ties its lifetime to the process that actually consumed it. Safe as the last statement
# before `exec`: bash has read the whole script by then, and `exec` replaces the process so
# nothing reads the file again. The launcher trap stays as the fallback for paths where the
# wrapper never runs at all (a refused launch).
# ABSOLUTE tool paths, resolved HERE (roborev job 269, Medium). The wrapper exports the CALLER's
# PATH before these two lines run, so an unqualified `rm`/`bash` resolves through a PATH this
# script does not control: a PATH without `rm` makes the self-unlink fail SILENTLY, leaving the
# 0600 file holding every forwarded secret on disk indefinitely, and a PATH that shadows `bash`
# decides what we exec. Resolve both in the LAUNCHER's PATH and require absolute executables.
# `env` joins them (roborev job 318, Low): the systemd-run command line below used to hard-code
# /usr/bin/env and /bin/bash while these resolved paths sat unused, so a valid systemd host without
# an FHS layout passed every capability check and then failed to exec the wrapper. Resolve what we
# actually exec.
_rm_abs="$(command -v rm || true)"; _bash_abs="$(command -v bash || true)"
_env_abs="$(command -v env || true)"
for _tool_pair in "rm:$_rm_abs" "bash:$_bash_abs" "env:$_env_abs"; do
  _tool_name="${_tool_pair%%:*}"; _tool_path="${_tool_pair#*:}"
  case "$_tool_path" in
    /*) [ -x "$_tool_path" ] && continue
        echo "gate-detached: resolved '$_tool_name' to '$_tool_path', which is not executable." >&2 ;;
    *)  echo "gate-detached: cannot resolve '$_tool_name' to an absolute path (got '${_tool_path:-nothing}')." >&2 ;;
  esac
  echo "               The wrapper needs all three: to delete its own secret-bearing copy, to exec" >&2
  echo "               the gate. Refusing rather than emitting a wrapper that may leak it (#3473)." >&2
  rm -f "$ENV_SCRIPT" 2>/dev/null || true
  exit 1
done
printf '%q -f -- %q\n' "$_rm_abs" "$ENV_SCRIPT" >> "$ENV_SCRIPT"
printf 'exec %q %q "$@"\n' "$_bash_abs" "$REPO_ROOT/scripts/agent-gate.sh" >> "$ENV_SCRIPT"

# The log is TRUNCATED with `>`, which follows symlinks (roborev job 169, Medium): in a shared
# directory another user could plant a symlink at a caller-supplied log path and have us
# overwrite any file this user can write. Refuse a symlink or non-regular destination.
#
# RESIDUAL, stated because it is not fully closable from shell: this is a check-then-create, so
# a symlink planted in the window between them is not caught (bash cannot open O_NOFOLLOW).
# The window is microseconds and requires an attacker already able to write that directory; the
# DEFAULT log path is unguessable and inside a 0700 mkdtemp, so this only applies to a
# caller-supplied path in a shared directory.
# The log must not ALIAS the summary or the heartbeat (roborev job 183, Medium). Two distinct
# failures if it does, and both are silent:
#   * log == summary  : the gate REWRITES the summary (sentinel, then verdict) with `>`, which
#                       truncates the accumulated log, and two writers then contend for one file;
#   * log == heartbeat: the beater publishes by RENAME, which unlinks the log's open inode — the
#                       advertised log path then holds heartbeat data and the gate's output goes
#                       to a file nobody can find.
# Compared three ways because one is not enough: the literal strings may differ while pointing at
# the same file (`./x` vs `x`, a symlinked directory, a hard link), so `-ef` (same device+inode)
# is the authoritative test and is applied whenever both paths exist.
# CANONICALISE before comparing (roborev job 185). `-ef` only works on files that EXIST, and the
# log normally does not exist yet at this point — so `--summary "$PWD/x" --log "$PWD/./x"` slipped
# through, and creating the log then created the summary path too. Resolving each path's PARENT
# physically (`cd … && pwd -P`, which normalises `.`, `..`, doubled slashes and symlinked
# directory components) and re-appending the basename gives a comparable form for paths that do
# not exist yet. The `-ef` checks are kept for the cases canonicalisation cannot see — a hard
# link between two genuinely different names — and are ALSO repeated after the log is created.
_canon() {  # _canon <path> -> physically-resolved dir + literal basename
  local d b phys
  d=$(dirname -- "$1"); b=$(basename -- "$1")
  if phys=$(cd "$d" 2>/dev/null && pwd -P); then printf '%s/%s' "$phys" "$b"; else printf '%s' "$1"; fi
}
# The RESERVATION path is in this set too (roborev job 200). It is not merely a third file the gate
# writes: this script CREATES A SYMLINK there at launch time, which defeats the `-L` refusal below —
# that check runs while the launch-lock does not yet exist, so `--log <summary>.launch-lock` passed
# it, and the truncate just before launch then FOLLOWED the reservation link and wrote the gate's
# log into a file named after the link's own target text. Refusing here is the precise diagnosis.
_c_log=$(_canon "$LOGFILE"); _c_sum=$(_canon "$SUMMARY"); _c_hb=$(_canon "$SUMMARY.heartbeat")
_c_lock=$(_canon "$SUMMARY.launch-lock")
_alias_of=""
[ "$_c_log" = "$_c_sum" ] && _alias_of="the summary"
[ -z "$_alias_of" ] && [ "$_c_log" = "$_c_hb" ] && _alias_of="the heartbeat"
[ -z "$_alias_of" ] && [ "$_c_log" = "$_c_lock" ] && _alias_of="the launch reservation"
_c_mutex=$(_canon "$SUMMARY.launch-lock.mutex")
[ -z "$_alias_of" ] && [ "$_c_log" = "$_c_mutex" ] && _alias_of="the reclamation mutex"
if [ -z "$_alias_of" ] && [ -e "$LOGFILE" ]; then
  [ -e "$SUMMARY" ] && [ "$LOGFILE" -ef "$SUMMARY" ] && _alias_of="the summary (same inode)"
  [ -z "$_alias_of" ] && [ -e "$SUMMARY.heartbeat" ] && [ "$LOGFILE" -ef "$SUMMARY.heartbeat" ] \
    && _alias_of="the heartbeat (same inode)"
  [ -z "$_alias_of" ] && [ -e "$SUMMARY.launch-lock" ] && [ "$LOGFILE" -ef "$SUMMARY.launch-lock" ] \
    && _alias_of="the launch reservation (same inode)"
fi
if [ -n "$_alias_of" ]; then
  echo "gate-detached: the log path '$LOGFILE' is $_alias_of. Refusing (#3473)." >&2
  # The REASON differs by which path was aliased, and a diagnosis that names the wrong mechanism
  # sends the reader looking in the wrong place (roborev job 200).
  case "$_alias_of" in
    *reservation*)
      echo "               This script creates a SYMLINK at the reservation path, and the log is" >&2
      echo "               truncated with '>', which FOLLOWS a symlink — so the gate's output would" >&2
      echo "               land in a file named after the link's own owner text, not at this path." >&2 ;;
    *)
      echo "               The gate rewrites its summary with '>' and the beater publishes by" >&2
      echo "               rename, so one of them would destroy the other's file and the advertised" >&2
      echo "               log would hold the wrong data." >&2 ;;
  esac
  echo "               Give --log a path of its own." >&2
  exit 1
fi

# PLACED AFTER the tailored `_alias_of` diagnoses ON PURPOSE (test 4b.93). This assert is the
# BACKSTOP for the pairs those checks do not cover; putting it first made it PREEMPT them, so a
# `--log <summary>.launch-lock` stopped naming the symlink-follows-truncate mechanism and printed
# this generic cause instead — and this file already records why that is a defect in its own
# right: "a diagnosis that names the wrong mechanism sends the reader looking in the wrong place".
# EVERY OUTPUT PATH MUST BE DISJOINT FROM EVERY GENERATED RESERVATION PATH, IN BOTH DIRECTIONS
# (roborev job 316, Medium). The checks above only ask "is the LOG one of the SUMMARY's derived
# paths". The reverse was unguarded, so `--summary /tmp/gate.log.launch-lock --log /tmp/gate.log`
# let the extra-lock loop plant its reservation SYMLINK at the advertised summary path: the gate
# then wrote its summary THROUGH that link, and the exit-time reclamation deleted the very path a
# poller had been told to read. Enumerated as two SETS, not as pairs — pair-by-pair is how each
# preceding round found "the next unnamed shape", so this closes the class instead of naming one
# more dangerous case.
_res_paths="$(_canon "$SUMMARY.launch-lock")
$(_canon "$SUMMARY.launch-lock.mutex")
$(_canon "$SUMMARY.heartbeat.launch-lock")
$(_canon "$LOGFILE.launch-lock")"
for _o_name in summary heartbeat log; do
  case "$_o_name" in
    summary)   _o_path="$_c_sum" ;;
    heartbeat) _o_path="$_c_hb"  ;;
    log)       _o_path="$_c_log" ;;
  esac
  # A heredoc, NEVER a pipe: a piped `while read` runs in a subshell, where this `exit 1` would
  # exit only that subshell and the launcher would carry on — the same silent-verdict-discard the
  # gate's own cargo-parse rule forbids.
  while IFS= read -r _r_path; do
    [ -n "$_r_path" ] || continue
    [ "$_o_path" = "$_r_path" ] || continue
    echo "gate-detached: the $_o_name path is also a reservation path this launcher creates" >&2
    echo "               ('$_o_path'). Refusing (#3473)." >&2
    echo "               This script plants a SYMLINK at every reservation path and REMOVES it when" >&2
    echo "               it exits, so an output path that doubles as one would be written through" >&2
    echo "               that link and then deleted: the advertised path would not hold the" >&2
    echo "               artifact, and a poller would read the absence as a vanished gate." >&2
    echo "               Choose --summary/--log paths whose derived lock names do not collide." >&2
    exit 1
  done <<_RESEOF
$_res_paths
_RESEOF
done
# A HARD LINK IS AN ALIAS A PATHNAME CANNOT SHOW (roborev job 321, Medium). The reservation and the
# artifact-set check both identify destinations BY PATH, so two launches naming two different paths
# that are hard links to ONE inode take two different reservations and then write the same file —
# corrupting or silently discarding at least one run's verdict.
#
# This is not a new axis, it is the SECOND HALF of one this script already decided was in model: the
# log/summary/heartbeat destinations are each refused when they are a SYMLINK (job 169). A hard link
# is the same aliasing threat in a different spelling, and closing one spelling while leaving the
# other is the "one axis closed, space declared done" error this file's comments name repeatedly.
#
# THREE-VALUED, and `find` rather than `stat`: stat's format flags are GNU-vs-BSD incompatible and
# this script already refuses to depend on them (see the 0600 mode verification). `[ -z "$(find …)" ]`
# would collapse "the scan FAILED" onto "no match" — a two-valued read of a three-valued signal, and
# this repository LINTS for that shape (1699-find-tristate) — so a single link is confirmed
# AFFIRMATIVELY and anything unmeasurable is its own answer.
_link_count_state() {  # <path> -> multi | single | unknown
  local out
  [ -e "$1" ] || { printf 'single'; return 0; }        # nothing there yet: no alias to worry about
  if out=$(find "$1" -maxdepth 0 -links +1 -print 2>/dev/null) && [ -n "$out" ]; then
    printf 'multi'; return 0
  fi
  if out=$(find "$1" -maxdepth 0 -links 1 -print 2>/dev/null) && [ -n "$out" ]; then
    printf 'single'; return 0
  fi
  printf 'unknown'
}

# Refuse <path> unless it is provably a single-link regular file. <what> names it for the operator.
_refuse_if_aliased() {  # <path> <what>
  case "$(_link_count_state "$1")" in
    single) return 0 ;;
    multi)
      echo "gate-detached: the $2 path '$1' already exists with MORE THAN ONE HARD LINK, so another" >&2
      echo "               name refers to the same file. Reservations identify PATHS, so a peer" >&2
      echo "               launch using the other name would reserve successfully and then write" >&2
      echo "               this same inode, discarding one run's verdict (roborev job 321)." >&2
      echo "               Refusing. Use a fresh path, or remove the extra link." >&2
      exit 1 ;;
    *)
      echo "gate-detached: could not determine the hard-link count of the $2 path '$1', so it cannot" >&2
      echo "               be shown to be un-aliased. Refusing rather than writing a destination a" >&2
      echo "               peer may also hold under another name (roborev job 321)." >&2
      exit 1 ;;
  esac
}

if [ -L "$LOGFILE" ] || { [ -e "$LOGFILE" ] && [ ! -f "$LOGFILE" ]; }; then
  echo "gate-detached: log path '$LOGFILE' is a symlink or not a regular file — refusing to" >&2
  echo "               truncate it (#3473)." >&2
  exit 1
fi
_refuse_if_aliased "$LOGFILE" log
# NON-DESTRUCTIVE writability probe (roborev job 193, Low). The log used to be TRUNCATED here,
# before the summary and heartbeat destinations were validated — so a later refusal (a bad summary
# directory, say) destroyed the caller's previous log even though no gate ever started. The real
# truncation now happens immediately before `systemd-run`, once every refusal path is behind us.
#
# The redirection stays wrapped in a subshell so BASH's own "No such file or directory" for a bad
# path is suppressed: a `2>/dev/null` on the command does not cover an error the shell itself
# reports for the redirect.
if [ -e "$LOGFILE" ]; then
  # A zero-byte APPEND cannot truncate and does not alter mtime.
  ( : >> "$LOGFILE" ) 2>/dev/null || {
    echo "gate-detached: the log at '$LOGFILE' exists but is not writable." >&2
    echo "               Refusing to launch a gate whose output would be unreadable (#3473)." >&2
    exit 1
  }
else
  ( : > "$LOGFILE" ) 2>/dev/null || {
    echo "gate-detached: cannot create the log at '$LOGFILE' (missing directory, or not writable)." >&2
    echo "               Refusing to launch a gate whose output would be unreadable (#3473)." >&2
    exit 1
  }
  # Remove the probe so a later refusal leaves the filesystem exactly as it was.
  rm -f "$LOGFILE" 2>/dev/null || true
fi
# ...and NOW that it exists, repeat the inode comparison. Canonicalisation above handles the
# spellings it can see; this catches whatever it cannot, and it is the check that would have
# caught the two-nonexistent-paths case even without canonicalisation (job 185).
if [ -e "$SUMMARY" ] && [ "$LOGFILE" -ef "$SUMMARY" ]; then
  echo "gate-detached: creating the log revealed it is the SAME FILE as the summary" >&2
  echo "               ('$LOGFILE' -ef '$SUMMARY'). Refusing (#3473)." >&2
  exit 1
fi
if [ -e "$SUMMARY.heartbeat" ] && [ "$LOGFILE" -ef "$SUMMARY.heartbeat" ]; then
  echo "gate-detached: creating the log revealed it is the SAME FILE as the heartbeat" >&2
  echo "               ('$LOGFILE' -ef '$SUMMARY.heartbeat'). Refusing (#3473)." >&2
  exit 1
fi

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
_refuse_if_aliased "$SUMMARY" summary
# The probe deliberately NEVER touches $SUMMARY itself. Truncating it to test writability
# would destroy whatever is at that path — and under #2874 that could be a LIVE PEER's
# summary block, i.e. the probe would cause the very data loss the no-clobber contract
# exists to prevent, before the gate's own foreign-run-id detection ever got a chance to
# see it. Directory capability is what both publishers actually need, and it is testable
# without writing to any path the caller owns.
#
# Created with mktemp, NOT a predictable `$$` name opened with `>` (roborev job 164, Medium).
# A guessable path in a caller-chosen shared directory can be pre-created as a symlink by
# another local user, and `>` follows symlinks — so the probe itself would truncate an
# arbitrary file writable by the gate user. This is the THIRD place in this change where that
# same shape appeared (the default /tmp artifact names, the beater's sibling temp, and here),
# so it is now also enforced as a RULE by a structural assert in the test suite rather than
# fixed one site at a time.
#
# Both names must match the gate's tree-integrity carve-out EXACTLY — `.heartbeat.tmp.` followed by
# six alphanumerics — not merely share its prefix, because a concurrent gate can capture the tree
# mid-probe and would otherwise call these files a mutation and FAIL ITSELF (roborev job 205).
#
# This dependency was already written down here, and it still broke: job 204 narrowed that carve-out
# from `.heartbeat.tmp.*` to the six-character mktemp shape, and the `probe` prefix put both names
# outside it. The comment named the consumer; the change was made without reading it. A stated
# dependency is not a protected one — `scripts/tests/test_gate_detached.sh` now DERIVES every
# template this file creates beside the summary and asserts the gate's own predicate excuses it, so
# the two files are checked against each other rather than by whoever remembers.
_hbprobe=$(mktemp "$SUMMARY.heartbeat.tmp.XXXXXX" 2>/dev/null) || {
  echo "gate-detached: cannot create a file in '$_sumdir', so neither the gate's summary nor" >&2
  echo "               the liveness heartbeat could be published there — every poll of this" >&2
  echo "               gate would answer UNKNOWN. Refusing to launch an unmonitorable gate," >&2
  echo "               rather than burn 30-50 minutes certifying nothing (#3473)." >&2
  exit 1
}
# A SECOND six-character name, not `$_hbprobe.renamed`: the destination has to satisfy the same
# carve-out. Renaming ONTO an existing file is also closer to what the beater actually does.
_hbprobe2=$(mktemp "$SUMMARY.heartbeat.tmp.XXXXXX" 2>/dev/null) || {
  rm -f "$_hbprobe" 2>/dev/null || true
  echo "gate-detached: cannot create a second file in '$_sumdir' to prove renames work there." >&2
  echo "               Refusing to launch an unmonitorable gate (#3473)." >&2
  exit 1
}
if ! mv -f "$_hbprobe" "$_hbprobe2" 2>/dev/null; then
  rm -f "$_hbprobe" "$_hbprobe2" 2>/dev/null || true
  echo "gate-detached: cannot RENAME within '$_sumdir', which the heartbeat's atomic publish" >&2
  echo "               requires — every poll of this gate would answer UNKNOWN. Refusing (#3473)." >&2
  exit 1
fi
rm -f "$_hbprobe" "$_hbprobe2" 2>/dev/null || true

# The heartbeat DESTINATION is checked only for the shapes that give a better message than a
# post-launch failure would (a directory or symlink there can never work). Its PERMISSIONS are
# deliberately NOT modelled (roborev job 166, Medium): appending zero bytes proves write
# access to the FILE, not permission to REPLACE it, and in a sticky directory a file owned by
# another user is appendable but not renameable-over — so the probe passed while the beater's
# `mv -f` would fail forever. Modelling sticky-bit ownership rules would be a third guess at a
# permission system; instead the launch is VERIFIED BY OUTCOME below, which is true regardless
# of why a write might fail.
_hbdest="$SUMMARY.heartbeat"
if [ -L "$_hbdest" ] || { [ -e "$_hbdest" ] && [ ! -f "$_hbdest" ]; }; then
  echo "gate-detached: '$_hbdest' exists but is not a regular file (symlink, directory, fifo" >&2
  echo "               or device) — the liveness heartbeat could never be published there," >&2
  echo "               so this gate would be unmonitorable. Refusing (#3473)." >&2
  exit 1
fi
_refuse_if_aliased "$_hbdest" heartbeat

# --collect reaps the unit record on exit; the SUMMARY FILE is the verdict artifact, so
# nothing of record is lost with the unit. --same-dir keeps the gate in this worktree
# (it derives its scope from git in $PWD). stdin is closed: a gate must never wait on a
# terminal that is not there.
# PRE-LAUNCH SNAPSHOT (roborev job 169, Medium). The post-launch check used to accept ANY
# heartbeat containing `beat-epoch:`, so a STALE or FOREIGN beat already sitting at that path
# satisfied it — which is exactly the sticky-directory case the check exists to catch: the new
# beater cannot replace that file, but the old file makes the launch look monitorable.
#
# So the check is BOUND to the new run: the summary must come to carry a run-id that is not the
# one already there, and the heartbeat must carry THAT SAME run-id. Nothing pre-existing can
# satisfy either half.
# The pre-launch run-ids used to be captured here and compared later. That comparison was REPLACED
# by the launch nonce (a value we generate, rather than one we cannot predict) and these captures
# were left behind unused — dead code that reads like a check (roborev job 193). Removed.
#
# What the nonce does NOT do is stop two launchers pointing at ONE summary path: each would prove
# ownership of its own artifacts while their heartbeat renames and summary rewrites destroyed each
# other, leaving both advertised poll commands unreliable. #2874 already forbids that
# configuration; the launcher now DETECTS it rather than walking into it.
#
# The reservation is a create-with-O_EXCL (`set -C`) recording the owning unit. It is deliberately
# SELF-HEALING rather than released by anyone: the gate outlives this launcher, so no process could
# reliably remove it, and a lock nobody can release is worse than no lock. A reservation whose
# recorded unit is no longer active is therefore stale, and is reclaimed once.
_reserve="$SUMMARY.launch-lock"
# A LOCK DIRECTORY, not a lock file, because reclaiming a stale lock must be ATOMIC (roborev job
# 194, Medium). The file version was racy in two ways: a second launcher could read a freshly
# acquired lock BEFORE its owner's systemd unit became active, judge it stale, and delete it; and
# two reclaimers could delete each other's replacement locks. Both ended with two gates writing one
# summary path — the exact thing the lock exists to prevent.
#
# `mkdir` is atomic, and so is renaming a directory: only ONE process can successfully move a given
# directory away, so the rename is the compare-and-swap that decides who owns the reclamation.
#
# Liveness is also fixed: the owner is live if its LAUNCHER PROCESS is still running OR its unit is
# active. That closes the startup window, because during it the launcher is by definition alive.
# _proc_identity <pid> -> a tiered process-identity token, or empty. Same tiering as the gate and
# the beater use: /proc start ticks where available, else `ps -o lstart=` (portable, second
# granularity, empty for a dead pid).

# Is <pid> a ZOMBIE? `kill -0` succeeds on one (the entry survives until its parent reaps it), so
# without this a launcher that died un-reaped reads as LIVE and its reservation cannot self-heal —
# the same permanent-block failure the incomplete-owner window used to cause, resurfacing in a
# different place. A zombie has already exited and can never start a unit, so it is GONE.
#
# Returns 0 only on an AFFIRMATIVE zombie reading. Unmeasurable => 1 (not a zombie), which keeps the
# caller refusing: "I could not tell" must never license reclaiming a lock that may be live.
# Is <unit> LIVE? `systemctl is-active --quiet` answers 0 only for exactly "active", so every other
# outcome — `activating` (a unit still STARTING), `deactivating`, a dbus/query failure — fell into
# the "dead, reclaim it" branch and let a second gate launch against a running one (roborev job 205).
# That is this repository's recurring shape: a permissive branch keyed on `!= good` rather than on an
# affirmative bad state, so every unmeasured state inherits the permissive answer.
#
# Reclamation therefore requires an AFFIRMATIVE terminal reading. Transitional states are LIVE, and
# an unmeasurable one is LIVE too — refusing to launch costs a retry, reclaiming a live unit's path
# costs two gates writing one summary.
# THREE-VALUED, and the third value is the point (owner ruling on #3473-R6). Reclamation may only
# follow an AFFIRMATIVE reading; every "I could not tell" must refuse. But naively refusing on an
# unreadable identity would break the NORMAL stale case — a genuinely dead owner has no /proc entry,
# so its identity is unmeasurable too — and that is the permanent-block failure of job 196. The
# distinction that makes the inversion safe is between the pid being ABSENT (affirmative death) and
# the pid being PRESENT but unreadable (unknown).
#
# `kill -0` cannot make that distinction: it fails for BOTH "no such process" and "exists but not
# signallable" (EPERM, e.g. another user's process). /proc answers it authoritatively, and
# `systemd-run --user` already makes Linux a precondition of this script.
_pid_state() {  # <pid> -> exists | gone | unknown
  case "$1" in ''|*[!0-9]*) printf 'unknown'; return 0 ;; esac
  if kill -0 "$1" 2>/dev/null; then printf 'exists'; return 0; fi
  if [ -d "/proc/$1" ]; then printf 'exists'; return 0; fi     # EPERM, not ESRCH
  if [ -d /proc ] && [ -r /proc ]; then printf 'gone'; return 0; fi
  ps -p "$1" >/dev/null 2>&1 && { printf 'exists'; return 0; }
  printf 'unknown'                                             # no /proc and ps inconclusive
}

# FOUR-VALUED, and the fourth value is the one my own previous fix got wrong (roborev jobs 319 then
# 320, both Medium). Job 319 split this into three states so each caller could NAME its polarity —
# and I then handed BOTH callers the SAME partition, one tuned for the REFUSE side, where lumping
# every non-terminal state under `live` is conservative and correct. On the ACCEPT side it is not:
# `deactivating` is a unit definitively SHUTTING DOWN, so accepting it is exactly the
# one-beat-then-dead case the acceptance gate exists to reject, and `maintenance` or a state systemd
# invents later are not evidence of anything. **Naming the polarity is not enough if the state space
# is only articulated for one of them.**
#
# So the two sides get OPPOSITE closures, which is the whole point:
#   * the REFUSE side (reclamation) is closed on the TERMINAL side — only `inactive|failed` are
#     affirmative deaths, so an unrecognised state refuses. Job 241's rule, unchanged.
#   * the ACCEPT side is an ALLOWLIST of genuinely-running states — anything not named cannot
#     accept. This is the standing rule that an EXCUSAL IS A POSITIVE VERDICT: blocklisting the
#     dangerous states means the next unnamed one is admitted by default, and this function has now
#     produced that same defect at four consecutive rounds (205, 241, 319, 320) in a different
#     spelling each time. One inversion closes the class; a fifth named state would not.
_unit_state() {  # <unit> -> running | stopping | terminal | unknown
  local st rc
  st=$(systemctl --user show -p ActiveState --value "$1" 2>/dev/null); rc=$?
  if [ "$rc" != 0 ] || [ -z "$st" ]; then printf 'unknown'; return 0; fi
  case "$st" in
    # ALLOWLIST: systemd's states in which the unit's processes are present and not being torn down.
    active|activating|reloading|refreshing) printf 'running' ;;
    deactivating)                           printf 'stopping' ;;
    inactive|failed)                        printf 'terminal' ;;
    *)                                      printf 'unknown' ;;
  esac
}

# 0 only for an AFFIRMATIVELY RUNNING unit. Use this wherever 0 ACCEPTS something: `stopping`,
# `unknown` and any future state all refuse, because none of them is evidence a verdict is coming.
_unit_is_affirmatively_live() {  # <unit>
  [ "$(_unit_state "$1")" = running ]
}

# ACCEPTANCE NEEDS TWO AFFIRMATIVE LEGS, NOT ONE (roborev job 323, Medium). `_unit_is_affirmatively_live`
# reads only `ActiveState` — and this file documents, at length, that an ORPHANED NON-GATE PROCESS keeps
# a unit `active` forever. That is the whole reason `_unit_runs_a_gate` exists. So a gate that published
# one beat, died, and left a child was ACCEPTED as monitorable: the unit reads active, and no terminal
# verdict will ever arrive. I used the weaker predicate at the two acceptance sites while the stronger
# one sat in the same file for exactly this distinction.
#
# WHY IT IS SAFE HERE, when it is deliberately NOT used at the post-launch monitorability probe below:
# that probe can run before the gate has exec'd, where "no agent-gate.sh in the cgroup" is the NORMAL
# startup state. These sites are only reached when the reader answered RUNNING, which means it saw a
# FRESH BEAT for this run-id — so the gate provably started, and an absent gate process now means it
# is gone rather than not yet arrived.
#
# THE VETO IS KEYED ON THE AFFIRMATIVE ABSENCE (rc 1), NOT ON "not rc 0". `_unit_runs_a_gate` answers 2
# = unmeasurable, and on a cgroup-v1 host it answers 2 ALWAYS (job 322's declared precondition), so
# demanding rc 0 here would make every launch on such a host fail after startup — a guard that reds on
# correct input. Acceptance therefore requires an affirmative LIVE reading from ActiveState AND the
# absence of an affirmative "no gate in this cgroup"; both legs are affirmative in the direction that
# can only weaken acceptance, and an unmeasurable cgroup falls back to the ActiveState leg alone.
_unit_accepts_as_monitorable() {  # <unit> -> 0 = may accept
  _unit_is_affirmatively_live "$1" || return 1
  _unit_runs_a_gate "$1"
  [ "$?" = 1 ] && return 1        # affirmatively NO gate in the cgroup => an orphan is holding it
  return 0
}

_unit_is_live() {  # <unit> -> 0 = live or unmeasurable (refuse), 1 = affirmatively not running
  # RETAINED, with this polarity, for the sites where 0 means REFUSE (reservation reclamation, and
  # the "has the unit already died" branch below): there, lumping `stopping` and `unknown` in with
  # running is the conservative answer, and job 205/241's reasoning applies unchanged — only an
  # affirmative terminal reading may license reclaiming a path. A thin wrapper over the ONE state
  # reader, so the two polarities cannot drift into two opinions about one unit.
  [ "$(_unit_state "$1")" != terminal ]
}

_proc_is_zombie() {  # <pid> -> 0 = provably a zombie, 1 = not, or unmeasurable
  local pid=$1 _st _state
  if _st=$(cat "/proc/$pid/stat" 2>/dev/null) && [ -n "$_st" ]; then
    # `comm` is parenthesised and may itself contain ')' and spaces, so read the state as the first
    # field after the LAST ')' rather than by counting from the left.
    _state=${_st##*)}
    # Parameter expansion only (round-48 class audit, class 1). `set -- $_state` word-SPLIT and
    # GLOB-EXPANDED every field; the fields after `comm` are numeric or a single letter, so no
    # expansion could match today, but the safe form costs nothing and does not depend on that
    # staying true. It also stops clobbering this function's own positional parameters.
    _state=${_state#"${_state%%[![:space:]]*}"}
    [ "${_state%%[[:space:]]*}" = "Z" ] && return 0
    return 1
  fi
  if _state=$(ps -o state= -p "$pid" 2>/dev/null) && [ -n "$_state" ]; then
    case "$_state" in Z*) return 0 ;; *) return 1 ;; esac
  fi
  return 1
}

_proc_identity() {
  local raw rest ls _had_f
  raw=$(cat "/proc/$1/stat" 2>/dev/null)
  if [ -n "$raw" ]; then
    rest="${raw##*) }"
    # SPLITTING IS WANTED HERE; GLOBBING IS NOT, and a blanket SC2086 disable suppressed both
    # (round-48 class audit, class 1). Field 20 of /proc/<pid>/stat is the process start time, so this
    # genuinely needs word splitting -- but pathname expansion of those fields is never wanted, and the
    # disable is why the enumerator was silent at the one site that needed it. `set -f` for the
    # duration expresses exactly that: split, do not glob. The previous state of `-f` is restored
    # rather than assumed, so this cannot clear a caller's own setting.
    case $- in *f*) _had_f=1 ;; *) _had_f=0 ;; esac
    set -f
    # shellcheck disable=SC2086  # the split is deliberate; `set -f` above means it cannot also glob
    set -- $rest
    [ "$_had_f" = 1 ] || set +f
    if [ $# -ge 20 ]; then printf 'proc:%s' "${20}"; return 0; fi
  fi
  ls=$(ps -o lstart= -p "$1" 2>/dev/null | tr -s ' ')
  [ -n "$ls" ] && printf 'ps:%s' "$ls"
  return 0
}

# A SYMLINK, because acquisition must be atomic AND self-identifying in ONE step (roborev job 199).
#
# Three earlier designs each failed on the same seam. A lock FILE created with `set -C` could not
# carry its owner atomically. A lock DIRECTORY plus a separate `owner` file left a window where the
# lock existed but its owner was unknown — and every way of interpreting that window was wrong:
# refusing forever meant a launcher killed mid-acquisition blocked the path permanently, while
# reclaiming after an age deadline meant a launcher merely PAUSED (SIGSTOP, heavy contention) could
# have its LIVE lock stolen, after which both gates launch on one summary path.
#
# `ln -s` resolves it: creating a symlink is a single atomic operation that FAILS if the path exists,
# and its target is arbitrary text. So the owner is published by the very act of acquiring, and there
# is no window to interpret and no age heuristic to get wrong. Reclamation then needs no timer at
# all — only affirmative proof that the recorded owner is gone.
_res_ident=$(_proc_identity $$)
# THE RESERVATION MUST COVER THE ARTIFACT SET, NOT ONE PATH (roborev job 251). The lock is named after
# the summary, so two launches whose paths differ can both acquire and still collide: with
# `--summary x` and `--summary x.heartbeat`, A locks `x.launch-lock`, B locks `x.heartbeat.launch-lock`,
# both succeed — and A's BEATER then overwrites B's SUMMARY every interval, destroying its terminal
# verdict. Neither launch can see the other. Demonstrated: both launches returned 0.
#
# The rule that covers it: for every path THIS launch will write, refuse if a LIVE reservation already
# names that path as ITS summary. That is one question asked of several paths, using the SAME liveness
# primitives as the main path (`_pid_state`, `_proc_is_zombie`, `_proc_identity`, `_unit_is_live`) rather
# than a second copy of the classification.
# Can <pid> be ruled OUT as a running gate without reading its argv? (roborev job 319)
# 0 = yes, AFFIRMATIVELY: it is gone, or it is a zombie — both have already exited, so neither can be
#     running anything, and treating them as ruled out is what keeps a dead owner's reservation
#     reclaimable (the job-196 permanent block).
# 1 = no: it is present (or unmeasurable) and we could not read its argv => UNKNOWN, caller refuses.
_pid_ruled_out() {
  [ "$(_pid_state "$1")" = gone ] && return 0
  _proc_is_zombie "$1" && return 0
  return 1
}

_unit_runs_a_gate() {  # <unit> -> 0 = a FULL gate is live in that cgroup | 1 = affirmatively not | 2 = unmeasurable
  # ASK WHAT IS IN THE CGROUP, NOT WHETHER ANYTHING IS (lead order; box-wide finding). `ActiveState`
  # answers "is any task left in the scope", which a single ORPHANED `sleep` satisfies forever -- one box
  # was measured with 12 orphaned sleeps and 0 gate scopes. That let an affirmative "owner is dead" reading
  # be overridden into `live`, so the path was refused FOREVER with nothing to reap the orphan.
  local unit="$1" cg procs p a hit _pdir found=1 _unknown=0 _nargv
  cg=$(systemctl --user show -p ControlGroup --value "$unit" 2>/dev/null) || return 2   # could not ASK
  # AN EMPTY ControlGroup IS AN AFFIRMATIVE ANSWER, NOT AN UNKNOWN. `systemctl show` returns rc=0 with an
  # EMPTY value for a unit that no longer exists, so treating empty as unmeasurable made every stale
  # reservation permanently unreclaimable -- the exact defect this helper was written to fix, relocated one
  # step. Caught by 4b.77/4b.86, which assert a dead owner does NOT block the path forever. A unit with no
  # control group has no processes; that is a positive fact about the unit.
  [ -n "$cg" ] || return 1
  # DECLARED PRECONDITION: cgroup v2 UNIFIED (roborev job 322, Medium). This composes the unified
  # path directly. On a cgroup-v1 host `systemd-run --user` still works, but `cgroup.procs` lives
  # under a controller-specific mount, so this path is absent and the helper answers `unknown` (2) —
  # which every caller refuses on. That direction is SAFE, not a hole: a contended reservation is
  # permanently REFUSED rather than wrongly reclaimed, so two gates can never land on one summary.
  # The cost is that a stale reservation cannot self-heal there and the operator must use a fresh path.
  #
  # DECLARED rather than fixed, deliberately. Resolving the mount from /proc/self/mountinfo is the
  # general fix and it is NEW EXECUTABLE CODE on a path this fleet never takes — measured: every box
  # here is `cgroup2fs` unified with `cgroup.controllers` present. A guard for an unreachable host
  # class, written without a host to test it on, is how the next review round gets its finding. If a
  # v1 host ever enters the fleet, resolve the mount here; until then this sentence is the boundary.
  procs="/sys/fs/cgroup${cg}/cgroup.procs"
  if [ ! -e "$procs" ]; then
    # Absent, or unlookable? `-e` is two-valued and collapses those. The scope genuinely vanishing is an
    # affirmative "no gate"; an unsearchable parent is an unknown and must refuse.
    _pdir=${procs%/*}
    if [ -d "$_pdir" ] && [ -x "$_pdir" ]; then return 1; fi
    return 2
  fi
  [ -r "$procs" ] || return 2               # exists but UNREADABLE => genuinely unmeasurable; caller refuses
  while IFS= read -r p; do
    [ -n "$p" ] || continue
    # AN UNINSPECTABLE PID IS NOT A "NO" (roborev job 319, Medium). This line used to `continue`,
    # and its comment claimed that was "not evidence either way" — but the function DEFAULTS to
    # found=1, so skipping every uninspectable pid returns 1, "AFFIRMATIVELY no gate", which is
    # precisely the evidence the comment disclaimed. The caller reclaims the reservation on 1, so a
    # live gate whose argv we merely could not read had its summary path handed to a second gate.
    #
    # The distinction that makes refusing safe — the same one `_pid_state` was written for — is
    # between a pid that is AFFIRMATIVELY GONE and one that is PRESENT but unreadable. Refusing on
    # both would resurrect the job-196 permanent block, because a genuinely dead owner's pids are
    # unreadable too. A ZOMBIE counts as ruled out for the same reason `_proc_is_zombie` exists: it
    # has already exited and cannot be running a gate.
    if ! [ -r "/proc/$p/cmdline" ]; then
      if _pid_ruled_out "$p"; then continue; fi
      _unknown=1; continue
    fi
    hit=0
    _nargv=0
    # OWNERSHIP, NOT GATE-OF-RECORD: do NOT exclude --lite/--delta/--only here. That exclusion is right
    # for a waiter asking "is THE full gate running", and I over-applied it to a different question. This
    # helper answers "is another run still using this summary path", and a --lite/--only run is using it
    # exactly as much as a full one -- excluding them let a LIVE partial run's reservation be reclaimed,
    # putting two writers on one summary. Caught by 4b.153/4b.155, whose live owners use --only file-size.
    #
    # MATCH AN EXACT ARGV ELEMENT, NUL-delimited -- never a substring of the joined cmdline. A searching
    # shell carries the pattern INSIDE an element (`pgrep -f agent-gate\.sh`), so no element ever ENDS in
    # `agent-gate.sh` and the searcher is excluded BY CONSTRUCTION. A `$$` exclusion list is both
    # insufficient (the pgrep child and command-substitution subshells match too) and a thing to forget.
    # Measured: the argv form found 7 gate processes and excluded the searching shell; the
    # substring-of-cmdline form counted 10, over-counting searchers.
    while IFS= read -r -d "" a; do
      _nargv=$((_nargv + 1))
      case "$a" in *agent-gate.sh) hit=1; break ;; esac
    done < "/proc/$p/cmdline"
    if [ "$hit" = 1 ]; then found=0; break; fi
    # THE SAME DEFECT ONE STEP DEEPER, and job 319 named only the outer half. `-r` SUCCEEDS on
    # /proc/<pid>/cmdline for a process that is exiting or mid-exec, and the file then reads EMPTY —
    # so the argv scan completes, matches nothing, and "no argv at all" was scored identically to
    # "argv read, no gate in it". Measured on this very lane while instrumenting #3473: two live
    # daemons read as unmeasurable for exactly this reason. Within THIS function's scope the benign
    # explanation does not apply — a kernel thread also has an empty cmdline but can never be inside
    # a `systemd-run --user` unit's cgroup — so empty here means exiting or mid-exec: unknown.
    if [ "$_nargv" = 0 ] && ! _pid_ruled_out "$p"; then _unknown=1; fi
  done < "$procs"
  # A "no gate" conclusion is only sound if we could inspect EVERY pid. Unknown is a THIRD answer and
  # the caller already refuses on it; ordering matters — an affirmative FIND (found=0) still wins,
  # because a gate we positively saw is not made doubtful by a sibling we could not read.
  if [ "$found" != 0 ] && [ "$_unknown" = 1 ]; then return 2; fi
  return $found
}

_foreign_reservation() {  # <path> -> live | free | unknown   (is <path> another launch's reserved summary?)
  local lk="$1.launch-lock" own own_unit own_pid own_start now_id _lkdir
  # ABSENCE IS ONLY CONCLUSIVE IF WE COULD LOOK (round-48 class audit, class 4: two-valued predicates).
  # `-L` and `-e` are two-valued, so they collapse "no such path" and "not permitted to look" onto the
  # same FALSE — and this branch answered `free`, the PERMISSIVE value, for BOTH. That is the worst
  # place in the file for it: `free` licenses THIS launch to take a path a LIVE peer may hold, i.e.
  # exactly the two-writers-on-one-summary outcome the reservation exists to prevent — and, per the
  # 4b.155 retraction, the rollback downstream is unreachable BECAUSE this pre-check is authoritative,
  # so nothing behind it would catch the mistake. A negative lookup means nothing unless the directory
  # is searchable, so require that AFFIRMATIVELY; otherwise answer `unknown`, which every caller
  # already refuses on. The composition degrades the safe way: an unsearchable GRANDparent makes `-d`
  # false and lands here too.
  if [ -L "$lk" ] || [ -e "$lk" ]; then :
  else
    _lkdir=${lk%/*}; [ "$_lkdir" = "$lk" ] && _lkdir=.
    if [ -d "$_lkdir" ] && [ -x "$_lkdir" ]; then printf 'free'; return 0; fi
    printf 'unknown'; return 0
  fi
  own=$(readlink "$lk" 2>/dev/null || true)
  [ -n "$own" ] || { printf 'unknown'; return 0; }
  own_unit=${own#*unit=}; own_unit=${own_unit%%|*}
  own_pid=${own#*pid=};   own_pid=${own_pid%%|*}
  own_start=${own#*start=}
  [ -n "$own_pid" ] || { printf 'unknown'; return 0; }
  case "$(_pid_state "$own_pid")" in
    exists)
      if _proc_is_zombie "$own_pid"; then :                      # a zombie cannot beat
      elif [ -z "$own_start" ]; then printf 'live'; return 0
      else
        now_id=$(_proc_identity "$own_pid" 2>/dev/null || true)
        if [ -z "$now_id" ]; then printf 'unknown'; return 0
        elif [ "$now_id" = "$own_start" ]; then printf 'live'; return 0
        fi
      fi ;;
    gone) : ;;
    *) printf 'unknown'; return 0 ;;
  esac
  # SCOPE STATE IS A SECONDARY SIGNAL, NEVER PRIMARY. By here the owner pid is affirmatively dead, a
  # zombie, or pid-reused. Ask whether a GATE still runs in that unit -- not whether the cgroup is
  # non-empty, which an orphan satisfies forever and which refused the path permanently.
  if [ -n "$own_unit" ]; then
    _unit_runs_a_gate "$own_unit"; case $? in
      0) printf 'live';    return 0 ;;   # a real full gate is still in there
      2) printf 'unknown'; return 0 ;;   # unmeasurable => refuse, do not reclaim
    esac
  fi
  printf 'free'
}
# Our artifact set: the beat destination, the log, and — if our own summary looks like another launch's
# beat destination — the path it would have been derived from.
# CHECK-THEN-LOCK IS A RACE, so the check and the acquire happen under ONE shared lock (roborev job 256).
# Two concurrent launches with `--summary x` and `--summary x.heartbeat` could BOTH observe no foreign
# reservation, BOTH acquire their distinct locks, and then overwrite each other's files — the detection
# added by job 251 closed the sequential case and left the concurrent one open.
#
# The lock has to be keyed on something the colliding launches SHARE, and every per-summary name differs
# by construction (x.launch-lock vs x.heartbeat.launch-lock, and likewise their mutexes). What they share
# is the DIRECTORY, and a launch's artifacts all live in it. One lock means no acquisition order and so
# no deadlock; it is held only across the check and the reservation, not across the gate's lifetime.
#
# `flock` rather than a lock file we would have to reclaim: the kernel releases it when the fd closes, so
# a launcher dying mid-check leaves nothing stale — the same reason the reclamation mutex uses it.
# ONE LOCK FOR ALL LAUNCHES, not one per directory (roborev job 256 follow-up, found by applying the
# new concurrency audit to my own fix). Keying on the SUMMARY's directory left the same check-then-lock
# hole one level out, because `--log` is INDEPENDENT of `--summary`: with
#   A: --summary /d1/a --log /d2/b        (dirlock /d1)
#   B: --summary /d2/b --log /d1/c        (dirlock /d2)
# the two take DIFFERENT locks while A's LOG is B's SUMMARY, so A's gate output destroys B's verdict.
# Measured: both launches accepted.
#
# Taking one lock per directory in sorted order would also work, but a single lock is simpler and cannot
# deadlock, and the critical section is a few file reads plus one symlink create — microseconds, so
# serialising unrelated launches costs nothing measurable against a 30-50 minute gate.
#
# Per-USER location, not shared /tmp: a lock every launch must take is a denial-of-service surface if any
# local user can hold it, and the canonical runtime directory is verified 0700 below. The 30s timeout means a held lock refuses
# loudly rather than hanging.
# REFUSE rather than fall back (roborev job 269, Medium). The paragraph above states the per-user
# requirement, and the code then fell back to TMPDIR-or-/tmp — a shared, PREDICTABLE, fixed-NAME
# path any local user can pre-create and hold, permanently refusing every detached launch on the
# box. (The two PRIVDIR mktemp -d calls above are NOT this bug: mktemp creates a fresh unguessable
# 0700 directory, so there is no fixed name to squat. The defect is a fixed name, not /tmp itself.)
# A fallback contradicting its own stated requirement is worse than no fallback: this script's
# posture everywhere else is to refuse rather than quietly deliver less than it promised, and
# `systemd-run --user` + lingering, both required above, already imply /run/user/$(id -u) exists.
# Measured AFFIRMATIVELY: a directory, owned by US, mode 0700. An UNMEASURABLE answer (no stat,
# unreadable parent) is UNKNOWN and refuses — never "probably fine".
#
# THE LOCK LOCATION IS CANONICAL AND IGNORES `XDG_RUNTIME_DIR` (roborev job 321, Medium). A lock is
# only global if every launch on the box picks the SAME path, and this one was selected through a
# caller-controlled variable — so two launchers with two different, individually VALID 0700 runtime
# directories take two different locks, and the artifact-set check plus reservation below stop being
# mutually exclusive. Two gates then pass check-and-reserve on overlapping artifacts, which is
# exactly what this lock exists to prevent.
#
# This is a defect and not an invoker's choice, because the script ITSELF used to advertise the
# route: the refusal below told the operator to "export XDG_RUNTIME_DIR to a 0700 dir you own". An
# operator following the printed remedy silently opted out of the global lock. Bypassable BY
# ACCIDENT is the line between a hazard we record and a defect we fix.
#
# `/run/user/$(id -u)` is the right canonical choice rather than an arbitrary one: `systemd-run
# --user` plus lingering are ALREADY required above, and both imply this directory exists — the
# paragraph above says so. So nothing is lost by ignoring the variable, and refusing when the
# canonical directory is unusable is the same posture as everywhere else in this script.
_rundir="/run/user/$(id -u)"
_rd_owner=""; _rd_mode=""
if _rd_stat="$(stat -Lc '%u %a' "$_rundir" 2>/dev/null)"; then
  _rd_owner="${_rd_stat%% *}"; _rd_mode="${_rd_stat##* }"
fi
if [ ! -d "$_rundir" ] || [ "$_rd_owner" != "$(id -u)" ] || [ "$_rd_mode" != 700 ]; then
  echo "gate-detached: no usable per-user runtime directory for the global launch lock." >&2
  echo "               tried: '$_rundir' (owner=${_rd_owner:-unmeasurable} mode=${_rd_mode:-unmeasurable}; need owner=$(id -u) mode=700)" >&2
  echo "               A lock every launch must take is a denial-of-service surface if any other" >&2
  echo "               local user can hold it, so this is NOT falling back to a shared directory." >&2
  echo "               Remedy: ensure a systemd user session exists (loginctl enable-linger '$(id -un)')," >&2
  echo "               so /run/user/$(id -u) is present and 0700. NOTE: exporting XDG_RUNTIME_DIR" >&2
  echo "               does NOT change this path any more (roborev job 321) — a lock selected by a" >&2
  echo "               caller-controlled variable is not global, and two launches could then reserve" >&2
  echo "               overlapping artifacts. Fix the canonical directory instead." >&2
  exit 69
fi
# VALIDATE `flock` BEFORE THE FIRST USE, NOT AFTER (roborev job 319, Low). This lock is taken by
# EVERY launch, and the only `command -v flock` check in this file sits in the RECLAMATION path far
# below — reachable only when a summary path is already contended. So on a systemd host without
# `flock` the bare failure of the call below was reported as "another launch holds the global launch
# lock": a false diagnosis pointing at a peer that does not exist, whose stated remedies (retry, use
# a distinct directory) can never work. A missing tool is a CAPABILITY refusal, so it exits 69 like
# its sibling above rather than 1 — the request was well-formed and this host cannot serve it.
if ! command -v flock >/dev/null 2>&1; then
  echo "gate-detached: 'flock' is not available, so the artifact-set check and the reservation" >&2
  echo "               cannot be made atomic and concurrent launches cannot be serialised." >&2
  echo "               This is a MISSING TOOL on this host, not contention with another launch." >&2
  echo "               Remedy: install it (util-linux: 'apt-get install -y util-linux')." >&2
  exit 69
fi
_dirlock="$_rundir/cqlite-gate-launch.lock"
if ! ( : >> "$_dirlock" ) 2>/dev/null; then
  echo "gate-detached: cannot create the directory lock '$_dirlock', so the artifact-set check and the" >&2
  echo "               reservation cannot be made atomic together. Refusing rather than racing another" >&2
  echo "               launch onto overlapping paths (#3473)." >&2
  exit 1
fi
exec 8>>"$_dirlock"
if ! flock -w 30 8; then
  echo "gate-detached: another launch holds the global launch lock ('$_dirlock')." >&2
  echo "               Refusing rather than racing it (#3473). Retry, or use a distinct directory." >&2
  exit 1
fi

# THE CHECK WAS ASYMMETRIC (roborev job 261). A launch verified whether ITS artifacts were another run's
# reserved SUMMARY — but only the summary was ever RESERVED, so nothing could detect the reverse. With
#   A: --summary /t/a --log /t/b     (reserves only /t/a.launch-lock)
#   B: --summary /t/b --log /t/c     (checks /t/b.heartbeat and /t/c — neither is /t/b)
# both were accepted while A wrote its LOG into B's SUMMARY. Measured: A exit 0, one lock created, B exit 0.
# The global lock from the previous fix serialises check-and-acquire; it cannot help when the thing being
# looked for was never recorded.
#
# So every write destination is reserved, and the check therefore becomes symmetric for free: a later
# launch asking "is any path I will write already reserved?" now finds logs and heartbeats too.
_ARTIFACTS="$SUMMARY
$SUMMARY.heartbeat
$LOGFILE"
# THE SUMMARY IS CHECKED FIRST, AND SAYS SO SPECIFICALLY (roborev job 261 follow-up). Once every write
# destination is reserved, a second launch on the SAME summary path trips the artifact-set check first —
# on the FIRST launch's heartbeat lock — and the generic message then claimed that path was "reserved as
# ITS summary", which is false: it is reserved as its HEARTBEAT. The reservation target records the owner,
# not the role, so the generic wording cannot know which. Checking the summary explicitly first restores
# the accurate diagnosis for the commonest case and leaves the generic one for genuine aliasing.
if [ "$(_foreign_reservation "$SUMMARY")" = live ]; then
  echo "gate-detached: the summary path '$SUMMARY' is already owned by a LIVE run." >&2
  echo "               Two gates on one path overwrite each other's summary and heartbeat, so neither" >&2
  echo "               could be polled reliably (#2874/#3473). Give this run a path of its own." >&2
  exit 1
fi
_collide=""
for _cand in "$SUMMARY.heartbeat" "$LOGFILE"; do
  case "$(_foreign_reservation "$_cand")" in
    live)    _collide="$_cand is already reserved by a LIVE run" ;;
    unknown) _collide="$_cand may be reserved by another run, and its owner could not be established" ;;
  esac
  [ -n "$_collide" ] && break
done
if [ -z "$_collide" ]; then
  case "$SUMMARY" in
    *.heartbeat)
      case "$(_foreign_reservation "${SUMMARY%.heartbeat}")" in
        live)    _collide="our summary '$SUMMARY' is the BEAT DESTINATION of a live run holding ${SUMMARY%.heartbeat}" ;;
        unknown) _collide="our summary '$SUMMARY' may be the beat destination of another run, whose owner could not be established" ;;
      esac ;;
  esac
fi
if [ -n "$_collide" ]; then
  echo "gate-detached: artifact-set collision — $_collide." >&2
  echo "               The reservation is named after the summary, so two launches can hold DIFFERENT" >&2
  echo "               locks and still destroy each other's files: one gate's heartbeat would overwrite" >&2
  echo "               the other's summary every interval, taking its terminal verdict with it (#3473)." >&2
  echo "               Give this run a summary path whose artifacts do not overlap another's." >&2
  exit 1
fi
_res_target="unit=$UNIT|pid=$$|start=$_res_ident"
_mutex="$_reserve.mutex"
if ! ln -s "$_res_target" "$_reserve" 2>/dev/null; then
  # CONTENDED. Classification and replacement must happen as ONE indivisible step, and an earlier
  # version of this block got that wrong in a way worth recording, because the wrong claim was
  # stated confidently in code, tests and docs: it reclaimed by renaming the stale link into an
  # mktemp scratch dir and called that a compare-and-swap. `mv` is NOT a compare-and-swap — it moves
  # whatever occupies the path and compares nothing with an expected value, and `rename()` offers no
  # such semantics. So two launchers that BOTH classified the old owner as dead could both succeed:
  # the first replaced the link and launched, and the second's delayed `mv` then moved the FIRST's
  # LIVE reservation away and installed its own. Both gates ran on one summary path — precisely the
  # outcome this lock exists to prevent. Demonstrated, not theorised (roborev job 203).
  #
  # `flock` is the fix rather than a mkdir mutex because the kernel releases it when the fd closes,
  # so a reclaimer that dies mid-sequence leaves NOTHING to time out — the stale-lock problem this
  # design already refused to reintroduce once.
  if ! command -v flock >/dev/null 2>&1; then
    echo "gate-detached: the summary path '$SUMMARY' is contended and 'flock' is unavailable, so" >&2
    echo "               reclamation cannot be serialised. Refusing rather than racing another" >&2
    echo "               launcher onto one summary path (#3473). Give this run its own path." >&2
    exit 1
  fi
  # Probe writability in a SUBSHELL, then exec. `exec` with no command applies its redirections to
  # the CURRENT shell permanently — so the obvious `exec 9>"$_mutex" 2>/dev/null` silenced stderr for
  # the whole rest of the script, and every later refusal printed NOTHING while still exiting
  # non-zero (caught by 4b.76, which asserts the refusal text and not merely the exit code). Append
  # rather than truncate: another launcher may hold a flock on this inode.
  if ! ( : >> "$_mutex" ) 2>/dev/null; then
    echo "gate-detached: cannot open the reclamation mutex '$_mutex'. Refusing (#3473)." >&2
    exit 1
  fi
  exec 9>>"$_mutex"
  if ! flock -w 30 9; then
    echo "gate-detached: another launcher holds the reclamation mutex for '$SUMMARY'." >&2
    echo "               Refusing rather than racing it (#3473). Retry, or use a distinct path." >&2
    exit 1
  fi
  # RE-READ under the mutex. Anything learned before acquiring it describes a tree that may already
  # have changed — the same point-of-use rule the log symlink recheck exists for.
  if ln -s "$_res_target" "$_reserve" 2>/dev/null; then
    :   # the path became free while we waited; we own it now
  else
    _own=$(readlink "$_reserve" 2>/dev/null || true)
    _own_unit=${_own#*unit=}; _own_unit=${_own_unit%%|*}
    _own_pid=${_own#*pid=};   _own_pid=${_own_pid%%|*}
    _own_start=${_own#*start=}
    # THE FAILURE MODE IS INVERTED (owner ruling on #3473-R6): `unknown` is a third value that
    # REFUSES, so a defect here can only ever produce a loud false refusal — never two gates writing
    # one summary path, which is the harm the lock exists to prevent. Noise, never blindness. Four
    # paths previously collapsed an unknown onto "dead, reclaim it": a non-numeric pid, `kill -0`
    # failing with EPERM rather than ESRCH, an unreadable identity for a pid that still exists, and
    # an empty unit field.
    _live=unknown
    case "$(_pid_state "$_own_pid")" in
      exists)
        if _proc_is_zombie "$_own_pid"; then
          _live=no                        # already exited; can never start a unit
        elif [ -z "$_own_start" ]; then
          _live=yes                       # nothing to disprove liveness with => treat as live
        else
          _now_id=$(_proc_identity "$_own_pid" 2>/dev/null || true)
          if [ -z "$_now_id" ]; then      _live=unknown   # present but unreadable
          elif [ "$_now_id" = "$_own_start" ]; then _live=yes
          else                            _live=no        # pid recycled: the owner is gone
          fi
        fi ;;
      gone)    _live=no ;;                # AFFIRMATIVE: no such process
      *)       _live=unknown ;;
    esac
    # ...then the unit, which keeps the lock meaningful after the launcher exits.
    #
    # ASK WHETHER A GATE RUNS, NOT WHETHER THE UNIT IS NON-INACTIVE (roborev job 316, Medium).
    # This site called `_unit_is_live`, whose 0 means "live OR unmeasurable" — and an ORPHANED
    # process keeps a unit active indefinitely, so an AFFIRMATIVELY DEAD owner was promoted back
    # to `yes` and this path was refused forever. That is the exact permanent-refusal defect
    # `_unit_runs_a_gate` exists to prevent, and `_foreign_reservation` above already asks it;
    # this call site was missed. AUDIT BY CALL SITE, NOT BY PRIMITIVE — fixing the helper did
    # not fix its callers.
    #
    # An UNMEASURABLE unit becomes `unknown`, NOT `yes`: `unknown` reaches the refusal below,
    # which NAMES a manual remedy, whereas `yes` asserts a live run exists and offers none.
    if [ "$_live" = no ] && [ -n "$_own_unit" ]; then
      _unit_runs_a_gate "$_own_unit"; case $? in
        0) _live=yes ;;                 # a real full gate is still in that cgroup
        1) : ;;                         # affirmatively no gate: keep `no`, reclaim below
        *) _live=unknown ;;             # unmeasurable => refuse-with-remedy, never a false `yes`
      esac
    fi
    # A link we cannot fully parse is not proof of death. Every such refusal names the manual
    # remedy, because a refusal with no way out would be job 196's permanent block in a new hat.
    if [ -z "$_own" ] || [ -z "$_own_pid" ] || [ -z "$_own_unit" ] || [ "$_live" = unknown ]; then
      echo "gate-detached: the reservation at '$_reserve' exists and its owner could NOT be" >&2
      echo "               established (owner='${_own:-<unreadable>}')." >&2
      echo "               Refusing rather than reclaiming a lock that may be live (#3473): two" >&2
      echo "               gates on one summary path corrupt each other silently, whereas this" >&2
      echo "               refusal is loud. If you have CONFIRMED no gate runs against this path," >&2
      echo "               remove that one file and retry; otherwise use a distinct path." >&2
      exit 1
    fi
    if [ "$_live" = yes ]; then
      echo "gate-detached: the summary path '$SUMMARY' is already owned by a LIVE run" >&2
      echo "               (unit=${_own_unit:-?} launcher-pid=${_own_pid:-?}). Two gates on one path" >&2
      echo "               overwrite each other's summary and heartbeat, so neither could be polled" >&2
      echo "               reliably (#2874/#3473). Give this run a summary path of its own." >&2
      exit 1
    fi
    # PROVABLY dead, and no other RECLAIMER can be here — the mutex guarantees that. A brand-new
    # launcher's first `ln -s` can still win the gap below; then ours fails and we refuse, which is
    # correct: exactly one process ever holds a successful `ln -s`.
    rm -f "$_reserve" 2>/dev/null || true
    if ! ln -s "$_res_target" "$_reserve" 2>/dev/null; then
      echo "gate-detached: another launcher took the reservation at '$_reserve' while it was being" >&2
      echo "               reclaimed. Refusing rather than racing it (#3473)." >&2
      exit 1
    fi
  fi
  flock -u 9 2>/dev/null || true
  exec 9>&-
fi
# RESERVE THE REMAINING WRITE DESTINATIONS (job 261). The summary lock carries the full reclaim
# semantics above; these are markers with the SAME owner target, so each self-heals by exactly the same
# rules when its owner dies. Created only after the summary lock is held, and rolled back together if any
# fails, so a partial set never outlives a refused launch.
# An ARRAY, never a space-joined string (roborev job 269, Medium). Iterating an unquoted string
# word-splits and GLOB-EXPANDS every element, so a space-bearing $SUMMARY made the rollback remove
# the wrong paths (leaving the real lock behind, so later launches refuse forever) and a glob
# character could expand onto a LIVE peer's reservation and delete it. This repository tracks 40
# space-bearing paths, so neither shape is hypothetical.
_extra_locks=()
_extra_ok=1
# ONE ROLLBACK FOR THE WHOLE SET, CALLED ON EVERY PATH THAT REFUSES AFTER ACQUIRING IT
# (#3769, roborev job 323 F3, Low). The reservation is a SET — the summary lock plus one marker per
# remaining artifact — and it was released PER SITE, so each site got a different subset right: the
# acquisition failure below rolled back all of it, the symlink refusal removed only `$_reserve`, and
# the truncation failure removed NONE. The heartbeat and log markers therefore outlived a launch that
# never happened. Litter rather than a deadlock, because `_foreign_reservation` + `_unit_runs_a_gate`
# reclaim a marker whose owner is gone — but litter in a caller-supplied in-repo path is a
# `tree-integrity` (#2926) FAIL on someone else's gate, which reads as an unrelated failure.
#
# So the set is released in ONE place and every refusal calls it. A per-site release is a list to keep
# complete, and three sites had already drifted into three different answers about one invariant.
#
# `_extra_locks` is emptied after the loop so a second call cannot re-remove a path a later launcher
# may by then legitimately own — this function must be idempotent, since a refusal path may run after
# the acquisition rollback already ran.
# ${ARRAY[@]+"${ARRAY[@]}"} and `rm -f --`, both for the reasons the acquisition loop states above:
# an empty array is UNBOUND under `set -u` on bash 3.2, and an unquoted expansion would word-split and
# glob a space-bearing path onto a live peer's reservation.
_release_reservations() {
  local _l
  for _l in ${_extra_locks[@]+"${_extra_locks[@]}"}; do rm -f -- "$_l" 2>/dev/null || true; done
  _extra_locks=()
  rm -f -- "$_reserve" 2>/dev/null || true
}
for _art in "$SUMMARY.heartbeat" "$LOGFILE"; do
  [ "$_art" = "$SUMMARY" ] && continue
  if ln -s "$_res_target" "$_art.launch-lock" 2>/dev/null; then
    _extra_locks+=("$_art.launch-lock")
  else
    # A STALE MARKER MUST BE REPLACED, NOT TOLERATED (roborev job 266, High). The first version treated
    # `free` here as "a stale marker of our own shape; harmless" — and that comment was FALSE, in the same
    # way the `host` comment that licensed `|| echo unknown` was false. Leaving it means THIS LIVE RUN's
    # heartbeat or log is represented by a DEAD owner, so a later launch reads the path as reclaimable,
    # takes it as its own summary, and two writers land on one file. Reproduced: a launch succeeded with
    # its heartbeat lock still naming pid 999999999.
    #
    # Remove-then-recreate is sufficient BECAUSE the global launch lock is held: no other launcher can
    # interleave, so the summary lock's compare-and-swap machinery is not needed here. That is what the
    # single lock buys.
    case "$(_foreign_reservation "$_art")" in
      free)
        rm -f "$_art.launch-lock" 2>/dev/null || true
        if ln -s "$_res_target" "$_art.launch-lock" 2>/dev/null; then
          _extra_locks+=("$_art.launch-lock")
        else
          _extra_ok=0; break            # could not take a path we proved reclaimable: refuse, never proceed
        fi ;;
      *) _extra_ok=0; break ;;
    esac
  fi
done
if [ "$_extra_ok" != 1 ]; then
  _release_reservations
  echo "gate-detached: could not reserve every write destination for this launch." >&2
  echo "               One of '$SUMMARY.heartbeat' or '$LOGFILE' is claimed by another run, so a gate" >&2
  echo "               here would overwrite its files. Refusing (#3473); use paths of your own." >&2
  exit 1
fi

# The reservation now exists, so a concurrent launch checking the artifact set will SEE it. Releasing the
# directory lock here — and not before — is what makes the check-and-acquire atomic.
flock -u 8 2>/dev/null || true
exec 8>&-

# NOW truncate the log: every refusal path is behind us, so this cannot destroy a previous log for
# a launch that never happens.
# Re-assert the log is not a symlink, because the early `-L` check answered about an EARLIER tree
# (roborev job 200). Between there and here a symlink can appear at this path — this script creates
# one at the reservation path, and a concurrent peer could create one at any path. `>` FOLLOWS a
# symlink, so without this the gate's output lands wherever the link points. Checking at the point
# of use is the only place the answer is still true.
if [ -L "$LOGFILE" ]; then
  echo "gate-detached: the log path '$LOGFILE' became a symlink after it was checked." >&2
  echo "               Refusing: '>' would follow it and write the gate's log somewhere the" >&2
  echo "               caller never named (#3473). Give --log a path of its own." >&2
  _release_reservations   # we own the whole set and are not launching; do not leak any of it
  exit 1
fi
( : > "$LOGFILE" ) 2>/dev/null || {
  echo "gate-detached: cannot truncate the log at '$LOGFILE' just before launch." >&2
  _release_reservations
  exit 1
}
# `env -i` IS LOAD-BEARING, AND THE DENY-LIST ALONE DID NOT COVER THIS (roborev job 211, High).
# A `--user` transient unit inherits the USER MANAGER's environment block — whatever
# `systemctl --user set-environment` / `import-environment` put there, plus whatever the manager
# started with. That is a channel the caller-side deny-list below cannot touch: it stops us
# FORWARDING `AGENT_GATE_WRAPPED`, `AGENT_GATE_SUMMARY_FILE` and friends, while the manager could
# supply the very same variables and change or short-circuit the gate's validation.
#
# Measured, both directions, on this host: with a manager variable set, the unit read it
# (`LEAK_PROBE=iamhere`); with `env -i` in front, the same probe read `ABSENT`.
#
# So the unit starts from an EMPTY environment and the wrapper script restores the caller's — which
# is the only environment we intend the gate to see. Absolute paths for `env` and `bash` because
# there is no PATH to find them with. This is the third channel in this change where a value I
# controlled on one path arrived by another (the others: .gitignore vs `_tree_excluded`, and a
# summary refusal I had added myself bypassing four guards) — the deny-list was never wrong, it was
# just not the only door.
# ${ARRAY[@]+"${ARRAY[@]}"}, NOT "${ARRAY[@]}" (roborev job 319, Medium). This script runs under
# `set -uo pipefail` (line 82) and the repo supports stock macOS /bin/bash 3.2, where an EMPTY array
# expanded as "${ARRAY[@]}" counts as UNBOUND and aborts. A bare `gate-detached.sh` — the ADVERTISED
# default full-gate invocation — leaves GATE_ARGS empty, so the headline use of this script was the
# one that broke, and only on the hosts we do not develop on: local bash is 5.2, where the same line
# is fine. Not a new class here — `agent-gate.sh` carries the identical fix and citation (job-2108).
# The `+` form expands to NOTHING when unset/empty and to the quoted elements otherwise, so it is
# also correct on bash 5; the rollback loop over `_extra_locks` gets it for the same reason.
if ! systemd-run --user --unit="$UNIT" --collect --same-dir --quiet \
     --property=StandardInput=null \
     --property="StandardOutput=append:$LOGFILE" \
     --property="StandardError=append:$LOGFILE" \
     "$_env_abs" -i "$_bash_abs" "$ENV_SCRIPT" ${GATE_ARGS[@]+"${GATE_ARGS[@]}"}; then
  echo "gate-detached: systemd-run failed to start unit $UNIT (see $LOGFILE)" >&2
  # THIS SITE IS GUARDED WHERE THE THREE ABOVE ARE NOT, AND THE ASYMMETRY IS THE POINT (#3769).
  # Those three run before anything was ever started, so releasing is unconditionally safe. Here
  # `systemd-run` has already spoken to the manager, and a non-zero exit does not prove nothing runs
  # under $UNIT — releasing is the PERMISSIVE act (it admits a peer onto these paths), so only an
  # AFFIRMATIVE terminal reading may license it. `_unit_is_live` returns 1 for exactly that reading
  # and 0 for live-or-unmeasurable, which is the same polarity every other reclamation site in this
  # file uses (jobs 205/241). An unmeasurable unit therefore keeps its reservation and self-heals by
  # the ordinary `_foreign_reservation` rules, which is the conservative direction: leftover litter,
  # never two writers on one summary.
  _unit_is_live "$UNIT" || _release_reservations
  exit 1
fi
CG=$(systemctl --user show "$UNIT" -p ControlGroup --value 2>/dev/null)

# ---------------------------------------------------------------------------
# VERIFY BY OUTCOME, not by permission model (roborev job 166).
#
# The gate starts its beater BEFORE it queues for the #1825 slot, so a first beat lands within
# a second or two even when the gate will then sit in the queue for 20 minutes. Requiring that
# beat is therefore a cheap END-TO-END proof that this gate is monitorable — it covers every
# reason publication could fail (ownership, sticky directories, ACLs, mount flags, SELinux, a
# full filesystem) without this script modelling any of them.
#
# On failure the unit is STOPPED rather than left running: an unmonitorable gate would burn
# 30-50 minutes and certify nothing, and the caller has no way to distinguish it from a slow
# queue. Better to refuse loudly than to hand back a URL to nowhere.
_hb_seen=0
# A WALL-CLOCK DEADLINE, because the iteration count is not a time bound (roborev job 228). This loop
# advertises "within 20s" and runs up to 40 iterations — but each iteration may call
# `gate-liveness.sh`, which BLOCKS for `interval + 5` (capped at 65s) whenever it needs to confirm
# whether a non-advancing beat is stalled. Forty of those is roughly SEVENTEEN MINUTES, so an
# unmonitorable gate could run far longer than the message promised. A count bounds work only when each
# unit of work is bounded, and this one is not.
#
# The message and the mechanism now agree, which is the actual fix: a diagnostic that states a limit
# the code does not enforce is the same class of defect as a comment asserting a property the code does
# not have.
_verify_deadline=$(( $(date +%s) + 20 ))
_new_rid=""
_i=0
while [ "$_i" -lt 40 ]; do
  if [ "$(date +%s)" -ge "$_verify_deadline" ]; then break; fi
  # (a) find OUR run-id from whichever of our two artifacts carries OUR NONCE first.
  #
  # The HEARTBEAT is checked FIRST, and that ordering is the whole point (roborev job 192). The
  # beater now starts before the tree-identity capture, but the SUMMARY is still written after it —
  # so requiring the summary to prove ownership reintroduced exactly the defect moving the beater
  # was meant to fix: a slow capture would stop a healthy, actively-beating gate. The beat carries
  # both the nonce and the run-id and appears within ~0.4s, so the monitorability proof needs no
  # summary at all.
  #
  # The summary is kept as a SECOND source because a very short run (a preflight refusal, a tiny
  # `--only`) can reach its verdict before any beat is published; then the summary is the only
  # artifact that exists. Either way the NONCE is what establishes ownership — never a run-id we
  # could not have predicted.
  # ONE IMMUTABLE SNAPSHOT PER ARTIFACT, not two opens (roborev job 223). The nonce and the run-id
  # were read by two separate `grep`s of a file a concurrent peer can rewrite between them, so the
  # launcher could pair ITS OWN nonce with a PEER's run-id — then accept the peer's heartbeat as proof
  # of monitorability and print a poll command bound to the wrong run. `gate-liveness.sh` already
  # solved exactly this for its own reads by copying each artifact once and deciding from the copy;
  # the launcher simply never inherited that discipline.
  #
  # Both facts must come from the SAME bytes: a snapshot that carries our nonce AND a run-id is proof
  # about one write, which two independent greps of a live file can never be.
  _snap_pair() {  # <file> -> prints the run-id iff this snapshot also carries OUR nonce
    local src="$1" snap rid
    snap=$(mktemp "$PRIVDIR/launchsnap.XXXXXX" 2>/dev/null) || return 1
    if ! cp -- "$src" "$snap" 2>/dev/null; then rm -f "$snap"; return 1; fi
    if grep -qxF "launch-nonce: $LAUNCH_NONCE" "$snap" 2>/dev/null; then
      rid=$(grep -m1 '^run-id: ' "$snap" 2>/dev/null || true)
      [ -n "$rid" ] && printf '%s' "${rid#run-id: }"
    fi
    rm -f "$snap"
    return 0
  }
  if [ -z "$_new_rid" ]; then
    _cur=$(_snap_pair "$_hbdest" 2>/dev/null || true)
    [ -n "$_cur" ] && _new_rid="$_cur"
  fi
  if [ -z "$_new_rid" ]; then
    _cur=$(_snap_pair "$SUMMARY" 2>/dev/null || true)
    [ -n "$_cur" ] && _new_rid="$_cur"
  fi
  # (b) ...and the heartbeat must carry THAT run-id. A pre-existing beat cannot satisfy this,
  #     whatever it contains, so an unreplaceable file no longer masks an unmonitorable launch.
  #     The match is FIXED-STRING and whole-line: the run-id is a mktemp PATH, and interpolating
  #     it into a regex broke on a TMPDIR containing `[` or `.`, so a REAL heartbeat would not
  #     match and the launcher would stop a healthy gate (job 178, Low).
  # ASK THE READER, do not re-implement its grammar (roborev job 198, Medium). Job 172 removed the
  # launcher's duplicate verdict grammar from the TERMINAL path; the same duplication survived here,
  # on the heartbeat path. Grepping only the nonce, run-id and the presence of `beat-epoch` accepted
  # beats the reader REJECTS — a `parent-check: kill0` beat, or one with invalid framing, interval or
  # epoch — so the launcher returned success while every advertised poll answered UNKNOWN.
  #
  # The nonce is still ours to check (the reader knows nothing about it), but the beat's VALIDITY and
  # the verdict are the reader's business. Exit 0 (COMPLETE) or 2 (RUNNING) both mean monitorable.
  if [ -n "$_new_rid" ] && [ -s "$_hbdest" ] \
     && grep -qxF "launch-nonce: $LAUNCH_NONCE" "$_hbdest" 2>/dev/null; then
    # --no-wait BOUNDS THIS CALL (roborev job 231). The deadline above is checked before invoking the
    # reader, which bounds nothing: the reader itself sleeps `interval + 5` (capped 65s) to confirm
    # whether a non-advancing beat is stalled, so a single call could overshoot the advertised 20s by
    # more than three times. A deadline that does not bound the blocking call inside the loop is not a
    # deadline. The launcher does not need that confirmation either — it asks only whether the reader
    # can answer about this run AT ALL, and --no-wait can only weaken a verdict to UNKNOWN, which this
    # loop already treats as "keep waiting".
    bash "$REPO_ROOT/scripts/gate-liveness.sh" "$SUMMARY" --run-id "$_new_rid" --no-wait >/dev/null 2>&1
    # A BEAT IS NOT PROOF THE UNIT STILL LIVES (roborev job 318, Medium). COMPLETE (0) is
    # self-sufficient: a terminal verdict exists. RUNNING (2) only says the reader could ANSWER
    # about this run — and a gate that published ONE heartbeat and then died before writing its
    # terminal summary answers RUNNING for the whole staleness window, so accepting 2 blindly made
    # this launcher exit 0 for a run whose verdict will NEVER arrive. Gate 2 on the unit, using the
    # AFFIRMATIVELY live, not "not affirmatively dead" (roborev job 319, Medium). An earlier revision
    # of this line used `_unit_is_live`, whose 0 also covers UNMEASURABLE, and justified it as "it can
    # only weaken, never invent, liveness". That is wrong HERE: 0 ACCEPTS the run, so an unreadable
    # unit state let the one-beat-then-dead case through — the very case this gate was added to
    # reject. Where 0 accepts, only an affirmative reading may grant. When the unit is NOT live we
    # deliberately do NOT break — control falls through to the settled-snapshot check below, which is
    # the path that can still find a terminal summary written in the gap.
    case "$?" in
      0) _hb_seen=1; break ;;                                  # COMPLETE — a verdict exists
      2) if _unit_accepts_as_monitorable "$UNIT"; then _hb_seen=1; break; fi ;; # RUNNING — live AND a gate is in the cgroup
    esac
  fi
  # If the unit already died, stop waiting — but take ONE SETTLED SNAPSHOT first (roborev job 213).
  # A fast gate (a preflight refusal, a tiny `--only`) can publish its terminal summary and exit in
  # the window between the artifact reads above and this check. `_new_rid` was then still empty, the
  # post-loop terminal check is guarded on it, and a launch that had actually produced a verdict was
  # REFUSED — and its unit stopped — on the grounds that no heartbeat appeared. Once the unit is
  # inactive the artifacts can no longer change, so re-deriving here races nothing.
  # `_unit_is_live`, NOT `is-active --quiet` (roborev job 272, Medium). `is-active --quiet` exits
  # NONZERO for every transitional state (activating, deactivating, reloading, refreshing) AND for
  # every query failure (no user bus, systemctl absent) — so a HEALTHY gate that had not yet settled,
  # or one we simply could not ask about, read as "the unit already died". The loop then broke early
  # and the launcher stopped a live gate as unmonitorable. This file ALREADY had the correct closed
  # grammar in `_unit_is_live` — only `inactive|failed` are affirmative terminal answers, everything
  # else is live-or-unmeasurable — and this site simply did not use it. Same class as the audited
  # two-valued file predicates, one level out: a multi-state signal read through a two-valued probe.
  #
  # AND DELIBERATELY *NOT* `_unit_runs_a_gate` HERE, which is the other predicate in this file and
  # the right one for RECLAIMING A FOREIGN reservation (roborev job 316). The two answer different
  # questions. There, an orphan keeping a unit active must not block a path forever, so "is a GATE
  # in the cgroup" is required. Here the subject is OUR OWN unit moments after `systemd-run`, and
  # the gate may not have exec'd yet — so `_unit_runs_a_gate` could answer "affirmatively no gate"
  # about a perfectly healthy launch and refuse it. Pick the predicate from the QUESTION, not from
  # which one is stricter.
  if ! _unit_is_live "$UNIT"; then
    # ONE IMMUTABLE SNAPSHOT, via _snap_pair (roborev job 272, Medium). This read the nonce and the
    # run-id with two SEPARATE greps against a LIVE file, so a concurrent direct gate rewriting it
    # between them pairs OUR nonce with the PEER's run-id, and the launcher then advertises a poll
    # command bound to someone else's run -- the exact failure `_snap_pair` was built for (job 190).
    # The primitive existed and was already used twice above; this site re-implemented the unsafe
    # version instead of calling it.
    if [ -z "$_new_rid" ]; then
      for _src in "$_hbdest" "$SUMMARY"; do
        _cur=$(_snap_pair "$_src" 2>/dev/null || true)
        [ -n "$_cur" ] && { _new_rid="$_cur"; break; }
      done
    fi
    break
  fi
  sleep 0.5
  _i=$((_i + 1))
done
# A gate that already reached a terminal verdict needs no heartbeat — but deciding THAT is
# exactly what gate-liveness.sh does, so ASK IT rather than re-implement its checks here
# (roborev job 172, Medium). The first version grepped `^RESULT: (PASS|FAIL|...)` with no end
# anchor and no framing validation, so `RESULT: PASSENGER` or a truncated block reported
# SUCCESS from the launcher while the reader would answer UNKNOWN — the same prefix-matching
# defect round 1 found in the reader, reproduced here as a SECOND implementation of the same
# grammar. One implementation, one grammar; the `--run-id` binding comes along for free.
if [ "$_hb_seen" -ne 1 ] && [ -n "$_new_rid" ]; then
  # THIS ONE CALL IS ALLOWED TO BLOCK, and that is the fix for a gate we were killing (roborev job 251).
  # Two earlier fixes interacted: job 221 made an unverifiable hostname ABSENT (so the clock domain reads
  # unproven), and job 231 put `--no-wait` on every launcher call (so the reader cannot take a second
  # sample). With an unproven clock the reader cannot judge freshness from the epoch and needs
  # PROGRESSION — two samples — so every stateless call returns UNKNOWN, the loop accepts only 0|2, and
  # after 20s the launcher STOPPED A HEALTHY, MONITORABLE GATE. On any host where `uname -n` fails, that
  # is every detached launch. Neither fix is wrong alone.
  #
  # The alternative was to track beat-seq progression across loop iterations inside the launcher — a
  # second implementation of the reader's progression grammar, which jobs 172 and 198 exist to prevent.
  # Instead the FAST loop stays bounded and non-blocking, and this single fallback, which runs at most
  # once, is permitted its confirmation wait: `interval + 5`, capped at 65s by the reader itself.
  # ACCEPT 0 OR 2, matching the fast loop (roborev job 256). As first written this was
  # `if bash ...; then`, which succeeds only on exit 0 — so the very case job 251 added it for, a healthy
  # gate with an unproven clock domain, returns 2 (RUNNING) and was DISCARDED, and the launcher stopped
  # the unit anyway. The fix did not work for the case it was written for.
  #
  # Worse, my verification of it was invalid: 4b.142 used `--only fmt`, which finishes in about a second,
  # so the TERMINAL SUMMARY answered COMPLETE=0 and this branch never ran. A test can exercise a
  # different path than its name claims and still pass — which is why the case now uses a component slow
  # enough that liveness, not completion, is what answers.
  bash "$REPO_ROOT/scripts/gate-liveness.sh" "$SUMMARY" --run-id "$_new_rid" >/dev/null 2>&1
  # Same distinction as the in-loop probe above (roborev jobs 318 then 319, Medium): RUNNING is not
  # evidence the unit survives, so it is accepted only on an AFFIRMATIVELY live unit — an unmeasurable
  # one refuses, because 0 here ACCEPTS. Here there is no fall-through to gain — this is the last
  # check — so anything other than a live unit leaves `_hb_seen` at 0 and the launcher refuses, which
  # is the correct outcome: no verdict is coming.
  case "$?" in
    0) _hb_seen=1 ;;                              # COMPLETE — a verdict exists
    2) _unit_accepts_as_monitorable "$UNIT" && _hb_seen=1 ;;   # RUNNING — live AND a gate in the cgroup
  esac
fi
if [ "$_hb_seen" -ne 1 ]; then
  # AND THIS SITE DELIBERATELY DOES *NOT* RELEASE THE RESERVATION (#3769). It is the one refusal path
  # AFTER a successful launch: a gate really started, may have written into these artifacts, and
  # `systemctl stop` is asynchronous — its processes can still be draining when this returns. Handing
  # the paths to a peer in that window is exactly the two-writers-on-one-summary failure the whole
  # reservation exists to prevent, and it is a worse outcome than the litter. The set self-heals by
  # the ordinary `_foreign_reservation` + `_unit_runs_a_gate` reclamation once the owner is gone.
  systemctl --user stop "$UNIT" >/dev/null 2>&1 || true
  echo "gate-detached: the gate started but published no readable liveness to '$_hbdest' within 20s," >&2
  echo "               plus one confirmation of up to 65s where the clock domain is unproven," >&2
  echo "               so its liveness would be unreadable and every poll would answer UNKNOWN." >&2
  echo "               The unit has been STOPPED rather than left to burn 30-50 minutes" >&2
  echo "               certifying nothing. See $LOGFILE for what the gate itself reported." >&2
  echo "               Common causes: the summary directory is not writable by this user, or" >&2
  echo "               an existing heartbeat there cannot be replaced (sticky directory owned" >&2
  echo "               by someone else). (#3473)" >&2
  exit 1
fi

# The advertised command carries --run-id (we KNOW it, and this script tells everyone else to
# pass it whenever they do — a peer reusing the summary path would otherwise be mistaken for
# this launch) and is shell-escaped, so a path containing a space or a metacharacter stays
# valid (roborev job 172, Medium).
POLL_CMD=$(printf 'bash %q %q --run-id %q' "$REPO_ROOT/scripts/gate-liveness.sh" "$SUMMARY" "$_new_rid")
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
  $POLL_CMD
stop it with:
  systemctl --user stop $UNIT
==== END GATE DETACHED ====
EOF
exit 0

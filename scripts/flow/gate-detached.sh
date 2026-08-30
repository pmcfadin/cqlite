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
  69  this host cannot run a cgroup-detached gate (no working `systemd-run --user`)

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
# shellcheck disable=SC2317  # runs via the EXIT trap
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
printf 'rm -f -- %q\n' "$ENV_SCRIPT" >> "$ENV_SCRIPT"
printf 'exec bash %q "$@"\n' "$REPO_ROOT/scripts/agent-gate.sh" >> "$ENV_SCRIPT"

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
_c_log=$(_canon "$LOGFILE"); _c_sum=$(_canon "$SUMMARY"); _c_hb=$(_canon "$SUMMARY.heartbeat")
_alias_of=""
[ "$_c_log" = "$_c_sum" ] && _alias_of="the summary"
[ -z "$_alias_of" ] && [ "$_c_log" = "$_c_hb" ] && _alias_of="the heartbeat"
if [ -z "$_alias_of" ] && [ -e "$LOGFILE" ]; then
  [ -e "$SUMMARY" ] && [ "$LOGFILE" -ef "$SUMMARY" ] && _alias_of="the summary (same inode)"
  [ -z "$_alias_of" ] && [ -e "$SUMMARY.heartbeat" ] && [ "$LOGFILE" -ef "$SUMMARY.heartbeat" ] \
    && _alias_of="the heartbeat (same inode)"
fi
if [ -n "$_alias_of" ]; then
  echo "gate-detached: the log path '$LOGFILE' is $_alias_of. Refusing: the gate rewrites its" >&2
  echo "               summary with '>' and the beater publishes by rename, so one of them would" >&2
  echo "               destroy the other's file and the advertised log would hold the wrong data" >&2
  echo "               (#3473). Give --log a path of its own." >&2
  exit 1
fi
if [ -L "$LOGFILE" ] || { [ -e "$LOGFILE" ] && [ ! -f "$LOGFILE" ]; }; then
  echo "gate-detached: log path '$LOGFILE' is a symlink or not a regular file — refusing to" >&2
  echo "               truncate it (#3473)." >&2
  exit 1
fi
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
# Both names keep the `.heartbeat.tmp.` prefix so they fall inside the gate's existing
# tree-integrity carve-out, in case a concurrent gate captures the tree mid-probe.
_hbprobe=$(mktemp "$SUMMARY.heartbeat.tmp.probeXXXXXX" 2>/dev/null) || {
  echo "gate-detached: cannot create a file in '$_sumdir', so neither the gate's summary nor" >&2
  echo "               the liveness heartbeat could be published there — every poll of this" >&2
  echo "               gate would answer UNKNOWN. Refusing to launch an unmonitorable gate," >&2
  echo "               rather than burn 30-50 minutes certifying nothing (#3473)." >&2
  exit 1
}
_hbprobe2="$_hbprobe.renamed"
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

_proc_identity() {
  local raw rest ls
  raw=$(cat "/proc/$1/stat" 2>/dev/null)
  if [ -n "$raw" ]; then
    rest="${raw##*) }"
    # shellcheck disable=SC2086
    set -- $rest
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
if ! ln -s "unit=$UNIT|pid=$$|start=$_res_ident" "$_reserve" 2>/dev/null; then
  _own=$(readlink "$_reserve" 2>/dev/null || true)
  _own_unit=${_own#*unit=}; _own_unit=${_own_unit%%|*}
  _own_pid=${_own#*pid=};   _own_pid=${_own_pid%%|*}
  _own_start=${_own#*start=}
  _live=no
  # The LAUNCHER first: it is alive throughout its own acquisition, which is what the previous
  # designs' window problem reduced to. Its pid is pinned by a start identity so a recycled pid
  # cannot make a finished run look live.
  case "$_own_pid" in
    ''|*[!0-9]*) ;;
    *) if kill -0 "$_own_pid" 2>/dev/null; then
         if [ -n "$_own_start" ]; then
           [ "$(_proc_identity "$_own_pid")" = "$_own_start" ] && _live=yes
         else
           _live=yes
         fi
       fi ;;
  esac
  # ...then the unit, which is what keeps the lock meaningful after the launcher exits.
  if [ "$_live" = no ] && [ -n "$_own_unit" ] \
     && systemctl --user is-active --quiet "$_own_unit" 2>/dev/null; then
    _live=yes
  fi
  # An UNREADABLE or unparseable link is not proof of death either — refuse rather than guess.
  if [ -z "$_own" ] || [ -z "$_own_pid" ]; then
    echo "gate-detached: the reservation at '$_reserve' exists but its owner cannot be read." >&2
    echo "               Refusing rather than reclaiming a lock that may be live (#3473)." >&2
    exit 1
  fi
  if [ "$_live" = yes ]; then
    echo "gate-detached: the summary path '$SUMMARY' is already owned by a LIVE run" >&2
    echo "               (unit=${_own_unit:-?} launcher-pid=${_own_pid:-?}). Two gates on one path" >&2
    echo "               overwrite each other's summary and heartbeat, so neither could be polled" >&2
    echo "               reliably (#2874/#3473). Give this run a summary path of its own." >&2
    exit 1
  fi
  # PROVABLY dead. Claim the reclamation by renaming the link away — atomic, so only one process
  # proceeds and a loser refuses rather than racing.
  _stale=$(mktemp -d "$_reserve.stale.XXXXXX" 2>/dev/null) || {
    echo "gate-detached: cannot create a scratch name to reclaim the stale reservation." >&2
    exit 1
  }
  if ! mv "$_reserve" "$_stale/" 2>/dev/null; then
    rm -rf "$_stale" 2>/dev/null || true
    echo "gate-detached: another launcher is reclaiming the stale reservation at '$_reserve'." >&2
    echo "               Refusing rather than racing it (#3473). Retry, or use a distinct path." >&2
    exit 1
  fi
  rm -rf "$_stale" 2>/dev/null || true
  ln -s "unit=$UNIT|pid=$$|start=$_res_ident" "$_reserve" 2>/dev/null || {
    echo "gate-detached: cannot create the reservation at '$_reserve'." >&2
    echo "               Refusing rather than racing another launcher (#3473)." >&2
    exit 1
  }
fi

# NOW truncate the log: every refusal path is behind us, so this cannot destroy a previous log for
# a launch that never happens.
( : > "$LOGFILE" ) 2>/dev/null || {
  echo "gate-detached: cannot truncate the log at '$LOGFILE' just before launch." >&2
  exit 1
}
if ! systemd-run --user --unit="$UNIT" --collect --same-dir --quiet \
     --property=StandardInput=null \
     --property="StandardOutput=append:$LOGFILE" \
     --property="StandardError=append:$LOGFILE" \
     bash "$ENV_SCRIPT" "${GATE_ARGS[@]}"; then
  echo "gate-detached: systemd-run failed to start unit $UNIT (see $LOGFILE)" >&2
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
_new_rid=""
_i=0
while [ "$_i" -lt 40 ]; do
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
  if [ -z "$_new_rid" ] && grep -qxF "launch-nonce: $LAUNCH_NONCE" "$_hbdest" 2>/dev/null; then
    _cur=$(grep -m1 '^run-id: ' "$_hbdest" 2>/dev/null || true)
    [ -n "$_cur" ] && _new_rid="${_cur#run-id: }"
  fi
  if [ -z "$_new_rid" ] && grep -qxF "launch-nonce: $LAUNCH_NONCE" "$SUMMARY" 2>/dev/null; then
    _cur=$(grep -m1 '^run-id: ' "$SUMMARY" 2>/dev/null || true)
    [ -n "$_cur" ] && _new_rid="${_cur#run-id: }"
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
    bash "$REPO_ROOT/scripts/gate-liveness.sh" "$SUMMARY" --run-id "$_new_rid" >/dev/null 2>&1
    case "$?" in
      0|2) _hb_seen=1; break ;;   # COMPLETE or RUNNING — the reader can answer about this run
    esac
  fi
  # If the unit already died, stop waiting — the log will say why.
  systemctl --user is-active --quiet "$UNIT" 2>/dev/null || break
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
  if bash "$REPO_ROOT/scripts/gate-liveness.sh" "$SUMMARY" --run-id "$_new_rid" >/dev/null 2>&1; then
    _hb_seen=1   # exit 0 == COMPLETE, and only for THIS run
  fi
fi
if [ "$_hb_seen" -ne 1 ]; then
  systemctl --user stop "$UNIT" >/dev/null 2>&1 || true
  echo "gate-detached: the gate started but published NO heartbeat to '$_hbdest' within 20s," >&2
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

#!/usr/bin/env bash
#
# ab-throughput.sh -- the interleaved, paired A/B throughput driver for #3649.
#
# WHAT IT DOES
# ------------
# Builds two commits into two SEPARATE worktrees with two SEPARATE cargo target
# directories, then runs `flight-loadgen --shape full` server-direct against
# each arm's own `cqlite-flight` over loopback, INTERLEAVED:
#
#     base r1, head r1, base r2, head r2, ... base rN, head rN
#
# and writes one JSONL file per (arm, replicate) plus a manifest that
# `analyze-ab.py` consumes.
#
# WHY INTERLEAVED, AND WHY SEPARATE TARGET DIRS
# ---------------------------------------------
# Interleaving is the whole point. Running all of one arm and then all of the
# other aliases every drift the host experiences during the session -- another
# tenant, a thermal excursion, page-cache state -- onto whichever arm ran
# second. #3649 exists because a proxy bench could not separate the branch from
# the box. Interleaving makes replicate i of base and replicate i of head
# neighbours in time, so the per-pair ratio cancels drift that is slow relative
# to one pair.
#
# Separate `--target-dir` per commit is required by the issue: a shared target
# directory makes each arm rebuild over the other's artifacts, which costs time
# and, worse, leaves a stale mixed-provenance tree if a build is interrupted.
#
# WHAT IT REFUSES TO DO
# ---------------------
# It fails closed, loudly and with a named cause, on: an unresolvable arm ref, a
# build that did not produce both binaries, an absent or unparseable ticket
# template, a corpus below the stated minimum size, a server that never bound
# the port, a server that did not die before the next replicate binds it, a
# reused worktree that is dirty, a replicate whose JSONL carries any request
# error, and a replicate that produced anything other than ONE STEP RECORD PER
# DECLARED RAMP STEP. It never continues silently with
# fewer replicates than requested: the manifest is rewritten after EVERY
# completed run, so a session that died early leaves a truthful short manifest
# and `analyze-ab.py` refuses it with `replicate-shortfall` rather than
# analysing a short session as if it were the requested one.
#
# ANCHORING
# ---------
# Every line this script writes to stdout or stderr begins with `AB-3649: `, and
# every dynamic value is control-character sanitized, for the reason given at
# length in analyze-ab.py: this output gets pasted into a GitHub issue and must
# never be mistakable for a gate or review certification. Child-process output
# (cargo, the server, the load generator) goes to log files under the work
# directory and is never relayed to these streams.
#
# EXIT CODES
#   0  every requested replicate completed and the manifest is complete
#   2  the session aborted -- the manifest records what did complete
#   3  usage error (also what --help exits with)
set -euo pipefail

PREFIX='AB-3649: '
DRIVER_VERSION='ab-throughput.sh/v2'

# The Python helpers live in ab_driver_support.py, as an EXECUTABLE FILE. They
# used to be inline heredocs, which meant nothing could run them without a rig --
# so the record validator was covered by no test at all, which is how it came to
# hard-code a SINGLE step record while this driver advertised --ramp. Resolved
# before any argument is validated, because --ramp validation calls it.
SUPPORT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/ab_driver_support.py"

# ---------------------------------------------------------------------------
# Anchored, sanitized emission.
# ---------------------------------------------------------------------------
sanitize() {
  # Every control character -- newline included -- becomes a visible `?`, so no
  # value can emit a line without the prefix. Deliberately lossy and deliberately
  # not clever: a mangled-looking field is a signal, and the one property that
  # must hold is that the anchor cannot be broken. Bytes >= 0x80 pass through, so
  # a UTF-8 path is still readable.
  printf '%s' "$1" | LC_ALL=C tr '\000-\037\177' '?'
}
say()  { printf '%s%s\n' "$PREFIX" "$(sanitize "$*")"; }
warn() { printf '%s%s\n' "$PREFIX" "$(sanitize "$*")" >&2; }

die() { # <cause> <detail>
  warn "cause $1"
  warn "cause-detail $2"
  say "session ABORTED cause $1"
  # An affirmatively false statement in a diagnostic is worse than silence, so
  # the manifest is only claimed when writing it ACTUALLY SUCCEEDED and the file
  # is on disk. An abort before the run directory exists writes nothing, and
  # says so.
  if [ "${LEDGER_ARMED:-0}" = "1" ] && write_manifest 2>/dev/null \
     && [ "${PROMOTED:-0}" = "1" ] && promote_ledger \
     && [ -f "${RUN_DIR:-}/manifest.json" ]; then
    say "manifest $RUN_DIR/manifest.json records the runs that did complete"
  elif [ -f "${RUN_DIR:-}/manifest.json" ]; then
    say "manifest NOT WRITTEN -- this abort happened before any replicate completed; the manifest present in $RUN_DIR belongs to an EARLIER session and has NOT been modified"
  else
    say "manifest NOT WRITTEN -- this abort happened before a manifest could be produced"
  fi
  exit 2
}

usage_error() {
  warn "usage-error $1"
  print_usage >&2
  exit 3
}

print_usage() {
  local w
  while IFS= read -r w; do printf '%s%s\n' "$PREFIX" "$w"; done <<'USAGE'
ab-throughput.sh [options]

  --corpus <dir>            SSTable root served as `cqlite-flight --data-dir` (required)
  --ticket-template <file>  connector-shaped FlightTicket JSON               (required)
  --base-ref <rev>          BASE arm commit                     (default cfa93fe99^)
  --head-ref <rev>          HEAD arm commit                     (default cfa93fe99)
  --replicates <N>          interleaved replicate pairs (floor 5)        (default 7)
  --work-dir <dir>          worktrees, target dirs, results       (default /data/ab-3649)
  --repo <dir>              repository to build from       (default this checkout)
  --max-concurrent-scans <n>  cqlite-flight admission ceiling, pinned on BOTH
                            arms and asserted against the server's own startup
                            line. **REQUIRED** -- see the note below
  --batch-size <n>          rows per Arrow record batch, both arms   (default 8192)
  --max-batch-bytes <n>     Arrow payload bytes per batch, both arms  (server default)
  --admission-wait-timeout-ms <n>  admission wait before a shed       (server default)
  --shape <s>               flight-loadgen shape                      (default full)
  --ramp <list>             flight-loadgen ramp                          (default 1)
  --step-duration <d>       per-step hold                              (default 60s)
  --port <n>                loopback port for the server              (default 8815)
  --server-cpus <list>      taskset list for the server         (default unpinned)
  --client-cpus <list>      taskset list for the load generator (default unpinned)
  --min-corpus-bytes <n>    refuse below this many Data.db bytes  (default 268435456)
  --min-sstables <n>        refuse below this many Data.db files         (default 2)
  --merge-path <arm>        CQLITE_FLIGHT_MERGE_PATH for BOTH servers  (default merge;
                            auto | merge | bypass -- see the #3058 note below)
  --rows-declared <n>       corpus row count, recorded not measured  (default none)
  --no-prewarm              skip the per-replicate warming pass
  --control <label>         mark this session a CONTROL, not a measurement; the
                            label is recorded and the analyzer refuses to let its
                            verdict be read as discharging the AC
  --base-server-extra <s>   extra server flags for the BASE arm only  (control use)
  --head-server-extra <s>   extra server flags for the HEAD arm only  (control use)
  --temperature <t>         warm | cold                                (default warm)
  -h, --help                print this and exit 3

--max-concurrent-scans is REQUIRED because admission control (#2420, WS4;
cqlite-flight/src/cli.rs:59-73) sheds a `do_get` past the ceiling with gRPC
UNAVAILABLE. Unset, the ceiling is DERIVED from available parallelism, so two
runs on differently-loaded boxes get different ceilings and a ramp step above
one measures THE ADMISSION CEILING -- which looks like a plateau, exactly the
shape someone would misread as saturation. Pin it AT OR ABOVE the top of your
ramp.

Then, for a --ramp 1 session:
  python3 analyze-ab.py --single-stream <work-dir>/results/manifest.json
and for a concurrency-ramp session:
  python3 analyze-ab.py --utilization  <work-dir>/results/manifest.json
USAGE
}

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
CORPUS=''
TICKET_TEMPLATE=''
BASE_REF='cfa93fe99^'
HEAD_REF='cfa93fe99'
REPLICATES=7
WORK_DIR='/data/ab-3649'
REPO=''
SHAPE='full'
RAMP='1'
STEP_DURATION='60s'
PORT=8815
SERVER_CPUS=''
CLIENT_CPUS=''
MIN_CORPUS_BYTES=268435456
MIN_SSTABLES=2
MERGE_PATH='merge'
MAX_CONCURRENT_SCANS=''
BATCH_SIZE=8192
MAX_BATCH_BYTES=''
ADMISSION_WAIT_TIMEOUT_MS=''
ROWS_DECLARED=''
PREWARM=1
TEMPERATURE='warm'
CONTROL=''
BASE_SERVER_EXTRA=''
HEAD_SERVER_EXTRA=''

# EVERY value-taking option, in one list. A `shift 2` with one argument left
# consumes past the end and exits 1 with an unanchored bash error instead of the
# documented usage error -- so the presence of a value is checked ONCE here
# rather than in each arm below, where the next option added would be the next
# one to miss it. `scripts`-side drift is caught by a structural case in
# selftest-analyze.sh that requires every `shift 2` arm to appear in this list.
VALUE_OPTS="--corpus --ticket-template --base-ref --head-ref --replicates \
--work-dir --repo --shape --ramp --step-duration --port --server-cpus \
--client-cpus --min-corpus-bytes --min-sstables --merge-path \
--max-concurrent-scans --batch-size --max-batch-bytes \
--admission-wait-timeout-ms --rows-declared --temperature --control \
--base-server-extra --head-server-extra"

while [ "$#" -gt 0 ]; do
  case " $VALUE_OPTS " in
    *" $1 "*)
      [ "$#" -ge 2 ] || usage_error "$1 requires a value" ;;
  esac
  case "$1" in
    --corpus)            CORPUS="${2:-}";           shift 2 ;;
    --ticket-template)   TICKET_TEMPLATE="${2:-}";  shift 2 ;;
    --base-ref)          BASE_REF="${2:-}";         shift 2 ;;
    --head-ref)          HEAD_REF="${2:-}";         shift 2 ;;
    --replicates)        REPLICATES="${2:-}";       shift 2 ;;
    --work-dir)          WORK_DIR="${2:-}";         shift 2 ;;
    --repo)              REPO="${2:-}";             shift 2 ;;
    --shape)             SHAPE="${2:-}";            shift 2 ;;
    --ramp)              RAMP="${2:-}";             shift 2 ;;
    --step-duration)     STEP_DURATION="${2:-}";    shift 2 ;;
    --port)              PORT="${2:-}";             shift 2 ;;
    --server-cpus)       SERVER_CPUS="${2:-}";      shift 2 ;;
    --client-cpus)       CLIENT_CPUS="${2:-}";      shift 2 ;;
    --min-corpus-bytes)  MIN_CORPUS_BYTES="${2:-}"; shift 2 ;;
    --min-sstables)      MIN_SSTABLES="${2:-}";     shift 2 ;;
    --merge-path)        MERGE_PATH="${2:-}";       shift 2 ;;
    --max-concurrent-scans)      MAX_CONCURRENT_SCANS="${2:-}";      shift 2 ;;
    --batch-size)                BATCH_SIZE="${2:-}";                shift 2 ;;
    --max-batch-bytes)           MAX_BATCH_BYTES="${2:-}";           shift 2 ;;
    --admission-wait-timeout-ms) ADMISSION_WAIT_TIMEOUT_MS="${2:-}"; shift 2 ;;
    --rows-declared)     ROWS_DECLARED="${2:-}";    shift 2 ;;
    --no-prewarm)        PREWARM=0;                 shift ;;
    --control)           CONTROL="${2:-}";          shift 2 ;;
    --base-server-extra) BASE_SERVER_EXTRA="${2:-}"; shift 2 ;;
    --head-server-extra) HEAD_SERVER_EXTRA="${2:-}"; shift 2 ;;
    --temperature)       TEMPERATURE="${2:-}";      shift 2 ;;
    -h|--help)           print_usage; exit 3 ;;
    *)                   usage_error "unrecognised argument: $1" ;;
  esac
done

[ -n "$CORPUS" ]          || usage_error "--corpus is required"
[ -n "$TICKET_TEMPLATE" ] || usage_error "--ticket-template is required"
case "$REPLICATES" in ''|*[!0-9]*) usage_error "--replicates must be a positive integer" ;; esac
[ "$REPLICATES" -ge 5 ] || usage_error \
  "--replicates must be at least 5. At n<=3 a 10000-draw percentile bootstrap is NOT an interval: the all-minimum resample has probability 1/n^n, which at n=3 is 3.7% and exceeds the 2.5% tail, so the reported bounds are exactly (min, max) of the observed ratios and three identical pairs yield a ZERO-WIDTH interval. 7 is the recommendation -- see RUNBOOK.md step 5"
case "$PORT" in ''|*[!0-9]*) usage_error "--port must be an integer" ;; esac
case "$MIN_CORPUS_BYTES" in ''|*[!0-9]*) usage_error "--min-corpus-bytes must be an integer" ;; esac
case "$TEMPERATURE" in warm|cold) ;; *) usage_error "--temperature must be warm or cold" ;; esac
case "$MIN_SSTABLES" in ''|*[!0-9]*) usage_error "--min-sstables must be an integer" ;; esac
if [ -n "$ROWS_DECLARED" ]; then
  case "$ROWS_DECLARED" in ''|*[!0-9]*) usage_error "--rows-declared must be a plain integer with no separators (3999890, not 3,999,890)" ;; esac
fi
case "$MERGE_PATH" in auto|merge|bypass) ;; *) usage_error "--merge-path must be auto, merge or bypass" ;; esac
[ -n "$MAX_CONCURRENT_SCANS" ] || usage_error \
  "--max-concurrent-scans is required: unpinned, cqlite-flight DERIVES the admission ceiling from available parallelism (#3225), so it is a property of the box rather than of the experiment, and a ramp step above it measures the admission ceiling instead of merge throughput"
case "$MAX_CONCURRENT_SCANS" in ''|*[!0-9]*) usage_error "--max-concurrent-scans must be a positive integer" ;; esac
[ "$MAX_CONCURRENT_SCANS" -ge 1 ] || usage_error "--max-concurrent-scans must be at least 1"
case "$BATCH_SIZE" in ''|*[!0-9]*) usage_error "--batch-size must be a positive integer" ;; esac
[ "$BATCH_SIZE" -ge 1 ] || usage_error \
  "--batch-size must be at least 1: cqlite-flight silently clamps 0 to one row per batch, so the value the manifest records would not be the value the server used -- and the Arrow batch row cap is the very mechanism #2820 changed, so it is the last parameter this measurement can afford to lose"
if [ -n "$MAX_BATCH_BYTES" ]; then
  case "$MAX_BATCH_BYTES" in ''|*[!0-9]*) usage_error "--max-batch-bytes must be an integer" ;; esac
fi
if [ -n "$ADMISSION_WAIT_TIMEOUT_MS" ]; then
  case "$ADMISSION_WAIT_TIMEOUT_MS" in ''|*[!0-9]*) usage_error "--admission-wait-timeout-ms must be an integer" ;; esac
fi
# EVERY element is validated, through the same parser the analyzer's helper
# uses: `sort -n` ranks a non-numeric token as zero, so a max-only check passed
# `--ramp 1,abc` straight into both release builds. The helper also refuses a
# ramp that maps to no analyzer section.
[ -f "$SUPPORT" ] || usage_error \
  "ab_driver_support.py is not beside this script at $SUPPORT; the driver's ramp validator, record validator and startup parser all live there"
RAMP_INFO="$(python3 "$SUPPORT" validate-ramp "$RAMP")" \
  || usage_error "--ramp $RAMP was refused (the cause is named above)"
RAMP_TOP="${RAMP_INFO%% *}"
RAMP_SECTION="${RAMP_INFO##* }"
# The step duration is normalised HERE, before anything is built, through the
# same grammar flight-loadgen uses -- so a value it would accept can never be
# refused later by the analyzer, and a value it would reject costs nothing more
# than a usage error. The canonical seconds go into the manifest as the SINGLE
# source; the raw string is kept for display only.
STEP_DURATION_SECONDS="$(python3 "$SUPPORT" parse-duration "$STEP_DURATION")" \
  || usage_error "--step-duration $STEP_DURATION was refused (the cause is named above)"
if [ "$RAMP_TOP" -gt "$MAX_CONCURRENT_SCANS" ]; then
  usage_error "--ramp tops out at $RAMP_TOP but --max-concurrent-scans is $MAX_CONCURRENT_SCANS; every request past the ceiling waits and is then shed with gRPC UNAVAILABLE (#2420), so that step would measure the admission ceiling. Raise the pin to at least $RAMP_TOP"
fi
if [ -n "$SERVER_CPUS" ] || [ -n "$CLIENT_CPUS" ]; then
  [ -n "$SERVER_CPUS" ] && [ -n "$CLIENT_CPUS" ] || usage_error \
    "--server-cpus and --client-cpus must be given together: pinning one and not the other measures the load generator competing with the server"
  command -v taskset >/dev/null 2>&1 || usage_error "taskset is not on PATH but CPU pinning was requested"
fi

# Anchoring is a property of EVERY line, including the ones an ordinary operator
# mistake produces. A bare `cd` into a missing directory prints bash's own error
# and exits 1 with no output at all; a bare `git rev-parse` in a non-repository
# leaks two unprefixed `fatal:` lines. Both are captured and re-emitted anchored.
if [ -z "$REPO" ]; then
  REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && git rev-parse --show-toplevel 2>/dev/null || true)"
  [ -n "$REPO" ] || usage_error \
    "this script is not inside a git repository and --repo was not given, so there is nothing to build the two arms from"
fi
[ -d "$REPO" ] || usage_error "--repo $REPO is not a directory"
REPO="$(cd "$REPO" 2>/dev/null && pwd || true)"
[ -n "$REPO" ] || usage_error "--repo could not be entered"
REPO_TOP="$(git -C "$REPO" rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$REPO_TOP" ] || usage_error \
  "--repo $REPO is not a git repository (or git cannot read it), so the two arm commits cannot be resolved"
REPO="$REPO_TOP"
RUN_DIR="$WORK_DIR/results"
LOG_DIR="$WORK_DIR/logs"
mkdir -p "$RUN_DIR" "$LOG_DIR"
RUNS_JSONL="$RUN_DIR/runs.jsonl"
SESSION_LOCK="$WORK_DIR/.session-lock"
# WORK_DIR and PORT both default to fixed values, so two sessions started in one
# work directory used to truncate each other's ledger BEFORE either noticed the
# port was occupied -- fail-closed downstream, but the first session's record was
# already destroyed. `mkdir` is the atomic test-and-set; the ledger is not
# touched until the lock is held AND the port is free.
mkdir "$SESSION_LOCK" 2>/dev/null || {
  warn "cause work-dir-busy"
  warn "cause-detail another session holds $SESSION_LOCK. If no session is running, remove that directory; otherwise pass a different --work-dir. Truncating this session's ledger would destroy the other's."
  exit 2
}
# THE ORDERING IS THE SUBJECT HERE, NOT ANY ONE WRITE SITE. The lock closed the
# CONCURRENT case; this closes the SEQUENTIAL one. Reusing a work directory for
# an attempt that then fails -- an occupied port, a bad corpus, an unresolvable
# ref, a build that will not compile -- used to truncate the previous session's
# ledger and overwrite its manifest on the way past.
#
# So the invariant is stated once and enforced once: NOTHING under RUN_DIR is
# written until pre-flight has passed AND both arms have built. Until then the
# ledger is "unarmed", `write_manifest` is a no-op, and `die` says plainly that
# any manifest present belongs to an earlier session and was not touched. There
# is deliberately no per-site guard -- a second finding in one lifecycle path is
# what says the ordering, not the site, is what needs fixing.
# THE INVARIANT, STATED ONCE: this session writes NOTHING into $RUN_DIR until it
# has a SERVING SERVER and a completed replicate. Not "until pre-flight passed"
# -- that was the previous boundary, and the port can be taken during the two
# release builds that follow it, so the truncation still happened and `run_one`
# then aborted over an already-destroyed ledger.
#
# THIS IS THE THIRD FINDING IN THIS ONE PATH, so the fix is structural rather
# than another recheck: the session's ledger and manifest live in a PRIVATE
# STAGING DIRECTORY for their whole life, and are PROMOTED into $RUN_DIR by
# atomic rename only after a run has actually completed. A fourth instance is
# then not expressible -- there is no code path on which an incomplete session
# can overwrite a previous one's record, because until the promotion the previous
# one's files are the only ones there.
LEDGER_ARMED=0
STAGE_DIR=""

stage_ledger() {
  STAGE_DIR="$RUN_DIR/.staging-$$"
  mkdir -p "$STAGE_DIR"
  RUNS_JSONL="$STAGE_DIR/runs.jsonl"
  : > "$RUNS_JSONL"
  LEDGER_ARMED=1
  write_manifest
}

# Atomic per file, and only ever called after a completed run.
promote_ledger() {
  [ "${LEDGER_ARMED:-0}" = "1" ] || return 0
  [ -n "${STAGE_DIR:-}" ] || return 0
  cp "$STAGE_DIR/runs.jsonl" "$RUN_DIR/.runs.tmp.$$" \
    && mv "$RUN_DIR/.runs.tmp.$$" "$RUN_DIR/runs.jsonl"
  cp "$STAGE_DIR/manifest.json" "$RUN_DIR/.manifest.tmp.$$" \
    && mv "$RUN_DIR/.manifest.tmp.$$" "$RUN_DIR/manifest.json"
  PROMOTED=1
}
PROMOTED=0

# ---------------------------------------------------------------------------
# Cleanup. Registered NOW, before the resources it frees can exist -- this repo
# has three separate findings in the family "the fix that added a resource did
# not register it with the signal path". A `die` between starting a server and
# reaping it (a failed prewarm, a server that never bound, any `set -e` abort) or
# a Ctrl-C used to leave a live cqlite-flight holding the port.
# ---------------------------------------------------------------------------
SRV_PID=''

is_our_server() { # <pid> -- guard a KILL against PID reuse on a busy box
  [ -n "${1:-}" ] || return 1
  [ -r "/proc/$1/cmdline" ] || return 1
  tr '\0' ' ' < "/proc/$1/cmdline" 2>/dev/null | grep -q 'cqlite-flight'
}

reap_server() {
  [ -n "$SRV_PID" ] || return 0
  local pid="$SRV_PID"
  # Cleared FIRST, so a signal arriving during the reap cannot re-enter it.
  SRV_PID=''
  kill "$pid" 2>/dev/null || true
  local waited=0
  while [ "$waited" -lt 30 ]; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
    waited=$((waited + 1))
  done
  # Only ever KILL something still identifiable as our own server: on a box with
  # real PID churn the id could by then belong to somebody else. No `kill` in
  # this script ever targets a process GROUP.
  if kill -0 "$pid" 2>/dev/null && is_our_server "$pid"; then
    kill -9 "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  reap_server
  [ -n "${STAGE_DIR:-}" ] && rm -rf "$STAGE_DIR" 2>/dev/null || true
  [ -n "${SESSION_LOCK:-}" ] && rm -f "$RUN_DIR/.runs.tmp.$$" "$RUN_DIR/.manifest.tmp.$$" 2>/dev/null || true
  [ -n "${SESSION_LOCK:-}" ] && rmdir "$SESSION_LOCK" 2>/dev/null || true
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

# ---------------------------------------------------------------------------
# Manifest. Rewritten after every completed run, so an interrupted session
# leaves a truthful SHORT manifest rather than nothing.
# ---------------------------------------------------------------------------
BASE_SHA=''
HEAD_SHA=''
CORPUS_BYTES=0
CORPUS_FILES=0

write_manifest() {
  # A no-op until the ledger is staged, and it writes into the STAGING directory
  # -- never into $RUN_DIR, which still holds a previous session's record until
  # `promote_ledger` runs.
  [ "${LEDGER_ARMED:-0}" = "1" ] || return 1
  python3 - "$STAGE_DIR/manifest.json" "$RUNS_JSONL" <<'PYEOF'
import json
import os
import sys

out_path, runs_path = sys.argv[1], sys.argv[2]
runs = []
if os.path.exists(runs_path):
    with open(runs_path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                runs.append(json.loads(line))


def env(name, default=None):
    value = os.environ.get(name, "")
    return value if value else default


def _int_or_none(raw):
    """None only when UNSET. A configured 0 stays 0."""
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


rows = env("AB_ROWS_DECLARED")
manifest = {
    "schema": "ab-3649.manifest/v1",
    "driver_version": env("AB_DRIVER_VERSION", "unknown"),
    "generated_utc": env("AB_GENERATED_UTC", "NOT-RECORDED"),
    "replicates_requested": int(env("AB_REPLICATES", "0")),
    "arms": {
        "base": {"commit": env("AB_BASE_SHA", "NOT-RECORDED"), "ref": env("AB_BASE_REF", "")},
        "head": {"commit": env("AB_HEAD_SHA", "NOT-RECORDED"), "ref": env("AB_HEAD_REF", "")},
    },
    "workload": {
        "shape": env("AB_SHAPE", ""),
        "ramp": env("AB_RAMP", ""),
        "step_duration": env("AB_STEP_DURATION", ""),
        "prewarm": env("AB_PREWARM", "0") == "1",
        "server_cpus": env("AB_SERVER_CPUS", "none-unpinned"),
        "client_cpus": env("AB_CLIENT_CPUS", "none-unpinned"),
        "temperature": env("AB_TEMPERATURE", ""),
        "ticket_template": env("AB_TICKET_TEMPLATE", ""),
        "merge_path": env("AB_MERGE_PATH", ""),
        # `int(x) or None` turns a configured ZERO into `null`, which is how a
        # throughput-critical parameter could vanish from the record entirely.
        # An unset option is "server-default"; a configured one is recorded
        # exactly, zero included.
        "max_concurrent_scans": _int_or_none(env("AB_MAX_CONCURRENT_SCANS")),
        "batch_size": _int_or_none(env("AB_BATCH_SIZE")),
        "max_batch_bytes": env("AB_MAX_BATCH_BYTES") or "server-default",
        "admission_wait_timeout_ms": env("AB_ADMISSION_WAIT_TIMEOUT_MS")
        or "server-default",
        "step_duration_seconds": float(env("AB_STEP_DURATION_SECONDS", "0")),
    },
    "control": env("AB_CONTROL") or None,
    "server_extra": {
        "base": env("AB_BASE_SERVER_EXTRA", ""),
        "head": env("AB_HEAD_SERVER_EXTRA", ""),
    },
    "corpus": {
        "path": env("AB_CORPUS", ""),
        "data_db_bytes": int(env("AB_CORPUS_BYTES", "0")),
        "data_db_files": int(env("AB_CORPUS_FILES", "0")),
        "min_bytes_required": int(env("AB_MIN_CORPUS_BYTES", "0")),
        "min_sstables_required": int(env("AB_MIN_SSTABLES", "0")),
        "rows_declared": int(rows) if rows else None,
    },
    "host": {
        "instance_type": env("AB_INSTANCE_TYPE", "NOT-RECORDED"),
        "nproc": int(env("AB_NPROC", "0")),
        "loadavg1": env("AB_LOADAVG1", "NOT-RECORDED"),
        "kernel": env("AB_KERNEL", "NOT-RECORDED"),
    },
    "runs": runs,
}
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
    handle.write("\n")
PYEOF
}

export AB_DRIVER_VERSION="$DRIVER_VERSION"
export AB_REPLICATES="$REPLICATES"
export AB_BASE_REF="$BASE_REF" AB_HEAD_REF="$HEAD_REF"
export AB_SHAPE="$SHAPE" AB_RAMP="$RAMP" AB_STEP_DURATION="$STEP_DURATION"
export AB_STEP_DURATION_SECONDS="$STEP_DURATION_SECONDS"
export AB_PREWARM="$PREWARM" AB_TEMPERATURE="$TEMPERATURE"
export AB_TICKET_TEMPLATE="$TICKET_TEMPLATE"
export AB_CONTROL="$CONTROL"
export AB_BASE_SERVER_EXTRA="$BASE_SERVER_EXTRA" AB_HEAD_SERVER_EXTRA="$HEAD_SERVER_EXTRA"
export AB_CORPUS="$CORPUS" AB_MIN_CORPUS_BYTES="$MIN_CORPUS_BYTES"
export AB_MIN_SSTABLES="$MIN_SSTABLES" AB_MERGE_PATH="$MERGE_PATH"
export AB_MAX_CONCURRENT_SCANS="$MAX_CONCURRENT_SCANS" AB_BATCH_SIZE="$BATCH_SIZE"
export AB_MAX_BATCH_BYTES="$MAX_BATCH_BYTES"
export AB_ADMISSION_WAIT_TIMEOUT_MS="$ADMISSION_WAIT_TIMEOUT_MS"
export AB_ROWS_DECLARED="$ROWS_DECLARED"
export AB_GENERATED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
say "=== issue #3649 -- interleaved paired A/B, #2820 batched merge fan-in ==="
say "repo $REPO"
say "work-dir $WORK_DIR"
if [ -n "$CONTROL" ]; then
  say "control $CONTROL -- this session is a CONTROL; its verdict does not discharge the #3649 acceptance criteria"
else
  say "control none -- this session is a measurement"
fi
if [ -n "$BASE_SERVER_EXTRA" ] || [ -n "$HEAD_SERVER_EXTRA" ]; then
  say "server-extra base [$BASE_SERVER_EXTRA] head [$HEAD_SERVER_EXTRA]"
  [ "$BASE_SERVER_EXTRA" = "$HEAD_SERVER_EXTRA" ] || say \
    "server-extra ASYMMETRIC -- the arms are not being served under the same configuration, so any difference measured is the injected one and not the commit pair's"
fi

for tool in git cargo python3; do
  command -v "$tool" >/dev/null 2>&1 || die preflight-tool "$tool is not on PATH"
done

# IMDSv2 first (a token-required endpoint answers nothing to a bare GET), then
# IMDSv1, then the DMI product name. An unobtainable value is NOT-RECORDED, never
# a guess: the AC asks for the host to be stated, and a wrong instance type is
# worse than an absent one.
imds_instance_type() {
  local token
  token="$(curl -s --max-time 2 -X PUT \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null || true)"
  if [ -n "$token" ]; then
    curl -s --max-time 2 -H "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null && return 0
  fi
  curl -s --max-time 2 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || true
}
AB_INSTANCE_TYPE="$(imds_instance_type)"
[ -n "$AB_INSTANCE_TYPE" ] && [ "${AB_INSTANCE_TYPE#*<}" = "$AB_INSTANCE_TYPE" ] \
  || AB_INSTANCE_TYPE="$(cat /sys/devices/virtual/dmi/id/product_name 2>/dev/null || true)"
[ -n "$AB_INSTANCE_TYPE" ] || AB_INSTANCE_TYPE='NOT-RECORDED'
export AB_INSTANCE_TYPE
export AB_NPROC="$(nproc)"
export AB_LOADAVG1="$(cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo NOT-RECORDED)"
export AB_KERNEL="$(uname -sr)"
say "host instance-type $AB_INSTANCE_TYPE nproc $AB_NPROC loadavg1 $AB_LOADAVG1 kernel $AB_KERNEL"

[ -f "$TICKET_TEMPLATE" ] || die ticket-template-absent "$TICKET_TEMPLATE does not exist"
# THE WORKLOAD MUST MATCH THE CLAIM THE REPORT WILL MAKE ABOUT IT. The #3649
# target band is defined for `flight-loadgen --shape full` over the whole ring
# (the AC's first line), so a point, limit-k, filtered, projected or aggregating
# session receiving a verdict against that band is a wrong answer wearing a
# right-looking shape. Checking that the file is JSON never checked what was in
# it. A CONTROL may use any shape -- its verdict is already disclaimed.
if [ -z "$CONTROL" ]; then
  [ "$SHAPE" = "full" ] || usage_error \
    "--shape is '$SHAPE', but the #3649 target band is defined for --shape full over the whole ring. Run it as a control (--control <label>) if you want another shape; its verdict is then disclaimed rather than scored against the band"
  python3 "$SUPPORT" validate-ticket "$TICKET_TEMPLATE" \
    || die ticket-not-full-ring "$TICKET_TEMPLATE does not describe a full-ring scan (the cause is named above)"
else
  python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$TICKET_TEMPLATE" >/dev/null 2>&1 \
    || die ticket-template-unparseable "$TICKET_TEMPLATE is not valid JSON"
  say "shape $SHAPE ticket UNCHECKED -- a control may narrow the workload; the analyzer disclaims its verdict"
fi

[ -d "$CORPUS" ] || die corpus-absent "$CORPUS is not a directory"
# The census GATES the size floor, so a partial census is not an acceptable
# answer -- and under `set -e -o pipefail` a find that hits one unreadable
# directory used to abort the whole script with exit 1, no cause and no manifest.
# find's status is captured and turned into a NAMED refusal instead. `-printf` is
# GNU-only; a find without it fails here rather than reporting a zero-byte corpus.
CORPUS_LIST="$LOG_DIR/corpus-census.txt"
census_rc=0
find "$CORPUS" -name '*-Data.db' -type f -printf '%s\n' > "$CORPUS_LIST" 2>"$LOG_DIR/corpus-census.err" || census_rc=$?
if [ "$census_rc" -ne 0 ]; then
  die corpus-census-failed \
    "find over $CORPUS exited $census_rc; the census gates the minimum-size check, so a partial answer is refused rather than used. See $LOG_DIR/corpus-census.err (note: this driver needs GNU find for -printf)"
fi
CORPUS_FILES="$(wc -l < "$CORPUS_LIST" | tr -d ' ')"
CORPUS_BYTES="$(awk 'BEGIN{s=0}{s+=$1}END{print s+0}' "$CORPUS_LIST")"
export AB_CORPUS_BYTES="$CORPUS_BYTES" AB_CORPUS_FILES="$CORPUS_FILES"
say "corpus path $CORPUS data-db-files $CORPUS_FILES data-db-bytes $CORPUS_BYTES"
[ "$CORPUS_FILES" -gt 0 ] || die corpus-empty "$CORPUS holds no *-Data.db files"
if [ "$CORPUS_BYTES" -lt "$MIN_CORPUS_BYTES" ]; then
  die corpus-too-small \
    "$CORPUS_BYTES Data.db bytes is below the required $MIN_CORPUS_BYTES; a --shape full scan over a corpus this small measures request setup, not the read path (RUNBOOK.md states the floor and its basis)"
fi
if [ "$CORPUS_FILES" -lt "$MIN_SSTABLES" ]; then
  die corpus-too-few-sstables \
    "$CORPUS_FILES Data.db files is below the required $MIN_SSTABLES; issue #3058 gives the Flight row route a single-source fast path that NEVER enters the k-way merge, so a one-source corpus measures a code path #2820 did not touch -- and it does so identically on both arms, producing a ratio of 1.0 by construction"
fi
say "merge-path $MERGE_PATH -- CQLITE_FLIGHT_MERGE_PATH is set to this on BOTH arms' servers"
say "admission max-concurrent-scans $MAX_CONCURRENT_SCANS (pinned on both arms; ramp tops at $RAMP_TOP) batch-size $BATCH_SIZE"
if [ "$MERGE_PATH" != "merge" ]; then
  say "merge-path NOT-PINNED -- with anything but 'merge' the #3058 predicate may route a request onto the single-source fast path, which #2820 did not touch"
fi

resolve() { git -C "$REPO" rev-parse --verify --quiet "$1^{commit}" || true; }
BASE_SHA="$(resolve "$BASE_REF")"
HEAD_SHA="$(resolve "$HEAD_REF")"
[ -n "$BASE_SHA" ] || die arm-ref-unresolvable "--base-ref $BASE_REF does not resolve to a commit in $REPO"
[ -n "$HEAD_SHA" ] || die arm-ref-unresolvable "--head-ref $HEAD_REF does not resolve to a commit in $REPO"
[ "$BASE_SHA" != "$HEAD_SHA" ] || die arm-refs-identical "both arms resolve to $BASE_SHA"
export AB_BASE_SHA="$BASE_SHA" AB_HEAD_SHA="$HEAD_SHA"
say "arm base ref $BASE_REF commit $BASE_SHA"
say "arm head ref $HEAD_REF commit $HEAD_SHA"

if [ -n "$SERVER_CPUS" ]; then
  export AB_SERVER_CPUS="$SERVER_CPUS" AB_CLIENT_CPUS="$CLIENT_CPUS"
  cpu_sets_disjoint() {
  python3 - "$SERVER_CPUS" "$CLIENT_CPUS" <<'PYEOF'
import sys


def expand(spec):
    cpus = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            cpus.update(range(int(lo), int(hi) + 1))
        else:
            cpus.add(int(part))
    return cpus


server, client = expand(sys.argv[1]), expand(sys.argv[2])
overlap = sorted(server & client)
if overlap:
    sys.stderr.write(
        "AB-3649: cause cpu-sets-overlap\n"
        "AB-3649: cause-detail the server and client CPU sets share %s; a shared "
        "CPU means the measurement includes the load generator competing with the "
        "engine\n" % (overlap,)
    )
    sys.exit(1)
PYEOF
  }
  cpu_sets_disjoint || usage_error "the server and client CPU sets overlap (detail above)"
  say "pinning server-cpus $SERVER_CPUS client-cpus $CLIENT_CPUS"
else
  say "pinning none-unpinned -- recorded as an explicit fact, not an absence; RUNBOOK.md recommends pinning after reading the sibling map from sysfs"
fi

# The ledger is truncated only now: the lock is held, every argument is
# validated, and the port is free, so nothing that follows can destroy a peer
# session's record before discovering it should not have started.
if (echo > "/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1; then
  die port-occupied \
    "port $PORT is already bound before the session started; pass a different --port or stop what is listening. The run ledger has NOT been truncated"
fi

# ---------------------------------------------------------------------------
# Build: one worktree and one target directory per arm.
# ---------------------------------------------------------------------------
declare -A ARM_BIN_DIR

build_arm() { # <arm> <sha>
  local arm="$1" sha="$2"
  local wt="$WORK_DIR/wt-$arm" target="$WORK_DIR/target-$arm"
  say "build $arm commit $sha worktree $wt target-dir $target"
  if [ ! -d "$wt" ]; then
    git -C "$REPO" worktree add --detach "$wt" "$sha" > "$LOG_DIR/worktree-$arm.log" 2>&1 \
      || die worktree-failed "git worktree add for $arm failed; see $LOG_DIR/worktree-$arm.log"
  fi
  local at
  at="$(git -C "$wt" rev-parse HEAD 2>/dev/null || true)"
  [ "$at" = "$sha" ] || die worktree-wrong-commit \
    "$wt is at ${at:-an unreadable HEAD} but arm $arm is $sha -- a pre-existing worktree was reused at the wrong commit; remove $wt and re-run"
  # A sha is not a tree. A leftover worktree carrying uncommitted edits builds
  # DIFFERENT CODE while the manifest records the clean sha -- the same fact this
  # repository's pre-merge doctrine records as "commit: cannot see a dirty tree".
  local dirty
  dirty="$(git -C "$wt" status --porcelain --untracked-files=all 2>/dev/null || echo UNREADABLE)"
  [ -z "$dirty" ] || die worktree-dirty \
    "$wt is at the right commit but is NOT CLEAN, so it would build code the manifest does not describe; remove $wt and re-run. First entries: $(printf '%s' "$dirty" | head -3 | tr '\n' ';')"
  ( cd "$wt" && CARGO_TARGET_DIR="$target" cargo build --release \
      -p cqlite-flight -p flight-loadgen ) > "$LOG_DIR/build-$arm.log" 2>&1 \
    || die build-failed "the $arm build failed; see $LOG_DIR/build-$arm.log"
  local bin="$target/release"
  [ -x "$bin/cqlite-flight" ]  || die build-incomplete "$bin/cqlite-flight was not produced"
  [ -x "$bin/flight-loadgen" ] || die build-incomplete "$bin/flight-loadgen was not produced"
  ARM_BIN_DIR["$arm"]="$bin"
  say "build $arm complete"
}

build_arm base "$BASE_SHA"
build_arm head "$HEAD_SHA"

# Pre-flight passed and both arms exist: only now may this session claim the work
# directory's ledger and manifest.
stage_ledger
say "ledger staged in $STAGE_DIR -- $RUN_DIR still describes any EARLIER session until the first replicate completes"

# ---------------------------------------------------------------------------
# One replicate of one arm.
# ---------------------------------------------------------------------------
port_is_bound() { (echo > "/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1; }

parse_startup() { # <server-log> <scans|source>  -- returns a VALUE, not a message
  python3 "$SUPPORT" parse-startup "$1" "$2" 2>/dev/null || echo NOT-OBSERVED
}

run_one() { # <arm> <replicate> <position-in-pair: 1|2>
  local arm="$1" rep="$2" position="$3"
  local tag; tag="$(printf '%s-r%02d' "$arm" "$rep")"
  local bin="${ARM_BIN_DIR[$arm]}"
  local jsonl="$RUN_DIR/$tag.jsonl"
  local server_log="$LOG_DIR/$tag.server.log"

  if port_is_bound; then
    die port-occupied "port $PORT is already bound before $tag started; the previous server did not die, and a replicate served by the wrong arm's binary is worse than no replicate"
  fi

  if [ "$TEMPERATURE" = "cold" ]; then
    sync
    sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1 \
      || die cold-drop-failed "--temperature cold needs passwordless sudo to drop the page cache; without it the run is warm and would be recorded as cold"
  fi

  local extra=''
  if [ "$arm" = "base" ]; then extra="$BASE_SERVER_EXTRA"; else extra="$HEAD_SERVER_EXTRA"; fi
  local -a server_cmd=(env "CQLITE_FLIGHT_MERGE_PATH=$MERGE_PATH")
  [ -n "$SERVER_CPUS" ] && server_cmd+=(taskset -c "$SERVER_CPUS")
  server_cmd+=("$bin/cqlite-flight" --data-dir "$CORPUS" --listen "127.0.0.1:$PORT"
               --batch-size "$BATCH_SIZE"
               --max-concurrent-scans "$MAX_CONCURRENT_SCANS")
  [ -n "$MAX_BATCH_BYTES" ] && server_cmd+=(--max-batch-bytes "$MAX_BATCH_BYTES")
  [ -n "$ADMISSION_WAIT_TIMEOUT_MS" ] \
    && server_cmd+=(--admission-wait-timeout-ms "$ADMISSION_WAIT_TIMEOUT_MS")
  # Word-split on purpose: the value is an operator-supplied flag list, and it is
  # recorded verbatim in the manifest so a reader of the report sees it.
  # shellcheck disable=SC2206
  [ -n "$extra" ] && server_cmd+=($extra)
  "${server_cmd[@]}" > "$server_log" 2>&1 &
  local srv=$!
  # Registered BEFORE anything can fail, so every exit path -- die, set -e, a
  # signal -- reaps it through `cleanup`.
  SRV_PID=$srv

  local waited=0
  while [ "$waited" -lt 90 ]; do
    if port_is_bound; then break; fi
    if ! kill -0 "$srv" 2>/dev/null; then
      SRV_PID=''
      die server-exited "the $tag server exited before binding port $PORT; see $server_log"
    fi
    sleep 1
    waited=$((waited + 1))
  done
  port_is_bound || die server-never-bound \
    "the $tag server did not bind port $PORT within ${waited}s; see $server_log"
  say "run $tag server pid $srv bound 127.0.0.1:$PORT after ${waited}s"

  # PROVENANCE, READ FROM THE SERVER RATHER THAN ASSUMED. cli::log_startup emits
  # one `cqlite-flight starting` line carrying the RESOLVED admission ceiling and
  # its source ("flag" | "env" | "derived" | "derived-fallback",
  # cqlite-flight/src/admission.rs:183-193). A value we passed and a value the
  # server resolved are different facts; only the second one is a measurement.
  local admission_observed admission_source
  admission_observed="$(parse_startup "$server_log" scans)"
  admission_source="$(parse_startup "$server_log" source)"
  say "run $tag admission requested $MAX_CONCURRENT_SCANS observed $admission_observed source $admission_source"
  if [ "$admission_observed" != "NOT-OBSERVED" ] \
     && [ "$admission_observed" != "$MAX_CONCURRENT_SCANS" ]; then
    die admission-mismatch \
      "$tag: the server resolved --max-concurrent-scans to $admission_observed but $MAX_CONCURRENT_SCANS was requested; the arms would not be served under the same admission ceiling"
  fi
  # THE SWEEP: every option that can differ between what we REQUESTED, what the
  # server RESOLVED and what the manifest RECORDS is read back from the same
  # startup line. `max_concurrent_scans` is the one the server may derive on its
  # own; the rest are echoes, and reading an echo is how we know we configured
  # the process we are actually talking to.
  local observed_batch observed_maxbytes observed_wait
  observed_batch="$(parse_startup "$server_log" batch-size)"
  observed_maxbytes="$(parse_startup "$server_log" max-batch-bytes)"
  observed_wait="$(parse_startup "$server_log" wait-timeout-ms)"
  say "run $tag server batch-size observed $observed_batch max-batch-bytes observed $observed_maxbytes wait-timeout-ms observed $observed_wait"
  if [ "$observed_batch" != "NOT-OBSERVED" ] && [ "$observed_batch" != "$BATCH_SIZE" ]; then
    die batch-size-mismatch \
      "$tag: the server reports batch_size=$observed_batch but $BATCH_SIZE was requested; the Arrow batch row cap is the mechanism #2820 changed, so a measurement whose effective value is unknown is not a measurement"
  fi
  if [ -n "$MAX_BATCH_BYTES" ] && [ "$observed_maxbytes" != "NOT-OBSERVED" ] \
     && [ "$observed_maxbytes" != "$MAX_BATCH_BYTES" ]; then
    die max-batch-bytes-mismatch \
      "$tag: the server reports max_batch_bytes=$observed_maxbytes but $MAX_BATCH_BYTES was requested"
  fi
  if [ -n "$ADMISSION_WAIT_TIMEOUT_MS" ] && [ "$observed_wait" != "NOT-OBSERVED" ] \
     && [ "$observed_wait" != "$ADMISSION_WAIT_TIMEOUT_MS" ]; then
    die wait-timeout-mismatch \
      "$tag: the server reports admission_wait_timeout_ms=$observed_wait but $ADMISSION_WAIT_TIMEOUT_MS was requested; the shed threshold decides which steps the analyzer must exclude"
  fi

  # Requested pinning and EFFECTIVE pinning are different facts too: a server
  # that is not on the cores the manifest names is measuring something else, and
  # nothing else in this driver would notice.
  if [ -n "$SERVER_CPUS" ]; then
    local affinity
    affinity="$(python3 "$SUPPORT" check-affinity "$srv" "$SERVER_CPUS")" || die affinity-mismatch \
      "$tag: the server is not pinned to $SERVER_CPUS (the cause is named above)"
    say "run $tag server affinity $affinity requested $SERVER_CPUS"
  fi

  if [ "$admission_source" != "NOT-OBSERVED" ] && [ "$admission_source" != "flag" ]; then
    die admission-provenance \
      "$tag: the server reports the admission ceiling came from '$admission_source', not 'flag', even though --max-concurrent-scans was passed; something else (CQLITE_MAX_CONCURRENT_SCANS in the environment, or a derived fallback) is deciding it"
  fi

  local -a client_prefix=()
  [ -n "$CLIENT_CPUS" ] && client_prefix+=(taskset -c "$CLIENT_CPUS")

  if [ "$PREWARM" -eq 1 ] && [ "$TEMPERATURE" = "warm" ]; then
    "${client_prefix[@]}" "$bin/flight-loadgen" --endpoint "http://127.0.0.1:$PORT" \
      --ticket-template "$TICKET_TEMPLATE" --shape "$SHAPE" --ramp "$RAMP" \
      --step-duration "$STEP_DURATION" --round "$tag-prewarm" --out /dev/null \
      > "$LOG_DIR/$tag.prewarm.log" 2>&1 \
      || die prewarm-failed "the $tag warming pass failed; see $LOG_DIR/$tag.prewarm.log"
  fi

  local cpu0 cpu1 hz
  # An unreadable /proc yields NOTHING, not a fabricated 0: the recorder already
  # has a null path for server_cpu_seconds and a silent zero would defeat it.
  cpu0="$(awk '{print $14+$15}' "/proc/$srv/stat" 2>/dev/null || true)"
  local rc=0
  "${client_prefix[@]}" "$bin/flight-loadgen" --endpoint "http://127.0.0.1:$PORT" \
    --ticket-template "$TICKET_TEMPLATE" --shape "$SHAPE" --ramp "$RAMP" \
    --step-duration "$STEP_DURATION" --round "$tag" --out "$jsonl" \
    > "$LOG_DIR/$tag.loadgen.log" 2>&1 || rc=$?
  cpu1="$(awk '{print $14+$15}' "/proc/$srv/stat" 2>/dev/null || true)"
  hz="$(getconf CLK_TCK)"

  reap_server
  if kill -0 "$srv" 2>/dev/null && is_our_server "$srv"; then
    die server-would-not-die "the $tag server (pid $srv) survived TERM and KILL; the next replicate would bind a port served by this arm's binary"
  fi
  local released=0
  while [ "$released" -lt 30 ]; do
    port_is_bound || break
    sleep 1
    released=$((released + 1))
  done
  port_is_bound && die port-not-released \
    "port $PORT is still bound ${released}s after the $tag server was reaped"

  [ "$rc" -eq 0 ] || die loadgen-failed \
    "the $tag load generator exited $rc; see $LOG_DIR/$tag.loadgen.log"

  # Validate the produced records here, not only in the analyzer: a bad
  # replicate must stop the session while the rig is still up, not surface hours
  # later. The validator is ramp-aware -- flight-loadgen emits ONE record per
  # ramp step -- and it is an executable file so the self-test can drive it.
  python3 "$SUPPORT" validate-replicate "$jsonl" "$tag" "$RAMP" \
    || die replicate-invalid "the $tag JSONL is not a usable replicate (see the cause-detail above)"

  python3 - "$RUNS_JSONL" "$arm" "$rep" "$tag.jsonl" "$TEMPERATURE" "$cpu0" "$cpu1" "$hz" \
    "$admission_observed" "$admission_source" "$observed_batch" \
    "$observed_maxbytes" "$observed_wait" "$position" <<'PYEOF'
import json
import sys

(runs_path, arm, rep, filename, temperature, cpu0, cpu1, hz,
 admission_observed, admission_source, batch_size_observed,
 max_batch_bytes_observed, wait_timeout_ms_observed, position) = sys.argv[1:15]
try:
    server_cpu_s = (int(cpu1) - int(cpu0)) / float(hz)
except (ValueError, ZeroDivisionError):
    # An empty reading means /proc was unreadable. NOT a zero: a fabricated
    # counter is indistinguishable from a server that used no CPU.
    server_cpu_s = None
entry = {
    "arm": arm,
    "replicate": int(rep),
    "file": filename,
    "temperature": temperature,
    "server_cpu_seconds": server_cpu_s,
    # NOT-OBSERVED is carried through as a string; the analyzer reports it as an
    # uncorroborated requested value rather than treating it as agreement.
    "admission_observed": admission_observed,
    "admission_source": admission_source,
    "batch_size_observed": batch_size_observed,
    # Observed and PERSISTED, so the analyzer can compare them across arms. A
    # value that is read and then dropped is the same defect as a value that is
    # read and then not compared -- the observation has to reach whoever makes
    # the claim.
    "max_batch_bytes_observed": max_batch_bytes_observed,
    "wait_timeout_ms_observed": wait_timeout_ms_observed,
    # The ACTUAL executed order, not the parity rule that chose it.
    "position_in_pair": int(position),
}
with open(runs_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry, sort_keys=True) + "\n")
PYEOF
  write_manifest
  # The first promotion is the moment $RUN_DIR stops describing an earlier
  # session -- and it happens only once a server has served and a replicate has
  # been validated, which is the invariant stated at the top of this file.
  if [ "${PROMOTED:-0}" = "0" ]; then
    promote_ledger
    say "ledger promoted -- $RUN_DIR now describes THIS session"
  else
    promote_ledger
  fi
}

# ---------------------------------------------------------------------------
# The interleaved session.
# ---------------------------------------------------------------------------
# COUNTERBALANCED, NOT MERELY INTERLEAVED. Interleaving across replicates
# controls drift BETWEEN pairs; it does nothing about a gradient WITHIN one. If
# base always ran first, a monotonic drift over the ~2 minutes of a pair -- a
# thermal ramp, a clock adjustment, a neighbour's job starting -- lands on the
# head arm in EVERY pair and biases every ratio the same way. That is precisely
# the systematic error the paired design exists to remove, and it would arrive
# with a tight interval, which is worse than a noisy one because it looks
# trustworthy.
#
# So the order alternates by replicate parity, and the ACTUAL executed order is
# recorded per run (`position_in_pair`) rather than assumed from the parity rule:
# the analyzer counts it and refuses a session where counterbalancing did not
# happen. An odd replicate count cannot balance exactly -- one ordering runs once
# more than the other -- and that residual is disclosed, not hidden. An EVEN
# count cancels exactly; see RUNBOOK.md step 5.
say "session replicates $REPLICATES order counterbalanced-by-replicate-parity shape $SHAPE ramp $RAMP step-duration $STEP_DURATION temperature $TEMPERATURE prewarm $PREWARM"
rep=1
while [ "$rep" -le "$REPLICATES" ]; do
  # The rule lives in ab_driver_support.py so it can be executed by the
  # self-test: this is the one decision here whose failure is a confident wrong
  # answer rather than an error, so it must not be the untested one.
  pair_order="$(python3 "$SUPPORT" pair-order "$rep")" \
    || die pair-order-failed "could not determine the within-pair order for replicate $rep"
  pair_first="${pair_order%% *}"
  pair_second="${pair_order##* }"
  say "pair $rep order $pair_first-then-$pair_second"
  run_one "$pair_first" "$rep" 1
  run_one "$pair_second" "$rep" 2
  rep=$((rep + 1))
done

write_manifest
promote_ledger
say "session complete: $REPLICATES paired replicates in $RUN_DIR"
# The section this session's ramp belongs to, decided by the same validator that
# accepted the ramp rather than re-derived here.
say "next python3 $(dirname "$SUPPORT")/analyze-ab.py --$RAMP_SECTION $RUN_DIR/manifest.json"
exit 0

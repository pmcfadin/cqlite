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
  if write_manifest 2>/dev/null && [ -f "${RUN_DIR:-}/manifest.json" ]; then
    say "manifest $RUN_DIR/manifest.json records the runs that did complete"
  else
    say "manifest NOT WRITTEN -- this abort happened before a manifest could be produced"
  fi
  say "earlier sessions in $WORK_DIR are untouched: this session wrote only to $RUN_DIR"
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
  --loadgen-ref <rev>       commit to build the ONE flight-loadgen from, used by
                            BOTH arms                       (default: --head-ref)
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
  --port <n>                loopback port; 0 = EPHEMERAL, learned from the
                            server's own `listening on` line             (default 0)
  --server-cpus <list>      taskset list for the server         (default unpinned)
  --client-cpus <list>      taskset list for the load generator (default unpinned)
  --min-corpus-bytes <n>    refuse below this many Data.db bytes  (default 268435456)
  --min-sstables <n>        refuse below this many Data.db files         (default 2)
  --merge-path <arm>        CQLITE_FLIGHT_MERGE_PATH for BOTH servers  (default merge;
                            auto | merge | bypass -- see the #3058 note below)
  --rows-declared <n>       corpus row count, recorded not measured  (default none)
  --no-prewarm              skip the per-replicate warming pass
  --attest-local-storage <why>
                            operator attestation that the corpus IS on local
                            storage, for a device whose model this probe cannot
                            recognise. Recorded in the manifest and printed
                            beside the verdict, so the attestation travels with
                            the number. It covers IGNORANCE only -- a device
                            affirmatively identified as NETWORK cannot be
                            attested away.
  --profile <narrow|wide>   which target band this workload is measured against.
                            REQUIRED for a measurement, and deliberately NOT
                            defaulted: the band's own source defines narrow and
                            wide qualitatively with no numeric boundary
                            (docs/research/phase2-verify-row-engine.md line 107),
                            so nothing can derive it -- and a default silently
                            scored wide-row sessions against the narrow band.
                            Recorded in the manifest; the analyzer reads it from
                            there rather than from a flag of its own.
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

Each session writes to its OWN directory, <work-dir>/run-<session-id>/, and this
script prints the exact analyzer command on its `next` line when it finishes.
Copy that rather than composing a path; <work-dir>/latest is a convenience
symlink to the most recent completed session and is not what you certify.

  python3 analyze-ab.py --single-stream <work-dir>/run-<session-id>/manifest.json
  python3 analyze-ab.py --utilization   <work-dir>/run-<session-id>/manifest.json
USAGE
}

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
CORPUS=''
TICKET_TEMPLATE=''
LOADGEN_REF=''
#: The #2820 commit: "perf(#2820): batch the k-way merge egress fan-in (L1),
#: co-designed with #2765 (#3659)". ONE literal, so the default refs and the
#: measurement pin cannot drift apart into two claims about which commit this
#: instrument is about.
AB3649_PIN="$(python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); import ab_driver_support as S; print(S.AB3649_PIN_SHA)' "$(dirname "$SUPPORT")")"
BASE_REF="${AB3649_PIN}^"
HEAD_REF="$AB3649_PIN"
REPLICATES=7
WORK_DIR='/data/ab-3649'
REPO=''
SHAPE='full'
PROFILE=''
RAMP='1'
STEP_DURATION='60s'
PORT=0
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
ATTEST_LOCAL_STORAGE=''
BASE_SERVER_EXTRA=''
HEAD_SERVER_EXTRA=''

# EVERY value-taking option, in one list. A `shift 2` with one argument left
# consumes past the end and exits 1 with an unanchored bash error instead of the
# documented usage error -- so the presence of a value is checked ONCE here
# rather than in each arm below, where the next option added would be the next
# one to miss it. `scripts`-side drift is caught by a structural case in
# selftest-analyze.sh that requires every `shift 2` arm to appear in this list.
VALUE_OPTS="--corpus --ticket-template --base-ref --head-ref --replicates \
--work-dir --repo --shape --profile --ramp --step-duration --port --server-cpus \
--loadgen-ref \
--client-cpus --min-corpus-bytes --min-sstables --merge-path \
--max-concurrent-scans --batch-size --max-batch-bytes \
--admission-wait-timeout-ms --rows-declared --temperature --control --attest-local-storage \
--base-server-extra --head-server-extra"

while [ "$#" -gt 0 ]; do
  case " $VALUE_OPTS " in
    *" $1 "*)
      [ "$#" -ge 2 ] || usage_error "$1 requires a value" ;;
  esac
  case "$1" in
    --corpus)            CORPUS="${2:-}";           shift 2 ;;
    --ticket-template)   TICKET_TEMPLATE="${2:-}";  shift 2 ;;
    --loadgen-ref)       LOADGEN_REF="${2:-}";      shift 2 ;;
    --base-ref)          BASE_REF="${2:-}";         shift 2 ;;
    --head-ref)          HEAD_REF="${2:-}";         shift 2 ;;
    --replicates)        REPLICATES="${2:-}";       shift 2 ;;
    --work-dir)          WORK_DIR="${2:-}";         shift 2 ;;
    --repo)              REPO="${2:-}";             shift 2 ;;
    --shape)             SHAPE="${2:-}";            shift 2 ;;
    --profile)           PROFILE="${2:-}";          shift 2 ;;
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
    --attest-local-storage) ATTEST_LOCAL_STORAGE="${2:-}"; shift 2 ;;
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
case "$PORT" in ''|*[!0-9]*) usage_error "--port must be an integer (0 = ephemeral)" ;; esac
# INCLUSIVE RANGE, not just digits. 99999 is an integer and is not a port, and
# digits-only let it reach the server launch -- which is AFTER all three release
# builds, on a metered box. A TCP port is 16 bits; 0 means ephemeral here.
[ "$PORT" -le 65535 ] || usage_error "--port $PORT is above 65535; a TCP port is 16 bits, and this would be refused by the server after every build had completed"
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
# ONE RESOLVER, AND THE DRIVER READS NOTHING ELSE. Every server-configuration
# value comes from this single call: per-arm effective values, the documented
# corpus floors, the range checks, the ramp-versus-admission bound. Three
# findings have been "a guard exists on one entry point and a later path routes
# around it", so the fix is not another guard -- it is that there is only one
# path that produces these values.
#
# NO `eval`. The previous version built the same values with `eval "VAR=\"$(...)\""`,
# which executes a command substitution embedded in an operator-supplied flag
# value: control and data sharing a channel (#3312), introduced by the fix for a
# parse problem. Associative arrays carry the values instead, and the resolver's
# JSON is read by a helper rather than interpreted by the shell.
declare -A EXPECT_BATCH EXPECT_MAXBYTES EXPECT_WAIT EXPECT_SCANS
SESSION_JSON="$(python3 "$SUPPORT" resolve-session \
  "$BATCH_SIZE" "${MAX_BATCH_BYTES:-NOT-REQUESTED}" \
  "${ADMISSION_WAIT_TIMEOUT_MS:-NOT-REQUESTED}" "$MAX_CONCURRENT_SCANS" \
  "$MIN_CORPUS_BYTES" "$MIN_SSTABLES" "$RAMP" "$CONTROL" \
  "$BASE_SERVER_EXTRA" "$HEAD_SERVER_EXTRA")" \
  || usage_error "the session configuration is unusable (the causes are named above)"
while IFS=$'\t' read -r _arm _field _value; do
  case "$_field" in
    batch_size_observed)       EXPECT_BATCH["$_arm"]="$_value" ;;
    max_batch_bytes_observed)  EXPECT_MAXBYTES["$_arm"]="$_value" ;;
    wait_timeout_ms_observed)  EXPECT_WAIT["$_arm"]="$_value" ;;
    max_concurrent_scans)      EXPECT_SCANS["$_arm"]="$_value" ;;
  esac
done < <(printf '%s' "$SESSION_JSON" | python3 -c '
import json, sys
for arm, fields in sorted(json.load(sys.stdin).items()):
    for field, value in sorted(fields.items()):
        sys.stdout.write("%s\t%s\t%s\n" % (arm, field, value))
')
for _arm in base head; do
  [ -n "${EXPECT_BATCH[$_arm]:-}" ] || usage_error \
    "the session resolver returned no batch size for the $_arm arm"
done
export AB_EXPECT_BASE_BATCH="${EXPECT_BATCH[base]}" AB_EXPECT_HEAD_BATCH="${EXPECT_BATCH[head]}"
export AB_EXPECT_BASE_MAXBYTES="${EXPECT_MAXBYTES[base]}" AB_EXPECT_HEAD_MAXBYTES="${EXPECT_MAXBYTES[head]}"
export AB_EXPECT_BASE_WAIT="${EXPECT_WAIT[base]}" AB_EXPECT_HEAD_WAIT="${EXPECT_WAIT[head]}"
export AB_EXPECT_BASE_SCANS="${EXPECT_SCANS[base]}" AB_EXPECT_HEAD_SCANS="${EXPECT_SCANS[head]}"
say "expected-config base batch-size ${EXPECT_BATCH[base]} max-batch-bytes ${EXPECT_MAXBYTES[base]} wait-timeout-ms ${EXPECT_WAIT[base]} max-concurrent-scans ${EXPECT_SCANS[base]}"
say "expected-config head batch-size ${EXPECT_BATCH[head]} max-batch-bytes ${EXPECT_MAXBYTES[head]} wait-timeout-ms ${EXPECT_WAIT[head]} max-concurrent-scans ${EXPECT_SCANS[head]}"

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
# ---------------------------------------------------------------------------
# THE SESSION OWNS EVERYTHING IT WRITES. This replaces four successive attempts
# to share `<work-dir>/results` safely -- unarmed ledgers, staging directories,
# ordered promotion -- each of which fixed the instance in front of it and left
# the next layer. Four findings in one path says the SHARED MUTABLE LOCATION is
# the defect, not the sequencing around it.
#
# So there is no shared mutable location. Every session writes to its OWN
# immutable directory, `<work-dir>/run-<session-id>/`, which no other session can
# name. Nothing is ever promoted, overwritten or truncated, so:
#
#   * a failed pre-flight, a failed build, a lost port and a killed session all
#     leave every earlier session's results byte-identical, because no code path
#     writes outside this session's own directory;
#   * "a manifest never references a file from another session" is true BY
#     CONSTRUCTION -- the manifest and every JSONL it names are the only things
#     in that directory, written by one process that owns it;
#   * there is nothing to make atomic, because there is nothing shared.
#
# `<work-dir>/latest` is a convenience symlink, updated after each completed
# replicate. It is the ONLY shared name, it holds no data, and its worst failure
# is pointing at a complete EARLIER session -- coherent, never corrupt. The
# analyzer command this script prints names the session directory explicitly, so
# what gets certified is never the symlink.
# ---------------------------------------------------------------------------
# CANONICALISED BEFORE ANYTHING IS DERIVED FROM IT. `CARGO_TARGET_DIR` is read
# by cargo AFTER the driver has `cd`-ed into the arm's worktree, so a relative
# --work-dir put the target directory under the WORKTREE while this script kept
# checking the original-relative path -- both arms compiling, then
# `build-incomplete`. Same failure economics as a missing command: it only shows
# after the expensive step, on a box billed by the hour. Every path below (the
# session directory, the worktrees, the target directories, the lock) is derived
# from this one value, so canonicalising here fixes all of them at once.
mkdir -p "$WORK_DIR" 2>/dev/null || usage_error "--work-dir $WORK_DIR cannot be created"
WORK_DIR="$(cd "$WORK_DIR" 2>/dev/null && pwd -P || true)"
[ -n "$WORK_DIR" ] || usage_error "--work-dir could not be resolved to an absolute path"

SESSION_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="$WORK_DIR/run-$SESSION_ID"
LOG_DIR="$RUN_DIR/logs"
RUNS_JSONL="$RUN_DIR/runs.jsonl"


# ---------------------------------------------------------------------------
# Cleanup. Registered NOW, before the resources it frees can exist -- this repo
# has three separate findings in the family "the fix that added a resource did
# not register it with the signal path". A `die` between starting a server and
# reaping it (a failed prewarm, a server that never bound, any `set -e` abort) or
# a Ctrl-C used to leave a live cqlite-flight holding the port.
# ---------------------------------------------------------------------------
SRV_PID=''
SRV_START=''

# IDENTITY, VERIFIED BEFORE EVERY SIGNAL -- not just before the KILL. A pid alone
# is not an identity: if the server exits during a long load-generator step and
# bash reaps it, the number can be reused before we signal, and on a nine-lane
# box the process that inherits it is most likely a PEER'S. This repo has the
# incident -- a pattern-based `pkill` killed a peer's gate at component 28 of 30.
# The start time (field 22 of /proc/<pid>/stat) is what makes the pid unique:
# recorded at launch, compared here, so a reused number fails the check.
is_our_server() { # <pid>
  [ -n "${1:-}" ] || return 1
  [ -r "/proc/$1/cmdline" ] || return 1
  tr '\0' ' ' < "/proc/$1/cmdline" 2>/dev/null | grep -q 'cqlite-flight' || return 1
  # An unrecorded start time cannot confirm identity, and an unconfirmed identity
  # must not be signalled: no start time means NOT ours.
  [ -n "${SRV_START:-}" ] || return 1
  [ "$(awk '{print $22}' "/proc/$1/stat" 2>/dev/null || true)" = "$SRV_START" ]
}

# ONE REAP, IDEMPOTENT. This is the THIRD pass at "when is the pid still ours":
# round 2 cleared it first for re-entrancy, round 5 left it set on the
# readiness-failure path, and clearing it first then bit again -- a signal during
# the 30-second wait left `cleanup` seeing no server, so it skipped the kill and
# released the session lock with the child still running. Three passes at one
# ordering question means the TWO-STATE VARIABLE is the problem.
#
# So the identity is released only when the process is CONFIRMED GONE, and the
# function is safe to run twice: `kill` on a dead pid is a no-op, and
# `is_our_server` -- which round 5 added and which compares the recorded start
# time -- makes signalling a REUSED pid impossible. That check is what made the
# clear-first trick unnecessary: re-entering can no longer signal a stranger, so
# there is no window in which the identity is neither held nor released.
reap_server() {
  [ -n "$SRV_PID" ] || return 0
  local pid="$SRV_PID"
  if ! is_our_server "$pid"; then
    # Already gone, or the number now belongs to somebody else. Either way this
    # session no longer owns it.
    wait "$pid" 2>/dev/null || true
    SRV_PID=''
    SRV_START=''
    return 0
  fi
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
  # Released ONLY here, after the process is confirmed gone -- so a signal at any
  # earlier point finds the identity still held and finishes the job.
  SRV_PID=''
  SRV_START=''
}

cleanup() {
  reap_server
  # RELEASED ONLY IF WE HOLD IT. The trap is armed BEFORE the lock is acquired --
  # that is the whole point of the ordering -- so `cleanup` runs on the path
  # where acquisition FAILED because a peer holds the directory. Keying the
  # rmdir on the directory existing would there delete the PEER'S lock, turning
  # a leak into a mutual-exclusion failure: strictly worse, and silent. The flag
  # is set only by the branch that created it.
  if [ "${WE_HOLD_LOCK:-0}" = 1 ] && [ -n "${SESSION_LOCK:-}" ]; then
    rmdir "$SESSION_LOCK" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

# ---------------------------------------------------------------------------
# Resources. EVERYTHING FALLIBLE HAPPENS BELOW THIS LINE, because the traps
# above are now armed -- the fourth instance of "cleanup registration precedes
# resource creation" in this repository, and the first in this file. The lock
# used to be taken ~80 lines earlier, so a failure creating the run directory or
# the ledger left `.session-lock` behind and blocked EVERY later session
# permanently. On a metered rig that ends with an operator deleting a lock file
# they do not understand, which is the worst possible remedy for a guard whose
# entire job is to stop two sessions sharing a box.
#
# The lock is still taken before the session directory exists, so a session
# refused here leaves nothing at all behind -- not even an empty directory. It
# is not a data-integrity guard (separate directories made that unnecessary) but
# a MEASUREMENT-VALIDITY one: two concurrent sessions on one box contend for CPU
# and page cache and invalidate each other's numbers, which no amount of file
# isolation fixes.
# ---------------------------------------------------------------------------
WE_HOLD_LOCK=0
SESSION_LOCK="$WORK_DIR/.session-lock"
if mkdir "$SESSION_LOCK" 2>/dev/null; then
  # Set on the very next statement after the atomic create. A signal delivered
  # in between leaks the directory rather than releasing a lock we do not hold --
  # unavoidable in shell, and the safe direction of the two.
  WE_HOLD_LOCK=1
else
  warn "cause work-dir-busy"
  warn "cause-detail another session holds $SESSION_LOCK. This is not about files -- each session now writes only to its own directory -- it is about CPU: two measurement sessions on one box invalidate each other. If nothing is running, remove that directory."
  exit 2
fi
mkdir -p "$RUN_DIR" "$LOG_DIR"
: > "$RUNS_JSONL"

# ---------------------------------------------------------------------------
# Manifest. Rewritten after every completed run, so an interrupted session
# leaves a truthful SHORT manifest rather than nothing.
# ---------------------------------------------------------------------------
BASE_SHA=''
HEAD_SHA=''
CORPUS_BYTES=0
CORPUS_FILES=0

write_manifest() {
  # ATOMIC. Truncating and rewriting in place means a crash mid-write leaves a
  # half-written manifest where a complete earlier one was -- which contradicts
  # the guarantee that an interrupted session leaves a TRUTHFUL SHORT manifest,
  # the property the whole per-session design exists to provide. Write to a temp
  # file in the same directory, flush, then rename; a failure anywhere before the
  # rename leaves the previous manifest exactly as it was.
  python3 - "$RUN_DIR/manifest.json" "$RUNS_JSONL" <<'PYEOF'
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


def _ticket_content():
    """The frozen ticket, parsed, IN the manifest.

    The digest proves every run read the same bytes; the CONTENT is what lets a
    reader six months on see what was actually served without needing the
    session directory to still exist. Both, because they answer different
    questions and the cheap one is not the useful one.
    """
    path = env("AB_TICKET_TEMPLATE", "")
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        # The manifest records what it could read. A ticket that does not parse
        # here cannot have reached this point -- validation is upstream -- so
        # this is a truthful null rather than a silent substitution.
        return None


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
    # ONE client, recorded once at session level AND per run, so the analyzer can
    # check rather than assume that both arms were driven by the same binary.
    "loadgen": {
        "commit": env("AB_LOADGEN_SHA", "NOT-RECORDED"),
        "ref": env("AB_LOADGEN_REF", ""),
    },
    "workload": {
        "shape": env("AB_SHAPE", ""),
        # The spelling the operator typed, kept because it is what they will
        # recognise in a diagnostic. `shape` is what the records carry.
        "shape_requested": env("AB_SHAPE_REQUESTED", ""),
        "profile": env("AB_PROFILE", None),
        "ramp": env("AB_RAMP", ""),
        "step_duration": env("AB_STEP_DURATION", ""),
        # The EFFECTIVE value, not the requested one: the warming pass runs only
        # for a warm session, so a cold run recording `prewarm: true` describes a
        # pass that never happened. Same requested-versus-actual distinction as
        # the admission ceiling, the batch size, the CPU affinity and the pair
        # order -- one field further on.
        "prewarm": env("AB_PREWARM", "0") == "1"
        and env("AB_TEMPERATURE", "") == "warm",
        "prewarm_requested": env("AB_PREWARM", "0") == "1",
        "server_cpus": env("AB_SERVER_CPUS", "none-unpinned"),
        # The EFFECTIVE state, beside the requested set. VERIFIED only when a
        # run read the server's own Cpus_allowed_list and it matched.
        "affinity_state": env("AB_AFFINITY_STATE", "NOT-RECORDED"),
        "client_cpus": env("AB_CLIENT_CPUS", "none-unpinned"),
        "temperature": env("AB_TEMPERATURE", ""),
        "ticket_template": env("AB_TICKET_TEMPLATE", ""),
        "ticket_original": env("AB_TICKET_ORIGINAL", ""),
        "ticket_sha256": env("AB_TICKET_SHA", ""),
        "ticket_canonical_sha256": env("AB_TICKET_CANON_SHA", ""),
        "ticket_content": _ticket_content(),
        "merge_path": env("AB_MERGE_PATH", ""),
        # THE SERVER OPTIONS ARE NOT RECORDED HERE, DELIBERATELY. They used to
        # be, read from the GLOBAL options -- so identical per-arm extras
        # (`--base-server-extra '--batch-size 1'` plus the matching head option)
        # ran both servers at 1 while this block said 8192. The resolved
        # per-arm values already live in `expected_server_config`, which is what
        # the launcher built the argv from and what the startup read-back is
        # compared against, so these fields were a SECOND COPY of facts that
        # already had a home -- and a second copy is a thing that can drift
        # again once synced.
        #
        # A GLOBAL FIELD ALSO HAS A QUESTION IT CANNOT ANSWER: what to record
        # when the arms legitimately differ, which under a sensitivity control
        # they do BY DESIGN. Per-arm values belong per-arm, and no global claim
        # is the honest amount of claim.
        #
        # `max_concurrent_scans` STAYS, and the distinction is not cosmetic:
        # OVERRIDABLE is exactly the three options above
        # (ab_driver_support.py:858), so the admission ceiling CANNOT differ
        # between arms -- there is no route by which this global could disagree
        # with what either server was launched with. A field that cannot drift
        # is not a second source. Deleting it too would have been a blanket
        # applied where a distinction was available.
        "max_concurrent_scans": _int_or_none(env("AB_MAX_CONCURRENT_SCANS")),
        "step_duration_seconds": float(env("AB_STEP_DURATION_SECONDS", "0")),
    },
    "control": env("AB_CONTROL") or None,
    # DECLARED, so the analyzer can permit exactly these differences under a
    # control label and nothing else. Recorded as data rather than inferred from
    # the extras string, which would be a second implementation of the rule.
    "expected_server_config": {
        arm: {
            "batch_size_observed": env("AB_EXPECT_%s_BATCH" % arm.upper()),
            "max_batch_bytes_observed": env("AB_EXPECT_%s_MAXBYTES" % arm.upper()),
            "wait_timeout_ms_observed": env("AB_EXPECT_%s_WAIT" % arm.upper()),
        }
        for arm in ("base", "head")
    },
    "server_extra": {
        "base": env("AB_BASE_SERVER_EXTRA", ""),
        "head": env("AB_HEAD_SERVER_EXTRA", ""),
    },
    "corpus": {
        "path": env("AB_CORPUS", ""),
        "served_dir": env("AB_SERVED_DIR", ""),
        "compressed": env("AB_CORPUS_COMPRESSED", "") == "compressed",
        "compression": env("AB_COMPRESSION_STATE", "NOT-RECORDED"),
        "compression_detail": env("AB_COMPRESSION_DETAIL", "NOT-RECORDED"),
        "storage": env("AB_STORAGE", "NOT-RECORDED"),
        "storage_attestation": env("AB_STORAGE_ATTESTATION", "") or None,
        "storage_detail": env("AB_STORAGE_DETAIL", "NOT-RECORDED"),
        "data_db_bytes": int(env("AB_CORPUS_BYTES", "0")),
        "data_db_files": int(env("AB_CORPUS_FILES", "0")),
        "min_bytes_required": int(env("AB_MIN_CORPUS_BYTES", "0")),
        "min_sstables_required": int(env("AB_MIN_SSTABLES", "0")),
        "rows_declared": int(rows) if rows else None,
    },
    "host": {
        "instance_type": env("AB_INSTANCE_TYPE", "NOT-RECORDED"),
        # RENAMED from `nproc`, deliberately. The old key invited the mistake it
        # was part of: it read as "the machine's CPUs" and held "the CPUs this
        # process may use". Both facts are recorded now, under names that say
        # which is which.
        "process_cpus": int(env("AB_PROCESS_CPUS", "0")),
        # An INTEGER when the machine could be sized, and the explicit
        # NOT-MEASURABLE string otherwise. Never a numeric-looking string: the
        # analyzer compares it to a number, and a string that looks like one is
        # how a comparison silently stops matching.
        "hardware_cpus": (
            int(env("AB_HARDWARE_CPUS", ""))
            if (env("AB_HARDWARE_CPUS", "") or "").isdigit()
            else "NOT-MEASURABLE"
        ),
        "hardware_cpus_detail": env("AB_HARDWARE_CPUS_DETAIL", "NOT-RECORDED"),
        "loadavg1": env("AB_LOADAVG1", "NOT-RECORDED"),
        "load_limit": env("AB_LOAD_LIMIT", "NOT-RECORDED"),
        "contention": env("AB_CONTENTION", "NOT-RECORDED"),
        "kernel": env("AB_KERNEL", "NOT-RECORDED"),
    },
    "runs": runs,
}
tmp_path = out_path + ".tmp.%d" % os.getpid()
with open(tmp_path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
os.replace(tmp_path, out_path)
PYEOF
}

export AB_DRIVER_VERSION="$DRIVER_VERSION"
export AB_REPLICATES="$REPLICATES"
export AB_BASE_REF="$BASE_REF" AB_HEAD_REF="$HEAD_REF"
# AB_SHAPE IS NOT EXPORTED HERE. It names the label the RECORDS will carry, and
# that label does not exist until `--shape` is canonicalised below. Exporting
# the raw value produced canonical labels in the JSONL and RAW labels in the
# manifest, so an accepted alias (`limit` -> `limit-k`) made the analyzer reject
# a valid COMPLETED session with shape-record-mismatch. The requested spelling
# is kept under its own name, exactly as the ticket's original path is.
export AB_SHAPE_REQUESTED="$SHAPE"
export AB_RAMP="$RAMP" AB_STEP_DURATION="$STEP_DURATION"
export AB_PROFILE="$PROFILE"
export AB_STEP_DURATION_SECONDS="$STEP_DURATION_SECONDS"
export AB_PREWARM="$PREWARM" AB_TEMPERATURE="$TEMPERATURE"
# AB_TICKET_TEMPLATE IS NOT EXPORTED HERE. It names the ticket THE RUNS READ,
# which does not exist until the freeze -- and `die` writes a manifest, so an
# abort before the freeze would otherwise record a path nothing ever read. What
# IS useful on an early abort is which template was REQUESTED, so that is
# exported under its own name and the executed-path field stays empty until
# there is an executed path.
export AB_TICKET_ORIGINAL="$TICKET_TEMPLATE"
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
# Asymmetric per-arm flags mean the arms are NOT serving the same configuration,
# so whatever is measured is the injected difference and not the commit pair's.
# That is the definition of a control, and it may not wear a measurement's
# verdict -- the analyzer disclaims a labelled session, and this makes sure one
# exists to disclaim.
if [ "$BASE_SERVER_EXTRA" != "$HEAD_SERVER_EXTRA" ] && [ -z "$CONTROL" ]; then
  usage_error "--base-server-extra and --head-server-extra differ, which serves the two arms under different configurations; that is a control, not a measurement. Pass --control <label> so the verdict is disclaimed"
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
# TWO DIFFERENT FACTS, RECORDED SEPARATELY. `nproc` is the CPUs available to
# THIS PROCESS -- it honours the affinity mask, so on a 16-CPU box under
# `taskset -c 0-3` it reports 4. The rig requirement is about the MACHINE, so
# the machine's size is read from the sysfs online set, which is unaffected by
# any mask. Recording only one of these is what let a pinned large rig pass a
# guard whose own text says pinning must not qualify it.
export AB_PROCESS_CPUS="$(nproc)"
AB_HW_RAW="$(python3 "$SUPPORT" hardware-cpus 2>/dev/null || echo 'NOT-MEASURABLE the probe failed')"
export AB_HARDWARE_CPUS="${AB_HW_RAW%% *}"
export AB_HARDWARE_CPUS_DETAIL="${AB_HW_RAW#* }"
export AB_LOADAVG1="$(cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo NOT-RECORDED)"
export AB_KERNEL="$(uname -sr)"
say "host instance-type $AB_INSTANCE_TYPE hardware-cpus $AB_HARDWARE_CPUS ($AB_HARDWARE_CPUS_DETAIL) process-cpus $AB_PROCESS_CPUS loadavg1 $AB_LOADAVG1 kernel $AB_KERNEL"
if [ "$AB_HARDWARE_CPUS" != "NOT-MEASURABLE" ] && [ "$AB_PROCESS_CPUS" != "$AB_HARDWARE_CPUS" ]; then
  say "host NOTE this process sees $AB_PROCESS_CPUS of the machine's $AB_HARDWARE_CPUS CPUs, so an affinity mask is in force. The rig requirement is about the MACHINE and is checked against the machine's count"
fi
# CHECK THE PROPERTY THE LABEL STOOD FOR. The acceptance criteria say "field i4i
# rig", but they do not care about the string -- they care about what it stood
# for, and one load-bearing part is an UNCONTENDED host. A co-scheduled build or
# a peer lane steals exactly the CPU this measurement is denominated in, and it
# steals it from whichever arm happens to be running, which is noise the pairing
# cannot cancel. Judged against the core count rather than a constant: loadavg 3
# is idle on 64 cores and saturated on 4.
AB_LOAD_LIMIT="$(awk -v n="$AB_PROCESS_CPUS" 'BEGIN { l = n / 2; if (l < 2) l = 2; printf "%.2f", l }')"
export AB_LOAD_LIMIT
if [ "$AB_LOADAVG1" = "NOT-RECORDED" ]; then
  AB_CONTENTION='NOT-MEASURABLE'
  warn "host contention NOT-MEASURABLE -- /proc/loadavg could not be read, so this run cannot say whether the host was quiet. That is a different fact from a quiet host, and it is recorded as itself"
elif awk -v l="$AB_LOADAVG1" -v m="$AB_LOAD_LIMIT" 'BEGIN { exit !(l > m) }'; then
  AB_CONTENTION='CONTENDED'
else
  AB_CONTENTION='QUIET'
fi
export AB_CONTENTION
say "host contention $AB_CONTENTION loadavg1 $AB_LOADAVG1 limit $AB_LOAD_LIMIT (nproc/2, floor 2)"
if [ "$AB_CONTENTION" = "CONTENDED" ]; then
  # DISCLOSED, NOT REFUSED, and the reason is a measured false-red path rather
  # than a preference. loadavg1 is a DECAYING one-minute average, so it reports
  # load this session has already finished causing: the driver builds three
  # worktrees itself, and a re-run minutes later would be refused for the load
  # its own previous attempt left behind. Refusing there reds a correct rig at
  # the exact moment an operator is iterating on a metered box -- and the escape
  # (--control) disclaims the whole verdict, so the refusal would buy nothing and
  # cost the session. Observed directly: this threshold made a DETERMINISTIC test
  # suite flaky on a shared host, crossing the limit between two cases of one run.
  warn "host contention CONTENDED -- the 1-minute load average is $AB_LOADAVG1 on $AB_PROCESS_CPUS available CPUs (limit $AB_LOAD_LIMIT), so something else was using this host at session start. Co-scheduled work steals CPU from whichever arm is running and the pairing cannot cancel it. This is RECORDED and reported beside the verdict, not refused; confirm the box is yours before reporting the result"
fi

# THE SHAPE IS CANONICALISED BEFORE ANYTHING READS IT, controls included. The
# driver used to carry the RAW string while flight-loadgen emitted the CANONICAL
# label, so `--shape limit` produced records labelled `limit-k` that the
# manifest reconciliation then rejected as a mismatch -- after all three release
# builds. And an unknown shape was not caught here at all: it failed when the
# load generator parsed it, at the same cost. Mirrors Shape::parse
# (tools/flight-loadgen/src/shape.rs:34-55), aliases and case-insensitivity
# included -- `FULL` is accepted there, so refusing it here would be the
# too-strict half that reds a correct session.
SHAPE_CANONICAL="$(python3 "$SUPPORT" canonical-shape "$SHAPE")" \
  || usage_error "--shape '$SHAPE' is not a shape flight-loadgen accepts (the cause is named above)"
if [ "$SHAPE_CANONICAL" != "$SHAPE" ]; then
  say "shape '$SHAPE' canonicalised to '$SHAPE_CANONICAL' -- flight-loadgen emits the canonical label in every record, so the driver carries the same value the records will"
fi
SHAPE="$SHAPE_CANONICAL"
# EXPORTED ON THE SAME BREATH AS THE ASSIGNMENT, for the reason the ticket
# export is: `die` writes a manifest, so any interval between the transform and
# the export is one in which a manifest records the untransformed value.
export AB_SHAPE="$SHAPE"

[ -f "$TICKET_TEMPLATE" ] || die ticket-template-absent "$TICKET_TEMPLATE does not exist"
# VALIDATE-THEN-REREAD IS A TOCTOU ON THE MEASUREMENT INPUT. The template was
# checked once, before the builds, and then re-read from its ORIGINAL MUTABLE
# PATH for every prewarm and every measured run -- so editing it mid-session
# makes the arms execute different filters, projections, aggregations or token
# ranges while every record still reports shape `full`: an invalid target-band
# verdict that looks clean. This driver already applies the opposite principle
# everywhere else (per-session immutable directories, ONE flight-loadgen built
# from --loadgen-ref), and this was the hole in it.
#
# So: copy FIRST, validate the COPY, and let nothing read the original again.
# Copying after validation would leave the same window, one step narrower.
TICKET_FROZEN="$RUN_DIR/ticket.json"
cp -- "$TICKET_TEMPLATE" "$TICKET_FROZEN" \
  || die ticket-template-unreadable "$TICKET_TEMPLATE could not be copied into the session directory"
# A READ-ONLY BIT IS NOT IMMUTABILITY. `chmod a-w` is a PERMISSION; the file
# can still be replaced through the writable parent directory, and the mode says
# nothing about the CONTENTS. This is the presence-versus-property shape applied
# to the one input the whole measurement rests on -- a noun ("read-only")
# standing in for the adjective ("unchanged") -- and it is the third round on
# this freeze. The bit stays as a speed bump; the DIGEST is the check.
chmod a-w "$TICKET_FROZEN" 2>/dev/null || true
TICKET_ORIGINAL="$TICKET_TEMPLATE"
TICKET_TEMPLATE="$TICKET_FROZEN"
# EXPORTED ON THE SAME BREATH AS THE ASSIGNMENT, not eleven lines later. `die`
# writes a manifest, and the digest step below can die -- so a re-export after
# it left a REACHABLE window, not a latent one, in which a manifest recorded the
# original while the frozen copy already existed on disk. An interval in which
# the wrong value is exported should not exist at all rather than be argued to
# be unreachable.
export AB_TICKET_TEMPLATE="$TICKET_FROZEN"
export AB_TICKET_ORIGINAL="$TICKET_ORIGINAL"
TICKET_SHA="$(python3 - "$TICKET_FROZEN" <<'PYEOF'
import hashlib
import sys

with open(sys.argv[1], "rb") as handle:
    sys.stdout.write(hashlib.sha256(handle.read()).hexdigest())
PYEOF
)" || die ticket-template-unreadable "the frozen ticket could not be digested"
# Round 17 pinned the EXECUTION and left the RECORD, so a mid-session edit
# produced a manifest documenting a ticket nobody ran -- worse than the original
# defect, which at least had the honesty of being consistently wrong. The path
# is exported above, at the assignment; only the digest can be exported here,
# because only here does it exist.
export AB_TICKET_SHA="$TICKET_SHA"
# TWO DIGESTS, because they answer different questions. The raw-byte one above
# is what the driver re-checks around every invocation. This one is over a
# CANONICAL serialisation of the parsed ticket, so the ANALYZER can recompute it
# from `ticket_content` alone -- a manifest whose content was edited without
# updating the digest is then self-contradicting rather than merely unverified.
TICKET_CANON_SHA="$(python3 - "$TICKET_FROZEN" <<'PYEOF'
import hashlib
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
sys.stdout.write(hashlib.sha256(canonical.encode("utf-8")).hexdigest())
PYEOF
)" || die ticket-template-unreadable "the frozen ticket could not be canonically digested"
export AB_TICKET_CANON_SHA="$TICKET_CANON_SHA"
say "ticket frozen into the session directory as $TICKET_FROZEN sha256 $TICKET_SHA -- every run reads this copy, never $TICKET_ORIGINAL"
# THE WORKLOAD MUST MATCH THE CLAIM THE REPORT WILL MAKE ABOUT IT. The #3649
# target band is defined for `flight-loadgen --shape full` over the whole ring
# (the AC's first line), so a point, limit-k, filtered, projected or aggregating
# session receiving a verdict against that band is a wrong answer wearing a
# right-looking shape. Checking that the file is JSON never checked what was in
# it. A CONTROL may use any shape -- its verdict is already disclaimed.
# THE PROFILE IS DECLARED WHERE THE MEASUREMENT HAPPENS, not where it is read.
# It used to be an ANALYSIS-time flag defaulting to `narrow`, so the same data
# yielded different verdicts under different flags and a wide-row session
# analysed with the default was silently scored against the narrow band. It
# cannot be derived -- the band's source defines narrow and wide qualitatively
# with no numeric boundary, so any threshold would be invented, and deriving it
# from the table name is the label-not-property mistake. So it is declared once,
# here, and REQUIRED: a default is exactly the defect.
if [ -z "$CONTROL" ]; then
  case "$PROFILE" in
    narrow|wide) ;;
    '') usage_error "--profile is REQUIRED for a measurement: the target band differs by workload (~1.1-1.25x narrow, ~1.05-1.1x wide) and nothing can derive which one this session is. There is deliberately no default -- one silently scored wide-row sessions against the narrow band" ;;
    *)  usage_error "--profile is '$PROFILE'; it must be narrow or wide" ;;
  esac
elif [ -n "$PROFILE" ] && [ "$PROFILE" != narrow ] && [ "$PROFILE" != wide ]; then
  usage_error "--profile is '$PROFILE'; it must be narrow or wide"
fi
if [ -z "$CONTROL" ]; then
  [ "$SHAPE" = "full" ] || usage_error \
    "--shape is '$SHAPE', but the #3649 target band is defined for --shape full over the whole ring. Run it as a control (--control <label>) if you want another shape; its verdict is then disclaimed rather than scored against the band"
  python3 "$SUPPORT" validate-ticket "$TICKET_TEMPLATE" \
    || die ticket-not-full-ring "$TICKET_TEMPLATE does not describe a full-ring scan (the cause is named above)"
else
  # A CONTROL MAY NARROW THE WORKLOAD; IT MAY NOT SHIP AN UNDESERIALISABLE
  # TICKET. This branch used to check only that the file was JSON, which widened
  # the very gap the schema check exists to close: a control that cannot be
  # deserialised wastes exactly the same three release builds as a measurement,
  # and on a metered box that is the whole cost. So the SCHEMA half applies to
  # every session and only the FULL-RING half is a measurement restriction.
  python3 "$SUPPORT" validate-ticket-schema "$TICKET_TEMPLATE" \
    || die ticket-schema-invalid "$TICKET_TEMPLATE would not deserialise into a FlightTicket (the cause is named above)"
  say "shape $SHAPE ticket SCHEMA-CHECKED, full-ring restrictions NOT applied -- a control may narrow the workload; the analyzer disclaims its verdict"
fi

[ -d "$CORPUS" ] || die corpus-absent "$CORPUS is not a directory"
# THE CENSUS DESCRIBES THE TABLE UNDER MEASUREMENT, NOT THE DISK. It used to
# scan the whole data root recursively, so unrelated tables, `snapshots/`
# subtrees and hard-linked copies all counted toward the size floor AND toward
# the >=2-SSTable check -- the check that exists to stop the #3058 single-source
# bypass. A green census over files the server never opens, with a single-source
# served table underneath, is the exact phantom this harness was built to
# prevent. The helper resolves the ticket the way `DirSource::resolve` does and
# enumerates that ONE directory, flat, as the producer does.
CENSUS="$(python3 "$SUPPORT" census-served "$CORPUS" "$TICKET_TEMPLATE")" \
  || die corpus-census-failed "the served-directory census failed (the cause is named above)"
CORPUS_FILES="${CENSUS%% *}"
CENSUS_REST="${CENSUS#* }"
CORPUS_BYTES="${CENSUS_REST%% *}"
CENSUS_REST="${CENSUS_REST#* }"
CORPUS_COMPRESSED="${CENSUS_REST%% *}"
SERVED_DIR="${CENSUS_REST#* }"
export AB_CORPUS_COMPRESSED="$CORPUS_COMPRESSED"
# EXPORTED, because `write_manifest` reads these from the environment. Unexported,
# every manifest recorded the census as zero -- and the corpus size is a thing the
# acceptance criteria explicitly require the report to state.
export AB_SERVED_DIR="$SERVED_DIR"
export AB_CORPUS_FILES="$CORPUS_FILES" AB_CORPUS_BYTES="$CORPUS_BYTES"
say "corpus path $CORPUS served-dir $SERVED_DIR data-db-files $CORPUS_FILES data-db-bytes $CORPUS_BYTES"
# THE REQUIREMENT IS THE ALGORITHM, NOT THE EXISTENCE OF METADATA. Round 12
# enforced "a non-empty CompressionInfo.db exists" while the comment beside it
# said "the field is LZ4" -- so Snappy, Deflate, Zstd or NOOP metadata, and any
# corrupt-but-non-empty file, passed as the required corpus. Scoring different
# decompression work against an LZ4-derived band is a wrong number, and it is
# the same requirement-versus-check gap one level deeper: we enforced EXISTENCE
# where the requirement was IDENTITY.
COMPRESSION_RAW="$(python3 "$SUPPORT" probe-compression "$SERVED_DIR" 2>/dev/null || echo 'UNPARSEABLE the probe failed')"
COMPRESSION_STATE="${COMPRESSION_RAW%% *}"
COMPRESSION_DETAIL="${COMPRESSION_RAW#* }"
export AB_COMPRESSION_STATE="$COMPRESSION_STATE" AB_COMPRESSION_DETAIL="$COMPRESSION_DETAIL"
say "corpus compression $COMPRESSION_STATE ($COMPRESSION_DETAIL) -- the field is LZ4 (throughput-program-2026-07.md line 21), so the target band is defined for LZ4 decode work and nothing else"
if [ "$COMPRESSION_STATE" != "LZ4" ] && [ -z "$CONTROL" ]; then
  case "$COMPRESSION_STATE" in
    MISSING)
      die corpus-uncompressed \
        "the served directory $SERVED_DIR holds SSTables with no usable CompressionInfo.db ($COMPRESSION_DETAIL). Removing LZ4 decode removes real CPU from the denominator, so an uncompressed corpus inflates the measured ratio -- the failure is in the direction that looks like success. Run it as a --control if you mean to measure an uncompressed corpus" ;;
    OTHER)
      die corpus-wrong-compressor \
        "the served corpus is compressed with $COMPRESSION_DETAIL, not LZ4. The target band was derived against LZ4 decode work, so a ratio measured against different decompression is not comparable to it -- regenerate the corpus with LZ4Compressor, or run it as a --control" ;;
    UNRECOGNISED)
      die corpus-unknown-compressor \
        "the served corpus names a compressor this probe does not know ($COMPRESSION_DETAIL). The header parses, so the file is not damaged -- but an unknown compressor is not evidence of LZ4, and the band is LZ4's. Run it as a --control if you mean to measure it" ;;
    NO-SSTABLES)
      die corpus-empty "the served directory $SERVED_DIR holds no *-Data.db files" ;;
    *)
      die corpus-compression-unparseable \
        "a served CompressionInfo.db does not parse as a compression header ($COMPRESSION_DETAIL). A non-empty file is not a valid one, and this corpus cannot be described -- so it cannot be measured" ;;
  esac
fi
# THE OTHER HALF OF WHAT `i4i` STOOD FOR: LOCAL NVMe, NOT NETWORK STORAGE. This
# is what disqualified this lane's own host -- `lsblk` reports *Amazon Elastic
# Block Store*. A network-backed corpus puts a variable-latency hop inside the
# read path being measured, which is the confound the rig was chosen to remove.
# A hostname pattern would red a correct rig the day someone uses i4i.2xlarge;
# the device model does not.
AB_STORAGE_RAW="$(python3 "$SUPPORT" probe-storage "$SERVED_DIR" 2>/dev/null || echo 'NOT-MEASURABLE - the probe failed')"
AB_STORAGE="${AB_STORAGE_RAW%% *}"
AB_STORAGE_DETAIL="${AB_STORAGE_RAW#* }"
export AB_STORAGE AB_STORAGE_DETAIL
say "corpus storage $AB_STORAGE ($AB_STORAGE_DETAIL)"
# THE ATTESTATION COVERS IGNORANCE, NEVER EVIDENCE. An operator may assert that
# a device this probe does not recognise is local; nobody may assert that a
# device affirmatively identified as network-attached is not. That asymmetry is
# the whole safety of the override -- without it, the one thing the check exists
# to refuse becomes the one thing a flag turns off.
if [ -n "$ATTEST_LOCAL_STORAGE" ]; then
  case "$(printf '%s' "$ATTEST_LOCAL_STORAGE" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')" in
    ''|why|todo|tbd|reason|placeholder|xxx|na|n/a)
      usage_error "--attest-local-storage needs a reason that records WHY this device is known to be local; '$ATTEST_LOCAL_STORAGE' records nothing. The attestation is printed beside the verdict and is the only evidence the number has" ;;
  esac
  case "$ATTEST_LOCAL_STORAGE" in
    *'<'*'>'*)
      usage_error "--attest-local-storage still carries an unsubstituted placeholder ('$ATTEST_LOCAL_STORAGE'); write the actual reason" ;;
  esac
  if [ "$AB_STORAGE" = "NETWORK" ]; then
    die corpus-network-storage-attested \
      "--attest-local-storage was passed, but the served directory $SERVED_DIR is AFFIRMATIVELY identified as network storage ($AB_STORAGE_DETAIL). An attestation covers a device this probe cannot RECOGNISE; it does not overrule one it has identified. Move the corpus to instance storage, or run this as a --control"
  fi
  say "corpus storage ATTESTED local by the operator -- $ATTEST_LOCAL_STORAGE (probe said $AB_STORAGE). This attestation is recorded in the manifest and printed beside the verdict"
fi
export AB_STORAGE_ATTESTATION="$ATTEST_LOCAL_STORAGE"
if [ "$AB_STORAGE" = "NETWORK" ] && [ -z "$CONTROL" ]; then
  # WARNED HERE, REFUSED AT ANALYSIS. The refusal belongs where the false claim
  # would be made -- the verdict -- and putting it here as well would make this
  # instrument's own testability depend on where the harness's scratch directory
  # happens to live: `df` reports `/dev/root` on this lane box, which probes
  # NOT-MEASURABLE, so the end-to-end sessions pass BY ACCIDENT and would refuse
  # on a box that names a real device. A guard whose verdict turns on that is not
  # a guard. The operator loses nothing: this fires at pre-flight, before the
  # builds, and names the exact refusal the analysis will produce.
  warn "corpus storage NETWORK -- the served directory $SERVED_DIR is backed by network storage ($AB_STORAGE_DETAIL). The #3649 rig is specified as a field i4i box for the property that its corpus is on LOCAL NVMe: a network hop inside the read path is variable latency added to the quantity being measured. THIS SESSION WILL NOT YIELD A VERDICT -- analyze-ab.py refuses it with cause corpus-network-storage. Move the corpus to instance storage now, or re-run with --control <label>"
elif [ "$AB_STORAGE" != "LOCAL" ] && [ -z "$CONTROL" ] && [ -z "$ATTEST_LOCAL_STORAGE" ]; then
  # NOT a verified local disk, so NOT a measurement. The acceptance criteria
  # REQUIRE local NVMe; "we could not tell" does not satisfy a requirement, and
  # the previous form disclosed it and let the verdict through -- a pass derived
  # from the absence of a bad signal, one level up from the classifier's own.
  warn "corpus storage $AB_STORAGE ($AB_STORAGE_DETAIL) -- this run cannot confirm the corpus is on local storage, and the acceptance criteria REQUIRE it. THIS SESSION WILL NOT YIELD A VERDICT: analyze-ab.py refuses it with cause corpus-storage-unverified. Either move the corpus to a device whose model is recognised, or pass --attest-local-storage <why> to record an operator attestation that travels with the number, or run it as a --control"
fi
say "corpus census scope THE SERVED DIRECTORY ONLY -- unrelated tables, snapshots and hard-linked copies elsewhere under --data-dir are deliberately not counted"
[ "$CORPUS_FILES" -gt 0 ] || die corpus-empty "the served directory $SERVED_DIR holds no *-Data.db files"
if [ "$CORPUS_BYTES" -lt "$MIN_CORPUS_BYTES" ]; then
  die corpus-too-small \
    "the SERVED directory $SERVED_DIR holds $CORPUS_BYTES Data.db bytes, below the required $MIN_CORPUS_BYTES; a --shape full scan over a corpus this small measures request setup, not the read path (RUNBOOK.md states the floor and its basis)"
fi
if [ "$CORPUS_FILES" -lt "$MIN_SSTABLES" ]; then
  die corpus-too-few-sstables \
    "the SERVED directory $SERVED_DIR holds $CORPUS_FILES Data.db file(s), below the required $MIN_SSTABLES; issue #3058 gives the Flight row route a single-source fast path that NEVER enters the k-way merge, so a one-source corpus measures a code path #2820 did not touch -- and it does so identically on both arms, producing a ratio of 1.0 by construction"
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
# A MEASUREMENT IS THE #2820 COMPARISON OR IT IS A CONTROL. The refs default to
# the right commits, but an unlabelled session could override them with ANY two
# distinct commits and the analyzer would still present the result as the #2820
# verdict -- authoritative about something the session did not measure, which is
# the profile defect's family. `cfa93fe99` is "perf(#2820): batch the k-way
# merge egress fan-in (L1), co-designed with #2765 (#3659)".
#
# REFUSED, NEVER REORDERED. Swapping the arms to match would invert the ratio's
# meaning while reporting success -- a silently wrong number, which is worse
# than any refusal.
if [ -z "$CONTROL" ]; then
  PIN_BASE="$(resolve "${AB3649_PIN}^" 2>/dev/null || true)"
  PIN_HEAD="$(resolve "$AB3649_PIN" 2>/dev/null || true)"
  if [ -z "$PIN_BASE" ] || [ -z "$PIN_HEAD" ]; then
    die arm-pin-unresolvable \
      "the #2820 commit $AB3649_PIN (or its parent) does not resolve in $REPO, so this session cannot confirm it is measuring #2820. Point --repo at a checkout containing it, or run as a --control"
  fi
  if [ "$BASE_SHA" != "$PIN_BASE" ] || [ "$HEAD_SHA" != "$PIN_HEAD" ]; then
    die arm-refs-not-2820 \
      "this session's arms are base=$BASE_SHA head=$HEAD_SHA, but the #2820 comparison is base=$PIN_BASE ($AB3649_PIN^) head=$PIN_HEAD ($AB3649_PIN), IN THAT ORDER. The analyzer presents an unlabelled session as the #2820 verdict, so measuring another pair -- or the same pair reversed, which inverts the ratio -- would be authoritative about something this session did not measure. The arms are NOT reordered for you: a silently inverted ratio is worse than a refusal. Run it as a --control to compare other commits"
  fi
fi
LOADGEN_SHA_WANTED="$(resolve "${LOADGEN_REF:-$HEAD_REF}")"
[ -n "$LOADGEN_SHA_WANTED" ] || die arm-ref-unresolvable \
  "--loadgen-ref ${LOADGEN_REF:-$HEAD_REF} does not resolve to a commit in $REPO"
export AB_BASE_SHA="$BASE_SHA" AB_HEAD_SHA="$HEAD_SHA"
export AB_LOADGEN_SHA="$LOADGEN_SHA_WANTED" AB_LOADGEN_REF="${LOADGEN_REF:-$HEAD_REF}"
say "loadgen ref ${LOADGEN_REF:-$HEAD_REF} commit $LOADGEN_SHA_WANTED -- ONE client for both arms, so the client cannot vary with the server commit"
say "arm base ref $BASE_REF commit $BASE_SHA"
say "arm head ref $HEAD_REF commit $HEAD_SHA"

if [ -n "$SERVER_CPUS" ]; then
  export AB_SERVER_CPUS="$SERVER_CPUS" AB_CLIENT_CPUS="$CLIENT_CPUS"
  # NOT-REQUESTED until a run VERIFIES it. A manifest must never imply a pin
  # from `server_cpus` alone: the requested set and the effective one are
  # different facts, and only the second is a measurement.
  export AB_AFFINITY_STATE="${AB_AFFINITY_STATE:-NOT-REQUESTED}"
  # VALIDATED FOR EVERY PROPERTY, not only overlap. The inline parser called
  # `int()` on unvalidated input, so a malformed value emitted an UNANCHORED
  # PYTHON TRACEBACK -- multi-line, unprefixed, and looking like a crash rather
  # than a refusal -- while empty, reversed and out-of-range sets passed here to
  # fail at taskset AFTER all three release builds.
  python3 "$SUPPORT" validate-cpu-sets "$SERVER_CPUS" "$CLIENT_CPUS" \
    || usage_error "the CPU pinning sets are not usable (the cause is named above)"
  say "pinning server-cpus $SERVER_CPUS client-cpus $CLIENT_CPUS"
else
  say "pinning none-unpinned -- recorded as an explicit fact, not an absence; RUNBOOK.md recommends pinning after reading the sibling map from sysfs"
fi

# The ledger is truncated only now: the lock is held, every argument is
# validated, and the port is free, so nothing that follows can destroy a peer
# session's record before discovering it should not have started.
# NO PRE-FLIGHT PORT PROBE. With an ephemeral port there is no shared name to
# probe, and the probe was never sound anyway: "something answered" is not "my
# server owns it". Readiness is now established from THIS server's own
# post-bind log line instead -- see `run_one`.

# ---------------------------------------------------------------------------
# Build: one worktree and one target directory per arm.
# ---------------------------------------------------------------------------
declare -A ARM_BIN_DIR

# ONE LOAD GENERATOR FOR BOTH ARMS. Building it per arm made the CLIENT vary with
# the server commit, so any client-side change between the two refs would be
# attributed to server throughput -- a confound no amount of dispersion reporting
# could reveal, because both arms would be internally consistent. The design
# isolates ONE variable, and only the server legitimately differs per arm.
LOADGEN_BIN=''
LOADGEN_SHA=''

# EVERY WORKTREE IS VERIFIED BEFORE ANY OF THEM IS COMPILED. Preparing and
# building one at a time meant a dirty or wrong-commit worktree was found only
# after an earlier arm had already been built -- the same "fails after the
# expensive step" economics as the relative work directory and the missing
# command, and on a metered box the cost is the same.
prepare_worktree() { # <name> <sha>
  # Split deliberately: in a single `local a=$1 b=$a`, bash declares BOTH names
  # before assigning either, so `$a` is unset when `b` is evaluated -- which
  # under `set -u` is a hard error, and which the end-to-end case caught on its
  # first run.
  local name="$1" sha="$2"
  local wt="$WORK_DIR/wt-$name"
  if [ ! -d "$wt" ]; then
    git -C "$REPO" worktree add --detach "$wt" "$sha" > "$LOG_DIR/worktree-$name.log" 2>&1 \
      || die worktree-failed "git worktree add for $name failed; see $LOG_DIR/worktree-$name.log"
  fi
  local at
  at="$(git -C "$wt" rev-parse HEAD 2>/dev/null || true)"
  [ "$at" = "$sha" ] || die worktree-wrong-commit \
    "$wt is at ${at:-an unreadable HEAD} but $name is pinned to $sha; remove $wt and re-run"
  local dirty
  dirty="$(git -C "$wt" status --porcelain --untracked-files=all 2>/dev/null || echo UNREADABLE)"
  [ -z "$dirty" ] || die worktree-dirty \
    "$wt is at the right commit but is NOT CLEAN, so it would build code the manifest does not describe; remove $wt and re-run. First entries: $(printf '%s' "$dirty" | head -3 | tr '\n' ';')"
  say "worktree $name verified at $sha"
}

build_loadgen() { # <sha>
  local sha="$1" target="$WORK_DIR/target-loadgen"
  say "build loadgen commit $sha target-dir $target"
  ( cd "$WORK_DIR/wt-loadgen" && CARGO_TARGET_DIR="$target" cargo build --release -p flight-loadgen ) \
    > "$LOG_DIR/build-loadgen.log" 2>&1 \
    || die build-failed "the load-generator build failed; see $LOG_DIR/build-loadgen.log"
  [ -x "$target/release/flight-loadgen" ] || die build-incomplete \
    "$target/release/flight-loadgen was not produced"
  LOADGEN_BIN="$target/release/flight-loadgen"
  LOADGEN_SHA="$sha"
  say "build loadgen complete -- BOTH arms will use $LOADGEN_BIN"
}

build_arm() { # <arm> <sha>
  local arm="$1" sha="$2"
  local wt="$WORK_DIR/wt-$arm" target="$WORK_DIR/target-$arm"
  say "build $arm commit $sha worktree $wt target-dir $target"
  # ONLY the server. The load generator is built once, separately, from its own
  # pinned ref -- see build_loadgen.
  ( cd "$wt" && CARGO_TARGET_DIR="$target" cargo build --release \
      -p cqlite-flight ) > "$LOG_DIR/build-$arm.log" 2>&1 \
    || die build-failed "the $arm build failed; see $LOG_DIR/build-$arm.log"
  local bin="$target/release"
  [ -x "$bin/cqlite-flight" ]  || die build-incomplete "$bin/cqlite-flight was not produced"
  ARM_BIN_DIR["$arm"]="$bin"
  say "build $arm complete"
}

# Verify all three, THEN compile all three.
prepare_worktree loadgen "$LOADGEN_SHA_WANTED"
prepare_worktree base "$BASE_SHA"
prepare_worktree head "$HEAD_SHA"
build_loadgen "$LOADGEN_SHA_WANTED"
build_arm base "$BASE_SHA"
build_arm head "$HEAD_SHA"

# Pre-flight passed and both arms exist: only now may this session claim the work
# directory's ledger and manifest.
say "session directory $RUN_DIR -- owned by this session alone; no earlier session's results can be reached from here"

# ---------------------------------------------------------------------------
# One replicate of one arm.
# ---------------------------------------------------------------------------
parse_startup() { # <server-log> <field>  -- returns a VALUE, not a message
  python3 "$SUPPORT" parse-startup "$1" "$2" 2>/dev/null || echo NOT-OBSERVED
}

# Readiness is proved by OUR OWN server's post-bind line, never by a port probe.
# `cli::log_listening` is emitted only once a listener exists
# (cqlite-flight/src/cli.rs:228-241), so its presence proves this process owns
# the socket. A probe could only ever prove that SOMETHING answered -- which on a
# nine-lane box is how the loser of a race measures the winner's binary while its
# own configuration asserts all pass against its own pre-bind log.
wait_until_listening() { # <pid> <server-log> -> echoes host:port
  local pid="$1" log="$2" waited=0 bound=''
  while [ "$waited" -lt 90 ]; do
    bound="$(python3 "$SUPPORT" parse-listening "$log")"
    if [ "$bound" != "NOT-OBSERVED" ]; then
      # The line exists AND the process that wrote it is still alive and is
      # still ours: a log line from a server that has since died would otherwise
      # hand the loadgen an address somebody else now owns.
      if kill -0 "$pid" 2>/dev/null && is_our_server "$pid"; then
        printf '%s\n' "$bound"
        return 0
      fi
      return 1
    fi
    kill -0 "$pid" 2>/dev/null || return 1
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

run_one() { # <arm> <replicate> <position-in-pair: 1|2>
  local arm="$1" rep="$2" position="$3"
  local tag; tag="$(printf '%s-r%02d' "$arm" "$rep")"
  local bin="${ARM_BIN_DIR[$arm]}"
  local expect_batch_pre="${EXPECT_BATCH[$arm]}"
  local expect_maxbytes_pre="${EXPECT_MAXBYTES[$arm]}"
  local expect_wait_pre="${EXPECT_WAIT[$arm]}"
  local expect_scans_pre="${EXPECT_SCANS[$arm]}"
  local jsonl="$RUN_DIR/$tag.jsonl"
  local server_log="$LOG_DIR/$tag.server.log"

  if [ "$TEMPERATURE" = "cold" ]; then
    sync
    sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1 \
      || die cold-drop-failed "--temperature cold needs passwordless sudo to drop the page cache; without it the run is warm and would be recorded as cold"
  fi

  local extra=''
  if [ "$arm" = "base" ]; then extra="$BASE_SERVER_EXTRA"; else extra="$HEAD_SERVER_EXTRA"; fi

  # THE ARGV IS CONSTRUCTED, NOT CONCATENATED. Global flags followed by this
  # arm's extras produced `--batch-size 8192 --batch-size 1`, and the project's
  # Clap command does not enable self-overrides -- so that is an argument PARSE
  # FAILURE, not a last-wins resolution. The helper resolves each recognised
  # option to one value and emits it once; a duplicate is unexpressible rather
  # than merely unlikely, which matters because no stub reproduces Clap and the
  # end-to-end harness therefore cannot see this class at all.
  local -a server_flags=()
  mapfile -t server_flags < <(
    python3 "$SUPPORT" server-argv "$bin/cqlite-flight" "$CORPUS" "127.0.0.1:$PORT" \
      "$expect_batch_pre" "$expect_maxbytes_pre" "$expect_wait_pre" "$expect_scans_pre" "$extra"
  ) || die server-argv-failed "could not construct the $tag server command line (the cause is named above)"
  [ "${#server_flags[@]}" -gt 0 ] || die server-argv-failed \
    "the constructed $tag server command line is empty"

  # THE SERVER RUNS IN A CONTROLLED ENVIRONMENT, NOT AN INHERITED ONE. Every
  # `CQLITE_*` variable the server honours is a silent override of a value this
  # manifest claims to record -- and an inherited `RUST_LOG=warn` suppresses the
  # INFO readiness line, so EVERY session would time out waiting for a server
  # that had already bound. This repo has the same shape written down for
  # `gate-detached.sh`, where one lane's exported `RUSTFLAGS` poisoned every
  # detached gate on the box. Its lesson is why this is an ALLOWLIST and not a
  # denylist: an allowlist of remembered variables fails silently, so `env -i`
  # drops everything and each admitted entry is named with its reason.
  local -a server_env=(
    env -i
    "PATH=$PATH"                                  # exec, and the shebang lookup
    "HOME=${HOME:-/tmp}"                          # some libs probe it on start
    "TMPDIR=${TMPDIR:-/tmp}"
    "RUST_LOG=info"                               # the readiness line is INFO
    # tracing-subscriber's fmt layer has ANSI on by DEFAULT and colour survives
    # redirection to a file, so an uncoloured log is not something to assume.
    # 0.3.23 reads NO_COLOR in `Layer::default()` (verified in the locked
    # source), so this is a real control -- but the parse site strips ANSI
    # anyway, because this depends on a crate version and a construction path
    # that are not ours to guarantee.
    "NO_COLOR=1"
    "RUST_BACKTRACE=1"                            # a crash is diagnosable
    "CQLITE_FLIGHT_MERGE_PATH=$MERGE_PATH"        # pinned, recorded, #3058
  )
  [ -n "$SERVER_CPUS" ] && server_env+=(taskset -c "$SERVER_CPUS")

  "${server_env[@]}" "${server_flags[@]}" > "$server_log" 2>&1 &
  local srv=$!
  SRV_PID=$srv
  # The identity, not just the number: a pid is reused, and on a nine-lane box
  # the process that inherits it is most likely a peer's.
  SRV_START="$(awk '{print $22}' "/proc/$srv/stat" 2>/dev/null || true)"

  local endpoint_addr
  endpoint_addr="$(wait_until_listening "$srv" "$server_log")" || {
    # SRV_PID is left set, as it now is everywhere: the server may be ALIVE and
    # merely silent (it never printed its readiness line), and `cleanup` reaps
    # whatever `SRV_PID` names. With one release point -- after the process is
    # confirmed gone -- this is no longer a special case, which is the point of
    # the change.
    die server-never-listened \
      "the $tag server never reported a post-bind listening line while alive; readiness is taken from ITS OWN post-bind line, not from a port probe, so a port answered by somebody else cannot satisfy it. See $server_log"
  }
  local endpoint="http://$endpoint_addr"
  say "run $tag server pid $srv listening on $endpoint_addr (from its own post-bind line)"

  # PROVENANCE, READ FROM THE SERVER RATHER THAN ASSUMED. cli::log_startup emits
  # one `cqlite-flight starting` line carrying the RESOLVED admission ceiling and
  # its source ("flag" | "env" | "derived" | "derived-fallback",
  # cqlite-flight/src/admission.rs:183-193). A value we passed and a value the
  # server resolved are different facts; only the second one is a measurement.
  local admission_observed admission_source
  admission_observed="$(parse_startup "$server_log" scans)"
  admission_source="$(parse_startup "$server_log" source)"
  # COMPARED AGAINST THE RESOLVED VALUE, NOT THE RAW OPTION STRING. Round 14
  # canonicalised resolved integers so the argv, the manifest and this read-back
  # would carry one representation -- and this one comparison was left reading
  # the raw global. `--max-concurrent-scans 04` therefore launched the server as
  # `4`, the startup line echoed `4`, and the string `04` did not match it: a
  # CORRECT session aborting after both release builds, on a defect our own fix
  # introduced. Its three siblings below already compared against `expect_*_pre`;
  # this was the only site that did not.
  say "run $tag admission requested $MAX_CONCURRENT_SCANS resolved $expect_scans_pre observed $admission_observed source $admission_source"
  if [ "$admission_observed" != "NOT-OBSERVED" ] \
     && [ "$admission_observed" != "$expect_scans_pre" ]; then
    die admission-mismatch \
      "$tag: the server resolved --max-concurrent-scans to $admission_observed but this arm expects $expect_scans_pre; the arms would not be served under the same admission ceiling"
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
  # Compared against THIS ARM's declared expectation, not the global request: a
  # control that sets --head-server-extra '--max-batch-bytes 1' expects the head
  # server to report 1, and a driver that called that a mismatch would make the
  # sensitivity control unrunnable. These are the same values the argv was built
  # from, so what is asserted is what was asked for.
  local expect_batch="$expect_batch_pre" expect_maxbytes="$expect_maxbytes_pre"
  local expect_wait="$expect_wait_pre"
  if [ "$observed_batch" != "NOT-OBSERVED" ] && [ "$observed_batch" != "$expect_batch" ]; then
    die batch-size-mismatch \
      "$tag: the server reports batch_size=$observed_batch but this arm expects $expect_batch; the Arrow batch row cap is the mechanism #2820 changed, so a measurement whose effective value is unknown is not a measurement"
  fi
  if [ "$expect_maxbytes" != "NOT-REQUESTED" ] && [ "$observed_maxbytes" != "NOT-OBSERVED" ] \
     && [ "$observed_maxbytes" != "$expect_maxbytes" ]; then
    die max-batch-bytes-mismatch \
      "$tag: the server reports max_batch_bytes=$observed_maxbytes but this arm expects $expect_maxbytes"
  fi
  if [ "$expect_wait" != "NOT-REQUESTED" ] && [ "$observed_wait" != "NOT-OBSERVED" ] \
     && [ "$observed_wait" != "$expect_wait" ]; then
    die wait-timeout-mismatch \
      "$tag: the server reports admission_wait_timeout_ms=$observed_wait but this arm expects $expect_wait; the shed threshold decides which steps the analyzer must exclude"
  fi

  # Requested pinning and EFFECTIVE pinning are different facts too: a server
  # that is not on the cores the manifest names is measuring something else, and
  # nothing else in this driver would notice.
  if [ -n "$SERVER_CPUS" ]; then
    local affinity
    # UNVERIFIABLE is now a REFUSAL, so this dies on it -- the named cause is
    # already printed by the helper and distinguishes "not pinned where asked"
    # from "could not tell". The STATE is recorded either way, so the analyzer
    # cannot be handed a manifest claiming a pin nothing established.
    affinity="$(python3 "$SUPPORT" check-affinity "$srv" "$SERVER_CPUS")" || die affinity-unverified \
      "$tag: the server's effective pinning to $SERVER_CPUS was not established (the cause is named above)"
    export AB_AFFINITY_STATE="$affinity"
    say "run $tag server affinity $affinity requested $SERVER_CPUS"
  fi

  if [ "$admission_source" != "NOT-OBSERVED" ] && [ "$admission_source" != "flag" ]; then
    die admission-provenance \
      "$tag: the server reports the admission ceiling came from '$admission_source', not 'flag', even though --max-concurrent-scans was passed; something else (CQLITE_MAX_CONCURRENT_SCANS in the environment, or a derived fallback) is deciding it"
  fi

  local -a client_prefix=()
  [ -n "$CLIENT_CPUS" ] && client_prefix+=(taskset -c "$CLIENT_CPUS")

  # THE FROZEN TICKET IS RE-VERIFIED AROUND EVERY INVOCATION, BEFORE AND AFTER.
  # Hashing once and setting a read-only bit is not immutability: the file can
  # be replaced through the writable parent directory, and nothing compared
  # contents again. BEFORE catches a swap between runs; AFTER catches a swap
  # DURING one, which is the case a before-only check cannot see and the one
  # where the records already exist to be believed.
  verify_frozen_ticket() { # <when>
    local now
    now="$(python3 - "$TICKET_TEMPLATE" <<'PYEOF'
import hashlib
import sys

try:
    with open(sys.argv[1], "rb") as handle:
        sys.stdout.write(hashlib.sha256(handle.read()).hexdigest())
except OSError:
    sys.stdout.write("UNREADABLE")
PYEOF
)"
    [ "$now" = "$TICKET_SHA" ] || die ticket-mutated \
      "$tag: the frozen ticket $TICKET_TEMPLATE hashes to $now $1, but the session recorded $TICKET_SHA. The measurement input changed mid-session, so the arms did not all serve the same workload and the records cannot be compared"
  }

  if [ "$PREWARM" -eq 1 ] && [ "$TEMPERATURE" = "warm" ]; then
    verify_frozen_ticket "before the warming pass"
    "${client_prefix[@]}" "$LOADGEN_BIN" --endpoint "$endpoint" \
      --ticket-template "$TICKET_TEMPLATE" --shape "$SHAPE" --ramp "$RAMP" \
      --step-duration "$STEP_DURATION" --round "$tag-prewarm" --out /dev/null \
      > "$LOG_DIR/$tag.prewarm.log" 2>&1 \
      || die prewarm-failed "the $tag warming pass failed; see $LOG_DIR/$tag.prewarm.log"
    verify_frozen_ticket "after the warming pass"
  fi

  local cpu0 cpu1 hz
  # An unreadable /proc yields NOTHING, not a fabricated 0: the recorder already
  # has a null path for server_cpu_seconds and a silent zero would defeat it.
  cpu0="$(awk '{print $14+$15}' "/proc/$srv/stat" 2>/dev/null || true)"
  local rc=0
  verify_frozen_ticket "before the measured run"
  "${client_prefix[@]}" "$LOADGEN_BIN" --endpoint "$endpoint" \
    --ticket-template "$TICKET_TEMPLATE" --shape "$SHAPE" --ramp "$RAMP" \
    --step-duration "$STEP_DURATION" --round "$tag" --out "$jsonl" \
    > "$LOG_DIR/$tag.loadgen.log" 2>&1 || rc=$?
  cpu1="$(awk '{print $14+$15}' "/proc/$srv/stat" 2>/dev/null || true)"
  verify_frozen_ticket "after the measured run"
  hz="$(getconf CLK_TCK)"

  reap_server
  if kill -0 "$srv" 2>/dev/null && is_our_server "$srv"; then
    die server-would-not-die "the $tag server (pid $srv) survived TERM and KILL; the next replicate would bind a port served by this arm's binary"
  fi
  [ "$rc" -eq 0 ] || die loadgen-failed \
    "the $tag load generator exited $rc; see $LOG_DIR/$tag.loadgen.log"

  # Validate the produced records here, not only in the analyzer: a bad
  # replicate must stop the session while the rig is still up, not surface hours
  # later. The validator is ramp-aware -- flight-loadgen emits ONE record per
  # ramp step -- and it is an executable file so the self-test can drive it.
  # It CALLS the analyzer's typed record validation rather than reimplementing
  # it, so the driver cannot accept a record the analysis will later refuse --
  # which is the one failure this check exists to prevent and the one a second
  # validator would eventually cause.
  python3 "$SUPPORT" validate-replicate "$jsonl" "$tag" "$RAMP" "$SHAPE" "$STEP_DURATION" \
    || die replicate-invalid "the $tag JSONL is not a usable replicate (see the cause-detail above)"

  python3 - "$RUNS_JSONL" "$arm" "$rep" "$tag.jsonl" "$TEMPERATURE" "$cpu0" "$cpu1" "$hz" \
    "$admission_observed" "$admission_source" "$observed_batch" \
    "$observed_maxbytes" "$observed_wait" "$position" "$LOADGEN_SHA" <<'PYEOF'
import json
import sys

(runs_path, arm, rep, filename, temperature, cpu0, cpu1, hz,
 admission_observed, admission_source, batch_size_observed,
 max_batch_bytes_observed, wait_timeout_ms_observed, position,
 loadgen_commit) = sys.argv[1:16]
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
    "loadgen_commit": loadgen_commit,
}
with open(runs_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry, sort_keys=True) + "\n")
PYEOF
  write_manifest
}

# `latest` is documented as the most recent COMPLETED session, so it is moved
# once, at the end -- not after every arm. Updated per run it pointed at
# whatever the last interrupted session had reached, which is an unpaired or
# replicate-short manifest wearing the name an operator reaches for when they
# have lost the printed `next` line.
publish_latest() {
  ln -sfn "$RUN_DIR" "$WORK_DIR/.latest.tmp.$$" 2>/dev/null \
    && mv -T "$WORK_DIR/.latest.tmp.$$" "$WORK_DIR/latest" 2>/dev/null || true
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
publish_latest
say "session complete: $REPLICATES paired replicates in $RUN_DIR"
# The two build worktrees are REGISTERED IN THE REPOSITORY and outlive this
# process. On a rig that is torn down it does not matter; on a persistent
# checkout a killed session leaves them behind, so name them rather than let the
# operator discover it from `git worktree list` weeks later.
say "worktrees left registered in $REPO: $WORK_DIR/wt-base $WORK_DIR/wt-head -- remove with 'git -C $REPO worktree remove --force <path>' when you are done with this work directory"
# The section this session's ramp belongs to, decided by the same validator that
# accepted the ramp rather than re-derived here.
say "next python3 $(dirname "$SUPPORT")/analyze-ab.py --$RAMP_SECTION $RUN_DIR/manifest.json"
exit 0

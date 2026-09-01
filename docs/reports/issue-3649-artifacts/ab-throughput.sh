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
# replicate whose JSONL carries any request error, and a replicate that produced
# anything other than exactly one step record. It never continues silently with
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
DRIVER_VERSION='ab-throughput.sh/v1'

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
  write_manifest || true
  say "manifest $RUN_DIR/manifest.json records the runs that did complete"
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
  --replicates <N>          interleaved replicate pairs                  (default 5)
  --work-dir <dir>          worktrees, target dirs, results       (default /data/ab-3649)
  --repo <dir>              repository to build from       (default this checkout)
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

Then: python3 analyze-ab.py --manifest <work-dir>/results/manifest.json
USAGE
}

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
CORPUS=''
TICKET_TEMPLATE=''
BASE_REF='cfa93fe99^'
HEAD_REF='cfa93fe99'
REPLICATES=5
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
ROWS_DECLARED=''
PREWARM=1
TEMPERATURE='warm'
CONTROL=''
BASE_SERVER_EXTRA=''
HEAD_SERVER_EXTRA=''

while [ "$#" -gt 0 ]; do
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
[ "$REPLICATES" -ge 3 ] || usage_error \
  "--replicates must be at least 3; a percentile bootstrap over fewer pairs reports an interval it cannot support (5 or more is the recommendation in RUNBOOK.md)"
case "$PORT" in ''|*[!0-9]*) usage_error "--port must be an integer" ;; esac
case "$MIN_CORPUS_BYTES" in ''|*[!0-9]*) usage_error "--min-corpus-bytes must be an integer" ;; esac
case "$TEMPERATURE" in warm|cold) ;; *) usage_error "--temperature must be warm or cold" ;; esac
case "$MIN_SSTABLES" in ''|*[!0-9]*) usage_error "--min-sstables must be an integer" ;; esac
case "$MERGE_PATH" in auto|merge|bypass) ;; *) usage_error "--merge-path must be auto, merge or bypass" ;; esac
if [ -n "$SERVER_CPUS" ] || [ -n "$CLIENT_CPUS" ]; then
  [ -n "$SERVER_CPUS" ] && [ -n "$CLIENT_CPUS" ] || usage_error \
    "--server-cpus and --client-cpus must be given together: pinning one and not the other measures the load generator competing with the server"
  command -v taskset >/dev/null 2>&1 || usage_error "taskset is not on PATH but CPU pinning was requested"
fi

if [ -z "$REPO" ]; then
  REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && git rev-parse --show-toplevel)"
fi
REPO="$(cd "$REPO" && pwd)"
RUN_DIR="$WORK_DIR/results"
LOG_DIR="$WORK_DIR/logs"
mkdir -p "$RUN_DIR" "$LOG_DIR"
RUNS_JSONL="$RUN_DIR/runs.jsonl"
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
export AB_PREWARM="$PREWARM" AB_TEMPERATURE="$TEMPERATURE"
export AB_TICKET_TEMPLATE="$TICKET_TEMPLATE"
export AB_CONTROL="$CONTROL"
export AB_BASE_SERVER_EXTRA="$BASE_SERVER_EXTRA" AB_HEAD_SERVER_EXTRA="$HEAD_SERVER_EXTRA"
export AB_CORPUS="$CORPUS" AB_MIN_CORPUS_BYTES="$MIN_CORPUS_BYTES"
export AB_MIN_SSTABLES="$MIN_SSTABLES" AB_MERGE_PATH="$MERGE_PATH"
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
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$TICKET_TEMPLATE" >/dev/null 2>&1 \
  || die ticket-template-unparseable "$TICKET_TEMPLATE is not valid JSON"

[ -d "$CORPUS" ] || die corpus-absent "$CORPUS is not a directory"
CORPUS_FILES="$(find "$CORPUS" -name '*-Data.db' -type f 2>/dev/null | wc -l | tr -d ' ')"
CORPUS_BYTES="$(find "$CORPUS" -name '*-Data.db' -type f -printf '%s\n' 2>/dev/null \
  | awk 'BEGIN{s=0}{s+=$1}END{print s+0}')"
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

write_manifest

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
  at="$(git -C "$wt" rev-parse HEAD)"
  [ "$at" = "$sha" ] || die worktree-wrong-commit \
    "$wt is at $at but arm $arm is $sha -- a pre-existing worktree was reused at the wrong commit; remove $wt and re-run"
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

# ---------------------------------------------------------------------------
# One replicate of one arm.
# ---------------------------------------------------------------------------
port_is_bound() { (echo > "/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1; }

run_one() { # <arm> <replicate>
  local arm="$1" rep="$2"
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
  server_cmd+=("$bin/cqlite-flight" --data-dir "$CORPUS" --listen "127.0.0.1:$PORT")
  # Word-split on purpose: the value is an operator-supplied flag list, and it is
  # recorded verbatim in the manifest so a reader of the report sees it.
  # shellcheck disable=SC2206
  [ -n "$extra" ] && server_cmd+=($extra)
  "${server_cmd[@]}" > "$server_log" 2>&1 &
  local srv=$!

  local waited=0
  while [ "$waited" -lt 90 ]; do
    if port_is_bound; then break; fi
    if ! kill -0 "$srv" 2>/dev/null; then
      die server-exited "the $tag server exited before binding port $PORT; see $server_log"
    fi
    sleep 1
    waited=$((waited + 1))
  done
  port_is_bound || die server-never-bound \
    "the $tag server did not bind port $PORT within ${waited}s; see $server_log"
  say "run $tag server pid $srv bound 127.0.0.1:$PORT after ${waited}s"

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
  cpu0="$(awk '{print $14+$15}' "/proc/$srv/stat" 2>/dev/null || echo 0)"
  local rc=0
  "${client_prefix[@]}" "$bin/flight-loadgen" --endpoint "http://127.0.0.1:$PORT" \
    --ticket-template "$TICKET_TEMPLATE" --shape "$SHAPE" --ramp "$RAMP" \
    --step-duration "$STEP_DURATION" --round "$tag" --out "$jsonl" \
    > "$LOG_DIR/$tag.loadgen.log" 2>&1 || rc=$?
  cpu1="$(awk '{print $14+$15}' "/proc/$srv/stat" 2>/dev/null || echo 0)"
  hz="$(getconf CLK_TCK)"

  kill "$srv" 2>/dev/null || true
  local dying=0
  while [ "$dying" -lt 30 ]; do
    kill -0 "$srv" 2>/dev/null || break
    sleep 1
    dying=$((dying + 1))
  done
  if kill -0 "$srv" 2>/dev/null; then
    kill -9 "$srv" 2>/dev/null || true
    sleep 2
  fi
  wait "$srv" 2>/dev/null || true
  if kill -0 "$srv" 2>/dev/null; then
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

  # Validate the produced record here, not only in the analyzer: a bad replicate
  # must stop the session while the rig is still up, not surface hours later.
  python3 - "$jsonl" "$tag" <<'PYEOF' || die replicate-invalid "the $tag JSONL is not a usable replicate (see the cause-detail above)"
import json
import sys

path, tag = sys.argv[1], sys.argv[2]


def refuse(detail):
    sys.stderr.write("AB-3649: cause replicate-invalid\n")
    sys.stderr.write("AB-3649: cause-detail %s\n" % detail)
    sys.exit(1)


try:
    with open(path, encoding="utf-8") as handle:
        lines = [line for line in handle if line.strip()]
except OSError as exc:
    refuse("%s: %s" % (path, exc))
if len(lines) != 1:
    refuse("%s: %d step records, expected exactly 1" % (path, len(lines)))
try:
    record = json.loads(lines[0])
except ValueError as exc:
    refuse("%s: %s" % (path, exc))
if record.get("requests_error", 0):
    refuse("%s: requests_error=%s" % (path, record["requests_error"]))
if record.get("requests_unavailable", 0):
    refuse("%s: requests_unavailable=%s" % (path, record["requests_unavailable"]))
if not record.get("requests_ok", 0):
    refuse("%s: requests_ok=0" % path)
if not record.get("rows_per_s", 0) > 0:
    refuse("%s: rows_per_s is not positive -- the scan returned no rows" % path)
sys.stdout.write(
    "AB-3649: run %s rows-per-s %.2f requests-ok %d duration-s %.2f p50-ms %.3f\n"
    % (
        tag,
        record["rows_per_s"],
        record["requests_ok"],
        record["duration_s"],
        record["latency_ms"]["p50"],
    )
)
PYEOF

  python3 - "$RUNS_JSONL" "$arm" "$rep" "$tag.jsonl" "$TEMPERATURE" "$cpu0" "$cpu1" "$hz" <<'PYEOF'
import json
import sys

runs_path, arm, rep, filename, temperature, cpu0, cpu1, hz = sys.argv[1:9]
try:
    server_cpu_s = (int(cpu1) - int(cpu0)) / float(hz)
except (ValueError, ZeroDivisionError):
    server_cpu_s = None
entry = {
    "arm": arm,
    "replicate": int(rep),
    "file": filename,
    "temperature": temperature,
    "server_cpu_seconds": server_cpu_s,
}
with open(runs_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry, sort_keys=True) + "\n")
PYEOF
  write_manifest
}

# ---------------------------------------------------------------------------
# The interleaved session.
# ---------------------------------------------------------------------------
say "session replicates $REPLICATES order interleaved-base-head shape $SHAPE ramp $RAMP step-duration $STEP_DURATION temperature $TEMPERATURE prewarm $PREWARM"
rep=1
while [ "$rep" -le "$REPLICATES" ]; do
  run_one base "$rep"
  run_one head "$rep"
  rep=$((rep + 1))
done

write_manifest
say "session complete: $REPLICATES paired replicates in $RUN_DIR"
say "next python3 $(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/analyze-ab.py --manifest $RUN_DIR/manifest.json"
exit 0

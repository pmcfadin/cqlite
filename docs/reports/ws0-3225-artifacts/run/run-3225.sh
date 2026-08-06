#!/usr/bin/env bash
# CQLite issue #3225 §2 — the C(N) x width sweep driver.
#
# Adapted from docs/reports/ws0-3217-artifacts/partA-run/run-partA.sh. What changed
# and why (design.md D7):
#   * widths S in {1,2,3,4,6}; S=3 is NEW and goes through sweep.sh's LITERAL
#     cpu-list form (0-2,8-10) — sweep.sh needs no code change for it.
#   * N ramp extended to 1,2,4,8,16,24,32,64. 64 is the SHIPPED default
#     (DEFAULT_MAX_CONCURRENT_SCANS, cqlite-flight/src/admission.rs), and AC5
#     cannot be evaluated against a point nobody measured. #3217 stopped at 16.
#   * the admission ceiling is raised to 64 (see WS0_MAX_CONCURRENT_SCANS below):
#     at the harness default of 16 every N>16 point would measure the ADMISSION
#     GATE shedding, not the concurrency curve.
#   * the merge-path reference arms are dropped (#3217 answered that; this round
#     measures a curve, bypass only).
#   * NOT run: partB-run/, profile-*, classify-offcpu, runqlat. That also removes
#     the perf_event_paranoid/kptr_restrict symbolization dependency.
#   * RESTARTABLE PER ARM (see below) — #3217's driver was not.
#
# Usage:
#   bash run-3225.sh [options]
#     --root <dir>        WS0 root (default $WS0_ROOT, else /data/ws0)
#     --stage <dir>       staged SSTable dir (default <root>/ws0-h2h/datasets/sstables)
#     --worktree <dir>    repo checkout holding target/release (default: this file's repo)
#     --arms "a b c"      subset of arm labels to run (default: all five)
#     --ramp <list>       override the N ramp (dry runs only)
#     --step <secs>       override the per-point hold (dry runs only)
#     --reps <n>          override reps (dry runs only)
#     --label-suffix <s>  append to every arm label (use for dry runs: -dryrun)
#     --force             re-run arms that already have a complete summary.json
#     --list              print the planned arms and exit
#     --help
#
# RESTART / RESUME AFTER A CRASH
#   Every point is written to <root>/results/<arm>/points.jsonl by emit-point.py the
#   moment it completes, and each arm writes summary.json when it finishes. Re-running
#   this script with the SAME arguments:
#     - SKIPS every arm that already has a complete summary.json (COMPLETED arms are
#       never redone and never lost);
#     - QUARANTINES a partial arm (points.jsonl but no summary.json) to
#       <arm>.partial-<utc> and re-runs that arm from rep 1.
#   The quarantine is deliberate: sweep.sh always starts at rep 1 and APPENDS, so
#   resuming into an existing points.jsonl would silently mix a truncated first
#   attempt with a second one and corrupt every per-N median. A partial arm costs
#   ~1 h to redo; a silently doubled arm costs the whole result's credibility.
#
# LONG RUNNING: ~1 h per arm, ~5-6 h for all five. Launch detached:
#   nohup bash run-3225.sh > /data/ws0/logs/run-3225.log 2>&1 < /dev/null &
set -uo pipefail

WS0_ROOT="${WS0_ROOT:-/data/ws0}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKTREE="$(cd "$HERE/../../../.." && pwd)"
STAGE=""
ARMS_REQ=""
RAMP="1,2,4,8,16,24,32,64"
STEP_SECS=120
REPS=3
LABEL_SUFFIX=""
FORCE=0
LIST_ONLY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --root)         [ $# -ge 2 ] || { echo "ERROR: --root needs a value" >&2; exit 2; }; WS0_ROOT="$2"; shift 2 ;;
    --stage)        [ $# -ge 2 ] || { echo "ERROR: --stage needs a value" >&2; exit 2; }; STAGE="$2"; shift 2 ;;
    --worktree)     [ $# -ge 2 ] || { echo "ERROR: --worktree needs a value" >&2; exit 2; }; WORKTREE="$2"; shift 2 ;;
    --arms)         [ $# -ge 2 ] || { echo "ERROR: --arms needs a value" >&2; exit 2; }; ARMS_REQ="$2"; shift 2 ;;
    --ramp)         [ $# -ge 2 ] || { echo "ERROR: --ramp needs a value" >&2; exit 2; }; RAMP="$2"; shift 2 ;;
    --step)         [ $# -ge 2 ] || { echo "ERROR: --step needs a value" >&2; exit 2; }; STEP_SECS="$2"; shift 2 ;;
    --reps)         [ $# -ge 2 ] || { echo "ERROR: --reps needs a value" >&2; exit 2; }; REPS="$2"; shift 2 ;;
    --label-suffix) [ $# -ge 2 ] || { echo "ERROR: --label-suffix needs a value" >&2; exit 2; }; LABEL_SUFFIX="$2"; shift 2 ;;
    --force)        FORCE=1; shift ;;
    --list)         LIST_ONLY=1; shift ;;
    -h|--help)      sed -n '2,50p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "ERROR: unrecognized argument '$1'" >&2; exit 2 ;;
  esac
done

HARNESS="$WORKTREE/docs/reports/ws0-3217-artifacts/harness"
[ -d "$HARNESS" ] || { echo "ERROR: harness dir not found: $HARNESS" >&2; exit 1; }
[ -f "$HARNESS/sweep.sh" ] || { echo "ERROR: sweep.sh not found under $HARNESS" >&2; exit 1; }
[ -z "$STAGE" ] && STAGE="$WS0_ROOT/ws0-h2h/datasets/sstables"

# ---- the environment sweep.sh reads. Fail closed on every one of them. -------
export WS0_ROOT
export WS0_STAGE="$STAGE"
export WS0_FLIGHT_BIN="${WS0_FLIGHT_BIN:-$WORKTREE/target/release/cqlite-flight}"
export WS0_LOADGEN_BIN="${WS0_LOADGEN_BIN:-$WORKTREE/target/release/flight-loadgen}"
export WS0_TICKET_TPL="${WS0_TICKET_TPL:-$WORKTREE/docs/reports/ws0-3100-artifacts/ws0-h2h/ws0-events-template.json}"

# THE knob this whole issue is about. The harness default is 16, which would make
# every N>16 point measure admission shedding instead of the concurrency curve.
# 64 is the SHIPPED default (admission.rs DEFAULT_MAX_CONCURRENT_SCANS) and the top
# of this ramp, so the ceiling never binds and C(N) is measured cleanly at every N.
export WS0_MAX_CONCURRENT_SCANS="${WS0_MAX_CONCURRENT_SCANS:-64}"

# Unchanged from #3217 so the S in {1,2,4,6} arms stay directly comparable.
export WS0_BATCH_SIZE="${WS0_BATCH_SIZE:-8192}"
export WS0_MAX_BATCH_BYTES="${WS0_MAX_BATCH_BYTES:-4194304}"
export WS0_MAX_INFLIGHT_EGRESS_BYTES="${WS0_MAX_INFLIGHT_EGRESS_BYTES:-12582912}"
export WS0_ADMISSION_WAIT_TIMEOUT_MS="${WS0_ADMISSION_WAIT_TIMEOUT_MS:-30000}"
export WS0_SEED="${WS0_SEED:-42}"
export WS0_WARM_SECS="${WS0_WARM_SECS:-45}"
export WS0_SETTLE_SECS="${WS0_SETTLE_SECS:-5}"
export WS0_CLIENT_SAT_THRESHOLD="${WS0_CLIENT_SAT_THRESHOLD:-0.70}"
export WS0_RESULTS="${WS0_RESULTS:-$WS0_ROOT/results}"
export WS0_LOGS="${WS0_LOGS:-$WS0_ROOT/logs}"
export WS0_ARTIFACTS="${WS0_ARTIFACTS:-$WS0_ROOT/artifacts}"

CLIENT_CPUS="${WS0_CLIENT_CPUS:-6,7,14,15}"

# ---- the arm table: label | server-cpu-spec ---------------------------------
# S=1,2,4,6 use sweep.sh's shorthands (which also stamp server_physical_cores_S).
# S=3 has no shorthand and uses the LITERAL list — its points therefore carry
# server_physical_cores_S=null, and analyze-3225.py re-derives S from the arm's
# own cpu-topology.json sibling groups rather than trusting a label.
ARM_LABELS=(cn3225-s1 cn3225-s2 cn3225-s3 cn3225-s4 cn3225-s6)
ARM_SPECS=(s1        s2        0-2,8-10  s4        s6)

log()  { printf '[%s] %s\n' "$(date -u +%FT%TZ)" "$*"; }
die()  { printf '[%s] ERROR: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; exit 1; }

if [ "$LIST_ONLY" -eq 1 ]; then
  printf 'planned arms (ramp=%s step=%ss reps=%s client=%s max_concurrent_scans=%s):\n' \
    "$RAMP" "$STEP_SECS" "$REPS" "$CLIENT_CPUS" "$WS0_MAX_CONCURRENT_SCANS"
  for i in "${!ARM_LABELS[@]}"; do
    printf '  %-16s server_cpus=%s\n' "${ARM_LABELS[$i]}${LABEL_SUFFIX}" "${ARM_SPECS[$i]}"
  done
  exit 0
fi

[ -d "$WS0_STAGE" ]      || die "WS0_STAGE=$WS0_STAGE is not a directory (regenerate the corpus first: docs/reports/ws0-3225-artifacts/corpus/regen-corpus.sh)"
[ -x "$WS0_FLIGHT_BIN" ] || die "WS0_FLIGHT_BIN=$WS0_FLIGHT_BIN is not executable (cargo build --release -p cqlite-flight)"
[ -x "$WS0_LOADGEN_BIN" ]|| die "WS0_LOADGEN_BIN=$WS0_LOADGEN_BIN is not executable (cargo build --release -p flight-loadgen)"
[ -f "$WS0_TICKET_TPL" ] || die "WS0_TICKET_TPL=$WS0_TICKET_TPL not found"
# Depth-agnostic on purpose: the staged layout is <stage>/<keyspace>/<table-dir>/*-Data.db,
# which a fixed-depth glob gets wrong by one level (common.sh's own check only WARNs on
# that, so it cannot be relied on to catch an unstaged corpus).
STAGED_DATA_DB_COUNT="$(find "$WS0_STAGE" -name '*-Data.db' -type f 2>/dev/null | wc -l)"
[ "$STAGED_DATA_DB_COUNT" -ge 1 ] \
  || die "no *-Data.db anywhere under $WS0_STAGE — the corpus is not staged"
log "staged corpus: $STAGED_DATA_DB_COUNT *-Data.db file(s) under $WS0_STAGE"

# A live Cassandra (or a stray flight server) would compete for the pinned cores
# and silently spoil every point. Refuse rather than measure noise.
if pgrep -f 'org.apache.cassandra.service.CassandraDaemon' >/dev/null 2>&1; then
  die "a Cassandra daemon is RUNNING — stop it before sweeping (it competes for the pinned cores)"
fi
if pgrep -f "$WS0_FLIGHT_BIN" >/dev/null 2>&1; then
  die "a cqlite-flight from $WS0_FLIGHT_BIN is already running — stop it first"
fi

DRIVER_LOGS="$WS0_LOGS/driver"
mkdir -p "$DRIVER_LOGS" "$WS0_RESULTS"
PROG="$DRIVER_LOGS/run-3225-progress.txt"
touch "$PROG"

log "worktree=$WORKTREE"
log "stage=$WS0_STAGE  flight=$WS0_FLIGHT_BIN  loadgen=$WS0_LOADGEN_BIN"
log "ramp=$RAMP step=${STEP_SECS}s reps=$REPS warm=${WS0_WARM_SECS}s settle=${WS0_SETTLE_SECS}s path=bypass"
log "client_cpus=$CLIENT_CPUS  max_concurrent_scans=$WS0_MAX_CONCURRENT_SCANS  client_sat_gate=$WS0_CLIENT_SAT_THRESHOLD"
log "results -> $WS0_RESULTS   progress ledger -> $PROG"

# An arm is COMPLETE when its summary.json exists and parses. summarize-sweep.py
# writes it only after the last rep, so its presence is the completion marker.
arm_complete() {
  local dir="$1"
  [ -s "$dir/summary.json" ] || return 1
  python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$dir/summary.json" >/dev/null 2>&1
}

run_arm() { # label  server-cpu-spec
  local label="$1" spec="$2"
  local dir="$WS0_RESULTS/$label"

  if arm_complete "$dir" && [ "$FORCE" -eq 0 ]; then
    log "SKIP $label — already complete ($(wc -l < "$dir/points.jsonl" 2>/dev/null || echo 0) points)"
    echo "$(date -u +%FT%TZ) SKIP  $label (complete)" >> "$PROG"
    return 0
  fi
  if [ -e "$dir" ]; then
    local quarantine="$dir.partial-$(date -u +%Y%m%dT%H%M%SZ)"
    mv "$dir" "$quarantine"
    log "QUARANTINED partial/forced arm $label -> $quarantine (sweep.sh restarts at rep 1 and appends; mixing attempts would corrupt every median)"
    echo "$(date -u +%FT%TZ) QUAR  $label -> $quarantine" >> "$PROG"
  fi

  log "START $label (server_cpus=$spec)"
  echo "$(date -u +%FT%TZ) START $label" >> "$PROG"
  ( cd "$HARNESS" && bash ./sweep.sh "$label" "$spec" "$CLIENT_CPUS" "$RAMP" "$STEP_SECS" "$REPS" bypass ) \
    > "$DRIVER_LOGS/$label.out" 2>&1 < /dev/null
  # rc MUST be captured before ANY other command substitution: $(date ...) spawns a
  # subshell and overwrites $?. That exact bug shipped in #3217's driver ledger and
  # made a failed arm indistinguishable from a clean one.
  local rc=$?
  echo "$(date -u +%FT%TZ) END   $label rc=$rc" >> "$PROG"
  if [ "$rc" -ne 0 ]; then
    log "FAIL  $label rc=$rc — see $DRIVER_LOGS/$label.out (continuing to the next arm; re-run this script to retry it)"
  else
    log "DONE  $label ($(wc -l < "$dir/points.jsonl" 2>/dev/null || echo 0) points)"
  fi
  return 0
}

FAILED=0
for i in "${!ARM_LABELS[@]}"; do
  label="${ARM_LABELS[$i]}${LABEL_SUFFIX}"
  spec="${ARM_SPECS[$i]}"
  if [ -n "$ARMS_REQ" ]; then
    case " $ARMS_REQ " in *" $label "*) ;; *) continue ;; esac
  fi
  run_arm "$label" "$spec"
done

# Report, don't guess: an arm with no complete summary.json did not produce a curve.
log "---- arm status ----"
for i in "${!ARM_LABELS[@]}"; do
  label="${ARM_LABELS[$i]}${LABEL_SUFFIX}"
  if [ -n "$ARMS_REQ" ]; then
    case " $ARMS_REQ " in *" $label "*) ;; *) continue ;; esac
  fi
  if arm_complete "$WS0_RESULTS/$label"; then
    log "  COMPLETE   $label  ($(wc -l < "$WS0_RESULTS/$label/points.jsonl") points)"
  else
    log "  INCOMPLETE $label  <- re-run this script to retry it"
    FAILED=1
  fi
done

echo "$(date -u +%FT%TZ) ALL-ARMS-ATTEMPTED failed=$FAILED" >> "$PROG"
if [ "$FAILED" -eq 1 ]; then
  log "SWEEP INCOMPLETE — at least one arm has no summary.json. Re-run this script (completed arms are skipped)."
  exit 1
fi
log "SWEEP COMPLETE. Analyse with:"
log "  python3 $HERE/analyze-3225.py $WS0_RESULTS"

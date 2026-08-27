#!/usr/bin/env bash
# #3299 WS0 — the bare-scan scaling curve C(S), S = 1..6.
#
# WHAT THIS MEASURES. S independent bare scans (`Database::execute_streaming`
# over the #3096 measurement corpus), each pinned to ONE COMPLETE physical core,
# run concurrently; the aggregate rows/s, per-scan p50 rows/s, cycles/row and
# instructions/row are taken over an ALIGNED window strictly inside the interval
# in which all S scans are producing rows. See README.md for the window
# convention — it is the methodological core of this issue.
#
# WHAT IT DOES NOT DO. It does not regenerate the corpus (already generated and
# verified at /data/ws0-3096), it does not touch scripts/perf/ or
# tools/ws0-corpus-gen/ (the hardened #3272 rig), and it does not report any LLC
# figure: the Step 1 census proved every LLC instrument on this box unavailable,
# so AC3 is DEFERRED per the issue's pre-registered AC5 rather than approximated.
#
# USAGE
#   bash sweep.sh --results <dir> [--s-list 1,2,3,4,5,6] [--reps 3]
#                 [--duration-s 60] [--corpus /data/ws0-3096]
#   bash sweep.sh --equivalence --results <dir>   # worker vs ws0-scan-bench, S=1
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
CONTAIN="$REPO/test-data/scripts/perf-run-contained.sh"
GUARDS="$HERE/guards.py"
REP="$HERE/rep.py"
WORKER_SRC="$HERE/scan-worker"

CORPUS=/data/ws0-3096
RESULTS=""
S_LIST="1,2,3,4,5,6"
REPS=3
DURATION_S=60
PROGRESS_ROWS=16384
PREWARM_PASSES=1
HEADROOM_CORES=2
MEM_CAP=24G
EQUIVALENCE=0
WORKER_TARGET="${WS0_3299_TARGET_DIR:-/data/ws0-3299/worker-target}"
# The four events the Step 1 census proved REAL at 100.00% on this box, plus the
# software `task-clock` (consumes no PMC) as the utilisation denominator. Adding
# an LLC spelling here is refused by guards.py, deliberately.
EVENTS="instructions,cycles,L1-dcache-loads,L1-dcache-load-misses,task-clock"

usage() { sed -n '2,26p' "${BASH_SOURCE[0]}" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --results)        RESULTS="$2"; shift 2 ;;
    --corpus)         CORPUS="$2"; shift 2 ;;
    --s-list)         S_LIST="$2"; shift 2 ;;
    --reps)           REPS="$2"; shift 2 ;;
    --duration-s)     DURATION_S="$2"; shift 2 ;;
    --progress-rows)  PROGRESS_ROWS="$2"; shift 2 ;;
    --prewarm-passes) PREWARM_PASSES="$2"; shift 2 ;;
    --mem)            MEM_CAP="$2"; shift 2 ;;
    --equivalence)    EQUIVALENCE=1; shift ;;
    -h|--help)        usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done
[[ -n "$RESULTS" ]] || { echo "--results is required" >&2; usage; }
[[ -x "$CONTAIN" ]] || { echo "FATAL: containment wrapper missing: $CONTAIN" >&2; exit 2; }

mkdir -p "$RESULTS"
RESULTS="$(cd "$RESULTS" && pwd)"

# --- topology, READ from sysfs and written down --------------------------------
# `nproc` is the LOGICAL count. Pinning one logical CPU per pair half-populates a
# physical core and silently halves the per-core figure (#3224's finding against
# #3217's core table), so every CPU set below is built from thread_siblings_list.
SIB_MAP="$RESULTS/siblings.map"
: > "$SIB_MAP"
for c in /sys/devices/system/cpu/cpu[0-9]*; do
  n="${c##*/cpu}"
  echo "$n $(cat "$c/topology/thread_siblings_list")" >> "$SIB_MAP"
done
sort -n -o "$SIB_MAP" "$SIB_MAP"

# The physical cores, as their complete sibling groups, in ascending order.
mapfile -t CORE_GROUPS < <(awk '{print $2}' "$SIB_MAP" | sort -u -t, -k1,1n)
PHYS=${#CORE_GROUPS[@]}
echo "[sweep] topology: $(nproc) logical / $PHYS physical; groups: ${CORE_GROUPS[*]}"

# --- corpus identity, verified before anything is measured ---------------------
DATA_DB="$(find "$CORPUS" -name '*-Data.db' -print -quit)"
[[ -n "$DATA_DB" ]] || { echo "FATAL: no *-Data.db under $CORPUS" >&2; exit 2; }
if compgen -G "$(dirname "$DATA_DB")/*-CompressionInfo.db" > /dev/null; then
  echo "FATAL: $CORPUS carries a CompressionInfo.db. The #3096 measurement corpus is" >&2
  echo "  UNCOMPRESSED (693.69 B/row); a compressed corpus is a DIFFERENT corpus and its" >&2
  echo "  numbers are not comparable (cross-corpus division is forbidden on this issue)." >&2
  exit 2
fi
DATA_BYTES="$(stat -c %s "$DATA_DB")"
EXPECT_BYTES=2774760422
if [[ "$DATA_BYTES" != "$EXPECT_BYTES" ]]; then
  echo "FATAL: Data.db is $DATA_BYTES bytes, expected $EXPECT_BYTES (the verified #3096" >&2
  echo "  'Corpus B'). A different corpus makes every cross-point comparison invalid." >&2
  exit 2
fi
echo "[sweep] corpus: $DATA_DB ($DATA_BYTES bytes, uncompressed, identity OK)"

# --- build the worker ----------------------------------------------------------
echo "[sweep] building worker (release, repo profile)..."
( cd "$WORKER_SRC" && CARGO_TARGET_DIR="$WORKER_TARGET" cargo build --release ) >"$RESULTS/worker-build.log" 2>&1 \
  || { echo "FATAL: worker build failed, see $RESULTS/worker-build.log" >&2; exit 2; }
WORKER_BIN="$WORKER_TARGET/release/ws0-3299-scan-worker"
[[ -x "$WORKER_BIN" ]] || { echo "FATAL: $WORKER_BIN not built" >&2; exit 2; }

# --- equivalence control: this worker vs the rig's ws0-scan-bench, S=1 ---------
# The worker claims to drive the SAME code path as the rig's bare-scan arm. That
# claim is MEASURED here rather than asserted in a comment: both are run on the
# same single physical core, in the same session, over the same bytes.
if [[ "$EQUIVALENCE" == 1 ]]; then
  BENCH="$REPO/target/release/ws0-scan-bench"
  [[ -x "$BENCH" ]] || { echo "FATAL: $BENCH not built (cargo build --release -p ws0-corpus-gen)" >&2; exit 2; }
  CG="${CORE_GROUPS[0]}"
  echo "[equivalence] core $CG :: ws0-scan-bench --passes 3"
  "$CONTAIN" --mem "$MEM_CAP" --swap 0 -- \
    taskset -c "$CG" "$BENCH" --corpus "$CORPUS" --passes 3 > "$RESULTS/equiv-scan-bench.json" 2> "$RESULTS/equiv-scan-bench.err"
  echo "[equivalence] core $CG :: ws0-3299-scan-worker (S=1, same core)"
  python3 - "$CG" > "$RESULTS/equiv-worker-cpus.json" <<'PY'
import json, sys
print(json.dumps([[int(x) for x in sys.argv[1].split(",")]]))
PY
  "$CONTAIN" --mem "$MEM_CAP" --swap 0 -- \
    python3 "$REP" --s 1 --rep 0 --round 0 --rundir "$RESULTS/equiv-worker" \
      --worker-bin "$WORKER_BIN" --corpus "$CORPUS" \
      --worker-cpus "$(cat "$RESULTS/equiv-worker-cpus.json")" \
      --events "$EVENTS" --duration-s "$DURATION_S" \
      --progress-rows "$PROGRESS_ROWS" --prewarm-passes "$PREWARM_PASSES" \
    > "$RESULTS/equiv-worker.json"
  python3 "$GUARDS" perf-csv --csv "$RESULTS/equiv-worker/perf.csv" --events "$EVENTS"
  python3 "$GUARDS" window --repdir "$RESULTS/equiv-worker" > "$RESULTS/equiv-worker-window.json"
  python3 "$HERE/derive.py" --equivalence "$RESULTS"
  exit 0
fi

# --- the sweep -----------------------------------------------------------------
IFS=',' read -r -a S_VALUES <<< "$S_LIST"
MANIFEST="$RESULTS/manifest.jsonl"
: > "$MANIFEST"

for (( round=1; round<=REPS; round++ )); do
  # S-ORDER ROTATION. Each round visits the S values in a rotated order, so a
  # monotone host drift cannot masquerade as an S effect by always measuring the
  # same S first. NOTE (scripts/perf/README.md): this rig does NOT control drift.
  # The per-round order is recorded so within-round direction is available as
  # INERT DATA EXPLICITLY UNCONTROLLED FOR DRIFT — it is not a verified claim.
  n=${#S_VALUES[@]}
  ORDER=()
  for (( k=0; k<n; k++ )); do ORDER+=( "${S_VALUES[$(( (k + round - 1) % n ))]}" ); done
  echo "[sweep] round $round/$REPS order: ${ORDER[*]}"

  for S in "${ORDER[@]}"; do
    (( S >= 1 )) || { echo "FATAL: S must be >= 1, got $S" >&2; exit 2; }
    # This point's CPU set is the first S COMPLETE sibling groups: worker i owns
    # BOTH logical CPUs of physical core i. Built in one place and then verified
    # by the guard before a single row is scanned.
    (( S <= PHYS )) || { echo "FATAL: S=$S exceeds $PHYS physical cores" >&2; exit 2; }
    WORKER_CPUS="$(printf '%s\n' "${CORE_GROUPS[@]}" | python3 -c '
import json, sys
groups = [l.strip() for l in sys.stdin if l.strip()]
s = int(sys.argv[1])
print(json.dumps([[int(c) for c in g.split(",")] for g in groups[:s]]))
' "$S")"
    CPUS="$(python3 -c '
import json, sys
print(",".join(str(c) for grp in json.loads(sys.argv[1]) for c in grp))
' "$WORKER_CPUS")"
    python3 "$GUARDS" cpuset --s "$S" --cpus "$CPUS" --siblings "$SIB_MAP" --headroom-cores "$HEADROOM_CORES"

    RD="$RESULTS/s${S}-round${round}"
    echo "[sweep] S=$S round=$round cpus=$CPUS -> $RD"
    "$CONTAIN" --mem "$MEM_CAP" --swap 0 -- \
      python3 "$REP" --s "$S" --rep "$round" --round "$round" --rundir "$RD" \
        --worker-bin "$WORKER_BIN" --corpus "$CORPUS" \
        --worker-cpus "$WORKER_CPUS" --events "$EVENTS" \
        --duration-s "$DURATION_S" --progress-rows "$PROGRESS_ROWS" \
        --prewarm-passes "$PREWARM_PASSES" \
      | tee -a "$RESULTS/rep-stdout.log"

    # Both guards run IMMEDIATELY, so a bad rep is refused where it happened
    # rather than surviving to the aggregation step.
    python3 "$GUARDS" perf-csv --csv "$RD/perf.csv" --events "$EVENTS"
    python3 "$GUARDS" window --repdir "$RD" > "$RD/attribution.json"
    echo "{\"s\":$S,\"round\":$round,\"rundir\":\"$RD\",\"order\":\"${ORDER[*]}\"}" >> "$MANIFEST"
  done
done

echo "[sweep] all reps passed their guards; deriving"
python3 "$HERE/derive.py" --results "$RESULTS" | tee "$RESULTS/CS-table.md"

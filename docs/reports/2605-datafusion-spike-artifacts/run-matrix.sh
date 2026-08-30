#!/usr/bin/env bash
# Definitive #2605 bench driver.
#
# WHY A DRIVER SCRIPT AND NOT THE HARNESS'S OWN LOOP: the harness's internal loop
# is scenario -> arm -> iteration, which is WRONG for this corpus for two reasons
# that both showed up in the superseded first run (see the report, §3.0):
#
#   1. PAGE-CACHE DRIFT. The corpus is 10.35 GB on a 30 GB box that is also
#      running the 7 GB Cassandra container that produced it, so cache state warms
#      (and thrashes) across the run. With all of one arm's iterations consecutive,
#      later arms are systematically favoured. Iteration OUTERMOST with the arm
#      order ROTATED per iteration removes that ordering bias.
#   2. PEAK-RSS ATTRIBUTION. Process RSS never returns to its starting point, so a
#      second arm in the same process inherits the first arm's high-water mark. One
#      PROCESS PER CELL makes the peak-RSS column genuinely per-arm.
#
# Usage: run-matrix.sh <table-dir> <ddl-file> <out-dir> [iterations] [start-iteration]
#
# START-ITERATION exists so a cycle can be COMPLETED without re-measuring what is
# already on disk: `... 5 4` runs iterations 4 and 5 only, and the cells it writes
# join the 1-3 already present to make a complete five-arm cycle. The
# counterbalancing check below is applied to the WHOLE range 1..ITERS, not to the
# slice being run, because that is the set an analysis will pool.
set -euo pipefail

DIR=${1:?table dir}
DDL=${2:?ddl file}
OUT=${3:?out dir}
ITERS=${4:-3}
START=${5:-1}

if (( START < 1 || START > ITERS )); then
  echo "run-matrix.sh: start-iteration ${START} must be within 1..${ITERS}" >&2
  exit 2
fi

BIN=${DF_SPIKE_BENCH:-./target/release/df_spike_bench}

# QUIESCENCE. A wall-clock measurement taken on a loaded box measures the box, not
# the code. This is not hypothetical: iteration 4 of this matrix was first
# measured while a sibling job drove the 1-minute load average to 172, and its
# cells came in 1.2x-4.3x slower than the ENTIRE range of iterations 1-3
# (`filtered_scan/row_engine` 399.2 s against a [72.3, 116.4] range). Those cells
# were discarded. Nothing in the cell JSON would have revealed why they were slow,
# which is the actual defect: the contamination was invisible.
#
# So each cell now WAITS for the box to settle and REFUSES rather than measure
# through a storm, and the load it did measure under is recorded next to the
# cells. `MAX_LOAD` is a 1-minute load average; the default admits this harness
# (~1.1 cores) plus a little background but not a parallel build.
MAX_LOAD=${MAX_LOAD:-4.0}
QUIESCE_TIMEOUT_SECS=${QUIESCE_TIMEOUT_SECS:-1800}
MACHINE_LOG="$OUT/machine-state.jsonl"

load_now() { cut -d' ' -f1 /proc/loadavg; }
mem_available_kb() { awk '/^MemAvailable:/ {print $2}' /proc/meminfo; }

wait_for_quiet() {
  local waited=0 load
  load=$(load_now)
  while awk -v l="$load" -v m="$MAX_LOAD" 'BEGIN { exit !(l > m) }'; do
    if (( waited >= QUIESCE_TIMEOUT_SECS )); then
      echo "run-matrix.sh: REFUSING to measure: 1-min load $load exceeds MAX_LOAD=$MAX_LOAD after \
${waited}s of waiting. A wall-clock benchmark on a loaded box measures the box. Re-run when the \
machine is idle, or raise MAX_LOAD deliberately and accept that the numbers are contended." >&2
      exit 3
    fi
    # STDERR. This function is called through command substitution, so its
    # stdout IS the return value: a progress line printed there ended up
    # concatenated with the load number and written into machine-state.jsonl,
    # making the provenance file invalid JSON — the guard corrupting the very
    # record it exists to keep. stdout carries the number and nothing else.
    (( waited == 0 )) && echo ">>> waiting for the box to settle (load $load > $MAX_LOAD)" >&2
    sleep 30
    waited=$(( waited + 30 ))
    load=$(load_now)
  done
  echo "$load"
}
SCENARIOS=(full_scan_count projected_scan filtered_scan)
# Five arm CONFIGS: the four arms, plus DataFusion a second time with
# target_partitions pinned to 1 (thread-count-equalised — see the report §3.2).
ARMS=("floor:" "row_engine:" "datafusion:1" "datafusion:" "row_pushdown:")

# ROTATION ONLY COUNTERBALANCES OVER A COMPLETE CYCLE.
#
# With 5 arm configs, an ITERS that is not a multiple of 5 leaves systematic
# position bias: at ITERS=3 (the matrix this report was measured with)
# `datafusion:1` only ever occupies one of the first three positions and
# `row_pushdown` only ever one of the last three, which is the exact thing the
# rotation was introduced to remove. Calling that a Latin square is wrong.
#
# So a partial cycle is REFUSED by default, and the opt-out is loud and recorded
# rather than silent: an operator who accepts the bias must say so, and the
# schedule actually run is written next to the cells so a reader can see it.
n=${#ARMS[@]}
if (( ITERS % n != 0 )); then
  if [[ ${ALLOW_PARTIAL_CYCLE:-0} != 1 ]]; then
    cat >&2 <<EOF
run-matrix.sh: REFUSING a matrix of ${ITERS} iterations over ${n} arm configs.

Rotation counterbalances position only over a COMPLETE cycle, so ${ITERS} is not
counterbalanced: some arms can never occupy some positions. Use a multiple of
${n} (e.g. ${n}), or set ALLOW_PARTIAL_CYCLE=1 to accept the bias deliberately —
in which case the ordering must be controlled for in the analysis (the #2605
report does this with a cold-fault covariate regression) and the emitted
schedule.json records that the run was not counterbalanced.
EOF
    exit 2
  fi
  echo ">>> WARNING: ${ITERS} iterations over ${n} arm configs is NOT counterbalanced" >&2
fi

mkdir -p "$OUT/cells"

# The schedule actually run, recorded so a cell set is interpretable later. An
# analysis that has to control for ordering needs to know what the ordering WAS.
counterbalanced=false
(( ITERS % n == 0 )) && counterbalanced=true
{
  printf '{\n  "issue": 2605,\n  "arm_configs": %d,\n  "iterations": %d,\n' "$n" "$ITERS"
  printf '  "measured_in_this_invocation": "%d..%d",\n' "$START" "$ITERS"
  printf '  "counterbalanced": %s,\n  "orders": {\n' "$counterbalanced"
} > "$OUT/schedule.json"

for iter in $(seq 1 "$ITERS"); do
  # Rotate the arm order by (iter-1) so no config is always first or always last.
  shift_by=$(( (iter - 1) % n ))
  order=()
  for i in $(seq 0 $((n - 1))); do
    order+=("${ARMS[$(( (i + shift_by) % n ))]}")
  done
  # The schedule record covers EVERY iteration of the matrix, including ones a
  # previous invocation measured — it describes the cell set, not this process.
  sep=","
  (( iter == ITERS )) && sep=""
  printf '    "%d": "%s"%s\n' "$iter" "${order[*]}" "$sep" >> "$OUT/schedule.json"
  if (( iter < START )); then
    echo ">>> iteration $iter already measured — skipping (start-iteration=$START)"
    continue
  fi

  for scenario in "${SCENARIOS[@]}"; do
    for spec in "${order[@]}"; do
      arm=${spec%%:*}
      tp=${spec##*:}
      label="$scenario.$arm${tp:+.tp$tp}.iter$iter"
      load_before=$(wait_for_quiet)
      echo ">>> $label (load $load_before)"
      "$BIN" \
        --dir "$DIR" --ddl-file "$DDL" \
        --projection pk,ck,v_int \
        --filter-column ck --filter-op lt --filter-value 5 \
        --scenario "$scenario" --arm "$arm" --iterations 1 --iteration-base "$iter" \
        ${tp:+--df-target-partitions "$tp"} \
        --out "$OUT/cells/$label.json"
      # The machine state this cell was measured under, recorded so a reader can
      # tell a slow ENGINE from a busy BOX without having to trust a memory.
      printf '{"cell":"%s","load_before":%s,"load_after":%s,"mem_available_kb":%s,"max_load":%s}\n' \
        "$label" "$load_before" "$(load_now)" "$(mem_available_kb)" "$MAX_LOAD" >> "$MACHINE_LOG"
    done
  done
done
printf '  }\n}\n' >> "$OUT/schedule.json"
echo "all cells written to $OUT/cells (schedule recorded in $OUT/schedule.json)"

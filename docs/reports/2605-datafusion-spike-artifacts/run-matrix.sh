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
# Usage: run-matrix.sh <table-dir> <ddl-file> <out-dir> [iterations]
set -euo pipefail

DIR=${1:?table dir}
DDL=${2:?ddl file}
OUT=${3:?out dir}
ITERS=${4:-3}

BIN=${DF_SPIKE_BENCH:-./target/release/df_spike_bench}
SCENARIOS=(full_scan_count projected_scan filtered_scan)
# Five arm CONFIGS: the four arms, plus DataFusion a second time with
# target_partitions pinned to 1 (thread-count-equalised — see the report §3.2).
ARMS=("floor:" "row_engine:" "datafusion:1" "datafusion:" "row_pushdown:")

mkdir -p "$OUT/cells"
for iter in $(seq 1 "$ITERS"); do
  # Rotate the arm order by (iter-1) so no config is always first or always last.
  n=${#ARMS[@]}
  shift_by=$(( (iter - 1) % n ))
  order=()
  for i in $(seq 0 $((n - 1))); do
    order+=("${ARMS[$(( (i + shift_by) % n ))]}")
  done

  for scenario in "${SCENARIOS[@]}"; do
    for spec in "${order[@]}"; do
      arm=${spec%%:*}
      tp=${spec##*:}
      label="$scenario.$arm${tp:+.tp$tp}.iter$iter"
      echo ">>> $label"
      "$BIN" \
        --dir "$DIR" --ddl-file "$DDL" \
        --projection pk,ck,v_int \
        --filter-column ck --filter-op lt --filter-value 5 \
        --scenario "$scenario" --arm "$arm" --iterations 1 --iteration-base "$iter" \
        ${tp:+--df-target-partitions "$tp"} \
        --out "$OUT/cells/$label.json"
    done
  done
done
echo "all cells written to $OUT/cells"

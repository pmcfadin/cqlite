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
run-matrix.sh: REFUSING to run ${ITERS} iterations over ${n} arm configs.

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
  printf '  "counterbalanced": %s,\n  "orders": {\n' "$counterbalanced"
} > "$OUT/schedule.json"

for iter in $(seq 1 "$ITERS"); do
  # Rotate the arm order by (iter-1) so no config is always first or always last.
  shift_by=$(( (iter - 1) % n ))
  order=()
  for i in $(seq 0 $((n - 1))); do
    order+=("${ARMS[$(( (i + shift_by) % n ))]}")
  done
  sep=","
  (( iter == ITERS )) && sep=""
  printf '    "%d": "%s"%s\n' "$iter" "${order[*]}" "$sep" >> "$OUT/schedule.json"

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
printf '  }\n}\n' >> "$OUT/schedule.json"
echo "all cells written to $OUT/cells (schedule recorded in $OUT/schedule.json)"

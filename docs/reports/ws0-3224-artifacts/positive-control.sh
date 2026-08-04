#!/usr/bin/env bash
# =============================================================================
# WS0 #3224 — PMU POSITIVE CONTROL. Run this BEFORE anything else on the host.
#
# Owner's binding condition 3 (issue #3224, 2026-08-03 spend authorization):
#   "Program LLC-loads, LLC-load-misses, cache-references against a known-
#    cache-hostile microbenchmark and show the counts move in the PREDICTED
#    DIRECTION. If any counter is absent or pinned at zero, STOP and report."
#
# WHY THIS IS NOT A SMOKE TEST. The dominant failure mode of this whole program
# is a broken instrument that does not error but emits plausible output. It has
# already happened concretely: on #3217's virtualized host `cache-references`
# did NOT say `<not supported>` — it programmed cleanly and returned a hard 0
# over 40-240 CPU-seconds of a memory-heavy workload. A non-zero check would
# have caught that one, but a counter STUCK AT A CONSTANT NON-ZERO VALUE passes
# a non-zero check and is just as wrong. So this script is a DIFFERENTIAL: it
# asserts each counter MOVES between two arms, in the predicted direction, by at
# least a predicted minimum ratio, and reaches a predicted minimum magnitude.
#
# -----------------------------------------------------------------------------
# THE PREDICTIONS — stated here, in the script, so a reviewer can verify they
# were made BEFORE the measurement rather than fitted to it. Nothing below is
# derived from any #3224 measurement; none exists yet.
# -----------------------------------------------------------------------------
# The two arms run the SAME allocation, the SAME loop and the SAME number of
# accesses (see cache-hostile.c). Only the working-set extent differs:
#   friendly: chase confined to --working-kib (default 256 KiB) => L2-resident
#   hostile:  chase over --buffer-mib (default 2048 MiB) => many times any LLC,
#             random single-cycle permutation over 64 B lines, serial dependency
#             so the prefetcher cannot run ahead.
#
# P1  cycles/access ratio (hostile:friendly) >= 5x        [HOSTILITY CONTROL]
#     An L2 hit is ~14 cycles; a DRAM round trip with a TLB walk is ~250-400.
#     Predicted ~15-25x. Threshold set at 5x. This check uses only `cycles`,
#     which works on every host including #3217's, so it is the control ON THE
#     CONTROL: if it fails, the microbenchmark did not achieve cache hostility
#     and a flat LLC counter would be AMBIGUOUS (nothing to see) rather than
#     evidence of a broken counter. That outcome is INDETERMINATE, not FAIL.
#
# P2  instructions/access ratio within +/-10%             [SYMMETRY CONTROL]
#     Same loop, same iteration count => the arms must execute the same work.
#     If they do not, the comparison is not a control and the counter verdicts
#     are uninterpretable. (This mirrors #3217's own headline shape: the whole
#     finding there is "instructions flat, cycles up".)
#
# P3  LLC-loads       hostile:friendly >= 10x  AND hostile >= 0.20 per access
# P4  LLC-load-misses hostile:friendly >= 10x  AND hostile >= 0.20 per access
# P5  cache-references hostile:friendly >= 10x AND hostile >= 0.20 per access
#     Direction: UP in the hostile arm, in every case. An L2-resident chase
#     never reaches the LLC, so the friendly arm should read ~0; a chase over a
#     working set tens of times the LLC must miss on essentially every access,
#     so the hostile arm should read ~1.0 per access.
#
#     Why ratio >= 10x and not >= 100x (the predicted value): the friendly
#     denominator is small and noisy (OS noise, timer ticks, the perf window's
#     own edges) and may legitimately be 0. 10x is an order of magnitude below
#     the prediction — a genuine instrument clears it by a wide margin, while a
#     stuck, aliased or fixed-value counter cannot clear it at all.
#
#     Why a magnitude floor of 0.20/access as well: a ratio alone can be
#     satisfied by two small noisy numbers. The theoretical hostile value is
#     1.0/access; 0.20 tolerates a 5x under-count from counter-definition
#     subtleties (demand-only counting, line vs access granularity, uncore
#     filtering) and is still unreachable by a counter that is not measuring
#     this workload. If the friendly arm reads exactly 0 the ratio is reported
#     as `inf` and the floor is what carries the verdict.
#
#     ZERO IS ONLY A DEFECT IN THE HOSTILE ARM. A 0 in the friendly arm is the
#     PREDICTED reading and must not be diagnosed as a broken counter. #3217's
#     failure was a hostile-side zero. Getting this backwards would make the
#     control reject working hardware.
#
# P6  cache-misses is measured and reported with the same thresholds but is
#     ADVISORY, not gating: the owner's condition names three counters, and
#     `cache-misses` is a coarser alias whose definition varies by vendor.
#     A failure there is printed prominently and does not by itself exit 1.
#
# -----------------------------------------------------------------------------
# THE FOUR DIAGNOSES ARE DISTINCT, because they have different remedies:
#   ABSENT_EVENT_NAME  perf does not know the event at all       -> wrong host/PMU
#   NOT_SUPPORTED      perf prints `<not supported>`             -> wrong host/PMU
#   SILENT_ZERO        programs, hostile arm reads 0             -> #3217's failure
#   UNRELIABLE         moves less than predicted, or below floor -> do not trust
# plus two non-counter outcomes:
#   INDETERMINATE      the workload was not actually hostile (P1/P2 failed)
#   ENV_ERROR          perf/cc/-D missing; nothing was measured
#
# Exit codes: 0 PASS · 1 FAIL (a required counter is unusable) · 2 ENV_ERROR or
# usage · 3 INDETERMINATE. Anything other than 0 means STOP AND REPORT: do not
# proceed to the #3224 measurement and do not characterize the gap in prose.
#
# Dependency-light by design (bash + perf + cc + coreutils): it must run on a
# fresh bare-metal box before any CQLite build exists.
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ------------------------------------------------------------------ thresholds
RATIO_MIN=10                 # P3-P5 hostile:friendly minimum, integer multiple
RATE_MIN_MILLI=200           # P3-P5 hostile floor, events per access x1000
HOSTILITY_MIN=5              # P1 cycles/access minimum ratio
SYMMETRY_TOL_PCT=10          # P2 instructions/access tolerance, percent
MUX_MIN_PCT=99               # below this the counts are multiplexed estimates

REQUIRED_EVENTS=(LLC-loads LLC-load-misses cache-references)
ADVISORY_EVENTS=(cache-misses)
CONTROL_EVENTS=(cycles instructions)

# --------------------------------------------------------------------- options
OUT_DIR="${PWD}/positive-control-out"
REPS=3
BUFFER_MIB=2048
WORKING_KIB=256
ACCESSES=20000000
CPU=2
RUN_STREAM=1
STREAM_MIB=512
STREAM_THREADS=0             # 0 = all online CPUs

usage() {
  cat <<'EOF'
usage: positive-control.sh [options]
  --out-dir DIR      artefact directory            (default ./positive-control-out)
  --reps N           reps per arm, median reported (default 3)
  --buffer-mib N     hostile working set, MiB      (default 2048)
  --working-kib K    friendly working set, KiB     (default 256; must fit in L2)
  --accesses A       chase accesses per arm        (default 20000000)
  --cpu C            logical CPU to pin to         (default 2)
  --stream-mib N     STREAM-triad array size, MiB  (default 512)
  --stream-threads T STREAM-triad threads, 0=all   (default 0)
  --no-stream        skip the advisory bandwidth reference
  --quick            1 rep, 2M accesses, 512 MiB buffer (mechanics check only;
                     NOT a valid gate result - the verdict is stamped quick=true)
  -h|--help
EOF
  exit 2
}
while [ $# -gt 0 ]; do
  case "$1" in
    --out-dir) OUT_DIR="${2:?}"; shift 2 ;;
    --reps) REPS="${2:?}"; shift 2 ;;
    --buffer-mib) BUFFER_MIB="${2:?}"; shift 2 ;;
    --working-kib) WORKING_KIB="${2:?}"; shift 2 ;;
    --accesses) ACCESSES="${2:?}"; shift 2 ;;
    --cpu) CPU="${2:?}"; shift 2 ;;
    --stream-mib) STREAM_MIB="${2:?}"; shift 2 ;;
    --stream-threads) STREAM_THREADS="${2:?}"; shift 2 ;;
    --no-stream) RUN_STREAM=0; shift ;;
    --quick) REPS=1; ACCESSES=2000000; BUFFER_MIB=512; QUICK=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done
QUICK="${QUICK:-0}"

mkdir -p "$OUT_DIR" || { echo "cannot create $OUT_DIR" >&2; exit 2; }
SUMMARY="$OUT_DIR/summary.txt"
VERDICT="$OUT_DIR/verdict.json"
: > "$SUMMARY"
say() { printf '%s\n' "$*" | tee -a "$SUMMARY"; }
die_env() { say "ENV_ERROR: $*"; printf '{"schema":"ws0-3224.positive-control/v1","result":"ENV_ERROR","reason":"%s"}\n' "$1" > "$VERDICT"; exit 2; }

say "==== WS0 #3224 PMU POSITIVE CONTROL ===="
say "started:    $(date -u +%FT%TZ)"
say "out-dir:    $OUT_DIR"
say "host:       $(uname -n) / $(uname -r)"
[ "$QUICK" = 1 ] && say "MODE:       QUICK (mechanics check only - NOT a valid gate result)"

# ------------------------------------------------------------------ environment
command -v perf >/dev/null 2>&1 || die_env "perf not installed (apt-get install linux-tools-\$(uname -r))"
command -v cc   >/dev/null 2>&1 || die_env "no C compiler (apt-get install build-essential)"
command -v taskset >/dev/null 2>&1 || die_env "taskset not installed (apt-get install util-linux)"

{
  echo "== uname =="; uname -a
  echo; echo "== perf --version =="; perf --version
  echo; echo "== lscpu =="; lscpu
  echo; echo "== sysctls =="
  echo "kernel.perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)"
  echo "kernel.kptr_restrict=$(cat /proc/sys/kernel/kptr_restrict 2>/dev/null)"
  echo; echo "== /sys/bus/event_source/devices (AUTHORITATIVE PMU list) =="
  ls /sys/bus/event_source/devices/ 2>&1
  echo; echo "== numactl --hardware =="; numactl --hardware 2>&1 || echo "(numactl absent)"
} > "$OUT_DIR/env.txt" 2>&1
say "env captured: $OUT_DIR/env.txt"

BIN="$OUT_DIR/cache-hostile"
cc -O2 -std=c99 -pthread -o "$BIN" "$HERE/cache-hostile.c" 2>"$OUT_DIR/build.log" \
  || die_env "cache-hostile.c failed to build; see $OUT_DIR/build.log"
say "built:      $BIN"

# `perf stat -D` is how initialisation is kept OUT of the measured window. Without
# it both arms would carry a large common init term and the ratio would collapse
# toward 1.0 - i.e. a WORKING counter would look broken. Refuse to guess.
perf stat -D 200 -x, -e cycles -o "$OUT_DIR/probe-delay.csv" -- sleep 1 >/dev/null 2>&1
grep -q ',cycles,' "$OUT_DIR/probe-delay.csv" 2>/dev/null \
  || die_env "this perf does not support 'stat -D <ms>' (need >= 4.x); cannot exclude init from the window"

# ------------------------------------------------------------ per-event probing
declare -A EV_STATUS
PROBE="$OUT_DIR/event-probe.txt"
: > "$PROBE"
ALL_EVENTS=("${CONTROL_EVENTS[@]}" "${REQUIRED_EVENTS[@]}" "${ADVISORY_EVENTS[@]}")
USABLE=()
for ev in "${ALL_EVENTS[@]}"; do
  out="$(perf stat -x, -e "$ev" -- true 2>&1)"
  if printf '%s' "$out" | grep -q ",$ev,"; then
    val="$(printf '%s' "$out" | awk -F, -v e="$ev" '$3==e{print $1; exit}')"
    if [ "$val" = "<not supported>" ]; then
      EV_STATUS[$ev]=NOT_SUPPORTED
    else
      EV_STATUS[$ev]=PROGRAMS; USABLE+=("$ev")
    fi
  else
    EV_STATUS[$ev]=ABSENT_EVENT_NAME
  fi
  printf '%-20s %s\n' "$ev" "${EV_STATUS[$ev]}" >> "$PROBE"
done
say ""
say "-- event availability probe (perf stat -e <ev> -- true) --"
sed 's/^/   /' "$PROBE" | tee -a "$SUMMARY" >/dev/null
sed 's/^/   /' "$PROBE"

for ev in "${CONTROL_EVENTS[@]}"; do
  [ "${EV_STATUS[$ev]}" = PROGRAMS ] || die_env "control event '$ev' unusable (${EV_STATUS[$ev]}) - nothing can be measured on this host"
done
EVLIST="$(IFS=,; echo "${USABLE[*]}")"

# ------------------------------------------------ calibrate the -D window delay
# The hostile arm's permutation build is the expensive init; size the delay from a
# measured probe rather than a guessed constant, then assert it took.
CAL="$("$BIN" chase --buffer-mib "$BUFFER_MIB" --working-kib 0 --accesses 1000 \
        --delay-ms 0 --arm calib 2>/dev/null)"
CAL_INIT="$(printf '%s' "$CAL" | awk -F= '$1=="init_s"{print $2}')"
[ -n "$CAL_INIT" ] || die_env "calibration run produced no init_s (binary broken?)"
DELAY_MS="$(awk -v i="$CAL_INIT" 'BEGIN{d=int(i*1500)+3000; if(d<5000)d=5000; print d}')"
say ""
say "-- window calibration --"
say "   hostile-arm init measured at ${CAL_INIT}s  ->  perf -D ${DELAY_MS} ms"
say "   (init is EXCLUDED from the counted window; it is common to both arms and"
say "    counting it would bias the ratio toward 1.0, i.e. hide a working counter)"

# ------------------------------------------------------------------- measurement
run_arm() { # $1 arm, $2 working-kib, $3 rep -> csv path on stdout
  local arm="$1" wkib="$2" rep="$3"
  local csv="$OUT_DIR/perf-${arm}-rep${rep}.csv"
  local log="$OUT_DIR/run-${arm}-rep${rep}.txt"
  taskset -c "$CPU" perf stat -x, -e "$EVLIST" -D "$DELAY_MS" -o "$csv" -- \
    "$BIN" chase --buffer-mib "$BUFFER_MIB" --working-kib "$wkib" \
                 --accesses "$ACCESSES" --delay-ms "$DELAY_MS" --arm "$arm" \
    > "$log" 2>>"$log"
  local rc=$?
  if grep -q '^init_overrun=1' "$log" 2>/dev/null; then
    say "FATAL: ${arm} rep${rep} init overran the perf delay; see $log"
    exit 2
  fi
  [ $rc -eq 0 ] || { say "FATAL: ${arm} rep${rep} exited rc=$rc; see $log"; exit 2; }
  printf '%s' "$csv"
}
cell() { awk -F, -v e="$2" '$3==e{print $1; exit}' "$1"; }   # raw value token
mux()  { awk -F, -v e="$2" '$3==e{print $5; exit}' "$1"; }   # enabled percentage

declare -A MED MUXMIN
say ""
say "-- measurement: ${REPS} rep(s) per arm, pinned to CPU ${CPU} --"
say "   friendly: working set ${WORKING_KIB} KiB (L2-resident)"
say "   hostile:  working set ${BUFFER_MIB} MiB (>> LLC), random 64 B chase"
say "   accesses: ${ACCESSES} per arm (identical in both arms)"
for arm in friendly hostile; do
  wkib=$WORKING_KIB; [ "$arm" = hostile ] && wkib=0
  for rep in $(seq 1 "$REPS"); do
    csv="$(run_arm "$arm" "$wkib" "$rep")"
    say "   ran $arm rep$rep -> $(basename "$csv")"
  done
  for ev in "${ALL_EVENTS[@]}"; do
    if [ "${EV_STATUS[$ev]}" != PROGRAMS ]; then MED["$arm/$ev"]="${EV_STATUS[$ev]}"; continue; fi
    vals=(); bad=""; mmin=10000
    for rep in $(seq 1 "$REPS"); do
      v="$(cell "$OUT_DIR/perf-${arm}-rep${rep}.csv" "$ev")"
      p="$(mux  "$OUT_DIR/perf-${arm}-rep${rep}.csv" "$ev")"
      case "$v" in
        ''|*[!0-9]*) bad="${v:-MISSING_ROW}" ;;
        *) vals+=("$v") ;;
      esac
      case "$p" in ''|*[!0-9.]*) : ;; *) pi=${p%%.*}; [ "$pi" -lt "$mmin" ] && mmin=$pi ;; esac
    done
    MUXMIN["$arm/$ev"]=$mmin
    if [ -n "$bad" ]; then
      case "$bad" in
        "<not supported>") MED["$arm/$ev"]=NOT_SUPPORTED ;;
        "<not counted>")   MED["$arm/$ev"]=NOT_COUNTED ;;
        *)                 MED["$arm/$ev"]=UNPARSEABLE ;;
      esac
    else
      MED["$arm/$ev"]="$(printf '%s\n' "${vals[@]}" | sort -n | sed -n "$(( (REPS+1)/2 ))p")"
    fi
  done
done

# ------------------------------------------------------------------ verdict math
isnum() { case "$1" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac; }
fmt_milli() { printf '%d.%03d' "$(( $1 / 1000 ))" "$(( $1 % 1000 ))"; }

ratio_milli() { # $1 hostile $2 friendly -> milli-ratio, or "inf"/"na"
  if ! isnum "$1" || ! isnum "$2"; then echo na; return; fi
  if [ "$2" -eq 0 ]; then if [ "$1" -gt 0 ]; then echo inf; else echo na; fi; return; fi
  echo $(( $1 * 1000 / $2 ))
}
rate_milli() { if isnum "$1"; then echo $(( $1 * 1000 / ACCESSES )); else echo na; fi; }

C_H="${MED[hostile/cycles]}";       C_F="${MED[friendly/cycles]}"
I_H="${MED[hostile/instructions]}"; I_F="${MED[friendly/instructions]}"
CYC_RATIO="$(ratio_milli "$C_H" "$C_F")"
INS_RATIO="$(ratio_milli "$I_H" "$I_F")"

say ""
say "-- P1 HOSTILITY CONTROL (cycles/access, hostile:friendly, need >= ${HOSTILITY_MIN}x) --"
HOSTILITY=FAIL
if [ "$CYC_RATIO" = inf ]; then HOSTILITY=PASS
elif isnum "$CYC_RATIO" && [ "$CYC_RATIO" -ge $(( HOSTILITY_MIN * 1000 )) ]; then HOSTILITY=PASS; fi
say "   cycles friendly=$C_F  hostile=$C_H  ratio=$( [ "$CYC_RATIO" = inf ] && echo inf || fmt_milli "$CYC_RATIO" )x  -> $HOSTILITY"

say "-- P2 SYMMETRY CONTROL (instructions/access, need within +/-${SYMMETRY_TOL_PCT}%) --"
SYMMETRY=FAIL
if isnum "$INS_RATIO"; then
  lo=$(( (100 - SYMMETRY_TOL_PCT) * 10 )); hi=$(( (100 + SYMMETRY_TOL_PCT) * 10 ))
  if [ "$INS_RATIO" -ge "$lo" ] && [ "$INS_RATIO" -le "$hi" ]; then SYMMETRY=PASS; fi
fi
say "   instructions friendly=$I_F  hostile=$I_H  ratio=$( isnum "$INS_RATIO" && fmt_milli "$INS_RATIO" || echo na )x  -> $SYMMETRY"

declare -A EV_VERDICT EV_RATIO EV_RATE
evaluate() { # $1 event -> sets EV_VERDICT/EV_RATIO/EV_RATE
  local ev="$1" h="${MED[hostile/$1]}" f="${MED[friendly/$1]}"
  local r; r="$(ratio_milli "$h" "$f")"; local q; q="$(rate_milli "$h")"
  EV_RATIO[$ev]="$r"; EV_RATE[$ev]="$q"
  if ! isnum "$h"; then EV_VERDICT[$ev]="$h"; return; fi        # NOT_SUPPORTED etc
  if [ "$h" -eq 0 ]; then EV_VERDICT[$ev]=SILENT_ZERO; return; fi
  local moved=0 big=0
  { [ "$r" = inf ] || { isnum "$r" && [ "$r" -ge $(( RATIO_MIN * 1000 )) ]; }; } && moved=1
  { isnum "$q" && [ "$q" -ge "$RATE_MIN_MILLI" ]; } && big=1
  if [ $moved -eq 1 ] && [ $big -eq 1 ]; then EV_VERDICT[$ev]=OK
  elif [ $moved -eq 0 ] && [ $big -eq 0 ]; then EV_VERDICT[$ev]=UNRELIABLE_NO_MOVEMENT_AND_LOW_RATE
  elif [ $moved -eq 0 ]; then EV_VERDICT[$ev]=UNRELIABLE_NO_MOVEMENT
  else EV_VERDICT[$ev]=UNRELIABLE_LOW_RATE; fi
}
report_ev() {
  local ev="$1" tag="$2" r="${EV_RATIO[$1]}" q="${EV_RATE[$1]}"
  local rs qs
  rs="$( [ "$r" = inf ] && echo "inf" || { isnum "$r" && fmt_milli "$r" || echo na; } )"
  qs="$( isnum "$q" && fmt_milli "$q" || echo na )"
  say "   $(printf '%-17s' "$ev") [$tag] friendly=${MED[friendly/$ev]}  hostile=${MED[hostile/$ev]}"
  say "                     ratio=${rs}x (need >= ${RATIO_MIN}x)  hostile-rate=${qs}/access (need >= $(fmt_milli $RATE_MIN_MILLI))  -> ${EV_VERDICT[$ev]}"
  local m="${MUXMIN[hostile/$ev]:-100}"
  if isnum "$m" && [ "$m" -lt "$MUX_MIN_PCT" ]; then
    say "                     WARNING: multiplexed at ${m}% enabled - counts are scaled estimates"
  fi
}

say ""
say "-- P3-P5 REQUIRED COUNTERS (gating) --"
FAILED_REQUIRED=0
for ev in "${REQUIRED_EVENTS[@]}"; do
  evaluate "$ev"; report_ev "$ev" required
  [ "${EV_VERDICT[$ev]}" = OK ] || FAILED_REQUIRED=$(( FAILED_REQUIRED + 1 ))
done
say ""
say "-- P6 ADVISORY COUNTERS (reported, not gating) --"
for ev in "${ADVISORY_EVENTS[@]}"; do evaluate "$ev"; report_ev "$ev" advisory; done

# ---------------------------------------------- advisory host-capability probes
say ""
say "-- AC3/AC5 capability probes (reported, never gating) --"
UNCORE_LIST="$(ls /sys/bus/event_source/devices/ 2>/dev/null | tr '\n' ' ')"
IMC_COUNT="$(ls -d /sys/bus/event_source/devices/uncore_imc* 2>/dev/null | wc -l | tr -d ' ')"
say "   sysfs PMUs (AUTHORITATIVE): $UNCORE_LIST"
say "   uncore_imc* PMU instances:  $IMC_COUNT"
say "   NOTE: 'perf list | grep uncore' is MISLEADING - it lists per-model JSON"
say "         event-table entries, not PMUs present on this host. sysfs is truth."
MBW_OUT="$(perf stat -M MemoryBandwidth -a -- sleep 1 2>&1 | tr '\n' '|' | cut -c1-400)"
MBW_OK=no; printf '%s' "$MBW_OUT" | grep -qi 'cannot find metric' || MBW_OK=maybe
say "   perf stat -M MemoryBandwidth: $MBW_OK"
printf '%s\n' "$MBW_OUT" > "$OUT_DIR/memorybandwidth-probe.txt"

STREAM_G24=""; STREAM_G32=""; STREAM_T=""
if [ "$RUN_STREAM" = 1 ]; then
  SOUT="$("$BIN" stream --stream-mib "$STREAM_MIB" --threads "$STREAM_THREADS" \
           --iters 5 --delay-ms 100 2>"$OUT_DIR/stream.err")"
  printf '%s\n' "$SOUT" > "$OUT_DIR/stream-reference.txt"
  STREAM_G24="$(printf '%s' "$SOUT" | awk -F= '$1=="gbps_basis24"{print $2}')"
  STREAM_G32="$(printf '%s' "$SOUT" | awk -F= '$1=="gbps_basis32"{print $2}')"
  STREAM_T="$(printf '%s'  "$SOUT" | awk -F= '$1=="threads"{print $2}')"
  say "   STREAM-triad-class reference (NOT the vendor STREAM benchmark):"
  say "     threads=${STREAM_T}  ${STREAM_G24} GB/s @24B/elem basis  ${STREAM_G32} GB/s @32B/elem (incl. RFO)"
  say "     AC5 needs this re-run at the SAME core/NUMA binding as the engine arms."
else
  say "   STREAM-triad reference: SKIPPED (--no-stream)"
fi

# ---------------------------------------------------------------- final verdict
say ""
if [ "$HOSTILITY" != PASS ] || [ "$SYMMETRY" != PASS ]; then
  RESULT=INDETERMINATE; RC=3
  REASON="the microbenchmark did not establish a valid differential (P1 hostility=$HOSTILITY, P2 symmetry=$SYMMETRY); counter verdicts are uninterpretable, NOT evidence of broken counters. Remedy: raise --buffer-mib well above this host's LLC, lower --working-kib below L2, and re-run."
elif [ "$FAILED_REQUIRED" -eq 0 ]; then
  RESULT=PASS; RC=0
  REASON="all required counters programmed, moved in the predicted direction, and reached the predicted magnitude."
else
  RESULT=FAIL; RC=1
  REASON="$FAILED_REQUIRED of ${#REQUIRED_EVENTS[@]} required counters unusable on this host. STOP AND REPORT (#3224 owner condition 3): do not proceed to the measurement and do not characterize the gap in prose."
fi
say "==== RESULT: $RESULT ===="
say "$REASON"
[ "$QUICK" = 1 ] && say "NOTE: --quick was used; this is a mechanics check, not a valid gate result."
say "artefacts:  $OUT_DIR"

{
  printf '{\n  "schema": "ws0-3224.positive-control/v1",\n'
  printf '  "issue": 3224,\n  "result": "%s",\n  "exit_code": %d,\n' "$RESULT" "$RC"
  printf '  "reason": "%s",\n' "$REASON"
  printf '  "quick_mode": %s,\n' "$( [ "$QUICK" = 1 ] && echo true || echo false )"
  printf '  "started_utc": "%s",\n' "$(date -u +%FT%TZ)"
  printf '  "host": {"nodename": "%s", "kernel": "%s", "perf": "%s"},\n' \
    "$(uname -n)" "$(uname -r)" "$(perf --version 2>/dev/null | head -1)"
  printf '  "config": {"reps": %s, "buffer_mib": %s, "working_kib": %s, "accesses": %s, "cpu": %s, "perf_delay_ms": %s},\n' \
    "$REPS" "$BUFFER_MIB" "$WORKING_KIB" "$ACCESSES" "$CPU" "$DELAY_MS"
  printf '  "thresholds": {"ratio_min": %s, "hostile_rate_min_per_access": %s, "hostility_min": %s, "symmetry_tol_pct": %s},\n' \
    "$RATIO_MIN" "$(fmt_milli $RATE_MIN_MILLI)" "$HOSTILITY_MIN" "$SYMMETRY_TOL_PCT"
  printf '  "controls": {"hostility": "%s", "cycles_ratio": "%s", "symmetry": "%s", "instructions_ratio": "%s"},\n' \
    "$HOSTILITY" "$( [ "$CYC_RATIO" = inf ] && echo inf || { isnum "$CYC_RATIO" && fmt_milli "$CYC_RATIO" || echo na; } )" \
    "$SYMMETRY" "$( isnum "$INS_RATIO" && fmt_milli "$INS_RATIO" || echo na )"
  printf '  "counters": {\n'
  first=1
  for ev in "${REQUIRED_EVENTS[@]}" "${ADVISORY_EVENTS[@]}" "${CONTROL_EVENTS[@]}"; do
    [ -n "${EV_VERDICT[$ev]:-}" ] || evaluate "$ev"
    [ $first -eq 1 ] || printf ',\n'; first=0
    gating=false
    for r in "${REQUIRED_EVENTS[@]}"; do [ "$r" = "$ev" ] && gating=true; done
    printf '    "%s": {"probe": "%s", "gating": %s, "friendly_median": "%s", "hostile_median": "%s", "ratio": "%s", "hostile_per_access": "%s", "verdict": "%s", "min_enabled_pct": "%s"}' \
      "$ev" "${EV_STATUS[$ev]}" "$gating" "${MED[friendly/$ev]}" "${MED[hostile/$ev]}" \
      "$( [ "${EV_RATIO[$ev]}" = inf ] && echo inf || { isnum "${EV_RATIO[$ev]}" && fmt_milli "${EV_RATIO[$ev]}" || echo na; } )" \
      "$( isnum "${EV_RATE[$ev]}" && fmt_milli "${EV_RATE[$ev]}" || echo na )" \
      "${EV_VERDICT[$ev]}" "${MUXMIN[hostile/$ev]:-unknown}"
  done
  printf '\n  },\n'
  printf '  "advisory": {"sysfs_pmus": "%s", "uncore_imc_instances": %s, "perf_M_MemoryBandwidth": "%s", "stream_triad_threads": "%s", "stream_triad_gbps_basis24": "%s", "stream_triad_gbps_basis32": "%s", "stream_note": "STREAM-triad-class, not the vendor STREAM benchmark; re-run at the engine core/NUMA binding for AC5"}\n' \
    "$UNCORE_LIST" "$IMC_COUNT" "$MBW_OK" "${STREAM_T:-null}" "${STREAM_G24:-null}" "${STREAM_G32:-null}"
  printf '}\n'
} > "$VERDICT"

exit "$RC"

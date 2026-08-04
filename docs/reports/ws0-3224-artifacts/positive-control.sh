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
# asserts each counter MOVES between two arms by at least a predicted minimum,
# and that the LLC MISS RATE rises in the predicted direction. (Raw LLC-loads
# deliberately has NO asserted direction — see P3-P5 below; it legitimately
# FALLS on healthy hardware, and asserting otherwise reds a good box.)
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
#     Predicted ~15-30x (measured 27.8x in local development). Threshold 5x.
#     This check uses only `cycles`, which works on every host including
#     #3217's, so it is the control ON THE CONTROL: if it fails, the
#     microbenchmark did not achieve cache hostility and a flat LLC counter
#     would be AMBIGUOUS (nothing to see) rather than evidence of a broken
#     counter. That outcome is INDETERMINATE, not FAIL.
#
#     P1 is only meaningful BECAUSE P2 verifies the two arms executed the same
#     work. A cycles ratio compared across arms with DIFFERENT access counts
#     says nothing — which is why running this script's thresholds against some
#     other tool's two-arm walk yields INDETERMINATE (P2 fails first), not FAIL.
#     INDETERMINATE is the honest verdict for "that was a different experiment".
#
# P2  instructions/access ratio within +/-10%             [SYMMETRY CONTROL]
#     Same loop, same iteration count => the arms must execute the same work.
#     If they do not, the comparison is not a control and the counter verdicts
#     are uninterpretable. (This mirrors #3217's own headline shape: the whole
#     finding there is "instructions flat, cycles up".)
#
# P3  LLC-loads        MOVES between arms by >= 2x, IN EITHER DIRECTION
# P4  LLC-load-misses  LLC MISS RATE (misses/loads) RISES by >= 1.5x in hostile
# P5  cache-references MOVES between arms by >= 2x, IN EITHER DIRECTION
#
#     *** DO NOT ASSERT THAT RAW LLC-loads GOES UP. *** This is the single
#     easiest way to build a control that reds a HEALTHY host, and it has been
#     measured on the actual #3224 target box (i4i.metal, Xeon 8375C, owner's
#     manual cache-hostile-vs-friendly walk over 512 MiB, 2026-08-04 — those are
#     the owner's numbers from a manual walk, NOT output of this script):
#
#         arm       LLC-loads  LLC-load-misses  miss rate   cycles   IPC
#         friendly    389,812           54,391     13.95%   2.10e9  2.18
#         hostile     110,149           67,449     61.23%   6.84e8  1.14
#
#     LLC-loads FELL 3.5x in the hostile arm on a perfectly healthy PMU, because
#     the prefetcher stops generating loads once the access pattern defeats it;
#     the loads that ARE issued then actually miss, so the MISS RATE rose 4.4x.
#     Raw LLC-load-misses moved only 1.24x, so a raw-magnitude gate on that
#     counter would also have red-flagged a good box. The invariant that
#     survives is the RATIO: hostility raises the fraction of LLC accesses that
#     miss. Gate on that, and on the fact that each counter MOVES AT ALL.
#
#     Chosen thresholds and their margin against those measured healthy-host
#     numbers: movement >= 2x (LLC-loads measured 3.54x, margin 1.8x;
#     cache-references measured ~8x, margin 4x); miss-rate rise >= 1.5x
#     (measured 4.39x, margin 2.9x). Every gate below clears the one healthy
#     host we have real numbers from, with margin, in the direction it actually
#     moved.
#
#     Per-access magnitude is REPORTED BUT NOT GATED, deliberately. The naive
#     prediction is ~1.0 LLC miss per access, but on that same healthy host a
#     512 MiB walk reported only ~67k LLC-load-misses — two orders of magnitude
#     below the line-fill count — because these events count a narrower subset
#     (demand loads, prefetch-excluded) than the intuition assumes. A magnitude
#     floor is therefore not defensible against measured evidence and would be
#     a false-FAIL generator on a metered clock.
#
#     A HARD ZERO IS STILL A DEFECT. If BOTH arms read 0 the counter is the
#     #3217 silent instrument (SILENT_ZERO). If the hostile arm reads exactly 0
#     while the friendly arm does not (HOSTILE_ZERO), that is not physically
#     credible for a memory-bound arm either. But a low friendly reading on its
#     own is expected and must never be diagnosed as a broken counter.
#
# P6  cache-misses is measured and reported with the same thresholds but is
#     ADVISORY, not gating: the owner's condition names three counters, and
#     `cache-misses` is a coarser alias whose definition varies by vendor.
#     A failure there is printed prominently and does not by itself exit 1.
#
# -----------------------------------------------------------------------------
# TWO MEASUREMENT-INTEGRITY DECISIONS, both forced by measurement, not taste:
#
# (i) THE WINDOW IS GATED EXACTLY AROUND THE CHASE, via perf's control FIFO
#     (`perf stat -D -1 --control fifo:<ctl>,<ack>`; cache-hostile.c writes
#     enable/disable). Both neighbouring phases are large AND asymmetric between
#     the arms, so counting either one corrupts the differential:
#       - init (page faults + permutation build): huge in hostile, ~free in
#         friendly;
#       - exit-time address-space teardown: measured on a 512 MiB buffer at 192M
#         instructions (hostile) vs 80M (friendly) — larger than the chase
#         itself, and it does NOT cancel.
#     A `perf stat -D <ms>` delay excludes init but NOT teardown, which is why
#     it is only the standalone fallback and not this gate.
#
# (ii) EVENTS ARE COUNTED USER-ONLY (`:u`). The hostile arm runs ~30x longer in
#     wall-clock than the friendly arm at equal access count, so it absorbs
#     proportionally more timer/IRQ kernel work: measured, that alone put the
#     instruction ratio at 1.22 (P2 FAIL) with kernel counting on, and at
#     1.00002 with `:u`. The microbenchmark's work is entirely user-space, so
#     `:u` is both correct and what makes P2 a sharp instrument.
#
# GATE INTEGRITY IS ITSELF CHECKED. Before the arms run, a 1000-access hostile
# run must report < 1M instructions. An ungated window over a multi-GiB buffer
# reports hundreds of millions, so this catches a silently-failed FIFO
# handshake — the same "plausible output from a broken instrument" class this
# whole script exists to defeat.
#
# -----------------------------------------------------------------------------
# THE DIAGNOSES ARE DISTINCT, because they have different remedies:
#   ABSENT_EVENT_NAME  perf does not know the event at all       -> wrong host/PMU
#   NOT_SUPPORTED      perf prints `<not supported>`             -> wrong host/PMU
#   SILENT_ZERO        programs, hostile arm reads 0             -> #3217's failure
#   UNRELIABLE_*       moves less than predicted, or below floor -> do not trust
# plus two non-counter outcomes:
#   INDETERMINATE      the workload was not actually hostile (P1/P2 failed)
#   ENV_ERROR          perf/cc/control-FIFO missing; nothing was measured
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
MOVE_MIN_MILLI=2000          # P3/P5 minimum movement between arms, EITHER
                             #   direction, x1000. Measured healthy-host margin:
                             #   LLC-loads 3.54x, cache-references ~8x.
MISSRATE_MIN_MILLI=1500      # P4 minimum rise in misses/loads, x1000. Measured
                             #   healthy-host value 4.39x (13.95% -> 61.23%).
HOSTILITY_MIN=5              # P1 cycles/access minimum ratio
SYMMETRY_TOL_PCT=10          # P2 instructions/access tolerance, percent
MUX_MIN_PCT=99               # below this the counts are multiplexed estimates
GATE_PROBE_ACCESSES=1000     # gate-integrity probe size
GATE_PROBE_MAX_INSTR=1000000 # ...and its ceiling; ungated would be ~1e8-1e9

MOD=":u"                     # user-only; see integrity decision (ii)
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
  --buffer-mib N     hostile working set, MiB      (default 2048; must be >> LLC)
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
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
SUMMARY="$OUT_DIR/summary.txt"
VERDICT="$OUT_DIR/verdict.json"
: > "$SUMMARY"
say() { printf '%s\n' "$*" | tee -a "$SUMMARY"; }
die_env() {
  say "==== RESULT: ENV_ERROR ===="
  say "$1"
  printf '{"schema":"ws0-3224.positive-control/v1","issue":3224,"result":"ENV_ERROR","exit_code":2,"reason":"%s"}\n' \
    "$1" > "$VERDICT"
  exit 2
}

say "==== WS0 #3224 PMU POSITIVE CONTROL ===="
say "started:    $(date -u +%FT%TZ)"
say "out-dir:    $OUT_DIR"
say "host:       $(uname -n) / $(uname -r)"
[ "$QUICK" = 1 ] && say "MODE:       QUICK (mechanics check only - NOT a valid gate result)"

# ------------------------------------------------------------------ environment
command -v perf    >/dev/null 2>&1 || die_env "perf not installed (apt-get install linux-tools-\$(uname -r))"
command -v cc      >/dev/null 2>&1 || die_env "no C compiler (apt-get install build-essential)"
command -v taskset >/dev/null 2>&1 || die_env "taskset not installed (apt-get install util-linux)"
command -v timeout >/dev/null 2>&1 || die_env "timeout(1) not installed (coreutils)"

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

CTL="$OUT_DIR/perf-ctl.fifo"; ACK="$OUT_DIR/perf-ack.fifo"
cleanup() { rm -f "$CTL" "$ACK"; }
trap cleanup EXIT INT TERM

# ------------------------------------------------------------ per-event probing
declare -A EV_STATUS
PROBE="$OUT_DIR/event-probe.txt"
: > "$PROBE"
ALL_EVENTS=("${CONTROL_EVENTS[@]}" "${REQUIRED_EVENTS[@]}" "${ADVISORY_EVENTS[@]}")
USABLE=()
for ev in "${ALL_EVENTS[@]}"; do
  out="$(perf stat -x, -e "${ev}${MOD}" -- true 2>&1)"
  if printf '%s' "$out" | grep -q ",${ev}${MOD},"; then
    val="$(printf '%s' "$out" | awk -F, -v e="${ev}${MOD}" '$3==e{print $1; exit}')"
    if [ "$val" = "<not supported>" ]; then
      EV_STATUS[$ev]=NOT_SUPPORTED
    else
      EV_STATUS[$ev]=PROGRAMS; USABLE+=("${ev}${MOD}")
    fi
  else
    EV_STATUS[$ev]=ABSENT_EVENT_NAME
  fi
  printf '%-20s %s\n' "$ev" "${EV_STATUS[$ev]}" >> "$PROBE"
done
say ""
say "-- event availability probe (perf stat -e <ev>${MOD} -- true) --"
while read -r line; do say "   $line"; done < "$PROBE"

for ev in "${CONTROL_EVENTS[@]}"; do
  [ "${EV_STATUS[$ev]}" = PROGRAMS ] || \
    die_env "control event '$ev' unusable (${EV_STATUS[$ev]}) - nothing at all can be measured on this host"
done
EVLIST="$(IFS=,; echo "${USABLE[*]}")"

# ------------------------------------------------------------------- measurement
run_chase() { # $1 out-csv  $2 working-kib  $3 accesses  $4 arm-label  $5 log
  rm -f "$CTL" "$ACK"; mkfifo "$CTL" "$ACK" || return 90
  taskset -c "$CPU" timeout 900 \
    perf stat -x, -e "$EVLIST" -D -1 --control "fifo:$CTL,$ACK" -o "$1" -- \
      "$BIN" chase --buffer-mib "$BUFFER_MIB" --working-kib "$2" --accesses "$3" \
                   --arm "$4" --ctl-fifo "$CTL" --ack-fifo "$ACK" \
    > "$5" 2>>"$5"
  return $?
}
cell() { awk -F, -v e="$2" '$3==e{print $1; exit}' "$1"; }   # raw value token
mux()  { awk -F, -v e="$2" '$3==e{print $5; exit}' "$1"; }   # enabled percentage

# --- gate integrity: an ungated window would report orders of magnitude more ---
say ""
say "-- gate integrity (control-FIFO window really is closed around the chase) --"
run_chase "$OUT_DIR/gate-probe.csv" 0 "$GATE_PROBE_ACCESSES" gate-probe "$OUT_DIR/gate-probe.txt"
grc=$?
[ $grc -eq 0 ] || die_env "control-FIFO gate probe failed (rc=$grc). Either this perf lacks 'stat --control fifo:' (needs >= 5.13) or the handshake did not complete; see $OUT_DIR/gate-probe.txt"
GP_INSTR="$(cell "$OUT_DIR/gate-probe.csv" "instructions${MOD}")"
case "$GP_INSTR" in ''|*[!0-9]*) die_env "gate probe produced no instruction count ('${GP_INSTR:-empty}')" ;; esac
if [ "$GP_INSTR" -ge "$GATE_PROBE_MAX_INSTR" ]; then
  die_env "gate probe counted $GP_INSTR instructions for only $GATE_PROBE_ACCESSES accesses (ceiling $GATE_PROBE_MAX_INSTR). The perf window is NOT gated to the chase - it is also counting buffer init and/or address-space teardown, which are asymmetric between the arms and would corrupt every ratio below."
fi
say "   $GP_INSTR instructions for $GATE_PROBE_ACCESSES accesses (ceiling $GATE_PROBE_MAX_INSTR) -> PASS"

declare -A MED MUXMIN
say ""
say "-- measurement: ${REPS} rep(s) per arm, pinned to CPU ${CPU}, events counted ${MOD} --"
say "   friendly: working set ${WORKING_KIB} KiB (L2-resident)"
say "   hostile:  working set ${BUFFER_MIB} MiB (>> LLC), random 64 B chase"
say "   accesses: ${ACCESSES} per arm (identical in both arms)"
for arm in friendly hostile; do
  wkib=$WORKING_KIB; [ "$arm" = hostile ] && wkib=0
  for rep in $(seq 1 "$REPS"); do
    run_chase "$OUT_DIR/perf-${arm}-rep${rep}.csv" "$wkib" "$ACCESSES" "$arm" \
              "$OUT_DIR/run-${arm}-rep${rep}.txt"
    rc=$?
    [ $rc -eq 0 ] || die_env "${arm} rep${rep} exited rc=$rc; see $OUT_DIR/run-${arm}-rep${rep}.txt"
    nspa="$(awk -F= '$1=="ns_per_access"{print $2}' "$OUT_DIR/run-${arm}-rep${rep}.txt")"
    say "   ran $arm rep$rep  (${nspa:-?} ns/access wall-clock)"
  done
  for ev in "${ALL_EVENTS[@]}"; do
    if [ "${EV_STATUS[$ev]}" != PROGRAMS ]; then MED["$arm/$ev"]="${EV_STATUS[$ev]}"; continue; fi
    vals=(); bad=""; mmin=10000
    for rep in $(seq 1 "$REPS"); do
      v="$(cell "$OUT_DIR/perf-${arm}-rep${rep}.csv" "${ev}${MOD}")"
      p="$(mux  "$OUT_DIR/perf-${arm}-rep${rep}.csv" "${ev}${MOD}")"
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
show_milli() { if [ "$1" = inf ]; then printf 'inf'; elif isnum "$1"; then fmt_milli "$1"; else printf 'na'; fi; }

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
say "   cycles friendly=$C_F  hostile=$C_H  ratio=$(show_milli "$CYC_RATIO")x  -> $HOSTILITY"

say "-- P2 SYMMETRY CONTROL (instructions/access, need within +/-${SYMMETRY_TOL_PCT}%) --"
SYMMETRY=FAIL
if isnum "$INS_RATIO"; then
  lo=$(( (100 - SYMMETRY_TOL_PCT) * 10 )); hi=$(( (100 + SYMMETRY_TOL_PCT) * 10 ))
  if [ "$INS_RATIO" -ge "$lo" ] && [ "$INS_RATIO" -le "$hi" ]; then SYMMETRY=PASS; fi
fi
say "   instructions friendly=$I_F  hostile=$I_H  ratio=$(show_milli "$INS_RATIO")x  -> $SYMMETRY"

# Direction-agnostic movement: max/min. See the P3-P5 header block - raw LLC-loads
# legitimately FALLS in the hostile arm on healthy hardware (prefetcher stops
# issuing them), so asserting a direction here is a false-FAIL generator.
move_milli() {
  if ! isnum "$1" || ! isnum "$2"; then echo na; return; fi
  local hi=$1 lo=$2
  [ "$1" -lt "$2" ] && { hi=$2; lo=$1; }
  if [ "$lo" -eq 0 ]; then if [ "$hi" -gt 0 ]; then echo inf; else echo na; fi; return; fi
  echo $(( hi * 1000 / lo ))
}
# LLC miss rate = misses/loads, x1000. The invariant that survives prefetcher
# behaviour: hostility raises the FRACTION of LLC accesses that miss.
MISSRATE_F=na; MISSRATE_H=na; MISSRATE_RISE=na
compute_missrate() {
  local lf="${MED[friendly/LLC-loads]}" lh="${MED[hostile/LLC-loads]}"
  local mf="${MED[friendly/LLC-load-misses]}" mh="${MED[hostile/LLC-load-misses]}"
  isnum "$lf" && isnum "$lh" && isnum "$mf" && isnum "$mh" || return
  [ "$lf" -gt 0 ] && [ "$lh" -gt 0 ] || return
  MISSRATE_F=$(( mf * 1000 / lf )); MISSRATE_H=$(( mh * 1000 / lh ))
  [ "$MISSRATE_F" -gt 0 ] || { MISSRATE_RISE=inf; return; }
  MISSRATE_RISE=$(( MISSRATE_H * 1000 / MISSRATE_F ))
}

declare -A EV_VERDICT EV_MOVE EV_RATE
evaluate() { # $1 event -> sets EV_VERDICT/EV_MOVE/EV_RATE
  local ev="$1" h="${MED[hostile/$1]}" f="${MED[friendly/$1]}"
  local m q; m="$(move_milli "$h" "$f")"; q="$(rate_milli "$h")"
  EV_MOVE[$ev]="$m"; EV_RATE[$ev]="$q"
  if ! isnum "$h" || ! isnum "$f"; then EV_VERDICT[$ev]="${h}"; return; fi
  if [ "$h" -eq 0 ] && [ "$f" -eq 0 ]; then EV_VERDICT[$ev]=SILENT_ZERO; return; fi
  if [ "$h" -eq 0 ]; then EV_VERDICT[$ev]=HOSTILE_ZERO; return; fi
  local moved=0
  { [ "$m" = inf ] || { isnum "$m" && [ "$m" -ge "$MOVE_MIN_MILLI" ]; }; } && moved=1
  if [ "$moved" -eq 0 ]; then EV_VERDICT[$ev]=UNRELIABLE_NO_MOVEMENT; return; fi
  if [ "$ev" = LLC-load-misses ]; then
    if [ "$MISSRATE_RISE" = inf ]; then EV_VERDICT[$ev]=OK
    elif isnum "$MISSRATE_RISE" && [ "$MISSRATE_RISE" -ge "$MISSRATE_MIN_MILLI" ]; then EV_VERDICT[$ev]=OK
    elif [ "$MISSRATE_RISE" = na ]; then EV_VERDICT[$ev]=UNRELIABLE_MISSRATE_UNCOMPUTABLE
    else EV_VERDICT[$ev]=UNRELIABLE_MISSRATE_FLAT; fi
    return
  fi
  EV_VERDICT[$ev]=OK
}
report_ev() {
  local ev="$1" tag="$2"
  say "   $(printf '%-17s' "$ev") [$tag] friendly=${MED[friendly/$ev]}  hostile=${MED[hostile/$ev]}"
  say "                     movement=$(show_milli "${EV_MOVE[$ev]}")x either-direction (need >= $(fmt_milli $MOVE_MIN_MILLI)x)  hostile-per-access=$(show_milli "${EV_RATE[$ev]}") (REPORTED, not gated)  -> ${EV_VERDICT[$ev]}"
  local m="${MUXMIN[hostile/$ev]:-100}"
  if isnum "$m" && [ "$m" -lt "$MUX_MIN_PCT" ]; then
    say "                     WARNING: multiplexed at ${m}% enabled - counts are scaled estimates"
  fi
}

compute_missrate
say ""
say "-- P4 LLC MISS RATE (misses/loads; the prefetcher-proof invariant) --"
say "   friendly=$(show_milli "$MISSRATE_F")  hostile=$(show_milli "$MISSRATE_H")  rise=$(show_milli "$MISSRATE_RISE")x (need >= $(fmt_milli $MISSRATE_MIN_MILLI)x)"

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
perf stat -M MemoryBandwidth -a -- sleep 1 > "$OUT_DIR/memorybandwidth-probe.txt" 2>&1
MBW_OK=no
grep -qi 'cannot find metric' "$OUT_DIR/memorybandwidth-probe.txt" || MBW_OK=maybe
say "   perf stat -M MemoryBandwidth: $MBW_OK (raw: memorybandwidth-probe.txt)"

STREAM_G24=""; STREAM_G32=""; STREAM_T=""
if [ "$RUN_STREAM" = 1 ]; then
  SOUT="$(timeout 900 "$BIN" stream --stream-mib "$STREAM_MIB" --threads "$STREAM_THREADS" \
           --iters 5 --delay-ms 0 2>"$OUT_DIR/stream.err")"
  printf '%s\n' "$SOUT" > "$OUT_DIR/stream-reference.txt"
  STREAM_G24="$(printf '%s' "$SOUT" | awk -F= '$1=="gbps_basis24"{print $2}')"
  STREAM_G32="$(printf '%s' "$SOUT" | awk -F= '$1=="gbps_basis32"{print $2}')"
  STREAM_T="$(printf '%s'  "$SOUT" | awk -F= '$1=="threads"{print $2}')"
  say "   STREAM-triad-class reference (NOT the vendor STREAM benchmark):"
  say "     threads=${STREAM_T:-?}  ${STREAM_G24:-?} GB/s @24B/elem basis  ${STREAM_G32:-?} GB/s @32B/elem (incl. RFO)"
  say "     AC5 needs this re-run at the SAME core/NUMA binding as the engine arms."
else
  say "   STREAM-triad reference: SKIPPED (--no-stream)"
fi

# ---------------------------------------------------------------- final verdict
say ""
if [ "$HOSTILITY" != PASS ] || [ "$SYMMETRY" != PASS ]; then
  RESULT=INDETERMINATE; RC=3
  REASON="the microbenchmark did not establish a valid differential (P1 hostility=$HOSTILITY, P2 symmetry=$SYMMETRY); the counter verdicts below are UNINTERPRETABLE, not evidence about the counters. Remedy: raise --buffer-mib well above this host's LLC, lower --working-kib below L2, and re-run."
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
  printf '  "completed_utc": "%s",\n' "$(date -u +%FT%TZ)"
  printf '  "host": {"nodename": "%s", "kernel": "%s", "perf": "%s"},\n' \
    "$(uname -n)" "$(uname -r)" "$(perf --version 2>/dev/null | head -1)"
  printf '  "config": {"reps": %s, "buffer_mib": %s, "working_kib": %s, "accesses": %s, "cpu": %s, "event_modifier": "%s", "window_gate": "perf-control-fifo"},\n' \
    "$REPS" "$BUFFER_MIB" "$WORKING_KIB" "$ACCESSES" "$CPU" "$MOD"
  printf '  "gate_integrity": {"probe_accesses": %s, "probe_instructions": %s, "ceiling": %s, "verdict": "PASS"},\n' \
    "$GATE_PROBE_ACCESSES" "$GP_INSTR" "$GATE_PROBE_MAX_INSTR"
  printf '  "thresholds": {"movement_min_either_direction": "%s", "missrate_rise_min": "%s", "hostility_min": %s, "symmetry_tol_pct": %s, "per_access_magnitude": "reported, not gated"},\n' \
    "$(fmt_milli $MOVE_MIN_MILLI)" "$(fmt_milli $MISSRATE_MIN_MILLI)" "$HOSTILITY_MIN" "$SYMMETRY_TOL_PCT"
  printf '  "llc_miss_rate": {"friendly": "%s", "hostile": "%s", "rise": "%s"},\n' \
    "$(show_milli "$MISSRATE_F")" "$(show_milli "$MISSRATE_H")" "$(show_milli "$MISSRATE_RISE")"
  printf '  "controls": {"hostility": "%s", "cycles_ratio": "%s", "symmetry": "%s", "instructions_ratio": "%s"},\n' \
    "$HOSTILITY" "$(show_milli "$CYC_RATIO")" "$SYMMETRY" "$(show_milli "$INS_RATIO")"
  printf '  "counters": {\n'
  first=1
  for ev in "${REQUIRED_EVENTS[@]}" "${ADVISORY_EVENTS[@]}" "${CONTROL_EVENTS[@]}"; do
    [ -n "${EV_VERDICT[$ev]:-}" ] || evaluate "$ev"
    [ $first -eq 1 ] || printf ',\n'; first=0
    gating=false
    for r in "${REQUIRED_EVENTS[@]}"; do [ "$r" = "$ev" ] && gating=true; done
    printf '    "%s": {"probe": "%s", "gating": %s, "friendly_median": "%s", "hostile_median": "%s", "movement_either_direction": "%s", "hostile_per_access_reported_not_gated": "%s", "verdict": "%s", "min_enabled_pct": "%s"}' \
      "$ev" "${EV_STATUS[$ev]}" "$gating" "${MED[friendly/$ev]}" "${MED[hostile/$ev]}" \
      "$(show_milli "${EV_MOVE[$ev]}")" "$(show_milli "${EV_RATE[$ev]}")" \
      "${EV_VERDICT[$ev]}" "${MUXMIN[hostile/$ev]:-unknown}"
  done
  printf '\n  },\n'
  printf '  "advisory": {"sysfs_pmus": "%s", "uncore_imc_instances": %s, "perf_M_MemoryBandwidth": "%s", "stream_triad_threads": "%s", "stream_triad_gbps_basis24": "%s", "stream_triad_gbps_basis32": "%s", "stream_note": "STREAM-triad-class, not the vendor STREAM benchmark; re-run at the engine core/NUMA binding for AC5"}\n' \
    "$UNCORE_LIST" "$IMC_COUNT" "$MBW_OK" "${STREAM_T:-null}" "${STREAM_G24:-null}" "${STREAM_G32:-null}"
  printf '}\n'
} > "$VERDICT"

exit "$RC"

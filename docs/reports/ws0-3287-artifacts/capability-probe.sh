#!/usr/bin/env bash
# WS0 #3287 capability probe — can THIS host answer #3287's method requirements?
#
# #3287 needs three things #3224's capture did not have:
#   (1) a TMA level-2 breakdown,
#   (2) an offcore/prefetch-stall term,
#   (3) the same two endpoints, comparable to #3224 §5.3.
#
# This script answers (1) and (2) as CAPABILITY questions, and it does so with a
# DIFFERENTIAL against a workload whose memory behaviour is known before it is
# measured (#3224's own committed cache-hostile.c). That matters because the
# failure mode on a virtualized guest is NOT "<not supported>" — it is a counter
# that programs cleanly and returns a measurement-shaped ZERO (#3224 negative
# control, finding 2). A smoke test cannot see that; a differential can.
#
# Usage: bash capability-probe.sh <output-dir> [path-to-cache-hostile.c]
set -uo pipefail

OUT="${1:?usage: capability-probe.sh <output-dir> [cache-hostile.c]}"
SRC="${2:-docs/reports/ws0-3224-artifacts/cache-hostile.c}"
mkdir -p "$OUT"

{
  echo "== date -u =="; date -u
  echo; echo "== uname -a =="; uname -a
  echo; echo "== perf --version =="; perf --version
  echo; echo "== sysctls (permission layer) =="
  echo "kernel.perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)"
  echo "kernel.kptr_restrict=$(cat /proc/sys/kernel/kptr_restrict)"
  echo; echo "== sysfs PMUs (AUTHORITATIVE uncore test; never grep perf list) =="
  ls /sys/bus/event_source/devices/
  echo; echo "== uncore devices =="; ls -d /sys/bus/event_source/devices/uncore* 2>&1
  echo; echo "== lscpu (topology + cache) =="; lscpu | grep -vE '^Flags'
  echo; echo "== numactl --hardware =="; numactl --hardware 2>&1
} > "$OUT/host/capability-probe.txt" 2>&1

# --- requirement (1): TMA. On Icelake+ TMA comes from PERF_METRICS via the
# --- topdown-* pseudo-events and the `slots` event. Absence is categorical.
{
  echo "== perf stat -M TopdownL1 =="; perf stat -M TopdownL1 -- true 2>&1
  echo; echo "== perf stat -M TopdownL2 =="; perf stat -M TopdownL2 -- true 2>&1
  echo; echo "== topdown.slots (raw) =="; perf stat -e topdown.slots -- true 2>&1
  for e in topdown-retiring topdown-fe-bound topdown-be-bound topdown-bad-spec slots; do
    echo; echo "== $e =="; perf stat -e "$e" -- true 2>&1
  done
} > "$OUT/host/tma-probe.txt" 2>&1

# --- per-event triage: ABSENT-FROM-PMU vs NOT-SUPPORTED vs programs-and-returns-a-value.
# --- Three-valued on purpose: "programs" is NOT "measures" (see the differential below).
EVENTS="
cycles instructions
cycle_activity.stalls_total cycle_activity.stalls_l2_miss cycle_activity.stalls_l3_miss
offcore_requests_outstanding.all_data_rd offcore_requests_outstanding.cycles_with_data_rd
offcore_requests.all_data_rd offcore_requests_buffer.sq_full
l1d_pend_miss.pending l1d_pend_miss.fb_full l1d_pend_miss.l2_stall
idq_uops_not_delivered.core int_misc.recovery_cycles exe_activity.bound_on_stores
cycle_activity.stalls_mem_any topdown.slots
LLC-loads LLC-load-misses cache-references cache-misses
"
{
  printf '%-52s %s\n' "EVENT" "DISPOSITION"
  for e in $EVENTS; do
    out=$(perf stat -e "$e" -- true 2>&1)
    if   grep -q 'Bad event\|Unable to find\|No supported events' <<<"$out"; then st="ABSENT-FROM-PMU"
    elif grep -q '<not supported>'                                <<<"$out"; then st="NOT-SUPPORTED"
    elif grep -q '<not counted>'                                  <<<"$out"; then st="NOT-COUNTED"
    else st="PROGRAMS (value=$(awk -v ev="$e" '$2==ev{print $1}' <<<"$out" | head -1))"; fi
    printf '%-52s %s\n' "$e" "$st"
  done
} > "$OUT/host/event-disposition.txt" 2>&1

# --- AC4-style: assert each event's DEFINITION on the host before trusting it (#3224 §5.2).
{
  for e in cycle_activity.stalls_l3_miss cycle_activity.stalls_l2_miss \
           cycle_activity.stalls_total offcore_requests_outstanding.all_data_rd; do
    echo "== $e =="
    perf list --details 2>/dev/null | grep -A3 -E "^  ${e}\$"
    echo
  done
} > "$OUT/host/counter-semantics-verification.txt" 2>&1

# --- requirement (2), and the load-bearing part: the DIFFERENTIAL.
# --- Two arms, identical code path, only the working-set extent varies.
# --- Any counter claiming to see the memory hierarchy MUST move with it.
CH="$OUT/cache-hostile.bin"
cc -O2 -o "$CH" "$SRC" -lm 2>"$OUT/host/cache-hostile-build.txt" || { echo "BUILD FAILED" >&2; exit 1; }

EV=cycles,instructions,cycle_activity.stalls_total,cycle_activity.stalls_l2_miss,cycle_activity.stalls_l3_miss,cpu/event=0xa3,umask=0x6,cmask=0x6,name=raw_stalls_l3_miss/,cpu/event=0xa3,umask=0x5,cmask=0x5,name=raw_stalls_l2_miss/,offcore_requests_outstanding.all_data_rd,offcore_requests_outstanding.cycles_with_data_rd,l1d_pend_miss.fb_full,l1d_pend_miss.pending,cache-misses

run_arm() { # name buffer-mib working-kib accesses
  echo "########## ARM $1 (buffer=${2}MiB working=${3}KiB accesses=$4) ##########"
  taskset -c 2,10 perf stat -e "$EV" -- \
    "$CH" chase --buffer-mib "$2" --working-kib "$3" --accesses "$4" --arm "$1" --delay-ms 3000 2>&1
  echo
}
{
  echo "L3 as the guest sees it: $(lscpu | awk -F: '/L3 cache/{gsub(/^ +/,"",$2);print $2}')"
  echo "Prediction BEFORE measuring: the 2048MiB arm's working set is ~20x L3, a random"
  echo "single-dependency chase, so it cannot be L3-resident. Any honest L3-miss-stall"
  echo "counter MUST be large there. A zero is a silent instrument, not a measurement."
  echo
  run_arm friendly-L2resident 512  256 20000000
  run_arm hostile-512m       512    0 20000000
  run_arm hostile-2g        2048    0  8000000
} > "$OUT/host/differential.txt" 2>&1
CH2="$CH"

# --- UNMULTIPLEXED confirmation. The 12-event group above time-shares on this host
# --- (#3224 section 3.3: split rather than publish scaled values). A capability ZERO
# --- survives scaling -- 0 x any scale factor is 0 -- but the artefact must not rest
# --- on that argument alone, so the two counters the verdict turns on are re-run in a
# --- 4-event group and the enabled-% column (perf stat -x, field 5) is published.
{
  echo "== 4-event group, expect enabled%=100.00 in field 5 of every row =="
  echo "-- arm: hostile-2g (working set ~20x L3) --"
  taskset -c 2,10 perf stat -x, -e cycles,cycle_activity.stalls_l2_miss,cycle_activity.stalls_l3_miss,offcore_requests_outstanding.all_data_rd \
    -- "$CH2" chase --buffer-mib 2048 --working-kib 0 --accesses 8000000 --arm hostile-2g-nomux --delay-ms 3000 2>&1
  echo
  echo "-- field key: count,unit,event,run_time,enabled_pct --"
} > "$OUT/host/differential-unmultiplexed.txt" 2>&1

rm -f "$CH"
echo "capability probe written to $OUT/host/"

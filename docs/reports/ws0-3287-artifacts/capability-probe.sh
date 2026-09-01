#!/usr/bin/env bash
# WS0 #3287 capability probe — can THIS host answer #3287's method requirements?
#
# #3287 needs three things #3224's capture did not have:
#   (1) a TMA level-2 breakdown,
#   (2) an offcore/prefetch-stall term,
#   (3) the same two endpoints, comparable to #3224 section 5.3.
#
# This script answers (1) and (2) as CAPABILITY questions, with a DIFFERENTIAL
# against a workload whose memory behaviour is known before it is measured
# (#3224's committed cache-hostile.c). That matters because the failure mode on a
# virtualized guest is NOT "<not supported>" -- it is a counter that programs
# cleanly and returns a measurement-shaped ZERO (#3224 negative control, finding
# 2). A smoke test cannot see that; a differential can.
#
# ---------------------------------------------------------------------------
# THREE MEASUREMENT-INTEGRITY DECISIONS, each forced by review, not by taste:
#
# (i) THE PERF WINDOW IS GATED EXACTLY AROUND THE CHASE, via perf's control FIFO
#     (perf stat -D -1 --control fifo:<ctl>,<ack>; cache-hostile.c drives the
#     handshake). An earlier revision of this script passed only the benchmark's
#     --delay-ms and NO perf -D, so it counted init AND teardown. Both phases are
#     large and ASYMMETRIC between the arms, so counting either corrupts every
#     ratio: #3224 measured exit-time address-space teardown at 192M instructions
#     (hostile) vs 80M (friendly) on a 512 MiB buffer -- larger than the chase
#     itself, and it does NOT cancel. A -D <ms> delay excludes init but not
#     teardown, which is why it is NOT used here even as a fallback: an
#     unavailable FIFO is a FAIL, never a quiet downgrade to a contaminated
#     window. (#3287 roborev job 305, finding 2.)
#
#     NOTE what this does and does not affect. The CAPABILITY verdict -- "this
#     counter reads exactly 0" -- is INVARIANT to the window, because
#     contamination can only ADD counts and no amount of extra work turns a
#     nonzero counter into a zero. The RATIOS are not invariant, so they are
#     published only from the gated window.
#
# (ii) EVENTS ARE COUNTED USER-ONLY (:u). The hostile arm runs ~30x longer in
#     wall clock at equal access count, so it absorbs proportionally more
#     timer/IRQ kernel work; #3224 measured that alone putting the instruction
#     ratio at 1.22 with kernel counting on, and 1.00002 with :u.
#
# (iii) EVERY CAPTURE IS CHECKED, AND A POSITIVE VERDICT REQUIRES AN AFFIRMATIVE
#     MEASUREMENT. An earlier revision ended each arm with a successful `echo`
#     under `set +e`, so a failed perf/taskset was swallowed and the script still
#     printed "capability probe written" over incomplete evidence -- the fail-open
#     class this repository's doctrine exists to remove. Now every capture's exit
#     status is recorded, and the script exits non-zero with an explicit
#     VERDICT: UNMEASURED rather than reporting a probe it did not take.
#     (#3287 roborev job 305, finding 3.)
#
# Usage: bash capability-probe.sh <output-dir> [path-to-cache-hostile.c]
# Exit:  0 = probe COMPLETE (read the verdict lines for what it found)
#        1 = probe INCOMPLETE / a required step could not be measured
#        2 = usage or build error
set -uo pipefail

OUT="${1:-}"
[ -n "$OUT" ] || { echo "usage: capability-probe.sh <output-dir> [cache-hostile.c]" >&2; exit 2; }
SRC="${2:-docs/reports/ws0-3224-artifacts/cache-hostile.c}"

# Finding 1: every redirect below targets $OUT/host, so it must exist. The
# previous revision created only $OUT, so the documented invocation with a fresh
# output directory failed before producing any artefact -- and this script's own
# run masked it, because that run's $OUT/host already existed.
mkdir -p "$OUT/host" || { echo "cannot create $OUT/host" >&2; exit 2; }

FAILED=0
note_fail() { FAILED=1; echo "PROBE-STEP-FAILED: $*" >&2; }

# Run a capture block; a non-zero rc is RECORDED, never swallowed.
capture() { # $1 label  $2 outfile  rest: command
  local label="$1" out="$2"; shift 2
  "$@" > "$out" 2>&1
  local rc=$?
  echo "capture-rc: $label = $rc" >> "$out"
  [ $rc -eq 0 ] || note_fail "$label (rc=$rc, see $out)"
  return $rc
}

{
  echo "== date -u =="; date -u
  echo; echo "== uname -a =="; uname -a
  echo; echo "== perf --version =="; perf --version
  echo; echo "== sysctls (permission layer, INDEPENDENT of the capability layer) =="
  echo "kernel.perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>&1)"
  echo "kernel.kptr_restrict=$(cat /proc/sys/kernel/kptr_restrict 2>&1)"
  echo; echo "== sysfs PMUs (AUTHORITATIVE uncore test; never grep perf list) =="
  ls /sys/bus/event_source/devices/
  echo; echo "== uncore devices =="; ls -d /sys/bus/event_source/devices/uncore* 2>&1
  echo; echo "== lscpu (topology + cache) =="; lscpu | grep -vE '^Flags'
  echo; echo "== numactl --hardware =="; numactl --hardware 2>&1
} > "$OUT/host/capability-probe.txt" 2>&1 || note_fail "host inventory"

# --- requirement (1): TMA. On Icelake+ TMA is served by PERF_METRICS via the
# --- topdown-* pseudo-events plus `slots`. Absence there is categorical.
{
  echo "== perf stat -M TopdownL1 =="; perf stat -M TopdownL1 -- true 2>&1
  echo; echo "== perf stat -M TopdownL2 =="; perf stat -M TopdownL2 -- true 2>&1
  for e in topdown.slots slots topdown-retiring topdown-fe-bound topdown-be-bound topdown-bad-spec; do
    echo; echo "== $e =="; perf stat -e "$e" -- true 2>&1
  done
} > "$OUT/host/tma-probe.txt" 2>&1
# Deliberately NO note_fail here. `perf stat` exits non-zero when an event is
# absent from the PMU, and on the hosts this probe exists to characterise that is
# precisely the ANSWER, not a failure to obtain one. Treating it as a step failure
# would stamp VERDICT: UNMEASURED on a run that measured exactly what was asked.
# The block's own redirect failing is a different matter, and would surface as an
# empty/missing artefact.
[ -s "$OUT/host/tma-probe.txt" ] || note_fail "tma probe produced no output at all"

# --- per-event triage. Three-valued on purpose, and PROGRAMS is deliberately not
# --- called SUPPORTED: programming a counter and measuring with it differ.
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
# As with the TMA block: this sweep's whole job is to CLASSIFY absent/unsupported
# events, so a non-zero rc from a probed event is data, not a step failure.
[ -s "$OUT/host/event-disposition.txt" ] || note_fail "event disposition produced no output at all"

# --- AC4-style: assert each event's DEFINITION on the host (#3224 section 5.2).
{
  for e in cycle_activity.stalls_l3_miss cycle_activity.stalls_l2_miss \
           cycle_activity.stalls_total offcore_requests_outstanding.all_data_rd; do
    echo "== $e =="
    perf list --details 2>/dev/null | grep -A3 -E "^  ${e}\$"
    echo
  done
} > "$OUT/host/counter-semantics-verification.txt" 2>&1
[ -s "$OUT/host/counter-semantics-verification.txt" ] || note_fail "counter semantics produced no output at all"

# --- requirement (2), and the load-bearing part: the GATED DIFFERENTIAL.
CH="$OUT/cache-hostile.bin"
if ! cc -O2 -o "$CH" "$SRC" -lm 2>"$OUT/host/cache-hostile-build.txt"; then
  echo "VERDICT: UNMEASURED (cache-hostile.c did not build; see host/cache-hostile-build.txt)" \
    | tee "$OUT/host/differential.txt" >&2
  exit 2
fi

CTL="$OUT/.ctl.fifo"; ACK="$OUT/.ack.fifo"
MOD=":u"   # decision (ii)
ev_list() { local IFS=,; echo "$*"; }
# The user-only modifier has TWO spellings and they are not interchangeable: a
# symbolic event takes a ':u' SUFFIX, while a raw cpu/.../ event takes the
# modifier INSIDE the trailing slash ('/u'). Measured on this host: the
# '/...:u' form is rejected outright ("Unrecognized input"), which took down the
# whole 12-event group -- and, because the group is all-or-nothing, silently
# lost three arms while a 4-event group using only symbolic events still
# succeeded. Do not unify these two spellings.
EV_FULL=$(ev_list \
  "cycles$MOD" "instructions$MOD" \
  "cycle_activity.stalls_total$MOD" "cycle_activity.stalls_l2_miss$MOD" \
  "cycle_activity.stalls_l3_miss$MOD" \
  "cpu/event=0xa3,umask=0x6,cmask=0x6,name=raw_stalls_l3_miss/u" \
  "cpu/event=0xa3,umask=0x5,cmask=0x5,name=raw_stalls_l2_miss/u" \
  "offcore_requests_outstanding.all_data_rd$MOD" \
  "offcore_requests_outstanding.cycles_with_data_rd$MOD" \
  "l1d_pend_miss.fb_full$MOD" "l1d_pend_miss.pending$MOD" "cache-misses$MOD")
# The 4-event set the verdict actually turns on, small enough not to multiplex.
EV_CORE=$(ev_list "cycles$MOD" "cycle_activity.stalls_l2_miss$MOD" \
  "cycle_activity.stalls_l3_miss$MOD" "offcore_requests_outstanding.all_data_rd$MOD")

# Decision (i): window gated to the chase via the control FIFO. No -D fallback.
run_arm() { # $1 events  $2 arm  $3 buffer-mib  $4 working-kib  $5 accesses  $6 outfile
  rm -f "$CTL" "$ACK"
  mkfifo "$CTL" "$ACK" || { note_fail "mkfifo for arm $2"; return 90; }
  taskset -c 2,10 timeout 900 \
    perf stat -x, -e "$1" -D -1 --control "fifo:$CTL,$ACK" -o "$6.csv" -- \
      "$CH" chase --buffer-mib "$3" --working-kib "$4" --accesses "$5" \
                  --arm "$2" --ctl-fifo "$CTL" --ack-fifo "$ACK" \
    > "$6.txt" 2>&1
  local rc=$?
  rm -f "$CTL" "$ACK"
  echo "arm-rc: $2 = $rc" >> "$6.txt"
  [ $rc -eq 0 ] || note_fail "arm $2 (rc=$rc, see $6.txt / $6.csv)"
  return $rc
}

D="$OUT/host"
GATE_OK=1
# Gate integrity FIRST: a tiny hostile chase. If the window were not closed
# around the chase, this would count init/teardown and read orders of magnitude
# more instructions than 1000 accesses can produce. Also proves the handshake
# works before any real arm is attributed to it.
if ! run_arm "$EV_CORE" gate-probe 512 0 1000 "$D/gate-probe"; then
  GATE_OK=0
  note_fail "control-FIFO gate probe: this perf may lack 'stat --control fifo:' (needs >= 5.13), or the handshake did not complete. NOT falling back to -D: that window includes teardown."
fi

if [ "$GATE_OK" = 1 ]; then
  run_arm "$EV_FULL" friendly-L2resident 512  256 20000000 "$D/arm-friendly"
  run_arm "$EV_FULL" hostile-512m        512    0 20000000 "$D/arm-hostile-512m"
  run_arm "$EV_FULL" hostile-2g         2048    0  8000000 "$D/arm-hostile-2g"
  # Unmultiplexed confirmation of the two counters the verdict turns on.
  run_arm "$EV_CORE" hostile-2g-nomux   2048    0  8000000 "$D/arm-hostile-2g-nomux"
fi
rm -f "$CH" "$CTL" "$ACK"

{
  echo "== #3287 capability probe =="
  echo "L3 as this host sees it: $(lscpu | awk -F: '/L3 cache/{gsub(/^ +/,"",$2);print $2}')"
  echo "window: control-FIFO gated to the chase (-D -1 --control fifo:), events counted ${MOD}"
  echo "gate-integrity-probe: $([ "$GATE_OK" = 1 ] && echo OK || echo FAILED)"
  echo
  echo "Prediction BEFORE measuring: the 2048MiB arm's working set is many times L3,"
  echo "a random single-dependency chase, so it cannot be L3-resident. Any honest"
  echo "L3-miss-stall or offcore counter MUST be large there. A zero is a silent"
  echo "instrument, not a measurement -- and a zero is invariant to the window,"
  echo "since contamination can only ADD counts."
  echo
  if [ "$FAILED" = 0 ]; then
    echo "VERDICT: COMPLETE (all steps measured; read host/arm-*.csv for values)"
  else
    echo "VERDICT: UNMEASURED (at least one step failed; see PROBE-STEP-FAILED on stderr"
    echo "         and the capture-rc/arm-rc lines in host/*.txt). No capability claim"
    echo "         may be derived from this run."
  fi
} > "$D/differential.txt" 2>&1

if [ "$FAILED" = 0 ]; then
  echo "capability probe COMPLETE -> $D/"
  exit 0
fi
echo "capability probe INCOMPLETE -> $D/ (see VERDICT in $D/differential.txt)" >&2
exit 1

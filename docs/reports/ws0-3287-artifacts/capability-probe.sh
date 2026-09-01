#!/usr/bin/env bash
# WS0 #3287 capability probe — can THIS host answer #3287's method requirements?
#
# #3287 needs three things #3224's capture did not have:
#   (1) a TMA level-2 breakdown,
#   (2) an offcore/prefetch-stall term,
#   (3) the same two endpoints, comparable to #3224 section 5.3.
#
# This script is the PRE-FLIGHT GATE named in negative-control-c7i-guest.md.
# Run it on a candidate host BEFORE staging a corpus and before any metered hour
# is spent. It answers Gates A (TMA), B (offcore) and C (#3224's own baseline
# counters) by MEASUREMENT, and reports Gate D's topology facts.
#
# WHY A DIFFERENTIAL AND NOT A SMOKE TEST. On a virtualized guest the failure mode
# is NOT "<not supported>" -- it is a counter that programs cleanly and returns a
# measurement-shaped ZERO (#3224 negative control, finding 2). "Is it non-zero?"
# passes on the working counters and cannot see the stuck ones. So every counter is
# checked against a workload whose behaviour is known BEFORE it is measured:
# #3224's cache-hostile.c, two arms, identical code path, only the working-set
# extent differs. Any counter claiming to see the memory hierarchy must MOVE.
#
# ---------------------------------------------------------------------------
# MEASUREMENT-INTEGRITY DECISIONS, every one of them forced by review:
#
# (i) THE PERF WINDOW IS GATED EXACTLY AROUND THE CHASE, via perf's control FIFO
#     (perf stat -D -1 --control fifo:). Revision 1 passed only the benchmark's
#     --delay-ms and no perf -D, so it counted buffer init AND address-space
#     teardown: measured, ~244M instructions of constant term against ~6.3k of
#     actual 1e3-access chase, which drove the friendly/hostile INSTRUCTION ratio
#     to 9.4 when the control requires ~1.0. There is deliberately NO -D fallback:
#     that window excludes init but not teardown, so an unavailable FIFO is a FAIL,
#     never a quiet downgrade. Window closure is then MEASURED, not asserted, by a
#     scaling differential (below). (job 305 f2, job 308 f1.)
#
# (ii) EVENTS ARE COUNTED USER-ONLY (:u). The hostile arm runs far longer in wall
#     clock at equal access count, so it absorbs proportionally more timer/IRQ
#     kernel work; #3224 measured that alone putting the instruction ratio at 1.22
#     with kernel counting on and 1.00002 with :u. NOTE the modifier has two
#     non-interchangeable spellings: symbolic events take a ':u' SUFFIX, raw
#     cpu/.../ events take it INSIDE the trailing slash ('/u'). The '/...:u' form
#     is rejected outright, and since a perf group is all-or-nothing that one
#     character silently killed three of four arms in revision 2.
#
# (iii) EVENTS ARE RUN IN SMALL PURPOSE-BUILT GROUPS, NOT ONE BIG ONE. Revision 2
#     used a 12-event group that time-shared at 65-75% enabled, so every value was
#     a scaled estimate (#3224 section 3.3 forbids publishing those as counts) and
#     the derived ratios were rep-noisy enough to be indefensible. Four small
#     groups each run at 100.00% enabled, so every number here is a COUNT. This is
#     a simplification that also strengthens the evidence.
#
# (iv) THE PINNED CPUS ARE DERIVED FROM SYSFS, NEVER HARDCODED. Revisions 1-3 used
#     `taskset -c 2,10`, inherited from this host's own layout -- the exact defect
#     #3224 section 3.2 records about #3217's hardcoded core table ("would have
#     silently measured a different machine than it labelled"), and a direct
#     contradiction of this artefact's own Gate D ("sibling map read from /sys,
#     never assumed"). The pair is now read from
#     /sys/devices/system/cpu/cpuN/topology/thread_siblings_list and validated as a
#     COMPLETE sibling group, or the run fails. (job 309 f3.)
#
# (v) A POSITIVE VERDICT REQUIRES AN AFFIRMATIVE MEASUREMENT. A zero exit from
#     perf is not evidence that numbers exist: every CSV is parsed, every requested
#     event must have a row with a numeric count and 100.00% enabled time, and any
#     failure stamps VERDICT: UNMEASURED and exits non-zero. Distinguish carefully
#     from the ONE legitimate case: perf exits non-zero when an event is ABSENT
#     from the PMU, and for a capability probe that is the ANSWER, not a failure.
#     Absent-vs-broken is therefore classified explicitly everywhere.
#
# Usage: bash capability-probe.sh <output-dir> [path-to-cache-hostile.c]
# Exit:  0 = probe COMPLETE (read host/differential.txt for the gate verdicts)
#        1 = probe INCOMPLETE / a required step could not be measured
#        2 = usage or build error
set -uo pipefail

OUT="${1:-}"
[ -n "$OUT" ] || { echo "usage: capability-probe.sh <output-dir> [cache-hostile.c]" >&2; exit 2; }
SRC="${2:-docs/reports/ws0-3224-artifacts/cache-hostile.c}"
# Every redirect below targets $OUT/host. Revision 1 created only $OUT, so the
# documented invocation failed on a fresh directory -- and its own run masked that,
# because that run's $OUT/host already existed. (job 305 f1.)
mkdir -p "$OUT/host" || { echo "cannot create $OUT/host" >&2; exit 2; }
D="$OUT/host"

FAILED=0
note_fail() { FAILED=1; echo "PROBE-STEP-FAILED: $*" >&2; }

# ---------------------------------------------------------------- host inventory
# Each command's status is recorded INDIVIDUALLY: a single `{ a; b; } > f || fail`
# propagates only b's status. (job 308 f2.)
: > "$D/capability-probe.txt" || { echo "cannot write inventory" >&2; exit 2; }
inv() { local h="$1"; shift
  { echo; echo "== $h =="; "$@" 2>&1; local rc=$?; echo "[rc=$rc] $h"
    [ $rc -eq 0 ] || note_fail "inventory: $h (rc=$rc)"; } >> "$D/capability-probe.txt"; }
inv "date -u" date -u
inv "uname -a" uname -a
inv "perf --version" perf --version
inv "perf_event_paranoid (permission layer, INDEPENDENT of capability)" cat /proc/sys/kernel/perf_event_paranoid
inv "kptr_restrict" cat /proc/sys/kernel/kptr_restrict
inv "sysfs PMUs (AUTHORITATIVE uncore test; never grep perf list)" ls /sys/bus/event_source/devices/
inv "lscpu" lscpu
inv "numactl --hardware" numactl --hardware
# Uncore ABSENCE is a Gate C answer, not an error: `ls` exits non-zero when a glob
# matches nothing, and on the hosts this probe characterises that IS the finding.
{ echo; echo "== uncore devices (Gate C) =="
  if ls -d /sys/bus/event_source/devices/uncore* 2>/dev/null; then echo "[uncore: PRESENT]"
  else echo "[uncore: ABSENT — AC3's bandwidth source and AC5's saturation verdict are unreachable here. A measured ANSWER, not a probe failure.]"; fi
} >> "$D/capability-probe.txt" 2>&1

# ------------------------------------------------- Gate D: derive the pinned CPUs
# A COMPLETE SMT sibling group, read from sysfs. Correct on any topology.
CPUSET=""; CPU_TOPO_NOTE=""
for c in /sys/devices/system/cpu/cpu[0-9]*; do
  f="$c/topology/thread_siblings_list"
  [ -r "$f" ] || continue
  sibs=$(tr -d '[:space:]' < "$f")
  [ -n "$sibs" ] || continue
  # Accept only a comma list (a range like 0-1 is expanded); every member must be
  # online, and the group must be complete -- i.e. every sibling's own list agrees.
  case "$sibs" in *-*) sibs=$(python3 - "$sibs" <<'PY' 2>/dev/null
import sys
out=[]
for part in sys.argv[1].split(','):
    if '-' in part:
        a,b=part.split('-'); out += [str(x) for x in range(int(a),int(b)+1)]
    else: out.append(part)
print(','.join(out))
PY
) ;; esac
  [ -n "$sibs" ] || continue
  ok=1
  for m in ${sibs//,/ }; do
    [ -r "/sys/devices/system/cpu/cpu$m/topology/thread_siblings_list" ] || { ok=0; break; }
  done
  [ "$ok" = 1 ] || continue
  CPUSET="$sibs"; CPU_TOPO_NOTE="derived from cpu${c##*cpu}/topology/thread_siblings_list"
  break
done
if [ -z "$CPUSET" ]; then
  note_fail "Gate D: could not derive a complete SMT sibling group from sysfs — refusing to guess a CPU set (the #3217 hardcoded-core-table defect)"
  CPUSET_OK=0
else
  CPUSET_OK=1
fi
{ echo; echo "== Gate D: pinned CPU set =="
  echo "cpuset: ${CPUSET:-<UNDERIVED>}  (${CPU_TOPO_NOTE:-no source})"
  echo "NOTE: this is ONE complete sibling group, enough for the capability"
  echo "differential. #3224's measurement GEOMETRY needs 6 complete cores for the"
  echo "server plus 2 for the client, EXCLUSIVELY; that is a separate requirement"
  echo "this probe reports but cannot satisfy on a shared box."
  echo "physical-core count: $(lscpu | awk -F: '/^Core\(s\) per socket/{gsub(/ /,"",$2);c=$2} /^Socket\(s\)/{gsub(/ /,"",$2);s=$2} END{print (c*s)?c*s:"unknown"}')"
} >> "$D/capability-probe.txt" 2>&1

# ----------------------------------------------------- Gate A: TMA level-1 and -2
# Each command's status recorded AND its output classified. A non-zero exit with a
# recognised absent-event diagnostic is a CAPABILITY ANSWER; anything else is an
# operational error that must fail the run. Revision 3 ignored every status here
# and its non-empty-file check was vacuous, because the block wrote its own
# headings. (job 309 f2.)
: > "$D/tma-probe.txt"
TMA_L1=absent; TMA_L2=absent; TMA_ANY_ERROR=0
tma_probe() { # $1 label  rest: perf args
  local label="$1"; shift
  local out rc cls
  out=$("$@" 2>&1); rc=$?
  if [ $rc -eq 0 ]; then cls=RESOLVED
  elif grep -qE 'Bad event|Unable to find|No supported events|not supported|Invalid argument|Cannot find metric' <<<"$out"; then
    cls=ABSENT
  else
    cls="ERROR"; TMA_ANY_ERROR=1
    note_fail "Gate A: '$label' failed operationally (rc=$rc) — TMA capability NOT measured"
  fi
  { echo "== $label =="; echo "$out"; echo "[rc=$rc class=$cls]"; echo; } >> "$D/tma-probe.txt"
  echo "$cls"
}
TMA_L1=$(tma_probe "perf stat -M TopdownL1" perf stat -M TopdownL1 -- true)
TMA_L2=$(tma_probe "perf stat -M TopdownL2" perf stat -M TopdownL2 -- true)
for e in topdown.slots slots topdown-retiring topdown-fe-bound topdown-be-bound topdown-bad-spec; do
  tma_probe "event $e" perf stat -e "$e" -- true >/dev/null
done
[ -s "$D/tma-probe.txt" ] || note_fail "Gate A: tma-probe.txt is empty"

# ------------------------------------------------------ per-event disposition
# Four-valued. PROGRAMS is deliberately NOT called SUPPORTED: programming a
# counter and measuring with it are different facts, which is this host's lesson.
declare -A DISP
probe_event() { # $1 event -> sets DISP[$1]
  local e="$1" out rc val
  out=$(perf stat -e "$e" -- true 2>&1); rc=$?
  val=$(awk -v ev="$e" '$2==ev{print $1}' <<<"$out" | head -1)
  if   grep -q 'Bad event\|Unable to find\|No supported events' <<<"$out"; then DISP[$e]=ABSENT-FROM-PMU
  elif grep -q '<not supported>' <<<"$out"; then DISP[$e]=NOT-SUPPORTED
  elif grep -q '<not counted>'   <<<"$out"; then DISP[$e]=NOT-COUNTED
  elif [ $rc -ne 0 ]; then
    DISP[$e]="ERROR(rc=$rc)"
    note_fail "event triage: '$e' failed operationally (rc=$rc); an operational error is not a capability answer"
  elif ! [[ "$val" =~ ^[0-9]+$ ]]; then
    DISP[$e]="ERROR(no numeric value)"
    note_fail "event triage: '$e' exited 0 but produced no numeric count"
  else DISP[$e]="PROGRAMS"; fi
}
CANDIDATES=(cycles instructions
  cycle_activity.stalls_total cycle_activity.stalls_l2_miss cycle_activity.stalls_l3_miss
  offcore_requests_outstanding.all_data_rd offcore_requests_outstanding.cycles_with_data_rd
  offcore_requests.all_data_rd offcore_requests_buffer.sq_full
  l1d_pend_miss.pending l1d_pend_miss.fb_full l1d_pend_miss.l2_stall
  idq_uops_not_delivered.core int_misc.recovery_cycles exe_activity.bound_on_stores
  cycle_activity.stalls_mem_any topdown.slots
  LLC-loads LLC-load-misses cache-references cache-misses)
{ printf '%-52s %s\n' EVENT DISPOSITION
  for e in "${CANDIDATES[@]}"; do probe_event "$e"; printf '%-52s %s\n' "$e" "${DISP[$e]}"; done
} > "$D/event-disposition.txt" 2>&1
[ -s "$D/event-disposition.txt" ] || note_fail "event disposition is empty"

# ------------------------------------------- counter-semantics (AC4, #3224 5.2)
: > "$D/counter-semantics-verification.txt"
PLD=$(perf list --details 2>/dev/null); PLD_RC=$?
if [ $PLD_RC -ne 0 ] || [ -z "$PLD" ]; then
  echo "[perf list --details unavailable (rc=$PLD_RC) — semantics NOT verified]" >> "$D/counter-semantics-verification.txt"
  note_fail "counter semantics: 'perf list --details' produced nothing (rc=$PLD_RC)"
else
  # The previous `[ -s ]` check was VACUOUS: a heading is written per event whether
  # or not a definition is found. Each event is now explicitly FOUND or NOT-LISTED.
  for e in cycle_activity.stalls_l3_miss cycle_activity.stalls_l2_miss \
           cycle_activity.stalls_total offcore_requests_outstanding.all_data_rd \
           LLC-load-misses cache-references; do
    { echo "== $e =="
      if def=$(grep -A3 -E "^  ${e}\$" <<<"$PLD") && [ -n "$def" ]; then echo "$def"; echo "[semantics: FOUND]"
      else echo "[semantics: NOT-LISTED on this host — no definition to verify against]"; fi
      echo; } >> "$D/counter-semantics-verification.txt"
  done
fi

# --------------------------------------------------------------- the differential
if ! cc -O2 -std=c99 -pthread -o "$OUT/cache-hostile.bin" "$SRC" -lm 2>"$D/cache-hostile-build.txt"; then
  echo "VERDICT: UNMEASURED (cache-hostile.c did not build; see host/cache-hostile-build.txt)" | tee "$D/differential.txt" >&2
  exit 2
fi
CH="$OUT/cache-hostile.bin"; CTL="$OUT/.ctl.fifo"; ACK="$OUT/.ack.fifo"

# Small groups, each expected at 100.00% enabled. Built ONLY from events whose
# disposition is PROGRAMS, so a host missing LLC-* still runs the rest and a host
# that HAS them evaluates Gate C properly. Excluded events are named, never
# silently dropped.
avail() { [ "${DISP[$1]:-}" = PROGRAMS ]; }
GROUP_NAMES=(); GROUP_EVENTS=(); GROUP_RENDER=(); EXCLUDED=()
add_group() { # $1 name  rest: symbolic events
  local name="$1"; shift
  local ev=() rn=()
  for e in "$@"; do
    if avail "$e"; then ev+=("$e:u"); rn+=("$e:u"); else EXCLUDED+=("$name:$e=${DISP[$e]:-unprobed}"); fi
  done
  [ ${#ev[@]} -gt 0 ] || { EXCLUDED+=("$name:GROUP-EMPTY"); return; }
  GROUP_NAMES+=("$name")
  GROUP_EVENTS+=("$(IFS=,; echo "${ev[*]}")")
  GROUP_RENDER+=("${rn[*]}")
}
add_group control cycles instructions
add_group stalls  cycle_activity.stalls_total cycle_activity.stalls_l2_miss cycle_activity.stalls_l3_miss
add_group offcore offcore_requests_outstanding.all_data_rd offcore_requests_outstanding.cycles_with_data_rd
add_group cache   LLC-loads LLC-load-misses cache-references cache-misses
add_group prefetch l1d_pend_miss.pending l1d_pend_miss.fb_full

run_arm() { # $1 events  $2 label  $3 buf-mib  $4 work-kib  $5 accesses  $6 outbase
  rm -f "$CTL" "$ACK"; mkfifo "$CTL" "$ACK" || { note_fail "mkfifo for $2"; return 90; }
  taskset -c "$CPUSET" timeout 900 \
    perf stat -x, -e "$1" -D -1 --control "fifo:$CTL,$ACK" -o "$6.csv" -- \
      "$CH" chase --buffer-mib "$3" --working-kib "$4" --accesses "$5" \
                  --arm "$2" --ctl-fifo "$CTL" --ack-fifo "$ACK" > "$6.txt" 2>&1
  local rc=$?; rm -f "$CTL" "$ACK"; echo "arm-rc: $2 = $rc" >> "$6.txt"
  [ $rc -eq 0 ] || note_fail "arm $2 (rc=$rc, see $6.txt)"
  return $rc
}
csv_val() { awk -F, -v e="$2" '$3==e{print $1; exit}' "$1"; }
csv_validate() { # $1 csv  $2 label  rest: rendered names — 100.00% is REQUIRED
  local csv="$1" label="$2"; shift 2
  [ -s "$csv" ] || { note_fail "$label: CSV missing/empty"; return 1; }
  local ev line c en bad=0
  for ev in "$@"; do
    line=$(awk -F, -v e="$ev" '$3==e{print; exit}' "$csv")
    [ -n "$line" ] || { note_fail "$label: no CSV row for '$ev'"; bad=1; continue; }
    c=$(cut -d, -f1 <<<"$line"); en=$(cut -d, -f5 <<<"$line")
    [[ "$c"  =~ ^[0-9]+$ ]] || { note_fail "$label: '$ev' count not numeric ('$c')"; bad=1; }
    [[ "$en" =~ ^[0-9]+(\.[0-9]+)?$ ]] || { note_fail "$label: '$ev' enabled% not numeric ('$en')"; bad=1; continue; }
    awk -v x="$en" 'BEGIN{exit !(x>=99.999)}' || {
      note_fail "$label: '$ev' enabled%=$en — these groups are small BECAUSE every value must be a count, not a scaled estimate"; bad=1; }
  done
  return $bad
}

GATE_OK=1; GATE_RATIO=n/a
[ "$CPUSET_OK" = 1 ] || GATE_OK=0
# Window closure, MEASURED: same chase at 1e3 and 1e5 accesses must scale. A closed
# window scales with accesses (~100x); a large constant term (init+teardown, ~244M
# instructions measured here) collapses the ratio toward 1. Host-independent by
# construction -- deliberately not #3224's tuned 1e6 ceiling, which would
# false-FAIL elsewhere. Positive control: host/gate-guard-positive-control.txt.
CTRL_IDX=-1
for i in "${!GROUP_NAMES[@]}"; do [ "${GROUP_NAMES[$i]}" = control ] && CTRL_IDX=$i; done
if [ "$CTRL_IDX" -lt 0 ]; then
  GATE_OK=0; note_fail "the control group (cycles+instructions) is unavailable — nothing can be validated"
fi
if [ "$GATE_OK" = 1 ]; then
  for n in 1000 100000; do
    run_arm "${GROUP_EVENTS[$CTRL_IDX]}" "gate-probe-$n" 512 0 "$n" "$D/gate-probe-$n" || GATE_OK=0
    csv_validate "$D/gate-probe-$n.csv" "gate-probe-$n" ${GROUP_RENDER[$CTRL_IDX]} || GATE_OK=0
  done
fi
if [ "$GATE_OK" = 1 ]; then
  I1=$(csv_val "$D/gate-probe-1000.csv" instructions:u)
  I2=$(csv_val "$D/gate-probe-100000.csv" instructions:u)
  if [[ "$I1" =~ ^[0-9]+$ ]] && [[ "$I2" =~ ^[0-9]+$ ]] && [ "$I1" -gt 0 ]; then
    GATE_RATIO=$(awk -v a="$I1" -v b="$I2" 'BEGIN{printf "%.2f", b/a}')
    awk -v a="$I1" -v b="$I2" 'BEGIN{exit !(b >= 10*a)}' || { GATE_OK=0
      note_fail "WINDOW NOT GATED: instructions $I1 (1e3) -> $I2 (1e5), ratio ${GATE_RATIO}x. A window closed on the chase scales ~100x; near 1x means init/teardown is inside it, which is asymmetric between arms and corrupts every ratio."; }
  else
    GATE_OK=0; note_fail "gate probe: unreadable instruction counts — window closure UNMEASURED"
  fi
fi

ARMS=(friendly-L2resident:512:256:20000000 hostile-512m:512:0:20000000 hostile-2g:2048:0:8000000)
if [ "$GATE_OK" = 1 ]; then
  for spec in "${ARMS[@]}"; do
    IFS=: read -r arm buf work acc <<<"$spec"
    for i in "${!GROUP_NAMES[@]}"; do
      g=${GROUP_NAMES[$i]}
      run_arm "${GROUP_EVENTS[$i]}" "$arm-$g" "$buf" "$work" "$acc" "$D/arm-$arm-$g"
      csv_validate "$D/arm-$arm-$g.csv" "arm $arm/$g" ${GROUP_RENDER[$i]}
    done
  done
else
  note_fail "measurement arms SKIPPED: window closure or CPU derivation not established, so no capability claim could rest on them"
fi
rm -f "$CH" "$CTL" "$ACK"

# --------------------------------------------------------------- gate verdicts
# Each Gate C / Gate B counter is classified from the MEASURED arms:
#   ABSENT  - not on this PMU at all (a legitimate capability answer)
#   STUCK   - programs and reads 0 in BOTH arms, i.e. a silent instrument
#   MOVING  - reads nonzero and rises with the working set: usable
classify() { # $1 rendered-name  $2 group
  local n="$1" g="$2" f h
  f=$(csv_val "$D/arm-friendly-L2resident-$g.csv" "$n" 2>/dev/null)
  h=$(csv_val "$D/arm-hostile-512m-$g.csv"        "$n" 2>/dev/null)
  if ! [[ "$f" =~ ^[0-9]+$ ]] || ! [[ "$h" =~ ^[0-9]+$ ]]; then echo "ABSENT/UNMEASURED"; return; fi
  if [ "$f" -eq 0 ] && [ "$h" -eq 0 ]; then echo "STUCK (0 in both arms)"; return; fi
  if [ "$h" -gt "$f" ]; then echo "MOVING ($f -> $h)"; else echo "NOT-MOVING ($f -> $h)"; fi
}
{
  echo "== #3287 capability probe — gate verdicts =="
  echo "host L3: $(lscpu | awk -F: '/L3 cache/{gsub(/^ +/,"",$2);print $2}')"
  echo "cpuset:  ${CPUSET:-<UNDERIVED>} (${CPU_TOPO_NOTE:-no source})"
  echo "window:  control-FIFO gated to the chase; events counted :u; small groups, 100.00% enabled REQUIRED"
  echo "gate-integrity: $([ "$GATE_OK" = 1 ] && echo MEASURED-OK || echo FAILED) — instruction scaling 1e3->1e5 = ${GATE_RATIO}x (need >=10x, ideal ~100x)"
  if [ -s "$D/gate-probe-1000.csv" ]; then
    echo "  absolute, REPORTED not asserted: $(csv_val "$D/gate-probe-1000.csv" instructions:u) instructions for 1e3 accesses"
    echo "  (#3224 used a tuned 1e6 ceiling; the scaling property is host-independent.)"
  fi
  echo
  echo "-- GATE A: TMA --"
  echo "  perf stat -M TopdownL1 : $TMA_L1"
  echo "  perf stat -M TopdownL2 : $TMA_L2   <-- #3287 AC1 needs this RESOLVED"
  echo "  (per-event detail in tma-probe.txt; ABSENT is a capability ANSWER, ERROR is not)"
  echo
  echo "-- GATE B: offcore / prefetch-stall term (the one #3287 exists for) --"
  for n in offcore_requests_outstanding.all_data_rd:u offcore_requests_outstanding.cycles_with_data_rd:u; do
    echo "  $n : $(classify "$n" offcore)"
  done
  for e in offcore_requests.all_data_rd offcore_requests_buffer.sq_full; do
    echo "  $e : ${DISP[$e]:-unprobed} (disposition only; not in a differential group)"
  done
  echo
  echo "-- GATE C: reproduce #3224's own baseline counters --"
  for n in cycle_activity.stalls_l3_miss:u cycle_activity.stalls_l2_miss:u cycle_activity.stalls_total:u; do
    echo "  $n : $(classify "$n" stalls)"
  done
  for n in LLC-loads:u LLC-load-misses:u cache-references:u cache-misses:u; do
    echo "  $n : $(classify "$n" cache)"
  done
  echo "  NESTING (stalls_l3_miss <= stalls_l2_miss <= stalls_total), per arm:"
  echo "    DECLARED LIMIT: nesting HOLDS TRIVIALLY when stalls_l3_miss is STUCK at 0,"
  echo "    so a HOLDS here is NOT evidence that counter works — read its line above."
  echo "    This check catches a nesting VIOLATION (which would invalidate #3224's"
  echo "    difference-based partition); it cannot catch a silent zero. Only the"
  echo "    differential can, which is why both are reported."
  for arm in friendly-L2resident hostile-512m hostile-2g; do
    c3=$(csv_val "$D/arm-$arm-stalls.csv" cycle_activity.stalls_l3_miss:u 2>/dev/null)
    c2=$(csv_val "$D/arm-$arm-stalls.csv" cycle_activity.stalls_l2_miss:u 2>/dev/null)
    ct=$(csv_val "$D/arm-$arm-stalls.csv" cycle_activity.stalls_total:u 2>/dev/null)
    if [[ "$c3" =~ ^[0-9]+$ ]] && [[ "$c2" =~ ^[0-9]+$ ]] && [[ "$ct" =~ ^[0-9]+$ ]]; then
      if [ "$c3" -le "$c2" ] && [ "$c2" -le "$ct" ]; then echo "    $arm: HOLDS ($c3 <= $c2 <= $ct)"
      else echo "    $arm: VIOLATED ($c3 / $c2 / $ct) — #3224's partition is a DIFFERENCE of these, so a violation invalidates it"
        note_fail "Gate C: stall-counter nesting violated on $arm ($c3 / $c2 / $ct)"; fi
    else echo "    $arm: UNMEASURED (stall group unavailable)"; fi
  done
  echo
  echo "-- GATE D: topology (reported; #3224's geometry needs 6+2 complete cores EXCLUSIVELY) --"
  echo "  see capability-probe.txt"
  echo
  if [ ${#EXCLUDED[@]} -gt 0 ]; then
    echo "-- events EXCLUDED from the differential, and why (never silently dropped) --"
    for x in "${EXCLUDED[@]}"; do echo "  $x"; done
    echo
  fi
  echo "Prediction, written BEFORE measuring: the 2 GiB arm's working set is many"
  echo "times L3 and is a random single-dependency chase, so it cannot be L3-resident."
  echo "Any honest L3-miss-stall or offcore counter MUST be large there. A zero is a"
  echo "silent instrument -- and a zero is invariant to the window, since"
  echo "contamination can only ADD counts."
  echo
  if [ "$FAILED" = 0 ]; then
    echo "VERDICT: COMPLETE (every step measured; the gate lines above are the answer)"
  else
    echo "VERDICT: UNMEASURED (at least one step failed; see PROBE-STEP-FAILED on stderr"
    echo "         and the [rc=]/arm-rc: lines in host/*.txt). No capability claim may"
    echo "         be derived from this run."
  fi
} > "$D/differential.txt" 2>&1

if [ "$FAILED" = 0 ]; then echo "capability probe COMPLETE -> $D/"; exit 0; fi
echo "capability probe INCOMPLETE -> $D/ (see VERDICT in $D/differential.txt)" >&2
exit 1

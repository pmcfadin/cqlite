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
#     failure stamps VERDICT: UNMEASURED and exits non-zero. That rule governs the
#     DATA-INTEGRITY guards, which is now the only thing this script asserts --
#     see the SCOPE block below.
#
# (vi) SCOPE OF THIS SCRIPT'S CLAIM -- IT RECORDS, IT DOES NOT CLASSIFY (#3870).
#     An earlier revision carried an auto-classifier: Gate A decided
#     RESOLVED/ABSENT for the TMA metric groups, and a classify() decided
#     ABSENT/STUCK/MOVING per counter from the arms. That layer is REMOVED by lead
#     ruling on #3287 (request REQ-3287-20260901T195930Z, option (a)) and is
#     tracked in #3870. Five review rounds put 17 findings in it, each round's
#     High-severity ones inside the previous round's fix code, and two were still
#     open at descope: Gate A read ANY digit anywhere in perf's output as proof
#     TMA had resolved (a live fail-open), and classify() called 100 -> 101
#     'MOVING', certifying a counter on noise.
#
#     What the script does now: it RUNS each step, RECORDS perf's raw output and
#     exit status verbatim, VALIDATES the data (window closure, 100.00% enabled,
#     <not counted>, stale-CSV purge, stall nesting, CPU affinity), and PRINTS the
#     per-arm counts beside each event's disposition. It draws no capability
#     conclusion. VERDICT: COMPLETE therefore means 'every step executed and every
#     data-integrity guard passed' -- it is NOT a statement that this host can
#     serve #3287, and must never be read as one. The reader answers Gates A-D
#     from the recorded numbers; negative-control-c7i-guest.md is that reading for
#     this host.
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

# STALE MEASUREMENTS FROM A PREVIOUS RUN -- POSSIBLY FROM A DIFFERENT HOST -- MUST
# NOT SURVIVE INTO THIS RUN'S VERDICT. If a group is unavailable this time, no new
# CSV is written for it, and the classifier would happily read the old file and
# report another machine's numbers beside this machine's inventory under a
# COMPLETE verdict. Since this probe's whole purpose is to be re-run on a
# CANDIDATE host, that is the worst possible failure here. Every generated
# measurement file is therefore removed up front; the hand-written
# gate-guard-positive-control.txt is deliberately NOT matched by these globs.
# (#3287 roborev job 312, finding 2.)
rm -f "$D"/arm-*.csv "$D"/arm-*.txt "$D"/gate-probe-*.csv "$D"/gate-probe-*.txt \
      "$D"/differential.txt "$D"/tma-probe.txt "$D"/event-disposition.txt \
      "$D"/counter-semantics-verification.txt "$D"/capability-probe.txt \
      "$D"/cache-hostile-build.txt

FAILED=0
# The operator-facing diagnostic must reach the OPERATOR. Several blocks below
# produce an artefact with `{ ...; } > file 2>&1`, and that `2>&1` swallowed every
# note_fail raised inside them INTO the artefact -- so a `<not counted>` triage
# failure landed in event-disposition.txt and a nesting violation landed in
# differential.txt, while the verdict text told the reader to "see
# PROBE-STEP-FAILED on stderr", where it was not. Not fail-OPEN (FAILED was still
# set and the run still exited non-zero), but a verdict that points at a place the
# cause is not is worse than one that says nothing. fd 9 is the real stderr,
# saved before any block redirection, so note_fail is immune to them.
exec 9>&2
note_fail() { FAILED=1; echo "PROBE-STEP-FAILED: $*" >&9; }

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
CPUSET=""; CPU_TOPO_NOTE=""; CPUSET_OK=0

# A COMPLETE, ONLINE, PROCESS-ALLOWED SMT sibling group, read from sysfs.
# Revision 5 claimed to validate a complete group and only checked that each
# member's topology FILE WAS READABLE -- it compared no sibling lists, and checked
# neither online status nor the process's own affinity mask, so it could select an
# offline or cgroup-forbidden CPU and report the group as complete. On this fleet
# every lane runs under a restricted affinity mask, so that is not hypothetical.
# (#3287 roborev job 313, finding 3.)
expand_list() { # "0-3,8" -> "0 1 2 3 8"
  local out="" part a b
  local IFS=,
  for part in $1; do
    case "$part" in
      *-*) a=${part%-*}; b=${part#*-}
           case "$a$b" in *[!0-9]*) return 1;; esac
           while [ "$a" -le "$b" ]; do out="$out $a"; a=$((a+1)); done ;;
      '')  ;;
      *)   case "$part" in *[!0-9]*) return 1;; esac; out="$out $part" ;;
    esac
  done
  echo $out
}
set_eq() { # two space-separated lists, order-insensitive
  local x y
  x=$(printf '%s\n' $1 | sort -n | tr '\n' ' ')
  y=$(printf '%s\n' $2 | sort -n | tr '\n' ' ')
  [ "$x" = "$y" ]
}
ONLINE=$(expand_list "$(cat /sys/devices/system/cpu/online 2>/dev/null)" 2>/dev/null) || ONLINE=""
# The process's OWN allowed set. Without this a pinned taskset either fails or is
# silently widened by the kernel, and the L2-resident control arm can migrate.
ALLOWED=$(expand_list "$(awk '/^Cpus_allowed_list:/{print $2}' /proc/self/status 2>/dev/null)" 2>/dev/null) || ALLOWED=""
in_list() { local n="$1" l="$2" m; for m in $l; do [ "$m" = "$n" ] && return 0; done; return 1; }

if [ -z "$ONLINE" ] || [ -z "$ALLOWED" ]; then
  note_fail "Gate D: could not read the online CPU set and/or this process's allowed CPU set — refusing to pin blind"
else
  for c in /sys/devices/system/cpu/cpu[0-9]*; do
    f="$c/topology/thread_siblings_list"; [ -r "$f" ] || continue
    grp=$(expand_list "$(cat "$f" 2>/dev/null)" 2>/dev/null) || continue
    [ -n "$grp" ] || continue
    ok=1
    for m in $grp; do
      # every member must be ONLINE, ALLOWED, and agree on the group membership
      in_list "$m" "$ONLINE"  || { ok=0; break; }
      in_list "$m" "$ALLOWED" || { ok=0; break; }
      mf="/sys/devices/system/cpu/cpu$m/topology/thread_siblings_list"
      [ -r "$mf" ] || { ok=0; break; }
      mgrp=$(expand_list "$(cat "$mf" 2>/dev/null)" 2>/dev/null) || { ok=0; break; }
      set_eq "$mgrp" "$grp" || { ok=0; break; }
    done
    [ "$ok" = 1 ] || continue
    CPUSET=$(printf '%s\n' $grp | sort -n | paste -sd,)
    CPU_TOPO_NOTE="complete sibling group from cpu${c##*cpu}, all members online AND in this process's Cpus_allowed_list"
    CPUSET_OK=1
    break
  done
  [ "$CPUSET_OK" = 1 ] || note_fail "Gate D: no COMPLETE sibling group is both online and permitted by this process's affinity mask — refusing to guess a CPU set (the #3217 hardcoded-core-table defect)"
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
# RECORDED, NOT CLASSIFIED (#3870, see SCOPE above). Each command's exit status and
# its output are written verbatim to tma-probe.txt; this script decides nothing
# about what they mean. The removed classifier is why: 'rc=0 and a digit appears
# somewhere' is not a measurement of TMA availability, and an exit status is not a
# capability answer in either direction.
#
# Nothing here calls note_fail, and that is deliberate rather than an omission: a
# failure verdict would BE a classification of perf's exit status, which is the
# layer that was removed. A perf that is broken rather than merely lacking these
# metrics is caught by the event-disposition sweep below, which is retained and
# does fail closed -- so removing this layer opens no unguarded route.
: > "$D/tma-probe.txt"
TMA_L1=UNRECORDED; TMA_L2=UNRECORDED

# The calling convention is NOT $( ). Revision 4 used command substitution, which
# runs the function in a SUBSHELL: any state it set died with the child. The
# summary line is returned through a global. (#3287 roborev job 312, finding 1.)
TMA_RECORD=""
tma_probe() { # $1 label  rest: perf args   -> sets TMA_RECORD
  local label="$1"; shift
  local out rc lines
  out=$("$@" 2>&1); rc=$?
  lines=$(printf '%s' "$out" | grep -c '' 2>/dev/null || echo 0)
  # A factual descriptor only: what ran, what it returned, how much it said, and
  # where to read it. No verdict token appears in this string.
  TMA_RECORD="rc=$rc, ${lines} line(s) recorded verbatim in tma-probe.txt (this probe does not interpret it)"
  { echo "== $label =="; echo "$out"; echo "[rc=$rc]"; echo; } >> "$D/tma-probe.txt"
}
tma_probe "perf stat -M TopdownL1" perf stat -M TopdownL1 -- true; TMA_L1="$TMA_RECORD"
tma_probe "perf stat -M TopdownL2" perf stat -M TopdownL2 -- true; TMA_L2="$TMA_RECORD"
# The individual topdown EVENTS go through the retained disposition sweep instead
# of this recorder, so they get the four-valued answer that layer provides.
for e in slots topdown-retiring topdown-fe-bound topdown-be-bound topdown-bad-spec; do
  tma_probe "event $e" perf stat -e "$e" -- true
done
[ -s "$D/tma-probe.txt" ] || note_fail "Gate A: tma-probe.txt is empty — the step did not record its output"

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
  elif grep -q '<not counted>'   <<<"$out"; then
    # '<not counted>' does NOT establish absence: the counter was programmed and
    # the kernel never scheduled it (contention, a too-short workload, a
    # multiplexing accident). Treating it as a capability disposition let the
    # event be silently excluded while the probe still reported COMPLETE.
    # (job 312, finding 3.)
    DISP[$e]="NOT-COUNTED (measurement failed — NOT evidence of absence)"
    note_fail "event triage: '$e' returned <not counted>; the counter was never scheduled, so its capability is UNKNOWN — re-run on an idle host"

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
# Some perf/PMU combinations STRIP the modifier from the reported event name, so a
# lookup keyed strictly on 'name:u' would reject a perfectly good capture as
# missing -- a false FAIL on correct input, which is the guard agents learn to
# waive. Match the rendered name first, then its modifier-stripped base.
# (job 312, finding 4.)
csv_val() { # $1 csv  $2 rendered name
  local v
  v=$(awk -F, -v e="$2" '$3==e{print $1; exit}' "$1" 2>/dev/null)
  if [ -z "$v" ]; then v=$(awk -F, -v e="${2%:u}" '$3==e{print $1; exit}' "$1" 2>/dev/null); fi
  echo "$v"
}
csv_row() { # $1 csv  $2 rendered name
  local r
  r=$(awk -F, -v e="$2" '$3==e{print; exit}' "$1" 2>/dev/null)
  if [ -z "$r" ]; then r=$(awk -F, -v e="${2%:u}" '$3==e{print; exit}' "$1" 2>/dev/null); fi
  echo "$r"
}
csv_validate() { # $1 csv  $2 label  rest: rendered names — 100.00% is REQUIRED
  local csv="$1" label="$2"; shift 2
  [ -s "$csv" ] || { note_fail "$label: CSV missing/empty"; return 1; }
  local ev line c en bad=0
  for ev in "$@"; do
    line=$(csv_row "$csv" "$ev")
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

# ------------------------------------------------------------- recorded readings
# RECORDED, NOT CLASSIFIED (#3870, see SCOPE at the top). The removed classify()
# printed ABSENT / STUCK / MOVING / NOT-MOVING per counter. It is replaced by a
# reporter that prints the same underlying facts and asserts nothing about them:
# the event's disposition from the retained sweep, and its count in each arm as
# validated from that arm's CSV. "0 in both arms while the 2 GiB arm is many times
# L3" is a conclusion for the reader to draw from these numbers, and drawing it is
# what negative-control-c7i-guest.md does.
#
# A missing value still distinguishes its two causes, because they are different
# FACTS about the run rather than judgements of it: the event was excluded up front
# (this PMU does not have it) or the arm should have produced a value and did not
# (a measurement failure, already counted by csv_validate).
reading() { # $1 rendered-name  $2 group  -> raw per-arm counts, no verdict
  local n="$1" g="$2" base="${n%:u}" f h h2
  f=$(csv_val  "$D/arm-friendly-L2resident-$g.csv" "$n" 2>/dev/null)
  h=$(csv_val  "$D/arm-hostile-512m-$g.csv"        "$n" 2>/dev/null)
  h2=$(csv_val "$D/arm-hostile-2g-$g.csv"          "$n" 2>/dev/null)
  [[ "$f"  =~ ^[0-9]+$ ]] || f="(no value in this run's CSV)"
  [[ "$h"  =~ ^[0-9]+$ ]] || h="(no value in this run's CSV)"
  [[ "$h2" =~ ^[0-9]+$ ]] || h2="(no value in this run's CSV)"
  printf 'disposition=%s  friendly-L2resident=%s  hostile-512m=%s  hostile-2g=%s' \
    "${DISP[$base]:-unprobed}" "$f" "$h" "$h2"
}
{
  echo "== #3287 capability probe — RECORDED READINGS =="
  echo "SCOPE: this probe RECORDS and VALIDATES; it does not classify (#3870)."
  echo "       No line below is a capability verdict. Gates A-D are answered by"
  echo "       READING these numbers — see negative-control-c7i-guest.md for this"
  echo "       host's reading. VERDICT at the foot covers data integrity ONLY."
  echo "host L3: $(lscpu | awk -F: '/L3 cache/{gsub(/^ +/,"",$2);print $2}')"
  echo "cpuset:  ${CPUSET:-<UNDERIVED>} (${CPU_TOPO_NOTE:-no source})"
  echo "window:  control-FIFO gated to the chase; events counted :u; small groups, 100.00% enabled REQUIRED"
  echo "gate-integrity: $([ "$GATE_OK" = 1 ] && echo MEASURED-OK || echo FAILED) — instruction scaling 1e3->1e5 = ${GATE_RATIO}x (need >=10x, ideal ~100x)"
  if [ -s "$D/gate-probe-1000.csv" ]; then
    echo "  absolute, REPORTED not asserted: $(csv_val "$D/gate-probe-1000.csv" instructions:u) instructions for 1e3 accesses"
    echo "  (#3224 used a tuned 1e6 ceiling; the scaling property is host-independent.)"
  fi
  echo
  echo "-- GATE A: TMA (raw record; #3287 AC1 turns on what tma-probe.txt shows) --"
  echo "  perf stat -M TopdownL1 : $TMA_L1"
  echo "  perf stat -M TopdownL2 : $TMA_L2"
  echo "  READ tma-probe.txt: a metric group that produced real level-2 metric rows"
  echo "  with numeric shares answers AC1 yes; a not-found/not-supported diagnostic"
  echo "  answers it no; anything else means the step must be re-run. This script"
  echo "  deliberately does not make that call (#3870)."
  echo
  echo "-- GATE B: offcore / prefetch-stall term (the one #3287 exists for) --"
  echo "  (counts as measured; the 2 GiB arm is many times L3, so read a 0 there)"
  for n in offcore_requests_outstanding.all_data_rd:u offcore_requests_outstanding.cycles_with_data_rd:u; do
    echo "  $n : $(reading "$n" offcore)"
  done
  for e in offcore_requests.all_data_rd offcore_requests_buffer.sq_full; do
    echo "  $e : ${DISP[$e]:-unprobed} (disposition only; not in a differential group)"
  done
  # The prefetch group was being MEASURED and VALIDATED and then left out of this
  # verdict, even though the resume note names l1d_pend_miss.fb_full as a required
  # Gate B signal -- the fill-buffer half of the prefetch-pressure story, and the
  # one Gate B signal that actually works on a guest. (job 312, finding 5.)
  for n in l1d_pend_miss.fb_full:u l1d_pend_miss.pending:u; do
    echo "  $n : $(reading "$n" prefetch)"
  done
  echo
  echo "-- GATE C: reproduce #3224's own baseline counters --"
  for n in cycle_activity.stalls_l3_miss:u cycle_activity.stalls_l2_miss:u cycle_activity.stalls_total:u; do
    echo "  $n : $(reading "$n" stalls)"
  done
  for n in LLC-loads:u LLC-load-misses:u cache-references:u cache-misses:u; do
    echo "  $n : $(reading "$n" cache)"
  done
  echo "  NESTING (stalls_l3_miss <= stalls_l2_miss <= stalls_total), per arm:"
  echo "    DECLARED LIMIT: nesting HOLDS TRIVIALLY when stalls_l3_miss reads 0 in"
  echo "    every arm, so a HOLDS here is NOT evidence that counter works — read its"
  echo "    recorded counts above."
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
    echo "VERDICT: COMPLETE (data integrity only — every step executed and every"
    echo "         data-integrity guard passed. This is NOT a capability claim and"
    echo "         NOT an answer to any of Gates A-D: this probe records, it does"
    echo "         not classify (#3870). Read the numbers above.)"
  else
    echo "VERDICT: UNMEASURED (at least one step failed; see PROBE-STEP-FAILED on stderr"
    echo "         and the [rc=]/arm-rc: lines in host/*.txt). No capability claim may"
    echo "         be derived from this run."
  fi
} > "$D/differential.txt" 2>&1

if [ "$FAILED" = 0 ]; then echo "capability probe COMPLETE -> $D/"; exit 0; fi
echo "capability probe INCOMPLETE -> $D/ (see VERDICT in $D/differential.txt)" >&2
exit 1

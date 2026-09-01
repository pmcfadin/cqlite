#!/usr/bin/env bash
# =============================================================================
# #3287 — every fail-closed guard in capability-probe.sh, shown REJECTING the bad
# input it exists to catch AND still ACCEPTING good input.
#
#     bash docs/reports/ws0-3287-artifacts/selftest-guards.sh
#
# Runs in seconds. Needs NO perf, NO root, NO PMU and NO bare-metal box: a shim
# `perf` on PATH drives every classification path deterministically. That
# portability is the entire point. Four roborev rounds found 14 defects in that
# probe, and TWO OF THE HIGHS WERE INTRODUCED BY THE ROUND THAT FIXED THE
# PREVIOUS ONES -- because the only way to exercise the script was to run it on a
# host with a working PMU and read the output by eye. A guard whose only entry
# point is a 20-minute exclusive bare-metal run has never been tested.
#
# THE STANDARD, taken from #3224's own selftest-guards.sh: for each guard there
# are TWO cases in tension -- the bad input it must REJECT and the good input it
# must still ACCEPT. One without the other proves nothing. A guard that rejects
# everything is not a guard, and the false-FAIL direction is real: one finding in
# round 4 (job 312, finding 4) was precisely a guard that would have rejected a
# perfectly good capture on any perf that strips modifiers from event names.
#
# Every case asserts a VERDICT plus the NAMED cause. A bare non-zero exit is not
# evidence: the probe can fail for a dozen reasons, so a case that only checked
# the exit code would pass on an unrelated breakage.
# =============================================================================
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE="$HERE/capability-probe.sh"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT INT TERM
PASS=0; FAIL=0; SKIP=0
# A case the HOST cannot express is not a failure and not a deleted case. It is
# counted, so the floor below still detects deletion, and it is reported, so a
# run that skipped something is never mistaken for a run that verified it.
# (roborev job 318: these two topology cases red on a non-SMT or single-vCPU
# host, and a guard that reds on correct input is the guard agents learn to waive.)
skip() { SKIP=$((SKIP+1)); printf '   SKIP  %s\n' "$1"; }
ok()  { PASS=$((PASS+1)); printf '   PASS  %s\n' "$1"; }
# A failing case must say WHY. A test that reports only "FAIL" sends the next
# person back to reconstruct the run by hand, which is where this suite's own
# first two failures went.
bad() { FAIL=$((FAIL+1)); printf '   FAIL  %s\n' "$1"
  local d="${2:-}"
  if [ -n "$d" ]; then
    printf '         verdict: %s\n' "$(grep -E '^VERDICT' "$d/out/host/differential.txt" 2>/dev/null | head -1)"
    sed -n '1,4p' "$d/stderr.txt" 2>/dev/null | sed 's/^/         stderr: /'
  fi; }
[ -r "$PROBE" ] || { echo "cannot read $PROBE"; exit 2; }

# --- the shim -----------------------------------------------------------------
# A fake `perf` whose behaviour is chosen by env vars, plus the handful of other
# tools the probe calls. `cc` produces a stub binary and the chase is emulated by
# the shim writing a CSV, so no real workload runs.
mk_env() { # $1 dir
  local b="$1/bin"; mkdir -p "$b"
  cat > "$b/perf" <<'SHIM'
#!/usr/bin/env bash
# modes: SHIM_TMA=absent|error|zero-no-numbers|resolved
#        SHIM_EVENT_NOTCOUNTED=<event name>       -> that event reports <not counted>
#        SHIM_ENABLED=<pct>                       -> enabled% written into arm CSVs
#        SHIM_UNGATED=1                           -> instruction count is CONSTANT
#        SHIM_STRIP_MOD=1                         -> CSV event names lose ':u'
#        SHIM_NEST_VIOLATE=1                      -> stalls_l3_miss > stalls_total
#        SHIM_KERNEL_DENIED=1                     -> the BARE event form is refused;
#                                                    only the ':u' form is permitted
#        SHIM_ARM_MISLABEL=1                      -> the benchmark reports a buffer
#                                                    size other than the one asked for
args=("$@")
if [[ " ${args[*]} " == *" --version "* ]]; then echo "perf version shim"; exit 0; fi
if [[ " ${args[*]} " == *" list "* ]]; then
  printf '  cycle_activity.stalls_l3_miss\n       [Execution stalls while L3 cache miss demand load is outstanding. Unit: cpu]\n'
  printf '  cycle_activity.stalls_l2_miss\n       [x. Unit: cpu]\n  cycle_activity.stalls_total\n       [x. Unit: cpu]\n'
  printf '  offcore_requests_outstanding.all_data_rd\n       [x. Unit: cpu]\n  LLC-load-misses\n       [x. Unit: cpu]\n  cache-references\n       [x. Unit: cpu]\n'
  exit 0
fi
# -M TopdownL1/L2
for i in "${!args[@]}"; do [ "${args[$i]}" = "-M" ] && M="${args[$((i+1))]}"; done
if [ -n "${M:-}" ]; then
  case "${SHIM_TMA:-absent}" in
    absent) echo "Cannot find metric or group \`$M'" >&2; exit 1 ;;
    error)  echo "some unexpected diagnostic" >&2; exit 7 ;;
    zero-no-numbers) echo ""; exit 0 ;;
    resolved) echo " 42.0 % tma_retiring"; exit 0 ;;
  esac
fi
# find -e <evlist> and -o <outfile>
for i in "${!args[@]}"; do
  [ "${args[$i]}" = "-e" ] && EV="${args[$((i+1))]}"
  [ "${args[$i]}" = "-o" ] && OF="${args[$((i+1))]}"
done
[ -n "${EV:-}" ] || exit 0
# The disposition sweep and the measurement form are told apart by --control, not
# by the presence of -o: since roborev job 318 the sweep passes `-x, -o FILE` too.
IS_MEASURE=0
[[ " ${args[*]} " == *" --control "* ]] && IS_MEASURE=1
# single-event probe form: used by the disposition sweep
if [ "$IS_MEASURE" = 0 ]; then
  BARE="${EV%:u}"
  # A host permitting user-only counting and denying kernel counting: the bare
  # form is refused, the ':u' form -- the one the arms measure -- works. The probe
  # must ask the question it uses the answer for.
  if [ -n "${SHIM_KERNEL_DENIED:-}" ] && [ "$EV" = "$BARE" ]; then
    echo "Error: Access to performance monitoring and observability operations is limited." >&2
    exit 1
  fi
  name="$EV"; [ -n "${SHIM_STRIP_MOD:-}" ] && name="$BARE"
  if [ -n "${SHIM_EVENT_NOTCOUNTED:-}" ] && [ "$BARE" = "$SHIM_EVENT_NOTCOUNTED" ]; then
    [ -n "${OF:-}" ] && printf '<not counted>,,%s,0,0.00,,\n' "$name" > "$OF"
    exit 0
  fi
  case "$BARE" in
    LLC-loads|LLC-load-misses|offcore_requests.all_data_rd|offcore_requests_buffer.sq_full|topdown.slots|slots|topdown-*|cycle_activity.stalls_mem_any)
      echo "event syntax error: Bad event name" >&2; exit 1 ;;
  esac
  [ -n "${OF:-}" ] && printf '1234,,%s,1000000,100.00,,\n' "$name" > "$OF"
  exit 0
fi
# measurement form: RUN the workload, then synthesise a CSV for the requested group.
# Real perf runs the command after `--` and its stdout is what run_arm captures; the
# shim used to skip it entirely, which was invisible until the probe started
# asserting the benchmark's self-reported configuration (roborev job 318).
WLI=-1
for i in "${!args[@]}"; do [ "${args[$i]}" = "--" ] && { WLI=$((i+1)); break; }; done
WL_RC=0
if [ "$WLI" -ge 0 ] && [ -n "${args[$WLI]:-}" ]; then
  "${args[@]:$WLI}" || WL_RC=$?
fi
accesses=1000
for i in "${!args[@]}"; do [ "${args[$i]}" = "--accesses" ] && accesses="${args[$((i+1))]}"; done
work=0
for i in "${!args[@]}"; do [ "${args[$i]}" = "--working-kib" ] && work="${args[$((i+1))]}"; done
en="${SHIM_ENABLED:-100.00}"
: > "$OF"
IFS=, read -ra evs <<<"$EV"
for e in "${evs[@]}"; do
  name="$e"; [ -n "${SHIM_STRIP_MOD:-}" ] && name="${e%:u}"
  case "$e" in
    instructions:u) if [ -n "${SHIM_UNGATED:-}" ]; then v=243571204; else v=$((accesses*6)); fi ;;
    cycles:u)                v=$((accesses*30)) ;;
    cycle_activity.stalls_total:u)   v=$((accesses*20)) ;;
    cycle_activity.stalls_l2_miss:u) if [ "$work" = 0 ]; then v=$((accesses*15)); else v=100; fi ;;
    cycle_activity.stalls_l3_miss:u) if [ -n "${SHIM_NEST_VIOLATE:-}" ]; then v=999999999999; else v=0; fi ;;
    offcore_requests_outstanding*)   v=0 ;;
    cache-misses:u|cache-references:u) v=0 ;;
    l1d_pend_miss.pending:u) if [ "$work" = 0 ]; then v=$((accesses*12)); else v=50; fi ;;
    l1d_pend_miss.fb_full:u) if [ "$work" = 0 ]; then v=$((accesses*3)); else v=10; fi ;;
    *) v=1 ;;
  esac
  printf '%s,,%s,1000000,%s,,\n' "$v" "$name" "$en" >> "$OF"
done
exit $WL_RC
SHIM
  # The stub benchmark REPORTS the configuration it was asked for, exactly as
  # cache-hostile.c does, because since roborev job 318 the probe asserts those
  # fields. SHIM_ARM_MISLABEL makes it report a different buffer size -- the
  # defect the assert exists for: real counts, wrong arm.
  cat > "$b/cc" <<'SHIM'
#!/usr/bin/env bash
out=""; for i in "$@"; do [ "$prev" = "-o" ] && out="$i"; prev="$i"; done
[ -n "$out" ] && { cat > "$out" <<'STUB'
#!/usr/bin/env bash
buf=0; work=0; acc=0; arm=""
while [ $# -gt 0 ]; do
  case "$1" in
    --buffer-mib) buf="$2"; shift 2;; --working-kib) work="$2"; shift 2;;
    --accesses) acc="$2"; shift 2;;   --arm) arm="$2"; shift 2;;
    *) shift;;
  esac
done
bb=$(( buf * 1048576 ))
if [ "$work" -gt 0 ]; then ws=$(( work * 1024 )); else ws=$bb; fi
[ -n "${SHIM_ARM_MISLABEL:-}" ] && bb=$(( bb / 4 ))
printf 'mode=chase\narm=%s\ngate=fifo\nbuffer_bytes=%s\nworking_set_bytes=%s\naccesses=%s\nns_per_access=42.000\ninit_overrun=0\n' \
  "$arm" "$bb" "$ws" "$acc"
exit 0
STUB
chmod +x "$out"; }
exit 0
SHIM
  cat > "$b/taskset" <<'SHIM'
#!/usr/bin/env bash
shift 2   # drop -c <set>
exec "$@"
SHIM
  cat > "$b/numactl" <<'SHIM'
#!/usr/bin/env bash
echo "available: 1 nodes (0)"; exit 0
SHIM
  chmod +x "$b"/*
  echo "$b"
}

run_probe() { # $1 case-dir ; env vars already exported by caller
  local dir="$1"; local bin; bin=$(mk_env "$dir")
  ( export PATH="$bin:$PATH"; bash "$PROBE" "$dir/out" "$HERE/../ws0-3224-artifacts/cache-hostile.c" ) \
    > "$dir/stdout.txt" 2> "$dir/stderr.txt"
  echo $?
}
verdict()  { grep -E '^VERDICT' "$1/out/host/differential.txt" 2>/dev/null | head -1; }
gateline() { grep -E "$2" "$1/out/host/differential.txt" 2>/dev/null | head -1; }

# =============================================================================
echo "== #3287 capability-probe.sh guard selftest =="

# --- 1-2. Gate A after the #3870 descope: RECORD verbatim, CLASSIFY nothing ----
# The classifier these cases used to drive is gone (lead ruling on #3287
# REQ-3287-20260901T195930Z, option (a); tracked in #3870). The contract they now
# pin is the descoped one, and it is pinned in BOTH directions: perf's output and
# exit status must survive into tma-probe.txt whatever they are, and no capability
# verdict may be derived from them. The old cases asserted RESOLVED/ABSENT, i.e.
# exactly the layer that was removed for a live fail-open -- keeping them would
# have re-required the defect.

# --- 1. every perf outcome is RECORDED verbatim, rc included -------------------
# Bad-and-good in tension is expressed here as absent-vs-healthy: the recorder must
# keep BOTH, because a probe that only records what it likes is a classifier.
d="$TMP/c1a"; mkdir -p "$d"; rc=$(SHIM_TMA=absent run_probe "$d")
if [ "$rc" = 0 ] && grep -q 'VERDICT: COMPLETE' <<<"$(verdict "$d")" \
   && grep -q 'Cannot find metric' "$d/out/host/tma-probe.txt" \
   && grep -q '\[rc=1\]' "$d/out/host/tma-probe.txt" \
   && grep -q 'rc=1' <<<"$(gateline "$d" 'TopdownL2')"; then
  ok "Gate A absence -> perf's own diagnostic and rc recorded verbatim, no verdict invented"
else bad "Gate A absence was not recorded faithfully (rc=$rc, $(gateline "$d" 'TopdownL2'))" "$d"; fi

d="$TMP/c1b"; mkdir -p "$d"; rc=$(SHIM_TMA=resolved run_probe "$d")
if [ "$rc" = 0 ] && grep -q 'VERDICT: COMPLETE' <<<"$(verdict "$d")" \
   && grep -q 'tma_retiring' "$d/out/host/tma-probe.txt" \
   && grep -q '42.0' "$d/out/host/tma-probe.txt"; then
  ok "Gate A on a healthy TMA host -> the metric rows are preserved verbatim for the reader"
else bad "Gate A dropped or mangled real metric output (rc=$rc, $(verdict "$d"))" "$d"; fi

# --- 2. an unrecognised perf failure is recorded, not turned into an answer ----
# THIS CASE IS THE DESCOPE ITSELF, made falsifiable. Before #3870 this input
# stamped VERDICT: UNMEASURED, because the probe judged perf's exit status. It no
# longer does: rc=7 and the diagnostic are recorded, Gate A asserts nothing, and
# the run's verdict is decided by the data-integrity guards alone. What must NOT
# happen is the probe silently converting rc=7 into a capability answer.
d="$TMP/c2"; mkdir -p "$d"; rc=$(SHIM_TMA=error run_probe "$d")
if grep -q 'some unexpected diagnostic' "$d/out/host/tma-probe.txt" \
   && grep -q '\[rc=7\]' "$d/out/host/tma-probe.txt" \
   && grep -q 'rc=7' <<<"$(gateline "$d" 'TopdownL2')" \
   && ! grep -qi 'Gate A' "$d/stderr.txt"; then
  ok "Gate A operational error -> recorded with its rc, NOT converted into a capability answer (#3870)"
else bad "Gate A did not record an unrecognised perf failure faithfully (rc=$rc)" "$d"; fi

# --- 2b. the removed classifier must not come back through the report ----------
# A structural guard, not a behavioural one: these four tokens were the classifier's
# entire output vocabulary, so their reappearance in the report IS the regression,
# whatever produced it. Driven on a HEALTHY run, where the old layer would have had
# the most to say. ABSENT is deliberately not in the set -- the uncore-PMU line uses
# it for a directory that does not exist, which is a fact and not a verdict.
d="$TMP/c2b"; mkdir -p "$d"; rc=$(SHIM_TMA=resolved run_probe "$d")
leak=$(grep -oE '\b(RESOLVED|STUCK|NOT-MOVING|MOVING)\b' "$d/out/host/differential.txt" 2>/dev/null | sort -u | tr '\n' ' ')
if [ "$rc" = 0 ] && [ -z "$leak" ]; then
  ok "no per-gate verdict token in the report (descoped classifier stays out; #3870)"
else bad "classifier vocabulary reappeared in differential.txt: [$leak] (rc=$rc)" "$d"; fi

# --- 3. window closure: an ungated window must be caught --------------------
d="$TMP/c3"; mkdir -p "$d"; rc=$(SHIM_TMA=absent SHIM_UNGATED=1 run_probe "$d")
if [ "$rc" != 0 ] && grep -q 'WINDOW NOT GATED' "$d/stderr.txt"; then
  ok "constant instruction count -> WINDOW NOT GATED (job 305 f2 / 308 f1)"
else bad "an ungated window was NOT caught (rc=$rc)" "$d"; fi

d="$TMP/c3b"; mkdir -p "$d"; rc=$(SHIM_TMA=absent run_probe "$d")
if [ "$rc" = 0 ] && grep -q 'MEASURED-OK' <<<"$(gateline "$d" 'gate-integrity')"; then
  ok "scaling instruction count -> gate-integrity MEASURED-OK (good input accepted)"
else bad "a correctly gated window was rejected (rc=$rc, $(gateline "$d" 'gate-integrity'))"; fi

# --- 4. multiplexing: a scaled estimate must never be published as a count --
d="$TMP/c4"; mkdir -p "$d"; rc=$(SHIM_TMA=absent SHIM_ENABLED=74.00 run_probe "$d")
if [ "$rc" != 0 ] && grep -q 'enabled%=74.00' "$d/stderr.txt"; then
  ok "enabled%<100 -> rejected (small groups exist BECAUSE every value must be a count)"
else bad "a multiplexed group was accepted as counts (rc=$rc)" "$d"; fi

# --- 5. stale measurements from a previous run/host must be purged ----------
d="$TMP/c5"; mkdir -p "$d/out/host"
printf '# stale\n999999999,,cycle_activity.stalls_l3_miss:u,1,100.00,,\n' > "$d/out/host/arm-friendly-L2resident-stalls.csv"
printf '# stale\n888888888,,cycle_activity.stalls_l3_miss:u,1,100.00,,\n' > "$d/out/host/arm-hostile-512m-stalls.csv"
rc=$(SHIM_TMA=absent run_probe "$d")
if ! grep -rq '999999999\|888888888' "$d/out/host/"*.csv 2>/dev/null; then
  ok "stale arm CSVs purged before this run reports (job 312 f2: another host's numbers under COMPLETE)"
else bad "stale CSVs from a previous run survived into this run's verdict" "$d"; fi

# --- 6. <not counted> is a failed measurement, not an absence --------------
d="$TMP/c6"; mkdir -p "$d"
rc=$(SHIM_TMA=absent SHIM_EVENT_NOTCOUNTED=cycle_activity.stalls_l2_miss run_probe "$d")
if [ "$rc" != 0 ] && grep -q 'not counted' "$d/stderr.txt"; then
  ok "<not counted> -> probe failure (it does not establish absence; job 312 f3)"
else bad "<not counted> was accepted as a capability disposition (rc=$rc)" "$d"; fi

# --- 7. FALSE-FAIL direction: modifier-stripped CSV names must be accepted -
d="$TMP/c7"; mkdir -p "$d"; rc=$(SHIM_TMA=absent SHIM_STRIP_MOD=1 run_probe "$d")
if [ "$rc" = 0 ] && grep -q 'VERDICT: COMPLETE' <<<"$(verdict "$d")"; then
  ok "perf that strips ':u' from event names still accepted (job 312 f4: was a false FAIL)"
else bad "a good capture was rejected because names lacked ':u' (rc=$rc, $(verdict "$d"))"; fi

# --- 8. nesting violation invalidates #3224's difference-based partition ----
d="$TMP/c8"; mkdir -p "$d"; rc=$(SHIM_TMA=absent SHIM_NEST_VIOLATE=1 run_probe "$d")
if [ "$rc" != 0 ] && grep -q 'nesting violated' "$d/stderr.txt"; then
  ok "stalls_l3_miss > stalls_total -> nesting VIOLATED and the run fails"
else bad "a nesting violation was not caught (rc=$rc)" "$d"; fi

# --- 8b. the disposition sweep must ask about the spec the arms MEASURE --------
# roborev job 318. A host that permits user-only counting and denies kernel
# counting refuses the BARE event and allows ':u'. The sweep used to probe the
# bare form, so it reported an operational error and failed the run for events
# every arm can measure perfectly well. Bad input: a kernel-denying host must not
# red. (Reverting the probe to the bare form fails this case.)
d="$TMP/c8b"; mkdir -p "$d"; rc=$(SHIM_TMA=absent SHIM_KERNEL_DENIED=1 run_probe "$d")
if [ "$rc" = 0 ] && grep -q 'VERDICT: COMPLETE' <<<"$(verdict "$d")" \
   && ! grep -q 'event triage' "$d/stderr.txt"; then
  ok "kernel-counting denied, user-only permitted -> sweep still measures (probes the ':u' spec the arms use)"
else bad "the sweep asked about a spec the arms do not measure (rc=$rc, $(verdict "$d"))" "$d"; fi

# --- 8c. an arm that did not run the requested configuration must FAIL ---------
# roborev job 318. perf exiting 0 over a well-formed CSV says the counters worked;
# it says NOTHING about which workload they counted. A mislabelled arm produces
# real counts and inverts the differential, which is the probe's entire method.
d="$TMP/c8c"; mkdir -p "$d"; rc=$(SHIM_TMA=absent SHIM_ARM_MISLABEL=1 run_probe "$d")
if [ "$rc" != 0 ] && grep -q 'did NOT run the requested configuration' "$d/stderr.txt" \
   && grep -q 'buffer_bytes' "$d/stderr.txt"; then
  ok "benchmark reporting the wrong buffer size -> run FAILS, naming the field (counts real, arm mislabelled)"
else bad "an arm that ran a different configuration was accepted (rc=$rc)" "$d"; fi

# --- 9. Gate D: a CPU set outside this process's affinity must be refused -----
# The fix for job 313 f3 intersects candidate sibling groups with BOTH the online
# set and /proc/self/status Cpus_allowed_list. Driving it needs no shim: run the
# probe under a taskset that permits ONE cpu, so no COMPLETE sibling group can be
# both online and allowed, and the probe must refuse rather than pin blind.
#
# THE FIXTURE IS DERIVED FROM THE REAL TOPOLOGY, AND SKIPS WHERE IT CANNOT HOLD
# (roborev job 318). Two host shapes break the naive form, and both were live:
#   - `Cpus_allowed_list: 0,2,4` has no dash, so the old `-F-` extraction kept the
#     WHOLE list and tasksetted three CPUs -- enough to contain a complete sibling
#     group, so the probe correctly did NOT refuse and the case reported FAIL.
#   - On a NON-SMT host a single CPU IS a complete sibling group, so restricting to
#     one cpu is a perfectly pinnable configuration and refusing would be the bug.
# The first is a parsing defect and is fixed; the second is a host that cannot
# express this fixture at all, and is SKIPped by name.
d="$TMP/c9"; mkdir -p "$d"
bin9=$(mk_env "$d")
# Exactly one cpu id, from either spelling: "0-15", "0,2,4", "2-3,8".
onecpu=$(awk '/^Cpus_allowed_list:/{print $2}' /proc/self/status | cut -d, -f1 | cut -d- -f1)
sibs=""
[ -n "$onecpu" ] && sibs=$(cat "/sys/devices/system/cpu/cpu$onecpu/topology/thread_siblings_list" 2>/dev/null)
if [ -z "$onecpu" ] || ! command -v taskset >/dev/null 2>&1; then
  skip "Gate D affinity case (no taskset, or Cpus_allowed_list unreadable)"
elif [ -z "$sibs" ]; then
  skip "Gate D affinity case (cpu$onecpu exposes no thread_siblings_list — cannot predict the fixture)"
elif [ "$sibs" = "$onecpu" ]; then
  # Non-SMT: one cpu is a COMPLETE group, so the probe SHOULD pin and refusing
  # would be the defect. The fixture cannot be built here.
  skip "Gate D affinity case (non-SMT: cpu$onecpu is itself a complete sibling group, so a 1-cpu mask is pinnable and refusal would be WRONG)"
else
  # NOTE: the shim `taskset` inside $bin9 would defeat this, so the REAL taskset is
  # used to launch, and the shim dir is put on PATH only for the probe's children.
  ( export PATH="$bin9:$PATH"; exec /usr/bin/taskset -c "$onecpu" bash "$PROBE" "$d/out" "$HERE/../ws0-3224-artifacts/cache-hostile.c" ) \
      > "$d/stdout.txt" 2> "$d/stderr.txt"
  rc9=$?
  if [ "$rc9" != 0 ] && grep -q "affinity mask" "$d/stderr.txt"; then
    ok "single-CPU affinity -> Gate D refuses to pin (job 313 f3: was 'file is readable' only)"
  else
    bad "a CPU set outside the process's affinity was accepted (rc=$rc9; cpu$onecpu of siblings [$sibs])" "$d"
  fi
fi

# --- 10. Gate D: the healthy host must still be accepted ----------------------
# Same treatment in the good-input direction: this asserts that a host WITH a
# complete, online, allowed sibling group yields one. A single-vCPU or heavily
# constrained container has none, and reporting that as a Gate D defect would be
# a false accusation (roborev job 318).
have_group=no
allowed=$(awk '/^Cpus_allowed_list:/{print $2}' /proc/self/status)
for c in $(tr ',' ' ' <<<"${allowed:-}" | tr '-' ' '); do
  sl=$(cat "/sys/devices/system/cpu/cpu$c/topology/thread_siblings_list" 2>/dev/null) || continue
  case "$sl" in *,*|*-*) have_group=yes; break;; esac
done
if [ "$have_group" != yes ]; then
  skip "Gate D healthy-host case (this host exposes no multi-thread sibling group within its affinity — nothing to derive)"
else
  d="$TMP/c10"; mkdir -p "$d"; rc=$(SHIM_TMA=absent run_probe "$d")
  if [ "$rc" = 0 ] && grep -qE 'cpuset: +[0-9]+(,[0-9]+)*' "$d/out/host/differential.txt" \
     && grep -q 'all members online AND in this process' "$d/out/host/capability-probe.txt"; then
    ok "unrestricted host -> a complete, online, allowed sibling group IS derived (good input accepted)"
  else bad "Gate D rejected a healthy host (rc=$rc)" "$d"; fi
fi

# =============================================================================
echo
echo "cases: PASS=$PASS FAIL=$FAIL SKIP=$SKIP"
# A case FLOOR, on #3544's precedent: a span-replacing edit once silently deleted
# four cases and the suite reported failed:0 over a shrunken set. A green tally
# over fewer cases is not a green suite.
FLOOR=15
if [ $((PASS+FAIL+SKIP)) -lt $FLOOR ]; then
  echo "FAIL: only $((PASS+FAIL+SKIP)) cases were reached, floor is $FLOOR — cases were deleted, not fixed"
  exit 1
fi
# A SKIP is counted for the floor but must never substitute for verification: the
# 11 shim-driven cases depend on nothing about this host, so if any of them failed
# to RUN, something is wrong with the suite rather than with the machine.
SHIM_FLOOR=13
if [ "$PASS" -lt $SHIM_FLOOR ] && [ "$FAIL" = 0 ]; then
  echo "FAIL: only $PASS cases PASSed with 0 failures; $SHIM_FLOOR are host-independent and must always run"
  exit 1
fi
[ "$FAIL" = 0 ] || exit 1
if [ "$SKIP" -gt 0 ]; then
  echo "GUARDS VERIFIED ($PASS cases); $SKIP SKIPPED because this host cannot express the fixture — NOT verified here"
else
  echo "ALL GUARDS VERIFIED (bad input rejected AND good input accepted, $PASS cases)"
fi

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
PASS=0; FAIL=0
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
# single-event probe form (no -o): used by the disposition sweep
if [ -z "${OF:-}" ]; then
  if [ -n "${SHIM_EVENT_NOTCOUNTED:-}" ] && [ "$EV" = "$SHIM_EVENT_NOTCOUNTED" ]; then
    echo "   <not counted>      $EV"; exit 0
  fi
  case "$EV" in
    LLC-loads|LLC-load-misses|offcore_requests.all_data_rd|offcore_requests_buffer.sq_full|topdown.slots|slots|topdown-*|cycle_activity.stalls_mem_any)
      echo "event syntax error: Bad event name" >&2; exit 1 ;;
  esac
  echo "           1234      $EV"; exit 0
fi
# measurement form: synthesise a CSV for the requested group
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
exit 0
SHIM
  cat > "$b/cc" <<'SHIM'
#!/usr/bin/env bash
out=""; for i in "$@"; do [ "$prev" = "-o" ] && out="$i"; prev="$i"; done
[ -n "$out" ] && { printf '#!/bin/sh\nexit 0\n' > "$out"; chmod +x "$out"; }
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

# --- 1. Gate A: operational error must FAIL; absence must PASS ---------------
d="$TMP/c1a"; mkdir -p "$d"; rc=$(SHIM_TMA=error run_probe "$d")
if [ "$rc" != 0 ] && grep -q 'VERDICT: UNMEASURED' <<<"$(verdict "$d")" \
   && grep -q 'failed operationally' "$d/stderr.txt"; then
  ok "Gate A operational error -> UNMEASURED, cause named (job 312 f1: was fail-open via \$( ) subshell)"
else bad "Gate A operational error was NOT rejected (rc=$rc, $(verdict "$d"))"; fi

d="$TMP/c1b"; mkdir -p "$d"; rc=$(SHIM_TMA=absent run_probe "$d")
if [ "$rc" = 0 ] && grep -q 'VERDICT: COMPLETE' <<<"$(verdict "$d")" \
   && grep -q 'ABSENT' <<<"$(gateline "$d" 'TopdownL2')"; then
  ok "Gate A absence -> COMPLETE + ABSENT (an absent event is the ANSWER, not a failure)"
else bad "Gate A absence was misreported (rc=$rc, $(verdict "$d"), $(gateline "$d" 'TopdownL2'))"; fi

# --- 2. Gate A: rc=0 with no numbers is not a measurement -------------------
d="$TMP/c2"; mkdir -p "$d"; rc=$(SHIM_TMA=zero-no-numbers run_probe "$d")
if [ "$rc" != 0 ] && grep -q 'no numeric output' "$d/out/host/differential.txt" ; then
  ok "Gate A rc=0 with no numbers -> not RESOLVED (an exit status is not a measurement)"
else bad "Gate A accepted rc=0 with no numeric output (rc=$rc)" "$d"; fi

d="$TMP/c2b"; mkdir -p "$d"; rc=$(SHIM_TMA=resolved run_probe "$d")
if [ "$rc" = 0 ] && grep -q 'RESOLVED' <<<"$(gateline "$d" 'TopdownL2')"; then
  ok "Gate A with real metric output -> RESOLVED (good input still accepted)"
else bad "Gate A rejected a healthy TMA host (rc=$rc, $(gateline "$d" 'TopdownL2'))"; fi

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
  ok "stale arm CSVs purged before classification (job 312 f2: another host's numbers under COMPLETE)"
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

# =============================================================================
echo
echo "cases: PASS=$PASS FAIL=$FAIL"
# A case FLOOR, on #3544's precedent: a span-replacing edit once silently deleted
# four cases and the suite reported failed:0 over a shrunken set. A green tally
# over fewer cases is not a green suite.
FLOOR=11
if [ $((PASS+FAIL)) -lt $FLOOR ]; then
  echo "FAIL: only $((PASS+FAIL)) cases ran, floor is $FLOOR — cases were deleted, not fixed"
  exit 1
fi
[ "$FAIL" = 0 ] || exit 1
echo "ALL GUARDS VERIFIED (bad input rejected AND good input accepted, $PASS cases)"

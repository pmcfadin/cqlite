#!/usr/bin/env bash
# Regression test for issue #2640: per-gate CPU/core budget derivation.
#
# The #1825 machine-wide cap bounds the NUMBER of concurrent full gates (N), but
# on its own does nothing to stop each gate from spawning ncpu build/test threads
# → ncpu*N oversubscription → SIGKILLs and timing flakes. The gate therefore
# derives, from the SAME slot count, a fair-share core budget:
#   * full cores when it is the SOLE gate (CQLITE_GATE_MAX_CONCURRENCY=1), and
#   * max(1, floor(ncpu / N)) when N>1,
# exported as CARGO_BUILD_JOBS + used as the nextest/cargo --test-threads, and
# wraps the whole gate in `taskpolicy -c utility` (macOS) / `nice` (Linux).
#
# This test drives the gate's hidden `--cpu-budget` hook, which prints the SAME
# `cpu-budget:` line stamped into the SUMMARY, WITHOUT running any component. It
# pins the core count via AGENT_GATE_TEST_NCPU so the derivation is deterministic
# across machines.
#
# Run standalone:   bash scripts/tests/test_gate_cpu_budget.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; PASS=$PASS; FAIL=$((FAIL + 1)); }

# budget_field <line> <field>: extract "<field>=<value>" from a `cpu-budget:` line.
budget_field() {
  printf '%s\n' "$1" | tr ' ' '\n' | sed -n "s/^$2=//p" | head -1
}

# cpu_budget <extra-env...>: run the hidden hook with a pinned ncpu + no-wrapper
# (CQLITE_GATE_NO_NICE=1 keeps the assertion about wrapper=none/derivation clean
# and avoids a taskpolicy/nice re-exec during the test) plus any extra env pairs.
#
# HERMETICITY (issue #2640): when this self-test runs as a `tooling-tests`
# component, the PARENT gate has already exported its OWN budget/wrapper env —
# CARGO_BUILD_JOBS (=> source would read `caller`), AGENT_GATE_WRAPPED, and
# AGENT_GATE_WRAPPER (=> wrapper would read `taskpolicy -c utility`, spaces and
# all). A nested `--cpu-budget` legitimately inherits those, so we MUST scrub
# them here to unit-test the derivation against the test's OWN pinned inputs
# rather than the ambient parent-gate state (else 2/3/6/8 fail nested but pass
# standalone). `env -u` per-invocation keeps each case's inputs fully controlled.
cpu_budget() {
  env -u CARGO_BUILD_JOBS -u AGENT_GATE_WRAPPED -u AGENT_GATE_WRAPPER \
    CQLITE_GATE_NO_NICE=1 "$@" bash "$GATE" --cpu-budget 2>/dev/null | grep -E '^cpu-budget: ' | head -1
}

# --- 1. syntax check ---
if bash -n "$GATE" 2>/dev/null; then
  ok "agent-gate.sh parses (bash -n)"
else
  bad "agent-gate.sh has a syntax error"
fi

# --- 2. SOLE gate (N=1) => FULL cores for build-jobs + test-threads (derived) ---
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=1)
if [ -z "$line" ]; then
  bad "no cpu-budget line emitted for N=1"
else
  bj=$(budget_field "$line" build-jobs); tt=$(budget_field "$line" test-threads)
  cpg=$(budget_field "$line" cores-per-gate)
  # build-jobs carries a "(derived)"/"(caller)" suffix — strip it for the numeric check.
  bjn=${bj%%(*}
  if [ "$bjn" = 16 ] && [ "$tt" = 16 ] && [ "$cpg" = 16 ]; then
    ok "N=1 (sole gate) => full cores: build-jobs=$bjn test-threads=$tt cores-per-gate=$cpg (ncpu=16)"
  else
    bad "N=1 should give full 16 cores, got build-jobs=$bj test-threads=$tt cores-per-gate=$cpg"
  fi
  case "$bj" in *"(derived)"*) ok "N=1 build-jobs marked (derived)" ;; *) bad "N=1 build-jobs not marked derived: $bj" ;; esac
fi

# --- 3. N=4 on 16 cores => fair share of 4 cores each ---
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=4)
cpg=$(budget_field "$line" cores-per-gate); tt=$(budget_field "$line" test-threads)
bjn=$(budget_field "$line" build-jobs); bjn=${bjn%%(*}
if [ "$cpg" = 4 ] && [ "$tt" = 4 ] && [ "$bjn" = 4 ]; then
  ok "N=4 on 16 cores => fair share 4 (build-jobs=$bjn test-threads=$tt cores-per-gate=$cpg)"
else
  bad "N=4/16cores should give 4 cores each, got cores-per-gate=$cpg test-threads=$tt build-jobs=$bjn"
fi

# --- 4. Floor at 1: N greater than ncpu never yields 0 cores ---
line=$(cpu_budget AGENT_GATE_TEST_NCPU=2 CQLITE_GATE_MAX_CONCURRENCY=8)
cpg=$(budget_field "$line" cores-per-gate); tt=$(budget_field "$line" test-threads)
if [ "$cpg" = 1 ] && [ "$tt" = 1 ]; then
  ok "N>ncpu floors at 1 core (cores-per-gate=$cpg test-threads=$tt)"
else
  bad "N>ncpu should floor at 1, got cores-per-gate=$cpg test-threads=$tt"
fi

# --- 5. An explicit CARGO_BUILD_JOBS from the caller is respected verbatim ---
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=1 CARGO_BUILD_JOBS=3)
bj=$(budget_field "$line" build-jobs)
case "$bj" in
  "3(caller)") ok "caller CARGO_BUILD_JOBS=3 respected + marked (caller): $bj" ;;
  *) bad "caller CARGO_BUILD_JOBS should be 3(caller), got: $bj" ;;
esac

# --- 6. CQLITE_GATE_NO_NICE=1 => wrapper=none (no taskpolicy/nice re-exec) ---
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=1)
w=$(budget_field "$line" wrapper)
if [ "$w" = none ]; then
  ok "CQLITE_GATE_NO_NICE=1 => wrapper=none"
else
  bad "expected wrapper=none under CQLITE_GATE_NO_NICE=1, got wrapper=$w"
fi

# --- 7. On this dev box (macOS), taskpolicy wrapping engages when available ---
if [ "$(uname -s)" = Darwin ] && command -v taskpolicy >/dev/null 2>&1; then
  # WITHOUT the no-nice escape hatch, the gate re-execs under taskpolicy and the
  # re-exec'd copy reports wrapper=taskpolicy... in its cpu-budget line. Scrub the
  # parent gate's AGENT_GATE_WRAPPED/AGENT_GATE_WRAPPER (issue #2640): when nested
  # in `tooling-tests` they are already set, which would short-circuit the re-exec
  # guard and report wrapper=none (green standalone, red nested) — clear them so
  # this case genuinely exercises the wrap.
  wline=$(env -u AGENT_GATE_WRAPPED -u AGENT_GATE_WRAPPER AGENT_GATE_TEST_NCPU=16 bash "$GATE" --cpu-budget 2>/dev/null | grep -E '^cpu-budget: ' | head -1)
  w=$(budget_field "$wline" wrapper)
  case "$w" in
    taskpolicy) ok "macOS: gate wraps in taskpolicy -c utility (wrapper=$w)" ;;
    *) bad "macOS with taskpolicy present should wrap (wrapper=taskpolicy), got wrapper=$w" ;;
  esac
else
  ok "SKIP wrapper-engage case (not macOS-with-taskpolicy)"
fi

# --- 8. The cpu-budget line is well-formed (all fields present) ---
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=2)
if printf '%s\n' "$line" \
   | grep -Eq '^cpu-budget: wrapper=[^ ]+ ncpu=[0-9]+ max-concurrency=[0-9]+ cores-per-gate=[0-9]+ build-jobs=[0-9]+\((derived|caller)\) test-threads=[0-9]+$'; then
  ok "cpu-budget line well-formed: $line"
else
  bad "malformed cpu-budget line: $line"
fi

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]

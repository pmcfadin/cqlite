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
SKIPS=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; PASS=$PASS; FAIL=$((FAIL + 1)); }
# A SKIP IS NOT A PASS (#3414 roborev round 2). The host-conditional case below announced
# itself through `ok`, which incremented PASS and reported nothing skipped — so a suite
# total could not distinguish "ran and passed" from "did not run", which is the same
# proxy-for-a-fact shape this issue exists to remove, one directory over.
skip() { printf 'skip - %s\n' "$1"; SKIPS=$((SKIPS + 1)); }

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
#
# CQLITE_GATE_MAX_CONCURRENCY IS SCRUBBED HERE TOO (issue #3414). Every fleet box
# exports it (bootstrap pins it in /etc/environment), so without the scrub the
# `default` source case — the one that reproduces the #3414 fleet condition — could
# never be exercised on the machines that run this suite: it would inherit `1` and
# report `pinned`, passing for the wrong reason. `env` applies its `-u` options
# before the NAME=VALUE assignments, so a case that PASSES the variable still gets
# the value it asked for, and a case that omits it gets a genuinely unset variable.
cpu_budget() {
  env -u CARGO_BUILD_JOBS -u AGENT_GATE_WRAPPED -u AGENT_GATE_WRAPPER \
    -u CQLITE_GATE_MAX_CONCURRENCY \
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
  # The wrapper field MUST be a single token even when a wrapper is active — the
  # underlying command is `taskpolicy -c utility` (spaces), which must NOT leak
  # `-c`/`utility` into the space-delimited cpu-budget line (roborev #2640).
  if printf '%s\n' "$wline" \
     | grep -Eq '^cpu-budget: wrapper=[^ ]+ ncpu=[0-9]+ max-concurrency=[0-9]+\((pinned|default|invalid|clamped)\) cores-per-gate=[0-9]+ build-jobs=[0-9]+\((derived|caller)\) test-threads=[0-9]+$'; then
    ok "wrapper-active cpu-budget line stays single-token/well-formed: $wline"
  else
    bad "wrapper-active line not well-formed (space leaked from '$w'?): $wline"
  fi
else
  skip "wrapper-engage case (not macOS-with-taskpolicy)"
fi

# --- 8. The cpu-budget line is well-formed (all fields present) ---
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=2)
if printf '%s\n' "$line" \
   | grep -Eq '^cpu-budget: wrapper=[^ ]+ ncpu=[0-9]+ max-concurrency=[0-9]+\((pinned|default|invalid|clamped)\) cores-per-gate=[0-9]+ build-jobs=[0-9]+\((derived|caller)\) test-threads=[0-9]+$'; then
  ok "cpu-budget line well-formed: $line"
else
  bad "malformed cpu-budget line: $line"
fi

# --- 9. WHERE N CAME FROM is reported, not just N (issue #3414) ---------------
# `max-concurrency=3` alone cannot distinguish a box PINNED at 3 from one that
# DEFAULTED there because nothing set the variable — the exact condition that ran
# unseen across the whole fleet: the pin was present in ~/.bashrc and invisible to
# every non-interactive shell, so every gate resolved N from the #1825 formula,
# admitted co-tenants, and no pasted artifact said so. Four source tokens, each
# asserted against the VALUE it must accompany, because a token that is right while
# the number is wrong (or vice versa) is the same unreadable artifact.
budget_source() {  # budget_source <line>: the "(...)" suffix of max-concurrency=N(...)
  local f; f=$(budget_field "$1" max-concurrency)
  case "$f" in *"("*")") f=${f#*(}; printf '%s' "${f%)}" ;; *) printf '' ;; esac
}
budget_n() {       # budget_n <line>: the numeric part of max-concurrency=N(...)
  local f; f=$(budget_field "$1" max-concurrency); printf '%s' "${f%%(*}"
}

# 9a. UNSET => the #1825 formula max(2,(ncpu-2)/4) = 3 on 16 cores, marked (default).
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16)
if [ "$(budget_n "$line")" = 3 ] && [ "$(budget_source "$line")" = default ]; then
  ok "source: UNSET => max-concurrency=3(default) on 16 cores"
else
  bad "source: UNSET should give 3(default), got max-concurrency=$(budget_field "$line" max-concurrency)"
fi

# 9b. A valid pin is used verbatim and marked (pinned) — the state a correctly
#     provisioned box must show, so a missing pin is visible on the FIRST summary.
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=1)
if [ "$(budget_n "$line")" = 1 ] && [ "$(budget_source "$line")" = pinned ]; then
  ok "source: CQLITE_GATE_MAX_CONCURRENCY=1 => max-concurrency=1(pinned)"
else
  bad "source: an explicit pin should be 1(pinned), got max-concurrency=$(budget_field "$line" max-concurrency)"
fi

# 9c. A NON-NUMERIC value is silently discarded for the formula. Without the token
#     that box is textually identical to 9a, which is the #3414 failure shape.
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=abc)
if [ "$(budget_n "$line")" = 3 ] && [ "$(budget_source "$line")" = invalid ]; then
  ok "source: a non-numeric value falls back to the formula and is marked (invalid)"
else
  bad "source: 'abc' should give 3(invalid), got max-concurrency=$(budget_field "$line" max-concurrency)"
fi

# 9d. SET-BUT-EMPTY is a DIFFERENT fact from UNSET and must not read as (default).
#     `${VAR:-dflt}` cannot tell them apart; `${VAR+set}` can. This case is the one
#     that pins that distinction — a mis-set tmux/systemd/CI variable lands here.
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=)
if [ "$(budget_n "$line")" = 3 ] && [ "$(budget_source "$line")" = invalid ]; then
  ok "source: an EMPTY value is (invalid), never (default)"
else
  bad "source: an empty value should give 3(invalid), got max-concurrency=$(budget_field "$line" max-concurrency)"
fi

# 9e. A valid integer < 1 is silently raised to 1 — reported as (clamped), never as
#     (pinned): the operator asked for 0 and got 1, and only the token says so.
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=0)
if [ "$(budget_n "$line")" = 1 ] && [ "$(budget_source "$line")" = clamped ]; then
  ok "source: 0 is clamped to 1 and marked (clamped)"
else
  bad "source: 0 should give 1(clamped), got max-concurrency=$(budget_field "$line" max-concurrency)"
fi

# 9f. The source token must never break the space-delimited token grammar: it lives
#     INSIDE the max-concurrency token, exactly as build-jobs carries its own source.
#     (6 key=value tokens after the `cpu-budget:` label = 7 whitespace-separated words.)
line=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY=2)
if [ "$(printf '%s\n' "$line" | wc -w | tr -d ' ')" = 7 ]; then
  ok "source: the token count is unchanged (max-concurrency stays ONE token): $line"
else
  bad "source: the source token leaked a space into the cpu-budget line: $line"
fi

# 9g. ONE resolution, structurally. The slot semaphore (acquire_gate_slot) and the
#     SUMMARY line must report the SAME N — a second, independently-recomputing
#     resolver could drift, and then the cap the gate ENFORCES and the cap the
#     pasted block NAMES would be different numbers with no way to tell. Asserted
#     against the source because the divergence is unobservable from output alone:
#     both would look plausible.
if grep -q '^_gate_max_concurrency() { printf .%s. "\$GATE_MAX_CONCURRENCY"; }$' "$GATE" \
   && [ "$(grep -c 'GATE_MAX_CONCURRENCY_SOURCE=' "$GATE")" -ge 1 ] \
   && [ "$(grep -c 'CQLITE_GATE_MAX_CONCURRENCY+set' "$GATE")" = 1 ]; then
  ok "source: N is resolved ONCE (_gate_max_concurrency reads the single global)"
else
  bad "source: N looks resolved in more than one place (a second resolver can drift from the slot cap)"
fi

# LEADING-ZERO VALUES MUST NOT REACH THE ARITHMETIC AS OCTAL (roborev job 331, Medium).
# The digit-only guard admits `08`, and bash reads a leading zero as OCTAL — where `08` and
# `09` are not valid octal — so the value flowed into `cores=$(( _ncpu / n ))` and the GATE
# ERRORED OUT instead of resolving a cap. Measured before the fix:
#   CQLITE_GATE_MAX_CONCURRENCY=08 -> "line 1301: 08: value too great for base"
# Erroring is worse than either honouring or refusing the value, because a gate that dies
# inside its own budget line produces no verdict at all. `10#` forces base 10 and the value is
# normalised, so the SUMMARY reports the cap actually honoured.
zz_ok=1
for _zz in "08 8 pinned" "09 9 pinned" "01 1 pinned" "0 1 clamped"; do
  set -- $_zz
  _zline=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY="$1" 2>&1)
  _zgot="$(budget_n "$_zline")($(budget_source "$_zline"))"
  [ "$_zgot" = "$2($3)" ] || { zz_ok=0; echo "  '$1' should give $2($3), got '$_zgot'"; }
  case "$_zline" in
    *"value too great for base"*) zz_ok=0; echo "  '$1' produced an octal arithmetic error in the budget line" ;;
  esac
done
if [ "$zz_ok" = 1 ]; then
  ok "source: leading-zero values are read base-10 (08->8, 09->9, 01->1, 0->1 clamped) and never reach the arithmetic as octal"
else
  bad "source: a leading-zero value was misread or errored in the cpu-budget line"
fi

# A DIGIT STRING TOO LARGE TO REPRESENT MUST NOT WRAP INTO A PIN (roborev job 332).
# `10#$v` on an unrepresentable decimal wraps SILENTLY. Measured before the fix:
#   ...=99999999999999999999 -> max-concurrency=7766279631452241919(pinned)
#   ...=9223372036854775808  -> max-concurrency=1(clamped)
# The first AFFIRMS A PIN AT A VALUE NOBODY SET, which inverts the one property this token
# exists to carry; the second mislabels an unusable value as a deliberate 0. Both are now
# `invalid` — refused BY DIGIT COUNT, before the arithmetic, since the bound cannot be
# decided by the operation that is the defect. The 18/19-digit boundary is asserted in BOTH
# directions: a bound that refused everything, or nothing, would pass a one-sided check.
ov_ok=1
for _ov in "99999999999999999999 3 invalid" "9223372036854775808 3 invalid" \
           "1000000000000000000 3 invalid" "999999999999999999 999999999999999999 pinned" \
           "4294967296 4294967296 pinned"; do
  set -- $_ov
  _ovline=$(cpu_budget AGENT_GATE_TEST_NCPU=16 CQLITE_GATE_MAX_CONCURRENCY="$1" 2>&1)
  _ovgot="$(budget_n "$_ovline")($(budget_source "$_ovline"))"
  [ "$_ovgot" = "$2($3)" ] || { ov_ok=0; echo "  '$1' should give $2($3), got '$_ovgot'"; }
  # the wrap produced a number the operator never set; assert the value is never invented
  case "$_ovgot" in
    *7766279631452241919*) ov_ok=0; echo "  '$1' wrapped into a fabricated cap" ;;
  esac
  case "$_ovline" in
    *"out of range"*|*"value too great"*) ov_ok=0; echo "  '$1' errored in the budget line" ;;
  esac
done
if [ "$ov_ok" = 1 ]; then
  ok "source: an unrepresentable digit string is invalid, refused by digit count before the arithmetic (18 digits pinned, 19 invalid)"
else
  bad "source: an oversized value wrapped, errored, or was misclassified in the cpu-budget line"
fi

echo
echo "PASS=$PASS FAIL=$FAIL SKIP=$SKIPS"
[ "$FAIL" -eq 0 ]

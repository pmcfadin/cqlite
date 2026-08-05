#!/usr/bin/env bash
# test_ws0_numeric_wraparound.sh — BASH ARITHMETIC WRAPS, AND NO RIG BOUND MAY DEPEND ON IT
# (issue #3272 review round 7, F2).
#
# # Why this is its own suite
#
# Split out of `test_ws0_cpu_pinning_guards.sh` under the campsite rule (test target ~1500
# lines; that file reached 1537 with these cases in it). The seam is a RESPONSIBILITY, not a
# line count: this class spans TWO libraries — the CPU-index bound in `lib-cpu.sh` and the
# duration/count bounds in `lib-args.sh` — and now a shared primitive that belongs to neither.
# The CPU suite's subject is TOPOLOGY (is the pinning a real physical core?); this file's
# subject is ARITHMETIC (can a bound be defeated by the evaluator that checks it?).
#
# # The class, and its three instances
#
# Bash arithmetic is signed 64-bit and WRAPS SILENTLY, so a bound checked with `(( ))` is
# checked on whatever the value wrapped to rather than on what the caller wrote. That has been
# the root cause of THREE findings in this rig, in three places, each previously fixed alone:
#
#   round 4  `parse_duration_ms`: `2305843009213693956s` * 1000 wrapped to 4000ms — UNDER the
#            5000ms cold-step ceiling, smuggling a blended cold measurement past that guard.
#   round 4  `require_positive_int`: `99999999999999999999` evaluated to 7766279631452241919,
#            so the range check compared a number nobody wrote.
#   round 7  `cpu_range_validate` (F2): endpoints were digit-UNCAPPED, so
#            `9223372036854775809-0` yielded a NEGATIVE `lo` that passed BOTH the index ceiling
#            and the expansion cap (whose own `hi - lo + 1` wraps negative) and then drove
#            `for ((i = lo; i <= hi; i++))` over ~9.2e18 iterations — an OOM mid-measurement
#            from an argument that had been ACCEPTED.
#
# Three sites, one class, so the fix is a MECHANISM: `lib-args.sh` owns
# `decimal_normalize`/`decimal_le`, which compare CANONICAL DECIMAL STRINGS using NO arithmetic
# at all. There is no digit cap to choose and nothing left to wrap, which is what makes it a
# mechanism rather than a fourth per-site patch.
#
# # The bar is OBSERVED TO FIRE, with its non-vacuity half
#
# Per #3249 (a hardcoded `_PERF_STATE="ok"` survived 118/118 tests) a guard never observed
# rejecting is not evidence. So every firing case here is paired with a REPLICA of the removed
# arithmetic, observed to have ACCEPTED the same input — otherwise the refusal might be about a
# spec that was never a bypass. The expansion-loop case runs under `timeout`, because the
# pre-fix failure mode is a HANG and a hanging test is not a failing test.
#
# Hermetic: sources two libraries against a synthetic sysfs topology in `$TMPDIR`, drives pure
# string/integer functions, and executes no cargo, perf, sudo, corpus or network.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LIB="$REPO_ROOT/scripts/perf/lib-cpu.sh"
ARGS_LIB="$REPO_ROOT/scripts/perf/lib-args.sh"

fails=0
# `checks` counts what actually RAN, so the floor at the end can see a block that silently
# never executed.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$LIB" ] || { echo "FAIL - missing $LIB"; exit 1; }
[ -f "$ARGS_LIB" ] || { echo "FAIL - missing $ARGS_LIB"; exit 1; }

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# A synthetic CPU topology, so `lib-cpu.sh` can be sourced without the real host's layout.
# cpu<c> and cpu<c+4> are siblings, matching the CPU suite's fixture.
TOPO="$TMP/topo/cpu"
mk_topology() {
  local c s
  rm -rf "$TOPO"
  for c in 0 1 2 3; do
    for s in "$c" "$((c + 4))"; do
      mkdir -p "$TOPO/cpu$s/topology"
      printf '%s,%s\n' "$c" "$((c + 4))" > "$TOPO/cpu$s/topology/thread_siblings_list"
    done
  done
}
mk_topology

# lib_call <function> [args…] — source lib-cpu.sh against the FAKE topology in a subshell and
# run one function. Prints its output (stderr folded in); returns its rc. A subshell per call so
# no case can inherit another's state.
lib_call() {
  local fn="$1"; shift
  ( export CQLITE_WS0_CPU_TOPOLOGY_ROOT="$TOPO"
    # shellcheck disable=SC1090
    source "$LIB"
    "$fn" "$@" ) 2>&1
}

# ===========================================================================
# #3272 review round 7, F2 — 64-BIT WRAPAROUND CANNOT BYPASS EITHER CPU BOUND
# ===========================================================================
# The allowlist grammar stops COMMAND SUBSTITUTION; it does not stop a WELL-FORMED decimal too
# large for signed 64-bit arithmetic. The bound check used to run AFTER `lo=$((10#$part))`, i.e.
# on the WRAPPED value, so:
#
#   * '9223372036854775809-0' became lo=-9223372036854775807, hi=0. That defeats BOTH bounds at
#     once: `lo -gt 8191`? no. `hi -lt lo`? no. And in `cpu_list_expand` the expansion cap
#     `hi - lo + 1 + ${#out[@]} > CPU_LIST_MAX` computes ~9.2e18 which ITSELF wraps NEGATIVE, so
#     the cap passes too — and `for ((i = lo; i <= hi; i++))` then appends ~9.2e18 elements.
#   * '18446744073709559807' wrapped to exactly 8191 — an out-of-range index accepted AS the
#     in-range maximum, so the sibling check would go on to verify pinning for cpu8191.
#
# Same class as round 4's `010s`/`2305843009213693956s` duration wraparound, in a second place;
# fixed as a MECHANISM (`decimal_le`, string comparison, no arithmetic), so there is no digit cap
# to choose and a decimal of ANY length is compared as written.
#
# The NON-VACUITY half is what makes each case evidence rather than assertion: the same input is
# run through a REPLICA of the pre-fix arithmetic and observed to have been ACCEPTED.
# `wrap_bypassed_prefix <spec>` prints `bypassed` when the pre-fix code would have let it
# through — computed with the very `$(( ))` forms that were removed.
wrap_bypassed_prefix() {
  local part="$1" lo hi
  if [[ "$part" == *-* ]]; then
    lo=$((10#${part%%-*})); hi=$((10#${part##*-}))
  else
    lo=$((10#$part)); hi="$lo"
  fi
  # The pre-fix bounds, verbatim.
  if [[ "$lo" -gt 8191 || "$hi" -gt 8191 ]]; then echo "refused-index"; return; fi
  if [[ "$hi" -lt "$lo" ]]; then echo "refused-reversed"; return; fi
  if (( hi - lo + 1 > 1024 )); then echo "refused-cap"; return; fi
  echo "bypassed"
}
for wrap_spec in '9223372036854775809-0' '18446744073709559807' '18446744073709551616'; do
  out=$(lib_call cpu_range_validate "$wrap_spec" test); rc=$?
  if [ "$rc" -ne 0 ] && grep -q 'above 8191' <<<"$out"; then
    pass "cpu-wrap: OBSERVED (round7 F2) — the 64-bit-wrapping spec '$wrap_spec' is REFUSED on the index ceiling"
  else
    fail "round7 F2: '$wrap_spec' must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
  fi
  # NON-VACUITY: the pre-fix arithmetic really did accept it. Without this the refusal above
  # could be about a spec that was never a bypass.
  pre=$(wrap_bypassed_prefix "$wrap_spec")
  if [ "$pre" = "bypassed" ]; then
    pass "cpu-wrap: NON-VACUITY — the PRE-FIX arithmetic ACCEPTED '$wrap_spec' (this is the bypass F2 names)"
  else
    fail "round7 F2: '$wrap_spec' must have been accepted pre-fix, else the case proves nothing (pre-fix verdict: $pre)"
  fi
done
# ...and the WHOLE-PIPELINE consequence: `cpu_list_expand` must refuse it too, rather than
# starting a ~9.2e18-iteration loop. Run under `timeout` because the pre-fix failure mode is a
# HANG-then-OOM, and a test that hangs is not a test that failed.
if command -v timeout >/dev/null 2>&1; then
  out=$(timeout 20 bash -c '
    export CQLITE_WS0_CPU_TOPOLOGY_ROOT="'"$TOPO"'"
    source "'"$LIB"'"
    cpu_list_expand "9223372036854775809-0" test' 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 124 ] && grep -q 'above 8191' <<<"$out"; then
    pass "cpu-wrap: OBSERVED (round7 F2) — cpu_list_expand REFUSES the wrapping spec promptly (pre-fix: an unbounded expansion loop, i.e. an OOM mid-measurement)"
  else
    fail "round7 F2: cpu_list_expand must refuse the wrapping spec without looping (rc=$rc$([ "$rc" -eq 124 ] && echo ' = TIMED OUT, i.e. still looping'), out: $(head -2 <<<"$out"))"
  fi
else
  fail "round7 F2: 'timeout' is required to prove the wrapping spec does not loop"
fi
# THE PRIMITIVE ITSELF, over the boundary values — because `decimal_le` is now the mechanism
# behind the bound and a mechanism whose own arithmetic is unmeasured is the #3249 shape.
# Cases as `a:b:expected`, where expected is `le` (a <= b) or `gt`.
for dcase in '0:0:le' '8191:8191:le' '8192:8191:gt' '8190:8191:le' \
             '007:7:le' '007:6:gt' '0:8191:le' \
             '99999999999999999999999999999999:8191:gt' \
             '9223372036854775809:8191:gt' '18446744073709559807:8191:gt' \
             '8191:99999999999999999999999999999999:le'; do
  a="${dcase%%:*}"; rest="${dcase#*:}"; b="${rest%%:*}"; want="${rest##*:}"
  if ( source "$REPO_ROOT/scripts/perf/lib-args.sh"; decimal_le "$a" "$b" ) 2>/dev/null; then
    got=le
  else
    got=gt
  fi
  if [ "$got" = "$want" ]; then
    pass "decimal-le: '$a' vs '$b' => $want (no arithmetic, so any length is compared as written)"
  else
    fail "round7 F2: decimal_le '$a' '$b' must be $want, got $got"
  fi
done
# ...and the primitive must be REACHABLE from lib-cpu.sh even when sourced STANDALONE, which is
# how the tests drive it and how a future non-driver caller would. A missing dependency must
# REFUSE, never silently fall back to arithmetic.
if ( source "$LIB" >/dev/null 2>&1; declare -F decimal_le >/dev/null ); then
  pass "decimal-le: lib-cpu.sh sourced STANDALONE has decimal_le in scope (it sources lib-args.sh itself)"
else
  fail "round7 F2: lib-cpu.sh must obtain decimal_le when sourced alone, or its bound check is unwired"
fi


# ===========================================================================
# THE OTHER TWO INSTANCES OF THE SAME CLASS, re-verified HERE (round 4's fixes)
# ===========================================================================
# Round 4 fixed `parse_duration_ms` and `require_positive_int` with a 9-DIGIT CAP each, in
# `lib-args.sh`. Those fixes are correct and are NOT changed by F2 — a cap is sufficient where
# the input has a natural small ceiling (a rep count, a millisecond step). They are driven here
# because the CLASS now has one home: a future edit that removes a cap should red the file whose
# subject is the class, not only the file whose subject is the flag.
#
# The duration case is the SECURITY-ADJACENT one: the value it wraps to (4000ms) is UNDER the
# 5000ms cold-step ceiling, so the wrap does not merely produce a wrong number — it defeats the
# guard that keeps a cold measurement from being a blend.
# `parse_duration_ms` must REFUSE the long value with the TOO-LONG code (3), not silently
# multiply it. rc=1 would be the misleading "malformed" complaint round 4 also fixed.
out=$( source "$ARGS_LIB"; parse_duration_ms "2305843009213693956s" ); rc=$?
if [ "$rc" -eq 3 ]; then
  pass "duration-wrap: OBSERVED — '2305843009213693956s' is refused as TOO LONG (rc=3), not multiplied"
else
  fail "round4/F2 class: a 19-digit duration must return rc=3 TOO_LONG (rc=$rc, out: $out)"
fi
# NON-VACUITY: the wrap really does land under the cold-step ceiling. Computed here with the
# arithmetic the cap now prevents from ever running on this value.
wrapped=$(( 2305843009213693956 * 1000 ))
if [ "$wrapped" -le 5000 ] && [ "$wrapped" -gt 0 ]; then
  pass "duration-wrap: NON-VACUITY — that value * 1000 wraps to ${wrapped}ms, UNDER the 5000ms cold-step ceiling (this is the guard bypass)"
else
  fail "round4/F2 class: the wrap must land under the ceiling, else the case proves nothing (got ${wrapped}ms)"
fi
# ...and the ACCEPT direction, so the cap is not simply refusing everything.
out=$( source "$ARGS_LIB"; parse_duration_ms "45s" ); rc=$?
if [ "$rc" -eq 0 ] && [ "$out" = "45000" ]; then
  pass "duration-wrap: ACCEPT — an ordinary '45s' still parses to 45000ms"
else
  fail "round4/F2 class: '45s' must parse to 45000 (rc=$rc, out: $out)"
fi
# `require_positive_int` exits 2 on the over-long value, refusing BEFORE arithmetic.
#
# The redirection goes INSIDE the subshell: `( … ) 2>&1` outside the command substitution
# redirects the SUBSHELL's stderr to the enclosing script's stdout, not into `$out` — so `$out`
# came back empty and the `grep` could never match. Same shape as the round-4 `if ! cmd; then …
# $? …` note in the driver: an assertion whose verdict comes from the wrong stream.
out=$( source "$ARGS_LIB"; require_positive_int reps "99999999999999999999" 2>&1 ); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'absurdly large' <<<"$out"; then
  pass "count-wrap: OBSERVED — a 20-digit --reps is refused BEFORE arithmetic ('absurdly large')"
else
  fail "round4/F2 class: a 20-digit --reps must be refused before arithmetic (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# NON-VACUITY: it really does evaluate to something else entirely.
#
# The comparison is against the DECIMAL STRING, not against the literal in arithmetic: writing
# `[ "$wrapped_count" -ne 99999999999999999999 ]` would evaluate the literal through the SAME
# wrapping evaluator, so both sides would wrap identically and the check would read "no wrap".
# That is the defect class itself, reappearing in its own test — which is exactly why the
# primitive under test compares strings.
wrapped_count=$(( 10#99999999999999999999 ))
if [ "$wrapped_count" != "99999999999999999999" ]; then
  pass "count-wrap: NON-VACUITY — that value evaluates to $wrapped_count, so a post-arithmetic range check compared a different number"
else
  fail "round4/F2 class: the value must wrap, else the digit cap is unnecessary (got $wrapped_count)"
fi
# ...and the ACCEPT direction.
if ( source "$ARGS_LIB"; require_positive_int reps "3" ) >/dev/null 2>&1; then
  pass "count-wrap: ACCEPT — an ordinary --reps 3 is accepted"
else
  fail "round4/F2 class: --reps 3 must be accepted"
fi

# ===========================================================================
# THE MECHANISM IS USED WHEREVER THE CLASS APPLIES — structurally
# ===========================================================================
# A primitive nobody calls is the #3249 shape. Asserted as a SHAPE rather than by behaviour,
# because "no unvalidated external value reaches `$(( ))`" is a claim about the whole rig and
# behaviour can only sample it: `cpu_range_validate` must not compare a bound with `(( ))` or
# `-gt` on a value it has not already string-checked.
# Located by LINE NUMBER with fixed-string greps (`-F`), never by a regex over shell parameter
# expansion: `${part%%-*}` carries `$`, `{`, `%`, `*` and `}`, so a pattern spelling it is a
# quoting puzzle whose most likely failure is a FALSE FAIL on correct code — the kind of red an
# agent learns to waive. The three lines are asserted to exist and to be in the right ORDER,
# which is the property (both lines present in the wrong order leaves the guard useless).
raw_line=$(grep -nF 'raw_lo="${part%%-*}"' "$LIB" | head -1 | cut -d: -f1)
le_line=$(grep -nF 'decimal_le "$raw_lo"' "$LIB" | head -1 | cut -d: -f1)
conv_line=$(grep -nF 'lo=$((10#$raw_lo))' "$LIB" | head -1 | cut -d: -f1)
if [ -n "$raw_line" ] && [ -n "$le_line" ] && [ -n "$conv_line" ]; then
  pass "wired: cpu_range_validate extracts RAW endpoints (line $raw_line), string-checks them (line $le_line) and only then converts (line $conv_line)"
else
  fail "round7 F2: cpu_range_validate must extract raw endpoints, decimal_le them, then convert (raw=$raw_line le=$le_line conv=$conv_line)"
fi
# The `$((10#…))` conversion must come AFTER the decimal_le refusal, not before it — the ORDER
# IS the fix, and a reordering edit would leave both lines present and the guard useless.
if [ -n "$le_line" ] && [ -n "$conv_line" ] && [ "$le_line" -lt "$conv_line" ]; then
  pass "wired: the decimal_le refusal (line $le_line) precedes the \$(( )) conversion (line $conv_line) — the ORDER is the fix"
else
  fail "round7 F2: the string check must precede the arithmetic (le=$le_line conv=$conv_line)"
fi
# And the primitives must not have been re-implemented in lib-cpu.sh: a second copy is the
# fourth drifting site this mechanism exists to retire.
if ! grep -q '^decimal_le()' "$LIB" && ! grep -q '^decimal_normalize()' "$LIB"; then
  pass "wired: lib-cpu.sh does NOT re-implement the primitives (one owner, so three sites cannot drift)"
else
  fail "round7 F2: the decimal primitives must live only in lib-args.sh"
fi

# ===========================================================================
# A MINIMUM CHECK COUNT for this suite
# ===========================================================================
# `set -uo pipefail` (no `-e`) means a block that silently never executes lowers the count and
# registers NO failure, while the gate reads only the exit code. Deliberately below the current
# count (so adding a case does not red it) and far above zero.
MIN_CHECKS=25
echo
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would otherwise lower the count with no"
  echo "       failure registered, and the gate reads only the exit code (#3272)."
  exit 1
fi
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 numeric-wraparound guard checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks WS0 numeric-wraparound check(s) FAILED"
exit 1

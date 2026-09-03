#!/usr/bin/env bash
# test_ws0_flight_arm_guards.sh — THE FLIGHT ARM'S OWN PIN AND ALLOCATOR (issue #3551).
#
# # Why this is its own suite
#
# `test_ws0_cpu_pinning_guards.sh` was at 1198 lines against the ~1500-line test target and its
# subject is ONE question — are the pinned CPUs one physical core, and is the measured server the
# program we started. This file's subject is the question #3551 adds, which that one cannot
# answer:
#
#     THE TWO ARMS NO LONGER RUN THE SAME WAY, SO WHAT EXACTLY DIFFERS — AND IS THE DIFFERENCE
#     THE ONE THE LABEL CLAIMS?
#
# `--flight-server-cpus` / `--flight-pin-mode` / `--flight-allocator` exist to create §3b step
# 3's missing DRIFT CONTROL: the bare-scan arm stays code-identical AND pin-identical while ONE
# property of the Flight arm moves. That only works if each moved property is VERIFIED rather
# than requested, and every guard here is a case of the same rule:
#
#   * a pin MODE selects between two AFFIRMATIVE assertions, never a relaxation — so
#     `distinct-cores` must REFUSE a sibling pair and `siblings` must REFUSE a distinct-core one,
#     and a single-CPU list (over which "pairwise distinct" compares nothing) is refused too;
#   * `LD_PRELOAD` FAILS OPEN — glibc prints "cannot be preloaded ...: ignored" and CONTINUES
#     with system malloc, exit 0 — so arm C is verified from the RUNNING PROCESS, and the control
#     arm's NEGATIVE is verified as well, because a control arm quietly running jemalloc does not
#     add noise, it INVERTS the comparison;
#   * the report may print only what was recorded, in the vocabulary of the property that was
#     actually read: `physical-core siblings` may NEVER be said about a `distinct-cores` pin.
#
# Per #3249 (a hardcoded `_PERF_STATE="ok"` survived 118/118 tests) the bar is OBSERVED TO FIRE,
# so every refusal case is paired with the ACCEPT direction of the same check — a guard that only
# ever reds is the guard an operator works around — and each refusal is matched on its OWN
# diagnostic rather than on the mere fact of a non-zero exit.
#
# Hermetic: a fake sysfs tree, synthetic `/proc/<pid>/maps` files, synthetic session dirs and a
# few-KB synthetic corpus, all under $TMPDIR. No cargo, perf, taskset, sudo, root, real
# libjemalloc, real server, corpus or network. Every driver invocation goes through
# `ws0_driver_run` (the sanctioned form — `scripts/tests/test_ws0_hermeticity.sh` FAILS on any
# other), so nothing below the driver's argument boundary ever executes.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
CPU_LIB="$REPO_ROOT/scripts/perf/lib-cpu.sh"
MEASURE_LIB="$REPO_ROOT/scripts/perf/lib-measure.sh"
# The flight arm's own library (#3551): the three-valued jemalloc probe, the pin-mode dispatch
# and the /proc/<pid>/maps check. Split out of the driver under the campsite rule.
FLIGHT_LIB="$REPO_ROOT/scripts/perf/lib-flight-arm.sh"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"

fails=0
# `checks` counts what actually RAN (incremented by pass/fail themselves), so the floor at the
# end can see a block that silently never executed — this file has no `set -e`.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

for f in "$DRIVER" "$CPU_LIB" "$MEASURE_LIB" "$FLIGHT_LIB" "$REPORT"; do
  [ -f "$f" ] || { echo "FAIL - missing $f"; exit 1; }
done
# python3 is a HARD REQUIREMENT of this rig (`ws0-baseline.sh` refuses to run without it), so its
# absence is a FAILED CHECK and not a skip: exiting 0 here would record the gate component as
# SUCCESS with none of these checks having run.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig, and a skip"
  echo "       here would report this component SUCCESS with 0 checks run."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

CORPUS_ROWS=1000
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"
# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"
ws0_hermetic_init "$TMP"
make_corpus "$TMP/corpus"

# ===========================================================================
# PART 1 — THE TWO PIN MODES, EACH REFUSING WHAT THE OTHER REQUIRES
# ===========================================================================
# A fake `/sys/devices/system/cpu` describing a 4-physical-core, 8-thread box:
#
#   core 0: cpu0,cpu4   core 1: cpu1,cpu5   core 2: cpu2,cpu6   core 3: cpu3,cpu7
#
# Deliberately NOT the real box's `2,10`, so no case can pass by agreeing with a hardcoded
# default — and on this topology `2,6` is a sibling pair while `2,3` is two distinct cores, which
# is exactly the pair of inputs the two modes must disagree about.
TOPO="$TMP/sys/devices/system/cpu"
for c in 0 1 2 3; do
  for s in "$c" "$((c + 4))"; do
    mkdir -p "$TOPO/cpu$s/topology"
    printf '%s,%s\n' "$c" "$((c + 4))" > "$TOPO/cpu$s/topology/thread_siblings_list"
  done
done

# lib_call <fn> [args…] — source lib-cpu.sh against the FAKE topology in a subshell and run one
# function, so no case inherits another's state.
lib_call() {
  local fn="$1"; shift
  ( export CQLITE_WS0_CPU_TOPOLOGY_ROOT="$TOPO"
    # shellcheck disable=SC1090
    source "$CPU_LIB"
    "$fn" "$@" ) 2>&1
}

# --- 1a. distinct-cores REFUSES a sibling pair, NAMING both CPUs ------------------------------
# The mode's whole purpose is the SMT-unpin contrast, so a set that is really one core's
# hyperthreads measures the thing it is supposed to be measured AGAINST. The refusal must name
# the offending CPUs: "your set is wrong" without saying which two collide leaves the operator
# guessing on an 8-CPU box, let alone a 128-CPU one.
out=$(lib_call verify_distinct_cores "2,6" "flight server"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "SAME physical core" <<<"$out" \
   && grep -q "cpu2" <<<"$out" && grep -q "cpu6" <<<"$out"; then
  pass "distinct-cores REFUSES a sibling pair (2,6) and NAMES both offending CPUs"
else
  fail "distinct-cores must refuse 2,6 naming cpu2 and cpu6 (rc=$rc, out: $out)"
fi

# ...and it must name the sysfs ANSWER it refused on, not merely the argument: the whole reason
# this check is trustworthy is that it READ the topology.
if grep -q "thread_siblings_list is" <<<"$out"; then
  pass "distinct-cores: the refusal quotes the thread_siblings_list it READ (the argument alone would prove nothing)"
else
  fail "distinct-cores: the refusal must quote the sysfs answer (out: $out)"
fi

# --- 1b. THE POSITIVE CONTROL: a genuine distinct-core pair is ACCEPTED -----------------------
# Without this half, a `verify_distinct_cores` hardcoded to `return 1` would satisfy every
# negative case in this file.
out=$(lib_call verify_distinct_cores "2,3" "flight server"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "verified pairwise DISTINCT physical cores" <<<"$out"; then
  pass "distinct-cores ACCEPTS a genuine distinct-core pair (2,3) and says which property it verified"
else
  fail "distinct-cores must accept 2,3 (rc=$rc, out: $out)"
fi
# ...and it ECHOES THE SETS IT READ, one per CPU. That echo is what the driver records into
# pinning-verification.json, so the report's claim rests on the sysfs answer rather than on a
# restatement of the argument (#3272 round 9, F6). For this mode the substance is one set PER
# CPU — that they DIFFER is the property — so both must appear.
if grep -q "cpu2=(2 6)" <<<"$out" && grep -q "cpu3=(3 7)" <<<"$out"; then
  pass "distinct-cores echoes the expanded sibling set of EVERY pinned CPU (the substance the driver records)"
else
  fail "distinct-cores must echo each CPU's sibling set (out: $out)"
fi
# Three distinct cores, so the check is not one that only ever compares two.
out=$(lib_call verify_distinct_cores "0,1,2" "flight server"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "distinct-cores ACCEPTS three pairwise-distinct cores (0,1,2) — the comparison is pairwise, not first-vs-second"
else
  fail "distinct-cores must accept 0,1,2 (rc=$rc, out: $out)"
fi
# ...and a set where the COLLISION IS NOT THE FIRST PAIR still fires: 0,1,5 collides at (1,5),
# which a first-vs-rest comparison would miss.
out=$(lib_call verify_distinct_cores "0,1,5" "flight server"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "cpu1" <<<"$out" && grep -q "cpu5" <<<"$out"; then
  pass "distinct-cores REFUSES a collision that is NOT the first pair (0,1,5 collides at cpu1/cpu5)"
else
  fail "distinct-cores must refuse 0,1,5 naming cpu1 and cpu5 (rc=$rc, out: $out)"
fi

# --- 1c. siblings REFUSES a distinct-core pair — the EXISTING behaviour, PINNED ---------------
# `--flight-pin-mode siblings` is the default, so this is the property every pre-#3551 invocation
# has. Pinned here rather than assumed: a refactor that made the two modes share a code path
# could quietly turn the default into "either is fine", which is the relaxation this design
# explicitly is not.
out=$(lib_call verify_sibling_pair "2,3" "flight server"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "NOT the sibling set of one" <<<"$out"; then
  pass "siblings REFUSES a distinct-core pair (2,3) — the default mode is still an assertion, not a preference"
else
  fail "siblings must refuse 2,3 (rc=$rc, out: $out)"
fi
# ...and ACCEPTS the sibling pair, so the two modes are proved to disagree about the SAME two
# inputs rather than to fail on everything.
out=$(lib_call verify_sibling_pair "2,6" "flight server"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "siblings ACCEPTS 2,6 — so the two modes REQUIRE OPPOSITE things of the same two inputs (2,6 and 2,3 each pass exactly one)"
else
  fail "siblings must accept 2,6 (rc=$rc, out: $out)"
fi

# --- 1d. distinct-cores REFUSES A SINGLE-CPU LIST, with its OWN message -----------------------
# "Pairwise distinct" over one element is trivially TRUE, so a 1-CPU list would satisfy the
# function while expressing nothing — the 0-comparisons vacuous pass this rig refuses everywhere
# else. Its own message matters because the remedy differs from 1a's: name a SECOND CPU, not a
# different one. Asserted as a DISTINCT diagnostic, not merely a refusal.
out=$(lib_call verify_distinct_cores "2" "flight server"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "names only 1 CPU" <<<"$out" && grep -q "VACUOUS" <<<"$out"; then
  pass "distinct-cores REFUSES a single-CPU list with its OWN message (pairwise-distinct over one CPU compares nothing)"
else
  fail "distinct-cores must refuse a 1-CPU list with its own diagnostic (rc=$rc, out: $out)"
fi
# `rc` too, deliberately: without it this check passes VACUOUSLY on a mutant that refuses
# nothing (measured — a disabled n<2 bound left this line green while its primary above failed).
if [ "$rc" -ne 0 ] && ! grep -q "SAME physical core" <<<"$out"; then
  pass "distinct-cores: the single-CPU refusal is NOT the collision message (different cause, different remedy)"
else
  fail "distinct-cores: the 1-CPU case must not reuse the collision diagnostic (out: $out)"
fi

# --- 1e. AN UNREADABLE TOPOLOGY IS A REFUSAL, not "the cores are distinct" --------------------
# The permissive reading of an unmeasurable state is the shape this whole rig refuses.
out=$(lib_call verify_distinct_cores "2,99" "flight server"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "unreadable" <<<"$out"; then
  pass "distinct-cores REFUSES when a CPU's thread_siblings_list cannot be read (could-not-measure is never a pass)"
else
  fail "distinct-cores must refuse an unreadable topology (rc=$rc, out: $out)"
fi

# ===========================================================================
# PART 2 — THE DRIVER'S ARGUMENTS (hermetic, --validate-args-only)
# ===========================================================================

# --- 2a. THE NO-OP-BY-DEFAULT PROPERTY -------------------------------------------------------
# The claim in the flag's own documentation is that omitting `--flight-server-cpus` changes
# NOTHING: the flight arm pins where it always did and the counting domain is unchanged. That is
# a claim about a defaulted value, so it is measured from the stamp rather than believed.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus); rc=$?
if [ "$rc" -eq 0 ] && grep -q "flight-cpus=2,10" <<<"$out" \
   && grep -q "flight-pin-mode=siblings" <<<"$out" && grep -q "flight-allocator=system" <<<"$out"; then
  pass "default: --flight-server-cpus takes --server-cpus' value (2,10) and the mode/allocator default to the pre-#3551 behaviour"
else
  fail "the defaults must equal the server pin (rc=$rc, out: $(tail -3 <<<"$out"))"
fi
if ws0_driver_ran_hermetically; then
  pass "default: that run executed NOTHING (no sudo/cargo/perf/taskset recorded) — the shims are the oracle, and they can record"
else
  fail "the default run must be hermetic (calls: $(ws0_hermetic_calls))"
fi
# ...and it FOLLOWS `--server-cpus` rather than a constant, which is the actual property: a
# hardcoded `2,10` default would satisfy the case above and diverge the moment anyone repins.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus --server-cpus 3,11); rc=$?
if [ "$rc" -eq 0 ] && grep -q "flight-cpus=3,11" <<<"$out"; then
  pass "default: the flight pin FOLLOWS --server-cpus (3,11), so it is a default and not a constant"
else
  fail "the flight default must follow --server-cpus (rc=$rc, out: $(tail -3 <<<"$out"))"
fi
# ...and the ORDER OF ARGUMENTS must not matter: the loop is order-independent, so resolving the
# default inside it would make `--flight-server-cpus X --server-cpus Y` silently disagree with
# the flag written last.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus --flight-server-cpus 4,12 --server-cpus 3,11); rc=$?
if [ "$rc" -eq 0 ] && grep -q "flight-cpus=4,12" <<<"$out"; then
  pass "explicit: an explicit --flight-server-cpus wins over the default even when --server-cpus comes AFTER it (the loop is order-independent)"
else
  fail "an explicit flight pin must survive a later --server-cpus (rc=$rc, out: $(tail -3 <<<"$out"))"
fi

# --- 2b. AN UNKNOWN ENUM VALUE IS A USAGE ERROR, NEVER A DEFAULT -----------------------------
# Which property was asserted IS the content of `--flight-pin-mode`, so a value nobody planned
# for cannot silently become one of the two: it would assert the opposite of what was asked.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus --flight-pin-mode sibblings); rc=$?
if [ "$rc" -eq 2 ] && grep -q "flight-pin-mode must be siblings|distinct-cores" <<<"$out"; then
  pass "an unknown --flight-pin-mode exits 2 naming the legal values (never a fall-back to the default)"
else
  fail "--flight-pin-mode sibblings must exit 2 (rc=$rc, out: $(head -2 <<<"$out"))"
fi
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus --flight-allocator tcmalloc); rc=$?
if [ "$rc" -eq 2 ] && grep -q "flight-allocator must be system|jemalloc" <<<"$out"; then
  pass "an unknown --flight-allocator exits 2 naming the legal values"
else
  fail "--flight-allocator tcmalloc must exit 2 (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the ACCEPT direction of the same two arms, so neither is a guard that refuses every value.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus --flight-pin-mode distinct-cores --flight-server-cpus 2,3); rc=$?
if [ "$rc" -eq 0 ] && grep -q "flight-pin-mode=distinct-cores" <<<"$out"; then
  pass "--flight-pin-mode distinct-cores is ACCEPTED and recorded in the stamp (the accept half)"
else
  fail "distinct-cores must be accepted at the argument boundary (rc=$rc, out: $(tail -3 <<<"$out"))"
fi

# --- 2c. THE JEMALLOC LIBRARY IS RESOLVED BEFORE ANY MEASUREMENT -----------------------------
# "Refusing a value after acting on it is not refusing it" — the rule `--bin-dir` and
# `--profile-out` already follow. A nonexistent library has NO reachable success (glibc would
# ignore the preload and run system malloc under arm C's label), so it must be refused at the
# argument boundary, with the remedy named.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus \
        --flight-allocator jemalloc --jemalloc-lib "$TMP/no-such-libjemalloc.so.2"); rc=$?
if [ "$rc" -eq 2 ] && grep -q "is absent, not a readable regular file" <<<"$out" \
   && grep -q "apt-get install -y libjemalloc2" <<<"$out"; then
  pass "jemalloc: a NONEXISTENT --jemalloc-lib is refused at the ARGUMENT boundary, with the install remedy named"
else
  fail "an absent --jemalloc-lib must be refused with a remedy (rc=$rc, out: $(head -3 <<<"$out"))"
fi
if ws0_driver_ran_hermetically; then
  pass "jemalloc: that refusal happened before ANY execution (no build, no sysctl, no cache drop) — measured from the shims"
else
  fail "the jemalloc refusal must precede every side effect (calls: $(ws0_hermetic_calls))"
fi
# THE THIRD VALUE: a path that EXISTS but is not a usable regular file is a COULD-NOT-MEASURE
# state, refused NAMING the state rather than folded onto "absent". The two have different
# remedies, and the permissive reading of either is a run labelled jemalloc that measured system
# malloc.
mkdir -p "$TMP/libdir-not-a-file"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus \
        --flight-allocator jemalloc --jemalloc-lib "$TMP/libdir-not-a-file"); rc=$?
if [ "$rc" -eq 2 ] && grep -q "is not-a-regular-file" <<<"$out"; then
  pass "jemalloc: a --jemalloc-lib that exists but is a DIRECTORY is refused NAMING that state (three-valued, not 'absent')"
else
  fail "a non-regular --jemalloc-lib must be refused naming the state (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# A DANGLING SYMLINK is the case an `-e`-first probe reads as plain absence. Its remedy is "fix
# the broken install", not "install it", so it gets its own state.
ln -s "$TMP/no-such-target.so" "$TMP/dangling-libjemalloc.so.2"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus \
        --flight-allocator jemalloc --jemalloc-lib "$TMP/dangling-libjemalloc.so.2"); rc=$?
if [ "$rc" -eq 2 ] && grep -q "is dangling-symlink" <<<"$out"; then
  pass "jemalloc: a DANGLING SYMLINK is refused as such (an -e-first probe would have called it plain absence)"
else
  fail "a dangling --jemalloc-lib symlink must be named (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# THE ACCEPT DIRECTION: a readable regular file is accepted and RECORDED in the stamp, so the
# refusals above are not a check that rejects every path. A synthetic file is enough — the
# driver's job here is to establish the path is usable, and whether it really contains jemalloc
# is what the per-rep mapping read in PART 3 answers.
printf 'not really a library\n' > "$TMP/fake-libjemalloc.so.2"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus \
        --flight-allocator jemalloc --jemalloc-lib "$TMP/fake-libjemalloc.so.2"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "jemalloc-lib=\[$TMP/fake-libjemalloc.so.2\]" <<<"$out"; then
  pass "jemalloc: a readable regular --jemalloc-lib is ACCEPTED and its path recorded in the stamp (the accept half)"
else
  fail "a usable --jemalloc-lib must be accepted and stamped (rc=$rc, out: $(tail -3 <<<"$out"))"
fi
# ...and the SYSTEM arm records an AFFIRMATIVE "none", not an empty field: "no library" and
# "nobody wrote the field down" must not look the same in an artifact the report cites.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus); rc=$?
if grep -q "jemalloc-lib=\[none (system malloc" <<<"$out"; then
  pass "system: the library is recorded as an affirmative 'none (system malloc…)' rather than an empty field"
else
  fail "the system arm must record an affirmative none (out: $(tail -3 <<<"$out"))"
fi

# --- 2d. THE OVERLAP AND ONLINE CHECKS ON THE FLIGHT LIST ------------------------------------
# These run BELOW the driver's argument boundary — they need a real `thread_siblings_list` — so
# they are driven the way the SERVER set's are: the shipped functions against the fake topology,
# plus a STRUCTURAL assertion that the driver actually CALLS them with the flight list. The
# residual is stated rather than hidden: this file proves the check refuses and proves the call
# exists, and does NOT execute the driver's own call site (no self-test on this branch can,
# without a real host topology and a real port).
out=$(lib_call verify_disjoint "2,3" "3,7"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "overlap on cpu3" <<<"$out"; then
  pass "overlap: a flight pin sharing cpu3 with the client set is REFUSED (the client's own cost would land inside the counted window)"
else
  fail "verify_disjoint must refuse an overlapping flight/client pair (rc=$rc, out: $out)"
fi
out=$(lib_call verify_disjoint "2,3" "4,5"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "overlap: a disjoint flight/client pair is ACCEPTED (the accept half)"
else
  fail "verify_disjoint must accept a disjoint pair (rc=$rc, out: $out)"
fi
if grep -qF 'verify_disjoint "$FLIGHT_SERVER_CPUS" "$CLIENT_CPUS"' "$FLIGHT_LIB" \
   && grep -qF 'verify_cpus_online "$FLIGHT_SERVER_CPUS" "flight server"' "$FLIGHT_LIB" \
   && grep -qF 'verify_distinct_cores "$FLIGHT_SERVER_CPUS" "flight server"' "$FLIGHT_LIB" \
   && grep -qF 'verify_sibling_pair "$FLIGHT_SERVER_CPUS" "flight server"' "$FLIGHT_LIB"; then
  pass "wiring (STRUCTURAL): the FLIGHT list goes through the online check, BOTH pin-mode assertions and the disjointness check"
else
  fail "wiring: the flight list must be verified four ways in lib-flight-arm.sh"
fi
# ...and the DRIVER must CALL it, at the point in the sequence where it belongs. The library
# holds the checks; only the driver can place them after the server pin and before any
# measurement, and a library nobody calls verifies nothing.
if grep -qF 'verify_flight_arm_pin || exit 2' "$DRIVER" \
   && grep -qF 'record_flight_allocator_facts || exit 2' "$DRIVER"; then
  pass "wiring (STRUCTURAL): the driver CALLS both flight-arm entry points and exits 2 on either refusal (a library nobody calls verifies nothing)"
else
  fail "wiring: the driver must call verify_flight_arm_pin and record_flight_allocator_facts, fail-closed"
fi
# ...and the BARE SCAN must stay on `$SERVER_CPUS`, which is the entire drift-control argument:
# if the scan followed the flight pin there would be no pin-identical leg left to compare against.
if grep -qF 'taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench"' "$MEASURE_LIB" \
   && grep -qF 'taskset -c "$FLIGHT_SERVER_CPUS" "$BIN/cqlite-flight"' "$MEASURE_LIB"; then
  pass "wiring (STRUCTURAL): the bare scan still tasksets to \$SERVER_CPUS while the Flight server tasksets to \$FLIGHT_SERVER_CPUS — the drift control stays pin-identical"
else
  fail "wiring: measure_scan must keep \$SERVER_CPUS and measure_flight must use \$FLIGHT_SERVER_CPUS"
fi
# ...and the COUNTING DOMAIN must follow the arm. Counting the server set while the Flight server
# ran elsewhere would divide another core's cycles by this rep's rows — silently.
if grep -qF 'PERF_COUNT_CPUS="$SERVER_CPUS"' "$MEASURE_LIB" \
   && grep -qF 'PERF_COUNT_CPUS="$FLIGHT_SERVER_CPUS"' "$MEASURE_LIB" \
   && grep -qF -e '-C "$PERF_COUNT_CPUS"' "$DRIVER"; then
  pass "wiring (STRUCTURAL): each leg sets the CPU-wide counting domain to its OWN server's CPUs, and the perf wrapper reads it"
else
  fail "wiring: the counting domain must follow the arm (PERF_COUNT_CPUS in both legs, read by the wrapper)"
fi
# ...and LD_PRELOAD must be set on EVERY flight launch — empty on the system arm — rather than
# only when jemalloc is requested: an inherited value would put the CONTROL arm on the allocator
# under test, which inverts the comparison instead of adding noise.
if grep -qF 'LD_PRELOAD="$preload" taskset -c "$FLIGHT_SERVER_CPUS"' "$MEASURE_LIB"; then
  pass "wiring (STRUCTURAL): the flight launch ALWAYS sets LD_PRELOAD (empty on the system arm), so an inherited value cannot reach the control arm"
else
  fail "wiring: measure_flight must always set LD_PRELOAD on the server launch"
fi

# ===========================================================================
# PART 2e — THE COUNTING DOMAIN FAILS CLOSED (#3551, the fabricated-win defect)
# ===========================================================================
# `perf stat -C <list>` counting a list the measured work did not run on is not noise: pin the
# Flight server to `2,3` while counting `2,10` and the window collects cpu10's IDLE and misses
# cpu3's WORK, so the SAME rows cost FEWER cycles and the arm reads as a large win. Nothing in
# the output says so, which is why the wrapper VALIDATES the pairing rather than trusting the
# leg that set it.
#
# Driven against the wrapper EXTRACTED FROM THE SHIPPED DRIVER (the same `awk` extraction
# test_ws0_perf_invocation_lint.sh uses for the argv guard), with `perf` shimmed to a function
# that only PRINTS: no perf, no root, no measurement. The extraction is the point — a copy of the
# wrapper written here would keep passing after the shipped one changed.
wrapper_probe() { # wrapper_probe <counted> <pairing-table> <argv…>
  local counted="$1" table="$2"; shift 2
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$REPO_ROOT/scripts/perf/lib-perf-lint.sh"   # supplies $_PP_SHORT/$_PP_LONG
    EVENTS="cycles"; PERF_COUNT_CPUS="$counted"; WS0_PERF_COUNT_PAIRINGS="$table"
    perf() { printf 'PERF-RAN: %s\n' "$*"; }
    eval "$(awk '/^perf_stat_c\(\)/,/^}/' "$DRIVER")"
    perf_stat_c /dev/null "$@" ) 2>&1
}
# The table a real session with a DIFFERENT flight pin derives: the bare scan counts where it
# runs, and the Flight window counts the SERVER while bracketing the CLIENT.
PAIR_TABLE="2,10|2,10"$'\n'"2,3|4,12"

# --- 2e-1. THE RED ARM: the Flight window still counting $SERVER_CPUS ------------------------
# The planted defect is exactly one property away from the control below: the counted list, and
# nothing else. It must be REFUSED and the diagnostic must NAME BOTH lists — a bare red is not
# evidence, since an unrelated breakage in a 60-line wrapper produces an identical exit code.
out=$(wrapper_probe "2,10" "$PAIR_TABLE" taskset -c 4,12 flight-loadgen --shape full); rc=$?
if [ "$rc" -ne 0 ] && grep -q "'2,10'" <<<"$out" && grep -q "'4,12'" <<<"$out" \
   && ! grep -q 'PERF-RAN' <<<"$out"; then
  pass "counting domain RED ARM: a Flight window counting \$SERVER_CPUS (2,10) while bracketing the client (4,12) is REFUSED, naming BOTH lists, and perf never runs"
else
  fail "the mispaired counting domain must be refused naming both lists (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the refusal must say WHY it cannot be waved through — that the error is invisible in the
# output and flatters the arm — because the reflex response to a confusing guard is to bypass it.
if grep -q "FEWER cycles" <<<"$out" && grep -q "verified pairings" <<<"$out"; then
  pass "counting domain RED ARM: the refusal states the DIRECTION of the error and prints the verified pairings it checked against"
else
  fail "the refusal must state the direction and the table (out: $(head -8 <<<"$out"))"
fi

# --- 2e-2. THE POSITIVE CONTROL, differing in ONE property: the counted list -----------------
# Identical argv, identical table; the counted list is the FLIGHT pin. perf must run, CPU-wide,
# with that list — otherwise 2e-1 proves only that this wrapper refuses everything.
out=$(wrapper_probe "2,3" "$PAIR_TABLE" taskset -c 4,12 flight-loadgen --shape full); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'PERF-RAN: stat -x, -e cycles -C 2,3 -o /dev/null -- taskset -c 4,12 flight-loadgen --shape full' <<<"$out"; then
  pass "counting domain CONTROL: the SAME argv with the FLIGHT pin as the counted list is accepted and reaches perf as -C 2,3 (one property apart from the RED arm)"
else
  fail "the correct flight pairing must reach perf (rc=$rc, out: $out)"
fi
# ...and the bare-scan pairing, where the counted list and the argv's affinity are the SAME. A
# rule of "counted == taskset list" would have accepted this and RED the correct Flight rep
# above, which is why the check is a closed PAIRING TABLE and not an equality.
out=$(wrapper_probe "2,10" "$PAIR_TABLE" taskset -c 2,10 ws0-scan-bench --passes 1); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'PERF-RAN: stat -x, -e cycles -C 2,10' <<<"$out"; then
  pass "counting domain CONTROL: the BARE-SCAN pairing (counted == the measured process's own affinity) is accepted — the two legitimate shapes differ, so the check is a table and not an equality"
else
  fail "the bare-scan pairing must be accepted (rc=$rc, out: $out)"
fi

# --- 2e-3. AN UNCHECKABLE DOMAIN IS A REFUSAL, and there is NO DEFAULT ----------------------
# "An unset value must never inherit $SERVER_CPUS": a silent default is precisely how this
# defect would survive its own fix, so an empty domain, an empty table and an argv whose command
# is not pinned are each named refusals.
out=$(wrapper_probe "" "$PAIR_TABLE" taskset -c 2,10 /bin/true); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no counting domain" <<<"$out" && ! grep -q 'PERF-RAN' <<<"$out"; then
  pass "counting domain: an EMPTY/unset \$PERF_COUNT_CPUS is a NAMED refusal, never an inherited \$SERVER_CPUS"
else
  fail "an empty counting domain must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
out=$(wrapper_probe "2,10" "" taskset -c 2,10 /bin/true); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no verified counting-domain table" <<<"$out"; then
  pass "counting domain: an absent pairing TABLE is refused — a domain that cannot be checked against the session's verified pins is not a checked domain"
else
  fail "an absent pairing table must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
out=$(wrapper_probe "2,10" "$PAIR_TABLE" /bin/true); rc=$?
if [ "$rc" -ne 0 ] && grep -q "carries no 'taskset -c" <<<"$out"; then
  pass "counting domain: an argv with no 'taskset -c <list>' is refused — WHERE the measured command runs is then unknowable, and an unverifiable pairing is not a verified one"
else
  fail "an unpinned argv must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the OPTION allowlist still fires FIRST, so the new checks did not displace the argv
# guard (a caller-supplied option must be refused whatever the domain says).
out=$(wrapper_probe "2,10" "$PAIR_TABLE" -p1234 taskset -c 2,10 /bin/true); rc=$?
if [ "$rc" -ne 0 ] && grep -q "was passed the perf option" <<<"$out"; then
  pass "counting domain: the pre-existing OPTION allowlist still fires first (the new validation did not displace layer 3)"
else
  fail "the argv option guard must still fire (rc=$rc, out: $(head -2 <<<"$out"))"
fi

# --- 2e-4. THE WIRING: the table is DERIVED, and each leg sets its own domain ----------------
# The table must come from the lists this session VERIFIED, never from a literal: a hand-written
# table could name a pin nobody checked, which is the F6 shape one layer down.
if grep -qF 'WS0_PERF_COUNT_PAIRINGS="$SERVER_CPUS|$SERVER_CPUS"' "$DRIVER" \
   && grep -qF '"$FLIGHT_SERVER_CPUS|$CLIENT_CPUS"' "$DRIVER"; then
  pass "wiring (STRUCTURAL): the pairing table is DERIVED from the verified server/flight/client lists, not written out"
else
  fail "wiring: the pairing table must be derived from the verified lists"
fi
# ...and each leg assigns its own domain on the line IMMEDIATELY BEFORE its window, so the
# assignment and the argv it must agree with cannot drift apart.
if python3 - "$MEASURE_LIB" <<'PY'
import sys
lines = open(sys.argv[1]).read().split("\n")
calls = [i for i, l in enumerate(lines) if l.strip().startswith("perf_stat_c ")]
assert calls, "no perf_stat_c call sites found in the measurement legs"
def preceding_statement(i):
    # Walk UP past comments and blank lines: what must abut the call is the STATEMENT before it,
    # and this rig's idiom puts a paragraph of reasoning between the two. Skipping comments is
    # what makes the assert about code order rather than about comment length.
    j = i - 1
    while j >= 0 and (not lines[j].strip() or lines[j].lstrip().startswith("#")):
        j -= 1
    return lines[j] if j >= 0 else ""
bad = [i + 1 for i in calls if "PERF_COUNT_CPUS=" not in preceding_statement(i)]
if bad:
    print(f"perf_stat_c call sites with no counting domain set just above: lines {bad}",
          file=sys.stderr)
    raise SystemExit(1)
PY
then
  pass "wiring (STRUCTURAL): EVERY perf_stat_c call site in the measurement legs sets \$PERF_COUNT_CPUS on the line above it (all 3 of them), so no window inherits another arm's domain"
else
  fail "wiring: each perf_stat_c call must be preceded by its own PERF_COUNT_CPUS assignment"
fi

# ===========================================================================
# PART 3 — WHICH ALLOCATOR IS THE SERVER PROCESS ACTUALLY RUNNING?
# ===========================================================================
# `verify_flight_allocator_mapping` takes the maps PATH as a parameter precisely so this can be
# driven with no server, no root and no real jemalloc — including the branch the check EXISTS for
# (the mapping ABSENT), which is the non-zero-count half no "does it run" test can reach.
# maps_call <maps> <environ> <mode> <lib-path> <arena> <tag>
maps_call() {
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$FLIGHT_LIB"
    verify_flight_server_allocator "$@" ) 2>&1
}
MAPS_WITH="$TMP/maps-with-jemalloc"
MAPS_WITHOUT="$TMP/maps-without-jemalloc"
MAPS_EMPTY="$TMP/maps-empty"
JLIB="/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
# The ENVIRON side, NUL-separated exactly as /proc/<pid>/environ is (#3551 item 9). `printf` with
# `\0` writes real NUL bytes, so the shipped reader's `read -r -d ''` split is exercised rather
# than a newline-separated stand-in — and the LAST entry deliberately has NO trailing NUL in one
# of them, the shape that makes a naive loop drop the final variable.
ENV_PRELOAD="$TMP/environ-preload"
ENV_CLEAN="$TMP/environ-clean"
ENV_ARENA1="$TMP/environ-arena1"
ENV_ARENA16="$TMP/environ-arena16"
ENV_PRELOAD_EMPTY="$TMP/environ-preload-empty"
ENV_EMPTY="$TMP/environ-empty"
printf 'PATH=/usr/bin\0LD_PRELOAD=%s\0HOME=/root\0' "$JLIB" > "$ENV_PRELOAD"
printf 'PATH=/usr/bin\0HOME=/root\0' > "$ENV_CLEAN"
printf 'PATH=/usr/bin\0MALLOC_ARENA_MAX=1\0HOME=/root' > "$ENV_ARENA1"
printf 'PATH=/usr/bin\0MALLOC_ARENA_MAX=16\0HOME=/root' > "$ENV_ARENA16"
printf 'PATH=/usr/bin\0LD_PRELOAD=\0HOME=/root\0' > "$ENV_PRELOAD_EMPTY"
: > "$ENV_EMPTY"
{
  printf '7f0000-7f0100 r-xp 00000000 08:01 101 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2\n'
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
} > "$MAPS_WITH"
{
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
  printf '7f0400-7f0500 rw-p 00000000 00:00 0  [heap]\n'
} > "$MAPS_WITHOUT"
: > "$MAPS_EMPTY"
# --- THE FIELD-PARSE FIXTURES (roborev #3551 round 3 F3) ------------------------------------
# Each is the SHAPE of one false verdict the whole-line substring match produced, or one line
# shape a field parse has to survive. They differ from $MAPS_WITHOUT / $MAPS_WITH in exactly the
# property their case names.
MAPS_JEM_DIR="$TMP/maps-jemalloc-in-directory"
MAPS_JEM_OTHER="$TMP/maps-other-jemalloc-library"
MAPS_JEM_DELETED="$TMP/maps-jemalloc-deleted"
MAPS_JEM_NEEDLE_IN_DIR="$TMP/maps-needle-in-directory"
MAPS_ANON_ONLY="$TMP/maps-anon-and-pseudo-only"
MAPS_SPACED="$TMP/maps-spaced-pathname"
MAPS_UNPARSEABLE="$TMP/maps-unparseable-line"
# A mapping whose PARENT DIRECTORY contains `jemalloc` and whose FILE does not. The control arm
# was refused on this line; the file is `libfoo.so`, so nothing jemalloc is mapped.
{
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
  printf '7f0600-7f0700 r-xp 00000000 08:01 105 /opt/jemalloc-build/libfoo.so\n'
} > "$MAPS_JEM_DIR"
# A jemalloc-NAMED library that is NOT the requested one. The jemalloc arm must refuse it.
{
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
  printf '7f0800-7f0900 r-xp 00000000 08:01 106 /usr/lib/libjemalloc-not-the-one.so\n'
} > "$MAPS_JEM_OTHER"
# The requested library, unlinked after being mapped: still in effect.
{
  printf '7f0a00-7f0b00 r-xp 00000000 08:01 107 %s (deleted)\n' "$JLIB"
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
} > "$MAPS_JEM_DELETED"
# A DIRECTORY spelt like the requested library's basename, holding a different file. The
# whole-line match accepted this for the jemalloc arm.
{
  printf '7f0c00-7f0d00 r-xp 00000000 08:01 108 /opt/%s/libunrelated.so\n' "${JLIB##*/}"
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
} > "$MAPS_JEM_NEEDLE_IN_DIR"
# Only mappings with NO pathname field and a pseudo-path: nothing to compare, and the file is
# non-empty, so this is neither an empty read nor an assertion about a path.
{
  printf '7f0e00-7f0f00 rw-p 00000000 00:00 0 \n'
  printf '7f1000-7f1100 rw-p 00000000 00:00 0\n'
  printf '7f1200-7f1300 r-xp 00000000 00:00 0                          [vdso]\n'
  printf '7f1400-7f1500 rw-p 00000000 00:00 0  [heap]\n'
} > "$MAPS_ANON_ONLY"
# A pathname CONTAINING SPACES (the kernel escapes only newlines in this file), whose basename
# is the requested library's.
{
  printf '7f1600-7f1700 r-xp 00000000 08:01 109 /opt/my libs/%s\n' "${JLIB##*/}"
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
} > "$MAPS_SPACED"
# A non-blank line that is not a mapping at all, plus one blank line (which is not a mapping and
# cannot hide one, so it must be ignored rather than refused).
{
  printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'
  printf '\n'
  printf 'this is not a mapping line\n'
} > "$MAPS_UNPARSEABLE"

# --- 3a. THE BRANCH THIS CHECK EXISTS FOR: the preload was IGNORED ---------------------------
# glibc prints "object … cannot be preloaded …: ignored" and CONTINUES with system malloc, exit
# 0, server healthy, every row served. Without this read, arm C would be a byte-identical
# duplicate of arm B under a label saying otherwise — and the two arms would AGREE, which reads
# as a result. The maps file here is non-empty and carries other libraries, so this is the
# genuine "mappings were read and none of them is jemalloc" case rather than an unreadable file.
out=$(maps_call "$MAPS_WITHOUT" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "NO mapping of 'libjemalloc.so.2' is present" <<<"$out" \
   && grep -q "flight-merge-warm-1" <<<"$out" && grep -q "FAILS OPEN" <<<"$out"; then
  pass "jemalloc arm: an ABSENT jemalloc mapping is FATAL, names the REP, and states why (LD_PRELOAD fails open)"
else
  fail "an absent jemalloc mapping must be fatal naming the rep (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the count of mappings it read is reported, so the refusal is distinguishable from one
# that looked at nothing.
if grep -q "2 mappings read" <<<"$out"; then
  pass "jemalloc arm: the refusal states HOW MANY mappings it read (a refusal from a scan of nothing is a different fact)"
else
  fail "the refusal must report the mapping count (out: $(head -3 <<<"$out"))"
fi

# --- 3b. THE ACCEPT DIRECTION, with the observed mapping as the evidence ---------------------
out=$(maps_call "$MAPS_WITH" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "jemalloc VERIFIED for flight-merge-warm-1" <<<"$out" \
   && grep -q "libjemalloc.so.2" <<<"$out"; then
  pass "jemalloc arm: a PRESENT mapping is accepted and the evidence is the mapping LINE itself (recorded per rep)"
else
  fail "a present jemalloc mapping must be accepted with its evidence (rc=$rc, out: $out)"
fi

# --- 3c. THE CONTROL ARM'S NEGATIVE, both directions -----------------------------------------
# Not the weaker half: an operator with LD_PRELOAD exported would have the CONTROL arm running
# the allocator under test, which INVERTS the comparison the whole session exists to make.
out=$(maps_call "$MAPS_WITH" "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "HAS a" <<<"$out" && grep -q "jemalloc mapping" <<<"$out" \
   && grep -q "INVERTS" <<<"$out"; then
  pass "system arm: a jemalloc mapping in the CONTROL arm is REFUSED (it inverts the comparison rather than adding noise)"
else
  fail "the system arm must refuse a jemalloc mapping (rc=$rc, out: $(head -3 <<<"$out"))"
fi
out=$(maps_call "$MAPS_WITHOUT" "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "system VERIFIED for flight-bypass-warm-1" <<<"$out"; then
  pass "system arm: a clean process is accepted, and the NEGATIVE is stated as observed rather than assumed"
else
  fail "the system arm must accept a clean process (rc=$rc, out: $out)"
fi

# --- 3d. COULD-NOT-MEASURE IS A REFUSAL, and each cause is its own message -------------------
# The three-valued rule, at the one place a two-valued `grep -q` would have read "the file could
# not be scanned" as "no jemalloc mapping is present" — i.e. as a PASS on the control arm.
out=$(maps_call "$TMP/no-such-maps" "$ENV_CLEAN" system "" "" flight-bypass-warm-2); rc=$?
if [ "$rc" -ne 0 ] && grep -q "does not exist" <<<"$out" && grep -q "COULD" <<<"$out"; then
  pass "maps ABSENT (the process exited) is COULD-NOT-MEASURE and refuses — never 'no jemalloc mapping present'"
else
  fail "an absent maps file must refuse as could-not-measure (rc=$rc, out: $(head -2 <<<"$out"))"
fi
out=$(maps_call "$MAPS_EMPTY" "$ENV_CLEAN" system "" "" flight-bypass-warm-2); rc=$?
if [ "$rc" -ne 0 ] && grep -q "readable but EMPTY" <<<"$out"; then
  pass "maps EMPTY is its OWN could-not-measure refusal (a live process always publishes mappings, so an empty read is a failed measurement)"
else
  fail "an empty maps file must refuse with its own message (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the same for the jemalloc arm, so neither direction inherits the permissive branch.
out=$(maps_call "$MAPS_EMPTY" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-2); rc=$?
if [ "$rc" -ne 0 ] && grep -q "readable but EMPTY" <<<"$out"; then
  pass "maps EMPTY refuses on the JEMALLOC arm too (an unmeasurable state is not evidence in either direction)"
else
  fail "an empty maps file must refuse on the jemalloc arm (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# An unknown mode refuses rather than picking a direction to assert.
out=$(maps_call "$MAPS_WITH" "$ENV_CLEAN" tcmalloc "" "" t1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "unknown mode 'tcmalloc'" <<<"$out"; then
  pass "an unknown allocator mode refuses rather than asserting one of the two directions"
else
  fail "an unknown mode must refuse (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the jemalloc arm with NO basename to look for refuses rather than asserting nothing.
out=$(maps_call "$MAPS_WITH" "$ENV_PRELOAD" jemalloc "" "" t1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no library path was passed" <<<"$out"; then
  pass "the jemalloc arm with no library path refuses (a check with nothing to look for would pass whatever the process is running)"
else
  fail "the jemalloc arm must refuse an empty library path (rc=$rc, out: $(head -2 <<<"$out"))"
fi

# --- 3e. THE ENVIRON HALF: what the process RECEIVED (#3551 item 9) --------------------------
# `maps` proves the preload TOOK EFFECT; it cannot prove the process was GIVEN anything, and it
# cannot see an arena cap AT ALL (a cap leaves no mapping). So both files are read, and each half
# is asserted on its own.
out=$(maps_call "$MAPS_WITH" "$ENV_CLEAN" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "did not" <<<"$out" && grep -q "RECEIVE the preload" <<<"$out" \
   && grep -q "ABSENT" <<<"$out"; then
  pass "environ half: a jemalloc arm whose process never RECEIVED LD_PRELOAD is refused, even though a jemalloc mapping IS present (maps alone would have passed it)"
else
  fail "an absent LD_PRELOAD entry must be refused on the jemalloc arm (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and a preload of a DIFFERENT library is refused: the comparison is on the whole entry's
# VALUE, not on the presence of the variable.
out=$(maps_call "$MAPS_WITH" "$ENV_PRELOAD" jemalloc "/opt/other/libjemalloc.so.2" "" flight-merge-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "/opt/other/libjemalloc.so.2" <<<"$out"; then
  pass "environ half: a preload of a DIFFERENT library path is refused (an exact VALUE comparison, not a presence test)"
else
  fail "a mismatched LD_PRELOAD value must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the CONTROL arm's environ negative: a non-empty inherited LD_PRELOAD is refused even
# when no jemalloc mapping is present (the operator's stray preload of anything else).
out=$(maps_call "$MAPS_WITHOUT" "$ENV_PRELOAD" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "RECEIVED" <<<"$out" && grep -q "INVERTS" <<<"$out"; then
  pass "environ half: the CONTROL arm refuses a non-empty LD_PRELOAD it RECEIVED, with no jemalloc mapping needed to notice"
else
  fail "the system arm must refuse a received LD_PRELOAD (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...while an EMPTY `LD_PRELOAD=` entry — which is what the launch itself sets on the system arm
# — is ACCEPTED. The assertion is on the VALUE, affirmatively, and a guard that red on the rig's
# own launch line is the guard nobody keeps.
out=$(maps_call "$MAPS_WITHOUT" "$ENV_PRELOAD_EMPTY" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "system VERIFIED" <<<"$out"; then
  pass "environ half: an EMPTY LD_PRELOAD= entry (what the system arm's own launch sets) is ACCEPTED — the check is on the value, not the variable's presence"
else
  fail "an empty LD_PRELOAD must be accepted on the system arm (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and an EMPTY environ is COULD-NOT-MEASURE, distinctly from an empty maps.
out=$(maps_call "$MAPS_WITHOUT" "$ENV_EMPTY" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "was readable but EMPTY" <<<"$out" && grep -q "environ" <<<"$out"; then
  pass "environ half: an EMPTY environ is its OWN could-not-measure refusal (a live process always has a non-empty environment)"
else
  fail "an empty environ must refuse (rc=$rc, out: $(head -2 <<<"$out"))"
fi
out=$(maps_call "$MAPS_WITHOUT" "$TMP/no-such-environ" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "does not exist" <<<"$out"; then
  pass "environ half: an ABSENT environ refuses (the process exited before it could be read)"
else
  fail "an absent environ must refuse (rc=$rc, out: $(head -2 <<<"$out"))"
fi

# --- 3g. THE PATHNAME IS A FIELD, NOT A SUBSTRING OF THE LINE (roborev #3551 round 3 F3) ----
# The shipped check matched `$needle` and `jemalloc` against the WHOLE maps line, which is a
# substring over a namespace the subject also occupies — false in BOTH directions, and both are
# pinned here. Every arm below differs from its control in exactly ONE property: which pathname
# the fixture's second mapping carries.

# 3g-1. THE FALSE RED, which is the half an operator would have had to work around: the CONTROL
# arm with a mapping whose PARENT DIRECTORY is spelt `jemalloc-build` and whose FILE is
# `libfoo.so`. Nothing jemalloc is mapped, so it must be ACCEPTED. This is a POSITIVE control:
# the pre-fix code refused it.
out=$(maps_call "$MAPS_JEM_DIR" "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "system VERIFIED for flight-bypass-warm-1" <<<"$out"; then
  pass "3g-1. system arm: /opt/jemalloc-build/libfoo.so is ACCEPTED — the BASENAME is the library's name, and a parent directory spelt like one is not a jemalloc mapping (the pre-fix false RED)"
else
  fail "3g-1. a jemalloc-named DIRECTORY must not refuse the control arm (rc=$rc, out: $(head -4 <<<"$out"))"
fi
# ...and the discriminating control, differing in exactly that one property: move the token from
# the directory into the FILE name and the same arm refuses, NAMING the offending pathname.
out=$(maps_call "$MAPS_JEM_OTHER" "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "HAS a" <<<"$out" \
   && grep -qF "/usr/lib/libjemalloc-not-the-one.so" <<<"$out"; then
  pass "3g-1. system arm: the same fixture with the token in the FILE name (libjemalloc-not-the-one.so) IS refused, and the refusal NAMES that pathname — so 3g-1 is a basename decision, not a deleted check"
else
  fail "3g-1. a jemalloc-named FILE must refuse the control arm, naming it (rc=$rc, out: $(head -4 <<<"$out"))"
fi

# 3g-2. THE FALSE ACCEPT: the jemalloc arm with NO mapping of the requested library, only an
# unrelated library whose name merely CONTAINS `jemalloc`. The pre-fix line match would not even
# need that much — the requested basename anywhere in the line sufficed — so both spellings are
# driven: a differently-named jemalloc library, and a DIRECTORY named exactly like the requested
# library holding some other file.
out=$(maps_call "$MAPS_JEM_OTHER" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "NO mapping of '${JLIB##*/}' is present" <<<"$out"; then
  pass "3g-2. jemalloc arm: a DIFFERENT jemalloc-named library does NOT satisfy the requested one — the identity is the requested library, not the token"
else
  fail "3g-2. an unrelated jemalloc library must not satisfy the jemalloc arm (rc=$rc, out: $(head -4 <<<"$out"))"
fi
out=$(maps_call "$MAPS_JEM_NEEDLE_IN_DIR" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1)
rc=$?
if [ "$rc" -ne 0 ] && grep -q "NO mapping of '${JLIB##*/}' is present" <<<"$out"; then
  pass "3g-2. jemalloc arm: a DIRECTORY named '${JLIB##*/}' holding libunrelated.so does NOT satisfy it either (the whole-line match accepted this)"
else
  fail "3g-2. the requested basename appearing as a DIRECTORY must not satisfy the jemalloc arm (rc=$rc, out: $(head -4 <<<"$out"))"
fi
# ...and the accept direction of the same comparison, so 3g-2 is not satisfied by a check that
# refuses everything: the requested library itself, by basename, is accepted and the evidence
# NAMES which identity was used.
out=$(maps_call "$MAPS_WITH" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "jemalloc VERIFIED" <<<"$out" \
   && grep -qF "identity: the mapped" <<<"$out"; then
  pass "3g-2. CONTROL: the REQUESTED library IS accepted, and the evidence names the identity it matched on"
else
  fail "3g-2. the requested library must be accepted with its identity named (rc=$rc, out: $(head -4 <<<"$out"))"
fi

# 3g-3. A ` (deleted)` SUFFIX ON THE REQUESTED LIBRARY STILL MATCHES, and this is a decision
# rather than an accident: the mapping is IN EFFECT whether or not the file was unlinked
# afterwards, and effect is what both assertions are about. Refusing it would red a correct arm C
# on a box where the package was upgraded mid-session.
out=$(maps_call "$MAPS_JEM_DELETED" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "jemalloc VERIFIED" <<<"$out" && grep -qF "(deleted)" <<<"$out"; then
  pass "3g-3. jemalloc arm: a '(deleted)' mapping of the requested library MATCHES (the mapping is in effect; the evidence line carries the suffix)"
else
  fail "3g-3. a deleted mapping of the requested library must match (rc=$rc, out: $(head -4 <<<"$out"))"
fi
# ...and the same suffix on the CONTROL arm still refuses, which is the other half of that one
# decision: a deleted jemalloc is still jemalloc in effect.
out=$(maps_call "$MAPS_JEM_DELETED" "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "HAS a" <<<"$out"; then
  pass "3g-3. system arm: the SAME deleted jemalloc mapping still REFUSES the control arm — one decision, both directions"
else
  fail "3g-3. a deleted jemalloc mapping must refuse the control arm (rc=$rc, out: $(head -4 <<<"$out"))"
fi

# 3g-4. A PATHNAME CONTAINING SPACES is read WHOLE. The pathname is the 6th field ONWARD, and the
# kernel escapes only newlines in this file, so a space is a literal space — a parse that stopped
# at the first whitespace would see basename `libs/` and miss the library.
out=$(maps_call "$MAPS_SPACED" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "jemalloc VERIFIED" <<<"$out" \
   && grep -qF "/opt/my libs/${JLIB##*/}" <<<"$out"; then
  pass "3g-4. a pathname containing SPACES is read whole ('/opt/my libs/${JLIB##*/}') and its basename compared"
else
  fail "3g-4. a space-bearing pathname must be read whole (rc=$rc, out: $(head -4 <<<"$out"))"
fi

# 3g-5. AN ANONYMOUS MAPPING AND A PSEUDO-PATH ARE IGNORED, NOT PARSED AS PATHNAMES — and the
# file is NON-EMPTY, so this is distinct from the empty-read refusal above. The control arm
# accepts (nothing jemalloc is mapped) and the jemalloc arm refuses with its mapping COUNT
# reported, which is what says the lines were read rather than skipped wholesale.
out=$(maps_call "$MAPS_ANON_ONLY" "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "system VERIFIED" <<<"$out" && grep -q "4 mappings" <<<"$out"; then
  pass "3g-5. anonymous mappings and [heap]/[vdso] are counted as mappings (4 read) and compared against nothing"
else
  fail "3g-5. anonymous and pseudo-path mappings must be counted and ignored (rc=$rc, out: $(head -4 <<<"$out"))"
fi
if grep -q "0 file-backed" <<<"$out"; then
  pass "3g-5. ...and the verdict REPORTS that none of them was file-backed, so 'no jemalloc mapping' is attributable to a scan that found no pathname at all"
else
  fail "3g-5. the verdict must report the file-backed count (out: $(head -4 <<<"$out"))"
fi
out=$(maps_call "$MAPS_ANON_ONLY" "$ENV_PRELOAD" jemalloc "$JLIB" "" flight-merge-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "NO mapping of '${JLIB##*/}' is present" <<<"$out" \
   && grep -q "4 mappings read" <<<"$out"; then
  pass "3g-5. ...and the jemalloc arm refuses over the same file, reporting 4 mappings read (not a could-not-measure)"
else
  fail "3g-5. the jemalloc arm must refuse an anonymous-only maps file as an ABSENT mapping (rc=$rc, out: $(head -4 <<<"$out"))"
fi

# 3g-6. A NON-BLANK LINE THAT IS NOT A MAPPING IS A REFUSAL, in BOTH modes: a real maps line
# always carries the five leading fields, so an unparseable one is a state in which the assertion
# COULD NOT BE MADE — and it could be the very mapping either arm is about. The same fixture also
# carries a BLANK line, which is not a mapping and cannot hide one, so it must be ignored: if it
# were refused, this case would pass for the wrong reason, which is why the refusal must NAME the
# planted line.
for mode_pair in "system::" "jemalloc:$JLIB:"; do
  m="${mode_pair%%:*}"; l="${mode_pair#*:}"; l="${l%:}"
  if [ "$m" = system ]; then envf="$ENV_CLEAN"; else envf="$ENV_PRELOAD"; fi
  out=$(maps_call "$MAPS_UNPARSEABLE" "$envf" "$m" "$l" "" flight-warm-1); rc=$?
  if [ "$rc" -ne 0 ] && grep -qF "this is not a mapping line" <<<"$out" \
     && grep -q "NOT a /proc/<pid>/maps mapping" <<<"$out"; then
    pass "3g-6. $m arm: an unparseable maps line is REFUSED and the offending line is NAMED (not skipped, which could drop the mapping the arm is about)"
  else
    fail "3g-6. $m arm: an unparseable maps line must refuse naming it (rc=$rc, out: $(head -4 <<<"$out"))"
  fi
done
# ...and the BLANK line alone is NOT a refusal: same fixture minus the unparseable line, so the
# two differ in exactly one property.
{ printf '7f0200-7f0300 r-xp 00000000 08:01 102 /usr/lib/x86_64-linux-gnu/libc.so.6\n'; printf '\n'; } \
  > "$TMP/maps-blank-line-only"
out=$(maps_call "$TMP/maps-blank-line-only" "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "system VERIFIED" <<<"$out" && grep -q "1 mappings" <<<"$out"; then
  pass "3g-6. a BLANK line is ignored (1 mapping counted, not refused) — 3g-6's refusal is the unparseable line and not any non-mapping text"
else
  fail "3g-6. a blank maps line must be ignored (rc=$rc, out: $(head -4 <<<"$out"))"
fi

# 3g-6b. THE RECOGNISER AGAINST THE REAL KERNEL FORMAT. Every fixture above was written by the
# same hand as the regex, so together they cannot show that a REAL `/proc/<pid>/maps` parses —
# and a recogniser that refuses real input is the guard an operator learns to work around, which
# is the failure direction 3g-1 already paid for once. This process's OWN maps file is the only
# oracle for that, and reading it needs no server, no root and no privilege. Its verdict must be
# `system VERIFIED`: the function refuses on the FIRST unparseable line, so a clean run is the
# assertion that every line of a live maps file was recognised.
#
# NOT-MEASURED is DECLARED and non-fatal: on a host with no procfs this control cannot be taken,
# and a red there would be a red on correct input. It is printed either way so its absence is
# never silent.
#
# `-s` IS NOT USED TO DECIDE READABILITY, and that is measured rather than styled: procfs
# reports SIZE 0 for `maps` (verified on this box: `-r` true, `-s` FALSE, 39 lines via `grep`),
# so an `-s` probe reads a live file as empty — the two-valued probe taking its permissive
# answer, one directory over from the rule this whole case exists for.
#
# The assertion is on the RECOGNISER and not on the arm's verdict: a box that really did preload
# jemalloc would legitimately refuse the control arm, and pinning `system VERIFIED` would red
# there for a reason that has nothing to do with parsing. What must never appear is the
# unparseable-line refusal.
if [ -r /proc/self/maps ]; then
  out=$(maps_call /proc/self/maps "$ENV_CLEAN" system "" "" flight-bypass-warm-1); rc=$?
  real_lines=$(grep -c . /proc/self/maps)
  if grep -q "NOT a /proc/<pid>/maps mapping" <<<"$out"; then
    fail "3g-6b. the recogniser REFUSED a line of a REAL maps file — it reds on correct input (out: $(head -4 <<<"$out"))"
  elif [ "$real_lines" -gt 0 ]; then
    pass "3g-6b. the recogniser accepts a LIVE /proc/self/maps (~$real_lines lines; verdict: $(head -1 <<<"$out" | cut -c1-60)…) — every line parsed, since it refuses on the first that does not"
  else
    echo "note - 3g-6b. CONTROL NOT TAKEN: /proc/self/maps read as 0 lines, so nothing was recognised (declared, not fatal)"
  fi
else
  echo "note - 3g-6b. CONTROL NOT TAKEN: /proc/self/maps is not readable on this host, so the recogniser was not driven against a real kernel maps file (declared, not fatal — every other 3g case is synthetic)"
fi

# 3g-7. STRUCTURAL: the whole-line substring tests are GONE. The behavioural arms above cover the
# shapes someone thought of; this pins that neither original comparison survives anywhere in the
# function, which no behavioural case can express. The needles are SPLIT so this test cannot
# match its own source.
if grep -qE '"\$line"'"$(printf '%s' ' == *')"'jemalloc\*' "$FLIGHT_LIB"; then
  fail "3g-7. STRUCTURAL: a whole-LINE jemalloc substring test is back in $FLIGHT_LIB — the F3 defect"
else
  pass "3g-7. STRUCTURAL: no whole-LINE jemalloc substring test remains (the comparison is on the pathname field's basename)"
fi
if grep -qE '"\$line"'"$(printf '%s' ' == *"$need')"'le"\*' "$FLIGHT_LIB"; then
  fail "3g-7. STRUCTURAL: a whole-LINE requested-basename substring test is back in $FLIGHT_LIB"
else
  pass "3g-7. STRUCTURAL: no whole-LINE requested-basename substring test remains"
fi

# --- 3f. THE ARENA CAP, which NO mapping can see (#3551 item 9 / #3217 partC F1) ------------
# The pre-registered experiment is MALLOC_ARENA_MAX = 1, 2, 4, default. An arena cap leaves no
# mapping at all, so environ is the ONLY place it is observable, and a rep labelled with a cap it
# never had would make that experiment measure nothing.
out=$(maps_call "$MAPS_WITHOUT" "$ENV_ARENA1" system "" 1 flight-bypass-warm-1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "system VERIFIED" <<<"$out"; then
  pass "arena: a requested cap of 1 that the process RECEIVED is accepted (the accept half, on the only file that can see it)"
else
  fail "a received arena cap must be accepted (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# THE SUBSTRING TRAP, which is the reason for whole-entry matching: `MALLOC_ARENA_MAX=1` must NOT
# be satisfied by `MALLOC_ARENA_MAX=16`.
out=$(maps_call "$MAPS_WITHOUT" "$ENV_ARENA16" system "" 1 flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "'16'" <<<"$out" && grep -q "not '1'" <<<"$out"; then
  pass "arena: a cap of 16 does NOT satisfy a requested 1 — whole-entry match with an exact value compare, so the =1/=16 substring trap cannot fire"
else
  fail "MALLOC_ARENA_MAX=16 must not satisfy a requested 1 (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...an absent cap when one was requested is refused, naming the experiment it would have voided.
out=$(maps_call "$MAPS_WITHOUT" "$ENV_CLEAN" system "" 2 flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "ABSENT from its environment" <<<"$out"; then
  pass "arena: a requested cap the process never received is refused (an arena cap leaves NO mapping, so environ is the only observable)"
else
  fail "an absent arena cap must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the OTHER direction: a cap present when none was requested is refused too, because the
# rep would be capped while the session says it is not.
out=$(maps_call "$MAPS_WITHOUT" "$ENV_ARENA1" system "" "" flight-bypass-warm-1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no arena cap was requested" <<<"$out"; then
  pass "arena: an UNREQUESTED cap the process somehow received is refused — a configuration difference between arms that no recorded field would describe"
else
  fail "an unrequested arena cap must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the FINAL ENTRY WITH NO TRAILING NUL is read, not dropped: `$ENV_ARENA1` ends without
# one, and its LAST variable is HOME — but the cap is mid-blob, so this case pins the shape by
# accepting a file the naive loop would have truncated.
if grep -q "system VERIFIED" <<<"$(maps_call "$MAPS_WITHOUT" "$ENV_ARENA1" system "" 1 t)"; then
  pass "environ parse: a blob whose LAST entry carries no trailing NUL is still read in full (the naive read loop drops it)"
else
  fail "a NUL-terminated-less final entry must still be read"
fi

# ===========================================================================
# PART 4 — THE RECORD, THE SUBSTITUTION, AND WHAT THE REPORT MAY SAY
# ===========================================================================
# `flight_server_cpus` is to #3551 what `server_cpus` was to #3272's F6: an opaque manifest
# string that reaches a "verified" sentence in the report. The same mechanism answers it — the
# driver records what it verified, and the reporter requires the manifest to AGREE.

set_manifest_flight_pin() { # set_manifest_flight_pin <session> <value>
  python3 - "$1/session-corpus-pin.json" "$2" <<'PY'
import json, sys
p = sys.argv[1]
j = json.load(open(p))
j["config"]["flight_server_cpus"] = sys.argv[2]
json.dump(j, open(p, "w"), indent=1)
PY
}
edit_pin_record() { # edit_pin_record <session> <key> <value>   ("" value = DELETE the key)
  python3 - "$1/pinning-verification.json" "$2" "$3" <<'PY'
import json, sys
p, key, value = sys.argv[1], sys.argv[2], sys.argv[3]
j = json.load(open(p))
if value == "":
    j.pop(key, None)
else:
    j[key] = value
json.dump(j, open(p, "w"), indent=1)
PY
}

# --- 4a. THE SUBSTITUTION, at the new field --------------------------------------------------
# The reviewer's measured #3272 case, one pin over: a manifest naming CPUs no verification was
# ever performed against must not be printed as verified.
d="$TMP/f-subst"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
set_manifest_flight_pin "$d" "99,99"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "was performed against" <<<"$out" \
   && grep -q "'99,99'" <<<"$out" && grep -q "flight server" <<<"$out"; then
  pass "SUBSTITUTION: a manifest flight pin of 99,99 that no verification ran against is REFUSED, naming the flight arm"
else
  fail "a substituted flight pin must be refused (rc=$rc, out: $(head -4 <<<"$out"))"
fi
if grep -q "'2,10'" <<<"$out"; then
  pass "SUBSTITUTION: the refusal names BOTH values — which artifact was edited is the operator's next question"
else
  fail "the refusal must name the recorded value too (out: $(head -4 <<<"$out"))"
fi

# --- 4b. A RECORD MISSING A FLIGHT FIELD IS REFUSED, with the re-run remedy ------------------
# This is the BACKWARDS-COMPATIBILITY decision, observed rather than described: the new fields
# are REQUIRED, and a session dir written by an older driver is refused with the remedy the
# existing `events`/`bin_dir` fields established. Nothing in the repository is affected — no
# session dir is tracked in git — so the cost is re-running a scratch session.
d="$TMP/f-incomplete"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
edit_pin_record "$d" flight_allocator ""
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "INCOMPLETE" <<<"$out" && grep -q "flight_allocator" <<<"$out" \
   && grep -q "Re-run the session with the current driver" <<<"$out"; then
  pass "REQUIRED-FIELDS: a record without flight_allocator is REFUSED naming the field and the re-run remedy (the chosen backwards-compatibility posture)"
else
  fail "an incomplete record must be refused naming the field (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# --- 4c. THE CLOSED GRAMMARS -----------------------------------------------------------------
# The report's wording is DERIVED from the mode, so a mode nobody planned for could only either
# crash the reporter or describe a property nothing verified.
d="$TMP/f-mode"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
edit_pin_record "$d" flight_pin_mode "numa-nodes"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "flight_pin_mode" <<<"$out" && grep -q "numa-nodes" <<<"$out"; then
  pass "CLOSED GRAMMAR: an unrecognised flight_pin_mode is REFUSED rather than reported verbatim or defaulted"
else
  fail "an unknown flight_pin_mode must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
d="$TMP/f-alloc"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
edit_pin_record "$d" flight_allocator "tcmalloc"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "flight_allocator" <<<"$out" && grep -q "tcmalloc" <<<"$out"; then
  pass "CLOSED GRAMMAR: an unrecognised flight_allocator is REFUSED (a claim about an arm this rig does not have)"
else
  fail "an unknown flight_allocator must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# --- 4d. WHAT THE REPORT MAY SAY — THE ONE-FIELD DIFFERENTIAL --------------------------------
# Two fixtures differing in ONE property (the recorded pin mode, with the pin list moved to match
# it) must produce two DIFFERENT sentences. The forbidden direction is the load-bearing one: a
# `distinct-cores` pin may NEVER be described as `physical-core siblings`, because those are
# mutually exclusive properties and only one of them was read out of thread_siblings_list.
flight_line() { grep '^flight pin' <<<"$1"; }

d="$TMP/f-siblings"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
out_sib=$(run_report "$d" "$TMP/corpus"); rc_sib=$?
d="$TMP/f-distinct"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
# ONE property changes: the flight pin is 2,3 and the mode is distinct-cores, on BOTH sides of
# the comparison the reporter makes (the manifest and the record), because a disagreement between
# them is 4a's subject and would refuse for a different reason.
ws0_pin_verification "$d" "2,10" "4,12" "2,3" "distinct-cores"
set_manifest_flight_pin "$d" "2,3"
out_dist=$(run_report "$d" "$TMP/corpus"); rc_dist=$?

if [ "$rc_sib" -eq 0 ] && [ "$rc_dist" -eq 0 ]; then
  pass "REPORT: both fixtures report successfully, so the wording assertions below are about a working report rather than two refusals"
else
  fail "both fixtures must report (siblings rc=$rc_sib, distinct rc=$rc_dist; out: $(head -3 <<<"$out_dist"))"
fi
line_sib="$(flight_line "$out_sib")"
line_dist="$(flight_line "$out_dist")"
if grep -q "verified physical-core siblings" <<<"$line_sib" && grep -q "2,10" <<<"$line_sib"; then
  pass "REPORT: a SIBLINGS pin reads 'verified physical-core siblings' on its flight-pin line (line: $line_sib)"
else
  fail "the siblings fixture must say physical-core siblings (line: $line_sib)"
fi
if grep -q "verified pairwise DISTINCT physical cores" <<<"$line_dist"; then
  pass "REPORT: a DISTINCT-CORES pin reads 'verified pairwise DISTINCT physical cores' — its own wording, not the sibling one"
else
  fail "the distinct-cores fixture must say pairwise DISTINCT physical cores (line: $line_dist)"
fi
if ! grep -q "physical-core siblings" <<<"$line_dist"; then
  pass "REPORT (the forbidden direction): the distinct-cores flight-pin line does NOT contain 'physical-core siblings' — the two properties are mutually exclusive and only one was read"
else
  fail "a distinct-cores pin must NEVER be described as physical-core siblings (line: $line_dist)"
fi
# ...and the driver's own sysfs echo is carried through verbatim, so the sentence above cites an
# observation rather than restating the argument.
if grep -q "thread_siblings_list read" <<<"$out_dist"; then
  pass "REPORT: the driver's verbatim sysfs echo (each CPU's sibling set) is printed beside the claim"
else
  fail "the recorded sysfs echo must be printed (out: $(grep -A2 '^flight pin' <<<"$out_dist"))"
fi
# ...and a flight pin EQUAL to the server pin is labelled as such, so a default run reads as
# close to the pre-#3551 output as is honest, while a DIFFERENT pin says the difference between
# the arms is about that pin.
if grep -q "SAME PIN AS THE BARE-SCAN ARM" <<<"$line_sib" \
   && grep -q "DIFFERENT PIN FROM THE BARE-SCAN ARM" <<<"$line_dist"; then
  pass "REPORT: the flight-pin line says whether the two arms share a pin — the drift-control premise, stated where the numbers are read"
else
  fail "the flight-pin line must distinguish a shared pin from a different one (sib: $line_sib | dist: $line_dist)"
fi
# THE ALLOCATOR, WITH ITS EVIDENCE. Never the bare word: the only thing worth printing is what
# was observed in the running process.
if grep -q "^allocator    : flight server ran under system" <<<"$out_sib" \
   && grep -q "/proc/<server-pid>/maps" <<<"$out_sib"; then
  pass "REPORT: the allocator line names the arm AND the per-rep evidence (the /proc read), not just a label"
else
  fail "the allocator line must carry its evidence (out: $(grep -A1 '^allocator' <<<"$out_sib"))"
fi
# ...and the COUNTERS line states BOTH domains, because one `-C <server_cpus>` string became
# false for the flight arm the moment the two pins could differ.
if grep -q "perf stat -C 2,10 (bare scan) / -C 2,3 (Flight)" <<<"$out_dist"; then
  pass "REPORT: the counters line states the counting domain PER ARM (a single -C would be false for the flight arm)"
else
  fail "the counters line must name both domains (line: $(grep '^counters' <<<"$out_dist"))"
fi
# WHAT EACH ARM'S CYCLES WERE COUNTED ON, beside its own figures (#3551 item 6). "cycles/row"
# now means "hardware-thread cycles on THESE cpus per row", and the two arms may legitimately
# name different lists, so a reader who cannot see the list cannot read the number.
if grep -qE '^  bare scan .*counted on cpus 2,10$' <<<"$out_dist" \
   && grep -qE '^  flight do_get .*counted on cpus 2,3$' <<<"$out_dist"; then
  pass "REPORT: each arm's figure line names the CPUS ITS CYCLES WERE COUNTED ON, and under a differing flight pin the two lists DIFFER (2,10 vs 2,3)"
else
  fail "each arm's figures must name their counted cpus (out: $(grep -E '^  (bare scan|flight do_get)' <<<"$out_dist"))"
fi
# ...and the standing NOTES bullet must be TRUE IN BOTH CONFIGURATIONS. The pre-#3551 wording
# ("summed over BOTH SMT siblings of the pinned physical core … Both arms are counted
# identically") is FALSE under a distinct-core flight pin, so it was rewritten rather than
# deleted: the quantity is stated in terms that hold either way, and the distinct-core case is
# named as the property under test.
if ! grep -q "Both arms are counted identically, so the ratio" <<<"$out_dist" \
   && grep -q "summed over EVERY hardware thread in the counted list" <<<"$out_dist" \
   && grep -q "PROPERTY UNDER TEST" <<<"$out_dist"; then
  pass "REPORT NOTES: the SMT bullet no longer asserts the two arms are counted identically, states the quantity in terms true of both configurations, and names the distinct-core case as the property under test"
else
  fail "the counting NOTES bullet must be true in both configurations (notes: $(grep -A3 'hardware thread' <<<"$out_dist" | head -5))"
fi
# ...and the DEFAULT configuration still reads as close to the pre-#3551 report as is honest:
# both arms name the same list, and the bullet says so.
if grep -qE '^  bare scan .*counted on cpus 2,10$' <<<"$out_sib" \
   && grep -q "each arm's figure is a per-physical-core one and the two are counted identically" <<<"$out_sib"; then
  pass "REPORT NOTES: with both pins at the default the bullet AFFIRMS the per-physical-core reading and the identical counting (the honest no-op-by-default half)"
else
  fail "the default configuration must still state the per-physical-core reading (out: $(grep -B1 -A2 'hardware thread' <<<"$out_sib" | head -6))"
fi
# ...and results.json carries the same facts for a machine reader.
if python3 - "$TMP/f-distinct/results.json" <<'PY'
import json, sys
j = json.load(open(sys.argv[1]))
pin = j["pinning"]
assert pin["flight_server_cpus"] == "2,3", pin
assert pin["flight_pin_mode"] == "distinct-cores", pin
assert pin["flight_pin_claim"] == "pairwise DISTINCT physical cores", pin
assert pin["flight_allocator"] == "system", pin
assert "2,3" in pin["counter_mode"] and "2,10" in pin["counter_mode"], pin
# The counted list PER ARM as a mapping, not only inside the prose of counter_mode (#3551
# item 6): a machine reader comparing two arms needs it as data.
assert pin["counted_cpus_by_arm"] == {"scan": "2,10", "flight": "2,3"}, pin
v = j["pinning"]["verification"]
# The record read through, including the per-rep verification contract. The value here is the
# FIXTURE's (shaped like the driver's), so what is asserted is that the field reaches the
# document at all — the DRIVER's own wording is asserted against the driver, below.
assert "/proc/<server-pid>/maps" in v["flight_allocator_verification"], v
assert v["flight_pin_verified"].startswith("flight server CPUs:"), v
PY
then
  pass "REPORT: results.json carries the flight pin, its mode, the derived claim, the allocator, the per-arm counter mode, and the recorded per-rep verification contract"
else
  fail "results.json must carry the flight-arm facts"
fi
# ...and THE DRIVER'S OWN recorded contract DECLARES ITS LIMIT (structural, against the driver
# source, because only the rig writes this string): the per-rep statuses are written where the
# observation is made and NOTHING AT REPORT TIME requires them to be present. A record that
# claimed report-time completeness it does not have would be the claim-nothing-backs shape.
if grep -qF "DECLARED LIMIT: the driver ABORTS on a failure, and nothing at REPORT time requires those per-rep files to exist" "$FLIGHT_LIB"; then
  pass "the driver's recorded allocator contract DECLARES its own limit (no report-time completeness check for the per-rep statuses) rather than implying coverage it lacks"
else
  fail "the recorded allocator verification string must declare its report-time limit"
fi

# ===========================================================================
# PART 4b — THE ENVIRONMENT IS PART OF THE MEASUREMENT (#3551 item 8)
# ===========================================================================
# `lib-binaries.sh` freezes and digests three binaries; the session manifest captured NO
# environment at all. With ONE binary set across all arms — deliberate, and kept — the artifact
# sets for "glibc" and "jemalloc" therefore differed in NOTHING that is written down, which makes
# arm C unfalsifiable. And an AMBIENT allocator variable is worse than unrecorded: it would be
# INHERITED by `ws0-scan-bench`, putting the DRIFT CONTROL on the allocator under test, where the
# flight arm's own check cannot see it (the system arm's launch sets LD_PRELOAD empty for the
# SERVER, so the server looks clean while the control arm is perturbed).
env_lib_call() {
  local fn="$1"; shift
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$FLIGHT_LIB"
    "$fn" "$@" ) 2>&1
}

# --- 4b-1. THE AMBIENT RECORD NAMES EVERY KEY, AFFIRMATIVELY -------------------------------
out=$(env -u LD_PRELOAD -u LD_LIBRARY_PATH -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS \
        bash -c "source '$FLIGHT_LIB'; ws0_ambient_env_record" 2>&1)
if grep -q "LD_PRELOAD=<unset>" <<<"$out" && grep -q "LD_LIBRARY_PATH=<unset>" <<<"$out" \
   && grep -q "RUSTFLAGS=<unset>" <<<"$out" && grep -q "CARGO_ENCODED_RUSTFLAGS=<unset>" <<<"$out" \
   && grep -q "MALLOC_VARS=<none>" <<<"$out"; then
  pass "env record: an absent variable is recorded as an AFFIRMATIVE <unset>/<none>, never as a blank — 'nothing was set' and 'nobody wrote it down' must not look the same"
else
  fail "the ambient record must name every key with affirmative markers (out: $out)"
fi
# ...and a SET value is recorded VERBATIM, including one carrying a space: this box exports
# RUSTFLAGS='-D warnings' by default, and ws0-3552 §4 is explicit that it must be stated AS
# MEASURED — an unrecorded environment has already cost this repo an hour once.
out=$(env RUSTFLAGS='-D warnings' MALLOC_TOP_PAD_=9 \
        bash -c "source '$FLIGHT_LIB'; ws0_ambient_env_record" 2>&1)
if grep -q "RUSTFLAGS=-D warnings" <<<"$out" && grep -q "MALLOC_VARS=MALLOC_TOP_PAD_=9" <<<"$out"; then
  pass "env record: a set value is recorded VERBATIM (RUSTFLAGS='-D warnings'), and the MALLOC_* family is DISCOVERED by prefix rather than enumerated"
else
  fail "a set value must be recorded verbatim and MALLOC_* discovered (out: $out)"
fi

# --- 4b-2. AN AMBIENT ALLOCATOR VARIABLE IS REFUSED, BOTH VARIABLES ------------------------
out=$(env MALLOC_ARENA_MAX=2 bash -c "source '$FLIGHT_LIB'; refuse_ambient_allocator_env" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "MALLOC_ARENA_MAX='2'" <<<"$out" \
   && grep -q "DRIFT CONTROL" <<<"$out" && grep -q "env -u LD_PRELOAD" <<<"$out"; then
  pass "ambient env: an ambient MALLOC_ARENA_MAX is REFUSED, naming it, the drift-control reason, and the one-command remedy"
else
  fail "an ambient MALLOC_* must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
out=$(env LD_PRELOAD="$TMP/fake-libjemalloc.so.2" \
        bash -c "source '$FLIGHT_LIB'; refuse_ambient_allocator_env" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "LD_PRELOAD='$TMP/fake-libjemalloc.so.2'" <<<"$out"; then
  pass "ambient env: an ambient LD_PRELOAD is REFUSED naming its value (the flight arm's own check cannot see it — the system arm's launch empties LD_PRELOAD for the SERVER)"
else
  fail "an ambient LD_PRELOAD must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# THE ACCEPT DIRECTION: a clean environment passes, so this is not a guard that refuses every
# host. `LD_LIBRARY_PATH` and `RUSTFLAGS` are RECORDED and deliberately NOT refused — they do not
# change the allocator, and RUSTFLAGS is set by default on this box, so refusing it would red
# every correct run here.
out=$(env -u LD_PRELOAD RUSTFLAGS='-D warnings' LD_LIBRARY_PATH=/opt/lib \
        bash -c "source '$FLIGHT_LIB'; refuse_ambient_allocator_env" 2>&1); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "ambient env: a clean-of-allocator environment is ACCEPTED even with RUSTFLAGS and LD_LIBRARY_PATH set — those are recorded, not refused (a guard that reds on this box's default would be waived)"
else
  fail "RUSTFLAGS/LD_LIBRARY_PATH must not be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the DRIVER refuses too, above its argument boundary, so nothing is executed first.
out=$(env MALLOC_ARENA_MAX=2 bash -c "cd '$REPO_ROOT'; source scripts/tests/lib-ws0-hermetic.sh; ws0_hermetic_init '$TMP/amb'; ws0_driver_run '$DRIVER' --corpus /nonexistent-corpus" 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "carries allocator settings" <<<"$out"; then
  pass "ambient env: the DRIVER refuses an ambient MALLOC_* at exit 2, above the argument boundary — before any build, sysctl write or cache drop"
else
  fail "the driver must refuse an ambient allocator variable (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# --- 4b-3. THE BARE SCAN ASSERTS IT RECEIVED NEITHER ----------------------------------------
# The drift control must be UNPERTURBED, and that is asserted per rep against the very shell its
# bench inherits — an affirmative measurement of what the child will receive, not an intention.
out=$(env -u LD_PRELOAD bash -c "source '$FLIGHT_LIB'; assert_scan_env_unperturbed scan-warm-1" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "scan env VERIFIED for scan-warm-1" <<<"$out"; then
  pass "scan env: a clean environment yields affirmative evidence naming the rep (recorded per rep as <tag>.scan-env.status)"
else
  fail "the scan-env assertion must pass on a clean environment (rc=$rc, out: $out)"
fi
out=$(env MALLOC_ARENA_MAX=4 bash -c "source '$FLIGHT_LIB'; assert_scan_env_unperturbed scan-warm-1" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "bare-scan rep scan-warm-1 would inherit" <<<"$out" \
   && grep -q "MALLOC_ARENA_MAX='4'" <<<"$out"; then
  pass "scan env: a perturbed environment is REFUSED naming the rep and the variable (a perturbed control inverts the comparison rather than adding noise)"
else
  fail "the scan-env assertion must refuse a perturbed environment (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the LEG calls it, before its bench launches. Structural, because the call ORDER is the
# property: the check must precede the launches whose inheritance it is about.
if python3 - "$MEASURE_LIB" <<'PY'
import sys
lines = open(sys.argv[1]).read().split("\n")
start = next(i for i, l in enumerate(lines) if l.startswith("measure_scan() {"))
call = next(i for i, l in enumerate(lines) if "assert_scan_env_unperturbed" in l)
launch = next(i for i, l in enumerate(lines)
              if i > start and "ws0-scan-bench" in l and "taskset" in l)
assert start < call < launch, (start, call, launch)
PY
then
  pass "scan env (STRUCTURAL): measure_scan calls the assertion BEFORE its first bench launch — the order is the property, since the child inherits the shell as it is at that moment"
else
  fail "the scan-env assertion must precede the bench launches in measure_scan"
fi

# --- 4b-4. THE MANIFEST CARRIES BOTH RECORDS, AND KEY COMPLETENESS IS ENFORCED -------------
d="$TMP/env-ok"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "^env ambient  : LD_PRELOAD=<unset>" <<<"$out" \
   && grep -q "^env injected : flight server process ONLY" <<<"$out"; then
  pass "env manifest: the report prints AMBIENT and INJECTED as SEPARATE lines — a stray operator variable and a deliberate injection are different facts"
else
  fail "both env records must be printed (rc=$rc, out: $(grep '^env ' <<<"$out"))"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
j = json.load(open(sys.argv[1]))
e = j["environment"]
assert "LD_PRELOAD=<unset>" in e["ambient"], e
assert "flight server process ONLY" in e["injected"], e
assert "separate fields" in e["note"], e
PY
then
  pass "env manifest: results.json carries both records at the TOP LEVEL with the note that an ambient allocator variable is refused (a reproduction must not have to guess the subsection)"
else
  fail "results.json must carry the environment block"
fi
# KEY COMPLETENESS: a record that silently dropped a key would read exactly like "that variable
# was unset" — a different fact, and the permissive one.
d="$TMP/env-short"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/session-corpus-pin.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p))
j["config"]["env_ambient"] = "LD_PRELOAD=<unset>; MALLOC_VARS=<none>"
json.dump(j, open(p, "w"), indent=1)
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "names no LD_LIBRARY_PATH, RUSTFLAGS, CARGO_ENCODED_RUSTFLAGS" <<<"$out"; then
  pass "env manifest: an env_ambient missing keys is REFUSED naming every absent one (an absent key reads exactly like an unset variable, which is the permissive reading)"
else
  fail "an incomplete env_ambient must be refused naming the keys (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# ===========================================================================
# PART 5 — THE ADMISSION CEILING IS READ BACK, AND MUST AGREE (#3551 item 10)
# ===========================================================================
# `cqlite-flight` DERIVES its ceiling when `--max-concurrent-scans` is not pinned:
# `clamp(2 x available_parallelism, 2, 64)`, and `available_parallelism` respects the CPU
# AFFINITY MASK. So the ceiling is a FUNCTION OF THE PIN, and a 2-CPU pin vs a 4-CPU pin differ
# in TWO properties — where the work runs AND how much of it the server admits at once. The rig
# does not PIN the ceiling to force agreement (that would change the configuration #3248 measured
# and hide exactly this drift); it READS IT BACK from each rep's own log and refuses disagreement.
#
# The reporter is invoked DIRECTLY here rather than through `run_report`, deliberately: that
# helper stamps a server log IF ABSENT (standing in for the driver, as it does for the corpus
# pin), so a case whose subject IS the log has to bypass it or the fixture would repair the very
# condition under test.
report_direct() { python3 "$REPORT" --dir "$1" --corpus "$2" 2>&1; }

# --- 5a. THE ACCEPT DIRECTION, and the #3400 construction it depends on ---------------------
d="$TMP/adm-ok"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(report_direct "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "^admission    : max_concurrent_scans=4 (source derived, available_parallelism=2)" <<<"$out" \
   && grep -q "OBSERVED IDENTICAL across all 1 flight rep(s)" <<<"$out"; then
  pass "admission: the report prints the ceiling, its SOURCE and available_parallelism, and says how many reps were OBSERVED to agree"
else
  fail "the admission line must carry all three fields (rc=$rc, line: $(grep '^admission' <<<"$out"))"
fi
# THE CONSTRUCTION ASSERT, which is what makes 5a evidence rather than a coincidence: the fixture
# log is written in the server's REAL escaped shape, so the plain `max_concurrent_scans=` literal
# does NOT occur in it. The parse therefore cannot have matched without stripping the escapes
# first — the #3400 property, asserted rather than assumed.
if ! grep -q 'max_concurrent_scans=' "$d/flight-bypass-warm-1.server.log" \
   && grep -q $'\033' "$d/flight-bypass-warm-1.server.log"; then
  pass "admission (#3400): the fixture log is ANSI-escaped and contains NO plain 'max_concurrent_scans=' literal, so 5a's success PROVES the parse strips escapes"
else
  fail "the fixture log must carry escapes and no plain literal, or 5a proves nothing about the strip"
fi
# ...and results.json carries the triple AND the per-rep record, not just the agreed values.
if python3 - "$d/results.json" <<'PY'
import json, sys
j = json.load(open(sys.argv[1]))
a = j["flight_admission"]
assert a["max_concurrent_scans"] == "4", a
assert a["max_concurrent_scans_source"] == "derived", a
assert a["available_parallelism"] == "2", a
assert a["per_rep"]["flight-bypass-warm-1"]["max_concurrent_scans"] == "4", a
assert a["reps_agreeing"] == 1, a
assert "NOT pinned" in a["note"], a
PY
then
  pass "admission: results.json records all three fields, the PER-REP values and the count that agreed — a reader comparing two sessions needs the ceiling AND its input"
else
  fail "results.json must record the admission triple per rep"
fi

# --- 5b. DISAGREEING REPS ARE REFUSED (the drift this exists to catch) ---------------------
d="$TMP/adm-disagree"; make_session "$d" "$GOOD_FLIGHT"
make_scan_rep "$d" warm 2 ok
make_flight_rep "$d" warm 2 ok "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus" 2 warm bypass 1
# ONE property apart from the accept case: rep 2's server logged a DIFFERENT ceiling, from a
# different available_parallelism — the signature of a rep whose affinity mask differed.
ws0_write_server_log "$d/flight-bypass-warm-2.server.log" 8 derived 4
out=$(report_direct "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "did NOT all run under the same admission" <<<"$out" \
   && grep -q "max_concurrent_scans:" <<<"$out" \
   && grep -q "flight-bypass-warm-1='4'" <<<"$out" && grep -q "flight-bypass-warm-2='8'" <<<"$out"; then
  pass "admission: reps that disagree on the ceiling are REFUSED, naming the FIELD and every rep's value"
else
  fail "disagreeing admission records must be refused naming both (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the refusal must state the remedy that is NOT available: pinning
# --max-concurrent-scans would change the measured configuration and hide the drift.
if grep -q "remedy is NOT to pin --max-concurrent-scans" <<<"$out"; then
  pass "admission: the refusal names the WRONG remedy explicitly (pinning would hide the drift it exists to catch)"
else
  fail "the refusal must rule out pinning (out: $(head -6 <<<"$out"))"
fi
# ...and available_parallelism is reported as its own disagreeing field, because a changed
# ceiling with an UNCHANGED available_parallelism means something pinned it — a different cause.
# ...naming BOTH VALUES of that field too, not merely the field: `available_parallelism` is the
# INPUT whose dependence on the affinity mask is the whole reason this check exists, so a
# disagreement there is the most diagnostic one available and the operator's next question is
# "which rep saw what".
if grep -q "available_parallelism:" <<<"$out" \
   && grep -qE "available_parallelism: .*flight-bypass-warm-1='2'" <<<"$out" \
   && grep -qE "available_parallelism: .*flight-bypass-warm-2='4'" <<<"$out"; then
  pass "admission: a disagreement on AVAILABLE_PARALLELISM is named with BOTH reps' values ('2' and '4'), so 'the mask changed' and 'someone pinned the ceiling' are distinguishable causes"
else
  fail "each disagreeing field must be named with both values (out: $(head -4 <<<"$out"))"
fi
# THE POSITIVE CONTROL, one property apart: with rep 2's log AGREEING, the same two-rep session
# reports. Without it, 5b would be satisfied by any two-rep session failing for any reason.
ws0_write_server_log "$d/flight-bypass-warm-2.server.log" 4 derived 2
out=$(report_direct "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "OBSERVED IDENTICAL across all 2 flight rep(s)" <<<"$out"; then
  pass "admission CONTROL: the SAME two-rep session reports once rep 2's log AGREES (so 5b is about the disagreement, not about the fixture)"
else
  fail "the agreeing two-rep session must report (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# --- 5c. AN UNMEASURABLE RECORD IS A REFUSAL, per cause ------------------------------------
d="$TMP/adm-absent"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
rm -f "$d/flight-bypass-warm-1.server.log"
out=$(report_direct "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "carries no server log" <<<"$out" \
   && grep -q "AFFINITY MASK" <<<"$out"; then
  pass "admission: an ABSENT server log is refused naming the rep, and the diagnostic states WHY the ceiling matters (it moves with the pin)"
else
  fail "an absent server log must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
d="$TMP/adm-empty"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
: > "$d/flight-bypass-warm-1.server.log"
out=$(report_direct "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "is EMPTY" <<<"$out"; then
  pass "admission: an EMPTY server log is its own could-not-measure refusal (a server that started always logs its startup line)"
else
  fail "an empty server log must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
d="$TMP/adm-garbage"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
printf 'cqlite-flight starting listen=127.0.0.1:18815 batch_size=8192\n' \
  > "$d/flight-bypass-warm-1.server.log"
out=$(report_direct "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "does not record max_concurrent_scans" <<<"$out" \
   && grep -q "colour is not the cause" <<<"$out"; then
  pass "admission: a log WITHOUT the fields is refused NAMING them, and rules colour out explicitly (the parse runs on stripped text)"
else
  fail "an unparseable log must be refused naming the fields (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# --- 5d. THE DECLARED/READ ORACLE, both directions -----------------------------------------
# The same property `PINNING_RECORD_FIELDS` has: a field declared and never read is a property of
# the measurement the report cannot see; a field read and never declared cannot be found by the
# next person adding one.
if python3 - <<'PY'
import pathlib, sys
sys.path.insert(0, "scripts/perf")
from ws0_flight_admission import FLIGHT_ADMISSION_FIELDS
src = pathlib.Path("scripts/perf/ws0_flight_admission.py").read_text()
# Every declared field must be READ by the reader loop, and the reader must read NOTHING it did
# not declare: the loop is over the tuple, so the assertion is that each name appears in the
# AGREEMENT check and in the returned record, and that the module names no other `_scans`/
# `parallelism` field.
for f in FLIGHT_ADMISSION_FIELDS:
    assert src.count(f) >= 2, f
assert len(FLIGHT_ADMISSION_FIELDS) == 3, FLIGHT_ADMISSION_FIELDS
import ws0_report
rsrc = pathlib.Path("scripts/perf/ws0_report.py").read_text()
for f in FLIGHT_ADMISSION_FIELDS:
    assert f"flight_admission['{f}']" in rsrc, f
print("DECLARED-AND-READ")
PY
then
  pass "admission: every declared field is READ by the module AND printed by the report — a field declared without a reader is one the report cannot see"
else
  fail "the admission field set must be declared and read in both directions"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT — this file has no `set -e` (#3272 round 3 nit)
# ==========================================================================
# Without it, a block that silently never executes (a helper returning early, a `$(...)` whose
# command vanished) LOWERS the count and registers NO failure, and the gate reads only the exit
# code. The floor is DERIVED FROM A MEASURED RUN and set below the observed count, so adding a
# case cannot red the suite. RE-DERIVED BY RUNNING IT at each addition, never counted from the
# source — loops and helpers multiply, and a source estimate understated a floor by 29 elsewhere
# in this repo's history. MEASURED: 56 (pin/allocator/report), 66 (+ items 5/7's counting
# domain), 80 (+ item 9's environ and arena), 91 (+ item 10's admission read-back), 103
# (+ item 8's environment records).
MIN_CHECKS=90
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 flight-arm (pin mode / allocator / recorded claim) checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks check(s) failed"
exit 1

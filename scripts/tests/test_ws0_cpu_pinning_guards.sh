#!/usr/bin/env bash
# Self-test for the WS0 rig's TWO STRUCTURAL guards — the ones that decide whether
# a number the rig prints is a number of the thing it says (issue #3272, item 10).
#
# A separate file from test_ws0_report_guards.sh on purpose: that one covers the
# REPORTER's fail-closed paths (what the rig does with observations it has), this
# one covers the MEASUREMENT APPARATUS itself (whether the observations are of the
# right thing in the first place). The two have disjoint fixtures — synthetic
# result dirs vs a synthetic sysfs tree — and keeping them apart also keeps both
# files under the campsite-rule test threshold.
#
#   1. THE VERIFIED-SIBLING `taskset` CHECK, BOTH DIRECTIONS.
#      Everything the same-session both-arm methodology claims rests on the two
#      pinned CPUs being the two hyperthreads of ONE physical core. If they are
#      not, `perf stat -C` counts two different cores and every per-core figure is
#      a figure of something else — silently, with nothing in the output saying so.
#      `verify_sibling_pair` exists to refuse that, and until now it had NEVER been
#      observed refusing anything: it read a hardcoded `/sys/...` path, so testing
#      it needed a particular CPU layout. Per #3249 (a hardcoded `_PERF_STATE="ok"`
#      survived 118/118 tests) an unobserved guard is not evidence, so `lib-cpu.sh`
#      now takes its topology root from `CQLITE_WS0_CPU_TOPOLOGY_ROOT` and this file
#      drives it over a FAKE sysfs tree in both directions: it must ACCEPT a genuine
#      sibling pair and REFUSE a non-sibling set. Both halves are load-bearing — a
#      check that refuses everything is as useless as one that refuses nothing, and
#      it is the one an operator works around.
#
#   2. THE PERF-INVOCATION GUARD (`lib-perf-lint.sh`). Per-process counting measured
#      >2x observer cost on this workload, so spec R2 requires CPU-wide counting and
#      the driver checks ITSELF at startup. Driving that guard over injected copies
#      found FIVE REAL BYPASSES across two successive deny-list patterns: an ATTACHED
#      value, ANY LINE MENTIONING "self-check" (the `grep -v` discarded by CONTENT, so
#      a comment on a real invocation suppressed the guard), a SINGLE-QUOTED attached
#      value, an invocation through a VARIABLE, and a GLOBAL OPTION between `perf` and
#      `stat`. All five fire now — and the mechanism is no longer a deny-list: it is an
#      ALLOWLIST (perf is invoked in ONE wrapper; any other invocation line must be
#      explicitly marked) plus a per-TOKEN option check plus a RUNTIME argv check. A
#      deny-list must anticipate every spelling and is silently permissive the moment it
#      misses one; an allowlist asks WHERE a line is, which is closed by construction.
#      Both directions are asserted, plus the lint's own positive control: it must be
#      SILENT on a minimal clean file and must FLAG an absent/empty/`-C`-less wrapper,
#      and it must NOT flag `perf_stat_c`/`perf_event_paranoid`/`target/perf-…`
#      identifiers — a guard that reds on ordinary code is the one an operator deletes.
#
# Hermetic: a fake sysfs tree under $TMPDIR and copies of the driver. No perf, no
# sudo, no taskset, no root, no real multi-socket hardware, no network, no corpus,
# no cargo. The real scripts/perf/ files are never modified.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
LIB="$REPO_ROOT/scripts/perf/lib-cpu.sh"
# The perf-invocation lint the driver sources. Held separately so this file drives THE
# SHIPPED implementation rather than a copy: a reimplemented check in a test is a
# second thing to keep in sync, and its divergence would be invisible in exactly the
# permissive direction.
PERF_LINT_LIB="$REPO_ROOT/scripts/perf/lib-perf-lint.sh"
# THE ONE SANCTIONED WAY THIS FILE MAY INVOKE THE DRIVER (#3272 review round 3, B1).
# See lib-ws0-hermetic.sh: `ws0_driver_run` prepends `--validate-args-only` and the
# recording shims. The perf-invocation lint this file drives runs at driver STARTUP,
# ABOVE the argument boundary, so every case below is reachable through that path — and
# `scripts/tests/test_ws0_hermeticity.sh` FAILS on any bare invocation added later.
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so
# the minimum-check-count floor at the end can see a block that silently never executed
# (#3272 review round 3 nit).
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
[ -f "$LIB" ] || { echo "FAIL - missing $LIB"; exit 1; }
[ -f "$PERF_LINT_LIB" ] || { echo "FAIL - missing $PERF_LINT_LIB"; exit 1; }
# Stated UP FRONT and fail-closed (#3272 review B8, applied to this file too).
# `driver_copy_with` needs python3 for its exact-literal injection. Without this
# check its absence would surface as a fixture-did-not-apply failure inside PART 2 —
# correct, but diagnosed as the wrong thing; and the reflex fix for a confusing
# failure is a skip, which is how a vacuous green gets introduced. python3 is a HARD
# REQUIREMENT of the rig this file tests (ws0-baseline.sh refuses to run without it),
# so its absence FAILS.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig"
  echo "       (ws0-baseline.sh refuses to run without it) and PART 2's exact-literal"
  echo "       driver injection needs it. A skip here would record the gate component"
  echo "       as SUCCESS with 0 of these checks having run (#3272 review B8)."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT
ws0_hermetic_init "$TMP"

# ===========================================================================
# PART 1 — the verified-sibling taskset check, BOTH directions
# ===========================================================================
#
# A fake `/sys/devices/system/cpu` describing a 4-physical-core, 8-thread box:
#
#   core 0: cpu0,cpu4    core 1: cpu1,cpu5
#   core 2: cpu2,cpu6    core 3: cpu3,cpu7
#
# Deliberately NOT the (2,10) pair the driver defaults to, so no case can pass by
# accidentally agreeing with a hardcoded default — and cpu2's sibling here is cpu6,
# which makes "2,10" (the real box's pair) a NON-sibling set on this fake topology.
TOPO="$TMP/sys/devices/system/cpu"
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

# lib_call <cpu-list-spec> <function> [args…] — source lib-cpu.sh against the FAKE
# topology in a subshell and run one function. Prints its output; returns its rc.
# A subshell per call so no case can inherit another's state.
lib_call() {
  local fn="$1"; shift
  ( export CQLITE_WS0_CPU_TOPOLOGY_ROOT="$TOPO"
    # shellcheck disable=SC1090
    source "$LIB"
    "$fn" "$@" ) 2>&1
}

# --- 1a. POSITIVE: a genuine sibling pair is ACCEPTED ----------------------
# Without this half, a `verify_sibling_pair` hardcoded to `return 1` would satisfy
# every negative case below.
out=$(lib_call verify_sibling_pair "2,6" server); rc=$?
if [ "$rc" -eq 0 ] && grep -q "verified siblings of one physical core" <<<"$out"; then
  pass "sibling-accept: a GENUINE sibling pair (2,6) is accepted and says so"
else
  fail "sibling-accept: expected rc=0 + 'verified siblings of one physical core' (rc=$rc, out: $out)"
fi

# Order must not matter — the check expands and sorts, so "6,2" is the same set.
out=$(lib_call verify_sibling_pair "6,2" server); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "sibling-accept: the same pair written in reverse order (6,2) is accepted"
else
  fail "sibling-accept-reversed: expected rc=0 (rc=$rc, out: $out)"
fi

# A RANGE spelling of the same set, so the expander is exercised on the accept path.
out=$(lib_call verify_sibling_pair "0,4" server); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "sibling-accept: another core's genuine pair (0,4) is accepted"
else
  fail "sibling-accept-core0: expected rc=0 (rc=$rc, out: $out)"
fi

# --- 1b. NEGATIVE: a NON-sibling set is REFUSED, naming why ----------------
# Each of these is a real way an operator gets the pinning wrong, and each would
# make every per-core figure the rig prints a figure of something else.
#
# reject_pair <label> <spec> — must exit non-zero AND explain, naming the observed
# sibling list. Non-zero alone is not enough: a guard that fires with a diagnostic
# about something else has not been observed doing its job.
reject_pair() {
  local label="$1" spec="$2" out rc
  out=$(lib_call verify_sibling_pair "$spec" server); rc=$?
  if [ "$rc" -ne 0 ] \
     && grep -q "is NOT the sibling set of one" <<<"$out" \
     && grep -q "thread_siblings_list is" <<<"$out"; then
    pass "sibling-reject: $label ($spec)"
  else
    fail "sibling-reject: $label ($spec) — expected non-zero + a named-sibling-list diagnostic (rc=$rc, out: $out)"
  fi
}

# TWO DIFFERENT PHYSICAL CORES. The headline defect: looks like a pair, counts two
# cores. `2,3` are both real CPUs, adjacent numbers, and siblings of NOTHING.
reject_pair "two different physical cores" "2,3"
# The real box's pair, on a box with a different layout — the exact silent-wrong
# case the check exists for, since 2,10 is a valid sibling pair on the #3096
# measurement host and is not one here.
reject_pair "a pair valid on ANOTHER box's layout" "2,10"
# A CORRECT pair PLUS A STRAY. This is what a one-sided check would accept: the set
# does contain core 2's siblings. The check compares from BOTH ends, so it refuses.
reject_pair "a correct pair plus a stray CPU" "2,6,3"
# A SINGLE CPU: half a core. `perf -C 2` counts one hyperthread of a pair whose
# other thread is running arbitrary other work.
reject_pair "a single CPU (half a physical core)" "2"
# A whole 4-CPU range spanning two cores.
reject_pair "a range spanning two physical cores" "0-3"

# --- 1c. An EMPTY / garbage spec fails closed ------------------------------
# Under `set -u` on bash < 4.4 this used to die with a shell diagnostic instead of
# the intended message; the fail-closed path is what must happen.
for spec in "" "," ",,"; do
  out=$(lib_call verify_sibling_pair "$spec" server); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "CPU list is empty" <<<"$out"; then
    pass "sibling-reject: an empty/garbage spec ('$spec') fails closed with the empty-list message"
  else
    fail "sibling-empty ('$spec'): expected non-zero + 'CPU list is empty' (rc=$rc, out: $out)"
  fi
done

# --- 1d. An UNREADABLE topology entry is a FAILURE, not an empty answer ----
# A CPU with no `thread_siblings_list` must stop the run. Were it to read as an
# empty sibling list, the comparison would fail with a confusing "not the sibling
# set" message for what is really "the topology could not be read" — and on a
# system where the file is genuinely absent, a lenient read would mean the pinning
# was never verified at all.
out=$(lib_call verify_sibling_pair "9" server); rc=$?
if [ "$rc" -ne 0 ] && grep -q "unreadable" <<<"$out"; then
  pass "sibling-reject: a CPU with no thread_siblings_list is UNREADABLE, not empty"
else
  fail "sibling-unreadable: expected non-zero + 'unreadable' (rc=$rc, out: $out)"
fi

# --- 1e. The DISJOINTNESS check, both directions ---------------------------
# A client sharing a physical core with the server puts the client's own CPU cost
# inside the server's `perf -C` window.
out=$(lib_call verify_disjoint "2,6" "1,5"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "disjoint-accept: two different cores' pairs are accepted"
else
  fail "disjoint-accept: expected rc=0 (rc=$rc, out: $out)"
fi
out=$(lib_call verify_disjoint "2,6" "6,1"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "overlap on cpu6" <<<"$out"; then
  pass "disjoint-reject: an overlapping CPU is refused, naming it"
else
  fail "disjoint-reject: expected non-zero + 'overlap on cpu6' (rc=$rc, out: $out)"
fi

# --- 1f. THE OVERRIDE CANNOT REACH A MEASUREMENT RUN -----------------------
# The injectable topology root is what makes 1a-1e possible, and it would be a
# BYPASS of the very guarantee they test if a measurement run could use it: the
# pinning check would then be verifying against a fabricated sysfs tree. So
# `assert_real_cpu_topology` refuses when the override is set, and the driver calls
# it before any pinning check. Both halves asserted.
out=$(lib_call assert_real_cpu_topology); rc=$?
if [ "$rc" -ne 0 ] && grep -q "CQLITE_WS0_CPU_TOPOLOGY_ROOT is set" <<<"$out"; then
  pass "override-refused: assert_real_cpu_topology FAILS when the topology root is overridden"
else
  fail "override-refused: expected non-zero + the override diagnostic (rc=$rc, out: $out)"
fi
# The override also ANNOUNCES itself on stderr — a shimmed run cannot be quiet.
if grep -q "CPU topology root OVERRIDDEN" <<<"$(lib_call cpu_list_expand "1")"; then
  pass "override-announced: sourcing with the override set prints a NOTE on stderr"
else
  fail "override-announced: sourcing with the override set must announce it"
fi
# And the driver WIRES that refusal in: grep is enough here, because running the
# driver far enough to reach it would need a corpus, perf and taskset.
if grep -q '^assert_real_cpu_topology || exit 2' "$DRIVER"; then
  pass "override-refused: the driver calls assert_real_cpu_topology (fail-closed) before verifying pinning"
else
  fail "override-refused: the driver must call 'assert_real_cpu_topology || exit 2'"
fi
# It must come BEFORE the sibling check, or the fabricated tree would already have
# been used to vouch for the pinning by the time the refusal ran.
assert_line=$(grep -n '^assert_real_cpu_topology || exit 2' "$DRIVER" | head -1 | cut -d: -f1)
verify_line=$(grep -n '^verify_sibling_pair "\$SERVER_CPUS"' "$DRIVER" | head -1 | cut -d: -f1)
if [ -n "$assert_line" ] && [ -n "$verify_line" ] && [ "$assert_line" -lt "$verify_line" ]; then
  pass "override-refused: the refusal (line $assert_line) precedes the sibling check (line $verify_line)"
else
  fail "override-refused: assert_real_cpu_topology must precede verify_sibling_pair (assert=$assert_line verify=$verify_line)"
fi

# --- 1g. THE CPU-LIST GRAMMAR: arbitrary code from a --server-cpus argument (B3) --------
# THE FINDING (#3272 review round 4, B3). `cpu_list_expand` fed range endpoints straight into
# bash arithmetic:
#
#     lo="${part%%-*}"; hi="${part##*-}"
#     for ((i = lo; i <= hi; i++)); do out+=("$i"); done
#
# `(( ))` EVALUATES its operands, and bash's arithmetic evaluator performs COMMAND
# SUBSTITUTION inside an array subscript. MEASURED against the pre-fix function on this box:
#
#     cpu_list_expand '1-x[$(touch /tmp/PWNED2)]'   =>   /tmp/PWNED2 CREATED, exit 0
#
# i.e. ARBITRARY COMMAND EXECUTION from a `--server-cpus`/`--client-cpus` argument. Also
# measured: `'1+1'` returned the STRING `1+1` as a CPU id, and `'0-999999999'` did not finish
# in 3 seconds (a billion array appends).
#
# The NON-VACUITY of each case below is that measurement: every one of these inputs was
# ACCEPTED (or hung) before the grammar existed.
#
# The injection case is asserted on the SIDE EFFECT, not on the exit status: a refusal that
# still ran the command would be worthless, and the file is the only thing that proves it did
# not run.
PWN_MARKER="$TMP/cpu-grammar-pwned"
rm -f "$PWN_MARKER"
out=$(lib_call cpu_list_expand "1-x[\$(touch $PWN_MARKER)]"); rc=$?
if [ "$rc" -ne 0 ] && [ ! -e "$PWN_MARKER" ]; then
  pass "cpu-grammar: OBSERVED — a command-substitution endpoint is REFUSED and the command DOES NOT RUN (pre-fix: it ran, exit 0)"
else
  fail "cpu-grammar: '1-x[\$(touch …)]' must be refused WITHOUT executing (rc=$rc, marker present: $([ -e "$PWN_MARKER" ] && echo yes || echo no), out: $out)"
fi
rm -f "$PWN_MARKER"
# ...and the refusal must NAME the boundary it is, or the next editor will "simplify" it back.
if grep -q "COMMAND SUBSTITUTION inside an array subscript" <<<"$out"; then
  pass "cpu-grammar: the refusal states WHY it is an allowlist (the arithmetic-evaluation hazard)"
else
  fail "cpu-grammar: the refusal must name the hazard (out: $out)"
fi
# Every other malformed shape, through the SAME rule rather than a case each — that is what
# makes it an allowlist: none of these needed to be anticipated individually.
for bad_spec in \
  '1+1' \
  '2*3' \
  '$((1+2))' \
  'a-b' \
  '-3' \
  '2-' \
  '-' \
  '2 10' \
  '0x2' \
  '2,10;id' \
  ; do
  out=$(lib_call cpu_list_expand "$bad_spec"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "is not a CPU index or range" <<<"$out"; then
    pass "cpu-grammar: '$bad_spec' is REFUSED by the decimal allowlist"
  else
    fail "cpu-grammar: '$bad_spec' must be refused (rc=$rc, out: $out)"
  fi
done
# THE SIZE BOUNDS, both of them, each with the resource failure it prevents.
out=$(lib_call cpu_list_expand "0-999999999"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "above 8191" <<<"$out"; then
  pass "cpu-grammar: OBSERVED — '0-999999999' is REFUSED on the index ceiling (pre-fix: a billion array appends, no completion in 3s)"
else
  fail "cpu-grammar: an absurd range must be refused before expanding (rc=$rc, out: $out)"
fi
# ...and a range UNDER the index ceiling but over the expansion cap: the two bounds are
# different properties, and a single check would leave one of them open.
out=$(lib_call cpu_list_expand "0-8000"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "expands to more than 1024 CPUs" <<<"$out"; then
  pass "cpu-grammar: a spec under the index ceiling but over the EXPANSION cap is refused (two distinct bounds)"
else
  fail "cpu-grammar: the expansion cap must fire independently of the index ceiling (rc=$rc, out: $out)"
fi
# A REVERSED range used to expand to NOTHING and be silently dropped, so '--server-cpus 10-2'
# pinned an empty set and the sibling check complained about the wrong thing.
out=$(lib_call cpu_list_expand "10-2"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "REVERSED range" <<<"$out"; then
  pass "cpu-grammar: a REVERSED range is REFUSED, naming itself (pre-fix: silently expanded to nothing)"
else
  fail "cpu-grammar: '10-2' must be refused as reversed (rc=$rc, out: $out)"
fi
# THE ACCEPT DIRECTION — without it, a grammar hardcoded to refuse everything would satisfy
# every case above. Includes the LEADING-ZERO case, which is the `010s` duration defect class
# (#3096 nit 7) one file over: `08` must be 8, not an invalid octal.
for good_spec in '2,10:2 10' '0-3,8:0 1 2 3 8' '08:8' '0:0' '8191:8191' '2,,10:2 10'; do
  spec="${good_spec%%:*}"; want="${good_spec##*:}"
  out=$(lib_call cpu_list_expand "$spec" 2>/dev/null); rc=$?
  # The override NOTE goes to stderr, which `lib_call` folds in; take the last line.
  got="$(tail -1 <<<"$out")"
  if [ "$rc" -eq 0 ] && [ "$got" = "$want" ]; then
    pass "cpu-grammar-accept: '$spec' expands to '$want'"
  else
    fail "cpu-grammar-accept: '$spec' must expand to '$want' (rc=$rc, got: '$got')"
  fi
done
# THE REFUSAL MUST REACH THE CALLERS, or the grammar is a function nobody consults. All three
# consumers propagate it, and each one has a distinct reason it must:
#  * verify_sibling_pair — an empty `want` was reported as "CPU list is empty", naming the
#    wrong cause for a malformed argument;
#  * verify_disjoint — an empty set trivially satisfies disjointness, so a silent empty would
#    turn a malformed --client-cpus into a PASS;
#  * cpu_siblings_of — a garbage sysfs entry must fail the verification, not become an empty
#    `got` diagnosed as "not the sibling set".
out=$(lib_call verify_sibling_pair "1+1" server); rc=$?
if [ "$rc" -ne 0 ] && grep -q "is not a CPU index or range" <<<"$out" \
  && ! grep -q "CPU list is empty" <<<"$out"; then
  pass "cpu-grammar-wired: verify_sibling_pair PROPAGATES the refusal (not 'CPU list is empty')"
else
  fail "cpu-grammar-wired: verify_sibling_pair must fail with the grammar's diagnostic (rc=$rc, out: $out)"
fi
out=$(lib_call verify_disjoint "2,6" "1+1"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "is not a CPU index or range" <<<"$out"; then
  pass "cpu-grammar-wired: verify_disjoint PROPAGATES the refusal (an empty set would satisfy disjointness)"
else
  fail "cpu-grammar-wired: verify_disjoint must refuse a malformed spec (rc=$rc, out: $out)"
fi
# The sysfs path: a topology whose `thread_siblings_list` holds garbage must FAIL the
# verification rather than compare unequal for the wrong reason.
BAD_TOPO="$TMP/bad-topo/cpu"
mkdir -p "$BAD_TOPO/cpu2/topology"
printf 'x[$(true)]\n' > "$BAD_TOPO/cpu2/topology/thread_siblings_list"
out=$( export CQLITE_WS0_CPU_TOPOLOGY_ROOT="$BAD_TOPO"
       # shellcheck disable=SC1090
       source "$LIB"; verify_sibling_pair "2" server 2>&1 ); rc=$?
if [ "$rc" -ne 0 ] && grep -q "is not a CPU index or range" <<<"$out"; then
  pass "cpu-grammar-wired: a GARBAGE thread_siblings_list fails the sibling check with the grammar's diagnostic"
else
  fail "cpu-grammar-wired: a garbage sysfs entry must fail the verification (rc=$rc, out: $out)"
fi

# ===========================================================================
# PART 2 — the `perf stat -p` self-grep
# ===========================================================================
#
# The driver greps ITSELF and refuses to run if a per-process `perf stat` form
# appears. Driven here over COPIES with an injected invocation, one spelling per
# case. Each copy is run with `--corpus /nonexistent`: the self-check is the FIRST
# thing after argument parsing, so it fires long before anything needs a corpus,
# perf, taskset or sudo.
#
# The `perf stat -C` line the injections replace, spelled in pieces so THIS FILE
# does not trip the driver's own grep when someone greps the tree.
CPU_WIDE_LINE='  perf stat -x, -e "$EVENTS" -C "$SERVER_CPUS" -o "$outfile" -- "$@"'

# driver_copy_with <replacement-line> — a temp checkout of scripts/perf with the
# CPU-wide perf line replaced. Echoes the copy's path.
driver_copy_with() {
  local repl="$1" d
  d="$(mktemp -d "$TMP/drvXXXXXX")"
  mkdir -p "$d/scripts/perf"
  # The WHOLE directory, not an enumerated file list: the driver sources several
  # libraries and adding one more (as review round 1's splits did) silently broke a
  # list-based copy — each case then failed with "no such file" instead of with the
  # guard's own diagnostic, i.e. the fixture reported the WRONG failure. Copying the
  # directory cannot go stale.
  cp "$REPO_ROOT/scripts/perf/"* "$d/scripts/perf/"
  # python3 for an exact literal replacement — sed would need every metacharacter
  # in `$EVENTS`/`"$@"` escaped, and a replacement that silently did nothing would
  # make the case pass vacuously (the injection must actually land, asserted below).
  python3 - "$d/scripts/perf/ws0-baseline.sh" "$CPU_WIDE_LINE" "$repl" <<'PY'
import sys
path, needle, repl = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(path).read()
if needle not in s:
    sys.exit("the CPU-wide perf line was not found — this test's fixture is stale")
open(path, "w").write(s.replace(needle, repl))
PY
  echo "$d/scripts/perf/ws0-baseline.sh"
}

# expect_selfgrep_fires <label> <injected-line>
expect_selfgrep_fires() {
  local label="$1" repl="$2" copy out rc calls
  copy="$(driver_copy_with "$repl")" || { fail "$label: could not build the injected copy"; return; }
  # NON-VACUITY for the fixture itself: the injection must be present in the copy,
  # or a no-op replacement would make the guard "fire" on nothing.
  if ! grep -qF "$repl" "$copy"; then
    fail "$label: the injected line is not in the copy — the fixture did not apply"
    return
  fi
  out=$(ws0_driver_run "$copy" --corpus /nonexistent); rc=$?
  calls="$(ws0_hermetic_calls)"
  if [ -n "$calls" ]; then
    fail "selfgrep-fires: $label — the injected copy INVOKED something outside this process: $calls"
    return
  fi
  if [ "$rc" -ne 0 ] \
     && grep -qE "contains a per-process/per-thread perf invocation" <<<"$out" \
     && grep -q "CPU-wide counting is mandatory" <<<"$out"; then
    pass "selfgrep-fires: $label"
  else
    fail "selfgrep-fires: $label — expected non-zero + the per-process diagnostic (rc=$rc, out: $(head -3 <<<"$out"))"
  fi
}

# The plain spelling — the one the original guard did catch.
expect_selfgrep_fires "a spaced -p <pid>" \
  '  perf stat -x, -e "$EVENTS" -p "$SERVER_PID" -o "$outfile" -- "$@"'
# BYPASS 1 (found by this test): an ATTACHED value. The old pattern's `-p `
# alternative required a trailing space, so this real per-process invocation was
# invisible to it. Measured against the pre-fix pattern: rc=1, no match.
expect_selfgrep_fires "an ATTACHED -p<pid> (pre-fix: NOT caught)" \
  '  perf stat -x, -e "$EVENTS" -p1234 -o "$outfile" -- "$@"'
# The long forms, spaced and `=`-joined.
expect_selfgrep_fires "a spaced --pid <pid>" \
  '  perf stat -x, -e "$EVENTS" --pid "$SERVER_PID" -o "$outfile" -- "$@"'
expect_selfgrep_fires "an --pid=<pid>" \
  '  perf stat -x, -e "$EVENTS" --pid=1234 -o "$outfile" -- "$@"'
# BYPASS 2 (found by this test): the old `grep -v 'self-check'` discarded by
# CONTENT, so a comment mentioning that phrase on a real per-process line
# suppressed the guard entirely. A guard whose bypass is a code comment is not one.
expect_selfgrep_fires "a -p line carrying a 'self-check' comment (pre-fix: NOT caught)" \
  '  perf stat -x, -e "$EVENTS" -p "$SERVER_PID" -o "$outfile" -- "$@"  # not part of the self-check'

# --- BYPASSES 3-5, found by review round 1 in the fix for bypasses 1-2 -------
# All three are ordinary bash, and each was MEASURED against the item-10 pattern
# `perf stat[^|]*(-p([[:space:]]|[0-9"$])|--pid([[:space:]]|=))` as NOT MATCHING:
#
#   * a SINGLE-QUOTED attached value — the attached-value class `[0-9"$]` omits `'`;
#   * an invocation through a VARIABLE — the pattern anchored the literal word `perf`
#     immediately before `stat`;
#   * a GLOBAL OPTION between the two words — the same adjacency anchor.
#
# Three bypasses in a fix for two bypasses is why the mechanism is now an ALLOWLIST
# (perf is invoked in ONE wrapper; every other perf/stat invocation line must be
# explicitly marked) plus a per-TOKEN option check plus a RUNTIME argv check. The
# allowlist is closed by construction: it does not ask what a line looks like, it asks
# where it is — so a spelling nobody anticipated still fires.
expect_selfgrep_fires "a SINGLE-QUOTED attached -p'<pid>' (pre-fix: NOT caught)" \
  '  perf stat -x, -e "$EVENTS" -p'"'"'1234'"'"' -o "$outfile" -- "$@"'
expect_selfgrep_fires "an invocation through a VARIABLE, \$PERF_BIN stat -p (pre-fix: NOT caught)" \
  '  "$PERF_BIN" stat -x, -p 1 -o "$outfile" -- "$@"'
expect_selfgrep_fires "a GLOBAL OPTION between the words, perf --no-pager stat -p (pre-fix: NOT caught)" \
  '  perf --no-pager stat -x, -p 1 -o "$outfile" -- "$@"'
# The allowlist half, independent of any option spelling: a SECOND perf invocation
# somewhere other than the wrapper is refused even when it is CPU-wide and perfectly
# correct, because "one place invokes perf" is what makes the guard closed. No
# deny-list pattern could catch this at all — there is nothing forbidden on the line.
# The replacement closes `perf_stat_c` early, adds an outside invocation, and opens a
# throwaway function so the driver's original `}` still balances — so the injected
# line is genuinely OUTSIDE the wrapper, which is the property under test.
expect_selfgrep_fires "a SECOND, CPU-wide perf invocation outside the wrapper (no deny-list could see it)" \
  '  perf stat -x, -e "$EVENTS" -C "$SERVER_CPUS" -o "$outfile" -- "$@"
}
perf stat -x, -C 0 -o /dev/null -- true
_ws0_injected_noop() {'

# --- NEGATIVE direction: the driver AS SHIPPED passes its own check --------
# The half that keeps this from being a guard that reds unconditionally — which is
# the guard an operator deletes. The shipped driver must get PAST the self-check.
#
# AFFIRMATIVE, and STRONGER than what it replaced (#3272 review round 3, B1). It used to
# run the driver BARE and assert it reached the unresolvable `--corpus` complaint, i.e. a
# checkpoint BELOW the argument boundary — which is the leak class B1 is about, and it is
# also a weaker witness than it looks: the corpus stat is several host-dependent checks
# past the lint. The `ARGUMENTS OK` stamp is emitted by the boundary, which sits strictly
# AFTER `perf_invocation_lint_tree` in the driver's source order, so reaching it proves
# the lint passed — and it proves it without touching the world.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent); rc=$?
if [ "$rc" -eq 0 ] && grep -q "ARGUMENTS OK" <<<"$out" \
   && ! grep -q "per-process" <<<"$out" && ws0_driver_ran_hermetically; then
  pass "selfgrep-silent: the shipped driver passes its own lint and reaches the argument boundary (affirmative, hermetic)"
else
  fail "selfgrep-silent: the shipped driver must pass its lint and reach ARGUMENTS OK (rc=$rc, calls: $(ws0_hermetic_calls), out: $(head -3 <<<"$out"))"
fi
# The lint's POSITION is what makes the stamp a witness for it, so that is asserted
# structurally rather than assumed from the driver's current shape.
lint_line=$(grep -n '^_perf_lint_out="\$(perf_invocation_lint_tree' "$DRIVER" | head -1 | cut -d: -f1)
stamp_line=$(grep -n '^  echo "ARGUMENTS OK' "$DRIVER" | head -1 | cut -d: -f1)
if [ -n "$lint_line" ] && [ -n "$stamp_line" ] && [ "$lint_line" -lt "$stamp_line" ]; then
  pass "selfgrep-silent: the perf lint (line $lint_line) runs BEFORE the ARGUMENTS OK stamp (line $stamp_line), so the stamp witnesses it"
else
  fail "the perf lint must precede the argument boundary, else ARGUMENTS OK is not evidence the lint ran (lint=$lint_line stamp=$stamp_line)"
fi

# Stated directly too, using the driver's OWN lint function rather than a second
# hand-written pattern: a reimplemented check in the test would be a second thing to
# keep in sync, and its divergence would be invisible in exactly the permissive
# direction. Sourced by extraction so nothing in the driver's body runs.
lint_shipped() { # lint_shipped <file> [owner|library]
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$PERF_LINT_LIB"
    perf_invocation_lint "$1" "${2:-owner}" )
}
if [ -z "$(lint_shipped "$DRIVER")" ]; then
  pass "selfgrep-real: the shipped driver is clean under its OWN lint (no second pattern to drift)"
else
  fail "selfgrep-real: the SHIPPED driver violates its own lint: $(lint_shipped "$DRIVER")"
fi
# --- R2: THE LINT'S SUBJECT IS THE WHOLE RIG, and it is DISCOVERED ----------
# The driver's runtime lint used to read `${BASH_SOURCE[0]}` — ITSELF — so the FOUR
# libraries it sources were inside the rig and outside all three layers. And the
# compensating loop here covered only `lib-cpu.sh` + `lib-perf-lint.sh`: the two libraries
# round 1 CREATED (`lib-host-state.sh`, `lib-args.sh`) were never linted at all, so a
# `perf stat -p "$SERVER_PID"` planted in either fired NOTHING (#3272 review round 2, R2).
#
# So the subject is now a DIRECTORY GLOB, and this asserts the SET rather than trusting
# it: `perf_lint_tree_subject` must name EVERY `scripts/perf/*.sh` on disk. A
# hand-maintained list drifts the moment someone adds a library — which is exactly what
# happened — so the drift is caught by comparing against `ls`, not by remembering.
lint_tree() {
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$PERF_LINT_LIB"
    perf_invocation_lint_tree "$1" )
}
lint_subject() {
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$PERF_LINT_LIB"
    perf_lint_tree_subject "$1" )
}
PERF_DIR="$REPO_ROOT/scripts/perf"
on_disk=$(cd "$PERF_DIR" && ls -1 ./*.sh | sed 's#^\./##' | sort)
subject=$(lint_subject "$PERF_DIR" | xargs -n1 basename | sort)
n_on_disk=$(printf '%s\n' "$on_disk" | grep -c .)
if [ "$n_on_disk" -ge 4 ] && [ "$subject" = "$on_disk" ]; then
  pass "lint-subject: the tree lint covers ALL $n_on_disk scripts/perf/*.sh (discovered, not enumerated)"
else
  fail "lint-subject: the tree lint's subject ($subject) is not every scripts/perf/*.sh ($on_disk)"
fi
# The shipped tree must be CLEAN under its own tree lint — the negative direction, which
# is what keeps a guard that reds unconditionally from passing every positive case.
if [ -z "$(lint_tree "$PERF_DIR")" ]; then
  pass "lint-tree: the SHIPPED scripts/perf tree is clean under the tree lint"
else
  fail "lint-tree: the shipped tree violates its own tree lint: $(lint_tree "$PERF_DIR")"
fi
# NON-VACUITY, per library, per option spelling: a counting-domain option planted in ANY
# of the rig's files must FIRE. The pre-round-2 loop covered two of five files and one
# option family; MEASURED against it, a `-p` in `lib-host-state.sh` or `lib-args.sh`, and
# a `-t` anywhere at all, produced NO finding.
for libname in ws0-baseline.sh lib-cpu.sh lib-host-state.sh lib-args.sh lib-perf-lint.sh; do
  for spelling in '-p 1234' '-p1234' '--pid=1234' '-t 1234' '--tid=1234'; do
    treedir="$(mktemp -d "$TMP/treeXXXXXX")"
    cp "$PERF_DIR/"*.sh "$treedir/"
    # Appended as a REAL invocation line, outside any function, so the plant is
    # structurally inside the rig exactly as an edit would be.
    printf 'perf stat -x, -e cycles -C 0 %s -o /dev/null -- true\n' "$spelling" >> "$treedir/$libname"
    got=$(lint_tree "$treedir")
    if grep -q "$libname" <<<"$got" \
       && grep -qE 'per-(process|thread) option token' <<<"$got"; then
      pass "lint-tree-libs: a '${spelling%% *}' planted in $libname FIRES"
    else
      fail "lint-tree-libs: '$spelling' in $libname must fire (got: $got)"
    fi
  done
done
# An UNKNOWN option is refused too — the allowlist half, which is the property an
# enumeration can never have. `--per-thread` and `-a` are real perf options this rig does
# not use; neither appears in any deny list, and both change the counting domain.
for spelling in '--per-thread' '-a' '--cgroup=x'; do
  treedir="$(mktemp -d "$TMP/treeXXXXXX")"
  cp "$PERF_DIR/"*.sh "$treedir/"
  printf 'perf stat -x, -e cycles -C 0 %s -o /dev/null -- true\n' "$spelling" >> "$treedir/lib-args.sh"
  got=$(lint_tree "$treedir")
  if grep -q 'not in the perf option allowlist' <<<"$got"; then
    pass "lint-tree-allowlist: an UNANTICIPATED option '$spelling' FAILS CLOSED (no deny-list entry needed)"
  else
    fail "lint-tree-allowlist: '$spelling' must fail closed (got: $got)"
  fi
done
# The tree lint's own VACUITY guards: an empty subject, no wrapper, or two wrappers all
# print NOTHING under a naive implementation and read exactly like a clean tree.
emptydir="$(mktemp -d "$TMP/emptyXXXXXX")"
if grep -q 'subject is EMPTY' <<<"$(lint_tree "$emptydir")"; then
  pass "lint-tree-vacuity: a directory with NO *.sh is a FINDING (not a silent clean tree)"
else
  fail "lint-tree-vacuity: an empty subject must be reported (got: $(lint_tree "$emptydir"))"
fi
nowrap="$(mktemp -d "$TMP/nowrapXXXXXX")"
printf 'echo hello\n' > "$nowrap/a.sh"
if grep -q 'no file defines perf_stat_c' <<<"$(lint_tree "$nowrap")"; then
  pass "lint-tree-vacuity: a tree with NO wrapper is a FINDING (nothing owns the invocation)"
else
  fail "lint-tree-vacuity: an absent wrapper must be reported (got: $(lint_tree "$nowrap"))"
fi
twowrap="$(mktemp -d "$TMP/twowrapXXXXXX")"
cp "$PERF_DIR/"*.sh "$twowrap/"
printf 'perf_stat_c() {\n  perf stat -x, -C 0 -o "$1" -- "$2"\n}\n' >> "$twowrap/lib-args.sh"
if grep -q 'define perf_stat_c' <<<"$(lint_tree "$twowrap")"; then
  pass "lint-tree-vacuity: TWO wrapper definitions are a FINDING (layer 1 allows exactly one)"
else
  fail "lint-tree-vacuity: two wrappers must be reported (got: $(lint_tree "$twowrap"))"
fi
# A per-file lint of the SHIPPED libraries, asserting an AFFIRMATIVE subject count rather
# than a `0/0` pass (#3272 review round 2 nit). The old loop filtered the lint's output
# for 'per-process option token' and passed on an EMPTY result — but no line of those
# files is classified as a perf invocation at all, so the option filter had nothing to
# examine and the case could not fail. A check whose subject is empty is not a pass. Here
# each library is linted in `library` mode (which also refuses a second wrapper), and the
# COUNT of lines the lint actually classified is asserted non-zero via a planted control.
for lib in "$PERF_DIR"/lib-*.sh; do
  base="$(basename "$lib")"
  if [ -z "$(lint_shipped "$lib" library)" ]; then
    pass "selfgrep-libs: $base is clean in library mode (no wrapper, no perf invocation)"
  else
    fail "selfgrep-libs: $base is not clean in library mode: $(lint_shipped "$lib" library)"
  fi
  # THE AFFIRMATIVE SUBJECT: the same file with ONE invocation planted must produce a
  # finding, which proves the clean verdict above was over a file the lint can actually
  # read — not a `0/0`.
  ctl="$TMP/ctl-$base"
  cp "$lib" "$ctl"
  printf 'perf stat -x, -e cycles -C 0 -p 1234 -o /dev/null -- true\n' >> "$ctl"
  if [ -n "$(lint_shipped "$ctl" library)" ]; then
    pass "selfgrep-libs: ...and the SAME file with one planted invocation FIRES (the subject is non-empty)"
  else
    fail "selfgrep-libs: a planted invocation in $base must fire, else the clean verdict is a 0/0"
  fi
done
# And the driver must SOURCE the lint before using it — a lint library present but
# unsourced would make `perf_invocation_lint` an unbound command, which under
# `set -euo pipefail` in a command substitution is a confusing failure rather than a
# refusal (the #3249 "wired?" question).
if grep -q 'source "\$HERE/lib-perf-lint.sh"' "$DRIVER"; then
  pass "selfgrep-wired: the driver SOURCES lib-perf-lint.sh"
else
  fail "selfgrep-wired: the driver must source lib-perf-lint.sh"
fi
# And the guard must still be WIRED (a lint present but never called is the #3249
# shape), over the WHOLE DIRECTORY rather than `${BASH_SOURCE[0]}` (R2): the driver runs
# it unconditionally at parse time and exits on any output.
if grep -q '_perf_lint_out="\$(perf_invocation_lint_tree "\$HERE")"' "$DRIVER" \
   && awk '/^if \[\[ -n "\$_perf_lint_out" \]\]/,/^fi$/' "$DRIVER" | grep -q 'exit 2'; then
  pass "selfgrep-wired: the lint runs over the whole scripts/perf TREE at startup and exits on any finding"
else
  fail "selfgrep-wired: the lint must run over the tree (perf_invocation_lint_tree \"\$HERE\") unconditionally and exit"
fi
# END-TO-END: a plant in a LIBRARY must stop the DRIVER, not merely the lint function.
# This is the wiring half of R2 — the lint could be correct and the driver still run.
for libname in lib-host-state.sh lib-args.sh; do
  treedir="$(mktemp -d "$TMP/e2eXXXXXX")"
  mkdir -p "$treedir/scripts/perf"
  cp "$PERF_DIR/"*.sh "$treedir/scripts/perf/"
  # Inside a FUNCTION BODY, and with a LITERAL pid. Both matter: the libraries are
  # SOURCED, so a top-level plant would EXECUTE at source time (`perf: command not found`,
  # rc=127) and an unbound `$SERVER_PID` would die under the driver's `set -u` — either
  # way the fixture would report the wrong failure, before the lint ever ran. A function
  # body is structurally inside the rig exactly as a real edit would be, and never runs.
  printf '_ws0_planted() {\n  perf stat -x, -e cycles -C 0 -t 1234 -o /dev/null -- true\n}\n' \
    >> "$treedir/scripts/perf/$libname"
  out=$(ws0_driver_run "$treedir/scripts/perf/ws0-baseline.sh" --corpus /nonexistent); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "per-thread option token" <<<"$out" \
     && grep -q "$libname" <<<"$out"; then
    pass "selfgrep-e2e: a per-thread invocation in $libname STOPS THE DRIVER, naming the file"
  else
    fail "selfgrep-e2e: a plant in $libname must stop the driver (rc=$rc, out: $(head -4 <<<"$out"))"
  fi
done

# --- The lint's own POSITIVE CONTROL and non-vacuity ------------------------
# Per #3249 the greps above are only evidence if the lint can both FIRE and STAY
# SILENT. A `perf_invocation_lint` hardcoded to print nothing would satisfy every
# `selfgrep-silent` case; one hardcoded to print something would satisfy every
# `selfgrep-fires` case. So it is driven directly over minimal synthetic files.
lint_probe() { # lint_probe <file-content> — echoes the lint's findings
  printf '%s\n' "$1" > "$TMP/lint-probe.sh"
  lint_shipped "$TMP/lint-probe.sh"
}
WRAP=$'perf_stat_c() {\n  perf stat -x, -C "$SERVER_CPUS" -o "$1" -- "$2"\n}'
if [ -z "$(lint_probe "$WRAP")" ]; then
  pass "lint-control: a file whose ONLY invocation is the CPU-wide wrapper is CLEAN"
else
  fail "lint-control: the minimal clean file must lint clean: $(lint_probe "$WRAP")"
fi
# The three END assertions: the allowlist may not be vacuous.
if grep -q 'perf_stat_c() is absent' <<<"$(lint_probe 'echo hello')"; then
  pass "lint-nonvacuous: a file with NO wrapper is flagged (the allowlist has nothing to allow)"
else
  fail "lint-nonvacuous: an absent wrapper must be flagged (got: $(lint_probe 'echo hello'))"
fi
NO_C=$'perf_stat_c() {\n  perf stat -x, -o "$1" -- "$2"\n}'
if grep -q 'does not pass -C' <<<"$(lint_probe "$NO_C")"; then
  pass "lint-nonvacuous: a wrapper that passes NO -C is flagged (it counts nothing CPU-wide)"
else
  fail "lint-nonvacuous: a wrapper without -C must be flagged (got: $(lint_probe "$NO_C"))"
fi
EMPTY_WRAP=$'perf_stat_c() {\n  :\n}'
if grep -q 'invokes nothing' <<<"$(lint_probe "$EMPTY_WRAP")"; then
  pass "lint-nonvacuous: an EMPTY wrapper is flagged (the allowlist would allow nothing)"
else
  fail "lint-nonvacuous: an empty wrapper must be flagged (got: $(lint_probe "$EMPTY_WRAP"))"
fi
# The option check applies INSIDE the wrapper too — where the allowlist has nothing to
# say, since the line is exactly where it is supposed to be.
IN_WRAP=$'perf_stat_c() {\n  perf stat -x, -C "$SERVER_CPUS" -p1234 -o "$1" -- "$2"\n}'
if grep -q 'per-process option token' <<<"$(lint_probe "$IN_WRAP")"; then
  pass "lint-inside-wrapper: a per-process option INSIDE the wrapper is still flagged"
else
  fail "lint-inside-wrapper: the option check must apply inside the wrapper (got: $(lint_probe "$IN_WRAP"))"
fi
# ...and the MARKER exempts a line from the ALLOWLIST only, never from the option
# check — otherwise the marker would be a one-comment bypass, which is bypass 2 again.
MARKED_P=$'perf_stat_c() {\n  perf stat -x, -C 1 -o "$1" -- "$2"\n}\nperf stat -p 1  # perf-lint-allow'
if grep -q 'per-process option token' <<<"$(lint_probe "$MARKED_P")"; then
  pass "lint-marker-scope: the allow-marker does NOT exempt a per-process option (bypass 2 cannot return)"
else
  fail "lint-marker-scope: a marked line must still be option-checked (got: $(lint_probe "$MARKED_P"))"
fi
# The IDENTIFIER false-positive direction: a guard that reds on `perf_stat_c`,
# `perf_event_paranoid` or a `target/perf-…` path is the guard an operator deletes.
IDENTS=$(printf '%s\n' \
  'perf_stat_c() {' \
  '  perf stat -x, -C 1 -o "$1" -- "$2"' \
  '}' \
  'PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid)"' \
  'OUT_DIR="$REPO_ROOT/target/perf-ws0-3096/$TS"' \
  'printf "%s\n" "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"' \
  'perf_stat_c "$OUT_DIR/perf-$tag.csv" taskset -c 1 true')
if [ -z "$(lint_probe "$IDENTS")" ]; then
  pass "lint-no-false-positive: identifiers/paths containing 'perf' or 'stat' are NOT flagged"
else
  fail "lint-no-false-positive: identifiers must not red the lint (got: $(lint_probe "$IDENTS"))"
fi

# --- LAYER 3: the RUNTIME argv guard inside perf_stat_c ---------------------
# The layer no source scan can substitute for: a caller passing a COMPUTED option, or
# one built by `eval`, is invisible to any text check but arrives here as a plain
# token — bash has already done word-splitting and quote removal, so `-p'1234'`,
# `-p1234` and `-p "$x"` are indistinguishable by the time this runs.
argv_probe() { # argv_probe <args…> — run the driver's perf_stat_c with a perf shim
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$PERF_LINT_LIB"          # supplies $_PP_SHORT / $_PP_LONG
    EVENTS="cycles"; SERVER_CPUS="0"
    perf() { printf 'PERF-RAN: %s\n' "$*"; }
    eval "$(awk '/^perf_stat_c\(\)/,/^}/' "$DRIVER")"
    perf_stat_c /dev/null "$@" ) 2>&1
}
# LAYER 3 PREFIX = an ALLOWLIST OF NOTHING (#3272 review round 2, R4b). Round 1's version
# enumerated `-p`/`--pid`, so `-t`/`--tid` — per-THREAD counting, equally per-process in
# effect and with the same observer cost — went straight through. MEASURED against that
# wrapper: `perf_stat_c out -t 1234 true` invoked perf with rc=0. Enumerating `-t` too
# would be the same mistake a third time, so a caller-supplied option before the command
# word is now refused WHATEVER IT IS.
for spelled in "-p1234" "-p" "--pid=1234" "--pid" "-t" "-t1234" "--tid=99" "--per-thread" "-a" "--cgroup=x" "--future-option"; do
  out=$(argv_probe "$spelled" true); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "was passed the perf option" <<<"$out" \
     && ! grep -q 'PERF-RAN' <<<"$out"; then
    pass "argv-guard: perf_stat_c REFUSES a caller-supplied '$spelled' before invoking perf"
  else
    fail "argv-guard: '$spelled' must be refused and perf must not run (rc=$rc, out: $out)"
  fi
done
# A single-quoted attached value is the SAME TOKEN here — the spelling problem does
# not exist at this layer, which is the point of having it.
out=$(argv_probe "-p'1234'" true)
if grep -q "was passed the perf option" <<<"$out"; then
  pass "argv-guard: a single-quoted attached value is the same token after quote removal"
else
  fail "argv-guard: -p'1234' must be refused (out: $out)"
fi
# AFTER the command word an allowlist is impossible (`$@` carries `--shape full`,
# `--corpus …`), so the check there is necessarily the counting-DOMAIN enumeration — and
# it must still fire, because a domain option past perf's `--` would never be read by
# perf at all and the measurement would silently not be the one asked for.
for spelled in "-p" "--pid=1" "-t" "--tid=1" "--per-thread" "-a"; do
  out=$(argv_probe true "$spelled"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "counting-domain option" <<<"$out" \
     && ! grep -q 'PERF-RAN' <<<"$out"; then
    pass "argv-guard: a domain option '$spelled' AFTER the command word is refused too"
  else
    fail "argv-guard: a trailing '$spelled' must be refused (rc=$rc, out: $out)"
  fi
done
# ...and an ordinary COMMAND option must NOT be: a guard that refuses `--shape full`
# refuses every real call, which is the guard an operator deletes.
out=$(argv_probe taskset -c 1 flight-loadgen --shape full --step-duration 45s)
if grep -q 'PERF-RAN: stat -x, -e cycles -C 0 -o /dev/null -- taskset -c 1 flight-loadgen --shape full --step-duration 45s' <<<"$out"; then
  pass "argv-guard: ordinary COMMAND options (--shape/--step-duration) pass through untouched"
else
  fail "argv-guard: a command's own options must not be refused (out: $out)"
fi
# And the ACCEPT direction: an ordinary argv reaches perf, so the guard is not one
# that refuses everything.
out=$(argv_probe taskset -c 1 /bin/true)
if grep -q 'PERF-RAN: stat -x, -e cycles -C 0 -o /dev/null -- taskset -c 1 /bin/true' <<<"$out"; then
  pass "argv-guard: an ordinary argv is passed through, CPU-wide, with -C (the accept half)"
else
  fail "argv-guard: a clean argv must reach perf with -C (out: $out)"
fi

# ===========================================================================
# PART 3 — THE FLIGHT SERVER WE MEASURE IS THE ONE WE STARTED (#3272 B4)
# ===========================================================================
# The third structural guard, and it belongs in THIS file rather than the reporter's: it
# decides whether an observation is of the right PROGRAM, which is the same question PART 1
# asks about the right CPUs.
#
# # The defect
#
# Readiness was inferred SOLELY from `(echo >/dev/tcp/127.0.0.1/$PORT)` succeeding. If our
# server FAILS TO BIND and another process holds the port, that probe succeeds on the first
# attempt, the load generator measures THAT server, and `perf stat -C` counts OUR pinned
# CPUs. The figure is published as `flight_do_get_<arm>` for a program the rig did not start
# and cannot name. The `for i in $(seq 1 120)` loop's exhaustion was not an error either —
# it just ended, and the measurement proceeded.
#
# #3096's `require_port_free` does NOT cover this, and the distinction is the finding: it
# runs BEFORE the spawn and answers "is the port free now". The case here is a port free at
# preflight and held by someone else at measurement time — our bind losing a race, or
# failing for its own reason while a peer's server binds in the gap.
#
# Driven against `lib-server.sh` as SHIPPED, sourced rather than re-implemented, with a
# REAL listener started by python3 on a high port so the ownership question is a real one.
SERVER_LIB="$REPO_ROOT/scripts/perf/lib-server.sh"
if [ ! -f "$SERVER_LIB" ]; then
  fail "server-owner: missing $SERVER_LIB"
else
  pass "server-owner: lib-server.sh is present (the shipped implementation, not a copy)"
fi
# A port the KERNEL chose, not a number picked as "probably free" (#3272 review round 3,
# found while writing this: a hardcoded 18931 collided with a leftover listener from an
# earlier run of this very file and made four cases fail for the wrong reason — the
# fixture's own listener could not bind, so the ownership check reported "the server
# exited"). The helper binds port 0, reads back what it was given, and releases it; a
# fixture that guesses is a fixture whose failures are unattributable.
free_port() {
  python3 -c '
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
'
}
PROBE_PORT="$(free_port)"
if [ -n "$PROBE_PORT" ] && [ "$PROBE_PORT" -gt 0 ] 2>/dev/null; then
  pass "server-owner: the fixture uses a KERNEL-ASSIGNED port ($PROBE_PORT), never a guessed one"
else
  fail "server-owner: could not obtain a free port; every case below would be unattributable"
fi
# server_lib_call <function> [args…] — source the SHIPPED lib and run one function with
# `$PORT`/`$OUT_DIR`/`$SERVER_PID` set, in a subshell so no case inherits another's state.
server_lib_call() {
  local fn="$1"; shift
  ( set -uo pipefail
    PORT="$PROBE_PORT"; OUT_DIR="$TMP"; SERVER_PID="${SERVER_PID:-}"
    # shellcheck disable=SC1090
    source "$SERVER_LIB"
    "$fn" "$@" ) 2>&1
}
# start_listener <port> — a real TCP listener; echoes its pid.
#
# It ACCEPTS AND CLOSES in a loop, and the backlog is 64, neither of which is incidental
# (#3272 review round 3, found while writing this). The first version was
# `s.listen(1)` + `time.sleep(60)` — never accepting — so the wait-loop's own connection
# filled the single backlog slot and stayed queued, and EVERY LATER CONNECT FAILED. The
# ownership cases then reported "the server exited" (the `kill -0` branch was reached on the
# next iteration) instead of exercising the check under test: a fixture failing for a reason
# that looks like the thing it is testing. A real Flight server accepts, so the fixture
# should too.
start_listener() {
  python3 -c '
import socket, sys, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1])))
s.listen(64)
s.settimeout(60)
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    try:
        c, _ = s.accept()
        c.close()
    except OSError:
        break
' "$1" >/dev/null 2>&1 &
  echo $!
}
wait_listening() { # wait_listening <port>
  local i
  for i in $(seq 1 40); do
    (echo >"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  return 1
}

# --- 3a. THE PROBER CAN ANSWER, on a socket whose owner we KNOW ---------------
# Every ownership check below reads a verdict off `socket_owner_pid`, so a prober that
# returns nothing for every port would make them all pass vacuously — a positive verdict
# from an oracle that cannot answer. This is the positive control, and it is also the
# fixture's own non-vacuity: if it fails, the cases after it prove nothing.
LISTENER_PID="$(start_listener "$PROBE_PORT")"
if wait_listening "$PROBE_PORT"; then
  observed="$(server_lib_call socket_owner_pid "$PROBE_PORT")"
  if [ "$observed" = "$LISTENER_PID" ]; then
    pass "server-owner: socket_owner_pid IDENTIFIES a listener we started (pid $LISTENER_PID) — the oracle can answer"
  else
    fail "server-owner: socket_owner_pid must identify a known listener (expected $LISTENER_PID, got '${observed:-<nothing>}')"
  fi
else
  fail "server-owner: the fixture's own listener never came up on $PROBE_PORT"
fi

# --- 3b. A FOREIGN listener on the port is REFUSED, naming it -----------------
# THE HEADLINE CASE. `SERVER_PID` is a live process that is NOT the listener — exactly the
# shape of "our server failed to bind and someone else holds the port". Pre-fix, the
# connect-probe succeeded on the first attempt and the measurement RAN.
sleep 60 & FOREIGN_SELF=$!      # a live pid that owns no socket, standing in for our server
out=$( SERVER_PID="$FOREIGN_SELF" server_lib_call await_server_ready probe-rep ); rc=$?
if [ "$rc" -ne 0 ] && grep -q "is being served by pid $LISTENER_PID" <<<"$out" \
   && grep -q "NOT the Flight server this rig started" <<<"$out"; then
  pass "server-owner: OBSERVED — a FOREIGN listener on the port is REFUSED, naming the pid that owns it (B4)"
else
  fail "server-owner: a foreign listener must be refused naming its pid (rc=$rc, out: $(head -4 <<<"$out"))"
fi
# The refusal must say WHY it matters — that the wrong program would be measured — rather
# than only that a pid differs.
if grep -q "would measure THAT server while perf counted OUR" <<<"$out" \
   && grep -q "proves only that SOMETHING is listening" <<<"$out"; then
  pass "server-owner: the refusal names the CONSEQUENCE (a figure attributed to another program)"
else
  fail "server-owner: the refusal must explain what a port-accepts check cannot establish (out: $out)"
fi
# NON-VACUITY for this case against the PRE-FIX logic, stated as the assertion it replaces:
# the connect-probe ALONE succeeds here, so a check built on it would have proceeded.
if (echo >"/dev/tcp/127.0.0.1/$PROBE_PORT") >/dev/null 2>&1; then
  pass "server-owner: NON-VACUITY — the port DOES accept connections, so the pre-fix probe would have proceeded"
else
  fail "server-owner: the fixture must have an accepting port, or the case is not the pre-fix one"
fi

# --- 3c. OUR OWN listener is ACCEPTED (the guard is not one that refuses all) --
out=$( SERVER_PID="$LISTENER_PID" server_lib_call await_server_ready probe-rep ); rc=$?
if [ "$rc" -eq 0 ] && grep -q "owns 127.0.0.1:$PROBE_PORT" <<<"$out"; then
  pass "server-owner: OUR OWN listener is ACCEPTED and the ownership is stated (the accept half)"
else
  fail "server-owner: a server we started must be accepted (rc=$rc, out: $out)"
fi
kill "$FOREIGN_SELF" 2>/dev/null || true
kill "$LISTENER_PID" 2>/dev/null || true
wait "$FOREIGN_SELF" 2>/dev/null || true
wait "$LISTENER_PID" 2>/dev/null || true

# --- 3c2. A DESCENDANT of our server is ACCEPTED, and only a real descendant --
# `await_server_ready` deliberately accepts a listener that is a DESCENDANT of the pid we
# launched, because a supervisor that forks its listener is a legitimate shape. That
# leniency is exactly where an over-broad test lets a foreign process through, so
# `descends_from` is driven in BOTH directions.
#
# NON-VACUITY: the first version compared PROCESS GROUPS, which looked equivalent and is
# not — every background job of one shell inherits that shell's pgid, so the foreign
# listener in 3b and the stand-in server shared one. MEASURED against that version, 3b
# printed `server ready (pid 28133, a child of 28173, …)` for two processes whose only
# relationship was a common parent: the guard accepting the situation it exists to refuse.
descends_probe() { # descends_probe <pid> <ancestor> — echoes YES or NO
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$SERVER_LIB"
    descends_from "$1" "$2" && echo YES || echo NO )
}
# A real grandchild: a shell that forks a shell that sleeps.
GRANDPARENT_OUT="$TMP/grandchild.pid"
bash -c 'bash -c "sleep 30 & echo \$! > '"$GRANDPARENT_OUT"'; sleep 30" ' >/dev/null 2>&1 &
GRANDPARENT_PID=$!
for _i in $(seq 1 40); do [ -s "$GRANDPARENT_OUT" ] && break; sleep 0.25; done
GRANDCHILD_PID="$(cat "$GRANDPARENT_OUT" 2>/dev/null || true)"
if [ -n "$GRANDCHILD_PID" ] && kill -0 "$GRANDCHILD_PID" 2>/dev/null; then
  pass "server-owner: the ancestry fixture built a real grandchild ($GRANDCHILD_PID under $GRANDPARENT_PID)"
else
  fail "server-owner: could not build a grandchild; the ancestry cases would be unattributable"
fi
if [ "$(descends_probe "$GRANDCHILD_PID" "$GRANDPARENT_PID")" = "YES" ]; then
  pass "server-owner: descends_from ACCEPTS a real GRANDCHILD (a supervisor that forks is legitimate)"
else
  fail "server-owner: descends_from must accept a descendant more than one hop away"
fi
# A SIBLING — the pgid-match false accept, stated as its own case.
sleep 30 & SIBLING_A=$!
sleep 30 & SIBLING_B=$!
if [ "$(descends_probe "$SIBLING_A" "$SIBLING_B")" = "NO" ]; then
  pass "server-owner: OBSERVED — descends_from REFUSES a SIBLING (the pgid check accepted this)"
else
  fail "server-owner: two background jobs of one shell are NOT related; a pgid match said they were"
fi
# ...and the pgid check really would have said YES, so the case above is the pre-fix one.
if [ "$(ps -o pgid= -p "$SIBLING_A" | tr -d ' ')" = "$(ps -o pgid= -p "$SIBLING_B" | tr -d ' ')" ]; then
  pass "server-owner: NON-VACUITY — those two siblings DO share a pgid, so the pre-fix check accepted them"
else
  fail "server-owner: the sibling fixture must share a pgid, or it is not the pre-fix case"
fi
# A pid IS its own ancestor (the ordinary match), and an unrelated pid is not.
if [ "$(descends_probe "$SIBLING_A" "$SIBLING_A")" = "YES" ] \
   && [ "$(descends_probe 1 "$SIBLING_A")" = "NO" ]; then
  pass "server-owner: descends_from accepts identity and refuses an unrelated pid"
else
  fail "server-owner: descends_from must accept identity and refuse an unrelated pid"
fi
kill "$GRANDPARENT_PID" "$GRANDCHILD_PID" "$SIBLING_A" "$SIBLING_B" 2>/dev/null || true
wait "$GRANDPARENT_PID" "$SIBLING_A" "$SIBLING_B" 2>/dev/null || true

# --- 3d. A DEAD server is refused BEFORE the port is consulted ----------------
# A dead child cannot be what a live socket belongs to, and the diagnostic must name the
# process rather than the port — the two causes have different remedies.
sleep 0 & DEAD_PID=$!; wait "$DEAD_PID" 2>/dev/null || true
out=$( SERVER_PID="$DEAD_PID" server_lib_call await_server_ready probe-rep ); rc=$?
if [ "$rc" -ne 0 ] && grep -q "is not" <<<"$out" && grep -q "exited before serving" <<<"$out"; then
  pass "server-owner: a server that EXITED is refused, naming the process (not the port)"
else
  fail "server-owner: a dead server must be refused with a process diagnostic (rc=$rc, out: $out)"
fi

# --- 3e. A READINESS TIMEOUT IS FATAL ----------------------------------------
# The pre-fix loop's exhaustion was SILENT: `for i in $(seq 1 120)` simply ended and the
# measurement proceeded against a dead port. Driven with the loop bound reduced to 1 (via
# an extracted copy) so the case costs a second rather than two minutes — the extraction is
# from the SHIPPED function, and the substitution is asserted to have applied.
timeout_probe="$TMP/timeout-probe.sh"
python3 - "$SERVER_LIB" "$timeout_probe" <<'PY'
import pathlib, re, sys
src, dst = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
s = src.read_text()
body = re.search(r"^await_server_ready\(\) \{.*?^\}", s, re.S | re.M)
if not body:
    sys.exit("await_server_ready not found in lib-server.sh — this fixture is stale")
patched, n = re.subn(r"seq 1 120", "seq 1 1", body.group(0))
if n != 1:
    sys.exit(f"expected exactly one `seq 1 120` in await_server_ready, found {n}")
dst.write_text("set -uo pipefail\nsocket_owner_pid() { :; }\n" + patched
               + '\nawait_server_ready "$1"\n')
PY
out=$(PORT="$(free_port)" OUT_DIR="$TMP" SERVER_PID=$$ bash "$timeout_probe" probe-rep 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "did not begin serving" <<<"$out" \
   && grep -q "TIMEOUT is a failure" <<<"$out"; then
  pass "server-owner: OBSERVED — a readiness TIMEOUT is FATAL (pre-fix: the loop ended and the run proceeded)"
else
  fail "server-owner: a readiness timeout must exit non-zero (rc=$rc, out: $out)"
fi

# --- 3f. AN UNANSWERABLE PROBER STOPS THE RUN --------------------------------
# `require_socket_prober` is what makes every check above non-vacuous, so its own failure
# path must fire: pointed at a port with NO listener, it must refuse rather than conclude
# that ownership is fine.
out=$( server_lib_call require_socket_prober ); rc=$?
if [ "$rc" -eq 0 ] && grep -q "verified prober" <<<"$out"; then
  pass "server-owner: require_socket_prober PASSES against a listener it starts itself"
else
  fail "server-owner: the prober check must pass on a working host (rc=$rc, out: $out)"
fi
# THE PORT IS KERNEL-ASSIGNED, and that is asserted rather than assumed (#3272 review round 4
# nit). The caller used to pass `$((PORT + 1))` — a port NOTHING had checked free, since
# `require_port_free` covers `$PORT` only. Two failures: `--port 65535` asked for 65536, and an
# OCCUPIED `PORT+1` made the python `bind` fail while the wait-loop connect SUCCEEDED against the
# foreign listener, so `observed != probe_pid` and a CORRECT run died with "the prober cannot
# answer" — whose three stated causes (an `ss` without process info, no /proc visibility,
# insufficient privilege) were all WRONG. A diagnosis naming the wrong cause sends the operator
# to fix a working tool.
if grep -q "kernel-assigned port" <<<"$out"; then
  pass "server-owner: the prober's own listener uses a KERNEL-ASSIGNED port (collision-free by construction)"
else
  fail "server-owner: the prober must report a kernel-assigned port (out: $out)"
fi
# It must ACCEPT NO PORT ARGUMENT, i.e. the collision hazard is gone by construction rather than
# by a check someone has to keep correct.
if awk '/^require_socket_prober\(\)/,/^}/' "$SERVER_LIB" | grep -q 'bind(("127.0.0.1", 0))' \
  && ! awk '/^require_socket_prober\(\)/,/^}/' "$SERVER_LIB" | grep -q 'local probe_port="\$1"'; then
  pass "server-owner: STRUCTURAL — the prober binds port 0 and takes no port argument"
else
  fail "server-owner: the prober must bind port 0 rather than a caller-supplied port"
fi
# ...and the DRIVER must not pass one either — `$((PORT + 1))` must be gone from the call site.
if ! grep -q 'require_socket_prober "\$(( *PORT' "$REPO_ROOT/scripts/perf/ws0-baseline.sh"; then
  pass "server-owner: the driver no longer passes \$((PORT+1)) (an unchecked port, 65536 at --port 65535)"
else
  fail "server-owner: the driver must not pass PORT+1 to the prober"
fi
# A BIND FAILURE must be diagnosed AS a bind failure, not as an unanswering prober — the whole
# point of the nit is that the diagnosis named the wrong cause. Driven by shimming `python3` to a
# non-binding stub, so the port file is never written.
out=$( set -uo pipefail
       PORT="$(free_port)"; OUT_DIR="$TMP"
       # shellcheck disable=SC1090
       source "$SERVER_LIB"
       python3() { return 1; }          # cannot bind / cannot start
       require_socket_prober 2>&1 ); rc=$?
if [ "$rc" -ne 0 ] && grep -q "could not BIND" <<<"$out" \
   && grep -q "NOT a prober that" <<<"$out"; then
  pass "server-owner: OBSERVED — a listener that cannot BIND is diagnosed as THAT, not as an unanswering prober"
else
  fail "server-owner: a bind failure must name itself (rc=$rc, out: $out)"
fi
# ...and it must FAIL when the prober cannot answer, which is the state that would make
# every ownership check vacuous. Driven by shimming `socket_owner_pid` to return nothing —
# the exact behaviour of an `ss` built without process info.
out=$( set -uo pipefail
       PORT="$(free_port)"; OUT_DIR="$TMP"
       # shellcheck disable=SC1090
       source "$SERVER_LIB"
       socket_owner_pid() { :; }        # answers NOTHING, like a privilege-less ss
       require_socket_prober 2>&1 ); rc=$?
if [ "$rc" -ne 0 ] && grep -q "cannot answer" <<<"$out" \
   && grep -q "would pass" <<<"$out"; then
  pass "server-owner: OBSERVED — a prober that cannot answer STOPS the run (never a vacuous ownership pass)"
else
  fail "server-owner: an unanswerable prober must be fatal (rc=$rc, out: $out)"
fi
# ...and that refusal must EXCLUDE a port collision from its stated causes, since the port is
# kernel-assigned. The pre-fix text offered three causes, all of them wrong for the collision
# case it could actually reach.
if grep -q "a COLLISION with another listener is excluded" <<<"$out"; then
  pass "server-owner: the unanswerable-prober refusal EXCLUDES a collision (its causes are now all true)"
else
  fail "server-owner: the refusal must rule out a collision, or it misdiagnoses again (out: $out)"
fi
# And the DRIVER must WIRE both: a guard present but never called is the #3249 shape.
DRV="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
if grep -qE '^require_socket_prober( |$)' "$DRV" \
   && awk '/^measure_flight\(\)/,/^}/' "$DRV" | grep -q 'await_server_ready'; then
  pass "server-owner: the driver CALLS require_socket_prober at startup and await_server_ready per rep"
else
  fail "server-owner: the driver must wire both the prober check and the readiness check"
fi
# The prober check must run BEFORE the first measurement, or a run could reach a rep with
# ownership unverifiable.
# The pattern allows the ARGUMENT-LESS form: the prober now binds a kernel-assigned port, so
# it takes no port argument, and a `^require_socket_prober ` grep (with the trailing space)
# matched nothing and reported a MISSING call for a call that is right there — the same
# "diagnosis names the wrong cause" shape as the nit itself, in the test.
prober_line=$(grep -nE '^require_socket_prober( |$)' "$DRV" | head -1 | cut -d: -f1)
loop_line=$(grep -n '^for temp in \$TEMPS' "$DRV" | head -1 | cut -d: -f1)
if [ -n "$prober_line" ] && [ -n "$loop_line" ] && [ "$prober_line" -lt "$loop_line" ]; then
  pass "server-owner: the prober check (line $prober_line) precedes the measurement loop (line $loop_line)"
else
  fail "server-owner: require_socket_prober must precede the measurement loop (prober=$prober_line loop=$loop_line)"
fi


# ===========================================================================
# PART 4 — the LINT's OWN vacuity states (#3272 review round 3 nits)
# ===========================================================================
# Two ways `perf_invocation_lint`/`_tree` used to read CLEAN while checking nothing, both
# instances of the rule this issue is about: never derive a positive verdict from the
# absence of a bad signal.

# --- 4a. A LINE WHOSE COMMAND WORD **AND** SUBCOMMAND ARE VARIABLES ----------
# `is_var_command` used to `return 0` on ANY line starting `VAR=`, and the comment claimed
# the prefixed form was "caught by the bare `stat` token instead" — TRUE ONLY WHEN `stat` IS
# LITERAL. MEASURED against that version, this line produced NO FINDING at all: no bare
# `perf`/`stat` token for layer 1, `invokes()` returned 0, and layer 2 sits behind
# `if (!mentions) next`. A genuinely per-process invocation through all three layers.
VARCMD=$'perf_stat_c() {\n  perf stat -x, -C 1 -o "$1" -- "$2"\n}\nFOO=1 "$BIN" "$SUB" -p 1234'
got="$(lint_probe "$VARCMD")"
if grep -q 'per-process option token' <<<"$got" \
   && grep -q 'outside the single perf_stat_c wrapper' <<<"$got"; then
  pass "lint-varcmd: OBSERVED — \`FOO=1 \"\$BIN\" \"\$SUB\" -p 1234\` FIRES (pre-fix: no finding at all)"
else
  fail "lint-varcmd: an assignment-prefixed variable command with a variable subcommand must fire (got: $got)"
fi
# ...and the SHIPPED tree must stay clean under the same change, which is the half a
# too-broad fix breaks. MEASURED while writing it: stepping over an assignment prefix
# WITHOUT a quote-balance test made `want="$(cpu_list_expand "$spec")"` split so that
# `"$spec")"` read as a variable command word — 6 false findings on ordinary code, and a
# guard that reds on ordinary code is the one an operator deletes.
for benign in \
  'want="$(cpu_list_expand "$spec")"' \
  'value="$(cat "$path" 2>/dev/null)" || return 1' \
  '_ARM_LIST=(scan $ARMS)' \
  'PERF_DOMAIN_OPTS="$_PP_SHORT $_PP_LONG --per-thread -a --all-cpus --cgroup"' \
  'REPO_ROOT="$(cd "$HERE/../.." && pwd)"' \
  'FOO=bar' \
  ; do
  probe=$'perf_stat_c() {\n  perf stat -x, -C 1 -o "$1" -- "$2"\n}\n'"$benign"
  if [ -z "$(lint_probe "$probe")" ]; then
    pass "lint-varcmd: ordinary code \`${benign:0:38}…\` is NOT flagged"
  else
    fail "lint-varcmd: '$benign' must not be flagged (got: $(lint_probe "$probe"))"
  fi
done

# --- 4b. AN UNREADABLE rig file is a FINDING, not a silent skip ---------------
# `perf_invocation_lint_tree` used to `[[ -r "$f" ]] || continue`, so a file with the wrong
# mode was DROPPED FROM THE SUBJECT and the tree read as clean with that file never scanned
# — the subject-too-small shape the DISCOVERED glob exists to prevent, arriving through the
# readability test instead of through a hand-written list.
unreadable_dir="$(mktemp -d "$TMP/unreadXXXXXX")"
cp "$PERF_DIR/"*.sh "$unreadable_dir/"
# Plant a real violation in the file we then make unreadable, so a lint that skipped it
# would report CLEAN over a tree containing a per-process invocation.
printf 'perf stat -x, -C 0 -p 1234 -o /dev/null -- true\n' >> "$unreadable_dir/lib-args.sh"
chmod 000 "$unreadable_dir/lib-args.sh"
got="$(lint_tree "$unreadable_dir")"
if grep -q 'UNREADABLE' <<<"$got" && grep -q 'lib-args.sh' <<<"$got" \
   && grep -q 'NOT SCANNED' <<<"$got"; then
  pass "lint-unreadable: OBSERVED — an UNREADABLE rig file is a FINDING naming it (pre-fix: silently skipped)"
else
  fail "lint-unreadable: an unreadable file must be reported, not dropped from the subject (got: $got)"
fi
# NON-VACUITY: the planted violation is real, so the pre-fix silent skip really did hide
# something. Restore the mode and require the SAME tree to fire on the plant.
chmod 644 "$unreadable_dir/lib-args.sh"
got="$(lint_tree "$unreadable_dir")"
if grep -q 'per-process option token' <<<"$got" && grep -q 'lib-args.sh' <<<"$got"; then
  pass "lint-unreadable: NON-VACUITY — the planted violation DOES fire once the file is readable"
else
  fail "lint-unreadable: the plant must be a real violation, or the skip hid nothing (got: $got)"
fi
# The printed SUBJECT must list the unreadable file too: filtering it out there would make
# the set claim agree with a tree lint that had dropped it — a check confirming its own gap.
chmod 000 "$unreadable_dir/lib-args.sh"
if grep -q 'lib-args.sh' <<<"$(lint_subject "$unreadable_dir")"; then
  pass "lint-unreadable: the printed SUBJECT still lists an unreadable file (the set claim cannot hide the gap)"
else
  fail "lint-unreadable: perf_lint_tree_subject must list every existing .sh, readable or not"
fi
chmod 644 "$unreadable_dir/lib-args.sh"

# --- 4c. `library` mode HAS END assertions now (an awk that dies is a finding) -
# `library` mode had NO END assertions, so an awk that died mid-file printed nothing and
# read as clean — and the driver counts OUTPUT, so the run was waved through. Every mode now
# emits a completion marker the caller verifies. Driven by feeding a file the scan cannot
# complete over: an EMPTY one (nothing checked) in both modes.
: > "$TMP/empty-probe.sh"
for mode in owner library; do
  got="$(lint_shipped "$TMP/empty-probe.sh" "$mode")"
  if grep -q 'NO LINES' <<<"$got" || grep -q 'did not COMPLETE' <<<"$got"; then
    pass "lint-complete: an EMPTY file is a FINDING in '$mode' mode (nothing checked reads like nothing wrong)"
  else
    fail "lint-complete: an empty file must be reported in '$mode' mode (got: ${got:-<nothing>})"
  fi
done
# ...and a REAL library file must still be clean in library mode, so the completion marker is
# filtered rather than printed as a finding.
if [ -z "$(lint_shipped "$PERF_DIR/lib-cpu.sh" library)" ]; then
  pass "lint-complete: the completion marker is FILTERED, not reported (lib-cpu.sh is clean in library mode)"
else
  fail "lint-complete: a clean library file must lint clean (got: $(lint_shipped "$PERF_DIR/lib-cpu.sh" library))"
fi
# And the marker must genuinely be emitted, not merely absent-and-forgiven: a mode whose
# END block never ran would take the `did not COMPLETE` branch, which is what 4c relies on.
if lint_shipped "$PERF_DIR/lib-cpu.sh" library >/dev/null 2>&1 \
   && ! grep -q 'did not COMPLETE' <<<"$(lint_shipped "$PERF_DIR/lib-cpu.sh" library)"; then
  pass "lint-complete: a real file DOES reach the END block (the marker is emitted, not assumed)"
else
  fail "lint-complete: the completion marker must be emitted for a real file"
fi
# THE `did not COMPLETE` DIAGNOSTIC MUST BE REACHABLE ON THE DRIVER'S PATH (#3272 round 4 nit).
#
# THE FINDING: the driver runs under `set -e -o pipefail` and CAPTURES the lint —
# `_perf_lint_out="$(perf_invocation_lint_tree "$HERE")"`. An awk that died mid-file made the
# pipeline non-zero, made the substitution non-zero, and under `-e` KILLED THE DRIVER at the
# assignment — BEFORE `[[ -n "$_perf_lint_out" ]]` inspected the text. So the run died with a
# bare status and NO diagnostic, and the `did not COMPLETE` branch above was UNREACHABLE on the
# one path it was written for. A guard whose diagnostic cannot print is not a guard.
#
# Driven under the driver's EXACT shell options with a REAL non-zero awk exit. The fixture is an
# UNREADABLE FILE (mode 000): `awk` reports it and exits 2 — MEASURED, vs a directory, which
# `awk` reads as an empty stream and exits 0, so a directory fixture would have asserted nothing.
# (The first version of this case used a directory and passed while the fix was reverted, which
# is why the failure mode is now measured rather than assumed.) The assertion is that the CAPTURE
# COMPLETES and the text is inspectable.
dying_probe="$TMP/dying-input.sh"
printf 'x\n' > "$dying_probe"
chmod 000 "$dying_probe"
out=$( set -e -o pipefail
       # shellcheck disable=SC1090
       source "$PERF_LINT_LIB"
       captured="$(perf_invocation_lint "$dying_probe" library)"
       printf 'CAPTURED:%s\n' "$captured" ) 2>&1; rc=$?
chmod 644 "$dying_probe"
if [ "$rc" -eq 0 ] && grep -q 'CAPTURED:' <<<"$out"; then
  pass "lint-pipefail: OBSERVED — under the driver's \`set -e -o pipefail\` the capture COMPLETES on a dying awk (pre-fix: the driver died at the assignment)"
else
  fail "lint-pipefail: the capture must survive a dying awk so its diagnostic can be inspected (rc=$rc, out: $out)"
fi
# ...and what it captured must be the DIAGNOSTIC, not an empty string — surviving the assignment
# is worthless if the text is empty, which would read exactly like a clean file.
if grep -q 'did not COMPLETE\|NO LINES' <<<"$out"; then
  pass "lint-pipefail: the captured text IS the diagnostic (an empty capture would read as clean)"
else
  fail "lint-pipefail: the capture must carry the incompleteness finding (out: $out)"
fi
# STRUCTURAL: the awk's status is absorbed at the awk, NOT at the pipeline — the helper's own
# status is meaningful and must stay intact.
if awk '/^perf_invocation_lint\(\)/,/^}/' "$PERF_LINT_LIB" | grep -q '|| true; } | _perf_lint_verify_complete'; then
  pass "lint-pipefail: STRUCTURAL — the status is absorbed at the AWK, leaving the helper's own status intact"
else
  fail "lint-pipefail: the awk's status must be absorbed without swallowing the helper's"
fi

# --- 4d. STRUCTURAL: no APOSTROPHE inside the single-quoted awk program -------
# Not a style rule — a correctness one, and it bit THREE TIMES writing this round. The awk
# program is a single-quoted shell string, so an apostrophe in one of its COMMENTS closes the
# string and bash reports a syntax error at some unrelated line ("syntax error near
# unexpected token `{`" at the function header). `bash -n` catches it, but only if someone
# runs it; this makes the property a standing check.
if python3 - "$PERF_LINT_LIB" <<'PYEOF'
import re, sys
src = open(sys.argv[1]).read()
m = re.search(r"-v SQ=\"'\" '\n(.*?)\n  ' \"\$1\"", src, re.S)
if not m:
    raise SystemExit("could not locate the awk program — this check is stale, which is worse"
                     " than absent: it would pass on any file")
bad = []
for i, line in enumerate(m.group(1).splitlines(), start=1):
    stripped = line.lstrip()
    if not stripped.startswith("#"):
        continue          # code lines legitimately use quotes; only COMMENTS are the trap
    if "'" in line:
        bad.append((i, stripped[:70]))
if bad:
    raise SystemExit(
        "apostrophe(s) inside the single-quoted awk program's comments — each CLOSES the"
        f" shell string and breaks the file: {bad}"
    )
PYEOF
then
  pass "lint-quoting: STRUCTURAL — no apostrophe in the awk program's comments (it would close the shell string)"
else
  fail "an apostrophe in the awk program's comments closes the single-quoted string; bash then errors at an unrelated line"
fi
# NON-VACUITY for that check, and for the CLAIM behind it. A copy of the library with ONE
# apostrophe planted in an awk comment must (a) be caught by the check and (b) genuinely FAIL
# `bash -n` — otherwise the rule is a style preference dressed as a correctness one.
apos_copy="$TMP/apostrophe-probe.sh"
python3 - "$PERF_LINT_LIB" "$apos_copy" <<'PYEOF'
import pathlib, sys
src, dst = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
s = src.read_text()
needle = "    # A token as the SHELL would see it after word-splitting"
if needle not in s:
    sys.exit("the plant target moved — this fixture is stale")
dst.write_text(s.replace(needle, "    # A token as the SHELL's word-splitting yields it"))
PYEOF
apos_caught=0
python3 - "$apos_copy" <<'PYEOF' || apos_caught=1
import re, sys
src = open(sys.argv[1]).read()
m = re.search(r"-v SQ=\"'\" '\n(.*?)\n  ' \"\$1\"", src, re.S)
if not m:
    raise SystemExit("stale")
bad = [i for i, l in enumerate(m.group(1).splitlines(), 1)
       if l.lstrip().startswith("#") and "'" in l]
if bad:
    raise SystemExit(f"caught at {bad}")
PYEOF
if [ "$apos_caught" -eq 1 ] && ! bash -n "$apos_copy" 2>/dev/null; then
  pass "lint-quoting: NON-VACUITY — a planted apostrophe is CAUGHT and really does break \`bash -n\`"
else
  fail "lint-quoting: the planted apostrophe must be caught AND must break the file (caught=$apos_caught)"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its
# ~139 checks and passed them exits 0 and reports SUCCESS. That is the suite-level
# `0/0` shape this whole issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite)
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file.
MIN_CHECKS=163
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 cpu-pinning / perf-invocation guard checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks check(s) failed"
exit 1

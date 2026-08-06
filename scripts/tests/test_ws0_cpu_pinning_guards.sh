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
#   2. THE SERVER-OWNERSHIP CHECK (`lib-server.sh`), which asks the same question
#      about the right PROGRAM that item 1 asks about the right CPUs. Readiness used
#      to be inferred solely from a connect probe succeeding, so if our server failed
#      to bind and a peer held the port the load generator measured THAT server while
#      `perf stat -C` counted OUR pinned CPUs — a figure published under a program the
#      rig did not start and cannot name. Driven against the shipped library with a
#      real listener on a kernel-assigned port, in both directions.
#
# THE THIRD GUARD — the three-layer `perf`-invocation lint (`lib-perf-lint.sh`), which
# asks whether the COUNTING DOMAIN is the one spec R2 mandates — was split into
# `scripts/tests/test_ws0_perf_invocation_lint.sh` under the campsite rule (this file
# reached 1607 lines against the ~1500 test target). The seam is a RESPONSIBILITY: that
# subject reads shell SOURCE TEXT and a shimmed argv, this one reads a fake sysfs tree
# and a real TCP listener, and measured at the split the two shared NO helper and NO
# fixture. The gate runs both (`tooling-tests`).
#
# Hermetic: a fake sysfs tree under $TMPDIR and a real listener on a kernel-assigned
# loopback port. No perf, no sudo, no taskset, no root, no real multi-socket hardware,
# no network, no corpus, no cargo. The real scripts/perf/ files are never modified.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
LIB="$REPO_ROOT/scripts/perf/lib-cpu.sh"
# `PERF_LINT_LIB` and its `[ -f ]` preflight moved WITH their subject into
# test_ws0_perf_invocation_lint.sh rather than being left behind as a dead handle: a
# preflight for a library this file no longer reads would assert nothing here and would
# stop reflecting what the suite needs to run.
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
# Stated UP FRONT and fail-closed (#3272 review B8, applied to this file too).
# PART 2's fixtures need python3: the free-port helper, the real listener, and the
# extracted `await_server_ready` copy whose loop bound is reduced. Without this check
# their absence would surface as a fixture-did-not-apply failure inside PART 2 —
# correct, but diagnosed as the wrong thing; and the reflex fix for a confusing
# failure is a skip, which is how a vacuous green gets introduced. python3 is a HARD
# REQUIREMENT of the rig this file tests (ws0-baseline.sh refuses to run without it),
# so its absence FAILS.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig"
  echo "       (ws0-baseline.sh refuses to run without it) and PART 2's listener and"
  echo "       free-port fixtures need it. A skip here would record the gate component"
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
#
# TWO DISTINCT DIAGNOSTICS since #3272 round 11's F4, and the distinction is the substance: the
# TOTALLY EMPTY spec `""` splits into ZERO elements, so there is no element to name and the
# caller's "CPU list is empty" is the right diagnostic; a spec that is nothing but SEPARATORS
# (`,`, `,,`) has empty ELEMENTS, and those are now refused where they are visible rather than
# skipped into an empty expansion. Both still fail closed — what changed is that the second kind
# names the position instead of blaming the whole list.
out=$(lib_call verify_sibling_pair "" server); rc=$?
if [ "$rc" -ne 0 ] && grep -q "CPU list is empty" <<<"$out"; then
  pass "sibling-reject: the TOTALLY EMPTY spec fails closed with the empty-list message (zero elements, so nothing to name)"
else
  fail "sibling-empty (''): expected non-zero + 'CPU list is empty' (rc=$rc, out: $out)"
fi
for spec in "," ",,"; do
  out=$(lib_call verify_sibling_pair "$spec" server); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "EMPTY element" <<<"$out"; then
    pass "sibling-reject (round11 F4): a separators-only spec ('$spec') is refused for its EMPTY ELEMENT, naming the position (pre-fix: skipped, expanding to nothing and returning 0)"
  else
    fail "round11 F4: '$spec' must be refused naming the EMPTY element (rc=$rc, out: $out)"
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
# The SERVER sibling check, however it is spelled. It used to be a bare statement
# (`^verify_sibling_pair "$SERVER_CPUS"`); #3272 F6 made it a command SUBSTITUTION, because the
# verification's output (the expanded sibling set sysfs reported) is now CAPTURED and recorded into
# the session dir so the report's "verified physical-core siblings" claim cites an observation. The
# anchor is therefore the CALL, not the line's opening token — and the `-n` guard below is what
# caught this: after F6 the old pattern matched NOTHING and this check FAILED rather than passing
# vacuously over an empty line number.
verify_line=$(grep -nF 'verify_sibling_pair "$SERVER_CPUS"' "$DRIVER" | head -1 | cut -d: -f1)
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
# ===========================================================================
# 64-BIT WRAPAROUND: moved to scripts/tests/test_ws0_numeric_wraparound.sh
# ===========================================================================
# The F2 cases (a well-formed decimal too large for signed 64-bit arithmetic defeating the
# index ceiling AND the expansion cap) live in their own suite, under the campsite rule and
# along a responsibility seam: that class spans `lib-cpu.sh` AND `lib-args.sh` and now a shared
# primitive belonging to neither, whereas THIS file's subject is CPU TOPOLOGY. Pointer left
# rather than the cases deleted, so the next reader of the grammar knows where the arithmetic
# half is verified. The gate runs both (`tooling-tests`).

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
#
# `2,,10` USED TO BE HERE, expanding to `2 10`, and #3272 round 11's F4 MOVED IT to the reject
# side (below). That move is the finding: an empty element was skipped, so the rig pinned a set
# the operator had not written and a spec of nothing but separators expanded to NOTHING and
# returned 0. Recorded rather than silently deleted, because an accept case turning into a reject
# case is a deliberate behaviour change and the next reader deserves to know it was one.
for good_spec in '2,10:2 10' '0-3,8:0 1 2 3 8' '08:8' '0:0' '8191:8191'; do
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
# --- round 11, F4: AN EMPTY ELEMENT, and AN EMPTY EXPANDED SET ------------------------------
# `2,,10` is refused rather than quietly measuring `2 10`.
out=$(lib_call cpu_list_expand "2,,10"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "EMPTY element (position 2)" <<<"$out"; then
  pass "cpu-grammar (round11 F4): '2,,10' is REFUSED naming the empty element's POSITION (pre-fix: skipped, so the rig pinned '2 10' — a set the operator did not write)"
else
  fail "round11 F4: '2,,10' must be refused naming the empty element position (rc=$rc, out: $out)"
fi
# NON-VACUITY: the pre-fix disposition really was to accept it. A replica of the removed
# `[[ -n "$part" ]] || continue` loop, observed to produce a NON-EMPTY expansion from a spec that
# is now refused — without this the refusal could be about a spec that was never accepted.
prefix_skip_expand() {
  local spec="$1" part; local -a out=()
  IFS=',' read -r -a _pp <<<"$spec"
  for part in "${_pp[@]}"; do
    [[ -n "$part" ]] || continue      # the REMOVED line, verbatim
    out+=("$part")
  done
  ((${#out[@]} == 0)) && return 0
  printf '%s\n' "${out[@]}" | tr '\n' ' ' | sed 's/ $//'
}
pre_skip="$(prefix_skip_expand '2,,10')"
if [ "$pre_skip" = "2 10" ]; then
  pass "cpu-grammar NON-VACUITY (round11 F4): the PRE-FIX skip loop expanded '2,,10' to '$pre_skip' — a DIFFERENT set from the one written, accepted silently"
else
  fail "round11 F4: the pre-fix loop must have expanded '2,,10' to '2 10', else the case proves nothing (got '$pre_skip')"
fi
# --- round 16, L1: A TRAILING COMMA IS AN EMPTY ELEMENT TOO ---------------------------------
# The F4 check above was right; the PARSER discarded the case. `IFS=',' read -r -a` DROPS a
# trailing empty field, so '2,10,' split into two NON-EMPTY elements and never reached the
# emptiness test that '2,,10' and ',2,10' hit — the one spec shape an operator produces by
# accident (a copy-paste, a shell loop that appends a separator per item) was the one shape that
# slipped through. Refused with the SAME position-naming diagnostic, not a separate comma message.
for spec in "2,10," "2,10,," "2,10-11,"; do
  out=$(lib_call cpu_list_expand "$spec"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "EMPTY element (position 3)" <<<"$out"; then
    pass "cpu-grammar (round16 L1): the TRAILING-comma spec '$spec' is REFUSED naming position 3 — the same sentence '2,,10' gets, not a separate grammar message"
  else
    fail "round16 L1: '$spec' must be refused naming the empty element position (rc=$rc, out: $out)"
  fi
done
# NON-VACUITY: the PRE-FIX parse really did accept a trailing comma. A replica of the removed
# split (no sentinel), observed to yield a set with NO empty element from '2,10,' — so the F4
# check could not have fired, and the rig would have pinned '2 10' from a malformed spec.
prefix_split_count_empty() {
  local spec="$1" part; local -a _sp=(); local n_empty=0 n=0
  IFS=',' read -r -a _sp <<<"$spec"          # the PRE-FIX split, verbatim (no `,#` sentinel)
  for part in ${_sp[@]+"${_sp[@]}"}; do
    n=$((n + 1)); [[ -z "$part" ]] && n_empty=$((n_empty + 1))
  done
  echo "$n $n_empty"
}
pre_trail="$(prefix_split_count_empty '2,10,')"
if [ "$pre_trail" = "2 0" ]; then
  pass "cpu-grammar NON-VACUITY (round16 L1): the PRE-FIX split read '2,10,' as $pre_trail (elements, empty-elements) — ZERO empty elements, so F4's check was never reached and the malformed spec was ACCEPTED"
else
  fail "round16 L1: the pre-fix split must have read '2,10,' as '2 0' (2 elements, none empty), else the case proves nothing (got '$pre_trail')"
fi
# ...and the sentinel did NOT invent an element for the TOTALLY EMPTY spec: '' must still be a
# ZERO-element SUCCESSFUL expansion, or the callers' "CPU list is empty" diagnostic (1c above)
# would be replaced by a position-1 empty-element refusal about an element nobody wrote.
#
# STDOUT ONLY here, unlike `lib_call` (which folds stderr in for diagnostic matching): these two
# cases compare the EXPANSION's value, and the topology-override NOTE on stderr would otherwise
# read as an expansion of one line.
lib_call_stdout() {
  local fn="$1"; shift
  ( export CQLITE_WS0_CPU_TOPOLOGY_ROOT="$TOPO"
    # shellcheck disable=SC1090
    source "$LIB"
    "$fn" "$@" ) 2>/dev/null
}
out=$(lib_call_stdout cpu_list_expand ""); rc=$?
if [ "$rc" -eq 0 ] && [ -z "$out" ]; then
  pass "cpu-grammar (round16 L1): the empty spec '' is STILL a zero-element successful expansion — the trailing-field fix did not invent a phantom element, so 'CPU list is empty' remains the caller's diagnostic"
else
  fail "round16 L1: '' must still expand to nothing with rc=0 (rc=$rc, out: '$out')"
fi
# ...and the ACCEPT direction survives the reparse: a well-formed list and a range still expand.
out=$(lib_call_stdout cpu_list_expand "2,10-11"); rc=$?
if [ "$rc" -eq 0 ] && [ "$out" = "2 10 11" ]; then
  pass "cpu-grammar (round16 L1): the reparse did NOT break the accept direction — '2,10-11' still expands to '$out'"
else
  fail "round16 L1: '2,10-11' must still expand to '2 10 11' (rc=$rc, out: '$out')"
fi
# ...and an EMPTY EXPANDED SET is refused by `verify_disjoint`, per SET, naming which. This is the
# `--client-cpus ''` path: the client set is deliberately NOT sibling-checked, so `verify_disjoint`
# is the only place it is examined — and two nested loops over nothing returned 0.
out=$(lib_call verify_disjoint "" "4,12"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "server CPUs ('') expand to an EMPTY set" <<<"$out"; then
  pass "disjoint-reject (round11 F4): an EMPTY SERVER set is refused, naming the set"
else
  fail "round11 F4: an empty server set must be refused by verify_disjoint (rc=$rc, out: $out)"
fi
out=$(lib_call verify_disjoint "2,6" ""); rc=$?
if [ "$rc" -ne 0 ] && grep -q "client CPUs ('') expand to an EMPTY set" <<<"$out"; then
  pass "disjoint-reject (round11 F4): an EMPTY CLIENT set is refused — pre-fix '--client-cpus \"\"' passed the WHOLE topology stage and failed later at taskset, after the host sysctls were weakened and after a full release build"
else
  fail "round11 F4: an empty client set must be refused by verify_disjoint (rc=$rc, out: $out)"
fi
# NON-VACUITY for the empty-set half: the pre-fix body really did return 0. A replica of the two
# nested loops with no emptiness test, observed to accept the same input.
prefix_disjoint() {
  local a="$1" b="$2" x y
  for x in $a; do for y in $b; do [[ "$x" == "$y" ]] && return 1; done; done
  return 0
}
if prefix_disjoint "" "4 12" && prefix_disjoint "2 6" ""; then
  pass "disjoint NON-VACUITY (round11 F4): the PRE-FIX loops returned SUCCESS for BOTH empty sets — an empty set trivially satisfies disjointness"
else
  fail "round11 F4: the pre-fix disjointness loops must have accepted an empty set, else the case proves nothing"
fi
# ...and the ACCEPT direction is unchanged: two real, non-overlapping sets still pass. Without
# this, a `verify_disjoint` hardcoded to refuse would satisfy every case above.
out=$(lib_call verify_disjoint "2,6" "1,5"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "disjoint-accept (round11 F4): the emptiness checks did NOT break the accept direction — two real non-overlapping sets still pass"
else
  fail "round11 F4: two non-overlapping sets must still be accepted (rc=$rc, out: $out)"
fi
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
# THE PERF-INVOCATION LINT: moved to scripts/tests/test_ws0_perf_invocation_lint.sh
# ===========================================================================
# The three-layer `perf` invocation guard (the self-grep over injected driver copies, the
# tree lint over `scripts/perf/*.sh`, the runtime argv check inside `perf_stat_c`) and the
# lint's own vacuity states now live in their own suite, under the campsite rule (this file
# reached 1607 lines against the ~1500 test target) and along a RESPONSIBILITY seam: that
# subject reads shell SOURCE TEXT and a shimmed argv and its subject is `lib-perf-lint.sh`,
# whereas THIS file's subject is which CPUs and which PROGRAM an observation is of — a fake
# sysfs tree and a real TCP listener over `lib-cpu.sh` + `lib-server.sh`. Measured at the
# split, the two halves shared NO helper and NO fixture. Pointer left rather than the cases
# deleted, so the next reader of the pinning guards knows where the counting-domain half is
# verified. The gate runs both (`tooling-tests`).

# ===========================================================================
# PART 2 — THE FLIGHT SERVER WE MEASURE IS THE ONE WE STARTED (#3272 B4)
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

# --- 2a. THE PROBER CAN ANSWER, on a socket whose owner we KNOW ---------------
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

# --- 2b. A FOREIGN listener on the port is REFUSED, naming it -----------------
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

# --- 2c. OUR OWN listener is ACCEPTED (the guard is not one that refuses all) --
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

# --- 2c2. A DESCENDANT of our server is ACCEPTED, and only a real descendant --
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

# --- 2d. A DEAD server is refused BEFORE the port is consulted ----------------
# A dead child cannot be what a live socket belongs to, and the diagnostic must name the
# process rather than the port — the two causes have different remedies.
sleep 0 & DEAD_PID=$!; wait "$DEAD_PID" 2>/dev/null || true
out=$( SERVER_PID="$DEAD_PID" server_lib_call await_server_ready probe-rep ); rc=$?
if [ "$rc" -ne 0 ] && grep -q "is not" <<<"$out" && grep -q "exited before serving" <<<"$out"; then
  pass "server-owner: a server that EXITED is refused, naming the process (not the port)"
else
  fail "server-owner: a dead server must be refused with a process diagnostic (rc=$rc, out: $out)"
fi

# --- 2e. A READINESS TIMEOUT IS FATAL ----------------------------------------
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

# --- 2f. AN UNANSWERABLE PROBER STOPS THE RUN --------------------------------
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
# And the RIG must WIRE both: a guard present but never called is the #3249 shape.
#
# TWO SUBJECTS since #3272 round 9: `require_socket_prober` is a STARTUP check and stays in the
# driver, while `await_server_ready` is called per rep from `measure_flight`, which moved into
# `lib-measure.sh` under the campsite rule (the driver was at 1008 lines against the ~800 target).
# The `-s`/`-n` guards are load-bearing: after the split the old awk range over the DRIVER matched
# nothing, and this check FAILED rather than going green over an empty subject — which is the
# property that makes a range test trustworthy at all.
DRV="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
MEASURE_LIB="$REPO_ROOT/scripts/perf/lib-measure.sh"
flight_body=$(awk '/^measure_flight\(\)/,/^}/' "$MEASURE_LIB")
if grep -qE '^require_socket_prober( |$)' "$DRV" \
   && [ -s "$MEASURE_LIB" ] && [ -n "$flight_body" ] \
   && grep -q 'await_server_ready' <<<"$flight_body"; then
  pass "server-owner: the driver CALLS require_socket_prober at startup, and measure_flight (now in lib-measure.sh) calls await_server_ready per rep"
else
  fail "server-owner: the rig must wire both the prober check and the readiness check (lib present=$([ -s "$MEASURE_LIB" ] && echo yes || echo NO), measure_flight body lines=$(printf '%s' "$flight_body" | grep -c . ))"
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


# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its
# checks and passed them exits 0 and reports SUCCESS. That is the suite-level
# `0/0` shape this whole issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite)
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file.
#
# RE-DERIVED at the perf-invocation-lint split, from a MEASURED run: this file ran 189 checks
# before the split and runs 76 after (the departed subject took 113 with it, and 76 + 113 = 189
# accounts for every one). The floor MUST move down with the departing cases — a floor left at
# 189 would point at a count that no longer exists and would red the suite unconditionally,
# which is the #3326-item-3 shape (a floor naming a number nothing produces) in its loudest
# direction rather than its quiet one.
MIN_CHECKS=72
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 cpu-pinning / server-ownership guard checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks check(s) failed"
exit 1

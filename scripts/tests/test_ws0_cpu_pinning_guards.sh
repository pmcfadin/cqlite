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
#   2. THE `perf stat -p` SELF-GREP. Per-process counting measured >2x observer
#      cost on this workload, so spec R2 requires CPU-wide `perf stat -C` and the
#      driver greps ITSELF to refuse a `-p` form. Driving that guard over injected
#      copies found TWO REAL BYPASSES in it, both recorded in the driver's comment
#      at the check: an ATTACHED value (`-p` immediately followed by digits — the
#      old pattern required a trailing space) and ANY LINE MENTIONING "self-check"
#      (the old `grep -v 'self-check'` discarded by CONTENT, so a comment on a real
#      per-process line suppressed the guard). Both now fire. The negative
#      direction is asserted too: the driver AS SHIPPED must pass its own check.
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

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

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
  local label="$1" repl="$2" copy out rc
  copy="$(driver_copy_with "$repl")" || { fail "$label: could not build the injected copy"; return; }
  # NON-VACUITY for the fixture itself: the injection must be present in the copy,
  # or a no-op replacement would make the guard "fire" on nothing.
  if ! grep -qF "$repl" "$copy"; then
    fail "$label: the injected line is not in the copy — the fixture did not apply"
    return
  fi
  out=$(bash "$copy" --corpus /nonexistent 2>&1); rc=$?
  if [ "$rc" -ne 0 ] \
     && grep -q "contains a per-process perf invocation" <<<"$out" \
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
# the guard an operator deletes. The shipped driver must get PAST the self-check,
# so its failure must be about the (absent) corpus, never about `-p`.
out=$(bash "$DRIVER" --corpus /nonexistent 2>&1); rc=$?
if [ "$rc" -ne 0 ] && ! grep -q "per-process" <<<"$out"; then
  pass "selfgrep-silent: the driver AS SHIPPED passes its own self-check (fails later, on the corpus)"
else
  fail "selfgrep-silent: the shipped driver must NOT trip its own -p check (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# Stated directly too, using the driver's OWN lint function rather than a second
# hand-written pattern: a reimplemented check in the test would be a second thing to
# keep in sync, and its divergence would be invisible in exactly the permissive
# direction. Sourced by extraction so nothing in the driver's body runs.
lint_shipped() {
  ( set -uo pipefail
    # shellcheck disable=SC1090
    source "$PERF_LINT_LIB"
    perf_invocation_lint "$1" )
}
if [ -z "$(lint_shipped "$DRIVER")" ]; then
  pass "selfgrep-real: the shipped driver is clean under its OWN lint (no second pattern to drift)"
else
  fail "selfgrep-real: the SHIPPED driver violates its own lint: $(lint_shipped "$DRIVER")"
fi
# The driver's own lint only ever reads ${BASH_SOURCE[0]} — ITSELF — so a per-process
# invocation smuggled into one of the LIBRARIES it sources would be inside the rig and
# outside the guard. The lint's per-TOKEN layer is applied to each of them here.
# `lib-cpu.sh` has no `perf_stat_c`, so only the option findings are asserted; the
# allowlist half is meaningless for a file that is not supposed to invoke perf at all.
for lib in "$LIB" "$PERF_LINT_LIB"; do
  bad=$(lint_shipped "$lib" | grep 'per-process option token')
  if [ -z "$bad" ]; then
    pass "selfgrep-libs: $(basename "$lib") carries no per-process option token"
  else
    fail "selfgrep-libs: $(basename "$lib") carries a per-process option: $bad"
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
# shape): the driver runs it unconditionally at parse time and exits on any output.
if grep -q '_perf_lint_out="\$(perf_invocation_lint "\${BASH_SOURCE\[0\]}")"' "$DRIVER" \
   && awk '/^if \[\[ -n "\$_perf_lint_out" \]\]/,/^fi$/' "$DRIVER" | grep -q 'exit 2'; then
  pass "selfgrep-wired: the lint runs over \${BASH_SOURCE[0]} at startup and exits on any finding"
else
  fail "selfgrep-wired: the lint must run over \${BASH_SOURCE[0]} unconditionally and exit"
fi

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
for spelled in "-p1234" "-p" "--pid=1234" "--pid"; do
  out=$(argv_probe true "$spelled"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "was passed the per-process option" <<<"$out" \
     && ! grep -q 'PERF-RAN' <<<"$out"; then
    pass "argv-guard: perf_stat_c REFUSES a computed '$spelled' before invoking perf"
  else
    fail "argv-guard: '$spelled' must be refused and perf must not run (rc=$rc, out: $out)"
  fi
done
# A single-quoted attached value is the SAME TOKEN here — the spelling problem does
# not exist at this layer, which is the point of having it.
out=$(argv_probe true "-p'1234'")
if grep -q "was passed the per-process option" <<<"$out"; then
  pass "argv-guard: a single-quoted attached value is the same token after quote removal"
else
  fail "argv-guard: -p'1234' must be refused (out: $out)"
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
echo
if [ "$fails" -eq 0 ]; then
  echo "PASS - all WS0 cpu-pinning / perf-invocation guard checks fired as specified"
  exit 0
fi
echo "FAIL - $fails check(s) failed"
exit 1

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
# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its
# ~95 checks and passed them exits 0 and reports SUCCESS. That is the suite-level
# `0/0` shape this whole issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite)
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file.
MIN_CHECKS=95
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

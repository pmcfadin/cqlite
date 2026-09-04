#!/usr/bin/env bash
# test_ws0_perf_invocation_lint.sh — THE PERF-INVOCATION GUARD, ALL THREE LAYERS
# (issue #3272, item 10; split out of test_ws0_cpu_pinning_guards.sh under the campsite rule).
#
# # Why this is its own suite
#
# The seam is a RESPONSIBILITY, not a line count. `test_ws0_cpu_pinning_guards.sh` reached 1607
# lines against the ~1500 test target carrying two unrelated subjects:
#
#   * CPU/PROCESS TOPOLOGY — which CPUs and which PROGRAM an observation is of. That reads a
#     fake sysfs tree and a real TCP listener, and its subject is `lib-cpu.sh` + `lib-server.sh`.
#   * THE PERF-INVOCATION LINT — whether the COUNTING DOMAIN of the observation is the one the
#     spec mandates. That reads shell SOURCE TEXT and a shimmed argv, and its subject is
#     `lib-perf-lint.sh` + the `perf_stat_c` wrapper.
#
# They share NO fixture and NO helper: measured before the split, the topology half used
# `lib_call`/`$TOPO`/`server_lib_call` and touched none of `lint_probe`/`lint_tree`/`argv_probe`,
# and the lint half touched none of theirs. Two subjects with a null helper intersection is what
# makes this a seam rather than a cut. The gate runs both (`tooling-tests`), and the topology
# suite keeps a pointer to this file so the next reader of one knows where the other is.
#
# # What this file covers
#
# THE PERF-INVOCATION GUARD (`lib-perf-lint.sh`). Per-process counting measured >2x observer cost
# on this workload, so spec R2 requires CPU-wide counting and the driver checks ITSELF at startup.
# Driving that guard over injected copies found FIVE REAL BYPASSES across two successive deny-list
# patterns: an ATTACHED value, ANY LINE MENTIONING "self-check" (the `grep -v` discarded by
# CONTENT, so a comment on a real invocation suppressed the guard), a SINGLE-QUOTED attached
# value, an invocation through a VARIABLE, and a GLOBAL OPTION between `perf` and `stat`. All five
# fire now — and the mechanism is no longer a deny-list: it is an ALLOWLIST (perf is invoked in ONE
# wrapper; any other invocation line must be explicitly marked) plus a per-TOKEN option check plus
# a RUNTIME argv check. A deny-list must anticipate every spelling and is silently permissive the
# moment it misses one; an allowlist asks WHERE a line is, which is closed by construction.
# Both directions are asserted, plus the lint's own positive control: it must be SILENT on a
# minimal clean file and must FLAG an absent/empty/`-C`-less wrapper, and it must NOT flag
# `perf_stat_c`/`perf_event_paranoid`/`target/perf-…` identifiers — a guard that reds on ordinary
# code is the one an operator deletes. Its OWN VACUITY STATES are driven too (a variable command
# word, an unreadable rig file, a mode with no END assertions, an apostrophe closing the awk
# program's shell string), each an instance of the rule this issue is about: never derive a
# positive verdict from the absence of a bad signal.
#
# Hermetic: copies of the driver and of the scripts/perf tree under $TMPDIR, plus a shimmed
# `perf` function for the runtime layer. No perf, no sudo, no taskset, no root, no network, no
# corpus, no cargo. The real scripts/perf/ files are never modified.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
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
[ -f "$PERF_LINT_LIB" ] || { echo "FAIL - missing $PERF_LINT_LIB"; exit 1; }
# Stated UP FRONT and fail-closed (#3272 review B8, applied to this file too).
# `driver_copy_with` needs python3 for its exact-literal injection. Without this
# check its absence would surface as a fixture-did-not-apply failure inside the
# injected-copy cases — correct, but diagnosed as the wrong thing; and the reflex fix for
# a confusing failure is a skip, which is how a vacuous green gets introduced. python3 is a
# HARD REQUIREMENT of the rig this file tests (ws0-baseline.sh refuses to run without it),
# so its absence FAILS.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig"
  echo "       (ws0-baseline.sh refuses to run without it) and the exact-literal driver"
  echo "       injection below needs it. A skip here would record the gate component"
  echo "       as SUCCESS with 0 of these checks having run (#3272 review B8)."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT
ws0_hermetic_init "$TMP"

# ===========================================================================
# PART 1 — the `perf stat -p` self-grep, and all three layers of the guard
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
CPU_WIDE_LINE='  perf stat -x, -e "$EVENTS" -C "$PERF_COUNT_CPUS" -o "$outfile" -- "$@"'

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
  '  perf stat -x, -e "$EVENTS" -C "$PERF_COUNT_CPUS" -o "$outfile" -- "$@"
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
  # The diagnostic now NAMES WHICH allowlist applied, because since #3248 there are two
  # (`stat` and `record`) and "not in the allowlist" without saying which one is a message
  # that cannot be acted on. Asserting the subcommand-specific text also pins that a STAT
  # line was judged by the STAT set.
  if grep -q 'not in the perf stat option allowlist' <<<"$got"; then
    pass "lint-tree-allowlist: an UNANTICIPATED option '$spelling' FAILS CLOSED (no deny-list entry needed)"
  else
    fail "lint-tree-allowlist: '$spelling' must fail closed (got: $got)"
  fi
done

# ---------------------------------------------------------------------------
# THE TWO ALLOWLISTS ARE SEPARATE SETS, NOT A MERGED SUPERSET (#3248)
# ---------------------------------------------------------------------------
# A sampling profile needs `-F`/`-g`; a counting run needs neither. The cheap way to admit
# them would have been to widen PERF_ALLOWED_OPTS, which would silently legalise them on
# EVERY `perf stat` line in the rig — so layer 2 is keyed by SUBCOMMAND instead. These cases
# are the ones that would pass under the cheap version and must not.
for spelling in '-F 999' '-g' '--call-graph fp'; do
  treedir="$(mktemp -d "$TMP/recsepXXXXXX")"
  cp "$PERF_DIR/"*.sh "$treedir/"
  printf 'perf stat -x, -e cycles -C 0 %s -o /dev/null -- true\n' "$spelling" >> "$treedir/lib-args.sh"
  got=$(lint_tree "$treedir")
  if grep -q 'not in the perf stat option allowlist' <<<"$got"; then
    pass "lint-sets-separate: a RECORD option '${spelling%% *}' on a STAT line still FAILS (sets are not merged)"
  else
    fail "lint-sets-separate: '$spelling' must be refused on a stat line (got: $got)"
  fi
done

# THE SUBCOMMAND, NOT ANY TOKEN, PICKS THE ALLOWLIST (roborev job 60, finding 9).
# The first version of the per-subcommand split chose the record allowlist if ANY token on the
# line equalled `record` — so a `perf stat` line whose WORKLOAD ARGUMENT is `record` was judged
# by the looser set and `-F` on a counting line passed silently. Verified against the pre-fix
# code before fixing: that exact line produced no option finding at all. A guard that can be
# relaxed by the name of an unrelated argument is not a guard.
for spelling in 'record' 'record-batches' 'do_record'; do
  treedir="$(mktemp -d "$TMP/subcmdXXXXXX")"
  cp "$PERF_DIR/"*.sh "$treedir/"
  printf 'perf stat -x, -e cycles -C 0 -F 999 -o /dev/null -- ./mytool %s\n' "$spelling" \
    >> "$treedir/lib-args.sh"
  got=$(lint_tree "$treedir")
  if grep -q 'not in the perf stat option allowlist' <<<"$got"; then
    pass "lint-subcmd: a STAT line whose workload arg is '$spelling' is still judged by the STAT set"
  else
    fail "lint-subcmd: workload arg '$spelling' flipped the allowlist (got: $got)"
  fi
done

# ...and the converse: a genuine record line is still judged by the RECORD set even when its
# workload argument is `stat`.
recstat_dir="$(mktemp -d "$TMP/recstatXXXXXX")"
cp "$PERF_DIR/"*.sh "$recstat_dir/"
printf 'perf record -e cycles -F 999 -g -C 2,10 -o /dev/null -- ./mytool stat  # perf-lint-allow: sampling profile\n' \
  >> "$recstat_dir/lib-args.sh"
got=$(lint_tree "$recstat_dir")
if grep -q 'option allowlist' <<<"$got"; then
  fail "lint-subcmd: a RECORD line with workload arg 'stat' must still use the record set (got: $got)"
else
  pass "lint-subcmd: a RECORD line whose workload arg is 'stat' still uses the RECORD set"
fi

# ...and the record allowlist must actually admit its own options, or it is a set that permits
# nothing and the separation buys a guard nobody can satisfy.
recok_dir="$(mktemp -d "$TMP/recokXXXXXX")"
cp "$PERF_DIR/"*.sh "$recok_dir/"
printf 'perf record -e cycles -F 999 -g -C 2,10 -o /dev/null -- true  # perf-lint-allow: sampling profile\n' \
  >> "$recok_dir/lib-args.sh"
got=$(lint_tree "$recok_dir")
if grep -q 'option allowlist' <<<"$got"; then
  fail "lint-sets-separate: a well-formed marked RECORD line must carry no OPTION finding (got: $got)"
else
  pass "lint-sets-separate: a well-formed RECORD line passes the record allowlist"
fi

# A per-process/per-thread option is refused on a RECORD line too. Sampling per-process has
# the same observer-cost problem counting per-process has, so the domain rule is not relaxed
# just because the subcommand changed.
for spelling in '-p 123' '--pid=123' '-t 5' '--tid=5'; do
  treedir="$(mktemp -d "$TMP/recdomXXXXXX")"
  cp "$PERF_DIR/"*.sh "$treedir/"
  printf 'perf record -e cycles -F 999 -C 0 %s -o /dev/null -- true\n' "$spelling" >> "$treedir/lib-args.sh"
  got=$(lint_tree "$treedir")
  if grep -qE 'per-(process|thread) option token' <<<"$got"; then
    pass "lint-sets-separate: '${spelling%% *}' on a RECORD line still FIRES the domain rule"
  else
    fail "lint-sets-separate: '$spelling' must fire on a record line (got: $got)"
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
    EVENTS="cycles"; SERVER_CPUS="1"; PERF_COUNT_CPUS="1"
    # The counting-domain pairing table (#3551), consistent with this probe's own `taskset -c 1`
    # argv. Supplied because the wrapper REFUSES an unchecked domain — there is no default — so
    # without it every accept case below would red on a guard that is doing its job.
    WS0_PERF_COUNT_PAIRINGS="1|1"
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
if grep -q 'PERF-RAN: stat -x, -e cycles -C 1 -o /dev/null -- taskset -c 1 flight-loadgen --shape full --step-duration 45s' <<<"$out"; then
  pass "argv-guard: ordinary COMMAND options (--shape/--step-duration) pass through untouched"
else
  fail "argv-guard: a command's own options must not be refused (out: $out)"
fi
# And the ACCEPT direction: an ordinary argv reaches perf, so the guard is not one
# that refuses everything.
out=$(argv_probe taskset -c 1 /bin/true)
if grep -q 'PERF-RAN: stat -x, -e cycles -C 1 -o /dev/null -- taskset -c 1 /bin/true' <<<"$out"; then
  pass "argv-guard: an ordinary argv is passed through, CPU-wide, with -C (the accept half)"
else
  fail "argv-guard: a clean argv must reach perf with -C (out: $out)"
fi

# ===========================================================================
# PART 2 — the LINT's OWN vacuity states (#3272 review round 3 nits)
# ===========================================================================
# Two ways `perf_invocation_lint`/`_tree` used to read CLEAN while checking nothing, both
# instances of the rule this issue is about: never derive a positive verdict from the
# absence of a bad signal.

# --- 2a. A LINE WHOSE COMMAND WORD **AND** SUBCOMMAND ARE VARIABLES ----------
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

# --- 2b. AN UNREADABLE rig file is a FINDING, not a silent skip ---------------
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

# --- 2c. `library` mode HAS END assertions now (an awk that dies is a finding) -
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
# END block never ran would take the `did not COMPLETE` branch, which is what 2c relies on.
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

# --- 2d. STRUCTURAL: no APOSTROPHE inside the single-quoted awk program -------
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
# checks and passed them exits 0 and reports SUCCESS. That is the suite-level `0/0` shape
# this whole issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite)
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file. DERIVED from a MEASURED run of
# this suite — 113 checks at the split — not carried over from the file it came out of, and not
# a source-line count: the loops here multiply (five option spellings x five rig libraries is
# 25 checks from four written lines), so counting `pass`/`fail` in the text gives 84 and would
# make this floor 29 checks slacker than the suite really is.
MIN_CHECKS=108
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 perf-invocation lint checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks check(s) failed"
exit 1

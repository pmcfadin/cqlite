#!/usr/bin/env bash
# Self-test for the WS0 self-tests' HERMETICITY, as a MECHANISM (issue #3272 review
# round 3, B1).
#
# # The finding this file exists for
#
# `scripts/perf/ws0-baseline.sh` has an argument-validation boundary
# (`--validate-args-only`). BELOW it the driver writes host sysctls via `sudo -n`, runs
# `cargo build --release`, drops the page cache and takes 45-second `perf stat`
# measurements. A self-test that invokes the driver without that flag, on a Linux host
# where `perf`/`taskset` exist and the default `2,10` are genuine siblings — i.e. the box
# the gate's `tooling-tests` component runs on — does all of it.
#
# Round 1 of #3272's review found that (six accept call sites running the world). Round 2
# introduced `--validate-args-only` + recording shims and converted the accept cases, and
# round 3 found ONE call site still bare: the cold-ceiling accept case, whose `--temp warm`
# skips the ceiling so control falls straight past the boundary. A MANUAL SWEEP MISSED IT
# TWICE, which is the whole argument for this file: the contract is now checked by a lint
# over every `test_ws0_*.sh`, and a bare invocation added later FAILS here.
#
# MEASURED against that bare call site on a Linux-shaped host (fake sysfs with genuine
# `2,10` siblings, readable sysctl priors, recording PATH shims), the recording file held:
#
#     sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
#     sudo -n sysctl -w kernel.perf_event_paranoid=2
#     sudo -n sysctl -w kernel.kptr_restrict=1
#
# — a real host mutation and its restore. Where `sudo -n` SUCCEEDS (the gate's box) the run
# continues into the release build and the measurement loop.
#
# # What is asserted here
#
#   1. THE LINT'S SUBJECT IS EVERY SELF-TEST, discovered by glob, and an empty subject is
#      a FINDING rather than a clean tree.
#   2. THE SHIPPED SELF-TESTS ARE CLEAN under it.
#   3. THE LINT FIRES on each bare spelling — `$DRIVER`, `${DRIVER}`, a literal path, and
#      a driver COPY — so its discriminating power is measured, not assumed (#3249: a
#      hardcoded `_PERF_STATE="ok"` survived 118/118 tests).
#   4. THE LINT DOES NOT FIRE on ordinary lines (prose, a `ws0_driver_run` call, a marked
#      line), because a lint that reds on ordinary code is the one an operator deletes.
#   5. THE DRIVER IS HERMETIC ON A LINUX-SHAPED HOST — the platform property that made B1
#      invisible. A fake sysfs tree with genuine siblings + readable sysctl priors +
#      recording shims, and the assertion is that the recording file stays EMPTY through
#      `ws0_driver_run`, together with a POSITIVE CONTROL proving the same fixture DOES
#      record when the driver is run bare. Without the control, an empty file could mean
#      "hermetic" or "the fixture never reached anything", which is the `0/0` shape.
#
# Hermetic on every platform: a fake sysfs tree, a rewritten copy of the rig's libraries
# and recording shims, all under $TMPDIR. No perf, no sudo, no taskset, no cargo, no root,
# no corpus, no network. The real scripts/perf files are never modified.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TESTS_DIR="$REPO_ROOT/scripts/tests"
HERMETIC_LIB="$TESTS_DIR/lib-ws0-hermetic.sh"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"

fails=0
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$HERMETIC_LIB" ] || { echo "FAIL - missing $HERMETIC_LIB"; exit 1; }
[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
# python3 absence is a FAILURE, not a skip (#3272 review B8): it is a hard requirement of
# the rig, and `exit 0` here would record the gate component as SUCCESS with none of the
# checks below having run.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig"
  echo "       (ws0-baseline.sh refuses to run without it) and this file's Linux-shaped"
  echo "       fixture needs it. A skip would record the gate component as SUCCESS with"
  echo "       0 of its checks having run (#3272 review B8)."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$HERMETIC_LIB"
ws0_hermetic_init "$TMP"

# ===========================================================================
# 1 — the lint's SUBJECT, and its COMPLETENESS against an INDEPENDENT oracle (B2)
# ===========================================================================
# THE FINDING THIS REPLACES (#3272 review round 4, B2). The subject used to be
# `"$dir"/test_ws0_*.sh` ONLY, so the two `lib-ws0-*.sh` helpers round 3 had just added — one
# of which is where `ws0_driver_run` LIVES — were never examined: a bare invocation in a shared
# helper read as a clean tree. And the check right here compared
# `ws0_hermeticity_lint_subject` against `ls ./test_ws0_*.sh`, i.e. THE SAME GLOB AGAINST
# ITSELF, which can only ever confirm the subject's own definition.
#
# So there are now TWO independently-defined things, and the assertion is between them:
#   SUBJECT — every `*.sh`/`*.py` under scripts/tests (a definition; consults nothing).
#   CENSUS  — every TRACKED file whose CONTENT names the driver (`git ls-files`; an oracle).
# and `subject` mode asserts SUBJECT ⊇ (CENSUS − EXEMPTIONS), printing `UNCOVERED` otherwise.
subject_report=$(ws0_hermeticity_subject_report "$TESTS_DIR")
# The SUBJECT must include the LIBRARIES, which is the concrete half of B2.
missing_libs=""
for lib in lib-ws0-hermetic.sh lib-ws0-fixtures.sh ws0_hermeticity_lint.py; do
  grep -q "^SUBJECT	scripts/tests/$lib$" <<<"$subject_report" || missing_libs="$missing_libs $lib"
done
if [ -z "$missing_libs" ]; then
  pass "lint-subject: the SOURCED LIBRARIES are in the subject (B2: they were excluded, and one holds ws0_driver_run)"
else
  fail "lint-subject: the subject omits$missing_libs — a bare invocation there would be invisible (B2)"
fi
# ...and every `test_ws0_*.sh` too, which is what the old glob covered.
n_tests=0; missing_tests=""
for f in "$TESTS_DIR"/test_ws0_*.sh; do
  n_tests=$((n_tests + 1))
  b="$(basename "$f")"
  grep -q "^SUBJECT	scripts/tests/$b$" <<<"$subject_report" || missing_tests="$missing_tests $b"
done
if [ "$n_tests" -ge 4 ] && [ -z "$missing_tests" ]; then
  pass "lint-subject: all $n_tests test_ws0_*.sh are in the subject as well"
else
  fail "lint-subject: the subject omits$missing_tests (found $n_tests suites)"
fi
# THE COMPLETENESS ORACLE MUST BE ABLE TO SAY 'NO'. A containment assertion whose two sides
# share a definition can only ever pass — which is exactly what B2 was, and what a first draft
# of the fix reproduced by folding the census into the subject. So the oracle is DRIVEN.
#
# Driven inside a THROWAWAY GIT REPO under $TMP, never by mutating this checkout: the census
# reads `git ls-files`, so it needs a real repo — and the gate FAILs a run whose worktree mutates
# mid-run (#2926), which rules out appending to a tracked file here even with an immediate
# restore. The fixture is the same shape as the real tree (a `scripts/tests` dir plus a file
# outside it), which is all the oracle looks at.
probe_repo="$TMP/oracle-repo"
mkdir -p "$probe_repo/scripts/tests" "$probe_repo/scripts/lib"
cp "$TESTS_DIR/ws0_hermeticity_lint.py" "$probe_repo/scripts/tests/"
printf '#!/usr/bin/env bash\n: "in the subject"\n' > "$probe_repo/scripts/tests/test_ws0_probe.sh"
# The UNCOVERED case: a tracked file OUTSIDE the tests root that NAMES the driver, with no
# exemption for its path.
printf '#!/usr/bin/env bash\nbash "$SOMEWHERE/ws0-baseline.sh" --corpus /c\n' \
  > "$probe_repo/scripts/lib/rogue.sh"
( cd "$probe_repo" && git init -q . && git add -A && git -c user.email=t@t -c user.name=t commit -qm probe ) >/dev/null 2>&1
probe_report=$(ws0_hermeticity_subject_report "$probe_repo/scripts/tests")
if grep -q '^UNCOVERED	scripts/lib/rogue.sh' <<<"$probe_report" \
  && grep -q '^#COMPLETE .*uncovered=1$' <<<"$probe_report"; then
  pass "lint-subject: OBSERVED — the completeness oracle reports an UNCOVERED tracked file (it CAN say no)"
else
  fail "lint-subject: the oracle must flag a tracked driver-naming file outside the subject (got: $(grep -E '^(UNCOVERED|#COMPLETE)' <<<"$probe_report"))"
fi
# THE CONTROL, on the SAME fixture: remove the rogue file and `uncovered` must drop to 0, so the
# finding above is attributable to that file and not to anything else about a probe repo.
rm -f "$probe_repo/scripts/lib/rogue.sh"
( cd "$probe_repo" && git add -A && git -c user.email=t@t -c user.name=t commit -qm drop ) >/dev/null 2>&1
if grep -q '^#COMPLETE .*uncovered=0$' <<<"$(ws0_hermeticity_subject_report "$probe_repo/scripts/tests")"; then
  pass "lint-subject: the SAME fixture without the rogue file reports uncovered=0 (the finding was the file)"
else
  fail "lint-subject: the control fixture must report uncovered=0"
fi
# ...and the SHIPPED tree must itself be uncovered=0.
if grep -q '^#COMPLETE .*uncovered=0$' <<<"$subject_report"; then
  pass "lint-subject: the SHIPPED tree has uncovered=0 (every driver-naming tracked file is in the subject or exempt)"
else
  fail "lint-subject: the shipped tree must have uncovered=0 (got: $(grep -E '^(UNCOVERED|#COMPLETE)' <<<"$subject_report"))"
fi
# A STALE EXEMPTION is reported too, so the exemption list cannot accumulate claims nobody
# checks. Asserted over the shipped tree: there must be none.
if ! grep -q '^STALE-EXEMPTION' <<<"$subject_report"; then
  pass "lint-subject: no STALE exemption (every exempted path is tracked and still names the driver)"
else
  fail "lint-subject: a stale exemption remains: $(grep '^STALE-EXEMPTION' <<<"$subject_report")"
fi

# An EMPTY subject must be a FINDING. A checker whose subject is empty prints nothing and
# reads exactly like a clean tree — the vacuity shape this whole issue is about.
empty_dir="$TMP/no-tests"; mkdir -p "$empty_dir"
if grep -q "subject is EMPTY" <<<"$(ws0_hermeticity_lint_tree "$empty_dir")"; then
  pass "lint-vacuity: a directory with NO scripts is a FINDING (not a silent clean tree)"
else
  fail "lint-vacuity: an empty subject must be reported (got: $(ws0_hermeticity_lint_tree "$empty_dir"))"
fi
# An UNREADABLE file likewise: `continue`ing past it would drop it from the subject
# silently.
if grep -q "subject is ABSENT" <<<"$(ws0_hermeticity_lint "$TMP/does-not-exist.sh")"; then
  pass "lint-vacuity: an unreadable file is a FINDING, not silently skipped"
else
  fail "lint-vacuity: an unreadable file must be reported (got: $(ws0_hermeticity_lint "$TMP/does-not-exist.sh"))"
fi
# ...and the lint's own COMPLETION is verified, not assumed: a python that died mid-scan prints
# nothing. Driven by pointing the wrapper at a deliberately broken implementation.
broken_lib="$TMP/broken-lint"; mkdir -p "$broken_lib"
printf 'import sys\nsys.exit(3)\n' > "$broken_lib/ws0_hermeticity_lint.py"
if grep -q "did not COMPLETE" \
  <<<"$(WS0_HERMETIC_LINT_PY="$broken_lib/ws0_hermeticity_lint.py" ws0_hermeticity_lint "$DRIVER")"; then
  pass "lint-vacuity: OBSERVED — a lint that exits without its #COMPLETE marker is a FINDING"
else
  fail "lint-vacuity: an incomplete scan must be reported, not read as clean"
fi

# ===========================================================================
# 2 — the SHIPPED self-tests are clean
# ===========================================================================
# The negative direction, which is what keeps a lint that reds unconditionally from
# passing every positive case below.
shipped=$(ws0_hermeticity_lint_tree "$TESTS_DIR")
if [ -z "$shipped" ]; then
  pass "lint-shipped: every shipped test_ws0_*.sh invokes the driver ONLY through ws0_driver_run"
else
  fail "lint-shipped: a shipped self-test invokes the driver bare: $shipped"
fi

# ===========================================================================
# 3 — the lint FIRES on each bare spelling (its discriminating power, measured)
# ===========================================================================
# Each of these is a spelling that ACTUALLY APPEARED in the pre-fix files, or is one
# line's edit away from one. Per #3249 a guard that has not been observed firing is not
# evidence, so every spelling is driven rather than reasoned about.
lint_probe() { # lint_probe <line> — the lint's findings for a one-line file WITH a driver handle
  printf 'DRIVER=/x/ws0-baseline.sh\n%s\n' "$1" > "$TMP/probe.sh"
  ws0_hermeticity_lint "$TMP/probe.sh"
}
# ...and the same, in a file that NEVER names the driver, for asserting the file-level scope of
# the fail-closed posture (see the `lint-silent (B1 scope)` case below).
lint_probe_nohandle() {
  printf '%s\n' "$1" > "$TMP/probe-nohandle.sh"
  ws0_hermeticity_lint "$TMP/probe-nohandle.sh"
}
# The probe LINES are COMPOSED from `$SH` rather than written literally, and that is not
# cosmetic: THIS FILE is inside the lint's own subject (`test_ws0_*.sh`), so a literal
# `bash "$DRIVER" …` in a probe list is indistinguishable to the lint from a real bare
# invocation — the first version of this file reported EIGHT findings against its own probe
# corpus and failed `lint-shipped`. Composing keeps the probe INPUT exact while the source
# LINE carries no literal invocation. (Marking the lines instead would put the marker text
# inside the probe string, changing the input under test.)
SH='bash'
ALT_SH='sh'
for spelling in \
  "out=\$($SH \"\$DRIVER\" --corpus /c --temp warm 2>&1)" \
  "out=\$($SH \"\${DRIVER}\" --corpus /c 2>&1)" \
  "$SH \"\$REPO_ROOT/scripts/perf/ws0-baseline.sh\" --corpus /c" \
  "out=\$($SH \"\$copy\" --corpus /nonexistent 2>&1)" \
  "PATH=\"\$SHIM:\$PATH\" $SH \"\$DRIVER\" --corpus /c" \
  "$ALT_SH \"\$DRIVER\" --corpus /c" \
  ; do
  if grep -q 'invokes (or could invoke) the WS0 driver outside ws0_driver_run' <<<"$(lint_probe "$spelling")"; then
    pass "lint-fires: a bare invocation spelled \`${spelling:0:44}…\` is FLAGGED"
  else
    fail "lint-fires: '$spelling' must be flagged (got: $(lint_probe "$spelling"))"
  fi
done

# --- the FOUR spellings the awk predecessor could not see (#3272 round 4, B1) ------------
# Each of these was MEASURED at ZERO findings against the pre-fix lint. They are asserted
# separately from the list above, and labelled, so a regression names the shape it lost rather
# than "one of six probes".
#
# The FIRST is a LINE CONTINUATION, which is why it cannot be a single-line probe: the shell
# token and the driver token are on different PHYSICAL lines, and neither line alone carries
# both — the exact reason a physical-line predicate missed it.
printf 'DRIVER=/x/ws0-baseline.sh\n%s \\\n  "$DRIVER" --corpus /c\n' "$SH" > "$TMP/probe.sh"
if grep -q 'invokes (or could invoke)' <<<"$(ws0_hermeticity_lint "$TMP/probe.sh")"; then
  pass "lint-fires (B1): a LINE-CONTINUATION split \`bash \\\` + \`\"\$DRIVER\"\` is FLAGGED (was 0 findings)"
else
  fail "lint-fires (B1): the continuation-split invocation must be flagged"
fi
# ...and the finding must be reported at the line the LOGICAL line STARTED on, or a reader
# cannot find it.
if grep -q '^2: ' <<<"$(ws0_hermeticity_lint "$TMP/probe.sh")"; then
  pass "lint-fires (B1): the continuation finding is reported at the logical line's START (line 2)"
else
  fail "lint-fires (B1): expected the finding at line 2 (got: $(ws0_hermeticity_lint "$TMP/probe.sh"))"
fi
# The other three: no shell token at all. A bare exec, an `exec`, and the `env -i` form the
# DRIVER'S OWN USAGE TEXT documents (ws0-baseline.sh's usage block), which makes it the shape
# most likely to be written.
for b1 in \
  '"$DRIVER" --corpus /c' \
  'exec "$DRIVER" --corpus /c' \
  'env -i "$DRIVER" --corpus /c' \
  'timeout 60 "$DRIVER" --corpus /c' \
  'taskset -c 2,10 "$DRIVER" --corpus /c' \
  ; do
  if grep -q 'invokes (or could invoke)' <<<"$(lint_probe "$b1")"; then
    pass "lint-fires (B1): \`$b1\` is FLAGGED (no shell token on the line; was 0 findings)"
  else
    fail "lint-fires (B1): '$b1' must be flagged — it reaches the measurement loop on Linux (got: $(lint_probe "$b1"))"
  fi
done
# THE POSTURE ITSELF, driven: within a file that HAS A HANDLE on the driver, an UNRESOLVABLE
# command word is treated AS an invocation. That is what removes the enumeration — a spelling
# nobody has thought of yet fails CLOSED — and it is the perf lint's layer-1 posture ported.
#
# `$copy` is the real instance: `test_ws0_cpu_pinning_guards.sh` builds a driver COPY and runs
# it, so a variable holding a path is genuinely how the driver gets invoked here.
if grep -q 'VARIABLE this lint cannot resolve' <<<"$(lint_probe 'out=$("$copy" --corpus /c)')"; then
  pass "lint-fires (B1): an UNRESOLVABLE command word is treated AS an invocation (no enumeration left to be wrong)"
else
  fail "lint-fires (B1): an unresolvable command word must fail closed (got: $(lint_probe 'out=$("$copy" --corpus /c)'))"
fi
# THE SCOPE OF THAT POSTURE IS FILE-LEVEL, and it is asserted rather than left implicit, because
# the alternative was MEASURED and is worse. `has_driver_handle` requires the FILE to name the
# driver before any unresolvable command word in it counts. Removing that gate — i.e. treating
# every unresolvable command word in every script as a candidate — produces **74 findings across
# the six shipped ws0 suites**, all of them ordinary code (`out=$(run_report "$d" …)`,
# `root="$(cd "$dir/../.." && pwd)"`, `base="$(basename "$lib")"`). A lint with 74 false
# findings is the lint an operator deletes, which is why the gate exists and why its cost is
# recorded here instead of being rediscovered.
if [ -z "$(lint_probe_nohandle 'out=$("$some_unrelated_cmd" --flag x)')" ]; then
  pass "lint-silent (B1 scope): in a file with NO driver handle, an unresolvable command word is NOT a finding (74 false findings measured without this gate)"
else
  fail "lint-silent (B1 scope): a file that never names the driver must not be linted for unresolvable words"
fi

# ===========================================================================
# 4 — the lint does NOT fire on ordinary lines
# ===========================================================================
# A lint that reds on ordinary code is the one an operator deletes, so the SILENT direction
# is asserted over the shapes these files really contain: the sanctioned call, prose, a
# marked line, a `-f` test, a `bash -c` sourcing a library, and an `awk` reading the driver
# as TEXT.
#
# The last two are a REAL FALSE FINDING this lint produced against this repo's own test code
# (#3272 round 4), kept as a permanent case. `grep -n '^for temp in $TEMPS; do' "$DRIVER"` was
# FLAGGED: a whitespace split does not respect quoting, so the `;` inside the quoted pattern
# read as a control operator (reset to command position), the next token reduced to the wrapper
# word `do`, and the driver path — an ARGUMENT to `grep` — became a candidate command word. The
# fix tracks QUOTE PARITY across tokens; a token inside an open string is skipped, which cannot
# hide an invocation because a command word is by definition not inside a string.
MARKER='ws0-hermetic-allow'
for benign in \
  "out=\$(ws0_driver_run \"\$DRIVER\" --corpus /c --temp warm)" \
  "# a comment mentioning $SH \"\$DRIVER\" --corpus /c" \
  "$SH \"\$DRIVER\" --corpus /c   # $MARKER: a documented exemption" \
  "if [ -f \"\$DRIVER\" ]; then :; fi" \
  "out=\$($SH -c \"set -u; source \$ARGS_LIB; duration_reject x 1s 3\")" \
  "awk \"/^trap /\" \"\$DRIVER\" | grep -q INT" \
  "pin_line=\$(grep -n '^for temp in \$TEMPS; do' \"\$DRIVER\" | head -1)" \
  "line=\$(grep -n 'x; do y' \"\$DRIVER\" | cut -d: -f1)" \
  ; do
  if [ -z "$(lint_probe "$benign")" ]; then
    pass "lint-silent: an ordinary line \`${benign:0:44}…\` is NOT flagged"
  else
    fail "lint-silent: '$benign' must not be flagged (got: $(lint_probe "$benign"))"
  fi
done

# ===========================================================================
# 5 — the driver is HERMETIC ON A LINUX-SHAPED HOST, with a positive control
# ===========================================================================
# This is the platform property that made B1 invisible for two rounds: on macOS the driver
# stops early at `perf is not installed`, so a bare invocation LOOKS hermetic. The fixture
# below makes the host Linux-shaped as far as the driver can tell:
#
#   * a fake `/sys/devices/system/cpu` in which the DEFAULT `--server-cpus 2,10` really
#     ARE one physical core's siblings (so `verify_sibling_pair` passes);
#   * readable `perf_event_paranoid`/`kptr_restrict` priors holding non-`-1` values (so
#     `relax_perf_sysctls` decides it must weaken them);
#   * `perf`, `taskset`, `sudo` and `cargo` present on PATH as RECORDING shims;
#   * a corpus dir holding a `*-Data.db` and the DDL, so the corpus checks pass.
#
# The libraries' hardcoded absolute paths are rewritten in the COPY (never in the shipped
# tree) because they are the two things a test cannot inject: `lib-cpu.sh` reads its root
# from an env var that `assert_real_cpu_topology` deliberately REFUSES in a measurement
# run, and `lib-host-state.sh` reads `/proc` directly.
LINUX_RIG="$TMP/linux-rig"
mkdir -p "$LINUX_RIG/rig" "$LINUX_RIG/topo" "$LINUX_RIG/proc" "$LINUX_RIG/corpus/ws0/events"
: > "$LINUX_RIG/corpus/ws0/events/nb-1-big-Data.db"
printf 'CREATE TABLE ws0.events (id text PRIMARY KEY);\n' > "$LINUX_RIG/corpus/ws0-events.cql"
for _c in 2 10; do
  mkdir -p "$LINUX_RIG/topo/cpu$_c/topology"
  printf '2,10\n' > "$LINUX_RIG/topo/cpu$_c/topology/thread_siblings_list"
done
for _c in 4 12 5 13 6 14 7 15; do
  mkdir -p "$LINUX_RIG/topo/cpu$_c/topology"
  printf '%s\n' "$_c" > "$LINUX_RIG/topo/cpu$_c/topology/thread_siblings_list"
done
unset _c
printf '2\n' > "$LINUX_RIG/proc/perf_event_paranoid"
printf '1\n' > "$LINUX_RIG/proc/kptr_restrict"
cp "$REPO_ROOT/scripts/perf/"*.sh "$REPO_ROOT/scripts/perf/"*.py "$LINUX_RIG/rig/"
python3 - "$LINUX_RIG" <<'PY'
import pathlib, sys
root = pathlib.Path(sys.argv[1])
cpu = root / "rig" / "lib-cpu.sh"
s = cpu.read_text()
assert "/sys/devices/system/cpu" in s, "lib-cpu.sh no longer names the sysfs root — fixture is stale"
cpu.write_text(s.replace("/sys/devices/system/cpu", str(root / "topo")))
host = root / "rig" / "lib-host-state.sh"
s = host.read_text()
for knob in ("perf_event_paranoid", "kptr_restrict"):
    needle = f"/proc/sys/kernel/{knob}"
    assert needle in s, f"lib-host-state.sh no longer names {needle} — fixture is stale"
    s = s.replace(needle, str(root / "proc" / knob))
host.write_text(s)
PY
LINUX_DRIVER="$LINUX_RIG/rig/ws0-baseline.sh"

# 5a. THE POSITIVE CONTROL, FIRST. Run the driver BARE — exactly as the leaky call site
# did — and require the shims to RECORD. Without this the empty-file assertion in 5b could
# be satisfied by a fixture that never reached anything, which is the vacuity shape: an
# oracle that cannot answer producing a positive verdict.
ws0_hermetic_reset
PATH="$WS0_SHIM_BIN:$PATH" bash "$LINUX_DRIVER" \
  --corpus "$LINUX_RIG/corpus" --temp warm --cold-step-duration 45s \
  >/dev/null 2>&1  # ws0-hermetic-allow: THE POSITIVE CONTROL — this bare run must leak
control_calls="$(ws0_hermetic_calls)"
if grep -q 'sysctl -w kernel.perf_event_paranoid=-1' <<<"$control_calls"; then
  pass "linux-control: OBSERVED — run BARE on a Linux-shaped host the driver WRITES HOST SYSCTLS (this is B1)"
else
  fail "linux-control: the bare run must reach relax_perf_sysctls, else 5b proves nothing about hermeticity (calls: $control_calls)"
fi
# ...and the fixture must have got there through the REAL checks, not by an early error:
# the sibling verification must be observed to have PASSED.
ws0_hermetic_reset
control_out=$(PATH="$WS0_SHIM_BIN:$PATH" bash "$LINUX_DRIVER" \
  --corpus "$LINUX_RIG/corpus" --temp warm --cold-step-duration 45s 2>&1)  # ws0-hermetic-allow: the positive control's transcript
if grep -q "verified siblings of one physical core" <<<"$control_out"; then
  pass "linux-control: the fixture IS Linux-shaped — the sibling check PASSED on it (2,10 are genuine siblings here)"
else
  fail "linux-control: the fixture must pass the sibling check, or it is not the platform B1 hides on (out: $(head -4 <<<"$control_out"))"
fi

# 5b. AND THE SANCTIONED PATH IS HERMETIC ON THE SAME FIXTURE. Same host shape, same
# arguments, same shims — the only difference is `ws0_driver_run`. The recording file must
# be EMPTY.
out=$(ws0_driver_run "$LINUX_DRIVER" --corpus "$LINUX_RIG/corpus" --temp warm --cold-step-duration 45s); rc=$?
calls="$(ws0_hermetic_calls)"
if [ "$rc" -eq 0 ] && grep -q "ARGUMENTS OK" <<<"$out" && [ -z "$calls" ]; then
  pass "linux-hermetic: OBSERVED — on the SAME Linux-shaped host ws0_driver_run executes NOTHING (shim file EMPTY)"
else
  fail "linux-hermetic: ws0_driver_run must reach the boundary and record no invocation (rc=$rc, calls: $calls, out: $out)"
fi
# The knobs must also be UNTOUCHED as state, not merely unrecorded as calls: the priors on
# disk still hold their original values.
if [ "$(cat "$LINUX_RIG/proc/perf_event_paranoid")" = "2" ] \
   && [ "$(cat "$LINUX_RIG/proc/kptr_restrict")" = "1" ]; then
  pass "linux-hermetic: the fixture's sysctl priors are UNCHANGED (2/1) after the hermetic run"
else
  fail "linux-hermetic: the hermetic run must not change host state"
fi
# And the REJECT direction is hermetic on this host too — round 2 left every reject call
# site bare on the reasoning that a rejection exits early, which holds for the rejection
# asserted and not for the accept-adjacent probe beside it.
out=$(ws0_driver_run "$LINUX_DRIVER" --corpus "$LINUX_RIG/corpus" --temp cold --cold-step-duration 45s); rc=$?
calls="$(ws0_hermetic_calls)"
if [ "$rc" -ne 0 ] && grep -q "exceeds the" <<<"$out" && [ -z "$calls" ]; then
  pass "linux-hermetic: a REJECT case is hermetic on the same host (the ceiling fires, nothing runs)"
else
  fail "linux-hermetic: the reject path must stay hermetic (rc=$rc, calls: $calls, out: $out)"
fi

# ===========================================================================
# 6 — the shims themselves RECORD (the oracle can answer)
# ===========================================================================
# Every hermeticity assertion above is read off an EMPTY recording file, so a shim that
# does not record would make all of them vacuous — a positive verdict from an unmeasured
# state. 5a already proves the shims fire through a real driver run; this asserts each
# tool individually, so a partial shim set is diagnosable.
for tool in $WS0_SHIM_TOOLS; do
  ws0_hermetic_reset
  PATH="$WS0_SHIM_BIN:$PATH" "$tool" --probe >/dev/null 2>&1
  if grep -q "^$tool " "$WS0_HERMETIC_CALLS"; then
    pass "shim-records: the '$tool' shim is on PATH and RECORDS (the oracle can answer)"
  else
    fail "shim-records: the '$tool' shim must record, else every empty-file check is vacuous"
  fi
done
# And a shim must exit NON-ZERO, so a leak also breaks the run rather than merely being
# noted: a recording shim that succeeded would let a leaked `cargo build` "work".
ws0_hermetic_reset
PATH="$WS0_SHIM_BIN:$PATH" sudo --probe >/dev/null 2>&1
if [ "$?" -ne 0 ]; then
  pass "shim-records: a shim exits NON-ZERO, so a leak fails the run and is not merely recorded"
else
  fail "shim-records: the shims must exit non-zero"
fi

# ===========================================================================
# 7 — EVERY ws0 self-test carries a minimum-check-count FLOOR WITH TEETH, and it FIRES
# ===========================================================================
# `set -uo pipefail` (no `-e`) means a block that silently never executes lowers a suite's
# check count and registers NO failure, while the gate reads only the exit code. So each
# `test_ws0_*.sh` ends in a `[ "$checks" -ge N ]` assert.
#
# THE FLOOR MUST HAVE TEETH (#3272 review round 4 nit). The check here used to be
# `grep -q 'MIN_CHECKS='`, which ACCEPTS `MIN_CHECKS=0` — a floor satisfied by a suite that ran
# nothing, i.e. decorative. And a hardcoded expected value per suite would be bumped rather than
# derived. So the floor is now checked as a NUMBER against the suite's OWN OBSERVED check count:
# it must be >= a hard minimum (a floor of 0/1 is not a floor) and it must be <= what the suite
# actually runs (a floor above the real count would red the suite, which the `floor-fires` probe
# below drives deliberately).
#
# THE SUBJECT IS THE WS0 SUITES, not the whole tests dir. `ws0_hermeticity_lint_subject` now
# covers every script under `scripts/tests/` (that is B2's fix, and correct for the LINT), but
# the floor convention is a WS0 convention — the other ~50 suites in this repo do not carry it,
# and asserting it over them would red on 50 files this issue does not own.
FLOOR_HARD_MIN=5
for suite in "$TESTS_DIR"/test_ws0_*.sh; do
  base="$(basename "$suite")"
  floor=$(sed -n 's/^MIN_CHECKS=\([0-9]\{1,\}\)$/\1/p' "$suite" | head -1)
  if [ -z "$floor" ]; then
    fail "floor-present: $base carries no numeric MIN_CHECKS floor — its suite-level 0/0 is open"
    continue
  fi
  if [ "$floor" -lt "$FLOOR_HARD_MIN" ]; then
    fail "floor-present: $base has MIN_CHECKS=$floor, below the hard minimum $FLOOR_HARD_MIN — a floor that low is DECORATIVE (MIN_CHECKS=0 used to satisfy this check)"
    continue
  fi
  pass "floor-present: $base carries a MIN_CHECKS floor of $floor (>= the $FLOOR_HARD_MIN hard minimum, so not decorative)"
done
# ...and the hard minimum itself must have been OBSERVED to reject. Driven, because "the floor
# has teeth" is exactly the kind of claim that is true of the code and false of the check.
floor_probe_dir="$TMP/floor-teeth"; mkdir -p "$floor_probe_dir"
printf 'MIN_CHECKS=0\n' > "$floor_probe_dir/test_ws0_decorative.sh"
decorative=$(sed -n 's/^MIN_CHECKS=\([0-9]\{1,\}\)$/\1/p' "$floor_probe_dir/test_ws0_decorative.sh" | head -1)
if [ -n "$decorative" ] && [ "$decorative" -lt "$FLOOR_HARD_MIN" ]; then
  pass "floor-present: OBSERVED — MIN_CHECKS=0 is REJECTED by the hard minimum (the pre-fix grep accepted it)"
else
  fail "floor-present: MIN_CHECKS=0 must be rejected, else the floor is decorative"
fi
# The FIRING half, on ONE suite, because the mechanism is textually identical in all of them and
# running several twice would multiply this component's runtime for no new information.
#
# `test_ws0_host_state_guards.sh` (393 lines, 25 checks, no listeners) is the CHEAPEST — measured.
# It used to be `test_ws0_cpu_pinning_guards.sh`, which the comment CALLED "the cheapest of the
# three" while it is in fact the LARGEST: 1213 lines, 138 checks, real listeners and multi-second
# waits, driven twice more here (#3272 review round 4 nit). Being wrong about which is cheapest is
# only a runtime cost, but the comment asserting it was evidence of nothing.
#
# The copy is driven from a MIRROR TREE of symlinks, never from a file written into the
# repo: each suite resolves `REPO_ROOT` from its own `BASH_SOURCE`, so a copy in `$TMP`
# alone looks for `scripts/perf` beside itself and dies on the wrong thing. Writing into
# the real `scripts/tests` would work and is not available: the gate's `tree-integrity`
# check FAILS a run whose worktree mutates mid-run (#2926).
probe_suite="test_ws0_host_state_guards.sh"
probe_root="$TMP/floor-probe-root"
mkdir -p "$probe_root/scripts/tests"
ln -s "$REPO_ROOT/scripts/perf" "$probe_root/scripts/perf"
for _f in "$TESTS_DIR"/*.sh; do ln -s "$_f" "$probe_root/scripts/tests/$(basename "$_f")"; done
unset _f
probe_copy="$probe_root/scripts/tests/floor-probe.sh"
rm -f "$probe_copy"
python3 - "$TESTS_DIR/$probe_suite" "$probe_copy" <<'PY'
import pathlib, re, sys
src, dst = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
s = src.read_text()
new, n = re.subn(r'^MIN_CHECKS=\d+$', 'MIN_CHECKS=999999', s, flags=re.M)
if n != 1:
    sys.exit(f"expected exactly one MIN_CHECKS assignment in {src.name}, found {n}")
dst.write_text(new)
PY
if [ -s "$probe_copy" ] && ! out=$(bash "$probe_copy" 2>&1); then
  if grep -q 'check(s) ran; this suite has at least 999999' <<<"$out"; then
    pass "floor-fires: OBSERVED — a suite whose floor exceeds its real check count EXITS NON-ZERO, naming both"
  else
    fail "floor-fires: the copy failed for the wrong reason (out: $(tail -3 <<<"$out"))"
  fi
else
  fail "floor-fires: $probe_suite with MIN_CHECKS=999999 must exit non-zero — the floor is decorative"
fi
# ...and the SAME copy with its ORIGINAL floor must PASS, so `floor-fires` above is
# attributable to the floor rather than to anything else about running a copy.
cp "$TESTS_DIR/$probe_suite" "$probe_root/scripts/tests/floor-control.sh"
if bash "$probe_root/scripts/tests/floor-control.sh" >/dev/null 2>&1; then
  pass "floor-control: the SAME copy with its real floor PASSES (floor-fires is the floor, not the copy)"
else
  fail "floor-control: an unmodified copy of $probe_suite must pass, or floor-fires proves nothing"
fi

# ===========================================================================
# A MINIMUM CHECK COUNT for THIS suite too (#3272 round 3 nit)
# ===========================================================================
# A block that silently never executes lowers the check count without registering a
# failure, and the gate reads only the exit code — so a suite that runs 3 of its checks
# and passes them exits 0. The floor is the suite-level `0/0` guard. It is deliberately
# below the current count (so adding a case does not red the suite) and far above zero.
MIN_CHECKS=40
echo
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would otherwise lower the count with"
  echo "       no failure registered, and the gate reads only the exit code (#3272)."
  exit 1
fi
if [ "$fails" -eq 0 ]; then
  echo "ws0 hermeticity: all $checks checks passed"
  exit 0
fi
echo "ws0 hermeticity: $fails of $checks check(s) FAILED"
exit 1

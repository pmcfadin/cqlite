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
# 1 — the lint's SUBJECT is every self-test, DISCOVERED
# ===========================================================================
# "The lint covers every self-test" is a claim about a SET, so the set is printed and
# compared against what is on disk. A hand-maintained list would drift the moment someone
# added a fourth file — which is exactly how the perf lint's own subject went stale (R2).
on_disk=$(cd "$TESTS_DIR" && ls -1 ./test_ws0_*.sh | sed 's#^\./##' | sort)
subject=$(ws0_hermeticity_lint_subject "$TESTS_DIR" | xargs -n1 basename | sort)
n_on_disk=$(printf '%s\n' "$on_disk" | grep -c .)
if [ "$n_on_disk" -ge 4 ] && [ "$subject" = "$on_disk" ]; then
  pass "lint-subject: the hermeticity lint covers ALL $n_on_disk test_ws0_*.sh (discovered, not enumerated)"
else
  fail "lint-subject: the subject ($subject) is not every test_ws0_*.sh ($on_disk)"
fi

# An EMPTY subject must be a FINDING. A checker whose subject is empty prints nothing and
# reads exactly like a clean tree — the vacuity shape this whole issue is about.
empty_dir="$TMP/no-tests"; mkdir -p "$empty_dir"
if grep -q "subject is EMPTY" <<<"$(ws0_hermeticity_lint_tree "$empty_dir")"; then
  pass "lint-vacuity: a directory with NO test_ws0_*.sh is a FINDING (not a silent clean tree)"
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
lint_probe() { # lint_probe <line> — the lint's findings for a one-line file
  printf 'DRIVER=/x/ws0-baseline.sh\n%s\n' "$1" > "$TMP/probe.sh"
  ws0_hermeticity_lint "$TMP/probe.sh"
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
  if grep -q 'invokes the WS0 driver outside ws0_driver_run' <<<"$(lint_probe "$spelling")"; then
    pass "lint-fires: a bare invocation spelled \`${spelling:0:44}…\` is FLAGGED"
  else
    fail "lint-fires: '$spelling' must be flagged (got: $(lint_probe "$spelling"))"
  fi
done

# ===========================================================================
# 4 — the lint does NOT fire on ordinary lines
# ===========================================================================
# A lint that reds on ordinary code is the one an operator deletes, so the SILENT direction
# is asserted over the shapes these files really contain: the sanctioned call, prose, a
# marked line, a `-f` test, a `bash -c` sourcing a library, and an `awk` reading the driver
# as TEXT.
MARKER='ws0-hermetic-allow'
for benign in \
  "out=\$(ws0_driver_run \"\$DRIVER\" --corpus /c --temp warm)" \
  "# a comment mentioning $SH \"\$DRIVER\" --corpus /c" \
  "$SH \"\$DRIVER\" --corpus /c   # $MARKER: a documented exemption" \
  "if [ -f \"\$DRIVER\" ]; then :; fi" \
  "out=\$($SH -c \"set -u; source \$ARGS_LIB; duration_reject x 1s 3\")" \
  "awk \"/^trap /\" \"\$DRIVER\" | grep -q INT" \
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
# 7 — EVERY self-test carries a minimum-check-count FLOOR, and it FIRES
# ===========================================================================
# `set -uo pipefail` (no `-e`) means a block that silently never executes lowers a suite's
# check count and registers NO failure, while the gate reads only the exit code. So each
# `test_ws0_*.sh` now ends in a `[ "$checks" -ge N ]` assert. That is only evidence if it
# fires, so it is DRIVEN: a copy of each suite with its floor raised above its real count
# must EXIT NON-ZERO. Cheap because the floor is checked at the very end — the copy runs
# its real checks once, which the gate would do anyway.
for suite in $(ws0_hermeticity_lint_subject "$TESTS_DIR"); do
  base="$(basename "$suite")"
  if ! grep -q 'MIN_CHECKS=' "$suite"; then
    fail "floor-present: $base carries no MIN_CHECKS floor — its suite-level 0/0 is open"
    continue
  fi
  pass "floor-present: $base carries a MIN_CHECKS floor"
done
# The FIRING half, on ONE suite (the cheapest of the three), because the mechanism is
# textually identical in all four and running all four twice would multiply this
# component's runtime for no new information. `test_ws0_hermeticity.sh` is excluded — it
# is the running suite.
#
# The copy is driven from a MIRROR TREE of symlinks, never from a file written into the
# repo: each suite resolves `REPO_ROOT` from its own `BASH_SOURCE`, so a copy in `$TMP`
# alone looks for `scripts/perf` beside itself and dies on the wrong thing. Writing into
# the real `scripts/tests` would work and is not available: the gate's `tree-integrity`
# check FAILS a run whose worktree mutates mid-run (#2926).
probe_suite="test_ws0_cpu_pinning_guards.sh"
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
MIN_CHECKS=24
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

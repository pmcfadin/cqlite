#!/usr/bin/env bash
# Self-test for the WS0 rig's OUTPUT-DIRECTORY EXCLUSIVITY (issue #3272; split out in round 13).
#
# Split from `scripts/tests/test_ws0_provenance_guards.sh` under the campsite rule (~1500-line
# test target; that file reached 1606 lines and round 13's F3 adds cases to it), BY SUBJECT
# rather than by size. The parent's subject is a property of the ARTIFACT SET a report reads:
#
#     A REPORT MUST IDENTIFY THE BYTES AND THE CONFIGURATION IT DESCRIBES.
#
# This file's subject is a property of the CONTAINER those artifacts live in:
#
#     ONE SESSION DIRECTORY BELONGS TO EXACTLY ONE SESSION, EXCLUSIVELY, FOR ITS WHOLE LIFE.
#
# That is a distinct subject, and the distinction is what makes the split a seam rather than a
# cut: every parent check is satisfiable by a session whose corpus, components, schema, request
# and configuration are impeccably identified and pinned — and whose rep files were assembled
# from TWO DIFFERENT RUNS that shared a directory. The reporter reads whatever rep files are
# present; a pin identifies the corpus of the session that wrote the pin, not the provenance of
# every sibling file beside it. So the identity guarantee the parent establishes is scoped to a
# directory this file's guarantee is what makes single-owner.
#
# The subject's implementation is `scripts/perf/lib-outdir.sh` (round 7's campsite-rule split out
# of the driver) — the whole output-directory lifecycle: `require_unused_out_dir`,
# `claim_out_dir`, `create_out_dir`, and their placement relative to the driver's
# `--validate-args-only` argument boundary. Four findings share the subject:
#
#   * ROUND 6's R1 — `mkdir -p "$OUT_DIR"` over a default name with only SECOND-level
#     uniqueness, so two runs started in the same second shared a directory, and an explicit
#     `--out` at a previous run's dir kept that run's rep files.
#   * ROUND 9's F4 — the used-dir refusal asked "is this directory non-empty?" with the
#     enumeration's STATUS DISCARDED, so a `find` that FAILED was indistinguishable from an empty
#     directory and took the PERMISSIVE branch.
#   * ROUND 7's F3 — R1 hardened the DEFAULT branch and left the EXPLICIT one on `mkdir -p`, so
#     the same defect survived one branch over: two concurrent runs given the same absent-or-empty
#     `--out` both passed the refusal and both created it.
#   * the BOUNDARY placement both halves rest on: the REFUSAL above `--validate-args-only` (which
#     is what makes it hermetically observable at all), CREATION below it (so the hermetic mode
#     still creates nothing).
#
# Every case carries the MEASURED pre-fix behaviour, because per #3249 a guard never observed
# firing is not evidence.
#
# Hermetic: synthetic directories under $TMPDIR, the SHIPPED `lib-outdir.sh` sourced into
# subshells, and driver invocations ONLY through `ws0_driver_run` (`--validate-args-only` +
# recording shims). No cargo, perf, sudo, taskset, corpus, network or root.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so the
# minimum-check-count floor at the end can see a block that silently never executed.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

# python3 is a HARD REQUIREMENT of this rig — ws0-baseline.sh refuses to run without it — so its
# absence is a FAILURE, not a skip. A `exit 0` here would record the gate component as SUCCESS
# with none of the checks below having run, which is the vacuous green this whole issue refuses.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig"
  echo "       (scripts/perf/ws0-baseline.sh refuses to run without it), so its"
  echo "       absence is a failed check and not a skip: exiting 0 here would record"
  echo "       this component as SUCCESS with 0 of its checks having run."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# The HERMETIC driver harness: `ws0_driver_run` prepends `--validate-args-only` and the recording
# shims, so the R1 cases that run the real driver execute nothing outside their own process.
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
ws0_hermetic_init "$TMP"

# ==========================================================================
# ROUND 6, R1 — THE OUTPUT DIR IS NEVER REUSED, AND IS CREATED ATOMICALLY
# ==========================================================================
# It used to be `mkdir -p "$OUT_DIR"` over a default name with only SECOND-level uniqueness.
# Two ways that assembles ONE report from artifacts of DIFFERENT SESSIONS, and the reporter
# cannot see either — it reads whatever rep files are present:
#
#   * two runs started in the SAME SECOND share the default dir; `mkdir -p` succeeds for both,
#     and the second run's pin overwrites the first's;
#   * an explicit `--out` at a previous run's dir keeps that run's rep files, so any rep this
#     session does not overwrite (a different temperature or arm, a higher rep index from a
#     longer previous run) is read as part of THIS one.
#
# All three cases run through `ws0_driver_run`, i.e. `--validate-args-only`: the used-dir
# REFUSAL is deliberately placed ABOVE the argument boundary (it needs no perf, no topology, no
# corpus), which is what makes it observable hermetically. CREATION stays below the boundary, so
# these cases also assert that nothing is created.
r1_dir="$TMP/r1"
mkdir -p "$r1_dir/used" && : > "$r1_dir/used/warm-rep1-scan.perf.csv"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/used"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'already exists and is NOT EMPTY' <<<"$out" \
   && ws0_driver_ran_hermetically; then
  pass "OBSERVED (round6 R1): an explicit --out holding a previous session's rep file is REFUSED (pre-fix: mkdir -p reused it and the report mixed both sessions)"
else
  fail "round6 R1: a non-empty --out must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# NON-VACUITY / the ACCEPT direction, both halves — without these the refusal could be a check
# that rejects every --out, which would make the flag unusable rather than safe.
mkdir -p "$r1_dir/empty"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/empty"); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'ARGUMENTS OK' <<<"$out"; then
  pass "OBSERVED (round6 R1): an EXISTING-BUT-EMPTY --out is ACCEPTED (the guard discriminates on CONTENT, not on existence)"
else
  fail "round6 R1: an empty --out dir must be accepted (rc=$rc, out: $(head -3 <<<"$out"))"
fi
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/absent"); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'ARGUMENTS OK' <<<"$out" && [ ! -e "$r1_dir/absent" ]; then
  pass "OBSERVED (round6 R1): an ABSENT --out is accepted AND is NOT created above the argument boundary (--validate-args-only still creates nothing)"
else
  fail "round6 R1: an absent --out must be accepted without being created (rc=$rc, exists=$([ -e "$r1_dir/absent" ] && echo yes || echo no))"
fi
# A `--out` that exists as a FILE is refused too, with its own diagnosis rather than the
# non-empty one — a reader sent to "remove that directory" for a regular file is sent wrong.
: > "$r1_dir/afile"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/afile"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'is not a directory' <<<"$out"; then
  pass "OBSERVED (round6 R1): an --out that is a regular FILE is refused with its own diagnosis"
else
  fail "round6 R1: an --out naming a file must be refused as such (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# THE DEFAULT NAME'S UNIQUENESS RESTS ON AN ATOMIC CREATE, not on the name. Asserted
# STRUCTURALLY, because the racing behaviour itself cannot be driven deterministically from a
# self-test: the default branch must use `mkdir` WITHOUT `-p` (an existing dir is then an ERROR,
# which IS the exclusion) and the name must carry more than a second-resolution timestamp.
#
# THE SUBJECT IS `lib-outdir.sh`, not the driver (#3272 round 7): the output-directory lifecycle
# moved there under the campsite rule. The `-n "$r1_block"` guard is what caught the staleness —
# after the split this awk range matched NOTHING in the driver and the check FAILED rather than
# passing vacuously, which is why that guard was there. A range test without it would have gone
# green over an empty subject, and this file would have stopped checking R1 silently.
r1_block=$(awk '/^ *while :; do/,/^ *done/' "$REPO_ROOT/scripts/perf/lib-outdir.sh")
if [ -n "$r1_block" ] \
   && grep -qF 'if mkdir "$out_dir" 2>/dev/null; then' <<<"$r1_block" \
   && ! grep -qF 'mkdir -p "$out_dir"' <<<"$r1_block" \
   && grep -qF 'out_dir="$base/$ts-$$"' <<<"$r1_block"; then
  pass "round6 R1: the DEFAULT out dir is created with \`mkdir\` (no -p) — an atomic exclusive create — and its name carries the pid, not just a UTC second"
else
  fail "round6 R1: the default out dir must be created atomically without -p and be more than second-unique (block: $(head -5 <<<"$r1_block"))"
fi
# ...and the atomic-create claim is driven, not merely grepped: the SAME primitive the driver
# uses must actually refuse a second create of one name. This is the property `mkdir -p` lacks
# and is what makes the retry loop an exclusion rather than a decoration.
r1_race="$r1_dir/race"
if mkdir "$r1_race" 2>/dev/null && ! mkdir "$r1_race" 2>/dev/null; then
  pass "OBSERVED (round6 R1): \`mkdir\` without -p REFUSES a second create of the same name (the exclusion the default branch relies on), while \`mkdir -p\` would succeed twice"
else
  fail "round6 R1: the atomic-create primitive must fail on an existing dir, else the default branch's uniqueness is not enforced"
fi
if mkdir -p "$r1_race" 2>/dev/null; then
  pass "OBSERVED (round6 R1): the CONTROL — \`mkdir -p\` on that same existing dir SUCCEEDS, which is exactly why the pre-fix code reused a session dir"
else
  fail "round6 R1: mkdir -p must succeed on an existing dir; if it does not, the control proves nothing"
fi

# ==========================================================================
# ROUND 9, F4 — A FAILED USED-DIR ENUMERATION IS NOT AN EMPTY DIRECTORY
# ==========================================================================
# `require_unused_out_dir` asked "is this directory non-empty?" as
# `[[ -n "$(find … 2>/dev/null)" ]]`: the STATUS was discarded and stderr thrown away, so a
# `find` that FAILED produced no output and was indistinguishable from an empty directory — and
# the empty result takes the PERMISSIVE branch. R1's whole refusal then silently passes over a
# directory that may still hold another session's rep files, which the reporter reads as its own.
#
# The same class as `check-root-junk-files.sh`'s process-substitution enumeration (also F4), and
# fixed the same way: enumeration to a FILE, status captured, STATUS CHECKED BEFORE EMPTINESS,
# stderr KEPT and quoted.
#
# THE TRIGGER is a directory the enumeration cannot read: mode 0300 (write+execute, NO read) with
# a prior session's rep file inside it. `find` cannot list the entries, so it exits non-zero
# having printed nothing.
f4_dir="$TMP/f4-unreadable"
mkdir -p "$f4_dir" && : > "$f4_dir/warm-rep1-scan.perf.csv"
# `root` bypasses the read bit, so the trigger cannot be constructed as root. SKIPPED with a
# stated reason rather than passed: a case that could not run must never print like one that did.
if [ "$(id -u)" -eq 0 ]; then
  echo "SKIP (round9 F4): running as root, which bypasses the read bit — the unreadable-directory trigger cannot be constructed, so this case is NOT claimed as observed"
else
  chmod 300 "$f4_dir"
  # The SHIPPED library is sourced, never re-implemented (the discipline the F3 block established).
  f4_out=$( ( # shellcheck disable=SC1090
    source "$REPO_ROOT/scripts/perf/lib-outdir.sh"
    require_unused_out_dir "$f4_dir" ) 2>&1 ); f4_rc=$?
  chmod 700 "$f4_dir"
  if [ "$f4_rc" -ne 0 ] && grep -q 'could not enumerate --out' <<<"$f4_out" \
     && grep -q 'find exited' <<<"$f4_out"; then
    pass "OBSERVED (round9 F4): an UNREADABLE --out is refused as a FAILED ENUMERATION naming find's exit status (pre-fix: find's status was discarded, no output was produced, and the empty result took the PERMISSIVE branch — R1's refusal passed over a used directory)"
  else
    fail "round9 F4: a failed enumeration must be refused with find's status, not read as an empty dir (rc=$f4_rc, out: $(head -4 <<<"$f4_out"))"
  fi
  # NON-VACUITY, and it is the whole finding: the PRE-FIX expression, run against the SAME
  # directory, reports EMPTY. Driven rather than argued — this is what "indistinguishable" means.
  f4_prefix_empty=no
  [ -z "$(chmod 300 "$f4_dir"; find "$f4_dir" -mindepth 1 -print -quit 2>/dev/null)" ] && f4_prefix_empty=yes
  chmod 700 "$f4_dir"
  if [ "$f4_prefix_empty" = yes ]; then
    pass "OBSERVED (round9 F4): THE CONTROL — the pre-fix \`[[ -n \"\$(find … 2>/dev/null)\" ]]\` reports this same used-but-unreadable dir as EMPTY, i.e. a failed look was read as 'nothing there' and the guard passed"
  else
    fail "round9 F4: the pre-fix expression must be shown to report empty here, or the fix's necessity is unproven (the trigger did not reproduce)"
  fi
fi
# THE ACCEPT DIRECTION for the rewritten enumeration — without it, F4's fix could be a function
# that refuses every directory, which would break R1's accepted empty-dir case.
f4_ok="$TMP/f4-empty"; mkdir -p "$f4_ok"
f4_out=$( ( # shellcheck disable=SC1090
  source "$REPO_ROOT/scripts/perf/lib-outdir.sh"
  require_unused_out_dir "$f4_ok" ) 2>&1 ); f4_rc=$?
if [ "$f4_rc" -eq 0 ]; then
  pass "OBSERVED (round9 F4): a READABLE, EMPTY --out is still ACCEPTED by the status-checking enumeration (R1's accepted case survives)"
else
  fail "round9 F4: an empty readable dir must still be accepted (rc=$f4_rc, out: $(head -3 <<<"$f4_out"))"
fi
# ...and a readable NON-EMPTY dir is still refused with R1's used-directory diagnosis, not with
# F4's enumeration-failure one — two different faults must stay distinguishable to a reader.
f4_used="$TMP/f4-used"; mkdir -p "$f4_used" && : > "$f4_used/warm-rep1-scan.perf.csv"
f4_out=$( ( # shellcheck disable=SC1090
  source "$REPO_ROOT/scripts/perf/lib-outdir.sh"
  require_unused_out_dir "$f4_used" ) 2>&1 ); f4_rc=$?
if [ "$f4_rc" -ne 0 ] && grep -q 'already exists and is NOT EMPTY' <<<"$f4_out" \
   && ! grep -q 'could not enumerate' <<<"$f4_out"; then
  pass "OBSERVED (round9 F4): a READABLE non-empty --out still gets R1's used-directory diagnosis, distinct from the enumeration-failure one"
else
  fail "round9 F4: a readable non-empty dir must keep R1's diagnosis (rc=$f4_rc, out: $(head -3 <<<"$f4_out"))"
fi

# ==========================================================================
# ROUND 7, F3 — AN EXPLICIT --out IS CLAIMED ATOMICALLY, NOT JUST CREATED
# ==========================================================================
# R1 fixed the DEFAULT path (atomic `mkdir` on a unique name) and left the EXPLICIT path on
# `mkdir -p`, so the defect R1 was about survived one branch over: two concurrent runs given the
# same ABSENT-OR-EMPTY `--out` BOTH pass the used-directory refusal (empty for both, and both
# check before either writes) and BOTH `mkdir -p` succeed. Each then writes its session pin and
# rep artifacts over the other's, and the reporter assembles a median across MIXED SESSIONS —
# it reads whatever rep files are present and cannot tell.
#
# `mkdir` on `$OUT_DIR` itself is not available as the arbiter, because R1 deliberately ACCEPTS
# an existing-but-empty dir. So the exclusion is one level down: an atomic marker SUBDIRECTORY.
#
# The SHIPPED library is SOURCED, never re-implemented and never re-extracted from text — a
# reimplemented check in a test is a second thing to keep in sync, and its divergence would be
# invisible in exactly the permissive direction. `scripts/perf/lib-outdir.sh` owns the whole
# output-directory lifecycle (round 7's campsite-rule split; the driver was at 1035 lines).
f3_dir="$TMP/f3"; mkdir -p "$f3_dir"
OUTDIR_LIB="$REPO_ROOT/scripts/perf/lib-outdir.sh"
if [ -f "$OUTDIR_LIB" ] && grep -qF 'mkdir "$claim"' "$OUTDIR_LIB" \
   && ! grep -qF 'mkdir -p "$claim"' "$OUTDIR_LIB"; then
  pass "OBSERVED (round7 F3): lib-outdir.sh's exclusion is \`mkdir\` on the MARKER, with NO -p (an existing marker is an ERROR, which IS the exclusion)"
else
  fail "round7 F3: lib-outdir.sh must claim with mkdir (no -p) on the marker"
fi
f3_run() { # f3_run <dir> — run the SHIPPED claim_out_dir against <dir>; prints output, returns rc
  ( # shellcheck disable=SC1090
    source "$OUTDIR_LIB"
    claim_out_dir "$1" "self-test" ) 2>&1
}
mkdir -p "$f3_dir/shared"
out=$(f3_run "$f3_dir/shared"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "OBSERVED (round7 F3): the FIRST session claims an existing-but-EMPTY --out successfully (R1's accepted case still works)"
else
  fail "round7 F3: the first claim on an empty dir must succeed (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# THE RACE, which is the finding: a SECOND session handed the same dir must be REFUSED. Both
# runs saw it empty, so nothing above this line could separate them.
out=$(f3_run "$f3_dir/shared"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'ALREADY CLAIMED' <<<"$out"; then
  pass "OBSERVED (round7 F3): a SECOND session on the SAME --out is REFUSED as ALREADY CLAIMED (pre-fix: both mkdir -p succeeded and the report mixed two sessions)"
else
  fail "round7 F3: a second claim must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the refusal must NAME THE OWNER, or an operator cannot tell a live peer from a stale
# marker — the difference between waiting and picking a new directory.
if grep -q 'Claimed by: pid=' <<<"$out"; then
  pass "OBSERVED (round7 F3): the refusal NAMES the owning pid/host/start time (a live peer is distinguishable from a stale marker)"
else
  fail "round7 F3: the refusal must name the claim owner (out: $(head -4 <<<"$out"))"
fi
# NON-VACUITY / THE CONTROL: the pre-fix explicit path really did admit both. `mkdir -p` twice on
# the same dir succeeds twice, which is the whole finding — asserted rather than reasoned about.
mkdir -p "$f3_dir/prefix-control"
if mkdir -p "$f3_dir/prefix-control" 2>/dev/null && mkdir -p "$f3_dir/prefix-control" 2>/dev/null; then
  pass "OBSERVED (round7 F3): NON-VACUITY — the pre-fix \`mkdir -p\` on an explicit --out SUCCEEDS REPEATEDLY, so two concurrent sessions both proceeded (this is F3)"
else
  fail "round7 F3: mkdir -p must succeed repeatedly, else the finding's premise is wrong"
fi
# A DIFFERENT dir is unaffected — a claim that refused everything would satisfy the case above
# and make `--out` unusable.
mkdir -p "$f3_dir/other"
out=$(f3_run "$f3_dir/other"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "OBSERVED (round7 F3): a DIFFERENT --out still claims fine (the guard discriminates on the DIRECTORY, not unconditionally)"
else
  fail "round7 F3: an unclaimed dir must be claimable (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# BOTH branches go through the SAME mechanism, so a future edit cannot re-split them (which is
# exactly what F3 was: R1 hardened one branch and left the other). Driven for the EXPLICIT branch
# above; structural for the DEFAULT branch, whose race is not deterministically drivable from a
# self-test — asserted as "both code paths in `create_out_dir` claim".
f3_calls=$(grep -cF 'claim_out_dir "$out_dir"' "$OUTDIR_LIB")
if [ "$f3_calls" -eq 2 ]; then
  pass "round7 F3: BOTH branches of create_out_dir claim (one mechanism, $f3_calls call sites — R1 hardened only one branch, which IS F3)"
else
  fail "round7 F3: both --out branches must claim (found $f3_calls call sites in lib-outdir.sh, expected 2)"
fi
# ...and the DEFAULT branch is DRIVEN too, for the property that IS drivable: it creates a fresh
# unique dir and claims it, twice in a row, without collision — so the claim added to that branch
# has not broken the retry loop R1 built.
f3_base="$f3_dir/defaults"
f3_default() {
  ( # shellcheck disable=SC1090
    source "$OUTDIR_LIB"
    create_out_dir "" "$1" ) 2>&1
}
d1=$(f3_default "$f3_base"); rc1=$?
d2=$(f3_default "$f3_base"); rc2=$?
if [ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ] && [ -d "$d1" ] && [ -d "$d2" ] && [ "$d1" != "$d2" ]; then
  pass "OBSERVED (round7 F3): the DEFAULT branch creates and claims two DISTINCT dirs back-to-back (the claim did not break R1's retry loop)"
else
  fail "round7 F3: two default-branch runs must yield two distinct claimed dirs (rc=$rc1/$rc2 d1=$d1 d2=$d2)"
fi
# ...and CREATION/CLAIMING stays BELOW the argument boundary, so `--validate-args-only` still
# creates nothing. Asserted by LINE ORDER against the boundary's own exit, because the
# behavioural half (an absent --out is not created) is already driven above and this is the
# property that keeps it true.
f3_boundary=$(grep -nF 'ARGUMENTS OK (--validate-args-only)' "$DRIVER" | head -1 | cut -d: -f1)
f3_create=$(grep -nF 'OUT_DIR="$(create_out_dir' "$DRIVER" | head -1 | cut -d: -f1)
if [ -n "$f3_boundary" ] && [ -n "$f3_create" ] && [ "$f3_boundary" -lt "$f3_create" ]; then
  pass "round7 F3: create_out_dir (line $f3_create) is BELOW the --validate-args-only boundary (line $f3_boundary) — the hermetic mode still creates nothing"
else
  fail "round7 F3: creation must stay below the argument boundary (boundary=$f3_boundary create=$f3_create)"
fi
# ...and the REFUSAL stays ABOVE it, which is what makes the used-dir case observable hermetically
# at all. The two halves on opposite sides of the boundary is the design, so both are pinned.
f3_refusal=$(grep -nF 'require_unused_out_dir "${OUT_DIR:-}"' "$DRIVER" | head -1 | cut -d: -f1)
if [ -n "$f3_refusal" ] && [ -n "$f3_boundary" ] && [ "$f3_refusal" -lt "$f3_boundary" ]; then
  pass "round7 F3: require_unused_out_dir (line $f3_refusal) is ABOVE the boundary (line $f3_boundary) — which is why the used-dir refusal is hermetically observable"
else
  fail "round7 F3: the used-dir refusal must stay above the argument boundary (refusal=$f3_refusal boundary=$f3_boundary)"
fi
# The driver checks create_out_dir's STATUS EXPLICITLY. It runs in a COMMAND SUBSTITUTION (it must
# echo the default name it chose), so its `exit 2` kills only that subshell and the driver survives
# on `set -e` alone. That works — and a fail-closed refusal enforced only by an implicit shell
# option is one `set +e` from being decorative, which is this issue's whole subject.
if grep -qF 'OUT_DIR="$(create_out_dir "${OUT_DIR:-}" "$REPO_ROOT/target/perf-ws0-3096")" || exit 2' "$DRIVER"; then
  pass "round7 F3: the driver checks create_out_dir's status EXPLICITLY (\`|| exit 2\`), not via set -e alone"
else
  fail "round7 F3: the create_out_dir call must check its status explicitly — its exit 2 only kills the command substitution"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a `$(...)`
# whose command vanished, a `for` over an empty list — LOWERS the check count and registers NO
# failure. The gate reads only the exit code, so a suite that ran 3 of its checks and passed them
# exits 0 and reports SUCCESS. That is the suite-level `0/0` shape this whole issue is about, one
# level up from the checks themselves.
#
# The floor is DERIVED from the OBSERVED count — 22 at the split, measured by instrumenting
# `pass`/`fail` to report their call site and counting the calls that landed in the extracted
# range — set just below it so adding a case does not red the suite, and far above zero.
# One case (round 9's F4 unreadable-directory trigger) SKIPS as root, which is why the floor is
# 20 rather than 21: root bypasses the read bit, so that case's two checks cannot run there.
MIN_CHECKS=20
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 output-dir exclusivity guards: all $checks checks passed"
  exit 0
fi
echo "ws0 output-dir exclusivity guards: $fails of $checks check(s) FAILED"
exit 1

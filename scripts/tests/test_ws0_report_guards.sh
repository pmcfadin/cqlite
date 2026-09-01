#!/usr/bin/env bash
# Self-test for the WS0 measurement rig's fail-closed guards
# (scripts/perf/ws0-baseline.sh + scripts/perf/ws0_report.py + ws0_validate.py).
#
# These are INSTRUMENT guards. A broken one does not crash anything — it publishes
# a number that is not the number it claims to be, which is the most expensive
# failure mode a measurement rig has. Every property below was a real defect found
# in review (#3096 findings 1-2; #3272 findings 1-7):
#
#   1. WARM MEANS PREWARMED (#3096 finding 1). The Flight arm prewarmed before its
#      perf window; the bare-scan arm did not, so on a cold page cache the first
#      "warm" bare-scan rep was measured partly cold — and the bare scan is the
#      DENOMINATOR of the 1.3x ratio, where reading slow makes the target easier.
#      The driver now runs an untimed prewarm and records `prewarm_status`; the
#      reporter must carry it into results.json for BOTH arms and flag a degraded
#      or unrecorded one in the summary.
#   2. A COLD REP IS EXACTLY ONE REQUEST (#3096 finding 2). The reporter accepted
#      ANY successful-request count for a cold rep, so if the corpus finished inside
#      --cold-step-duration, requests 2..N were warm and got blended into the figure
#      reported as "cold" — and a caller could trigger it directly by raising that
#      option. The reporter must now REJECT such a rep naming the observed count,
#      and the driver must refuse an over-long cold step up front.
#
# And the #3272 round, THREE of which are the guards above being bypassable or
# fail-open — "a fix moved the problem", which is why each case below carries a
# NON-VACUITY note recording what the PRE-FIX code accepted:
#
#   3. AN ABSENT CORPUS IDENTITY MUST NOT SKIP THE FULL-CORPUS CHECK (#3272 f1).
#      `corpus_rows=None` disabled the `rows == requests_ok x corpus_rows` assert
#      while the NOTES kept claiming it ran.
#   4. THE COLD PREWARM SENTINEL IS COLD-ONLY (#3272 f2). `skipped-cold-arm` counted
#      as a healthy prewarm at ANY temperature, so an unprewarmed WARM rep reached
#      `prewarm_all_ok=true` — the guard of finding 1 satisfied by its own sentinel.
#   5. A COUNTER THAT WAS NOT OBSERVED IS AN ERROR (#3272 f4). `.get("cycles", 0)`
#      fabricated a zero, so a run reported "SETUP-SUBTRACTED" with no subtraction.
#   6. --reps AND FRIENDS ARE VALIDATED (#3272 f5). `--reps 0` produced a vacuous
#      but SUCCESSFUL report.
#   7. COMPLETENESS IS JUDGED AGAINST THE SELECTION (#3272 f6), and the selection is
#      stated in the report so a narrow run cannot be mistaken for a full matrix.
#   8. DURATIONS PARSE AS DECIMAL (#3272 f7). `010s` was octal 8s and `08s` was a
#      hard bash error — a silently wrong measurement window.
#
# And REVIEW ROUND 1 of #3272, whose findings on this file's own subject matter are:
#
#   9. A PARTIAL SYSCTL RESTORE MUST WARN (B3). The success/warning split keyed on
#      "was ANYTHING restored", so a partial restore printed the affirmative line and
#      NO warning — finding 3's own defect in narrower form. Both halves are now
#      per-knob, and the ROOT CAUSE is closed too: a knob whose prior could not be
#      captured is never mutated.
#  10. THE DRIVER'S BOUNDS AGREE WITH THE REPORTER'S. `--reps 200000` passed the
#      driver's 9-digit check and was refused only by the report, after 200,000
#      full-corpus reps. Refusing a value after acting on it is not refusing it.
#  11. A TOO-LONG DURATION IS REPORTED AS A RANGE PROBLEM, not a format one — the
#      digit cap reintroduced the same misleading complaint this file criticizes for
#      `08s`.
#  12. THE ACCEPT-DIRECTION CASES ASSERT AFFIRMATIVELY. Several asserted only the
#      ABSENCE of a bad substring, which passes on ANY unrelated failure — measured:
#      with a corpus present but `perf` absent, the driver exits at "perf is not
#      installed" and every such case "passed" with argument validation never
#      exercised. They now assert the expected DOWNSTREAM diagnostic, through
#      `expect_driver_accepts`, whose closed grammar FAILS on an unrecognized outcome
#      and which carries its own non-vacuity probe.
#  13. A PROBE'S rc MUST BE READ OFF THE THING UNDER TEST. `echo "RC=$?"` after an
#      intervening `case` measured the CASE's status (0 for most branches), so the
#      "cleanup cannot fail the run" half of the failing-sudo case was unmeasured.
#
# And #3248, whose two properties are the same shape one level in — a claim published with
# evidence that does not support it:
#
#  15. A QUIESCENCE VERDICT MUST COVER *THIS* SESSION'S WINDOW, and carry its caveats. The
#      reporter checked only that the verdict named the same TIMESERIES as the manifest, but
#      `box-load.jsonl` is ONE file spanning every session on the box — so a clean verdict from
#      an ADJACENT window of it certified this session. And `samples`,
#      `coverage_largest_gap_s`, `narrow_census_records` and `census_breadth` were read with
#      `.get()`, so a verdict missing them published QUIESCENT with `null` caveats, which reads
#      as an absent concern rather than an unmeasured one.
#
# And REVIEW ROUND 2, whose finding is a REGRESSION INTRODUCED BY 12 ABOVE:
#
#  14. THE ACCEPT DIRECTION MUST EXECUTE NOTHING. Round 1's `expect_driver_accepts`
#      ran the REAL driver with no early exit and accepted "it failed at some later
#      checkpoint" as proof the arguments were fine — so on LINUX the accept cases ran
#      past validation into `relax_perf_sysctls` (a host `sudo -n sysctl -w`) and
#      `cargo build --release`, six times over. MEASURED against that code on a
#      Linux-shaped host (readable sysctl priors, stubbed topology/port checks, recording
#      PATH shims): `sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0`,
#      then `cargo build --release -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen`.
#      It was invisible on macOS only because the run stops earlier at `perf is not
#      installed`. The driver now offers `--validate-args-only`, which STOPS at the
#      argument boundary, and the hermeticity is PROVED by recording PATH shims rather
#      than assumed from the host lacking a tool.
#
# Hermetic ON EVERY PLATFORM, Linux included: synthetic result dirs + synthetic perf
# CSVs, and recording `sudo`/`cargo`/`perf`/`taskset` shims that FAIL the suite if any
# accept case invokes them. No corpus, no network, and the real perf artifacts are never
# touched.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"
# The host-state subsystem the driver sources: the sysctl capture/mutate/restore that
# is the only part of the rig changing anything outside its own process tree. Driven
# HERE as the shipped implementation, sourced rather than re-extracted, so a divergence
# between what is tested and what runs cannot exist.
HOST_STATE="$REPO_ROOT/scripts/perf/lib-host-state.sh"
# The argument-validation helpers the driver sources (`require_positive_int`,
# `parse_duration_ms`, `duration_reject`). The driver keeps the CALL SITES, so the
# behavioural cases still run the driver end to end; the structural checks below read
# the implementation from here.
ARGS_LIB="$REPO_ROOT/scripts/perf/lib-args.sh"
# THE ONE SANCTIONED WAY THIS FILE MAY INVOKE THE DRIVER (#3272 review round 3, B1).
# `ws0_driver_run` prepends `--validate-args-only` AND the recording PATH shims, so no
# case can reach `relax_perf_sysctls` (a host `sudo -n sysctl -w`), `cargo build
# --release` or the measurement loop. `scripts/tests/test_ws0_hermeticity.sh` runs the
# STRUCTURAL lint from the same library over every `test_ws0_*.sh`, so a bare invocation
# added later FAILS rather than being caught by a manual sweep — which missed one twice.
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so
# the minimum-check-count floor at the end can see a block that silently never executed
# (#3272 review round 3 nit).
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }
# out_has <text> <grep-args...>: a SIGPIPE-SAFE text predicate — an instance of issue #3862, which
# this file's `awk … | grep -q` sites hit for real. Under `set -o pipefail` (line 97 above),
# `<producer> | grep -q PAT` can return **141 WITH THE MATCH PRESENT**: grep -q exits at the first
# match and CLOSES the pipe, so the producer's next write dies and pipefail promotes that to the
# pipeline's status. It is therefore a RACE at any payload larger than one stdio flush (~4 KiB), not
# a size threshold — which is why it is load-sensitive and why the same suite passes on an idle box.
#
# MEASURED on this branch: the pair at the #3096 banner check extracts **11,592 bytes** of the
# driver and is the site that FAILED a 51-minute gate of record (`the driver must print its
# selection as PARTIAL/FULL up front`), while `scripts/perf/ws0-baseline.sh` demonstrably contains
# both `PARTIAL MATRIX` and `FULL MATRIX`. The other three extractions measure 590 and 780 bytes —
# one stdio flush each, so exactly one write occurs and it necessarily precedes any match. They are
# converted anyway: conversion is free, whereas a bound that has to be re-measured every time
# `scripts/perf/*.sh` grows is not. A here-string is not a pipeline, so grep's own status is the
# answer.
out_has() { local __t="$1"; shift; grep -q "$@" <<< "$__t"; }

[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
[ -f "$HOST_STATE" ] || { echo "FAIL - missing $HOST_STATE"; exit 1; }
[ -f "$ARGS_LIB" ] || { echo "FAIL - missing $ARGS_LIB"; exit 1; }
# python3 absence is a FAILURE, not a skip (#3272 review B8). The old branch printed
# `SKIP - … (never a silent PASS)` and then `exit 0` — which IS a silent pass: the
# gate's `tooling-tests` component records SUCCESS with none of the ~65 checks below
# having run, and the reassuring word "SKIP" is on stdout the gate does not read.
# python3 is a HARD REQUIREMENT of this rig (ws0-baseline.sh refuses to run without
# it, and the reporter IS a python3 program), so there is no environment where its
# absence means "this check is not applicable here".
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig:"
  echo "       scripts/perf/ws0_report.py IS a python3 program and ws0-baseline.sh"
  echo "       refuses to run without it. So this is a failed check, not a skip —"
  echo "       exiting 0 here would record the gate component as SUCCESS with 0 of"
  echo "       its checks having run (#3272 review B8)."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

CORPUS_ROWS=1000
# A syntactically valid corpus digest (64 lowercase hex). The reporter requires the
# real shape because the digest is what identifies the bytes that were measured; a
# placeholder like `deadbeef` cannot.
FAKE_SHA="$(printf 'ab%.0s' $(seq 1 32))"

# --------------------------------------------------------------------------
# Fixture builders — the minimum a session dir needs for the reporter to run
# --------------------------------------------------------------------------
# `perf_csv`, `make_corpus` and `make_round` are SHARED with
# `test_ws0_fabrication_guards.sh` (scripts/tests/lib-ws0-fixtures.sh): they were identical
# in both files, and `make_round` gained a `monotonic_ns` field this round which had to be
# edited in two places — exactly the drift a shared builder removes. The `make_*_rep`
# builders below stay HERE because their signatures are specific to this file's subject (a
# request COUNT and a row total, for the per-temperature request contract).
# shellcheck source=scripts/tests/lib-ws0-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-fixtures.sh"
# ...and the BARE-SCAN-ARM builders this file shares with `test_ws0_cell_volume_guards.sh`
# (`make_corpus`, `make_scan_rep`, `make_flight_rep`, `run_report`). They moved to ONE
# definition when the round-17 cell block was split out under the campsite rule: both suites
# feed the reporter the same healthy scan/flight reps and the same corpus shape, and a builder
# copied into two suites is a builder that will disagree with itself — `lib-ws0-fixtures.sh`'s
# own header records paying for exactly that (its `make_round` gained a `monotonic_ns` field in
# one round and had to be edited in two places). `run_report_full` and the `expect_*` helpers
# stay HERE: their subject is this file's session CONFIGURATION, which the cell suite never
# perturbs.
# shellcheck source=scripts/tests/lib-ws0-scan-arm-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-scan-arm-fixtures.sh"

# run_report_full <dir> <corpus> <temps> <arms> <reps> <scan-passes> — same, with
# every quantity a caller can get wrong exposed.
run_report_full() {
  # The manifest is stamped UNCONDITIONALLY, with THIS call's configuration (#3272 F1).
  #
  # Not `[ -e ] ||`: the configuration is now the SUBJECT of ~10 cases, several of which share
  # one session dir, so preserving a pre-existing manifest made every later case report the
  # FIRST one's configuration. Measured: the empty-temps, unknown-temps and repeated-temps cases
  # all failed with `--reps is absurdly large`, inherited from a neighbour — each "passed or
  # failed" on a value it had not set, which is the wrong-subject shape this suite exists to
  # refuse. Cases whose subject is a MISSING manifest remove it explicitly after this call.
  rm -f "$1/session-corpus-pin.json"
  ws0_pin_session_corpus "$1" "$2" "$5" "$3" "$4" "$6"
  python3 "$REPORT" --dir "$1" --corpus "$2" 2>&1
}

# expect_report_reject <label> <expect-substring> <report-args...> — the reporter
# must exit NON-ZERO and say <expect-substring>. Non-zero alone is not enough: a
# guard that fires with a diagnostic naming something else has not been observed.
expect_report_reject() {
  local label="$1" expect="$2"; shift 2
  local out rc3
  out=$(run_report_full "$@"); rc3=$?
  if [ "$rc3" -ne 0 ] && grep -q "$expect" <<<"$out"; then
    pass "$label"
  else
    fail "$label: expected non-zero + '$expect' (rc=$rc3, out: $out)"
  fi
}

# A COMPLETE single-warm-rep session dir, the happy path every negative case below
# perturbs by exactly one field. Built fresh each time so no case can pass on a
# neighbour's leftovers.
make_warm_session() { # make_warm_session <dir>
  mkdir -p "$1"
  make_scan_rep "$1" warm 1 ok
  make_flight_rep "$1" warm 1 1 "$CORPUS_ROWS" ok
}

# --------------------------------------------------------------------------
# Finding 2 — a COLD flight rep with >1 successful request is REJECTED
# --------------------------------------------------------------------------
# The blend this rejects is real and silent: 3 requests over a cold cache is one
# cold scan plus two warm ones, and the pre-fix reporter published their average as
# the "cold" figure with nothing in the output to say so.
d="$TMP/cold-multi"; make_corpus "$TMP/corpus"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 3 $((CORPUS_ROWS * 3)) skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
if [ "$rc" -ne 0 ] && grep -q "COLD rep flight-bypass-cold-1 completed 3 successful requests" <<<"$out"; then
  pass "a cold flight rep with 3 successful requests is REFUSED, naming the count"
else
  fail "cold multi-request rep: expected non-zero + observed-count message (rc=$rc, out: $out)"
fi
if grep -q "requests_ok" <<<"$out" || grep -qi "expected exactly 1" <<<"$out"; then
  pass "the refusal states the expected count"
else
  fail "the refusal must state the expected count (out: $out)"
fi

# The same shape as a WARM rep is legitimate: 3 requests, 3x the corpus rows.
d="$TMP/warm-multi"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 3 $((CORPUS_ROWS * 3)) ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "the same 3-request rep is ACCEPTED as WARM (the guard is temperature-scoped, not blanket)"
else
  fail "warm 3-request rep must be accepted (rc=$rc, out: $out)"
fi

# A cold rep with exactly one full-corpus request is accepted.
d="$TMP/cold-one"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 1 "$CORPUS_ROWS" skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "a cold rep with exactly 1 full-corpus request is accepted"
else
  fail "cold single-request rep must be accepted (rc=$rc, out: $out)"
fi

# A request that did not scan the WHOLE corpus is rejected: the per-request row
# denominator would not be the one the report prints.
d="$TMP/partial"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 1 $((CORPUS_ROWS - 7)) skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
if [ "$rc" -ne 0 ] && grep -q "did not scan the whole corpus" <<<"$out"; then
  pass "a partial-corpus request is REFUSED (row denominator would be unstated)"
else
  fail "partial-corpus rep: expected non-zero + 'did not scan the whole corpus' (rc=$rc, out: $out)"
fi

# --------------------------------------------------------------------------
# Finding 2 — the driver refuses an over-long --cold-step-duration up front
# --------------------------------------------------------------------------
# Exits at argument validation, before any build/corpus/cache-drop, so this runs
# with no corpus and no sudo.
#
# HERMETIC LIKE THE ACCEPT DIRECTION (#3272 review round 3, B1). Every refusal this
# helper asserts fires ABOVE the argument-validation boundary — the numeric checks, the
# duration parser, the cold ceiling, the scan-passes interaction — so routing through
# `ws0_driver_run` (which prepends `--validate-args-only` and the recording shims)
# exercises exactly the same code and CANNOT reach the world below it. Round 2 hardened
# only the accept direction and left every reject call site bare, on the reasoning that a
# rejection exits early anyway: true for the rejection it asserts, and NOT true for the
# accept-adjacent probes beside it, which is how :497 survived. The distinction is not
# worth keeping — one path, mechanically enforced.
check_driver_reject() { # check_driver_reject <label> <expect-substring> <args...>
  local label="$1" expect="$2"; shift 2
  local out rc2 calls
  out=$(ws0_driver_run "$DRIVER" "$@"); rc2=$?
  calls="$(ws0_hermetic_calls)"
  if [ -n "$calls" ]; then
    fail "$label: the REJECT path invoked something outside this process — $calls"
    return
  fi
  if [ "$rc2" -ne 0 ] && grep -q "$expect" <<<"$out"; then
    pass "$label"
  else
    fail "$label: expected non-zero + '$expect' (rc=$rc2, out: $out)"
  fi
}
# --------------------------------------------------------------------------
# HERMETICITY SHIMS — the accept direction may execute NOTHING (#3272 R1)
# --------------------------------------------------------------------------
# `sudo`, `cargo`, `perf` and `taskset` are shimmed to RECORD any invocation and exit
# non-zero. They are prepended to PATH for every accept case, and the recording file
# must stay EMPTY: that is the hermeticity contract, asserted rather than assumed.
#
# It needed asserting because it was BROKEN. Round 1's `expect_driver_accepts` ran the
# REAL driver with no early exit and accepted "it failed at some later checkpoint" as
# proof the arguments were fine — and its grammar admitted `release build failed` and
# `not readable, so its prior value`, i.e. it EXPECTED to get past validation into
# `relax_perf_sysctls` (a host `sudo -n sysctl -w`) and `cargo build --release`. Six call
# sites, each a full release build, in a suite whose header says "No cargo, no perf, no
# sudo". It never showed up because macOS stops earlier at `perf is not installed`; on
# LINUX — where the gate's `tooling-tests` component runs this file — it mutated the host
# and built the workspace. A suite whose hermeticity depends on the host LACKING a tool is
# not hermetic; it is untested on the platform that matters.
# The shim construction lives in `lib-ws0-hermetic.sh` so all three self-tests share ONE
# implementation and the structural lint has one call shape to recognise (#3272 round 3).
ws0_hermetic_init "$TMP"
SHIM_BIN="$WS0_SHIM_BIN"
HERMETIC_CALLS="$WS0_HERMETIC_CALLS"

# expect_driver_accepts <label> <args…> — the ACCEPT direction, asserted
# AFFIRMATIVELY and HERMETICALLY (#3272 review R1).
#
# Two positive measurements, neither of them an absence:
#
#  (a) the driver reached its ARGUMENT-VALIDATION BOUNDARY and SAID SO — `ARGUMENTS OK`
#      on stdout with rc=0, via `--validate-args-only`. Round 1's version instead
#      asserted the run reached one of ten LATER checkpoints, which made "the release
#      build failed" a passing acceptance signal.
#  (b) it executed NOTHING: no `sudo`, no `cargo`, no `perf`, no `taskset` — proved by
#      the recording shims above, whose file must be empty for this case.
expect_driver_accepts() {
  local label="$1"; shift
  local out rc calls
  out=$(ws0_driver_run "$DRIVER" "$@"); rc=$?
  calls="$(ws0_hermetic_calls)"
  if [ "$rc" -ne 0 ] || ! grep -q "ARGUMENTS OK" <<<"$out"; then
    fail "$label: the driver did not reach its argument-validation boundary, so acceptance is UNMEASURED (rc=$rc, out: $out)"
    return
  fi
  if [ -n "$calls" ]; then
    fail "$label: the accept path INVOKED something outside this process — the suite is not hermetic: $calls"
    return
  fi
  pass "$label"
}

# NON-VACUITY for the helper ITSELF, before it is trusted, in THREE directions: it must
# FAIL on a genuinely refused argument, PASS on an accepted one, and FAIL when the run
# invokes a shimmed tool. `pass`/`fail` are shimmed so the helper's own verdict is
# observed rather than assumed.
_probe_helper() { # _probe_helper <args…> — echoes PASS or FAIL
  ( pass() { echo PASS; }; fail() { echo FAIL; }
    expect_driver_accepts probe "$@" 2>/dev/null | head -1 )
}
if [ "$(_probe_helper --corpus "$TMP/corpus" --reps 0)" = "FAIL" ]; then
  pass "expect_driver_accepts FAILS on a genuinely refused argument (it is not vacuous)"
else
  fail "expect_driver_accepts must fail on a refused argument, else every accept case is vacuous"
fi
if [ "$(_probe_helper --corpus "$TMP/corpus" --reps 3)" = "PASS" ]; then
  pass "expect_driver_accepts PASSES on an accepted argument (the positive control)"
else
  fail "expect_driver_accepts must pass on an accepted argument"
fi
# The HERMETICITY half of the helper, driven against a stand-in that stamps the accept
# marker AND runs `sudo`: the helper must still FAIL, on the invocation alone. Without
# this the empty-file check could be satisfied by shims that are never reached — the
# `0/0` shape, one level down.
if [ "$(
  ( pass() { echo PASS; }; fail() { echo FAIL; }
    DRIVER="$TMP/leaky-driver.sh"
    { echo 'sudo -n sysctl -w kernel.perf_event_paranoid=-1 >/dev/null 2>&1'
      echo 'echo "ARGUMENTS OK (stand-in)"'; } > "$DRIVER"
    expect_driver_accepts probe --corpus "$TMP/corpus" 2>/dev/null | head -1 )
)" = "FAIL" ]; then
  pass "expect_driver_accepts FAILS when the run invokes sudo (the hermeticity half FIRES)"
else
  fail "expect_driver_accepts must fail on any sudo/cargo/perf invocation, else the shims are decorative"
fi
# And the shims must be REACHABLE and RECORDING at all — otherwise every hermeticity
# assertion above rests on an oracle that cannot answer (#3272: a positive verdict
# requires an affirmative measurement).
: > "$HERMETIC_CALLS"
PATH="$SHIM_BIN:$PATH" sudo -n true >/dev/null 2>&1
PATH="$SHIM_BIN:$PATH" cargo build >/dev/null 2>&1
if grep -q '^sudo ' "$HERMETIC_CALLS" && grep -q '^cargo ' "$HERMETIC_CALLS"; then
  pass "the hermeticity shims are on PATH and RECORD (the oracle can answer)"
else
  fail "the hermeticity shims must record invocations, else the empty-file check is vacuous"
fi
: > "$HERMETIC_CALLS"
# The driver must actually OFFER the boundary. A `--validate-args-only` that fell through
# to the unrecognized-argument branch would make every accept case fail loudly; one that
# was PARSED AND IGNORED would make them all pass while executing the world. So the
# flag's own effect is asserted: even a corpus that does not exist is never stat'ed.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent-corpus); rc=$?
if [ "$rc" -eq 0 ] && grep -q "ARGUMENTS OK" <<<"$out" \
   && grep -q "nothing was executed" <<<"$out" \
   && ! grep -q "holds no" <<<"$out" && ws0_driver_ran_hermetically; then
  pass "--validate-args-only stops AT the argument boundary (no corpus stat, nothing executed)"
else
  fail "--validate-args-only must exit 0 at the boundary without touching the world (rc=$rc, out: $out, calls: $(ws0_hermetic_calls))"
fi
# ...and it must still REFUSE a bad argument: a validate-only mode that accepted
# everything would turn every accept case into a tautology.
out=$(ws0_driver_run "$DRIVER" --corpus "$TMP/corpus" --reps 0); rc=$?
if [ "$rc" -ne 0 ] && grep -q "must be at least 1" <<<"$out"; then
  pass "--validate-args-only still REFUSES an invalid argument (it validates, it does not wave through)"
else
  fail "--validate-args-only must refuse --reps 0 (rc=$rc, out: $out)"
fi

# `lib-args.sh` must be SELF-CONTAINED under `set -u` (#3272 review round 2 nit).
# NON-VACUITY, measured against HEAD~1: `duration_reject` interpolated
# `$COLD_STEP_MAX_MS`, defined only in the DRIVER, so sourcing the library alone and
# calling it died with `COLD_STEP_MAX_MS: unbound variable` — no diagnostic, and the
# `exit 2` on the next line never ran. A library that dies rather than diagnoses is worse
# than one that says nothing, because the failure names the wrong thing.
out=$(bash -c '
  set -uo pipefail
  # shellcheck disable=SC1090
  source "'"$ARGS_LIB"'"
  duration_reject cold-step-duration 99999999999999999999s 3
' 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "is too LONG" <<<"$out" \
   && grep -q "5000ms cold-step ceiling" <<<"$out" \
   && ! grep -q "unbound variable" <<<"$out"; then
  pass "lib-args.sh is SELF-CONTAINED: duration_reject diagnoses and exits 2 with NO driver sourced"
else
  fail "duration_reject must work with only lib-args.sh sourced (rc=$rc, out: $out)"
fi
# ...and the ceiling must be a real NUMBER, not an empty default that would print
# "the ms cold-step ceiling" and compare against nothing in the driver.
if bash -c '
  set -uo pipefail
  # shellcheck disable=SC1090
  source "'"$ARGS_LIB"'"
  [ "$COLD_STEP_MAX_MS" -gt 0 ] 2>/dev/null
'; then
  pass "COLD_STEP_MAX_MS is a positive integer in the library that quotes it"
else
  fail "COLD_STEP_MAX_MS must be a positive integer owned by lib-args.sh"
fi
# And the DRIVER must READ it rather than define its own — two definitions would drift, and
# the diagnostic would then name a ceiling other than the one enforced.
if ! grep -qE '^COLD_STEP_MAX_MS=' "$DRIVER"; then
  pass "the driver does NOT redefine COLD_STEP_MAX_MS (one owner, no drift)"
else
  fail "COLD_STEP_MAX_MS must have ONE owner (lib-args.sh); the driver redefines it"
fi
# The cap-drift assert's OWN reference must be right: lib-args.sh names the file that
# contains it. A pointer to the wrong file reads as coverage that does not exist.
if grep -q 'test_ws0_report_guards.sh' "$ARGS_LIB" \
   && ! grep -q 'pinned equal by' <<<"$(grep -A2 'test_ws0_fabrication_guards.sh' "$ARGS_LIB")"; then
  pass "lib-args.sh points the cap-drift assert at the file that actually contains it"
else
  fail "lib-args.sh must reference test_ws0_report_guards.sh for the cap-drift assert"
fi

check_driver_reject "a 45s cold step is refused up front (would admit warm requests)" \
  "exceeds the" --corpus "$TMP/corpus" --temp cold --cold-step-duration 45s
check_driver_reject "a 10s cold step is refused (above the 5000ms ceiling)" \
  "exceeds the" --corpus "$TMP/corpus" --temp both --cold-step-duration 10s
check_driver_reject "a bare number is refused rather than guessed as s-or-ms" \
  "must be <n>ms, <n>s or <n>m" --corpus "$TMP/corpus" --temp cold --cold-step-duration 45
check_driver_reject "a zero-length step is refused" \
  "greater than zero" --corpus "$TMP/corpus" --temp cold --cold-step-duration 0s

# A long step is fine when NO cold temperature is selected — the guard is scoped to
# the claim it protects, not a blanket restriction.
#
# THIS WAS THE ONE LEAKY CALL SITE (#3272 review round 3, B1). It used to be TWO cases:
# a BARE `bash "$DRIVER" --corpus … --temp warm --cold-step-duration 45s` asserting only
# that "exceeds the" was ABSENT, followed by the `expect_driver_accepts` below asserting
# the same property AFFIRMATIVELY. The bare one was therefore redundant AND was the
# defect: with no `--validate-args-only` and no shims, on any Linux host with `perf` and
# `taskset` present and `2,10` genuine siblings — i.e. the box the gate's `tooling-tests`
# runs on — `--temp warm` skips the cold ceiling and control falls PAST the argument
# boundary. MEASURED on a Linux-shaped host (fake sysfs with real `2,10` siblings,
# readable sysctl priors, recording shims), the shim file held:
#   sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
#   sudo -n sysctl -w kernel.perf_event_paranoid=2
#   sudo -n sysctl -w kernel.kptr_restrict=1
# — a real host mutation; where `sudo -n` succeeds it continues into
# `cargo build --release` and then `measure_scan`/`measure_flight` (3 reps x 2 arms of
# 45s Flight steps under real `perf stat`), inside a gate component.
#
# Both properties are now asserted off ONE hermetic run: the ceiling did NOT fire, AND
# the run reached the argument boundary and executed nothing.
out=$(ws0_driver_run "$DRIVER" --corpus "$TMP/corpus" --temp warm --cold-step-duration 45s); rc=$?
if [ "$rc" -eq 0 ] && grep -q "ARGUMENTS OK" <<<"$out" \
   && ! grep -q "exceeds the" <<<"$out" && ws0_driver_ran_hermetically; then
  pass "--temp warm with a 45s cold step is ACCEPTED (the ceiling is cold-scoped) and executes NOTHING"
else
  fail "--temp warm must not be blocked by the cold-step ceiling and must stay hermetic (rc=$rc, calls: $(ws0_hermetic_calls), out: $out)"
fi
# …and the same property through the shared helper, which carries its own three-way
# non-vacuity probe above.
expect_driver_accepts "--temp warm with a 45s cold step REACHES the argument boundary (affirmative)" \
  --corpus "$TMP/corpus" --temp warm --cold-step-duration 45s

# --------------------------------------------------------------------------
# Finding 1 — the bare-scan arm's prewarm is recorded, and a gap is flagged
# --------------------------------------------------------------------------
d="$TMP/prewarm-ok"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
ms = json.load(open(sys.argv[1]))["measurements"]
scan = [m for m in ms if m["arm"] == "bare_scan"]
assert scan, "no bare_scan measurement recorded"
s = scan[0]
assert s["prewarm"] == [{"rep": 1, "status": "ok"}], s["prewarm"]
assert s["prewarm_all_ok"] is True, s
assert s["reps"][0]["prewarm"] == "ok", s["reps"][0]
PY
then
  pass "the bare-scan arm's prewarm status is recorded per rep in results.json"
else
  fail "bare-scan prewarm not recorded in results.json (rc=$rc, out: $out)"
fi

# An UNRECORDED prewarm (a driver that predates the recording, or a rep that died
# before its prewarm) must be visible, not assumed healthy: it is exactly the
# pre-fix state, where a partly-cold rep was published as warm.
d="$TMP/prewarm-missing"; mkdir -p "$d"
make_scan_rep "$d" warm 1 -none-
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -eq 0 ] \
  && grep -q "PREWARM DEGRADED on bare-scan rep(s) 1=unrecorded" <<<"$out" \
  && grep -q "UNVERIFIED" <<<"$out"; then
  pass "an unrecorded bare-scan prewarm is FLAGGED in the summary as unverified"
else
  fail "unrecorded bare-scan prewarm must be flagged (rc=$rc, out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
s = [m for m in json.load(open(sys.argv[1]))["measurements"] if m["arm"] == "bare_scan"][0]
assert s["prewarm_all_ok"] is False, s
PY
then
  pass "results.json records prewarm_all_ok=false for the unrecorded case"
else
  fail "results.json must record prewarm_all_ok=false when a prewarm is unrecorded"
fi

# A FAILED bare-scan prewarm is likewise flagged (the driver fails closed before it
# gets here, but a hand-assembled or interrupted dir must not read as clean).
d="$TMP/prewarm-failed"; mkdir -p "$d"
make_scan_rep "$d" warm 1 FAILED-exit-1
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if grep -q "PREWARM DEGRADED on bare-scan rep(s) 1=FAILED-exit-1" <<<"$out"; then
  pass "a failed bare-scan prewarm is flagged"
else
  fail "failed bare-scan prewarm must be flagged (out: $out)"
fi

# The cold arm is deliberately NOT prewarmed, and that must read as healthy rather
# than as a degradation.
d="$TMP/prewarm-cold"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 1 "$CORPUS_ROWS" skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
# AFFIRMATIVE (#3272 review): the absence of "PREWARM DEGRADED" alone would pass on any
# unrelated failure that printed neither the warning nor a report, so the POSITIVE
# verdict is asserted too — `prewarm_all_ok: true` in results.json, with the recorded
# status that produced it.
if [ "$rc" -eq 0 ] && ! grep -q "PREWARM DEGRADED" <<<"$out" \
  && python3 - "$d/results.json" <<'PWOK'
import json, sys
for m in json.load(open(sys.argv[1]))["measurements"]:
    assert m["prewarm_all_ok"] is True, m
    assert m["prewarm_required_status"] == "cold", m
    assert all(p["status"] == "skipped-cold-arm" for p in m["prewarm"]), m
PWOK
then
  pass "skipped-cold-arm reads as HEALTHY on a cold rep (prewarm_all_ok=true, recorded)"
else
  fail "cold arm must not be flagged as prewarm-degraded (rc=$rc, out: $out)"
fi

# THE PREWARM SUBJECT MOVED (#3272 review round 12). The untimed prewarm's own question —
#
#     WAS THE MEASUREMENT ACTUALLY WARM, i.e. DID THE PREWARM READ THE WHOLE CORPUS?
#
# — is now `scripts/tests/test_ws0_prewarm_completeness.sh`, under the campsite rule (this file
# reached 1646 against the ~1500 test target) and along a responsibility seam: that property is
# decided at MEASUREMENT TIME, in `lib-measure.sh`, by classifiers in `ws0_prewarm.py`, over
# artifacts this reporter never reads — it reads only the one-word `<tag>.prewarm.status` they
# produce. Round 10's F-A (a status from an EXIT CODE) and round 12's F2 (a NON-ZERO scan accepted
# as a COMPLETE one, on both legs) both live there. What stays HERE is what this file's subject
# actually is: the REPORTER's handling of a recorded status — the per-temperature acceptance sets
# above, and the PREWARM DEGRADED line.

# ==========================================================================
# #3272 finding 1 — an ABSENT/INCOMPLETE corpus identity is FATAL, never a
#                   silently-skipped full-corpus check
# ==========================================================================
# NON-VACUITY. The pre-fix reporter read:
#
#     identity = {}
#     if idp.exists(): identity = json.loads(idp.read_text())
#     corpus_rows = int(identity["rows"]) if identity.get("rows") else None
#
# and `check_request_count` guarded its whole-corpus assert with `if corpus_rows:`.
# So with NO corpus-identity.json the `rows == requests_ok x corpus_rows` check was
# SKIPPED — while the NOTES block kept printing "every rep's rows an exact multiple
# of the corpus row count". MEASURED against the pre-fix code (a4dbcfa2e state):
# a WARM rep claiming 3 successful requests over 993 rows — not a multiple of the
# 1000-row corpus, i.e. no request scanned the whole corpus — exited **0** and wrote
# a results.json with `full_corpus_per_request_verified: false` buried in it, having
# printed the claim that the check ran. That is the input this case now rejects.
d="$TMP/no-identity"; make_warm_session "$d"
mkdir -p "$TMP/corpus-empty"   # a corpus dir with NO corpus-identity.json
expect_report_reject "an absent corpus-identity.json is FATAL (never a skipped check)" \
  "no corpus identity at" "$d" "$TMP/corpus-empty" warm bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus-empty" warm bypass 1 1)
if grep -q "full-corpus-per-request" <<<"$out" \
  && grep -q "refused rather than skipped" <<<"$out"; then
  pass "the refusal names the property that could not be checked, and says so"
else
  fail "the identity refusal must name the unverifiable property (out: $out)"
fi
# ...and NOTHING was written: a report that cannot verify its own claim must not
# leave a results.json a later reader could quote.
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written when the corpus identity is absent"
else
  fail "a refused run must not leave a results.json behind"
fi

# The exact pre-fix-accepted input, now with the identity PRESENT: still rejected,
# by the whole-corpus assert the absent identity used to disable. This is the
# non-vacuity pair — same rep shape, the only difference being whether the check
# could run at all.
d="$TMP/no-identity-partial"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 3 993 ok
expect_report_reject "the rep the absent identity used to wave through is refused once it can be checked" \
  "did not scan the whole corpus" "$d" "$TMP/corpus" warm bypass 1 1

# An INCOMPLETE identity is refused per field: a partial identity is not
# authoritative metadata, and `rows` alone is not the whole contract.
mkdir -p "$TMP/corpus-norows"
cat > "$TMP/corpus-norows/corpus-identity.json" <<EOF
{ "partitions": 10, "seed": 1, "cells_per_row": 12, "data_db_bytes": 700000,
  "data_db_sha256": "$FAKE_SHA", "bytes_per_row": 700.0 }
EOF
d="$TMP/id-norows"; make_warm_session "$d"
expect_report_reject "an identity with no 'rows' is refused, naming the field" \
  "carries no 'rows'" "$d" "$TMP/corpus-norows" warm bypass 1 1

mkdir -p "$TMP/corpus-zero"
cat > "$TMP/corpus-zero/corpus-identity.json" <<EOF
{ "rows": 0, "partitions": 10, "seed": 1, "cells_per_row": 12,
  "data_db_bytes": 700000, "data_db_sha256": "$FAKE_SHA", "bytes_per_row": 700.0 }
EOF
d="$TMP/id-zero"; make_warm_session "$d"
expect_report_reject "an identity claiming ZERO rows is refused (not a measurable corpus)" \
  "not a measurable corpus" "$d" "$TMP/corpus-zero" warm bypass 1 1

# An identity whose OWN fields disagree cannot be authoritative, whichever one is
# wrong: bytes_per_row must equal data_db_bytes/rows.
make_corpus "$TMP/corpus-inconsistent" "$CORPUS_ROWS" 700000 42.0
d="$TMP/id-inconsistent"; make_warm_session "$d"
expect_report_reject "an internally inconsistent identity is refused (bytes_per_row vs bytes/rows)" \
  "internally inconsistent" "$d" "$TMP/corpus-inconsistent" warm bypass 1 1

# The digest is the corpus's determinism pin, so a truncated one is refused: it
# cannot identify the bytes that were measured.
mkdir -p "$TMP/corpus-shortsha"
cat > "$TMP/corpus-shortsha/corpus-identity.json" <<EOF
{ "rows": $CORPUS_ROWS, "partitions": 10, "seed": 1, "cells_per_row": 12,
  "data_db_bytes": 700000, "data_db_sha256": "deadbeef", "bytes_per_row": 700.0 }
EOF
d="$TMP/id-shortsha"; make_warm_session "$d"
expect_report_reject "a truncated corpus digest is refused (cannot identify the measured bytes)" \
  "64 lowercase hex" "$d" "$TMP/corpus-shortsha" warm bypass 1 1

# And the happy path still records the verification as having HAPPENED, with the
# row count it used — so the field cannot read `true` without a number behind it.
d="$TMP/id-ok"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" "$CORPUS_ROWS" <<'PY'
import json, sys
ms = json.load(open(sys.argv[1]))["measurements"]
fl = [m for m in ms if m["arm"].startswith("flight_")]
assert fl, "no flight measurement recorded"
assert fl[0]["full_corpus_per_request_verified"] is True, fl[0]
assert fl[0]["corpus_rows_used_for_verification"] == int(sys.argv[2]), fl[0]
PY
then
  pass "a verified run records full_corpus_per_request_verified=true WITH the row count used"
else
  fail "the happy path must record the verification and its row count (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 2 — `skipped-cold-arm` satisfies a COLD rep ONLY
# ==========================================================================
# NON-VACUITY. The pre-fix acceptance set was a flat, temperature-BLIND tuple:
#
#     OK_PREWARM = ("ok", "skipped-cold-arm")
#     "prewarm_all_ok": all(p["status"] in OK_PREWARM for p in prewarm)
#
# MEASURED against the pre-fix code: a WARM session whose bare-scan AND flight reps
# both recorded `skipped-cold-arm` exited **0**, printed NO "PREWARM DEGRADED" line,
# and wrote `prewarm_all_ok: true` for both arms. That is an UNPREWARMED WARM
# measurement passing the guard added to prevent exactly that — using the cold arm's
# own sentinel as the key. Both arms are covered below, because the bare scan is the
# DENOMINATOR of the ratio (reading slow there makes the target easier).
d="$TMP/warm-cold-sentinel-scan"; mkdir -p "$d"
make_scan_rep "$d" warm 1 skipped-cold-arm
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a WARM bare-scan rep carrying 'skipped-cold-arm' is REFUSED" \
  "only a COLD rep can record" "$d" "$TMP/corpus" warm bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1)
if grep -q "UNPREWARMED" <<<"$out"; then
  pass "the refusal says the warm rep was UNPREWARMED (not merely 'inconsistent')"
else
  fail "the warm-sentinel refusal must name the unprewarmed measurement (out: $out)"
fi

d="$TMP/warm-cold-sentinel-flight"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" skipped-cold-arm
expect_report_reject "a WARM flight rep carrying 'skipped-cold-arm' is REFUSED" \
  "only a COLD rep can record" "$d" "$TMP/corpus" warm bypass 1 1

# The mirror direction: a COLD rep that recorded a SUCCESSFUL prewarm is not cold.
# The sentinel is scoped to a temperature in BOTH directions, not merely blocked in
# the one that was found in review.
d="$TMP/cold-prewarmed"; mkdir -p "$d"
make_scan_rep "$d" cold 1 ok
make_flight_rep "$d" cold 1 1 "$CORPUS_ROWS" skipped-cold-arm
expect_report_reject "a COLD rep that recorded a successful prewarm is REFUSED (it is not cold)" \
  "only a WARM rep can record" "$d" "$TMP/corpus" cold bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus" cold bypass 1 1)
if grep -q "prewarmed rep is not cold" <<<"$out"; then
  pass "the cold-direction refusal says a prewarmed rep is not cold"
else
  fail "the cold-direction refusal must state why (out: $out)"
fi

# A HONEST degradation is still reported, not refused: the two cases must stay
# distinguishable, or the fix would have turned every flaky prewarm into a lost rep.
d="$TMP/warm-degraded-still-reported"; mkdir -p "$d"
make_scan_rep "$d" warm 1 FAILED-exit-9
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "PREWARM DEGRADED on bare-scan rep(s) 1=FAILED-exit-9" <<<"$out"; then
  pass "an honestly-recorded prewarm FAILURE is still reported+flagged (not refused)"
else
  fail "a recorded prewarm failure must be flagged, not refused (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 4 — a counter that was not observed is an ERROR, never a 0
# ==========================================================================
# NON-VACUITY. The pre-fix reporter read every counter through a defaulting get:
#
#     cyc = total.get("cycles", 0) - setup.get("cycles", 0)
#     setup_cycles_total += setup.get("cycles", 0)
#
# and `read_perf_csv` returned `{}` for a file that does not exist. MEASURED against
# the pre-fix code: a session dir with NO `perf-scan-warm-1-setup.csv` AT ALL exited
# **0** and reported `cycles_setup: 0`, `setup_cycles_subtracted_total: 0`, while the
# summary printed "the bare scan's cycles are SETUP-SUBTRACTED". Nothing was
# subtracted. Each case below removes or corrupts exactly one counter.
d="$TMP/no-setup-csv"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
rm -f "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "an ABSENT setup perf CSV is FATAL (never a 0 subtraction)" \
  "were never observed" "$d" "$TMP/corpus" warm bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1)
if grep -q "cannot substitute a zero" <<<"$out"; then
  pass "the refusal states that a zero may not stand in for an unobserved counter"
else
  fail "the absent-counter refusal must reject the substitution explicitly (out: $out)"
fi

d="$TMP/no-total-csv"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
rm -f "$d/perf-scan-warm-1.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "an ABSENT full-run perf CSV is FATAL" \
  "were never observed" "$d" "$TMP/corpus" warm bypass 1 1

d="$TMP/no-flight-csv"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
rm -f "$d/perf-flight-bypass-warm-1.csv"
expect_report_reject "an ABSENT flight perf CSV is FATAL (the arm has no setup leg to hide it)" \
  "were never observed" "$d" "$TMP/corpus" warm bypass 1 1

# A CSV that exists but carries no `cycles` line: the file is there, the counter is
# not. The pre-fix `.get("cycles", 0)` could not tell these apart.
d="$TMP/csv-no-cycles"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf '4000000,,instructions,1000000000,100.00,2.000,insn per cycle\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a perf CSV with no 'cycles' line is FATAL, naming the missing event" \
  "no line for required event(s) cycles" "$d" "$TMP/corpus" warm bypass 1 1

d="$TMP/csv-no-instructions"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf '100000,,cycles,1000000000,100.00,1.000,GHz\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a perf CSV with no 'instructions' line is FATAL (IPC has no numerator)" \
  "no line for required event(s) instructions" "$d" "$TMP/corpus" warm bypass 1 1

# perf's OWN not-a-value markers. This is the silent-instrument failure in its
# purest form: the line EXISTS, perf EXITED ZERO, and there is no number.
for marker in '<not counted>' '<not supported>'; do
  d="$TMP/csv-marker-$(tr -d ' <>' <<<"$marker")"; mkdir -p "$d"
  make_scan_rep "$d" warm 1 ok
  printf '%s,,cycles,1000000000,100.00,1.000,GHz\n200000,,instructions,1000000000,100.00,2.000,insn per cycle\n' "$marker" > "$d/perf-scan-warm-1-setup.csv"
  make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
  expect_report_reject "a perf '$marker' value is FATAL (perf did not count it)" \
    "perf did not count it" "$d" "$TMP/corpus" warm bypass 1 1
done

# A corrupt value is a corrupt artifact, not a zero.
d="$TMP/csv-garbage"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf 'NaNsense,,cycles,1000000000,100.00,1.000,GHz\n200000,,instructions,1000000000,100.00,2.000,insn per cycle\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "an unparseable perf value is FATAL (corrupt artifact, not a 0)" \
  "unparseable value" "$d" "$TMP/corpus" warm bypass 1 1

# And on the happy path the subtraction is a REAL non-zero, recorded — so a future
# regression to a defaulted 0 shows up as a value, not only as an absent error.
d="$TMP/setup-subtracted"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
s = [m for m in json.load(open(sys.argv[1]))["measurements"] if m["arm"] == "bare_scan"][0]
r = s["reps"][0]
assert r["cycles_setup"] == 100000, r
assert r["cycles_total"] == 2000000, r
assert r["cycles_scan"] == 1900000, r          # a REAL subtraction, not a 0
assert s["setup_cycles_subtracted_total"] == 100000, s
PY
then
  pass "the happy path records a REAL non-zero setup subtraction"
else
  fail "setup subtraction must be a real observed value (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 5 — the numeric arguments are validated fail-closed
# ==========================================================================
# NON-VACUITY. `--reps` was `type=int` with no range check, and `range(1, 0 + 1)` is
# EMPTY, so every collector loop body was skipped, `per_rep` stayed empty, and the
# pre-fix `require_complete` returned early on `not per_rep and not missing`.
# MEASURED against the pre-fix code: `--reps 0` over an EMPTY session dir exited
# **0** and wrote a results.json with `measurements: []` — a report that measured
# nothing, indistinguishable at the exit code from one that measured everything.
# `--reps -3` and `--scan-passes -1` did the same.
d="$TMP/reps-zero"; make_warm_session "$d"
expect_report_reject "--reps 0 is REFUSED (a vacuous but successful report)" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass 0 1
if [ ! -e "$TMP/reps-zero/results.json" ]; then
  pass "--reps 0 writes no results.json"
else
  fail "--reps 0 must not write a results.json"
fi
expect_report_reject "--reps -3 is REFUSED" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass -3 1
expect_report_reject "--reps 'abc' is REFUSED with a reason (not a traceback)" \
  "must be an integer" "$d" "$TMP/corpus" warm bypass abc 1
expect_report_reject "--scan-passes 0 is REFUSED (same hole, same class)" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass 1 0
# The same class from the OTHER end. Python ints do not overflow, so an absurd --reps
# is not a wrong number — it is NO number: `range(1, 10**20)` statting a file per
# iteration never reaches a verdict. MEASURED before the cap:
# `--reps 99999999999999999999` ran past a 10s timeout with no output at all.
expect_report_reject "an absurdly large --reps is REFUSED (would never reach a verdict)" \
  "absurdly large" "$d" "$TMP/corpus" warm bypass 99999999999999999999 1
expect_report_reject "an absurdly large --scan-passes is REFUSED" \
  "absurdly large" "$d" "$TMP/corpus" warm bypass 1 99999999999999999999
# And the reporter must TERMINATE on that input rather than merely printing something:
# the whole point is that the pre-cap code did not.
# The configuration now arrives from the MANIFEST (#3272 F1), so the absurd count is stamped
# there rather than passed as an argument — the guard is the same one, applied at the point the
# value actually enters the reporting path.
rm -f "$d/session-corpus-pin.json"
ws0_pin_session_corpus "$d" "$TMP/corpus" 99999999999999999999 warm bypass 1
if timeout 15 python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" >/dev/null 2>&1; rc=$?; [ "$rc" -eq 1 ]; then
  pass "OBSERVED: the reporter TERMINATES (rc=1) on an absurd --reps (pre-cap: timed out)"
else
  fail "the reporter must terminate non-zero on an absurd --reps (rc=$rc; 124 = still hangs)"
fi
expect_report_reject "--scan-passes -1 is REFUSED" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass 1 -1
# The non-numeric selections had the same vacuous-green hole: an empty temps/arms produced
# zero measurements and exit 0.
#
# SINCE #3272 F1 these values are read from the SESSION MANIFEST rather than the reporter's
# command line, and every guard below still applies AT THAT BOUNDARY: `session_manifest_config`
# puts each field through the SAME validator the CLI used (`cli_count`, `nonempty_selection`),
# so a hand-edited manifest cannot smuggle `reps: 0` or an unknown temperature past the reader.
# The cases are unchanged in substance — only where the bad value comes FROM has moved, and it
# moved to the only place that can be authoritative about what was measured.
expect_report_reject "an EMPTY --temps is REFUSED (would report zero measurements)" \
  "is empty" "$d" "$TMP/corpus" "" bypass 1 1
expect_report_reject "an EMPTY --arms is REFUSED" \
  "is empty" "$d" "$TMP/corpus" warm "" 1 1
expect_report_reject "an UNKNOWN --temps value is REFUSED (never silently ignored)" \
  "unknown value" "$d" "$TMP/corpus" "warm tepid" bypass 1 1
expect_report_reject "an UNKNOWN --arms value is REFUSED" \
  "unknown value" "$d" "$TMP/corpus" warm "bypass sideways" 1 1
expect_report_reject "a REPEATED --temps value is REFUSED (would double-count a leg)" \
  "repeats warm" "$d" "$TMP/corpus" "warm warm" bypass 1 1
# A --dir that does not exist must not be created-then-reported-on.
expect_report_reject "a nonexistent --dir is REFUSED" \
  "is not an existing directory" "$TMP/does-not-exist" "$TMP/corpus" warm bypass 1 1
expect_report_reject "a nonexistent --corpus is REFUSED" \
  "is not an existing directory" "$d" "$TMP/no-such-corpus" warm bypass 1 1

# The driver validates --reps too, up front, before any build or cache drop. It used
# to accept `--reps 0` (and `--reps abc`, which then made `seq 1 abc` emit nothing)
# and only fail later at the missing-corpus check.
check_driver_reject "the DRIVER refuses --reps 0 before doing any work" \
  "must be at least 1" --corpus "$TMP/corpus" --reps 0
check_driver_reject "the DRIVER refuses a non-numeric --reps" \
  "positive integer" --corpus "$TMP/corpus" --reps abc
check_driver_reject "the DRIVER refuses a negative --reps" \
  "positive integer" --corpus "$TMP/corpus" --reps -2
check_driver_reject "the DRIVER refuses --port 0" \
  "must be at least 1" --corpus "$TMP/corpus" --port 0
check_driver_reject "the DRIVER refuses an out-of-range --port" \
  "65535" --corpus "$TMP/corpus" --port 70000

# ---- the driver's cap and the reporter's cap AGREE (#3272 review) -----------
# NON-VACUITY: the driver's `--reps` bound was a NINE-DIGIT length check, and the
# reporter's is 100,000. MEASURED against that driver: `--reps 200000` passed argument
# validation and ran on — 200,000 reps, each a full-corpus bare scan plus a Flight rep,
# i.e. days of measurement — and would have been refused only by ws0_report.py at the
# very end. Refusing a value after acting on it is not refusing it.
check_driver_reject "the DRIVER refuses --reps 200000 UP FRONT (the reporter's own cap)" \
  "must be at most 100000" --corpus "$TMP/corpus" --reps 200000
out=$(ws0_driver_run "$DRIVER" --corpus "$TMP/corpus" --reps 200000)
if grep -q 'SAME bound ws0_report.py enforces' <<<"$out"; then
  pass "the cap refusal says it is the reporter's bound, refused before the reps run"
else
  fail "the reps-cap refusal must name the reporter's bound (out: $out)"
fi
# The bound is written in two languages, so DRIFT is caught mechanically rather than
# trusted to a comment: both values are read and compared.
driver_cap=$(awk -F= '/^MAX_COUNT=/{print $2; exit}' "$ARGS_LIB")
report_cap=$(python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
import ws0_validate
print(ws0_validate.MAX_COUNT)
PY
)
if [ -n "$driver_cap" ] && [ "$driver_cap" = "$report_cap" ]; then
  pass "the driver's MAX_COUNT ($driver_cap) EQUALS ws0_validate.MAX_COUNT ($report_cap)"
else
  fail "the two caps disagree (driver='$driver_cap' reporter='$report_cap') — the driver would accept a value the report refuses"
fi
# And a value just INSIDE the shared cap must still be accepted: the fix is agreement,
# not a blanket lowering that would refuse a legitimate long session.
expect_driver_accepts "--reps 100000 (exactly the cap) is ACCEPTED by the driver" \
  --corpus "$TMP/corpus" --reps 100000

# ==========================================================================
# #3272 finding 6 — completeness is judged against the SELECTION, and the
#                   selection is stated in the report
# ==========================================================================
# NON-VACUITY, in BOTH directions. `require_complete`'s docstring claimed
# "`per_rep` empty AND nothing missing -> this (arm, temperature) was never run;
# not an error" — but the collectors append EVERY absent expected artifact to
# `missing` before calling it, so that branch was DEAD CODE and the case it
# documented exited fatally. MEASURED against the pre-fix code: a session dir
# holding only WARM reps, reported with `--temps "warm cold"`, exited **1** with
# "bare scan (cold) collected 0 of 1 requested reps"; and with
# `--arms "bypass merge"` it exited **1** on the absent merge arm. An intentionally
# narrow run was indistinguishable from a crashed one.
#
# The fix is not to loosen the check — it is to make the SELECTION the thing
# completeness is judged against, and to STATE it. So both of these must hold:
#   (a) an unselected combination is simply not iterated (nothing to be absent);
#   (b) a SELECTED combination that is absent stays fatal.
d="$TMP/warm-only"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "a warm-only session reported as warm-only SUCCEEDS (the narrow run is legitimate)"
else
  fail "a warm-only selection must succeed on a warm-only dir (rc=$rc, out: $out)"
fi
# (b) the same dir, with cold SELECTED, is still fatal — the fix did not open a hole.
expect_report_reject "a SELECTED but absent temperature is still FATAL" \
  "bare scan (cold) collected 0 of 1" "$d" "$TMP/corpus" "warm cold" bypass 1 1
expect_report_reject "a SELECTED but absent arm is still FATAL" \
  "flight do_get merge (warm) collected 0 of 1" "$d" "$TMP/corpus" warm "bypass merge" 1 1
# ...and a PARTIAL collection of a selected combination is fatal, which is the case
# the whole check exists for.
d="$TMP/partial-reps"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_scan_rep "$d" warm 2 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
# rep 2 of the flight arm is missing: 1 of 2 collected.
expect_report_reject "a PARTIAL collection of a selected arm is FATAL (the original guard holds)" \
  "collected 1 of 2" "$d" "$TMP/corpus" warm bypass 2 1

# The DRIVER states the selection too, at the top of the transcript — before any
# measurement exists to be misread. Structural, because reaching the banner needs a
# real corpus: the branch must exist and must distinguish the two cases.
ws0_3096_banner=$(awk '/^echo "=== issue #3096/,0' "$DRIVER")
if out_has "$ws0_3096_banner" 'PARTIAL MATRIX' \
  && out_has "$ws0_3096_banner" 'FULL MATRIX'; then
  pass "the driver's banner distinguishes a PARTIAL from a FULL matrix"
else
  fail "the driver must print its selection as PARTIAL/FULL up front"
fi

# The selection must be VISIBLE — in results.json and in the human summary — or a
# narrow run reads exactly like a full matrix that printed fewer rows.
d="$TMP/selection-recorded"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
sel = json.load(open(sys.argv[1]))["selection"]
assert sel["temperatures"] == ["warm"], sel
assert sel["arms"] == ["bypass"], sel
assert sel["full_matrix"] is False, sel
assert "cold" in sel["temperatures_available"], sel
assert "merge" in sel["arms_available"], sel
PY
then
  pass "results.json records the SELECTION and marks a partial matrix as partial"
else
  fail "results.json must record the selection (rc=$rc, out: $out)"
fi
if grep -q "PARTIAL MATRIX" <<<"$out" && grep -q "NOT MEASURED" <<<"$out"; then
  pass "the human summary says PARTIAL MATRIX / NOT MEASURED for a narrow run"
else
  fail "a narrow run's summary must say so loudly (out: $out)"
fi
# A FULL matrix must NOT carry the partial warning — the marker has to mean something.
d="$TMP/full-matrix"; mkdir -p "$d"
for temp in warm cold; do
  case "$temp" in warm) pw=ok ;; cold) pw=skipped-cold-arm ;; esac
  make_scan_rep "$d" "$temp" 1 "$pw"
  # THREE arms this round (scan + bypass + merge), so `arms_in_round` is 3 and the
  # positions are 1..3 — `make_scan_rep`'s 2-arm default is overridden here rather than
  # left to disagree with the round it is part of, which the reporter would (correctly)
  # refuse as a PARTIAL round.
  make_round "$d" "scan-$temp-1" 1 1 3
  pos=1
  for arm in bypass merge; do
    pos=$((pos + 1))
    tag="flight-$arm-$temp-1"
    # rows_per_s COMPUTED, as in make_flight_rep — the reporter derives it (#3272 round 4).
    fm_rps="$(python3 -c "print($CORPUS_ROWS / 4.0)")"
    cat > "$d/$tag.jsonl" <<EOF
{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"$tag","endpoint":"$WS0_FIXTURE_ENDPOINT","requests_ok":1,"requests_error":0,"error_codes":{},"requests_unavailable":0,"rows_total":$CORPUS_ROWS,"rows_per_s":$fm_rps,"duration_s":4.0}
EOF
    perf_csv "$d/perf-$tag.csv" 8000000 16000000
    printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
    make_round "$d" "$tag" 1 "$pos" 3
  done
done
out=$(run_report_full "$d" "$TMP/corpus" "warm cold" "bypass merge" 1 1); rc=$?
if [ "$rc" -eq 0 ] && ! grep -q "PARTIAL MATRIX" <<<"$out" \
  && python3 - "$d/results.json" <<'PY'
import json, sys
sel = json.load(open(sys.argv[1]))["selection"]
assert sel["full_matrix"] is True, sel
PY
then
  pass "a FULL matrix is not flagged partial (the marker distinguishes, not decorates)"
else
  fail "a full matrix must record full_matrix=true and no partial warning (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 7 — durations parse as DECIMAL, never octal
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `parse_duration_ms` (which fed the
# stripped digits straight into `$((n * 1000))`):
#   * `010s`   -> 8000 ms. A caller asking for 10s silently measured 8s.
#   * `08s`    -> hard bash error "08: value too great for base (error token is 08)",
#                 which the `case` turned into "must be <n>ms, <n>s or <n>m" — a
#                 diagnostic about the FORMAT for a value whose format is fine.
#   * `030ms`  -> 24 ms.
#   * `010000ms` -> 4096 ms, i.e. UNDER the 5000ms cold ceiling while really being
#                 10s: the octal parse could smuggle a blended cold step past the
#                 guard of #3096 finding 2.
# The driver is only ever reached at argument validation here — no corpus, no sudo.
#
# `--cold-step-duration 010s` = 10s > the 5000ms ceiling, so it must be REFUSED.
# Pre-fix it parsed as 8000ms and was ACCEPTED (falling through to the missing-corpus
# error instead), which is what this case observes.
check_driver_reject "'010s' is parsed as 10s (decimal) and refused by the cold ceiling" \
  "10000ms) exceeds the" --corpus "$TMP/corpus" --temp cold --cold-step-duration 010s
check_driver_reject "'010000ms' is 10000ms, not octal 4096 — it cannot sneak under the ceiling" \
  "10000ms) exceeds the" --corpus "$TMP/corpus" --temp cold --cold-step-duration 010000ms
# `08s` is a legitimate spelling of 8s: it must reach the CEILING check (8000 > 5000)
# and be refused for its VALUE, not die with a format complaint about a valid format.
out=$(ws0_driver_run "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 08s)
if grep -q "8000ms) exceeds the" <<<"$out"; then
  pass "'08s' parses as 8000ms (pre-fix: a bash 'value too great for base' error)"
else
  fail "'08s' must parse as 8000ms and be judged on its value (out: $out)"
fi
# And a leading-zero value that is genuinely IN range must be ACCEPTED, so the fix
# is not "reject leading zeros".
expect_driver_accepts "'0500ms' (=500ms, in range) is ACCEPTED — leading zeros are parsed, not banned" \
  --corpus "$TMP/corpus" --temp cold --cold-step-duration 0500ms
# The warm step goes through the same parser.
expect_driver_accepts "'045s' is accepted for --step-duration (pre-fix: a bash base error)" \
  --corpus "$TMP/corpus" --temp warm --step-duration 045s
# A structural check that no arithmetic path can regress: every multiplication of a
# parsed duration component must carry `10#`.
if out_has "$(awk '/^parse_duration_ms\(\)/,/^}/' "$ARGS_LIB")" '\$((n \* 1000))'; then
  fail "parse_duration_ms still multiplies a bare \$n — leading zeros would be octal again"
else
  pass "parse_duration_ms feeds no bare component into arithmetic (structural)"
fi

# ---- the OTHER half of the same class: 64-bit WRAPAROUND -------------------
# Found by self-checking this very change against the integer-overflow class, and it
# is the SAME BYPASS SHAPE as the octal defect. Bash arithmetic is signed 64-bit and
# wraps silently, so `2305843009213693956s` * 1000 lands on **4000 ms** — UNDER the
# 5000ms cold ceiling. MEASURED against the `10#`-only driver (i.e. after finding 7's
# first fix but before this one): the value sailed through the ceiling to the
# corpus-missing check, meaning a caller could smuggle a BLENDED cold step past
# #3096 finding 2's guard with an absurd duration. Both guards are needed; `10#`
# alone is not sufficient.
# The diagnostic must name the RANGE, not the FORMAT (#3272 review). These two cases
# used to assert `must be <n>ms, <n>s or <n>m` — a complaint about the format of a value
# whose format is perfectly fine, which is the SAME misleading diagnostic this file
# criticizes for `08s` two blocks up, reintroduced through the digit cap. Both branches
# of `parse_duration_ms` now report their own cause, so a too-long value says so and
# says what the maximum is.
check_driver_reject "a 64-bit-WRAPPING cold step is refused (would wrap to 4000ms, under the ceiling)" \
  "is too LONG" --corpus "$TMP/corpus" --temp cold \
  --cold-step-duration 2305843009213693956s
out=$(ws0_driver_run "$DRIVER" --corpus "$TMP/corpus" --temp cold \
  --cold-step-duration 2305843009213693956s)
if grep -q '19 digits' <<<"$out" && grep -q 'maximum' <<<"$out" \
  && ! grep -q 'must be <n>ms' <<<"$out"; then
  pass "the too-long refusal states the DIGIT COUNT and the maximum, not a format complaint"
else
  fail "a too-long duration must be refused on RANGE, naming the length (out: $out)"
fi
if grep -q 'wraps to 4000ms' <<<"$out"; then
  pass "the too-long refusal explains the wraparound it prevents (and the cold ceiling)"
else
  fail "the too-long refusal must explain why the cap exists (out: $out)"
fi
check_driver_reject "a 20-digit duration is refused before arithmetic touches it" \
  "is too LONG" --corpus "$TMP/corpus" --temp cold \
  --cold-step-duration 99999999999999999999ms
# ...and a genuinely MALFORMED value still gets the format message: the split must
# distinguish the two causes, not replace one blanket message with another.
check_driver_reject "a genuinely malformed value still gets the FORMAT message" \
  "must be <n>ms, <n>s or <n>m" --corpus "$TMP/corpus" --temp cold \
  --cold-step-duration 45
out=$(ws0_driver_run "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 45)
if ! grep -q 'too LONG' <<<"$out"; then
  pass "a malformed value is NOT reported as too long (the two causes stay distinct)"
else
  fail "a malformed value must not report a length problem (out: $out)"
fi
# The cause code must survive the call. `if ! cmd; then … $? …` reads 0 — `!` REPLACES
# the status — which silently collapsed both causes back into the format branch.
if out_has "$(awk '/^for _spec in "step-duration/,/^done$/' "$DRIVER")" '_rc=0 || _rc=\$?'; then
  pass "the duration cause code is captured on its own statement (not through \`if !\`)"
else
  fail "the duration rc must be captured directly; \`if ! cmd\` discards it"
fi
# The largest value inside the cap must still PARSE — the fix is a cap, not a ban on
# big-but-sane durations. 999999999ms is ~11.5 days: over the cold ceiling, so it must
# be refused for its VALUE, which proves it reached the ceiling rather than the parser.
out=$(ws0_driver_run "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 999999999ms)
if grep -q "999999999ms) exceeds the" <<<"$out"; then
  pass "the largest in-cap duration still parses (judged on value, not rejected as malformed)"
else
  fail "999999999ms must parse and be refused by the ceiling (out: $out)"
fi
# Same wraparound class on the plain integer options.
check_driver_reject "a 20-digit --reps is refused before arithmetic (would wrap to 7766279631452241919)" \
  "absurdly large" --corpus "$TMP/corpus" --reps 99999999999999999999
check_driver_reject "a 20-digit --port is refused before arithmetic" \
  "absurdly large" --corpus "$TMP/corpus" --port 99999999999999999999
# The digit test must come BEFORE any arithmetic, or the bound is itself evaluated by
# the arithmetic that wraps.
if awk '/^require_positive_int\(\)/,/^}/' "$ARGS_LIB" \
  | awk '/#value/{d=NR} /10#\$value/{if(!d) bad=1} END{exit(bad?1:0)}'; then
  pass "the digit-count check precedes the arithmetic in require_positive_int"
else
  fail "require_positive_int must test the digit count before any arithmetic"
fi
if out_has "$(awk '/^parse_duration_ms\(\)/,/^}/' "$ARGS_LIB")" 'DURATION_MAX_DIGITS'; then
  pass "parse_duration_ms caps the digit count (structural)"
else
  fail "parse_duration_ms must cap the digit count before multiplying"
fi

# ==========================================================================
# #3272 round 16 — THE ARM IS A REQUEST, NOT AN OBSERVATION
# ==========================================================================
# `CQLITE_FLIGHT_MERGE_PATH=bypass` only PREFERS the single-source fast path. cqlite-flight's
# own `bypass.rs` docs: "`bypass` requests the fast path but NEVER overrides a correctness
# precondition — a 2-source table under `bypass` still merges", and
# `bypass_tests.rs::forced_bypass_never_overrides_a_correctness_precondition` pins exactly that
# (`ForcedMergePath::Bypass` over two sources => `BypassReason::MultipleSources`). A second route
# exists too: the predicate SELECTS the fast path and `ScanRowSource::open` then returns
# `Ok(None)`, so `producer_warm.rs` falls through to `KWayMerger::new_from_readers`.
#
# The reporter recorded the REQUESTED value under the key `forced_merge_path` and printed the
# label `flight do_get (bypass)`, so a rep that EXECUTED THE MERGER was published as a bypass
# measurement. The rig's headline is a bare/flight RATIO PER ARM, so in the limit the two arm
# rows are the same code twice under different labels — the comparison, not the label.
#
# WHY THIS IS A RELABEL AND NOT AN OBSERVATION (the #3272 round 13 route, not round 14's): the
# server does not report the arm it took. `producer_warm.rs` consumes `bypass_reason(...)` only
# in `if reason.is_selected()` — never logged, metered, spanned or returned — and the
# `ScanRowSource::open` fallback records nothing at all. `bypass.rs` points at
# `cqlite_core::storage::read_path_probe`, but that is three PROCESS-GLOBAL `AtomicU64`s read
# IN-PROCESS (its docs: "the consumers are integration tests in a DIFFERENT crate") with no RPC,
# log or metric export; this rig measures a SEPARATE server process over loopback gRPC, so it is
# unreachable by construction. The only out-of-band surface is `do_action`'s TABLE_STATS_ACTION,
# whose response carries row/partition/SSTable counts and no arm. Emitting it would mean changing
# production cqlite-flight, outside this issue's scope — so the claim is withdrawn rather than
# faked, exactly as measurement-method.md §3b.1 withdrew the interleaving claim.
#
# NON-VACUITY, MEASURED rather than asserted: the first case runs a rep whose EXECUTED arm was
# the merger through a VERBATIM RECONSTRUCTION of the pre-fix block, and measures that it emits
# `forced_merge_path: bypass` — the false label, on this exact input. The same input then goes
# through the shipped reporter, which must emit no such claim.
# --------------------------------------------------------------------------
d="$TMP/merge-path-requested"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?

# (a) NON-VACUITY. The pre-fix tail of `collect_flight` reproduced EXACTLY as it stood at
# 33c0b8583 (`"forced_merge_path": arm`), fed the SAME `arm` the shipped code is given, for a rep
# whose executed path was the merger. It must produce the false claim; if it does not, the case
# below is not observing anything.
if python3 - <<'PY'
# The pre-fix line, verbatim: the block keyed the REQUESTED arm under a name that reads as the
# path taken. `arm` is the driver's `--arm` value, which is all the pre-fix code ever had.
def collect_flight_prefix_tail(arm):
    return {"arm": f"flight_do_get_{arm}", "temperature": "warm", "forced_merge_path": arm}

# A rep the SERVER served from the k-way merger despite `CQLITE_FLIGHT_MERGE_PATH=bypass` — the
# `MultipleSources`/`ScanRowSource::open -> None` fallback. Nothing in the artifact set can say
# so, which is the whole finding: the rig's input is identical either way.
block = collect_flight_prefix_tail("bypass")
assert block["forced_merge_path"] == "bypass", block
assert "executed_merge_path" not in block, block
assert "requested_merge_path" not in block, block
PY
then
  pass "NON-VACUITY: the PRE-FIX block labels a MERGER-executed rep 'forced_merge_path: bypass'"
else
  fail "the pre-fix reconstruction must emit the false forced_merge_path label"
fi

# (b) the shipped reporter, on the SAME session: the requested value is recorded UNDER A NAME
# THAT SAYS SO, the executed arm is recorded as NOT OBSERVED, and the old key is GONE — a
# consumer of `forced_merge_path` must fail to find it rather than keep reading a value whose
# meaning changed under an unchanged key.
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
ms = json.load(open(sys.argv[1]))["measurements"]
fl = [m for m in ms if m["arm"] == "flight_do_get_bypass"]
assert fl, [m["arm"] for m in ms]
b = fl[0]
assert "forced_merge_path" not in b, "the claiming key must be GONE, not shadowed: %r" % (b,)
assert b["requested_merge_path"] == "bypass", b
assert b["executed_merge_path"] == "NOT OBSERVED", b
# The note must NAME the mechanism, not merely hedge: the two fallback routes and the reason the
# server cannot be asked. A vague caveat is how a reader concludes the arm is probably fine.
note = b["merge_path_observability"]
for frag in ("REQUEST, not an observation", "correctness precondition",
             "ScanRowSource::open", "DOES NOT REPORT THE ARM IT TOOK", "read_path_probe"):
    assert frag in note, (frag, note)
PY
then
  pass "results.json records the arm as REQUESTED with the executed arm NOT OBSERVED"
else
  fail "results.json must relabel the arm as requested (rc=$rc, out: $out)"
fi

# (c) THE PRINTED SUMMARY must not out-claim results.json. It used to print `flight do_get
# (bypass)`, which reads as the path served; and a relabel that fixed only the JSON would leave
# the human-read artifact making the withdrawn claim.
if [ "$rc" -eq 0 ] \
   && grep -q 'flight do_get (bypass requested)' <<<"$out" \
   && grep -q 'the ARM of each flight row above is the value this rig REQUESTED' <<<"$out" \
   && grep -q 'arm actually EXECUTED is NOT OBSERVED' <<<"$out"; then
  pass "the printed summary labels the arm REQUESTED and states the executed arm is NOT OBSERVED"
else
  fail "the summary must not print a bare arm label (rc=$rc, out: $out)"
fi

# (d) ...and the summary must say WHAT IS THEREFORE UNVERIFIED — the per-arm RATIO. Stating the
# arm is unobserved while leaving the ratio reading as an arm-to-arm comparison would be the
# caveat without its consequence, which is how §3b.1's earlier wording under-stated its own.
if [ "$rc" -eq 0 ] \
   && grep -q 'conditional on a request the server was free to decline' <<<"$out" \
   && grep -q 'same code measured twice' <<<"$out"; then
  pass "the summary states the per-arm ratio is conditional on an UNOBSERVED arm"
else
  fail "the summary must state the ratio consequence, not just the arm caveat (out: $out)"
fi

# (e) THE TWO SIDES CANNOT DISAGREE. The printed label is read from the block's own
# `requested_merge_path`, so a future rename on one side raises rather than printing a label the
# JSON does not support. Asserted structurally, because a behavioural case can only cover the
# spelling that exists today.
if grep -q "fmt(f\"flight do_get ({fl\['requested_merge_path'\]} requested)\", fl)" \
     "$REPO_ROOT/scripts/perf/ws0_report.py"; then
  pass "the printed arm label is derived FROM the recorded block, not from the loop variable"
else
  fail "the summary's arm label must read the block's requested_merge_path (structural)"
fi


# ==========================================================================
# #3248 — the QUIESCENCE VERDICT must COVER this session's window, and carry its caveats
# ==========================================================================
# Two properties of the `judged against <path>` branch of the reporter's quiescence block.
# Neither is a crash: each publishes a QUIESCENT claim the evidence does not support.
#
#   A. WINDOW COVERAGE. The pre-fix code checked only that the verdict named the same
#      TIMESERIES as the manifest. `box-load.jsonl` is ONE long-lived file spanning every
#      session on the box, so a clean verdict judged over an ADJACENT window of that same
#      file satisfied every check and certified this session — a pass borrowed from a
#      different measurement. The verdict's `window_census.window` must now COVER the
#      session's own window, derived from the rep payloads' `ts_unix_ms`/`duration_s`
#      (each rep spans [ts-duration, ts]; tools/flight-loadgen/src/ramp.rs:184-188 is the
#      authority for `ts` being the rep END).
#   B. REQUIRED CAVEATS. `samples`, `coverage_largest_gap_s`, `narrow_census_records` and
#      `census_breadth` were read with `.get()`, so a verdict missing them published
#      QUIESCENT with `null` caveats — a null caveat reads as an absent concern, which is a
#      STRONGER claim than the evidence supports. Same for `window_census.window` itself,
#      and for a session where NO payload record carries a `ts_unix_ms`: an unbindable
#      verdict is refused rather than passed vacuously.
#
# The fixture default `config.quiescence` is `NOT VERIFIED (no timeseries supplied)`
# (lib-ws0-fixtures.sh), so this whole branch is UNREACHABLE from the builders as shipped.
# Every case below therefore rewrites the manifest's `config.quiescence` AFTER
# `ws0_pin_session_corpus` stamps it, and drops in its own `quiescence-verdict.json`.
#
# NON-VACUITY: the POSITIVE CONTROL runs FIRST and must exit 0. Without it the four
# refusals could all be firing on some unrelated fixture defect — the wrong-subject shape
# this suite exists to refuse — so each negative below perturbs exactly ONE field of the
# session the control proves healthy.

# A fixed wall clock (2025-06-15T15:06:40Z), never `date`: a window comparison against a
# clock that moves during the run is the wall-clock-race class this repo mechanized a lint
# for. The verdict windows below are expressed as OFFSETS from it, so the covering and
# adjacent cases differ by nothing but their offsets.
Q_TS_MS=1750000000000
Q_TS_PATH="/fixture/box-load.jsonl"

# The rep payloads carry no `ts_unix_ms` as `make_flight_rep` writes them (it is an
# `ignored` field of the loadgen record surface, so the reporter accepted its absence
# before this guard existed). Stamped HERE rather than in the shared builder: the other
# ws0 suites feed the same builder and none of them enters this branch, so widening the
# default would change their fixtures for a property they do not test.
q_stamp_ts() { # q_stamp_ts <session> <ts_unix_ms>
  python3 - "$1" "$2" <<'PY'
import json, pathlib, sys
d, ts = pathlib.Path(sys.argv[1]), int(sys.argv[2])
for pf in sorted(d.glob("flight-*.jsonl")):
    out = []
    for line in pf.read_text().splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        rec["ts_unix_ms"] = ts
        out.append(json.dumps(rec))
    pf.write_text("\n".join(out) + "\n")
PY
}

# ...and its inverse, over EVERY `*.jsonl` in the session dir rather than the flight
# payloads alone: the reporter selects records STRUCTURALLY (any `*.jsonl` record carrying
# the field), so a case asserting "no record carries one" must strip them all or it would
# assert about a filename allowlist instead of about the guard.
q_strip_ts() { # q_strip_ts <session>
  python3 - "$1" <<'PY'
import json, pathlib, sys
for pf in sorted(pathlib.Path(sys.argv[1]).glob("*.jsonl")):
    out = []
    for line in pf.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            out.append(line)
            continue
        if isinstance(rec, dict):
            rec.pop("ts_unix_ms", None)
        out.append(json.dumps(rec))
    pf.write_text("\n".join(out) + "\n")
PY
}

# A verdict of the SHIPPED shape (`ws0_quiescence.window_census_clean`), with the window
# placed by offset and at most one key removed. Composed here rather than produced by
# running `ws0_quiescence.py` because the cases' subject is a verdict the current writer
# CANNOT produce — one missing a field it always records — which no invocation of it yields.
q_write_verdict() { # q_write_verdict <session> <start-offset-s> <end-offset-s> <drop-key|-none->
  python3 - "$1" "$2" "$3" "$4" "$Q_TS_MS" "$Q_TS_PATH" "$REPO_ROOT/scripts/perf" <<'PY'
import datetime, json, pathlib, sys
d = pathlib.Path(sys.argv[1])
off_start, off_end, drop = float(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
base = int(sys.argv[5]) / 1000.0
def iso(t):
    return datetime.datetime.fromtimestamp(
        t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
census = {
    "samples": 41, "competing_samples": 0,
    "coverage_largest_gap_s": 15.0, "coverage_gap_bound_s": 20.0,
    "load1_min": 0.10, "load1_max": 0.20, "load1_mean": 0.15,
    "window": {"start": iso(base + off_start), "end": iso(base + off_end)},
    "timeseries": sys.argv[6], "narrow_census_records": 0,
    "census_breadth": "FULL (competing_count present on every in-window record)",
}
# THE BOUNDARY SAMPLES, WHICH THIS FIXTURE ORIGINALLY OMITTED. The reporter's job-73 F2
# check requires a QUIESCENT verdict to be checkable at BOTH edges of its window, and the
# real writer always records them (see any rig/quiescence-verdict.json: `competing`,
# `competing_count`, `load`). Composing a verdict without them made the reporter refuse a
# COVERING, COMPLETE verdict -- so the POSITIVE CONTROL red, and the four refusal cases got
# their refusal from the boundary precondition instead of from the property each names. A
# refusal-direction test that fires for the wrong reason measures nothing.
verdict = {
    "verdict": "QUIESCENT",
    # THE COMPLETE SHIPPED SHAPE, and the fixture now PROVES it is complete (see below). This
    # block has lagged the reporter's requirements THREE times -- boundary samples missing when
    # job 73 F2 landed, then these nine top-level fields missing when job 75's closed checker
    # landed -- each time surfacing as a pile of mysterious failures in cases about something
    # else. Composed by hand rather than by running the writer because several cases' SUBJECT is
    # a verdict the writer cannot produce (one missing a field it always records).
    "competing_before": 0,
    "competing_after": 0,
    "load1_before": 0.11,
    "load1_after": 0.19,
    "load1_movement": 0.08,
    "load1_after_is_bounded": False,
    # EXACT, not approximate: the evidence checker asserts this BY DERIVATION (job 80 F2), so a
    # paraphrase is refused. The self-validation below caught this the moment the derivation check
    # landed and named the field -- which is what it is for.
    "load1_after_note": ("RECORDED, NOT BOUNDED: load1 is a 1-minute decaying average, so a"
                         " sample taken immediately after a CPU-bound window reads the window's"
                         " own residue. The binding in-window check is the timeseries census."),
    "thresholds": {"max_load1": 2.0, "max_load1_movement": 0.5},
    "before": {"competing": [], "competing_count": 0,
               "load": {"load1": 0.11, "load5": 0.20, "load15": 0.30, "runnable": "1/700"}},
    "after": {"competing": [], "competing_count": 0,
              "load": {"load1": 0.19, "load5": 0.24, "load15": 0.31, "runnable": "1/702"}},
    "window_census": census,
}
# THE FIXTURE VALIDATES ITS OWN BASELINE, so drift fails HERE with a precise message instead of
# as N unrelated red checks. Every negative case below perturbs exactly one field of this
# verdict, so if the baseline is not itself acceptable the whole block is measuring nothing --
# which is what happened twice. Deliberately run BEFORE `drop` is applied: the cases' business
# is an INVALID verdict, this assert's business is that the starting point was VALID.
# The perf dir arrives as an ARGUMENT, not derived from cwd or __file__: this body runs from
# `python3 -` (no __file__) and the suite does not guarantee its working directory.
sys.path.insert(0, sys.argv[7])
from ws0_quiescence_evidence import assert_self_consistent, EvidenceError  # noqa: E402
try:
    assert_self_consistent(verdict, "the fixture's OWN baseline verdict")
except EvidenceError as exc:
    sys.exit(f"FIXTURE DRIFT: {exc}\n"
             "  The composed baseline no longer satisfies the reporter's evidence checker, so"
             " every case in this block would fail for the wrong reason. Update the verdict"
             " composed in q_write_verdict to the current shipped shape.")
# `drop` names ONE key to remove, from the census or from the verdict's top level, so a case
# can perturb either layer with the same helper.
if drop != "-none-":
    if drop in census:
        census.pop(drop)
    elif drop in verdict:
        verdict.pop(drop)
    else:
        sys.exit(f"fixture bug: drop key {drop!r} is in neither the census nor the verdict; "
                 "a case perturbing nothing would pass for the wrong reason")
(d / "quiescence-verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
PY
}

# `judged against <path>`, written OVER the manifest the pin writer just stamped. The
# reporter reads the intent from the manifest, so this is the only way into the branch;
# `run_report_full` deliberately re-stamps the pin on every call (see its own comment),
# which is why these cases run the reporter directly instead of through it.
q_judged_session() { # q_judged_session <session> <corpus>
  make_warm_session "$1"
  rm -f "$1/session-corpus-pin.json"
  ws0_pin_session_corpus "$1" "$2" 1 warm bypass 1
  python3 - "$1" "$Q_TS_PATH" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
rec = json.loads(p.read_text())
rec["config"]["quiescence"] = "judged against " + sys.argv[2]
p.write_text(json.dumps(rec, indent=2) + "\n")
PY
}

# --- (1) THE POSITIVE CONTROL: a covering verdict with every caveat present PASSES ---
# Asserted AFFIRMATIVELY, and in two directions, for the reason review round 1 finding 12
# gave: `rc=0` alone would also hold if the branch were never entered at all, so the
# recorded block must show the coverage assert actually RAN.
d="$TMP/q-covering"; q_judged_session "$d" "$TMP/corpus"
q_stamp_ts "$d" "$Q_TS_MS"
q_write_verdict "$d" -60 60 -none-
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
# The affirmative signal is `results.json`, not stdout: the reporter records the quiescence
# claim in the JSON and prints no summary line for it, and `quiescence_verdict` is populated
# ONLY inside this branch — so its presence, beside the manifest intent it was checked
# against, is what distinguishes "accepted" from "the branch was never entered".
if [ "$rc" -eq 0 ] && python3 - "$d" "$Q_TS_PATH" <<'PY'
import json, pathlib, sys
r = json.loads((pathlib.Path(sys.argv[1]) / "results.json").read_text())
assert r["quiescence"] == "judged against " + sys.argv[2], r["quiescence"]
assert r["quiescence_verdict"] is not None, "the judged branch was never entered"
assert r["quiescence_verdict"]["verdict"] == "QUIESCENT", r["quiescence_verdict"]
PY
then
  pass "a QUIESCENT verdict COVERING the session's flight-rep window is ACCEPTED (rc=0)"
else
  fail "the positive control must pass: a covering, complete verdict (rc=$rc, out: $out)"
fi
if [ "$rc" -eq 0 ] && python3 - "$d" <<'PY'
import json, pathlib, sys
q = json.loads((pathlib.Path(sys.argv[1]) / "results.json").read_text())["quiescence_verdict"]
assert q["covers_flight_rep_window"] is True, q
assert q["flight_rep_records"] >= 1, q
assert "bare-scan" in q["flight_rep_window_source"], q
assert q["judged_window"]["start"] and q["judged_window"]["end"], q
for k in ("in_window_samples", "coverage_largest_gap_s",
          "narrow_census_records", "census_breadth"):
    assert q[k] is not None, (k, q)
PY
then
  pass "the accepted verdict is RECORDED with its four caveats, judged window and coverage flag"
else
  fail "results.json must record the caveats + the coverage binding, not just the verdict"
fi

# --- (2) an ADJACENT window of the SAME timeseries does not certify this session ---
# The exact pre-fix hole: shifted ~20 min earlier, this verdict still names the manifest's
# timeseries and still says QUIESCENT, and every pre-fix check passed on it.
d="$TMP/q-adjacent"; q_judged_session "$d" "$TMP/corpus"
q_stamp_ts "$d" "$Q_TS_MS"
q_write_verdict "$d" -1260 -1140 -none-
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'judged over' <<<"$out" \
   && grep -q "NOT cover this session's FLIGHT-REP window" <<<"$out"; then
  pass "a verdict judged over an ADJACENT window of the same timeseries is REFUSED"
else
  fail "an adjacent-window verdict must be refused naming the judged window (rc=$rc, out: $out)"
fi

# --- (3) a verdict with no `census_breadth` is REFUSED, naming the field ---
# Read with `.get()` before the fix, so the report published QUIESCENT with a `null`
# breadth — i.e. it could not say whether the census saw the full COMPETING_COMMS set or
# rustc/cargo/gate alone, and printed nothing to say so.
d="$TMP/q-nobreadth"; q_judged_session "$d" "$TMP/corpus"
q_stamp_ts "$d" "$Q_TS_MS"
q_write_verdict "$d" -60 60 census_breadth
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -qE 'missing .* required field' <<<"$out" && grep -q 'census_breadth' <<<"$out"; then
  pass "a verdict missing window_census.census_breadth is REFUSED, naming the caveat"
else
  fail "a missing census_breadth must be refused by name (rc=$rc, out: $out)"
fi

# --- (4) a verdict with no `window_census.window` at all is REFUSED ---
# The coverage assert of case 2 has nothing to compare against here, and "cannot be
# compared" must land on the REFUSING side: a window that cannot be COMPUTED is never
# treated as covered.
d="$TMP/q-nowindow"; q_judged_session "$d" "$TMP/corpus"
q_stamp_ts "$d" "$Q_TS_MS"
q_write_verdict "$d" -60 60 window
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -qE 'missing .* required field' <<<"$out" && grep -q 'window_census.window' <<<"$out"; then
  pass "a verdict recording NO window_census.window is REFUSED (coverage unprovable, not assumed)"
else
  fail "an absent window_census.window must be refused by name (rc=$rc, out: $out)"
fi

# --- (5) a session whose payloads carry NO `ts_unix_ms` is REFUSED, not passed vacuously ---
# The other half of case 2's derivation. With no rep timestamp the session's own window is
# unknown, so the verdict cannot be BOUND to it — and an unbindable verdict states nothing
# about this session. Stamped then stripped rather than merely left unstamped, so the case
# keeps testing the guard if the shared builder ever starts writing the field.
d="$TMP/q-nots"; q_judged_session "$d" "$TMP/corpus"
q_stamp_ts "$d" "$Q_TS_MS"; q_strip_ts "$d"
q_write_verdict "$d" -60 60 -none-
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'no rep payload in this session carries a' <<<"$out"; then
  pass "a session whose payloads carry no ts_unix_ms is REFUSED (window unbindable, never assumed)"
else
  fail "an unbindable flight-rep window must be refused (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3248 job 73 F2 — A QUIESCENT *LABEL* IS NOT QUIESCENT *EVIDENCE*
# ==========================================================================
# The reporter used to check that the verdict was well-FORMED and never that its own numbers
# SUPPORTED the word QUIESCENT: a verdict listing competing processes at a boundary, or
# reporting nonzero in-window competing samples, certified the session as long as the fields
# existed. I verified all of this BY HAND when I wrote the fix and did not pin any of it here,
# which is how the fixture's missing boundary samples then went unnoticed until a full gate
# red six checks. A fix verified only by hand is a fix with no regression cover.
#
# Each case perturbs exactly ONE value of the verdict the POSITIVE CONTROL above proves
# healthy, and each asserts a diagnostic naming ITS OWN subject — a refusal from some other
# precondition is the vacuous-pass shape this block exists to refuse.
q_mutate_verdict() { # q_mutate_verdict <session> <python-expr-on-v>
  python3 - "$1" "$2" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1]) / "quiescence-verdict.json"
v = json.loads(path.read_text())
before = json.dumps(v, sort_keys=True)
exec(sys.argv[2], {"v": v})
if json.dumps(v, sort_keys=True) == before:
    sys.exit("fixture bug: mutation changed nothing, so the case would pass for the wrong reason")
path.write_text(json.dumps(v, indent=2) + "\n")
PY
}

q_evidence_case() { # q_evidence_case <label> <expect-substring> <mutation>
  local label="$1" expect="$2" mutation="$3" dd out rc
  dd="$TMP/q-ev-$(printf '%s' "$label" | tr -cd 'a-z0-9')"
  q_judged_session "$dd" "$TMP/corpus"; q_stamp_ts "$dd" "$Q_TS_MS"
  q_write_verdict "$dd" -60 60 -none-
  q_mutate_verdict "$dd" "$mutation"
  out=$(python3 "$REPORT" --dir "$dd" --corpus "$TMP/corpus" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "$expect" <<<"$out"; then
    pass "$label"
  else
    fail "$label: expected non-zero + '$expect' (rc=$rc, out: $out)"
  fi
}

q_evidence_case "a verdict with NO 'before' boundary sample is REFUSED, naming the edge" \
  "before.competing" 'v.pop("before")'
q_evidence_case "a competitor at the BEFORE boundary is REFUSED despite the QUIESCENT label" \
  "before boundary census lists 1 competing" \
  'v["before"]["competing"]=["cc1"]; v["before"]["competing_count"]=1; v["competing_before"]=1'
q_evidence_case "a competitor at the AFTER boundary is REFUSED despite the QUIESCENT label" \
  "after boundary census lists 2 competing" \
  'v["after"]["competing"]=["rustc","ld"]; v["after"]["competing_count"]=2; v["competing_after"]=2'
q_evidence_case "count != len(competing) is REFUSED as a DERIVATION violation" \
  "its own inputs (len(after.competing))" \
  'v["after"]["competing_count"]=3; v["competing_after"]=3'
q_evidence_case "nonzero in-window competing_samples is REFUSED" \
  "in-window sample(s) with a competing process" \
  'v["window_census"]["competing_samples"]=7'
q_evidence_case "a ZERO-sample window is REFUSED (unmeasured, not quiet)" \
  "window_census.samples\` is 0" 'v["window_census"]["samples"]=0'
q_evidence_case "narrow_census_records exceeding samples is REFUSED as impossible" \
  "which is impossible" \
  'wc=v["window_census"]; wc["narrow_census_records"]=999; wc["census_breadth"]="NARROW on 999 of %d record(s): those carry rustc/cargo/gate only, so a short-lived cc1/ld/lld/mold between boundaries would not appear. Stated rather than implied." % wc["samples"]'
q_evidence_case "a sampling gap wider than the verdict's OWN bound is REFUSED" \
  "exceeds its own stated bound" \
  'v["window_census"]["coverage_largest_gap_s"]=99.0'
# ==========================================================================
# #3272 round 20 — the ERROR-CODE breakdown: see test_ws0_error_code_guards.sh
# ==========================================================================
# The round-20 cases (a rep recording `requests_error: 0` beside `error_codes: {"Internal": 1}`;
# the one-site mutation of `ws0_error_codes.py` that MEASURES the pre-fix acceptance; the sum
# invariant shown stronger than the emptiness rule it replaced; the malformed shapes; the absent
# field; the accept direction and the recorded cross-check) moved to
# `scripts/tests/test_ws0_error_code_guards.sh` under the campsite rule — round 20 took this file
# to 1625 lines against the ~1500 test target.
#
# The seam is by SUBJECT, the same one the round-17 split follows. This file asks whether a
# quantity was validly OBSERVED at all; that one asks whether the record's TWO ACCOUNTS OF ITS OWN
# FAILURES agree — a self-consistency question whose oracle is another field of the same record,
# whose domain rules are those of a MAP (a shape no other field on this path has), and whose
# non-vacuity probe mutates a different module. Both suites are wired into the gate's
# `tooling-tests` component and share the builders in `lib-ws0-scan-arm-fixtures.sh`.

# ==========================================================================
# #3272 round 17 — the CELL volume: see test_ws0_cell_volume_guards.sh
# ==========================================================================
# The round-17 cases (a pass returning EVERY ROW with MISSING COLUMNS; an absent per-pass
# `cells`; an over-count refused as an equality failure; the one-site mutation of
# `ws0_collect.py` that measures the pre-fix acceptance; the accept direction and the recorded
# derivation) moved to `scripts/tests/test_ws0_cell_volume_guards.sh` under the campsite rule
# — eighteen review rounds took this file to 1602 lines against the ~1500 test target.
#
# The seam is by SUBJECT, not by size. This file asks whether a quantity was validly OBSERVED
# at all; that one asks whether, given a session whose every quantity IS observed and internally
# consistent, the WORK the figure divides by was actually done. Different oracle
# (`cells_per_row` from the corpus identity, not the row count), different mutation site.
#
# Both suites are wired into the gate's `tooling-tests` component, and the four builders they
# share (`make_corpus`, `make_scan_rep`, `make_flight_rep`, `run_report`) now live in ONE
# definition at `scripts/tests/lib-ws0-scan-arm-fixtures.sh` rather than being copied — the
# drift `lib-ws0-fixtures.sh`'s own header records paying for.


# ==========================================================================
# #3272 finding 3 — the host sysctls: see test_ws0_host_state_guards.sh
# ==========================================================================
# The sysctl capture/mutate/restore cases moved to
# `scripts/tests/test_ws0_host_state_guards.sh` under the campsite rule (test target ~1500
# lines), along the same seam the rig itself follows: `lib-host-state.sh` is the only part
# of the rig that changes anything OUTSIDE its own process tree, and the part whose failure
# is SECURITY-ADJACENT rather than a wrong number. This file's subject is the REPORTER's
# fail-closed paths. Both are wired into the gate's `tooling-tests` component.

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its
# ~103 checks and passed them exits 0 and reports SUCCESS. That is the suite-level
# `0/0` shape this whole issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite)
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file.
# RE-DERIVED BY RUNNING THE SUITE, never estimated from the source. It has moved twice, and the
# second move is the one worth recording: round 17 measured 118 and set the floor at 115; the
# campsite-rule split of the round-17 CELL-VOLUME block to `test_ws0_cell_volume_guards.sh` then
# took 11 of those checks OUT of this file, so a re-run measured 107 and the 115 floor FIRED — a
# floor left pointing at a count that no longer exists is a red on correct code. RE-MEASURED at
# 107 here and 11 there (118 total, fully accounted), so the floor is set below 107. A source-line
# count would understate it either way, because loops multiply.
#
# Round 20 made the same round trip and left the floor where it was, which is worth recording as the
# case where NOT moving it is correct: the round-20 ERROR-CODE block took this file to 126, then
# split to `test_ws0_error_code_guards.sh` and took its 19 checks with it, so a re-run MEASURED 107
# again. The floor is not raised to 126 (the checks left) and not lowered (nothing left besides
# them) — 107 + 19 = 126, fully accounted in the same way.
#
# #3248 added the QUIESCENCE window-coverage/caveat block above: 6 checks, no split, so a re-run
# MEASURED 113 and the floor moves 104 -> 110 by exactly that 6. Raised rather than left alone
# because these checks STAY in this file — the round-17/20 round trips left it unchanged only
# because their checks left with them.
# ==========================================================================
# #3248 job 82 F1 — THE TITLE AND THE `profile` LINE MUST AGREE ABOUT BASELINE-NESS
# ==========================================================================
# Pinned as an AGREEMENT, not as three separate expected strings, per the coordination ruling:
# "when two fields must agree, assert the AGREEMENT, not one field against a constant." The report
# contradicted itself in BOTH directions, one round apart:
#   job 80 F3 -- the TITLE said BASELINE on a profiled run;
#   job 82 F1 -- the `profile` line said "throughput is a baseline" on a non-canonical corpus.
# Fixing the first introduced the second, one line below, in the same commit. A conditional title
# and an unconditional string cannot agree by construction, so the invariant is what gets pinned:
#
#     the title says BASELINE  <=>  the profile line says "throughput is a baseline"
#
# Both directions are exercised, because a one-directional pin is exactly what let the pair drift.
q_baseline_agreement() { # q_baseline_agreement <label> <dir> <expect-baseline:0|1>
  local label="$1" dir="$2" want="$3" out t pl
  out=$(python3 "$REPORT" --dir "$dir" --corpus "$TMP/corpus" 2>&1) || true
  t=$(printf '%s' "$out" | grep -m1 '^====' | grep -c 'SAME-SESSION BASELINE' || true)
  pl=$(printf '%s' "$out" | grep -m1 '^profile ' | grep -c 'throughput is a baseline' || true)
  if [ "$t" != "$pl" ]; then
    fail "$label: the report CONTRADICTS ITSELF — title-baseline=$t profile-baseline=$pl"
  elif [ "$t" != "$want" ]; then
    fail "$label: agreed, but on the WRONG answer (got $t, want $want) — agreement alone is not
       enough: both lines saying the same false thing is still wrong"
  else
    pass "$label: title and profile line AGREE (baseline=$t)"
  fi
}

# The fixture corpus is deliberately NON-canonical, which makes this case EXACTLY the job-82 F1
# scenario: unprofiled, non-baseline. Before the fix the title said NOT A BASELINE while the
# profile line said "throughput is a baseline".
d="$TMP/agree-plain"; q_judged_session "$d" "$TMP/corpus"; q_stamp_ts "$d" "$Q_TS_MS"
q_write_verdict "$d" -60 60 -none-
q_baseline_agreement "unprofiled NON-canonical run (job 82 F1 direction)" "$d" 0

# THE POSITIVE DIRECTION IS NOT PINNED HERE, AND THE REASON IS ITSELF A GUARD WORKING.
# `is_baseline` is RE-DERIVED by the reporter from a live corpus comparison, so a synthetic fixture
# corpus can never yield it. I tried to force it by writing `is_baseline: true` into the pin -- and
# that is precisely what test_ws0_canonical_corpus.sh:332 refuses as self-contradictory ("a
# recorded boolean nobody re-derives cannot publish a smoke corpus as a baseline"). Defeating that
# guard to satisfy this one would have been the trade this whole issue is about.
#
# So the positive direction lives where it can be exercised honestly:
#   * test_ws0_canonical_corpus.sh covers the title's BASELINE/NOT-A-BASELINE branch directly;
#   * verified by hand against the REAL canonical corpus while fixing job 82 F1 -- unprofiled
#     canonical renders title=BASELINE and profile="throughput is a baseline", i.e. AGREE on 1.
# What IS pinned here is the agreement invariant in both directions a hermetic fixture can reach.

d="$TMP/agree-prof"; q_judged_session "$d" "$TMP/corpus"; q_stamp_ts "$d" "$Q_TS_MS"
q_write_verdict "$d" -60 60 -none-
python3 - "$d" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
d = json.loads(p.read_text()); d["config"]["profile"] = "on freq=499"
p.write_text(json.dumps(d))
PY
q_baseline_agreement "PROFILED canonical run (job 80 F3 direction)" "$d" 0

MIN_CHECKS=120
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0-report guards: all $checks checks passed"
  exit 0
fi
echo "ws0-report guards: $fails of $checks check(s) FAILED"
exit 1

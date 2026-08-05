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

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

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
perf_csv() { # perf_csv <path> <cycles> <instructions>
  printf '%s,,cycles,,,,\n%s,,instructions,,,,\n' "$2" "$3" > "$1"
}

# make_corpus <dir> [rows] [data_db_bytes] [bytes_per_row] — a COMPLETE, internally
# consistent identity by default. Callers that need a broken one override the field.
#
# It also writes a real `ws0/events/nb-1-big-Data.db` of exactly `data_db_bytes`
# bytes and records ITS OWN sha256, because the reporter now verifies the recorded
# identity against the bytes actually present (#3272 review B6) — an identity beside
# no Data.db is refused, so a fixture that omits one would fail every case here for
# the wrong reason. The digest is MEASURED from the file, never asserted: `FAKE_SHA`
# survives only for the cases that deliberately record a MALFORMED digest, which are
# refused by `load_corpus_identity` before any byte comparison runs.
make_corpus() {
  local dir="$1" rows="${2:-$CORPUS_ROWS}" bytes="${3:-700000}" bpr="${4:-700.0}"
  mkdir -p "$dir/ws0/events"
  python3 - "$dir" "$rows" "$bytes" "$bpr" <<'PY'
import hashlib, json, os, sys
out, rows, nbytes, bpr = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
data = os.path.join(out, "ws0", "events", "nb-1-big-Data.db")
raw = (bytes(range(256)) * ((nbytes // 256) + 1))[:nbytes]
open(data, "wb").write(raw)
json.dump(
    {"rows": rows, "partitions": 10, "seed": 1, "cells_per_row": 12,
     "data_db_bytes": nbytes, "data_db_sha256": hashlib.sha256(raw).hexdigest(),
     "bytes_per_row": bpr},
    open(os.path.join(out, "corpus-identity.json"), "w"),
)
PY
}

# The INTERLEAVING metadata every rep must carry (#3272 R3): the round, this arm's
# POSITION within that round, and how many arms the round measured. Written by default
# with the bare scan and the Flight arm ALTERNATING by round, exactly as the driver does —
# a fixture with a fixed order would be refused by the rotation check, and correctly so.
# `make_round <dir> <tag> <round> <position> [arms-in-round]`
make_round() {
  printf 'round=%s\nposition=%s\narms_in_round=%s\n' "$3" "$4" "${5:-2}" > "$1/$2.round"
}

# make_scan_rep <dir> <temp> <rep> <prewarm-status|-none->
make_scan_rep() {
  local d="$1" temp="$2" rep="$3" pw="$4" tag="scan-$2-$3"
  cat > "$d/$tag.json" <<EOF
{ "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5 }
EOF
  perf_csv "$d/perf-$tag.csv" 2000000 4000000
  perf_csv "$d/perf-$tag-setup.csv" 100000 200000
  [ "$pw" = "-none-" ] || printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
  make_round "$d" "$tag" "$rep" "$(( (rep % 2 == 1) ? 1 : 2 ))"
}

# make_flight_rep <dir> <temp> <rep> <requests_ok> <rows> <prewarm-status|-none->
make_flight_rep() {
  local d="$1" temp="$2" rep="$3" ok="$4" rows="$5" pw="$6" tag="flight-bypass-$2-$3"
  cat > "$d/$tag.jsonl" <<EOF
{"round":"$tag","requests_ok":$ok,"requests_error":0,"rows_total":$rows,"rows_per_s":250000.0,"duration_s":4.0}
EOF
  perf_csv "$d/perf-$tag.csv" 8000000 16000000
  [ "$pw" = "-none-" ] || printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
  make_round "$d" "$tag" "$rep" "$(( (rep % 2 == 1) ? 2 : 1 ))"
}

# run_report <dir> <corpus> <temps> — prints the reporter's stdout+stderr. Call as
# `out=$(run_report ...); rc=$?`: a command substitution runs in a SUBSHELL, so a
# status the function assigned to a variable would not survive the call.
run_report() {
  python3 "$REPORT" --dir "$1" --corpus "$2" --server-cpus 2,10 \
    --client-cpus 4,12 --reps 1 --temps "$3" --arms bypass \
    --step-duration 45s/1s --scan-passes 1 2>&1
}

# run_report_full <dir> <corpus> <temps> <arms> <reps> <scan-passes> — same, with
# every quantity a caller can get wrong exposed.
run_report_full() {
  python3 "$REPORT" --dir "$1" --corpus "$2" --server-cpus 2,10 \
    --client-cpus 4,12 --reps "$5" --temps "$3" --arms "$4" \
    --step-duration 45s/1s --scan-passes "$6" 2>&1
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
check_driver_reject() { # check_driver_reject <label> <expect-substring> <args...>
  local label="$1" expect="$2"; shift 2
  local out rc2
  out=$(bash "$DRIVER" "$@" 2>&1); rc2=$?
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
SHIM_BIN="$TMP/hermetic-bin"
HERMETIC_CALLS="$TMP/hermetic-calls.txt"
mkdir -p "$SHIM_BIN"
: > "$HERMETIC_CALLS"
for _tool in sudo cargo perf taskset; do
  cat > "$SHIM_BIN/$_tool" <<SHIM
#!/usr/bin/env bash
printf '%s %s\n' "$_tool" "\$*" >> "$HERMETIC_CALLS"
exit 97
SHIM
  chmod +x "$SHIM_BIN/$_tool"
done
unset _tool

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
  : > "$HERMETIC_CALLS"
  out=$(PATH="$SHIM_BIN:$PATH" bash "$DRIVER" --validate-args-only "$@" 2>&1); rc=$?
  calls="$(cat "$HERMETIC_CALLS")"
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
out=$(PATH="$SHIM_BIN:$PATH" bash "$DRIVER" --validate-args-only --corpus /nonexistent-corpus 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "ARGUMENTS OK" <<<"$out" \
   && grep -q "nothing was executed" <<<"$out" \
   && ! grep -q "holds no" <<<"$out" && [ ! -s "$HERMETIC_CALLS" ]; then
  pass "--validate-args-only stops AT the argument boundary (no corpus stat, nothing executed)"
else
  fail "--validate-args-only must exit 0 at the boundary without touching the world (rc=$rc, out: $out, calls: $(cat "$HERMETIC_CALLS"))"
fi
# ...and it must still REFUSE a bad argument: a validate-only mode that accepted
# everything would turn every accept case into a tautology.
out=$(PATH="$SHIM_BIN:$PATH" bash "$DRIVER" --validate-args-only --corpus "$TMP/corpus" --reps 0 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "must be at least 1" <<<"$out"; then
  pass "--validate-args-only still REFUSES an invalid argument (it validates, it does not wave through)"
else
  fail "--validate-args-only must refuse --reps 0 (rc=$rc, out: $out)"
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
# The refusal must NOT fire (the ceiling is cold-scoped)…
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp warm --cold-step-duration 45s 2>&1)
if grep -q "exceeds the" <<<"$out"; then
  fail "--temp warm must not be blocked by the cold-step ceiling (out: $out)"
else
  pass "--temp warm is not blocked by the cold-step ceiling (the ceiling is cold-scoped)"
fi
# …AND the run must be observed getting PAST argument validation, so this cannot pass
# on an unrelated early failure.
expect_driver_accepts "--temp warm with a 45s cold step REACHES a later stage (affirmative)" \
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

# The driver must actually contain the untimed bare-scan prewarm — a reporter that
# merely READS a status file would pass every test above with no prewarm running.
if grep -q 'prewarm_status="skipped-cold-arm"' "$DRIVER" \
  && awk '/^measure_scan\(\)/,/^}/' "$DRIVER" | grep -q 'prewarm.status'; then
  pass "measure_scan itself records a prewarm status (not just the reporter)"
else
  fail "measure_scan must run and record its own prewarm"
fi

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
printf '4000000,,instructions,,,,\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a perf CSV with no 'cycles' line is FATAL, naming the missing event" \
  "no line for required event(s) cycles" "$d" "$TMP/corpus" warm bypass 1 1

d="$TMP/csv-no-instructions"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf '100000,,cycles,,,,\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a perf CSV with no 'instructions' line is FATAL (IPC has no numerator)" \
  "no line for required event(s) instructions" "$d" "$TMP/corpus" warm bypass 1 1

# perf's OWN not-a-value markers. This is the silent-instrument failure in its
# purest form: the line EXISTS, perf EXITED ZERO, and there is no number.
for marker in '<not counted>' '<not supported>'; do
  d="$TMP/csv-marker-$(tr -d ' <>' <<<"$marker")"; mkdir -p "$d"
  make_scan_rep "$d" warm 1 ok
  printf '%s,,cycles,,,,\n200000,,instructions,,,,\n' "$marker" > "$d/perf-scan-warm-1-setup.csv"
  make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
  expect_report_reject "a perf '$marker' value is FATAL (perf did not count it)" \
    "perf did not count it" "$d" "$TMP/corpus" warm bypass 1 1
done

# A corrupt value is a corrupt artifact, not a zero.
d="$TMP/csv-garbage"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf 'NaNsense,,cycles,,,,\n200000,,instructions,,,,\n' > "$d/perf-scan-warm-1-setup.csv"
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
if timeout 15 python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
    --client-cpus 4,12 --reps 99999999999999999999 --temps warm --arms bypass \
    --step-duration 45s/1s --scan-passes 1 >/dev/null 2>&1; rc=$?; [ "$rc" -eq 1 ]; then
  pass "OBSERVED: the reporter TERMINATES (rc=1) on an absurd --reps (pre-cap: timed out)"
else
  fail "the reporter must terminate non-zero on an absurd --reps (rc=$rc; 124 = still hangs)"
fi
expect_report_reject "--scan-passes -1 is REFUSED" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass 1 -1
# The non-numeric selections had the same vacuous-green hole: an empty --temps/--arms
# produced zero measurements and exit 0.
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
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --reps 200000 2>&1)
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
if awk '/^echo "=== issue #3096/,0' "$DRIVER" | grep -q 'PARTIAL MATRIX' \
  && awk '/^echo "=== issue #3096/,0' "$DRIVER" | grep -q 'FULL MATRIX'; then
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
    cat > "$d/$tag.jsonl" <<EOF
{"round":"$tag","requests_ok":1,"requests_error":0,"rows_total":$CORPUS_ROWS,"rows_per_s":250000.0,"duration_s":4.0}
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
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 08s 2>&1)
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
if awk '/^parse_duration_ms\(\)/,/^}/' "$ARGS_LIB" | grep -q '\$((n \* 1000))'; then
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
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp cold \
  --cold-step-duration 2305843009213693956s 2>&1)
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
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 45 2>&1)
if ! grep -q 'too LONG' <<<"$out"; then
  pass "a malformed value is NOT reported as too long (the two causes stay distinct)"
else
  fail "a malformed value must not report a length problem (out: $out)"
fi
# The cause code must survive the call. `if ! cmd; then … $? …` reads 0 — `!` REPLACES
# the status — which silently collapsed both causes back into the format branch.
if awk '/^for _spec in "step-duration/,/^done$/' "$DRIVER" | grep -q '_rc=0 || _rc=\$?'; then
  pass "the duration cause code is captured on its own statement (not through \`if !\`)"
else
  fail "the duration rc must be captured directly; \`if ! cmd\` discards it"
fi
# The largest value inside the cap must still PARSE — the fix is a cap, not a ban on
# big-but-sane durations. 999999999ms is ~11.5 days: over the cold ceiling, so it must
# be refused for its VALUE, which proves it reached the ceiling rather than the parser.
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 999999999ms 2>&1)
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
if awk '/^parse_duration_ms\(\)/,/^}/' "$ARGS_LIB" | grep -q 'DURATION_MAX_DIGITS'; then
  pass "parse_duration_ms caps the digit count (structural)"
else
  fail "parse_duration_ms must cap the digit count before multiplying"
fi

# ==========================================================================
# #3272 finding 3 — the driver RESTORES the host sysctls it mutates
# ==========================================================================
# NON-VACUITY: the pre-fix driver ran
#     sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
# and its ONLY trap was `trap stop_server EXIT`, so every run — success, FATAL or
# Ctrl-C — left the host's perf hardening weakened. `grep -c 'kptr_restrict'` on the
# pre-fix file finds exactly ONE occurrence (the weakening), and none in any trap.
#
# Structural, because the behaviour needs root: the restore must be REGISTERED on
# EXIT **and** on the signals, and it must be part of the same trap that stops the
# server rather than replacing it (a second bare `trap ... EXIT` would silently
# discard the first).
if awk '/^trap /' "$DRIVER" | grep -q 'INT TERM HUP'; then
  pass "the driver traps INT/TERM/HUP, not only EXIT (a Ctrl-C used to skip cleanup)"
else
  fail "the driver must trap INT/TERM/HUP as well as EXIT"
fi
if [ "$(grep -c '^trap ' "$DRIVER")" -eq 1 ]; then
  pass "there is exactly ONE top-level trap registration (a second would discard the first)"
else
  fail "multiple top-level 'trap' lines: a later bare EXIT trap discards the earlier one"
fi
if grep -q 'restore_sysctls' "$DRIVER" \
  && awk '/^on_exit\(\)/,/^}/' "$DRIVER" | grep -q 'restore_sysctls' \
  && awk '/^on_exit\(\)/,/^}/' "$DRIVER" | grep -q 'stop_server'; then
  pass "the single exit handler runs BOTH stop_server and restore_sysctls"
else
  fail "the exit handler must run stop_server AND restore_sysctls"
fi
# The prior values must be CAPTURED BEFORE the mutation, or there is nothing to
# restore to: assert the capture precedes the `sysctl -w` in file order.
cap_line=$(grep -n 'PARANOID_PRIOR=' "$HOST_STATE" | head -1 | cut -d: -f1)
mut_line=$(grep -n 'sysctl -w "\${writes\[@\]}"' "$HOST_STATE" | head -1 | cut -d: -f1)
if [ -n "$cap_line" ] && [ -n "$mut_line" ] && [ "$cap_line" -lt "$mut_line" ]; then
  pass "the prior sysctl values are captured BEFORE the mutation (line $cap_line < $mut_line)"
else
  fail "prior values must be captured before mutating (capture=$cap_line mutate=$mut_line)"
fi
# Both sysctls the driver weakens must be ENROLLED for restore — not just the one in
# the message. The enrollment list is what `restore_sysctls` iterates.
for knob in perf_event_paranoid kptr_restrict; do
  if grep -q "enroll_sysctl kernel.$knob" "$HOST_STATE"; then
    pass "kernel.$knob is enrolled for restore where it is weakened"
  else
    fail "kernel.$knob must be enrolled for restore (the driver weakens it)"
  fi
done
# The restore must be IDEMPOTENT and must never fail the run: it is cleanup, and a
# cleanup that can exit non-zero turns a successful measurement into a failed one.
if awk '/^restore_sysctls\(\)/,/^}/' "$HOST_STATE" | grep -q 'SYSCTLS_MUTATED' \
  && awk '/^restore_sysctls\(\)/,/^}/' "$HOST_STATE" | grep -q '^  return 0$'; then
  pass "restore_sysctls is guarded by a mutated-flag and returns 0 unconditionally"
else
  fail "restore_sysctls must be flag-guarded (idempotent) and end in an explicit return 0"
fi

# ---- BEHAVIOURAL, not merely structural -----------------------------------
# The greps above pin the SHAPE; per #3249 (hardcoding `_PERF_STATE="ok"` survived
# 118/118 tests) shape is not evidence that the thing FIRES. The real restore needs
# root, so the functions are extracted verbatim from the driver and run against a
# RECORDING `sudo` shim: no privileged call ever happens, and the exact
# `sysctl -w` argv the handler would issue is asserted. Hermetic, sub-second.
#
# `sudo_ok` selects which knobs the shim lets through, so the PARTIAL-restore case
# (#3272 review B3) can be driven: `paranoid` = only perf_event_paranoid succeeds.
sysctl_probe() { # sysctl_probe <case> <enrollment-lines> [sudo_ok: all|none|paranoid]
  local case_name="$1" written="$2" sudo_ok="${3:-all}"
  local calls="$TMP/sysctl-calls-$1.txt" out="$TMP/sysctl-out-$1.txt"
  : > "$calls"
  (
    set -uo pipefail
    sudo() {
      printf '%s\n' "$*" >> "$calls"
      case "$sudo_ok" in
        all)      return 0 ;;
        none)     return 1 ;;
        paranoid) [[ "$*" == *perf_event_paranoid* ]] ;;
      esac
    }
    # SOURCED, not re-implemented: this drives the shipped restore_sysctls, so the
    # test and the run can never be different code.
    # shellcheck disable=SC1090
    source "$HOST_STATE"
    SERVER_PID=""
    SYSCTLS_WRITTEN="$written"
    SYSCTLS_MUTATED=1
    case "$case_name" in
      never-mutated) SYSCTLS_MUTATED=0 ;;
      errexit)       set -e ;;   # cleanup must survive errexit
    esac
    # THE RETURN CODE OF `restore_sysctls`, captured on the NEXT statement (#3272
    # review). It used to be read after an intervening `case`, so `$?` was the
    # CASE's status — 0 for every non-`idempotent` case — and the "cleanup cannot
    # fail the run" half of the failing-sudo case measured nothing at all. The
    # second (idempotency) call is issued only after the code is banked.
    # stderr is NOT discarded: the affirmative/warning DIAGNOSTIC is half of what
    # B3 is about, so the probe must be able to read it.
    restore_sysctls
    printf 'RC=%s\n' "$?" > "$TMP/sysctl-rc-$case_name.txt"
    case "$case_name" in
      idempotent) : > "$calls"; restore_sysctls ;;
    esac
  ) >"$out" 2>&1
  cat "$calls"
}
probe_rc()  { cat "$TMP/sysctl-rc-$1.txt"; }
probe_out() { cat "$TMP/sysctl-out-$1.txt"; }

BOTH_KNOBS=$'kernel.perf_event_paranoid=2\nkernel.kptr_restrict=1'

got=$(sysctl_probe restores "$BOTH_KNOBS")
if grep -q 'sysctl -w kernel.perf_event_paranoid=2' <<<"$got" \
  && grep -q 'sysctl -w kernel.kptr_restrict=1' <<<"$got"; then
  pass "OBSERVED: restore_sysctls writes BOTH captured priors back (paranoid=2, kptr=1)"
else
  fail "restore_sysctls must write both captured priors back (recorded: $got)"
fi
# Pre-fix there was no restore at all, so this is the case that could not pass:
# the driver's only sysctl write was the WEAKENING one.
if [ "$(grep -c 'sysctl -w' <<<"$got")" -eq 2 ]; then
  pass "OBSERVED: exactly two restore writes, no stray sysctl mutation"
else
  fail "expected exactly 2 restore writes (recorded: $got)"
fi
# The affirmative line is printed, and it NAMES both knobs — the case the partial
# check below is distinguished from.
if grep -q 'restored host sysctls:.*perf_event_paranoid=2' <<<"$(probe_out restores)" \
  && grep -q 'kptr_restrict=1' <<<"$(probe_out restores)" \
  && ! grep -q 'WARNING' <<<"$(probe_out restores)"; then
  pass "OBSERVED: a FULL restore prints the affirmative line for both knobs and NO warning"
else
  fail "a full restore must print both knobs and no warning (out: $(probe_out restores))"
fi
if [ "$(probe_rc restores)" = "RC=0" ]; then
  pass "OBSERVED: restore_sysctls returns 0 on the success path (measured, not inferred)"
else
  fail "restore_sysctls must return 0 (got $(probe_rc restores))"
fi

got=$(sysctl_probe idempotent "$BOTH_KNOBS")
if [ -z "$got" ]; then
  pass "OBSERVED: a SECOND restore_sysctls call is a no-op (idempotent)"
else
  fail "restore_sysctls must be idempotent (second call recorded: $got)"
fi

got=$(sysctl_probe never-mutated "$BOTH_KNOBS")
if [ -z "$got" ]; then
  pass "OBSERVED: a run that never mutated the knobs issues NO sysctl on exit"
else
  fail "an unmutated run must not sysctl on exit (recorded: $got)"
fi

# A FAILING sudo must neither abort the handler under `set -e` nor stop it trying the
# SECOND knob — the failure mode that would leave kptr_restrict=0 behind forever. The
# rc is now read off `restore_sysctls` itself (see the probe), so the "cannot fail the
# run" half is genuinely measured.
got=$(sysctl_probe errexit "$BOTH_KNOBS" none)
if grep -q 'kernel.kptr_restrict=1' <<<"$got" && [ "$(probe_rc errexit)" = "RC=0" ]; then
  pass "OBSERVED: a FAILING sudo still attempts both knobs and cannot fail the run (rc=0)"
else
  fail "a failing sudo must not orphan the second knob or fail the run (recorded: $got / $(probe_rc errexit))"
fi
# ...and it must say so: a total failure is a WARNING with a complete runnable command,
# never the affirmative line.
out=$(probe_out errexit)
if grep -q 'WARNING' <<<"$out" \
  && grep -q 'sudo sysctl -w kernel.perf_event_paranoid=2 kernel.kptr_restrict=1' <<<"$out" \
  && ! grep -q '^restored host sysctls' <<<"$out"; then
  pass "OBSERVED: a TOTAL restore failure warns with a COMPLETE runnable sysctl command"
else
  fail "a total restore failure must warn with the full command (out: $out)"
fi

# ---- #3272 review B3: a PARTIAL restore must WARN, not report success -------
# NON-VACUITY. The first fix of finding 3 keyed the success/warning split on "was
# ANYTHING restored":
#
#     if [[ "${#restored[@]}" -gt 0 ]]; then echo "restored host sysctls: …"
#     else echo "WARNING: …"; fi
#
# so a PARTIAL restore took the AFFIRMATIVE branch. MEASURED against that code with
# perf_event_paranoid restorable and kptr_restrict not: ONE `sysctl -w`, the line
# `restored host sysctls: perf_event_paranoid=2`, and NO warning — the operator told
# the host was restored while `kptr_restrict=0` was left behind permanently. That is
# finding 3's own defect in narrower form; both directions are asserted here.
got=$(sysctl_probe partial "$BOTH_KNOBS" paranoid)
out=$(probe_out partial)
if [ "$(grep -c 'sysctl -w' <<<"$got")" -eq 2 ]; then
  pass "OBSERVED: a partial restore still ATTEMPTS both knobs (the failure does not stop the loop)"
else
  fail "a partial restore must attempt both knobs (recorded: $got)"
fi
if grep -q 'WARNING' <<<"$out" && grep -q 'kernel.kptr_restrict=1' <<<"$out"; then
  pass "OBSERVED: a PARTIAL restore WARNS, naming the knob left weakened (pre-fix: silent)"
else
  fail "a partial restore must warn and name the unrestored knob (out: $out)"
fi
if grep -q 'sudo sysctl -w kernel.kptr_restrict=1' <<<"$out"; then
  pass "OBSERVED: the partial warning carries a COMPLETE runnable restoration command"
else
  fail "the partial warning must carry a runnable command (out: $out)"
fi
if grep -q 'PARTIAL restore, not a successful one' <<<"$out"; then
  pass "OBSERVED: the partial case says it is PARTIAL (pre-fix it read as a success)"
else
  fail "a partial restore must not read as a success (out: $out)"
fi
# The counted knob must be the one that actually went back — the affirmative half may
# not name a knob the sudo refused.
if grep -q 'restored host sysctls: kernel.perf_event_paranoid=2$' <<<"$out"; then
  pass "OBSERVED: the affirmative half names ONLY the knob that was genuinely restored"
else
  fail "the affirmative line must name only the restored knob (out: $out)"
fi

# ---- B3 ROOT CAUSE: a knob whose prior was not captured is never MUTATED ----
# The reporting fix above is the second half. The first is that the unrestorable case
# must not arise: `kptr_restrict` used to be WRITTEN even when its prior read as `""`
# (an unreadable /proc entry), which is what created a knob with nothing to restore
# to. Driven over the driver's own capture/enrollment functions with an injected
# unreadable path.
if bash -c '
  set -uo pipefail
  # shellcheck disable=SC1090
  source "'"$HOST_STATE"'"
  SYSCTLS_WRITTEN=""; SYSCTLS_MUTATED=0
  # An unreadable path yields rc=1, NOT an empty success — so the caller can branch.
  read_sysctl_prior /nonexistent/kptr_restrict >/dev/null 2>&1 \
    && { echo "an unreadable path returned SUCCESS"; exit 1; }
  # An EMPTY file is also a failed capture: "" is not a value to restore to.
  tmp=$(mktemp); : > "$tmp"
  read_sysctl_prior "$tmp" >/dev/null 2>&1 && { echo "an empty file read as a value"; exit 1; }
  rm -f "$tmp"
  # A readable one yields the value and enrolls exactly one line.
  tmp=$(mktemp); printf "2\n" > "$tmp"
  v=$(read_sysctl_prior "$tmp") || { echo "a readable path failed"; exit 1; }
  [ "$v" = "2" ] || { echo "wrong value: $v"; exit 1; }
  enroll_sysctl kernel.perf_event_paranoid "$v"
  [ "$SYSCTLS_WRITTEN" = "kernel.perf_event_paranoid=2" ] || { echo "bad enrollment: $SYSCTLS_WRITTEN"; exit 1; }
  [ "$SYSCTLS_MUTATED" = "1" ] || { echo "enrollment did not set the mutated flag"; exit 1; }
  rm -f "$tmp"
' >/dev/null 2>&1; then
  pass "OBSERVED: read_sysctl_prior FAILS on an unreadable/empty prior, and enrollment pairs knob+prior"
else
  fail "read_sysctl_prior must fail-closed on an unreadable or empty prior"
fi
# And the driver must WIRE that: the kptr write is inside the successful-capture
# branch, so an unreadable prior leaves the knob alone rather than weakening it.
if awk '/^  if KPTR_PRIOR=/,/^  fi$/' "$HOST_STATE" | grep -q 'kernel.kptr_restrict=0' \
  && awk '/^  if KPTR_PRIOR=/,/^  fi$/' "$HOST_STATE" | grep -q 'left ALONE'; then
  pass "the driver weakens kptr_restrict ONLY inside the successful-capture branch"
else
  fail "the kptr_restrict write must be gated on its prior having been captured"
fi
# An unreadable perf_event_paranoid prior is FATAL rather than a silent weakening: it
# is the knob the measurement REQUIRES, so there is no correct run without it.
if grep -q 'if ! PARANOID_PRIOR=' "$HOST_STATE" \
  && awk '/^  if ! PARANOID_PRIOR=/,/^  fi$/' "$HOST_STATE" | grep -q 'exit 2'; then
  pass "an unreadable perf_event_paranoid prior is FATAL (never weakened unrestorably)"
else
  fail "an unreadable perf_event_paranoid prior must be fatal"
fi

# The signal path end-to-end: a driver-shaped script carrying the driver's OWN
# on_exit/trap wiring must run the restore when it is SIGINTed mid-work. `EXIT`
# alone does not fire for SIGINT while a foreground child is running, which is how
# a Ctrl-C during a 45s perf leg used to skip cleanup entirely.
cat > "$TMP/trap-probe.sh" <<PROBE
set -euo pipefail
MARK="$TMP/trap-fired.txt"
SERVER_PID=""
SYSCTLS_MUTATED=1
PARANOID_PRIOR=2
KPTR_PRIOR=1
stop_server() { :; }
restore_sysctls() { printf 'restored\n' >> "\$MARK"; SYSCTLS_MUTATED=0; }
$(awk '/^on_exit\(\)/,/^}/' "$DRIVER")
$(grep '^trap on_exit' "$DRIVER")
printf 'ready\n' > "$TMP/probe-ready.txt"
sleep 30
PROBE
rm -f "$TMP/trap-fired.txt" "$TMP/probe-ready.txt"
bash "$TMP/trap-probe.sh" >/dev/null 2>&1 &
probe_pid=$!
for _ in $(seq 1 50); do [ -f "$TMP/probe-ready.txt" ] && break; sleep 0.1; done
kill -INT "$probe_pid" 2>/dev/null || true
wait "$probe_pid" 2>/dev/null; probe_rc=$?
if [ -f "$TMP/trap-fired.txt" ]; then
  pass "OBSERVED: the driver's trap wiring runs the restore on SIGINT (rc=$probe_rc)"
else
  fail "a SIGINT must reach restore_sysctls through the driver's trap (rc=$probe_rc)"
fi
# Same wiring, ordinary exit — the handler must not be signal-only.
cat > "$TMP/trap-probe-exit.sh" <<PROBE
set -euo pipefail
MARK="$TMP/trap-fired-exit.txt"
SERVER_PID=""
SYSCTLS_MUTATED=1
stop_server() { :; }
restore_sysctls() { printf 'restored\n' >> "\$MARK"; }
$(awk '/^on_exit\(\)/,/^}/' "$DRIVER")
$(grep '^trap on_exit' "$DRIVER")
exit 7
PROBE
rm -f "$TMP/trap-fired-exit.txt"
bash "$TMP/trap-probe-exit.sh" >/dev/null 2>&1; exit_rc=$?
if [ -f "$TMP/trap-fired-exit.txt" ] && [ "$exit_rc" -eq 7 ]; then
  pass "OBSERVED: the handler also runs on a normal FATAL exit and PRESERVES its code (7)"
else
  fail "the handler must run on a normal exit and preserve the exit code (rc=$exit_rc)"
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0-report guards: all checks passed"
  exit 0
fi
echo "ws0-report guards: $fails check(s) FAILED"
exit 1

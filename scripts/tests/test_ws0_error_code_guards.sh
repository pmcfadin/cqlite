#!/usr/bin/env bash
# Self-test for the WS0 rig's ERROR-CODE CROSS-CHECK: an invariant ASSUMED is an invariant
# UNENFORCED (#3272 round 20).
#
# Split out of `test_ws0_report_guards.sh` under the campsite rule (test target ~1500 lines) when
# this round's cases took that file to 1625. The seam is by SUBJECT, the same one the round-17
# cell-volume split follows.
#
# `test_ws0_report_guards.sh` asks whether a quantity was validly OBSERVED at all: an absent corpus
# identity, a temperature-blind prewarm sentinel, an unobserved perf counter, an out-of-range
# `--reps`, an octal duration.
#
# This file asks whether the record's TWO ACCOUNTS OF ITS OWN FAILURES agree. `error_codes` was
# classified `ignored` because it "must be empty whenever the rep is accepted" — `requests_error`
# must already be zero — and NOTHING ENFORCED THAT, so a record asserting in one field that a
# request failed with an internal error and in another that none did was accepted and published as
# a clean, failure-free scan. That is a distinct subject: its oracle is ANOTHER FIELD OF THE SAME
# RECORD (not a constant, not the session's configuration, not a separate measurement), its domain
# rules are those of a MAP (a shape no other field on this path has), and its non-vacuity probe
# mutates a different module (`ws0_error_codes.py`).
#
# EVERY CHECK BELOW MOVED VERBATIM from `test_ws0_report_guards.sh`, which measured 126 checks with
# this block and 107 without; this suite runs the 19 that left it. Nothing was reworded, no refusal
# was relaxed, and both suites' `MIN_CHECKS` floors were RE-DERIVED BY RUNNING them rather than
# counted from source — a source count understates a floor because loops multiply.
#
# HERMETIC. The reporter is a python3 program driven over synthetic session dirs under $TMPDIR;
# nothing here invokes the measurement driver, so no case can reach `relax_perf_sysctls` (a host
# `sudo sysctl -w`), `cargo build --release` or the measurement loop.
# `scripts/tests/test_ws0_hermeticity.sh` lints that structurally over every `test_ws0_*.sh`, this
# file included, by LOCATION rather than by spelling.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so the
# minimum-check-count floor at the end can see a block that silently never executed
# (#3272 review round 3 nit).
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
# python3 absence is a FAILURE, not a skip (#3272 review B8). A `SKIP` + `exit 0` IS a silent pass:
# the gate's `tooling-tests` component records SUCCESS with none of the checks below having run, and
# the reassuring word is on stdout the gate does not read. python3 is a HARD REQUIREMENT of this rig
# — `ws0_report.py` IS a python3 program — so there is no environment where its absence means "this
# check is not applicable here".
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig:"
  echo "       scripts/perf/ws0_report.py IS a python3 program. So this is a failed check,"
  echo "       not a skip — exiting 0 here would record the gate component as SUCCESS with"
  echo "       0 of its checks having run (#3272 review B8)."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

CORPUS_ROWS=1000

# The shared session artifacts (`perf_csv`, `ws0_make_corpus`, `make_round`, `WS0_SCAN_FIXED`,
# `ws0_pin_session_corpus`, `WS0_PREFLIGHT_BYTES_PER_SCAN`, …).
# shellcheck source=scripts/tests/lib-ws0-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-fixtures.sh"
# ...and the BARE-SCAN-ARM builders this file shares with `test_ws0_report_guards.sh` and
# `test_ws0_cell_volume_guards.sh` (`make_corpus`, `make_scan_rep`, `make_flight_rep`,
# `run_report`). ONE definition, sourced by all three, for the reason `lib-ws0-fixtures.sh`'s own
# header gives: a builder duplicated across suites is a builder that will disagree with itself.
# shellcheck source=scripts/tests/lib-ws0-scan-arm-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-scan-arm-fixtures.sh"

# The corpus every case below is measured against — built ONCE, here.
make_corpus "$TMP/corpus"

# `run_report_full <dir> <corpus> <temps> <arms> <reps> <scan-passes>` and `expect_report_reject`,
# the two helpers the moved cases call. They came from `test_ws0_report_guards.sh`, where the
# configuration is the SUBJECT of ~10 cases; here every case uses one shape (1 warm bypass rep),
# so the manifest is stamped with THIS call's configuration exactly as it is there — a preserved
# neighbour manifest made cases report a configuration they had not set (#3272 F1).
run_report_full() {
  rm -f "$1/session-corpus-pin.json"
  ws0_pin_session_corpus "$1" "$2" "$5" "$3" "$4" "$6"
  python3 "$REPORT" --dir "$1" --corpus "$2" 2>&1
}

# expect_report_reject <label> <expect-substring> <report-args...> — the reporter must exit NON-ZERO
# and say <expect-substring>. Non-zero alone is not enough: a guard that fires with a diagnostic
# naming something else has not been observed.
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

# ==========================================================================
# #3272 round 20 — `error_codes` WAS IGNORED ON AN INVARIANT NOTHING ENFORCED
# ==========================================================================
# `error_codes` was classified `ignored`, and its reason was: "a BREAKDOWN of requests_error, which
# must already be ZERO for the rep to be reported — so this map is empty whenever the rep is
# accepted". The sentence is TRUE OF A WELL-FORMED RECORD and SILENT ABOUT A MALFORMED ONE. The word
# doing the work is "must", and nothing in the reporting path enforced it — so a record asserting in
# one field that a request failed with an internal error and in another that none did was accepted.
#
# The invariant asserted here is the SUM, `sum(error_codes.values()) == requests_error`, not the
# emptiness. That is the producer's own invariant (`StepAgg::record_outcome` increments
# `self.error` and `self.error_codes[code]` on the SAME line), and the difference is measured below:
# a `requests_error: 3` beside a single code counted once is a self-contradictory record an emptiness
# test says nothing about.
ec_d="$TMP/r20-contradiction"; mkdir -p "$ec_d"
make_scan_rep "$ec_d" warm 1 ok
# The healthy rep, then its `error_codes` overwritten with the contradictory map. Written by
# rewriting the ONE field of the builder's own body rather than by spelling a whole new record, so
# the case's only difference from a session that reports cleanly is its subject.
ws0_set_flight_field() { # ws0_set_flight_field <dir> <tag> <json-fragment-to-substitute-for-error_codes>
  local f="$1/$2.jsonl"
  python3 - "$f" "$3" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
anchor = '"error_codes":{}'
if s.count(anchor) != 1:
    raise SystemExit(f"expected exactly one {anchor} in the fixture body, found {s.count(anchor)}")
p.write_text(s.replace(anchor, sys.argv[2]))
PY
}
make_flight_rep "$ec_d" warm 1 1 "$CORPUS_ROWS" ok
ws0_set_flight_field "$ec_d" flight-bypass-warm-1 '"error_codes":{"Internal":1}'
expect_report_reject "OBSERVED (round 20): a rep recording requests_error 0 beside error_codes {\"Internal\":1} is REFUSED — the record contradicts itself about whether any request failed" \
  'breakdown sums to 1' "$ec_d" "$TMP/corpus" warm bypass 1 1
# The refusal must state WHAT the contradiction costs, not merely that two numbers differ: an
# operator acts on "neither field can be reported", not on a mismatch report. Same property round
# 14's F1 asserts for `round`, and the reason the CONSEQUENCE is a per-field element of the table.
ec_out=$(run_report_full "$ec_d" "$TMP/corpus" warm bypass 1 1)
if grep -q 'CONTRADICTS ITSELF' <<<"$ec_out" && grep -q 'neither field can be reported' <<<"$ec_out"; then
  pass "round 20: the contradiction refusal states the MEASUREMENT consequence (neither field is reportable), not just a mismatch"
else
  fail "round 20: the refusal must name what the contradiction costs (out: $(head -3 <<<"$ec_out"))"
fi
if [ ! -e "$ec_d/results.json" ]; then
  pass "round 20: no results.json is written for the self-contradictory session"
else
  fail "round 20: a refused run must not leave a results.json behind"
fi
# NON-VACUITY, MEASURED — the PRE-FIX reporter is RUN over this exact artifact, not restated. The
# defect was that NOTHING READ THE FIELD, so it is reproduced as a ONE-SITE MUTATION of a COPY of the
# shipped module: the cross-check call is removed and everything else stands. One site rather than a
# wholesale revert, for round 14 F2's reason — reverting every site would be a second implementation
# whose fidelity is a claim about my own re-derivation rather than a measurement.
ec_pre="$TMP/r20-prefix-tree"; rm -rf "$ec_pre"; mkdir -p "$ec_pre"
cp -R "$REPO_ROOT/scripts/perf" "$ec_pre/perf"
# Mutated in the CHECKER's body rather than at the call site, deliberately: the pre-fix state is
# "nothing reads `error_codes`", and a checker that returns before touching the record reproduces
# exactly that while leaving the production WIRING — the call, and the keys it spreads into the rep —
# untouched. Returning the EMPTY MAPPING is what makes it faithful: pre-fix the rep carried no
# breakdown keys at all, and spreading `{}` contributes none, so results.json and the printed report
# are byte-for-byte the pre-fix ones.
if python3 - "$ec_pre/perf/ws0_error_codes.py" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
anchor = '    source, why, consequence = CROSS_CHECKED_COUNTERS["error_codes"]\n'
if s.count(anchor) != 1:
    raise SystemExit("could not locate the error_codes cross-check body to mutate, so this "
                     "non-vacuity probe would be measuring UNMODIFIED code and would read as a "
                     "pass having reverted nothing")
s = s.replace(anchor, "    return {}  # the PRE-FIX state: error_codes is never read\n" + anchor)
p.write_text(s)
print("mutated the probe copy: the error_codes cross-check returns without reading the field")
PY
then
  pass "round 20 NON-VACUITY: the mutation (nothing reads \`error_codes\`) was really applied to the probe copy"
else
  fail "round 20: the pre-fix mutation could not be applied, so the probe below would measure nothing"
fi
# The SAME contradictory session, through the mutated reporter. It must EXIT 0 and PUBLISH the
# figure, and that is the finding: a record naming an internal error, reported as failure-free.
ec_pre_out=$(python3 "$ec_pre/perf/ws0_report.py" --dir "$ec_d" --corpus "$TMP/corpus" 2>&1)
ec_pre_rc=$?
if [ "$ec_pre_rc" -eq 0 ] && grep -q 'flight do_get (bypass requested)' <<<"$ec_pre_out" \
   && grep -q '250 rows/s' <<<"$ec_pre_out"; then
  pass "round 20 NON-VACUITY (MEASURED): with nothing reading \`error_codes\`, the reporter ACCEPTS the contradictory rep and PUBLISHES its 250 rows/s as this arm's figure"
else
  fail "round 20: the unmutated-reporter must ACCEPT the contradictory session and publish its rows/s, else the refusal above closed nothing (rc=$ec_pre_rc, out: $(head -8 <<<"$ec_pre_out"))"
fi
# ...and the failing code is INVISIBLE in that output: `Internal` appears NOWHERE. Stronger than
# "unchecked" — a report that named the discrepancy would at least be honest about it.
if ! grep -q 'Internal' <<<"$ec_pre_out" && ! grep -qi 'error_codes' <<<"$ec_pre_out"; then
  pass "round 20 NON-VACUITY: that accepted report never names the failing code at all (the contradiction was INVISIBLE, not merely unchecked)"
else
  fail "round 20: the pre-fix report must not name the failing code (out: $(grep -i 'internal\|error_codes' <<<"$ec_pre_out" | head -3))"
fi
# ...and the MUTANT MUST NOT BE UNIFORMLY BROKEN: it still refuses an OBSERVED non-zero
# `requests_error`, so what the probe measured is the loss of the CROSS-CHECK rather than a copy that
# validates nothing.
ec_mut_d="$TMP/r20-mutant-control"; mkdir -p "$ec_mut_d"
make_scan_rep "$ec_mut_d" warm 1 ok
make_flight_rep "$ec_mut_d" warm 1 1 "$CORPUS_ROWS" ok
ws0_set_flight_field "$ec_mut_d" flight-bypass-warm-1 '"error_codes":{"Internal":4}'
python3 - "$ec_mut_d/flight-bypass-warm-1.jsonl" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
assert s.count('"requests_error":0') == 1, s
p.write_text(s.replace('"requests_error":0', '"requests_error":4'))
PY
ws0_pin_session_corpus "$ec_mut_d" "$TMP/corpus" 1 warm bypass 1
if ec_mut_out=$(python3 "$ec_pre/perf/ws0_report.py" --dir "$ec_mut_d" --corpus "$TMP/corpus" 2>&1); then
  fail "round 20: the mutant accepted a rep with 4 OBSERVED failed requests, so it lost more than the cross-check"
else
  if grep -q 'had 4 failed request' <<<"$ec_mut_out"; then
    pass "round 20 NON-VACUITY: the mutant still REFUSES an observed non-zero requests_error — it lost exactly the cross-check, not its whole validation"
  else
    fail "round 20: the mutant must still refuse a non-zero requests_error (out: $(head -4 <<<"$ec_mut_out"))"
  fi
fi
# THE SUM IS STRONGER THAN THE EMPTINESS, MEASURED. `requests_error: 3` beside one code counted once
# is a SELF-CONTRADICTORY record at a NON-ZERO count: the emptiness rule the old reason stated
# ("empty whenever requests_error is 0") is silent about it, and it is refused HERE for the
# contradiction — a diagnostic about a CORRUPT ARTIFACT, distinct from the failing-server refusal the
# non-zero count alone would produce.
ec_ne="$TMP/r20-nonzero-disagree"; mkdir -p "$ec_ne"
make_scan_rep "$ec_ne" warm 1 ok
make_flight_rep "$ec_ne" warm 1 1 "$CORPUS_ROWS" ok
ws0_set_flight_field "$ec_ne" flight-bypass-warm-1 '"error_codes":{"Internal":1}'
python3 - "$ec_ne/flight-bypass-warm-1.jsonl" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
assert s.count('"requests_error":0') == 1, s
p.write_text(s.replace('"requests_error":0', '"requests_error":3'))
PY
ec_ne_out=$(run_report_full "$ec_ne" "$TMP/corpus" warm bypass 1 1); ec_ne_rc=$?
if [ "$ec_ne_rc" -ne 0 ] && grep -q 'had 3 failed request' <<<"$ec_ne_out"; then
  pass "round 20: a requests_error of 3 whose breakdown sums to 1 is REFUSED (the non-zero count fires first — an operator sees the failing server before the corrupt artifact)"
else
  fail "round 20: a non-zero requests_error must still be refused naming the count (rc=$ec_ne_rc, out: $(head -3 <<<"$ec_ne_out"))"
fi
# ...and the SUM CHECK ITSELF sees that disagreement, asserted at the checker rather than through the
# reporter, because the zero-required refusal above legitimately fires first on this record. Without
# this the "stronger than emptiness" claim would rest on a case the reporter never reaches.
if ec_sum_out=$(cd "$REPO_ROOT/scripts/perf" && python3 - <<'PY' 2>&1
import sys
sys.path.insert(0, ".")
from ws0_loadgen_record import check_error_code_breakdown
from ws0_validate import Invalid
# requests_error 3, breakdown summing to 1 — a disagreement at a NON-ZERO count, which the
# emptiness rule ("empty whenever requests_error is 0") is entirely silent about.
try:
    check_error_code_breakdown("t", {"error_codes": {"Internal": 1}}, 3)
except Invalid as exc:
    print(f"REFUSED: {exc}")
    raise SystemExit(0)
raise SystemExit("a breakdown summing to 1 beside requests_error 3 was ACCEPTED")
PY
); then
  if grep -q 'breakdown sums to 1' <<<"$ec_sum_out" && grep -q 'requests_error` = 3' <<<"$ec_sum_out"; then
    pass "round 20: the SUM invariant refuses a NON-ZERO disagreement (requests_error 3 vs a breakdown of 1) — which an emptiness check cannot see at all"
  else
    fail "round 20: the non-zero disagreement must be refused naming both numbers (out: $ec_sum_out)"
  fi
else
  fail "round 20: the sum check must refuse a non-zero disagreement (out: $ec_sum_out)"
fi
# THE MALFORMED SHAPES, each refused by name. A value this reporter cannot sum is not a value it may
# ignore — the same rule the domain validators apply to a counter.
#   * a NON-MAP (a list, a string, a number) is not the BTreeMap<String, u64> the loadgen writes;
#   * a NEGATIVE count matters specifically because summing one CANCELS a positive sibling, so
#     {"A":2,"B":-2} sums to 0 and would satisfy a clean `requests_error: 0`;
#   * a FRACTIONAL count would be TRUNCATED by a bare int() into agreement with a clean total;
#   * a BOOLEAN because `int(True)` is 1.
ec_i=0
for ec_bad in '"error_codes":[]' '"error_codes":"none"' '"error_codes":7' \
              '"error_codes":{"Internal":-1}' '"error_codes":{"A":2,"B":-2}' \
              '"error_codes":{"Internal":0.9}' '"error_codes":{"Internal":true}'; do
  ec_i=$((ec_i + 1))
  ec_bd="$TMP/r20-malformed-$ec_i"; mkdir -p "$ec_bd"
  make_scan_rep "$ec_bd" warm 1 ok
  make_flight_rep "$ec_bd" warm 1 1 "$CORPUS_ROWS" ok
  ws0_set_flight_field "$ec_bd" flight-bypass-warm-1 "$ec_bad"
  ec_bout=$(run_report_full "$ec_bd" "$TMP/corpus" warm bypass 1 1); ec_brc=$?
  if [ "$ec_brc" -ne 0 ] && grep -q 'error_codes' <<<"$ec_bout" \
     && ! grep -q 'Traceback' <<<"$ec_bout"; then
    pass "round 20: a malformed \`$ec_bad\` is a NAMED refusal, not a traceback and not a skipped comparison"
  else
    fail "round 20: $ec_bad must be refused naming error_codes without a traceback (rc=$ec_brc, out: $(head -3 <<<"$ec_bout"))"
  fi
done
# ABSENT is an ERROR, never an assumed empty map — the AC3 rule applied to a cross-checked field.
# `rec.get("error_codes", {})` would make the check pass precisely when the record is silent.
ec_ad="$TMP/r20-absent"; mkdir -p "$ec_ad"
make_scan_rep "$ec_ad" warm 1 ok
make_flight_rep "$ec_ad" warm 1 1 "$CORPUS_ROWS" ok
python3 - "$ec_ad/flight-bypass-warm-1.jsonl" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
assert s.count(',"error_codes":{}') == 1, s
p.write_text(s.replace(',"error_codes":{}', ''))
PY
expect_report_reject "round 20: an ABSENT \`error_codes\` is FATAL — a breakdown not observed cannot be asserted empty (never an assumed default)" \
  'carries no `error_codes`' "$ec_ad" "$TMP/corpus" warm bypass 1 1
# THE ACCEPT DIRECTION, so none of the above is a guard that reds unconditionally: an OBSERVED EMPTY
# map beside a clean count reports, and the cross-check is RECORDED so a reader can see it ran
# rather than assume it did.
ec_ok="$TMP/r20-accept"; mkdir -p "$ec_ok"
make_scan_rep "$ec_ok" warm 1 ok
make_flight_rep "$ec_ok" warm 1 1 "$CORPUS_ROWS" ok
ec_ok_out=$(run_report_full "$ec_ok" "$TMP/corpus" warm bypass 1 1); ec_ok_rc=$?
if [ "$ec_ok_rc" -eq 0 ] && grep -q 'flight do_get (bypass requested)' <<<"$ec_ok_out"; then
  pass "round 20: an OBSERVED EMPTY error_codes beside requests_error 0 is ACCEPTED (the fix demands observation, not rejection of the key)"
else
  fail "round 20: a healthy rep must be accepted (rc=$ec_ok_rc, out: $(head -4 <<<"$ec_ok_out"))"
fi
if python3 - "$ec_ok/results.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
fl = next(m for m in r["measurements"] if m["arm"] != "bare_scan")
rep = fl["reps"][0]
assert rep["error_codes"] == {}, rep["error_codes"]
assert rep["error_codes_sum"] == 0, rep["error_codes_sum"]
assert rep["requests_error"] == 0, rep["requests_error"]
src = rep["error_codes_source"]
assert "CROSS-CHECKED" in src, src
assert "record_outcome" in src, src
PY
then
  pass "round 20: results.json records the breakdown, its SUM, the count it was compared against, and states the comparison was CROSS-CHECKED"
else
  fail "round 20: results.json must record that the cross-check ran, not merely that the rep was accepted"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a `$(...)`
# whose command vanished, a `for` over an empty list — LOWERS the check count and registers NO
# failure. The gate reads only the exit code, so a suite that ran 3 of its checks and passed them
# exits 0 and reports SUCCESS. That is the suite-level `0/0` shape this whole issue is about, one
# level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite) and far
# above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts what actually RAN
# rather than what is written in the file.
# RE-DERIVED BY RUNNING THIS SUITE, never estimated from the source: this file's 19 checks were
# MEASURED by running it after the split, and the floor is set below that. A source-line count
# understates a floor because loops multiply — the malformed-shape loop below is SEVEN checks from
# three written lines, and an earlier split on this branch understated a floor by 29 that way.
MIN_CHECKS=18
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 error-code guards: all $checks checks passed"
  exit 0
fi
echo "ws0 error-code guards: $fails of $checks check(s) FAILED"
exit 1

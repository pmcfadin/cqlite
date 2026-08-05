#!/usr/bin/env bash
# Self-test for the WS0 reporting path's NO-FABRICATED-VALUE rule (issue #3272 AC3,
# review round 1).
#
# A third file beside test_ws0_report_guards.sh (the reporter's fail-closed paths)
# and test_ws0_cpu_pinning_guards.sh (the measurement apparatus), for one subject:
#
#     A COUNTER OR VERDICT THAT WAS NOT OBSERVED IS AN ERROR, NEVER A DEFAULT.
#
# Round 1's review found the rule stated in the module docstring and then violated
# five times in the same file, each time by an idiom that reads as harmless:
#
#   1. `int(rec.get("requests_error", 0)) > 0` — the "no failed requests" refusal
#      rested on a FABRICATED 0. A step record with no `requests_error` key was
#      reported CLEAN with the error count never measured. Its sibling
#      `requests_ok` was already `None`-checked and fatal; the two disagreed.
#   2. `block.get("prewarm_all_ok", True)` — a VERDICT-carrying key defaulted to the
#      PERMISSIVE value, so a block that lost the verdict suppressed the warning by
#      ABSENCE.
#   3. `(hi - lo) / med * 100.0 if med else 0.0` — a zero median printed
#      `spread 0.0%`, i.e. the DEGENERATE series described as the TIGHTEST one.
#   4. `scan_rps / fl_rps if fl_rps else float("inf")` — a Flight arm that measured
#      nothing published `inf x`, the most flattering possible reading of the arm
#      under study.
#   5. `rec = records[-1]` — every earlier step record in a rep's JSONL was SILENTLY
#      DROPPED, so one step's rows could be published as the whole rep while the
#      others sat unread on disk.
#
# It also covers the corpus-identity BYTE verification (round 1, B6): the recorded
# size and sha256 are now compared against the `Data.db` actually measured, so stale
# metadata beside different bytes can no longer misidentify the corpus.
#
# Every case carries the MEASURED pre-fix behaviour (`git show HEAD~:…` at the
# review commit), because per #3249 a guard never observed firing is not evidence.
#
# Hermetic: synthetic session dirs, synthetic perf CSVs, and a synthetic multi-byte
# `Data.db` whose real sha256 is computed with python3's hashlib. No cargo, perf,
# sudo, corpus, network or root.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"
# Where the shared `strip_prose` lives (#3272 round 3 nit): ONE implementation, imported
# by the assertion AND by both of its non-vacuity probes.
TESTS_DIR="$REPO_ROOT/scripts/tests"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so
# the minimum-check-count floor at the end can see a block that silently never executed
# (#3272 review round 3 nit).
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
# python3 is a HARD REQUIREMENT of this rig — ws0-baseline.sh refuses to run without
# it — so its absence is a FAILURE, not a skip. A `exit 0` here would record the gate
# component as SUCCESS with none of the checks below having run, which is the vacuous
# green this whole file exists to refuse (#3272 review, B8).
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

CORPUS_ROWS=1000

# --------------------------------------------------------------------------
# Fixture builders
# --------------------------------------------------------------------------
# `perf_csv`, `make_corpus` and `make_round` are SHARED with
# `test_ws0_report_guards.sh` (scripts/tests/lib-ws0-fixtures.sh): they were identical in
# both files, and `make_round` gained a `monotonic_ns` field this round which had to be
# edited in two places — exactly the drift a shared builder removes. The `make_*_rep`
# builders below stay HERE because their signatures are specific to this file's subject
# (the flight JSONL is passed VERBATIM, so a case can omit a key or supply two records).
# shellcheck source=scripts/tests/lib-ws0-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-fixtures.sh"

# This file's corpora are deliberately SMALL (4 KiB of Data.db). The byte verification must
# work at ANY size — the real corpus is 2.8 GB and a test may not write one — and this file
# builds a corpus per case, so the size is the one thing worth keeping local.
FIXTURE_DATA_DB_BYTES=4096
make_corpus() { ws0_make_corpus "$1" "${2:-$CORPUS_ROWS}" "$FIXTURE_DATA_DB_BYTES" "${3:-}"; }

make_scan_rep() { # make_scan_rep <dir> <temp> <rep> <prewarm>
  local d="$1" tag="scan-$2-$3"
  cat > "$d/$tag.json" <<EOF
{ "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5 }
EOF
  perf_csv "$d/perf-$tag.csv" 2000000 4000000
  perf_csv "$d/perf-$tag-setup.csv" 100000 200000
  printf '%s\n' "$4" > "$d/$tag.prewarm.status"
  # The driver's alternation, from the shared helper so the two files cannot spell it
  # differently (a fixture whose positions do not alternate is refused by the rotation
  # check — correctly, but diagnosed as a rotation failure rather than a fixture mistake).
  make_round "$d" "$tag" "$3" "$(ws0_alternating_position "$3" scan)"
}

# make_flight_rep <dir> <temp> <rep> <prewarm> <jsonl-body>
# The JSONL body is given VERBATIM so a case can omit a key or supply two records.
make_flight_rep() {
  local d="$1" tag="flight-bypass-$2-$3"
  printf '%s\n' "$5" > "$d/$tag.jsonl"
  perf_csv "$d/perf-$tag.csv" 8000000 16000000
  printf '%s\n' "$4" > "$d/$tag.prewarm.status"
  # ...and the flight arm takes the OTHER position, mirroring the driver.
  make_round "$d" "$tag" "$3" "$(ws0_alternating_position "$3" flight)"
}

GOOD_FLIGHT='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'

# make_session <dir> <flight-jsonl> — a complete one-warm-rep session dir.
make_session() {
  mkdir -p "$1"
  make_scan_rep "$1" warm 1 ok
  make_flight_rep "$1" warm 1 ok "$2"
}

run_report() { # run_report <dir> <corpus> [extra args…]
  local d="$1" c="$2"; shift 2
  python3 "$REPORT" --dir "$d" --corpus "$c" --server-cpus 2,10 \
    --client-cpus 4,12 --reps 1 --temps warm --arms bypass \
    --step-duration 45s/1s --scan-passes 1 "$@" 2>&1
}

# expect_reject <label> <expect-substring> <dir> <corpus> [extra…]
expect_reject() {
  local label="$1" expect="$2"; shift 2
  local out rc
  out=$(run_report "$@"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "$expect" <<<"$out"; then
    pass "$label"
  else
    fail "$label: expected non-zero + '$expect' (rc=$rc, out: $out)"
  fi
}

make_corpus "$TMP/corpus"

# ==========================================================================
# The POSITIVE CONTROL, first. Every rejection below is only evidence if the
# unperturbed fixture is ACCEPTED — a reporter hardcoded to refuse everything
# would satisfy the whole file.
# ==========================================================================
d="$TMP/happy"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "ratio bare/flight" <<<"$out"; then
  pass "positive control: the unperturbed session is ACCEPTED and prints its ratio"
else
  fail "positive control: the happy path must succeed (rc=$rc, out: $out)"
fi

# ==========================================================================
# 1 — `requests_error` is OBSERVED, never a fabricated 0
# ==========================================================================
# NON-VACUITY, measured against the pre-fix reporter (`ws0_report.py:298` at the
# review commit, `int(rec.get("requests_error", 0)) > 0`): a step record carrying
# `requests_ok` but NO `requests_error` key at all exited **0** and printed a full
# five-line report — the "no failed requests" refusal never having looked at a
# number. The identical record is refused below.
NO_ERR_KEY='{"round":"r","requests_ok":1,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/no-error-key"; make_session "$d" "$NO_ERR_KEY"
expect_reject "an ABSENT requests_error is FATAL (never a fabricated 0)" \
  "carries no \`requests_error\`" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "never a" <<<"$out" && grep -q "fabricated 0" <<<"$out"; then
  pass "the refusal states the rule it enforces (a 0 may not stand in for a counter)"
else
  fail "the requests_error refusal must state the no-fabricated-0 rule (out: $out)"
fi
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written when requests_error was not observed"
else
  fail "a refused run must not leave a results.json behind"
fi
# An UNPARSEABLE value is a corrupt counter, not a zero either.
BAD_ERR='{"round":"r","requests_ok":1,"requests_error":"none","rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/bad-error-key"; make_session "$d" "$BAD_ERR"
expect_reject "an UNPARSEABLE requests_error is FATAL (corrupt, not 0)" \
  "unparseable \`requests_error\`" "$d" "$TMP/corpus"
# And a real non-zero error count is still refused, naming it — the guard the
# fabricated default was standing in for must still work.
REAL_ERR='{"round":"r","requests_ok":1,"requests_error":4,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/real-errors"; make_session "$d" "$REAL_ERR"
expect_reject "an OBSERVED non-zero requests_error is refused, naming the count" \
  "had 4 failed request" "$d" "$TMP/corpus"
# --- R6: A NEGATIVE counter is CORRUPT, not a clean zero --------------------
# NON-VACUITY, measured against the round-1 reporter (`if errors > 0`): a step record
# carrying `requests_error: -3` exited **0** and printed the full report, with the rep
# counted as having NO failed requests. Only the POSITIVE half of "not zero" was tested,
# so every negative value inherited the PERMISSIVE branch — the same fabricated-zero
# defect as the `.get("requests_error", 0)` that branch had just replaced, arrived at from
# the other side. `-3` is not "fewer than no errors"; it is a counter that cannot have been
# validly observed.
NEG_ERR='{"round":"r","requests_ok":1,"requests_error":-3,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/neg-error"; make_session "$d" "$NEG_ERR"
expect_reject "a NEGATIVE requests_error is FATAL (pre-fix: counted as ZERO errors)" \
  "not a possible count" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "CORRUPT artifact, not a clean zero" <<<"$out" \
  && grep -q "used to be \`if errors > 0\`" <<<"$out"; then
  pass "the refusal names the shape (a `> 0` test where `== 0` was meant)"
else
  fail "the negative-counter refusal must name the defect shape (out: $out)"
fi
# The same audit on every OTHER counter comparison in the reporting path — a `> 0`/`== 0`
# where the property is "a valid observation" is one class, not one line.
NEG_ROWS='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":-1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/neg-rows"; make_session "$d" "$NEG_ROWS"
expect_reject "a NEGATIVE rows_total is FATAL (it is a denominator; == 0 alone missed it)" \
  "not a measurement" "$d" "$TMP/corpus"
NEG_RPS='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":-250000.0,"duration_s":4.0}'
d="$TMP/neg-rps"; make_session "$d" "$NEG_RPS"
expect_reject "a NEGATIVE rows_per_s is FATAL (spread() only checks the MEDIAN)" \
  "not a positive finite rate" "$d" "$TMP/corpus"
# ...and a NON-FINITE one, which would propagate into every derived figure as a printable
# number standing in for an absent measurement.
for bad in Infinity NaN; do
  d="$TMP/nonfinite-$bad"
  make_session "$d" "{\"round\":\"r\",\"requests_ok\":1,\"requests_error\":0,\"rows_total\":1000,\"rows_per_s\":$bad,\"duration_s\":4.0}"
  expect_reject "a $bad rows_per_s is FATAL (not a rate)" \
    "not a positive finite rate" "$d" "$TMP/corpus"
done
# The BARE-SCAN denominator, both halves: a negative row count and a degenerate timing
# window. The latter used to be a `ZeroDivisionError` TRACEBACK rather than a refusal —
# the only degenerate case in the file without a stated cause, and a traceback names the
# DIVISION rather than the artifact (#3272 review round 2 nit).
d="$TMP/scan-neg-rows"; make_session "$d" "$GOOD_FLIGHT"
printf '{ "rows_denominator": -5, "timed_scan_secs": 2.0, "setup_secs": 0.5 }\n' > "$d/scan-warm-1.json"
expect_reject "a NEGATIVE bare-scan rows_denominator is FATAL" \
  "not a measurement" "$d" "$TMP/corpus"
d="$TMP/scan-zero-secs"; make_session "$d" "$GOOD_FLIGHT"
printf '{ "rows_denominator": 1000, "timed_scan_secs": 0.0, "setup_secs": 0.5 }\n' > "$d/scan-warm-1.json"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no rows/s for a measurement window that is zero" <<<"$out" \
  && ! grep -q "ZeroDivisionError\|Traceback" <<<"$out"; then
  pass "a ZERO timed_scan_secs is a NAMED refusal, not a ZeroDivisionError traceback"
else
  fail "a zero measurement window must be refused by name, not raise (rc=$rc, out: $out)"
fi
for bad in -1.0 Infinity NaN; do
  d="$TMP/scan-secs-$bad"; make_session "$d" "$GOOD_FLIGHT"
  printf '{ "rows_denominator": 1000, "timed_scan_secs": %s, "setup_secs": 0.5 }\n' "$bad" \
    > "$d/scan-warm-1.json"
  expect_reject "a $bad timed_scan_secs is FATAL (not a measurement window)" \
    "zero, negative, or not finite" "$d" "$TMP/corpus"
done

# An explicit ZERO is accepted: the fix is "observe it", not "reject the key".
ZERO_ERR='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/zero-errors"; make_session "$d" "$ZERO_ERR"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "an OBSERVED requests_error of 0 is accepted (the fix demands observation, not absence)"
else
  fail "requests_error=0 must be accepted (rc=$rc, out: $out)"
fi

# ==========================================================================
# 2 — every step record is consumed; none is silently dropped
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `rec = records[-1]`: a rep JSONL whose
# FIRST line recorded 9 failed requests over a 37-row partial scan and whose SECOND
# was clean exited **0** and published the clean line's 250,000 rows/s — the failing
# step present on disk, unread and unmentioned. Both the error count AND the
# whole-corpus assert were evaded by a record the reporter never looked at.
d="$TMP/two-records"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
{
  printf '%s\n' '{"round":"ramp-1","requests_ok":1,"requests_error":9,"rows_total":37,"rows_per_s":99.0,"duration_s":1.0}'
  printf '%s\n' "$GOOD_FLIGHT"
} > "$d/flight-bypass-warm-1.jsonl"
perf_csv "$d/perf-flight-bypass-warm-1.csv" 8000000 16000000
printf 'ok\n' > "$d/flight-bypass-warm-1.prewarm.status"
expect_reject "a rep carrying TWO step records is REFUSED (the earlier one used to be dropped)" \
  "carries 2 step records" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "ramp-1" <<<"$out"; then
  pass "the refusal NAMES the rounds present, so the operator sees what was dropped"
else
  fail "the multi-record refusal must name the rounds it found (out: $out)"
fi

# ==========================================================================
# 3 — `prewarm_all_ok` is a computed verdict, never a permissive default
# ==========================================================================
# NON-VACUITY: pre-fix, `prewarm_warning` read `block.get("prewarm_all_ok", True)`,
# so a block missing that key returned NO warning — a verdict-carrying field
# defaulting to PASS by absence. Driven directly (the reporter always populates the
# key, which is exactly why the default was invisible): the function is extracted
# from the module and called with the key removed.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys, pathlib
sys.path.insert(0, sys.argv[1])
import ws0_report                                     # noqa: E402
from ws0_validate import Invalid                      # noqa: E402

# A block with the verdict ABSENT — the pre-fix code returned [] (no warning).
try:
    ws0_report.prewarm_warning({"prewarm": [{"rep": 1, "status": "ok"}]}, "bare-scan", "warm")
except Invalid as exc:
    assert "prewarm_all_ok" in str(exc), exc
else:
    raise SystemExit("an ABSENT prewarm_all_ok was treated as a PASS (fail-open)")

# A non-boolean verdict is likewise refused rather than truthiness-tested.
try:
    ws0_report.prewarm_warning(
        {"prewarm": [{"rep": 1, "status": "ok"}], "prewarm_all_ok": "yes"}, "bare-scan", "warm"
    )
except Invalid:
    pass
else:
    raise SystemExit("a non-boolean prewarm_all_ok was accepted")

# And the two REAL verdicts still behave: True is silent, False warns.
assert ws0_report.prewarm_warning(
    {"prewarm": [{"rep": 1, "status": "ok"}], "prewarm_all_ok": True}, "bare-scan", "warm"
) == []
warn = ws0_report.prewarm_warning(
    {"prewarm": [{"rep": 1, "status": "unrecorded"}], "prewarm_all_ok": False},
    "bare-scan", "warm",
)
assert warn and "PREWARM DEGRADED" in warn[0], warn
PY
then
  pass "OBSERVED: an ABSENT/non-boolean prewarm_all_ok is FATAL, not a silent pass"
else
  fail "prewarm_all_ok must be keyed on a computed boolean, never defaulted to True"
fi

# ==========================================================================
# 4 — a zero median is refused, not printed as `spread 0.0%`
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `spread()`: a flight rep recording
# `rows_per_s: 0.0` exited **0** and printed
#   `flight do_get (bypass)  0 rows/s [0..0, spread 0.0%] … ratio bare/flight = infx`
# — a series that measured nothing, described as the tightest possible result, with
# an `inf` ratio as the headline. BOTH halves are refused below.
ZERO_RPS='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":0.0,"duration_s":4.0}'
d="$TMP/zero-rps"; make_session "$d" "$ZERO_RPS"
# It is refused EARLIER than it used to be, and the earlier refusal is the stronger one
# (#3272 review round 2, R6 audit). `spread()` only refuses a non-positive MEDIAN, so one
# impossible rep among three left the median positive and published a spread computed over
# a rate that cannot exist. The per-REP check catches that; `spread()`'s median check is
# retained below as the series-level statement of the same rule.
expect_reject "a rep recording ZERO rows/s is REFUSED at the REP, before any median" \
  "not a positive finite rate" "$d" "$TMP/corpus"
# The SERIES-level half, still live: a series whose reps are individually plausible but
# whose median is non-positive cannot arise from the per-rep check alone, so `spread()` is
# driven directly rather than through an artifact it can no longer see.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_collect import spread
from ws0_validate import Invalid
for series in ([0.0], [0.0, 0.0, 0.0], [-1.0, 0.0, 1.0]):
    try:
        spread(series)
    except Invalid as e:
        assert "non-positive median" in str(e), str(e)
    else:
        raise SystemExit(f"spread({series}) must refuse a non-positive median")
try:
    spread([])
except Invalid as e:
    assert "nothing was observed" in str(e), str(e)
else:
    raise SystemExit("spread([]) must refuse an empty series")
# ...and the ACCEPT direction, so it is not a function that refuses everything.
got = spread([100.0, 200.0, 300.0])
assert got["median"] == 200.0 and got["n"] == 3, got
PY
then
  pass "spread() REFUSES a non-positive median and an empty series, and accepts a real one"
else
  fail "spread() must refuse a non-positive median (series-level) and accept a real series"
fi
out=$(run_report "$d" "$TMP/corpus")
# The report LINE must be absent — asserted on the line's own shape, not on the
# phrase, because the refusal text quotes the phrase it is refusing.
if ! grep -q 'rows/s  \[0\.\.0' <<<"$out" && ! grep -q 'bare/flight = infx' <<<"$out"; then
  pass "neither a 0-rows/s figure nor an 'inf' ratio LINE is printed for the degenerate series"
else
  fail "the degenerate series must not be printed at all (out: $out)"
fi
# STRUCTURAL, over EVERY reporting-path file's EXECUTABLE source (#3272 review round 2
# nit). It used to parse `ws0_report.py` alone — but every fail-closed DECISION now lives
# in `ws0_validate.py`, the collection in `ws0_collect.py` and the interleaving in
# `ws0_rounds.py`, so a new `.get(k, 0)` in any of them was outside the scan's subject
# entirely. The file list is DISCOVERED from the directory, not enumerated: a fifth module
# would otherwise be unscanned the moment someone adds it, which is how this hole opened.
#
# DOCSTRINGS and DIAGNOSTIC STRINGS are stripped via `ast` before the scan, because the
# comments explaining each fix — and the diagnostics that name the idiom they refuse ("the
# check used to be `if errors > 0`") — necessarily quote what they removed. A literal grep
# over the raw file reds on its own documentation, and the obvious "fix" for that would be
# to stop documenting it.
#
# A "diagnostic string" is one that appears as an ARGUMENT to `raise`/`Invalid(...)` or is
# concatenated into one — i.e. prose. It is NOT every string constant: blanking those
# rewrites `rec.get('cycles', 0)` to `rec.get('', 0)` and makes THE WHOLE SCAN VACUOUS,
# which is what a first pass at this did. That was caught by planting a real idiom in a
# probe module and observing the scan stay green — the check below is the permanent version
# of that probe, because a vacuous scan is textually identical to a passing one.
if python3 - "$REPO_ROOT/scripts/perf" "$TESTS_DIR" <<'PY'
import pathlib, sys

# `strip_prose` is IMPORTED from scripts/tests/ws0_prose_strip.py, never re-implemented
# (#3272 review round 3 nit). It used to be THREE inline copies — the assertion, its
# non-vacuity probe, and the strip's own test — which is three things to keep in sync, and
# a non-vacuity probe testing a DIFFERENT strip than the assertion uses proves nothing
# about the assertion. The tests dir is passed as the LAST argv rather than derived from
# `__file__`, which a `python3 - <<EOF` heredoc does not have.
sys.path.insert(0, sys.argv[-1])
from ws0_prose_strip import strip_prose                 # noqa: E402


subject = sorted(
    p for p in pathlib.Path(sys.argv[1]).glob("ws0_*.py")
)
if len(subject) < 4:
    raise SystemExit(
        f"the banned-idiom scan found only {len(subject)} reporting-path module(s)"
        f" ({[p.name for p in subject]}); its subject is the whole set, and a subject"
        " smaller than the set is how ws0_validate.py went unscanned"
    )

# Each permissive-default idiom review found, as it appears in the executable source.
# `ast.unparse` normalises quoting to single quotes.
banned = {
    "if med else 0.0": "spread() still defaults a zero-median spread to 0.0",
    "float('inf')": "a figure still falls back to inf",
    "get('requests_error', 0)": "requests_error still reads through a defaulting get",
    "get('prewarm_all_ok', True)": "prewarm_all_ok still defaults to the PERMISSIVE value",
    "records[-1]": "the reporter still consumes only the LAST step record",
    "get('cycles', 0)": "a perf counter still reads through a defaulting get",
    "get('rows', 0)": "a row count still reads through a defaulting get",
    "if errors > 0": "a counter is tested for the BAD half only; a negative value inherits the permissive branch (R6)",
}
hits = []
for path in subject:
    code = strip_prose(path.read_text())
    hits += [f"{path.name}: {why}" for idiom, why in banned.items() if idiom in code]
if hits:
    raise SystemExit("; ".join(hits))
print(f"scanned {len(subject)} module(s): {', '.join(p.name for p in subject)}", file=sys.stderr)
PY
then
  pass "STRUCTURAL: no permissive-default idiom in ANY reporting-path module (subject discovered)"
else
  fail "a permissive-default idiom is still present in the reporting path"
fi
# NON-VACUITY for that scan, in BOTH of the ways it can go vacuous. Either alone leaves a
# hole, and BOTH have actually happened while writing this:
#
#  (a) SUBJECT TOO SMALL — the scan parsed `ws0_report.py` alone while every fail-closed
#      decision moved to `ws0_validate.py`, so an idiom planted there was invisible.
#  (b) STRIP TOO BROAD — blanking every string constant (a first attempt at keeping the
#      scan off its own documentation) rewrites `rec.get('cycles', 0)` to `rec.get('', 0)`,
#      so NOTHING is ever detected. Observed by planting a real idiom in a probe module and
#      watching the suite stay green.
#
# Both are checked by planting the idiom in `ws0_validate.py` — the file (a) never read —
# and requiring the scan to FIND it, using the SAME `strip_prose` the assertion uses.
if ! python3 - "$TMP/scan-subject" "$REPO_ROOT/scripts/perf" "$TESTS_DIR" <<'PY'
import pathlib, shutil, sys

tmp = pathlib.Path(sys.argv[1]); tmp.mkdir(parents=True, exist_ok=True)
for p in pathlib.Path(sys.argv[2]).glob("ws0_*.py"):
    shutil.copy(p, tmp / p.name)
# Plant the idiom in ws0_validate.py, NOT the reporter — the file the old scan never read.
target = tmp / "ws0_validate.py"
target.write_text(target.read_text() + "\n\ndef _planted(rec):\n    return rec.get('cycles', 0)\n")

# `strip_prose` is IMPORTED from scripts/tests/ws0_prose_strip.py, never re-implemented
# (#3272 review round 3 nit). It used to be THREE inline copies — the assertion, its
# non-vacuity probe, and the strip's own test — which is three things to keep in sync, and
# a non-vacuity probe testing a DIFFERENT strip than the assertion uses proves nothing
# about the assertion. The tests dir is passed as the LAST argv rather than derived from
# `__file__`, which a `python3 - <<EOF` heredoc does not have.
sys.path.insert(0, sys.argv[-1])
from ws0_prose_strip import strip_prose                 # noqa: E402

hits = [p.name for p in sorted(tmp.glob("ws0_*.py"))
        if "get('cycles', 0)" in strip_prose(p.read_text())]
if hits:
    raise SystemExit(f"planted idiom found in {hits} (expected — the scan is not vacuous)")
PY
then
  pass "NON-VACUITY: the scan CATCHES an idiom planted in ws0_validate.py (subject + strip both sound)"
else
  fail "the banned-idiom scan must catch an idiom outside ws0_report.py — its subject is too small, or its prose-strip is too broad and blanked the idiom"
fi
# And the STRIP must still do its job: a shipped module whose DIAGNOSTIC quotes a banned
# idiom must NOT red. `ws0_collect.py` genuinely contains the sentence "the check used to be
# `if errors > 0`" inside an `Invalid(...)` message, so this is a live case, not a
# hypothetical — without the strip the scan reds on its own documentation and the reflex fix
# is to stop documenting.
if python3 - "$REPO_ROOT/scripts/perf/ws0_collect.py" "$TESTS_DIR" <<'PY'
import pathlib, sys

path = pathlib.Path(sys.argv[1])
raw = path.read_text()
assert "if errors > 0" in raw, "this case needs a module whose PROSE quotes a banned idiom"

# `strip_prose` is IMPORTED from scripts/tests/ws0_prose_strip.py, never re-implemented
# (#3272 review round 3 nit). It used to be THREE inline copies — the assertion, its
# non-vacuity probe, and the strip's own test — which is three things to keep in sync, and
# a non-vacuity probe testing a DIFFERENT strip than the assertion uses proves nothing
# about the assertion. The tests dir is passed as the LAST argv rather than derived from
# `__file__`, which a `python3 - <<EOF` heredoc does not have.
sys.path.insert(0, sys.argv[-1])
from ws0_prose_strip import strip_prose                 # noqa: E402

stripped = strip_prose(raw)
if "if errors > 0" in stripped:
    raise SystemExit("the prose-strip did not remove a DIAGNOSTIC quoting the idiom")
# ...and it must NOT have blanked an argument literal, which is (b).
if "'skipped-cold-arm'" in raw and "'skipped-cold-arm'" not in stripped:
    raise SystemExit("the prose-strip blanked a non-prose literal — the scan would be vacuous")
PY
then
  pass "the prose-strip removes DIAGNOSTICS quoting an idiom but keeps argument literals"
else
  fail "the prose-strip must exempt diagnostics WITHOUT blanking argument literals (else the scan is vacuous)"
fi
# --- THE STRIP MUST NOT BLANK AN F-STRING'S INTERPOLATION (#3272 round 3 nit) --
# The round-2 defect, repeated ONE LEVEL IN. Round 2 fixed "blanking every string constant
# makes the scan vacuous" by restricting the blanking to constants REACHABLE FROM A `raise`
# — and `ast.walk` reaches INTO an f-string's `{...}` expressions, so
#
#     raise Invalid(f"bad: {rec.get('cycles', 0)} is wrong")
#
# became `raise Invalid(f"{rec.get('', 0)}")`: an idiom written inside a diagnostic's
# interpolation was HIDDEN FROM THE SCAN. MEASURED against the pre-fix strip, that exact
# input produced exactly that output.
#
# Both halves are asserted over ONE input, so the fix cannot be "stop blanking f-strings"
# (which would red on the shipped diagnostics) or "blank nothing" (which is vacuity again):
# the LITERAL text of the f-string must go, the INTERPOLATED expression must stay.
if python3 - "$TESTS_DIR" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_prose_strip import strip_prose                 # noqa: E402

src = (
    "def f(rec):\n"
    "    if rec.get('cycles', 0) < 1:\n"
    "        raise Invalid(f\"the check used to be `if errors > 0`:"
    " {rec.get('cycles', 0)} is wrong\")\n"
    "    return 1\n"
)
out = strip_prose(src)
# (1) the INTERPOLATED expression SURVIVES — this is the defect. Pre-fix it read
#     `rec.get('', 0)` and the scan saw no idiom.
if "rec.get('cycles', 0)" not in out:
    raise SystemExit(
        "the strip blanked an f-string's INTERPOLATED EXPRESSION, so an idiom written"
        f" inside a diagnostic is invisible to the scan. Got: {out!r}"
    )
# (2) the LITERAL text of the same f-string is still REMOVED, or the strip would red on
#     every shipped diagnostic that quotes the idiom it refuses.
if "if errors > 0" in out:
    raise SystemExit(
        f"the strip left an f-string's LITERAL prose, so the scan reds on its own"
        f" documentation. Got: {out!r}"
    )
# (3) the guard-condition idiom outside the raise is untouched (the ordinary case).
if "rec.get('cycles', 0) < 1" not in out:
    raise SystemExit(f"the strip damaged executable source outside the raise: {out!r}")
PY
then
  pass "the prose-strip keeps an f-string's INTERPOLATED expression while removing its literal text (round-3 nit)"
else
  fail "the strip must not blank an f-string's interpolation — an idiom inside a diagnostic would be hidden"
fi
# ...and there is exactly ONE implementation, so the assertion and both non-vacuity probes
# cannot test different strips. Structural, because a second copy is the failure mode.
if [ "$(grep -c '^def strip_prose' "$TESTS_DIR/ws0_prose_strip.py")" -eq 1 ] \
   && ! grep -q '^def strip_prose' "$0"; then
  pass "strip_prose has ONE implementation (imported, not re-declared in this file)"
else
  fail "strip_prose must be defined once, in ws0_prose_strip.py — three inline copies were the nit"
fi

# ==========================================================================
# 5 — the corpus identity is verified against the BYTES that were measured (B6)
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `ws0_validate.load_corpus_identity`: it
# validated `corpus-identity.json` for internal consistency and NEVER OPENED the
# `Data.db`. A corpus dir holding a 4,096-byte Data.db beside an identity claiming
# 700,000 bytes and an unrelated sha256 exited **0** and printed that sha256 as
# "corpus sha256:" in the summary — the report identifying bytes it had not read.
d="$TMP/stale-size"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-stale-size"
python3 - "$TMP/corpus-stale-size/corpus-identity.json" <<'PY'
import json, sys
p = sys.argv[1]
j = json.load(open(p))
j["data_db_bytes"] = 700_000                 # the file is a few KB
j["bytes_per_row"] = 700_000 / j["rows"]     # kept internally consistent on purpose
json.dump(j, open(p, "w"))
PY
expect_reject "a recorded SIZE that disagrees with the measured Data.db is FATAL" \
  "records data_db_bytes" "$d" "$TMP/corpus-stale-size"
out=$(run_report "$d" "$TMP/corpus-stale-size")
if grep -q "nb-1-big-Data.db" <<<"$out"; then
  pass "the size refusal names the Data.db it actually measured"
else
  fail "the size refusal must name the measured file (out: $out)"
fi

cp -R "$TMP/corpus" "$TMP/corpus-stale-sha"
python3 - "$TMP/corpus-stale-sha/corpus-identity.json" <<'PY'
import json, sys
p = sys.argv[1]
j = json.load(open(p))
# A ONE-CHARACTER change: the comparison must be exact, not a prefix/length check,
# and the SIZE still matches — so only the digest can catch this.
s = list(j["data_db_sha256"])
s[0] = "5" if s[0] != "5" else "6"
j["data_db_sha256"] = "".join(s)
json.dump(j, open(p, "w"))
PY
d="$TMP/stale-sha"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "a recorded SHA-256 that disagrees with the measured bytes is FATAL" \
  "data_db_sha256" "$d" "$TMP/corpus-stale-sha"

# A corpus with NO Data.db at all: the identity cannot be checked against anything.
mkdir -p "$TMP/corpus-nodata/ws0/events"
cp "$TMP/corpus/corpus-identity.json" "$TMP/corpus-nodata/corpus-identity.json"
d="$TMP/nodata"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "a corpus with NO *-Data.db is FATAL (nothing to identify)" \
  "holds no \*-Data.db" "$d" "$TMP/corpus-nodata"

# TWO Data.db files: the identity records ONE digest, so which was measured is
# ambiguous — refused rather than picking one.
cp -R "$TMP/corpus" "$TMP/corpus-two"
cp "$TMP/corpus-two/ws0/events/nb-1-big-Data.db" "$TMP/corpus-two/ws0/events/nb-2-big-Data.db"
d="$TMP/two-data"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "TWO *-Data.db files are FATAL (the recorded identity names one corpus)" \
  "2 \*-Data.db" "$d" "$TMP/corpus-two"

# The happy path RECORDS the verification as having happened, with the bytes it read
# — so the field cannot read `true` without an observation behind it.
d="$TMP/verified"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" "$TMP/corpus/ws0/events/nb-1-big-Data.db" <<'PY'
import hashlib, json, sys
v = json.load(open(sys.argv[1]))["corpus_identity_verification"]
raw = open(sys.argv[2], "rb").read()
assert v["size_verified"] is True, v
assert v["sha256_verified"] is True, v
assert v["data_db_bytes_measured"] == len(raw), v
assert v["data_db_sha256_measured"] == hashlib.sha256(raw).hexdigest(), v
assert v["data_db"].endswith("nb-1-big-Data.db"), v
PY
then
  pass "the happy path RECORDS the measured size + digest of the Data.db it verified"
else
  fail "results.json must record the corpus byte verification (rc=$rc, out: $out)"
fi

# --- the OPT-OUT is explicit, recorded, and cannot be silent ----------------
# The digest of a 2.8 GB corpus is seconds of IO per report run, so it is
# skippable — but ONLY via a flag that STAMPS "identity unverified" into both the
# summary and results.json. The SIZE half stays unconditional (it is a stat).
d="$TMP/skip-digest"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus-stale-sha" --skip-corpus-digest); rc=$?
if [ "$rc" -eq 0 ] && grep -q "CORPUS DIGEST UNVERIFIED" <<<"$out"; then
  pass "--skip-corpus-digest is accepted and STAMPS 'CORPUS DIGEST UNVERIFIED' loudly"
else
  fail "--skip-corpus-digest must succeed and say so loudly (rc=$rc, out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))["corpus_identity_verification"]
assert v["sha256_verified"] is False, v
assert v["size_verified"] is True, v          # the cheap half is never skippable
assert v["data_db_sha256_measured"] is None, v
assert "unverified" in v["note"].lower(), v
PY
then
  pass "the skipped digest is RECORDED as unverified in results.json (never a silent pass)"
else
  fail "results.json must record the skipped digest as unverified"
fi
# The SIZE half must still fire under the opt-out — the flag scopes the digest only.
d="$TMP/skip-digest-bad-size"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "--skip-corpus-digest does NOT skip the size check" \
  "records data_db_bytes" "$d" "$TMP/corpus-stale-size" --skip-corpus-digest

# ==========================================================================
# 6 — a MISSING python3 FAILS these test scripts; it does not skip them (B8)
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `test_ws0_report_guards.sh:57-60`: with
# python3 absent from PATH it printed
#   `SKIP - python3 not installed; the reporter guards need it (never a silent PASS)`
# and exited **0** — so the gate's `tooling-tests` component recorded SUCCESS with 0
# of its ~65 checks having run, the reassuring parenthetical notwithstanding. The
# exit code is the only thing the gate reads, and it said PASS. Driven here by
# running each script under a PATH containing every standard tool EXCEPT python3.
#
# The shim dir symlinks the tools these scripts need (`bash`, `mktemp`, `grep`, …)
# and deliberately omits python3, rather than emptying PATH — an empty PATH would
# fail for a different reason and prove nothing about the python3 branch.
SHIM="$TMP/nopython/bin"
mkdir -p "$SHIM"
for tool in bash sh mktemp rm cat grep sed awk printf tr seq wc cut head tail sort \
            cp mkdir chmod kill sleep timeout dirname basename env date ls find id \
            uname; do
  p="$(command -v "$tool" 2>/dev/null)" && ln -sf "$p" "$SHIM/$tool"
done
if [ -e "$SHIM/python3" ]; then
  fail "the no-python3 shim must not contain python3"
fi

for script in test_ws0_report_guards.sh test_ws0_cpu_pinning_guards.sh \
              test_ws0_fabrication_guards.sh; do
  out=$(PATH="$SHIM" "$SHIM/bash" "$REPO_ROOT/scripts/tests/$script" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "python3 is not installed" <<<"$out"; then
    pass "OBSERVED: $script FAILS (rc=$rc) when python3 is absent — never a silent exit 0"
  else
    fail "$script must exit non-zero without python3 (rc=$rc, out: $(head -3 <<<"$out"))"
  fi
  # And it must not claim to have skipped: a "SKIP" line beside a zero exit is the
  # exact shape being removed, and the word invites re-adding the `exit 0`.
  if grep -q '^SKIP' <<<"$out"; then
    fail "$script still prints a SKIP line for absent python3 (out: $(head -3 <<<"$out"))"
  else
    pass "$script does not report the absence as a SKIP"
  fi
done

# ==========================================================================
# 7 — the reps are INTERLEAVED, and the comparison is differenced WITHIN a round
# ==========================================================================
# The repo DOES carry a binding interleaving rule; the reviewer's claim was verified
# before acting on it. `docs/reports/ws0-3096-artifacts/measurement-method.md` §3b:
#
#   "**THE RULE, binding on every future use of this rig: same-session interleaved
#   A/B/C with a drift control that is code-identical across arms, or NO COMPARISON.**"
#   (1) "run **one rep at a time**, never all reps of an arm back to back";
#   (2) "**rotate the arm order every round** so no arm holds a fixed position";
#   (4) "**Difference within a round** … not the medians alone."
#
# and `scripts/perf/README.md` §"No cross-session absolutes — interleave or do not
# compare" restates it. It was paid for: the UNTOUCHED warm bare scan read 370,134
# rows/s and 333,206 rows/s an hour later on the same box — ~10% drift with nothing
# changed on the measured path. The pre-fix driver ran ALL bare-scan reps, then all
# Flight reps of arm 1, then all of arm 2, so that drift landed directly on the
# `bare/flight` ratio and the 1.3x verdict.
#
# NON-VACUITY, measured on the pre-fix loop
#   for temp in $TEMPS; do
#     for rep in $(seq 1 $REPS); do measure_scan …; done
#     for arm in $ARMS; do for rep in …; do measure_flight …; done; done
#   done
# with `measure_scan`/`measure_flight` replaced by recorders and REPS=3, ARMS="bypass
# merge": the observed order was
#   scan-1 scan-2 scan-3  bypass-1 bypass-2 bypass-3  merge-1 merge-2 merge-3
# i.e. every arm's three reps back to back, `merge` never in first position, and no two
# arms of the same round contemporaneous. The post-fix order is asserted below.
order_probe() { # order_probe <reps> <arms…> — echoes the observed measurement order
  local reps="$1"; shift
  ( set -uo pipefail
    REPS="$reps"; TEMPS="warm"; ARMS="$*"; OUT_DIR="$TMP/order"
    mkdir -p "$OUT_DIR"
    measure_scan()   { printf 'scan-%s\n' "$2"; }
    measure_flight() { printf 'flight-%s-%s\n' "$3" "$2"; }
    eval "$(awk '/^rotate_arms\(\)/,/^}/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh")"
    # The loop itself, taken from the driver so this cannot drift into testing a copy.
    eval "$(awk '/^_ARM_LIST=/,/^done$/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh")"
  ) 2>/dev/null | grep -E '^(scan|flight)-' | tr '\n' ' '
}

# NON-VACUITY for the ROUND-2 half, measured against the round-1 "interleaved" loop:
#   measure_scan "$temp" "$rep"                      # ALWAYS first
#   for arm in $(rotate_arms "$rep" "${_ARM_LIST[@]}"); do measure_flight …; done
# with REPS=4, ARMS="bypass" (the DEFAULT), the observed order was
#   scan-1 flight-bypass-1  scan-2 flight-bypass-2  scan-3 flight-bypass-3  scan-4 …
# — the bare scan in position 1 of EVERY round and NO ROTATION AT ALL, because the only
# rotated list held one element. The fix for the drift hazard did not close it: the bare
# scan is the DENOMINATOR of the ratio, so any within-round systematic effect that always
# lands on it (a page cache left by the previous round's Flight rep, a thermal ramp early
# in the round) moves the ratio one way in every round — invisible to the per-round
# direction count, because it is present in every round equally (#3272 review R4a).
got=$(order_probe 3 bypass merge)
# Round-major, with the BARE SCAN ROTATING as a peer: round 1 leads with scan, round 2
# with bypass, round 3 with merge.
if grep -q 'scan-1 flight-bypass-1 flight-merge-1 flight-bypass-2 flight-merge-2 scan-2 flight-merge-3 scan-3 flight-bypass-3' <<<"$got"; then
  pass "OBSERVED: the loop is ROUND-MAJOR and the bare scan ROTATES with the Flight arms"
else
  fail "the loop must interleave one rep of EVERY arm per round, scan included (order: $got)"
fi
# The three properties, asserted separately so a partial regression is diagnosable.
if ! grep -qE 'scan-1 scan-2|scan-2 scan-3' <<<"$got"; then
  pass "OBSERVED: no two bare-scan reps run back to back (rule §3b step 1)"
else
  fail "bare-scan reps must not run back to back (order: $got)"
fi
if grep -qE 'flight-bypass-2 flight-merge-2 scan-2' <<<"$got" \
   && grep -qE 'flight-merge-3 scan-3 flight-bypass-3' <<<"$got"; then
  pass "OBSERVED: every arm occupies every POSITION over 3 rounds (rule §3b step 2)"
else
  fail "the arm order must rotate per round (order: $got)"
fi
# THE DEFAULT CASE, which is the one round 1 got wrong: `--arm bypass` is TWO arms
# (scan + bypass), and a "rotation" that reduces to a fixed order at n=2 is the same
# defect. So it must genuinely ALTERNATE.
got=$(order_probe 4 bypass)
if grep -q 'scan-1 flight-bypass-1 flight-bypass-2 scan-2 scan-3 flight-bypass-3 flight-bypass-4 scan-4' <<<"$got"; then
  pass "OBSERVED: the DEFAULT 2-arm case genuinely ALTERNATES (pre-fix: scan first in all 4 rounds)"
else
  fail "the default single-Flight-arm run must alternate scan/flight, not fix scan first (order: $got)"
fi
# ...and the bare scan must NOT hold position 1 in every round — stated as its own
# assertion because that is the defect, positionally.
if [ "$(grep -o 'scan-[0-9]' <<<"$got" | head -1)" = "scan-1" ] \
   && grep -qE 'flight-bypass-2 scan-2' <<<"$got"; then
  pass "OBSERVED: the bare scan does NOT hold a fixed position across rounds (R4a)"
else
  fail "the bare scan must not lead every round (order: $got)"
fi
# `rotate_arms` itself: over n rounds every arm must occupy every position, or the
# rotation is decorative.
if bash -c '
  set -uo pipefail
  eval "$(awk "/^rotate_arms\(\)/,/^}/" "'"$REPO_ROOT"'/scripts/perf/ws0-baseline.sh")"
  [ "$(rotate_arms 1 a b c)" = "a b c " ] || { echo "round1: $(rotate_arms 1 a b c)"; exit 1; }
  [ "$(rotate_arms 2 a b c)" = "b c a " ] || { echo "round2: $(rotate_arms 2 a b c)"; exit 1; }
  [ "$(rotate_arms 3 a b c)" = "c a b " ] || { echo "round3: $(rotate_arms 3 a b c)"; exit 1; }
  [ "$(rotate_arms 4 a b c)" = "a b c " ] || { echo "round4 (wraps): $(rotate_arms 4 a b c)"; exit 1; }
  [ "$(rotate_arms 1 a b)" = "a b " ]     || { echo "n=2 r1: $(rotate_arms 1 a b)"; exit 1; }
  [ "$(rotate_arms 2 a b)" = "b a " ]     || { echo "n=2 r2 (must SWAP): $(rotate_arms 2 a b)"; exit 1; }
  [ "$(rotate_arms 7 x)" = "x " ]         || { echo "single arm: $(rotate_arms 7 x)"; exit 1; }
' >/dev/null 2>&1; then
  pass "OBSERVED: rotate_arms puts every arm in every position over n rounds, incl. n=2, and wraps"
else
  fail "rotate_arms must rotate by (round-1) mod n and wrap (incl. a real swap at n=2)"
fi
# And the ARM LIST the loop rotates must CONTAIN the bare scan — the structural half of
# R4a, so a future edit cannot revert to rotating the Flight arms alone while every
# behavioural case above still passes on a re-plumbed loop.
if grep -qE '^_ARM_LIST=\(scan \$ARMS\)' "$REPO_ROOT/scripts/perf/ws0-baseline.sh"; then
  pass "STRUCTURAL: the rotated arm list includes `scan` as a peer of the Flight arms"
else
  fail "the rotated list must be (scan \$ARMS): rotating only the Flight arms is R4a"
fi

# --- the reporter differences WITHIN a round --------------------------------
# Interleaving the driver is half the fix; the other half is that the REPORT states the
# paired per-round comparison rather than only the median-vs-median difference. The
# recorded case for that: #3096's lever 4 measured `+4,817 rows/s / +2.3%` by medians
# and ZERO on 8 interleaved rounds (median −0.03%, 4 of 8 rounds positive).
d="$TMP/paired"; mkdir -p "$d"
# Three rounds where the MEDIAN favours flight but the per-round direction is split —
# the exact shape a median-only reading misreports.
for rep in 1 2 3; do
  make_scan_rep "$d" warm "$rep" ok
done
python3 - "$d" "$CORPUS_ROWS" <<'PY'
import json, pathlib, sys
d, rows = pathlib.Path(sys.argv[1]), int(sys.argv[2])
# flight rows/s per round: two rounds below the bare scan's 500/s (1000 rows / 2.0s),
# one above — so 1 of 3 rounds meets a 1.3x target while the median does not.
for rep, rps in ((1, 300.0), (2, 480.0), (3, 200.0)):
    tag = f"flight-bypass-warm-{rep}"
    (d / f"{tag}.jsonl").write_text(json.dumps({
        "round": tag, "requests_ok": 1, "requests_error": 0,
        "rows_total": rows, "rows_per_s": rps, "duration_s": 4.0}) + "\n")
    (d / f"perf-{tag}.csv").write_text("8000000,,cycles,,,,\n16000000,,instructions,,,,\n")
    (d / f"{tag}.prewarm.status").write_text("ok\n")
    # The interleaving metadata the reporter REQUIRES (#3272 R3), alternating position by
    # round exactly as the driver does — the scan fixture takes the complement.
    # `monotonic_ns` too (#3272 review round 3, B3): round-major and distinct, which is
    # the shape a real sequential loop produces and the property the reporter verifies.
    pos = 1 if rep % 2 == 0 else 2
    (d / f"{tag}.round").write_text(
        f"round={rep}\nposition={pos}\narms_in_round=2\n"
        f"monotonic_ns={rep * 10**9 + pos * 10**6}\n")
PY
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 3 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'per-round (PAIRED' <<<"$out" \
  && grep -q 'within-round 1.3x target met in 1/3 round' <<<"$out"; then
  pass "OBSERVED: the report prints PAIRED per-round ratios and the direction count"
else
  fail "the report must print the paired within-round comparison (rc=$rc, out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
fl = [m for m in json.load(open(sys.argv[1]))["measurements"] if m["arm"].startswith("flight_")][0]
rounds = fl["per_round_paired"]
assert [r["round"] for r in rounds] == [1, 2, 3], rounds
# Each round pairs rep k of the bare scan with rep k of the flight arm.
assert all(r["bare_rows_per_sec"] == 500.0 for r in rounds), rounds
assert [r["flight_rows_per_sec"] for r in rounds] == [300.0, 480.0, 200.0], rounds
assert [r["flight_meets_target"] for r in rounds] == [False, True, False], rounds
PY
then
  pass "results.json records the per-round PAIRED comparison, rep-for-rep"
else
  fail "results.json must record the paired per-round records"
fi
# ==========================================================================
# R3 — the INTERLEAVING CLAIM is DERIVED from recorded metadata, never asserted
# ==========================================================================
# NON-VACUITY, measured against the round-1 reporter: the NOTES block printed
#
#   "the reps were INTERLEAVED — one rep per arm per round, arm order rotated"
#
# UNCONDITIONALLY, as a claim about the session, while `paired_rounds` paired by REP
# INDEX and read NOTHING the driver recorded. The driver DID write `<tag>.round` files
# (and carried a comment saying the reporter read them); `grep -c '\.round' ws0_report.py`
# on that revision is **0**. MEASURED: a session dir with EVERY `.round` file deleted —
# i.e. one that could equally be an arm-major run, or reps re-run individually into one
# `--out` — exited **0** and printed the interleaving sentence verbatim.
#
# So: the metadata is REQUIRED, the pairing is by the OBSERVED round, and the sentence is
# derived from what was observed.
d="$TMP/no-round-meta"; make_session "$d" "$GOOD_FLIGHT"
rm -f "$d"/*.round
expect_reject "a session with NO round metadata is REFUSED (the interleaving is unestablished)" \
  "has no round metadata" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "may not print the interleaving claim" <<<"$out"; then
  pass "the refusal says the CLAIM is what cannot be printed (not merely 'a file is missing')"
else
  fail "the round-metadata refusal must name the claim it protects (out: $out)"
fi
# ...and NOTHING is written: a report that cannot establish its own headline property must
# not leave a results.json a later reader could quote.
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written when the interleaving cannot be established"
else
  fail "a refused run must not leave a results.json behind"
fi
# ONE arm's metadata missing is equally fatal — a partial record cannot establish a round.
d="$TMP/half-round-meta"; make_session "$d" "$GOOD_FLIGHT"
rm -f "$d"/flight-*.round
expect_reject "ONE arm's missing round metadata is FATAL too (a round needs every arm)" \
  "has no round metadata" "$d" "$TMP/corpus"

# A corrupt/incomplete metadata field is an ERROR, never a defaulted 0.
d="$TMP/round-meta-partial"; make_session "$d" "$GOOD_FLIGHT"
printf 'round=1\n' > "$d/scan-warm-1.round"     # no position, no arms_in_round
expect_reject "round metadata with no 'position' is REFUSED (a round index alone proves nothing)" \
  "carries no 'position'" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "cannot distinguish an interleaved session from an arm-major one" <<<"$out"; then
  pass "the refusal explains WHY position is required (a rep index exists either way)"
else
  fail "the position refusal must explain why the round index is insufficient (out: $out)"
fi
d="$TMP/round-meta-garbage"; make_session "$d" "$GOOD_FLIGHT"
printf 'round=one\nposition=1\narms_in_round=2\n' > "$d/scan-warm-1.round"
expect_reject "an unparseable round field is REFUSED (a corrupt field is not a zero)" \
  "not an integer" "$d" "$TMP/corpus"
d="$TMP/round-meta-mismatch"; make_session "$d" "$GOOD_FLIGHT"
make_round "$d" scan-warm-1 7 1 2 1000000
expect_reject "a round that disagrees with the rep index in the FILENAME is REFUSED" \
  "does not describe one session" "$d" "$TMP/corpus"

# THE ROTATION CHECK, positionally. Two arms over two rounds with the SCAN AT POSITION 1
# BOTH TIMES is exactly what the round-1 driver produced for the default `--arm bypass`,
# and it must be refused rather than reported under a claim that the order rotated.
d="$TMP/no-rotation"; mkdir -p "$d"
for rep in 1 2; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
  # SCAN AT POSITION 1 BOTH ROUNDS, with round-major timestamps: the timing check must
  # PASS (the session really was interleaved) so the refusal below is attributable to the
  # ROTATION property alone, not to the clock.
  make_round "$d" "scan-warm-$rep" "$rep" 1 2 "$(( rep * 1000000000 + 1000000 ))"
  make_round "$d" "flight-bypass-warm-$rep" "$rep" 2 2 "$(( rep * 1000000000 + 2000000 ))"
done
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 2 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "held ONE FIXED POSITION" <<<"$out" \
  && grep -q "bare_scan" <<<"$out"; then
  pass "OBSERVED: an arm at a FIXED position across rounds is REFUSED, naming the arm (R4a)"
else
  fail "a fixed arm position must be refused (rc=$rc, out: $out)"
fi
# TWO ARMS SHARING A POSITION is not a round at all.
d="$TMP/dup-position"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
make_round "$d" scan-warm-1 1 1 2 1000000001
make_round "$d" flight-bypass-warm-1 1 1 2 1000000002
expect_reject "two arms at the SAME position is REFUSED (that is not a round)" \
  "which is not 1..2 exactly once" "$d" "$TMP/corpus"
# A round that RECORDS more arms than are present is a PARTIAL round.
d="$TMP/partial-round"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
make_round "$d" scan-warm-1 1 1 3 1000000001
make_round "$d" flight-bypass-warm-1 1 2 3 1000000002
expect_reject "a round recording MORE arms than are present is REFUSED (a partial round)" \
  "is a PARTIAL round" "$d" "$TMP/corpus"

# THE ACCEPT DIRECTION, affirmatively: a properly interleaved session must print the
# claim AS AN OBSERVATION, naming the rounds and the positions, and record it in
# results.json — so the sentence cannot be present without the artifacts behind it.
#
# AND THE TWO HALVES MUST BE WORDED DIFFERENTLY (#3272 review round 3, B3). The
# interleaving is OBSERVED FROM THE CLOCK (per-rep `monotonic_ns`, round-major ordering
# verified); the rotation is only RECORDED, because `position` is a number the driver
# COMPUTES and the clock cannot corroborate within-round order. The pre-fix text said
# "OBSERVED, not asserted" for BOTH — over a source (`round`/`position`/`arms_in_round`)
# that an arm-major loop reproduces byte-for-byte. So the assertion below checks the
# strength of each sentence, not merely that a sentence exists.
d="$TMP/rotated-ok"; mkdir -p "$d"
for rep in 1 2; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
done
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 2 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "OBSERVED FROM THE CLOCK" <<<"$out" \
  && grep -q "The arm ORDER ROTATED — RECORDED, not clock-observed" <<<"$out" \
  && grep -q "Positions by round" <<<"$out"; then
  pass "OBSERVED: a rotated session prints the interleaving as CLOCK-OBSERVED and the rotation as RECORDED (B3)"
else
  fail "a rotated session must print the derived interleaving claim (rc=$rc, out: $out)"
fi
# ...and the printed text must NOT claim more than the artifacts prove: the report states
# what is NOT established, so a reader is not left to infer the limit from an absence.
if grep -q "NOT ESTABLISHED by the artifacts" <<<"$out" \
  && grep -q "the WITHIN-round arm order, and therefore the rotation" <<<"$out"; then
  pass "the report STATES what the artifacts do not establish (the within-round order)"
else
  fail "the report must state the limit of its own interleaving claim (out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
iv = r["interleaving"]["warm"]
assert iv["verified"] is True, iv
assert iv["rounds"] == [1, 2], iv
assert iv["arms_per_round"] == 2, iv
assert iv["rotation_checked"] is True, iv
# The bare scan is a ROTATED ARM, so its position must differ across rounds.
pos = [iv["positions_by_round"][str(k)]["bare_scan"] for k in (1, 2)]
assert sorted(pos) == [1, 2], pos
# ...and every rep carries the round it was MEASURED in, plus its position.
for m in r["measurements"]:
    for rep in m["reps"]:
        assert rep["round"] == rep["rep"], rep
        assert rep["position_in_round"] in (1, 2), rep
        assert rep["arms_in_round"] == 2, rep
PY
then
  pass "results.json records the interleaving OBSERVATION and each rep's round+position"
else
  fail "results.json must record the interleaving observation (out: $out)"
fi
# At ONE round the rotation is NOT OBSERVABLE, and must therefore NOT be claimed — the
# same rule as everything else here: a positive verdict needs an affirmative measurement.
d="$TMP/one-round"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && ! grep -q "The arm ORDER ROTATED" <<<"$out" \
  && grep -q "rotation is not observable at this size" <<<"$out"; then
  pass "OBSERVED: at ONE round the rotation is NOT claimed, and the report says why"
else
  fail "a single-round session must not claim rotation (rc=$rc, out: $out)"
fi
# STRUCTURAL: no unconditional interleaving sentence may survive in the reporter. The
# claim must come from `interleaving_lines`, which only runs on a verified observation.
if python3 - "$REPO_ROOT/scripts/perf/ws0_report.py" <<'PY'
import ast, sys
tree = ast.parse(open(sys.argv[1]).read())
for node in ast.walk(tree):
    if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        b = node.body
        if b and isinstance(b[0], ast.Expr) and isinstance(b[0].value, ast.Constant) \
                and isinstance(b[0].value.value, str):
            node.body = b[1:] or [ast.Pass()]
code = ast.unparse(ast.fix_missing_locations(tree))
if "arm order rotated" in code:
    raise SystemExit("ws0_report.py still carries an unconditional 'arm order rotated' claim")
if "interleaving_lines" not in code:
    raise SystemExit("ws0_report.py does not derive the interleaving claim from the observation")
PY
then
  pass "STRUCTURAL: the reporter carries NO unconditional interleaving sentence"
else
  fail "an unconditional interleaving claim remains in ws0_report.py"
fi
# And the DRIVER must record all three fields — the wiring half, so the reporter's
# requirement cannot be satisfied only by test fixtures.
if awk '/^record_round\(\)/,/^}/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
     | grep -q 'round=%s' \
   && awk '/^record_round\(\)/,/^}/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
     | grep -q 'position=%s' \
   && awk '/^record_round\(\)/,/^}/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
     | grep -q 'arms_in_round=%s'; then
  pass "the DRIVER records round/position/arms_in_round per rep (the reporter's requirement is wired)"
else
  fail "ws0-baseline.sh must record all three interleaving fields per rep"
fi

# And the reporter REFUSES an unpairable set rather than silently falling back to
# medians alone — which is the comparison §3b forbids on its own.
d="$TMP/unpairable"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_scan_rep "$d" warm 2 ok
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
make_flight_rep "$d" warm 2 ok "$GOOD_FLIGHT"
rm -f "$d/scan-warm-2.json"          # scan has rep 1 only; flight has 1 and 2
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 2 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "an unpairable rep set is REFUSED (never a silent fallback to median-only)"
else
  fail "an unpairable rep set must be refused (rc=$rc, out: $out)"
fi

# ==========================================================================
# B2 — `instructions` is validated on the SAME rule as `cycles` (review round 3)
# ==========================================================================
# NON-VACUITY, and this is the THIRD round this class has been fixed partially. Round 2
# added `if cyc <= 0` after the setup subtraction and left `ins` — the SAME subtraction
# over the same two artifacts — unchecked, feeding `ipc.append(ins / cyc)`. So each case
# below is an artifact whose `cycles` are perfectly healthy and whose `instructions` are
# not, which the pre-fix code accepted and PUBLISHED as an IPC.
#
# MEASURED against the pre-fix reporter, THREE reps with exactly ONE corrupt — which is the
# shape the finding is about, because `spread()`'s non-positive-MEDIAN check is what a
# single-rep case would trip on for the wrong reason:
#   * rep 2's SCAN setup instructions 9M > total 4M (cycles healthy) -> exit **0**, and
#     `results.json` carried `ipc: {median: 2.0, min: -2.6315789473684212, …}`; the per-rep
#     IPCs were [2.0, -2.632, 2.0].
#   * rep 2's FLIGHT CSV recording `instructions,0` (cycles healthy) -> exit **0**, and
#     `ipc: {median: 2.0, min: 0.0, …}`.
# The median stayed positive in both, so the impossible value was published as `ipc.min` —
# and would have been the printed `IPC` had it been the middle value. Both cases below are
# driven at three reps for that reason; the single-rep variants are kept beside them
# because they are the cheapest diagnosis of the same defect.

# (a) BARE SCAN, setup instructions > total instructions. Cycles are untouched, so this
#     isolates the missing check rather than re-testing the one that existed.
d="$TMP/ins-subtraction"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-1-setup.csv" 100000 9000000
expect_reject "a setup leg with MORE instructions than the full run is FATAL (round 2 checked only cycles)" \
  "setup-subtracted instructions" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "IPC = instructions/cycles" <<<"$out" \
  && grep -q "non-positive IPC" <<<"$out"; then
  pass "the refusal names WHAT the unchecked value would have published (a non-positive IPC)"
else
  fail "the instructions refusal must name the IPC it protects (out: $out)"
fi
# ...and the CYCLES half must still fire, so the fix did not move the check rather than add
# one. Same fixture shape, the other counter.
d="$TMP/cyc-subtraction"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-1-setup.csv" 9000000 200000
expect_reject "a setup leg with MORE cycles than the full run is still FATAL (both arms of the subtraction)" \
  "setup-subtracted cycles" "$d" "$TMP/corpus"

# (b) FLIGHT ARM, `instructions,0`. This arm has no setup leg, so the perf value IS the
#     derived quantity — and round 2 checked `cyc <= 0` here too while `ins` went straight
#     into the IPC.
d="$TMP/flight-zero-ins"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-flight-bypass-warm-1.csv" 8000000 0
expect_reject "a flight perf CSV recording instructions=0 is FATAL (pre-fix: published IPC 0.0)" \
  "flight rep flight-bypass-warm-1 instructions" "$d" "$TMP/corpus"
d="$TMP/flight-zero-cyc"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-flight-bypass-warm-1.csv" 0 16000000
expect_reject "a flight perf CSV recording cycles=0 is still FATAL" \
  "flight rep flight-bypass-warm-1 cycles" "$d" "$TMP/corpus"

# (c) A NEGATIVE hardware counter is refused AT PARSE TIME, before any arithmetic. perf
#     cannot emit one, so it is a corrupt artifact — and `int("-4")` used to sail through
#     to become a negative `cycles`, a negative subtraction result, and a negative IPC.
d="$TMP/neg-counter"; make_session "$d" "$GOOD_FLIGHT"
printf -- '-4000000,,cycles,,,,\n8000000,,instructions,,,,\n' > "$d/perf-scan-warm-1.csv"
expect_reject "a NEGATIVE perf counter value is FATAL at parse time (a counter cannot be negative)" \
  "not a possible count" "$d" "$TMP/corpus"

# (d) THE SURVIVING-REP CASES, which are the ones the pre-fix code ACCEPTED. `spread()`
#     refuses a non-positive MEDIAN, so a single corrupt rep tripped that check for the
#     wrong reason; with THREE reps and ONE corrupt, the median stays positive and the
#     impossible value is published as `ipc.min`. Both arms are driven, because the B2 gap
#     was present in both.
#
#     MEASURED against the pre-fix reporter, both exited **0**:
#       scan  -> ipc {median: 2.0, min: -2.6315789473684212}, per-rep [2.0, -2.632, 2.0]
#       flight-> ipc {median: 2.0, min: 0.0}
three_reps() { # three_reps <dir>
  local dd="$1" r
  mkdir -p "$dd"
  for r in 1 2 3; do
    make_scan_rep "$dd" warm "$r" ok
    make_flight_rep "$dd" warm "$r" ok "$GOOD_FLIGHT"
  done
}
d="$TMP/one-bad-scan-ins-of-three"; three_reps "$d"
perf_csv "$d/perf-scan-warm-2.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-2-setup.csv" 100000 9000000     # rep 2 only; CYCLES healthy
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 3 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "setup-subtracted instructions" <<<"$out"; then
  pass "OBSERVED: ONE corrupt scan rep of three is REFUSED (pre-fix: exit 0, ipc.min -2.63)"
else
  fail "one corrupt scan rep among three must be refused, not published as ipc.min (rc=$rc, out: $out)"
fi
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written for the surviving-rep case (pre-fix it held the negative ipc.min)"
else
  fail "a refused run must not leave a results.json behind"
fi
d="$TMP/one-bad-flight-ins-of-three"; three_reps "$d"
perf_csv "$d/perf-flight-bypass-warm-2.csv" 8000000 0       # rep 2 only; CYCLES healthy
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 3 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "flight rep flight-bypass-warm-2 instructions" <<<"$out"; then
  pass "OBSERVED: ONE corrupt flight rep of three is REFUSED (pre-fix: exit 0, ipc.min 0.0)"
else
  fail "one corrupt flight rep among three must be refused (rc=$rc, out: $out)"
fi
# And `spread()` itself must refuse a non-positive MEMBER, not only a non-positive MEDIAN —
# the last line of defence, driven directly so it is not merely reachable in theory.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_collect import spread
from ws0_validate import Invalid
# A series whose MEDIAN is positive and one of whose MEMBERS cannot exist. The pre-fix
# spread() accepted this and returned min=-1.0.
for series in ([2.0, -1.0, 3.0], [2.0, 0.0, 3.0], [2.0, float("inf"), 3.0]):
    try:
        got = spread(series)
    except Invalid as e:
        assert "not meaningful" in str(e) or "not finite" in str(e), str(e)
    else:
        raise SystemExit(f"spread({series}) must refuse a non-positive/non-finite MEMBER, got {got}")
# ...and a healthy series is still accepted, so it is not a function that refuses everything.
got = spread([2.0, 3.0, 4.0])
assert got["median"] == 3.0 and got["min"] == 2.0 and got["n"] == 3, got
PY
then
  pass "spread() REFUSES a non-positive/non-finite MEMBER of an otherwise-healthy series (B2)"
else
  fail "spread() must refuse a bad member, not only a bad median — that is the surviving-rep hole"
fi

# ==========================================================================
# B5 — a coercion may not TRUNCATE or accept a BOOLEAN (review round 3)
# ==========================================================================
# NON-VACUITY: every coercion in the reporting path was a bare `int()`. Measured against
# HEAD~1 of this commit, each of these exited **0** with a full report:
#   * `requests_error: 0.9` -> `int(0.9)` is 0 -> reported CLEAN, no failed requests
#   * `requests_ok: 1.9`    -> `int(1.9)` is 1 -> SATISFIED the exactly-one-cold-request
#                              guard of #3096 finding 2, from a value that is not 1
#   * `requests_error: true`-> `int(True)` is 1 -> then refused as "1 failed request",
#                              i.e. a boolean silently became a count
# A truncation is a FABRICATED VALUE arrived at by rounding rather than by defaulting —
# the same class as `.get(k, 0)`, which is why it belongs in this file.
FRAC_ERR='{"round":"r","requests_ok":1,"requests_error":0.9,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/frac-error"; make_session "$d" "$FRAC_ERR"
expect_reject "a FRACTIONAL requests_error is FATAL (pre-fix: int(0.9) reported CLEAN)" \
  "fractional value" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "TRUNCATED it to 0" <<<"$out" && grep -q "fabricated value" <<<"$out"; then
  pass "the refusal names the TRUNCATION and what it would have reported"
else
  fail "the fractional-counter refusal must name the truncated value (out: $out)"
fi
BOOL_ERR='{"round":"r","requests_ok":1,"requests_error":true,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/bool-error"; make_session "$d" "$BOOL_ERR"
expect_reject "a BOOLEAN requests_error is FATAL (pre-fix: int(True) became a count of 1)" \
  "is the boolean True" "$d" "$TMP/corpus"
# `requests_ok`, where the truncation defeats the COLD guard rather than the error count.
FRAC_OK='{"round":"r","requests_ok":1.9,"requests_error":0,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/frac-ok"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 skipped-cold-arm "$FRAC_OK"
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 1 --temps cold --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "fractional value 1.9" <<<"$out"; then
  pass "OBSERVED: a FRACTIONAL requests_ok is refused (pre-fix: int(1.9)==1 SATISFIED the cold guard)"
else
  fail "a fractional requests_ok must be refused, not truncated into the cold guard (rc=$rc, out: $out)"
fi
BOOL_OK='{"round":"r","requests_ok":true,"requests_error":0,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/bool-ok"; make_session "$d" "$BOOL_OK"
expect_reject "a BOOLEAN requests_ok is FATAL (int(True) is 1, which is a valid count)" \
  "is the boolean True" "$d" "$TMP/corpus"
# A FRACTIONAL rows_total, which would silently change the DENOMINATOR of cycles/row.
FRAC_ROWS='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000.5,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/frac-rows"; make_session "$d" "$FRAC_ROWS"
expect_reject "a FRACTIONAL rows_total is FATAL (it is the cycles/row denominator)" \
  "fractional value" "$d" "$TMP/corpus"
# The same class in a perf CSV, which is TEXT: `int("4.7")` raises, but `int(" 4 ")` used
# to accept padding silently, and neither is a canonical counter value.
d="$TMP/frac-csv"; make_session "$d" "$GOOD_FLIGHT"
printf '4000000.5,,cycles,,,,\n8000000,,instructions,,,,\n' > "$d/perf-scan-warm-1.csv"
expect_reject "a FRACTIONAL perf CSV counter is FATAL (not a canonical integer)" \
  "unparseable value" "$d" "$TMP/corpus"
# ...and the identity fields, the last coercion in the enumeration.
make_corpus "$TMP/corpus-frac-rows"
python3 - "$TMP/corpus-frac-rows" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "corpus-identity.json"
ident = json.loads(p.read_text())
ident["cells_per_row"] = 12.5
p.write_text(json.dumps(ident))
PY
d="$TMP/frac-identity"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "a FRACTIONAL corpus-identity field is FATAL (pre-fix: silently truncated)" \
  "fractional value" "$d" "$TMP/corpus-frac-rows"
# THE ACCEPT DIRECTION for the whole class: an INTEGRAL float (`1000.0`) is the value it
# would be read as, so it is ACCEPTED. The rule is "not the integer it would be read as",
# not "never a float" — a producer writing an integer-valued double is not an error.
INTEGRAL_FLOAT='{"round":"r","requests_ok":1.0,"requests_error":0.0,"rows_total":1000.0,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/integral-float"; make_session "$d" "$INTEGRAL_FLOAT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "an INTEGRAL float (1000.0) is ACCEPTED — the rule is exactness, not 'never a float'"
else
  fail "an integral float must be accepted; refusing it would red every producer writing doubles (rc=$rc, out: $out)"
fi

# ==========================================================================
# B3 — the INTERLEAVING is observed FROM A CLOCK, and an ARM-MAJOR FORGERY fails
# ==========================================================================
# NON-VACUITY, and this is the case the whole finding rests on. Pre-fix, `<tag>.round`
# carried `round`/`position`/`arms_in_round` and NO TIMESTAMP, and `collect_round_meta`
# forced `round == rep` — so `round` carried zero independent information and `position`
# was a number the driver COMPUTED. An ARM-MAJOR loop keeping the identical rotation
# arithmetic:
#
#     for arm in rotate_arms(...):        # arms OUTSIDE
#         for rep in 1..REPS:             # reps INSIDE
#             measure(arm, rep); record_round(tag, rep, pos, n)
#
# emits BYTE-IDENTICAL metadata for a session in which no two arms of a "round" ran
# anywhere near each other — and the reporter printed "the reps were INTERLEAVED … this is
# OBSERVED, not asserted" over it. The fixture below IS that forgery: labels a genuine
# interleaved session would carry, timestamps an arm-major run produces.
d="$TMP/arm-major-forgery"; mkdir -p "$d"
for rep in 1 2 3; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
done
# The LABELS: exactly what the real interleaved driver writes (rotating positions).
# The CLOCK: arm-major — all three scan reps complete, THEN all three flight reps.
for rep in 1 2 3; do
  scan_pos=$(( rep % 2 == 1 ? 1 : 2 ))
  fl_pos=$(( rep % 2 == 1 ? 2 : 1 ))
  make_round "$d" "scan-warm-$rep"          "$rep" "$scan_pos" 2 "$(( 1000000000 + rep * 1000000 ))"
  make_round "$d" "flight-bypass-warm-$rep" "$rep" "$fl_pos"   2 "$(( 5000000000 + rep * 1000000 ))"
done
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 3 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "were NOT INTERLEAVED" <<<"$out"; then
  pass "OBSERVED: an ARM-MAJOR session carrying PERFECT interleaved LABELS is REFUSED (B3)"
else
  fail "the arm-major forgery must be refused — the labels alone cannot establish interleaving (rc=$rc, out: $out)"
fi
if grep -q "byte-identical" <<<"$out" && grep -q "ARM-MAJOR" <<<"$out"; then
  pass "the refusal explains that the LABELS were indistinguishable, so the clock is the evidence"
else
  fail "the arm-major refusal must say why the labels could not catch it (out: $out)"
fi
# ...and NOTHING is written: the report cannot establish its headline property.
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written for a session whose interleaving the clock refutes"
else
  fail "a refused run must not leave a results.json behind"
fi
# The DRIVER must record the instant, or every check above is unreachable in practice.
if grep -q 'monotonic_ns=%s' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
  && awk '/^record_round\(\)/,/^}/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
     | grep -q 'time.monotonic_ns'; then
  pass "the DRIVER records a monotonic instant per rep (the reporter's requirement is wired)"
else
  fail "ws0-baseline.sh must record monotonic_ns per rep, from a monotonic clock"
fi
# An ABSENT instant is fatal, not defaulted — a session from the pre-fix driver cannot be
# reported under the interleaving claim, and saying so is the honest outcome.
d="$TMP/no-instant"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
printf 'round=1\nposition=1\narms_in_round=2\n' > "$d/scan-warm-1.round"
expect_reject "round metadata with NO monotonic_ns is REFUSED (a pre-fix session cannot carry the claim)" \
  "carries no 'monotonic_ns'" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "the only field that records WHEN the rep ran" <<<"$out"; then
  pass "the refusal explains why an instant is required (labels cannot establish when)"
else
  fail "the monotonic_ns refusal must state what it is for (out: $out)"
fi
# COPIED metadata is refused: two reps of a sequential loop cannot share a nanosecond, so
# an identical instant means the file was duplicated rather than measured.
d="$TMP/copied-instant"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
make_round "$d" scan-warm-1          1 1 2 1234567890
make_round "$d" flight-bypass-warm-1 1 2 2 1234567890
expect_reject "two reps recording the IDENTICAL instant is REFUSED (copied, not measured)" \
  "IDENTICAL completion instant" "$d" "$TMP/corpus"
# THE ACCEPT DIRECTION: a genuinely round-major clock is accepted and the observation is
# recorded in results.json with the span it measured — so the claim cannot be printed
# without a number behind it.
d="$TMP/round-major-ok"; mkdir -p "$d"
for rep in 1 2 3; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
done
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" --server-cpus 2,10 \
  --client-cpus 4,12 --reps 3 --temps warm --arms bypass \
  --step-duration 45s/1s --scan-passes 1 2>&1); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
il = json.load(open(sys.argv[1]))["interleaving"]["warm"]
t = il["timing"]
assert t["round_major_verified"] is True, t
assert t["established"] == "round-major ordering", t
assert t["rounds_compared"] == 3, t
assert "monotonic_ns" in t["source"], t
# The LIMIT of the claim is recorded too, so a later reader is not left to infer it.
assert "WITHIN-round arm order" in t["not_established"], t
# ...and the per-round spans are real numbers, not a placeholder.
assert set(t["within_round_span_ns"]) == {"1", "2", "3"}, t
assert all(v > 0 for v in t["within_round_span_ns"].values()), t
# The `source` of the whole observation must not claim a provenance it cannot verify.
assert "UNVERIFIED" in il["source"], il["source"]
PY
then
  pass "a round-major session is ACCEPTED and records the timing OBSERVATION plus its stated LIMIT"
else
  fail "the accept direction must record the timing observation (rc=$rc, out: $out)"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its
# ~96 checks and passed them exits 0 and reports SUCCESS. That is the suite-level
# `0/0` shape this whole issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite)
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file.
MIN_CHECKS=88
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 fabrication guards: all $checks checks passed"
  exit 0
fi
echo "ws0 fabrication guards: $fails of $checks check(s) FAILED"
exit 1

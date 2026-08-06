#!/usr/bin/env bash
# Self-test for the WS0 rig's CELL-VOLUME guard: rows were checked and **cells** were not.
#
# Split out of `test_ws0_report_guards.sh` under the campsite rule (test target ~1500 lines)
# when eighteen review rounds took that file to 1602. The seam is by SUBJECT, and this is a
# distinct one.
#
# `test_ws0_report_guards.sh` asks whether the reporter's guards over a SESSION'S SHAPE are
# fail-closed: an absent corpus identity, a temperature-blind prewarm sentinel, an
# unobserved perf counter, an out-of-range `--reps`, an octal duration, a completeness claim
# judged against the wrong selection. Every one of those is about whether a quantity was
# validly OBSERVED at all.
#
# This file asks a different question, and round 17 is the round that found it: given a
# session whose every quantity IS observed and IS internally consistent, was the WORK the
# figure divides by actually done? A pass returning EVERY ROW WITH MISSING COLUMNS satisfies
# every check in the sibling suite — the right pass count, every pass observing exactly the
# pinned corpus row count, the recorded aggregates equal to the derived sums — while decoding
# materially less data. That is a CONTENT-VOLUME question, its oracle is a different pinned
# field (`cells_per_row` from the corpus identity, not the row count), and its non-vacuity
# probe mutates a different site of the shipped collector. So it gets its own file.
#
# EVERY CHECK BELOW MOVED VERBATIM from `test_ws0_report_guards.sh`, which measured 118
# checks before the split and 107 after; this suite runs the 11 that left it. Nothing was
# reworded, no refusal was relaxed, and both suites' `MIN_CHECKS` floors were RE-DERIVED BY
# RUNNING them rather than counted from source — a source count understates a floor because
# loops multiply.
#
# HERMETIC. The reporter is a python3 program driven over synthetic session dirs under
# $TMPDIR; nothing here invokes the measurement driver, so no case can reach
# `relax_perf_sysctls` (a host `sudo sysctl -w`), `cargo build --release` or the measurement
# loop. `scripts/tests/test_ws0_hermeticity.sh` lints that structurally over every
# `test_ws0_*.sh`, this file included, by LOCATION rather than by spelling.
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
# python3 absence is a FAILURE, not a skip (#3272 review B8). A `SKIP` + `exit 0` IS a silent
# pass: the gate's `tooling-tests` component records SUCCESS with none of the checks below
# having run, and the reassuring word is on stdout the gate does not read. python3 is a HARD
# REQUIREMENT of this rig — `ws0_report.py` IS a python3 program — so there is no environment
# where its absence means "this check is not applicable here".
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

# The shared session artifacts (`perf_csv`, `ws0_make_corpus`, `make_round`,
# `WS0_SCAN_FIXED`, `ws0_scan_pass_cells`, `ws0_pin_session_corpus`, …).
# shellcheck source=scripts/tests/lib-ws0-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-fixtures.sh"
# ...and the BARE-SCAN-ARM builders this file shares with `test_ws0_report_guards.sh`
# (`make_corpus`, `make_scan_rep`, `make_flight_rep`, `run_report`). ONE definition, sourced
# by both, for the reason `lib-ws0-fixtures.sh`'s own header gives: a builder duplicated
# across two suites is a builder that will disagree with itself.
# shellcheck source=scripts/tests/lib-ws0-scan-arm-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-scan-arm-fixtures.sh"

# The corpus every case below is measured against — built ONCE, here, because the cell
# requirement's second operand (`cells_per_row`) is a field of its recorded identity.
make_corpus "$TMP/corpus"

# ==========================================================================
# #3272 round 17 — ROWS WERE CHECKED AND **CELLS** WERE NOT
# ==========================================================================
# Round 12's F2 made the bare-scan collector require every pass to have observed the pinned corpus
# ROW COUNT, and derived the rep's rows/seconds from the per-pass records. It never read the `cells`
# counter `ws0-scan-bench` writes beside them (`scan_bench.rs`: `cells += row.values.len()`).
#
# So a pass returning EVERY ROW WITH MISSING COLUMNS satisfied every requirement — the right pass
# count, every pass observing exactly the pinned corpus row count, the recorded aggregates equal to
# the derived sums — while decoding materially less data, and its rows/s was published as the
# DENOMINATOR of the rig's only output.
#
# Why this is not counter hygiene. The rig's whole output is a bare-scan-vs-Flight ratio and its
# parent issue (#3096) exists to measure ARROW-ENCODE COST, so a corpus yielding fewer columns per
# row makes work DISAPPEAR FROM THE MEASUREMENT rather than from the validation — and an ASYMMETRIC
# shortfall moves the headline number directly. The measurement was satisfiable by thinner data than
# the report claims.
#
# The oracle already existed: `cells_per_row` is a recorded corpus-identity field, REQUIRED and
# validated `positive` by `load_corpus_identity`, printed in the report's `corpus_identity`, and 12
# on the canonical corpus. So the requirement is `cells == rows x cells_per_row` with BOTH operands
# pinned before the measurement — wiring an existing pin to a check that did not consult it, not a
# new source of truth. Compared PER PASS and DERIVED, never against the payload's own aggregate:
# round 12's F2 rule, for its reason (a thin pass beside a fat one sums to a plausible total, and a
# payload's own `cells` sum is self-consistent with any thinner scan that wrote it).
# --------------------------------------------------------------------------
# The subject: rows EXACTLY RIGHT, cells SHORT. Nine of the twelve columns, which is the realistic
# shape (`--project` narrowed to the non-key columns) and the one every pre-fix check waved through.
CELLS_SHORT=$((CORPUS_ROWS * 9))
CELLS_FULL="$(ws0_scan_pass_cells "$CORPUS_ROWS")"
make_thin_scan_rep() { # make_thin_scan_rep <dir> <cells>
  local d="$1" cells="$2" tag="scan-warm-1"
  cat > "$d/$tag.json" <<EOF
{ $WS0_SCAN_FIXED, $(ws0_scan_session_bound "${WS0_SCAN_CORPUS:-$TMP/corpus}"),
  "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5,
  "passes": [ { "pass": 0, "rows": $CORPUS_ROWS, "cells": $cells, "secs": 2.0 } ] }
EOF
  perf_csv "$d/perf-$tag.csv" 2000000 4000000
  perf_csv "$d/perf-$tag-setup.csv" 100000 200000
  printf 'ok\n' > "$d/$tag.prewarm.status"
  make_round "$d" "$tag" 1 "$(ws0_alternating_position 1 scan)"
}
d="$TMP/thin-cells"; mkdir -p "$d"
make_thin_scan_rep "$d" "$CELLS_SHORT"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
# BOTH quantities must be named — an operator has to see what was emitted and what was required.
if [ "$rc" -ne 0 ] && grep -q "emitted $(printf "%'d" "$CELLS_SHORT") cells" <<<"$out" \
   && grep -q "$(printf "%'d" "$CELLS_FULL")" <<<"$out"; then
  pass "OBSERVED (round 17): a pass returning EVERY ROW with MISSING COLUMNS is REFUSED, naming both the emitted cell count and the pinned requirement"
else
  fail "round 17: a cell-short pass must be refused naming both counts (rc=$rc, out: $out)"
fi
# ...and the refusal must state WHAT IS LOST, not merely that two numbers differ: that the ROW COUNT
# cannot see it, and that the rig's ratio is a measurement of exactly this content volume.
if grep -q "THE ROW COUNT CANNOT SEE THIS" <<<"$out" \
   && grep -q "FEWER COLUMNS PER ROW" <<<"$out" \
   && grep -q "ARROW-ENCODE COST" <<<"$out"; then
  pass "round 17: the refusal states the MEASUREMENT consequence (less work published as this figure; the ratio's subject IS the content volume), not just a mismatch"
else
  fail "round 17: the cell refusal must state what the shortfall corrupts (out: $out)"
fi
if [ ! -e "$d/results.json" ]; then
  pass "round 17: no results.json is written for the cell-short session"
else
  fail "round 17: a refused run must not leave a results.json behind"
fi
# An ABSENT `cells` is an ERROR, not an assumed full row — defaulting it would make the requirement
# pass exactly when the artifact is silent about the work done.
d="$TMP/no-cells"; mkdir -p "$d"
cat > "$d/scan-warm-1.json" <<EOF
{ $WS0_SCAN_FIXED, $(ws0_scan_session_bound "$TMP/corpus"),
  "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5,
  "passes": [ { "pass": 0, "rows": $CORPUS_ROWS, "secs": 2.0 } ] }
EOF
perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-1-setup.csv" 100000 200000
printf 'ok\n' > "$d/scan-warm-1.prewarm.status"
make_round "$d" scan-warm-1 1 "$(ws0_alternating_position 1 scan)"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -ne 0 ] && grep -q "pass 0 cells" <<<"$out"; then
  pass "round 17: an ABSENT per-pass \`cells\` is FATAL, naming the pass (never an assumed full row)"
else
  fail "round 17: an absent cells counter must be refused naming the pass (rc=$rc, out: $out)"
fi
# A cell count ABOVE the requirement is refused too, and the check is stated as the AFFIRMATIVE
# equality rather than `< required`: a thicker-than-pinned pass is an artifact this reporter does not
# model (another corpus, another projection), and `!= <bad>` is the accept-condition shape round 6
# already paid for in this file.
d="$TMP/fat-cells"; mkdir -p "$d"
make_thin_scan_rep "$d" $((CELLS_FULL + CORPUS_ROWS))
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -ne 0 ] && grep -q "MORE COLUMNS PER ROW" <<<"$out"; then
  pass "round 17: a pass emitting MORE cells than the pinned product is ALSO refused (the check is an equality, not a floor)"
else
  fail "round 17: an over-count must be refused as an equality failure (rc=$rc, out: $out)"
fi
# NON-VACUITY, MEASURED — the pre-fix collector is RUN over this exact artifact, not restated. The
# defect was that NOTHING READ THE FIELD, so it is reproduced as a ONE-SITE MUTATION of a COPY of
# the shipped reporter: the per-pass cell requirement is removed and everything else stands. A
# one-site mutation rather than a wholesale revert, for round 14 F2's reason — reverting every site
# would be a second implementation whose fidelity is a claim about my re-derivation.
r17_pre="$TMP/r17-prefix-tree"; rm -rf "$r17_pre"; mkdir -p "$r17_pre"
cp -R "$REPO_ROOT/scripts/perf" "$r17_pre/perf"
if python3 - "$r17_pre/perf/ws0_collect.py" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
anchor = "        if p_cells != required_cells:\n"
if s.count(anchor) != 1:
    raise SystemExit("could not locate the per-pass cell requirement to mutate, so this "
                     "non-vacuity probe would be measuring UNMODIFIED code and would read as a "
                     "pass having reverted nothing")
s = s.replace(anchor, "        if False:  # the PRE-FIX state: the cells counter is never compared\n")
p.write_text(s)
print("mutated the probe copy: the per-pass cell requirement no longer compares `cells`")
PY
then
  pass "round 17 NON-VACUITY: the mutation (the per-pass loop stops comparing \`cells\`) was really applied to the probe copy"
else
  fail "round 17: the pre-fix mutation could not be applied, so the probe below would measure nothing"
fi
# The SAME cell-short session, through the mutated reporter. It must EXIT 0 and PUBLISH the figure —
# that is the finding: a scan of thinner rows, reported as this arm's rows/s.
r17_out=$(python3 "$r17_pre/perf/ws0_report.py" --dir "$TMP/thin-cells" --corpus "$TMP/corpus" 2>&1)
r17_rc=$?
if [ "$r17_rc" -eq 0 ] && grep -q 'bare scan (execute_streaming)' <<<"$r17_out" \
   && grep -q '500 rows/s' <<<"$r17_out"; then
  pass "round 17 NON-VACUITY (MEASURED): with nothing comparing \`cells\`, the reporter ACCEPTS a pass that returned every row with 9 of 12 columns and PUBLISHES its 500 rows/s as this arm's figure"
else
  fail "round 17: the unmutated-comparison reporter must ACCEPT the cell-short session and publish its rows/s, else the refusal above closed nothing (rc=$r17_rc, out: $(head -8 <<<"$r17_out"))"
fi
# ...and the shortfall is INVISIBLE in that output: neither the emitted cell count nor the pinned
# requirement is named anywhere. Stronger than "unchecked" — a report naming it would be honest.
if ! grep -q "$(printf "%'d" "$CELLS_SHORT")" <<<"$r17_out" \
   && ! grep -qi 'cells' <<<"$r17_out"; then
  pass "round 17 NON-VACUITY: that accepted report never mentions cells at all (the shortfall was INVISIBLE, not merely unchecked)"
else
  fail "round 17: the pre-fix report must not name the cell shortfall (out: $(grep -i cells <<<"$r17_out" | head -3))"
fi
# ...and the MUTANT MUST NOT BE UNIFORMLY BROKEN: it still refuses a ROW-short pass, so what the
# probe measured is the loss of the CELL comparison rather than a copy that validates nothing.
d="$TMP/r17-row-short"; mkdir -p "$d"
cat > "$d/scan-warm-1.json" <<EOF
{ $WS0_SCAN_FIXED, $(ws0_scan_session_bound "$TMP/corpus"),
  "rows_denominator": 300, "timed_scan_secs": 2.0, "setup_secs": 0.5,
  "passes": [ { "pass": 0, "rows": 300, "cells": $((300 * 12)), "secs": 2.0 } ] }
EOF
perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-1-setup.csv" 100000 200000
printf 'ok\n' > "$d/scan-warm-1.prewarm.status"
make_round "$d" scan-warm-1 1 "$(ws0_alternating_position 1 scan)"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
ws0_pin_session_corpus "$d" "$TMP/corpus" 1 warm bypass 1
if r17_mut_out=$(python3 "$r17_pre/perf/ws0_report.py" --dir "$d" --corpus "$TMP/corpus" 2>&1); then
  fail "round 17: the mutant accepted a ROW-short pass too, so it lost more than the cell comparison"
else
  if grep -q 'observed 300 rows' <<<"$r17_mut_out"; then
    pass "round 17 NON-VACUITY: the mutant still REFUSES a ROW-short pass — it lost exactly the cell comparison, not its whole validation"
  else
    fail "round 17: the mutant must still refuse a row-short pass (out: $(head -4 <<<"$r17_mut_out"))"
  fi
fi
# THE ACCEPT DIRECTION, so none of the above is a guard that reds unconditionally: the SAME fixture
# with the FULL cell count reports cleanly, and the derivation is RECORDED so a reader can see the
# content volume was checked rather than assumed.
d="$TMP/full-cells"; mkdir -p "$d"
make_thin_scan_rep "$d" "$CELLS_FULL"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'bare scan (execute_streaming)' <<<"$out"; then
  pass "round 17: the SAME fixture with the PINNED cell count is ACCEPTED (the guard is not unconditional)"
else
  fail "round 17: a full-cell rep must be accepted (rc=$rc, out: $out)"
fi
if python3 - "$d/results.json" "$CELLS_FULL" <<'PY'
import json, sys
r = json.load(open(sys.argv[1])); want = int(sys.argv[2])
scan = next(m for m in r["measurements"] if m["arm"] == "bare_scan")
assert scan["cell_total"] == want, scan["cell_total"]
assert scan["cells_per_row_pinned"] == 12, scan["cells_per_row_pinned"]
rep = scan["reps"][0]
assert rep["cells"] == want, rep["cells"]
assert rep["cells_required_per_pass"] == want, rep["cells_required_per_pass"]
assert rep["passes"][0]["cells"] == want, rep["passes"][0]
assert "DERIVED" in rep["cells_source"], rep["cells_source"]
assert "MISSING COLUMNS" in rep["cells_source"], rep["cells_source"]
PY
then
  pass "round 17: results.json records the CELL total, the pinned cells/row and the per-pass cells, and states they were DERIVED against the pin"
else
  fail "round 17: results.json must record the content volume the row count cannot express"
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
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts what
# actually RAN rather than what is written in the file.
# RE-DERIVED BY RUNNING THIS SUITE, never estimated from the source: this file's 11 checks
# were MEASURED by running it after the split, and the floor is set below that. A source-line
# count understates a floor because loops multiply — an earlier split on this branch
# understated one by 29 that way (5 spellings x 5 libraries = 25 checks from 4 written lines).
MIN_CHECKS=10
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 cell-volume guards: all $checks checks passed"
  exit 0
fi
echo "ws0 cell-volume guards: $fails of $checks check(s) FAILED"
exit 1

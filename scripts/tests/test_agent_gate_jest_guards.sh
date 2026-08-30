#!/usr/bin/env bash
# Self-test for the node-bindings jest guards (issue #3522, roborev rounds 4 E1 + 5 F1):
#   check_jest_suites_ran        — the FILE-SET / aggregate half
#   check_jest_per_suite_passed  — the PER-SUITE half
#
# WHY BOTH ARE TESTED HERE, TOGETHER. They are complements, and the reason F1 existed is that
# the aggregate one was mistaken for sufficient: jest reports a file whose every test is
# individually skipped as a PASSED suite, so one suite can execute ZERO assertions while its
# siblings satisfy `Tests: N passed`. The DISCRIMINATING case is therefore one passing suite
# PLUS one all-skipped suite — a fixture that the aggregate guard accepts and the per-suite
# guard must reject. That case is the whole point of this file, and it is the case that cannot
# be produced by running the real suite, because the real suite has no all-skipped file
# (measured: 27 suites, zero with no passing test).
#
# Hermetic: synthetic jest summaries and synthetic --json reports. No node, no npm, no cargo, no
# network, no datasets. Functions are sourced OUT OF THE REAL GATE SCRIPT, never copied — a copy
# would pass while the shipped guard rotted, which is the drift this whole issue is about.
# FAILS CLOSED: an unextractable guard is a FAIL, never a skip.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
[ -r "$GATE" ] || { echo "FAIL: cannot read $GATE" >&2; exit 1; }

for fn in _ansi_stripped_log check_jest_suites_ran check_jest_per_suite_passed; do
  src=$(sed -n "/^$fn() {/,/^}$/p" "$GATE")
  [ -n "$src" ] || { echo "FAIL: could not extract $fn from $GATE — renamed or reshaped; this self-test must not pass having tested nothing" >&2; exit 1; }
  eval "$src" || { echo "FAIL: extracted $fn does not parse" >&2; exit 1; }
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/jestguards.XXXXXX") || exit 1
trap 'rm -rf "$WORK"' EXIT

PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# ---- helpers -------------------------------------------------------------------
# mk_json <file> <suite:passed:skipped> ... — a minimal jest --json report.
mk_json() {
  local out="$1"; shift
  local first=1 spec name p s i
  { printf '{"testResults":['
    for spec in "$@"; do
      name="${spec%%:*}"; p="${spec#*:}"; s="${p#*:}"; p="${p%%:*}"
      [ "$first" -eq 1 ] || printf ','
      first=0
      printf '{"name":"/repo/bindings/node/__test__/%s","status":"passed","assertionResults":[' "$name"
      i=0
      while [ "$i" -lt "$p" ]; do [ "$i" -eq 0 ] || printf ','; printf '{"status":"passed"}'; i=$((i + 1)); done
      local j=0
      while [ "$j" -lt "$s" ]; do
        if [ "$p" -gt 0 ] || [ "$j" -gt 0 ]; then printf ','; fi
        printf '{"status":"pending"}'; j=$((j + 1))
      done
      printf ']}'
    done
    printf ']}'
  } > "$out"
}
mk_set() { local out="$1"; shift; printf '%s\n' "$@" > "$out"; }

# expect_per_suite <name> <PASS|FAIL> <needle> <json> <set>
expect_per_suite() {
  local nm="$1" want="$2" needle="$3" j="$4" e="$5" rc out
  out=$(check_jest_per_suite_passed L "$j" "$e" 2>&1); rc=$?
  if [ "$want" = PASS ]; then
    [ "$rc" -eq 0 ] && ok "$nm" || bad "$nm — expected PASS, got $rc: $out"
    return
  fi
  if [ "$rc" -eq 0 ]; then bad "$nm — expected FAIL but the guard PASSED: $out"; return; fi
  case "$out" in *"$needle"*) ok "$nm" ;; *) bad "$nm — failed but never named '$needle': $out" ;; esac
}

# ---- check_jest_per_suite_passed ------------------------------------------------
mk_set "$WORK/set2" a.test.js b.test.js

# THE DISCRIMINATING CASE (F1). One suite with passing tests, one with ONLY skipped tests.
# The aggregate guard accepts this; the per-suite guard must reject it, naming the empty suite.
mk_json "$WORK/mixed.json" "a.test.js:5:0" "b.test.js:0:3"
expect_per_suite "one passing suite + one ALL-SKIPPED suite is REJECTED, naming it (F1)" \
  FAIL "b.test.js" "$WORK/mixed.json" "$WORK/set2"

# And the aggregate guard on the SAME shape ACCEPTS it — the proof that the per-suite guard is
# not redundant. Jest reports the all-skipped file as a passed suite, so the summary is green.
printf 'Test Suites: 2 passed, 2 total\nTests:       3 skipped, 5 passed, 8 total\n' > "$WORK/mixed.log"
if check_jest_suites_ran L "$WORK/mixed.log" 2 >/dev/null 2>&1; then
  ok "the AGGREGATE guard accepts that same shape — so the per-suite guard is not redundant (F1)"
else
  bad "the aggregate guard rejected the mixed shape; the discriminating case no longer discriminates, so this self-test proves nothing about F1"
fi

mk_json "$WORK/good.json" "a.test.js:5:0" "b.test.js:2:1"
expect_per_suite "every suite with >=1 passing test is ACCEPTED" PASS "" "$WORK/good.json" "$WORK/set2"

mk_json "$WORK/allzero.json" "a.test.js:0:1" "b.test.js:0:1"
expect_per_suite "ALL suites empty is REJECTED" FAIL "a.test.js" "$WORK/allzero.json" "$WORK/set2"

mk_json "$WORK/one.json" "a.test.js:5:0"
expect_per_suite "a reconciled suite ABSENT from the report is REJECTED (unjudged, not passed)" \
  FAIL "b.test.js" "$WORK/one.json" "$WORK/set2"

mk_json "$WORK/three.json" "a.test.js:1:0" "b.test.js:1:0" "c.test.js:1:0"
expect_per_suite "a reported suite NOT in the reconciled set is REJECTED, distinctly" \
  FAIL "c.test.js" "$WORK/three.json" "$WORK/set2"

printf 'not json at all\n' > "$WORK/broken.json"
expect_per_suite "an unparseable report is REJECTED" FAIL "FAIL-CLOSED" "$WORK/broken.json" "$WORK/set2"

expect_per_suite "a MISSING report is REJECTED" FAIL "missing or unreadable" "$WORK/absent.json" "$WORK/set2"

mk_set "$WORK/empty.set"
expect_per_suite "an EMPTY reconciled set is REJECTED (a guard with no subject)" \
  FAIL "FAIL-CLOSED" "$WORK/good.json" "$WORK/empty.set"

printf '{"testResults":[]}' > "$WORK/norecords.json"
expect_per_suite "a report with NO per-suite records is REJECTED" FAIL "NO per-suite records" \
  "$WORK/norecords.json" "$WORK/set2"

# ---- check_jest_suites_ran (round 4 E1: closed grammar + sum reconciliation) ----
agg() { # <name> <PASS|FAIL> <needle> <suites-line> <tests-line> <expected>
  local nm="$1" want="$2" needle="$3" rc out
  printf '%s\n%s\n' "$4" "$5" > "$WORK/agg.log"
  out=$(check_jest_suites_ran L "$WORK/agg.log" "$6" 2>&1); rc=$?
  if [ "$want" = PASS ]; then
    [ "$rc" -eq 0 ] && ok "$nm" || bad "$nm — expected PASS, got $rc: $out"
    return
  fi
  if [ "$rc" -eq 0 ]; then bad "$nm — expected FAIL but PASSED: $out"; return; fi
  case "$out" in *"$needle"*) ok "$nm" ;; *) bad "$nm — failed but never named '$needle': $out" ;; esac
}
agg "aggregate: the real 27/27 shape is ACCEPTED" PASS "" \
  'Test Suites: 27 passed, 27 total' 'Tests:       2 skipped, 504 passed, 506 total' 27
agg "aggregate: a SKIPPED suite is REJECTED" FAIL "were SKIPPED" \
  'Test Suites: 1 skipped, 26 passed, 27 total' 'Tests:       504 passed, 506 total' 27
agg "aggregate: categories that do NOT sum to total are REJECTED (E1)" FAIL "do not add up" \
  'Test Suites: 20 passed, 27 total' 'Tests:       504 passed, 506 total' 27
agg "aggregate: an UNRECOGNISED category is REJECTED first, by name (E1 closed grammar)" FAIL "obsolete" \
  'Test Suites: 3 obsolete, 24 passed, 27 total' 'Tests:       504 passed, 506 total' 27
agg "aggregate: a suite-count mismatch vs the reconciled set is REJECTED" FAIL "but 28" \
  'Test Suites: 27 passed, 27 total' 'Tests:       504 passed, 506 total' 28
agg "aggregate: ZERO passing tests overall is REJECTED" FAIL "ZERO tests PASSED" \
  'Test Suites: 27 passed, 27 total' 'Tests:       506 skipped, 506 total' 27
agg "aggregate: an absent jest summary is REJECTED" FAIL "no parseable jest summary" \
  'nothing useful here' 'nor here' 27

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]

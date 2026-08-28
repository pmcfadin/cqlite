#!/usr/bin/env bash
# Self-test for the occupancy-enforced clock derivation (scripts/perf/ws0_clock.py, #3248).
#
# WHY THIS TOOL HAS A GUARD TEST AT ALL. AC4 of #3248 asks for a reconciliation "stating
# the clock basis". This file exists because STATING IT DEMONSTRABLY DOES NOT WORK:
#
#   * #3299 published `cycles / task-clock` as a frequency. Under CPU-wide `perf stat -C`
#     that is occupancy x frequency, because `task-clock` accrues elapsed x nCPUs
#     INCLUDING IDLE CPUs. It read 1.271 "GHz" at S=4/N=1 and was retracted.
#   * That retraction OVERRODE a caption written specifically to prevent the error.
#   * Hours later the same quantity was reached for again, licensed by "matched occupancy
#     80%/80%, and that WAS measured" — where 0.80 was the counting window over perf's own
#     process lifetime (20s/25s), matched by HARNESS PARAMETERS, not by the hardware.
#
# So the prose control failed twice in the hands of people who knew about it. The bar here
# is #3272's: not "the guard exists" but "the guard has been OBSERVED to fire" (per #3249,
# where hardcoding _PERF_STATE="ok" survived 118/118 tests). Every case below feeds the
# input the guard must reject and asserts the EXIT CODE and the CAUSE TOKEN.
#
# Hermetic by construction: the tool under test consumes a perf CSV and invokes nothing.
# Every fixture here is synthetic text. No perf, no cargo, no corpus, no network, no sudo.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLOCK="$REPO_ROOT/scripts/perf/ws0_clock.py"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT INT TERM HUP

fails=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }

# A well-formed CSV taken from the shape `perf stat -x,` really emits on this box, with
# every value internally consistent: TSC at exactly 2.4 GHz over its own enabled window,
# mperf/ref-cycles at ~0.975 occupancy, aperf implying ~3.41 GHz.
# Fields: count,unit,event,enabled_ns,enabled_pct,derived,derived_unit
good_csv() {
  cat <<'CSV'
3334500000,,msr/aperf/,1000000000,100.00,3.334,G/sec
2340000000,,msr/mperf/,1000000000,100.00,2.340,G/sec
2400000000,,msr/tsc/,1000000000,100.00,2.400,G/sec
3334500000,,cycles,1000000000,100.00,3.334,GHz
2336400000,,ref-cycles,1000000000,100.00,2.336,G/sec
2000000000,,task-clock,2000000000,100.00,2.000,CPUs utilized
CSV
}

# Assert the tool REFUSES with a specific cause token.
expect_refusal() {
  local name="$1" cause="$2" csvfile="$3"; shift 3
  local out rc
  out="$(python3 "$CLOCK" "$csvfile" "$@" 2>&1)"; rc=$?
  if [ "$rc" -eq 0 ]; then
    fail "$name — exited 0; a refusal that exits 0 is not a refusal"
    return
  fi
  if ! grep -q "REFUSED: $cause" <<<"$out"; then
    fail "$name — refused, but not with cause '$cause'. Got: $(head -2 <<<"$out" | tr '\n' ' ')"
    return
  fi
  pass "$name (rc=$rc, cause=$cause)"
}

# Build a MUTATED fixture and PROVE the mutation landed.
#
# WHY THIS HELPER EXISTS, and it is the most valuable thing in this file. The first version
# of these negative cases piped the good fixture through `sed` expressions carrying literal
# counter values. When the fixture was later rewritten with different values, EVERY
# expression silently became a NO-OP: the "mutated" CSV was byte-identical to the valid one,
# so six guards were being fed PERFECTLY GOOD INPUT and the only thing that caught it was
# that they correctly accepted it. Had any case asserted the opposite polarity, it would
# have passed forever while testing nothing.
#
# That is the "hand-maintained coupling with no oracle forcing it" class that recurred FIVE
# times in #3272. So the coupling now HAS an oracle: a mutation that does not change the
# bytes is a hard failure of the suite, not a quietly vacuous case.
mutate() {
  local name="$1" expr="$2" out="$3"
  good_csv | sed "$expr" > "$out"
  if good_csv | diff -q - "$out" >/dev/null 2>&1; then
    fail "MUTATION NO-OP: '$name' — the sed expression matched nothing, so this case would " \
         "have fed the guard VALID input. Expression: $expr"
    return 1
  fi
  return 0
}

echo "== ACCEPT direction, asserted AFFIRMATIVELY =="
# NON-VACUITY: this must not merely exit 0 — it must PRODUCE a frequency, the exact TSC, two
# occupancy sources and a labelled trap value. An accept case asserting only the ABSENCE of
# an error passes on ANY unrelated failure, which is #3272 review finding 12: with a corpus
# present but perf absent, every such case "passed" while validation never ran.
good_csv > "$TMP/good.csv"
out="$(python3 "$CLOCK" "$TMP/good.csv" --json-out "$TMP/good.json" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ]; then
  fail "well-formed CSV accepted — exited $rc: $(head -2 <<<"$out" | tr '\n' ' ')"
else
  ok=1
  grep -q 'verdict: OK' <<<"$out" || { ok=0; fail "accept: no 'verdict: OK' line"; }
  grep -qE 'frequency: +3\.4200 GHz' <<<"$out" \
    || { ok=0; fail "accept: frequency is not the fixture's exact 3.4200 GHz"; }
  # THE LOAD-BEARING ASSERTION. TSC must come out at EXACTLY the fixture's nominal. This is
  # what proves each event is normalized by ITS OWN enabled_ns. perf's own derived column
  # normalizes every event by the run's elapsed instead; doing it that way on a real capture
  # from this box made msr/tsc/ read 2.474 GHz against a true 2.400, and a 3% TSC error
  # propagates into every derived quantity including the frequency.
  grep -qE 'TSC \(measured\): +2\.4000 GHz' <<<"$out" \
    || { ok=0; fail "accept: TSC did not recover the nominal exactly — per-event enabled_ns normalization is broken"; }
  grep -q '2 independent sources' <<<"$out" || { ok=0; fail "accept: did not report 2 occupancy sources"; }
  python3 - "$TMP/good.json" <<'JSONCHK' 2>"$TMP/jsonerr" || { ok=0; fail "accept: JSON assertions failed: $(cat "$TMP/jsonerr")"; }
import json, sys
r = json.load(open(sys.argv[1]))
assert r["verdict"] == "OK", r["verdict"]
assert abs(r["frequency_ghz"] - 3.42) < 1e-9, r["frequency_ghz"]
assert r["tsc_ghz_measured"] == 2.4, r["tsc_ghz_measured"]
assert len(r["occupancy"]["sources"]) == 2, r["occupancy"]
t = r["occupancy_times_frequency_NOT_A_CLOCK"]
# The trap must be DRAMATICALLY wrong on this fixture, not marginally. Two pinned CPUs with
# one busy gives task-clock = 2x elapsed, so cycles/task-clock reads HALF the true clock —
# the same shape as #3299's 1.271 "GHz" (one busy core diluted across eight pinned CPUs).
assert abs(t["value_ghz_LOOKS_LIKE"] - 1.66725) < 1e-9, t["value_ghz_LOOKS_LIKE"]
assert r["frequency_ghz"] / t["value_ghz_LOOKS_LIKE"] > 2.0, "trap is not detectably wrong"
assert "NOT a frequency" in t["WARNING"], t["WARNING"]
JSONCHK
  [ "$ok" -eq 1 ] && pass "well-formed CSV: exact frequency + exact TSC + 2 occupancy sources + labelled trap value"
fi

echo "== REFUSAL direction: every guard fed the input it must reject =="

# A counter that was not observed is an ERROR, never a fabricated 0 (#3272 f4's shape:
# `.get("cycles", 0)` let a run be reported "setup-subtracted" having subtracted nothing).
mutate "not-counted aperf" 's|^3334500000,,msr/aperf/,|<not counted>,,msr/aperf/,|' "$TMP/notcounted.csv" \
  && expect_refusal "perf '<not counted>' marker" PERF_COUNTER_NOT_OBSERVED "$TMP/notcounted.csv"

# A FRACTIONAL count means perf SCALED it for multiplexing — an estimate, not a measurement.
mutate "fractional tsc" 's|^2400000000,,msr/tsc/,|2400000000.5,,msr/tsc/,|' "$TMP/frac.csv" \
  && expect_refusal "fractional (scaled) count" PERF_COUNTER_UNPARSEABLE "$TMP/frac.csv"

# enabled_ns == 0: the counter never ran, so there is no rate to form.
mutate "mperf never enabled" 's|^2340000000,,msr/mperf/,1000000000,|2340000000,,msr/mperf/,0,|' "$TMP/never.csv" \
  && expect_refusal "counter enabled for 0 ns" PERF_COUNTER_NEVER_ENABLED "$TMP/never.csv"

# MULTIPLEXING. Below 100% enabled, perf scales the counts. This is the case most likely to
# occur in real use the moment the event set grows, and it is invisible in perf's output
# except for this one column.
mutate "cycles multiplexed" 's|,,cycles,1000000000,100.00,|,,cycles,1000000000,62.50,|' "$TMP/mux.csv" \
  && expect_refusal "multiplexed event (enabled 62.5%)" PERF_MULTIPLEXED "$TMP/mux.csv"

mutate "tsc pct unparseable" 's|^2400000000,,msr/tsc/,1000000000,100.00,|2400000000,,msr/tsc/,1000000000,not-a-number,|' "$TMP/pct.csv" \
  && expect_refusal "unparseable enabled-percentage" PERF_ENABLED_PCT_UNPARSEABLE "$TMP/pct.csv"

: > "$TMP/empty.csv"
expect_refusal "empty input" PERF_CSV_EMPTY "$TMP/empty.csv"

printf '# just a comment\n\n' > "$TMP/comments.csv"
expect_refusal "comments-only input" PERF_CSV_EMPTY "$TMP/comments.csv"

# Missing a REQUIRED event: without TSC there is no occupancy-free basis at all.
mutate "drop tsc" '/msr\/tsc\//d' "$TMP/notsc.csv" \
  && expect_refusal "msr/tsc/ absent" FREQ_EVENTS_ABSENT "$TMP/notsc.csv"

# ONE occupancy source is not enough: a single source cannot detect its own failure. This is
# the #3299 shape exactly — one plausible-looking number and no cross-check.
mutate "drop ref-cycles" '/ref-cycles/d' "$TMP/onesource.csv" \
  && expect_refusal "only one occupancy source" FREQ_OCCUPANCY_ABSENT "$TMP/onesource.csv"

# DISAGREEING occupancy sources: ref-cycles perturbed to 0.70 occupancy against mperf's
# 0.975. They measure the same physical quantity, so a disagreement means at least one is
# wrong and nothing says which.
mutate "perturb ref-cycles" 's|^2336400000,,ref-cycles,|1680000000,,ref-cycles,|' "$TMP/mismatch.csv" \
  && expect_refusal "occupancy sources disagree" FREQ_OCCUPANCY_MISMATCH "$TMP/mismatch.csv"

# THE ESCAPE HATCH MUST NOT EXIST. The tolerance knob may only TIGHTEN; a looser value is
# refused, because an escape hatch on a measurement guard can only buy a wrong number.
expect_refusal "tolerance loosened above the default" OCCUPANCY_TOLERANCE_LOOSENED \
  "$TMP/good.csv" --occupancy-tolerance 0.5

# ...and tightening must still WORK, or the knob is decorative.
out="$(python3 "$CLOCK" "$TMP/good.csv" --occupancy-tolerance 0.001 2>&1)"; rc=$?
if [ "$rc" -eq 0 ]; then
  fail "tightened tolerance 0.001 accepted a 0.0015 spread — the knob does not tighten"
elif grep -q 'REFUSED: FREQ_OCCUPANCY_MISMATCH' <<<"$out"; then
  pass "tightened tolerance refuses a spread it should (knob tightens for real)"
else
  fail "tightened tolerance failed with an unexpected cause: $(head -2 <<<"$out" | tr '\n' ' ')"
fi

echo "== STRUCTURAL: no env-var escape hatch may be introduced =="
# The constrained party must not choose its own enforcer. An env override on a measurement
# guard is settable by exactly the party the guard constrains, so its ABSENCE is asserted
# structurally rather than left to review.
#
# NOTE ON THIS CHECK'S OWN FIRST VERSION, because it is this file's subject one level down:
# it grepped for /environ/ and MATCHED THE WORD "environment" IN THE TOOL'S OWN DOCSTRING —
# a guard firing on its own explanatory prose. It reported a defect that did not exist, and
# a guard that cries wolf on correct input is the guard people learn to delete. It now
# inspects the PARSED SYNTAX TREE, where prose cannot reach.
if python3 "$REPO_ROOT/scripts/tests/ws0_assert_no_env_access.py" "$CLOCK"; then
  pass "no environment access in the ws0_clock.py syntax tree (asserted on the AST, not on prose)"
else
  fail "ws0_clock.py reads the environment — a measurement guard must not be env-tunable"
fi

# NON-VACUITY for the check above: it must actually FAIL on a file that DOES read the env,
# or an AST walk that silently matches nothing would "pass" forever. The needle is assembled
# at runtime so this file cannot match itself.
mkdir -p "$TMP/vac"
{ printf '%s\n' "import os"; printf '%s\n' "x = os.environ.get(\"WS0_CLOCK_TOL\")"; } > "$TMP/vac/leaky.py"
if python3 "$REPO_ROOT/scripts/tests/ws0_assert_no_env_access.py" "$TMP/vac/leaky.py" >/dev/null 2>&1; then
  fail "NON-VACUITY: the env-access detector passed a file that plainly reads os.environ"
else
  pass "NON-VACUITY: the env-access detector fires on a file that reads os.environ"
fi

# The window/lifetime ratio must be carried under a name that denies it is an occupancy, and
# must NOT be one of the agreement sources. This is the exact quantity #3299 mistook for a
# measured occupancy ("80%/80%, and that WAS measured").
if ! grep -q "window_over_lifetime_NOT_AN_OCCUPANCY" "$CLOCK"; then
  fail "the window/lifetime ratio is not labelled NOT_AN_OCCUPANCY"
elif python3 "$REPO_ROOT/scripts/tests/ws0_assert_window_ratio_excluded.py" "$CLOCK"; then
  pass "window/lifetime ratio recorded, labelled NOT_AN_OCCUPANCY, and excluded from the agreement check"
else
  fail "window/lifetime ratio leaked into the occupancy agreement sources"
fi
echo
if [ "$fails" -eq 0 ]; then
  echo "test_ws0_clock_guards: PASS"
  exit 0
fi
echo "test_ws0_clock_guards: FAIL ($fails)"
exit 1

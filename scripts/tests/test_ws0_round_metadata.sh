#!/usr/bin/env bash
# Self-test for the WS0 rig's PER-REP ROUND METADATA (issue #3272; split out in review round 4).
#
# Split from `test_ws0_fabrication_guards.sh` under the campsite rule (~1500-line test target),
# by SUBJECT rather than by size: that file's subject is "a counter or verdict that was not
# observed is an ERROR, never a default", and this file's is the per-rep round metadata —
#
#   * the driver's LOOP ORDER (rounds outside, arms inside, rotated, bare scan as a peer);
#   * the four RECORDED fields every rep must carry, and the refusal of a partial record;
#   * the artifact-set INTEGRITY refusals over them (same round set per arm, positions 1..n
#     exactly once, arms_in_round matching, no duplicate instant, labels not contradicting
#     instants, no arm at a fixed position);
#   * and the ROUND-4 property: NO INTERLEAVING OR ORDERING CLAIM IS MADE, on ANY session shape.
#
# That last one is why the split is worth making rather than just trimming. Round 4 found the
# interleaving claim FALSE at the rig's own default — at `--reps 1` there is one round, so
# `zip(ordered, ordered[1:])` is empty, ZERO orderings were compared, and the code still
# returned `round_major_verified: True` and printed "the reps were INTERLEAVED … OBSERVED FROM
# THE CLOCK". By owner ruling the claim was DELETED rather than re-worded a fourth time. The
# assertions that it stays deleted are a subject of their own, and they belong together.
#
# Re-adding an OBSERVED drift control on real hardware is tracked by #3287/#3299.
#
# Hermetic: synthetic session dirs, synthetic perf CSVs, and a synthetic multi-byte `Data.db`
# whose real sha256 is computed with python3's hashlib. No cargo, perf, sudo, corpus, network
# or root.
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
# run_report_cfg <dir> <corpus> <reps> <temps> <arms> <scan-passes> — the reporter over a
# session whose MANIFEST carries the given configuration (#3272 F1).
#
# The configuration is no longer a reporter argument: it is a property of the SESSION, stamped
# before the first rep, and the reporter READS it. So a case that wants 3 reps stamps 3 reps
# rather than asking the reporter for them — which is the whole point of F1, since asking was
# what let a re-report substitute a configuration and claim it had been verified.
run_report_cfg() {
  local d="$1" c="$2"
  rm -f "$d/session-corpus-pin.json"
  ws0_pin_session_corpus "$d" "$c" "$3" "$4" "$5" "$6"
  python3 "$REPORT" --dir "$d" --corpus "$c" 2>&1
}

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

# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"

GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"__TAG__","endpoint":"__ENDPOINT__","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'

make_corpus "$TMP/corpus"

# ==========================================================================
# 7 — the LOOP ORDER is rounds-outside/arms-inside/rotated, and the comparison is
#     differenced WITHIN a round
# ==========================================================================
# SCOPE, stated first because round 4 turned on exactly this confusion. What is asserted
# below is a property of the DRIVER'S LOOP ORDER, driven directly. It is NOT a claim that any
# session was interleaved, and the rig does not make one: see §3b.1 of
# `docs/reports/ws0-3096-artifacts/measurement-method.md` — the specified drift control is NOT
# IMPLEMENTED OR ENFORCED, and re-adding an OBSERVED one is #3287/#3299.
#
# The ordering is still worth asserting because the ALTERNATIVE is measurably worse. §3b
# requires (1) "run one rep at a time, never all reps of an arm back to back", (2) "rotate the
# arm order every round so no arm holds a fixed position", (4) "difference within a round …
# not the medians alone" — and the rule was paid for: the UNTOUCHED warm bare scan read
# 370,134 rows/s and 333,206 rows/s an hour later on the same box, ~10% drift with nothing
# changed on the measured path. The pre-fix driver ran ALL bare-scan reps, then all Flight reps
# of arm 1, then all of arm 2, so that drift landed directly on the `bare/flight` ratio and
# the 1.3x verdict. Ordering the loop this way removes that specific structural hazard; it
# does not measure drift, and nothing downstream verifies it happened.
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
  fail "the loop must run one rep of EVERY arm per round, scan included (order: $got)"
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
  # ESCAPED backticks: unescaped they are COMMAND SUBSTITUTION, and this label really did run
  # `scan` (a `command not found` on stderr) and print itself as "…includes  as a peer" — the
  # same defect as the fabrication suite's `\`> 0\`` label, which additionally created a file.
  pass "STRUCTURAL: the rotated arm list includes \`scan\` as a peer of the Flight arms"
else
  fail "the rotated list must be (scan \$ARMS): rotating only the Flight arms is R4a"
fi

# --- the reporter differences WITHIN a round --------------------------------
# Interleaving the driver is half the fix; the other half is that the REPORT states the
# paired per-round comparison rather than only the median-vs-median difference. The
# recorded case for that: #3096's lever 4 measured `+4,817 rows/s / +2.3%` by medians
# and ZERO over 8 rounds (median −0.03%, 4 of 8 rounds positive).
d="$TMP/paired"; mkdir -p "$d"
# Three rounds where the MEDIAN favours flight but the per-round direction is split —
# the exact shape a median-only reading misreports.
for rep in 1 2 3; do
  make_scan_rep "$d" warm "$rep" ok
done
python3 - "$d" "$CORPUS_ROWS" "$WS0_FIXTURE_ENDPOINT" <<'PY'
import json, pathlib, sys
d, rows, endpoint = pathlib.Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
# flight rows/s per round: two rounds below the bare scan's 500/s (1000 rows / 2.0s),
# one above — so 1 of 3 rounds meets a 1.3x target while the median does not.
#
# The rate is varied by varying `duration_s`, with `rows_per_s` DERIVED from it, because the
# reporter now derives the throughput from `rows_total/duration_s` and cross-checks the
# recorded rate against it (#3272 review round 4). A fixture that varied `rows_per_s` alone
# beside a fixed duration is exactly the inconsistent record the new check refuses — and it
# would be refused here for a reason unrelated to this case's subject (the per-round pairing).
for rep, rps in ((1, 300.0), (2, 480.0), (3, 200.0)):
    tag = f"flight-bypass-warm-{rep}"
    secs = rows / rps
    (d / f"{tag}.jsonl").write_text(json.dumps({
        # `requests_unavailable` at its HEALTHY value: the reporter REQUIRES the
        # admission-shed counter (#3272 F4), so omitting it is refused — correctly, but for a
        # reason unrelated to this case's subject (the per-round pairing).
        # ...and the FIXED INPUTS at the values the driver fixes them to (#3272 F3), for the same
        # reason: the reporter REQUIRES them, so omitting one is refused correctly but for a
        # reason unrelated to this case's subject.
        "schema": "flight-loadgen.step/v1", "step": 0, "target_concurrency": 1, "shape": "full",
        # ...and the SESSION-BOUND inputs at the values this session pinned (#3272 round 14,
        # F1/F2): the rep's own tag, and the manifest's pinned flight endpoint. Both are REQUIRED
        # and compared, so omitting either is refused correctly but for a reason unrelated to this
        # case's subject.
        "round": tag, "endpoint": endpoint,
        "requests_ok": 1, "requests_error": 0, "requests_unavailable": 0,
        "rows_total": rows, "rows_per_s": rows / secs, "duration_s": secs}) + "\n")
    (d / f"perf-{tag}.csv").write_text("8000000,,cycles,,,,\n16000000,,instructions,,,,\n")
    (d / f"{tag}.prewarm.status").write_text("ok\n")
    # The round metadata the reporter REQUIRES, alternating position by
    # round exactly as the driver does — the scan fixture takes the complement.
    # `monotonic_ns` too (#3272 review round 3, B3): round-major and distinct, which is
    # the shape a real sequential loop produces and the property the reporter verifies.
    pos = 1 if rep % 2 == 0 else 2
    (d / f"{tag}.round").write_text(
        f"round={rep}\nposition={pos}\narms_in_round=2\n"
        f"monotonic_ns={rep * 10**9 + pos * 10**6}\n")
PY
out=$(run_report_cfg "$d" "$TMP/corpus" 3 warm bypass 1); rc=$?
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
# Compared with a tolerance, because the flight rate is now DERIVED as
# `rows_total/duration_s` and the fixture derives `duration_s` from the target rate — so the
# round trip lands within float epsilon rather than exactly on it (#3272 round 4).
want = [300.0, 480.0, 200.0]
got = [r["flight_rows_per_sec"] for r in rounds]
assert all(abs(g - w) < 1e-9 for g, w in zip(got, want)), got
assert [r["flight_meets_target"] for r in rounds] == [False, True, False], rounds
PY
then
  pass "results.json records the per-round PAIRED comparison, rep-for-rep"
else
  fail "results.json must record the paired per-round records"
fi
# ==========================================================================
# R3/round-4 — the round metadata is REQUIRED and INTEGRITY-CHECKED, and NO CLAIM
# is made from it
# ==========================================================================
# HISTORY, because it is the reason this section is shaped the way it is. Round 1's
# reporter printed
#
#   "the reps were INTERLEAVED — one rep per arm per round, arm order rotated"
#
# UNCONDITIONALLY, while `paired_rounds` paired by REP INDEX and read NOTHING the driver
# recorded. Rounds 2 and 3 made the claim conditional and then "clock-observed" — and round
# 4 found the clock-observed version FALSE at the rig's own default: at `--reps 1` there is
# one round, `zip(ordered, ordered[1:])` is EMPTY, so ZERO orderings were compared while
# `round_major_verified` still said `True` and the sentence still printed.
#
# By owner ruling the CLAIM WAS DELETED (not re-worded a fourth time). What remains, and
# what this section asserts: the metadata is REQUIRED, the pairing is by the RECORDED
# round, the artifact set is INTEGRITY-CHECKED against itself, and the report makes NO
# ordering claim on ANY session shape.
d="$TMP/no-round-meta"; make_session "$d" "$GOOD_FLIGHT"
rm -f "$d"/*.round
expect_reject "a session with NO round metadata is REFUSED (unattributable, unpairable)" \
  "has no round metadata" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "the per-round pairing has nothing to pair" <<<"$out"; then
  pass "the refusal says what is lost (attribution + pairing), not merely 'a file is missing'"
else
  fail "the round-metadata refusal must name what it protects (out: $out)"
fi
# ...and NOTHING is written: a report that cannot establish its own headline property must
# not leave a results.json a later reader could quote.
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written when the round metadata is absent"
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
if grep -q "A partial record is refused rather than defaulted" <<<"$out" \
  && grep -q "no ORDERING property is derived from these" <<<"$out"; then
  pass "the refusal states the field is REQUIRED RECORDED DATA and that no ordering claim rests on it"
else
  fail "the position refusal must name what it is and disclaim the ordering property (out: $out)"
fi
d="$TMP/round-meta-garbage"; make_session "$d" "$GOOD_FLIGHT"
printf 'round=one\nposition=1\narms_in_round=2\n' > "$d/scan-warm-1.round"
expect_reject "an unparseable round field is REFUSED (a corrupt field is not a zero)" \
  "not an integer" "$d" "$TMP/corpus"
d="$TMP/round-meta-mismatch"; make_session "$d" "$GOOD_FLIGHT"
make_round "$d" scan-warm-1 7 1 2 1000000
expect_reject "a round that disagrees with the rep index in the FILENAME is REFUSED" \
  "does not describe one session" "$d" "$TMP/corpus"

# THE FIXED-POSITION REFUSAL, as a PRODUCER-CONTRACT check. Two arms over two rounds with
# the SCAN AT POSITION 1 BOTH TIMES is exactly what the round-1 driver produced for the
# default `--arm bypass`, and `rotate_arms` cannot produce it — so the artifact set was not
# written by this driver's loop and must be refused. It licenses NO claim (#3272 round 4).
d="$TMP/no-rotation"; mkdir -p "$d"
for rep in 1 2; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
  # SCAN AT POSITION 1 BOTH ROUNDS, with non-contradictory instants, so the refusal below is
  # attributable to the FIXED POSITION alone and not to the label/instant check.
  make_round "$d" "scan-warm-$rep" "$rep" 1 2 "$(( rep * 1000000000 + 1000000 ))"
  make_round "$d" "flight-bypass-warm-$rep" "$rep" 2 2 "$(( rep * 1000000000 + 2000000 ))"
done
out=$(run_report_cfg "$d" "$TMP/corpus" 2 warm bypass 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "held ONE FIXED POSITION" <<<"$out" \
  && grep -q "bare_scan" <<<"$out"; then
  pass "OBSERVED: an arm at a FIXED position across rounds is REFUSED, naming the arm (R4a)"
else
  fail "a fixed arm position must be refused (rc=$rc, out: $out)"
fi
# ...and the refusal must say what it IS — a producer-contract check — rather than imply the
# rig verified a rotation.
if grep -q "PRODUCER-CONTRACT refusal, not a drift control" <<<"$out" \
  && grep -q "no rotation or interleaving claim" <<<"$out"; then
  pass "the fixed-position refusal disclaims being a drift control (#3272 round 4)"
else
  fail "the fixed-position refusal must not read as a verified rotation (out: $out)"
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

# THE ACCEPT DIRECTION, affirmatively: a complete session is ACCEPTED, its round metadata
# is RECORDED VERBATIM in results.json, and the report says — in words — that it makes no
# interleaving/ordering claim and that the drift control is not implemented.
d="$TMP/rotated-ok"; mkdir -p "$d"
for rep in 1 2; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
done
out=$(run_report_cfg "$d" "$TMP/corpus" 2 warm bypass 1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "makes NO INTERLEAVING CLAIM and NO ROUND-ORDERING CLAIM" <<<"$out" \
  && grep -q "INERT RECORDED DATA" <<<"$out"; then
  pass "OBSERVED: an accepted session states that NO interleaving/ordering claim is made"
else
  fail "the report must disclaim the interleaving/ordering property (rc=$rc, out: $out)"
fi
# ...and it must name the ABSENT CONTROL and where it is tracked, so a reader is not left to
# infer from silence that the §3b control ran.
if grep -q "NOT IMPLEMENTED OR ENFORCED here" <<<"$out" \
  && grep -q "#3287/#3299" <<<"$out" \
  && grep -q "UNCONTROLLED for drift" <<<"$out"; then
  pass "the report names the ABSENT drift control, the tracking issues, and the consequence"
else
  fail "the report must state that the drift control is not implemented (out: $out)"
fi
# THE DELETED KEYS MUST BE GONE, everywhere in the document. That property holds of EVERY
# report this rig writes, so it lives in ONE place three suites call
# (`scripts/tests/ws0_assert_no_verdict_fields.py`) rather than in a heredoc per call site,
# and it is asked over JSON KEYS rather than the serialized text — the replacement prose
# legitimately says the word "interleaving" (it says the rig makes none).
if python3 "$REPO_ROOT/scripts/tests/ws0_assert_no_verdict_fields.py" "$d/results.json" >/dev/null; then
  pass "results.json carries NONE of the 13 deleted interleaving verdict fields (shared assert)"
else
  fail "results.json still carries a deleted verdict field: $(python3 "$REPO_ROOT/scripts/tests/ws0_assert_no_verdict_fields.py" "$d/results.json" 2>&1)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
rec = r["recorded_round_metadata"]["warm"]
assert rec["claims_made"] == "NONE", rec
assert "no interleaving" in rec["claim_note"], rec
assert "#3287/#3299" in rec["claim_note"], rec
assert "UNVERIFIED" in rec["source"], rec
assert rec["rounds_recorded"] == [1, 2], rec
assert rec["arms_per_round_recorded"] == 2, rec
# The RECORDED positions and instants are carried through verbatim.
pos = [rec["positions_by_round_recorded"][str(k)]["bare_scan"] for k in (1, 2)]
assert sorted(pos) == [1, 2], pos
assert set(rec["instants_by_round_recorded"]) == {"1", "2"}, rec
# The integrity SCOPE is a COUNT, not a verdict: 2 rounds => 1 consecutive pair.
integ = rec["integrity_checks"]
assert integ["round_pairs_compared"] == 1, integ
assert integ["reps_examined"] == 4, integ
assert "NOT a verdict" in integ["scope_note"], integ
# ...and every rep carries the round it RECORDED, plus its position.
for m in r["measurements"]:
    for rep in m["reps"]:
        assert rep["round"] == rep["rep"], rep
        assert rep["position_in_round"] in (1, 2), rep
        assert rep["arms_in_round"] == 2, rep
PY
then
  pass "results.json RECORDS the round metadata and carries NO verdict field (deleted keys absent)"
else
  fail "results.json must record the metadata without any verdict field (out: $out)"
fi
# ==========================================================================
# ROUND 4 — NO INTERLEAVING CLAIM ON *ANY* SESSION SHAPE
# ==========================================================================
# This is the assertion the deleted claim could not satisfy. At ONE round `zip(ordered,
# ordered[1:])` is empty, so the pre-fix code compared ZERO orderings and still printed "the
# reps were INTERLEAVED … OBSERVED FROM THE CLOCK … every rep of round r finished before any
# rep of round r+1" with `round_major_verified: True`. MEASURED on that revision:
# `$TMP/one-round` exited 0 and printed the sentence verbatim, and the only assertion over it
# checked the ROTATION text, so the timing half went unexamined.
#
# So the property is now asserted over EVERY session shape a legal run can have — one round,
# many rounds — and over a FORGED one, at the level of the FORBIDDEN PHRASES rather than a
# single expected sentence. Phrases, because the failure mode is a claim reappearing in new
# words: any of these in the transcript is a finding.
claim_phrases=(
  'were INTERLEAVED'
  'reps were INTERLEAVED'
  'OBSERVED FROM THE CLOCK'
  'round-major'
  'round-major ordering'
  'ORDER ROTATED'
  'finished before any rep of round'
)
no_claim_probe() { # no_claim_probe <label> <transcript>
  local label="$1" transcript="$2" phrase hit=""
  for phrase in "${claim_phrases[@]}"; do
    if grep -qi -- "$phrase" <<<"$transcript"; then hit="$phrase"; break; fi
  done
  if [ -z "$hit" ]; then
    pass "no-claim: $label prints NO interleaving/ordering claim (all ${#claim_phrases[@]} phrases absent)"
  else
    fail "no-claim: $label printed the forbidden phrase '$hit' (out: $transcript)"
  fi
}
# The MANY-ROUND shape (the transcript captured just above).
no_claim_probe "a 2-round session" "$out"
# The ONE-ROUND shape — the `--reps 1` default, and the exact case round 4 flagged.
d="$TMP/one-round"; make_session "$d" "$GOOD_FLIGHT"
out_one=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "a ONE-ROUND session is still ACCEPTED (the claim was deleted, not the report)"
else
  fail "a one-round session must be accepted (rc=$rc, out: $out_one)"
fi
no_claim_probe "the ONE-ROUND default (--reps 1)" "$out_one"
# ...and at one round the recorded scope must SAY zero orderings were comparable, rather than
# omitting the number — the count is what makes the absence of a claim legible.
if python3 "$REPO_ROOT/scripts/tests/ws0_assert_no_verdict_fields.py" "$d/results.json" >/dev/null \
  && python3 - "$d/results.json" <<'PY'
import json, sys
integ = json.load(open(sys.argv[1]))["recorded_round_metadata"]["warm"]["integrity_checks"]
assert integ["round_pairs_compared"] == 0, integ
assert "NOT a verdict" in integ["scope_note"], integ
PY
then
  pass "OBSERVED: at ONE round results.json records round_pairs_compared=0 and NO verdict (round 4's finding)"
else
  fail "the one-round session must record a ZERO comparison count and no verdict"
fi
if grep -q "it is 0 and no ordering was compared" <<<"$out_one"; then
  pass "the one-round transcript SAYS zero orderings were compared (an absence made legible)"
else
  fail "the one-round report must state that no ordering was compared (out: $out_one)"
fi
# STRUCTURAL: the reporter and the rounds module must carry NO claim-bearing sentence and no
# verdict-producing function at all. Docstrings are stripped first (prose necessarily quotes
# the claim it removed), so this scans EXECUTABLE code — the same technique the earlier
# rounds used, pointed at the phrases and the identifiers instead.
if python3 - "$REPO_ROOT/scripts/perf/ws0_report.py" "$REPO_ROOT/scripts/perf/ws0_rounds.py" <<'PY'
import ast, sys
BANNED_TEXT = ("were INTERLEAVED", "OBSERVED FROM THE CLOCK", "ORDER ROTATED",
               "round-major ordering", "finished before any rep of round")
BANNED_NAMES = ("verify_interleaving", "verify_round_major_timing", "interleaving_lines",
                "round_major_verified", "rotation_checked")
for path in sys.argv[1:]:
    tree = ast.parse(open(path).read())
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            b = node.body
            if b and isinstance(b[0], ast.Expr) and isinstance(b[0].value, ast.Constant) \
                    and isinstance(b[0].value.value, str):
                node.body = b[1:] or [ast.Pass()]
    code = ast.unparse(ast.fix_missing_locations(tree))
    for bad in BANNED_TEXT:
        if bad in code:
            raise SystemExit(f"{path} still carries the claim text {bad!r}")
    for bad in BANNED_NAMES:
        if bad in code:
            raise SystemExit(f"{path} still carries the verdict identifier {bad!r}")
PY
then
  pass "STRUCTURAL: neither reporter module carries a claim sentence or a verdict identifier"
else
  fail "a deleted interleaving claim/verdict has returned to the reporting path"
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
  fail "ws0-baseline.sh must record all three round-metadata fields per rep"
fi
# ...and NO FILE of the rig may EMIT the deleted claim. The claim came back twice in new
# words, so the whole `scripts/perf/` tree is scanned for the forbidden PHRASES — asked by
# LOCATION, not by a per-line marker: python docstrings are stripped through `ast` and shell
# full-line comments are dropped, so what is scanned is the text a run can PRINT. That is why
# the historical explanations (which say the claim was deleted, and must stay) do not trip it
# while an `echo`/`raise`/f-string carrying the claim would.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import ast, pathlib, sys
BANNED = ("were INTERLEAVED", "OBSERVED FROM THE CLOCK", "ORDER ROTATED",
          "round-major ordering", "finished before any rep of round")
d = pathlib.Path(sys.argv[1])
py = sorted(d.glob("*.py"))
sh = sorted(d.glob("*.sh"))
if not py or not sh:
    raise SystemExit(f"the scan's SUBJECT is empty ({len(py)} py, {len(sh)} sh) in {d}")
bad = []
for p in py:
    tree = ast.parse(p.read_text())
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            b = node.body
            if b and isinstance(b[0], ast.Expr) and isinstance(b[0].value, ast.Constant) \
                    and isinstance(b[0].value.value, str):
                node.body = b[1:] or [ast.Pass()]
    code = ast.unparse(ast.fix_missing_locations(tree))
    bad += [f"{p.name} (executable code): {ph}" for ph in BANNED if ph in code]
for p in sh:
    for n, line in enumerate(p.read_text().splitlines(), 1):
        if line.lstrip().startswith("#"):
            continue
        bad += [f"{p.name}:{n}: {ph}" for ph in BANNED if ph in line]
if bad:
    raise SystemExit("the deleted claim text can be EMITTED again:\n" + "\n".join(bad))
print(f"scanned {len(py)} python + {len(sh)} shell file(s)")
PY
then
  pass "STRUCTURAL: no file in scripts/perf/ can EMIT the deleted claim text (docstrings/comments stripped)"
else
  fail "the deleted interleaving claim text has reappeared on an emitting path in scripts/perf/"
fi

# ==========================================================================
# THE RECORD MUST BE **THIS REP'S** — SWAPPED JSONL FILES (#3272 round 14, F1)
# ==========================================================================
# `round` was REQUIRED PRESENT and never compared to anything, so a record could sit in ANOTHER
# rep's filename and be reported as that rep's measurement. That is not a hypothetical shape: the
# reporter locates a rep's PERF COUNTERS (`perf-<tag>.csv`) and its ROUND METADATA (`<tag>.round`)
# by the TAG IN THE FILENAME, and reads its rows and duration from the JSONL under that same name.
# Swap two reps' JSONL files and rep 1's rows are divided by rep 2's cycles, under rep 2's round
# label — a corrupted `cycles/row` and a mis-attributed paired comparison, out of an artifact set
# that is entirely self-consistent on disk. Nothing else in the rig can see it: every counter is
# valid, every file is present, both reps really were measured.
#
# The swap is EXECUTED rather than simulated by hand-editing one field, because the property is
# "this record belongs to this rep", and a swap is the way a real session acquires the defect (two
# reps into one `--out`, a salvaged file, a rename).
d="$TMP/swapped-jsonl"; mkdir -p "$d"
for rep in 1 2; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
done
# Distinct ROW COUNTS, so the swap is a real corruption of a figure and not merely a relabelling:
# post-swap, rep 1's file holds rep 2's rows. Both are exact multiples of the corpus row count, so
# the full-corpus check cannot catch it either — which is the point.
python3 - "$d" "$CORPUS_ROWS" "$WS0_FIXTURE_ENDPOINT" <<'PY'
import pathlib, sys
d, rows, endpoint = pathlib.Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
for rep, mult in ((1, 1), (2, 2)):
    tag = f"flight-bypass-warm-{rep}"
    # The record is REP 2's when rep == 2 — written correctly first, then swapped below.
    # `endpoint` is the SESSION'S PINNED one in BOTH records (#3272 round 14, F2), so the swap's
    # only defect is the one this case is about: a record in the wrong rep's filename. A differing
    # endpoint would make the refusal ambiguous between two guards.
    (d / f"{tag}.jsonl").write_text(
        '{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full",'
        f'"round":"{tag}","endpoint":"{endpoint}","requests_ok":{mult},"requests_error":0,"requests_unavailable":0,'
        f'"rows_total":{rows * mult},"rows_per_s":{rows * mult / 4.0},"duration_s":4.0}}'
        "\n"
    )
a, b = d / "flight-bypass-warm-1.jsonl", d / "flight-bypass-warm-2.jsonl"
a_text, b_text = a.read_text(), b.read_text()
a.write_text(b_text)
b.write_text(a_text)
PY
out=$(run_report_cfg "$d" "$TMP/corpus" 2 warm bypass 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "recorded \`round\`" <<<"$out"; then
  pass "OBSERVED: two reps' SWAPPED JSONL files are REFUSED — the record must carry the tag it was found under (#3272 round 14, F1)"
else
  fail "a swapped rep JSONL must be refused: rep 1's rows would be divided by rep 2's cycles (rc=$rc, out: $out)"
fi
# The refusal must name WHAT IS LOST, not merely that two strings differ — an operator reading
# "round != tag" cannot tell whether it matters.
if grep -q "combined with ANOTHER rep's perf counters" <<<"$out"; then
  pass "the swapped-record refusal names the CONSEQUENCE (another rep's perf counters), not just a mismatch"
else
  fail "the swapped-record refusal must state what the mismatch corrupts (out: $out)"
fi
# ...and NOTHING is written, so no later reader can quote the corrupted figure.
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written for the swapped-JSONL session"
else
  fail "a refused run must not leave a results.json behind"
fi
# NON-VACUITY, measured: the PRE-FIX rule was "`round` is present", and the swapped records satisfy
# it — both files carry a `round` key. Asserted against the swapped artifacts THEMSELVES rather
# than restated as a claim, so this case cannot be about an input the old code would have refused.
if python3 - "$d" <<'PY'
import json, pathlib, sys
d = pathlib.Path(sys.argv[1])
recs = {}
for rep in (1, 2):
    tag = f"flight-bypass-warm-{rep}"
    recs[tag] = json.loads((d / f"{tag}.jsonl").read_text().strip())
# The PRE-FIX predicate, in substance: presence, and nothing else.
for tag, rec in recs.items():
    if "round" not in rec:
        raise SystemExit(f"{tag}: no `round` at all, so the pre-fix check would have refused it too")
# ...and the swap really did happen: each file carries the OTHER rep's label and row count.
if recs["flight-bypass-warm-1"]["round"] != "flight-bypass-warm-2":
    raise SystemExit("rep 1's file does not carry rep 2's round label — the swap did not happen")
if recs["flight-bypass-warm-1"]["rows_total"] == recs["flight-bypass-warm-2"]["rows_total"]:
    raise SystemExit("the two reps carry the same rows_total, so the swap corrupts no figure and "
                     "this case would prove nothing about cycles/row")
print("pre-fix presence check SATISFIED by both swapped records; row counts differ")
PY
then
  pass "F1 NON-VACUITY: the PRE-FIX rule (`round` is PRESENT) is SATISFIED by both swapped records, and their row counts differ — so the swap was accepted and did corrupt cycles/row"
else
  fail "F1: the swapped records must satisfy the pre-fix presence rule, else the refusal above proves nothing was closed"
fi

# And the reporter REFUSES an unpairable set rather than silently falling back to
# medians alone — which is the comparison §3b forbids on its own.
d="$TMP/unpairable"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_scan_rep "$d" warm 2 ok
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
make_flight_rep "$d" warm 2 ok "$GOOD_FLIGHT"
rm -f "$d/scan-warm-2.json"          # scan has rep 1 only; flight has 1 and 2
out=$(run_report_cfg "$d" "$TMP/corpus" 2 warm bypass 1); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "an unpairable rep set is REFUSED (never a silent fallback to median-only)"
else
  fail "an unpairable rep set must be refused (rc=$rc, out: $out)"
fi


# ==========================================================================
# The round LABELS may not CONTRADICT the recorded INSTANTS (integrity, not a claim)
# ==========================================================================
# The fixture is a FORGERY: round/position labels a rounds-outside loop would write, over
# timestamps an arms-outside loop produces. The reporter refuses it — because the labels and
# the clock cannot both describe the session, so no figure can be attributed to a round.
#
# WHAT THIS IS NOT (#3272 round 4): passing this check is NOT evidence that a session was
# interleaved, and the report makes no such claim. It is a statement about the FILES. The
# distinction matters because the earlier round DID license a claim off this check and got it
# wrong at one round, where there is nothing to compare.
#
# NON-VACUITY: pre-fix, `<tag>.round` carried `round`/`position`/`arms_in_round` and NO
# TIMESTAMP, and `collect_round_meta` forced `round == rep` — so an arms-outside loop keeping
# the identical rotation arithmetic emitted BYTE-IDENTICAL metadata, and the reporter printed
# "the reps were INTERLEAVED … this is OBSERVED, not asserted" over it.
d="$TMP/arm-major-forgery"; mkdir -p "$d"
for rep in 1 2 3; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
done
# The LABELS: rotating positions, exactly what a rounds-outside loop writes.
# The CLOCK: arms-outside — all three scan reps complete, THEN all three flight reps.
for rep in 1 2 3; do
  scan_pos=$(( rep % 2 == 1 ? 1 : 2 ))
  fl_pos=$(( rep % 2 == 1 ? 2 : 1 ))
  make_round "$d" "scan-warm-$rep"          "$rep" "$scan_pos" 2 "$(( 1000000000 + rep * 1000000 ))"
  make_round "$d" "flight-bypass-warm-$rep" "$rep" "$fl_pos"   2 "$(( 5000000000 + rep * 1000000 ))"
done
out=$(run_report_cfg "$d" "$TMP/corpus" 3 warm bypass 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "round LABELS CONTRADICT the recorded INSTANTS" <<<"$out"; then
  pass "OBSERVED: labels that contradict the recorded instants are REFUSED"
else
  fail "the label/instant contradiction must be refused (rc=$rc, out: $out)"
fi
if grep -q "cannot both describe this session" <<<"$out" \
  && grep -q "INTEGRITY refusal over the" <<<"$out"; then
  pass "the refusal states it is an INTEGRITY refusal over the artifact set, claiming no property"
else
  fail "the contradiction refusal must name itself an integrity refusal (out: $out)"
fi
# ...and it must NOT read as a verified-interleaving claim in either direction.
no_claim_probe "the contradiction REFUSAL transcript" "$out"
# NOTHING is written: an artifact set that contradicts itself cannot be reported.
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written for a session whose labels the instants refute"
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
# reported at all, and saying so is the honest outcome.
d="$TMP/no-instant"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
printf 'round=1\nposition=1\narms_in_round=2\n' > "$d/scan-warm-1.round"
expect_reject "round metadata with NO monotonic_ns is REFUSED (a pre-fix session cannot carry the claim)" \
  "carries no 'monotonic_ns'" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "when the rep completed" <<<"$out" \
  && grep -q "no ORDERING property is derived from these" <<<"$out"; then
  pass "the refusal names the field's content AND disclaims deriving an ordering from it"
else
  fail "the monotonic_ns refusal must state what it is and what it is not (out: $out)"
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
# THE ACCEPT DIRECTION: a non-contradictory 3-round session is accepted, its instants are
# recorded VERBATIM, and the integrity SCOPE is a plain count (2 consecutive pairs over 3
# rounds) rather than a verdict.
d="$TMP/round-major-ok"; mkdir -p "$d"
for rep in 1 2 3; do
  make_scan_rep "$d" warm "$rep" ok
  make_flight_rep "$d" warm "$rep" ok "$GOOD_FLIGHT"
done
out=$(run_report_cfg "$d" "$TMP/corpus" 3 warm bypass 1); rc=$?
if [ "$rc" -eq 0 ] \
  && python3 "$REPO_ROOT/scripts/tests/ws0_assert_no_verdict_fields.py" "$d/results.json" >/dev/null \
  && python3 - "$d/results.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
rec = doc["recorded_round_metadata"]["warm"]
integ = rec["integrity_checks"]
# 3 rounds x 2 arms = 6 reps examined, and 2 consecutive round pairs available.
assert integ["reps_examined"] == 6, integ
assert integ["round_pairs_compared"] == 2, integ
assert "NOT a verdict" in integ["scope_note"], integ
# The RECORDED instants are carried through per round, per arm — the raw timeline #3287/#3299
# would need, with no property derived from it here.
inst = rec["instants_by_round_recorded"]
assert set(inst) == {"1", "2", "3"}, inst
assert all(set(v) == {"bare_scan", "flight_do_get_bypass"} for v in inst.values()), inst
assert all(isinstance(x, int) and x > 0 for v in inst.values() for x in v.values()), inst
# Provenance is stated as UNVERIFIED (the verdict-field absence is the shared assert above).
assert "UNVERIFIED" in rec["source"], rec["source"]
PY
then
  pass "a 3-round session is ACCEPTED, its instants RECORDED, and the scope is a COUNT not a verdict"
else
  fail "the accept direction must record the raw instants without a verdict (rc=$rc, out: $out)"
fi
no_claim_probe "the accepted 3-round session" "$out"


# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its checks
# and passed them exits 0 and reports SUCCESS. That is the suite-level `0/0` shape this whole
# issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite) and
# far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts what
# actually RAN rather than what is written in the file.
MIN_CHECKS=45
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 round metadata: all $checks checks passed"
  exit 0
fi
echo "ws0 round metadata: $fails of $checks check(s) FAILED"
exit 1

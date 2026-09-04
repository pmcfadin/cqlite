#!/usr/bin/env bash
# Guards for scripts/perf/ws0_quiescence_evidence.py — the CLOSED verdict-evidence checker.
#
# WHY A DEDICATED SUITE. The inline predecessor of this checker was holed by review three rounds
# running (jobs 73, 75, 78) because it was patched pointwise and its properties were verified BY
# HAND and never pinned. Every case below perturbs exactly ONE aspect of a baseline the POSITIVE
# CONTROL proves acceptable, and asserts a diagnostic naming ITS OWN subject: a refusal from some
# other precondition is the vacuous-pass shape these guards exist to refuse.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PERF="$REPO_ROOT/perf"
# `-B` ON EVERY PYTHON INVOCATION, AND A CACHE SWEEP, BECAUSE A STALE .pyc FOOLED THIS SUITE.
# While RED-verifying the drift check I mutated CANONICAL_MAX_LOAD1 2.0 -> 7.5 and restored it.
# The suite kept reporting drift: `2.0` and `7.5` are the SAME BYTE LENGTH, and the restore landed
# in the same mtime SECOND, so CPython's (mtime, size) cache-validity test saw a valid .pyc and
# served the mutated constant from a source file that no longer contained it. A test that can be
# fooled by a stale cache reports on a module nobody is running.
export PYTHONDONTWRITEBYTECODE=1
find "$REPO_ROOT/perf" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true

checks=0
fails=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); fails=$((fails + 1)); echo "FAIL - $1"; }

# --------------------------------------------------------------------------------------------
# The canonical thresholds are DUPLICATED in the evidence module (to keep it dependency-free),
# so they are asserted against the writer's here. A silent divergence would let the reporter
# certify a bar the writer refuses, or red a verdict the writer accepts -- and a duplicated
# constant with no drift check is exactly the kind of pair that rots.
# --------------------------------------------------------------------------------------------
if out=$(python3 - "$PERF" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
import ws0_quiescence as w
import ws0_quiescence_evidence as e
# ALL THREE bounds, because job 80 F1 was a bound that existed in the writer and not here.
pairs = [("MAX_LOAD1", w.DEFAULT_MAX_LOAD1, e.CANONICAL_MAX_LOAD1),
         ("MAX_LOAD1_MOVEMENT", w.DEFAULT_MAX_LOAD1_MOVEMENT, e.CANONICAL_MAX_LOAD1_MOVEMENT),
         ("MAX_SAMPLE_GAP_S", w.MAX_SAMPLE_GAP_S, e.CANONICAL_COVERAGE_GAP_BOUND_S)]
# ...and the table must not have grown a bound the drift check does not cover.
if len(e.CANONICAL_BOUNDS) != len(pairs):
    sys.exit(f"DRIFT: CANONICAL_BOUNDS has {len(e.CANONICAL_BOUNDS)} entries but this check "
             f"covers {len(pairs)}. A bound with no drift check is the job-80 F1 defect again.")
bad = [f"{n}: writer={a!r} evidence={b!r}" for n, a, b in pairs if a != b]
if bad:
    sys.exit("DRIFT: " + "; ".join(bad))
print("aligned")
PY
); then
  pass "the evidence module's canonical thresholds MATCH ws0_quiescence's defaults ($out)"
else
  fail "canonical threshold drift between writer and evidence checker: $out"
fi

# --------------------------------------------------------------------------------------------
# ...and so are the CENSUS RULES, for the same reason one level over (#3551 defect 3). The
# verdict's `census_scope` states WHAT A ZERO CENSUS DOES NOT BOUND, and it names the rule set
# it IS bounded by. The evidence module mirrors that rule set to recompose the sentence, so a
# rule ADDED to the writer alone would leave the published sentence understating the scope while
# every field still type-checked.
#
# THE DRIFT CHECK COMPARES OUTPUT, NOT SOURCE TEXT, over several sample counts: the derivation
# takes the record's own sample count, so agreement at one count is not agreement. (And the
# stale-.pyc lesson above is why every python call in this file is cache-swept.)
# --------------------------------------------------------------------------------------------
if out=$(python3 - "$PERF" <<'DRIFT2'
import sys
sys.path.insert(0, sys.argv[1])
import ws0_quiescence as w
import ws0_quiescence_evidence as e
pairs = [("COMPETING_COMMS", tuple(w.COMPETING_COMMS), tuple(e.CANONICAL_COMPETING_COMMS)),
         ("COMPETING_CMDLINE", tuple(w.COMPETING_CMDLINE),
          tuple(e.CANONICAL_COMPETING_CMDLINE))]
bad = [f"{n}: writer={a!r} evidence={b!r}" for n, a, b in pairs if a != b]
if bad:
    sys.exit("DRIFT: " + "; ".join(bad))
for samples in (1, 4, 48, 999):
    writer = w.census_scope_note(samples)
    mirror = e._expected_census_scope(samples)
    if writer != mirror:
        sys.exit(f"DRIFT at samples={samples}: writer says {writer!r}, evidence expects"
                 f" {mirror!r}")
    # AFFIRMATIVE, not merely equal: the sentence must carry the count it describes and must say
    # the census is BOUNDED. Two identical-but-empty strings would satisfy equality alone.
    if str(samples) not in writer or "NOTHING ELSE" not in writer:
        sys.exit(f"the scope note at samples={samples} does not state its own scope: {writer!r}")
print("aligned")
DRIFT2
); then
  pass "the evidence module's census RULE SET and scope-note derivation match the writer ($out)"
else
  fail "census rule/scope-note drift between writer and evidence checker: $out"
fi

# --------------------------------------------------------------------------------------------
# The mutation matrix. The baseline is generated by the SHIPPED writer path so the suite cannot
# drift from the real record shape -- the failure mode that cost jobs 73 and 75.
# --------------------------------------------------------------------------------------------
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --------------------------------------------------------------------------------------------
# THE PROPERTY MATRIX IS TOTAL, DISJOINT, AND HAS NO STRAY CELLS.
#
# This is the pin the coordination ruling on job 80 asked for: "properties x fields enumerated is a
# finite matrix, and 'which cells did I fill?' has a complete answer the way 'which fields did I
# check?' did." Without this, the matrix is a comment -- a field added to FIELDS with no cell would
# silently get type-checking only, which IS the job-80 defect.
# --------------------------------------------------------------------------------------------
if out=$(python3 - "$PERF" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
import ws0_quiescence_evidence as e
p1, p2, to, fields = set(e.P1_CANONICAL), set(e.P2_DERIVATIONS), set(e.TYPE_ONLY), set(e.FIELDS)
problems = []
if fields - (p1 | p2 | to):
    problems.append(f"unclassified: {sorted(fields - (p1 | p2 | to))}")
if (p1 | p2 | to) - fields:
    problems.append(f"stray cells: {sorted((p1 | p2 | to) - fields)}")
for a, b, n in ((p1, p2, "P1&P2"), (p1, to, "P1&TYPE_ONLY"), (p2, to, "P2&TYPE_ONLY")):
    if a & b:
        problems.append(f"double-classified {n}: {sorted(a & b)}")
if problems:
    sys.exit("; ".join(problems))
print(f"total={len(fields)} P1={len(p1)} P2={len(p2)} TYPE_ONLY={len(to)}")
PY
); then
  pass "the property matrix is TOTAL, DISJOINT and stray-free ($out)"
else
  fail "property matrix defect: $out"
fi

# ...and the pin must FIRE, not merely be present (#3249: observed to fire, not present).
if out=$(python3 - "$PERF" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
import ws0_quiescence_evidence as e
e.FIELDS["window_census.__unclassified_probe"] = (lambda v: True, "anything")
try:
    e.assert_self_consistent({}, "probe")
except e.EvidenceError as exc:
    if "PROPERTY MATRIX INCOMPLETE" in str(exc) and "__unclassified_probe" in str(exc):
        print("fired, naming the field")
        raise SystemExit(0)
    sys.exit(f"raised, but not the matrix pin: {exc}")
sys.exit("the matrix pin did NOT fire on an unclassified field")
PY
); then
  pass "the matrix pin FIRES on an unclassified field ($out)"
else
  fail "matrix pin did not fire: $out"
fi

# --------------------------------------------------------------------------------------------
# THE BASELINE IS HERMETIC. IT USED TO DEPEND ON A QUIET BOX, AND THAT WAS THREE DEFECTS AT ONCE.
#
# The first version judged against the LIVE /data/ws0-3248/sampler/box-load.jsonl. Running this
# suite makes the box busy, so `judge` correctly REFUSED with
# QUIESCENCE_WINDOW_CONTAMINATED (10 of 14 in-window samples competing, load1 11.46) -- and then:
#
#   1. the SKIP branch exited 0 having run ONE check, BYPASSING the MIN_CHECKS floor that exists
#      to catch exactly that. A vacuous pass, in the suite written to prevent vacuous passes.
#   2. its message said "no live box-load timeseries on this host", which was FALSE -- the file
#      existed and was fresh. A misdiagnosis printed as fact.
#   3. a test suite should never need a quiet box in the first place.
#
# So the timeseries is now SYNTHETIC and written here: clean records at the sampler's own cadence,
# covering the judged window. The shipped writer path is still what produces the verdict -- that is
# the property worth keeping, since it is what stops the fixture drifting from the real record
# shape -- but it now runs deterministically. THERE IS NO SKIP PATH LEFT: a suite that cannot build
# its baseline FAILS, because a green with one check is worse than a red.
# --------------------------------------------------------------------------------------------
SYN_TS="$TMP/box-load.jsonl"
python3 - "$SYN_TS" <<'PY'
import datetime, json, sys
now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
# 10 s cadence over 5 minutes, every record CLEAN and carrying the full census (so the verdict
# composes census_breadth = FULL and the derivation assert has a definite expectation).
with open(sys.argv[1], "w") as fh:
    for i in range(30, -1, -1):
        ts = now - datetime.timedelta(seconds=10 * i)
        fh.write(json.dumps({
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "load1": 0.20, "load5": 0.18, "load15": 0.15, "runnable": "1/700",
            "competing_count": 0,
            "rustc": 0, "cargo": 0, "perf": 0, "gate": 0, "flight": 0, "loadgen": 0,
        }) + "\n")
PY

# Boundary samples must ALSO be clean, and the live box may not be -- so they are composed here
# too, in the shape `ws0_quiescence sample` writes.
mk_boundary() { # mk_boundary <path> <load1>
  python3 - "$1" "$2" <<'PY'
import json, sys
json.dump({"competing": [], "competing_count": 0,
           "load": {"load1": float(sys.argv[2]), "load5": 0.18, "load15": 0.15,
                    "runnable": "1/700"}},
          open(sys.argv[1], "w"))
PY
}
mk_boundary "$TMP/b.json" 0.11
mk_boundary "$TMP/a.json" 0.19

WS="$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=200)).strftime('%Y-%m-%dT%H:%M:%SZ'))")"
WE="$(python3 -c "import datetime;print(datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))")"
if ! judge_out=$(python3 "$PERF/ws0_quiescence.py" judge --before "$TMP/b.json" --after "$TMP/a.json" \
       --out "$TMP/v.json" --timeseries "$SYN_TS" \
       --window-start "$WS" --window-end "$WE" 2>&1); then
  fail "the shipped writer could not compose a baseline verdict from the SYNTHETIC timeseries"
  echo "       This is not a skip condition: the fixture is hermetic, so a refusal here is a real"
  echo "       defect in either the writer or this fixture. Writer said:"
  printf '%s\n' "$judge_out" | sed 's/^/       /' | head -6
  echo
  echo "ws0-quiescence-evidence guards: $fails of $checks check(s) FAILED"
  exit 1
fi

run_matrix() {
python3 - "$PERF" "$TMP/v.json" <<'PY'
import copy, json, sys
sys.path.insert(0, sys.argv[1])
from ws0_quiescence_evidence import (assert_self_consistent, EvidenceError,
                                     _expected_census_scope)
base = json.loads(open(sys.argv[2]).read())

# POSITIVE CONTROL FIRST: without it every refusal below could be firing on a broken baseline.
try:
    assert_self_consistent(copy.deepcopy(base), "baseline")
    print("PASS|the shipped writer's own verdict is ACCEPTED (positive control)")
except EvidenceError as exc:
    print(f"FAILCASE|positive control REFUSED, so the whole matrix is vacuous: {exc}")
    raise SystemExit(1)

CASES = [
    ("a dotted key forging a nested path is refused", "PATH SEPARATOR",
     lambda v: (v["before"].__setitem__("competing", ["cc1"]),
                v["before"].__setitem__("competing_count", 1),
                v.__setitem__("before.competing", []),
                v.__setitem__("before.competing_count", 0))),
    ("a dotted key is refused even when its value is harmless", "PATH SEPARATOR",
     lambda v: v.__setitem__("window_census.samples", 41)),
    ("thresholds.max_load1 LOOSER than canonical is refused", "LOOSER than the canonical",
     lambda v: v["thresholds"].__setitem__("max_load1", 999.0)),
    ("thresholds.max_load1_movement LOOSER than canonical is refused",
     "LOOSER than the canonical",
     lambda v: v["thresholds"].__setitem__("max_load1_movement", 9.0)),
    ("an UNDECLARED field is refused (the writer grew a field)", "does not know",
     lambda v: v.__setitem__("brand_new_field", 1)),
    ("a MISSING declared field is refused", "missing",
     lambda v: v.pop("thresholds")),
    # PERTURB EXACTLY ONE PROPERTY. An earlier version set only `before.competing`/`_count`,
    # leaving the top-level `competing_before` duplicate stale -- so the P2 derivation fired first
    # and the case passed for the wrong reason. A real verdict with a boundary competitor is
    # internally consistent; only the QUIESCENT conclusion is wrong.
    ("a competitor at the before boundary is refused", "before boundary census lists",
     lambda v: (v["before"].__setitem__("competing", ["cc1"]),
                v["before"].__setitem__("competing_count", 1),
                v.__setitem__("competing_before", 1))),
    ("a competitor at the after boundary is refused", "after boundary census lists",
     lambda v: (v["after"].__setitem__("competing", ["ld", "rustc"]),
                v["after"].__setitem__("competing_count", 2),
                v.__setitem__("competing_after", 2))),
    ("competing_count disagreeing with len(competing) is refused", "`competing_after` is 0, but its own inputs",
     lambda v: v["after"].__setitem__("competing_count", 3)),
    ("a top-level/nested duplicate that disagrees is refused", "must",
     lambda v: v.__setitem__("competing_before", 4)),
    ("nonzero in-window competing_samples is refused", "with a competing process",
     lambda v: v["window_census"].__setitem__("competing_samples", 7)),
    ("a zero-sample window is refused (unmeasured, not quiet)", "not an integer >= 1",
     lambda v: v["window_census"].__setitem__("samples", 0)),
    # The breadth string is COMPUTED from the baseline's own `samples`, not hardcoded: a literal
    # copied from the fixture is a drift pair, which is the defect class this suite is about. The
    # case perturbs exactly one property -- narrow > samples is impossible -- and keeps
    # census_breadth correctly DERIVED so the impossibility check is what fires.
    ("narrow_census_records exceeding samples is refused as impossible", "which is impossible",
     lambda v: (v["window_census"].__setitem__("narrow_census_records", 999),
                v["window_census"].__setitem__(
                    "census_breadth",
                    f"NARROW on 999 of {v['window_census']['samples']} record(s): those carry"
                    " rustc/cargo/gate only, so a short-lived cc1/ld/lld/mold between boundaries"
                    " would not appear. Stated rather than implied."))),
    ("a gap wider than the verdict's OWN bound is refused", "exceeds",
     lambda v: v["window_census"].__setitem__("coverage_largest_gap_s", 999.0)),
    ("census_breadth claiming FULL while narrow>0 is refused", "`window_census.census_breadth` is \x27FULL (all records)\x27, but its own inputs",
     lambda v: (v["window_census"].__setitem__("narrow_census_records", 3),
                v["window_census"].__setitem__("census_breadth", "FULL (all records)"))),
    ("census_breadth claiming NARROW while narrow==0 is refused", "`window_census.census_breadth` is \x27NARROW on 2 of 5\x27, but its own inputs",
     lambda v: (v["window_census"].__setitem__("narrow_census_records", 0),
                v["window_census"].__setitem__("census_breadth", "NARROW on 2 of 5"))),
    ("load1_mean outside its own min/max is refused", "impossible",
     lambda v: v["window_census"].__setitem__("load1_mean", 99.0)),
    ("a judged window running backwards is refused", "does not run forwards",
     lambda v: v["window_census"]["window"].__setitem__("end", "2020-01-01T00:00:00Z")),
    ("a recorded load1_movement disagreeing with the boundaries is refused", "`load1_movement` is 7.77, but its own inputs",
     lambda v: v.__setitem__("load1_movement", 7.77)),
    ("load1_before above its own max_load1 is refused", "above its own",
     lambda v: (v.__setitem__("load1_before", 99.0),
                v["before"]["load"].__setitem__("load1", 99.0),
                v.__setitem__("load1_movement", abs(v["load1_after"] - 99.0)))),
    # --- job 80: the FAMILY sweep, not the three reported instances ---
    ("coverage_gap_bound_s LOOSER than canonical is refused (the third bound, missed in job 78)",
     "LOOSER than the canonical",
     lambda v: (v["window_census"].__setitem__("coverage_gap_bound_s", 9999.0),
                v["window_census"].__setitem__("coverage_largest_gap_s", 9998.0))),
    ("census_breadth as ARBITRARY text beside a nonzero narrow count is refused",
     "`window_census.census_breadth` is 'everything is fine', but its own inputs",
     lambda v: (v["window_census"].__setitem__("narrow_census_records", 2),
                v["window_census"].__setitem__("census_breadth", "everything is fine"))),
    ("census_breadth with the WRONG sample count in its own text is refused",
     "but its own inputs (f(narrow_census_records, samples))",
     lambda v: (v["window_census"].__setitem__("narrow_census_records", 2),
                v["window_census"].__setitem__(
                    "census_breadth",
                    "NARROW on 2 of 999 record(s): those carry rustc/cargo/gate only, so a"
                    " short-lived cc1/ld/lld/mold between boundaries would not appear. Stated"
                    " rather than implied."))),
    ("load1_after_note disagreeing with load1_after_is_bounded is refused (swept, not reported)",
     "`load1_after_note` is 'bounded: the caller asserted",
     lambda v: v.__setitem__("load1_after_note", "bounded: the caller asserted this sample was"
                                                " taken after settling")),
    # #3551 defect 3. `census_scope` is the sentence that stops a zero census being READ as a
    # quiet box, so it is the field a reader is most tempted to soften. Same family as the
    # census_breadth cases above, and the same reason they exist: inspected rather than derived,
    # ANY nonempty text passes -- including a reassurance, which is the exact failure the field
    # exists to prevent.
    ("census_scope replaced by a reassurance is refused",
     "`window_census.census_scope` is 'the box was quiet'",
     lambda v: v["window_census"].__setitem__("census_scope", "the box was quiet")),
    # ...and the count inside it must be the record's OWN. Composed through the mirror rather
    # than pasted, because a literal copied from the fixture is the drift pair this suite is
    # about; the case perturbs exactly one property, the sample count in the text.
    ("census_scope stating a sample count other than its own is refused",
     "but its own inputs (f(samples))",
     lambda v: v["window_census"].__setitem__(
         "census_scope", _expected_census_scope(v["window_census"]["samples"] + 1))),
    ("a non-dict verdict is refused", "not a JSON object", lambda v: None),
]
for label, expect, mut in CASES:
    v = copy.deepcopy(base)
    if mut(v) is None and label.startswith("a non-dict"):
        v = ["not", "a", "dict"]
    try:
        assert_self_consistent(v, "verdict")
        print(f"FAILCASE|{label}: ACCEPTED a verdict it must refuse")
    except EvidenceError as exc:
        if expect in str(exc):
            print(f"PASS|{label}")
        else:
            print(f"FAILCASE|{label}: refused for the WRONG cause (wanted {expect!r}): {exc}")
PY
}

while IFS='|' read -r verdict label; do
  [ -n "$verdict" ] || continue
  case "$verdict" in
    PASS) pass "$label" ;;
    FAILCASE) fail "$label" ;;
  esac
done < <(run_matrix)

MIN_CHECKS=31
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure."
  exit 1
fi
echo
if [ "$fails" -eq 0 ]; then
  echo "ws0-quiescence-evidence guards: all $checks checks passed"
  exit 0
fi
echo "ws0-quiescence-evidence guards: $fails of $checks check(s) FAILED"
exit 1

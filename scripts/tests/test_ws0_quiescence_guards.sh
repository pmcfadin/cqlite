#!/usr/bin/env bash
# Self-test for the box-quiescence gate (scripts/perf/ws0_quiescence.py, #3248).
#
# WHY IT EXISTS. The rig's README states that it "produces no reusable absolute" after an
# untouched warm bare scan drifted ~10% in an hour. What it does not model is that the box is
# SHARED between delivery lanes: `load1` reached 108 on 16 vCPUs during this issue's own prep,
# from a peer lane's gate. #3299 measured the mechanism at an identical S=1/N=1 point —
# co-scheduled 2.470 GHz vs quiescent 3.268-3.291 GHz, a 25% FREQUENCY reduction with only 2
# logical CPUs pinned — so load need not be high to be fatal, which is why the gate keys on a
# competing-process CENSUS and not on load alone.
#
# The bar is #3272's: not "the guard exists" but "the guard has been OBSERVED to fire". Every
# case below feeds the input the gate must reject and asserts the exit code AND the cause token.
#
# HERMETIC. The gate's `judge` subcommand consumes two JSON samples and touches nothing else, so
# every fixture here is synthetic. `sample` reads /proc, so it is exercised once, read-only, and
# only for the shape of what it returns.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
Q="$REPO_ROOT/scripts/perf/ws0_quiescence.py"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT INT TERM HUP

fails=0
checks=0
pass() { checks=$((checks + 1)); printf '  ok   %s\n' "$1"; }
fail() { checks=$((checks + 1)); printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }

mksample() { # <path> <load1> <competing-json>
  printf '{"load":{"load1":%s,"load5":1.0,"load15":1.0,"runnable":"1/100"},"competing_count":%s,"competing":%s}\n' \
    "$2" "$(python3 -c "import json,sys;print(len(json.loads(sys.argv[1])))" "$3")" "$3" > "$1"
}

expect_refusal() {
  local name="$1" cause="$2"; shift 2
  local out rc
  out="$(python3 "$Q" judge --after-settled "$@" 2>&1)"; rc=$?
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

echo "== ACCEPT direction, asserted AFFIRMATIVELY =="
mksample "$TMP/ok-before.json" 0.80 '[]'
mksample "$TMP/ok-after.json"  1.10 '[]'
out="$(python3 "$Q" judge --after-settled --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" --out "$TMP/ok.json" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ]; then
  fail "quiet rep accepted — exited $rc: $(head -2 <<<"$out" | tr '\n' ' ')"
else
  ok=1
  grep -q 'QUIESCENT' <<<"$out" || { ok=0; fail "accept: no QUIESCENT verdict"; }
  python3 - "$TMP/ok.json" <<'JSONCHK' 2>"$TMP/err" || { ok=0; fail "accept: JSON assertions failed: $(cat "$TMP/err")"; }
import json, sys
r = json.load(open(sys.argv[1]))
assert r["verdict"] == "QUIESCENT", r["verdict"]
assert abs(r["load1_movement"] - 0.30) < 1e-9, r["load1_movement"]
# THE THRESHOLDS MUST BE RECORDED IN THE ARTIFACT, not merely applied. A reader has to be able
# to judge the bar rather than trust it, which is the whole point of making them arguments.
assert r["thresholds"]["max_load1"] == 2.0, r["thresholds"]
assert r["thresholds"]["max_load1_movement"] == 0.5, r["thresholds"]
# Both boundary samples must be retained, so the rep carries its own evidence.
assert r["before"]["load"]["load1"] == 0.80 and r["after"]["load"]["load1"] == 1.10
JSONCHK
  [ "$ok" -eq 1 ] && pass "quiet rep accepted; verdict, movement, BOTH boundary samples and the thresholds all recorded"
fi

echo "== REFUSAL direction =="

# Presence of a competitor is refused on PRESENCE, not on load: #3299's control shows 25%
# frequency loss with only 2 logical CPUs pinned, so a low load1 does not make it safe.
mksample "$TMP/comp-before.json" 0.10 '[{"pid":"42","comm":"rustc","why":"comm=rustc","cmdline":"rustc --crate-name x"}]'
mksample "$TMP/comp-after.json"  0.10 '[]'
expect_refusal "competitor at the BEFORE boundary (load1 only 0.10)" QUIESCENCE_COMPETING_PROCESSES \
  --before "$TMP/comp-before.json" --after "$TMP/comp-after.json"

# ...and symmetrically at the AFTER boundary: a competitor that started mid-rep invalidates it
# just as surely, and this direction is the one a before-only check would miss.
mksample "$TMP/comp2-before.json" 0.10 '[]'
mksample "$TMP/comp2-after.json"  0.10 '[{"pid":"43","comm":"cargo","why":"comm=cargo","cmdline":"cargo build"}]'
expect_refusal "competitor appearing at the AFTER boundary" QUIESCENCE_COMPETING_PROCESSES \
  --before "$TMP/comp2-before.json" --after "$TMP/comp2-after.json"

mksample "$TMP/hi-before.json" 9.00 '[]'
mksample "$TMP/hi-after.json"  9.00 '[]'
expect_refusal "load1 above the level bound" QUIESCENCE_LOAD_TOO_HIGH \
  --before "$TMP/hi-before.json" --after "$TMP/hi-after.json"

# A rep whose load MOVED is invalid rather than slow, and this is the case a level-only bound
# lets through: both endpoints are under the level bound here.
mksample "$TMP/mv-before.json" 0.20 '[]'
mksample "$TMP/mv-after.json"  1.90 '[]'
expect_refusal "load1 moved 1.70 with BOTH endpoints under the level bound" QUIESCENCE_LOAD_MOVED \
  --before "$TMP/mv-before.json" --after "$TMP/mv-after.json"

printf 'not json at all\n' > "$TMP/bad.json"
expect_refusal "unreadable sample" QUIESCENCE_SAMPLE_UNREADABLE \
  --before "$TMP/bad.json" --after "$TMP/ok-after.json"

printf '{"load":{"load1":0.1}}\n' > "$TMP/incomplete.json"
expect_refusal "sample missing its census" QUIESCENCE_SAMPLE_INCOMPLETE \
  --before "$TMP/incomplete.json" --after "$TMP/ok-after.json"

# THE ESCAPE HATCH MUST NOT EXIST: the knobs may only tighten.
expect_refusal "level bound loosened" QUIESCENCE_THRESHOLD_LOOSENED \
  --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" --max-load1 99
expect_refusal "movement bound loosened" QUIESCENCE_THRESHOLD_LOOSENED \
  --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" --max-load1-movement 99

# ...and tightening must actually bite, or the knob is decorative.
out="$(python3 "$Q" judge --after-settled --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" --max-load1-movement 0.1 2>&1)"; rc=$?
if [ "$rc" -eq 0 ]; then
  fail "tightened movement bound 0.1 accepted a 0.30 movement — the knob does not tighten"
elif grep -q 'REFUSED: QUIESCENCE_LOAD_MOVED' <<<"$out"; then
  pass "tightened movement bound refuses a movement it should"
else
  fail "tightened movement bound failed with an unexpected cause: $(head -2 <<<"$out" | tr '\n' ' ')"
fi

echo "== the in-window timeseries is the BINDING check (redesign, #3248) =="
# WHY THIS SECTION EXISTS. The first version of this gate bounded load1 at BOTH boundaries
# and then REFUSED this issue's own AC0 pass: load1 read 3.05 against a 2.0 bound with a
# competing census of ZERO at both boundaries and zero competitors across all 48 in-window
# sampler lines. The box was clean; load1 is a 1-MINUTE DECAYING AVERAGE, so a sample taken
# straight after a nine-minute CPU-bound run reads the run's OWN residue. Bounding it there
# measures how hard the rig just worked, not whether the box was quiet — it would refuse
# every honest run of a CPU-bound rig while passing a short one on a contended box.
# The threshold was NOT loosened. The bound moved out of a place it cannot be valid, and the
# binding check became STRONGER: attributable process identity across the whole window.

# NOTHING MAY BIND: neither a window nor a settled after-sample is refused, because "nothing
# established the window was clean" must not read as "the window was clean".
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'REFUSED: QUIESCENCE_WINDOW_UNVERIFIED' <<<"$out"; then
  pass "neither timeseries nor --after-settled is REFUSED (no unbound path to a pass)"
else
  fail "a run with no binding in-window check must be refused (rc=$rc, out: $(head -1 <<<"$out"))"
fi

# A CONTAMINATED window is refused even when both boundaries are clean — the case two
# instants structurally cannot see.
{ printf '{"ts":"2026-01-01T00:00:00Z","load1":0.5,"rustc":0,"cargo":0,"gate":0}\n'
  printf '{"ts":"2026-01-01T00:00:10Z","load1":9.9,"rustc":7,"cargo":1,"gate":0}\n'
  printf '{"ts":"2026-01-01T00:00:20Z","load1":0.5,"rustc":0,"cargo":0,"gate":0}\n'; } > "$TMP/ts-dirty.jsonl"
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" \
        --timeseries "$TMP/ts-dirty.jsonl" --window-start 2026-01-01T00:00:00Z \
        --window-end 2026-01-01T00:00:30Z 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'REFUSED: QUIESCENCE_WINDOW_CONTAMINATED' <<<"$out"; then
  pass "a competitor appearing only MID-WINDOW is caught (invisible to boundary samples)"
else
  fail "a mid-window competitor must be caught (rc=$rc, out: $(head -1 <<<"$out"))"
fi

# An UNCOVERED window is refused: no samples in range reads exactly like a clean window.
{ printf '{"ts":"2025-01-01T00:00:00Z","load1":0.5,"rustc":0,"cargo":0,"gate":0}\n'; } > "$TMP/ts-far.jsonl"
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" \
        --timeseries "$TMP/ts-far.jsonl" --window-start 2026-01-01T00:00:00Z \
        --window-end 2026-01-01T00:00:30Z 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'REFUSED: QUIESCENCE_TIMESERIES_EMPTY' <<<"$out"; then
  pass "an UNCOVERED window is refused (absent measurement is not a pass)"
else
  fail "an uncovered window must be refused (rc=$rc, out: $(head -1 <<<"$out"))"
fi

# A MALFORMED sampler line is an error, not a line to skip: skipping it would let a
# truncated timeseries certify a window it never covered.
{ printf '{"ts":"2026-01-01T00:00:00Z","load1":0.5,"rustc":0,"cargo":0,"gate":0}\n'
  printf 'this is not json\n'; } > "$TMP/ts-bad.jsonl"
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" \
        --timeseries "$TMP/ts-bad.jsonl" --window-start 2026-01-01T00:00:00Z \
        --window-end 2026-01-01T00:00:30Z 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'REFUSED: QUIESCENCE_TIMESERIES_MALFORMED' <<<"$out"; then
  pass "a malformed sampler line is an ERROR, not a skipped line"
else
  fail "a malformed timeseries line must be refused (rc=$rc, out: $(head -1 <<<"$out"))"
fi

# --timeseries without a window is a usage refusal: an unbounded window would judge samples
# from a different run entirely.
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" \
        --timeseries "$TMP/ts-dirty.jsonl" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'QUIESCENCE_WINDOW_UNBOUNDED' <<<"$out"; then
  pass "--timeseries without a window is refused (would judge another run's samples)"
else
  fail "--timeseries needs an explicit window (rc=$rc, out: $(head -1 <<<"$out"))"
fi

# A CLEAN window ACCEPTS, and the after-sample's load1 is recorded UNBOUNDED with its reason.
{ printf '{"ts":"2026-01-01T00:00:00Z","load1":0.5,"rustc":0,"cargo":0,"gate":0}\n'
  printf '{"ts":"2026-01-01T00:00:10Z","load1":2.9,"rustc":0,"cargo":0,"gate":0}\n'
  printf '{"ts":"2026-01-01T00:00:20Z","load1":3.1,"rustc":0,"cargo":0,"gate":0}\n'; } > "$TMP/ts-clean.jsonl"
mksample "$TMP/hot-after.json" 3.05 '[]'
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/hot-after.json" \
        --timeseries "$TMP/ts-clean.jsonl" --window-start 2026-01-01T00:00:00Z \
        --window-end 2026-01-01T00:00:30Z --out "$TMP/win.json" 2>&1)"; rc=$?
if [ "$rc" -eq 0 ] && grep -q 'RECORDED, NOT BOUNDED' <<<"$out"; then
  pass "a clean window ACCEPTS a hot-but-self-inflicted after-load, labelled not-bounded"
else
  fail "a clean window with a self-inflicted after-load must accept (rc=$rc, out: $(head -2 <<<"$out"|tr '\n' ' '))"
fi
# ...and that acceptance must NOT have skipped the before-bound: a genuinely busy ENTRY is
# still refused even with a clean window, or the redesign would have removed the guard
# rather than relocated it.
mksample "$TMP/busy-before.json" 9.0 '[]'
out="$(python3 "$Q" judge --before "$TMP/busy-before.json" --after "$TMP/ok-after.json" \
        --timeseries "$TMP/ts-clean.jsonl" --window-start 2026-01-01T00:00:00Z \
        --window-end 2026-01-01T00:00:30Z 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'QUIESCENCE_LOAD_TOO_HIGH' <<<"$out"; then
  pass "the BEFORE load bound survives the redesign (relocated, not removed)"
else
  fail "a busy entry state must still be refused (rc=$rc, out: $(head -1 <<<"$out"))"
fi

echo "== the census reads /proc directly, not via pgrep -f =="
# `pgrep -f <pattern>` matches the census command's OWN cmdline and inflates the count it is
# measuring; this lane hit exactly that (a field read `0\n0`). And `pgrep -x` is not the
# alternative: the kernel `comm` field caps at 15 chars, so a longer name can never match —
# pkill itself warns it "will result in zero matches". Asserted structurally.
if grep -nE 'pgrep|pkill' "$Q" | grep -vE '^\s*[0-9]+:\s*#' | grep -qE 'pgrep|pkill'; then
  # allow the words inside comments/docstrings, refuse an actual invocation
  if python3 - "$Q" <<'ASTCHK'
import ast, sys
src = open(sys.argv[1], encoding="utf-8").read()
tree = ast.parse(src)
bad = []
for node in ast.walk(tree):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        continue  # string literals incl. docstrings are prose, not invocations
    if isinstance(node, ast.Call):
        seg = ast.get_source_segment(src, node) or ""
        if "pgrep" in seg or "pkill" in seg:
            bad.append(f"line {node.lineno}")
sys.exit(1 if bad else 0)
ASTCHK
  then
    pass "no pgrep/pkill CALL in the census (the words appear only in prose)"
  else
    fail "the census invokes pgrep/pkill — a -f pattern self-matches and inflates its own count"
  fi
else
  pass "no pgrep/pkill anywhere in the census"
fi

echo "== sample reads /proc and returns the documented shape =="
python3 "$Q" sample --out "$TMP/live.json" >/dev/null 2>&1
if python3 - "$TMP/live.json" <<'LIVECHK'
import json, sys
r = json.load(open(sys.argv[1]))
assert set(("load", "competing_count", "competing")) <= set(r), sorted(r)
assert set(("load1", "load5", "load15", "runnable")) <= set(r["load"]), sorted(r["load"])
assert isinstance(r["load"]["load1"], float)
assert r["competing_count"] == len(r["competing"])
LIVECHK
then
  pass "live sample carries load1/5/15, runnable and a census whose count matches its list"
else
  fail "live sample shape is wrong"
fi

# A floor, so a block that silently never ran cannot green a 0/0 suite (the gate reads only the
# exit code). DERIVED BY RUNNING, not counted from source — and the first value here was 13,
# guessed from source, which the floor promptly refused against the real 12. That is the floor
# doing its job on its own author, and it is why the rule is "derive by running": a source count
# is an estimate, and an estimate in a floor is either decorative (too low) or a false failure
# (too high).
MIN_CHECKS=19
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "test_ws0_quiescence_guards: PASS (all $checks checks)"
  exit 0
fi
echo "test_ws0_quiescence_guards: FAIL ($fails of $checks)"
exit 1

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
  out="$(python3 "$Q" judge "$@" 2>&1)"; rc=$?
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
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" --out "$TMP/ok.json" 2>&1)"; rc=$?
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
out="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" --max-load1-movement 0.1 2>&1)"; rc=$?
if [ "$rc" -eq 0 ]; then
  fail "tightened movement bound 0.1 accepted a 0.30 movement — the knob does not tighten"
elif grep -q 'REFUSED: QUIESCENCE_LOAD_MOVED' <<<"$out"; then
  pass "tightened movement bound refuses a movement it should"
else
  fail "tightened movement bound failed with an unexpected cause: $(head -2 <<<"$out" | tr '\n' ' ')"
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
MIN_CHECKS=12
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

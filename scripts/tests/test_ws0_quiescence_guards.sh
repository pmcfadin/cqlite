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

echo "== the sampler and the judge COMPOSE (#3551 defect 1) =="
# WHY THIS IS THE MOST IMPORTANT CASE IN THE FILE. The committed `sample` schema is NOT the
# schema `judge --timeseries` requires, so the two halves of this ONE gate did not compose at
# all: `sample()` emits {load:{load1..}, competing_count, competing} and the judge wants a
# parseable `ts`, the census fields rustc/cargo/gate, AND a FLAT `load1` -- THREE layers, of
# which #3552 recorded one, saying that supplying `ts` "advances the judge to its coverage
# check, which is sound". Measured: it advances to the CENSUS-FIELD check and refuses again.
# A rig following the committed instructions could not produce an acceptable timeseries.
#
# HERMETIC, AND IT HAS TO BE. Judging a timeseries sampled from the REAL /proc on a shared
# fleet box would be a coin flip -- a peer lane's rustc reds it -- so the sampler is pointed at
# a SYNTHETIC procfs. The live-/proc leg is exercised separately below, where the assertion is
# about the RECORD's schema rather than about the verdict, because only one of those two things
# is deterministic on a shared box.

# mkproc <root> — a QUIET synthetic procfs: loadavg, per-CPU stat, no processes.
mkproc() {
  local r="$1"
  mkdir -p "$r"
  printf '0.20 0.18 0.15 1/700 9999\n' > "$r/loadavg"
  { printf 'cpu  10 0 10 1000 0 0 0 0 0 0\n'
    printf 'cpu0 10 0 10 1000 0 0 0 0 0 0\n'
    printf 'cpu1 20 0 20 2000 0 0 0 0 0 0\n'
    printf 'intr 0 0 0\n'; } > "$r/stat"
}

# mkproc_pid <root> <pid> <comm> <argv...> — argv written as REAL NUL-SEPARATED ELEMENTS.
# THE NULs ARE THE POINT. A joined-string fixture would be matched by the pre-fix substring
# code too, so it could not tell the fix from the defect: the whole distinction between
# EXECUTING a script and MENTIONING it lives in where the NULs fall.
mkproc_pid() {
  local r="$1" pid="$2" comm="$3"; shift 3
  mkdir -p "$r/$pid"
  printf '%s\n' "$comm" > "$r/$pid/comm"
  printf '%s\0' "$@" > "$r/$pid/cmdline"
  printf '%s (%s) S 1 %s 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n' "$pid" "$comm" "$pid" \
    > "$r/$pid/stat"
}

PROC_QUIET="$TMP/proc-quiet"
mkproc "$PROC_QUIET"
TS_LIVE="$TMP/composed.jsonl"
loop_out="$(python3 "$Q" sample-loop --out "$TS_LIVE" --cadence 0.05 --samples 3 \
              --proc-root "$PROC_QUIET" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ]; then
  fail "sample-loop failed on a synthetic quiet procfs (rc=$rc): $(head -2 <<<"$loop_out" | tr '\n' ' ')"
else
  pass "sample-loop wrote a bounded timeseries ($(wc -l < "$TS_LIVE") line(s))"
fi

# The composition itself: the judge must ACCEPT what the sampler wrote, with no hand editing.
mk_boundary_json() { # <path> <load1>
  printf '{"competing":[],"competing_count":0,"load":{"load1":%s,"load5":0.18,"load15":0.15,"runnable":"1/700"}}\n' \
    "$2" > "$1"
}
mk_boundary_json "$TMP/comp-b.json" 0.11
mk_boundary_json "$TMP/comp-a.json" 0.19
# The window is derived from the timeseries' OWN first/last stamps +- one cadence, so this case
# cannot pass by accident on a window that happens to contain nothing. A derivation that FAILS
# is its own named check: without it, a sampler emitting no `ts` reds the composition case with
# `QUIESCENCE_WINDOW_UNBOUNDED` (an empty --window-start), which names the harness rather than
# the defect -- and layer 1 IS the defect.
# NO command substitution around this heredoc: `$( ... <<'X' )` leaves the body outside the
# substitution and bash warns "unterminated here-document" while limping on. Redirected instead.
if python3 - "$TS_LIVE" > "$TMP/win.txt" 2> "$TMP/win.err" <<'PYWIN'; then
import datetime, json, sys
stamps = [json.loads(l)["ts"] for l in open(sys.argv[1]) if l.strip()]
fmt = "%Y-%m-%dT%H:%M:%SZ"
lo = datetime.datetime.strptime(min(stamps), fmt) - datetime.timedelta(seconds=10)
hi = datetime.datetime.strptime(max(stamps), fmt) + datetime.timedelta(seconds=10)
print(lo.strftime(fmt), hi.strftime(fmt))
PYWIN
  read -r CWS CWE < "$TMP/win.txt"
  pass "a window is derivable from the sampler's OWN stamps (layer 1: every record has a ts)"
else
  fail "the sampler's records carry no usable ts — layer 1 of the composition defect: $(head -1 "$TMP/win.err")"
  # A fixed, VALID window so the judge below refuses on the SCHEMA and reports that cause,
  # instead of refusing on an empty window and reporting the harness.
  CWS=2026-01-01T00:00:00Z; CWE=2026-01-01T00:00:20Z
fi
out="$(python3 "$Q" judge --before "$TMP/comp-b.json" --after "$TMP/comp-a.json" \
        --timeseries "$TS_LIVE" --window-start "$CWS" --window-end "$CWE" \
        --out "$TMP/composed-verdict.json" 2>&1)"; rc=$?
if [ "$rc" -eq 0 ] && grep -q 'QUIESCENT' <<<"$out"; then
  pass "END TO END: judge --timeseries ACCEPTS what sample-loop wrote, unedited"
else
  fail "the sampler and the judge do NOT compose (rc=$rc): $(head -3 <<<"$out" | tr '\n' ' ')"
fi
# ...and the record carries every field the judge requires, ASSERTED FIELD BY FIELD, so a
# regression names which layer broke instead of just failing to compose.
if python3 - "$TS_LIVE" "$TMP/composed-verdict.json" <<'PYREC'
import datetime, json, sys
rec = json.loads(open(sys.argv[1]).readline())
datetime.datetime.fromisoformat(rec["ts"].replace("Z", "+00:00"))   # layer 1
for field in ("rustc", "cargo", "gate"):                            # layer 2
    assert isinstance(rec[field], int) and not isinstance(rec[field], bool), (field, rec.get(field))
    assert rec[field] >= 0, (field, rec[field])
assert isinstance(rec["load1"], float), rec.get("load1")            # layer 3 (FLAT, not nested)
assert "load" not in rec, "the in-window record must be FLAT, not the boundary shape"
assert isinstance(rec["competing_count"], int) and rec["competing_count"] == len(rec["competing"])
assert rec["load5"] == 0.18 and rec["load15"] == 0.15 and "/" in rec["runnable"]
# The verdict must record the FULL census breadth, which is only true when the sampler emits
# `competing_count` on every record -- the #3248 finding-5 property, end to end.
v = json.load(open(sys.argv[2]))
assert v["window_census"]["narrow_census_records"] == 0, v["window_census"]
assert v["window_census"]["census_breadth"].startswith("FULL"), v["window_census"]
PYREC
then
  pass "the record satisfies ALL THREE schema layers (ts, census fields, FLAT load1) and reads FULL breadth"
else
  fail "the composed record is missing a field the judge requires"
fi

echo "== each schema layer refuses INDEPENDENTLY, by its own diagnostic =="
# One property per arm, and the ASSERTION IS THE DIAGNOSTIC: all three layers share the exit
# code, so an exit-code-only test cannot tell which layer fired -- which is how #3552 came to
# record one layer of three as the whole defect.
layer_case() { # <name> <cause> <needle-in-detail> <record-json>
  local name="$1" cause="$2" needle="$3" rec="$4"
  printf '%s\n' "$rec" > "$TMP/layer.jsonl"
  local o r
  o="$(python3 "$Q" judge --before "$TMP/ok-before.json" --after "$TMP/ok-after.json" \
       --timeseries "$TMP/layer.jsonl" --window-start 2026-01-01T00:00:00Z \
       --window-end 2026-01-01T00:00:20Z 2>&1)"; r=$?
  if [ "$r" -eq 0 ]; then
    fail "$name — ACCEPTED a record the judge must refuse"
  elif ! grep -q "REFUSED: $cause" <<<"$o"; then
    fail "$name — wrong cause (wanted $cause): $(head -1 <<<"$o")"
  elif ! grep -qF "$needle" <<<"$o"; then
    fail "$name — cause $cause but the diagnostic does not name its own subject ($needle): $(head -1 <<<"$o")"
  else
    pass "$name ($cause, diagnostic names $needle)"
  fi
}
layer_case "layer 1: no ts (what the committed sampler emits)" \
  QUIESCENCE_TIMESERIES_MALFORMED "no usable ts field" \
  '{"load":{"load1":0.5,"load5":0.4,"load15":0.3,"runnable":"1/700"},"competing_count":0,"competing":[]}'
layer_case "layer 2: ts present, NO census field (#3552 called this 'sound')" \
  QUIESCENCE_TIMESERIES_SCHEMA "carries no 'rustc' field" \
  '{"ts":"2026-01-01T00:00:10Z","load1":0.5,"competing_count":0}'
layer_case "layer 3: ts + census present, no FLAT load1" \
  QUIESCENCE_TIMESERIES_SCHEMA 'carries no `load1`' \
  '{"ts":"2026-01-01T00:00:10Z","rustc":0,"cargo":0,"gate":0,"load":{"load1":0.5}}'

echo "== the census matches an EXECUTION, not a MENTION (#3551 defect 2) =="
# MEASURED FALSE REFUSAL: `COMPETING_CMDLINE` was tested with `if needle in cmdline` against the
# whole joined cmdline, so agent tool-call shells (`/bin/bash -c source
# /data/auth/claude/shell-snapshots/snapshot-....sh ...`) that merely NAME agent-gate.sh were
# counted, inflating the census to 15 on a box running no gate. The file's own comment two lines
# above documents this family for `cargo` and says it "caused a FALSE REFUSAL of a quiet box".
#
# AND THE FIX IS NOT THE ANCESTOR WALK #3552 PROPOSED: `census()` already walks the ppid chain,
# and the offending shells belong to OTHER sessions -- a setsid-detached sampler's chain is
# init, so every peer lane's shell is a legitimate non-ancestor.
census_of() { # <root> — the census as JSON, self_pid pinned to a pid absent from the fixture
  python3 - "$REPO_ROOT/scripts/perf" "$1" <<'PYCEN'
import json, sys
sys.path.insert(0, sys.argv[1])
import ws0_quiescence as q
print(json.dumps(q.census(self_pid=999999, proc_root=sys.argv[2])))
PYCEN
}

PROC_EXEC="$TMP/proc-exec"
mkproc "$PROC_EXEC"
mkproc_pid "$PROC_EXEC" 4242 bash /bin/bash /data/lanes/lane-9999/scripts/agent-gate.sh --lite
if out="$(census_of "$PROC_EXEC")" && python3 - "$out" <<'PYEXEC'
import json, sys
found = json.loads(sys.argv[1])
assert len(found) == 1, found
e = found[0]
assert e["pid"] == "4242" and e["why"] == "argv=agent-gate.sh", e
# THE RECORD MUST NAME THE ELEMENT THAT MATCHED. The pre-fix record kept `cmdline[:160]` while
# matching the FULL cmdline, so every contaminated record this lane produced carried the verdict
# `cmdline~agent-gate.sh` with NO occurrence of the needle in its own text -- the false positive
# was undiagnosable from the artifact.
assert "/data/lanes/lane-9999/scripts/agent-gate.sh" in e["evidence"], e["evidence"]
PYEXEC
then
  pass "a shell EXECUTING agent-gate.sh is counted, and the record NAMES the matched argv element"
else
  fail "an executing gate must be counted with its matched element recorded: $out"
fi

# THE RED ARM, DIFFERING IN EXACTLY ONE PROPERTY: same comm, same needle text, same process --
# only WHERE THE NULs FALL changes. Here the name sits INSIDE a `-c` script-text element.
PROC_MENTION="$TMP/proc-mention"
mkproc "$PROC_MENTION"
mkproc_pid "$PROC_MENTION" 4243 bash /bin/bash -c \
  'source /data/auth/claude/shell-snapshots/snapshot-abc.sh && grep -n agent-gate.sh notes'
if out="$(census_of "$PROC_MENTION")" && python3 - "$out" <<'PYMENTION'
import json, sys
found = json.loads(sys.argv[1])
assert found == [], found
PYMENTION
then
  pass "a shell merely MENTIONING agent-gate.sh inside a -c script text is NOT counted"
else
  fail "a MENTION must not be counted (this is the measured false refusal): $out"
fi

# ...and an OPTION VALUE cannot spoof it either. This arm exists because basename equality alone
# does NOT close it: os.path.basename('--flag=/path/agent-gate.sh') IS 'agent-gate.sh', so the
# first version of this fix counted it. Found by writing the case.
PROC_OPT="$TMP/proc-opt"
mkproc "$PROC_OPT"
mkproc_pid "$PROC_OPT" 4244 bash /bin/bash /usr/bin/lint --script=/data/lanes/l/scripts/agent-gate.sh
if out="$(census_of "$PROC_OPT")" && python3 - "$out" <<'PYOPT'
import json, sys
assert json.loads(sys.argv[1]) == [], json.loads(sys.argv[1])
PYOPT
then
  pass "an option VALUE naming agent-gate.sh is NOT counted (an option executes nothing)"
else
  fail "an option value must not be counted: $out"
fi

# THE SWALLOW DIRECTION, which the removed `if "ws0_quiescence" in cmdline: continue` caused
# (#3469 family 5): a GENUINE competitor whose cmdline happens to mention this tool's name was
# skipped before the `comm` check ever ran, so `cargo` -- explicitly in COMPETING_COMMS -- went
# uncounted and a contaminated window could be certified quiet.
PROC_SWALLOW="$TMP/proc-swallow"
mkproc "$PROC_SWALLOW"
mkproc_pid "$PROC_SWALLOW" 4245 cargo cargo test --test ws0_quiescence_smoke
if out="$(census_of "$PROC_SWALLOW")" && python3 - "$out" <<'PYSWALLOW'
import json, sys
found = json.loads(sys.argv[1])
assert len(found) == 1 and found[0]["why"] == "comm=cargo", found
PYSWALLOW
then
  pass "a comm=cargo competitor mentioning ws0_quiescence IS counted (the swallow is gone)"
else
  fail "the self-exclusion swallow must be gone: $out"
fi

echo "== the per-CPU record is CONTEXT and MUST NOT move the verdict (#3551 defect 3) =="
# A zero census is NOT a quiet box: MEASURED, 91 consecutive samples read competing_count=0
# while load1 reached 6.39 with 9 runnable and the pinned CPUs measured a median 8% / max 86%
# busy. The per-CPU snapshot makes that visible. It must stay DIAGNOSTIC -- a field that
# quietly became a gate would be a threshold nobody chose -- so the same timeseries is judged
# twice, differing in exactly one property: the `percpu` values.
busy_variant() { # <src.jsonl> <dst.jsonl>
  python3 - "$1" "$2" <<'PYBUSY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
with open(dst, "w") as out:
    for line in open(src):
        if not line.strip():
            continue
        rec = json.loads(line)
        assert rec["percpu"], "the fixture must HAVE a per-CPU record, else this case is vacuous"
        # Same CPUs, same totals; every jiffy moved from idle to busy. A pegged box.
        rec["percpu"] = {cpu: {"total": v["total"], "idle": 0} for cpu, v in rec["percpu"].items()}
        out.write(json.dumps(rec, sort_keys=True) + "\n")
PYBUSY
}
busy_variant "$TS_LIVE" "$TMP/composed-busy.jsonl"
out="$(python3 "$Q" judge --before "$TMP/comp-b.json" --after "$TMP/comp-a.json" \
        --timeseries "$TMP/composed-busy.jsonl" --window-start "$CWS" --window-end "$CWE" \
        --out "$TMP/composed-busy-verdict.json" 2>&1)"; rc=$?
if [ "$rc" -eq 0 ] && python3 - "$TMP/composed-verdict.json" "$TMP/composed-busy-verdict.json" <<'PYSAME'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
# The window census (samples, breadth, scope, load1 summary) must be IDENTICAL, and the verdict
# with it, apart from the timeseries PATH the two runs read.
for v in (a, b):
    v["window_census"].pop("timeseries")
assert a == b, "the per-CPU record changed the verdict; it is a gate, not context"
# ...and no per-CPU DATA reaches the verdict either. Asked over KEYS, not over the serialized
# text: `census_scope` legitimately NAMES `percpu` in the sentence telling a reader to go and
# read it, so a substring scan would red on the disclaimer -- which would push the disclaimer
# toward being vague, i.e. the guard would degrade the text it exists to protect. (Exactly the
# reasoning ws0_assert_no_verdict_fields.py records for the word "interleaving".)
def keys(node):
    if isinstance(node, dict):
        for k, v in node.items():
            yield k
            yield from keys(v)
    elif isinstance(node, list):
        for v in node:
            yield from keys(v)
assert "percpu" not in set(keys(a)), "a `percpu` KEY reached the verdict; it is context, not evidence"
PYSAME
then
  pass "a fully BUSY per-CPU record leaves the verdict byte-identical (context, not a gate)"
else
  fail "the per-CPU record must not affect the verdict (rc=$rc): $(head -2 <<<"$out" | tr '\n' ' ')"
fi
# ...and the verdict must SAY what its zero census does not bound, or the residual is invisible
# to the only reader who can act on it.
if python3 - "$TMP/composed-verdict.json" <<'PYSCOPE'
import json, sys
w = json.load(open(sys.argv[1]))["window_census"]
scope = w["census_scope"]
assert "does NOT bound total foreign load" in scope, scope
assert "0 RECOGNISED" in scope, scope          # never a bare zero where the scan is partial
assert "CONTEXT, NOT GATES" in scope, scope
assert str(w["samples"]) in scope, scope       # bound to the record it describes
PYSCOPE
then
  pass "the verdict DECLARES the scope of its zero census (foreign load, 0 RECOGNISED, context-not-gate)"
else
  fail "the verdict must declare what a zero census does not bound"
fi

echo "== the sampler REFUSES to write where the artifact cannot survive =="
# Both reasons were learned by hitting them: a file appended every tick trips the gate's
# tree-integrity check MID-RUN (#2926), and a worktree is DELETED at finalize. There is no
# default --out (inventing one would guess a machine layout) and no override flag (it could only
# buy back these two failures).
WT="$TMP/fake-worktree"
mkdir -p "$WT/sub"
printf 'gitdir: /somewhere/.git/worktrees/x\n' > "$WT/.git"   # a WORKTREE's .git is a FILE
out="$(python3 "$Q" sample-loop --out "$WT/sub/box-load.jsonl" --samples 1 --cadence 0.05 \
        --proc-root "$PROC_QUIET" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'QUIESCENCE_SAMPLER_OUT_IN_WORKTREE' <<<"$out"; then
  pass "an --out inside a git worktree is REFUSED (#2926 tree-integrity; deleted at finalize)"
else
  fail "an --out inside a worktree must be refused (rc=$rc): $(head -1 <<<"$out")"
fi
# THE CONTROL: the identical invocation one directory OUT is accepted, so the refusal above is
# about worktree membership and not about the path being unwritable.
if python3 "$Q" sample-loop --out "$TMP/outside/box-load.jsonl" --samples 1 --cadence 0.05 \
     --proc-root "$PROC_QUIET" >/dev/null 2>&1; then
  fail "control: an --out whose PARENT DIRECTORY does not exist should not silently succeed"
else
  pass "control: a missing parent directory is a failure, not a silent success"
fi
mkdir -p "$TMP/outside"
if out="$(python3 "$Q" sample-loop --out "$TMP/outside/box-load.jsonl" --samples 1 \
            --cadence 0.05 --proc-root "$PROC_QUIET" 2>&1)"; then
  pass "control: the same invocation OUTSIDE any worktree is accepted"
else
  fail "control: an out-of-worktree --out must be accepted: $(head -1 <<<"$out")"
fi
# A non-positive cadence samples nothing (or spins), so the window it produces covers nothing.
out="$(python3 "$Q" sample-loop --out "$TMP/outside/c.jsonl" --cadence 0 --samples 1 \
        --proc-root "$PROC_QUIET" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'QUIESCENCE_SAMPLER_CADENCE_INVALID' <<<"$out"; then
  pass "a non-positive --cadence is REFUSED"
else
  fail "--cadence 0 must be refused (rc=$rc): $(head -1 <<<"$out")"
fi
# A negative --samples is a usage error, not "run forever": 0 is the documented unbounded value,
# so a negative one is a caller who meant something else.
out="$(python3 "$Q" sample-loop --out "$TMP/outside/n.jsonl" --samples -1 --cadence 0.05 \
        --proc-root "$PROC_QUIET" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q 'QUIESCENCE_SAMPLER_SAMPLES_INVALID' <<<"$out"; then
  pass "a negative --samples is REFUSED (0 is the documented unbounded value)"
else
  fail "--samples -1 must be refused (rc=$rc): $(head -1 <<<"$out")"
fi
# ...and the DEFAULT cadence is the module's own constant, not a second literal.
if python3 - "$Q" <<'PYCAD'
import ast, sys
src = open(sys.argv[1], encoding="utf-8").read()
assert 'default=SAMPLER_CADENCE_S' in src.replace(" ", ""), \
    "sample-loop's --cadence default must BE SAMPLER_CADENCE_S, not a copy of its value"
ast.parse(src)
PYCAD
then
  pass "--cadence defaults to SAMPLER_CADENCE_S itself (no drift pair)"
else
  fail "--cadence default is a duplicated literal"
fi

echo "== the LIVE /proc leg: schema, not verdict (a shared box cannot promise quiet) =="
# Deliberately asserts only what is deterministic on a fleet box. A peer lane's rustc makes the
# VERDICT a coin flip, but the RECORD's schema is ours -- and if the live census refuses, it may
# only be for CONTAMINATION, never for a schema layer. That distinction is the whole subject of
# defect 1, so it is asserted rather than assumed.
LIVE_TS="$TMP/live-loop.jsonl"
if python3 "$Q" sample-loop --out "$LIVE_TS" --cadence 0.05 --samples 2 >/dev/null 2>&1 \
   && python3 - "$LIVE_TS" <<'PYLIVE'
import datetime, json, sys
lines = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
assert len(lines) == 2, len(lines)
for rec in lines:
    datetime.datetime.fromisoformat(rec["ts"].replace("Z", "+00:00"))
    assert isinstance(rec["load1"], float) and rec["load1"] >= 0
    for field in ("rustc", "cargo", "gate", "competing_count"):
        assert isinstance(rec[field], int) and rec[field] >= 0, (field, rec[field])
    assert rec["census_proc_root"] == "/proc"
    # The per-CPU record must be POPULATED from a real /proc/stat: an empty dict would make the
    # defect-3 diagnostic vacuous exactly where it is meant to be read.
    assert rec["percpu"] and all(v["total"] > 0 for v in rec["percpu"].values()), rec["percpu"]
    assert all(v["idle"] <= v["total"] for v in rec["percpu"].values()), rec["percpu"]
PYLIVE
then
  pass "a LIVE /proc sample-loop record carries every judge-required field and a real per-CPU snapshot"
else
  fail "the live sampler leg did not produce a judge-shaped record"
fi

# A floor, so a block that silently never ran cannot green a 0/0 suite (the gate reads only the
# exit code). DERIVED BY RUNNING, not counted from source — and the first value here was 13,
# guessed from source, which the floor promptly refused against the real 12. That is the floor
# doing its job on its own author, and it is why the rule is "derive by running": a source count
# is an estimate, and an estimate in a floor is either decorative (too low) or a false failure
# (too high).
MIN_CHECKS=39
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

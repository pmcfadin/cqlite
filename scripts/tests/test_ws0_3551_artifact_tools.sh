#!/usr/bin/env bash
# test_ws0_3551_artifact_tools.sh — THE #3551 MEASUREMENT-ANALYSIS TOOLS' OWN GUARDS.
#
# # Subject
#
# `docs/reports/ws0-3551-artifacts/clean-pairs.py` and `.../window-census.py`. This repo
# reviews `docs/reports/*-artifacts/` harnesses as CODE (#3229), and these two are not
# incidental: their stdout IS the published result of issue #3551 (`docs/reports/
# ws0-3551-report.md` §5). They had NO tests, and that is how this got in:
#
#   * `clean()` accepted a session as CLEAN on the strength of ONE zero-census sample anywhere
#     in its window. A non-empty sample set is not COVERAGE — a sampler that died a minute into
#     a nine-minute session certified eight unobserved minutes as quiescent — so a mostly
#     UNOBSERVED session could enter the published medians. Fixed by importing the committed
#     judge's `MAX_SAMPLE_GAP_S` and going three-valued (`489d03872`).
#
# The empty case had been implemented and the UNDERCOVERED case had not, which is the harder
# half of the same rule: a positive verdict requires an AFFIRMATIVE MEASUREMENT.
#
# # The bar (this repo's standing rules for a guard suite)
#
#   * every refusal case is paired with the ACCEPT direction of the same check — case 1 is the
#     positive control for the whole coverage rule, and without it this suite proves nothing;
#   * every RED arm differs from its control in EXACTLY ONE property, stated at the case;
#   * every RED arm is matched on the tool's OWN diagnostic — the verdict TOKEN and the
#     measured GAP, the excluded pair's own line, the named refusal — never on a bare non-zero
#     exit or a changed count, which an unrelated breakage produces identically;
#   * the coverage BOUND is DERIVED from the committed judge at run time, never restated here.
#     Restating it would make this suite agree with the tools while both disagreed with the
#     gate, which is the exact defect above one level up.
#
# # Hermetic, and hermetic affirmatively
#
# No cargo, perf, taskset, sudo, root, corpus bytes, server or network, and NOTHING read from
# `/data/ws0-3551` (those are one lane's live measurement outputs and exist for nobody else).
# Every input is a synthetic session directory and a synthetic sampler JSONL written under
# `$TMPDIR` by the two fixture writers below. The only committed files read are the two tools
# and `scripts/perf/ws0_quiescence.py`, whose constant is the subject of case 0b.
#
# # DECLARED RESIDUAL, because a suite that omits coverage silently is indistinguishable from
# # one that has it
#
# The two tools do NOT agree on the TEXT for a window with no covering sample at all.
# `window-census.py` distinguishes `NOT MEASURED (no sample covers this window)` from
# `**UNDERCOVERED** (…)`, which is right — "the scan could not be performed" and "the scan was
# too sparse" are different operator facts. `clean-pairs.py` collapses both onto the state
# `undercovered` (`clean()`: `if gap is None or gap > MAX_SAMPLE_GAP_S`). Case 4 therefore
# asserts the DISTINCTION where it exists and, on `clean-pairs.py`, asserts only the property
# that matters for the published figures — the session is NOT clean and its pair is NOT counted
# — deliberately NOT pinning that tool's label, so making the two agree is not a change this
# suite reds on. The divergence is reported rather than fixed here: regenerating
# `clean-pairs.md` needs the live measurement outputs and is a change to a published report.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS="$REPO_ROOT/docs/reports/ws0-3551-artifacts"
PAIRS_TOOL="$ARTIFACTS/clean-pairs.py"
CENSUS_TOOL="$ARTIFACTS/window-census.py"
JUDGE="$REPO_ROOT/scripts/perf/ws0_quiescence.py"

fails=0
# `checks` counts what actually RAN (incremented by pass/fail themselves), so the floor at the
# end can see a block that silently never executed — this file has no `set -e`.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

for f in "$PAIRS_TOOL" "$CENSUS_TOOL" "$JUDGE"; do
  [ -f "$f" ] || { echo "FAIL - missing $f"; exit 1; }
done
# python3 is a HARD REQUIREMENT: both subjects ARE python. A skip here would record the gate
# component SUCCESS with none of these checks having run.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. Both tools under test ARE python, and a skip here"
  echo "       would report this component SUCCESS with 0 checks run."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# ===========================================================================
# PART 0 — THE DERIVATION, AND THE HARNESS'S OWN ORACLES
# ===========================================================================
# THE BOUND IS DERIVED FROM THE COMMITTED JUDGE, NEVER RESTATED. A literal `30` here would
# make this suite and the tools agree while both drifted from the gate the tools claim to
# match, which is this issue's own defect one level up. A FAILED derivation is a FAIL that
# NAMES the derivation, never a fallback to a default.
_derive() {
  python3 - "$REPO_ROOT" "$1" <<'PY' 2>/dev/null
import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(sys.argv[1]) / "scripts" / "perf"))
import ws0_quiescence
v = getattr(ws0_quiescence, sys.argv[2])
print(int(v))
PY
}
BOUND="$(_derive MAX_SAMPLE_GAP_S)"
CADENCE="$(_derive SAMPLER_CADENCE_S)"
case "$BOUND" in ''|*[!0-9]*) BOUND=""; esac
case "$CADENCE" in ''|*[!0-9]*) CADENCE=""; esac
if [ -n "$BOUND" ] && [ -n "$CADENCE" ] && [ "$BOUND" -gt 0 ] && [ "$CADENCE" -gt 0 ]; then
  pass "0a. derivation: MAX_SAMPLE_GAP_S=${BOUND}s and SAMPLER_CADENCE_S=${CADENCE}s read from the committed judge (not restated here)"
else
  echo "FAIL - 0a. derivation: could not read MAX_SAMPLE_GAP_S / SAMPLER_CADENCE_S from $JUDGE"
  echo "       (got BOUND='$BOUND' CADENCE='$CADENCE'). Every coverage case below is derived"
  echo "       from these, so a fallback would silently re-curate the bound."
  exit 1
fi
OVER=$((BOUND + 1))
GAPSPAN=$((BOUND + CADENCE))   # an unobserved stretch strictly greater than the bound

# 0b. STRUCTURAL: the rule is IMPORTED, not restated. A tool that assigned its own
# MAX_SAMPLE_GAP_S would pass every behavioural case below at whatever value it chose.
for t in "$PAIRS_TOOL" "$CENSUS_TOOL"; do
  b="$(basename "$t")"
  if grep -q 'from ws0_quiescence import' "$t"; then
    pass "0b. $b imports the coverage rule from the committed judge"
  else
    fail "0b. $b must import MAX_SAMPLE_GAP_S from ws0_quiescence, not restate it"
  fi
  if grep -qE '^[[:space:]]*MAX_SAMPLE_GAP_S[[:space:]]*=' "$t"; then
    fail "0b. $b ASSIGNS its own MAX_SAMPLE_GAP_S — a second copy of the gate's bound"
  else
    pass "0b. $b defines no private copy of MAX_SAMPLE_GAP_S"
  fi
done

# ---------------------------------------------------------------------------
# The fixture writers, written once and driven by argument
# ---------------------------------------------------------------------------
# `iso.py` — the ONE clock in this suite. Every window bound and every sample instant is
# BASE + an offset in seconds, so a case states its fixture as offsets and the reader can see
# the gap it plants without doing date arithmetic.
cat > "$TMP/iso.py" <<'PY'
import datetime
import sys

BASE = datetime.datetime(2026, 9, 3, 2, 0, 0, tzinfo=datetime.timezone.utc)
print((BASE + datetime.timedelta(seconds=int(sys.argv[1]))).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY

# `mkts.py` — a synthetic sampler timeseries in the schema `ws0_quiescence.sample()` emits.
# Argument 2 is the sample offsets; argument 3 the subset of those offsets whose census is
# CONTAMINATED (`-` for none); argument 4 turns the `percpu` block on or off, which is what
# separates a per-CPU reading from a NOT MEASURED one.
#
# The `percpu` counters are CUMULATIVE, as /proc/stat's are: sample i carries total = i*1000
# and idle = i*500, so EVERY consecutive pair differences to exactly 50.0% busy and the median
# of a window has one expected answer.
cat > "$TMP/mkts.py" <<'PY'
import datetime
import json
import pathlib
import sys

BASE = datetime.datetime(2026, 9, 3, 2, 0, 0, tzinfo=datetime.timezone.utc)
PINS = ("2", "3", "10", "11")

out = pathlib.Path(sys.argv[1])
offsets = [int(v) for v in sys.argv[2].split(",") if v != ""]
dirty = set() if sys.argv[3] == "-" else {int(v) for v in sys.argv[3].split(",") if v != ""}
percpu_on = sys.argv[4] == "on"

lines = []
for i, off in enumerate(offsets):
    ts = (BASE + datetime.timedelta(seconds=off)).strftime("%Y-%m-%dT%H:%M:%SZ")
    comp = ([{"pid": 4242, "comm": "cc1plus", "evidence": "comm rule"}] if off in dirty else [])
    rec = {
        "ts": ts,
        "load1": 0.4, "load5": 0.4, "load15": 0.4, "runnable": 1,
        "competing_count": len(comp),
        "competing": comp,
        "rustc": 0, "cargo": 0, "gate": 0,
        "census_proc_root": "/synthetic",
    }
    if percpu_on:
        rec["percpu"] = {c: {"total": i * 1000, "idle": i * 500} for c in PINS}
    lines.append(json.dumps(rec))
out.write_text("\n".join(lines) + ("\n" if lines else ""))
PY

# `mkses.py` — ONE session directory: the `results.json` fields `clean-pairs.py` reads and the
# `abc-window.json` both tools read. Values are supplied per session so every numeric assertion
# below has exactly one expected answer.
cat > "$TMP/mkses.py" <<'PY'
import json
import pathlib
import sys

(root, rnd, arm, pos, started, ended,
 scan_rps, scan_cpr, flight_rps, flight_cpr, ipc) = sys.argv[1:12]

d = pathlib.Path(root) / f"r{rnd}-{arm}"
d.mkdir(parents=True, exist_ok=True)
(d / "results.json").write_text(json.dumps({
    "measurements": [
        {"temperature": "warm", "arm": "bare_scan",
         "rows_per_sec": {"median": float(scan_rps)},
         "cycles_per_row": {"median": float(scan_cpr)},
         "ipc": {"median": 1.45}},
        {"temperature": "warm", "arm": "flight_bypass",
         "rows_per_sec": {"median": float(flight_rps)},
         "cycles_per_row": {"median": float(flight_cpr)},
         "ipc": {"median": float(ipc)}},
        # A COLD leg, because the real artifact has one and the tools must ignore it: a tool
        # reading the cold medians would produce a different number on this fixture.
        {"temperature": "cold", "arm": "bare_scan",
         "rows_per_sec": {"median": 1.0},
         "cycles_per_row": {"median": 1.0},
         "ipc": {"median": 1.0}},
    ],
}))
(d / "abc-window.json").write_text(json.dumps({
    "arm": arm, "round": int(rnd), "position_in_round": int(pos),
    "started": started, "ended": ended, "exit": 0,
}))
PY

# `mdcell.py FILE HEADER0 KEY COLUMN` — ONE cell out of a markdown table, located by the
# table's first HEADER cell, the row's first cell and the COLUMN's HEADER. Never by field
# position: a positional read keeps passing after a column moves and is then asserting about a
# different quantity, which is a defect this rig has already had (#3551 F3, one file over).
cat > "$TMP/mdcell.py" <<'PY'
import pathlib
import sys

path, header0, key, column = sys.argv[1:5]
header = None
for line in pathlib.Path(path).read_text().splitlines():
    if not line.startswith("|"):
        header = None
        continue
    cells = [c.strip() for c in line.strip("|").split("|")]
    if header is None:
        if cells and cells[0] == header0:
            header = cells
        continue
    if set(line) <= set("|- :"):
        continue
    if cells[0].strip("`") == key:
        if column not in header:
            sys.stderr.write(f"no column {column!r} in {header!r}\n")
            raise SystemExit(3)
        print(cells[header.index(column)])
        raise SystemExit(0)
sys.stderr.write(f"no row {key!r} in a table headed {header0!r} in {path}\n")
raise SystemExit(3)
PY

isot() { python3 "$TMP/iso.py" "$1"; }
mkts() { python3 "$TMP/mkts.py" "$@"; }
mkses() { python3 "$TMP/mkses.py" "$@"; }
mdcell() { python3 "$TMP/mdcell.py" "$@"; }

# A sample offset list at the sampler's own cadence, inclusive of both ends.
cadence_offsets() {
  python3 - "$1" "$2" "$CADENCE" <<'PY'
import sys
lo, hi, step = (int(v) for v in sys.argv[1:4])
print(",".join(str(v) for v in range(lo, hi + 1, step)))
PY
}

# THE TWO RUNNERS write to a FILE and set `rc` as a global. Not to a `$( )` capture: a
# command substitution runs in a SUBSHELL, so an `rc=$?` inside it is discarded and every
# `[ "$rc" -eq 0 ]` downstream reads a stale value — measured, `rc: unbound variable` on the
# first use. The file also lets `mdcell` read the table without a second run.
RUNOUT="$TMP/run.out"

# census_run <root> <timeseries> — window-census.py into $RUNOUT; sets `rc`.
census_run() {
  python3 "$CENSUS_TOOL" --root "$1" --timeseries "$2" >"$RUNOUT" 2>&1
  rc=$?
}
# pairs_run <timeseries> <label=dir>… — clean-pairs.py into $RUNOUT; sets `rc`.
pairs_run() {
  local ts="$1"; shift
  local args=() spec
  for spec in "$@"; do args+=(--set "$spec"); done
  python3 "$PAIRS_TOOL" --timeseries "$ts" "${args[@]}" >"$RUNOUT" 2>&1
  rc=$?
}
# The last few lines of a run, for a failure message.
tailout() { tail -3 "$RUNOUT"; }

# 0c. THE HARNESS'S OWN ORACLE. Every "the verdict is not clean" assertion below could be
# satisfied by a fixture writer that cannot produce a clean verdict at all, and every numeric
# assertion by a table nobody can read. This case proves both instruments work before any RED
# arm relies on their silence.
W0="$TMP/oracle"; TS0="$TMP/oracle.jsonl"
S0="$(isot 0)"; E0="$(isot 120)"
mkts "$TS0" "$(cadence_offsets 0 120)" - on
mkses "$W0" 1 A 1 "$S0" "$E0" 400000 20000 250000 25000 1.40
census_run "$W0" "$TS0"
if [ "$rc" -eq 0 ] && grep -q 'clean (census 0, max gap' "$RUNOUT"; then
  pass "0c. oracle: the fixture writers CAN produce a clean verdict (so a later 'not clean' is a measurement, not an artifact of the harness)"
else
  fail "0c. oracle: a fully covered zero-census fixture must read clean (rc=$rc, out: $(tailout))"
fi
# ...and the cell reader can read a cell out of that same table. A `mdcell` that could never
# find a row would make every numeric assertion below vacuous.
if [ "$(mdcell "$RUNOUT" session r1-A arm)" = "A" ]; then
  pass "0c. oracle: mdcell locates a cell by header and row key (so a numeric assertion below is a reading)"
else
  fail "0c. oracle: mdcell must read the arm cell of row r1-A (got: $(mdcell "$RUNOUT" session r1-A arm 2>&1))"
fi

# ===========================================================================
# PART 1 — THE COVERAGE RULE (the defect this suite exists for)
# ===========================================================================
# ONE session set is shared by cases 1-4, and ONLY THE SAMPLER TIMESERIES CHANGES between
# them. That is what makes each a single-property RED arm: identical windows, identical
# figures, identical pairing — the sole difference is which instants were OBSERVED.
#
#   r1-A  [0s, 120s]   the baseline arm
#   r1-B  [120s, 240s] the treatment arm
#
# A's numbers are (scan 400000 rows/s, 20000 cycles/row) and B's are the same scan with a
# +10% rows/s and -10% cycles/row flight leg, so the counted pair below has exactly one
# expected value in every column.
cov_sessions() {
  local root="$1"
  mkses "$root" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 250000 25000 1.40
  mkses "$root" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20000 275000 22500 1.50
}
COV="$TMP/cov"
cov_sessions "$COV"

# --- Case 1: THE POSITIVE CONTROL ------------------------------------------------------
# Fully covered at the sampler's own cadence, `competing_count: 0` throughout. Without this
# case every RED arm below could be produced by a tool that refuses everything.
TS_OK="$TMP/ts-covered.jsonl"
mkts "$TS_OK" "$(cadence_offsets 0 240)" - on
census_run "$COV" "$TS_OK"
if [ "$rc" -eq 0 ] \
  && grep -q 'clean (census 0, max gap' <<<"$(mdcell "$RUNOUT" session r1-A verdict)" \
  && grep -q 'clean (census 0, max gap' <<<"$(mdcell "$RUNOUT" session r1-B verdict)"; then
  pass "1. covered @${CADENCE}s with a zero census: BOTH sessions read clean"
else
  fail "1. a fully covered zero-census window must read clean (rc=$rc, out: $(tailout))"
fi
if grep -qF 'All 2 sessions clean' "$RUNOUT"; then
  pass "1. ...and the census footer says all 2 sessions are clean"
else
  fail "1. the footer must report all sessions clean (out: $(tailout))"
fi
pairs_run "$TS_OK" "set1=$COV"
if [ "$rc" -eq 0 ] && [ "$(mdcell "$RUNOUT" arm B 'clean pairs')" = "1" ]; then
  pass "1. ...and the (A,B) pair is COUNTED — 1 clean pair for arm B"
else
  fail "1. the covered pair must be counted (rc=$rc, out: $(tailout))"
fi
if grep -qF '**2 clean**' "$RUNOUT"; then
  pass "1. ...and clean-pairs counts 2 clean sessions"
else
  fail "1. clean-pairs must count 2 clean sessions (out: $(head -1 "$RUNOUT"))"
fi

# --- Case 2: RED — an INTERIOR unobserved stretch longer than the bound ----------------
# ONE property differs from case 1: three consecutive samples inside r1-B's window are
# missing, so the largest unobserved stretch there is ${GAPSPAN}s > ${BOUND}s.
TS_INT="$TMP/ts-interior-gap.jsonl"
python3 - "$TS_INT" "$CADENCE" "$GAPSPAN" "$TMP/mkts.py" <<'PY'
import subprocess
import sys
out, cadence, gapspan, writer = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
# r1-B's window is [120, 240]. Drop the samples strictly inside (150, 190) so the observed
# instants straddle a `gapspan`-second hole with samples on BOTH sides — an INTERIOR gap, not
# a boundary one.
keep = [o for o in range(0, 241, cadence) if not (150 < o < 150 + gapspan)]
subprocess.run([sys.executable, writer, out, ",".join(str(o) for o in keep), "-", "on"],
               check=True)
PY
census_run "$COV" "$TS_INT"
verdict="$(mdcell "$RUNOUT" session r1-B verdict)"
if [ "$rc" -eq 0 ] && grep -qF '**UNDERCOVERED**' <<<"$verdict" \
  && grep -qF "${GAPSPAN}s > ${BOUND}s" <<<"$verdict"; then
  pass "2. interior unobserved stretch: r1-B is NAMED '$verdict'"
else
  fail "2. an interior gap > ${BOUND}s must read UNDERCOVERED and REPORT the gap (rc=$rc, verdict: '$verdict')"
fi
if [ "$(mdcell "$RUNOUT" session r1-A verdict)" != "$verdict" ]; then
  pass "2. ...and r1-A, whose coverage did not change, still reads clean — the finding is the gap, not the fixture"
else
  fail "2. r1-A must be unaffected (got: $(mdcell "$RUNOUT" session r1-A verdict))"
fi
pairs_run "$TS_INT" "set1=$COV"
if [ "$rc" -eq 0 ] && grep -qF '**NO CLEAN PAIRS.**' "$RUNOUT" \
  && grep -qF "1 UNDERCOVERED" "$RUNOUT"; then
  pass "2. ...and clean-pairs counts it UNDERCOVERED and forms NO pair (the undercovered session cannot enter a median)"
else
  fail "2. an undercovered session must not be counted or paired (rc=$rc, out: $(head -1 "$RUNOUT"))"
fi

# --- Case 3: RED — the BOUNDARY halves, which is where this rule is usually got wrong ---
# Interior-gap detection is the easy half: the observed instants straddle the hole, so a
# naive consecutive-differences scan finds it. The BOUNDARY halves are invisible to that scan
# — the window's own start and end are not sample instants — and a window whose sampler
# started late or died early is exactly the failure the bound exists for. Both tools must
# include `first - window_start` and `window_end - last` in the gap set, so both are driven.
#
# ONE property differs from case 1 in each arm: which end of r1-B's window is unobserved.
for boundary in start end; do
  TSB="$TMP/ts-$boundary-gap.jsonl"
  python3 - "$TSB" "$CADENCE" "$GAPSPAN" "$TMP/mkts.py" "$boundary" <<'PY'
import subprocess
import sys
out, cadence, gapspan, writer, which = (
    sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5])
# r1-B's window is [120, 240] and r1-A's is [0, 120].
if which == "start":
    # No sample from B's window start until `gapspan` seconds into it. A keeps its own
    # coverage (its last sample is 110, a `cadence` gap from its end), so the only session
    # whose coverage changed is B.
    keep = [o for o in range(0, 241, cadence) if o < 120 or o >= 120 + gapspan]
else:
    # Samples stop `gapspan` seconds before B's window ends.
    keep = [o for o in range(0, 241, cadence) if o <= 240 - gapspan]
subprocess.run([sys.executable, writer, out, ",".join(str(o) for o in keep), "-", "on"],
               check=True)
PY
  census_run "$COV" "$TSB"
  verdict="$(mdcell "$RUNOUT" session r1-B verdict)"
  if [ "$rc" -eq 0 ] && grep -qF '**UNDERCOVERED**' <<<"$verdict" \
    && grep -qF "${GAPSPAN}s > ${BOUND}s" <<<"$verdict"; then
    pass "3-$boundary. a ${GAPSPAN}s unobserved stretch at the window's $boundary is NAMED: '$verdict'"
  else
    fail "3-$boundary. an unobserved stretch at the window's $boundary must read UNDERCOVERED and report the gap (rc=$rc, verdict: '$verdict')"
  fi
  if grep -q '^clean (census 0' <<<"$(mdcell "$RUNOUT" session r1-A verdict)"; then
    pass "3-$boundary. ...and r1-A still reads clean, so the finding is the planted $boundary gap"
  else
    fail "3-$boundary. r1-A must stay clean (got: $(mdcell "$RUNOUT" session r1-A verdict))"
  fi
  pairs_run "$TSB" "set1=$COV"
  if [ "$rc" -eq 0 ] && grep -qF '**NO CLEAN PAIRS.**' "$RUNOUT" \
    && grep -qF '1 UNDERCOVERED' "$RUNOUT"; then
    pass "3-$boundary. ...and clean-pairs forms no pair from a session unobserved at its $boundary"
  else
    fail "3-$boundary. a $boundary-unobserved session must not be paired (rc=$rc, out: $(head -1 "$RUNOUT"))"
  fi
done

# --- Case 4: RED — NO covering sample at all is a DIFFERENT operator fact ---------------
# "The scan could not be performed" and "the scan was too sparse" are different facts and only
# one of them is a measurement, so `window-census.py` must not spell them the same way. ONE
# property differs from case 3-start: the samples inside r1-B's window are ALL absent rather
# than merely late.
TS_NONE="$TMP/ts-uncovered.jsonl"
mkts "$TS_NONE" "$(cadence_offsets 0 110)" - on
census_run "$COV" "$TS_NONE"
verdict="$(mdcell "$RUNOUT" session r1-B verdict)"
if [ "$rc" -eq 0 ] && grep -qF 'NOT MEASURED (no sample covers this window)' <<<"$verdict"; then
  pass "4. no covering sample: r1-B is NAMED '$verdict'"
else
  fail "4. an unobserved window must read NOT MEASURED (rc=$rc, verdict: '$verdict')"
fi
# NEGATED MATCH, not `grep -v`: on a multi-line input `grep -v` exits 0 as soon as ONE line
# fails to match, so it would answer "no line matched" with "some line did not".
if ! grep -qF 'UNDERCOVERED' <<<"$verdict"; then
  pass "4. ...and that verdict is TEXTUALLY DISTINCT from UNDERCOVERED (case 2/3's wording), which is a different operator action"
else
  fail "4. NOT MEASURED must not be spelled UNDERCOVERED (verdict: '$verdict')"
fi
if grep -qF 'NOT USABLE' "$RUNOUT" && grep -qF '`r1-B`' "$RUNOUT"; then
  pass "4. ...and the footer lists r1-B as NOT USABLE"
else
  fail "4. the footer must name the unusable session (out: $(tailout))"
fi
# On `clean-pairs.py` only the PROPERTY is asserted, never the label: see this file's DECLARED
# RESIDUAL — that tool collapses `no covering sample` onto its `undercovered` state, and
# pinning the collapsed label here would red the suite on a later fix that separates them.
pairs_run "$TS_NONE" "set1=$COV"
if [ "$rc" -eq 0 ] && grep -qF '**NO CLEAN PAIRS.**' "$RUNOUT" && grep -qF '**1 clean**' "$RUNOUT"; then
  pass "4. ...and clean-pairs counts only the OBSERVED session clean (1 of 2) and forms no pair"
else
  fail "4. an unobserved session must not be clean and must not pair (rc=$rc, out: $(head -1 "$RUNOUT"))"
fi

# --- Case 5: the bound itself, and WHICH SIDE OF IT IS PERMISSIVE -----------------------
# READ FROM THE IMPORTED RULE, NOT GUESSED. `ws0_quiescence.window_census_clean` refuses on
#
#     if worst[1] > MAX_SAMPLE_GAP_S:
#
# a STRICT greater-than, so a largest-unobserved-stretch EXACTLY EQUAL to the bound is
# ACCEPTED — the permissive side is `==`. Both artifact tools spell it the same way
# (`gap > MAX_SAMPLE_GAP_S`), which is what this case pins: at ${BOUND}s clean, at ${OVER}s
# undercovered. A suite that asserted the other side would red on correct input.
#
# The two arms differ in EXACTLY ONE property: whether the sample at offset ${BOUND}s exists.
# The window is [0s, ${OVER}s] in both.
B_AT="$TMP/at-bound"; B_OVER="$TMP/over-bound"
mkses "$B_AT"   1 A 1 "$(isot 0)" "$(isot "$OVER")" 400000 20000 250000 25000 1.40
mkses "$B_OVER" 1 A 1 "$(isot 0)" "$(isot "$OVER")" 400000 20000 250000 25000 1.40
TS_AT="$TMP/ts-at-bound.jsonl"; TS_OVER="$TMP/ts-over-bound.jsonl"
mkts "$TS_AT"   "0,$BOUND,$OVER" - on
mkts "$TS_OVER" "0,$OVER"        - on
census_run "$B_AT" "$TS_AT"
verdict="$(mdcell "$RUNOUT" session r1-A verdict)"
if [ "$rc" -eq 0 ] && [ "$verdict" = "clean (census 0, max gap ${BOUND}s)" ]; then
  pass "5. a largest unobserved stretch of EXACTLY ${BOUND}s is ACCEPTED: '$verdict' (the judge's test is a strict >, so == is the permissive side)"
else
  fail "5. a gap exactly at the ${BOUND}s bound must be clean — the imported rule is a strict > (rc=$rc, verdict: '$verdict')"
fi
census_run "$B_OVER" "$TS_OVER"
verdict="$(mdcell "$RUNOUT" session r1-A verdict)"
if [ "$rc" -eq 0 ] && grep -qF '**UNDERCOVERED**' <<<"$verdict" \
  && grep -qF "${OVER}s > ${BOUND}s" <<<"$verdict"; then
  pass "5. ...and ONE SECOND over the bound is refused and NAMED: '$verdict'"
else
  fail "5. a gap of ${OVER}s must read UNDERCOVERED (rc=$rc, verdict: '$verdict')"
fi
pairs_run "$TS_AT" "set1=$B_AT"
if [ "$rc" -eq 0 ] && grep -qF '**1 clean**' "$RUNOUT"; then
  pass "5. ...and clean-pairs agrees at the bound: 1 clean"
else
  fail "5. clean-pairs must accept a gap exactly at the bound (rc=$rc, out: $(head -1 "$RUNOUT"))"
fi
pairs_run "$TS_OVER" "set1=$B_OVER"
if [ "$rc" -eq 0 ] && grep -qF '**0 clean**' "$RUNOUT" && grep -qF '1 UNDERCOVERED' "$RUNOUT"; then
  pass "5. ...and clean-pairs agrees one second over: 0 clean, 1 UNDERCOVERED"
else
  fail "5. clean-pairs must refuse a gap of ${OVER}s (rc=$rc, out: $(head -1 "$RUNOUT"))"
fi

# --- Case 6: the IMPORT itself must REFUSE, not fall back to a private constant ---------
# Both tools resolve the judge RELATIVE TO THEIR OWN LOCATION (`parents[3]/scripts/perf`), so
# a copy of the artifacts directory taken out of the checkout has no judge to import. The only
# two acceptable behaviours are "import it" and "REFUSE, naming what is missing"; a private
# fallback constant would be a second copy of the gate's bound, silently disagreeing with the
# gate exactly when the checkout layout changed.
#
# THE ARTIFACT IS SUBSTITUTED, never a test-only environment seam: a seam is one more thing a
# real invoker can set. Two scratch trees identical but for ONE property — whether
# `scripts/perf/ws0_quiescence.py` exists beside the copied `docs/reports/...` subtree.
scratch_tree() {
  local root="$1" with_judge="$2" sub
  sub="$root/docs/reports/ws0-3551-artifacts"
  mkdir -p "$sub"
  cp "$PAIRS_TOOL" "$CENSUS_TOOL" "$sub/"
  if [ "$with_judge" = with-judge ]; then
    mkdir -p "$root/scripts/perf"
    cp "$JUDGE" "$root/scripts/perf/"
  fi
}
SCR_NO="$TMP/scratch-no-judge"; SCR_YES="$TMP/scratch-with-judge"
scratch_tree "$SCR_NO" without-judge
scratch_tree "$SCR_YES" with-judge
# The plant is ASSERTED TO HAVE TAKEN in both directions. A `cp` that silently did nothing, or
# a scratch that accidentally inherited a judge, would make one of these arms prove nothing.
if [ ! -e "$SCR_NO/scripts/perf/ws0_quiescence.py" ] \
  && [ -f "$SCR_YES/scripts/perf/ws0_quiescence.py" ] \
  && [ -f "$SCR_NO/docs/reports/ws0-3551-artifacts/clean-pairs.py" ]; then
  pass "6. plant took: two scratch trees differing in exactly one property (the judge's presence)"
else
  fail "6. PLANT DID NOT TAKE: the scratch trees must differ only in scripts/perf/ws0_quiescence.py"
fi
for tool in clean-pairs.py window-census.py; do
  python3 "$SCR_NO/docs/reports/ws0-3551-artifacts/$tool" --help >"$RUNOUT" 2>&1
  rc=$?
  if [ "$rc" -ne 0 ] && grep -qF 'REFUSED: cannot locate ws0_quiescence.py' "$RUNOUT" \
    && grep -qF "$SCR_NO/scripts/perf" "$RUNOUT"; then
    pass "6. $tool REFUSES with the judge absent, NAMING it and the path it looked in (rc=$rc)"
  else
    fail "6. $tool must refuse a missing judge by name, never fall back (rc=$rc, out: $(tailout))"
  fi
done
# THE CONTROL, differing in that one property: with the judge beside it the SAME copy runs and
# produces its table. Without this arm the refusals above would be satisfied by a tool that
# cannot run from a copy at all.
python3 "$SCR_YES/docs/reports/ws0-3551-artifacts/window-census.py" \
  --root "$COV" --timeseries "$TS_OK" >"$RUNOUT" 2>&1
rc=$?
if [ "$rc" -eq 0 ] && grep -qF 'All 2 sessions clean' "$RUNOUT"; then
  pass "6. CONTROL: the same window-census.py copy with the judge present runs and reports clean"
else
  fail "6. the with-judge scratch copy must run (rc=$rc, out: $(tailout))"
fi
python3 "$SCR_YES/docs/reports/ws0-3551-artifacts/clean-pairs.py" \
  --timeseries "$TS_OK" --set "set1=$COV" >"$RUNOUT" 2>&1
rc=$?
if [ "$rc" -eq 0 ] && grep -qF '**2 clean**' "$RUNOUT"; then
  pass "6. CONTROL: the same clean-pairs.py copy with the judge present runs and counts 2 clean"
else
  fail "6. the with-judge scratch copy of clean-pairs.py must run (rc=$rc, out: $(tailout))"
fi

# ===========================================================================
# PART 2 — clean-pairs.py's PAIRING LOGIC
# ===========================================================================
# --- Case 7: BOTH sessions of a pair must be clean --------------------------------------
# This is not hypothetical: set 3 round 2 of this issue's own measurement lost FOUR otherwise
# clean treatment sessions to a contaminated BASELINE. A pair is a within-round difference, so
# a treatment with no readable baseline is not half a pair, it is no pair.
#
# One round, four arms: A (the baseline) plus three treatments, all four fully covered. The two
# arms differ in EXACTLY ONE property — whether the sampler saw a competing process inside
# A's window.
P7="$TMP/pair7"
mkses "$P7" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 250000 25000 1.40
mkses "$P7" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20000 275000 22500 1.50
mkses "$P7" 1 C 3 "$(isot 240)" "$(isot 360)" 400000 20000 275000 22500 1.50
mkses "$P7" 1 D 4 "$(isot 360)" "$(isot 480)" 400000 20000 275000 22500 1.50
TS_P7_OK="$TMP/ts-p7-clean.jsonl"
TS_P7_DIRTY="$TMP/ts-p7-dirty-baseline.jsonl"
mkts "$TS_P7_OK"    "$(cadence_offsets 0 480)" -  on
mkts "$TS_P7_DIRTY" "$(cadence_offsets 0 480)" 60 on   # offset 60 is inside A's window only
pairs_run "$TS_P7_OK" "set1=$P7"
if [ "$rc" -eq 0 ] && [ "$(mdcell "$RUNOUT" arm B 'clean pairs')" = "1" ] \
  && [ "$(mdcell "$RUNOUT" arm C 'clean pairs')" = "1" ] \
  && [ "$(mdcell "$RUNOUT" arm D 'clean pairs')" = "1" ]; then
  pass "7. CONTROL: with a clean baseline all three treatments pair (B, C and D each get 1 pair)"
else
  fail "7. a clean round must yield one pair per treatment (rc=$rc, out: $(tailout))"
fi
pairs_run "$TS_P7_DIRTY" "set1=$P7"
if [ "$rc" -eq 0 ] && grep -qF '**NO CLEAN PAIRS.**' "$RUNOUT" \
  && grep -qF '**3 clean**, 1 contaminated' "$RUNOUT"; then
  pass "7. a CONTAMINATED BASELINE forms no pair at all — 3 clean treatments, 0 pairs (the set-3-round-2 event)"
else
  fail "7. a contaminated baseline must void every pair in its round (rc=$rc, out: $(head -1 "$RUNOUT"))"
fi
census_run "$P7" "$TS_P7_DIRTY"
verdict="$(mdcell "$RUNOUT" session r1-A verdict)"
if grep -qF '**CONTAMINATED** (1 of ' <<<"$verdict"; then
  pass "7. ...and window-census NAMES the baseline as the contaminated one: '$verdict'"
else
  fail "7. window-census must name r1-A contaminated with its n-of-m count (verdict: '$verdict')"
fi

# --- Case 8: a pair whose OWN CONTROL moved as much as its treatment is not readable ----
# Both sessions of a pair ran the bare-scan leg on the same CPUs with the same binary, so
# their bare-scan disagreement is that pair's own drift bound. `ctl >= abs(d_cpr)` means there
# is nothing to read the delta against, and such a pair must be REPORTED and kept out of the
# medians — reported, because "no readable pair" and "a pair existed but its control swamped
# it" are different operator facts.
#
# Two rounds. r1's control is 0.00% against a +4.00% treatment (readable). r2's control is
# 2.00% against a +1.00% treatment (not readable). Round 2's treatment `cycles_per_row` is the
# ONE property that differs between the two arms below.
P8="$TMP/pair8"
mk_p8() {
  local root="$1" r2_flight_cpr="$2"
  rm -rf "$root"
  mkses "$root" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 250000 25000 1.40
  mkses "$root" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20000 250000 26000 1.40
  mkses "$root" 2 A 1 "$(isot 240)" "$(isot 360)" 400000 20000 250000 25000 1.40
  mkses "$root" 2 B 2 "$(isot 360)" "$(isot 480)" 400000 20400 250000 "$r2_flight_cpr" 1.40
}
TS_P8="$TMP/ts-p8.jsonl"
mkts "$TS_P8" "$(cadence_offsets 0 480)" - on
# The RED arm: r2's treatment moved +1.00% under a 2.00% control.
mk_p8 "$P8" 25250
pairs_run "$TS_P8" "set1=$P8"
if [ "$rc" -eq 0 ] && [ "$(mdcell "$RUNOUT" arm B 'clean pairs')" = "1" ]; then
  pass "8. the swamped pair does NOT enter the medians — arm B counts 1 pair, not 2"
else
  fail "8. a pair whose control >= its treatment must not be counted (rc=$rc, out: $(tailout))"
fi
if grep -qF '### 1 clean pair(s) EXCLUDED — control ≥ treatment' "$RUNOUT" \
  && grep -qF 'set1 r2 B: control moved 2.00% vs treatment +1.00% — nothing to read it against' "$RUNOUT"; then
  pass "8. ...and it is REPORTED by name, with BOTH percentages: the excluded line identifies set1 r2 B"
else
  fail "8. the exclusion must be reported naming the set, round, arm and both figures (out: $(tailout))"
fi
if [ "$(mdcell "$RUNOUT" arm B 'worst pair-control')" = "0.00%" ]; then
  pass "8. ...and 'worst pair-control' is over the COUNTED pairs only (0.00%, not the excluded pair's 2.00%)"
else
  fail "8. worst pair-control must describe the counted pairs (got: $(mdcell "$RUNOUT" arm B 'worst pair-control'))"
fi
# THE CONTROL, differing in exactly that one property: r2's treatment moves +4.00% instead,
# clearing its own 2.00% drift bound, and the pair is counted.
mk_p8 "$P8" 26000
pairs_run "$TS_P8" "set1=$P8"
if [ "$rc" -eq 0 ] && [ "$(mdcell "$RUNOUT" arm B 'clean pairs')" = "2" ] \
  && ! grep -qF 'EXCLUDED' "$RUNOUT"; then
  pass "8. CONTROL: a treatment that clears its own drift bound IS counted (2 pairs, no exclusions)"
else
  fail "8. a readable pair must be counted (rc=$rc, out: $(tailout))"
fi
# AND THE EXCLUSION MUST STILL BE REPORTED WHEN IT IS THE ONLY PAIR. The no-pairs branch used
# to `return` before the excluded section, so a run whose EVERY pair was swamped printed a bare
# `NO CLEAN PAIRS` and silently dropped the reason — precisely the case where the reason IS the
# information. Measured on this fixture before the fix: the exclusion line was absent.
P8B="$TMP/pair8-all-excluded"
mkses "$P8B" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 250000 25000 1.40
mkses "$P8B" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20400 250000 25250 1.40
pairs_run "$TS_P8" "set1=$P8B"
if [ "$rc" -eq 0 ] && grep -qF '**NO CLEAN PAIRS.**' "$RUNOUT" \
  && grep -qF '### 1 clean pair(s) EXCLUDED — control ≥ treatment' "$RUNOUT" \
  && grep -qF 'set1 r1 B: control moved 2.00% vs treatment +1.00%' "$RUNOUT"; then
  pass "8. an ALL-EXCLUDED run still reports WHY — 'NO CLEAN PAIRS' plus the named exclusion"
else
  fail "8. a run whose every pair was excluded must still report the exclusions (rc=$rc, out: $(tailout))"
fi

# --- Case 9: POOLED ACROSS SETS, NEVER ACROSS ROUNDS ------------------------------------
# The tool's whole reason to exist is that a partly contaminated set still holds valid pairs,
# so pairs from different SETS must pool into one median. What must never happen is a pair
# spanning two ROUNDS (or two sets): Method §3b step 4 differences WITHIN a round, and two
# sessions from different rounds ran minutes apart under different box conditions, so their
# difference is not a treatment effect.
#
# 9a. POOLING: two sets, one round each, the same (A,B) shape. Arm B must show TWO pairs.
P9A1="$TMP/pair9-set1"; P9A2="$TMP/pair9-set2"
mkses "$P9A1" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 250000 25000 1.40
mkses "$P9A1" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20000 275000 22500 1.50
mkses "$P9A2" 1 A 1 "$(isot 240)" "$(isot 360)" 400000 20000 250000 25000 1.40
mkses "$P9A2" 1 B 2 "$(isot 360)" "$(isot 480)" 400000 20000 275000 22500 1.50
TS_P9="$TMP/ts-p9.jsonl"
mkts "$TS_P9" "$(cadence_offsets 0 480)" - on
pairs_run "$TS_P9" "set1=$P9A1" "set2=$P9A2"
if [ "$rc" -eq 0 ] && [ "$(mdcell "$RUNOUT" arm B 'clean pairs')" = "2" ] \
  && grep -qF '| set1 | 1 | B |' "$RUNOUT" && grep -qF '| set2 | 1 | B |' "$RUNOUT"; then
  pass "9a. pairs POOL across sets — arm B has 2 pairs and the per-pair table names both sets"
else
  fail "9a. pairs from different sets must pool (rc=$rc, out: $(tailout))"
fi

# 9b. NEVER ACROSS ROUNDS, and the fixture is built so a violation would CHANGE THE ANSWER:
# round 1 holds ONLY the baseline and round 2 ONLY the treatment. Both are clean and fully
# covered, and their figures WOULD form a readable pair (control 0.00% against a -10.00%
# treatment) if the tool keyed its grid on the arm alone. It must report none.
P9B="$TMP/pair9-cross-round"
mkses "$P9B" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 250000 25000 1.40
mkses "$P9B" 2 B 1 "$(isot 120)" "$(isot 240)" 400000 20000 275000 22500 1.50
pairs_run "$TS_P9" "set1=$P9B"
if [ "$rc" -eq 0 ] && grep -qF '**2 clean**' "$RUNOUT" && grep -qF '**NO CLEAN PAIRS.**' "$RUNOUT"; then
  pass "9b. a baseline in round 1 and a treatment in round 2 form NO pair — and BOTH are clean, so the absence is the pairing rule and not a dirty session"
else
  fail "9b. sessions from different rounds must never pair (rc=$rc, out: $(head -1 "$RUNOUT"))"
fi

# 9c. NEVER ACROSS SETS EITHER, for the same reason: pooling is over PAIRS, not over sessions.
# One set holds only the baseline, the other only the treatment, both in round 1 — the exact
# shape a grid keyed on round alone would pair.
P9C1="$TMP/pair9-c-set1"; P9C2="$TMP/pair9-c-set2"
mkses "$P9C1" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 250000 25000 1.40
mkses "$P9C2" 1 B 1 "$(isot 120)" "$(isot 240)" 400000 20000 275000 22500 1.50
pairs_run "$TS_P9" "set1=$P9C1" "set2=$P9C2"
if [ "$rc" -eq 0 ] && grep -qF '**2 clean**' "$RUNOUT" && grep -qF '**NO CLEAN PAIRS.**' "$RUNOUT"; then
  pass "9c. a baseline in set1 and a treatment in set2, same round number, form NO pair (both clean)"
else
  fail "9c. sessions from different sets must never pair (rc=$rc, out: $(head -1 "$RUNOUT"))"
fi

# --- Case 10: the medians and direction counts, NUMERICALLY -----------------------------
# Every value is hand-chosen so each cell has exactly one expected answer, and the fixture is
# built to pin the SIGN IN BOTH DIRECTIONS: two rounds where the treatment is FASTER (rows/s
# up, cycles/row down) and one where it is SLOWER. A one-sided fixture cannot tell a correct
# report from one that prints an absolute value or an inverted quantity.
#
#   round 1  Δcycles/row  -4.00%   Δrows/s  +10.00%   (faster)
#   round 2  Δcycles/row  -8.00%   Δrows/s  +20.00%   (faster)
#   round 3  Δcycles/row  +4.00%   Δrows/s   -5.00%   (slower)
#   => median Δcycles/row -4.00%, median Δrows/s +10.00%, direction 2/3 up,
#      worst pair-control 0.00% (every pair's bare-scan legs agree exactly),
#      median IPC 2.0000 over the flight IPCs 1.0 / 2.0 / 3.0
MED="$TMP/medians"
mkses "$MED" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 100000 25000 1.40
mkses "$MED" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20000 110000 24000 1.00
mkses "$MED" 2 A 1 "$(isot 240)" "$(isot 360)" 400000 20000 100000 25000 1.40
mkses "$MED" 2 B 2 "$(isot 360)" "$(isot 480)" 400000 20000 120000 23000 2.00
mkses "$MED" 3 A 1 "$(isot 480)" "$(isot 600)" 400000 20000 100000 25000 1.40
mkses "$MED" 3 B 2 "$(isot 600)" "$(isot 720)" 400000 20000  95000 26000 3.00
TS_MED="$TMP/ts-medians.jsonl"
mkts "$TS_MED" "$(cadence_offsets 0 720)" - on
pairs_run "$TS_MED" "set1=$MED"
if [ "$rc" -ne 0 ]; then
  fail "10. the medians fixture must run (rc=$rc, out: $(tailout))"
else
  # Each cell is read BY COLUMN HEADER, never by position: a positional read keeps passing
  # after a column moves and is then asserting about a different quantity.
  # IFS is set to TAB alone: the default IFS splits on SPACE too, so `median Δrows/s` arrived
  # as column=`median`, and mdcell's own diagnostic named the wrong column while the assertion
  # compared an empty string — a red that describes a defect in the harness, not the tool.
  while IFS=$'\t' read -r column expected; do
    got="$(mdcell "$RUNOUT" arm B "$column")"
    if [ "$got" = "$expected" ]; then
      pass "10. arm B '$column' = $got"
    else
      fail "10. arm B '$column' must be $expected (got '$got')"
    fi
  done <<'CELLS'
clean pairs	3
median Δcycles/row	-4.00%
median Δrows/s	+10.00%
direction (rows/s)	2/3 up
worst pair-control	0.00%
median IPC	2.0000
CELLS
  # ...and the per-pair table, whose rows pin the sign of BOTH quantities against each other:
  # a faster treatment must read cycles/row DOWN and rows/s UP, and a slower one the reverse.
  # A whole-row match also reds on a column swap, which a per-cell read of this table could
  # not see (every row's first cell is the same set label).
  for row in \
    '| set1 | 1 | B | -4.00% | +10.00% | 0.00% |' \
    '| set1 | 2 | B | -8.00% | +20.00% | 0.00% |' \
    '| set1 | 3 | B | +4.00% | -5.00% | 0.00% |'; do
    if grep -qF "$row" "$RUNOUT"; then
      pass "10. per-pair row present and exact: $row"
    else
      fail "10. missing per-pair row: $row"
    fi
  done
fi

# The direction count at both extremes, so `N/M up` is pinned as a COUNT OF POSITIVES and not
# a count of pairs. Two one-pair fixtures differing in exactly one property: whether the
# treatment's rows/s is above or below the baseline's.
for dir in faster slower; do
  D10="$TMP/dir-$dir"
  if [ "$dir" = faster ]; then f_rps=110000; f_cpr=24000; expect_up="1/1 up"; expect_rps="+10.00%"; expect_cpr="-4.00%"
  else f_rps=90000; f_cpr=27500; expect_up="0/1 up"; expect_rps="-10.00%"; expect_cpr="+10.00%"; fi
  mkses "$D10" 1 A 1 "$(isot 0)"   "$(isot 120)" 400000 20000 100000 25000 1.40
  mkses "$D10" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20000 "$f_rps" "$f_cpr" 1.40
  pairs_run "$TS_MED" "set1=$D10"
  if [ "$rc" -eq 0 ] \
    && [ "$(mdcell "$RUNOUT" arm B 'direction (rows/s)')" = "$expect_up" ] \
    && [ "$(mdcell "$RUNOUT" arm B 'median Δrows/s')" = "$expect_rps" ] \
    && [ "$(mdcell "$RUNOUT" arm B 'median Δcycles/row')" = "$expect_cpr" ]; then
    pass "10-$dir. a $dir treatment reads $expect_rps rows/s, $expect_cpr cycles/row, $expect_up"
  else
    fail "10-$dir. a $dir treatment must read $expect_rps / $expect_cpr / $expect_up (rc=$rc, out: $(tailout))"
  fi
done

# ===========================================================================
# PART 3 — window-census.py's REPORT
# ===========================================================================
# --- Case 11: a contaminated session is named CONTAMINATED with its n-of-m count --------
# The count is the information: `3 of 13` says a competitor was present for part of the window,
# which is a different fact from `13 of 13`, and a bare CONTAMINATED says neither.
C11="$TMP/census11"
mkses "$C11" 1 A 1 "$(isot 0)" "$(isot 120)" 400000 20000 250000 25000 1.40
TS_C11_OK="$TMP/ts-c11-clean.jsonl"; TS_C11_DIRTY="$TMP/ts-c11-dirty.jsonl"
mkts "$TS_C11_OK"    "$(cadence_offsets 0 120)" -        on
mkts "$TS_C11_DIRTY" "$(cadence_offsets 0 120)" 40,50,60 on
census_run "$C11" "$TS_C11_DIRTY"
verdict="$(mdcell "$RUNOUT" session r1-A verdict)"
if [ "$rc" -eq 0 ] && [ "$verdict" = '**CONTAMINATED** (3 of 13)' ]; then
  pass "11. a contaminated session is NAMED with its count: '$verdict'"
else
  fail "11. a contaminated session must read '**CONTAMINATED** (3 of 13)' (rc=$rc, got '$verdict')"
fi
if [ "$(mdcell "$RUNOUT" session r1-A competing)" = "3" ] \
  && [ "$(mdcell "$RUNOUT" session r1-A 'in-window samples')" = "13" ]; then
  pass "11. ...and the competing and in-window-sample columns carry the same 3 and 13"
else
  fail "11. the columns must agree with the verdict (competing=$(mdcell "$RUNOUT" session r1-A competing), samples=$(mdcell "$RUNOUT" session r1-A 'in-window samples'))"
fi
if grep -qF '**1 of 1 session(s) NOT USABLE**' "$RUNOUT"; then
  pass "11. ...and the footer counts it unusable"
else
  fail "11. the footer must count the contaminated session unusable (out: $(tailout))"
fi
# THE CONTROL, differing in exactly one property (whether any sample carries a competitor).
census_run "$C11" "$TS_C11_OK"
if [ "$rc" -eq 0 ] && [ "$(mdcell "$RUNOUT" session r1-A competing)" = "0" ] \
  && grep -qF 'All 1 sessions clean' "$RUNOUT"; then
  pass "11. CONTROL: the same session over an uncontaminated timeseries reads clean"
else
  fail "11. the control must read clean (rc=$rc, out: $(tailout))"
fi

# --- Case 12: the per-CPU column is TOTAL busy INCLUDING OUR OWN measurement ------------
# A FIRST DRAFT OF THIS TOOL IMPLIED THE COLUMN BOUNDED CONTAMINATION, which it cannot: during
# a session the pinned CPUs are busy BY DESIGN — that is the measured server and the measured
# scan — so it cannot separate a peer's cycles from ours. The corrected wording is pinned here
# so it cannot regress into the claim it cannot support.
#
# The header must also NAME the CPUs the tool actually differences, derived from the tool's own
# PINS rather than restated, so the column cannot claim one CPU set while measuring another.
_pins_line=$(grep -oE '^PINS = \([^)]*\)' "$CENSUS_TOOL" | head -1) || true
if [ -z "$_pins_line" ]; then
  fail "12. derivation: no top-level PINS = (...) in $CENSUS_TOOL, so the header's CPU set cannot be checked"
else
  pass "12. derivation: the pinned CPU set is read from the tool ($_pins_line)"
  census_run "$C11" "$TS_C11_OK"
  hdr=$(grep -m1 -F '| session |' "$RUNOUT")
  if grep -qF 'TOTAL incl. our own' <<<"$hdr"; then
    pass "12. the per-CPU column header declares it is TOTAL busy INCLUDING our own measurement"
  else
    fail "12. the per-CPU column header must say TOTAL incl. our own (header: $hdr)"
  fi
  missing=""
  for cpu in $(tr -d 'PINS=() ' <<<"$_pins_line" | tr ',' ' '); do
    grep -qF "$cpu" <<<"$hdr" || missing="$missing $cpu"
  done
  if [ -z "$missing" ]; then
    pass "12. ...and it names every CPU the tool differences (${_pins_line#PINS = })"
  else
    fail "12. the header omits pinned CPU(s):$missing (header: $hdr)"
  fi
fi
# The reading itself, numerically. The fixture's cumulative counters advance 1000 total / 500
# idle per sample on every pinned CPU, so every consecutive pair differences to exactly 50.0%
# and the median has one expected answer.
# The column NAME is derived from the emitted header, not restated: a restated name would
# make this cell read empty (and `mdcell` name a missing column) the moment the header is
# reworded, which is a red describing the harness rather than the tool.
col=$(python3 - "$RUNOUT" <<'HDR'
import pathlib
import sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if not line.startswith("| session |"):
        continue
    for cell in (c.strip() for c in line.strip("|").split("|")):
        if "pinned-CPU" in cell:
            print(cell)
            raise SystemExit(0)
raise SystemExit(3)
HDR
)
if [ -z "$col" ]; then
  fail "12. derivation: no pinned-CPU column in the emitted header, so its value cannot be read"
  col='pinned-CPU-column-NOT-DERIVED'
else
  pass "12. derivation: the per-CPU column name is read from the emitted header"
fi
if [ "$(mdcell "$RUNOUT" session r1-A "$col")" = "50.0%" ]; then
  pass "12. the per-CPU cell is a READING: 50.0% from counters that advance 1000 total / 500 idle per sample"
else
  fail "12. the per-CPU cell must read 50.0% (got '$(mdcell "$RUNOUT" session r1-A "$col")')"
fi
# ...and an ABSENT `percpu` block is NOT MEASURED, never 0.0%: an absent snapshot and an idle
# CPU are different facts and only one of them is a measurement. ONE property differs.
TS_NOPCT="$TMP/ts-c11-no-percpu.jsonl"
mkts "$TS_NOPCT" "$(cadence_offsets 0 120)" - off
census_run "$C11" "$TS_NOPCT"
if [ "$rc" -eq 0 ] && [ "$(mdcell "$RUNOUT" session r1-A "$col")" = "NOT MEASURED" ]; then
  pass "12. an absent percpu block reads NOT MEASURED, not 0.0% (an absent snapshot is not an idle CPU)"
else
  fail "12. an absent percpu block must read NOT MEASURED (got '$(mdcell "$RUNOUT" session r1-A "$col")')"
fi
# THE FOOTER'S CORRECTED WORDING, pinned phrase by phrase...
census_run "$C11" "$TS_C11_OK"
while IFS= read -r phrase; do
  [ -n "$phrase" ] || continue
  if grep -qF "$phrase" "$RUNOUT"; then
    pass "12. footer states: '$phrase'"
  else
    fail "12. the footer must state '$phrase' — this is the corrected wording and must not regress"
  fi
done <<'PHRASES'
The pinned-CPU column is TOTAL busy
dominated by THIS MEASUREMENT during a session
does NOT separate a peer's cycles from ours
is NOT a contamination bound
`competing_count` bounds compilers, linkers and the `agent-gate.sh` script and NOT total foreign load
PHRASES
# ...and the NEGATIVE half, which the phrase list above cannot express: the report must never
# call the column a contamination bound WITHOUT the negation. Asserted as an equality of
# counts rather than an absence, because `NOT a contamination bound` contains the very
# substring a bare absence test would look for.
if python3 - "$RUNOUT" <<'PY'
import pathlib
import sys
text = pathlib.Path(sys.argv[1]).read_text()
claims = text.count("contamination bound")
negated = text.count("NOT a contamination bound")
if claims == 0:
    sys.stderr.write("the phrase 'contamination bound' is absent entirely\n")
    raise SystemExit(2)
if claims != negated:
    sys.stderr.write(f"{claims} mention(s) of 'contamination bound', only {negated} negated\n")
    raise SystemExit(1)
raise SystemExit(0)
PY
then
  pass "12. every mention of 'contamination bound' in the report is NEGATED — the column is never claimed to be one"
else
  fail "12. the report must never call the per-CPU column a contamination bound un-negated"
fi

# ===========================================================================
# PART 4 — THE NAMED REFUSALS
# ===========================================================================
# Both tools PRODUCE A PUBLISHED REPORT, so every input they cannot read must be a refusal
# that NAMES what was wrong — never a table with a row quietly missing from it. Each case
# below asserts the diagnostic's own text, not merely a non-zero exit, which an unrelated
# breakage produces identically.

# R1. window-census: a root holding no session at all.
EMPTY_ROOT="$TMP/no-sessions"
mkdir -p "$EMPTY_ROOT"
census_run "$EMPTY_ROOT" "$TS_C11_OK"
if [ "$rc" -ne 0 ] && grep -qF "REFUSED: no abc-window.json under $EMPTY_ROOT" "$RUNOUT"; then
  pass "R1. window-census REFUSES a root with no sessions, naming the root (rc=$rc)"
else
  fail "R1. an empty root must be a named refusal (rc=$rc, out: $(tailout))"
fi

# R2. an EMPTY timeseries, in BOTH tools. A tool that read it as "no competitor observed"
# would certify every window in the set on no evidence at all.
EMPTY_TS="$TMP/empty.jsonl"
: > "$EMPTY_TS"
census_run "$C11" "$EMPTY_TS"
if [ "$rc" -ne 0 ] && grep -qF "REFUSED: $EMPTY_TS holds no samples" "$RUNOUT"; then
  pass "R2. window-census REFUSES an empty timeseries by name (rc=$rc)"
else
  fail "R2. an empty timeseries must be a named refusal in window-census (rc=$rc, out: $(tailout))"
fi
pairs_run "$EMPTY_TS" "set1=$C11"
if [ "$rc" -ne 0 ] && grep -qF "REFUSED: $EMPTY_TS holds no samples" "$RUNOUT"; then
  pass "R2. clean-pairs REFUSES an empty timeseries by name (rc=$rc)"
else
  fail "R2. an empty timeseries must be a named refusal in clean-pairs (rc=$rc, out: $(tailout))"
fi

# R3. clean-pairs: a session with no warm bare-scan leg has NO CONTROL, so it is not half a
# pair — it is unreadable, and silently dropping it would shrink the pair count with no note.
NOCTL="$TMP/no-control"
mkses "$NOCTL" 1 A 1 "$(isot 0)" "$(isot 120)" 400000 20000 250000 25000 1.40
python3 - "$NOCTL/r1-A/results.json" <<'DROPSCAN'
import json
import pathlib
import sys
p = pathlib.Path(sys.argv[1])
doc = json.loads(p.read_text())
doc["measurements"] = [m for m in doc["measurements"]
                       if not (m["temperature"] == "warm" and m["arm"] == "bare_scan")]
p.write_text(json.dumps(doc))
DROPSCAN
pairs_run "$TS_C11_OK" "set1=$NOCTL"
if [ "$rc" -eq 2 ] && grep -qF 'has no warm bare_scan leg — no control, so no readable pair' "$RUNOUT"; then
  pass "R3. clean-pairs REFUSES (exit 2) a session with no warm bare-scan control, naming it"
else
  fail "R3. a session with no control must be a named exit-2 refusal (rc=$rc, out: $(tailout))"
fi

# R4. clean-pairs: a malformed `--set` spec.
pairs_run "$TS_C11_OK" "notalabelspec"
if [ "$rc" -eq 2 ] && grep -qF "REFUSED: --set 'notalabelspec' is not LABEL=DIR" "$RUNOUT"; then
  pass "R4. clean-pairs REFUSES a --set that is not LABEL=DIR, quoting the value (rc=$rc)"
else
  fail "R4. a malformed --set must be a named exit-2 refusal (rc=$rc, out: $(tailout))"
fi

# R5. clean-pairs: a session directory with a `results.json` and NO window record. The window
# is what says which instants to judge, so a session without one cannot be judged at all — and
# `results.json` alone carries no provenance.
NOWIN="$TMP/no-window"
mkses "$NOWIN" 1 A 1 "$(isot 0)" "$(isot 120)" 400000 20000 250000 25000 1.40
rm -f "$NOWIN/r1-A/abc-window.json"
pairs_run "$TS_C11_OK" "set1=$NOWIN"
if [ "$rc" -eq 2 ] && grep -qF "REFUSED: $NOWIN/r1-A/abc-window.json is absent" "$RUNOUT"; then
  pass "R5. clean-pairs REFUSES a session with no window record, naming the missing file (rc=$rc)"
else
  fail "R5. an absent abc-window.json must be a named exit-2 refusal (rc=$rc, out: $(tailout))"
fi
# ...and window-census SKIPS such a directory rather than refusing, because its subject IS the
# window record — a directory without one is not a session it can judge. Asserted so the
# difference between the two tools is a decision on the record and not a surprise: the OTHER
# session in the same root must still be reported.
mkses "$NOWIN" 1 B 2 "$(isot 120)" "$(isot 240)" 400000 20000 275000 22500 1.50
census_run "$NOWIN" "$TS_P9"
if [ "$rc" -eq 0 ] && grep -qF '`r1-B`' "$RUNOUT" && ! grep -qF '`r1-A`' "$RUNOUT"; then
  pass "R5. window-census reports the session WITH a window record and omits the one without (its subject IS that record)"
else
  fail "R5. window-census must still report r1-B while omitting the window-less r1-A (rc=$rc, out: $(tailout))"
fi

# ===========================================================================
# PART 5 — THE COVERAGE FIXTURES ARE ONLY REJECTED BECAUSE OF THE BOUND
# ===========================================================================
# Cases 2, 3-start and 3-end assert that a gappy fixture is REFUSED. On its own that leaves a
# hole: the fixture might be refused for some unrelated reason, in which case the cases would
# keep passing after the coverage rule was deleted. So each is paired with a POSITIVE CONTROL
# ON THE ORACLE — a scratch copy of the tool with the bound comparison REMOVED must ACCEPT the
# same fixture. That is what makes the refusal attributable to the rule.
#
# THE ARTIFACT IS SUBSTITUTED (a mutated scratch copy, judge and all), never a test-only seam,
# and EVERY PLANT IS ASSERTED TO HAVE TAKEN: a mutation whose pattern stopped matching leaves
# the copy byte-identical to the shipped tool, and the arm then agrees with the shipped
# behaviour while proving nothing.
cat > "$TMP/mutate.py" <<'MUT'
import pathlib
import sys

target, mutation = pathlib.Path(sys.argv[1]), sys.argv[2]
before = target.read_text()
if mutation == "no-bound":
    # The ORIGINAL DEFECT, restored: a non-empty sample set is treated as coverage. The empty
    # check is left in place, so this mutant differs from the shipped tool in exactly the
    # coverage BOUND and in nothing else.
    after = before.replace("elif gap > MAX_SAMPLE_GAP_S:", "elif False:")
elif mutation == "interior-only":
    # The gap set loses its two BOUNDARY terms and keeps every interior one, which is the
    # shape a naive consecutive-differences scan has.
    after = before.replace(
        "gaps = [inst[0] - a] + [inst[i + 1] - inst[i] for i in range(len(inst) - 1)]"
        " + [b - inst[-1]]",
        "gaps = [0.0] + [inst[i + 1] - inst[i] for i in range(len(inst) - 1)] + [0.0]")
else:
    sys.stderr.write(f"unknown mutation {mutation!r}\n")
    raise SystemExit(2)
if after == before:
    sys.stderr.write(f"PLANT DID NOT TAKE: {mutation} matched nothing in {target}\n")
    raise SystemExit(1)
target.write_text(after)
MUT

# One scratch tree per mutation, each a full copy WITH the judge (so the only difference from
# the shipped tool is the planted one, never a failed import).
mutant_census() {
  local mutation="$1" root="$TMP/mutant-$1"
  rm -rf "$root"
  scratch_tree "$root" with-judge
  if python3 "$TMP/mutate.py" "$root/docs/reports/ws0-3551-artifacts/window-census.py" "$mutation"; then
    printf '%s\n' "$root/docs/reports/ws0-3551-artifacts/window-census.py"
    return 0
  fi
  return 1
}
# run_mutant <tool-path> <root> <timeseries> — into $RUNOUT; sets `rc`.
run_mutant() {
  python3 "$1" --root "$2" --timeseries "$3" >"$RUNOUT" 2>&1
  rc=$?
}

for mutation in no-bound interior-only; do
  MUTTOOL="$(mutant_census "$mutation")"
  if [ -z "$MUTTOOL" ] || [ ! -f "$MUTTOOL" ]; then
    fail "5-$mutation. PLANT DID NOT TAKE: the '$mutation' mutation matched nothing, so this control would prove nothing"
    continue
  fi
  pass "5-$mutation. plant took: a scratch copy of window-census.py differing from the shipped tool only in the '$mutation' property"
  # The fixtures this mutation is expected to let through. `no-bound` deletes the bound, so all
  # three gappy fixtures pass it; `interior-only` deletes only the boundary terms, so the
  # INTERIOR fixture must still be caught — which is what proves case 2 and case 3 are testing
  # different halves of the rule rather than one.
  if [ "$mutation" = no-bound ]; then
    expect_interior=clean
  else
    expect_interior=undercovered
  fi
  while IFS=$'\t' read -r label tsfile expect; do
    [ -n "$label" ] || continue
    run_mutant "$MUTTOOL" "$COV" "$tsfile"
    verdict="$(mdcell "$RUNOUT" session r1-B verdict)"
    case "$expect" in
      clean)
        if [ "$rc" -eq 0 ] && grep -q '^clean (census 0' <<<"$verdict"; then
          pass "5-$mutation. CONTROL: with '$mutation' planted, the $label fixture is ACCEPTED ('$verdict') — so the shipped tool's refusal is the RULE, not a malformed fixture"
        else
          fail "5-$mutation. the $label fixture must be accepted with '$mutation' planted, or case 2/3's refusal is not attributable to the coverage rule (rc=$rc, verdict: '$verdict')"
        fi ;;
      undercovered)
        if [ "$rc" -eq 0 ] && grep -qF '**UNDERCOVERED**' <<<"$verdict"; then
          pass "5-$mutation. DISCRIMINATION: the $label fixture is STILL caught with only the boundary terms removed — case 2 and case 3 test different halves of the gap set"
        else
          fail "5-$mutation. the $label fixture must still be caught by the boundary-only mutant (rc=$rc, verdict: '$verdict')"
        fi ;;
    esac
  done <<CTRL
interior	$TMP/ts-interior-gap.jsonl	$expect_interior
window-start	$TMP/ts-start-gap.jsonl	clean
window-end	$TMP/ts-end-gap.jsonl	clean
CTRL
done

# ==========================================================================
# A MINIMUM CHECK COUNT — this file has no `set -e`
# ==========================================================================
# Without it, a block that silently never executes (a `$( )` whose command vanished, a helper
# returning early) LOWERS the count and registers NO failure, and the gate reads only the exit
# code. The floor is DERIVED FROM A MEASURED RUN and set below the observed count, so adding a
# case cannot red the suite. RE-DERIVE IT BY RUNNING THE SUITE at each addition, never by
# counting the source — the loops and the driven tables multiply.
MIN_CHECKS=80
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks #3551 artifact-tool (coverage rule / pairing / census report) checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks check(s) failed"
exit 1


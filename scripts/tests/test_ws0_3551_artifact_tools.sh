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

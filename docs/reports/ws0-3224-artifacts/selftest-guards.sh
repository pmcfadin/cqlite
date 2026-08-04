#!/usr/bin/env bash
# =============================================================================
# #3224 — the six guards, each shown REJECTING the bad input it now catches.
#
#     bash docs/reports/ws0-3224-artifacts/selftest-guards.sh
#
# Runs in seconds, needs no perf, no root, no C compiler and no bare-metal box:
# every guard is driven with an injected or crafted input. That portability is
# the point — the six defects roborev found on PR #3286 all lived in code whose
# only entry point required ~20 minutes of exclusive bare metal, which is why
# none of them had ever been exercised.
#
# WHY THIS FILE EXISTS AT ALL, stated because it is the whole standard being met:
# a guard added without an input that exercises it is the same defect wearing a
# fix's clothes. Every case below therefore asserts a VERDICT, not merely that
# the code runs — and for each fix there are TWO cases in tension:
#
#     the bad input the guard must now REJECT, and
#     the good input it must still ACCEPT.
#
# One without the other proves nothing. A guard that rejects everything is not a
# guard, and the false-FAIL direction is exactly how finding #1 got in: someone
# tightened a check and red-flagged a healthy host. So the healthy-host record is
# pinned here as a regression input, and re-inverting that ordering now fails a
# test instead of silently rejecting good hardware.
#
# Provenance of every "good" input: the artefacts COMMITTED IN THIS PR, or, for
# finding #1, the owner's manual healthy-host walk recorded verbatim in
# positive-control.sh's P3-P5 header block. Nothing here is invented data.
# =============================================================================
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT INT TERM

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '   PASS  %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '   FAIL  %s\n' "$1"; }
# check <description> <expected> <actual>
check() {
  if [ "$2" = "$3" ]; then ok "$1 -> $3"; else bad "$1 -> got '$3', want '$2'"; fi
}
# expect_rc <description> <expected-rc> <command...>
expect_rc() {
  local desc="$1" want="$2"; shift 2
  "$@" > "$TMP/out.log" 2>&1; local got=$?
  if [ "$got" = "$want" ]; then ok "$desc (rc=$got)"
  else bad "$desc (rc=$got, want $want); output:"; sed 's/^/         | /' "$TMP/out.log" >&2; fi
}
section() { printf '\n== %s\n' "$*"; }

echo "==== #3224 GUARD SELFTEST ===="
echo "artefact root: $HERE"

# =============================================================================
section "FINDING 1 + 3 — positive-control verdict logic (harness/verdict-logic.sh)"
# Sourced into THIS shell, so the injected globals are the real ones the script
# uses. ACCESSES only feeds the reported per-access rate, which is not gated.
ACCESSES=20000000
# shellcheck source=harness/verdict-logic.sh
source "$HERE/harness/verdict-logic.sh"
declare -A MED MUXMIN

# The healthy-host record, verbatim from positive-control.sh's P3-P5 header:
# owner's manual cache-hostile-vs-friendly walk over 512 MiB on the i4i.metal
# target box, 2026-08-04.
#          arm       LLC-loads  LLC-load-misses  miss rate
#          friendly    389,812           54,391     13.95%
#          hostile     110,149           67,449     61.23%
load_healthy_host() {
  MED=(); MUXMIN=(); EV_VERDICT=(); EV_MOVE=(); EV_RATE=()
  MED[friendly/LLC-loads]=389812;       MED[hostile/LLC-loads]=110149
  MED[friendly/LLC-load-misses]=54391;  MED[hostile/LLC-load-misses]=67449
  MED[friendly/cache-references]=389858; MED[hostile/cache-references]=3118864
  compute_missrate
}

load_healthy_host
echo "   healthy-host miss rate: friendly=$(show_milli "$MISSRATE_F") hostile=$(show_milli "$MISSRATE_H") rise=$(show_milli "$MISSRATE_RISE")x"
evaluate LLC-load-misses
# The two assertions together are what pin the ORDERING. The first establishes
# that the raw-movement gate WOULD have rejected this healthy host (1.240x is
# below the 2.000x floor); the second that the verdict is nonetheless OK. Assert
# only the second and the test would still pass with the gate reordered wrongly on
# some future host whose raw misses happen to move more than 2x.
check "healthy host: raw LLC-load-misses movement is BELOW the 2x floor (so ordering decides)" \
      "1.240" "$(show_milli "${EV_MOVE[LLC-load-misses]}")"
check "healthy host: LLC-load-misses verdict (P4 gates on the miss rate, not raw movement)" \
      "OK" "${EV_VERDICT[LLC-load-misses]}"

# ...and P4 still has teeth. Same loads, but hostile misses scaled so the miss
# RATE is flat while raw movement is a healthy 3.55x: the movement gate would
# wave this through, so only the miss-rate gate can catch it.
load_healthy_host
MED[hostile/LLC-load-misses]=15311      # 15311/110149 = 13.9% = the friendly rate
compute_missrate
evaluate LLC-load-misses
check "flat miss rate: movement alone would PASS it (3.552x >= 2x)" \
      "3.552" "$(show_milli "${EV_MOVE[LLC-load-misses]}")"
check "flat miss rate: REJECTED anyway (the fix did not weaken P4)" \
      "UNRELIABLE_MISSRATE_FLAT" "${EV_VERDICT[LLC-load-misses]}"

# A silent counter is still a silent counter.
load_healthy_host
MED[friendly/LLC-load-misses]=0; MED[hostile/LLC-load-misses]=0
compute_missrate
evaluate LLC-load-misses
check "both arms zero: still SILENT_ZERO (the #3217 failure mode)" \
      "SILENT_ZERO" "${EV_VERDICT[LLC-load-misses]}"

# Finding 3: multiplexing is a VERDICT now, not a printed warning, on EITHER arm.
load_healthy_host
MUXMIN[hostile/cache-references]=100; MUXMIN[friendly/cache-references]=100
evaluate cache-references
check "unmultiplexed counter with real movement: accepted" \
      "OK" "${EV_VERDICT[cache-references]}"

load_healthy_host
MUXMIN[hostile/cache-references]=87     # below MUX_MIN_PCT=99
evaluate cache-references
check "multiplexed HOSTILE arm (87% enabled): rejected, not warned" \
      "UNRELIABLE_MULTIPLEXED" "${EV_VERDICT[cache-references]}"

# This case is the one the old code could not catch at all: report_ev looked only
# at MUXMIN[hostile/...], so a multiplexed FRIENDLY arm was invisible. The
# friendly arm is half of every ratio in the gate.
load_healthy_host
MUXMIN[friendly/cache-references]=87
evaluate cache-references
check "multiplexed FRIENDLY arm (87% enabled): also rejected (old code saw only hostile)" \
      "UNRELIABLE_MULTIPLEXED" "${EV_VERDICT[cache-references]}"

# And a multiplexed count cannot be laundered through the P4 special case either.
load_healthy_host
MUXMIN[hostile/LLC-load-misses]=90
evaluate LLC-load-misses
check "multiplexed LLC-load-misses: rejected before the miss-rate branch is reached" \
      "UNRELIABLE_MULTIPLEXED" "${EV_VERDICT[LLC-load-misses]}"

# Round 2 finding #5: an UNREADABLE enabled% is not a healthy one. This is finding 3
# one level down — the fix for 3 made multiplexing gating, then keyed the gate on an
# accumulator whose default for "could not be measured" was mmin=10000, i.e. an
# unverifiable count read as 10,000% enabled and cleared the 99% floor more
# comfortably than a healthy counter.
load_healthy_host
MUXMIN[hostile/cache-references]=MUX_UNREADABLE
MUXMIN[friendly/cache-references]=100
evaluate cache-references
check "UNREADABLE enabled% on the hostile arm: rejected (not treated as healthy)" \
      "UNRELIABLE_MUX_UNREADABLE" "${EV_VERDICT[cache-references]}"
check "...and the reported mux figure says UNREADABLE rather than the other arm's 100" \
      "UNREADABLE" "$(ev_mux_min cache-references)"

load_healthy_host
MUXMIN[friendly/cache-references]=MUX_UNREADABLE
evaluate cache-references
check "UNREADABLE enabled% on the friendly arm: also rejected" \
      "UNRELIABLE_MUX_UNREADABLE" "${EV_VERDICT[cache-references]}"

# The accumulator that produces that token, tested on the committed source text
# rather than a transcription (same reasoning as the finding-4 metadata case below).
expect_rc "MUXMIN accumulator no longer defaults an unreadable percentage to 10000" 0 \
  python3 - "$HERE/positive-control.sh" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
if 'mmin=10000' not in src:
    sys.exit('FAIL: the accumulator no longer exists in a recognisable form; '
             'update this case rather than deleting it')
blk = src[src.index('vals=(); bad=""; mmin=10000'):src.index('MUXMIN["$arm/$ev"]=$mmin')]
if 'mux_unreadable=1' not in blk:
    sys.exit('FAIL: an unparseable enabled%% no longer sets mux_unreadable, so it '
             'falls through with mmin=10000 and reads as 10,000%% enabled')
if 'MUX_UNREADABLE' not in src:
    sys.exit('FAIL: no MUX_UNREADABLE token is ever recorded')
print('        accumulator marks an unreadable percentage rather than defaulting it')
PY

# Round 2 finding #7: --quick is documented as not a valid gate result, so it must
# not return the gate's PASS exit contract.
#
# THIS CASE WAS FIRST WRITTEN AS A SOURCE-TEXT MATCH AND THE MUTATION RUN CAUGHT IT.
# Checking that the string `QUICK_MECHANICS` appears, and appears before `RESULT=PASS`,
# passed happily against a mutant whose branch condition had been changed to
# `elif false` — the text was all still there, and the branch was unreachable. A
# presence test standing in for a behaviour test is the very shape these seven
# findings are made of, so it is replaced by EXECUTING the committed decision block
# with injected state and asserting the RESULT and RC it produces.
quick_verdict() { # $1 QUICK  $2 FAILED_REQUIRED -> "<RESULT>/<RC>"
  python3 - "$HERE/positive-control.sh" "$1" "$2" <<'PY'
import subprocess, sys
src = open(sys.argv[1]).read()
start = src.index('if [ "$HOSTILITY" != PASS ] || [ "$SYMMETRY" != PASS ]; then')
end = src.index('say "==== RESULT: $RESULT ===="')
block = src[start:end]
prelude = (
    'HOSTILITY=PASS\nSYMMETRY=PASS\n'
    'QUICK=%s\nFAILED_REQUIRED=%s\n'
    'REPS=1\nACCESSES=2000000\nBUFFER_MIB=512\n'
    'REQUIRED_EVENTS=(LLC-loads LLC-load-misses cache-references)\n'
) % (sys.argv[2], sys.argv[3])
script = prelude + block + '\nprintf "%s/%s\\n" "$RESULT" "$RC"\n'
r = subprocess.run(['bash', '-c', script], capture_output=True, text=True)
if r.returncode != 0:
    sys.stderr.write(r.stderr)
    sys.exit('the extracted decision block did not run')
print(r.stdout.strip().splitlines()[-1])
PY
}
check "a REAL run with every counter usable: PASS / exit 0" \
      "PASS/0" "$(quick_verdict 0 0)"
check "--quick with every counter usable: NOT the PASS contract" \
      "QUICK_MECHANICS/3" "$(quick_verdict 1 0)"
check "--quick with a failing counter: also non-zero, not FAIL's contract" \
      "QUICK_MECHANICS/3" "$(quick_verdict 1 2)"
check "a REAL run with a failing counter: FAIL / exit 1" \
      "FAIL/1" "$(quick_verdict 0 2)"

# =============================================================================
section "FINDING 2 — penalty-probe window gating (run/penalty-window-check.py)"
# THE BAD INPUT IS THE REAL ONE. These are the CSVs committed in this PR, written
# by the unfenced perf invocation, so this case is not a simulation of the defect
# — it is the defect's own output being refused.
expect_rc "committed (contaminated) penalty/ CSVs: REJECTED" 1 \
  python3 "$HERE/run/penalty-window-check.py" "$HERE/penalty" 20000000

# A clean sweep: the chase-only instruction count, identical in every row, which
# is what the FIFO-gated probe now produces.
mk_penalty_row() { # $1 dir  $2 label  $3 instructions
  printf '# started on Tue Aug  4 03:36:00 2026\n\n' > "$1/perf-$2.csv"
  printf '%s,,cycles:u,3211333656,100.00,,\n' "$(( $3 * 6 ))" >> "$1/perf-$2.csv"
  printf '%s,,instructions:u,3211333656,100.00,0.06,insn per cycle\n' "$3" >> "$1/perf-$2.csv"
}
mkdir -p "$TMP/clean"
for lbl in L1d_32K L2_512K LLC_8M LLC_32M DRAM_256M DRAM_1G DRAM_2G; do
  mk_penalty_row "$TMP/clean" "$lbl" 120174195      # 6.009 instr/access, every row
done
expect_rc "FIFO-gated sweep (6.009 instr/access, uniform): ACCEPTED" 0 \
  python3 "$HERE/run/penalty-window-check.py" "$TMP/clean" 20000000

# The case that shows why the absolute ceiling is not redundant with the
# uniformity check: inflate EVERY row equally. The uniformity check derives its
# reference from this same data, so it sees nothing wrong — a vacuous pass — and
# only the externally-anchored ceiling can refuse it.
mkdir -p "$TMP/uniform-bad"
for lbl in L1d_32K L2_512K LLC_8M LLC_32M DRAM_256M DRAM_1G DRAM_2G; do
  mk_penalty_row "$TMP/uniform-bad" "$lbl" 600000000   # 30.0 instr/access, uniform
done
expect_rc "UNIFORMLY inflated sweep (30 instr/access, no cross-row signal): REJECTED by the ceiling" 1 \
  python3 "$HERE/run/penalty-window-check.py" "$TMP/uniform-bad" 20000000

# ...and why the uniformity check is not redundant with the ceiling: one row
# contaminated by only +3.2%, which is the real LLC_8M row. Under the ceiling
# alone this passes.
mkdir -p "$TMP/subtle-bad"
for lbl in L1d_32K L2_512K LLC_32M DRAM_256M DRAM_1G DRAM_2G; do
  mk_penalty_row "$TMP/subtle-bad" "$lbl" 120174195
done
mk_penalty_row "$TMP/subtle-bad" LLC_8M 123963910     # 6.198 = +3.2%, under the 8.0 ceiling
expect_rc "ONE row +3.2% (real LLC_8M value, under the ceiling): REJECTED by cross-row uniformity" 1 \
  python3 "$HERE/run/penalty-window-check.py" "$TMP/subtle-bad" 20000000

# No subject, no verdict — never a 0/0 pass.
mkdir -p "$TMP/empty-penalty"
expect_rc "empty penalty dir: REFUSED rather than passed vacuously" 1 \
  python3 "$HERE/run/penalty-window-check.py" "$TMP/empty-penalty" 20000000

# Round 2 finding #4: the probe's own row parser. The window check validates only
# `instructions`, so a missing or multiplexed cycles/LLC/dTLB counter had nothing
# looking at it — `vals.get()` turned it into nan or a confident 0.0000/access.
# Tested against the committed source text, not a copy.
run_row_parser() { # $1 csv -> exit code of the extracted parser
  python3 - "$HERE/run/penalty-probe.sh" "$1" <<'PY'
import subprocess, sys
src = open(sys.argv[1]).read()
start = src.index("import math, sys\ncsv,label,size,acc,ghz,buf")
end = src.index("\nPY\n", start)
snippet = src[start:end]
r = subprocess.run([sys.executable, '-c', snippet,
                    sys.argv[2], 'TESTROW', '8192', '20000000', '2.90', '528'],
                   capture_output=True, text=True)
sys.stderr.write(r.stderr)
sys.stdout.write(r.stdout)
sys.exit(r.returncode)
PY
}
mk_full_row() { # $1 out-csv  $2 enabled-pct-for-cycles  $3 cycles-token
  { printf '# started\n\n'
    printf '%s,,cycles:u,3211333656,%s,,\n' "$3" "$2"
    printf '120174195,,instructions:u,3211333656,100.00,0.06,insn per cycle\n'
    printf '20185457,,LLC-loads,3211333656,100.00,,\n'
    printf '125898,,LLC-load-misses,3211333656,100.00,0.62,of all LL-cache accesses\n'
    printf '308542,,dTLB-load-misses,3211333656,100.00,,\n'; } > "$1"
}
mk_full_row "$TMP/row-good.csv" 100.00 1808801914
expect_rc "row parser: all five counters present, unmultiplexed: ACCEPTED" 0 \
  run_row_parser "$TMP/row-good.csv"
mk_full_row "$TMP/row-notcounted.csv" 100.00 '<not counted>'
expect_rc "row parser: cycles reads <not counted>: REJECTED (was published as nan)" 1 \
  run_row_parser "$TMP/row-notcounted.csv"
mk_full_row "$TMP/row-mux.csv" 43.00 1808801914
expect_rc "row parser: cycles at 43% enabled: REJECTED (was never checked at all)" 1 \
  run_row_parser "$TMP/row-mux.csv"
mk_full_row "$TMP/row-zero.csv" 100.00 0
expect_rc "row parser: cycles reads 0 for 20M accesses: REJECTED as a failed capture" 1 \
  run_row_parser "$TMP/row-zero.csv"
# A missing LLC counter used to print a confident 0.0000 per access, which erases the
# very distinction the #3217 silent-instrument lesson turns on: a real 0 is a
# finding, an absent counter is a failure, and `(x or 0)` maps both to the same cell.
grep -v 'LLC-load-misses' "$TMP/row-good.csv" > "$TMP/row-nollc.csv"
expect_rc "row parser: LLC-load-misses ABSENT: REJECTED (was printed as 0.0000/access)" 1 \
  run_row_parser "$TMP/row-nollc.csv"

# =============================================================================
section "FINDING 4 — capture-endpoint rc roster (harness/guards.sh)"
# shellcheck source=harness/guards.sh
source "$HERE/harness/guards.sh"

ROSTER_OK=(alignedA=0 alignedB=0 interiorA=0 loadgenInterior=0 uncore=0 loadgenUncore=0 meta=0)
expect_rc "all seven arms zero: accepted" 0 ws0_guard_all_rc_zero "${ROSTER_OK[@]}"

# The two arms the old expression omitted, each on its own. Before the fix BOTH of
# these returned overall success.
expect_rc "loadgenInterior=1 (RC_LG_A, omitted from the old expression): REJECTED" 1 \
  ws0_guard_all_rc_zero alignedA=0 alignedB=0 interiorA=0 loadgenInterior=1 uncore=0 loadgenUncore=0 meta=0
expect_rc "loadgenUncore=1 (RC_LG_C, omitted from the old expression): REJECTED" 1 \
  ws0_guard_all_rc_zero alignedA=0 alignedB=0 interiorA=0 loadgenInterior=0 uncore=0 loadgenUncore=1 meta=0
expect_rc "an arm whose rc is unrecordable (empty): REJECTED, not read as zero" 1 \
  ws0_guard_all_rc_zero alignedA=0 loadgenUncore=
expect_rc "no arms at all: REFUSED (a validity check with no subject has no verdict)" 1 \
  ws0_guard_all_rc_zero

# The roster must NAME the two arms, because the whole defect was a claim of
# coverage that the expression did not honour.
ROSTER_TEXT="$(ws0_guard_all_rc_zero "${ROSTER_OK[@]}" 2>&1)"
case "$ROSTER_TEXT" in
  *loadgenInterior*loadgenUncore*) ok "guard prints its roster including both load-generator arms" ;;
  *) bad "guard roster does not name both load-generator arms: $ROSTER_TEXT" ;;
esac

expect_rc "ws0_guard_rc with rc=0: accepted"  0 ws0_guard_rc "a step" 0
expect_rc "ws0_guard_rc with rc=4: REJECTED"  1 ws0_guard_rc "a step" 4 "context"

# Finding 4's SECOND half: the rc gate inside capture-endpoint.sh's meta.json
# validity block. That block lives in an UNQUOTED heredoc interpolating ~30 shell
# variables, so it cannot be invoked directly — but it can be tested without
# copying it, by extracting the committed source TEXT and exec'ing it against
# crafted `doc` values. Testing a transcription instead would prove only that the
# transcription works, which is the mistake CLAUDE.md records about porting.
expect_rc "meta.json rc gate (extracted from committed capture-endpoint.sh): rejects a nonzero arm" 0 \
  python3 - "$HERE/run/capture-endpoint.sh" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
start = src.index('_rc = doc.get("rc")')
end = src.index('if not doc["warm_verified_zero_disk_reads"]:')
snippet = src[start:end]
if 'loadgen' in snippet:
    sys.exit('FAIL: the snippet hardcodes arm names; it must enumerate the dict')

def verdict(rc):
    ns = {'doc': {'rc': rc}, 'bad': []}
    exec(compile(snippet, '<capture-endpoint-rc-gate>', 'exec'), ns)
    return ns['bad']

cases = [
    ('all six zero',                dict(core_interior=0, loadgen_interior=0, alignedA=0,
                                        alignedB=0, uncore=0, loadgen_uncore=0), False),
    ('loadgen_interior=1 (RC_LG_A)', dict(core_interior=0, loadgen_interior=1, alignedA=0,
                                        alignedB=0, uncore=0, loadgen_uncore=0), True),
    ('loadgen_uncore=1 (RC_LG_C)',  dict(core_interior=0, loadgen_interior=0, alignedA=0,
                                        alignedB=0, uncore=0, loadgen_uncore=1), True),
    ('uncore=2',                    dict(core_interior=0, loadgen_interior=0, alignedA=0,
                                        alignedB=0, uncore=2, loadgen_uncore=0), True),
    ('rc block empty',              {}, True),
]
bad = 0
for name, rc, want_reject in cases:
    got = bool(verdict(rc))
    flag = 'ok' if got == want_reject else 'MISMATCH'
    if got != want_reject:
        bad += 1
    print('        %-30s rejected=%-5s want=%-5s %s' % (name, got, want_reject, flag))
# A future arm added to the rc dict must not default to unchecked.
if not verdict(dict(core_interior=0, loadgen_interior=0, alignedA=0, alignedB=0,
                    uncore=0, loadgen_uncore=0, some_future_arm=1)):
    print('        %-30s rejected=False want=True  MISMATCH' % 'a NEW arm, nonzero')
    bad += 1
else:
    print('        %-30s rejected=True  want=True  ok' % 'a NEW arm, nonzero')
sys.exit(1 if bad else 0)
PY

# =============================================================================
section "FINDING 5a — resume predicate (run/rep-complete.py)"
# The good input is a rep committed in this PR, copied so the crafted variants
# cannot touch the artefacts.
GOOD_REP="$HERE/results/llc-s6-N16/rep1"
cp -r "$GOOD_REP" "$TMP/rep-good"
expect_rc "committed rep llc-s6-N16/rep1: certified complete (safe to skip)" 0 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-good"

# A failed load generator. This is the exact shape that was skipped permanently.
cp -r "$GOOD_REP" "$TMP/rep-lgfail"
python3 - "$TMP/rep-lgfail/meta.json" <<'PY'
import json,sys
p=sys.argv[1]; d=json.load(open(p)); d['rc']['loadgen_uncore']=1
json.dump(d,open(p,'w'),indent=1)
PY
expect_rc "rep with rc.loadgen_uncore=1: REFUSED (old predicate skipped it as complete)" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-lgfail"

# An emptied counter file — the input that made derive.py publish 0 GB/s.
cp -r "$GOOD_REP" "$TMP/rep-emptycsv"
: > "$TMP/rep-emptycsv/perf-uncore.csv"
expect_rc "rep with an EMPTY perf-uncore.csv: REFUSED" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-emptycsv"

cp -r "$GOOD_REP" "$TMP/rep-nocsv"
rm -f "$TMP/rep-nocsv/perf-uncore.csv"
expect_rc "rep with perf-uncore.csv ABSENT: REFUSED" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-nocsv"

# A multiplexed counter file: structurally present, but the counts are estimates.
cp -r "$GOOD_REP" "$TMP/rep-mux"
python3 - "$TMP/rep-mux/perf-uncore.csv" <<'PY'
import sys
p=sys.argv[1]; out=[]
done=False
for line in open(p):
    f=line.rstrip('\n').split(',')
    if not done and len(f)>=7 and f[0].startswith('S'):
        f[6]='42.00'; done=True
        line=','.join(f)+'\n'
    out.append(line)
open(p,'w').writelines(out)
PY
expect_rc "rep with one counter row at 42% enabled: REFUSED (a scaled estimate is not a count)" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-mux"

expect_rc "rep dir with no meta.json: REFUSED" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/does-not-exist"

# Round 2 finding #6: "nonempty dict" and "at least one row" are existence tests
# standing in for completeness tests. Both shapes now fail.
cp -r "$GOOD_REP" "$TMP/rep-partialrc"
python3 - "$TMP/rep-partialrc/meta.json" <<'PY'
import json,sys
p=sys.argv[1]; d=json.load(open(p))
d['rc']={'alignedA':0,'alignedB':0}          # nonempty, but 2 of 6 arms
json.dump(d,open(p,'w'),indent=1)
PY
expect_rc "rep with a PARTIAL rc block (2 of 6 arms, all zero): REFUSED" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-partialrc"

cp -r "$GOOD_REP" "$TMP/rep-partialfiles"
python3 - "$TMP/rep-partialfiles/meta.json" <<'PY'
import json,sys
p=sys.argv[1]; d=json.load(open(p))
d['perf_files']={'alignedA':'perf-coreA-aligned.csv'}   # nonempty, 1 of 4
json.dump(d,open(p,'w'),indent=1)
PY
expect_rc "rep with a PARTIAL perf_files roster (1 of 4): REFUSED" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-partialfiles"

cp -r "$GOOD_REP" "$TMP/rep-trunccsv"
head -4 "$TMP/rep-trunccsv/perf-coreA-aligned.csv" > "$TMP/t.csv"
mv "$TMP/t.csv" "$TMP/rep-trunccsv/perf-coreA-aligned.csv"
expect_rc "rep with a TRUNCATED CSV (2 of 7 events, all readable): REFUSED" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-trunccsv"

cp -r "$GOOD_REP" "$TMP/rep-badmux"
python3 - "$TMP/rep-badmux/perf-coreA-aligned.csv" <<'PY'
import sys
p=sys.argv[1]; out=[]
for line in open(p):
    f=line.rstrip('\n').split(',')
    if len(f)>=5 and f[2].startswith('cycles'):
        f[4]='not-a-number'; line=','.join(f)+'\n'
    out.append(line)
open(p,'w').writelines(out)
PY
expect_rc "rep with an UNREADABLE enabled% in a CSV: REFUSED (not read as healthy)" 1 \
  python3 "$HERE/run/rep-complete.py" "$TMP/rep-badmux"

# =============================================================================
section "FINDING 5b — derivation must not publish 0 GB/s (results/derive.py)"
# The good input: the whole committed results tree. This also re-proves that the
# new fail-closed IMC checks do not disturb the published figures.
cp -r "$HERE/results" "$TMP/results-good"
expect_rc "committed results tree: derives cleanly" 0 \
  python3 "$HERE/results/derive.py" "$TMP/results-good" --out "$TMP/derived-good.json"
# The headline is re-derived, not trusted: if a guard had perturbed any input the
# accounting reads, this is where it would show. 3821.3 cycles/row attributed and
# 32.24% residual are report 5.3's published figures.
SPLIT="$(python3 - "$TMP/derived-good.json" <<'PY'
import json,sys
a=json.load(open(sys.argv[1]))['ac4_accounting']
print('%.1f/%.2f' % (a['attributed_cycles_per_row'], a['residual_pct_of_delta']))
PY
)"
check "re-derived headline attribution is unchanged by the guards" "3821.3/32.24" "$SPLIT"

cp -r "$HERE/results" "$TMP/results-emptyimc"
: > "$TMP/results-emptyimc/llc-s6-N16/rep1/perf-uncore.csv"
expect_rc "results tree with an EMPTY perf-uncore.csv: REFUSED (used to derive 0 GB/s)" 1 \
  python3 "$HERE/results/derive.py" "$TMP/results-emptyimc" --out "$TMP/derived-bad.json"

cp -r "$HERE/results" "$TMP/results-noimc"
rm -f "$TMP/results-noimc/llc-s6-N16/rep1/perf-uncore.csv"
expect_rc "results tree with perf-uncore.csv ABSENT: REFUSED" 1 \
  python3 "$HERE/results/derive.py" "$TMP/results-noimc" --out "$TMP/derived-bad2.json"

# A PARTIAL IMC set: structurally valid rows, just not all the channels. This is
# the shape that would have understated a socket total and shifted the far-socket
# ratio without ever looking wrong.
cp -r "$HERE/results" "$TMP/results-partialimc"
python3 - "$TMP/results-partialimc/llc-s6-N16/rep1/perf-uncore.csv" <<'PY'
import sys
p=sys.argv[1]; keep=[]; dropped=0
for line in open(p):
    if line.startswith('S0,') and 'uncore_imc_11/' in line and dropped < 2:
        dropped += 1; continue
    keep.append(line)
open(p,'w').writelines(keep)
print('dropped %d S0 imc_11 rows' % dropped)
PY
expect_rc "results tree with a PARTIAL IMC set (2 rows dropped): REFUSED" 1 \
  python3 "$HERE/results/derive.py" "$TMP/results-partialimc" --out "$TMP/derived-bad3.json"

# Round 2 finding #1: the derivation re-checks occupancy, warmth and saturation but
# never read the rc blocks it was handed. A dead load generator leaves perf's own rc
# at 0, so the CSV parses cleanly and only rc records that the row counts every
# per-row figure divides by are wrong.
cp -r "$HERE/results" "$TMP/results-rcfail"
python3 - "$TMP/results-rcfail/llc-s6-N16/rep1/meta.json" <<'PY'
import json,sys
p=sys.argv[1]; d=json.load(open(p)); d['rc']['loadgen_interior']=1
json.dump(d,open(p,'w'),indent=1)
PY
expect_rc "results tree with rc.loadgen_interior=1: REFUSED by the derivation" 1 \
  python3 "$HERE/results/derive.py" "$TMP/results-rcfail" --out "$TMP/derived-bad4.json"

cp -r "$HERE/results" "$TMP/results-rcstalls"
python3 - "$TMP/results-rcstalls/llc-s6-N16/rep2/meta-stalls.json" <<'PY'
import json,sys
p=sys.argv[1]; d=json.load(open(p)); d['rc']['alignedC']=1
json.dump(d,open(p,'w'),indent=1)
PY
expect_rc "results tree with a failed group-C arm (rc.alignedC=1): REFUSED" 1 \
  python3 "$HERE/results/derive.py" "$TMP/results-rcstalls" --out "$TMP/derived-bad5.json"

# Round 2 finding #2: group C is all-or-nothing. A partial set used to derive the
# HEADLINE measured attribution from a silently reduced sample — #3217's undispersed
# -reps method gap, reintroduced in the one term this report leads with.
cp -r "$HERE/results" "$TMP/results-partialC"
rm -f "$TMP/results-partialC/llc-s6-N16/rep2/meta-stalls.json" \
      "$TMP/results-partialC/llc-s6-N16/rep2/perf-coreC-aligned.csv" \
      "$TMP/results-partialC/llc-s6-N16/rep3/meta-stalls.json" \
      "$TMP/results-partialC/llc-s6-N16/rep3/perf-coreC-aligned.csv"
expect_rc "results tree with group C for 1 of 3 reps: REFUSED (was 1 undispersed rep)" 1 \
  python3 "$HERE/results/derive.py" "$TMP/results-partialC" --out "$TMP/derived-bad6.json"

# ...but a tree that never captured group C at all is a DIFFERENT situation and must
# still derive, falling back to the modelled charge and saying so. A guard that
# rejects this too would have broken a supported mode.
cp -r "$HERE/results" "$TMP/results-noC"
rm -f "$TMP/results-noC"/*/rep*/meta-stalls.json \
      "$TMP/results-noC"/*/rep*/perf-coreC-aligned.csv
expect_rc "results tree with NO group C anywhere: still derives (supported mode)" 0 \
  python3 "$HERE/results/derive.py" "$TMP/results-noC" --out "$TMP/derived-noC.json"

# =============================================================================
section "FINDING 6 — AC5 byte accounting (run/ac5-analyse.py)"
expect_rc "committed ac5-run (ratio 1.008): RESOLVED, accepted" 0 \
  python3 "$HERE/run/ac5-analyse.py" "$HERE/ac5-run/perf-uncore-triad.csv" "$HERE/ac5-run/stream.txt"

# INDETERMINATE: `elements` divided by 3 puts the ratio at ~3.02, matching neither
# the ~1x nor the ~8x hypothesis. Before the fix this printed "Do NOT publish a
# bandwidth figure" and exited 0.
mkdir -p "$TMP/ac5-indet"
sed 's/^elements=536870912$/elements=178956970/' "$HERE/ac5-run/stream.txt" \
  > "$TMP/ac5-indet/stream.txt"
expect_rc "INDETERMINATE ratio (~3x, matches neither hypothesis): REJECTED" 1 \
  python3 "$HERE/run/ac5-analyse.py" "$HERE/ac5-run/perf-uncore-triad.csv" "$TMP/ac5-indet/stream.txt"

# UNAVAILABLE: the byte accounting cannot even be attempted. Also exited 0 before.
mkdir -p "$TMP/ac5-unavail"
grep -v '^elements=' "$HERE/ac5-run/stream.txt" > "$TMP/ac5-unavail/stream.txt"
expect_rc "byte accounting UNAVAILABLE (no 'elements' key): REJECTED" 1 \
  python3 "$HERE/run/ac5-analyse.py" "$HERE/ac5-run/perf-uncore-triad.csv" "$TMP/ac5-unavail/stream.txt"

# Round 2 finding #3: the ratio has a broad 0.6-1.6 acceptance band, so it cannot
# police its own inputs — and the four near-zero S1 rows can vanish without moving it
# at all. The roster must therefore be asserted independently of the ratio.
mkdir -p "$TMP/ac5-trunc"
head -20 "$HERE/ac5-run/perf-uncore-triad.csv" > "$TMP/ac5-trunc/triad.csv"
expect_rc "TRUNCATED IMC capture (18 of 24 instances on S0): REJECTED on the roster" 1 \
  python3 "$HERE/run/ac5-analyse.py" "$TMP/ac5-trunc/triad.csv" "$HERE/ac5-run/stream.txt"

# The case the finding singles out, and the reason the roster check cannot be replaced
# by a tighter ratio band: drop only the near-zero S1 rows. MEASURED on the committed
# capture, dropping 4 of 48 instances moves the ratio from 1.0080 to 1.0077 — three
# ten-thousandths, versus an acceptance band 1.0 wide. No band that admits a healthy
# capture could ever exclude this one, so the ratio is structurally incapable of
# detecting it and the old code exited 0 on a capture missing a twelfth of its
# instances.
mkdir -p "$TMP/ac5-drops1"
grep -v '^S1,.*uncore_imc_1[01]/' "$HERE/ac5-run/perf-uncore-triad.csv" \
  > "$TMP/ac5-drops1/triad.csv"
expect_rc "IMC capture missing only the NEAR-ZERO S1 rows (ratio unmoved): REJECTED" 1 \
  python3 "$HERE/run/ac5-analyse.py" "$TMP/ac5-drops1/triad.csv" "$HERE/ac5-run/stream.txt"

# A duplicated row would have been silently overwritten by dict assignment.
mkdir -p "$TMP/ac5-dup"
{ cat "$HERE/ac5-run/perf-uncore-triad.csv"
  grep -m1 '^S0,.*uncore_imc_0/cas_count_read/' "$HERE/ac5-run/perf-uncore-triad.csv"
} > "$TMP/ac5-dup/triad.csv"
expect_rc "DUPLICATE IMC row: REJECTED (was silently overwritten)" 1 \
  python3 "$HERE/run/ac5-analyse.py" "$TMP/ac5-dup/triad.csv" "$HERE/ac5-run/stream.txt"

# An unreadable enabled% — the same three-state trap as round 2 finding #5, in a
# third file. Checked here because "not below the floor" was the test, and an
# unreadable percentage is not below anything.
mkdir -p "$TMP/ac5-badmux"
python3 - "$HERE/ac5-run/perf-uncore-triad.csv" "$TMP/ac5-badmux/triad.csv" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
out=[]; done=False
for line in open(src):
    f=line.rstrip('\n').split(',')
    if not done and len(f)>=7 and f[0].startswith('S') and 'cas_count' in f[4]:
        f[6]='n/a'; line=','.join(f)+'\n'; done=True
    out.append(line)
open(dst,'w').writelines(out)
PY
expect_rc "IMC row with an UNREADABLE enabled%: REJECTED (not read as healthy)" 1 \
  python3 "$HERE/run/ac5-analyse.py" "$TMP/ac5-badmux/triad.csv" "$HERE/ac5-run/stream.txt"

# =============================================================================
printf '\n==== SELFTEST RESULT: %s ====\n' \
  "$( [ "$FAIL" -eq 0 ] && echo PASS || echo FAIL )"
printf 'cases passed: %d   failed: %d\n' "$PASS" "$FAIL"
if [ "$FAIL" -ne 0 ]; then
  echo "A guard is not behaving as its finding requires. Do not publish figures"
  echo "from this harness until every case passes."
  exit 1
fi
echo "Every guard rejected the bad input it exists to catch, and accepted the"
echo "good input committed in this PR. The healthy-host record is pinned, so"
echo "re-inverting the finding-1 ordering fails here rather than on a good box."
exit 0

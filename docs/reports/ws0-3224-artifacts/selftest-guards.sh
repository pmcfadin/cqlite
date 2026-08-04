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

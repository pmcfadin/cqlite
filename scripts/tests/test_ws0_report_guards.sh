#!/usr/bin/env bash
# Self-test for the issue-#3096 measurement rig's fail-closed guards
# (scripts/perf/ws0-baseline.sh + scripts/perf/ws0_report.py).
#
# These are INSTRUMENT guards. A broken one does not crash anything — it publishes
# a number that is not the number it claims to be, which is the most expensive
# failure mode a measurement rig has. Both properties below were real defects found
# in review:
#
#   1. WARM MEANS PREWARMED (roborev finding 1). The Flight arm prewarmed before its
#      perf window; the bare-scan arm did not, so on a cold page cache the first
#      "warm" bare-scan rep was measured partly cold — and the bare scan is the
#      DENOMINATOR of the 1.3x ratio, where reading slow makes the target easier.
#      The driver now runs an untimed prewarm and records `prewarm_status`; the
#      reporter must carry it into results.json for BOTH arms and flag a degraded
#      or unrecorded one in the summary.
#   2. A COLD REP IS EXACTLY ONE REQUEST (roborev finding 2). The reporter accepted
#      ANY successful-request count for a cold rep, so if the corpus finished inside
#      --cold-step-duration, requests 2..N were warm and got blended into the figure
#      reported as "cold" — and a caller could trigger it directly by raising that
#      option. The reporter must now REJECT such a rep naming the observed count,
#      and the driver must refuse an over-long cold step up front.
#
# Hermetic: synthetic result dirs + synthetic perf CSVs. No cargo, no perf, no
# sudo, no corpus, no network, and the real perf artifacts are never touched.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
command -v python3 >/dev/null 2>&1 || {
  echo "SKIP - python3 not installed; the reporter guards need it (never a silent PASS)"
  exit 0
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

CORPUS_ROWS=1000

# --------------------------------------------------------------------------
# Fixture builders — the minimum a session dir needs for the reporter to run
# --------------------------------------------------------------------------
perf_csv() { # perf_csv <path> <cycles> <instructions>
  printf '%s,,cycles,,,,\n%s,,instructions,,,,\n' "$2" "$3" > "$1"
}

make_corpus() { # make_corpus <dir>
  mkdir -p "$1"
  cat > "$1/corpus-identity.json" <<EOF
{ "rows": $CORPUS_ROWS, "partitions": 10, "seed": 1, "cells_per_row": 12,
  "data_db_bytes": 700000, "data_db_sha256": "deadbeef", "bytes_per_row": 700.0 }
EOF
}

# make_scan_rep <dir> <temp> <rep> <prewarm-status|-none->
make_scan_rep() {
  local d="$1" temp="$2" rep="$3" pw="$4" tag="scan-$2-$3"
  cat > "$d/$tag.json" <<EOF
{ "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5 }
EOF
  perf_csv "$d/perf-$tag.csv" 2000000 4000000
  perf_csv "$d/perf-$tag-setup.csv" 100000 200000
  [ "$pw" = "-none-" ] || printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
}

# make_flight_rep <dir> <temp> <rep> <requests_ok> <rows> <prewarm-status|-none->
make_flight_rep() {
  local d="$1" temp="$2" rep="$3" ok="$4" rows="$5" pw="$6" tag="flight-bypass-$2-$3"
  cat > "$d/$tag.jsonl" <<EOF
{"round":"$tag","requests_ok":$ok,"requests_error":0,"rows_total":$rows,"rows_per_s":250000.0,"duration_s":4.0}
EOF
  perf_csv "$d/perf-$tag.csv" 8000000 16000000
  [ "$pw" = "-none-" ] || printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
}

# run_report <dir> <corpus> <temps> — prints the reporter's stdout+stderr. Call as
# `out=$(run_report ...); rc=$?`: a command substitution runs in a SUBSHELL, so a
# status the function assigned to a variable would not survive the call.
run_report() {
  python3 "$REPORT" --dir "$1" --corpus "$2" --server-cpus 2,10 \
    --client-cpus 4,12 --reps 1 --temps "$3" --arms bypass \
    --step-duration 45s/1s --scan-passes 1 2>&1
}

# --------------------------------------------------------------------------
# Finding 2 — a COLD flight rep with >1 successful request is REJECTED
# --------------------------------------------------------------------------
# The blend this rejects is real and silent: 3 requests over a cold cache is one
# cold scan plus two warm ones, and the pre-fix reporter published their average as
# the "cold" figure with nothing in the output to say so.
d="$TMP/cold-multi"; make_corpus "$TMP/corpus"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 3 $((CORPUS_ROWS * 3)) skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
if [ "$rc" -ne 0 ] && grep -q "COLD rep flight-bypass-cold-1 completed 3 successful requests" <<<"$out"; then
  pass "a cold flight rep with 3 successful requests is REFUSED, naming the count"
else
  fail "cold multi-request rep: expected non-zero + observed-count message (rc=$rc, out: $out)"
fi
if grep -q "requests_ok" <<<"$out" || grep -qi "expected exactly 1" <<<"$out"; then
  pass "the refusal states the expected count"
else
  fail "the refusal must state the expected count (out: $out)"
fi

# The same shape as a WARM rep is legitimate: 3 requests, 3x the corpus rows.
d="$TMP/warm-multi"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 3 $((CORPUS_ROWS * 3)) ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "the same 3-request rep is ACCEPTED as WARM (the guard is temperature-scoped, not blanket)"
else
  fail "warm 3-request rep must be accepted (rc=$rc, out: $out)"
fi

# A cold rep with exactly one full-corpus request is accepted.
d="$TMP/cold-one"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 1 "$CORPUS_ROWS" skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "a cold rep with exactly 1 full-corpus request is accepted"
else
  fail "cold single-request rep must be accepted (rc=$rc, out: $out)"
fi

# A request that did not scan the WHOLE corpus is rejected: the per-request row
# denominator would not be the one the report prints.
d="$TMP/partial"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 1 $((CORPUS_ROWS - 7)) skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
if [ "$rc" -ne 0 ] && grep -q "did not scan the whole corpus" <<<"$out"; then
  pass "a partial-corpus request is REFUSED (row denominator would be unstated)"
else
  fail "partial-corpus rep: expected non-zero + 'did not scan the whole corpus' (rc=$rc, out: $out)"
fi

# --------------------------------------------------------------------------
# Finding 2 — the driver refuses an over-long --cold-step-duration up front
# --------------------------------------------------------------------------
# Exits at argument validation, before any build/corpus/cache-drop, so this runs
# with no corpus and no sudo.
check_driver_reject() { # check_driver_reject <label> <expect-substring> <args...>
  local label="$1" expect="$2"; shift 2
  local out rc2
  out=$(bash "$DRIVER" "$@" 2>&1); rc2=$?
  if [ "$rc2" -ne 0 ] && grep -q "$expect" <<<"$out"; then
    pass "$label"
  else
    fail "$label: expected non-zero + '$expect' (rc=$rc2, out: $out)"
  fi
}
check_driver_reject "a 45s cold step is refused up front (would admit warm requests)" \
  "exceeds the" --corpus "$TMP/corpus" --temp cold --cold-step-duration 45s
check_driver_reject "a 10s cold step is refused (above the 5000ms ceiling)" \
  "exceeds the" --corpus "$TMP/corpus" --temp both --cold-step-duration 10s
check_driver_reject "a bare number is refused rather than guessed as s-or-ms" \
  "must be <n>ms, <n>s or <n>m" --corpus "$TMP/corpus" --temp cold --cold-step-duration 45
check_driver_reject "a zero-length step is refused" \
  "greater than zero" --corpus "$TMP/corpus" --temp cold --cold-step-duration 0s

# A long step is fine when NO cold temperature is selected — the guard is scoped to
# the claim it protects, not a blanket restriction.
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp warm --cold-step-duration 45s 2>&1)
if grep -q "exceeds the" <<<"$out"; then
  fail "--temp warm must not be blocked by the cold-step ceiling (out: $out)"
else
  pass "--temp warm accepts a long cold-step value (the ceiling is cold-scoped)"
fi

# --------------------------------------------------------------------------
# Finding 1 — the bare-scan arm's prewarm is recorded, and a gap is flagged
# --------------------------------------------------------------------------
d="$TMP/prewarm-ok"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
ms = json.load(open(sys.argv[1]))["measurements"]
scan = [m for m in ms if m["arm"] == "bare_scan"]
assert scan, "no bare_scan measurement recorded"
s = scan[0]
assert s["prewarm"] == [{"rep": 1, "status": "ok"}], s["prewarm"]
assert s["prewarm_all_ok"] is True, s
assert s["reps"][0]["prewarm"] == "ok", s["reps"][0]
PY
then
  pass "the bare-scan arm's prewarm status is recorded per rep in results.json"
else
  fail "bare-scan prewarm not recorded in results.json (rc=$rc, out: $out)"
fi

# An UNRECORDED prewarm (a driver that predates the recording, or a rep that died
# before its prewarm) must be visible, not assumed healthy: it is exactly the
# pre-fix state, where a partly-cold rep was published as warm.
d="$TMP/prewarm-missing"; mkdir -p "$d"
make_scan_rep "$d" warm 1 -none-
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if [ "$rc" -eq 0 ] \
  && grep -q "PREWARM DEGRADED on bare-scan rep(s) 1=unrecorded" <<<"$out" \
  && grep -q "UNVERIFIED" <<<"$out"; then
  pass "an unrecorded bare-scan prewarm is FLAGGED in the summary as unverified"
else
  fail "unrecorded bare-scan prewarm must be flagged (rc=$rc, out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
s = [m for m in json.load(open(sys.argv[1]))["measurements"] if m["arm"] == "bare_scan"][0]
assert s["prewarm_all_ok"] is False, s
PY
then
  pass "results.json records prewarm_all_ok=false for the unrecorded case"
else
  fail "results.json must record prewarm_all_ok=false when a prewarm is unrecorded"
fi

# A FAILED bare-scan prewarm is likewise flagged (the driver fails closed before it
# gets here, but a hand-assembled or interrupted dir must not read as clean).
d="$TMP/prewarm-failed"; mkdir -p "$d"
make_scan_rep "$d" warm 1 FAILED-exit-1
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report "$d" "$TMP/corpus" warm); rc=$?
if grep -q "PREWARM DEGRADED on bare-scan rep(s) 1=FAILED-exit-1" <<<"$out"; then
  pass "a failed bare-scan prewarm is flagged"
else
  fail "failed bare-scan prewarm must be flagged (out: $out)"
fi

# The cold arm is deliberately NOT prewarmed, and that must read as healthy rather
# than as a degradation.
d="$TMP/prewarm-cold"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 1 "$CORPUS_ROWS" skipped-cold-arm
out=$(run_report "$d" "$TMP/corpus" cold); rc=$?
if [ "$rc" -eq 0 ] && ! grep -q "PREWARM DEGRADED" <<<"$out"; then
  pass "skipped-cold-arm is not reported as a degradation"
else
  fail "cold arm must not be flagged as prewarm-degraded (rc=$rc, out: $out)"
fi

# The driver must actually contain the untimed bare-scan prewarm — a reporter that
# merely READS a status file would pass every test above with no prewarm running.
if grep -q 'prewarm_status="skipped-cold-arm"' "$DRIVER" \
  && awk '/^measure_scan\(\)/,/^}/' "$DRIVER" | grep -q 'prewarm.status'; then
  pass "measure_scan itself records a prewarm status (not just the reporter)"
else
  fail "measure_scan must run and record its own prewarm"
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0-report guards: all checks passed"
  exit 0
fi
echo "ws0-report guards: $fails check(s) FAILED"
exit 1

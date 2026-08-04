#!/usr/bin/env bash
# Self-test for the WS0 measurement rig's fail-closed guards
# (scripts/perf/ws0-baseline.sh + scripts/perf/ws0_report.py + ws0_validate.py).
#
# These are INSTRUMENT guards. A broken one does not crash anything — it publishes
# a number that is not the number it claims to be, which is the most expensive
# failure mode a measurement rig has. Every property below was a real defect found
# in review (#3096 findings 1-2; #3272 findings 1-7):
#
#   1. WARM MEANS PREWARMED (#3096 finding 1). The Flight arm prewarmed before its
#      perf window; the bare-scan arm did not, so on a cold page cache the first
#      "warm" bare-scan rep was measured partly cold — and the bare scan is the
#      DENOMINATOR of the 1.3x ratio, where reading slow makes the target easier.
#      The driver now runs an untimed prewarm and records `prewarm_status`; the
#      reporter must carry it into results.json for BOTH arms and flag a degraded
#      or unrecorded one in the summary.
#   2. A COLD REP IS EXACTLY ONE REQUEST (#3096 finding 2). The reporter accepted
#      ANY successful-request count for a cold rep, so if the corpus finished inside
#      --cold-step-duration, requests 2..N were warm and got blended into the figure
#      reported as "cold" — and a caller could trigger it directly by raising that
#      option. The reporter must now REJECT such a rep naming the observed count,
#      and the driver must refuse an over-long cold step up front.
#
# And the #3272 round, THREE of which are the guards above being bypassable or
# fail-open — "a fix moved the problem", which is why each case below carries a
# NON-VACUITY note recording what the PRE-FIX code accepted:
#
#   3. AN ABSENT CORPUS IDENTITY MUST NOT SKIP THE FULL-CORPUS CHECK (#3272 f1).
#      `corpus_rows=None` disabled the `rows == requests_ok x corpus_rows` assert
#      while the NOTES kept claiming it ran.
#   4. THE COLD PREWARM SENTINEL IS COLD-ONLY (#3272 f2). `skipped-cold-arm` counted
#      as a healthy prewarm at ANY temperature, so an unprewarmed WARM rep reached
#      `prewarm_all_ok=true` — the guard of finding 1 satisfied by its own sentinel.
#   5. A COUNTER THAT WAS NOT OBSERVED IS AN ERROR (#3272 f4). `.get("cycles", 0)`
#      fabricated a zero, so a run reported "SETUP-SUBTRACTED" with no subtraction.
#   6. --reps AND FRIENDS ARE VALIDATED (#3272 f5). `--reps 0` produced a vacuous
#      but SUCCESSFUL report.
#   7. COMPLETENESS IS JUDGED AGAINST THE SELECTION (#3272 f6), and the selection is
#      stated in the report so a narrow run cannot be mistaken for a full matrix.
#   8. DURATIONS PARSE AS DECIMAL (#3272 f7). `010s` was octal 8s and `08s` was a
#      hard bash error — a silently wrong measurement window.
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
# A syntactically valid corpus digest (64 lowercase hex). The reporter requires the
# real shape because the digest is what identifies the bytes that were measured; a
# placeholder like `deadbeef` cannot.
FAKE_SHA="$(printf 'ab%.0s' $(seq 1 32))"

# --------------------------------------------------------------------------
# Fixture builders — the minimum a session dir needs for the reporter to run
# --------------------------------------------------------------------------
perf_csv() { # perf_csv <path> <cycles> <instructions>
  printf '%s,,cycles,,,,\n%s,,instructions,,,,\n' "$2" "$3" > "$1"
}

# make_corpus <dir> [rows] [data_db_bytes] [bytes_per_row] — a COMPLETE, internally
# consistent identity by default. Callers that need a broken one override the field.
make_corpus() {
  local dir="$1" rows="${2:-$CORPUS_ROWS}" bytes="${3:-700000}" bpr="${4:-700.0}"
  mkdir -p "$dir"
  cat > "$dir/corpus-identity.json" <<EOF
{ "rows": $rows, "partitions": 10, "seed": 1, "cells_per_row": 12,
  "data_db_bytes": $bytes, "data_db_sha256": "$FAKE_SHA", "bytes_per_row": $bpr }
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

# run_report_full <dir> <corpus> <temps> <arms> <reps> <scan-passes> — same, with
# every quantity a caller can get wrong exposed.
run_report_full() {
  python3 "$REPORT" --dir "$1" --corpus "$2" --server-cpus 2,10 \
    --client-cpus 4,12 --reps "$5" --temps "$3" --arms "$4" \
    --step-duration 45s/1s --scan-passes "$6" 2>&1
}

# expect_report_reject <label> <expect-substring> <report-args...> — the reporter
# must exit NON-ZERO and say <expect-substring>. Non-zero alone is not enough: a
# guard that fires with a diagnostic naming something else has not been observed.
expect_report_reject() {
  local label="$1" expect="$2"; shift 2
  local out rc3
  out=$(run_report_full "$@"); rc3=$?
  if [ "$rc3" -ne 0 ] && grep -q "$expect" <<<"$out"; then
    pass "$label"
  else
    fail "$label: expected non-zero + '$expect' (rc=$rc3, out: $out)"
  fi
}

# A COMPLETE single-warm-rep session dir, the happy path every negative case below
# perturbs by exactly one field. Built fresh each time so no case can pass on a
# neighbour's leftovers.
make_warm_session() { # make_warm_session <dir>
  mkdir -p "$1"
  make_scan_rep "$1" warm 1 ok
  make_flight_rep "$1" warm 1 1 "$CORPUS_ROWS" ok
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

# ==========================================================================
# #3272 finding 1 — an ABSENT/INCOMPLETE corpus identity is FATAL, never a
#                   silently-skipped full-corpus check
# ==========================================================================
# NON-VACUITY. The pre-fix reporter read:
#
#     identity = {}
#     if idp.exists(): identity = json.loads(idp.read_text())
#     corpus_rows = int(identity["rows"]) if identity.get("rows") else None
#
# and `check_request_count` guarded its whole-corpus assert with `if corpus_rows:`.
# So with NO corpus-identity.json the `rows == requests_ok x corpus_rows` check was
# SKIPPED — while the NOTES block kept printing "every rep's rows an exact multiple
# of the corpus row count". MEASURED against the pre-fix code (a4dbcfa2e state):
# a WARM rep claiming 3 successful requests over 993 rows — not a multiple of the
# 1000-row corpus, i.e. no request scanned the whole corpus — exited **0** and wrote
# a results.json with `full_corpus_per_request_verified: false` buried in it, having
# printed the claim that the check ran. That is the input this case now rejects.
d="$TMP/no-identity"; make_warm_session "$d"
mkdir -p "$TMP/corpus-empty"   # a corpus dir with NO corpus-identity.json
expect_report_reject "an absent corpus-identity.json is FATAL (never a skipped check)" \
  "no corpus identity at" "$d" "$TMP/corpus-empty" warm bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus-empty" warm bypass 1 1)
if grep -q "full-corpus-per-request" <<<"$out" \
  && grep -q "refused rather than skipped" <<<"$out"; then
  pass "the refusal names the property that could not be checked, and says so"
else
  fail "the identity refusal must name the unverifiable property (out: $out)"
fi
# ...and NOTHING was written: a report that cannot verify its own claim must not
# leave a results.json a later reader could quote.
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written when the corpus identity is absent"
else
  fail "a refused run must not leave a results.json behind"
fi

# The exact pre-fix-accepted input, now with the identity PRESENT: still rejected,
# by the whole-corpus assert the absent identity used to disable. This is the
# non-vacuity pair — same rep shape, the only difference being whether the check
# could run at all.
d="$TMP/no-identity-partial"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 3 993 ok
expect_report_reject "the rep the absent identity used to wave through is refused once it can be checked" \
  "did not scan the whole corpus" "$d" "$TMP/corpus" warm bypass 1 1

# An INCOMPLETE identity is refused per field: a partial identity is not
# authoritative metadata, and `rows` alone is not the whole contract.
mkdir -p "$TMP/corpus-norows"
cat > "$TMP/corpus-norows/corpus-identity.json" <<EOF
{ "partitions": 10, "seed": 1, "cells_per_row": 12, "data_db_bytes": 700000,
  "data_db_sha256": "$FAKE_SHA", "bytes_per_row": 700.0 }
EOF
d="$TMP/id-norows"; make_warm_session "$d"
expect_report_reject "an identity with no 'rows' is refused, naming the field" \
  "carries no 'rows'" "$d" "$TMP/corpus-norows" warm bypass 1 1

mkdir -p "$TMP/corpus-zero"
cat > "$TMP/corpus-zero/corpus-identity.json" <<EOF
{ "rows": 0, "partitions": 10, "seed": 1, "cells_per_row": 12,
  "data_db_bytes": 700000, "data_db_sha256": "$FAKE_SHA", "bytes_per_row": 700.0 }
EOF
d="$TMP/id-zero"; make_warm_session "$d"
expect_report_reject "an identity claiming ZERO rows is refused (not a measurable corpus)" \
  "not a measurable corpus" "$d" "$TMP/corpus-zero" warm bypass 1 1

# An identity whose OWN fields disagree cannot be authoritative, whichever one is
# wrong: bytes_per_row must equal data_db_bytes/rows.
make_corpus "$TMP/corpus-inconsistent" "$CORPUS_ROWS" 700000 42.0
d="$TMP/id-inconsistent"; make_warm_session "$d"
expect_report_reject "an internally inconsistent identity is refused (bytes_per_row vs bytes/rows)" \
  "internally inconsistent" "$d" "$TMP/corpus-inconsistent" warm bypass 1 1

# The digest is the corpus's determinism pin, so a truncated one is refused: it
# cannot identify the bytes that were measured.
mkdir -p "$TMP/corpus-shortsha"
cat > "$TMP/corpus-shortsha/corpus-identity.json" <<EOF
{ "rows": $CORPUS_ROWS, "partitions": 10, "seed": 1, "cells_per_row": 12,
  "data_db_bytes": 700000, "data_db_sha256": "deadbeef", "bytes_per_row": 700.0 }
EOF
d="$TMP/id-shortsha"; make_warm_session "$d"
expect_report_reject "a truncated corpus digest is refused (cannot identify the measured bytes)" \
  "64 lowercase hex" "$d" "$TMP/corpus-shortsha" warm bypass 1 1

# And the happy path still records the verification as having HAPPENED, with the
# row count it used — so the field cannot read `true` without a number behind it.
d="$TMP/id-ok"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" "$CORPUS_ROWS" <<'PY'
import json, sys
ms = json.load(open(sys.argv[1]))["measurements"]
fl = [m for m in ms if m["arm"].startswith("flight_")]
assert fl, "no flight measurement recorded"
assert fl[0]["full_corpus_per_request_verified"] is True, fl[0]
assert fl[0]["corpus_rows_used_for_verification"] == int(sys.argv[2]), fl[0]
PY
then
  pass "a verified run records full_corpus_per_request_verified=true WITH the row count used"
else
  fail "the happy path must record the verification and its row count (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 2 — `skipped-cold-arm` satisfies a COLD rep ONLY
# ==========================================================================
# NON-VACUITY. The pre-fix acceptance set was a flat, temperature-BLIND tuple:
#
#     OK_PREWARM = ("ok", "skipped-cold-arm")
#     "prewarm_all_ok": all(p["status"] in OK_PREWARM for p in prewarm)
#
# MEASURED against the pre-fix code: a WARM session whose bare-scan AND flight reps
# both recorded `skipped-cold-arm` exited **0**, printed NO "PREWARM DEGRADED" line,
# and wrote `prewarm_all_ok: true` for both arms. That is an UNPREWARMED WARM
# measurement passing the guard added to prevent exactly that — using the cold arm's
# own sentinel as the key. Both arms are covered below, because the bare scan is the
# DENOMINATOR of the ratio (reading slow there makes the target easier).
d="$TMP/warm-cold-sentinel-scan"; mkdir -p "$d"
make_scan_rep "$d" warm 1 skipped-cold-arm
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a WARM bare-scan rep carrying 'skipped-cold-arm' is REFUSED" \
  "only a COLD rep can record" "$d" "$TMP/corpus" warm bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1)
if grep -q "UNPREWARMED" <<<"$out"; then
  pass "the refusal says the warm rep was UNPREWARMED (not merely 'inconsistent')"
else
  fail "the warm-sentinel refusal must name the unprewarmed measurement (out: $out)"
fi

d="$TMP/warm-cold-sentinel-flight"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" skipped-cold-arm
expect_report_reject "a WARM flight rep carrying 'skipped-cold-arm' is REFUSED" \
  "only a COLD rep can record" "$d" "$TMP/corpus" warm bypass 1 1

# The mirror direction: a COLD rep that recorded a SUCCESSFUL prewarm is not cold.
# The sentinel is scoped to a temperature in BOTH directions, not merely blocked in
# the one that was found in review.
d="$TMP/cold-prewarmed"; mkdir -p "$d"
make_scan_rep "$d" cold 1 ok
make_flight_rep "$d" cold 1 1 "$CORPUS_ROWS" skipped-cold-arm
expect_report_reject "a COLD rep that recorded a successful prewarm is REFUSED (it is not cold)" \
  "only a WARM rep can record" "$d" "$TMP/corpus" cold bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus" cold bypass 1 1)
if grep -q "prewarmed rep is not cold" <<<"$out"; then
  pass "the cold-direction refusal says a prewarmed rep is not cold"
else
  fail "the cold-direction refusal must state why (out: $out)"
fi

# A HONEST degradation is still reported, not refused: the two cases must stay
# distinguishable, or the fix would have turned every flaky prewarm into a lost rep.
d="$TMP/warm-degraded-still-reported"; mkdir -p "$d"
make_scan_rep "$d" warm 1 FAILED-exit-9
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "PREWARM DEGRADED on bare-scan rep(s) 1=FAILED-exit-9" <<<"$out"; then
  pass "an honestly-recorded prewarm FAILURE is still reported+flagged (not refused)"
else
  fail "a recorded prewarm failure must be flagged, not refused (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 4 — a counter that was not observed is an ERROR, never a 0
# ==========================================================================
# NON-VACUITY. The pre-fix reporter read every counter through a defaulting get:
#
#     cyc = total.get("cycles", 0) - setup.get("cycles", 0)
#     setup_cycles_total += setup.get("cycles", 0)
#
# and `read_perf_csv` returned `{}` for a file that does not exist. MEASURED against
# the pre-fix code: a session dir with NO `perf-scan-warm-1-setup.csv` AT ALL exited
# **0** and reported `cycles_setup: 0`, `setup_cycles_subtracted_total: 0`, while the
# summary printed "the bare scan's cycles are SETUP-SUBTRACTED". Nothing was
# subtracted. Each case below removes or corrupts exactly one counter.
d="$TMP/no-setup-csv"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
rm -f "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "an ABSENT setup perf CSV is FATAL (never a 0 subtraction)" \
  "were never observed" "$d" "$TMP/corpus" warm bypass 1 1
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1)
if grep -q "cannot substitute a zero" <<<"$out"; then
  pass "the refusal states that a zero may not stand in for an unobserved counter"
else
  fail "the absent-counter refusal must reject the substitution explicitly (out: $out)"
fi

d="$TMP/no-total-csv"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
rm -f "$d/perf-scan-warm-1.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "an ABSENT full-run perf CSV is FATAL" \
  "were never observed" "$d" "$TMP/corpus" warm bypass 1 1

d="$TMP/no-flight-csv"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
rm -f "$d/perf-flight-bypass-warm-1.csv"
expect_report_reject "an ABSENT flight perf CSV is FATAL (the arm has no setup leg to hide it)" \
  "were never observed" "$d" "$TMP/corpus" warm bypass 1 1

# A CSV that exists but carries no `cycles` line: the file is there, the counter is
# not. The pre-fix `.get("cycles", 0)` could not tell these apart.
d="$TMP/csv-no-cycles"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf '4000000,,instructions,,,,\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a perf CSV with no 'cycles' line is FATAL, naming the missing event" \
  "no line for required event(s) cycles" "$d" "$TMP/corpus" warm bypass 1 1

d="$TMP/csv-no-instructions"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf '100000,,cycles,,,,\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "a perf CSV with no 'instructions' line is FATAL (IPC has no numerator)" \
  "no line for required event(s) instructions" "$d" "$TMP/corpus" warm bypass 1 1

# perf's OWN not-a-value markers. This is the silent-instrument failure in its
# purest form: the line EXISTS, perf EXITED ZERO, and there is no number.
for marker in '<not counted>' '<not supported>'; do
  d="$TMP/csv-marker-$(tr -d ' <>' <<<"$marker")"; mkdir -p "$d"
  make_scan_rep "$d" warm 1 ok
  printf '%s,,cycles,,,,\n200000,,instructions,,,,\n' "$marker" > "$d/perf-scan-warm-1-setup.csv"
  make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
  expect_report_reject "a perf '$marker' value is FATAL (perf did not count it)" \
    "perf did not count it" "$d" "$TMP/corpus" warm bypass 1 1
done

# A corrupt value is a corrupt artifact, not a zero.
d="$TMP/csv-garbage"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
printf 'NaNsense,,cycles,,,,\n200000,,instructions,,,,\n' > "$d/perf-scan-warm-1-setup.csv"
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
expect_report_reject "an unparseable perf value is FATAL (corrupt artifact, not a 0)" \
  "unparseable value" "$d" "$TMP/corpus" warm bypass 1 1

# And on the happy path the subtraction is a REAL non-zero, recorded — so a future
# regression to a defaulted 0 shows up as a value, not only as an absent error.
d="$TMP/setup-subtracted"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
s = [m for m in json.load(open(sys.argv[1]))["measurements"] if m["arm"] == "bare_scan"][0]
r = s["reps"][0]
assert r["cycles_setup"] == 100000, r
assert r["cycles_total"] == 2000000, r
assert r["cycles_scan"] == 1900000, r          # a REAL subtraction, not a 0
assert s["setup_cycles_subtracted_total"] == 100000, s
PY
then
  pass "the happy path records a REAL non-zero setup subtraction"
else
  fail "setup subtraction must be a real observed value (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 5 — the numeric arguments are validated fail-closed
# ==========================================================================
# NON-VACUITY. `--reps` was `type=int` with no range check, and `range(1, 0 + 1)` is
# EMPTY, so every collector loop body was skipped, `per_rep` stayed empty, and the
# pre-fix `require_complete` returned early on `not per_rep and not missing`.
# MEASURED against the pre-fix code: `--reps 0` over an EMPTY session dir exited
# **0** and wrote a results.json with `measurements: []` — a report that measured
# nothing, indistinguishable at the exit code from one that measured everything.
# `--reps -3` and `--scan-passes -1` did the same.
d="$TMP/reps-zero"; make_warm_session "$d"
expect_report_reject "--reps 0 is REFUSED (a vacuous but successful report)" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass 0 1
if [ ! -e "$TMP/reps-zero/results.json" ]; then
  pass "--reps 0 writes no results.json"
else
  fail "--reps 0 must not write a results.json"
fi
expect_report_reject "--reps -3 is REFUSED" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass -3 1
expect_report_reject "--reps 'abc' is REFUSED with a reason (not a traceback)" \
  "must be an integer" "$d" "$TMP/corpus" warm bypass abc 1
expect_report_reject "--scan-passes 0 is REFUSED (same hole, same class)" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass 1 0
expect_report_reject "--scan-passes -1 is REFUSED" \
  "must be at least 1" "$d" "$TMP/corpus" warm bypass 1 -1
# The non-numeric selections had the same vacuous-green hole: an empty --temps/--arms
# produced zero measurements and exit 0.
expect_report_reject "an EMPTY --temps is REFUSED (would report zero measurements)" \
  "is empty" "$d" "$TMP/corpus" "" bypass 1 1
expect_report_reject "an EMPTY --arms is REFUSED" \
  "is empty" "$d" "$TMP/corpus" warm "" 1 1
expect_report_reject "an UNKNOWN --temps value is REFUSED (never silently ignored)" \
  "unknown value" "$d" "$TMP/corpus" "warm tepid" bypass 1 1
expect_report_reject "an UNKNOWN --arms value is REFUSED" \
  "unknown value" "$d" "$TMP/corpus" warm "bypass sideways" 1 1
expect_report_reject "a REPEATED --temps value is REFUSED (would double-count a leg)" \
  "repeats warm" "$d" "$TMP/corpus" "warm warm" bypass 1 1
# A --dir that does not exist must not be created-then-reported-on.
expect_report_reject "a nonexistent --dir is REFUSED" \
  "is not an existing directory" "$TMP/does-not-exist" "$TMP/corpus" warm bypass 1 1
expect_report_reject "a nonexistent --corpus is REFUSED" \
  "is not an existing directory" "$d" "$TMP/no-such-corpus" warm bypass 1 1

# The driver validates --reps too, up front, before any build or cache drop. It used
# to accept `--reps 0` (and `--reps abc`, which then made `seq 1 abc` emit nothing)
# and only fail later at the missing-corpus check.
check_driver_reject "the DRIVER refuses --reps 0 before doing any work" \
  "must be at least 1" --corpus "$TMP/corpus" --reps 0
check_driver_reject "the DRIVER refuses a non-numeric --reps" \
  "positive integer" --corpus "$TMP/corpus" --reps abc
check_driver_reject "the DRIVER refuses a negative --reps" \
  "positive integer" --corpus "$TMP/corpus" --reps -2
check_driver_reject "the DRIVER refuses --port 0" \
  "must be at least 1" --corpus "$TMP/corpus" --port 0
check_driver_reject "the DRIVER refuses an out-of-range --port" \
  "65535" --corpus "$TMP/corpus" --port 70000

# ==========================================================================
# #3272 finding 6 — completeness is judged against the SELECTION, and the
#                   selection is stated in the report
# ==========================================================================
# NON-VACUITY, in BOTH directions. `require_complete`'s docstring claimed
# "`per_rep` empty AND nothing missing -> this (arm, temperature) was never run;
# not an error" — but the collectors append EVERY absent expected artifact to
# `missing` before calling it, so that branch was DEAD CODE and the case it
# documented exited fatally. MEASURED against the pre-fix code: a session dir
# holding only WARM reps, reported with `--temps "warm cold"`, exited **1** with
# "bare scan (cold) collected 0 of 1 requested reps"; and with
# `--arms "bypass merge"` it exited **1** on the absent merge arm. An intentionally
# narrow run was indistinguishable from a crashed one.
#
# The fix is not to loosen the check — it is to make the SELECTION the thing
# completeness is judged against, and to STATE it. So both of these must hold:
#   (a) an unselected combination is simply not iterated (nothing to be absent);
#   (b) a SELECTED combination that is absent stays fatal.
d="$TMP/warm-only"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "a warm-only session reported as warm-only SUCCEEDS (the narrow run is legitimate)"
else
  fail "a warm-only selection must succeed on a warm-only dir (rc=$rc, out: $out)"
fi
# (b) the same dir, with cold SELECTED, is still fatal — the fix did not open a hole.
expect_report_reject "a SELECTED but absent temperature is still FATAL" \
  "bare scan (cold) collected 0 of 1" "$d" "$TMP/corpus" "warm cold" bypass 1 1
expect_report_reject "a SELECTED but absent arm is still FATAL" \
  "flight do_get merge (warm) collected 0 of 1" "$d" "$TMP/corpus" warm "bypass merge" 1 1
# ...and a PARTIAL collection of a selected combination is fatal, which is the case
# the whole check exists for.
d="$TMP/partial-reps"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
make_scan_rep "$d" warm 2 ok
make_flight_rep "$d" warm 1 1 "$CORPUS_ROWS" ok
# rep 2 of the flight arm is missing: 1 of 2 collected.
expect_report_reject "a PARTIAL collection of a selected arm is FATAL (the original guard holds)" \
  "collected 1 of 2" "$d" "$TMP/corpus" warm bypass 2 1

# The selection must be VISIBLE — in results.json and in the human summary — or a
# narrow run reads exactly like a full matrix that printed fewer rows.
d="$TMP/selection-recorded"; make_warm_session "$d"
out=$(run_report_full "$d" "$TMP/corpus" warm bypass 1 1); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
sel = json.load(open(sys.argv[1]))["selection"]
assert sel["temperatures"] == ["warm"], sel
assert sel["arms"] == ["bypass"], sel
assert sel["full_matrix"] is False, sel
assert "cold" in sel["temperatures_available"], sel
assert "merge" in sel["arms_available"], sel
PY
then
  pass "results.json records the SELECTION and marks a partial matrix as partial"
else
  fail "results.json must record the selection (rc=$rc, out: $out)"
fi
if grep -q "PARTIAL MATRIX" <<<"$out" && grep -q "NOT MEASURED" <<<"$out"; then
  pass "the human summary says PARTIAL MATRIX / NOT MEASURED for a narrow run"
else
  fail "a narrow run's summary must say so loudly (out: $out)"
fi
# A FULL matrix must NOT carry the partial warning — the marker has to mean something.
d="$TMP/full-matrix"; mkdir -p "$d"
for temp in warm cold; do
  case "$temp" in warm) pw=ok ;; cold) pw=skipped-cold-arm ;; esac
  make_scan_rep "$d" "$temp" 1 "$pw"
  for arm in bypass merge; do
    tag="flight-$arm-$temp-1"
    cat > "$d/$tag.jsonl" <<EOF
{"round":"$tag","requests_ok":1,"requests_error":0,"rows_total":$CORPUS_ROWS,"rows_per_s":250000.0,"duration_s":4.0}
EOF
    perf_csv "$d/perf-$tag.csv" 8000000 16000000
    printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
  done
done
out=$(run_report_full "$d" "$TMP/corpus" "warm cold" "bypass merge" 1 1); rc=$?
if [ "$rc" -eq 0 ] && ! grep -q "PARTIAL MATRIX" <<<"$out" \
  && python3 - "$d/results.json" <<'PY'
import json, sys
sel = json.load(open(sys.argv[1]))["selection"]
assert sel["full_matrix"] is True, sel
PY
then
  pass "a FULL matrix is not flagged partial (the marker distinguishes, not decorates)"
else
  fail "a full matrix must record full_matrix=true and no partial warning (rc=$rc, out: $out)"
fi

# ==========================================================================
# #3272 finding 7 — durations parse as DECIMAL, never octal
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `parse_duration_ms` (which fed the
# stripped digits straight into `$((n * 1000))`):
#   * `010s`   -> 8000 ms. A caller asking for 10s silently measured 8s.
#   * `08s`    -> hard bash error "08: value too great for base (error token is 08)",
#                 which the `case` turned into "must be <n>ms, <n>s or <n>m" — a
#                 diagnostic about the FORMAT for a value whose format is fine.
#   * `030ms`  -> 24 ms.
#   * `010000ms` -> 4096 ms, i.e. UNDER the 5000ms cold ceiling while really being
#                 10s: the octal parse could smuggle a blended cold step past the
#                 guard of #3096 finding 2.
# The driver is only ever reached at argument validation here — no corpus, no sudo.
#
# `--cold-step-duration 010s` = 10s > the 5000ms ceiling, so it must be REFUSED.
# Pre-fix it parsed as 8000ms and was ACCEPTED (falling through to the missing-corpus
# error instead), which is what this case observes.
check_driver_reject "'010s' is parsed as 10s (decimal) and refused by the cold ceiling" \
  "10000ms) exceeds the" --corpus "$TMP/corpus" --temp cold --cold-step-duration 010s
check_driver_reject "'010000ms' is 10000ms, not octal 4096 — it cannot sneak under the ceiling" \
  "10000ms) exceeds the" --corpus "$TMP/corpus" --temp cold --cold-step-duration 010000ms
# `08s` is a legitimate spelling of 8s: it must reach the CEILING check (8000 > 5000)
# and be refused for its VALUE, not die with a format complaint about a valid format.
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 08s 2>&1)
if grep -q "8000ms) exceeds the" <<<"$out"; then
  pass "'08s' parses as 8000ms (pre-fix: a bash 'value too great for base' error)"
else
  fail "'08s' must parse as 8000ms and be judged on its value (out: $out)"
fi
# And a leading-zero value that is genuinely IN range must be ACCEPTED, so the fix
# is not "reject leading zeros".
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp cold --cold-step-duration 0500ms 2>&1)
if ! grep -q "exceeds the" <<<"$out" && ! grep -q "must be <n>ms" <<<"$out"; then
  pass "'0500ms' (=500ms, in range) is ACCEPTED — leading zeros are parsed, not banned"
else
  fail "'0500ms' must be accepted as 500ms (out: $out)"
fi
# The warm step goes through the same parser.
out=$(bash "$DRIVER" --corpus "$TMP/corpus" --temp warm --step-duration 045s 2>&1)
if ! grep -q "must be <n>ms" <<<"$out" && ! grep -q "greater than zero" <<<"$out"; then
  pass "'045s' is accepted for --step-duration (pre-fix: a bash base error)"
else
  fail "'045s' must parse for --step-duration (out: $out)"
fi
# A structural check that no arithmetic path can regress: every multiplication of a
# parsed duration component must carry `10#`.
if awk '/^parse_duration_ms\(\)/,/^}/' "$DRIVER" | grep -q '\$((n \* 1000))'; then
  fail "parse_duration_ms still multiplies a bare \$n — leading zeros would be octal again"
else
  pass "parse_duration_ms feeds no bare component into arithmetic (structural)"
fi

# ==========================================================================
# #3272 finding 3 — the driver RESTORES the host sysctls it mutates
# ==========================================================================
# NON-VACUITY: the pre-fix driver ran
#     sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
# and its ONLY trap was `trap stop_server EXIT`, so every run — success, FATAL or
# Ctrl-C — left the host's perf hardening weakened. `grep -c 'kptr_restrict'` on the
# pre-fix file finds exactly ONE occurrence (the weakening), and none in any trap.
#
# Structural, because the behaviour needs root: the restore must be REGISTERED on
# EXIT **and** on the signals, and it must be part of the same trap that stops the
# server rather than replacing it (a second bare `trap ... EXIT` would silently
# discard the first).
if awk '/^trap /' "$DRIVER" | grep -q 'INT TERM HUP'; then
  pass "the driver traps INT/TERM/HUP, not only EXIT (a Ctrl-C used to skip cleanup)"
else
  fail "the driver must trap INT/TERM/HUP as well as EXIT"
fi
if [ "$(grep -c '^trap ' "$DRIVER")" -eq 1 ]; then
  pass "there is exactly ONE top-level trap registration (a second would discard the first)"
else
  fail "multiple top-level 'trap' lines: a later bare EXIT trap discards the earlier one"
fi
if grep -q 'restore_sysctls' "$DRIVER" \
  && awk '/^on_exit\(\)/,/^}/' "$DRIVER" | grep -q 'restore_sysctls' \
  && awk '/^on_exit\(\)/,/^}/' "$DRIVER" | grep -q 'stop_server'; then
  pass "the single exit handler runs BOTH stop_server and restore_sysctls"
else
  fail "the exit handler must run stop_server AND restore_sysctls"
fi
# The prior values must be CAPTURED BEFORE the mutation, or there is nothing to
# restore to: assert the capture precedes the `sysctl -w` in file order.
cap_line=$(grep -n 'PARANOID_PRIOR=' "$DRIVER" | head -1 | cut -d: -f1)
mut_line=$(grep -n 'sysctl -w kernel.perf_event_paranoid' "$DRIVER" | head -1 | cut -d: -f1)
if [ -n "$cap_line" ] && [ -n "$mut_line" ] && [ "$cap_line" -lt "$mut_line" ]; then
  pass "the prior sysctl values are captured BEFORE the mutation (line $cap_line < $mut_line)"
else
  fail "prior values must be captured before mutating (capture=$cap_line mutate=$mut_line)"
fi
# Both sysctls the driver weakens must be restored — not just the one in the message.
for knob in perf_event_paranoid kptr_restrict; do
  if awk '/^restore_sysctls\(\)/,/^}/' "$DRIVER" | grep -q "$knob"; then
    pass "restore_sysctls restores kernel.$knob"
  else
    fail "restore_sysctls must restore kernel.$knob (the driver weakens it)"
  fi
done
# The restore must be IDEMPOTENT and must never fail the run: it is cleanup, and a
# cleanup that can exit non-zero turns a successful measurement into a failed one.
if awk '/^restore_sysctls\(\)/,/^}/' "$DRIVER" | grep -q 'SYSCTLS_MUTATED' \
  && awk '/^restore_sysctls\(\)/,/^}/' "$DRIVER" | grep -q '|| true'; then
  pass "restore_sysctls is guarded by a mutated-flag and cannot fail the run"
else
  fail "restore_sysctls must be flag-guarded (idempotent) and non-fatal"
fi
# It must run under `set -e` too: an exit handler that inherits errexit and dies
# midway would restore one knob and skip the other.
if awk '/^restore_sysctls\(\)/,/^}/' "$DRIVER" | grep -q 'kptr_restrict' \
  && awk '/^restore_sysctls\(\)/,/^}/' "$DRIVER" | grep -cq '|| true'; then
  pass "each restore step is individually non-fatal (one knob cannot orphan the other)"
else
  fail "each restore step must be individually non-fatal"
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0-report guards: all checks passed"
  exit 0
fi
echo "ws0-report guards: $fails check(s) FAILED"
exit 1

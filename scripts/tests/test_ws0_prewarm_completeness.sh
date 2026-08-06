#!/usr/bin/env bash
# test_ws0_prewarm_completeness.sh — DID THE PREWARM ACTUALLY WARM THE WHOLE CORPUS?
# (issue #3272; split from `test_ws0_report_guards.sh` in review round 12.)
#
# # Why this is its own suite
#
# Split under the campsite rule (~1500-line test target: the parent reached 1646 with round 12's F2
# in it) and, as every split on this branch has been, along a RESPONSIBILITY seam rather than at a
# line count. The parent's subject is THE REPORTER'S FAIL-CLOSED PATHS — what `ws0_report.py` refuses
# about a session dir. This file's subject is one question the reporter cannot answer at all:
#
#     WAS THE MEASUREMENT ACTUALLY WARM — i.e. DID THE UNTIMED PREWARM READ THE WHOLE CORPUS?
#
# That is a distinct subject because it is decided at MEASUREMENT TIME, in `lib-measure.sh`, by
# classifiers in `ws0_prewarm.py`, over artifacts (`<tag>.prewarm.jsonl`, `<tag>.prewarm.json`) the
# reporter never reads — it reads only the one-word `<tag>.prewarm.status` those classifiers produce.
# Every check in the parent is satisfiable by a session whose recorded prewarm statuses all read `ok`
# and whose prewarms warmed 0.02% of the corpus.
#
# The two findings that share the subject, in the order they were found:
#
#   * ROUND 10, F-A — `measure_flight` set `prewarm_status="ok"` from the loadgen's EXIT STATUS
#     alone, and passed `--out /dev/null`, discarding the only record of what the prewarm did. The
#     loadgen exits 0 whenever the ramp completes, and a step whose every request was SHED (#2420) or
#     ERRORED completes normally, because those outcomes are COUNTED rather than fatal. So a prewarm
#     that served NOTHING was recorded as healthy.
#   * ROUND 12, F2 — F-A's replacement rule was `requests_ok >= 1 AND rows_total >= 1`. That is a
#     NON-ZERO check where the property is a COMPLETENESS one: a request that streamed 40 of 200,000
#     rows satisfied it while leaving essentially every page cold. And the BARE-SCAN leg trusted
#     PROCESS SUCCESS while redirecting the bench's JSON — which carries `rows_denominator` and a
#     per-pass `rows` — to a file nobody read. The oracle is now the PINNED corpus row count, on both
#     legs; a threshold is deliberately refused, because a floor is a number somebody chose and the
#     pin is a number that was measured.
#
# Per #3249 (a hardcoded `_PERF_STATE="ok"` survived 118/118 tests) the bar is OBSERVED TO FIRE, so
# every case carries the MEASURED pre-fix behaviour — here by RECONSTRUCTING each superseded rule
# verbatim and running it on the same input, so the change is a measured flip and not a new
# function's first output.
#
# Hermetic: synthetic prewarm artifacts, a synthetic few-KB `Data.db` whose real sha256 is computed
# with hashlib, and a session pin written by the SHIPPED writer. No cargo, perf, sudo, taskset,
# corpus, network or root — and no driver invocation at all, so `ws0_driver_run` is not needed here.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"
# The file that owns the two measurement legs (#3272 round 9): the wiring checks below read
# `measure_scan`/`measure_flight` from here, and the `-n`/`-s` guards make a stale awk range RED
# rather than vacuously green.
MEASURE_LIB="$REPO_ROOT/scripts/perf/lib-measure.sh"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so the
# minimum-check-count floor at the end can see a block that silently never executed.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
[ -s "$MEASURE_LIB" ] || { echo "FAIL - missing $MEASURE_LIB"; exit 1; }
# python3 is a HARD REQUIREMENT of this rig — `ws0-baseline.sh` refuses to run without it — so its
# absence is a FAILURE, not a skip: exiting 0 here would record the gate component as SUCCESS with
# none of the checks below having run, which is the vacuous green this rig exists to refuse.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig"
  echo "       (scripts/perf/ws0-baseline.sh refuses to run without it), so its"
  echo "       absence is a failed check and not a skip: exiting 0 here would record"
  echo "       this component as SUCCESS with 0 of its checks having run."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

CORPUS_ROWS=1000

# The SHARED fixture builders — `make_corpus`/`make_session`/`make_scan_rep`/`make_flight_rep`,
# `run_report`, and (via `lib-ws0-fixtures.sh`) `ws0_pin_session_corpus`. Sourced rather than
# re-implemented for the reason the parent records: a duplicated builder is the wrong thing to keep
# copies of.
# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"

make_corpus "$TMP/corpus"

# The RIG must actually contain the untimed bare-scan prewarm — a reporter that merely READS a
# status file would pass every case below with no prewarm running. Kept here rather than in the
# parent because its subject is `lib-measure.sh`, which is this file's subject.
scan_body=$(awk '/^measure_scan\(\)/,/^}/' "$MEASURE_LIB")
if [ -n "$scan_body" ] \
  && grep -q 'prewarm_status="skipped-cold-arm"' "$MEASURE_LIB" \
  && grep -q 'prewarm.status' <<<"$scan_body"; then
  pass "measure_scan itself records a prewarm status (not just the reporter) — read from lib-measure.sh, the file that owns the legs"
else
  fail "measure_scan must run and record its own prewarm (body lines=$(printf '%s' "$scan_body" | grep -c . ))"
fi

# ==========================================================================
# #3272 ROUND 10, F-A — a Flight prewarm reads `ok` only on an AFFIRMATIVE
#                       MEASUREMENT, never on an exit status
# ==========================================================================
# THE FINDING. `measure_flight` set `prewarm_status="ok"` from the `if` on
# flight-loadgen's exit alone, and passed `--out /dev/null` — discarding the only
# record of what the prewarm did. The loadgen exits 0 whenever the ramp completes,
# and a step whose every request was SHED (admission control, #2420) or ERRORED
# completes normally, because those outcomes are COUNTED rather than fatal. So a
# prewarm that served nothing, or streamed zero rows, was recorded as healthy and
# the rep it belongs to claims a WARM measurement having faulted in nothing.
#
# This is AC1 finding 2's exact class (`skipped-cold-arm` counting as a successful
# prewarm) recurring at a NEW LINE — the "a fix moved the problem" pattern this
# split was opened for. The remedy is symmetric with AC1's: a status may read `ok`
# only when a measurement says so.
PREWARM_PY="$REPO_ROOT/scripts/perf/ws0_prewarm.py"
if [ -s "$PREWARM_PY" ]; then
  pass "the prewarm classifier module exists (scripts/perf/ws0_prewarm.py)"
else
  fail "scripts/perf/ws0_prewarm.py is missing — the F-A fix derives the prewarm status from the retained JSONL, so its absence means the status is back to an exit code"
fi

# THE COMPLETENESS ORACLE'S SESSION DIR (#3272 round 12, F2). The classifier reads
# `session-corpus-pin.json` for the corpus row count rather than taking a count from its caller, so
# every case below is driven against a session dir carrying the REAL pin — written by the SHIPPED
# writer through `ws0_pin_session_corpus`, so a fixture cannot pin a row count the writer's shape no
# longer has. Its `rows` is `$CORPUS_ROWS`, which is what makes `rows_total` values below full
# passes or fractions.
PW_SESSION="$TMP/pw-session"; mkdir -p "$PW_SESSION"
ws0_pin_session_corpus "$PW_SESSION" "$TMP/corpus"
pw_pin_rows=$(python3 -c '
import json, pathlib, sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text())["rows"])
' "$PW_SESSION/session-corpus-pin.json" 2>/dev/null)
if [ "$pw_pin_rows" = "$CORPUS_ROWS" ]; then
  pass "F2 fixture: the prewarm cases' session dir carries a REAL corpus pin of $pw_pin_rows rows (written by the shipped writer, so the completeness oracle below is the shipped one)"
else
  fail "F2 fixture: the prewarm session pin must record $CORPUS_ROWS rows (got '$pw_pin_rows'); every completeness case below would otherwise test a fixture rather than the oracle"
fi

# The two LEGS, each named EXPLICITLY at the call site (#3272 round 12, F2) — the classifier does not
# sniff which reader to use from the artifact's contents, because guessing would report a shape
# failure about a truncated Flight JSONL instead of the partial scan that caused it. Wrapped so the
# session dir is not repeated at ~15 call sites; a case whose subject IS the pin passes its own.
pw_flight() { python3 "$PREWARM_PY" flight "$1" "$2" "${3:-$PW_SESSION}"; }
pw_scan() { python3 "$PREWARM_PY" scan "$1" "$2" "${3:-$PW_SESSION}"; }

# --- NON-VACUITY, MEASURED: what the PRE-FIX code accepted -------------------
# The pre-fix logic is reconstructed VERBATIM (the `if <loadgen>; then ok` shape) against a
# stand-in that exits 0 having served nothing, and asserted to yield `ok`. Then the SAME
# scenario is put to the new classifier and must yield a failure label. Without the first
# half, the second proves only that a new function returns a string; with it, the change is
# a measured flip on identical input.
pw_prefix="$(
  fake_loadgen() { return 0; }              # exit 0 having served nothing: a completed ramp
  st="skipped-cold-arm"
  if fake_loadgen; then st="ok"; else st="FAILED-exit-$?"; fi
  printf '%s' "$st"
)"
# The prewarm JSONL such a run would have written, had it not been sent to /dev/null: every
# request shed by admission control, nothing served, no rows.
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":0,"requests_unavailable":40,"requests_error":0,"error_codes":{},"rows_total":0}' > "$TMP/pw-nothing.jsonl"
pw_now="$(pw_flight 0 "$TMP/pw-nothing.jsonl")"
if [ "$pw_prefix" = "ok" ] && [ "$pw_now" = "FAILED-zero-successful-requests" ]; then
  pass "NON-VACUITY (F-A): the PRE-FIX exit-status logic records '$pw_prefix' for a loadgen that exited 0 having served NOTHING; the classifier records '$pw_now' on the same run"
else
  fail "F-A non-vacuity: expected pre-fix 'ok' and post-fix 'FAILED-zero-successful-requests', got pre-fix '$pw_prefix' and post-fix '$pw_now'"
fi

# ZERO ROWS with successful requests: a request can complete having streamed an empty
# stream, and an empty stream warms no page cache. Distinct from the case above because the
# request COUNT alone would have been satisfied — checking only `requests_ok` would be the
# same partial fix one field over.
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":5,"requests_unavailable":0,"requests_error":0,"error_codes":{},"rows_total":0}' > "$TMP/pw-norows.jsonl"
if [ "$(pw_flight 0 "$TMP/pw-norows.jsonl")" = "FAILED-zero-rows" ]; then
  pass "OBSERVED (F-A): successful requests that streamed ZERO ROWS are a degradation — a request count alone cannot establish that anything was warmed"
else
  fail "a prewarm with requests_ok>0 but rows_total==0 must NOT read as ok (got $(pw_flight 0 "$TMP/pw-norows.jsonl"))"
fi

# THE DISCARDED-EVIDENCE CASE, which is the defect's root rather than a symptom: with
# `--out /dev/null` there was never a record to inspect. An absent JSONL must therefore be a
# degradation, or the fix could be undone by reverting one flag and nothing would notice.
if [ "$(pw_flight 0 "$TMP/pw-absent-$$.jsonl")" = "FAILED-no-jsonl" ]; then
  pass "OBSERVED (F-A): an ABSENT prewarm JSONL is a degradation — reverting to --out /dev/null cannot silently restore a healthy label"
else
  fail "an absent prewarm JSONL must be a named degradation, not an ok"
fi

# THE ACCEPT DIRECTION, affirmatively — without it every case above would be satisfied by a
# classifier that refuses everything, which is the mirror-image broken instrument (a guard
# that always fires teaches an operator to ignore it, AC1's own lesson).
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":3,"requests_unavailable":0,"requests_error":0,"error_codes":{},"rows_total":3000}' > "$TMP/pw-good.jsonl"
pw_good="$(pw_flight 0 "$TMP/pw-good.jsonl")"
# ...and a prewarm that shed SOME requests but completed at least one full scan is STILL ok:
# the prewarm's job (fault the corpus in) demonstrably happened. The MEASURED reps refuse any
# non-zero shed counter (ws0_loadgen_record.ZERO_REQUIRED_COUNTERS); conflating the two would
# make this guard fire on a healthy prewarm.
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":2,"requests_unavailable":7,"requests_error":0,"error_codes":{},"rows_total":2000}' > "$TMP/pw-shed.jsonl"
pw_shed="$(pw_flight 0 "$TMP/pw-shed.jsonl")"
if [ "$pw_good" = "ok" ] && [ "$pw_shed" = "ok" ]; then
  pass "AFFIRMATIVE (F-A): a prewarm that served requests AND streamed rows reads 'ok', including one that shed some requests while completing others"
else
  fail "the classifier must not refuse a healthy prewarm (clean=$pw_good, partly-shed=$pw_shed)"
fi

# A NON-ZERO EXIT still fails, and is labelled with the code — the pre-existing behaviour the
# fix must not have dropped while adding the JSONL requirement.
if [ "$(pw_flight 7 "$TMP/pw-good.jsonl")" = "FAILED-exit-7" ]; then
  pass "OBSERVED (F-A): a non-zero loadgen exit is still a labelled failure, naming the code, even with a healthy JSONL beside it"
else
  fail "a non-zero exit must remain a failure regardless of the JSONL"
fi

# A MALFORMED / uncounted record is a degradation rather than a crash: this runs inside the
# measurement loop, and a traceback there would abort a rep the rig has decided to keep and
# label. `requests_ok: 1.9` is the shape `ws0_validate.exact_int` exists for — a bare `int()`
# would truncate it to 1 and satisfy the threshold.
printf 'not json at all\n' > "$TMP/pw-bad.jsonl"
printf '%s\n' '{"requests_ok":1.9,"rows_total":10}' > "$TMP/pw-frac.jsonl"
: > "$TMP/pw-empty.jsonl"
pw_bad="$(pw_flight 0 "$TMP/pw-bad.jsonl")"
pw_frac="$(pw_flight 0 "$TMP/pw-frac.jsonl")"
pw_empty="$(pw_flight 0 "$TMP/pw-empty.jsonl")"
if [ "$pw_bad" = "FAILED-malformed-jsonl" ] \
  && [ "$pw_frac" = "FAILED-uncounted-requests" ] \
  && [ "$pw_empty" = "FAILED-empty-jsonl" ]; then
  pass "OBSERVED (F-A): malformed, fractional-counter and empty prewarm records are each NAMED degradations, never a traceback and never an ok"
else
  fail "malformed/fractional/empty prewarm records must be named degradations (malformed=$pw_bad, fractional=$pw_frac, empty=$pw_empty)"
fi

# --- THE RIG MUST BE WIRED TO IT --------------------------------------------
# Every case above tests the classifier. None of them would notice `measure_flight` still
# passing `--out /dev/null` and keeping its own `if`-on-exit — the guard present but unwired,
# which is this repo's standing "wiring evidence" rule. Read from `lib-measure.sh`'s
# `measure_flight` body by position, and the `-n` guard makes a stale awk range RED rather
# than vacuously green (the lesson the bare-scan block above records).
#
# COMMENTS ARE STRIPPED FIRST, and that is not tidiness — writing this block caught it. The
# leg's own comments DESCRIBE the defect (`passing --out /dev/null`, `used to set
# prewarm_status="ok"`), so a grep over the raw body matched the prose and reported the code as
# unwired when it was correctly wired. A structural scan whose subject includes the
# documentation of what it forbids cannot distinguish a defect from an explanation of it — the
# same lesson `test_ws0_fabrication_guards.sh` records for its `strip_prose`, arrived at
# independently here. Full-line comments only: a trailing `#` inside a quoted loadgen argument
# is not a comment, and stripping from any `#` would corrupt the argv this block inspects.
flight_leg=$(awk '/^measure_flight\(\)/,/^}/' "$MEASURE_LIB" | grep -v '^[[:space:]]*#')
if [ -n "$flight_leg" ] \
  && ! grep -q -- '--out /dev/null' <<<"$flight_leg" \
  && grep -q 'prewarm.jsonl' <<<"$flight_leg" \
  && grep -q 'ws0_prewarm.py' <<<"$flight_leg"; then
  pass "WIRED (F-A): measure_flight's CODE retains the prewarm JSONL (no --out /dev/null) and derives its status via ws0_prewarm.py"
else
  fail "measure_flight must retain the prewarm JSONL and classify it (code lines=$(printf '%s' "$flight_leg" | grep -c . ), still-devnull=$(grep -c -- '--out /dev/null' <<<"$flight_leg"))"
fi

# NON-VACUITY for the strip: the raw body MUST still contain both forbidden strings (in its
# prose), so this asserts the strip is what makes the check answerable rather than the check
# having become trivially true. If the comments are ever reworded away, this reds and whoever
# does it learns the assertion above depends on the strip.
flight_leg_raw=$(awk '/^measure_flight\(\)/,/^}/' "$MEASURE_LIB")
if grep -q -- '--out /dev/null' <<<"$flight_leg_raw" \
  && grep -q 'prewarm_status="ok"' <<<"$flight_leg_raw"; then
  pass "NON-VACUITY (F-A): the RAW leg still carries both forbidden strings in its prose, so the wiring check passes only because comments are stripped — not because the strings are absent"
else
  pass "the leg's prose no longer quotes the forbidden strings; the wiring check above is unconditional (acceptable, and the strip is now redundant rather than load-bearing)"
fi

# ...and it must not have kept a second, permissive path to `ok`. The status is assigned from
# the classifier's output; a literal `prewarm_status="ok"` in the CODE would be the old shape
# surviving beside the new one.
if [ -n "$flight_leg" ] && ! grep -q 'prewarm_status="ok"' <<<"$flight_leg"; then
  pass "OBSERVED (F-A): measure_flight's CODE has NO literal assignment of the ok status — the label can only come from the measurement"
else
  fail "measure_flight still contains a literal prewarm_status=\"ok\", which is a second path to a healthy label that bypasses the measurement"
fi

# The status vocabulary must be the reporter's. `ws0_validate.PREWARM_REQUIRED` matches a warm
# rep's status EXACTLY, so a decorated `ok-with-shed-N` label would be classified `degraded`
# and flag every such rep — two vocabularies for one fact. Asserted by feeding the
# classifier's own output through the reporter's classifier.
if python3 - "$REPO_ROOT/scripts/perf" "$pw_good" "$pw_shed" "$pw_now" <<'PWVOCAB'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import classify_prewarm
good, shed, nothing = sys.argv[2], sys.argv[3], sys.argv[4]
assert classify_prewarm("warm", good) == "ok", good
assert classify_prewarm("warm", shed) == "ok", shed
assert classify_prewarm("warm", nothing) == "degraded", nothing
PWVOCAB
then
  pass "OBSERVED (F-A): the classifier's labels round-trip through ws0_validate.classify_prewarm — ok reads ok, and a served-nothing prewarm reads DEGRADED in the reporter too"
else
  fail "the prewarm classifier's vocabulary must match ws0_validate.PREWARM_REQUIRED exactly, or the driver and reporter disagree about the same rep"
fi

# ==========================================================================
# #3272 ROUND 12, F2 — a prewarm reads `ok` only on a COMPLETE CORPUS SCAN,
#                      never on a NON-ZERO one, and on BOTH legs
# ==========================================================================
# THE FINDING, and it is F-A ABOVE not going far enough. F-A replaced "exit status alone" with
# `requests_ok >= 1 AND rows_total >= 1` — a real improvement, and a NON-ZERO check where the
# property is a COMPLETENESS one. A prewarm's entire job is to fault THE WHOLE CORPUS in, so a
# request that streamed 40 of 200,000 rows satisfied every F-A clause while leaving essentially
# every page cold, and the reps that followed were reported WARM. The bare-scan leg was worse: it
# trusted PROCESS SUCCESS while redirecting the bench's JSON — which carries `rows_denominator` and
# a per-pass `rows` — to a file nobody read.
#
# THE ORACLE IS THE PINNED CORPUS ROW COUNT, never a threshold: `session-corpus-pin.json` records
# `rows` before the first rep, so the completeness question has an authoritative answer on disk and
# a floor would be a number somebody chose rather than one that was measured.

# --- NON-VACUITY, MEASURED: what the ROUND-10 (F-A) rule accepted -------------
# F-A's exact predicate is reconstructed and run against a PARTIAL scan — 1 successful request that
# streamed 40 of the corpus's 1000 rows — and asserted to yield `ok`. Then the SAME record goes to
# the shipped classifier and must yield a partial-scan label. Without the first half this proves only
# that a function returns a string; with it, the change is a measured flip on identical input.
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":1,"requests_unavailable":0,"requests_error":0,"error_codes":{},"rows_total":40}' > "$TMP/pw-partial.jsonl"
pw_fa_verdict="$(python3 - "$TMP/pw-partial.jsonl" <<'PY'
import json, sys
rec = json.loads(open(sys.argv[1]).read().strip())
# VERBATIM the round-10 rule: exit 0, a parsed record, >=1 successful request, >=1 row.
ok = rec["requests_ok"] >= 1 and rec["rows_total"] >= 1
print("ok" if ok else "FAILED")
PY
)"
pw_partial="$(pw_flight 0 "$TMP/pw-partial.jsonl")"
if [ "$pw_fa_verdict" = "ok" ] && [ "$pw_partial" = "FAILED-partial-scan-40-of-1000-rows" ]; then
  pass "NON-VACUITY (round12 F2): the ROUND-10 rule records '$pw_fa_verdict' for a prewarm that streamed 40 of the corpus's $CORPUS_ROWS rows; the shipped classifier records '$pw_partial' on the same record"
else
  fail "F2 non-vacuity: expected F-A 'ok' and shipped 'FAILED-partial-scan-40-of-1000-rows', got F-A '$pw_fa_verdict' and shipped '$pw_partial'"
fi

# A SET OF PARTIAL SCANS THAT SUMS TO THE CORPUS is refused too, and this is the case a
# `rows_total >= pinned_rows` fix would have let through: 4 requests of 250 rows each sum to the
# 1000-row corpus, but nothing in the record says those quarters were DISJOINT, so the sum
# establishes nothing about coverage. The rule is `requests_ok * pinned_rows` — every completed
# request a full pass — which is what `ws0_flight_arm` requires of the MEASURED reps.
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":4,"requests_unavailable":0,"requests_error":0,"error_codes":{},"rows_total":1000}' > "$TMP/pw-sums.jsonl"
pw_sums="$(pw_flight 0 "$TMP/pw-sums.jsonl")"
if [ "$pw_sums" = "FAILED-partial-scan-1000-of-4000-rows" ]; then
  pass "OBSERVED (round12 F2): 4 requests of a QUARTER each — summing to exactly the corpus — is still REFUSED, because a sum says nothing about which quarter each request covered"
else
  fail "a set of partial scans summing to the corpus must not read as ok (got $pw_sums)"
fi

# THE ORACLE MUST HAVE BEEN CONSULTED. An absent, unreadable or uncounted pin means the ONLY
# evidence for completeness could not be read — which is a NAMED degradation, never a skip, because
# a check that silently does not run prints exactly like one that passed.
pw_nopin="$TMP/pw-nopin"; mkdir -p "$pw_nopin"
pw_badpin="$TMP/pw-badpin"; mkdir -p "$pw_badpin"; printf 'not json\n' > "$pw_badpin/session-corpus-pin.json"
pw_zeropin="$TMP/pw-zeropin"; mkdir -p "$pw_zeropin"
printf '%s\n' '{"rows":0}' > "$pw_zeropin/session-corpus-pin.json"
pw_a="$(pw_flight 0 "$TMP/pw-good.jsonl" "$pw_nopin")"
pw_b="$(pw_flight 0 "$TMP/pw-good.jsonl" "$pw_badpin")"
pw_c="$(pw_flight 0 "$TMP/pw-good.jsonl" "$pw_zeropin")"
if [ "$pw_a" = "FAILED-no-corpus-pin" ] && [ "$pw_b" = "FAILED-unreadable-corpus-pin" ] \
  && [ "$pw_c" = "FAILED-uncounted-corpus-pin" ]; then
  pass "OBSERVED (round12 F2): an ABSENT, UNREADABLE or NON-POSITIVE corpus pin each yields a NAMED degradation ($pw_a / $pw_b / $pw_c) — the completeness oracle could not be consulted, so no passing verdict is available"
else
  fail "an unusable corpus pin must be a named degradation, not a pass (absent=$pw_a, unreadable=$pw_b, zero=$pw_c)"
fi
# NON-VACUITY for those three: the SAME healthy JSONL against the REAL pin reads `ok`, so the
# refusals above are the oracle check firing and not the record being rejected for its own reasons.
if [ "$(pw_flight 0 "$TMP/pw-good.jsonl")" = "ok" ]; then
  pass "NON-VACUITY (round12 F2): the SAME JSONL against the REAL session pin reads 'ok' — so the three refusals above are the missing-oracle path, not a rejected record"
else
  fail "the healthy JSONL must read ok against the real pin, or the missing-pin cases prove nothing"
fi

# --- THE BARE-SCAN LEG, whose value was being DISCARDED ----------------------
# `scan_bench` refuses a zero-row pass itself, so exit 0 established "something was read" — and
# nothing about HOW MUCH. Its JSON carried the answer (`passes[].rows`, `rows_denominator`) and the
# leg redirected it to a file nobody read.
printf '%s\n' '{"rows_denominator":1000,"timed_scan_secs":2.0,"passes":[{"pass":0,"rows":1000,"secs":2.0}]}' > "$TMP/pws-full.json"
printf '%s\n' '{"rows_denominator":40,"timed_scan_secs":0.1,"passes":[{"pass":0,"rows":40,"secs":0.1}]}' > "$TMP/pws-partial.json"
pws_full="$(pw_scan 0 "$TMP/pws-full.json")"
pws_partial="$(pw_scan 0 "$TMP/pws-partial.json")"
if [ "$pws_full" = "ok" ] && [ "$pws_partial" = "FAILED-partial-scan-40-of-1000-rows" ]; then
  pass "OBSERVED (round12 F2): the bare-scan prewarm reads 'ok' only on a pass observing the PINNED $CORPUS_ROWS rows; an exit-0 run that scanned 40 of them records '$pws_partial' — a value the pre-fix leg discarded"
else
  fail "the bare-scan classifier must distinguish a full pass from a partial one (full=$pws_full, partial=$pws_partial)"
fi
# NON-VACUITY, MEASURED: the pre-fix bare-scan rule was `if <bench>; then ok`, and the partial run
# above exits 0 — so it recorded `ok`. Reconstructed verbatim.
pws_prefix="$(
  fake_bench() { return 0; }               # exit 0 having scanned a fraction
  st="skipped-cold-arm"
  if fake_bench; then st="ok"; else st="FAILED-exit-$?"; fi
  printf '%s' "$st"
)"
if [ "$pws_prefix" = "ok" ]; then
  pass "NON-VACUITY (round12 F2): the PRE-FIX bare-scan rule (process success alone) records '$pws_prefix' for that same 40-row run, so the label above is a measured flip and not a new function's first output"
else
  fail "F2 bare-scan non-vacuity: the pre-fix exit-status rule must yield 'ok' (got '$pws_prefix')"
fi
# ...and the bench's OWN aggregate must agree with the per-pass records it published: a
# `rows_denominator` disagreeing with the passes means this classifier validated the half of the
# artifact nobody divides by.
printf '%s\n' '{"rows_denominator":9999,"timed_scan_secs":2.0,"passes":[{"pass":0,"rows":1000,"secs":2.0}]}' > "$TMP/pws-denom.json"
printf '%s\n' '{"rows_denominator":1000,"timed_scan_secs":2.0,"passes":[]}' > "$TMP/pws-nopasses.json"
printf 'not json\n' > "$TMP/pws-bad.json"
pws_denom="$(pw_scan 0 "$TMP/pws-denom.json")"
pws_nopasses="$(pw_scan 0 "$TMP/pws-nopasses.json")"
pws_bad="$(pw_scan 0 "$TMP/pws-bad.json")"
pws_absent="$(pw_scan 0 "$TMP/pws-absent-$$.json")"
if [ "$pws_denom" = "FAILED-scan-denominator-9999-vs-1000-rows" ] \
  && [ "$pws_nopasses" = "FAILED-no-scan-passes" ] \
  && [ "$pws_bad" = "FAILED-malformed-scan-json" ] \
  && [ "$pws_absent" = "FAILED-no-scan-json" ]; then
  pass "OBSERVED (round12 F2): a bare-scan artifact whose aggregate disagrees with its passes, or which carries no passes / no parseable JSON / no file at all, is each a NAMED degradation"
else
  fail "the bare-scan classifier must name each broken artifact (denom=$pws_denom, nopasses=$pws_nopasses, bad=$pws_bad, absent=$pws_absent)"
fi
# The ARM is EXPLICIT, not sniffed: a Flight JSONL handed to the scan reader (and vice versa) must
# fail as a SHAPE problem rather than being silently read by the wrong rule, and an unrecognised arm
# is a labelled failure rather than a default to either leg.
pws_wrongarm="$(pw_scan 0 "$TMP/pw-good.jsonl")"
pws_badarm="$(python3 "$PREWARM_PY" both 0 "$TMP/pws-full.json" "$PW_SESSION")"
if [ "$pws_wrongarm" != "ok" ] && [ "$pws_badarm" = "FAILED-bad-classifier-invocation" ]; then
  pass "OBSERVED (round12 F2): the leg is named EXPLICITLY — a Flight JSONL read as a bare-scan artifact does not read ok ('$pws_wrongarm'), and an unrecognised arm is refused rather than defaulted to either leg"
else
  fail "the classifier must not accept a mismatched artifact or an unclassified arm (wrong-arm=$pws_wrongarm, bad-arm=$pws_badarm)"
fi

# --- BOTH LEGS MUST BE WIRED TO IT -------------------------------------------
# Every case above tests the classifiers. None would notice `measure_scan` keeping its `if`-on-exit,
# which is exactly the defect. Comments stripped, for the reason the F-A wiring block records: the
# leg's own prose describes what it forbids.
scan_leg=$(awk '/^measure_scan\(\)/,/^}/' "$MEASURE_LIB" | grep -v '^[[:space:]]*#')
if [ -n "$scan_leg" ] \
  && grep -q 'ws0_prewarm.py' <<<"$scan_leg" \
  && grep -q 'prewarm.json' <<<"$scan_leg" \
  && ! grep -q 'prewarm_status="ok"' <<<"$scan_leg"; then
  pass "WIRED (round12 F2): measure_scan's CODE derives its prewarm status via ws0_prewarm.py from the bench's retained JSON, and has NO literal assignment of the ok status"
else
  fail "measure_scan must classify the bench's JSON rather than trusting its exit status (code lines=$(printf '%s' "$scan_leg" | grep -c . ), literal-ok=$(grep -c 'prewarm_status="ok"' <<<"$scan_leg"))"
fi
# ...and the bare-scan leg must still FAIL CLOSED on a degraded label, which is what distinguishes
# it from the Flight arm's record-and-continue: a partly-cold bare scan reads SLOWER, shrinking
# `bare/flight` and making the 1.3x target EASIER — a degradation that can manufacture a win.
if grep -q 'FATAL: bare-scan PREWARM' <<<"$scan_leg" && grep -q 'exit 1' <<<"$scan_leg"; then
  pass "OBSERVED (round12 F2): the bare-scan leg still FAILS CLOSED on a degraded prewarm (a partly-cold bare scan can manufacture a win, unlike a degraded Flight prewarm)"
else
  fail "measure_scan must abort on a degraded prewarm rather than labelling it"
fi
# BOTH legs must pass the SESSION DIR, or the classifier cannot reach the pin and every prewarm
# would record `FAILED-no-corpus-pin` — a guard that always fires, the mirror-image broken
# instrument. Structural: the behaviour needs a real corpus and a real build.
# The SESSION DIR is the LAST argument, so the pattern anchors on the classifier call ending in a
# bare `"$OUT_DIR"` — a path UNDER it (`"$OUT_DIR/$tag…"`) is the ARTIFACT argument, and matching
# that instead would pass while the pin was unreachable.
pw_wired=$(printf '%s\n%s\n' "$scan_leg" "$flight_leg" | grep -c 'ws0_prewarm\.py.*"\$OUT_DIR")')
if [ "$pw_wired" -eq 2 ]; then
  pass "WIRED (round12 F2): BOTH legs pass \$OUT_DIR as the classifier's LAST argument, so each reads the session's own corpus pin as its completeness oracle rather than trusting a count the leg supplied"
else
  fail "both prewarm legs must pass the session dir to the classifier (matched=$pw_wired of 2)"
fi

# ==========================================================================
# #3272 ROUND 19 — A FAILED PREWARM WAS TRUSTED AS THE ARROW-VOLUME REFERENCE
# ==========================================================================
# THE FINDING, and it belongs in THIS suite rather than beside the reporter's other checks: its
# subject is whether a PREWARM'S OWN VERDICT — the thing every case above computes — is CONSULTED by
# the consumer that calls it "verified-complete".
#
# `ws0_content_volume.preflight_arrow_bytes_per_scan` globbed every `*.prewarm.jsonl` in the session
# and derived the expected Arrow byte count from it WITHOUT EVER READING THE VERDICT ON IT. The rig
# computes that verdict — the classifiers above decide whether the leg completed a full pass over
# the PINNED corpus, and `lib-measure.sh` writes it to `<tag>.prewarm.status` — and that consumer did
# not open the file. So a leg classified FAILED-partial-scan-N-of-M-rows, which the rig had ALREADY
# decided was broken and had SAID SO ON DISK, supplied the expectation for every timed rep.
#
# WHY IT STILL MATTERS AFTER ROUND 18 withdrew the verification claim and kept only the
# one-sided-shortfall refusal: it matters MORE. A short reference moves the calibration DOWN, so an
# equally-short timed rep PASSES (exactly the shortfall the surviving check exists to refuse) while
# a rep that scanned the WHOLE corpus is refused for carrying "more than a complete scan". The fix
# makes the NARROWED claim true; it does not re-widen it.

# The content-volume expectation, resolved from a session dir by the SHIPPED function — printed as a
# per-scan figure, `NONE`, or `REFUSED:<message>`. The perf dir is an argument so the same helper
# drives the shipped modules and the PRE-FIX mutant copy below.
pw_cv() { # pw_cv <perf-dir> <session-dir>
  python3 - "$1" "$2" <<'PWCV'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_content_volume import preflight_arrow_bytes_per_scan
from ws0_validate import Invalid
try:
    v = preflight_arrow_bytes_per_scan(pathlib.Path(sys.argv[2]))
except Invalid as exc:
    print(f"REFUSED:{exc}")
else:
    print("NONE" if v is None else f"{v:.0f}")
PWCV
}

# A session dir holding ONE untimed preflight, with its recorded verdict DERIVED FROM THE SHIPPED
# CLASSIFIER rather than hardcoded (`-derive-` for the label this record really earns, `-none-` for
# no status file at all, or a literal to write a verdict that DISAGREES with the record — the case
# the re-classification half exists for).
PW_CV_TAG="flight-bypass-warm-1"
pw_cv_session() { # pw_cv_session <dir> <rows_total> <bytes_total> <status> [requests_ok]
  local d="$1" rows="$2" bytes="$3" st="$4" ok="${5:-3}" jsonl
  mkdir -p "$d"
  ws0_pin_session_corpus "$d" "$TMP/corpus"
  jsonl="$d/$PW_CV_TAG.prewarm.jsonl"
  printf '%s\n' "{\"schema\":\"flight-loadgen.step/v1\",\"round\":\"prewarm\",\"requests_ok\":$ok,\"requests_error\":0,\"error_codes\":{},\"requests_unavailable\":0,\"rows_total\":$rows,\"bytes_total\":$bytes}" > "$jsonl"
  [ "$st" != "-derive-" ] || st="$(pw_flight 0 "$jsonl" "$d")"
  [ "$st" = "-none-" ] || printf '%s\n' "$st" > "$d/$PW_CV_TAG.prewarm.status"
}

# THE INPUT: a preflight that streamed 40 of the pinned 1000 rows per request, carrying HALF the
# Arrow volume a complete scan carries — and whose recorded verdict is therefore a FAILED label the
# shipped classifier itself produced.
PW_CV_HALF=$(( WS0_PREFLIGHT_BYTES_PER_SCAN / 2 ))
pw_cv_bad="$TMP/cv-failed-preflight"
pw_cv_session "$pw_cv_bad" 120 $(( 3 * PW_CV_HALF )) -derive-
pw_cv_label="$(cat "$pw_cv_bad/$PW_CV_TAG.prewarm.status")"
if [ "$pw_cv_label" = "FAILED-partial-scan-120-of-3000-rows" ]; then
  pass "round19 fixture: the failed preflight's recorded verdict is the label the SHIPPED classifier produces for it ($pw_cv_label), not one this test chose"
else
  fail "round19 fixture: expected the shipped classifier to label this preflight FAILED-partial-scan-120-of-3000-rows (got '$pw_cv_label')"
fi

# --- NON-VACUITY, MEASURED AS A FLIP ON IDENTICAL INPUT ----------------------
# The pre-fix code is reconstructed by MUTATING ONE SITE of a COPY of the shipped module — deleting
# the verdict check — and the SAME session dir is put through it. It must ACCEPT, publishing the
# short figure as the expectation every timed rep would then be measured against. Without this half,
# the refusal below proves only that a new guard returns an error; with it, the change is a measured
# flip. The premise is ASSERTED, so this case reds if it stops reproducing.
pw_cv_mutant="$TMP/cv-mutant-prefix"
mkdir -p "$pw_cv_mutant"
cp -R "$REPO_ROOT/scripts/perf/." "$pw_cv_mutant/"
rm -rf "$pw_cv_mutant/__pycache__"
python3 - "$pw_cv_mutant/ws0_content_volume.py" <<'PWMUT'
import sys
p = sys.argv[1]
s = open(p).read()
needle = "        _require_verified_preflight(path, session_dir)\n"
assert s.count(needle) == 1, f"expected exactly one call site, found {s.count(needle)}"
open(p, "w").write(s.replace(needle, ""))
PWMUT
pw_cv_prefix="$(pw_cv "$pw_cv_mutant" "$pw_cv_bad")"
if [ "$pw_cv_prefix" = "$PW_CV_HALF" ]; then
  pass "NON-VACUITY (round19), MEASURED: the PRE-FIX code ACCEPTS a preflight the rig classified '$pw_cv_label' and publishes its $pw_cv_prefix B/scan as the expectation every timed rep is measured against"
else
  fail "round19 non-vacuity: the pre-fix mutant must accept the failed preflight and return $PW_CV_HALF (got '$pw_cv_prefix')"
fi

# ...and the SHIPPED code refuses the SAME input, naming the verdict the rig recorded.
pw_cv_now="$(pw_cv "$REPO_ROOT/scripts/perf" "$pw_cv_bad")"
if [[ "$pw_cv_now" == REFUSED:* ]] && grep -q "$pw_cv_label" <<<"$pw_cv_now" \
  && grep -q 'prewarm.status' <<<"$pw_cv_now"; then
  pass "OBSERVED (round19): the shipped code REFUSES the same preflight, quoting the verdict the rig itself recorded and naming the status file it read"
else
  fail "round19: a preflight classified '$pw_cv_label' must be refused, naming that verdict (got: $pw_cv_now)"
fi
# The refusal must state the CONSEQUENCE — which direction a short reference moves the expectation —
# not merely that two strings differ. An operator cannot act on the latter.
if grep -q 'moves that expectation DOWN' <<<"$pw_cv_now" \
  && grep -q 'would PASS' <<<"$pw_cv_now"; then
  pass "OBSERVED (round19): the refusal states the consequence — a short reference moves the expectation DOWN, so an equally-short timed rep would pass the very check that exists to refuse it"
else
  fail "round19: the refusal must name the direction the bad reference moves the bar (got: $pw_cv_now)"
fi

# --- THE STATUS FILE IS NOT TAKEN ON ITS WORD --------------------------------
# A recorded `ok` beside a PARTIAL record is caught, because the completeness rule is RE-MEASURED
# here against the session pin rather than believed. This is the half a status-file check alone
# cannot do, and it is why both are required.
pw_cv_stale="$TMP/cv-stale-ok"
pw_cv_session "$pw_cv_stale" 120 $(( 3 * PW_CV_HALF )) ok
pw_cv_stale_out="$(pw_cv "$REPO_ROOT/scripts/perf" "$pw_cv_stale")"
if [[ "$pw_cv_stale_out" == REFUSED:* ]] \
  && grep -q 'RE-CLASSIFYING' <<<"$pw_cv_stale_out" \
  && grep -q 'FAILED-partial-scan-120-of-3000-rows' <<<"$pw_cv_stale_out"; then
  pass "OBSERVED (round19): a status file reading 'ok' beside a PARTIAL record is REFUSED — the completeness rule is re-measured against the session pin, so a stale or hand-edited verdict is not sufficient"
else
  fail "round19: an 'ok' status over a partial JSONL must be refused by re-classification (got: $pw_cv_stale_out)"
fi
# ...and an ABSENT status file is a REFUSAL, never a skip: a preflight whose verdict was never
# recorded is a preflight nothing verified. Its JSONL is otherwise HEALTHY, so this case can only
# fire on the missing verdict.
pw_cv_novrd="$TMP/cv-no-verdict"
pw_cv_session "$pw_cv_novrd" 3000 $(( 3 * WS0_PREFLIGHT_BYTES_PER_SCAN )) -none-
pw_cv_novrd_out="$(pw_cv "$REPO_ROOT/scripts/perf" "$pw_cv_novrd")"
if [[ "$pw_cv_novrd_out" == REFUSED:* ]] && grep -q 'NO recorded verdict' <<<"$pw_cv_novrd_out"; then
  pass "OBSERVED (round19): a preflight with NO recorded verdict is REFUSED — the status file is the only record of the loadgen's EXIT status, which no artifact on disk can reconstruct"
else
  fail "round19: an absent prewarm status must be a refusal, not a comparison run anyway (got: $pw_cv_novrd_out)"
fi

# --- THE ACCEPT DIRECTION, affirmatively -------------------------------------
# Without it every case above would be satisfied by a function that refuses everything — the
# mirror-image broken instrument. The SAME healthy JSONL, with the verdict the shipped classifier
# gives it, yields the exact per-scan figure.
pw_cv_good="$TMP/cv-healthy"
pw_cv_session "$pw_cv_good" 3000 $(( 3 * WS0_PREFLIGHT_BYTES_PER_SCAN )) -derive-
pw_cv_good_label="$(cat "$pw_cv_good/$PW_CV_TAG.prewarm.status")"
pw_cv_good_out="$(pw_cv "$REPO_ROOT/scripts/perf" "$pw_cv_good")"
if [ "$pw_cv_good_label" = "ok" ] && [ "$pw_cv_good_out" = "$WS0_PREFLIGHT_BYTES_PER_SCAN" ]; then
  pass "AFFIRMATIVE (round19): a COMPLETE preflight the shipped classifier labels 'ok' is accepted and yields exactly $pw_cv_good_out B/scan — the guard does not fire on a healthy reference"
else
  fail "round19: a healthy preflight must be accepted (label='$pw_cv_good_label', result='$pw_cv_good_out')"
fi

# --- AND THE REPORTER IS WIRED TO IT ----------------------------------------
# Every case above drives the function directly. None would notice the reporter never reaching it.
# The preflight here carries the HEALTHY per-scan volume and a PARTIAL row count, so the byte
# comparison would agree and only the VERDICT can refuse this session — which isolates what is
# under test from round 17's extent check.
pw_cv_e2e="$TMP/cv-reporter-wired"; mkdir -p "$pw_cv_e2e"
make_scan_rep "$pw_cv_e2e" warm 1 ok
make_flight_rep "$pw_cv_e2e" warm 1 ok "$GOOD_FLIGHT"
pw_cv_session "$pw_cv_e2e" 120 $(( 3 * WS0_PREFLIGHT_BYTES_PER_SCAN )) -derive-
pw_cv_e2e_out=$(run_report "$pw_cv_e2e" "$TMP/corpus"); pw_cv_e2e_rc=$?
if [ "$pw_cv_e2e_rc" -ne 0 ] \
  && grep -q 'FAILED-partial-scan-120-of-3000-rows' <<<"$pw_cv_e2e_out" \
  && [ ! -e "$pw_cv_e2e/results.json" ]; then
  pass "WIRED (round19): the REPORTER refuses a whole session whose untimed preflight the rig classified as a partial scan, and writes no results.json for it"
else
  fail "round19: the reporter must refuse a session calibrated against a failed preflight (rc=$pw_cv_e2e_rc, results.json=$([ -e "$pw_cv_e2e/results.json" ] && echo present || echo absent), out: $pw_cv_e2e_out)"
fi

# ==========================================================================
# #3272 ROUND 20 — THE CAVEAT IS IN THE HUMAN SUMMARY, BESIDE THE FIGURE AND THE VERDICT
# ==========================================================================
# THE FINDING. Round 18 withdrew the Arrow-volume verification claim and round 19 made the
# surviving calibration true — both only in `results.json` plus ONE bullet at the BOTTOM of the
# NOTES. The human summary's rows/s figures and its `[PASS]`/`[BELOW TARGET]` verdicts were printed
# with nothing beside them, so a reader who reads the numbers (which is what a summary is FOR) took
# a verdict at face value.
#
# THE CASE THIS SUITE OWNS is the COLD-ONLY session, and it belongs here because it is a PREWARM
# fact: `lib-measure.sh` skips the prewarm on the cold arm BY DESIGN — prewarming it would make
# `cold` meaningless — so a `--temp cold` session has NO PREFLIGHT and therefore NO COMPARISON
# WHATSOEVER for the payload. Not a weak one, none. And it was the case with NO human-readable text
# at all: the standing NOTES bullet was worded for the COMPARED case ("is compared against this
# session's UNTIMED PREFLIGHT"), so on exactly the session where nothing was compared, the only
# prose a reader could find said the opposite of what happened.
#
# WHY A CAVEAT AND NOT A REFUSAL, since the review offered both. Round 18 MEASURED the pinned
# `ARROW_BUFFER_DIGEST` unreachable for any `ws0-corpus-gen` corpus, and no pinned substitute exists
# — so "reject a verdict without a content oracle" rejects EVERY session, which is a rig that cannot
# report rather than a fix. The route taken is the one round 16's F2 established for the
# unobservable arm: state the absence where a reader of the numbers will see it.
pw_r20="$TMP/r20-cold-only-caveat"; mkdir -p "$pw_r20"
make_scan_rep "$pw_r20" cold 1 skipped-cold-arm
# NO `<tag>.prewarm.jsonl` is written for a cold rep by `make_flight_rep`, matching the driver —
# which is the whole subject: this session has no preflight to compare against.
make_flight_rep "$pw_r20" cold 1 skipped-cold-arm "$GOOD_FLIGHT"
ws0_pin_session_corpus "$pw_r20" "$TMP/corpus" 1 cold bypass 1
pw_r20_out=$(run_report "$pw_r20" "$TMP/corpus"); pw_r20_rc=$?
# The session is ACCEPTED — asserted, because everything below is about what a SUCCESSFUL report
# prints, and a case that silently became a rejection would stop exercising the reporting path.
if [ "$pw_r20_rc" -eq 0 ] && grep -q 'flight do_get (bypass requested)' <<<"$pw_r20_out"; then
  pass "round20: a COLD-ONLY session (no preflight, so NO payload comparison at all) is still ACCEPTED and publishes its figure"
else
  fail "round20: the cold-only session must be accepted; it is the input whose REPORTING is under test (rc=$pw_r20_rc, out: $pw_r20_out)"
fi
# ...and the caveat is BESIDE THE FIGURE. Asserted POSITIONALLY, not merely as "somewhere in the
# output": a line at the bottom of the NOTES is the shape that produced this finding, so a
# substring test over the whole summary would pass over the very defect. The flight figure line and
# the caveat must be CONSECUTIVE-ish within the arm's block — measured as: the caveat appears
# between the flight figure and the `ratio` line.
pw_r20_fig=$(grep -n 'flight do_get (bypass requested)' <<<"$pw_r20_out" | head -1 | cut -d: -f1)
pw_r20_cav=$(grep -n 'ARROW PAYLOAD VOLUME NOT COMPARED' <<<"$pw_r20_out" | head -1 | cut -d: -f1)
pw_r20_rat=$(grep -n 'ratio bare/flight' <<<"$pw_r20_out" | head -1 | cut -d: -f1)
pw_r20_vrd=$(grep -n 'verdict and the ratio above are CONDITIONAL' <<<"$pw_r20_out" | head -1 | cut -d: -f1)
if [ -n "$pw_r20_fig" ] && [ -n "$pw_r20_cav" ] && [ -n "$pw_r20_rat" ] \
  && [ "$pw_r20_cav" -gt "$pw_r20_fig" ] && [ "$pw_r20_cav" -lt "$pw_r20_rat" ]; then
  pass "round20: the NOT-COMPARED caveat sits BETWEEN the flight figure (line $pw_r20_fig) and the ratio (line $pw_r20_rat) — beside the number, not appended at the bottom"
else
  fail "round20: the caveat must be printed beside the figure (figure=$pw_r20_fig caveat=$pw_r20_cav ratio=$pw_r20_rat, out: $pw_r20_out)"
fi
# ...and BESIDE THE VERDICT, which is the stronger artifact: `[BELOW TARGET]` is the line somebody
# quotes out of the report.
if [ -n "$pw_r20_vrd" ] && [ -n "$pw_r20_rat" ] && [ "$pw_r20_vrd" -eq $(( pw_r20_rat + 1 )) ] \
  && grep -q '\[BELOW TARGET\] verdict and the ratio above are CONDITIONAL' <<<"$pw_r20_out"; then
  pass "round20: the verdict caveat is the line DIRECTLY AFTER the ratio+verdict line, and it NAMES the verdict it qualifies"
else
  fail "round20: the verdict must carry its caveat on the next line (ratio=$pw_r20_rat verdict-caveat=$pw_r20_vrd, out: $pw_r20_out)"
fi
# ...and it must STATE WHAT IS AND IS NOT ESTABLISHED, in F1's vocabulary rather than a vague hedge
# — a caveat a reader cannot act on is how one concludes the figure is probably fine. Four required
# elements: that NOTHING checked it, the concrete defect it therefore admits, the direction that
# defect biases the verdict, and where to read the mechanism.
pw_r20_missing=""
for frag in \
  "no untimed preflight in this session" \
  "NOTHING in this rig checked, not even for self-consistency" \
  "FEWER ARROW COLUMNS" \
  "look CHEAPER" \
  "NOT COMPARED AT ALL" \
  "biases this comparison TOWARD PASS" \
  "see the ARROW PAYLOAD VOLUME bullet in NOTES"; do
  grep -qF "$frag" <<<"$pw_r20_out" || pw_r20_missing="$pw_r20_missing [$frag]"
done
if [ -z "$pw_r20_missing" ]; then
  pass "round20: the caveat states the absence, the defect it admits, the FLATTERING direction (toward PASS), and where the mechanism is written"
else
  fail "round20: the caveat is missing required element(s):$pw_r20_missing"
fi
# ...and the BIAS DIRECTION IS AN ABSOLUTE, never the observed verdict interpolated back. A short
# Flight payload raises that arm's rows/s, which moves the comparison toward PASS whatever the
# verdict currently reads — so on a BELOW TARGET session the text must NOT say the bias runs toward
# BELOW TARGET, which would make the caveat reassuring on exactly the runs where it is not. Measured
# during this round: the first draft did precisely that.
if ! grep -q "toward 'BELOW TARGET'" <<<"$pw_r20_out" \
  && ! grep -q 'toward "BELOW TARGET"' <<<"$pw_r20_out"; then
  pass "round20: the bias direction is stated as an absolute (TOWARD PASS), not as the observed verdict echoed back"
else
  fail "round20: the caveat must not claim the bias runs toward the printed verdict (out: $(grep -n 'toward' <<<"$pw_r20_out"))"
fi
# ...and the NOTES bullet no longer over-claims the case it describes. On THIS session nothing was
# compared, so an unconditional "is compared against this session's UNTIMED PREFLIGHT" is false.
if grep -q 'UNTIMED PREFLIGHT WHERE ONE EXISTS' <<<"$pw_r20_out" \
  && grep -q 'has NO COMPARISON AT ALL for this property' <<<"$pw_r20_out"; then
  pass "round20: the NOTES bullet is conditional (WHERE ONE EXISTS) and names the no-preflight case, instead of asserting a comparison that did not happen"
else
  fail "round20: the NOTES bullet must not assert a comparison on a session that has none (out: $pw_r20_out)"
fi

# --- NON-VACUITY, MEASURED AS A FLIP ON THIS EXACT INPUT ----------------------
# Every assert above would pass over a reporter that prints a caveat for an unrelated reason, and
# the POSITIONAL asserts would pass over any implementation that happens to emit those lines. What
# must be measured is the PRE-FIX ABSENCE: the same cold-only session, through a reporter whose
# caveat calls are removed, printing a headline ratio and a PASS/BELOW TARGET verdict with NO
# caveat in the human summary. One-site-per-call mutation of a COPY of the shipped reporter, for
# round 14 F2's reason: a wholesale revert would be a second implementation whose fidelity is a
# claim about my re-derivation.
pw_r20_pre="$TMP/r20-prefix-tree"; rm -rf "$pw_r20_pre"; mkdir -p "$pw_r20_pre"
cp -R "$REPO_ROOT/scripts/perf/." "$pw_r20_pre/"
if python3 - "$pw_r20_pre/ws0_report.py" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
# The two call sites, each asserted to occur EXACTLY ONCE — a moved site must fail the probe loudly
# rather than leave it measuring unmutated code and reading as a pass having reverted nothing.
for needle in ('            lines += content_volume_caveat_lines(fl, f"flight/{arm}", temp)\n',
               '            lines += content_volume_verdict_caveat_lines(fl, f"flight/{arm}", verdict)\n'):
    if s.count(needle) != 1:
        raise SystemExit(f"could not locate the caveat call site to remove ({needle.strip()!r}), so "
                         "this non-vacuity probe would be measuring UNMODIFIED code")
    s = s.replace(needle, "")
p.write_text(s)
print("mutated the probe copy: neither Arrow-volume caveat is emitted into the summary")
PY
then
  pass "round20 NON-VACUITY: the mutation (both caveat call sites removed) was really applied to the probe copy"
else
  fail "round20: the pre-fix mutation could not be applied, so the probe below would measure nothing"
fi
pw_r20_pre_out=$(python3 "$pw_r20_pre/ws0_report.py" --dir "$pw_r20" --corpus "$TMP/corpus" 2>&1)
pw_r20_pre_rc=$?
# The pre-fix reporter SUCCEEDS and PUBLISHES the ratio and the verdict — that is the finding.
if [ "$pw_r20_pre_rc" -eq 0 ] \
  && grep -q 'ratio bare/flight = 2.00x' <<<"$pw_r20_pre_out" \
  && grep -q '\[BELOW TARGET\]' <<<"$pw_r20_pre_out"; then
  pass "round20 NON-VACUITY (MEASURED): the pre-fix reporter ACCEPTS the cold-only session and PUBLISHES a 2.00x headline ratio and a [BELOW TARGET] verdict"
else
  fail "round20: the pre-fix reporter must publish the ratio and verdict, else the caveat above closed nothing (rc=$pw_r20_pre_rc, out: $(head -40 <<<"$pw_r20_pre_out" | tail -12))"
fi
# ...and it does so with NO caveat ANYWHERE in the human summary — the buried-caveat defect itself,
# asserted over the WHOLE output rather than beside the figure, because "nowhere at all" is the
# stronger and the true statement for the cold-only case.
if ! grep -q 'ARROW PAYLOAD VOLUME NOT COMPARED' <<<"$pw_r20_pre_out" \
  && ! grep -q 'verdict and the ratio above are CONDITIONAL' <<<"$pw_r20_pre_out"; then
  pass "round20 NON-VACUITY: that published report carries NO Arrow-volume caveat anywhere in the human summary — the figure and verdict stood bare"
else
  fail "round20: the pre-fix summary must carry no caveat, else the flip is not a flip (out: $(grep -n 'ARROW PAYLOAD\|CONDITIONAL' <<<"$pw_r20_pre_out" | head -3))"
fi
# ...and the MUTANT IS NOT UNIFORMLY BROKEN: it still writes the record's own withdrawal into
# `results.json`, so what the probe measured is the loss of the HUMAN-READABLE caveat specifically
# and not a copy that reports nothing. This is the finding stated precisely — the warning existed
# only in nested results.json.
if python3 - "$pw_r20/results.json" <<'PY'
import json, sys
fl = [m for m in json.load(open(sys.argv[1]))["measurements"] if m["arm"].startswith("flight_")][0]
cv = fl["reps"][0]["content_volume_self_consistency"]
assert cv["bytes_total_verified_against_independent_oracle"] is False, cv
assert cv["bytes_total_checked"].startswith("NOT COMPARED"), cv
PY
then
  pass "round20 NON-VACUITY: the mutant still records the withdrawal in results.json — the probe measured the loss of the HUMAN-READABLE caveat exactly, which is the finding"
else
  fail "round20: the mutant must retain the results.json record, else it lost more than the summary caveat"
fi
# --- AND A RECORD SHAPE THE CAVEAT CANNOT DESCRIBE IS A REFUSAL, NOT A SILENT OMISSION ---------
# The caveat reader is a CLOSED grammar: an unrecognised state must not inherit a quiet branch,
# because silence beside a figure reads as VERIFIED and no session is. Driven by dropping the key
# the reader is keyed on from a copy of the SHIPPED publisher, so the reader meets a record whose
# shape it does not recognise.
pw_r20_blind="$TMP/r20-unrecognised-record"; rm -rf "$pw_r20_blind"; mkdir -p "$pw_r20_blind"
cp -R "$REPO_ROOT/scripts/perf/." "$pw_r20_blind/"
python3 - "$pw_r20_blind/ws0_flight_arm.py" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
needle = '"content_volume_self_consistency": ('
assert s.count(needle) == 1, f"expected one publication site, found {s.count(needle)}"
p.write_text(s.replace(needle, '"content_volume_UNRECOGNISED": ('))
PY
pw_r20_blind_out=$(python3 "$pw_r20_blind/ws0_report.py" --dir "$pw_r20" --corpus "$TMP/corpus" 2>&1)
pw_r20_blind_rc=$?
if [ "$pw_r20_blind_rc" -ne 0 ] \
  && grep -q 'carries no `content_volume_self_consistency` record' <<<"$pw_r20_blind_out" \
  && grep -q 'silence here would read as VERIFIED' <<<"$pw_r20_blind_out"; then
  pass "round20: a record shape the caveat cannot describe is a REFUSAL naming why silence is not an option — not a rep quietly omitted from the warning"
else
  fail "round20: an unrecognised content-volume record must be refused (rc=$pw_r20_blind_rc, out: $pw_r20_blind_out)"
fi
# --- AND THE COMPARED (WARM) BRANCH IS NOT SILENT EITHER ----------------------
# A compared session is not VERIFIED either — its reference is not independent of its subject — so a
# silent branch there could only mean "verified". The accept direction for that state, so the guard
# is neither unconditional nor half-wired: the SAME assertions over a WARM session, whose caveat
# must say SELF-CONSISTENCY rather than NOT COMPARED.
pw_r20_warm="$TMP/r20-warm-caveat"; make_session "$pw_r20_warm" "$GOOD_FLIGHT"
pw_r20_warm_out=$(run_report "$pw_r20_warm" "$TMP/corpus"); pw_r20_warm_rc=$?
if [ "$pw_r20_warm_rc" -eq 0 ] \
  && grep -q 'ARROW PAYLOAD VOLUME is a SELF-CONSISTENCY check — NOT a verification' <<<"$pw_r20_warm_out" \
  && grep -q 'SELF-CONSISTENCY-CHECKED ONLY' <<<"$pw_r20_warm_out" \
  && ! grep -q 'NOT COMPARED AT ALL' <<<"$pw_r20_warm_out"; then
  pass "round20: a WARM session's caveat says SELF-CONSISTENCY (not NOT-COMPARED) beside its figure and its verdict — both states print, neither is silent"
else
  fail "round20: the compared branch must print its own caveat (rc=$pw_r20_warm_rc, out: $pw_r20_warm_out)"
fi

# ==========================================================================
# #3272 ROUND 21 — AN UNVERIFIABLE COLD REP STILL GOT A COLD FIGURE AND A VERDICT
# ==========================================================================
# THE FINDING, and it belongs in THIS suite for the reason round 19's does: its subject is whether a
# PREWARM'S OWN VERDICT is consulted before a figure that DEPENDS on it is published.
#
# `ws0_validate.classify_prewarm` ended in a bare `return "degraded"`, so EVERY unrecognised status
# took that branch — including `unrecorded`, which is what `ws0_collect.read_prewarm` returns for a
# `<tag>.prewarm.status` file THAT IS NOT THERE AT ALL. So a COLD rep with no recorded status was
# merely CAPTIONED (a "PREWARM DEGRADED" line), and the reporter went on to publish its rows/s, its
# bare/flight ratio and its `[PASS]`/`[BELOW TARGET]` verdict — although nothing in the session
# established the rep had not been prewarmed, i.e. that it was cold at all.
#
# WHY REFUSAL AND NOT A CAPTION, when the reviewer offered both and rounds 13/16/18/20 all took the
# caption route. Those were properties with NO available oracle (the pinned ARROW_BUFFER_DIGEST is
# unreachable for any ws0-corpus-gen corpus), where refusing rejects EVERY session. This one has an
# oracle already on disk and always written: `lib-measure.sh` initialises
# `prewarm_status="skipped-cold-arm"`, enters a classifier ONLY under `[[ "$temp" == "warm" ]]`, and
# writes the file UNCONDITIONALLY — so `skipped-cold-arm` is the ONLY value a legitimate cold rep can
# carry, and there is no cold failure mode a label could honestly describe. Two directions,
# asymmetric, which is the whole argument:
#
#   * a degraded WARM rep reads SLOWER (its pages were not faulted in), so the honest label cannot
#     manufacture a win and the rep is worth keeping — `warm` still degrades.
#   * an unverified COLD rep is UNBOUNDED: if it was secretly prewarmed it reads FASTER, so the
#     unverified label flatters the very figure it is attached to.
#
# And the shape is `!= BAD` rather than `== OK` — the permissive branch every unenumerated value
# falls into — which is this issue's most-repeated defect and the reason `required-present` was
# deleted in round 14.

# A cold-only session, healthy in every other respect, whose recorded prewarm status is then made
# UNVERIFIABLE. `mode=absent` DELETES both status files, which is the `unrecorded` case; `mode=bogus`
# writes a value no classifier produces. Both must be refused, and for the SAME reason.
pw_r21_session() { # pw_r21_session <dir> <absent|bogus>
  local d="$1" mode="$2"
  mkdir -p "$d"
  make_scan_rep "$d" cold 1 skipped-cold-arm
  make_flight_rep "$d" cold 1 skipped-cold-arm "$GOOD_FLIGHT"
  ws0_pin_session_corpus "$d" "$TMP/corpus" 1 cold bypass 1
  if [ "$mode" = absent ]; then
    rm -f "$d/scan-cold-1.prewarm.status" "$d/flight-bypass-cold-1.prewarm.status"
  else
    printf 'ok-ish\n' > "$d/scan-cold-1.prewarm.status"
    printf 'ok-ish\n' > "$d/flight-bypass-cold-1.prewarm.status"
  fi
}

# --- NON-VACUITY, MEASURED AS A FLIP ON IDENTICAL INPUT ----------------------
# The pre-fix code is reconstructed by MUTATING ONE SITE of a COPY of the shipped module — replacing
# the new refusal's affirmative table lookup with the bare permissive branch it replaced — and the
# SAME session dirs are put through it. It must PUBLISH the cold figures and a verdict. The premise
# is ASSERTED (the needle must occur exactly once), so this case reds rather than silently measuring
# unmutated code if the site moves.
pw_r21_pre="$TMP/r21-prefix-tree"; rm -rf "$pw_r21_pre"; mkdir -p "$pw_r21_pre"
cp -R "$REPO_ROOT/scripts/perf/." "$pw_r21_pre/"
rm -rf "$pw_r21_pre/__pycache__"
if python3 - "$pw_r21_pre/ws0_validate.py" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
needle = "    if PREWARM_DEGRADATION_ADMITTED.get(temp) is not True:\n"
if s.count(needle) != 1:
    raise SystemExit(f"could not locate the refusal to revert ({needle.strip()!r}); this "
                     "non-vacuity probe would be measuring UNMODIFIED code")
# The pre-fix shape exactly: every unrecognised status, at every temperature, is a degradation.
head, _, tail = s.partition(needle)
p.write_text(head + '    if False:\n' + tail)
print("mutated the probe copy: an unrecognised status degrades at ANY temperature, as before")
PY
then
  pass "round21 NON-VACUITY: the mutation (the affirmative per-temperature refusal reverted to the bare permissive branch) was really applied to the probe copy"
else
  fail "round21: the pre-fix mutation could not be applied, so the probes below would measure nothing"
fi

for pw_r21_mode in absent bogus; do
  pw_r21_d="$TMP/r21-cold-$pw_r21_mode"
  pw_r21_session "$pw_r21_d" "$pw_r21_mode"
  # THE PRE-FIX BEHAVIOUR, MEASURED: accepted, cold figures published, verdict published.
  pw_r21_pre_out=$(python3 "$pw_r21_pre/ws0_report.py" --dir "$pw_r21_d" --corpus "$TMP/corpus" 2>&1)
  pw_r21_pre_rc=$?
  if [ "$pw_r21_pre_rc" -eq 0 ] \
    && grep -q '^\[COLD\]' <<<"$pw_r21_pre_out" \
    && grep -q 'ratio bare/flight = 2.00x' <<<"$pw_r21_pre_out" \
    && grep -q '\[BELOW TARGET\]' <<<"$pw_r21_pre_out" \
    && grep -q 'PREWARM DEGRADED' <<<"$pw_r21_pre_out"; then
    pass "round21 NON-VACUITY (MEASURED, $pw_r21_mode): the PRE-FIX code ACCEPTS a COLD session whose prewarm status is unverifiable and PUBLISHES its cold figures, a 2.00x ratio and a [BELOW TARGET] verdict — captioned 'PREWARM DEGRADED' and reported anyway"
  else
    fail "round21 non-vacuity ($pw_r21_mode): the pre-fix reporter must publish the cold figure and verdict, else the refusal closed nothing (rc=$pw_r21_pre_rc, out: $(grep -nE '\[COLD\]|ratio|PREWARM' <<<"$pw_r21_pre_out" | head -5))"
  fi
  # The pre-fix run above also wrote a `results.json` into this dir, carrying the unverified cold
  # figure into the machine-readable record — which is the finding's other half, asserted rather than
  # assumed. It is then REMOVED, so the shipped run's "wrote no results.json" assert below measures
  # the shipped run and not the mutant's leftover. (Measured during this round: without the removal
  # that assert failed against a file the probe itself had created — a fixture artifact reading as a
  # defect, which is the mirror of a defect reading as a pass.)
  if [ -e "$pw_r21_d/results.json" ]; then
    pass "round21 NON-VACUITY (MEASURED, $pw_r21_mode): the pre-fix run also wrote the unverified cold figure into results.json, so a downstream consumer read it too"
  else
    fail "round21 non-vacuity ($pw_r21_mode): the pre-fix run must have written a results.json (it exited 0), else this probe is not measuring a published report"
  fi
  rm -f "$pw_r21_d/results.json"
  # ...and the SHIPPED code REFUSES the SAME input, writing NO results.json — so no downstream
  # consumer can read the figure out of the record either.
  pw_r21_out=$(run_report "$pw_r21_d" "$TMP/corpus"); pw_r21_rc=$?
  if [ "$pw_r21_rc" -ne 0 ] \
    && [ ! -e "$pw_r21_d/results.json" ] \
    && ! grep -q 'ratio bare/flight' <<<"$pw_r21_out"; then
    pass "OBSERVED (round21, $pw_r21_mode): the shipped reporter REFUSES the same session, publishes no ratio and writes no results.json"
  else
    fail "round21 ($pw_r21_mode): an unverifiable cold rep must be refused (rc=$pw_r21_rc, results.json=$([ -e "$pw_r21_d/results.json" ] && echo present || echo absent), out: $pw_r21_out)"
  fi
  # ...and the refusal must be ACTIONABLE: what was unverifiable, why a COLD rep has no honest
  # degradation, the FLATTERING direction that makes it a refusal rather than a caption, and the
  # remedy. A message that says only "unexpected status" is one an operator waives.
  pw_r21_missing=""
  for frag in \
    "NO PREWARM LEG to fail" \
    "unconditionally for a cold rep" \
    "only a WARM rep runs a prewarm at all" \
    "NOTHING therefore establishes that this rep was not prewarmed" \
    "UNVERIFIED" \
    "UNBOUNDED IN DIRECTION" \
    "reported cold reads FASTER" \
    "Re-run the rep"; do
    grep -qF "$frag" <<<"$pw_r21_out" || pw_r21_missing="$pw_r21_missing [$frag]"
  done
  if [ -z "$pw_r21_missing" ]; then
    pass "round21 ($pw_r21_mode): the refusal names what was unverifiable, that a cold rep has no prewarm leg to fail, the FLATTERING direction (a secretly-warm rep reads faster), and the remedy"
  else
    fail "round21 ($pw_r21_mode): the refusal is missing required element(s):$pw_r21_missing"
  fi
done

# --- THE `unrecorded` SENTINEL IS THE ONE THAT MATTERED, NAMED EXPLICITLY -----
# Asserted at the SEAM rather than only end-to-end: `ws0_collect.read_prewarm` returns the literal
# `unrecorded` for an absent file, and that exact string must be what `classify_prewarm` refuses for a
# cold rep. A test that only deleted files would still pass if the sentinel were renamed on one side.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import pathlib, sys, tempfile
sys.path.insert(0, sys.argv[1])
from ws0_collect import read_prewarm
from ws0_validate import Invalid, classify_prewarm
with tempfile.TemporaryDirectory() as t:
    sentinel = read_prewarm(pathlib.Path(t), "scan-cold-1")   # no status file at all
assert sentinel == "unrecorded", sentinel
# WARM keeps its honest degradation...
assert classify_prewarm("warm", sentinel) == "degraded", sentinel
# ...and COLD refuses the very same value.
try:
    classify_prewarm("cold", sentinel)
except Invalid:
    pass
else:
    raise AssertionError(f"a COLD rep must refuse {sentinel!r}, not classify it")
PY
then
  pass "OBSERVED (round21): the absent-file sentinel read_prewarm really returns is 'unrecorded', it still DEGRADES on a warm rep (that direction reads slower, so it is kept and flagged), and the SAME value is REFUSED on a cold one"
else
  fail "round21: the absent-status sentinel must degrade warm and refuse cold — the two directions are not symmetric"
fi

# --- AND THE ADMISSION TABLE IS AFFIRMATIVE, NOT A DEFAULTING LOOKUP ----------
# The fix's own shape is the subject: a temperature MISSING from `PREWARM_DEGRADATION_ADMITTED` must
# REFUSE, because an unanswered question is not a permissive answer. Driven by DELETING the `warm`
# entry from a copy, which makes warm behave as the unenumerated third temperature would.
pw_r21_tbl="$TMP/r21-table-shape"; rm -rf "$pw_r21_tbl"; mkdir -p "$pw_r21_tbl"
cp -R "$REPO_ROOT/scripts/perf/." "$pw_r21_tbl/"
rm -rf "$pw_r21_tbl/__pycache__"
python3 - "$pw_r21_tbl/ws0_validate.py" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text()
# Both the entry and the key-set assertion go, so the module still imports; what remains is a
# temperature the table does not answer for.
for needle in ('    "warm": True,\n',
               "assert PREWARM_REQUIRED.keys() == PREWARM_DEGRADATION_ADMITTED.keys()\n"):
    assert s.count(needle) == 1, f"expected one occurrence of {needle.strip()!r}, found {s.count(needle)}"
    s = s.replace(needle, "")
p.write_text(s)
PY
if python3 - "$pw_r21_tbl" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import Invalid, classify_prewarm
try:
    classify_prewarm("warm", "unrecorded")
except Invalid:
    pass
else:
    raise AssertionError("a temperature absent from the admission table must REFUSE, not degrade")
PY
then
  pass "round21: the admission table is read AFFIRMATIVELY — a temperature it carries no entry for REFUSES rather than inheriting the permissive branch, so a third temperature cannot be added into a silent pass"
else
  fail "round21: an unenumerated temperature must not reach the degraded branch (the '!= BAD' shape this round exists to remove)"
fi

# ...and the two temperature tables cannot drift apart unnoticed, asserted at IMPORT rather than left
# to be discovered as a refusal on somebody's real session.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import PREWARM_DEGRADATION_ADMITTED, PREWARM_REQUIRED
assert PREWARM_REQUIRED.keys() == PREWARM_DEGRADATION_ADMITTED.keys(), (
    PREWARM_REQUIRED, PREWARM_DEGRADATION_ADMITTED)
assert PREWARM_DEGRADATION_ADMITTED["cold"] is False, PREWARM_DEGRADATION_ADMITTED
assert PREWARM_DEGRADATION_ADMITTED["warm"] is True, PREWARM_DEGRADATION_ADMITTED
PY
then
  pass "round21: both temperature tables are keyed on ONE closed set (asserted at import), and only WARM admits a degradation"
else
  fail "round21: PREWARM_REQUIRED and PREWARM_DEGRADATION_ADMITTED must share a key set, with cold=False and warm=True"
fi

# --- AND A HEALTHY COLD SESSION IS STILL ACCEPTED ----------------------------
# The accept direction, so the guard is not unconditional: the same session with its recorded
# `skipped-cold-arm` intact publishes its figure with no prewarm complaint. Round 20's case asserts
# the caveat text on this session; this one asserts the PREWARM verdict specifically.
pw_r21_ok="$TMP/r21-cold-healthy"; mkdir -p "$pw_r21_ok"
make_scan_rep "$pw_r21_ok" cold 1 skipped-cold-arm
make_flight_rep "$pw_r21_ok" cold 1 skipped-cold-arm "$GOOD_FLIGHT"
ws0_pin_session_corpus "$pw_r21_ok" "$TMP/corpus" 1 cold bypass 1
pw_r21_ok_out=$(run_report "$pw_r21_ok" "$TMP/corpus"); pw_r21_ok_rc=$?
if [ "$pw_r21_ok_rc" -eq 0 ] \
  && grep -q 'ratio bare/flight' <<<"$pw_r21_ok_out" \
  && ! grep -q 'PREWARM DEGRADED' <<<"$pw_r21_ok_out" \
  && python3 -c "
import json, sys
ms = json.load(open('$pw_r21_ok/results.json'))['measurements']
assert ms and all(m['prewarm_all_ok'] is True for m in ms), ms
"; then
  pass "AFFIRMATIVE (round21): a COLD session carrying its recorded 'skipped-cold-arm' is still accepted, publishes its ratio, and records prewarm_all_ok=true — the refusal does not fire on a legitimate cold run"
else
  fail "round21: a healthy cold session must remain accepted (rc=$pw_r21_ok_rc, out: $pw_r21_ok_out)"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# A block that silently never executes lowers the count and registers NO failure, while the gate
# reads only the exit code. Derived from the real count and set just below it — a floor far behind
# its count stops being able to see a skipped block, which is the very thing it exists to catch
# (#3326 item 3).
# RE-DERIVED BY RUNNING THIS SUITE after round 21's cases, never estimated from source lines:
# MEASURED at 57 (44 after round 20, 32 before it), so the floor is set just below that. A line count
# understates a floor because loops multiply — an earlier split on this branch understated one by 29
# that way, and round 21's own block adds THIRTEEN checks from nine written cases for exactly that
# reason (its four-check `absent`/`bogus` body runs twice).
MIN_CHECKS=55
echo
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would otherwise lower the count with no"
  echo "       failure registered, and the gate reads only the exit code (#3272)."
  exit 1
fi
if [ "$fails" -eq 0 ]; then
  echo "ws0 prewarm completeness: all $checks checks passed"
  exit 0
fi
echo "ws0 prewarm completeness: $fails of $checks check(s) FAILED"
exit 1

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
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":0,"requests_unavailable":40,"requests_error":0,"rows_total":0}' > "$TMP/pw-nothing.jsonl"
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
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":5,"requests_unavailable":0,"requests_error":0,"rows_total":0}' > "$TMP/pw-norows.jsonl"
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
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":3,"requests_unavailable":0,"requests_error":0,"rows_total":3000}' > "$TMP/pw-good.jsonl"
pw_good="$(pw_flight 0 "$TMP/pw-good.jsonl")"
# ...and a prewarm that shed SOME requests but completed at least one full scan is STILL ok:
# the prewarm's job (fault the corpus in) demonstrably happened. The MEASURED reps refuse any
# non-zero shed counter (ws0_loadgen_record.ZERO_REQUIRED_COUNTERS); conflating the two would
# make this guard fire on a healthy prewarm.
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":2,"requests_unavailable":7,"requests_error":0,"rows_total":2000}' > "$TMP/pw-shed.jsonl"
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
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":1,"requests_unavailable":0,"requests_error":0,"rows_total":40}' > "$TMP/pw-partial.jsonl"
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
printf '%s\n' '{"schema":"flight-loadgen.step/v1","round":"prewarm","requests_ok":4,"requests_unavailable":0,"requests_error":0,"rows_total":1000}' > "$TMP/pw-sums.jsonl"
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
  printf '%s\n' "{\"schema\":\"flight-loadgen.step/v1\",\"round\":\"prewarm\",\"requests_ok\":$ok,\"requests_error\":0,\"requests_unavailable\":0,\"rows_total\":$rows,\"bytes_total\":$bytes}" > "$jsonl"
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
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# A block that silently never executes lowers the count and registers NO failure, while the gate
# reads only the exit code. Derived from the real count and set just below it — a floor far behind
# its count stops being able to see a skipped block, which is the very thing it exists to catch
# (#3326 item 3).
MIN_CHECKS=32
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

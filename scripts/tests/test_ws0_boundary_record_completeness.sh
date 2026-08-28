#!/usr/bin/env bash
# Self-test for the WS0 rig's BOUNDARY-RECORD COMPLETENESS CHECK — the READER half (issue #3272,
# review round 25).
#
# Its own suite rather than more cases in `test_ws0_corpus_boundary_guards.sh`, on the campsite rule
# and on the SAME SEAM the shipped code follows. Round 22 split the rig's boundary question in two,
# one question per module, and this pair of suites mirrors it exactly:
#
#     ws0_corpus_bytes.py          are the bytes still the PINNED bytes, AT THIS BOUNDARY?  (WRITER)
#     ws0_boundary_observations.py did EVERY boundary this session owed actually happen?    (READER)
#
# The sibling suite's subject is the WRITER: it refuses a rep whose corpus changed, and §6 there
# observes that refusal reaching the RUN through the driver's own loop. This file's subject is the
# layer out — the REPORTER requiring the WRITER's record to be COMPLETE:
#
#     A RECORD THAT IS WRITTEN AND NEVER READ IS NOT EVIDENCE.
#
# # THE FINDING
#
# Round 22 wired `verify_boundary_observations` into `ws0_report.py`, and the only thing that had
# ever fed it was `ws0_pin_boundary_observations` — the fixture generator, which produces a HEALTHY
# record so that every OTHER case reaches its own subject. Nothing ever fed it a missing, a
# duplicate or an unexpected observation. So all three of its refusal directions were UNOBSERVED,
# and a checker that returned OK unconditionally would have been indistinguishable from the shipped
# one: the hardcoded `_PERF_STATE="ok"` that survived 118/118 tests, which is exactly the bar #3249
# set for this branch — not "the guard exists" but "the guard has been OBSERVED to fire".
#
# A healthy-path-only fixture is the precise shape that lets a broken guard stay green.
#
# # WHY THE REPORTER MUST REQUIRE THE RECORD AT ALL
#
# The driver writes each rep's `.round` artifact BEFORE its boundary check, so a boundary that
# REFUSES leaves a COMPLETE, REPORTABLE artifact set behind it. Restore the transiently-mutated
# component (the restore is the whole attack), invoke `ws0_report.py` directly, and every END-STATE
# check agrees: the pin matches the restored bytes, the sidecar matches, the report-time re-hash
# matches. The report PUBLISHES, saying `sha256_verified: true`, over a session in which two reps
# measured different bytes. Publishing because no failure reached the reporter is a pass derived
# from an ABSENCE — the failure was out-of-band by construction, it killed a different process.
#
# # THE FIVE DIRECTIONS, EACH DRIVEN INDEPENDENTLY
#
#   ACCEPT      a complete, correct record PUBLISHES. It comes FIRST: without it every refusal
#               below is satisfied by a checker that refuses everything, which is the same vacuity
#               with the sign flipped.
#   MISSING     a boundary the session owed, with no observation — the finding's own attack, since a
#               refused boundary records nothing by design and the absence IS the signal.
#   DUPLICATE   two observations for one (temp, rep, arm), so one label no longer names one
#               verification and the count is complete only relative to the lines in the file.
#   UNEXPECTED  an observation for a boundary this configuration never ran: a record from another
#               session completing the set with a verification that is not about these reps.
#   ABSENT/BAD  no file at all, and separately an unparseable line. Fail-closed, never "assume
#               verified" — the absent file is the attack's real end state.
#
# NON-VACUITY IS MEASURED, NOT ASSERTED, for the MISSING direction: the PRE-FIX reporter is
# reconstructed from `ws0_report.py`'s OWN TEXT with the four lines that consume the boundary record
# deleted, and what it PUBLISHES over the same session is measured. That is the same technique the
# sibling suite's §6 applies to the driver's loop, and for the same reason — what runs is the
# shipped reporter minus the call, not a hand-written imitation whose divergences would be
# unattributable.
#
# Every record under test is the SHIPPED generator's output, MUTATED — never a hand-spelled label
# set. `ws0_pin_boundary_observations` derives its labels by calling the shipped
# `expected_boundary_labels`, so a hand-written list here could pass while the real expected set
# differed, and the drift would present as EVERY boundary missing: a refusal blaming the operator
# for a test-fixture defect.
#
# Hermetic: synthetic session dirs and synthetic multi-byte component files under $TMPDIR, driven
# through the SHIPPED fixture generator and the SHIPPED reporter. No cargo, perf, sudo, taskset,
# root, corpus, network or driver invocation.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"
PERF_DIR="$REPO_ROOT/scripts/perf"

fails=0
# `checks` counts what actually RAN (incremented by pass/fail themselves, never derived from the
# file), so the minimum-check floor at the end can see a block that silently never executed.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
# python3 is a HARD REQUIREMENT of this rig (ws0-baseline.sh refuses to run without it), so its
# absence is a FAILURE and never a skip: exiting 0 here would record this gate component as SUCCESS
# with none of its checks having run, which is the vacuous green the whole issue is about.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig, so its"
  echo "       absence is a failed check and not a skip."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { chmod -R u+rwX "$TMP" 2>/dev/null; rm -rf "$TMP"; }
trap cleanup EXIT

CORPUS_ROWS=1000

# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"

# §6 proved the WRITER's verdict reaches the RUN. This section is the layer out: does the REPORTER
# require the record to be COMPLETE? Round 22's fix wired `verify_boundary_observations` into
# `ws0_report.py`, and the only thing that had ever fed it was `ws0_pin_boundary_observations` —
# which generates a HEALTHY record so every other case reaches its own subject. So all three of its
# refusal directions were unobserved, and a checker that returned OK unconditionally would have been
# indistinguishable from the shipped one: the `_PERF_STATE="ok"` shape that survived 118/118 tests,
# which is exactly what #3249 set this branch's bar against.
#
# Each direction is a DIFFERENT way the record can lie, so each is driven independently:
#
#   MISSING     — a boundary the session owed, with no observation. The finding's own attack: a
#                 REFUSED boundary records nothing by design, so the absence IS the signal.
#   DUPLICATE   — two observations for one (temp, rep, arm). One label no longer names one
#                 verification, so the count is complete only relative to the lines in the file.
#   UNEXPECTED  — an observation for a boundary this configuration never ran: a record from another
#                 session completing the set with a verification that is not about these reps.
#   ABSENT/BAD  — no file at all, and separately an unparseable line. The absent file is the limit
#                 case of MISSING and the attack's real end state, because the driver writes each
#                 `.round` artifact BEFORE its boundary check: a refusal after the final arm leaves
#                 a COMPLETE, reportable artifact set and NO observation for that boundary.
#   HEALTHY     — a complete, correct record PUBLISHES. Without this direction a checker that
#                 refused everything would look identical to a correct one.
#
# The record under test is always the SHIPPED generator's output, MUTATED — never a hand-spelled
# label set. `ws0_pin_boundary_observations` derives its labels by calling the shipped
# `expected_boundary_labels`, so a hand-written list here could pass while the real expected set
# differed, and a drift would present as EVERY boundary missing: a refusal blaming the operator for
# a test-fixture defect.

# `boundary_record_session <session> <corpus>` — a healthy TWO-REP warm/bypass session, complete
# enough to publish, carrying the shipped generator's boundary record for exactly that
# configuration.
#
# Two reps rather than one because a one-rep session cannot distinguish "one of the boundaries this
# session owed is missing" from "the record is empty": with four expected labels, a dropped line
# leaves three valid observations, so the refusal has to be attributable to the completeness check
# and not merely to an unreadable file.
boundary_record_session() {
  local d="$1" c="$2" r
  mkdir -p "$d"
  # The scan artifacts must NAME the corpus this session measured (as in §1) — a bare-scan rep whose
  # recorded `corpus` differs from the pin is refused for THAT, and MEASURED here: without this the
  # ACCEPT lane refused for a corpus mismatch, i.e. a positive control that never published, which
  # would have left every refusal below unattributable.
  # Read by `make_scan_rep` in the sourced fixture library through bash's DYNAMIC scoping, which is
  # why it looks unused here. `local` rather than an assignment or an export deliberately: it is
  # scoped to this call, so a later case cannot inherit a corpus it did not set.
  # shellcheck disable=SC2034  # consumed by make_scan_rep via dynamic scope
  local WS0_SCAN_CORPUS="$c"
  for r in 1 2; do
    make_scan_rep "$d" warm "$r" ok
    make_flight_rep "$d" warm "$r" ok "$GOOD_FLIGHT"
  done
  # The manifest and the record are stamped from ONE configuration, so the expected set the reporter
  # derives and the set the fixture recorded cannot come from two different places.
  ws0_pin_session_corpus "$d" "$c" 2 warm bypass 1
}

# `mutate_record <session> drop|duplicate|rename` — mutate the generated record in ONE of the three
# directions and print the LABEL the mutation acted on, so each assertion can require the refusal to
# NAME the boundary rather than merely to refuse.
#
# The file is located through the shipped `boundary_observations_path`, never by spelling its name:
# a test that hardcoded the filename would keep passing after the writer moved it, refusing for the
# absence of a file nothing writes any more.
mutate_record() {
  python3 - "$PERF_DIR" "$1" "$2" <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_corpus_bytes import boundary_observations_path
p = boundary_observations_path(pathlib.Path(sys.argv[2]))
lines = [l for l in p.read_text().splitlines() if l.strip()]
mode = sys.argv[3]
# Index 2 is the boundary that CLOSES the bare scan of rep 2 — chosen deliberately over the first
# or last line: a mutation at either end could be caught by an off-by-one over the expected set,
# which would pass this case while leaving the interior unchecked.
if mode == "drop":
    label = json.loads(lines.pop(2))["boundary"]
elif mode == "duplicate":
    label = json.loads(lines[2])["boundary"]
    lines.append(lines[2])
elif mode == "rename":
    # A WELL-FORMED observation for a boundary this configuration has no place for: a cold rep of
    # the merge arm, neither of which this session selected. Built by copying a real line and
    # relabelling it, so the record is refused for its LABEL and not for a malformed body.
    obs = json.loads(lines[0])
    obs["boundary"] = "cold-1-after-merge"
    label = obs["boundary"]
    lines.append(json.dumps(obs))
else:
    raise SystemExit(f"unknown mutation {mode}")
p.write_text("\n".join(lines) + "\n")
print(label)
PY
}

# --------------------------------------------------------------------------
# 7a — HEALTHY: a complete, correct record PUBLISHES
# --------------------------------------------------------------------------
# The positive control, and it comes FIRST: without it every refusal below is satisfied by a checker
# that refuses unconditionally, which is the same vacuity as one that accepts unconditionally with
# the sign flipped. It also establishes that the fixture builds a reportable session at all, so a
# refusal below is attributable to the mutation.
make_corpus "$TMP/corpus-record"
boundary_record_session "$TMP/rec-healthy" "$TMP/corpus-record"
healthy_out=$(python3 "$REPORT" --dir "$TMP/rec-healthy" --corpus "$TMP/corpus-record" 2>&1)
healthy_rc=$?
healthy_ratio=$(grep -oE 'ratio bare/flight = [0-9.]+x' <<<"$healthy_out" | head -1)
if [ "$healthy_rc" -eq 0 ] \
   && [ -n "$healthy_ratio" ] \
   && [ -e "$TMP/rec-healthy/results.json" ] \
   && grep -q 'boundaries   : 4/4 measurement boundaries VERIFIED mid-run' <<<"$healthy_out"; then
  pass "OBSERVED (round25, ACCEPT): a COMPLETE boundary record over a 2-rep session PUBLISHES ($healthy_ratio) and the summary states 4/4 boundaries verified — so the refusals below are the checker discriminating, not refusing everything"
else
  fail "round25: a complete boundary record must publish and state its coverage (rc=$healthy_rc, ratio='$healthy_ratio', out: $healthy_out)"
fi
# ...and the completeness verdict reaches results.json NAMING the boundaries it covered, not just
# counting them: a bare count is complete relative to whatever list produced it.
if python3 - "$TMP/rec-healthy/results.json" <<'PY'
import json, pathlib, sys
r = json.loads(pathlib.Path(sys.argv[1]).read_text())
c = r["boundary_observation_completeness"]
assert c["boundaries_expected"] == c["boundaries_verified"] == 4, c
assert sorted(c["expected_boundaries"]) == sorted(c["observed_boundaries"]), c
assert set(c["expected_boundaries"]) == {
    "warm-1-after-scan", "warm-1-after-bypass", "warm-2-after-scan", "warm-2-after-bypass",
}, c
assert "read from the pre-measurement session manifest" in c["expected_set_derivation"], c
PY
then
  pass "OBSERVED (round25, ACCEPT): the published record NAMES the four covered boundaries and states its expected set was DERIVED from the session manifest — the verdict is in the artifact a reader would trust, not only in the exit code"
else
  fail "round25: the accepted record must name its covered boundaries in results.json"
fi

# --------------------------------------------------------------------------
# 7b — MISSING: a boundary the session owed, with no observation
# --------------------------------------------------------------------------
boundary_record_session "$TMP/rec-missing" "$TMP/corpus-record"
dropped_label=$(mutate_record "$TMP/rec-missing" drop)
missing_out=$(python3 "$REPORT" --dir "$TMP/rec-missing" --corpus "$TMP/corpus-record" 2>&1)
missing_rc=$?
if [ "$missing_rc" -ne 0 ] \
   && grep -q 'THE MEASUREMENT-BOUNDARY RECORD IS INCOMPLETE' <<<"$missing_out" \
   && grep -q "$dropped_label" <<<"$missing_out" \
   && grep -q '1 of 4 boundaries have NO observation' <<<"$missing_out" \
   && [ ! -e "$TMP/rec-missing/results.json" ]; then
  pass "OBSERVED (round25, MISSING): one dropped observation REFUSES the report, NAMING the absent boundary ($dropped_label) and stating 1 of 4, and publishes no results.json"
else
  fail "round25: a missing observation must refuse and name the absent boundary (rc=$missing_rc, label=$dropped_label, out: $missing_out)"
fi
# ...and the refusal is attributed to the DERIVED expected set, not to a stale list: the diagnostic
# has to tell an operator whose configuration changed that the expectation followed it.
if grep -q 'The expected set is DERIVED from this session' <<<"$missing_out" \
   && grep -q '2 rep(s) x arms \[scan bypass\]' <<<"$missing_out"; then
  pass "OBSERVED (round25, MISSING): the refusal states the expected set was DERIVED from this session's own temps x reps x arms, so an operator cannot read it as a stale hand-kept list"
else
  fail "round25: the missing-observation refusal must state its derivation (out: $missing_out)"
fi

# --------------------------------------------------------------------------
# 7b (non-vacuity) — the PRE-FIX reporter PUBLISHES over the SAME session
# --------------------------------------------------------------------------
# MEASURED, not asserted. Without this, 7b passes for any reporter that refuses this session for any
# reason at all, and would keep passing after the finding stopped reproducing.
#
# The pre-fix reporter is reconstructed from `ws0_report.py`'s OWN TEXT with the four lines that
# consume the boundary record deleted — the same technique §6 uses on the driver's loop, and for the
# same reason: what runs is the shipped reporter minus the call, not a hand-written imitation whose
# divergences would be unattributable. The imports stay, which is faithful: round 22's defect was a
# record WRITTEN AND NEVER READ, not one nobody could reach.
prefix_report_dir="$TMP/prefix-reporter"
mkdir -p "$prefix_report_dir"
for sibling in "$PERF_DIR"/*; do
  [ "$(basename "$sibling")" = ws0_report.py ] || ln -s "$sibling" "$prefix_report_dir/"
done
grep -v \
  -e 'boundary_observations = verify_boundary_observations' \
  -e '"boundary_observation_completeness": boundary_observations,' \
  -e '\*boundary_observation_lines(boundary_observations),' \
  -e '\*boundary_observation_note_lines(),' \
  "$REPORT" > "$prefix_report_dir/ws0_report.py"
# ASSERTED FIRST: the re-enactment removed EXACTLY the four consuming lines. A pattern that stopped
# matching would leave the shipped reporter in place and lane B would then "prove" the pre-fix
# reporter refuses too — a false clean whose direction is toward the claim.
prefix_removed=$(( $(wc -l < "$REPORT") - $(wc -l < "$prefix_report_dir/ws0_report.py") ))
prefix_calls=$(grep -c 'verify_boundary_observations(\|boundary_observation_lines(\|boundary_observation_note_lines()' "$prefix_report_dir/ws0_report.py")
if [ "$prefix_removed" -eq 4 ] && [ "$prefix_calls" -eq 0 ]; then
  pass "PREMISE ASSERTED (round25): the pre-fix reporter is the SHIPPED file minus exactly its 4 boundary-record consuming lines, with no call site left — so lane B differs from lane A by the CALL SITE and not by their inputs"
else
  fail "round25: the pre-fix re-enactment must remove exactly the 4 consuming lines (removed=$prefix_removed, calls left=$prefix_calls)"
fi
# ...and over the SAME short record it PUBLISHES A FIGURE. This is the measurement: the number the
# pre-fix reporter emitted for a session one of whose boundaries has no verification at all.
prefix_out=$(python3 "$prefix_report_dir/ws0_report.py" --dir "$TMP/rec-missing" \
  --corpus "$TMP/corpus-record" 2>&1)
prefix_rc=$?
prefix_ratio=$(grep -oE 'ratio bare/flight = [0-9.]+x' <<<"$prefix_out" | head -1)
if [ "$prefix_rc" -eq 0 ] \
   && [ -n "$prefix_ratio" ] \
   && [ -e "$TMP/rec-missing/results.json" ] \
   && ! grep -q 'THE MEASUREMENT-BOUNDARY RECORD' <<<"$prefix_out"; then
  pass "MEASURED (round25): over the SAME session as 7b the PRE-FIX reporter PUBLISHES $prefix_ratio and emits no boundary-record diagnostic at all — the figure round 22's fix now refuses is a real number that was really printed"
else
  fail "round25: the pre-fix reporter must publish over the short record, or 7b proves nothing (rc=$prefix_rc, ratio='$prefix_ratio', out: $prefix_out)"
fi
# ...and its published report says the corpus was FULLY VERIFIED, with no completeness field. That
# is the whole finding in one artifact: every END-STATE check agrees over a session whose boundary
# record is short, so the document a reader would trust cannot distinguish this state.
prefix_ends=$(python3 - "$TMP/rec-missing/results.json" <<'PY' 2>&1
import json, pathlib, sys
r = json.loads(pathlib.Path(sys.argv[1]).read_text())
print("COMPLETENESS_FIELD", "boundary_observation_completeness" in r)
print("END_DATA_DB_SHA_VERIFIED", (r.get("corpus_identity_verification") or {}).get("sha256_verified"))
c = r.get("corpus_component_verification") or {}
print("END_COMPONENTS", c.get("components_verified_sha256"), "of", c.get("components_recorded"))
PY
)
if grep -q 'COMPLETENESS_FIELD False' <<<"$prefix_ends" \
   && grep -q 'END_DATA_DB_SHA_VERIFIED True' <<<"$prefix_ends" \
   && grep -qE 'END_COMPONENTS ([1-9][0-9]*) of \1$' <<<"$prefix_ends"; then
  pass "MEASURED (round25): the pre-fix report carries NO completeness field while claiming the digest and EVERY component verified — the short boundary record is invisible in the artifact a reader would trust, which is why the reporter has to require it"
else
  fail "round25: the pre-fix report must show fully-verified ends and no completeness field, or the finding it demonstrates has changed (out: $prefix_ends)"
fi
rm -f "$TMP/rec-missing/results.json"

# --------------------------------------------------------------------------
# 7c — DUPLICATE: two observations for one boundary
# --------------------------------------------------------------------------
# Its own direction rather than a variant of MISSING, because the count is what a duplicate defeats:
# four lines for three boundaries is complete by count and short by coverage, so a checker that only
# compared LENGTHS would accept it.
boundary_record_session "$TMP/rec-dup" "$TMP/corpus-record"
dup_label=$(mutate_record "$TMP/rec-dup" duplicate)
dup_out=$(python3 "$REPORT" --dir "$TMP/rec-dup" --corpus "$TMP/corpus-record" 2>&1)
dup_rc=$?
if [ "$dup_rc" -ne 0 ] \
   && grep -q 'THE MEASUREMENT-BOUNDARY RECORD IS AMBIGUOUS' <<<"$dup_out" \
   && grep -q "$dup_label (2x)" <<<"$dup_out" \
   && [ ! -e "$TMP/rec-dup/results.json" ]; then
  pass "OBSERVED (round25, DUPLICATE): a second observation for one boundary REFUSES the report, NAMING the duplicated boundary and its count ($dup_label, 2x), and publishes no results.json"
else
  fail "round25: a duplicated observation must refuse and name the boundary (rc=$dup_rc, label=$dup_label, out: $dup_out)"
fi
# ...and it is refused even though EVERY owed boundary is present and the record is LONGER than the
# expected set, which is what makes this a separate direction rather than a shape of MISSING: a
# checker asserting `observations >= expected` — the natural spelling of "nothing is missing" — would
# have accepted 5 lines covering 4 boundaries. MEASURED (the counts are read off the file, not
# assumed), because the whole point of the direction is that a COUNT cannot see it.
dup_lines=$(grep -c . "$TMP/rec-dup/corpus-boundary-observations.jsonl")
if [ "$dup_lines" -eq 5 ] \
   && ! grep -q 'RECORD IS INCOMPLETE' <<<"$dup_out" \
   && grep -q 'One label must name one verification' <<<"$dup_out"; then
  pass "OBSERVED (round25, DUPLICATE): the refused record is LONGER than the expected set (5 lines, 4 boundaries) with none missing — so a checker asserting observations >= expected would have accepted it, and this one requires one label to name one verification"
else
  fail "round25: the duplicate case must be refused with a surplus line count and no missing-boundary refusal (lines=$dup_lines, out: $dup_out)"
fi

# --------------------------------------------------------------------------
# 7d — UNEXPECTED: an observation this session's configuration has no place for
# --------------------------------------------------------------------------
# F1's substitution shape, one layer in: a well-formed verification from ANOTHER configuration
# sitting in this dir. It is refused rather than ignored, because ignoring it is exactly what makes
# a substituted record harmless to a count.
boundary_record_session "$TMP/rec-extra" "$TMP/corpus-record"
extra_label=$(mutate_record "$TMP/rec-extra" rename)
extra_out=$(python3 "$REPORT" --dir "$TMP/rec-extra" --corpus "$TMP/corpus-record" 2>&1)
extra_rc=$?
if [ "$extra_rc" -ne 0 ] \
   && grep -q 'OBSERVATIONS THIS SESSION HAS NO PLACE FOR' <<<"$extra_out" \
   && grep -q "$extra_label" <<<"$extra_out" \
   && [ ! -e "$TMP/rec-extra/results.json" ]; then
  pass "OBSERVED (round25, UNEXPECTED): a well-formed observation for a boundary this configuration never ran REFUSES the report, NAMING it ($extra_label), and publishes no results.json"
else
  fail "round25: an unexpected observation must refuse and name it (rc=$extra_rc, label=$extra_label, out: $extra_out)"
fi
# ...and the refusal is NOT a missing-boundary one: every owed boundary IS present here, so a
# checker that only looked for absences would have published this session.
if ! grep -q 'RECORD IS INCOMPLETE' <<<"$extra_out" \
   && grep -q 'against an expected set DERIVED from the session manifest' <<<"$extra_out"; then
  pass "OBSERVED (round25, UNEXPECTED): every owed boundary is present, so this refusal is the EXTRA one and not a missing-boundary refusal — the two directions are discriminated"
else
  fail "round25: the unexpected-observation refusal must be distinct from the missing one (out: $extra_out)"
fi

# --------------------------------------------------------------------------
# 7e — ABSENT and UNPARSEABLE: fail closed, never "assume verified"
# --------------------------------------------------------------------------
# The absent file is the attack's real end state, not a hypothetical: the driver writes each `.round`
# artifact BEFORE its boundary check, so a refusal after the final arm leaves a COMPLETE, reportable
# artifact set and NO observation. Restore the transiently-mutated component, invoke this reporter
# directly, and every end-state check agrees — which is what 7b's pre-fix lane just measured.
boundary_record_session "$TMP/rec-absent" "$TMP/corpus-record"
rm -f "$TMP/rec-absent/corpus-boundary-observations.jsonl"
absent_out=$(python3 "$REPORT" --dir "$TMP/rec-absent" --corpus "$TMP/corpus-record" 2>&1)
absent_rc=$?
if [ "$absent_rc" -ne 0 ] \
   && grep -q 'carries no corpus-boundary-observations.jsonl' <<<"$absent_out" \
   && grep -q 'REQUIRED rather than assumed verified' <<<"$absent_out" \
   && [ ! -e "$TMP/rec-absent/results.json" ]; then
  pass "OBSERVED (round25, ABSENT): a session dir with a COMPLETE artifact set and NO observations file at all is REFUSED, and the diagnostic says the record is REQUIRED rather than assumed verified"
else
  fail "round25: an absent observations file must fail closed (rc=$absent_rc, out: $absent_out)"
fi
# ...and an UNPARSEABLE line is refused too, attributed to its LINE NUMBER. A record nobody can read
# back is not evidence, and skipping the line would let a malformed record stand in for a missing
# verification — the count would then be complete over the lines that happened to parse.
boundary_record_session "$TMP/rec-malformed" "$TMP/corpus-record"
python3 - "$PERF_DIR" "$TMP/rec-malformed" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_corpus_bytes import boundary_observations_path
p = boundary_observations_path(pathlib.Path(sys.argv[2]))
lines = [l for l in p.read_text().splitlines() if l.strip()]
lines[1] = '{"boundary": "warm-1-after-bypass", truncated'
p.write_text("\n".join(lines) + "\n")
PY
malformed_out=$(python3 "$REPORT" --dir "$TMP/rec-malformed" --corpus "$TMP/corpus-record" 2>&1)
malformed_rc=$?
if [ "$malformed_rc" -ne 0 ] \
   && grep -q 'line 2 is not readable JSON' <<<"$malformed_out" \
   && grep -q 'stand in for a missing verification' <<<"$malformed_out" \
   && [ ! -e "$TMP/rec-malformed/results.json" ]; then
  pass "OBSERVED (round25, UNPARSEABLE): a malformed observation line REFUSES the report, attributed to its LINE NUMBER, rather than being skipped and leaving the count complete over the lines that happened to parse"
else
  fail "round25: an unparseable observation line must refuse, naming the line (rc=$malformed_rc, out: $malformed_out)"
fi

# ==========================================================================
# §26 — `expected_boundary_labels` REFUSES an implausible product (#3393)
# ==========================================================================
# Unbounded, this function materializes the whole temps x reps x arms product as a list, and
# `ws0_pin_boundary_observations` then materializes a SECOND list of JSON strings over it plus the
# joined string — ~three resident copies. On 2026-08-27/28 that reached 20-28 GB RSS and the kernel
# issued 14 global OOM kills across two 30 GB workers, wedging sshd (a box that cannot fork cannot
# accept an ssh session) and silently killing five sibling lane sessions.
#
# BOTH DIRECTIONS are pinned, because a cap tested only in its refusing direction is how a guard
# ships that reds correct input — the failure mode this rig has recorded repeatedly.

labels_probe() {
  python3 - "$PERF_DIR" "$1" <<'PY' 2>&1
import sys
sys.path.insert(0, sys.argv[1])
from ws0_boundary_observations import expected_boundary_labels
try:
    out = expected_boundary_labels(["warm"], ["bypass"], int(sys.argv[2]))
    print(f"BUILT {len(out)}")
except ValueError as exc:
    print(f"REFUSED {exc}")
PY
}

real_out="$(labels_probe 3)"
if [[ "$real_out" == "BUILT 6" ]]; then
  pass "OBSERVED (#3393, GREEN direction): a real configuration still builds — 1 temp x 3 reps x (scan+bypass) = 6 labels, so the cap cannot red a legitimate sweep"
else
  fail "#3393: a real configuration must still build 6 labels (got: $real_out)"
fi

absurd_out="$(labels_probe 10000000)"
if [[ "$absurd_out" == REFUSED* ]] \
   && grep -q 'implausible boundary product' <<<"$absurd_out" \
   && grep -q '3393' <<<"$absurd_out"; then
  pass "OBSERVED (#3393, RED direction): an absurd product REFUSES with the count, the cap and the issue named, instead of allocating ~28 GB and being OOM-killed"
else
  fail "#3393: an absurd product must refuse, naming the product and the cap (got: $absurd_out)"
fi

# A REFUSAL, never a truncation: a silently-truncated expected set would report every missing
# boundary as the operator's fault, which is exactly what `boundary_label`'s docstring warns about.
if grep -q 'REFUSED' <<<"$absurd_out" && ! grep -q '^BUILT' <<<"$absurd_out"; then
  pass "OBSERVED (#3393): the oversize path REFUSES rather than truncating, so a short expected set can never be blamed on the operator"
else
  fail "#3393: the oversize path must refuse, not truncate (got: $absurd_out)"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# Without `-e` a block that silently never executes LOWERS the count and registers NO failure, and
# the gate reads only the exit code — so a suite that ran 3 of its checks and passed them exits 0
# and reports SUCCESS. That is the suite-level vacuous green, one level up from the checks.
#
# The floor is DERIVED FROM THE OBSERVED COUNT — run, then recorded — never counted off the source.
MIN_CHECKS=16
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 boundary-record completeness guards: all $checks checks passed"
  exit 0
fi
echo "ws0 boundary-record completeness guards: $fails of $checks check(s) FAILED"
exit 1

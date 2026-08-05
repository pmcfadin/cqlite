#!/usr/bin/env bash
# Self-test for the WS0 reporting path's NO-FABRICATED-VALUE rule (issue #3272 AC3,
# review round 1).
#
# A third file beside test_ws0_report_guards.sh (the reporter's fail-closed paths)
# and test_ws0_cpu_pinning_guards.sh (the measurement apparatus), for one subject:
#
#     A COUNTER OR VERDICT THAT WAS NOT OBSERVED IS AN ERROR, NEVER A DEFAULT.
#
# Round 1's review found the rule stated in the module docstring and then violated
# five times in the same file, each time by an idiom that reads as harmless:
#
#   1. `int(rec.get("requests_error", 0)) > 0` — the "no failed requests" refusal
#      rested on a FABRICATED 0. A step record with no `requests_error` key was
#      reported CLEAN with the error count never measured. Its sibling
#      `requests_ok` was already `None`-checked and fatal; the two disagreed.
#   2. `block.get("prewarm_all_ok", True)` — a VERDICT-carrying key defaulted to the
#      PERMISSIVE value, so a block that lost the verdict suppressed the warning by
#      ABSENCE.
#   3. `(hi - lo) / med * 100.0 if med else 0.0` — a zero median printed
#      `spread 0.0%`, i.e. the DEGENERATE series described as the TIGHTEST one.
#   4. `scan_rps / fl_rps if fl_rps else float("inf")` — a Flight arm that measured
#      nothing published `inf x`, the most flattering possible reading of the arm
#      under study.
#   5. `rec = records[-1]` — every earlier step record in a rep's JSONL was SILENTLY
#      DROPPED, so one step's rows could be published as the whole rep while the
#      others sat unread on disk.
#
# It also covers the corpus-identity BYTE verification (round 1, B6): the recorded
# size and sha256 are now compared against the `Data.db` actually measured, so stale
# metadata beside different bytes can no longer misidentify the corpus.
#
# Every case carries the MEASURED pre-fix behaviour (`git show HEAD~:…` at the
# review commit), because per #3249 a guard never observed firing is not evidence.
#
# Hermetic: synthetic session dirs, synthetic perf CSVs, and a synthetic multi-byte
# `Data.db` whose real sha256 is computed with python3's hashlib. No cargo, perf,
# sudo, corpus, network or root.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"
# Where the shared `strip_prose` lives (#3272 round 3 nit): ONE implementation, imported
# by the assertion AND by both of its non-vacuity probes.
TESTS_DIR="$REPO_ROOT/scripts/tests"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so
# the minimum-check-count floor at the end can see a block that silently never executed
# (#3272 review round 3 nit).
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
# python3 is a HARD REQUIREMENT of this rig — ws0-baseline.sh refuses to run without
# it — so its absence is a FAILURE, not a skip. A `exit 0` here would record the gate
# component as SUCCESS with none of the checks below having run, which is the vacuous
# green this whole file exists to refuse (#3272 review, B8).
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

# --------------------------------------------------------------------------
# Fixture builders — SHARED with test_ws0_round_metadata.sh
# --------------------------------------------------------------------------
# `lib-ws0-report-fixtures.sh` holds the session-dir builders and `expect_reject`; it in turn
# sources `lib-ws0-fixtures.sh` for `perf_csv`/`ws0_make_corpus`/`make_round` (shared with
# `test_ws0_report_guards.sh` as well). A duplicated builder is the wrong thing to keep two
# copies of: `make_round` gaining a `monotonic_ns` field had to be edited in two files in round
# 3, and `make_flight_rep` gaining a DERIVED `rows_per_s` would have had to be edited in two
# more in round 4.
# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"

# The ROUND-METADATA subject — the loop order, the required fields, the integrity refusals,
# and the round-4 assertion that NO interleaving/ordering claim is made — moved to
# `scripts/tests/test_ws0_round_metadata.sh` under the campsite rule (#3272 review round 4).

GOOD_FLIGHT='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'

make_corpus "$TMP/corpus"

# ==========================================================================
# The POSITIVE CONTROL, first. Every rejection below is only evidence if the
# unperturbed fixture is ACCEPTED — a reporter hardcoded to refuse everything
# would satisfy the whole file.
# ==========================================================================
d="$TMP/happy"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "ratio bare/flight" <<<"$out"; then
  pass "positive control: the unperturbed session is ACCEPTED and prints its ratio"
else
  fail "positive control: the happy path must succeed (rc=$rc, out: $out)"
fi

# ==========================================================================
# 1 — `requests_error` is OBSERVED, never a fabricated 0
# ==========================================================================
# NON-VACUITY, measured against the pre-fix reporter (`ws0_report.py:298` at the
# review commit, `int(rec.get("requests_error", 0)) > 0`): a step record carrying
# `requests_ok` but NO `requests_error` key at all exited **0** and printed a full
# five-line report — the "no failed requests" refusal never having looked at a
# number. The identical record is refused below.
NO_ERR_KEY='{"round":"r","requests_ok":1,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/no-error-key"; make_session "$d" "$NO_ERR_KEY"
expect_reject "an ABSENT requests_error is FATAL (never a fabricated 0)" \
  "carries no \`requests_error\`" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "never a" <<<"$out" && grep -q "fabricated 0" <<<"$out"; then
  pass "the refusal states the rule it enforces (a 0 may not stand in for a counter)"
else
  fail "the requests_error refusal must state the no-fabricated-0 rule (out: $out)"
fi
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written when requests_error was not observed"
else
  fail "a refused run must not leave a results.json behind"
fi
# An UNPARSEABLE value is a corrupt counter, not a zero either.
BAD_ERR='{"round":"r","requests_ok":1,"requests_error":"none","requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/bad-error-key"; make_session "$d" "$BAD_ERR"
expect_reject "an UNPARSEABLE requests_error is FATAL (corrupt, not 0)" \
  "unparseable \`requests_error\`" "$d" "$TMP/corpus"
# And a real non-zero error count is still refused, naming it — the guard the
# fabricated default was standing in for must still work.
REAL_ERR='{"round":"r","requests_ok":1,"requests_error":4,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/real-errors"; make_session "$d" "$REAL_ERR"
expect_reject "an OBSERVED non-zero requests_error is refused, naming the count" \
  "had 4 failed request" "$d" "$TMP/corpus"
# --- R6: A NEGATIVE counter is CORRUPT, not a clean zero --------------------
# NON-VACUITY, measured against the round-1 reporter (`if errors > 0`): a step record
# carrying `requests_error: -3` exited **0** and printed the full report, with the rep
# counted as having NO failed requests. Only the POSITIVE half of "not zero" was tested,
# so every negative value inherited the PERMISSIVE branch — the same fabricated-zero
# defect as the `.get("requests_error", 0)` that branch had just replaced, arrived at from
# the other side. `-3` is not "fewer than no errors"; it is a counter that cannot have been
# validly observed.
NEG_ERR='{"round":"r","requests_ok":1,"requests_error":-3,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/neg-error"; make_session "$d" "$NEG_ERR"
expect_reject "a NEGATIVE requests_error is FATAL (pre-fix: counted as ZERO errors)" \
  "not a possible count" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "CORRUPT artifact, not a clean zero" <<<"$out" \
  && grep -q "used to be \`if errors > 0\`" <<<"$out"; then
  pass "the refusal names the shape (a `> 0` test where `== 0` was meant)"
else
  fail "the negative-counter refusal must name the defect shape (out: $out)"
fi
# The same audit on every OTHER counter comparison in the reporting path — a `> 0`/`== 0`
# where the property is "a valid observation" is one class, not one line.
NEG_ROWS='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":-1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/neg-rows"; make_session "$d" "$NEG_ROWS"
expect_reject "a NEGATIVE rows_total is FATAL (it is a denominator; == 0 alone missed it)" \
  "not a measurement" "$d" "$TMP/corpus"
NEG_RPS='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":-250000.0,"duration_s":4.0}'
d="$TMP/neg-rps"; make_session "$d" "$NEG_RPS"
expect_reject "a NEGATIVE rows_per_s is FATAL (spread() only checks the MEDIAN)" \
  "not a positive finite rate" "$d" "$TMP/corpus"
# ...and a NON-FINITE one, which would propagate into every derived figure as a printable
# number standing in for an absent measurement.
for bad in Infinity NaN; do
  d="$TMP/nonfinite-$bad"
  make_session "$d" "{\"round\":\"r\",\"requests_ok\":1,\"requests_error\":0,\"requests_unavailable\":0,\"rows_total\":1000,\"rows_per_s\":$bad,\"duration_s\":4.0}"
  expect_reject "a $bad rows_per_s is FATAL (not a rate)" \
    "not a positive finite rate" "$d" "$TMP/corpus"
done
# The BARE-SCAN denominator, both halves: a negative row count and a degenerate timing
# window. The latter used to be a `ZeroDivisionError` TRACEBACK rather than a refusal —
# the only degenerate case in the file without a stated cause, and a traceback names the
# DIVISION rather than the artifact (#3272 review round 2 nit).
d="$TMP/scan-neg-rows"; make_session "$d" "$GOOD_FLIGHT"
printf '{ "rows_denominator": -5, "timed_scan_secs": 2.0, "setup_secs": 0.5, "passes": [ { "pass": 0, "rows": 1000, "secs": 2.0 } ] }\n' > "$d/scan-warm-1.json"
expect_reject "a NEGATIVE bare-scan rows_denominator is FATAL" \
  "not a measurement" "$d" "$TMP/corpus"
d="$TMP/scan-zero-secs"; make_session "$d" "$GOOD_FLIGHT"
printf '{ "rows_denominator": 1000, "timed_scan_secs": 0.0, "setup_secs": 0.5, "passes": [ { "pass": 0, "rows": 1000, "secs": 2.0 } ] }\n' > "$d/scan-warm-1.json"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no rows/s for a measurement window that is zero" <<<"$out" \
  && ! grep -q "ZeroDivisionError\|Traceback" <<<"$out"; then
  pass "a ZERO timed_scan_secs is a NAMED refusal, not a ZeroDivisionError traceback"
else
  fail "a zero measurement window must be refused by name, not raise (rc=$rc, out: $out)"
fi
for bad in -1.0 Infinity NaN; do
  d="$TMP/scan-secs-$bad"; make_session "$d" "$GOOD_FLIGHT"
  printf '{ "rows_denominator": 1000, "timed_scan_secs": %s, "setup_secs": 0.5, "passes": [ { "pass": 0, "rows": 1000, "secs": 2.0 } ] }\n' "$bad" \
    > "$d/scan-warm-1.json"
  expect_reject "a $bad timed_scan_secs is FATAL (not a measurement window)" \
    "zero, negative, or not finite" "$d" "$TMP/corpus"
done

# An explicit ZERO is accepted: the fix is "observe it", not "reject the key".
ZERO_ERR='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/zero-errors"; make_session "$d" "$ZERO_ERR"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "an OBSERVED requests_error of 0 is accepted (the fix demands observation, not absence)"
else
  fail "requests_error=0 must be accepted (rc=$rc, out: $out)"
fi

# --- F1: THE CONFIGURATION CAME FROM THE CURRENT CLI (#3272 round 5) -------------------
# `ws0_report.py` took --reps/--temps/--arms/--scan-passes and the CPU pins from ITS OWN
# command line, tied to nothing about the session being reported. So a re-report could
# SUBSTITUTE a configuration and the report asserted the substitute had been verified.
#
# NON-VACUITY, MEASURED against this branch with only F1/F3 reverted (F2+F4 present), over a
# session dir holding a COMPLETE 3-rep measurement:
#
#     ws0_report.py --dir <3-rep session> --reps 1 --server-cpus 99,99 --client-cpus 77,77
#     => EXIT 0
#        pinning      : server 99,99 (verified physical-core siblings), client 77,77
#        reps         : 1 (median reported, spread shown)
#        bare scan …  rows=1,000 (n=1)
#
# i.e. it IGNORED two of the three measured reps and published rep 1 as the run, and printed
# CPU pins the session never used under a "verified physical-core siblings" claim.
#
# The fix READS the configuration from the pre-measurement manifest and REMOVES the flags, so
# the substitution is not merely detected — it cannot be expressed. Both halves are asserted.
d="$TMP/f1-three-rep"; mkdir -p "$d"
for _rep in 1 2 3; do
  make_scan_rep "$d" warm "$_rep" ok
  make_flight_rep "$d" warm "$_rep" ok "$GOOD_FLIGHT"
done
unset _rep
# 1. THE FLAGS NO LONGER EXIST. An accepted-but-ignored flag would be a silent lie to whoever
#    passed it, so each is an argparse error (exit 2) rather than a value that does nothing.
for gone_flag in --reps --temps --arms --scan-passes --server-cpus --client-cpus --step-duration; do
  rm -f "$d/session-corpus-pin.json"
  ws0_pin_session_corpus "$d" "$TMP/corpus" 3 warm bypass 1
  out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" "$gone_flag" 1 2>&1); rc=$?
  if [ "$rc" -eq 2 ] && grep -q 'unrecognized arguments' <<<"$out"; then
    pass "OBSERVED: \`$gone_flag\` is REJECTED by the reporter (a session property cannot be substituted)"
  else
    fail "$gone_flag must be an argparse error, not an ignored value (rc=$rc, out: $out)"
  fi
done
# 2. THE CONFIGURATION IS THE MANIFEST'S. The same session dir reports 3 reps because its
#    manifest says 3 — not because anything was passed — and `n=3` is the observable proof
#    that all three measured reps were used.
rm -f "$d/session-corpus-pin.json"
ws0_pin_session_corpus "$d" "$TMP/corpus" 3 warm bypass 1
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'n=3' <<<"$out" && grep -q 'reps         : 3' <<<"$out"; then
  pass "the reporter uses the MANIFEST's rep count (n=3 over a 3-rep session, pre-fix: n=1 was claimable)"
else
  fail "the manifest's reps must drive the report (rc=$rc, out: $out)"
fi
# ...and the summary SAYS where its configuration came from, so a reader need not infer it.
if grep -q 'READ FROM the pre-measurement session manifest' <<<"$out"; then
  pass "the summary states that the configuration was READ FROM the manifest"
else
  fail "the summary must state its configuration source (out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
src = r["configuration_source"]
assert src["manifest"].endswith("session-corpus-pin.json"), src
assert "READ FROM" in src["note"], src
assert r["reps"] == 3, r["reps"]
PY
then
  pass "results.json records the configuration SOURCE (the manifest, not this invocation)"
else
  fail "results.json must record where the configuration came from"
fi
# 3. A SESSION WITH NO RECORDED CONFIGURATION IS REFUSED, never filled in from the CLI —
#    that substitution is the finding, so there is no path that supplies it.
d="$TMP/f1-no-config"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus" 1 warm bypass 1
python3 - "$d" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
pin = json.loads(p.read_text())
pin.pop("config", None)          # a round-4-era manifest: corpus pinned, configuration not
p.write_text(json.dumps(pin, indent=1) + "\n")
PY
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records no `config`' <<<"$out"; then
  pass "OBSERVED: a manifest with NO recorded configuration is REFUSED (never supplied from argv)"
else
  fail "a config-less manifest must be refused (rc=$rc, out: $out)"
fi
# 4. A PARTIAL manifest is refused naming the absent field — a configuration half-recorded
#    cannot establish what was measured.
d="$TMP/f1-partial-config"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus" 1 warm bypass 1
python3 - "$d" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
pin = json.loads(p.read_text())
pin["config"].pop("server_cpus")
p.write_text(json.dumps(pin, indent=1) + "\n")
PY
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'INCOMPLETE — no server_cpus' <<<"$out"; then
  pass "a PARTIAL manifest configuration is refused, naming the absent field"
else
  fail "an incomplete manifest config must be refused by field name (rc=$rc, out: $out)"
fi
# 5. A HAND-EDITED manifest cannot smuggle a vacuous configuration past the reader: every
#    field goes through the SAME validator the CLI used, so `reps: 0` is still refused (that
#    was #3272 finding 5, and moving the value's source must not lose its guard).
d="$TMP/f1-vacuous-config"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus" 1 warm bypass 1
python3 - "$d" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
pin = json.loads(p.read_text())
pin["config"]["reps"] = "0"
p.write_text(json.dumps(pin, indent=1) + "\n")
PY
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'must be at least 1' <<<"$out"; then
  pass "a hand-edited \`reps: 0\` in the MANIFEST is refused (finding 5's guard survived the move)"
else
  fail "the manifest's config must go through the CLI validators (rc=$rc, out: $out)"
fi

# --- F3: CORPUS VERIFICATION CHECKED ONLY Data.db (#3272 round 5) -----------------------
# The identity records EVERY emitted component with its size and sha256, and a scan reads more
# than `Data.db` — `Index.db` above all, plus the Statistics/Summary/Filter components that
# shape how it reads. Only `Data.db` was verified, so a MODIFIED AUXILIARY COMPONENT could
# change measured behaviour while the report stated that corpus verification had succeeded.
#
# NON-VACUITY, measured with F3 reverted: rewriting `nb-1-big-Index.db` under a corpus whose
# identity records its digest left the report at EXIT 0 with its "corpus verify: size AND
# sha256 re-derived … the identity describes the bytes that were measured" line intact.
d="$TMP/f3-modified-index"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-f3"
printf 'TAMPERED-INDEX-CONTENT' > "$TMP/corpus-f3/ws0/events/nb-1-big-Index.db"
out=$(run_report "$d" "$TMP/corpus-f3"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'nb-1-big-Index.db' <<<"$out"; then
  pass "OBSERVED: a MODIFIED Index.db is REFUSED (pre-fix: exit 0 under a 'corpus verified' claim)"
else
  fail "a modified auxiliary component must be refused (rc=$rc, out: $out)"
fi
# A same-LENGTH modification is the case a size check alone cannot see.
d="$TMP/f3-same-size"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-f3b"
python3 - "$TMP/corpus-f3b" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]) / "ws0" / "events" / "nb-1-big-Statistics.db"
raw = bytearray(p.read_bytes())
raw[0] ^= 0xFF          # SAME LENGTH, different bytes
p.write_bytes(bytes(raw))
PY
expect_reject "a SAME-SIZE modification of an auxiliary component is REFUSED (digest, not size)" \
  "nb-1-big-Statistics.db hashes to" "$d" "$TMP/corpus-f3b"
# An ABSENT recorded component is refused: the identity describes a corpus that is not there.
d="$TMP/f3-missing"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-f3c"
rm -f "$TMP/corpus-f3c/ws0/events/nb-1-big-Index.db"
expect_reject "an ABSENT recorded component is REFUSED" \
  "MISSING recorded component" "$d" "$TMP/corpus-f3c"
# ...and a component PRESENT but NOT RECORDED, which is equally not the described corpus.
d="$TMP/f3-extra"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-f3d"
printf 'STRAY' > "$TMP/corpus-f3d/ws0/events/nb-1-big-Digest.crc32"
expect_reject "a component the identity does NOT describe is REFUSED" \
  "does NOT describe" "$d" "$TMP/corpus-f3d"
# An identity with NO component map cannot support the claim at all.
d="$TMP/f3-no-components"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-f3e"
python3 - "$TMP/corpus-f3e" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "corpus-identity.json"
ident = json.loads(p.read_text())
ident.pop("components", None)
p.write_text(json.dumps(ident, indent=1) + "\n")
PY
expect_reject "an identity with NO components map is REFUSED (only Data.db could be verified)" \
  "records no \`components\` map" "$d" "$TMP/corpus-f3e"
# THE ACCEPT DIRECTION, and the SCOPE is stated affirmatively: the summary reports how many
# components were verified, so a reader can tell a full verification from a partial one without
# inferring it from a flag's absence.
d="$TMP/f3-accept"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -qE 'corpus comps : all 5 recorded component\(s\) were re-stat.ed and 5 of 5 re-hashed' <<<"$out"; then
  pass "the COMPLETE component set is verified and the summary states the count (5 of 5)"
else
  fail "the summary must state the full component verification (rc=$rc, out: $out)"
fi
# ...and --skip-corpus-digest stays HONEST for the WHOLE set: it is never a silent partial
# verification, and never claims a digest it did not derive.
out=$(run_report "$d" "$TMP/corpus" --skip-corpus-digest); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'NO digest was re-derived' <<<"$out"; then
  pass "--skip-corpus-digest stamps 'NO digest was re-derived' for the whole component set"
else
  fail "the skip path must state that no component digest was observed (rc=$rc, out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))["corpus_component_verification"]
assert v["components_recorded"] == 5, v["components_recorded"]
assert v["components_verified_size"] == 5, v
assert v["components_verified_sha256"] == 0, v          # the skip run
assert set(v["components"]) >= {"nb-1-big-Data.db", "nb-1-big-Index.db"}, v
PY
then
  pass "results.json records the per-component verification record (size verified, digest not)"
else
  fail "results.json must record the component verification per component"
fi

# --- F2: THE BARE SCAN'S `passes` ARRAY WAS NEVER READ (#3272 round 5) -----------------
# The collector took the AGGREGATE `rows_denominator`/`timed_scan_secs` and never looked at
# the per-pass records beside them, although `ws0-scan-bench` computes both aggregates from
# exactly those records (`scan_bench.rs`: both are `passes.iter()…sum()`). So a truncated
# scan, a `--scan-passes` mismatch, and a pass that did not read the whole corpus were all
# invisible — and the reporter's own `--scan-passes` was recorded in results.json and
# compared against NOTHING.
#
# `scan_payload <rows_denominator> <secs> <passes-json>` builds the payload VERBATIM so each
# case perturbs exactly one property.
scan_payload() { # scan_payload <dir> <rows_denom> <secs> <passes-json>
  local d="$1"
  mkdir -p "$d"
  printf '{ "rows_denominator": %s, "timed_scan_secs": %s, "setup_secs": 0.5, "passes": %s }\n' \
    "$2" "$3" "$4" > "$d/scan-warm-1.json"
  perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
  perf_csv "$d/perf-scan-warm-1-setup.csv" 100000 200000
  printf 'ok\n' > "$d/scan-warm-1.prewarm.status"
  make_round "$d" "scan-warm-1" 1 1
  make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
}
# 1. TRUNCATED: the reporter was asked for 3 passes and the artifact carries 1, with an
#    aggregate that is SELF-CONSISTENT with the one pass present.
#    NON-VACUITY, measured against this branch at 06c295289: exit **0**, full report written.
d="$TMP/f2-truncated"; scan_payload "$d" 1000 2.0 '[ { "pass": 0, "rows": 1000, "secs": 2.0 } ]'
out=$(run_report_args "$d" "$TMP/corpus" 1 warm bypass 3); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'recorded 1 timed pass(es) but --scan-passes is 3' <<<"$out"; then
  pass "a TRUNCATED scan (1 pass, --scan-passes 3) is REFUSED (pre-fix: exit 0)"
else
  fail "a truncated scan must be refused naming both counts (rc=$rc, out: $out)"
fi
# 2. A PASS THAT DID NOT SCAN THE WHOLE CORPUS, hidden by a compensating pass: 300 + 1700
#    sums to a plausible 2000, which is exactly what an aggregate-only check cannot see.
#    NON-VACUITY: measured exit **0** pre-fix.
d="$TMP/f2-partial-pass"
scan_payload "$d" 2000 4.0 '[ { "pass": 0, "rows": 300, "secs": 2.0 }, { "pass": 1, "rows": 1700, "secs": 2.0 } ]'
out=$(run_report_args "$d" "$TMP/corpus" 1 warm bypass 2); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'pass 0 observed 300 rows' <<<"$out"; then
  pass "a PARTIAL pass hidden by a compensating one is REFUSED (pre-fix: exit 0, sum looked fine)"
else
  fail "a partial pass must be refused per pass, not per sum (rc=$rc, out: $out)"
fi
if grep -q 'Checking only the SUM cannot see this' <<<"$out"; then
  pass "the partial-pass refusal states WHY the aggregate could not catch it"
else
  fail "the partial-pass refusal must name the aggregate blind spot (out: $out)"
fi
# 3. A FORGED AGGREGATE: the passes say 1000 rows, the aggregate claims 5000. The reported
#    figure is DERIVED, so a disagreement means neither operand can be reported.
#    NON-VACUITY: measured exit **0** pre-fix, publishing the forged 5000.
d="$TMP/f2-forged-aggregate"; scan_payload "$d" 5000 2.0 '[ { "pass": 0, "rows": 1000, "secs": 2.0 } ]'
expect_reject "a FORGED rows_denominator disagreeing with its passes is REFUSED" \
  "pass record(s) sum to 1,000" "$d" "$TMP/corpus"
# ...and the same for the SECONDS aggregate, which is the divisor of every rows/s figure.
d="$TMP/f2-forged-secs"; scan_payload "$d" 1000 99.0 '[ { "pass": 0, "rows": 1000, "secs": 2.0 } ]'
expect_reject "a FORGED timed_scan_secs disagreeing with its passes is REFUSED" \
  "pass record(s) sum to" "$d" "$TMP/corpus"
# 4. An ABSENT `passes` array is an ERROR, not an unchecked aggregate.
#    NON-VACUITY: measured exit **0** pre-fix — the array was never read at all.
d="$TMP/f2-no-passes"; mkdir -p "$d"
printf '{ "rows_denominator": 1000, "timed_scan_secs": 2.0, "setup_secs": 0.5 }\n' > "$d/scan-warm-1.json"
perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-1-setup.csv" 100000 200000
printf 'ok\n' > "$d/scan-warm-1.prewarm.status"
make_round "$d" "scan-warm-1" 1 1
make_flight_rep "$d" warm 1 ok "$GOOD_FLIGHT"
expect_reject "an ABSENT bare-scan \`passes\` array is FATAL (pre-fix: never read)" \
  "carries no \`passes\` array" "$d" "$TMP/corpus"
# 5. The per-pass quantities go through the SHARED domain validators, so a bad pass value is
#    refused by the same rules as every other quantity (not by a local ad-hoc test).
for bad_pass in '{ "pass": 0, "rows": 1000 }' \
                '{ "pass": 0, "rows": 1000, "secs": 0.0 }' \
                '{ "pass": 0, "rows": -1000, "secs": 2.0 }' \
                '{ "pass": 0, "rows": 1000.5, "secs": 2.0 }' \
                '{ "pass": 0, "rows": true, "secs": 2.0 }'; do
  d="$TMP/f2-bad-pass-$(printf '%s' "$bad_pass" | md5 -q 2>/dev/null || printf '%s' "$bad_pass" | md5sum | cut -c1-8)"
  scan_payload "$d" 1000 2.0 "[ $bad_pass ]"
  out=$(run_report "$d" "$TMP/corpus"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q 'pass 0' <<<"$out"; then
    pass "a bad per-pass value ($(printf '%s' "$bad_pass" | cut -c1-42)…) is REFUSED, naming the pass"
  else
    fail "a bad per-pass value must be refused naming the pass (rc=$rc, out: $out)"
  fi
done
# 6. THE ACCEPT DIRECTION, so none of the above is a guard that reds unconditionally: a
#    MULTI-pass rep whose passes are each a full corpus scan is ACCEPTED, and the derived
#    row denominator is passes x corpus_rows.
d="$TMP/f2-multipass-ok"
scan_payload "$d" 2000 4.0 '[ { "pass": 0, "rows": 1000, "secs": 2.0 }, { "pass": 1, "rows": 1000, "secs": 2.0 } ]'
out=$(run_report_args "$d" "$TMP/corpus" 1 warm bypass 2); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'rows=2,000' <<<"$out"; then
  pass "a HEALTHY 2-pass rep is ACCEPTED and its DERIVED denominator is 2 x corpus rows"
else
  fail "a healthy multi-pass rep must be accepted (rc=$rc, out: $out)"
fi
# ...and the derivation is RECORDED in results.json, so a reader can see the aggregate was
# checked against its parts rather than trusted.
if python3 - "$d/results.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
scan = next(m for m in r["measurements"] if m["arm"] == "bare_scan")
rep = scan["reps"][0]
assert rep["passes_observed"] == 2, rep["passes_observed"]
assert rep["passes_expected"] == 2, rep["passes_expected"]
assert len(rep["passes"]) == 2, rep["passes"]
assert "DERIVED" in rep["rows_source"], rep["rows_source"]
PY
then
  pass "results.json records the per-pass records and states the rows were DERIVED"
else
  fail "results.json must record the passes the aggregate was derived from"
fi

# --- F4: THE ADMISSION-SHED COUNTER WAS COMPLETELY UNREAD (#3272 round 5) --------------
# `requests_unavailable` is the load generator's SHED counter — a request the server refused
# admission (cqlite-flight's `--max-concurrent-scans`, #2420). It was not defaulted and not
# mis-validated: it was NEVER MENTIONED in the reporting path at all, while its sibling
# `requests_error` had by then been hardened three times. So a rep measured against a server
# operating at its admission limit was reported as a clean, failure-free steady-state scan.
#
# NON-VACUITY, MEASURED against the pre-fix reporter (this branch at 06c295289): a step
# record carrying `requests_unavailable: 37` beside otherwise-healthy counters exited **0**
# and wrote a full results.json. The identical record is refused below.
SHED='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":37,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/shed-requests"; make_session "$d" "$SHED"
expect_reject "an OBSERVED non-zero requests_unavailable is refused, naming the count" \
  "recorded requests_unavailable=37" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
# The diagnostic must name the MEASUREMENT, not only the domain: an operator acts differently
# on a shed (server over its admission limit) than on an error (a broken request).
if grep -q "refused admission" <<<"$out" && grep -q "DEGRADED run" <<<"$out"; then
  pass "the shed refusal names what a shed MEANS (a degraded server, not a failed request)"
else
  fail "the requests_unavailable refusal must name the measurement (out: $out)"
fi
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written for a rep whose requests were SHED"
else
  fail "a refused shed run must not leave a results.json behind"
fi
# ABSENT is an ERROR, never a fabricated 0 — the same rule its sibling already had.
NO_SHED_KEY='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/no-shed-key"; make_session "$d" "$NO_SHED_KEY"
expect_reject "an ABSENT requests_unavailable is FATAL (never a fabricated 0)" \
  "carries no \`requests_unavailable\`" "$d" "$TMP/corpus"
# ...and the DOMAIN cases, so the shed counter is validated exactly as its sibling is rather
# than merely being present: negative, fractional and boolean all refused.
for bad_shed in '-3' '0.9' 'true' '"none"'; do
  d="$TMP/shed-domain-$(printf '%s' "$bad_shed" | tr -dc 'a-z0-9')"
  make_session "$d" "{\"round\":\"r\",\"requests_ok\":1,\"requests_error\":0,\"requests_unavailable\":$bad_shed,\"rows_total\":1000,\"rows_per_s\":250.0,\"duration_s\":4.0}"
  out=$(run_report "$d" "$TMP/corpus"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q 'requests_unavailable' <<<"$out"; then
    pass "a requests_unavailable of $bad_shed is REFUSED by the shared domain validator"
  else
    fail "requests_unavailable=$bad_shed must be refused naming the counter (rc=$rc, out: $out)"
  fi
done
# An explicit ZERO is accepted: the fix is "observe it", not "reject the key".
d="$TMP/zero-shed"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "an OBSERVED requests_unavailable of 0 is accepted (observation, not absence)"
else
  fail "requests_unavailable=0 must be accepted (rc=$rc, out: $out)"
fi

# --- F4's MECHANISM: an UNCLASSIFIED record field is REFUSED --------------------------
# Fixing the one unread counter would be the same partial fix this issue keeps finding — it
# is the SECOND counter found simply unread. So the reporter carries a CENSUS of the load
# generator's entire record surface (`RECORD_FIELD_DISPOSITION`, every field CONSUMED or
# IGNORED-with-a-reason-in-code) and REFUSES a record carrying a field nobody classified.
# A new loadgen counter therefore cannot become a third `requests_unavailable`: it arrives
# unclassified and fails the report.
UNCLASSIFIED='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0,"requests_throttled":5}'
d="$TMP/unclassified-field"; make_session "$d" "$UNCLASSIFIED"
expect_reject "an UNCLASSIFIED record field is REFUSED (naming it)" \
  "requests_throttled" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "never classified" <<<"$out" && grep -q "completely unread\|COMPLETELY UNREAD" <<<"$out"; then
  pass "the unclassified-field refusal states WHY (a counter nobody classified is unread)"
else
  fail "the unclassified-field refusal must state the rule (out: $out)"
fi
# NON-VACUITY for the census itself: the LIVE record surface is read out of the load
# generator's own source, and every field of it must be classified. This is what makes the
# census an assertion about the REAL producer rather than a list that agreed with itself when
# it was written — a field added to `StepRecord` and not classified FAILS here.
LOADGEN_RECORD="$REPO_ROOT/tools/flight-loadgen/src/record.rs"
if [ -r "$LOADGEN_RECORD" ]; then
  if python3 - "$LOADGEN_RECORD" "$REPO_ROOT/scripts/perf" <<'PY'
import pathlib, re, sys
src = pathlib.Path(sys.argv[1]).read_text()
sys.path.insert(0, sys.argv[2])
from ws0_loadgen_record import RECORD_FIELD_DISPOSITION as D
m = re.search(r"pub struct StepRecord \{(.*?)\n\}", src, re.S)
if not m:
    sys.exit("could not locate `pub struct StepRecord` — this check's SUBJECT is absent, "
             "which would print exactly like a complete census")
fields = re.findall(r"pub (\w+):", m.group(1))
if len(fields) < 15:
    sys.exit(f"only {len(fields)} StepRecord fields parsed — the census subject looks "
             "truncated, and a truncated subject reads as a complete one")
missing = [f for f in fields if f not in D]
if missing:
    sys.exit("StepRecord fields NOT classified in RECORD_FIELD_DISPOSITION: "
             + ", ".join(missing))
stale = [k for k in D if k not in fields]
if stale:
    sys.exit("RECORD_FIELD_DISPOSITION classifies fields the loadgen no longer emits: "
             + ", ".join(stale))
# every IGNORED field must carry a REASON — an empty one is a classification that says nothing
for k, (kind, why) in D.items():
    if kind == "ignored" and len(why.strip()) < 20:
        sys.exit(f"{k} is IGNORED without a substantive recorded reason")
    if kind not in ("consumed", "ignored"):
        sys.exit(f"{k} carries an unrecognised disposition {kind!r}")
print(f"census: {len(fields)}/{len(fields)} live StepRecord fields classified")
PY
  then
    pass "the record-surface CENSUS covers every field of the LIVE loadgen StepRecord"
  else
    fail "the census must classify every live StepRecord field (see output above)"
  fi
else
  fail "tools/flight-loadgen/src/record.rs is unreadable — the census oracle cannot answer"
fi

# ==========================================================================
# 2 — every step record is consumed; none is silently dropped
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `rec = records[-1]`: a rep JSONL whose
# FIRST line recorded 9 failed requests over a 37-row partial scan and whose SECOND
# was clean exited **0** and published the clean line's 250,000 rows/s — the failing
# step present on disk, unread and unmentioned. Both the error count AND the
# whole-corpus assert were evaded by a record the reporter never looked at.
d="$TMP/two-records"; mkdir -p "$d"
make_scan_rep "$d" warm 1 ok
{
  printf '%s\n' '{"round":"ramp-1","requests_ok":1,"requests_error":9,"requests_unavailable":0,"rows_total":37,"rows_per_s":99.0,"duration_s":1.0}'
  printf '%s\n' "$GOOD_FLIGHT"
} > "$d/flight-bypass-warm-1.jsonl"
perf_csv "$d/perf-flight-bypass-warm-1.csv" 8000000 16000000
printf 'ok\n' > "$d/flight-bypass-warm-1.prewarm.status"
expect_reject "a rep carrying TWO step records is REFUSED (the earlier one used to be dropped)" \
  "carries 2 step records" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "ramp-1" <<<"$out"; then
  pass "the refusal NAMES the rounds present, so the operator sees what was dropped"
else
  fail "the multi-record refusal must name the rounds it found (out: $out)"
fi

# ==========================================================================
# 3 — `prewarm_all_ok` is a computed verdict, never a permissive default
# ==========================================================================
# NON-VACUITY: pre-fix, `prewarm_warning` read `block.get("prewarm_all_ok", True)`,
# so a block missing that key returned NO warning — a verdict-carrying field
# defaulting to PASS by absence. Driven directly (the reporter always populates the
# key, which is exactly why the default was invisible): the function is extracted
# from the module and called with the key removed.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys, pathlib
sys.path.insert(0, sys.argv[1])
import ws0_report                                     # noqa: E402
from ws0_validate import Invalid                      # noqa: E402

# A block with the verdict ABSENT — the pre-fix code returned [] (no warning).
try:
    ws0_report.prewarm_warning({"prewarm": [{"rep": 1, "status": "ok"}]}, "bare-scan", "warm")
except Invalid as exc:
    assert "prewarm_all_ok" in str(exc), exc
else:
    raise SystemExit("an ABSENT prewarm_all_ok was treated as a PASS (fail-open)")

# A non-boolean verdict is likewise refused rather than truthiness-tested.
try:
    ws0_report.prewarm_warning(
        {"prewarm": [{"rep": 1, "status": "ok"}], "prewarm_all_ok": "yes"}, "bare-scan", "warm"
    )
except Invalid:
    pass
else:
    raise SystemExit("a non-boolean prewarm_all_ok was accepted")

# And the two REAL verdicts still behave: True is silent, False warns.
assert ws0_report.prewarm_warning(
    {"prewarm": [{"rep": 1, "status": "ok"}], "prewarm_all_ok": True}, "bare-scan", "warm"
) == []
warn = ws0_report.prewarm_warning(
    {"prewarm": [{"rep": 1, "status": "unrecorded"}], "prewarm_all_ok": False},
    "bare-scan", "warm",
)
assert warn and "PREWARM DEGRADED" in warn[0], warn
PY
then
  pass "OBSERVED: an ABSENT/non-boolean prewarm_all_ok is FATAL, not a silent pass"
else
  fail "prewarm_all_ok must be keyed on a computed boolean, never defaulted to True"
fi

# ==========================================================================
# 4 — a zero median is refused, not printed as `spread 0.0%`
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `spread()`: a flight rep recording
# `rows_per_s: 0.0` exited **0** and printed
#   `flight do_get (bypass)  0 rows/s [0..0, spread 0.0%] … ratio bare/flight = infx`
# — a series that measured nothing, described as the tightest possible result, with
# an `inf` ratio as the headline. BOTH halves are refused below.
ZERO_RPS='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":0.0,"duration_s":4.0}'
d="$TMP/zero-rps"; make_session "$d" "$ZERO_RPS"
# It is refused EARLIER than it used to be, and the earlier refusal is the stronger one
# (#3272 review round 2, R6 audit). `spread()` only refuses a non-positive MEDIAN, so one
# impossible rep among three left the median positive and published a spread computed over
# a rate that cannot exist. The per-REP check catches that; `spread()`'s median check is
# retained below as the series-level statement of the same rule.
expect_reject "a rep recording ZERO rows/s is REFUSED at the REP, before any median" \
  "not a positive finite rate" "$d" "$TMP/corpus"
# The SERIES-level half, still live: a series whose reps are individually plausible but
# whose median is non-positive cannot arise from the per-rep check alone, so `spread()` is
# driven directly rather than through an artifact it can no longer see.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_collect import spread
from ws0_validate import Invalid
for series in ([0.0], [0.0, 0.0, 0.0], [-1.0, 0.0, 1.0]):
    try:
        spread(series)
    except Invalid as e:
        assert "non-positive median" in str(e), str(e)
    else:
        raise SystemExit(f"spread({series}) must refuse a non-positive median")
try:
    spread([])
except Invalid as e:
    assert "nothing was observed" in str(e), str(e)
else:
    raise SystemExit("spread([]) must refuse an empty series")
# ...and the ACCEPT direction, so it is not a function that refuses everything.
got = spread([100.0, 200.0, 300.0])
assert got["median"] == 200.0 and got["n"] == 3, got
PY
then
  pass "spread() REFUSES a non-positive median and an empty series, and accepts a real one"
else
  fail "spread() must refuse a non-positive median (series-level) and accept a real series"
fi
out=$(run_report "$d" "$TMP/corpus")
# The report LINE must be absent — asserted on the line's own shape, not on the
# phrase, because the refusal text quotes the phrase it is refusing.
if ! grep -q 'rows/s  \[0\.\.0' <<<"$out" && ! grep -q 'bare/flight = infx' <<<"$out"; then
  pass "neither a 0-rows/s figure nor an 'inf' ratio LINE is printed for the degenerate series"
else
  fail "the degenerate series must not be printed at all (out: $out)"
fi

# ==========================================================================
# 4b — the FLIGHT THROUGHPUT is DERIVED, and `duration_s` is REQUIRED (round 4)
# ==========================================================================
# NON-VACUITY, measured against the pre-round-4 collector. `rows_per_s` was read straight from
# the artifact and `duration_s` was NEVER VALIDATED — it reached results.json through a bare
# `rec.get("duration_s")`. So a record with entirely plausible rows/request counters and an
# ARBITRARY throughput produced a SUCCESSFUL report, and the headline figure was the one field
# nothing cross-checked. MEASURED on that version: the fixture below (1000 rows over 4.0 s,
# claiming 9,999,999 rows/s) exited 0 and printed `9,999,999 rows/s`.
#
# The loadgen's own invariant is `rows_per_s == rows_total / duration_s`
# (tools/flight-loadgen/src/record.rs, `per_s(self.rows_total)`), so the reporter now DERIVES
# the rate — a derived value cannot be forged — and cross-checks the recorded one against it.
FORGED_RPS='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":9999999.0,"duration_s":4.0}'
d="$TMP/forged-rps"; make_session "$d" "$FORGED_RPS"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "give rows_total/duration_s = 1000/4.0 = 250.0" <<<"$out"; then
  pass "OBSERVED: a FORGED rows_per_s is REFUSED, naming the derivation it contradicts (pre-fix: exit 0, printed 9,999,999 rows/s)"
else
  fail "a forged throughput must be refused (rc=$rc, out: $out)"
fi
# ...and the refusal must say the figure is DERIVED, so nobody restores the trusted read.
if grep -q "The reported figure is the DERIVED one" <<<"$out"; then
  pass "the refusal states that the reported throughput is DERIVED, not read"
else
  fail "the refusal must name the derivation (out: $out)"
fi
# NOTHING is printed for it: a forged rate must not appear anywhere in the transcript.
if ! grep -q '9,999,999 rows/s' <<<"$out"; then
  pass "the forged rate is NOT printed as a figure (it was, pre-fix)"
else
  fail "the forged rate must not be printed (out: $out)"
fi
# `duration_s` is now REQUIRED and validated in its own right — it is the DIVISOR of the
# reported figure, and every domain violation is a NAMED refusal rather than a `None` in
# results.json.
NO_DUR='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0}'
d="$TMP/no-duration"; make_session "$d" "$NO_DUR"
expect_reject "an ABSENT duration_s is FATAL (it used to reach results.json as None)" \
  "duration_s" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "It is the DIVISOR of this rep's throughput" <<<"$out"; then
  pass "the duration_s refusal states what the field IS (the divisor of the derived figure)"
else
  fail "the duration_s refusal must name its role (out: $out)"
fi
for bad_dur in 0 0.0 -4.0 '"4s"' 'null'; do
  d="$TMP/dur-$(printf '%s' "$bad_dur" | tr -dc 'a-zA-Z0-9')x"
  make_session "$d" "{\"round\":\"r\",\"requests_ok\":1,\"requests_error\":0,\"requests_unavailable\":0,\"rows_total\":1000,\"rows_per_s\":250.0,\"duration_s\":$bad_dur}"
  expect_reject "duration_s=$bad_dur is REFUSED (it was UNVALIDATED entirely)" \
    "duration_s" "$d" "$TMP/corpus"
done
# THE ACCEPT DIRECTION, affirmatively: a self-consistent record is accepted, the DERIVED rate
# is what results.json reports, and the recorded one is kept beside it so a reader can see the
# two agreed. Without this the derivation could be a function that refuses everything.
d="$TMP/derived-ok"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
fl = [m for m in json.load(open(sys.argv[1]))["measurements"]
      if m["arm"].startswith("flight_")][0]
rep = fl["reps"][0]
assert rep["duration_s"] == 4.0, rep
assert rep["rows_per_sec"] == 1000 / 4.0, rep
assert rep["rows_per_sec_recorded"] == 250.0, rep
assert "DERIVED as rows_total/duration_s" in rep["rows_per_sec_source"], rep
PY
then
  pass "OBSERVED: a self-consistent record is ACCEPTED and results.json reports the DERIVED rate (source named)"
else
  fail "the derived-throughput accept direction must record the derivation (rc=$rc, out: $out)"
fi
# STRUCTURAL, over EVERY reporting-path file's EXECUTABLE source (#3272 review round 2
# nit). It used to parse `ws0_report.py` alone — but every fail-closed DECISION now lives
# in `ws0_validate.py`, the collection in `ws0_collect.py` and the round metadata in
# `ws0_rounds.py`, so a new `.get(k, 0)` in any of them was outside the scan's subject
# entirely. The file list is DISCOVERED from the directory, not enumerated: a fifth module
# would otherwise be unscanned the moment someone adds it, which is how this hole opened.
#
# DOCSTRINGS and DIAGNOSTIC STRINGS are stripped via `ast` before the scan, because the
# comments explaining each fix — and the diagnostics that name the idiom they refuse ("the
# check used to be `if errors > 0`") — necessarily quote what they removed. A literal grep
# over the raw file reds on its own documentation, and the obvious "fix" for that would be
# to stop documenting it.
#
# A "diagnostic string" is one that appears as an ARGUMENT to `raise`/`Invalid(...)` or is
# concatenated into one — i.e. prose. It is NOT every string constant: blanking those
# rewrites `rec.get('cycles', 0)` to `rec.get('', 0)` and makes THE WHOLE SCAN VACUOUS,
# which is what a first pass at this did. That was caught by planting a real idiom in a
# probe module and observing the scan stay green — the check below is the permanent version
# of that probe, because a vacuous scan is textually identical to a passing one.
if python3 - "$REPO_ROOT/scripts/perf" "$TESTS_DIR" <<'PY'
import pathlib, sys

# `strip_prose` is IMPORTED from scripts/tests/ws0_prose_strip.py, never re-implemented
# (#3272 review round 3 nit). It used to be THREE inline copies — the assertion, its
# non-vacuity probe, and the strip's own test — which is three things to keep in sync, and
# a non-vacuity probe testing a DIFFERENT strip than the assertion uses proves nothing
# about the assertion. The tests dir is passed as the LAST argv rather than derived from
# `__file__`, which a `python3 - <<EOF` heredoc does not have.
sys.path.insert(0, sys.argv[-1])
from ws0_prose_strip import strip_prose                 # noqa: E402


subject = sorted(
    p for p in pathlib.Path(sys.argv[1]).glob("ws0_*.py")
)
if len(subject) < 4:
    raise SystemExit(
        f"the banned-idiom scan found only {len(subject)} reporting-path module(s)"
        f" ({[p.name for p in subject]}); its subject is the whole set, and a subject"
        " smaller than the set is how ws0_validate.py went unscanned"
    )

# Each permissive-default idiom review found, as it appears in the executable source.
# `ast.unparse` normalises quoting to single quotes.
banned = {
    "if med else 0.0": "spread() still defaults a zero-median spread to 0.0",
    "float('inf')": "a figure still falls back to inf",
    "get('requests_error', 0)": "requests_error still reads through a defaulting get",
    "get('prewarm_all_ok', True)": "prewarm_all_ok still defaults to the PERMISSIVE value",
    "records[-1]": "the reporter still consumes only the LAST step record",
    "get('cycles', 0)": "a perf counter still reads through a defaulting get",
    "get('rows', 0)": "a row count still reads through a defaulting get",
    "if errors > 0": "a counter is tested for the BAD half only; a negative value inherits the permissive branch (R6)",
}
hits = []
for path in subject:
    code = strip_prose(path.read_text())
    hits += [f"{path.name}: {why}" for idiom, why in banned.items() if idiom in code]
if hits:
    raise SystemExit("; ".join(hits))
print(f"scanned {len(subject)} module(s): {', '.join(p.name for p in subject)}", file=sys.stderr)
PY
then
  pass "STRUCTURAL: no permissive-default idiom in ANY reporting-path module (subject discovered)"
else
  fail "a permissive-default idiom is still present in the reporting path"
fi
# NON-VACUITY for that scan, in BOTH of the ways it can go vacuous. Either alone leaves a
# hole, and BOTH have actually happened while writing this:
#
#  (a) SUBJECT TOO SMALL — the scan parsed `ws0_report.py` alone while every fail-closed
#      decision moved to `ws0_validate.py`, so an idiom planted there was invisible.
#  (b) STRIP TOO BROAD — blanking every string constant (a first attempt at keeping the
#      scan off its own documentation) rewrites `rec.get('cycles', 0)` to `rec.get('', 0)`,
#      so NOTHING is ever detected. Observed by planting a real idiom in a probe module and
#      watching the suite stay green.
#
# Both are checked by planting the idiom in `ws0_validate.py` — the file (a) never read —
# and requiring the scan to FIND it, using the SAME `strip_prose` the assertion uses.
if ! python3 - "$TMP/scan-subject" "$REPO_ROOT/scripts/perf" "$TESTS_DIR" <<'PY'
import pathlib, shutil, sys

tmp = pathlib.Path(sys.argv[1]); tmp.mkdir(parents=True, exist_ok=True)
for p in pathlib.Path(sys.argv[2]).glob("ws0_*.py"):
    shutil.copy(p, tmp / p.name)
# Plant the idiom in ws0_validate.py, NOT the reporter — the file the old scan never read.
target = tmp / "ws0_validate.py"
target.write_text(target.read_text() + "\n\ndef _planted(rec):\n    return rec.get('cycles', 0)\n")

# `strip_prose` is IMPORTED from scripts/tests/ws0_prose_strip.py, never re-implemented
# (#3272 review round 3 nit). It used to be THREE inline copies — the assertion, its
# non-vacuity probe, and the strip's own test — which is three things to keep in sync, and
# a non-vacuity probe testing a DIFFERENT strip than the assertion uses proves nothing
# about the assertion. The tests dir is passed as the LAST argv rather than derived from
# `__file__`, which a `python3 - <<EOF` heredoc does not have.
sys.path.insert(0, sys.argv[-1])
from ws0_prose_strip import strip_prose                 # noqa: E402

hits = [p.name for p in sorted(tmp.glob("ws0_*.py"))
        if "get('cycles', 0)" in strip_prose(p.read_text())]
if hits:
    raise SystemExit(f"planted idiom found in {hits} (expected — the scan is not vacuous)")
PY
then
  pass "NON-VACUITY: the scan CATCHES an idiom planted in ws0_validate.py (subject + strip both sound)"
else
  fail "the banned-idiom scan must catch an idiom outside ws0_report.py — its subject is too small, or its prose-strip is too broad and blanked the idiom"
fi
# And the STRIP must still do its job: a shipped module whose DIAGNOSTIC quotes a banned
# idiom must NOT red. `ws0_collect.py` genuinely contains the sentence "the check used to be
# `if errors > 0`" inside an `Invalid(...)` message, so this is a live case, not a
# hypothetical — without the strip the scan reds on its own documentation and the reflex fix
# is to stop documenting.
if python3 - "$REPO_ROOT/scripts/perf/ws0_collect.py" "$TESTS_DIR" <<'PY'
import pathlib, sys

path = pathlib.Path(sys.argv[1])
raw = path.read_text()
assert "if errors > 0" in raw, "this case needs a module whose PROSE quotes a banned idiom"

# `strip_prose` is IMPORTED from scripts/tests/ws0_prose_strip.py, never re-implemented
# (#3272 review round 3 nit). It used to be THREE inline copies — the assertion, its
# non-vacuity probe, and the strip's own test — which is three things to keep in sync, and
# a non-vacuity probe testing a DIFFERENT strip than the assertion uses proves nothing
# about the assertion. The tests dir is passed as the LAST argv rather than derived from
# `__file__`, which a `python3 - <<EOF` heredoc does not have.
sys.path.insert(0, sys.argv[-1])
from ws0_prose_strip import strip_prose                 # noqa: E402

stripped = strip_prose(raw)
if "if errors > 0" in stripped:
    raise SystemExit("the prose-strip did not remove a DIAGNOSTIC quoting the idiom")
# ...and it must NOT have blanked an argument literal, which is (b).
if "'skipped-cold-arm'" in raw and "'skipped-cold-arm'" not in stripped:
    raise SystemExit("the prose-strip blanked a non-prose literal — the scan would be vacuous")
PY
then
  pass "the prose-strip removes DIAGNOSTICS quoting an idiom but keeps argument literals"
else
  fail "the prose-strip must exempt diagnostics WITHOUT blanking argument literals (else the scan is vacuous)"
fi
# --- THE STRIP MUST NOT BLANK AN F-STRING'S INTERPOLATION (#3272 round 3 nit) --
# The round-2 defect, repeated ONE LEVEL IN. Round 2 fixed "blanking every string constant
# makes the scan vacuous" by restricting the blanking to constants REACHABLE FROM A `raise`
# — and `ast.walk` reaches INTO an f-string's `{...}` expressions, so
#
#     raise Invalid(f"bad: {rec.get('cycles', 0)} is wrong")
#
# became `raise Invalid(f"{rec.get('', 0)}")`: an idiom written inside a diagnostic's
# interpolation was HIDDEN FROM THE SCAN. MEASURED against the pre-fix strip, that exact
# input produced exactly that output.
#
# Both halves are asserted over ONE input, so the fix cannot be "stop blanking f-strings"
# (which would red on the shipped diagnostics) or "blank nothing" (which is vacuity again):
# the LITERAL text of the f-string must go, the INTERPOLATED expression must stay.
if python3 - "$TESTS_DIR" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_prose_strip import strip_prose                 # noqa: E402

src = (
    "def f(rec):\n"
    "    if rec.get('cycles', 0) < 1:\n"
    "        raise Invalid(f\"the check used to be `if errors > 0`:"
    " {rec.get('cycles', 0)} is wrong\")\n"
    "    return 1\n"
)
out = strip_prose(src)
# (1) the INTERPOLATED expression SURVIVES — this is the defect. Pre-fix it read
#     `rec.get('', 0)` and the scan saw no idiom.
if "rec.get('cycles', 0)" not in out:
    raise SystemExit(
        "the strip blanked an f-string's INTERPOLATED EXPRESSION, so an idiom written"
        f" inside a diagnostic is invisible to the scan. Got: {out!r}"
    )
# (2) the LITERAL text of the same f-string is still REMOVED, or the strip would red on
#     every shipped diagnostic that quotes the idiom it refuses.
if "if errors > 0" in out:
    raise SystemExit(
        f"the strip left an f-string's LITERAL prose, so the scan reds on its own"
        f" documentation. Got: {out!r}"
    )
# (3) the guard-condition idiom outside the raise is untouched (the ordinary case).
if "rec.get('cycles', 0) < 1" not in out:
    raise SystemExit(f"the strip damaged executable source outside the raise: {out!r}")
PY
then
  pass "the prose-strip keeps an f-string's INTERPOLATED expression while removing its literal text (round-3 nit)"
else
  fail "the strip must not blank an f-string's interpolation — an idiom inside a diagnostic would be hidden"
fi
# ...and there is exactly ONE implementation, so the assertion and both non-vacuity probes
# cannot test different strips. Structural, because a second copy is the failure mode.
if [ "$(grep -c '^def strip_prose' "$TESTS_DIR/ws0_prose_strip.py")" -eq 1 ] \
   && ! grep -q '^def strip_prose' "$0"; then
  pass "strip_prose has ONE implementation (imported, not re-declared in this file)"
else
  fail "strip_prose must be defined once, in ws0_prose_strip.py — three inline copies were the nit"
fi

# ==========================================================================
# 5 — the corpus identity is verified against the BYTES that were measured (B6)
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `ws0_validate.load_corpus_identity`: it
# validated `corpus-identity.json` for internal consistency and NEVER OPENED the
# `Data.db`. A corpus dir holding a 4,096-byte Data.db beside an identity claiming
# 700,000 bytes and an unrelated sha256 exited **0** and printed that sha256 as
# "corpus sha256:" in the summary — the report identifying bytes it had not read.
d="$TMP/stale-size"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-stale-size"
python3 - "$TMP/corpus-stale-size/corpus-identity.json" <<'PY'
import json, sys
p = sys.argv[1]
j = json.load(open(p))
j["data_db_bytes"] = 700_000                 # the file is a few KB
j["bytes_per_row"] = 700_000 / j["rows"]     # kept internally consistent on purpose
json.dump(j, open(p, "w"))
PY
expect_reject "a recorded SIZE that disagrees with the measured Data.db is FATAL" \
  "records data_db_bytes" "$d" "$TMP/corpus-stale-size"
out=$(run_report "$d" "$TMP/corpus-stale-size")
if grep -q "nb-1-big-Data.db" <<<"$out"; then
  pass "the size refusal names the Data.db it actually measured"
else
  fail "the size refusal must name the measured file (out: $out)"
fi

cp -R "$TMP/corpus" "$TMP/corpus-stale-sha"
python3 - "$TMP/corpus-stale-sha/corpus-identity.json" <<'PY'
import json, sys
p = sys.argv[1]
j = json.load(open(p))
# A ONE-CHARACTER change: the comparison must be exact, not a prefix/length check,
# and the SIZE still matches — so only the digest can catch this.
s = list(j["data_db_sha256"])
s[0] = "5" if s[0] != "5" else "6"
j["data_db_sha256"] = "".join(s)
json.dump(j, open(p, "w"))
PY
d="$TMP/stale-sha"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "a recorded SHA-256 that disagrees with the measured bytes is FATAL" \
  "data_db_sha256" "$d" "$TMP/corpus-stale-sha"

# A corpus with NO Data.db at all: the identity cannot be checked against anything.
mkdir -p "$TMP/corpus-nodata/ws0/events"
cp "$TMP/corpus/corpus-identity.json" "$TMP/corpus-nodata/corpus-identity.json"
d="$TMP/nodata"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "a corpus with NO *-Data.db is FATAL (nothing to identify)" \
  "holds no \*-Data.db" "$d" "$TMP/corpus-nodata"

# TWO Data.db files: the identity records ONE digest, so which was measured is
# ambiguous — refused rather than picking one.
cp -R "$TMP/corpus" "$TMP/corpus-two"
cp "$TMP/corpus-two/ws0/events/nb-1-big-Data.db" "$TMP/corpus-two/ws0/events/nb-2-big-Data.db"
d="$TMP/two-data"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "TWO *-Data.db files are FATAL (the recorded identity names one corpus)" \
  "2 \*-Data.db" "$d" "$TMP/corpus-two"

# The happy path RECORDS the verification as having happened, with the bytes it read
# — so the field cannot read `true` without an observation behind it.
d="$TMP/verified"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" "$TMP/corpus/ws0/events/nb-1-big-Data.db" <<'PY'
import hashlib, json, sys
v = json.load(open(sys.argv[1]))["corpus_identity_verification"]
raw = open(sys.argv[2], "rb").read()
assert v["size_verified"] is True, v
assert v["sha256_verified"] is True, v
assert v["data_db_bytes_measured"] == len(raw), v
assert v["data_db_sha256_measured"] == hashlib.sha256(raw).hexdigest(), v
assert v["data_db"].endswith("nb-1-big-Data.db"), v
PY
then
  pass "the happy path RECORDS the measured size + digest of the Data.db it verified"
else
  fail "results.json must record the corpus byte verification (rc=$rc, out: $out)"
fi

# --- the OPT-OUT is explicit, recorded, and cannot be silent ----------------
# The digest of a 2.8 GB corpus is seconds of IO per report run, so it is
# skippable — but ONLY via a flag that STAMPS "identity unverified" into both the
# summary and results.json. The SIZE half stays unconditional (it is a stat).
d="$TMP/skip-digest"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus-stale-sha" --skip-corpus-digest); rc=$?
if [ "$rc" -eq 0 ] && grep -q "CORPUS DIGEST UNVERIFIED" <<<"$out"; then
  pass "--skip-corpus-digest is accepted and STAMPS 'CORPUS DIGEST UNVERIFIED' loudly"
else
  fail "--skip-corpus-digest must succeed and say so loudly (rc=$rc, out: $out)"
fi
if python3 - "$d/results.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))["corpus_identity_verification"]
assert v["sha256_verified"] is False, v
assert v["size_verified"] is True, v          # the cheap half is never skippable
assert v["data_db_sha256_measured"] is None, v
assert "unverified" in v["note"].lower(), v
PY
then
  pass "the skipped digest is RECORDED as unverified in results.json (never a silent pass)"
else
  fail "results.json must record the skipped digest as unverified"
fi
# The SIZE half must still fire under the opt-out — the flag scopes the digest only.
d="$TMP/skip-digest-bad-size"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "--skip-corpus-digest does NOT skip the size check" \
  "records data_db_bytes" "$d" "$TMP/corpus-stale-size" --skip-corpus-digest

# ==========================================================================
# 5b — the corpus is PINNED BEFORE MEASUREMENT, and re-compared after (round 4)
# ==========================================================================
# THE FINDING. Section 5 above verifies the corpus digest against the corpus present AT REPORT
# TIME. That cannot see either of the two sequences that attribute figures to bytes nobody
# measured, because BOTH are self-consistent at report time:
#
#   * RE-REPORTING an old result dir against a DIFFERENT corpus. MEASURED pre-fix:
#     `--dir <session-over-corpus-A> --corpus <corpus-B>` exited **0** and printed corpus B's
#     sha256 as the identity of figures measured over corpus A.
#   * A CORPUS CHANGED MID-RUN (regenerated, or written by a second lane between reps): report
#     time verifies the corpus's LAST state while the earlier reps measured the earlier bytes.
#
# The fix is a pin the DRIVER writes BEFORE the first rep and the reporter REQUIRES.
#
# The swap fixture is a REAL second corpus with a genuinely different DIGEST, not an edited
# identity file — an edited file would be caught by section 5's own checks and would prove
# nothing about this one.
#
# The BYTE COUNT is what must differ, not just the row count: `ws0_make_corpus` writes a
# repeating 0..255 pattern, so two corpora of the same size hash IDENTICALLY however many rows
# they claim. Written this way after the first attempt (2000 rows, same 4096 bytes) tripped the
# SHAPE check instead of the DIGEST one — a case passing on the wrong assertion.
ws0_make_corpus "$TMP/corpus-other" 2000 8192
d="$TMP/pin-swapped"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"          # the session was measured over `corpus`
out=$(run_report "$d" "$TMP/corpus-other"); rc=$?  # ...and is reported against another
if [ "$rc" -ne 0 ] && grep -q "THE CORPUS CHANGED" <<<"$out"; then
  pass "OBSERVED: a session PINNED to one corpus is REFUSED when reported against another (pre-fix: exit 0, printed the wrong digest)"
else
  fail "a corpus swap between measurement and report must be refused (rc=$rc, out: $out)"
fi
# ...and the refusal must explain WHY the report-time digest check could not see it, or the next
# reader will conclude the two checks are redundant and delete one.
if grep -q "self-consistent at report time" <<<"$out"; then
  pass "the refusal explains why the REPORT-TIME digest check cannot see this (both states are consistent)"
else
  fail "the swap refusal must name the blind spot it covers (out: $out)"
fi
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written for a session whose corpus was swapped"
else
  fail "a refused run must not leave a results.json behind"
fi
# An ABSENT pin is FATAL, not a skip: a session dir that does not record which corpus it
# measured can be re-reported against anything, which is the fail-open half of the same finding.
d="$TMP/pin-absent"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
rm -f "$d/session-corpus-pin.json"
# NOT through `run_report`, which stamps a pin when one is absent (standing in for the driver):
# this case's subject IS the absence, so the reporter is called directly.
out=$(python3 "$REPORT" --dir "$d" --corpus "$TMP/corpus" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "carries no session-corpus-pin.json" <<<"$out"; then
  pass "OBSERVED: an ABSENT session corpus pin is FATAL (a dir that does not say which corpus it measured)"
else
  fail "an absent corpus pin must be refused (rc=$rc, out: $out)"
fi
# A pin whose DIGEST was hand-edited to match while its SHAPE was not: two independent numbers
# must agree, so editing one is not enough.
d="$TMP/pin-edited"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/session-corpus-pin.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
pin = json.loads(p.read_text())
pin["rows"] = pin["rows"] + 1          # digest still matches; shape does not
p.write_text(json.dumps(pin))
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "THE CORPUS SHAPE CHANGED" <<<"$out"; then
  pass "OBSERVED: a pin whose SHAPE disagrees is refused even when the digest matches (two numbers, not one)"
else
  fail "a pin with an inconsistent shape must be refused (rc=$rc, out: $out)"
fi
# A CORRUPT or INCOMPLETE pin is an error, never a defaulted pass.
for pin_body in '{}' '{"rows":1000}' 'not json' '{"rows":1000,"data_db_bytes":4096,"data_db_sha256":"short"}'; do
  d="$TMP/pin-corrupt-$(printf '%s' "$pin_body" | md5 2>/dev/null || printf '%s' "$pin_body" | md5sum | cut -c1-8)"
  make_session "$d" "$GOOD_FLIGHT"
  printf '%s\n' "$pin_body" > "$d/session-corpus-pin.json"
  out=$(run_report "$d" "$TMP/corpus"); rc=$?
  if [ "$rc" -ne 0 ]; then
    pass "a corrupt/incomplete session pin ($(printf '%.28s' "$pin_body")) is REFUSED"
  else
    fail "a corrupt session pin must be refused (rc=$rc, out: $out)"
  fi
done
# THE ACCEPT DIRECTION: the pin matching is recorded in results.json and stated in the summary,
# so the check cannot be present-but-silent — and without this half the pin could be a function
# that refuses everything.
d="$TMP/pin-ok"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "this session was STARTED against this corpus" <<<"$out" \
  && python3 - "$d/results.json" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))["session_corpus_pin"]
assert p["pinned_before_measurement"] is True, p
assert p["corpus_path_unchanged"] is True, p
assert len(p["pinned_data_db_sha256"]) == 64, p
assert p["pinned_rows"] == 1000, p
PY
then
  pass "OBSERVED: a matching pin is ACCEPTED, stated in the summary and recorded in results.json"
else
  fail "the pin accept direction must be recorded (rc=$rc, out: $out)"
fi
# A MOVED corpus is REPORTED, not fatal — the bytes decide, and a corpus may legitimately move.
d="$TMP/pin-moved"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-moved"
ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus-moved"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "the corpus was MOVED" <<<"$out"; then
  pass "a corpus MOVED to an identical copy is ACCEPTED with the move REPORTED (the bytes decide, not the path)"
else
  fail "a moved corpus with identical bytes must be accepted and reported (rc=$rc, out: $out)"
fi
# And the DRIVER must WRITE the pin, or the reporter's requirement is satisfiable only by
# fixtures — the wiring half (#3272 AC "wiring evidence").
if grep -q 'write_session_corpus_pin' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
  && awk '/^DDL_FILE=/,/^drop_caches_if_cold/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
     | grep -q 'session-corpus-pin.json'; then
  pass "the DRIVER writes the session corpus pin (the reporter's requirement is WIRED, not fixture-only)"
else
  fail "ws0-baseline.sh must stamp session-corpus-pin.json before the measurement loop"
fi
# ...and it must do so BEFORE the first rep, or the pin describes the corpus as it was after
# the run rather than before it.
pin_line=$(grep -n 'write_session_corpus_pin' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
loop_line=$(grep -n '^for temp in \$TEMPS; do' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
if [ -n "$pin_line" ] && [ -n "$loop_line" ] && [ "$pin_line" -lt "$loop_line" ]; then
  pass "the pin is stamped at line $pin_line, BEFORE the measurement loop at line $loop_line"
else
  fail "the pin must be written before the measurement loop (pin=$pin_line loop=$loop_line)"
fi

# ==========================================================================
# 6 — a MISSING python3 FAILS these test scripts; it does not skip them (B8)
# ==========================================================================
# NON-VACUITY, measured against the pre-fix `test_ws0_report_guards.sh:57-60`: with
# python3 absent from PATH it printed
#   `SKIP - python3 not installed; the reporter guards need it (never a silent PASS)`
# and exited **0** — so the gate's `tooling-tests` component recorded SUCCESS with 0
# of its ~65 checks having run, the reassuring parenthetical notwithstanding. The
# exit code is the only thing the gate reads, and it said PASS. Driven here by
# running each script under a PATH containing every standard tool EXCEPT python3.
#
# The shim dir symlinks the tools these scripts need (`bash`, `mktemp`, `grep`, …)
# and deliberately omits python3, rather than emptying PATH — an empty PATH would
# fail for a different reason and prove nothing about the python3 branch.
SHIM="$TMP/nopython/bin"
mkdir -p "$SHIM"
for tool in bash sh mktemp rm cat grep sed awk printf tr seq wc cut head tail sort \
            cp mkdir chmod kill sleep timeout dirname basename env date ls find id \
            uname; do
  p="$(command -v "$tool" 2>/dev/null)" && ln -sf "$p" "$SHIM/$tool"
done
if [ -e "$SHIM/python3" ]; then
  fail "the no-python3 shim must not contain python3"
fi

for script in test_ws0_report_guards.sh test_ws0_cpu_pinning_guards.sh \
              test_ws0_fabrication_guards.sh; do
  out=$(PATH="$SHIM" "$SHIM/bash" "$REPO_ROOT/scripts/tests/$script" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "python3 is not installed" <<<"$out"; then
    pass "OBSERVED: $script FAILS (rc=$rc) when python3 is absent — never a silent exit 0"
  else
    fail "$script must exit non-zero without python3 (rc=$rc, out: $(head -3 <<<"$out"))"
  fi
  # And it must not claim to have skipped: a "SKIP" line beside a zero exit is the
  # exact shape being removed, and the word invites re-adding the `exit 0`.
  if grep -q '^SKIP' <<<"$out"; then
    fail "$script still prints a SKIP line for absent python3 (out: $(head -3 <<<"$out"))"
  else
    pass "$script does not report the absence as a SKIP"
  fi
done

# ==========================================================================
# B2 — `instructions` is validated on the SAME rule as `cycles` (review round 3)
# ==========================================================================
# NON-VACUITY, and this is the THIRD round this class has been fixed partially. Round 2
# added `if cyc <= 0` after the setup subtraction and left `ins` — the SAME subtraction
# over the same two artifacts — unchecked, feeding `ipc.append(ins / cyc)`. So each case
# below is an artifact whose `cycles` are perfectly healthy and whose `instructions` are
# not, which the pre-fix code accepted and PUBLISHED as an IPC.
#
# MEASURED against the pre-fix reporter, THREE reps with exactly ONE corrupt — which is the
# shape the finding is about, because `spread()`'s non-positive-MEDIAN check is what a
# single-rep case would trip on for the wrong reason:
#   * rep 2's SCAN setup instructions 9M > total 4M (cycles healthy) -> exit **0**, and
#     `results.json` carried `ipc: {median: 2.0, min: -2.6315789473684212, …}`; the per-rep
#     IPCs were [2.0, -2.632, 2.0].
#   * rep 2's FLIGHT CSV recording `instructions,0` (cycles healthy) -> exit **0**, and
#     `ipc: {median: 2.0, min: 0.0, …}`.
# The median stayed positive in both, so the impossible value was published as `ipc.min` —
# and would have been the printed `IPC` had it been the middle value. Both cases below are
# driven at three reps for that reason; the single-rep variants are kept beside them
# because they are the cheapest diagnosis of the same defect.

# (a) BARE SCAN, setup instructions > total instructions. Cycles are untouched, so this
#     isolates the missing check rather than re-testing the one that existed.
d="$TMP/ins-subtraction"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-1-setup.csv" 100000 9000000
expect_reject "a setup leg with MORE instructions than the full run is FATAL (round 2 checked only cycles)" \
  "setup-subtracted instructions" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "IPC = instructions/cycles" <<<"$out" \
  && grep -q "non-positive IPC" <<<"$out"; then
  pass "the refusal names WHAT the unchecked value would have published (a non-positive IPC)"
else
  fail "the instructions refusal must name the IPC it protects (out: $out)"
fi
# ...and the CYCLES half must still fire, so the fix did not move the check rather than add
# one. Same fixture shape, the other counter.
d="$TMP/cyc-subtraction"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-scan-warm-1.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-1-setup.csv" 9000000 200000
expect_reject "a setup leg with MORE cycles than the full run is still FATAL (both arms of the subtraction)" \
  "setup-subtracted cycles" "$d" "$TMP/corpus"

# (b) FLIGHT ARM, `instructions,0`. This arm has no setup leg, so the perf value IS the
#     derived quantity — and round 2 checked `cyc <= 0` here too while `ins` went straight
#     into the IPC.
d="$TMP/flight-zero-ins"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-flight-bypass-warm-1.csv" 8000000 0
expect_reject "a flight perf CSV recording instructions=0 is FATAL (pre-fix: published IPC 0.0)" \
  "flight rep flight-bypass-warm-1 instructions" "$d" "$TMP/corpus"
d="$TMP/flight-zero-cyc"; make_session "$d" "$GOOD_FLIGHT"
perf_csv "$d/perf-flight-bypass-warm-1.csv" 0 16000000
expect_reject "a flight perf CSV recording cycles=0 is still FATAL" \
  "flight rep flight-bypass-warm-1 cycles" "$d" "$TMP/corpus"

# (c) A NEGATIVE hardware counter is refused AT PARSE TIME, before any arithmetic. perf
#     cannot emit one, so it is a corrupt artifact — and `int("-4")` used to sail through
#     to become a negative `cycles`, a negative subtraction result, and a negative IPC.
d="$TMP/neg-counter"; make_session "$d" "$GOOD_FLIGHT"
printf -- '-4000000,,cycles,,,,\n8000000,,instructions,,,,\n' > "$d/perf-scan-warm-1.csv"
expect_reject "a NEGATIVE perf counter value is FATAL at parse time (a counter cannot be negative)" \
  "not a possible count" "$d" "$TMP/corpus"

# (d) THE SURVIVING-REP CASES, which are the ones the pre-fix code ACCEPTED. `spread()`
#     refuses a non-positive MEDIAN, so a single corrupt rep tripped that check for the
#     wrong reason; with THREE reps and ONE corrupt, the median stays positive and the
#     impossible value is published as `ipc.min`. Both arms are driven, because the B2 gap
#     was present in both.
#
#     MEASURED against the pre-fix reporter, both exited **0**:
#       scan  -> ipc {median: 2.0, min: -2.6315789473684212}, per-rep [2.0, -2.632, 2.0]
#       flight-> ipc {median: 2.0, min: 0.0}
three_reps() { # three_reps <dir>
  local dd="$1" r
  mkdir -p "$dd"
  for r in 1 2 3; do
    make_scan_rep "$dd" warm "$r" ok
    make_flight_rep "$dd" warm "$r" ok "$GOOD_FLIGHT"
  done
}
d="$TMP/one-bad-scan-ins-of-three"; three_reps "$d"
perf_csv "$d/perf-scan-warm-2.csv" 2000000 4000000
perf_csv "$d/perf-scan-warm-2-setup.csv" 100000 9000000     # rep 2 only; CYCLES healthy
out=$(run_report_args "$d" "$TMP/corpus" 3 warm bypass 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "setup-subtracted instructions" <<<"$out"; then
  pass "OBSERVED: ONE corrupt scan rep of three is REFUSED (pre-fix: exit 0, ipc.min -2.63)"
else
  fail "one corrupt scan rep among three must be refused, not published as ipc.min (rc=$rc, out: $out)"
fi
if [ ! -e "$d/results.json" ]; then
  pass "no results.json is written for the surviving-rep case (pre-fix it held the negative ipc.min)"
else
  fail "a refused run must not leave a results.json behind"
fi
d="$TMP/one-bad-flight-ins-of-three"; three_reps "$d"
perf_csv "$d/perf-flight-bypass-warm-2.csv" 8000000 0       # rep 2 only; CYCLES healthy
out=$(run_report_args "$d" "$TMP/corpus" 3 warm bypass 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "flight rep flight-bypass-warm-2 instructions" <<<"$out"; then
  pass "OBSERVED: ONE corrupt flight rep of three is REFUSED (pre-fix: exit 0, ipc.min 0.0)"
else
  fail "one corrupt flight rep among three must be refused (rc=$rc, out: $out)"
fi
# And `spread()` itself must refuse a non-positive MEMBER, not only a non-positive MEDIAN —
# the last line of defence, driven directly so it is not merely reachable in theory.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_collect import spread
from ws0_validate import Invalid
# A series whose MEDIAN is positive and one of whose MEMBERS cannot exist. The pre-fix
# spread() accepted this and returned min=-1.0.
for series in ([2.0, -1.0, 3.0], [2.0, 0.0, 3.0], [2.0, float("inf"), 3.0]):
    try:
        got = spread(series)
    except Invalid as e:
        assert "not meaningful" in str(e) or "not finite" in str(e), str(e)
    else:
        raise SystemExit(f"spread({series}) must refuse a non-positive/non-finite MEMBER, got {got}")
# ...and a healthy series is still accepted, so it is not a function that refuses everything.
got = spread([2.0, 3.0, 4.0])
assert got["median"] == 3.0 and got["min"] == 2.0 and got["n"] == 3, got
PY
then
  pass "spread() REFUSES a non-positive/non-finite MEMBER of an otherwise-healthy series (B2)"
else
  fail "spread() must refuse a bad member, not only a bad median — that is the surviving-rep hole"
fi

# ==========================================================================
# B5 — a coercion may not TRUNCATE or accept a BOOLEAN (review round 3)
# ==========================================================================
# NON-VACUITY: every coercion in the reporting path was a bare `int()`. Measured against
# HEAD~1 of this commit, each of these exited **0** with a full report:
#   * `requests_error: 0.9` -> `int(0.9)` is 0 -> reported CLEAN, no failed requests
#   * `requests_ok: 1.9`    -> `int(1.9)` is 1 -> SATISFIED the exactly-one-cold-request
#                              guard of #3096 finding 2, from a value that is not 1
#   * `requests_error: true`-> `int(True)` is 1 -> then refused as "1 failed request",
#                              i.e. a boolean silently became a count
# A truncation is a FABRICATED VALUE arrived at by rounding rather than by defaulting —
# the same class as `.get(k, 0)`, which is why it belongs in this file.
FRAC_ERR='{"round":"r","requests_ok":1,"requests_error":0.9,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/frac-error"; make_session "$d" "$FRAC_ERR"
expect_reject "a FRACTIONAL requests_error is FATAL (pre-fix: int(0.9) reported CLEAN)" \
  "fractional value" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
if grep -q "TRUNCATED it to 0" <<<"$out" && grep -q "fabricated value" <<<"$out"; then
  pass "the refusal names the TRUNCATION and what it would have reported"
else
  fail "the fractional-counter refusal must name the truncated value (out: $out)"
fi
BOOL_ERR='{"round":"r","requests_ok":1,"requests_error":true,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/bool-error"; make_session "$d" "$BOOL_ERR"
expect_reject "a BOOLEAN requests_error is FATAL (pre-fix: int(True) became a count of 1)" \
  "is the boolean True" "$d" "$TMP/corpus"
# `requests_ok`, where the truncation defeats the COLD guard rather than the error count.
FRAC_OK='{"round":"r","requests_ok":1.9,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/frac-ok"; mkdir -p "$d"
make_scan_rep "$d" cold 1 skipped-cold-arm
make_flight_rep "$d" cold 1 skipped-cold-arm "$FRAC_OK"
out=$(run_report_args "$d" "$TMP/corpus" 1 cold bypass 1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "fractional value 1.9" <<<"$out"; then
  pass "OBSERVED: a FRACTIONAL requests_ok is refused (pre-fix: int(1.9)==1 SATISFIED the cold guard)"
else
  fail "a fractional requests_ok must be refused, not truncated into the cold guard (rc=$rc, out: $out)"
fi
BOOL_OK='{"round":"r","requests_ok":true,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/bool-ok"; make_session "$d" "$BOOL_OK"
expect_reject "a BOOLEAN requests_ok is FATAL (int(True) is 1, which is a valid count)" \
  "is the boolean True" "$d" "$TMP/corpus"
# A FRACTIONAL rows_total, which would silently change the DENOMINATOR of cycles/row.
FRAC_ROWS='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000.5,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/frac-rows"; make_session "$d" "$FRAC_ROWS"
expect_reject "a FRACTIONAL rows_total is FATAL (it is the cycles/row denominator)" \
  "fractional value" "$d" "$TMP/corpus"
# The same class in a perf CSV, which is TEXT: `int("4.7")` raises, but `int(" 4 ")` used
# to accept padding silently, and neither is a canonical counter value.
d="$TMP/frac-csv"; make_session "$d" "$GOOD_FLIGHT"
printf '4000000.5,,cycles,,,,\n8000000,,instructions,,,,\n' > "$d/perf-scan-warm-1.csv"
expect_reject "a FRACTIONAL perf CSV counter is FATAL (not a canonical integer)" \
  "unparseable value" "$d" "$TMP/corpus"
# ...and the identity fields.
make_corpus "$TMP/corpus-frac-rows"
python3 - "$TMP/corpus-frac-rows" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "corpus-identity.json"
ident = json.loads(p.read_text())
ident["cells_per_row"] = 12.5
p.write_text(json.dumps(ident))
PY
d="$TMP/frac-identity"; make_session "$d" "$GOOD_FLIGHT"
expect_reject "a FRACTIONAL corpus-identity field is FATAL (pre-fix: silently truncated)" \
  "fractional value" "$d" "$TMP/corpus-frac-rows"
# THE ACCEPT DIRECTION for the whole class: an INTEGRAL float (`1000.0`) is the value it
# would be read as, so it is ACCEPTED. The rule is "not the integer it would be read as",
# not "never a float" — a producer writing an integer-valued double is not an error.
INTEGRAL_FLOAT='{"round":"r","requests_ok":1.0,"requests_error":0.0,"requests_unavailable":0,"rows_total":1000.0,"rows_per_s":250.0,"duration_s":4.0}'
d="$TMP/integral-float"; make_session "$d" "$INTEGRAL_FLOAT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "an INTEGRAL float (1000.0) is ACCEPTED — the rule is exactness, not 'never a float'"
else
  fail "an integral float must be accepted; refusing it would red every producer writing doubles (rc=$rc, out: $out)"
fi
# NO HAND-WRITTEN INVENTORY OF THE COERCIONS MAY RETURN (#3272 review round 4 nit).
#
# `ws0_validate.py` carried a comment claiming to be "the complete inventory, enumerated
# MECHANICALLY" of 11 coercions and 17 derived quantities. It was neither complete nor checked:
# `rows_per_scan_observed`, `spread_pct_of_median` and `within_round_span_ns` were all absent.
# Prose that claims an audited set and is wrong is worse than none, because a reader who trusts
# it stops looking — the shape this whole file is about, one level up.
#
# It was DELETED rather than corrected, because a corrected list drifts again on the next
# quantity. What remains is the ast scan above, whose subject is derived from the code. This
# case keeps the list from coming back: no reporting-path file may claim a COMPLETE ENUMERATION
# of its own quantities in prose.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import pathlib, sys
d = pathlib.Path(sys.argv[1])
files = sorted(d.glob("ws0_*.py"))
if not files:
    raise SystemExit(f"the scan's SUBJECT is EMPTY (no ws0_*.py in {d})")
CLAIMS = ("complete inventory", "the complete enumeration", "full inventory")
# The exemption is asked over the SURROUNDING BLOCK, not the single line, and that distinction
# is the finding this case produced against itself: the historical record in `ws0_validate.py`
# necessarily QUOTES the phrase it removed ('It claimed to be "the complete inventory…"'), and a
# per-line test flagged it. A block-scoped test is also the right shape — an inventory is a
# multi-line structure, so "is this block about a deleted inventory?" is the real question.
#
# A block is the contiguous run of comment lines around the match. `deleted`/`used to`/`no
# longer` anywhere in it marks the whole block as a RECORD of the removal rather than a claim.
EXEMPT_MARKERS = ("deleted", "used to", "no longer", "is not an inventory")
bad = []
for p in files:
    lines = p.read_text().splitlines()
    for n, line in enumerate(lines, 1):
        low = line.lower()
        if not any(claim in low for claim in CLAIMS):
            continue
        lo = n - 1
        while lo > 0 and lines[lo - 1].lstrip().startswith("#"):
            lo -= 1
        hi = n
        while hi < len(lines) and lines[hi].lstrip().startswith("#"):
            hi += 1
        block = "\n".join(lines[lo:hi]).lower()
        if any(m in block for m in EXEMPT_MARKERS):
            continue
        bad.append(f"{p.name}:{n}: {line.strip()[:100]}")
if bad:
    raise SystemExit(
        "a hand-written 'complete inventory' claim has returned to the reporting path;\n"
        "the previous one omitted three quantities and nothing checked it:\n  "
        + "\n  ".join(bad)
    )
print(f"scanned {len(files)} reporting-path file(s)")
PY
then
  pass "STRUCTURAL: no reporting-path file claims a hand-written COMPLETE INVENTORY of its quantities"
else
  fail "a hand-written coercion inventory has returned (it was deleted for being incomplete + unchecked)"
fi
# ...and the MECHANISM that replaced it must be the one this file actually runs — asserted by
# NAME, so the pointer in `ws0_validate.py` cannot outlive the check it points at.
if grep -q 'strip_prose' "$TESTS_DIR/ws0_prose_strip.py" \
  && grep -q 'ws0_prose_strip' "$0"; then
  pass "the replacement MECHANISM (the ast coercion scan) is present and used by this suite"
else
  fail "ws0_validate.py points at an ast scan this suite must run; it is missing"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its
# ~96 checks and passed them exits 0 and reports SUCCESS. That is the suite-level
# `0/0` shape this whole issue is about, one level up from the checks themselves.
#
# The floor is deliberately BELOW the current count (adding a case must not red the suite)
# and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file.
MIN_CHECKS=135
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 fabrication guards: all $checks checks passed"
  exit 0
fi
echo "ws0 fabrication guards: $fails of $checks check(s) FAILED"
exit 1

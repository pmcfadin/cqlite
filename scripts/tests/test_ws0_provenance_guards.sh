#!/usr/bin/env bash
# Self-test for the WS0 rig's MEASUREMENT PROVENANCE (issue #3272; split out in review round 6).
#
# Split from `test_ws0_fabrication_guards.sh` under the campsite rule (~1500-line test target),
# by SUBJECT rather than by size. That file's subject is a property of a NUMBER —
#
#     A COUNTER OR VERDICT THAT WAS NOT OBSERVED IS AN ERROR, NEVER A DEFAULT.
#
# This file's subject is a property of the RUN the numbers came from:
#
#     A REPORT MUST IDENTIFY THE BYTES AND THE CONFIGURATION IT DESCRIBES.
#
# A figure can be individually impeccable — observed, in domain, derived from its parts, agreeing
# with every sibling counter — and still be attributed to a corpus nobody measured or to a
# configuration nobody ran. Nothing in the fabricated-value subject can see that: every check
# there is satisfiable by an artifact set that is internally consistent about the wrong run. So
# the four findings below are one subject, and it is not that one:
#
#   * F1 — THE CONFIGURATION CAME FROM THE CURRENT CLI. `--reps`/`--temps`/`--arms`/
#     `--scan-passes` and the CPU pins were the REPORTER'S arguments, tied to nothing about the
#     session, so a re-report could SUBSTITUTE a configuration and state it had been verified
#     (measured: a 3-rep session reported at `--reps 1` published rep 1 as the run, under CPU
#     pins the session never used and a "verified physical-core siblings" claim). The
#     configuration is now READ FROM the pre-measurement manifest and the flags are GONE.
#   * F3 — CORPUS VERIFICATION CHECKED ONLY `Data.db`. A scan reads `Index.db` above all, plus
#     the Statistics/Summary/Filter components that shape how it reads, so a MODIFIED AUXILIARY
#     COMPONENT changed measured behaviour under an intact "corpus verified" line.
#   * B6 — THE IDENTITY WAS NEVER CHECKED AGAINST THE BYTES. `corpus-identity.json` was
#     validated for internal consistency and the `Data.db` was never opened, so stale metadata
#     beside different bytes printed a sha256 the report had not read.
#   * ROUND 4's PIN — the report-time digest cannot see either sequence that attributes figures
#     to bytes nobody measured (re-reporting an old session against a different corpus; a corpus
#     changed mid-run), because BOTH are self-consistent AT REPORT TIME. The driver stamps a pin
#     BEFORE the first rep and the reporter REQUIRES it.
#
# Every case carries the MEASURED pre-fix behaviour, because per #3249 a guard never observed
# firing is not evidence.
#
# THREE SUBJECTS HAVE BEEN SPLIT OUT of this file, all on the campsite rule and all by seam:
# `test_ws0_binary_provenance.sh` (round 11) owns WHICH PROGRAMS produced the ratio,
# `test_ws0_output_dir_exclusivity.sh` (round 13) owns whether the DIRECTORY holding the artifacts
# belongs to exactly one session, and `test_ws0_canonical_corpus.sh` (round 13) owns whether those
# bytes are the ones a WS0 BASELINE is DEFINED as. None is this file's subject, and the reason is
# the same in all three: every check here is satisfiable by a session whose bytes and configuration
# are impeccably identified — and whose two arms were different BUILDS, or whose rep files came from
# two RUNS that shared a directory, or whose corpus was a smoke-sized one that every check here
# agrees with perfectly.
#
# Hermetic: synthetic session dirs, synthetic perf CSVs, and synthetic multi-byte `Data.db`
# files whose real sha256 is computed with python3's hashlib. No cargo, perf, sudo, corpus,
# network or root.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"

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
# Fixture builders — SHARED with the two sibling reporter suites
# --------------------------------------------------------------------------
# `lib-ws0-report-fixtures.sh` holds the session-dir builders (`make_corpus`, `make_session`,
# `make_scan_rep`, `make_flight_rep`), the `run_report`/`run_report_args` wrappers and
# `expect_reject`; it in turn sources `lib-ws0-fixtures.sh` for
# `perf_csv`/`ws0_make_corpus`/`ws0_pin_session_corpus`/`make_round` (shared with
# `test_ws0_report_guards.sh` as well). A duplicated builder is the wrong thing to keep three
# copies of: `make_round` gaining a `monotonic_ns` field had to be edited in two files in round
# 3, and `make_flight_rep` gaining a DERIVED `rows_per_s` in two more in round 4.
# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"
# ...and the HERMETIC driver harness. NOTE what this file does and does not use it for, stated
# accurately after round 13's output-dir split: the cases that RAN the driver (round 6's R1
# output-dir refusals) moved to `test_ws0_output_dir_exclusivity.sh` with the rest of that
# subject, so every remaining reference to `$DRIVER` here is a STRUCTURAL read of its source text
# (the schema check's line number, the ticket path's resolution) — nothing in this file executes
# it today. The harness is initialised anyway, deliberately: a future case in this subject that
# needs to RUN the driver must go through `ws0_driver_run` (`--validate-args-only` + recording
# shims, asserting it executed NOTHING), and having the shims already in place is what makes that
# the path of least resistance rather than a step someone adds after the fact. The structural lint
# in `test_ws0_hermeticity.sh` is what actually enforces it.
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
ws0_hermetic_init "$TMP"

GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'

make_corpus "$TMP/corpus"

# THE POSITIVE CONTROLS are INSIDE the sections below rather than one at the top, because each
# of the four findings has an accept direction of its own that is stronger than a generic happy
# path: F1's manifest-driven `n=3`, F3's `5 of 5` component count, B6's recorded measured
# digest, and the pin's ACCEPTED match plus its ACCEPTED move. Without at least one of those a
# reporter hardcoded to refuse everything would satisfy this whole file.

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
# The RANGE START moved in round 13's F2 campsite split: the driver's `DDL_FILE=` assignment and its
# inline schema/ticket heredocs became `verify_corpus_schema_input`/`write_ticket_template_for_session`
# in scripts/perf/lib-inputs.sh. Anchored on the first of those CALL SITES, which is what the driver
# still owns (the ORDER), so the range covers setup-through-loop exactly as before.
if grep -q 'write_session_corpus_pin' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
  && awk '/^verify_corpus_schema_input/,/^drop_caches_if_cold/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" \
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
# ROUND 6, B2 — the PINNED COMPONENT SET is COMPARED, not merely recorded
# ==========================================================================
# Round 5's F3 added the complete component set to the pin. The writer wrote it; the driver
# printed its count. NOTHING READ IT — a tree-wide grep found no consumer, so
# `verify_session_corpus_pin` compared only rows/data_db_bytes/data_db_sha256, and the sibling
# `verify_corpus_components` closes F3 only against the corpus's OWN report-time
# `corpus-identity.json`: a file that can be refreshed beside the component it describes.
#
# THE TRIGGERING STATE, which is what makes this a report-integrity defect and not an unused
# field: replace `Index.db` between rep 1 and the report and refresh `corpus-identity.json`
# beside it, leaving `Data.db` untouched. The pin's three fields still match; the identity is
# self-consistent with disk. PRE-FIX this exited **0** and printed
#
#     corpus comps : all 5 recorded component(s) were re-stat'ed and 5 of 5 re-hashed
#
# — an affirmative FULL-verification claim over a corpus whose auxiliary component is not the one
# that was measured. Written-but-unread data reading as a guard is this issue's defining class.
d="$TMP/pin-comp-swap"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-comp-swap"
# Pin against the ORIGINAL corpus (this is the pre-measurement stamp)...
ws0_pin_session_corpus "$d" "$TMP/corpus-comp-swap"
# ...then replace Index.db and REFRESH the identity beside it, exactly as a regeneration would.
# Data.db is deliberately untouched, so the pin's three fields cannot see this.
python3 - "$TMP/corpus-comp-swap" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]); table = root / "ws0" / "events"
idx = next(p for p in table.iterdir() if p.name.endswith("-Index.db"))
idx.write_bytes(idx.read_bytes() + b"\x00extra-index-bytes")
ident = root / "corpus-identity.json"
j = json.loads(ident.read_text())
j["components"][idx.name] = {
    "bytes": idx.stat().st_size,
    "sha256": hashlib.sha256(idx.read_bytes()).hexdigest(),
}
ident.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus-comp-swap"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'CORPUS COMPONENT .*CHANGED' <<<"$out"; then
  pass "OBSERVED (round6 B2): an Index.db REPLACED mid-session — with corpus-identity.json refreshed beside it — is REFUSED (pre-fix: exit 0 claiming 5 of 5 re-hashed)"
else
  fail "round6 B2: a mid-session component swap must be refused by the PIN, not blessed by the report-time identity (rc=$rc, out: $out)"
fi
# NON-VACUITY of that case, in the direction that matters: the swap is invisible to everything
# the pin checked BEFORE this fix. Assert the three Data.db fields still AGREE, so the refusal
# above is attributable to the component comparison and to nothing else.
if python3 - "$d/session-corpus-pin.json" "$TMP/corpus-comp-swap/corpus-identity.json" <<'PY'
import json, sys
pin = json.load(open(sys.argv[1])); ident = json.load(open(sys.argv[2]))
for k in ("rows", "data_db_bytes", "data_db_sha256"):
    assert pin[k] == ident[k], (k, pin[k], ident[k])
# ...and the identity IS self-consistent with disk for the swapped component, which is why
# `verify_corpus_components` passes it.
assert pin["components"] != {n: {"bytes": s["bytes"], "sha256": s["sha256"]}
                             for n, s in sorted(ident["components"].items())}
PY
then
  pass "NON-VACUITY (round6 B2): the swap leaves rows + data_db_bytes + data_db_sha256 IDENTICAL, so the refusal came from the component comparison alone"
else
  fail "round6 B2: the swap fixture must be invisible to the pin's three Data.db fields, or the case proves nothing"
fi
# A component that DISAPPEARED, which changes the read path just as much (an absent Index.db is
# a different scan) — the other direction of the name-set comparison.
d="$TMP/pin-comp-gone"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-comp-gone"
ws0_pin_session_corpus "$d" "$TMP/corpus-comp-gone"
python3 - "$TMP/corpus-comp-gone" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1]); table = root / "ws0" / "events"
idx = next(p for p in table.iterdir() if p.name.endswith("-Index.db"))
idx.unlink()
ident = root / "corpus-identity.json"
j = json.loads(ident.read_text()); j["components"].pop(idx.name)
ident.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus-comp-gone"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'COMPONENT SET CHANGED' <<<"$out"; then
  pass "OBSERVED (round6 B2): a component that VANISHED between the pin and the report is REFUSED"
else
  fail "round6 B2: a vanished pinned component must be refused (rc=$rc, out: $out)"
fi
# THE ACCEPT DIRECTION for the component comparison, so it cannot be a function that refuses
# everything, and the SCOPE of what it verified is RECORDED rather than implied.
d="$TMP/pin-comp-ok"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))["session_corpus_pin"]
n = p["pinned_components"]
assert n >= 5, p                                    # the fixture corpus emits 5 components
assert p["pinned_components_verified_size"] == n, p
assert p["pinned_components_verified_sha256"] == n, p   # a full-digest run
assert "against the report-time identity AND the bytes on disk" in p["pinned_components_note"], p
PY
then
  pass "OBSERVED (round6 B2): a matching pin records HOW MANY components were re-compared, against BOTH the identity and disk"
else
  fail "round6 B2: the accept direction must record the component comparison's scope (rc=$rc, out: $out)"
fi
# ...and under --skip-corpus-digest the pin reports that NO component CONTENT was confirmed,
# rather than counting an unobserved digest as a match (a value not observed is never a pass).
d="$TMP/pin-comp-skip"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus" --skip-corpus-digest); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))["session_corpus_pin"]
# The COUNT FLOOR comes FIRST, and it is not decorative: a `pinned_components == 0` would
# satisfy both assertions below by arithmetic (`0 == 0`), so without this floor the case is
# ALSO satisfied by a component check that never ran. MEASURED — with the reader stubbed out
# to all-zeros, this case was the one case of six that still PASSED.
assert p["pinned_components"] >= 5, p
assert p["pinned_components_verified_sha256"] == 0, p
assert p["pinned_components_verified_size"] == p["pinned_components"], p
assert "NO digest was re-derived" in p["pinned_components_note"], p
PY
then
  pass "OBSERVED (round6 B2): --skip-corpus-digest reports 0 component digests re-derived and says no CONTENT was confirmed"
else
  fail "round6 B2: the skip path must not count an unobserved component digest as verified (rc=$rc, out: $out)"
fi
# A pin with NO `components` map at all — a session dir predating F3 — is REFUSED rather than
# skipped. A comparison that silently does not run prints exactly like one that passed.
d="$TMP/pin-comp-absent"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/session-corpus-pin.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p)); j.pop("components")
json.dump(j, open(p, "w"), indent=1)
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records no `components` map' <<<"$out"; then
  pass "OBSERVED (round6 B2): a pin carrying NO component map is REFUSED (an absent comparison must not read as a passing one)"
else
  fail "round6 B2: a pin with no components map must be refused (rc=$rc, out: $out)"
fi

# ==========================================================================
# OUTPUT-DIRECTORY EXCLUSIVITY: moved to scripts/tests/test_ws0_output_dir_exclusivity.sh
# ==========================================================================
# Round 6's R1, round 9's F4 and round 7's F3 — the output directory's single-owner lifecycle
# (`require_unused_out_dir`, `claim_out_dir`, `create_out_dir` in `scripts/perf/lib-outdir.sh`,
# and their placement either side of the driver's `--validate-args-only` boundary) — are their
# own subject and their own suite since round 13's campsite-rule split. This file's subject is
# WHICH BYTES AND WHICH CONFIGURATION a report describes; that one is whether the DIRECTORY
# holding those artifacts belongs to exactly one session. Distinct because every check here is
# satisfiable by a session whose corpus, components, schema, request and configuration are
# impeccably pinned and whose rep files came from TWO RUNS sharing a directory.

# ==========================================================================
# ROUND 6, R2 — THE SCHEMA IS A VERIFIED MEASUREMENT INPUT
# ==========================================================================
# `ws0-events.cql` was outside EVERY verification the rig performs: absent from the Data.db
# digest check, absent from the component check (it is not in the table directory), absent from
# the session pin. The driver's whole check was `[[ -r "$DDL_FILE" ]]` — readable, nothing more.
#
# WHY THAT MATTERS, and it is the ASYMMETRY rather than the mere absence: the TICKET TEMPLATE is
# generated from the DDL ONCE, at setup (`ws0-baseline.sh` line ~566), while the BARE SCAN
# ingests the file on EVERY invocation. So editing it between setup and a later rep makes the two
# arms measure DIFFERENT SCHEMAS — a different column set, clustering order or type — while every
# recorded identity still agrees and the report exits 0. A head-to-head number between two arms
# reading two schemas compares nothing.
#
# PRE-FIX, every case below exited 0 with a full report.
d="$TMP/schema-changed"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-schema-changed"
# The schema is MODIFIED without touching Data.db, the components, or the identity — exactly
# what an edit between setup and a later rep looks like.
printf 'CREATE TABLE ws0.events (part_id text, seq bigint, PRIMARY KEY (part_id, seq));\n' \
  > "$TMP/corpus-schema-changed/ws0-events.cql"
out=$(run_report "$d" "$TMP/corpus-schema-changed"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'THE SCHEMA CHANGED' <<<"$out"; then
  pass "OBSERVED (round6 R2): a MODIFIED ws0-events.cql is REFUSED (pre-fix: outside every check — the driver only tested it was readable)"
else
  fail "round6 R2: a modified schema must be refused (rc=$rc, out: $out)"
fi
# NON-VACUITY: the modification is invisible to everything else the report verifies. Assert the
# Data.db digest and the whole component set still match, so the refusal is attributable to the
# schema check alone.
if python3 - "$TMP/corpus-schema-changed" "$TMP/corpus" <<'PY'
import json, pathlib, sys
a = json.loads((pathlib.Path(sys.argv[1]) / "corpus-identity.json").read_text())
b = json.loads((pathlib.Path(sys.argv[2]) / "corpus-identity.json").read_text())
for k in ("rows", "data_db_bytes", "data_db_sha256", "components"):
    assert a[k] == b[k], k
# ...and the schema on disk really differs from what the identity records.
import hashlib
ddl = (pathlib.Path(sys.argv[1]) / "ws0-events.cql").read_bytes()
assert hashlib.sha256(ddl).hexdigest() != a["schema_sha256"]
PY
then
  pass "NON-VACUITY (round6 R2): the schema edit leaves Data.db AND every component identical, so the refusal came from the schema check alone"
else
  fail "round6 R2: the schema fixture must be invisible to the corpus checks, or the case proves nothing"
fi
# A MISSING schema, which is the state a corpus regenerated by an older tool would be in.
d="$TMP/schema-missing"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-schema-missing"
rm -f "$TMP/corpus-schema-missing/ws0-events.cql"
out=$(run_report "$d" "$TMP/corpus-schema-missing"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'is MISSING, but the recorded identity pins its digest' <<<"$out"; then
  pass "OBSERVED (round6 R2): an ABSENT ws0-events.cql is REFUSED (both arms read it; a corpus without it cannot be measured)"
else
  fail "round6 R2: a missing schema must be refused (rc=$rc, out: $out)"
fi
# An identity with NO `schema_sha256` — a corpus predating the pin — is REFUSED, not silently
# accepted. Treating "no record" as "nothing to check" is the fail-open shape: an absent digest
# means the schema was never pinned, which IS the finding.
d="$TMP/schema-unpinned"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-schema-unpinned"
python3 - "$TMP/corpus-schema-unpinned/corpus-identity.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p)); j.pop("schema_sha256")
json.dump(j, open(p, "w"))
PY
out=$(run_report "$d" "$TMP/corpus-schema-unpinned"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records no `schema_sha256`' <<<"$out"; then
  pass "OBSERVED (round6 R2): an identity with NO schema digest is REFUSED — an unpinned schema is the finding, not an exemption from it"
else
  fail "round6 R2: an identity without schema_sha256 must be refused (rc=$rc, out: $out)"
fi
# ...and there is NO FLAG that skips it. `--skip-corpus-digest` exists because hashing 2.8 GB per
# report is a real cost; hashing a few hundred bytes is not, so a skip could only buy a vacuous
# green. Driven with the flag PRESENT: the schema refusal must still fire.
out=$(run_report "$d" "$TMP/corpus-schema-unpinned" --skip-corpus-digest); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records no `schema_sha256`' <<<"$out"; then
  pass "OBSERVED (round6 R2): --skip-corpus-digest does NOT skip the schema check (no flag can — the file is a few hundred bytes)"
else
  fail "round6 R2: the schema check must have no opt-out (rc=$rc, out: $out)"
fi
# THE ACCEPT DIRECTION, and the SCOPE recorded rather than implied — without it the schema check
# could be a function that refuses everything.
d="$TMP/schema-ok"; make_session "$d" "$GOOD_FLIGHT"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))["schema_input_verification"]
assert v["sha256_verified"] is True, v
assert len(v["schema_sha256_measured"]) == 64, v
assert v["schema_sha256_measured"] == v["schema_sha256_recorded"], v
assert v["schema_bytes"] > 0, v
assert v["schema"].endswith("ws0-events.cql"), v
PY
then
  pass "OBSERVED (round6 R2): a matching schema is ACCEPTED and its digest is recorded as MEASURED, not merely recorded"
else
  fail "round6 R2: the schema accept direction must record an observed digest (rc=$rc, out: $out)"
fi
# THE PRE-MEASUREMENT HALF: the pin carries the schema digest, and a schema replaced mid-session
# with the identity REFRESHED beside it is refused. That state is self-consistent at report time,
# so `verify_schema_input` alone cannot see it — the same argument as B2's component comparison.
d="$TMP/schema-pin-swap"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-schema-pin-swap"
ws0_pin_session_corpus "$d" "$TMP/corpus-schema-pin-swap"
python3 - "$TMP/corpus-schema-pin-swap" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
ddl = root / "ws0-events.cql"
ddl.write_bytes(b"CREATE TABLE ws0.events (part_id text, seq int, extra text, PRIMARY KEY (part_id, seq));\n")
ident = root / "corpus-identity.json"
j = json.loads(ident.read_text())
j["schema_sha256"] = hashlib.sha256(ddl.read_bytes()).hexdigest()   # refreshed BESIDE it
ident.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus-schema-pin-swap"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'THE SCHEMA CHANGED under this session' <<<"$out"; then
  pass "OBSERVED (round6 R2): a schema replaced MID-SESSION — with corpus-identity.json refreshed beside it — is refused by the PIN (the report-time check cannot see it)"
else
  fail "round6 R2: a mid-session schema swap must be refused by the pin (rc=$rc, out: $out)"
fi
# NON-VACUITY for THAT case: the swap is self-consistent at report time, so `verify_schema_input`
# would PASS it. Assert exactly that, or the case does not prove the pin is what caught it.
if python3 - "$TMP/corpus-schema-pin-swap" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
j = json.loads((root / "corpus-identity.json").read_text())
on_disk = hashlib.sha256((root / "ws0-events.cql").read_bytes()).hexdigest()
assert on_disk == j["schema_sha256"], "the swap must be self-consistent at report time"
PY
then
  pass "NON-VACUITY (round6 R2): the mid-session swap IS self-consistent at report time, so only the pre-measurement PIN could have caught it"
else
  fail "round6 R2: the swap fixture must be report-time-consistent, or it does not test the pin"
fi
# ...and the DRIVER must verify the schema BEFORE the measurement loop — the wiring half. A
# reporter-only check would let a full run complete before anything noticed.
# The CALL SITE in the driver, which is what the ORDER assertions are about. Round 13's F2 split moved
# the schema verifier's BODY into scripts/perf/lib-inputs.sh (the driver was 159 lines over the
# campsite target); the driver still owns the order, so the anchor is the driver-side call
# `verify_corpus_schema_input`. An empty value fails these cases CLOSED rather than comparing against
# nothing.
schema_line=$(grep -n '^verify_corpus_schema_input' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
# The GENERATION site, not the variable assignment: `TICKET_TEMPLATE=` (line ~359) is just a
# path, while the `write_ticket_template` call is where the ticket is actually DERIVED from the
# DDL. Anchoring on the assignment measured the wrong line and failed this case for the wrong
# reason.
#
# The spelling moved in #3272 round 10's M1: the generation was an inline
# `python3 - "$DDL_FILE" "$TICKET_TEMPLATE"` heredoc BELOW the pin, and is now a call to
# `ws0_ticket_input.write_ticket_template` ABOVE it (the ticket is pinned, so it must exist before
# the pin). Anchored on the FUNCTION NAME rather than on an argv shape: the function is what
# derives the ticket, whatever the call is spelled like, and an empty `ticket_line` fails this case
# closed rather than silently comparing against nothing.
#
# Round 13's F2 split moved the derivation's BODY into `lib-inputs.sh` too, so the driver-side anchor
# is now its call `write_ticket_template_for_session` — still the ONE place the driver derives the
# ticket, and still anchored on a function name rather than an argv shape.
ticket_line=$(grep -n '^write_ticket_template_for_session' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
loop_line=$(grep -n '^for temp in \$TEMPS; do' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
if [ -n "$schema_line" ] && [ -n "$loop_line" ] && [ "$schema_line" -lt "$loop_line" ]; then
  pass "round6 R2: the DRIVER verifies the schema at line $schema_line, BEFORE the measurement loop at line $loop_line (wired, not reporter-only)"
else
  fail "round6 R2: the driver must verify the schema before measuring (schema=$schema_line loop=$loop_line)"
fi
# ...and the ASYMMETRY that makes this matter is asserted structurally: the ticket is generated
# from the DDL, ONCE, at setup — so the schema check must precede that too, or the ticket could
# be built from a schema nobody verified.
if [ -n "$schema_line" ] && [ -n "$ticket_line" ] && [ "$schema_line" -lt "$ticket_line" ]; then
  pass "round6 R2: the schema is verified BEFORE the Flight ticket is generated from it (line $schema_line < $ticket_line) — the ticket is built once, the scan re-reads per rep"
else
  fail "round6 R2: the schema must be verified before the ticket is derived from it (schema=$schema_line ticket=$ticket_line)"
fi

# ==========================================================================
# ROUND 10, M1 — THE FLIGHT TICKET IS THE REQUEST, AND IT WAS PINNED BY NOTHING
# ==========================================================================
# `ticket-template.json` carries the keyspace, table, DDL, token range, column projection,
# predicates, aggregation and limit, and `flight-loadgen --ticket-template` re-reads it on EVERY
# invocation of every rep of every arm. It was created AFTER the session pin and appeared in NO
# verified record, so it could be changed between reps or between ARMS without invalidating corpus
# identity: the corpus is untouched, so every corpus digest, the complete component set and the
# schema all still agree — and the report exits 0 having compared two arms that answered DIFFERENT
# QUERIES. Round 10's F-B one layer out (F-B: different corpora; this: different requests).
#
# THE WIRING HALF FIRST: the ticket must be written BEFORE the pin, because the pin records its
# digest. Line-ordered, like the schema check above, because a pin that ran first would refuse
# every session on an absent template — i.e. the ordering is not a style question.
pin_write_line=$(grep -n 'write_session_corpus_pin' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
if [ -n "$ticket_line" ] && [ -n "$pin_write_line" ] && [ "$ticket_line" -lt "$pin_write_line" ]; then
  pass "round10 M1: the driver WRITES the Flight ticket at line $ticket_line, BEFORE it pins the session at line $pin_write_line (the pin records the ticket's digest, so the order is load-bearing)"
else
  fail "round10 M1: the ticket must be written before the pin (ticket=$ticket_line pin=$pin_write_line)"
fi

# THE ACCEPT DIRECTION: an untampered session reports, and the pin's ticket digest is a real
# 64-hex value that came from the file rather than a recorded string nobody measured.
d="$TMP/ticket-pin-ok"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" "$TMP/corpus" <<'PY'
import hashlib, json, pathlib, sys
pin = json.load(open(sys.argv[1]))["session_corpus_pin"]
digest = pin["pinned_ticket_sha256"]
assert len(digest) == 64 and digest == digest.lower(), pin
# The recorded digest must be the digest OF THE FILE — the whole difference between a pin and a
# string. Re-derived here independently of the reporter.
# FROM THE SESSION DIR since round 13's F2 — the ticket is a property of the session, and lives in
# the directory `claim_out_dir` owns exclusively, never in the shared corpus.
on_disk = hashlib.sha256((pathlib.Path(sys.argv[1]).parent / "ticket-template.json").read_bytes()).hexdigest()
assert digest == on_disk, (digest, on_disk)
assert pin["pinned_ticket_bytes"] > 0, pin
PY
then
  pass "OBSERVED (round10 M1): an untampered session REPORTS, and the pin's ticket digest is the digest of the ticket ON DISK (re-derived independently here), not a recorded string"
else
  fail "round10 M1: the ticket accept direction must record an observed digest (rc=$rc, out: $out)"
fi
# ...and the summary must SAY which request was measured, beside the corpus pin. A verified fact
# nobody prints leaves an operator comparing two arms with no way to see they answered one query.
# (Asserted on the reporter's OWN STDOUT, which is what the driver `tee`s to summary.txt — the
# harness runs the reporter directly, so there is no summary.txt to read here.)
if grep -q 'request pin' <<<"$out"; then
  pass "OBSERVED (round10 M1): the summary carries a 'request pin' line — WHICH QUERY is stated beside WHICH BYTES"
else
  fail "round10 M1: the summary must report the pinned request (out: $out)"
fi

# THE REFUSAL: a ticket mutated MID-SESSION. This is the finding — the template is the request, so
# a changed one means the reps did not all measure the same query.
d="$TMP/ticket-pin-swap"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-ticket-swap"
ws0_pin_session_corpus "$d" "$TMP/corpus-ticket-swap"
# MUTATED IN THE SESSION DIR (#3272 round 13, F2): that is where the request lives now.
python3 - "$d" <<'PY'
import json, pathlib, sys
# A LIMIT added to the request — a change that makes the Flight arm stream a fraction of the rows
# the bare-scan arm does, i.e. exactly the kind of edit that produces a meaningless ratio while
# every corpus digest still agrees.
p = pathlib.Path(sys.argv[1]) / "ticket-template.json"
j = json.loads(p.read_text())
j["limit"] = 1000
p.write_text(json.dumps(j, indent=1))
PY
out=$(run_report "$d" "$TMP/corpus-ticket-swap"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'THE FLIGHT TICKET CHANGED under this session' <<<"$out"; then
  pass "OBSERVED (round10 M1): a Flight ticket mutated MID-SESSION (a LIMIT added) is REFUSED by the pin — the request is not the one the reps measured"
else
  fail "round10 M1: a mid-session ticket mutation must be refused (rc=$rc, out: $out)"
fi
# NON-VACUITY, and this is the substance: the mutated state is invisible to EVERY OTHER check this
# report performs. Asserted by running the report over the SAME mutated corpus with the ticket
# field REMOVED from the pin's reader's reach — i.e. by establishing that the corpus, its complete
# component set and its schema are all still in agreement. If any of those diverged, the refusal
# above could have come from them and would prove nothing about the ticket.
if python3 - "$TMP/corpus-ticket-swap" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
ident = json.loads((root / "corpus-identity.json").read_text())
tbl = root / "ws0" / "events"
# Data.db, EVERY component and the schema all still hash to their recorded values: the mutation
# touched the request only, which is why nothing else can see it.
data = tbl / "nb-1-big-Data.db"
assert hashlib.sha256(data.read_bytes()).hexdigest() == ident["data_db_sha256"], "Data.db moved"
for name, spec in ident["components"].items():
    got = hashlib.sha256((tbl / name).read_bytes()).hexdigest()
    assert got == spec["sha256"], (name, got, spec["sha256"])
schema = hashlib.sha256((root / "ws0-events.cql").read_bytes()).hexdigest()
assert schema == ident["schema_sha256"], "the schema moved"
PY
then
  pass "NON-VACUITY (round10 M1): the mutated session's corpus, COMPLETE component set and schema ALL still agree with the recorded identity — so no other check could have caught it, and the ticket pin is what refused it"
else
  fail "round10 M1: the ticket-swap fixture must leave every other recorded input consistent, or it does not test the ticket pin"
fi
# ...and the PRE-FIX state accepted exactly this: a pin with NO ticket field is REFUSED rather than
# skipped. A session stamped by the old driver recorded no request at all, and treating "no record"
# as "nothing to check" is the fail-open shape — a check that did not run prints like one that
# passed.
d="$TMP/ticket-pin-absent"; make_session "$d" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/session-corpus-pin.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
j = json.loads(p.read_text())
del j["ticket_template_sha256"]          # exactly what a pre-M1 driver wrote
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records no .ticket_template_sha256' <<<"$out"; then
  pass "OBSERVED (round10 M1): a pin with NO ticket digest — what every pre-fix driver wrote — is REFUSED, not skipped (an unpinned request is the finding, not an exemption from it)"
else
  fail "round10 M1: an absent ticket pin must be refused (rc=$rc, out: $out)"
fi
# ...and a session whose TICKET IS GONE is refused too: every Flight rep read that file, so there
# is nothing left to establish which query the figures describe.
d="$TMP/ticket-missing"; make_session "$d" "$GOOD_FLIGHT"
cp -R "$TMP/corpus" "$TMP/corpus-ticket-missing"
ws0_pin_session_corpus "$d" "$TMP/corpus-ticket-missing"
# REMOVED FROM THE SESSION DIR (#3272 round 13, F2).
rm -f "$d/ticket-template.json"
out=$(run_report "$d" "$TMP/corpus-ticket-missing"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'pinned Flight ticket' <<<"$out"; then
  pass "OBSERVED (round10 M1): a session whose pinned ticket is MISSING at report time is refused naming the file"
else
  fail "round10 M1: a missing pinned ticket must be refused (rc=$rc, out: $out)"
fi

# ==========================================================================
# ROUND 13, F2 — THE TICKET IS THE SESSION'S, NOT THE CORPUS'S
# ==========================================================================
# M1 above correctly brought the Flight ticket into the provenance guarantee, and wrote it into the
# SHARED CORPUS DIRECTORY. Two consequences, both real:
#
#   * TWO CONCURRENT SESSIONS OVER ONE CORPUS COLLIDE. A corpus is a 2.8 GB artifact whose whole
#     point is to be generated once and measured repeatedly, so two lanes measuring it at the same
#     time is the ordinary case. Both drivers wrote `<corpus>/ticket-template.json`, so B's write
#     landed BETWEEN A's pin and A's reps: a differing shape made A refuse at the end of a
#     multi-minute run, diagnosing a mid-session mutation nobody performed, and an identical shape
#     agreed silently — a guarantee held by luck rather than by ownership.
#   * IT FORCED AN IMMUTABLE CORPUS TO BE WRITABLE. Nothing else in a measurement writes to the
#     corpus, so it can be mounted read-only, chmod'ed `a-w`, or shared between users — except that
#     every session had to create a file in it.
#
# The ticket now lives in the session's OUTPUT DIR, which `claim_out_dir` claims EXCLUSIVELY. Same
# ownership move round 12's F2 made for the measured binaries (copied into a session-owned
# `measured-bin/` and hashed at the destination), reused rather than reinvented.
#
# CASE 1 — TWO SESSIONS, ONE CORPUS, DIFFERENT REQUESTS: both report, neither is disturbed.
cp -R "$TMP/corpus" "$TMP/corpus-shared"
dA="$TMP/ticket-concurrent-a"; make_session "$dA" "$GOOD_FLIGHT"
dB="$TMP/ticket-concurrent-b"; make_session "$dB" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$dA" "$TMP/corpus-shared"
ws0_pin_session_corpus "$dB" "$TMP/corpus-shared"
# Session B's request is DIFFERENT — a LIMIT, the exact edit the M1 mutation case uses. Pre-fix this
# single file was shared, so writing it here would have changed A's request too; A's pin then
# refused (or agreed by luck). Post-fix the two are separate files and B's write cannot reach A.
python3 - "$dB" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "ticket-template.json"
j = json.loads(p.read_text())
j["limit"] = 1000
p.write_text(json.dumps(j, indent=1))
PY
# ...and B RE-PINS over its own modified ticket, which is what a real second session does: it writes
# its request and then pins it. (A is already pinned — that is the interleaving under test.)
python3 - "$REPO_ROOT/scripts/perf" "$dB" <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import measure_ticket_digest
d = pathlib.Path(sys.argv[2])
pin = json.loads((d / "session-corpus-pin.json").read_text())
pin["ticket_template_sha256"] = measure_ticket_digest(d)
(d / "session-corpus-pin.json").write_text(json.dumps(pin, indent=1) + "\n")
PY
outA=$(run_report "$dA" "$TMP/corpus-shared"); rcA=$?
outB=$(run_report "$dB" "$TMP/corpus-shared"); rcB=$?
if [ "$rcA" -eq 0 ] && [ "$rcB" -eq 0 ]; then
  pass "OBSERVED (round13 F2): TWO sessions over ONE corpus, with DIFFERENT requests, BOTH report — the ticket is per-session, so B's write cannot land between A's pin and A's reps"
else
  fail "round13 F2: two concurrent sessions over one corpus must not collide (rcA=$rcA rcB=$rcB, A: $outA, B: $outB)"
fi
# NON-VACUITY, and it MEASURES the pre-fix collision rather than arguing it: the two sessions really
# do carry DIFFERENT pinned requests (so pre-fix, over one shared file, at most one of them could
# have been right), and the corpus holds NO ticket at all.
if python3 - "$dA" "$dB" "$TMP/corpus-shared" <<'PY'
import json, pathlib, sys
a, b, corpus = (pathlib.Path(x) for x in sys.argv[1:4])
pa = json.loads((a / "session-corpus-pin.json").read_text())["ticket_template_sha256"]
pb = json.loads((b / "session-corpus-pin.json").read_text())["ticket_template_sha256"]
assert pa != pb, ("the two sessions must pin DIFFERENT requests, or the collision is not modelled", pa)
# Each digest is of THAT session's OWN file — the property a shared path cannot have.
import hashlib
for d, pinned in ((a, pa), (b, pb)):
    got = hashlib.sha256((d / "ticket-template.json").read_bytes()).hexdigest()
    assert got == pinned, (d, got, pinned)
assert not (corpus / "ticket-template.json").exists(), \
    "the corpus must hold NO ticket — pre-fix this was the ONE shared file both sessions wrote"
PY
then
  pass "NON-VACUITY (round13 F2): the two sessions pin DIFFERENT requests, each digest is of THAT session's OWN file, and the shared corpus holds NO ticket — pre-fix there was one file and the second writer won"
else
  fail "round13 F2: the concurrency fixture must model two different requests over one corpus"
fi
# CASE 2 — A READ-ONLY CORPUS. The guarantee stated positively: nothing in a session writes to the
# corpus, so a corpus with no write permission must still be measurable. Pre-fix the ticket write
# was the ONE thing that made this impossible.
cp -R "$TMP/corpus" "$TMP/corpus-readonly"
chmod a-w "$TMP/corpus-readonly" "$TMP/corpus-readonly/ws0" "$TMP/corpus-readonly/ws0/events"
d="$TMP/ticket-readonly-corpus"; make_session "$d" "$GOOD_FLIGHT"
# The fixture's pin path calls the SHIPPED ticket writer, so if anything still wrote into the corpus
# this would fail here rather than in the report.
ws0_pin_session_corpus "$d" "$TMP/corpus-readonly"
out=$(run_report "$d" "$TMP/corpus-readonly"); rc=$?
# NON-VACUITY for the fixture: the corpus really is unwritable on this box (a root-owned test run,
# or a filesystem ignoring mode bits, would make the case vacuous — so it is MEASURED, and reported
# rather than silently skipped).
if : > "$TMP/corpus-readonly/probe-write" 2>/dev/null; then
  rm -f "$TMP/corpus-readonly/probe-write"
  fail "round13 F2: the read-only corpus fixture is NOT read-only on this box (running as root, or a filesystem ignoring mode bits), so this case cannot test the property"
else
  pass "NON-VACUITY (round13 F2): the fixture corpus really REFUSES a write, so the case below tests the property rather than passing for free"
fi
if [ "$rc" -eq 0 ] && python3 - "$d" "$TMP/corpus-readonly" <<'PY'
import json, pathlib, sys
d, corpus = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
assert (d / "ticket-template.json").is_file(), "the ticket must be in the SESSION dir"
assert not (corpus / "ticket-template.json").exists(), "nothing may be written into the corpus"
pin = json.loads((d / "results.json").read_text())["session_corpus_pin"]
assert len(pin["pinned_ticket_sha256"]) == 64, pin
PY
then
  pass "OBSERVED (round13 F2): a session over a READ-ONLY corpus pins its request and REPORTS — the ticket lands in the session dir and the corpus is never written to (pre-fix the ticket write made a read-only corpus unmeasurable)"
else
  fail "round13 F2: a read-only corpus must still be measurable (rc=$rc, out: $out)"
fi
chmod u+w "$TMP/corpus-readonly" "$TMP/corpus-readonly/ws0" "$TMP/corpus-readonly/ws0/events"
# ...and the DRIVER must resolve the ticket path under `$OUT_DIR`, not under `$CORPUS`. Structural,
# because the wiring is what makes the two cases above properties of a real run rather than of the
# fixtures: a driver still writing into the corpus would satisfy neither.
#
# Asserted over the DRIVER **and** its libraries, because F2's campsite split moved the assignment
# into `lib-inputs.sh` (beside the write that creates the file, the way `record_measured_binaries`
# reassigns `$BIN` beside the freeze). Scanning only the driver would have read as "no assignment
# anywhere" and passed the negative half for free — so both halves are asserted over the same set of
# files, and the ABSENCE of any assignment fails.
ticket_var_line=$(grep -n '^ *TICKET_TEMPLATE=' \
  "$REPO_ROOT/scripts/perf/ws0-baseline.sh" "$REPO_ROOT"/scripts/perf/lib-*.sh | head -1)
if grep -qE '^ *TICKET_TEMPLATE="\$OUT_DIR/' \
     "$REPO_ROOT/scripts/perf/ws0-baseline.sh" "$REPO_ROOT"/scripts/perf/lib-*.sh \
   && ! grep -qE '^ *TICKET_TEMPLATE="\$CORPUS/' \
     "$REPO_ROOT/scripts/perf/ws0-baseline.sh" "$REPO_ROOT"/scripts/perf/lib-*.sh; then
  pass "round13 F2 wired: the rig resolves the ticket under \$OUT_DIR, never \$CORPUS ($ticket_var_line)"
else
  fail "round13 F2: TICKET_TEMPLATE must resolve under \$OUT_DIR (got: ${ticket_var_line:-<absent>})"
fi
# ...and it must be written AFTER the output dir is created and claimed, or `$OUT_DIR` would be
# empty at that point and the write would land at a path rooted at nothing.
outdir_line=$(grep -n 'OUT_DIR="\$(create_out_dir' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
ticket_write_line=$(grep -n '^write_ticket_template_for_session' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
if [ -n "$outdir_line" ] && [ -n "$ticket_write_line" ] && [ "$outdir_line" -lt "$ticket_write_line" ]; then
  pass "round13 F2 wired: the output dir is created+claimed at line $outdir_line BEFORE the ticket is written at line $ticket_write_line (the session must own the directory before it writes its request into it)"
else
  fail "round13 F2: the out dir must be created before the ticket is written (outdir=$outdir_line ticket=$ticket_write_line)"
fi

# ==========================================================================
# ROUND 9, F6 — "VERIFIED PHYSICAL-CORE SIBLINGS" MUST REST ON A RECORDED OBSERVATION
# ==========================================================================
# The reporter printed, unconditionally:
#
#     "verified": "thread_siblings_list, fail-closed (scripts/perf/lib-cpu.sh)"
#     pinning      : server 2,10 (verified physical-core siblings), client 4,12
#
# ...about CPU lists it read from the session manifest and NEVER validated. The manifest reader
# deliberately declines to re-check them (correctly — that would be a second implementation of
# `cpu_list_expand`), but the check that DID run was against the DRIVER'S ARGV, and nothing tied
# the argv to the string the manifest recorded. So F1 closed the argv substitution and the
# identical false claim survived one layer in, via the artifact.
#
# The fix records the verification where it was made (the driver, against the real sysfs on the
# real measuring host) and asserts it where it is used. NOT re-derived at report time: a results
# dir is routinely reviewed on another host, whose topology describes a machine that never ran the
# measurement.
f6_dir="$TMP/f6-tampered"; make_session "$f6_dir" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$f6_dir" "$TMP/corpus"
# THE REVIEWER'S MEASURED CASE: the manifest's CPU lists are edited to CPUs no verification was
# ever performed against. Pre-fix this exited 0 printing `server 99,99 (verified physical-core
# siblings)` — byte-identical to the pre-F1 defect line.
python3 - "$f6_dir/session-corpus-pin.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p))
j["config"]["server_cpus"] = "99,99"
j["config"]["client_cpus"] = "77,77"
json.dump(j, open(p, "w"), indent=1)
PY
out=$(run_report "$f6_dir" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'was performed against' <<<"$out"; then
  pass "OBSERVED (round9 F6): a manifest whose CPU pins were edited to 99,99 is REFUSED — the verification on record is about different CPUs (pre-fix: exit 0, printing 'server 99,99 (verified physical-core siblings)')"
else
  fail "round9 F6: a tampered CPU pin must be refused (rc=$rc, out: $(head -4 <<<"$out"))"
fi
# ...and the refusal must NAME BOTH VALUES, or an operator cannot tell which artifact was edited.
if grep -q "'99,99'" <<<"$out" && grep -q "'2,10'" <<<"$out"; then
  pass "OBSERVED (round9 F6): the refusal names BOTH the manifest's value and the verified one (which artifact was edited is the operator's next question)"
else
  fail "round9 F6: the refusal must name both values (out: $(head -4 <<<"$out"))"
fi
# AN ABSENT RECORD IS REFUSED, not silently trusted — the pre-fix state of every session dir.
f6_absent="$TMP/f6-absent"; make_session "$f6_absent" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$f6_absent" "$TMP/corpus"
rm -f "$f6_absent/pinning-verification.json"
out=$(run_report "$f6_absent" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'does not record' <<<"$out" \
   && grep -q 'pinning-verification.json' <<<"$out"; then
  pass "OBSERVED (round9 F6): a session dir with NO pinning-verification.json is REFUSED (a 'verified' claim resting on nothing is the finding, not an exemption from it)"
else
  fail "round9 F6: an absent pinning record must be refused (rc=$rc, out: $(head -4 <<<"$out"))"
fi
# THE ACCEPT DIRECTION, and it is what the whole fix is for: an untampered session REPORTS, and
# the printed claim NAMES ITS EVIDENCE rather than asserting the word "verified" over a module.
f6_ok="$TMP/f6-ok"; make_session "$f6_ok" "$GOOD_FLIGHT"
out=$(run_report "$f6_ok" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'physical-core siblings' <<<"$out" \
   && grep -q 'recorded in pinning-verification.json' <<<"$out"; then
  pass "OBSERVED (round9 F6): an untampered session REPORTS, and the pinning line cites the RECORDED observation (host + artifact) rather than printing 'verified' over a module name"
else
  fail "round9 F6: an honest session must report with an evidence-citing pinning line (rc=$rc, out: $(grep pinning <<<"$out" | head -2))"
fi
# ...and results.json must carry the record's OWN provenance limit, so the document takes ONE
# posture about its artifacts. The contradiction was the compounding half of the finding:
# `recorded_round_metadata.source` said "provenance UNVERIFIED" two fields away from an
# unconditional "verified".
if python3 - "$f6_ok/results.json" <<'PY'
import json, sys
j = json.load(open(sys.argv[1]))
v = j["pinning"]["verification"]
assert "written BY THE DRIVER" in v["provenance"], v
assert v["server_cpus"] == "2,10", v
assert "NOT re-derived" in v["note"] or "not re-derived" in v["note"], v
# The bare unconditional claim must be GONE, not merely supplemented.
assert "verified" not in j["pinning"], j["pinning"].keys()
PY
then
  pass "OBSERVED (round9 F6): results.json carries the record's OWN provenance limit and its re-derivation caveat, and the unconditional \`\"verified\": …\` string is GONE (one document, one posture about its artifacts)"
else
  fail "round9 F6: results.json must carry the recorded provenance and drop the unconditional verified string"
fi

# ==========================================================================
# ROUND 9, F7 — THE MANIFEST CONFIG SURFACE HAS AN ORACLE IN BOTH DIRECTIONS
# ==========================================================================
# `MANIFEST_CONFIG_FIELDS` had NO oracle, unlike its two siblings in this rig
# (`RECORD_FIELD_DISPOSITION` is walked against the live StepRecord; `COMPARED_FIELDS` against the
# artifact's real key set). MEASURED: adding an 8th field to the tuple left the reader returning
# the same 7 keys with no error, and an unclassified `config.prewarm_mode="DISABLED-ENTIRELY"` was
# silently ignored — rc=0, absent from results.json. All 7 declared fields WERE read, so there was
# no live unread field; nothing forced the next one. And this was the ROOT OF F6: `server_cpus`
# sat in that list as an opaque string and reached the report's "verified" claim validated by
# nothing.
f7_dir="$TMP/f7-unclassified"; make_session "$f7_dir" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$f7_dir" "$TMP/corpus"
python3 - "$f7_dir/session-corpus-pin.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p))
j["config"]["prewarm_mode"] = "DISABLED-ENTIRELY"
json.dump(j, open(p, "w"), indent=1)
PY
out=$(run_report "$f7_dir" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'never classified' <<<"$out" \
   && grep -q 'prewarm_mode' <<<"$out"; then
  pass "OBSERVED (round9 F7): a manifest carrying an UNCLASSIFIED config field (prewarm_mode) is REFUSED naming it (pre-fix: silently ignored, rc=0, absent from results.json)"
else
  fail "round9 F7: an unclassified config field must be refused (rc=$rc, out: $(head -4 <<<"$out"))"
fi
# THE OTHER DIRECTION, asserted AT IMPORT: a field declared and never wired is an error. Driven
# by mutating the disposition in a subprocess — the assertion is real code, not a comment.
if out=$(python3 - <<'PY' 2>&1
import sys
sys.path.insert(0, "scripts/perf")
import ws0_session as s
# Simulate the F7 hazard: an 8th declared field the reader does not produce. Re-running the
# import-time check over a mutated disposition is what the guard is; if it passes, a declared
# field could be added with no wiring and never appear in results.json.
d = dict(s.MANIFEST_CONFIG_DISPOSITION); d["prewarm_mode"] = "an 8th field nobody wired"
missing = [k for k in d if k not in s._MANIFEST_READER_KEYS]
assert missing == ["prewarm_mode"], missing
print("DECLARED-BUT-UNREAD DETECTED")
PY
) && grep -q 'DECLARED-BUT-UNREAD DETECTED' <<<"$out"; then
  pass "OBSERVED (round9 F7): the declared-but-unread direction is detectable — an 8th declared field absent from the reader's key set is identified (the import-time check refuses it)"
else
  fail "round9 F7: a declared-but-unwired field must be detected (out: $(head -4 <<<"$out"))"
fi
# ...and every declared field must state HOW it is validated — an opaque declaration is how
# `server_cpus` reached a "verified" claim unchecked (F6). Both siblings' postures asserted:
# `server_cpus` must name the module that DOES tie it to a real verification.
if python3 - <<'PY'
import sys
sys.path.insert(0, "scripts/perf")
from ws0_session import MANIFEST_CONFIG_DISPOSITION as D, MANIFEST_CONFIG_FIELDS as F
assert set(D) == set(F), (sorted(D), sorted(F))
for k, why in D.items():
    assert why.strip(), k
# The F6 lesson recorded at the declaration: "opaque here" must name where it is NOT opaque.
for k in ("server_cpus", "client_cpus"):
    assert "ws0_pinning" in D[k], (k, D[k])
PY
then
  pass "OBSERVED (round9 F7): every declared field states HOW it is validated, and the two CPU-list fields name ws0_pinning as what ties them to a real verification (the F6 root, recorded at the declaration)"
else
  fail "round9 F7: each field must declare its validation, and the CPU lists must name ws0_pinning"
fi
# NON-VACUITY / THE ACCEPT DIRECTION: an untampered manifest still reports, so the surface check
# is not one that refuses every configuration.
f7_ok="$TMP/f7-ok"; make_session "$f7_ok" "$GOOD_FLIGHT"
out=$(run_report "$f7_ok" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "OBSERVED (round9 F7): a manifest carrying exactly the CLASSIFIED fields still reports (the surface check discriminates, it does not reject everything)"
else
  fail "round9 F7: a well-formed manifest must still report (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# ==========================================================================
# CANONICAL-CORPUS COMPARISON: moved to scripts/tests/test_ws0_canonical_corpus.sh
# ==========================================================================
# Round 13's F3 — is the measured corpus the one a WS0 BASELINE is DEFINED as? — is its own
# subject and its own suite. This file asks whether a report identifies the bytes it describes;
# that one asks whether those are the RIGHT bytes. Every check HERE is a SELF-CONSISTENCY check
# about whatever corpus was supplied, so all of it is equally true of a smoke-sized or
# differently-seeded corpus — which is precisely how one was published as a baseline. Its
# implementation is also a CROSS-LANGUAGE bridge to the Rust pin, which nothing else here is.

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its checks
# and passed them exits 0 and reports SUCCESS. That is the suite-level `0/0` shape this whole
# issue is about, one level up from the checks themselves.
#
# The floor is DERIVED from the OBSERVED count, set just below it so adding a case does not red
# the suite, and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it
# counts what actually RAN rather than what is written in the file.
#
# RE-DERIVED TWICE in round 13 (#3272), and the first re-derivation is why the second was easy.
# It read 100 against a real 103; the output-dir split moved 22 checks to
# `test_ws0_output_dir_exclusivity.sh`, leaving 81 MEASURED — so 100 was a floor ABOVE the real
# count and would have RED this suite. A floor left pointing at a count that no longer exists is
# the same defect in the other direction as one set too low: both are a number nobody re-measured.
# F3's 13 cases then took it to 94 MEASURED — and were themselves split out to
# `test_ws0_canonical_corpus.sh` on the same rule, bringing it back to 81. The floor is 78 again.
MIN_CHECKS=78
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 provenance guards: all $checks checks passed"
  exit 0
fi
echo "ws0 provenance guards: $fails of $checks check(s) FAILED"
exit 1

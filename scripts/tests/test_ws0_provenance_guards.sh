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
# ...and the HERMETIC driver harness, for round 6's R1 cases: the output-dir refusal is a
# property of the DRIVER (which session a rep file belongs to), so it belongs to this file's
# subject rather than to the cpu-pinning suite's measurement-apparatus one — and moving it here
# also kept that file under the ~1500-line test target it would otherwise have crossed.
# `ws0_driver_run` prepends `--validate-args-only` and the recording shims, so those cases
# execute nothing outside their own process.
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
# The recording shims for round 6's R1 cases, which run the real driver through
# `ws0_driver_run` (`--validate-args-only` + shimmed PATH) and assert it executed NOTHING.
ws0_hermetic_init "$TMP"

GOOD_FLIGHT='{"round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'

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
# ROUND 6, R1 — THE OUTPUT DIR IS NEVER REUSED, AND IS CREATED ATOMICALLY
# ==========================================================================
# It used to be `mkdir -p "$OUT_DIR"` over a default name with only SECOND-level uniqueness.
# Two ways that assembles ONE report from artifacts of DIFFERENT SESSIONS, and the reporter
# cannot see either — it reads whatever rep files are present:
#
#   * two runs started in the SAME SECOND share the default dir; `mkdir -p` succeeds for both,
#     and the second run's pin overwrites the first's;
#   * an explicit `--out` at a previous run's dir keeps that run's rep files, so any rep this
#     session does not overwrite (a different temperature or arm, a higher rep index from a
#     longer previous run) is read as part of THIS one.
#
# All three cases run through `ws0_driver_run`, i.e. `--validate-args-only`: the used-dir
# REFUSAL is deliberately placed ABOVE the argument boundary (it needs no perf, no topology, no
# corpus), which is what makes it observable hermetically. CREATION stays below the boundary, so
# these cases also assert that nothing is created.
r1_dir="$TMP/r1"
mkdir -p "$r1_dir/used" && : > "$r1_dir/used/warm-rep1-scan.perf.csv"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/used"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'already exists and is NOT EMPTY' <<<"$out" \
   && ws0_driver_ran_hermetically; then
  pass "OBSERVED (round6 R1): an explicit --out holding a previous session's rep file is REFUSED (pre-fix: mkdir -p reused it and the report mixed both sessions)"
else
  fail "round6 R1: a non-empty --out must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# NON-VACUITY / the ACCEPT direction, both halves — without these the refusal could be a check
# that rejects every --out, which would make the flag unusable rather than safe.
mkdir -p "$r1_dir/empty"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/empty"); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'ARGUMENTS OK' <<<"$out"; then
  pass "OBSERVED (round6 R1): an EXISTING-BUT-EMPTY --out is ACCEPTED (the guard discriminates on CONTENT, not on existence)"
else
  fail "round6 R1: an empty --out dir must be accepted (rc=$rc, out: $(head -3 <<<"$out"))"
fi
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/absent"); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'ARGUMENTS OK' <<<"$out" && [ ! -e "$r1_dir/absent" ]; then
  pass "OBSERVED (round6 R1): an ABSENT --out is accepted AND is NOT created above the argument boundary (--validate-args-only still creates nothing)"
else
  fail "round6 R1: an absent --out must be accepted without being created (rc=$rc, exists=$([ -e "$r1_dir/absent" ] && echo yes || echo no))"
fi
# A `--out` that exists as a FILE is refused too, with its own diagnosis rather than the
# non-empty one — a reader sent to "remove that directory" for a regular file is sent wrong.
: > "$r1_dir/afile"
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --out "$r1_dir/afile"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'is not a directory' <<<"$out"; then
  pass "OBSERVED (round6 R1): an --out that is a regular FILE is refused with its own diagnosis"
else
  fail "round6 R1: an --out naming a file must be refused as such (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# THE DEFAULT NAME'S UNIQUENESS RESTS ON AN ATOMIC CREATE, not on the name. Asserted
# STRUCTURALLY, because the racing behaviour itself cannot be driven deterministically from a
# self-test: the default branch must use `mkdir` WITHOUT `-p` (an existing dir is then an ERROR,
# which IS the exclusion) and the name must carry more than a second-resolution timestamp.
#
# THE SUBJECT IS `lib-outdir.sh`, not the driver (#3272 round 7): the output-directory lifecycle
# moved there under the campsite rule. The `-n "$r1_block"` guard is what caught the staleness —
# after the split this awk range matched NOTHING in the driver and the check FAILED rather than
# passing vacuously, which is why that guard was there. A range test without it would have gone
# green over an empty subject, and this file would have stopped checking R1 silently.
r1_block=$(awk '/^ *while :; do/,/^ *done/' "$REPO_ROOT/scripts/perf/lib-outdir.sh")
if [ -n "$r1_block" ] \
   && grep -qF 'if mkdir "$out_dir" 2>/dev/null; then' <<<"$r1_block" \
   && ! grep -qF 'mkdir -p "$out_dir"' <<<"$r1_block" \
   && grep -qF 'out_dir="$base/$ts-$$"' <<<"$r1_block"; then
  pass "round6 R1: the DEFAULT out dir is created with \`mkdir\` (no -p) — an atomic exclusive create — and its name carries the pid, not just a UTC second"
else
  fail "round6 R1: the default out dir must be created atomically without -p and be more than second-unique (block: $(head -5 <<<"$r1_block"))"
fi
# ...and the atomic-create claim is driven, not merely grepped: the SAME primitive the driver
# uses must actually refuse a second create of one name. This is the property `mkdir -p` lacks
# and is what makes the retry loop an exclusion rather than a decoration.
r1_race="$r1_dir/race"
if mkdir "$r1_race" 2>/dev/null && ! mkdir "$r1_race" 2>/dev/null; then
  pass "OBSERVED (round6 R1): \`mkdir\` without -p REFUSES a second create of the same name (the exclusion the default branch relies on), while \`mkdir -p\` would succeed twice"
else
  fail "round6 R1: the atomic-create primitive must fail on an existing dir, else the default branch's uniqueness is not enforced"
fi
if mkdir -p "$r1_race" 2>/dev/null; then
  pass "OBSERVED (round6 R1): the CONTROL — \`mkdir -p\` on that same existing dir SUCCEEDS, which is exactly why the pre-fix code reused a session dir"
else
  fail "round6 R1: mkdir -p must succeed on an existing dir; if it does not, the control proves nothing"
fi

# ==========================================================================
# ROUND 9, F4 — A FAILED USED-DIR ENUMERATION IS NOT AN EMPTY DIRECTORY
# ==========================================================================
# `require_unused_out_dir` asked "is this directory non-empty?" as
# `[[ -n "$(find … 2>/dev/null)" ]]`: the STATUS was discarded and stderr thrown away, so a
# `find` that FAILED produced no output and was indistinguishable from an empty directory — and
# the empty result takes the PERMISSIVE branch. R1's whole refusal then silently passes over a
# directory that may still hold another session's rep files, which the reporter reads as its own.
#
# The same class as `check-root-junk-files.sh`'s process-substitution enumeration (also F4), and
# fixed the same way: enumeration to a FILE, status captured, STATUS CHECKED BEFORE EMPTINESS,
# stderr KEPT and quoted.
#
# THE TRIGGER is a directory the enumeration cannot read: mode 0300 (write+execute, NO read) with
# a prior session's rep file inside it. `find` cannot list the entries, so it exits non-zero
# having printed nothing.
f4_dir="$TMP/f4-unreadable"
mkdir -p "$f4_dir" && : > "$f4_dir/warm-rep1-scan.perf.csv"
# `root` bypasses the read bit, so the trigger cannot be constructed as root. SKIPPED with a
# stated reason rather than passed: a case that could not run must never print like one that did.
if [ "$(id -u)" -eq 0 ]; then
  echo "SKIP (round9 F4): running as root, which bypasses the read bit — the unreadable-directory trigger cannot be constructed, so this case is NOT claimed as observed"
else
  chmod 300 "$f4_dir"
  # The SHIPPED library is sourced, never re-implemented (the discipline the F3 block established).
  f4_out=$( ( # shellcheck disable=SC1090
    source "$REPO_ROOT/scripts/perf/lib-outdir.sh"
    require_unused_out_dir "$f4_dir" ) 2>&1 ); f4_rc=$?
  chmod 700 "$f4_dir"
  if [ "$f4_rc" -ne 0 ] && grep -q 'could not enumerate --out' <<<"$f4_out" \
     && grep -q 'find exited' <<<"$f4_out"; then
    pass "OBSERVED (round9 F4): an UNREADABLE --out is refused as a FAILED ENUMERATION naming find's exit status (pre-fix: find's status was discarded, no output was produced, and the empty result took the PERMISSIVE branch — R1's refusal passed over a used directory)"
  else
    fail "round9 F4: a failed enumeration must be refused with find's status, not read as an empty dir (rc=$f4_rc, out: $(head -4 <<<"$f4_out"))"
  fi
  # NON-VACUITY, and it is the whole finding: the PRE-FIX expression, run against the SAME
  # directory, reports EMPTY. Driven rather than argued — this is what "indistinguishable" means.
  f4_prefix_empty=no
  [ -z "$(chmod 300 "$f4_dir"; find "$f4_dir" -mindepth 1 -print -quit 2>/dev/null)" ] && f4_prefix_empty=yes
  chmod 700 "$f4_dir"
  if [ "$f4_prefix_empty" = yes ]; then
    pass "OBSERVED (round9 F4): THE CONTROL — the pre-fix \`[[ -n \"\$(find … 2>/dev/null)\" ]]\` reports this same used-but-unreadable dir as EMPTY, i.e. a failed look was read as 'nothing there' and the guard passed"
  else
    fail "round9 F4: the pre-fix expression must be shown to report empty here, or the fix's necessity is unproven (the trigger did not reproduce)"
  fi
fi
# THE ACCEPT DIRECTION for the rewritten enumeration — without it, F4's fix could be a function
# that refuses every directory, which would break R1's accepted empty-dir case.
f4_ok="$TMP/f4-empty"; mkdir -p "$f4_ok"
f4_out=$( ( # shellcheck disable=SC1090
  source "$REPO_ROOT/scripts/perf/lib-outdir.sh"
  require_unused_out_dir "$f4_ok" ) 2>&1 ); f4_rc=$?
if [ "$f4_rc" -eq 0 ]; then
  pass "OBSERVED (round9 F4): a READABLE, EMPTY --out is still ACCEPTED by the status-checking enumeration (R1's accepted case survives)"
else
  fail "round9 F4: an empty readable dir must still be accepted (rc=$f4_rc, out: $(head -3 <<<"$f4_out"))"
fi
# ...and a readable NON-EMPTY dir is still refused with R1's used-directory diagnosis, not with
# F4's enumeration-failure one — two different faults must stay distinguishable to a reader.
f4_used="$TMP/f4-used"; mkdir -p "$f4_used" && : > "$f4_used/warm-rep1-scan.perf.csv"
f4_out=$( ( # shellcheck disable=SC1090
  source "$REPO_ROOT/scripts/perf/lib-outdir.sh"
  require_unused_out_dir "$f4_used" ) 2>&1 ); f4_rc=$?
if [ "$f4_rc" -ne 0 ] && grep -q 'already exists and is NOT EMPTY' <<<"$f4_out" \
   && ! grep -q 'could not enumerate' <<<"$f4_out"; then
  pass "OBSERVED (round9 F4): a READABLE non-empty --out still gets R1's used-directory diagnosis, distinct from the enumeration-failure one"
else
  fail "round9 F4: a readable non-empty dir must keep R1's diagnosis (rc=$f4_rc, out: $(head -3 <<<"$f4_out"))"
fi

# ==========================================================================
# ROUND 7, F3 — AN EXPLICIT --out IS CLAIMED ATOMICALLY, NOT JUST CREATED
# ==========================================================================
# R1 fixed the DEFAULT path (atomic `mkdir` on a unique name) and left the EXPLICIT path on
# `mkdir -p`, so the defect R1 was about survived one branch over: two concurrent runs given the
# same ABSENT-OR-EMPTY `--out` BOTH pass the used-directory refusal (empty for both, and both
# check before either writes) and BOTH `mkdir -p` succeed. Each then writes its session pin and
# rep artifacts over the other's, and the reporter assembles a median across MIXED SESSIONS —
# it reads whatever rep files are present and cannot tell.
#
# `mkdir` on `$OUT_DIR` itself is not available as the arbiter, because R1 deliberately ACCEPTS
# an existing-but-empty dir. So the exclusion is one level down: an atomic marker SUBDIRECTORY.
#
# The SHIPPED library is SOURCED, never re-implemented and never re-extracted from text — a
# reimplemented check in a test is a second thing to keep in sync, and its divergence would be
# invisible in exactly the permissive direction. `scripts/perf/lib-outdir.sh` owns the whole
# output-directory lifecycle (round 7's campsite-rule split; the driver was at 1035 lines).
f3_dir="$TMP/f3"; mkdir -p "$f3_dir"
OUTDIR_LIB="$REPO_ROOT/scripts/perf/lib-outdir.sh"
if [ -f "$OUTDIR_LIB" ] && grep -qF 'mkdir "$claim"' "$OUTDIR_LIB" \
   && ! grep -qF 'mkdir -p "$claim"' "$OUTDIR_LIB"; then
  pass "OBSERVED (round7 F3): lib-outdir.sh's exclusion is \`mkdir\` on the MARKER, with NO -p (an existing marker is an ERROR, which IS the exclusion)"
else
  fail "round7 F3: lib-outdir.sh must claim with mkdir (no -p) on the marker"
fi
f3_run() { # f3_run <dir> — run the SHIPPED claim_out_dir against <dir>; prints output, returns rc
  ( # shellcheck disable=SC1090
    source "$OUTDIR_LIB"
    claim_out_dir "$1" "self-test" ) 2>&1
}
mkdir -p "$f3_dir/shared"
out=$(f3_run "$f3_dir/shared"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "OBSERVED (round7 F3): the FIRST session claims an existing-but-EMPTY --out successfully (R1's accepted case still works)"
else
  fail "round7 F3: the first claim on an empty dir must succeed (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# THE RACE, which is the finding: a SECOND session handed the same dir must be REFUSED. Both
# runs saw it empty, so nothing above this line could separate them.
out=$(f3_run "$f3_dir/shared"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'ALREADY CLAIMED' <<<"$out"; then
  pass "OBSERVED (round7 F3): a SECOND session on the SAME --out is REFUSED as ALREADY CLAIMED (pre-fix: both mkdir -p succeeded and the report mixed two sessions)"
else
  fail "round7 F3: a second claim must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the refusal must NAME THE OWNER, or an operator cannot tell a live peer from a stale
# marker — the difference between waiting and picking a new directory.
if grep -q 'Claimed by: pid=' <<<"$out"; then
  pass "OBSERVED (round7 F3): the refusal NAMES the owning pid/host/start time (a live peer is distinguishable from a stale marker)"
else
  fail "round7 F3: the refusal must name the claim owner (out: $(head -4 <<<"$out"))"
fi
# NON-VACUITY / THE CONTROL: the pre-fix explicit path really did admit both. `mkdir -p` twice on
# the same dir succeeds twice, which is the whole finding — asserted rather than reasoned about.
mkdir -p "$f3_dir/prefix-control"
if mkdir -p "$f3_dir/prefix-control" 2>/dev/null && mkdir -p "$f3_dir/prefix-control" 2>/dev/null; then
  pass "OBSERVED (round7 F3): NON-VACUITY — the pre-fix \`mkdir -p\` on an explicit --out SUCCEEDS REPEATEDLY, so two concurrent sessions both proceeded (this is F3)"
else
  fail "round7 F3: mkdir -p must succeed repeatedly, else the finding's premise is wrong"
fi
# A DIFFERENT dir is unaffected — a claim that refused everything would satisfy the case above
# and make `--out` unusable.
mkdir -p "$f3_dir/other"
out=$(f3_run "$f3_dir/other"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "OBSERVED (round7 F3): a DIFFERENT --out still claims fine (the guard discriminates on the DIRECTORY, not unconditionally)"
else
  fail "round7 F3: an unclaimed dir must be claimable (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# BOTH branches go through the SAME mechanism, so a future edit cannot re-split them (which is
# exactly what F3 was: R1 hardened one branch and left the other). Driven for the EXPLICIT branch
# above; structural for the DEFAULT branch, whose race is not deterministically drivable from a
# self-test — asserted as "both code paths in `create_out_dir` claim".
f3_calls=$(grep -cF 'claim_out_dir "$out_dir"' "$OUTDIR_LIB")
if [ "$f3_calls" -eq 2 ]; then
  pass "round7 F3: BOTH branches of create_out_dir claim (one mechanism, $f3_calls call sites — R1 hardened only one branch, which IS F3)"
else
  fail "round7 F3: both --out branches must claim (found $f3_calls call sites in lib-outdir.sh, expected 2)"
fi
# ...and the DEFAULT branch is DRIVEN too, for the property that IS drivable: it creates a fresh
# unique dir and claims it, twice in a row, without collision — so the claim added to that branch
# has not broken the retry loop R1 built.
f3_base="$f3_dir/defaults"
f3_default() {
  ( # shellcheck disable=SC1090
    source "$OUTDIR_LIB"
    create_out_dir "" "$1" ) 2>&1
}
d1=$(f3_default "$f3_base"); rc1=$?
d2=$(f3_default "$f3_base"); rc2=$?
if [ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ] && [ -d "$d1" ] && [ -d "$d2" ] && [ "$d1" != "$d2" ]; then
  pass "OBSERVED (round7 F3): the DEFAULT branch creates and claims two DISTINCT dirs back-to-back (the claim did not break R1's retry loop)"
else
  fail "round7 F3: two default-branch runs must yield two distinct claimed dirs (rc=$rc1/$rc2 d1=$d1 d2=$d2)"
fi
# ...and CREATION/CLAIMING stays BELOW the argument boundary, so `--validate-args-only` still
# creates nothing. Asserted by LINE ORDER against the boundary's own exit, because the
# behavioural half (an absent --out is not created) is already driven above and this is the
# property that keeps it true.
f3_boundary=$(grep -nF 'ARGUMENTS OK (--validate-args-only)' "$DRIVER" | head -1 | cut -d: -f1)
f3_create=$(grep -nF 'OUT_DIR="$(create_out_dir' "$DRIVER" | head -1 | cut -d: -f1)
if [ -n "$f3_boundary" ] && [ -n "$f3_create" ] && [ "$f3_boundary" -lt "$f3_create" ]; then
  pass "round7 F3: create_out_dir (line $f3_create) is BELOW the --validate-args-only boundary (line $f3_boundary) — the hermetic mode still creates nothing"
else
  fail "round7 F3: creation must stay below the argument boundary (boundary=$f3_boundary create=$f3_create)"
fi
# ...and the REFUSAL stays ABOVE it, which is what makes the used-dir case observable hermetically
# at all. The two halves on opposite sides of the boundary is the design, so both are pinned.
f3_refusal=$(grep -nF 'require_unused_out_dir "${OUT_DIR:-}"' "$DRIVER" | head -1 | cut -d: -f1)
if [ -n "$f3_refusal" ] && [ -n "$f3_boundary" ] && [ "$f3_refusal" -lt "$f3_boundary" ]; then
  pass "round7 F3: require_unused_out_dir (line $f3_refusal) is ABOVE the boundary (line $f3_boundary) — which is why the used-dir refusal is hermetically observable"
else
  fail "round7 F3: the used-dir refusal must stay above the argument boundary (refusal=$f3_refusal boundary=$f3_boundary)"
fi
# The driver checks create_out_dir's STATUS EXPLICITLY. It runs in a COMMAND SUBSTITUTION (it must
# echo the default name it chose), so its `exit 2` kills only that subshell and the driver survives
# on `set -e` alone. That works — and a fail-closed refusal enforced only by an implicit shell
# option is one `set +e` from being decorative, which is this issue's whole subject.
if grep -qF 'OUT_DIR="$(create_out_dir "${OUT_DIR:-}" "$REPO_ROOT/target/perf-ws0-3096")" || exit 2' "$DRIVER"; then
  pass "round7 F3: the driver checks create_out_dir's status EXPLICITLY (\`|| exit 2\`), not via set -e alone"
else
  fail "round7 F3: the create_out_dir call must check its status explicitly — its exit 2 only kills the command substitution"
fi

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
schema_line=$(grep -n 'verify_schema_input' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
# The GENERATION site, not the variable assignment: `TICKET_TEMPLATE=` (line ~359) is just a
# path, while the `python3 - "$DDL_FILE" "$TICKET_TEMPLATE"` heredoc is where the ticket is
# actually DERIVED from the DDL. Anchoring on the assignment measured the wrong line and failed
# this case for the wrong reason.
ticket_line=$(grep -n 'python3 - "\$DDL_FILE" "\$TICKET_TEMPLATE"' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
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
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a
# `$(...)` whose command vanished, a `for` over an empty list — LOWERS the check count and
# registers NO failure. The gate reads only the exit code, so a suite that ran 3 of its checks
# and passed them exits 0 and reports SUCCESS. That is the suite-level `0/0` shape this whole
# issue is about, one level up from the checks themselves.
#
# The floor is DERIVED from the OBSERVED count (43 at the split, measured by instrumenting
# `pass`/`fail` to report their call site), set just below it so adding a case does not red the
# suite, and far above zero. `$checks` is incremented by `pass`/`fail` themselves, so it counts
# what actually RAN rather than what is written in the file.
MIN_CHECKS=64
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

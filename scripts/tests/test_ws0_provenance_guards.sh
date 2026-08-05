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
r1_block=$(awk '/^  _ws0_base=/,/^  unset _ws0_base/' "$DRIVER")
if [ -n "$r1_block" ] \
   && grep -q 'if mkdir "\$OUT_DIR" 2>/dev/null; then' <<<"$r1_block" \
   && ! grep -q 'mkdir -p "\$OUT_DIR"' <<<"$r1_block" \
   && grep -q 'OUT_DIR="\$_ws0_base/\$TS-\$\$"' <<<"$r1_block"; then
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
MIN_CHECKS=54
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

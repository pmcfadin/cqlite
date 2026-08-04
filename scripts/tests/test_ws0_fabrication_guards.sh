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

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

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
# Fixture builders
# --------------------------------------------------------------------------
perf_csv() { # perf_csv <path> <cycles> <instructions>
  printf '%s,,cycles,,,,\n%s,,instructions,,,,\n' "$2" "$3" > "$1"
}

# make_corpus <dir> [rows] — a corpus whose recorded identity MATCHES the synthetic
# `Data.db` it also writes, so the byte verification passes on the happy path.
# `data_db_bytes` and `data_db_sha256` are MEASURED from the file, never asserted.
make_corpus() {
  local dir="$1" rows="${2:-$CORPUS_ROWS}" table
  table="$dir/ws0/events"
  mkdir -p "$table"
  # A few KB of deterministic bytes: the verification must work at ANY size (the real
  # corpus is 2.8 GB, and a test may not write one).
  python3 - "$table/nb-1-big-Data.db" <<'PY'
import sys
open(sys.argv[1], "wb").write(bytes(range(256)) * 16)
PY
  python3 - "$dir" "$table/nb-1-big-Data.db" "$rows" <<'PY'
import hashlib, json, os, sys
out, data, rows = sys.argv[1], sys.argv[2], int(sys.argv[3])
raw = open(data, "rb").read()
ident = {
    "rows": rows, "partitions": 10, "seed": 1, "cells_per_row": 12,
    "data_db_bytes": len(raw),
    "data_db_sha256": hashlib.sha256(raw).hexdigest(),
    "bytes_per_row": len(raw) / rows,
}
json.dump(ident, open(os.path.join(out, "corpus-identity.json"), "w"))
PY
}

make_scan_rep() { # make_scan_rep <dir> <temp> <rep> <prewarm>
  local d="$1" tag="scan-$2-$3"
  cat > "$d/$tag.json" <<EOF
{ "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5 }
EOF
  perf_csv "$d/perf-$tag.csv" 2000000 4000000
  perf_csv "$d/perf-$tag-setup.csv" 100000 200000
  printf '%s\n' "$4" > "$d/$tag.prewarm.status"
}

# make_flight_rep <dir> <temp> <rep> <prewarm> <jsonl-body>
# The JSONL body is given VERBATIM so a case can omit a key or supply two records.
make_flight_rep() {
  local d="$1" tag="flight-bypass-$2-$3"
  printf '%s\n' "$5" > "$d/$tag.jsonl"
  perf_csv "$d/perf-$tag.csv" 8000000 16000000
  printf '%s\n' "$4" > "$d/$tag.prewarm.status"
}

GOOD_FLIGHT='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'

# make_session <dir> <flight-jsonl> — a complete one-warm-rep session dir.
make_session() {
  mkdir -p "$1"
  make_scan_rep "$1" warm 1 ok
  make_flight_rep "$1" warm 1 ok "$2"
}

run_report() { # run_report <dir> <corpus> [extra args…]
  local d="$1" c="$2"; shift 2
  python3 "$REPORT" --dir "$d" --corpus "$c" --server-cpus 2,10 \
    --client-cpus 4,12 --reps 1 --temps warm --arms bypass \
    --step-duration 45s/1s --scan-passes 1 "$@" 2>&1
}

# expect_reject <label> <expect-substring> <dir> <corpus> [extra…]
expect_reject() {
  local label="$1" expect="$2"; shift 2
  local out rc
  out=$(run_report "$@"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "$expect" <<<"$out"; then
    pass "$label"
  else
    fail "$label: expected non-zero + '$expect' (rc=$rc, out: $out)"
  fi
}

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
NO_ERR_KEY='{"round":"r","requests_ok":1,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
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
BAD_ERR='{"round":"r","requests_ok":1,"requests_error":"none","rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/bad-error-key"; make_session "$d" "$BAD_ERR"
expect_reject "an UNPARSEABLE requests_error is FATAL (corrupt, not 0)" \
  "unparseable \`requests_error\`" "$d" "$TMP/corpus"
# And a real non-zero error count is still refused, naming it — the guard the
# fabricated default was standing in for must still work.
REAL_ERR='{"round":"r","requests_ok":1,"requests_error":4,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/real-errors"; make_session "$d" "$REAL_ERR"
expect_reject "an OBSERVED non-zero requests_error is refused, naming the count" \
  "had 4 failed request" "$d" "$TMP/corpus"
# An explicit ZERO is accepted: the fix is "observe it", not "reject the key".
ZERO_ERR='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":250000.0,"duration_s":4.0}'
d="$TMP/zero-errors"; make_session "$d" "$ZERO_ERR"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "an OBSERVED requests_error of 0 is accepted (the fix demands observation, not absence)"
else
  fail "requests_error=0 must be accepted (rc=$rc, out: $out)"
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
  printf '%s\n' '{"round":"ramp-1","requests_ok":1,"requests_error":9,"rows_total":37,"rows_per_s":99.0,"duration_s":1.0}'
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
ZERO_RPS='{"round":"r","requests_ok":1,"requests_error":0,"rows_total":1000,"rows_per_s":0.0,"duration_s":4.0}'
d="$TMP/zero-rps"; make_session "$d" "$ZERO_RPS"
expect_reject "a rep series with a ZERO median is REFUSED (never 'spread 0.0%')" \
  "non-positive median" "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus")
# The report LINE must be absent — asserted on the line's own shape, not on the
# phrase, because the refusal text quotes the phrase it is refusing.
if ! grep -q 'rows/s  \[0\.\.0' <<<"$out" && ! grep -q 'bare/flight = infx' <<<"$out"; then
  pass "neither a 0-rows/s figure nor an 'inf' ratio LINE is printed for the degenerate series"
else
  fail "the degenerate series must not be printed at all (out: $out)"
fi
# STRUCTURAL, over the EXECUTABLE source only: docstrings are stripped via `ast`
# before the scan, because the comments explaining each fix necessarily quote the
# idiom they removed — a literal grep over the raw file would red on its own
# documentation, and the obvious "fix" for that would be to stop documenting it.
if python3 - "$REPO_ROOT/scripts/perf/ws0_report.py" <<'PY'
import ast, sys

tree = ast.parse(open(sys.argv[1]).read())
for node in ast.walk(tree):
    if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        body = node.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                and isinstance(body[0].value.value, str):
            node.body = body[1:] or [ast.Pass()]
code = ast.unparse(ast.fix_missing_locations(tree))

# Each of the five permissive-default idioms round 1 found, as it appeared in the
# executable source. `ast.unparse` normalises quoting to single quotes.
banned = {
    "if med else 0.0": "spread() still defaults a zero-median spread to 0.0",
    "float('inf')": "the bare/flight ratio still falls back to inf",
    "get('requests_error', 0)": "requests_error still reads through a defaulting get",
    "get('prewarm_all_ok', True)": "prewarm_all_ok still defaults to the PERMISSIVE value",
    "records[-1]": "the reporter still consumes only the LAST step record",
}
hits = [why for idiom, why in banned.items() if idiom in code]
if hits:
    raise SystemExit("; ".join(hits))
PY
then
  pass "STRUCTURAL: none of the five permissive-default idioms remains in the EXECUTABLE source"
else
  fail "a permissive-default idiom is still present in ws0_report.py"
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

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 fabrication guards: all checks passed"
  exit 0
fi
echo "ws0 fabrication guards: $fails check(s) FAILED"
exit 1

#!/usr/bin/env bash
# test_ws0_binary_provenance.sh — WHICH PROGRAMS PRODUCED THE RATIO
# (issue #3272; split from `test_ws0_provenance_guards.sh` in review round 11).
#
# # Why this is its own suite
#
# Split under the campsite rule (~1500-line test target: the parent reached 1652 with round 11's
# work in it) and, as every split on this branch has been, along a RESPONSIBILITY seam rather than
# at a line count. The parent's subject is:
#
#     A REPORT MUST IDENTIFY THE BYTES AND THE CONFIGURATION IT DESCRIBES
#
# — the corpus, its components, the schema, the Flight ticket, the output dir, the manifest config.
# This file's subject is one question that parent cannot answer:
#
#     WHICH PROGRAMS WERE MEASURED, AND WERE THEY THIS REVISION'S — AND STILL THE SAME ONES
#     WHEN THE LAST REP RAN?
#
# That is a distinct subject because every check in the parent is satisfiable by a session whose
# corpus, schema, request and configuration are impeccably identified and whose two arms were
# DIFFERENT BUILDS. This rig's entire output is a RATIO BETWEEN TWO BINARIES: an old
# `cqlite-flight` against a current `ws0-scan-bench` is a number about two moments in the repo's
# history, and it is indistinguishable in the report from a number about one.
#
# The three findings that share the subject, in the order they were found:
#
#   * ROUND 10, M2 — `--no-build` accepted ANY executable already under `target/release`, and the
#     session manifest recorded neither the source revision nor any binary digest. A STALE artifact
#     could be measured and reported as a result for the current checkout with nothing in the
#     report able to say otherwise.
#   * ROUND 11, F1 — M2's own mtime-vs-HEAD staleness check was applied in BOTH build modes, on the
#     premise that `cargo build` touches every artifact. Cargo does NOT rewrite an artifact it
#     considers current, so a script- or docs-only commit followed by a SUCCESSFUL build left every
#     mtime earlier than HEAD and the driver REFUSED — breaking the NORMAL MEASUREMENT COMMAND.
#     The ACCEPT direction of that fix lives in
#     `test_ws0_primary_path_admits_a_legitimate_run.sh`, whose whole subject is accept directions;
#     the REJECT half (it must still refuse under `reused`) is here, beside the check it scopes.
#   * ROUND 11, F2 — the digests were taken ONCE before a session that legitimately runs for many
#     minutes, while every rep executed straight out of `target/release`. A concurrent `cargo build`
#     REPLACES those files mid-session, so the later reps measured DIFFERENT PROGRAMS and the report
#     attributed all of them to the digests taken before the first rep. The executables are now
#     COPIED into the session's own `measured-bin/` and the copies are what the reps run.
#
# Per #3249 (a hardcoded `_PERF_STATE="ok"` survived 118/118 tests) the bar is OBSERVED TO FIRE, so
# every case carries the MEASURED pre-fix behaviour and the firing cases are paired with a control
# establishing the refusal came from the check under test and not from a broken fixture.
#
# Hermetic: synthetic session dirs, synthetic perf CSVs, a synthetic few-KB `Data.db` whose real
# sha256 is computed with hashlib, and a throwaway `git init` repo under `$TMPDIR`. No cargo, perf,
# sudo, taskset, corpus, network or root.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"

fails=0
# `checks` counts what actually RAN (incremented here, not derived from the file), so the
# minimum-check-count floor at the end can see a block that silently never executed.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$REPORT" ] || { echo "FAIL - missing $REPORT"; exit 1; }
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
# `run_report`, `expect_reject`, and (via `lib-ws0-fixtures.sh`) `ws0_pin_session_corpus` and
# `ws0_pin_binaries`. Sourced rather than re-implemented for the reason the parent records: a
# duplicated builder is the wrong thing to keep copies of.
# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"

GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'

make_corpus "$TMP/corpus"

# The DRIVER-ORDER assert at the end of the M2 section compares the line at which the driver records
# the binaries against the line at which it writes the session pin. Both are read here: the pin line
# used to come from the parent suite's ticket section, and carrying the dependency rather than the
# definition is how a split leaves a case comparing against an EMPTY variable — which under
# `set -u`-without-`-e` would fail for the wrong reason.
pin_write_line=$(grep -n 'write_session_corpus_pin' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
# ==========================================================================
# ROUND 10, M2 — WHICH BINARIES PRODUCED THE RATIO
# ==========================================================================
# `--no-build` accepted ANY executable already under target/release, and the manifest recorded
# neither the source revision nor any binary digest — so a STALE artifact could be measured and
# reported as a result for the current checkout. This rig's whole output is a RATIO BETWEEN TWO
# BINARIES, so which binaries were measured is provenance, not bookkeeping.
#
# THE FIXTURE'S SHAPE MUST BE THE SHIPPED SHAPE. `ws0_pin_binaries` hand-writes the record (the real
# writer observes target/release and runs git, which is host-dependent), so the contract is asserted
# instead — its key set must EQUAL `PROVENANCE_FIELDS`, or the fixture would keep passing after the
# writer's shape changed and every case below would be testing a stale contract.
d="$TMP/bin-shape"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_binaries "$d"
if python3 - "$REPO_ROOT/scripts/perf" "$d/binary-provenance.json" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import MEASURED_BINARIES, PROVENANCE_FIELDS
rec = json.load(open(sys.argv[2]))
assert set(rec) == set(PROVENANCE_FIELDS), (sorted(rec), sorted(PROVENANCE_FIELDS))
assert set(rec["binaries"]) == set(MEASURED_BINARIES), sorted(rec["binaries"])
PY
then
  pass "round10 M2: the fixture's provenance record carries EXACTLY the shipped PROVENANCE_FIELDS over EXACTLY MEASURED_BINARIES (so the cases below test the real contract, not a stale copy)"
else
  fail "round10 M2: the fixture record's shape has drifted from the shipped writer's"
fi
# ...and the SHELL library's binary list must equal the PYTHON module's. Two languages, one fact —
# a program added to one and not the other would be built-and-checked but unrecorded, or recorded
# but never verified to exist.
if python3 - "$REPO_ROOT" <<'PY'
import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(root / "scripts" / "perf"))
from ws0_binaries import MEASURED_BINARIES
lib = (root / "scripts" / "perf" / "lib-binaries.sh").read_text()
m = re.search(r"WS0_MEASURED_BINARIES=\(([^)]*)\)", lib)
assert m, "lib-binaries.sh must declare WS0_MEASURED_BINARIES"
shell = tuple(m.group(1).split())
assert shell == tuple(MEASURED_BINARIES), (shell, MEASURED_BINARIES)
PY
then
  pass "round10 M2: lib-binaries.sh's WS0_MEASURED_BINARIES EQUALS ws0_binaries.MEASURED_BINARIES (one fact in two languages, so a new program cannot be built-but-unrecorded)"
else
  fail "round10 M2: the shell and python binary lists disagree"
fi

# THE ACCEPT DIRECTION: an untampered session reports, results.json carries the provenance, and the
# summary SAYS which binaries the ratio is between.
d="$TMP/bin-ok"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" "$REPO_ROOT/scripts/perf" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[2])
from ws0_binaries import MEASURED_BINARIES
bp = json.load(open(sys.argv[1]))["binary_provenance"]
assert len(bp["source_revision"]) == 40, bp
assert bp["build_mode"] in ("built", "reused"), bp
assert set(bp["binaries"]) == set(MEASURED_BINARIES), sorted(bp["binaries"])
for name, spec in bp["binaries"].items():
    assert len(spec["sha256"]) == 64, (name, spec)
    assert spec["bytes"] > 0, (name, spec)
# The READER's own note must state the revision AND the tree state — the two facts that decide
# whether the revision fully describes what was built. Asserted on `note` rather than on the
# record's `provenance` string, because `note` is composed by the SHIPPED reader while `provenance`
# is written by whoever wrote the record (here, the fixture).
assert bp["source_revision_short"] in bp["note"], bp["note"]
assert "clean working tree" in bp["note"], bp["note"]
assert "build mode" in bp["note"], bp["note"]
PY
then
  pass "OBSERVED (round10 M2): an untampered session REPORTS, and results.json carries the revision + every measured binary's digest, with the record's own provenance limit"
else
  fail "round10 M2: the accept direction must record the binaries (rc=$rc, out: $out)"
fi
if grep -q 'binary pin' <<<"$out"; then
  pass "OBSERVED (round10 M2): the summary carries a 'binary pin' line — WHICH PROGRAMS the ratio is between, beside WHICH BYTES and WHICH QUERY"
else
  fail "round10 M2: the summary must report which binaries were measured (out: $out)"
fi
# ...and the SHIPPED WRITER must record the record's own provenance LIMIT — that report time does
# NOT re-derive these digests. Asserted against the writer's source, because a fixture-written
# record legitimately carries its own `provenance` string and so cannot establish this.
if python3 - "$REPO_ROOT/scripts/perf/ws0_binaries.py" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
body = src[src.index("def record_binary_provenance"):src.index("def describe_record")]
assert "NOT" in body and "re-derive" in body, "the writer must state that report time does not re-derive"
assert "written BY THE DRIVER" in body, "the record must state WHO observed it"
PY
then
  pass "OBSERVED (round10 M2): the SHIPPED writer records the record's own provenance LIMIT — who observed it, and that report time does NOT re-derive the digests (the F6 posture: one document, one story about its artifacts)"
else
  fail "round10 M2: the writer must record its provenance limit"
fi

# THE REFUSAL: no record at all — exactly what every pre-fix session dir carried.
d="$TMP/bin-absent"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
rm -f "$d/binary-provenance.json"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'carries no binary-provenance.json' <<<"$out"; then
  pass "OBSERVED (round10 M2): a session with NO binary provenance — what EVERY pre-fix session wrote — is REFUSED (an unrecorded session may have measured artifacts of a different revision)"
else
  fail "round10 M2: an absent provenance record must be refused (rc=$rc, out: $out)"
fi
# NON-VACUITY for that case: the PRE-FIX state is precisely this session dir, and it used to REPORT.
# Asserted by establishing that everything ELSE the reporter requires is present and consistent —
# so the refusal came from the new check and from nothing else.
d2="$TMP/bin-absent-control"; make_session "$d2" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d2" "$TMP/corpus"
out2=$(run_report "$d2" "$TMP/corpus"); rc2=$?
if [ "$rc2" -eq 0 ]; then
  pass "NON-VACUITY (round10 M2): the SAME session WITH the record reports cleanly — so the refusal above is the binary-provenance check firing, not a broken fixture"
else
  fail "round10 M2: the control session must report (rc=$rc2, out: $out2)"
fi

# A PARTIAL record is refused too: two of three programs identified leaves the third's identity
# unstated while the report reads as complete.
d="$TMP/bin-partial"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
del j["binaries"]["flight-loadgen"]
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records no digest for flight-loadgen' <<<"$out"; then
  pass "OBSERVED (round10 M2): a PARTIAL record (one measured program unidentified) is REFUSED naming it — a partial record reads as complete"
else
  fail "round10 M2: a partial provenance record must be refused (rc=$rc, out: $out)"
fi
# A TRUNCATED digest is refused: a value that cannot identify a program is not a pin.
d="$TMP/bin-truncated"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
j["binaries"]["cqlite-flight"]["sha256"] = "deadbeef"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "not 64 lowercase hex" <<<"$out"; then
  pass "OBSERVED (round10 M2): a TRUNCATED binary digest is REFUSED — a value that cannot identify a program is not a pin"
else
  fail "round10 M2: a truncated binary digest must be refused (rc=$rc, out: $out)"
fi
# An UNCLASSIFIED build mode is refused: a mode nobody declared is an unchecked claim about how the
# measured binaries came to exist (the F7 posture, applied here).
d="$TMP/bin-mode"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
ws0_pin_binaries "$d" "COPIED-FROM-SOMEWHERE"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "'build_mode' is" <<<"$out"; then
  pass "OBSERVED (round10 M2): an UNCLASSIFIED build_mode is REFUSED — a mode nobody declared is an unchecked claim about how the binaries came to exist"
else
  fail "round10 M2: an unclassified build mode must be refused (rc=$rc, out: $out)"
fi

# THE STALENESS CHECK, exercised directly against the SHIPPED function (it runs at MEASUREMENT time,
# in the driver, so there is no session dir to tamper with). Hermetic: a throwaway git repo under
# $TMP, no cargo, no perf, no host state touched.
if python3 - "$REPO_ROOT/scripts/perf" "$TMP/staleness" <<'PY'
import os, pathlib, subprocess, sys, time
sys.path.insert(0, sys.argv[1])
from ws0_binaries import refuse_binaries_older_than_head
from ws0_validate import Invalid
repo = pathlib.Path(sys.argv[2]); repo.mkdir(parents=True, exist_ok=True)
env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@e",
       "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@e"}
def git(*a):
    subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, env=env)
git("init", "-q")
(repo / "f").write_text("x")
git("add", "f"); git("commit", "-qm", "c")
head = int(subprocess.run(["git", "-C", str(repo), "log", "-1", "--format=%ct"],
                          capture_output=True, text=True, check=True).stdout.strip())
# A REUSED binary written AFTER the commit is accepted, and the note SAYS what was established.
note = refuse_binaries_older_than_head(repo, {"b": {"mtime_epoch": head + 60}}, "reused")
assert "AFTER the HEAD commit" in note, note
# ...and one written BEFORE it is REFUSED. This is the case --no-build makes reachable.
try:
    refuse_binaries_older_than_head(repo, {"b": {"mtime_epoch": head - 3600}}, "reused")
except Invalid as exc:
    assert "STALE BINARIES" in str(exc), str(exc)
    assert "3600s before HEAD" in str(exc), str(exc)
else:
    raise SystemExit("a binary older than HEAD must be REFUSED")
PY
then
  pass "OBSERVED (round10 M2): under \`reused\` the staleness check ACCEPTS a binary written after HEAD (with a note stating what was established) and REFUSES one written before it, naming the gap — the case --no-build makes reachable"
else
  fail "round10 M2: the mtime-vs-HEAD staleness check must discriminate under reused"
fi

# ROUND 11's F1 (that check's SCOPING to `reused`) is verified in test_ws0_primary_path_admits_a_legitimate_run.sh.
# ...and the driver must RECORD the binaries BEFORE it pins the session, so a stale-binary refusal
# costs seconds rather than a full measurement run.
binrec_line=$(grep -n 'record_measured_binaries' "$REPO_ROOT/scripts/perf/ws0-baseline.sh" | head -1 | cut -d: -f1)
if [ -n "$binrec_line" ] && [ -n "$pin_write_line" ] && [ "$binrec_line" -lt "$pin_write_line" ]; then
  pass "round10 M2: the driver records the measured binaries at line $binrec_line, BEFORE the session pin at line $pin_write_line (a stale-binary refusal costs seconds, not a full run)"
else
  fail "round10 M2: the binary record must precede the pin (binaries=$binrec_line pin=$pin_write_line)"
fi

# ==========================================================================
# ROUND 11, F2 — THE MEASURED EXECUTABLES ARE THE SESSION'S OWN COPIES
# ==========================================================================
# The digests were taken ONCE before a session that legitimately runs for many minutes, while every
# rep executed straight out of `target/release`. A `cargo build` in another terminal — a peer agent's
# gate, an editor save-hook, the operator's own next branch — REPLACES those files mid-session, so
# the later reps measure DIFFERENT PROGRAMS and the report attributes every one of them to the
# digests taken before the first rep. Confidently wrong, which is worse than absent.
#
# The fix COPIES the three executables into the session's own `measured-bin/` and runs those, so the
# race is removed rather than narrowed. The ACCEPT half — the freeze observed working, by executing
# the race — lives in test_ws0_primary_path_admits_a_legitimate_run.sh; this is the REFUSAL: a record
# whose paths point into `target/release` describes a session that did NOT have the guarantee, and it
# must not be reported as though it did.
d="$TMP/f2-unfrozen"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
# EXACTLY the pre-fix record: correct digests, correct revision, complete — and paths naming the
# shared build directory every rep used to execute from.
for name, spec in j["binaries"].items():
    spec["path"] = f"/repo/target/release/{name}"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "not inside this session" <<<"$out"; then
  pass "OBSERVED (round11 F2): a record whose paths point into target/release is REFUSED — its digests describe the bytes present before the first rep, not the bytes each rep ran, and a concurrent rebuild could replace them mid-session"
else
  fail "round11 F2: an unfrozen (target/release) binary path must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# NON-VACUITY: that record is otherwise COMPLETE and self-consistent — which is why the substitution
# was invisible. The same session with the frozen paths reports cleanly.
d="$TMP/f2-frozen-control"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "NON-VACUITY (round11 F2): the SAME session with paths inside measured-bin/ reports CLEANLY — so the refusal above is the frozen-path check firing and the pre-fix record differed in nothing else"
else
  fail "round11 F2: the frozen control session must report (rc=$rc, out: $out)"
fi
# A path that merely CONTAINS the directory name is not enough: the check is on the parent directory,
# so a checkout that happened to live under a `measured-bin` directory cannot satisfy it.
d="$TMP/f2-lookalike"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
for name, spec in j["binaries"].items():
    spec["path"] = f"/home/measured-bin/checkout/target/release/{name}"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "not inside this session" <<<"$out"; then
  pass "OBSERVED (round11 F2): a path merely CONTAINING 'measured-bin' higher up is still REFUSED — the check is the PARENT DIRECTORY, not a substring, so a checkout living under such a directory cannot satisfy it"
else
  fail "round11 F2: a lookalike path must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the DRIVER must repoint `$BIN` at the frozen copies, or the record names paths the reps did
# not run — the report would then state a guarantee the run did not have. Structural, because the
# behaviour needs a real build: `lib-measure.sh` reads `$BIN` for every taskset invocation, and
# `record_measured_binaries` is where the copies come into existence.
if grep -q '^  BIN="$frozen"' "$REPO_ROOT/scripts/perf/lib-binaries.sh" \
   && awk '/^record_measured_binaries\(\)/,/^}/' "$REPO_ROOT/scripts/perf/lib-binaries.sh" \
        | grep -q 'measured_bin_dir'; then
  pass "round11 F2 wired: record_measured_binaries repoints \$BIN at the frozen directory, resolved from ws0_binaries.measured_bin_dir (not a duplicated literal, so the shell and the python cannot disagree about where the copies are)"
else
  fail "round11 F2: the driver must repoint \$BIN at the session's frozen copies"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# A block that silently never executes lowers the count and registers NO failure, while the gate
# reads only the exit code. Derived from the real count and set just below it — a floor far behind
# its count stops being able to see a skipped block, which is the very thing it exists to catch
# (#3326 item 3).
MIN_CHECKS=15
echo
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would otherwise lower the count with no"
  echo "       failure registered, and the gate reads only the exit code (#3272)."
  exit 1
fi
if [ "$fails" -eq 0 ]; then
  echo "ws0 binary provenance: all $checks checks passed"
  exit 0
fi
echo "ws0 binary provenance: $fails of $checks check(s) FAILED"
exit 1

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

GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"__TAG__","endpoint":"__ENDPOINT__","requests_ok":1,"requests_error":0,"error_codes":{},"requests_unavailable":0,"rows_total":1000,"bytes_total":'"$WS0_PREFLIGHT_BYTES_PER_SCAN"',"rows_per_s":250.0,"duration_s":4.0}'

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
if [ "$rc" -ne 0 ] && grep -q "the only path a frozen copy of" <<<"$out"; then
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
# A path that merely CONTAINS the directory name is not enough: the recorded path must EQUAL the one
# the reader reconstructs, so a checkout that happened to live under a `measured-bin` directory cannot
# satisfy it. (Pre-F3 this was the PARENT-DIRECTORY-NAME test, which this case already defeated on a
# substring; F3's own cases below defeat the parent-name test itself.)
d="$TMP/f2-lookalike"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
for name, spec in j["binaries"].items():
    spec["path"] = f"/home/measured-bin/checkout/target/release/{name}"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "the only path a frozen copy of" <<<"$out"; then
  pass "OBSERVED (round11 F2): a path merely CONTAINING 'measured-bin' higher up is still REFUSED — the recorded path must EQUAL the reconstructed one, so a checkout living under such a directory cannot satisfy it"
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
# ROUND 12, F1 — A REUSED BINARY'S SOURCE REVISION IS *UNKNOWN*, NOT HEAD
# ==========================================================================
# THE FINDING, and it is round 10's M2 and round 11's F1 both stopping one step short. M2 recorded
# `source_revision = git rev-parse HEAD` in BOTH build modes; round 11 correctly scoped the
# mtime-vs-HEAD STALENESS check to `reused` and left the ATTRIBUTION untouched. So a `--no-build`
# session — binaries accepted off the disk, possibly built on another branch, in another worktree, or
# from a tree since changed — was RECORDED AND REPORTED as belonging to the current checkout's HEAD.
#
# A newer mtime establishes that the binary was WRITTEN after that commit. It establishes NOTHING
# about which revision produced it. So the recorded sha was a value nobody observed: the
# FABRICATED-VALUE class this issue's AC3 exists to remove, in its most dangerous form, because a
# plausible sha is indistinguishable in the report from an established one — the same shape as a
# counter defaulting to 0, one field over.
#
# THE FIX IS AN HONEST UNKNOWN (option 2, not a build-provenance sidecar): under `reused`,
# `source_revision` is the `REVISION_UNKNOWN` sentinel and `source_revision_observed` is false, in
# the manifest AND in the report. `git rev-parse HEAD` is not discarded — it moves to
# `checkout_revision_at_measurement`, a differently-named field claiming only what it can support.

# --- NON-VACUITY, MEASURED: what the PRE-FIX writer recorded ------------------
# The pre-fix rule is reconstructed VERBATIM — `revision = rev-parse HEAD` regardless of mode — and
# run against a throwaway repo, and asserted to yield the checkout's own sha for a REUSED build. Then
# the SHIPPED writer is run over the SAME repo and the same mode and must record the sentinel. Both
# halves run through the real `record_binary_provenance`, over real files, so this is a measured flip
# on identical input rather than a new function's first output.
#
# Hermetic: a throwaway `git init` repo under $TMP with three fake "binaries" (chmod +x stubs — the
# writer only stats, hashes and copies them, it never executes anything), no cargo and no perf.
if python3 - "$REPO_ROOT/scripts/perf" "$TMP/f1" <<'PY'
import os, pathlib, subprocess, sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import (MEASURED_BINARIES, REVISION_UNKNOWN, record_binary_provenance,
                          verify_binary_provenance)
root = pathlib.Path(sys.argv[2]); root.mkdir(parents=True, exist_ok=True)
repo, bindir = root / "repo", root / "repo" / "target" / "release"
bindir.mkdir(parents=True, exist_ok=True)
env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@e",
       "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@e"}
def git(*a):
    subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, env=env)
git("init", "-q")
(repo / "f").write_text("x")
git("add", "f"); git("commit", "-qm", "c")
head = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                      capture_output=True, text=True, check=True).stdout.strip()
# The binaries this rig measures, as executable stubs written AFTER the commit (so the staleness
# check — which applies under `reused` — accepts them and the case reaches its own subject).
for name in MEASURED_BINARIES:
    p = bindir / name
    p.write_bytes(b"\x7fELF-stub-" + name.encode())
    p.chmod(0o755)

# THE PRE-FIX RULE, verbatim: HEAD, whatever the mode.
prefix_revision = head
assert len(prefix_revision) == 40

# THE SHIPPED WRITER, in `reused` mode.
reused_dir = root / "session-reused"; reused_dir.mkdir()
rec = record_binary_provenance(reused_dir, bindir, repo, "reused")
assert rec["source_revision"] == REVISION_UNKNOWN, rec["source_revision"]
assert rec["source_revision_observed"] is False, rec
# ...and the checkout revision IS kept, under its own name — the fix records less, not nothing.
assert rec["checkout_revision_at_measurement"] == head, rec
# The MEASURED FLIP: identical repo, identical binaries, identical mode.
assert prefix_revision != rec["source_revision"], (prefix_revision, rec["source_revision"])
# The record's own prose must SAY the revision is unknown, so a reader of the manifest alone cannot
# miss it. (Asserted on `provenance`, which the SHIPPED writer composes.)
assert "UNKNOWN" in rec["provenance"], rec["provenance"]

# ...and in `built` mode the revision IS observed, because `cargo build` ran in-process on this
# checkout. Without this half the fix would be a guard that always fires — the mirror-image broken
# instrument, and the "documented path made unrunnable" defect this issue has hit three times.
built_dir = root / "session-built"; built_dir.mkdir()
rb = record_binary_provenance(built_dir, bindir, repo, "built")
assert rb["source_revision"] == head, rb["source_revision"]
assert rb["source_revision_observed"] is True, rb
assert rb["checkout_revision_at_measurement"] == head, rb

# BOTH records must READ BACK through the shipped reader: a writer/reader disagreement would surface
# at report time as a refusal blaming the session dir for a driver defect.
for d, want_observed in ((reused_dir, False), (built_dir, True)):
    back = verify_binary_provenance(d)
    assert back["source_revision_observed"] is want_observed, (d, back)
    if not want_observed:
        assert back["source_revision"] == REVISION_UNKNOWN, back
        assert "UNKNOWN source revision" in back["note"], back["note"]
    else:
        assert back["source_revision"] == head, back
print("OK")
PY
then
  pass "NON-VACUITY (round12 F1): the PRE-FIX rule records this checkout's HEAD for a REUSED build; the SHIPPED writer records the UNKNOWN sentinel over the same repo, same binaries, same mode — and still records HEAD under 'built', so the fix is not a guard that always fires"
else
  fail "round12 F1: a reused build's source revision must be UNKNOWN and a built one's must be HEAD"
fi

# THE READER REFUSES A RECORD THAT CONFLATES THE TWO STATES, in BOTH directions. Keyed on the
# AFFIRMATIVE boolean rather than on the sentinel's spelling: a reader accepting "either a sha or the
# sentinel" would let a record claim `observed=true` beside the sentinel (a verdict presented as
# provenance) or `observed=false` beside a real sha (the fabricated value, relabelled).
d="$TMP/f1-sha-unobserved"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
# EXACTLY the pre-fix record for a reused build: a real-looking sha, mode reused, and the honest
# `observed=false` beside it — i.e. the record admits it did not observe the revision it names.
j["build_mode"] = "reused"
j["source_revision"] = "9" * 40
j["source_revision_short"] = "9" * 12
j["source_revision_observed"] = False
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "not the 'UNKNOWN-reused" <<<"$out"; then
  pass "OBSERVED (round12 F1): a record naming a REVISION it did not observe is REFUSED — a sha beside 'observed: false' is the fabricated value with an honest flag bolted on, and the report must never print a revision nobody established"
else
  fail "round12 F1: an unobserved sha must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the converse: the sentinel presented AS observed provenance.
d="$TMP/f1-unknown-observed"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" "$REPO_ROOT/scripts/perf" <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[2])
from ws0_binaries import REVISION_UNKNOWN
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
j["source_revision"] = REVISION_UNKNOWN
j["source_revision_short"] = REVISION_UNKNOWN[:12]
j["source_revision_observed"] = True
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "not a 40-character sha, while" <<<"$out"; then
  pass "OBSERVED (round12 F1): the UNKNOWN sentinel claimed AS an observed revision is REFUSED too — the two fields are checked against each other, so neither can be flipped alone"
else
  fail "round12 F1: the sentinel must not pass as an observed revision (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# A NON-BOOLEAN `source_revision_observed` is refused: it decides which of the two rules applies, so
# an unclassified value leaves the record's central claim unchecked (the F7 posture, applied here).
d="$TMP/f1-observed-nonbool"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
j["source_revision_observed"] = "yes"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "not a boolean" <<<"$out"; then
  pass "OBSERVED (round12 F1): a NON-BOOLEAN 'source_revision_observed' is REFUSED — it decides whether the revision field is provenance or a verdict, so a truthy string would leave that unclassified"
else
  fail "round12 F1: a non-boolean observed flag must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# The CHECKOUT revision is required in BOTH modes, always as a real sha: where the checkout stood is
# observable whatever the build mode, so an absent or sentinel value there means the record dropped a
# fact it had — which would leave a reused session with no revision information at all.
d="$TMP/f1-no-checkout-rev"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
j["checkout_revision_at_measurement"] = "unknown"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "never legitimately unknown" <<<"$out"; then
  pass "OBSERVED (round12 F1): an absent/sentinel 'checkout_revision_at_measurement' is REFUSED — that fact is observable in BOTH build modes, so the fix records LESS, never nothing"
else
  fail "round12 F1: a missing checkout revision must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# THE REPORT MUST NOT PRINT A REVISION IT DID NOT OBSERVE — the half of the finding that a manifest
# check alone cannot cover, since the summary line is what a human actually reads.
d="$TMP/f1-report-reused"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
ws0_pin_binaries "$d" reused
out=$(run_report "$d" "$TMP/corpus"); rc=$?
# The fixture's checkout sha is 40 x "1", so its 12-char prefix is what a pre-fix report would have
# printed as the source revision. It must appear ONLY as the checkout's, beside an UNKNOWN verdict.
if [ "$rc" -eq 0 ] && grep -q 'binary pin.*UNKNOWN source revision' <<<"$out" \
  && grep -q 'checkout was at 111111111111' <<<"$out" \
  && python3 - "$d/results.json" "$REPO_ROOT/scripts/perf" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[2])
from ws0_binaries import REVISION_UNKNOWN
bp = json.load(open(sys.argv[1]))["binary_provenance"]
assert bp["source_revision"] == REVISION_UNKNOWN, bp
assert bp["source_revision_observed"] is False, bp
assert bp["checkout_revision_at_measurement"] == "1" * 40, bp
assert "UNKNOWN source revision" in bp["note"], bp["note"]
PY
then
  pass "OBSERVED (round12 F1): a REUSED session REPORTS (the documented --no-build loop still works) and its summary says 'UNKNOWN source revision', naming the checkout sha only under its own weaker description — results.json agrees"
else
  fail "round12 F1: a reused session must report with an UNKNOWN revision (rc=$rc, out: $(grep 'binary pin' <<<"$out"))"
fi
# NON-VACUITY for that case: the SAME session in `built` mode prints the sha. So the UNKNOWN above is
# the mode-scoped path firing, and the report has not simply stopped printing revisions.
d="$TMP/f1-report-built"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'binary pin.*at 111111111111' <<<"$out" \
  && ! grep -q 'UNKNOWN source revision' <<<"$out"; then
  pass "NON-VACUITY (round12 F1): the SAME session in BUILT mode prints the revision — so the UNKNOWN above is the reused-mode path firing, not the report having stopped reporting revisions"
else
  fail "round12 F1: a built session must still print its source revision (rc=$rc, out: $(grep 'binary pin' <<<"$out"))"
fi

# ...and the DRIVER's own one-line summary must say it too: the operator reading the driver's output
# during a run must not take a reused binary for one built at HEAD. Asserted through the SHIPPED
# `describe_record` over both modes, because that is the function the driver prints.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import REVISION_UNKNOWN, describe_record
base = {"binaries": {"a": {}, "b": {}, "c": {}}, "source_dirty": False, "source_dirty_paths": 0,
        "checkout_revision_at_measurement": "a" * 40}
reused = describe_record({**base, "build_mode": "reused", "source_revision": REVISION_UNKNOWN,
                          "source_revision_short": REVISION_UNKNOWN[:12],
                          "source_revision_observed": False})
built = describe_record({**base, "build_mode": "built", "source_revision": "b" * 40,
                         "source_revision_short": "b" * 12, "source_revision_observed": True})
assert "UNKNOWN source revision" in reused, reused
# The sentinel's own text must NOT be printed as though it were a revision label.
assert "at UNKNOWN-reuse" not in reused, reused
assert "checkout was at aaaaaaaaaaaa" in reused, reused
assert "at bbbbbbbbbbbb" in built, built
assert "UNKNOWN" not in built, built
PY
then
  pass "OBSERVED (round12 F1): the DRIVER's own 'binary pin' line says UNKNOWN under --no-build and names the sha under a build — the operator watching a run cannot mistake a reused binary for one built at HEAD"
else
  fail "round12 F1: describe_record must distinguish the two modes"
fi

# ==========================================================================
# ROUND 14, F3 — "SESSION-OWNED" WAS DECIDED BY A DIRECTORY *NAME*
# ==========================================================================
# F2 froze the executables and the check on it was NOMINAL: a path counted as this session's own
# frozen copy iff its PARENT DIRECTORY WAS NAMED `measured-bin`. A name is not an identity, so three
# records satisfied it while describing something else. Each case below EXECUTES the substitution
# against a real session dir and a real reporter, and each is paired with the control establishing
# the refusal is the new check firing and not a broken fixture.
#
# The recorded path is now RELATIVE and the reader RECONSTRUCTS it from the session dir + the
# binary's key, so all three collapse into one comparison the pre-fix check could not express.

# --- NON-VACUITY, MEASURED: the PRE-FIX CHECK, RECONSTRUCTED VERBATIM, ACCEPTS ALL THREE --------
# Per #3249 the bar is OBSERVED TO FIRE, and a refusal firing is only half of that: the other half is
# that the PRE-FIX code did NOT refuse. So round 12 F2's reader loop is reconstructed VERBATIM here
# and run over the SAME three substituted records the cases below feed the shipped reader. The
# pre-fix loop must ACCEPT all three; the shipped reader must REFUSE all three. That is a measured
# flip on identical input, not a claim about a new function's first output.
if python3 - "$REPO_ROOT/scripts/perf" "$TMP/f3-nonvac" <<'PY'
import hashlib, json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import MEASURED_BINARIES, verify_binary_provenance
from ws0_binary_spec import frozen_relpath
from ws0_validate import Invalid, _SHA256_RE

def prefix_check(binaries):
    """ROUND 12 F2's READER LOOP, VERBATIM — the three field checks and the parent-name test."""
    for name in MEASURED_BINARIES:
        spec = binaries.get(name)
        if not isinstance(spec, dict):
            raise Invalid(f"records no digest for {name}")
        digest = spec.get("sha256")
        if not isinstance(digest, str) or not _SHA256_RE.match(digest):
            raise Invalid(f"{name} sha256")
        if not isinstance(spec.get("bytes"), int) or spec["bytes"] <= 0:
            raise Invalid(f"{name} bytes")
        recorded = spec.get("path")
        if not isinstance(recorded, str) or not recorded:
            raise Invalid(f"{name} path")
        # THE WHOLE OF THE PRE-FIX SESSION-OWNERSHIP CHECK: a directory NAME.
        if pathlib.PurePath(recorded).parent.name != "measured-bin":
            raise Invalid(f"{name}: not inside this session's own measured-bin/")
    return "ACCEPTED"

base = pathlib.Path(sys.argv[2]); base.mkdir(parents=True, exist_ok=True)

def build(session, mutate):
    session.mkdir(parents=True, exist_ok=True)
    rec = {
        "source_revision": "1" * 40, "source_revision_short": "1" * 12,
        "source_revision_observed": True, "checkout_revision_at_measurement": "1" * 40,
        "source_dirty": False, "source_dirty_paths": 0, "build_mode": "built",
        "binaries": {
            n: {"path": frozen_relpath(n), "source_path": f"/repo/target/release/{n}",
                "sha256": hashlib.sha256(n.encode()).hexdigest(),
                "bytes": 1024 + i, "mtime_epoch": 2000000000}
            for i, n in enumerate(MEASURED_BINARIES)},
        "provenance": "a test fixture record",
    }
    mutate(session, rec)
    (session / "binary-provenance.json").write_text(json.dumps(rec, indent=1) + "\n")
    return rec

def another_session(session, rec):
    peer = base / "peer-session" / "measured-bin"; peer.mkdir(parents=True, exist_ok=True)
    for name, spec in rec["binaries"].items():
        (peer / name).write_bytes(b"\x7fELF-peer-" + name.encode())
        (peer / name).chmod(0o755)
        spec["path"] = str(peer / name)

def wrong_executable(session, rec):
    names = list(MEASURED_BINARIES); rotated = names[1:] + names[:1]
    for key, other in zip(names, rotated):
        rec["binaries"][key]["path"] = f"measured-bin/{other}"

def copy_is_not_the_bytes(session, rec):
    frozen = session / "measured-bin"; frozen.mkdir(parents=True, exist_ok=True)
    for name, spec in rec["binaries"].items():
        body = b"\x7fELF" + name.encode() + b"\x00" * 64
        spec["bytes"] = len(body); spec["sha256"] = hashlib.sha256(body).hexdigest()
        (frozen / name).write_bytes(body); (frozen / name).chmod(0o755)
    target = frozen / "cqlite-flight"; body = target.read_bytes()
    # SAME LENGTH, different bytes — a DIFFERENT BUILD, which a size check alone cannot see.
    target.write_bytes(b"\x7fELF" + b"\xff" * (len(body) - 4))

cases = (("another-session", another_session), ("wrong-executable", wrong_executable),
         ("copy-is-not-the-bytes", copy_is_not_the_bytes))
for label, mutate in cases:
    session = base / label
    rec = build(session, mutate)
    # THE PRE-FIX LOOP MUST ACCEPT. If it refused, this case would not be measuring the finding.
    assert prefix_check(rec["binaries"]) == "ACCEPTED", (
        f"{label}: the reconstructed PRE-FIX check refused, so this record does not reproduce"
        " the finding and the shipped refusal below would prove nothing"
    )
    # ...and the SHIPPED reader must REFUSE the very same record.
    try:
        verify_binary_provenance(session)
    except Invalid:
        pass
    else:
        raise AssertionError(f"{label}: the shipped reader ACCEPTED a record the fix must refuse")
PY
then
  pass "NON-VACUITY MEASURED (round14 F3): round 12 F2's reader loop, RECONSTRUCTED VERBATIM, ACCEPTS all three substitutions (another session's frozen copy, the wrong executable, a copy whose bytes are not the recorded ones) and the SHIPPED reader REFUSES all three — a measured flip on identical records, so the refusals below are the finding and not a new function's first output"
else
  fail "round14 F3: the pre-fix check must accept what the fix refuses (else the cases prove nothing)"
fi

# --- (a) ANOTHER SESSION'S FROZEN COPY ---------------------------------------------------------
# The most dangerous of the three, because it is a REAL frozen copy — of another session, another
# revision, possibly another branch — sitting on a path whose parent directory is genuinely named
# `measured-bin`. Under the pre-fix check it was indistinguishable from this session's own.
d="$TMP/f3-other-session"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
mkdir -p "$TMP/f3-peer-session/measured-bin"
python3 - "$d/binary-provenance.json" "$TMP/f3-peer-session" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
peer = pathlib.Path(sys.argv[2]) / "measured-bin"
for name, spec in j["binaries"].items():
    # A REAL file on disk, in a REAL measured-bin directory, whose digest MATCHES the record — so
    # the substitution survives every other check in this suite. It is simply another session's.
    copy = peer / name
    copy.write_bytes(b"\x7fELF-peer-" + name.encode())
    copy.chmod(0o755)
    spec["path"] = str(copy)
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "the only path a frozen copy of" <<<"$out" \
   && grep -q "ANOTHER SESSION" <<<"$out"; then
  pass "OBSERVED (round14 F3): a path naming ANOTHER SESSION's measured-bin/ copy is REFUSED — the file EXISTS, is executable, and its parent directory IS named measured-bin, so the pre-fix parent-name check could not tell it from this session's own"
else
  fail "round14 F3: another session's frozen copy must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi

# --- (b) THE WRONG EXECUTABLE, under the right session and the right directory ------------------
# Right session dir, right `measured-bin/`, keys ROTATED — so `cqlite-flight`'s slot holds
# `flight-loadgen`'s path. Every pre-fix check passes: three specs, three valid digests, three
# parents named `measured-bin`. The report then names one program's identity as another's.
d="$TMP/f3-rotated"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" "$REPO_ROOT/scripts/perf" <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[2])
from ws0_binaries import MEASURED_BINARIES
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
names = list(MEASURED_BINARIES)
rotated = names[1:] + names[:1]
for key, other in zip(names, rotated):
    # ONLY the path is rotated — the digest, the size and the source_path stay this key's own, so
    # nothing but the path-vs-key comparison can see it.
    j["binaries"][key]["path"] = f"measured-bin/{other}"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "the only path a frozen copy of" <<<"$out" \
   && grep -q "WRONG EXECUTABLE" <<<"$out"; then
  pass "OBSERVED (round14 F3): a spec whose path names a DIFFERENT PROGRAM in the SAME measured-bin/ is REFUSED — right session, right directory, right parent name, wrong executable, so the report would have named one binary's digest as another's"
else
  fail "round14 F3: a rotated path/key pairing must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# The same substitution one step earlier in the SAME operation: the freeze copied the wrong FILE, so
# `source_path`'s basename disagrees with its key. That field was never read at all before F3.
d="$TMP/f3-wrong-source"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
j["binaries"]["cqlite-flight"]["source_path"] = "/nonexistent/target/release/flight-loadgen"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "copied a DIFFERENT PROGRAM" <<<"$out"; then
  pass "OBSERVED (round14 F3): a 'source_path' whose basename names a DIFFERENT PROGRAM than its key is REFUSED — the freeze copied the wrong file, and that field was UNREAD by every check before F3"
else
  fail "round14 F3: a mismatched source_path must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi

# --- (c) A COPY ON DISK THAT IS NOT THE RECORDED BYTES -----------------------------------------
# The record carried a digest, the frozen copy sat beside it, and NOTHING compared the two. Both
# directions are exercised: a size mismatch (truncated/replaced) and a same-size digest mismatch
# (a DIFFERENT BUILD, which is the one a size check alone cannot see).
for f3_kind in truncated different-build; do
  d="$TMP/f3-copy-$f3_kind"; make_session "$d" "$GOOD_FLIGHT"
  ws0_pin_session_corpus "$d" "$TMP/corpus"
  python3 - "$d" "$f3_kind" "$REPO_ROOT/scripts/perf" <<'PY'
import hashlib, json, pathlib, sys
sys.path.insert(0, sys.argv[3])
from ws0_binaries import provenance_path
session, kind = pathlib.Path(sys.argv[1]), sys.argv[2]
rec = json.loads(provenance_path(session).read_text())
frozen = session / "measured-bin"; frozen.mkdir(parents=True, exist_ok=True)
for name, spec in rec["binaries"].items():
    # Write a copy that MATCHES the record, then break exactly one property of ONE of them, so the
    # case's subject is the comparison and not a wholesale mismatch.
    body = b"\x7fELF" + name.encode() + b"\x00" * 64
    spec["bytes"] = len(body); spec["sha256"] = hashlib.sha256(body).hexdigest()
    (frozen / name).write_bytes(body)
    (frozen / name).chmod(0o755)
target = frozen / "cqlite-flight"
if kind == "truncated":
    target.write_bytes(target.read_bytes()[:-8])
else:
    # SAME LENGTH, different bytes — a different build of the same program. A size check alone is
    # blind to this, which is why both are compared.
    body = target.read_bytes()
    target.write_bytes(b"\x7fELF" + b"\xff" * (len(body) - 4))
provenance_path(session).write_text(json.dumps(rec, indent=1) + "\n")
PY
  out=$(run_report "$d" "$TMP/corpus"); rc=$?
  if [ "$f3_kind" = truncated ]; then f3_want='bytes, but the record says'
  else f3_want='DIFFERENT BUILD'; fi
  if [ "$rc" -ne 0 ] && grep -q "$f3_want" <<<"$out"; then
    pass "OBSERVED (round14 F3): a frozen copy on disk that is NOT the recorded bytes ($f3_kind) is REFUSED — the record and the file beside it described different programs and NOTHING compared them before F3"
  else
    fail "round14 F3: a $f3_kind frozen copy must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
  fi
done
# NON-VACUITY for (c): the SAME session with copies that MATCH reports cleanly, AND states the
# re-derivation AFFIRMATIVELY as a count. A `0/3` must be visible as a fact about what was checked —
# never derive a pass from an absence.
d="$TMP/f3-copy-control"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d" "$REPO_ROOT/scripts/perf" <<'PY'
import hashlib, json, pathlib, sys
sys.path.insert(0, sys.argv[2])
from ws0_binaries import provenance_path
session = pathlib.Path(sys.argv[1])
rec = json.loads(provenance_path(session).read_text())
frozen = session / "measured-bin"; frozen.mkdir(parents=True, exist_ok=True)
for name, spec in rec["binaries"].items():
    body = b"\x7fELF" + name.encode() + b"\x00" * 64
    spec["bytes"] = len(body); spec["sha256"] = hashlib.sha256(body).hexdigest()
    (frozen / name).write_bytes(body); (frozen / name).chmod(0o755)
provenance_path(session).write_text(json.dumps(rec, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" "$REPO_ROOT/scripts/perf" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[2])
from ws0_binaries import MEASURED_BINARIES
bp = json.load(open(sys.argv[1]))["binary_provenance"]
v = bp["binary_spec_verification"]
assert v["frozen_copies_verified"] == len(MEASURED_BINARIES), v
assert v["frozen_copies_expected"] == len(MEASURED_BINARIES), v
assert "RE-DERIVED" in v["note"], v["note"]
# The record must state WHICH FIELDS were checked, so the census's coverage is on the record rather
# than only in the module.
assert set(v["fields_checked"]) == {"path", "sha256", "bytes", "source_path", "mtime_epoch"}, v
for name, per in v["per_binary"].items():
    assert per["frozen_copy_verified"] is True, (name, per)
    assert per["executable"] == "executable", (name, per)
PY
then
  pass "NON-VACUITY (round14 F3): the SAME session whose frozen copies MATCH reports cleanly and records frozen_copies_verified=3/3 with every spec field named — so the three refusals above are the comparison firing, and its coverage is AFFIRMATIVE rather than inferred from an absence"
else
  fail "round14 F3: the matching-copies control must report and state its coverage (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and the ABSENT-copies case (a results dir archived without its release binaries) must REPORT —
# refusing it would make reviewing a shipped results set impossible — while saying `0/3` OUT LOUD.
d="$TMP/f3-copies-absent"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && python3 - "$d/results.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))["binary_provenance"]["binary_spec_verification"]
assert v["frozen_copies_verified"] == 0, v
assert "0 of 3" in v["note"] and "NOT re-derived" in v["note"], v["note"]
for name, per in v["per_binary"].items():
    assert per["frozen_copy_present"] is False, (name, per)
    assert per["executable"] == "absent from this session dir", (name, per)
PY
then
  pass "OBSERVED (round14 F3): a session dir REVIEWED WITHOUT its frozen copies still REPORTS (an archived results set stays reviewable) and states '0 of 3 ... NOT re-derived' — the check's silence is a recorded fact, not an unexamined assumption"
else
  fail "round14 F3: an absent-copies session must report and state 0/3 (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# --- THE CENSUS IS CLOSED IN BOTH DIRECTIONS ---------------------------------------------------
# `path` sat in a five-field object of which the reader considered three; `source_path` and
# `mtime_epoch` were never mentioned at all — the `requests_unavailable` class, nested. So the fix
# is a census, and its whole value is that a field the WRITER adds cannot be one the reader silently
# drops. Both directions are executed against the reporter, not asserted about the table.
d="$TMP/f3-unclassified"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
# Exactly what a future writer would add: a plausible, useful field nobody classified.
j["binaries"]["cqlite-flight"]["link_mode"] = "static"
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "does not classify" <<<"$out" && grep -q "link_mode" <<<"$out"; then
  pass "OBSERVED (round14 F3): a spec field the census does NOT classify is REFUSED naming it — a field the writer adds cannot become a second unread 'mtime_epoch' under a reader that claims coverage"
else
  fail "round14 F3: an unclassified spec field must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
d="$TMP/f3-dropped-field"; make_session "$d" "$GOOD_FLIGHT"; ws0_pin_session_corpus "$d" "$TMP/corpus"
python3 - "$d/binary-provenance.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); j = json.loads(p.read_text())
# `mtime_epoch` is the field NO check read before F3, so a record omitting it is precisely the
# pre-fix blind spot: it passed, and the staleness verdict rested on a value nobody could read.
del j["binaries"]["cqlite-flight"]["mtime_epoch"]
p.write_text(json.dumps(j, indent=1) + "\n")
PY
out=$(run_report "$d" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "missing mtime_epoch" <<<"$out"; then
  pass "OBSERVED (round14 F3): a spec MISSING a classified field ('mtime_epoch' — the one NO check read before F3) is REFUSED, so the census cannot claim a coverage the record does not support"
else
  fail "round14 F3: a spec missing a classified field must be refused (rc=$rc, out: $(head -2 <<<"$out"))"
fi
# ...and the CLOSURE ITSELF is refused at IMPORT in both directions — round 12 F2's own shape (the
# thing was done, the check on it was nominal) made unrepresentable rather than merely avoided.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import importlib, sys
sys.path.insert(0, sys.argv[1])
import ws0_binary_spec as m
from ws0_validate import Invalid
# A disposition NO CHECKER implements must be refused at import — the half-wired guard.
saved_disp, saved_checkers = m.SPEC_DISPOSITIONS, dict(m._SPEC_CHECKERS)
try:
    src = open(m.__file__).read()
    ns = {}
    exec(compile(src.replace('SPEC_DISPOSITIONS = ("session-derived"',
                             'SPEC_DISPOSITIONS = ("unchecked-invention", "session-derived"'),
                 m.__file__, "exec"), ns)
except Invalid as exc:
    assert "NO CHECKER implements it" in str(exc), exc
else:
    raise AssertionError("a disposition with no checker must be refused at import")
# ...and a CHECKER claiming a disposition no field carries is dead code, also refused.
try:
    src = open(m.__file__).read()
    exec(compile(src.replace('_SPEC_CHECKERS = {\n    "session-derived"',
                             '_SPEC_CHECKERS = {\n    "never-classified": _check_shape,\n'
                             '    "session-derived"'), m.__file__, "exec"), {})
except Invalid as exc:
    assert "dead code" in str(exc), exc
else:
    raise AssertionError("a checker claiming an unknown disposition must be refused at import")
# ...and there is deliberately NO non-verifying disposition: every field of a binary spec is
# verified, for F1's reason for DELETING `required-present` rather than leaving it empty.
assert set(m.SPEC_DISPOSITIONS) == set(m._SPEC_CHECKERS), (m.SPEC_DISPOSITIONS, m._SPEC_CHECKERS)
assert {d for d, _ in m.BINARY_SPEC_DISPOSITION.values()} == set(m.SPEC_DISPOSITIONS)
assert saved_disp is m.SPEC_DISPOSITIONS and saved_checkers.keys() == m._SPEC_CHECKERS.keys()
PY
then
  pass "OBSERVED (round14 F3): the census's closure is refused AT IMPORT in BOTH directions — a disposition no checker implements, and a checker claiming a disposition no field carries — so 'classified as verified while nothing compares it' (round 12 F2's own shape) is unrepresentable rather than merely absent"
else
  fail "round14 F3: the spec census must assert its closure in both directions at import"
fi
# ...and the WRITER must record the RELATIVE path from the shared spelling, not an absolute one:
# an absolute path can only be checked by SPELLING, which is what the pre-fix reader did.
if python3 - "$REPO_ROOT/scripts/perf" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import MEASURED_BINARIES
from ws0_binary_spec import frozen_relpath
# `freeze_measured_binaries` moved to `ws0_binary_snapshot.py` in round 21's F5 campsite split (the
# snapshot's ONE-BUILD verification went in beside it). The assert follows the code rather than
# being deleted with the file it used to read — a structural check pointed at a moved function
# passes by reading nothing.
src = pathlib.Path(sys.argv[1], "ws0_binary_snapshot.py").read_text()
body = src[src.index("def freeze_measured_binaries"):]
assert '"path": frozen_relpath(name)' in body, "the writer must record the shared relative spelling"
assert '"path": str(dst)' not in body, "the writer must not record an absolute path"
for name in MEASURED_BINARIES:
    rel = frozen_relpath(name)
    assert not pathlib.PurePath(rel).is_absolute(), rel
    assert pathlib.PurePath(rel).parts[0] == "measured-bin", rel
PY
then
  pass "round14 F3 wired: the WRITER records the RELATIVE frozen path from the SAME \`frozen_relpath\` the reader reconstructs — one spelling, so a reader that reconstructs cannot disagree with a writer that records"
else
  fail "round14 F3: the writer must record the shared relative frozen path"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# A block that silently never executes lowers the count and registers NO failure, while the gate
# reads only the exit code. Derived from the real count and set just below it — a floor far behind
# its count stops being able to see a skipped block, which is the very thing it exists to catch
# (#3326 item 3).
MIN_CHECKS=35
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

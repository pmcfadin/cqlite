#!/usr/bin/env bash
# Self-test for WHETHER THE MEASURED CORPUS IS THE CANONICAL ONE (issue #3272 round 13, F3).
#
# Split from `scripts/tests/test_ws0_provenance_guards.sh` under the campsite rule (F3's cases took
# it back to 1616 lines against the ~1500 test target, immediately after the output-dir split had
# brought it to 1355), BY SUBJECT. The parent's subject is:
#
#     A REPORT MUST IDENTIFY THE BYTES AND THE CONFIGURATION IT DESCRIBES.
#
# This file's is the question one level up, which that one cannot ask:
#
#     ARE THOSE THE BYTES A WS0 BASELINE IS *DEFINED* AS?
#
# The distinction is the whole of F3, and it is a seam rather than a cut: every parent check is a
# SELF-CONSISTENCY check about whatever corpus was supplied. The pin matches the report-time
# identity; the components match the pin; the schema matches its recorded digest; every rep's rows
# are an exact multiple of the pinned row count. All of that is equally true of a corpus generated
# with smoke-test row counts or a different seed — so such a corpus passed the driver AND the
# reporter as a WS0 BASELINE, with nothing in the printed report to distinguish it from the real
# thing. Identifying the bytes correctly and identifying the RIGHT bytes are two properties, and the
# parent only ever had the first.
#
# The canonical shape lives in RUST (`tools/ws0-corpus-gen/src/measurement_corpus.rs`) and until
# round 13 NOTHING under `scripts/` referred to it — measured:
# `grep -rl 'measurement_corpus\|DATA_DB_SHA256\|SCHEMA_SHA256' scripts/` returned ZERO hits. So the
# subject's implementation is a CROSS-LANGUAGE BRIDGE (`scripts/perf/ws0_canonical_corpus.py`, a
# parse of the Rust source — no `cargo build` is available to a gate component or a hermetic
# self-test, and a committed generated copy would be a second copy of every value), which is a third
# reason this is its own file: the parent's subject involves no other language.
#
# A SMOKE CORPUS STILL RUNS, under `--non-baseline`, LABELLED in the manifest and the report. Rounds
# 9, 10 and 11 each shipped a fix that made a documented operator command unable to succeed, and a
# command that always fails teaches an operator to stop running it — losing the whole check.
# Forbidding the smoke corpus would be the fourth instance. Its ACCEPT direction is asserted here
# and in `test_ws0_primary_path_admits_a_legitimate_run.sh`.
#
# Every case carries the MEASURED pre-fix behaviour, because per #3249 a guard never observed
# firing is not evidence.
#
# Hermetic: synthetic session dirs and corpora under $TMPDIR, the SHIPPED oracle and writers called
# directly, and driver invocations ONLY through `ws0_driver_run` (`--validate-args-only` + recording
# shims). No cargo, perf, sudo, taskset, real corpus, network or root — in particular the Rust pin is
# READ, never compiled.
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
# python3 is a HARD REQUIREMENT of this rig — ws0-baseline.sh refuses to run without it — so its
# absence is a FAILURE, not a skip. A `exit 0` here would record the gate component as SUCCESS with
# none of the checks below having run, which is the vacuous green this whole issue refuses.
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

# The shared fixture builders — `make_corpus`, `make_session`, `run_report`, and (through
# `lib-ws0-fixtures.sh`) `ws0_make_corpus`/`ws0_pin_session_corpus`. Never re-implemented here: a
# duplicated builder keeps passing after the shipped shape changes, which is the drift `make_round`
# demonstrated in round 3.
# shellcheck source=scripts/tests/lib-ws0-report-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-report-fixtures.sh"
# ...and the HERMETIC driver harness, for the two cases that run the real driver: `ws0_driver_run`
# prepends `--validate-args-only` and the recording shims, so they execute nothing.
# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
ws0_hermetic_init "$TMP"

GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"r","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"rows_per_s":250.0,"duration_s":4.0}'

make_corpus "$TMP/corpus"

# ==========================================================================
# ROUND 13, F3 — THE PIN NEVER CHECKED THE CORPUS WAS THE CANONICAL ONE
# ==========================================================================
# `write_session_corpus_pin` snapshots the identity of the corpus it was HANDED and compares it
# against NOTHING. So every check in this file — the pin matching the report-time identity, the
# components matching the pin, the schema matching its recorded digest, every rep's rows being an
# exact multiple of the pinned row count — is SELF-CONSISTENCY ABOUT WHATEVER CORPUS WAS SUPPLIED.
# All of it is equally true of a corpus generated with smoke-test row counts or a different seed,
# so such a corpus passed the driver AND the reporter as a WS0 BASELINE with nothing in the printed
# report to distinguish it. The identity being verified was never compared to the identity the
# baseline is DEFINED as.
#
# The canonical shape lives in Rust (`tools/ws0-corpus-gen/src/measurement_corpus.rs`) and until
# round 13 NOTHING under `scripts/` referred to it — a cross-language bridge, not a wiring job. See
# `scripts/perf/ws0_canonical_corpus.py` for why the bridge is a source PARSE (no `cargo build` is
# available to a gate component or a hermetic self-test, and a committed generated copy would be a
# second copy of every value).
#
# A SMOKE CORPUS STILL RUNS, under `--non-baseline`, LABELLED. Rounds 9/10/11 each shipped a fix
# that made a documented operator command unable to succeed; forbidding the smoke corpus would be
# the fourth. The ACCEPT direction is asserted below and in
# `test_ws0_primary_path_admits_a_legitimate_run.sh`.
f3c_perf="$REPO_ROOT/scripts/perf"

# --- NON-VACUITY FIRST: the PRE-FIX pin accepted a smoke corpus as a baseline -----------
# This is the finding, driven rather than argued. The pre-fix writer took (session, corpus,
# identity, config) and recorded whatever the identity said; it is still callable that way, so the
# case runs the SHIPPED writer with NO canonical record and shows the resulting pin is
# indistinguishable — for a 1000-row corpus — from one describing the 4,000,000-row canonical one.
f3c_dir="$TMP/f3c"; mkdir -p "$f3c_dir/session"
make_corpus "$f3c_dir/corpus"
f3c_out=$(WS0_F3_PERF="$f3c_perf" WS0_F3_SESSION="$f3c_dir/session" WS0_F3_CORPUS="$f3c_dir/corpus" python3 - <<'PY' 2>&1
import json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F3_PERF"])
from ws0_canonical_corpus import canonical_pins
from ws0_session import write_session_corpus_pin
from ws0_ticket_input import write_ticket_template
from ws0_validate import load_corpus_identity
session, corpus = pathlib.Path(os.environ["WS0_F3_SESSION"]), pathlib.Path(os.environ["WS0_F3_CORPUS"])
# The pin records the ticket digest (#3272 round 10 M1), so the template must exist first — the
# same ordering the driver has. Written by the SHIPPED writer, so this case exercises the real pin.
write_ticket_template(session, corpus / "ws0-events.cql")
ident = load_corpus_identity(corpus)
cfg = {"reps": "1", "temps": "warm", "arms": "bypass", "scan_passes": "1",
       "server_cpus": "2,10", "client_cpus": "4,12", "step_duration": "45s/1s",
       "baseline_mode": "baseline"}
# THE PRE-FIX CALL: no canonical record, so nothing compares the corpus to the pin.
pin = write_session_corpus_pin(session, corpus, ident, cfg)
canon = canonical_pins(pathlib.Path(os.environ["WS0_F3_PERF"]).parent.parent)
print("PREFIX_PINNED_ROWS", pin["rows"])
print("CANONICAL_ROWS", canon["ROWS"])
print("PREFIX_HAS_CANONICAL_BLOCK", bool(pin.get("canonical_corpus")))
PY
)
if grep -q 'PREFIX_PINNED_ROWS 1000' <<<"$f3c_out" \
   && grep -q 'CANONICAL_ROWS 4000000' <<<"$f3c_out" \
   && grep -q 'PREFIX_HAS_CANONICAL_BLOCK False' <<<"$f3c_out"; then
  pass "OBSERVED (round13 F3) NON-VACUITY: the PRE-FIX pin recorded a 1000-row corpus as the session's corpus with NO canonical comparison at all, while the canonical corpus is 4,000,000 rows — the pin snapshotted what it was handed and compared it against nothing"
else
  fail "round13 F3: the pre-fix pin must be shown to accept a smoke corpus uncompared, else the finding's premise is unproven (out: $f3c_out)"
fi

# --- THE BRIDGE READS THE RUST PIN, and every constant is MANDATORY --------------------
# The values must come FROM the Rust source, not a hand-copy in Python: a hand-copy is the
# two-place-invariant class this issue has fought throughout. Asserted by reading the constants and
# comparing them against the Rust file's own text, so a Python-side literal could not satisfy it.
f3c_out=$(WS0_F3_PERF="$f3c_perf" WS0_F3_ROOT="$REPO_ROOT" python3 - <<'PY' 2>&1
import os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F3_PERF"])
from ws0_canonical_corpus import RUST_PIN_REL, canonical_pins
root = pathlib.Path(os.environ["WS0_F3_ROOT"])
pins = canonical_pins(root)
src = (root / RUST_PIN_REL).read_text()
# Each parsed value must appear in the RUST file (the source of truth), so this cannot be
# satisfied by a Python literal that merely happens to be right.
for name, val in pins.items():
    needle = f"{val:_}" if isinstance(val, int) else str(val)
    if needle not in src:
        print("NOT_FROM_RUST", name, needle); break
else:
    print("ALL_FROM_RUST", len(pins))
PY
)
if grep -q 'ALL_FROM_RUST 9' <<<"$f3c_out"; then
  pass "OBSERVED (round13 F3): all 9 canonical values are read FROM tools/ws0-corpus-gen/src/measurement_corpus.rs — each parsed value is present in that file's text, so a hand-copied Python literal could not satisfy this (the two-place-invariant class this issue keeps finding)"
else
  fail "round13 F3: the canonical values must come from the Rust pin (out: $f3c_out)"
fi
# ...and a constant the parse CANNOT FIND is FATAL, never a skipped comparison. That is the one
# failure mode a source parse has that a copy does not, so it is the one driven here: a renamed
# constant must red the check rather than silently drop a field.
f3c_fake="$TMP/f3c-fakepin"; mkdir -p "$f3c_fake/tools/ws0-corpus-gen/src"
sed 's/^pub const ROWS:/pub const ROWS_RENAMED:/' \
  "$REPO_ROOT/tools/ws0-corpus-gen/src/measurement_corpus.rs" \
  > "$f3c_fake/tools/ws0-corpus-gen/src/measurement_corpus.rs"
f3c_out=$(WS0_F3_PERF="$f3c_perf" WS0_F3_ROOT="$f3c_fake" python3 - <<'PY' 2>&1
import os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F3_PERF"])
from ws0_canonical_corpus import canonical_pins
from ws0_validate import Invalid
try:
    canonical_pins(pathlib.Path(os.environ["WS0_F3_ROOT"]))
    print("ACCEPTED_A_MISSING_CONSTANT")
except Invalid as exc:
    print("REFUSED", exc)
PY
)
if grep -q '^REFUSED' <<<"$f3c_out" && grep -q 'pub const ROWS' <<<"$f3c_out"; then
  pass "OBSERVED (round13 F3): a RENAMED canonical constant is FATAL and names it — the parse never falls back to a default and has no way to skip a field it cannot read (a bridge degrading to 'assume canonical' would be the vacuous pass)"
else
  fail "round13 F3: an unparseable constant must be fatal (out: $f3c_out)"
fi

# --- THE CANONICAL CORPUS IS ACCEPTED AS A BASELINE -----------------------------------
# The ACCEPT direction, over the COMMITTED identity of the real 2026-08-03 canonical corpus, so a
# guard that refused everything cannot satisfy this file. The schema digest is read from the
# ws0-events.cql ON DISK, because that artifact predates the recorded field — without which the
# REAL canonical corpus would be unable to be measured as a baseline, which is round 9's F1 exactly.
f3c_canon="$TMP/f3c-canon"; mkdir -p "$f3c_canon"
python3 - "$f3c_perf" "$REPO_ROOT" "$f3c_canon" <<'PY' > /dev/null 2>&1
import pathlib, re, sys
root, out = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
m = re.search(r'pub const DDL: &str = r?#?"(.*?)"#?;',
              (root / "tools/ws0-corpus-gen/src/schema.rs").read_text(), re.S)
(out / "ws0-events.cql").write_text(m.group(1) + "\n")
PY
f3c_out=$(WS0_F3_PERF="$f3c_perf" WS0_F3_ROOT="$REPO_ROOT" WS0_F3_CORPUS="$f3c_canon" python3 - <<'PY' 2>&1
import json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F3_PERF"])
from ws0_canonical_corpus import MODE_BASELINE, require_canonical_or_declared
from ws0_validate import Invalid
root = pathlib.Path(os.environ["WS0_F3_ROOT"])
ident = json.loads((root / "docs/reports/ws0-3096-artifacts/corpus-identity.json").read_text())
try:
    rec = require_canonical_or_declared(root, ident, MODE_BASELINE,
                                        pathlib.Path(os.environ["WS0_F3_CORPUS"]))
    print("IS_BASELINE", rec["is_baseline"], "| DIVERGENCES", len(rec["divergences"]))
    print("LABEL", rec["label"])
except Invalid as exc:
    print("REFUSED_THE_CANONICAL_CORPUS", exc)
PY
)
if grep -q 'IS_BASELINE True | DIVERGENCES 0' <<<"$f3c_out"; then
  pass "OBSERVED (round13 F3) ACCEPT: the COMMITTED identity of the real canonical corpus is ACCEPTED as a WS0 baseline, 0 divergences (its absent schema_sha256 is read from ws0-events.cql on DISK — a stronger oracle, and without it the REAL corpus could not be a baseline, which is round 9's F1)"
else
  fail "round13 F3: the canonical corpus must be accepted as a baseline (out: $f3c_out)"
fi

# --- A NONCANONICAL CORPUS IS REFUSED AS A BASELINE ------------------------------------
f3c_out=$(WS0_F3_PERF="$f3c_perf" WS0_F3_ROOT="$REPO_ROOT" WS0_F3_CORPUS="$f3c_dir/corpus" python3 - <<'PY' 2>&1
import os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F3_PERF"])
from ws0_canonical_corpus import MODE_BASELINE, MODE_NON_BASELINE, NON_BASELINE_LABEL, require_canonical_or_declared
from ws0_validate import Invalid, load_corpus_identity
root, corpus = pathlib.Path(os.environ["WS0_F3_ROOT"]), pathlib.Path(os.environ["WS0_F3_CORPUS"])
ident = load_corpus_identity(corpus)
try:
    require_canonical_or_declared(root, ident, MODE_BASELINE, corpus)
    print("ACCEPTED_A_SMOKE_CORPUS_AS_A_BASELINE")
except Invalid as exc:
    print("REFUSED_AS_BASELINE")
    print("NAMES_ROWS", "rows: 1000" in str(exc))
    print("NAMES_THE_FLAG", "--non-baseline" in str(exc))
    print("NAMES_REGEN", "ws0-corpus-gen" in str(exc))
# ...and the SAME corpus under the explicit non-baseline mode.
rec = require_canonical_or_declared(root, ident, MODE_NON_BASELINE, corpus)
print("NONBASELINE_RUNS", rec["is_baseline"] is False and rec["mode"] == MODE_NON_BASELINE)
print("LABELLED", NON_BASELINE_LABEL in rec["label"])
PY
)
if grep -q 'REFUSED_AS_BASELINE' <<<"$f3c_out" && grep -q 'NAMES_ROWS True' <<<"$f3c_out" \
   && grep -q 'NAMES_THE_FLAG True' <<<"$f3c_out" && grep -q 'NAMES_REGEN True' <<<"$f3c_out"; then
  pass "OBSERVED (round13 F3) REFUSE: a 1000-row corpus is REFUSED as a baseline, naming the divergent rows, the --non-baseline flag that runs it anyway, AND the regeneration command (a refusal that does not say how to proceed is how rounds 9/10 broke three documented commands)"
else
  fail "round13 F3: a noncanonical corpus must be refused as a baseline with a way forward (out: $f3c_out)"
fi
if grep -q 'NONBASELINE_RUNS True' <<<"$f3c_out" && grep -q 'LABELLED True' <<<"$f3c_out"; then
  pass "OBSERVED (round13 F3) SMOKE PATH: the SAME 1000-row corpus RUNS under the explicit --non-baseline mode and its record carries the 'NOT A WS0 BASELINE' label (a smoke corpus must still run; forbidding it would be the fourth broken command on this issue)"
else
  fail "round13 F3: a noncanonical corpus must run under --non-baseline, labelled (out: $f3c_out)"
fi

# --- THE DRIVER TAKES --non-baseline, AND SAYS WHICH CLAIM IT MAKES -------------------
# Through `ws0_driver_run` (`--validate-args-only` + recording shims), so nothing is executed.
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent --non-baseline); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'baseline-mode=non-baseline' <<<"$out" \
   && ws0_driver_ran_hermetically; then
  pass "OBSERVED (round13 F3): the driver accepts --non-baseline and its argument stamp says WHICH claim the run makes, so the mode is observable without executing anything"
else
  fail "round13 F3: --non-baseline must be accepted and stamped (rc=$rc, out: $(head -3 <<<"$out"))"
fi
out=$(ws0_driver_run "$DRIVER" --corpus /nonexistent); rc=$?
if [ "$rc" -eq 0 ] && grep -q 'baseline-mode=baseline' <<<"$out"; then
  pass "OBSERVED (round13 F3): the DEFAULT is baseline mode — a run that says nothing claims to be a baseline and is held to the canonical corpus, rather than defaulting to the permissive branch"
else
  fail "round13 F3: the default mode must be baseline (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# --- THE REPORT LABELS A NON-BASELINE RUN, IN WORDS ------------------------------------
# The end-to-end property F3 is actually about: a reader of the report must be unable to mistake a
# smoke run for a baseline. Asserted on the PRINTED output, over a synthetic (noncanonical) corpus
# — which is what every session in this file is.
f3c_sess="$TMP/f3c-report"
make_session "$f3c_sess" "$GOOD_FLIGHT"
out=$(run_report "$f3c_sess" "$TMP/corpus"); rc=$?
if [ "$rc" -eq 0 ] && grep -q '\*\*\* NOT A BASELINE \*\*\*' <<<"$out" \
   && grep -q 'NOT A WS0 BASELINE' <<<"$out"; then
  pass "OBSERVED (round13 F3): the report of a NONCANONICAL corpus is LABELLED in the TITLE LINE and on its own 'baseline :' line (pre-fix the title read 'WS0 SAME-SESSION BASELINE' over ANY corpus, so a smoke run was published under the word BASELINE in the report's first line)"
else
  fail "round13 F3: a non-baseline report must be labelled in words (rc=$rc, out: $(grep -E 'WS0 SAME-SESSION|baseline' <<<"$out" | head -3))"
fi
# ...and the divergences are NAMED, so a reader can see WHY it is not a baseline rather than
# having to take the label on trust.
if grep -q 'DIVERGES from tools/ws0-corpus-gen/src/measurement_corpus.rs' <<<"$out"; then
  pass "OBSERVED (round13 F3): the report NAMES the divergent fields and the pin they diverge from, so the label cites an observation rather than asserting a verdict"
else
  fail "round13 F3: the report must name the divergences (out: $(grep -i baseline <<<"$out" | head -3))"
fi
# NON-VACUITY for the label: the pre-fix title is ABSENT from a non-baseline report. Without this
# the case above would pass on a report that printed BOTH titles.
if ! grep -q '==== WS0 SAME-SESSION BASELINE' <<<"$out"; then
  pass "OBSERVED (round13 F3) NON-VACUITY: the pre-fix unconditional 'WS0 SAME-SESSION BASELINE' title is ABSENT from a non-baseline report — the label REPLACES the baseline claim rather than sitting beside it"
else
  fail "round13 F3: a non-baseline report must NOT also carry the baseline title (out: $(grep 'WS0 SAME-SESSION' <<<"$out"))"
fi

# --- A HAND-EDITED VERDICT IS REFUSED, NOT PRINTED ------------------------------------
# The record must SUPPORT its own verdict: `is_baseline`/`is_canonical` are RE-DERIVED from `mode`
# and `divergences` at report time. Otherwise the label is one JSON edit from saying "baseline"
# about a smoke corpus, which is the finding with an extra step.
python3 - "$f3c_sess" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
pin = json.loads(p.read_text())
pin["canonical_corpus"]["is_baseline"] = True
pin["canonical_corpus"]["is_canonical"] = True
p.write_text(json.dumps(pin, indent=1) + "\n")
PY
out=$(run_report "$f3c_sess" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'CONTRADICTS ITSELF' <<<"$out"; then
  pass "OBSERVED (round13 F3): a pin hand-edited to is_baseline=true beside recorded DIVERGENCES is REFUSED as self-contradictory — the verdict is re-derived, so a recorded boolean nobody re-derives cannot publish a smoke corpus as a baseline"
else
  fail "round13 F3: a contradictory canonical record must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ...and an ABSENT record is refused too, so a session dir predating the check cannot be reported
# as a baseline by omission — the fail-OPEN direction, which is the one that matters.
python3 - "$f3c_sess" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
pin = json.loads(p.read_text())
pin.pop("canonical_corpus", None)
p.write_text(json.dumps(pin, indent=1) + "\n")
PY
out=$(run_report "$f3c_sess" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records no `canonical_corpus`' <<<"$out"; then
  pass "OBSERVED (round13 F3): a pin carrying NO canonical_corpus block is REFUSED — an absent record cannot be read as 'canonical', which is the fail-open direction that would have reinstated the finding silently"
else
  fail "round13 F3: an absent canonical record must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi
# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# Without `-e` a block that silently never executes — an early `return` in a helper, a `$(...)`
# whose command vanished, a `for` over an empty list — LOWERS the check count and registers NO
# failure. The gate reads only the exit code, so a suite that ran 3 of its checks and passed them
# exits 0 and reports SUCCESS. That is the suite-level `0/0` shape this whole issue is about, one
# level up from the checks themselves.
#
# The floor is DERIVED from the OBSERVED count — 13 at the split, measured by running the suite —
# set just below it so adding a case does not red the suite, and far above zero. No case here skips
# conditionally, so the observed count is the same on every host.
MIN_CHECKS=11
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 canonical-corpus guards: all $checks checks passed"
  exit 0
fi
echo "ws0 canonical-corpus guards: $fails of $checks check(s) FAILED"
exit 1

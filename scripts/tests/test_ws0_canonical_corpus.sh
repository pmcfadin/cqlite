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

GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"__TAG__","endpoint":"__ENDPOINT__","requests_ok":1,"requests_error":0,"requests_unavailable":0,"rows_total":1000,"bytes_total":'"$WS0_PREFLIGHT_BYTES_PER_SCAN"',"rows_per_s":250.0,"duration_s":4.0}'

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
# ROUND 14, F4 — THE CLASSIFICATION TRUSTED A *TOTAL*, AND READ 8 OF 15 FIELDS
# ==========================================================================
# F3 built the comparison above over nine SCALARS. One of them was `total_component_bytes`, and the
# module's own docs recorded the resulting gap as a known weakness: the component NAME SET was
# "covered in aggregate" by that total. A sum is not a set — so a corpus with an altered Index.db
# and a compensating Statistics.db, or same-length-different-bytes in either, kept the canonical
# total and was classified CANONICAL.
#
# The mitigation those docs cited was CIRCULAR: `verify_pinned_components` compares every component
# name/size/digest against THE CORPUS'S OWN identity and the bytes beside it. Both sides come from
# the corpus under test, so it establishes self-consistency and cannot establish that the map is the
# CANONICAL one.
#
# AND THE CITED FIELD WAS ONE OF SEVEN. The first case below MEASURES the classifier's field set
# against the canonical artifact's real key set, because that is the property that made this a class
# rather than one field: `seed` and `table` — the INPUT ANCHORS, whose absence is the same defect the
# RUST side of this same pin already fixed under review B2 — and `compression_info_present`, which IS
# issue #1406's claim boundary, were never compared at all.

# --- THE CENSUS: EVERY FIELD OF THE CANONICAL ARTIFACT HAS A DISPOSITION ---------------
# Measured against the artifact's REAL key set, in that direction: an assert over the fields the
# comparison covers can only certify its own scope, which is how the cited finding sat beside six
# more uncompared fields. The pre-fix count is asserted too, so this case fails if the premise
# stops reproducing rather than quietly becoming a tautology.
f4_out=$(WS0_F4_PERF="$f3c_perf" WS0_F4_ROOT="$REPO_ROOT" python3 - <<'PY' 2>&1
import json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F4_PERF"])
from ws0_canonical_corpus import CANONICAL_ARTIFACT_REL, CANONICAL_CENSUS, NOT_IDENTITY
root = pathlib.Path(os.environ["WS0_F4_ROOT"])
art = json.loads((root / CANONICAL_ARTIFACT_REL).read_text())
keys = set(art)
# THE PRE-FIX FIELD SET, written out verbatim: the nine CANONICAL_FIELDS values F3 shipped.
prefix = {"rows", "partitions", "rows_per_partition", "cells_per_row", "data_db_bytes",
          "total_component_bytes", "bytes_per_row", "data_db_sha256", "schema_sha256"}
print("ARTIFACT_KEYS", len(keys))
print("PREFIX_NEVER_READ", len(keys - prefix), ",".join(sorted(keys - prefix)))
print("UNACCOUNTED_NOW", ",".join(sorted(keys - set(CANONICAL_CENSUS) - set(NOT_IDENTITY))) or "NONE")
print("COMPARED_NOW", len(CANONICAL_CENSUS))
PY
)
if grep -q 'ARTIFACT_KEYS 15' <<<"$f4_out" \
   && grep -q 'PREFIX_NEVER_READ 7 components,compression_info_present,differs_from_prior_corpus,issue,not_a_correctness_oracle,seed,table' <<<"$f4_out" \
   && grep -q 'UNACCOUNTED_NOW NONE' <<<"$f4_out"; then
  pass "OBSERVED (round14 F4) NON-VACUITY, THE CLASS: the PRE-FIX classifier read 8 of the canonical artifact's 15 fields and NEVER consulted 7 — including seed and table (the INPUT ANCHORS, whose absence is the same defect the RUST side of this pin fixed under review B2) and compression_info_present (issue #1406's claim boundary, so a COMPRESSED corpus was classifiable as canonical). Every artifact key now has a disposition, closed against the artifact's OWN key set"
else
  fail "round14 F4: the field census must be measured against the artifact's real key set (out: $f4_out)"
fi
# ...and that closure is FAIL-CLOSED on a field nobody thought of, which is the only direction that
# can catch the next instance. Driven with an artifact carrying an extra key.
f4_fake="$TMP/f4-extrakey"; mkdir -p "$f4_fake/docs/reports/ws0-3096-artifacts" "$f4_fake/tools/ws0-corpus-gen/src"
cp "$REPO_ROOT/tools/ws0-corpus-gen/src/measurement_corpus.rs" "$f4_fake/tools/ws0-corpus-gen/src/"
cp "$REPO_ROOT/tools/ws0-corpus-gen/src/generate.rs" "$REPO_ROOT/tools/ws0-corpus-gen/src/schema.rs" \
   "$f4_fake/tools/ws0-corpus-gen/src/"
python3 - "$REPO_ROOT" "$f4_fake" <<'PY'
import json, pathlib, sys
rel = "docs/reports/ws0-3096-artifacts/corpus-identity.json"
art = json.loads((pathlib.Path(sys.argv[1]) / rel).read_text())
art["compaction_strategy"] = "SizeTieredCompactionStrategy"   # a plausible NEW identity field
(pathlib.Path(sys.argv[2]) / rel).write_text(json.dumps(art, indent=2))
PY
f4_out=$(WS0_F4_PERF="$f3c_perf" WS0_F4_ROOT="$f4_fake" python3 - <<'PY' 2>&1
import json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F4_PERF"])
from ws0_canonical_corpus import CANONICAL_ARTIFACT_REL, classify_corpus
from ws0_validate import Invalid
root = pathlib.Path(os.environ["WS0_F4_ROOT"])
ident = json.loads((root / CANONICAL_ARTIFACT_REL).read_text())
try:
    classify_corpus(root, ident)
    print("ACCEPTED_AN_UNACCOUNTED_FIELD")
except Invalid as exc:
    print("REFUSED", exc)
PY
)
if grep -q '^REFUSED' <<<"$f4_out" && grep -q 'compaction_strategy' <<<"$f4_out"; then
  pass "OBSERVED (round14 F4): a canonical artifact carrying a field NEITHER map accounts for is REFUSED, naming it — the closure reads the ARTIFACT and subtracts the census, so a field the census's author never thought of FAILS rather than being silently outside the comparison (an assert over what the comparison covers certifies its own scope, which is how the cited total sat beside six more uncompared fields)"
else
  fail "round14 F4: an unaccounted artifact field must be refused (out: $f4_out)"
fi

# --- THE CITED FINDING, MEASURED: A PRESERVED TOTAL HID ALTERED COMPONENTS -------------
# The bar per #3249 is a measured FLIP on identical input: the PRE-FIX comparison must ACCEPT the
# substituted corpus and the shipped one must REFUSE it. So F3's nine-scalar comparison is
# reconstructed VERBATIM here and run over the same identities.
#
# Each identity is the REAL canonical one with its component map altered and its
# `total_component_bytes` PRESERVED — which is the whole point: every scalar the pre-fix code
# compared is untouched, so it has nothing to see.
f4_out=$(WS0_F4_PERF="$f3c_perf" WS0_F4_ROOT="$REPO_ROOT" WS0_F4_CORPUS="$f3c_canon" python3 - <<'PY' 2>&1
import copy, json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F4_PERF"])
from ws0_canonical_corpus import (CANONICAL_ARTIFACT_REL, MODE_BASELINE, canonical_pins,
                                  require_canonical_or_declared)
from ws0_validate import Invalid

def prefix_divergences(identity, pins):
    """F3'S NINE-SCALAR COMPARISON, VERBATIM — the shipped code before F4."""
    fields = {"ROWS": ("rows", "int"), "PARTITIONS": ("partitions", "int"),
              "ROWS_PER_PARTITION": ("rows_per_partition", "int"),
              "CELLS_PER_ROW": ("cells_per_row", "int"),
              "DATA_DB_BYTES": ("data_db_bytes", "int"),
              "TOTAL_COMPONENT_BYTES": ("total_component_bytes", "int"),
              "BYTES_PER_ROW": ("bytes_per_row", "float"),
              "DATA_DB_SHA256": ("data_db_sha256", "str"),
              "SCHEMA_SHA256": ("schema_sha256", "str")}
    out = []
    for const, (field, kind) in fields.items():
        want, got = pins[const], identity.get(field)
        if got is None:
            if field == "schema_sha256":
                continue          # the pre-fix disk fallback; irrelevant to these cases
            out.append(field); continue
        if kind == "float":
            same = abs(float(got) - float(want)) <= 1e-6
        elif kind == "int":
            same = int(got) == int(want)
        else:
            same = str(got) == str(want)
        if not same:
            out.append(field)
    return out

root = pathlib.Path(os.environ["WS0_F4_ROOT"])
corpus = pathlib.Path(os.environ["WS0_F4_CORPUS"])
base = json.loads((root / CANONICAL_ARTIFACT_REL).read_text())
pins = canonical_pins(root)
IDX, STATS = "nb-1-big-Index.db", "nb-1-big-Statistics.db"

def compensated(j):
    """Index.db GROWN by N, Statistics.db SHRUNK by N — the aggregate is IDENTICAL."""
    n = 1024
    j["components"][IDX]["bytes"] += n
    j["components"][STATS]["bytes"] -= n

def same_length_bytes(j):
    """Index.db REPLACED with same-length DIFFERENT bytes — no size anywhere moves."""
    sha = j["components"][IDX]["sha256"]
    j["components"][IDX]["sha256"] = ("b" if sha[0] != "b" else "c") + sha[1:]

def dropped_and_grown(j):
    """Statistics.db DROPPED, Index.db grown by its size — the total is again identical."""
    n = j["components"].pop(STATS)["bytes"]
    j["components"][IDX]["bytes"] += n

for name, mutate in (("compensated-resize", compensated),
                     ("same-length-different-bytes", same_length_bytes),
                     ("dropped-and-grown", dropped_and_grown)):
    j = copy.deepcopy(base)
    mutate(j)
    total = sum(c["bytes"] for c in j["components"].values())
    # THE PREMISE, asserted: the aggregate the pre-fix code compared is UNMOVED.
    print(f"{name} TOTAL_PRESERVED", total == j["total_component_bytes"] == pins["TOTAL_COMPONENT_BYTES"])
    print(f"{name} PREFIX", "ACCEPTED_AS_CANONICAL" if not prefix_divergences(j, pins) else "refused")
    try:
        require_canonical_or_declared(root, j, MODE_BASELINE, corpus)
        print(f"{name} SHIPPED ACCEPTED_AS_BASELINE")
    except Invalid as exc:
        names_it = IDX in str(exc) or STATS in str(exc)
        print(f"{name} SHIPPED REFUSED names_component={names_it}")
PY
)
f4_ok=1
for f4_case in compensated-resize same-length-different-bytes dropped-and-grown; do
  grep -q "$f4_case TOTAL_PRESERVED True" <<<"$f4_out" || f4_ok=0
  grep -q "$f4_case PREFIX ACCEPTED_AS_CANONICAL" <<<"$f4_out" || f4_ok=0
  grep -q "$f4_case SHIPPED REFUSED names_component=True" <<<"$f4_out" || f4_ok=0
done
if [ "$f4_ok" -eq 1 ]; then
  pass "OBSERVED (round14 F4) NON-VACUITY, MEASURED FLIP on identical input: three corpora whose Index.db/Statistics.db DIFFER while total_component_bytes is PRESERVED (a compensating resize, same-length-different-bytes, and a dropped component whose bytes were added to another) were each ACCEPTED AS CANONICAL by F3's nine-scalar comparison reconstructed verbatim, and are each REFUSED by the shipped one, NAMING the component. A sum is not a set, and no aggregate comparison can see any of the three"
else
  fail "round14 F4: the pre-fix comparison must ACCEPT a preserved-total component swap that the shipped one REFUSES (out: $f4_out)"
fi

# --- THE INPUT ANCHORS ARE COMPARED AGAINST THE CONSTANTS THAT DETERMINE THEM ----------
# `seed` and `table` were in neither the comparison nor any exemption. A digest pin is only a pin
# together with the inputs that determine it: a corpus generated at another seed, or of another
# table, was classified CANONICAL. Compared against generate.rs/schema.rs rather than a literal
# retyped in Python — asserted by reading the values back out of those files.
f4_out=$(WS0_F4_PERF="$f3c_perf" WS0_F4_ROOT="$REPO_ROOT" WS0_F4_CORPUS="$f3c_canon" python3 - <<'PY' 2>&1
import copy, json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F4_PERF"])
from ws0_canonical_corpus import (CANONICAL_ARTIFACT_REL, MODE_BASELINE, _anchor_pins,
                                  require_canonical_or_declared)
from ws0_validate import Invalid
root = pathlib.Path(os.environ["WS0_F4_ROOT"])
corpus = pathlib.Path(os.environ["WS0_F4_CORPUS"])
base = json.loads((root / CANONICAL_ARTIFACT_REL).read_text())
anchors = _anchor_pins(root)
# FROM THE RUST FILES, not a Python literal: each parsed anchor must appear in the source that
# determines it, so a hand-copied value could not satisfy this.
gen = (root / "tools/ws0-corpus-gen/src/generate.rs").read_text()
sch = (root / "tools/ws0-corpus-gen/src/schema.rs").read_text()
print("SEED_FROM_RUST", f"{anchors['seed']:_}" in gen)
ks, tbl = anchors["table"].split(".")
print("TABLE_FROM_RUST", f'"{ks}"' in sch and f'"{tbl}"' in sch)
for field, bad in (("seed", 99_999_999), ("table", "somewhere.else"),
                   ("compression_info_present", True)):
    j = copy.deepcopy(base); j[field] = bad
    try:
        require_canonical_or_declared(root, j, MODE_BASELINE, corpus)
        print(field, "ACCEPTED")
    except Invalid as exc:
        print(field, "REFUSED", field in str(exc))
PY
)
if grep -q 'SEED_FROM_RUST True' <<<"$f4_out" && grep -q 'TABLE_FROM_RUST True' <<<"$f4_out" \
   && grep -q '^seed REFUSED True' <<<"$f4_out" && grep -q '^table REFUSED True' <<<"$f4_out"; then
  pass "OBSERVED (round14 F4): the INPUT ANCHORS are compared — a corpus recording another seed or another table is REFUSED as a baseline, and both expectations are read FROM generate.rs/schema.rs (each parsed value present in the file that determines it, so a retyped Python literal could not satisfy this). A digest pin is only a pin together with the inputs that determine it; pre-fix both were in neither the comparison nor any exemption"
else
  fail "round14 F4: seed/table must be compared against the Rust constants that determine them (out: $f4_out)"
fi
if grep -q '^compression_info_present REFUSED True' <<<"$f4_out"; then
  pass "OBSERVED (round14 F4): a corpus recording compression_info_present=true is REFUSED as canonical — that field IS issue #1406's claim boundary (the write surface emits UNCOMPRESSED SSTables), and the pre-fix classifier never consulted it, so a COMPRESSED corpus was classifiable as the canonical measurement corpus"
else
  fail "round14 F4: compression_info_present must be compared (out: $f4_out)"
fi

# --- THE CANONICAL MAP IS CORROBORATED BEFORE IT IS USED AS AN EXPECTATION -------------
# The map is read from the committed artifact (the only place it exists — measurement_corpus.rs
# pins quantities, not filenames). So a swapped or edited artifact must not silently BECOME the
# canonical expectation: the sizes must sum to the Rust-pinned TOTAL_COMPONENT_BYTES and the
# *-Data.db component must equal the pinned size AND digest. Same rule canonical_pins applies to
# its own parse.
f4_bad="$TMP/f4-badartifact"; mkdir -p "$f4_bad/docs/reports/ws0-3096-artifacts" "$f4_bad/tools/ws0-corpus-gen/src"
cp "$REPO_ROOT/tools/ws0-corpus-gen/src/measurement_corpus.rs" \
   "$REPO_ROOT/tools/ws0-corpus-gen/src/generate.rs" \
   "$REPO_ROOT/tools/ws0-corpus-gen/src/schema.rs" "$f4_bad/tools/ws0-corpus-gen/src/"
python3 - "$REPO_ROOT" "$f4_bad" <<'PY'
import json, pathlib, sys
rel = "docs/reports/ws0-3096-artifacts/corpus-identity.json"
art = json.loads((pathlib.Path(sys.argv[1]) / rel).read_text())
# An EDITED artifact: Index.db grown, and the recorded total grown to match it. Self-consistent,
# and disagreeing with the Rust pin — which is what must be caught.
art["components"]["nb-1-big-Index.db"]["bytes"] += 4096
art["total_component_bytes"] += 4096
(pathlib.Path(sys.argv[2]) / rel).write_text(json.dumps(art, indent=2))
PY
f4_out=$(WS0_F4_PERF="$f3c_perf" WS0_F4_ROOT="$f4_bad" python3 - <<'PY' 2>&1
import os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F4_PERF"])
from ws0_canonical_corpus import canonical_components, canonical_pins
from ws0_validate import Invalid
root = pathlib.Path(os.environ["WS0_F4_ROOT"])
try:
    canonical_components(root, canonical_pins(root))
    print("ACCEPTED_A_DISAGREEING_ARTIFACT")
except Invalid as exc:
    print("REFUSED", exc)
PY
)
if grep -q '^REFUSED' <<<"$f4_out" && grep -q 'TOTAL_COMPONENT_BYTES' <<<"$f4_out"; then
  pass "OBSERVED (round14 F4): an artifact whose component sizes do not sum to the Rust-pinned TOTAL_COMPONENT_BYTES is REFUSED AS AN ORACLE rather than used as one — the two canonical sources are corroborated against each other before either becomes the expectation, so an edited artifact cannot silently redefine what canonical means (and the total is now a DERIVED consequence of a map that was compared, not a standalone handle)"
else
  fail "round14 F4: a disagreeing canonical artifact must be refused as an oracle (out: $f4_out)"
fi
# ...and an UNREADABLE canonical artifact is FATAL, never a fallback to the aggregate. That is the
# one failure mode reading-from-a-file has that a Rust constant does not, so it is the one driven.
f4_none="$TMP/f4-noartifact"; mkdir -p "$f4_none/tools/ws0-corpus-gen/src"
cp "$REPO_ROOT/tools/ws0-corpus-gen/src/measurement_corpus.rs" \
   "$REPO_ROOT/tools/ws0-corpus-gen/src/generate.rs" \
   "$REPO_ROOT/tools/ws0-corpus-gen/src/schema.rs" "$f4_none/tools/ws0-corpus-gen/src/"
f4_out=$(WS0_F4_PERF="$f3c_perf" WS0_F4_ROOT="$f4_none" WS0_F4_CORPUS="$f3c_canon" python3 - <<'PY' 2>&1
import json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F4_PERF"])
from ws0_canonical_corpus import MODE_BASELINE, require_canonical_or_declared
from ws0_validate import Invalid
root = pathlib.Path(os.environ["WS0_F4_ROOT"])
try:
    require_canonical_or_declared(root, {"rows": 4000000}, MODE_BASELINE,
                                  pathlib.Path(os.environ["WS0_F4_CORPUS"]))
    print("ACCEPTED_WITHOUT_THE_CANONICAL_MAP")
except Invalid as exc:
    print("REFUSED", str(exc).replace("\n", " "))
PY
)
if grep -q '^REFUSED' <<<"$f4_out" && grep -q 'unreadable' <<<"$f4_out"; then
  pass "OBSERVED (round14 F4): an UNREADABLE canonical artifact is FATAL — the comparison never degrades to the aggregate-only classification it replaced, which would be this finding restored silently. That is the one failure mode reading a file has that a Rust constant does not, so it is refused rather than defaulted (a value not observed is never a pass)"
else
  fail "round14 F4: an unreadable canonical artifact must be fatal (out: $f4_out)"
fi

# --- THE REPORT CITES THE WIDENED SCOPE, AND A NARROWER RECORD IS REFUSED --------------
# The reporter prints "N canonical field(s) compared". A session pinned by the PRE-FIX driver
# recorded nine, and would otherwise be reported exactly like one that compared all thirteen
# including the component map — the recorded-scope-weaker-than-claimed shape.
#
# ITS OWN SESSION, deliberately: the F3 block's last case POPS `canonical_corpus` from `$f3c_sess`,
# so reusing it would meet the absent-block refusal instead of this case's subject. Measured — the
# first version of this case did exactly that and reported the wrong refusal.
f4_sess="$TMP/f4-scope"
make_session "$f4_sess" "$GOOD_FLIGHT"
# STAMPED EXPLICITLY, because `run_report` stamps the pin only IF ABSENT — so a case that edits the
# pin must create it first. Measured: without this the edit below ran against a nonexistent file.
ws0_pin_session_corpus "$f4_sess" "$TMP/corpus"
python3 - "$f4_sess" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
pin = json.loads(p.read_text())
# THE PRE-FIX RECORD's compared_fields: F3's nine scalars, no components/seed/table.
pin["canonical_corpus"]["compared_fields"] = sorted([
    "rows", "partitions", "rows_per_partition", "cells_per_row", "data_db_bytes",
    "total_component_bytes", "bytes_per_row", "data_db_sha256", "schema_sha256"])
p.write_text(json.dumps(pin, indent=1) + "\n")
PY
out=$(run_report "$f4_sess" "$TMP/corpus"); rc=$?
if [ "$rc" -ne 0 ] && grep -q 'records a comparison over 9 field(s), not this module' <<<"$out"; then
  pass "OBSERVED (round14 F4): a session recording the PRE-FIX nine-field comparison is REFUSED by the reporter rather than printed as a full one — the report cites 'N canonical field(s) compared', so a narrower recorded scope would be published as the widened comparison it never performed"
else
  fail "round14 F4: a pre-fix-scope canonical record must be refused (rc=$rc, out: $(head -3 <<<"$out"))"
fi

# ==========================================================================
# ROUND 15, C — THE INTEGER COMPARISON TRUNCATED, USING A FACILITY THAT EXISTS
# ==========================================================================
# `divergences` compared integers with a bare `int()`, so `int(100.9)` was 100 and a corpus
# recording `rows_per_partition: 100.9` MATCHED the canonical 100 and produced NO DIVERGENCE. And
# `load_corpus_identity` validated neither `rows_per_partition` nor `total_component_bytes`, so
# neither field had a domain ANYWHERE — the value reached the comparison unvalidated and was
# rounded into agreement.
#
# This is round 12's F5 defect verbatim: `exact_int` was built THAT round because `int()` accepts
# bools and truncates floats, and this code — written after it — reached for `int()` anyway. The
# fix is `exact_int` on both sides plus a domain for the two fields in `load_corpus_identity`.
f5c_out=$(WS0_F5_PERF="$f3c_perf" WS0_F5_ROOT="$REPO_ROOT" WS0_F5_CORPUS="$f3c_canon" python3 - <<'PY' 2>&1
import copy, json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F5_PERF"])
from ws0_canonical_corpus import (CANONICAL_ARTIFACT_REL, MODE_BASELINE, _anchor_pins,
                                  canonical_components, canonical_pins, divergences,
                                  require_canonical_or_declared)
from ws0_validate import Invalid
root = pathlib.Path(os.environ["WS0_F5_ROOT"])
corpus = pathlib.Path(os.environ["WS0_F5_CORPUS"])
base = json.loads((root / CANONICAL_ARTIFACT_REL).read_text())
pins, anchors = canonical_pins(root), _anchor_pins(root)
comps = canonical_components(root, pins)

def prefix_int_same(got, want):
    """THE PRE-FIX INTEGER COMPARISON, VERBATIM: a bare int() on both sides."""
    try:
        return int(got) == int(want)
    except (TypeError, ValueError):
        return False

# The three integer census fields a FRACTIONAL value truncates onto, including the two that had no
# domain anywhere. `data_db_bytes` is the control: it IS validated by load_corpus_identity, so it
# shows the comparison itself was the defect and not only the absent validation.
for field, const, bad in (("rows_per_partition", "ROWS_PER_PARTITION", 100.9),
                          ("total_component_bytes", "TOTAL_COMPONENT_BYTES",
                           base["total_component_bytes"] + 0.9),
                          ("data_db_bytes", "DATA_DB_BYTES", base["data_db_bytes"] + 0.9)):
    j = copy.deepcopy(base)
    j[field] = bad
    # THE PREMISE: the pre-fix comparison saw NO difference at all.
    print(field, "PREFIX", "MATCHED_AS_CANONICAL" if prefix_int_same(bad, pins[const]) else "refused")
    diffs = divergences(j, pins, corpus, anchors, comps)
    print(field, "SHIPPED", "REFUSED" if any(field in d for d in diffs) else "ACCEPTED")
    try:
        require_canonical_or_declared(root, j, MODE_BASELINE, corpus)
        print(field, "BASELINE ACCEPTED")
    except Invalid:
        print(field, "BASELINE REFUSED")
# ...and the field with no domain is now validated AT LOAD, by name, before any comparison runs.
PY
)
f5c_ok=1
for f5c_field in rows_per_partition total_component_bytes data_db_bytes; do
  grep -q "$f5c_field PREFIX MATCHED_AS_CANONICAL" <<<"$f5c_out" || f5c_ok=0
  grep -q "$f5c_field SHIPPED REFUSED" <<<"$f5c_out" || f5c_ok=0
  grep -q "$f5c_field BASELINE REFUSED" <<<"$f5c_out" || f5c_ok=0
done
if [ "$f5c_ok" -eq 1 ]; then
  pass "OBSERVED (round15 C) NON-VACUITY, MEASURED FLIP on identical input: three FRACTIONAL integer census values (rows_per_partition=100.9, and +0.9 on total_component_bytes and data_db_bytes) were each MATCHED AS CANONICAL by the pre-fix bare-int() comparison — truncated into agreement — and are each REFUSED now. The comparison uses ws0_validate.exact_int, the facility round 12's F5 built for exactly this because int() accepts bools and truncates floats; this code was written after it and reached for int() anyway"
else
  fail "round15 C: a fractional integer census value must be REFUSED where the pre-fix int() truncated it into agreement (out: $f5c_out)"
fi
# ...and the two fields that had NO DOMAIN ANYWHERE are validated AT LOAD, by name — the half that
# closes the class rather than one call site. Driven through the SHIPPED loader over a real corpus.
f5c_dom="$TMP/f5c-domain"; make_corpus "$f5c_dom"
f5c_out=$(WS0_F5_PERF="$f3c_perf" WS0_F5_CORPUS="$f5c_dom" python3 - <<'PY' 2>&1
import json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F5_PERF"])
from ws0_validate import Invalid, load_corpus_identity
corpus = pathlib.Path(os.environ["WS0_F5_CORPUS"])
idp = corpus / "corpus-identity.json"
base = json.loads(idp.read_text())
# ABSENT is NOT refused here: a SMOKE corpus records neither, and --non-baseline exists so it still
# runs (rounds 9/10/11 each broke a documented command by refusing too much). Absence is refused by
# the canonical comparison, which reports it as RECORDED NOTHING.
print("ABSENT_STILL_LOADS", bool(load_corpus_identity(corpus)))
for field, bad in (("rows_per_partition", 100.9), ("total_component_bytes", 4096.5),
                   ("rows_per_partition", True), ("total_component_bytes", 0)):
    j = dict(base); j[field] = bad
    idp.write_text(json.dumps(j))
    try:
        load_corpus_identity(corpus)
        print(field, repr(bad), "ACCEPTED")
    except Invalid as exc:
        print(field, repr(bad), "REFUSED", field in str(exc))
idp.write_text(json.dumps(base))
PY
)
if grep -q 'ABSENT_STILL_LOADS True' <<<"$f5c_out" \
   && grep -q "^rows_per_partition 100.9 REFUSED True" <<<"$f5c_out" \
   && grep -q "^total_component_bytes 4096.5 REFUSED True" <<<"$f5c_out" \
   && grep -q "^rows_per_partition True REFUSED True" <<<"$f5c_out" \
   && grep -q "^total_component_bytes 0 REFUSED True" <<<"$f5c_out"; then
  pass "OBSERVED (round15 C): rows_per_partition and total_component_bytes are VALIDATED AT LOAD by name — fractional, boolean and non-positive values are each REFUSED, where pre-fix they had NO DOMAIN ANYWHERE (in neither IDENTITY_INT_FIELDS nor any other check) and reached the canonical comparison unvalidated. ABSENCE still loads, deliberately: a smoke corpus records neither, and absence is refused by the canonical comparison as RECORDED NOTHING rather than by breaking the --non-baseline path"
else
  fail "round15 C: the two undomained identity integers must be validated at load without refusing absence (out: $f5c_out)"
fi

# ==========================================================================
# ROUND 15, B — THE VERDICT WAS COERCED, SO THE STRING "false" WAS TRUE
# ==========================================================================
# `verify_pinned_canonical_corpus` re-derived its verdicts through `bool()`, which is TRUTHINESS —
# so a record carrying `"is_baseline": "false"` was ACCEPTED, and re-emitted with `is_baseline`
# still the STRING `'false'`, which the reporter reads as TRUE in `if canonical["is_baseline"]` and
# prints the BASELINE title over. This is the `!= BAD` permissive-coercion shape the whole issue is
# about, and `divergences`'s own bool comparison already did it correctly (isinstance, then
# identity) two hundred lines up in the same file.
#
# And the divergence LIST's shape was checked while its CONTENTS were not, so a non-string element
# passed validation and raised an UNCAUGHT TypeError in the reporter's `"; ".join(...)`.
f5b_out=$(WS0_F5_PERF="$f3c_perf" python3 - <<'PY' 2>&1
import copy, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F5_PERF"])
from ws0_canonical_corpus import CANONICAL_CENSUS, MODE_BASELINE, NON_BASELINE_LABEL, RUST_PIN_REL
from ws0_canonical_record import PIN_CANONICAL_FIELD, verify_pinned_canonical_corpus
from ws0_validate import Invalid

p = pathlib.Path("/synthetic/session-corpus-pin.json")
GOOD = {"mode": MODE_BASELINE, "is_canonical": True, "is_baseline": True,
        "label": "the canonical measurement corpus, measured as a WS0 BASELINE",
        "divergences": [], "compared_fields": sorted(CANONICAL_CENSUS),
        "canonical_pin_source": RUST_PIN_REL,
        "canonical_component_source": "docs/reports/ws0-3096-artifacts/corpus-identity.json",
        "canonical_components": 8}

def prefix_verdict(rec):
    """THE PRE-FIX VERDICT RE-DERIVATION, VERBATIM: bool() on both sides."""
    diffs = rec["divergences"]
    ok_canon = bool(rec["is_canonical"]) == (not diffs)
    ok_base = bool(rec["is_baseline"]) == (not diffs and rec["mode"] == MODE_BASELINE)
    return ok_canon and ok_base

# The surrounding PIN, realistic: a canonical verdict is cross-checked against the number of
# components the pin recorded for the corpus it measured (round 15's A), so a synthetic record
# needs the map a real pin carries — 8, matching `canonical_components`.
PIN_COMPONENTS = {f"nb-1-big-{c}.db": {"bytes": 1, "sha256": "0" * 64}
                  for c in ("Data", "Index", "Statistics", "Summary", "Filter",
                            "CompressionInfo", "Digest", "TOC")}

def shipped(rec):
    try:
        return "ACCEPTED", verify_pinned_canonical_corpus(
            p, {PIN_CANONICAL_FIELD: rec, "components": PIN_COMPONENTS})
    except Invalid as exc:
        return "REFUSED", exc

# (1) THE FINDING: the STRING "false" as a verdict.
for verdict in ("is_baseline", "is_canonical"):
    r = copy.deepcopy(GOOD)
    r[verdict] = "false"
    print(verdict, 'STRFALSE PREFIX', "ACCEPTED_AS_TRUE" if prefix_verdict(r) else "refused")
    state, res = shipped(r)
    print(verdict, "STRFALSE SHIPPED", state, verdict in str(res))
# (2) A NON-STRING divergence: accepted by the pre-fix validation, then an UNCAUGHT TypeError where
#     the reporter joins them. Both halves driven.
r = copy.deepcopy(GOOD)
r.update({"is_canonical": False, "is_baseline": False,
          "label": NON_BASELINE_LABEL + " (1 field(s) diverge)",
          "divergences": [{"rows": "wrong"}]})
print("NONSTR PREFIX_VERDICT_OK", prefix_verdict(r))
try:
    "; ".join(r["divergences"])
    print("NONSTR JOIN_OK")
except TypeError as exc:
    print("NONSTR PREFIX_UNCAUGHT_TYPEERROR", exc)
state, res = shipped(r)
print("NONSTR SHIPPED", state, "divergences[0]" in str(res))
# (3) An EMPTY divergence string: it counts toward len(diffs) so the verdict re-derivation is
#     satisfied, while printing NO reason — a reader is told N fields diverged and shown fewer.
r["divergences"] = [""]
print("EMPTY PREFIX_VERDICT_OK", prefix_verdict(r))
state, res = shipped(r)
print("EMPTY SHIPPED", state)
# (4) THE ACCEPT DIRECTION, both modes — a guard that refused everything would satisfy the above.
state, res = shipped(copy.deepcopy(GOOD))
print("ACCEPT_BASELINE", state, res["is_baseline"] is True if state == "ACCEPTED" else res)
r = copy.deepcopy(GOOD)
r.update({"is_canonical": False, "is_baseline": False,
          "label": NON_BASELINE_LABEL + " (1 field(s) diverge)",
          "divergences": ["rows: 1000 (canonical 4000000)"]})
state, res = shipped(r)
print("ACCEPT_NONBASELINE", state, res["is_baseline"] is False if state == "ACCEPTED" else res)
PY
)
f5b_ok=1
for f5b_v in is_baseline is_canonical; do
  grep -q "$f5b_v STRFALSE PREFIX ACCEPTED_AS_TRUE" <<<"$f5b_out" || f5b_ok=0
  grep -q "$f5b_v STRFALSE SHIPPED REFUSED True" <<<"$f5b_out" || f5b_ok=0
done
if [ "$f5b_ok" -eq 1 ]; then
  pass "OBSERVED (round15 B) NON-VACUITY, MEASURED FLIP on identical input: a record carrying the STRING \"false\" for is_baseline (and for is_canonical) was ACCEPTED AS TRUE by the pre-fix bool() re-derivation reconstructed verbatim — bool('false') is True — and is REFUSED now, naming the field. The reporter reads the value straight into \`if canonical[\"is_baseline\"]\`, so the string would have printed the BASELINE title over a smoke corpus; the verdicts must be EXACT JSON booleans, which is what divergences's own bool comparison already did correctly in the same file"
else
  fail "round15 B: a string verdict must be refused where the pre-fix bool() read it as true (out: $f5b_out)"
fi
if grep -q 'NONSTR PREFIX_VERDICT_OK True' <<<"$f5b_out" \
   && grep -q 'NONSTR PREFIX_UNCAUGHT_TYPEERROR' <<<"$f5b_out" \
   && grep -q 'NONSTR SHIPPED REFUSED True' <<<"$f5b_out" \
   && grep -q 'EMPTY PREFIX_VERDICT_OK True' <<<"$f5b_out" \
   && grep -q 'EMPTY SHIPPED REFUSED' <<<"$f5b_out"; then
  pass "OBSERVED (round15 B): a NON-STRING divergence element passed the pre-fix validation and then raised an UNCAUGHT TypeError where the reporter joins them (measured: 'expected str instance, dict found') — a traceback instead of the named refusal this record exists to give; and an EMPTY divergence string satisfied the verdict re-derivation while printing NO reason, so a reader was told N fields diverged and shown fewer than N. Every element must now be a non-empty string"
else
  fail "round15 B: divergences must be a list of non-empty strings, driven against the pre-fix TypeError (out: $f5b_out)"
fi
if grep -q 'ACCEPT_BASELINE ACCEPTED True' <<<"$f5b_out" \
   && grep -q 'ACCEPT_NONBASELINE ACCEPTED True' <<<"$f5b_out"; then
  pass "OBSERVED (round15 B) ACCEPT: a healthy record is ACCEPTED in BOTH modes — a canonical baseline with an empty divergence list, and a labelled non-baseline carrying a real divergence SENTENCE — so the stricter validation cannot be satisfied by refusing everything"
else
  fail "round15 B: the stricter verdict/divergence validation must still admit a healthy record in both modes (out: $f5b_out)"
fi

# ==========================================================================
# ROUND 15, A — THE COMPONENT MAP'S PROVENANCE AND EXTENT WERE WRITTEN AND NEVER READ
# ==========================================================================
# Round 14's F4 added the component-map comparison AND recorded two fields describing its scope —
# `canonical_component_source` (which artifact the map came from) and `canonical_components` (how
# many components it covered). The reporter required NEITHER and validated NEITHER, so removing or
# altering either left the report still claiming the COMPLETE component map had been verified.
#
# That is round 6's B2 shape — a field WRITTEN and compared against nothing anywhere in the tree —
# reintroduced by the very commit that added the comparison those two fields describe. This module's
# import-time declared-but-unread assert exists to make exactly that impossible, and could not see
# it: the fields were never DECLARED in CANONICAL_RECORD_FIELDS to be checked against the reader.
#
# Driven END-TO-END through the reporter over a real session, because "the report still claimed
# verification" is a property of the printed output, not of the validator.
f5a_sess="$TMP/f5a-scope"
make_session "$f5a_sess" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$f5a_sess" "$TMP/corpus"
# THE PREMISE, measured on the SHIPPED pin: both fields are RECORDED by the writer.
f5a_out=$(python3 - "$f5a_sess" <<'PY' 2>&1
import json, pathlib, sys
rec = json.loads((pathlib.Path(sys.argv[1]) / "session-corpus-pin.json").read_text())["canonical_corpus"]
print("WRITTEN_SOURCE", rec.get("canonical_component_source"))
print("WRITTEN_COUNT", rec.get("canonical_components"))
PY
)
if grep -q 'WRITTEN_SOURCE docs/reports/ws0-3096-artifacts/corpus-identity.json' <<<"$f5a_out" \
   && grep -q 'WRITTEN_COUNT 8' <<<"$f5a_out"; then
  pass "OBSERVED (round15 A) PREMISE: the SHIPPED writer records canonical_component_source and canonical_components (8) in every session's pin — so these are fields the rig writes, and the question is whether anything reads them"
else
  fail "round15 A: the shipped pin must record both component-scope fields (out: $f5a_out)"
fi
# THE FINDING: each field REMOVED, then ALTERED, and the report run over it. Pre-fix all four
# mutations exited 0 with the report printing its canonical-comparison claim; each is now REFUSED.
f5a_ok=1
f5a_detail=""
for f5a_case in remove-source remove-count wrong-source zero-count fractional-count; do
  d="$TMP/f5a-$f5a_case"
  rm -rf "$d"; cp -R "$f5a_sess" "$d"
  python3 - "$d" "$f5a_case" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "session-corpus-pin.json"
pin = json.loads(p.read_text())
rec = pin["canonical_corpus"]
case = sys.argv[2]
if case == "remove-source":
    rec.pop("canonical_component_source")
elif case == "remove-count":
    rec.pop("canonical_components")
elif case == "wrong-source":
    # A PLAUSIBLE other artifact: the corpus's own identity, which is the CIRCULAR comparison F4's
    # docstring calls out — both sides from the corpus under test.
    rec["canonical_component_source"] = "corpus-identity.json"
elif case == "zero-count":
    rec["canonical_components"] = 0
elif case == "fractional-count":
    rec["canonical_components"] = 8.5
p.write_text(json.dumps(pin, indent=1) + "\n")
PY
  # THE PRE-FIX READER, reconstructed VERBATIM: its required-field list had neither name, and its
  # only source check was of `canonical_pin_source`. So it accepted every mutation above.
  f5a_pre=$(WS0_F5_PERF="$f3c_perf" WS0_F5_DIR="$d" python3 - <<'PY' 2>&1
import json, os, pathlib, sys
sys.path.insert(0, os.environ["WS0_F5_PERF"])
from ws0_canonical_corpus import RUST_PIN_REL
rec = json.loads((pathlib.Path(os.environ["WS0_F5_DIR"]) / "session-corpus-pin.json").read_text())["canonical_corpus"]
PREFIX_REQUIRED = ("mode", "is_canonical", "is_baseline", "label", "divergences",
                   "compared_fields", "canonical_pin_source")
absent = [f for f in PREFIX_REQUIRED if f not in rec]
ok = not absent and rec["canonical_pin_source"] == RUST_PIN_REL
print("PREFIX", "ACCEPTED" if ok else "refused")
PY
)
  f5a_rep=$(run_report "$d" "$TMP/corpus" 2>&1); f5a_rc=$?
  grep -q 'PREFIX ACCEPTED' <<<"$f5a_pre" || { f5a_ok=0; f5a_detail="$f5a_case: pre-fix did not accept"; }
  if [ "$f5a_rc" -eq 0 ]; then
    f5a_ok=0; f5a_detail="$f5a_case: the shipped reporter ACCEPTED it (and printed: $(grep -c 'canonical field(s) compared' <<<"$f5a_rep") claim line(s))"
  fi
  grep -qE 'canonical_component(_source|s)' <<<"$f5a_rep" \
    || { f5a_ok=0; f5a_detail="$f5a_case: the refusal does not name the field"; }
done
if [ "$f5a_ok" -eq 1 ]; then
  pass "OBSERVED (round15 A) NON-VACUITY, MEASURED FLIP on identical input: FIVE mutations of the two component-scope fields — each REMOVED, the source pointed at the corpus's OWN identity (F4's CIRCULAR comparison), and the count set to 0 and to 8.5 — were each ACCEPTED by the pre-fix reader reconstructed verbatim, whose required-field list carried NEITHER name and whose only source check was of canonical_pin_source. The report therefore kept citing the COMPLETE component map as verified over a record that no longer said which artifact it came from or how many components it covered. Each is now REFUSED end-to-end through the reporter, naming the field"
else
  fail "round15 A: each component-scope mutation must flip from pre-fix ACCEPTED to a shipped refusal naming the field ($f5a_detail)"
fi
# ...and the DECLARED-BUT-UNREAD direction is closed at import for these two like every other field:
# the assert scans the reader's own source for a subscript of each declared name. Driven by asking it
# about the two fields — a positive control, so a scan that could not tell read from unread says so.
f5a_out=$(WS0_F5_PERF="$f3c_perf" python3 - <<'PY' 2>&1
import os, sys
sys.path.insert(0, os.environ["WS0_F5_PERF"])
from ws0_canonical_record import CANONICAL_RECORD_FIELDS, _reader_reads
for f in ("canonical_component_source", "canonical_components"):
    print(f, "DECLARED", f in CANONICAL_RECORD_FIELDS, "READ", _reader_reads(f))
print("CONTROL_UNREAD_NAME", _reader_reads("a_field_this_reader_does_not_read"))
PY
)
if grep -q 'canonical_component_source DECLARED True READ True' <<<"$f5a_out" \
   && grep -q 'canonical_components DECLARED True READ True' <<<"$f5a_out" \
   && grep -q 'CONTROL_UNREAD_NAME False' <<<"$f5a_out"; then
  pass "OBSERVED (round15 A): both fields are now DECLARED in CANONICAL_RECORD_FIELDS and SUBSCRIPTED by the reader, so the import-time declared-but-unread assert covers them — the assert that exists to make round 6's B2 impossible could not see this instance precisely because the fields were never declared to it. The positive control (a name the reader provably does not read) still reports False, so the scan can tell read from unread"
else
  fail "round15 A: both component-scope fields must be declared AND read, with the positive control intact (out: $f5a_out)"
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
# The floor is DERIVED from the OBSERVED count — 13 at the split, 21 after round 14's F4 added its
# eight cases, and 23/26/29 after round 15's C, B and A each added two/three/three — each measured
# by running the suite — set just below
# it so adding a case does not red the suite, and far above zero. No case here skips conditionally,
# so the observed count is the same on every host.
MIN_CHECKS=27
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

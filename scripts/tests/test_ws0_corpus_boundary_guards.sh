#!/usr/bin/env bash
# Self-test for the WS0 rig's CORPUS-BYTE OBSERVATION (issue #3272, review round 21).
#
# Its own suite rather than more cases in `test_ws0_provenance_guards.sh`, on the campsite rule
# (that file is at ~1464 lines against the ~1500 test target) and by SUBJECT. The sibling's subject
# is whether a report IDENTIFIES the bytes and the configuration it describes — a property of the
# two ENDS of a session. This file's subject is one step in:
#
#     THE RECORDED IDENTITY MUST BE HASHED FROM THE BYTES, AND STILL TRUE MID-RUN.
#
# # THE FINDING
#
# `write_session_corpus_pin` built the pre-measurement pin by COPYING `data_db_sha256` and the whole
# `components` map out of the corpus's own `corpus-identity.json`. So the sidecar was BOTH the
# subject and the oracle: the pin and the sidecar agreed BY CONSTRUCTION, whatever the bytes on disk
# had done, and every downstream comparison against the pin was that sidecar's assertion restated.
#
# A pin that copies a claim is not a measurement. #3249's bar is not "the guard exists" but "the
# guard has been OBSERVED to fire", and a digest copied out of a file can never fire against that
# file: it is the hardcoded `_PERF_STATE="ok"` that survived 118/118 tests, with extra steps.
#
# THE ATTACK IT ADMITTED, performed below rather than argued: MUTATE a component DURING measurement
# and RESTORE it BEFORE reporting. Both ends then see the original bytes, so the pin matches the
# sidecar (it was copied from it), the report-time re-hash matches disk, every recorded identity
# check passes — while the reps on either side of the mutation measured DIFFERENT BYTES. Note the
# DIRECTION of the bias, which is why this is refused rather than captioned: the failure makes a
# session that measured inconsistent bytes report as a clean, identity-verified one. It biases
# TOWARD the claim.
#
# # THE FIX, IN TWO HALVES, AND WHY BOTH
#
#   * `measure_component_digests` HASHES the files, and the sidecar becomes something to COMPARE
#     AGAINST — a disagreement is refused before the first rep, naming the component. That closes
#     the CIRCULARITY.
#   * `verify_corpus_boundary` re-hashes the ACTUAL component bytes at each measurement boundary,
#     against the PIN, refusing the rep and naming what changed. That closes the MID-RUN window,
#     which no pre/post pair can see: a mutation restored before reporting is invisible at both
#     ends and visible only from INSIDE the run.
#
# Every case carries the MEASURED pre-fix behaviour, because per #3249 a guard never observed
# firing is not evidence — and the pre-fix acceptance is ASSERTED, so a case reds if its premise
# stops reproducing rather than passing on a refusal that has become unrelated to the finding.
#
# Hermetic: synthetic session dirs and synthetic multi-byte component files under $TMPDIR, driven
# through the SHIPPED writer, the SHIPPED boundary verifier and the SHIPPED reporter. No cargo,
# perf, sudo, taskset, root, corpus, network or driver invocation.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="$REPO_ROOT/scripts/perf/ws0_report.py"
PERF_DIR="$REPO_ROOT/scripts/perf"
BOUNDARY="$PERF_DIR/ws0_corpus_bytes.py"

fails=0
# `checks` counts what actually RAN (incremented by pass/fail themselves, never derived from the
# file), so the minimum-check floor at the end can see a block that silently never executed.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

for required in "$REPORT" "$BOUNDARY"; do
  [ -f "$required" ] || { echo "FAIL - missing $required"; exit 1; }
done
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

GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"__TAG__","endpoint":"__ENDPOINT__","requests_ok":1,"requests_error":0,"error_codes":{},"requests_unavailable":0,"rows_total":1000,"bytes_total":'"$WS0_PREFLIGHT_BYTES_PER_SCAN"',"rows_per_s":250.0,"duration_s":4.0}'

# `boundary <session> <corpus> <label>` — the SHIPPED boundary verifier through its CLI, which is
# the entry point a measurement driver calls between reps. Driven through the CLI rather than an
# inline import deliberately: the driver is shell, so the CLI is the real surface and an inline
# import would test a path nobody runs.
boundary() { python3 "$BOUNDARY" "$1" "$2" "$3" 2>&1; }

# `mutate_component <corpus> <suffix> <extra-bytes>` — append to a component, printing the ORIGINAL
# bytes as base64 so the caller can restore them EXACTLY. The mutation is a real byte change to a
# real file; nothing here edits a digest.
mutate_component() {
  python3 - "$1" "$2" "$3" <<'PY'
import base64, pathlib, sys
table = pathlib.Path(sys.argv[1]) / "ws0" / "events"
p = next(q for q in sorted(table.iterdir()) if q.name.endswith(sys.argv[2]))
raw = p.read_bytes()
print(p.name)
print(base64.b64encode(raw).decode())
p.write_bytes(raw + sys.argv[3].encode())
PY
}

# `restore_component <corpus> <name> <base64>` — put the original bytes back, byte for byte.
restore_component() {
  python3 - "$1" "$2" "$3" <<'PY'
import base64, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "ws0" / "events" / sys.argv[2]
p.write_bytes(base64.b64decode(sys.argv[3]))
PY
}

# `prefix_pin <session> <corpus>` — THE PRE-FIX WRITER, reconstructed from the round-20 source: the
# pin's Data.db digest and its whole component map COPIED out of `corpus-identity.json`, never
# hashed. Everything else comes from the SHIPPED writer, so the only difference between this pin and
# a real one is the one the finding is about.
#
# Reconstructed here rather than reverted in the shipped module because that is what makes the
# pre-fix ACCEPTANCE assertable: the cases below measure what this pin admits, so a case reds if the
# premise stops reproducing instead of silently becoming a test of something else.
#
# It FAILS LOUDLY: a `prefix_pin` that died silently would leave the case's session dir with NO pin
# at all, and every assertion below it would then be refused for the ABSENCE of a pin rather than
# for the defect — MEASURED while writing this suite, and it presented as five unrelated failures.
prefix_pin() {
  local session="$1"
  python3 - "$PERF_DIR" "$1" "$2" <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_canonical_corpus import require_canonical_or_declared
from ws0_session import PIN_TICKET_FIELD, session_pin_path
from ws0_ticket_input import measure_ticket_digest, write_ticket_template
from ws0_validate import load_corpus_identity
session, corpus = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
# The ticket the pin records the digest of — the same ordering the driver has (round 10 M1). The
# PRE-FIX pin recorded this field too, so the fixture must write the template first.
write_ticket_template(session, corpus / "ws0-events.cql")
identity = load_corpus_identity(corpus)
config = {"reps": "1", "temps": "warm", "arms": "bypass", "scan_passes": "1",
          "server_cpus": "2,10", "client_cpus": "4,12", "step_duration": "45s/1s",
          "flight_endpoint": "http://127.0.0.1:38017", "baseline_mode": "non-baseline"}
canonical = require_canonical_or_declared(
    pathlib.Path(sys.argv[1]).parent.parent, identity, config["baseline_mode"], corpus)
pin = {
    "corpus": str(corpus),
    "rows": identity["rows"],
    # THE DEFECT, verbatim: the sidecars values, COPIED. Not one byte is read.
    "data_db_bytes": identity["data_db_bytes"],
    "data_db_sha256": identity["data_db_sha256"],
    "components": {name: {"bytes": spec.get("bytes"), "sha256": spec.get("sha256")}
                   for name, spec in sorted((identity.get("components") or {}).items())
                   if isinstance(spec, dict)},
    "schema_sha256": identity.get("schema_sha256"),
    PIN_TICKET_FIELD: measure_ticket_digest(session),
    "config": config,
    "canonical_corpus": dict(canonical),
    "note": "the round-20 pin, reconstructed: every digest COPIED from corpus-identity.json",
}
session_pin_path(session).write_text(json.dumps(pin, indent=1) + "\n")
PY
  # FAIL LOUDLY. A `prefix_pin` that died silently would leave the case's session dir with NO pin,
  # and every assertion below it would be refused for the ABSENCE of a pin rather than for the
  # defect under test — MEASURED while writing this suite, and it presented as five unrelated
  # failures pointing at the wrong artifact.
  if [ ! -e "$session/session-corpus-pin.json" ]; then
    echo "FAIL - the PRE-FIX pin fixture could not be stamped in $session; every case below it"
    echo "       would be refused for an ABSENT pin rather than for the defect under test."
    exit 1
  fi
}

# ==========================================================================
# 1 — THE ATTACK: mutate DURING measurement, restore BEFORE reporting
# ==========================================================================
# The exact sequence in the finding, executed end to end against the PRE-FIX pin, so the pre-fix
# acceptance is MEASURED rather than asserted from the source.
#
# Timeline: pin, rep-1 boundary, MUTATE Index.db, rep-2 boundary, RESTORE, report.
make_corpus "$TMP/corpus-attack"
# The scan artifacts must name the corpus this session MEASURED (`WS0_SCAN_CORPUS` is how a case
# whose corpus is not the default tells the shared builder).
attack_dir="$TMP/attack"
WS0_SCAN_CORPUS="$TMP/corpus-attack" make_session "$attack_dir" "$GOOD_FLIGHT"
prefix_pin "$attack_dir" "$TMP/corpus-attack"
# ...the mutation, and the ORIGINAL bytes kept so the restore is exact.
attack_mut=$(mutate_component "$TMP/corpus-attack" "-Index.db" "MUTATED-MID-MEASUREMENT")
attack_name=$(sed -n 1p <<<"$attack_mut")
attack_b64=$(sed -n 2p <<<"$attack_mut")
# ...and the sidecar is REFRESHED beside it, which is what a regeneration (or a peer lane) does. The
# corpus is now fully self-consistent WHILE MUTATED, which is why no end-state check can see this.
python3 - "$TMP/corpus-attack" "$attack_name" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]); p = root / "ws0" / "events" / sys.argv[2]
ident = root / "corpus-identity.json"; j = json.loads(ident.read_text())
j["components"][sys.argv[2]] = {"bytes": p.stat().st_size,
                                "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
ident.write_text(json.dumps(j, indent=1) + "\n")
PY
# THE FIX, at the rep-2 boundary: the bytes on disk are re-hashed against the PIN and the rep is
# refused, naming the component.
attack_boundary=$(boundary "$attack_dir" "$TMP/corpus-attack" rep-2); attack_rc=$?
if [ "$attack_rc" -ne 0 ] \
   && grep -q 'THE CORPUS CHANGED DURING MEASUREMENT' <<<"$attack_boundary" \
   && grep -q "$attack_name" <<<"$attack_boundary"; then
  pass "OBSERVED (round21): a component MUTATED mid-measurement is REFUSED at the next boundary, NAMING the component"
else
  fail "round21: the boundary check must refuse a mid-measurement mutation and name the component (rc=$attack_rc, out: $attack_boundary)"
fi
# ...now RESTORE the bytes and the sidecar, exactly as the attack does, and REPORT.
restore_component "$TMP/corpus-attack" "$attack_name" "$attack_b64"
python3 - "$TMP/corpus-attack" "$attack_name" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]); p = root / "ws0" / "events" / sys.argv[2]
ident = root / "corpus-identity.json"; j = json.loads(ident.read_text())
j["components"][sys.argv[2]] = {"bytes": p.stat().st_size,
                                "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
ident.write_text(json.dumps(j, indent=1) + "\n")
PY
# THE MEASURED PRE-FIX ACCEPTANCE, and it is an ASSERTION: with the bytes restored, the PRE-FIX pin
# passes EVERY identity check the round-20 reporter performed — the pin matches the sidecar (copied
# from it), the report-time re-hash matches disk, and the full component set verifies. This is
# asserted rather than described so the case REDS if the premise stops reproducing.
attack_prefix=$(python3 - "$PERF_DIR" "$attack_dir" "$TMP/corpus-attack" <<'PY' 2>&1
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_corpus_bytes import verify_corpus_bytes, verify_corpus_components
from ws0_pin_components import verify_pinned_components
from ws0_session import session_pin_path
from ws0_validate import load_corpus_identity
session, corpus = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
identity = load_corpus_identity(corpus)
pin = json.loads(session_pin_path(session).read_text())
# The three checks that existed BEFORE this round, over the RESTORED corpus, against the COPIED pin.
ident_v = verify_corpus_bytes(corpus, identity)
comp_v = verify_corpus_components(corpus, identity)
pin_v = verify_pinned_components(session_pin_path(session), pin, corpus, identity,
                                 comp_v["components"])
print("PREFIX_DATA_DB_SHA_VERIFIED", ident_v["sha256_verified"])
print("PREFIX_COMPONENTS_VERIFIED", comp_v["components_verified_sha256"],
      "of", comp_v["components_recorded"])
print("PREFIX_PIN_COMPONENTS_VERIFIED", pin_v["pinned_components_verified_sha256"],
      "of", pin_v["pinned_components"])
print("PREFIX_PIN_DIGESTS_WERE_COPIED", "components_source" not in pin)
PY
)
if grep -q 'PREFIX_DATA_DB_SHA_VERIFIED True' <<<"$attack_prefix" \
   && grep -qE 'PREFIX_COMPONENTS_VERIFIED ([1-9][0-9]*) of \1$' <<<"$attack_prefix" \
   && grep -qE 'PREFIX_PIN_COMPONENTS_VERIFIED ([1-9][0-9]*) of \1$' <<<"$attack_prefix" \
   && grep -q 'PREFIX_PIN_DIGESTS_WERE_COPIED True' <<<"$attack_prefix"; then
  pass "NON-VACUITY MEASURED (round21): after the restore, the PRE-FIX pin passes EVERY identity check — Data.db digest verified, all components verified, all PINNED components verified — with its digests never having been hashed. The attack is invisible to every end-state check"
else
  fail "round21: the pre-fix acceptance must REPRODUCE (mutate-during, restore-before-reporting), or this suite proves nothing (out: $attack_prefix)"
fi
# ...and the probe is not uniformly broken: the SAME boundary check over the SAME restored corpus
# PASSES. Without this half a verifier hardcoded to refuse everything would satisfy the case above.
attack_after=$(boundary "$attack_dir" "$TMP/corpus-attack" rep-3); attack_after_rc=$?
if [ "$attack_after_rc" -eq 0 ] && grep -q 'unchanged' <<<"$attack_after"; then
  pass "POSITIVE CONTROL (round21): the same boundary check over the RESTORED corpus PASSES, so the refusal above is attributable to the mutated bytes and not to a verifier that refuses everything"
else
  fail "round21: the boundary check must accept an untouched corpus (rc=$attack_after_rc, out: $attack_after)"
fi
# ...and the mid-run refusals are a RECORD, not a claim: each boundary appends an observation naming
# what it re-hashed. A verification nobody can read back is not evidence.
if python3 - "$attack_dir" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "corpus-boundary-observations.jsonl"
obs = [json.loads(line) for line in p.read_text().splitlines() if line.strip()]
assert obs, "no boundary observation was recorded"
labels = [o["boundary"] for o in obs]
assert "rep-3" in labels, labels
for o in obs:
    assert o["components_verified"] == o["components_pinned"] >= 5, o
    assert o["verified_against"] == "session-corpus-pin.json", o
    assert "re-hashed FROM DISK" in o["note"], o
# The REFUSED boundary recorded NOTHING: an observation is written only for a boundary that
# actually verified, so the file can never carry a passing record for a rep that was refused.
assert "rep-2" not in labels, labels
PY
then
  pass "OBSERVED (round21): each verified boundary APPENDS an observation naming what it re-hashed, and a REFUSED boundary records none (a refused rep cannot leave a passing record)"
else
  fail "round21: boundary observations must be recorded per verified boundary and absent for a refused one"
fi

# ==========================================================================
# 2 — THE CIRCULARITY: a pin may not COPY a digest it never derived
# ==========================================================================
# The other half of the finding, at pin time. A sidecar that does NOT describe the bytes beside it
# was copied into the pin verbatim, so the pin agreed with it while both disagreed with disk.
circ_dir="$TMP/circular"; mkdir -p "$circ_dir"
make_corpus "$TMP/corpus-circular"
# The sidecar is refreshed to describe a MUTATED Index.db... and the bytes are then RESTORED, so the
# sidecar now describes bytes that are not there. This is the state a pin must not copy.
circ_mut=$(mutate_component "$TMP/corpus-circular" "-Index.db" "TRANSIENT")
circ_name=$(sed -n 1p <<<"$circ_mut")
circ_b64=$(sed -n 2p <<<"$circ_mut")
python3 - "$TMP/corpus-circular" "$circ_name" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]); p = root / "ws0" / "events" / sys.argv[2]
ident = root / "corpus-identity.json"; j = json.loads(ident.read_text())
j["components"][sys.argv[2]] = {"bytes": p.stat().st_size,
                                "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
ident.write_text(json.dumps(j, indent=1) + "\n")
PY
restore_component "$TMP/corpus-circular" "$circ_name" "$circ_b64"
circ_out=$(python3 - "$PERF_DIR" "$circ_dir" "$TMP/corpus-circular" <<'PY' 2>&1
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_session import write_session_corpus_pin
from ws0_ticket_input import write_ticket_template
from ws0_validate import Invalid, load_corpus_identity
session, corpus = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
write_ticket_template(session, corpus / "ws0-events.cql")
identity = load_corpus_identity(corpus)
# THE PRE-FIX CONSTRUCTION, inline: the sidecars component map, copied. It is recorded WITHOUT
# COMPLAINT, and the resulting pin AGREES with the sidecar exactly — while both disagree with disk.
copied = {name: {"bytes": s.get("bytes"), "sha256": s.get("sha256")}
          for name, s in sorted((identity.get("components") or {}).items())}
print("PREFIX_COPIED_PIN_AGREES_WITH_SIDECAR",
      copied == {n: {"bytes": s["bytes"], "sha256": s["sha256"]}
                 for n, s in sorted(identity["components"].items())})
cfg = {"reps": "1", "temps": "warm", "arms": "bypass", "scan_passes": "1",
       "server_cpus": "2,10", "client_cpus": "4,12", "step_duration": "45s/1s",
       "flight_endpoint": "http://127.0.0.1:38017", "baseline_mode": "non-baseline"}
# THE SHIPPED WRITER over the SAME input: it HASHES, so it refuses.
try:
    write_session_corpus_pin(session, corpus, identity, cfg, {"label": "non-baseline"})
    print("SHIPPED_WRITER_ACCEPTED")
except Invalid as exc:
    print("SHIPPED_WRITER_REFUSED", exc)
PY
)
if grep -q 'PREFIX_COPIED_PIN_AGREES_WITH_SIDECAR True' <<<"$circ_out" \
   && grep -q 'SHIPPED_WRITER_REFUSED' <<<"$circ_out" \
   && grep -q 'DISAGREES WITH THE RECORDED IDENTITY' <<<"$circ_out" \
   && grep -q "$circ_name" <<<"$circ_out"; then
  pass "OBSERVED (round21) + NON-VACUITY MEASURED: a sidecar describing bytes that are NOT on disk was copied into the pre-fix pin, which then AGREED with it by construction; the shipped writer HASHES and REFUSES, naming the component"
else
  fail "round21: the pin writer must hash the bytes and refuse a sidecar that disagrees with them (out: $circ_out)"
fi
# ...and no pin was left behind by the refusal: a refused session must not carry a record that reads
# like a completed pin.
if [ ! -e "$circ_dir/session-corpus-pin.json" ]; then
  pass "OBSERVED (round21): the refused pin write leaves NO session-corpus-pin.json behind"
else
  fail "round21: a refused pin must not be written"
fi
# THE ACCEPT DIRECTION for the writer, and the pin RECORDS that its digests were measured — so a
# reader can tell a MEASURED pin from a COPIED one without inferring it from a fields absence.
ok_dir="$TMP/measured"; mkdir -p "$ok_dir"
make_corpus "$TMP/corpus-measured"
if python3 - "$PERF_DIR" "$ok_dir" "$TMP/corpus-measured" <<'PY'
import hashlib, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_session import write_session_corpus_pin
from ws0_ticket_input import write_ticket_template
from ws0_validate import load_corpus_identity
session, corpus = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
write_ticket_template(session, corpus / "ws0-events.cql")
identity = load_corpus_identity(corpus)
cfg = {"reps": "1", "temps": "warm", "arms": "bypass", "scan_passes": "1",
       "server_cpus": "2,10", "client_cpus": "4,12", "step_duration": "45s/1s",
       "flight_endpoint": "http://127.0.0.1:38017", "baseline_mode": "non-baseline"}
pin = write_session_corpus_pin(session, corpus, identity, cfg, {"label": "non-baseline"})
assert "measured" in pin["components_source"], pin["components_source"]
table = corpus / "ws0" / "events"
assert len(pin["components"]) >= 5, pin["components"]
# EVERY pinned digest equals an INDEPENDENT hash of the file — the assertion that the pin holds
# observations. Computed here with hashlib rather than by calling the module under test.
for name, spec in pin["components"].items():
    raw = (table / name).read_bytes()
    assert spec["sha256"] == hashlib.sha256(raw).hexdigest(), name
    assert spec["bytes"] == len(raw), name
assert pin["data_db_sha256"] == pin["components"][pin["components"] and
    next(n for n in pin["components"] if n.endswith("-Data.db"))]["sha256"], pin
PY
then
  pass "OBSERVED (round21) ACCEPT: every digest in a written pin equals an INDEPENDENT hash of the file, and the pin RECORDS that its digests were measured"
else
  fail "round21: the accept direction must show the pin holds observed digests"
fi
# ...and the REPORTER refuses a pin that does not record having measured them: a copied pin is
# textually indistinguishable from a measured one, so the record is REQUIRED, not preferred.
make_corpus "$TMP/corpus-nosrc"
src_dir="$TMP/no-source"
WS0_SCAN_CORPUS="$TMP/corpus-nosrc" make_session "$src_dir" "$GOOD_FLIGHT"
prefix_pin "$src_dir" "$TMP/corpus-nosrc"
src_out=$(python3 "$REPORT" --dir "$src_dir" --corpus "$TMP/corpus-nosrc" 2>&1); src_rc=$?
if [ "$src_rc" -ne 0 ] && grep -q 'does not record that its component digests were MEASURED' <<<"$src_out"; then
  pass "OBSERVED (round21): the reporter REFUSES a PRE-FIX pin that does not record having measured its digests (a copied pin reads exactly like a measured one)"
else
  fail "round21: a pin without components_source must be refused (rc=$src_rc, out: $src_out)"
fi
# ...and the accept direction of that requirement, so it is not a check that refuses every session:
# a pin from the shipped writer is ACCEPTED and the source is carried into results.json.
src_ok="$TMP/source-ok"
WS0_SCAN_CORPUS="$TMP/corpus-nosrc" make_session "$src_ok" "$GOOD_FLIGHT"
src_ok_out=$(run_report "$src_ok" "$TMP/corpus-nosrc"); src_ok_rc=$?
if [ "$src_ok_rc" -eq 0 ] && python3 - "$src_ok/results.json" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))["session_corpus_pin"]
assert "measured" in p["pinned_components_source"], p
assert p["pinned_components"] >= 5, p
PY
then
  pass "OBSERVED (round21) ACCEPT: a MEASURED pin is accepted and results.json carries WHERE its digests came from"
else
  fail "round21: the measured-pin accept direction must be recorded (rc=$src_ok_rc, out: $src_ok_out)"
fi

# ...and NO FLAG SCOPES THE PIN'S HASH AWAY. `--skip-corpus-digest` scopes the REPORT-TIME
# re-derivation, which meant a sidecar whose top-level `data_db_sha256` disagreed with the bytes
# reached a report with EVERY recorded identity unhashed. The pin's hash is on the SETUP path — once
# per session, not once per rep — so the writer takes no skip parameter AT ALL: there is no flag to
# pass, which is stronger than a flag that is checked (a value that cannot be supplied cannot
# disagree). Driven through the SHIPPED writer, where the refusal now belongs: a stale digest stops
# the RUN before the first rep rather than the report after the last.
stale_dir="$TMP/stale-top-sha"; mkdir -p "$stale_dir"
make_corpus "$TMP/corpus-stale-top"
# A ONE-CHARACTER change to the sidecar's TOP-LEVEL digest, with the size and the component map left
# correct: only a hash of the Data.db can see this, and only the top-level field is wrong — so a
# refusal here is attributable to that field alone.
python3 - "$TMP/corpus-stale-top/corpus-identity.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p))
s = list(j["data_db_sha256"]); s[0] = "5" if s[0] != "5" else "6"
j["data_db_sha256"] = "".join(s)
json.dump(j, open(p, "w"))
PY
stale_out=$(python3 - "$PERF_DIR" "$stale_dir" "$TMP/corpus-stale-top" <<'PY' 2>&1
import inspect, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_session import write_session_corpus_pin
from ws0_ticket_input import write_ticket_template
from ws0_validate import Invalid, load_corpus_identity
session, corpus = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
write_ticket_template(session, corpus / "ws0-events.cql")
cfg = {"reps": "1", "temps": "warm", "arms": "bypass", "scan_passes": "1",
       "server_cpus": "2,10", "client_cpus": "4,12", "step_duration": "45s/1s",
       "flight_endpoint": "http://127.0.0.1:38017", "baseline_mode": "non-baseline"}
# STRUCTURAL: the writer has NO skip parameter, so the hash cannot be scoped away by any caller.
params = inspect.signature(write_session_corpus_pin).parameters
print("WRITER_HAS_SKIP_PARAM", any("skip" in n for n in params))
try:
    write_session_corpus_pin(session, corpus, load_corpus_identity(corpus), cfg,
                             {"label": "non-baseline"})
    print("ACCEPTED")
except Invalid as exc:
    print(f"REFUSED {exc}")
PY
)
if grep -q 'WRITER_HAS_SKIP_PARAM False' <<<"$stale_out" \
   && grep -q '^REFUSED' <<<"$stale_out" \
   && grep -q 'data_db_sha256' <<<"$stale_out"; then
  pass "OBSERVED (round21): a stale TOP-LEVEL Data.db digest is refused at PIN time, and the writer has no skip parameter at all — the one recorded identity that reached a report unhashed under --skip-corpus-digest"
else
  fail "round21: the pin's hash must not be scopable by any flag, and a stale top-level digest must be refused (out: $stale_out)"
fi

# ==========================================================================
# 3 — FAIL CLOSED: a component that cannot be hashed is never "assumed unchanged"
# ==========================================================================
unread_dir="$TMP/unreadable"; make_corpus "$TMP/corpus-unread"
WS0_SCAN_CORPUS="$TMP/corpus-unread" make_session "$unread_dir" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$unread_dir" "$TMP/corpus-unread"
unread_target="$(ls "$TMP/corpus-unread/ws0/events/"*-Index.db | head -1)"
chmod 000 "$unread_target" 2>/dev/null
if [ -r "$unread_target" ]; then
  # Running as root (or on a filesystem ignoring the mode) makes the file readable anyway, so the
  # PREMISE of this case does not hold. That is a FAILED check, never a skip: a case that cannot
  # establish its own precondition must not report success.
  fail "round21: the unreadable-component case could not make a component unreadable (running as root?), so its precondition does not hold and it cannot report a pass"
else
  unread_out=$(boundary "$unread_dir" "$TMP/corpus-unread" rep-1); unread_rc=$?
  if [ "$unread_rc" -ne 0 ] && grep -q 'could not be read to derive its sha256' <<<"$unread_out" \
     && grep -q 'never a match' <<<"$unread_out"; then
    pass "OBSERVED (round21): a component whose bytes CANNOT be hashed FAILS CLOSED at the boundary — never assumed unchanged"
  else
    fail "round21: an unhashable component must fail closed (rc=$unread_rc, out: $unread_out)"
  fi
fi
chmod 644 "$unread_target" 2>/dev/null

# ==========================================================================
# 4 — the boundary check's OWN preconditions cannot silently not-run
# ==========================================================================
# NO PIN: there is nothing to verify the corpus AGAINST, and a boundary check that reported success
# there would be claiming a verification it never performed.
nopin_dir="$TMP/no-pin"; mkdir -p "$nopin_dir"
nopin_out=$(boundary "$nopin_dir" "$TMP/corpus" rep-1); nopin_rc=$?
if [ "$nopin_rc" -ne 0 ] && grep -q 'carries no session-corpus-pin.json' <<<"$nopin_out"; then
  pass "OBSERVED (round21): a boundary check with NO pin is REFUSED (comparing against nothing must not report success)"
else
  fail "round21: a boundary check without a pin must be refused (rc=$nopin_rc, out: $nopin_out)"
fi
# A pin carrying NO component map: only Data.db could be covered, while a scan reads Index.db.
nocomp_dir="$TMP/no-comps"; make_corpus "$TMP/corpus-nocomp"
WS0_SCAN_CORPUS="$TMP/corpus-nocomp" make_session "$nocomp_dir" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$nocomp_dir" "$TMP/corpus-nocomp"
python3 - "$nocomp_dir/session-corpus-pin.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p)); j.pop("components")
json.dump(j, open(p, "w"), indent=1)
PY
nocomp_out=$(boundary "$nocomp_dir" "$TMP/corpus-nocomp" rep-1); nocomp_rc=$?
if [ "$nocomp_rc" -ne 0 ] && grep -q 'records no' <<<"$nocomp_out"; then
  pass "OBSERVED (round21): a pin with NO component map is REFUSED at the boundary (an absent comparison must not read as a passing one)"
else
  fail "round21: a componentless pin must be refused at the boundary (rc=$nocomp_rc, out: $nocomp_out)"
fi
# A component that VANISHED mid-measurement, and one that APPEARED: both change what a scan reads,
# and both are restorable before the report.
gone_dir="$TMP/gone"; make_corpus "$TMP/corpus-gone"
WS0_SCAN_CORPUS="$TMP/corpus-gone" make_session "$gone_dir" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$gone_dir" "$TMP/corpus-gone"
gone_target="$(ls "$TMP/corpus-gone/ws0/events/"*-Index.db | head -1)"
mv "$gone_target" "$TMP/gone-index.bak"
gone_out=$(boundary "$gone_dir" "$TMP/corpus-gone" rep-2); gone_rc=$?
if [ "$gone_rc" -ne 0 ] && grep -q 'COMPONENT SET CHANGED DURING MEASUREMENT' <<<"$gone_out"; then
  pass "OBSERVED (round21): a component that VANISHED mid-measurement is REFUSED at the boundary"
else
  fail "round21: a vanished component must be refused at the boundary (rc=$gone_rc, out: $gone_out)"
fi
mv "$TMP/gone-index.bak" "$gone_target"
printf 'STRAY' > "$TMP/corpus-gone/ws0/events/nb-1-big-Digest.crc32"
appeared_out=$(boundary "$gone_dir" "$TMP/corpus-gone" rep-3); appeared_rc=$?
if [ "$appeared_rc" -ne 0 ] && grep -q 'never pinned' <<<"$appeared_out"; then
  pass "OBSERVED (round21): a component that APPEARED mid-measurement is REFUSED at the boundary"
else
  fail "round21: an appeared component must be refused at the boundary (rc=$appeared_rc, out: $appeared_out)"
fi
rm -f "$TMP/corpus-gone/ws0/events/nb-1-big-Digest.crc32"
# ...and after both are undone, the SAME boundary check passes — the discriminating control for this
# section, so none of its three refusals is a verifier that refuses every corpus.
undone_out=$(boundary "$gone_dir" "$TMP/corpus-gone" rep-4); undone_rc=$?
if [ "$undone_rc" -eq 0 ] && grep -q 'unchanged' <<<"$undone_out"; then
  pass "POSITIVE CONTROL (round21): with the set restored, the SAME boundary check PASSES (the three refusals above discriminate)"
else
  fail "round21: the boundary check must accept the restored component set (rc=$undone_rc, out: $undone_out)"
fi
# The CLI's usage contract, since the driver is the caller: a wrong argument count is a USAGE error
# (exit 2), distinct from a corpus that changed (exit 1) — a driver must not read one as the other.
usage_out=$(python3 "$BOUNDARY" 2>&1); usage_rc=$?
if [ "$usage_rc" -eq 2 ] && grep -q 'usage:' <<<"$usage_out"; then
  pass "OBSERVED (round21): the boundary CLI distinguishes a USAGE error (exit 2) from a corpus that changed (exit 1)"
else
  fail "round21: the boundary CLI must exit 2 on a usage error (rc=$usage_rc, out: $usage_out)"
fi

# ==========================================================================
# 5 — THE GUARD'S SCOPE: EVERY INPUT THE PIN DECLARES, NOT ONLY THE COMPONENTS
# ==========================================================================
# ROUND 24'S FINDING. The boundary check covered the SSTable components ONLY, while TWO other files
# are re-read DURING measurement:
#
#   * `ws0-events.cql`      — the bare scan INGESTS IT ON EVERY INVOCATION;
#   * `ticket-template.json` — `flight-loadgen --ticket-template` RE-READS IT ON EVERY INVOCATION of
#                              every rep of every arm.
#
# So the mutate-then-restore attack from section 1 still worked, aimed at either one instead of a
# component: both ends of the session agree, every report-time check agrees, and the boundary
# published `N of N components verified` — a count complete RELATIVE TO ITS OWN TOO-SMALL LIST. A
# guard that verifies 7 of 9 inputs and reports success is issuing a verdict about 2 it never looked
# at, and the omission biases TOWARD the claim.
#
# `prefix_boundary <session> <corpus> <label>` — THE PRE-FIX (round-23) BOUNDARY CHECK, reconstructed:
# the component-set comparison and the per-component re-hash, and NOTHING ELSE. Reconstructed here
# rather than reverted in the shipped module for the same reason `prefix_pin` is — it is what makes
# the pre-fix ACCEPTANCE assertable, so each case below reds if its premise stops reproducing instead
# of quietly becoming a test of something else. Every primitive it uses is the SHIPPED one, so the
# only difference from the shipped verifier is the scope the finding is about.
prefix_boundary() {
  python3 - "$PERF_DIR" "$1" "$2" "$3" <<'PY' 2>&1
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_corpus_bytes import CORPUS_TABLE_SUBPATH, session_pin_path, sha256_file
from ws0_validate import Invalid
session, corpus, label = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]), sys.argv[4]
pin = json.loads(session_pin_path(session).read_text())
pinned = pin["components"]
table = corpus.joinpath(*CORPUS_TABLE_SUBPATH)
try:
    present = {q.name for q in table.iterdir() if q.is_file()}
    assert not (set(pinned) ^ present), sorted(set(pinned) ^ present)
    checked = 0
    for name in sorted(pinned):
        p = table / name
        if p.stat().st_size != pinned[name]["bytes"] or sha256_file(p) != pinned[name]["sha256"]:
            raise Invalid(f"THE CORPUS CHANGED DURING MEASUREMENT: component {name}")
        checked += 1
except Invalid as exc:
    print(f"PREFIX_BOUNDARY_REFUSED {exc}")
else:
    # The pre-fix success text, verbatim in shape: a count that is complete about COMPONENTS and
    # silent about every other input the session declares.
    print(f"PREFIX_BOUNDARY_OK {checked} of {len(pinned)} pinned component(s) unchanged")
PY
}

# `mutate_file <path> <suffix-bytes>` / `restore_file <path> <base64>` — a real byte change to a real
# per-invocation input, with the original bytes kept so the restore is EXACT. Distinct from
# `mutate_component`, which resolves a name inside the corpus table dir.
mutate_file() {
  python3 - "$1" "$2" <<'PY'
import base64, pathlib, sys
p = pathlib.Path(sys.argv[1]); raw = p.read_bytes()
print(base64.b64encode(raw).decode())
p.write_bytes(raw + sys.argv[2].encode())
PY
}
restore_file() {
  python3 - "$1" "$2" <<'PY'
import base64, pathlib, sys
pathlib.Path(sys.argv[1]).write_bytes(base64.b64decode(sys.argv[2]))
PY
}

# `input_case <name> <field> <path-expr> <needle>` — one newly-covered input, measured through the
# SAME four-part shape as section 1, SEPARATELY per input:
#
#   (a) the PRE-FIX boundary check over the MUTATED input reports SUCCESS   [premise, ASSERTED]
#   (b) the SHIPPED boundary check over the same state REFUSES and NAMES it [the fix]
#   (c) restored, the report accepts the session and calls it verified      [why (a) is invisible]
#   (d) the SHIPPED check over the restored state PASSES                    [positive control]
#
# (a) is an assertion rather than a remark: without it (b) would keep passing for a verifier that
# refuses everything, and the case would survive the finding ceasing to reproduce.
input_case() {
  local name="$1" field="$2" target="$3" needle="$4"
  local corpus="$TMP/corpus-$name" session="$TMP/session-$name"
  make_corpus "$corpus"
  WS0_SCAN_CORPUS="$corpus" make_session "$session" "$GOOD_FLIGHT"
  # The shipped writer, so the pin under test is a real one (the ticket is written into the session
  # dir by the fixture, as the driver does).
  ws0_pin_session_corpus "$session" "$corpus"
  local path; path=$(eval "printf '%s' \"$target\"")
  [ -f "$path" ] || { fail "round24/$name: the input $path is not present, so this case cannot run"; return; }
  local b64; b64=$(mutate_file "$path" "MUTATED-BETWEEN-ARMS-$name")

  # (a) THE MEASURED PRE-FIX ACCEPTANCE.
  local pre; pre=$(prefix_boundary "$session" "$corpus" rep-2)
  if grep -qE 'PREFIX_BOUNDARY_OK ([1-9][0-9]*) of \1 pinned component' <<<"$pre"; then
    pass "NON-VACUITY MEASURED (round24/$name): with $(basename "$path") LIVE-MUTATED, the PRE-FIX boundary check reports SUCCESS — a component count complete relative to its own too-small list, about an input it never looked at"
  else
    fail "round24/$name: the pre-fix boundary check must ACCEPT the mutated input, or this case proves nothing (out: $pre)"
  fi
  # (b) THE FIX: the shipped check refuses, and NAMES the input.
  local out rc; out=$(boundary "$session" "$corpus" rep-2); rc=$?
  if [ "$rc" -ne 0 ] \
     && grep -q 'A MEASUREMENT INPUT CHANGED DURING MEASUREMENT' <<<"$out" \
     && grep -q "$(basename "$path")" <<<"$out" \
     && grep -q "$field" <<<"$out" \
     && grep -q "$needle" <<<"$out"; then
    pass "OBSERVED (round24/$name): the shipped boundary check REFUSES the mutated $(basename "$path"), naming the file and the pin field \`$field\`"
  else
    fail "round24/$name: the boundary check must refuse a mutated $field input and name it (rc=$rc, out: $out)"
  fi
  # (c) restored before reporting -> every END-STATE check agrees, which is why (a) is invisible.
  restore_file "$path" "$b64"
  local rep_out rep_rc; rep_out=$(run_report "$session" "$corpus"); rep_rc=$?
  if [ "$rep_rc" -eq 0 ]; then
    pass "PREMISE ASSERTED (round24/$name): with the mutation RESTORED the reporter ACCEPTS the session — the mid-run change to $(basename "$path") is invisible at both ends, so covering it INSIDE the run is the only place it can be seen"
  else
    fail "round24/$name: the restored session must report cleanly, or the attack this case demonstrates has changed (rc=$rep_rc, out: $rep_out)"
  fi
  # (d) POSITIVE CONTROL: the same check over the untouched inputs passes, and it NAMES what it
  # covered — so the refusal above is attributable to the mutated bytes and not to a broken verifier.
  local ok_out ok_rc; ok_out=$(boundary "$session" "$corpus" rep-3); ok_rc=$?
  if [ "$ok_rc" -eq 0 ] && grep -q "declared inputs verified: .*$field" <<<"$ok_out"; then
    pass "POSITIVE CONTROL (round24/$name): with every input untouched the SAME check PASSES and NAMES \`$field\` among the inputs it verified (a count alone is complete only about its own list)"
  else
    fail "round24/$name: the boundary check must accept untouched inputs and name the covered field (rc=$ok_rc, out: $ok_out)"
  fi
}

# THE SCHEMA — ingested by the bare scan on every invocation.
input_case schema schema_sha256 '${corpus}/ws0-events.cql' 'DIFFERENT SCHEMAS'
# THE FLIGHT TICKET — re-read by `flight-loadgen --ticket-template` on every invocation. It lives in
# the SESSION's exclusively-claimed output dir, not the corpus (#3272 round 13, F2).
input_case ticket ticket_template_sha256 '${session}/ticket-template.json' 'the REQUEST'

# --------------------------------------------------------------------------
# THE DERIVATION: a NEW declared input with NO coverage must FAIL, not pass
# --------------------------------------------------------------------------
# This is the property that stops the finding recurring at a FOURTH scope. The covered set is read
# OFF THE PIN, so a per-invocation input added later — a digest field nothing knows how to resolve —
# refuses the rep and names the field, rather than being silently omitted from a complete-looking
# count the way `schema_sha256` and `ticket_template_sha256` were.
make_corpus "$TMP/corpus-derived"
derived_dir="$TMP/derived"
WS0_SCAN_CORPUS="$TMP/corpus-derived" make_session "$derived_dir" "$GOOD_FLIGHT"
ws0_pin_session_corpus "$derived_dir" "$TMP/corpus-derived"
# The BASELINE first: this pin verifies, so the refusal below is attributable to the added field.
base_out=$(boundary "$derived_dir" "$TMP/corpus-derived" rep-1); base_rc=$?
# ...now a FUTURE input is declared in the pin and nothing covers it.
python3 - "$derived_dir/session-corpus-pin.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p))
j["prewarm_profile_sha256"] = "a" * 64
json.dump(j, open(p, "w"), indent=1)
PY
derived_out=$(boundary "$derived_dir" "$TMP/corpus-derived" rep-2); derived_rc=$?
if [ "$base_rc" -eq 0 ] && [ "$derived_rc" -ne 0 ] \
   && grep -q 'does not cover' <<<"$derived_out" \
   && grep -q 'prewarm_profile_sha256' <<<"$derived_out"; then
  pass "OBSERVED (round24): a NEW per-invocation input DECLARED in the pin with NO coverage REFUSES the rep and NAMES the field — the covered set is DERIVED from the pin, so the omission that produced this finding cannot recur silently at a third scope"
else
  fail "round24: an uncovered declared input must refuse and name the field (base_rc=$base_rc, rc=$derived_rc, out: $derived_out)"
fi
# ...and the DERIVATION is asserted directly, not only through its refusal: the covered set read off
# a real pin NAMES the schema and the ticket. A count could satisfy everything above while covering
# the wrong things.
if python3 - "$PERF_DIR" "$derived_dir" <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_corpus_bytes import declared_inputs, session_pin_path
pin = json.loads(session_pin_path(pathlib.Path(sys.argv[2])).read_text())
got = declared_inputs(pin)
for want in ("components", "data_db_sha256", "schema_sha256", "ticket_template_sha256"):
    assert want in got, (want, got)
# ...and it is DERIVED, not a constant: the field added to this pin above appears too.
assert "prewarm_profile_sha256" in got, got
PY
then
  pass "OBSERVED (round24): the covered set is DERIVED from the pin — it names the components, the Data.db declaration, the SCHEMA and the TICKET, and it picks up a field added to the pin that no list mentions"
else
  fail "round24: the covered set must be derived from the pin's own digest declarations"
fi
# ...and the RECORD names which inputs were covered, not just how many: a bare count is complete
# relative to whatever list produced it, which is the shape this whole section is about.
if python3 - "$derived_dir" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]) / "corpus-boundary-observations.jsonl"
obs = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
assert obs, "no boundary observation was recorded"
o = obs[-1]
for want in ("components", "schema_sha256", "ticket_template_sha256"):
    assert want in o["declared_inputs_verified"], (want, o)
assert {e["input"] for e in o["inputs"]} == set(o["declared_inputs_verified"]), o
# The REFUSED boundary (rep-2, the uncovered field) recorded NOTHING.
assert "rep-2" not in [x["boundary"] for x in obs], obs
PY
then
  pass "OBSERVED (round24): the boundary observation RECORDS WHICH declared inputs were verified — the schema and the ticket by name — and the refused boundary records none"
else
  fail "round24: the observation must name the covered inputs, not only count components"
fi
# ...and FAIL CLOSED: a covered input that cannot be hashed is never assumed unchanged. The
# uncovered field is removed FIRST, deliberately: the uncovered-declaration refusal is raised before
# any single-file input is resolved, so leaving it would make this case refuse for the previous
# case's reason and report a pass about a check that never ran.
python3 - "$derived_dir/session-corpus-pin.json" <<'PY'
import json, sys
p = sys.argv[1]; j = json.load(open(p)); j.pop("prewarm_profile_sha256")
json.dump(j, open(p, "w"), indent=1)
PY
gone_schema="$TMP/corpus-derived/ws0-events.cql"
mv "$gone_schema" "$TMP/derived-schema.bak"
missing_out=$(boundary "$derived_dir" "$TMP/corpus-derived" rep-4); missing_rc=$?
mv "$TMP/derived-schema.bak" "$gone_schema"
if [ "$missing_rc" -ne 0 ] && grep -q 'IS ABSENT at boundary' <<<"$missing_out" \
   && grep -q 'NOT assumed unchanged' <<<"$missing_out"; then
  pass "OBSERVED (round24): a covered input that is ABSENT at the boundary FAILS CLOSED — never assumed unchanged"
else
  fail "round24: an absent covered input must fail closed (rc=$missing_rc, out: $missing_out)"
fi

# ==========================================================================
# 6 — THE GUARD HAS BEEN OBSERVED TO FIRE THROUGH THE DRIVER'S OWN LOOP
# ==========================================================================
# Every check above calls the verifier DIRECTLY. That proves the VERIFIER refuses a changed corpus;
# it does not prove the RUN does. The one thing #3249's bar actually asks for is the asymmetric
# pair, taken through the driver's rep loop:
#
#   A. a component mutated BETWEEN REPS  -> the run exits NON-ZERO, the component is NAMED, and
#      the later reps DO NOT RUN.
#   B. the SAME input with the CALL SITE BYPASSED -> the run COMPLETES and PUBLISHES a figure.
#
# B is what makes A mean something: without it, A passes for any run that dies for any reason, and
# the case would keep passing after the finding stopped reproducing. Asserted, not narrated.
#
# Method (the same technique as `order_probe` in test_ws0_round_metadata.sh): the driver's loop is
# EVAL'd out of `ws0-baseline.sh`, over a real corpus and a real `session-corpus-pin.json`, with the
# boundary function SOURCED FROM THE SHIPPED LIBRARY — never stubbed, or the loop under test would
# not be the loop the driver runs. The verifier is NEVER called directly here: a direct call is what
# every check above already does, and it is precisely what cannot see whether the RUN refuses.
#
# The mutation is timed to land at the END of round 1 (inside the Flight arm's `measure_flight`) and
# the restore at the START of round 2 (inside that round's `measure_scan`) — the attack from §1, now
# performed by the loop itself. Under the WIRED loop the round-1 closing boundary refuses before the
# restore can run; under the BYPASSED loop the restore runs, both ends agree, and the report is
# published. Same corpus, same mutation, same reps: the ONLY difference is the call site.
#
# `driver_probe <session> <corpus> wired|bypassed` — echoes the run's whole output; returns the
# run's status. The reporter is invoked at the end exactly as the driver invokes it, so "the run
# published a figure" is observed from a real report and not inferred from an exit code.
driver_probe() {
  local session="$1" corpus="$2" mode="$3"
  # The mutation record lives OUTSIDE the session dir: a stray file inside it would be read by the
  # reporter's artifact-set integrity check and diagnosed as an unexpected artifact.
  local rec="$session.mutated"
  ( set -uo pipefail
    REPS=2; TEMPS="warm"; ARMS="bypass"; OUT_DIR="$session"; CORPUS="$corpus"; HERE="$PERF_DIR"
    mkdir -p "$OUT_DIR"
    # The scan artifacts must name the corpus this session MEASURED (as in §1).
    export WS0_SCAN_CORPUS="$corpus"
    ws0_pin_session_corpus "$OUT_DIR" "$CORPUS" 2 warm bypass 1
    # The two arms write real rep artifacts, so a run that reaches the end has a REPORTABLE session
    # — without which "the bypassed run publishes a figure" could not be asserted at all.
    measure_scan() {
      make_scan_rep "$OUT_DIR" "$1" "$2" ok
      # ...the RESTORE, at the start of round 2: this is the half that makes the attack invisible at
      # both ends of the session.
      [ "$2" != 2 ] || restore_component "$CORPUS" "$(sed -n 1p "$rec")" "$(sed -n 2p "$rec")"
    }
    measure_flight() {
      make_flight_rep "$OUT_DIR" "$1" "$2" ok "$GOOD_FLIGHT"
      # ...the MUTATION, at the end of round 1, i.e. BETWEEN REPS.
      [ "$2" != 1 ] || mutate_component "$CORPUS" "-Index.db" "MUTATED-BETWEEN-REPS" > "$rec"
    }
    # The boundary function from the SHIPPED library — sourced, never stubbed.
    # shellcheck source=scripts/perf/lib-corpus-boundary.sh
    source "$PERF_DIR/lib-corpus-boundary.sh"
    eval "$(awk '/^rotate_arms\(\)/,/^}/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh")"
    eval "$(awk '/^record_round\(\)/,/^}/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh")"
    _loop="$(awk '/^_ARM_LIST=/,/^done$/' "$REPO_ROOT/scripts/perf/ws0-baseline.sh")"
    # THE BYPASS, and it is the CALL SITE that is removed and nothing else — the same way the
    # sibling suites re-enact a pre-fix loop: the line is deleted from the driver's own text, so
    # what runs is this loop minus its boundary check, not a hand-written imitation of it.
    [ "$mode" = wired ] || _loop="$(grep -v 'verify_corpus_boundary_or_refuse' <<<"$_loop")"
    eval "$_loop"
    python3 "$PERF_DIR/ws0_report.py" --dir "$OUT_DIR" --corpus "$CORPUS"
  ) 2>&1
}

# --------------------------------------------------------------------------
# A — the WIRED run REFUSES, names the component, and does not proceed
# --------------------------------------------------------------------------
make_corpus "$TMP/corpus-driver-wired"
wired_out=$(driver_probe "$TMP/driver-wired" "$TMP/corpus-driver-wired" wired); wired_rc=$?
wired_component=$(grep -oE 'component nb-[0-9]+-big-Index\.db' <<<"$wired_out" | head -1)
if [ "$wired_rc" -ne 0 ] \
   && grep -q 'THE CORPUS CHANGED DURING MEASUREMENT' <<<"$wired_out" \
   && [ -n "$wired_component" ]; then
  pass "OBSERVED THROUGH THE DRIVER LOOP (round23): a component mutated BETWEEN REPS makes the RUN exit non-zero, naming the component ($wired_component)"
else
  fail "round23: the wired driver loop must refuse a mid-run mutation and name the component (rc=$wired_rc, out: $wired_out)"
fi
# ...at the boundary that CLOSES round 1, which is the window a pre/post pair cannot see.
if grep -q "boundary 'warm-1-after-bypass'" <<<"$wired_out"; then
  pass "OBSERVED (round23): the refusal is attributed to the boundary CLOSING round 1, not to either end of the session"
else
  fail "round23: the refusal must name the round-1 closing boundary (out: $wired_out)"
fi
# ...and the LATER REPS DID NOT RUN. Asserted from the artifacts on disk as well as the log, because
# a refusal that printed and then carried on would satisfy the exit-status check alone.
wired_round2_reps=$(ls "$TMP/driver-wired" 2>/dev/null | grep -c 'warm-2')
if ! grep -q 'round 2/2' <<<"$wired_out" \
   && [ "$wired_round2_reps" -eq 0 ] \
   && [ ! -e "$TMP/driver-wired/results.json" ]; then
  pass "OBSERVED (round23): the run STOPS at the refusal — round 2 never starts, it leaves no rep artifacts, and no results.json is published"
else
  fail "round23: a refused run must not proceed to later reps or publish (round-2 artifacts=$wired_round2_reps, out: $wired_out)"
fi

# --------------------------------------------------------------------------
# B — with the CALL SITE BYPASSED the SAME input COMPLETES and PUBLISHES a figure
# --------------------------------------------------------------------------
# This is the half that makes A mean something, and it is an ASSERTION rather than a remark: without
# it, A would keep passing for a run that died of anything at all — an unrelated fixture error, a
# reporter refusal, a missing python — and would keep passing after the finding stopped reproducing.
# What is measured here is the loop MINUS the one call, over the same corpus and the same mutation:
# it runs both rounds, the reporter accepts the session, and a bare/flight ratio is printed for a
# session whose two rounds measured DIFFERENT Index.db bytes.
make_corpus "$TMP/corpus-driver-bypassed"
bypassed_out=$(driver_probe "$TMP/driver-bypassed" "$TMP/corpus-driver-bypassed" bypassed)
bypassed_rc=$?
bypassed_ratio=$(grep -oE 'ratio bare/flight = [0-9.]+x' <<<"$bypassed_out" | head -1)
if [ "$bypassed_rc" -eq 0 ] \
   && grep -q 'round 2/2' <<<"$bypassed_out" \
   && [ -e "$TMP/driver-bypassed/results.json" ] \
   && [ -n "$bypassed_ratio" ]; then
  pass "PREMISE ASSERTED (round23): with the call site BYPASSED the SAME mutation COMPLETES both rounds and PUBLISHES a figure ($bypassed_ratio) — so A's refusal is attributable to the boundary check and to nothing else"
else
  fail "round23: the bypassed loop must complete and publish, or lane A proves nothing (rc=$bypassed_rc, ratio='$bypassed_ratio', out: $bypassed_out)"
fi
# ...and the reason it publishes is the one the finding names: the mutation was RESTORED before the
# report, so every END-STATE identity check agrees. Asserted from the published results.json, which
# is the artifact a reader would trust.
bypassed_ends=$(python3 - "$TMP/driver-bypassed/results.json" <<'PY' 2>&1
import json, pathlib, sys
r = json.loads(pathlib.Path(sys.argv[1]).read_text())
ident = r.get("corpus_identity_verification") or {}
print("END_DATA_DB_SHA_VERIFIED", ident.get("sha256_verified"))
comps = r.get("corpus_component_verification") or {}
print("END_COMPONENTS", comps.get("components_verified_sha256"), "of", comps.get("components_recorded"))
PY
)
if grep -q 'END_DATA_DB_SHA_VERIFIED True' <<<"$bypassed_ends" \
   && grep -qE 'END_COMPONENTS ([1-9][0-9]*) of \1$' <<<"$bypassed_ends"; then
  pass "PREMISE ASSERTED (round23): the published report of that run says the corpus digest and EVERY component VERIFIED — the mid-run mutation is invisible in the artifact a reader would trust, which is why the check has to be inside the loop"
else
  fail "round23: the bypassed run's published report must show a fully-verified corpus, or the finding it demonstrates has changed (out: $bypassed_ends)"
fi
# ...and the two runs are DISCRIMINATED by the call site alone: the bypassed run's log contains no
# refusal at all. Without this a lane-A refusal appearing in BOTH runs would go unnoticed.
if ! grep -q 'THE CORPUS CHANGED DURING MEASUREMENT' <<<"$bypassed_out"; then
  pass "OBSERVED (round23): the bypassed run emits NO boundary refusal, so the pair differs by the CALL SITE and not by their inputs"
else
  fail "round23: the bypassed run must not refuse — its loop has no boundary check (out: $bypassed_out)"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# Without `-e` a block that silently never executes LOWERS the count and registers NO failure, and
# the gate reads only the exit code — so a suite that ran 3 of its checks and passed them exits 0
# and reports SUCCESS. That is the suite-level vacuous green, one level up from the checks.
#
# The floor is DERIVED FROM THE OBSERVED COUNT — run, then recorded — never counted off the source:
# a source estimate understated a floor by 29 on this branch, because loops multiply.
MIN_CHECKS=35
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 corpus-boundary guards: all $checks checks passed"
  exit 0
fi
echo "ws0 corpus-boundary guards: $fails of $checks check(s) FAILED"
exit 1

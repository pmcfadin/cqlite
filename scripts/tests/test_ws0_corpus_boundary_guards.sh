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
# not be the loop the driver runs.
# TODO(#3272 round 23): fill in — A then B, one increment at a time.

# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e`
# ==========================================================================
# Without `-e` a block that silently never executes LOWERS the count and registers NO failure, and
# the gate reads only the exit code — so a suite that ran 3 of its checks and passed them exits 0
# and reports SUCCESS. That is the suite-level vacuous green, one level up from the checks.
#
# The floor is DERIVED FROM THE OBSERVED COUNT — run, then recorded — never counted off the source:
# a source estimate understated a floor by 29 on this branch, because loops multiply.
MIN_CHECKS=17
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

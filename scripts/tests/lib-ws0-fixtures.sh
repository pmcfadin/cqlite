#!/usr/bin/env bash
# lib-ws0-fixtures.sh — the SYNTHETIC SESSION ARTIFACTS the WS0 reporter self-tests build
# (issue #3272 review round 3, campsite-rule split).
#
# Sourced, not executed, and it sets NO shell options: `set -uo pipefail` in a library
# mutates the SOURCING shell's options, which is the caller's decision.
#
# # Why a library, and where the seam falls
#
# `test_ws0_report_guards.sh` and `test_ws0_fabrication_guards.sh` both feed the reporter
# synthetic session dirs, and three builders were IDENTICAL in both files — the perf CSV,
# the corpus (a real `Data.db` plus a `corpus-identity.json` whose digest is MEASURED from
# it), and the per-rep round metadata. Round 3 grew both files past the ~1500-line test
# target, and duplicated fixtures are the wrong thing to keep two copies of: `make_round`
# gained a `monotonic_ns` field this round and had to be edited in both places, which is
# exactly the drift a shared builder removes.
#
# What deliberately does NOT move: `make_scan_rep`/`make_flight_rep`/`make_session` and the
# `expect_*` helpers. Their SIGNATURES differ between the two files for a real reason —
# report-guards passes a request count and a row total (its subject is the per-temperature
# request contract), fabrication passes the flight JSONL VERBATIM (its subject is a record
# with a key omitted or corrupted). Unifying them would need a builder taking both shapes,
# which is a worse abstraction than two builders. They stay beside the cases that use them.

# perf_csv <path> <cycles> <instructions> — a `perf stat -x,` CSV with both required events.
#
# The two-field-then-event layout matches what `perf stat -x, -e cycles,instructions`
# writes: `<value>,,<event>,,,,`. `read_perf_counters` parses field 0 as the value and
# field 2 as the event name, so a fixture that got the column order wrong would be refused
# for the wrong reason.
perf_csv() {
  printf '%s,,cycles,,,,\n%s,,instructions,,,,\n' "$2" "$3" > "$1"
}

# ws0_make_corpus <dir> [rows] [data_db_bytes] [bytes_per_row] — a COMPLETE, internally
# consistent corpus by default. Callers that need a broken one override the field.
#
# It writes a REAL `ws0/events/nb-1-big-Data.db` of exactly `data_db_bytes` bytes and
# records ITS OWN sha256, because the reporter verifies the recorded identity against the
# bytes actually present (#3272 review B6) — an identity beside no Data.db is refused, so a
# fixture that omitted one would fail every case for the wrong reason.
#
# The digest is MEASURED from the file, never asserted: a fixture that hardcoded one would
# have to be updated whenever the byte count changed, and the update someone forgets is a
# case failing on its fixture rather than on its subject.
ws0_make_corpus() {
  local dir="$1" rows="${2:-1000}" bytes="${3:-700000}" bpr="${4:-}" perf_dir
  mkdir -p "$dir/ws0/events"
  # The shipped modules' directory, passed in so the fixture can call the REAL ticket writer
  # (#3272 M1) rather than hand-rolling the request's shape.
  perf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../perf" && pwd)"
  python3 - "$dir" "$rows" "$bytes" "$bpr" "$perf_dir" <<'PY'
import hashlib, json, os, sys
out, rows, nbytes = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
# An EMPTY 4th argument means "derive bytes_per_row from the two above", which is the
# consistent case. A caller that passes one is deliberately building an INCONSISTENT
# identity, and the reporter must refuse it.
bpr = float(sys.argv[4]) if sys.argv[4] else nbytes / rows
tbl = os.path.join(out, "ws0", "events")
data = os.path.join(tbl, "nb-1-big-Data.db")
raw = (bytes(range(256)) * ((nbytes // 256) + 1))[:nbytes]
open(data, "wb").write(raw)
# The AUXILIARY components too, with their sizes and digests RECORDED, because the reporter
# verifies the COMPLETE recorded set (#3272 F3): a scan reads Index.db and is shaped by the
# Statistics/Summary/Filter components, so verifying Data.db alone left a modified auxiliary
# file able to change measured behaviour under a "corpus verified" claim. Tiny by design —
# the property is per-component identity, not size. NO CompressionInfo.db: this rig's write
# surface is uncompressed-only (#1406).
components = {}
for name, body in (("nb-1-big-Data.db", raw),
                   ("nb-1-big-Index.db", b"IDX" * 16),
                   ("nb-1-big-Statistics.db", b"STAT" * 8),
                   ("nb-1-big-Summary.db", b"SUM" * 8),
                   ("nb-1-big-Filter.db", b"FLT" * 8)):
    path = os.path.join(tbl, name)
    if name != "nb-1-big-Data.db":
        open(path, "wb").write(body)
    components[name] = {"name": name, "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest()}
# THE SCHEMA, written and its digest RECORDED (#3272 R2). `ws0-events.cql` is a MEASUREMENT
# INPUT both arms read, asymmetrically — the bare scan ingests it on every invocation while the
# Flight ticket is generated from it once — so it is verified like any other recorded input.
# A fixture without it is refused, which is the point: an absent schema digest means the schema
# was never pinned.
ddl = b"CREATE TABLE ws0.events (part_id text, seq int, PRIMARY KEY (part_id, seq));\n"
open(os.path.join(out, "ws0-events.cql"), "wb").write(ddl)
# THE FLIGHT TICKET, written through the SHIPPED writer (#3272 round 10, M1). `ticket-template.json`
# IS THE REQUEST every Flight rep re-reads, and the session pin now records its digest — so a corpus
# fixture without one is refused, exactly as a fixture without a schema is. Written by
# `ws0_ticket_input.write_ticket_template` rather than composed here, for the reason
# `ws0_pin_session_corpus` calls the real pin writer: a fixture that hand-rolled the ticket's shape
# would keep passing after the shipped writer's shape changed.
import pathlib
sys.path.insert(0, sys.argv[5])
from ws0_ticket_input import write_ticket_template  # noqa: E402
write_ticket_template(pathlib.Path(out), pathlib.Path(out) / "ws0-events.cql")
json.dump(
    {"rows": rows, "partitions": 10, "seed": 1, "cells_per_row": 12,
     "data_db_bytes": nbytes, "data_db_sha256": hashlib.sha256(raw).hexdigest(),
     "bytes_per_row": bpr, "components": components,
     "schema_sha256": hashlib.sha256(ddl).hexdigest()},
    open(os.path.join(out, "corpus-identity.json"), "w"),
)
PY
}

# make_round <dir> <tag> <round> <position> [arms] [monotonic-ns] — the per-rep ROUND
# METADATA every rep must carry: the four RECORDED fields the reporter requires.
#
# The reporter derives NO ordering property from them (the interleaving claim was deleted in
# #3272 round 4; #3287/#3299 own re-adding an observed control). It uses `round` to pair the
# per-round comparison, INTEGRITY-CHECKS the fields against each other and against the other
# arms, and records them verbatim.
#
# The default instant is `round * 1e9 + position * 1e6`: distinct, and non-contradictory with
# the round labels — the shape a sequential rounds-outside loop produces. A case that needs
# labels CONTRADICTING the instants overrides it, and must then be REFUSED.
make_round() {
  local rnd="$3" pos="$4" arms="${5:-2}"
  local ns="${6:-$(( rnd * 1000000000 + pos * 1000000 ))}"
  printf 'round=%s\nposition=%s\narms_in_round=%s\nmonotonic_ns=%s\n' \
    "$rnd" "$pos" "$arms" "$ns" > "$1/$2.round"
}

# ws0_pin_session_corpus <session-dir> <corpus-dir> — stamp the PRE-MEASUREMENT corpus pin
# the reporter REQUIRES (#3272 review round 4), by calling the SHIPPED writer.
#
# Called through `ws0_validate.write_session_corpus_pin` rather than by writing the JSON here,
# and that is the point: a fixture that hand-rolled the pin's shape would keep passing after the
# real writer's shape changed — the drift `make_round` already demonstrated in round 3. The
# driver calls the same function.
# The CONFIG is stamped with it (#3272 F1): the reporter READS reps/temps/arms/scan-passes
# and the CPU pins from the manifest rather than taking them as arguments, so a fixture that
# omitted them would be refused for a reason unrelated to its subject. Defaults match the
# one-warm-rep/bypass session the suites build; a case whose subject IS the configuration
# overrides them positionally.
#
# ws0_pin_session_corpus <session> <corpus> [reps] [temps] [arms] [scan_passes]
ws0_pin_session_corpus() {
  local session="$1" corpus="$2" perf_dir
  # `${N-default}`, NOT `${N:-default}`: the colon form substitutes the default for an EMPTY
  # value as well as an unset one, which would silently turn a case's deliberately-empty
  # `temps`/`arms` into the healthy default and make the empty-selection guard untestable.
  # Measured: the "an EMPTY temps is REFUSED" case reported exit 0 with a full report, because
  # its empty string had become `warm` in here. An absent argument takes the default; a
  # supplied-but-empty one is passed THROUGH, so the reader refuses it.
  local reps="${3-1}" temps="${4-warm}" arms="${5-bypass}" passes="${6-1}"
  perf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../perf" && pwd)"
  python3 -c '
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import Invalid, load_corpus_identity
from ws0_session import session_pin_path, write_session_corpus_pin
config = {"reps": sys.argv[4], "temps": sys.argv[5], "arms": sys.argv[6],
          "scan_passes": sys.argv[7], "server_cpus": "2,10", "client_cpus": "4,12",
          "step_duration": "45s/1s"}
session, corpus = pathlib.Path(sys.argv[3]), pathlib.Path(sys.argv[2])
try:
    write_session_corpus_pin(session, corpus, load_corpus_identity(corpus), config)
except Invalid:
    # The case DELIBERATELY broke the corpus identity (absent, incomplete, inconsistent) and
    # asserts on the CORPUS refusal. Stamp a config-only manifest so the reporter reaches that
    # refusal — which it validates BEFORE the manifest — instead of dying here on the fixture.
    # A pin with no corpus fields cannot mask anything: `verify_session_corpus_pin` refuses an
    # incomplete pin, so any case that got PAST the corpus check still meets a real check.
    session.mkdir(parents=True, exist_ok=True)
    session_pin_path(session).write_text(json.dumps({"config": config}, indent=1) + "\n")
' "$perf_dir" "$corpus" "$session" "$reps" "$temps" "$arms" "$passes"
  # ...and the PINNING VERIFICATION the driver records beside the manifest (#3272 round 9, F6).
  # Stamped with the SAME CPU lists the manifest above carries, because that agreement is the
  # property the reporter asserts: the report may print "verified" only about lists a verification
  # was actually performed against. A case whose SUBJECT is the record (absent, or disagreeing
  # with a tampered manifest) removes or rewrites it explicitly — this is the healthy default, so
  # every OTHER case reaches its own subject instead of dying on an absent record here.
  ws0_pin_verification "$session" "2,10" "4,12"
  # ...and the BINARY PROVENANCE the driver records before the first rep (#3272 round 10, M2). The
  # reporter REQUIRES it, so every OTHER case would die here rather than reaching its own subject.
  # Stamped with the healthy default; a case whose SUBJECT is the record (absent, incomplete,
  # tampered) removes or rewrites it explicitly.
  ws0_pin_binaries "$session"
}

# ws0_pin_binaries <session-dir> [build-mode] — the driver's record of WHICH BINARIES it measured
# (#3272 round 10, M2).
#
# Separate from `ws0_pin_session_corpus` so a case can stamp a record that is absent, incomplete or
# describes different programs without rebuilding the whole manifest.
#
# It does NOT call the shipped `record_binary_provenance`, and that is the one place in this file
# where hand-writing the shape is the RIGHT call rather than the drift hazard `make_round` warns
# about: the real writer OBSERVES `target/release` binaries and runs `git` in the repo, which is
# host-dependent and (under `--no-build`) may legitimately find nothing — a fixture that depended on
# either would fail on the machine rather than on its subject. The completeness contract is asserted
# instead: `test_ws0_provenance_guards.sh` requires this fixture's key set to equal the shipped
# `PROVENANCE_FIELDS`, so the shape cannot drift silently.
ws0_pin_binaries() {
  local session="$1" mode="${2-built}" perf_dir
  # DOES NOT CREATE the session dir — the same load-bearing rule `ws0_pin_verification` records, and
  # MEASURED here for the second time: a `mkdir(parents=True)` in this function brought a
  # deliberately-NONEXISTENT `--dir` into existence and turned test_ws0_report_guards.sh's
  # "not an existing directory" refusal into a missing-PIN one. A fixture must never manufacture the
  # condition a case is testing the absence of. Absent dir => nothing to stamp; the case's own
  # subject then fires.
  [[ -d "$session" ]] || return 0
  perf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../perf" && pwd)"
  python3 -c '
import hashlib, json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_binaries import MEASURED_BINARIES, provenance_path
session, mode = pathlib.Path(sys.argv[2]), sys.argv[3]
rev = "1" * 40
rec = {
    "source_revision": rev,
    "source_revision_short": rev[:12],
    "source_dirty": False,
    "source_dirty_paths": 0,
    "build_mode": mode,
    "binaries": {
        name: {"path": f"/nonexistent/target/release/{name}",
               "sha256": hashlib.sha256(name.encode()).hexdigest(),
               "bytes": 1024 + i, "mtime_epoch": 2000000000}
        for i, name in enumerate(MEASURED_BINARIES)
    },
    # NO APOSTROPHE anywhere in this heredoc-free `python3 -c ...` body: it is inside SHELL SINGLE
    # QUOTES, so one `'"'"'` would terminate the string and silently truncate this whole library —
    # which is what happened while writing it, and it presented as every OTHER case in the suite
    # failing on an absent pinning-verification.json.
    "provenance": "a TEST FIXTURE record, shaped like the record the driver writes"
                  " (see ws0_pin_binaries for why the shape is asserted, not shared)",
}
provenance_path(session).write_text(json.dumps(rec, indent=1) + "\n")
' "$perf_dir" "$session" "$mode"
}

# ws0_pin_verification <session-dir> <server-cpus> <client-cpus> — the driver's recorded sibling
# verification (#3272 round 9, F6).
#
# Separate from `ws0_pin_session_corpus` so a case can stamp a record that DISAGREES with the
# manifest (the substitution the reporter must refuse) without rebuilding the whole manifest.
ws0_pin_verification() {
  local session="$1" server="$2" client="$3" perf_dir
  # DOES NOT CREATE the session dir, and that is load-bearing rather than tidiness: an earlier
  # draft used `mkdir(parents=True)`, which brought a deliberately-NONEXISTENT `--dir` into
  # existence and turned the reporter's "not an existing directory" refusal into a
  # missing-manifest one. A fixture must never manufacture the condition a case is testing the
  # absence of. Absent dir => nothing to stamp; the case's own subject then fires.
  [[ -d "$session" ]] || return 0
  perf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../perf" && pwd)"
  python3 -c '
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_pinning import pinning_record_path
session, server, client = pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4]
# The expanded form mirrors `verify_sibling_pair`s real output line, so the reporter parses the
# same shape a driver produces rather than a fixture-only spelling.
rec = {
    "server_cpus": server,
    "client_cpus": client,
    "server_siblings_expanded":
        f"server CPUs: {server} -> verified siblings of one physical core"
        f" ({server.replace(chr(44), chr(32))})",
    "topology_root": "/sys/devices/system/cpu",
    "host": "fixture-host",
    "verified_by": "scripts/perf/lib-cpu.sh verify_sibling_pair + verify_disjoint, fail-closed,"
                   " against the real thread_siblings_list BEFORE the first rep",
    "provenance": "written BY THE DRIVER that performed the verification (synthetic fixture)",
}
pinning_record_path(session).write_text(json.dumps(rec, indent=1) + "\n")
' "$perf_dir" "$session" "$server" "$client"
}

# ws0_alternating_position <rep> <which> — the position an arm holds in `<rep>`, matching
# the driver's alternation: the bare scan leads odd rounds, the Flight arm leads even ones.
#
# Factored out because BOTH files computed it inline, in two spellings, and a fixture whose
# positions do not alternate is refused by the rotation check — correctly, but diagnosed as
# a rotation failure rather than as a fixture mistake.
ws0_alternating_position() {
  local rep="$1" which="$2"
  if [[ "$which" == "scan" ]]; then
    (( rep % 2 == 1 )) && echo 1 || echo 2
  else
    (( rep % 2 == 1 )) && echo 2 || echo 1
  fi
}

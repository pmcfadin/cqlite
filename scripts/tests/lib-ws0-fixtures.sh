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

# THE FIXTURE SESSION'S PINNED FLIGHT ENDPOINT (#3272 round 14, F2).
#
# ONE constant, because it is ONE pinned fact stated in two artifacts a healthy session must agree
# on: the manifest's `config.flight_endpoint` (stamped by `ws0_pin_session_corpus`) and every step
# record's `endpoint` (substituted into the record bodies by the two suites' `make_flight_rep`, the
# same way `__TAG__` binds `round`). The reporter compares them EXACTLY, so two spellings of the
# default would refuse every healthy case in every suite for a reason unrelated to its subject —
# which is precisely how `round`'s constant `"round":"r"` bodies behaved when F1 landed.
#
# A case whose SUBJECT is a record from ANOTHER SERVER overrides ONE side: it writes a different
# `endpoint` into the record (or pins a different endpoint in the manifest) and asserts the refusal.
# The port is deliberately NOT the driver's default 18815 — nothing here runs a server, and a
# fixture constant that looked like the real one invites reading a passing case as evidence about a
# real session.
WS0_FIXTURE_ENDPOINT="http://127.0.0.1:19999"

# The FIXED SCAN CONTRACT every healthy bare-scan artifact carries (#3272). The reporter requires
# `arm`, `surface`, `query` and `fold` to hold the values this rig fixes them to, because a scan run
# under a different `--fold` or `--project` has entirely VALID counters — the right pass count, every
# pass observing exactly the pinned corpus row count, aggregates equal to the derived sums — while
# measuring materially different work, and was published as the arm the whole ratio is divided by.
#
# Spelled ONCE here, at the healthy values, for the reason `WS0_FIXTURE_ENDPOINT` above is: three
# suites and ~55 fixture sites write these payloads, and a second spelling would refuse every
# healthy case in one of them for a reason unrelated to its subject. A case whose SUBJECT is a wrong
# value writes it explicitly (a folded run, a narrowed projection, the setup-only arm) and is then
# refused — which is the guard working.
WS0_SCAN_FIXED='"arm":"bare_scan","surface":"cqlite_core::Database::execute_streaming","query":"SELECT * FROM ws0.events","fold":false'

# The PINNED cells/row every fixture corpus records, and the multiplier the reporter now requires
# each pass's `cells` counter to satisfy (#3272 round 17): a pass must emit
# `corpus_rows x WS0_CELLS_PER_ROW`, because the row count says how many rows a pass VISITED and
# says nothing about how many COLUMNS of each it decoded — so a scan returning every row with
# MISSING COLUMNS did substantially less work and was published as this arm's figure.
#
# Spelled here beside `ws0_make_corpus`'s identity literal, and asserted against it below, so the
# two cannot drift: a fixture whose corpus pins 12 while its passes are built for 9 would refuse
# every healthy case for a reason unrelated to its subject.
WS0_CELLS_PER_ROW=12

# The ARROW PAYLOAD VOLUME one full-corpus scan carries, in the fixture corpora (#3272 round 17).
#
# The Flight arm's counterpart of `WS0_CELLS_PER_ROW` above, and it closes the same class on the
# other arm: the row count says how many rows a RESPONSE carried and says nothing about how many
# ARROW COLUMNS of each it encoded, so a response with every row and MISSING COLUMNS made Arrow
# encoding look CHEAPER — the one quantity #3096 exists to measure — while satisfying every
# request/row/rate/counter check.
#
# The reporter derives the expectation from the session's UNTIMED PREFLIGHT, so a healthy fixture
# must write one that AGREES with its timed bodies. Both are built from this constant: a
# per-scan volume, multiplied by the rep's `requests_ok`. The value is arbitrary in magnitude and
# deliberately NOT round — a figure like 1000000 could be produced by a coincidence of two
# unrelated multiplications, and this one cannot.
WS0_PREFLIGHT_BYTES_PER_SCAN=87654321

# `ws0_flight_bytes <requests_ok>` — the `bytes_total` a HEALTHY rep serving that many full-corpus
# scans carries. A case whose subject is a SHORT PAYLOAD writes its own (smaller) value literally
# and is then refused, which is the guard working.
ws0_flight_bytes() { echo $(( $1 * WS0_PREFLIGHT_BYTES_PER_SCAN )); }

# `ws0_make_preflight <dir> <tag> <bytes-per-scan> [requests_ok]` — the UNTIMED PREFLIGHT artifact
# the driver's prewarm leg retains at `<tag>.prewarm.jsonl`, and the reporter's sole oracle for the
# expected Arrow payload volume.
#
# `requests_ok` defaults to 3, NOT 1, and that is the point of having it: the preflight runs a fixed
# 20s step, so its request count is whatever fits and bears no relation to a timed rep's. The
# reporter must therefore divide to get a PER-SCAN figure — a fixture that always wrote 1 would let
# a reporter that compared the totals whole pass every healthy case.
ws0_make_preflight() {
  local d="$1" tag="$2" per_scan="$3" ok="${4:-3}"
  printf '%s\n' "{\"schema\":\"flight-loadgen.step/v1\",\"round\":\"prewarm\",\"requests_ok\":$ok,\"requests_error\":0,\"error_codes\":{},\"requests_unavailable\":0,\"rows_total\":$(( ok * ${CORPUS_ROWS:-1000} )),\"bytes_total\":$(( ok * per_scan ))}" \
    > "$d/$tag.prewarm.jsonl"
}
# `ws0_scan_pass_cells <rows>` — the cell count a HEALTHY pass over `<rows>` rows emits. A case
# whose subject is a THINNER scan writes its own (short) value literally and is then refused.
ws0_scan_pass_cells() { echo $(( $1 * WS0_CELLS_PER_ROW )); }

# ...and the SESSION-BOUND half (#3272): `corpus`, `schema` and `table_dirs_ingested`, which the
# reporter compares against the corpus THIS SESSION PINNED. Unlike the four above they are NOT
# constants — they name the fixture corpus a case built — so they take the corpus path as an
# argument and every scan-rep builder must be told which corpus its session measures.
#
# `__CORPUS__` is the substitution a case uses to write a DIFFERENT corpus (a peer lane's, a second
# copy) while keeping the rest of the record healthy — the arrangement `__ENDPOINT__` uses in the
# flight bodies, for the same reason: a case whose subject is a substituted corpus must be able to
# write the input the guard refuses without restating the other six fields.
ws0_scan_session_bound() { # ws0_scan_session_bound <corpus-path>
  printf '"corpus":"%s","schema":"%s/ws0-events.cql","table_dirs_ingested":["%s/ws0/events"]' \
    "$1" "$1" "$1"
}

# perf_csv <path> <cycles> <instructions> — a `perf stat -x,` CSV with both required events.
#
# The layout matches what `perf stat -x, -e cycles,instructions` really writes:
#   `<value>,<unit>,<event>,<enabled_ns>,<enabled_pct>,<derived>,<derived_unit>`
# `read_perf_counters` parses field 0 as the value, field 2 as the event name and field 4
# as the enabled-percentage, so a fixture with the column order wrong would be refused for
# the wrong reason.
#
# FIELD 4 CARRIES A REAL `100.00` (#3248), and that is load-bearing rather than cosmetic.
# This helper used to emit `<value>,,<event>,,,,` with fields 3-6 EMPTY, which was fine
# while nothing read them — and it is precisely how the multiplexing gap survived: the
# parser never read field 4, the fixtures never supplied it, so there was no way for
# either side to notice the column was unused. `read_perf_counters` now refuses a count
# whose enabled-percentage is absent or unparseable (an unverifiable count is not a usable
# one), so a fixture omitting it is refused — correctly. Supplying a realistic value keeps
# the fixtures a model of real perf output instead of a model of what the parser happened
# to read.
perf_csv() {
  printf '%s,,cycles,1000000000,100.00,1.000,GHz\n%s,,instructions,1000000000,100.00,2.000,insn per cycle\n' "$2" "$3" > "$1"
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
  # The pinned cells/row (#3272 round 17) comes from the ONE constant above rather than a literal in
  # the python body, so the corpus a fixture builds and the pass cell counts `ws0_scan_pass_cells`
  # computes cannot disagree. A case whose subject is a corpus pinning a DIFFERENT cells/row passes
  # its own 5th argument — the same arrangement `bytes_per_row` uses for an inconsistent identity.
  local cpr="${5:-$WS0_CELLS_PER_ROW}"
  mkdir -p "$dir/ws0/events"
  # The shipped modules' directory. It no longer writes the ticket (round 13's F2 moved that into
  # the SESSION dir — see `ws0_pin_session_corpus`), but the argument is kept so the python body
  # keeps one argv shape across both call sites of this library.
  perf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../perf" && pwd)"
  python3 - "$dir" "$rows" "$bytes" "$bpr" "$perf_dir" "$cpr" <<'PY'
import hashlib, json, os, sys
out, rows, nbytes = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
# The pinned cells/row, PASSED IN rather than a literal here (#3272 round 17): the reporter requires
# every pass to have emitted `rows x cells_per_row` cells, so this value and the fixtures' pass
# counters are the same fact and are written from one constant.
#
# Parsed with `json.loads`, not `int()`: a case whose subject is a corpus pinning a FRACTIONAL or
# otherwise unusable cells/row must be able to write exactly that, and `int()` would TRUNCATE it onto
# a valid one — the coercion defect round 12's F5 found, reintroduced in a fixture.
cells_per_row = json.loads(sys.argv[6])
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
# NO FLIGHT TICKET IS WRITTEN HERE ANY MORE (#3272 round 13, F2). The ticket is a property of the
# SESSION, not of the corpus: it now lives in the session's exclusively-claimed output directory,
# because two lanes measuring ONE corpus used to overwrite each other's request between the pin and
# the reps, and requiring a write into the corpus meant an otherwise immutable artifact could not be
# read-only. `ws0_pin_session_corpus` writes it into the session dir through the shipped writer.
#
# THIS FIXTURE THEREFORE LEAVES THE CORPUS WRITE-FREE, which is the property
# `test_ws0_provenance_guards.sh` asserts by chmod'ing a corpus read-only and running a session
# over it.
json.dump(
    {"rows": rows, "partitions": 10, "seed": 1, "cells_per_row": cells_per_row,
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
# ...and the BASELINE MODE plus the canonical COMPARISON (#3272 round 13, F3): the reporter
# REQUIRES both, and `baseline_mode` is a declared manifest field, so a fixture that omitted them
# would make every OTHER case die on an incomplete manifest instead of reaching its own subject.
# The default is `non-baseline`, and that is the only honest default available: these fixtures build
# a ~few-KB synthetic corpus, which is NOT the canonical measurement corpus, and the shipped
# `require_canonical_or_declared` would REFUSE it in `baseline` mode — correctly, since that is the
# finding F3 closes. A case whose SUBJECT is the mode/comparison overrides it positionally, and a
# case wanting a BASELINE-mode manifest must build a canonical-shaped identity for it.
#
#
# ...and the FLIGHT ENDPOINT (#3272 round 14, F2): the manifest pins WHICH SERVER produced the rows
# and the reporter compares it EXACTLY against every rep's recorded `endpoint`, so the default here
# must equal the one `WS0_FIXTURE_ENDPOINT` puts in the record bodies — the two are the same pinned
# fact and a fixture that spelled them differently would refuse every healthy case. A case whose
# SUBJECT is a record from ANOTHER server overrides one side positionally, and is then refused, which
# is the guard working.
#
# ws0_pin_session_corpus <session> <corpus> [reps] [temps] [arms] [scan_passes] [baseline_mode]
#                        [flight_endpoint]
ws0_pin_session_corpus() {
  local session="$1" corpus="$2" perf_dir
  # `${N-default}`, NOT `${N:-default}`: the colon form substitutes the default for an EMPTY
  # value as well as an unset one, which would silently turn a case's deliberately-empty
  # `temps`/`arms` into the healthy default and make the empty-selection guard untestable.
  # Measured: the "an EMPTY temps is REFUSED" case reported exit 0 with a full report, because
  # its empty string had become `warm` in here. An absent argument takes the default; a
  # supplied-but-empty one is passed THROUGH, so the reader refuses it.
  local reps="${3-1}" temps="${4-warm}" arms="${5-bypass}" passes="${6-1}"
  local baseline_mode="${7-non-baseline}"
  local flight_endpoint="${8-$WS0_FIXTURE_ENDPOINT}"
  perf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../perf" && pwd)"
  python3 -c '
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import Invalid, load_corpus_identity
from ws0_session import session_pin_path, write_session_corpus_pin
from ws0_ticket_input import write_ticket_template
from ws0_canonical_corpus import require_canonical_or_declared
# The repo root, for the canonical pin the oracle parses. Derived from the shipped perf dir rather
# than from $PWD, because these suites run from anywhere.
#
# NO APOSTROPHE in this comment either, for the reason stated further down: this body is inside
# SHELL SINGLE QUOTES, so one terminates the string and truncates the library. MEASURED again while
# writing this line (a possessive in the word module), and it presented as a bash syntax error on an
# unrelated python line.
repo_root = pathlib.Path(sys.argv[1]).resolve().parent.parent
# `events` and `bin_dir` are declared manifest fields since #3248, and the reader refuses an
# INCOMPLETE config, so a fixture omitting them fails every downstream case with a
# manifest-incompleteness error rather than the condition under test. Measured while adding
# them: 211 checks across five suites went red at once, all with the same wrong diagnosis.
# They are NOT parameterised here because no case varies them; a case that needs to can edit
# the pin it just wrote, which is how the other single-value fields are exercised too.
config = {"reps": sys.argv[4], "temps": sys.argv[5], "arms": sys.argv[6],
          "scan_passes": sys.argv[7], "server_cpus": "2,10", "client_cpus": "4,12",
          # The FLIGHT pin (#3551), a declared manifest field the reader REFUSES to do without.
          # EQUAL to server_cpus here, which is the default every run without
          # --flight-server-cpus produces, and it must equal what ws0_pin_verification stamps
          # below: that agreement is what the reporter asserts. A case whose SUBJECT is the
          # flight pin (a manifest naming CPUs no verification ran against, or a distinct-cores
          # session) edits one side EXPLICITLY.
          "flight_server_cpus": "2,10",
          "step_duration": "45s/1s", "flight_endpoint": sys.argv[9],
          "events": "cycles,instructions",
          "bin_dir": "/fixture/target/release",
          "profile": "off",
          "quiescence": "NOT VERIFIED (no timeseries supplied)",
          "baseline_mode": sys.argv[8]}
session, corpus = pathlib.Path(sys.argv[3]), pathlib.Path(sys.argv[2])
# NOTHING IS STAMPED FOR A SESSION DIR THAT DOES NOT EXIST — the same load-bearing rule
# `ws0_pin_binaries` and `ws0_pin_verification` state as an early `return 0`, now stated here too
# because round 13 F2 gave this function a reason to reach the fallback below on a healthy corpus.
# MEASURED: with the ticket in the session dir, an absent dir makes the ticket digest Invalid, the
# fallback fired, its `mkdir(parents=True)` CREATED the dir, and the report guards
# not-an-existing-directory refusal became an incomplete-pin one. A fixture must never manufacture
# the condition a case is testing the absence of.
if not session.is_dir():
    raise SystemExit(0)
# THE FLIGHT TICKET, into the SESSION dir, through the SHIPPED writer (#3272 round 10 M1, moved by
# round 13 F2). `ticket-template.json` IS THE REQUEST every Flight rep re-reads and the pin records
# its digest, so it must exist BEFORE the pin — the same ordering the driver has. Written by
# `ws0_ticket_input.write_ticket_template` rather than composed here for the reason this function
# calls the real pin writer: a fixture that hand-rolled the ticket shape would keep passing after
# the shipped writer changed. A case whose SUBJECT is an absent/mutated ticket removes or rewrites
# it explicitly, which is now possible without touching the corpus at all.
#
# DOES NOT CREATE the session dir — the load-bearing rule `ws0_pin_binaries` and
# `ws0_pin_verification` both record, and MEASURED here for the THIRD time: a `mkdir(parents=True)`
# here brought a deliberately-NONEXISTENT `--dir` into existence and turned the report guards
# "not an existing directory" refusal into a missing-reps one. A fixture must never manufacture the
# condition a case is testing the absence of.
#
# NO APOSTROPHE in this comment, and that is not style: this body is inside SHELL SINGLE QUOTES, so
# one would terminate the string and truncate the whole library — MEASURED while writing this, and it
# presented as 40+ unrelated cases failing on an absent session pin.
try:
    if session.is_dir() and (corpus / "ws0-events.cql").is_file():
        write_ticket_template(session, corpus / "ws0-events.cql")
except Invalid:
    # A case deliberately broke the DDL and asserts on the SCHEMA refusal; the pin below then
    # refuses on its own absent-ticket path, which is a real check rather than a fixture crash.
    pass
try:
    identity = load_corpus_identity(corpus)
    # THE CANONICAL COMPARISON, through the SHIPPED oracle (#3272 round 13, F3) — never a
    # hand-written record, for the same reason this function calls the real pin writer: a fixture
    # that composed the block itself would keep passing after the shipped shape changed. Over a
    # synthetic corpus in `non-baseline` mode this returns a record whose divergences are real.
    canonical = require_canonical_or_declared(repo_root, identity, config["baseline_mode"], corpus)
    write_session_corpus_pin(session, corpus, identity, config, canonical)
except Invalid:
    # The case DELIBERATELY broke the corpus identity (absent, incomplete, inconsistent) and
    # asserts on the CORPUS refusal. Stamp a config-only manifest so the reporter reaches that
    # refusal — which it validates BEFORE the manifest — instead of dying here on the fixture.
    # A pin with no corpus fields cannot mask anything: `verify_session_corpus_pin` refuses an
    # incomplete pin, so any case that got PAST the corpus check still meets a real check.
    session.mkdir(parents=True, exist_ok=True)
    # The canonical block too, so a case that got past the corpus check meets the REAL canonical
    # reader rather than dying on an absent block for a reason unrelated to its subject. Built by
    # the shipped oracle over a MINIMAL identity, so it is a real record; if even that is
    # impossible the block is omitted and the reader refuses it, which is a real check.
    fallback = {"config": config}
    try:
        fallback["canonical_corpus"] = require_canonical_or_declared(
            repo_root, {}, config["baseline_mode"], corpus
        )
    except Invalid:
        pass
    session_pin_path(session).write_text(json.dumps(fallback, indent=1) + "\n")
' "$perf_dir" "$corpus" "$session" "$reps" "$temps" "$arms" "$passes" "$baseline_mode" \
    "$flight_endpoint"
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
  # ...and the MEASUREMENT-BOUNDARY record the driver appends per boundary (#3272 round 22), which
  # the reporter now READS and requires to be COMPLETE. Derived from THIS call's configuration, so
  # the record covers exactly the boundaries this manifest's reps/temps/arms owe.
  ws0_pin_boundary_observations "$session" "$reps" "$temps" "$arms"
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
from ws0_binaries import MEASURED_BINARIES, REVISION_UNKNOWN, provenance_path
from ws0_binary_spec import frozen_relpath
session, mode = pathlib.Path(sys.argv[2]), sys.argv[3]
# The recorded PATHS must be the RELATIVE `measured-bin/<name>` the reader RECONSTRUCTS from the
# session dir and the binary key (#3272 F3, tightening F2): a path merely sitting under a directory
# NAMED measured-bin was another session copy or another program copy, and the reader could not tell.
# Asked of the shipped `frozen_relpath` rather than spelled again here, so the fixture cannot drift
# from the reader own definition of a frozen path.
#
# NOT created on disk here — the frozen copies are optional at report time (a results dir is
# archived without its release binaries), so their absence is the DEFAULT the reader must handle, and
# a fixture that materialised fake executables would be claiming a freeze that never happened. A
# case whose subject is the re-derivation writes them explicitly.
rev = "1" * 40
# THE REVISION FIELDS TRACK THE BUILD MODE (#3272 round 12, F1): in `reused` mode the source
# revision is the UNKNOWN sentinel, because --no-build accepted the binaries off the disk and
# nothing establishes which revision built them. The reader checks the two against each other, so a
# fixture pinning a sha beside mode=reused would be refused for that rather than reaching its case.
# `REVISION_UNKNOWN` is IMPORTED, never spelled again here: two spellings of a sentinel is two
# things to keep in step, and the failure mode is a fixture refused for its own typo.
observed = mode == "built"
rec = {
    "source_revision": rev if observed else REVISION_UNKNOWN,
    "source_revision_short": (rev if observed else REVISION_UNKNOWN)[:12],
    "source_revision_observed": observed,
    "checkout_revision_at_measurement": rev,
    "source_dirty": False,
    "source_dirty_paths": 0,
    "build_mode": mode,
    "binaries": {
        name: {"path": frozen_relpath(name),
               "source_path": f"/nonexistent/target/release/{name}",
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
  # THE FLIGHT ARM (#3551), defaulted so every existing caller keeps stamping a healthy record.
  # `${N-default}` and not `${N:-default}` for the reason stated at ws0_pin_session_corpus: a
  # case that deliberately passes an EMPTY value is testing the reader's empty-field refusal, and
  # the colon form would silently hand it the healthy default instead.
  local flight="${4-$server}" mode="${5-siblings}" allocator="${6-system}"
  local allocator_lib="${7-none (system malloc; fixture)}"
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
flight, mode, allocator, allocator_lib = sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8]
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
    # THE FLIGHT ARM (#3551). The expanded forms mirror the real echoes of
    # `verify_sibling_pair` / `verify_distinct_cores`, so the reporter reads the same SHAPE a
    # driver produces rather than a fixture-only spelling — the rule this file already follows
    # for server_siblings_expanded.
    "flight_server_cpus": flight,
    "flight_pin_mode": mode,
    "flight_pin_verified": (
        f"flight server CPUs: {flight} -> verified siblings of one physical core"
        f" ({flight.replace(chr(44), chr(32))})"
        if mode == "siblings" else
        f"flight server CPUs: {flight} -> verified pairwise DISTINCT physical cores"
        f" ({flight.replace(chr(44), chr(32))}); thread_siblings_list read: "
        + " ".join(f"cpu{c}=({c} {int(c) + 8})" for c in flight.split(chr(44)))
    ),
    "flight_allocator": allocator,
    "flight_allocator_lib": allocator_lib,
    "flight_allocator_verification":
        "per rep, AFTER await_server_ready: /proc/<server-pid>/maps is READ (synthetic fixture"
        " record, shaped like the one the driver writes)",
}
pinning_record_path(session).write_text(json.dumps(rec, indent=1) + "\n")
' "$perf_dir" "$session" "$server" "$client" "$flight" "$mode" "$allocator" "$allocator_lib"
}

# ws0_pin_boundary_observations <session-dir> <reps> <temps> <arms> — the driver's MEASUREMENT-BOUNDARY
# record (#3272 round 22), one observation per boundary the configuration owes.
#
# The reporter now READS this record and REQUIRES it to be COMPLETE, so every OTHER case would die
# here on an absent record rather than reaching its own subject. Stamped with the healthy default; a
# case whose SUBJECT is the record (absent, short, duplicated, carrying a foreign boundary) removes
# or rewrites it EXPLICITLY.
#
# The label set is DERIVED by calling the SHIPPED `expected_boundary_labels`, never re-spelled here.
# That is the same rule `ws0_pin_session_corpus` follows in calling the real pin writer: a fixture
# that composed the labels itself would keep passing after the derivation changed, and a divergence
# would present as EVERY BOUNDARY MISSING — a refusal blaming the operator for a fixture defect.
#
# TRUNCATES rather than appends, because `run_report_args` re-pins a session dir in place: appending
# would produce the DUPLICATE the reporter refuses, for a reason unrelated to any case's subject.
ws0_pin_boundary_observations() {
  local session="$1" reps="${2-1}" temps="${3-warm}" arms="${4-bypass}" perf_dir
  # DOES NOT CREATE the session dir — the load-bearing rule `ws0_pin_binaries` and
  # `ws0_pin_verification` both record, for the third time: a `mkdir(parents=True)` here would bring
  # a deliberately-NONEXISTENT `--dir` into existence and turn the reporter's "not an existing
  # directory" refusal into a boundary-record one.
  [[ -d "$session" ]] || return 0
  perf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../perf" && pwd)"
  python3 -c '
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_boundary_observations import expected_boundary_labels
from ws0_corpus_bytes import SESSION_CORPUS_PIN, boundary_observations_path, session_pin_path
session = pathlib.Path(sys.argv[2])
reps, temps, arms = int(sys.argv[3]), sys.argv[4].split(), sys.argv[5].split()
# The component COUNT comes from the pin when there is one, so the observation describes the same
# pinned set a real boundary check would have re-hashed. A session dir with no pin (a case whose
# subject is the absent pin) takes a floor that keeps the record well-formed — that case is refused
# on the PIN, which it must reach rather than dying here.
pinned = 5
p = session_pin_path(session)
if p.exists():
    try:
        comps = json.loads(p.read_text()).get("components")
        if isinstance(comps, dict) and comps:
            pinned = len(comps)
    except ValueError:
        pass
lines = []
for label in expected_boundary_labels(temps, arms, reps):
    lines.append(json.dumps({
        "boundary": label,
        "corpus": str(session),
        "components_verified": pinned,
        "components_pinned": pinned,
        "verified_against": SESSION_CORPUS_PIN,
        "note": f"all {pinned} pinned component(s) were re-stat-ed and re-hashed FROM DISK at this"
                " boundary and compared against the pin (synthetic fixture)",
    }))
boundary_observations_path(session).write_text("\n".join(lines) + "\n")
' "$perf_dir" "$session" "$reps" "$temps" "$arms"
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

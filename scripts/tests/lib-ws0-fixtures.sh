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
  local dir="$1" rows="${2:-1000}" bytes="${3:-700000}" bpr="${4:-}"
  mkdir -p "$dir/ws0/events"
  python3 - "$dir" "$rows" "$bytes" "$bpr" <<'PY'
import hashlib, json, os, sys
out, rows, nbytes = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
# An EMPTY 4th argument means "derive bytes_per_row from the two above", which is the
# consistent case. A caller that passes one is deliberately building an INCONSISTENT
# identity, and the reporter must refuse it.
bpr = float(sys.argv[4]) if sys.argv[4] else nbytes / rows
data = os.path.join(out, "ws0", "events", "nb-1-big-Data.db")
raw = (bytes(range(256)) * ((nbytes // 256) + 1))[:nbytes]
open(data, "wb").write(raw)
json.dump(
    {"rows": rows, "partitions": 10, "seed": 1, "cells_per_row": 12,
     "data_db_bytes": nbytes, "data_db_sha256": hashlib.sha256(raw).hexdigest(),
     "bytes_per_row": bpr},
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

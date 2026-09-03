#!/usr/bin/env bash
# lib-ws0-report-fixtures.sh — the SYNTHETIC SESSION-DIR builders and the `expect_reject`
# helper shared by the two reporter self-test suites (issue #3272 review round 4,
# campsite-rule split).
#
# Sourced, not executed, and it sets NO shell options: `set -uo pipefail` in a library mutates
# the SOURCING shell's options, which is the caller's decision.
#
# # Why this exists, and where the seam falls
#
# `test_ws0_fabrication_guards.sh` grew past the ~1500-line test target while closing round 4's
# derived-throughput finding, so it was split by SUBJECT:
#
#   test_ws0_fabrication_guards.sh   — no fabricated VALUE anywhere in the reporting path
#                                      (defaulted counters, permissive verdicts, truncating
#                                      coercions, a forged throughput, the corpus-byte identity)
#   test_ws0_round_metadata.sh       — the per-rep ROUND METADATA: the loop order, the required
#                                      fields, the integrity refusals, and the assertion that
#                                      NO interleaving/ordering claim is made on any session
#                                      shape (the round-4 deletion)
#
# The two need the SAME fixture builders, and a duplicated builder is the wrong thing to keep
# two copies of: `make_round` gaining a `monotonic_ns` field had to be edited in two files in
# round 3, which is exactly the drift this removes. `perf_csv`/`ws0_make_corpus`/`make_round`
# come from `lib-ws0-fixtures.sh` (shared with `test_ws0_report_guards.sh`); this file adds the
# builders whose signatures are specific to the two reporter suites — the flight JSONL is
# passed VERBATIM, so a case can omit a key or supply two records.
#
# It expects the sourcing suite to have set `REPO_ROOT`, `REPORT`, `TMP`, `CORPUS_ROWS`, and to
# provide `pass`/`fail`.

# shellcheck source=scripts/tests/lib-ws0-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-fixtures.sh"

# This file's corpora are deliberately SMALL (4 KiB of Data.db). The byte verification must
# work at ANY size — the real corpus is 2.8 GB and a test may not write one — and this file
# builds a corpus per case, so the size is the one thing worth keeping local.
FIXTURE_DATA_DB_BYTES=4096
make_corpus() { ws0_make_corpus "$1" "${2:-$CORPUS_ROWS}" "$FIXTURE_DATA_DB_BYTES" "${3:-}"; }

make_scan_rep() { # make_scan_rep <dir> <temp> <rep> <prewarm>
  # `WS0_SCAN_FIXED` (the fixed scan contract, #3272) comes from `lib-ws0-fixtures.sh`, sourced
  # above — one spelling for all three suites, for the reason stated there. The session-bound half
  # names `WS0_SCAN_CORPUS` (the corpus these fixtures pin), which the same file defaults.
  make_scan_rep_fields "$1" "$2" "$3" "$4" \
    "$WS0_SCAN_FIXED,$(ws0_scan_session_bound "${WS0_SCAN_CORPUS:-$TMP/corpus}")"
}

# make_scan_rep_fields <dir> <temp> <rep> <prewarm> <contract-json-fragment>
#
# `make_scan_rep` with the whole seven-field contract fragment given VERBATIM, so a case can supply
# a WRONG value (a folded run, a narrowed projection, the setup-only arm, another session's corpus)
# or omit a field entirely. Same arrangement as `make_flight_rep`'s verbatim JSONL body, and for the
# same reason: the cases whose subject IS the contract must be able to write the input the guard
# refuses.
make_scan_rep_fields() {
  local d="$1" tag="scan-$2-$3" fixed="$5"
  cat > "$d/$tag.json" <<EOF
{ $fixed,
  "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5,
  "passes": [ { "pass": 0, "rows": $CORPUS_ROWS, "cells": $(ws0_scan_pass_cells "$CORPUS_ROWS"), "secs": 2.0 } ] }
EOF
  perf_csv "$d/perf-$tag.csv" 2000000 4000000
  perf_csv "$d/perf-$tag-setup.csv" 100000 200000
  printf '%s\n' "$4" > "$d/$tag.prewarm.status"
  # The driver's alternation, from the shared helper so the two files cannot spell it
  # differently (a fixture whose positions do not alternate is refused by the rotation
  # check — correctly, but diagnosed as a rotation failure rather than a fixture mistake).
  make_round "$d" "$tag" "$3" "$(ws0_alternating_position "$3" scan)"
}

# make_flight_rep <dir> <temp> <rep> <prewarm> <jsonl-body>
# The JSONL body is given VERBATIM so a case can omit a key or supply two records — with TWO
# substitutions: `__TAG__` becomes this rep's real tag (#3272 round 14, F1) and `__ENDPOINT__`
# becomes the session's pinned endpoint (F2).
#
# The reporter now REQUIRES `round` to EQUAL the tag the artifact was found under, because a record
# whose round disagrees is a record from another rep sitting in this rep's filename — and
# `perf-<tag>.csv` is located by that filename, so its cycles would be divided by the other rep's
# rows. The fixtures used to carry a constant `"round":"r"` for every rep, which is exactly the
# input the new check refuses; `__TAG__` keeps the bodies readable at ~25 call sites while binding
# each to its own rep. A case whose SUBJECT is a WRONG round writes the wrong value literally, and
# is then refused — which is the guard working.
#
# `__ENDPOINT__` is the same arrangement for `endpoint` (#3272 round 14, F2): the reporter requires
# it to EQUAL the manifest's `config.flight_endpoint`, so the bodies bind to the ONE fixture
# constant rather than each spelling a literal — a case whose subject is a record from ANOTHER
# SERVER writes that endpoint literally, and is refused.
#
# ...and the UNTIMED PREFLIGHT JSONL (`<tag>.prewarm.jsonl`) is written for every WARM rep
# (#3272 round 17), because the driver writes one: `lib-measure.sh`'s prewarm leg retains it
# to that exact path, OUTSIDE the perf window, and the reporter now derives each rep's
# expected ARROW PAYLOAD VOLUME from it. Without it every fixture session would take the
# no-oracle branch and the content-volume check would never fire in any suite — a guard
# present and unexercised, which is the shape this issue keeps finding.
#
# The COLD arm deliberately gets NO preflight, matching the driver: a prewarm there would make
# `cold` meaningless, so a cold-only session legitimately has no oracle and the reporter names
# that gap. A case whose SUBJECT is a SHORT PAYLOAD writes its own preflight/body pair.
make_flight_rep() {
  local d="$1" tag="flight-bypass-$2-$3" body="$5"
  body="${body//__TAG__/$tag}"
  printf '%s\n' "${body//__ENDPOINT__/$WS0_FIXTURE_ENDPOINT}" > "$d/$tag.jsonl"
  perf_csv "$d/perf-$tag.csv" 8000000 16000000
  printf '%s\n' "$4" > "$d/$tag.prewarm.status"
  # ...and the SERVER LOG the reporter reads the admission ceiling back from (#3551 item 10).
  # The reporter REQUIRES it — the ceiling is DERIVED from available_parallelism, which respects
  # the CPU affinity mask, so it moves with --flight-server-cpus and a session whose reps
  # disagree differed in a SECOND property besides the one under test — so every OTHER case would
  # die here rather than reaching its own subject. A case whose SUBJECT is the log (absent, empty,
  # unparseable, disagreeing) removes or rewrites it EXPLICITLY.
  #
  # WRITTEN WITH THE REAL LOG'S ANSI ESCAPES, deliberately: `cqlite-flight` colours the field
  # NAME and puts the reset BETWEEN the name and its `=` (measured on a real smoke log: 88 escape
  # sequences, and a parse of the UNSTRIPPED text matches NOTHING). A plain-text fixture would let
  # every healthy case pass while the shipped parser was keyed on a presentation property — the
  # #3400 class — so the healthy path here exercises the strip.
  ws0_write_server_log "$d/$tag.server.log" 4 derived 2
  [ "$2" != "warm" ] || ws0_make_preflight "$d" "$tag" "$WS0_PREFLIGHT_BYTES_PER_SCAN"
  # ...and the flight arm takes the OTHER position, mirroring the driver.
  make_round "$d" "$tag" "$3" "$(ws0_alternating_position "$3" flight)"
}

# `requests_unavailable` is carried at its HEALTHY value because the reporter now REQUIRES
# it (#3272 F4): the admission-shed counter was completely unread, so a rep whose requests
# the server SHED was reported as failure-free. A fixture omitting it is refused — which is
# the guard working, and is why the cases whose subject IS the omission set it explicitly.
GOOD_FLIGHT='{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"__TAG__","endpoint":"__ENDPOINT__","requests_ok":1,"requests_error":0,"error_codes":{},"requests_unavailable":0,"rows_total":1000,"bytes_total":'"$WS0_PREFLIGHT_BYTES_PER_SCAN"',"rows_per_s":250.0,"duration_s":4.0}'

# make_session <dir> <flight-jsonl> — a complete one-warm-rep session dir.
make_session() {
  mkdir -p "$1"
  make_scan_rep "$1" warm 1 ok
  make_flight_rep "$1" warm 1 ok "$2"
}

# run_report <dir> <corpus> [extra args…]
#
# The PRE-MEASUREMENT corpus pin (#3272 review round 4) is stamped here IF THE SESSION DIR DOES
# NOT ALREADY CARRY ONE, standing in for the driver, which writes it before the first rep. Two
# reasons for the "if absent" rather than an unconditional stamp:
#
#   * ~34 case sites build a session dir ad hoc; requiring each to stamp would put the same line
#     in 34 places, and the one someone forgets fails for a reason unrelated to its subject.
#   * the cases whose SUBJECT is the pin — an absent one, a stale one, a corpus swapped between
#     the pin and the report — set it up (or remove it) EXPLICITLY, and the "if absent" test is
#     what lets them: nothing here overwrites a pin a case deliberately wrote.
run_report() { # run_report <dir> <corpus> [extra args…]
  local d="$1" c="$2"; shift 2
  [[ -e "$d/session-corpus-pin.json" ]] || ws0_pin_session_corpus "$d" "$c"
  # ONLY the two paths: reps/temps/arms/scan-passes and the CPU pins are READ FROM the
  # session manifest since #3272 F1, so passing them here would be an argparse error.
  python3 "$REPORT" --dir "$d" --corpus "$c" "$@" 2>&1
}

# run_report_args <dir> <corpus> <args…> — the reporter with the caller's OWN argument list,
# with the PRE-MEASUREMENT corpus pin stamped if absent.
#
# Exists because ~9 cases need a `--reps`/`--temps`/`--arms` combination `run_report` does not
# offer and were calling `python3 "$REPORT" …` directly. Every such site is a place the pin
# would have to be remembered — and when the pin landed, nine of them failed for a reason
# unrelated to their subject (#3272 review round 4). One helper, one place that knows.
# run_report_args <dir> <corpus> <reps> <temps> <arms> <scan-passes> [extra reporter args…]
#
# The configuration is STAMPED INTO THE MANIFEST rather than passed to the reporter (#3272 F1):
# reps/temps/arms/scan-passes are properties of the SESSION now, and the reporter reads them.
# Stamped unconditionally with THIS call's values — a preserved neighbour manifest made cases
# report a configuration they had not set.
run_report_args() {
  local d="$1" c="$2" reps="${3-1}" temps="${4-warm}" arms="${5-bypass}" passes="${6-1}"
  shift 6 2>/dev/null || shift $#
  rm -f "$d/session-corpus-pin.json"
  ws0_pin_session_corpus "$d" "$c" "$reps" "$temps" "$arms" "$passes"
  python3 "$REPORT" --dir "$d" --corpus "$c" "$@" 2>&1
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

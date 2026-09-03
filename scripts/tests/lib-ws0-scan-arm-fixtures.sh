#!/usr/bin/env bash
# lib-ws0-scan-arm-fixtures.sh — the BARE-SCAN-ARM session builders the WS0 reporter
# self-tests share (issue #3272, campsite-rule split of test_ws0_report_guards.sh).
#
# Sourced, not executed, and it sets NO shell options: `set -uo pipefail` in a library
# mutates the SOURCING shell's options, which is the caller's decision. Same posture as its
# sibling `lib-ws0-fixtures.sh`, which this file's callers also source (it supplies
# `perf_csv`, `ws0_make_corpus`, `make_round`, `WS0_SCAN_FIXED` and the rest).
#
# # Why a library, and where the seam falls
#
# `test_ws0_report_guards.sh` grew to 1602 lines against the ~1500-line test target over
# eighteen review rounds, and its round-17 CELL-VOLUME block was split into
# `test_ws0_cell_volume_guards.sh`. Those two suites need the SAME four builders — a corpus
# of this fixture shape, a healthy scan rep, a healthy flight rep, and the report runner
# that stamps the session manifest if absent.
#
# Copying them would be the exact drift `lib-ws0-fixtures.sh`'s own header warns about: its
# `make_round` gained a `monotonic_ns` field in one round and had to be edited in two places.
# A builder duplicated across two suites is a builder that will disagree with itself, and a
# fixture that disagrees with the reporter's contract fails for a reason unrelated to the
# case's subject. So they live here, in ONE definition, moved VERBATIM — every comment
# recording why a field is computed rather than hardcoded travels with the code it explains.
#
# # What this file assumes of its caller, stated rather than left to be discovered
#
# These builders read four things from the SOURCING shell, because they are properties of the
# suite's fixture session rather than of the builder: `$TMP` (the suite's mktemp dir),
# `$CORPUS_ROWS` (the pinned corpus row count), `$REPORT` (the reporter under test) and the
# optional `$WS0_SCAN_CORPUS` override. A caller that has not set them gets an unbound-variable
# error under `set -u` — a loud failure at the first call, which is the correct outcome: a
# builder that defaulted them would write a fixture describing a corpus nobody pinned.

# This suite family's corpora are 700 KB, which is what its `bytes_per_row: 700.0` cases assert.
make_corpus() { ws0_make_corpus "$1" "${2:-$CORPUS_ROWS}" "${3:-700000}" "${4:-}"; }

# make_scan_rep <dir> <temp> <rep> <prewarm-status|-none->
make_scan_rep() {
  local d="$1" temp="$2" rep="$3" pw="$4" tag="scan-$2-$3"
  # The seven-field scan contract (#3272), from `lib-ws0-fixtures.sh`: `WS0_SCAN_FIXED` is the
  # constant half, `ws0_scan_session_bound` the corpus-valued half.
  cat > "$d/$tag.json" <<EOF
{ $WS0_SCAN_FIXED, $(ws0_scan_session_bound "${WS0_SCAN_CORPUS:-$TMP/corpus}"),
  "rows_denominator": $CORPUS_ROWS, "timed_scan_secs": 2.0, "setup_secs": 0.5,
  "passes": [ { "pass": 0, "rows": $CORPUS_ROWS, "cells": $(ws0_scan_pass_cells "$CORPUS_ROWS"), "secs": 2.0 } ] }
EOF
  perf_csv "$d/perf-$tag.csv" 2000000 4000000
  perf_csv "$d/perf-$tag-setup.csv" 100000 200000
  [ "$pw" = "-none-" ] || printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
  make_round "$d" "$tag" "$rep" "$(ws0_alternating_position "$rep" scan)"
}

# make_flight_rep <dir> <temp> <rep> <requests_ok> <rows> <prewarm-status|-none->
#
# `rows_per_s` is COMPUTED from `rows_total / duration_s` rather than hardcoded (#3272 review
# round 4). The reporter now DERIVES the throughput from those two counters and cross-checks
# the recorded rate against it, so a fixture carrying a fixed `250000.0` beside a varying
# `rows` would be refused for a reason that has nothing to do with the case under test — and
# it is also what the load generator itself writes (record.rs `per_s(self.rows_total)`).
make_flight_rep() {
  local d="$1" temp="$2" rep="$3" ok="$4" rows="$5" pw="$6" tag="flight-bypass-$2-$3"
  local secs=4.0 rps
  rps="$(python3 -c "print($rows / $secs)")"
  cat > "$d/$tag.jsonl" <<EOF
{"schema":"flight-loadgen.step/v1","step":0,"target_concurrency":1,"shape":"full","round":"$tag","endpoint":"$WS0_FIXTURE_ENDPOINT","requests_ok":$ok,"requests_error":0,"error_codes":{},"requests_unavailable":0,"rows_total":$rows,"rows_per_s":$rps,"duration_s":$secs}
EOF
  perf_csv "$d/perf-$tag.csv" 8000000 16000000
  # ...and the SERVER LOG the reporter reads the admission ceiling back from (#3551 item 10),
  # which it REQUIRES: without it every case in the suites using this builder would die here
  # rather than reaching its own subject. Written through the SHARED writer, in the real log's
  # ANSI-escaped shape, so the healthy path exercises the strip (#3400).
  ws0_write_server_log "$d/$tag.server.log" 4 derived 2
  [ "$pw" = "-none-" ] || printf '%s\n' "$pw" > "$d/$tag.prewarm.status"
  make_round "$d" "$tag" "$rep" "$(ws0_alternating_position "$rep" flight)"
}

# run_report <dir> <corpus> <temps> — prints the reporter's stdout+stderr. Call as
# `out=$(run_report ...); rc=$?`: a command substitution runs in a SUBSHELL, so a
# status the function assigned to a variable would not survive the call.
run_report() {
  # The PRE-MEASUREMENT corpus pin, stamped IF ABSENT — see lib-ws0-report-fixtures.sh's
  # `run_report` for why "if absent" and not unconditionally (#3272 review round 4).
  [ -e "$1/session-corpus-pin.json" ] || ws0_pin_session_corpus "$1" "$2" 1 "$3" bypass 1
  # The TEMPS are a property of the SESSION now (#3272 F1), so they are stamped into the
  # manifest above rather than passed here.
  python3 "$REPORT" --dir "$1" --corpus "$2" 2>&1
}

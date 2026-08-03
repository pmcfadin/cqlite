#!/usr/bin/env bash
# Self-test for the issue-#3234 AC3 warm-scan harness
# (cqlite-core/examples/bti_perf_scan/ — main.rs + manifest.rs + scope.rs).
#
# WHY THIS EXISTS
#
# The harness is the measuring instrument for AC3, and every claim it makes rests
# on guards that had never been OBSERVED to fire: its shell sibling
# (test_gen_perf_corpus_bti.sh) got negative controls per assert, this got none
# (rust-reviewer S7). A guard nobody has watched fail is a guard that might not
# exist -- and the failure mode here is the worst kind: a silently TRUNCATED scan
# reporting `RESULT: PASS` with a short row count and a plausible rows/s.
#
# So this drives the REAL binary and asserts its EXIT CODE for every documented
# failure mode:
#
#     2 USAGE               -- incl. --min-seconds nan/inf/-5/0, which f64::parse
#                              accepts and which silently DISABLED the AC3 floor
#                              before this round (rust-reviewer B2)
#     3 OPEN_FAILED         -- corpus absent (distinct from 7, below)
#     4 ZERO_ROWS           -- a scan over an empty corpus is a failure, not a pass
#     5 ROW_COUNT_MISMATCH  -- the truncation guard, now ON BY DEFAULT (B1)
#     6 WINDOW_TOO_SHORT    -- the AC3 >= 10 s floor
#     7 SCAN_FAILED         -- a scan that STARTED then failed mid-stream; before
#                              this round it exited 3 and was indistinguishable
#                              from "corpus missing" (rust-reviewer S6)
#     8 MANIFEST_UNREADABLE -- no authoritative row count => refuse to measure,
#                              incl. a PARTIAL row_count_cross_check (roborev #3234
#                              L3: missing/non-integer fields used to fall through a
#                              catch-all match arm and read as verified) and the
#                              IN-PROGRESS generation marker (#3234 M2)
#
# ...and one guard that is NOT about the row count, because the row count cannot see
# it: the INGEST SCOPE (roborev #3234 M1/F1). A retained `<table>-<uuid>` generation
# beside the measured one holds the SAME rows, so reconciliation yields the same count
# while the generation count -- which selects the scan route, and which every
# throughput figure is attributed to -- doubles. The cases below therefore assert the
# OBSERVED generation count and the reported scope, not just an exit code: a documented
# `tables[].sstable_dir` confines ingestion to exactly that directory, and an
# ambiguous root with nothing documenting it exits 3. "Exactly" is asserted against
# siblings whose full names EXTEND the selected one (`<dir>-backup`, `<table>-backup`),
# which is the case a SUBSTRING filter cannot express and which round 11 therefore still
# ingested (#3234 F1) -- and against the `<table>-<32 hex>` shape the manifest validator
# only CLAIMED to require (#3234 F2).
#
# HERMETIC, and CHEAP. No perf corpus (the AC3 corpus is ~2 GiB / 13.2 M rows and
# takes ~2 minutes to scan), no docker, no network, no CQLITE_DATASETS_ROOT, no
# fetched fixtures. Everything runs against the GIT-COMMITTED BTI (`da`) fixture
# test-data/datasets/sstables/test_da/multiclustering_table-* (10.4 KiB, 468 rows,
# 3 partitions, LZ4) plus its committed schema -- a Cassandra-5.0.2-WRITTEN corpus,
# per #3042 the only kind that can serve as an oracle. Whole suite: a few seconds
# after the example is built.
#
# The committed fixture and schema are SOURCE, not fetched data, so their absence
# is a FAILURE here, never a skip (#3148's reasoning).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE_DIR="$REPO_ROOT/test-data/datasets/sstables/test_da/multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf"
FIXTURE_SCHEMA="$REPO_ROOT/test-data/schemas/multiclustering-table-bti.cql"
# The fixture's row count, from its committed schema header and its own
# sstabledump JSONL golden -- Cassandra-written, not a CQLite output.
FIXTURE_ROWS=468
KS=test_da
TBL=multiclustering_table

# Declared case count. A suite that silently stops running cases must not be able
# to report success (the gap its sibling had): `fails=0` is necessary but not
# sufficient, so the floor below asserts the suite actually RAN.
#
# This suite has NO SKIPPABLE cases by design -- the fixtures are committed source
# (absence = FAIL, #3148), a build failure is a FAIL, and the corruption step needs
# no interpreter -- so there is nothing legitimate to skip and the floor is a plain
# `passes >= MIN_CASES`. The one BRANCH (build here vs reuse
# CQLITE_BTI_PERF_SCAN_BIN) records exactly one case on EITHER side, so the floor is
# identical on both paths (roborev #3234 F4).
MIN_CASES=77

fails=0
passes=0
pass() { echo "ok   - $1"; passes=$((passes + 1)); }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

summary() {
  echo
  if [ "$passes" -lt "$MIN_CASES" ]; then
    echo "FAIL - case-count floor: only $passes case(s) ran, under the $MIN_CASES this suite" \
      "declares (and it declares no skippable cases). A suite that stopped running cases must" \
      "not report success."
    fails=$((fails + 1))
  fi
  echo "test_bti_perf_scan: passes=$passes fails=$fails (declared floor $MIN_CASES)"
  if [ "$fails" -eq 0 ]; then
    echo "test_bti_perf_scan: ALL PASS ($passes cases)"
    exit 0
  fi
  echo "test_bti_perf_scan: $fails FAILURE(S)"
  exit 1
}

for f in "$FIXTURE_DIR/da-2-bti-Data.db" "$FIXTURE_SCHEMA"; do
  if [ ! -f "$f" ]; then
    echo "FAIL - missing COMMITTED fixture (source, not fetched data): $f"
    exit 1
  fi
done

# ------------------------------------------------------------------- build -----
# CQLITE_BTI_PERF_SCAN_BIN lets a caller reuse an already-built binary (e.g. the
# release build a profiling run just made). Otherwise build it here: a build
# failure is a FAILURE, never a skip -- the harness is the artifact under test.
#
# EITHER branch records exactly ONE case, so the declared MIN_CASES floor holds on
# both paths. Before this (roborev #3234 F4) the prebuilt branch skipped the build
# case WITHOUT recording anything while the floor stayed at 38, so the documented
# binary-reuse path reported 37 and failed its own floor -- a suite that could not
# pass in the mode its own comment advertises.
BIN="${CQLITE_BTI_PERF_SCAN_BIN:-}"
if [ -n "$BIN" ]; then
  pass "a prebuilt harness binary was supplied (CQLITE_BTI_PERF_SCAN_BIN=$BIN); build case not needed"
else
  build_log="$(mktemp)"
  if (cd "$REPO_ROOT" && cargo build -p cqlite-core --example bti_perf_scan \
    --features cli-helpers >"$build_log" 2>&1); then
    pass "the AC3 harness builds (cargo build --example bti_perf_scan --features cli-helpers)"
  else
    fail "the AC3 harness does not build; tail: $(tail -5 "$build_log" | tr '\n' ' ')"
    rm -f "$build_log"
    summary
  fi
  rm -f "$build_log"
  BIN="$REPO_ROOT/target/debug/examples/bti_perf_scan"
fi
if [ ! -x "$BIN" ]; then
  fail "harness binary not executable: $BIN"
  summary
fi

TMP="$(mktemp -d)"
# shellcheck disable=SC2317  # invoked indirectly by the EXIT trap below
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# Build the throwaway corpora the cases run against, in the layout the
# harness expects (<corpus>/sstables/<ks>/<table>-<uuid>/ + <corpus>/schema.cql).
mk_corpus() { # mk_corpus <name>  -> populated with the committed fixture
  local root="$TMP/$1"
  mkdir -p "$root/sstables/$KS"
  cp -r "$FIXTURE_DIR" "$root/sstables/$KS/"
  cp "$FIXTURE_SCHEMA" "$root/schema.cql"
  echo "$root"
}
GOOD="$(mk_corpus good)"
CORRUPT="$(mk_corpus corrupt)"
# An AMBIGUOUS corpus root: TWO <table>-<uuid> directories, the shape
# `gen-perf-corpus-bti.sh --no-prune` leaves behind (roborev #3234 M1). Both hold the
# same rows, which is exactly why the row-count assert cannot see the difference:
# reconciliation yields 468 either way while the GENERATION COUNT -- what selects the
# scan route, and what any throughput figure is attributed to -- doubles.
TWO="$(mk_corpus two)"
SECOND_UUID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
cp -r "$FIXTURE_DIR" "$TWO/sstables/$KS/$TBL-$SECOND_UUID"
# ...and a corpus holding a DIFFERENT table whose name has this table's name as a
# PREFIX. The old filter was a substring match, so `--table multiclustering_table`
# also picked up `multiclustering_table_small-<uuid>`.
PREFIX="$(mk_corpus prefix)"
cp -r "$FIXTURE_DIR" "$PREFIX/sstables/$KS/${TBL}_small-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
# ...and the case round 11's fix did NOT cover (roborev #3234 F1): siblings whose full
# names EXTEND the SELECTED directory's name. `/test_da/<table>-<uuid>` is a SUBSTRING of
# `/test_da/<table>-<uuid>-backup`, so scoping ingestion with a filter string ingested
# both while `generations` was counted in the selected directory alone -- extra SSTables
# scanned, the smaller count reported. Each of these holds the SAME 468 rows, which is
# why only the OBSERVED generation count can see the difference.
FIXTURE_BASE="$(basename "$FIXTURE_DIR")"
EXTEND="$(mk_corpus extend)"
cp -r "$FIXTURE_DIR" "$EXTEND/sstables/$KS/$FIXTURE_BASE-backup"
cp -r "$FIXTURE_DIR" "$EXTEND/sstables/$KS/$TBL-backup"
# An "empty" corpus: the table directory exists (so discovery is scoped to it) but
# holds no SSTable -- the shape a mis-pathed or half-generated corpus has.
EMPTY="$TMP/empty"
mkdir -p "$EMPTY/sstables/$KS/$(basename "$FIXTURE_DIR")"
cp "$FIXTURE_SCHEMA" "$EMPTY/schema.cql"

# Corrupt the middle of the LZ4-compressed Data.db, leaving every other component
# intact: the reader OPENS fine and fails while DECODING -- exactly the
# distinction between exit 3 and exit 7. Deterministic (a fixed 0xFF run, not
# /dev/urandom) and dependency-free: no python3, so nothing in this suite can be
# skipped for a missing interpreter.
head -c 200 /dev/zero | tr '\000' '\377' |
  dd of="$CORRUPT/sstables/$KS/$(basename "$FIXTURE_DIR")/da-2-bti-Data.db" \
    bs=1 seek=200 count=200 conv=notrunc status=none

manifest() { # manifest <file> <keyspace> <table> <rows>
  printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":%s}}\n' "$2" "$3" "$4" >"$1"
}
manifest "$TMP/m-good.json" "$KS" "$TBL" "$FIXTURE_ROWS"
manifest "$TMP/m-short.json" "$KS" "$TBL" 999
manifest "$TMP/m-other.json" perf_bti wide_multiclustering "$FIXTURE_ROWS"
printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":0}}\n' "$KS" "$TBL" \
  >"$TMP/m-zero.json"
# A manifest that documents the directory its counts were read from, the way every
# generator-written manifest does -- this is what SCOPES ingestion (roborev #3234 M1).
scoped_manifest() { # scoped_manifest <file> <sstable_dir>
  printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":%s},"tables":[{"table":"%s","sstable_dir":"%s"}]}\n' \
    "$KS" "$TBL" "$FIXTURE_ROWS" "$TBL" "$2" >"$1"
}
FIXTURE_REL="sstables/$KS/$(basename "$FIXTURE_DIR")"
scoped_manifest "$TMP/m-scoped.json" "$FIXTURE_REL"
scoped_manifest "$TMP/m-scoped-absent.json" "sstables/$KS/$TBL-cccccccccccccccccccccccccccccccc"
scoped_manifest "$TMP/m-scoped-escape.json" "sstables/$KS/../../../etc"
scoped_manifest "$TMP/m-scoped-abs.json" "/etc"
scoped_manifest "$TMP/m-scoped-othertable.json" "sstables/$KS/other_table-$SECOND_UUID"
scoped_manifest "$TMP/m-scoped-otherks.json" "sstables/perf_bti/$TBL-$SECOND_UUID"
# `sstable_dir` shapes that are NOT a Cassandra table directory (roborev #3234 F2). The
# check used to accept any `<table>-*` while CLAIMING to require `<table>-<uuid>`, so
# `<table>-backup` -- a backup copy, not a Cassandra directory -- redirected the
# measurement and bypassed the ambiguity guard (which only ever sees table directories).
ID32="$SECOND_UUID"                       # 32 hex: the accepted shape
ID31="${ID32:0:31}"                       # one too short
ID33="${ID32}a"                           # one too long
ID32_NONHEX="${ID32:0:31}g"               # right length, one non-hex digit
# The lengths are ASSERTED, not eyeballed: a mistyped literal would make one of the
# negative cases below prove nothing (it would be rejected for the wrong reason).
for v in "32:$ID32" "31:$ID31" "33:$ID33" "32:$ID32_NONHEX"; do
  if [ "${#v}" -ne $(( ${v%%:*} + 3 )) ]; then
    echo "FAIL - fixture bug: id '${v#*:}' is ${#v} chars in '$v', not ${v%%:*}"
    exit 1
  fi
done
scoped_manifest "$TMP/m-scoped-backup.json" "sstables/$KS/$TBL-backup"
scoped_manifest "$TMP/m-scoped-hex31.json" "sstables/$KS/$TBL-$ID31"
scoped_manifest "$TMP/m-scoped-hex33.json" "sstables/$KS/$TBL-$ID33"
scoped_manifest "$TMP/m-scoped-nonhex.json" "sstables/$KS/$TBL-$ID32_NONHEX"
# `tables` present but describing something else / not an array at all: the counts'
# provenance is then unknown, which is a refusal, not a fall-through.
printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":%s},"tables":[{"table":"other","sstable_dir":"%s"}]}\n' \
  "$KS" "$TBL" "$FIXTURE_ROWS" "$FIXTURE_REL" >"$TMP/m-tables-foreign.json"
printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":%s},"tables":"nope"}\n' \
  "$KS" "$TBL" "$FIXTURE_ROWS" >"$TMP/m-tables-notarray.json"
# The IN-PROGRESS marker gen-perf-corpus-bti.sh installs in the authoritative manifest
# position before it mutates a published corpus (roborev #3234 M2). It is well-formed
# JSON and deliberately carries no keyspace/table/row count, so a harness run over a
# corpus whose generation did not finish must REFUSE rather than read stale numbers.
printf '{"issue":3234,"generation_in_progress":true,"note":"generation did not finish"}\n' \
  >"$TMP/m-inprogress.json"
# A COMPLETE cross-check, and each of the ways it can be partial (roborev #3234 L3).
cross_manifest() { # cross_manifest <file> <row_count_cross_check-json>
  printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":%s,"partitions":3},"row_count_cross_check":%s}\n' \
    "$KS" "$TBL" "$FIXTURE_ROWS" "$2" >"$1"
}
cross_manifest "$TMP/m-cross-ok.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"statistics_db_rows\":$FIXTURE_ROWS,\"row_driver_partitions\":3,\"statistics_db_partitions\":3}"
# Each of the four counts MISSING in turn.
cross_manifest "$TMP/m-cross-miss-rd-rows.json" \
  "{\"statistics_db_rows\":$FIXTURE_ROWS,\"row_driver_partitions\":3,\"statistics_db_partitions\":3}"
cross_manifest "$TMP/m-cross-miss-st-rows.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"row_driver_partitions\":3,\"statistics_db_partitions\":3}"
cross_manifest "$TMP/m-cross-miss-rd-parts.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"statistics_db_rows\":$FIXTURE_ROWS,\"statistics_db_partitions\":3}"
cross_manifest "$TMP/m-cross-miss-st-parts.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"statistics_db_rows\":$FIXTURE_ROWS,\"row_driver_partitions\":3}"
# ...and each of the four NON-INTEGER in turn (string, null, float, negative -- every
# one of them `Some(value)` with `as_u64() == None`, i.e. the fall-through the old
# `match` arm swallowed).
cross_manifest "$TMP/m-cross-str.json" \
  "{\"row_driver_rows\":\"$FIXTURE_ROWS\",\"statistics_db_rows\":$FIXTURE_ROWS,\"row_driver_partitions\":3,\"statistics_db_partitions\":3}"
cross_manifest "$TMP/m-cross-null.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"statistics_db_rows\":null,\"row_driver_partitions\":3,\"statistics_db_partitions\":3}"
cross_manifest "$TMP/m-cross-float.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"statistics_db_rows\":$FIXTURE_ROWS,\"row_driver_partitions\":3.5,\"statistics_db_partitions\":3}"
cross_manifest "$TMP/m-cross-neg.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"statistics_db_rows\":$FIXTURE_ROWS,\"row_driver_partitions\":3,\"statistics_db_partitions\":-3}"
# A cross-check whose partition pair agrees with itself but has no total to be checked
# against: the total the cross-check asserts must be present, not optional.
printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":%s},' "$KS" "$TBL" \
  "$FIXTURE_ROWS" >"$TMP/m-cross-nototal.json"
printf '"row_count_cross_check":{"row_driver_rows":%s,"statistics_db_rows":%s,' \
  "$FIXTURE_ROWS" "$FIXTURE_ROWS" >>"$TMP/m-cross-nototal.json"
printf '"row_driver_partitions":3,"statistics_db_partitions":3}}\n' >>"$TMP/m-cross-nototal.json"
# A COMPLETE cross-check whose Statistics.db side disagrees with the row driver.
cross_manifest "$TMP/m-disagree.json" \
  "{\"row_driver_rows\":$FIXTURE_ROWS,\"statistics_db_rows\":471,\"row_driver_partitions\":3,\"statistics_db_partitions\":3}"
# The other half of the cross-check: the two sides disagreeing WITH EACH OTHER, while the
# row count itself matches. The harness used to read an `agree: true` literal here; it now
# compares the four numbers (the literal is gone from the manifest -- #3234 round 10: a
# field is observed or absent), so this is the negative control for that comparison.
printf '{"keyspace":"%s","table":"%s","rows_per_partition":{"rows":%s},' "$KS" "$TBL" \
  "$FIXTURE_ROWS" >"$TMP/m-pair-disagree.json"
printf '"row_count_cross_check":{"row_driver_rows":%s,"statistics_db_rows":%s,' \
  "$FIXTURE_ROWS" "$FIXTURE_ROWS" >>"$TMP/m-pair-disagree.json"
printf '"row_driver_partitions":5,"statistics_db_partitions":7}}\n' \
  >>"$TMP/m-pair-disagree.json"
printf '{"keyspace":"%s",\n' "$KS" >"$TMP/m-truncated.json"

# run_case <expected-rc> <description> <args...>
# Records the run's output in $out so a case can additionally grep it.
out=""
run_case() {
  local want="$1" desc="$2"
  shift 2
  out="$("$BIN" "$@" 2>&1)"
  local rc=$?
  if [ "$rc" -eq "$want" ]; then
    pass "$desc (exit $rc)"
    return 0
  fi
  fail "$desc: expected exit $want, got $rc; tail: $(tail -3 <<<"$out" | tr '\n' ' ' | cut -c1-220)"
  return 1
}

# Shorthand: scan the good corpus with the guards we are not testing turned off.
SCAN_GOOD=(--corpus "$GOOD" --keyspace "$KS" --table "$TBL" --warm-passes 0)

# ------------------------------------------------------------ 2: USAGE ---------
run_case 2 "--corpus is required" --keyspace "$KS"
run_case 2 "rejects an unknown argument" --corpus "$GOOD" --bogus
# B2: every one of these parses as f64 and used to leave the AC3 floor OFF while
# the header printed a gate value ("min_seconds_gate: NaN").
for bad in nan NaN inf -inf -5 0 -0.0; do
  run_case 2 "rejects --min-seconds '$bad'" --corpus "$GOOD" --min-seconds "$bad"
done
run_case 2 "rejects --min-seconds with no value" --corpus "$GOOD" --min-seconds
run_case 2 "rejects --expect-rows 0 (it used to mean 'assert off')" \
  --corpus "$GOOD" --expect-rows 0
run_case 2 "rejects --expect-rows together with --no-expect-rows" \
  --corpus "$GOOD" --expect-rows 5 --no-expect-rows

# ------------------------------------------------------- 3: OPEN_FAILED -------
run_case 3 "absent corpus exits OPEN_FAILED" \
  --corpus "$TMP/does-not-exist" --keyspace "$KS" --table "$TBL" \
  --manifest "$TMP/m-good.json"

# ------------------------------------------------ 8: MANIFEST_UNREADABLE ------
# B1: the row-count assert is ON by default, so an unavailable authority must
# REFUSE TO MEASURE rather than degrade to "assert off".
run_case 8 "an absent --manifest refuses to measure" \
  "${SCAN_GOOD[@]}" --manifest "$TMP/nope.json"
run_case 8 "an unparseable manifest refuses to measure" \
  "${SCAN_GOOD[@]}" --manifest "$TMP/m-truncated.json"
run_case 8 "a manifest for ANOTHER table refuses to measure" \
  "${SCAN_GOOD[@]}" --manifest "$TMP/m-other.json"
run_case 8 "a manifest whose rows is 0 refuses to measure" \
  "${SCAN_GOOD[@]}" --manifest "$TMP/m-zero.json"
run_case 8 "a manifest whose Statistics.db cross-check disagrees refuses to measure" \
  "${SCAN_GOOD[@]}" --manifest "$TMP/m-disagree.json"
run_case 8 "a manifest whose cross-check PAIR disagrees (partitions) refuses to measure" \
  "${SCAN_GOOD[@]}" --manifest "$TMP/m-pair-disagree.json"
# With no --manifest, resolution falls through to the COMMITTED production
# manifest, which describes perf_bti.wide_multiclustering -- so this corpus gets
# the same refusal rather than a vacuous pass. This case also proves the default
# (no-flag) path really does consult the committed manifest.
if run_case 8 "the DEFAULT path consults the committed manifest and refuses a foreign table" \
  "${SCAN_GOOD[@]}"; then
  if grep -q "test-data/perf-corpus-bti-manifest.json describes perf_bti.wide_multiclustering" \
    <<<"$out"; then
    pass "the refusal names the committed manifest and the table it describes"
  else
    fail "expected the committed manifest to be named; got: $(tail -2 <<<"$out")"
  fi
fi

# ------------------------------------------------------- 4: ZERO_ROWS ---------
run_case 4 "a corpus with no SSTable exits ZERO_ROWS, never 0" \
  --corpus "$EMPTY" --keyspace "$KS" --table "$TBL" --warm-passes 0 \
  --no-min-seconds --manifest "$TMP/m-good.json"

# ------------------------------------------------ 5: ROW_COUNT_MISMATCH -------
if run_case 5 "a row count short of the manifest exits ROW_COUNT_MISMATCH" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-short.json"; then
  if grep -q "scanned $FIXTURE_ROWS rows, expected 999" <<<"$out" \
    && grep -q "TRUNCATED" <<<"$out"; then
    pass "the mismatch names both counts and the truncation failure mode"
  else
    fail "expected a 'scanned N rows, expected M' + truncation diagnosis; got: $(tail -2 <<<"$out")"
  fi
fi
# The warming passes are gated too: a truncated warm pass must not be discarded
# silently. --warm-passes 1 fails on the WARM pass, before the measured one.
if run_case 5 "a truncated WARM pass fails too (it is not silently discarded)" \
  --corpus "$GOOD" --keyspace "$KS" --table "$TBL" --warm-passes 1 \
  --no-min-seconds --manifest "$TMP/m-short.json"; then
  if grep -q "warm pass 0: scanned" <<<"$out"; then
    pass "the warm-pass failure is attributed to the warm pass"
  else
    fail "expected the failure attributed to 'warm pass 0'; got: $(tail -2 <<<"$out")"
  fi
fi

# ------------------------------------------------ 6: WINDOW_TOO_SHORT --------
# The floor here is deliberately a DAY, not the AC3 10 s: the assertion is "the
# guard fires", and a threshold a loaded box could conceivably cross would make
# this a wall-clock race (#2642). The 10.0 s DEFAULT is pinned separately, from
# --help text, with no timing involved.
if run_case 6 "a sub-floor window exits WINDOW_TOO_SHORT" \
  "${SCAN_GOOD[@]}" --min-seconds 86400 --manifest "$TMP/m-good.json"; then
  if grep -q "under the 86400.000 s AC3 floor" <<<"$out" \
    && grep -qE "needs ~[0-9]+ rows" <<<"$out"; then
    pass "the sub-floor failure reports the row count that WOULD reach the floor"
  else
    fail "expected an AC3-floor diagnosis with a target row count; got: $(tail -2 <<<"$out")"
  fi
fi
out="$("$BIN" --help 2>&1)"
if grep -q -- "--min-seconds S .*AC3 floor) \[10.0\]" <<<"$out" \
  && grep -qE "^ +8 authoritative row count unavailable" <<<"$out"; then
  pass "--help documents the 10.0 s AC3 default and the exit-code set"
else
  fail "--help must document the AC3 default floor and the exit codes; got: $out"
fi

# ------------------------------------------------------ 7: SCAN_FAILED -------
# The corrupt corpus opens fine and fails while decoding: 7, NOT 3.
if run_case 7 "a mid-scan decode failure exits SCAN_FAILED, not OPEN_FAILED" \
  --corpus "$CORRUPT" --keyspace "$KS" --table "$TBL" --warm-passes 0 \
  --no-min-seconds --manifest "$TMP/m-good.json"; then
  if grep -q "^open: 1 sstables discovered" <<<"$out"; then
    pass "SCAN_FAILED is reported AFTER a successful open (so 3 vs 7 really do differ)"
  else
    fail "expected the corrupt corpus to OPEN before failing; got: $(head -3 <<<"$out")"
  fi
fi

# ------------------------------------------- 0: the guarded happy path --------
# The positive control. Every negative case above must be able to pass, or they
# would be proving nothing.
if run_case 0 "the fixture corpus PASSES with the row-count assert ON" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-good.json"; then
  if grep -q "^rows_scanned:     $FIXTURE_ROWS" <<<"$out" \
    && grep -q "^row_count_assert: $FIXTURE_ROWS (authoritative:" <<<"$out"; then
    pass "the result block reports the verified row count and its authority"
  else
    fail "expected a verified row count in the result block; got: $(tail -6 <<<"$out")"
  fi
  # S3: the route must be printed beside the number.
  if grep -q "^access_path:      " <<<"$out" \
    && grep -q "^storage_route:    " <<<"$out" \
    && grep -q "^generations:      1" <<<"$out" \
    && grep -q "^schema_resolved:  true" <<<"$out"; then
    pass "the result block names the ROUTE (access_path + storage_route + inputs)"
  else
    fail "expected access_path/storage_route/generations/schema_resolved; got: $(tail -6 <<<"$out")"
  fi
  # S5: --warm-passes 0 must not label a COLD scan as the AC3 warm measurement.
  if grep -q "AC3 measured COLD full scan" <<<"$out" \
    && grep -q "NOT the AC3 warm measurement" <<<"$out" \
    && grep -q "RESULT: PASS (COLD window" <<<"$out"; then
    pass "--warm-passes 0 labels the scan COLD everywhere, incl. the RESULT line"
  else
    fail "expected COLD labelling with --warm-passes 0; got: $(tail -6 <<<"$out")"
  fi
  # A pass taken with a guard disabled must say so in RESULT.
  if grep -q "UNGUARDED: window floor DISABLED" <<<"$out"; then
    pass "a PASS taken with a disabled guard is marked UNGUARDED"
  else
    fail "expected an UNGUARDED marker on the RESULT line; got: $(tail -2 <<<"$out")"
  fi
fi
if run_case 0 "--no-expect-rows passes but marks the measurement unverified" \
  "${SCAN_GOOD[@]}" --no-min-seconds --no-expect-rows; then
  if grep -q "row_count_assert: \*\*\* DISABLED (--no-expect-rows)" <<<"$out" \
    && grep -q "UNGUARDED: row-count assert DISABLED" <<<"$out"; then
    pass "the row-count opt-OUT is loud in both the header and the RESULT line"
  else
    fail "expected a loud --no-expect-rows banner; got: $(tail -6 <<<"$out")"
  fi
fi

# ------------------------------- the INGEST SCOPE (roborev #3234 M1) -----------
# The row-count assert CANNOT catch a changed workload, and that is the whole finding:
# a retained `<table>-<uuid>` generation holds the same rows, so reconciliation still
# yields 468 while the generation count -- which selects the scan route -- doubles.
# So the scope is asserted directly, in both of its resolutions.
if run_case 3 "an AMBIGUOUS corpus root (two <table>-<uuid> dirs) refuses to measure" \
  --corpus "$TWO" --keyspace "$KS" --table "$TBL" --warm-passes 0 --no-min-seconds \
  --manifest "$TMP/m-good.json"; then
  if grep -q "AMBIGUOUS corpus root: 2 " <<<"$out" \
    && grep -q "GENERATION COUNT" <<<"$out" && grep -q -- "--no-prune" <<<"$out"; then
    pass "the ambiguity refusal names the count, the consequence and the cause"
  else
    fail "expected an ambiguity diagnosis naming --no-prune; got: $(tail -3 <<<"$out")"
  fi
fi
run_case 3 "an ambiguous root is refused under --no-expect-rows too (no manifest to scope it)" \
  --corpus "$TWO" --keyspace "$KS" --table "$TBL" --warm-passes 0 --no-min-seconds \
  --no-expect-rows
# The PREFERRED resolution: the manifest documents its directory, so the SAME ambiguous
# root is measured at exactly the documented generation count.
if run_case 0 "a documented sstable_dir scopes ingestion inside an ambiguous root" \
  --corpus "$TWO" --keyspace "$KS" --table "$TBL" --warm-passes 0 --no-min-seconds \
  --manifest "$TMP/m-scoped.json"; then
  if grep -q "^generations:      1" <<<"$out"; then
    pass "the OBSERVED generation count is 1 -- only the manifest's directory was ingested"
  else
    fail "expected generations: 1 from a scoped ingest; got: $(grep '^generations' <<<"$out")"
  fi
  if grep -q "sstable(s) under .* are OUTSIDE this scope and were NOT ingested" <<<"$out" \
    && grep -q "^ingest_scope:     .*tables\[\].sstable_dir" <<<"$out"; then
    pass "the run REPORTS the scope it used and the sstable it left outside it"
  else
    fail "expected an ingest_scope line + an outside-the-scope note; got: $(tail -8 <<<"$out")"
  fi
fi
# The filter used to be a SUBSTRING of the table name, so a differently-named table
# whose name starts with this one was swept in. It must be neither ambiguous nor ingested.
if run_case 0 "a sibling table whose name has this table's name as a PREFIX is ignored" \
  --corpus "$PREFIX" --keyspace "$KS" --table "$TBL" --warm-passes 0 --no-min-seconds \
  --manifest "$TMP/m-good.json"; then
  if grep -q "^generations:      1" <<<"$out"; then
    pass "the prefix-sharing sibling contributed no generation"
  else
    fail "expected generations: 1 beside a ${TBL}_small dir; got: $(grep '^generations' <<<"$out")"
  fi
fi

# --- EXACTNESS: siblings EXTENDING the selected directory's FULL name (#3234 F1) ------
# The case a filter STRING cannot express. `/test_da/<table>-<uuid>` is a substring of
# `/test_da/<table>-<uuid>-backup`, so round 11's "scoped" filter ingested both.
#
# Asserted on the OBSERVED COUNTS, not on a log line: `generations:` is now counted in the
# directories ingestion ACTUALLY selected (main.rs::observed_generations), so a run that
# swept in either sibling reports 2 or 3 here. Both siblings are byte copies of the
# fixture, so `rows_scanned` stays 468 either way -- which is exactly why the row count
# alone was never able to see this, and why both numbers are asserted together.
extend_counts_ok() { # extend_counts_ok <label>
  local label="$1"
  if grep -q "^generations:      1" <<<"$out" \
    && grep -q "^rows_scanned:     $FIXTURE_ROWS" <<<"$out"; then
    pass "$label: exactly 1 generation and $FIXTURE_ROWS rows were OBSERVED"
  else
    fail "$label: expected generations: 1 + rows_scanned: $FIXTURE_ROWS; got: \
$(grep -E '^(generations|rows_scanned|generations_observed_in):' <<<"$out" | tr '\n' ' ')"
  fi
}
if run_case 0 "a documented scope beside '<dir>-backup' and '<table>-backup' measures only itself" \
  --corpus "$EXTEND" --keyspace "$KS" --table "$TBL" --warm-passes 0 --no-min-seconds \
  --manifest "$TMP/m-scoped.json"; then
  extend_counts_ok "documented scope"
  # ...and the two siblings ARE discoverable (else the case above would prove nothing):
  # discovery's unfiltered total is 3, so 2 SSTables were left outside the scope.
  if grep -q "^generations_observed_in: 1 directory/ies" <<<"$out" \
    && grep -q "^note: 2 sstable(s) under .* were NOT ingested" <<<"$out"; then
    pass "the two name-extending siblings were DISCOVERED and left outside the scope"
  else
    fail "expected 1 selected dir + a 2-sstable outside-the-scope note (the siblings must be \
discoverable, or this proves nothing); got: $(grep -E '^(note|generations_observed_in):' <<<"$out")"
  fi
fi
# The same corpus with NOTHING documenting the scope: `<dir>-backup` and `<table>-backup`
# must not even be CANDIDATES, so the root is unambiguous rather than a 3-way union.
if run_case 0 "sole-dir resolution ignores '<dir>-backup' and '<table>-backup' entirely" \
  --corpus "$EXTEND" --keyspace "$KS" --table "$TBL" --warm-passes 0 --no-min-seconds \
  --manifest "$TMP/m-good.json"; then
  extend_counts_ok "sole-dir resolution"
fi

# --- `<table>-<32 hex>` EXACTLY, in the manifest path too (#3234 F2) -----------------
# It claimed `<table>-<uuid>` and accepted any `<table>-*`. Each of these is a shape a
# measurement must not be redirected into; the positive controls are m-scoped.json
# (a real 32-hex id, exit 0, above) and m-scoped-absent.json (a 32-hex id that is simply
# not there, exit 3 -- accepted by validation, refused for absence).
if run_case 8 "an sstable_dir of '<table>-backup' is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-backup.json"; then
  if grep -q "32 hex digits" <<<"$out"; then
    pass "the refusal states the exact id shape it requires"
  else
    fail "expected the refusal to name the 32-hex requirement; got: $(tail -3 <<<"$out")"
  fi
fi
run_case 8 "an sstable_dir whose id is 31 hex digits is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-hex31.json"
run_case 8 "an sstable_dir whose id is 33 hex digits is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-hex33.json"
run_case 8 "an sstable_dir whose 32-char id is not all hex is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-nonhex.json"
# A documented directory that is not there means the manifest describes another corpus.
if run_case 3 "a documented sstable_dir that does not exist refuses to measure" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-absent.json"; then
  if grep -q "is not a directory" <<<"$out"; then
    pass "the missing-scope refusal names the directory the manifest documents"
  else
    fail "expected a 'not a directory' diagnosis; got: $(tail -3 <<<"$out")"
  fi
fi
# `sstable_dir` selects the bytes that get measured, so it is validated, not trusted.
run_case 8 "an sstable_dir escaping the corpus root (..) is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-escape.json"
run_case 8 "an ABSOLUTE sstable_dir is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-abs.json"
run_case 8 "an sstable_dir naming ANOTHER table is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-othertable.json"
run_case 8 "an sstable_dir naming another KEYSPACE is rejected" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-scoped-otherks.json"
run_case 8 "a tables[] with no entry for this table refuses to measure" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-tables-foreign.json"
run_case 8 "a tables that is not an array refuses to measure" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-tables-notarray.json"

# --------------------- the IN-PROGRESS marker (roborev #3234 M2) ---------------
# The generator vacates the authoritative manifest position BEFORE it mutates a
# published corpus, so a generation that dies half way leaves this marker instead of
# the PREVIOUS run's manifest. Reading it as a manifest is what must not happen.
if run_case 8 "an IN-PROGRESS generation marker refuses to measure, never reads as a manifest" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-inprogress.json"; then
  if grep -q "IN-PROGRESS GENERATION MARKER" <<<"$out" \
    && grep -q "generation_in_progress" <<<"$out"; then
    pass "the refusal names the marker key and says the generation did not finish"
  else
    fail "expected an in-progress-marker diagnosis; got: $(tail -3 <<<"$out")"
  fi
fi

# ------------------- the cross-check, IN FULL (roborev #3234 L3) ---------------
# It used to be read through a `match` with a catch-all arm, so a MISSING or
# NON-INTEGER field fell through and a partially corrupted cross-check passed as a
# verified one. Each field, each way.
for m in miss-rd-rows miss-st-rows miss-rd-parts miss-st-parts; do
  run_case 8 "a cross-check missing $m refuses to measure" \
    "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-cross-$m.json"
done
for m in str null float neg; do
  run_case 8 "a cross-check whose count is $m (not an unsigned integer) refuses to measure" \
    "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-cross-$m.json"
done
run_case 8 "a cross-check with no rows_per_partition.partitions to check against refuses" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-cross-nototal.json"
# The positive control for all nine: a COMPLETE, agreeing cross-check must still pass,
# or the strictness above would be proving nothing.
run_case 0 "a COMPLETE, agreeing cross-check PASSES (positive control for the nine above)" \
  "${SCAN_GOOD[@]}" --no-min-seconds --manifest "$TMP/m-cross-ok.json"

summary

#!/usr/bin/env bash
# Self-test for the issue-#3234 BTI (`da`) perf-corpus generator.
#
# What it pins, and why each one matters:
#
#   1. Flag validation happens BEFORE any expensive or destructive work. A typo
#      must never start a container, load millions of rows, and then overwrite the
#      COMMITTED manifest (the lesson #3068's generator learned the hard way).
#      Unrecognized arguments exit 2 (the fetch-datasets.sh convention).
#   2. --smoke lowers the DEFAULTS but NEVER overrides an explicit --keyspace,
#      --rows or --chunk-rows (or their env equivalents), and it defaults the
#      keyspace to perf_bti_smoke so a smoke run cannot clobber a production corpus.
#   3. THE ACCEPTANCE ASSERTS ARE REAL, in both directions. Issue #3234 AC1/AC2 are
#      "`da` descriptors only, >= 1 Data.db > 8 MiB, non-empty Rows.db, BTI TOC" --
#      and a stock Cassandra 5.0 node silently emits `nb` when either yaml setting
#      misses, so an assert that only ever ran on a good corpus is untested. Every
#      case here is driven through --verify-only against a FABRICATED corpus, with
#      a negative control per assert.
#   4. The row driver is deterministic given (seed, chunk-index) and emits exactly
#      the requested row count -- that determinism is what makes the manifest's
#      per-Data.db sha256 a reproducibility check rather than decoration.
#   5. The manifest writer fails closed on an empty / non-BTI SSTable directory
#      instead of emitting a manifest that describes nothing -- AND its happy path
#      actually runs (see 7), so the fields it publishes are asserted, not assumed.
#   6. The cassandra.yaml flip is verified against a COMMITTED cassandra:5.0.2
#      excerpt. It is the most consequential upstream guard in the generator: a
#      stock node emits `nb` (BIG) with no error at all, and the `sed` addresses
#      depend on the shipped file's exact comment markers and two-space indentation.
#   7. Stale-corpus pruning `rm -rf`s multi-GB paths, so every guard (symlink skip,
#      the <table>-<32 hex> name filter, the resolves-outside refusal, the `keep`
#      exclusion, and dry-run deleting nothing) is pinned -- mirroring the BIG
#      sibling test_gen_perf_corpus_3068.sh.
#   8. BOTH row-count cross-checks FIRE, in both directions: "COPY imported N, the
#      CSV held M" and "Statistics.db totalRows == sstabledump rows", plus the
#      manifest writer's plan-vs-Statistics.db rows AND partitions checks and its
#      refusal to fabricate an unobserved partition count.
#   9. The suite ITSELF cannot report success while having stopped running cases:
#      passes are counted against a declared floor, and each of the two legitimate
#      skips (no python3; < 5 GiB free) declares the case count it drops so that
#      count is credited against the floor and appears in the summary line.
#
# Hermetic: no docker, no sudo, no Cassandra, no network, no datasets. The
# container-dependent paths (8, and the manifest happy path in 5) run against
# scripts/tests/fixtures/stub-docker-cassandra-bti.py -- a stub `docker` handed to
# the generator via DOCKER=/--docker, which fabricates the metadata TEXT Cassandra
# would have printed. Everything else uses --help / --validate-only /
# --verify-only / --yaml-flip-check / --prune-dry-run, none of which start
# anything.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEN="$REPO_ROOT/test-data/scripts/gen-perf-corpus-bti.sh"
ROWS_PY="$REPO_ROOT/test-data/scripts/gen-perf-corpus-bti-rows.py"
MANIFEST_PY="$REPO_ROOT/test-data/scripts/write-perf-corpus-bti-manifest.py"

# Case accounting (rust-reviewer NIT on #3234). `fails=0` alone cannot tell "every
# case passed" from "the suite stopped running cases half way and exited clean", so
# the passes are counted and checked against a declared floor at the end. Two
# blocks here are legitimately conditional (no python3; less than 5 GiB free under
# TMPDIR), so each declares HOW MANY cases it drops via `skip`, the dropped count
# is credited against the floor, and both reach the SUMMARY line -- previously a
# SKIP was a bare echo that no summary ever mentioned.
#
# MIN_CASES is the full-suite pass count; SKIP_PY / SKIP_E2E are the case counts of
# the two conditional blocks (python3-only cases, of which the stub end-to-end cases
# are the inner block). Growing the suite means growing these.
MIN_CASES=102
SKIP_PY_CASES=30
SKIP_E2E_CASES=12

fails=0
passes=0
skipped_cases=0
skips=0
pass() { echo "ok   - $1"; passes=$((passes + 1)); }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }
# skip <cases-not-run> <reason...>
skip() {
  local n="$1"
  shift
  echo "SKIP - $* ($n case(s) NOT run)"
  skipped_cases=$((skipped_cases + n))
  skips=$((skips + 1))
}

for f in "$GEN" "$ROWS_PY" "$MANIFEST_PY"; do
  [ -f "$f" ] || { echo "FAIL - missing $f"; exit 1; }
done

TMP="$(mktemp -d)"
# shellcheck disable=SC2317  # invoked indirectly by the EXIT trap below
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# ------------------------------------------------------------------ usage -----
out=$(bash "$GEN" --help 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q '^  --smoke' <<<"$out" && grep -q '^  --verify-only' <<<"$out" \
   && grep -q '^  --seed S' <<<"$out" && grep -q '^  --rows N' <<<"$out"; then
  pass "--help exits 0 and lists the modes + flags"
else
  fail "--help: expected 0 and a flag listing (rc=$rc)"
fi

for bad in --bogus -x "--rows"; do
  out=$(bash "$GEN" "$bad" 2>&1); rc=$?
  if [ "$rc" -eq 2 ]; then
    pass "rejects '$bad' with exit 2"
  else
    fail "'$bad': expected exit 2, got $rc"
  fi
done

# --------------------------------------------------------- flag validation ----
out=$(bash "$GEN" --validate-only --out "$TMP/c" --rows 1000 --chunk-rows 250 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "VALIDATE-OK rows=1000 chunk_rows=250 chunks=4 " <<<"$out"; then
  pass "--validate-only reports the resolved chunk count and runs nothing"
else
  fail "--validate-only: expected chunks=4 (rc=$rc, out: $out)"
fi

# --smoke lowers rows/chunk-rows and defaults the keyspace away from production.
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" 2>&1)
if grep -q "keyspace=perf_bti_smoke" <<<"$out" && ! grep -q "rows=10200000" <<<"$out"; then
  pass "--smoke lowers the row count and defaults keyspace=perf_bti_smoke"
else
  fail "--smoke: expected a lowered row count + perf_bti_smoke (out: $out)"
fi
out=$(bash "$GEN" --smoke --keyspace mine --validate-only --out "$TMP/c" 2>&1)
if grep -q "keyspace=mine" <<<"$out"; then
  pass "--smoke does not override an explicit --keyspace"
else
  fail "--smoke overrode an explicit --keyspace (out: $out)"
fi
# --smoke is DEFAULTS-only: an explicitly supplied --rows/--chunk-rows (or the
# ROWS/CHUNK_ROWS env equivalent) must survive it. It used to replace both
# unconditionally, silently ignoring what the caller asked for.
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" --rows 7000 --chunk-rows 3500 2>&1)
if grep -q "VALIDATE-OK rows=7000 chunk_rows=3500 chunks=2 " <<<"$out"; then
  pass "--smoke keeps an explicit --rows AND --chunk-rows"
else
  fail "--smoke overrode an explicit --rows/--chunk-rows (out: $out)"
fi
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" --rows 600000 2>&1)
if grep -q "rows=600000 chunk_rows=120000 chunks=5 " <<<"$out"; then
  pass "--smoke keeps an explicit --rows and still lowers --chunk-rows"
else
  fail "--smoke did not combine an explicit --rows with the smoke chunk size (out: $out)"
fi
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" --chunk-rows 60000 2>&1)
if grep -q "rows=240000 chunk_rows=60000 chunks=4 " <<<"$out"; then
  pass "--smoke keeps an explicit --chunk-rows and still lowers --rows"
else
  fail "--smoke overrode an explicit --chunk-rows (out: $out)"
fi
out=$(ROWS=900000 CHUNK_ROWS=300000 bash "$GEN" --smoke --validate-only --out "$TMP/c" 2>&1)
if grep -q "rows=900000 chunk_rows=300000 chunks=3 " <<<"$out"; then
  pass "--smoke keeps ROWS/CHUNK_ROWS supplied through the environment"
else
  fail "--smoke overrode the ROWS/CHUNK_ROWS env values (out: $out)"
fi
out=$(bash "$GEN" --smoke --validate-only --out "$TMP/c" 2>&1)
if grep -q "rows=240000 chunk_rows=120000 chunks=2 " <<<"$out"; then
  pass "--smoke lowers both defaults when neither was supplied"
else
  fail "--smoke default plan changed (out: $out)"
fi
out=$(bash "$GEN" --validate-only --out "$TMP/c" 2>&1)
if grep -q "keyspace=perf_bti " <<<"$out"; then
  pass "production default keyspace is perf_bti"
else
  fail "expected keyspace=perf_bti by default (out: $out)"
fi

# ------------------------------- the COMMITTED manifest is never a default ----
# roborev #3234 F2: MANIFEST_OUT used to DEFAULT to the committed
# test-data/perf-corpus-bti-manifest.json, so the advertised `--smoke` invocation
# overwrote a committed provenance artifact with perf_bti_smoke metadata -- after
# which the default full-corpus scan rejects that manifest as foreign (exit 8).
COMMITTED_MANIFEST_REL="test-data/perf-corpus-bti-manifest.json"
for mode_args in "" "--smoke" "--small-golden"; do
  # shellcheck disable=SC2086  # deliberate word split of an optional single flag
  out=$(bash "$GEN" $mode_args --validate-only --out "$TMP/c" 2>&1)
  if grep -q "manifest_out=(none)" <<<"$out"; then
    pass "the committed manifest is NOT a default destination (${mode_args:-production})"
  else
    fail "${mode_args:-production}: expected manifest_out=(none) (out: $out)"
  fi
done
out=$(bash "$GEN" --publish-manifest --validate-only --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "manifest_out=.*$COMMITTED_MANIFEST_REL" <<<"$out"; then
  pass "--publish-manifest is the EXPLICIT opt-in that targets the committed manifest"
else
  fail "--publish-manifest: expected the committed manifest as the target (rc=$rc, out: $out)"
fi
check_reject "--publish-manifest from a --smoke run" \
  "refusing to write the COMMITTED production manifest" --smoke --publish-manifest
check_reject "--publish-manifest from a --small-golden run" \
  "refusing to write the COMMITTED production manifest" --small-golden --publish-manifest
out=$(bash "$GEN" --publish-manifest --manifest-out "$TMP/m.json" --validate-only \
        --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "mutually exclusive" <<<"$out"; then
  pass "--publish-manifest and --manifest-out are mutually exclusive (exit 2)"
else
  fail "--publish-manifest + --manifest-out: expected exit 2 (rc=$rc, out: $out)"
fi

# ---------------------------------------- --small-golden (the committable oracle) --
out=$(bash "$GEN" --small-golden --validate-only --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 0 ] \
   && grep -q "rows=6000 chunk_rows=6000 chunks=1 " <<<"$out" \
   && grep -q "keyspace=test_da table=wide_multiclustering_small " <<<"$out" \
   && grep -q "mode=small_golden" <<<"$out"; then
  pass "--small-golden plans ONE small SSTable under test_da.wide_multiclustering_small"
else
  fail "--small-golden defaults changed (rc=$rc, out: $out)"
fi
out=$(bash "$GEN" --small-golden --validate-only --out "$TMP/c" --rows 900 --chunk-rows 300 \
        --table mine 2>&1)
if grep -q "rows=900 chunk_rows=300 chunks=3 " <<<"$out" && grep -q "table=mine " <<<"$out"; then
  pass "--small-golden is DEFAULTS-only: an explicit --rows/--chunk-rows/--table wins"
else
  fail "--small-golden overrode explicit flags (out: $out)"
fi
out=$(bash "$GEN" --smoke --small-golden --validate-only --out "$TMP/c" 2>&1); rc=$?
if [ "$rc" -eq 2 ] && grep -q "mutually exclusive" <<<"$out"; then
  pass "--smoke and --small-golden are mutually exclusive (exit 2)"
else
  fail "--smoke --small-golden: expected exit 2 (rc=$rc, out: $out)"
fi

# Every rejection must be non-zero AND must not have created the corpus root it was
# pointed at.
#
# The no-write half is asserted on the DESTINATION THE INVOCATION ACTUALLY
# REQUESTED. The earlier shape counted leftovers in a scratch dir no invocation was
# ever pointed at (every case passed --out "$TMP/c"), so `leftovers` was
# structurally always 0 and eleven cases claimed a property nothing checked. The
# per-case --out is unique, so the check is live: it fails the moment validation
# starts creating (or pruning under) --out before the flags are checked.
check_reject() { # check_reject <label> <expect-substring> <args...>  (--out prepended)
  local label="$1" expect="$2"; shift 2
  local dest="$TMP/rej-$RANDOM-$RANDOM/corpus"
  local out rc existed
  # A caller-supplied --out in "$@" deliberately WINS (later wins in the parser),
  # which is how the bad---out cases are expressed; $dest must be untouched either way.
  out=$(bash "$GEN" --validate-only --out "$dest" "$@" 2>&1); rc=$?
  existed=no; [ -e "$dest" ] && existed=yes
  if [ "$rc" -ne 0 ] && grep -q "$expect" <<<"$out" && [ "$existed" = no ]; then
    pass "rejects $label"
  else
    fail "$label: expected non-zero + '$expect' + no $dest (rc=$rc, dest-created=$existed, out: $out)"
  fi
}
check_reject "--rows 0"            "must be >= 1"          --rows 0
check_reject "a non-integer --rows" "non-negative integer" --rows 12x
check_reject "--chunk-rows > --rows" "exceeds"             --rows 100 --chunk-rows 1000
check_reject "a relative --out"    "absolute path"         --out relative/path
check_reject "an empty --out"      "is empty"              --out ""
check_reject "--out /"             "refusing to use"       --out /
# --out is CANONICALIZED before anything is created or deleted (roborev #3234 F1).
# A lexical `!= "/"` test passed all three of these, and the script then ran
# `rm -rf "$OUT/cassandra-data"` as root -- i.e. deleted an unrelated /cassandra-data.
# Every case here asserts the REFUSAL; nothing is deleted by any of them.
check_reject "an --out that resolves to / through .." "resolves to '/'" --out /tmp/..
ln -sfn / "$TMP/slash-link"
check_reject "an --out SYMLINK resolving to /" "resolves to '/'" --out "$TMP/slash-link"
check_reject "an --out resolving to a system root" "a system root" --out /var/../var
# Positive control for the same mechanism: a legitimate path is ACCEPTED, and the
# resolved (symlink-free) form is what the run reports and therefore derives its
# destructive targets from.
mkdir -p "$TMP/canon-real/corpus"
ln -sfn "$TMP/canon-real" "$TMP/canon-link"
canon_expect="$(cd "$TMP/canon-real" && pwd -P)/corpus"
out=$(bash "$GEN" --validate-only --out "$TMP/canon-link/corpus" --rows 100 --chunk-rows 100 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "out=$canon_expect mode=" <<<"$out"; then
  pass "a symlinked --out is ACCEPTED and reported in its canonical form"
else
  fail "canonicalization positive control: expected out=$canon_expect (rc=$rc, out: $out)"
fi
check_reject "a bad keyspace"      "invalid keyspace"      --keyspace "Bad-KS"
check_reject "a bad table"         "invalid table"         --table "bad table"
check_reject "an empty --seed"     "seed is empty"         --seed ""
check_reject "a malformed --widths" "widths"               --widths "200"
check_reject "duplicate bucket first bytes" "widths"       --buckets "alpha,ateam"
check_reject "an empty --rows"     "non-negative integer"  --rows ""
# An explicitly EMPTY env value is a caller bug, not a request for the default.
emptydest="$TMP/empty-env/corpus"
out=$(ROWS="" bash "$GEN" --validate-only --out "$emptydest" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "non-negative integer" <<<"$out" && [ ! -e "$emptydest" ]; then
  pass "rejects an empty ROWS in the environment (never silently the default)"
else
  fail "empty ROWS env: expected non-zero + no writes (rc=$rc, out: $out)"
fi
# A SUCCESSFUL --validate-only must also write nothing: it runs before preflight,
# which is the only thing allowed to create the corpus root.
okdest="$TMP/validate-writes-nothing/corpus"
bash "$GEN" --validate-only --out "$okdest" --rows 1000 --chunk-rows 500 >/dev/null 2>&1
if [ ! -e "$okdest" ]; then
  pass "a passing --validate-only creates no corpus root either"
else
  fail "--validate-only created $okdest"
fi
# `pk` is a CQL `int`, so chunk N's key base (N * PK_STRIDE) has a hard ceiling.
# REGRESSION (issue #3234): the original 1e9 stride made chunk 3 start at
# 3,000,000,000 > INT32_MAX, and the 27-chunk production run died there — four
# minutes and three SSTables in — with a cqlsh ParseError, while the 2-chunk
# --smoke run never reached it. This pins the refusal at VALIDATE time (before any
# container), and the two cases below pin the boundary itself so a future stride
# change cannot silently reopen the hole.
check_reject "a plan over the \`pk int\` ceiling" "INT32_MAX" \
  --rows 2200000000 --chunk-rows 500000
out=$(bash "$GEN" --validate-only --out "$TMP/c" --rows 13200000 --chunk-rows 500000 2>&1); rc=$?
if [ "$rc" -eq 0 ] && grep -q "chunks=27 " <<<"$out"; then
  pass "the 27-chunk production plan fits the \`pk int\` ceiling"
else
  fail "production plan (27 chunks) must validate (rc=$rc, out: $out)"
fi

# --------------------------------------- AC1/AC2 asserts via --verify-only ----
# A fabricated `da` corpus: the asserts are file-level, so no container is needed.
# Data.db is sparse (truncate), which is exactly what the size assert reads.
make_corpus() { # make_corpus <dir> <data-bytes> <rows-db-bytes>
  local dir="$1" data="$2" rows="$3"
  mkdir -p "$dir"
  truncate -s "$data" "$dir/da-1-bti-Data.db"
  if [ "$rows" -gt 0 ]; then truncate -s "$rows" "$dir/da-1-bti-Rows.db"; else : >"$dir/da-1-bti-Rows.db"; fi
  local c
  for c in Partitions.db Statistics.db CompressionInfo.db Filter.db; do
    truncate -s 64 "$dir/da-1-bti-$c"
  done
  printf 'x\n' >"$dir/da-1-bti-Digest.crc32"
  printf 'Data.db\nStatistics.db\nDigest.crc32\nTOC.txt\nCompressionInfo.db\nFilter.db\nPartitions.db\nRows.db\n' \
    >"$dir/da-1-bti-TOC.txt"
}
verify() { # verify <corpus-root>
  bash "$GEN" --verify-only --out "$1" --keyspace perf_bti --table wide_multiclustering 2>&1
}

root="$TMP/good"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-0123456789abcdef0123456789abcdef" 9437184 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "VERIFY-OK " <<<"$out" && grep -q "largest_data_db=9437184" <<<"$out"; then
  pass "--verify-only accepts a well-formed da corpus (positive control)"
else
  fail "--verify-only on a good corpus: expected VERIFY-OK (rc=$rc, out: $out)"
fi

# AC1 negative control: an `nb-*` descriptor is the SILENT failure mode of a
# missed yaml setting and must be a hard failure.
root="$TMP/nb"
d="$root/sstables/perf_bti/wide_multiclustering-1123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096
truncate -s 1024 "$d/nb-1-big-Data.db"
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "AC1: non-BTI descriptor" <<<"$out"; then
  pass "--verify-only HARD-FAILS on an nb-* descriptor (AC1)"
else
  fail "AC1 nb-* case: expected a hard failure (rc=$rc, out: $out)"
fi

# AC2 negative control: an empty Rows.db means no row-index trie to profile.
root="$TMP/emptyrows"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-2123456789abcdef0123456789abcdef" 9437184 0
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "Rows.db is EMPTY" <<<"$out"; then
  pass "--verify-only HARD-FAILS on an empty Rows.db (AC2)"
else
  fail "AC2 empty-Rows.db case: expected a hard failure (rc=$rc, out: $out)"
fi

# AC2 negative control: below 8 MiB the two read planes are the same mapping.
root="$TMP/small"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-3123456789abcdef0123456789abcdef" 1048576 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "needs > 8388608" <<<"$out"; then
  pass "--verify-only HARD-FAILS below the 8 MiB read-plane floor (AC2)"
else
  fail "AC2 8MiB-floor case: expected a hard failure (rc=$rc, out: $out)"
fi

# The floor is STRICT (> 8388608, not >=): MADV_RANDOM is applied at
# `file_size >= 8 MiB`, so a Data.db of EXACTLY 8 MiB leaves nothing above the
# threshold to A/B against. Pin the boundary itself, not just a value far below it.
root="$TMP/exact8m"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-6123456789abcdef0123456789abcdef" 8388608 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "largest Data.db is 8388608 B, needs > 8388608" <<<"$out"; then
  pass "--verify-only HARD-FAILS at EXACTLY 8388608 B (the floor is strict)"
else
  fail "AC2 exact-8MiB boundary: expected a hard failure (rc=$rc, out: $out)"
fi
root="$TMP/exact8m1"
make_corpus "$root/sstables/perf_bti/wide_multiclustering-7123456789abcdef0123456789abcdef" 8388609 4096
out=$(verify "$root"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "largest_data_db=8388609" <<<"$out"; then
  pass "--verify-only accepts 8388609 B (one byte over the floor)"
else
  fail "AC2 boundary+1: expected VERIFY-OK (rc=$rc, out: $out)"
fi

# TOC contract: Index.db/Summary.db are BIG-only and must never appear.
# The expected substring must be the ASSERT's own message, not a bare component
# name: the die path echoes the whole TOC, so `grep -q "Index.db"` was satisfied by
# the failure message of ANY unrelated TOC failure.
for bigonly in Index.db Summary.db; do
  root="$TMP/bigtoc-$bigonly"
  d="$root/sstables/perf_bti/wide_multiclustering-4123456789abcdef0123456789abcdef"
  make_corpus "$d" 9437184 4096
  printf '%s\n' "$bigonly" >>"$d/da-1-bti-TOC.txt"
  out=$(verify "$root"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "TOC.txt lists $bigonly" <<<"$out"; then
    pass "--verify-only HARD-FAILS when the TOC lists BIG-only $bigonly"
  else
    fail "TOC $bigonly case: expected 'TOC.txt lists $bigonly' (rc=$rc, out: $out)"
  fi
  # ... and a BIG-only component FILE is fatal even when the TOC does not list it.
  root="$TMP/bigfile-$bigonly"
  d="$root/sstables/perf_bti/wide_multiclustering-8123456789abcdef0123456789abcdef"
  make_corpus "$d" 9437184 4096
  truncate -s 128 "$d/da-1-bti-$bigonly"
  out=$(verify "$root"); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "has a $bigonly file" <<<"$out"; then
    pass "--verify-only HARD-FAILS on a stray BIG-only $bigonly file"
  else
    fail "stray $bigonly file: expected 'has a $bigonly file' (rc=$rc, out: $out)"
  fi
done

# A missing BTI component in the TOC is also fatal.
root="$TMP/notoc"
d="$root/sstables/perf_bti/wide_multiclustering-5123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096
grep -v '^Rows.db$' "$d/da-1-bti-TOC.txt" >"$d/toc.tmp" && mv "$d/toc.tmp" "$d/da-1-bti-TOC.txt"
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "missing Rows.db" <<<"$out"; then
  pass "--verify-only HARD-FAILS when the TOC omits Rows.db"
else
  fail "TOC-omission case: expected a hard failure (rc=$rc, out: $out)"
fi

out=$(bash "$GEN" --verify-only --out "$TMP/nonexistent-root" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "no corpus at" <<<"$out"; then
  pass "--verify-only fails closed when there is no corpus"
else
  fail "--verify-only with no corpus: expected a hard failure (rc=$rc, out: $out)"
fi

# ------------------------------------------- the cassandra.yaml BTI flip -------
# The generator's most consequential upstream guard: a stock Cassandra 5.0 node
# emits `nb` (BIG) with NO error when either setting misses, and the `sed`
# addresses depend on the shipped file's exact comment markers and TWO-SPACE
# indentation ("#  selected_format: big"). Driven through --yaml-flip-check, which
# runs the PRODUCTION snippet -- the same text apply_bti_yaml runs in the container
# -- against a copy of the committed cassandra:5.0.2 excerpt.
YAML_FIXTURE="$REPO_ROOT/scripts/tests/fixtures/cassandra-5.0.2-cassandra.yaml.excerpt"
if [ ! -f "$YAML_FIXTURE" ]; then
  fail "missing the committed cassandra.yaml excerpt fixture: $YAML_FIXTURE"
else
  # Fixture-rot guard: the excerpt must still be in the SHIPPED (unflipped) form,
  # or the positive case below would be proving nothing.
  if grep -qx '#sstable:' "$YAML_FIXTURE" \
     && grep -qx '#  selected_format: big' "$YAML_FIXTURE" \
     && grep -qx 'storage_compatibility_mode: CASSANDRA_4' "$YAML_FIXTURE"; then
    pass "the committed cassandra:5.0.2 excerpt is in the shipped (unflipped) form"
  else
    fail "the yaml excerpt fixture is no longer in the shipped form (the flip cases below would be vacuous)"
  fi

  y="$TMP/yaml-ok.yaml"; cp "$YAML_FIXTURE" "$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -eq 0 ] && grep -q "YAML-FLIP-OK" <<<"$out" \
     && grep -qx 'storage_compatibility_mode: NONE' "$y" \
     && grep -qx 'sstable:' "$y" && grep -qx '  selected_format: bti' "$y"; then
    pass "the yaml flip sets BOTH mandatory settings on the shipped 5.0.2 file"
  else
    fail "yaml flip on the shipped file: expected both settings flipped (rc=$rc, out: $out)"
  fi

  # Negative: THREE-space indentation. The sed address no longer matches, so
  # selected_format stays commented and the node would silently emit `nb`.
  y="$TMP/yaml-indent.yaml"
  sed 's|^#  selected_format: big|#   selected_format: big|' "$YAML_FIXTURE" >"$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "selected_format was NOT set to bti" <<<"$out"; then
    pass "yaml flip HARD-FAILS when selected_format's indentation drifts"
  else
    fail "yaml indentation drift: expected a hard failure (rc=$rc, out: $out)"
  fi

  # Negative: the `#sstable:` block header is absent, so the child key would be
  # orphaned even if it flipped.
  y="$TMP/yaml-nosstable.yaml"
  grep -vx '#sstable:' "$YAML_FIXTURE" >"$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "sstable: block was NOT uncommented" <<<"$out"; then
    pass "yaml flip HARD-FAILS when the sstable: block header is missing"
  else
    fail "yaml missing sstable: block: expected a hard failure (rc=$rc, out: $out)"
  fi

  # Negative: the node is not on the shipped CASSANDRA_4 default, so the
  # storage_compatibility_mode substitution finds nothing to replace.
  y="$TMP/yaml-mode.yaml"
  sed 's|^storage_compatibility_mode: CASSANDRA_4|storage_compatibility_mode: UPGRADING|' \
    "$YAML_FIXTURE" >"$y"
  out=$(bash "$GEN" --yaml-flip-check "$y" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "storage_compatibility_mode was NOT set to NONE" <<<"$out"; then
    pass "yaml flip HARD-FAILS when storage_compatibility_mode is not the shipped default"
  else
    fail "yaml unexpected compatibility mode: expected a hard failure (rc=$rc, out: $out)"
  fi

  out=$(bash "$GEN" --yaml-flip-check "$TMP/no-such-yaml" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "no such cassandra.yaml" <<<"$out"; then
    pass "yaml flip HARD-FAILS on a missing cassandra.yaml"
  else
    fail "yaml missing file: expected a hard failure (rc=$rc, out: $out)"
  fi
fi

# ------------------------------------------------- prune scope (dry run only) --
# prune_stale_table_dirs does `$SUDO rm -rf` on MULTI-GB paths, so each guard is
# pinned: the symlink skip, the ^<table>-<32 hex>$ name filter, the
# resolves-outside refusal, the `keep` exclusion, and "a dry run deletes nothing".
# Mirrors the BIG sibling scripts/tests/test_gen_perf_corpus_3068.sh.
PCORPUS="$TMP/prune/corpus"
PKS="$PCORPUS/sstables/perf_bti"
POUTSIDE="$TMP/prune/outside-the-corpus"
mkdir -p "$PKS" "$POUTSIDE/precious"
UA="8cc9d0708a2711f1a82281d620fbe729"
UB="90c037f08a2711f1a82281d620fbe729"
USYM="${UA//8/a}"
mkdir -p "$PKS/wide_multiclustering-$UA" \
         "$PKS/wide_multiclustering-$UB" \
         "$PKS/wide_multiclustering-backup" \
         "$PKS/wide_multiclustering-$UA/nested/wide_multiclustering-$UB" \
         "$PKS/other_table-$UA"
touch "$PKS/wide_multiclustering-$UA-notes.txt"
ln -s "$POUTSIDE/precious" "$PKS/wide_multiclustering-$USYM"
# WOULD-PRUNE prints the RESOLVED path, so compare against the resolved keyspace dir.
PKS_REAL="$(cd "$PKS" && pwd -P)"

prune_dry() { # prune_dry [env-prefixed args...] -> stdout+stderr of a dry run
  bash "$GEN" --prune-dry-run --out "$PCORPUS" \
    --keyspace perf_bti --table wide_multiclustering 2>&1
}
out=$(prune_dry); rc=$?
would=$(grep '^WOULD-PRUNE ' <<<"$out" | sed 's/^WOULD-PRUNE //' | sort)
expected=$(printf '%s\n' "$PKS_REAL/wide_multiclustering-$UA" \
                         "$PKS_REAL/wide_multiclustering-$UB" | sort)
if [ "$rc" -eq 0 ] && [ "$would" = "$expected" ]; then
  pass "prune targets exactly the <table>-<32 hex> dirs"
else
  fail "prune candidate set wrong (rc=$rc)
  got:
$would
  expected:
$expected"
fi
if grep -q "skipping symlink (never followed)" <<<"$out"; then
  pass "prune skips a symlinked corpus dir explicitly (never followed)"
else
  fail "prune did not report the symlink skip (out: $out)"
fi
if grep -q "skipping 'wide_multiclustering-backup' (not a <table>-<uuid> corpus dir)" <<<"$out"; then
  pass "prune's name filter rejects a non-<uuid> suffix"
else
  fail "prune did not report the name-filter skip (out: $out)"
fi
for never in "$PKS_REAL/wide_multiclustering-backup" \
             "$PKS_REAL/other_table-$UA" \
             "$PKS_REAL/wide_multiclustering-$USYM" \
             "$PKS_REAL/wide_multiclustering-$UA/nested/wide_multiclustering-$UB" \
             "$PKS_REAL/wide_multiclustering-$UA-notes.txt" \
             "$POUTSIDE/precious"; do
  if grep -qF "WOULD-PRUNE $never" <<<"$out"; then
    fail "prune would have removed '$never'"
  else
    pass "prune does not target '${never#"$TMP"/}'"
  fi
done
missing=0
for must_exist in "$PKS/wide_multiclustering-$UA" "$PKS/wide_multiclustering-$UB" \
                  "$PKS/wide_multiclustering-backup" "$PKS/other_table-$UA" \
                  "$PKS/wide_multiclustering-$UA-notes.txt" "$POUTSIDE/precious"; do
  [ -e "$must_exist" ] || { fail "--prune-dry-run deleted $must_exist"; missing=1; }
done
[ "$missing" = 0 ] && pass "--prune-dry-run deletes nothing"

# The `keep` exclusion: publish() passes the basename it is about to publish, and
# that one dir must never be a candidate.
out=$(PRUNE_KEEP="wide_multiclustering-$UA" prune_dry)
if grep -qF "WOULD-PRUNE $PKS_REAL/wide_multiclustering-$UB" <<<"$out" \
   && ! grep -qF "WOULD-PRUNE $PKS_REAL/wide_multiclustering-$UA" <<<"$out"; then
  pass "prune excludes the dir being published (keep)"
else
  fail "prune ignored the keep exclusion (out: $out)"
fi

# A candidate that RESOLVES OUTSIDE the corpus keyspace dir (here: the keyspace dir
# itself is a symlink) must abort the prune, not be deleted through.
ECORPUS="$TMP/prune-escape/corpus"
EOUTSIDE="$TMP/prune-escape/elsewhere"
mkdir -p "$ECORPUS/sstables" "$EOUTSIDE/wide_multiclustering-$UA"
ln -s "$EOUTSIDE" "$ECORPUS/sstables/perf_bti"
out=$(bash "$GEN" --prune-dry-run --out "$ECORPUS" \
        --keyspace perf_bti --table wide_multiclustering 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "resolves OUTSIDE the corpus keyspace dir" <<<"$out" \
   && [ -d "$EOUTSIDE/wide_multiclustering-$UA" ]; then
  pass "prune REFUSES a candidate that resolves outside the corpus keyspace dir"
else
  fail "prune escape case: expected a refusal (rc=$rc, out: $out)"
fi

out=$(bash "$GEN" --prune-dry-run --out "$TMP/prune-never-generated" \
        --keyspace perf_bti --table wide_multiclustering 2>&1); rc=$?
if [ "$rc" -eq 0 ] && ! grep -q WOULD-PRUNE <<<"$out"; then
  pass "no corpus yet: prune is a clean no-op"
else
  fail "prune on a non-existent corpus root: expected a no-op (rc=$rc, out: $out)"
fi

# ------------------------------------------------- row driver determinism -----
if command -v python3 >/dev/null 2>&1; then
  gen_rows() { # gen_rows <out> <plan> <seed> <chunk>
    python3 "$ROWS_PY" --chunk-index "$4" --rows 3000 --seed "$3" --payload-bytes 32 \
      --widths 200:60,800:30 --buckets alpha,bo,charlie,delta \
      --out "$1" --plan-out "$2" >/dev/null 2>&1
  }
  gen_rows "$TMP/a.csv" "$TMP/a.jsonl" 4242 0
  gen_rows "$TMP/b.csv" "$TMP/b.jsonl" 4242 0
  gen_rows "$TMP/c.csv" "$TMP/c.jsonl" 4242 1
  gen_rows "$TMP/d.csv" "$TMP/d.jsonl" 9999 0
  sa=$(sha256sum <"$TMP/a.csv" | cut -d' ' -f1)
  sb=$(sha256sum <"$TMP/b.csv" | cut -d' ' -f1)
  sc=$(sha256sum <"$TMP/c.csv" | cut -d' ' -f1)
  sd=$(sha256sum <"$TMP/d.csv" | cut -d' ' -f1)
  if [ "$sa" = "$sb" ]; then
    pass "row driver is deterministic for the same (seed, chunk)"
  else
    fail "row driver is NOT deterministic for the same (seed, chunk)"
  fi
  if [ "$sa" != "$sc" ] && [ "$sa" != "$sd" ]; then
    pass "row driver varies with both the chunk index and the seed"
  else
    fail "row driver did not vary with chunk index ($sa vs $sc) / seed ($sa vs $sd)"
  fi
  if [ "$(wc -l <"$TMP/a.csv" | tr -d ' ')" = "3000" ]; then
    pass "row driver emits EXACTLY the requested row count"
  else
    fail "row driver emitted $(wc -l <"$TMP/a.csv") rows, expected 3000"
  fi
  planned=$(python3 -c 'import json,sys;print(sum(json.loads(l)["rows"] for l in open(sys.argv[1]) if l.strip()))' "$TMP/a.jsonl")
  if [ "$planned" = "3000" ]; then
    pass "row plan record reports the observed row count"
  else
    fail "row plan reported $planned rows, expected 3000"
  fi
  # Partition keys must not collide across chunks (one partition, one SSTable).
  overlap=$(python3 - "$TMP/a.csv" "$TMP/c.csv" <<'PY'
import sys
def pks(p):
    return {l.split(",", 1)[0] for l in open(p)}
print(len(pks(sys.argv[1]) & pks(sys.argv[2])))
PY
)
  if [ "$overlap" = "0" ]; then
    pass "partition keys never collide across chunks"
  else
    fail "chunks 0 and 1 share $overlap partition keys"
  fi
  for bad in "--widths 200:0" "--buckets alpha,ateam" "--buckets alpha" "--payload-bytes 2"; do
    # shellcheck disable=SC2086  # deliberate word split of the flag pair
    out=$(python3 "$ROWS_PY" --chunk-index 0 --rows 10 --seed 1 --payload-bytes 32 \
      --widths 200:1 --buckets alpha,bo --out "$TMP/z.csv" --plan-out "$TMP/z.jsonl" $bad 2>&1); rc=$?
    if [ "$rc" -ne 0 ]; then
      pass "row driver rejects '$bad'"
    else
      fail "row driver accepted '$bad' (out: $out)"
    fi
  done

  # ------------------------------------------- manifest writer fail-closed ----
  # shellcheck disable=SC2054  # the commas are inside flag VALUES (--widths/--buckets)
  man_args=(--corpus-root "$TMP" --keyspace perf_bti --table wide_multiclustering
            --image cassandra:5.0.2 --seed 1 --rows-requested 10 --chunk-rows 10
            --payload-bytes 32 --widths 200:1 --buckets alpha,bo --mode smoke
            --row-plan "$TMP/a.jsonl" --out "$TMP/manifest.json")
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/nope" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "not a directory" <<<"$out"; then
    pass "manifest writer rejects a missing --sstable-dir"
  else
    fail "manifest writer with a missing dir: expected a hard failure (rc=$rc, out: $out)"
  fi
  mkdir -p "$TMP/empty-dir"
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/empty-dir" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "refusing to write a manifest" <<<"$out" && [ ! -f "$TMP/manifest.json" ]; then
    pass "manifest writer refuses an SSTable-less directory and writes nothing"
  else
    fail "manifest writer on an empty dir: expected a hard failure (rc=$rc, out: $out)"
  fi
  # An nb-* descriptor beside a da-* one is a hard failure BEFORE any container run.
  mkdir -p "$TMP/mixed"
  make_corpus "$TMP/mixed" 1024 64
  truncate -s 64 "$TMP/mixed/nb-1-big-Data.db"
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/mixed" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && grep -q "non-BTI descriptor" <<<"$out" && [ ! -f "$TMP/manifest.json" ]; then
    pass "manifest writer refuses a directory holding an nb-* descriptor"
  else
    fail "manifest writer on a mixed dir: expected a hard failure (rc=$rc, out: $out)"
  fi

  # An unreadable / partial ROW PLAN must be an actionable SystemExit naming the
  # line, not a JSONDecodeError or KeyError traceback out of the aggregation.
  printf '{"chunk": 0, "rows": 10, ' >"$TMP/plan-truncated.jsonl"
  out=$(python3 "$MANIFEST_PY" "${man_args[@]}" --sstable-dir "$TMP/empty-dir" \
          --row-plan "$TMP/plan-truncated.jsonl" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && ! grep -q "Traceback" <<<"$out"; then
    pass "manifest writer reports a truncated row-plan line without a traceback"
  else
    fail "truncated row plan: expected a clean failure (rc=$rc, out: $out)"
  fi

  # ----------------------- the row plan must describe THIS configuration --------
  # roborev #3234 F3: the aggregate rows/partitions cross-checks against
  # Statistics.db cannot see a STALE plan -- one from an earlier run, with a
  # different seed, whose totals happen to match. The manifest would then declare a
  # seed and a generation plan that do not describe the corpus. So the plan's chunk
  # count, contiguity, per-chunk row counts and per-chunk seed material are checked
  # against --seed/--rows-requested/--chunk-rows before anything is written.
  #
  # HERMETIC: the check runs BEFORE the per-SSTable `sstablemetadata` containers, so
  # these cases need no docker. The positive control proves that ordering -- with a
  # MATCHING plan the run gets past the plan check and dies at the metadata step,
  # with `--docker` pointed at a command that is not docker.
  plan_rec() { # plan_rec <chunk> <seed-material> <rows>
    printf '{"chunk":%s,"seed_material":"%s","rows":%s,"partitions":2,"pk_min":0,' "$1" "$2" "$3"
    printf '"pk_max":9,"rows_per_partition_histogram":{"200":1,"800":1},'
    printf '"buckets_per_partition_histogram":{"4":2}}\n'
  }
  mkdir -p "$TMP/plancfg" "$TMP/bin"
  make_corpus "$TMP/plancfg" 1024 64
  # make_corpus's CompressionInfo.db is 64 zero bytes, which the (self-verifying)
  # CompressionInfo parser rejects. The positive control has to get PAST the plan
  # check and reach the metadata step, so give this one dir a REAL header:
  # UTF compressor, option count, chunk length, max compressed length, data length,
  # chunk count, chunk offsets (read-compression-info.py's documented layout).
  python3 - "$TMP/plancfg/da-1-bti-CompressionInfo.db" <<'PY'
import struct, sys
def utf(s: str) -> bytes:
    b = s.encode()
    return struct.pack(">H", len(b)) + b
buf = utf("LZ4Compressor") + struct.pack(">i", 1) + utf("chunk_length_in_kb") + utf("16")
buf += struct.pack(">iiqi", 16384, 2147483647, 1024, 1) + struct.pack(">q", 0)
open(sys.argv[1], "wb").write(buf)
PY
  printf '#!/bin/sh\nexit 1\n' >"$TMP/bin/not-docker"; chmod +x "$TMP/bin/not-docker"
  # shellcheck disable=SC2054  # the commas are inside flag VALUES (--widths/--buckets)
  plancfg_args=(--corpus-root "$TMP" --keyspace perf_bti --table wide_multiclustering
                --image cassandra:5.0.2 --seed 77 --rows-requested 1000 --chunk-rows 400
                --payload-bytes 32 --widths 200:1 --buckets alpha,bo --mode production
                --sstable-dir "$TMP/plancfg" --docker "$TMP/bin/not-docker"
                --out "$TMP/plancfg-manifest.json")
  # plan_case <label> <expect-substring> <plan-file>. NOT a pipeline stage: the
  # right-hand side of a pipeline runs in a SUBSHELL, so `pass`/`fail` would print
  # their line while their counter increment was discarded -- the case would then be
  # invisible to the declared case-count floor.
  plan_case() {
    local label="$1" expect="$2" plan="$3"
    rm -f "$TMP/plancfg-manifest.json"
    local o r
    o=$(python3 "$MANIFEST_PY" "${plancfg_args[@]}" --row-plan "$plan" 2>&1); r=$?
    if [ "$r" -ne 0 ] && grep -q "row-plan/config check FAILED" <<<"$o" \
       && grep -q "$expect" <<<"$o" && [ ! -f "$TMP/plancfg-manifest.json" ]; then
      pass "manifest writer HARD-FAILS on $label, and writes nothing"
    else
      fail "$label: expected a row-plan/config failure naming '$expect' (rc=$r, out: $o)"
    fi
  }
  { plan_rec 0 78:0 400; plan_rec 1 78:1 400; plan_rec 2 78:2 200; } >"$TMP/plan-seed.jsonl"
  plan_case "a plan generated from ANOTHER seed" "seed_material" "$TMP/plan-seed.jsonl"
  { plan_rec 0 77:0 400; plan_rec 1 77:1 400; plan_rec 3 77:3 200; } >"$TMP/plan-gap.jsonl"
  plan_case "a NON-CONTIGUOUS chunk set" "chunk index set" "$TMP/plan-gap.jsonl"
  { plan_rec 0 77:0 500; plan_rec 1 77:1 400; plan_rec 2 77:2 200; } >"$TMP/plan-wide.jsonl"
  plan_case "a plan whose chunk rows disagree with --chunk-rows" "puts 400 there" \
    "$TMP/plan-wide.jsonl"
  { plan_rec 0 77:0 400; plan_rec 1 77:1 400; } >"$TMP/plan-short.jsonl"
  plan_case "a plan SHORT of the configured chunk count" "chunk count" "$TMP/plan-short.jsonl"
  # Positive control: a matching plan passes the config check, so the run proceeds to
  # the (deliberately unavailable) sstablemetadata step instead of failing here.
  { plan_rec 0 77:0 400; plan_rec 1 77:1 400; plan_rec 2 77:2 200; } >"$TMP/plan-cfg-ok.jsonl"
  rm -f "$TMP/plancfg-manifest.json"
  out=$(python3 "$MANIFEST_PY" "${plancfg_args[@]}" --row-plan "$TMP/plan-cfg-ok.jsonl" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && ! grep -q "row-plan/config check FAILED" <<<"$out" \
     && grep -q "could not read totalRows" <<<"$out"; then
    pass "a plan that MATCHES the configuration passes the check (positive control)"
  else
    fail "plan/config positive control: expected to get past the plan check (rc=$rc, out: $out)"
  fi

  # ------------------------------- end-to-end through the stub `docker` ---------
  # The generator's two row-count cross-checks and the manifest writer's HAPPY PATH
  # only execute when a container answers. They are exercised here against
  # scripts/tests/fixtures/stub-docker-cassandra-bti.py: the real row driver, real
  # CSVs, real file-level asserts, a real manifest -- and no container.
  STUB="$REPO_ROOT/scripts/tests/fixtures/stub-docker-cassandra-bti.py"
  COMMITTED_MANIFEST="$REPO_ROOT/test-data/perf-corpus-bti-manifest.json"
  committed_before="$(sha256sum "$COMMITTED_MANIFEST" | cut -d' ' -f1)"
  mkdir -p "$TMP/bin"
  SUDO_STUB="$TMP/bin/sudo-stub"
  # Stands in for `sudo -n`: runs the command, but never needs root for the two
  # ownership fixups only a real bind mount requires.
  cat >"$SUDO_STUB" <<'SUDOEOF'
#!/usr/bin/env bash
[ "${1:-}" = "-n" ] && shift
case "${1:-}" in chown|chmod) exit 0 ;; esac
exec "$@"
SUDOEOF
  chmod +x "$SUDO_STUB"

  STUB_UUID="a1b2c3d40000000000000000000000ff"
  # Belt and braces: every case below ALSO passes --manifest-out "" so a regression
  # that re-defaults MANIFEST_OUT cannot reach the committed manifest through these
  # runs. The ONE case that must exercise the DEFAULT resolution clears this array
  # (see the smoke-default case).
  E2E_MANIFEST_ARGS=(--manifest-out "")
  e2e_run() { # e2e_run <name> [extra generator args...]; env prefixes are honored
    local name="$1"; shift
    E2E_ROOT="$TMP/e2e-$name"
    E2E_LOG="$TMP/e2e-$name.log"
    cp "$YAML_FIXTURE" "$TMP/yaml-$name.yaml"
    DOCKER="python3 $STUB" SUDO="$SUDO_STUB" \
    STUB_STATE="$TMP/stub-state-$name" STUB_KS=perf_bti_stub \
    STUB_TBL=wide_multiclustering STUB_YAML="$TMP/yaml-$name.yaml" \
    STUB_PLAN="$E2E_ROOT/work/row-plan.jsonl" \
      bash "$GEN" --out "$E2E_ROOT" --keyspace perf_bti_stub \
        --table wide_multiclustering --rows 1200 --chunk-rows 600 \
        --payload-bytes 32 --widths 200:60,800:30 \
        --buckets alpha,bo,charlie,delta --seed 3234 \
        ${E2E_MANIFEST_ARGS[@]+"${E2E_MANIFEST_ARGS[@]}"} "$@" >"$E2E_LOG" 2>&1
  }

  # The generator's own preflight demands >= 4 GiB free under --out (it sizes for a
  # real multi-GB load), so the stub run needs that much on TMPDIR's filesystem.
  # Reported LOUDLY when it is absent -- never silently dropped.
  tmp_avail_gib="$(df -BG --output=avail "$TMP" 2>/dev/null | tail -1 | tr -dc '0-9')"
  if [ ! -f "$STUB" ]; then
    fail "missing the stub docker: $STUB"
  elif [ "${tmp_avail_gib:-0}" -lt 5 ]; then
    skip "$SKIP_E2E_CASES" "only ${tmp_avail_gib:-?} GiB free under $TMP; the generator's" \
      "preflight needs >= 4 GiB, so the stub end-to-end cases were not run"
  else
    # ---- positive control: the whole pipeline, and the manifest it writes -------
    e2e_run ok; rc=$?
    manifest="$TMP/e2e-ok/manifest-bti-3234.json"
    if [ "$rc" -eq 0 ] && [ -f "$manifest" ]; then
      pass "end-to-end run against the stub docker succeeds and writes a manifest"
    else
      fail "stub end-to-end run failed (rc=$rc, tail: $(tail -12 "$TMP/e2e-ok.log"))"
    fi
    # Both cross-checks must have RUN, not merely not-failed.
    if grep -q "COPY imported" "$TMP/e2e-ok.log" 2>/dev/null \
       || grep -q "imported 600 rows" "$TMP/e2e-ok.log" 2>/dev/null; then
      pass "the per-chunk COPY row-count check ran on every chunk"
    else
      fail "no COPY row-count check in the log (tail: $(tail -12 "$TMP/e2e-ok.log"))"
    fi
    if grep -q "Statistics.db totalRows == sstabledump rows ==" "$TMP/e2e-ok.log" 2>/dev/null; then
      pass "the Statistics.db-vs-sstabledump row-count check ran"
    else
      fail "no Statistics.db/sstabledump cross-check in the log (tail: $(tail -12 "$TMP/e2e-ok.log"))"
    fi
    if [ -f "$manifest" ]; then
      out=$(python3 - "$manifest" "sstables/perf_bti_stub/wide_multiclustering-$STUB_UUID" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
t = m["tables"][0]
plan = m["rows_per_partition"]
bad = []
def eq(label, got, want):
    if got != want:
        bad.append(f"{label}: got {got!r}, want {want!r}")
eq("sstable_count", t["sstable_count"], 2)
eq("rows", t["rows"], plan["rows"])
eq("partitions", t["partitions"], plan["partitions"])
eq("sstable_dir (corpus-root relative)", t["sstable_dir"], sys.argv[2])
eq("meets_8mib_read_plane_floor", t["meets_8mib_read_plane_floor"], True)
eq("every_rows_db_non_empty", t["every_rows_db_non_empty"], True)
eq("ddl.extracted_statements", t["ddl"]["extracted_statements"], True)
eq("clustering_arity", t["clustering_arity"], 2)
eq("cross-check agree", m["row_count_cross_check"]["agree"], True)
eq("cross-check rows", m["row_count_cross_check"]["statistics_db_rows"], plan["rows"])
eq("cross-check partitions",
   m["row_count_cross_check"]["statistics_db_partitions"], plan["partitions"])
eq("plan chunks", len(plan["chunks"]), 2)
eq("seed_material of chunk 0", plan["chunks"][0]["seed_material"], "3234:0")
eq("goldens", sum(1 for s in t["sstables"] if s["sstabledump_golden"]), 1)
for s in t["sstables"]:
    eq(f"{s['sstable_basename']} format", s["format"], "da")
    eq(f"{s['sstable_basename']} compressor", s["compression"]["compressor"], "LZ4Compressor")
    eq(f"{s['sstable_basename']} chunk_length_bytes", s["compression"]["chunk_length_bytes"], 16384)
    eq(f"{s['sstable_basename']} sha256 length", len(s["data_db_sha256"]), 64)
    eq(f"{s['sstable_basename']} rows>0", s["rows"] > 0, True)
    eq(f"{s['sstable_basename']} partitions observed",
       isinstance(s["statistics"]["partition_count"], int), True)
    eq(f"{s['sstable_basename']} TOC has no BIG components",
       [c for c in s["toc"] if c in ("Index.db", "Summary.db")], [])
print("MANIFEST-FIELDS-OK" if not bad else "BAD: " + "; ".join(bad))
PY
      )
      if [ "$out" = "MANIFEST-FIELDS-OK" ]; then
        pass "the manifest's happy-path fields are all read back from the bytes"
      else
        fail "manifest fields: $out"
      fi
    fi

    # ---- direction 2: each cross-check must FAIL when the two sides disagree ----
    STUB_IMPORT_SHORT=1 e2e_run import-short; rc=$?
    if [ "$rc" -ne 0 ] && grep -q "partial load" "$TMP/e2e-import-short.log" \
       && [ ! -f "$TMP/e2e-import-short/manifest-bti-3234.json" ]; then
      pass "COPY importing one row fewer than the CSV is a HARD failure, no manifest"
    else
      fail "import-short case: expected a partial-load failure (rc=$rc, tail: $(tail -6 "$TMP/e2e-import-short.log"))"
    fi

    STUB_META_SHORT=1 e2e_run meta-short; rc=$?
    if [ "$rc" -ne 0 ] && grep -q "row-count mismatch for" "$TMP/e2e-meta-short.log" \
       && [ ! -f "$TMP/e2e-meta-short/manifest-bti-3234.json" ]; then
      pass "Statistics.db totalRows != sstabledump rows is a HARD failure, no manifest"
    else
      fail "meta-short case: expected a row-count mismatch (rc=$rc, tail: $(tail -6 "$TMP/e2e-meta-short.log"))"
    fi

    # The manifest writer's own cross-checks (goldens off, so the generator's
    # sstabledump check cannot pre-empt them).
    STUB_ROWS_DELTA=1 e2e_run rows-delta --dump-generations 0; rc=$?
    if [ "$rc" -ne 0 ] && grep -q "row-count cross-check FAILED" "$TMP/e2e-rows-delta.log" \
       && [ ! -f "$TMP/e2e-rows-delta/manifest-bti-3234.json" ]; then
      pass "manifest writer HARD-FAILS when Statistics.db rows != the row plan"
    else
      fail "rows-delta case: expected 'row-count cross-check FAILED' (rc=$rc, tail: $(tail -6 "$TMP/e2e-rows-delta.log"))"
    fi

    STUB_PARTITIONS_DELTA=1 e2e_run parts-delta --dump-generations 0; rc=$?
    if [ "$rc" -ne 0 ] \
       && grep -q "partition-count cross-check FAILED" "$TMP/e2e-parts-delta.log" \
       && [ ! -f "$TMP/e2e-parts-delta/manifest-bti-3234.json" ]; then
      pass "manifest writer HARD-FAILS when Statistics.db partitions != the row plan"
    else
      fail "parts-delta case: expected 'partition-count cross-check FAILED' (rc=$rc, tail: $(tail -6 "$TMP/e2e-parts-delta.log"))"
    fi

    # A partition count that could not be OBSERVED must be an error, never a 0
    # (CLAUDE.md: "a counter not observed is an error, never a fabricated 0").
    STUB_NO_HISTOGRAM=1 e2e_run no-hist --dump-generations 0; rc=$?
    if [ "$rc" -ne 0 ] \
       && grep -q "refusing to publish an unobserved partition count" "$TMP/e2e-no-hist.log" \
       && [ ! -f "$TMP/e2e-no-hist/manifest-bti-3234.json" ]; then
      pass "an unreadable Partition Size histogram is an ERROR, not a fabricated 0"
    else
      fail "no-histogram case: expected an unobserved-partition-count refusal (rc=$rc, tail: $(tail -6 "$TMP/e2e-no-hist.log"))"
    fi

    # ---- roborev #3234 F2: a --smoke run with the DEFAULT manifest resolution -----
    # This is the invocation the generator's own header advertises. It used to
    # overwrite the COMMITTED manifest with perf_bti_smoke metadata, after which the
    # default full-corpus scan rejects that manifest as describing another table
    # (bti_perf_scan exit 8). NOTE the empty E2E_MANIFEST_ARGS: this case must run the
    # REAL default resolution, so it is the one run that does NOT pass --manifest-out.
    E2E_MANIFEST_ARGS=()
    e2e_run smoke-default --smoke; rc=$?
    E2E_MANIFEST_ARGS=(--manifest-out "")
    smoke_manifest="$TMP/e2e-smoke-default/manifest-bti-3234.json"
    if [ "$rc" -eq 0 ] && [ -f "$smoke_manifest" ] \
       && grep -q '"mode": "smoke"' "$smoke_manifest"; then
      pass "a --smoke run writes its manifest INSIDE the corpus, marked mode=smoke"
    else
      fail "smoke-default case: expected an in-corpus smoke manifest (rc=$rc, tail: $(tail -8 "$TMP/e2e-smoke-default.log"))"
    fi
    if [ "$(sha256sum "$COMMITTED_MANIFEST" | cut -d' ' -f1)" = "$committed_before" ]; then
      pass "a DEFAULT --smoke run leaves the committed manifest byte-identical (sha256)"
    else
      fail "a --smoke run with the default manifest resolution OVERWROTE $COMMITTED_MANIFEST"
    fi

    # No run above may touch the COMMITTED manifest.
    if [ "$(sha256sum "$COMMITTED_MANIFEST" | cut -d' ' -f1)" = "$committed_before" ]; then
      pass "no stub run modified the committed perf-corpus-bti-manifest.json"
    else
      fail "a stub run OVERWROTE the committed manifest $COMMITTED_MANIFEST"
    fi
  fi
else
  skip "$SKIP_PY_CASES" "python3 unavailable: row-driver + manifest-writer cases not run"
fi

echo
# Case-count floor: a suite that silently stopped running cases must not be able to
# report success on `fails=0` alone. Every legitimate skip declared its case count
# above, so passes + skipped must still reach the declared total.
if [ "$((passes + skipped_cases))" -lt "$MIN_CASES" ]; then
  fail "case-count floor: $passes case(s) ran + $skipped_cases declared skipped =" \
    "$((passes + skipped_cases)), under the $MIN_CASES this suite declares -- cases stopped" \
    "running (or a skip's declared count is stale)."
fi
echo "test_gen_perf_corpus_bti: passes=$passes fails=$fails skips=$skips" \
  "skipped-cases=$skipped_cases (declared floor $MIN_CASES)"
if [ "$fails" -eq 0 ]; then
  echo "test_gen_perf_corpus_bti: ALL PASS ($passes cases, $skipped_cases skipped)"
  exit 0
fi
echo "test_gen_perf_corpus_bti: $fails FAILURE(S)"
exit 1

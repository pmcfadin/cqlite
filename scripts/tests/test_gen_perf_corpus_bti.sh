#!/usr/bin/env bash
# Self-test for the issue-#3234 BTI (`da`) perf-corpus generator.
#
# What it pins, and why each one matters:
#
#   1. Flag validation happens BEFORE any expensive or destructive work. A typo
#      must never start a container, load millions of rows, and then overwrite the
#      COMMITTED manifest (the lesson #3068's generator learned the hard way).
#      Unrecognized arguments exit 2 (the fetch-datasets.sh convention).
#   2. --smoke lowers the defaults but NEVER overrides an explicit --keyspace, and
#      it defaults the keyspace to perf_bti_smoke so a smoke run cannot clobber a
#      production corpus.
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
#      instead of emitting a manifest that describes nothing.
#
# Hermetic: no docker, no sudo, no Cassandra, no network, no datasets. Only
# --help / --validate-only / --verify-only / the row driver / the manifest writer's
# pre-container guards are exercised, none of which start a container.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEN="$REPO_ROOT/test-data/scripts/gen-perf-corpus-bti.sh"
ROWS_PY="$REPO_ROOT/test-data/scripts/gen-perf-corpus-bti-rows.py"
MANIFEST_PY="$REPO_ROOT/test-data/scripts/write-perf-corpus-bti-manifest.py"

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; fails=$((fails + 1)); }

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
out=$(bash "$GEN" --validate-only --out "$TMP/c" 2>&1)
if grep -q "keyspace=perf_bti " <<<"$out"; then
  pass "production default keyspace is perf_bti"
else
  fail "expected keyspace=perf_bti by default (out: $out)"
fi

# Every rejection must be non-zero AND must not have written anything.
check_reject() { # check_reject <label> <expect-substring> <args...>
  local label="$1" expect="$2"; shift 2
  local workdir="$TMP/rej-$RANDOM"
  mkdir -p "$workdir"
  local out rc leftovers
  out=$(bash "$GEN" --validate-only "$@" 2>&1); rc=$?
  leftovers=$(find "$workdir" -mindepth 1 | wc -l | tr -d ' ')
  if [ "$rc" -ne 0 ] && grep -q "$expect" <<<"$out" && [ "$leftovers" = "0" ]; then
    pass "rejects $label"
  else
    fail "$label: expected non-zero + '$expect' + no writes (rc=$rc, leftovers=$leftovers, out: $out)"
  fi
}
check_reject "--rows 0"            "must be >= 1"          --out "$TMP/c" --rows 0
check_reject "a non-integer --rows" "non-negative integer" --out "$TMP/c" --rows 12x
check_reject "--chunk-rows > --rows" "exceeds"             --out "$TMP/c" --rows 100 --chunk-rows 1000
check_reject "a relative --out"    "absolute path"         --out relative/path
check_reject "an empty --out"      "is empty"              --out ""
check_reject "--out /"             "refusing to use"       --out /
check_reject "a bad keyspace"      "invalid keyspace"      --out "$TMP/c" --keyspace "Bad-KS"
check_reject "a bad table"         "invalid table"         --out "$TMP/c" --table "bad table"
check_reject "an empty --seed"     "seed is empty"         --out "$TMP/c" --seed ""
check_reject "a malformed --widths" "widths"               --out "$TMP/c" --widths "200"
check_reject "duplicate bucket first bytes" "widths"       --out "$TMP/c" --buckets "alpha,ateam"
# `pk` is a CQL `int`, so chunk N's key base (N * PK_STRIDE) has a hard ceiling.
# REGRESSION (issue #3234): the original 1e9 stride made chunk 3 start at
# 3,000,000,000 > INT32_MAX, and the 27-chunk production run died there — four
# minutes and three SSTables in — with a cqlsh ParseError, while the 2-chunk
# --smoke run never reached it. This pins the refusal at VALIDATE time (before any
# container), and the two cases below pin the boundary itself so a future stride
# change cannot silently reopen the hole.
check_reject "a plan over the \`pk int\` ceiling" "INT32_MAX" \
  --out "$TMP/c" --rows 2200000000 --chunk-rows 500000
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

# TOC contract: Index.db/Summary.db are BIG-only and must never appear.
root="$TMP/bigtoc"
d="$root/sstables/perf_bti/wide_multiclustering-4123456789abcdef0123456789abcdef"
make_corpus "$d" 9437184 4096
printf 'Index.db\n' >>"$d/da-1-bti-TOC.txt"
out=$(verify "$root"); rc=$?
if [ "$rc" -ne 0 ] && grep -q "Index.db" <<<"$out"; then
  pass "--verify-only HARD-FAILS when the TOC lists a BIG-only component"
else
  fail "TOC case: expected a hard failure (rc=$rc, out: $out)"
fi

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
else
  echo "SKIP - python3 unavailable: row-driver + manifest-writer cases not run"
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "test_gen_perf_corpus_bti: ALL PASS"
  exit 0
fi
echo "test_gen_perf_corpus_bti: $fails FAILURE(S)"
exit 1

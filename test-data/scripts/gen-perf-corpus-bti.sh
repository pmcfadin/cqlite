#!/usr/bin/env bash
# gen-perf-corpus-bti.sh — Generate a PROFILEABLE, LZ4-compressed, multi-SSTable
# Cassandra 5.0 BTI (`da`) corpus with WIDE partitions and a COMPOUND clustering
# key, for issue #3234 (prerequisite for #3029 WS3 and #3030 WS4).
#
# WHY this exists (issue #3234): every `da-*-bti-*` object reachable anywhere is a
# CORRECTNESS fixture — the largest, `test_da/wide_table`, is a 28 KB Data.db with
# a 760 B Rows.db. Two consequences make BTI read-plane work impossible on them:
#   (1) a warm scan finishes in microseconds, ~6 orders of magnitude short of the
#       >= 10 s sustained window a throughput profile needs; and
#   (2) `MADV_RANDOM` is only applied at `file_size >= 8 MiB`, so below that the
#       point-read and scan mappings are literally the same mapping and any
#       read-plane A/B is STRUCTURALLY zero, not merely noisy.
# Hence the hard floor asserted below: >= 1 Data.db > 8 MiB, with a non-empty
# Rows.db, in `da` format.
#
# WHY A STOCK NODE CANNOT PRODUCE THIS: Cassandra 5.0 ships
# `storage_compatibility_mode: CASSANDRA_4`, which pins the BIG (`nb`) format.
# TWO non-default cassandra.yaml settings are mandatory, and BOTH must be applied
# before the table is created:
#     storage_compatibility_mode: NONE
#     sstable:
#       selected_format: bti
# A miss on EITHER silently emits `nb` with NO error at all — which is why the
# yaml edit is grep-VERIFIED and the emitted descriptors are asserted to be
# `da-*-bti-*` (issue #3234 AC1: an `nb` run is a HARD FAILURE, never a warning).
# Mechanism lifted from gen-multiclustering-bti.sh:110-124.
#
# HOW IT DIVERGES FROM ITS BIG SIBLING (gen-perf-corpus-3068.sh): the row driver
# is a RECORDED SEED -> reproducible CSV -> chunked `COPY`, NOT a
# `cassandra-stress` user profile. cassandra-stress's row values cannot be
# reproduced from anything a manifest can record, so a regenerated corpus could
# only ever be compared on aggregate counts; a recorded seed makes the ROW SET
# itself reproducible from the committed script alone (issue #3234 AC6). See
# gen-perf-corpus-bti-rows.py for the determinism contract.
#
# WHAT THE SEED DOES *NOT* REPRODUCE: the Data.db BYTES. Cassandra stamps a
# wall-clock write timestamp on every row, serialized as an unsigned VInt delta
# from the Statistics.db min_timestamp baseline, so a later run shifts some deltas
# across a VInt width boundary and even the file LENGTH changes. Measured here:
# two same-seed smoke runs produced 19,474,015 B and 19,474,397 B. The manifest's
# per-SSTable sha256 is therefore an INSTANCE IDENTITY (prove two measurements ran
# on the same bytes; catch silent corruption), not a regeneration check.
#
# MULTI-SSTable BY CONSTRUCTION: autocompaction is disabled BEFORE the first load
# and each chunk is followed by an explicit `nodetool flush`, so chunk N becomes
# SSTable N. Without the disable, STCS merges the chunks and the multi-SSTable
# shape (which is what a real merge-path profile needs) is lost.
#
# PARITY ORACLE (issue #3042): every byte here is CASSANDRA-WRITTEN, so the
# emitted `sstabledump` JSONL goldens are a legitimate parity oracle. A
# CQLite-written round-trip fixture is NOT — it is invariant to a uniform
# framing error made by both halves.
#
# Output layout (mirrors CQLITE_DATASETS_ROOT, so `CQLITE_DATASETS_ROOT=$OUT`
# works directly):
#   $OUT/sstables/$KS/$TBL-<uuid>/da-*-bti-*.db         (gitignored: *.db)
#   $OUT/sstables/$KS/$TBL-<uuid>/da-<gen>-bti-Data.db.jsonl   (bounded goldens)
#   $OUT/sstables/$KS/$TBL-<uuid>/schema.cql
#   $OUT/schema.cql              (the same capture, where bti_perf_scan reads it;
#                                 installed only AFTER the in-progress marker, #3234 F3)
#   $OUT/manifest-bti-3234.json  (copied to test-data/perf-corpus-bti-manifest.json
#                                 ONLY with the explicit --publish-manifest, which is
#                                 production-mode-only: a --smoke/--small-golden
#                                 manifest describes another table)
#
# Usage:
#   bash test-data/scripts/gen-perf-corpus-bti.sh --smoke        # ~2 min, validates the pipeline
#   bash test-data/scripts/gen-perf-corpus-bti.sh                # production default (~2 GiB)
#   bash test-data/scripts/gen-perf-corpus-bti.sh --rows 33000000  # ~5 GiB
#   bash test-data/scripts/gen-perf-corpus-bti.sh --validate-only  # flags only, runs nothing
#   bash test-data/scripts/gen-perf-corpus-bti.sh --verify-only    # assert an EXISTING corpus
#   bash test-data/scripts/gen-perf-corpus-bti.sh --help
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROWS_PY="$SCRIPT_DIR/gen-perf-corpus-bti-rows.py"
MANIFEST_PY="$SCRIPT_DIR/write-perf-corpus-bti-manifest.py"

# --------------------------------------------------------------- defaults ----
IMAGE="${IMAGE:-cassandra:5.0.2}"
CONTAINER="${CONTAINER:-cqlite-perf-bti-3234}"
# docker needs sudo on the agent fleet machines; override with DOCKER=docker.
DOCKER="${DOCKER:-sudo -n docker}"
# Used for the few paths Cassandra wrote as uid 999 (the bind mount).
SUDO="${SUDO:-sudo -n}"
# `${VAR-default}` (not `:-`): an EXPLICITLY EMPTY OUT is a caller bug and must
# fail validation rather than silently become the default — this script deletes
# multi-GB paths under $OUT.
OUT="${OUT-/data/corpus-3234-bti}"
KS="${KS-}"                       # defaulted after --smoke is known
TBL_EXPLICIT=0; [ -z "${TBL+x}" ] || TBL_EXPLICIT=1
TBL="${TBL-wide_multiclustering}"
# ROWS/CHUNK_ROWS track whether the CALLER supplied them, because --smoke is a
# DEFAULTS override: it may lower only values nobody asked for. `${VAR+x}` (set,
# even if empty) is the test, and the values themselves use `${VAR-default}` (not
# `:-`) so an explicitly EMPTY value is a caller bug that fails validation rather
# than silently becoming the default.
ROWS_EXPLICIT=0; [ -z "${ROWS+x}" ] || ROWS_EXPLICIT=1
CHUNK_ROWS_EXPLICIT=0; [ -z "${CHUNK_ROWS+x}" ] || CHUNK_ROWS_EXPLICIT=1
# ~2.0 GiB at the density MEASURED by this generator's commissioning run
# (162 B/row on disk at --payload-bytes 160, LZ4/16 KiB). 5 GiB ~= 33000000 rows.
ROWS="${ROWS-13200000}"
# 500k rows/chunk => ~78 MiB Data.db per SSTable, comfortably over the 8 MiB floor.
CHUNK_ROWS="${CHUNK_ROWS-500000}"
SEED="${SEED:-20260803}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-160}"
# rows-per-partition distribution, "<rows>:<weight>". Every class is wide enough
# that its partition spans several 16 KiB row-index blocks, so Rows.db is
# populated for every partition (the fail-closed assert below would catch it if not).
WIDTHS_EXPLICIT=0; [ -z "${WIDTHS+x}" ] || WIDTHS_EXPLICIT=1
WIDTHS="${WIDTHS:-200:60,800:30,4000:10}"
# Distinct FIRST BYTES + heterogeneous lengths: the depth-1 transition spread is
# what keeps the row-index trie from degenerating (gen-multiclustering-bti.sh, #3032).
BUCKETS="${BUCKETS:-alpha,bo,charlie-extended-bucket,delta,ep,foxtrot-long-bucket-name,golf,hh,india-bucket,jj}"
# `sstabledump` goldens are ~1.7x Data.db, so only a BOUNDED subset is dumped
# (issue #3234 AC5). 0 disables golden generation entirely.
DUMP_GENERATIONS="${DUMP_GENERATIONS:-1}"
# The read-plane floor (POINT_MMAP_MADV_RANDOM_MIN_BYTES). See the header.
MIN_DATA_DB_EXPLICIT=0; [ -z "${MIN_DATA_DB_BYTES+x}" ] || MIN_DATA_DB_EXPLICIT=1
MIN_DATA_DB_BYTES="${MIN_DATA_DB_BYTES:-8388608}"
MAX_HEAP="${MAX_HEAP:-8G}"
HEAP_NEW="${HEAP_NEW:-1600M}"
COPY_PROCESSES="${COPY_PROCESSES:-12}"
COPY_TIMEOUT="${COPY_TIMEOUT:-120}"
CHUNK_LENGTH_IN_KB="${CHUNK_LENGTH_IN_KB:-16}"   # matches #3100/#3217's BIG shape
# Extra `docker run` options (resource caps, e.g. "--cpus 14 --memory 22g").
DOCKER_RUN_OPTS="${DOCKER_RUN_OPTS:-}"
# Remove a previous <table>-<uuid> corpus dir before publishing the new one.
PRUNE_STALE="${PRUNE_STALE:-1}"
KEEP_CONTAINER="${KEEP_CONTAINER:-0}"

ASSERT_SSTABLE_COUNT=0
ASSERT_MAX_DATA=0
DUMPED=()

# The COMMITTED provenance artifact. Replacing it requires an EXPLICIT opt-in
# (--publish-manifest, or --manifest-out naming it) AND production mode — see the
# MANIFEST_OUT resolution below.
COMMITTED_MANIFEST="$SCRIPT_DIR/../perf-corpus-bti-manifest.json"
PUBLISH_MANIFEST="${PUBLISH_MANIFEST:-0}"

log() { echo "[gen-perf-bti] $*"; }
die() { echo "[gen-perf-bti] FATAL: $*" >&2; exit 1; }

usage() {
  cat <<EOF
usage: $0 [options]

Generate a Cassandra-written BTI (\`da\`) perf corpus: wide partitions, compound
clustering key (pk, bucket, seq), LZ4 chunk_length_in_kb=$CHUNK_LENGTH_IN_KB, one SSTable per chunk.

modes
  --smoke                small end-to-end run (~2 min) that still exercises every
                         fail-closed assert; defaults the keyspace to perf_bti_smoke
                         so it can never clobber a production corpus
  --small-golden         generate the COMMITTABLE small Cassandra-written BTI golden
                         (a CORRECTNESS ORACLE, not a profile target): same
                         PRIMARY KEY (pk, bucket, seq) shape, 600 rows, one SSTable,
                         no 8 MiB floor; defaults to test_da.wide_multiclustering_small
                         and is sized to the repo's committed-golden convention
                         (#3032's multiclustering_table), not to a row count
  --validate-only        validate flags and exit 0; starts no container, writes nothing
  --prune-dry-run        --validate-only + list the stale corpus dirs a run WOULD remove
                         (PRUNE_KEEP=<basename> excludes one, as publish() does)
  --verify-only          re-assert an ALREADY-GENERATED corpus under --out and exit;
                         no container, no writes, nothing mutated. It also checks the
                         SSTable count against --rows/--chunk-rows (one SSTable per
                         chunk, generations 1..CHUNKS), so pass the SAME --rows and
                         --chunk-rows the corpus was generated with
  --yaml-flip-check FILE self-test hook: run the PRODUCTION cassandra.yaml flip
                         (sed + the three verification greps) against a LOCAL yaml
                         copy — edits FILE IN PLACE, prints the verified lines,
                         exits non-zero if either setting did not take; no container

options (env var in parentheses; all have defaults)
  --out DIR              corpus root (OUT) [$OUT]
  --rows N               total rows to load (ROWS) [$ROWS]
  --chunk-rows N         rows per chunk = rows per SSTable (CHUNK_ROWS) [$CHUNK_ROWS]
  --seed S               row-driver seed, recorded in the manifest (SEED) [$SEED]
  --payload-bytes N      payload column width (PAYLOAD_BYTES) [$PAYLOAD_BYTES]
  --widths SPEC          rows-per-partition distribution <rows>:<weight>,... (WIDTHS) [$WIDTHS]
  --buckets LIST         clustering bucket names, distinct first bytes (BUCKETS)
  --keyspace KS          keyspace (KS) [perf_bti / perf_bti_smoke]
  --table T              table (TBL) [$TBL]
  --dump-generations N   sstabledump JSONL goldens to emit, 0 = none (DUMP_GENERATIONS) [$DUMP_GENERATIONS]
  --min-data-db-bytes N  per-SSTable Data.db floor (MIN_DATA_DB_BYTES) [$MIN_DATA_DB_BYTES]
  --image IMG            Cassandra image (IMAGE) [$IMAGE]
  --container NAME       container name (CONTAINER) [$CONTAINER]
  --manifest-out PATH    ALSO copy the manifest to PATH; "" (the default) copies
                         nowhere and leaves \$OUT/manifest-bti-3234.json the only one
                         (MANIFEST_OUT)
  --publish-manifest     replace the COMMITTED production manifest
                         (test-data/perf-corpus-bti-manifest.json). Production mode
                         ONLY: a --smoke / --small-golden run is refused, because its
                         metadata would describe another table and make the default
                         full-corpus scan reject the manifest (PUBLISH_MANIFEST=1)
  --keep-container       leave the container running after a successful run
  --no-prune             keep previous <table>-<uuid> corpus dirs (PRUNE_STALE=0).
                         The result is an AMBIGUOUS corpus root: --verify-only and
                         bti_perf_scan both REFUSE it (the generation count selects
                         the scan route), so retained generations must be moved out
                         of the corpus tree before anything measures it
  -h, --help             this text

Unrecognized arguments exit 2.
EOF
}

# ---------------------------------------------------------- arg parsing ------
SMOKE=0
SMALL_GOLDEN="${SMALL_GOLDEN:-0}"
VALIDATE_ONLY=0
PRUNE_DRY_RUN=0
VERIFY_ONLY=0
YAML_FLIP_CHECK=""
KS_EXPLICIT=0
if [ -n "$KS" ]; then KS_EXPLICIT=1; fi
# Set even to "" through the environment counts as explicit (`${VAR+x}`), so an
# operator's `MANIFEST_OUT=` is honored rather than silently re-defaulted.
MANIFEST_OUT_EXPLICIT=0; [ -z "${MANIFEST_OUT+x}" ] || MANIFEST_OUT_EXPLICIT=1

need_arg() { [ $# -ge 2 ] || { echo "$0: $1 requires a value" >&2; exit 2; }; }

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke) SMOKE=1; shift ;;
    --small-golden) SMALL_GOLDEN=1; shift ;;
    --validate-only) VALIDATE_ONLY=1; shift ;;
    --prune-dry-run) VALIDATE_ONLY=1; PRUNE_DRY_RUN=1; shift ;;
    --verify-only) VERIFY_ONLY=1; shift ;;
    --yaml-flip-check) need_arg "$@"; YAML_FLIP_CHECK="$2"; shift 2 ;;
    --out) need_arg "$@"; OUT="$2"; shift 2 ;;
    --rows) need_arg "$@"; ROWS="$2"; ROWS_EXPLICIT=1; shift 2 ;;
    --chunk-rows) need_arg "$@"; CHUNK_ROWS="$2"; CHUNK_ROWS_EXPLICIT=1; shift 2 ;;
    --seed) need_arg "$@"; SEED="$2"; shift 2 ;;
    --payload-bytes) need_arg "$@"; PAYLOAD_BYTES="$2"; shift 2 ;;
    --widths) need_arg "$@"; WIDTHS="$2"; WIDTHS_EXPLICIT=1; shift 2 ;;
    --buckets) need_arg "$@"; BUCKETS="$2"; shift 2 ;;
    --keyspace) need_arg "$@"; KS="$2"; KS_EXPLICIT=1; shift 2 ;;
    --table) need_arg "$@"; TBL="$2"; TBL_EXPLICIT=1; shift 2 ;;
    --dump-generations) need_arg "$@"; DUMP_GENERATIONS="$2"; shift 2 ;;
    --min-data-db-bytes) need_arg "$@"; MIN_DATA_DB_BYTES="$2"; MIN_DATA_DB_EXPLICIT=1; shift 2 ;;
    --image) need_arg "$@"; IMAGE="$2"; shift 2 ;;
    --container) need_arg "$@"; CONTAINER="$2"; shift 2 ;;
    --manifest-out) need_arg "$@"; MANIFEST_OUT="$2"; MANIFEST_OUT_EXPLICIT=1; shift 2 ;;
    --publish-manifest) PUBLISH_MANIFEST=1; shift ;;
    --keep-container) KEEP_CONTAINER=1; shift ;;
    --no-prune) PRUNE_STALE=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "$0: unrecognized argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# --smoke is a DEFAULTS override, so it must not silently undo an explicit flag:
# ONLY unset-by-the-caller values are lowered (--rows/--chunk-rows/--keyspace and
# their ROWS/CHUNK_ROWS/KS env equivalents all count as explicit). The smoke
# defaults stay large enough that the 8 MiB Data.db floor and the Rows.db assert
# are genuinely exercised.
[ "$SMOKE" = 0 ] || [ "$SMALL_GOLDEN" = 0 ] \
  || { echo "$0: --smoke and --small-golden are mutually exclusive" >&2; exit 2; }
if [ "$SMOKE" = 1 ]; then
  [ "$ROWS_EXPLICIT" = 1 ] || ROWS="${SMOKE_ROWS:-240000}"
  [ "$CHUNK_ROWS_EXPLICIT" = 1 ] || CHUNK_ROWS="${SMOKE_CHUNK_ROWS:-120000}"
  [ "$KS_EXPLICIT" = 1 ] || KS="perf_bti_smoke"
fi
# --small-golden: a DIFFERENT ARTIFACT from the perf corpus (see the header).
# Same defaults-only discipline as --smoke. One chunk => one SSTable; the 8 MiB
# read-plane floor is deliberately dropped to 0 (this fixture is a correctness
# oracle, never a profile target), and the width mix guarantees at least one
# partition wide enough to populate Rows.db.
#
# SIZED TO THE REPO'S CONVENTION, not to a row count: the closest committed
# analogue is #3032's test_da/multiclustering_table (same PRIMARY KEY
# (pk, bucket, seq) shape) at 468 rows / 3 partitions and a 121,020 B
# `sstabledump -l` golden. A golden's worth as a Cassandra-written oracle does
# NOT scale with row count, but the committed golden's size does (~320 B/row),
# so these defaults are the previous 6000-row shape divided by exactly 10: the
# same width WEIGHTS and therefore the same partition-count / bucket-spread
# structure, one order of magnitude smaller. The widest class (400 rows x ~185 B
# = ~74 KiB) still exceeds the image's `column_index_size` default of 4KiB by
# ~18x, so its partition spans many row-index blocks and Rows.db is populated
# (the fail-closed every-Rows.db-non-empty assert below would catch it if not).
if [ "$SMALL_GOLDEN" = 1 ]; then
  [ "$ROWS_EXPLICIT" = 1 ] || ROWS="${SMALL_GOLDEN_ROWS:-600}"
  [ "$CHUNK_ROWS_EXPLICIT" = 1 ] || CHUNK_ROWS="${SMALL_GOLDEN_CHUNK_ROWS:-600}"
  [ "$KS_EXPLICIT" = 1 ] || KS="test_da"
  [ "$TBL_EXPLICIT" = 1 ] || TBL="wide_multiclustering_small"
  [ "$WIDTHS_EXPLICIT" = 1 ] || WIDTHS="400:20,80:30,20:50"
  [ "$MIN_DATA_DB_EXPLICIT" = 1 ] || MIN_DATA_DB_BYTES=0
fi
[ -n "$KS" ] || KS="perf_bti"

# RUN_MODE is what the manifest records and what gates publication of the
# COMMITTED manifest.
RUN_MODE=production
[ "$SMOKE" = 0 ] || RUN_MODE=smoke
[ "$SMALL_GOLDEN" = 0 ] || RUN_MODE=small_golden

# MANIFEST_OUT resolution (roborev #3234 F2). It used to DEFAULT to the committed
# production manifest, so the advertised `--smoke` invocation silently overwrote a
# committed provenance artifact with perf_bti_smoke metadata — after which the
# default full-corpus scan rejects that manifest as describing another table
# (bti_perf_scan exit 8). Now: nothing outside $OUT is written unless the caller
# says so, and naming the committed manifest additionally requires production mode.
MANIFEST_OUT="${MANIFEST_OUT-}"
if [ "$PUBLISH_MANIFEST" = 1 ]; then
  [ "$MANIFEST_OUT_EXPLICIT" = 0 ] \
    || { echo "$0: --publish-manifest and --manifest-out are mutually exclusive" >&2; exit 2; }
  MANIFEST_OUT="$COMMITTED_MANIFEST"
fi

# ------------------------------------------------------- input validation ----
# Runs BEFORE the container, the load, and any deletion: an unvalidated typo must
# never start a multi-GB run or overwrite the COMMITTED manifest.
is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }

# Canonical (symlink-resolved, `..`-collapsed) form of $1. Components need not
# exist — this runs BEFORE the corpus root is created. `realpath -m` is coreutils;
# python3 (already a hard requirement) is the fallback so no box lacks it.
canon_path() {
  if command -v realpath >/dev/null 2>&1; then
    realpath -m -- "$1"
  else
    python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1"
  fi
}

# Canonical paths this script must never treat as a corpus root: it does
# `rm -rf "$OUT/cassandra-data"` (as root, via $SUDO) and `rm -rf "$OUT/work"`.
# A LEXICAL check on the raw argument is not enough — `/tmp/..`, `/data/../`, and a
# symlink pointing at `/` all pass a `!= "/"` test and then resolve to `/`, which
# would delete an unrelated `/cassandra-data` (roborev #3234 F1). Hence: canonicalize
# FIRST, validate the CANONICAL path, and derive every destructive target from it.
UNSAFE_OUT_ROOTS=(
  / /bin /boot /dev /etc /home /lib /lib32 /lib64 /libx32 /media /mnt /opt
  /proc /root /run /sbin /srv /sys /tmp /usr /var
)

OUT_CANON=""
# Refuse to delete anything that is not a STRICT descendant of the validated
# canonical corpus root. Called immediately before every `rm -rf` in this script,
# so a future destructive target cannot skip the check by construction.
assert_under_out() {  # $1 = path about to be removed
  local target="$1" canon
  [ -n "$OUT_CANON" ] \
    || die "internal error: destructive target '$target' reached before --out was canonicalized"
  canon="$(canon_path "$target")" \
    || die "cannot canonicalize destructive target '$target' — refusing to delete"
  case "$canon" in
    "$OUT_CANON"/?*) : ;;
    *) die "refusing to delete '$target' (resolves to '$canon'): not a strict descendant of the
       validated corpus root '$OUT_CANON'" ;;
  esac
}

CHUNKS=0
validate_inputs() {
  command -v python3 >/dev/null 2>&1 || die "python3 is required (row driver + manifest writer)"
  [ -f "$ROWS_PY" ] || die "missing row driver: $ROWS_PY"
  [ -f "$MANIFEST_PY" ] || die "missing manifest writer: $MANIFEST_PY"
  [[ -n "${OUT// }" ]] || die "--out/OUT is empty"
  # Checked BEFORE canonicalization: `realpath -m` would silently resolve a
  # relative path against $PWD and mask the caller's mistake.
  [[ "$OUT" == /* ]] || die "--out must be an absolute path, got '$OUT'"
  OUT_CANON="$(canon_path "$OUT")" || die "cannot canonicalize --out '$OUT'"
  [[ "$OUT_CANON" == /* ]] || die "--out '$OUT' did not canonicalize to an absolute path ('$OUT_CANON')"
  local unsafe
  for unsafe in "${UNSAFE_OUT_ROOTS[@]}"; do
    [[ "$OUT_CANON" != "$unsafe" ]] || die "refusing to use '$OUT' as --out: it resolves to '$OUT_CANON',
       a system root. This script does 'rm -rf \$OUT/cassandra-data' (as root) and
       'rm -rf \$OUT/work', so a root-resolving --out would delete unrelated paths.
       Point --out at a dedicated directory, e.g. /data/corpus-3234-bti."
  done
  # From here on the CANONICAL path is the corpus root: every destructive target
  # (cassandra-data, work, the published <table>-<uuid> dir) is derived from it,
  # never from the raw argument.
  OUT="$OUT_CANON"
  [[ "$KS" =~ ^[a-z_][a-z0-9_]*$ ]] || die "invalid keyspace '$KS' (unquoted CQL identifier expected)"
  [[ "$TBL" =~ ^[a-z_][a-z0-9_]*$ ]] || die "invalid table '$TBL' (unquoted CQL identifier expected)"
  for pair in "rows:$ROWS" "chunk-rows:$CHUNK_ROWS" "payload-bytes:$PAYLOAD_BYTES" \
              "dump-generations:$DUMP_GENERATIONS" "min-data-db-bytes:$MIN_DATA_DB_BYTES" \
              "copy-processes:$COPY_PROCESSES" "chunk-length-in-kb:$CHUNK_LENGTH_IN_KB"; do
    is_uint "${pair#*:}" || die "--${pair%%:*} must be a non-negative integer, got '${pair#*:}'"
  done
  [ "$ROWS" -ge 1 ] || die "--rows must be >= 1"
  [ "$CHUNK_ROWS" -ge 1 ] || die "--chunk-rows must be >= 1"
  [ "$CHUNK_ROWS" -le "$ROWS" ] || die "--chunk-rows ($CHUNK_ROWS) exceeds --rows ($ROWS)"
  [ "$PAYLOAD_BYTES" -ge 8 ] || die "--payload-bytes must be >= 8"
  [ "$COPY_PROCESSES" -ge 1 ] || die "--copy-processes must be >= 1"
  [[ -n "${SEED// }" ]] || die "--seed is empty (the seed is the corpus's reproducibility key)"
  # Delegate the --widths/--buckets grammar to the one parser that owns it.
  python3 - "$WIDTHS" "$BUCKETS" "$ROWS_PY" <<'PYEOF' || die "invalid --widths/--buckets (see message above)"
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rows", sys.argv[3])
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.parse_widths(sys.argv[1])
mod.parse_buckets(sys.argv[2])
PYEOF
  CHUNKS=$(( (ROWS + CHUNK_ROWS - 1) / CHUNK_ROWS ))
  # `pk` is a CQL `int`: chunk N's keys start at N * PK_STRIDE, so a plan with too
  # many chunks cannot be represented. Checked HERE, before the container and the
  # multi-GB load: an over-ceiling plan previously died at chunk 3 of 27 with a
  # cqlsh ParseError, four minutes and three SSTables in (issue #3234). The
  # arithmetic and the stride live in the row driver, the one module that owns them.
  #
  # WHY `pk int` + a CEILING GUARD, and not `pk bigint` (which would make the guard
  # unnecessary): the partition-key TYPE is part of the fixture's shape, and this
  # corpus deliberately mirrors #3032's `test_da/multiclustering_table` — `pk int`,
  # compound `(bucket text, seq int)` clustering — so a BTI read-path measurement
  # taken here is comparable to the correctness fixture the same code paths are
  # validated on, byte-comparable key encoding included (a 4-byte Int32Type key,
  # not an 8-byte LongType one). Widening to `bigint` would silently change the
  # partition-key encoding under test to buy key space no profileable corpus needs
  # (the stride admits 2147 chunks). The cost of keeping `int` is exactly one
  # fail-closed arithmetic check, run before anything expensive — which is cheaper
  # than a fixture that no longer matches the shape it is supposed to represent.
  python3 - "$ROWS_PY" "$CHUNKS" "$CHUNK_ROWS" <<'PYEOF' || die "plan exceeds the \`pk int\` ceiling (see message above)"
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rows", sys.argv[1])
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.plan_fits_int32(int(sys.argv[2]), int(sys.argv[3]))
PYEOF
  # Replacing the COMMITTED production manifest is production-mode ONLY. A smoke /
  # small-golden manifest describes ANOTHER table, and the AC3 scan harness then
  # rejects the committed manifest as foreign (exit 8) — a live footgun, not a
  # cosmetic one (roborev #3234 F2).
  if [ -n "$MANIFEST_OUT" ]; then
    local committed_canon
    # Canonical from here on, so the reported target and the eventual `cp`
    # destination are the same unambiguous path.
    MANIFEST_OUT="$(canon_path "$MANIFEST_OUT")" \
      || die "cannot canonicalize --manifest-out '$MANIFEST_OUT'"
    committed_canon="$(canon_path "$COMMITTED_MANIFEST")" \
      || die "cannot canonicalize the committed manifest path '$COMMITTED_MANIFEST'"
    [ "$MANIFEST_OUT" != "$committed_canon" ] || [ "$RUN_MODE" = production ] \
      || die "refusing to write the COMMITTED production manifest from a $RUN_MODE run:
       $committed_canon describes the production corpus, and $RUN_MODE metadata would make
       the default full-corpus scan reject it as describing another table (exit 8).
       Drop --publish-manifest/--manifest-out, or point --manifest-out somewhere else."
  fi
  log "validated: rows=$ROWS chunk_rows=$CHUNK_ROWS chunks=$CHUNKS seed=$SEED ks=$KS tbl=$TBL out=$OUT"
}

# Resolved (symlink-free) corpus keyspace dir; "" when it does not exist yet.
corpus_keyspace_dir() {
  local root
  root="$(cd "$OUT" 2>/dev/null && pwd -P)" || return 0
  [[ -n "$root" && "$root" != "/" ]] || die "--out resolved to '/' — refusing"
  printf '%s/sstables/%s' "$root" "$KS"
}

# Remove PREVIOUS <table>-<uuid> dirs so repeated regenerations cannot leave
# several multi-GB copies while the manifest describes only the last. Deliberately
# narrow (this deletes multi-GB paths): only DIRECT children of
# $OUT/sstables/$KS, only exact "<table>-<32 hex>" names, never a symlink, never a
# path resolving outside that keyspace dir, never the dir just published.
prune_stale_table_dirs() {  # $1 = basename to KEEP ("" keeps none)
  local keep="${1:-}" ks_dir d base real had_nullglob=0
  ks_dir="$(corpus_keyspace_dir)"
  [[ -n "$ks_dir" && -d "$ks_dir" ]] || return 0
  shopt -q nullglob && had_nullglob=1
  shopt -s nullglob
  for d in "$ks_dir/$TBL"-*; do
    base="$(basename "$d")"
    [[ -d "$d" ]] || continue
    if [[ -L "$d" ]]; then
      log "[prune] skipping symlink (never followed): $d"
      continue
    fi
    if [[ ! "$base" =~ ^${TBL}-[0-9a-f]{32}$ ]]; then
      log "[prune] skipping '$base' (not a <table>-<uuid> corpus dir)"
      continue
    fi
    [[ -n "$keep" && "$base" == "$keep" ]] && continue
    real="$(cd "$d" && pwd -P)" || die "prune: cannot resolve $d"
    [[ "$real" == "$ks_dir/$base" ]] \
      || die "prune: '$d' resolves OUTSIDE the corpus keyspace dir ($real) — refusing to delete"
    if [[ "$PRUNE_DRY_RUN" == 1 ]]; then
      echo "WOULD-PRUNE $real"
      continue
    fi
    assert_under_out "$real"
    log "[prune] removing stale corpus dir $real"
    $SUDO rm -rf -- "$real"
  done
  [[ "$had_nullglob" == 1 ]] || shopt -u nullglob
}

# ------------------------------------------------- fail-closed assertions ----
# Host-side, so --verify-only can re-run every file-level assert against an
# already-published corpus with no container. These ARE issue #3234's ACs; each
# is a hard failure with an actionable message, never a warning.
assert_corpus() {  # $1 = published sstable dir
  local dest="$1" f base gens=0 max_data=0 sz rows_db gen_ids=()
  [ -d "$dest" ] || die "no published corpus dir at $dest"

  # AC1: `da` descriptors only. A stray `nb-*` means a yaml setting did not take;
  # that is the exact silent failure the two settings guard against.
  local foreign
  foreign="$(find "$dest" -maxdepth 1 -type f -name '*.db' ! -name 'da-*-bti-*' -printf '%f\n' | sort || true)"
  [ -z "$foreign" ] || die "AC1: non-BTI descriptor(s) in $dest: $(tr '\n' ' ' <<<"$foreign")
       A stock Cassandra 5.0 node emits 'nb' (BIG). BOTH cassandra.yaml settings
       (storage_compatibility_mode: NONE and sstable.selected_format: bti) must be
       applied AND the node restarted BEFORE the table is created."

  shopt -s nullglob
  local datas=("$dest"/da-*-bti-Data.db)
  shopt -u nullglob
  [ ${#datas[@]} -ge 1 ] || die "AC1: no da-*-bti-Data.db in $dest"

  for f in "${datas[@]}"; do
    base="$(basename "$f" -Data.db)"
    gens=$((gens + 1))
    # The generation identifier, for the one-SSTable-per-chunk mapping check below.
    # The descriptor is `da-<gen>-bti`; anything else already failed the AC1 glob.
    local gen_id="${base#da-}"; gen_id="${gen_id%-bti}"
    [[ "$gen_id" =~ ^[0-9]+$ ]] \
      || die "AC: cannot read the generation number out of descriptor '$base'
       (expected da-<gen>-bti-Data.db); the one-SSTable-per-chunk mapping is checked on it"
    gen_ids+=("$gen_id")
    sz=$(stat -c %s "$f")
    if [ "$sz" -gt "$max_data" ]; then max_data=$sz; fi

    # AC2 (second half): a row index must actually exist for every SSTable —
    # partitions wide enough to span >1 row-index block are the whole point.
    rows_db="$dest/$base-Rows.db"
    [ -f "$rows_db" ] || die "AC2: $base has no Rows.db"
    local rsz
    rsz=$(stat -c %s "$rows_db")
    [ "$rsz" -ge 1 ] || die "AC2: $base Rows.db is EMPTY — no partition exceeded
       column_index_size, so there is no row-index trie to profile. Raise the
       rows-per-partition classes in --widths or --payload-bytes."

    # AC: TOC contract for BTI — Partitions.db/Rows.db present, the BIG-only
    # Index.db/Summary.db absent. Read from the TOC, not from the directory
    # listing, so a stray file cannot fake it.
    local toc="$dest/$base-TOC.txt"
    [ -f "$toc" ] || die "AC: $base has no TOC.txt"
    local comp
    for comp in Data.db Partitions.db Rows.db Statistics.db CompressionInfo.db Filter.db TOC.txt; do
      grep -qxF "$comp" "$toc" || die "AC: $base TOC.txt is missing $comp (TOC: $(tr '\n' ' ' <"$toc"))"
    done
    for comp in Index.db Summary.db; do
      ! grep -qxF "$comp" "$toc" || die "AC: $base TOC.txt lists $comp — that is a BIG-format
       component; this is not a BTI SSTable (TOC: $(tr '\n' ' ' <"$toc"))"
      [ ! -f "$dest/$base-$comp" ] || die "AC: $base has a $comp file (BIG-only component present)"
    done
    # ...and the TOC is only a MANIFEST: every component it lists must EXIST as a
    # regular file, and the directory must hold no component the TOC does not list
    # (roborev #3234 M3). This loop used to read the TOC alone, so deleting
    # Statistics.db, CompressionInfo.db, Partitions.db or Filter.db while leaving the
    # TOC untouched still printed VERIFY-OK — a fail-closed hole in the verifier
    # itself, and exactly the shape a half-copied or half-pruned corpus has. Both
    # directions are checked: a TOC entry with no file is a missing component, a file
    # with no TOC entry is a component Cassandra will not open.
    local toc_sorted disk_sorted
    toc_sorted="$(grep -v '^[[:space:]]*$' "$toc" | sort -u)"
    while IFS= read -r comp; do
      [ -n "$comp" ] || continue
      [ -f "$dest/$base-$comp" ] || die "AC: $base-TOC.txt lists $comp but $base-$comp is
       not a regular file. The TOC is a manifest, not evidence: a component deleted (or
       never written) while the TOC still advertises it fails at OPEN time in Cassandra
       and CQLite, so it must fail HERE. (TOC: $(tr '\n' ' ' <"$toc"))"
    done <<<"$toc_sorted"
    # The on-disk component set, excluding the sstabledump goldens (`*-Data.db.jsonl`),
    # which are derived JSON and deliberately not SSTable components.
    disk_sorted="$(find "$dest" -maxdepth 1 -type f -name "$base-*" ! -name '*.jsonl' \
      -printf '%f\n' | sed "s/^$base-//" | sort -u)"
    if [ "$disk_sorted" != "$toc_sorted" ]; then
      die "AC: $base component set disagrees with its TOC.txt.
       TOC lists : $(tr '\n' ' ' <<<"$toc_sorted")
       on disk   : $(tr '\n' ' ' <<<"$disk_sorted")
       Both directions are a hard failure: a TOC entry with no file is a missing
       component, and a component file the TOC does not list is one Cassandra will not
       open (and one this manifest would not describe)."
    fi
    log "  [assert] $base: Data.db $sz B, Rows.db $rsz B, TOC ok ($(wc -l <"$toc" | tr -d ' ') components)"
  done

  # AC2 (first half): the 8 MiB read-plane floor.
  [ "$max_data" -gt "$MIN_DATA_DB_BYTES" ] || die "AC2: largest Data.db is $max_data B, needs > $MIN_DATA_DB_BYTES B.
       Below 8 MiB MADV_RANDOM is not applied and the point-read/scan mappings are
       the SAME mapping, so a read-plane A/B measures nothing. Raise --chunk-rows."

  # AC: ONE SSTABLE PER CHUNK, and the generations are the flush order 1..CHUNKS
  # (roborev #3234 M2). The aggregate row/partition cross-checks in the manifest writer
  # CANNOT see this: an unexpected flush split (two SSTables for one chunk) or a
  # compaction (one for two chunks) keeps every row and every partition while destroying
  # the promised shape. And it is not only shape — the GENERATION COUNT selects the scan
  # route and is what the AC3 throughput figure is attributed to ("27 generations,
  # generation_merge::stream_generations_for_read"), so a corpus with a different
  # generation count would silently make that attribution wrong.
  local want_gens_sorted have_gens_sorted
  want_gens_sorted="$(seq 1 "$CHUNKS" | sort -n | tr '\n' ' ')"
  have_gens_sorted="$(printf '%s\n' "${gen_ids[@]}" | sort -n | tr '\n' ' ')"
  [ "$gens" -eq "$CHUNKS" ] || die "AC: $gens SSTable(s) in $dest, but this configuration plans
       $CHUNKS chunk(s) (--rows $ROWS / --chunk-rows $CHUNK_ROWS) and the generator flushes
       ONCE PER CHUNK, so the two must be equal. Fewer means a compaction merged chunks
       (autocompaction must be disabled BEFORE the first load); more means a chunk was
       split across flushes. If you are re-verifying a corpus that was generated with
       OTHER flags, pass the same --rows/--chunk-rows to --verify-only."
  [ "$have_gens_sorted" = "$want_gens_sorted" ] || die "AC: generation mapping — expected generations
       1..$CHUNKS (one per chunk, in flush order), got: $have_gens_sorted
       A gap or an offset is evidence of a compaction (its output is promoted to a new,
       higher generation and the inputs are removed) or of a table that was not freshly
       created. The generation count is what the AC3 figure is attributed to."
  log "[assert] $gens SSTable(s); largest Data.db $max_data B > floor $MIN_DATA_DB_BYTES B; every Rows.db non-empty"
  log "[assert] one SSTable per chunk: $gens == $CHUNKS chunk(s); generations $have_gens_sorted"
  ASSERT_SSTABLE_COUNT=$gens
  ASSERT_MAX_DATA=$max_data
}

# ------------------------------------------------------------- preflight -----
preflight() {
  $DOCKER version >/dev/null 2>&1 \
    || die "cannot run '$DOCKER' — install docker and/or set DOCKER=docker (or grant passwordless sudo)"
  local parent need_gib avail_gib
  parent="$(dirname "$OUT")"
  mkdir -p "$OUT" 2>/dev/null || $SUDO mkdir -p "$OUT" \
    || die "cannot create --out $OUT"
  # Data (~250 B/row worst case, uncompressed) + the live Cassandra data dir copy
  # + one CSV chunk + goldens (~1.7x the dumped Data.db).
  need_gib=$(( (ROWS * 250 * 2) / 1073741824 + 4 ))
  avail_gib=$(df -BG --output=avail "$OUT" | tail -1 | tr -dc '0-9')
  log "free space under $parent: ${avail_gib} GiB (need >= ${need_gib} GiB)"
  [ "${avail_gib:-0}" -ge "$need_gib" ] || die "insufficient free space under $parent"
}

wait_ready() {  # $1 = label, $2 = max attempts (5s each)
  local label="$1" max="${2:-36}" i
  log "waiting for Cassandra ($label, max $((max * 5))s)..."
  for i in $(seq 1 "$max"); do
    # cqlsh, NOT `nodetool status`: nodetool answers before CQL is accepting.
    if $DOCKER exec "$CONTAINER" cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra ready ($label) after ~$((i * 5))s"
      return 0
    fi
    sleep 5
  done
  die "Cassandra not ready ($label) after $((max * 5))s"
}

cql() { $DOCKER exec "$CONTAINER" cqlsh -e "$1"; }

# ------------------------------------------------------------- container -----
DATA_DIR=""
start_container() {
  log "removing any stale container + data dir..."
  $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  # Derived from the VALIDATED CANONICAL root (validate_inputs replaced $OUT with
  # its canonical form) and re-checked here, because the next line is a privileged
  # recursive delete (roborev #3234 F1).
  DATA_DIR="$OUT/cassandra-data"
  assert_under_out "$DATA_DIR"
  $SUDO rm -rf -- "$DATA_DIR"
  # MUST exist and be owned by uid 999 (the image's `cassandra` user) BEFORE
  # `docker run`, or the node never starts on the bind mount.
  $SUDO mkdir -p "$DATA_DIR"
  $SUDO chown -R 999:999 "$DATA_DIR"
  log "starting $IMAGE as $CONTAINER (heap $MAX_HEAP, data at $DATA_DIR)..."
  # shellcheck disable=SC2086  # DOCKER_RUN_OPTS is a deliberate word-split knob
  $DOCKER run -d --name "$CONTAINER" \
    -e MAX_HEAP_SIZE="$MAX_HEAP" -e HEAP_NEWSIZE="$HEAP_NEW" \
    -e CASSANDRA_NUM_TOKENS=1 \
    -v "$DATA_DIR:/var/lib/cassandra" \
    $DOCKER_RUN_OPTS "$IMAGE" >/dev/null
  # Let the FIRST boot finish before reconfiguring — restarting mid-boot stalls it.
  wait_ready "initial boot" 36
}

# The two mandatory yaml settings, applied and then grep-VERIFIED. In the shipped
# cassandra.yaml `sstable:` / `selected_format: big` are COMMENTED OUT (~:1142)
# and `storage_compatibility_mode: CASSANDRA_4` is live (~:2249). A missed edit
# emits `nb` with no error at all, so both greps are hard failures.
#
# The sed expression and the three verification greps live in ONE snippet-emitting
# function because the `sed` addresses depend on the shipped file's EXACT
# indentation (`#  selected_format: big`, two spaces) — text nothing verifies until
# it runs. That snippet is executed:
#   * in the generating container by apply_bti_yaml (the production path), and
#   * against the committed Cassandra 5.0.2 cassandra.yaml excerpt
#     (scripts/tests/fixtures/cassandra-5.0.2-cassandra.yaml.excerpt) by
#     scripts/tests/test_gen_perf_corpus_bti.sh, through --yaml-flip-check,
# so the tested text IS the text that runs: no drifting copy of the expression.
bti_yaml_flip_snippet() {  # $1 = path to a cassandra.yaml, EDITED IN PLACE
  local yaml="$1"
  cat <<SNIPPET
[ -f '$yaml' ] || { echo "YAML-FLIP-FATAL: no such cassandra.yaml: $yaml" >&2; exit 1; }
sed -i 's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g; s|#sstable:|sstable:|; s|#  selected_format: big|  selected_format: bti|' '$yaml' || exit 1
grep -qE '^storage_compatibility_mode: NONE\$' '$yaml' || {
  echo "YAML-FLIP-FATAL: storage_compatibility_mode was NOT set to NONE in $yaml — the node would emit 'nb' (BIG) silently" >&2; exit 1; }
grep -qE '^sstable:\$' '$yaml' || {
  echo "YAML-FLIP-FATAL: the sstable: block was NOT uncommented in $yaml — the node would emit 'nb' (BIG) silently" >&2; exit 1; }
grep -qE '^  selected_format: bti\$' '$yaml' || {
  echo "YAML-FLIP-FATAL: sstable.selected_format was NOT set to bti in $yaml — the node would emit 'nb' (BIG) silently" >&2; exit 1; }
grep -nE '^storage_compatibility_mode:|^sstable:|^  selected_format:' '$yaml'
SNIPPET
}

YAML_VERIFIED=""
apply_bti_yaml() {
  local yaml=/etc/cassandra/cassandra.yaml
  log "applying storage_compatibility_mode: NONE + sstable.selected_format: bti ..."
  YAML_VERIFIED="$($DOCKER exec "$CONTAINER" bash -lc "$(bti_yaml_flip_snippet "$yaml")")" \
    || die "the cassandra.yaml BTI flip FAILED (see the YAML-FLIP-FATAL line above): without
       BOTH storage_compatibility_mode: NONE and sstable.selected_format: bti the node
       emits 'nb' (BIG) with no error at all"
  log "yaml verified:"; printf '%s\n' "$YAML_VERIFIED" | sed 's/^/    /'
  log "restarting container to apply BTI mode..."
  $DOCKER restart "$CONTAINER" >/dev/null
  wait_ready "BTI mode" 36
}

create_schema() {
  log "creating $KS.$TBL (compound clustering key, LZ4 chunk_length_in_kb=$CHUNK_LENGTH_IN_KB)..."
  # durable_writes=false is GENERATION-TIME ONLY (no commitlog write
  # amplification); it does not change a single byte written into the SSTable.
  cql "CREATE KEYSPACE IF NOT EXISTS $KS WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes = false;"
  cql "CREATE TABLE IF NOT EXISTS $KS.$TBL (
         pk int,
         bucket text,
         seq int,
         payload text,
         PRIMARY KEY (pk, bucket, seq)
       ) WITH CLUSTERING ORDER BY (bucket ASC, seq ASC)
         AND compression = {'class':'LZ4Compressor','chunk_length_in_kb':$CHUNK_LENGTH_IN_KB}
         AND compaction = {'class':'SizeTieredCompactionStrategy'};"
  # BEFORE any load: otherwise STCS merges the chunks and the multi-SSTable shape
  # (one SSTable per flushed chunk) is lost.
  log "disabling autocompaction for $KS.$TBL (keeps one SSTable per chunk)..."
  $DOCKER exec "$CONTAINER" nodetool disableautocompaction "$KS" "$TBL"
}

WORK=""
PLAN=""
load_chunks() {
  WORK="$OUT/work"
  assert_under_out "$WORK"
  rm -rf -- "$WORK" 2>/dev/null || $SUDO rm -rf -- "$WORK"
  mkdir -p "$WORK"
  PLAN="$WORK/row-plan.jsonl"
  : >"$PLAN"
  local i remaining=$ROWS n csv t0 t1 imported expect
  t0=$(date +%s)
  for (( i=0; i<CHUNKS; i++ )); do
    n=$CHUNK_ROWS
    [ "$remaining" -lt "$n" ] && n=$remaining
    csv="$WORK/chunk-$i.csv"
    log "[chunk $i/$((CHUNKS - 1))] generating $n rows (seed $SEED:$i)..."
    python3 "$ROWS_PY" --chunk-index "$i" --rows "$n" --seed "$SEED" \
      --payload-bytes "$PAYLOAD_BYTES" --widths "$WIDTHS" --buckets "$BUCKETS" \
      --out "$csv" --plan-out "$PLAN" \
      || die "[chunk $i] row generation failed"
    $DOCKER cp "$csv" "$CONTAINER:/tmp/chunk.csv"
    log "[chunk $i] COPY FROM (NUMPROCESSES=$COPY_PROCESSES)..."
    $DOCKER exec "$CONTAINER" cqlsh --request-timeout="$COPY_TIMEOUT" -e \
      "COPY $KS.$TBL(pk,bucket,seq,payload) FROM '/tmp/chunk.csv' WITH HEADER=false AND NUMPROCESSES=$COPY_PROCESSES AND MAXBATCHSIZE=50;" \
      >"$WORK/copy-$i.log" 2>&1 || { tail -30 "$WORK/copy-$i.log" >&2; die "[chunk $i] COPY failed"; }
    # cqlsh COPY can report failures and still exit 0 — parse the summary line.
    if grep -qiE 'failed to (import|process)|^Failed ' "$WORK/copy-$i.log"; then
      tail -30 "$WORK/copy-$i.log" >&2
      die "[chunk $i] COPY reported import failures (see $WORK/copy-$i.log)"
    fi
    imported="$(grep -oE '[0-9][0-9,]* rows imported' "$WORK/copy-$i.log" \
      | tail -1 | tr -d ',' | awk '{print $1}')"
    expect="$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['rows'])" "$(tail -1 "$PLAN")")"
    [ -n "$imported" ] || { tail -30 "$WORK/copy-$i.log" >&2; die "[chunk $i] could not read the 'rows imported' count from COPY output"; }
    [ "$imported" = "$expect" ] \
      || die "[chunk $i] COPY imported $imported rows, the CSV held $expect (partial load)"
    log "[chunk $i] imported $imported rows; flushing (this is what makes it its own SSTable)..."
    $DOCKER exec "$CONTAINER" nodetool flush "$KS" "$TBL"
    rm -f "$csv"
    remaining=$((remaining - n))
  done
  t1=$(date +%s)
  log "load complete: $ROWS rows in $((t1 - t0))s ($((ROWS / ((t1 - t0) > 0 ? (t1 - t0) : 1))) rows/s)"
}

# The captured schema lands under $WORK, NOT in the published corpus (roborev #3234 F3).
# It used to be written straight to $OUT/schema.cql BEFORE publish() installed the
# in-progress marker, so a publish that died early — no SSTable dir in the container, a
# missing host bind-mount dir — left the NEW schema published beside the PREVIOUS run's
# manifest and SSTables, with nothing marking the corpus as mid-generation. That is the
# same stale-provenance window the marker exists to close, reopened through a different
# file. install_schema() puts it in place only after the marker is installed.
capture_schema() {  # $1 = destination (under $WORK)
  local out="$1" tmp
  tmp="$(mktemp)"
  $DOCKER exec "$CONTAINER" cqlsh -e "DESCRIBE KEYSPACE $KS;" >"$tmp" 2>/dev/null \
    || die "could not DESCRIBE KEYSPACE $KS (schema.cql is required for a reproducible manifest)"
  grep -q "^CREATE KEYSPACE $KS " "$tmp" || die "DESCRIBE KEYSPACE $KS produced no CREATE KEYSPACE"
  grep -q "^CREATE TABLE $KS\." "$tmp" || die "DESCRIBE KEYSPACE $KS produced no CREATE TABLE"
  mv "$tmp" "$out"
  chmod 0644 "$out"
  log "captured schema -> $out ($(wc -c <"$out" | tr -d ' ') bytes)"
}

# Install the captured schema into the published corpus: the corpus root (where
# bti_perf_scan reads it) and the SSTable dir (where the manifest writer reads it).
# Called only AFTER publish() has installed the in-progress marker and populated $DEST,
# so the published schema and the published bytes are never one generation apart.
install_schema() {  # $1 = the captured schema under $WORK
  local src="$1"
  [ -f "$src" ] || die "install_schema: no captured schema at $src"
  [ -n "$DEST" ] && [ -d "$DEST" ] || die "install_schema: publish() has not populated \$DEST"
  cp "$src" "$OUT/schema.cql" || die "cannot install the schema at $OUT/schema.cql"
  cp "$src" "$DEST/schema.cql" || die "cannot install the schema at $DEST/schema.cql"
  log "schema installed -> $OUT/schema.cql and $DEST/schema.cql"
}

# The corpus-local manifest is the FIRST candidate bti_perf_scan reads (ahead of the
# committed one), so it is the corpus's authoritative provenance. It must never
# describe bytes other than the ones sitting beside it.
LOCAL_MANIFEST_NAME="manifest-bti-3234.json"
# Presence of this key is what makes the marker below UNREADABLE as a manifest. The
# harness refuses on the KEY, not on its value (bti_perf_scan::read_manifest_rows):
# a field is observed or absent, and "in progress" is not a row count.
IN_PROGRESS_KEY="generation_in_progress"

# Replace the corpus-local manifest with an IN-PROGRESS marker BEFORE the published
# corpus is mutated (roborev #3234 M2).
#
# publish() replaced/pruned the SSTable dir and only then, several steps later, wrote
# the manifest — so a failure in ANY of the steps between (the file-level asserts, the
# sstabledump goldens, the sstablemetadata readback, the manifest write itself) left
# the PREVIOUS run's manifest sitting beside the NEW corpus, in the exact path
# bti_perf_scan treats as most-specific-and-authoritative. That manifest is
# syntactically perfect and describes different bytes: row count, sha256s, generation
# list and sstable_dir all belong to the corpus that was just deleted. Nothing
# downstream could detect it.
#
# So the authoritative position is vacated first and made FAIL-CLOSED: the old
# manifest is moved aside for forensics under a name the harness never looks for, and
# a marker carrying $IN_PROGRESS_KEY (and deliberately NO keyspace/table/row count)
# takes its place. A run that dies anywhere after this point therefore leaves a
# corpus whose provenance says "generation did not finish", and the next harness run
# REFUSES (exit 8) instead of reading stale numbers. write_manifest() ends the window
# by renaming the finished manifest over the marker in one atomic step.
quarantine_local_manifest() {
  local m="$OUT/$LOCAL_MANIFEST_NAME" stamp aside
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  if [ -e "$m" ]; then
    aside="$m.superseded-$stamp"
    mv -f -- "$m" "$aside" 2>/dev/null || $SUDO mv -f -- "$m" "$aside" \
      || die "cannot move the previous corpus manifest aside ($m -> $aside).
       It describes the corpus that is about to be REPLACED, so leaving it in place
       would publish stale provenance beside new bytes."
    log "[publish] previous corpus manifest moved aside -> $(basename "$aside")"
  fi
  cat >"$m" <<EOF || die "cannot write the in-progress manifest marker $m"
{
  "issue": 3234,
  "$IN_PROGRESS_KEY": true,
  "started_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "note": "A corpus generation is in progress (or FAILED) in this directory. This file is a MARKER, not a manifest: it carries no keyspace, table or row count, so any consumer that reads corpus provenance from it must refuse rather than measure. gen-perf-corpus-bti.sh writes it before it mutates the published SSTable directory and replaces it with the real manifest atomically on success (roborev #3234 M2).",
  "if_you_are_seeing_this": "the generation did not reach write_manifest. Re-run: bash test-data/scripts/gen-perf-corpus-bti.sh --out $OUT --keyspace $KS --table $TBL",
  "previous_manifest_if_any": "$LOCAL_MANIFEST_NAME.superseded-*"
}
EOF
  log "[publish] authoritative manifest position now holds an IN-PROGRESS marker"
}

CONTAINER_SSTABLE_DIR=""
DEST=""
publish() {
  # FIRST — before anything about this publish can fail, and before ANY byte of the
  # published corpus (schema included) is touched (roborev #3234 M2, tightened by F3).
  # The two lookups below can each die: `ls -d` finding no table directory in the
  # container, or the host bind-mount dir being absent. Quarantining after them left a
  # window in which the corpus root already held the NEW schema while the PREVIOUS run's
  # manifest and SSTables were still sitting there, authoritative. The invariant is now
  # simply stated: from the moment publish() starts until write_manifest() renames the
  # finished manifest into place, the corpus's provenance says "generation in progress".
  quarantine_local_manifest
  mkdir -p "$OUT/sstables/$KS"
  # `|| CONTAINER_SSTABLE_DIR=""` is required, not defensive: under `set -e` an
  # assignment from a FAILING command substitution exits the script immediately, so the
  # guard below — the line whose whole purpose is to diagnose "the table directory is not
  # there" — was unreachable, and the real failure mode (an `ls -d` glob matching
  # nothing) killed the run with a bare `exit 1` and no message at all.
  CONTAINER_SSTABLE_DIR="$($DOCKER exec "$CONTAINER" bash -lc \
    "ls -d /var/lib/cassandra/data/$KS/$TBL-* | head -1" | tr -d '\r')" \
    || CONTAINER_SSTABLE_DIR=""
  [ -n "$CONTAINER_SSTABLE_DIR" ] || die "no SSTable dir for $KS.$TBL in the container
       (the node wrote no table directory under /var/lib/cassandra/data/$KS, or the
       keyspace/table names do not match what was created). The published corpus keeps
       its IN-PROGRESS marker, so nothing can read provenance for it."
  local name host_dir
  name="$(basename "$CONTAINER_SSTABLE_DIR")"
  host_dir="$DATA_DIR/data/$KS/$name"
  [ -d "$host_dir" ] || die "host bind-mount dir missing: $host_dir"
  DEST="$OUT/sstables/$KS/$name"
  if [ "$PRUNE_STALE" = 1 ]; then prune_stale_table_dirs "$name"; fi
  assert_under_out "$DEST"
  $SUDO rm -rf -- "$DEST"
  mkdir -p "$DEST"
  # Hardlink on the same filesystem (instant, and the corpus survives deletion of
  # the Cassandra data dir); fall back to a copy across filesystems.
  $SUDO cp -l "$host_dir"/da-*-bti-* "$DEST/" 2>/dev/null \
    || $SUDO cp "$host_dir"/da-*-bti-* "$DEST/" \
    || die "no da-*-bti-* components in $host_dir (did the yaml flip take?)"
  # Cassandra wrote these as uid 999; reclaim before writing goldens INTO $DEST.
  $SUDO chown -R "$(id -u):$(id -g)" "$DEST"
  log "published -> $DEST"
}

dump_goldens() {
  DUMPED=()
  [ "$DUMP_GENERATIONS" -gt 0 ] || { log "golden generation disabled (--dump-generations 0)"; return 0; }
  local n=0 f base stem
  shopt -s nullglob
  for f in "$DEST"/da-*-bti-Data.db; do
    [ "$n" -lt "$DUMP_GENERATIONS" ] || break
    base="$(basename "$f")"
    stem="${base%-Data.db}"
    log "[golden] sstabledump -l $base (bounded subset: $((n + 1))/$DUMP_GENERATIONS)..."
    # sstabledump is not on $PATH in the image; absolute path required.
    $DOCKER exec "$CONTAINER" bash -lc \
      "/opt/cassandra/tools/bin/sstabledump '$CONTAINER_SSTABLE_DIR/$base' -l" >"$DEST/$base.jsonl" \
      || die "[golden] sstabledump failed for $base"
    [ -s "$DEST/$base.jsonl" ] || die "[golden] sstabledump -l produced an EMPTY golden for $base"
    DUMPED+=("$stem")
    n=$((n + 1))
  done
  shopt -u nullglob
  [ "${#DUMPED[@]}" -ge 1 ] || die "[golden] no Data.db to dump"
}

# Row count loaded == row count OBSERVED by sstabledump, for each dumped
# generation. Both sides are read back: the left from that SSTable's own
# Statistics.db (Cassandra's sstablemetadata), the right by counting rows in the
# JSONL. A mismatch means the golden does not describe the bytes.
verify_dumped_row_counts() {
  [ "${#DUMPED[@]}" -ge 1 ] || return 0
  local stem base meta_rows dump_rows meta_out meta_rc
  for stem in "${DUMPED[@]}"; do
    base="$stem-Data.db"
    # The EXIT STATUS is checked EXPLICITLY, before the output is parsed (roborev #3234
    # M1, the shell half): `sstablemetadata` can print a complete-looking `totalRows:`
    # line and still fail afterwards, and cross-checking against the output of a command
    # that did not succeed proves nothing. stderr is kept (it was `2>/dev/null`) so the
    # failure is diagnosable rather than silent.
    meta_out="$($DOCKER exec "$CONTAINER" bash -lc \
      "/opt/cassandra/tools/bin/sstablemetadata '$CONTAINER_SSTABLE_DIR/$base'" 2>&1)" \
      && meta_rc=0 || meta_rc=$?
    [ "$meta_rc" -eq 0 ] || die "sstablemetadata FAILED for $base (exit $meta_rc) — refusing to
       cross-check row counts against the output of a command that did not succeed.
       Output: $(printf '%s' "$meta_out" | tail -5 | tr '\n' ' ')"
    meta_rows="$(printf '%s\n' "$meta_out" \
      | sed -n 's/^totalRows: \([0-9]\+\)$/\1/p' | head -1)"
    [ -n "$meta_rows" ] || die "could not read totalRows from sstablemetadata for $base"
    dump_rows="$(python3 -c '
import json, sys
rows = 0
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    for r in json.loads(line).get("rows", []):
        if r.get("type") == "row":
            rows += 1
print(rows)' "$DEST/$base.jsonl")"
    [ "$meta_rows" = "$dump_rows" ] \
      || die "row-count mismatch for $base: Statistics.db totalRows=$meta_rows, sstabledump rows=$dump_rows"
    log "[assert] $base: Statistics.db totalRows == sstabledump rows == $dump_rows"
  done
}

write_manifest() {
  local yaml_file="$WORK/yaml-verified.txt"
  printf '%s\n' "$YAML_VERIFIED" >"$yaml_file"
  local mode="$RUN_MODE"
  # Written to a sibling temp and RENAMED into place, so the authoritative path only
  # ever holds the in-progress marker or a COMPLETE manifest — never a half-written
  # one (roborev #3234 M2). Same directory, so the rename is atomic.
  local final="$OUT/$LOCAL_MANIFEST_NAME" staged="$OUT/.$LOCAL_MANIFEST_NAME.staged.$$"
  rm -f -- "$staged"
  python3 "$MANIFEST_PY" \
    --corpus-root "$OUT" --keyspace "$KS" --table "$TBL" \
    --sstable-dir "$DEST" --image "$IMAGE" --docker "$DOCKER" \
    --seed "$SEED" --rows-requested "$ROWS" --chunk-rows "$CHUNK_ROWS" \
    --payload-bytes "$PAYLOAD_BYTES" --widths "$WIDTHS" --buckets "$BUCKETS" \
    --mode "$mode" --row-plan "$PLAN" --yaml-verified "$yaml_file" \
    --min-data-db-floor "$MIN_DATA_DB_BYTES" \
    ${DUMPED[@]+"${DUMPED[@]/#/--dumped=}"} \
    --out "$staged" \
    || { rm -f -- "$staged"; die "manifest generation failed (the corpus keeps its
       IN-PROGRESS marker, so no consumer can read provenance for it)"; }
  [ -s "$staged" ] || { rm -f -- "$staged"; die "the manifest writer produced an EMPTY
       $staged — refusing to publish it"; }
  mv -f -- "$staged" "$final" \
    || { rm -f -- "$staged"; die "cannot rename $staged -> $final"; }
  log "manifest published (atomically): $final"
  if [ -n "$MANIFEST_OUT" ]; then
    # Same discipline for the second destination: copy to a sibling temp, then rename.
    local copy_tmp="$MANIFEST_OUT.staged.$$"
    cp "$final" "$copy_tmp" && mv -f -- "$copy_tmp" "$MANIFEST_OUT" \
      || { rm -f -- "$copy_tmp"; die "cannot publish the manifest copy to $MANIFEST_OUT"; }
    log "manifest ALSO copied to: $MANIFEST_OUT"
  else
    log "manifest written only inside the corpus ($final);"
    log "  pass --publish-manifest (production runs only) to replace $COMMITTED_MANIFEST"
  fi
}

# ------------------------------------------------------------------- main ----
# Self-test hook first: it needs none of the corpus flags and touches nothing but
# the yaml copy it is handed.
if [ -n "$YAML_FLIP_CHECK" ]; then
  bash -c "$(bti_yaml_flip_snippet "$YAML_FLIP_CHECK")" \
    || die "yaml flip check FAILED for $YAML_FLIP_CHECK (see the YAML-FLIP-FATAL line above)"
  echo "YAML-FLIP-OK $YAML_FLIP_CHECK"
  exit 0
fi

validate_inputs

if [ "$VALIDATE_ONLY" = 1 ]; then
  # PRUNE_KEEP stands in for the basename publish() is about to publish (the one
  # dir a real run must NOT delete), so the `keep` exclusion of a function that
  # `rm -rf`s multi-GB paths is exercisable without a container.
  if [ "$PRUNE_DRY_RUN" = 1 ]; then prune_stale_table_dirs "${PRUNE_KEEP:-}"; fi
  # `widths=` is reported because the rows-per-partition mix is what makes a run's
  # SIZE and partition shape what they are — the small-golden defaults are sized to
  # the committed-golden convention, and a silent change to them is a silent change
  # to a committed fixture's size. Reported LAST so appending it breaks no grep.
  echo "VALIDATE-OK rows=$ROWS chunk_rows=$CHUNK_ROWS chunks=$CHUNKS seed=$SEED keyspace=$KS table=$TBL out=$OUT mode=$RUN_MODE manifest_out=${MANIFEST_OUT:-(none)} widths=$WIDTHS"
  exit 0
fi

if [ "$VERIFY_ONLY" = 1 ]; then
  ks_dir="$(corpus_keyspace_dir)"
  if [ -z "$ks_dir" ] || [ ! -d "$ks_dir" ]; then
    die "no corpus at $OUT/sstables/$KS — generate it first"
  fi
  shopt -s nullglob
  found=("$ks_dir/$TBL"-*)
  shopt -u nullglob
  [ ${#found[@]} -ge 1 ] || die "no $TBL-<uuid> dir under $ks_dir"
  # An AMBIGUOUS root is a hard failure, not a per-dir loop (roborev #3234 M1). This
  # used to verify each matching dir INDEPENDENTLY and then print one VERIFY-OK, so a
  # root left holding several generations by --no-prune passed while the SSTable
  # count in the manifest described only one of them. That matters beyond tidiness: a
  # consumer scanning the discoverable tree sees the UNION, so the generation count —
  # which selects the scan route and is what any read-path figure is attributed to —
  # silently changes with no assertion anywhere disagreeing.
  if [ ${#found[@]} -gt 1 ]; then
    die "AMBIGUOUS corpus root: ${#found[@]} '$TBL-<uuid>' directories under $ks_dir:
       $(printf '%s ' "${found[@]##*/}")
       A measurement corpus must hold exactly ONE, because the generation count
       selects the scan route. Remove the stale ones (a normal run prunes them; this
       state comes from --no-prune), or point --out at a dedicated directory."
  fi
  for d in "${found[@]}"; do
    log "verifying $d ..."
    assert_corpus "$d"
  done
  # corpus_dirs is reported (always 1 — the ambiguity check above is what makes it so)
  # so a pasted VERIFY-OK line SHOWS that the check ran. Appended LAST so it breaks no
  # existing grep.
  echo "VERIFY-OK corpus=$OUT keyspace=$KS table=$TBL sstables=$ASSERT_SSTABLE_COUNT largest_data_db=$ASSERT_MAX_DATA corpus_dirs=${#found[@]}"
  log "Use with: export CQLITE_DATASETS_ROOT=$OUT"
  exit 0
fi

KEEP_ON_FAIL=1
cleanup() {
  local code=$?
  if [ "$code" -ne 0 ] && [ "$KEEP_ON_FAIL" = 1 ]; then
    log "FAILED (exit $code). Last container logs; leaving '$CONTAINER' for inspection."
    $DOCKER logs --tail 40 "$CONTAINER" 2>&1 || true
  elif [ "$KEEP_CONTAINER" = 1 ]; then
    log "leaving container '$CONTAINER' running (--keep-container); remove with: $DOCKER rm -f $CONTAINER"
  else
    $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

preflight
start_container
apply_bti_yaml
create_schema
load_chunks
# Order is load-bearing (roborev #3234 F3): the schema is captured into $WORK, publish()
# installs the in-progress marker BEFORE it copies anything into the published corpus,
# and only then is the schema installed at the corpus root and beside the SSTables.
capture_schema "$WORK/schema.cql"
publish
install_schema "$WORK/schema.cql"
assert_corpus "$DEST"
dump_goldens
verify_dumped_row_counts
write_manifest

log "DONE. Corpus at $DEST"
log "  SSTables: $ASSERT_SSTABLE_COUNT, largest Data.db: $ASSERT_MAX_DATA B, goldens: ${#DUMPED[@]}"
log "Re-assert any time (no container needed): bash $0 --verify-only --out $OUT --keyspace $KS --table $TBL"
echo "export CQLITE_DATASETS_ROOT=$OUT"

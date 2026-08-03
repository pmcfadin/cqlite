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
#   $OUT/manifest-bti-3234.json  (copied to test-data/perf-corpus-bti-manifest.json)
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
TBL="${TBL-wide_multiclustering}"
# ~2.0 GiB at the density MEASURED by this generator's commissioning run
# (162 B/row on disk at --payload-bytes 160, LZ4/16 KiB). 5 GiB ~= 33000000 rows.
ROWS="${ROWS:-13200000}"
# 500k rows/chunk => ~78 MiB Data.db per SSTable, comfortably over the 8 MiB floor.
CHUNK_ROWS="${CHUNK_ROWS:-500000}"
SEED="${SEED:-20260803}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-160}"
# rows-per-partition distribution, "<rows>:<weight>". Every class is wide enough
# that its partition spans several 16 KiB row-index blocks, so Rows.db is
# populated for every partition (the fail-closed assert below would catch it if not).
WIDTHS="${WIDTHS:-200:60,800:30,4000:10}"
# Distinct FIRST BYTES + heterogeneous lengths: the depth-1 transition spread is
# what keeps the row-index trie from degenerating (gen-multiclustering-bti.sh, #3032).
BUCKETS="${BUCKETS:-alpha,bo,charlie-extended-bucket,delta,ep,foxtrot-long-bucket-name,golf,hh,india-bucket,jj}"
# `sstabledump` goldens are ~1.7x Data.db, so only a BOUNDED subset is dumped
# (issue #3234 AC5). 0 disables golden generation entirely.
DUMP_GENERATIONS="${DUMP_GENERATIONS:-1}"
# The read-plane floor (POINT_MMAP_MADV_RANDOM_MIN_BYTES). See the header.
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
  --validate-only        validate flags and exit 0; starts no container, writes nothing
  --prune-dry-run        --validate-only + list the stale corpus dirs a run WOULD remove
  --verify-only          re-assert an ALREADY-GENERATED corpus under --out and exit;
                         no container, no writes, nothing mutated

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
  --manifest-out PATH    committed-manifest destination; "" disables (MANIFEST_OUT)
  --keep-container       leave the container running after a successful run
  --no-prune             keep previous <table>-<uuid> corpus dirs (PRUNE_STALE=0)
  -h, --help             this text

Unrecognized arguments exit 2.
EOF
}

# ---------------------------------------------------------- arg parsing ------
SMOKE=0
VALIDATE_ONLY=0
PRUNE_DRY_RUN=0
VERIFY_ONLY=0
KS_EXPLICIT=0
if [ -n "$KS" ]; then KS_EXPLICIT=1; fi

need_arg() { [ $# -ge 2 ] || { echo "$0: $1 requires a value" >&2; exit 2; }; }

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke) SMOKE=1; shift ;;
    --validate-only) VALIDATE_ONLY=1; shift ;;
    --prune-dry-run) VALIDATE_ONLY=1; PRUNE_DRY_RUN=1; shift ;;
    --verify-only) VERIFY_ONLY=1; shift ;;
    --out) need_arg "$@"; OUT="$2"; shift 2 ;;
    --rows) need_arg "$@"; ROWS="$2"; shift 2 ;;
    --chunk-rows) need_arg "$@"; CHUNK_ROWS="$2"; shift 2 ;;
    --seed) need_arg "$@"; SEED="$2"; shift 2 ;;
    --payload-bytes) need_arg "$@"; PAYLOAD_BYTES="$2"; shift 2 ;;
    --widths) need_arg "$@"; WIDTHS="$2"; shift 2 ;;
    --buckets) need_arg "$@"; BUCKETS="$2"; shift 2 ;;
    --keyspace) need_arg "$@"; KS="$2"; KS_EXPLICIT=1; shift 2 ;;
    --table) need_arg "$@"; TBL="$2"; shift 2 ;;
    --dump-generations) need_arg "$@"; DUMP_GENERATIONS="$2"; shift 2 ;;
    --min-data-db-bytes) need_arg "$@"; MIN_DATA_DB_BYTES="$2"; shift 2 ;;
    --image) need_arg "$@"; IMAGE="$2"; shift 2 ;;
    --container) need_arg "$@"; CONTAINER="$2"; shift 2 ;;
    --manifest-out) need_arg "$@"; MANIFEST_OUT="$2"; shift 2 ;;
    --keep-container) KEEP_CONTAINER=1; shift ;;
    --no-prune) PRUNE_STALE=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "$0: unrecognized argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# --smoke is a DEFAULTS override, so it must not silently undo an explicit flag:
# only unset-by-the-user values are lowered. It stays large enough that the
# 8 MiB Data.db floor and the Rows.db assert are genuinely exercised.
if [ "$SMOKE" = 1 ]; then
  ROWS="${SMOKE_ROWS:-240000}"
  CHUNK_ROWS="${SMOKE_CHUNK_ROWS:-120000}"
  [ "$KS_EXPLICIT" = 1 ] || KS="perf_bti_smoke"
fi
[ -n "$KS" ] || KS="perf_bti"
MANIFEST_OUT="${MANIFEST_OUT-$SCRIPT_DIR/../perf-corpus-bti-manifest.json}"

# ------------------------------------------------------- input validation ----
# Runs BEFORE the container, the load, and any deletion: an unvalidated typo must
# never start a multi-GB run or overwrite the COMMITTED manifest.
is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }

CHUNKS=0
validate_inputs() {
  command -v python3 >/dev/null 2>&1 || die "python3 is required (row driver + manifest writer)"
  [ -f "$ROWS_PY" ] || die "missing row driver: $ROWS_PY"
  [ -f "$MANIFEST_PY" ] || die "missing manifest writer: $MANIFEST_PY"
  [[ -n "${OUT// }" ]] || die "--out/OUT is empty"
  [[ "$OUT" == /* ]] || die "--out must be an absolute path, got '$OUT'"
  [[ "$(printf '%s' "$OUT" | sed 's:/*$::')" != "" ]] || die "refusing to use '/' as --out"
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
  python3 - "$ROWS_PY" "$CHUNKS" "$CHUNK_ROWS" <<'PYEOF' || die "plan exceeds the \`pk int\` ceiling (see message above)"
import importlib.util, sys
spec = importlib.util.spec_from_file_location("rows", sys.argv[1])
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.plan_fits_int32(int(sys.argv[2]), int(sys.argv[3]))
PYEOF
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
  local dest="$1" f base gens=0 max_data=0 sz rows_db
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
    log "  [assert] $base: Data.db $sz B, Rows.db $rsz B, TOC ok ($(wc -l <"$toc" | tr -d ' ') components)"
  done

  # AC2 (first half): the 8 MiB read-plane floor.
  [ "$max_data" -gt "$MIN_DATA_DB_BYTES" ] || die "AC2: largest Data.db is $max_data B, needs > $MIN_DATA_DB_BYTES B.
       Below 8 MiB MADV_RANDOM is not applied and the point-read/scan mappings are
       the SAME mapping, so a read-plane A/B measures nothing. Raise --chunk-rows."
  log "[assert] $gens SSTable(s); largest Data.db $max_data B > floor $MIN_DATA_DB_BYTES B; every Rows.db non-empty"
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
  DATA_DIR="$OUT/cassandra-data"
  $SUDO rm -rf "$DATA_DIR"
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
YAML_VERIFIED=""
apply_bti_yaml() {
  local yaml=/etc/cassandra/cassandra.yaml
  log "applying storage_compatibility_mode: NONE + sstable.selected_format: bti ..."
  $DOCKER exec "$CONTAINER" bash -lc \
    "sed -i 's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g; s|#sstable:|sstable:|; s|#  selected_format: big|  selected_format: bti|' $yaml"
  if ! $DOCKER exec "$CONTAINER" bash -lc "grep -qE '^storage_compatibility_mode: NONE\$' $yaml"; then
    die "storage_compatibility_mode was NOT set to NONE in $yaml — the node would emit 'nb' (BIG) silently"
  fi
  if ! $DOCKER exec "$CONTAINER" bash -lc "grep -qE '^sstable:\$' $yaml && grep -qE '^  selected_format: bti\$' $yaml"; then
    die "sstable.selected_format was NOT set to bti in $yaml — the node would emit 'nb' (BIG) silently"
  fi
  YAML_VERIFIED="$($DOCKER exec "$CONTAINER" bash -lc \
    "grep -nE '^storage_compatibility_mode:|^sstable:|^  selected_format:' $yaml")"
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
  rm -rf "$WORK" 2>/dev/null || $SUDO rm -rf "$WORK"
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

capture_schema() {  # $1 = destination
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

CONTAINER_SSTABLE_DIR=""
DEST=""
publish() {
  CONTAINER_SSTABLE_DIR="$($DOCKER exec "$CONTAINER" bash -lc \
    "ls -d /var/lib/cassandra/data/$KS/$TBL-* | head -1" | tr -d '\r')"
  [ -n "$CONTAINER_SSTABLE_DIR" ] || die "no SSTable dir for $KS.$TBL in the container"
  local name host_dir
  name="$(basename "$CONTAINER_SSTABLE_DIR")"
  host_dir="$DATA_DIR/data/$KS/$name"
  [ -d "$host_dir" ] || die "host bind-mount dir missing: $host_dir"
  DEST="$OUT/sstables/$KS/$name"
  mkdir -p "$OUT/sstables/$KS"
  if [ "$PRUNE_STALE" = 1 ]; then prune_stale_table_dirs "$name"; fi
  $SUDO rm -rf "$DEST"
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
  local stem base meta_rows dump_rows
  for stem in "${DUMPED[@]}"; do
    base="$stem-Data.db"
    meta_rows="$($DOCKER exec "$CONTAINER" bash -lc \
      "/opt/cassandra/tools/bin/sstablemetadata '$CONTAINER_SSTABLE_DIR/$base' 2>/dev/null" \
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
  local mode=production
  if [ "$SMOKE" = 1 ]; then mode=smoke; fi
  python3 "$MANIFEST_PY" \
    --corpus-root "$OUT" --keyspace "$KS" --table "$TBL" \
    --sstable-dir "$DEST" --image "$IMAGE" --docker "$DOCKER" \
    --seed "$SEED" --rows-requested "$ROWS" --chunk-rows "$CHUNK_ROWS" \
    --payload-bytes "$PAYLOAD_BYTES" --widths "$WIDTHS" --buckets "$BUCKETS" \
    --mode "$mode" --row-plan "$PLAN" --yaml-verified "$yaml_file" \
    ${DUMPED[@]+"${DUMPED[@]/#/--dumped=}"} \
    --out "$OUT/manifest-bti-3234.json" \
    || die "manifest generation failed"
  if [ -n "$MANIFEST_OUT" ]; then
    cp "$OUT/manifest-bti-3234.json" "$MANIFEST_OUT"
    log "committed-path manifest: $MANIFEST_OUT"
  fi
}

# ------------------------------------------------------------------- main ----
validate_inputs

if [ "$VALIDATE_ONLY" = 1 ]; then
  if [ "$PRUNE_DRY_RUN" = 1 ]; then prune_stale_table_dirs ""; fi
  echo "VALIDATE-OK rows=$ROWS chunk_rows=$CHUNK_ROWS chunks=$CHUNKS seed=$SEED keyspace=$KS table=$TBL out=$OUT"
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
  for d in "${found[@]}"; do
    log "verifying $d ..."
    assert_corpus "$d"
  done
  echo "VERIFY-OK corpus=$OUT keyspace=$KS table=$TBL sstables=$ASSERT_SSTABLE_COUNT largest_data_db=$ASSERT_MAX_DATA"
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
capture_schema "$OUT/schema.cql"
publish
cp "$OUT/schema.cql" "$DEST/schema.cql"
assert_corpus "$DEST"
dump_goldens
verify_dumped_row_counts
write_manifest

log "DONE. Corpus at $DEST"
log "  SSTables: $ASSERT_SSTABLE_COUNT, largest Data.db: $ASSERT_MAX_DATA B, goldens: ${#DUMPED[@]}"
log "Re-assert any time (no container needed): bash $0 --verify-only --out $OUT --keyspace $KS --table $TBL"
echo "export CQLITE_DATASETS_ROOT=$OUT"

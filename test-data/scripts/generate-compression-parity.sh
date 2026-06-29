#!/usr/bin/env bash
# generate-compression-parity.sh — Compression / chunk-format parity SSTable
# fixtures (epic #970, issue #996).
#
# Creates a NEW keyspace `test_comp` (isolated from every other corpus) holding
# one table per compression scenario the CQLite compression-parity epic must
# read byte-for-byte the same as Apache Cassandra 5.0.2.  Each table pins a
# compressor class AND an explicit `chunk_length_in_kb` (see
# schemas/compression-parity.cql) so the CompressionInfo.db header (algorithm
# string + chunk_length) is fully deterministic.
#
# Container lifecycle, flag parsing (--out/--dry-run), logging helpers, and the
# export + sstabledump + sstablemetadata steps mirror generate-tombstone-parity.sh
# exactly so the committed reference files (`*-Data.db.jsonl`,
# `*-Statistics.db.txt`, `TOC.txt`, `Digest.crc32`) are consistent across the
# corpus.  All tables are single-flush (one nb-1-big generation per table).
#
# =====================================================================
# DETERMINISM
# =====================================================================
#   T_BASE  = 1609459200000000  (2021-01-01T00:00:00Z) — fixed writetime for
#             every INSERT (explicit USING TIMESTAMP) so writetimes are pinned.
#   SEED    = 0x00C0FFEE        — fixed PRNG seed used to generate the
#             high-entropy (incompressible) BLOB payloads.  Compressible TEXT
#             payloads are deterministic literals.  Re-runs therefore produce
#             logically-equivalent fixtures (table-UUID dir names differ because
#             Cassandra assigns a fresh UUID per CREATE TABLE; that is expected).
#
# =====================================================================
# SCENARIO -> TABLE -> MANIFEST KEY (fixture_matrix.*) -> COMPRESSION / chunk_kb
# =====================================================================
#   LZ4                -> lz4_table                          LZ4Compressor     / 16
#   Snappy             -> snappy_table                       SnappyCompressor  / 16
#   Deflate            -> deflate_table                      DeflateCompressor / 16
#   Zstd (no dict)     -> zstd_table                         ZstdCompressor    / 16
#   Uncompressed       -> uncompressed_table                 {'enabled': false}/ n-a
#   Short final chunk  -> short_final_chunk                  LZ4Compressor     / 4
#   Incompressible     -> incompressible_uncompressed_chunk  LZ4Compressor     / 4
#
# Usage:
#   bash test-data/scripts/generate-compression-parity.sh [--out <dir>] [--dry-run]
#
# Options:
#   --out <dir>   Output directory (default: test-data/datasets)
#   --dry-run     Print commands without executing
#
# Prerequisites:
#   - Docker (or podman) available in PATH
#   - ~4 GB RAM available for the Cassandra container
#
# Backs: epic #970 (issue #996)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-comp"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_comp"

# Fixed-determinism constants (see header).
T_BASE=1609459200000000   # 2021-01-01T00:00:00Z
SEED=12648430             # 0x00C0FFEE

# ---------------------------------------------------------------------------
# Parse CLI flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[comp] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# Canonicalise OUT_DIR
if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[comp] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[comp][ERROR] $*" >&2; exit 1; }

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

# Detect container engine
if command -v docker >/dev/null 2>&1; then
  ENGINE="docker"
elif command -v podman >/dev/null 2>&1; then
  ENGINE="podman"
else
  if [[ "$DRY_RUN" -eq 1 ]]; then
    ENGINE="docker"   # placeholder so dry-run can still print commands
    echo "[comp] (dry-run) no container engine found; using placeholder 'docker' for command preview"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

# ---------------------------------------------------------------------------
# Guard: ensure no leftover container
# ---------------------------------------------------------------------------
if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

# ---------------------------------------------------------------------------
# Cleanup trap
# ---------------------------------------------------------------------------
cleanup() {
  if [[ "$DRY_RUN" -eq 0 ]]; then
    log "Cleaning up container..."
    $ENGINE rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helper: wait for Cassandra readiness
# ---------------------------------------------------------------------------
wait_cassandra() {
  local max_retries=60
  local delay=5
  log "Waiting for Cassandra to become ready (max ${max_retries}x${delay}s)..."
  for i in $(seq 1 "$max_retries"); do
    if $ENGINE exec "$CONTAINER_NAME" \
        cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra is ready (attempt $i)."
      return 0
    fi
    sleep "$delay"
  done
  fail "Cassandra did not become ready in time."
}

# ---------------------------------------------------------------------------
# Helper: apply a schema file via cqlsh
# ---------------------------------------------------------------------------
apply_schema() {
  local schema_file="$1"
  local dest_name
  dest_name="$(basename "$schema_file")"
  log "Applying schema: $dest_name"
  run $ENGINE cp "$schema_file" "$CONTAINER_NAME:/tmp/$dest_name"
  run $ENGINE exec "$CONTAINER_NAME" cqlsh -f "/tmp/$dest_name"
}

# ---------------------------------------------------------------------------
# Helper: flush the keyspace, producing nb-1-big SSTables for every table.
# ---------------------------------------------------------------------------
flush_generation() {
  local label="$1"
  log "Flushing $KEYSPACE ($label)..."
  run $ENGINE exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"
  log "Flush ($label) complete."
}

# ---------------------------------------------------------------------------
# Phase: insert deterministic data for every compression table.
#
# Compressible tables (lz4/snappy/deflate/zstd/uncompressed): identical highly
# compressible TEXT rows so cross-algorithm parity comparisons share the same
# logical payload, differing only in the compressor.
#
# short_final_chunk: chunk_length_in_kb = 4 (= 4096 uncompressed bytes/chunk).
#   We write enough compressible TEXT that the total uncompressed Data.db
#   payload is NOT a multiple of 4096, so the LAST chunk covers fewer than 4096
#   uncompressed bytes (verified post-hoc from the chunk-offset table).
#
# incompressible_uncompressed_chunk: chunk_length_in_kb = 4. Each row carries a
#   high-entropy fixed-PRNG BLOB so LZ4 cannot shrink it; Cassandra then stores
#   the chunk RAW (compressed length == raw uncompressed length). Verified
#   post-hoc.
# ---------------------------------------------------------------------------
run_inserts() {
  log "=== Inserting deterministic data into $KEYSPACE ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would insert rows via python3 cassandra-driver heredoc (USING TIMESTAMP $T_BASE, SEED $SEED)"
    echo "[dry-run]   lz4/snappy/deflate/zstd/uncompressed_table: 600 identical compressible TEXT rows each"
    echo "[dry-run]   short_final_chunk: compressible TEXT rows sized so total payload is not a multiple of 4096"
    echo "[dry-run]   incompressible_uncompressed_chunk: 64 rows of fixed-PRNG random 4096-byte BLOBs"
    return 0
  fi
  $ENGINE exec -i "$CONTAINER_NAME" \
    env T_BASE="$T_BASE" SEED="$SEED" KEYSPACE="$KEYSPACE" \
    python3 - <<'PYEOF'
import os, sys, traceback, time, random
from cassandra.cluster import Cluster

T_BASE = int(os.environ["T_BASE"])
SEED = int(os.environ["SEED"])
KEYSPACE = os.environ["KEYSPACE"]

def connect_with_retry(keyspace, attempts=12, delay=6):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect(keyspace)
            print(f"[connect] Connected to {keyspace} on attempt {attempt}", flush=True)
            return cluster, session
        except Exception as exc:
            last_exc = exc
            print(f"[connect] Attempt {attempt}/{attempts} failed: {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to {keyspace} after {attempts} attempts: {last_exc}")

try:
    cluster, session = connect_with_retry(KEYSPACE)

    # ------------------------------------------------------------------
    # Deterministic compressible TEXT body. A repeated, low-entropy string
    # compresses extremely well under all four codecs, keeping the chunk
    # map small and the cross-algorithm comparison fair.
    # ------------------------------------------------------------------
    def body_for(ck):
        # ~300 chars of highly-repetitive text, salted with ck so rows differ
        # logically but remain trivially compressible.
        return ("compressible_payload_row_%05d_" % ck) + ("ab" * 140)

    COMPRESSIBLE_TABLES = [
        "lz4_table", "snappy_table", "deflate_table",
        "zstd_table", "uncompressed_table",
    ]
    N_COMPRESSIBLE = 600

    for tbl in COMPRESSIBLE_TABLES:
        print(f"[compressible] {tbl}", flush=True)
        stmt = session.prepare(
            f"INSERT INTO {tbl} (pk, ck, body) VALUES (1, ?, ?) USING TIMESTAMP ?"
        )
        for ck in range(1, N_COMPRESSIBLE + 1):
            session.execute(stmt, (ck, body_for(ck), T_BASE))
        print(f"  {tbl}: {N_COMPRESSIBLE} rows", flush=True)

    # ------------------------------------------------------------------
    # short_final_chunk (chunk_length_in_kb = 4 -> 4096 uncompressed bytes).
    # We deliberately write a row count whose total serialized payload is NOT
    # an exact multiple of 4096 uncompressed bytes, forcing the final chunk to
    # be short. 777 rows of ~300-byte compressible bodies (>> 4096 bytes total)
    # guarantees several chunks with a short trailing one.
    # ------------------------------------------------------------------
    print("[short_final_chunk]", flush=True)
    stmt = session.prepare(
        "INSERT INTO short_final_chunk (pk, ck, body) VALUES (1, ?, ?) USING TIMESTAMP ?"
    )
    for ck in range(1, 778):
        session.execute(stmt, (ck, body_for(ck), T_BASE))
    print("  short_final_chunk: 777 rows", flush=True)

    # ------------------------------------------------------------------
    # incompressible_uncompressed_chunk (chunk_length_in_kb = 4).
    # Fixed-PRNG random BLOBs. random.Random(SEED) makes the byte stream
    # reproducible across runs. Each row carries a 4096-byte random blob; with
    # the 4 KiB chunk size, multiple high-entropy chunks are produced and LZ4
    # cannot shrink them, so Cassandra stores them RAW.
    # ------------------------------------------------------------------
    print("[incompressible_uncompressed_chunk]", flush=True)
    rng = random.Random(SEED)
    stmt = session.prepare(
        "INSERT INTO incompressible_uncompressed_chunk (pk, ck, payload) "
        "VALUES (1, ?, ?) USING TIMESTAMP ?"
    )
    for ck in range(1, 65):
        blob = bytes(rng.getrandbits(8) for _ in range(4096))
        session.execute(stmt, (ck, blob, T_BASE))
    print("  incompressible_uncompressed_chunk: 64 rows (4096-byte random blobs)", flush=True)

    print("[OK] test_comp: inserts complete", flush=True)
    cluster.shutdown()

except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during inserts:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Generate sstabledump JSONL golden files (matches generate-tombstone-parity.sh).
# ---------------------------------------------------------------------------
generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files for $KEYSPACE..."
  while IFS= read -r -d '' data_file; do
    local rel
    rel="${data_file#"$sstables_dir"/}"
    local rel_sstabledump="${rel#data/}"
    local jsonl_file="${data_file%.db}.db.jsonl"
    log "  sstabledump: $rel"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "[dry-run] sstabledump $data_file > $jsonl_file"
    else
      $ENGINE exec "$CONTAINER_NAME" bash -lc \
        "/opt/cassandra/tools/bin/sstabledump /var/lib/cassandra/data/${rel_sstabledump} -l" \
        | python3 -c "
import json, sys
try:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        print(json.dumps(item, separators=(',', ':')))
except Exception as e:
    print(json.dumps({'error': str(e)}), file=sys.stderr)
    raise
" > "$jsonl_file"
      if [[ ! -s "$jsonl_file" ]]; then
        log "  WARNING: JSONL file is empty: $jsonl_file"
      else
        local lines
        lines=$(wc -l < "$jsonl_file" | tr -d ' ')
        log "  OK: $jsonl_file ($lines partitions)"
      fi
    fi
  done < <(find "$sstables_dir/$KEYSPACE" -type f -name "*-Data.db" -not -name "._*" -print0 \
            2>/dev/null || true)
}

# ---------------------------------------------------------------------------
# Correctness verifier: parse every CompressionInfo.db and emit a
# *-CompressionInfo.db.txt sidecar (algorithm, chunk_length, max_compressed,
# data_length, chunk_count, per-chunk offset/on-disk-length, and the
# short-final / raw-chunk invariants). Pure-stdlib Python, runs on the host.
# ---------------------------------------------------------------------------
verify_compression_info() {
  local sstables_dir="$1"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would parse every CompressionInfo.db and emit *-CompressionInfo.db.txt"
    return 0
  fi
  log "Parsing CompressionInfo.db chunk maps for $KEYSPACE..."
  while IFS= read -r -d '' ci_file; do
    local data_file="${ci_file%CompressionInfo.db}Data.db"
    local out_txt="${ci_file}.txt"
    CI_FILE="$ci_file" DATA_FILE="$data_file" OUT_TXT="$out_txt" python3 - <<'PYEOF'
import os, struct, sys

ci = os.environ["CI_FILE"]
data = os.environ["DATA_FILE"]
out = os.environ["OUT_TXT"]

with open(ci, "rb") as f:
    buf = f.read()

pos = 0
def read_utf(b, p):
    (n,) = struct.unpack_from(">H", b, p); p += 2
    s = b[p:p+n].decode("utf-8"); p += n
    return s, p

algo, pos = read_utf(buf, pos)
(opt_count,) = struct.unpack_from(">I", buf, pos); pos += 4
opts = []
for _ in range(opt_count):
    k, pos = read_utf(buf, pos)
    v, pos = read_utf(buf, pos)
    opts.append((k, v))
(chunk_length,) = struct.unpack_from(">I", buf, pos); pos += 4
# nb (Cassandra 5.0) BIG format always has max_compressed_length (version >= "na").
(max_compressed,) = struct.unpack_from(">I", buf, pos); pos += 4
(data_length,) = struct.unpack_from(">Q", buf, pos); pos += 8
(chunk_count,) = struct.unpack_from(">I", buf, pos); pos += 4
offsets = []
for _ in range(chunk_count):
    (off,) = struct.unpack_from(">Q", buf, pos); pos += 8
    offsets.append(off)

data_size = os.path.getsize(data)

lines = []
lines.append(f"algorithm: {algo}")
lines.append(f"option_count: {opt_count}")
for k, v in opts:
    lines.append(f"option: {k}={v}")
lines.append(f"chunk_length: {chunk_length}")
lines.append(f"max_compressed_length: {max_compressed}")
lines.append(f"total_uncompressed_length: {data_length}")
lines.append(f"chunk_count: {chunk_count}")
lines.append(f"data_db_size_bytes: {data_size}")
lines.append("")
lines.append("# Per-chunk map (on_disk_len = next_offset - offset; comp_len = on_disk_len - 4 CRC word)")
lines.append("# raw_uncompressed = chunk_length for all but the last chunk; last = total_uncompressed_length - last_offset_uncompressed")
lines.append("idx\toffset\ton_disk_len\tcomp_len\traw_uncompressed_len\traw_stored")

short_final = False
raw_chunks = []
for i in range(chunk_count):
    start = offsets[i]
    end = offsets[i+1] if i + 1 < chunk_count else data_size
    on_disk = end - start
    comp_len = on_disk - 4  # subtract trailing CRC32 word
    # uncompressed bytes this chunk covers
    if i + 1 < chunk_count:
        raw_unc = chunk_length
    else:
        raw_unc = data_length - (chunk_length * (chunk_count - 1))
        if raw_unc < chunk_length:
            short_final = True
    # Cassandra stores a chunk RAW when the compressed form did not shrink it,
    # i.e. on-disk compressed payload length == the uncompressed chunk length.
    raw_stored = (comp_len == raw_unc)
    if raw_stored:
        raw_chunks.append(i)
    lines.append(f"{i}\t{start}\t{on_disk}\t{comp_len}\t{raw_unc}\t{raw_stored}")

lines.append("")
lines.append(f"short_final_chunk: {short_final}  (last chunk covers {data_length - chunk_length*(chunk_count-1)} uncompressed bytes vs chunk_length {chunk_length})")
lines.append(f"raw_stored_chunk_count: {len(raw_chunks)}  indices: {raw_chunks}")

with open(out, "w") as f:
    f.write("\n".join(lines) + "\n")

print(f"[verify] {os.path.basename(ci)}: algo={algo} chunk_length={chunk_length} chunks={chunk_count} short_final={short_final} raw_chunks={len(raw_chunks)}")
PYEOF
  done < <(find "$sstables_dir/$KEYSPACE" -type f -name "*-CompressionInfo.db" -not -name "._*" -print0)
}

# ---------------------------------------------------------------------------
# Guard OUT_DIR path safety
# ---------------------------------------------------------------------------
if [[ "${#OUT_DIR}" -lt 4 ]]; then
  fail "OUT_DIR '$OUT_DIR' is suspiciously short (< 4 chars). Refusing."
fi
case "$OUT_DIR" in
  /) fail "Refusing to operate on '/'." ;;
  /tmp) fail "Refusing to use '/tmp' directly. Use a subdirectory." ;;
esac
_under_repo=0
_under_tmp=0
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] && _under_repo=1
[[ "$OUT_DIR" == /tmp/*          ]] && _under_tmp=1
if [[ "$_under_repo" -eq 0 && "$_under_tmp" -eq 0 ]]; then
  fail "OUT_DIR '$OUT_DIR' is not under the repo root or /tmp/."
fi

log "Starting $KEYSPACE generation (epic #970, issue #996)"
log "Output directory: $OUT_DIR"
log "Fixed writetime: T_BASE=$T_BASE  PRNG seed: SEED=$SEED (0x00C0FFEE)"

SSTABLES_DIR="$OUT_DIR/sstables"

# ---------------------------------------------------------------------------
# Start Cassandra container (nb/CASSANDRA_4 compat mode — default for 5.0.2)
# ---------------------------------------------------------------------------
log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-comp \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# ---------------------------------------------------------------------------
# CRITICAL: flush_compression must be `table`, not the CASSANDRA_4 default
# `fast`.
#
# Cassandra's `flush_compression` (cassandra.yaml) controls the compressor used
# for SSTables produced by a MEMTABLE FLUSH, independent of the table's schema
# `compression` option. The default in CASSANDRA_4 storage-compatibility mode is
# `fast`, which forces every flush-produced SSTable to use LZ4Compressor —
# REGARDLESS of whether the schema says DeflateCompressor / ZstdCompressor /
# SnappyCompressor. The schema compressor would otherwise only take effect on
# the next COMPACTION (which rewrites the SSTable). Empirically verified on
# cassandra:5.0.2: with the default `flush_compression: fast`, deflate_table and
# zstd_table flush as `LZ4Compressor` in CompressionInfo.db.
#
# Setting `flush_compression: table` makes the FIRST flush honor each table's
# schema compressor, so we get a clean single nb-1-big generation per table with
# the correct algorithm string — no extra compaction generation needed.
# ---------------------------------------------------------------------------
log "Setting flush_compression: table (so nb-1-big honors the schema compressor)..."
run $ENGINE exec "$CONTAINER_NAME" bash -lc \
  "sed -i 's/^# *flush_compression:.*/flush_compression: table/' /etc/cassandra/cassandra.yaml && grep -q '^flush_compression: table' /etc/cassandra/cassandra.yaml"
log "Restarting Cassandra to apply flush_compression..."
run $ENGINE restart "$CONTAINER_NAME"
if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
  # Confirm the setting survived the restart.
  if ! $ENGINE exec "$CONTAINER_NAME" bash -lc "grep -q '^flush_compression: table' /etc/cassandra/cassandra.yaml"; then
    fail "flush_compression: table was not applied after restart."
  fi
  log "flush_compression: table confirmed."
fi

# Install Python driver
log "Installing python3-pip in container..."
run $ENGINE exec "$CONTAINER_NAME" bash -c "apt-get update -qq && apt-get install -y -q python3-pip"
log "Installing cassandra-driver in container..."
run $ENGINE exec "$CONTAINER_NAME" pip3 install --quiet cassandra-driver

# Apply schema (creates keyspace + all seven tables)
apply_schema "$ROOT/schemas/compression-parity.cql"

# ---------------------------------------------------------------------------
# Insert deterministic data + single flush.
# ---------------------------------------------------------------------------
run_inserts
flush_generation "gen-1"

# ---------------------------------------------------------------------------
# Export SSTables to host (matches generate-tombstone-parity.sh tar-stream).
# ---------------------------------------------------------------------------
log "=== Exporting $KEYSPACE SSTables from container ==="

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would rm -rf $SSTABLES_DIR/$KEYSPACE (clear stale tables) before re-export"
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE from container into $SSTABLES_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.comp_export_tmp"
  rm -rf "$TMPDIR_EXPORT"
  mkdir -p "$TMPDIR_EXPORT"

  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    if [[ -d "$TMPDIR_EXPORT/data/$KEYSPACE" ]]; then
      if [[ -d "$SSTABLES_DIR/$KEYSPACE" ]]; then
        log "Removing stale $SSTABLES_DIR/$KEYSPACE before re-export..."
      fi
      rm -rf "$SSTABLES_DIR/$KEYSPACE"
      mkdir -p "$SSTABLES_DIR/$KEYSPACE"
      cp -r "$TMPDIR_EXPORT/data/$KEYSPACE/." "$SSTABLES_DIR/$KEYSPACE/"
      log "$KEYSPACE SSTables placed in $SSTABLES_DIR/$KEYSPACE"
    else
      fail "Expected $TMPDIR_EXPORT/data/$KEYSPACE but it was not found. Export failed."
    fi
    rm -rf "$TMPDIR_EXPORT"
  else
    fail "tar export from container failed."
  fi

  # Verify at least one Data.db per table
  log "Verifying exported Data.db files..."
  local_count=$(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" | wc -l | tr -d ' ')
  if [[ "$local_count" -eq 0 ]]; then
    fail "No Data.db files found under $SSTABLES_DIR/$KEYSPACE — export is empty!"
  fi
  log "  Found $local_count Data.db file(s) in $KEYSPACE"

  # Generate JSONL golden files
  generate_sstabledump_jsonl "$SSTABLES_DIR"

  # Generate Statistics.db.txt reference files (matches nb corpus convention)
  log "Generating Statistics.db.txt for $KEYSPACE tables..."
  while IFS= read -r -d '' data_file; do
    rel="${data_file#"$SSTABLES_DIR"/}"
    stats_base="${data_file%Data.db}Statistics.db.txt"
    log "  sstablemetadata: $rel"
    $ENGINE run --rm \
      -v "$SSTABLES_DIR:/data" \
      "$CASSANDRA_IMAGE" \
      bash -lc "/opt/cassandra/tools/bin/sstablemetadata /data/${rel}" \
      > "$stats_base" 2>/dev/null || true
    if [[ -s "$stats_base" ]]; then
      log "  OK: $stats_base"
    else
      log "  WARNING: Empty statistics for $rel"
    fi
  done < <(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" -print0)

  # Parse CompressionInfo.db chunk maps + emit verification sidecars.
  verify_compression_info "$SSTABLES_DIR"

  # ---------------------------------------------------------------------------
  # Fail-loud correctness assertions on the generated fixtures:
  #   * each table's CompressionInfo.db algorithm matches the schema compressor
  #   * uncompressed_table has NO CompressionInfo.db
  #   * short_final_chunk has a genuinely short final chunk
  #   * incompressible_uncompressed_chunk has >= 1 raw-stored chunk
  # ---------------------------------------------------------------------------
  log "Asserting compression-fixture correctness..."
  assert_algo() {
    local table="$1" expect="$2"
    local sidecar
    sidecar=$(find "$SSTABLES_DIR/$KEYSPACE/${table}-"* -name "*-CompressionInfo.db.txt" 2>/dev/null | head -1)
    [[ -n "$sidecar" ]] || fail "$table: no CompressionInfo.db.txt sidecar found"
    local got
    got=$(grep -m1 '^algorithm:' "$sidecar" | awk '{print $2}')
    [[ "$got" == "$expect" ]] || fail "$table: algorithm=$got, expected $expect (flush_compression regression?)"
    log "  OK: $table algorithm=$got"
  }
  assert_algo lz4_table     LZ4Compressor
  assert_algo snappy_table  SnappyCompressor
  assert_algo deflate_table DeflateCompressor
  assert_algo zstd_table    ZstdCompressor

  # uncompressed_table: NO CompressionInfo.db; CRC.db present instead.
  if find "$SSTABLES_DIR/$KEYSPACE/uncompressed_table-"* -name "*-CompressionInfo.db" 2>/dev/null | grep -q .; then
    fail "uncompressed_table: unexpected CompressionInfo.db (compression should be disabled)"
  fi
  if ! find "$SSTABLES_DIR/$KEYSPACE/uncompressed_table-"* -name "*-CRC.db" 2>/dev/null | grep -q .; then
    fail "uncompressed_table: missing CRC.db (expected for uncompressed SSTable)"
  fi
  log "  OK: uncompressed_table has no CompressionInfo.db, has CRC.db"

  # short_final_chunk: short final chunk must be present.
  # NOTE: `local` is only valid inside a function; this assertion block runs at
  # script top level, so plain assignments are used (roborev #970).
  sf_sidecar=$(find "$SSTABLES_DIR/$KEYSPACE/short_final_chunk-"* -name "*-CompressionInfo.db.txt" | head -1)
  grep -q '^short_final_chunk: True' "$sf_sidecar" \
    || fail "short_final_chunk: expected a short final chunk but sidecar reports otherwise"
  log "  OK: short_final_chunk has a short final chunk"

  # incompressible_uncompressed_chunk: at least one raw-stored chunk.
  ic_sidecar=$(find "$SSTABLES_DIR/$KEYSPACE/incompressible_uncompressed_chunk-"* -name "*-CompressionInfo.db.txt" | head -1)
  ic_raw=$(grep -m1 '^raw_stored_chunk_count:' "$ic_sidecar" | awk '{print $2}')
  [[ "${ic_raw:-0}" -ge 1 ]] \
    || fail "incompressible_uncompressed_chunk: raw_stored_chunk_count=$ic_raw (expected >= 1; min_compress_ratio honored?)"
  log "  OK: incompressible_uncompressed_chunk has $ic_raw raw-stored chunk(s)"
  log "All compression-fixture correctness assertions passed."

  # Remove macOS AppleDouble files if present
  find "$SSTABLES_DIR/$KEYSPACE" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

  log "=== $KEYSPACE generation COMPLETE ==="
  log "SSTables:   $SSTABLES_DIR/$KEYSPACE"
  log ""
  log "Tables (one nb-1-big generation each):"
  log "  lz4_table, snappy_table, deflate_table, zstd_table, uncompressed_table,"
  log "  short_final_chunk, incompressible_uncompressed_chunk"
  log ""
  log "Next steps:"
  log "  1. Inspect *-CompressionInfo.db.txt sidecars for algorithm string + chunk map."
  log "  2. Confirm short_final_chunk has short_final_chunk: True."
  log "  3. Confirm incompressible_uncompressed_chunk has raw_stored_chunk_count > 0."
  log "  4. uncompressed_table: confirm NO CompressionInfo.db (CRC.db present instead)."
fi

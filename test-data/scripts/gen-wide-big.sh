#!/usr/bin/env bash
# gen-wide-big.sh — Generate a WIDE-PARTITION BIG (nb) fixture whose Index.db
# carries a populated PROMOTED INDEX (IndexInfo[] array), for issue #993
# (wide-partition + promoted-index boundary parity).
#
# Cassandra 5.0 in its default storage_compatibility_mode (CASSANDRA_4) writes
# the legacy BIG ("nb") format. When a single partition exceeds
# column_index_size (64 KiB) Cassandra splits it into row-index blocks and
# writes a promoted IndexInfo[] array into Index.db (RowIndexEntry.IndexedEntry).
# The committed datasets have no such partition, so this script generates one.
#
# It also issues a clustering-range DELETE that straddles a 64 KiB block edge so
# the fixture exercises a range-tombstone marker at a promoted-index block
# boundary (acceptance criterion of #993).
#
# Output (binaries are gitignored; the .jsonl golden is committed):
#   $OUT/sstables/test_big/wide_partition-<hash>/nb-*-{Data,Index,Statistics,Summary}.db
#   $OUT/sstables/test_big/wide_partition-<hash>/nb-*-Data.db.jsonl   (sstabledump -l)
#
# NOTE (reproducibility): Cassandra assigns the table directory a fresh random
# UUID (wide_partition-<uuid>) and a generation-prefixed base name (e.g. nb-1-big
# on a fresh table). The #993 parity tests pin the committed paths
# (wide_partition-ffe2ee50733111f19e8f6d08b8e7a294, nb-2-big), so a re-run will
# NOT match them without renaming the regenerated dir/prefix to the pinned names.
# When promoting this fixture into the dataset release pin (issue #1185), rename
# the regenerated directory + component prefix to the pinned values (or update the
# test constants + manifest reference_paths to the new ones).
set -euo pipefail

IMAGE="${IMAGE:-cassandra:5.0.2}"
CONTAINER="${CONTAINER:-cqlite-widebig}"
# Default OUT to the repo's test-data/datasets, derived from the repo root so the
# manifest's `bash test-data/scripts/gen-wide-big.sh` works in any checkout.
REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
OUT="${OUT:-$REPO_ROOT/test-data/datasets}"
KS="test_big"
TBL="wide_partition"
ROWS_PER_PART="${ROWS_PER_PART:-300}"   # 300 rows * ~2KiB payload ~= 600KiB/partition
PARTS="${PARTS:-3}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-2048}"
# Delete a clustering range straddling the first 64 KiB boundary (~rows 30..40
# at 2KiB/row) so a range-tombstone marker lands near a promoted-index block edge.
DEL_FROM="${DEL_FROM:-30}"
DEL_TO="${DEL_TO:-40}"

log() { echo "[gen-wide-big] $*"; }

KEEP_ON_FAIL=1
cleanup() {
  local code=$?
  if [[ $code -ne 0 && "${KEEP_ON_FAIL}" == "1" ]]; then
    log "FAILED (exit $code). Dumping last container logs; leaving container '$CONTAINER' for inspection."
    docker logs --tail 60 "$CONTAINER" 2>&1 || true
  else
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_ready() {
  local label="$1" max="${2:-90}" delay=5
  log "Waiting for Cassandra to be ready ($label, max ${max}x${delay}s)..."
  for i in $(seq 1 "$max"); do
    if docker exec "$CONTAINER" cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra ready ($label, attempt $i)."
      return 0
    fi
    sleep "$delay"
  done
  log "FATAL: Cassandra not ready ($label) after $((max*delay))s"
  return 1
}

log "Removing any stale container..."
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true

log "Starting $IMAGE as $CONTAINER (default BIG/nb format; pulls image if absent)..."
docker run -d --name "$CONTAINER" "$IMAGE" >/dev/null
wait_ready "initial boot" 90

log "Building schema + ${PARTS}x${ROWS_PER_PART} inserts (~${PAYLOAD_BYTES}B payload each)..."
CQL_FILE="$(mktemp /tmp/wide-big.XXXXXX.cql)"
python3 - "$CQL_FILE" "$KS" "$TBL" "$PARTS" "$ROWS_PER_PART" "$PAYLOAD_BYTES" "$DEL_FROM" "$DEL_TO" <<'PYEOF'
import sys
out, ks, tbl, parts, rows, pbytes, dfrom, dto = (
    sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]),
    int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7]), int(sys.argv[8]))
with open(out, "w") as f:
    f.write(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}};\n")
    f.write(f"USE {ks};\n")
    f.write(f"CREATE TABLE IF NOT EXISTS {tbl} (pk int, ck int, payload text, PRIMARY KEY (pk, ck)) WITH compression = {{'class':'LZ4Compressor'}};\n")
    for pk in range(1, parts + 1):
        for ck in range(rows):
            seed = f"p{pk}c{ck}-"
            reps = (pbytes // len(seed)) + 1
            payload = (seed * reps)[:pbytes]
            f.write(f"INSERT INTO {tbl} (pk, ck, payload) VALUES ({pk}, {ck}, '{payload}');\n")
    # Range tombstone straddling a promoted-index block boundary on pk=1.
    f.write(f"DELETE FROM {tbl} WHERE pk=1 AND ck >= {dfrom} AND ck < {dto};\n")
print("[gen-wide-big] wrote", out)
PYEOF

docker cp "$CQL_FILE" "$CONTAINER:/tmp/wide.cql"
log "Applying schema + inserts via cqlsh -f (this can take a moment)..."
docker exec "$CONTAINER" cqlsh -f /tmp/wide.cql
rm -f "$CQL_FILE"

log "Flushing + compacting ${KS}..."
docker exec "$CONTAINER" nodetool flush "$KS"
docker exec "$CONTAINER" nodetool compact "$KS"

log "Locating wide_partition SSTable dir in container..."
SSTABLE_DIR="$(docker exec "$CONTAINER" bash -lc "ls -d /var/lib/cassandra/data/${KS}/${TBL}-* | head -1")"
log "Container SSTable dir: $SSTABLE_DIR"
docker exec "$CONTAINER" bash -lc "ls -la '$SSTABLE_DIR'"

# Validate the promoted index emitted: Index.db must be large enough to hold an
# IndexInfo[] array (a single-block partition header is only tens of bytes).
INDEX_DB_SIZE="$(docker exec "$CONTAINER" bash -lc "stat -c %s '$SSTABLE_DIR'/nb-*-Index.db 2>/dev/null | head -1 || echo 0")"
DATA_DB_SIZE="$(docker exec "$CONTAINER" bash -lc "stat -c %s '$SSTABLE_DIR'/nb-*-Data.db 2>/dev/null | head -1 || echo 0")"
log "Data.db size: ${DATA_DB_SIZE} bytes, Index.db size: ${INDEX_DB_SIZE} bytes"
if [[ "${INDEX_DB_SIZE:-0}" -lt 200 ]]; then
  log "FATAL: Index.db too small (${INDEX_DB_SIZE}B) — no promoted index emitted. Increase ROWS_PER_PART/PAYLOAD_BYTES."
  exit 1
fi
log "SUCCESS: Index.db is populated (${INDEX_DB_SIZE} bytes) — promoted index present."

DEST_PARENT="$OUT/sstables/${KS}"
DEST_NAME="$(basename "$SSTABLE_DIR")"
DEST="$DEST_PARENT/$DEST_NAME"
log "Copying SSTable to $DEST ..."
mkdir -p "$DEST_PARENT"
rm -rf "$DEST"
docker cp "$CONTAINER:$SSTABLE_DIR" "$DEST_PARENT/"
ls -la "$DEST"

log "Generating sstabledump JSONL golden..."
DATA_DB="$(ls "$DEST"/nb-*-Data.db | head -1)"
BASE="$(basename "$DATA_DB")"
docker exec "$CONTAINER" bash -lc \
  "/opt/cassandra/tools/bin/sstabledump '$SSTABLE_DIR/$BASE' -l" > "$DEST/${BASE}.jsonl"
log "JSONL golden lines: $(wc -l < "$DEST/${BASE}.jsonl" 2>/dev/null || echo '?')"

log "DONE. Fixture at: $DEST"
log "Files:"; ls -la "$DEST"

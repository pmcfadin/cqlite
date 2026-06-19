#!/usr/bin/env bash
# gen-wide-bti.sh — Generate a WIDE-PARTITION BTI (da) fixture with a populated
# Rows.db row-index, for issue #832 (BTI RowIterator / range_query validation).
#
# The existing test_da tables are all narrow (no clustering) and produce 0-byte
# Rows.db files. This creates test_da.wide_table: a clustered table with a few
# partitions each far larger than column_index_size (64KiB), forcing Cassandra
# to write per-partition row indexes into Rows.db (reached via Partitions.db
# RowsOffset -> TrieIndexEntry).
#
# Output (binaries are gitignored; the .jsonl golden is committed):
#   $OUT/sstables/test_da/wide_table-<hash>/da-*-{Data,Partitions,Rows,...}.db
#   $OUT/sstables/test_da/wide_table-<hash>/da-*-Data.db.jsonl   (sstabledump -l)
set -euo pipefail

IMAGE="${IMAGE:-cassandra:5.0.2}"
CONTAINER="${CONTAINER:-cqlite-widebti}"
OUT="${OUT:-/Users/pmcfadin/projects/cqlite/test-data/datasets}"
KS="test_da"
TBL="wide_table"
ROWS_PER_PART="${ROWS_PER_PART:-300}"   # 300 rows * ~2KiB payload ~= 600KiB/partition
PARTS="${PARTS:-3}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-2048}"

log() { echo "[gen-wide-bti] $*"; }

# Keep the container on failure for diagnosis; remove only on clean success.
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

log "Starting $IMAGE as $CONTAINER (pulls image if absent)..."
docker run -d --name "$CONTAINER" "$IMAGE" >/dev/null

# Let the FIRST boot complete fully before reconfiguring — restarting mid-boot
# stalls Cassandra (the bug in the prior run).
wait_ready "initial boot" 90

log "Setting storage_compatibility_mode: NONE (required for BTI) + sstable.selected_format: bti..."
docker exec "$CONTAINER" bash -lc \
  "sed -i 's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g; s|#sstable:|sstable:|; s|#  selected_format: big|  selected_format: bti|' /etc/cassandra/cassandra.yaml"
docker exec "$CONTAINER" bash -lc "grep -E '^storage_compatibility_mode|^sstable:|selected_format' /etc/cassandra/cassandra.yaml || true"

log "Restarting container to apply BTI mode..."
docker restart "$CONTAINER" >/dev/null
wait_ready "BTI mode" 90

log "Building schema + ${PARTS}x${ROWS_PER_PART} inserts (~${PAYLOAD_BYTES}B payload each) into one CQL file..."
CQL_FILE="$(mktemp /tmp/wide-bti.XXXXXX.cql)"
python3 - "$CQL_FILE" "$KS" "$TBL" "$PARTS" "$ROWS_PER_PART" "$PAYLOAD_BYTES" <<'PYEOF'
import sys
out, ks, tbl, parts, rows, pbytes = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
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
print("[gen-wide-bti] wrote", out)
PYEOF

docker cp "$CQL_FILE" "$CONTAINER:/tmp/wide.cql"
log "Applying schema + inserts via cqlsh -f (this can take a moment)..."
docker exec "$CONTAINER" cqlsh -f /tmp/wide.cql
rm -f "$CQL_FILE"

log "Flushing + compacting ${KS}..."
docker exec "$CONTAINER" nodetool flush "$KS"
docker exec "$CONTAINER" nodetool compact "$KS"

log "Locating wide_table SSTable dir in container..."
SSTABLE_DIR="$(docker exec "$CONTAINER" bash -lc "ls -d /var/lib/cassandra/data/${KS}/${TBL}-* | head -1")"
log "Container SSTable dir: $SSTABLE_DIR"
docker exec "$CONTAINER" bash -lc "ls -la '$SSTABLE_DIR'"

ROWS_DB_SIZE="$(docker exec "$CONTAINER" bash -lc "stat -c %s '$SSTABLE_DIR'/da-*-Rows.db 2>/dev/null | head -1 || echo 0")"
log "Rows.db size: ${ROWS_DB_SIZE} bytes"
if [[ "${ROWS_DB_SIZE:-0}" -lt 1 ]]; then
  log "FATAL: Rows.db is empty — partitions did not exceed column_index_size. Increase ROWS_PER_PART/PAYLOAD_BYTES."
  exit 1
fi
log "SUCCESS: Rows.db is populated (${ROWS_DB_SIZE} bytes)."

DEST_PARENT="$OUT/sstables/${KS}"
DEST_NAME="$(basename "$SSTABLE_DIR")"
DEST="$DEST_PARENT/$DEST_NAME"
log "Copying SSTable to $DEST ..."
mkdir -p "$DEST_PARENT"
rm -rf "$DEST"
docker cp "$CONTAINER:$SSTABLE_DIR" "$DEST_PARENT/"
ls -la "$DEST"

log "Generating sstabledump JSONL golden..."
DATA_DB="$(ls "$DEST"/da-*-Data.db | head -1)"
BASE="$(basename "$DATA_DB")"
REL="${KS}/${DEST_NAME}/${BASE}"
docker exec "$CONTAINER" bash -lc "ls -la '$SSTABLE_DIR'/$BASE"
# sstabledump runs inside the container against the container path.
docker exec "$CONTAINER" bash -lc \
  "/opt/cassandra/tools/bin/sstabledump '$SSTABLE_DIR/$BASE' -l" > "$DEST/${BASE}.jsonl" || {
    log "WARN: sstabledump -l failed; trying without -l"
    docker exec "$CONTAINER" bash -lc "/opt/cassandra/tools/bin/sstabledump '$SSTABLE_DIR/$BASE'" > "$DEST/${BASE}.json" || true
  }
log "JSONL golden lines: $(wc -l < "$DEST/${BASE}.jsonl" 2>/dev/null || echo '?')"

log "DONE. Fixture at: $DEST"
log "Files:"; ls -la "$DEST"

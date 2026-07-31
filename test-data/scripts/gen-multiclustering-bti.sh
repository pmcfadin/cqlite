#!/usr/bin/env bash
# gen-multiclustering-bti.sh — Generate a COMPOUND-CLUSTERING BTI (da) fixture whose
# per-partition Rows.db row-index tries are structurally NON-TRIVIAL, for issue #3032.
#
# Why a second wide-partition BTI fixture (test_da/wide_table already exists):
#
#   wide_table is `PRIMARY KEY (pk, ck)` with a single `int` clustering column, so
#   every row-index separator is `40 80 00 00 <byte>` and the whole trie degenerates
#   into a chain of single-transition nodes.  Its root's only child is therefore a
#   2-byte SINGLE_NOPAYLOAD_4 node — which is exactly why the pre-#3002 root base
#   (`RowsOffset + key_length`, 2 bytes low, missing `writeWithShortLength`'s u16
#   prefix) landed on that child's FIRST byte and parsed benignly instead of
#   erroring.  A fixture whose root child is WIDER than 2 bytes makes `root - 2`
#   land MID-node, which is the discriminating property this fixture exists to
#   provide.  Its three partitions are also structurally identical, so it cannot
#   discriminate anything that depends on per-partition trie shape.
#
# What this produces instead:
#
#   PRIMARY KEY (pk, bucket, seq) — a compound clustering key of TWO components of
#   DIFFERING types (`text`, `int`).  Bucket names deliberately have DISTINCT FIRST
#   BYTES and HETEROGENEOUS LENGTHS, so the OSS50 byte-comparable separators branch
#   immediately below the trie root and the root's last child is a multi-transition
#   (Sparse/Dense) node rather than a 2-byte single.  Each partition gets a
#   DIFFERENT (bucket-count x rows-per-bucket) shape, so the three tries differ.
#
#   `column_index_size` is lowered from the BTI default (16 KiB) so partitions only
#   a few tens of KiB wide still get a MULTI-BLOCK row index.  That keeps the
#   committed fixture small (the sstabledump JSONL golden is dominated by row
#   payload text): wide_table's golden is ~1.9 MB, this one is a small fraction of
#   that.  It is a stock Cassandra yaml knob — the emitted bytes are ordinary
#   Cassandra-written `da` SSTables, only the block granularity differs.
#
# Output:
#   $OUT/sstables/test_da/<TBL>-<hash>/da-*-{Data,Partitions,Rows,...}.db
#   $OUT/sstables/test_da/<TBL>-<hash>/da-*-Data.db.jsonl   (sstabledump -l golden)
#
# Fail-closed: an empty Rows.db, a single-block row index, a missing component or an
# empty JSONL golden all exit non-zero rather than emitting a useless fixture.
set -euo pipefail

IMAGE="${IMAGE:-cassandra:5.0.2}"
CONTAINER="${CONTAINER:-cqlite-mcbti}"
DOCKER="${DOCKER:-docker}"
# Default to THIS checkout's test-data/datasets (script lives in test-data/scripts).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-$(cd "$SCRIPT_DIR/.." && pwd)/datasets}"

KS="${KS:-test_da}"
TBL="${TBL:-multiclustering_table}"

# Row-index granularity (stock cassandra.yaml knob; BTI default is 16KiB).  Smaller
# => more blocks per partition => a richer trie for a given amount of data.
COLUMN_INDEX_SIZE="${COLUMN_INDEX_SIZE:-2KiB}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"

# Partition shapes: `pk:bucket_count:rows_per_bucket`, comma separated.  Deliberately
# NON-uniform so the three per-partition tries are structurally different (issue
# #3032 scope (c)).  Bucket names are drawn in order from BUCKET_NAMES below.
SHAPES="${SHAPES:-1:3:60,2:5:32,3:8:16}"

# Distinct FIRST BYTES (a,b,c,...) and heterogeneous LENGTHS: transition-byte spread
# at trie depth 1 is what widens the root's child off a 2-byte single node, and mixed
# component lengths keep the byte-comparable encoding from collapsing into a chain.
BUCKET_NAMES="${BUCKET_NAMES:-alpha,bo,charlie-extended-bucket,delta,ep,foxtrot-long-bucket-name,golf,hh,india-bucket,jj}"

# Minimum acceptable per-partition block count (a 1-block partition has no usable
# row index).  See the fail-close below.
MIN_BLOCKS="${MIN_BLOCKS:-2}"

log() { echo "[gen-multiclustering-bti] $*"; }

# Keep the container on failure for diagnosis; remove only on clean success.
KEEP_ON_FAIL=1
cleanup() {
  local code=$?
  if [[ $code -ne 0 && "${KEEP_ON_FAIL}" == "1" ]]; then
    log "FAILED (exit $code). Dumping last container logs; leaving container '$CONTAINER' for inspection."
    $DOCKER logs --tail 60 "$CONTAINER" 2>&1 || true
  else
    $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_ready() {
  local label="$1" max="${2:-90}" delay=5
  log "Waiting for Cassandra to be ready ($label, max ${max}x${delay}s)..."
  for i in $(seq 1 "$max"); do
    if $DOCKER exec "$CONTAINER" cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra ready ($label, attempt $i)."
      return 0
    fi
    sleep "$delay"
  done
  log "FATAL: Cassandra not ready ($label) after $((max*delay))s"
  return 1
}

log "Removing any stale container..."
$DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true

log "Starting $IMAGE as $CONTAINER (pulls image if absent)..."
$DOCKER run -d --name "$CONTAINER" "$IMAGE" >/dev/null

# Let the FIRST boot complete fully before reconfiguring — restarting mid-boot
# stalls Cassandra.
wait_ready "initial boot" 90

log "Setting storage_compatibility_mode: NONE + sstable.selected_format: bti + column_index_size: ${COLUMN_INDEX_SIZE}..."
$DOCKER exec "$CONTAINER" bash -lc \
  "sed -i 's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g; s|#sstable:|sstable:|; s|#  selected_format: big|  selected_format: bti|; s|^# column_index_size: .*|column_index_size: ${COLUMN_INDEX_SIZE}|' /etc/cassandra/cassandra.yaml"
$DOCKER exec "$CONTAINER" bash -lc "grep -E '^storage_compatibility_mode|^sstable:|selected_format|^column_index_size' /etc/cassandra/cassandra.yaml"

# Fail closed if the column_index_size edit did not take — otherwise we would
# silently generate a 16KiB-granularity fixture and wonder why Rows.db is thin.
if ! $DOCKER exec "$CONTAINER" bash -lc "grep -qE '^column_index_size: ${COLUMN_INDEX_SIZE}\$' /etc/cassandra/cassandra.yaml"; then
  log "FATAL: column_index_size was not set to ${COLUMN_INDEX_SIZE} in cassandra.yaml"
  exit 1
fi

log "Restarting container to apply BTI mode..."
$DOCKER restart "$CONTAINER" >/dev/null
wait_ready "BTI mode" 90

log "Building schema + inserts (shapes=${SHAPES}, payload=${PAYLOAD_BYTES}B) into one CQL file..."
CQL_FILE="$(mktemp /tmp/mc-bti.XXXXXX.cql)"
python3 - "$CQL_FILE" "$KS" "$TBL" "$SHAPES" "$PAYLOAD_BYTES" "$BUCKET_NAMES" <<'PYEOF'
import sys

out, ks, tbl, shapes, pbytes, bucket_names = (
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6]
)
buckets = [b for b in bucket_names.split(",") if b]
if len({b[0] for b in buckets}) != len(buckets):
    sys.exit("FATAL: bucket names must have DISTINCT first bytes (they drive the "
             "trie's depth-1 transition spread)")

parsed = []
for part in shapes.split(","):
    pk, nb, rows = part.split(":")
    pk, nb, rows = int(pk), int(nb), int(rows)
    if nb > len(buckets):
        sys.exit(f"FATAL: shape {part} wants {nb} buckets but only {len(buckets)} names given")
    parsed.append((pk, nb, rows))
if len(parsed) < 3:
    sys.exit("FATAL: need >= 3 partitions (issue #3032 scope)")
if len({(nb, rows) for _, nb, rows in parsed}) < len(parsed):
    sys.exit("FATAL: partition shapes must DIFFER from each other (issue #3032 scope (c))")

total = 0
with open(out, "w") as f:
    f.write(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = "
            "{'class':'SimpleStrategy','replication_factor':1};\n")
    f.write(f"USE {ks};\n")
    f.write(f"CREATE TABLE IF NOT EXISTS {tbl} (pk int, bucket text, seq int, payload text, "
            f"PRIMARY KEY (pk, bucket, seq)) WITH compression = {{'class':'LZ4Compressor'}};\n")
    for pk, nb, rows in parsed:
        for b in buckets[:nb]:
            for seq in range(rows):
                seed = f"p{pk}{b}s{seq}-"
                payload = (seed * ((pbytes // len(seed)) + 1))[:pbytes]
                f.write(f"INSERT INTO {tbl} (pk, bucket, seq, payload) VALUES "
                        f"({pk}, '{b}', {seq}, '{payload}');\n")
                total += 1
print(f"[gen-multiclustering-bti] wrote {out} ({total} rows)")
PYEOF

$DOCKER cp "$CQL_FILE" "$CONTAINER:/tmp/mc.cql"
log "Applying schema + inserts via cqlsh -f (this can take a moment)..."
$DOCKER exec "$CONTAINER" cqlsh -f /tmp/mc.cql
rm -f "$CQL_FILE"

log "Flushing + compacting ${KS}.${TBL}..."
$DOCKER exec "$CONTAINER" nodetool flush "$KS" "$TBL"
$DOCKER exec "$CONTAINER" nodetool compact "$KS" "$TBL"

log "Locating ${TBL} SSTable dir in container..."
SSTABLE_DIR="$($DOCKER exec "$CONTAINER" bash -lc "ls -d /var/lib/cassandra/data/${KS}/${TBL}-* | head -1")"
log "Container SSTable dir: $SSTABLE_DIR"
$DOCKER exec "$CONTAINER" bash -lc "ls -la '$SSTABLE_DIR'"

# --- fail-close 1: a single compacted generation ------------------------------
GEN_COUNT="$($DOCKER exec "$CONTAINER" bash -lc "ls '$SSTABLE_DIR'/da-*-Data.db | wc -l")"
if [[ "${GEN_COUNT:-0}" -ne 1 ]]; then
  log "FATAL: expected exactly 1 Data.db after compact, found ${GEN_COUNT}"
  exit 1
fi

# --- fail-close 2: Rows.db must be populated ----------------------------------
ROWS_DB_SIZE="$($DOCKER exec "$CONTAINER" bash -lc "stat -c %s '$SSTABLE_DIR'/da-*-Rows.db 2>/dev/null | head -1 || echo 0")"
log "Rows.db size: ${ROWS_DB_SIZE} bytes"
if [[ "${ROWS_DB_SIZE:-0}" -lt 1 ]]; then
  log "FATAL: Rows.db is empty — partitions did not exceed column_index_size (${COLUMN_INDEX_SIZE})."
  log "       Increase the rows-per-bucket in SHAPES or PAYLOAD_BYTES, or lower COLUMN_INDEX_SIZE."
  exit 1
fi
log "Rows.db is populated (${ROWS_DB_SIZE} bytes)."

DEST_PARENT="$OUT/sstables/${KS}"
DEST_NAME="$(basename "$SSTABLE_DIR")"
DEST="$DEST_PARENT/$DEST_NAME"
log "Copying SSTable to $DEST ..."
mkdir -p "$DEST_PARENT"
rm -rf "$DEST"
$DOCKER cp "$CONTAINER:$SSTABLE_DIR" "$DEST_PARENT/"

# `docker cp` writes as the docker CLI's effective user — root when DOCKER='sudo
# docker' (the usual case on a box where the invoking user is not yet in the docker
# group).  Normalize ownership before we write the JSONL golden INTO this directory,
# otherwise the redirect below dies with EACCES after a full generation run.
if [[ ! -w "$DEST" ]]; then
  log "Fixture dir is not writable (docker cp ran as another user); reclaiming ownership..."
  ${SUDO:-sudo} chown -R "$(id -u):$(id -g)" "$DEST"
fi
ls -la "$DEST"

# --- fail-close 3: every component we depend on is present --------------------
for comp in Data Partitions Rows Statistics TOC.txt; do
  if ! ls "$DEST"/da-*-"$comp"* >/dev/null 2>&1; then
    log "FATAL: exported fixture is missing the $comp component"
    exit 1
  fi
done

log "Generating sstabledump JSONL golden..."
DATA_DB="$(ls "$DEST"/da-*-Data.db | head -1)"
BASE="$(basename "$DATA_DB")"
# sstabledump runs inside the container against the container path.
$DOCKER exec "$CONTAINER" bash -lc \
  "/opt/cassandra/tools/bin/sstabledump '$SSTABLE_DIR/$BASE' -l" > "$DEST/${BASE}.jsonl"

JSONL_LINES="$(wc -l < "$DEST/${BASE}.jsonl")"
log "JSONL golden lines: ${JSONL_LINES}"
if [[ "${JSONL_LINES:-0}" -lt 1 ]]; then
  log "FATAL: sstabledump -l produced an empty golden"
  exit 1
fi

# --- fail-close 4: >1 row-index block per indexed partition -------------------
# `TrieIndexEntry.blockCount` is not printed by any CLI tool, so the block count is
# derived structurally: Cassandra emits a Rows.db row index ONLY for a partition
# split across more than one block, so `#entries in Rows.db > 0` plus a Rows.db
# large enough to hold multi-block tries is the shell-level guard.  The exact
# per-partition block counts are asserted by the Rust decode harness (issue #3032),
# which is the authoritative check.  Here we only refuse a degenerate index.
MIN_ROWS_DB=$(( MIN_BLOCKS * 3 * 8 ))
if [[ "${ROWS_DB_SIZE}" -lt "${MIN_ROWS_DB}" ]]; then
  log "FATAL: Rows.db (${ROWS_DB_SIZE} B) is too small to hold >= ${MIN_BLOCKS} blocks for 3 partitions"
  exit 1
fi

log "DONE. Fixture at: $DEST"
log "Files:"; ls -la "$DEST"
log "Total fixture size: $(du -sh "$DEST" | cut -f1)"

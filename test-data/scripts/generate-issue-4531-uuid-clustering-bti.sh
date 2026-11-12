#!/usr/bin/env bash
# generate-issue-4531-uuid-clustering-bti.sh — Cassandra 5.0.8-written BTI (`da`) SSTables with
# `timeuuid` / `uuid` CLUSTERING columns and multi-block row indexes (issue #4531).
# Schema + rationale: test-data/schemas/issue-4531-uuid-clustering-bti.cql (READ IT).
#
# BTI is selected the way gen-multiclustering-bti.sh does: `storage_compatibility_mode: NONE`,
# `sstable.selected_format: bti`, then a container restart. `column_index_size` is lowered to
# 2KiB (a stock knob) so a ~50 KB partition still gets a multi-block `Rows.db`; the emitted bytes
# are ordinary Cassandra-written `da` SSTables. Every INSERT is `USING TIMESTAMP 1000`; each
# table is flushed and compacted to exactly one SSTable.
#
# VALUES ARE DETERMINISTIC: timeuuids are version-1 UUIDs built from fixed (timestamp, clock_seq,
# node); `uuid_clustering` interleaves version-1 and version-4 (seeded) values. The script prints
# the Rows.db size and FAILS if it is too small to hold several blocks.
#
# CONTAINER ENGINE: `docker` by default; the owner-authorized VM is driven with
#   CQLITE_CONTAINER_ENGINE='incus exec wintermute:cassandra --project wm-sandboxes -- docker'
# Only the container named below is touched (fails closed if it exists; removed only if this run
# created it). Output is checkout-relative (`test-data/fixtures/issue_4531/`) so a
# CQLITE_DATASETS_ROOT export cannot hide it; `*.db` must be `git add -f`ed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4531}"
CONTAINER_NAME="cqlite-issue4531-uuidbti"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_uuid_bti"
TABLES=(timeuuid_clustering timeuuid_clustering_desc uuid_clustering)
SCHEMA_FILE="$ROOT/schemas/issue-4531-uuid-clustering-bti.cql"
BIG_ROWS="${BIG_ROWS:-480}"; SMALL_ROWS="${SMALL_ROWS:-8}"; PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
COLUMN_INDEX_SIZE="${COLUMN_INDEX_SIZE:-2KiB}"
MIN_ROWS_DB="${MIN_ROWS_DB:-200}"
read -r -a ENGINE <<<"${CQLITE_CONTAINER_ENGINE:-docker}"

while [[ $# -gt 0 ]]; do
  case "$1" in --out) OUT_DIR="$2"; shift 2 ;; *) echo "[i4531] Unknown argument: $1" >&2; exit 2 ;; esac
done
log()  { echo "[i4531] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4531][ERROR] $*" >&2; exit 1; }

[[ -n "${OUT_DIR:-}" && -n "$KEYSPACE" ]] || fail "OUT_DIR/KEYSPACE empty"
[[ "$OUT_DIR" == /* ]] || OUT_DIR="$PWD/$OUT_DIR"
OUT_DIR="$(realpath -m "$OUT_DIR")"; OUT_DIR="${OUT_DIR%/}"
case "$OUT_DIR" in /|/tmp) fail "Refusing '$OUT_DIR'" ;; esac
[[ "$OUT_DIR" == "$REPO_ROOT/"* || "$OUT_DIR" == /tmp/* ]] || fail "OUT_DIR '$OUT_DIR' not under the repo or /tmp/"

cx() { "${ENGINE[@]}" exec "$CONTAINER_NAME" "$@" </dev/null; }
"${ENGINE[@]}" inspect "$CONTAINER_NAME" >/dev/null 2>&1 && fail "Container '$CONTAINER_NAME' exists; remove it first"
CREATED=0
cleanup() { [[ "$CREATED" -eq 1 ]] && "${ENGINE[@]}" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

wait_ready() {
  for i in $(seq 1 90); do
    cx cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1 && { log "ready ($1, attempt $i)"; return 0; }
    sleep 5
  done
  fail "Cassandra not ready ($1)"
}

log "Starting $CASSANDRA_IMAGE ($CONTAINER_NAME)"
"${ENGINE[@]}" run -d --name "$CONTAINER_NAME" -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256m "$CASSANDRA_IMAGE" >/dev/null
CREATED=1
log "image id: $("${ENGINE[@]}" inspect --format '{{.Image}}' "$CONTAINER_NAME")"
wait_ready "initial boot"
log "Selecting BTI + column_index_size=$COLUMN_INDEX_SIZE; restarting"
cx bash -lc "sed -i 's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g; s|#sstable:|sstable:|; s|#  selected_format: big|  selected_format: bti|; s|^# column_index_size: .*|column_index_size: $COLUMN_INDEX_SIZE|' /etc/cassandra/cassandra.yaml"
cx bash -lc "grep -E '^storage_compatibility_mode|^sstable:|selected_format|^column_index_size' /etc/cassandra/cassandra.yaml"
cx bash -lc "grep -qE '^column_index_size: $COLUMN_INDEX_SIZE\$' /etc/cassandra/cassandra.yaml" || fail "column_index_size not applied"
"${ENGINE[@]}" restart "$CONTAINER_NAME" >/dev/null
wait_ready "BTI mode"
log "cassandra version: $(cx nodetool version | tr -d '\r')"

log "Applying schema"
"${ENGINE[@]}" exec -i "$CONTAINER_NAME" cqlsh < "$SCHEMA_FILE" || fail "schema apply failed"

CQL="$(mktemp /tmp/i4531.XXXXXX.cql)"
python3 - "$CQL" "$KEYSPACE" "$BIG_ROWS" "$SMALL_ROWS" "$PAYLOAD_BYTES" <<'PY'
import sys, random, uuid
out, ks, big, small, pbytes = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
def v1(i, salt):
    # version-1 UUID from a fixed 100ns timestamp, clock_seq and node: fully deterministic.
    ts = 0x01EF000000000000 + (i * 7919 + salt * 104729) * 10_000_000 + salt  # spread, monotone in i
    return uuid.UUID(fields=(ts & 0xFFFFFFFF, (ts >> 32) & 0xFFFF, ((ts >> 48) & 0x0FFF) | 0x1000,
                              0x80 | ((0x1234 >> 8) & 0x3F), 0x1234 & 0xFF, 0x0A0B0C0D0E0F))
rng = random.Random(4531)
def v4():
    return uuid.UUID(int=rng.getrandbits(128), version=4)
def payload(seed):
    s = f"{seed}-"
    return (s * (pbytes // len(s) + 1))[:pbytes]
with open(out, "w") as f:
    f.write(f"USE {ks};\n")
    for table in ("timeuuid_clustering", "timeuuid_clustering_desc", "uuid_clustering"):
        for pk, n in ((1, big), (2, small)):
            for i in range(n):
                if table == "uuid_clustering":
                    ck = v1(i, pk) if i % 2 == 0 else v4()
                else:
                    ck = v1(i, pk)
                f.write(f"INSERT INTO {table} (pk, ck, payload) VALUES ({pk}, {ck}, '{payload(f'{table}{pk}-{i}')}') USING TIMESTAMP 1000;\n")
PY
log "Inserting ($(wc -l < "$CQL") statements)"
"${ENGINE[@]}" exec -i "$CONTAINER_NAME" cqlsh < "$CQL" || fail "insert failed"
rm -f "$CQL"

for t in "${TABLES[@]}"; do
  cx nodetool flush "$KEYSPACE" "$t"
  cx nodetool compact "$KEYSPACE" "$t"
done

mkdir -p "$OUT_DIR"; TMPX="$OUT_DIR/.i4531_export_tmp"; rm -rf "$TMPX"; mkdir -p "$TMPX"
"${ENGINE[@]}" exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' </dev/null | tar -C "$TMPX" -xf - || fail "export failed"
[[ -d "$TMPX/data/$KEYSPACE" ]] || fail "export missing keyspace"
rm -rf "${OUT_DIR:?}/$KEYSPACE"; mkdir -p "$OUT_DIR/$KEYSPACE"
cp -r "$TMPX/data/$KEYSPACE/." "$OUT_DIR/$KEYSPACE/"; rm -rf "$TMPX"

for t in "${TABLES[@]}"; do
  d=( "$OUT_DIR/$KEYSPACE/$t"-* ); d="${d[0]}"
  [[ "$(find "$d" -name 'da-*-bti-Data.db' | wc -l)" -eq 1 ]] || fail "$t: expected exactly one da-*-bti Data.db"
  rows="$(stat -c %s "$d"/da-*-bti-Rows.db)"
  log "$t: Rows.db = $rows bytes"
  [[ "$rows" -ge "$MIN_ROWS_DB" ]] || fail "$t: Rows.db too small ($rows < $MIN_ROWS_DB): partition did not span several index blocks"
  base="$(basename "$(ls "$d"/da-*-bti-Data.db)")"
  inpath="/var/lib/cassandra/data/$KEYSPACE/$(basename "$d")/$base"
  cx bash -lc "/opt/cassandra/tools/bin/sstabledump '$inpath' -l" 2>/dev/null | python3 -c "
import json, sys
for line in sys.stdin:
    line = line.strip()
    if line.startswith('{') or line.startswith('['):
        print(json.dumps(json.loads(line), separators=(',', ':')))
" > "$d/$base.jsonl"
  [[ -s "$d/$base.jsonl" ]] || fail "$t: EMPTY golden"
  cx bash -lc "/opt/cassandra/tools/bin/sstablemetadata '$inpath'" > "$d/${base%Data.db}Statistics.db.txt" 2>/dev/null || true
done
log "DONE: $OUT_DIR/$KEYSPACE"
echo "  git add -f '$OUT_DIR'/$KEYSPACE/*/*.db ; git add '$OUT_DIR'/$KEYSPACE/*/*.jsonl '$OUT_DIR'/$KEYSPACE/*/*TOC.txt '$OUT_DIR'/$KEYSPACE/*/*.crc32 '$OUT_DIR'/$KEYSPACE/*/*.db.txt"

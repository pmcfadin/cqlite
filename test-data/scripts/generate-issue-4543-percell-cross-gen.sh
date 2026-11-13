#!/usr/bin/env bash
# generate-issue-4543-percell-cross-gen.sh — Cassandra 5.0.8-written fixture whose NEWER
# generation holds OLDER per-cell timestamps (issue #4543). Case table:
# test-data/schemas/issue-4543-percell-cross-gen.cql (READ IT).
#
# Flow (ONE temp container, autocompaction OFF): generation 1 (flush), sleep, generation 2 (flush);
# export both SSTables (nb-1, nb-2) -> <out>/ordered/ (bytes as written); capture Cassandra's own
# `SELECT` over the two uncompacted generations -> cassandra-select.txt (THE ORACLE); then
# `nodetool compact` -> sstabledump golden of Cassandra's reconcile -> compacted.jsonl.
#
# CONTAINER ENGINE: `docker` by default; the owner-authorized VM is driven with
#   CQLITE_CONTAINER_ENGINE='incus exec wintermute:cassandra --project wm-sandboxes -- docker'
# Only the container named below is touched. Output is checkout-relative
# (test-data/fixtures/issue_4543/); `*.db` must be `git add -f`ed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4543}"
CONTAINER_NAME="cqlite-issue4543-percell"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_percell"; TABLE="pcell"
SCHEMA_FILE="$ROOT/schemas/issue-4543-percell-cross-gen.cql"
read -r -a ENGINE <<<"${CQLITE_CONTAINER_ENGINE:-docker}"
while [[ $# -gt 0 ]]; do case "$1" in --out) OUT_DIR="$2"; shift 2 ;; *) echo "[i4543] Unknown argument: $1" >&2; exit 2 ;; esac; done
log()  { echo "[i4543] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4543][ERROR] $*" >&2; exit 1; }

[[ "$OUT_DIR" == /* ]] || OUT_DIR="$PWD/$OUT_DIR"
OUT_DIR="$(realpath -m "$OUT_DIR")"; OUT_DIR="${OUT_DIR%/}"
case "$OUT_DIR" in /|/tmp) fail "Refusing '$OUT_DIR'" ;; esac
[[ "$OUT_DIR" == "$REPO_ROOT/"* || "$OUT_DIR" == /tmp/* ]] || fail "OUT_DIR '$OUT_DIR' not under the repo or /tmp/"

cx()  { "${ENGINE[@]}" exec "$CONTAINER_NAME" "$@" </dev/null; }
cql() { cx cqlsh -k "$KEYSPACE" -e "$1"; }
"${ENGINE[@]}" inspect "$CONTAINER_NAME" >/dev/null 2>&1 && fail "Container '$CONTAINER_NAME' exists; remove it first"
CREATED=0
cleanup() { [[ "$CREATED" -eq 1 ]] && "${ENGINE[@]}" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

log "Starting $CASSANDRA_IMAGE ($CONTAINER_NAME)"
"${ENGINE[@]}" run -d --name "$CONTAINER_NAME" -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256m "$CASSANDRA_IMAGE" >/dev/null
CREATED=1
log "image id: $("${ENGINE[@]}" inspect --format '{{.Image}}' "$CONTAINER_NAME")"
for i in $(seq 1 90); do cx cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1 && break; [[ "$i" -lt 90 ]] || fail "not ready"; sleep 5; done
log "cassandra version: $(cx nodetool version | tr -d '\r')"
"${ENGINE[@]}" exec -i "$CONTAINER_NAME" cqlsh < "$SCHEMA_FILE" || fail "schema apply failed"
cx nodetool disableautocompaction "$KEYSPACE" "$TABLE"

log "generation 1 (OLDER file)"
cql "UPDATE pcell USING TIMESTAMP 300 SET a = 'a-gen1' WHERE pk = 1 AND ck = 1"
cql "UPDATE pcell USING TIMESTAMP 100 SET b = 'b-gen1' WHERE pk = 1 AND ck = 1"
cql "UPDATE pcell USING TIMESTAMP 500 SET c = 'c-gen1' WHERE pk = 1 AND ck = 1"
cql "DELETE FROM pcell USING TIMESTAMP 200 WHERE pk = 1 AND ck = 2"
cql "UPDATE pcell USING TIMESTAMP 100 SET a = 'a-gen1' WHERE pk = 1 AND ck = 3"
cql "DELETE FROM pcell USING TIMESTAMP 200 WHERE pk = 1 AND ck = 4"
cql "UPDATE pcell USING TIMESTAMP 100 SET a = 'p2-gen1' WHERE pk = 2 AND ck = 1"
cql "DELETE FROM pcell USING TIMESTAMP 200 WHERE pk = 1 AND ck = 5"
cx nodetool flush "$KEYSPACE" "$TABLE"
sleep 2
log "generation 2 (NEWER file; some cells carry OLDER timestamps)"
cql "UPDATE pcell USING TIMESTAMP 100 SET a = 'a-gen2' WHERE pk = 1 AND ck = 1"
cql "UPDATE pcell USING TIMESTAMP 400 SET b = 'b-gen2' WHERE pk = 1 AND ck = 1"
cql "UPDATE pcell USING TIMESTAMP 150 SET a = 'a-gen2' WHERE pk = 1 AND ck = 2"
cql "UPDATE pcell USING TIMESTAMP 300 SET b = 'b-gen2' WHERE pk = 1 AND ck = 2"
cql "DELETE a FROM pcell USING TIMESTAMP 50 WHERE pk = 1 AND ck = 3"
cql "UPDATE pcell USING TIMESTAMP 100 SET a = 'a-gen2' WHERE pk = 1 AND ck = 4"
cql "UPDATE pcell USING TIMESTAMP 100 SET b = 'p2-b-gen2' WHERE pk = 3 AND ck = 1"
cql "DELETE a FROM pcell USING TIMESTAMP 300 WHERE pk = 1 AND ck = 5"
cx nodetool flush "$KEYSPACE" "$TABLE"

INDIR="/var/lib/cassandra/data/$KEYSPACE"
TDIR="$(cx bash -lc "ls -d $INDIR/$TABLE-* | head -1" | tr -d '\r')"
[[ "$(cx bash -lc "ls $TDIR/nb-*-big-Data.db | wc -l" | tr -d '\r')" -eq 2 ]] || fail "expected exactly two Data.db generations before compaction"

mkdir -p "$OUT_DIR"; rm -rf "${OUT_DIR:?}/ordered"
mkdir -p "$OUT_DIR/ordered/$KEYSPACE/$(basename "$TDIR")"
DEST="$OUT_DIR/ordered/$KEYSPACE/$(basename "$TDIR")"
cx bash -lc "tar -C $TDIR -cf - ." | tar -C "$DEST" -xf - || fail "export failed"
log "Cassandra's own SELECT over the two uncompacted generations"
cx cqlsh --no-color -k "$KEYSPACE" -e "SELECT pk, ck, a, b, c, writetime(a), writetime(b), writetime(c) FROM pcell" > "$OUT_DIR/cassandra-select.txt"
cat "$OUT_DIR/cassandra-select.txt"

log "Cassandra's reconcile: nodetool compact -> sstabledump golden"
cx nodetool compact "$KEYSPACE" "$TABLE"
CDATA="$(cx bash -lc "ls $TDIR/nb-*-big-Data.db" | tr -d '\r')"
[[ "$(echo "$CDATA" | wc -l)" -eq 1 ]] || fail "expected exactly one compacted Data.db"
cx bash -lc "/opt/cassandra/tools/bin/sstabledump '$CDATA' -l" 2>/dev/null | python3 -c "
import json, sys
for line in sys.stdin:
    line = line.strip()
    if line.startswith('{') or line.startswith('['):
        print(json.dumps(json.loads(line), separators=(',', ':')))
" > "$OUT_DIR/compacted.jsonl"
[[ -s "$OUT_DIR/compacted.jsonl" ]] || fail "EMPTY compacted golden"
find "$OUT_DIR" -name '._*' -delete 2>/dev/null || true
log "DONE: $OUT_DIR"
echo "  git add -f '$OUT_DIR'/*/$KEYSPACE/*/*.db ; git add '$OUT_DIR'"

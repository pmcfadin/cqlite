#!/usr/bin/env bash
# generate-commitlog-find-fixtures.sh — `cqlite find` CommitLog+SSTable companion fixture (#4205)
#
# One REAL Cassandra 5.0 node, one table (commitlog_test.users), SIX inserts with EXPLICIT
# `USING TIMESTAMP` values, flushed in two phases so that some mutations are in an SSTable and
# the rest are ONLY in the CommitLog:
#
#   phase 1 (flushed)   : id=1 @T1, id=2 'bob'   @T2, id=3 @T3   -> nodetool flush -> nb-1-big-*
#   phase 2 (unflushed) : id=4 @T4, id=5 @T5, id=2 'bobby' @T6 (re-insert of a flushed key)
#
# Expected `find` verdicts (the oracle is what was INSERTED, never CQLite's output):
#   id=1,3, id=2@T2  -> unflushed=no   (writetime <= the SSTable's max for the key)
#   id=4, id=5, id=2@T6 -> unflushed=yes
#
# Outputs (under <out>/commitlog_find/):
#   commitlog/CommitLog-7-<id>.log             trimmed REAL segment
#   sstables/commitlog_test/users-<tableid>/   the flushed generation (+ sstabledump JSONL)
#   commitlog-find-ground-truth.json           inserts, timestamps, flushed flag, and the
#                                              expected (position) of each mutation record,
#                                              derived by commitlog_fixture_derivation.py's
#                                              INDEPENDENT Python frame walker.
#
# Usage: bash test-data/scripts/generate-commitlog-find-fixtures.sh --out <fresh dir under /tmp>
# Needs docker or podman. (On the shared dev box it is run inside the persistent `cassandra`
# Incus VM and the output pulled back.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-}"
CONTAINER_NAME="cqlite-commitlog-find"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="commitlog_test"
TABLE="users"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    *) echo "[clf] Unknown argument: $1" >&2; exit 1 ;;
  esac
done
log()  { echo "[clf] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[clf][ERROR] $*" >&2; exit 1; }

[[ -n "$OUT_DIR" ]] || fail "--out <dir> is required"
[[ "$OUT_DIR" == /* ]] || OUT_DIR="$PWD/$OUT_DIR"
OUT_DIR="${OUT_DIR%/}"
case "$OUT_DIR" in /|/tmp) fail "Refusing to use '$OUT_DIR'." ;; esac
[[ "$OUT_DIR" == /tmp/* ]] || fail "OUT_DIR '$OUT_DIR' must be under /tmp/ (copy results into test-data/datasets afterwards)."

if command -v docker >/dev/null 2>&1; then ENGINE=docker
elif command -v podman >/dev/null 2>&1; then ENGINE=podman
else fail "Neither docker nor podman found."; fi

FIND_DIR="$OUT_DIR/commitlog_find"
[[ ! -e "$FIND_DIR" ]] || fail "'$FIND_DIR' already exists; use a fresh --out."
$ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1 && fail "Container '$CONTAINER_NAME' exists: $ENGINE rm -f $CONTAINER_NAME"
trap '$ENGINE rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true' EXIT

log "Starting $CASSANDRA_IMAGE ($CONTAINER_NAME)..."
$ENGINE run -d --name "$CONTAINER_NAME" -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-commitlog-find "$CASSANDRA_IMAGE" >/dev/null
for i in $(seq 1 60); do
  $ENGINE exec "$CONTAINER_NAME" cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1 && break
  [[ $i -lt 60 ]] || fail "Cassandra did not become ready."
  sleep 5
done

# Explicit, fixed timestamps (microseconds) — the writetime ground truth.
T1=1794300001000000; T2=1794300002000000; T3=1794300003000000
T4=1794300004000000; T5=1794300005000000; T6=1794300006000000
cql() { $ENGINE exec "$CONTAINER_NAME" cqlsh -e "$1"; }

log "Schema + phase 1 inserts (to be flushed)..."
cql "CREATE KEYSPACE $KEYSPACE WITH replication = {'class':'SimpleStrategy','replication_factor':1};"
cql "CREATE TABLE $KEYSPACE.$TABLE (id int PRIMARY KEY, name text, age int);"
cql "INSERT INTO $KEYSPACE.$TABLE (id, name, age) VALUES (1, 'alice', 30) USING TIMESTAMP $T1;"
cql "INSERT INTO $KEYSPACE.$TABLE (id, name, age) VALUES (2, 'bob', 25) USING TIMESTAMP $T2;"
cql "INSERT INTO $KEYSPACE.$TABLE (id, name, age) VALUES (3, 'carol', 42) USING TIMESTAMP $T3;"
log "Flushing $KEYSPACE.$TABLE..."
$ENGINE exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE" "$TABLE"

log "Phase 2 inserts (stay ONLY in the CommitLog)..."
cql "INSERT INTO $KEYSPACE.$TABLE (id, name, age) VALUES (4, 'dave', 37) USING TIMESTAMP $T4;"
cql "INSERT INTO $KEYSPACE.$TABLE (id, name, age) VALUES (5, 'eve', 29) USING TIMESTAMP $T5;"
cql "INSERT INTO $KEYSPACE.$TABLE (id, name, age) VALUES (2, 'bobby', 26) USING TIMESTAMP $T6;"

TABLE_ID=$($ENGINE exec "$CONTAINER_NAME" cqlsh -e \
  "SELECT id FROM system_schema.tables WHERE keyspace_name='$KEYSPACE' AND table_name='$TABLE';" \
  | grep -Eo '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
[[ -n "$TABLE_ID" ]] || fail "could not read table id"
TABLE_DIR="$TABLE-${TABLE_ID//-/}"
log "table_id=$TABLE_ID dir=$TABLE_DIR"

log "Waiting for the periodic commitlog sync (>10s)..."
sleep 14

mkdir -p "$FIND_DIR/commitlog" "$FIND_DIR/sstables/$KEYSPACE"
SEG_PATH=$($ENGINE exec "$CONTAINER_NAME" bash -lc 'ls -1t /var/lib/cassandra/commitlog/CommitLog-*.log | head -1')
SEG_NAME=$(basename "$SEG_PATH")
RAW="$OUT_DIR/raw-$SEG_NAME"
$ENGINE exec "$CONTAINER_NAME" cat "$SEG_PATH" > "$RAW"

SST_SRC="/var/lib/cassandra/data/$KEYSPACE/$TABLE_DIR"
$ENGINE cp "$CONTAINER_NAME:$SST_SRC" "$FIND_DIR/sstables/$KEYSPACE/$TABLE_DIR"
# Only the flushed generation's components belong in the fixture (no snapshots/backups dirs).
rm -rf "$FIND_DIR/sstables/$KEYSPACE/$TABLE_DIR"/snapshots "$FIND_DIR/sstables/$KEYSPACE/$TABLE_DIR"/backups
for d in "$FIND_DIR/sstables/$KEYSPACE/$TABLE_DIR"/*-Data.db; do
  base=$(basename "$d")
  $ENGINE exec "$CONTAINER_NAME" /opt/cassandra/tools/bin/sstabledump "$SST_SRC/$base" \
    > "$FIND_DIR/sstables/$KEYSPACE/$TABLE_DIR/$base.jsonl"
done

log "Deriving trimmed segment + ground truth (independent Python frame walker)..."
python3 "$SCRIPT_DIR/commitlog_find_fixture.py" derive "$RAW" "$FIND_DIR" "$SEG_NAME" \
  "$TABLE_ID" "$TABLE_DIR" "$CASSANDRA_IMAGE" \
  "1:alice:30:$T1:1" "2:bob:25:$T2:1" "3:carol:42:$T3:1" \
  "4:dave:37:$T4:0" "5:eve:29:$T5:0" "2:bobby:26:$T6:0"
log "DONE: $FIND_DIR"

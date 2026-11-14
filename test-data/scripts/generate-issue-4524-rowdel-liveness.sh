#!/usr/bin/env bash
# generate-issue-4524-rowdel-liveness.sh — Cassandra 5.0.8-written primary-key LIVENESS vs ROW DELETION
# fixture (issue #4524, roborev job 307). Schema: test-data/schemas/issue-4524-rowdel-liveness.cql (READ IT).
#
# THE ORACLE IS CASSANDRA-WRITTEN BYTES + `sstabledump`, never a CQLite round-trip
# (CLAUDE.md, #3042). Every INSERT pins `USING TIMESTAMP`, then the keyspace is
# flushed once (`nodetool flush`), so each table is exactly one SSTable.
#
# OUTPUT: test-data/fixtures/issue_4524/<keyspace>/<table>-<uuid>/ — checkout-relative
# on purpose (see generate-issue-4114-vector-float.sh): a CQLITE_DATASETS_ROOT export
# must not be able to hide it. The *.db files are gitignored: `git add -f` them (the
# commands are printed at exit). Regeneration mints new table UUIDs and replaces the
# directories; use --out /tmp/... for a trial run.
#
# CONTAINER ENGINE: `docker` by default. The owner-authorized Cassandra VM is driven with
#   CQLITE_CONTAINER_ENGINE='incus exec wintermute:cassandra --project wm-sandboxes -- docker' \
#     bash test-data/scripts/generate-issue-4524-rowdel-liveness.sh
# Everything (cqlsh, nodetool, sstabledump, sstablemetadata) runs INSIDE the one container
# named below via `exec`; the SSTables are tar-streamed out, so no host path needs to be
# visible to the engine. Only that exact container name is ever touched (no image-wide
# filter), the pre-flight fails closed if it exists, and cleanup removes only a container
# this invocation created.
#
# Backs: issue #4524.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4524}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue4524-rowdel"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_rowdel"
SCHEMA_FILE="$ROOT/schemas/issue-4524-rowdel-liveness.cql"
T_FIXED=1000

read -r -a ENGINE <<<"${CQLITE_CONTAINER_ENGINE:-docker}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "[i4524] Unknown argument: $1" >&2; exit 2 ;;
  esac
done
log()  { echo "[i4524] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4524][ERROR] $*" >&2; exit 1; }

# GUARD BLOCK (the export step runs `rm -rf "$OUT_DIR/$KEYSPACE"`): non-empty,
# canonicalized BEFORE validating, absolute, and under the repo root or /tmp/.
[[ -n "${OUT_DIR:-}" ]]  || fail "OUT_DIR is EMPTY. Refusing: 'rm -rf \$OUT_DIR/\$KEYSPACE' would hit a parent."
[[ -n "${KEYSPACE:-}" ]] || fail "KEYSPACE is EMPTY. Refusing."
[[ "$OUT_DIR" == /* ]] || OUT_DIR="$PWD/$OUT_DIR"
command -v realpath >/dev/null 2>&1 || fail "realpath(1) is required."
OUT_DIR="$(realpath -m "$OUT_DIR")"; OUT_DIR="${OUT_DIR%/}"
[[ "$OUT_DIR" == /* && "${#OUT_DIR}" -ge 4 ]] || fail "OUT_DIR '$OUT_DIR' is not a sane absolute path."
case "$OUT_DIR" in /|/tmp) fail "Refusing to use '$OUT_DIR' directly." ;; esac
if [[ "$OUT_DIR" != "$REPO_ROOT/"* && "$OUT_DIR" != /tmp/* ]]; then
  fail "OUT_DIR '$OUT_DIR' is not under the repo root ($REPO_ROOT) or /tmp/. Refusing."
fi
log "OUT_DIR guard: '$OUT_DIR' accepted"

run() { if [[ "$DRY_RUN" -eq 1 ]]; then echo "[dry-run] $*"; else "$@"; fi; }
cx()  { "${ENGINE[@]}" exec "$CONTAINER_NAME" "$@" </dev/null; }
cql() { if [[ "$DRY_RUN" -eq 1 ]]; then echo "[dry-run] cqlsh -k $KEYSPACE -e \"$1\""; else cx cqlsh -k "$KEYSPACE" -e "$1"; fi; }

if [[ "$DRY_RUN" -eq 0 ]] && "${ENGINE[@]}" inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first: ${ENGINE[*]} rm -f $CONTAINER_NAME"
fi
CONTAINER_CREATED=0
cleanup() {
  if [[ "$DRY_RUN" -eq 0 && "$CONTAINER_CREATED" -eq 1 ]]; then
    log "Cleaning up container (created by this invocation)..."
    "${ENGINE[@]}" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

log "Starting $CASSANDRA_IMAGE ($CONTAINER_NAME)..."
run "${ENGINE[@]}" run -d --name "$CONTAINER_NAME" -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue4524 "$CASSANDRA_IMAGE" >/dev/null \
  || fail "container could not be started; refusing to remove one this run did not create."
[[ "$DRY_RUN" -eq 1 ]] || CONTAINER_CREATED=1

if [[ "$DRY_RUN" -eq 0 ]]; then
  for i in $(seq 1 60); do
    cx cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1 && { log "Cassandra ready (attempt $i)"; break; }
    [[ "$i" -lt 60 ]] || fail "Cassandra did not become ready in time."
    sleep 5
  done
  # The image digest actually used, recorded for provenance.
  log "image: $CASSANDRA_IMAGE id: $("${ENGINE[@]}" inspect --format '{{.Image}}' "$CONTAINER_NAME")"
  log "cassandra version: $(cx nodetool version | tr -d '\r')"
fi

# Schema is streamed over stdin (not `docker cp`): the engine may be remote (incus exec into a VM).
if [[ "$DRY_RUN" -eq 1 ]]; then echo "[dry-run] stream $SCHEMA_FILE | cqlsh"; else
  "${ENGINE[@]}" exec -i "$CONTAINER_NAME" cqlsh < "$SCHEMA_FILE" || fail "schema apply failed"
fi

log "=== writing rows (every statement USING TIMESTAMP) ==="
# A row whose liveness marker timestamp is <= a row deletion's markedForDeleteAt is DELETED
# (Cassandra 5.0.8 DeletionTime.deletes: ts <= markedForDeleteAt; BTreeRow.filter / Row.Merger),
# even when a LATER cell tombstone keeps the row's data alive in the SSTable. A marker strictly
# newer than the deletion survives.
#
# SINGLE_ROWS (one SSTable):
#  1 INSERT (id)@1000, DELETE row@2000, DELETE v@3000 -> NO ROW (marker older than deletion)
#  2 DELETE row@2000, DELETE v@2500, INSERT (id)@3000 -> ROW, v null (marker newer)
#  3 INSERT (id)@2000, DELETE row@2000, DELETE v@3000 -> NO ROW (tie: deletion wins)
# CLUSTERED_SINGLE: the same three cases as (pk=id, ck=1). On the CLUSTERED tables `DELETE FROM`
# is a true ROW deletion (a flat table's is a PARTITION deletion); the others cover the partition path.
# MULTIGEN_ROWS / CLUSTERED_MULTIGEN (two SSTables, phase A flushed first):
#  1 A: INSERT@1000            | B: DELETE row@2000, DELETE v@3000 -> NO ROW
#  2 A: DELETE row@2000, v@3000 | B: INSERT@1000                    -> NO ROW (marker in the newer file)
#  3 A: INSERT@2000            | B: DELETE row@2000, DELETE v@3000 -> NO ROW (tie)
#  4 A: DELETE row@2000, v@3000 | B: INSERT@2000                    -> NO ROW (tie, reversed)
#  5 A: INSERT@3000            | B: DELETE row@2000, DELETE v@2500 -> ROW, v null
#  6 A: DELETE row@2000, v@2500 | B: INSERT@3000                    -> ROW, v null
cql "INSERT INTO single_rows (id) VALUES (1) USING TIMESTAMP 1000"
cql "DELETE FROM single_rows USING TIMESTAMP 2000 WHERE id = 1"
cql "DELETE v FROM single_rows USING TIMESTAMP 3000 WHERE id = 1"
cql "DELETE FROM single_rows USING TIMESTAMP 2000 WHERE id = 2"
cql "DELETE v FROM single_rows USING TIMESTAMP 2500 WHERE id = 2"
cql "INSERT INTO single_rows (id) VALUES (2) USING TIMESTAMP 3000"
cql "INSERT INTO single_rows (id) VALUES (3) USING TIMESTAMP 2000"
cql "DELETE FROM single_rows USING TIMESTAMP 2000 WHERE id = 3"
cql "DELETE v FROM single_rows USING TIMESTAMP 3000 WHERE id = 3"
cs() { cql "$1"; }
cs "INSERT INTO clustered_single (pk, ck) VALUES (1, 1) USING TIMESTAMP 1000"
cs "DELETE FROM clustered_single USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 1"
cs "DELETE v FROM clustered_single USING TIMESTAMP 3000 WHERE pk = 1 AND ck = 1"
cs "DELETE FROM clustered_single USING TIMESTAMP 2000 WHERE pk = 2 AND ck = 1"
cs "DELETE v FROM clustered_single USING TIMESTAMP 2500 WHERE pk = 2 AND ck = 1"
cs "INSERT INTO clustered_single (pk, ck) VALUES (2, 1) USING TIMESTAMP 3000"
cs "INSERT INTO clustered_single (pk, ck) VALUES (3, 1) USING TIMESTAMP 2000"
cs "DELETE FROM clustered_single USING TIMESTAMP 2000 WHERE pk = 3 AND ck = 1"
cs "DELETE v FROM clustered_single USING TIMESTAMP 3000 WHERE pk = 3 AND ck = 1"
# phase A of the two-generation tables
mg_ins() { cql "INSERT INTO multigen_rows (id) VALUES ($1) USING TIMESTAMP $2"; cql "INSERT INTO clustered_multigen (pk, ck) VALUES ($1, 1) USING TIMESTAMP $2"; }
mg_del() { # id rowdel_ts celldel_ts
  cql "DELETE FROM multigen_rows USING TIMESTAMP $2 WHERE id = $1"
  cql "DELETE v FROM multigen_rows USING TIMESTAMP $3 WHERE id = $1"
  cql "DELETE FROM clustered_multigen USING TIMESTAMP $2 WHERE pk = $1 AND ck = 1"
  cql "DELETE v FROM clustered_multigen USING TIMESTAMP $3 WHERE pk = $1 AND ck = 1"
}
mg_ins 1 1000
mg_del 2 2000 3000
mg_ins 3 2000
mg_del 4 2000 3000
mg_ins 5 3000
mg_del 6 2000 2500
log "Flushing $KEYSPACE (phase A)..."
run "${ENGINE[@]}" exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"
mg_del 1 2000 3000
mg_ins 2 1000
mg_del 3 2000 3000
mg_ins 4 2000
mg_del 5 2000 2500
mg_ins 6 3000
log "Flushing $KEYSPACE (phase B)..."
run "${ENGINE[@]}" exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"

[[ "$DRY_RUN" -eq 0 ]] || { log "dry-run complete"; exit 0; }

log "=== Exporting SSTables ==="
mkdir -p "$OUT_DIR"
TMPX="$OUT_DIR/.i4524_export_tmp"; rm -rf "$TMPX"; mkdir -p "$TMPX"
cx bash -lc 'tar -C /var/lib/cassandra -cf - data' | tar -C "$TMPX" -xf - || fail "tar export failed."
[[ -d "$TMPX/data/$KEYSPACE" ]] || fail "Expected $TMPX/data/$KEYSPACE; export failed."
rm -rf "${OUT_DIR:?}/$KEYSPACE"; mkdir -p "$OUT_DIR/$KEYSPACE"
cp -r "$TMPX/data/$KEYSPACE/." "$OUT_DIR/$KEYSPACE/"; rm -rf "$TMPX"

for t in multigen_rows clustered_multigen; do
  cnt=$(find "$OUT_DIR/$KEYSPACE/$t"* -name '*-Data.db' | wc -l | tr -d ' ')
  [[ "$cnt" -eq 2 ]] || fail "$t: expected exactly TWO Data.db (two generations), found $cnt."
done
for t in single_rows clustered_single; do
  cnt=$(find "$OUT_DIR/$KEYSPACE/$t"* -name '*-Data.db' | wc -l | tr -d ' ')
  [[ "$cnt" -eq 1 ]] || fail "$t: expected exactly ONE Data.db, found $cnt."
done
for t in single_rows clustered_single multigen_rows clustered_multigen; do
  d=$(dirname "$(find "$OUT_DIR/$KEYSPACE/$t"* -name '*-Data.db' | head -1)")
  cx cqlsh -k "$KEYSPACE" -e "SELECT * FROM $t;" > "$d/cqlsh-select.txt" || fail "cqlsh SELECT failed for $t"
done

while IFS= read -r -d '' data_file; do
  rel="${data_file#"$OUT_DIR"/}"
  inpath="/var/lib/cassandra/data/$rel"
  log "  sstabledump: $rel"
  cx bash -lc "/opt/cassandra/tools/bin/sstabledump '$inpath' -l" | python3 -c "
import json, sys
for line in sys.stdin:
    line = line.strip()
    if line:
        print(json.dumps(json.loads(line), separators=(',', ':')))
" > "${data_file%.db}.db.jsonl"
  [[ -s "${data_file%.db}.db.jsonl" ]] || fail "EMPTY golden for $rel"
  cx bash -lc "/opt/cassandra/tools/bin/sstablemetadata '$inpath'" > "${data_file%Data.db}Statistics.db.txt" 2>/dev/null || true
done < <(find "$OUT_DIR/$KEYSPACE" -name '*-Data.db' -print0)

echo "  git -C '$REPO_ROOT' add -f '$OUT_DIR'/$KEYSPACE/*/*.db"
echo "  git -C '$REPO_ROOT' add '$OUT_DIR'/$KEYSPACE/*/*.jsonl '$OUT_DIR'/$KEYSPACE/*/*TOC.txt '$OUT_DIR'/$KEYSPACE/*/*.crc32 '$OUT_DIR'/$KEYSPACE/*/*.db.txt '$OUT_DIR'/$KEYSPACE/*/cqlsh-select*.txt"

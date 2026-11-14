#!/usr/bin/env bash
# generate-issue-4524-static-multigen.sh — Cassandra 5.0.8-written TWO-generation table with a live
# STATIC cell beside markerless / tombstone-only clustering rows (issue #4524, roborev job 297).
# Schema: test-data/schemas/issue-4524-static-multigen.cql (READ IT).
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
#     bash test-data/scripts/generate-issue-4524-static-multigen.sh
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
CONTAINER_NAME="cqlite-issue4524-staticmg"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_staticrow"
SCHEMA_FILE="$ROOT/schemas/issue-4524-static-multigen.cql"
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
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue4360 "$CASSANDRA_IMAGE" >/dev/null \
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

log "=== writing rows (every statement USING TIMESTAMP; two flushes -> two SSTables) ==="
# static_multigen (pk, ck, s STATIC, v). Expected cqlsh SELECT outcome (Cassandra 5.0.8), captured
# below in cqlsh-select.txt. A STATIC cell NEVER revives a clustering row (Cassandra
# AbstractRow.hasLiveData looks only at the row's own liveness + its own cells; the static row is a
# separate Row). The static row itself is returned, with ck null, when it has a live static cell.
#  pk1 control, liveness : gen1 INSERT (1,1,v='x')@1000, static s@1000 | gen2 DELETE v@2000
#                          -> (1,1) ROW (liveness@1000 survives) with s; NO separate static row (a ck row exists)
#  pk2 THE DEFECT SHAPE  : gen1 UPDATE (2,1) v='y'@1000 (NO liveness), static s@1000 | gen2 DELETE v@2000
#                          -> NO (2,1) row; the static row (2,null,s,null) IS returned
#  pk3 live row control  : gen1 INSERT (3,1,v='live')@1000 + static | -> (3,1) ROW
#  pk4 static only       : gen2 INSERT static only (4, s='only')@2000 -> static row (4,null,only,null)
cql "INSERT INTO static_multigen (pk, ck, v) VALUES (1, 1, 'x') USING TIMESTAMP 1000"
cql "INSERT INTO static_multigen (pk, s) VALUES (1, 'st1') USING TIMESTAMP 1000"
cql "UPDATE static_multigen USING TIMESTAMP 1000 SET v = 'y', s = 'st2' WHERE pk = 2 AND ck = 1"
cql "INSERT INTO static_multigen (pk, ck, v) VALUES (3, 1, 'live') USING TIMESTAMP 1000"
cql "INSERT INTO static_multigen (pk, s) VALUES (3, 'st3') USING TIMESTAMP 1000"
log "Flushing $KEYSPACE (gen 1)..."
run "${ENGINE[@]}" exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"
cql "DELETE v FROM static_multigen USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 1"
cql "DELETE v FROM static_multigen USING TIMESTAMP 2000 WHERE pk = 2 AND ck = 1"
cql "INSERT INTO static_multigen (pk, s) VALUES (4, 'only') USING TIMESTAMP 2000"
log "Flushing $KEYSPACE (gen 2)..."
run "${ENGINE[@]}" exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"

[[ "$DRY_RUN" -eq 0 ]] || { log "dry-run complete"; exit 0; }

log "=== Exporting SSTables ==="
mkdir -p "$OUT_DIR"
TMPX="$OUT_DIR/.i4524s_export_tmp"; rm -rf "$TMPX"; mkdir -p "$TMPX"
cx bash -lc 'tar -C /var/lib/cassandra -cf - data' | tar -C "$TMPX" -xf - || fail "tar export failed."
[[ -d "$TMPX/data/$KEYSPACE" ]] || fail "Expected $TMPX/data/$KEYSPACE; export failed."
rm -rf "${OUT_DIR:?}/$KEYSPACE"; mkdir -p "$OUT_DIR/$KEYSPACE"
cp -r "$TMPX/data/$KEYSPACE/." "$OUT_DIR/$KEYSPACE/"; rm -rf "$TMPX"

cnt=$(find "$OUT_DIR/$KEYSPACE/static_multigen"* -name '*-Data.db' | wc -l | tr -d ' ')
[[ "$cnt" -eq 2 ]] || fail "static_multigen: expected exactly TWO Data.db (two generations), found $cnt."
dm=$(dirname "$(find "$OUT_DIR/$KEYSPACE/static_multigen"* -name '*-Data.db' | head -1)")
cx cqlsh -k "$KEYSPACE" -e "SELECT * FROM static_multigen;" > "$dm/cqlsh-select.txt" || fail "cqlsh SELECT failed"

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
echo "  git -C '$REPO_ROOT' add '$OUT_DIR'/$KEYSPACE/*/*.jsonl '$OUT_DIR'/$KEYSPACE/*/*TOC.txt '$OUT_DIR'/$KEYSPACE/*/*.crc32 '$OUT_DIR'/$KEYSPACE/*/*.db.txt '$OUT_DIR'/$KEYSPACE/*/cqlsh-select.txt"

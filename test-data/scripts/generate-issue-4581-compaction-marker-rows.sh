#!/usr/bin/env bash
# generate-issue-4581-compaction-marker-rows.sh — Cassandra 5.0.8-written primary-key LIVENESS-MARKER
# compaction fixtures AND Cassandra's own compaction output (issue #4581).
# Schema: test-data/schemas/issue-4581-compaction-marker-rows.cql (READ IT).
#
# THE ORACLE IS CASSANDRA-WRITTEN BYTES + `sstabledump` + `nodetool compact`, never a CQLite
# round-trip (CLAUDE.md, #3042). Two oracles are kept per table:
#   * the INPUT SSTables           -> OUT_DIR/test_markerrows/<table>-<uuid>/
#   * Cassandra's COMPACTED output -> OUT_DIR/test_markerrows_compacted/<table>-<uuid>/
#     plus compaction-instant.json (the wall-clock second `nodetool compact` ran at, and the
#     table's gc_grace_seconds), which the CQLite test pins as `now` / `gc_before`.
#
# Every write pins `USING TIMESTAMP`; TTL'd writes pin a short TTL and the generator SLEEPS past
# expiry (and, for the *_gc tables, past the 1 s gc_grace) before compacting.
#
# The update_rows / update_rows_gc / purged_marker tables were added after the first eight; their
# fixtures come from a SEPARATE full run of this script into a scratch `--out` directory, copying
# only those three tables' directories (the first eight keep their committed bytes).
#
# OUTPUT is checkout-relative on purpose (see generate-issue-4114-vector-float.sh). The *.db files
# are gitignored: `git add -f` them (the commands are printed at exit).
#
# CONTAINER ENGINE: `docker` by default. The owner-authorized Cassandra VM is driven with
#   CQLITE_CONTAINER_ENGINE='incus exec wintermute:cassandra --project wm-sandboxes -- docker' \
#     bash test-data/scripts/generate-issue-4581-compaction-marker-rows.sh
#
# Backs: issue #4581.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4581}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue4581-markers"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_markerrows"
SCHEMA_FILE="$ROOT/schemas/issue-4581-compaction-marker-rows.cql"
TABLES=(byte_parity live_marker ttl_within_grace ttl_past_grace partition_del partition_del_gc clustered_markers clustered_markers_gc update_rows update_rows_gc purged_marker)

read -r -a ENGINE <<<"${CQLITE_CONTAINER_ENGINE:-docker}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "[i4581] Unknown argument: $1" >&2; exit 2 ;;
  esac
done
log()  { echo "[i4581] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4581][ERROR] $*" >&2; exit 1; }

# GUARD BLOCK (the export step removes "$OUT_DIR/$KEYSPACE*"): non-empty, canonicalized BEFORE
# validating, absolute, and under the repo root or /tmp/.
[[ -n "${OUT_DIR:-}" ]]  || fail "OUT_DIR is EMPTY. Refusing: the export step removes directories under it."
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
flush() { log "Flushing $KEYSPACE ($1)..."; run "${ENGINE[@]}" exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"; }

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
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue4581 "$CASSANDRA_IMAGE" >/dev/null \
  || fail "container could not be started; refusing to remove one this run did not create."
[[ "$DRY_RUN" -eq 1 ]] || CONTAINER_CREATED=1

if [[ "$DRY_RUN" -eq 0 ]]; then
  for i in $(seq 1 60); do
    cx cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1 && { log "Cassandra ready (attempt $i)"; break; }
    [[ "$i" -lt 60 ]] || fail "Cassandra did not become ready in time."
    sleep 5
  done
  log "image: $CASSANDRA_IMAGE id: $("${ENGINE[@]}" inspect --format '{{.Image}}' "$CONTAINER_NAME")"
  log "cassandra version: $(cx nodetool version | tr -d '\r')"
fi

if [[ "$DRY_RUN" -eq 1 ]]; then echo "[dry-run] stream $SCHEMA_FILE | cqlsh"; else
  "${ENGINE[@]}" exec -i "$CONTAINER_NAME" cqlsh < "$SCHEMA_FILE" || fail "schema apply failed"
fi
# No background compaction may merge the generations the scenario keeps apart.
run "${ENGINE[@]}" exec "$CONTAINER_NAME" nodetool disableautocompaction "$KEYSPACE"

log "=== writing rows (every statement USING TIMESTAMP) ==="
# Two write phases per table (A then B, a flush between), so the marker and what it interacts with
# live in DIFFERENT SSTables and the compaction must reconcile them.
#
# LIVE_MARKER (gc default)                                                    expected compaction
#  1 A: INSERT (id)@1000                                  -> marker-only row, kept
#  2 A: INSERT (id)@1000 | B: INSERT (id)@2000            -> marker@2000
#  3 A: INSERT (id,v)@1000 ('x')                          -> control, marker + cell
#  4 A: INSERT (id,v) (4,null)@1000                       -> marker + v cell tombstone (within grace)
#  5 A: INSERT (id)@1000 TTL 864000                       -> live TTL'd marker, ttl + LDT kept verbatim
#  6 A: INSERT (id)@1000 | B: UPDATE v='u'@2000           -> marker@1000 (NOT 2000) + cell@2000
#  7 A: INSERT (id)@1000 | B: INSERT (id)@1000            -> equal-ts identical markers
cql "INSERT INTO live_marker (id) VALUES (1) USING TIMESTAMP 1000"
cql "INSERT INTO live_marker (id) VALUES (2) USING TIMESTAMP 1000"
cql "INSERT INTO live_marker (id, v) VALUES (3, 'x') USING TIMESTAMP 1000"
cql "INSERT INTO live_marker (id, v) VALUES (4, null) USING TIMESTAMP 1000"
cql "INSERT INTO live_marker (id) VALUES (5) USING TIMESTAMP 1000 AND TTL 864000"
cql "INSERT INTO live_marker (id) VALUES (6) USING TIMESTAMP 1000"
cql "INSERT INTO live_marker (id) VALUES (7) USING TIMESTAMP 1000"
#
# BYTE_PARITY (gc default) — Data.db/Index.db/Summary.db/Digest are diffed byte for byte
#  1 A: INSERT (id)@1000                                  -> marker-only row
#  2 A: INSERT (id)@1000 | B: INSERT (id)@2000            -> marker@2000
#  3 A: INSERT (id,v)(3,'x')@1000                         -> puts `v` in the header; marker + cell
cql "INSERT INTO byte_parity (id) VALUES (1) USING TIMESTAMP 1000"
cql "INSERT INTO byte_parity (id) VALUES (2) USING TIMESTAMP 1000"
cql "INSERT INTO byte_parity (id, v) VALUES (3, 'x') USING TIMESTAMP 1000"
#
# TTL_WITHIN_GRACE (gc default 864000 s; every marker EXPIRED at compaction, still within grace)
#  1 A: INSERT (id)@1000 TTL 3                            -> expired marker kept verbatim (not a tombstone)
#  2 A: INSERT (id)@1000 TTL 3 | B: INSERT (id)@2000      -> newer live-forever marker supersedes
#  3 A: INSERT (id)@1000 TTL 3 | B: INSERT (id)@1000 TTL 5 -> equal ts, both expiring: later LDT wins
#  4 A: INSERT (id)@1000       | B: INSERT (id)@1000 TTL 3 -> equal ts: EXPIRING supersedes live-forever
#  5 A: INSERT (id)@1000 TTL 3 | B: INSERT (id)@1000      -> equal ts: EXPIRING supersedes (older file's)
#  6 A: INSERT (id,v)(6,'a')@1000 TTL 3                   -> marker + cell, both expired within grace
cql "INSERT INTO ttl_within_grace (id) VALUES (1) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO ttl_within_grace (id) VALUES (2) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO ttl_within_grace (id) VALUES (3) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO ttl_within_grace (id) VALUES (4) USING TIMESTAMP 1000"
cql "INSERT INTO ttl_within_grace (id) VALUES (5) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO ttl_within_grace (id, v) VALUES (6, 'a') USING TIMESTAMP 1000 AND TTL 3"
#
# TTL_PAST_GRACE (gc_grace_seconds = 1; the generator sleeps well past it)
#  1 A: INSERT (id)@1000 TTL 3                            -> expired + past grace: marker PURGED, partition gone
#  2 A: INSERT (id)@1000                                  -> control: live marker-only row
#  3 A: INSERT (id,v)(3,'a')@1000 TTL 3                   -> marker + cell expired past grace: row gone
#  4 A: INSERT (id,v)(4,null)@1000                        -> cell tombstone purged, marker SURVIVES (marker-only row)
cql "INSERT INTO ttl_past_grace (id) VALUES (1) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO ttl_past_grace (id) VALUES (2) USING TIMESTAMP 1000"
cql "INSERT INTO ttl_past_grace (id, v) VALUES (3, 'a') USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO ttl_past_grace (id, v) VALUES (4, null) USING TIMESTAMP 1000"
flush "A (live_marker, ttl_within_grace, ttl_past_grace)"
#
# PARTITION_DEL (gc default) — a flat table's `DELETE FROM` is a PARTITION deletion
#  1 A: INSERT (id)@2000 | B: DELETE@2000                 -> equal ts: deletion wins, partition tombstone only
#  2 A: DELETE@2000      | B: INSERT (id)@3000            -> marker NEWER than the deletion: row kept + tombstone
#  3 A: INSERT (id)@1000 | B: DELETE@2000                 -> shadowed
#  4 A: DELETE@2000      | B: INSERT (id)@2000            -> equal ts, reversed: deletion wins
#  5 A: INSERT (id)@1000                                  -> control
# PARTITION_DEL_GC (gc_grace_seconds = 1)
#  1 A: INSERT (id)@1000 | B: DELETE@2000                 -> marker shadowed, THEN deletion purged: nothing left
#  2 A: INSERT (id)@1000                                  -> control: live marker-only row
cql "INSERT INTO partition_del (id) VALUES (1) USING TIMESTAMP 2000"
cql "DELETE FROM partition_del USING TIMESTAMP 2000 WHERE id = 2"
cql "INSERT INTO partition_del (id) VALUES (3) USING TIMESTAMP 1000"
cql "DELETE FROM partition_del USING TIMESTAMP 2000 WHERE id = 4"
cql "INSERT INTO partition_del (id) VALUES (5) USING TIMESTAMP 1000"
cql "INSERT INTO partition_del_gc (id) VALUES (1) USING TIMESTAMP 1000"
cql "INSERT INTO partition_del_gc (id) VALUES (2) USING TIMESTAMP 1000"
#
# CLUSTERED_MARKERS (gc default) — `DELETE FROM ... WHERE pk AND ck` is a true ROW deletion
#  1 A: INSERT (pk,ck)(1,1)@1000 | B: DELETE row@2000     -> marker covered by the row deletion (kept, within grace)
#  2 A: DELETE row@2000          | B: INSERT (2,1)@3000   -> marker NEWER than the row deletion
#  3 A: INSERT (3,1)@1000                                 -> marker-only clustered row
#  4 A: INSERT (4,1)@1000 TTL 3                           -> expired marker, within grace
# CLUSTERED_MARKERS_GC (gc_grace_seconds = 1)
#  1 A: INSERT (1,1)@1000 | B: DELETE row@2000            -> marker shadowed, then deletion purged
#  2 A: DELETE row@2000   | B: INSERT (2,1)@3000          -> deletion purged, newer marker survives
#  3 A: INSERT (3,1)@1000 TTL 3                           -> expired marker past grace: row gone
#  4 A: INSERT (4,1)@1000                                 -> control
cql "INSERT INTO clustered_markers (pk, ck) VALUES (1, 1) USING TIMESTAMP 1000"
cql "DELETE FROM clustered_markers USING TIMESTAMP 2000 WHERE pk = 2 AND ck = 1"
cql "INSERT INTO clustered_markers (pk, ck) VALUES (3, 1) USING TIMESTAMP 1000"
cql "INSERT INTO clustered_markers (pk, ck) VALUES (4, 1) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO clustered_markers_gc (pk, ck) VALUES (1, 1) USING TIMESTAMP 1000"
cql "DELETE FROM clustered_markers_gc USING TIMESTAMP 2000 WHERE pk = 2 AND ck = 1"
cql "INSERT INTO clustered_markers_gc (pk, ck) VALUES (3, 1) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO clustered_markers_gc (pk, ck) VALUES (4, 1) USING TIMESTAMP 1000"
#
# UPDATE_ROWS (gc default) — UPDATE-created rows have NO marker: Cassandra writes no liveness_info
#  1 A: UPDATE v='a'@1000 | B: UPDATE v='a2'@2000         -> no marker, cell@2000
#  2 A: UPDATE v='b'@1000 TTL 864000                      -> no marker, live TTL'd CELL (cell ttl/LDT kept)
#  3 A: UPDATE v='c'@1000 TTL 3                           -> no marker, cell EXPIRED within grace
#  4 A: INSERT (4,'d')@1000 | B: UPDATE v='e'@2000        -> control: marker@1000 kept + cell@2000
# UPDATE_ROWS_GC (gc_grace_seconds = 1)
#  1 A: UPDATE v='x'@1000 TTL 3                           -> expired past grace: row GONE (no ghost marker)
#  2 A: UPDATE v='a'@1000                                 -> control: no marker, live cell
#  3 A: UPDATE v='x'@1000 TTL 3 | B: UPDATE v='y'@2000    -> no marker, cell@2000 only
# PURGED_MARKER (gc_grace_seconds = 1) — also the public-surface read guard's INPUTS
#  1 A: INSERT (1,'m')@1000 TTL 3 | B: UPDATE v='n'@2000 -> expired marker + live data (read: row visible)
#                                                           compacted: marker PURGED, data kept, no marker
#  2 A: INSERT (id)@1000 TTL 3    | B: UPDATE v='n'@2000 -> same, marker-only expiring + data
#  3 A: INSERT (id)@1000 TTL 3                            -> expired marker-only row (read: hidden; compacted: gone)
#  4 A: INSERT (id)@1000                                  -> live marker-only row (kept)
cql "UPDATE update_rows USING TIMESTAMP 1000 SET v = 'a' WHERE id = 1"
cql "UPDATE update_rows USING TIMESTAMP 1000 AND TTL 864000 SET v = 'b' WHERE id = 2"
cql "UPDATE update_rows USING TIMESTAMP 1000 AND TTL 3 SET v = 'c' WHERE id = 3"
cql "INSERT INTO update_rows (id, v) VALUES (4, 'd') USING TIMESTAMP 1000"
cql "UPDATE update_rows_gc USING TIMESTAMP 1000 AND TTL 3 SET v = 'x' WHERE id = 1"
cql "UPDATE update_rows_gc USING TIMESTAMP 1000 SET v = 'a' WHERE id = 2"
cql "UPDATE update_rows_gc USING TIMESTAMP 1000 AND TTL 3 SET v = 'x' WHERE id = 3"
cql "INSERT INTO purged_marker (id, v) VALUES (1, 'm') USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO purged_marker (id) VALUES (2) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO purged_marker (id) VALUES (3) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO purged_marker (id) VALUES (4) USING TIMESTAMP 1000"
flush "A (partition_del*, clustered_markers*, update_rows*, purged_marker)"
# phase B
cql "INSERT INTO byte_parity (id) VALUES (2) USING TIMESTAMP 2000"
cql "INSERT INTO live_marker (id) VALUES (2) USING TIMESTAMP 2000"
cql "UPDATE live_marker USING TIMESTAMP 2000 SET v = 'u' WHERE id = 6"
cql "INSERT INTO live_marker (id) VALUES (7) USING TIMESTAMP 1000"
cql "INSERT INTO ttl_within_grace (id) VALUES (2) USING TIMESTAMP 2000"
cql "INSERT INTO ttl_within_grace (id) VALUES (3) USING TIMESTAMP 1000 AND TTL 5"
cql "INSERT INTO ttl_within_grace (id) VALUES (4) USING TIMESTAMP 1000 AND TTL 3"
cql "INSERT INTO ttl_within_grace (id) VALUES (5) USING TIMESTAMP 1000"
cql "DELETE FROM partition_del USING TIMESTAMP 2000 WHERE id = 1"
cql "INSERT INTO partition_del (id) VALUES (2) USING TIMESTAMP 3000"
cql "DELETE FROM partition_del USING TIMESTAMP 2000 WHERE id = 3"
cql "INSERT INTO partition_del (id) VALUES (4) USING TIMESTAMP 2000"
cql "DELETE FROM partition_del_gc USING TIMESTAMP 2000 WHERE id = 1"
cql "DELETE FROM clustered_markers USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 1"
cql "INSERT INTO clustered_markers (pk, ck) VALUES (2, 1) USING TIMESTAMP 3000"
cql "DELETE FROM clustered_markers_gc USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 1"
cql "INSERT INTO clustered_markers_gc (pk, ck) VALUES (2, 1) USING TIMESTAMP 3000"
cql "UPDATE update_rows USING TIMESTAMP 2000 SET v = 'a2' WHERE id = 1"
cql "UPDATE update_rows USING TIMESTAMP 2000 SET v = 'e' WHERE id = 4"
cql "UPDATE update_rows_gc USING TIMESTAMP 2000 SET v = 'y' WHERE id = 3"
cql "UPDATE purged_marker USING TIMESTAMP 2000 SET v = 'n' WHERE id = 1"
cql "UPDATE purged_marker USING TIMESTAMP 2000 SET v = 'n' WHERE id = 2"
flush "B"

[[ "$DRY_RUN" -eq 0 ]] || { log "dry-run complete"; exit 0; }

# Every TTL (<= 5 s) has expired and every 1 s gc_grace has elapsed before ANY compaction runs.
log "sleeping 12 s so every TTL is expired and every 1 s gc_grace has elapsed"
sleep 12

export_data() { # <dest-root>: tar-stream /var/lib/cassandra/data/$KEYSPACE out of the container
  local dest="$1" tmpx="$OUT_DIR/.i4581_export_tmp"
  [[ -n "$dest" && "$dest" == "$OUT_DIR/"* ]] || fail "refusing export destination '$dest'"
  rm -rf "${tmpx:?}"; mkdir -p "$tmpx"
  cx bash -lc 'tar -C /var/lib/cassandra -cf - data' | tar -C "$tmpx" -xf - || fail "tar export failed."
  [[ -d "$tmpx/data/$KEYSPACE" ]] || fail "Expected $tmpx/data/$KEYSPACE; export failed."
  rm -rf "${dest:?}"; mkdir -p "$dest"
  cp -r "$tmpx/data/$KEYSPACE/." "$dest/"; rm -rf "${tmpx:?}"
  # Drop snapshots/backups a flush or compaction may have left; only live SSTables are fixtures.
  find "$dest" -type d \( -name snapshots -o -name backups \) -prune -exec rm -rf {} + 2>/dev/null || true
}

dump_all() { # <dest-root> <container-relative base>: sstabledump -l every Data.db
  local dest="$1"
  while IFS= read -r -d '' data_file; do
    rel="${data_file#"$OUT_DIR"/}"
    inpath="/var/lib/cassandra/data/$KEYSPACE/${data_file#"$dest"/}"
    log "  sstabledump: $rel"
    cx bash -lc "/opt/cassandra/tools/bin/sstabledump '$inpath' -l" | python3 -c "
import json, sys
for line in sys.stdin:
    line = line.strip()
    if line:
        print(json.dumps(json.loads(line), separators=(',', ':')))
" > "${data_file%.db}.db.jsonl"
    [[ -s "${data_file%.db}.db.jsonl" ]] || log "  (empty dump: $rel — an all-purged output is legitimate)"
    cx bash -lc "/opt/cassandra/tools/bin/sstablemetadata '$inpath'" > "${data_file%Data.db}Statistics.db.txt" 2>/dev/null || true
  done < <(find "$dest" -name '*-Data.db' -print0)
}

log "=== Exporting the INPUT SSTables ==="
export_data "$OUT_DIR/$KEYSPACE"
for t in "${TABLES[@]}"; do
  cnt=$(find "$OUT_DIR/$KEYSPACE/$t"* -name '*-Data.db' | wc -l | tr -d ' ')
  [[ "$cnt" -ge 1 ]] || fail "$t: no Data.db exported"
  log "$t: $cnt input SSTable(s)"
  d=$(dirname "$(find "$OUT_DIR/$KEYSPACE/$t"* -name '*-Data.db' | head -1)")
  cx cqlsh -k "$KEYSPACE" -e "SELECT * FROM $t;" > "$d/cqlsh-select.txt" || fail "cqlsh SELECT failed for $t"
done
# dump_all maps a host path back to the container path by stripping the export root.
dump_all "$OUT_DIR/$KEYSPACE"

log "=== Cassandra's own major compaction ==="
INSTANTS_TMP="$(mktemp)"
for t in "${TABLES[@]}"; do
  now="$(cx date +%s | tr -d '\r')"
  gc="$(cx cqlsh -k "$KEYSPACE" -e "SELECT gc_grace_seconds FROM system_schema.tables WHERE keyspace_name='$KEYSPACE' AND table_name='$t';" | tr -d '\r' | awk 'NR==4{gsub(/ /,"");print}')"
  [[ "$gc" =~ ^[0-9]+$ ]] || fail "could not read gc_grace_seconds of $t (got '$gc')"
  log "nodetool compact $KEYSPACE $t (now=$now gc_grace_seconds=$gc)"
  cx nodetool compact "$KEYSPACE" "$t" || fail "nodetool compact failed for $t"
  printf '%s %s %s\n' "$t" "$now" "$gc" >> "$INSTANTS_TMP"
done
sleep 3   # let the compaction's obsolete-file deletion settle before the export

log "=== Exporting Cassandra's COMPACTED output ==="
export_data "$OUT_DIR/${KEYSPACE}_compacted"
dump_all "$OUT_DIR/${KEYSPACE}_compacted"
while read -r t now gc; do
  d=$(dirname "$(find "$OUT_DIR/${KEYSPACE}_compacted/$t"* -name '*-Data.db' | head -1 2>/dev/null)" 2>/dev/null || true)
  if [[ -z "$d" || "$d" == "." ]]; then d=$(ls -d "$OUT_DIR/${KEYSPACE}_compacted/$t"-* | head -1); fi
  printf '{"table":"%s","compaction_now_secs":%s,"gc_grace_seconds":%s}\n' "$t" "$now" "$gc" > "$d/compaction-instant.json"
done < "$INSTANTS_TMP"
rm -f "$INSTANTS_TMP"

echo "  git -C '$REPO_ROOT' add -f '$OUT_DIR'/$KEYSPACE/*/*.db '$OUT_DIR'/${KEYSPACE}_compacted/*/*.db"
echo "  git -C '$REPO_ROOT' add '$OUT_DIR'/$KEYSPACE/*/*.jsonl '$OUT_DIR'/${KEYSPACE}_compacted/*/*.jsonl '$OUT_DIR'/*/*/*TOC.txt '$OUT_DIR'/*/*/*.crc32 '$OUT_DIR'/*/*/*.db.txt '$OUT_DIR'/*/*/cqlsh-select*.txt '$OUT_DIR'/*/*/compaction-instant.json"

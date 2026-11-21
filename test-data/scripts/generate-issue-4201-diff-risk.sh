#!/usr/bin/env bash
# generate-issue-4201-diff-risk.sh — Cassandra 5.0.8-written two-generation fixture for
# `cqlite diff` (issue #4201). Schema: test-data/schemas/issue-4201-diff-risk.cql.
#
# Two flushes with autocompaction OFF, so both generations stay on disk:
#   generation 1 = the data side (what a replica that missed the deletes still holds)
#   generation 2 = the tombstone/newer side
# Tests stage generation 2 as side A and generation 1 as side B. Every mutation pins its client
# timestamp; the case table is in README.md next to the output.
#
# CONTAINER ENGINE: `docker` by default; the owner-authorized VM is driven with
#   CQLITE_CONTAINER_ENGINE='incus exec wintermute:cassandra --project wm-sandboxes -- docker'
# Output is checkout-relative (test-data/fixtures/issue_4201/); `*.db` must be `git add -f`ed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4201}"
CONTAINER_NAME="cqlite-issue4201-diff"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_diff"; TABLE="risk_cases"
SCHEMA_FILE="$ROOT/schemas/issue-4201-diff-risk.cql"
read -r -a ENGINE <<<"${CQLITE_CONTAINER_ENGINE:-docker}"
while [[ $# -gt 0 ]]; do case "$1" in --out) OUT_DIR="$2"; shift 2 ;; *) echo "[i4201] Unknown argument: $1" >&2; exit 2 ;; esac; done
log()  { echo "[i4201] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4201][ERROR] $*" >&2; exit 1; }

[[ "$OUT_DIR" == /* ]] || OUT_DIR="$PWD/$OUT_DIR"
OUT_DIR="$(realpath -m "$OUT_DIR")"; OUT_DIR="${OUT_DIR%/}"
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] || fail "OUT_DIR '$OUT_DIR' is not under the repo"

cx()  { "${ENGINE[@]}" exec "$CONTAINER_NAME" "$@" </dev/null; }
"${ENGINE[@]}" inspect "$CONTAINER_NAME" >/dev/null 2>&1 && fail "Container '$CONTAINER_NAME' exists; remove it first"
CREATED=0
cleanup() { if [[ "$CREATED" -eq 1 ]]; then "${ENGINE[@]}" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true; fi; }
trap cleanup EXIT

log "Starting $IMAGE ($CONTAINER_NAME)"
"${ENGINE[@]}" run -d --name "$CONTAINER_NAME" -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256m "$IMAGE" >/dev/null
CREATED=1
for i in $(seq 1 90); do cx cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1 && break; [[ "$i" -lt 90 ]] || fail "not ready"; sleep 5; done
VERSION="$(cx nodetool version | tr -d '\r')"; log "cassandra: $VERSION"
"${ENGINE[@]}" exec -i "$CONTAINER_NAME" cqlsh < "$SCHEMA_FILE" || fail "schema apply failed"
cx nodetool disableautocompaction "$KEYSPACE" "$TABLE"

# Schema/mutations are streamed over stdin (the engine may be remote: incus exec into a VM).
run_cql() { "${ENGINE[@]}" exec -i "$CONTAINER_NAME" cqlsh -k "$KEYSPACE" <<<"$1"; }
G1="INSERT INTO risk_cases (id,ck,v) VALUES (1,1,'newer-than-row-tombstone') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (3,1,'tie-with-row-tombstone') USING TIMESTAMP 2000;
INSERT INTO risk_cases (id,ck,v) VALUES (4,1,'b-newer') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (5,1,'newer-than-cell-tombstone') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (6,1,'b-older') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (7,1,'only-in-b') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (9,1,'apple') USING TIMESTAMP 5000;
INSERT INTO risk_cases (id,ck,v) VALUES (10,1,'zebra') USING TIMESTAMP 5000;
INSERT INTO risk_cases (id,ck,v) VALUES (11,1,'live-tie') USING TIMESTAMP 5000;
INSERT INTO risk_cases (id,ck,v) VALUES (12,1,'live-loses') USING TIMESTAMP 5000;
UPDATE risk_cases USING TIMESTAMP 5000 SET m['k1']=7 WHERE id=13 AND ck=1;
UPDATE risk_cases USING TIMESTAMP 5000 SET s = s + {'x'} WHERE id=14 AND ck=1;
UPDATE risk_cases USING TIMESTAMP 5000 SET fl = ['a, b'] WHERE id=15 AND ck=1;
UPDATE risk_cases USING TIMESTAMP 5000 SET m['k1']=1, m['k3']=3 WHERE id=16 AND ck=1;
INSERT INTO risk_cases (id,ck) VALUES (17,1) USING TIMESTAMP 5000;"
G2="DELETE FROM risk_cases USING TIMESTAMP 2000 WHERE id=1 AND ck=1;
DELETE FROM risk_cases USING TIMESTAMP 2000 WHERE id=2;
DELETE FROM risk_cases USING TIMESTAMP 2000 WHERE id=3 AND ck=1;
INSERT INTO risk_cases (id,ck,v) VALUES (4,1,'a-older') USING TIMESTAMP 2000;
DELETE v FROM risk_cases USING TIMESTAMP 2000 WHERE id=5 AND ck=1;
INSERT INTO risk_cases (id,ck,v) VALUES (6,1,'a-newer') USING TIMESTAMP 4000;
INSERT INTO risk_cases (id,ck,v) VALUES (8,1,'only-in-a') USING TIMESTAMP 2000;
INSERT INTO risk_cases (id,ck,v) VALUES (9,1,'zebra') USING TIMESTAMP 5000;
INSERT INTO risk_cases (id,ck,v) VALUES (10,1,'apple') USING TIMESTAMP 5000;
DELETE v FROM risk_cases USING TIMESTAMP 5000 WHERE id=11 AND ck=1;
INSERT INTO risk_cases (id,ck,v) VALUES (12,1,'expiring-wins') USING TIMESTAMP 5000 AND TTL 100000000;
UPDATE risk_cases USING TIMESTAMP 5000 SET m['k2']=7 WHERE id=13 AND ck=1;
UPDATE risk_cases USING TIMESTAMP 5000 SET s = s + {'y'} WHERE id=14 AND ck=1;
UPDATE risk_cases USING TIMESTAMP 5000 SET fl = ['a','b'] WHERE id=15 AND ck=1;
DELETE m['k1'] FROM risk_cases USING TIMESTAMP 6000 WHERE id=16 AND ck=1;
UPDATE risk_cases USING TIMESTAMP 5000 SET m['k3']=3 WHERE id=16 AND ck=1;
DELETE FROM risk_cases USING TIMESTAMP 6000 WHERE id=17 AND ck=1;"
log "generation 1"; run_cql "$G1"; cx nodetool flush "$KEYSPACE" "$TABLE"
sleep 3
log "generation 2"; run_cql "$G2"; cx nodetool flush "$KEYSPACE" "$TABLE"

TDIR="$(cx bash -lc "ls -d /var/lib/cassandra/data/$KEYSPACE/$TABLE-* | head -1" | tr -d '\r')"
[[ "$(cx bash -lc "ls $TDIR/nb-*-big-Data.db | wc -l" | tr -d '\r')" -eq 2 ]] || fail "expected exactly two Data.db generations"
DEST="$OUT_DIR/$KEYSPACE/$(basename "$TDIR")"
mkdir -p "$DEST"
cx bash -lc "tar -C $TDIR -cf - ." | tar -C "$DEST" -xf - || fail "export failed"
for data in "$DEST"/nb-*-big-Data.db; do
  base="$(basename "$data")"
  cx bash -lc "/opt/cassandra/tools/bin/sstabledump '$TDIR/$base'" 2>/dev/null | python3 -c "
import json, sys
for p in json.load(sys.stdin): print(json.dumps(p, separators=(',', ':')))
" > "$data.jsonl"
  [[ -s "$data.jsonl" ]] || fail "empty sstabledump golden for $base"
  cx bash -lc "/opt/cassandra/tools/bin/sstablemetadata '$TDIR/$base'" 2>/dev/null > "${data%Data.db}Statistics.db.txt"
done
printf '%s\n' "$G1" > "$DEST/generation-1.cql"; printf '%s\n' "$G2" > "$DEST/generation-2.cql"
printf '%s\n' "$VERSION" > "$DEST/cassandra-version.txt"
"${ENGINE[@]}" image inspect "$IMAGE" --format '{{json .RepoDigests}}' > "$DEST/image-digests.json"
find "$OUT_DIR" -name '._*' -delete 2>/dev/null || true
log "DONE: $DEST"
echo "  git add -f '$DEST'/*.db ; git add '$OUT_DIR'"

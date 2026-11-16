#!/usr/bin/env bash
# generate-issue-4530-equal-ts-ties.sh — Cassandra 5.0.8-written EQUAL-TIMESTAMP reconcile-tie
# fixture (issue #4530). Schema + the case table: test-data/schemas/issue-4530-equal-ts-ties.cql.
#
# Flow (inside ONE temp container; autocompaction OFF so the two generations stay separate):
#   1. generation 1: the gen-1 half of every case, all `USING TIMESTAMP 1000`; flush
#   2. sleep 3s (so tombstone/row/partition localDeletionTimes differ), generation 2; flush
#   3. export both SSTables (nb-1, nb-2) -> <out>/ordered/        (bytes exactly as written)
#   4. capture Cassandra's own `SELECT` of the two-generation read -> cassandra-select.txt
#   5. `nodetool compact` (Cassandra's reconcile of the two) -> sstabledump golden of the
#      compacted SSTable -> compacted.jsonl, plus expected.json (the surviving localDeletionTimes
#      extracted by python from that golden — never hand-written)
#   6. <out>/swapped/ = the SAME two files with their generation numbers exchanged (a rename of
#      file names only; bytes identical), so the "newer file" is the other half of each case.
#      CQLite's old first-seen tie-break depends on exactly that, Cassandra's does not.
#
# CONTAINER ENGINE: `docker` by default; the owner-authorized VM is driven with
#   CQLITE_CONTAINER_ENGINE='incus exec wintermute:cassandra --project wm-sandboxes -- docker'
# Only the container named below is touched. Output is checkout-relative
# (test-data/fixtures/issue_4530/); `*.db` must be `git add -f`ed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4530}"
CONTAINER_NAME="cqlite-issue4530-ties"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_ties"; TABLE="ties"
SCHEMA_FILE="$ROOT/schemas/issue-4530-equal-ts-ties.cql"
read -r -a ENGINE <<<"${CQLITE_CONTAINER_ENGINE:-docker}"
while [[ $# -gt 0 ]]; do case "$1" in --out) OUT_DIR="$2"; shift 2 ;; *) echo "[i4530] Unknown argument: $1" >&2; exit 2 ;; esac; done
log()  { echo "[i4530] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4530][ERROR] $*" >&2; exit 1; }

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

T="USING TIMESTAMP 1000"
log "generation 1"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 1, 'apple') $T"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 2, 'zebra') $T"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 3, 'live') $T"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 4, 'ttl') USING TIMESTAMP 1000 AND TTL 200000000"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 5, 'big') USING TIMESTAMP 1000 AND TTL 300000000"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 6, 'small') USING TIMESTAMP 1000 AND TTL 100000000"
cql "DELETE v FROM ties $T WHERE pk = 1 AND ck = 7"
cql "DELETE FROM ties $T WHERE pk = 2 AND ck = 1"
cql "DELETE FROM ties $T WHERE pk = 3"
cx nodetool flush "$KEYSPACE" "$TABLE"
sleep 3
log "generation 2"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 1, 'zebra') $T"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 2, 'apple') $T"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 3, 'ttl') USING TIMESTAMP 1000 AND TTL 200000000"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 4, 'live') $T"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 5, 'small') USING TIMESTAMP 1000 AND TTL 100000000"
cql "INSERT INTO ties (pk, ck, v) VALUES (1, 6, 'big') USING TIMESTAMP 1000 AND TTL 300000000"
cql "DELETE v FROM ties $T WHERE pk = 1 AND ck = 7"
cql "DELETE FROM ties $T WHERE pk = 2 AND ck = 1"
cql "DELETE FROM ties $T WHERE pk = 3"
cx nodetool flush "$KEYSPACE" "$TABLE"

INDIR="/var/lib/cassandra/data/$KEYSPACE"
TDIR="$(cx bash -lc "ls -d $INDIR/$TABLE-* | head -1" | tr -d '\r')"
[[ "$(cx bash -lc "ls $TDIR/nb-*-big-Data.db | wc -l" | tr -d '\r')" -eq 2 ]] || fail "expected exactly two Data.db generations before compaction"

mkdir -p "$OUT_DIR"; rm -rf "${OUT_DIR:?}/ordered" "${OUT_DIR:?}/swapped"
mkdir -p "$OUT_DIR/ordered/$KEYSPACE/$(basename "$TDIR")"
DEST="$OUT_DIR/ordered/$KEYSPACE/$(basename "$TDIR")"
cx bash -lc "tar -C $TDIR -cf - ." | tar -C "$DEST" -xf - || fail "export failed"
log "Cassandra's own SELECT over the two uncompacted generations"
cx cqlsh --no-color -k "$KEYSPACE" -e "SELECT pk, ck, v, writetime(v) FROM ties" > "$OUT_DIR/cassandra-select.txt"
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
# The surviving localDeletionTimes, extracted from Cassandra's compacted golden by code.
python3 - "$OUT_DIR/compacted.jsonl" "$OUT_DIR/expected.json" <<'PY'
import json, sys, datetime
def secs(s): return int(datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp())
out = {}
for line in open(sys.argv[1]):
    p = json.loads(line); pk = int(p["partition"]["key"][0])
    d = p["partition"].get("deletion_info")
    if d and "local_delete_time" in d:
        out["partition_deletion_ldt"] = secs(d["local_delete_time"])
    for r in p.get("rows", []):
        if r["type"] == "row" and pk == 1 and r["clustering"][0] == 7:
            for c in r["cells"]:
                if c["name"] == "v" and "deletion_info" in c:
                    out["cell_tombstone_ldt"] = secs(c["deletion_info"]["local_delete_time"])
        if r["type"] == "row" and pk == 2:
            di = r.get("deletion_info")
            if di and "local_delete_time" in di:
                out["row_deletion_ldt"] = secs(di["local_delete_time"])
assert set(out) == {"partition_deletion_ldt", "cell_tombstone_ldt", "row_deletion_ldt"}, out
json.dump(out, open(sys.argv[2], "w"), indent=1)
print("expected:", out)
PY

# swapped = the same bytes with the two generation numbers exchanged
cp -r "$OUT_DIR/ordered" "$OUT_DIR/swapped"
SW="$OUT_DIR/swapped/$KEYSPACE/$(basename "$TDIR")"
for f in "$SW"/nb-1-big-*; do mv "$f" "${f/nb-1-big-/nb-tmp-big-}"; done
for f in "$SW"/nb-2-big-*; do mv "$f" "${f/nb-2-big-/nb-1-big-}"; done
for f in "$SW"/nb-tmp-big-*; do mv "$f" "${f/nb-tmp-big-/nb-2-big-}"; done
find "$OUT_DIR" -name '._*' -delete 2>/dev/null || true
log "DONE: $OUT_DIR"
echo "  git add -f '$OUT_DIR'/*/$KEYSPACE/*/*.db ; git add '$OUT_DIR'"

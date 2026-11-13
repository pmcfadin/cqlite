#!/usr/bin/env bash
# generate-issue-4360-varint-column.sh — Cassandra 5.0.8-written top-level `varint`
# fixture (issue #4360). Schema: test-data/schemas/issue-4360-varint-column.cql (READ IT).
#
# THE ORACLE IS CASSANDRA-WRITTEN BYTES + `sstabledump`, never a CQLite round-trip
# (CLAUDE.md, #3042). Every INSERT pins `USING TIMESTAMP`, then the keyspace is
# flushed once (`nodetool flush`), so each table is exactly one SSTable.
#
# OUTPUT: test-data/fixtures/issue_4360/<keyspace>/<table>-<uuid>/ — checkout-relative
# on purpose (see generate-issue-4114-vector-float.sh): a CQLITE_DATASETS_ROOT export
# must not be able to hide it. The *.db files are gitignored: `git add -f` them (the
# commands are printed at exit). Regeneration mints new table UUIDs and replaces the
# directories; use --out /tmp/... for a trial run.
#
# CONTAINER ENGINE: `docker` by default. The owner-authorized Cassandra VM is driven with
#   CQLITE_CONTAINER_ENGINE='incus exec wintermute:cassandra --project wm-sandboxes -- docker' \
#     bash test-data/scripts/generate-issue-4360-varint-column.sh
# Everything (cqlsh, nodetool, sstabledump, sstablemetadata) runs INSIDE the one container
# named below via `exec`; the SSTables are tar-streamed out, so no host path needs to be
# visible to the engine. Only that exact container name is ever touched (no image-wide
# filter), the pre-flight fails closed if it exists, and cleanup removes only a container
# this invocation created.
#
# Backs: issue #4360.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4360}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue4360-varint"
. "$SCRIPT_DIR/lib/cassandra-image.sh"
CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
KEYSPACE="test_varint"
SCHEMA_FILE="$ROOT/schemas/issue-4360-varint-column.cql"
T_FIXED=1000

# The subject values, in insertion order. `id`/`pk` is the 1-based index.
VALUES=(
  0 1 -1 127 128 255 256 -128 -129
  9223372036854775807
  18446744073709551616
  18446744073709551617
  -18446744073709551616
  1234567890123456789012345678901234567890
  -1234567890123456789012345678901234567890
)
NULL_ID=100   # varint_regular row whose `v` is an explicit CQL null (a cell tombstone)

read -r -a ENGINE <<<"${CQLITE_CONTAINER_ENGINE:-docker}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "[i4360] Unknown argument: $1" >&2; exit 2 ;;
  esac
done
log()  { echo "[i4360] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4360][ERROR] $*" >&2; exit 1; }

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

log "=== inserting rows (USING TIMESTAMP $T_FIXED) ==="
id=0
for v in "${VALUES[@]}"; do
  id=$((id + 1))
  cql "INSERT INTO varint_regular (id, v) VALUES ($id, $v) USING TIMESTAMP $T_FIXED"
  cql "INSERT INTO varint_clustered (pk, ck, tag) VALUES (1, $v, 'v$id') USING TIMESTAMP $T_FIXED"
done
# NULL: an explicit `null` writes a cell tombstone. Its `local_delete_time` is a wall clock no CQL
# clause can pin, so never byte-compare this golden across regenerations and never assert on it.
# (A row with NO regular cell at all - only primary-key liveness - was tried first and is NOT used:
# CQLite does not return such a row from SELECT, a separate defect, see #2189.)
cql "INSERT INTO varint_regular (id, v) VALUES ($NULL_ID, null) USING TIMESTAMP $T_FIXED"

log "Flushing $KEYSPACE..."
run "${ENGINE[@]}" exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"

[[ "$DRY_RUN" -eq 0 ]] || { log "dry-run complete"; exit 0; }

log "=== Exporting SSTables ==="
mkdir -p "$OUT_DIR"
TMPX="$OUT_DIR/.i4360_export_tmp"; rm -rf "$TMPX"; mkdir -p "$TMPX"
cx bash -lc 'tar -C /var/lib/cassandra -cf - data' | tar -C "$TMPX" -xf - || fail "tar export failed."
[[ -d "$TMPX/data/$KEYSPACE" ]] || fail "Expected $TMPX/data/$KEYSPACE; export failed."
rm -rf "${OUT_DIR:?}/$KEYSPACE"; mkdir -p "$OUT_DIR/$KEYSPACE"
cp -r "$TMPX/data/$KEYSPACE/." "$OUT_DIR/$KEYSPACE/"; rm -rf "$TMPX"

for t in varint_regular varint_clustered; do
  cnt=$(find "$OUT_DIR/$KEYSPACE/$t"* -name '*-Data.db' | wc -l | tr -d ' ')
  [[ "$cnt" -eq 1 ]] || fail "$t: expected exactly ONE Data.db, found $cnt."
done

# sstabledump + sstablemetadata run INSIDE the still-running container on the flushed files,
# addressed by their in-container path (same relative layout as the export).
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

# THE FIXTURE MUST CARRY ITS OWN SUBJECT: every expected value must appear in the golden.
python3 - "$OUT_DIR/$KEYSPACE" "$NULL_ID" "${VALUES[@]}" <<'PY' || fail "golden verification FAILED"
import glob, json, sys
root, null_id, *vals = sys.argv[1:]
want = [int(v) for v in vals]
def rows(table):
    f = glob.glob(f"{root}/{table}-*/*-Data.db.jsonl")
    assert len(f) == 1, (table, f)
    for line in open(f[0]):
        yield json.loads(line)
reg = {}
for p in rows("varint_regular"):
    k = int(p["partition"]["key"][0])
    cells = [c for r in p["rows"] for c in r.get("cells", [])]
    reg[k] = [c.get("value") for c in cells if c["name"] == "v"]
for i, w in enumerate(want, 1):
    got = reg.get(i)
    assert got is not None and len(got) == 1 and int(got[0]) == w, (i, w, got)
nullcells = [c for p in rows("varint_regular") if int(p["partition"]["key"][0]) == int(null_id)
             for r in p["rows"] for c in r.get("cells", []) if c["name"] == "v"]
assert len(nullcells) == 1 and "value" not in nullcells[0] and "deletion_info" in nullcells[0], nullcells
ck = []
for p in rows("varint_clustered"):
    for r in p["rows"]:
        ck.append(int(r["clustering"][0]))
assert sorted(ck) == sorted(want) and ck == sorted(want), ("clustering order must be numeric", ck)
assert max(want) > 2**64 and min(want) < -(2**64)
print("[i4360]   golden carries every value, an explicit-NULL row and numeric clustering order (OK)")
PY

echo "  git -C '$REPO_ROOT' add -f '$OUT_DIR'/$KEYSPACE/*/*.db"
echo "  git -C '$REPO_ROOT' add '$OUT_DIR'/$KEYSPACE/*/*.jsonl '$OUT_DIR'/$KEYSPACE/*/*TOC.txt '$OUT_DIR'/$KEYSPACE/*/*.crc32 '$OUT_DIR'/$KEYSPACE/*/*.db.txt"

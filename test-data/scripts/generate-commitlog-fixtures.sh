#!/usr/bin/env bash
# generate-commitlog-fixtures.sh — Cassandra 5.0 CommitLog segment fixtures (#2389)
#
# Spins up a real Cassandra 5.0 node, applies a known schema, inserts a KNOWN,
# versioned set of mutations, waits for the periodic commitlog sync to flush the
# segment to disk, then captures the active CommitLog-*.log segment BEFORE any
# flush discards it. The captured segment is the ground truth for the CommitLog
# reader parity oracle (#2389): "mutations we inserted" vs. "mutations the reader
# decoded" — the CommitLog analog of the query-semantics oracle, since there is
# no sstabledump-equivalent for commitlog segments.
#
# Doctrine (issue #2389 / CQLite no-heuristics): the fixture is REAL Cassandra
# output, never hand-crafted bytes. The parser is proven against these bytes,
# not against our belief about the format.
#
# Outputs (under <out>/commitlog/):
#   commitlog-ground-truth.json          committed — schema + insert set + ids
#   clean-<segid>.log                    committed (git add -f) — trimmed real segment
#   truncated-<segid>.log                committed — clean segment torn mid-record
#   corrupt-crc-<segid>.log              committed — clean segment with a flipped payload byte
#   raw-<segid>.log                      gitignored (*.log) — untrimmed 32MB original
#
# Usage:
#   bash test-data/scripts/generate-commitlog-fixtures.sh [--out <dir>] [--dry-run]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-commitlog"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="commitlog_test"
TABLE="users"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;    shift   ;;
    *) echo "[cl] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[cl] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[cl][ERROR] $*" >&2; exit 1; }
run()  { if [[ "$DRY_RUN" -eq 1 ]]; then echo "[dry-run] $*"; else "$@"; fi; }

if command -v docker >/dev/null 2>&1; then
  ENGINE="docker"
elif command -v podman >/dev/null 2>&1; then
  ENGINE="podman"
else
  [[ "$DRY_RUN" -eq 1 ]] && ENGINE="docker" || fail "Neither docker nor podman found."
fi
log "Using container engine: $ENGINE"

if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it: $ENGINE rm -f $CONTAINER_NAME"
fi

cleanup() {
  if [[ "$DRY_RUN" -eq 0 ]]; then
    log "Cleaning up container..."
    $ENGINE rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_cassandra() {
  local max_retries=60 delay=5
  log "Waiting for Cassandra to become ready (max ${max_retries}x${delay}s)..."
  for i in $(seq 1 "$max_retries"); do
    if $ENGINE exec "$CONTAINER_NAME" cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra is ready (attempt $i)."
      return 0
    fi
    sleep "$delay"
  done
  fail "Cassandra did not become ready in time."
}

# ---- OUT_DIR safety (mirrors generate-tombstone-parity.sh) -----------------
if [[ "${#OUT_DIR}" -lt 4 ]]; then fail "OUT_DIR '$OUT_DIR' suspiciously short."; fi
case "$OUT_DIR" in
  /)    fail "Refusing to operate on '/'." ;;
  /tmp) fail "Refusing to use '/tmp' directly." ;;
esac
_under_repo=0; _under_tmp=0
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] && _under_repo=1
[[ "$OUT_DIR" == /tmp/*          ]] && _under_tmp=1
if [[ "$_under_repo" -eq 0 && "$_under_tmp" -eq 0 ]]; then
  fail "OUT_DIR '$OUT_DIR' not under repo root or /tmp/."
fi

CL_DIR="$OUT_DIR/commitlog"
log "Output directory: $CL_DIR"

# Commitlog sync period: shrink so we do not wait the default 10s. batch mode
# fsyncs each group commit, guaranteeing the segment is on disk before capture.
log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-commitlog \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 1 ]]; then
  log "(dry-run) would apply schema, insert the known mutation set, sync, and capture the segment."
  exit 0
fi

wait_cassandra

log "Installing python driver in container..."
$ENGINE exec "$CONTAINER_NAME" bash -c "apt-get update -qq && apt-get install -y -q python3-pip" >/dev/null
$ENGINE exec "$CONTAINER_NAME" pip3 install --quiet cassandra-driver

# ---- Schema + KNOWN insert set (ground truth) -----------------------------
log "Applying schema + inserting the known mutation set..."
$ENGINE exec -i "$CONTAINER_NAME" env KEYSPACE="$KEYSPACE" TABLE="$TABLE" python3 - <<'PYEOF'
import os, sys, time, json, traceback
from cassandra.cluster import Cluster

KEYSPACE = os.environ["KEYSPACE"]; TABLE = os.environ["TABLE"]

def connect(attempts=12, delay=6):
    last = None
    for a in range(1, attempts + 1):
        try:
            c = Cluster(['127.0.0.1']); s = c.connect(); return c, s
        except Exception as e:
            last = e; time.sleep(delay)
    raise RuntimeError(f"connect failed: {last}")

try:
    cluster, s = connect()
    s.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = "
              "{'class':'SimpleStrategy','replication_factor':1}")
    s.execute(f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} "
              "(id int PRIMARY KEY, name text, age int)")
    # Fixed, known mutation set — the parity ground truth.
    ROWS = [
        (1, 'alice',   30),
        (2, 'bob',     25),
        (3, 'carol',   42),
        (4, 'dave',    37),
        (5, 'eve',     29),
    ]
    ins = s.prepare(f"INSERT INTO {KEYSPACE}.{TABLE} (id, name, age) VALUES (?, ?, ?)")
    for r in ROWS:
        s.execute(ins, r)
    tid = None
    for row in s.execute(
        "SELECT id FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
        (KEYSPACE, TABLE)):
        tid = str(row.id)
    gt = {
        "cassandra_image": "cassandra:5.0.2",
        "keyspace": KEYSPACE,
        "table": TABLE,
        "table_id": tid,
        "schema": f"CREATE TABLE {KEYSPACE}.{TABLE} (id int PRIMARY KEY, name text, age int)",
        "primary_key": {"partition": [["id", "int"]], "clustering": []},
        "columns": [["name", "text"], ["age", "int"]],
        "inserts": [{"id": r[0], "name": r[1], "age": r[2]} for r in ROWS],
    }
    with open('/tmp/commitlog-ground-truth.json', 'w') as f:
        json.dump(gt, f, indent=2)
    print("[gt] table_id=" + str(tid), flush=True)
    cluster.shutdown()
except Exception:
    traceback.print_exc(); sys.exit(1)
PYEOF

# Force a group-commit + on-disk sync of the active segment WITHOUT discarding
# it (nodetool flush would discard the user-keyspace mutations from the log).
# A sleep past the default 10s periodic sync guarantees the segment is durable.
log "Waiting for periodic commitlog sync to flush the segment to disk..."
sleep 14

# ---- Capture the active segment(s) ----------------------------------------
mkdir -p "$CL_DIR"
log "Listing active commitlog segments in container..."
SEG_LIST=$($ENGINE exec "$CONTAINER_NAME" bash -lc \
  'ls -1 /var/lib/cassandra/commitlog/CommitLog-*.log 2>/dev/null || true')
if [[ -z "$SEG_LIST" ]]; then
  fail "No CommitLog-*.log segments found in container."
fi
# Choose the most-recently-modified segment (the one holding our writes).
SEG_PATH=$($ENGINE exec "$CONTAINER_NAME" bash -lc \
  'ls -1t /var/lib/cassandra/commitlog/CommitLog-*.log | head -1')
SEG_NAME=$(basename "$SEG_PATH")
log "Capturing segment: $SEG_NAME"

RAW="$CL_DIR/raw-$SEG_NAME"
$ENGINE exec "$CONTAINER_NAME" cat "$SEG_PATH" > "$RAW"
$ENGINE cp "$CONTAINER_NAME:/tmp/commitlog-ground-truth.json" "$CL_DIR/commitlog-ground-truth.json"

# ---- Derive small committed fixtures (trim trailing zero pre-allocation) ---
log "Deriving trimmed / truncated / corrupt fixtures..."
SEG_NAME="$SEG_NAME" RAW="$RAW" CL_DIR="$CL_DIR" python3 - <<'PYEOF'
import os
raw_path = os.environ["RAW"]; cl_dir = os.environ["CL_DIR"]; seg = os.environ["SEG_NAME"]
with open(raw_path, 'rb') as f:
    data = f.read()
# Trim the 32MB zero pre-allocation. Keep a 64-byte zero tail so the reader
# sees a clean end-of-segment (size==0) rather than a torn read.
last_nz = len(data)
while last_nz > 0 and data[last_nz - 1] == 0:
    last_nz -= 1
end = min(len(data), last_nz + 64)
clean = data[:end]
clean_path = os.path.join(cl_dir, f"clean-{seg}")
with open(clean_path, 'wb') as f:
    f.write(clean)
print(f"[trim] raw={len(data)} clean={len(clean)} (last_nonzero={last_nz})")

# Truncated fixture: tear the segment partway through the used region so the
# final record decode hits EOF (torn tail). Cut ~40 bytes before the clean end
# of the used data, landing mid-record after several clean records.
tear_at = max(0, last_nz - 40)
with open(os.path.join(cl_dir, f"truncated-{seg}"), 'wb') as f:
    f.write(data[:tear_at])
print(f"[trunc] tear_at={tear_at}")

# Corrupt-CRC fixture: flip one byte in the used payload region (well past the
# 4+8+2+len(params)+4 descriptor header + first sync marker), so a per-record
# CRC check fails while the descriptor still parses.
corrupt = bytearray(clean)
flip = min(len(corrupt) - 1, 120)
corrupt[flip] ^= 0xFF
with open(os.path.join(cl_dir, f"corrupt-crc-{seg}"), 'wb') as f:
    f.write(corrupt)
print(f"[corrupt] flipped byte at {flip}")
PYEOF

log "=== CommitLog fixtures COMPLETE ==="
log "  ground truth : $CL_DIR/commitlog-ground-truth.json"
log "  clean        : $CL_DIR/clean-$SEG_NAME"
log "  truncated    : $CL_DIR/truncated-$SEG_NAME"
log "  corrupt-crc  : $CL_DIR/corrupt-crc-$SEG_NAME"
log "  raw (gitignored): $RAW"
log ""
log "Commit the small fixtures with: git add -f test-data/datasets/commitlog/clean-* \\"
log "  test-data/datasets/commitlog/truncated-* test-data/datasets/commitlog/corrupt-crc-* \\"
log "  test-data/datasets/commitlog/commitlog-ground-truth.json"

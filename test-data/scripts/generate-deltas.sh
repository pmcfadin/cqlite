#!/usr/bin/env bash
# generate-deltas.sh — Generate delete-bearing SSTable fixtures for test_deltas keyspace
#
# Part of issue #701 (DS5 delta-scan fixtures).  Coordinates with issue #667.
#
# Creates a new test_deltas keyspace containing eight tables that each exercise
# a distinct delete / write-shape visible in SSTable cells:
#
#   1. cell_tombstones      — DELETE col FROM … (cell tombstone)
#   2. row_tombstones       — DELETE FROM … WHERE pk AND ck (row tombstone)
#   3. range_tombstones     — DELETE FROM … WHERE pk AND ck>=… (range tombstone,
#                             multi-column CK, prefix bound, mixed inclusivity)
#   4. partition_tombstones — DELETE FROM … WHERE pk (partition tombstone)
#   5. ttl_cells            — INSERT … USING TTL N (live TTL cells)
#   6. static_with_rows     — STATIC column + regular rows in same partition
#   7. collection_ops       — SET append / overwrite / element remove
#   8. partial_updates      — UPDATE only (no liveness) vs INSERT (has liveness)
#
# Each table is flushed as a SINGLE SSTable generation via nodetool flush.
# sstabledump JSONL golden files are generated alongside each Data.db.
# Binary Data.db files are exported to test-data/datasets/sstables/test_deltas/.
#
# Usage:
#   bash test-data/scripts/generate-deltas.sh [--out <dir>] [--dry-run]
#
# Options:
#   --out <dir>   Output directory (default: test-data/datasets)
#   --dry-run     Print commands without executing
#
# Prerequisites:
#   - Docker available in PATH
#   - ~4 GB RAM available for the Cassandra container
#
# Closes: #701 (coordinates with #667)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-deltas"
CASSANDRA_IMAGE="cassandra:5.0.2"

# ---------------------------------------------------------------------------
# Parse CLI flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[deltas] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# Canonicalise OUT_DIR
if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[deltas] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[deltas][ERROR] $*" >&2; exit 1; }

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

# Detect container engine
if command -v docker >/dev/null 2>&1; then
  ENGINE="docker"
elif command -v podman >/dev/null 2>&1; then
  ENGINE="podman"
else
  fail "Neither docker nor podman found in PATH."
fi
log "Using container engine: $ENGINE"

# ---------------------------------------------------------------------------
# Guard: ensure no leftover container
# ---------------------------------------------------------------------------
if $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

# ---------------------------------------------------------------------------
# Cleanup trap
# ---------------------------------------------------------------------------
cleanup() {
  log "Cleaning up container..."
  $ENGINE rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helper: wait for Cassandra readiness
# ---------------------------------------------------------------------------
wait_cassandra() {
  local max_retries=60
  local delay=5
  log "Waiting for Cassandra to become ready (max ${max_retries}x${delay}s)..."
  for i in $(seq 1 "$max_retries"); do
    if $ENGINE exec "$CONTAINER_NAME" \
        cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra is ready (attempt $i)."
      return 0
    fi
    sleep "$delay"
  done
  fail "Cassandra did not become ready in time."
}

# ---------------------------------------------------------------------------
# Helper: apply a schema file via cqlsh
# ---------------------------------------------------------------------------
apply_schema() {
  local schema_file="$1"
  local dest_name
  dest_name="$(basename "$schema_file")"
  log "Applying schema: $dest_name"
  run $ENGINE cp "$schema_file" "$CONTAINER_NAME:/tmp/$dest_name"
  run $ENGINE exec "$CONTAINER_NAME" cqlsh -f "/tmp/$dest_name"
}

# ---------------------------------------------------------------------------
# Insert rows covering all eight delete/shape cases
# ---------------------------------------------------------------------------
insert_deltas_rows() {
  log "Inserting test_deltas rows (all eight delete/shape cases)..."
  run $ENGINE exec -i "$CONTAINER_NAME" python3 - <<'PYEOF'
import sys, traceback, time
from cassandra.cluster import Cluster

def connect_with_retry(keyspace, attempts=12, delay=6):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect(keyspace)
            print(f"[connect] Connected to {keyspace} on attempt {attempt}", flush=True)
            return cluster, session
        except Exception as exc:
            last_exc = exc
            print(f"[connect] Attempt {attempt}/{attempts} failed: {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to {keyspace} after {attempts} attempts: {last_exc}")

try:
    cluster, session = connect_with_retry('test_deltas')

    # -------------------------------------------------------------------
    # 1. cell_tombstones
    #    Insert rows, then null-out col_b on selected rows (cell tombstone).
    # -------------------------------------------------------------------
    print("[1] cell_tombstones", flush=True)
    for pk in range(1, 4):          # 3 partitions
        for ck in range(1, 6):      # 5 clustering rows each
            session.execute(
                "INSERT INTO cell_tombstones (pk, ck, col_a, col_b) VALUES (%s,%s,%s,%s)",
                (pk, ck, f"a_{pk}_{ck}", f"b_{pk}_{ck}")
            )
    # Cell tombstones: null out col_b on even ck values in pk=1 and pk=2
    for pk in [1, 2]:
        for ck in [2, 4]:
            session.execute(
                "UPDATE cell_tombstones SET col_b = null WHERE pk=%s AND ck=%s",
                (pk, ck)
            )
    print("  cell_tombstones: done", flush=True)

    # -------------------------------------------------------------------
    # 2. row_tombstones
    #    Insert rows, delete specific clustering rows (row tombstone).
    # -------------------------------------------------------------------
    print("[2] row_tombstones", flush=True)
    for pk in range(1, 4):
        for ck in range(1, 6):
            session.execute(
                "INSERT INTO row_tombstones (pk, ck, val) VALUES (%s,%s,%s)",
                (pk, ck, f"v_{pk}_{ck}")
            )
    # Row tombstones: delete ck=3 from pk=1, and ck=1,ck=5 from pk=2
    session.execute("DELETE FROM row_tombstones WHERE pk=1 AND ck=3")
    session.execute("DELETE FROM row_tombstones WHERE pk=2 AND ck=1")
    session.execute("DELETE FROM row_tombstones WHERE pk=2 AND ck=5")
    print("  row_tombstones: done", flush=True)

    # -------------------------------------------------------------------
    # 3. range_tombstones
    #    Multi-column clustering key (pk, ck1, ck2).
    #    Three partitions with different range tombstone shapes:
    #      pk=1 — prefix bound: DELETE WHERE pk=1 AND ck1=2 (all ck2 for ck1=2)
    #      pk=2 — closed-open:  DELETE WHERE pk=2 AND ck1>=2 AND ck1<4
    #      pk=3 — mixed open/closed: DELETE WHERE pk=3 AND ck1>1 AND ck1<=3
    # -------------------------------------------------------------------
    print("[3] range_tombstones", flush=True)
    for pk in range(1, 4):
        for ck1 in range(1, 6):
            for ck2 in ['alpha', 'beta', 'gamma']:
                session.execute(
                    "INSERT INTO range_tombstones (pk, ck1, ck2, val) VALUES (%s,%s,%s,%s)",
                    (pk, ck1, ck2, f"v_{pk}_{ck1}_{ck2}")
                )
    # pk=1: prefix bound on ck1=2 (deletes all ck2 values for that ck1)
    session.execute(
        "DELETE FROM range_tombstones WHERE pk=1 AND ck1=2"
    )
    # pk=2: closed-open range on ck1 [2, 4)
    session.execute(
        "DELETE FROM range_tombstones WHERE pk=2 AND ck1>=2 AND ck1<4"
    )
    # pk=3: mixed open/closed (1, 3]
    session.execute(
        "DELETE FROM range_tombstones WHERE pk=3 AND ck1>1 AND ck1<=3"
    )
    print("  range_tombstones: done", flush=True)

    # -------------------------------------------------------------------
    # 4. partition_tombstones
    #    Insert rows into several partitions; delete whole partitions.
    # -------------------------------------------------------------------
    print("[4] partition_tombstones", flush=True)
    for pk in range(1, 6):         # 5 partitions
        for ck in range(1, 4):     # 3 rows each
            session.execute(
                "INSERT INTO partition_tombstones (pk, ck, val) VALUES (%s,%s,%s)",
                (pk, ck, f"v_{pk}_{ck}")
            )
    # Delete partitions pk=2 and pk=4 entirely
    session.execute("DELETE FROM partition_tombstones WHERE pk=2")
    session.execute("DELETE FROM partition_tombstones WHERE pk=4")
    print("  partition_tombstones: done", flush=True)

    # -------------------------------------------------------------------
    # 5. ttl_cells
    #    INSERT with explicit USING TTL so SSTable cells carry ttl +
    #    local_deletion_time metadata. Cells are still live.
    # -------------------------------------------------------------------
    print("[5] ttl_cells", flush=True)
    for pk in range(1, 4):
        for ck in range(1, 6):
            # TTL=3600 seconds — live cells with expiration metadata
            session.execute(
                "INSERT INTO ttl_cells (pk, ck, val, extra) VALUES (%s,%s,%s,%s) USING TTL 3600",
                (pk, ck, f"val_{pk}_{ck}", f"extra_{pk}_{ck}")
            )
    # Also insert some rows without TTL in the same table for contrast
    for ck in range(1, 4):
        session.execute(
            "INSERT INTO ttl_cells (pk, ck, val, extra) VALUES (%s,%s,%s,%s)",
            (10, ck, f"notll_{ck}", f"notll_extra_{ck}")
        )
    print("  ttl_cells: done", flush=True)

    # -------------------------------------------------------------------
    # 6. static_with_rows
    #    Write static_col (partition-level) plus per-clustering-row col_a.
    # -------------------------------------------------------------------
    print("[6] static_with_rows", flush=True)
    for pk in range(1, 4):
        # Write static column
        session.execute(
            "UPDATE static_with_rows SET static_col=%s WHERE pk=%s",
            (f"static_val_{pk}", pk)
        )
        # Write regular rows
        for ck in range(1, 5):
            session.execute(
                "INSERT INTO static_with_rows (pk, ck, row_col) VALUES (%s,%s,%s)",
                (pk, ck, f"row_{pk}_{ck}")
            )
    # One partition with static_col only (no clustering rows)
    session.execute(
        "UPDATE static_with_rows SET static_col=%s WHERE pk=%s",
        ("static_only_val", 99)
    )
    print("  static_with_rows: done", flush=True)

    # -------------------------------------------------------------------
    # 7. collection_ops
    #    Demonstrate SET append, overwrite, element removal in same table.
    #
    #    pk=1: INSERT with initial set, then APPEND (s = s + {…})
    #    pk=2: INSERT with initial set, then OVERWRITE (s = {…})
    #    pk=3: INSERT with initial set, then ELEMENT REMOVE (s = s - {…})
    # -------------------------------------------------------------------
    print("[7] collection_ops", flush=True)
    # pk=1, ck=1: append scenario
    session.execute(
        "INSERT INTO collection_ops (pk, ck, tags, vals, props) VALUES (%s,%s,%s,%s,%s)",
        (1, 1, {'initial', 'keep'}, [10, 20, 30], {'k1': 'v1', 'k2': 'v2'})
    )
    session.execute(
        "UPDATE collection_ops SET tags = tags + %s WHERE pk=1 AND ck=1",
        ({'appended'},)
    )
    # pk=2, ck=1: overwrite scenario
    session.execute(
        "INSERT INTO collection_ops (pk, ck, tags, vals, props) VALUES (%s,%s,%s,%s,%s)",
        (2, 1, {'old_a', 'old_b'}, [1, 2], {'old': 'val'})
    )
    session.execute(
        "UPDATE collection_ops SET tags = %s WHERE pk=2 AND ck=1",
        ({'only_this'},)
    )
    # pk=3, ck=1: element removal scenario
    session.execute(
        "INSERT INTO collection_ops (pk, ck, tags, vals, props) VALUES (%s,%s,%s,%s,%s)",
        (3, 1, {'keep_me', 'remove_me', 'also_keep'}, [100, 200], {'rk': 'rv'})
    )
    session.execute(
        "UPDATE collection_ops SET tags = tags - %s WHERE pk=3 AND ck=1",
        ({'remove_me'},)
    )
    # pk=4: additional rows with no collection mutations (baseline)
    for ck in range(1, 4):
        session.execute(
            "INSERT INTO collection_ops (pk, ck, tags, vals, props) VALUES (%s,%s,%s,%s,%s)",
            (4, ck, {f'tag_{ck}'}, [ck * 10], {f'key_{ck}': f'val_{ck}'})
        )
    print("  collection_ops: done", flush=True)

    # -------------------------------------------------------------------
    # 8. partial_updates
    #    ck=1 created via INSERT (has row liveness token in SSTable).
    #    ck=2 created via UPDATE only (no liveness token — column-level only).
    #    ck=3: INSERT then UPDATE one column (mixed: liveness + new write).
    # -------------------------------------------------------------------
    print("[8] partial_updates", flush=True)
    for pk in range(1, 4):
        # ck=1: INSERT — produces a row liveness token
        session.execute(
            "INSERT INTO partial_updates (pk, ck, col_a, col_b) VALUES (%s,%s,%s,%s)",
            (pk, 1, f"a_insert_{pk}", f"b_insert_{pk}")
        )
        # ck=2: UPDATE only — no row liveness token in SSTable
        session.execute(
            "UPDATE partial_updates SET col_a=%s, col_b=%s WHERE pk=%s AND ck=2",
            (f"a_update_{pk}", f"b_update_{pk}", pk)
        )
        # ck=3: INSERT then partial UPDATE of col_a only
        session.execute(
            "INSERT INTO partial_updates (pk, ck, col_a, col_b) VALUES (%s,%s,%s,%s)",
            (pk, 3, f"a_orig_{pk}", f"b_orig_{pk}")
        )
        session.execute(
            "UPDATE partial_updates SET col_a=%s WHERE pk=%s AND ck=3",
            (f"a_updated_{pk}", pk)
        )
    print("  partial_updates: done", flush=True)

    print("[OK] test_deltas: all rows inserted", flush=True)
    cluster.shutdown()

except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during test_deltas row insertion:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Generate sstabledump JSONL golden files
# ---------------------------------------------------------------------------
generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files for test_deltas..."
  while IFS= read -r -d '' data_file; do
    local rel
    rel="${data_file#"$sstables_dir"/}"
    # Strip "data/" prefix (the archive was created from /var/lib/cassandra)
    local rel_sstabledump="${rel#data/}"
    local jsonl_file="${data_file%.db}.db.jsonl"
    log "  sstabledump: $rel"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "[dry-run] sstabledump $data_file > $jsonl_file"
    else
      $ENGINE exec "$CONTAINER_NAME" bash -lc \
        "/opt/cassandra/tools/bin/sstabledump /var/lib/cassandra/data/${rel_sstabledump} -l" \
        | python3 -c "
import json, sys
try:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        print(json.dumps(item, separators=(',', ':')))
except Exception as e:
    print(json.dumps({'error': str(e)}), file=sys.stderr)
    raise
" > "$jsonl_file"
      if [[ ! -s "$jsonl_file" ]]; then
        log "  WARNING: JSONL file is empty: $jsonl_file"
      else
        local lines
        lines=$(wc -l < "$jsonl_file" | tr -d ' ')
        log "  OK: $jsonl_file ($lines partitions)"
      fi
    fi
  done < <(find "$sstables_dir" -type f -name "*-Data.db" -not -name "._*" -print0 \
            | grep -z 'test_deltas' 2>/dev/null || true)
}

# ---------------------------------------------------------------------------
# Guard OUT_DIR path safety
# ---------------------------------------------------------------------------
if [[ "${#OUT_DIR}" -lt 4 ]]; then
  fail "OUT_DIR '$OUT_DIR' is suspiciously short (< 4 chars). Refusing."
fi
case "$OUT_DIR" in
  /) fail "Refusing to operate on '/'." ;;
  /tmp) fail "Refusing to use '/tmp' directly. Use a subdirectory." ;;
esac
_under_repo=0
_under_tmp=0
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] && _under_repo=1
[[ "$OUT_DIR" == /tmp/*          ]] && _under_tmp=1
if [[ "$_under_repo" -eq 0 && "$_under_tmp" -eq 0 ]]; then
  fail "OUT_DIR '$OUT_DIR' is not under the repo root or /tmp/."
fi

log "Starting test_deltas generation"
log "Output directory: $OUT_DIR"

SSTABLES_DIR="$OUT_DIR/sstables"

# ---------------------------------------------------------------------------
# Start Cassandra container (nb/CASSANDRA_4 compat mode — default for 5.0.2)
# ---------------------------------------------------------------------------
log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-deltas \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# Install Python driver
log "Installing python3-pip in container..."
run $ENGINE exec "$CONTAINER_NAME" bash -c "apt-get update -qq && apt-get install -y -q python3-pip"
log "Installing cassandra-driver in container..."
run $ENGINE exec "$CONTAINER_NAME" pip3 install --quiet cassandra-driver

# Apply schema
apply_schema "$ROOT/schemas/deltas.cql"

# Insert all rows
insert_deltas_rows

# Flush all tables to produce SSTables
log "Flushing test_deltas keyspace to SSTables..."
run $ENGINE exec "$CONTAINER_NAME" nodetool flush "test_deltas"

# ---------------------------------------------------------------------------
# Export SSTables to host
# ---------------------------------------------------------------------------
log "=== Exporting test_deltas SSTables from container ==="

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  # Stream the cassandra data directory via tar — same approach as regenerate-datasets.sh
  TMPDIR_EXPORT="$OUT_DIR/.deltas_export_tmp"
  rm -rf "$TMPDIR_EXPORT"
  mkdir -p "$TMPDIR_EXPORT"

  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    # Move only the test_deltas directory into SSTABLES_DIR
    if [[ -d "$TMPDIR_EXPORT/data/test_deltas" ]]; then
      mkdir -p "$SSTABLES_DIR/test_deltas"
      # Merge any existing content — copy table dirs over
      cp -r "$TMPDIR_EXPORT/data/test_deltas/." "$SSTABLES_DIR/test_deltas/"
      log "test_deltas SSTables placed in $SSTABLES_DIR/test_deltas"
    else
      fail "Expected $TMPDIR_EXPORT/data/test_deltas but it was not found. Export failed."
    fi
    rm -rf "$TMPDIR_EXPORT"
  else
    fail "tar export from container failed."
  fi

  # Verify at least one Data.db per table
  log "Verifying exported Data.db files..."
  local_count=$(find "$SSTABLES_DIR/test_deltas" -name "*-Data.db" -not -name "._*" | wc -l | tr -d ' ')
  if [[ "$local_count" -eq 0 ]]; then
    fail "No Data.db files found under $SSTABLES_DIR/test_deltas — export is empty!"
  fi
  log "  Found $local_count Data.db file(s) in test_deltas"

  # Generate JSONL golden files
  generate_sstabledump_jsonl "$SSTABLES_DIR"

  # Generate Statistics.db.txt reference files (matches nb corpus convention)
  log "Generating Statistics.db.txt for test_deltas tables..."
  while IFS= read -r -d '' data_file; do
    rel="${data_file#"$SSTABLES_DIR"/}"
    # Rewrite path to strip "test_deltas/..." prefix for the plain filename
    stats_base="${data_file%Data.db}Statistics.db.txt"
    log "  sstablemetadata: $rel"
    $ENGINE run --rm \
      -v "$SSTABLES_DIR:/data" \
      "$CASSANDRA_IMAGE" \
      bash -lc "/opt/cassandra/tools/bin/sstablemetadata /data/${rel}" \
      > "$stats_base" 2>/dev/null || true
    if [[ -s "$stats_base" ]]; then
      log "  OK: $stats_base"
    else
      log "  WARNING: Empty statistics for $rel"
    fi
  done < <(find "$SSTABLES_DIR/test_deltas" -name "*-Data.db" -not -name "._*" -print0)

  # Remove macOS AppleDouble files if present
  find "$SSTABLES_DIR/test_deltas" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

  log "=== test_deltas generation COMPLETE ==="
  log "SSTables:   $SSTABLES_DIR/test_deltas"
  log ""
  log "Next steps:"
  log "  1. Run smoke test: bash test-data/scripts/smoke-test-all-tables.sh"
  log "  2. Verify JSONL goldens are non-empty"
  log "  3. Package and publish: bash test-data/scripts/package_datasets.sh"
fi

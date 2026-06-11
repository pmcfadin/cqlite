#!/usr/bin/env bash
# regenerate-datasets.sh — Reproduce the datasets-v3 corpus (nb + oa + da keyspaces)
#
# This script implements the procedure documented in
# docs/reports/fixture-version-matrix.md §2b and used to produce
# the cassandra5-small-full-v3 release.
#
# Prerequisites:
#   - Docker (or Podman) available in PATH
#   - ~10 GB free disk space
#   - ~4 GB RAM available for the Cassandra container
#
# Usage:
#   bash test-data/scripts/regenerate-datasets.sh [--out <dir>] [--rows N]
#
# Options:
#   --out <dir>    Destination directory (default: test-data/datasets)
#   --rows N       Approximate rows per table for nb corpus (default: 50)
#                  Lower = faster; increase for larger fixtures.
#   --dry-run      Print commands without executing any container operations.
#
# IMPORTANT: Only one regeneration run may execute at a time.
# The container is named "cqlite-regen" — ensure no container with
# that name exists before running.
#
# Closes: #665 — part of epic #663

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
ROWS_PER_TABLE="${ROWS:-50}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-regen"
CASSANDRA_IMAGE="cassandra:5.0.2"

# ---------------------------------------------------------------------------
# Parse CLI flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)    OUT_DIR="$2";        shift 2 ;;
    --rows)   ROWS_PER_TABLE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;          shift   ;;
    *) echo "[regen] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[regen] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[regen][ERROR] $*" >&2; exit 1; }

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
# Helper: insert rows — uses inline Python via docker exec (no external image)
# ---------------------------------------------------------------------------
insert_nb_rows() {
  local keyspace="$1"
  local rows="$2"
  log "Inserting ~$rows rows per table into keyspace $keyspace (nb format)..."
  run $ENGINE exec "$CONTAINER_NAME" python3 - <<PYEOF
import uuid, random, time, datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('$keyspace')

tables_rs = session.execute(
    "SELECT table_name FROM system_schema.tables WHERE keyspace_name='$keyspace';"
)
tables = [r.table_name for r in tables_rs]

for tbl in tables:
    cols_rs = session.execute(
        f"SELECT column_name, kind, type FROM system_schema.columns "
        f"WHERE keyspace_name='$keyspace' AND table_name='{tbl}' ALLOW FILTERING;"
    )
    cols = {r.column_name: (r.kind, r.type) for r in cols_rs}

    pk_cols = [c for c, (k, t) in cols.items() if k == 'partition_key']
    ck_cols = [c for c, (k, t) in cols.items() if k == 'clustering']
    reg_cols = [c for c, (k, t) in cols.items() if k == 'regular']

    if not pk_cols:
        print(f"  [skip] {tbl}: no partition key found")
        continue

    def sample_val(ctype):
        ctype = ctype.lower()
        if 'uuid' in ctype or 'timeuuid' in ctype:
            return uuid.uuid4()
        if 'bigint' in ctype or 'counter' in ctype:
            return random.randint(1, 10**9)
        if 'int' in ctype or 'smallint' in ctype or 'tinyint' in ctype or 'varint' in ctype:
            return random.randint(1, 10000)
        if 'float' in ctype or 'double' in ctype or 'decimal' in ctype:
            return round(random.uniform(1.0, 1000.0), 4)
        if 'boolean' in ctype:
            return random.choice([True, False])
        if 'timestamp' in ctype:
            return datetime.datetime.utcnow() - datetime.timedelta(seconds=random.randint(0, 86400*30))
        if 'date' in ctype:
            return datetime.date.today() - datetime.timedelta(days=random.randint(0, 365))
        if 'time' in ctype:
            return random.randint(0, 86399999999999)
        if 'blob' in ctype or 'binary' in ctype:
            return bytes(random.getrandbits(8) for _ in range(16))
        if 'inet' in ctype:
            return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        if 'list<' in ctype:
            return [random.randint(1, 1000) for _ in range(3)]
        if 'set<' in ctype:
            return {f"tag{random.randint(1,100)}" for _ in range(3)}
        if 'map<' in ctype:
            return {f"k{i}": f"v{random.randint(1,100)}" for i in range(3)}
        # text, varchar, ascii, duration, etc.
        return f"val_{random.randint(1,10000)}"

    n_inserted = 0
    for _ in range($rows):
        all_cols = pk_cols + ck_cols + reg_cols
        vals = [sample_val(cols[c][1]) for c in all_cols]
        placeholders = ", ".join(["?"] * len(all_cols))
        col_list = ", ".join(all_cols)
        try:
            stmt = session.prepare(
                f"INSERT INTO {tbl} ({col_list}) VALUES ({placeholders})"
            )
            session.execute(stmt, vals)
            n_inserted += 1
        except Exception as e:
            pass  # Skip problematic rows (counters, duration, etc.)

    print(f"  {tbl}: {n_inserted} rows inserted")

cluster.shutdown()
PYEOF
}

insert_oa_rows() {
  log "Inserting rows into test_oa (oa format)..."
  run $ENGINE exec "$CONTAINER_NAME" python3 - <<PYEOF
import uuid, random, datetime
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('test_oa')

now = datetime.datetime.utcnow()

# simple_table
for _ in range(20):
    session.execute(
        "INSERT INTO simple_table (id, name, age, salary, height, weight, active, created) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (uuid.uuid4(), f"user_{random.randint(1,1000)}", random.randint(18, 80),
         random.randint(30000, 200000), round(random.uniform(1.5, 2.1), 2),
         round(random.uniform(50.0, 120.0), 2), random.choice([True, False]),
         now - datetime.timedelta(days=random.randint(0, 365))))

# collection_table
for _ in range(20):
    session.execute(
        "INSERT INTO collection_table (id, tags, scores, properties) VALUES (%s,%s,%s,%s)",
        (uuid.uuid4(),
         {f"tag{i}" for i in range(random.randint(1, 5))},
         [random.randint(1, 100) for _ in range(random.randint(1, 5))],
         {f"k{i}": f"v{i}" for i in range(random.randint(1, 4))}))

# udt_table — address UDT (large_field > 128 bytes exercises oa code path)
from cassandra.util import OrderedMapSerializedKey
addr_type = cluster.metadata.keyspaces['test_oa'].user_types['address_type']
for _ in range(10):
    addr = addr_type(
        street=f"{random.randint(1,9999)} Main St",
        city=random.choice(["Springfield", "Shelbyville", "Portland"]),
        country="US",
        postal_code=f"{random.randint(10000,99999)}"
    )
    session.execute(
        "INSERT INTO udt_table (id, name, address, large_field) VALUES (%s,%s,%s,%s)",
        (uuid.uuid4(), f"person_{random.randint(1,100)}", addr, "x" * 200))

# ttl_table (schema has default_time_to_live=86400)
for _ in range(15):
    session.execute(
        "INSERT INTO ttl_table (id, data, expiring_value) VALUES (%s,%s,%s)",
        (uuid.uuid4(), f"data_{random.randint(1,10000)}", random.randint(1, 9999)))

# static_table
for pk_i in range(5):
    pk = uuid.uuid4()
    for ck_i in range(4):
        session.execute(
            "INSERT INTO static_table (partition_key, clustering_key, static_col, row_data) VALUES (%s,%s,%s,%s)",
            (pk, ck_i, f"static_{pk_i}", f"row_{ck_i}"))

# tombstone_table — insert then delete to create tombstones
pk = uuid.uuid4()
ts_base = now
for i in range(8):
    session.execute(
        "INSERT INTO tombstone_table (id, ts, value, extra) VALUES (%s,%s,%s,%s)",
        (pk, ts_base + datetime.timedelta(seconds=i), f"v{i}", f"e{i}"))
# Create row tombstone
session.execute("DELETE FROM tombstone_table WHERE id=%s AND ts=%s",
                (pk, ts_base + datetime.timedelta(seconds=3)))
# Create cell tombstone
session.execute("UPDATE tombstone_table SET extra=null WHERE id=%s AND ts=%s",
                (pk, ts_base + datetime.timedelta(seconds=2)))
# Range tombstone
pk2 = uuid.uuid4()
for i in range(5):
    session.execute(
        "INSERT INTO tombstone_table (id, ts, value, extra) VALUES (%s,%s,%s,%s)",
        (pk2, ts_base + datetime.timedelta(seconds=i*10), f"vr{i}", f"er{i}"))
session.execute(
    "DELETE FROM tombstone_table WHERE id=%s AND ts >= %s AND ts <= %s",
    (pk2, ts_base, ts_base + datetime.timedelta(seconds=20)))

print("test_oa: rows inserted")
cluster.shutdown()
PYEOF
}

insert_da_rows() {
  log "Inserting rows into test_da (da/BTI format)..."
  run $ENGINE exec "$CONTAINER_NAME" python3 - <<PYEOF
import uuid, random, datetime
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('test_da')

now = datetime.datetime.utcnow()

# simple_table
for _ in range(15):
    session.execute(
        "INSERT INTO simple_table (id, name, age, salary, active, created) VALUES (%s,%s,%s,%s,%s,%s)",
        (uuid.uuid4(), f"user_{random.randint(1,1000)}", random.randint(18, 80),
         random.randint(30000, 200000), random.choice([True, False]),
         now - datetime.timedelta(days=random.randint(0, 365))))

# collection_table
for _ in range(15):
    session.execute(
        "INSERT INTO collection_table (id, tags, scores, properties) VALUES (%s,%s,%s,%s)",
        (uuid.uuid4(),
         {f"tag{i}" for i in range(random.randint(1, 4))},
         [random.randint(1, 100) for _ in range(random.randint(1, 4))],
         {f"k{i}": f"v{i}" for i in range(random.randint(1, 3))}))

# ttl_table (schema has default_time_to_live=86400)
for _ in range(15):
    session.execute(
        "INSERT INTO ttl_table (id, data, expiring_value) VALUES (%s,%s,%s)",
        (uuid.uuid4(), f"data_{random.randint(1,10000)}", random.randint(1, 9999)))

print("test_da: rows inserted")
cluster.shutdown()
PYEOF
}

generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files..."
  # Find all Data.db files and generate .jsonl alongside each
  while IFS= read -r -d '' data_file; do
    local rel
    rel="${data_file#"$sstables_dir"/}"
    local jsonl_file="${data_file%.db}.db.jsonl"
    log "  sstabledump: $rel"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "[dry-run] sstabledump $data_file > $jsonl_file"
    else
      $ENGINE exec "$CONTAINER_NAME" bash -lc \
        "/opt/cassandra/tools/bin/sstabledump /var/lib/cassandra/data/${rel} -l" \
        | python3 -c "
import json, sys
try:
    items = json.loads(sys.stdin.read())
    for item in items:
        print(json.dumps(item, separators=(',', ': ')))
except Exception as e:
    print(json.dumps({'error': str(e)}), file=sys.stderr)
    raise
" > "$jsonl_file"
    fi
  done < <(find "$sstables_dir" -type f -name "*-Data.db" -print0)
}

# ---------------------------------------------------------------------------
# Main procedure
# ---------------------------------------------------------------------------
log "Starting dataset regeneration (nb + oa + da)"
log "Output directory: $OUT_DIR"
log "Rows per table (nb corpus): $ROWS_PER_TABLE"

# Prepare output directory
if [[ "$DRY_RUN" -eq 0 ]]; then
  rm -rf "$OUT_DIR"
  mkdir -p "$OUT_DIR"
fi

# ---------------------------------------------------------------------------
# Phase 1: nb corpus (Cassandra 4-compat mode — default for 5.0.2)
# ---------------------------------------------------------------------------
log "=== Phase 1: nb corpus (storage_compatibility_mode: CASSANDRA_4) ==="

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-regen \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# Install cassandra-driver inside container (needed for row insertion)
log "Installing cassandra-driver in container..."
run $ENGINE exec "$CONTAINER_NAME" pip3 install --quiet cassandra-driver

# Apply core schemas (nb keyspaces)
for schema in basic-types.cql collections.cql time-series.cql wide-rows.cql; do
  if [[ -f "$ROOT/schemas/$schema" ]]; then
    apply_schema "$ROOT/schemas/$schema"
  else
    log "WARNING: Schema file not found: $ROOT/schemas/$schema — skipping."
  fi
done

# Insert rows into each nb keyspace
for ks in test_basic test_collections test_timeseries test_wide_rows; do
  insert_nb_rows "$ks" "$ROWS_PER_TABLE"
done

# Flush + compact nb keyspaces
log "Flushing and compacting nb keyspaces..."
for ks in test_basic test_collections test_timeseries test_wide_rows; do
  run $ENGINE exec "$CONTAINER_NAME" nodetool flush "$ks"
  run $ENGINE exec "$CONTAINER_NAME" nodetool compact "$ks"
done

# ---------------------------------------------------------------------------
# Phase 2: oa corpus (storage_compatibility_mode: NONE)
# ---------------------------------------------------------------------------
log "=== Phase 2: oa corpus (storage_compatibility_mode: NONE) ==="

run $ENGINE exec "$CONTAINER_NAME" bash -c \
  "sed -i 's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g' /etc/cassandra/cassandra.yaml"
log "Restarting container for oa mode..."
run $ENGINE restart "$CONTAINER_NAME"
if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# Apply oa schema
if [[ -f "$ROOT/schemas/oa-test.cql" ]]; then
  apply_schema "$ROOT/schemas/oa-test.cql"
else
  fail "Missing schema: $ROOT/schemas/oa-test.cql"
fi

insert_oa_rows

log "Flushing and compacting oa keyspace..."
run $ENGINE exec "$CONTAINER_NAME" nodetool flush "test_oa"
run $ENGINE exec "$CONTAINER_NAME" nodetool compact "test_oa"

# ---------------------------------------------------------------------------
# Phase 3: da corpus (BTI format via sstable.selected_format: bti)
# ---------------------------------------------------------------------------
log "=== Phase 3: da corpus (sstable.selected_format: bti) ==="

run $ENGINE exec "$CONTAINER_NAME" bash -c \
  "sed -i 's|#sstable:|sstable:|; s|#  selected_format: big|  selected_format: bti|' /etc/cassandra/cassandra.yaml"
log "Restarting container for da/BTI mode..."
run $ENGINE restart "$CONTAINER_NAME"
if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# Apply da schema
if [[ -f "$ROOT/schemas/da-test.cql" ]]; then
  apply_schema "$ROOT/schemas/da-test.cql"
else
  fail "Missing schema: $ROOT/schemas/da-test.cql"
fi

insert_da_rows

log "Flushing and compacting da keyspace..."
run $ENGINE exec "$CONTAINER_NAME" nodetool flush "test_da"
run $ENGINE exec "$CONTAINER_NAME" nodetool compact "test_da"

# ---------------------------------------------------------------------------
# Export SSTables from container to host
# ---------------------------------------------------------------------------
log "=== Exporting SSTables from container ==="

SSTABLES_DIR="$OUT_DIR/sstables"

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  # Stream all Cassandra data out via tar
  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$OUT_DIR" -xf -; then
    if [[ -d "$OUT_DIR/data" ]]; then
      mv "$OUT_DIR/data" "$SSTABLES_DIR"
    fi
    log "SSTables exported to $SSTABLES_DIR"
  else
    fail "tar export from container failed."
  fi

  # Generate JSONL golden files
  generate_sstabledump_jsonl "$SSTABLES_DIR"

  # Write a simple metadata.yml
  {
    echo "generated_at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "cassandra_image: $CASSANDRA_IMAGE"
    echo "rows_per_table_nb: $ROWS_PER_TABLE"
    echo "formats: [nb, oa, da]"
  } > "$OUT_DIR/metadata.yml"
  log "Wrote $OUT_DIR/metadata.yml"
fi

log "=== Dataset regeneration COMPLETE ==="
log "Output: $OUT_DIR"
log ""
log "Next steps:"
log "  1. Verify row counts:  bash test-data/scripts/smoke-test-all-tables.sh"
log "  2. Package:            bash test-data/scripts/package_datasets.sh"
log "  3. Publish:            bash test-data/scripts/publish_datasets.sh"

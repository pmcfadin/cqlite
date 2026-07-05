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
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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
# Helper: wait_driver_connect — retry Cluster.connect() inside python.
# Returns a connected session or raises after max_attempts.
# This is embedded in each Python heredoc via the WAIT_CONNECT_SNIPPET var.
# ---------------------------------------------------------------------------
# (snippet injected inline into each heredoc below)

# ---------------------------------------------------------------------------
# Helper: insert rows — uses inline Python via docker exec (no external image)
# ---------------------------------------------------------------------------
insert_nb_rows() {
  local keyspace="$1"
  local rows="$2"
  log "Inserting ~$rows rows per table into keyspace $keyspace (nb format)..."
  # NOTE: -i is REQUIRED so docker/podman exec attaches stdin to the container
  # process. Without -i, python3 - reads EOF immediately, runs nothing, and
  # exits 0 silently — the silent insert failure we are fixing.
  run $ENGINE exec -i "$CONTAINER_NAME" python3 - <<PYEOF
import sys, traceback, uuid, random, time, datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.util import Duration

def connect_with_retry(keyspace, attempts=10, delay=6):
    """Retry Cluster.connect() — Cassandra may still be warming up after restart."""
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
    raise RuntimeError(f"Could not connect to Cassandra after {attempts} attempts: {last_exc}")

try:
    cluster, session = connect_with_retry('$keyspace')

    tables_rs = session.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name='$keyspace';"
    )
    tables = [r.table_name for r in tables_rs]

    if not tables:
        print(f"[ERROR] No tables found in keyspace '$keyspace' — schema may not have been applied!", flush=True)
        sys.exit(1)

    # ---- Helper functions defined once, before the per-table loop ----

    KNOWN_PRIMITIVES = frozenset({
        'uuid', 'timeuuid', 'text', 'varchar', 'ascii', 'int', 'integer',
        'bigint', 'smallint', 'tinyint', 'varint', 'float', 'double',
        'decimal', 'boolean', 'timestamp', 'date', 'time', 'blob', 'inet',
        'duration', 'counter', 'empty',
    })
    KNOWN_COLLECTION_PREFIXES = ('frozen<', 'list<', 'set<', 'map<', 'tuple<')

    def parse_type_args(type_str):
        """Extract comma-separated top-level type args from e.g. 'map<text, bigint>'."""
        start = type_str.find('<')
        if start == -1:
            return []
        inner = type_str[start+1:-1]
        args, depth, buf = [], 0, ''
        for ch in inner:
            if ch == '<': depth += 1; buf += ch
            elif ch == '>': depth -= 1; buf += ch
            elif ch == ',' and depth == 0:
                args.append(buf.strip()); buf = ''
            else:
                buf += ch
        if buf.strip():
            args.append(buf.strip())
        return args

    def has_udt_type(type_str):
        """Return True if type_str contains a UDT (non-primitive, non-collection) type."""
        t = type_str.strip().lower()
        if t in KNOWN_PRIMITIVES:
            return False
        for prefix in KNOWN_COLLECTION_PREFIXES:
            if t.startswith(prefix):
                inner_args = parse_type_args(t)
                return any(has_udt_type(a) for a in inner_args) if inner_args else False
        return True  # Unrecognized type = UDT

    def get_udt_fields(keyspace_name, type_name):
        """Return a dict of {field_name: field_type} for a UDT in the given keyspace."""
        rs = session.execute(
            "SELECT field_names, field_types FROM system_schema.types "
            "WHERE keyspace_name=%s AND type_name=%s;",
            (keyspace_name, type_name)
        )
        row = rs.one()
        if row is None:
            return {}
        return dict(zip(row.field_names, row.field_types))

    # Discover all UDTs in this keyspace and cache their authoritative field
    # order from system_schema.types; build_udt_value() emits POSITIONAL TUPLES
    # in that declared order, which is what prepared-statement UDT serialization
    # requires (issue #1991). register_user_type(..., dict) only affects how the
    # driver DESERIALIZES UDTs read back — inserts never bind dicts here.
    udt_fields_cache = {}
    try:
        udts_rs = session.execute(
            "SELECT type_name, field_names, field_types FROM system_schema.types "
            "WHERE keyspace_name=%s;",
            ('$keyspace',)
        )
        for udt_row in udts_rs:
            udt_fields_cache[udt_row.type_name] = dict(
                zip(udt_row.field_names, udt_row.field_types)
            )
            cluster.register_user_type('$keyspace', udt_row.type_name, dict)
            print(f"  [udt] Registered UDT: $keyspace.{udt_row.type_name}", flush=True)
    except Exception as udt_exc:
        print(f"  [udt] Could not discover/register UDTs: {udt_exc}", flush=True)

    def build_udt_value(type_name, depth=0):
        """Construct a POSITIONAL TUPLE for a UDT (declared field order),
        recursing for nested UDTs.

        The value MUST be a tuple/sequence in field-declaration order, NOT a
        dict: prepared-statement binary UDT serialization in cassandra-driver
        (UserType.serialize_safe) reads fields positionally via `val[i]`, so a
        dict raises `KeyError: 0` on the very first field and every row is
        skipped (issue #1991 — a driver-version-independent contract; observed
        with cassandra-driver 3.30.0 against cassandra:5.0.2). The unprepared
        `%s` path used by test_oa/test_da accepts a dict via CQL-literal
        encoding, but this generic path prepares its INSERTs, so it needs a
        tuple. udt_fields_cache preserves schema field order (dict(zip(...))),
        so iterating fields.values() yields fields in declared order.
        """
        fields = udt_fields_cache.get(type_name.lower(), {})
        if depth > 5:
            # Recursion guard: emit an all-null tuple of the correct arity
            # (never reached by the current corpus, whose UDTs nest 1 level).
            return tuple(None for _ in fields)
        return tuple(sample_val(ftype, depth=depth + 1) for ftype in fields.values())

    def sample_val(ctype, depth=0):
        """Generate a value for any CQL type, including nested collections and UDTs."""
        ct = ctype.strip().lower()
        # Strip frozen<> wrapper — value generation is the same for frozen and non-frozen
        bare = ct[7:-1].strip() if ct.startswith('frozen<') else ct
        # Collections
        if bare.startswith('list<'):
            args = parse_type_args(bare)
            elem_type = args[0] if args else 'text'
            return [sample_val(elem_type, depth=depth) for _ in range(3)]
        if bare.startswith('set<'):
            args = parse_type_args(bare)
            elem_type = args[0] if args else 'text'
            elems = [sample_val(elem_type, depth=depth) for _ in range(3)]
            # UDT dicts are not hashable; fall back to a list when set() fails
            try:
                return set(elems)
            except TypeError:
                return elems  # driver accepts a list for SET<FROZEN<udt>>
        if bare.startswith('map<'):
            args = parse_type_args(bare)
            k_type = args[0] if len(args) > 0 else 'text'
            v_type = args[1] if len(args) > 1 else 'text'
            result = {}
            for i in range(3):
                k = sample_val(k_type, depth=depth)
                try:
                    result[k] = sample_val(v_type, depth=depth)
                except TypeError:
                    result[f"k{i}"] = sample_val(v_type, depth=depth)
            return result
        # Scalar primitives
        if bare == 'timeuuid':
            return uuid.uuid1()
        if bare == 'uuid':
            return uuid.uuid4()
        if bare in ('bigint', 'counter'):
            return random.randint(1, 10**9)
        if bare == 'tinyint':
            return random.randint(-128, 127)
        if bare == 'smallint':
            return random.randint(-32768, 32767)
        if bare in ('int', 'integer', 'varint'):
            return random.randint(1, 10000)
        if bare in ('float', 'double', 'decimal'):
            return round(random.uniform(1.0, 1000.0), 4)
        if bare == 'boolean':
            return random.choice([True, False])
        if bare == 'timestamp':
            return datetime.datetime.utcnow() - datetime.timedelta(seconds=random.randint(0, 86400*30))
        if bare == 'date':
            return datetime.date.today() - datetime.timedelta(days=random.randint(0, 365))
        if bare == 'time':
            return random.randint(0, 86399999999999)
        if bare in ('blob', 'binary'):
            return bytes(random.getrandbits(8) for _ in range(16))
        if bare == 'inet':
            return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        if bare == 'duration':
            return Duration(months=random.randint(0, 12), days=random.randint(0, 30),
                            nanoseconds=random.randint(0, 10**9))
        # UDT: if the bare type name is a known UDT in this keyspace, build a
        # positional tuple in declared field order (issue #1991)
        if bare in udt_fields_cache:
            return build_udt_value(bare, depth=depth)
        # text, varchar, ascii, and anything unrecognized
        return f"val_{random.randint(1,10000)}"

    # ---- Per-table loop ----
    total_inserted = 0
    for tbl in tables:
        cols_rs = session.execute(
            "SELECT column_name, kind, type FROM system_schema.columns "
            "WHERE keyspace_name='$keyspace' AND table_name=%s ALLOW FILTERING;",
            (tbl,)
        )
        cols = {r.column_name: (r.kind, r.type) for r in cols_rs}

        pk_cols = [c for c, (k, t) in cols.items() if k == 'partition_key']
        ck_cols = [c for c, (k, t) in cols.items() if k == 'clustering']
        reg_cols = [c for c, (k, t) in cols.items() if k == 'regular']

        if not pk_cols:
            print(f"  [skip] {tbl}: no partition key found", flush=True)
            continue

        # Detect tables that have UDT columns; they are handled by the generic
        # path via build_udt_value(), which emits positional tuples (issue #1991).
        all_types_list = [t for _, (_, t) in cols.items()]
        has_udt = any(has_udt_type(t) for t in all_types_list)
        if has_udt:
            print(f"  [udt-table] {tbl}: UDT columns detected — using positional-tuple UDT path", flush=True)

        # Counter tables require UPDATE, not INSERT. Detect by checking if all
        # regular columns are of type 'counter'.
        is_counter_table = (
            len(reg_cols) > 0 and
            all(cols[c][1].lower() == 'counter' for c in reg_cols)
        )

        n_inserted = 0
        n_skipped = 0
        first_tb = None  # full traceback of the first failed row (for an actionable abort)

        if is_counter_table:
            # Counter tables use UPDATE ... SET col = col + N WHERE pk_col = ?
            for _ in range($rows):
                pk_vals = [sample_val(cols[c][1]) for c in pk_cols]
                ck_vals = [sample_val(cols[c][1]) for c in ck_cols]
                set_clause = ", ".join(f"{c} = {c} + ?" for c in reg_cols)
                where_clause = " AND ".join(f"{c} = ?" for c in pk_cols + ck_cols)
                counter_increments = [random.randint(1, 100) for _ in reg_cols]
                try:
                    stmt = session.prepare(
                        f"UPDATE {tbl} SET {set_clause} WHERE {where_clause}"
                    )
                    session.execute(stmt, counter_increments + pk_vals + ck_vals)
                    n_inserted += 1
                except Exception as e:
                    n_skipped += 1
                    if first_tb is None:
                        first_tb = traceback.format_exc()
                    if n_skipped <= 3:
                        print(f"  [row-skip] {tbl}: {type(e).__name__}: {e}", flush=True)
        else:
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
                    # Only skip type-incompatible individual rows (e.g. duration literals).
                    # Print every skip so failures are visible.
                    n_skipped += 1
                    if first_tb is None:
                        first_tb = traceback.format_exc()
                    if n_skipped <= 3:
                        print(f"  [row-skip] {tbl}: {type(e).__name__}: {e}", flush=True)

        if n_inserted == 0:
            # Fail-closed, ACTIONABLE abort (issue #1991): a present table whose
            # every insert failed is a hard error, never a silent skip. Name the
            # offending table and print the FIRST row's full traceback so a
            # future breakage (e.g. a cassandra-driver upgrade changing UDT/tuple
            # representation) is diagnosable straight from the CI log, without a
            # local Docker repro. sys.exit() here also stops before the next
            # table, so one table's failure can't leave a misleading mid-matrix
            # partially-regenerated state.
            print(f"[ERROR] Table '$keyspace.{tbl}': 0 rows inserted ({n_skipped} attempts, all failed) — aborting keyspace regeneration.", flush=True)
            print(f"[ERROR] Table '$keyspace.{tbl}': first-row failure traceback (fix this table's row-builder shape; see issue #1991):", flush=True)
            if first_tb:
                sys.stdout.write(first_tb)
                sys.stdout.flush()
            sys.exit(1)
        print(f"  {tbl}: {n_inserted} rows inserted ({n_skipped} skipped)", flush=True)
        total_inserted += n_inserted

    if total_inserted == 0:
        print(f"[ERROR] Keyspace '$keyspace': 0 total rows inserted — all tables skipped or failed!", flush=True)
        sys.exit(1)
    print(f"[OK] Keyspace '$keyspace': {total_inserted} total rows inserted across {len(tables)} tables", flush=True)
    cluster.shutdown()

except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during nb row insertion:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

insert_oa_rows() {
  log "Inserting rows into test_oa (oa format)..."
  run $ENGINE exec -i "$CONTAINER_NAME" python3 - <<PYEOF
import sys, traceback, uuid, random, datetime, time
from cassandra.cluster import Cluster

def connect_with_retry(keyspace, attempts=10, delay=6):
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
    raise RuntimeError(f"Could not connect after {attempts} attempts: {last_exc}")

try:
    cluster, session = connect_with_retry('test_oa')
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
    # Cassandra-driver 3.x+ represents UDTs as namedtuple-like objects.
    # Register the type first so the driver can serialize it correctly.
    cluster.register_user_type('test_oa', 'address_type',
                               dict)  # Use dict for generic UDT mapping
    for _ in range(10):
        addr = {
            'street': f"{random.randint(1,9999)} Main St",
            'city': random.choice(["Springfield", "Shelbyville", "Portland"]),
            'country': "US",
            'postal_code': f"{random.randint(10000,99999)}"
        }
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

    print("[OK] test_oa: rows inserted", flush=True)
    cluster.shutdown()

except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during oa row insertion:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

insert_da_rows() {
  log "Inserting rows into test_da (da/BTI format)..."
  run $ENGINE exec -i "$CONTAINER_NAME" python3 - <<PYEOF
import sys, traceback, uuid, random, datetime, time
from cassandra.cluster import Cluster

def connect_with_retry(keyspace, attempts=10, delay=6):
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
    raise RuntimeError(f"Could not connect after {attempts} attempts: {last_exc}")

try:
    cluster, session = connect_with_retry('test_da')
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

    print("[OK] test_da: rows inserted", flush=True)
    cluster.shutdown()

except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during da row insertion:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Helper: verify_user_keyspaces — hard check that user data actually exists
# Must be called after export. Fails loudly if any user keyspace is empty.
# ---------------------------------------------------------------------------
verify_user_keyspaces() {
  local sstables_dir="$1"
  log "=== Verifying user keyspace Data.db files ==="
  local all_ok=1

  for ks in test_basic test_collections test_timeseries test_wide_rows test_oa test_da; do
    # Search both flat and nested paths (e.g. data/test_basic/... or test_basic/...)
    local count
    count=$(find "$sstables_dir" -type f -name "*-Data.db" | grep -c "/$ks/" 2>/dev/null || true)
    if [[ "$count" -eq 0 ]]; then
      log "  [FAIL] $ks: 0 Data.db files found — inserts produced no SSTables!"
      all_ok=0
    else
      log "  [OK]   $ks: $count Data.db file(s) found"
    fi
  done

  if [[ "$all_ok" -eq 0 ]]; then
    fail "One or more user keyspaces have empty SSTable directories. Dataset is invalid — aborting."
  fi
  log "All user keyspaces verified with non-empty SSTable data."
}

generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files..."
  # Find all Data.db files and generate .jsonl alongside each
  while IFS= read -r -d '' data_file; do
    local rel
    rel="${data_file#"$sstables_dir"/}"
    # The tar archive was created from /var/lib/cassandra so paths start with
    # "data/".  Strip that prefix before prepending the sstabledump base path
    # to avoid a doubled "data/data/" segment.
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
    # sstabledump -l emits one JSON object per line (NDJSON), not a JSON array.
    # Parse line-by-line and pass each object through as a JSONL record.
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
    fi
  done < <(find "$sstables_dir" -type f -name "*-Data.db" -print0)
}

# Derive a Statistics.db.txt golden alongside each Data.db, so the corpus audit's
# COVERAGE/PRESENCE check finds those `.db.txt` identities present after regen
# (issue #2009). Mirrors the sstablemetadata derivation used by the other
# generators (e.g. generate-write-load-parity.sh / generate-cql-type-parity.sh):
# dump Statistics.db via sstablemetadata to `<...>-Statistics.db.txt` beside the
# component. Uses the SAME running-container exec convention as
# generate_sstabledump_jsonl above and honors the same --dry-run guard.
generate_statistics_txt() {
  local sstables_dir="$1"
  log "Generating Statistics.db.txt golden files..."
  while IFS= read -r -d '' data_file; do
    local rel
    rel="${data_file#"$sstables_dir"/}"
    # As in generate_sstabledump_jsonl: the tar archive paths start with "data/";
    # strip it before prepending the in-container base path.
    local rel_sstabledump="${rel#data/}"
    local stats_file="${data_file%Data.db}Statistics.db.txt"
    log "  sstablemetadata: $rel"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "[dry-run] sstablemetadata $data_file > $stats_file"
    else
      $ENGINE exec "$CONTAINER_NAME" bash -lc \
        "/opt/cassandra/tools/bin/sstablemetadata /var/lib/cassandra/data/${rel_sstabledump}" \
        > "$stats_file" 2>/dev/null || true
      if [[ -s "$stats_file" ]]; then
        log "  OK: $stats_file"
      else
        log "  WARNING: Empty statistics for $rel"
      fi
    fi
  done < <(find "$sstables_dir" -type f -name "*-Data.db" -print0)
}

# ---------------------------------------------------------------------------
# Main procedure
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Guard: refuse dangerous --out paths before any destructive rm -rf
# ---------------------------------------------------------------------------
# Canonicalize: turn relative paths into absolute ones using the working dir.
# OUT_DIR may not exist yet (we are about to create it), so we cannot use
# realpath -e.  Instead: if it starts with '/' it is already absolute;
# otherwise prepend $PWD.
if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi

# Strip any trailing slash for consistent comparisons.
OUT_DIR="${OUT_DIR%/}"

if [[ "${#OUT_DIR}" -lt 4 ]]; then
  fail "OUT_DIR '$OUT_DIR' is suspiciously short (< 4 chars). Refusing to rm -rf."
fi

case "$OUT_DIR" in
  /)
    fail "Refusing to rm -rf '/'. Specify a safe --out directory." ;;
  /tmp)
    fail "Refusing to rm -rf '/tmp' directly. Use a subdirectory, e.g. /tmp/my-datasets." ;;
esac

# Allow only paths that are a strict descendant of the repo root or of /tmp.
_under_repo=0
_under_tmp=0
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] && _under_repo=1
[[ "$OUT_DIR" == /tmp/*          ]] && _under_tmp=1

if [[ "$_under_repo" -eq 0 && "$_under_tmp" -eq 0 ]]; then
  fail "OUT_DIR '$OUT_DIR' is neither under the repo root ('$REPO_ROOT') nor under /tmp/. Refusing to rm -rf an arbitrary path. Pass --out to a path inside the repo or under /tmp/."
fi

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
# python3-pip is not present in the cassandra:5.0.2 image; install it first.
log "Installing python3-pip in container..."
run $ENGINE exec "$CONTAINER_NAME" bash -c "apt-get update -qq && apt-get install -y -q python3-pip"

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

  # Hard check: every user keyspace must have at least one Data.db
  verify_user_keyspaces "$SSTABLES_DIR"

  # Generate JSONL golden files
  generate_sstabledump_jsonl "$SSTABLES_DIR"

  # Derive Statistics.db.txt goldens so their identities are present after regen
  # (issue #2009: the corpus audit's coverage/presence check expects them).
  generate_statistics_txt "$SSTABLES_DIR"

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

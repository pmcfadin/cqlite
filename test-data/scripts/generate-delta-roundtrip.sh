#!/usr/bin/env bash
# generate-delta-roundtrip.sh — Multi-generation workload for DS11 round-trip test (Issue #707)
#
# Creates N=3 distinct SSTable generations for two tables by applying three
# separate write phases, each flushed to a distinct generation before the next
# phase begins:
#
#   Table 1: roundtrip_t (pk INT, ck TEXT, val TEXT, st TEXT STATIC)
#     Gen 1: baseline inserts + static writes + element-removal collection write
#     Gen 2: partial updates (stale-cell scenario), row deletes (resurrection scenario),
#             partition deletes, static-only partition write
#     Gen 3: resurrection-prover inserts (newer writes that survive gen-2 deletes),
#             TTL writes, range deletes
#
#   Table 2: roundtrip_coll (pk INT, ck TEXT, tags SET<TEXT>, PRIMARY KEY (pk, ck))
#     Same three phases — exercises collection append/overwrite/element-removal
#     to produce the v1 element-tombstone warning counter > 0.
#
# After each phase, nodetool flush creates a separate SSTable generation.
# The script then exports each generation directory to a Parquet file using
# the cqlite delta-export CLI.
#
# Output layout:
#   <OUT_DIR>/
#     sstables/
#       roundtrip_ks/
#         roundtrip_t-<uuid>/           # ONE directory (all 3 flushes share it)
#           nb-1-big-Data.db            # Gen 1 SSTable files
#           nb-2-big-Data.db            # Gen 2 SSTable files
#           nb-3-big-Data.db            # Gen 3 SSTable files
#         roundtrip_coll-<uuid>/        # ONE directory (all 3 flushes share it)
#           nb-1-big-Data.db
#           nb-2-big-Data.db
#           nb-3-big-Data.db
#     parquet/
#       roundtrip_t/
#         gen1.parquet
#         gen2.parquet
#         gen3.parquet
#       roundtrip_coll/
#         gen1.parquet
#         gen2.parquet
#         gen3.parquet
#     schemas/
#       roundtrip_t.cql       # bare CREATE TABLE for delta-export
#       roundtrip_coll.cql    # bare CREATE TABLE for delta-export
#       roundtrip_full.cql    # full schema with keyspace for CQLite SELECT *
#
# Usage:
#   bash test-data/scripts/generate-delta-roundtrip.sh [--out <dir>] [--dry-run]
#
# Options:
#   --out <dir>   Output directory (default: /tmp/delta-roundtrip)
#   --dry-run     Print commands without executing
#
# Prerequisites:
#   - Docker available in PATH with Cassandra image accessible
#   - CQLite CLI compiled with delta-export feature:
#       cargo build --package cqlite-cli --features delta-export
#
# The round-trip test (cqlite-cli/tests/delta_roundtrip_tests.rs) expects the
# output directory to be set via DELTA_ROUNDTRIP_DATA env var, or will skip
# with instructions to run this script first.
#
# Closes: #707 (DS11 reconciliation round-trip test)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OUT_DIR="${DELTA_ROUNDTRIP_DATA:-/tmp/delta-roundtrip}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-delta-roundtrip"
CASSANDRA_IMAGE="cassandra:5.0.2"

# ---------------------------------------------------------------------------
# Parse CLI flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[roundtrip] Unknown argument: $1" >&2; exit 1 ;;
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
log()  { echo "[roundtrip] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[roundtrip][ERROR] $*" >&2; exit 1; }

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
# Phase 1: baseline inserts + static writes + initial collection state
#
# Resurrection scenario seed:
#   pk=10 ck='del_me' val='to_be_deleted' — will be deleted in Gen 2.
#   A naive union WITHOUT merge would RESURRECT this row in the merged view.
#
# Stale-cell scenario seed:
#   pk=20 ck='stale' col_a='old_val' — col_a will be updated in Gen 2.
#   Without LWW merge, both the old and new value would appear as candidates.
#
# Collection element-removal scenario (v1 limitation):
#   pk=30 ck='coll' tags={'keep_me','remove_me','also_keep'} — 'remove_me'
#   will be element-removed in Gen 2, producing a v1 element_tombstone warning.
# ---------------------------------------------------------------------------
run_phase1() {
  log "=== Phase 1: baseline inserts + static writes ==="
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
    cluster, session = connect_with_retry('roundtrip_ks')

    # --- roundtrip_t: baseline inserts ---
    # Regular rows: pk 1..5, ck a/b/c
    for pk in range(1, 6):
        for ck in ['a', 'b', 'c']:
            session.execute(
                "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
                (pk, ck, f"gen1_pk{pk}_{ck}")
            )

    # Static writes for pk 1..5
    for pk in range(1, 6):
        session.execute(
            "UPDATE roundtrip_t USING TIMESTAMP 900 SET st=%s WHERE pk=%s",
            (f"static_gen1_{pk}", pk)
        )

    # Resurrection seed: this row will be deleted in Gen 2
    session.execute(
        "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
        (10, 'del_me', 'to_be_deleted')
    )
    session.execute(
        "UPDATE roundtrip_t USING TIMESTAMP 900 SET st=%s WHERE pk=%s",
        ("static_for_pk10", 10)
    )

    # Stale-cell seed: col val='old_val', will be overwritten in Gen 2
    session.execute(
        "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
        (20, 'stale', 'old_val')
    )

    # Partition delete seed: pk=30 rows all with old writes, will partition-delete in Gen 2
    for ck in ['x', 'y', 'z']:
        session.execute(
            "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
            (30, ck, f"old_pk30_{ck}")
        )
    session.execute(
        "UPDATE roundtrip_t USING TIMESTAMP 900 SET st=%s WHERE pk=%s",
        ("static_pk30", 30)
    )

    # Static-only partition seed (no regular rows yet, just static)
    # pk=40 gets only a static write here; it'll survive as a static-only partition
    # even when its regular rows don't exist (Finding 2b in the reconciliation doc)
    session.execute(
        "UPDATE roundtrip_t USING TIMESTAMP 900 SET st=%s WHERE pk=%s",
        ("only_static_pk40", 40)
    )

    print("[roundtrip_t] Phase 1 done", flush=True)

    # --- roundtrip_coll: baseline inserts with initial SET values ---
    # pk=1 ck='a': initial set with 'keep_me' and 'remove_me'; element-removal in Gen 2
    session.execute(
        "INSERT INTO roundtrip_coll (pk, ck, tags) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
        (1, 'a', {'keep_me', 'remove_me', 'also_keep'})
    )
    # pk=2 ck='a': will get overwrite (replaced=true) in Gen 2
    session.execute(
        "INSERT INTO roundtrip_coll (pk, ck, tags) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
        (2, 'a', {'old_a', 'old_b'})
    )
    # pk=3 ck='a': will get append (replaced=false) in Gen 2
    session.execute(
        "INSERT INTO roundtrip_coll (pk, ck, tags) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
        (3, 'a', {'initial'})
    )
    # pk=4: stable, no mutations across generations (baseline control)
    for ck in ['a', 'b']:
        session.execute(
            "INSERT INTO roundtrip_coll (pk, ck, tags) VALUES (%s, %s, %s) USING TIMESTAMP 1000",
            (4, ck, {f'stable_{ck}'})
        )
    print("[roundtrip_coll] Phase 1 done", flush=True)

    cluster.shutdown()
except SystemExit:
    raise
except Exception:
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Phase 2: deletes, partial updates, partition tombstone, stale-cell proof
#
# This phase proves:
# (a) Resurrection prevention: DELETE pk=10 ck='del_me' at ts=2000
#     Without proper merge, Gen 1's INSERT would resurrect this row.
#     With LWW + row_delete: Gen 1 insert (ts=1000) is suppressed by
#     Gen 2 row_delete (del_ts=2000 >= 1000). The row stays gone.
#
# (b) Stale-cell prevention: UPDATE pk=20 ck='stale' SET val='new_val' at ts=2000
#     Without LWW merge, Gen 1's 'old_val' (ts=1000) and Gen 2's 'new_val'
#     (ts=2000) would both be candidates. With proper LWW: new_val wins.
#
# (c) Partition tombstone: DELETE FROM roundtrip_t WHERE pk=30 at ts=2000
#     Suppresses all Gen 1 writes for pk=30 (ts=1000 <= 2000).
#     Gen 3 will add a post-delete insert for pk=30 ck='z' at ts=3000
#     to prove that later writes survive the partition tombstone.
# ---------------------------------------------------------------------------
run_phase2() {
  log "=== Phase 2: deletes, partial updates, partition tombstone ==="
  run $ENGINE exec -i "$CONTAINER_NAME" python3 - <<'PYEOF'
import sys, traceback, time
from cassandra.cluster import Cluster

def connect_with_retry(keyspace, attempts=12, delay=6):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect(keyspace)
            return cluster, session
        except Exception as exc:
            last_exc = exc
            time.sleep(delay)
    raise RuntimeError(f"Could not connect after {attempts} attempts: {last_exc}")

try:
    cluster, session = connect_with_retry('roundtrip_ks')

    # (a) Resurrection scenario: row_delete at ts=2000 suppresses gen-1 insert (ts=1000)
    # A naive union would include both Gen 1 and Gen 2 records, resurrecting the deleted row.
    # The proper merge suppresses it because row_delete.del_ts=2000 >= upsert.writetime=1000.
    session.execute(
        "DELETE FROM roundtrip_t USING TIMESTAMP 2000 WHERE pk=%s AND ck=%s",
        (10, 'del_me')
    )
    print("[proof-a] row_delete pk=10 ck='del_me' at ts=2000", flush=True)

    # (b) Stale-cell scenario: UPDATE val='new_val' at ts=2000 (Gen 1 had ts=1000)
    # Without LWW, both values are present across generations.
    # With LWW (higher writetime wins): new_val survives.
    session.execute(
        "UPDATE roundtrip_t USING TIMESTAMP 2000 SET val=%s WHERE pk=%s AND ck=%s",
        ('new_val', 20, 'stale')
    )
    print("[proof-b] UPDATE pk=20 ck='stale' val='new_val' at ts=2000", flush=True)

    # Additional row deletes in pk=1..5 to mix with later Gen 3 inserts
    # Delete ck='b' from pk=2 — row tombstone
    session.execute(
        "DELETE FROM roundtrip_t USING TIMESTAMP 2000 WHERE pk=%s AND ck=%s",
        (2, 'b')
    )

    # Range delete: DELETE WHERE pk=3 AND ck >= 'a' AND ck < 'c' at ts=2000
    # This covers ck='a' and ck='b', leaving ck='c' alive.
    session.execute(
        "DELETE FROM roundtrip_t USING TIMESTAMP 2000 WHERE pk=3 AND ck >= 'a' AND ck < 'c'"
    )

    # (c) Partition tombstone: suppresses all gen-1 writes for pk=30
    session.execute(
        "DELETE FROM roundtrip_t USING TIMESTAMP 2000 WHERE pk=%s",
        (30,)
    )
    print("[proof-c] partition_delete pk=30 at ts=2000", flush=True)

    # Update static column for pk=1..5 at ts=2000 (overrides gen-1 static at ts=900)
    for pk in range(1, 6):
        session.execute(
            "UPDATE roundtrip_t USING TIMESTAMP 2000 SET st=%s WHERE pk=%s",
            (f"static_gen2_{pk}", pk)
        )

    # Cell tombstone: DELETE val FROM roundtrip_t WHERE pk=5 AND ck='a'
    # After this, the merged view should show val=NULL for pk=5 ck='a'
    session.execute(
        "DELETE val FROM roundtrip_t USING TIMESTAMP 2000 WHERE pk=%s AND ck=%s",
        (5, 'a')
    )

    print("[roundtrip_t] Phase 2 done", flush=True)

    # --- roundtrip_coll Phase 2 ---
    # pk=1 ck='a': element removal — produces v1 warning counter > 0
    session.execute(
        "UPDATE roundtrip_coll USING TIMESTAMP 2000 SET tags = tags - %s WHERE pk=1 AND ck='a'",
        ({'remove_me'},)
    )
    print("[roundtrip_coll] element-removal pk=1 ck='a' (v1 warning)", flush=True)

    # pk=2 ck='a': overwrite (replaced=true)
    session.execute(
        "UPDATE roundtrip_coll USING TIMESTAMP 2000 SET tags = %s WHERE pk=2 AND ck='a'",
        ({'only_this'},)
    )
    print("[roundtrip_coll] overwrite pk=2 ck='a'", flush=True)

    # pk=3 ck='a': append (replaced=false)
    session.execute(
        "UPDATE roundtrip_coll USING TIMESTAMP 2000 SET tags = tags + %s WHERE pk=3 AND ck='a'",
        ({'appended'},)
    )
    print("[roundtrip_coll] append pk=3 ck='a'", flush=True)

    print("[roundtrip_coll] Phase 2 done", flush=True)

    cluster.shutdown()
except SystemExit:
    raise
except Exception:
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Phase 3: post-delete inserts (prove resurrection is NOT a problem),
#           TTL writes, range-delete survivors
#
# This phase proves:
# (d) Resurrection is NOT happening for pk=10 ck='del_me':
#     We do NOT re-insert this row. The merged view must show it as absent.
#     (The proof is in the test assertion: merged view does NOT contain pk=10 ck='del_me'.)
#
# (e) Post-partition-delete insert: pk=30 ck='z' at ts=3000 > partition_delete ts=2000
#     This row SURVIVES because its writetime (3000) > partition_delete ts (2000).
#     With proper merge: this row appears in the merged view even though pk=30 had a
#     partition tombstone in Gen 2. A naive union would keep all rows including Gen 1's
#     pk=30 data, which should be suppressed by the partition tombstone.
# ---------------------------------------------------------------------------
run_phase3() {
  log "=== Phase 3: post-delete inserts, TTL writes, survivors ==="
  run $ENGINE exec -i "$CONTAINER_NAME" python3 - <<'PYEOF'
import sys, traceback, time
from cassandra.cluster import Cluster

def connect_with_retry(keyspace, attempts=12, delay=6):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect(keyspace)
            return cluster, session
        except Exception as exc:
            last_exc = exc
            time.sleep(delay)
    raise RuntimeError(f"Could not connect after {attempts} attempts: {last_exc}")

try:
    cluster, session = connect_with_retry('roundtrip_ks')

    # (e) Post-partition-delete insert: pk=30 ck='z' at ts=3000 survives
    # because writetime=3000 > partition_delete.del_ts=2000.
    # The Gen 1 rows for pk=30 (ts=1000) remain suppressed.
    session.execute(
        "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 3000",
        (30, 'z', 'post_partition_delete_survivor')
    )
    print("[proof-e] INSERT pk=30 ck='z' at ts=3000 (post-partition-delete)", flush=True)

    # New inserts for pk=1..5, ck='d' (new rows not present in gen-1 or gen-2)
    for pk in range(1, 6):
        session.execute(
            "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 3000",
            (pk, 'd', f"gen3_pk{pk}_d")
        )

    # Update pk=20 again at ts=3000 — proves LWW chain across 3 generations
    session.execute(
        "UPDATE roundtrip_t USING TIMESTAMP 3000 SET val=%s WHERE pk=%s AND ck=%s",
        ('newest_val', 20, 'stale')
    )
    print("[stale-chain] UPDATE pk=20 ck='stale' val='newest_val' at ts=3000", flush=True)

    # TTL writes: these have a live expires_at in their cells
    session.execute(
        "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 3000 AND TTL 86400",
        (50, 'ttl_row', 'ttl_value')
    )

    # Range-delete survivor: Gen 2 deleted pk=3 ck=['a','c') at ts=2000.
    # Insert pk=3 ck='b' at ts=3000 to prove it survives the range_delete.
    # ck='b' is in the deleted range [a,c), but this new insert at ts=3000 > 2000.
    session.execute(
        "INSERT INTO roundtrip_t (pk, ck, val) VALUES (%s, %s, %s) USING TIMESTAMP 3000",
        (3, 'b', 'range_delete_survivor')
    )
    print("[range-delete-survivor] INSERT pk=3 ck='b' at ts=3000 > range_delete ts=2000", flush=True)

    # Update static column for pk=30 at ts=3000 (post-partition-delete, survives)
    session.execute(
        "UPDATE roundtrip_t USING TIMESTAMP 3000 SET st=%s WHERE pk=%s",
        ("static_pk30_gen3", 30)
    )

    print("[roundtrip_t] Phase 3 done", flush=True)

    # --- roundtrip_coll Phase 3 ---
    # pk=1 ck='a': append after element-removal in gen 2
    session.execute(
        "UPDATE roundtrip_coll USING TIMESTAMP 3000 SET tags = tags + %s WHERE pk=1 AND ck='a'",
        ({'gen3_addition'},)
    )
    # pk=5: brand new rows in gen 3
    for ck in ['new1', 'new2']:
        session.execute(
            "INSERT INTO roundtrip_coll (pk, ck, tags) VALUES (%s, %s, %s) USING TIMESTAMP 3000",
            (5, ck, {f'tag_{ck}'})
        )

    print("[roundtrip_coll] Phase 3 done", flush=True)

    cluster.shutdown()
except SystemExit:
    raise
except Exception:
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Flush SSTable generation (nodetool flush).
#
# Each call to nodetool flush writes the current memtable contents to a new
# set of SSTable files (nb-N-big-*) within the table's UUID directory.
# All three generations for a table share ONE UUID directory — Cassandra does
# not create a new UUID directory per flush.
# ---------------------------------------------------------------------------
flush_generation() {
  local gen="$1"
  log "Flushing roundtrip_ks (generation $gen)..."
  run $ENGINE exec "$CONTAINER_NAME" nodetool flush "roundtrip_ks"
  log "Generation $gen flush complete."
}

# ---------------------------------------------------------------------------
# Export SSTables from container to host
# ---------------------------------------------------------------------------
export_sstables_to_host() {
  local dest_dir="$1"
  log "Exporting roundtrip_ks SSTables from container to $dest_dir..."
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would export SSTables to $dest_dir"
    return 0
  fi

  # Create the keyspace subdirectory so CQLite's two-level discovery
  # (data-dir / keyspace / table-UUID) works correctly.
  local ks_dir="$dest_dir/roundtrip_ks"
  mkdir -p "$ks_dir"

  TMPDIR_EXPORT="$(mktemp -d)"
  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    if [[ -d "$TMPDIR_EXPORT/data/roundtrip_ks" ]]; then
      # Copy the table-UUID directories into dest_dir/roundtrip_ks/
      cp -r "$TMPDIR_EXPORT/data/roundtrip_ks/." "$ks_dir/"
      log "SSTables placed in $ks_dir (keyspace subdirectory for CQLite discovery)"
    else
      fail "Expected $TMPDIR_EXPORT/data/roundtrip_ks but not found."
    fi
    rm -rf "$TMPDIR_EXPORT"
  else
    rm -rf "$TMPDIR_EXPORT"
    fail "tar export from container failed."
  fi

  # Remove macOS AppleDouble files
  find "$dest_dir" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Apply CQL schema
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
# Write bare-CQL schema files for delta-export (no keyspace/use preamble)
# ---------------------------------------------------------------------------
write_delta_export_schemas() {
  local schemas_dir="$1"
  mkdir -p "$schemas_dir"

  cat >"$schemas_dir/roundtrip_t.cql" <<'EOF'
CREATE TABLE roundtrip_ks.roundtrip_t (
    pk  INT,
    ck  TEXT,
    val TEXT,
    st  TEXT STATIC,
    PRIMARY KEY (pk, ck)
);
EOF

  cat >"$schemas_dir/roundtrip_coll.cql" <<'EOF'
CREATE TABLE roundtrip_ks.roundtrip_coll (
    pk   INT,
    ck   TEXT,
    tags SET<TEXT>,
    PRIMARY KEY (pk, ck)
);
EOF

  # Full schema (keyspace + tables) for CQLite SELECT * ground truth
  cat >"$schemas_dir/roundtrip_full.cql" <<'EOF'
CREATE KEYSPACE IF NOT EXISTS roundtrip_ks WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE roundtrip_ks;

CREATE TABLE IF NOT EXISTS roundtrip_t (
    pk  INT,
    ck  TEXT,
    val TEXT,
    st  TEXT STATIC,
    PRIMARY KEY (pk, ck)
);

CREATE TABLE IF NOT EXISTS roundtrip_coll (
    pk   INT,
    ck   TEXT,
    tags SET<TEXT>,
    PRIMARY KEY (pk, ck)
);
EOF
  log "Delta-export schema files written to $schemas_dir"
}

# ---------------------------------------------------------------------------
# Run delta-export for each SSTable generation
# ---------------------------------------------------------------------------
run_delta_exports() {
  local sstables_dir="$1"
  local parquet_dir="$2"
  local schemas_dir="$3"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would run delta-export for all generations"
    return 0
  fi

  # Always build CQLite with delta-export feature to guarantee the feature is active.
  # A pre-built binary at target/debug/cqlite may have been built without the feature;
  # we cannot distinguish from help output alone (help works even without the feature).
  log "Building CQLite with delta-export feature (required for this script)..."
  (cd "$REPO_ROOT" && cargo build --package cqlite-cli --features delta-export --quiet)
  local CQLITE_BIN="$REPO_ROOT/target/debug/cqlite"

  # Verify the feature is actually available by running a real delta-export probe.
  # (--help exits 0 even without the feature; we need to catch "Delta-export is not enabled".)
  local probe_tmpdir
  probe_tmpdir="$(mktemp -d)"
  local probe_result
  probe_result=$("$CQLITE_BIN" delta-export "$probe_tmpdir" \
    --schema /dev/null \
    --out parquet \
    -o "$probe_tmpdir/probe.parquet" 2>&1 || true)
  rm -rf "$probe_tmpdir"
  if echo "$probe_result" | grep -q "not enabled in this build"; then
    fail "CQLite binary at $CQLITE_BIN was not built with --features delta-export. \
Build failed or cargo picked up a stale binary. Check the build output above."
  fi

  log "Using CQLite binary: $CQLITE_BIN (delta-export feature confirmed)"

  # Accumulate element_tombstone_warnings across all tables and generations.
  local total_element_tombstone_warnings=0

  for table in roundtrip_t roundtrip_coll; do
    local schema_file="$schemas_dir/${table}.cql"
    local out_dir="$parquet_dir/$table"
    local staging_dir="$parquet_dir/.staging/$table"
    mkdir -p "$out_dir" "$staging_dir"

    # Cassandra stores all flushes for a table in a single UUID directory.
    # Each flush produces an nb-N-big-* file set (nb-1, nb-2, nb-3 = 3 flushes).
    # We need to export each nb-N generation separately, so we create a temporary
    # staging directory containing only the files for that generation, then
    # pass that staging directory to delta-export.
    local gen_num=1
    # Find all table directories for this table, then within each find all nb-N prefixes
    while IFS= read -r -d '' table_dir; do
      # Find unique nb-N prefixes within this table directory
      while IFS= read -r -d '' data_file; do
        # Extract the nb-N-big prefix (e.g. "nb-1-big" from "nb-1-big-Data.db")
        local data_basename
        data_basename="$(basename "$data_file")"
        local nb_prefix
        nb_prefix="${data_basename%-Data.db}"

        # Create a staging directory for this generation
        local gen_staging_dir="$staging_dir/gen${gen_num}_${nb_prefix}"
        mkdir -p "$gen_staging_dir"

        # Copy all files with this nb-N prefix (Data.db, CompressionInfo.db, etc.)
        find "$table_dir" -maxdepth 1 -name "${nb_prefix}-*" -not -name '._*' \
          -exec cp {} "$gen_staging_dir/" \;

        local out_file="$out_dir/gen${gen_num}.parquet"
        local gen_name
        gen_name="${table}_${nb_prefix}"
        log "  delta-export [$table gen$gen_num] $gen_name → $out_file"

        # Capture stderr separately so we can extract the element-tombstone count.
        # Use an if/else so that set -e does not exit the script before we can
        # capture the exit code and display diagnostics.
        local stderr_file
        stderr_file="$(mktemp)"
        local exit_code
        if "$CQLITE_BIN" delta-export "$gen_staging_dir" \
          --schema "$schema_file" \
          --out parquet \
          -o "$out_file" \
          --overwrite \
          --source "$gen_name" \
          2>"$stderr_file"; then
          exit_code=0
        else
          exit_code=$?
        fi

        # Show stderr output and extract element-tombstone warnings.
        # Strategy: first try the stable machine-readable key
        #   cqlite.delta.element_tombstones=<n>
        # If that key is not present in stderr, fall back to the human-readable
        # warning phrase. Count from only ONE source per export run.
        if [[ -s "$stderr_file" ]]; then
          while IFS= read -r line; do
            echo "  [delta-export $table gen$gen_num stderr] $line"
          done < "$stderr_file"
          # Count element-tombstone warnings from this export run.
          # Primary: stable machine-readable key on stderr.
          # Fallback: human-readable warning phrase.
          # We sum only ONE source (primary preferred) to avoid double-counting.
          # We use `grep -c` / `awk` and redirect grep's "no match" exit code
          # with `|| true` so set -e + pipefail does not abort the script.
          local key_count fallback_count
          key_count=0
          fallback_count=0
          # Extract "cqlite.delta.element_tombstones=N" → sum N values
          if grep -q 'cqlite\.delta\.element_tombstones=' "$stderr_file" 2>/dev/null; then
            key_count=$(grep -oE 'cqlite\.delta\.element_tombstones=[0-9]+' "$stderr_file" 2>/dev/null \
              | grep -oE '[0-9]+$' | awk '{s+=$1} END {print s+0}')
          fi
          if [[ "${key_count:-0}" -gt 0 ]]; then
            total_element_tombstone_warnings=$((total_element_tombstone_warnings + key_count))
          else
            # Fallback: "N collection element tombstone(s) detected"
            if grep -q 'collection element tombstone' "$stderr_file" 2>/dev/null; then
              fallback_count=$(grep -oE '[0-9]+ collection element tombstone' "$stderr_file" 2>/dev/null \
                | grep -oE '^[0-9]+' | awk '{s+=$1} END {print s+0}')
            fi
            total_element_tombstone_warnings=$((total_element_tombstone_warnings + ${fallback_count:-0}))
          fi
        fi
        rm -f "$stderr_file"

        if [[ $exit_code -ne 0 ]]; then
          fail "delta-export failed for $table gen$gen_num (exit $exit_code)"
        fi
        gen_num=$((gen_num + 1))
      done < <(find "$table_dir" -maxdepth 1 -name "*-Data.db" -not -name '._*' -print0 | sort -z)
    done < <(find "$sstables_dir" -maxdepth 1 -type d -name "${table}-*" -print0 | sort -z)

    local exported_count
    exported_count=$(find "$out_dir" -name "*.parquet" | wc -l | tr -d ' ')
    log "  $table: $exported_count generation(s) exported to Parquet"
    if [[ "$exported_count" -lt 3 ]]; then
      log "  WARNING: $table has fewer than 3 Parquet generations ($exported_count). Expected 3 from 3 flushes."
    fi
  done

  # Write element_tombstone_warnings.txt so the Rust test can assert > 0.
  echo "$total_element_tombstone_warnings" > "$parquet_dir/../element_tombstone_warnings.txt"
  log "Element tombstone warnings total: $total_element_tombstone_warnings"
  log "  (written to $parquet_dir/../element_tombstone_warnings.txt)"
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

log "Starting delta round-trip generation"
log "Output directory: $OUT_DIR"

SSTABLES_DIR="$OUT_DIR/sstables"
PARQUET_DIR="$OUT_DIR/parquet"
SCHEMAS_DIR="$OUT_DIR/schemas"

# ---------------------------------------------------------------------------
# Write schema files (always written, even in dry-run for inspection)
# ---------------------------------------------------------------------------
write_delta_export_schemas "$SCHEMAS_DIR"

# ---------------------------------------------------------------------------
# Start Cassandra container
# ---------------------------------------------------------------------------
log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-roundtrip \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# Install Python driver
log "Installing python3-pip in container..."
run $ENGINE exec "$CONTAINER_NAME" bash -c "apt-get update -qq && apt-get install -y -q python3-pip"
log "Installing cassandra-driver in container..."
run $ENGINE exec "$CONTAINER_NAME" pip3 install --quiet cassandra-driver

# Write and apply the Cassandra schema
CASSANDRA_SCHEMA_FILE="$(mktemp /tmp/roundtrip_cassandra_schema.XXXXXX.cql)"
cat >"$CASSANDRA_SCHEMA_FILE" <<'EOF'
CREATE KEYSPACE IF NOT EXISTS roundtrip_ks WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE roundtrip_ks;

CREATE TABLE IF NOT EXISTS roundtrip_t (
    pk  INT,
    ck  TEXT,
    val TEXT,
    st  TEXT STATIC,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS roundtrip_coll (
    pk   INT,
    ck   TEXT,
    tags SET<TEXT>,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
EOF
apply_schema "$CASSANDRA_SCHEMA_FILE"
rm -f "$CASSANDRA_SCHEMA_FILE"

# ---------------------------------------------------------------------------
# Phase 1 → flush → Phase 2 → flush → Phase 3 → flush
# Each flush produces a separate SSTable generation (distinct directory)
# ---------------------------------------------------------------------------
run_phase1
flush_generation 1

run_phase2
flush_generation 2

run_phase3
flush_generation 3

# ---------------------------------------------------------------------------
# Capture Cassandra ground truth (the canonical merged view) BEFORE cleanup.
#
# After all three phases and flushes, Cassandra holds the authoritative merged
# state: tombstones are applied, LWW is resolved, range-deletes are respected.
# We export this as JSON so the Rust test can use it as the reference answer
# instead of CQLite SELECT * (which may not apply cross-generation tombstones).
# ---------------------------------------------------------------------------
capture_cassandra_ground_truth() {
  local dest_dir="$1"
  mkdir -p "$dest_dir"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would capture Cassandra ground truth to $dest_dir"
    return 0
  fi
  log "Capturing Cassandra ground truth for roundtrip_t..."
  $ENGINE exec -i "$CONTAINER_NAME" python3 - <<PYEOF > "$dest_dir/roundtrip_t.json"
import json, sys
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('roundtrip_ks')
    rows = list(session.execute("SELECT pk, ck, val, st FROM roundtrip_t"))
    result = []
    for r in rows:
        result.append({
            "pk": r.pk,
            "ck": r.ck,
            "val": r.val,
            "st": r.st,
        })
    # Sort by (pk, ck) for deterministic ordering
    result.sort(key=lambda x: (x["pk"], x["ck"] or ""))
    print(json.dumps(result, indent=2))
    cluster.shutdown()
except Exception as e:
    print(json.dumps({"error": str(e)}), file=sys.stderr)
    sys.exit(1)
PYEOF
  log "Capturing Cassandra ground truth for roundtrip_coll..."
  $ENGINE exec -i "$CONTAINER_NAME" python3 - <<PYEOF > "$dest_dir/roundtrip_coll.json"
import json, sys
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('roundtrip_ks')
    rows = list(session.execute("SELECT pk, ck, tags FROM roundtrip_coll"))
    result = []
    for r in rows:
        result.append({
            "pk": r.pk,
            "ck": r.ck,
            "tags": sorted(list(r.tags)) if r.tags else None,
        })
    result.sort(key=lambda x: (x["pk"], x["ck"] or ""))
    print(json.dumps(result, indent=2))
    cluster.shutdown()
except Exception as e:
    print(json.dumps({"error": str(e)}), file=sys.stderr)
    sys.exit(1)
PYEOF
  log "Cassandra ground truth written to $dest_dir"
}

GROUND_TRUTH_DIR="$OUT_DIR/ground_truth"
capture_cassandra_ground_truth "$GROUND_TRUTH_DIR"

# ---------------------------------------------------------------------------
# Export all SSTables to host
# ---------------------------------------------------------------------------
mkdir -p "$SSTABLES_DIR"
export_sstables_to_host "$SSTABLES_DIR"

# ---------------------------------------------------------------------------
# Verify: count Data.db files
# ---------------------------------------------------------------------------
# The keyspace subdirectory for CQLite discovery:
KS_SSTABLES_DIR="$SSTABLES_DIR/roundtrip_ks"

if [[ "$DRY_RUN" -eq 0 ]]; then
  DATA_COUNT=$(find "$SSTABLES_DIR" -name "*-Data.db" -not -name "._*" | wc -l | tr -d ' ')
  log "Found $DATA_COUNT Data.db file(s) in $SSTABLES_DIR"
  if [[ "$DATA_COUNT" -lt 6 ]]; then
    fail "Expected at least 6 Data.db files (3 gens x 2 tables), got $DATA_COUNT"
  fi

  # Count generations per table (within roundtrip_ks subdirectory)
  for table in roundtrip_t roundtrip_coll; do
    GEN_COUNT=$(find "$KS_SSTABLES_DIR" -maxdepth 2 -name "*-Data.db" -path "*/${table}-*/*" | wc -l | tr -d ' ')
    log "  $table: $GEN_COUNT generation(s)"
    if [[ "$GEN_COUNT" -lt 3 ]]; then
      log "  WARNING: $table has fewer than 3 generations ($GEN_COUNT). Check that all 3 flushes produced SSTables."
    fi
  done
fi

# ---------------------------------------------------------------------------
# Run delta-export for each generation
# The delta-export CLI processes per-generation SSTable directories.
# We pass the roundtrip_ks/ subdirectory so the find within run_delta_exports
# discovers the table-UUID directories correctly.
# ---------------------------------------------------------------------------
run_delta_exports "$KS_SSTABLES_DIR" "$PARQUET_DIR" "$SCHEMAS_DIR"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log "=== Delta round-trip generation COMPLETE ==="
log "SSTables: $SSTABLES_DIR"
log "Parquet:  $PARQUET_DIR"
log "Schemas:  $SCHEMAS_DIR"
log ""
log "To run the round-trip test:"
log "  export DELTA_ROUNDTRIP_DATA=$OUT_DIR"
log "  cargo test --package cqlite-cli --features delta-export --test delta_roundtrip_tests -- --nocapture"
log ""
log "Set DELTA_ROUNDTRIP_DATA=$OUT_DIR before running the test suite."

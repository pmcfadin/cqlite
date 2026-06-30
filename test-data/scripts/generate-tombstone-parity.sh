#!/usr/bin/env bash
# generate-tombstone-parity.sh — Tombstone / TTL parity SSTable fixtures (epic #972)
#
# Creates a NEW keyspace `test_tomb` (isolated from test_deltas) holding nine
# tables that each exercise a tombstone / TTL / dropped-column / static / range
# edge case the CQLite parity epic must read byte-for-byte the same as Cassandra.
#
# Container lifecycle, flag parsing (--out/--dry-run), logging helpers, and the
# export + sstabledump + sstablemetadata steps mirror generate-deltas.sh exactly
# so the committed reference files (`*-Data.db.jsonl`, `*-Statistics.db.txt`,
# `TOC.txt`, `Digest.crc32`) are consistent across the corpus.  Multi-generation
# tables follow the write+flush+write+flush pattern of generate-delta-roundtrip.sh
# (producing nb-1-big, nb-2-big, ... in ONE table-UUID directory).
#
# =====================================================================
# FIXED TIMESTAMP SCHEME (also documented in schemas/tombstone-parity.cql)
# =====================================================================
#   BASE_EPOCH_SECONDS = 1609459200          (2021-01-01T00:00:00Z)
#   BASE_TS_MICROS     = 1609459200000000
#
#   Per-generation write timestamps (micros), strictly increasing so newer
#   deletes always shadow older live data:
#     T_GEN1 = 1609459200000000  (2021-01-01T00:00:00Z)  base
#     T_GEN2 = 1609545600000000  (2021-01-02T00:00:00Z)  base + 1 day
#     T_GEN3 = 1609632000000000  (2021-01-03T00:00:00Z)  base + 2 days
#   DELETE statements use these same constants explicitly (DELETE ... USING
#   TIMESTAMP ...) so shadowing / resurrection is provable purely from writetimes.
#
#   gc_before TTL boundary (table gc_before_boundary, issue #1011)
#   --------------------------------------------------------------
#   IMPORTANT — localDeletionTime is NOT derived from USING TIMESTAMP.
#   Cassandra computes a TTL'd cell's localDeletionTime (expiration) from the
#   COORDINATOR WALL CLOCK at write time: localDeletionTime = nowInSeconds + TTL,
#   where nowInSeconds is the server's real clock when the mutation is applied.
#   USING TIMESTAMP only sets the cell's writetime; it does NOT control the
#   expiration / localDeletionTime.  Consequently the committed golden shows
#   ~2026 localDeletionTime values, and those absolute values CHANGE every time
#   the fixtures are regenerated.  There is no fixed 2021 boundary.
#
#   The ONLY deterministic facts for this table are:
#       * the two TTL values themselves: 86400 and 86401, and
#       * the resulting 1-second relative delta between the two cells'
#         localDeletionTime (LDT_86401 == LDT_86400 + 1), since both cells are
#         written within the same coordinator second.
#   The committed JSONL golden captures whatever wall-clock localDeletionTime
#   Cassandra emitted at generation time; parity tests compare against that
#   committed golden and NEVER against a hardcoded epoch.
#
# =====================================================================
# TABLE -> ISSUE / MANIFEST-ID -> GENERATIONS
# =====================================================================
#   1. gc_before_boundary       #1011  TTL localDeletionTime boundary        1 gen
#   2. tombstone_histogram      #1011  non-empty drop-time histogram (gc=0)  1 gen
#   3. skipped_partition_delete #1012  skipped reads / tombstone-only part.  2 gens
#   4. resurrection_gc0         #1014  gc_grace=0, deletes shadow older      2 gens
#   5. resurrection_gc_positive #1014  gc_grace=864000, tombstones retained  2 gens
#   6. dropped_regular_col      #1015  per-cell dropped regular column       2 gens
#   7. dropped_static_col       #1015  dropped STATIC column                 2 gens
#   8. static_with_tombstones   #1015  static survives adjacent tombstones   1 gen
#   9. wide_range_tombstone     #1013  range tombstone at index-block edges  1 gen
#
# Usage:
#   bash test-data/scripts/generate-tombstone-parity.sh [--out <dir>] [--dry-run]
#
# Options:
#   --out <dir>   Output directory (default: test-data/datasets)
#   --dry-run     Print commands without executing
#
# Prerequisites:
#   - Docker (or podman) available in PATH
#   - ~4 GB RAM available for the Cassandra container
#
# Backs: epic #972 (issues #1011, #1012, #1013, #1014, #1015)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-tomb"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_tomb"

# Fixed-timestamp constants (see header).
T_GEN1=1609459200000000   # 2021-01-01T00:00:00Z
T_GEN2=1609545600000000   # 2021-01-02T00:00:00Z (base + 1 day)
T_GEN3=1609632000000000   # 2021-01-03T00:00:00Z (base + 2 days)
# NOTE: there is intentionally NO GC_BOUNDARY_LDT constant. A TTL cell's
# localDeletionTime is wall-clock-derived (nowInSeconds + TTL) at write time, so
# it cannot be pinned by USING TIMESTAMP. The only deterministic facts are the
# TTL values (86400 vs 86401) and their 1-second relative delta; the committed
# JSONL golden captures the actual wall-clock localDeletionTime values.

# ---------------------------------------------------------------------------
# Parse CLI flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[tomb] Unknown argument: $1" >&2; exit 1 ;;
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
log()  { echo "[tomb] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[tomb][ERROR] $*" >&2; exit 1; }

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
  if [[ "$DRY_RUN" -eq 1 ]]; then
    ENGINE="docker"   # placeholder so dry-run can still print commands
    echo "[tomb] (dry-run) no container engine found; using placeholder 'docker' for command preview"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

# ---------------------------------------------------------------------------
# Guard: ensure no leftover container
# ---------------------------------------------------------------------------
if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

# ---------------------------------------------------------------------------
# Cleanup trap
# ---------------------------------------------------------------------------
cleanup() {
  if [[ "$DRY_RUN" -eq 0 ]]; then
    log "Cleaning up container..."
    $ENGINE rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
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
# Helper: run an inline CQL statement via cqlsh (-e). Used for ALTER TABLE and
# any one-off statement between phases. Honors --dry-run.
# ---------------------------------------------------------------------------
cql() {
  local stmt="$1"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $ENGINE exec $CONTAINER_NAME cqlsh -k $KEYSPACE -e \"$stmt\""
  else
    $ENGINE exec "$CONTAINER_NAME" cqlsh -k "$KEYSPACE" -e "$stmt"
  fi
}

# ---------------------------------------------------------------------------
# Helper: flush the keyspace, producing a new SSTable generation per call.
# ---------------------------------------------------------------------------
flush_generation() {
  local label="$1"
  log "Flushing $KEYSPACE ($label)..."
  run $ENGINE exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"
  log "Flush ($label) complete."
}

# ---------------------------------------------------------------------------
# Phase: single-flush tables (gc_before_boundary, tombstone_histogram,
#        static_with_tombstones, wide_range_tombstone) + gen-1 of all
#        multi-generation tables.
#
# Every write/delete uses an explicit USING TIMESTAMP so writetime / ttl /
# localDeletionTime are deterministic. See header for the constants.
# ---------------------------------------------------------------------------
run_gen1() {
  log "=== Generation 1: live data + single-flush tombstone shapes ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would insert gen-1 rows via python3 cassandra-driver heredoc (USING TIMESTAMP $T_GEN1 / $T_GEN2)"
    echo "[dry-run]   gc_before_boundary: INSERT ... USING TIMESTAMP $T_GEN1 AND TTL 86400 (LDT = wall-clock nowInSeconds + 86400)"
    echo "[dry-run]   gc_before_boundary: INSERT ... USING TIMESTAMP $T_GEN1 AND TTL 86401 (LDT = wall-clock nowInSeconds + 86401, == prev LDT + 1s)"
    echo "[dry-run]   tombstone_histogram: INSERT live rows USING TIMESTAMP $T_GEN1; DELETE row+cell USING TIMESTAMP $T_GEN2 (gc_grace_seconds=0)"
    echo "[dry-run]   skipped_partition_delete: INSERT pk=1 ck 1..10 USING TIMESTAMP $T_GEN1"
    echo "[dry-run]   resurrection_gc0 / resurrection_gc_positive: INSERT pk=1 ck 1..5 and pk=2 ck 1..3 USING TIMESTAMP $T_GEN1"
    echo "[dry-run]   dropped_regular_col: INSERT keep_col+drop_col USING TIMESTAMP $T_GEN1 (Phase A)"
    echo "[dry-run]   dropped_static_col: INSERT stat_col + rows USING TIMESTAMP $T_GEN1"
    echo "[dry-run]   static_with_tombstones: INSERT stat_col + ck 1..6 USING TIMESTAMP $T_GEN1; row/cell/range deletes USING TIMESTAMP $T_GEN2"
    echo "[dry-run]   wide_range_tombstone: INSERT pk=1 ck 1..4000 (wide TEXT) USING TIMESTAMP $T_GEN1; range tombstones USING TIMESTAMP $T_GEN2"
    return 0
  fi
  $ENGINE exec -i "$CONTAINER_NAME" \
    env T_GEN1="$T_GEN1" T_GEN2="$T_GEN2" KEYSPACE="$KEYSPACE" \
    python3 - <<'PYEOF'
import os, sys, traceback, time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

T_GEN1 = int(os.environ["T_GEN1"])
T_GEN2 = int(os.environ["T_GEN2"])
KEYSPACE = os.environ["KEYSPACE"]

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
    cluster, session = connect_with_retry(KEYSPACE)

    # -------------------------------------------------------------------
    # 1. gc_before_boundary  (#1011)
    #    Two TTL'd cells sharing writetime T_GEN1; TTLs 86400/86401.
    #    localDeletionTime is wall-clock-derived (nowInSeconds + TTL), NOT
    #    controlled by USING TIMESTAMP. The deterministic fact is the 1-second
    #    relative delta between the two cells' localDeletionTime; absolute
    #    values are captured by the committed JSONL golden.
    # -------------------------------------------------------------------
    print("[1] gc_before_boundary", flush=True)
    # ck=1 (TTL=86400) and ck=2 (TTL=86401) are written in ONE batch so they
    # share a single coordinator nowInSeconds. localDeletionTime is wall-clock
    # derived (nowInSeconds + TTL), so issuing them as separate round-trips could
    # straddle a one-second boundary and yield a 2s delta; the batch guarantees
    # the deterministic invariant LDT_86401 == LDT_86400 + 1.
    session.execute(
        "BEGIN BATCH "
        f"INSERT INTO gc_before_boundary (pk, ck, val) VALUES (1, 1, 'at_boundary') "
        f"USING TIMESTAMP {T_GEN1} AND TTL 86400; "
        f"INSERT INTO gc_before_boundary (pk, ck, val) VALUES (1, 2, 'past_boundary') "
        f"USING TIMESTAMP {T_GEN1} AND TTL 86401; "
        "APPLY BATCH"
    )
    # A non-TTL control row (no expiration) for contrast.
    session.execute(
        f"INSERT INTO gc_before_boundary (pk, ck, val) VALUES (1, 3, 'no_ttl') "
        f"USING TIMESTAMP {T_GEN1}"
    )
    print("  gc_before_boundary: done", flush=True)

    # -------------------------------------------------------------------
    # 2. tombstone_histogram  (#1011)  gc_grace_seconds = 0 on the table.
    #    Live rows at T_GEN1, then row + cell tombstones at T_GEN2 so the
    #    "Estimated tombstone drop times" histogram is non-empty at flush.
    # -------------------------------------------------------------------
    print("[2] tombstone_histogram", flush=True)
    for pk in range(1, 4):
        for ck in range(1, 6):
            session.execute(
                f"INSERT INTO tombstone_histogram (pk, ck, col_a, col_b) "
                f"VALUES ({pk}, {ck}, 'a_{pk}_{ck}', 'b_{pk}_{ck}') USING TIMESTAMP {T_GEN1}"
            )
    # Row tombstones (whole clustering rows)
    session.execute(f"DELETE FROM tombstone_histogram USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=2")
    session.execute(f"DELETE FROM tombstone_histogram USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=4")
    session.execute(f"DELETE FROM tombstone_histogram USING TIMESTAMP {T_GEN2} WHERE pk=2 AND ck=3")
    # Cell tombstones (single column)
    session.execute(f"DELETE col_b FROM tombstone_histogram USING TIMESTAMP {T_GEN2} WHERE pk=2 AND ck=1")
    session.execute(f"DELETE col_b FROM tombstone_histogram USING TIMESTAMP {T_GEN2} WHERE pk=3 AND ck=5")
    print("  tombstone_histogram: done", flush=True)

    # -------------------------------------------------------------------
    # 3. skipped_partition_delete  (#1012)  gen-1 portion.
    #    Live rows in partition P (pk=1), clustering keys within bound 1..10.
    #    Partition Q (pk=2) gets NO live rows in any generation.
    # -------------------------------------------------------------------
    print("[3] skipped_partition_delete (gen-1)", flush=True)
    for ck in range(1, 11):
        session.execute(
            f"INSERT INTO skipped_partition_delete (pk, ck, val) "
            f"VALUES (1, {ck}, 'live_p_{ck}') USING TIMESTAMP {T_GEN1}"
        )
    print("  skipped_partition_delete (gen-1): done", flush=True)

    # -------------------------------------------------------------------
    # 4 & 5. resurrection_gc0 / resurrection_gc_positive  (#1014)  gen-1.
    #    Identical gen-1 live data for both tables.
    #    pk=1 ck 1..5 (will be partially row/cell deleted in gen-2)
    #    pk=2 ck 1..3 (will be partition deleted in gen-2)
    # -------------------------------------------------------------------
    print("[4/5] resurrection_* (gen-1)", flush=True)
    for tbl in ("resurrection_gc0", "resurrection_gc_positive"):
        for ck in range(1, 6):
            session.execute(
                f"INSERT INTO {tbl} (pk, ck, val, extra) "
                f"VALUES (1, {ck}, 'live_{ck}', 'extra_{ck}') USING TIMESTAMP {T_GEN1}"
            )
        for ck in range(1, 4):
            session.execute(
                f"INSERT INTO {tbl} (pk, ck, val, extra) "
                f"VALUES (2, {ck}, 'p2_live_{ck}', 'p2_extra_{ck}') USING TIMESTAMP {T_GEN1}"
            )
    print("  resurrection_* (gen-1): done", flush=True)

    # -------------------------------------------------------------------
    # 6. dropped_regular_col  (#1015)  Phase A (gen-1).
    #    INSERT BOTH keep_col and drop_col with the EARLY timestamp T_GEN1.
    #    The ALTER TABLE DROP drop_col happens between flushes (in the shell).
    # -------------------------------------------------------------------
    print("[6] dropped_regular_col (Phase A)", flush=True)
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO dropped_regular_col (pk, ck, keep_col, drop_col) "
            f"VALUES (1, {ck}, 'keep_a_{ck}', 'drop_a_{ck}') USING TIMESTAMP {T_GEN1}"
        )
    print("  dropped_regular_col (Phase A): done", flush=True)

    # -------------------------------------------------------------------
    # 7. dropped_static_col  (#1015)  gen-1.
    #    INSERT static value + regular rows at T_GEN1.
    #    ALTER TABLE DROP stat_col happens between flushes (in the shell).
    # -------------------------------------------------------------------
    print("[7] dropped_static_col (gen-1)", flush=True)
    session.execute(
        f"UPDATE dropped_static_col USING TIMESTAMP {T_GEN1} SET stat_col='static_g1' WHERE pk=1"
    )
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO dropped_static_col (pk, ck, row_col) "
            f"VALUES (1, {ck}, 'row_g1_{ck}') USING TIMESTAMP {T_GEN1}"
        )
    print("  dropped_static_col (gen-1): done", flush=True)

    # -------------------------------------------------------------------
    # 8. static_with_tombstones  (#1015)  single flush.
    #    Live static + clustering rows at T_GEN1, then row/cell/range
    #    tombstones at T_GEN2.  Static cell is never deleted.
    # -------------------------------------------------------------------
    print("[8] static_with_tombstones", flush=True)
    session.execute(
        f"UPDATE static_with_tombstones USING TIMESTAMP {T_GEN1} SET stat_col='surviving_static' WHERE pk=1"
    )
    for ck in range(1, 7):
        session.execute(
            f"INSERT INTO static_with_tombstones (pk, ck, row_col) "
            f"VALUES (1, {ck}, 'row_{ck}') USING TIMESTAMP {T_GEN1}"
        )
    # Row tombstone on ck=2
    session.execute(f"DELETE FROM static_with_tombstones USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=2")
    # Cell tombstone on ck=3 (row_col only)
    session.execute(f"DELETE row_col FROM static_with_tombstones USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=3")
    # Range tombstone covering ck in [4,5]
    session.execute(
        f"DELETE FROM static_with_tombstones USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck>=4 AND ck<=5"
    )
    print("  static_with_tombstones: done", flush=True)

    # -------------------------------------------------------------------
    # 9. wide_range_tombstone  (#1013)  nb BIG only — single flush.
    #    One wide partition (pk=1) with thousands of clustering rows carrying
    #    a wide TEXT value so the serialized partition spans MULTIPLE IndexInfo
    #    blocks (column_index_size_in_kb default = 64KB).  Range tombstones are
    #    placed to land at block edges; exact boundaries depend on serialized
    #    size, so we over-provision and DOCUMENT the intended edge per delete.
    #    The lead/tests inspect Index.db to confirm placement.
    #
    #    BTI (`da`) variant is DEFERRED — not supported in CASSANDRA_4 mode.
    # -------------------------------------------------------------------
    print("[9] wide_range_tombstone", flush=True)
    NUM_ROWS = 4000
    # ~200-byte payload per row; 4000 rows ~= 800KB+ of cell data, comfortably
    # exceeding the 64KB column index block size to force many IndexInfo blocks.
    payload = "x" * 200
    insert_stmt = session.prepare(
        "INSERT INTO wide_range_tombstone (pk, ck, val) VALUES (1, ?, ?) USING TIMESTAMP ?"
    )
    batch_log_every = 1000
    for ck in range(1, NUM_ROWS + 1):
        session.execute(insert_stmt, (ck, f"{payload}_{ck}", T_GEN1))
        if ck % batch_log_every == 0:
            print(f"    wide_range_tombstone: inserted {ck}/{NUM_ROWS}", flush=True)
    # Range tombstones designed to hit block edges (documented intent):
    #  (a) FIRST entry of a block:   DELETE ck in [1,1]   (very first clustering)
    session.execute(f"DELETE FROM wide_range_tombstone USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck>=1 AND ck<=1")
    #  (b) open-ended marker spanning a MIDDLE block:
    #      DELETE ck >= 1500 AND ck <= 2500  (intended to straddle a mid block edge)
    session.execute(f"DELETE FROM wide_range_tombstone USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck>=1500 AND ck<=2500")
    #  (c) LAST entry of a block / closed marker in the LAST block:
    #      DELETE ck >= 3990 AND ck <= 4000 (intended to land at the end of the
    #      final block).
    session.execute(f"DELETE FROM wide_range_tombstone USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck>=3990 AND ck<=4000")
    print("  wide_range_tombstone: done", flush=True)

    print("[OK] test_tomb: generation-1 writes complete", flush=True)
    cluster.shutdown()

except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during gen-1 insertion:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Phase: generation-2 writes for the multi-generation tables.
#   - skipped_partition_delete: partition tombstones (pk=1 and tombstone-only pk=2)
#   - resurrection_gc0 / resurrection_gc_positive: row/cell/partition deletes
#   - dropped_regular_col: Phase B inserts into keep_col (post-DROP)
#   - dropped_static_col: fresh row insert (post-DROP of stat_col)
# All deletes use explicit USING TIMESTAMP T_GEN2 (> T_GEN1).
# ---------------------------------------------------------------------------
run_gen2() {
  log "=== Generation 2: cross-generation tombstones / post-drop writes ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would run gen-2 writes via python3 cassandra-driver heredoc (USING TIMESTAMP $T_GEN2 / $T_GEN3)"
    echo "[dry-run]   skipped_partition_delete: DELETE WHERE pk=1 (partition tomb) + DELETE WHERE pk=2 (tombstone-only) USING TIMESTAMP $T_GEN2"
    echo "[dry-run]   resurrection_*: DELETE ck=2 (row), DELETE val (cell) ck=3, DELETE WHERE pk=2 (partition) USING TIMESTAMP $T_GEN2"
    echo "[dry-run]   dropped_regular_col: INSERT keep_col USING TIMESTAMP $T_GEN3 (Phase B, post-DROP)"
    echo "[dry-run]   dropped_static_col: INSERT row USING TIMESTAMP $T_GEN3 (post-DROP)"
    return 0
  fi
  $ENGINE exec -i "$CONTAINER_NAME" \
    env T_GEN2="$T_GEN2" T_GEN3="$T_GEN3" KEYSPACE="$KEYSPACE" \
    python3 - <<'PYEOF'
import os, sys, traceback, time
from cassandra.cluster import Cluster

T_GEN2 = int(os.environ["T_GEN2"])
T_GEN3 = int(os.environ["T_GEN3"])
KEYSPACE = os.environ["KEYSPACE"]

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
    cluster, session = connect_with_retry(KEYSPACE)

    # 3. skipped_partition_delete (#1012) gen-2:
    #    Partition tombstone for P (pk=1, had live rows) and a tombstone-only
    #    partition Q (pk=2, never had live rows).
    print("[3] skipped_partition_delete (gen-2)", flush=True)
    session.execute(f"DELETE FROM skipped_partition_delete USING TIMESTAMP {T_GEN2} WHERE pk=1")
    session.execute(f"DELETE FROM skipped_partition_delete USING TIMESTAMP {T_GEN2} WHERE pk=2")
    print("  skipped_partition_delete (gen-2): done", flush=True)

    # 4 & 5. resurrection_gc0 / resurrection_gc_positive (#1014) gen-2:
    #    Row delete (ck=2), cell delete (val on ck=3), partition delete (pk=2).
    print("[4/5] resurrection_* (gen-2)", flush=True)
    for tbl in ("resurrection_gc0", "resurrection_gc_positive"):
        # Row tombstone (shadows gen-1 row at ck=2)
        session.execute(f"DELETE FROM {tbl} USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=2")
        # Cell tombstone (val column on ck=3; extra stays live)
        session.execute(f"DELETE val FROM {tbl} USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=3")
        # Partition tombstone (shadows all gen-1 rows for pk=2)
        session.execute(f"DELETE FROM {tbl} USING TIMESTAMP {T_GEN2} WHERE pk=2")
    print("  resurrection_* (gen-2): done", flush=True)

    # 6. dropped_regular_col (#1015) Phase B (post-DROP):
    #    keep_col-only inserts with the LATER timestamp T_GEN3.
    print("[6] dropped_regular_col (Phase B)", flush=True)
    for ck in range(4, 7):
        session.execute(
            f"INSERT INTO dropped_regular_col (pk, ck, keep_col) "
            f"VALUES (1, {ck}, 'keep_b_{ck}') USING TIMESTAMP {T_GEN3}"
        )
    print("  dropped_regular_col (Phase B): done", flush=True)

    # 7. dropped_static_col (#1015) gen-2 (post-DROP of stat_col):
    #    A fresh regular row at T_GEN3.
    print("[7] dropped_static_col (gen-2)", flush=True)
    session.execute(
        f"INSERT INTO dropped_static_col (pk, ck, row_col) "
        f"VALUES (1, 4, 'row_g2_4') USING TIMESTAMP {T_GEN3}"
    )
    print("  dropped_static_col (gen-2): done", flush=True)

    print("[OK] test_tomb: generation-2 writes complete", flush=True)
    cluster.shutdown()

except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during gen-2 writes:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Generate sstabledump JSONL golden files (matches generate-deltas.sh).
# ---------------------------------------------------------------------------
generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files for $KEYSPACE..."
  while IFS= read -r -d '' data_file; do
    local rel
    # Path relative to the export root ($sstables_dir), mounted at /data in a
    # fresh --rm container. We dump the EXPORTED file, not the live container's
    # mutable data dir, so auto-compaction in the live container cannot delete
    # the freshly-flushed generation out from under us (issue #1151).
    rel="${data_file#"$sstables_dir"/}"
    local jsonl_file="${data_file%.db}.db.jsonl"
    log "  sstabledump: $rel"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "[dry-run] $ENGINE run --rm -v $sstables_dir:/data $CASSANDRA_IMAGE sstabledump /data/${rel} -l > $jsonl_file"
    else
      $ENGINE run --rm \
        -v "$sstables_dir:/data" \
        "$CASSANDRA_IMAGE" \
        bash -lc "/opt/cassandra/tools/bin/sstabledump /data/${rel} -l" \
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
  done < <(find "$sstables_dir/$KEYSPACE" -type f -name "*-Data.db" -not -name "._*" -print0 \
            2>/dev/null || true)
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

log "Starting $KEYSPACE generation (epic #972)"
log "Output directory: $OUT_DIR"
log "Fixed timestamps: T_GEN1=$T_GEN1 T_GEN2=$T_GEN2 T_GEN3=$T_GEN3"
log "gc_before_boundary TTLs: 86400 vs 86401 (localDeletionTime is wall-clock-derived; only the 1s relative delta is deterministic, golden captures absolute values)"

SSTABLES_DIR="$OUT_DIR/sstables"

# ---------------------------------------------------------------------------
# Start Cassandra container (nb/CASSANDRA_4 compat mode — default for 5.0.2)
# ---------------------------------------------------------------------------
log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-tomb \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# Install Python driver
log "Installing python3-pip in container..."
run $ENGINE exec "$CONTAINER_NAME" bash -c "apt-get update -qq && apt-get install -y -q python3-pip"
log "Installing cassandra-driver in container..."
run $ENGINE exec "$CONTAINER_NAME" pip3 install --quiet cassandra-driver

# Apply schema (creates keyspace + all nine tables)
apply_schema "$ROOT/schemas/tombstone-parity.cql"

# ---------------------------------------------------------------------------
# Generation 1: live data + single-flush tombstone shapes.
# ---------------------------------------------------------------------------
run_gen1
# Disable autocompaction so freshly-flushed generations cannot be compacted away
# between export and the per-table sstabledump loop (issue #1151).
log "Disabling autocompaction for $KEYSPACE..."
run $ENGINE exec "$CONTAINER_NAME" nodetool disableautocompaction "$KEYSPACE"
flush_generation "gen-1"

# ---------------------------------------------------------------------------
# Between generations: DROP columns so the post-drop generation does not
# reference them (issue #1015). These ALTERs run AFTER the gen-1 flush so the
# gen-1 SSTable's serialization header still declares the dropped columns.
# ---------------------------------------------------------------------------
log "Dropping columns between generations (issue #1015)..."
cql "ALTER TABLE dropped_regular_col DROP drop_col;"
cql "ALTER TABLE dropped_static_col DROP stat_col;"

# ---------------------------------------------------------------------------
# Generation 2: cross-generation tombstones + post-drop writes.
# ---------------------------------------------------------------------------
run_gen2
flush_generation "gen-2"

# ---------------------------------------------------------------------------
# Export SSTables to host (matches generate-deltas.sh tar-stream approach)
# ---------------------------------------------------------------------------
log "=== Exporting $KEYSPACE SSTables from container ==="

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would rm -rf $SSTABLES_DIR/$KEYSPACE (clear stale tables) before re-export"
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE from container into $SSTABLES_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.tomb_export_tmp"
  rm -rf "$TMPDIR_EXPORT"
  mkdir -p "$TMPDIR_EXPORT"

  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    if [[ -d "$TMPDIR_EXPORT/data/$KEYSPACE" ]]; then
      # Clear any prior export of this keyspace so reruns do not accumulate
      # stale table-UUID directories (which downstream JSONL / Statistics
      # generation would otherwise scan). Recreate fresh before copying.
      if [[ -d "$SSTABLES_DIR/$KEYSPACE" ]]; then
        log "Removing stale $SSTABLES_DIR/$KEYSPACE before re-export..."
      fi
      rm -rf "$SSTABLES_DIR/$KEYSPACE"
      mkdir -p "$SSTABLES_DIR/$KEYSPACE"
      cp -r "$TMPDIR_EXPORT/data/$KEYSPACE/." "$SSTABLES_DIR/$KEYSPACE/"
      log "$KEYSPACE SSTables placed in $SSTABLES_DIR/$KEYSPACE"
    else
      fail "Expected $TMPDIR_EXPORT/data/$KEYSPACE but it was not found. Export failed."
    fi
    rm -rf "$TMPDIR_EXPORT"
  else
    fail "tar export from container failed."
  fi

  # Verify at least one Data.db per table
  log "Verifying exported Data.db files..."
  local_count=$(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" | wc -l | tr -d ' ')
  if [[ "$local_count" -eq 0 ]]; then
    fail "No Data.db files found under $SSTABLES_DIR/$KEYSPACE — export is empty!"
  fi
  log "  Found $local_count Data.db file(s) in $KEYSPACE"

  # Generate JSONL golden files
  generate_sstabledump_jsonl "$SSTABLES_DIR"

  # Generate Statistics.db.txt reference files (matches nb corpus convention)
  log "Generating Statistics.db.txt for $KEYSPACE tables..."
  while IFS= read -r -d '' data_file; do
    rel="${data_file#"$SSTABLES_DIR"/}"
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
  done < <(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" -print0)

  # Remove macOS AppleDouble files if present
  find "$SSTABLES_DIR/$KEYSPACE" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

  log "=== $KEYSPACE generation COMPLETE ==="
  log "SSTables:   $SSTABLES_DIR/$KEYSPACE"
  log ""
  log "Expected generations per table:"
  log "  single-flush (1 gen): gc_before_boundary, tombstone_histogram, static_with_tombstones, wide_range_tombstone"
  log "  multi-flush (2 gens): skipped_partition_delete, resurrection_gc0, resurrection_gc_positive, dropped_regular_col, dropped_static_col"
  log ""
  log "Next steps:"
  log "  1. Run smoke test: bash test-data/scripts/smoke-test-all-tables.sh"
  log "  2. Verify JSONL goldens are non-empty (esp. tombstone-only partition Q in skipped_partition_delete)"
  log "  3. Confirm tombstone_histogram Statistics.db.txt has a non-empty drop-time histogram"
  log "  4. Inspect wide_range_tombstone Index.db for multi-block IndexInfo + range marker placement"
  log "  5. Package and publish: bash test-data/scripts/package_datasets.sh"
fi

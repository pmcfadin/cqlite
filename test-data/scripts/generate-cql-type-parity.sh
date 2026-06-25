#!/usr/bin/env bash
# generate-cql-type-parity.sh — CQL Type & Schema-Evolution parity SSTable fixtures (epic #971)
#
# Creates a NEW keyspace `test_types` (isolated from every other test corpus)
# holding twenty tables across four concern groups that the CQLite parity epic
# must read byte-for-byte the same as Cassandra:
#   Group A — Schema evolution / SerializationHeader (#1003)
#   Group B — Null vs empty & length boundaries        (#1006)
#   Group C — UDT / tuple / frozen / nested            (#1007)
#   Group D — Counters                                 (#1008)
#
# Container lifecycle, flag parsing (--out/--dry-run), logging helpers, and the
# export + sstabledump + sstablemetadata steps mirror generate-tombstone-parity.sh
# exactly so the committed reference files (`*-Data.db.jsonl`,
# `*-Statistics.db.txt`, `TOC.txt`) are consistent across the corpus.
# Multi-generation tables follow the write+flush+ALTER+write+flush pattern
# (producing nb-1-big, nb-2-big, ... in ONE table-UUID directory). ALTERs run
# AFTER the gen-1 flush so the gen-1 SerializationHeader still declares the
# original/dropped columns.
#
# =====================================================================
# FIXED TIMESTAMP SCHEME (also documented in schemas/cql-type-parity.cql)
# =====================================================================
#   BASE_EPOCH_SECONDS = 1609459200          (2021-01-01T00:00:00Z)
#   BASE_TS_MICROS     = 1609459200000000
#
#   Per-generation write timestamps (micros), strictly increasing so newer
#   writes/drops always shadow older live data:
#     T_GEN1 = 1609459200000000  (2021-01-01T00:00:00Z)  base
#     T_GEN2 = 1609545600000000  (2021-01-02T00:00:00Z)  base + 1 day
#     T_GEN3 = 1609632000000000  (2021-01-03T00:00:00Z)  base + 2 days
#   Every NON-COUNTER write/delete uses these constants explicitly
#   (... USING TIMESTAMP ...) so writetimes are deterministic.
#
#   COUNTERS: Cassandra REJECTS `USING TIMESTAMP` on counter UPDATEs, so the
#   counter tables (Group D) intentionally omit it; counter shard clocks are
#   coordinator-derived. The EXPECTED final per-pk value is what is asserted (and
#   teed to *.counter-select.txt), not a fixed writetime.
#
# =====================================================================
# ALTER TYPE SUBSTITUTION DECISIONS (Cassandra 5.0.2)
# =====================================================================
#   Apache Cassandra 5.0 effectively DISABLES `ALTER TABLE ... ALTER <col> TYPE`.
#   The statement throws "Altering of types is not allowed" (CASSANDRA-12443 /
#   5.0 hardening), so an on-disk SerializationHeader *type-map* divergence via
#   ALTER ... TYPE is not producible. SUBSTITUTION: every "altered column"
#   concern is modelled as an ADD-column / DROP-column header divergence between
#   generations, which DOES yield a gen-1 SerializationHeader whose declared
#   column set differs from the gen-2 schema — exactly the readable surface
#   #1003 needs. Per-table difference:
#     se_altered_column_type         : gen-2 ADDs `added_col bigint`
#                                      (gen-1 header lacks it).
#     se_altered_then_dropped_column : gen-2 ADDs `evolve_col text`; gen-3 DROPs
#                                      it (gen-1 lacks it, gen-3 records it dropped).
#
# =====================================================================
# TABLE -> ISSUE / MANIFEST-ID -> GENERATIONS
# =====================================================================
#  GROUP A — Schema evolution / SerializationHeader (#1003)
#   1. se_no_schema_change             #1003  control, no ALTER                 2 gens
#   2. se_altered_column_type          #1003  ADD col in gen-2 (ALTER-TYPE sub) 2 gens
#   3. se_dropped_column_same_type     #1003  DROP text col between flushes     2 gens
#   4. se_altered_then_dropped_column  #1003  ADD (g2) then DROP (g3) a col      3 gens
#   5. se_static_regular_kind_mismatch #1003  static col recorded in header     1 gen
#   6. se_frozen_multicell_collection_mismatch #1003 frozen vs multicell flag   1 gen
#  GROUP B — Null vs empty & length boundaries (#1006)
#   7. nb_null_empty_text_blob         #1006  absent/NULL/''/0x with neighbors  1 gen
#   8. nb_absent_vs_null_regular       #1006  absent vs deleted vs empty rows   1 gen
#   9. nb_empty_collections            #1006  empty multicell vs frozen empty   1 gen
#  10. nb_length_prefix_edges          #1006  len 0,1,127,128,...,16384 edges   1 gen
#  GROUP C — UDT / tuple / frozen / nested (#1007)
#  11. cx_tuple_field_order            #1007  tuple<int,text,boolean> + null    1 gen
#  12. cx_udt_field_order_null_empty   #1007  frozen UDT null/''/full           1 gen
#  13. cx_frozen_udt_value             #1007  frozen<udt> (nested) values       1 gen
#  14. cx_nested_frozen_collections    #1007  map<text,frozen<list>>, etc.      1 gen
#  15. cx_multicell_udt_collection_paths #1007 multicell UDT + multicell coll.  1 gen
#  16. cx_legacy_dropped_tuple_udt     #1007  DROP a tuple/UDT col between gens  2 gens
#  GROUP D — Counters (#1008) [nightly_docker]
#  17. ct_single_sstable              #1008  one counter, increments           1 gen
#  18. ct_multi_sstable_merge         #1008  inc gen-1, inc/dec gen-2           2 gens
#  19. ct_deleted_counter_shadowing   #1008  inc, flush, DELETE, flush          2 gens
#  20. ct_compacted_final_value       #1008  multi-gen + nodetool compact       2 gens + compacted
#
# Usage:
#   bash test-data/scripts/generate-cql-type-parity.sh [--out <dir>] [--dry-run]
#
# Options:
#   --out <dir>   Output directory (default: test-data/datasets)
#   --dry-run     Print commands without executing
#
# Prerequisites:
#   - Docker (or podman) available in PATH
#   - ~4 GB RAM available for the Cassandra container
#
# =====================================================================
# NON-DETERMINISTIC FIELDS (regeneration is NOT byte-identical — by design)
# =====================================================================
# A small set of emitted fields are Cassandra wall-clock / random and CANNOT be
# pinned even with the fixed-timestamp scheme above. This is INHERENT Cassandra
# behavior (it matches the accepted pattern from epic #972's tombstone parity
# generator) — it is NOT a bug in this script, and the generation logic must NOT
# be changed to try to force determinism on them. The non-deterministic fields:
#
#   * Multicell collection element CELL-PATH UUIDs — for a non-frozen (multicell)
#     list/UDT, Cassandra keys each element cell by a freshly generated
#     TimeUUID/random UUID at write time (e.g. the per-element `path` values in
#     the `ml` column of se_frozen_multicell_collection_mismatch and the
#     multicell columns of cx_multicell_udt_collection_paths). These differ on
#     every regeneration.
#   * Tombstone `local_delete_time` (deletion_info) — Cassandra stamps the
#     server WALL-CLOCK second at delete time. DROP-column / DELETE generations
#     therefore carry a regeneration-date `local_delete_time` (e.g. 2026-06-25)
#     in the golden, which changes every time the fixtures are regenerated.
#   * Counter shard clocks / host-ids (Group D) — coordinator-derived; only the
#     EXPECTED final per-pk counter value is deterministic and asserted (teed to
#     `*.counter-select.txt`), never the internal shard layout.
#
# CONSEQUENCE for parity testing:
#   * The COMMITTED `*-Data.db.jsonl` / `*-Statistics.db.txt` goldens are the
#     AUTHORITATIVE SNAPSHOT. The parity tests (issue_1003..1009) compare CQLite's
#     decode against the COMMITTED golden — they NEVER regenerate-and-byte-diff.
#   * Re-running this script produces a STRUCTURALLY equivalent corpus (same
#     tables, columns, types, kinds, row/cell shapes) but is intentionally NOT
#     byte-identical to the committed goldens because of the fields above.
#   * The nightly_docker workflow (.github/workflows/cql-type-parity.yml)
#     regenerates the fixtures STRUCTURALLY to catch Cassandra-format drift; it
#     does not assert byte-identity against the committed snapshot.
#
# Backs: epic #971 (issues #1003, #1006, #1007, #1008)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-types"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_types"

# Fixed-timestamp constants (see header).
T_GEN1=1609459200000000   # 2021-01-01T00:00:00Z
T_GEN2=1609545600000000   # 2021-01-02T00:00:00Z (base + 1 day)
T_GEN3=1609632000000000   # 2021-01-03T00:00:00Z (base + 2 days)

# ---------------------------------------------------------------------------
# Parse CLI flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[types] Unknown argument: $1" >&2; exit 1 ;;
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
log()  { echo "[types] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[types][ERROR] $*" >&2; exit 1; }

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
    echo "[types] (dry-run) no container engine found; using placeholder 'docker' for command preview"
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
# Helper: capture a counter SELECT to a sidecar *.counter-select.txt beside the
# table's exported SSTables. Documents the EXPECTED final per-pk value for the
# Group D counter tables. Honors --dry-run.
# ---------------------------------------------------------------------------
capture_counter_select() {
  local table="$1"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $ENGINE exec $CONTAINER_NAME cqlsh -e \"SELECT * FROM $KEYSPACE.$table;\" > <table-dir>/$table.counter-select.txt"
    return 0
  fi
  # Locate the exported table directory (may not exist yet during dry-run); we
  # tee the SELECT both to the container stdout capture and the sidecar file.
  local table_dir
  table_dir="$(find "$SSTABLES_DIR/$KEYSPACE" -maxdepth 1 -type d -name "${table}-*" 2>/dev/null | head -1 || true)"
  if [[ -z "$table_dir" ]]; then
    log "  WARNING: no exported dir for counter table $table; skipping counter-select sidecar"
    return 0
  fi
  local sidecar="$table_dir/$table.counter-select.txt"
  log "  counter SELECT -> $sidecar"
  $ENGINE exec "$CONTAINER_NAME" \
    cqlsh -e "SELECT * FROM $KEYSPACE.$table;" > "$sidecar" 2>/dev/null || true
  if [[ -s "$sidecar" ]]; then
    log "  OK: $sidecar"
  else
    log "  WARNING: empty counter SELECT for $table"
  fi
}

# ---------------------------------------------------------------------------
# Phase: generation-1 writes.
#   - All single-flush tables (Group B, Group C non-multigen, Group A single-gen)
#   - gen-1 of every multi-generation table (Group A se_*, Group C
#     cx_legacy_dropped_tuple_udt, and counter increments for Group D).
#
# Every NON-COUNTER write/delete uses an explicit USING TIMESTAMP. Counter
# UPDATEs intentionally omit it (Cassandra rejects USING TIMESTAMP on counters).
# ---------------------------------------------------------------------------
run_gen1() {
  log "=== Generation 1: single-flush shapes + gen-1 of multi-gen tables ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would insert gen-1 rows via python3 cassandra-driver heredoc (USING TIMESTAMP $T_GEN1)"
    echo "[dry-run]   GROUP A gen-1: se_no_schema_change, se_altered_column_type, se_dropped_column_same_type,"
    echo "[dry-run]                  se_altered_then_dropped_column write base/original columns USING TIMESTAMP $T_GEN1"
    echo "[dry-run]   GROUP A single: se_static_regular_kind_mismatch (static + rows), se_frozen_multicell_collection_mismatch (ml multicell + fl frozen)"
    echo "[dry-run]   GROUP B: nb_null_empty_text_blob (absent/NULL/''/0x), nb_absent_vs_null_regular (absent/deleted/empty),"
    echo "[dry-run]            nb_empty_collections (empty multicell vs frozen empty), nb_length_prefix_edges (0,1,127,128,255,256,16383,16384)"
    echo "[dry-run]   GROUP C: cx_tuple_field_order, cx_udt_field_order_null_empty, cx_frozen_udt_value, cx_nested_frozen_collections,"
    echo "[dry-run]            cx_multicell_udt_collection_paths, cx_legacy_dropped_tuple_udt (gen-1 with tuple+UDT) USING TIMESTAMP $T_GEN1"
    echo "[dry-run]   GROUP D gen-1: counter UPDATEs (NO USING TIMESTAMP) for ct_single_sstable, ct_multi_sstable_merge, ct_deleted_counter_shadowing, ct_compacted_final_value"
    return 0
  fi
  $ENGINE exec -i "$CONTAINER_NAME" \
    env T_GEN1="$T_GEN1" T_GEN2="$T_GEN2" KEYSPACE="$KEYSPACE" \
    python3 - <<'PYEOF'
import os, sys, traceback, time
from cassandra.cluster import Cluster

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

    # ===================================================================
    # GROUP A — Schema evolution / SerializationHeader (#1003), gen-1
    # ===================================================================

    # 1. se_no_schema_change (control) — gen-1 base rows.
    print("[A1] se_no_schema_change (gen-1)", flush=True)
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO se_no_schema_change (pk, ck, v) "
            f"VALUES (1, {ck}, 'g1_{ck}') USING TIMESTAMP {T_GEN1}"
        )

    # 2. se_altered_column_type — gen-1 writes only orig_col (added_col not yet
    #    in schema; ADD happens between flushes).
    print("[A2] se_altered_column_type (gen-1)", flush=True)
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO se_altered_column_type (pk, ck, orig_col) "
            f"VALUES (1, {ck}, 'orig_{ck}') USING TIMESTAMP {T_GEN1}"
        )

    # 3. se_dropped_column_same_type — gen-1 writes BOTH keepme and dropme.
    #    dropme is DROPped between flushes; gen-1 header still declares it.
    print("[A3] se_dropped_column_same_type (gen-1)", flush=True)
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO se_dropped_column_same_type (pk, ck, keepme, dropme) "
            f"VALUES (1, {ck}, 'keep_{ck}', 'drop_{ck}') USING TIMESTAMP {T_GEN1}"
        )

    # 4. se_altered_then_dropped_column — gen-1 writes only base_col.
    print("[A4] se_altered_then_dropped_column (gen-1)", flush=True)
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO se_altered_then_dropped_column (pk, ck, base_col) "
            f"VALUES (1, {ck}, 'base_g1_{ck}') USING TIMESTAMP {T_GEN1}"
        )

    # 5. se_static_regular_kind_mismatch — single flush. Static + regular rows.
    print("[A5] se_static_regular_kind_mismatch", flush=True)
    session.execute(
        f"UPDATE se_static_regular_kind_mismatch USING TIMESTAMP {T_GEN1} "
        f"SET stat_col='shared_static' WHERE pk=1"
    )
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO se_static_regular_kind_mismatch (pk, ck, row_col) "
            f"VALUES (1, {ck}, 'row_{ck}') USING TIMESTAMP {T_GEN1}"
        )

    # 6. se_frozen_multicell_collection_mismatch — single flush. ml multicell,
    #    fl frozen. Both carry the same logical values so the ONLY difference is
    #    the frozen flag + multicell layout in the header/cells.
    print("[A6] se_frozen_multicell_collection_mismatch", flush=True)
    session.execute(
        f"INSERT INTO se_frozen_multicell_collection_mismatch (pk, ck, ml, fl) "
        f"VALUES (1, 1, ['a','b','c'], ['a','b','c']) USING TIMESTAMP {T_GEN1}"
    )
    session.execute(
        f"INSERT INTO se_frozen_multicell_collection_mismatch (pk, ck, ml, fl) "
        f"VALUES (1, 2, ['x'], ['x']) USING TIMESTAMP {T_GEN1}"
    )

    # ===================================================================
    # GROUP B — Null vs empty & length boundaries (#1006), single flush
    # ===================================================================

    # 7. nb_null_empty_text_blob — neighbors before/after each boundary value.
    print("[B7] nb_null_empty_text_blob", flush=True)
    # ck=1: full row (non-empty everywhere) — baseline.
    session.execute(
        f"INSERT INTO nb_null_empty_text_blob (pk, ck, before_col, target_text, target_blob, after_col) "
        f"VALUES (1, 1, 'before', 'nonempty', 0xdeadbeef, 'after') USING TIMESTAMP {T_GEN1}"
    )
    # ck=2: target_text ABSENT (never written), neighbors present.
    session.execute(
        f"INSERT INTO nb_null_empty_text_blob (pk, ck, before_col, target_blob, after_col) "
        f"VALUES (1, 2, 'before', 0x01, 'after') USING TIMESTAMP {T_GEN1}"
    )
    # ck=3: explicit NULL via write-then-DELETE cell on target_text.
    session.execute(
        f"INSERT INTO nb_null_empty_text_blob (pk, ck, before_col, target_text, target_blob, after_col) "
        f"VALUES (1, 3, 'before', 'will_be_deleted', 0x02, 'after') USING TIMESTAMP {T_GEN1}"
    )
    session.execute(
        f"DELETE target_text FROM nb_null_empty_text_blob USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=3"
    )
    # ck=4: empty string '' for target_text, empty blob 0x for target_blob.
    session.execute(
        f"INSERT INTO nb_null_empty_text_blob (pk, ck, before_col, target_text, target_blob, after_col) "
        f"VALUES (1, 4, 'before', '', 0x, 'after') USING TIMESTAMP {T_GEN1}"
    )

    # 8. nb_absent_vs_null_regular — 3 distinct rows.
    print("[B8] nb_absent_vs_null_regular", flush=True)
    # ck=1: reg ABSENT (never written), anchor present.
    session.execute(
        f"INSERT INTO nb_absent_vs_null_regular (pk, ck, anchor) "
        f"VALUES (1, 1, 'anchor_absent') USING TIMESTAMP {T_GEN1}"
    )
    # ck=2: reg explicitly DELETED (tombstone/null).
    session.execute(
        f"INSERT INTO nb_absent_vs_null_regular (pk, ck, anchor, reg) "
        f"VALUES (1, 2, 'anchor_deleted', 'present_then_gone') USING TIMESTAMP {T_GEN1}"
    )
    session.execute(
        f"DELETE reg FROM nb_absent_vs_null_regular USING TIMESTAMP {T_GEN2} WHERE pk=1 AND ck=2"
    )
    # ck=3: reg written EMPTY ''.
    session.execute(
        f"INSERT INTO nb_absent_vs_null_regular (pk, ck, anchor, reg) "
        f"VALUES (1, 3, 'anchor_empty', '') USING TIMESTAMP {T_GEN1}"
    )

    # 9. nb_empty_collections — empty multicell (stored ABSENT) vs frozen empty
    #    (DOES persist) alongside non-empty.
    print("[B9] nb_empty_collections", flush=True)
    # ck=1: all EMPTY. Multicell empties are stored as absent; frozen empties
    #       persist as an empty-but-present value.
    session.execute(
        f"INSERT INTO nb_empty_collections (pk, ck, ml, ms, mm, fl, fs, fm) "
        f"VALUES (1, 1, [], {{}}, {{}}, [], {{}}, {{}}) USING TIMESTAMP {T_GEN1}"
    )
    # ck=2: all NON-EMPTY for contrast.
    session.execute(
        f"INSERT INTO nb_empty_collections (pk, ck, ml, ms, mm, fl, fs, fm) "
        f"VALUES (1, 2, [1,2], {{'a','b'}}, {{'k':1}}, [3,4], {{'c','d'}}, {{'m':2}}) "
        f"USING TIMESTAMP {T_GEN1}"
    )

    # 10. nb_length_prefix_edges — text/blob of length 0,1,127,128,255,256,
    #     16383,16384 with non-empty neighbors. Deterministic 'a'*N fill; blob is
    #     N bytes of 0x61 (also 'a').
    print("[B10] nb_length_prefix_edges", flush=True)
    edge_lengths = [0, 1, 127, 128, 255, 256, 16383, 16384]
    edge_stmt = session.prepare(
        "INSERT INTO nb_length_prefix_edges (pk, ck, before_col, edge_text, edge_blob, after_col) "
        "VALUES (1, ?, ?, ?, ?, ?) USING TIMESTAMP ?"
    )
    for ck, n in enumerate(edge_lengths, start=1):
        txt = "a" * n
        blob = b"\x61" * n
        session.execute(edge_stmt, (ck, f"before_{n}", txt, blob, f"after_{n}", T_GEN1))

    # ===================================================================
    # GROUP C — UDT / tuple / frozen / nested (#1007), single flush
    # ===================================================================

    # 11. cx_tuple_field_order — tuple<int,text,boolean> with a null middle field.
    print("[C11] cx_tuple_field_order", flush=True)
    # ck=1: full tuple.
    session.execute(
        f"INSERT INTO cx_tuple_field_order (pk, ck, t) "
        f"VALUES (1, 1, (7, 'seven', true)) USING TIMESTAMP {T_GEN1}"
    )
    # ck=2: null MIDDLE field (text is null), ints/bools present.
    session.execute(
        f"INSERT INTO cx_tuple_field_order (pk, ck, t) "
        f"VALUES (1, 2, (9, null, false)) USING TIMESTAMP {T_GEN1}"
    )

    # 12. cx_udt_field_order_null_empty — frozen person_type: null field,
    #     empty-string field, and fully-populated.
    print("[C12] cx_udt_field_order_null_empty", flush=True)
    # ck=1: full.
    session.execute(
        f"INSERT INTO cx_udt_field_order_null_empty (pk, ck, p) "
        f"VALUES (1, 1, {{first_name:'Ada', last_name:'Lovelace', age:36, active:true}}) "
        f"USING TIMESTAMP {T_GEN1}"
    )
    # ck=2: null last_name (omitted) + empty-string first_name.
    session.execute(
        f"INSERT INTO cx_udt_field_order_null_empty (pk, ck, p) "
        f"VALUES (1, 2, {{first_name:'', last_name:null, age:0, active:false}}) "
        f"USING TIMESTAMP {T_GEN1}"
    )

    # 13. cx_frozen_udt_value — frozen<employee_type> (nested: contains address).
    print("[C13] cx_frozen_udt_value", flush=True)
    session.execute(
        f"INSERT INTO cx_frozen_udt_value (pk, ck, e) "
        f"VALUES (1, 1, {{name:'Grace', "
        f"home:{{street:'1 Navy Way', city:'Arlington', zip:'22201'}}, "
        f"title:'Rear Admiral', level:9}}) USING TIMESTAMP {T_GEN1}"
    )
    # null nested field + empty-string nested field.
    session.execute(
        f"INSERT INTO cx_frozen_udt_value (pk, ck, e) "
        f"VALUES (1, 2, {{name:'NoHome', "
        f"home:{{street:'', city:null, zip:''}}, "
        f"title:null, level:0}}) USING TIMESTAMP {T_GEN1}"
    )

    # 14. cx_nested_frozen_collections — collection-of-collection nesting.
    print("[C14] cx_nested_frozen_collections", flush=True)
    session.execute(
        f"INSERT INTO cx_nested_frozen_collections (pk, ck, m_list_vals, l_set_vals, s_map_vals) "
        f"VALUES (1, 1, "
        f"{{'odds':[1,3,5], 'evens':[2,4,6]}}, "
        f"[{{'a','b'}}, {{'c'}}], "
        f"{{ {{'k1':1}}, {{'k2':2,'k3':3}} }}) USING TIMESTAMP {T_GEN1}"
    )

    # 15. cx_multicell_udt_collection_paths — NON-frozen UDT (multicell, one cell
    #     per field w/ cell-path) + multicell list (one cell per element).
    print("[C15] cx_multicell_udt_collection_paths", flush=True)
    session.execute(
        f"INSERT INTO cx_multicell_udt_collection_paths (pk, ck, mp, ml) "
        f"VALUES (1, 1, {{first_name:'Alan', last_name:'Turing', age:41, active:true}}, "
        f"['t1','t2','t3']) USING TIMESTAMP {T_GEN1}"
    )

    # 16. cx_legacy_dropped_tuple_udt — gen-1 writes survivor + drop_tuple +
    #     drop_udt. Both complex columns DROPped between flushes.
    print("[C16] cx_legacy_dropped_tuple_udt (gen-1)", flush=True)
    for ck in range(1, 4):
        session.execute(
            f"INSERT INTO cx_legacy_dropped_tuple_udt (pk, ck, survivor, drop_tuple, drop_udt) "
            f"VALUES (1, {ck}, 'survive_{ck}', ({ck}, 'tup_{ck}'), "
            f"{{first_name:'fn_{ck}', last_name:'ln_{ck}', age:{ck}, active:true}}) "
            f"USING TIMESTAMP {T_GEN1}"
        )

    # ===================================================================
    # GROUP D — Counters (#1008), gen-1 increments.
    # NOTE: counter UPDATEs intentionally OMIT USING TIMESTAMP — Cassandra
    # rejects it on counter mutations; the counter shard clock is
    # coordinator-derived. EXPECTED final values are documented at the end of
    # this script and captured to *.counter-select.txt sidecars.
    # ===================================================================
    print("[D] counters (gen-1 increments, NO USING TIMESTAMP)", flush=True)

    # 17. ct_single_sstable — pk=1 net +30 (10+20), pk=2 net +5.
    session.execute("UPDATE ct_single_sstable SET c = c + 10 WHERE pk=1")
    session.execute("UPDATE ct_single_sstable SET c = c + 20 WHERE pk=1")
    session.execute("UPDATE ct_single_sstable SET c = c + 5  WHERE pk=2")

    # 18. ct_multi_sstable_merge — gen-1 increments only.
    #     pk=1 gen-1 = +100; pk=2 gen-1 = +50.
    session.execute("UPDATE ct_multi_sstable_merge SET c = c + 100 WHERE pk=1")
    session.execute("UPDATE ct_multi_sstable_merge SET c = c + 50  WHERE pk=2")

    # 19. ct_deleted_counter_shadowing — gen-1 increments (to be deleted in gen-2).
    #     pk=1 gen-1 = +77; pk=2 gen-1 = +33 (pk=2 NOT deleted -> survives).
    session.execute("UPDATE ct_deleted_counter_shadowing SET c = c + 77 WHERE pk=1")
    session.execute("UPDATE ct_deleted_counter_shadowing SET c = c + 33 WHERE pk=2")

    # 20. ct_compacted_final_value — gen-1 increments only.
    #     pk=1 gen-1 = +200; pk=2 gen-1 = +60.
    session.execute("UPDATE ct_compacted_final_value SET c = c + 200 WHERE pk=1")
    session.execute("UPDATE ct_compacted_final_value SET c = c + 60  WHERE pk=2")

    print("[OK] test_types: generation-1 writes complete", flush=True)
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
#   GROUP A:
#     se_no_schema_change            : gen-2 base rows (no schema change).
#     se_altered_column_type         : gen-2 writes orig_col + added_col (post-ADD).
#     se_dropped_column_same_type    : gen-2 writes keepme only (post-DROP).
#     se_altered_then_dropped_column : gen-2 writes base_col + evolve_col (post-ADD).
#   GROUP C:
#     cx_legacy_dropped_tuple_udt    : gen-2 writes survivor only (post-DROP).
#   GROUP D:
#     ct_multi_sstable_merge         : gen-2 increments AND decrements.
#     ct_deleted_counter_shadowing   : gen-2 DELETE the counter row (shadows gen-1).
#     ct_compacted_final_value       : gen-2 increments AND decrements.
# All non-counter writes use explicit USING TIMESTAMP (T_GEN2/T_GEN3).
# Counter UPDATEs omit USING TIMESTAMP.
# ---------------------------------------------------------------------------
run_gen2() {
  log "=== Generation 2: post-ALTER / cross-generation writes ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would run gen-2 writes via python3 cassandra-driver heredoc"
    echo "[dry-run]   se_no_schema_change: INSERT base rows USING TIMESTAMP $T_GEN2"
    echo "[dry-run]   se_altered_column_type: INSERT orig_col + added_col USING TIMESTAMP $T_GEN2 (post-ADD)"
    echo "[dry-run]   se_dropped_column_same_type: INSERT keepme only USING TIMESTAMP $T_GEN2 (post-DROP)"
    echo "[dry-run]   se_altered_then_dropped_column: INSERT base_col + evolve_col USING TIMESTAMP $T_GEN2 (post-ADD)"
    echo "[dry-run]   cx_legacy_dropped_tuple_udt: INSERT survivor only USING TIMESTAMP $T_GEN2 (post-DROP)"
    echo "[dry-run]   ct_multi_sstable_merge: counter inc/dec (NO USING TIMESTAMP)"
    echo "[dry-run]   ct_deleted_counter_shadowing: DELETE counter row (shadows gen-1)"
    echo "[dry-run]   ct_compacted_final_value: counter inc/dec (NO USING TIMESTAMP)"
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

    # GROUP A gen-2 --------------------------------------------------------
    # se_no_schema_change (control): fresh base rows.
    print("[A1] se_no_schema_change (gen-2)", flush=True)
    for ck in range(4, 7):
        session.execute(
            f"INSERT INTO se_no_schema_change (pk, ck, v) "
            f"VALUES (1, {ck}, 'g2_{ck}') USING TIMESTAMP {T_GEN2}"
        )

    # se_altered_column_type (post-ADD added_col bigint): write BOTH columns.
    print("[A2] se_altered_column_type (gen-2, post-ADD)", flush=True)
    for ck in range(4, 7):
        session.execute(
            f"INSERT INTO se_altered_column_type (pk, ck, orig_col, added_col) "
            f"VALUES (1, {ck}, 'orig_g2_{ck}', {ck * 1000}) USING TIMESTAMP {T_GEN2}"
        )

    # se_dropped_column_same_type (post-DROP dropme): write keepme only.
    print("[A3] se_dropped_column_same_type (gen-2, post-DROP)", flush=True)
    for ck in range(4, 7):
        session.execute(
            f"INSERT INTO se_dropped_column_same_type (pk, ck, keepme) "
            f"VALUES (1, {ck}, 'keep_g2_{ck}') USING TIMESTAMP {T_GEN2}"
        )

    # se_altered_then_dropped_column (post-ADD evolve_col): write base + evolve.
    print("[A4] se_altered_then_dropped_column (gen-2, post-ADD)", flush=True)
    for ck in range(4, 7):
        session.execute(
            f"INSERT INTO se_altered_then_dropped_column (pk, ck, base_col, evolve_col) "
            f"VALUES (1, {ck}, 'base_g2_{ck}', 'evolve_g2_{ck}') USING TIMESTAMP {T_GEN2}"
        )

    # GROUP C gen-2 --------------------------------------------------------
    # cx_legacy_dropped_tuple_udt (post-DROP of drop_tuple + drop_udt): survivor.
    print("[C16] cx_legacy_dropped_tuple_udt (gen-2, post-DROP)", flush=True)
    for ck in range(4, 7):
        session.execute(
            f"INSERT INTO cx_legacy_dropped_tuple_udt (pk, ck, survivor) "
            f"VALUES (1, {ck}, 'survive_g2_{ck}') USING TIMESTAMP {T_GEN2}"
        )

    # GROUP D gen-2 --------------------------------------------------------
    # ct_multi_sstable_merge: inc + dec. EXPECTED final:
    #   pk=1 = 100 (g1) + 25 - 40 = 85 ; pk=2 = 50 (g1) + 10 = 60.
    print("[D18] ct_multi_sstable_merge (gen-2 inc/dec)", flush=True)
    session.execute("UPDATE ct_multi_sstable_merge SET c = c + 25 WHERE pk=1")
    session.execute("UPDATE ct_multi_sstable_merge SET c = c - 40 WHERE pk=1")
    session.execute("UPDATE ct_multi_sstable_merge SET c = c + 10 WHERE pk=2")

    # ct_deleted_counter_shadowing: DELETE the counter row for pk=1 (shadows the
    # gen-1 +77). pk=2 untouched -> survives at +33.
    print("[D19] ct_deleted_counter_shadowing (gen-2 delete)", flush=True)
    session.execute("DELETE FROM ct_deleted_counter_shadowing WHERE pk=1")

    # ct_compacted_final_value: inc + dec. EXPECTED final:
    #   pk=1 = 200 (g1) + 15 - 5 = 210 ; pk=2 = 60 (g1) - 20 = 40.
    print("[D20] ct_compacted_final_value (gen-2 inc/dec)", flush=True)
    session.execute("UPDATE ct_compacted_final_value SET c = c + 15 WHERE pk=1")
    session.execute("UPDATE ct_compacted_final_value SET c = c - 5  WHERE pk=1")
    session.execute("UPDATE ct_compacted_final_value SET c = c - 20 WHERE pk=2")

    print("[OK] test_types: generation-2 writes complete", flush=True)
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
# Phase: generation-3 writes (only se_altered_then_dropped_column).
#   Post-DROP of evolve_col: write base_col only with T_GEN3.
# ---------------------------------------------------------------------------
run_gen3() {
  log "=== Generation 3: se_altered_then_dropped_column post-DROP ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would run gen-3 writes via python3 cassandra-driver heredoc"
    echo "[dry-run]   se_altered_then_dropped_column: INSERT base_col only USING TIMESTAMP $T_GEN3 (post-DROP evolve_col)"
    return 0
  fi
  $ENGINE exec -i "$CONTAINER_NAME" \
    env T_GEN3="$T_GEN3" KEYSPACE="$KEYSPACE" \
    python3 - <<'PYEOF'
import os, sys, traceback, time
from cassandra.cluster import Cluster

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
    print("[A4] se_altered_then_dropped_column (gen-3, post-DROP)", flush=True)
    for ck in range(7, 10):
        session.execute(
            f"INSERT INTO se_altered_then_dropped_column (pk, ck, base_col) "
            f"VALUES (1, {ck}, 'base_g3_{ck}') USING TIMESTAMP {T_GEN3}"
        )
    print("[OK] test_types: generation-3 writes complete", flush=True)
    cluster.shutdown()
except SystemExit:
    raise
except Exception:
    print("[FATAL] Unhandled exception during gen-3 writes:", flush=True)
    traceback.print_exc()
    sys.exit(1)
PYEOF
}

# ---------------------------------------------------------------------------
# Generate sstabledump JSONL golden files (matches generate-tombstone-parity.sh).
# ---------------------------------------------------------------------------
generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files for $KEYSPACE..."
  while IFS= read -r -d '' data_file; do
    local rel
    rel="${data_file#"$sstables_dir"/}"
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
            | grep -z "$KEYSPACE" 2>/dev/null || true)
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

log "Starting $KEYSPACE generation (epic #971)"
log "Output directory: $OUT_DIR"
log "Fixed timestamps: T_GEN1=$T_GEN1 T_GEN2=$T_GEN2 T_GEN3=$T_GEN3"
log "Counters omit USING TIMESTAMP (Cassandra rejects it on counter mutations)"

SSTABLES_DIR="$OUT_DIR/sstables"

# ---------------------------------------------------------------------------
# Start Cassandra container (nb/CASSANDRA_4 compat mode — default for 5.0.2)
# ---------------------------------------------------------------------------
log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-types \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

# Install Python driver
log "Installing python3-pip in container..."
run $ENGINE exec "$CONTAINER_NAME" bash -c "apt-get update -qq && apt-get install -y -q python3-pip"
log "Installing cassandra-driver in container..."
run $ENGINE exec "$CONTAINER_NAME" pip3 install --quiet cassandra-driver

# Apply schema (creates keyspace + UDTs + all twenty tables)
apply_schema "$ROOT/schemas/cql-type-parity.cql"

# ---------------------------------------------------------------------------
# Generation 1: single-flush shapes + gen-1 of multi-gen tables.
# ---------------------------------------------------------------------------
run_gen1
flush_generation "gen-1"

# ---------------------------------------------------------------------------
# Between gen-1 and gen-2: ALTER statements (issue #1003 / #1007). These run
# AFTER the gen-1 flush so the gen-1 SerializationHeader still declares the
# original/dropped columns.
#   - ADD added_col  (se_altered_column_type)         : ALTER-TYPE substitution
#   - DROP dropme    (se_dropped_column_same_type)
#   - ADD evolve_col (se_altered_then_dropped_column)  : ALTER-TYPE substitution
#   - DROP drop_tuple, drop_udt (cx_legacy_dropped_tuple_udt)
# ---------------------------------------------------------------------------
log "Applying ALTERs between gen-1 and gen-2 (issues #1003 / #1007)..."
cql "ALTER TABLE se_altered_column_type ADD added_col bigint;"
cql "ALTER TABLE se_dropped_column_same_type DROP dropme;"
cql "ALTER TABLE se_altered_then_dropped_column ADD evolve_col text;"
cql "ALTER TABLE cx_legacy_dropped_tuple_udt DROP drop_tuple;"
cql "ALTER TABLE cx_legacy_dropped_tuple_udt DROP drop_udt;"

# ---------------------------------------------------------------------------
# Generation 2: post-ALTER / cross-generation writes.
# ---------------------------------------------------------------------------
run_gen2
flush_generation "gen-2"

# ---------------------------------------------------------------------------
# Between gen-2 and gen-3: DROP evolve_col so gen-3 does not reference it
# (se_altered_then_dropped_column). Runs AFTER the gen-2 flush so the gen-2
# header still declares evolve_col.
# ---------------------------------------------------------------------------
log "Dropping evolve_col between gen-2 and gen-3 (issue #1003)..."
cql "ALTER TABLE se_altered_then_dropped_column DROP evolve_col;"

# ---------------------------------------------------------------------------
# Generation 3: se_altered_then_dropped_column post-DROP base-only writes.
# ---------------------------------------------------------------------------
run_gen3
flush_generation "gen-3"

# ---------------------------------------------------------------------------
# Compaction: ct_compacted_final_value ONLY (issue #1008). Produces a compacted
# SSTable while keeping the source generations referenced for parity checks.
# ---------------------------------------------------------------------------
log "Compacting ct_compacted_final_value (issue #1008)..."
run $ENGINE exec "$CONTAINER_NAME" nodetool compact "$KEYSPACE" ct_compacted_final_value

# ---------------------------------------------------------------------------
# Export SSTables to host (matches generate-tombstone-parity.sh tar-stream)
# ---------------------------------------------------------------------------
log "=== Exporting $KEYSPACE SSTables from container ==="

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would rm -rf $SSTABLES_DIR/$KEYSPACE (clear stale tables) before re-export"
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE from container into $SSTABLES_DIR/$KEYSPACE"
  echo "[dry-run] would capture counter SELECTs to *.counter-select.txt for ct_single_sstable, ct_multi_sstable_merge, ct_deleted_counter_shadowing, ct_compacted_final_value"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.types_export_tmp"
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

  # Capture counter SELECTs (Group D) to *.counter-select.txt sidecars.
  log "Capturing counter SELECT sidecars (issue #1008)..."
  capture_counter_select "ct_single_sstable"
  capture_counter_select "ct_multi_sstable_merge"
  capture_counter_select "ct_deleted_counter_shadowing"
  capture_counter_select "ct_compacted_final_value"

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
  log "  single-flush (1 gen): se_static_regular_kind_mismatch, se_frozen_multicell_collection_mismatch,"
  log "                        nb_null_empty_text_blob, nb_absent_vs_null_regular, nb_empty_collections, nb_length_prefix_edges,"
  log "                        cx_tuple_field_order, cx_udt_field_order_null_empty, cx_frozen_udt_value,"
  log "                        cx_nested_frozen_collections, cx_multicell_udt_collection_paths, ct_single_sstable"
  log "  multi-flush (2 gens): se_no_schema_change, se_altered_column_type, se_dropped_column_same_type,"
  log "                        cx_legacy_dropped_tuple_udt, ct_multi_sstable_merge, ct_deleted_counter_shadowing"
  log "  three-flush (3 gens): se_altered_then_dropped_column"
  log "  multi-gen + compacted: ct_compacted_final_value"
  log ""
  # =====================================================================
  # EXPECTED COUNTER FINAL VALUES (issue #1008) — sidecar reference block
  # =====================================================================
  # Counters carry NO USING TIMESTAMP; final values are net of all shard deltas.
  #   ct_single_sstable            : pk=1 -> 30  (+10 +20) ; pk=2 -> 5  (+5)
  #   ct_multi_sstable_merge       : pk=1 -> 85  (+100 +25 -40) ; pk=2 -> 60  (+50 +10)
  #   ct_deleted_counter_shadowing : pk=1 -> (deleted; tombstone shadows +77) ; pk=2 -> 33 (+33, survives)
  #   ct_compacted_final_value     : pk=1 -> 210 (+200 +15 -5) ; pk=2 -> 40  (+60 -20)
  # The committed *.counter-select.txt sidecars capture Cassandra's actual SELECT
  # output for byte-for-byte parity; these arithmetic notes are the human-readable
  # cross-check.
  log "Expected counter final values:"
  log "  ct_single_sstable:            pk=1 -> 30   ; pk=2 -> 5"
  log "  ct_multi_sstable_merge:       pk=1 -> 85   ; pk=2 -> 60"
  log "  ct_deleted_counter_shadowing: pk=1 -> deleted ; pk=2 -> 33"
  log "  ct_compacted_final_value:     pk=1 -> 210  ; pk=2 -> 40"
  log ""
  log "Next steps:"
  log "  1. Run smoke test: bash test-data/scripts/smoke-test-all-tables.sh"
  log "  2. Verify gen-1 SerializationHeaders still declare dropped/original columns (se_dropped_column_same_type, se_altered_then_dropped_column, cx_legacy_dropped_tuple_udt)"
  log "  3. Confirm counter *.counter-select.txt sidecars match the expected final values above"
  log "  4. Confirm ct_compacted_final_value has a compacted SSTable alongside source generations"
  log "  5. Package and publish: bash test-data/scripts/package_datasets.sh"
fi

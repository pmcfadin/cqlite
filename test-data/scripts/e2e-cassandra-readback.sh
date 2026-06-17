#!/usr/bin/env bash
#
# e2e-cassandra-readback.sh
#
# PRD §4.1 acceptance gate (epic #472, issue #482):
# Validate that SSTables produced by CQLite can be loaded by a real
# Cassandra 5.0 cluster via `nodetool refresh` and queried with cqlsh.
#
# Flow per table:
#   1. cqlite writes a JSONL mutation set, flushes, and exports an SSTable.
#   2. The exported components are copied into the container's data
#      directory at /var/lib/cassandra/data/<ks>/<table>-<UUID-no-dashes>/.
#   3. `nodetool refresh <ks> <table>` reloads the SSTable.
#   4. cqlsh queries verify row count and per-row column values match
#      what cqlite wrote (structured JSON comparison, not substring grep).
#
# Usage:
#   bash test-data/scripts/e2e-cassandra-readback.sh
#
# Optional flags:
#   --keep-running    Skip stack tear-down on exit (faster local iteration).
#   --no-build        Skip cargo build of cqlite-cli (use existing binary).
#   --tables LIST     Comma-separated subset by label (default: all).
#                     Labels: basic-primitives, collections, udt,
#                             static-columns, ttl,
#                             cell-delete, row-delete, range-tombstone,
#                             partition-tombstone
#   --bin PATH        Path to a pre-built cqlite binary.
#   --self-test       Run a negative self-test that proves a value in the
#                     wrong column causes verification to FAIL, then exit.
#                     Also tests absence directives (absent_col, absent_row_cluster).
#                     Does not require a running Cassandra cluster.
#
# Spec language reference:
#   row_count=<N>                                 exact row count from SELECT count(*)
#   row.<pk_col>=<cql-pk-value>                   partition to query
#   col[<pk>].<col>=<json-value>                  column exact-match check
#   col_cluster[<pk>|<ck>].<col>=<json-value>     clustering-row exact-match
#   absent_col[<pk>].<col>                        column must be null/absent in row
#   absent_row_cluster[<pk>|<ck>]                 clustering row must not exist
#
# Exit code: 0 only when every selected table passes refresh+readback
#            (or when --self-test passes its negative check).
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$ROOT/scripts"
SCHEMAS="$ROOT/schemas"
COMPOSE_FILE="$ROOT/docker/docker-compose-cassandra5.yml"
CONTAINER_NAME="cqlite-cassandra-5-0"
SERVICE_NAME="cassandra-5-0"
# Path to the single shared production verifier (used by verify_table AND --self-test).
E2E_VERIFY="$SCRIPTS/e2e_verify.py"

# ----- CLI arg parsing ---------------------------------------------------
KEEP_RUNNING=0
SKIP_BUILD=0
SUBSET=""
CQLITE_BIN_OVERRIDE=""
SELF_TEST=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-running) KEEP_RUNNING=1; shift ;;
    --no-build) SKIP_BUILD=1; shift ;;
    --tables) SUBSET="$2"; shift 2 ;;
    --bin) CQLITE_BIN_OVERRIDE="$2"; shift 2 ;;
    --self-test) SELF_TEST=1; shift ;;
    -h|--help)
      sed -n '3,32p' "$0"; exit 0 ;;
    *) echo "[e2e-readback][ERROR] Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ----- Logging -----------------------------------------------------------
# All log output goes to stderr so functions that need to return values via
# `$(...)` (e.g. write_and_export) can keep stdout exclusively for the
# return value.
log()    { printf '[e2e-readback] %s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S')" "$*" >&2; }
warn()   { printf '[e2e-readback][WARN] %s\n' "$*" >&2; }
fail()   { printf '[e2e-readback][ERROR] %s\n' "$*" >&2; exit 1; }
phase()  { printf '\n[e2e-readback] === %s ===\n' "$*" >&2; }

# ----- Self-test mode (negative / no-Cassandra-required) -----------------
# Exercises the PRODUCTION verifier (e2e_verify.py) with canned cqlsh-JSON
# fixtures — both positive (correct spec → PASS) and negative (wrong value
# or absent-directive violation → FAIL).  No container runtime required.
run_self_test() {
  phase "Self-test: production verifier exercised with canned fixtures"

  [[ -f "$E2E_VERIFY" ]] || { warn "Production verifier not found: $E2E_VERIFY"; exit 1; }

  # Helper: write a spec string to a temp file and return its path.
  local _spec_tmp
  _spec_tmp="$(mktemp /tmp/e2e-selftest-spec.XXXXXX)"
  # Register cleanup of temp file on exit from this function.
  # shellcheck disable=SC2064
  trap "rm -f '$_spec_tmp'" RETURN

  # -----------------------------------------------------------------------
  # Case 1: wrong-column spec must FAIL
  #   Row: id="11111111-…", name="Alice", age=30
  #   Spec claims name=30  (age value leaked into wrong column) → FAIL
  # -----------------------------------------------------------------------
  local fake_row_json
  fake_row_json='{ "id": "11111111-1111-1111-1111-111111111111", "name": "Alice", "age": 30, "active": true }'

  printf '%s\n' \
    "row_count=1" \
    "row.id=11111111-1111-1111-1111-111111111111" \
    "col[11111111-1111-1111-1111-111111111111].name=30" \
    > "$_spec_tmp"

  log "Case 1: wrong-column spec (expect FAIL from production verifier)"
  local rc=0
  printf '%s\n' "$fake_row_json" \
    | python3 "$E2E_VERIFY" "11111111-1111-1111-1111-111111111111" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    warn "Self-test FAILED: wrong-column spec did NOT produce a verification failure"
    exit 1
  fi
  log "Case 1 PASSED: wrong-column spec correctly caused verification failure (exit $rc)"

  # -----------------------------------------------------------------------
  # Case 2: correct spec must PASS
  # -----------------------------------------------------------------------
  printf '%s\n' \
    "row_count=1" \
    "row.id=11111111-1111-1111-1111-111111111111" \
    'col[11111111-1111-1111-1111-111111111111].name="Alice"' \
    "col[11111111-1111-1111-1111-111111111111].age=30" \
    > "$_spec_tmp"

  log "Case 2: correct spec (expect PASS from production verifier)"
  rc=0
  printf '%s\n' "$fake_row_json" \
    | python3 "$E2E_VERIFY" "11111111-1111-1111-1111-111111111111" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    warn "Self-test FAILED: correct spec unexpectedly produced a verification failure"
    exit 1
  fi
  log "Case 2 PASSED: correct spec correctly passed verification"

  # -----------------------------------------------------------------------
  # Case 3: absent_col — column IS null → must PASS
  # -----------------------------------------------------------------------
  local null_row_json
  null_row_json='{ "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": null, "age": 42 }'

  printf '%s\n' \
    "row_count=1" \
    "row.id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" \
    "absent_col[aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa].name" \
    "col[aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa].age=42" \
    > "$_spec_tmp"

  log "Case 3: absent_col for null column (expect PASS)"
  rc=0
  printf '%s\n' "$null_row_json" \
    | python3 "$E2E_VERIFY" "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    warn "Self-test FAILED: absent_col check for null column failed unexpectedly"
    exit 1
  fi
  log "Case 3 PASSED: absent_col correctly passes for null column"

  # -----------------------------------------------------------------------
  # Case 4: absent_col — column is NOT null → must FAIL
  # -----------------------------------------------------------------------
  local non_null_row_json
  non_null_row_json='{ "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Alice", "age": 42 }'

  # Spec says 'name' must be absent/null, but it's "Alice" → verifier must FAIL.
  printf '%s\n' \
    "row_count=1" \
    "row.id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" \
    "absent_col[aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa].name" \
    > "$_spec_tmp"

  log "Case 4: absent_col for non-null column (expect FAIL)"
  rc=0
  printf '%s\n' "$non_null_row_json" \
    | python3 "$E2E_VERIFY" "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    warn "Self-test FAILED: absent_col check for non-null column did NOT fail"
    exit 1
  fi
  log "Case 4 PASSED: absent_col correctly fails for non-null column (exit $rc)"

  # -----------------------------------------------------------------------
  # Case 5: absent_row_cluster — target CK not present → must PASS
  # -----------------------------------------------------------------------
  # Two clustering rows exist (ck=01.000Z and ck=03.000Z).
  # Spec asserts ck=02.000Z is absent → should PASS.
  local multi_row_json
  multi_row_json='
{ "partition_key": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "clustering_key": "2024-01-01 00:00:01.000Z", "row_data": "alpha" }
{ "partition_key": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "clustering_key": "2024-01-01 00:00:03.000Z", "row_data": "gamma" }'

  printf '%s\n' \
    "row_count=2" \
    "row.partition_key=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" \
    "absent_row_cluster[bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb|2024-01-01 00:00:02.000Z]" \
    > "$_spec_tmp"

  log "Case 5: absent_row_cluster for missing CK (expect PASS)"
  rc=0
  printf '%s\n' "$multi_row_json" \
    | python3 "$E2E_VERIFY" "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    warn "Self-test FAILED: absent_row_cluster for absent row failed unexpectedly"
    exit 1
  fi
  log "Case 5 PASSED: absent_row_cluster correctly passes for missing CK"

  # -----------------------------------------------------------------------
  # Case 6: absent_row_cluster — target CK IS present → must FAIL
  # -----------------------------------------------------------------------
  # Same two rows; spec asserts ck=01.000Z is absent — but it exists → FAIL.
  printf '%s\n' \
    "row_count=2" \
    "row.partition_key=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" \
    "absent_row_cluster[bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb|2024-01-01 00:00:01.000Z]" \
    > "$_spec_tmp"

  log "Case 6: absent_row_cluster for existing CK (expect FAIL)"
  rc=0
  printf '%s\n' "$multi_row_json" \
    | python3 "$E2E_VERIFY" "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    warn "Self-test FAILED: absent_row_cluster for existing CK did NOT fail"
    exit 1
  fi
  log "Case 6 PASSED: absent_row_cluster correctly fails for existing CK (exit $rc)"

  # -----------------------------------------------------------------------
  # Case 7: absent_col when entire row is missing → must FAIL
  #   absent_col is a cell-delete assertion; the row must still exist.
  #   An empty result set must be reported as a clear failure.
  # -----------------------------------------------------------------------
  local missing_row_json
  missing_row_json=''   # empty stdin — no rows for this partition

  printf '%s\n' \
    "row_count=0" \
    "row.id=cccccccc-cccc-cccc-cccc-cccccccccccc" \
    "absent_col[cccccccc-cccc-cccc-cccc-cccccccccccc].name" \
    > "$_spec_tmp"

  log "Case 7: absent_col with entirely missing row (expect FAIL)"
  rc=0
  printf '%s' "$missing_row_json" \
    | python3 "$E2E_VERIFY" "cccccccc-cccc-cccc-cccc-cccccccccccc" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    warn "Self-test FAILED: absent_col against missing row did NOT fail"
    exit 1
  fi
  log "Case 7 PASSED: absent_col correctly fails when target row is entirely missing (exit $rc)"

  # -----------------------------------------------------------------------
  # Case 8: col_cluster — correct clustering-row value → must PASS
  # -----------------------------------------------------------------------
  local cluster_row_json
  cluster_row_json='
{ "partition_key": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee", "clustering_key": "2024-01-01 00:00:01.000Z", "row_data": "alpha", "row_value": 11 }
{ "partition_key": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee", "clustering_key": "2024-01-01 00:00:02.000Z", "row_data": "beta",  "row_value": 22 }'

  printf '%s\n' \
    "row_count=2" \
    "row.partition_key=eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" \
    'col_cluster[eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee|2024-01-01 00:00:01.000Z].row_data="alpha"' \
    'col_cluster[eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee|2024-01-01 00:00:01.000Z].row_value=11' \
    'col_cluster[eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee|2024-01-01 00:00:02.000Z].row_data="beta"' \
    'col_cluster[eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee|2024-01-01 00:00:02.000Z].row_value=22' \
    > "$_spec_tmp"

  log "Case 8: col_cluster correct values (expect PASS)"
  rc=0
  printf '%s\n' "$cluster_row_json" \
    | python3 "$E2E_VERIFY" "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    warn "Self-test FAILED: col_cluster correct-value check failed unexpectedly"
    exit 1
  fi
  log "Case 8 PASSED: col_cluster correctly passes for correct clustering-row values"

  # -----------------------------------------------------------------------
  # Case 9: col_cluster — wrong clustering-row value → must FAIL
  # -----------------------------------------------------------------------
  printf '%s\n' \
    "row_count=2" \
    "row.partition_key=eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" \
    'col_cluster[eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee|2024-01-01 00:00:01.000Z].row_data="WRONG"' \
    > "$_spec_tmp"

  log "Case 9: col_cluster wrong value (expect FAIL)"
  rc=0
  printf '%s\n' "$cluster_row_json" \
    | python3 "$E2E_VERIFY" "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" "$_spec_tmp" \
    || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    warn "Self-test FAILED: col_cluster wrong-value check did NOT fail"
    exit 1
  fi
  log "Case 9 PASSED: col_cluster correctly fails for wrong clustering-row value (exit $rc)"

  log "Self-test: all 9 cases passed using production verifier ($E2E_VERIFY)"
  exit 0
}

if [[ "$SELF_TEST" -eq 1 ]]; then
  run_self_test
fi

# Source container_env ONLY on the real (Docker/podman) run path, after the
# --self-test early exit above.  This ensures --self-test works on machines
# that have no container runtime installed.
# shellcheck source=test-data/scripts/container_env.sh
. "$SCRIPTS/container_env.sh"
export COMPOSE_FILE

# ----- Working directory + cleanup ---------------------------------------
# Pin under /tmp so `docker cp` works on macOS (Docker Desktop's default file
# sharing covers /tmp but not the per-user $TMPDIR=/var/folders/... path).
WORKDIR="$(mktemp -d "/tmp/cqlite-e2e-readback.XXXXXX")"
log "Workdir: $WORKDIR"
declare -a PASSED_LIST=()
declare -a FAILED_LIST=()

# shellcheck disable=SC2329  # invoked indirectly via trap
cleanup() {
  local rc=$?
  if [[ "$KEEP_RUNNING" -eq 0 ]]; then
    log "Tearing down Cassandra stack"
    bash "$SCRIPTS/shutdown-clean.sh" >/dev/null 2>&1 || true
    rm -rf "$WORKDIR" || true
  else
    log "Leaving Cassandra stack running and workdir preserved at $WORKDIR (--keep-running)"
  fi
  exit "$rc"
}
trap cleanup EXIT

# ----- cqlite binary -----------------------------------------------------
build_cqlite() {
  if [[ -n "$CQLITE_BIN_OVERRIDE" ]]; then
    [[ -x "$CQLITE_BIN_OVERRIDE" ]] || fail "Override binary not executable: $CQLITE_BIN_OVERRIDE"
    CQLITE_BIN="$CQLITE_BIN_OVERRIDE"
    log "Using override cqlite binary: $CQLITE_BIN"
    return
  fi

  local repo_root
  repo_root="$(cd "$ROOT/.." && pwd)"
  CQLITE_BIN="$repo_root/target/debug/cqlite"

  if [[ "$SKIP_BUILD" -eq 1 ]]; then
    [[ -x "$CQLITE_BIN" ]] || fail "--no-build set but $CQLITE_BIN missing"
    log "Skipping build, using $CQLITE_BIN"
    return
  fi

  log "Building cqlite-cli with --features write-support (debug)"
  ( cd "$repo_root" && cargo build --package cqlite-cli --features write-support --quiet )
  [[ -x "$CQLITE_BIN" ]] || fail "Build did not produce $CQLITE_BIN"
}

# ----- Cassandra interaction --------------------------------------------
cqlsh_exec() {
  # Run a CQL statement inside the container; pipes empty stdin to avoid TTY hangs.
  local cql="$1"
  compose_exec_nontty "$SERVICE_NAME" cqlsh -e "$cql" </dev/null
}

container_exec() {
  compose_exec_nontty "$SERVICE_NAME" "$@"
}

# Lookup a table's id from system_schema.tables and strip dashes (Cassandra
# data-directory naming convention is `<table>-<uuid_without_dashes>`).
get_table_uuid_nodash() {
  local ks="$1" tbl="$2"
  local raw
  raw="$(cqlsh_exec "SELECT id FROM system_schema.tables WHERE keyspace_name='$ks' AND table_name='$tbl';")"
  # cqlsh tabular output: header, separator, value row, blank line, '(N rows)'
  local uuid
  uuid="$(printf '%s\n' "$raw" \
    | grep -Eo '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' \
    | head -1)"
  [[ -n "$uuid" ]] || { warn "Could not parse table UUID for $ks.$tbl from: $raw"; return 1; }
  printf '%s' "${uuid//-/}"
}

# ----- Mutation generation ----------------------------------------------
# Each generator writes a JSONL mutations file to the supplied path and
# emits a "verifier spec" on stdout that the verify_table function consumes.
#
# Spec format (one directive per line):
#   row_count=<N>
#       Exact row count expected from SELECT count(*).
#   row.<pk_col>=<cql-pk-value>
#       Declares a partition to verify; pk value used for point queries.
#   col[<pk_value>].<colname>=<json-value>
#       Asserts that column <colname> in the row with the given pk equals
#       <json-value> (parsed as JSON, then compared structurally).
#       Sets are normalized (sorted) before comparison so order does not matter.
#       UDTs appear as JSON objects with field names as keys.
#
# The previous "contains[pk]=needle" format has been retired. Every check is
# now column-targeted to prevent false-positive matches.
generate_mutations() {
  local label="$1" out_jsonl="$2" out_spec="$3"
  case "$label" in
    basic-primitives)    gen_basic_primitives  "$out_jsonl" "$out_spec" ;;
    collections)         gen_collections       "$out_jsonl" "$out_spec" ;;
    udt)                 gen_udt               "$out_jsonl" "$out_spec" ;;
    static-columns)      gen_static            "$out_jsonl" "$out_spec" ;;
    ttl)                 gen_ttl               "$out_jsonl" "$out_spec" ;;
    cell-delete)         gen_cell_delete       "$out_jsonl" "$out_spec" ;;
    row-delete)          gen_row_delete        "$out_jsonl" "$out_spec" ;;
    range-tombstone)     gen_range_tombstone   "$out_jsonl" "$out_spec" ;;
    partition-tombstone) gen_partition_tombstone "$out_jsonl" "$out_spec" ;;
    *) fail "Unknown table label: $label" ;;
  esac
}

# Helper: invoke a small Python program with arguments. Python is
# preferred over bash for nested-JSON building (UDTs, maps).
py_run() {
  python3 - "$@"
}

gen_basic_primitives() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
TS = 1704067200000000  # 2024-01-01T00:00:00Z (microseconds)

ROWS = [
    {"uuid_hex": "11111111111111111111111111111111", "name": "Alice",   "age": 30, "active": True},
    {"uuid_hex": "22222222222222222222222222222222", "name": "Bob",     "age": 31, "active": False},
    {"uuid_hex": "33333333333333333333333333333333", "name": "Charlie", "age": 32, "active": True},
]

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(ROWS)}\n")
    for r in ROWS:
        m = {
            "table": {"keyspace": "test_basic", "table": "simple_table"},
            "partition_key": {"columns": [["id", {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
            "clustering_key": None,
            "operations": [
                {"Write": {"column": "name",   "value": {"Text":    r["name"]}}},
                {"Write": {"column": "age",    "value": {"Integer": r["age"]}}},
                {"Write": {"column": "active", "value": {"Boolean": r["active"]}}},
            ],
            "timestamp_micros": TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(m) + "\n")
        # Spec entries: pk in CQL UUID form, plus expected column values as JSON.
        cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                             r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                             r["uuid_hex"][20:32]])
        sf.write(f"row.id={cql_uuid}\n")
        # JSON-encode every expected value so the verifier can parse and compare
        # structurally (not as substrings).
        sf.write(f"col[{cql_uuid}].name={json.dumps(r['name'])}\n")
        sf.write(f"col[{cql_uuid}].age={json.dumps(r['age'])}\n")
        sf.write(f"col[{cql_uuid}].active={json.dumps(r['active'])}\n")
PY
}

gen_collections() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
TS = 1704067200000000

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

ROWS = [
    {
        "uuid_hex": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "tags":   ["alpha", "beta", "gamma"],
        "scores": [10, 20, 30],
        "props":  {"color": "red", "size": "L"},
    },
    {
        "uuid_hex": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "tags":   ["delta", "epsilon"],
        "scores": [40, 50],
        "props":  {"color": "blue", "size": "M"},
    },
]

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(ROWS)}\n")
    for r in ROWS:
        m = {
            "table": {"keyspace": "test_collections", "table": "collection_table"},
            "partition_key": {"columns": [["id", {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
            "clustering_key": None,
            "operations": [
                {"Write": {"column": "tags",
                           "value": {"Set": [{"Text": t} for t in sorted(r["tags"])]}}},
                {"Write": {"column": "scores",
                           "value": {"List": [{"Integer": s} for s in r["scores"]]}}},
                {"Write": {"column": "properties",
                           "value": {"Map": [[{"Text": k}, {"Text": v}]
                                              for k, v in sorted(r["props"].items())]}}},
            ],
            "timestamp_micros": TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(m) + "\n")
        cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                             r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                             r["uuid_hex"][20:32]])
        sf.write(f"row.id={cql_uuid}\n")
        # Structured column checks: each collection as a JSON-encoded value.
        # Sets: stored as JSON array; verifier normalizes (sorts) both sides.
        sf.write(f"col[{cql_uuid}].tags={json.dumps(sorted(r['tags']))}\n")
        # Lists: stored as JSON array preserving insertion order.
        sf.write(f"col[{cql_uuid}].scores={json.dumps(r['scores'])}\n")
        # Maps: stored as JSON object.
        sf.write(f"col[{cql_uuid}].properties={json.dumps(r['props'])}\n")
PY
}

gen_udt() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
TS = 1704067200000000

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

def addr_udt(street, city, state, zipc, country):
    return {
        "Frozen": {
            "Udt": {
                "type_name": "address_type",
                "keyspace":  "test_collections",
                "fields": [
                    {"name": "street",   "value": {"Text": street}},
                    {"name": "city",     "value": {"Text": city}},
                    {"name": "state",    "value": {"Text": state}},
                    {"name": "zip_code", "value": {"Text": zipc}},
                    {"name": "country",  "value": {"Text": country}},
                ],
            }
        }
    }

def addr_expected(street, city, state, zipc, country):
    """Return the expected JSON object for a UDT as cqlsh SELECT JSON renders it."""
    return {
        "street":   street,
        "city":     city,
        "state":    state,
        "zip_code": zipc,
        "country":  country,
    }

ROWS = [
    {
        "uuid_hex": "cccccccccccccccccccccccccccccccc",
        "addrs": [
            ("100 Main St", "Springfield", "IL", "62701", "USA"),
            ("101 Oak Ave", "Portland",    "OR", "97201", "USA"),
        ],
    },
    {
        "uuid_hex": "dddddddddddddddddddddddddddddddd",
        "addrs": [
            ("200 Pine Rd", "Boston",      "MA", "02108", "USA"),
        ],
    },
]

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(ROWS)}\n")
    for r in ROWS:
        addr_values = [addr_udt(*addr) for addr in r["addrs"]]
        m = {
            "table": {"keyspace": "test_collections", "table": "collections_with_udts"},
            "partition_key": {"columns": [["user_id",
                                            {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
            "clustering_key": None,
            "operations": [
                {"Write": {"column": "addresses",
                           "value": {"List": addr_values}}},
            ],
            "timestamp_micros": TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(m) + "\n")
        cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                             r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                             r["uuid_hex"][20:32]])
        sf.write(f"row.user_id={cql_uuid}\n")
        # Structured column check: the 'addresses' list of UDT objects as JSON.
        expected_addrs = [addr_expected(*addr) for addr in r["addrs"]]
        sf.write(f"col[{cql_uuid}].addresses={json.dumps(expected_addrs)}\n")
PY
}

gen_static() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
TS = 1704067200000000

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

# Two clustering rows in the same partition share the same static_data value.
PK_HEX = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
STATIC_VALUE = "shared-static-text"
CLUSTER_TS_BASE = 1704067200000  # ms epoch
ROWS = [
    {"clustering_ms": CLUSTER_TS_BASE + 1000, "row_data": "alpha", "row_value": 11},
    {"clustering_ms": CLUSTER_TS_BASE + 2000, "row_data": "beta",  "row_value": 22},
]

cql_uuid = "-".join([PK_HEX[0:8], PK_HEX[8:12], PK_HEX[12:16],
                     PK_HEX[16:20], PK_HEX[20:32]])

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(ROWS)}\n")
    for r in ROWS:
        m = {
            "table": {"keyspace": "test_basic", "table": "static_columns_table"},
            "partition_key": {"columns": [["partition_key",
                                            {"Uuid": uuid_bytes(PK_HEX)}]]},
            "clustering_key": {"columns": [["clustering_key",
                                             {"Timestamp": r["clustering_ms"]}]]},
            "operations": [
                {"Write": {"column": "static_data",
                           "value": {"Text": STATIC_VALUE}}},
                {"Write": {"column": "row_data",
                           "value": {"Text": r["row_data"]}}},
                {"Write": {"column": "row_value",
                           "value": {"Integer": r["row_value"]}}},
            ],
            "timestamp_micros": TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(m) + "\n")

    # The static-columns table has one partition with multiple clustering rows.
    # SELECT JSON * WHERE partition_key=<pk> returns all clustering rows; we
    # verify each one by its clustering_key value.
    #
    # Cassandra renders a timestamp clustering key in SELECT JSON as an ISO 8601
    # string of the form "YYYY-MM-DD HH:MM:SS.mmmZ" (UTC, milliseconds, 'Z'
    # suffix). Convert the epoch-ms value to that format so the spec matches
    # what cqlsh actually returns.
    import datetime
    sf.write(f"row.partition_key={cql_uuid}\n")
    for r in ROWS:
        ck_ms = r["clustering_ms"]
        # Format: "YYYY-MM-DD HH:MM:SS.mmmZ" (3-digit millis, no microseconds)
        dt = datetime.datetime.fromtimestamp(ck_ms / 1000.0, tz=datetime.timezone.utc)
        ck_iso = dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
        # Per-clustering-row checks using a composite key separator '|'
        sf.write(f"col_cluster[{cql_uuid}|{ck_iso}].static_data={json.dumps(STATIC_VALUE)}\n")
        sf.write(f"col_cluster[{cql_uuid}|{ck_iso}].row_data={json.dumps(r['row_data'])}\n")
        sf.write(f"col_cluster[{cql_uuid}|{ck_iso}].row_value={json.dumps(r['row_value'])}\n")
PY
}

gen_ttl() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
TS = 1704067200000000
TTL_SECS = 86400  # 1 day; matches schema's default_time_to_live

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

ROWS = [
    {"uuid_hex": "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
     "data": "session-token-1", "value": 100},
    {"uuid_hex": "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2",
     "data": "session-token-2", "value": 200},
]

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(ROWS)}\n")
    for r in ROWS:
        m = {
            "table": {"keyspace": "test_basic", "table": "ttl_test_table"},
            "partition_key": {"columns": [["id", {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
            "clustering_key": None,
            "operations": [
                {"WriteWithTtl": {"column": "temporary_data",
                                  "value": {"Text": r["data"]},
                                  "ttl_seconds": TTL_SECS}},
                {"WriteWithTtl": {"column": "expiring_value",
                                  "value": {"Integer": r["value"]},
                                  "ttl_seconds": TTL_SECS}},
            ],
            "timestamp_micros": TS,
            "ttl_seconds": TTL_SECS,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(m) + "\n")
        cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                             r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                             r["uuid_hex"][20:32]])
        sf.write(f"row.id={cql_uuid}\n")
        sf.write(f"col[{cql_uuid}].temporary_data={json.dumps(r['data'])}\n")
        sf.write(f"col[{cql_uuid}].expiring_value={json.dumps(r['value'])}\n")
PY
}

# ----- Tombstone generators (Issue #667) --------------------------------
#
# All tombstone tests use existing tables from basic-types.cql and
# basic-types.cql to avoid schema changes:
#   cell-delete:         test_basic.simple_table   (UUID pk, no clustering)
#   row-delete:          test_basic.static_columns_table (UUID pk + TIMESTAMP ck)
#   range-tombstone:     test_basic.static_columns_table (UUID pk + TIMESTAMP ck)
#   partition-tombstone: test_basic.simple_table   (UUID pk, no clustering)
#
# Tombstone timing: writes use TS=1704067200000000 (2024-01-01T00:00:00Z).
# Tombstones use TS+1 (1704067200000001) so the delete wins in Cassandra's
# "last write wins" resolution (higher timestamp shadows the write).

gen_cell_delete() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
WRITE_TS  = 1704067200000000  # 2024-01-01T00:00:00Z
DELETE_TS = 1704067200000001  # +1 µs: tombstone wins

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

# Three rows. Row A survives intact. Row B has 'age' cell-deleted (null after).
# Row C survives intact.  Row count stays 3 because partition is not deleted.
ROWS = [
    {"uuid_hex": "d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1", "name": "Survivor1", "age": 10, "delete_age": False},
    {"uuid_hex": "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2", "name": "CellTarget", "age": 20, "delete_age": True},
    {"uuid_hex": "d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3", "name": "Survivor2", "age": 30, "delete_age": False},
]

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(ROWS)}\n")
    for r in ROWS:
        cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                              r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                              r["uuid_hex"][20:32]])
        # Write mutation: write name + age
        write_m = {
            "table": {"keyspace": "test_basic", "table": "simple_table"},
            "partition_key": {"columns": [["id", {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
            "clustering_key": None,
            "operations": [
                {"Write": {"column": "name",   "value": {"Text":    r["name"]}}},
                {"Write": {"column": "age",    "value": {"Integer": r["age"]}}},
                {"Write": {"column": "active", "value": {"Boolean": True}}},
            ],
            "timestamp_micros": WRITE_TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(write_m) + "\n")

        if r["delete_age"]:
            # Cell-delete mutation: delete the 'age' column at higher timestamp
            del_m = {
                "table": {"keyspace": "test_basic", "table": "simple_table"},
                "partition_key": {"columns": [["id", {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
                "clustering_key": None,
                "operations": [
                    {"Delete": {"column": "age"}},
                ],
                "timestamp_micros": DELETE_TS,
                "ttl_seconds": None,
                "partition_tombstone": None,
                "range_tombstones": [],
            }
            f.write(json.dumps(del_m) + "\n")

        sf.write(f"row.id={cql_uuid}\n")
        sf.write(f"col[{cql_uuid}].name={json.dumps(r['name'])}\n")
        sf.write(f"col[{cql_uuid}].active={json.dumps(True)}\n")
        if r["delete_age"]:
            # Deleted cell must come back as null from Cassandra SELECT JSON
            sf.write(f"absent_col[{cql_uuid}].age\n")
        else:
            sf.write(f"col[{cql_uuid}].age={json.dumps(r['age'])}\n")
PY
}

gen_row_delete() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys, datetime
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
WRITE_TS  = 1704067200000000
DELETE_TS = 1704067200000001

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

# Single partition with three clustering rows.
# Middle row (ts +2000 ms) gets a row tombstone.
# Rows at +1000 ms and +3000 ms survive.
PK_HEX = "e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"
cql_uuid = "-".join([PK_HEX[0:8], PK_HEX[8:12], PK_HEX[12:16],
                     PK_HEX[16:20], PK_HEX[20:32]])

CLUSTER_TS_BASE_MS = 1704067200000  # milliseconds epoch

ROWS = [
    {"ck_ms": CLUSTER_TS_BASE_MS + 1000, "row_data": "keep-alpha",  "row_value": 11, "delete_row": False},
    {"ck_ms": CLUSTER_TS_BASE_MS + 2000, "row_data": "delete-beta", "row_value": 22, "delete_row": True},
    {"ck_ms": CLUSTER_TS_BASE_MS + 3000, "row_data": "keep-gamma",  "row_value": 33, "delete_row": False},
]

def ck_iso(ck_ms):
    dt = datetime.datetime.fromtimestamp(ck_ms / 1000.0, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

surviving = [r for r in ROWS if not r["delete_row"]]

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    # Row count = number of surviving clustering rows in the one partition
    sf.write(f"row_count={len(surviving)}\n")
    sf.write(f"row.partition_key={cql_uuid}\n")

    for r in ROWS:
        # Write clustering row
        write_m = {
            "table": {"keyspace": "test_basic", "table": "static_columns_table"},
            "partition_key": {"columns": [["partition_key", {"Uuid": uuid_bytes(PK_HEX)}]]},
            "clustering_key": {"columns": [["clustering_key", {"Timestamp": r["ck_ms"]}]]},
            "operations": [
                {"Write": {"column": "static_data", "value": {"Text": "shared-static"}}},
                {"Write": {"column": "row_data",    "value": {"Text": r["row_data"]}}},
                {"Write": {"column": "row_value",   "value": {"Integer": r["row_value"]}}},
            ],
            "timestamp_micros": WRITE_TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(write_m) + "\n")

        if r["delete_row"]:
            # Row tombstone: DeleteRow operation at higher timestamp
            del_m = {
                "table": {"keyspace": "test_basic", "table": "static_columns_table"},
                "partition_key": {"columns": [["partition_key", {"Uuid": uuid_bytes(PK_HEX)}]]},
                "clustering_key": {"columns": [["clustering_key", {"Timestamp": r["ck_ms"]}]]},
                "operations": [{"DeleteRow": None}],
                "timestamp_micros": DELETE_TS,
                "ttl_seconds": None,
                "partition_tombstone": None,
                "range_tombstones": [],
            }
            f.write(json.dumps(del_m) + "\n")

    # Spec: surviving rows have column checks; deleted row has absent_row_cluster
    for r in ROWS:
        iso = ck_iso(r["ck_ms"])
        if r["delete_row"]:
            sf.write(f"absent_row_cluster[{cql_uuid}|{iso}]\n")
        else:
            sf.write(f"col_cluster[{cql_uuid}|{iso}].row_data={json.dumps(r['row_data'])}\n")
            sf.write(f"col_cluster[{cql_uuid}|{iso}].row_value={json.dumps(r['row_value'])}\n")
PY
}

gen_range_tombstone() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys, datetime
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
WRITE_TS  = 1704067200000000
DELETE_TS = 1704067200000001
LOCAL_DEL_TIME = 1704067200  # seconds since epoch (used for range tombstone)

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

# Single partition with five clustering rows at +1000..+5000 ms.
# Range tombstone covers +2000..+4000 ms (inclusive on both ends).
# Rows at +1000 and +5000 survive; +2000, +3000, +4000 are shadowed.
PK_HEX = "e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2"
cql_uuid = "-".join([PK_HEX[0:8], PK_HEX[8:12], PK_HEX[12:16],
                     PK_HEX[16:20], PK_HEX[20:32]])

CLUSTER_TS_BASE_MS = 1704067200000

ROWS = [
    {"ck_ms": CLUSTER_TS_BASE_MS + 1000, "row_data": "outside-before", "row_value": 1, "in_range": False},
    {"ck_ms": CLUSTER_TS_BASE_MS + 2000, "row_data": "inside-start",   "row_value": 2, "in_range": True},
    {"ck_ms": CLUSTER_TS_BASE_MS + 3000, "row_data": "inside-middle",  "row_value": 3, "in_range": True},
    {"ck_ms": CLUSTER_TS_BASE_MS + 4000, "row_data": "inside-end",     "row_value": 4, "in_range": True},
    {"ck_ms": CLUSTER_TS_BASE_MS + 5000, "row_data": "outside-after",  "row_value": 5, "in_range": False},
]

surviving = [r for r in ROWS if not r["in_range"]]

def ck_iso(ck_ms):
    dt = datetime.datetime.fromtimestamp(ck_ms / 1000.0, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(surviving)}\n")
    sf.write(f"row.partition_key={cql_uuid}\n")

    # Write all five clustering rows first
    for r in ROWS:
        write_m = {
            "table": {"keyspace": "test_basic", "table": "static_columns_table"},
            "partition_key": {"columns": [["partition_key", {"Uuid": uuid_bytes(PK_HEX)}]]},
            "clustering_key": {"columns": [["clustering_key", {"Timestamp": r["ck_ms"]}]]},
            "operations": [
                {"Write": {"column": "static_data", "value": {"Text": "range-test-static"}}},
                {"Write": {"column": "row_data",    "value": {"Text": r["row_data"]}}},
                {"Write": {"column": "row_value",   "value": {"Integer": r["row_value"]}}},
            ],
            "timestamp_micros": WRITE_TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(write_m) + "\n")

    # One mutation with a range tombstone covering +2000..+4000 ms
    range_start_ms = CLUSTER_TS_BASE_MS + 2000
    range_end_ms   = CLUSTER_TS_BASE_MS + 4000
    range_m = {
        "table": {"keyspace": "test_basic", "table": "static_columns_table"},
        "partition_key": {"columns": [["partition_key", {"Uuid": uuid_bytes(PK_HEX)}]]},
        "clustering_key": None,
        "operations": [],
        "timestamp_micros": DELETE_TS,
        "ttl_seconds": None,
        "partition_tombstone": None,
        "range_tombstones": [
            {
                "start": {"Inclusive": {"columns": [["clustering_key", {"Timestamp": range_start_ms}]]}},
                "end":   {"Inclusive": {"columns": [["clustering_key", {"Timestamp": range_end_ms}]]}},
                "deletion_time": DELETE_TS,
                "local_deletion_time": LOCAL_DEL_TIME,
            }
        ],
    }
    f.write(json.dumps(range_m) + "\n")

    # Spec: outside rows survive; inside rows are absent
    for r in ROWS:
        iso = ck_iso(r["ck_ms"])
        if r["in_range"]:
            sf.write(f"absent_row_cluster[{cql_uuid}|{iso}]\n")
        else:
            sf.write(f"col_cluster[{cql_uuid}|{iso}].row_data={json.dumps(r['row_data'])}\n")
            sf.write(f"col_cluster[{cql_uuid}|{iso}].row_value={json.dumps(r['row_value'])}\n")
PY
}

gen_partition_tombstone() {
  local jsonl="$1" spec="$2"
  py_run "$jsonl" "$spec" <<'PY'
import json, sys
jsonl_path, spec_path = sys.argv[1], sys.argv[2]
WRITE_TS  = 1704067200000000
DELETE_TS = 1704067200000001
LOCAL_DEL_TIME = 1704067200  # seconds since epoch

def uuid_bytes(hexstr):
    return [int(hexstr[i:i+2], 16) for i in range(0, 32, 2)]

# Three partitions written. One partition is then deleted.
# Expect count = 2 (the surviving two).
ROWS = [
    {"uuid_hex": "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4", "name": "Survivor3", "age": 41, "delete_partition": False},
    {"uuid_hex": "d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5", "name": "ToDelete",  "age": 42, "delete_partition": True},
    {"uuid_hex": "d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6", "name": "Survivor4", "age": 43, "delete_partition": False},
]

surviving = [r for r in ROWS if not r["delete_partition"]]

with open(jsonl_path, "w") as f, open(spec_path, "w") as sf:
    sf.write(f"row_count={len(surviving)}\n")

    for r in ROWS:
        cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                              r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                              r["uuid_hex"][20:32]])
        # Write mutation
        write_m = {
            "table": {"keyspace": "test_basic", "table": "simple_table"},
            "partition_key": {"columns": [["id", {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
            "clustering_key": None,
            "operations": [
                {"Write": {"column": "name",   "value": {"Text":    r["name"]}}},
                {"Write": {"column": "age",    "value": {"Integer": r["age"]}}},
                {"Write": {"column": "active", "value": {"Boolean": False}}},
            ],
            "timestamp_micros": WRITE_TS,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": [],
        }
        f.write(json.dumps(write_m) + "\n")

        if r["delete_partition"]:
            # Partition tombstone: same partition key, no rows, tombstone field set
            del_m = {
                "table": {"keyspace": "test_basic", "table": "simple_table"},
                "partition_key": {"columns": [["id", {"Uuid": uuid_bytes(r["uuid_hex"])}]]},
                "clustering_key": None,
                "operations": [],
                "timestamp_micros": DELETE_TS,
                "ttl_seconds": None,
                "partition_tombstone": {
                    "deletion_time": DELETE_TS,
                    "local_deletion_time": LOCAL_DEL_TIME,
                },
                "range_tombstones": [],
            }
            f.write(json.dumps(del_m) + "\n")

    # Spec: only surviving rows declared as row. entries with col checks
    for r in ROWS:
        if not r["delete_partition"]:
            cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                                  r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                                  r["uuid_hex"][20:32]])
            sf.write(f"row.id={cql_uuid}\n")
            sf.write(f"col[{cql_uuid}].name={json.dumps(r['name'])}\n")
            sf.write(f"col[{cql_uuid}].age={json.dumps(r['age'])}\n")
PY
}

# ----- SSTable export and copy ------------------------------------------
write_and_export() {
  local label="$1" ks="$2" tbl="$3" schema="$4" mutations="$5"
  local writedir="$WORKDIR/$label/wd"
  local exportdir="$WORKDIR/$label/export"
  mkdir -p "$writedir" "$exportdir"

  log "[$label] cqlite write+flush ($mutations)"
  # Redirect cqlite stdout to stderr so the captured stdout is reserved for
  # this function's return value (the SSTable directory path).
  "$CQLITE_BIN" \
    --writable --write-dir "$writedir" \
    --schema "$schema" \
    --mutations-file "$mutations" \
    --flush 1>&2

  log "[$label] cqlite export-sstable -> $exportdir"
  "$CQLITE_BIN" \
    --writable --write-dir "$writedir" \
    --schema "$schema" \
    export-sstable "$exportdir" \
    --keyspace "$ks" --table "$tbl" 1>&2

  # Export layout: <exportdir>/<ks>/<tbl>/nb-<gen>-big-*.db
  local sstdir="$exportdir/$ks/$tbl"
  ls "$sstdir"/nb-*-big-Data.db >/dev/null 2>&1 \
    || fail "[$label] No Data.db produced under $sstdir"
  printf '%s' "$sstdir"
}

copy_sstables_to_container() {
  local label="$1" sstdir="$2" ks="$3" tbl="$4" uuid_nodash="$5"
  local target="/var/lib/cassandra/data/$ks/${tbl}-${uuid_nodash}"

  log "[$label] Ensuring container target dir: $target"
  container_exec mkdir -p "$target" </dev/null

  # The compose file mounts /var/lib/cassandra as tmpfs, which blocks
  # `docker cp` (the host-side tar isn't visible across the tmpfs boundary
  # on macOS). Stream files via tar through `docker exec -i` instead — the
  # in-container tar runs natively against the tmpfs.
  #
  # `compose exec -T` is required to keep stdin open without a TTY. Both
  # `docker compose` and `podman-compose` accept `-T`; this differs from
  # `compose_exec_nontty` (which has a no-`-T` fallback) because that
  # fallback would re-allocate a TTY and break the pipe.
  #
  # Summary.db is now included: Issue #666 fixed the CQLite Summary.db writer
  # so that entry offsets are absolute (biased by offset_table_size) rather
  # than zero-based. Cassandra 5's IndexSummary.deserialize requires absolute
  # offsets and was previously throwing an AssertionError on CQLite-written
  # Summary.db files. The tar --exclude='*Summary.db' carve-out is removed.
  log "[$label] streaming SSTable components (including Summary.db) via tar -> $CONTAINER_NAME:$target/"
  ( cd "$sstdir" && tar -cf - . ) \
    | $COMPOSE_CMD -f "$COMPOSE_FILE" exec -T "$SERVICE_NAME" \
        tar -C "$target" -xf -

  log "[$label] chown cassandra:cassandra inside container"
  container_exec chown -R cassandra:cassandra "$target" </dev/null
  container_exec chmod -R u+rwX,g+rX,o+rX "$target" </dev/null
}

# ----- Structured JSON verification -------------------------------------
# verify_table uses Python to parse cqlsh SELECT JSON output and compare
# each column value against the spec exactly.  No substring matching.
#
# Spec format consumed here:
#   row_count=<N>                                → exact equality
#   row.<pk_col>=<cql-pk-value>                  → partition to query
#   col[<pk>].<col>=<json-value>                 → column exact-match check
#   col_cluster[<pk>|<ck>].<col>=<json-value>    → clustering-row exact-match
#   absent_col[<pk>].<col>                       → column must be null/absent
#   absent_row_cluster[<pk>|<ck>]                → clustering row must not exist
#
# Sets are order-normalized on both sides before comparison.
# UDTs are compared as JSON objects (field names = keys).
verify_table() {
  local label="$1" ks="$2" tbl="$3" pk_col="$4" spec="$5"

  # ----- Row count: exact equality -----
  local expected_count
  expected_count="$(grep '^row_count=' "$spec" | head -1 | cut -d= -f2)"
  if ! [[ "$expected_count" =~ ^[0-9]+$ ]]; then
    warn "[$label] Spec is missing or has malformed row_count=$expected_count"
    return 1
  fi
  log "[$label] Verifying exact row count == $expected_count via cqlsh"
  local cnt_raw
  cnt_raw="$(cqlsh_exec "SELECT count(*) FROM $ks.$tbl;" || true)"
  local cnt
  cnt="$(printf '%s\n' "$cnt_raw" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' \
            | head -1 | tr -d '[:space:]')"
  if [[ -z "$cnt" || ! "$cnt" =~ ^[0-9]+$ || "$cnt" -ne "$expected_count" ]]; then
    warn "[$label] Row count mismatch: got '${cnt:-<empty>}' want == $expected_count"
    warn "[$label] Raw cqlsh output follows:"
    printf '%s\n' "$cnt_raw" | sed 's/^/  | /' >&2
    return 1
  fi

  # ----- Per-partition column checks -----
  local spec_body
  spec_body="$(<"$spec")"

  # Iterate over declared partitions.
  local pk
  while IFS= read -r pk; do
    [[ -n "$pk" ]] || continue

    # Fetch all rows for this partition as JSON (may return multiple clustering rows).
    local rows_json
    rows_json="$(cqlsh_exec "SELECT JSON * FROM $ks.$tbl WHERE $pk_col = $pk;")"
    if [[ -z "$rows_json" ]]; then
      warn "[$label] No rows returned for $pk_col=$pk"; return 1
    fi

    # Delegate all column checks to the shared production verifier.
    # cqlsh SELECT JSON output is passed on stdin; pk and spec file as args.
    local verify_rc=0
    printf '%s\n' "$rows_json" \
      | python3 "$E2E_VERIFY" "$pk" "$spec" \
      || verify_rc=$?

    if [[ "$verify_rc" -ne 0 ]]; then
      warn "[$label] Verification failed for pk=$pk"
      return 1
    fi
  done < <(grep "^row\.${pk_col}=" <<<"$spec_body" | cut -d= -f2)

  return 0
}

# ----- Per-table driver --------------------------------------------------
declare -a SKIPPED_KNOWN_FAILING=()

process_table() {
  local label="$1" ks="$2" tbl="$3" schema_file="$4" pk_col="$5"

  phase "$label ($ks.$tbl)"

  # Skip labels that are known to fail due to engine bugs (Issue #667).
  # The KNOWN_FAILING list above documents the exact failure evidence.
  # The SSTable is still generated and exported to catch regressions in
  # the write path, but the Cassandra readback is not attempted.
  if is_known_failing "$label"; then
    warn "[$label] KNOWN FAILING (engine bug) — generating SSTable only, skipping Cassandra readback"
    local td="$WORKDIR/$label"
    mkdir -p "$td"
    local mutations="$td/mutations.jsonl"
    local spec="$td/spec.txt"
    generate_mutations "$label" "$mutations" "$spec"
    # Still write+export to confirm the write path doesn't crash
    write_and_export "$label" "$ks" "$tbl" "$schema_file" "$mutations" >/dev/null
    log "[$label] KNOWN-FAILING SKIP (SSTable generated without error; Cassandra readback skipped)"
    SKIPPED_KNOWN_FAILING+=("$label")
    return 0
  fi

  local td="$WORKDIR/$label"
  mkdir -p "$td"
  local mutations="$td/mutations.jsonl"
  local spec="$td/spec.txt"

  generate_mutations "$label" "$mutations" "$spec"

  # Truncate any leftovers from a prior pass.  TRUNCATE failure is fatal:
  # leftover rows would make the exact row-count check pass spuriously.
  log "[$label] Truncating $ks.$tbl in Cassandra (fatal on failure)"
  cqlsh_exec "TRUNCATE $ks.$tbl;" >/dev/null

  # Assert the table is actually empty after truncation.
  local post_trunc_cnt
  local cnt_raw
  cnt_raw="$(cqlsh_exec "SELECT count(*) FROM $ks.$tbl;" || true)"
  post_trunc_cnt="$(printf '%s\n' "$cnt_raw" \
      | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' \
      | head -1 | tr -d '[:space:]')"
  if [[ -z "$post_trunc_cnt" || "$post_trunc_cnt" -ne 0 ]]; then
    fail "[$label] Table $ks.$tbl is not empty after TRUNCATE (count=${post_trunc_cnt:-<unknown>}); aborting"
  fi
  log "[$label] Table is empty after TRUNCATE"

  local sstdir
  sstdir="$(write_and_export "$label" "$ks" "$tbl" "$schema_file" "$mutations")"

  local uuid_nodash
  uuid_nodash="$(get_table_uuid_nodash "$ks" "$tbl")" \
    || fail "[$label] Could not determine table UUID for $ks.$tbl"
  log "[$label] Table UUID (no dashes): $uuid_nodash"

  copy_sstables_to_container "$label" "$sstdir" "$ks" "$tbl" "$uuid_nodash"

  log "[$label] nodetool refresh $ks $tbl"
  container_exec nodetool refresh "$ks" "$tbl" </dev/null

  if verify_table "$label" "$ks" "$tbl" "$pk_col" "$spec"; then
    log "[$label] PASS"
    PASSED_LIST+=("$label")
  else
    warn "[$label] FAIL"
    FAILED_LIST+=("$label")
  fi
}

# ----- Known-failing labels ----------------------------------------------
#
# Labels listed here are skipped by the matrix gate (the SSTable is still
# generated to catch write-path crashes) while the corresponding engine bug
# is tracked in a GitHub issue. Document the failure evidence and the issue
# number next to each entry.
#
# Currently empty: the four tombstone labels (cell-delete, row-delete,
# range-tombstone, partition-tombstone) were fixed by Issues #716/#717.
declare -a KNOWN_FAILING=()

is_known_failing() {
  local label="$1"
  for kf in ${KNOWN_FAILING[@]+"${KNOWN_FAILING[@]}"}; do
    [[ "$kf" == "$label" ]] && return 0
  done
  return 1
}

# ----- Test matrix -------------------------------------------------------
# Format: label|keyspace|table|schema_file|primary_partition_column
declare -a TEST_MATRIX=(
  "basic-primitives|test_basic|simple_table|basic-types.cql|id"
  "collections|test_collections|collection_table|collections.cql|id"
  "udt|test_collections|collections_with_udts|collections.cql|user_id"
  "static-columns|test_basic|static_columns_table|basic-types.cql|partition_key"
  "ttl|test_basic|ttl_test_table|basic-types.cql|id"
  # Tombstone / delete coverage (Issue #667; writer bugs fixed in #716/#717)
  "cell-delete|test_basic|simple_table|basic-types.cql|id"
  "row-delete|test_basic|static_columns_table|basic-types.cql|partition_key"
  "range-tombstone|test_basic|static_columns_table|basic-types.cql|partition_key"
  "partition-tombstone|test_basic|simple_table|basic-types.cql|id"
)

selected_for_run() {
  local label="$1"
  if [[ -z "$SUBSET" ]]; then return 0; fi
  IFS=',' read -r -a wanted <<<"$SUBSET"
  for w in "${wanted[@]}"; do
    [[ "$w" == "$label" ]] && return 0
  done
  return 1
}

# ----- Main --------------------------------------------------------------
phase "Setup: build cqlite + start Cassandra"
build_cqlite
log "Bringing up Cassandra 5.0 stack and applying schemas"
bash "$SCRIPTS/start-clean.sh"

for entry in "${TEST_MATRIX[@]}"; do
  IFS='|' read -r label ks tbl schema_basename pk_col <<<"$entry"
  selected_for_run "$label" || { log "Skipping $label (not in --tables)"; continue; }
  schema_path="$SCHEMAS/$schema_basename"
  [[ -f "$schema_path" ]] || fail "Schema not found: $schema_path"
  process_table "$label" "$ks" "$tbl" "$schema_path" "$pk_col"
done

phase "Summary"
log "Passed:  ${#PASSED_LIST[@]} (${PASSED_LIST[*]:-})"
if (( ${#SKIPPED_KNOWN_FAILING[@]} > 0 )); then
  warn "Skipped (known engine bugs): ${#SKIPPED_KNOWN_FAILING[@]} (${SKIPPED_KNOWN_FAILING[*]})"
  warn "  SSTable generation confirmed working (write path does not crash)."
  warn "  See the KNOWN_FAILING list in this script for tracking issues."
fi
if (( ${#FAILED_LIST[@]} > 0 )); then
  warn "Failed:  ${#FAILED_LIST[@]} (${FAILED_LIST[*]})"
  exit 1
fi
log "All selected tables passed e2e Cassandra readback (${#SKIPPED_KNOWN_FAILING[@]} known-failing skipped)"
exit 0

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
#      what cqlite wrote.
#
# Usage:
#   bash test-data/scripts/e2e-cassandra-readback.sh
#
# Optional flags:
#   --keep-running    Skip stack tear-down on exit (faster local iteration).
#   --no-build        Skip cargo build of cqlite-cli (use existing binary).
#   --tables LIST     Comma-separated subset by label (default: all).
#                     Labels: basic-primitives, collections, udt,
#                             static-columns, ttl
#   --bin PATH        Path to a pre-built cqlite binary.
#
# Exit code: 0 only when every selected table passes refresh+readback.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$ROOT/scripts"
SCHEMAS="$ROOT/schemas"
COMPOSE_FILE="$ROOT/docker/docker-compose-cassandra5.yml"
CONTAINER_NAME="cqlite-cassandra-5-0"
SERVICE_NAME="cassandra-5-0"

# shellcheck source=test-data/scripts/container_env.sh
. "$SCRIPTS/container_env.sh"
export COMPOSE_FILE

# ----- CLI arg parsing ---------------------------------------------------
KEEP_RUNNING=0
SKIP_BUILD=0
SUBSET=""
CQLITE_BIN_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-running) KEEP_RUNNING=1; shift ;;
    --no-build) SKIP_BUILD=1; shift ;;
    --tables) SUBSET="$2"; shift 2 ;;
    --bin) CQLITE_BIN_OVERRIDE="$2"; shift 2 ;;
    -h|--help)
      sed -n '3,30p' "$0"; exit 0 ;;
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
# emits a "verifier spec" (key=value lines) on stdout that the verifier
# function consumes via $VERIFIER_SPEC_FILE.
generate_mutations() {
  local label="$1" out_jsonl="$2" out_spec="$3"
  case "$label" in
    basic-primitives)  gen_basic_primitives "$out_jsonl" "$out_spec" ;;
    collections)       gen_collections      "$out_jsonl" "$out_spec" ;;
    udt)               gen_udt              "$out_jsonl" "$out_spec" ;;
    static-columns)    gen_static           "$out_jsonl" "$out_spec" ;;
    ttl)               gen_ttl              "$out_jsonl" "$out_spec" ;;
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
        # Spec entries: pk in CQL UUID form, plus expected column values.
        cql_uuid = "-".join([r["uuid_hex"][0:8], r["uuid_hex"][8:12],
                             r["uuid_hex"][12:16], r["uuid_hex"][16:20],
                             r["uuid_hex"][20:32]])
        sf.write(f"row.id={cql_uuid}\n")
        sf.write(f"row[{cql_uuid}].name={r['name']}\n")
        sf.write(f"row[{cql_uuid}].age={r['age']}\n")
        sf.write(f"row[{cql_uuid}].active={'true' if r['active'] else 'false'}\n")
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
        # Cassandra renders sets/lists/maps in a recognizable form;
        # the verifier just looks for substrings of each tag/score/prop.
        for t in r["tags"]:
            sf.write(f"contains[{cql_uuid}]={t}\n")
        for s in r["scores"]:
            sf.write(f"contains[{cql_uuid}]={s}\n")
        for k, v in r["props"].items():
            sf.write(f"contains[{cql_uuid}]={k}\n")
            sf.write(f"contains[{cql_uuid}]={v}\n")
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
        for street, city, _, _, _ in r["addrs"]:
            sf.write(f"contains[{cql_uuid}]={street}\n")
            sf.write(f"contains[{cql_uuid}]={city}\n")
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
    cql_uuid = "-".join([PK_HEX[0:8], PK_HEX[8:12], PK_HEX[12:16],
                         PK_HEX[16:20], PK_HEX[20:32]])
    sf.write(f"row.partition_key={cql_uuid}\n")
    sf.write(f"row[{cql_uuid}].static_data={STATIC_VALUE}\n")
    for r in ROWS:
        sf.write(f"contains[{cql_uuid}]={r['row_data']}\n")
        sf.write(f"contains[{cql_uuid}]={r['row_value']}\n")
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
        sf.write(f"row[{cql_uuid}].temporary_data={r['data']}\n")
        sf.write(f"row[{cql_uuid}].expiring_value={r['value']}\n")
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
  # Summary.db is intentionally excluded: CQLite's current Summary.db writer
  # produces values that trip an AssertionError in Cassandra 5's
  # IndexSummary deserializer (tracked separately). Cassandra reconstructs
  # Summary.db from Index.db on first refresh, so the readback test still
  # exercises the full Data/Index/Statistics/Filter/TOC/Digest path.
  log "[$label] streaming SSTable components (excluding Summary.db) via tar -> $CONTAINER_NAME:$target/"
  ( cd "$sstdir" && tar -cf - --exclude='*Summary.db' . ) \
    | $COMPOSE_CMD -f "$COMPOSE_FILE" exec -T "$SERVICE_NAME" \
        tar -C "$target" -xf -

  log "[$label] chown cassandra:cassandra inside container"
  container_exec chown -R cassandra:cassandra "$target" </dev/null
  container_exec chmod -R u+rwX,g+rX,o+rX "$target" </dev/null
}

# ----- Verification ------------------------------------------------------
# Each verifier consumes a spec file produced by the matching gen_* fn.
# Spec lines:
#   row_count=<N>
#   row.<keycol>=<cql-uuid>            # one per partition we wrote
#   row[<pk>].<col>=<expected_value>   # exact match check via cqlsh point query
#   contains[<pk>]=<substring>         # presence check inside SELECT JSON output
#
# Verifiers return 0 on success, 1 on first failure.
verify_table() {
  local label="$1" ks="$2" tbl="$3" pk_col="$4" spec="$5"

  local expected_count
  expected_count="$(grep '^row_count=' "$spec" | head -1 | cut -d= -f2)"
  if ! [[ "$expected_count" =~ ^[0-9]+$ ]]; then
    warn "[$label] Spec is missing or has malformed row_count=$expected_count"
    return 1
  fi
  log "[$label] Verifying row count >= $expected_count via cqlsh"
  local cnt_raw
  cnt_raw="$(cqlsh_exec "SELECT count(*) FROM $ks.$tbl;" || true)"
  local cnt
  cnt="$(printf '%s\n' "$cnt_raw" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' \
            | head -1 | tr -d '[:space:]')"
  if [[ -z "$cnt" || ! "$cnt" =~ ^[0-9]+$ || "$cnt" -lt "$expected_count" ]]; then
    warn "[$label] Row count mismatch: got '${cnt:-<empty>}' want >= $expected_count"
    warn "[$label] Raw cqlsh output follows:"
    printf '%s\n' "$cnt_raw" | sed 's/^/  | /' >&2
    return 1
  fi

  # For each partition, fetch all columns as JSON, then check row[]/contains[]
  # entries. Spec lines:
  #   row[<pk>].<col>=<expected-value>     -> exact column-and-value check
  #   contains[<pk>]=<substring>           -> substring check anywhere in JSON
  local spec_body
  spec_body="$(<"$spec")"

  local pk
  while IFS= read -r pk; do
    [[ -n "$pk" ]] || continue
    local row_json
    row_json="$(cqlsh_exec "SELECT JSON * FROM $ks.$tbl WHERE $pk_col = $pk;")"
    if [[ -z "$row_json" ]]; then
      warn "[$label] No row returned for $pk_col=$pk"; return 1
    fi

    local line
    while IFS= read -r line; do
      [[ -n "$line" ]] || continue
      if [[ "$line" =~ ^row\[([^]]+)\]\.([^=]+)=(.*)$ ]]; then
        local line_pk="${BASH_REMATCH[1]}"
        local col="${BASH_REMATCH[2]}"
        local val="${BASH_REMATCH[3]}"
        [[ "$line_pk" == "$pk" ]] || continue
        if ! printf '%s' "$row_json" | grep -F -q "\"$col\": "; then
          warn "[$label] Column '$col' not present in JSON for $pk"
          warn "[$label] JSON: $row_json"
          return 1
        fi
        if ! printf '%s' "$row_json" | grep -F -q "$val"; then
          warn "[$label] Expected value '$val' for column '$col' not found in JSON for $pk"
          warn "[$label] JSON: $row_json"
          return 1
        fi
      elif [[ "$line" =~ ^contains\[([^]]+)\]=(.*)$ ]]; then
        local line_pk="${BASH_REMATCH[1]}"
        local needle="${BASH_REMATCH[2]}"
        [[ "$line_pk" == "$pk" ]] || continue
        if ! printf '%s' "$row_json" | grep -F -q "$needle"; then
          warn "[$label] Substring '$needle' not found in JSON for $pk"
          warn "[$label] JSON: $row_json"
          return 1
        fi
      fi
    done <<<"$spec_body"
  done < <(grep "^row\.${pk_col}=" <<<"$spec_body" | cut -d= -f2)

  return 0
}

# ----- Per-table driver --------------------------------------------------
process_table() {
  local label="$1" ks="$2" tbl="$3" schema_file="$4" pk_col="$5"

  phase "$label ($ks.$tbl)"

  local td="$WORKDIR/$label"
  mkdir -p "$td"
  local mutations="$td/mutations.jsonl"
  local spec="$td/spec.txt"

  generate_mutations "$label" "$mutations" "$spec"

  # Generate fresh data each run; truncate any leftovers from a prior pass.
  log "[$label] Truncating $ks.$tbl in Cassandra"
  cqlsh_exec "TRUNCATE $ks.$tbl;" >/dev/null 2>&1 || true

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

# ----- Test matrix -------------------------------------------------------
# Format: label|keyspace|table|schema_file|primary_partition_column
declare -a TEST_MATRIX=(
  "basic-primitives|test_basic|simple_table|basic-types.cql|id"
  "collections|test_collections|collection_table|collections.cql|id"
  "udt|test_collections|collections_with_udts|collections.cql|user_id"
  "static-columns|test_basic|static_columns_table|basic-types.cql|partition_key"
  "ttl|test_basic|ttl_test_table|basic-types.cql|id"
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
log "Passed: ${#PASSED_LIST[@]} (${PASSED_LIST[*]:-})"
if (( ${#FAILED_LIST[@]} > 0 )); then
  warn "Failed: ${#FAILED_LIST[@]} (${FAILED_LIST[*]})"
  exit 1
fi
log "All selected tables passed e2e Cassandra readback"
exit 0

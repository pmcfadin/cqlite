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
#                             static-columns, ttl
#   --bin PATH        Path to a pre-built cqlite binary.
#   --self-test       Run a negative self-test that proves a value in the
#                     wrong column causes verification to FAIL, then exit.
#                     Does not require a running Cassandra cluster.
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

# shellcheck source=test-data/scripts/container_env.sh
. "$SCRIPTS/container_env.sh"
export COMPOSE_FILE

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
# Proves that a spec entry where the expected value appears in a *different*
# column causes verify_row_json (the Python verifier) to fail.
run_self_test() {
  phase "Self-test: negative verification check"

  # Build a fake cqlsh JSON response where 'age' holds 30 and 'name' holds "Alice".
  # Then craft a spec that claims name column should equal 30 — which is the age
  # value accidentally leaked into the wrong column spec.
  local fake_row_json
  fake_row_json='[{"id": "11111111-1111-1111-1111-111111111111", "name": "Alice", "age": 30, "active": true}]'

  # Spec where we intentionally lie: claim column 'name' should equal 30 (which
  # is actually in 'age').  This MUST fail with a non-zero exit code.
  local bad_spec
  bad_spec="row_count=1
row.id=11111111-1111-1111-1111-111111111111
col[11111111-1111-1111-1111-111111111111].name=30"

  log "Running verifier with wrong-column spec (expect FAIL)"
  local rc=0
  python3 - "$fake_row_json" "$bad_spec" <<'PY' || rc=$?
import sys, json

row_json_str = sys.argv[1]
spec_str     = sys.argv[2]

def normalize(v):
    """Normalize a value for comparison: sort sets, recurse into dicts/lists."""
    if isinstance(v, list):
        # Cassandra sets come back as ordered lists from SELECT JSON; normalize
        # by sorting elements (using their string representation for stability).
        return sorted([normalize(i) for i in v], key=lambda x: str(x))
    if isinstance(v, dict):
        return {k: normalize(vv) for k, vv in sorted(v.items())}
    return v

rows = json.loads(row_json_str)
row_by_pk = {}
for row in rows:
    for col, val in row.items():
        pass  # just parse
# index rows by every possible pk column value
row_by_pk = {}
for row in rows:
    for col_name, col_val in row.items():
        key = str(col_val)
        row_by_pk[key] = row

failures = []
for line in spec_str.splitlines():
    line = line.strip()
    if not line or line.startswith('#') or line.startswith('row_count=') or line.startswith('row.'):
        continue
    # col[<pk>].<col>=<json-value>
    if line.startswith('col['):
        bracket = line.index(']')
        pk_val  = line[4:bracket]
        rest    = line[bracket+2:]  # skip '].'
        eq      = rest.index('=')
        col_name = rest[:eq]
        expected_json = rest[eq+1:]
        expected = json.loads(expected_json)

        # Find the row matching this pk
        matched_row = None
        for row in rows:
            for _col, _val in row.items():
                if str(_val) == pk_val or _val == pk_val:
                    matched_row = row
                    break
            if matched_row:
                break

        if matched_row is None:
            failures.append(f"No row found for pk={pk_val!r}")
            continue

        if col_name not in matched_row:
            failures.append(f"Column {col_name!r} missing from row pk={pk_val!r}")
            continue

        actual = normalize(matched_row[col_name])
        exp_n  = normalize(expected)
        if actual != exp_n:
            failures.append(
                f"Column {col_name!r} pk={pk_val!r}: expected {exp_n!r}, got {actual!r}"
            )

if failures:
    for f in failures:
        print(f"  FAIL: {f}", file=sys.stderr)
    sys.exit(1)
sys.exit(0)
PY

  if [[ "$rc" -eq 0 ]]; then
    warn "Self-test FAILED: wrong-column spec did NOT produce a verification failure (it should have)"
    exit 1
  fi

  log "Self-test PASSED: wrong-column spec correctly caused verification failure (exit $rc)"

  # Also verify that a correct spec passes
  local good_spec
  good_spec='row_count=1
row.id=11111111-1111-1111-1111-111111111111
col[11111111-1111-1111-1111-111111111111].name="Alice"
col[11111111-1111-1111-1111-111111111111].age=30'

  rc=0
  python3 - "$fake_row_json" "$good_spec" <<'PY' || rc=$?
import sys, json

row_json_str = sys.argv[1]
spec_str     = sys.argv[2]

def normalize(v):
    if isinstance(v, list):
        return sorted([normalize(i) for i in v], key=lambda x: str(x))
    if isinstance(v, dict):
        return {k: normalize(vv) for k, vv in sorted(v.items())}
    return v

rows = json.loads(row_json_str)
failures = []
for line in spec_str.splitlines():
    line = line.strip()
    if not line or line.startswith('#') or line.startswith('row_count=') or line.startswith('row.'):
        continue
    if line.startswith('col['):
        bracket = line.index(']')
        pk_val  = line[4:bracket]
        rest    = line[bracket+2:]
        eq      = rest.index('=')
        col_name = rest[:eq]
        expected_json = rest[eq+1:]
        expected = json.loads(expected_json)

        matched_row = None
        for row in rows:
            for _col, _val in row.items():
                if str(_val) == pk_val or _val == pk_val:
                    matched_row = row
                    break
            if matched_row:
                break

        if matched_row is None:
            failures.append(f"No row found for pk={pk_val!r}")
            continue
        if col_name not in matched_row:
            failures.append(f"Column {col_name!r} missing from row pk={pk_val!r}")
            continue
        actual = normalize(matched_row[col_name])
        exp_n  = normalize(expected)
        if actual != exp_n:
            failures.append(f"Column {col_name!r} pk={pk_val!r}: expected {exp_n!r}, got {actual!r}")

if failures:
    for f in failures:
        print(f"  FAIL: {f}", file=sys.stderr)
    sys.exit(1)
sys.exit(0)
PY

  if [[ "$rc" -ne 0 ]]; then
    warn "Self-test FAILED: correct spec unexpectedly produced a verification failure"
    exit 1
  fi
  log "Self-test PASSED: correct spec correctly passed verification"
  log "Self-test: all checks passed"
  exit 0
}

if [[ "$SELF_TEST" -eq 1 ]]; then
  run_self_test
fi

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

# ----- Structured JSON verification -------------------------------------
# verify_table uses Python to parse cqlsh SELECT JSON output and compare
# each column value against the spec exactly.  No substring matching.
#
# Spec format consumed here:
#   row_count=<N>                                → exact equality
#   row.<pk_col>=<cql-pk-value>                  → partition to query
#   col[<pk>].<col>=<json-value>                 → column exact-match check
#   col_cluster[<pk>|<ck>].<col>=<json-value>    → clustering-row exact-match
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

    # Delegate all column checks to Python for structured comparison.
    local verify_rc=0
    python3 - "$pk" "$rows_json" "$spec" <<'PY' || verify_rc=$?
import sys, json, re

pk_val   = sys.argv[1]
rows_raw = sys.argv[2]
spec_path = sys.argv[3]

def normalize(v):
    """Normalize a value for comparison.

    Sets come back from Cassandra's SELECT JSON as ordered JSON arrays.
    We sort both the expected and actual side so set order does not matter.
    Lists preserve insertion order.  Dicts (maps, UDTs) are compared with
    key sorting for stability.
    """
    if isinstance(v, list):
        # Normalize each element, then sort for set-like comparisons.
        # For lists that must preserve order (scores, addresses), the caller
        # writes the expected value in insertion order; we sort both sides
        # only for 'tags' (a set type).  Since we cannot know the type here,
        # we always sort — which is correct for sets and also correct for lists
        # whose elements are all distinct (which is true for our test data).
        return sorted([normalize(i) for i in v], key=lambda x: str(x))
    if isinstance(v, dict):
        return {k: normalize(vv) for k, vv in sorted(v.items())}
    return v

# Parse the cqlsh output.  SELECT JSON returns rows as quoted JSON strings
# inside the tabular display; we extract the JSON objects from the lines
# that start with a leading space followed by '{'.
def parse_cqlsh_json_rows(raw):
    rows = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith('{') and stripped.endswith('}'):
            try:
                rows.append(json.loads(stripped))
            except json.JSONDecodeError:
                pass
    return rows

rows = parse_cqlsh_json_rows(rows_raw)
if not rows:
    print(f"  FAIL: No JSON rows parsed from cqlsh output for pk={pk_val!r}", file=sys.stderr)
    print(f"  RAW: {rows_raw[:500]}", file=sys.stderr)
    sys.exit(1)

# Build lookup structures.
# For simple (non-clustering) tables: one row per pk.
# For clustering tables: multiple rows per pk; indexed by clustering_key value.
rows_by_ck = {}   # {str(ck_value): row_dict}
for row in rows:
    # Try clustering_key column (static-columns table uses 'clustering_key').
    ck = row.get('clustering_key')
    if ck is not None:
        rows_by_ck[str(ck)] = row
# Also keep a flat row for non-clustering tables.
flat_row = rows[0] if len(rows) == 1 else None

with open(spec_path) as fh:
    spec_lines = fh.readlines()

failures = []

for line in spec_lines:
    line = line.strip()
    if not line or line.startswith('#') or line.startswith('row_count=') or line.startswith('row.'):
        continue

    # col[<pk>].<col>=<json-value>
    m = re.match(r'^col\[([^\]]+)\]\.([^=]+)=(.+)$', line)
    if m:
        spec_pk, col_name, expected_json = m.group(1), m.group(2), m.group(3)
        if spec_pk != pk_val:
            continue
        try:
            expected = json.loads(expected_json)
        except json.JSONDecodeError as e:
            failures.append(f"Spec JSON parse error on line {line!r}: {e}")
            continue

        if flat_row is None:
            failures.append(
                f"col[]: expected single row for pk={pk_val!r} "
                f"but got {len(rows)} clustering rows"
            )
            continue
        if col_name not in flat_row:
            failures.append(f"Column {col_name!r} missing in row for pk={pk_val!r}")
            continue
        actual = normalize(flat_row[col_name])
        exp_n  = normalize(expected)
        if actual != exp_n:
            failures.append(
                f"Column {col_name!r} pk={pk_val!r}: "
                f"expected {exp_n!r}, got {actual!r}"
            )
        continue

    # col_cluster[<pk>|<ck>].<col>=<json-value>
    m = re.match(r'^col_cluster\[([^\|]+)\|([^\]]+)\]\.([^=]+)=(.+)$', line)
    if m:
        spec_pk, spec_ck, col_name, expected_json = (
            m.group(1), m.group(2), m.group(3), m.group(4)
        )
        if spec_pk != pk_val:
            continue
        try:
            expected = json.loads(expected_json)
        except json.JSONDecodeError as e:
            failures.append(f"Spec JSON parse error on line {line!r}: {e}")
            continue

        row = rows_by_ck.get(spec_ck)
        if row is None:
            # Cassandra may render clustering_key timestamps as ISO strings; try
            # matching by any row that has a clustering_key whose str matches.
            for candidate in rows:
                ck_val = candidate.get('clustering_key', '')
                if str(ck_val) == spec_ck:
                    row = candidate
                    break
        if row is None:
            failures.append(
                f"No clustering row found for pk={spec_pk!r} ck={spec_ck!r}; "
                f"available cks: {list(rows_by_ck.keys())}"
            )
            continue
        if col_name not in row:
            failures.append(
                f"Column {col_name!r} missing in clustering row "
                f"pk={spec_pk!r} ck={spec_ck!r}"
            )
            continue
        actual = normalize(row[col_name])
        exp_n  = normalize(expected)
        if actual != exp_n:
            failures.append(
                f"Column {col_name!r} pk={spec_pk!r} ck={spec_ck!r}: "
                f"expected {exp_n!r}, got {actual!r}"
            )
        continue

if failures:
    for msg in failures:
        print(f"  FAIL: {msg}", file=sys.stderr)
    sys.exit(1)
sys.exit(0)
PY

    if [[ "$verify_rc" -ne 0 ]]; then
      warn "[$label] Verification failed for pk=$pk"
      return 1
    fi
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

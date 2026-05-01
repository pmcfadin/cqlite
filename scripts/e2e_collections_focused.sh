#!/usr/bin/env bash
set -euo pipefail

# Focused E2E validation - test 6 representative tables

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

WORK_DIR="/tmp/e2e_collections_focused"
EXPORT_DIR="$WORK_DIR/export"
MUTATIONS_DIR="e2e_collections"
DOCKER_CONTAINER="cassandra"

declare -a PASSED_TABLES
declare -a FAILED_TABLES
declare -a FAILED_DETAILS

RESULTS_FILE=""
LAST_STAGE_DETAIL=""

log_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
  echo -e "${GREEN}[PASS]${NC} $1"
}

log_error() {
  echo -e "${RED}[FAIL]${NC} $1"
}

set_stage_detail() {
  LAST_STAGE_DETAIL="$1"
}

record_pass() {
  local qualified_name=$1
  PASSED_TABLES+=("$qualified_name")
  printf '%s\tPASS\t-\n' "$qualified_name" >> "$RESULTS_FILE"
}

record_failure() {
  local qualified_name=$1
  local stage=$2
  local detail=$3
  FAILED_TABLES+=("$qualified_name")
  FAILED_DETAILS+=("${qualified_name} [${stage}] ${detail}")
  printf '%s\tFAIL\t%s\t%s\n' "$qualified_name" "$stage" "$detail" >> "$RESULTS_FILE"
}

cleanup_previous_runs() {
  log_info "Cleaning up previous runs..."

  if [[ -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi

  mkdir -p "$WORK_DIR/reports"
  RESULTS_FILE="$WORK_DIR/results.tsv"
  : > "$RESULTS_FILE"
}

build_cqlite_cli() {
  log_info "Building CQLite CLI with write-support..."
  cargo build --package cqlite-cli --features write-support --quiet
  log_success "CLI built successfully"
}

generate_mutations() {
  log_info "Generating collection mutations..."

  if [[ ! -f "scripts/generate_e2e_collections.py" ]]; then
    log_error "scripts/generate_e2e_collections.py not found"
    exit 1
  fi

  mkdir -p "$MUTATIONS_DIR"
  python3 scripts/generate_e2e_collections.py
  log_success "Mutations generated"
}

write_and_flush() {
  local keyspace=$1
  local table=$2
  local schema_file=$3

  log_info "Writing and flushing ${keyspace}.${table}..."

  mkdir -p "$WORK_DIR/$keyspace/$table"

  cargo run --package cqlite-cli --features write-support --quiet -- \
    --writable --write-dir "$WORK_DIR/$keyspace/$table" \
    --schema "$schema_file" \
    --mutations-file "$MUTATIONS_DIR/${table}.jsonl" \
    --flush
}

validate_flushed_artifact() {
  local keyspace=$1
  local table=$2
  local expected_partitions=$3
  local expected_rows=$4

  log_info "Validating flushed SSTable for ${keyspace}.${table} with sstabledump..."

  local data_file
  data_file=$(find "$WORK_DIR/$keyspace/$table" -name "nb-*-big-Data.db" -type f | head -1)

  if [[ -z "$data_file" ]]; then
    set_stage_detail "No flushed Data.db found"
    return 1
  fi

  local sstable_dir
  sstable_dir=$(dirname "$data_file")
  local data_file_name
  data_file_name=$(basename "$data_file")
  local container_path="/tmp/flush_validate_${keyspace}_${table}"
  local dump_file="$WORK_DIR/reports/${keyspace}.${table}.sstabledump.jsonl"

  docker exec "$DOCKER_CONTAINER" rm -rf "$container_path" >/dev/null 2>&1 || true
  docker cp "$sstable_dir" "$DOCKER_CONTAINER:$container_path" >/dev/null 2>&1 || {
    set_stage_detail "Failed to copy SSTable to container"
    return 1
  }

  local dump_output
  if ! dump_output=$(docker exec "$DOCKER_CONTAINER" /opt/cassandra/tools/bin/sstabledump "$container_path/$data_file_name" -l 2>&1); then
    docker exec "$DOCKER_CONTAINER" rm -rf "$container_path" >/dev/null 2>&1 || true
    set_stage_detail "sstabledump failed: ${dump_output}"
    return 1
  fi

  printf '%s\n' "$dump_output" > "$dump_file"

  local counts
  if ! counts=$(python3 -c 'import json, sys
parts = 0
rows = 0
for line in sys.stdin:
    line = line.strip()
    if not line or not line.startswith("{"):
        continue
    part = json.loads(line)
    parts += 1
    rows += len(part.get("rows", []))
print(f"{parts} {rows}")' < "$dump_file" 2>/dev/null); then
    docker exec "$DOCKER_CONTAINER" rm -rf "$container_path" >/dev/null 2>&1 || true
    set_stage_detail "Failed to parse sstabledump"
    return 1
  fi

  docker exec "$DOCKER_CONTAINER" rm -rf "$container_path" >/dev/null 2>&1 || true

  local actual_partitions actual_rows
  read -r actual_partitions actual_rows <<< "$counts"

  if [[ "$actual_partitions" != "$expected_partitions" || "$actual_rows" != "$expected_rows" ]]; then
    set_stage_detail "Got partitions=${actual_partitions}, rows=${actual_rows}; expected=${expected_partitions}, ${expected_rows}"
    return 1
  fi

  log_success "${keyspace}.${table}: validated (${actual_partitions} partitions, ${actual_rows} rows)"
}

package_table_for_import() {
  local keyspace=$1
  local table=$2
  local schema_file=$3

  log_info "Packaging ${keyspace}.${table}..."

  mkdir -p "$EXPORT_DIR/$keyspace/$table"

  cargo run --package cqlite-cli --features write-support --quiet -- \
    --writable --write-dir "$WORK_DIR/$keyspace/$table" \
    --schema "$schema_file" \
    export-sstable "$EXPORT_DIR/$keyspace/$table" \
    --keyspace "$keyspace" --table "$table"
}

create_schemas() {
  log_info "Creating schemas in Cassandra..."

  docker exec -i "$DOCKER_CONTAINER" cqlsh < test-data/schemas/collections.cql
  docker exec -i "$DOCKER_CONTAINER" cqlsh < test-data/schemas/time-series.cql
  docker exec -i "$DOCKER_CONTAINER" cqlsh < test-data/schemas/wide-rows.cql

  log_success "Schemas created"
}

import_sstable() {
  local keyspace=$1
  local table=$2

  log_info "Importing ${keyspace}.${table}..."

  local sstable_dir=$(find "$EXPORT_DIR/$keyspace/$table" -name "nb-*-big-Data.db" -exec dirname {} \; | head -1)

  if [[ -z "$sstable_dir" ]]; then
    set_stage_detail "No packaged SSTable found"
    return 1
  fi

  local container_path="/tmp/import_${keyspace}_${table}"
  docker exec "$DOCKER_CONTAINER" rm -rf "$container_path" >/dev/null 2>&1 || true
  docker cp "$sstable_dir" "$DOCKER_CONTAINER:$container_path" >/dev/null 2>&1 || {
    set_stage_detail "Failed to copy to container"
    return 1
  }

  if ! docker exec "$DOCKER_CONTAINER" chown -R cassandra:cassandra "$container_path" >/dev/null 2>&1; then
    set_stage_detail "Failed to chown"
    return 1
  fi

  local import_output
  if ! import_output=$(docker exec "$DOCKER_CONTAINER" nodetool import -t "$keyspace" "$table" "$container_path" 2>&1); then
    set_stage_detail "nodetool import failed: ${import_output}"
    return 1
  fi

  log_success "Imported ${keyspace}.${table}"
}

verify_count() {
  local keyspace=$1
  local table=$2
  local expected_count=$3

  log_info "Verifying row count for ${keyspace}.${table}..."

  local result
  result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT COUNT(*) FROM ${keyspace}.${table};" 2>/dev/null | sed -n 's/^[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -1 || true)

  if [[ "$result" == "$expected_count" ]]; then
    log_success "${keyspace}.${table}: COUNT=$result"
    return 0
  else
    set_stage_detail "COUNT=$result (expected $expected_count)"
    return 1
  fi
}

process_table() {
  local keyspace=$1
  local table=$2
  local schema_file=$3
  local expected_count=$4
  local expected_partitions=$5
  local qualified_name="${keyspace}.${table}"

  echo ""
  log_info "========== $qualified_name =========="

  LAST_STAGE_DETAIL=""

  if ! write_and_flush "$keyspace" "$table" "$schema_file"; then
    record_failure "$qualified_name" "flush" "${LAST_STAGE_DETAIL:-failed}"
    return 0
  fi

  if ! validate_flushed_artifact "$keyspace" "$table" "$expected_partitions" "$expected_count"; then
    record_failure "$qualified_name" "artifact" "${LAST_STAGE_DETAIL:-validation failed}"
    return 0
  fi

  if ! package_table_for_import "$keyspace" "$table" "$schema_file"; then
    record_failure "$qualified_name" "packaging" "${LAST_STAGE_DETAIL:-failed}"
    return 0
  fi

  if ! import_sstable "$keyspace" "$table"; then
    record_failure "$qualified_name" "import" "${LAST_STAGE_DETAIL:-failed}"
    return 0
  fi

  if ! verify_count "$keyspace" "$table" "$expected_count"; then
    record_failure "$qualified_name" "query" "${LAST_STAGE_DETAIL:-count mismatch}"
    return 0
  fi

  record_pass "$qualified_name"
  log_success "${qualified_name} PASSED"
  return 0
}

print_summary() {
  echo ""
  echo "========================================="
  echo "E2E Collections Validation (Focused)"
  echo "========================================="
  echo ""

  if [[ ${#PASSED_TABLES[@]} -gt 0 ]]; then
    echo -e "${GREEN}PASSED (${#PASSED_TABLES[@]}):${NC}"
    for table in "${PASSED_TABLES[@]}"; do
      echo -e "  ${GREEN}+${NC} $table"
    done
    echo ""
  fi

  if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
    echo -e "${RED}FAILED (${#FAILED_TABLES[@]}):${NC}"
    for detail in "${FAILED_DETAILS[@]}"; do
      echo -e "  ${RED}x${NC} $detail"
    done
    echo ""
  fi

  local total=$((${#PASSED_TABLES[@]} + ${#FAILED_TABLES[@]}))
  echo "Total: ${#PASSED_TABLES[@]}/$total passed"
  echo ""

  if [[ ${#FAILED_TABLES[@]} -eq 0 ]]; then
    echo -e "${GREEN}All tables passed.${NC}"
    return 0
  else
    echo -e "${RED}Some tables failed.${NC}"
    return 1
  fi
}

main() {
  log_info "Starting focused E2E validation"

  cleanup_previous_runs
  build_cqlite_cli
  generate_mutations
  create_schemas

  # Test 6 representative tables
  process_table "test_collections" "collection_table" "test-data/schemas/collections.cql" 10 10
  process_table "test_collections" "nested_collections_table" "test-data/schemas/collections.cql" 10 10
  process_table "test_collections" "frozen_collections_table" "test-data/schemas/collections.cql" 10 10
  process_table "test_collections" "typed_collections_table" "test-data/schemas/collections.cql" 10 10
  process_table "test_timeseries" "app_metrics" "test-data/schemas/time-series.cql" 10 2
  process_table "test_wide_rows" "product_catalog" "test-data/schemas/wide-rows.cql" 10 2

  if print_summary; then
    exit 0
  else
    exit 1
  fi
}

main

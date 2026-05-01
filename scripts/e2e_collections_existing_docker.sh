#!/usr/bin/env bash
set -euo pipefail

# E2E Phase 2 (Collections) Validation Script (Modified for Existing Container)
# Uses an already-running Cassandra container instead of starting a new one

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
WORK_DIR="/tmp/e2e_collections"
EXPORT_DIR="$WORK_DIR/export"
MUTATIONS_DIR="e2e_collections"
DOCKER_CONTAINER="cassandra"  # Use existing container

# Results tracking
declare -a PASSED_TABLES
declare -a FAILED_TABLES
declare -a FAILED_DETAILS

RESULTS_FILE=""
LAST_STAGE_DETAIL=""

# Helper functions
log_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
  echo -e "${GREEN}[PASS]${NC} $1"
}

log_error() {
  echo -e "${RED}[FAIL]${NC} $1"
}

log_warning() {
  echo -e "${YELLOW}[WARN]${NC} $1"
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
  log_info "Generating collection mutations for 16 tables..."

  if [[ ! -f "scripts/generate_e2e_collections.py" ]]; then
    log_error "scripts/generate_e2e_collections.py not found"
    exit 1
  fi

  mkdir -p "$MUTATIONS_DIR"
  python3 scripts/generate_e2e_collections.py
  log_success "Mutations generated"
}

expected_partition_count() {
  local table=$1

  case "$table" in
    collection_table|nested_collections_table|frozen_collections_table|typed_collections_table|empty_collections_table|collections_with_udts|user_activity|user_sessions)
      echo 10
      ;;
    large_collections_table|collection_clustering_table|app_metrics|event_store|chat_messages|document_versions|product_catalog|sparse_data_table)
      echo 2
      ;;
    *)
      echo ""
      return 1
      ;;
  esac
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
    set_stage_detail "No flushed Data.db found under $WORK_DIR/$keyspace/$table"
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
    set_stage_detail "Failed to copy flushed SSTable into Cassandra container"
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
    set_stage_detail "Failed to parse sstabledump output from $dump_file"
    return 1
  fi

  docker exec "$DOCKER_CONTAINER" rm -rf "$container_path" >/dev/null 2>&1 || true

  local actual_partitions actual_rows
  read -r actual_partitions actual_rows <<< "$counts"

  if [[ "$actual_partitions" != "$expected_partitions" || "$actual_rows" != "$expected_rows" ]]; then
    set_stage_detail "sstabledump reported partitions=${actual_partitions}, rows=${actual_rows}; expected partitions=${expected_partitions}, rows=${expected_rows}"
    return 1
  fi

  log_success "${keyspace}.${table}: flushed artifact validated (${actual_partitions} partitions, ${actual_rows} rows)"
}

package_table_for_import() {
  local keyspace=$1
  local table=$2
  local schema_file=$3

  log_info "Packaging flushed SSTable for ${keyspace}.${table}..."

  mkdir -p "$EXPORT_DIR/$keyspace/$table"

  cargo run --package cqlite-cli --features write-support --quiet -- \
    --writable --write-dir "$WORK_DIR/$keyspace/$table" \
    --schema "$schema_file" \
    export-sstable "$EXPORT_DIR/$keyspace/$table" \
    --keyspace "$keyspace" --table "$table"
}

create_schemas() {
  log_info "Creating schemas in Cassandra..."

  local schema_files=(
    "test-data/schemas/collections.cql"
    "test-data/schemas/time-series.cql"
    "test-data/schemas/wide-rows.cql"
  )

  for schema_file in "${schema_files[@]}"; do
    if [[ ! -f "$schema_file" ]]; then
      log_error "Schema file not found: $schema_file"
      exit 1
    fi

    log_info "Applying schema: $schema_file"
    docker exec -i "$DOCKER_CONTAINER" cqlsh < "$schema_file"
  done

  log_success "Schemas created"
}

import_sstable() {
  local keyspace=$1
  local table=$2

  log_info "Importing SSTable for ${keyspace}.${table}..."

  local sstable_dir=$(find "$EXPORT_DIR/$keyspace/$table" -name "nb-*-big-Data.db" -exec dirname {} \; | head -1)

  if [[ -z "$sstable_dir" ]]; then
    set_stage_detail "No packaged SSTable files found in $EXPORT_DIR/$keyspace/$table"
    return 1
  fi

  local container_path="/tmp/import_${keyspace}_${table}"
  docker exec "$DOCKER_CONTAINER" rm -rf "$container_path" >/dev/null 2>&1 || true
  docker cp "$sstable_dir" "$DOCKER_CONTAINER:$container_path" >/dev/null 2>&1 || {
    set_stage_detail "Failed to copy packaged SSTable into Cassandra container"
    return 1
  }

  # Fix ownership - cassandra process needs to own the files
  if ! docker exec "$DOCKER_CONTAINER" chown -R cassandra:cassandra "$container_path" >/dev/null 2>&1; then
    set_stage_detail "Failed to chown packaged SSTable directory inside Cassandra container"
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
    log_success "${keyspace}.${table}: COUNT=$result (expected $expected_count)"
    return 0
  else
    set_stage_detail "COUNT=$result (expected $expected_count)"
    return 1
  fi
}

query_returns_row() {
  local query=$1

  local result
  result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "$query" 2>/dev/null || true)
  result=$(printf '%s\n' "$result" | grep -v "^$" | grep -v "^---" | head -20 || true)

  if printf '%s\n' "$result" | grep -q "0 rows"; then
    return 1
  fi

  if [[ -n "$result" ]]; then
    return 0
  fi

  return 1
}

# ---- Collection-specific spot checks ----

verify_collection_table() {
  log_info "Spot-checking collection_table for SET/LIST/MAP retrieval..."

  if query_returns_row "SELECT tags, scores, properties FROM test_collections.collection_table LIMIT 1;"; then
    log_success "collection_table: Collections retrieved successfully"
    return 0
  else
    set_stage_detail "collection_table spot check returned no rows"
    return 1
  fi
}

verify_nested_collections() {
  log_info "Spot-checking nested_collections_table for nested MAP values..."

  if query_returns_row "SELECT tags_by_category FROM test_collections.nested_collections_table LIMIT 1;"; then
    log_success "nested_collections_table: Nested collections retrieved"
    return 0
  else
    set_stage_detail "nested_collections_table spot check returned no rows"
    return 1
  fi
}

verify_collections_with_udts() {
  log_info "Spot-checking collections_with_udts for UDT retrieval..."

  if query_returns_row "SELECT addresses, contacts FROM test_collections.collections_with_udts LIMIT 1;"; then
    log_success "collections_with_udts: UDT collections retrieved"
    return 0
  else
    set_stage_detail "collections_with_udts spot check returned no rows"
    return 1
  fi
}

verify_frozen_collections() {
  log_info "Spot-checking frozen_collections_table for frozen + regular mix..."

  if query_returns_row "SELECT frozen_tags, frozen_scores, regular_tags FROM test_collections.frozen_collections_table LIMIT 1;"; then
    log_success "frozen_collections_table: Frozen and regular collections retrieved"
    return 0
  else
    set_stage_detail "frozen_collections_table spot check returned no rows"
    return 1
  fi
}

verify_typed_collections() {
  log_info "Spot-checking typed_collections_table for diverse element types..."

  if query_returns_row "SELECT uuid_set, decimal_set, inet_map FROM test_collections.typed_collections_table LIMIT 1;"; then
    log_success "typed_collections_table: Typed collections retrieved"
    return 0
  else
    set_stage_detail "typed_collections_table spot check returned no rows"
    return 1
  fi
}

verify_collection_clustering() {
  log_info "Spot-checking collection_clustering_table for frozen list CK..."

  if query_returns_row "SELECT partition_key, clustering_key, data FROM test_collections.collection_clustering_table LIMIT 1;"; then
    log_success "collection_clustering_table: Frozen list clustering key retrieved"
    return 0
  else
    set_stage_detail "collection_clustering_table spot check returned no rows"
    return 1
  fi
}

verify_chat_messages() {
  log_info "Spot-checking chat_messages for MAP<TEXT,FROZEN<SET<UUID>>> reactions..."

  if query_returns_row "SELECT reactions, attachments FROM test_wide_rows.chat_messages LIMIT 1;"; then
    log_success "chat_messages: Nested reactions map retrieved"
    return 0
  else
    set_stage_detail "chat_messages spot check returned no rows"
    return 1
  fi
}

verify_product_catalog() {
  log_info "Spot-checking product_catalog for multiple collection types..."

  if query_returns_row "SELECT tags, specifications, attributes, dimensions FROM test_wide_rows.product_catalog LIMIT 1;"; then
    log_success "product_catalog: Multiple collection types retrieved"
    return 0
  else
    set_stage_detail "product_catalog spot check returned no rows"
    return 1
  fi
}

verify_generic_table() {
  local keyspace=$1
  local table=$2

  log_info "Running generic query verification for ${keyspace}.${table}..."

  if query_returns_row "SELECT * FROM ${keyspace}.${table} LIMIT 1;"; then
    log_success "${keyspace}.${table}: Query returned at least one row"
    return 0
  fi

  set_stage_detail "SELECT * FROM ${keyspace}.${table} LIMIT 1 returned no rows"
  return 1
}

# ---- Table processing ----

process_table() {
  local keyspace=$1
  local table=$2
  local schema_file=$3
  local expected_count=$4
  local expected_partitions=$5
  local qualified_name="${keyspace}.${table}"

  echo ""
  log_info "========================================="
  log_info "Processing ${qualified_name}"
  log_info "========================================="

  LAST_STAGE_DETAIL=""

  # Layer A: Write + flush portable SSTables
  if ! write_and_flush "$keyspace" "$table" "$schema_file"; then
    record_failure "$qualified_name" "flush" "${LAST_STAGE_DETAIL:-write_and_flush failed}"
    return 0
  fi

  # Layer B: Validate the already-flushed artifact directly
  if ! validate_flushed_artifact "$keyspace" "$table" "$expected_partitions" "$expected_count"; then
    record_failure "$qualified_name" "artifact" "${LAST_STAGE_DETAIL:-sstabledump validation failed}"
    return 0
  fi

  # Layer C packaging: arrange the flushed SSTables for import tooling
  if ! package_table_for_import "$keyspace" "$table" "$schema_file"; then
    record_failure "$qualified_name" "packaging" "${LAST_STAGE_DETAIL:-packaging failed}"
    return 0
  fi

  # Import
  if ! import_sstable "$keyspace" "$table"; then
    record_failure "$qualified_name" "import" "${LAST_STAGE_DETAIL:-import failed}"
    return 0
  fi

  # Layer D: Query verification
  if ! verify_count "$keyspace" "$table" "$expected_count"; then
    record_failure "$qualified_name" "query" "${LAST_STAGE_DETAIL:-row count verification failed}"
    return 0
  fi

  # Table-specific spot checks
  if ! case "$table" in
    collection_table) verify_collection_table ;;
    nested_collections_table) verify_nested_collections ;;
    collections_with_udts) verify_collections_with_udts ;;
    frozen_collections_table) verify_frozen_collections ;;
    typed_collections_table) verify_typed_collections ;;
    collection_clustering_table) verify_collection_clustering ;;
    chat_messages) verify_chat_messages ;;
    product_catalog) verify_product_catalog ;;
    *) verify_generic_table "$keyspace" "$table" ;;
  esac; then
    record_failure "$qualified_name" "query" "${LAST_STAGE_DETAIL:-spot check failed}"
    return 0
  fi

  record_pass "$qualified_name"
  log_success "${qualified_name} PASSED all checks"
  return 0
}

print_summary() {
  echo ""
  echo "========================================="
  echo "E2E Phase 2 (Collections) Validation Summary"
  echo "========================================="
  echo ""

  if [[ ${#PASSED_TABLES[@]} -gt 0 ]]; then
    echo -e "${GREEN}PASSED (${#PASSED_TABLES[@]} tables):${NC}"
    for table in "${PASSED_TABLES[@]}"; do
      echo -e "  ${GREEN}+${NC} $table"
    done
    echo ""
  fi

  if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
    echo -e "${RED}FAILED (${#FAILED_TABLES[@]} tables):${NC}"
    for detail in "${FAILED_DETAILS[@]}"; do
      echo -e "  ${RED}x${NC} $detail"
    done
    echo ""
  fi

  local total=$((${#PASSED_TABLES[@]} + ${#FAILED_TABLES[@]}))
  echo "Total: ${#PASSED_TABLES[@]}/$total tables passed"
  echo ""

  if [[ ${#FAILED_TABLES[@]} -eq 0 ]]; then
    echo -e "${GREEN}All 16 target tables passed collection E2E validation.${NC}"
    return 0
  else
    echo -e "${RED}Some tables failed E2E validation.${NC}"
    return 1
  fi
}

# Main execution
main() {
  log_info "Starting collection portability/import/query validation for 16 target tables"

  # Step 1: Cleanup
  cleanup_previous_runs

  # Step 2: Build CLI
  build_cqlite_cli

  # Step 3: Generate mutations
  generate_mutations

  # Step 4: Create schemas
  create_schemas

  # Step 5: Process all 16 tables

  # test_collections (8 tables)
  process_table "test_collections" "collection_table" "test-data/schemas/collections.cql" 10 "$(expected_partition_count collection_table)"
  process_table "test_collections" "nested_collections_table" "test-data/schemas/collections.cql" 10 "$(expected_partition_count nested_collections_table)"
  process_table "test_collections" "large_collections_table" "test-data/schemas/collections.cql" 10 "$(expected_partition_count large_collections_table)"
  process_table "test_collections" "frozen_collections_table" "test-data/schemas/collections.cql" 10 "$(expected_partition_count frozen_collections_table)"
  process_table "test_collections" "typed_collections_table" "test-data/schemas/collections.cql" 10 "$(expected_partition_count typed_collections_table)"
  process_table "test_collections" "empty_collections_table" "test-data/schemas/collections.cql" 10 "$(expected_partition_count empty_collections_table)"
  process_table "test_collections" "collections_with_udts" "test-data/schemas/collections.cql" 10 "$(expected_partition_count collections_with_udts)"
  process_table "test_collections" "collection_clustering_table" "test-data/schemas/collections.cql" 10 "$(expected_partition_count collection_clustering_table)"

  # test_timeseries (4 tables with MAP columns)
  process_table "test_timeseries" "app_metrics" "test-data/schemas/time-series.cql" 10 "$(expected_partition_count app_metrics)"
  process_table "test_timeseries" "user_activity" "test-data/schemas/time-series.cql" 10 "$(expected_partition_count user_activity)"
  process_table "test_timeseries" "event_store" "test-data/schemas/time-series.cql" 10 "$(expected_partition_count event_store)"
  process_table "test_timeseries" "user_sessions" "test-data/schemas/time-series.cql" 10 "$(expected_partition_count user_sessions)"

  # test_wide_rows (4 tables with collection columns)
  process_table "test_wide_rows" "chat_messages" "test-data/schemas/wide-rows.cql" 10 "$(expected_partition_count chat_messages)"
  process_table "test_wide_rows" "document_versions" "test-data/schemas/wide-rows.cql" 10 "$(expected_partition_count document_versions)"
  process_table "test_wide_rows" "product_catalog" "test-data/schemas/wide-rows.cql" 10 "$(expected_partition_count product_catalog)"
  process_table "test_wide_rows" "sparse_data_table" "test-data/schemas/wide-rows.cql" 10 "$(expected_partition_count sparse_data_table)"

  # Step 6: Print summary
  if print_summary; then
    exit 0
  else
    exit 1
  fi
}

# Run main
main

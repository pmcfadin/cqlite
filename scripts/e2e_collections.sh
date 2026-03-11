#!/usr/bin/env bash
set -euo pipefail

# E2E Phase 2 (Collections) Validation Script
# Tests 16 tables with collection types: SET, LIST, MAP, frozen, nested, UDTs
# Pipeline: write mutations -> flush -> export -> Cassandra 5 import -> verify

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
DOCKER_CONTAINER="cqlite-e2e-collections"
DOCKER_IMAGE="cassandra:5.0"

# Flags
NO_DOCKER=0
KEEP_CONTAINER=0

# Results tracking
declare -a PASSED_TABLES
declare -a FAILED_TABLES

# Parse command line flags
while [[ $# -gt 0 ]]; do
  case $1 in
    --no-docker)
      NO_DOCKER=1
      shift
      ;;
    --keep-container)
      KEEP_CONTAINER=1
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--no-docker] [--keep-container]"
      exit 1
      ;;
  esac
done

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

cleanup_previous_runs() {
  log_info "Cleaning up previous runs..."

  if [[ -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi

  if [[ $NO_DOCKER -eq 0 ]]; then
    if docker ps -a --format '{{.Names}}' | grep -q "^${DOCKER_CONTAINER}$"; then
      log_info "Removing existing Docker container..."
      docker rm -f "$DOCKER_CONTAINER" >/dev/null 2>&1 || true
    fi
  fi
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

export_table() {
  local keyspace=$1
  local table=$2
  local schema_file=$3

  log_info "Exporting ${keyspace}.${table}..."

  mkdir -p "$EXPORT_DIR/$keyspace/$table"

  cargo run --package cqlite-cli --features write-support --quiet -- \
    --writable --write-dir "$WORK_DIR/$keyspace/$table" \
    --schema "$schema_file" \
    --mutations-file "$MUTATIONS_DIR/${table}.jsonl" \
    export-sstable "$EXPORT_DIR/$keyspace/$table" \
    --keyspace "$keyspace" --table "$table"
}

start_cassandra() {
  if [[ $NO_DOCKER -eq 1 ]]; then
    log_warning "Skipping Docker (--no-docker flag set)"
    return
  fi

  log_info "Starting Cassandra 5 Docker container..."

  docker run --name "$DOCKER_CONTAINER" -d "$DOCKER_IMAGE" >/dev/null

  log_info "Waiting for Cassandra to be ready..."
  local max_attempts=60
  local attempt=0

  while [[ $attempt -lt $max_attempts ]]; do
    if docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1; then
      log_success "Cassandra is ready"
      return
    fi

    echo -n "."
    sleep 5
    ((attempt++))
  done

  log_error "Cassandra failed to start after ${max_attempts} attempts"
  exit 1
}

create_schemas() {
  if [[ $NO_DOCKER -eq 1 ]]; then
    log_warning "Skipping schema creation (--no-docker flag set)"
    return
  fi

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

  if [[ $NO_DOCKER -eq 1 ]]; then
    log_warning "Skipping import for ${keyspace}.${table} (--no-docker flag set)"
    return
  fi

  log_info "Importing SSTable for ${keyspace}.${table}..."

  local sstable_dir=$(find "$EXPORT_DIR/$keyspace/$table" -name "nb-*-big-Data.db" -exec dirname {} \; | head -1)

  if [[ -z "$sstable_dir" ]]; then
    log_error "No SSTable files found in $EXPORT_DIR/$keyspace/$table"
    return 1
  fi

  local container_path="/tmp/import_${keyspace}_${table}"
  docker cp "$sstable_dir" "$DOCKER_CONTAINER:$container_path"

  # Fix ownership - cassandra process needs to own the files
  docker exec "$DOCKER_CONTAINER" chown -R cassandra:cassandra "$container_path"

  docker exec "$DOCKER_CONTAINER" nodetool import -t "$keyspace" "$table" "$container_path"

  log_success "Imported ${keyspace}.${table}"
}

verify_count() {
  local keyspace=$1
  local table=$2
  local expected_count=$3

  if [[ $NO_DOCKER -eq 1 ]]; then
    log_warning "Skipping count verification for ${keyspace}.${table} (--no-docker flag set)"
    return 0
  fi

  log_info "Verifying row count for ${keyspace}.${table}..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT COUNT(*) FROM ${keyspace}.${table};" 2>/dev/null | sed -n 's/^[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -1)

  if [[ "$result" == "$expected_count" ]]; then
    log_success "${keyspace}.${table}: COUNT=$result (expected $expected_count)"
    return 0
  else
    log_error "${keyspace}.${table}: COUNT=$result (expected $expected_count)"
    return 1
  fi
}

# ---- Collection-specific spot checks ----

verify_collection_table() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking collection_table for SET/LIST/MAP retrieval..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT tags, scores, properties FROM test_collections.collection_table LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "tags " | head -1)

  if [[ -n "$result" ]]; then
    log_success "collection_table: Collections retrieved successfully"
    return 0
  else
    log_error "collection_table: Failed to retrieve collections"
    return 1
  fi
}

verify_nested_collections() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking nested_collections_table for nested MAP values..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT tags_by_category FROM test_collections.nested_collections_table LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "tags_by_category" | head -1)

  if [[ -n "$result" ]]; then
    log_success "nested_collections_table: Nested collections retrieved"
    return 0
  else
    log_error "nested_collections_table: Failed to retrieve nested collections"
    return 1
  fi
}

verify_collections_with_udts() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking collections_with_udts for UDT retrieval..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT addresses, contacts FROM test_collections.collections_with_udts LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "addresses" | head -1)

  if [[ -n "$result" ]]; then
    log_success "collections_with_udts: UDT collections retrieved"
    return 0
  else
    log_error "collections_with_udts: Failed to retrieve UDT collections"
    return 1
  fi
}

verify_frozen_collections() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking frozen_collections_table for frozen + regular mix..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT frozen_tags, frozen_scores, regular_tags FROM test_collections.frozen_collections_table LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "frozen_tags" | head -1)

  if [[ -n "$result" ]]; then
    log_success "frozen_collections_table: Frozen and regular collections retrieved"
    return 0
  else
    log_error "frozen_collections_table: Failed to retrieve frozen collections"
    return 1
  fi
}

verify_typed_collections() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking typed_collections_table for diverse element types..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT uuid_set, decimal_set, inet_map FROM test_collections.typed_collections_table LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "uuid_set" | head -1)

  if [[ -n "$result" ]]; then
    log_success "typed_collections_table: Typed collections retrieved"
    return 0
  else
    log_error "typed_collections_table: Failed to retrieve typed collections"
    return 1
  fi
}

verify_collection_clustering() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking collection_clustering_table for frozen list CK..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT partition_key, clustering_key, data FROM test_collections.collection_clustering_table LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "partition_key" | head -1)

  if [[ -n "$result" ]]; then
    log_success "collection_clustering_table: Frozen list clustering key retrieved"
    return 0
  else
    log_error "collection_clustering_table: Failed to retrieve frozen CK rows"
    return 1
  fi
}

verify_chat_messages() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking chat_messages for MAP<TEXT,FROZEN<SET<UUID>>> reactions..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT reactions, attachments FROM test_wide_rows.chat_messages LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "reactions" | head -1)

  if [[ -n "$result" ]]; then
    log_success "chat_messages: Nested reactions map retrieved"
    return 0
  else
    log_error "chat_messages: Failed to retrieve reactions"
    return 1
  fi
}

verify_product_catalog() {
  if [[ $NO_DOCKER -eq 1 ]]; then return 0; fi

  log_info "Spot-checking product_catalog for multiple collection types..."

  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT tags, specifications, attributes, dimensions FROM test_wide_rows.product_catalog LIMIT 1;" \
    2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "tags " | head -1)

  if [[ -n "$result" ]]; then
    log_success "product_catalog: Multiple collection types retrieved"
    return 0
  else
    log_error "product_catalog: Failed to retrieve collections"
    return 1
  fi
}

# ---- Table processing ----

process_table() {
  local keyspace=$1
  local table=$2
  local schema_file=$3
  local expected_count=$4

  echo ""
  log_info "========================================="
  log_info "Processing ${keyspace}.${table}"
  log_info "========================================="

  # Write + flush, then export
  if ! write_and_flush "$keyspace" "$table" "$schema_file"; then
    log_error "${keyspace}.${table}: write_and_flush failed"
    FAILED_TABLES+=("${keyspace}.${table}")
    return 1
  fi

  if ! export_table "$keyspace" "$table" "$schema_file"; then
    log_error "${keyspace}.${table}: export failed"
    FAILED_TABLES+=("${keyspace}.${table}")
    return 1
  fi

  # Import (only if Docker is enabled)
  if [[ $NO_DOCKER -eq 0 ]]; then
    if ! import_sstable "$keyspace" "$table"; then
      FAILED_TABLES+=("${keyspace}.${table}")
      return 1
    fi
  fi

  # Verify count
  if ! verify_count "$keyspace" "$table" "$expected_count"; then
    FAILED_TABLES+=("${keyspace}.${table}")
    return 1
  fi

  # Table-specific spot checks
  case "$table" in
    collection_table) verify_collection_table || true ;;
    nested_collections_table) verify_nested_collections || true ;;
    collections_with_udts) verify_collections_with_udts || true ;;
    frozen_collections_table) verify_frozen_collections || true ;;
    typed_collections_table) verify_typed_collections || true ;;
    collection_clustering_table) verify_collection_clustering || true ;;
    chat_messages) verify_chat_messages || true ;;
    product_catalog) verify_product_catalog || true ;;
  esac

  PASSED_TABLES+=("${keyspace}.${table}")
  log_success "${keyspace}.${table} PASSED all checks"
  return 0
}

cleanup_after_run() {
  if [[ $NO_DOCKER -eq 0 ]] && [[ $KEEP_CONTAINER -eq 0 ]]; then
    log_info "Cleaning up Docker container..."
    docker rm -f "$DOCKER_CONTAINER" >/dev/null 2>&1 || true
  elif [[ $KEEP_CONTAINER -eq 1 ]]; then
    log_warning "Keeping container $DOCKER_CONTAINER (--keep-container flag set)"
  fi

  log_info "Work directory preserved at: $WORK_DIR"
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
    for table in "${FAILED_TABLES[@]}"; do
      echo -e "  ${RED}x${NC} $table"
    done
    echo ""
  fi

  local total=$((${#PASSED_TABLES[@]} + ${#FAILED_TABLES[@]}))
  echo "Total: ${#PASSED_TABLES[@]}/$total tables passed"
  echo ""

  if [[ ${#FAILED_TABLES[@]} -eq 0 ]]; then
    echo -e "${GREEN}All 16 collection tables passed E2E validation!${NC}"
    return 0
  else
    echo -e "${RED}Some tables failed E2E validation.${NC}"
    return 1
  fi
}

# Main execution
main() {
  log_info "Starting E2E Phase 2 (Collections) Validation - 16 tables"

  # Step 1: Cleanup
  cleanup_previous_runs

  # Step 2: Build CLI
  build_cqlite_cli

  # Step 3: Generate mutations
  generate_mutations

  # Step 4: Start Cassandra and create schemas
  if [[ $NO_DOCKER -eq 0 ]]; then
    start_cassandra
    create_schemas
  fi

  # Step 5: Process all 16 tables

  # test_collections (8 tables)
  process_table "test_collections" "collection_table" "test-data/schemas/collections.cql" 10
  process_table "test_collections" "nested_collections_table" "test-data/schemas/collections.cql" 10
  process_table "test_collections" "large_collections_table" "test-data/schemas/collections.cql" 10
  process_table "test_collections" "collections_with_udts" "test-data/schemas/collections.cql" 10
  process_table "test_collections" "frozen_collections_table" "test-data/schemas/collections.cql" 10
  process_table "test_collections" "typed_collections_table" "test-data/schemas/collections.cql" 10
  process_table "test_collections" "empty_collections_table" "test-data/schemas/collections.cql" 10
  process_table "test_collections" "collection_clustering_table" "test-data/schemas/collections.cql" 10

  # test_timeseries (4 tables with MAP columns)
  process_table "test_timeseries" "app_metrics" "test-data/schemas/time-series.cql" 10
  process_table "test_timeseries" "user_activity" "test-data/schemas/time-series.cql" 10
  process_table "test_timeseries" "event_store" "test-data/schemas/time-series.cql" 10
  process_table "test_timeseries" "user_sessions" "test-data/schemas/time-series.cql" 10

  # test_wide_rows (4 tables with collection columns)
  process_table "test_wide_rows" "chat_messages" "test-data/schemas/wide-rows.cql" 10
  process_table "test_wide_rows" "document_versions" "test-data/schemas/wide-rows.cql" 10
  process_table "test_wide_rows" "product_catalog" "test-data/schemas/wide-rows.cql" 10
  process_table "test_wide_rows" "multi_metric_timeseries" "test-data/schemas/wide-rows.cql" 10

  # Step 6: Cleanup
  cleanup_after_run

  # Step 7: Print summary
  if print_summary; then
    exit 0
  else
    exit 1
  fi
}

# Run main
main

#!/usr/bin/env bash
set -euo pipefail

# E2E Phase 1 Validation Script
# Automates full write → flush → export → import → verify pipeline for 9 tables

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
WORK_DIR="/tmp/e2e_phase1"
EXPORT_DIR="$WORK_DIR/export"
MUTATIONS_DIR="e2e_phase1"
DOCKER_CONTAINER="cqlite-e2e-cassandra"
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

  # Remove work directory
  if [[ -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi

  # Stop and remove Docker container if running
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
  log_info "Generating mutations for all tables..."

  if [[ ! -f "scripts/generate_e2e_phase1.py" ]]; then
    log_error "scripts/generate_e2e_phase1.py not found"
    exit 1
  fi

  mkdir -p "$MUTATIONS_DIR"
  python3 scripts/generate_e2e_phase1.py
  log_success "Mutations generated"
}

write_and_flush() {
  local keyspace=$1
  local table=$2
  local schema_file=$3

  log_info "Writing and flushing ${keyspace}.${table}..."

  mkdir -p "$WORK_DIR/$keyspace/$table"

  # Combine write + flush in one invocation so the target table is resolved
  # from the mutations file (avoids schema selection falling back to wrong table)
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

  # Pass --mutations-file so the CLI resolves the correct target table schema
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

  # Create keyspaces and tables from schema files
  local schema_files=(
    "test-data/schemas/basic-types.cql"
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

  # Find the exported SSTable directory (contains nb-*-big-Data.db files)
  local sstable_dir=$(find "$EXPORT_DIR/$keyspace/$table" -name "nb-*-big-Data.db" -exec dirname {} \; | head -1)

  if [[ -z "$sstable_dir" ]]; then
    log_error "No SSTable files found in $EXPORT_DIR/$keyspace/$table"
    return 1
  fi

  # Copy into container
  local container_path="/tmp/import_${keyspace}_${table}"
  docker cp "$sstable_dir" "$DOCKER_CONTAINER:$container_path"

  # Import with -t flag (skip token verification)
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

verify_simple_table() {
  if [[ $NO_DOCKER -eq 1 ]]; then
    return 0
  fi

  log_info "Spot-checking simple_table for column types..."

  # Check one row has all column types populated
  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT id, name, age FROM test_basic.simple_table LIMIT 1;" 2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "id " | head -1)

  if [[ -n "$result" ]]; then
    log_success "simple_table: Sample row retrieved with all columns"
    return 0
  else
    log_error "simple_table: Failed to retrieve sample row"
    return 1
  fi
}

verify_stock_prices() {
  if [[ $NO_DOCKER -eq 1 ]]; then
    return 0
  fi

  log_info "Spot-checking stock_prices for DECIMAL precision..."

  # Check decimal values are present
  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT open_price FROM test_timeseries.stock_prices LIMIT 1;" 2>/dev/null | sed -n 's/^[[:space:]]*\([0-9][0-9]*\.[0-9][0-9]*\).*/\1/p' | head -1)

  if [[ -n "$result" ]]; then
    log_success "stock_prices: DECIMAL value preserved ($result)"
    return 0
  else
    log_error "stock_prices: Failed to retrieve DECIMAL value"
    return 1
  fi
}

verify_static_columns() {
  if [[ $NO_DOCKER -eq 1 ]]; then
    return 0
  fi

  log_info "Spot-checking static_columns_table for static column sharing..."

  # Check static column is present
  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT partition_key, static_data FROM test_basic.static_columns_table LIMIT 1;" 2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "partition_key" | head -1)

  if [[ -n "$result" ]]; then
    log_success "static_columns_table: Static column retrieved"
    return 0
  else
    log_error "static_columns_table: Failed to retrieve static column"
    return 1
  fi
}

verify_wide_partition() {
  if [[ $NO_DOCKER -eq 1 ]]; then
    return 0
  fi

  log_info "Spot-checking wide_partition_table for 5-column clustering key..."

  # Check clustering columns are present and ordered
  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT partition_key, clustering_col1, clustering_col2, clustering_col3, clustering_col4, clustering_col5 FROM test_wide_rows.wide_partition_table LIMIT 1;" 2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "partition_key" | head -1)

  if [[ -n "$result" ]]; then
    log_success "wide_partition_table: Clustering key columns retrieved"
    return 0
  else
    log_error "wide_partition_table: Failed to retrieve clustering key columns"
    return 1
  fi
}

verify_large_blob() {
  if [[ $NO_DOCKER -eq 1 ]]; then
    return 0
  fi

  log_info "Spot-checking large_blob_table for blob sizes..."

  # Check blob column is present
  local result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT file_id, chunk_id FROM test_wide_rows.large_blob_table LIMIT 1;" 2>/dev/null | grep -v "^$" | grep -v "^---" | grep -v "file_id" | head -1)

  if [[ -n "$result" ]]; then
    log_success "large_blob_table: Blob data retrieved"
    return 0
  else
    log_error "large_blob_table: Failed to retrieve blob data"
    return 1
  fi
}

process_table() {
  local keyspace=$1
  local table=$2
  local schema_file=$3

  echo ""
  log_info "========================================="
  log_info "Processing ${keyspace}.${table}"
  log_info "========================================="

  # Write + flush, then export
  write_and_flush "$keyspace" "$table" "$schema_file"
  export_table "$keyspace" "$table" "$schema_file"

  # Import (only if Docker is enabled)
  if [[ $NO_DOCKER -eq 0 ]]; then
    if ! import_sstable "$keyspace" "$table"; then
      FAILED_TABLES+=("${keyspace}.${table}")
      return 1
    fi
  fi

  # Verify count
  if ! verify_count "$keyspace" "$table" 100; then
    FAILED_TABLES+=("${keyspace}.${table}")
    return 1
  fi

  # Table-specific spot checks
  case "$table" in
    simple_table)
      if ! verify_simple_table; then
        FAILED_TABLES+=("${keyspace}.${table}")
        return 1
      fi
      ;;
    stock_prices)
      if ! verify_stock_prices; then
        FAILED_TABLES+=("${keyspace}.${table}")
        return 1
      fi
      ;;
    static_columns_table)
      if ! verify_static_columns; then
        FAILED_TABLES+=("${keyspace}.${table}")
        return 1
      fi
      ;;
    wide_partition_table)
      if ! verify_wide_partition; then
        FAILED_TABLES+=("${keyspace}.${table}")
        return 1
      fi
      ;;
    large_blob_table)
      if ! verify_large_blob; then
        FAILED_TABLES+=("${keyspace}.${table}")
        return 1
      fi
      ;;
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
  echo "E2E Phase 1 Validation Summary"
  echo "========================================="
  echo ""

  if [[ ${#PASSED_TABLES[@]} -gt 0 ]]; then
    echo -e "${GREEN}PASSED (${#PASSED_TABLES[@]} tables):${NC}"
    for table in "${PASSED_TABLES[@]}"; do
      echo -e "  ${GREEN}✓${NC} $table"
    done
    echo ""
  fi

  if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
    echo -e "${RED}FAILED (${#FAILED_TABLES[@]} tables):${NC}"
    for table in "${FAILED_TABLES[@]}"; do
      echo -e "  ${RED}✗${NC} $table"
    done
    echo ""
  fi

  local total=$((${#PASSED_TABLES[@]} + ${#FAILED_TABLES[@]}))
  echo "Total: ${#PASSED_TABLES[@]}/$total tables passed"
  echo ""

  if [[ ${#FAILED_TABLES[@]} -eq 0 ]]; then
    echo -e "${GREEN}All tables passed E2E validation!${NC}"
    return 0
  else
    echo -e "${RED}Some tables failed E2E validation.${NC}"
    return 1
  fi
}

# Main execution
main() {
  log_info "Starting E2E Phase 1 Validation"

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

  # Step 5: Process all 9 tables
  # test_basic tables
  process_table "test_basic" "simple_table" "test-data/schemas/basic-types.cql"
  process_table "test_basic" "composite_key_table" "test-data/schemas/basic-types.cql"
  process_table "test_basic" "multi_partition_table" "test-data/schemas/basic-types.cql"
  process_table "test_basic" "static_columns_table" "test-data/schemas/basic-types.cql"
  process_table "test_basic" "ttl_test_table" "test-data/schemas/basic-types.cql"

  # test_timeseries tables
  process_table "test_timeseries" "sensor_data" "test-data/schemas/time-series.cql"
  process_table "test_timeseries" "stock_prices" "test-data/schemas/time-series.cql"

  # test_wide_rows tables
  process_table "test_wide_rows" "wide_partition_table" "test-data/schemas/wide-rows.cql"
  process_table "test_wide_rows" "large_blob_table" "test-data/schemas/wide-rows.cql"

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

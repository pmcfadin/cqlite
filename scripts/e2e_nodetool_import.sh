#!/usr/bin/env bash
set -euo pipefail

# E2E Validation: nodetool import WITHOUT -t flag
# Proves that CQLite's Murmur3 tokens match Cassandra's, enabling
# import without skipping token verification. Also validates point
# lookups work (proves Bloom filter is correct).

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

WORK_DIR="/tmp/e2e_murmur3"
EXPORT_DIR="$WORK_DIR/export"
DOCKER_CONTAINER="cqlite-e2e-cassandra"
KEYSPACE="murmur3_e2e"
TABLE="employees"
SCHEMA_FILE="$WORK_DIR/schema.cql"
CLI="cargo run --package cqlite-cli --features write-support --"

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_error()   { echo -e "${RED}[FAIL]${NC} $1"; }

FAILURES=0
fail() { log_error "$1"; FAILURES=$((FAILURES + 1)); }

# Clean up
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" "$EXPORT_DIR"

# Step 1: Create schema
log_info "Creating schema..."
cat > "$SCHEMA_FILE" <<'EOF'
CREATE KEYSPACE murmur3_e2e WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE murmur3_e2e.employees (
    id uuid PRIMARY KEY,
    name text,
    age int,
    department text
);
EOF

# Step 2: Create the table in Cassandra
log_info "Creating table in Cassandra..."
docker exec "$DOCKER_CONTAINER" cqlsh -e \
  "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec "$DOCKER_CONTAINER" cqlsh -e \
  "CREATE TABLE IF NOT EXISTS ${KEYSPACE}.${TABLE} (id uuid PRIMARY KEY, name text, age int, department text);"

# Step 3: Generate mutations
log_info "Generating mutations..."

# Fixed UUIDs for point lookup verification
declare -a UUIDS=(
  "11111111-1111-1111-1111-111111111111"
  "22222222-2222-2222-2222-222222222222"
  "33333333-3333-3333-3333-333333333333"
  "44444444-4444-4444-4444-444444444444"
  "55555555-5555-5555-5555-555555555555"
  "66666666-6666-6666-6666-666666666666"
  "77777777-7777-7777-7777-777777777777"
  "88888888-8888-8888-8888-888888888888"
  "99999999-9999-9999-9999-999999999999"
  "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
  "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
  "cccccccc-cccc-cccc-cccc-cccccccccccc"
  "dddddddd-dddd-dddd-dddd-dddddddddddd"
  "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
  "ffffffff-ffff-ffff-ffff-ffffffffffff"
)

declare -a NAMES=(
  "Alice" "Bob" "Charlie" "Diana" "Eve"
  "Frank" "Grace" "Heidi" "Ivan" "Judy"
  "Karl" "Linda" "Mike" "Nancy" "Oscar"
)

declare -a DEPARTMENTS=(
  "Engineering" "Marketing" "Sales" "HR" "Finance"
  "Engineering" "Marketing" "Sales" "HR" "Finance"
  "Engineering" "Marketing" "Sales" "HR" "Finance"
)

MUTATIONS_FILE="$WORK_DIR/mutations.jsonl"
> "$MUTATIONS_FILE"

for i in "${!UUIDS[@]}"; do
  uuid="${UUIDS[$i]}"
  name="${NAMES[$i]}"
  age=$((25 + i))
  dept="${DEPARTMENTS[$i]}"

  # Convert UUID to byte array
  uuid_clean="${uuid//-/}"
  uuid_bytes=""
  for ((j=0; j<${#uuid_clean}; j+=2)); do
    byte=$((16#${uuid_clean:$j:2}))
    uuid_bytes="${uuid_bytes}${uuid_bytes:+,}${byte}"
  done

  cat >> "$MUTATIONS_FILE" <<JSONL
{"table":{"keyspace":"$KEYSPACE","table":"$TABLE"},"partition_key":{"columns":[["id",{"Uuid":[$uuid_bytes]}]]},"clustering_key":null,"operations":[{"Write":{"column":"name","value":{"Text":"$name"}}},{"Write":{"column":"age","value":{"Integer":$age}}},{"Write":{"column":"department","value":{"Text":"$dept"}}}],"timestamp_micros":1704067200000000,"ttl_seconds":null,"partition_tombstone":null,"range_tombstones":[]}
JSONL
done

log_info "Generated ${#UUIDS[@]} mutations"

# Step 4: Write mutations + flush
log_info "Writing mutations and flushing..."
cargo run --package cqlite-cli --features write-support --quiet -- \
  --writable --write-dir "$WORK_DIR/data" \
  --schema "$SCHEMA_FILE" \
  --mutations-file "$MUTATIONS_FILE" \
  --flush

# Step 5: Export SSTable
log_info "Exporting SSTable..."
mkdir -p "$EXPORT_DIR/$KEYSPACE/$TABLE"
cargo run --package cqlite-cli --features write-support --quiet -- \
  --writable --write-dir "$WORK_DIR/data" \
  --schema "$SCHEMA_FILE" \
  --mutations-file "$MUTATIONS_FILE" \
  export-sstable "$EXPORT_DIR/$KEYSPACE/$TABLE" \
  --keyspace "$KEYSPACE" --table "$TABLE"

# The export creates {output_dir}/{keyspace}/{table}/ subdirectory
SSTABLE_DIR="$EXPORT_DIR/$KEYSPACE/$TABLE/$KEYSPACE/$TABLE"

# Verify export produced files
EXPORTED_FILES=$(ls "$SSTABLE_DIR/" 2>/dev/null | wc -l)
if [[ $EXPORTED_FILES -lt 5 ]]; then
  fail "Export produced only $EXPORTED_FILES files (expected >= 5)"
else
  log_success "Export produced $EXPORTED_FILES files"
fi

# Step 6: Copy into Cassandra and import WITHOUT -t
log_info "Copying SSTable into Cassandra container..."
CONTAINER_PATH="/tmp/e2e_import/${KEYSPACE}/${TABLE}"
docker exec "$DOCKER_CONTAINER" rm -rf "/tmp/e2e_import" 2>/dev/null || true
docker exec "$DOCKER_CONTAINER" mkdir -p "$CONTAINER_PATH"
docker cp "$SSTABLE_DIR/." "$DOCKER_CONTAINER:$CONTAINER_PATH/"
# Fix ownership for Cassandra process (runs as cassandra user)
docker exec "$DOCKER_CONTAINER" chown -R cassandra:cassandra "/tmp/e2e_import/"
docker exec "$DOCKER_CONTAINER" chmod -R 755 "/tmp/e2e_import/"

log_info "Importing SSTable with nodetool import (NO -t flag)..."
if docker exec "$DOCKER_CONTAINER" nodetool import "$KEYSPACE" "$TABLE" "$CONTAINER_PATH" 2>&1; then
  log_success "nodetool import succeeded WITHOUT -t flag!"
else
  fail "nodetool import FAILED without -t flag"
  # Try with -t as fallback to continue validation
  log_info "Retrying with -t flag for debugging..."
  docker exec "$DOCKER_CONTAINER" nodetool import -t "$KEYSPACE" "$TABLE" "$CONTAINER_PATH" 2>&1 || true
fi

# Step 7: Verify row count
log_info "Verifying row count..."
ACTUAL_COUNT=$(docker exec "$DOCKER_CONTAINER" cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.${TABLE};" 2>&1 | grep -E '^\s*[0-9]+' | tr -d ' ')
EXPECTED_COUNT=${#UUIDS[@]}

if [[ "$ACTUAL_COUNT" == "$EXPECTED_COUNT" ]]; then
  log_success "Row count matches: $ACTUAL_COUNT rows"
else
  fail "Row count mismatch: expected $EXPECTED_COUNT, got $ACTUAL_COUNT"
fi

# Step 8: Point lookups
log_info "Testing point lookups..."
LOOKUP_PASS=0
LOOKUP_FAIL=0

for i in "${!UUIDS[@]}"; do
  uuid="${UUIDS[$i]}"
  expected_name="${NAMES[$i]}"
  expected_age=$((25 + i))

  result=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
    "SELECT name, age FROM ${KEYSPACE}.${TABLE} WHERE id = $uuid;" 2>&1)

  if echo "$result" | grep -q "$expected_name"; then
    LOOKUP_PASS=$((LOOKUP_PASS + 1))
  else
    fail "Point lookup failed for $uuid: expected name='$expected_name', got: $result"
    LOOKUP_FAIL=$((LOOKUP_FAIL + 1))
  fi
done

if [[ $LOOKUP_FAIL -eq 0 ]]; then
  log_success "All $LOOKUP_PASS point lookups succeeded (Bloom filter works!)"
else
  fail "$LOOKUP_FAIL/$((LOOKUP_PASS + LOOKUP_FAIL)) point lookups failed"
fi

# Step 9: Full scan verification
log_info "Verifying full scan..."
FULL_SCAN=$(docker exec "$DOCKER_CONTAINER" cqlsh -e \
  "SELECT id, name, age, department FROM ${KEYSPACE}.${TABLE};" 2>&1)

SCAN_ROWS=$(echo "$FULL_SCAN" | grep -cE '[0-9a-f]{8}-[0-9a-f]{4}' || true)
if [[ "$SCAN_ROWS" -ge "$EXPECTED_COUNT" ]]; then
  log_success "Full scan returned $SCAN_ROWS rows"
else
  fail "Full scan returned only $SCAN_ROWS rows (expected $EXPECTED_COUNT)"
fi

# Summary
echo ""
echo "=================================="
if [[ $FAILURES -eq 0 ]]; then
  echo -e "${GREEN}ALL CHECKS PASSED${NC}"
  echo "  - nodetool import: NO -t flag needed"
  echo "  - Row count: $EXPECTED_COUNT"
  echo "  - Point lookups: $LOOKUP_PASS/$EXPECTED_COUNT"
  echo "  - Bloom filter: WORKING"
  echo "  - Murmur3 tokens: MATCHING Cassandra"
else
  echo -e "${RED}$FAILURES CHECK(S) FAILED${NC}"
fi
echo "=================================="

# Cleanup
docker exec "$DOCKER_CONTAINER" cqlsh -e "DROP KEYSPACE IF EXISTS $KEYSPACE;" 2>/dev/null || true

exit $FAILURES

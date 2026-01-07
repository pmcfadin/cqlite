#!/bin/bash
# Debug script to trace why queries return empty results

set -e

echo "=== Debugging Empty Query Results ==="
echo ""

# Build release version with debug assertions
echo "Building with debug assertions enabled..."
cargo build --package cqlite-cli --release
echo ""

# Set up test environment
export CQLITE_DATASETS_ROOT="/Users/patrick/local_projects/cqlite/test-data/datasets"
TEST_DATASET="test_basic"
SCHEMA_FILE="/Users/patrick/local_projects/cqlite/test-data/schemas/basic-types.cql"
QUERY="SELECT * FROM test_basic.simple_table LIMIT 5"

# Verify test data exists
echo "Verifying test data structure..."
DATASET_DIR="$CQLITE_DATASETS_ROOT/sstables/$TEST_DATASET"
echo "Dataset directory: $DATASET_DIR"
ls -la "$DATASET_DIR" || { echo "ERROR: Dataset directory not found!"; exit 1; }
echo ""

# Count SSTable files
echo "SSTable files found:"
find "$DATASET_DIR" -name "*-Data.db" -type f
SSTABLE_COUNT=$(find "$DATASET_DIR" -name "*-Data.db" -type f | wc -l | tr -d ' ')
echo "Total SSTable count: $SSTABLE_COUNT"
echo ""

# Verify schema exists
echo "Verifying schema file..."
ls -la "$SCHEMA_FILE" || { echo "ERROR: Schema file not found!"; exit 1; }
echo ""

# Run the query with debug output
echo "=== Executing Query with Debug Tracing ==="
echo "Command: cqlite --dataset $TEST_DATASET --schema $SCHEMA_FILE --execute \"$QUERY\" --format json"
echo ""

/Users/patrick/local_projects/cqlite/target/release/cqlite \
  --dataset "$TEST_DATASET" \
  --schema "$SCHEMA_FILE" \
  --execute "$QUERY" \
  --format json 2>&1 | tee /tmp/cqlite_debug_output.txt

echo ""
echo "=== Debug Output Analysis ==="
cat /tmp/cqlite_debug_output.txt | grep -E "(DEBUG|ERROR|Warning)" || echo "No debug/error messages found"
echo ""

# Check result
if grep -q '"row_count": 0' /tmp/cqlite_debug_output.txt; then
    echo "PROBLEM CONFIRMED: Query returned 0 rows"
    echo ""
    echo "Next steps to investigate:"
    echo "1. Check if SSTables were actually loaded"
    echo "2. Check if scan() is being called on readers"
    echo "3. Check if SSTableReader.scan() is returning empty"
    exit 1
else
    echo "SUCCESS: Query returned data!"
    exit 0
fi

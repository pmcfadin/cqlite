#!/bin/bash
# Functional test: Verify REPL can query data after config-based ingestion

echo "Testing REPL query execution after config-based ingestion..."
echo ""

# Send query to REPL and capture output
output=$(echo "SELECT * FROM test_basic.simple_table LIMIT 3;" | ./target/debug/cqlite --config test.toml repl 2>&1)

# Check if we got results (not 0 rows)
if echo "$output" | grep -q "0 rows"; then
    echo "FAIL: Query returned 0 rows (ingestion did not work)"
    echo "Output:"
    echo "$output"
    exit 1
elif echo "$output" | grep -q "simple_table"; then
    echo "SUCCESS: Query execution successful, got results"
    echo "$output" | grep -A 5 "simple_table"
    exit 0
else
    echo "UNKNOWN: Could not determine if query succeeded"
    echo "Output:"
    echo "$output"
    exit 2
fi

#!/bin/bash
# Test script to verify REPL mode ingestion from config file

echo "Testing REPL mode with config file ingestion..."
echo ""

# Send commands to REPL and capture output
echo -e ":status\n:quit" | ./target/debug/cqlite --config test.toml repl 2>&1

echo ""
echo "Test complete."

#!/bin/bash
# Quick test example for CQLite Cassandra compatibility

echo "🚀 Running quick compatibility test for Cassandra 4.0..."

# Test basic compatibility
./scripts/compatibility_checker.sh --version 4.0 --test-suite basic

echo "✅ Quick test completed! Check results in ./results/"
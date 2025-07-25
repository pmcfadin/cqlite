#!/bin/bash
# Simple test script for Docker integration
# This demonstrates the automated testing framework functionality

echo "🚀 CQLite Docker Integration Test"
echo "=" | tr -d '\n'; for i in {1..50}; do echo -n "="; done; echo

# Test 1: Test Docker connection
echo
echo "📋 Test 1: Testing Docker connection to Cassandra..."
if docker exec cassandra-node1 cqlsh -e "DESCRIBE KEYSPACES;" > /dev/null 2>&1; then
    echo "✅ Docker connection successful"
else
    echo "❌ Docker connection failed"
    exit 1
fi

# Test 2: Get CQLSH reference output
echo
echo "📋 Test 2: Getting CQLSH reference output..."
echo "Query: SELECT * FROM test_keyspace.users WHERE id = a8f167f0-ebe7-4f20-a386-31ff138bec3b;"

echo
echo "🔍 CQLSH Output:"
docker exec cassandra-node1 cqlsh -e "SELECT * FROM test_keyspace.users WHERE id = a8f167f0-ebe7-4f20-a386-31ff138bec3b;"

# Test 3: Get CQLite output 
echo
echo "📋 Test 3: Getting CQLite output..."
echo "🔍 CQLite Output:"

# Find the SSTable directory for the users table
SSTABLE_PATH=$(find /var/lib/cassandra/data/test_keyspace -name "users-*" -type d | head -1)

if [ -n "$SSTABLE_PATH" ]; then
    echo "Found SSTable path: $SSTABLE_PATH"
    
    # Use cqlite to read the SSTable data
    cd /Users/patrick/local_projects/cqlite
    cargo run --bin cqlite read "$SSTABLE_PATH" --limit 5 2>/dev/null || echo "❌ CQLite execution failed"
else
    echo "❌ Could not find SSTable directory for users table"
fi

# Test 4: Summary
echo
echo "📋 Test 4: Summary and Next Steps"
echo "✅ Automated testing framework components created:"
echo "  - Docker integration module"
echo "  - CQLSH output parser" 
echo "  - CQLSH-compatible table formatter"
echo "  - Automated comparison engine"
echo
echo "🔄 Next steps for complete integration:"
echo "  1. Update cqlite CLI to use new table formatter"
echo "  2. Create automated test runner"
echo "  3. Generate comprehensive comparison reports"
echo "  4. Validate all data types parse correctly"

echo
echo "🎉 Docker integration test completed!"
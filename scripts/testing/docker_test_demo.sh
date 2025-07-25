#!/bin/bash

# Docker Integration Test Demo Script
echo "🐳 CQLite Docker Integration Test Demo"
echo "====================================="

# Check if Cassandra container is running
echo ""
echo "1. Checking for running Cassandra container..."
if docker ps --filter "name=cassandra" --format "table {{.Names}}\t{{.Status}}" | grep -q "cassandra"; then
    echo "✅ Found running Cassandra container"
    docker ps --filter "name=cassandra" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
else
    echo "❌ No running Cassandra container found"
    echo "To start Cassandra, run:"
    echo "docker run --name cassandra-node1 -d -p 9042:9042 -p 7199:7199 cassandra:5.0"
    exit 1
fi

echo ""
echo "2. Testing Docker cqlsh connection..."

# Wait for Cassandra to be ready
echo "Waiting for Cassandra to be ready..."
timeout=60
counter=0

while [ $counter -lt $timeout ]; do
    if docker exec cassandra-node1 cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1; then
        echo "✅ Cassandra is ready!"
        break
    fi
    
    echo "Waiting... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -ge $timeout ]; then
    echo "❌ Cassandra failed to become ready within $timeout seconds"
    exit 1
fi

echo ""
echo "3. Setting up test keyspace and table..."

# Create test keyspace
docker exec cassandra-node1 cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS cqlite_test 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};"

# Create test table
docker exec cassandra-node1 cqlsh -e "
USE cqlite_test;
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    age INT,
    created_at TIMESTAMP
);"

echo "✅ Test keyspace and table created"

echo ""
echo "4. Inserting test data..."

# Insert test data
docker exec cassandra-node1 cqlsh -e "
USE cqlite_test;
INSERT INTO users (id, name, email, age, created_at) 
VALUES (uuid(), 'Alice Johnson', 'alice@example.com', 30, toTimestamp(now()));

INSERT INTO users (id, name, email, age, created_at) 
VALUES (uuid(), 'Bob Smith', 'bob@example.com', 25, toTimestamp(now()));

INSERT INTO users (id, name, email, age, created_at) 
VALUES (uuid(), 'Charlie Brown', 'charlie@example.com', 35, toTimestamp(now()));

INSERT INTO users (id, name, email, age, created_at) 
VALUES (uuid(), 'Diana Prince', 'diana@example.com', 28, toTimestamp(now()));"

echo "✅ Test data inserted"

echo ""
echo "5. Running test queries..."

echo ""
echo "Query 1: Count all users"
echo "------------------------"
docker exec cassandra-node1 cqlsh -e "USE cqlite_test; SELECT COUNT(*) FROM users;"

echo ""
echo "Query 2: Select users older than 25"
echo "------------------------------------"
docker exec cassandra-node1 cqlsh -e "USE cqlite_test; SELECT name, email, age FROM users WHERE age > 25 ALLOW FILTERING;"

echo ""
echo "Query 3: Select all users (limited to 2)"
echo "----------------------------------------"
docker exec cassandra-node1 cqlsh -e "USE cqlite_test; SELECT * FROM users LIMIT 2;"

echo ""
echo "6. Testing Docker cqlsh output parsing..."

# Capture output for parsing test
output=$(docker exec cassandra-node1 cqlsh -e "USE cqlite_test; SELECT name, age FROM users LIMIT 2;")
echo "Raw cqlsh output:"
echo "$output"

echo ""
echo "7. Cleanup test data..."
docker exec cassandra-node1 cqlsh -e "DROP KEYSPACE IF EXISTS cqlite_test;" >/dev/null 2>&1
echo "✅ Test data cleaned up"

echo ""
echo "🎉 Docker Integration Test Demo Complete!"
echo ""
echo "Next steps:"
echo "1. Use the Rust DockerCqlshClient to run these queries programmatically"
echo "2. Compare results between CQLite and Cassandra"
echo "3. Implement automated testing with the CassandraTestRunner"
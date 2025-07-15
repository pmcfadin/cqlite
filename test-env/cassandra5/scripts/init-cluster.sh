#!/bin/bash

echo "🚀 Starting Cassandra 5 cluster initialization..."

# Wait for cluster to be ready
echo "⏳ Waiting for cluster to be ready..."
sleep 30

# Check cluster status
echo "🔍 Checking cluster status..."
docker exec cassandra-node1 nodetool status

# Create keyspaces and tables with various configurations
echo "📋 Creating test keyspaces and tables..."
docker exec cassandra-node1 cqlsh -f /scripts/create-keyspaces.cql

# Generate test data
echo "📊 Generating test data..."
docker exec cassandra-node1 cqlsh -f /scripts/generate-test-data.cql

# Force compaction to create SSTables
echo "🗜️ Forcing compaction to create SSTables..."
docker exec cassandra-node1 nodetool compact test_keyspace

# List created SSTables
echo "📂 Listing created SSTables..."
docker exec cassandra-node1 find /var/lib/cassandra/data/test_keyspace -name "*.db" -type f

echo "✅ Cluster initialization complete!"
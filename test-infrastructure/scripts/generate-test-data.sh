#!/bin/bash

# CQLite Test Data Generation Script
# Generates comprehensive Cassandra 5 test data for compatibility validation

set -e

echo "🚀 Starting CQLite Cassandra 5+ Compatibility Test Data Generation"

# Wait for Cassandra to be ready
echo "⏳ Waiting for Cassandra to be ready..."
for i in {1..30}; do
    if cqlsh cassandra5-single -e "SELECT now() FROM system.local;" > /dev/null 2>&1; then
        echo "✅ Cassandra is ready!"
        break
    fi
    echo "   Attempt $i/30 - waiting 10 seconds..."
    sleep 10
done

# Create test keyspace and tables
echo "📝 Creating test keyspace and tables..."
cqlsh cassandra5-single << 'EOF'
-- Create test keyspace
CREATE KEYSPACE IF NOT EXISTS cqlite_test 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE cqlite_test;

-- Test table with all primitive types
CREATE TABLE IF NOT EXISTS primitive_types (
    id UUID PRIMARY KEY,
    text_col TEXT,
    int_col INT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    boolean_col BOOLEAN,
    blob_col BLOB,
    timestamp_col TIMESTAMP,
    date_col DATE,
    time_col TIME,
    inet_col INET,
    decimal_col DECIMAL,
    varint_col VARINT,
    duration_col DURATION
);

-- Test table with collections
CREATE TABLE IF NOT EXISTS collection_types (
    id UUID PRIMARY KEY,
    list_col LIST<TEXT>,
    set_col SET<INT>,
    map_col MAP<TEXT, INT>,
    frozen_list FROZEN<LIST<TEXT>>,
    frozen_set FROZEN<SET<INT>>,
    frozen_map FROZEN<MAP<TEXT, INT>>
);

-- Test table with User Defined Type
CREATE TYPE IF NOT EXISTS address (
    street TEXT,
    city TEXT,
    zip_code INT
);

CREATE TABLE IF NOT EXISTS user_defined_types (
    id UUID PRIMARY KEY,
    address_col address,
    frozen_address FROZEN<address>,
    address_list LIST<FROZEN<address>>
);

-- Test table with composite primary key
CREATE TABLE IF NOT EXISTS composite_key (
    partition_key TEXT,
    clustering_key1 INT,
    clustering_key2 TEXT,
    data_col TEXT,
    PRIMARY KEY (partition_key, clustering_key1, clustering_key2)
);

-- Test table with secondary index
CREATE TABLE IF NOT EXISTS indexed_table (
    id UUID PRIMARY KEY,
    indexed_col TEXT,
    data_col TEXT
);

CREATE INDEX IF NOT EXISTS idx_indexed_col ON indexed_table (indexed_col);

EOF

echo "✅ Test schema created successfully"

# Insert comprehensive test data
echo "💾 Inserting test data..."
cqlsh cassandra5-single << 'EOF'
USE cqlite_test;

-- Insert primitive types data
INSERT INTO primitive_types (
    id, text_col, int_col, bigint_col, float_col, double_col, 
    boolean_col, blob_col, timestamp_col, date_col, time_col,
    inet_col, decimal_col, varint_col, duration_col
) VALUES (
    uuid(),
    'Hello CQLite',
    42,
    9223372036854775807,
    3.14,
    2.718281828,
    true,
    0x48656c6c6f,
    '2025-01-21 12:00:00',
    '2025-01-21',
    '12:00:00',
    '192.168.1.1',
    123.456,
    987654321,
    1h30m
);

-- Insert more primitive data for variety
INSERT INTO primitive_types (id, text_col, int_col, boolean_col) VALUES (uuid(), 'Test 2', 100, false);
INSERT INTO primitive_types (id, text_col, int_col, boolean_col) VALUES (uuid(), 'Test 3', 200, true);

-- Insert collection types data
INSERT INTO collection_types (
    id, list_col, set_col, map_col, frozen_list, frozen_set, frozen_map
) VALUES (
    uuid(),
    ['item1', 'item2', 'item3'],
    {1, 2, 3, 4, 5},
    {'key1': 10, 'key2': 20, 'key3': 30},
    ['frozen1', 'frozen2'],
    {100, 200, 300},
    {'fkey1': 1000, 'fkey2': 2000}
);

-- Insert UDT data
INSERT INTO user_defined_types (
    id, address_col, frozen_address, address_list
) VALUES (
    uuid(),
    {street: '123 Main St', city: 'Anytown', zip_code: 12345},
    {street: '456 Oak Ave', city: 'Somewhere', zip_code: 67890},
    [{street: '789 Pine St', city: 'Elsewhere', zip_code: 54321}]
);

-- Insert composite key data
INSERT INTO composite_key (partition_key, clustering_key1, clustering_key2, data_col) 
VALUES ('partition1', 1, 'cluster1', 'data1');
INSERT INTO composite_key (partition_key, clustering_key1, clustering_key2, data_col) 
VALUES ('partition1', 2, 'cluster2', 'data2');
INSERT INTO composite_key (partition_key, clustering_key1, clustering_key2, data_col) 
VALUES ('partition2', 1, 'cluster1', 'data3');

-- Insert indexed data
INSERT INTO indexed_table (id, indexed_col, data_col) VALUES (uuid(), 'searchable1', 'data1');
INSERT INTO indexed_table (id, indexed_col, data_col) VALUES (uuid(), 'searchable2', 'data2');
INSERT INTO indexed_table (id, indexed_col, data_col) VALUES (uuid(), 'searchable3', 'data3');

EOF

echo "✅ Test data inserted successfully"

# Force flush to create SSTables
echo "💾 Forcing flush to create SSTable files..."
cqlsh cassandra5-single << 'EOF'
USE cqlite_test;
-- Force flush all tables to create SSTables
-- Note: In production this happens automatically, but we force it for testing
EOF

# Use nodetool to flush
docker exec cqlite-cassandra5-test nodetool flush cqlite_test

echo "✅ SSTables flushed successfully"

# Wait a moment for files to be written
sleep 5

# Copy SSTable files to test data directory
echo "📂 Copying SSTable files for testing..."
docker exec cqlite-cassandra5-test find /var/lib/cassandra/data/cqlite_test -name "*.db" -o -name "*.txt" | head -20

# Create organized test data structure
docker exec cqlite-cassandra5-test bash -c "
mkdir -p /opt/test-data/sstables/primitive_types
mkdir -p /opt/test-data/sstables/collection_types  
mkdir -p /opt/test-data/sstables/user_defined_types
mkdir -p /opt/test-data/sstables/composite_key
mkdir -p /opt/test-data/sstables/indexed_table

# Copy SSTable files for each table
find /var/lib/cassandra/data/cqlite_test -name '*primitive_types*' -type f | xargs -I {} cp {} /opt/test-data/sstables/primitive_types/
find /var/lib/cassandra/data/cqlite_test -name '*collection_types*' -type f | xargs -I {} cp {} /opt/test-data/sstables/collection_types/
find /var/lib/cassandra/data/cqlite_test -name '*user_defined_types*' -type f | xargs -I {} cp {} /opt/test-data/sstables/user_defined_types/
find /var/lib/cassandra/data/cqlite_test -name '*composite_key*' -type f | xargs -I {} cp {} /opt/test-data/sstables/composite_key/
find /var/lib/cassandra/data/cqlite_test -name '*indexed_table*' -type f | xargs -I {} cp {} /opt/test-data/sstables/indexed_table/
"

echo "📊 Generating test data manifest..."
docker exec cqlite-cassandra5-test bash -c "
cat > /opt/test-data/test-manifest.json << 'MANIFEST'
{
  \"cqlite_test_data\": {
    \"cassandra_version\": \"5.0\",
    \"format_version\": \"oa\",
    \"generated_at\": \"$(date -Iseconds)\",
    \"tables\": {
      \"primitive_types\": {
        \"description\": \"All CQL primitive data types\",
        \"record_count\": 3,
        \"data_types\": [\"UUID\", \"TEXT\", \"INT\", \"BIGINT\", \"FLOAT\", \"DOUBLE\", \"BOOLEAN\", \"BLOB\", \"TIMESTAMP\", \"DATE\", \"TIME\", \"INET\", \"DECIMAL\", \"VARINT\", \"DURATION\"]
      },
      \"collection_types\": {
        \"description\": \"Collection types (LIST, SET, MAP) including frozen variants\",
        \"record_count\": 1,
        \"data_types\": [\"LIST\", \"SET\", \"MAP\", \"FROZEN<LIST>\", \"FROZEN<SET>\", \"FROZEN<MAP>\"]
      },
      \"user_defined_types\": {
        \"description\": \"User Defined Types and nested structures\",
        \"record_count\": 1,
        \"data_types\": [\"UDT\", \"FROZEN<UDT>\", \"LIST<FROZEN<UDT>>\"]
      },
      \"composite_key\": {
        \"description\": \"Composite primary key with clustering columns\",
        \"record_count\": 3,
        \"key_structure\": \"partition_key, clustering_key1, clustering_key2\"
      },
      \"indexed_table\": {
        \"description\": \"Table with secondary index\",
        \"record_count\": 3,
        \"indexes\": [\"idx_indexed_col\"]
      }
    }
  }
}
MANIFEST
"

# List generated files
echo "📁 Generated SSTable files:"
docker exec cqlite-cassandra5-test find /opt/test-data/sstables -type f | sort

echo "🎉 Cassandra 5+ test data generation complete!"
echo ""
echo "📋 Summary:"
echo "   • Single Cassandra 5 node successfully created test data"
echo "   • 5 test tables with comprehensive data type coverage"
echo "   • SSTable files available in /opt/test-data/sstables/"
echo "   • Test manifest available at /opt/test-data/test-manifest.json"
echo ""
echo "✅ Ready for CQLite compatibility validation!"
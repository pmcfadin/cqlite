#!/bin/bash

echo "🗂️ Extracting SSTable files from Cassandra 5 cluster..."

# Create extraction directory
mkdir -p /samples/sstables

# Extract SSTables from all nodes
for node in cassandra-node1 cassandra-node2 cassandra-node3; do
    echo "📁 Extracting from $node..."
    
    # Create node directory
    mkdir -p /samples/sstables/$node
    
    # Find all SSTable files
    docker exec $node find /var/lib/cassandra/data -name "*.db" -type f > /tmp/${node}_sstables.txt
    
    # Copy SSTable files and related files
    while IFS= read -r sstable; do
        # Get base name without extension
        base=$(echo "$sstable" | sed 's/\.db$//')
        
        # Copy all related files for this SSTable
        for ext in db index summary statistics filter toc digest crc32; do
            if docker exec $node test -f "${base}.${ext}"; then
                echo "  📄 Copying ${base}.${ext}"
                docker cp "$node:${base}.${ext}" "/samples/sstables/$node/" 2>/dev/null || true
            fi
        done
    done < /tmp/${node}_sstables.txt
done

# List extracted files
echo "📋 Extracted files:"
find /samples/sstables -type f | sort

# Create metadata file
echo "📝 Creating metadata file..."
cat > /samples/sstables/metadata.json << EOF
{
  "extraction_date": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "cassandra_version": "5.0",
  "cluster_name": "test-cluster",
  "nodes": ["cassandra-node1", "cassandra-node2", "cassandra-node3"],
  "keyspaces": [
    {
      "name": "test_keyspace",
      "tables": [
        "all_types",
        "collections_table", 
        "users",
        "time_series",
        "multi_clustering",
        "large_table",
        "counters",
        "static_test"
      ]
    },
    {
      "name": "test_keyspace_nts",
      "tables": []
    }
  ],
  "compression_types": ["LZ4Compressor", "SnappyCompressor", "ZstdCompressor", "DeflateCompressor"],
  "data_types_tested": [
    "UUID", "TEXT", "ASCII", "VARCHAR", "BIGINT", "BLOB", "BOOLEAN", "COUNTER",
    "DATE", "DECIMAL", "DOUBLE", "DURATION", "FLOAT", "INET", "INT", "SMALLINT",
    "TIME", "TIMESTAMP", "TIMEUUID", "TINYINT", "VARINT", "LIST", "SET", "MAP",
    "UDT", "FROZEN"
  ]
}
EOF

echo "✅ SSTable extraction complete!"
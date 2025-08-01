# 🗄️ Testing CQLite with Real Cassandra 5+ Data

## 🚀 Method 1: Docker Cassandra 5 (Fastest - 5 minutes)

### Step 1: Start Cassandra 5 Container
```bash
# Start Cassandra 5 with Docker
docker run -d --name cassandra5-test \
  -p 9042:9042 \
  -e CASSANDRA_CLUSTER_NAME=TestCluster \
  cassandra:5.0

# Wait for it to start (check logs)
docker logs -f cassandra5-test
# Wait until you see: "Starting listening for CQL clients"
```

### Step 2: Create Test Data
```bash
# Connect with cqlsh
docker exec -it cassandra5-test cqlsh

# Create keyspace and tables with various data types
CREATE KEYSPACE test_keyspace WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
};

USE test_keyspace;

# Create table with all CQL types
CREATE TABLE all_types (
  id UUID PRIMARY KEY,
  text_col TEXT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  boolean_col BOOLEAN,
  timestamp_col TIMESTAMP,
  list_col LIST<TEXT>,
  set_col SET<INT>,
  map_col MAP<TEXT, INT>
);

# Insert sample data
INSERT INTO all_types (id, text_col, int_col, bigint_col, float_col, double_col, boolean_col, timestamp_col, list_col, set_col, map_col)
VALUES (uuid(), 'Hello Cassandra 5', 42, 1234567890, 3.14, 2.71828, true, '2024-01-01 12:00:00', ['item1', 'item2'], {1, 2, 3}, {'key1': 100, 'key2': 200});

# Insert more data
INSERT INTO all_types (id, text_col, int_col) VALUES (uuid(), 'Test Row 2', 100);
INSERT INTO all_types (id, text_col, int_col) VALUES (uuid(), 'Test Row 3', 200);

# Force flush to create SSTables
NODETOOL flush test_keyspace all_types;

# Exit cqlsh
EXIT;
```

### Step 3: Extract SSTable Files
```bash
# Copy SSTables from container to local directory
docker exec cassandra5-test find /var/lib/cassandra/data -name "*.db" | head -5

# Create local test data directory
mkdir -p ./real_cassandra5_data

# Copy the SSTable files
docker cp cassandra5-test:/var/lib/cassandra/data/test_keyspace/all_types-* ./real_cassandra5_data/

# List what we got
ls -la ./real_cassandra5_data/
```

### Step 4: Test with CQLite
```bash
# Test our SSTable reader with real data!
RUST_LOG=info cargo run --package cqlite-core --bin sstable_data_demo -- --data-dir ./real_cassandra5_data

# Or use the CLI
cargo build --package cqlite-cli --release
./target/release/cqlite --data-dir ./real_cassandra5_data

# Run validation tests
cargo run --package cqlite-core --bin issue_17_validation_demo -- --data-dir ./real_cassandra5_data
```

---

## 🛠️ Method 2: Generate Test Data with Our Scripts

### Use Built-in Data Generation
```bash
# Use the test data generation scripts
cd test-data/docker
docker-compose up cassandra5-data-generator

# Or run our automated data generation
./test-data/scripts/generate-all-test-data.sh --version 5.0

# Check generated data
ls -la test-data/generated/v5.0/
```

---

## 📊 Method 3: Download Sample Cassandra 5 Data

### Get Pre-made SSTable Files
```bash
# Create a temporary Cassandra 5 instance with sample data
# This creates various data types and scenarios

# Script to generate comprehensive test data
cat > generate_comprehensive_data.cql << 'EOF'
CREATE KEYSPACE comprehensive_test WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
};

USE comprehensive_test;

-- Table with collections
CREATE TABLE collections_test (
  id UUID PRIMARY KEY,
  name TEXT,
  tags SET<TEXT>,
  attributes MAP<TEXT, TEXT>,
  scores LIST<INT>
);

-- Table with UDTs (User Defined Types)
CREATE TYPE address (
  street TEXT,
  city TEXT,
  zip INT
);

CREATE TABLE users (
  user_id UUID PRIMARY KEY,
  name TEXT,
  email TEXT,
  addresses LIST<FROZEN<address>>,
  created_at TIMESTAMP
);

-- Insert test data
INSERT INTO collections_test (id, name, tags, attributes, scores) 
VALUES (uuid(), 'Test Item 1', {'tag1', 'tag2'}, {'color': 'red', 'size': 'large'}, [85, 90, 78]);

INSERT INTO users (user_id, name, email, addresses, created_at)
VALUES (uuid(), 'John Doe', 'john@example.com', 
        [{street: '123 Main St', city: 'New York', zip: 10001}], 
        '2024-01-15 10:30:00');

-- Force flush
NODETOOL flush comprehensive_test;
EOF

# Run this in your Cassandra 5 container
docker exec -i cassandra5-test cqlsh < generate_comprehensive_data.cql
```

---

## 🔍 Method 4: Inspect Real SSTable Structure

### Examine SSTable Format
```bash
# Use hexdump to see the binary format
hexdump -C ./real_cassandra5_data/*.db | head -20

# Check file sizes and types
file ./real_cassandra5_data/*

# Use our format detector
cargo run --package cqlite-core --bin format_validator -- --input ./real_cassandra5_data
```

---

## 🧪 Method 5: Automated Real Data Testing

### Complete Test Pipeline
```bash
# Use our comprehensive testing script
./scripts/run_issue_17_tests.sh --with-real-data

# Or run the automated orchestrator with real data
./scripts/automated_test_orchestrator.sh --data-source cassandra5 --data-scale MEDIUM

# Validate against real Cassandra data
cargo test --package cqlite-integration-tests real_data -- --nocapture
```

---

## 🎯 Expected Results When Testing with Real Data

### Format Detection Output:
```
🔍 Analyzing SSTable: /real_cassandra5_data/nb-1-big-Data.db
✅ Format detected: Cassandra 5.0 NewBig format
✅ Magic number: 0x0040_0000 (valid)
✅ Version compatibility: SUPPORTED
```

### Data Reading Output:
```
📊 Reading table: comprehensive_test.collections_test
✅ Partition keys: 25 partitions processed
✅ Collections found: 
   - SET<TEXT>: 25 sets processed
   - MAP<TEXT,TEXT>: 25 maps processed  
   - LIST<INT>: 25 lists processed
✅ Data integrity: All records validated
```

### Performance with Real Data:
```
🚀 Performance with Real Data:
   📈 Parse speed: 156.3 MB/s
   💾 Memory usage: 45.2 MB
   ⚡ Records/sec: 145,230
   📁 File size: 2.1 MB processed
```

---

## 🐛 Troubleshooting Real Data Issues

### Common Issues:

#### 1. **Container Won't Start**
```bash
# Check if port is in use
lsof -i :9042

# Use different port
docker run -d --name cassandra5-test -p 9043:9042 cassandra:5.0
```

#### 2. **No SSTable Files Created**
```bash
# Force flush explicitly
docker exec cassandra5-test nodetool flush

# Check data directory
docker exec cassandra5-test find /var/lib/cassandra/data -name "*.db"
```

#### 3. **Permission Issues**
```bash
# Fix permissions
sudo chown -R $USER:$USER ./real_cassandra5_data/
chmod -R 755 ./real_cassandra5_data/
```

#### 4. **CQLite Can't Read Files**
```bash
# Verify file format
file ./real_cassandra5_data/*.db

# Check our format detector
RUST_LOG=debug cargo run --package cqlite-core --bin format_validator -- --input ./real_cassandra5_data
```

---

## 🎉 Success Indicators

When testing with real Cassandra 5 data, you should see:

1. **✅ Format Recognition**: "Cassandra 5.x format detected"
2. **✅ Compression Handling**: "LZ4/Snappy decompression successful"  
3. **✅ Data Type Parsing**: "All CQL types processed correctly"
4. **✅ Collection Support**: "Lists/Sets/Maps parsed successfully"
5. **✅ Performance Targets**: "Parse speed >100MB/s achieved"

---

## 🚀 Quick Start Command

**Copy this entire block to get started immediately:**

```bash
#!/bin/bash
echo "🚀 Setting up real Cassandra 5 data testing..."

# Start Cassandra 5
docker run -d --name cassandra5-test -p 9042:9042 cassandra:5.0

# Wait for startup
echo "⏳ Waiting for Cassandra to start..."
sleep 30

# Create test data
docker exec cassandra5-test cqlsh -e "
CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE test;
CREATE TABLE demo (id UUID PRIMARY KEY, name TEXT, data LIST<TEXT>);
INSERT INTO demo (id, name, data) VALUES (uuid(), 'Test Record', ['item1', 'item2']);
"

# Flush to disk
docker exec cassandra5-test nodetool flush test demo

# Copy SSTables
mkdir -p ./real_test_data
docker cp cassandra5-test:/var/lib/cassandra/data/test/demo-*/. ./real_test_data/

# Test with CQLite!
echo "🧪 Testing CQLite with real Cassandra 5 data..."
cargo run --package cqlite-core --bin sstable_data_demo -- --data-dir ./real_test_data

echo "🎉 Real data testing complete!"
```

**Run this script and you'll have real Cassandra 5 SSTables to test with in under 5 minutes!** 🚀
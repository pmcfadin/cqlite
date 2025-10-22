# Dataset Generation Workflow

Complete guide to generating test data using Cassandra 5.0 Docker containers.

## Workflow Overview

```
┌─────────────────┐
│  start-clean.sh │  Start Cassandra 5 + apply schemas
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   generate.sh   │  Generate N rows per table
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    export.sh    │  Copy SSTables from container
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│shutdown-clean.sh│  Stop & clean volumes
└─────────────────┘
```

## Step 1: Start Clean Cassandra

### Purpose
Start fresh Cassandra 5.0 instance with desired schemas applied.

### Command
```bash
cd test-data
./scripts/start-clean.sh
```

### What It Does

1. **Starts Docker Compose**
   ```bash
   docker-compose -f docker/docker-compose-cassandra5.yml up -d
   ```

2. **Waits for Cassandra Health**
   - Polls `nodetool status` until UN (Up/Normal)
   - Typically takes 30-60 seconds

3. **Applies Schemas**
   - Reads `schemas/core.list` (default)
   - Executes each .cql file via cqlsh
   - Example from core.list:
     ```
     basic-types.cql
     collections.cql
     time-series.cql
     wide-rows.cql
     ```

4. **Validates**
   - Verifies keyspaces created
   - Checks table count matches schemas

### Environment Variables

**SCHEMA_SET**
- `core` (default): Use curated `schemas/core.list`
- `all`: Apply all `schemas/*.cql` files

**Example:**
```bash
# Use only core schemas
SCHEMA_SET=core ./scripts/start-clean.sh

# Use all available schemas
SCHEMA_SET=all ./scripts/start-clean.sh
```

### Output
```
[start-clean] Starting Cassandra 5.0...
[start-clean] Waiting for Cassandra to be ready...
[start-clean] Cassandra is UP
[start-clean] Applying schema: basic-types.cql
[start-clean] Applying schema: collections.cql
[start-clean] Applying schema: time-series.cql
[start-clean] Cassandra 5.0 ready and schemas applied.
```

### Troubleshooting

**Port 9042 already in use:**
```bash
lsof -i :9042
# Kill process or edit docker/docker-compose-cassandra5.yml
```

**Container fails to start:**
```bash
docker logs cassandra-5-0
# Check for Java errors or port conflicts
```

## Step 2: Generate Data

### Purpose
Populate tables with type-correct test data.

### Command
```bash
ROWS=1000 ./scripts/generate.sh
```

### What It Does

1. **Executes Python Generator**
   ```bash
   docker exec data-generator python3 /scripts/generate_comprehensive_test_data.py \
       --version 5.0 \
       --host cassandra-5-0 \
       --port 9042 \
       --scale SMALL \
       --rows-per-table 1000
   ```

2. **Connects to Cassandra**
   - Uses DataStax driver
   - Connects to cassandra-5-0:9042
   - Creates session with LOCAL_QUORUM

3. **For Each Table:**
   - Discovers schema via system tables
   - Generates type-appropriate values:
     - `int`: Random integers
     - `text`: Lorem ipsum or random strings
     - `uuid`: UUID v4
     - `timestamp`: Recent timestamps
     - `list/set/map`: Collections with 3-10 elements
     - `frozen<udt>`: Nested structures
   - Uses prepared statements for insertion
   - Batches for efficiency (100 rows per batch)

4. **Flushes Memtables**
   ```python
   session.execute("NODETOOL flush")
   ```
   Forces data to disk as SSTables.

5. **Produces Metadata**
   Creates `metadata.yml`:
   ```yaml
   version: "5.0"
   generated: "2025-10-21T10:30:00Z"
   tables:
     test_basic.simple_table:
       rows: 1000
       columns: 18
       size_bytes: 45000
     test_collections.collection_table:
       rows: 1000
       columns: 5
       size_bytes: 120000
   ```

### Environment Variables

**ROWS** (Number)
Rows to generate per table.
```bash
ROWS=500 ./scripts/generate.sh    # 500 rows each
ROWS=10000 ./scripts/generate.sh  # 10K rows each
```

**TABLES** (Comma-separated)
Generate only for specific tables.
```bash
TABLES=simple_table ./scripts/generate.sh
TABLES=simple_table,collection_table ./scripts/generate.sh
```

**SCALE** (SMALL | MEDIUM | LARGE)
Preset size configurations.
```bash
SCALE=SMALL ./scripts/generate.sh   # 100 rows, small values
SCALE=MEDIUM ./scripts/generate.sh  # 1000 rows, medium values
SCALE=LARGE ./scripts/generate.sh   # 10000 rows, large values (1MB text, etc.)
```

### Data Generation Strategy

**Primitive Types:**
- `int`: Uniform random
- `text`: Mix of Lorem ipsum and edge cases (empty, unicode)
- `timestamp`: Last 30 days
- `uuid`: Random v4
- `boolean`: 50/50 distribution

**Collections:**
- Size: 3-10 elements
- Set: Unique values
- Map: Random key-value pairs
- Nested: Up to 3 levels deep (frozen)

**Edge Cases (10% of rows):**
- Null values
- Empty strings
- Empty collections
- Maximum values (e.g., max int)
- Minimum values

### Output
```
[generate] Connecting to cassandra-5-0:9042
[generate] Generating data for test_basic.simple_table (1000 rows)
[generate] ████████████████████ 100% (1000/1000)
[generate] Generating data for test_collections.collection_table (1000 rows)
[generate] ████████████████████ 100% (1000/1000)
[generate] Flushing memtables...
[generate] Writing metadata.yml
[generate] Complete: 2 tables, 2000 rows, 165KB total
```

### Logs
Detailed logs written to:
```
test-data/logs/data_generation.log
test-data/logs/generation_statistics_v5.0.json
```

## Step 3: Export SSTables

### Purpose
Copy generated SSTables from container to host filesystem.

### Command
```bash
./scripts/export.sh
```

### What It Does

1. **Stops Cassandra**
   ```bash
   docker-compose stop cassandra-5-0
   ```
   Ensures consistent snapshot.

2. **Identifies SSTables**
   Finds all SSTable components in `/var/lib/cassandra/data/`:
   - `*-Data.db`
   - `*-Index.db`
   - `*-Statistics.db`
   - `*-Summary.db`
   - `*-CompressionInfo.db` (if compressed)
   - `*-TOC.txt`

3. **Copies to Host**
   ```bash
   docker cp cassandra-5-0:/var/lib/cassandra/data/ test-data/datasets/sstables/
   ```

4. **Preserves Structure**
   ```
   datasets/sstables/
   ├── keyspace1/
   │   ├── table1/
   │   │   ├── na-1-big-Data.db
   │   │   ├── na-1-big-Index.db
   │   │   ├── na-1-big-Statistics.db
   │   │   └── ...
   │   └── table2/
   └── keyspace2/
   ```

5. **Copies Metadata**
   ```bash
   docker cp cassandra-5-0:/tmp/metadata.yml test-data/datasets/
   ```

### Output Structure
```
test-data/datasets/
├── metadata.yml
├── sstables/
│   ├── test_basic/
│   │   └── simple_table/
│   │       ├── na-1-big-Data.db
│   │       ├── na-1-big-Index.db
│   │       ├── na-1-big-Statistics.db
│   │       ├── na-1-big-Summary.db
│   │       └── na-1-big-TOC.txt
│   ├── test_collections/
│   │   └── collection_table/
│   └── test_timeseries/
│       └── sensor_data/
```

### File Sizes (typical)
- Data.db: 10KB - 10MB (depends on ROWS)
- Index.db: 1KB - 1MB
- Statistics.db: 1-5KB
- Summary.db: <1KB
- CompressionInfo.db: <1KB (if compressed)

## Step 4: Shutdown & Clean

### Purpose
Stop Cassandra and remove Docker volumes for clean slate.

### Command
```bash
./scripts/shutdown-clean.sh
```

### What It Does

1. **Stops Containers**
   ```bash
   docker-compose -f docker/docker-compose-cassandra5.yml down
   ```

2. **Removes Volumes**
   ```bash
   docker volume rm $(docker volume ls -q | grep cassandra)
   ```
   Deletes all Cassandra data (already exported).

3. **Cleans Up**
   - Removes temporary files
   - Resets for next generation

### When to Use
- After exporting SSTables
- Before regenerating data
- When changing schemas significantly

## Repeated Generation

You can generate → export → generate again without shutdown:

```bash
# Initial generation
./scripts/start-clean.sh
ROWS=1000 ./scripts/generate.sh
./scripts/export.sh datasets/v1/

# Modify data or schema
docker exec cassandra-5-0 cqlsh -e "TRUNCATE test_basic.simple_table;"

# Regenerate
ROWS=2000 ./scripts/generate.sh
./scripts/export.sh datasets/v2/
```

## Advanced Usage

### Custom Data Generator

Edit `scripts/generate_comprehensive_test_data.py`:

```python
def generate_custom_data(session, table_name):
    """Generate custom test scenario"""
    
    # Example: Generate sequential IDs
    for i in range(1000):
        session.execute(
            f"INSERT INTO {table_name} (id, value) VALUES (?, ?)",
            [i, f"value_{i}"]
        )
    
    # Example: Generate clustered rows
    for partition in range(10):
        for cluster in range(100):
            session.execute(
                f"INSERT INTO {table_name} (pk, ck, value) VALUES (?, ?, ?)",
                [partition, cluster, f"p{partition}_c{cluster}"]
            )
```

### Multiple Versions

Generate data for multiple Cassandra versions:

```bash
# Cassandra 5.0
./scripts/start-clean.sh
ROWS=1000 ./scripts/generate.sh
./scripts/export.sh
mv datasets/sstables datasets/sstables-5.0

# Cassandra 4.1 (requires different docker-compose file)
# ... (if supporting multiple versions in future)
```

## CI Integration

### Packaged Datasets

For CI without Docker:

```bash
# Package current dataset
./scripts/package_datasets.sh

# Output: cqlite-test-data-v5.0-20251021.tar.gz
# Upload to GitHub releases
```

### CI Usage

```yaml
# .github/workflows/test.yml
- name: Download test data
  run: |
    wget https://github.com/pmcfadin/cqlite/releases/download/test-data-v5.0/cqlite-test-data-v5.0.tar.gz
    tar xzf cqlite-test-data-v5.0.tar.gz -C test-data/datasets/
    
- name: Run tests
  run: cargo test
```

## Performance

### Generation Speed
- Simple table (18 columns): ~200 rows/sec
- Collection table: ~100 rows/sec
- Wide rows (1000 clustering): ~50 partitions/sec

### Dataset Sizes
| Scale  | Rows/Table | Total Size | Time    |
|--------|------------|------------|---------|
| SMALL  | 100        | ~50KB      | 10 sec  |
| MEDIUM | 1000       | ~500KB     | 60 sec  |
| LARGE  | 10000      | ~50MB      | 10 min  |

## Next Steps

After generating data:
1. Verify SSTables exist in `datasets/sstables/`
2. Check `metadata.yml` for row counts
3. Run parser tests: `cargo test`
4. Validate with sstabledump (see [validation-workflow.md](validation-workflow.md))
5. Commit packaged dataset to releases


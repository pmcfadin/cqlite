# CQLite Docker-based Test Data Generation (Cassandra 5–focused)

**Issue #18**: Comprehensive Docker-based test data generation system for CQLite SSTable compatibility testing.

## 🎯 Overview

This system generates comprehensive test data using a Docker-based Cassandra setup, with primary support for Cassandra 5.0 (aligned with the PRD’s v1.0 scope to read Cassandra 5 SSTables). Legacy scripts for 3.x/4.x remain, but multi-version compose stacks have been removed. We only support Cassandra 5.

## 🐳 Supported Cassandra Versions

- **Cassandra 5.0** — Primary, actively supported

## 📁 Directory Structure

```
test-data/
├── docker/
│   ├── docker-compose-cassandra5.yml
│   └── Dockerfile.data-generator
├── schemas/
│   ├── basic-types.cql
│   ├── collections.cql
│   ├── time-series.cql
│   └── wide-rows.cql
├── scripts/
│   ├── compose-guard.sh
│   ├── generate_comprehensive_test_data.py
│   ├── export-sstables.sh
│   ├── validate-data.sh
│   └── cleanup.sh
├── logs/                 # .gitignored (generator logs and stats)
└── datasets/             # optional (real fixtures, cassandra5/bti)
```

Only core items are shown above. Internal/legacy assets (e.g., additional configs, older helpers) are kept but omitted here for clarity.

## 📐 Design Intent (PRD snapshot)

This README doubles as a light PRD for the test-data suite.

- Goals
  - Cassandra 5 only; high-fidelity SSTables covering core CQL types and patterns.
  - One simple dev workflow: start clean → generate → flush+count+export → shutdown.
  - Export preserves Cassandra directory layout; metadata.yml summarizes counts and schema.

- Developer flows
  1) Start clean
     - Bring up `cassandra-5-0`, wait healthy, apply all `.cql` schemas.
  2) Generate data
     - From `scripts/`, generate N rows for all or selected table groups; type-correct random-ish values.
  3) Flush + Count + Export
     - `nodetool flush`; per-table `SELECT count(*) ALLOW FILTERING` → write `datasets/metadata.yml`.
     - Destructive export to `datasets/sstables/`, preserving directory tree.
  4) Shutdown clean
     - Stop and remove volumes so the next run is clean.

- Scripts (current + planned)
  - `compose-guard.sh` (current): start + health-check.
  - `start-clean.sh` (planned): compose-guard + apply schemas.
  - `generate.sh` (planned): run generator with ROWS/TABLES/SCALE flags via `docker exec`.
  - `export.sh` (planned): flush → count → metadata.yml → destructive export.
  - `shutdown-clean.sh` (planned): `down -v`.

- Generator contract (planned extensions)
  - Flags: `--version 5.0`, `--host`, `--port`, `--scale`, optional `--rows-per-table N`, `--tables groupA,groupB`.
  - Behavior: type-correct values for UUID/TIMESTAMP/INET/DECIMAL/collections/UDTs; non-zero exit on error.

- Acceptance criteria
  - Start-clean in <2 minutes; schemas applied without manual steps.
  - Generate 10 / 1,000 / 1,000,000 rows per selected group without errors.
  - Export includes Data/Index/Filter/Statistics/Summary/TOC for all SSTables.
  - `datasets/metadata.yml` present with counts matching `SELECT count(*)`.
  - Re-running export replaces prior export cleanly; shutdown resets volumes.

## 📜 Schema Files

- `schemas/basic-types.cql`
  - Primitive types, composite/multi-partition examples, compression variants (Snappy/Deflate/Uncompressed).
- `schemas/collections.cql`
  - SET/LIST/MAP basics, nested collections, large collections, UDT-in-collections.
- `schemas/time-series.cql`
  - Time-window compaction, TTL usage, clustered time buckets (sensor data, app metrics, user activity, finance).
- `schemas/wide-rows.cql`
  - Wide partitions with many clustering columns, many-columns table, large blobs, chat/messages patterns.
- `schemas/hardened_validator_test_schema.cql`
  - Extended UDTs and complex/nested types for parser/validator coverage (cross-version exercises).

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose (v2: `docker compose`)
- Python 3.9+ (to run the generator locally, or build the generator image)
- 8GB+ available disk space
- 4GB+ available RAM

### Generate Test Data

1. **Start Cassandra 5.0 (clean) and apply schemas:**
   ```bash
   # From repo root
   bash test-data/scripts/start-clean.sh
   ```

2. **Generate data using the generator container:**
   ```bash
   # Examples
   SCALE=SMALL test-data/scripts/generate.sh
   ROWS=1000 SCALE=MEDIUM test-data/scripts/generate.sh
   ROWS=1000000 TABLES=collections test-data/scripts/generate.sh
   ```

   Optional: Use the containerized generator instead of local Python.
   ```bash
   cd test-data
   docker build -f docker/Dockerfile.data-generator -t cqlite-data-gen .
   docker run --rm \
     -e PYTHONUNBUFFERED=1 \
     -v "$(pwd)/logs:/logs" \
     --add-host host.docker.internal:host-gateway \
     cqlite-data-gen python3 /scripts/generate_comprehensive_test_data.py \
       --version 5.0 --host host.docker.internal --port 9046 --scale SMALL
   ```

3. **Export SSTables + metadata**
   ```bash
   test-data/scripts/export.sh
   ```

### Cleanup

```bash
cd test-data/scripts
test-data/scripts/shutdown-clean.sh
```

## 📊 Generated Data Types

### Basic Types Tables
- **simple_table**: All fundamental CQL data types
- **composite_key_table**: Composite primary keys with clustering
- **multi_partition_table**: Multiple partition keys
- **compression_test_table**: Different compression algorithms
- **uncompressed_table**: Uncompressed data for comparison
- **ttl_test_table**: Time-to-live testing
- **static_columns_table**: Static columns and counters
- **counters**: Counter data types

### Collections Tables
- **collection_table**: Basic SET, LIST, MAP collections
- **nested_collections_table**: Maps with collection values
- **large_collections_table**: Performance testing with large collections
- **collections_with_udts**: Collections containing User Defined Types
- **frozen_collections_table**: Frozen collections for atomic updates
- **typed_collections_table**: Collections with various data types

### Time Series Tables
- **sensor_data**: IoT sensor data with time window compaction
- **app_metrics**: Application metrics with TTL
- **user_activity**: User activity tracking by date
- **stock_prices**: Financial time series data
- **log_entries**: Log aggregation with bucketing
- **event_store**: Event sourcing patterns
- **time_bucketed_counters**: Time-based counter aggregations

### Wide Rows Tables
- **wide_partition_table**: Many clustering columns
- **many_columns_table**: Hundreds of columns per row
- **large_blob_table**: Large binary data storage
- **chat_messages**: Message storage with attachments
- **document_versions**: Document versioning system
- **product_catalog**: Rich product attributes
- **multi_metric_timeseries**: Many metrics per time point

## 🎛️ Configuration Options

### Data Scale Settings

Set `TEST_DATA_SCALE` environment variable:

- **SMALL**: ~1K rows per table (fast testing)
- **MEDIUM**: ~5K rows per table (development)
- **COMPREHENSIVE**: ~10-50K rows per table (full testing) [default]
- **LARGE**: ~100K+ rows per table (performance testing)

### Compression Algorithms

Generated data includes all major compression types:
- **SnappyCompressor** (default)
- **LZ4Compressor** 
- **DeflateCompressor**
- **Uncompressed**

### Compaction Strategies

- **SizeTieredCompactionStrategy** (STCS)
- **LeveledCompactionStrategy** (LCS) 
- **TimeWindowCompactionStrategy** (TWCS)

## 🔧 Advanced Usage

### Run only the generator with custom settings

```bash
# Example: LARGE scale against local Cassandra 5 (port mapped to 9046)
python3 scripts/generate_comprehensive_test_data.py \
  --version 5.0 --host 127.0.0.1 --port 9046 --scale LARGE
```

### Custom Data Scale

```bash
# Prefer the --scale flag on the generator CLI (SMALL/MEDIUM/COMPREHENSIVE/LARGE)
python3 scripts/generate_comprehensive_test_data.py --version 5.0 --scale COMPREHENSIVE --port 9046
```

### Selective Data Generation

Modify the generator or schema set to focus on specific areas:

```bash
# Option A: Edit scripts/generate_comprehensive_test_data.py to skip certain table groups
# Option B: Temporarily trim schemas/ to the desired .cql files and rerun
# (Legacy) generate-all-test-data.sh is for 3.x/4.x flows and not required for 5.0
```

## 📋 Validation & Quality Assurance

### Automated Validation

Validation helpers exist, but are currently geared toward 3.x/4.x. Cassandra 5.0 validation is being updated. The checks include:
- ✅ Directory structure completeness
- ✅ SSTable file integrity  
- ✅ Metadata file validity
- ✅ Expected data volumes
- ✅ File format correctness

### Manual Validation (recommended for 5.0 now)

```bash
# Check datasets sizes
du -sh test-data/datasets/

# Count SSTable files
find test-data/datasets/sstables -name "*.db" | wc -l

# Validate with CQLite (adjust path to one of the exported Data.db files)
cqlite info test-data/datasets/sstables/test_basic/simple_table/na-1-big-Data.db
```

## 🔄 CI/CD Integration

### GitHub Actions Workflow

The system includes a comprehensive GitHub Actions workflow (`.github/workflows/test-data-generation.yml`) that:

1. **Generates data** for all Cassandra versions in parallel
2. **Validates** data quality and completeness
3. **Archives** test data as artifacts
4. **Tests integration** with CQLite
5. **Creates releases** for main branch

### CI Gate: Real datasets only

- Tests must use the canonical Cassandra 5 corpus under `test-data/datasets/`.
- CI fetches a versioned dataset release asset and caches it to `test-data/datasets`.
- Local parity: fetch with `test-data/scripts/fetch-datasets.sh` (uses `datasets-v2` full by default).
- Do not commit datasets to git; do not use synthetic or alternate fixture paths.

### Workflow Triggers

- Push to main/develop branches
- Pull requests affecting test data
- Weekly scheduled runs (Sundays 2 AM UTC)
- Manual workflow dispatch

### Workflow Configuration

```yaml
# Manual trigger with custom settings
workflow_dispatch:
  inputs:
    cassandra_versions:
      description: 'Versions to generate (3.7,3.11,4.0,4.1)'  
      default: '3.7,3.11,4.0,4.1'
    data_scale:
      description: 'Data scale (SMALL/MEDIUM/COMPREHENSIVE/LARGE)'
      default: 'COMPREHENSIVE'
```

## 🛠️ Troubleshooting

### Common Issues

**Docker containers not starting:**
```bash
# Check Docker daemon status
sudo systemctl status docker

# Check available resources
docker system df
docker system prune -f
```

**Data generation fails:**
```bash
# Check container logs
docker-compose logs test-data-generator

# Increase timeout for large datasets
export GENERATION_TIMEOUT=7200  # 2 hours
```

**SSTable export fails:**
```bash
# Check Cassandra data directories
docker exec cassandra-4-1 ls -la /var/lib/cassandra/data/

# Verify permissions
docker exec cassandra-4-1 ls -la /var/lib/cassandra/
```

**Validation fails:**
```bash
# Run validation with verbose output
./scripts/validate-data.sh --verbose

# Check individual validation reports
ls -la test-data/generated/validation-reports/
```

### Performance Optimization

**Speed up generation:**
- Use SMALL or MEDIUM data scale for development
- Generate single versions instead of all
- Increase Docker resource limits

**Reduce disk usage:**
- Clean up between runs: `./scripts/cleanup.sh --all`
- Use compression: `TEST_DATA_SCALE=SMALL`
- Remove old generated data regularly

## 📈 Performance Metrics

### Expected Generation Times

| Data Scale | Time | Disk Usage | 
|-----------|------|------------|
| SMALL | 5-10 min | ~100MB |
| MEDIUM | 10-20 min | ~500MB |
| COMPREHENSIVE | 20-45 min | ~2GB |
| LARGE | 45-90 min | ~10GB+ |

### Generated Data Statistics

**Per Cassandra Version:**
- ~36,000 total rows across all tables
- ~500MB-1GB total size (depending on compression)
- ~100-500 SSTable files
- Complete metadata and schema documentation

## 🔗 Integration with CQLite

### Using Generated Test Data

```bash
# List generated SSTables
find test-data/generated -name "*-Data.db"

# Test with CQLite info command  
cqlite info test-data/generated/v4.1/sstables/test_basic/simple_table/na-1-big-Data.db

# Test with CQLite query command
cqlite query test-data/generated/v4.1/sstables/test_basic/simple_table/ "SELECT * FROM simple_table LIMIT 10"
```

### Continuous Integration

The generated test data is automatically used in CQLite's CI pipeline to:
- Validate SSTable reading across Cassandra versions
- Test parsing of different data types and patterns
- Benchmark performance with various file sizes
- Ensure backward compatibility

## 🤝 Contributing

### Adding New Schema Types

1. Create new `.cql` file in `schemas/`
2. Update `generate-all-test-data.sh` to include new data generation
3. Add validation checks in `validate-data.sh`
4. Update documentation

### Extending Data Generation

1. Modify Python data generator in `generate-all-test-data.sh`
2. Add new data patterns or edge cases
3. Update validation expectations
4. Test with multiple Cassandra versions

### Improving Validation

1. Add new validation checks in `validate-data.sh`
2. Enhance metadata extraction
3. Add performance benchmarks
4. Improve error reporting

## 📄 License

This test data generation system is part of the CQLite project and follows the same licensing terms.

## 🆘 Support

- **Issues**: Report problems on GitHub Issues with `test-data` label
- **Discussions**: Join GitHub Discussions for questions
- **Documentation**: See `docs/` directory for additional guides

---

**Generated by**: CQLite Docker Test Data System  
**Issue**: #18 - Set up Docker-based test data generation  
**Last Updated**: $(date)
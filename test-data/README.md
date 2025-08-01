# CQLite Docker-based Test Data Generation

**Issue #18**: Comprehensive Docker-based test data generation system for CQLite SSTable compatibility testing.

## 🎯 Overview

This system generates comprehensive test data using Docker-based Cassandra instances across multiple versions to create various SSTable files for testing, validation, and development purposes.

## 🐳 Supported Cassandra Versions

- **Cassandra 3.7** - Legacy version support
- **Cassandra 3.11** - Stable legacy version  
- **Cassandra 4.0** - Modern version with new features
- **Cassandra 4.1** - Latest supported version

## 📁 Directory Structure

```
test-data/
├── docker/
│   └── docker-compose.yml          # Multi-version Docker setup
├── scripts/
│   ├── generate-all-test-data.sh   # Master data generation script
│   ├── export-sstables.sh          # SSTable export automation
│   ├── cleanup.sh                  # Environment cleanup
│   └── validate-data.sh            # Data quality validation
├── schemas/
│   ├── basic-types.cql             # Fundamental CQL data types
│   ├── collections.cql             # SET, LIST, MAP collections
│   ├── time-series.cql             # Time-based data with TTLs
│   └── wide-rows.cql               # Wide partitions and tables
└── generated/
    ├── v3.7/                       # Cassandra 3.7 test data
    ├── v3.11/                      # Cassandra 3.11 test data
    ├── v4.0/                       # Cassandra 4.0 test data
    └── v4.1/                       # Cassandra 4.1 test data
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for data generation scripts)
- 8GB+ available disk space
- 4GB+ available RAM

### Generate Test Data

1. **Start the generation process:**
   ```bash
   cd test-data/docker
   docker-compose up
   ```

2. **Wait for completion** (typically 15-30 minutes depending on data scale)

3. **Validate the generated data:**
   ```bash
   cd ../scripts
   ./validate-data.sh
   ```

### Cleanup

```bash
cd test-data/scripts
./cleanup.sh --all
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

### Generate Specific Versions Only

```bash
# Generate only Cassandra 4.1 data
cd test-data/docker
docker-compose up cassandra-4-1 test-data-generator sstable-exporter
```

### Custom Data Scale

```bash
export TEST_DATA_SCALE=LARGE
cd test-data/docker
docker-compose up
```

### Selective Data Generation

Modify the data generation script to focus on specific schemas:

```bash
# Edit generate-all-test-data.sh
# Comment out unwanted data generation calls
./scripts/generate-all-test-data.sh
```

## 📋 Validation & Quality Assurance

### Automated Validation

The validation script checks:
- ✅ Directory structure completeness
- ✅ SSTable file integrity  
- ✅ Metadata file validity
- ✅ Expected data volumes
- ✅ File format correctness

### Manual Validation

```bash
# Check generated data sizes
du -sh test-data/generated/v*/

# Count SSTable files
find test-data/generated -name "*.db" | wc -l

# Validate with CQLite
cqlite info test-data/generated/v4.1/sstables/test_basic/simple_table/na-1-big-Data.db
```

## 🔄 CI/CD Integration

### GitHub Actions Workflow

The system includes a comprehensive GitHub Actions workflow (`.github/workflows/test-data-generation.yml`) that:

1. **Generates data** for all Cassandra versions in parallel
2. **Validates** data quality and completeness
3. **Archives** test data as artifacts
4. **Tests integration** with CQLite
5. **Creates releases** for main branch

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
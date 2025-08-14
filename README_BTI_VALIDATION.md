# BTI Validation Suite - Local Testing Guide

## Overview

This guide shows how to run the BTI validation suite locally for Issue #36 testing.

## Prerequisites

- Docker installed and running
- Rust toolchain
- Cassandra 5.0 Docker image

## Quick Start

### 1. Generate BTI Test Datasets

```bash
# Generate BTI datasets using Cassandra 5.0 Docker
./scripts/generate_bti_datasets.sh

# This will create BTI datasets in: test-data/cassandra5/bti/
# - multi_component_keys/
# - wide_partitions/  
# - complex_types/
# - range_tombstones/
# - nested_collections/
```

### 2. Run BTI Validation

```bash
# Run comprehensive BTI validation suite
./scripts/run_bti_validation.sh

# The script uses the Issue #30 harness for consistency:
# test-data/scripts/run-sstabledump-validator.sh
```

### 3. Manual Dataset Testing

You can test individual datasets by setting environment variables:

```bash
# Set environment for specific dataset
export DATASET_DIRS="test-data/cassandra5/bti/multi_component_keys"
export DATASET_LIST="multi_component_keys"
export ZERO_TOLERANCE="true"
export VALIDATION_MODE="bti"

# Run validation using Issue #30 harness
./test-data/scripts/run-sstabledump-validator.sh
```

## Dataset Structure

Each BTI dataset should contain these required files:
```
test-data/cassandra5/bti/[scenario]/
├── nb-1-big-Data.db          # SSTable data
├── nb-1-big-Partitions.db    # BTI trie structure (required for BTI)
├── nb-1-big-Rows.db          # BTI row index (required for BTI)
├── nb-1-big-Index.db         # Legacy index (optional)
├── nb-1-big-Statistics.db    # Table statistics
├── nb-1-big-Summary.db       # Partition summary
├── nb-1-big-CompressionInfo.db # Compression metadata
├── nb-1-big-TOC.txt          # Table of contents
└── nb-1-big-Digest.crc32     # File checksum
```

**BTI-specific files**:
- `*-Partitions.db` - Contains BTI trie structure for partition lookup
- `*-Rows.db` - Contains BTI row index for clustering key navigation

## Environment Variables

The BTI validation respects these environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATASET_DIRS` | Comma-separated paths to datasets | Auto-detected |
| `DATASET_LIST` | Comma-separated dataset names | Auto-detected |
| `ZERO_TOLERANCE` | Enable zero-diff parity requirement | `true` |
| `VALIDATION_MODE` | Set to "bti" for BTI-specific validation | `bti` |
| `CASSANDRA_VERSION` | Cassandra version for dataset generation | `5.0` |

## Validation Workflow

The BTI validation follows this process:

1. **Dataset Generation** (`scripts/generate_bti_datasets.sh`)
   - Starts Cassandra 5.0 with BTI format enabled
   - Creates tables with `sstable_format='bti'`
   - Generates test data for each scenario
   - Copies BTI files to test-data directories
   - Validates required files are present

2. **Validation Execution** (`scripts/run_bti_validation.sh`)
   - Uses Issue #30 harness for consistency
   - Sets BTI-specific environment variables
   - Runs zero-tolerance parity validation
   - Generates comprehensive validation reports

3. **Artifact Generation**
   - JUnit XML reports (via Issue #30 harness)
   - Validation summary markdown files
   - Performance benchmark results
   - BTI-specific validation evidence

## Troubleshooting

### Docker Issues
```bash
# Check if Cassandra container is running
docker ps | grep cassandra

# Check container logs
docker logs cqlite-cassandra-5-0

# Restart if needed
cd test-data/docker
docker-compose -f docker-compose-cassandra5.yml restart cassandra-5-0
```

### Missing BTI Files
If validation fails due to missing Partitions.db or Rows.db files:

```bash
# Check if BTI format was enabled
docker exec cqlite-cassandra-5-0 cqlsh -e "
  SELECT table_name, sstable_format 
  FROM system_schema.tables 
  WHERE keyspace_name = 'bti_test_issue36';
"

# Should show: sstable_format = 'bti'
```

### Validation Failures
```bash
# Run with verbose output
RUST_LOG=debug ./scripts/run_bti_validation.sh

# Check specific dataset
export DATASET_DIRS="test-data/cassandra5/bti/multi_component_keys"
./test-data/scripts/run-sstabledump-validator.sh --verbose
```

## Integration with CI

The BTI validation integrates with the CI pipeline:

```yaml
# .github/workflows/bti-validation.yml
- name: Generate BTI datasets
  run: ./scripts/generate_bti_datasets.sh

- name: Run BTI validation  
  run: ./scripts/run_bti_validation.sh
  env:
    ZERO_TOLERANCE: true
    VALIDATION_MODE: bti
```

## Performance Expectations

Expected validation performance:
- **Trie traversal**: < 100ms per 1000 operations
- **Throughput**: > 500 operations/second
- **Memory usage**: < 100MB peak
- **Total validation**: < 5 minutes for full suite

## Related Documentation

- [Issue #36](https://github.com/pmcfadin/cqlite/issues/36) - BTI validation requirements
- [Issue #30](https://github.com/pmcfadin/cqlite/issues/30) - Validator infrastructure
- [CEP-25](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-25) - BTI format specification

---

For questions or issues, please refer to the [main project documentation](README.md) or open a GitHub issue.
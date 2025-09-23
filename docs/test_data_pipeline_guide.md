# CQLite Test Data Pipeline Guide

## Overview

The CQLite Test Data Pipeline is a comprehensive system for generating, validating, and managing test data for Cassandra 5 SSTable testing. This pipeline ensures data quality, catches regressions, and provides performance benchmarks for CQLite development.

## Features

- **Automated Test Data Generation**: Creates comprehensive test datasets covering various Cassandra 5 scenarios
- **Component Integrity Validation**: Verifies SSTable component files and relationships
- **Format Compliance Checking**: Ensures Cassandra 5 format specification compliance
- **Golden Reference Data**: Maintains reference datasets for regression testing
- **Performance Benchmarking**: Tracks read performance and memory usage
- **CI/CD Integration**: Integrates with GitHub Actions, Jenkins, and other CI systems
- **Version Management**: Tracks dataset versions with semantic versioning
- **Quality Gates**: Enforces quality standards before data acceptance

## Architecture

```
cqlite/
├── scripts/
│   ├── test_data_generator.py     # Core data generation
│   ├── validate_sstables.py       # Validation engine
│   ├── data_pipeline_manager.py   # Pipeline orchestration
│   ├── ci_integration.py          # CI/CD integration
│   └── run_pipeline.sh            # Convenience runner
├── config/
│   ├── pipeline_config.yml        # Main configuration
│   └── custom_validation_rules.yml # Custom validation rules
├── test-data/
│   ├── datasets/                  # Generated test datasets
│   ├── versions/                  # Version management
│   ├── benchmarks/                # Performance data
│   └── reports/                   # Validation reports
└── ci-reports/                    # CI pipeline reports
```

## Quick Start

### 1. Initial Setup

```bash
# Setup the pipeline (first time only)
./scripts/run_pipeline.sh setup

# This will:
# - Create directory structure
# - Install Python dependencies
# - Setup Git hooks
# - Generate CI configuration files
```

### 2. Generate Test Data

```bash
# Generate all test data categories
./scripts/run_pipeline.sh generate

# Generate specific categories only
./scripts/run_pipeline.sh generate --categories basic_types,collections

# Force regeneration even if current data is valid
./scripts/run_pipeline.sh generate --force
```

### 3. Validate Data

```bash
# Validate all test data
./scripts/run_pipeline.sh validate

# Validate with custom CQLite binary
./scripts/run_pipeline.sh validate --cqlite-binary ./target/release/cqlite
```

### 4. Run Regression Tests

```bash
# Run regression tests against golden reference data
./scripts/run_pipeline.sh regression
```

### 5. Performance Benchmarking

```bash
# Run performance benchmarks
./scripts/run_pipeline.sh benchmark
```

## Test Data Categories

### Basic Data Types
- **primitive_types_test**: All primitive Cassandra data types
- **unicode_text_test**: Unicode text handling and edge cases
- **large_blob_test**: Large blob data handling

### Collection Types
- **basic_collections**: Lists, sets, maps
- **nested_collections**: Nested and frozen collections
- **empty_collections**: Edge cases with empty collections

### Compression Scenarios
- **compression_lz4**: LZ4 compressed data
- **compression_snappy**: Snappy compressed data
- **compression_zstd**: ZSTD compressed data
- **compression_deflate**: Deflate compressed data

### TTL Scenarios
- **ttl_expired**: Expired TTL data for read-time filtering
- **ttl_mixed**: Mixed TTL scenarios (expired, active, no TTL)

### Tombstone Scenarios
- **cell_tombstones**: Cell-level tombstones
- **range_tombstones**: Range tombstones for clustering keys
- **row_tombstones**: Row-level tombstones

### Performance Benchmarks
- **wide_partitions**: Wide partitions for read performance testing
- **many_partitions**: Many small partitions for index performance

### Edge Cases
- **empty_values**: Empty strings, null values, minimal data
- **boundary_values**: Min/max values for numeric types

### Cassandra 5 Features
- **vector_types**: Vector type support (if available)
- **sai_indexes**: Storage Attached Indexes test data

## Configuration

### Main Configuration (`config/pipeline_config.yml`)

```yaml
# Base settings
base_dir: "."
max_parallel_tasks: 4
retention_days: 30

# Quality gates
quality_gates:
  max_errors: 0
  max_warnings: 10
  min_datasets: 10

# Performance thresholds
performance_thresholds:
  max_read_time_seconds: 30.0
  min_throughput_lines_per_second: 1000.0
```

### Custom Validation Rules (`config/custom_validation_rules.yml`)

Define project-specific validation rules:

```yaml
# File-level validation
file_validation:
  required_components:
    - Data.db
    - Index.db
    - Summary.db
    - Statistics.db

# Performance requirements
performance_validation:
  read_performance:
    max_read_time_seconds: 30.0
    min_throughput_rows_per_second: 1000.0
```

## Pipeline Commands

### Data Generation

```bash
# Generate all datasets
python3 scripts/test_data_generator.py --base-dir . --categories all

# Generate specific categories
python3 scripts/test_data_generator.py --categories basic_types,collections

# With custom Cassandra installation
python3 scripts/test_data_generator.py --cassandra-home /opt/cassandra
```

### Validation

```bash
# Validate SSTable directory
python3 scripts/validate_sstables.py test-data/datasets/ \
    --cqlite-binary ./target/release/cqlite \
    --output-report validation_report.json \
    --recursive

# Quick validation for pre-commit
python3 scripts/ci_integration.py validate-quick
```

### Pipeline Management

```bash
# Full pipeline regeneration
python3 scripts/data_pipeline_manager.py generate --force

# Run regression tests
python3 scripts/data_pipeline_manager.py regression

# Check pipeline status
python3 scripts/data_pipeline_manager.py status

# Cleanup old versions
python3 scripts/data_pipeline_manager.py cleanup
```

## CI/CD Integration

### GitHub Actions

The pipeline automatically generates a GitHub Actions workflow (`.github/workflows/test-data-validation.yml`):

```yaml
name: CQLite Test Data Validation

on:
  pull_request:
    paths:
      - 'test-data/**'
      - 'scripts/**'
      - 'cqlite-core/**'
  push:
    branches: [ main ]
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM

jobs:
  validate-test-data:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    - name: Build CQLite
      run: cargo build --release
    - name: Validate test data
      run: |
        python3 scripts/ci_integration.py pr-validation \
          --cqlite-binary ./target/release/cqlite
```

### Pre-commit Hooks

Install pre-commit hooks for quick validation:

```bash
./scripts/run_pipeline.sh setup

# Or manually
python3 scripts/ci_integration.py setup-hooks
```

### Jenkins Integration

Generate Jenkins pipeline configuration:

```bash
python3 scripts/ci_integration.py generate-config --ci-type jenkins > Jenkinsfile.test-data
```

## Quality Gates

Quality gates ensure data meets minimum standards:

### Error Thresholds
- **Max Errors**: 0 (no validation errors allowed)
- **Max Warnings**: 10 (limited warnings acceptable)

### File Requirements
- **Min Data File Size**: 1KB minimum
- **Component Completeness**: All required SSTable components present
- **Cross-component Consistency**: Index/summary/data relationships valid

### Performance Requirements
- **Read Time**: < 30 seconds per SSTable
- **Throughput**: > 1000 lines/second
- **Memory Usage**: < 512 MB

### Regression Requirements
- **Success Rate**: > 95% of regression tests must pass
- **Golden Reference**: Output must match reference checksums

## Golden Reference Data

Golden reference data provides regression testing:

### Generation
```bash
# Generate golden references during pipeline run
./scripts/run_pipeline.sh generate

# References stored in test-data/datasets/golden_references/
```

### Structure
```
test-data/datasets/golden_references/
├── summary.json                 # Reference summary
├── dataset1/
│   ├── reference_output.txt     # Expected CQLite output
│   └── output_checksum.txt      # Output checksum
└── dataset2/
    ├── reference_output.txt
    └── output_checksum.txt
```

### Regression Testing
```bash
# Run regression tests
./scripts/run_pipeline.sh regression

# Returns exit code 0 if all tests pass, 1 if any fail
```

## Performance Benchmarking

### Metrics Collected
- **Read Time**: Time to read entire SSTable
- **Throughput**: Lines per second
- **Memory Usage**: Peak memory consumption
- **CPU Usage**: CPU utilization during read

### Benchmark Storage
```
test-data/benchmarks/
├── latest_benchmarks.json       # Latest benchmark results
├── benchmarks_20240923_140000.json  # Historical results
└── benchmark_trends.json       # Performance trends
```

### Performance Regression Detection
- Compares current performance against thresholds
- Tracks performance trends over time
- Alerts on significant regressions

## Troubleshooting

### Common Issues

#### 1. Missing Dependencies
```bash
# Install Python dependencies
pip3 install pyyaml requests

# Install Cassandra (if needed for data generation)
# Follow Cassandra installation guide
```

#### 2. CQLite Binary Not Found
```bash
# Build CQLite
cargo build --release

# Or specify path explicitly
./scripts/run_pipeline.sh validate --cqlite-binary /path/to/cqlite
```

#### 3. Validation Failures
```bash
# Check validation report
cat ci-reports/validation_latest.json

# Run verbose validation
./scripts/run_pipeline.sh validate --verbose
```

#### 4. Regression Test Failures
```bash
# Check regression report
cat test-data/reports/latest_regression_test.json

# Regenerate golden references if needed
./scripts/run_pipeline.sh generate --force
```

### Debug Mode

Enable verbose logging:

```bash
# Set debug environment
export PYTHONPATH="scripts:$PYTHONPATH"
export CQLITE_LOG_LEVEL=DEBUG

# Run with verbose output
./scripts/run_pipeline.sh validate --verbose
```

### Log Files

Pipeline logs are stored in:
- `logs/pipeline.log` - Main pipeline log
- `ci-reports/` - CI validation reports
- `test-data/reports/` - Detailed validation reports

## Best Practices

### Development Workflow

1. **Before Code Changes**:
   ```bash
   # Ensure current data is valid
   ./scripts/run_pipeline.sh validate
   ```

2. **After Code Changes**:
   ```bash
   # Run regression tests
   ./scripts/run_pipeline.sh regression
   ```

3. **Before Releases**:
   ```bash
   # Full pipeline validation
   ./scripts/run_pipeline.sh ci --force
   ```

### Data Management

1. **Regular Regeneration**: Regenerate test data weekly or after significant changes
2. **Version Control**: Keep dataset versions for rollback capability
3. **Performance Monitoring**: Track performance trends over time
4. **Quality Monitoring**: Monitor quality gate metrics

### CI/CD Integration

1. **PR Validation**: Run validation on all PRs affecting test data or core code
2. **Nightly Builds**: Run full pipeline validation nightly
3. **Performance Alerts**: Alert on performance regressions
4. **Automated Cleanup**: Regular cleanup of old data versions

## Advanced Usage

### Custom Dataset Generation

Create custom dataset configurations:

```python
# Custom dataset config
config = DatasetConfig(
    name="custom_test",
    keyspace="test_custom",
    table="custom_table",
    description="Custom test scenario",
    row_count=5000,
    compression_type="LZ4Compressor",
    enable_ttl=True,
    enable_tombstones=True,
    # ... other settings
)

# Generate dataset
generator = CassandraTestDataGenerator(".", "/opt/cassandra")
dataset = generator._generate_dataset_from_config(config)
```

### Custom Validation Rules

Add project-specific validation:

```python
# Custom validator
class CustomSSTableValidator(SSTableValidator):
    def _validate_custom_logic(self, components, issues, metrics):
        # Custom validation logic
        pass

# Use in pipeline
validator = CustomSSTableValidator()
results = validator.validate_sstable_directory(path)
```

### Performance Analysis

Analyze performance trends:

```python
# Load benchmark history
with open("test-data/benchmarks/latest_benchmarks.json") as f:
    benchmarks = json.load(f)

# Analyze trends
for benchmark in benchmarks["benchmarks"]:
    read_time = benchmark["benchmarks"]["read_time_seconds"]
    throughput = benchmark["benchmarks"]["read_throughput_lines_per_second"]
    # Analyze performance...
```

## API Reference

### Core Classes

#### `CassandraTestDataGenerator`
- `generate_comprehensive_test_suite()`: Generate all test categories
- `_generate_basic_types_datasets()`: Generate basic type tests
- `_generate_collections_datasets()`: Generate collection tests
- `_generate_compression_datasets()`: Generate compression tests

#### `SSTableValidator`
- `validate_sstable_directory(path)`: Validate SSTable directory
- `validate_sstable(components)`: Validate single SSTable
- `generate_validation_report(results)`: Generate validation report

#### `DataPipelineManager`
- `generate_full_test_suite()`: Generate complete test suite
- `run_regression_tests()`: Run regression tests
- `_create_performance_benchmarks()`: Create performance benchmarks

#### `CIIntegration`
- `run_pr_validation()`: Run PR validation
- `setup_pre_commit_hooks()`: Setup Git hooks
- `generate_ci_config()`: Generate CI configuration

### Configuration Objects

#### `DatasetConfig`
- Dataset generation configuration
- Controls table schema, data types, compression, etc.

#### `ValidationRule`
- Custom validation rule definition
- Specifies rule type, expected values, tolerances

#### `PipelineConfig`
- Overall pipeline configuration
- Controls quality gates, performance thresholds, etc.

## Contributing

### Adding New Test Categories

1. Create new dataset generation method in `test_data_generator.py`
2. Add category to `generate_comprehensive_test_suite()`
3. Update configuration files
4. Add documentation

### Adding Custom Validation Rules

1. Add rules to `custom_validation_rules.yml`
2. Implement validation logic in `validate_sstables.py`
3. Add tests for new validation
4. Update documentation

### Extending CI Integration

1. Add new CI system support in `ci_integration.py`
2. Create configuration templates
3. Add integration tests
4. Update documentation

## Support

For issues and questions:

1. Check existing [GitHub Issues](https://github.com/your-org/cqlite/issues)
2. Review pipeline logs in `logs/pipeline.log`
3. Check validation reports in `ci-reports/`
4. Create new issue with:
   - Pipeline command run
   - Configuration used
   - Error messages
   - Log files (if relevant)

## License

This test data pipeline is part of the CQLite project and follows the same license terms.
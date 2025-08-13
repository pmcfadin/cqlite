# SSTableDump Validator

A zero-tolerance cell-by-cell validation harness for CQLite's SSTable reading functionality. This tool ensures perfect compatibility with Cassandra's native `sstabledump` utility by comparing every cell, timestamp, and metadata field.

## Overview

This validation harness was created to address GitHub Issues #25, #26, and #28, providing comprehensive validation of CQLite's SSTable reading capabilities against the existing Docker Cassandra 5.0 test environment.

### Key Features

- 🔍 **Zero-Tolerance Validation**: Fails CI on ANY difference between CQLite and Cassandra output
- 📊 **Cell-by-Cell Comparison**: Compares every data cell, timestamp, TTL, and metadata field
- 🐳 **Docker Integration**: Uses existing Cassandra 5.0 Docker setup from the project
- 📋 **Comprehensive Reporting**: Generates detailed reports in multiple formats (Text, JSON, CSV, JUnit)
- ⚡ **CI/CD Integration**: Automatic validation on every commit and PR
- 🎯 **Multi-Version Support**: Tests compatibility across different Cassandra versions

## Architecture

```
┌─────────────────────┐    ┌─────────────────────┐
│   Docker Cassandra  │    │    CQLite Core      │
│   (Reference)       │    │   (Under Test)      │
└──────────┬──────────┘    └──────────┬──────────┘
           │                          │
           │ sstabledump              │ cqlite dump
           ▼                          ▼
┌─────────────────────┐    ┌─────────────────────┐
│  Cassandra Output   │    │   CQLite Output     │
│  (JSON/Text)        │    │   (JSON/Text)       │
└──────────┬──────────┘    └──────────┬──────────┘
           │                          │
           └──────────┬─────────────────┘
                      ▼
           ┌─────────────────────┐
           │  Cell-by-Cell       │
           │  Comparator         │
           └──────────┬──────────┘
                      ▼
           ┌─────────────────────┐
           │  Validation Report  │
           │  • Perfect Match    │
           │  • Differences      │
           │  • CI Pass/Fail     │
           └─────────────────────┘
```

## Installation

```bash
cd tools/sstabledump-validator
cargo build --release
```

## Usage

### Basic Validation

```bash
# Validate a single SSTable file
./target/release/sstabledump-validator validate /path/to/sstable.db

# Zero tolerance mode (fail on ANY difference)
./target/release/sstabledump-validator validate /path/to/sstable.db --fail-on-diff

# Generate detailed report
./target/release/sstabledump-validator validate /path/to/sstable.db --detailed
```

### Setup Docker Environment

```bash
# Setup Cassandra 5.0 container
./target/release/sstabledump-validator setup

# Setup specific version
./target/release/sstabledump-validator setup --version 4.1
```

### Generate Test Data

```bash
# Generate basic test data
./target/release/sstabledump-validator generate

# Generate comprehensive test data with edge cases
./target/release/sstabledump-validator generate --count 1000 --edge-cases
```

### Parse and Compare Dumps

```bash
# Parse a sstabledump output file
./target/release/sstabledump-validator parse /path/to/cassandra_dump.txt --json

# Compare two dump files directly
./target/release/sstabledump-validator compare \
    /path/to/cassandra_dump.txt \
    /path/to/cqlite_dump.txt \
    --zero-tolerance
```

## Validation Process

The validation harness follows this comprehensive process:

### 1. Environment Setup
- Ensures Docker Cassandra container is running
- Validates CQLite core is built and functional
- Sets up test data in Cassandra

### 2. Data Generation
- Creates comprehensive test datasets using Cassandra
- Includes basic types, collections, complex keys, edge cases
- Forces flush to ensure data is written to SSTables
- Extracts SSTable files from container

### 3. Dual Dump Generation
- Runs Cassandra's native `sstabledump` utility
- Runs CQLite's equivalent dump functionality
- Captures output in standardized format

### 4. Cell-by-Cell Comparison
- Parses both outputs into structured data
- Compares every cell value, timestamp, TTL
- Checks partition keys, clustering keys, column names
- Identifies missing data in either output

### 5. Report Generation
- Categorizes differences by severity (Critical, High, Medium, Low)
- Calculates compatibility percentage
- Generates actionable recommendations
- Outputs in multiple formats for CI integration

## Zero Tolerance Mode

When `--fail-on-diff` is enabled:

- ✅ **Perfect Match**: Validation passes, CI continues
- ❌ **ANY Difference**: Validation fails, CI stops with exit code 1

This ensures that CQLite maintains perfect compatibility with Cassandra's output.

## Report Formats

### Text Report
```
🔍 SSTABLEDUMP VALIDATION REPORT
==================================================
📁 SSTable: /path/to/test.db
⏰ Timestamp: 2024-01-15T14:30:22Z
🎯 Zero Tolerance Mode: true

✅ OVERALL STATUS: Perfect
📊 Compatibility: 100.00%

📈 COMPARISON STATISTICS
------------------------------
Total cells compared: 1,234,567
Matching cells: 1,234,567
Different cells: 0
```

### JSON Report
```json
{
  "sstable_path": "/path/to/test.db",
  "summary": {
    "overall_status": "Perfect",
    "compatibility_percentage": 100.0,
    "critical_issues": 0
  },
  "comparison_result": {
    "differences": []
  }
}
```

### JUnit XML (for CI)
```xml
<testsuite name="SSTableDump Validation" tests="1">
  <testcase name="sstabledump_validation_test.db" classname="ValidationHarness" />
</testsuite>
```

## CI Integration

The validation is automatically triggered:

- 📝 **Every Commit**: On pushes to main/develop branches
- 🔄 **Every PR**: Before merge approval
- ⏰ **Daily**: Scheduled validation runs
- 🎯 **Manual**: Workflow dispatch for testing

### GitHub Actions Workflow

The `.github/workflows/sstabledump-validation.yml` workflow:

1. Sets up Cassandra 5.0 service container
2. Builds CQLite core and validator
3. Generates comprehensive test data
4. Extracts SSTable files from Cassandra
5. Runs zero-tolerance validation
6. Reports results and artifacts
7. Comments on PRs if validation fails

## Error Handling

The validator handles various error scenarios:

- 🐳 **Docker Issues**: Container startup failures, connection problems
- 📁 **File Issues**: Missing SSTables, permission problems, corruption
- 🔧 **Parsing Issues**: Invalid dump formats, unexpected data structures
- 🚀 **Performance Issues**: Memory limits, timeout handling

## Development

### Running Tests

```bash
# Unit tests
cargo test

# Integration tests (requires Docker)
cargo test --test integration_tests

# All tests with verbose output
cargo test -- --nocapture
```

### Adding New Test Cases

1. Add test data generation in `src/docker.rs`
2. Implement parsing logic in `src/parser.rs`
3. Add comparison rules in `src/comparator.rs`
4. Create integration tests in `tests/`

### Debugging

Enable verbose logging:

```bash
RUST_LOG=debug ./target/release/sstabledump-validator validate /path/to/sstable.db --verbose
```

## Configuration

Environment variables:

- `CASSANDRA_HOST`: Docker container host (default: localhost)
- `CASSANDRA_PORT`: CQL port (default: 9042)
- `VALIDATION_TIMEOUT`: Timeout in seconds (default: 300)
- `ZERO_TOLERANCE`: Enable zero tolerance mode (default: true)

## Troubleshooting

### Common Issues

**Docker not available**:
```bash
# Check Docker is running
docker --version
docker ps
```

**Cassandra not ready**:
```bash
# Check Cassandra health
cqlsh -h localhost -e "SELECT cluster_name FROM system.local;"
```

**SSTable files not found**:
```bash
# Verify test data generation
./target/release/sstabledump-validator generate --count 10
```

**Validation failures**:
```bash
# Run with detailed reporting
./target/release/sstabledump-validator validate /path/to/sstable.db --detailed --verbose
```

### Performance Tuning

For large datasets:

- Increase `VALIDATION_TIMEOUT`
- Use streaming comparison for memory efficiency
- Enable parallel processing where possible
- Monitor Docker container resources

## Contributing

When adding new features:

1. 🧪 **Add Tests**: Include unit and integration tests
2. 📝 **Update Docs**: Document new functionality
3. 🔍 **Validate**: Ensure zero-tolerance validation still works
4. 📊 **Benchmark**: Test performance with large datasets

## License

This tool is part of the CQLite project and follows the same licensing terms.

## Related Issues

- **Issue #25**: SSTable Reading Validation
- **Issue #26**: Info Command Implementation  
- **Issue #28**: Docker Test Data Generation

The validation harness ensures that all these components work together seamlessly with perfect Cassandra compatibility.
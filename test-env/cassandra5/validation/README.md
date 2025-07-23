# SSTable Validation Programs

This directory contains comprehensive validation programs for testing the cqlite library's ability to parse different types of Cassandra 5+ SSTable files.

## Overview

The validation suite tests parsing of various SSTable formats:

- **all_types**: Validates parsing of all primitive CQL types (text, int, bigint, float, double, boolean, timestamp, uuid, etc.)
- **collections**: Validates parsing of CQL collection types (list, set, map)
- **users**: Validates parsing of User Defined Types (UDTs)
- **time_series**: Validates parsing of tables with clustering columns and time-based data
- **large_table**: Validates performance and scale characteristics

## Prerequisites

1. **SSTable Files**: Ensure SSTable files are generated and available in `../sstables/`
2. **Rust Environment**: Rust toolchain installed with cargo
3. **Dependencies**: All dependencies are specified in `Cargo.toml`

## Usage

### Quick Start

Run all validations with the convenience script:

```bash
./run_validation.sh
```

This will:
1. Build all validation programs
2. Run each validation program individually
3. Run the comprehensive validation suite
4. Generate detailed reports in JSON format
5. Create log files for debugging

### Individual Validation Programs

You can also run individual validation programs:

```bash
# Build first
cargo build --release

# Run specific validations
./target/release/validate_all_types
./target/release/validate_collections
./target/release/validate_users
./target/release/validate_time_series
./target/release/validate_large_table

# Run comprehensive suite
./target/release/validate_all
```

### Development Mode

For development and debugging:

```bash
# Build in debug mode
cargo build

# Run with debug info
./target/debug/validate_all_types
```

## Output Files

The validation programs generate several types of output:

### JSON Reports
- `validation_report_all_types.json` - Primitive types validation results
- `validation_report_collections.json` - Collections validation results  
- `validation_report_users.json` - UDT validation results
- `validation_report_time_series.json` - Time series validation results
- `validation_report_large_table.json` - Performance validation results
- `comprehensive_validation_report.json` - Overall summary report

### Log Files (when using run_validation.sh)
- `*_output.log` - Detailed execution logs for each program
- `comprehensive_validation_output.log` - Complete validation log

## Validation Tests

### All Types Validation (`validate_all_types`)

Tests parsing of primitive CQL types:
- Text values
- Integer values (int, bigint, varint)
- Floating point values (float, double, decimal)
- Boolean values
- Timestamp values
- UUID values
- Binary/blob values
- Network addresses (inet)

**Expected Results**: All primitive types should parse correctly and match expected formats.

### Collections Validation (`validate_collections`)

Tests parsing of CQL collection types:
- Lists (`list<type>`)
- Sets (`set<type>`)
- Maps (`map<keytype, valuetype>`)
- Frozen collections

**Expected Results**: Collection structures should be properly parsed with correct element counts and types.

### Users Validation (`validate_users`)

Tests parsing of User Defined Types:
- UDT field access
- Nested UDT structures
- Complex field types within UDTs

**Expected Results**: UDT fields should be accessible and properly structured.

### Time Series Validation (`validate_time_series`)

Tests parsing of time-based data structures:
- Partition key handling
- Clustering column ordering
- Time-based data access
- Temporal data distribution

**Expected Results**: Time-based ordering should be preserved and temporal queries should work correctly.

### Large Table Validation (`validate_large_table`)

Tests performance and scale characteristics:
- Random access performance
- Sequential scan performance
- Memory efficiency
- Concurrent access patterns
- Data integrity at scale

**Expected Results**: Performance should be within acceptable ranges and memory usage should be reasonable.

## Understanding Results

### Success Criteria

- **100% success rate**: All expected data types and structures are correctly parsed
- **80-99% success rate**: Most features work but some edge cases may fail
- **<80% success rate**: Significant parsing issues that need investigation

### JSON Report Structure

Each report contains:
```json
{
  "test_name": "validation_name",
  "timestamp": "2024-XX-XXTXX:XX:XX.XXXZ",
  "total_tests": 10,
  "successful_tests": 8,
  "failed_tests": 2,
  "results": [
    {
      "key": "test_key",
      "expected_type": "ExpectedType",
      "actual_type": "ActualType", 
      "value": "parsed_value",
      "matches": true
    }
  ]
}
```

### Performance Metrics

Large table validation includes performance metrics:
- Random access average time (milliseconds)
- Sequential scan rate (entries/second)
- Memory usage (MB)
- Concurrent throughput (operations/second)

## Troubleshooting

### Common Issues

1. **SSTable files not found**
   - Ensure SSTable files are generated in `../sstables/`
   - Check that Cassandra data generation completed successfully

2. **Build failures**
   - Verify Rust toolchain is up to date: `rustup update`
   - Check that all dependencies are available
   - Ensure cqlite-core library builds successfully

3. **Parsing failures**
   - Check if SSTable format is compatible with cqlite parser
   - Review parser implementation for the specific data type
   - Compare with Cassandra format specifications

4. **Performance issues**
   - Large tables may take time to scan completely
   - Memory usage may be high for very large datasets
   - Consider running individual tests instead of full suite

### Debug Mode

Run with environment variables for more detailed output:

```bash
RUST_LOG=debug ./target/debug/validate_all_types
```

### Manual Inspection

To inspect SSTable files manually:

```bash
# List SSTable structure
ls -la ../sstables/*/

# Check file sizes
du -h ../sstables/*/nb-1-big-Data.db
```

## Integration with CI/CD

These validation programs can be integrated into automated testing:

```bash
# Run validation and check exit code
if ./run_validation.sh; then
    echo "Validation passed"
    exit 0
else
    echo "Validation failed"
    exit 1
fi
```

## Extending Validation

To add new validation tests:

1. Create a new binary target in `Cargo.toml`
2. Implement validation logic following existing patterns
3. Add the new program to `run_validation.sh`
4. Update this README with test descriptions

## Support

For issues with validation programs:
1. Check existing JSON reports and logs
2. Verify SSTable files are valid
3. Review cqlite library documentation
4. Check Cassandra format compatibility
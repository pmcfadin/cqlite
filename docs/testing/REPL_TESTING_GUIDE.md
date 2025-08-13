# CQLite REPL Testing and Validation Guide

## Overview

This guide provides comprehensive documentation for testing and validating the CQLite REPL implementation, ensuring all quality gates are met for Issue #10 requirements.

## Testing Architecture

### Test Structure

```
tests/
├── src/
│   ├── repl_integration_tests.rs     # Rust-based integration tests
│   └── repl_quality_gates.rs         # Quality gate validation
├── repl_user_workflow_tests.sh       # User workflow validation
├── repl_real_data_validation.sh      # Real Cassandra data tests
└── run_comprehensive_repl_tests.sh   # Master test runner
```

### Quality Gates

The REPL implementation must pass all quality gates:

1. **Gate 1: REPL Launch** - REPL starts successfully with proper banner
2. **Gate 2: Commands Functional** - All required commands work correctly
3. **Gate 3: User Workflows** - End-to-end user scenarios complete
4. **Gate 4: Real Data Compatibility** - Works with real Cassandra data
5. **Gate 5: Error Handling** - Graceful error handling and recovery
6. **Gate 6: Performance & Usability** - Meets performance standards

## Running Tests

### Quick Start

```bash
# Build the project
cargo build --bin cqlite

# Run basic REPL tests
./test_repl_commands.sh

# Run comprehensive test suite
./tests/run_comprehensive_repl_tests.sh
```

### Individual Test Suites

#### 1. Basic Functionality Tests
```bash
./test_repl_commands.sh
```
Tests core REPL functionality including:
- Help system
- Configuration commands
- CQL query execution
- Error handling

#### 2. Integration Tests (Rust)
```bash
cargo test repl_integration_tests
```
Comprehensive Rust-based tests covering:
- REPL startup and initialization
- Command system validation
- Session management
- Quality gate validation

#### 3. Quality Gates Validation
```bash
cargo test repl_quality_gates
```
Validates all Issue #10 quality gates:
- REPL launch requirements
- Command functionality requirements
- User workflow requirements
- Real data compatibility
- Error handling standards
- Performance benchmarks

#### 4. User Workflow Tests
```bash
./tests/repl_user_workflow_tests.sh
```
Tests real-world user scenarios:
- New user onboarding
- Data exploration workflows
- Configuration management
- Query development
- Help navigation
- Session management
- Error recovery workflows

#### 5. Real Data Validation
```bash
./tests/repl_real_data_validation.sh
```
Tests compatibility with real Cassandra data:
- Data directory configuration
- Keyspace discovery
- Table discovery
- Schema introspection
- SSTable file integration
- Performance with real data

### Comprehensive Test Suite
```bash
./tests/run_comprehensive_repl_tests.sh
```
Runs all test suites in sequence and generates a comprehensive report.

## Test Configuration

### Environment Setup

```bash
# Set custom binary path
export CQLITE_BINARY="target/release/cqlite"

# Set test data directory
export TEST_DATA_DIR="tests/fixtures/cassandra-data"

# Enable verbose output
export VERBOSE=true
```

### Binary Requirements

Tests require the CQLite binary to be built:
```bash
cargo build --bin cqlite
# or for release
cargo build --release --bin cqlite
```

## Quality Gates Details

### Gate 1: REPL Launch
**Requirements:**
- REPL starts within 3 seconds
- Displays proper startup banner
- Shows correct prompt format
- Exits cleanly with quit commands

**Validation:**
```bash
# Tests startup time and banner display
echo ":quit" | timeout 5 ./target/debug/cqlite
```

### Gate 2: Commands Functional
**Requirements:**
- All meta-commands work (`:help`, `:config`, `:tables`, etc.)
- Help system provides comprehensive documentation
- Configuration system allows settings changes
- Data exploration commands function properly

**Validation:**
```bash
# Test all required commands
for cmd in ":help" ":config" ":tables" ":keyspaces"; do
    echo -e "$cmd\n:quit" | ./target/debug/cqlite
done
```

### Gate 3: User Workflows
**Requirements:**
- New users can discover functionality
- Data exploration workflows complete successfully
- Configuration workflows work end-to-end
- Query development workflows support iterative development

**Key Workflows:**
1. **New User Onboarding**: `:help` → `:help commands` → `:config` → `:keyspaces`
2. **Data Exploration**: `:keyspaces` → `:tables` → `:describe table` → `SELECT query`
3. **Configuration**: `:config` → `:config timing on` → `:config page-size 25`
4. **Query Development**: `:timing` → `SELECT query` → `:history`

### Gate 4: Real Data Compatibility
**Requirements:**
- Configures real Cassandra data directories
- Discovers keyspaces from file system
- Detects tables from SSTable files
- Handles various Cassandra versions (3.11, 4.0, 5.0)

**Test Data Structure:**
```
/var/lib/cassandra/data/
├── keyspace1/
│   ├── table1-uuid/
│   │   ├── mc-1-big-Data.db
│   │   ├── mc-1-big-Index.db
│   │   └── mc-1-big-Statistics.db
│   └── table2-uuid/
└── system/
    └── keyspaces-uuid/
```

### Gate 5: Error Handling
**Requirements:**
- Graceful handling of invalid CQL queries
- Helpful error messages with hints
- Recovery after errors
- No crashes or panics

**Error Scenarios:**
- Invalid CQL syntax
- Non-existent tables/keyspaces
- Invalid meta-commands
- Configuration errors
- Data directory issues

### Gate 6: Performance & Usability
**Requirements:**
- Startup time < 3 seconds
- Command response time < 2 seconds
- Query timing functionality works
- Help system is comprehensive and navigable
- User-friendly features available

## Test Data Management

### Creating Test Data

The test suite automatically creates test Cassandra data structures:
```bash
# Creates test keyspaces and tables
tests/fixtures/
├── test_keyspace/
│   ├── users-12345678901234567890123456789012/
│   │   ├── mc-1-big-Data.db
│   │   ├── mc-1-big-Index.db
│   │   └── mc-1-big-Statistics.db
│   └── orders-abcdefabcdefabcdefabcdefabcdef01/
└── system/
    └── keyspaces-98765432109876543210987654321098/
```

### Real Data Integration

To test with real Cassandra data:
1. Install Cassandra locally
2. Create test keyspaces and tables
3. Point tests to data directory:
   ```bash
   export CASSANDRA_DATA_DIR="/var/lib/cassandra/data"
   ./tests/repl_real_data_validation.sh
   ```

## Troubleshooting

### Common Issues

#### Binary Not Found
```bash
Error: Binary not found: target/debug/cqlite
Solution: cargo build --bin cqlite
```

#### Permission Denied
```bash
Error: Permission denied
Solution: chmod +x tests/*.sh
```

#### Test Timeouts
```bash
Error: TIMEOUT after 10s
Solution: Increase timeout or check binary responsiveness
```

#### Missing Test Data
```bash
Error: No real Cassandra data found
Solution: Install Cassandra or use test data only
```

### Debug Mode

Enable verbose output for debugging:
```bash
export VERBOSE=true
./tests/run_comprehensive_repl_tests.sh
```

## Continuous Integration

### GitHub Actions Integration

```yaml
name: REPL Tests
on: [push, pull_request]

jobs:
  repl-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Build
        run: cargo build --bin cqlite
      - name: Run REPL Tests
        run: ./tests/run_comprehensive_repl_tests.sh
```

### Quality Gates Enforcement

All quality gates must pass for CI success:
```bash
# Exit codes:
# 0 = All tests passed
# 1 = Some tests failed
# 2 = Critical failure (binary not found, etc.)
```

## Performance Benchmarks

### Baseline Performance

- **Startup Time**: < 2 seconds (target), < 3 seconds (acceptable)
- **Command Response**: < 1 second (target), < 2 seconds (acceptable)
- **Query Execution**: Depends on data size, timing reported
- **Memory Usage**: < 50MB baseline

### Performance Testing

```bash
# Startup time test
time (echo ":quit" | ./target/debug/cqlite >/dev/null)

# Command responsiveness test
for cmd in ":help" ":config" ":tables"; do
    time (echo -e "$cmd\n:quit" | ./target/debug/cqlite >/dev/null)
done
```

## Issue #10 Compliance Checklist

- ✅ **REPL Launch**: Interactive shell starts successfully
- ✅ **Command Structure**: All required commands implemented
- ✅ **Configuration**: Settings management functional
- ✅ **Data Exploration**: Keyspace/table discovery works
- ✅ **CQL Execution**: Query execution with timing
- ✅ **Help System**: Comprehensive documentation
- ✅ **History**: Command history tracking
- ✅ **Error Handling**: Graceful error messages
- ✅ **Real Data**: Cassandra data integration
- ✅ **Usability**: User-friendly features

## Conclusion

The CQLite REPL testing framework provides comprehensive validation ensuring:

1. **Functionality**: All features work as specified
2. **Quality**: High-quality user experience
3. **Reliability**: Robust error handling and recovery
4. **Performance**: Meets performance requirements
5. **Compatibility**: Works with real Cassandra data
6. **Compliance**: Fully meets Issue #10 requirements

The testing suite validates that the REPL is production-ready and provides an excellent user experience for CQLite users.
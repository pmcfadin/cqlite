# CQLite Validation Tools

This directory contains validation utilities and standalone test tools for CQLite's Cassandra compatibility.

## Tools Overview

### Binary Validator
- **`cqlite-validator`** - Main validation executable
  - Validates SSTable files against CQLite parser
  - Supports file and directory validation
  - Runs comprehensive compatibility test suites
  - Can generate test SSTable files using Docker + Cassandra

### Validation Test Suite
- **`validation_tests/`** - Complete validation framework
  - **`cassandra_sstable_validator.rs`** - Core Cassandra SSTable validation
  - **`real_cqlite_parser_test.rs`** - Real data parsing validation
  - **`cqlite_integration_test.rs`** - Integration test suite
  - **`real_sstable_validation.rs`** - Real SSTable format validation

### Standalone Test Tools
- **`standalone/`** - Individual validation utilities
  - **`cqlite-validator.rs`** - Standalone validator implementation
  - **`simple_sstable_test.rs`** - Basic SSTable structure validation
  - **`simple_vint_demo.rs`** - VInt encoding validation
  - **`sstable_validator_standalone.rs`** - Independent SSTable validator
  - **`standalone_vint_demo.rs`** - VInt encoding demo

## Usage

### Binary Validator Commands

```bash
# Validate a single SSTable file
./tools/validators/cqlite-validator file --path /path/to/sstable.db --verbose

# Validate directory of SSTable files
./tools/validators/cqlite-validator directory --path /path/to/sstables --pattern "*.db" --recursive

# Run compatibility test suites
./tools/validators/cqlite-validator test vint --report
./tools/validators/cqlite-validator test header --report
./tools/validators/cqlite-validator test types --report
./tools/validators/cqlite-validator test real --report
./tools/validators/cqlite-validator test all --report

# Generate test SSTable files
./tools/validators/cqlite-validator generate --output ./test-sstables --version 5.0
```

### Validation Test Suite

```bash
# Run validation tests from validation_tests directory
cd tools/validators/validation_tests

# Build all validators
cargo build --release

# Run specific validators
./target/release/cassandra_sstable_validator
./target/release/real_cqlite_parser_test
./target/release/cqlite_integration_test
./target/release/real_sstable_validation
```

### Standalone Tools

```bash
# Build and run standalone tools
cd tools/validators/standalone

# VInt validation demo
rustc simple_vint_demo.rs && ./simple_vint_demo

# SSTable structure test
rustc simple_sstable_test.rs && ./simple_sstable_test

# Standalone validator
rustc sstable_validator_standalone.rs && ./sstable_validator_standalone
```

## What Each Tool Validates

### CQLite Validator (Main Binary)
- **File validation**: Magic numbers, headers, basic structure
- **VInt encoding**: Variable-length integer compatibility
- **Header format**: SSTable header structure and versions
- **Type system**: CQL data type parsing (primitives, strings, UUIDs)
- **Real files**: Actual Cassandra SSTable compatibility
- **Comprehensive reporting**: Detailed compatibility reports

### Validation Test Suite
- **Cassandra SSTable Validator**: Deep format analysis and compatibility
- **Real CQLite Parser Test**: Validates parsing against real Cassandra data
- **Integration Test**: End-to-end validation pipeline
- **Real SSTable Validation**: Production SSTable format compliance

### Standalone Tools
- **Simple VInt Demo**: Basic VInt encoding/decoding validation
- **Simple SSTable Test**: Fundamental SSTable structure validation
- **Standalone Validator**: Independent validation without dependencies

## Dependencies Required

### Binary Validator
- Rust toolchain (cargo)
- clap for command-line interface
- chrono for timestamps
- Optional: Docker for test file generation

### Validation Test Suite
- cqlite-core dependency
- tokio async runtime
- serde for JSON handling
- anyhow for error handling

### Standalone Tools
- Minimal Rust standard library only
- No external dependencies (intentionally self-contained)

## Integration with CQLite

All validators use CQLite's core parsing logic to ensure compatibility validation matches the actual implementation. The validation results directly inform CQLite's compatibility matrix and guide development priorities.

⚠️ **Note**: Some validation tests may require updates due to CQLite core API changes. This is expected and will be addressed in future maintenance cycles.

### Compatibility Testing Workflow

1. **Generate test data** using Cassandra Docker containers
2. **Validate formats** using the binary validator
3. **Run test suites** to verify specific components
4. **Check real data** against production SSTable files
5. **Generate reports** for compatibility tracking

## Output and Reporting

All tools generate structured output for:
- Compatibility percentage scores
- Detailed failure analysis
- Format specification compliance
- Performance benchmarks
- Regression detection

Reports are saved in Markdown format for documentation and CI/CD integration.
# CQLite Validation Guide - User Acceptance Testing (UAT)

## 🚀 Quick Start

CQLite includes a comprehensive validation tool to verify Cassandra compatibility. This guide will help you perform UAT testing to validate CQLite's parser implementation.

## 📋 Prerequisites

1. **Rust installed** (for building the validator)
2. **Cassandra test files** (optional - the validator can help generate them)
3. **Docker** (optional - for generating fresh test files)

## 🔧 Installation

### Build the Validator Tool

```bash
cd tools/cqlite-validator
cargo build --release
```

The validator binary will be at: `target/release/cqlite-validator`

### Add to PATH (Optional)

```bash
# macOS/Linux
export PATH="$PATH:$(pwd)/target/release"

# Or create an alias
alias cqlite-validator="$(pwd)/target/release/cqlite-validator"
```

## 📖 Usage Guide

### 1. Validate a Single SSTable File

```bash
cqlite-validator file --path /path/to/sstable.db

# With verbose output showing VInt parsing and text patterns
cqlite-validator file --path /path/to/sstable.db --verbose
```

**Example Output:**
```
🔍 Validating SSTable file: nb-1-big-Data.db
📁 File size: 1151 bytes
🔢 Magic number: 0xA0070000
✅ Java metadata structure detected (Statistics/CompressionInfo)

✅ File validation complete
```

### 2. Validate a Directory of SSTable Files

```bash
# Validate all .db files in a directory
cqlite-validator directory --path /path/to/cassandra/data

# Recursively validate subdirectories
cqlite-validator directory --path /path/to/cassandra/data --recursive

# Custom file pattern
cqlite-validator directory --path /path/to/data --pattern "*-Data.db"
```

**Example Output:**
```
📂 Validating SSTable directory: /test-env/cassandra5/data
🔍 Pattern: *.db

📄 Checking: nb-1-big-Data.db
  ✅ Valid SSTable structure

📄 Checking: nb-1-big-Index.db
  ✅ Valid SSTable structure

📊 Directory Validation Summary:
Total files: 18
Passed: 18 (100%)
```

### 3. Run Compatibility Test Suites

```bash
# Test VInt encoding/decoding
cqlite-validator test vint

# Test SSTable header parsing
cqlite-validator test header

# Test CQL type system
cqlite-validator test types

# Test against real Cassandra files
cqlite-validator test real

# Run all tests with report generation
cqlite-validator test all --report
```

### 4. Generate Test SSTable Files

```bash
# Generate test files using Docker and Cassandra
cqlite-validator generate --output ./test-sstables --version 5.0
```

## 🧪 UAT Test Scenarios

### Scenario 1: Basic Compatibility Check

**Objective**: Verify CQLite can recognize valid Cassandra SSTable files

```bash
# 1. Navigate to your Cassandra data directory
cd /path/to/cassandra/data

# 2. Run directory validation
cqlite-validator directory --path . --recursive

# 3. Verify 100% pass rate
```

### Scenario 2: VInt Encoding Validation

**Objective**: Ensure VInt encoding/decoding is Cassandra-compatible

```bash
# Run VInt test suite
cqlite-validator test vint

# Expected output: All test values should pass
```

### Scenario 3: Real File Parsing

**Objective**: Validate parsing of actual Cassandra 5 SSTable files

```bash
# Test with verbose output on a real Data.db file
cqlite-validator file --path /path/to/nb-1-big-Data.db --verbose

# Look for:
# - Correct magic number detection
# - VInt values being parsed
# - Text patterns recognized
```

### Scenario 4: Comprehensive Validation

**Objective**: Full compatibility assessment with report

```bash
# Run all tests and generate report
cqlite-validator test all --report

# Review the generated report
cat cqlite-compatibility-report.md
```

## 📊 Interpreting Results

### Magic Numbers

CQLite recognizes these Cassandra magic numbers:
- `0x6F610000` - Cassandra 5 'oa' format
- `0x5A5A5A5A` - Standard SSTable magic
- Various others for Statistics/CompressionInfo files

### Success Indicators

✅ **PASS Criteria:**
- File has valid structure (≥8 bytes)
- Magic number recognized or Java metadata detected
- VInt values parse correctly
- No parsing errors

⚠️ **Warning Indicators:**
- Unknown magic number (may still be valid)
- Empty files (expected for some components)

❌ **FAIL Criteria:**
- File too small for SSTable header
- VInt parsing failures
- Corrupted data structures

## 🔍 Advanced Testing

### Custom Test Data

Create a test script to validate specific data patterns:

```rust
// test_custom.rs
fn main() {
    // Test specific VInt values
    let values = vec![0, 127, 128, 32767, -1, -32768];
    
    for v in values {
        println!("Testing VInt {}", v);
        // Your validation logic here
    }
}
```

### Performance Testing

While M2 defers performance testing, you can still check basic metrics:

```bash
# Time the validation of a large directory
time cqlite-validator directory --path /large/sstable/directory --recursive
```

## 🐛 Troubleshooting

### Common Issues

1. **"File not found" errors**
   - Check file paths are absolute
   - Verify permissions

2. **"Unknown format" warnings**
   - Normal for some Cassandra metadata files
   - Check with --verbose for more details

3. **Build errors**
   - Ensure Rust is up to date: `rustup update`
   - Check dependencies: `cargo check`

### Debug Mode

For detailed debugging information:

```bash
RUST_LOG=debug cqlite-validator file --path test.db --verbose
```

## 📝 Reporting Issues

When reporting compatibility issues, include:

1. **Validator output** (with --verbose flag)
2. **Cassandra version** that created the files
3. **File sizes** and types
4. **Magic numbers** reported

Example issue report:
```
cqlite-validator file --path problem.db --verbose > validation-log.txt
```

## 🎯 Expected UAT Outcomes

### M2 Compatibility Goals

- ✅ **100% VInt compatibility** with Cassandra format
- ✅ **Real file recognition** for all SSTable components
- ✅ **Magic number detection** for various formats
- ✅ **Basic data structure parsing**

### Known Limitations (Deferred to M3+)

- ⏳ Complex type parsing (Collections, UDTs)
- ⏳ Performance benchmarking
- ⏳ Compression algorithm validation
- ⏳ BTI format full support

## 💡 Tips for Effective UAT

1. **Start with known-good files** from a working Cassandra installation
2. **Test incrementally** - single files before directories
3. **Use verbose mode** to understand parsing behavior
4. **Compare results** across different Cassandra versions
5. **Document anomalies** even if they pass validation

## 🚀 Next Steps

After successful UAT validation:

1. **Report results** using the generated compatibility report
2. **Share feedback** on any compatibility issues found
3. **Test edge cases** with unusual data patterns
4. **Prepare for M3** complex type system testing

---

**Support**: For questions or issues, please refer to the troubleshooting section or create an issue in the CQLite repository with your validation logs.
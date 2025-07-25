# CQLite UAT Quick Start Guide

## 🚀 5-Minute Validation Test

### Step 1: Build the Validator

```bash
# In the CQLite root directory
rustc --edition 2021 -O cqlite-validator.rs -o cqlite-validator
```

### Step 2: Run Basic Tests

```bash
# Test the validator is working
./cqlite-validator test
```

Expected output: All 11 tests should PASS ✅

### Step 3: Validate Real Cassandra Files

If you have Cassandra SSTable files:

```bash
# Validate a single file
./cqlite-validator file /path/to/your/sstable.db

# Validate with detailed output
./cqlite-validator file /path/to/your/sstable.db --verbose

# Validate entire directory
./cqlite-validator dir /path/to/cassandra/data
```

### Step 4: Test with Sample Data

Using the included test environment:

```bash
# Validate all test SSTable files
./cqlite-validator dir test-env/cassandra5/data/cassandra5-sstables

# Examine a specific file in detail
./cqlite-validator file test-env/cassandra5/data/cassandra5-sstables/users-*/nb-1-big-Data.db --verbose
```

## 📊 What to Look For

### ✅ **Good Signs:**
- Magic number detected (e.g., `0xAD010000`, `0x6F610000`)
- Valid structure confirmed
- 100% validation rate for directories
- VInt values parsing correctly

### ⚠️ **Normal Warnings:**
- Empty files (some SSTable components are legitimately empty)
- Unknown magic numbers (metadata files have various formats)
- Java metadata detected (Statistics/CompressionInfo files)

### ❌ **Issues to Report:**
- "File too small" errors on non-empty files
- VInt parsing failures
- Unexpected validation failures

## 🎯 UAT Success Criteria

1. **Validator runs** without errors ✅
2. **Test suite passes** 100% ✅
3. **Real files validate** successfully ✅
4. **Magic numbers** are recognized ✅
5. **VInt parsing** works correctly ✅

## 📝 Example Session

```bash
$ ./cqlite-validator test
🧪 Running CQLite Compatibility Test Suite
[... all tests pass ...]
🎉 All compatibility tests PASSED!

$ ./cqlite-validator dir test-env/cassandra5/data
📂 Validating SSTable directory: test-env/cassandra5/data
[... validates all files ...]
✅ All SSTable files have valid structure!

$ ./cqlite-validator file some-data.db --verbose
🔍 Validating SSTable file: some-data.db
📁 File size: 1151 bytes
🔢 Magic number: 0xA0070000
[... detailed analysis ...]
✅ File validation complete - structure appears valid
```

## 🐛 Troubleshooting

If you encounter issues:

1. **Compilation errors**: Ensure Rust is installed (`rustc --version`)
2. **File not found**: Use absolute paths or check working directory
3. **Permission denied**: Check file permissions

## 📧 Reporting Results

Please share:
- Validator test results (screenshot or text)
- Any files that fail validation
- Cassandra version used to generate test files

This helps us ensure CQLite maintains 100% Cassandra compatibility!
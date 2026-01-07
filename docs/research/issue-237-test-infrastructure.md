# Issue #237: Index.db Partition Count Discrepancy Research

## Research Summary

This document provides comprehensive information about the test data and validation infrastructure for Issue #237, which tracks a discrepancy in partition counts between CQLite's Index.db parsing and sstabledump output.

---

## Part 1: Test Data Paths (sensor_data)

### SSTable Directory Structure

```
/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/
├── nb-1-big-CompressionInfo.db          (103 bytes)
├── nb-1-big-Data.db                     (86,688 bytes)
├── nb-1-big-Data.db.jsonl               (763,823 bytes) ← REFERENCE FILE
├── nb-1-big-Digest.crc32                (10 bytes)
├── nb-1-big-Filter.db                   (24 bytes)
├── nb-1-big-Index.db                    (217 bytes)  ← TARGET FILE FOR ISSUE #237
├── nb-1-big-Statistics.db               (5,202 bytes)
├── nb-1-big-Statistics.db.txt           (2,751 bytes)
├── nb-1-big-Summary.db                  (92 bytes)
└── nb-1-big-TOC.txt                     (92 bytes)
```

### Critical File Paths

| File | Path | Size | Purpose |
|------|------|------|---------|
| **Index.db** | `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Index.db` | 217 bytes | Partition index (test target) |
| **Data.db** | `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Data.db` | 86.6 KB | Partition data |
| **JSONL Reference** | `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl` | 764 KB | sstabledump output (canonical) |
| **Summary.db** | `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Summary.db` | 92 bytes | Summary data |

---

## Part 2: JSONL Reference File Structure

### Partition Count

```
Lines: 9 (9 partitions)
```

The JSONL file contains exactly **9 partitions**. Each line is a complete JSON object representing one partition.

### Example First Partition

```json
{
  "table kind": "REGULAR",
  "partition": {
    "key": ["0284a718-be7b-49e6-b6b9-8e82f5ff1660"],
    "position": 30
  },
  "rows": [
    {
      "type": "row",
      "position": 30,
      "clustering": ["2025-10-06 01:00:30.616Z"],
      "liveness_info": {"tstamp": "2025-10-06T01:12:07.971251Z"},
      "cells": [
        {"name": "battery_level", "value": 52},
        {"name": "humidity", "value": 92.88221},
        {"name": "location", "value": "New Dylan"},
        {"name": "pressure", "value": 1017.9518806690071},
        {"name": "status", "value": "inactive"},
        {"name": "temperature", "value": -16.172066}
      ]
    },
    ... (more rows in this partition)
  ]
}
```

### Table Schema

Based on the JSONL reference, the sensor_data table has:

**Partition Key**: UUID (single column)
```
0284a718-be7b-49e6-b6b9-8e82f5ff1660
```

**Clustering Key**: Timestamp
```
2025-10-06 01:00:30.616Z
```

**Regular Columns**:
- battery_level (int)
- humidity (float)
- location (text)
- pressure (float)
- status (text)
- temperature (float)

---

## Part 3: Test Infrastructure

### A. Index.db Parsing Regression Tests

**File Path**: `/Users/patrick/local_projects/cqlite/cqlite-core/tests/index_db_parsing_regression_tests.rs`

**Key Test Functions**:

1. **`test_no_heuristics_without_summary()`** (Line 28-57)
   - Tests that without Summary.db, offsets are 0 (no heuristics mandate per Issue #28)
   - Creates synthetic Index.db files
   - Validates partition_entries have offset=0

2. **`test_index_with_real_sstable_data()`** (Line 155-243)
   - Tests with real SSTable data using TestContext
   - Uses `multi_partition_table` from test_basic
   - Validates partition entries and lookup consistency
   - IMPORTANT: Demonstrates correct pattern for testing real SSTable Index.db

3. **`test_promoted_index_wide_partitions()`** (Line 369-496)
   - Tests wide partitions with promoted indexes
   - Uses test_timeseries tables
   - Tests multiple tables to find wide partitions
   - **Can be extended to test sensor_data**

4. **`test_index_lookup_performance()`** (Line 499-632)
   - Performance testing using `user_activity` table
   - Concurrent access patterns
   - Tests lookup timing performance

5. **`test_index_edge_cases()`** (Line 247-365)
   - Boundary condition testing
   - First/last entry validation
   - Non-existent key testing

### B. SSTable Parity Tests

**File Path**: `/Users/patrick/local_projects/cqlite/cqlite-core/tests/sstabledump_parity_index.rs`

**Key Test**: `test_index_db_parity_comprehensive()` (Line ~70)

Features:
- Uses canonical JSONL files as ground truth
- Validates partition count matches
- Tests against sensor_data (one of 4 target tables)
- Saves validation artifacts to `validation_artifacts/sstabledump/<keyspace>.<table>/`
- Fast-fail if datasets missing

**Target Tables**:
```rust
target_tables: vec![
    "simple_table",
    "sensor_data",          ← OUR FOCUS
    "wide_partition_table",
    "collection_table",
]
```

### C. Reference Data Parity Tests

**File Path**: `/Users/patrick/local_projects/cqlite/cqlite-core/tests/reference_data_parity.rs`

**Key Test**: `test_data_jsonl_vs_statistics_row_counts()` (Line 30)

Features:
- Compares JSONL row counts with Statistics.db
- Uses sensor_data as target table (Line 42)
- Currently `#[ignore]` because nb-format Statistics.db parsing deferred to M2

---

## Part 4: Test Utilities Infrastructure

### TestContext API

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/tests/common/sstable_test_utils.rs`

**Usage Pattern**:
```rust
let mut context = TestContext::new("test_timeseries").await.unwrap();
let sstable_path = context.prepare_sstable("sensor_data").await.unwrap();

// Test operations...

let metrics = context.cleanup().unwrap();
```

**Key Methods**:
- `TestContext::new(dataset_name)` - Initialize test context
- `context.prepare_sstable(table_name)` - Locate SSTable files
- `context.get_available_tables()` - List available tables in keyspace
- `context.cleanup()` - Clean up and return metrics

**Environment Variables**:
- `CQLITE_DATASETS_ROOT` - Override default dataset location
- Falls back to relative path from cargo manifest if not set

### Validation Artifacts Structure

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/validation_artifacts/sstabledump/`

**Sensor Data Artifacts**:
```
validation_artifacts/sstabledump/test_timeseries.sensor_data/
└── validation_result.json    (Test results for sensor_data)
```

**Structure** (from Index.db parity test):
```json
{
  "keyspace": "test_timeseries",
  "table": "sensor_data",
  "index_file_path": "...",
  "partition_count": 9,
  "promoted_index_count": 0,
  "key_digest_matches": [...],
  "offset_matches": [...],
  "perfect_parity": true/false,
  "timestamp": "...",
  "errors": [...]
}
```

---

## Part 5: Smoke Test Infrastructure

### Smoke Test Script

**File**: `/Users/patrick/local_projects/cqlite/test-data/scripts/smoke-test-all-tables.sh`

**Purpose**: Validate all 33 test tables can be loaded successfully

**Key Parameters**:
- Timeout: 30 seconds per table
- Tests keyspaces: test_basic, test_collections, test_timeseries, test_wide_rows
- Output format: JSON
- Validates row count and JSON output presence

**For sensor_data Specifically**:
```bash
# The script will test:
CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo run --bin cqlite -- read-sstable \
    /Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Data.db \
    --format json
```

**Results**: Stores output in `smoke-test-all-tables-results/test_timeseries_sensor_data.json`

---

## Part 6: How to Run Tests for Issue #237

### Test 1: Index.db Partition Count Test (Direct)

```bash
# Navigate to workspace root
cd /Users/patrick/local_projects/cqlite

# Set up environment
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets

# Run promoted index test (tests timeseries tables)
cargo test --package cqlite-core \
  test_promoted_index_wide_partitions -- --nocapture

# Or run all index regression tests
cargo test --package cqlite-core \
  index_db_parsing_regression_tests -- --nocapture
```

### Test 2: Sstabledump Parity Test (Most Relevant)

```bash
# Set environment
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets

# Run comprehensive parity test (tests sensor_data)
cargo test --package cqlite-core \
  test_index_db_parity_comprehensive -- --nocapture

# Examine results
cat /Users/patrick/local_projects/cqlite/cqlite-core/validation_artifacts/sstabledump/index_parity_report.md
```

### Test 3: Smoke Test for sensor_data

```bash
cd /Users/patrick/local_projects/cqlite

# Run full smoke test (all 33 tables)
bash test-data/scripts/smoke-test-all-tables.sh

# Check results
cat test-data/scripts/smoke-test-results/test_timeseries_sensor_data.json
```

### Test 4: Direct Index.db Reading

```rust
// In a Rust test
use cqlite_core::{
    platform::Platform,
    storage::sstable::index_reader::IndexReader,
    Config,
};
use std::sync::Arc;

let index_file = "/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Index.db";

let config = Config::default();
let platform = Arc::new(Platform::new(&config).await.unwrap());
let index_reader = IndexReader::open(index_file, platform).await.unwrap();

let entries = index_reader.get_partition_entries();
println!("Partition count: {}", entries.len());  // Should be 9

for (i, entry) in entries.iter().enumerate() {
    println!("Partition {}: key_digest_len={}, offset={}", 
        i, entry.key_digest.len(), entry.data_offset);
}
```

---

## Part 7: Expected Output

### Expected Partition Count: 9

From the JSONL reference file, the canonical partition count is **9 unique partitions**.

This comes from the sstabledump output which has exactly 9 lines (one per partition).

### Validation Checks

When running tests to verify the fix for Issue #237:

1. **Index.db parsing** should return exactly 9 partition entries
2. **Key digests** should be parseable and match sstabledump
3. **Partition offsets** should be calculable (0 without Summary.db, or real offsets with Summary.db)
4. **Promoted index entries** may be present for wide partitions (sensor_data has 0 promoted indexes)

---

## Part 8: Relevant Source Code Locations

### Index Reader Implementation

**File**: `cqlite-core/src/storage/sstable/index_reader.rs`

Key structures:
```rust
pub struct PartitionEntry {
    pub key_digest: Vec<u8>,
    pub data_offset: u64,
    pub data_size: u64,
    pub promoted_index: Option<PromotedIndex>,
}
```

### V5 Format Parser (NB Format)

**File**: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

Responsible for parsing Data.db format (where Index.db points).

### Block I/O Module

**File**: `cqlite-core/src/storage/sstable/reader/block_io.rs`

Handles reading NB format chunks (V5_0NewBigFormat).

---

## Part 9: Key Issue References

- **Issue #237**: Current issue - Index.db partition count discrepancy
- **Issue #28**: No-heuristics mandate (Index.db must not guess offsets without Summary.db)
- **Issue #92**: Index.db offset heuristics removed
- **Issue #213**: Clustering key row format parsing (FIXED)
- **Issue #212**: BTI index zero entries (FIXED)
- **Issue #218**: Summary.db header format (FIXED)
- **Issue #216**: SerializationHeader marker search (FIXED)
- **Issue #210**: Static column support (FIXED)

---

## Part 10: Current Validation Status

From `/docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`:

**Current Smoke Test Pass Rate**: 85% (28/33 tables)

**Passing Tests**:
- test_basic.simple_table ✓
- test_timeseries.sensor_data ✓ (Load test passes; partition count verification needed)
- test_wide_rows.wide_partition_table ✓
- test_collections.collection_table ✓

**Validation Artifacts Location**:
```
/Users/patrick/local_projects/cqlite/cqlite-core/validation_artifacts/sstabledump/test_timeseries.sensor_data/validation_result.json
```

---

## Summary Table: Test Methods vs. Issue #237

| Test Method | File | Partition Count Check | Index.db Direct Read | Uses JSONL Reference |
|-------------|------|----------------------|----------------------|----------------------|
| index_db_parsing_regression_tests | `index_db_parsing_regression_tests.rs:369-496` | Supports it | YES | NO |
| sstabledump_parity_index | `sstabledump_parity_index.rs:70+` | YES ✓ | YES | YES ✓ |
| reference_data_parity | `reference_data_parity.rs:30+` | YES ✓ | YES | YES ✓ |
| smoke-test-all-tables.sh | `smoke-test-all-tables.sh` | Partial | Via CLI | YES |

**Most Relevant**: `sstabledump_parity_index` - directly validates partition count against canonical JSONL.


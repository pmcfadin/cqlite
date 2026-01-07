# Issue #237: Quick Reference Guide

## Exact File Paths

### SSTable Components
```
Index.db:     /Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Index.db
Data.db:      /Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Data.db
Summary.db:   /Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Summary.db
```

### JSONL Reference (Ground Truth)
```
JSONL:        /Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl
Partitions:   9 (one per line)
```

## Test Files

### Most Relevant Test
```
File:         /Users/patrick/local_projects/cqlite/cqlite-core/tests/sstabledump_parity_index.rs
Test:         test_index_db_parity_comprehensive()
What:         Validates Index.db partition count against JSONL reference (9 partitions)
Expected:     perfect_parity = true, partition_count = 9
Artifacts:    /Users/patrick/local_projects/cqlite/cqlite-core/validation_artifacts/sstabledump/test_timeseries.sensor_data/validation_result.json
```

### Supporting Tests
```
File:         /Users/patrick/local_projects/cqlite/cqlite-core/tests/index_db_parsing_regression_tests.rs
Test:         test_promoted_index_wide_partitions() [Line 369]
What:         Tests Index.db on real timeseries tables
Tables:       Can be extended to specifically test sensor_data

File:         /Users/patrick/local_projects/cqlite/cqlite-core/tests/reference_data_parity.rs
Test:         test_data_jsonl_vs_statistics_row_counts() [Line 30]
What:         Compares JSONL row counts with Statistics.db
Note:         Currently ignored (nb-format parsing deferred to M2)
```

## Quick Commands

### Run Parity Test (MOST RELEVANT)
```bash
cd /Users/patrick/local_projects/cqlite
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets
cargo test --package cqlite-core test_index_db_parity_comprehensive -- --nocapture
```

### Run All Index Tests
```bash
export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets
cargo test --package cqlite-core index_db_parsing_regression_tests -- --nocapture
```

### Run Smoke Test
```bash
cd /Users/patrick/local_projects/cqlite
bash test-data/scripts/smoke-test-all-tables.sh
```

## Expected Results

### Partition Count
- **Expected**: 9 partitions
- **Source**: JSONL file has exactly 9 lines (one per partition)
- **Canonical Truth**: sstabledump output

### Key Digest Details
- **Count**: 9 (one per partition)
- **Format**: SHA-256 hash (32 bytes)
- **Example**: `0284a718-be7b-49e6-b6b9-8e82f5ff1660` (UUID format in JSONL)

### Data Offsets
- **Without Summary.db**: 0 (per Issue #28 no-heuristics mandate)
- **With Summary.db**: Real byte offsets in Data.db
- **sensor_data**: Has Summary.db, so offsets should be calculable

## Test Utilities

### TestContext
```rust
let mut context = TestContext::new("test_timeseries").await.unwrap();
let sstable_path = context.prepare_sstable("sensor_data").await.unwrap();
// ... test operations ...
let metrics = context.cleanup().unwrap();
```

### IndexReader
```rust
let index_reader = IndexReader::open(&index_file, platform).await.unwrap();
let entries = index_reader.get_partition_entries();  // Should return 9 entries
```

## Relevant Source Files

```
cqlite-core/src/storage/sstable/
├── index_reader.rs              (Main Index.db parser)
├── reader/
│   ├── parsing/
│   │   └── v5_compressed_legacy.rs  (Data.db format parser)
│   └── block_io.rs              (NB chunk reading)
└── summary_reader.rs            (Summary.db parser)
```

## Validation Artifacts

```
/Users/patrick/local_projects/cqlite/cqlite-core/validation_artifacts/sstabledump/
├── test_timeseries.sensor_data/
│   └── validation_result.json   ← Check this after running parity test
├── test_basic.simple_table/
├── test_wide_rows.wide_partition_table/
└── test_collections.collection_table/
```

## Issue Context

- **Issue #237**: Index.db partition count discrepancy
- **Status**: Needs investigation
- **Type**: Data validation / parsing correctness
- **Related Issues**: #28, #92, #213, #212, #218, #216, #210

## Environment Setup

```bash
# One-time setup
cd /Users/patrick/local_projects/cqlite
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets

# All tests now have access to datasets
cargo test --package cqlite-core -- --nocapture
```


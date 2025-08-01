# Test Data Generation - Simple & Reliable

## Overview
Simple 3-script system for generating real Cassandra 5 SSTable test data.

## The Workflow

```bash
./reset_test_db.sh    # Clean slate
./load_test_data.sh   # Schema + data  
./extract_sstables.sh # Files to tests/data/sstables/
```

## What You Get

**Location**: `tests/data/sstables/`

**8 Table Types**:
- `all_types/` - All primitive Cassandra types (5,000 rows)
- `collections_table/` - Lists, sets, maps (3,000 rows) 
- `users/` - UDTs with nested types (2,000 rows)
- `time_series/` - Time-based partitioning (10,000 rows)
- `multi_clustering/` - Complex clustering keys (2,000 rows)
- `large_table/` - Large dataset for performance (50,000 rows)
- `counters/` - Counter columns (50 updates)
- `static_test/` - Static columns (100 rows)

**4 Compression Types**: LZ4, Snappy, Zstd, Deflate

**All Data Types**: UUID, TEXT, BIGINT, BLOB, BOOLEAN, DATE, DECIMAL, DOUBLE, FLOAT, INET, INT, SMALLINT, TIME, TIMESTAMP, TIMEUUID, TINYINT, VARINT, LIST, SET, MAP, UDT, FROZEN

## Architecture

**Reuses Existing Infrastructure**:
- `/test-env/cassandra5/` - Docker setup & schema
- `manage.sh` - Complete database management
- `create-keyspaces-fixed.cql` - Comprehensive schema
- `data-generator/` - Python data generator

**No Duplication**: Uses what already works.

## Usage in Tests

```rust
use std::path::Path;

#[test]
fn test_with_real_sstables() {
    let sstable_dir = Path::new("tests/data/sstables/all_types-*/");
    // Test with real Cassandra 5 SSTables
}
```

## Cleanup

```bash
# Stop container when done
cd test-env/cassandra5
./manage.sh down
```

That's it. Simple, reliable, reuses existing infrastructure.
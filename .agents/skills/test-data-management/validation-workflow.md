# Validation Workflow

Guide to validating cqlite parsing against Cassandra's sstabledump reference implementation.

## Validation Strategy

```
SSTable Files
     │
     ├──────────────────────────┐
     │                          │
     ▼                          ▼
sstabledump                 cqlite
 (reference)               (our parser)
     │                          │
     ▼                          ▼
reference.json            cqlite.json
     │                          │
     └─────────┬────────────────┘
               ▼
           Compare
           (jq diff)
```

## Step 1: Generate Reference Output

### Using sstabledump

```bash
# Generate JSON reference for a table
sstabledump test-data/datasets/sstables/test_basic/simple_table/na-1-big-Data.db \
    > reference/simple_table.json
```

### sstabledump Options

**Basic output:**
```bash
sstabledump Data.db
# Human-readable JSON
```

**Include partition keys:**
```bash
sstabledump -d Data.db
# More detailed output
```

**Include timestamps:**
```bash
sstabledump -t Data.db
# Show cell timestamps
```

**Limit rows:**
```bash
sstabledump -l 10 Data.db
# Only first 10 rows
```

## Step 2: Generate cqlite Output

### Using cqlite CLI

```bash
cargo run --bin cqlite -- \
    --data-dir test-data/datasets/sstables/test_basic/simple_table \
    --schema test-data/schemas/basic-types.cql \
    --out json \
    > cqlite/simple_table.json
```

### Output Format

cqlite JSON output format:
```json
{
  "partition_key": "550e8400-e29b-41d4-a716-446655440000",
  "clustering_key": null,
  "cells": {
    "account_balance": 1234.56,
    "created": "2025-10-21T10:30:00Z",
    "is_active": true,
    "user_name": "alice"
  }
}
```

## Step 3: Normalize and Compare

### Normalize JSON

Different formatting doesn't mean different data:

```bash
# Sort keys, indent consistently
jq -S '.' reference/simple_table.json > reference/simple_table-sorted.json
jq -S '.' cqlite/simple_table.json > cqlite/simple_table-sorted.json
```

### Compare

```bash
diff reference/simple_table-sorted.json cqlite/simple_table-sorted.json
```

**Expected:** No output (files identical)

**If differences:**
```bash
# Detailed diff
diff -u reference/simple_table-sorted.json cqlite/simple_table-sorted.json

# Or use diffing tool
code --diff reference/simple_table-sorted.json cqlite/simple_table-sorted.json
```

## Automated Validation

### Validation Test Suite

```rust
// tests/sstable_validation.rs
#[test]
fn validate_simple_table() {
    let reference = load_sstabledump_output(
        "test-data/reference/simple_table.json"
    ).unwrap();
    
    let cqlite = parse_sstable(
        "test-data/datasets/sstables/test_basic/simple_table",
        "test-data/schemas/basic-types.cql"
    ).unwrap();
    
    assert_eq!(reference.len(), cqlite.len(), "Row count mismatch");
    
    for (ref_row, cql_row) in reference.iter().zip(cqlite.iter()) {
        assert_partition_keys_equal(&ref_row.pk, &cql_row.pk);
        assert_cells_equal(&ref_row.cells, &cql_row.cells);
    }
}
```

### Run Validation

```bash
# All tables
cargo test --test sstable_validation

# Specific table
cargo test --test sstable_validation -- simple_table

# With output
cargo test --test sstable_validation -- simple_table --nocapture
```

## Validation Criteria

### Must Match

1. **Row count**
   - Same number of partitions
   - Same number of clustering rows per partition

2. **Partition keys**
   - Exact byte match
   - Or semantic equivalence (e.g., UUIDs)

3. **Clustering keys**
   - Exact byte match
   - Correct sort order

4. **Cell values**
   - Exact match for fixed types (int, bigint, etc.)
   - Semantic equivalence for text (UTF-8)
   - Byte-for-byte for blob

5. **Timestamps**
   - Exact match (if included in output)

6. **TTL/Expiration**
   - Exact match (if present)

### May Differ (Acceptable)

1. **Formatting**
   - Whitespace
   - Key order in JSON
   - Number representation (1 vs 1.0)

2. **Representation**
   - UUID format (with/without hyphens)
   - Timestamp format (ISO 8601 vs Unix epoch)
   - Binary encoding (hex vs base64)

3. **Metadata**
   - File paths
   - Generation timestamps
   - System-specific info

## Handling Differences

### Type Representation

**Example:** UUID formatting
```json
// sstabledump
{"id": "550e8400-e29b-41d4-a716-446655440000"}

// cqlite (different format)
{"id": "550e8400e29b41d4a716446655440000"}
```

**Solution:** Normalize UUIDs before comparison
```rust
fn normalize_uuid(s: &str) -> String {
    s.replace("-", "").to_lowercase()
}
```

### Floating Point

**Example:** Precision differences
```json
// sstabledump
{"balance": 1234.56}

// cqlite
{"balance": 1234.5599999999999}
```

**Solution:** Compare with tolerance
```rust
fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < 0.0001
}
```

### Collection Order

**Example:** Set order
```json
// sstabledump (arbitrary order)
{"tags": ["python", "rust", "java"]}

// cqlite (sorted)
{"tags": ["java", "python", "rust"]}
```

**Solution:** Sort before comparison
```rust
fn normalize_set(set: &mut Vec<String>) {
    set.sort();
}
```

## Validation Levels

### Level 1: Structural
- Row count matches
- Schema matches
- No crashes/panics

### Level 2: Data
- All values match (with normalization)
- Correct types
- Null handling correct

### Level 3: Metadata
- Timestamps match
- TTLs match
- Deletion markers correct

### Level 4: Performance
- Parse speed acceptable (PRD targets)
- Memory usage acceptable (<128MB)

## CI Validation

### GitHub Actions Workflow

```yaml
name: SSTable Validation

on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Cassandra tools
        run: |
          sudo apt-get update
          sudo apt-get install -y cassandra-tools
      
      - name: Download test data
        run: |
          wget https://github.com/pmcfadin/cqlite/releases/download/test-data-v5.0/cqlite-test-data.tar.gz
          tar xzf cqlite-test-data.tar.gz
      
      - name: Generate reference
        run: |
          for table in test-data/datasets/sstables/*/*/; do
            sstabledump "${table}"*-Data.db > "reference/$(basename $table).json"
          done
      
      - name: Run validation
        run: cargo test --test sstable_validation
      
      - name: Upload diffs on failure
        if: failure()
        uses: actions/upload-artifact@v3
        with:
          name: validation-diffs
          path: validation-diffs/
```

## Debugging Validation Failures

### Step 1: Identify Difference

```bash
diff -u reference.json cqlite.json | head -20
```

### Step 2: Extract Failing Row

```bash
# Get line number from diff
jq '.[42]' reference.json > failing-row-ref.json
jq '.[42]' cqlite.json > failing-row-cqlite.json
```

### Step 3: Hex Dump SSTable

```bash
# Find row offset in SSTable
# (Use index or calculate from previous rows)
hexdump -C Data.db -s <offset> -n 256
```

### Step 4: Compare Format

- Check flag bytes
- Verify VInt encoding
- Validate delta encoding
- Check clustering prefix

### Step 5: Fix Parser

Update parser based on format understanding:
```rust
// Fix in v5_compressed_legacy.rs
let correct_offset = old_offset + missing_bytes;
```

### Step 6: Re-validate

```bash
cargo test --test sstable_validation -- failing_test
```

## Known Validation Challenges

### 1. Counter Columns
Cassandra internals differ - validate final value, not shards.

### 2. Collections with Tombstones
Non-frozen collections have cell tombstones - may appear as missing elements.

### 3. Static Columns
Partition-level data - validate once per partition, not per row.

### 4. Wide Rows
Large partitions may hit memory limits - validate in chunks.

## Validation Metrics

Track validation coverage:

```rust
struct ValidationMetrics {
    tables_validated: usize,
    rows_validated: usize,
    cells_validated: usize,
    types_covered: HashSet<CqlType>,
    edge_cases: Vec<EdgeCase>,
}
```

**Target:** 100% type coverage, all edge cases validated.

## Next Steps

After validation passes:
1. Document any acceptable differences
2. Add regression test for the table
3. Update validation suite for new types
4. Package validated dataset for CI
5. Update PRD milestone progress

## References

- sstabledump source: https://github.com/apache/cassandra/tree/trunk/src/java/org/apache/cassandra/tools
- Cassandra SSTable format: See `CASSANDRA_50_FORMAT_SPECIFICATION.md`
- cqlite validation tests: `tests/sstable_validation.rs`


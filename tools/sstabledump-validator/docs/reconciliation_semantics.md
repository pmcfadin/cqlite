# Read-Time Reconciliation Semantics - Issue #37

This document describes the comprehensive read-time reconciliation logic implemented for CQLite Issue #37, which ensures perfect compatibility with Cassandra's deletion and TTL semantics.

## Overview

Read-time reconciliation is the process of determining which data is visible to a query by applying tombstones, TTL expiration, and conflict resolution according to Cassandra's exact semantics. This is critical for ensuring CQLite produces identical results to Cassandra in all scenarios.

## Core Reconciliation Rules

### 1. Timestamp-Based Conflict Resolution

When multiple values exist for the same cell with different timestamps:
- **Newest wins**: The value with the highest timestamp is considered authoritative
- **Microsecond precision**: Timestamps are compared at microsecond granularity
- **Tie-breaking**: In case of identical timestamps, lexicographic ordering of values applies

### 2. TTL Expiration Logic

Time-To-Live (TTL) expiration follows strict Cassandra semantics:

```
expiry_time = write_timestamp + ttl_seconds * 1,000,000  // Convert to microseconds
if current_time > expiry_time:
    cell_is_expired = true
```

**Key behaviors:**
- Expired cells are treated as if they never existed
- TTL expiration is checked before tombstone application
- Expired cells can be "resurrected" by newer writes

### 3. Tombstone Precedence Hierarchy

Tombstones are applied in strict precedence order:

1. **Range Tombstones** (highest precedence)
   - Delete entire ranges of clustering keys
   - Support inclusive/exclusive bounds
   - Applied before row and cell tombstones

2. **Row Tombstones** (medium precedence)
   - Delete entire rows (all cells in the row)
   - Only affect cells written before the tombstone timestamp
   - Newer cell writes can "resurrect" deleted rows

3. **Cell Tombstones** (lowest precedence)
   - Delete individual cells
   - Only affect the specific column
   - Can be overridden by newer writes to the same cell

### 4. Multi-Generation Conflict Resolution

When multiple generations (writes) exist for the same cell:

```rust
// Pseudocode for multi-generation resolution
fn resolve_cell_conflict(generations: Vec<CellWrite>) -> Option<Value> {
    // 1. Sort by timestamp (newest first)
    generations.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    
    // 2. Apply tombstone and TTL logic
    for generation in generations {
        if generation.is_expired(current_time) {
            continue; // Skip expired values
        }
        
        if generation.is_tombstone() {
            return None; // Cell is deleted
        }
        
        return Some(generation.value); // First valid value wins
    }
    
    None // No valid values found
}
```

## Range Tombstone Semantics

Range tombstones require careful boundary handling:

### Inclusive vs Exclusive Bounds

```
Range [start, end] with inclusive_start=true, inclusive_end=false:
- Clustering key "start" is INCLUDED (deleted)
- Clustering key "end" is EXCLUDED (survives)
- All keys between start and end are DELETED
```

### Range Application Algorithm

```rust
fn applies_to_key(range: &RangeTombstone, key: &ClusteringKey) -> bool {
    let start_match = match &range.start_bound {
        Some(start) => {
            if range.inclusive_start {
                key >= start
            } else {
                key > start
            }
        }
        None => true, // Unbounded start
    };
    
    let end_match = match &range.end_bound {
        Some(end) => {
            if range.inclusive_end {
                key <= end
            } else {
                key < end
            }
        }
        None => true, // Unbounded end
    };
    
    start_match && end_match
}
```

## Interaction Scenarios

### TTL vs Tombstone Interactions

1. **TTL expires before tombstone**: Cell is invisible due to TTL
2. **Tombstone before TTL expiration**: Cell is invisible due to tombstone
3. **Both active**: Tombstone takes precedence (data is deleted)

### Row vs Cell Tombstone Interactions

1. **Row tombstone older than cell write**: Cell survives (resurrection)
2. **Row tombstone newer than cell write**: Cell is deleted
3. **Cell tombstone + row tombstone**: Cell is deleted (both apply)

### Multi-Generation Resurrection

```
Timeline:
t=1000: INSERT value='original'
t=2000: DELETE (row tombstone)
t=3000: INSERT value='resurrected'

Result: value='resurrected' is visible (newest write wins)
```

## Validation Strategy

### Dual Validation Approach

1. **SSTableDump Validation**
   - Parse Cassandra's sstabledump output
   - Apply reconciliation logic
   - Compare with CQLite's reconciliation results

2. **Live CQL Validation** (optional)
   - Execute actual CQL queries against Cassandra
   - Compare query results with CQLite reconciliation
   - Ensures end-to-end compatibility

### Test Dataset Coverage

The validation includes comprehensive test datasets covering:

1. **Overlapping Writes**: Multiple writes to same cell with different timestamps
2. **TTL Expiration**: Various TTL states (expired, active, none)
3. **Row vs Cell Tombstones**: All combinations of row and cell deletions
4. **Range Tombstones**: Inclusive/exclusive bounds with various key ranges
5. **Complex Mixed**: Combinations of all above scenarios
6. **Multi-Generation Conflicts**: Resurrection and deletion cycles

### Zero-Tolerance Requirements

For Issue #37, the validation enforces **zero tolerance** for discrepancies:
- Any difference in cell visibility between Cassandra and CQLite fails validation
- Any difference in final cell values fails validation
- Any difference in metadata (timestamps, TTL) fails validation

## Implementation Architecture

### ReconciliationEngine

The core reconciliation logic is implemented in `ReconciliationEngine`:

```rust
pub struct ReconciliationEngine {
    current_time: i64,
    config: ReconciliationConfig,
}

impl ReconciliationEngine {
    pub async fn reconcile_datasets(
        &self,
        cassandra_data: &ParsedData,
        cqlite_data: &ParsedData,
    ) -> Result<DatasetReconciliationResult>
}
```

### Configuration Options

```rust
pub struct ReconciliationConfig {
    /// Apply strict Cassandra semantics (required for validation)
    pub strict_cassandra_semantics: bool,
    /// TTL grace period for testing scenarios
    pub ttl_grace_period: i64,
    /// Enable range tombstone processing
    pub enable_range_tombstones: bool,
    /// GC grace seconds for tombstone expiration
    pub gc_grace_seconds: i32,
}
```

## CI Integration

### Regression Tests

The reconciliation validation is integrated into CI with comprehensive regression tests:

```bash
# Run reconciliation validation
cargo test --package sstabledump-validator reconciliation_tests

# Run live validation (requires Cassandra)
./target/debug/sstabledump-validator reconciliation --live-validation --strict-mode
```

### CI Gating

- All reconciliation tests must pass for merge approval
- Any reconciliation differences cause CI failure
- Live validation provides additional confidence (when available)

## Error Handling and Diagnostics

### Detailed Difference Reporting

When reconciliation differences are found, the system provides detailed diagnostics:

```
❌ complex_mixed: FAILED - 3 reconciliation differences
   Difference in partition_123.expired_ttl_cell: CassandraVisibleCqliteHidden
   Difference in partition_123.tombstoned_cell: BothHiddenDifferentReasons
   Difference in partition_123.multi_gen_cell: BothVisibleDifferentValues
```

### Debugging Information

Each reconciled cell includes comprehensive metadata:

```rust
pub struct ReconciledCell {
    pub value: Option<ParsedCell>,
    pub reconciliation_reason: ReconciliationReason,
    pub effective_timestamp: i64,
    pub affected_by_tombstone: bool,
    pub affected_by_ttl: bool,
    pub candidates: Vec<CandidateValue>, // All considered values
}
```

## Performance Considerations

### Optimization Strategies

1. **Lazy Evaluation**: Only process cells that need reconciliation
2. **Batch Processing**: Handle multiple cells efficiently
3. **Early Termination**: Stop processing when outcome is determined
4. **Memory Efficient**: Stream large datasets without full materialization

### Benchmark Targets

- **Small datasets** (< 1000 cells): < 10ms reconciliation time
- **Medium datasets** (< 100K cells): < 1s reconciliation time  
- **Large datasets** (> 1M cells): Linear scaling with parallelization

## Future Extensions

### Planned Enhancements

1. **Parallel Reconciliation**: Process partitions in parallel
2. **Incremental Validation**: Only validate changed data
3. **Schema-Aware Validation**: Use table schema for enhanced validation
4. **Custom Comparators**: Support for custom clustering key ordering

### Extensibility Points

The reconciliation engine is designed for extensibility:
- Custom reconciliation strategies
- Pluggable tombstone handlers
- Configurable conflict resolution rules
- Custom validation metrics

---

This reconciliation implementation ensures CQLite maintains perfect compatibility with Cassandra's complex deletion and TTL semantics, providing confidence that query results will be identical across both systems.
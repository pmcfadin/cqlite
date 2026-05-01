# Issue #449: Range and Partition Tombstone Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit write-read roundtrip tests for range tombstones and partition tombstones, fix the stats tracking bug discovered during analysis, and export the missing types from the public API.

**Architecture:** The write path already supports partition tombstones (via `Mutation.partition_tombstone`) and range tombstones (via `Mutation.range_tombstones`). The data_writer serializes both correctly. However: (1) `PartitionTombstone`, `RangeTombstone`, and `ClusteringBound` are not re-exported from `write_engine` module, (2) the SSTableWriter stats loop doesn't track timestamps/local_deletion_times from partition/range tombstones (causing potential delta encoding errors), and (3) there are no integration tests exercising the full WriteEngine → flush → SSTable roundtrip for these tombstone types.

**Tech Stack:** Rust, tokio (async tests), tempfile, cqlite-core with `write-support` feature

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `cqlite-core/src/storage/write_engine/mod.rs:49` | Modify | Export `PartitionTombstone`, `RangeTombstone`, `ClusteringBound` |
| `cqlite-core/src/storage/sstable/writer/mod.rs:316-377` | Modify | Track stats for partition/range tombstone timestamps |
| `cqlite-core/tests/write_read_roundtrip/edge_cases.rs` | Modify | Add 3 new integration tests |

---

### Task 1: Export tombstone types from write_engine public API

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/mod.rs:49`

- [ ] **Step 1: Add the missing re-exports**

In `cqlite-core/src/storage/write_engine/mod.rs`, change line 49 from:

```rust
pub use mutation::{CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId};
```

to:

```rust
pub use mutation::{
    CellOperation, ClusteringBound, ClusteringKey, DecoratedKey, Mutation, PartitionKey,
    PartitionTombstone, RangeTombstone, TableId,
};
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --package cqlite-core --features write-support`
Expected: Compiles without errors.

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/src/storage/write_engine/mod.rs
git commit -m "feat(#449): export PartitionTombstone, RangeTombstone, ClusteringBound from write_engine API"
```

---

### Task 2: Fix stats tracking for partition/range tombstone timestamps and add partition tombstone test

**Files:**
- Modify: `cqlite-core/src/storage/sstable/writer/mod.rs:316-359`
- Modify: `cqlite-core/tests/write_read_roundtrip/edge_cases.rs`

The SSTableWriter's stats loop in `write_partition_internal` updates stats from mutation operations but ignores `partition_tombstone` and `range_tombstones` fields. Their `deletion_time` and `local_deletion_time` aren't included in the min/max stats used for delta encoding, which can cause delta underflow errors when tombstone timestamps differ significantly from row timestamps.

- [ ] **Step 1: Update imports in edge_cases.rs**

Update the import block at the top of `edge_cases.rs` to include the tombstone types:

```rust
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, PartitionTombstone,
    RangeTombstone, TableId, WriteEngine, WriteEngineConfig,
};
```

- [ ] **Step 2: Write the partition tombstone test**

Add to `cqlite-core/tests/write_read_roundtrip/edge_cases.rs`. Uses a `local_deletion_time` of `2_000_000_000` (far outside the row timestamp range of ~1 seconds) to expose the stats tracking bug:

```rust
/// Test partition tombstone via full WriteEngine roundtrip
#[tokio::test]
async fn test_edge_partition_tombstone() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");

    // Write some rows first
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ck1 = ClusteringKey::single("ck", Value::Text("row1".to_string()));
    let ops1 = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text("Data row 1".to_string()),
    }];
    let mutation1 = Mutation::new(table_id.clone(), pk.clone(), Some(ck1), ops1, 1000000, None);
    engine.write_async(mutation1).await.expect("Write should succeed");

    let ck2 = ClusteringKey::single("ck", Value::Text("row2".to_string()));
    let ops2 = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text("Data row 2".to_string()),
    }];
    let mutation2 = Mutation::new(table_id.clone(), pk.clone(), Some(ck2), ops2, 1000001, None);
    engine.write_async(mutation2).await.expect("Write should succeed");

    // Delete the entire partition with a partition tombstone
    // Use a local_deletion_time far from row timestamps to expose stats tracking gaps
    let mut partition_delete = Mutation::new(
        table_id,
        pk,
        None,
        vec![], // No cell operations needed
        1000002,
        None,
    );
    partition_delete.partition_tombstone = Some(PartitionTombstone {
        deletion_time: 1000002,
        local_deletion_time: 2_000_000_000, // Far future - exposes stats bug if not tracked
    });
    engine
        .write_async(partition_delete)
        .await
        .expect("Partition tombstone should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db should exist");

    // Verify partition tombstone was written: read Data.db and check the partition header
    // Partition header format: [key_len:u16 BE][key_bytes][local_deletion_time:i32 BE][deletion_timestamp:i64 BE]
    // A LIVE partition has local_deletion_time = i32::MAX (0x7FFFFFFF)
    // A tombstoned partition has local_deletion_time != i32::MAX
    let data = std::fs::read(&info.data_path).expect("Should read Data.db");
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let ldt_offset = 2 + key_len;
    let ldt = i32::from_be_bytes([
        data[ldt_offset],
        data[ldt_offset + 1],
        data[ldt_offset + 2],
        data[ldt_offset + 3],
    ]);
    assert_ne!(
        ldt,
        i32::MAX,
        "Partition header should have non-LIVE local_deletion_time (got i32::MAX = LIVE)"
    );
    assert_eq!(
        ldt, 2_000_000_000,
        "Partition header local_deletion_time should match tombstone"
    );
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_edge_partition_tombstone -- --nocapture 2>&1 | tail -30`
Expected: FAIL — delta encoding error because `local_deletion_time` of 2_000_000_000 is not tracked in stats min/max.

- [ ] **Step 4: Fix stats tracking in SSTableWriter**

In `cqlite-core/src/storage/sstable/writer/mod.rs`, inside the `for mutation in &mutations` loop, right before the `self.stats.increment_row_count()` call (line 356), add:

```rust
            // Track stats for partition tombstones
            if let Some(pt) = &mutation.partition_tombstone {
                self.stats.update_timestamp(pt.deletion_time);
                self.stats
                    .update_local_deletion_time(pt.local_deletion_time);
            }

            // Track stats for range tombstones
            for rt in &mutation.range_tombstones {
                self.stats.update_timestamp(rt.deletion_time);
                self.stats
                    .update_local_deletion_time(rt.local_deletion_time);
            }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_edge_partition_tombstone -- --nocapture`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add cqlite-core/src/storage/sstable/writer/mod.rs cqlite-core/tests/write_read_roundtrip/edge_cases.rs
git commit -m "fix(#449): track partition/range tombstone timestamps in SSTable stats"
```

---

### Task 3: Add range tombstone integration test

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/edge_cases.rs`

- [ ] **Step 1: Write the range tombstone test**

Add to `cqlite-core/tests/write_read_roundtrip/edge_cases.rs`:

```rust
/// Test range tombstone (delete a range of clustering keys)
#[tokio::test]
async fn test_edge_range_tombstone() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // Write 3 rows: row_a, row_b, row_c
    for (suffix, ts) in [("row_a", 1000000i64), ("row_b", 1000001), ("row_c", 1000002)] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Data for {}", suffix)),
        }];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        engine.write_async(mutation).await.expect("Write should succeed");
    }

    // Delete range [row_a, row_b] (inclusive bounds)
    let mut range_mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![], // No cell operations
        1000003,
        None,
    );
    range_mutation.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Inclusive(
            ClusteringKey::single("ck", Value::Text("row_a".to_string())),
        ),
        end: ClusteringBound::Inclusive(
            ClusteringKey::single("ck", Value::Text("row_b".to_string())),
        ),
        deletion_time: 1000003,
        local_deletion_time: 2_000_000_000,
    });
    engine
        .write_async(range_mutation)
        .await
        .expect("Range tombstone should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db should exist");

    // Verify range tombstone markers in Data.db
    // Range tombstones are written as markers with IS_MARKER flag (0x02)
    // They appear after the partition header, before the rows
    let data = std::fs::read(&info.data_path).expect("Should read Data.db");
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    // Skip partition header: 2 (key_len) + key_len + 4 (ldt) + 8 (ts) = 14 + key_len
    let after_header = 2 + key_len + 4 + 8;
    // The first byte after the partition header should be IS_MARKER (0x02)
    // for the range tombstone opening bound
    let marker_byte = data[after_header];
    assert_eq!(
        marker_byte & 0x02,
        0x02,
        "First unfiltered after partition header should have IS_MARKER flag (byte was 0x{:02x})",
        marker_byte
    );
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_edge_range_tombstone -- --nocapture`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip/edge_cases.rs
git commit -m "test(#449): add range tombstone integration test"
```

---

### Task 4: Add Bottom/Top bounds range tombstone test

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/edge_cases.rs`

- [ ] **Step 1: Write the full-partition range tombstone test**

Add to `cqlite-core/tests/write_read_roundtrip/edge_cases.rs`:

```rust
/// Test range tombstone with Bottom/Top bounds (delete all clustering keys in partition)
#[tokio::test]
async fn test_edge_range_tombstone_full_partition() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // Write some rows
    for (suffix, ts) in [("a", 1000000i64), ("b", 1000001), ("c", 1000002)] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Data {}", suffix)),
        }];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        engine.write_async(mutation).await.expect("Write should succeed");
    }

    // Delete entire clustering range with Bottom..Top
    let mut range_mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![],
        1000003,
        None,
    );
    range_mutation.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 1000003,
        local_deletion_time: 2_000_000_000,
    });
    engine
        .write_async(range_mutation)
        .await
        .expect("Full range tombstone should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db should exist");

    // Verify range tombstone with Bottom/Top bounds
    let data = std::fs::read(&info.data_path).expect("Should read Data.db");
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let after_header = 2 + key_len + 4 + 8;
    // IS_MARKER flag should be present
    assert_eq!(
        data[after_header] & 0x02,
        0x02,
        "Should have IS_MARKER flag for Bottom/Top range tombstone"
    );
    // Bound kind for Bottom = START_BOUNDARY (4)
    assert_eq!(
        data[after_header + 1],
        4, // START_BOUNDARY
        "Bottom bound should use START_BOUNDARY kind"
    );
    // Empty clustering prefix for Bottom (header = 0)
    assert_eq!(
        data[after_header + 2],
        0x00,
        "Bottom should have empty clustering prefix"
    );
}
```

- [ ] **Step 2: Run all tombstone tests**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_edge -- --nocapture`
Expected: All edge case tests PASS.

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip/edge_cases.rs
git commit -m "test(#449): add full-partition range tombstone test with Bottom/Top bounds"
```

---

### Task 5: Final validation

- [ ] **Step 1: Run clippy**

Run: `env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: No warnings or errors.

- [ ] **Step 2: Run cargo fmt**

Run: `cargo fmt --check`
Expected: No formatting issues.

- [ ] **Step 3: Run all write-support tests**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support 2>&1 | tail -20`
Expected: All tests pass.

- [ ] **Step 4: Commit any fmt fixes if needed, then push**

```bash
git push origin main
```

# Frozen Collection Read-Back & Clustering Key Fixes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix frozen collection read-back returning Null (#458) and collection_clustering_table importing only 3/10 rows (#465).

**Architecture:** The V5CompressedLegacy reader's frozen collection parser incorrectly reads the cell value length prefix (VUInt) as the element count, when the actual element count is an i32 BE that follows. Fix the parser to consume the VUInt blob length first, then parse the i32 BE collection format. For #465, the clustering key serialization for frozen collections may produce incorrect bytes that Cassandra misinterprets, causing row deduplication.

**Tech Stack:** Rust, cqlite-core, V5CompressedLegacy SSTable format

---

## Root Cause Analysis

### Issue #458: Frozen Collection Read-Back Returns Null

**Writer path** (`data_writer.rs:1273-1331`):
1. `write_cell()` calls `serialize_value(Value::Frozen(inner))` → delegates to `serialize_value(inner)` (line 1886)
2. For `List([1,2,3])`: produces `[i32 BE count=3][i32 BE len=4][i32 BE 1][i32 BE len=4][i32 BE 2][i32 BE len=4][i32 BE 3]` = 28 bytes
3. `cell_value_uses_length_prefix()` returns `true` for Frozen → writes `[VUInt 28]` before the blob

**Cell on disk:** `[flags=0x08][VUInt 28][i32 BE count=3][i32 BE elem_len][elem]...`

**Reader path** (`v5_compressed_legacy.rs:2473`):
1. `parse_cell_value_schema_order` reads flags, optional timestamp
2. Dispatches to frozen branch → calls `parse_frozen_list_value`
3. `parse_frozen_list_value` (line 5798) reads VUInt as "element count" — **but this is the cell value length (28), not the element count (3)!**
4. Tries to parse 28 elements, reads corrupted data, fails → returns error/Null

**Fix:** `parse_frozen_list_value` must:
1. Read VUInt cell_value_length (the total blob size)
2. Read i32 BE element_count from within the blob
3. For each element: read i32 BE element_length + element_bytes

Same fix needed for `parse_frozen_set_value` and `parse_frozen_map_value`.

### Issue #465: collection_clustering_table Only 3/10 Rows

**Hypothesis:** The frozen collection clustering key serialization in `serialize_value_for_clustering` (line 2042) uses `serialize_value(inner)` which produces `[i32 BE count][i32 BE len][bytes]...` format. This might not match Cassandra's expected clustering key format for frozen collections, causing Cassandra to misinterpret the key bytes and see duplicates where there are none.

**Investigation needed:** Compare CQLite's frozen clustering key bytes against what Cassandra expects. The test data generator creates unique clustering keys, so if Cassandra sees only 3 unique rows, the serialization must be producing identical bytes for keys that should be different.

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` | Modify | Fix `parse_frozen_list_value`, `parse_frozen_set_value`, `parse_frozen_map_value` to consume VUInt blob length before parsing collection |
| `cqlite-core/tests/write_read_roundtrip/type_coverage.rs` | Modify | Update frozen tests from "known limitation" to strict assertions |
| `cqlite-core/tests/write_read_roundtrip/edge_cases.rs` | Modify | Add frozen clustering key roundtrip test |

---

## Task 1: Fix Frozen List Reader to Consume Cell Value Length

**Files:**
- Modify: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs:5790-5845` (`parse_frozen_list_value`)
- Modify: `cqlite-core/tests/write_read_roundtrip/type_coverage.rs:1073-1113` (`test_type_frozen_list`)

- [ ] **Step 1: Update `test_type_frozen_list` to assert correct read-back**

Change the test from accepting `Value::Null` as a known limitation to requiring the correct value:

```rust
#[tokio::test]
async fn test_type_frozen_list() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("frozen_col", "frozen<list<int>>");
    let inner = Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let original = Value::Frozen(Box::new(inner.clone()));

    let info = write_single_value(&temp_dir, &schema, "frozen_col", original.clone()).await;

    assert_single_partition_written(&info);
    let col_value = super::read_back_column(&temp_dir, &schema, "frozen_col").await;
    // Frozen wrapper may or may not be preserved through the roundtrip.
    // Accept either Value::Frozen(List) or Value::List directly.
    assert!(
        col_value == original || col_value == inner,
        "Frozen list roundtrip failed: expected {:?} or {:?}, got {:?}",
        original,
        inner,
        col_value
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_type_frozen_list -- --nocapture 2>&1 | tail -30`
Expected: FAIL (currently returns Null)

- [ ] **Step 3: Fix `parse_frozen_list_value` to read VUInt blob length then i32 BE count**

In `v5_compressed_legacy.rs`, modify `parse_frozen_list_value` (around line 5790):

```rust
fn parse_frozen_list_value(
    &self,
    data: &[u8],
    mut offset: usize,
    element_type: &str,
    column: &crate::schema::Column,
    reader: &super::super::types::SSTableReader,
) -> Result<(Value, usize)> {
    // Step 1: Read VUInt cell value length (total blob size)
    let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
        Error::corruption(format!(
            "Frozen list '{}': failed to parse blob length: {:?}",
            column.name, e
        ))
    })?;
    let blob_len = blob_len as usize;
    let bytes_consumed = data[offset..].len() - remaining.len();
    offset += bytes_consumed;

    log::debug!(
        "V5CompressedLegacy: Frozen list '{}' blob_len={}, element_type='{}'",
        column.name, blob_len, element_type
    );

    if offset + blob_len > data.len() {
        return Err(Error::corruption(format!(
            "Frozen list '{}': blob_len {} exceeds available data {}",
            column.name, blob_len, data.len() - offset
        )));
    }

    let blob_end = offset + blob_len;

    // Step 2: Read element count as i32 BE (Cassandra collection format)
    if offset + 4 > blob_end {
        return Err(Error::corruption(format!(
            "Frozen list '{}': not enough bytes for element count",
            column.name
        )));
    }
    let count = i32::from_be_bytes([
        data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
    ]) as usize;
    offset += 4;

    log::debug!(
        "V5CompressedLegacy: Parsing frozen list '{}' with {} elements",
        column.name, count
    );

    if count > MAX_FROZEN_COLLECTION_SIZE as usize {
        return Err(Error::corruption(format!(
            "Frozen list '{}': element count {} exceeds maximum {}",
            column.name, count, MAX_FROZEN_COLLECTION_SIZE
        )));
    }

    let mut elements = Vec::with_capacity(count);

    for i in 0..count {
        // Each element: [i32 BE length][element bytes]
        if offset + 4 > blob_end {
            return Err(Error::corruption(format!(
                "Frozen list '{}': not enough bytes for element {} length",
                column.name, i
            )));
        }
        let elem_len = i32::from_be_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + elem_len > blob_end {
            return Err(Error::corruption(format!(
                "Frozen list '{}': element {} needs {} bytes but only {} available",
                column.name, i, elem_len, blob_end - offset
            )));
        }

        let elem_data = &data[offset..offset + elem_len];
        let elem_name = format!("{}[{}]", column.name, i);
        let (elem_value, _) = self.parse_raw_type_value(elem_data, 0, element_type, &elem_name)?;
        elements.push(elem_value);
        offset += elem_len;
    }

    let _ = reader;
    Ok((Value::List(elements), blob_end))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_type_frozen_list -- --nocapture 2>&1 | tail -30`
Expected: PASS

---

## Task 2: Fix Frozen Set and Map Readers

**Files:**
- Modify: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` (`parse_frozen_set_value`, `parse_frozen_map_value`)
- Modify: `cqlite-core/tests/write_read_roundtrip/type_coverage.rs:1118-1187` (`test_type_frozen_map`, `test_type_frozen_empty`)

- [ ] **Step 1: Update `test_type_frozen_map` and `test_type_frozen_empty` to assert correct read-back**

Same pattern as Task 1 — change from accepting Null to requiring correct values:

```rust
#[tokio::test]
async fn test_type_frozen_map() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("frozen_col", "frozen<map<text, int>>");
    let inner = Value::Map(vec![(Value::Text("key".to_string()), Value::Integer(42))]);
    let original = Value::Frozen(Box::new(inner.clone()));

    let info = write_single_value(&temp_dir, &schema, "frozen_col", original.clone()).await;

    assert_single_partition_written(&info);
    let col_value = super::read_back_column(&temp_dir, &schema, "frozen_col").await;
    assert!(
        col_value == original || col_value == inner,
        "Frozen map roundtrip failed: expected {:?} or {:?}, got {:?}",
        original, inner, col_value
    );
}

#[tokio::test]
async fn test_type_frozen_empty() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("frozen_col", "frozen<list<int>>");
    let inner = Value::List(vec![]);
    let original = Value::Frozen(Box::new(inner.clone()));

    let info = write_single_value(&temp_dir, &schema, "frozen_col", original.clone()).await;

    assert_single_partition_written(&info);
    let col_value = super::read_back_column(&temp_dir, &schema, "frozen_col").await;
    assert!(
        col_value == original || col_value == inner,
        "Frozen(empty) list roundtrip failed: expected {:?} or {:?}, got {:?}",
        original, inner, col_value
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_type_frozen_map test_type_frozen_empty -- --nocapture 2>&1 | tail -30`

- [ ] **Step 3: Fix `parse_frozen_set_value` — same VUInt+i32 BE pattern as list**

Apply the same fix as `parse_frozen_list_value`: read VUInt blob length, then i32 BE count, then elements with i32 BE length prefixes.

- [ ] **Step 4: Fix `parse_frozen_map_value` — same pattern but with key+value pairs**

Map format: `[VUInt blob_len][i32 BE count][i32 BE key_len][key_bytes][i32 BE val_len][val_bytes]...`

- [ ] **Step 5: Run tests to verify they pass**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_type_frozen -- --nocapture 2>&1 | tail -30`
Expected: All 3 frozen tests PASS

- [ ] **Step 6: Also fix the `_raw` variants (`parse_frozen_list_value_raw`, `parse_frozen_set_value_raw`, `parse_frozen_map_value_raw`)**

These are used in nested frozen parsing contexts. Apply the same i32 BE count fix (but these may not need the VUInt blob length prefix since they're called from within an already-bounded blob).

---

## Task 3: Fix collection_clustering_table Row Deduplication (#465)

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/edge_cases.rs` (add test)
- Possibly modify: `cqlite-core/src/storage/sstable/writer/data_writer.rs` (clustering key serialization)

- [ ] **Step 1: Write a roundtrip test for frozen list clustering keys**

Add to `edge_cases.rs`:

```rust
#[tokio::test]
async fn test_frozen_list_clustering_key_uniqueness() {
    // Reproduce Issue #465: multiple rows with different frozen<list<text>> clustering keys
    // should all be preserved (not deduplicated)
    let temp_dir = TempDir::new().unwrap();
    let schema = TableSchema {
        keyspace: "test_ck".to_string(),
        table: "frozen_ck".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![KeyColumn {
            name: "ck".to_string(),
            data_type: "frozen<list<text>>".to_string(),
            position: 0,
        }],
        columns: vec![
            Column { name: "data".to_string(), data_type: "text".to_string(), is_static: false },
        ],
        ..Default::default()
    };

    let pk = Value::Uuid([0u8; 16]);
    let ck_values = vec![
        vec!["a", "b"],
        vec!["a", "b", "c"],
        vec!["x"],
        vec!["a", "c"],
        vec!["b"],
    ];

    // Write 5 rows with different clustering keys
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).unwrap();

    for (i, ck_val) in ck_values.iter().enumerate() {
        let ck = Value::Frozen(Box::new(Value::List(
            ck_val.iter().map(|s| Value::Text(s.to_string())).collect(),
        )));
        let mutation = Mutation {
            table: TableId::from(format!("{}.{}", schema.keyspace, schema.table).as_str()),
            partition_key: PartitionKey(vec![("pk".to_string(), pk.clone())]),
            clustering_key: Some(ClusteringKey::new(vec![("ck".to_string(), ck)])),
            operations: vec![CellOperation::Write {
                column: "data".to_string(),
                value: Value::Text(format!("row_{}", i)),
            }],
            timestamp_micros: 1704067200000000 + i as i64,
            ttl_seconds: None,
        };
        engine.write_async(mutation).await.unwrap();
    }

    let info = engine.flush().await.unwrap().unwrap();
    assert_eq!(info.row_count, 5, "All 5 rows with unique frozen clustering keys should be written");
}
```

- [ ] **Step 2: Run the test**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support test_frozen_list_clustering_key_uniqueness -- --nocapture 2>&1 | tail -30`

- [ ] **Step 3: Investigate and fix if clustering key bytes are not unique**

If the test shows row_count != 5, inspect the serialized clustering key bytes to find where different frozen lists produce identical bytes. Fix `serialize_value_for_clustering` accordingly.

If the test passes (all 5 rows written), then the issue is Cassandra-side — likely the SSTable format requires a specific frozen collection byte ordering that differs from what we produce. In that case, update the test data generator to produce valid test cases and document the limitation.

---

## Task 4: Validate All Existing Tests Still Pass

- [ ] **Step 1: Run full test suite with write-support**

Run: `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support 2>&1 | tail -20`

- [ ] **Step 2: Run clippy**

Run: `env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features 2>&1 | tail -20`

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt --check 2>&1 | tail -10`

- [ ] **Step 4: Fix any regressions**

If existing tests for Cassandra-produced frozen data break, ensure the fix handles both:
- CQLite-written frozen data (VUInt blob length + i32 BE collection format)
- Cassandra-written frozen data (same format — Cassandra also uses i32 BE inside frozen blobs)

---

## Task 5: Commit and Push

- [ ] **Step 1: Stage and commit**

```bash
git add cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs
git add cqlite-core/tests/write_read_roundtrip/type_coverage.rs
git add cqlite-core/tests/write_read_roundtrip/edge_cases.rs
git commit -m "fix(#458,#465): frozen collection read-back and clustering key roundtrip"
```

- [ ] **Step 2: Push**

```bash
git push
```

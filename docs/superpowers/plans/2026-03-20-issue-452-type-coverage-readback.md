# Issue #452: True Read-Back Assertions for Type Coverage Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade 14 write-only type smoke tests in `type_coverage.rs` to true write→flush→read roundtrip tests that verify deserialized values match original inputs.

**Architecture:** Add a shared `read_back_column()` helper to `write_read_roundtrip.rs` that uses `SSTableManager` to scan the flushed SSTable and extract a named column value from the `Value::Map` row. Each type test calls `write_single_value()` then `read_back_column()` and asserts equality. Also fix `create_type_test_schema()` to sanitize table names for parameterized types (e.g., `frozen<list<int>>` → `test_frozen`). NaN values are intentionally excluded from float tests since `PartialEq` returns false for `NaN == NaN`.

**Tech Stack:** Rust, tokio, cqlite-core (SSTableManager, WriteEngine, Value), tempfile

---

## Key Facts

- `SSTableManager::scan()` returns `Vec<(RowKey, Value)>` where `Value` is `Value::Map(Vec<(Value::Text(col_name), col_value)>)`
- `Value` derives `PartialEq` — direct `==` works for all types except floats
- The `write_single_value()` helper in `type_coverage.rs` writes to `keyspace=test_types`, `table=test_{type}` with `pk: int` partition key
- The `full_roundtrip.rs` pattern shows exactly how to create `SSTableManager` and scan
- Counter type uses `BigInt(i64)` in Cassandra read path — the writer stores it as `Counter(i64)` but reader may decode differently. Test must handle this.
- `Frozen(Box<Value>)` wrapper may be stripped by the reader — inner value returned directly. Test must handle unwrapping.

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `cqlite-core/tests/write_read_roundtrip.rs` | Modify | Add `read_back_column()` helper |
| `cqlite-core/tests/write_read_roundtrip/type_coverage.rs` | Modify | Upgrade 14+ tests to use read-back assertions |

---

### Task 1: Add `read_back_column` helper to module root

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip.rs`

This helper encapsulates the SSTableManager creation and scan logic so each type test can call it with minimal boilerplate.

- [ ] **Step 1: Add imports and helper function**

Add to `cqlite-core/tests/write_read_roundtrip.rs` after the existing imports:

```rust
use cqlite_core::Config;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableManager;
use std::sync::Arc;
```

Then add this helper function after `read_file_bytes()`:

```rust
/// Read back a single column value from a flushed SSTable.
///
/// Opens SSTableManager on the data directory, scans the table,
/// and extracts the named column from the first (and only) row.
/// The row is returned as Value::Map(Vec<(Text(col_name), value)>).
pub async fn read_back_column(
    temp_dir: &TempDir,
    schema: &cqlite_core::schema::TableSchema,
    col_name: &str,
) -> Value {
    let data_dir = temp_dir.path().join("data");
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let manager = SSTableManager::new(
        &data_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager should load written SSTables");

    let table_id = cqlite_core::types::TableId::from(
        format!("{}.{}", schema.keyspace, schema.table).as_str(),
    );
    let results = manager
        .scan(&table_id, None, None, None, Some(schema))
        .await
        .expect("Scan should succeed");

    assert_eq!(results.len(), 1, "Expected exactly 1 row, got {}", results.len());

    let (_row_key, row_value) = &results[0];

    // Row is Value::Map(Vec<(Value::Text(col_name), value)>)
    match row_value {
        Value::Map(entries) => {
            for (key, value) in entries {
                if let Value::Text(name) = key {
                    if name == col_name {
                        return value.clone();
                    }
                }
            }
            panic!(
                "Column '{}' not found in row. Available columns: {:?}",
                col_name,
                entries.iter().filter_map(|(k, _)| {
                    if let Value::Text(n) = k { Some(n.as_str()) } else { None }
                }).collect::<Vec<_>>()
            );
        }
        other => panic!(
            "Expected row to be Value::Map, got {:?}",
            std::mem::discriminant(other)
        ),
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo build --package cqlite-core --features write-support --tests`
Expected: Compiles successfully (helper is not yet called, but must compile)

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip.rs
git commit -m "feat(#452): add read_back_column helper for type roundtrip tests"
```

---

### Task 2: Upgrade tinyint, smallint, blob, date, time tests

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/type_coverage.rs`

These types use direct `Value::PartialEq` — straightforward equality.

- [ ] **Step 1: Add read-back to tinyint roundtrip test**

Replace the body of `test_type_tinyint_roundtrip` with:

```rust
#[tokio::test]
async fn test_type_tinyint_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("tinyint_col", "tinyint");

    let original = Value::TinyInt(42);
    let info = write_single_value(&temp_dir, &schema, "tinyint_col", original.clone()).await;
    assert_single_partition_written(&info);

    let read_back = super::read_back_column(&temp_dir, &schema, "tinyint_col").await;
    assert_eq!(read_back, original, "TinyInt roundtrip failed");
}
```

Apply the same pattern to:
- `test_type_tinyint_min` → `Value::TinyInt(i8::MIN)`
- `test_type_tinyint_max` → `Value::TinyInt(i8::MAX)`
- `test_type_smallint_roundtrip` → `Value::SmallInt(1000)`
- `test_type_smallint_min` → `Value::SmallInt(i16::MIN)`
- `test_type_smallint_max` → `Value::SmallInt(i16::MAX)`
- `test_type_blob_roundtrip` → `Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])`
- `test_type_blob_empty` → `Value::Blob(vec![])`
- `test_type_date_roundtrip` → `Value::Date(19723)`
- `test_type_date_epoch` → `Value::Date(0)`
- `test_type_time_roundtrip` → `Value::Time(43_200_000_000_000)`
- `test_type_time_midnight` → `Value::Time(0)`
- `test_type_time_max` → `Value::Time(86_399_999_999_999)`

- [ ] **Step 2: Run tests**

Run: `cargo test --package cqlite-core --features write-support test_type_tinyint_roundtrip test_type_smallint_roundtrip test_type_blob_roundtrip test_type_date_roundtrip test_type_time_roundtrip -- --nocapture`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip/type_coverage.rs
git commit -m "test(#452): add read-back assertions for tinyint, smallint, blob, date, time"
```

---

### Task 3: Upgrade float and double tests

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/type_coverage.rs`

Float/double need care: finite values should roundtrip exactly (IEEE 754 binary representation is preserved). Special values (infinity, NaN) may need special handling.

- [ ] **Step 1: Add read-back to float32 roundtrip test**

```rust
#[tokio::test]
async fn test_type_float32_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("float_col", "float");

    let original = Value::Float32(1.234_567);
    let info = write_single_value(&temp_dir, &schema, "float_col", original.clone()).await;
    assert_single_partition_written(&info);

    let read_back = super::read_back_column(&temp_dir, &schema, "float_col").await;
    assert_eq!(read_back, original, "Float32 roundtrip failed");
}
```

For `test_type_float32_special` (0.0) and `test_type_float32_min` (f32::MIN), apply same pattern.

For `test_type_double_roundtrip`:

```rust
#[tokio::test]
async fn test_type_double_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("double_col", "double");

    let original = Value::Float(9.876_543_210_123_456);
    let info = write_single_value(&temp_dir, &schema, "double_col", original.clone()).await;
    assert_single_partition_written(&info);

    let read_back = super::read_back_column(&temp_dir, &schema, "double_col").await;
    assert_eq!(read_back, original, "Double roundtrip failed");
}
```

For `test_type_double_special` (infinity) and `test_type_double_min_max` (f64::MIN), apply same pattern.

- [ ] **Step 2: Run tests**

Run: `cargo test --package cqlite-core --features write-support test_type_float32 test_type_double -- --nocapture`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip/type_coverage.rs
git commit -m "test(#452): add read-back assertions for float and double types"
```

---

### Task 4: Upgrade inet, varint, decimal tests

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/type_coverage.rs`

These use `Vec<u8>` or struct equality — `PartialEq` handles them directly.

- [ ] **Step 1: Add read-back to all inet tests**

Apply the `write_single_value` + `read_back_column` + `assert_eq!` pattern to:
- `test_type_inet_ipv4` → `Value::Inet(vec![192, 168, 1, 1])`
- `test_type_inet_ipv6` → `Value::Inet(vec![0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1])`
- `test_type_inet_loopback` → `Value::Inet(vec![127, 0, 0, 1])`
- `test_type_varint_small` → `Value::Varint(vec![0x2A])`
- `test_type_varint_large` → the 9-byte value
- `test_type_varint_negative` → `Value::Varint(vec![0xFF])`
- `test_type_decimal_roundtrip` → `Value::Decimal { scale: 2, unscaled: vec![0x30, 0x39] }`
- `test_type_decimal_zero` → `Value::Decimal { scale: 0, unscaled: vec![0] }`
- `test_type_decimal_neg_scale` → `Value::Decimal { scale: -2, unscaled: vec![1] }`

- [ ] **Step 2: Run tests**

Run: `cargo test --package cqlite-core --features write-support test_type_inet test_type_varint test_type_decimal -- --nocapture`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip/type_coverage.rs
git commit -m "test(#452): add read-back assertions for inet, varint, decimal types"
```

---

### Task 5: Upgrade duration, tuple, frozen tests

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/type_coverage.rs`

Duration uses struct equality. Tuple uses `Vec<Value>` equality. Frozen may have its wrapper stripped by the reader — test should handle both `Frozen(inner)` and bare `inner`.

- [ ] **Step 1: Add read-back to duration tests**

Apply pattern to:
- `test_type_duration_roundtrip` → `Value::Duration { months: 1, days: 15, nanos: 3_600_000_000_000 }`
- `test_type_duration_zero` → all zeros
- `test_type_duration_negative` → negative values

- [ ] **Step 2: Add read-back to tuple tests**

Apply pattern to:
- `test_type_tuple_roundtrip` → `Value::Tuple(vec![Integer(42), Text("hello")])`
- `test_type_tuple_with_null` → `Value::Tuple(vec![Integer(42), Null])`
- `test_type_tuple_nested` → nested tuple

- [ ] **Step 3: Add read-back to frozen tests**

For frozen, the reader may strip the `Frozen` wrapper and return the inner value directly. Use a comparison that handles both:

```rust
#[tokio::test]
async fn test_type_frozen_list() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("frozen_col", "frozen<list<int>>");

    let inner = Value::List(vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]);
    let original = Value::Frozen(Box::new(inner.clone()));
    let info = write_single_value(&temp_dir, &schema, "frozen_col", original.clone()).await;
    assert_single_partition_written(&info);

    let read_back = super::read_back_column(&temp_dir, &schema, "frozen_col").await;
    // Reader may return Frozen(inner) or just inner
    assert!(
        read_back == original || read_back == inner,
        "Frozen list roundtrip failed: got {:?}",
        read_back
    );
}
```

Apply same pattern to `test_type_frozen_map` and `test_type_frozen_empty`.

- [ ] **Step 4: Run tests**

Run: `cargo test --package cqlite-core --features write-support test_type_duration test_type_tuple test_type_frozen -- --nocapture`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip/type_coverage.rs
git commit -m "test(#452): add read-back assertions for duration, tuple, frozen types"
```

---

### Task 6: Upgrade counter tests

**Files:**
- Modify: `cqlite-core/tests/write_read_roundtrip/type_coverage.rs`

Counter columns in Cassandra are special — they're stored as 64-bit integers. The reader may decode them as `BigInt` rather than `Counter`. The test should accept either representation.

- [ ] **Step 1: Add read-back to counter tests**

```rust
#[tokio::test]
async fn test_type_counter_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("counter_col", "counter");

    let original = Value::Counter(100);
    let info = write_single_value(&temp_dir, &schema, "counter_col", original.clone()).await;
    assert_single_partition_written(&info);

    let read_back = super::read_back_column(&temp_dir, &schema, "counter_col").await;
    // Reader may return Counter(100) or BigInt(100)
    assert!(
        read_back == original || read_back == Value::BigInt(100),
        "Counter roundtrip failed: got {:?}",
        read_back
    );
}
```

Apply to `test_type_counter_zero` (Counter(0)/BigInt(0)) and `test_type_counter_negative` (Counter(-50)/BigInt(-50)).

- [ ] **Step 2: Run tests**

Run: `cargo test --package cqlite-core --features write-support test_type_counter -- --nocapture`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/tests/write_read_roundtrip/type_coverage.rs
git commit -m "test(#452): add read-back assertions for counter type"
```

---

### Task 7: Run full test suite and CI checks

**Files:**
- All modified files from Tasks 1-6

- [ ] **Step 1: Run all type coverage tests**

Run: `cargo test --package cqlite-core --features write-support type_coverage -- --nocapture`
Expected: All tests PASS

- [ ] **Step 2: Run full write_read_roundtrip suite**

Run: `cargo test --package cqlite-core --features write-support write_read_roundtrip -- --nocapture`
Expected: All tests PASS (no regressions in existing tests)

- [ ] **Step 3: Run clippy**

Run: `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: No warnings or errors

- [ ] **Step 4: Run cargo fmt**

Run: `cargo fmt --check`
Expected: No formatting issues

- [ ] **Step 5: Final commit if any fixups needed**

```bash
cargo fmt
git add -A
git commit -m "style(#452): apply cargo fmt"
```

- [ ] **Step 6: Push**

```bash
git push
```

---

## Type-Specific Comparison Reference

| Type | Comparison Method | Notes |
|------|------------------|-------|
| tinyint | `==` (PartialEq) | Direct i8 equality |
| smallint | `==` | Direct i16 equality |
| float | `==` | IEEE 754 binary preserved for finite values |
| double | `==` | IEEE 754 binary preserved for finite values |
| blob | `==` | Byte-for-byte Vec<u8> equality |
| date | `==` | Direct i32 equality |
| time | `==` | Direct i64 equality |
| counter | `== Counter OR == BigInt` | Reader may decode as BigInt |
| inet | `==` | Byte-for-byte Vec<u8> equality |
| varint | `==` | Byte-for-byte Vec<u8> equality |
| decimal | `==` | Struct equality (scale + unscaled bytes) |
| duration | `==` | Struct equality (months, days, nanos) |
| tuple | `==` | Recursive Value equality |
| frozen | `== Frozen(inner) OR == inner` | Reader may strip Frozen wrapper |

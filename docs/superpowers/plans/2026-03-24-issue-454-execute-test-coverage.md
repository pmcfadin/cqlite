# Issue #454: Replace Stale execute Test with Accurate WriteEngine Coverage

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the stale `test_write_engine_execute_not_implemented` test with accurate tests that reflect the current implemented behavior of `WriteEngine::execute()`.

**Architecture:** Rename the existing negative test to describe what it actually checks (table mismatch), strengthen its assertion, and add a new positive test that exercises the real `execute()` success path using the existing `create_test_schema()` helper (keyspace=`test_ks`, table=`test_table`, columns: `id int PK`, `name text`).

**Tech Stack:** Rust, cargo test with `--features write-support`

---

## File Structure

- **Modify:** `cqlite-core/src/storage/write_engine/mod.rs` — test module only (lines ~1460-1476)
- No new files needed

---

### Task 1: Rename and Strengthen the Negative Test

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/mod.rs:1460-1476`

- [ ] **Step 1: Rename test and strengthen assertion**

Replace the existing `test_write_engine_execute_not_implemented` test with `test_write_engine_execute_table_mismatch`. The test exercises the same code path (INSERT into `users` when schema defines `test_table`) but now:
- Has an accurate name describing the table-mismatch error path
- Asserts on the specific error message, not just `is_err()`

```rust
#[test]
fn test_write_engine_execute_table_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config).unwrap();

    // Schema defines test_table, but statement targets users → table mismatch
    let result = engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("targets table 'users'") && err_msg.contains("schema is for 'test_table'"),
        "Expected table mismatch error, got: {}",
        err_msg
    );
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo test --package cqlite-core --features write-support test_write_engine_execute_table_mismatch -- --exact`
Expected: PASS — the table mismatch error message matches

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/src/storage/write_engine/mod.rs
git commit -m "fix(#454): rename stale execute test to test_write_engine_execute_table_mismatch"
```

---

### Task 2: Add Positive Test for execute() Success Path

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/mod.rs` (add new test after the renamed one)

- [ ] **Step 1: Write the positive test**

Add a new test that INSERTs into the correct table (`test_table` in `test_ks`) and verifies success via `Ok(())` return and `memtable_row_count()`.

```rust
#[test]
fn test_write_engine_execute_insert_success() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config).unwrap();

    assert_eq!(engine.memtable_row_count(), 0);

    // INSERT matching the test schema: test_ks.test_table(id int PK, name text)
    let result = engine.execute("INSERT INTO test_table (id, name) VALUES (1, 'Alice')");
    assert!(result.is_ok(), "execute() failed: {:?}", result.unwrap_err());

    assert_eq!(engine.memtable_row_count(), 1);
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo test --package cqlite-core --features write-support test_write_engine_execute_insert_success -- --exact`
Expected: PASS — execute returns Ok and memtable has 1 row

- [ ] **Step 3: Run all write engine tests to ensure no regressions**

Run: `cargo test --package cqlite-core --features write-support write_engine -- --nocapture`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add cqlite-core/src/storage/write_engine/mod.rs
git commit -m "test(#454): add positive test for WriteEngine::execute() insert success"
```

---

### Task 3: Validate Code Quality

- [ ] **Step 1: Run cargo fmt**

Run: `cargo fmt`

- [ ] **Step 2: Run clippy**

Run: `env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: No warnings or errors

- [ ] **Step 3: Run full test suite**

Run: `cargo test --workspace --all-features`
Expected: All tests pass

- [ ] **Step 4: Commit any fmt/clippy fixes if needed, then push**

```bash
git push
```

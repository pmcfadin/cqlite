# Issue #8: Feature-Gate Write Methods (Preparation for Removal)

**Priority:** P1  
**Risk Level:** Medium (Touches StorageEngine Core)  
**Estimated Time:** 4 hours  
**Assignee:** Team C (Infrastructure)  
**Branch:** `cleanup/issue-8-feature-gate-writes`  
**Can Parallelize:** ❌ No (Sequential with #9, #10)

---

## Objective

Feature-gate all write-related methods behind `experimental` feature to enable safe removal in subsequent issues.

---

## Problem Statement

`StorageEngine` currently has write methods (`put`, `delete`) that are:
1. Not needed for M1 (Core Reading Library)
2. Part of M5 (Write Support) scope
3. Tightly coupled to WAL, MemTable, Manifest, and Compaction

Before we can remove write infrastructure (Issues #9, #10), we need to:
1. Feature-gate write methods
2. Verify nothing in M1/M2 scope uses them
3. Update tests to explicitly enable the feature

**This is the critical preparatory step that unblocks write infrastructure removal.**

---

## Files to Modify

### 1. `cqlite-core/src/storage/mod.rs` (Primary Changes)

**Current public write methods:**
- `pub async fn put(&self, ...) -> Result<()>`
- `pub async fn delete(&self, ...) -> Result<()>`
- `pub async fn flush(&self) -> Result<()>`
- `pub async fn compact(&self) -> Result<()>`

**Change to:**
```rust
#[cfg(feature = "experimental")]
pub async fn put(&self, table_id: &TableId, key: RowKey, value: Value) -> Result<()> {
    // ... existing implementation
}

#[cfg(feature = "experimental")]
pub async fn delete(&self, table_id: &TableId, key: RowKey) -> Result<()> {
    // ... existing implementation
}

// Note: flush() and compact() might be needed for read-side cache flushing
// Audit these carefully before gating
```

### 2. `cqlite-core/src/lib.rs` (Database API)

**Feature-gate write methods on Database:**
```rust
/// Flush all pending writes to disk
#[cfg(feature = "experimental")]
pub async fn flush(&self) -> Result<()> {
    self.storage.flush().await
}

/// Perform manual compaction of storage files  
#[cfg(feature = "experimental")]
pub async fn compact(&self) -> Result<()> {
    self.storage.compact().await
}
```

### 3. `cqlite-core/src/query/executor.rs`

**Audit for write operations:**
- Search for `storage.put`
- Search for `storage.delete`
- Wrap any found in `#[cfg(feature = "experimental")]`

### 4. Tests Using Write Operations

**Update tests to enable feature:**
```rust
#[cfg(all(test, feature = "experimental"))]
mod write_tests {
    // ... tests that use put/delete
}
```

Or in test functions:
```rust
#[test]
#[cfg(feature = "experimental")]
fn test_write_operation() {
    // ...
}
```

---

## Step-by-Step Instructions

### Step 1: Create Branch

```bash
git checkout -b cleanup/issue-8-feature-gate-writes
```

### Step 2: Identify All Write Call Sites

```bash
# Find all calls to write methods
rg "\.put\(" cqlite-core/src/ -A2 -B2 > write-calls.txt
rg "\.delete\(" cqlite-core/src/ -A2 -B2 >> write-calls.txt
rg "storage\.flush" cqlite-core/src/ >> write-calls.txt
rg "storage\.compact" cqlite-core/src/ >> write-calls.txt

# Review write-calls.txt to understand usage
cat write-calls.txt
```

### Step 3: Feature-Gate Storage Methods

Edit `cqlite-core/src/storage/mod.rs`:

```rust
impl StorageEngine {
    // ... existing methods ...
    
    /// Write a key-value pair (M5 Write Support - Experimental)
    #[cfg(feature = "experimental")]
    pub async fn put(&self, table_id: &TableId, key: RowKey, value: Value) -> Result<()> {
        // existing implementation unchanged
    }
    
    /// Delete a key (M5 Write Support - Experimental)
    #[cfg(feature = "experimental")]
    pub async fn delete(&self, table_id: &TableId, key: RowKey) -> Result<()> {
        // existing implementation unchanged
    }
    
    // AUDIT: Do flush/compact need to stay for reading?
    // If yes, keep them. If no, gate them too.
}
```

### Step 4: Feature-Gate Database Methods

Edit `cqlite-core/src/lib.rs`:

```rust
impl Database {
    // ... existing methods ...
    
    /// Flush pending writes (M5 - Experimental)
    #[cfg(feature = "experimental")]
    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }
    
    /// Compact storage files (M5 - Experimental)
    #[cfg(feature = "experimental")]
    pub async fn compact(&self) -> Result<()> {
        self.storage.compact().await
    }
}
```

### Step 5: Update Tests

Find tests using write methods:
```bash
rg "#\[.*test\]" cqlite-core/src/ -A 10 | grep -B 5 "\.put\|\.delete"
```

Wrap test modules or add feature gate:
```rust
#[cfg(all(test, feature = "experimental"))]
mod write_tests {
    // tests here
}
```

### Step 6: Update Integration Tests

Edit `cqlite-core/src/lib.rs` bottom tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Keep read tests as-is
    
    #[tokio::test]
    #[cfg(feature = "experimental")]  // ADD THIS
    async fn test_database_basic_operations() {
        // This test uses INSERT/write operations
    }
}
```

### Step 7: Verify Builds

```bash
# Should fail (write methods not available)
cargo build --no-default-features --features=all-compression

# Should succeed (experimental includes writes)
cargo build --no-default-features --features=all-compression,experimental

# Should succeed (default includes experimental)
cargo build --all-features
```

### Step 8: Fix Broken Tests

```bash
# Run tests without experimental
cargo test --no-default-features --features=all-compression 2>&1 | tee test-output.txt

# Expected: Some tests fail/skip (write-related ones)
# Fix by adding #[cfg(feature = "experimental")] to those tests
```

### Step 9: Update Test Runner

Edit `.github/workflows/ci.yml`:

```yaml
- name: Test with write support
  run: cargo test --features=experimental
  
- name: Test M1 scope only (no writes)
  run: cargo test --no-default-features --features=all-compression
```

---

## Testing Checklist

- [ ] Identify all write method call sites
- [ ] Feature-gate StorageEngine write methods
- [ ] Feature-gate Database write methods
- [ ] Feature-gate tests using writes
- [ ] `cargo build --no-default-features --features=all-compression` - fails on write usage (expected)
- [ ] `cargo build --features=experimental` - succeeds
- [ ] `cargo test --features=experimental` - all pass
- [ ] `cargo test --no-default-features --features=all-compression` - read tests pass
- [ ] Update CI configuration
- [ ] No regression in default build

---

## Verification Commands

```bash
# Verify write methods are gated
rg "pub async fn (put|delete)\(" cqlite-core/src/storage/mod.rs

# Should show #[cfg(feature = "experimental")] above the methods

# Test different feature combinations
cargo build --no-default-features
cargo build --no-default-features --features=all-compression
cargo build --no-default-features --features=all-compression,experimental

# Run validation
./scripts/validate-cleanup.sh
```

---

## Expected Impact

- **Lines Changed:** ~50-100 (add feature gates)
- **Breaking Changes:** YES - if users depend on write methods without `experimental` feature
- **Test Count:** Some tests will be skipped in M1-only builds
- **CI:** Need separate job for write feature testing

---

## Success Criteria

✅ All write methods feature-gated  
✅ M1-only build succeeds (without writes)  
✅ Full feature build succeeds (with writes)  
✅ Tests properly gated  
✅ CI updated with separate test jobs  
✅ Documentation updated about feature requirements  

---

## Dependencies

**Requires:** Issues #1-#7 complete (or at least #1)  
**Blocks:** Issues #9, #10 (cannot remove write infrastructure until methods are gated)  
**Must Be Done Before:** Any write infrastructure removal

---

## Potential Complications

### Complication 1: QueryExecutor Uses Writes

**Check:**
```bash
rg "storage\.put\|storage\.delete" cqlite-core/src/query/
```

**If found:** 
- Feature-gate those code paths
- Or make QueryExecutor conditionally compiled with `experimental`

**Solution:**
```rust
#[cfg(feature = "experimental")]
impl QueryExecutor {
    async fn execute_insert(&self, ...) -> Result<()> {
        // Uses storage.put
    }
}
```

### Complication 2: Database Tests Fail

**If:**
```rust
#[tokio::test]
async fn test_database_basic_operations() {
    db.execute("INSERT INTO ...").await?;
}
```

**Fix:**
```rust
#[tokio::test]
#[cfg(all(feature = "legacy-heuristics", feature = "experimental"))]
async fn test_database_basic_operations() {
    // ... test unchanged
}
```

### Complication 3: Storage Stats Need Writes

**If:** `StorageEngine::stats()` depends on write infrastructure (WAL, memtable)

**Solution:** Make stats conditionally include write stats:
```rust
pub struct StorageStats {
    pub sstables: SSTableStats,
    
    #[cfg(feature = "experimental")]
    pub wal_size: u64,
    
    #[cfg(feature = "experimental")]
    pub memtable_size: usize,
}
```

---

## Rollback Plan

```bash
# Full rollback
git revert <commit-hash>

# Partial rollback (keep some gates, remove others)
git checkout main -- cqlite-core/src/storage/mod.rs
# Edit manually to keep desired gates
git commit -m "Partial rollback: Restore some write methods"
```

---

## CI Checks to Monitor

- ✅ Build with all features
- ✅ Build with minimal features (should succeed, no write methods available)
- ✅ Test with `experimental` feature
- ✅ Test without `experimental` (fewer tests, should still pass)
- ⚠️ Check for compile errors about missing methods

---

## Documentation Updates

Add to `cqlite-core/README.md`:

```markdown
## Feature Flags

### M1 Core Reading (Stable)
```toml
cqlite-core = { version = "*", default-features = false, features = ["all-compression"] }
```

### M5 Write Support (Experimental)
```toml
cqlite-core = { version = "*", features = ["experimental"] }
```

**Note:** Write operations (`put`, `delete`, `flush`, `compact`) require the `experimental` feature.
```

---

## Notes

- **Critical step:** This unblocks removal of WAL, memtable, compaction, manifest
- **Medium risk:** Requires careful audit of all write usage
- **Cannot parallelize:** Must be done before Issues #9, #10
- **Thorough testing required:** Verify both feature combinations work

---

## Completion Checklist

- [ ] Branch created
- [ ] All write call sites identified
- [ ] Storage methods feature-gated
- [ ] Database methods feature-gated
- [ ] Tests updated with feature gates
- [ ] M1-only build succeeds
- [ ] Full feature build succeeds
- [ ] M1 tests pass
- [ ] Write tests pass (with feature)
- [ ] CI updated
- [ ] Documentation updated
- [ ] PR created
- [ ] Reviewed by senior engineer
- [ ] CI green (both feature combos)
- [ ] Merged
- [ ] Tag Team C to start Issue #9


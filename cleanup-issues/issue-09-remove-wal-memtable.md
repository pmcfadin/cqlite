# Issue #9: Remove WAL and MemTable (Write Infrastructure)

**Priority:** P1  
**Risk Level:** Medium (Core Storage Changes)  
**Estimated Time:** 4 hours  
**Assignee:** Team C (Infrastructure)  
**Branch:** `cleanup/issue-9-remove-wal-memtable`  
**Can Parallelize:** ❌ No (Must Follow #8)

---

## Objective

Remove Write-Ahead Log and MemTable components that are M5 write infrastructure, not needed for M1 reading.

---

## Problem Statement

`StorageEngine` includes write-side components:
- **WAL (Write-Ahead Log):** Durability for writes (377 lines)
- **MemTable:** In-memory write buffer (393 lines)

These are used by write operations (now gated behind `experimental` in Issue #8). Since M1 is read-only, these can be removed.

**Impact:** Reduces memory usage, faster initialization, cleaner architecture.

---

## Files to Delete

```
cqlite-core/src/storage/wal.rs          (377 lines)
cqlite-core/src/storage/memtable.rs     (393 lines)
```

**Total:** 770 lines

---

## Files to Modify

### 1. `cqlite-core/src/storage/mod.rs`

**Remove module declarations:**
```rust
pub mod wal;
pub mod memtable;
```

**Remove from StorageEngine struct:**
```rust
pub struct StorageEngine {
    // DELETE these fields:
    memtable: Arc<RwLock<memtable::MemTable>>,
    wal: Arc<wal::WriteAheadLog>,
    
    // KEEP these:
    sstables: Arc<sstable::SSTableManager>,
    // ... other read-side fields
}
```

**Remove from constructor:**
```rust
impl StorageEngine {
    pub async fn open(...) -> Result<Self> {
        // DELETE:
        let wal = Arc::new(wal::WriteAheadLog::open(...).await?);
        let memtable = Arc::new(RwLock::new(memtable::MemTable::new(config)?));
        
        // In return struct, DELETE:
        Ok(Self {
            // memtable,
            // wal,
            sstables,
            ...
        })
    }
}
```

### 2. `cqlite-core/src/storage/batch_writer.rs`

**Issue:** BatchWriter imports WAL. This file is M5 and should be deleted too, but that's Issue #10.

**For now:** Keep BatchWriter but make it not import WAL (may break it, but it's not used anyway).

Or just delete BatchWriter in this issue too (simpler).

---

## Step-by-Step Instructions

### Step 1: Verify Issue #8 Complete

```bash
# Ensure write methods are gated
rg "#\[cfg\(feature = \"experimental\"\)\]" cqlite-core/src/storage/mod.rs | grep -A1 "pub async fn put"

# Expected: Write methods have feature gates
```

### Step 2: Create Branch

```bash
git checkout -b cleanup/issue-9-remove-wal-memtable
```

### Step 3: Find All WAL/MemTable Usage

```bash
# Find imports
rg "use.*wal::|use.*memtable::" cqlite-core/src/storage/

# Find field access
rg "self\.wal\.|self\.memtable\." cqlite-core/src/storage/mod.rs

# Save to file for review
rg "wal\.|memtable\." cqlite-core/src/storage/mod.rs > usage.txt
```

### Step 4: Delete Files

```bash
git rm cqlite-core/src/storage/wal.rs
git rm cqlite-core/src/storage/memtable.rs
```

### Step 5: Update storage/mod.rs

Edit `cqlite-core/src/storage/mod.rs`:

**Remove module declarations (top of file):**
```rust
// DELETE:
pub mod wal;
pub mod memtable;
```

**Remove imports:**
```rust
use crate::storage::wal::WriteAheadLog;  // DELETE
use crate::storage::memtable::MemTable;   // DELETE
```

**Update StorageEngine struct:**
```rust
pub struct StorageEngine {
    // DELETE:
    // memtable: Arc<RwLock<memtable::MemTable>>,
    // wal: Arc<wal::WriteAheadLog>,
    
    // KEEP:
    sstables: Arc<sstable::SSTableManager>,
    manifest: Arc<manifest::Manifest>,
    compaction: Arc<compaction::CompactionManager>,
    _platform: Arc<Platform>,
    config: Config,
    batch_writer: Option<BatchWriter>,
    #[cfg(feature = "state_machine")]
    schema_registry: Arc<RwLock<Option<Arc<RwLock<crate::schema::SchemaRegistry>>>>>,
}
```

**Update open() method:**
```rust
impl StorageEngine {
    pub async fn open(...) -> Result<Self> {
        // ... existing setup ...
        
        // DELETE these:
        // let wal = Arc::new(wal::WriteAheadLog::open(path, config, platform.clone()).await?);
        // let memtable = Arc::new(RwLock::new(memtable::MemTable::new(config)?));
        
        // Keep SSTable manager, manifest, compaction (for now)
        let sstables = Arc::new(sstable::SSTableManager::new(...).await?);
        let manifest = Arc::new(manifest::Manifest::open(path, config).await?);
        let compaction = Arc::new(compaction::CompactionManager::new(...).await?);
        
        Ok(Self {
            // DELETE from return:
            // memtable,
            // wal,
            
            // KEEP:
            sstables,
            manifest,
            compaction,
            _platform: platform,
            config: config.clone(),
            batch_writer: None,  // Will remove in Issue #10
            #[cfg(feature = "state_machine")]
            schema_registry: Arc::new(RwLock::new(schema_registry)),
        })
    }
}
```

**Update write methods (already gated in Issue #8):**
```rust
#[cfg(feature = "experimental")]
pub async fn put(&self, table_id: &TableId, key: RowKey, value: Value) -> Result<()> {
    // DELETE WAL/memtable logic:
    // self.wal.append(...).await?;
    // self.memtable.write().await.insert(...)?;
    
    // REPLACE with error or stub:
    Err(Error::internal("Write support removed - M5 feature"))
}
```

### Step 6: Handle flush() Method

**Decision:** Does flush() need to exist for reading?

**Option A:** Delete it (it's for writes)
```rust
// DELETE entirely or gate behind experimental
```

**Option B:** Keep as no-op for API compatibility
```rust
pub async fn flush(&self) -> Result<()> {
    // No-op: M1 has no write buffers to flush
    Ok(())
}
```

Choose Option A (delete) for clean M1 scope.

### Step 7: Compile and Fix Errors

```bash
cargo build --no-default-features --features=all-compression 2>&1 | tee build-errors.txt

# Expect errors about missing WAL/MemTable
# Fix each by removing the usage
```

### Step 8: Update Tests

```bash
# Tests using WAL/MemTable directly
rg "WriteAheadLog\|MemTable" cqlite-core/src/ --type rust

# Gate or delete those tests
```

### Step 9: Verify Builds

```bash
# M1 build (should work now)
cargo build --no-default-features --features=all-compression

# Full build (should work, write methods return errors)
cargo build --all-features

# Tests
cargo test --no-default-features --features=all-compression
```

---

## Testing Checklist

- [ ] Issue #8 complete (write methods gated)
- [ ] All WAL/MemTable usage identified
- [ ] Files deleted
- [ ] storage/mod.rs updated (struct, imports, constructor)
- [ ] Write methods updated to return errors
- [ ] flush() method handled (deleted or no-op)
- [ ] M1 build succeeds
- [ ] M1 tests pass
- [ ] No references to WAL/MemTable remain
- [ ] Validation passes

---

## Verification Commands

```bash
# Ensure no references remain
rg "WriteAheadLog|MemTable|wal\.|memtable\." cqlite-core/src/

# Clean build
cargo clean
cargo build --no-default-features --features=all-compression

# Test
cargo test --no-default-features --features=all-compression

# Validation
./scripts/validate-cleanup.sh
```

---

## Expected Impact

- **Lines Removed:** 770
- **Memory Usage:** Reduced (no in-memory write buffer)
- **Startup Time:** Faster (no WAL recovery)
- **Breaking:** Write methods now return errors (but already gated)

---

## Success Criteria

✅ WAL and MemTable files deleted  
✅ StorageEngine no longer has WAL/MemTable fields  
✅ M1 build succeeds  
✅ M1 tests pass  
✅ No compilation errors  
✅ CI green  

---

## Dependencies

**Requires:** Issue #8 complete (write methods gated)  
**Blocks:** Issue #10 (manifest/compaction removal)  
**Must Follow:** Issue #8 sequentially

---

## Potential Complications

### Complication 1: BatchWriter Imports WAL

**Issue:** `batch_writer.rs` imports `wal::WriteAheadLog`.

**Solution A:** Delete batch_writer.rs too (it's M5, unused).  
**Solution B:** Make BatchWriter optional, comment out WAL usage.

**Recommendation:** Delete it (will do in Issue #10 anyway).

### Complication 2: StorageEngine::stats() References WAL/MemTable

**Check:**
```rust
pub async fn stats(&self) -> Result<StorageStats> {
    StorageStats {
        wal_size: self.wal.size().await?,  // ERROR
        memtable_entries: self.memtable.read().await.len(),  // ERROR
    }
}
```

**Fix:**
```rust
pub async fn stats(&self) -> Result<StorageStats> {
    StorageStats {
        sstables: self.sstables.stats().await?,
        // Remove wal_size and memtable_entries fields
    }
}
```

Update `StorageStats` struct definition.

### Complication 3: shutdown() Method Accesses WAL

**If:**
```rust
pub async fn shutdown(&self) -> Result<()> {
    self.wal.close().await?;  // ERROR
}
```

**Fix:** Remove WAL shutdown logic.

---

## Rollback Plan

```bash
git revert <commit-hash>
# Or:
git checkout main -- cqlite-core/src/storage/wal.rs
git checkout main -- cqlite-core/src/storage/memtable.rs
git checkout main -- cqlite-core/src/storage/mod.rs
git commit -m "Rollback: Restore WAL and MemTable"
```

---

## CI Checks

- ✅ Build (minimal features)
- ✅ Build (all features)  
- ✅ Test (minimal features)
- ⚠️ Watch for memory usage reduction
- ⚠️ Watch for initialization time improvement

---

## Notes

- **Medium risk:** Touches core storage initialization
- **Cannot parallelize:** Depends on Issue #8
- **Blocks Issue #10:** Manifest/compaction removal needs this done first
- **Good milestone:** After this, core is truly read-only

---

## Completion Checklist

- [ ] Issue #8 merged
- [ ] Branch created
- [ ] Usage analysis complete
- [ ] Files deleted
- [ ] storage/mod.rs updated
- [ ] Write methods updated
- [ ] Tests updated
- [ ] M1 build succeeds
- [ ] M1 tests pass
- [ ] Full build succeeds
- [ ] No WAL/MemTable references remain
- [ ] PR created
- [ ] Senior review
- [ ] CI green
- [ ] Merged
- [ ] Tag Team C for Issue #10


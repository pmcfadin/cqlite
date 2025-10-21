# Issue #2: Delete OptimizedExecutor (Dead Code)

**Priority:** P0  
**Risk Level:** Zero (Never Used)  
**Estimated Time:** 1 hour  
**Assignee:** Team A  
**Branch:** `cleanup/issue-2-delete-optimized-executor`  
**Can Parallelize:** ✅ Yes (with Issues #3, #4, #5)

---

## Objective

Remove 1,045 lines of dead code that is never instantiated or called.

---

## Problem Statement

`cqlite-core/src/query/optimized_executor.rs` is a 1,045-line file containing:
- Query result caching with TTL
- Parallel query execution
- Query plan optimization
- Batch query processing

**Evidence it's dead:**
```bash
$ grep -r "OptimizedExecutor::new" cqlite-core/src/
# NO MATCHES (except in the file itself)
```

The actual query execution uses `SelectExecutor` (different file). This is an orphaned implementation that was never wired up.

---

## Files to Delete

```
cqlite-core/src/query/optimized_executor.rs  (1,045 lines)
```

---

## Files to Modify

### 1. `cqlite-core/src/query/mod.rs`

**Remove:**
```rust
// Line ~15-20 (find exact lines)
pub mod optimized_executor;
pub use optimized_executor::OptimizedExecutor;
```

**Verify no other exports reference it.**

---

## Step-by-Step Instructions

### Step 1: Create Branch

```bash
git checkout -b cleanup/issue-2-delete-optimized-executor
```

### Step 2: Verify File is Unused

```bash
# Search for any imports
grep -r "use.*optimized_executor" cqlite-core/src/
grep -r "OptimizedExecutor" cqlite-core/src/ | grep -v "optimized_executor.rs"

# Expected: No matches outside the file itself
```

### Step 3: Delete the File

```bash
git rm cqlite-core/src/query/optimized_executor.rs
```

### Step 4: Remove Module Declaration

Edit `cqlite-core/src/query/mod.rs`:

```rust
// DELETE these lines:
pub mod optimized_executor;
// ... and any re-exports like:
pub use optimized_executor::OptimizedExecutor;
```

### Step 5: Verify Compilation

```bash
cd cqlite-core
cargo build --all-features
cargo test --all-features
```

**Expected:** Compiles cleanly, all tests pass.

### Step 6: Run Cleanup Validation

```bash
./scripts/validate-cleanup.sh
```

### Step 7: Check for Unused Warnings

```bash
cargo clippy --all-targets --all-features 2>&1 | tee clippy.log
# Should not introduce new warnings
```

---

## Testing Checklist

- [ ] Verify file is not imported anywhere: `grep -r "OptimizedExecutor" cqlite-core/src/`
- [ ] Delete file: `git rm cqlite-core/src/query/optimized_executor.rs`
- [ ] Remove from mod.rs
- [ ] `cargo build --all-features` - success
- [ ] `cargo test --all-features` - all pass
- [ ] `./scripts/validate-cleanup.sh` - success
- [ ] No new clippy warnings
- [ ] Commit with message: "Remove OptimizedExecutor (dead code, never instantiated)"

---

## Verification Commands

```bash
# Before deletion - verify it's unused
rg "OptimizedExecutor" cqlite-core/src/ --type rust

# After deletion - verify clean build
cargo clean
cargo build --all-features
cargo test --all-features

# Check binary size reduction (optional)
cargo build --release
ls -lh target/release/libcqlite_core.*
```

---

## Expected Impact

- **Lines Removed:** 1,045
- **Build Time:** Slightly faster (less to compile)
- **Binary Size:** Slightly smaller
- **Test Coverage:** No change (file had no tests using it)
- **Breaking Changes:** None (code was never used)

---

## Success Criteria

✅ File deleted  
✅ Module declaration removed  
✅ `cargo build --all-features` succeeds  
✅ `cargo test --all-features` passes (same number of tests)  
✅ No new compiler warnings  
✅ CI passes (all jobs green)  

---

## Dependencies

**Requires:** Issue #1 (Safety Nets) complete  
**Blocks:** None  
**Can Parallelize With:** Issues #3, #4, #5, #6, #7

---

## Rollback Plan

```bash
# If anything breaks (unlikely):
git revert <commit-hash>
git push origin main

# Or restore from main:
git checkout main -- cqlite-core/src/query/optimized_executor.rs
git checkout main -- cqlite-core/src/query/mod.rs
git commit -m "Rollback: Restore OptimizedExecutor"
```

---

## CI Checks to Monitor

- ✅ `cargo build` - Should pass
- ✅ `cargo test` - Should pass with same test count
- ✅ `cargo clippy` - Should not introduce warnings
- ✅ Minimal features build - Should pass

---

## Notes

- **Zero risk deletion** - file is provably unused
- This is the easiest cleanup task - good for new team members
- Can be completed in < 1 hour
- Good first PR for demonstrating cleanup process

---

## Completion Checklist

- [ ] Branch created
- [ ] File verified as unused
- [ ] File deleted
- [ ] Module exports removed
- [ ] Local build successful
- [ ] Local tests pass
- [ ] Validation script passes
- [ ] Commit pushed
- [ ] PR created
- [ ] CI green
- [ ] PR approved and merged
- [ ] Branch deleted


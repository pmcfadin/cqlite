# Remaining Issue Templates (Quick Reference)

These issues follow the same format as Issues #1-#4, #8-#9. Create full issues as needed.

---

## Issue #5: Move Docker Integration to Tests

**Files to Move:**
- `cqlite-core/src/docker/mod.rs` → `tests/helpers/docker.rs`

**Risk:** Low (only used in tests)  
**Time:** 2 hours  
**Can Parallelize:** ✅ Yes

---

## Issue #6: Feature-Gate Benchmarks

**Changes:**
- Remove `benchmarks` from `default` features in Cargo.toml
- Verify `#[cfg(feature = "benchmarks")]` on all benchmark files
- Update CI to not build benchmarks by default

**Risk:** Zero  
**Time:** 1 hour  
**Can Parallelize:** ✅ Yes

---

## Issue #7: Feature-Gate Tombstone Merger

**Files:**
- `cqlite-core/src/storage/sstable/tombstone_merger.rs`

**Changes:**
- Add `#[cfg(feature = "tombstones")]` to module
- Feature-gate any imports
- Update Cargo.toml to not include `tombstones` in default

**Risk:** Low  
**Time:** 2 hours  
**Can Parallelize:** ✅ Yes

---

## Issue #10: Remove Compaction, Manifest, and Batch Writer

**Files to Delete:**
- `cqlite-core/src/storage/compaction.rs` (457 lines)
- `cqlite-core/src/storage/manifest.rs` (388 lines)
- `cqlite-core/src/storage/batch_writer.rs` (543 lines)
- `cqlite-core/src/storage/sstable/writer.rs` (959 lines)

**Total:** 2,347 lines

**Changes to storage/mod.rs:**
- Remove from struct
- Remove from constructor
- Remove module declarations

**Risk:** Medium  
**Time:** 4 hours  
**Dependencies:** Must complete Issue #8, #9 first  
**Can Parallelize:** ❌ No

---

## Issue #11: Simplify SelectOptimizer

**File:** `cqlite-core/src/query/select_optimizer.rs` (681 lines → ~200 lines)

**Remove:**
- Cost estimation logic (lines ~220-250)
- Statistics gathering (lines ~450-500)
- Parallelization planning (lines ~347-428)
- Index selection (lines ~504-558)

**Keep:**
- Table extraction
- Basic predicate handling
- LIMIT processing

**Risk:** Medium (used in query path)  
**Time:** 6 hours  
**Dependencies:** Complete Issues #8-#10 first  
**Can Parallelize:** ❌ No

---

## Issue #12: Update Feature Defaults

**File:** `cqlite-core/Cargo.toml`

**Change:**
```toml
# Before:
default = ["all-compression", "metrics", "experimental", "state_machine"]

# After:
default = ["all-compression", "state_machine"]
```

**Remove from default:**
- `experimental`
- `metrics` (if not needed)
- `benchmarks` (already done in #6)

**Risk:** Zero (users can still opt-in)  
**Time:** 1 hour  
**Dependencies:** All other issues complete  
**Can Parallelize:** ✅ Yes (after #11)

---

## Issue #13: Final Validation Suite

**Tasks:**
1. Run full CI suite
2. Generate before/after metrics:
   - Line count comparison
   - Binary size comparison
   - Build time comparison
   - Memory usage comparison
3. Update documentation:
   - Feature flag guide
   - Migration guide for users
   - Roadmap showing M1 (done) → M5 (writes)
4. Tag release: `v0.2.0-m1-cleanup`

**Risk:** None (validation only)  
**Time:** 2 hours  
**Dependencies:** All issues complete  
**Can Parallelize:** N/A

---

## Issue Creation Script

To create full issues from these templates:

```bash
# Copy format from issue-02 (dead code deletion)
cp cleanup-issues/issue-02-delete-optimized-executor.md \
   cleanup-issues/issue-05-move-docker.md

# Edit and replace:
# - Title
# - File paths  
# - Line counts
# - Specific instructions

# Copy format from issue-08 (complex refactor)
cp cleanup-issues/issue-08-feature-gate-write-methods.md \
   cleanup-issues/issue-10-remove-compaction-manifest.md

# Edit for specific changes
```

---

## Quick Stats

| Issue | Type | Lines Removed | Risk | Time |
|-------|------|---------------|------|------|
| #5 | Move | ~260 | Low | 2h |
| #6 | Gate | ~0 (config) | Zero | 1h |
| #7 | Gate | ~0 (config) | Low | 2h |
| #10 | Delete | ~2,350 | Medium | 4h |
| #11 | Simplify | ~480 | Medium | 6h |
| #12 | Config | ~0 | Zero | 1h |
| #13 | Validate | ~0 | None | 2h |

**Total for remaining:** ~18 hours, ~3,090 lines removed


#!/bin/bash
set -e

# Script to create all cleanup GitHub issues
# Requires: gh CLI tool (brew install gh)

echo "🚀 Creating CQLite Core Cleanup GitHub Issues"
echo ""

# Check if gh is installed
if ! command -v gh &> /dev/null; then
    echo "❌ Error: GitHub CLI (gh) is not installed"
    echo "Install with: brew install gh"
    echo "Then authenticate: gh auth login"
    exit 1
fi

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo "❌ Error: Not authenticated with GitHub"
    echo "Run: gh auth login"
    exit 1
fi

echo "✅ GitHub CLI authenticated"
echo ""

# Function to create an issue
create_issue() {
    local issue_num=$1
    local title="$2"
    local body_file="$3"
    local labels="$4"
    local priority="$5"
    
    echo "Creating Issue #${issue_num}: ${title}"
    
    # Create the issue and capture the URL
    issue_url=$(gh issue create \
        --title "${title}" \
        --body-file "${body_file}" \
        --label "${labels}" \
        2>&1 | grep -o 'https://.*')
    
    if [ $? -eq 0 ]; then
        echo "✅ Created: ${issue_url}"
        echo "${issue_num}|${issue_url}" >> .issue-urls.txt
    else
        echo "❌ Failed to create issue #${issue_num}"
        return 1
    fi
    
    echo ""
}

# Clear previous URLs file
> .issue-urls.txt

echo "📝 Creating issues..."
echo ""

# Issue #1: Setup Safety Nets (CRITICAL - Must be first)
create_issue 1 \
    "Setup Safety Nets for Code Cleanup" \
    "issue-01-setup-safety-nets.md" \
    "cleanup,ci,infrastructure,P0" \
    "P0"

# Issue #2: Delete OptimizedExecutor
create_issue 2 \
    "Delete OptimizedExecutor (Dead Code)" \
    "issue-02-delete-optimized-executor.md" \
    "cleanup,dead-code,P0" \
    "P0"

# Issue #3: Delete PerformanceMonitor
create_issue 3 \
    "Delete PerformanceMonitor (Dead Code)" \
    "issue-03-delete-performance-monitor.md" \
    "cleanup,dead-code,P0" \
    "P0"

# Issue #4: Delete Parser Performance Code
create_issue 4 \
    "Delete Parser Performance Code (Dead Code)" \
    "issue-04-delete-parser-perf-code.md" \
    "cleanup,dead-code,P0" \
    "P0"

# Issue #5: Move Docker to Tests
cat > issue-05-move-docker.md << 'EOF'
# Issue #5: Move Docker Integration to Tests

**Priority:** P0  
**Risk Level:** Low  
**Estimated Time:** 2 hours  
**Branch:** `cleanup/issue-5-move-docker`  
**Can Parallelize:** ✅ Yes

## Objective

Move Docker integration module from core library to test utilities.

## Problem

`cqlite-core/src/docker/mod.rs` (262 lines) is test infrastructure that shouldn't be in the core library.

## Files to Move

- From: `cqlite-core/src/docker/mod.rs`
- To: `tests/helpers/docker.rs` or `cqlite-testing` crate

## Changes Required

1. Create `tests/helpers/` directory if needed
2. Move docker.rs to new location
3. Update any test imports
4. Remove from `cqlite-core/src/lib.rs`

## Success Criteria

- [ ] Docker code moved to tests
- [ ] All tests still pass
- [ ] Core library doesn't depend on Docker
- [ ] CI green

## Dependencies

**Requires:** Issue #1 complete
**Can Parallelize With:** Issues #2, #3, #4, #6, #7
EOF

create_issue 5 \
    "Move Docker Integration to Tests" \
    "issue-05-move-docker.md" \
    "cleanup,refactor,P0" \
    "P0"

# Issue #6: Feature-Gate Benchmarks
cat > issue-06-feature-gate-benchmarks.md << 'EOF'
# Issue #6: Feature-Gate Benchmarks

**Priority:** P0  
**Risk Level:** Zero  
**Estimated Time:** 1 hour  
**Branch:** `cleanup/issue-6-feature-gate-benchmarks`  
**Can Parallelize:** ✅ Yes

## Objective

Remove `benchmarks` from default features so they don't compile unless explicitly requested.

## Changes Required

### cqlite-core/Cargo.toml

```toml
# Before:
default = ["all-compression", "metrics", "experimental", "state_machine"]

# After:
default = ["all-compression", "state_machine"]
# Note: Remove "benchmarks" if present
```

## Verification

```bash
# Should NOT compile benchmarks
cargo build

# Should compile benchmarks
cargo build --features=benchmarks
```

## Success Criteria

- [ ] `benchmarks` not in default features
- [ ] Default build faster (doesn't compile benchmark code)
- [ ] `cargo build --features=benchmarks` still works
- [ ] CI updated to explicitly enable benchmarks when testing them

## Dependencies

**Requires:** Issue #1 complete
**Can Parallelize With:** All other Phase 1/2 issues
EOF

create_issue 6 \
    "Feature-Gate Benchmarks" \
    "issue-06-feature-gate-benchmarks.md" \
    "cleanup,configuration,P0" \
    "P0"

# Issue #7: Feature-Gate Tombstone Merger
cat > issue-07-feature-gate-tombstones.md << 'EOF'
# Issue #7: Feature-Gate Tombstone Merger

**Priority:** P1  
**Risk Level:** Low  
**Estimated Time:** 2 hours  
**Branch:** `cleanup/issue-7-feature-gate-tombstones`  
**Can Parallelize:** ✅ Yes

## Objective

Feature-gate tombstone merger module (M3+ feature) so it's not compiled for M1/M2.

## Files to Modify

### cqlite-core/src/storage/sstable/tombstone_merger.rs

Add at top of file:
```rust
#![cfg(feature = "tombstones")]
```

### Any files importing tombstone_merger

Wrap imports:
```rust
#[cfg(feature = "tombstones")]
use crate::storage::sstable::tombstone_merger::TombstoneMerger;
```

### cqlite-core/Cargo.toml

Ensure `tombstones` feature exists and is NOT in default:
```toml
tombstones = []  # M3+ tombstone/GC logic
```

## Verification

```bash
# Should build without tombstone code
cargo build --no-default-features --features=all-compression

# Should build with tombstones
cargo build --features=tombstones
```

## Success Criteria

- [ ] Module gated behind `tombstones` feature
- [ ] Not in default features
- [ ] M1-only build succeeds
- [ ] Full feature build succeeds

## Dependencies

**Requires:** Issue #1 complete
**Can Parallelize With:** All Phase 1/2 issues
EOF

create_issue 7 \
    "Feature-Gate Tombstone Merger" \
    "issue-07-feature-gate-tombstones.md" \
    "cleanup,feature-gate,P1" \
    "P1"

# Issue #8: Feature-Gate Write Methods (already created)
create_issue 8 \
    "Feature-Gate Write Methods" \
    "issue-08-feature-gate-write-methods.md" \
    "cleanup,architecture,P1" \
    "P1"

# Issue #9: Remove WAL and MemTable (already created)
create_issue 9 \
    "Remove WAL and MemTable" \
    "issue-09-remove-wal-memtable.md" \
    "cleanup,architecture,P1" \
    "P1"

# Issue #10: Remove Compaction and Manifest
cat > issue-10-remove-compaction-manifest.md << 'EOF'
# Issue #10: Remove Compaction, Manifest, Batch Writer, and SSTable Writer

**Priority:** P1  
**Risk Level:** Medium  
**Estimated Time:** 4 hours  
**Branch:** `cleanup/issue-10-remove-write-infrastructure`  
**Can Parallelize:** ❌ No (Must follow #8, #9)

## Objective

Remove remaining write infrastructure: compaction, manifest, batch writer, and SSTable writer.

## Files to Delete

- `cqlite-core/src/storage/compaction.rs` (457 lines)
- `cqlite-core/src/storage/manifest.rs` (388 lines)
- `cqlite-core/src/storage/batch_writer.rs` (543 lines)
- `cqlite-core/src/storage/sstable/writer.rs` (959 lines)

**Total:** 2,347 lines

## Changes to storage/mod.rs

Remove from `StorageEngine` struct:
- `compaction: Arc<compaction::CompactionManager>`
- `manifest: Arc<manifest::Manifest>`
- `batch_writer: Option<BatchWriter>`

Remove module declarations and imports.

## Success Criteria

- [ ] All write infrastructure files deleted
- [ ] StorageEngine is read-only
- [ ] M1 build succeeds
- [ ] M1 tests pass
- [ ] No write infrastructure remains

## Dependencies

**Requires:** Issues #8, #9 complete
**Blocks:** Issue #11
EOF

create_issue 10 \
    "Remove Compaction, Manifest, and Writers" \
    "issue-10-remove-compaction-manifest.md" \
    "cleanup,architecture,P1" \
    "P1"

# Issue #11: Simplify SelectOptimizer
cat > issue-11-simplify-select-optimizer.md << 'EOF'
# Issue #11: Simplify SelectOptimizer

**Priority:** P1  
**Risk Level:** Medium  
**Estimated Time:** 6 hours  
**Branch:** `cleanup/issue-11-simplify-optimizer`  
**Can Parallelize:** ❌ No (After #10)

## Objective

Simplify SelectOptimizer from 681 lines to ~200 lines by removing premature optimization.

## File to Modify

`cqlite-core/src/query/select_optimizer.rs`

## Remove

- Cost estimation logic (lines ~220-250)
- Statistics gathering (lines ~450-500)  
- Parallelization planning (lines ~347-428)
- Index selection (lines ~504-558)

## Keep

- Table extraction
- Basic predicate handling
- LIMIT processing
- Aggregation planning (basic)

## Success Criteria

- [ ] File reduced to ~200 lines
- [ ] Core query functionality preserved
- [ ] All SELECT tests pass
- [ ] No over-engineered optimization code remains

## Dependencies

**Requires:** Issues #8, #9, #10 complete
**Blocks:** Issue #12

## Notes

This is the most complex issue. Can be deferred if needed to ship M2 faster.
EOF

create_issue 11 \
    "Simplify SelectOptimizer" \
    "issue-11-simplify-select-optimizer.md" \
    "cleanup,optimization,P1" \
    "P1"

# Issue #12: Update Feature Defaults
cat > issue-12-update-feature-defaults.md << 'EOF'
# Issue #12: Update Feature Defaults

**Priority:** P0  
**Risk Level:** Zero  
**Estimated Time:** 1 hour  
**Branch:** `cleanup/issue-12-feature-defaults`  
**Can Parallelize:** ✅ Yes (after #11)

## Objective

Update Cargo.toml default features to reflect M1/M2 scope only.

## Changes

### cqlite-core/Cargo.toml

```toml
# Before:
default = ["all-compression", "metrics", "experimental", "state_machine"]

# After (M1/M2 only):
default = ["all-compression", "state_machine"]
```

## Documentation

Add to README:

```markdown
## Feature Flags

- `all-compression` - All compression codecs (LZ4, Snappy, Deflate, Zstd)
- `state_machine` - Query engine (required for M2 CLI)
- `experimental` - Write support (M5, unstable)
- `benchmarks` - Performance benchmarks (development only)
- `tombstones` - Tombstone merging (M3+)
```

## Success Criteria

- [ ] Default features = M1/M2 only
- [ ] Documentation updated
- [ ] Users can still opt-in to experimental features

## Dependencies

**Requires:** All other issues complete (or at least #1-#10)
EOF

create_issue 12 \
    "Update Feature Defaults" \
    "issue-12-update-feature-defaults.md" \
    "cleanup,configuration,P0" \
    "P0"

# Issue #13: Final Validation
cat > issue-13-final-validation.md << 'EOF'
# Issue #13: Final Validation Suite

**Priority:** P0  
**Risk Level:** None  
**Estimated Time:** 2 hours  
**Branch:** `cleanup/issue-13-validation`  

## Objective

Validate cleanup completion and capture metrics.

## Tasks

### 1. Generate Metrics Comparison

```bash
# Line count
echo "Before: ~37,000 lines"
echo "After: $(find cqlite-core/src -name "*.rs" | xargs wc -l | tail -1)"

# Binary size
cargo build --release --no-default-features --features=all-compression
ls -lh target/release/libcqlite_core.*

# Build time
time cargo build --release
```

### 2. Verify All Feature Combinations

```bash
# M1 only
cargo test --no-default-features --features=all-compression

# M1 + M2
cargo test --no-default-features --features=all-compression,state_machine

# Full (with experimental)
cargo test --all-features
```

### 3. Update Documentation

- [ ] README with feature flags
- [ ] CHANGELOG with cleanup summary
- [ ] Migration guide for users
- [ ] Roadmap showing M1 done → M5 next

### 4. Tag Release

```bash
git tag -a v0.2.0-m1-cleanup -m "Complete M1/M2 scope cleanup"
git push origin v0.2.0-m1-cleanup
```

## Success Criteria

- [ ] All previous issues merged
- [ ] Metrics captured and documented
- [ ] All feature combinations tested
- [ ] Documentation complete
- [ ] Release tagged

## Dependencies

**Requires:** All issues #1-#12 complete
EOF

create_issue 13 \
    "Final Validation Suite" \
    "issue-13-final-validation.md" \
    "cleanup,validation,P0" \
    "P0"

echo ""
echo "✅ All issues created!"
echo ""
echo "📋 Issue URLs saved to: .issue-urls.txt"
echo ""
echo "Next steps:"
echo "1. Review issues on GitHub"
echo "2. Assign to teams"
echo "3. Start with Issue #1 (Setup Safety Nets)"
echo "4. Once #1 is complete, teams can work on #2-#7 in parallel"
echo ""
echo "View all issues: gh issue list --label cleanup"


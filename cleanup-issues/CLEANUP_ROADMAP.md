# CQLite Core Cleanup Roadmap

**Objective:** Remove ~10,000 lines of out-of-scope code while maintaining CI green status

**Total Issues:** 12  
**Estimated Timeline:** 2-3 weeks with parallel teams  
**Risk Level:** Low (if done in order)

---

## Issue Dependency Graph

```
Phase 0: Preparation
└── Issue #1: Setup Safety Nets

Phase 1: Delete Dead Code (Zero Dependencies - Can Run in Parallel)
├── Issue #2: Delete OptimizedExecutor (P0, Zero Risk)
├── Issue #3: Delete PerformanceMonitor (P0, Zero Risk)
├── Issue #4: Delete Parser Performance Code (P0, Zero Risk)
└── Issue #5: Move Docker Integration to Tests (P0, Low Risk)

Phase 2: Feature Gating (Can Run in Parallel After Phase 1)
├── Issue #6: Feature-Gate Benchmarks (P0, Zero Risk)
└── Issue #7: Feature-Gate Tombstone Merger (P1, Low Risk)

Phase 3: Write Infrastructure Removal (Sequential - Medium Risk)
├── Issue #8: Extract Write Methods Behind Feature (P1, Medium Risk)
│   └── Blocks: Issue #9, #10
├── Issue #9: Remove WAL and MemTable (P1, Medium Risk)
│   └── Requires: Issue #8
└── Issue #10: Remove Compaction and Manifest (P1, Medium Risk)
    └── Requires: Issue #8, #9

Phase 4: Simplification (After Phase 3)
├── Issue #11: Simplify SelectOptimizer (P1, Medium Risk)
└── Issue #12: Update Feature Defaults (P0, Zero Risk)

Phase 5: Validation
└── Issue #13: Final CI Validation Suite
```

---

## Issue Summary Table

| Issue | Title | Priority | Risk | Can Parallelize? | Estimated Time |
|-------|-------|----------|------|------------------|----------------|
| #1 | Setup Safety Nets | P0 | None | N/A | 2 hours |
| #2 | Delete OptimizedExecutor | P0 | Zero | ✅ Yes (after #1) | 1 hour |
| #3 | Delete PerformanceMonitor | P0 | Zero | ✅ Yes (after #1) | 1 hour |
| #4 | Delete Parser Performance Code | P0 | Zero | ✅ Yes (after #1) | 1 hour |
| #5 | Move Docker to Tests | P0 | Low | ✅ Yes (after #1) | 2 hours |
| #6 | Feature-Gate Benchmarks | P0 | Zero | ✅ Yes (after #1) | 1 hour |
| #7 | Feature-Gate Tombstone Merger | P1 | Low | ✅ Yes (after #1) | 2 hours |
| #8 | Feature-Gate Write Methods | P1 | Medium | ❌ No | 4 hours |
| #9 | Remove WAL and MemTable | P1 | Medium | ❌ No (after #8) | 4 hours |
| #10 | Remove Compaction/Manifest | P1 | Medium | ❌ No (after #8,#9) | 4 hours |
| #11 | Simplify SelectOptimizer | P1 | Medium | ❌ No (after #10) | 6 hours |
| #12 | Update Feature Defaults | P0 | Zero | ✅ Yes (after #11) | 1 hour |
| #13 | Final Validation | P0 | None | N/A | 2 hours |

**Total Effort:** ~31 hours (can be reduced to ~1 week with 3 parallel teams)

---

## Team Allocation Strategy

### Team A: Dead Code Removal (Issues #2-#4)
- Low risk, high parallelism
- Can complete in 1 day
- No coordination needed

### Team B: Feature Gating (Issues #6-#7)
- Low risk, independent work
- Can complete in 1 day
- Parallel with Team A

### Team C: Infrastructure (Issues #5, #8-#10)
- Medium risk, sequential dependencies
- Requires careful coordination
- 3-4 days sequential work

### Integration Team: Simplification (Issues #11-#13)
- Requires all previous work complete
- Final validation and feature defaults
- 2-3 days

---

## Success Metrics

- [ ] All CI checks pass (existing + new minimal feature tests)
- [ ] Code coverage maintained or improved
- [ ] No new compiler warnings
- [ ] Crate size reduced by ~15-20%
- [ ] `cargo build --no-default-features` succeeds
- [ ] `cargo test --no-default-features --features=all-compression,state_machine` passes

---

## Rollback Strategy

Each issue includes:
1. Branch naming convention: `cleanup/issue-N-short-description`
2. PR with before/after CI comparison
3. Revert commit ready if needed
4. Clear testing checklist

If any issue breaks CI:
1. Revert the specific PR
2. Debug offline in feature branch
3. Re-submit with fixes

---

## Notes for Patrick

- **All issues are in `/cleanup-issues/` directory**
- **Start with Issue #1** - it sets up the safety infrastructure
- **Teams can work in parallel on Issues #2-#7** after Issue #1
- **Issues #8-#10 must be sequential** (write infrastructure is interconnected)
- **Issue #11 can be deferred** if you want to ship M2 faster (SelectOptimizer works, just over-engineered)
- **Each issue has rollback instructions** at the bottom

---

## Issue File Structure

```
cleanup-issues/
├── CLEANUP_ROADMAP.md (this file)
├── issue-01-setup-safety-nets.md
├── issue-02-delete-optimized-executor.md
├── issue-03-delete-performance-monitor.md
├── issue-04-delete-parser-perf-code.md
├── issue-05-move-docker-to-tests.md
├── issue-06-feature-gate-benchmarks.md
├── issue-07-feature-gate-tombstones.md
├── issue-08-feature-gate-write-methods.md
├── issue-09-remove-wal-memtable.md
├── issue-10-remove-compaction-manifest.md
├── issue-11-simplify-select-optimizer.md
├── issue-12-update-feature-defaults.md
└── issue-13-final-validation.md
```

---

**Next Step:** Review Issue #1 (Setup Safety Nets) and approve before teams start work.


# CQLite Core Cleanup: Executive Summary

**Date:** October 18, 2025  
**Prepared For:** Patrick & Development Teams  
**Objective:** Remove ~10,000 lines of out-of-scope code from cqlite-core while maintaining CI green status

---

## The Problem

After a thorough code review against PRD M1 (Core Reading Library) and M2 (CLI), we identified **~10,000 lines of code** (27% of codebase) that are out of scope:

- **Write infrastructure** (WAL, memtable, compaction, batch writers) → M5 scope
- **Performance optimization frameworks** → M6 scope  
- **Benchmarking code** → M6 scope
- **Dead code** (never called)
- **Premature optimization** (over-engineered for M2 needs)

**Impact:** Bloated codebase, slower builds, architectural confusion about what's stable vs. experimental.

---

## The Solution: Phased, Safe Cleanup

**13 GitHub issues** organized into 5 phases, designed for parallel team execution while maintaining CI green.

### Phase Breakdown

| Phase | Issues | Duration | Risk | Parallelizable? |
|-------|--------|----------|------|-----------------|
| **0: Preparation** | #1 | 2 hours | None | N/A |
| **1: Dead Code** | #2-#5 | 1 day | Zero | ✅ Yes (4 teams) |
| **2: Feature Gating** | #6-#7 | 1 day | Low | ✅ Yes (2 teams) |
| **3: Write Infrastructure** | #8-#10 | 1 week | Medium | ❌ No (sequential) |
| **4: Simplification** | #11-#12 | 2-3 days | Medium | ❌ No |
| **5: Validation** | #13 | 2 hours | None | N/A |

**Total Timeline:** 2-3 weeks (with parallel execution)  
**Sequential Timeline:** 4-5 weeks (if done by one person)

---

## Team Allocation

### Team A: Dead Code Removal (Issues #2-#4)
**Members:** 2-3 developers  
**Skills:** Junior-friendly, good for onboarding  
**Duration:** 1 day  
**Deliverables:**
- Delete `OptimizedExecutor` (1,045 lines)
- Delete `PerformanceMonitor` (596 lines)
- Delete parser performance code (2,107 lines)

**Total:** ~3,750 lines removed

### Team B: Feature Gating (Issues #5-#7)
**Members:** 2 developers  
**Skills:** Intermediate, Cargo.toml knowledge  
**Duration:** 1 day  
**Deliverables:**
- Move Docker integration to tests
- Feature-gate benchmarks
- Feature-gate tombstone merger

### Team C: Infrastructure Removal (Issues #8-#10)
**Members:** 2-3 senior developers  
**Skills:** Advanced, storage architecture knowledge  
**Duration:** 1 week (sequential)  
**Deliverables:**
- Feature-gate write methods
- Remove WAL and MemTable (770 lines)
- Remove compaction, manifest, writers (2,347 lines)

**Total:** ~3,100 lines removed

### Integration Team: Final Cleanup (Issues #11-#13)
**Members:** 1-2 senior developers  
**Skills:** Query engine knowledge, documentation  
**Duration:** 2-3 days  
**Deliverables:**
- Simplify SelectOptimizer (~480 lines saved)
- Update feature defaults
- Final validation and metrics

---

## Risk Mitigation

### Zero-Risk Issues (Can Start Immediately)
- #2: Delete OptimizedExecutor
- #3: Delete PerformanceMonitor  
- #4: Delete Parser Performance Code
- #6: Feature-Gate Benchmarks

**Why zero risk?** Code is provably unused (never called).

### Low-Risk Issues (Minimal Impact)
- #5: Move Docker to Tests
- #7: Feature-Gate Tombstones

**Why low risk?** Only affects test infrastructure or optional features.

### Medium-Risk Issues (Require Careful Review)
- #8: Feature-Gate Write Methods
- #9: Remove WAL/MemTable
- #10: Remove Compaction/Manifest
- #11: Simplify SelectOptimizer

**Mitigation:**
- Thorough code review required
- Senior engineer approval
- Comprehensive testing (M1 features only + full features)
- Rollback plan documented for each issue

---

## Safety Infrastructure (Issue #1)

Before any deletions, Issue #1 establishes:

1. **Minimal Features CI Job**
   - Validates M1-only builds (no write infrastructure)
   - Catches feature gate violations

2. **Coverage Baseline**
   - Tracks coverage before/after
   - Ensures no regression

3. **Validation Script**
   - Automated checks for each PR
   - Binary size, warning count, test count

4. **Baseline Metrics**
   - Documents current state
   - Enables before/after comparison

**All subsequent issues depend on #1 being complete first.**

---

## Expected Outcomes

### Quantitative

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total Lines | ~37,000 | ~27,000 | -27% |
| Files | ~150 | ~140 | -7% |
| Build Time | X seconds | X * 0.85 | -15% est. |
| Binary Size (M1) | Y MB | Y * 0.8 | -20% est. |
| Compilation Units | Z | Z - 10 | Faster incremental |

### Qualitative

- ✅ **Clearer architecture:** Read vs. Write separation obvious
- ✅ **Faster onboarding:** Less code to understand
- ✅ **Faster iteration:** Quicker builds, faster tests
- ✅ **Better PRD alignment:** Code matches stated scope
- ✅ **Maintainability:** Less code to maintain

---

## Success Criteria (Issue #13)

Project is complete when:

- [ ] All 13 issues merged
- [ ] CI fully green (all jobs passing)
- [ ] M1-only build: `cargo build --no-default-features --features=all-compression` succeeds
- [ ] M1-only tests: `cargo test --no-default-features --features=all-compression` passes
- [ ] Full feature build still works: `cargo build --all-features`
- [ ] No new compiler warnings introduced
- [ ] Code coverage maintained or improved
- [ ] Documentation updated (feature flags, migration guide)
- [ ] Metrics captured (before/after comparison)
- [ ] Release tagged: `v0.2.0-m1-cleanup`

---

## Communication Plan

### Week 0: Kickoff
- **Monday:** Review this summary with all teams
- **Tuesday:** Complete Issue #1 (Safety Nets)
- **Wednesday:** Distribute issues to teams
- **Thursday:** Teams start parallel work (#2-#7)
- **Friday:** First PRs submitted for review

### Week 1-2: Parallel Execution
- **Teams A & B:** Work on Issues #2-#7 in parallel
- **Team C:** Starts sequential work on #8-#10 after first PRs merge
- **Daily standups:** Report progress, blockers
- **Code reviews:** Senior engineers approve medium-risk changes

### Week 2-3: Integration
- **Integration Team:** Issues #11-#13
- **All Teams:** Final validation, documentation updates
- **Release:** Tag and announce cleanup completion

---

## Rollback Strategy

Each issue includes:
1. **Explicit rollback commands** in the issue description
2. **Branch naming convention:** `cleanup/issue-N-short-name`
3. **PR comparison:** Before/after CI metrics
4. **Revert commit:** Ready to apply if something breaks

**If any PR breaks CI:**
```bash
git revert <commit-hash>
git push origin main
# Debug offline, resubmit with fixes
```

---

## Questions & Answers

### Q: Can we skip any issues?

**A:** Issue #11 (Simplify SelectOptimizer) could be deferred. The optimizer works, it's just over-engineered. All others should be done.

### Q: What if we need write support before M5?

**A:** Keep the `experimental` feature flag. Code will be accessible with `features = ["experimental"]` but unsupported.

### Q: How do we coordinate Issue #8-#10 dependencies?

**A:** Team C owns all three. Issue #8 must merge before #9. Issue #9 must merge before #10. Use branch dependencies.

### Q: What if a user depends on removed code?

**A:** 
1. Check crates.io dependents (likely zero for experimental features)
2. Document breaking changes in CHANGELOG
3. Provide migration guide
4. Bump minor version (0.2.0)

### Q: Can we do this faster?

**A:** Yes, if all teams work in parallel:
- Week 1: Issues #1-#7 complete
- Week 2: Issues #8-#10 complete
- Week 3: Issues #11-#13 complete

Aggressive timeline: **2 weeks** with 6+ developers.

---

## Issue Directory Structure

```
cleanup-issues/
├── CLEANUP_ROADMAP.md          ← Dependency graph, overview
├── EXECUTIVE_SUMMARY.md         ← This file
├── ISSUE_TEMPLATES.md           ← Quick templates for #5-#7, #10-#13
├── issue-01-setup-safety-nets.md      ← MUST DO FIRST
├── issue-02-delete-optimized-executor.md
├── issue-03-delete-performance-monitor.md
├── issue-04-delete-parser-perf-code.md
├── issue-08-feature-gate-write-methods.md
└── issue-09-remove-wal-memtable.md
```

**Next Steps:**
1. Review this summary
2. Approve Issue #1
3. Assign teams
4. Begin execution

---

## Approval & Sign-off

- [ ] Patrick reviewed and approved
- [ ] Team leads assigned
- [ ] Issue #1 completed and merged
- [ ] Kickoff meeting held
- [ ] Teams have access to issue files
- [ ] CI infrastructure ready

---

## Contact

**Questions?** Post in `#cqlite-cleanup` Slack channel

**Issue files:** `/Users/patrick/local_projects/cqlite/cleanup-issues/`

**Related reports:**
- `/Users/patrick/local_projects/cqlite/CQLITE_CORE_M1_M2_CODE_REVIEW.md`
- `/Users/patrick/local_projects/cqlite/WRITER_VIABILITY_ASSESSMENT.md`
- `/Users/patrick/local_projects/cqlite/OPTIMIZATION_CODE_AUDIT.md`

---

**Let's ship a clean M1.** 🚀


# Cleanup Team Assignments - Issues #169-173

**Date:** October 20, 2025  
**Phase:** 1 (Dead Code & Feature Gating)  
**Status:** Ready to assign after PR #180 merges

---

## Team Structure

You will launch **5 parallel Claude Code instances**, each handling one issue:

| Instance | Issue | Title | Risk | Est. Time |
|----------|-------|-------|------|-----------|
| **Claude A** | #169 | Delete OptimizedExecutor | Zero | 1 hour |
| **Claude B** | #170 | Delete PerformanceMonitor | Zero | 1 hour |
| **Claude C** | #171 | Delete Parser Performance Code | Zero | 1 hour |
| **Claude D** | #172 | Move Docker to Tests | Low | 2 hours |
| **Claude E** | #173 | Feature-Gate Benchmarks | Zero | 1 hour |

---

## Prompt for Each Claude Instance

Copy this prompt when starting each Claude Code instance:

```markdown
# Mission: CQLite Cleanup Issue #XXX

You are an autonomous cleanup agent executing Issue #XXX from the CQLite M1/M2 scope reduction.

**Your complete instructions:** Read `@.claude/agents/cleanup/cleanup-agent-prompt.md`

**Your specific issue:** Read `@cleanup-issues/issue-XX-*.md`

**Your workflow:**
1. Use **@core/planner** to analyze your issue
2. Use **@core/coder** and **@development/rust/rust-developer** to make changes
3. Use **@core/reviewer** and **@analysis/code-review/rust-code-reviewer** for self-review
4. Use **@core/tester** and **@testing/validation/production-validator** for validation
5. Use **@github/pr-manager** and **@devops/ci-cd/ops-cicd-github** for CI/merge

**Critical requirements:**
- ✅ All local validation MUST pass before pushing
- ✅ Run `./scripts/validate-cleanup.sh` successfully
- ✅ Clippy must pass with `-D warnings`
- ✅ Monitor CI and only merge when 100% green
- ✅ Clean up branch after merge

**Success criteria:**
- Issue closed ✅
- PR merged ✅
- Branch deleted ✅
- Main CI green ✅

**Working directory:** `/Users/patrick/local_projects/cqlite`

Begin by reading your full instructions and creating an execution plan with @core/planner.
```

---

## Specific Instance Prompts

### Claude A (Issue #169)

```markdown
# Mission: Delete OptimizedExecutor (Issue #169)

**Full instructions:** @.claude/agents/cleanup/cleanup-agent-prompt.md  
**Issue spec:** @cleanup-issues/issue-02-delete-optimized-executor.md

**Your target:**
- File: `cqlite-core/src/query/optimized_executor.rs` (1,045 lines)
- Status: Dead code, never instantiated
- Action: DELETE

**Key steps:**
1. Verify with: `grep -r "OptimizedExecutor::new" cqlite-core/src/`
2. Delete file: `git rm cqlite-core/src/query/optimized_executor.rs`
3. Update: `cqlite-core/src/query/mod.rs` (remove module declaration)
4. Validate: Zero references remain
5. Test: All validation passes

**Branch:** `cleanup/issue-169-delete-optimized-executor`

**Working directory:** `/Users/patrick/local_projects/cqlite`

Start with @core/planner to analyze the issue.
```

### Claude B (Issue #170)

```markdown
# Mission: Delete PerformanceMonitor (Issue #170)

**Full instructions:** @.claude/agents/cleanup/cleanup-agent-prompt.md  
**Issue spec:** @cleanup-issues/issue-03-delete-performance-monitor.md

**Your target:**
- File: `cqlite-core/src/performance_monitor.rs` (596 lines)
- Status: Dead code, only used in own tests
- Action: DELETE

**Key steps:**
1. Verify with: `grep -r "PerformanceMonitor::new" cqlite-core/src/`
2. Delete file: `git rm cqlite-core/src/performance_monitor.rs`
3. Update: `cqlite-core/src/lib.rs` (remove module declaration)
4. Validate: Zero references remain
5. Test: All validation passes

**Branch:** `cleanup/issue-170-delete-performance-monitor`

**Working directory:** `/Users/patrick/local_projects/cqlite`

Start with @core/planner to analyze the issue.
```

### Claude C (Issue #171)

```markdown
# Mission: Delete Parser Performance Code (Issue #171)

**Full instructions:** @.claude/agents/cleanup/cleanup-agent-prompt.md  
**Issue spec:** @cleanup-issues/issue-04-delete-parser-perf-code.md

**Your targets:**
- File 1: `cqlite-core/src/parser/m3_performance_benchmarks.rs` (1,285 lines)
- File 2: `cqlite-core/src/parser/performance_regression_framework.rs` (822 lines)
- Status: Dead code, never used
- Action: DELETE BOTH

**Key steps:**
1. Verify with: `grep -r "M3PerformanceBenchmarks\|PerformanceRegressionFramework" cqlite-core/src/`
2. Delete both files
3. Update: `cqlite-core/src/parser/mod.rs` (remove both module declarations)
4. Validate: Zero references remain
5. Test: All validation passes

**Branch:** `cleanup/issue-171-delete-parser-perf-code`

**Working directory:** `/Users/patrick/local_projects/cqlite`

Start with @core/planner to analyze the issue.
```

### Claude D (Issue #172)

```markdown
# Mission: Move Docker Integration to Tests (Issue #172)

**Full instructions:** @.claude/agents/cleanup/cleanup-agent-prompt.md  
**Issue spec:** Created inline (see cleanup-agent-prompt.md)

**Your target:**
- File: `cqlite-core/src/docker/mod.rs` (262 lines)
- Status: Test infrastructure in wrong location
- Action: MOVE (not delete!)

**Key steps:**
1. Create: `tests/helpers/` directory
2. Move file: `git mv cqlite-core/src/docker/mod.rs tests/helpers/docker.rs`
3. Update: `cqlite-core/src/lib.rs` (remove docker module)
4. Update: Test imports from `cqlite_core::docker` to `helpers::docker`
5. Validate: Tests still pass
6. Test: All validation passes

**Branch:** `cleanup/issue-172-move-docker-to-tests`

**Working directory:** `/Users/patrick/local_projects/cqlite`

**NOTE:** This is a MOVE operation, not a deletion. Be careful with imports.

Start with @core/planner to analyze the issue.
```

### Claude E (Issue #173)

```markdown
# Mission: Feature-Gate Benchmarks (Issue #173)

**Full instructions:** @.claude/agents/cleanup/cleanup-agent-prompt.md  
**Issue spec:** Created inline (see cleanup-agent-prompt.md)

**Your target:**
- File: `cqlite-core/Cargo.toml`
- Status: Benchmarks in default features (shouldn't be)
- Action: Remove "benchmarks" from default

**Key steps:**
1. Edit: `cqlite-core/Cargo.toml`
2. Find: `default = ["all-compression", ..., "benchmarks", ...]`
3. Change: Remove "benchmarks" from the list
4. Verify: `cargo build` does NOT compile benchmarks
5. Verify: `cargo build --features=benchmarks` DOES compile them
6. Test: All validation passes

**Branch:** `cleanup/issue-173-feature-gate-benchmarks`

**Working directory:** `/Users/patrick/local_projects/cqlite`

Start with @core/planner to analyze the issue.
```

---

## Launch Sequence

### Prerequisite
```bash
# Verify PR #180 is merged
gh pr view 180 --json state -q .state
# Should output: MERGED
```

### Step 1: Launch All Instances

Open 5 Claude Code windows and paste the appropriate prompt into each.

### Step 2: Monitor Progress

```bash
# Watch for new branches
watch -n 10 'gh pr list --label cleanup'

# Watch CI status
watch -n 30 'gh run list --branch main --limit 1'
```

### Step 3: Merge Order

Issues can merge in any order (they're independent). Each Claude will auto-merge when CI is green.

Expected timeline:
- **Hour 1:** All 5 PRs created
- **Hour 2:** First PRs (169, 170, 173) merge
- **Hour 2-3:** Remaining PRs (171, 172) merge
- **Hour 3:** All complete

### Step 4: Verify Completion

```bash
# Check all issues are closed
gh issue list --label cleanup --state open | grep -E "169|170|171|172|173"
# Should return nothing

# Check all PRs merged
gh pr list --label cleanup --state closed | grep -E "169|170|171|172|173"
# Should show 5 merged PRs

# Check main is clean
git log main --oneline -10
# Should show 5 merge commits
```

---

## Troubleshooting

### If a Claude Gets Stuck

```bash
# Identify which issue
gh pr list --label cleanup --state open

# Check PR status
gh pr view XXX

# Check CI
gh pr checks XXX

# If needed, take over manually
git checkout cleanup/issue-XXX
# Fix issue
git push
```

### If Merge Conflicts

Claude instances will automatically rebase if conflicts occur. If manual intervention needed:

```bash
git checkout cleanup/issue-XXX
git fetch origin
git rebase origin/main
# Resolve conflicts
git rebase --continue
git push --force-with-lease
```

### If CI Fails

Each Claude will investigate and fix. If it can't:

```bash
# Get logs
gh run view $(gh run list --branch cleanup/issue-XXX --limit 1 --json databaseId -q '.[0].databaseId')

# Common issues:
# - Feature gate wrong → Check Cargo.toml
# - Import broken → Check mod.rs updates
# - Test fails → Check if test imports deleted code

# Fix and push
# Claude will monitor and retry
```

---

## Expected Outcomes

### Metrics Before (from BASELINE_METRICS.md)
- Total lines: 101,143
- Source files: 159
- Clippy warnings: 10

### Metrics After (estimated)
- Total lines: ~97,400 (-3,743 lines)
- Source files: 154 (-5 files)
- Clippy warnings: ≤10 (same or better)

### Files Removed
- `cqlite-core/src/query/optimized_executor.rs` (1,045 lines)
- `cqlite-core/src/performance_monitor.rs` (596 lines)
- `cqlite-core/src/parser/m3_performance_benchmarks.rs` (1,285 lines)
- `cqlite-core/src/parser/performance_regression_framework.rs` (822 lines)
- `cqlite-core/src/docker/mod.rs` (moved, not removed)

### Feature Changes
- `benchmarks` no longer in default features
- Default build faster (doesn't compile benchmark code)

---

## Success Declaration

All issues complete when:

```bash
# All 5 issues closed
gh issue list --search "is:issue is:closed label:cleanup 169 170 171 172 173" | wc -l
# Should output: 5

# All 5 PRs merged
gh pr list --search "is:pr is:merged label:cleanup" | grep -E "169|170|171|172|173" | wc -l
# Should output: 5

# Main CI green
gh run list --branch main --limit 1 --json conclusion -q '.[0].conclusion'
# Should output: success

# No cleanup branches remain
git branch -r | grep cleanup/issue-1
# Should output nothing
```

---

## Communication

### Status Channel

Create a Slack/Discord thread: "#cleanup-sprint-oct-20"

Each Claude should post:
- ✅ When PR created
- ✅ When CI passes
- ✅ When merged
- ❌ If blocked (with details)

### Final Report

After all 5 complete, generate summary:

```bash
gh issue comment 168 --body "## 🎉 Phase 1 Complete (Issues #169-173)

**Status:** All 5 issues closed and merged

**PRs Merged:**
- #169: Delete OptimizedExecutor (1,045 lines)
- #170: Delete PerformanceMonitor (596 lines)
- #171: Delete Parser Performance Code (2,107 lines)
- #172: Move Docker to Tests (262 lines moved)
- #173: Feature-Gate Benchmarks

**Total Impact:**
- Lines removed: 3,748
- Files removed: 4
- Files moved: 1
- Build time: Reduced
- Binary size: Reduced

**Next Steps:**
Ready to proceed with Issue #174 (Feature-Gate Write Methods)

**Timeline:**
- Started: <timestamp>
- All PRs created: <timestamp>
- All PRs merged: <timestamp>
- Total duration: <hours>

Parallel execution successful! ✅"
```

---

## Ready to Launch? ✅

1. ✅ PR #180 merged (safety nets in place)
2. ✅ 5 issue specs ready
3. ✅ Prompt files created
4. ✅ Agent subagents identified
5. ✅ Monitoring commands prepared

**You're ready to assign the cleanup team!** 🚀


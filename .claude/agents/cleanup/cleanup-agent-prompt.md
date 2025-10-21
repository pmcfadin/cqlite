# CQLite Cleanup Agent - Issue Execution Prompt

**Purpose:** Execute cleanup issues #169-173 with full CI validation and automated merge  
**Agent Type:** Autonomous Cleanup Specialist  
**Risk Level:** P0 (Low Risk - Dead Code Removal)

---

## Your Mission

You are an autonomous cleanup agent tasked with executing **one specific cleanup issue** from the CQLite M1/M2 scope reduction effort. Your goal is to:

1. ✅ **Execute the issue completely** following the specification
2. ✅ **Validate all changes** (format, lint, compile, test)
3. ✅ **Pass local CI** before pushing
4. ✅ **Monitor remote CI** and merge when green
5. ✅ **Clean up** the branch after merge

---

## Issue Assignment

**You will be assigned ONE of these issues:**
- Issue #169: Delete OptimizedExecutor (Dead Code)
- Issue #170: Delete PerformanceMonitor (Dead Code)
- Issue #171: Delete Parser Performance Code (Dead Code)
- Issue #172: Move Docker Integration to Tests
- Issue #173: Feature-Gate Benchmarks

**Location of full specs:** `/Users/patrick/local_projects/cqlite/cleanup-issues/`

---

## Required Subagents

You MUST use these agents from `@agents/` at appropriate stages:

### 1. Planning & Analysis
- **@core/planner.md** - Understand the issue, create execution plan
- **@development/rust/rust-developer.md** - Rust-specific planning

### 2. Code Changes
- **@core/coder.md** - Execute file deletions and modifications
- **@development/rust/rust-developer.md** - Ensure Rust best practices

### 3. Code Review
- **@core/reviewer.md** - Self-review before committing
- **@analysis/code-review/rust-code-reviewer.md** - Rust-specific review

### 4. Testing & Validation
- **@core/tester.md** - Run all tests and validations
- **@testing/validation/production-validator.md** - Pre-push validation

### 5. CI/CD & Merge
- **@devops/ci-cd/ops-cicd-github.md** - Monitor CI, coordinate merge
- **@github/pr-manager.md** - Manage PR lifecycle

---

## Execution Workflow

### Phase 0: Setup & Planning

```bash
# Invoke @core/planner.md
```

**Tasks:**
1. Read your assigned issue from `cleanup-issues/issue-XX-*.md`
2. Read the related spec from `cleanup-issues/CLEANUP_ROADMAP.md`
3. Verify Issue #168 is merged (safety nets in place)
4. Create execution plan with checkpoints
5. Identify all files to change/delete

**Deliverable:** Execution plan with file list and verification steps

---

### Phase 1: Branch & Baseline

```bash
# Current directory: /Users/patrick/local_projects/cqlite
git checkout main
git pull origin main
git checkout -b cleanup/issue-XXX-short-description
```

**Tasks:**
1. Create branch following naming convention: `cleanup/issue-XXX-description`
2. Capture baseline metrics:
   ```bash
   find cqlite-core/src -name "*.rs" | xargs wc -l | tail -1
   cargo clippy --package cqlite-core --lib 2>&1 | grep -c "warning:"
   ```
3. Document baseline in commit message

---

### Phase 2: Execute Changes

```bash
# Invoke @core/coder.md and @development/rust/rust-developer.md
```

**For file deletions (Issues #169-171):**
```bash
# Verify file is unused
grep -r "ModuleName" cqlite-core/src/ | grep -v "file-to-delete.rs"

# Delete file
git rm cqlite-core/src/path/to/file.rs

# Update module declarations
# Edit parent mod.rs to remove:
# - pub mod module_name;
# - pub use module_name::*;
```

**For file moves (Issue #172):**
```bash
# Create destination directory
mkdir -p tests/helpers

# Move file
git mv cqlite-core/src/docker/mod.rs tests/helpers/docker.rs

# Update imports in test files
# Remove from cqlite-core/src/lib.rs
```

**For feature gating (Issue #173):**
```bash
# Edit cqlite-core/Cargo.toml
# Remove "benchmarks" from default = [...]

# Verify benchmarks module has feature gate
# Should see: #[cfg(feature = "benchmarks")]
```

---

### Phase 3: Local Validation (CRITICAL - Do NOT skip)

```bash
# Invoke @core/tester.md and @testing/validation/production-validator.md
```

**Step 3.1: Format**
```bash
cargo fmt --all
git add -u
git commit -m "style: cargo fmt" || echo "No formatting needed"
```

**Step 3.2: Clippy (Zero Warnings)**
```bash
cargo clippy --package cqlite-core --lib --all-features -- -D warnings

# MUST PASS with zero warnings
# If warnings found, fix them before proceeding
```

**Step 3.3: Build (Minimal Features)**
```bash
# M1 scope (reading only)
cargo build --package cqlite-core --no-default-features --features=all-compression

# MUST SUCCEED
```

**Step 3.4: Build (All Features)**
```bash
cargo build --package cqlite-core --all-features

# MUST SUCCEED
```

**Step 3.5: Tests (Library)**
```bash
# Run library tests
cargo test --package cqlite-core --lib --all-features

# MUST PASS - Note the test count for comparison
```

**Step 3.6: Validation Script**
```bash
./scripts/validate-cleanup.sh

# MUST PASS all checks:
# - Core reading tests
# - Clippy warnings ≤ 50
# - No unused imports
# - Release binary builds
```

**Step 3.7: Verify Removal**
```bash
# For deletions: Verify no references remain
rg "DeletedModule|deleted_function" cqlite-core/src/

# Should return NO MATCHES
```

---

### Phase 4: Commit & Push

```bash
# Invoke @core/reviewer.md for self-review
```

**Step 4.1: Self-Review**

Use **@analysis/code-review/rust-code-reviewer.md** to review:
- [ ] All deleted files verified as unused
- [ ] Module declarations updated
- [ ] No broken imports
- [ ] Clippy passes with -D warnings
- [ ] Tests pass
- [ ] Validation script passes

**Step 4.2: Commit**
```bash
git add -A

# Commit message format:
git commit -m "cleanup(issue-XXX): <Short description>

<Detailed description of changes>

- Deleted: <list files>
- Updated: <list files>
- Verified: No references remain

Tests:
- Clippy: PASS (0 warnings)
- Build (minimal): PASS
- Build (all): PASS
- Tests: PASS (XXX tests)
- Validation script: PASS

Baseline:
- Lines removed: XXX
- Files removed: X
- Warnings reduced: X → Y

Closes #XXX"
```

**Step 4.3: Push**
```bash
# Only push if ALL local validations passed
git push origin cleanup/issue-XXX-description
```

---

### Phase 5: Create PR

```bash
# Invoke @github/pr-manager.md
```

**Step 5.1: Create PR**
```bash
gh pr create \
  --title "cleanup(issue-XXX): <Short description>" \
  --body-file cleanup-issues/issue-XX-*.md \
  --base main \
  --head cleanup/issue-XXX-description
```

**Step 5.2: Capture PR Number**
```bash
PR_NUMBER=$(gh pr view --json number -q .number)
echo "Created PR #$PR_NUMBER"
```

---

### Phase 6: Monitor CI (Automated)

```bash
# Invoke @devops/ci-cd/ops-cicd-github.md
```

**Step 6.1: Wait for CI**
```bash
echo "Monitoring CI checks for PR #$PR_NUMBER..."

# Poll every 30 seconds
while true; do
  # Get check status
  STATUS=$(gh pr checks $PR_NUMBER --json state -q '.[].state' | sort -u)
  
  if echo "$STATUS" | grep -q "FAILURE"; then
    echo "❌ CI FAILED - Investigating..."
    gh pr checks $PR_NUMBER
    exit 1
  elif echo "$STATUS" | grep -q "PENDING"; then
    echo "⏳ CI running... (waiting 30s)"
    sleep 30
  elif echo "$STATUS" | grep -q "SUCCESS"; then
    echo "✅ All CI checks passed!"
    break
  fi
done
```

**Step 6.2: If CI Fails**
```bash
# Invoke @core/reviewer.md to analyze failure

# Get failure details
gh pr checks $PR_NUMBER

# Fix locally
git commit --amend # or new commit
git push --force-with-lease origin cleanup/issue-XXX

# Return to Step 6.1 (monitor again)
```

---

### Phase 7: Merge & Cleanup

```bash
# Invoke @github/pr-manager.md
```

**Step 7.1: Final Verification**
```bash
# Ensure all checks passed
gh pr checks $PR_NUMBER | grep -q "All checks were successful"

if [ $? -eq 0 ]; then
  echo "✅ Ready to merge"
else
  echo "❌ Not ready - checks not all green"
  exit 1
fi
```

**Step 7.2: Merge**
```bash
# Merge using squash (clean history)
gh pr merge $PR_NUMBER --squash --delete-branch --auto

echo "✅ PR #$PR_NUMBER merged and branch deleted"
```

**Step 7.3: Cleanup Local**
```bash
git checkout main
git pull origin main
git branch -D cleanup/issue-XXX-description

echo "✅ Local cleanup complete"
```

**Step 7.4: Verify Merge**
```bash
# Verify the changes are in main
git log --oneline -1
# Should show your merge commit

# Verify CI still green on main
gh run list --branch main --limit 1
```

---

## Success Criteria Checklist

Before considering the issue complete, verify:

### Pre-Push Validation
- [ ] **Format:** `cargo fmt` run and committed
- [ ] **Clippy:** Zero warnings with `-D warnings` flag
- [ ] **Build (minimal):** `--no-default-features --features=all-compression` succeeds
- [ ] **Build (all):** `--all-features` succeeds
- [ ] **Tests (lib):** All library tests pass
- [ ] **Validation script:** `./scripts/validate-cleanup.sh` passes
- [ ] **No references:** Deleted code has zero references remaining

### Post-Push Validation
- [ ] **PR created:** With proper title and body
- [ ] **CI triggered:** All jobs started
- [ ] **CI passing:** All checks green (10/10)
- [ ] **No review blockers:** No requested changes
- [ ] **Mergeable:** Branch is up to date

### Post-Merge Validation
- [ ] **Merged:** PR merged to main
- [ ] **Branch deleted:** Remote branch cleaned up
- [ ] **Local cleaned:** Local branch deleted
- [ ] **Main CI:** CI passing on main branch
- [ ] **Issue closed:** GitHub issue auto-closed

---

## Error Handling

### If Clippy Fails
```bash
# Get details
cargo clippy --package cqlite-core --lib --all-features

# Fix each warning
# Common issues:
# - Unused imports → Remove
# - Dead code warnings → Expected (you're deleting dead code)
# - Type issues → Check module exports

# Re-run validation after fixes
```

### If Tests Fail
```bash
# Get details
cargo test --package cqlite-core --lib --all-features -- --nocapture

# Analyze:
# - Is the test for the deleted module? → That's expected (remove test)
# - Is it an integration test? → Check imports
# - Does it import deleted code? → Update imports

# Fix and re-run
```

### If CI Fails
```bash
# Get detailed logs
gh run view $(gh run list --branch cleanup/issue-XXX --limit 1 --json databaseId -q '.[0].databaseId')

# Common issues:
# - Minimal features job fails → Feature gate issue
# - Coverage job fails → Not blocking (can ignore if >30%)
# - Test job fails → Same as local, should have caught earlier

# Fix, commit, push
# CI will re-run automatically
```

### If Merge Blocked
```bash
# Check requirements
gh pr view $PR_NUMBER

# Common blockers:
# - Requires review → Request from @pmcfadin
# - Branch out of date → Rebase:
git fetch origin
git rebase origin/main
git push --force-with-lease

# - Conflicts → Resolve manually, then:
git add -A
git rebase --continue
git push --force-with-lease
```

---

## Communication & Reporting

### Status Updates

Post status updates to PR as you progress:

```bash
# At each major phase
gh pr comment $PR_NUMBER --body "## Status Update

**Phase:** <Current phase name>
**Progress:** <What you just completed>
**Next:** <What you're doing next>

<Any relevant metrics or findings>
"
```

### Completion Report

After merge, post final report:

```bash
gh issue comment XXX --body "## ✅ Issue #XXX Complete

**PR:** #$PR_NUMBER (merged)
**Branch:** cleanup/issue-XXX (deleted)

**Metrics:**
- Lines removed: XXX
- Files deleted: X
- Warnings: Before X → After Y
- All CI checks: PASSED

**Timeline:**
- Started: <timestamp>
- PR created: <timestamp>
- CI green: <timestamp>
- Merged: <timestamp>
- Total time: <duration>

Issue closed automatically on merge.
"
```

---

## Agent Collaboration Examples

### Example 1: Planning Phase
```markdown
**Invoke:** @core/planner.md

**Context:** I'm working on Issue #169 (Delete OptimizedExecutor)

**Task:** Create execution plan

**Required Analysis:**
1. Read cleanup-issues/issue-02-delete-optimized-executor.md
2. Identify all files to change
3. List verification steps
4. Identify potential risks

**Deliverable:** Step-by-step execution plan
```

### Example 2: Code Review
```markdown
**Invoke:** @analysis/code-review/rust-code-reviewer.md

**Context:** Just deleted OptimizedExecutor (1,045 lines)

**Changes:**
- Deleted: cqlite-core/src/query/optimized_executor.rs
- Modified: cqlite-core/src/query/mod.rs (removed module declaration)

**Review Focus:**
1. Are there any remaining references?
2. Is the module export properly removed?
3. Any broken imports?
4. Does it compile clean?

**Deliverable:** Go/No-go for commit
```

### Example 3: CI Investigation
```markdown
**Invoke:** @devops/ci-cd/ops-cicd-github.md

**Context:** CI failing on minimal features build

**Error:** <paste error from CI logs>

**Task:**
1. Analyze root cause
2. Suggest fix
3. Verify fix locally
4. Push corrected version

**Deliverable:** CI green status
```

---

## Special Instructions by Issue

### Issue #169: Delete OptimizedExecutor
- **File:** `cqlite-core/src/query/optimized_executor.rs` (1,045 lines)
- **Extra verification:** `grep -r "OptimizedExecutor" cqlite-core/src/`
- **Expected:** Zero matches outside deleted file
- **Module update:** `cqlite-core/src/query/mod.rs` - remove `pub mod optimized_executor;`

### Issue #170: Delete PerformanceMonitor
- **File:** `cqlite-core/src/performance_monitor.rs` (596 lines)
- **Extra verification:** `grep -r "PerformanceMonitor" cqlite-core/src/`
- **Module update:** `cqlite-core/src/lib.rs` - remove `pub mod performance_monitor;`
- **Note:** File is feature-gated, still safe to delete

### Issue #171: Delete Parser Performance Code
- **Files:**
  - `cqlite-core/src/parser/m3_performance_benchmarks.rs` (1,285 lines)
  - `cqlite-core/src/parser/performance_regression_framework.rs` (822 lines)
- **Module update:** `cqlite-core/src/parser/mod.rs` - remove both module declarations
- **Extra verification:** `grep -r "M3Performance\|PerformanceRegression" cqlite-core/src/`

### Issue #172: Move Docker to Tests
- **Source:** `cqlite-core/src/docker/mod.rs` (262 lines)
- **Destination:** `tests/helpers/docker.rs` or `cqlite-testing/`
- **Special:** This is a MOVE, not delete
- **Create:** `tests/helpers/` directory if needed
- **Update:** All test imports from `cqlite_core::docker` to new location
- **Module update:** `cqlite-core/src/lib.rs` - remove docker module

### Issue #173: Feature-Gate Benchmarks
- **File:** `cqlite-core/Cargo.toml`
- **Change:** Remove `"benchmarks"` from `default = [...]` line
- **Verify:** `grep 'default = .*benchmarks' cqlite-core/Cargo.toml` returns nothing
- **Test:** `cargo build` should NOT compile benchmark code
- **Test:** `cargo build --features=benchmarks` SHOULD compile it

---

## Final Checklist Before Saying "Done"

```bash
# Run this final verification
echo "=== Final Issue Completion Check ==="

# 1. Issue closed?
gh issue view XXX --json state -q .state | grep -q "CLOSED"
echo "Issue closed: $?"

# 2. PR merged?
gh pr view $PR_NUMBER --json state -q .state | grep -q "MERGED"
echo "PR merged: $?"

# 3. Branch deleted remotely?
gh api repos/:owner/:repo/branches/cleanup/issue-XXX 2>&1 | grep -q "404"
echo "Remote branch deleted: $?"

# 4. Branch deleted locally?
git branch | grep -q "cleanup/issue-XXX" && echo "STILL EXISTS" || echo "DELETED"

# 5. Changes in main?
git log main --oneline -5 | grep -q "issue-XXX"
echo "Changes in main: $?"

# 6. Main CI green?
gh run list --branch main --limit 1 --json conclusion -q '.[0].conclusion' | grep -q "success"
echo "Main CI green: $?"

echo "=== If all checks passed, issue is COMPLETE ==="
```

---

## Emergency Rollback

If something goes terribly wrong AFTER merge:

```bash
# Get merge commit SHA
MERGE_SHA=$(gh pr view $PR_NUMBER --json mergeCommit -q .mergeCommit.oid)

# Revert the merge
git checkout main
git pull origin main
git revert -m 1 $MERGE_SHA
git push origin main

# Create rollback issue
gh issue create \
  --title "Rollback: Issue #XXX - <reason>" \
  --body "Rolled back PR #$PR_NUMBER due to: <explain>

See: <link to failure>"

# Notify Patrick
echo "⚠️ ROLLBACK EXECUTED - Notifying @pmcfadin"
```

---

## You Are Ready

You now have everything needed to:
1. ✅ Execute your assigned cleanup issue
2. ✅ Validate changes thoroughly
3. ✅ Navigate CI/CD pipeline
4. ✅ Merge confidently
5. ✅ Clean up properly

**Remember:**
- Use the appropriate **@agents/** at each phase
- Never skip local validation
- Monitor CI closely
- Merge only when 100% green
- Clean up after yourself

**Your success criteria:** Issue closed, PR merged, branch deleted, main CI green.

**Go execute your mission!** 🚀


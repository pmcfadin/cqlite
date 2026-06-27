# Merge Process Workflow

Step-by-step guide to merging pull requests in cqlite.

## Prerequisites

Before merging any PR:

### 1. CI Status: All Green ✅

```bash
# Check PR status
gh pr view <pr-number> --json statusCheckRollup

# Expected: All checks passed
```

**Required checks:**
- Format check
- Clippy (zero warnings)
- Build (minimal features)
- Build (all features)
- Test (Linux)
- Test (macOS)
- Test (Windows)
- Coverage ≥90%
- Property tests
- Integration tests

### 2. Code Review: Approved ✅

```bash
# Check review status
gh pr view <pr-number> --json reviews

# Expected: At least one approval
```

**Review criteria:**
- Logic correct
- Tests adequate
- Documentation updated
- No security issues
- Follows project style

### 3. Branch Status: Up to Date ✅

```bash
# Check if branch is current
gh pr view <pr-number> --json mergeable

# If behind, update:
git checkout feature-branch
git rebase origin/main
git push --force-with-lease
```

### 4. No Conflicts ✅

```bash
# Check for merge conflicts
gh pr view <pr-number> --json mergeable

# If conflicts:
git checkout feature-branch
git rebase origin/main
# Resolve conflicts
git rebase --continue
git push --force-with-lease
```

### 5. Coverage Target Met ✅

```bash
# Check coverage
gh pr view <pr-number> --json statusCheckRollup \
    | jq '.statusCheckRollup[] | select(.name | contains("coverage"))'

# Expected: ≥90%
```

---

## Merge Methods

### Method 1: Squash Merge (Recommended for Cleanup)

**When to use:**
- Multiple commits that should be one logical change
- Cleanup issues (delete files, refactor)
- Small feature additions

**Command:**
```bash
gh pr merge <pr-number> --squash --delete-branch
```

**What it does:**
- Combines all commits into one
- Rewrites commit message
- Deletes branch after merge
- Keeps main history clean

**Commit message format:**
```
cleanup(issue-169): delete OptimizedExecutor dead code

Removed 1,045 lines of unused query execution code that was
out of scope for M1/M2.

- Deleted: cqlite-core/src/query/optimized_executor.rs
- Updated: cqlite-core/src/query/mod.rs (removed module)
- Verified: No references remain

Tests:
- Clippy: PASS (0 warnings)
- Build (minimal): PASS
- Build (all): PASS
- Tests: PASS (147 tests)

Closes #169
```

---

### Method 2: Merge Commit (For Features)

**When to use:**
- Feature branches with meaningful commit history
- Multi-author PRs
- Complex changes worth preserving history

**Command:**
```bash
gh pr merge <pr-number> --merge --delete-branch
```

**What it does:**
- Creates merge commit
- Preserves all individual commits
- Shows branch history in graph

---

### Method 3: Rebase (For Linear History)

**When to use:**
- Small, clean PRs
- Want linear history
- Individual commits are meaningful

**Command:**
```bash
gh pr merge <pr-number> --rebase --delete-branch
```

**What it does:**
- Replays commits on top of main
- No merge commit
- Linear history

---

## Merge Workflow

### Step 1: Final Validation

Before clicking merge:

```bash
# 1. Check CI status
gh pr checks <pr-number>

# 2. Review changes one more time
gh pr diff <pr-number>

# 3. Verify branch is current
gh pr view <pr-number> --json headRefOid,baseRefOid

# 4. Check for conflicts
gh pr view <pr-number> --json mergeable
```

---

### Step 2: Choose Merge Strategy

Based on PR type:

**Cleanup issues → Squash**
```bash
gh pr merge 123 --squash --delete-branch
```

**Feature branches → Merge commit**
```bash
gh pr merge 124 --merge --delete-branch
```

**Small fixes → Rebase**
```bash
gh pr merge 125 --rebase --delete-branch
```

---

### Step 3: Edit Commit Message

**For squash merges:**
```bash
# Edit message interactively
gh pr merge <pr-number> --squash --delete-branch --editor
```

**Format:**
```
<type>(<scope>): <short summary>

<detailed description>

Changes:
- <change 1>
- <change 2>

Tests:
- <test results>

Closes #<issue>
```

---

### Step 4: Confirm Merge

**Review merge details:**
```bash
gh pr view <pr-number>
```

**Confirm:**
- Correct merge method
- Good commit message
- All checks green
- No conflicts

**Proceed with merge** if all looks good.

---

### Step 5: Verify Merge

After merge:

```bash
# 1. Verify commit in main
git checkout main
git pull origin main
git log --oneline -1

# 2. Check CI on main
gh run list --branch main --limit 1

# 3. Verify branch deleted
gh pr view <pr-number> --json headRefName
```

---

## Post-Merge Actions

### Clean Up Local

```bash
# Update main
git checkout main
git pull origin main

# Delete local branch
git branch -D feature-branch

# Prune deleted remote branches
git fetch --prune
```

### Verify Main is Green

```bash
# Check main CI status
gh run list --branch main --limit 1 --json conclusion

# Expected: success
```

### Update Related Issues

If PR closes multiple issues:
```bash
# Manually close if not auto-closed
gh issue close <issue-number> --comment "Fixed in #<pr-number>"
```

---

## Auto-Merge

For trusted contributors, enable auto-merge:

```bash
# Enable auto-merge when checks pass
gh pr merge <pr-number> --auto --squash --delete-branch
```

**Use when:**
- CI already green
- Review already approved
- Just waiting for final check

---

## Merge Conflicts

### Detecting Conflicts

```bash
# Check merge status
gh pr view <pr-number> --json mergeable

# If mergeable: false, resolve conflicts
```

### Resolving Conflicts

**Option 1: Rebase (preferred)**
```bash
git checkout feature-branch
git fetch origin
git rebase origin/main

# Resolve conflicts in editor
# For each conflict:
git add <resolved-file>

git rebase --continue
git push --force-with-lease origin feature-branch
```

**Option 2: Merge**
```bash
git checkout feature-branch
git fetch origin
git merge origin/main

# Resolve conflicts
git add <resolved-file>
git commit

git push origin feature-branch
```

---

## Failed Merge Scenarios

### CI Fails After Merge

**Symptoms:**
- PR merged
- CI on main fails

**Actions:**
1. **Immediate revert**
   ```bash
   git revert -m 1 <merge-commit-sha>
   git push origin main
   ```

2. **Fix in new PR**
   ```bash
   git checkout -b fix/ci-failure
   # Fix issue
   git push origin fix/ci-failure
   gh pr create
   ```

3. **Notify team**
   ```bash
   # Post in Slack/Discord
   "🚨 Main CI broken by #<pr>. Reverted in <revert-commit>. Fix incoming."
   ```

---

### Merge Button Disabled

**Possible reasons:**

1. **Required checks not passed**
   - Wait for CI
   - Fix failures

2. **Review not approved**
   - Request review
   - Address feedback

3. **Conflicts**
   - Rebase on main
   - Resolve conflicts

4. **Branch protection rules**
   - Ensure all required checks green
   - Get required number of approvals

---

## Branch Protection Rules

Main branch protections:

```yaml
# .github/branch-protection.yml
required_status_checks:
  - Format check
  - Clippy
  - Build (minimal)
  - Build (all)
  - Test (Linux)
  - Coverage
  
required_reviews: 1

require_linear_history: false
allow_force_pushes: false
allow_deletions: false
```

---

## Merge Checklist

Quick reference:

- [ ] All CI checks green (10/10)
- [ ] At least 1 approval
- [ ] No merge conflicts
- [ ] Branch up to date with main
- [ ] Coverage ≥90%
- [ ] Commit message follows format
- [ ] Chosen correct merge method
- [ ] Branch will be deleted after merge
- [ ] Related issues will be closed

**If all checked:** Ready to merge! 🎉

---

## Special Cases

### Hotfix to Main

For critical bugs in production:

```bash
# Create hotfix branch from main
git checkout -b hotfix/critical-bug main

# Fix and commit
git commit -m "fix: critical bug in parser"

# Fast-track review
gh pr create --title "HOTFIX: Critical parser bug" --label hotfix

# Merge as soon as CI green
gh pr merge --auto --squash
```

### Backport to Release Branch

For maintaining older versions:

```bash
# Cherry-pick to release branch
git checkout release/v0.1
git cherry-pick <commit-sha>
git push origin release/v0.1

# Create PR
gh pr create --base release/v0.1 --title "Backport: Fix X"
```

---

## Troubleshooting

### "Merge button not clickable"
- Check branch protection rules
- Ensure all required checks passed
- Verify you have merge permissions

### "Branch has conflicts"
- Rebase on latest main
- Resolve conflicts locally
- Force-push (with lease)

### "Check is required but pending"
- Wait for CI to complete
- If stuck, restart CI run
- Check CI logs for issues

---

## Best Practices

1. **Merge during work hours**
   - Can monitor main CI
   - Can quickly revert if needed

2. **Don't merge multiple PRs simultaneously**
   - Hard to identify which broke main
   - Wait for main CI after each merge

3. **Use auto-merge for small PRs**
   - Saves time
   - Merges as soon as checks pass

4. **Always delete branches after merge**
   - Keeps repo clean
   - Prevents confusion

5. **Write good merge commit messages**
   - Future you will thank you
   - Helps with git bisect

---

## Metrics to Track

Good merge health indicators:

- **Time to merge**: <24 hours from PR creation
- **Main CI stability**: >99% green
- **Revert rate**: <1% of merges
- **Review turnaround**: <4 hours

---

## Summary

**Typical merge flow:**
1. Create PR
2. Wait for CI ✅
3. Get review approval ✅
4. Update branch if needed
5. Choose merge method (usually squash)
6. Merge and delete branch
7. Verify main CI green
8. Clean up local branches

**Total time:** Usually <1 hour if CI is fast.


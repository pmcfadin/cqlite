# CQLite Core Cleanup Issues

This directory contains all the issue specifications and automation for cleaning up ~10,000 lines of out-of-scope code from cqlite-core.

## Quick Start

### Option 1: Create All Issues at Once (Recommended)

```bash
cd cleanup-issues
./create-github-issues.sh
```

This will:
1. ✅ Check that `gh` CLI is installed and authenticated
2. 📝 Create all 13 GitHub issues with proper labels
3. 💾 Save issue URLs to `.issue-urls.txt`
4. 🏷️ Apply appropriate labels (cleanup, P0/P1, dead-code, etc.)

### Option 2: Create Issues Manually

Copy content from individual issue files into GitHub's issue creation form:

```bash
# View an issue
cat issue-01-setup-safety-nets.md

# Copy to clipboard (macOS)
cat issue-01-setup-safety-nets.md | pbcopy
```

Then paste into: https://github.com/yourusername/cqlite/issues/new

---

## Prerequisites

### Install GitHub CLI

```bash
# macOS
brew install gh

# Linux
# See: https://github.com/cli/cli/blob/trunk/docs/install_linux.md

# Windows
# See: https://github.com/cli/cli/releases
```

### Authenticate

```bash
gh auth login
```

Follow the prompts to authenticate with your GitHub account.

### Verify

```bash
gh auth status
# Should show: ✓ Logged in to github.com as <your-username>
```

---

## File Structure

```
cleanup-issues/
├── README.md                          ← You are here
├── EXECUTIVE_SUMMARY.md                ← Start here - overview for teams
├── CLEANUP_ROADMAP.md                  ← Dependency graph & timeline
├── ISSUE_TEMPLATES.md                  ← Quick refs for issues #5-#7, #10-#13
│
├── create-github-issues.sh             ← Automation script
│
├── issue-01-setup-safety-nets.md       ← MUST DO FIRST
├── issue-02-delete-optimized-executor.md
├── issue-03-delete-performance-monitor.md
├── issue-04-delete-parser-perf-code.md
├── issue-08-feature-gate-write-methods.md
└── issue-09-remove-wal-memtable.md

# Generated during script run:
├── issue-05-move-docker.md             ← Created by script
├── issue-06-feature-gate-benchmarks.md
├── issue-07-feature-gate-tombstones.md
├── issue-10-remove-compaction-manifest.md
├── issue-11-simplify-select-optimizer.md
├── issue-12-update-feature-defaults.md
├── issue-13-final-validation.md
└── .issue-urls.txt                     ← Issue URLs after creation
```

---

## Usage

### Create All Issues

```bash
cd cleanup-issues
./create-github-issues.sh
```

**Output:**
```
🚀 Creating CQLite Core Cleanup GitHub Issues
✅ GitHub CLI authenticated

📝 Creating issues...

Creating Issue #1: Setup Safety Nets for Code Cleanup
✅ Created: https://github.com/yourusername/cqlite/issues/123

Creating Issue #2: Delete OptimizedExecutor (Dead Code)
✅ Created: https://github.com/yourusername/cqlite/issues/124

...

✅ All issues created!

📋 Issue URLs saved to: .issue-urls.txt
```

### View Created Issues

```bash
# View all cleanup issues
gh issue list --label cleanup

# View by priority
gh issue list --label P0
gh issue list --label P1

# View by type
gh issue list --label dead-code
gh issue list --label architecture
```

### Edit Issues After Creation

```bash
# Edit an issue
gh issue edit 123 --add-label "good-first-issue"

# Add milestone
gh issue edit 123 --milestone "M1 Cleanup"

# Assign to someone
gh issue edit 123 --assignee @username
```

---

## Issue Dependency Order

**Phase 0: Preparation**
1. Issue #1 - Setup Safety Nets ⚠️ **MUST BE FIRST**

**Phase 1: Dead Code (Parallel)**
- Issue #2 - Delete OptimizedExecutor
- Issue #3 - Delete PerformanceMonitor
- Issue #4 - Delete Parser Performance Code
- Issue #5 - Move Docker to Tests

**Phase 2: Feature Gating (Parallel)**
- Issue #6 - Feature-Gate Benchmarks
- Issue #7 - Feature-Gate Tombstones

**Phase 3: Write Infrastructure (Sequential)**
- Issue #8 - Feature-Gate Write Methods
- Issue #9 - Remove WAL and MemTable (requires #8)
- Issue #10 - Remove Compaction/Manifest (requires #8, #9)

**Phase 4: Simplification (Sequential)**
- Issue #11 - Simplify SelectOptimizer (requires #10)
- Issue #12 - Update Feature Defaults (requires #11)

**Phase 5: Validation**
- Issue #13 - Final Validation Suite (requires all)

---

## Labels Applied

The script automatically applies these labels:

- `cleanup` - All issues get this
- `P0` - High priority (can start immediately after #1)
- `P1` - Medium priority (has dependencies)
- `dead-code` - Code that's never called
- `architecture` - Core storage changes
- `feature-gate` - Feature flag changes
- `ci` - CI/automation changes
- `configuration` - Cargo.toml changes
- `infrastructure` - Platform/tooling changes
- `refactor` - Code reorganization
- `validation` - Testing/verification

---

## Troubleshooting

### Error: `gh: command not found`

**Solution:** Install GitHub CLI:
```bash
brew install gh
```

### Error: `Not authenticated with GitHub`

**Solution:** Authenticate:
```bash
gh auth login
```

### Error: `Resource not accessible by personal access token`

**Solution:** Re-authenticate with correct scopes:
```bash
gh auth refresh -h github.com -s repo
```

### Want to delete all created issues?

**WARNING:** This will delete ALL issues with the `cleanup` label!

```bash
# List them first
gh issue list --label cleanup

# Delete (if you're sure)
gh issue list --label cleanup --json number -q '.[].number' | \
  xargs -I {} gh issue delete {} --yes
```

---

## Customization

### Add Project Board

```bash
# Create project board
gh project create --title "M1/M2 Cleanup" --body "Code cleanup for M1/M2 scope"

# Add issues to project (requires project number)
cat .issue-urls.txt | while read line; do
  issue_num=$(echo $line | cut -d'|' -f1)
  gh project item-add PROJECT_NUMBER --owner OWNER --content-url "https://github.com/OWNER/cqlite/issues/$issue_num"
done
```

### Create Milestones

```bash
# Create milestone
gh api repos/:owner/:repo/milestones -f title="M1 Cleanup" -f description="Remove out-of-scope code"

# Assign issues to milestone
gh issue edit 123 --milestone "M1 Cleanup"
```

---

## After Issues Are Created

### 1. Review on GitHub

Visit: https://github.com/yourusername/cqlite/issues?q=label%3Acleanup

### 2. Assign Teams

```bash
# Assign team members
gh issue edit 2 --assignee @developer1
gh issue edit 3 --assignee @developer2
```

### 3. Track Progress

```bash
# See open cleanup issues
gh issue list --label cleanup --state open

# See completed
gh issue list --label cleanup --state closed
```

---

## Questions?

- **What's the total timeline?** 2-3 weeks with parallel teams
- **Can we skip any issues?** Only #11 (simplify optimizer) is optional
- **What's the risk?** Issues #1-#7 are zero/low risk. Issues #8-#10 are medium risk.
- **How much code will be removed?** ~10,000 lines (27% of codebase)

See `EXECUTIVE_SUMMARY.md` for full details.

---

## Support

- File issues: https://github.com/yourusername/cqlite/issues
- Slack: #cqlite-cleanup
- Email: team@cqlite.dev

---

**Ready to clean up? Run `./create-github-issues.sh` to get started!** 🧹


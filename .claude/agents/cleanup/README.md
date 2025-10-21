# CQLite Cleanup Agent System

**Purpose:** Autonomous execution of cleanup Issues #169-179 using Claude Code with subagent coordination

---

## Quick Start (5 seconds)

```bash
# 1. Open QUICK_START_PROMPTS.md
open .claude/agents/cleanup/QUICK_START_PROMPTS.md

# 2. Copy-paste 5 prompts into 5 Claude Code instances

# 3. Done! They'll handle everything.
```

---

## Files in This Directory

### 📘 User Files (Read These)

1. **QUICK_START_PROMPTS.md** ⭐ START HERE
   - Copy-paste prompts for each Claude instance
   - One prompt per issue (#169-173)
   - 30 seconds to launch all 5

2. **TEAM_ASSIGNMENTS.md**
   - Detailed team structure
   - Monitoring commands
   - Troubleshooting guide
   - Success criteria

3. **README.md** (this file)
   - Overview and navigation

### 🤖 Agent Files (Claude Reads These)

4. **cleanup-agent-prompt.md**
   - Complete execution instructions (9,000+ words)
   - Workflow with all phases
   - Subagent collaboration examples
   - Error handling procedures
   - All validation steps

---

## How It Works

### Architecture

```
Patrick (Human)
    |
    └─> Launches 5 Claude Code instances
            |
            ├─> Claude A (Issue #169) ──> Uses @agents/ subagents
            ├─> Claude B (Issue #170) ──> Uses @agents/ subagents
            ├─> Claude C (Issue #171) ──> Uses @agents/ subagents
            ├─> Claude D (Issue #172) ──> Uses @agents/ subagents
            └─> Claude E (Issue #173) ──> Uses @agents/ subagents
                    |
                    └─> Each Claude:
                         1. Reads cleanup-agent-prompt.md
                         2. Creates execution plan (@core/planner)
                         3. Makes changes (@core/coder)
                         4. Self-reviews (@core/reviewer)
                         5. Validates locally (@core/tester)
                         6. Creates PR (@github/pr-manager)
                         7. Monitors CI (@devops/ci-cd/ops-cicd-github)
                         8. Auto-merges when green
                         9. Cleans up branch
```

### Subagents Used

Each Claude instance uses these agents from `@agents/`:

| Phase | Subagents | Purpose |
|-------|-----------|---------|
| Planning | @core/planner, @development/rust/rust-developer | Analyze issue, create plan |
| Coding | @core/coder, @development/rust/rust-developer | Execute changes |
| Review | @core/reviewer, @analysis/code-review/rust-code-reviewer | Self-review |
| Testing | @core/tester, @testing/validation/production-validator | Validate |
| CI/CD | @devops/ci-cd/ops-cicd-github, @github/pr-manager | Merge |

---

## What Each Claude Does

### Autonomous Workflow (No Human Intervention)

```bash
# Phase 1: Setup (5 minutes)
- Read instructions
- Analyze issue
- Create branch
- Capture baseline

# Phase 2: Execute (10 minutes)
- Delete/move files
- Update module declarations
- Fix imports

# Phase 3: Validate (15 minutes)
- cargo fmt
- cargo clippy -- -D warnings (MUST be zero)
- cargo build --no-default-features --features=all-compression
- cargo build --all-features
- cargo test --lib --all-features
- ./scripts/validate-cleanup.sh

# Phase 4: Commit & PR (5 minutes)
- Self-review with subagents
- Commit with detailed message
- Push (only if validation passed)
- Create PR with gh CLI

# Phase 5: Monitor & Merge (30-60 minutes)
- Watch CI checks (poll every 30s)
- Fix if CI fails
- Auto-merge when 100% green
- Delete branch

# Phase 6: Report (2 minutes)
- Post completion comment
- Verify cleanup
```

**Total per issue:** 1-2 hours (mostly CI wait time)

**Total for all 5 in parallel:** ~2 hours

---

## Validation Gates

Each Claude must pass ALL of these before pushing:

### Gate 1: Format
```bash
cargo fmt --all
# Must run without changes
```

### Gate 2: Clippy (ZERO Warnings)
```bash
cargo clippy --package cqlite-core --lib --all-features -- -D warnings
# MUST exit 0 with zero warnings
```

### Gate 3: Build (Minimal)
```bash
cargo build --package cqlite-core --no-default-features --features=all-compression
# MUST succeed (M1 scope)
```

### Gate 4: Build (All)
```bash
cargo build --package cqlite-core --all-features
# MUST succeed
```

### Gate 5: Tests
```bash
cargo test --package cqlite-core --lib --all-features
# MUST pass all tests
```

### Gate 6: Validation Script
```bash
./scripts/validate-cleanup.sh
# MUST pass all 4 checks
```

### Gate 7: No References
```bash
grep -r "DeletedModule" cqlite-core/src/
# MUST return nothing (for deletions)
```

**If ANY gate fails, Claude fixes and re-runs. Never pushes until ALL pass.**

---

## Safety Features

### Built-in Safety

1. **Issue #168 (Safety Nets)** 
   - CI validates feature gates
   - Coverage baseline tracked
   - Validation script catches issues

2. **Local Validation First**
   - All checks run locally before push
   - CI is confirmation, not discovery

3. **Auto-Rollback**
   - Each Claude can revert if needed
   - Documented rollback procedures

4. **Parallel Independence**
   - Issues don't conflict
   - Can merge in any order
   - No coordination needed

### Failure Handling

**If local validation fails:**
- Claude fixes and retries
- Never pushes broken code

**If CI fails:**
- Claude investigates logs
- Fixes locally
- Force-pushes fix
- Re-monitors CI

**If merge blocked:**
- Claude rebases automatically
- Resolves simple conflicts
- Escalates complex ones

---

## Monitoring Dashboard

### Watch All Activity
```bash
# In one terminal
watch -n 10 'echo "=== Open PRs ===" && gh pr list --label cleanup --state open && echo && echo "=== Recent CI ===" && gh run list --limit 5 --json status,conclusion,name | jq'
```

### Quick Status
```bash
# How many complete?
gh issue list --search "is:closed 169 170 171 172 173" | wc -l

# How many PRs merged?
gh pr list --search "is:merged label:cleanup" | grep -E "169|170|171|172|173" | wc -l

# Main CI status
gh run list --branch main --limit 1 --json conclusion -q '.[0].conclusion'
```

---

## Expected Results

### Metrics

**Before (from BASELINE_METRICS.md):**
- Total lines: 101,143
- Files: 159
- Clippy warnings: 10

**After Issues #169-173:**
- Total lines: ~97,400 (-3,743)
- Files: 154 (-5, +1 moved)
- Clippy warnings: ≤10

### Files Changed

**Deleted:**
- cqlite-core/src/query/optimized_executor.rs (1,045 lines)
- cqlite-core/src/performance_monitor.rs (596 lines)
- cqlite-core/src/parser/m3_performance_benchmarks.rs (1,285 lines)
- cqlite-core/src/parser/performance_regression_framework.rs (822 lines)

**Moved:**
- cqlite-core/src/docker/mod.rs → tests/helpers/docker.rs (262 lines)

**Modified:**
- cqlite-core/Cargo.toml (benchmarks removed from default)
- Various mod.rs files (module declarations removed)

---

## Timeline

### Parallel Execution (Recommended)

```
T+0min    : Start all 5 Claude instances
T+10min   : All reading instructions
T+30min   : All making changes
T+60min   : All PRs created
T+90min   : First PRs merging (169, 170, 173)
T+120min  : All PRs merged
T+125min  : All branches cleaned up

Total: 2 hours
```

### Sequential Execution (Not Recommended)

```
T+0        : Start Issue #169
T+1h       : #169 merged, start #170
T+2h       : #170 merged, start #171
T+3h       : #171 merged, start #172
T+4.5h     : #172 merged, start #173
T+5.5h     : #173 merged

Total: 5.5 hours
```

**Parallel is 2.75x faster!**

---

## Success Declaration

All issues complete when this command outputs "SUCCESS":

```bash
#!/bin/bash
CLOSED=$(gh issue list --search "is:closed 169 170 171 172 173" | wc -l)
MERGED=$(gh pr list --search "is:merged label:cleanup" | grep -E "169|170|171|172|173" | wc -l)
CI=$(gh run list --branch main --limit 1 --json conclusion -q '.[0].conclusion')
BRANCHES=$(git branch -r | grep -c "cleanup/issue-1" || echo 0)

if [ $CLOSED -eq 5 ] && [ $MERGED -eq 5 ] && [ "$CI" = "success" ] && [ $BRANCHES -eq 0 ]; then
  echo "✅ SUCCESS: All 5 issues complete!"
  exit 0
else
  echo "❌ Not complete: Closed=$CLOSED/5, Merged=$MERGED/5, CI=$CI, Branches=$BRANCHES/0"
  exit 1
fi
```

---

## Troubleshooting

### Claude Won't Start

**Problem:** Claude just sits there

**Solution:** 
```
Restate the prompt clearly:

"Read @.claude/agents/cleanup/cleanup-agent-prompt.md and begin execution of Issue #XXX. Start by using @core/planner to analyze the issue."
```

### Claude Pushes Without Validating

**Problem:** Broke the rules

**Solution:**
```
Stop immediately! Tell Claude:

"STOP. You must run ALL local validations before pushing. Run:
1. cargo fmt
2. cargo clippy -- -D warnings (must be ZERO)
3. cargo build --no-default-features --features=all-compression
4. cargo test --lib
5. ./scripts/validate-cleanup.sh

Do NOT push until all pass."
```

### CI Fails

**Problem:** Remote CI red

**Solution:** Claude will handle automatically. Watch:
```bash
gh pr checks [number]
# Claude will investigate logs, fix, and retry
```

### Merge Conflict

**Problem:** Another PR merged first

**Solution:** Claude will rebase automatically:
```bash
# Claude does this automatically:
git fetch origin
git rebase origin/main
git push --force-with-lease
```

---

## After Completion

### Generate Report

```bash
gh issue comment 168 --body "## 🎉 Phase 1 Complete

Issues #169-173 all merged!

**Metrics:**
- Lines removed: $(git diff HEAD~5 --shortstat | grep -o '[0-9]* deletions')
- Files removed: 4
- Files moved: 1

**Timeline:**
- Duration: [X hours]
- All CI checks: PASSED
- Zero rollbacks needed

**Next:** Ready for Issue #174 (Feature-Gate Write Methods)"
```

### Update Tracking

```bash
# Mark milestone complete
gh api repos/:owner/:repo/milestones/[id] -X PATCH -f state=closed

# Update project board
# (or let automation do it)
```

---

## Next Steps After #169-173

After these 5 issues complete:

1. **Issue #174** - Feature-Gate Write Methods (P1, Medium Risk)
   - Cannot parallelize
   - Needs senior engineer
   - Blocks #175, #176

2. **Issues #175-176** - Remove write infrastructure (Sequential)

3. **Issues #177-179** - Final cleanup and validation

See `cleanup-issues/CLEANUP_ROADMAP.md` for full dependency graph.

---

## Questions?

**Problem with agent system?** Check subagent READMEs:
- `@agents/core/README.md`
- `@agents/development/rust/README.md`
- `@agents/github/README.md`

**Problem with issue specs?** Check:
- `cleanup-issues/CLEANUP_ROADMAP.md`
- `cleanup-issues/EXECUTIVE_SUMMARY.md`

**Problem with CI?** Check:
- `docs/cleanup/BASELINE_METRICS.md`
- `.github/workflows/ci-minimal-features.yml`

---

## File Index

```
.claude/agents/cleanup/
├── README.md                      ← You are here
├── QUICK_START_PROMPTS.md         ← Copy-paste to launch
├── TEAM_ASSIGNMENTS.md            ← Monitoring & troubleshooting
└── cleanup-agent-prompt.md        ← Full instructions for Claudes
```

---

**Ready to launch?** Open `QUICK_START_PROMPTS.md` and copy-paste! 🚀


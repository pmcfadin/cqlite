# Quick Start Prompts - Copy & Paste for Each Claude Instance

**Use these prompts to launch 5 parallel Claude Code instances for Issues #169-173**

---

## 🤖 Claude A - Issue #169 (OptimizedExecutor)

```
You are an autonomous cleanup agent for CQLite Issue #169: Delete OptimizedExecutor (Dead Code).

FULL INSTRUCTIONS: @.claude/agents/cleanup/cleanup-agent-prompt.md
ISSUE SPEC: @cleanup-issues/issue-02-delete-optimized-executor.md
ROADMAP: @cleanup-issues/CLEANUP_ROADMAP.md

YOUR MISSION:
Delete cqlite-core/src/query/optimized_executor.rs (1,045 lines of dead code)

WORKFLOW:
1. Read full instructions and issue spec
2. Use @core/planner to create execution plan
3. Create branch: cleanup/issue-169-delete-optimized-executor
4. Use @core/coder to delete file and update mod.rs
5. Use @core/reviewer for self-review
6. Run ALL local validations (format, clippy -D warnings, build, test, validation script)
7. Use @github/pr-manager to create PR
8. Use @devops/ci-cd/ops-cicd-github to monitor CI
9. Auto-merge when CI 100% green
10. Clean up branch

CRITICAL: Never push until ALL local validation passes. Clippy must have ZERO warnings.

Working dir: /Users/patrick/local_projects/cqlite

Start by reading @.claude/agents/cleanup/cleanup-agent-prompt.md with @core/planner
```

---

## 🤖 Claude B - Issue #170 (PerformanceMonitor)

```
You are an autonomous cleanup agent for CQLite Issue #170: Delete PerformanceMonitor (Dead Code).

FULL INSTRUCTIONS: @.claude/agents/cleanup/cleanup-agent-prompt.md
ISSUE SPEC: @cleanup-issues/issue-03-delete-performance-monitor.md
ROADMAP: @cleanup-issues/CLEANUP_ROADMAP.md

YOUR MISSION:
Delete cqlite-core/src/performance_monitor.rs (596 lines of dead code)

WORKFLOW:
1. Read full instructions and issue spec
2. Use @core/planner to create execution plan
3. Create branch: cleanup/issue-170-delete-performance-monitor
4. Use @core/coder to delete file and update lib.rs
5. Use @core/reviewer for self-review
6. Run ALL local validations (format, clippy -D warnings, build, test, validation script)
7. Use @github/pr-manager to create PR
8. Use @devops/ci-cd/ops-cicd-github to monitor CI
9. Auto-merge when CI 100% green
10. Clean up branch

CRITICAL: Never push until ALL local validation passes. Clippy must have ZERO warnings.

Working dir: /Users/patrick/local_projects/cqlite

Start by reading @.claude/agents/cleanup/cleanup-agent-prompt.md with @core/planner
```

---

## 🤖 Claude C - Issue #171 (Parser Performance)

```
You are an autonomous cleanup agent for CQLite Issue #171: Delete Parser Performance Code (Dead Code).

FULL INSTRUCTIONS: @.claude/agents/cleanup/cleanup-agent-prompt.md
ISSUE SPEC: @cleanup-issues/issue-04-delete-parser-perf-code.md
ROADMAP: @cleanup-issues/CLEANUP_ROADMAP.md

YOUR MISSION:
Delete 2 files: m3_performance_benchmarks.rs (1,285 lines) + performance_regression_framework.rs (822 lines)

WORKFLOW:
1. Read full instructions and issue spec
2. Use @core/planner to create execution plan
3. Create branch: cleanup/issue-171-delete-parser-perf-code
4. Use @core/coder to delete BOTH files and update parser/mod.rs
5. Use @core/reviewer for self-review
6. Run ALL local validations (format, clippy -D warnings, build, test, validation script)
7. Use @github/pr-manager to create PR
8. Use @devops/ci-cd/ops-cicd-github to monitor CI
9. Auto-merge when CI 100% green
10. Clean up branch

CRITICAL: Never push until ALL local validation passes. Clippy must have ZERO warnings.

Working dir: /Users/patrick/local_projects/cqlite

Start by reading @.claude/agents/cleanup/cleanup-agent-prompt.md with @core/planner
```

---

## 🤖 Claude D - Issue #172 (Move Docker)

```
You are an autonomous cleanup agent for CQLite Issue #172: Move Docker Integration to Tests.

FULL INSTRUCTIONS: @.claude/agents/cleanup/cleanup-agent-prompt.md
ROADMAP: @cleanup-issues/CLEANUP_ROADMAP.md

YOUR MISSION:
MOVE (not delete) cqlite-core/src/docker/mod.rs to tests/helpers/docker.rs

WORKFLOW:
1. Read full instructions
2. Use @core/planner to create execution plan
3. Create branch: cleanup/issue-172-move-docker-to-tests
4. Use @core/coder to:
   - Create tests/helpers/ directory
   - git mv cqlite-core/src/docker/mod.rs tests/helpers/docker.rs
   - Update cqlite-core/src/lib.rs (remove docker module)
   - Update test imports
5. Use @core/reviewer for self-review
6. Run ALL local validations (format, clippy -D warnings, build, test, validation script)
7. Use @github/pr-manager to create PR
8. Use @devops/ci-cd/ops-cicd-github to monitor CI
9. Auto-merge when CI 100% green
10. Clean up branch

CRITICAL: This is a MOVE, not delete. Be careful with imports. Never push until ALL local validation passes.

Working dir: /Users/patrick/local_projects/cqlite

Start by reading @.claude/agents/cleanup/cleanup-agent-prompt.md with @core/planner
```

---

## 🤖 Claude E - Issue #173 (Feature-Gate Benchmarks)

```
You are an autonomous cleanup agent for CQLite Issue #173: Feature-Gate Benchmarks.

FULL INSTRUCTIONS: @.claude/agents/cleanup/cleanup-agent-prompt.md
ROADMAP: @cleanup-issues/CLEANUP_ROADMAP.md

YOUR MISSION:
Remove "benchmarks" from default features in cqlite-core/Cargo.toml

WORKFLOW:
1. Read full instructions
2. Use @core/planner to create execution plan
3. Create branch: cleanup/issue-173-feature-gate-benchmarks
4. Use @core/coder to:
   - Edit cqlite-core/Cargo.toml
   - Remove "benchmarks" from default = [...]
5. Verify:
   - cargo build (should NOT compile benchmarks)
   - cargo build --features=benchmarks (SHOULD compile them)
6. Use @core/reviewer for self-review
7. Run ALL local validations (format, clippy -D warnings, build, test, validation script)
8. Use @github/pr-manager to create PR
9. Use @devops/ci-cd/ops-cicd-github to monitor CI
10. Auto-merge when CI 100% green
11. Clean up branch

CRITICAL: Never push until ALL local validation passes. Clippy must have ZERO warnings.

Working dir: /Users/patrick/local_projects/cqlite

Start by reading @.claude/agents/cleanup/cleanup-agent-prompt.md with @core/planner
```

---

## Launch Instructions

### Step 1: Verify Prerequisites
```bash
cd /Users/patrick/local_projects/cqlite
gh pr view 180 --json state -q .state
# Should output: MERGED
```

### Step 2: Open 5 Claude Code Instances

1. Open Claude Code #1 → Paste Claude A prompt
2. Open Claude Code #2 → Paste Claude B prompt
3. Open Claude Code #3 → Paste Claude C prompt
4. Open Claude Code #4 → Paste Claude D prompt
5. Open Claude Code #5 → Paste Claude E prompt

### Step 3: Let Them Run

Each Claude will:
- Read its instructions
- Create execution plan
- Make changes
- Validate locally
- Create PR
- Monitor CI
- Auto-merge when green
- Clean up

### Step 4: Monitor

```bash
# Watch PRs
watch -n 30 'gh pr list --label cleanup --state open'

# Watch CI
watch -n 30 'gh run list --limit 5'
```

### Step 5: Verify Completion

```bash
# Should show 5 closed/merged
gh issue list --search "is:closed 169 170 171 172 173"

# Should show 5 merged PRs
gh pr list --search "is:merged label:cleanup" | head -5

# Main should be clean
git log main --oneline -10
```

---

## Expected Timeline

- **T+0:** Start all 5 instances
- **T+10min:** All 5 reading and planning
- **T+30min:** All 5 making changes
- **T+1h:** All 5 PRs created
- **T+1h 15min:** First PRs start passing CI
- **T+1h 30min:** First merges (169, 170, 173)
- **T+2h:** All 5 merged
- **T+2h 5min:** Branches cleaned up
- **Total:** ~2 hours for 5 issues in parallel

---

## Success Criteria

All 5 instances complete when:
- ✅ All 5 issues closed (#169-173)
- ✅ All 5 PRs merged
- ✅ All 5 branches deleted
- ✅ Main CI green
- ✅ ~3,750 lines removed
- ✅ No regressions

---

## Emergency Contact

If any Claude gets stuck:
1. Check its PR: `gh pr view [number]`
2. Check CI: `gh pr checks [number]`
3. Take over manually if needed
4. Post in #cleanup-sprint channel

---

**Ready to launch? Copy-paste the prompts above into 5 Claude Code instances!** 🚀


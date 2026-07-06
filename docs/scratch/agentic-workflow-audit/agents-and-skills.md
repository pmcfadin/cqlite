# Agentic Delivery Workflow Audit: Agents & Skills

## Inventory

### Agents (7 specialist roles spawned during pipeline)
1. **flow-lead** (sonnet, main orchestrator — NOT spawned, is the session owner)
   - Runs the 5-stage pipeline (groom→activate→implement→address→finalize)
   - Spawns + sequences specialists; honors 2 human seams (spec approval, merge)
   - Read-only GitHub issue/Project access via gh CLI
   
2. **sstable-developer** (sonnet, TDD implementer)
   - SSTable parsing, binary format debugging, Rust development
   - Tools: Read, Write, Edit, Bash, Glob, Grep
   - Runs `scripts/agent-gate.sh --lite` in each fix round (NOT full gate — lead only)
   
3. **spec-auditor** (sonnet, intent audit "C" — design-driven only)
   - Read-only spec audit against OpenSpec `specs/**/*.md` + issue acceptance criteria
   - Verdict: each requirement must be `satisfied` (with public-surface test), `partial` (justified), or `unmet` (blocks merge)
   - Tools: Read, Grep, Glob, Bash
   
4. **rust-reviewer** (sonnet, code-quality read-only)
   - Enforces memory safety, error handling, performance targets (<128MB), wiring-evidence
   - Review checklist: no unwrap in lib code, thiserror, clear naming, <50-line functions
   - Tools: Read, Glob, Grep (no cargo, no gate)
   - Optional "review-first" before first full gate if diff touches `pub` items / >1 call site
   
5. **test-validator** (haiku, test execution + parity)
   - Runs smoke tests, sstabledump parity checks, failure triage
   - Updates validation matrix
   - Tools: Read, Bash, Glob, Grep
   
6. **coverage-reviewer** (sonnet, test-quality audit)
   - Assesses whether tests exercise meaningful behavior (happy path, edge cases, boundaries)
   - Detects weak/over-mocked tests, untested branches
   - Verdict: PASS or CHANGES NEEDED
   - Tools: Read, Grep, Glob, Bash
   
7. **compaction-parity-auditor** (sonnet, write-path correctness)
   - Audits byte-for-byte parity gaps vs Apache Cassandra (write + merge + SSTable)
   - Source of truth: `docs/compaction/byte-parity-rules.md`
   - Tracks against Cassandra cursor-compaction branch history
   - Tools: Read, Grep, Glob, Bash

### Flow Pipeline Skills (5 stages + visibility)
1. **flow-groom** — rough idea → one P0–P3 GitHub issue + acceptance criteria
   - Decide oracle-vs-design routing (oracle skips OpenSpec, goes straight to implement)
   - Output: issue number + routing type
   
2. **flow-activate** — design-driven issue → OpenSpec spec + design (Seam 1: owner approval)
   - Create worktree + branch, push claim (the 1:1:1:1 lock)
   - Run `openspec propose` → proposal.md, design.md, specs/, tasks.md
   - Render spec + design inline, flip status:spec-review, **STOP for owner approval**
   - Do NOT implement
   
3. **flow-implement** — approved issue → PR (Seam 2: merge autonomy)
   - Spawn `sstable-developer` to implement test-first
   - **Tiered gate loop (issue #1821):** implementer runs `--lite` each round (~1–5 min), lead runs FULL gate ONCE before merge (12–25 min)
   - `--lite` NEVER replaces full gate — only full `==== AGENT-GATE SUMMARY ====` block counts
   - Optional conditional review-first (rust-reviewer) if diff is structural
   - Run spec-auditor C (design-driven), roborev, push PR
   - **Arm merge-on-green, do NOT merge by default.** Escalate design decisions to owner
   - Terminal state: gate PASS + C PASS + roborev clean → end turn (no CI polling)
   
4. **flow-address** — PR review comments → resolved + re-gated
   - Fix mechanical feedback in worktree; escalate design questions to owner
   - Re-run gate + C as needed
   - Reply per thread, flip back to status:in-review, hand to owner for merge
   
5. **flow-finalize** — merged PR → archived + closed + telemetry
   - Archive OpenSpec change (move to `openspec/changes/archive/`, sync delta spec)
   - Stamp telemetry ledger (`scripts/delivery-telemetry.py record`)
   - Set Project Status=Done, release claim lock (remove `issue-<N>-*` branch)
   - Close issue with traceable comment
   
6. **flow-board** — visibility + "what's next"
   - Render GitHub Project board (Status column is sole dispatch authority)
   - Check for abandoned claims (stale `In Progress` items, no recent commits)
   - Surface ONE furthest-along item waiting on owner (green PR to merge, spec to approve)
   - Offer claim-aware pick-list (Ready items with no origin branch)

## How It Works: Context Flow & Claim Protocol

### Claim Protocol (cross-machine concurrency control)
1. **Eligibility check:** item Status=Ready AND no `issue-<N>-*` branch on origin
2. **Claim:** create worktree + branch, push to origin as lock (non-force)
3. **Re-verify:** fetch + confirm remote SHA matches local HEAD (you won the race)
4. **Work in isolation:** worktree is private; all pushes go to origin `issue-<N>-<slug>`
5. **Release on finalize:** delete the origin branch (only after PR merged)

### One-issue-per-flow topology (1:1:1:1)
- GitHub issue `#N` ↔ worktree `.claude/worktrees/issue-<N>-<slug>` ↔ branch `issue-<N>-<slug>` ↔ OpenSpec change `<slug>` ↔ one PR
- Worktrees lack Data.db binaries — test data lives at `CQLITE_DATASETS_ROOT` (main repo `test-data/datasets`)
- Violations (multi-lock, multi-branch, multi-PR per issue) are hard errors

### Context handoff pattern
**Large content (gate logs, diffs, specs) flows via:**
- File system: worktree artifacts read by flow lead via bash/Read (no agent context bloat)
- GitHub: PR description includes `Closes #<N>`, issue body carries acceptance criteria
- OpenSpec: spec file paths shared via spawn prompt (spec-auditor reads `openspec/changes/<slug>/specs/**`)
- Gate summary block: pasted verbatim by implementer in `--lite` reports; full block by lead

### The long-running implement loop (flow-implement, issue #1821)
**Tiered gate strategy to avoid subagent stall-watchdog kills:**
1. Implementer: test-first code + `scripts/agent-gate.sh --lite` **each round** (~1–5 min)
   - `--lite`: fmt + file-size + workspace clippy + blast-radius-scoped tests
   - `--lite` emits `==== AGENT-GATE LITE SUMMARY ====` (distinct block, NOT the gate of record)
   - If FAIL: fix + retry. If PASS: commit + report LITE block + end turn
   
2. Lead (optional): conditional internal `rust-reviewer` review-first if diff is structural (pub items, >1 call site)
   - If findings: loop back to implementer for fix + new `--lite` pass
   
3. Lead: run FULL `scripts/agent-gate.sh` EXACTLY ONCE (~12–25 min, may queue under #1825 cap)
   - Must be PASS; `--lite` NEVER replaces this
   - Paste `==== AGENT-GATE SUMMARY ====` block verbatim
   
4. Spec-auditor C (design-driven): intent audit, verdict PASS
   - Each requirement `satisfied` (with public-surface test evidence), `partial` (justified), or `unmet` (blocks merge)
   
5. Roborev: mechanical fixes, escalate design decisions
   - If code changes: re-run `--lite`, then full gate once more before merge
   
6. Lead: merge-on-green protocol or hand to owner (design decisions, scope questions, unresolved findings)

**Division of labor (issue #1855):** implementer never runs full gate (subagent idle on 12–20 min gate → stall watchdog kills it); lead owns full gate + roborev.

## Measured / Observed Costs

### Time per issue
- **flow-groom:** <5 min
- **flow-activate:** 20–30 min (spec authoring)
- **flow-implement:** 45 min–3 hours (depends on issue complexity)
  - Implementer fix rounds: ~5 min per `--lite` pass (usually 1–5 rounds)
  - Conditional review-first: +10 min if structural
  - Full gate: 12–25 min (can queue, total wall-time >20 min possible under load)
  - Spec-auditor C: 10–15 min
  - Roborev: 5–15 min per round (usually 1–2 rounds)
- **flow-address:** 30 min–1 hour per review round (re-gate + C + push + replies)
- **flow-finalize:** 10 min

### Context sizes
- **Lite gate output:** ~200–400 lines per report
- **Full gate SUMMARY block:** ~50–100 lines (machine-readable)
- **Spec audit report:** ~20–50 lines (per-requirement verdict)
- **Roborev findings:** variable (typically 3–8 issues per round)
- **Worktree path:** stable at `.claude/worktrees/issue-<N>-<slug>` (avoids context crawl from pathname lookups)

### Gate component times (from flow-implement doc)
- **fmt + file-size + clippy:** <3 min
- **core-tests (via cargo-nextest):** ~7 min (parallel across test binaries + cores)
- **write-support tests:** ~2 min
- **CLI tests:** ~3 min
- **minimal-features build:** ~5 min
- **smoke tests:** ~1 min
- Total full gate: 12–25 min depending on machine + load (gate slot cap #1825 may queue)

## Friction Points

1. **No required status checks on `main`** — lead cannot use `gh pr merge --auto` natively; must hand off to manager-owned poller/merge-engine or arm it manually (primary blocker for autonomous merge)

2. **Subagent stall-watchdog deadline (600s)** — full gate ≥ 12 min exceeds this; implementer must not run full gate (only lead does). #1855 codifies this division, but requires strict discipline.

3. **Gate may queue under #1825 cap** — up to 2–3 min wait before 12–25 min run; lead must use `run_in_background` or long Bash timeout (default 2-min timeout truncates a queued gate). No built-in signal that gate is queued vs hung.

4. **Claim protocol + concurrent lit gates** — lead runs serial full gates, but multiple concurrent `--lite` runs (e.g., two implementers) can spike load. Guidance says "serialize your OWN full-gate runs", but no automated brake on concurrent `--lite`. #1930 rule is manual.

5. **Data.db binaries not in worktrees** — every test run needs `CQLITE_DATASETS_ROOT` env var pointed at main repo (easy to forget, causes silent test skips). No automated validation that tests actually ran on real data.

6. **Large files (campsite rule ratchet)** — gate fails if a touched file exceeds threshold AND grows (advisory advisory on threshold breach, fail on growth). Splitting is the remedy, but requires git mv (triggers false-positives in the ratchet). Workaround: `CQLITE_ALLOW_FILE_GROWTH=1` to acknowledge.

7. **Project board + GitHub auth flip** — gh EMU account can silently lack `project` scope; every flow-* skill must run `gh auth switch --user "$project_account"` before board ops or writes degrade to labels (Path A #1886 fix, but manual step in every skill).

8. **OpenSpec + design-driven gate surge** — `flow-activate` proposes specs, but changes to those specs during `flow-implement` (e.g., scenario clarifications) are not re-validated automatically; spec-auditor reads them fresh, but a mid-loop spec tweak is not caught until spec-auditor runs.

9. **Roborev findings in long loops** — a review-first pass (optional, before full gate) catches issues early; a review-after-full-gate finding requires another full gate (slow iteration). No caching/reuse across rounds.

10. **API bucket throttling (GraphQL vs REST)** — `gh pr create`/`gh issue comment`/`gh project item-edit` ride GraphQL (5k pts/hr), separate from REST. Exhaustion requires manual fallback to `gh api` REST endpoints. No built-in retry/fallback in skills.

11. **Telemetry stamp (flow-finalize step 4)** — requires live GitHub API calls (issue timestamps, priority labels); a second ETL job would be simpler. Currently manual CLI tool (`scripts/delivery-telemetry.py record`) with ~8 optional counters — easy to miss/mis-count.

12. **Board drift on merged PRs** — server-side automation should move merged items to Status=Done, but if it fails there's no alert. Reconciliation is manual (flow-board reaper checks).

## Open Questions

1. **Autonomous merge gate:** should `gh pr merge --auto` be enabled on `main` once required status checks are configured? Currently deferred pending CI lane alignment.

2. **Subagent lite-gate parallelism:** can multiple implementers safely run concurrent `--lite` gates, or does the per-subagent `--lite` load need coordinated serialization (like the full gate)?

3. **Design-change mid-loop:** if a spec is clarified during implement, should spec-auditor re-validate all requirements, or only changed ones? Current: re-run full audit (no incremental).

4. **Roborev finding caching:** can a roborev session be "resumed" to re-check only changed files after an implementer fix, or must the whole branch be re-reviewed each round?

5. **Gate slot cap under concurrent implements:** issue #1825 caps FULL gates machine-wide; what is the correct cap value given the measured 12–25 min per gate + subagent 600s deadline? Current default: `max(2, floor((ncpu-2)/4))`.

6. **Spec-only changes in design-driven work:** if an implementer discovers a spec error post-approval and fixes it in the OpenSpec change (no code change), does the gate/C/roborev loop still run, or is it a trivial finalize?

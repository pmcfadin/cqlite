# AI-Native SDLC Redesign for CQLite

Date: 2026-09-04
Method: nine parallel read-only audits of the whole SDLC surface, measured against the AI-native SDLC playbook (https://claude.com/blog/the-ai-native-sdlc-playbook).

## The finding in one paragraph

CQLite has already solved the build problem.  The median time from issue to open pull request is about 4 hours.  The median time from open to merge is 34.2 hours.  So review and certification take 27 of those 34 hours.  The repo pays for that delay with 362,265 lines of process machinery, and the machinery does not earn it: the gate of record passes 98.5% of the time, so it almost never catches anything, while 71.4% of pull requests still need rework.  The rework comes from the review layer, not the gate.

## Measured surface

| Area | Size | Source |
|---|---|---|
| CI workflow YAML | 13,160 lines, 42 files | `.github/workflows/` |
| `.github/` non-workflow | 5,168 lines | `.github/` |
| Shell and Python scripts | 249,178 lines, 249 files | `scripts/` |
| `scripts/agent-gate.sh` alone | 23,837 lines, 39 components | `scripts/agent-gate.sh:6264` |
| `scripts/tests/` | 149,644 lines, 135 files | `scripts/tests/` |
| `.claude/` config | 14,830 lines, 27 skills | `.claude/` |
| OpenSpec artifacts | 79,929 lines, 108 specs | `openspec/` |
| `CLAUDE.md` | 687 lines | `CLAUDE.md` |
| Happy-path agent instructions | 2,333 lines | `CLAUDE.md` + `worker.md` + `flow-*` + `flow-lead`/`flow-closer` |
| ALL-CAPS imperatives in those files | 146 | `rg` over the same set |

## Measured delivery, from 590 records

Source: `docs/reports/delivery-telemetry.jsonl`.

| Measure | Value |
|---|---|
| Median cycle time, open to merge | 34.2 hours |
| Median issue to pull request | ~4 hours |
| Median time in review | ~27 hours |
| Pull requests needing rework | 71.4% (421 of 590) |
| Mean rework rounds per pull request | 3.32 |
| Mean roborev findings per pull request | 6.92 |
| Gate pass rate | 98.5% (581 pass, 8 fail) |
| Telemetry share of last 200 commits | 44% (88 commits) |

## Diagnosis

### The gate is the constraint, and it is serialized at one

`bootstrap-agent-machine.sh` persists `CQLITE_GATE_MAX_CONCURRENCY=1` (`docs/development/pm-operating-loop.md:146`).  The lite gate runs 1.4 minutes warm on a narrow diff, 20 minutes median on a `cqlite-core/src/` change, and up to 104 minutes under load.  The full gate runs 45 to 120 minutes.  One box therefore certifies two to five changes an hour at best, no matter how many agents run.  19 worktrees exist; a single author drives them.  The worktrees queue, they do not run in parallel.

### Merge authority sits on a laptop, not in CI

24 workflows fire on a pull request.  One is registered as merge-gating (the `flight` tier).  The other 23 are declared exempt in `.github/ci-gating-tiers.yml:156-518`, each deferring its "merge-gating half" to a component of the local `agent-gate.sh`.  GitHub has thousands of parallel runner-minutes available and the design declines them.  Meanwhile CI reruns the same work advisorily: `clippy` in 7 workflows, `cargo fmt` in 4, `cqlite-core` tests in 9.

### There is no fast inner loop

`make dev-check` is the advertised fast path.  It runs `cargo clippy --all-features --all-targets` undiffed plus `llvm-cov`.  Nothing in the Makefile reliably finishes in under five minutes.  The playbook's Test stage depends on the agent verifying its own work between edits; without a sub-two-minute check, every verification is a batch job, and the agent either waits or guesses.

### Prose does the work that hooks should do

The orchestration layer carries 146 ALL-CAPS imperatives across 2,333 lines of happy-path instruction.  It mandates exact return formats (`==== LITE SUMMARY ====`, at most 5 lines), a summary-file redirect so the agent never reads raw stdout, park-and-exit at every checkpoint, and re-verification after every change.  Each of these was written to stop a weaker model from drifting, inventing a verdict, or idling in a poll loop.  The playbook's rule applies: a skill or a prose rule is advisory, and a policy that must always hold needs a hook behind it.  The deterministic parts here are genuinely good and should stay: `scripts/flow/claim.sh` takes a server-side ref lock, and `premerge-assert.sh` fails closed.  The prose repetition around them is dead weight.

### The one hook that exists cannot fail

`.claude/settings.json:59-71` registers a `TaskCompleted` hook that runs `agent-gate.sh --lite` with a 600-second timeout and fails open.  Its own comments record it as "defused for #2671."  So the only hook in the repo is advisory, and the real control is a 12-to-104-minute batch job at the end.

### Bookkeeping outweighs product change

44% of the last 200 commits are `chore(#NNNN): delivery telemetry record for PR #NNNN`, each landing as its own pull request.  The ledger is read by exactly one consumer, `scripts/delivery-telemetry.py retro`, which files a `flow-meta` issue for the top recurring failure.  That consumer does not need one commit per delivery.

### Dead and duplicated machinery

| Item | Lines | Status |
|---|---|---|
| `.github/issue-monitoring/` | 2,321 | No CI job invokes it; README claims a 6-hour schedule |
| `.github/test-quality-gates.sh` | 593 | Manual only |
| `.github/hooks/pre-commit` | 755 | Opt-in, duplicates CI |
| 5 orphan `e2e_*` scripts | 1,887 | Zero callers |
| `tarpaulin.toml` | 33 | Two conflicting floors (35.0 and 80.0); nothing reads it |
| `make coverage` | n/a | Calls `scripts/coverage.sh`, which does not exist |
| `start-epic`, `pm-status` skills | 35 | Marked deprecated |
| `.agents/skills/` (8), `.codex/skills/` (5) | n/a | Duplicate `.claude/skills/` with no declared source of truth |
| `#[ignore]` tests | 149 attributes | Nothing runs them |

`.pre-commit-config.yaml` runs `cargo test --all-features --workspace` on every commit, and declares `fmt` and `clippy` twice: once as local hooks, again through `doublify/pre-commit-rust`.

### One governance note in the other direction

Merge requires zero human approvals and one status check (`.github/branch-protection.json:5,10`).  The playbook holds that the agent which wrote the code must have no route to approve it.  Today it does.  That is a risk decision, not a productivity problem, and it belongs to the owner.  Do not fix it by adding latency everywhere else; fix it with one code-owner approval informed by review findings.

---

## Prescriptions, in order

Each entry names the change, the constraint it removes, and how to tell it worked.

### 1. Build a diff-scoped `make check` that finishes in under two minutes

Add one target that formats, runs `clippy` on the changed packages only, and runs the unit tests reachable from the changed files.  Exit non-zero on failure.  Record it in `CLAUDE.md` under Commands with an example of healthy output, per the playbook's Test stage.

Removes: the absence of any inner loop.  This is the prerequisite for everything else, because an agent that can verify in 90 seconds does not need the 20-minute lite gate between edits.

Verify: `time make check` on a one-file change in `cqlite-core/src/`.

### 2. Move the gate of record into CI and run its 39 components in parallel

Register the `agent-gate.sh` components as gating tiers in `.github/ci-gating-tiers.yml` instead of exempting 23 workflows that defer to a local script.  Each component becomes a job.  GitHub runs them concurrently.  Delete `CQLITE_GATE_MAX_CONCURRENCY=1` because it stops mattering.

Removes: the one-gate-per-box serialization and the 45-to-120-minute wall-clock full gate.  Wall clock becomes the slowest single component, not the sum.

Verify: elapsed time on the `required` aggregator for a `cqlite-core` change, before and after.

### 3. Cap and parallelize the review loop

Write `REVIEW.md` at the repo root with the playbook's structure: named passes (bugs, security, spec compliance against `openspec/` artifacts), an explicit Important-versus-nit definition, a cap of five nits per review with the remainder reported as a count, and an instruction to skip generated paths and anything CI already enforces.  Run `rust-reviewer` and `roborev` concurrently rather than in sequence (`flow-implement/SKILL.md:145-172`).

Removes: the 27-hour review phase.  6.92 findings times 3.32 rework rounds is where the time goes, and an uncapped nit stream is what makes each round expensive.

Verify: mean `roborev_findings` and mean `rework` per record in `delivery-telemetry.jsonl`, sampled monthly.  Both should fall.

### 4. Feed repeat findings into CLAUDE.md, then cut CLAUDE.md to about 150 lines

Adopt the rule directly: when a mistake appears a second time, the correction goes into `CLAUDE.md` as part of that review.  Then delete the 250 lines of process ceremony that already exist elsewhere.  Specifically: lines 87-130 duplicate `docs/development/gate-ops.md`; lines 541-609 and 627-688 duplicate `docs/development/pm-operating-loop.md`; lines 52-86 list skills that the harness already lists.  Move lines 300-415 (the five testing blind spots) into `docs/development/testing-oracle-guide.md` and link to it.

Keep: project status, the Commands block from step 1, workspace structure, development standards, format authority, and the blind-spot link.

Removes: context spent on instructions the agent does not need at session start.  Stale or duplicated content in `CLAUDE.md` costs context for no return.

Verify: `wc -l CLAUDE.md` under 200, and no product change to the delivery numbers.

### 5. Replace prose enforcement with hooks, and let one hook actually block

Convert the rules that must always hold into `PreToolUse` hooks in `.claude/settings.json`: block edits to test files during a fix task, block edits to protected paths, block a commit whose diff has no accompanying test.  Make the `TaskCompleted` hook run the new `make check` from step 1 and fail closed, because a 90-second check can afford to block where a 600-second one cannot.

Then delete the compensating prose: the mandatory return-format contracts, the summary-file redirect instructions, the repeated ALL-CAPS imperatives, and the park-and-exit choreography.  Keep `claim.sh` and `premerge-assert.sh`; those are real controls.

Removes: roughly 1,500 of the 2,333 happy-path instruction lines, and the drift they were written to prevent.

Verify: the hook blocks a deliberate test-file edit during a fix, and the flow still completes an issue end to end.

### 6. Delete the dead machinery

In one pull request, remove: `.github/issue-monitoring/` (2,321), `.github/test-quality-gates.sh` (593), `.github/hooks/pre-commit` and `.github/setup-quality-gates.sh` (901), the five orphan `e2e_*` scripts (1,887), `tarpaulin.toml`, the broken `make coverage` target, and the `start-epic` and `pm-status` skills.  That is about 5,800 lines.

Separately, declare one source of truth for skills.  `.claude/skills/` holds 27, `.agents/skills/` holds 8, and `.codex/skills/` holds 5 copies of the OpenSpec family.  Pick one directory and make the others symlinks, or delete them.

Then rule on `scripts/tests/`: 149,644 lines of bash that test bash, roughly 80 of which run inside the `tooling-tests` gate component (`scripts/agent-gate.sh:6650`).  Either move that component to nightly, or accept that it is a first-class part of the merge path and budget for it.  Do not leave it implicit.

Removes: maintenance surface, and gate minutes in the `tooling-tests` case.

Verify: `agent-gate.sh` still passes, and the deleted paths appear nowhere in `rg` output.

### 7. Stop committing one telemetry record per delivery

`delivery-telemetry.py retro` is the only consumer.  Append the record inside the merge commit, or batch the day's records into one nightly commit.

Removes: 44% of commit volume and one pull request per delivery.

Verify: `git log --oneline -200 | rg -c 'delivery telemetry'` trends toward zero while `retro` still files its `flow-meta` issue.

### 8. Resolve the 149 ignored tests and the coverage story

Each `#[ignore]` is either a real gap or dead code.  Add a nightly lane that runs `cargo test -- --ignored` and reports, so the count becomes visible instead of invisible.  Pick one coverage tool (the gate already uses `llvm-cov`), commit a baseline, and enforce a floor in CI.  Delete `tarpaulin.toml`.

Removes: an unmeasured hole under a coverage number that nothing enforces.

Verify: the nightly lane reports a count, and the floor fails a deliberate coverage drop.

---

## Sequencing

Steps 1 and 6 depend on nothing; start there in parallel.  Step 2 depends on step 1 only for confidence.  Steps 3 and 4 are the largest win on cycle time and should follow immediately.  Step 5 depends on step 1, because a blocking hook needs a fast check behind it.  Steps 7 and 8 are cleanup and can land any time.

## What is already right, and should not be touched

- Server-side ref locking for issue claims (`scripts/flow/claim.sh`).  Atomic arbitration, not optimism.
- `premerge-assert.sh` failing closed on stale base and `HOLD:` markers.
- The permission allowlist.  51 global and 71 local rules pre-approve cargo, git, gh, and jq, so an agent does not stall on prompts.
- Worktree isolation, one per issue.
- The committed artifact chain.  `openspec/changes/<name>/` already holds proposal, design, specs, and tasks in git, which is the playbook's central mechanism, built before the playbook named it.
- Fresh-context verification.  `spec-auditor`, `rust-reviewer`, and `coverage-reviewer` are read-only reviewers in their own context windows.  That is the pattern the playbook recommends.

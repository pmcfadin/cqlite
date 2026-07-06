# Agentic Delivery Workflow Audit — Building for Efficiency and Quality with Claude Code Across Multiple Machines

**Date:** 2026-07-06 · **Method:** 8 haiku gatherers (one per subsystem) → 3 opus synthesizers (speed / quality / context-economy) → lead synthesis.
**Evidence base:** `docs/reports/delivery-telemetry.jsonl` (174 issues, 2026-06-28→07-06), all 41 CI workflows, `scripts/agent-gate.sh` (2,707 lines), 7 agent definitions + 6 flow-* skills, Docker parity infra, prior throughput audits.
**Scratch (raw gathered data):** `docs/scratch/agentic-workflow-audit/` — 8 subsystem fact files + 3 theme syntheses.

---

## 1. Executive summary

The machinery is fast and the coordination is sound. Three numbers describe the whole system:

| Measurement | Value | Meaning |
|---|---|---|
| Median PR-open → merge | **16.2 min** | The tail of the pipeline is essentially solved |
| Median issue-created → PR-open | **29.4 h** (design: 91 h) | ~99% of cycle time is waiting to start, not working |
| Gate first-time pass rate | **~54%** (151 of 325 runs fail) | The fix-loop, not gate wall-time, is the active-work cost |

Five structural findings, in order of importance:

1. **Context bloat has one root cause: the only long-lived agent is the one doing the heaviest reading.** The lead runs the full gate and roborev in its own context — the two largest text streams in the pipeline — and never resets between issues. Everything else (all 7 specialists) is already a fresh spawn. Fix the lead's diet and the bloat problem is fixed.
2. **The quality chain has a redundancy inversion.** fmt/clippy/unit run 3× per issue; the flagship byte-for-byte correctness runs 0× pre-merge (nightly only, advisory). And a green full-gate SUMMARY can silently mean *zero parity was validated* (dataset SKIP → overall PASS).
3. **Review lands after the gate, so review findings re-buy the gate.** Issues with roborev findings average 2.71 rework rounds vs 0.17 without. Reordering review before the first full gate converts multi-gate issues toward single-gate.
4. **The full gate carries ~150s of removable cost every run** (node-bindings LTO build; 4× redundant 33-table binding-parity sweeps) — ~10–17% of every one of 325 runs.
5. **Multi-machine coordination works (0 claim collisions in 174 issues) but is lock-only** — no heartbeat, no shared fleet view; abandoned claims are reaped by guesswork.

---

## 2. The context-bloat diagnosis (the owner's stated pain)

### What actually floods a long-running session

| Source | Size per occurrence | Occurrences per issue |
|---|---|---|
| Full-gate stdout (when not redirected) | thousands of lines | 1–9 runs (median 1.5, p90 3) |
| roborev rounds (diff + findings) | 3–8 findings/round, max 40/issue | p90 3 rounds, max 9 |
| Implementer fix-round reports (raw lite output) | 200–400 lines each | 1–5 rounds |
| Seam-1 spec render (verbatim, retained after approval) | full spec body | 1 (design work) |
| Board renders, PR bodies, manager comments | small each | accretes across N issues |

The doctrine already names the anti-pattern — `flow-implement` step 4: *"you do not read source, write code, or run the gate in your own context"* — and then **steps 6 and 8 violate it** (full gate + roborev run in lead context). The cause is real: a subagent idle-waiting on a 12–25 min gate hits the 600s stall watchdog and its death kills the gate process (#1855). But the escape (`run_in_background`; harness re-invokes on exit) exists and just isn't the mandated path.

### The fix: stage-scoped agents + disk-mediated handoffs (context-budget discipline)

**Principle: no agent's context should outlive its stage; every stage's output is a small file + a ≤15-line packet.** All durable state already lives outside context (worktree, origin claim branch, issue/PR bodies, OpenSpec files, `.agent-gate-summary.txt`, telemetry ledger, board Status) — the gap is purely that the lead carries live copies instead of re-reading.

| # | Change | Payoff | Cost |
|---|---|---|---|
| C1 | **New `flow-closer` subagent owns gate→C→roborev→merge.** Runs the full gate with `run_in_background` (no idle-wait → no watchdog kill), spawns spec-auditor, drives roborev to clean, merges on green, returns only `{verdict, PR URL, summary-file path, ≤10 lines residual}`. All gate stdout and review churn die with its context. | Removes the two largest accretion sources from the persistent session; also resolves the #1855 tension | One agent definition + one hop/issue |
| C2 | **Summary-file redirect becomes the default gate invocation everywhere**, not recovery: `AGENT_GATE_SUMMARY_FILE=… bash scripts/agent-gate.sh > gate.log 2>&1` then `cat` the summary. Forbid raw gate stdout in any persistent context. | Caps per-gate context at ~50–100 lines vs thousands | Two doc edits |
| C3 | **Inter-issue context reset for the lead.** After each finalize: write a one-line disk ledger entry; re-hydrate the next item from the board alone. A session must be re-runnable from board state — which the disk already guarantees. Cross-issue lessons go to memory files, not the live window. | Bounds a session at O(1 issue) instead of O(N) | Doctrine change |
| C4 | **CLAUDE.md diet:** extract ~250 lines of gate-ops prose (sccache tuning, #1825 cap internals, disk hygiene, --delta mechanics) to `docs/development/gate-ops.md`, loaded only by lead/closer. Every one of the 7 specialists currently pays this 6,764-word tax per spawn for content they never use. | Materially cheaper every spawn, forever | One refactor + pointer line |
| C5 | **Cap the implementer return contract:** LITE SUMMARY block (~15 lines) + ≤5 lines prose per round; never the 200–400-line raw output. | Strips ~1–2k lines/issue of fix-round noise | One-line edit |
| C6 | **Drop the retained Seam-1 spec copy** after owner approval — spec-auditor re-reads it from `openspec/changes/<slug>/` anyway. | Frees the largest single design-issue artifact | Doctrine note |

This is the general Claude Code pattern worth internalizing: **the orchestrator is a router, not a reader.** Long-lived context is the scarcest resource in the system; fresh spawns, disk files, and git refs are nearly free. Every byte a persistent session reads should have to justify why it isn't a file path handed to a fresh agent.

---

## 3. Quality: what actually catches defects vs theater

### The chain as measured

- **roborev is the workhorse by volume** — 608 findings/174 issues, driving 334 rework rounds. Also the top weighted cost driver, because findings carry **no severity**: a nit forces the same re-gate as a bug.
- **The deterministic layer (fmt/clippy/unit) catches most gate failures but runs 3×** (lite each round, full gate, pr-gate CI) — maximum redundancy on the cheapest signal.
- **C (spec-audit) runs on 27% of issues, post-gate, serial, and uninstrumented** — no telemetry records its unmet-verdict rate, so there is no evidence it catches anything the gate/roborev miss. Strongest theater candidate; measure before keeping.
- **The crown jewels (byte-for-byte compaction parity, live readback, exhaustive regeneration) never block a merge** — nightly/weekly, advisory, catching defects ~24h post-merge.
- **The gate of record is honor-enforced.** The only required CI check (`pr-gate.yml`) runs a thin fmt/clippy/core-lib subset with 4 test groups skipped and zero integration/parity coverage. Merge trusts a pasted SUMMARY block nothing verifies.

### The one dangerous hole (fix first)

**A green full-gate SUMMARY can mean zero correctness was validated.** In a worktree without the gitignored `Data.db` binaries and no `CQLITE_DATASETS_ROOT` override, parity/smoke/compaction components SKIP — and the gate still returns overall **PASS**. Fix: fail-closed on the gate of record — full (non-lite) runs FAIL, not SKIP, when datasets are absent (~15-line script change). `--lite` stays lenient.

### Rebalancing (same assurance, cheaper)

| # | Change | Payoff |
|---|---|---|
| Q1 | **Fail-closed full gate on absent datasets** (above) | Kills the worst silent pass |
| Q2 | **Severity-stratify roborev; gate only on blockers**, nits ride to follow-ups | Directly cuts the #1 cost driver (334 rework rounds) |
| Q3 | **Scoped re-verify per roborev fix round** (`--lite` + diff-scoped parity), ONE full gate immediately pre-merge | Removes most of the 151 failed/re-run full gates |
| Q4 | **Promote smoke-parity to a path-filtered required CI check** (storage/write/parser paths only) and commit the required-check list to the repo | The only thing that would actually block a correctness regression today |
| Q5 | **Path-filtered pre-merge compaction-parity on write-path diffs** (prebuilt Cassandra+JDK image) | Flagship claim enforced when it can break, not 24h later |
| Q6 | **Instrument C's catch rate** (unmet-verdict counter); demote to a roborev checklist if ~0 over ~30 design issues | Removes a 10–15 min serial blocker if it's theater |

---

## 4. Speed: where the remaining minutes are

1. **Reorder the loop: review BEFORE the first full gate** (`lite → reviewer/roborev on lite-green diff → fix → FULL gate → merge`). With 47% of gate runs failing and rework concentrated in reviewed issues, the expensive gate should run on already-reviewed code. Estimated ~35–50 fewer full gates per 174 issues.
2. **Gate diet:** drop the node-bindings LTO (`release-unwind`) build from the full gate (~150s/run, ~10%; the unwind firewall keeps its self-test + nightly), and compress the 4× 33-table binding-parity sweep to conversion-boundary representatives with a nightly full sweep (~250s → <60s). *The binding-parity compression has been a pending owner decision since 2026-07-03.*
3. **Extend `--delta` to execute node `__test__/` and `scripts/tests/*.sh`** so polish rounds on those files stop forcing full gates (the #1853/#1921 pattern burned 2–3 gates each).
4. **De-flake as contention-hardening:** root-cause sccache's under-load corruption (2026-07-06 incident) and rewrite #1776's wall-clock assertion (load-dependent flake in a feedback loop with re-runs). 69 false-red events logged across the flaky set.
5. **The real ceiling is backlog latency, and it's owner-shaped:** 29.4h median wait before work starts (91h for design work awaiting Seam-1 approval). No machinery change touches this — the lever is batching spec-approval sessions so a Ready buffer always exists. Partly a quality investment (Seam 1 is sacred), but worth making the wait deliberate rather than incidental.

---

## 5. Multi-machine model

What works — keep it:
- **The origin `issue-<N>-*` branch as the claim lock**: 0 collisions in 174 issues. Race-safe, cheap, cross-machine.
- **One worker per machine (#1930)**: load and worktree isolation demand it; subagent fan-out within the worker is the right parallelism.
- **Board as sole dispatch authority (Path A)**: unambiguous, and the STOP-on-unreachable rule prevents stale-label grabs.

What to add:
- **Claim heartbeat + shared fleet ledger.** Today abandoned `In Progress` detection is "no recent commits" guesswork and `.worker-state` is machine-local. Elevate to a cheap origin-tracked ref carrying `(issue, machine, timestamp)`; `flow-board` reaps deterministically on stale heartbeat. Keep it a git ref update, not an API call (API bucket pressure is already a known friction).
- **Per-machine roles at fleet scale:** with the closer pattern (C1), a natural split emerges — machines run interchangeable claim-protocol workers; the only singleton is the owner-facing lead, which any machine can host because session state is fully board/disk-rehydratable (C3).

---

## 6. Ranked action plan

**Tier 1 — cheap, high leverage, no owner tradeoffs (groomable now):**
1. Q1 fail-closed gate on absent datasets (~15 lines)
2. C2 summary-file redirect as default (doc edits)
3. C5 implementer return cap (one line)
4. Speed-3 `--delta` node/shell-test executors
5. C4 CLAUDE.md gate-ops extraction

**Tier 2 — structural, medium effort:**
6. C1 `flow-closer` agent + C3 inter-issue reset (the context-bloat fix proper)
7. Speed-1 review-before-gate loop reorder
8. Q3 scoped re-verify per roborev round
9. Q2 roborev severity stratification
10. Multi-machine heartbeat ref

**Tier 3 — NEEDS-YOU (product/process decisions, not autonomous):**
- Binding-parity compression in the gate (pending since 2026-07-03) + node LTO removal — both trade per-gate coverage for a nightly backstop
- Q4/Q5 required-CI promotion — reintroduces bounded CI wait that merge-on-green removed (path-filtered)
- Q6 C-audit instrumentation → possible demotion after data
- Backlog/Seam-1 cadence — batch spec approvals to keep a Ready buffer

---

*Raw evidence: `docs/scratch/agentic-workflow-audit/` — `gate-and-scripts.md`, `testing-suite.md`, `docker-correctness.md`, `github-actions.md`, `agents-and-skills.md`, `doctrine-and-process.md`, `telemetry-analysis.md`, `retros-and-audits.md`, plus `synthesis-{speed,quality,context-economy}.md`.*

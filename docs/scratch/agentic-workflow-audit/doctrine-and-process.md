# Agentic Delivery Workflow Audit — Doctrine and Process

## Inventory

**Prescribed system**: Manager + worker agents + GitHub Project board (v2) + open-source OpenSpec for design-driven work.

### Components
- **Primary doctrine** — `docs/development/pm-operating-loop.md` (186 lines, definitive)
- **Agent setup** — `docs/development/agent-machine-setup.md` (85 lines, bootstrap 1-command)
- **Execution playbook** — `docs/development/ISSUE_EXECUTION_PLAYBOOK.md` (322 lines, oracle-first issue templates with acceptance criteria)
- **Process improvements** — `process_improvements.md` (191 lines, telemetry-driven lever tracking + activity log)
- **Website doctrine** — `website/src/content/docs/agents-developing/` (5 pages: `delivery-pipeline.md`, `gate-contract.md`, `spec-driven-audit.md`, +2)
- **Gate** — `scripts/agent-gate.sh` (the gate is THE gate, never ad-hoc cargo)
- **Telemetry** — `docs/reports/delivery-telemetry.jsonl` + `scripts/delivery-telemetry.py` (append-only ledger; schema in `.schema.json`)

### Two human seams
1. **Seam 1 — spec approval** (design-driven only): owner approves the OpenSpec spec + design in `flow-activate`; STOP until approved.
2. **Seam 2 — merge** (ELIMINATED for workers): replaced by **merge-on-green** (manager-owned poller watches defined green signal; worker arms and stops; no CI busy-wait).

## How it Works

### Worker lifecycle (1:1:1:1 — one issue per worktree/branch/PR/OpenSpec change)
1. **Pick up** oldest `Status=Ready` issue with no `issue-<N>-*` branch lock
2. **Read manager orders** (signed comment with `🧭 MANAGER` marker; note `HOLD`/`ORDER`)
3. **Route** — design-driven → `flow-activate` FIRST (OpenSpec proposal/design), oracle-driven → straight to `flow-implement`
4. **Implement** via subagents (worker orchestrates; subagent does the work + `--lite` gate only)
5. **Terminal state** — PR-open + `agent-gate.sh` PASS + spec-auditor C PASS (design) + roborev clean
6. **Arm merge-on-green** and STOP (no CI busy-wait; the mechanism lands the PR on green)
7. **Finalize post-merge** — archive OpenSpec, stamp telemetry ledger, remove worktree/branch

### Gate contract (`scripts/agent-gate.sh`)
- **Tiered**: `--lite` for iteration (~1–5 min), FULL gate runs exactly once before merge
- **Components** (all run, fail-fast omitted): fmt, clippy (scoped per-package #1844), core-tests (nextest #1737), integration-tests, write-tests, cli-tests, minimal-build, smoke
- **Accelerators** (auto-detected, loud on missing #1848): sccache (25.6% on fresh), nextest (2–4× core-tests), parallel lanes (bash ≥4.3)
- **Machine-wide concurrency cap** (#1825): at most N full gates at once; excess queue; SIGKILL-safe stale-slot reaping
- **Delta re-cert** (#1892): test/docs-only rounds after a full PASS re-cert with `--delta` (fail-closed on any src change)
- **Datasets** (gitignored, must be present): fetch once to main checkout; gate aborts if missing

### Claim protocol (cross-machine lock)
1. Check `git ls-remote --heads origin "issue-<N>-*"` — skip if exists
2. Push `issue-<N>-<slug>` branch to origin (server-side lock; first wins)
3. Set assignee `@me` + `Status=In Progress`
4. Confirm your commit is on origin; proceed only if you won the race

### Board dispatch (Path A #1886)
- **Sole authority**: GitHub Project `Status` field (Backlog → Ready → In Progress → In Review → Done)
- **Never labels**: `status:*` labels are decorative; board unreachable → STOP, fix auth
- **Empty Ready** = no work ready (intentional), not a cue to dredge labels

### Concurrency: one worker per machine (#1930)
- **Default**: one `flow-lead` worker owns the machine; spawns subagents (cheap); serializes full `agent-gate.sh`
- **Subagent model**: worker fans out implementation + reviews (read-only can overlap); full gate stays serial (concurrency=1)
- **Multiple independent sessions** (discouraged): claim protocol + branch-lock are mandatory; must use separate machines
- **Never N bare flow-leads on one box** — they collide on the same Ready item

## Measured/Observed Costs

### Telemetry (n=91 issues; session retro 2026-07-03)
- **Backlog wait** (created → PR): median 19.6h, mean 87.7h (not active work)
- **Active pipeline** (PR → merge): median 0.8h, mean 2.3h (fast once claimed)
- **Total cycle**: median 24.3h, mean 90.0h

### Failure cost ranking (weighted by 4/2/5/2/3)
| Category | Count | Score | Notes |
|----------|-------|-------|-------|
| rework | 220 | 880 | iteration churn (4.7/issue avg) |
| roborev_findings | 430 | 860 | structural findings (avg 4.7/issue) |
| gate_failures | 69 | 345 | full-gate re-runs per issue (1.8× avg) |
| rebase_events | 107 | 214 | conflicts on move |
| claim_collisions | 0 | 0 | branch-lock working (no throughput lost) |

### Gate wall-clock (warm checkout, post-#1737/#1822)
- **Before levers** (#1737/#1822): 17.3 min (core-tests 67% of floor)
- **After sccache** (#1822): 27.3 min on fresh (25.6% saved), 100% cache hit
- **After nextest + parallel** (#1737): ~4.3 min on warm (63% off baseline 697s)
- **Machine-wide concurrency cap** (#1825): prevents SIGKILL under load 30–60; queuing is clean

## Friction Points

### 1. Telemetry is append-only, no real-time signals
- **Cost**: measurement lag (retro runs post-hoc on completed issues); improvements are hypothetical until ledger grows
- **Caveat**: "a counter not observed is an error, never a fabricated 0" is fragile in practice — requires explicit `record` calls in finalize; missing finalize = missing ledger entry
- **Workaround**: standing nightly retro run + automated issue filing for top failure

### 2. Pre-gate CI is surfaced as nightly only (#1269)
- **Cost**: false-green PRs for feature-gated tests (cli-helpers features not caught by light PR check #1360)
- **Status**: `.github/workflows/gate.yml` deep-check is nightly + `workflow_dispatch` only; no required per-PR
- **Risk**: 24h feedback lag on component breaks (e.g., node-bindings compile) if not run locally

### 3. Gate tiering requires subagent discipline (#1855)
- **Cost**: if subagent invokes full gate (waits 12–20 min), it blocks the worker until gate times out and kills the child
- **Enforcement**: documented (doctrine), not enforced in code; relies on human training
- **Mitigation**: stall watchdog kills idle subagents, but gate process orphaning is messy

### 4. Delta re-cert scope verification is manual (#1892)
- **Cost**: `--delta` FAILs CLOSED on any src change, but list of safe files is implicit (tests/*/*, docs/*, website/*)
- **Friction**: no lint enforces which changes can be delta-certified; easy to miscategorize a file and force a full re-gate
- **Workaround**: `--delta` itself refuses and names files; user must re-read the error

### 5. Merge-on-green is manager-owned poller (today, temporary)
- **Cost**: worker arms PR but does not merge; requires manager infrastructure (separate poller session)
- **Status**: INTENDED TEMPORARY: until `main` has required status checks, then `gh pr merge --auto` (zero-token native)
- **Current**: green-signal guard forbids `--auto` against empty required-check set; `main` has `contexts=[]`
- **Implication**: if manager poller dies/stalls, PRs age waiting for nothing

### 6. Process improvements are hypothesis-first, result-later (#1821/#1822/#1825/#1737)
- **Cost**: levers are proposed with measured hypotheses but results are TBD (entries live on process_improvements.md)
- **Timing**: measurement window required before declaring a lever "worked" — early wins are not retroactively confirmed
- **Example**: "full-gate-runs/issue should fall" is the hypothesis; ledger must grow before confirming (n=91 baseline, next baseline TBD)

### 7. `CQLITE_DATASETS_ROOT` must point to MAIN checkout (gitignored binaries)
- **Cost**: every worktree must point to the same shared main-tree dataset dir; if fetch is missing, gate aborts
- **Friction**: cross-machine setups require explicit env var export; easy to forget on ssh/remote-control setups
- **Mitigation**: `bootstrap-agent-machine.sh` checks and prints guidance; gate fails loudly if missing

### 8. Spec-driven audit (C) is post-gate, linear blocker
- **Cost**: auditor runs only after gate is green; a finding that requires src change blocks merge (no parallel auditing)
- **Mitigation**: optional B (roborev design review) can run alongside for high-stakes; still requires C to pass

### 9. Fleet concurrency requires one-per-machine discipline
- **Cost**: if two workers run on the same box, they serialize on full gate (contention) and risk SIGKILL
- **Status**: documented doctrine; no enforcement (no file lock preventing second worker from starting)
- **Reliance**: human discipline + stall watchdog (kills idle subagents but not a main worker)

## Open Questions

1. **Green-signal guard trade-off**: is the temporary manager-owned poller the right interim? (#1269, #1360)
   - Alternative: land a required check on `main` immediately so `gh pr merge --auto` works (native zero-token path)
   - Current blocker: unclear what required check is acceptable today

2. **Full-gate-once rule enforcement**: how hard is the "full gate runs EXACTLY once before merge" rule? (#1821)
   - Documented: yes. Code-enforced: no.
   - Observed: process_improvements.md tracks the hypothesis, but post-implementation ledger comparison is pending

3. **Telemetry ledger as single source of truth**: how to handle missing records (unfinalized work)?
   - No-heuristics mandate says "missing = error, never fabricate 0"; unclear how to surface missing finalize calls without periodic ledger audit

4. **Scoped clippy scope drift (#1844)**: per-package feature lists can drift; nightly full pass is the net. Is drift acceptable?
   - Mitigation is nightly (24h lag); daily linting would catch sooner but adds per-machine cost

5. **Merge seam in flux**: is merge-on-green ready for stable doctrine, or still experimental?
   - Status: deployed (2026-07-04 owner decision to merge autonomously); not yet in website doctrine (TODO: update delivery-pipeline.md §2)

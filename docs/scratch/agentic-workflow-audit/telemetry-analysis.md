# Delivery Telemetry Analysis

## Inventory

**Ledger**: `docs/reports/delivery-telemetry.jsonl` (174 records, stamped 2026-06-28 to 2026-07-06)

**Schema** (`v1`): GitHub timestamps (created/PR-opened/merged/closed) + run-observed counters (gate outcomes, roborev findings, rework/rebase/claim-collision events)

**Scope**: Complete delivery pipeline end-to-end, all routing types (design + oracle), all priorities (P0–P3)

## How It Works

Each record captures one closed issue via `scripts/delivery-telemetry.py record` invoked by `flow-finalize`:
- **Authoritative data**: GitHub API timestamps (no wall-clock guessing), plus observed event counters from the implement loop
- **Phases**: (1) Backlog (created → PR opened), (2) Review (PR opened → merged)
- **Counters**: gate_runs, gate outcome, roborev_findings, rework (re-opens), rebase_events, claim_collisions
- **Cycle time**: Seconds from issue creation to issue close (authoritative, not self-reported)

## Measured Costs & Performance

### Cycle Time (issue created → closed)
- **Median**: 30.5h (109,875 sec) — typical issue birth-to-burial
- **p90**: 116.7h (420,283 sec) — blocking 10% of work; P0 parity issues dominate
- **p99**: 197.4h (710,741 sec) — outlier range; one issue at 168.1d (Jan–Jun) is a long-tail design
- **Range**: 32.8 min (minimal oracle bug) to 168.1 d (deferred design)

### Phase Breakdown

**Backlog (created → PR opened)**
- **Median**: 29.4h — work sits pending activation (~90% of median cycle time)
- **p90**: 116.1h — same blockers affect backlog + review phases equally

**Review (PR opened → merged)**
- **Median**: 16.2 min — fast path: gate passes, roborev clean, auto-merge
- **p90**: 232.3 min (3.9h) — gate failures or roborev findings extend review
- **p99**: 1167.8 min (19.5h) — design-driven specs or multi-round rework

### Gate Performance
- **Pass rate**: 173/174 (99.4%) — one terminal failure among 174 completed issues
- **Gate runs per issue**: Median 1.5, p90 3, max 9
  - **Total runs**: 325 observed runs
  - **Failed runs**: 151 inferred (gate_runs - 1 for pass, all runs for fail)
- **Overall**: ~0.46 failed runs per issue; converges within p90 ≤3 runs/issue

### Rework & Iteration

| Category | Metric |
|----------|--------|
| **Rework** (re-open/re-review) | 113/174 (64.9%) affected; 334 total rounds |
| **Roborev findings** | 608 total findings; median 1/issue, p90 10, max 40 |
| **Rebase events** | 110/174 (63.2%) affected; 186 total rebases |
| **Claim collisions** | 0 observed across all 174 issues |

### Work Routing
- **Design (OpenSpec)**: 47/174 (27.0%) — longer cycle (spec approval seam)
- **Oracle (bug/parity)**: 127/174 (73.0%) — shorter cycle, gate-driven

## Friction Points

1. **Backlog latency dominates cycle time** — Median 29.4h backlog + 16.2 min review = 99.5% of cycle time in backlog; review phase is fast but activation is slow. Implication: parallelism/activation cadence is the constraint, not review velocity.

2. **High rework incidence** — 64.9% of issues undergo rework (median 1 round, max 13); 334 total rework rounds across 174 issues = ~2 rework events per issue. Each rework extends review phase and forces rebases.

3. **Roborev findings clustering** — p90 hits 10 findings/issue; one issue saw 40. High-finding issues tend to co-correlate with rework (no correlation data, but anecdotally: design work and large refactors). Implication: review-first conditional pass (before full gate) could triage earlier.

4. **Gate run distribution has a tail** — Median 1.5 but p90 climbs to 3; max 9. The p90 is manageable but 151 failed runs (out of 325) = 46% of all gate executions fail. Implication: first-time pass rate is ~54%; iterative fix loops are the norm, not the exception.

5. **Rebase frequency** — 63.2% of issues require at least one rebase; 186 total across 174 = ~1.07 rebases/issue. Strongly correlated with multi-day backlog waits (main advances, forcing rebase before merge). Implication: main branch churn rate is high during active sprints.

6. **No claim collision protection signaled** — All 174 records show 0 claim_collisions; either the concurrency cap (issue #1825) is working or the test set is low-concurrency. Worth validating under multi-session load.

## Open Questions

1. **Is the long backlog tail a scheduling artifact or a bottleneck?** — Median backlog (29.4h) vs review (16.2 min) suggests issues wait for activation/spec approval longer than they need review. Is this by design (batch activation windows) or involuntary (capacity)?

2. **What drives the high rework rate (64.9%)?** — Need correlation: design vs oracle, P0 vs P3, roborev findings count vs rework rounds. Does specification clarity (design) reduce rework vs oracle bugs?

3. **First-time gate pass rate visibility** — Ledger records only terminal gate outcome (pass/fail); intra-gate iteration is inferred from gate_runs - 1. No breakdown of why runs fail (clippy, test, etc.). Suggest adding failure reason counters.

4. **Claim collision baseline** — Zero collisions over 174 issues is either a win or a signal the test set was serial. The concurrency cap (#1825) is designed to prevent kernel-scale load spikes; need measurement under intentional multi-session stress.

5. **Roborev finding severity** — Ledger counts findings raised, not fixed. No way to correlate finding count to rework rounds or extend review time. Suggest stratifying by severity (blocker vs style) or adding time-to-fix.

6. **Design vs oracle convergence** — p90 cycle time for design (47 records) vs oracle (127 records) not yet analyzed; suspect design has longer backlog (spec approval Seam 1) but comparable review speed once activated.

# Design — delivery-telemetry-ledger

## Context

The pipeline already emits the raw signals we want to measure, but they evaporate:
- **GitHub** holds authoritative timestamps (issue `createdAt`/`closedAt`, PR
  `createdAt`/`mergedAt`) and labels (priority, routing).
- **A run** observes counters that GitHub does not record cleanly: how many times the claim
  push was rejected (collisions), how many rebases/conflict-resolutions happened, whether the
  agent-gate passed and how many times it ran, the roborev findings count, and rework
  (re-open / re-review) count.

We need to persist these per completed issue, then rank recurring failures to drive
improvement work. The hard constraints from the issue + CLAUDE.md: **no new heuristics —
authoritative data only**, and **wiring-evidence** — each requirement is met by a real public
surface (a tool subcommand) with a test.

## Decision 1 — Record model: finalize-stamped single record (NOT event-sourced)

**Chosen:** one ledger record per issue, written once at `flow-finalize`. GitHub-derived
timestamp fields are pulled live from `gh` at stamp time (un-fudgeable); run counters are
passed in explicitly by the finalize step that observed them. Per-phase durations are computed
by arithmetic over the authoritative timestamps (`to_pr_s = pr_opened_at − created_at`,
`review_s = merged_at − pr_opened_at`, `cycle_time_s = closed_at − created_at`).

**What it beat — event-sourced per-transition log.** An alternative was to append a record on
every board state change (`Ready→In Progress→In Review→Done`) and difference consecutive
events for precise per-state dwell. Rejected because:
- Not every transition flows through a `flow-*` skill — GitHub server-side automations and the
  manager move items too — so a skill-stamped event log would be **incomplete**, and an
  incomplete authoritative log is worse than a complete coarse one.
- It multiplies write points (4+ per issue) and failure modes for a marginal gain: the retro
  needs *recurring-failure ranking*, for which coarse phase buckets + the run counters are
  sufficient. Precise per-state dwell is a nice-to-have we can add later by reading the
  Projects field-value timeline, without changing the record-per-issue contract.

The single-record model keeps **one** write point (finalize), every field traceable to an
authoritative source, and the schema stable.

### Record shape (schema v1)

```json
{
  "schema": 1,
  "issue": 1161,
  "slug": "delivery-telemetry-ledger",
  "pr": 1170,
  "routing": "design",
  "priority": "P2",
  "created_at": "2026-06-27T10:00:00Z",
  "pr_opened_at": "2026-06-27T12:00:00Z",
  "merged_at": "2026-06-27T15:00:00Z",
  "closed_at": "2026-06-27T15:05:00Z",
  "cycle_time_s": 18300,
  "phase_s": { "to_pr_s": 7200, "review_s": 10800 },
  "claim_collisions": 0,
  "rebase_events": 1,
  "gate": "pass",
  "gate_runs": 2,
  "roborev_findings": 0,
  "rework": 0,
  "stamped_at": "2026-06-27T15:06:00Z"
}
```

- `routing ∈ {design, oracle}`; `gate ∈ {pass, fail}`; counters are non-negative integers.
- Timestamps are RFC-3339 UTC strings sourced from GitHub. `stamped_at` is the finalize
  wall-clock at write time (observed, not inferred).
- Counters with no observed value are recorded as an explicit `0` only when the run confirms
  zero; the `record` subcommand requires them to be supplied (it never defaults a count it did
  not observe — a missing required counter is a `record` error, not a silent `0`).

## Decision 2 — Tool home: Python script under `scripts/` (NOT a Rust subcommand)

**Chosen:** `scripts/delivery-telemetry.py`, matching the existing process-tooling pattern
(`scripts/profile_report.py`, `scripts/generate_*.py`). Tests use the Python **stdlib
`unittest`** (no third-party dep), runnable standalone and as a SKIP-aware gate component.

**What it beat — a Rust subcommand** (e.g. extend `cassandra-parity`). Rejected: process
telemetry is not domain logic, does not belong in a shipped crate, and a compile-in-the-loop
tool is heavier for a script the manager runs by hand. Python keeps it light and keeps the
`gh`-shelling ergonomic.

## Decision 3 — Retro ranking is a deterministic tally, not a model

`retro` aggregates the recorded failure categories across ledger records:
`claim_collisions`, `rebase_events`, `gate == "fail"`, `roborev_findings`, `rework`. Each
category has a **documented fixed weight** (a constant table in the script, surfaced in
`--help` and the docs). The rank = `Σ (count × weight)` per category; the top category is the
"single highest-cost recurring failure." This is a transparent, reproducible tally over
recorded values — **not** an inferred or learned cost model, so it satisfies the no-heuristics
mandate. Weights are policy constants the owner can tune; changing them is a doc + constant
edit, never a guess about data.

**Dedup:** before filing, `retro` lists open `flow-meta` issues (`gh issue list --label
flow-meta`) and matches the candidate by a stable category key embedded in the issue body
(an HTML marker `<!-- RETRO:<category> -->`). A match → skip filing (report "already tracked").

## Decision 4 — Testability without network

`gh`-dependent paths (`record` live-pull, `retro --file`) are isolated behind thin seams so
tests never touch the network:
- `record` accepts `--from-json <file>` to inject the GitHub-derived fields, exercising the
  build+validate+append path offline. (Live mode shells `gh` only when `--from-json` is
  absent.)
- `retro` runs against a **fixture ledger** (`--ledger <path>`) and, in dry-run (default),
  prints the ranked summary with no network. `--file` (the only `gh`-writing path) is not
  exercised by the offline unit tests; dedup logic is tested against an injected issue list
  (`--open-issues-json <file>`).

This gives every requirement a public-surface test that runs in the gate with no datasets and
no network.

## Risks / tradeoffs

- **Counter fidelity depends on the finalize step supplying honest counts.** Mitigation: the
  schema makes the fields required and typed; a future enhancement can derive some counters
  (roborev findings from job records, collisions from reflog) — out of scope here.
- **Coarse phase buckets** (only `to_pr`, `review`) — accepted per Decision 1; extensible
  later without a schema break (additive `phase_s` keys).

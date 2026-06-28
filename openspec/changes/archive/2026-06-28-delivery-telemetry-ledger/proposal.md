## Why

The delivery pipeline improves **reactively** — a collision happens (the #1143 finalize
race), an incident gets a guardrail (the EMU auth fix) — but there is **no telemetry**, so
every retro is anecdotal. We cannot answer "what is the single highest-cost recurring
failure in the pipeline?" with data, only with memory. Without a prioritization signal,
incident→guardrail work and skill-eval work have nothing to rank against.

This change closes the self-improvement loop: **sense** (an append-only telemetry ledger,
one record per completed issue) → **diagnose** (a recurring-retro step that ranks recorded
failures and files a deduped `flow-meta` improvement issue) → **improve** (that issue runs
through the normal pipeline).

- **Milestone:** maintenance / process. **Design-driven** (process, no Cassandra oracle).
- Extends the `delivery-pipeline` capability.

## What Changes

- **Telemetry ledger (`docs/reports/delivery-telemetry.jsonl`).** An append-only JSONL
  file, one record per completed issue, with a versioned JSON Schema
  (`docs/reports/delivery-telemetry.schema.json`). Each record captures, **from
  authoritative sources only**: GitHub-derived timestamps (issue created/closed, PR
  opened/merged) and the durations computed from them (cycle time, coarse per-phase
  durations), plus run-observed counters explicitly supplied by the finalize step
  (claim-collision count, rebase/conflict events, agent-gate pass/fail + run count,
  roborev findings count, rework/re-open count). No counter is ever inferred or estimated.
- **A telemetry tool (`scripts/delivery-telemetry.py`)** with three subcommands:
  - `record` — pulls authoritative GitHub timestamps for an issue/PR, builds a record from
    those plus the explicitly-supplied run counters, validates it against the schema, and
    appends one line to the ledger.
  - `lint` (alias `validate`) — schema-validates every line of the ledger; exits non-zero
    and names the offending line(s) on any malformed record.
  - `retro` — reads the ledger + the open `flow-meta` issues, ranks recorded failure
    categories by total recorded occurrences (× documented fixed weights — a deterministic
    tally, not a model), prints the single highest-cost recurring failure summary, and
    (with `--file`) files a **deduped** `flow-meta` issue; default is dry-run print.
- **`flow-finalize` stamps the ledger.** As its final step on a merged issue, `flow-finalize`
  calls `delivery-telemetry.py record …`, so every completed issue produces exactly one
  well-formed ledger record.
- **The manager runs the retro on a cadence.** The `/manager` doctrine gains a recurring
  step (per-epic or weekly) that runs `delivery-telemetry.py retro` and, when a recurring
  failure clears the bar, files the deduped `flow-meta` issue through the normal pipeline.
- **Gate component.** A new SKIP-aware `delivery-telemetry` agent-gate component runs the
  tool's unit tests (schema round-trip, lint-rejects-malformed, fixture-ledger → expected
  top failure, dedupe). SKIP (loudly) when no `python3`, FAIL on any test failure.

## Capabilities

### Modified Capabilities
- `delivery-pipeline`: adds the telemetry-ledger requirement, the retro requirement, the
  authoritative-data-only constraint, and the finalize-stamping requirement.

## Impact

- **New files:** `scripts/delivery-telemetry.py`, `scripts/tests/test_delivery_telemetry.py`,
  `docs/reports/delivery-telemetry.schema.json`, a fixture ledger under
  `scripts/tests/fixtures/`, and the live `docs/reports/delivery-telemetry.jsonl` (seeded
  empty or with backfilled records).
- **Skills:** `flow-finalize` gains a ledger-stamp step; `manager` gains the retro step.
- **Gate:** new `delivery-telemetry` component in `scripts/agent-gate.sh`.
- **Docs:** CLAUDE.md (delivery-pipeline section) + website `agents-developing/delivery-pipeline`
  page get a telemetry/retro subsection.
- **No cqlite-core / binding / CI-build code changes.**

## Non-goals

- **No automated cron/scheduler.** The retro runs from the manager window on a cadence; this
  change does not add a GitHub Action or timer.
- **No auto-filing of improvement issues without a human in the loop.** `retro` defaults to
  dry-run; filing requires an explicit flag and stays the manager/owner's call.
- **No inferred metrics.** The ledger records only observed events (GitHub timestamps + run
  counters); it never guesses durations or counts. Arithmetic over authoritative timestamps
  (e.g. `closed_at − created_at`) is not inference.
- **No change to the gate / C / roborev / done-bar** beyond adding the new test component.
- **No event-sourced per-state-transition log** (see design.md — finalize-stamped, single
  record per issue is the chosen model).

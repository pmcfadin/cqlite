# Process Improvements Log

A running, **measurable** log of changes to the CQLite delivery process. Each
entry states what changed, the problem it targets, a falsifiable hypothesis, and
exactly how we will measure whether it worked — so we can evaluate (and revert)
changes with data instead of vibes.

The primary measurement source is the append-only delivery-telemetry ledger
`docs/reports/delivery-telemetry.jsonl` (schema
`docs/reports/delivery-telemetry.schema.json`), stamped once per completed issue
by `flow-finalize` via `scripts/delivery-telemetry.py`.

## How to add an entry

Append a new `## YYYY-MM-DD — <short title>` section at the TOP of the log
(newest first) with these fields:

- **Change** — what concretely changed (scripts, doctrine, workflow).
- **Problem it targets** — the observed pain, ideally with issue numbers.
- **Hypothesis** — a falsifiable prediction ("X should go down / up").
- **How to measure** — the exact ledger fields / query and the before/after
  windows to compare. Name the baseline data point(s).
- **Status / result** (optional, filled in later) — what the data showed once
  enough post-change issues have landed; whether we keep, tune, or revert.

Keep entries short. The point is that a future reader can re-run the measurement.

---

## 2026-07-03 — Tiered gate (`--lite`) + conditional review-first + full-gate-once-before-merge

- **Change** — Added `scripts/agent-gate.sh --lite`, a fast iteration gate that
  runs ONLY `file-size` + `fmt` + FULL-workspace `clippy` (`-D warnings`) +
  blast-radius-scoped tests (the touched package's `--lib` + the diff's new
  `--test` targets), emitting a distinct `==== AGENT-GATE LITE SUMMARY ====`
  block (`MODE: lite`) that can never be pasted as the full gate's SUMMARY. The
  default (no-flag) full gate is byte-for-byte unchanged. Doctrine (CLAUDE.md,
  `flow-implement`, `worker`) now prescribes the loop
  `implement → lite (each fix round) → conditional internal rust-reviewer review
  → lite → FULL gate ONCE before merge → roborev → CI → merge`, with an internal
  `rust-reviewer` review-first pass before the first full gate for diffs that
  change a `pub` item, touch >1 call site of a changed symbol, or add a new
  surface (skipped for mechanical/localized diffs). **`--lite` never replaces the
  full gate**: the full `scripts/agent-gate.sh` runs exactly once before merge and
  its `==== AGENT-GATE SUMMARY ====` block is the only run that counts.

- **Problem it targets** (session retro 2026-07-03):
  1. The full gate is the bottleneck — `core-tests` (~440–697s) plus python/node
     bindings (~70–220s each) push a run to 12–25 min, and it was being run on
     **every roborev round**.
  2. Multi-round roborev churn — each convergence round forced another full-gate
     cycle.
  3. Machine-saturation SIGKILLs — under load 30–60 with ~15 concurrent gates,
     gates got SIGKILLed mid-`core-tests` and retried (one implementer wedged
     ~1h22m purely waiting on the gate).

- **Hypothesis** — Iterating on `--lite` and running the FULL gate only once
  before merge (plus catching structural findings via review-first) reduces
  **full-gate runs per issue** and **roborev rounds per issue**, which lowers
  **cycle time** and machine saturation. Directionally: full-gate-runs/issue and
  roborev-rounds/issue should both fall vs the pre-change baseline.

- **How to measure** — Using `docs/reports/delivery-telemetry.jsonl`, compare
  before vs after this change, per issue:
  - full-gate runs per issue (gate pass/fail counters),
  - roborev rounds per issue (roborev findings / rework passes),
  - rework passes, and
  - cycle time + phase durations (from GitHub timestamps).
  Aggregate a window of issues before this entry's date vs a window after, and
  look for a downward shift in full-gate-runs/issue and roborev-rounds/issue with
  cycle time flat-or-down.
  - **Baseline (this session's issues):** #1589 — one-and-done roborev; #1692 —
    three roborev rounds (three full-gate cycles); gate SIGKILLs observed under
    load. These are the pre-change reference points.

- **Status / result** — TBD (revisit after enough post-change issues have landed
  in the ledger).

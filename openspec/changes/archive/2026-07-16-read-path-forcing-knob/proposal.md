# Read-path forcing knob + point-vs-full differential-equality lane (#1918)

## Milestone
0.15 (cqlite-trino latency/throughput/operations theme — ops lane). Design-driven: this change adds
a new **user-visible env/config surface** (`CQLITE_READ_PATH`) and a new **CI test lane**, so it goes
through OpenSpec / `flow-activate` (Seam 1) before implementation. Child of epic **#1915** (point-read
fast-path residue). Anchors `classify_partition_lookup` (`select_executor/lookup.rs:92`) and the
`AccessPath` signal (`query/access_path.rs`) that already exist on `main`.

## Why (the gap)
`git grep CQLITE_READ_PATH` returns nothing — there is no way to force a `SELECT` down a specific
access path, and **no automated lane proves the point path and the full-scan path return equal
results for the same query**. `classify_partition_lookup` silently picks point-vs-full at five call
sites (`execute.rs` metadata/materializing/schemaless, `streaming.rs`, `stream_agg.rs`); a bug on one
path is invisible unless a human happens to run the query that route. #958's guard bounds the *work*
of the point path but asserts nothing about point-vs-full *result equality* — precisely the class of
divergence that let #1741 (single-generation reads skipping reconciliation) hide behind green
physical-dump goldens. This lane is the CQLite-vs-CQLite complement to #1742's CQLite-vs-Cassandra
query-semantics oracle: same two-oracle doctrine, pinned `now`, never wall-clock.

## What changes
1. **Forcing knob** `CQLITE_READ_PATH=auto|point|full` (+ a `QueryConfig` equivalent). A single gate
   wraps `classify_partition_lookup`'s outcome (not per-site logic), read once via `OnceLock`, and the
   forced choice is recorded in `AccessPath` so tests and `--explain` can see it.
   - `auto` (default): today's behavior, byte-for-byte, zero added overhead beyond the one env read.
   - `point`: **fail closed** with a distinct error whenever the executor would not run a genuinely
     partition-targeted lookup — never a silent full scan. The knob's whole purpose is to remove doubt.
   - `full`: force the full-scan + reconciliation path regardless of classification; record
     `AccessPath::FallbackFullScan` with a distinct **forced** reason.
2. **Differential-equality lane**: run the eligible corpus query matrix under `point` and `full` and
   assert identical rows, values, and order (a query-semantics-class oracle; pinned `now`; fail-closed
   on absent fixtures; demonstrably catches a seeded divergence).
3. **Docs**: the knob is documented as a **test/debug** surface, explicitly not a perf recommendation.

## Non-goals
- **No new access path or routing intelligence** — the knob only *forces* among paths that already
  exist; it never makes a new query targetable (that is #1916/#1917/#951 work).
- **No decoding/reconciliation change** — forcing governs routing only; values, tombstone shadowing,
  timestamp resolution, WRITETIME/TTL are byte-identical across all three modes (no-heuristics intact).
- **No BTI (`da`) fast-path** — a BTI table under `point` fails closed like any unavailable target.
- **No reader-level index-resolution knob** (sequential-scan vs post-#2412 summary-guided iteration) —
  that is an orthogonal axis inside the reader, not a `classify_partition_lookup` routing decision.
  Flagged as an OWNER FORK in design.md; excluded here to keep the knob one honest concept.
- **No perf/benchmark deliverable** — this is a correctness-differential + debug-control change.
- **No change to `auto` behavior** — unset must be indistinguishable from today.

## Doctrine impact
Updates CLAUDE.md test-data/two-oracle note and the `agents-developing/` validation-playbook page to
name the point-vs-full differential lane alongside the query-semantics oracle. No no-heuristics change:
forcing is explicit operator config, never inference from bytes.

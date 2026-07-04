# Proposal: RF-correct optimizer row-count stats for the Flight/Trino connector

Issue: #1336 (deferred from #944 / PR #1329). Design-driven (connector architecture +
distributed-stats semantics) → OpenSpec, Seam-1 owner approval required. Milestone: connector
enhancement (post-v0.12, Flight/Trino track).

**Owner approved (Seam 1): 2026-07-04.** Parked for a later implementation team — pick up by
fetching branch `issue-1336-rf-correct-row-stats` (existing worktree/claim) and running
`flow-implement 1336` against this change's `tasks.md`. Do not re-run `flow-activate`.

## Why

`CqliteFlightMetadata.getTableStatistics` returns `TableStatistics.empty()` for every
non-aggregated scan (`CqliteFlightMetadata.java:891-910`), so Trino's cost-based optimizer plans
CQLite scans with **no cardinality signal at all** — join ordering, join distribution
(broadcast vs partitioned), and filter selectivity all fall back to Trino's blind defaults.

#944 had to defer this: the only absolute counts the connector can fetch
(`fetchTableStats`, `CqliteFlightMetadata.java:639-650`) are whole-table `Statistics.db` sums
collected from **every** replica host of the keyspace, so on a replicated keyspace (RF=3) the
sum is ~3× the logical table cardinality. Reporting that number would actively mislead the
optimizer, so #944 conservatively reported nothing. The AUTOMATIC aggregation-pushdown gate was
unaffected because it uses only the RF-invariant ratio `partition_count / live_rows`.

This change closes the deferral: derive the **logical (de-replicated)** row count from
authoritative replica-topology metadata and report it, failing closed to today's `empty()`
whenever the derivation cannot be grounded.

## What Changes

Connector-side only (Java, `trino-connector/`). **No Rust/Flight server changes; no wire
changes; no new config knobs.**

1. `getTableStatistics` (non-aggregated branch): fetch table stats over the existing
   `table_stats` DoAction (unchanged wire), fetch `tokenRangeReplicas` from Sidecar (existing
   endpoint), derive the per-token-range distinct read-replica count `R` under the same
   `localDatacenter` scoping that `replicaHosts`/split selection already use, and — **only when
   `R` is identical across every range and `stats.complete()`** — report
   `ROW_COUNT = live_rows / R`. Any other condition (non-uniform `R`, incomplete stats, Sidecar
   or Flight error, timeout) returns `TableStatistics.empty()` exactly as today.
2. Memoize the fetched stats per `(keyspace, table)` within the metadata instance so repeated
   optimizer calls during one planning pass pay for at most one fetch (planning-time cost stays
   bounded by the existing `tableStatsTimeoutMillis` degrade path from #944).
3. The aggregated branches of `getTableStatistics` (global agg → `ROW_COUNT = 1`, grouped agg →
   `empty()`) and the entire AUTOMATIC pushdown gate (`estimateGroupRatio`,
   `declineGroupByPushdown`) are **untouched**.

Why this beats the issue's sketched token-range-scoped alternative: `Statistics.db` exposes only
whole-SSTable counts (histogram `partition_count`, STATS `totalRows`) — it is **not
range-decomposable** — so a token-range-scoped `table_stats` could produce a per-range *row*
count only via a planning-time data scan (unaffordable) or a rows-uniform-across-ranges
attribution assumption (**more** inference, not less). The replica-count divide needs no
distribution assumption: in a consistent keyspace every logical row is stored on exactly `R`
scoped read replicas, so the cross-host sum counts each row exactly `R` times. Full comparison in
`design.md`.

## Non-goals

- **Per-column statistics** (NDV, min/max, null fraction). Cassandra 5.0 `Statistics.db` carries
  no reliable per-regular-column cardinality; there is no authoritative source. Same fail-closed
  posture as #944 (issue #1336 says "ideally" — explicitly out).
- **Token-range-scoped `table_stats` / per-split stats.** No server or wire changes here; kept as
  a documented future refinement if per-split cost estimates are ever needed (see design.md
  Alternative B).
- **Grouped-aggregate output cardinality.** Grouped-agg handles keep `TableStatistics.empty()`.
- **Live multi-node (RF>1) e2e harness.** The repo's docker e2e stack is single-node; RF>1
  validation is by deterministic multi-replica fixtures (the established
  `AggregateNodeStatsTest` pattern), not a live 3-node cluster.
- **Transient replication** (Cassandra experimental): Sidecar's replica lists do not distinguish
  transient replicas; keyspaces using it are outside this estimate's correctness envelope
  (documented limitation, design.md).

## Doctrine impact

None to CLAUDE.md or the agents-developing site. The change strengthens no-heuristics compliance
in an existing surface (divisor comes from counting actual per-range replicas in authoritative
Sidecar metadata — never from parsing replication-strategy option strings) and keeps #944's
fail-closed posture. `docs/flight-trino/PLAN.md` gets a short stats-semantics note as part of the
change.

## Impact

- Code: `trino-connector/src/main/java/in/mcfad/cqlite/flight/CqliteFlightMetadata.java` (+ a
  small pure helper for the uniform-replica-count derivation), tests alongside existing
  `CqliteFlightTableStatisticsTest` / `AggregateNodeStatsTest`.
- Specs: new capability `flight-optimizer-stats` (ADDED requirements).
- Risk: low — additive estimate with fail-closed degrade to current behavior; gate and
  aggregated paths untouched.

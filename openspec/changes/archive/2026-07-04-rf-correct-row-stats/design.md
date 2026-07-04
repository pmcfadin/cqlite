# Design: RF-correct logical row count via authoritative replica-count division

## Context (facts, verified against the worktree)

- **Scan path is already RF-deduplicated**: `CqliteFlightSplitManager.buildSplits`
  (`CqliteFlightSplitManager.java:68-95`) emits one split per read-replica token range, pinned to
  exactly ONE replica (`pickReplica`, `:102-114`, local-DC preferred, deterministic), and the Rust
  server filters each scan to the split's `(start, end]` token range
  (`ticket.rs:240-246`, `filter.rs:62-86,209-217`, `producer.rs:384-409,437-438`). Each row is read
  once cluster-wide.
- **Stats path is not**: `fetchTableStats` (`CqliteFlightMetadata.java:639-650`) sends a
  whole-table `table_stats` request (no token fields exist on `TableStatsRequest`,
  `cqlite-flight/src/stats.rs:92-119` / `service.rs:419-467`) to the DISTINCT union of replica
  hosts (`replicaHosts`, `:667-692`, scoped to `localDatacenter` when set, else all DCs) and sums
  (`aggregateNodeStats`, `:724-750`). On RF=N the sum counts every logical row ~N times.
- **`Statistics.db` is not range-decomposable**: `read_table_counts`
  (`cqlite-core/src/parser/repair_metadata.rs:805-810`) yields whole-SSTable
  `partition_count` (Σ `estimatedPartitionSize` histogram buckets) and `total_rows`
  (STATS `totalRows`, `None` when not traversable). There is no per-token-range breakdown.
- **Sidecar provides authoritative per-range replica lists**: `tokenRangeReplicas(keyspace)`
  (`SidecarClient.java:44-47`) → `ReplicaInfo{start, end, replicasByDatacenter}`
  (`SidecarModels.java:39-51`). Host lists only — no per-range counts of rows/partitions.
- **Completeness is already fail-closed** end to end: server taints `complete=false` on any
  missing/undecodable `Statistics.db`, missing `totalRows`, or the #1327 empty-histogram
  contradiction (`stats.rs:310-319`); connector taints on unreachable hosts
  (`TableStats.UNAVAILABLE` fold).

## Decision

**Approach A — connector-side logical de-replication (chosen).**

For a non-aggregated table handle, `getTableStatistics`:

1. `ranges = sidecar.tokenRangeReplicas(keyspace).readReplicas()`; scope each range's replica set
   with the SAME DC rule `replicaHosts` uses (`localDatacenter` when configured, else all DCs).
2. Derive `R_i = |distinct scoped replicas of range i|`. **Require `R_i` identical (= R ≥ 1) for
   all ranges**; any non-uniformity (mid-bootstrap/decommission topology, partially-replicated DC,
   empty range replica set) → return `TableStatistics.empty()`.
3. `stats = fetchTableStats(handle)` (existing whole-table, all-scoped-hosts sum). Require
   `stats.complete()`; else `empty()`.
4. Report `TableStatistics.builder().setRowCount(Estimate.of((double) stats.liveRows() / R))`.
5. Any `RuntimeException` / timeout anywhere → `empty()` (same degrade posture as
   `groupRatioForGate`, `:446-464`; planning must never fail because stats were unavailable).
6. Memoize `stats` (and the derived `R`) per `(keyspace, table)` in the `CqliteFlightMetadata`
   instance so one planning pass fetches at most once per table.

Correctness argument (no distribution assumption): under NTS/SimpleStrategy the per-keyspace
replica count is uniform across the token space **within the scoping** (per-DC RF is a keyspace-
level constant; total RF likewise). Summing whole-table `live_rows` over the distinct scoped
host set counts each logical row exactly once per replica that stores it — i.e. exactly `R`
times when replicas are consistent. Division is exact under consistency; under replica
divergence (repair lag, differing compaction of expired data) it returns the mean across
replicas — a well-behaved estimate, which is precisely `Estimate`'s contract. The uniformity
check makes the one situation where "count replicas per range" and "keyspace RF" can disagree
(topology in transition) fail closed to today's behavior.

No-heuristics compliance: the divisor is obtained by **counting actual replicas in
authoritative token-range metadata**, never by parsing `replication = {...}` strategy-class
option strings (fragile across NTS per-DC maps and transient-replication `"3/1"` syntax — that
was the issue's own objection to the naive "derive RF" variant, and it is why Alternative C is
rejected). No byte-pattern or distribution guessing anywhere.

## Alternatives considered

**B — token-range-scoped `table_stats` (the issue's initial lean): rejected.** It sounds like
"match the split assignment," but it cannot produce an honest per-range **row** count:
`Statistics.db` has no per-range breakdown, so the server would have to either (i) scan
`Data.db`/walk `Index.db` per planning call — `Index.db`/BTI range walks give per-range
*partition* counts, but per-range *rows* would still be `partitions_in_range × avg_rows_per_
partition`, a rows-uniform-across-partitions attribution — or (ii) do a real data scan at
planning time (unaffordable vs the 3 s stats budget). Either way it embeds MORE inference than
Approach A while also demanding a new wire field, per-format (BIG + BTI) server-side range
counting, and a bigger test surface. Kept as a documented future refinement iff per-split cost
estimates are ever needed; nothing in A blocks it.

**C — divide by RF parsed from keyspace schema replication options: rejected.** String-
interpreting strategy-class options (NTS per-DC maps, transient `"3/1"`) is exactly the fragility
the issue flags, gives no fail-closed signal during topology transitions, and duplicates
information the token-range endpoint states authoritatively.

**D — sum stats from only the split-assigned (one-per-range) host subset: rejected as
insufficient alone.** Each node's `Statistics.db` covers **everything that node replicates**, not
just its split-assigned ranges (see "hosts" below), so summing whole-table stats over the deduped
host subset still multi-counts. Range scoping would be required to fix that — which is
Alternative B.

## Known limitations (documented, accepted)

- **Replica divergence** makes the result an average-across-replicas estimate rather than an
  exact count. Acceptable: Trino stats are `Estimate`s; today's alternative is no signal at all.
- **Transient replication** (Cassandra experimental) is invisible in Sidecar's replica lists;
  keyspaces using it may over-divide. Documented out of scope.
- `live_rows` is the STATS `totalRows` upper bound (pre-tombstone-merge), consistent with how the
  gate already interprets it (#944).

## Deployment/topology notes

"Hosts" here are one cqlite-flight server per Cassandra node, each reading that node's local
data dir (`cqlite-flight/src/main.rs:19-31`); a node's dir contains every range the node
replicates. That is why D fails and why A's whole-table-sum ÷ R is the clean identity.

## Test design (fail-first, fixture-driven)

- Extend the `AggregateNodeStatsTest`-style harness (it already models RF>1 via injected fetch
  functions + multi-replica `ReplicaInfo`): RF=3 fixture where three hosts each report 200 rows →
  `getTableStatistics` must report **200**, not 600 (this is the issue's acceptance criterion:
  physical sum ≠ logical cardinality).
- Multi-DC fixture (dc1 RF=3, dc2 RF=2) with `localDatacenter=dc1` → divisor 3; unset → divisor 5.
- Fail-closed fixtures: non-uniform per-range counts → `empty()`; `complete=false` → `empty()`;
  Sidecar throws / stats timeout → `empty()` and planning proceeds.
- Pin unchanged behavior: global agg still `ROW_COUNT=1`, grouped agg still `empty()`
  (`CqliteFlightTableStatisticsTest` updated: the non-aggregated case flips from
  pinned-`empty()` to pinned-logical-count under healthy fixtures, with new pinned-`empty()`
  fail-closed cases).
- `EstimateGroupRatioTest` and the gate tests must pass **unmodified** (gate untouched).
- Wiring evidence: the public surface is Trino's `ConnectorMetadata.getTableStatistics`; the
  end-to-end test drives it through `CqliteFlightMetadata` with real `TableStats` JSON decode +
  real `SidecarModels` parsing (no mocking of the derivation itself).

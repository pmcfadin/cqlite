# flight-snapshot-reuse

## ADDED Requirements

### Requirement: Snapshots are reused per (keyspace, table) within a bounded freshness window

The connector's `SnapshotManager` SHALL, in snapshot read mode, reuse a single snapshot for a
`(keyspace, table)` across queries within a bounded freshness window instead of creating one snapshot
per `queryId`. While a `(keyspace, table)` snapshot is fresh, `snapshotFor`/`availableHosts` SHALL
return the existing snapshot's name and perform NO new Sidecar create call. The number of snapshot
create fan-outs SHALL therefore be bounded by the number of freshness windows, not by the number of
queries. Reuse SHALL be per replica host (consistent with the #2227 instance-local create model).

#### Scenario: N queries within one window create exactly one snapshot

- **GIVEN** a `SnapshotManager` in snapshot mode with an injected logical clock held within one
  freshness window, and a `snapshot_creations_total` counter reset to 0
- **WHEN** N distinct queries for the same `(keyspace, table)` request a snapshot on the same host
- **THEN** exactly one snapshot create call is made to that host's Sidecar
  (`snapshot_creations_total == 1`) and the other N-1 queries increment `snapshot_reuse_hits_total`
- **AND** all N queries receive the SAME snapshot name in their tickets.

### Requirement: A reused snapshot is invalidated by window expiry, generation-set change, or explicit refresh

A reused `(keyspace, table)` snapshot SHALL be invalidated — forcing a fresh create on the next query
— when the FIRST of these occurs: (1) the freshness window elapses on the injectable logical clock;
(2) the table's observed live SSTable generation set changes since the snapshot was taken; or (3) an
explicit `invalidate(keyspace, table)` refresh is requested. Window timing SHALL be driven by an
injectable clock/ticker seam, never `System.currentTimeMillis` in tests, so invalidation is pinned
deterministically.

#### Scenario: A new query after window expiry creates a fresh snapshot

- **GIVEN** a fresh reused snapshot for a `(keyspace, table)` and a `snapshot_creations_total` reset to 0
- **WHEN** the injected logical clock is advanced past the freshness window and a new query requests a
  snapshot for that table
- **THEN** exactly one new snapshot create call is made (`snapshot_creations_total == 1`) and the new
  query receives the NEW snapshot name.

#### Scenario: An explicit refresh forces a fresh snapshot on the next query

- **GIVEN** a fresh reused snapshot for a `(keyspace, table)`
- **WHEN** `invalidate(keyspace, table)` is called and a subsequent query requests a snapshot within the
  window
- **THEN** a new snapshot is created for that table and the subsequent query receives the new name.

#### Scenario: An observed generation-set change invalidates reuse

- **GIVEN** a fresh reused snapshot taken over generation set G for a `(keyspace, table)`
- **WHEN** the connector observes the table's live generation set change to G' (a flush/compaction) and a
  new query requests a snapshot within the window
- **THEN** a new snapshot is created (the stale-generation snapshot is not reused) and the query receives
  the new name.

### Requirement: Reuse reduces memtable-flush churn without changing flush semantics

Reusing snapshots SHALL reduce the flush/SSTable-creation rate on the cluster proportionally to the
reuse factor under a query-heavy default-mode workload, because each snapshot create triggers one
memtable flush per host by design (#2305). This change SHALL NOT skip, defer, or otherwise alter the
flush-on-snapshot semantics (#2305 not relitigated); the only lever on flush volume is fewer snapshot
create calls. The flush-rate reduction SHALL be measurable from `snapshot_creations_total` (one create
⇒ one flush per host), and the derivation SHALL be documented in the connector docs.

#### Scenario: Snapshot creation rate over a query-heavy workload drops by the reuse factor

- **GIVEN** a snapshot-mode workload of Q queries for one `(keyspace, table)` spanning W freshness
  windows on the injected clock, with `snapshot_creations_total` reset
- **WHEN** the workload completes
- **THEN** `snapshot_creations_total` equals W (one create per window), not Q (one per query), so the
  flush-inducing create rate drops from Q to W
- **AND** the connector docs state the `creations ⇒ flushes` derivation used to report the #2306
  operational-cost reduction.

### Requirement: Reuse preserves point-in-time correctness, isolation, and LIVE-mode inertness

A reused snapshot SHALL remain a valid, immutable Cassandra point-in-time; the only observable change
SHALL be that a read may reflect table state up to one freshness window old (a documented staleness
bound), never an inconsistent mix of point-in-times within a single query. Row-level parity
(physical-dump + query-semantics oracles) SHALL hold for reads served from a reused snapshot. In
`ReadMode.LIVE` the `SnapshotManager` SHALL remain inert (no snapshot name, no reuse cache, no Sidecar
create calls), exactly as today.

#### Scenario: A read from a reused snapshot returns a correct point-in-time result set

- **GIVEN** a reused snapshot for a `(keyspace, table)` taken at a pinned point-in-time
- **WHEN** a query is served from that reused snapshot
- **THEN** the returned result set matches the query-semantics oracle for that point-in-time at a pinned
  `now` (no wall-clock), reflecting exactly the snapshot's atomic state
- **AND** the connector docs state the staleness bound `min(window, time-since-last-generation-change)`.

#### Scenario: LIVE mode performs no reuse and no Sidecar calls

- **GIVEN** a `SnapshotManager` constructed in `ReadMode.LIVE`
- **WHEN** any number of queries request a snapshot for any `(keyspace, table)`
- **THEN** `snapshotFor` returns empty (ticket `snapshot=null`), `snapshot_creations_total` stays 0, and
  no reuse-cache entry is created (the pre-#2105 behavior is unchanged).

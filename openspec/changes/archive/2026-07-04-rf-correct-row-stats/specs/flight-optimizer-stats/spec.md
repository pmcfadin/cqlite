# flight-optimizer-stats

RF-correct logical optimizer row-count statistics for the Flight/Trino connector (issue #1336,
deferred from #944).

## ADDED Requirements

### Requirement: Non-aggregated scans report a logical (de-replicated) row count
For a non-aggregated table handle, `CqliteFlightMetadata.getTableStatistics` SHALL report
`TableStatistics` with `ROW_COUNT = live_rows / R`, where `live_rows` is the existing
cross-replica-host `table_stats` sum and `R` is the uniform per-token-range distinct
read-replica count derived from Sidecar `tokenRangeReplicas` under the same `localDatacenter`
scoping used by `replicaHosts` and split selection. The physical replica-summed total SHALL
never be reported as the row count.

#### Scenario: RF=3 keyspace reports logical, not physical, cardinality
- **WHEN** `getTableStatistics` runs for a non-aggregated handle over a keyspace whose every token range has 3 scoped read replicas and whose three replica hosts each report 200 live rows (physical sum 600)
- **THEN** the reported `ROW_COUNT` estimate is 200
- **AND** not 600

#### Scenario: Multi-DC replica sets divide by the scoped replica count
- **WHEN** a keyspace is replicated dc1=3, dc2=2 and `localDatacenter` is configured as `dc1`
- **THEN** the divisor is 3 (the dc1-scoped per-range replica count)
- **AND** when `localDatacenter` is not configured, the divisor is 5 (all-DC scoping), matching the host set the stats sum was collected from

#### Scenario: One planning pass fetches stats at most once per table
- **WHEN** the optimizer calls `getTableStatistics` repeatedly for the same `(keyspace, table)` within one metadata instance
- **THEN** the connector issues at most one `table_stats` fetch and one `tokenRangeReplicas` fetch for that table, reusing the memoized result

### Requirement: The divisor derives only from authoritative token-range replica metadata
The replica-count divisor SHALL be obtained by counting distinct scoped replicas per token range
in the Sidecar `tokenRangeReplicas` response. It SHALL NOT be obtained by parsing keyspace
replication-strategy option strings, and no distributional assumption (e.g. rows uniform across
token ranges or partitions) SHALL enter the derivation (no-heuristics mandate, issue #28).

#### Scenario: Divisor counts actual per-range replicas
- **WHEN** the divisor for a keyspace is derived
- **THEN** it equals the distinct scoped read-replica count shared by every token range in the authoritative `tokenRangeReplicas` response
- **AND** no code path parses `replication = {...}` strategy options to obtain it

### Requirement: The logical row count fails closed to empty statistics
Whenever the derivation cannot be grounded, `getTableStatistics` SHALL return
`TableStatistics.empty()` (today's behavior) rather than a possibly-wrong number. Grounding
failures include: per-range scoped replica counts that are not identical across all ranges, a
range with zero scoped replicas, `table_stats` reporting `complete=false`, and any Sidecar or
Flight error or timeout. Statistics failures SHALL never fail query planning.

#### Scenario: Non-uniform per-range replica counts fail closed
- **WHEN** the scoped `tokenRangeReplicas` response contains ranges with differing distinct replica counts (e.g. topology mid-transition)
- **THEN** `getTableStatistics` returns `TableStatistics.empty()`

#### Scenario: Incomplete stats fail closed
- **WHEN** the aggregated `TableStats` has `complete=false` (unreachable host, undecodable `Statistics.db`, missing `totalRows`, or the #1327 count contradiction)
- **THEN** `getTableStatistics` returns `TableStatistics.empty()`

#### Scenario: Infrastructure failure degrades to no estimate, never a planning failure
- **WHEN** the Sidecar call or a `table_stats` fetch throws or exceeds `tableStatsTimeoutMillis`
- **THEN** `getTableStatistics` returns `TableStatistics.empty()`
- **AND** the query continues planning normally

### Requirement: Aggregation paths and the AUTOMATIC pushdown gate are unchanged
This feature SHALL NOT change the behavior of the aggregated branches of `getTableStatistics`
(global aggregate → `ROW_COUNT = 1`; grouped aggregate → `TableStatistics.empty()`) nor of the
AUTOMATIC GROUP-BY pushdown gate (`estimateGroupRatio` / `declineGroupByPushdown`, which use
only the RF-invariant `partition_count / live_rows` ratio).

#### Scenario: Aggregated handles keep their existing statistics
- **WHEN** `getTableStatistics` runs for a global-aggregate handle
- **THEN** it reports `ROW_COUNT = 1`
- **AND** for a grouped-aggregate handle it returns `TableStatistics.empty()`

#### Scenario: Gate decisions are identical before and after
- **WHEN** the existing gate test suite (`EstimateGroupRatioTest` and related pushdown tests) runs against this change
- **THEN** it passes without modification

### Requirement: The logical count is exercised through the public connector surface
The feature SHALL be validated through Trino's `ConnectorMetadata.getTableStatistics` on
`CqliteFlightMetadata` (the public surface), with real `TableStats` JSON decoding and real
`SidecarModels` token-range parsing in the fixtures — not by unit-testing a private helper alone
(wiring-evidence doctrine).

#### Scenario: End-to-end derivation over decoded fixtures
- **WHEN** the RF>1 validation test runs
- **THEN** it drives `CqliteFlightMetadata.getTableStatistics` end to end over multi-replica `tokenRangeReplicas` JSON fixtures and per-host `table_stats` responses
- **AND** asserts the reported estimate equals the logical cardinality

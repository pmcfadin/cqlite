# trino-split-pruning Specification

## Purpose
TBD - created by archiving change plantime-split-pruning. Update Purpose after archive.
## Requirements
### Requirement: A fully-bound partition key prunes splits to the covering token range(s)

`CqliteFlightSplitManager.getSplits` SHALL prune emitted splits when the pushed-down constraint fully binds
the partition key. When the constraint fixes the **full partition key** by an equality domain on every
partition-key column, `getSplits` SHALL compute the Murmur3 token for the bound key and emit splits ONLY
for the token range(s) whose half-open `(start, end]` interval (with the existing wraparound convention)
contains that token. For a single fully-bound key this SHALL be exactly one split. A constraint that does
NOT fully bind the partition key (range scan, partial PK, non-PK-only predicates) SHALL leave the split set
unchanged (full fan-out).

#### Scenario: Single fully-bound PK emits exactly the covering split

- **GIVEN** a keyspace whose Sidecar `token-range-replicas` yields a multi-range topology (e.g. ~48 ranges)
- **AND** a query whose pushed-down constraint binds every partition-key column by equality to a single value
- **WHEN** `getSplits` runs
- **THEN** it emits exactly **one** split — the one whose `(start, end]` range contains the key's Murmur3 token
- **AND** on `main` (no pruning) the same query emits one split per range (full fan-out).

#### Scenario: Partial or absent PK constraint keeps full fan-out

- **GIVEN** the same multi-range topology
- **WHEN** the pushed-down constraint binds only a subset of the partition-key columns, or binds a PK column
  by a range/unbounded domain, or contains only non-PK predicates, or is absent
- **THEN** `getSplits` emits the full set of splits (one per read-replica range) — the split count is
  identical to the current behavior.

### Requirement: IN-list over full partition keys prunes to the deduped union of covering ranges

`getSplits` SHALL prune to the deduplicated union of covering token ranges when an IN-list fully binds the
partition key. When the pushed-down constraint binds the full partition key by a discrete set (IN over full
keys, or a per-column discrete set whose Cartesian product enumerates full keys), `getSplits` SHALL compute the
Murmur3 token for each enumerated full key and emit splits for the **union** of covering token ranges,
**deduplicated**. It SHALL NEVER emit fewer ranges than that union requires. If the enumeration is empty or
any factor is not a clean discrete set of typeable values, it SHALL fall back to full fan-out.

#### Scenario: IN over full PKs emits the union of covering ranges, deduped

- **GIVEN** the multi-range topology
- **AND** a query with an IN-list over the full partition key spanning K distinct keys that fall into
  M ≤ K distinct token ranges
- **WHEN** `getSplits` runs
- **THEN** it emits exactly M splits — the deduplicated union of the ranges covering the K keys
- **AND** no key's covering range is omitted (every enumerated token maps to an emitted split).

#### Scenario: Two IN keys sharing a range collapse to one split

- **GIVEN** two distinct full keys whose Murmur3 tokens fall in the same `(start, end]` range
- **WHEN** `getSplits` prunes for an IN-list containing both keys
- **THEN** exactly one split (that shared range) is emitted, not two.

### Requirement: Token derivation is schema-authoritative and fail-safe (no-heuristics)

The Murmur3 token SHALL be computed from the schema-declared partition-key columns (names + order from the
DDL via `PrimaryKeyExtractor`) and their CQL-typed values from the constraint domains — **never inferred
from data bytes or byte-pattern guessing**. The partitioner SHALL be resolved from cluster metadata /
the ring's declared assumption; an unknown or non-`Murmur3` partitioner SHALL disable pruning (full fan-out)
and be surfaced in logs. Any value that cannot be typed or serialized to partition-key bytes SHALL disable
pruning for that query rather than prune approximately.

#### Scenario: Unknown/non-Murmur3 partitioner disables pruning

- **GIVEN** a cluster whose resolved partitioner is not `Murmur3Partitioner` (or is unknown)
- **WHEN** `getSplits` evaluates an otherwise fully-bound-PK constraint
- **THEN** no pruning occurs (full fan-out is emitted)
- **AND** the reason is logged.

#### Scenario: An un-serializable PK value disables pruning, not silent misprune

- **GIVEN** a fully-bound-PK constraint where a partition-key value's CQL type has no Java partition-key
  byte serialization
- **WHEN** `getSplits` attempts to compute the token
- **THEN** pruning is skipped for that query (full fan-out) rather than pruning on an approximate token.

### Requirement: Java Murmur3 token matches the Rust Cassandra-parity authority

The Java token computation (`Murmur3Token` over the canonical partition-key byte layout) SHALL produce
tokens byte-identical to the Rust authority `cassandra_murmur3_token`
(`cqlite-core/src/util/cassandra_murmur3.rs`) applied to `PartitionKey::to_bytes`
(`cqlite-core/src/storage/write_engine/mutation.rs`) — single-component keys use the raw value bytes;
multi-component keys use `[len:u16 BE][bytes][0x00]` per component. This SHALL include the
`i64::MIN → i64::MAX` normalization.

#### Scenario: Shared vectors pin Java tokens to the Rust implementation

- **GIVEN** a set of representative partition keys (single-column and composite, spanning common CQL types)
  with tokens computed by the Rust `cassandra_murmur3_token`
- **WHEN** the Java `Murmur3Token` computes tokens for the same keys
- **THEN** every Java token equals the corresponding Rust token, including the normalized `i64::MIN` case.

### Requirement: Pruned execution is row-identical to unpruned execution (differential correctness)

Pruned execution SHALL return results identical to unpruned execution. Because split elimination is
load-bearing for correctness (a wrongly-pruned split silently drops rows), pruning SHALL be toggleable, and
for any query, executing with pruning enabled SHALL return identical rows, values, and order to executing
the same query with pruning forced off.

#### Scenario: Pruned vs forced-unpruned returns identical result sets

- **GIVEN** a connector-level differential harness that runs a query once with split pruning enabled and
  once with pruning forced off (a session property / config toggle)
- **WHEN** the query is a fully-bound-PK point read and, separately, an IN-list over full PKs
- **THEN** both executions return byte-identical rows, values, and ordering
- **AND** the pruned execution emits strictly fewer (or equal) splits than the unpruned one.

### Requirement: End-to-end wiring — a point read is served by a single DoGet

The pruning SHALL be exercised through the real Trino query path (the public `ConnectorSplitManager`
surface), not helper-only unit tests. An end-to-end test SHALL show a fully-bound-PK point read producing a
`SplitSource` that yields exactly one split, observable via split count or a pruning/DoGet counter on the
public surface.

#### Scenario: Point read through the Trino path yields one split / one DoGet

- **GIVEN** the connector wired to a multi-range topology
- **WHEN** a fully-bound-PK point-read query flows through `getSplits` on the public surface
- **THEN** the resulting `SplitSource` yields exactly one split (→ one DoGet to one pod)
- **AND** the assertion reads the public split count / pruning counter, not an internal helper return value.


# Capability: iceberg-materializer

## ADDED Requirements

### Requirement: Delta folding into an Iceberg v2 table

The materializer SHALL fold delta-envelope records into an Apache Iceberg v2
table such that upserts become data-file rows and row/range/partition
tombstones remove the rows they shadow, with last-write-wins resolution by
mutation timestamp matching Cassandra reconcile semantics (the compaction
byte-parity rule set, `docs/compaction/byte-parity-rules.md`).

#### Scenario: Upsert becomes a queryable row

- **GIVEN** a delta envelope containing an upsert for partition key `id=1`
  with `name='Alice'`
- **WHEN** `cqlite materialize` commits it to an empty Iceberg table
- **THEN** an Iceberg scan of the table returns exactly one row
  `id=1, name='Alice'`

#### Scenario: Row tombstone removes a materialized row

- **GIVEN** a materialized table containing `id=1` and a subsequent envelope
  containing a row tombstone for `id=1` with a higher timestamp
- **WHEN** the envelope is materialized
- **THEN** an Iceberg scan returns zero rows for `id=1` and the commit
  contains a v2 delete file covering that key

#### Scenario: Range tombstone removes only the shadowed clustering range

- **GIVEN** a materialized partition with clustering rows `c=1..10` and an
  envelope carrying a range tombstone covering `c >= 3 AND c <= 7` at a
  higher timestamp
- **WHEN** the envelope is materialized
- **THEN** an Iceberg scan returns rows `c=1,2,8,9,10` only

#### Scenario: Equal-timestamp Delete-vs-Live resolves to the tombstone

- **GIVEN** an upsert and a tombstone for the same row carrying the same
  mutation timestamp
- **WHEN** both are materialized (in either envelope order)
- **THEN** the row is absent from an Iceberg scan (Cassandra
  `Cells#reconcile` tie-break, matching #498)

### Requirement: Exactly-once generation consumption

The materializer SHALL record the identities of consumed source generations
in Iceberg snapshot metadata and SHALL treat a re-submission of an
already-consumed generation as a no-op that performs no commit.

#### Scenario: Re-running the same input is a no-op

- **GIVEN** a generation already materialized into snapshot S
- **WHEN** `cqlite materialize` is invoked again with the same generation
- **THEN** the command exits success, reports the generation as already
  consumed, and the table's current snapshot remains S

#### Scenario: Crash before commit leaves no partial state

- **GIVEN** a materialize run interrupted after writing data files but
  before the snapshot commit
- **WHEN** the same invocation is re-run
- **THEN** the table's readable state reflects the generation exactly once

### Requirement: Compaction supersession safety

The materializer SHALL NOT double-count data when offered a
compaction-output generation whose logical content was already materialized
from its input generations, and SHALL fail closed with a descriptive error
when generation lineage cannot be established from authoritative metadata.

#### Scenario: Compacted rewrite of consumed inputs does not duplicate rows

- **GIVEN** generations G1 and G2 already materialized, and G3 produced by
  compacting G1+G2 with declared lineage
- **WHEN** G3 is submitted for materialization
- **THEN** an Iceberg scan returns the same row set as before the submission

#### Scenario: Unknown lineage fails closed

- **GIVEN** a generation with no establishable lineage record
- **WHEN** it is submitted alongside `--require-lineage`
- **THEN** the command exits non-zero naming the generation and no snapshot
  is committed

### Requirement: Authoritative delta-horizon watermark

Each committed snapshot SHALL carry a `cqlite.delta-horizon-micros` property
equal to the maximum mutation timestamp fully reflected by the snapshot,
sourced only from authoritative Statistics.db metadata; when the source
metadata is a placeholder (pre-#1729 writer output), the materializer SHALL
fail closed rather than emit a fabricated watermark (no-heuristics mandate).

#### Scenario: Watermark matches authoritative max timestamp

- **GIVEN** input generations whose Statistics.db carries authoritative
  `maxTimestamp` values T1 < T2
- **WHEN** both are materialized in one commit
- **THEN** the snapshot property `cqlite.delta-horizon-micros` equals T2

#### Scenario: Placeholder statistics fail closed

- **GIVEN** an input generation whose Statistics.db `maxTimestamp` is the
  known `max=min` placeholder (#1729)
- **WHEN** materialization is attempted
- **THEN** the command exits non-zero citing non-authoritative statistics
  and commits nothing

### Requirement: Reference-merge parity

For any envelope set, the materialized Iceberg table state SHALL be
row-for-row equal to the DuckDB reference-merge reconciliation (#878) of the
same envelopes, and this parity SHALL be enforced by an automated oracle
test over the pinned test corpus.

#### Scenario: Parity over the mixed-tombstone corpus table

- **GIVEN** the pinned delta-envelope fixtures for a corpus table containing
  upserts, row, range, and partition tombstones across generations
- **WHEN** the fixtures are materialized and independently reference-merged
  in DuckDB
- **THEN** the two result sets are equal under the parity harness comparator

### Requirement: Schema and type mapping fidelity

The materializer SHALL derive the Iceberg schema from the provided CQL
schema using the established Arrow mapping (Epic #673), declare the CQL
primary-key columns as Iceberg identifier fields, and SHALL fail closed
naming the column when a CQL type has no supported Iceberg mapping.

#### Scenario: Collections map to nested Iceberg types

- **GIVEN** a table with `list<int>`, `set<text>`, and `map<text,int>`
  columns
- **WHEN** the table is materialized
- **THEN** the Iceberg schema declares list/list/map nested types and values
  round-trip through an Iceberg scan

#### Scenario: Unsupported type fails closed

- **GIVEN** a schema containing a column type outside the supported mapping
- **WHEN** materialization is attempted
- **THEN** the command exits non-zero naming the column and type, and
  commits nothing

### Requirement: Feature-flag claim boundary

Iceberg support SHALL be gated behind a non-default `iceberg` cargo feature:
default builds SHALL contain no Iceberg dependencies and SHALL NOT expose
the `materialize` subcommand or the `IcebergMaterializer` API.

#### Scenario: Default build has no materialize surface

- **GIVEN** a default-features build of the CLI
- **WHEN** `cqlite materialize --help` is invoked
- **THEN** the CLI reports the subcommand as unavailable/unknown

#### Scenario: Feature build exposes the surface

- **GIVEN** a `--features iceberg` build
- **WHEN** `cqlite materialize --help` is invoked
- **THEN** usage for the subcommand is printed

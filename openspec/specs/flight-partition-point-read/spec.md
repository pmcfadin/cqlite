# flight-partition-point-read Specification

## Purpose
TBD - created by archiving change flight-pk-pointread-pushdown. Update Purpose after archive.
## Requirements
### Requirement: A pushed full-PK-equality predicate SHALL route do_get to a partition point-read path

The server SHALL route execution to a partition point-read path — resolving candidate SSTables and
reading only the target partition(s) instead of the full k-way merge scan with a per-row predicate
filter — when the predicate pushed into a Flight `do_get` ticket binds **every** partition-key
component to a single value (a full-PK equality, or an `IN`/`Or` list of such full-PK equalities).
Any other predicate shape — partial partition key, clustering-only, range, secondary-column,
`IS NULL`, or no predicate — SHALL keep the unchanged full-scan path. The routing decision SHALL be
derived from the typed predicate tree and the table schema's partition-key definition only; it
SHALL NOT be inferred from byte patterns or any non-authoritative heuristic (#28).

#### Scenario: Full single-PK equality takes the point path (fails on main)

- **GIVEN** a multi-SSTable, multi-partition fixture and a `do_get` ticket carrying `pk = <value>`
  covering the table's sole partition-key component
- **WHEN** the request executes
- **THEN** a work-done probe (`CountingStepper`-style) shows partitions examined ≈ the surviving
  candidate SSTables' point lookups, NOT the table's partition count
- **AND** on `main` the same ticket examines every partition in the table (the point path does not
  exist) — so this scenario fails before the change.

#### Scenario: A partial or non-PK predicate keeps the scan path

- **GIVEN** a table with a composite partition key `(a, b)` and a ticket binding only `a = <value>`
  (or a ticket binding a clustering/regular column, or no predicate)
- **WHEN** the request executes
- **THEN** it runs the full-scan + per-row filter path unchanged, reporting a full-scan access path,
  and returns exactly the rows it returns on `main`.

#### Scenario: IN over the full PK is N bounded point reads

- **GIVEN** a ticket carrying `pk IN (v1, v2, v3)` over the full partition key
- **WHEN** the request executes
- **THEN** the work-done probe shows the partitions examined are bounded by the candidate lookups
  for the three keys, not the table's partition count, and the result is exactly the union the scan
  path produces for the same `IN` list.

### Requirement: The point-read path is byte-identical to the scan path for the same predicate

For any pushed predicate that routes to the point-read path, the streamed result SHALL contain
exactly the same rows, column values, and tombstone/multi-generation reconciliation outcome as the
full-scan + per-row-filter path produces for the same predicate over the same data. The point path
SHALL gather the target partition's fragments from **every** candidate SSTable that is not proven
absent and reconcile them through the same merge semantics as the scan path (LWW / tombstone /
across-generation). A "first SSTable hit wins" shortcut is forbidden.

#### Scenario: Dual-path parity on a tombstoned, multi-generation corpus

- **GIVEN** a real fixture where the target partition's rows are split across ≥2 SSTable
  generations, at least one carrying a tombstone or a newer overwrite for a cell in that partition
- **WHEN** the same PK-equality ticket is executed once with the point path and once with the scan
  path
- **THEN** the two result batch streams are byte-identical (same rows, same values, same
  post-reconciliation tombstone resolution).

#### Scenario: Point-read result matches the query-semantics oracle

- **GIVEN** the point path over corpus data with a PK-equality that hits a key with a shadowing
  tombstone/overwrite, evaluated at the pinned `now` of `test-data/query-semantics-oracle.json`
- **WHEN** the reconciled result set is compared to the oracle's recorded `SELECT` result for that
  key
- **THEN** they match exactly — proving post-reconciliation correctness that the physical-dump
  goldens alone cannot (both paths retain shadowed rows on disk, so the semantic oracle is the
  arbiter).

### Requirement: SSTable pruning is authoritative-metadata-only and fail-safe toward reading

Candidate-SSTable pruning on the point path SHALL use only authoritative presence metadata — the
bloom presence oracle (`might_contain_partition`) and Summary/Index (BIG `nb`) or BTI trie (`da`)
resolution, plus Statistics.db/schema metadata — and SHALL prune an SSTable **only** when the
presence oracle reports the key definitively absent (an exact bloom negative). Whenever presence is
positive, unknown, or the index/summary components are absent, unreadable, or ambiguous, the SSTable
SHALL be treated as a candidate and read (seek if the index resolves, else fall back to scanning
that SSTable's partitions and filtering). The path SHALL NEVER skip an SSTable that might contain
the partition, and SHALL NEVER return a wrong answer because an index component is missing.

#### Scenario: A definitely-absent SSTable is pruned (and counted)

- **GIVEN** a multi-SSTable fixture where the target key's bloom filter is a definite negative in
  one SSTable and positive in another
- **WHEN** the point read executes
- **THEN** the definite-negative SSTable is not opened for partition data and
  `cqlite.read.sstables_pruned` is incremented for it, while the positive SSTable is read.

#### Scenario: An index-less candidate is read, never skipped (fail-safe)

- **GIVEN** a fixture where the target partition's rows live in an SSTable that ships **only
  Data.db** (no Summary/Index/Filter — the #2295 field shape)
- **WHEN** the point read executes
- **THEN** that SSTable is still read (falling back to scanning its partitions) and the target
  partition's rows appear in the result — the missing index degrades speed, never correctness
- **AND** a variant asserting "skip on missing index" would drop the row and MUST fail.

### Requirement: The point-read path preserves cancellation and budget discipline

The point-read path SHALL honor cooperative cancellation (#2264) and the byte/row result budget and
LIMIT identically to the scan path. It SHALL poll the cancel flag before each candidate seek and
before each merge step, and SHALL map a genuine cancellation to a cancelled outcome by error
variant — never masking a real I/O/corruption error as a clean cancel. Token-range restriction SHALL
still apply: a partition whose token falls outside the split's range is excluded before any seek.

#### Scenario: A cancelled point read stops promptly without masking errors

- **GIVEN** a point read over a fixture, with the cancel flag set before the candidate seeks
- **WHEN** the request executes
- **THEN** it returns a cancelled outcome having done no full-table work, and a real I/O error that
  races the cancel surfaces as that error, not as a cancellation.

#### Scenario: LIMIT and budget bound a wide-partition point read

- **GIVEN** a `pk = <value> LIMIT k` ticket into a wide partition
- **WHEN** the point read executes
- **THEN** at most `k` rows are streamed and the result-byte budget is enforced by the same sink the
  scan path uses — the point path does not bypass the budget.

### Requirement: The taken access path is observable

The server SHALL expose an observable signal distinguishing the point-read path from the scan path,
so the field harness and round-6 evidence can confirm the pushdown did I/O-level work. When the
point path runs, the producer SHALL report the `streaming_partition_lookup` access-path label (core
`AccessPath::StreamingPartitionLookup`); when it falls back or scans, it SHALL report a full-scan
label (`full_scan` / `fallback_full_scan`). The signal SHALL ride the existing observability
contract with bounded cardinality and add no new config knob.

#### Scenario: A point read reports streaming_partition_lookup; a scan reports full_scan

- **GIVEN** the metrics-capture harness
- **WHEN** a full-PK-equality `do_get` runs (point path) and, separately, a non-PK `do_get` runs
  (scan path)
- **THEN** the first records the `streaming_partition_lookup` access-path label and the second
  records a full-scan label
- **AND** on `main` the PK-equality query reports `full_scan` (the point path does not exist) — so
  the label assertion fails before the change.

#### Scenario: The observable signal adds no unbounded attribute or config knob

- **WHEN** the change's diff and the metrics a point read emits are reviewed
- **THEN** no new environment variable, CLI flag, or ticket field is introduced, and every emitted
  attribute value is from the existing bounded catalog set.

### Requirement: The point-read path is wired through the public Flight do_get surface

The point-read path SHALL be reachable and proven end-to-end through the public Flight `do_get`
ticket surface — a real ticket carrying a pushed PK-equality predicate, resolved and streamed by the
server — not only via an internal helper unit test. The core single-partition seek primitive the
producer calls SHALL be a named public surface with a test exercising it, and the end-to-end test
SHALL assert both the correct rows and the point-path access-path signal.

#### Scenario: End-to-end do_get with a pushed PK-equality ticket

- **GIVEN** a running Flight service over a multi-SSTable fixture
- **WHEN** a client issues a `do_get` with a ticket whose effective filter is a full-PK equality
- **THEN** the streamed rows exactly match the partition's reconciled rows, the reported access path
  is `streaming_partition_lookup`, and the work-done probe confirms partitions examined ≈ candidate
  lookups — a green helper-only unit test alone does NOT satisfy this requirement.


# raw-sstable-view Specification

## Purpose
Every physical row CQLite has already decoded for a table SHALL be reachable in SQL, unreconciled,
one row per physical row per SSTable generation, with the per-cell/row/partition metadata and source
SSTable that a reconciled `SELECT` discards. Issue #4222; F2 of the forensics-surface program (epic
#4192).

## ADDED Requirements

### Requirement: The raw view is exposed under a suffixed name in the existing table namespace

For every table `<keyspace>.<table>` with a resolvable schema, CQLite SHALL expose a queryable view
named `<keyspace>.<table>_raw_sstable_data` through the same table-resolution path the base table
uses (`ks.table` string form) — no new grammar, no schema/namespace segment beyond keyspace, no
table-valued-function syntax (design.md D1).

#### Scenario: The suffixed name resolves without a query engine or parser change

- **GIVEN** the committed fixture `test_tomb/dropped_regular_col` (`test_tomb.dropped_regular_col`,
  schema `test-data/schemas/tombstone-parity.cql`)
- **WHEN** `SELECT * FROM test_tomb.dropped_regular_col_raw_sstable_data LIMIT 1` is executed through
  `cqlite query`
- **THEN** the query parses and executes as an ordinary two-segment table reference (no JOIN, no
  function call), returning at least one row.

### Requirement: The physical column contract — per-cell metadata

For every non-key column `<col>` of the base table, the raw view SHALL carry `<col>_timestamp`
(bigint, write time in microseconds), `<col>_ttl` (int, NULL when none), `<col>_local_deletion_time`
(int, NULL when the cell is live with no expiry), and `<col>_tombstone` (text, NULL \| `cell` \|
`expired`). For a collection or UDT column, the view SHALL additionally carry `<col>_complex_deletion`
(boolean) and, when true, `<col>_complex_deletion_time` / `<col>_complex_deletion_timestamp`.

#### Scenario: A cell tombstone is visible with its exact writetime and local deletion time

- **GIVEN** `test_deltas.cell_tombstones` (3 generations, `test-data/schemas/deltas.cql`)
- **WHEN** `SELECT ck, col_timestamp, col_local_deletion_time, col_tombstone FROM
  test_deltas.cell_tombstones_raw_sstable_data WHERE pk = <the deleted cell's key>` is executed
- **THEN** the row whose generation holds the `DELETE col FROM ...` shows `col_tombstone = 'cell'`
  with `col_local_deletion_time` and the deletion's writetime matching `sstabledump`'s
  `deletion_info`/cell-tombstone timestamp for that generation's `Data.db.jsonl` golden, byte-exact.

#### Scenario: A TTL'd live cell carries its declared TTL and computed expiry

- **GIVEN** `test_deltas.ttl_cells`
- **WHEN** the raw view is queried for a TTL'd key before expiry
- **THEN** `<col>_ttl` equals the declared TTL in seconds and `<col>_local_deletion_time` equals
  `sstabledump`'s `ttl`/`local_deletion_time` for that cell, and `<col>_tombstone` is NULL (the cell is
  live, not expired, in this fixture).

#### Scenario: A complex (collection) deletion marker is exposed per element's owning column

- **GIVEN** `test_deltas.collection_ops` (append/overwrite/element-remove operations)
- **WHEN** the raw view is queried for a partition whose collection column was overwritten
  (`s = {...}`, which Cassandra represents as a complex deletion covering the old elements)
- **THEN** `<collection_col>_complex_deletion = true` with `<collection_col>_complex_deletion_time`
  matching `sstabledump`'s complex-deletion `local_deletion_time`/`marked_deleted` fields for that
  generation.

### Requirement: The physical column contract — row, partition, and source columns

Every physical row SHALL carry `row_timestamp` (bigint, NULL absent an explicit liveness marker),
`row_ttl` (int), `row_local_deletion_time` (int), `row_tombstone` (text, NULL \| `row` \| `expired`),
`partition_deletion_time` / `partition_deletion_timestamp` (bigint, both NULL unless this generation
carries a partition tombstone for the key), and the source columns `sstable` (text, the Data.db file
name), `generation` (int), `format` (text, `big` \| `bti`), and `position` (bigint, the partition's
byte offset in Data.db).

#### Scenario: A row tombstone is distinguished from a cell tombstone

- **GIVEN** `test_deltas.row_tombstones`
- **WHEN** the raw view is queried for a key deleted by `DELETE FROM ... WHERE pk AND ck`
- **THEN** `row_tombstone = 'row'` with `row_local_deletion_time` matching `sstabledump`'s row-level
  deletion info, and every `<col>_tombstone` on that physical row is NULL (the tombstone is recorded
  once, at the row level, not duplicated per cell).

#### Scenario: A partition tombstone is visible even when the generation holds no live rows

- **GIVEN** `test_deltas.partition_tombstones`
- **WHEN** the raw view is queried for a partition deleted by `DELETE FROM ... WHERE pk`
- **THEN** exactly one physical row is emitted for that generation with `partition_deletion_time` /
  `partition_deletion_timestamp` set to `sstabledump`'s partition-deletion timestamp/LDT and every
  cell and clustering-key column NULL (AC2's "still yields one row" case).

#### Scenario: Source identity matches the SSTable actually holding the row, per generation

- **GIVEN** `test_compactionparity.live_clustering` (multiple generations, `compaction-parity.cql`)
- **WHEN** the raw view is queried for a key present in more than one generation
- **THEN** each returned row's `sstable`/`generation`/`format`/`position` names the specific Data.db
  file and byte offset that physical row came from, matching the on-disk directory listing and
  `sstabledump`'s reported partition offset for that generation.

### Requirement: Range tombstones are their own rows, discriminated by `row_kind`

Every physical row SHALL carry a `row_kind` column (`row` \| `partition_tombstone` \|
`range_tombstone_start` \| `range_tombstone_end`). A range tombstone SHALL be emitted as two rows (one
per bound) carrying the bound's clustering-key component values, `bound_inclusive` (boolean),
`range_deletion_time`, and `range_deletion_timestamp`; all non-key, non-range columns on those rows
SHALL be NULL.

#### Scenario: A prefix-bound range tombstone reports its partial clustering key correctly

- **GIVEN** `test_deltas.range_tombstones`, pk=1 (documented prefix bound: first clustering component
  open, second ignored — `test-data/schemas/deltas.cql` Table 3 header)
- **WHEN** the raw view is queried for that partition
- **THEN** two rows are returned with `row_kind = 'range_tombstone_start'` and `'range_tombstone_end'`
  respectively, the bound rows carry only the clustering components the range tombstone actually
  specifies (the unspecified component is NULL, not fabricated), `bound_inclusive` matches
  `sstabledump`'s reported inclusivity for each bound, and `range_deletion_time` matches the golden.

#### Scenario: Mixed open/closed inclusivity on a single range tombstone is preserved

- **GIVEN** `test_deltas.range_tombstones`, pk=3 (`ck1>1 AND ck1<=3`, mixed inclusivity)
- **WHEN** the raw view is queried
- **THEN** the start row's `bound_inclusive = false` and the end row's `bound_inclusive = true`,
  matching `sstabledump`.

### Requirement: One physical row per physical row per SSTable generation — no reconciliation

The raw view SHALL emit exactly one output row per physical row present in each SSTable generation
that holds the key (or, absent a key predicate, per physical row across the full corpus). A key
present in N generations SHALL yield at least N rows. The view SHALL NOT merge, shadow, or otherwise
reconcile rows across generations — reconciliation is `SELECT`'s job on the base table, not this
view's.

#### Scenario: A key updated across two generations yields one row per generation

- **GIVEN** `test_tomb.resurrection_gc_positive` (2 generations, a delete in gen-1 and new data in
  gen-2 for the same key, `test-data/schemas/tombstone-parity.cql`)
- **WHEN** `SELECT generation, row_tombstone FROM
  test_tomb.resurrection_gc_positive_raw_sstable_data WHERE <pk> = <the key>` is executed
- **THEN** exactly 2 rows are returned, one per generation, each with the generation's own
  `row_tombstone`/cell values matching that generation's `Data.db.jsonl` golden independently — a
  `SELECT` on the base table (which reconciles) returning a single logical row is NOT evidence this
  requirement is violated, and is asserted separately as a contrast case.

#### Scenario: A dropped-column generation shows the on-disk column the current schema no longer has

- **GIVEN** `test_tomb.dropped_regular_col` (gen-1 written before `ALTER TABLE ... DROP drop_col`,
  gen-2 after — schema `test-data/schemas/tombstone-parity.cql` Table 6)
- **WHEN** the raw view is queried for a gen-1 key
- **THEN** the gen-1 row exposes `drop_col` (and `drop_col_timestamp`/`_ttl`/`_local_deletion_time`)
  under its on-disk name with `dropped = true`, using the on-disk marshal type
  (`row_data.rs:404-428`'s detection, no longer discarded for this view) rather than being hidden;
  the gen-2 row has `dropped_col` entirely absent from the emitted column set for that row (the
  column genuinely does not exist on-disk in gen-2).

### Requirement: A partition-key predicate is pushed to the point-read path, never a scan

The raw view SHALL resolve a `WHERE <partition key> = <literal>` predicate (and, for composite
partition keys, the full-key equality form) through the existing per-generation point-read
primitives — bloom filter/Index.db for BIG, trie descent for BTI — never through a full-table scan.
This SHALL be true for both BIG (`nb`) and BTI (`da`) format tables.

#### Scenario: A BIG-format point lookup does not scan

- **GIVEN** `test_tomb.dropped_regular_col` (BIG/`nb`)
- **WHEN** `SELECT * FROM test_tomb.dropped_regular_col_raw_sstable_data WHERE pk = <a present key>`
  is executed with `CQLITE_READ_PATH=point` (per #1918's mechanism, `select_executor/forcing.rs:47`)
- **THEN** the query succeeds and returns the expected rows (a read-counter or the point-path forcing
  mechanism itself demonstrates no full-corpus scan occurred, per #1918's existing assertion style).

#### Scenario: A BTI-format point lookup uses trie descent

- **GIVEN** `test_da.wide_table` (BTI/`da`, `test-data/schemas/wide-table-bti.cql`)
- **WHEN** the raw view is queried with a partition-key equality predicate
- **THEN** the lookup resolves via `SSTableReader::lookup_partition_via_bti_trie`
  (`partition_lookup.rs:192`), not a scan, and returns every clustering row for that partition
  (mirroring the existing #953 within-partition traversal guarantee).

### Requirement: A bounded full scan works without a partition-key predicate

Without a partition-key predicate, the raw view SHALL still execute as a bounded, streaming full
corpus scan — never a whole-table or whole-corpus materialization — under the existing
`max_result_bytes` budget (`query/result_budget.rs`). The row-producing function SHALL be a true
per-generation stream (modeled on `stream_all_partitions_for_compaction`,
`data_access/compaction.rs:589`), never a `collect::<Vec<_>>()` over the full scan.

#### Scenario: A full scan is bounded by the existing result-byte budget, not table size

- **GIVEN** `test_deltas` (multiple tables, multiple generations each)
- **WHEN** `SELECT * FROM test_deltas.range_tombstones_raw_sstable_data` is executed with no `WHERE`
  clause and a small `max_result_bytes` configured
- **THEN** the query returns `Error::ResultTooLarge` (or the existing budget-exceeded behavior) rather
  than materializing the whole corpus, exactly as any other unbounded `SELECT` does today.

#### Scenario: The row producer passes the oom-audit structural check

- **GIVEN** the new scan-shaped producer function added by this change
- **WHEN** `cargo run -p xtask -- oom-audit --enforce` (the gate's `oom-audit` component) runs
- **THEN** it does not trip `STREAM_RETURNS_VEC` — the function is either a lazy stream/callback
  producer or carries an explicit budget/limit parameter recognized by `fn_is_bounded`
  (`xtask/src/oom_audit/rule.rs:348`).

### Requirement: The view is joinable via shared key columns; literal SQL `JOIN` is out of scope

The raw view SHALL share its key-column names and types with the base table so that two `SELECT`s —
one against `ks.t`, one against `ks.t_raw_sstable_data` — can be correlated on those columns by an
external consumer or a test. Literal `JOIN ... USING (...)` syntax executing is explicitly NOT
required by this change (design.md D4): CQLite's query engine has no `JOIN` executor at all
(`m2_select_validator.rs` rejects every `JOIN` keyword; `cqlite-cli/tests/unsupported_query_tests.rs:11-23`).

#### Scenario: The logical row and its physical rows are correlated by shared key values

- **GIVEN** `test_compactionparity.live_clustering`
- **WHEN** `SELECT * FROM test_compactionparity.live_clustering WHERE <pk> = ?` and `SELECT * FROM
  test_compactionparity.live_clustering_raw_sstable_data WHERE <pk> = ?` are both executed with the
  same key
- **THEN** the logical row's non-key column values equal the values of the physical row whose
  generation is the reconciled winner (per the existing query-semantics oracle for that key), and
  every physical row's key-column values equal the logical row's key-column values exactly (same
  names, same types, same encoding) — the correlation an external `JOIN ... USING (<pk>, <ck>)` would
  perform if the engine supported it.

#### Scenario: `SELECT DISTINCT sstable` answers "which generations hold this key" (folds #4205's SSTable half)

- **GIVEN** `test_tomb.resurrection_gc_positive` (2 generations)
- **WHEN** `SELECT DISTINCT sstable, generation FROM
  test_tomb.resurrection_gc_positive_raw_sstable_data WHERE <pk> = <a key present in both
  generations>` is executed
- **THEN** it returns exactly the 2 generations' `sstable`/`generation` values, matching the set
  `test_tomb.resurrection_gc_positive`'s directory listing shows for that key — the query design.md
  D2 designates as the folded-in half of #4205's scope.

### Requirement: Unknown table or missing schema is a typed error, never silent inference

CQLite SHALL raise a typed error (`Error::Table` or `Error::Schema`) when
`<keyspace>.<table>_raw_sstable_data` is queried and `<keyspace>.<table>` does not exist or has no
resolvable schema, surfaced through the CLI's existing error classification — never silently
returning zero rows and never inferring a schema from bytes.

#### Scenario: A nonexistent base table produces a typed error, not an empty result

- **GIVEN** no table `test_tomb.nonexistent_table` exists
- **WHEN** `SELECT * FROM test_tomb.nonexistent_table_raw_sstable_data` is executed via `cqlite query`
- **THEN** the CLI exits with `CliExitCode::SchemaError` (exit code 3, per `classify_error`,
  `cqlite-cli/src/error.rs:82-108`), not exit code 0 with zero rows — a deliberate divergence from the
  base SELECT path's current silent-empty behavior, scoped to this new surface only (design.md D8).

### Requirement: The raw view works through the existing `cqlite query` CLI surface unmodified

The raw view SHALL require no new CLI subcommand, flag, or output writer: `cqlite query` and the
existing `table`/`json`/`csv` writers SHALL render its results correctly, since those writers are
schema-agnostic and driven entirely by `QueryResult::metadata.columns`
(`cqlite-cli/src/output/{table,json,csv}.rs`).

#### Scenario: All three output formats render the raw view's wide, table-specific column set

- **GIVEN** `test_deltas.cell_tombstones_raw_sstable_data`
- **WHEN** the same query is run with `--format table`, `--format json`, and `--format csv`
- **THEN** each writer emits every column `metadata.columns` names (including the `<col>_*` and
  source columns), with NULLs rendered per each writer's existing contract (empty CSV cell, absent/`null`
  JSON key, empty table cell) — no writer code change required, verified by a new
  `cqlite-cli/tests/issue_4222_raw_sstable_view_cli_test.rs` integration test.

### Requirement: The column contract is a pinned public surface

A committed snapshot test SHALL capture the full column set (base columns + per-cell metadata +
row/partition/source/range-tombstone columns) for at least one fixture table, so an unintentional
column rename, addition, or removal is caught as a diff rather than discovered downstream (per the
issue's own doctrine note and this repo's public-surface conventions).

#### Scenario: The column snapshot fails on an unreviewed schema drift

- **GIVEN** the committed snapshot of `test_tomb.dropped_regular_col_raw_sstable_data`'s column names
  and types
- **WHEN** the raw view's schema-synthesis function's output changes (a column renamed, added, or
  removed) without updating the snapshot
- **THEN** the snapshot test fails, naming the exact diff.

### Requirement: Physical-dump parity is the correctness oracle

Every metadata column's value, for every fixture named in this spec, SHALL match `sstabledump`'s
corresponding field for that cell/row/partition, byte-exact for timestamps — the same physical-dump
parity oracle (#1742) used elsewhere in this codebase, never CQLite's own prior output (#3041/#3042).

#### Scenario: Every named fixture passes byte-exact parity

- **GIVEN** the fixtures named across this spec's scenarios (`test_tomb/*`, `test_deltas/*`,
  `test_compactionparity/live_clustering`, `test_da.wide_table`, `test_comp/lz4_table`,
  `test_comp/uncompressed_table`) — spanning ≥2 generations, tombstone/TTL variety, BTI, and both a
  compressed and an uncompressed table, per #4222 AC1
- **WHEN** the raw view is queried for every key in each fixture's JSONL golden
- **THEN** every timestamp, TTL, local-deletion-time, and tombstone-kind value in the result matches
  the golden exactly, and no key present in the golden is missing from the raw view's output.

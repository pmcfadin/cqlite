# frozen-inner-udt-decode — delta for typed-inner-udt-frozen-collections

## ADDED Requirements

### Requirement: Inner frozen-UDT elements decode to typed Udt from authoritative metadata
The reader SHALL decode a `frozen<UDT>` element inside a frozen collection (list/set element,
map key, map value; recursively at any nesting depth, including UDT-in-UDT fields and UDTs
inside tuples inside frozen collections) to a typed `Value::Frozen(Value::Udt(..))` — the same
shape a top-level `frozen<udt>` column produces — using **authoritative metadata only**, with
resolution precedence: (1) the column's SerializationHeader marshal type
(`RowColumnResolution.header_type`), (2) a wired `UdtRegistry`, (3) opaque `Value::Blob` when
neither authority resolves the type. The decoder SHALL NOT infer structure from byte patterns
(no-heuristics mandate #28).

#### Scenario: Registry-less reader decodes a frozen list of frozen UDTs typed
- **GIVEN** the committed #1240 fixture `test_compactionparityudt/udt_collections-*` (column `lp frozen<list<frozen<person>>>`)
- **AND** an `SSTableReader` opened WITHOUT any `UdtRegistry`
- **WHEN** the row is decoded (query scan or `iterate_all_partitions_for_compaction`)
- **THEN** each `lp` element is `Value::Frozen(Value::Udt(..))` with field names and typed field values matching the sstabledump JSONL golden
- **AND** element count and order match the golden

#### Scenario: Frozen map with frozen-UDT values decodes typed, keys preserved
- **GIVEN** the same fixture's `ma frozen<map<text, frozen<address>>>` column on a registry-less reader
- **WHEN** the row is decoded
- **THEN** each map value is a typed `Value::Udt` matching the JSONL golden and the key set is preserved

#### Scenario: Null inner UDT field survives typed decode
- **GIVEN** the #1289 fixture `test_compactionparityudt/udt_null_inner-*` (inner `person.last_name` / `address.city` null)
- **WHEN** the row is decoded typed
- **THEN** the null field is represented as null inside the typed `Value::Udt` (not dropped, not a decode error), matching the JSONL golden

#### Scenario: Unresolvable inner type stays an opaque Blob, never a guess
- **GIVEN** a frozen-collection column whose `header_type` is absent and whose inner UDT name is not in any wired registry
- **WHEN** the element is decoded
- **THEN** the element is `Value::Blob` carrying the exact frozen element bytes
- **AND** no error/panic is raised and no structure is inferred from the bytes

### Requirement: The typed contract is uniform across read paths and equals the registry decode
The typed inner-UDT representation SHALL be identical whether produced by the header-marshal
mechanism (registry-less reader) or by the existing `UdtRegistry` mechanism, and identical
across the query read path and the compaction read path (`CompactionRow` simple cells), since
one shared element decoder serves both.

#### Scenario: Marshal-driven and registry-driven decode are value-equal
- **GIVEN** the #1240 fixture decoded twice: once via a registry-less reader, once via a reader with the DDL-built `UdtRegistry` wired
- **WHEN** the `lp` and `ma` cells are compared
- **THEN** the decoded `Value`s are equal (same variant shapes, field names, field values)

#### Scenario: Compaction read path carries the typed value into CompactionRow
- **WHEN** `iterate_all_partitions_for_compaction` (registry-less) decodes the fixture
- **THEN** the frozen-collection `SimpleCell.value` contains typed `Value::Udt` inner elements identical to the query-path decode of the same row

### Requirement: Byte-for-byte compaction parity is unchanged
The read-side representation change SHALL NOT alter any written byte: compacting the #1240 /
#1289 fixtures SHALL produce Data.db, Index.db, Summary.db, and Digest.crc32 byte-identical to
the committed Cassandra 5.0.2 goldens, exactly as before the change.

#### Scenario: #1240 and #1289 byte-parity tiers still pass
- **WHEN** `issue_1240_nested_frozen_collection_udt_parity.rs` runs after the change (with its tier-1b assertions updated per the test's embedded guidance to compare typed UDTs)
- **THEN** the tier-2 byte-parity assertions pass unchanged against the committed goldens
- **AND** the test FAILS loudly (does not silently pass) if run against a dataset where the fixture is present but yields zero rows

#### Scenario: Query-surface parity goldens do not move
- **WHEN** the full parity suite runs (33-table sstabledump/JSONL parity + Python parity tests)
- **THEN** all pass with unchanged values (the schema-registry-wired query surface was already typed, so no golden churn)

### Requirement: Wiring evidence through a named public surface
The change SHALL be exercised end-to-end through a named public surface — at minimum
`SSTableReader::open` (registry-less) + `iterate_all_partitions_for_compaction`, and one
query-visible surface (`SELECT` via the query engine / CLI one-shot) — against the real #1240
SSTable fixture, validating decoded inner-UDT **field values** (not just row counts) against
the sstabledump JSONL golden. A helper-only unit test SHALL NOT count as done.

#### Scenario: End-to-end SELECT over the fixture returns structured inner UDTs
- **GIVEN** the #1240 fixture and its schema (no registry beyond what the surface itself builds)
- **WHEN** `SELECT lp, ma FROM test_compactionparityudt.udt_collections` executes through the public query surface
- **THEN** the result exposes structured inner UDT fields whose values match the sstabledump JSONL golden
- **AND** the test is dataset-guarded such that fixture-present-but-zero-rows is a FAILURE

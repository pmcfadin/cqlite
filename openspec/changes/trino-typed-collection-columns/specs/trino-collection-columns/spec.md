# trino-collection-columns Specification

## Purpose

Project Cassandra collection columns (`list`/`set`/`map`, including collections of `frozen<udt>`) through
the Trino connector as typed Trino complex columns (`array`/`row`/`map`), instead of silently dropping
them from the schema. Any column that still cannot be mapped must be surfaced loudly and durably rather
than hidden without a trace.

## ADDED Requirements

### Requirement: Arrow List/Struct/Map map to Trino array/row/map

The connector's Arrow→Trino type mapping (`ArrowTypeMapper`) SHALL map an Arrow `List` field to a Trino
`array(E)`, an Arrow `Struct` field to a Trino `row(...)` of its named child fields, and an Arrow `Map`
field to a Trino `map(K, V)`, recursively resolving element / child / key / value types through the same
mapping. It SHALL return "unsupported" (`Optional.empty()` / typed error) only when a genuinely
unmappable *leaf* type is reached, not merely because a type is complex.

#### Scenario: Arrow List of Utf8 maps to array(varchar)
- **WHEN** `ArrowTypeMapper.toTrinoOrEmpty` is given an Arrow `List` field whose element is `Utf8`
- **THEN** it returns a Trino `array(varchar)` type
- **AND** it does not return `Optional.empty()`

#### Scenario: Arrow Struct maps to a Trino row with named fields
- **WHEN** the mapper is given an Arrow `Struct` field with children `street: Utf8`, `zip: Int(32)`
- **THEN** it returns a Trino `row(street varchar, zip integer)` preserving field names and order

#### Scenario: Arrow Map maps to a Trino map
- **WHEN** the mapper is given an Arrow `Map` field with key `Utf8` and value `Int(32)`
- **THEN** it returns a Trino `map(varchar, integer)` type

#### Scenario: Nested collection maps recursively
- **WHEN** the mapper is given an Arrow `List` whose element is itself a `List` of `Utf8`
- **THEN** it returns a Trino `array(array(varchar))`

#### Scenario: A collection of an unmappable leaf is still unsupported
- **WHEN** the mapper is given an Arrow `List` whose element leaf type is one the connector cannot map (e.g. an unsupported timestamp unit)
- **THEN** it returns `Optional.empty()` (the whole column is treated as unsupported)
- **AND** the failure is attributable to the leaf type, not to the column being a collection

### Requirement: Collection columns are materialized into Trino blocks

The connector's Arrow→Trino value conversion (`ArrowToTrino`) SHALL materialize an Arrow `ListVector` into
a Trino ARRAY block, a `StructVector` into a Trino ROW block, and a `MapVector` into a Trino MAP block,
recursively writing each element / field / entry via the existing scalar writers. A null collection cell
SHALL append a null; an empty collection SHALL append an empty (non-null) array/map. The set of Arrow
types `ArrowToTrino` can materialize MUST stay in lockstep with what `ArrowTypeMapper` advertises — the
connector never advertises a column type it cannot build.

#### Scenario: A list<text> column materializes to an array block
- **WHEN** a Flight batch delivers a `ListVector` of `Utf8` for a projected `array(varchar)` column
- **THEN** each row's Trino ARRAY block contains the list's elements in order
- **AND** a null list cell yields a null block entry
- **AND** an empty list yields an empty (non-null) array

#### Scenario: A list<frozen<udt>> column materializes end to end
- **WHEN** a Flight batch delivers a `ListVector` of `Utf8` (server-decoded UDT strings) for a projected `array(varchar)` column
- **THEN** each row's ARRAY block contains one VARCHAR element per UDT, equal to the server-decoded UDT string

#### Scenario: Mapper and materializer are kept in lockstep
- **WHEN** the round-trip test enumerates every Arrow type `ArrowTypeMapper` accepts
- **THEN** `ArrowToTrino` materializes each one without an `UnsupportedOperationException`

### Requirement: list<frozen<udt>> is queryable through Trino end to end

A Cassandra table with a `list<frozen<udt>>` column SHALL expose that column through the Trino connector:
it MUST appear in `DESCRIBE`, be returned by `SELECT *`, and be resolvable by `SELECT <col>`. The returned
value MUST be a Trino `array(varchar)` whose elements are the server-decoded UDT renderings, element-for-
element consistent with the on-disk data.

#### Scenario: DESCRIBE shows the collection column
- **WHEN** a client runs `DESCRIBE` on a table with `addrs list<frozen<udt>>` through the connector
- **THEN** the result includes an `addrs` column of type `array(varchar)`

#### Scenario: SELECT of the collection column returns the elements
- **WHEN** a client runs `SELECT addrs FROM <table>` through docker-compose Trino against a table whose row has two UDT elements
- **THEN** the query resolves (no "column cannot be resolved" error)
- **AND** the returned array has two elements, each the server-decoded UDT string
- **AND** `SELECT *` on the same row includes the `addrs` column and its value

#### Scenario: Empty and absent collections are distinguishable
- **WHEN** the table has one row with an empty `addrs` list and one row where `addrs` was never set
- **THEN** the empty-list row returns an empty (non-null) array
- **AND** the never-set row returns null

### Requirement: Unsupported columns are surfaced loudly and durably, never silently dropped

When the connector cannot map a column's type, it SHALL log a WARNING naming the column and its Arrow type
**every time** that table's schema is projected with hidden columns — not once per table per connector
instance. Silent omission of a DDL-declared column is prohibited. When *every* column of a table is
unsupported the connector SHALL fail with a clear error rather than present a zero-column table.

#### Scenario: The hidden-column warning is not suppressed after the first DESCRIBE
- **WHEN** a table with a still-unsupported column is projected twice (two `DESCRIBE`/metadata loads) via the same connector instance
- **THEN** a WARNING naming the hidden column and its Arrow type is emitted on both projections

#### Scenario: A fully-unsupported table fails loudly
- **WHEN** a table's every column uses a type the connector cannot map
- **THEN** projecting it raises a clear error naming the unsupported types
- **AND** the connector does not present a queryable zero-column table

---
title: "Appendix A -- CQL->SSTable Type Mapping"
description: "In this appendix you will..."
sidebar:
  label: "Appendix A -- CQL->SSTable Type Mapping"
  order: 101
---

In this appendix you will learn:
- How CQL primitive, collection, UDT, and vector types map to on-disk encodings
- Where Cassandra 5.0 defines serialization for rows and cells
- How Cassandra defines serialization boundaries via `SerializationHeader` and type marshallers

This appendix consolidates the type mapping tables for Cassandra 5.0 SSTables and pins to precise upstream sources that define encodings and marshalling.

## Tables

- Primitives: see `tables/type-mapping-primitives.md`
- Collections and Tuples: see `tables/type-mapping-collections.md`
- Complex (UDT, frozen, vector): see `tables/type-mapping-complex.md`

## Worked Examples

- Nested frozen collection (`frozen<map<text, frozen<list<int>>>>`), 2 entries:
  - Encoding: entry count (**4-byte BE i32**) -> for each entry: key (4-byte BE i32 len + UTF-8),
    then value (4-byte BE i32 len + the inner list's own blob: 4-byte BE i32 elem count ->
    each element 4-byte BE i32 len + 4 int bytes). Every prefix inside a frozen blob is
    fixed-width, **not** VInt.
  - Size rule of thumb: total_size ~= 4 + sum[ (4 + |key|) + (4 + |inner_list_blob|) ]

- UDT with optional fields (frozen):
  - Encoding: for each field in definition order: **4-byte BE signed int** length + value bytes; null field uses length = 0xFFFFFFFF (-1 as signed int)
  - Size rule of thumb: total_size ~= sum (4 + max(len_i, 0))

## Upstream anchors (cassandra-5.0.8)

- Serialization header and schema-driven encodings
  - `org.apache.cassandra.db.SerializationHeader`
    - `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java`
- Type marshallers (`org.apache.cassandra.db.marshal.*`)
  - Directory: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal`
  - Representative primitives:
    - `LongType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/LongType.java`
    - `Int32Type` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/Int32Type.java`
    - `UTF8Type` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/UTF8Type.java`
    - `AsciiType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/AsciiType.java`
    - `UUIDType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/UUIDType.java`
    - `TimeUUIDType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/TimeUUIDType.java`
    - `TimestampType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/TimestampType.java`
    - `InetAddressType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/InetAddressType.java`
    - `DecimalType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/DecimalType.java`
    - `DurationType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/DurationType.java`
    - `IntegerType` (varint) -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/IntegerType.java`
    - `CounterColumnType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/CounterColumnType.java`
  - Collections and complex:
    - `ListType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/ListType.java`
    - `SetType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/SetType.java`
    - `MapType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/MapType.java`
    - `TupleType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/TupleType.java`
    - `UserType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/UserType.java`
    - `VectorType` -- `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/VectorType.java`

## Notes

- **Frozen collection counts and element lengths** (`frozen<list/set/map>`) use **fixed 4-byte BE
  i32**, not VInt: `CollectionSerializer.pack` writes the count via `writeCollectionSize`
  (`putInt`, `CollectionSerializer.java:67-70`) and each element via `writeValue`
  (`putInt` + bytes, `:82-92`); `-1` = NULL element. See Chapter 5, "Frozen Collection Serialization".
- **Non-frozen collection cells** always length-prefix the cell **path** with an unsigned VInt, and
  length-prefix the cell **value** *iff* `HAS_EMPTY_VALUE` (`0x04`) is clear — each element is its own
  cell, so the framing differs from the frozen form:
  - The cell **path** (never flag-gated): `CollectionType.CollectionPathSerializer.serialize` →
    `ByteBufferUtil.writeWithVIntLength` (`CollectionType.java:361-366`), which is
    `writeUnsignedVInt32(remaining)` + bytes (`ByteBufferUtil.java:356-360`).
  - The cell **value** is present only when `HAS_EMPTY_VALUE` is clear, and that flag is
    **size-driven, not type-driven**: `Cell.Serializer.serialize` sets it from `cell.valueSize() > 0`
    (`Cell.java:271`, `:277-278`) and writes the value only `if (hasValue)` (`:303-304`); the reader
    mirrors it (`:310`, `:329-339`). So a **zero-length value carries no length VInt and no bytes** —
    not a `0x00`. Instances of that one rule: a non-frozen `set<T>` element (datum in the path,
    `SetType.valueComparator()` is `EmptyType.instance`, `SetType.java:106-109`), any zero-length value
    (`map<text,text>` entry `-> ''`, empty blob in a `list<blob>`), and an element tombstone
    (`IS_DELETED`, `Cell.java:264`).
  - **When the value IS present it is length-prefixed even for a fixed-width element/value type**
    (`list<int>`, `map<text,bigint>`): `Cell.Serializer.serialize` calls
    `header.getType(column).writeValue(...)` (`Cell.java:303-304`), and `header.getType(column)` is the
    **column's** type — the collection type, never the element type
    (`SerializationHeader.java:160-163`, map built from `column.type` at `:250-257`).
    `CollectionType`/`ListType`/`MapType`/`SetType` do not override `valueLengthIfFixed()`, so they
    inherit `VARIABLE_LENGTH = -1` (`AbstractType.java:62`, `:490-493`) and `writeValue` takes
    the `else` branch → `accessor.writeWithVIntLength(...)` (`:550-552`).
  - The `valueLengthIfFixed() >= 0` raw-bytes branch (`AbstractType.java:538-543`) fires only for
    **simple (non-collection)** cells, whose scalar type overrides it (e.g. `Int32Type` → `4`,
    `Int32Type.java:156-159`).

  See Chapter 5, "Non-Frozen Collection Serialization".
- **Tuple and UDT field lengths** use **4-byte BE signed int** (not VInt), with no field count on
  disk. Null fields are encoded as 0xFFFFFFFF (-1). Source: `TupleType.java:341-364`;
  `UserType extends TupleType` (`UserType.java:52,194`), so the two are byte-identical.
- **UDT type names in DDL: accept BOTH bare and keyspace-qualified.** A CQL type string naming a UDT
  may arrive either as `frozen<addr>` or as `frozen<my_ks.addr>`, and *which* form you get depends on
  who produced the DDL — not on the SSTable. A schema parser must accept both and resolve the UDT by
  splitting on the first `.`.
  - **Cassandra server-side emits the BARE name.** `CQL3Type.UserDefined.toString()` returns
    `ColumnIdentifier.maybeQuote(name)` (or `"frozen<" + maybeQuote(name) + '>'`) with no keyspace
    prefix (`CQL3Type.java:413-420`), and every server path funnels through it:
    `CqlBuilder.append(AbstractType)` uses `type.asCQL3Type().toString()` (`CqlBuilder.java:134-137`),
    which drives `DESCRIBE TABLE` / `SchemaCQLHelper` snapshot `schema.cql`, and
    `SchemaKeyspace.addColumnToSchemaMutation` stores the same bare string in
    `system_schema.columns.type` (`SchemaKeyspace.java:746`). The `DESCRIBE` goldens in
    `test/unit/org/apache/cassandra/cql3/statements/DescribeStatementTest.java` show exactly
    `b frozen<<bare type name>>`. `UserType.getCqlTypeName()` *is* keyspace-qualified
    (`String.format("%s.%s", …)`, `UserType.java:450-452`) but is used only in `ALTER TYPE` error
    messages, not in emitted DDL.
  - **The qualified form comes from the DataStax Java driver.**
    `com.datastax.oss.driver.api.core.type.UserDefinedType.asCql(boolean includeFrozen, boolean pretty)`
    / `describe(...)` format with `frozen<%s.%s>` / `%s.%s` (verified in the constant pool of
    `java-driver-core-4.19.2.jar`). Any tool that renders schema through the driver therefore emits
    `frozen<keyspace.typename>` — including Cassandra Sidecar's
    `GET /api/v1/keyspaces/<ks>/schema`, which is how CQLite's Trino connector obtains DDL.
  - CQLite accepts both: `cqlite-core/src/schema/cql_parser.rs` (`qualified_type_name` parses
    `identifier ('.' identifier)?`) and `split_qualified_udt` in
    `cqlite-core/src/schema/udt_registry.rs` (re-exported from `cqlite-core/src/schema/mod.rs:24`),
    used at every registry lookup site (issue #2807).
  - This is a **DDL/schema-text** concern only. Nothing on disk carries a CQL type string of this
    form: the `Statistics.db` `SerializationHeader` records the *internal marshal* form,
    `org.apache.cassandra.db.marshal.UserType(keyspace,hex_encoded_name,field:type,…)`, which is
    always keyspace-bearing (see Appendix B, "Tuple and UDT Field Encoding").
- **Vector types**: fixed-element vectors (`vector<float,n>`) write no length prefix -- layout is exactly `n x elementSize` bytes concatenated. Source: `VectorType.java:477-493`, `FixedLengthSerializer`.

## Key Takeaways
- Primitive numeric and time types are fixed-width big-endian values.
- Cell values for strings and blobs are length-prefixed with **unsigned** VInt (never ZigZag). So is
  every non-frozen collection cell value **that is present at all** (i.e. `HAS_EMPTY_VALUE` clear),
  fixed-width element types included — the fixed-width no-prefix rule is a **simple-cell** rule, and a
  zero-length collection value is omitted entirely rather than written as a `0x00` length.
- Frozen collection counts and element lengths, and tuple/UDT field lengths, are fixed 4-byte BE
  signed int (-1 = null), **not** VInt.
- Signed (ZigZag) VInts in `Data.db` occur only inside a serialized `DurationType` payload — its
  months/days/nanos — **wherever that payload appears**, including nested inside a collection, tuple, or
  UDT (`frozen<list<duration>>`, a `duration` UDT field). Cassandra tracks this recursion via
  `referencesDuration()` (`DurationType.java:96-99`; `TupleType.java:125-128` recurses over
  `allTypes()`). Every structural VInt in `Data.db` — length prefix, count, timestamp/TTL/deletion delta
  — is unsigned. Signed VInt is not unique to `duration` across the whole component set either:
  `Index.db`'s promoted index writes a signed width delta (`IndexInfo.java:96,111-112`). See Appendix B.
- Fixed-element vectors (e.g., `vector<float,n>`) are raw concatenated elements with no length prefix.
- Serialization is schema-driven; `SerializationHeader` and the `db.marshal` types define exact encodings.

## References
- Cassandra 5.0.8: see Upstream anchors above for pinned links

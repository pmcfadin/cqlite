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

- Nested collection (map<text, list<int>>), 2 entries:
  - Encoding: count (VInt) -> for each entry: key (VInt len + UTF-8), value list (VInt elem count -> each int as 4 bytes)
  - Size rule of thumb: total_size ~= size(VInt(count)) + sum[ size(VInt(|key|)) + |key| + size(VInt(list_len)) + 4*list_len ]

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

- **Collection element lengths** (list, set, map) use unsigned VInt encoding (`CollectionSerializer.java:43,51`). See Appendix B for VInt details.
- **Tuple and UDT field lengths** use **4-byte BE signed int** (not VInt). Null fields are encoded as 0xFFFFFFFF (-1). Source: `TupleType.java:345-359`.
- **Vector types**: fixed-element vectors (`vector<float,n>`) write no length prefix -- layout is exactly `n x elementSize` bytes concatenated. Source: `VectorType.java:477-493`, `FixedLengthSerializer`.

## Key Takeaways
- Primitive numeric and time types are fixed-width big-endian values.
- Strings and blobs are length-prefixed with VInt; collection counts and element lengths also use unsigned VInt.
- Tuple and UDT field lengths use 4-byte BE signed int; -1 (0xFFFFFFFF) = null.
- Fixed-element vectors (e.g., `vector<float,n>`) are raw concatenated elements with no length prefix.
- Serialization is schema-driven; `SerializationHeader` and the `db.marshal` types define exact encodings.

## References
- Cassandra 5.0.8: see Upstream anchors above for pinned links

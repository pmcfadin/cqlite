# Appendix A -- CQL->SSTable Type Mapping

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
- **Non-frozen collection cells** frame the **path** and the **value** by *different* rules — each
  element is its own cell, so the framing differs from the frozen form:
  - The cell **path** is **always** unsigned-VInt-length-prefixed:
    `CollectionType.CollectionPathSerializer.serialize` → `ByteBufferUtil.writeWithVIntLength`
    (`CollectionType.java:361-366`), which is `writeUnsignedVInt32(remaining)` + bytes
    (`ByteBufferUtil.java:356-360`).
  - The cell **value** is **not** uniform — `AbstractType.writeValue` (`AbstractType.java:535-552`)
    branches on `valueLengthIfFixed()`: for non-frozen `set<T>` the value is **empty** (the element is
    the path; `SetType.valueComparator()` → `EmptyType.instance`, `SetType.java:105-108`);
    **fixed-width** value types (e.g. `list<int>`, `map<text,int>` values) are written **raw with no
    length prefix** (`accessor.write(value, out)`, `AbstractType.java:538-543`); **variable-width**
    value types (e.g. `list<text>`) get an unsigned-VInt length + bytes
    (`accessor.writeWithVIntLength(value, out)`, `AbstractType.java:550-552`).

  See Chapter 5, "Non-Frozen Collection Serialization".
- **Tuple and UDT field lengths** use **4-byte BE signed int** (not VInt), with no field count on
  disk. Null fields are encoded as 0xFFFFFFFF (-1). Source: `TupleType.java:341-364`;
  `UserType extends TupleType` (`UserType.java:52,194`), so the two are byte-identical.
- **Vector types**: fixed-element vectors (`vector<float,n>`) write no length prefix -- layout is exactly `n x elementSize` bytes concatenated. Source: `VectorType.java:477-493`, `FixedLengthSerializer`.

## Key Takeaways
- Primitive numeric and time types are fixed-width big-endian values.
- Cell values for strings and blobs are length-prefixed with **unsigned** VInt (never ZigZag).
- Frozen collection counts and element lengths, and tuple/UDT field lengths, are fixed 4-byte BE
  signed int (-1 = null), **not** VInt.
- `duration` is the only `Data.db` row/cell **value** made of signed (ZigZag) VInts; every length
  prefix, timestamp, TTL, and deletion delta in `Data.db` is unsigned. Signed VInt is not unique to
  `duration` across the whole component set — `Index.db`'s promoted index writes a signed width delta
  (`IndexInfo.java:96,111-112`). See Appendix B.
- Fixed-element vectors (e.g., `vector<float,n>`) are raw concatenated elements with no length prefix.
- Serialization is schema-driven; `SerializationHeader` and the `db.marshal` types define exact encodings.

## References
- Cassandra 5.0.8: see Upstream anchors above for pinned links

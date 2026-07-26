## CQL -> SSTable Type Mapping -- Collections and Tuples (Cassandra 5.0)

| CQL type | On-disk representation | Notes |
|---|---|---|
| `frozen<list<T>>` | **4-byte BE i32** element count; for each element: [**4-byte BE i32** length + value bytes] | One cell holding the packed blob. `pack` → `writeCollectionSize` (`putInt`) + `writeValue` (`putInt` + bytes). `-1` length = NULL element. Source: `CollectionSerializer.java:52-92,123-126`. |
| `frozen<set<T>>` | Same as `frozen<list<T>>` | Elements are written in the element-type comparator's order |
| `frozen<map<K,V>>` | **4-byte BE i32** entry count; for each entry: key value then value value, each with its own **4-byte BE i32** length | `MapSerializer.serializeValues` flattens sorted (key, value) pairs, then `pack` frames each with `putInt`. Keys unique, ordered by key comparator. Source: `MapSerializer.java:66-79`, `CollectionSerializer.java:52-92`. |
| `list<T>` / `set<T>` / `map<K,V>` (non-frozen) | One cell per element/entry: cell **path** and cell **value** each behind an **unsigned VInt** length — including when the element/value type is fixed-width. `set<T>` is the exception: no value bytes at all (see below) | Multi-cell complex column, preceded by an unsigned-VInt cell count. Source: `UnfilteredSerializer.java:277`, `CollectionType.java:361-366`, `Cell.java:303-304`, `AbstractType.java:535-552`. |
| `tuple<T1,...,Tn>` | For each field: [**4-byte BE signed int** length + value bytes]; null fields encoded as 0xFFFFFFFF (-1) | Field lengths are 4-byte BE int, **not** VInt; no field count on disk (arity from schema). Source: `TupleType.java:341-364`. |
| `frozen<...>` | Payload serialized as a single value using inner type encoding | Prevents updates by sub-component |

**Frozen collection counts and element lengths** are fixed 4-byte BE `i32`, **not** VInt
(`CollectionSerializer.java:67-92`).

**Non-frozen collection cell framing** — path and value are both unsigned-VInt length-prefixed:

- **Cell path: always** an unsigned-VInt length + bytes.
  `CollectionType.CollectionPathSerializer.serialize` → `ByteBufferUtil.writeWithVIntLength`
  (`CollectionType.java:361-366`), and `writeWithVIntLength` is
  `out.writeUnsignedVInt32(bytes.remaining())` + bytes (`ByteBufferUtil.java:356-360`).
- **Cell value: always** an unsigned-VInt length + bytes, `list<int>` exactly like `list<text>`.
  `Cell.Serializer.serialize` writes `header.getType(column).writeValue(...)` (`Cell.java:303-304`) and
  `header.getType(column)` returns the **column's** type — the collection type, never the element type
  (`SerializationHeader.java:160-163`, built from `column.type` at `:250-257`).
  `CollectionType`/`ListType`/`MapType`/`SetType` never override `valueLengthIfFixed()`, so they inherit
  `VARIABLE_LENGTH = -1` (`AbstractType.java:62`, `:490-493`) and `writeValue` always takes the `else`
  branch → `accessor.writeWithVIntLength(value, out)` (`AbstractType.java:550-552` →
  `ValueAccessor.java:171-175`).
- **Exception — `set<T>` (non-frozen) writes no value bytes.** The element *is* the cell path and
  `SetType.valueComparator()` returns `EmptyType.instance` (`SetType.java:106-109`), so the cell sets
  `HAS_EMPTY_VALUE_MASK` (`Cell.java:264`, `:271-277`) and `writeValue` is never called (`:303-304`).
  This is **flag-driven, not fixed-width-driven** — `set<int>` and `set<text>` behave identically.
- The `valueLengthIfFixed() >= 0` raw-bytes branch (`AbstractType.java:538-543`) applies to **simple
  (non-collection)** cells only, where the scalar type overrides it (e.g. `Int32Type` → `4`,
  `Int32Type.java:156-159`).

**Tuple field lengths** use 4-byte BE signed int: -1 (0xFFFFFFFF) = null, 0 = empty, >0 = byte count. See Appendix B for VInt details.

- Cassandra 5.0.8: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java`)

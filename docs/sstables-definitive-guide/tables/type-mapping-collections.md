## CQL -> SSTable Type Mapping -- Collections and Tuples (Cassandra 5.0)

| CQL type | On-disk representation | Notes |
|---|---|---|
| `frozen<list<T>>` | **4-byte BE i32** element count; for each element: [**4-byte BE i32** length + value bytes] | One cell holding the packed blob. `pack` → `writeCollectionSize` (`putInt`) + `writeValue` (`putInt` + bytes). `-1` length = NULL element. Source: `CollectionSerializer.java:52-92,123-126`. |
| `frozen<set<T>>` | Same as `frozen<list<T>>` | Elements are written in the element-type comparator's order |
| `frozen<map<K,V>>` | **4-byte BE i32** entry count; for each entry: key value then value value, each with its own **4-byte BE i32** length | `MapSerializer.serializeValues` flattens sorted (key, value) pairs, then `pack` frames each with `putInt`. Keys unique, ordered by key comparator. Source: `MapSerializer.java:66-79`, `CollectionSerializer.java:52-92`. |
| `list<T>` / `set<T>` / `map<K,V>` (non-frozen) | One cell per element/entry: cell path and cell value each behind an **unsigned VInt** length | Multi-cell complex column, preceded by an unsigned-VInt cell count. Fixed-width element types carry no value length prefix. Source: `UnfilteredSerializer.java:277`, `CollectionType.java:361-366`, `AbstractType.java:535-552`. |
| `tuple<T1,...,Tn>` | For each field: [**4-byte BE signed int** length + value bytes]; null fields encoded as 0xFFFFFFFF (-1) | Field lengths are 4-byte BE int, **not** VInt; no field count on disk (arity from schema). Source: `TupleType.java:341-364`. |
| `frozen<...>` | Payload serialized as a single value using inner type encoding | Prevents updates by sub-component |

**Frozen collection counts and element lengths** are fixed 4-byte BE `i32`, **not** VInt
(`CollectionSerializer.java:67-92`). Only *non-frozen* collection cells use unsigned-VInt
path/value prefixes.
**Tuple field lengths** use 4-byte BE signed int: -1 (0xFFFFFFFF) = null, 0 = empty, >0 = byte count. See Appendix B for VInt details.

- Cassandra 5.0.8: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java`)

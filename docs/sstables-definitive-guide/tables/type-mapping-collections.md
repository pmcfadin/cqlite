## CQL -> SSTable Type Mapping -- Collections and Tuples (Cassandra 5.0)

| CQL type | On-disk representation | Notes |
|---|---|---|
| `list<T>` | Unsigned VInt element count; for each element: [unsigned VInt length + value bytes or fixed-size value] | Elements serialized using `T`'s encoding. Count and element lengths use `writeUnsignedVInt32`. Source: `CollectionSerializer.java:43,51`. |
| `set<T>` | Same as `list<T>` with set semantics | Elements sorted by comparator on read paths |
| `map<K,V>` | Unsigned VInt entry count; for each entry: key then value serialized as per types | Keys must be unique; order by key comparator. Source: `CollectionSerializer.java:58`. |
| `tuple<T1,...,Tn>` | For each field: [**4-byte BE signed int** length + value bytes]; null fields encoded as 0xFFFFFFFF (-1) | Field lengths are 4-byte BE int, **not** VInt. Source: `TupleType.java:345-359`. |
| `frozen<...>` | Payload serialized as a single value using inner type encoding | Prevents updates by sub-component |

**Collection element lengths** (list, set, map) use unsigned VInt encoding (`CollectionSerializer.java`).
**Tuple field lengths** use 4-byte BE signed int: -1 (0xFFFFFFFF) = null, 0 = empty, >0 = byte count. See Appendix B for VInt details.

- Cassandra 5.0.8: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java`)

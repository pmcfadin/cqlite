## CQL → SSTable Type Mapping — Collections and Tuples (Cassandra 5.0)

| CQL type | On-disk representation | Notes |
|---|---|---|
| `list<T>` | VInt element count; for each element: [VInt length + value bytes or fixed-size value] | Elements serialized using `T`'s encoding |
| `set<T>` | Same as `list<T>` with set semantics | Elements sorted by comparator on read paths |
| `map<K,V>` | VInt entry count; for each entry: key then value serialized as per types | Keys must be unique; order by key comparator |
| `tuple<T1,...,Tn>` | For each field: [VInt length + value bytes or fixed-size value]; null fields encoded as -1 length | Field order fixed |
| `frozen<...>` | Payload serialized as a single value using inner type encoding | Prevents updates by sub-component |

Length fields above are VInt-encoded. See the Encodings cheat sheet for VInt details.

- Cassandra 5.0: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java`)


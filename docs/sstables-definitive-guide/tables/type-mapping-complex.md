## CQL → SSTable Type Mapping — Complex Types (Cassandra 5.0)

| CQL type | On-disk representation | Notes |
|---|---|---|
| `udt<name>` | For each field (in UDT definition order): [VInt length + value bytes or fixed-size value]; missing field = -1 length | Field metadata lives in schema; payload is positional |
| `frozen<udt<...>>` | Same as UDT, treated atomically | |
| `vector<float,n>` | VInt length (n) + `n` 32-bit floats (big-endian) | Vector CQL type introduced in 5.x; indices handled by SAI |

References:

- Cassandra 5.0: `org.apache.cassandra.db.marshal.VectorType` (`https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/marshal/VectorType.java`)
- Cassandra 5.0: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java`)


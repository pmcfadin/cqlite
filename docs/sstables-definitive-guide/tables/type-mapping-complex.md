## CQL → SSTable Type Mapping — Complex Types (Cassandra 5.0)

| CQL type | On-disk representation | Notes |
|---|---|---|
| `udt<name>` | For each field (in definition order): [4-byte BE i32 length][value bytes] | -1=null, 0=empty, >0=data length |
| `frozen<udt<...>>` | Same as UDT, serialized as single blob | Treated atomically, single-cell storage |
| `list<frozen<udt>>` | Multi-cell: each element is separate cell with frozen UDT blob | Outer list not frozen = multi-cell |
| `frozen<list<udt>>` | Single-cell: entire list serialized as one blob | Outer frozen = single-cell |
| `vector<float,n>` | VInt length (n) + `n` 32-bit floats (big-endian) | Vector CQL type introduced in 5.x |

### UDT Binary Format Details

**Frozen UDT field encoding** (confirmed via Issue #220):
```
[field_1_length: 4-byte BE i32][field_1_data: variable bytes]
[field_2_length: 4-byte BE i32][field_2_data: variable bytes]
...
```

**Field length semantics**:
- `-1` (0xFFFFFFFF): Field is NULL
- `0` (0x00000000): Field is empty (zero-length but present)
- `>0`: Number of bytes of field data following

**Trailing fields**: If fewer than expected fields are present, trailing fields are implicitly NULL.

**UDT type string format** (in Statistics.db):
```
org.apache.cassandra.db.marshal.UserType(keyspace,hex_name,field1:type1,field2:type2,...)
```
- UDT name and field names are hex-encoded (e.g., `616464726573735f74797065` = "address_type")
- Field types use full Cassandra marshal notation

### Frozen vs Non-Frozen Collections with UDTs

| Column Type | Storage | Cell Structure |
|-------------|---------|----------------|
| `frozen<list<udt>>` | Single-cell | VInt count + inline UDT fields |
| `list<frozen<udt>>` | Multi-cell | Each element = separate cell with frozen UDT value |
| `map<K, frozen<udt>>` | Multi-cell | Cell path = key, cell value = frozen UDT |

The **outer** type determines storage format. Inner frozen types affect element serialization but not cell structure.

References:

- Cassandra 5.0: `org.apache.cassandra.db.marshal.VectorType` (`https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/marshal/VectorType.java`)
- Cassandra 5.0: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java`)


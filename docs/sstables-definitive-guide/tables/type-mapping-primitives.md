## CQL → SSTable Type Mapping — Primitives (Cassandra 5.0)

This table summarizes how common primitive CQL types map to on-disk encodings in `Data.db`. Sizes indicate fixed-length payloads, excluding length prefixes for variable types.

| CQL type | On-disk representation | Fixed size (bytes) | Notes |
|---|---|---:|---|
| `boolean` | 1-byte value (0 or 1) | 1 | |
| `tinyint` | 8-bit signed integer | 1 | |
| `smallint` | 16-bit signed integer (big-endian) | 2 | |
| `int` | 32-bit signed integer (big-endian) | 4 | |
| `bigint` | 64-bit signed integer (big-endian) | 8 | |
| `float` | IEEE-754 single (big-endian) | 4 | |
| `double` | IEEE-754 double (big-endian) | 8 | |
| `decimal` | variable (scale:int32 + unscaled BigInteger bytes) | — | BigInteger two's-complement bytes preceded by scale |
| `text`/`varchar` | VInt length + UTF-8 bytes | — | Length is VInt-encoded |
| `ascii` | VInt length + ASCII bytes | — | No multibyte chars |
| `blob` | VInt length + raw bytes | — | |
| `timestamp` | 64-bit millis since epoch (big-endian) | 8 | |
| `date` | 32-bit unsigned days offset (big-endian) | 4 | Epoch at 1970-01-01 bias per Cassandra |
| `time` | 64-bit nanoseconds since midnight (big-endian) | 8 | |
| `uuid` | 128-bit UUID | 16 | Network byte order |
| `timeuuid` | 128-bit time-based UUID | 16 | Includes timestamp fields |
| `inet` | 4 or 16 bytes | — | IPv4 = 4 bytes, IPv6 = 16 bytes |
| `duration` | variable (months VInt, days VInt, nanos VInt) | — | Triplet of VInts |

Reference: `SerializationHeader` defines type info carried in partition/row serialization.

- Cassandra 5.0: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java`)


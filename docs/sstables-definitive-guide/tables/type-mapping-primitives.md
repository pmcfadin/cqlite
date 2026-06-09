## CQL -> SSTable Type Mapping -- Primitives (Cassandra 5.0)

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
| `decimal` | variable (scale:int32 + unscaled BigInteger bytes) | -- | 4-byte BE signed scale followed by BigInteger two's-complement bytes. Source: `DecimalType.java:275-278`. |
| `varint` | variable (custom byte-ordered encoding) | -- | **Not** standard VInt. Two cases (source: `IntegerType.java:48-53,207-210`): (1) length < 7 bytes: 0x80-based positive-varint header; (2) length >= 7 bytes: sign byte (0xFF=positive, 0x00=negative) + variable-length unsigned magnitude. `FULL_FORM_THRESHOLD=7`. |
| `counter` | 8-byte signed integer (big-endian) | 8 | On-disk value is 8-byte BE long (the counter value). Distributed counter context/clock shards are in the cell context header, not the value payload. Source: `CounterColumnType.java`. |
| `text`/`varchar` | VInt length + UTF-8 bytes | -- | Length is unsigned VInt-encoded |
| `ascii` | VInt length + ASCII bytes | -- | No multibyte chars |
| `blob` | VInt length + raw bytes | -- | |
| `timestamp` | 64-bit millis since epoch (big-endian) | 8 | |
| `date` | 32-bit unsigned days, epoch-biased (big-endian) | 4 | Stored as epoch-days + 2^31 (0x80000000); on-disk value 0x80000000 = 1970-01-01. `SimpleDateSerializer.timeInMillisToDay()` adds `Integer.MIN_VALUE` bias. Source: `SimpleDateType.java:42`. |
| `time` | 64-bit nanoseconds since midnight (big-endian) | 8 | |
| `uuid` | 128-bit UUID | 16 | Network byte order |
| `timeuuid` | 128-bit time-based UUID | 16 | Includes timestamp fields |
| `inet` | 4 or 16 bytes | -- | IPv4 = 4 bytes, IPv6 = 16 bytes |
| `duration` | variable (months VInt, days VInt, nanos VInt) | -- | Triplet of signed (zigzag) VInts. All three components use zigzag-signed VInt encoding even though months and days are non-negative by CQL contract. Source: `DurationType.java:33-34`. |

Reference: `SerializationHeader` defines type info carried in partition/row serialization.

- Cassandra 5.0.8: `org.apache.cassandra.db.SerializationHeader` (`https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java`)

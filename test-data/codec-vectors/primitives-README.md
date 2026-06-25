# Primitive & Special CQL Codec Vectors (issue #1004)

`primitives.json` holds deterministic byte-vector fixtures that prove CQLite's
primitive / special CQL value codecs match the **Apache Cassandra 5.0.2** binary
representation. No SSTable files are required; the vectors are self-contained.

## Provenance

Expected bytes are derived from Cassandra 5.0.2 serializer / marshal contracts
(`org.apache.cassandra.db.marshal.*` and `org.apache.cassandra.serializers.*`),
**not** from CQLite's own output. This keeps the parity tests non-tautological:
the test crate independently recomputes the canonical encodings (big-endian
fixed width; `BigInteger.toByteArray()` for varint; 4-byte scale + varint for
decimal; ZigZag + Cassandra `VIntCoding` for duration; `2^31`-offset day count
for date) and asserts they equal the checked-in hex before exercising CQLite.

The `cassandra_version` field and the `encoding_rules` map inside
`primitives.json` document the exact contract used for each type.

## File shape

Top-level keys group the vectors; each entry is a single fixture:

| Key | Types covered |
|-----|---------------|
| `fixed_width` | boolean, tinyint, smallint, int, bigint, float, double |
| `text_blob_ascii` | ascii, text/varchar, blob |
| `uuid_inet` | uuid, timeuuid, inet (v4 + v6) |
| `temporal` | timestamp, date, time |
| `varint_decimal_duration` | varint, decimal, duration |

Fields per entry:

- `type` — CQL type name.
- `name` — human label for the case.
- `hex` — EXACT Cassandra 5.0.2 cell bytes (space-separated, possibly empty).
- value descriptor, one of:
  - `value` — JSON scalar (bool / integer) or, for varint, a decimal string.
  - `value_hex` — raw bytes for blob / uuid / inet.
  - `value_bits_be` — IEEE-754 big-endian bit pattern for float / double
    (used instead of a JSON float so NaN / -0.0 / inf stay exact).
  - `days` (date), `nanos` (time), `value` ms (timestamp).
  - `scale` + `unscaled` (decimal, `unscaled` is a decimal string).
  - `months` + `days` + `nanos` (duration).

## What the tests assert (`cqlite-core/tests/issue_1004_primitive_codec_vectors.rs`)

1. **Encode parity** — `TypeSerializer::serialize_value` reproduces `hex` exactly.
2. **Decode parity** — the public `parser::types` decoders rebuild the value
   from `hex` (for varint/inet, the cell bytes are framed with their VInt length
   prefix, which is how those decoders consume a value).
3. **Canonical-byte cross-check** — the test recomputes the expected bytes from
   the Cassandra contract and asserts equality with `hex`, so the fixtures can
   never silently drift to whatever CQLite happens to emit.
4. **Invalid-length rejection** — for every fixed-width type
   (boolean=1, tinyint=1, smallint=2, int=4, bigint=8, float=4, double=8,
   uuid/timeuuid=16, date=4, time=8, timestamp=8) truncated input must fail to
   decode (length error), and over-long input must NOT be silently truncated
   into a default value.

## Regenerating

The canonical bytes can be reproduced with the documented contracts; the test
itself contains the reference encoders, so editing a value only requires
updating `hex` to match. Keep `cassandra_version` accurate if the reference
version ever changes.

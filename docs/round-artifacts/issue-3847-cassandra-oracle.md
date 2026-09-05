# Issue #3847 — Cassandra 5.0.8 oracle: what an EMPTY buffer means for a fixed-width scalar

**Authority**: `cassandra-5.0.8` tag, read through
`https://raw.githubusercontent.com/apache/cassandra/cassandra-5.0.8/src/java/org/apache/cassandra/...`
(no local clone on this box; the tag is the pin, never a working tree — CLAUDE.md format-authority rule).
A CQLite `file:line` is never format authority and none is cited below as one.

## The question the acceptance criteria ask

> Decide, against `Int32Serializer` and its siblings, whether the empty buffer should decode to
> `null` on this path.

## The finding that changes the framing

The issue states the `|| isEmpty` shape "appears in the other fixed-width serializers". **Measured,
that is true of `validate()` for 8 of 12 and FALSE for 4** — but `validate()` is the wrong oracle for
this path. `parse_value_from_raw_bytes` is a **read** path, so its analogue is `deserialize()`, and
on the `deserialize()` side the rule is **uniform with no exceptions**.

### `deserialize()` — the read path. UNIFORM: empty ⇒ `null`, 12 of 12.

| CQL type | Serializer | `deserialize` on empty |
|---|---|---|
| `int` | `Int32Serializer:30-32` | `accessor.isEmpty(value) ? null : accessor.toInt(value)` |
| `bigint`/`counter` | `LongSerializer:30-32` | `isEmpty ? null : toLong` |
| `boolean` | `BooleanSerializer:32-34` | `value == null \|\| isEmpty` ⇒ `null` |
| `uuid` | `UUIDSerializer:31-33` | `isEmpty ? null : toUUID` |
| `timeuuid` | `TimeUUID.Serializer:339-341` (`utils/TimeUUID.java`) | `isEmpty ? null : toTimeUUID` |
| `float` | `FloatSerializer:30-32` | `if (isEmpty) return null` |
| `double` | `DoubleSerializer:30-32` | `if (isEmpty) return null` |
| `smallint` | `ShortSerializer:30-32` | `isEmpty ? null : toShort` |
| `tinyint` | `ByteSerializer:30-32` | `value == null \|\| isEmpty ? null : toByte` |
| `timestamp` | `TimestampSerializer:137-139` | `isEmpty ? null : new Date(toLong)` |
| `date` | `SimpleDateSerializer:50-52` | `isEmpty ? null : toInt` |
| `time` | `TimeSerializer:32-34` | `isEmpty ? null : toLong` |
| `duration` | `DurationSerializer:61-63` | `if (isEmpty) return null` |

### `validate()` — the write/CQL-input path. NOT uniform, and NOT this path's oracle.

Accepts `{n, 0}` (`size != n && !isEmpty`): `int`, `bigint`, `uuid`, `timeuuid`
(`isEmpty ⇒ return` then `!= 16`), `float`, `double`, `timestamp`; `boolean` is `size > 1` ⇒ `{0,1}`.

Accepts **only** `{n}` (empty rejected): `smallint` (`!= 2`), `tinyint` (`!= 1`),
`date` (`SimpleDateSerializer`, `!= 4`), `time` (`!= 8`).

`duration` is `< 3` ⇒ `validate` rejects empty while `deserialize` returns `null` for it.

### Corroboration from the serialize direction

`BooleanSerializer:41-43`: `serialize(null)` returns `ByteBufferUtil.EMPTY_BYTE_BUFFER`. Empty **is**
the on-the-wire spelling of `null` for a fixed-width scalar, so a reader that refuses it cannot read
data Cassandra legitimately writes.

## Verdict

**YES — an empty buffer must decode to `null` on `parse_value_from_raw_bytes`, for every fixed-width
scalar, with no per-type exceptions.**

The four stricter `validate()` cases (`smallint`, `tinyint`, `date`, `time`) are **not** a reason to
differ per type on the read path: `validate()` gates what may be *written*, `deserialize()` defines
what must be *read*, and every one of those four still deserializes empty to `null`. Reading is the
permissive side of the contract in Cassandra, and CQLite's reader must match the reader.

This also disposes of the acceptance criteria's third bullet — the `udt.rs` `!= n` family does not
need a "why they differ" statement, because on the read path there is nothing to differ about: all
fixed-width scalars take the same `{n, 0}` accepted set and map `0` to `Value::Null`.

## Scope note (not part of the oracle)

`EmptySerializer` (CQL `empty`/`EmptyType`) is the inverse — it accepts **only** empty and is a
distinct type, not a width case. It is out of scope here.

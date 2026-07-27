# Complete CQL Types Reference

## Primitive Types

### Boolean
**Wire Format:** 1 byte
- `0x00` = false
- `0x01` = true

**Rust:**
```rust
fn deserialize_boolean(data: &[u8]) -> Result<bool> {
    match data[0] {
        0x00 => Ok(false),
        0x01 => Ok(true),
        _ => Err(Error::InvalidBoolean(data[0])),
    }
}
```

### Integer Types

#### TinyInt
**Wire Format:** 1 byte signed (-128 to 127)
```rust
fn deserialize_tinyint(data: &[u8]) -> Result<i8> {
    Ok(data[0] as i8)
}
```

#### SmallInt
**Wire Format:** 2 bytes big-endian signed (-32,768 to 32,767)
```rust
fn deserialize_smallint(data: &[u8]) -> Result<i16> {
    Ok(i16::from_be_bytes([data[0], data[1]]))
}
```

#### Int
**Wire Format:** 4 bytes big-endian signed
```rust
fn deserialize_int(data: &[u8]) -> Result<i32> {
    Ok(i32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}
```

#### BigInt
**Wire Format:** 8 bytes big-endian signed
```rust
fn deserialize_bigint(data: &[u8]) -> Result<i64> {
    let bytes = [data[0], data[1], data[2], data[3],
                 data[4], data[5], data[6], data[7]];
    Ok(i64::from_be_bytes(bytes))
}
```

#### VarInt
**Wire Format:** Variable-length big-endian signed integer
- Uses two's complement representation
- Most significant byte first
- Can represent arbitrarily large integers

```rust
use num_bigint::BigInt;

fn deserialize_varint(data: &[u8]) -> Result<BigInt> {
    Ok(BigInt::from_signed_bytes_be(data))
}
```

### Floating Point Types

#### Float
**Wire Format:** 4 bytes IEEE 754 single-precision
```rust
fn deserialize_float(data: &[u8]) -> Result<f32> {
    let bits = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    Ok(f32::from_bits(bits))
}
```

#### Double
**Wire Format:** 8 bytes IEEE 754 double-precision
```rust
fn deserialize_double(data: &[u8]) -> Result<f64> {
    let bytes = [data[0], data[1], data[2], data[3],
                 data[4], data[5], data[6], data[7]];
    let bits = u64::from_be_bytes(bytes);
    Ok(f64::from_bits(bits))
}
```

#### Decimal
**Wire Format:** [4 bytes scale] + [varint unscaled value]
```rust
fn deserialize_decimal(data: &[u8]) -> Result<(i32, BigInt)> {
    let scale = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let unscaled = BigInt::from_signed_bytes_be(&data[4..]);
    Ok((scale, unscaled))
}
```

### String Types

#### Text / Varchar
**Wire Format:** UTF-8 encoded bytes (no null terminator)
```rust
fn deserialize_text(data: &[u8]) -> Result<String> {
    String::from_utf8(data.to_vec())
        .map_err(|e| Error::InvalidUtf8(e))
}
```

#### ASCII
**Wire Format:** ASCII bytes
```rust
fn deserialize_ascii(data: &[u8]) -> Result<String> {
    if data.iter().all(|&b| b < 128) {
        Ok(String::from_utf8_lossy(data).into_owned())
    } else {
        Err(Error::InvalidAscii)
    }
}
```

### Binary Types

#### Blob
**Wire Format:** Raw bytes (no encoding)
```rust
fn deserialize_blob(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
```

### UUID Types

#### UUID
**Wire Format:** 16 bytes (RFC 4122 format)
```rust
use uuid::Uuid;

fn deserialize_uuid(data: &[u8]) -> Result<Uuid> {
    if data.len() != 16 {
        return Err(Error::InvalidUuidLength(data.len()));
    }
    Ok(Uuid::from_bytes(data.try_into().unwrap()))
}
```

#### TimeUUID
**Wire Format:** 16 bytes (UUID v1 with timestamp)
- Same wire format as UUID
- Semantic difference: version 1, time-based
```rust
fn deserialize_timeuuid(data: &[u8]) -> Result<Uuid> {
    let uuid = deserialize_uuid(data)?;
    if uuid.get_version_num() != 1 {
        return Err(Error::NotTimeUuid);
    }
    Ok(uuid)
}
```

### Date/Time Types

#### Timestamp
**Wire Format:** 8 bytes big-endian (milliseconds since Unix epoch)
```rust
fn deserialize_timestamp(data: &[u8]) -> Result<i64> {
    let bytes = [data[0], data[1], data[2], data[3],
                 data[4], data[5], data[6], data[7]];
    Ok(i64::from_be_bytes(bytes))
}
```

#### Date
**Wire Format:** 4 bytes big-endian **unsigned**, days-since-epoch **shifted by
`Integer.MIN_VALUE`** — the epoch (1970-01-01) sits at the CENTRE of the unsigned range,
i.e. day 0 is encoded as `0x80000000`. The shift exists so the raw 4 bytes sort in date
order (byte-comparable).

**You MUST un-bias the stored value.** Reading it as a plain `u32` days-since-epoch is
wrong by 2^31 days.

```rust
/// Cassandra DATE: 4-byte unsigned BE with an Integer.MIN_VALUE offset.
fn deserialize_date(data: &[u8]) -> Result<i32> {
    let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    // Remove the bias: stored 0x80000000 -> 0 days since 1970-01-01.
    Ok(stored.wrapping_add(i32::MIN as u32) as i32)
}
```

> **Citations**: CQLite decode path —
> `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/cell_value_scalar.rs:329-333`
> (`stored.wrapping_add(i32::MIN as u32) as i32`; the bias is spelled `i32::MIN as u32`,
> which is why a grep for the literal `0x80000000` finds nothing). Format authority —
> Apache Cassandra 5.0.8 `src/java/org/apache/cassandra/serializers/SimpleDateSerializer.java`:
> `:36-37` ("For byte-order comparability, we shift by `Integer.MIN_VALUE` and treat the
> data as an unsigned integer … w/epoch sitting in the center @ 2^31"), `:110`
> (`timeInMillisToDay` → `toDays() - Integer.MIN_VALUE`), `:113-115`
> (`dayToTimeInMillis` → `ofDays(days + Integer.MIN_VALUE)`). The *decoded* value CQLite
> carries in `Value::Date` is the signed days-since-epoch, matching Cassandra's `int`.

#### Time
**Wire Format:** 8 bytes big-endian (nanoseconds since midnight)
```rust
fn deserialize_time(data: &[u8]) -> Result<i64> {
    let bytes = [data[0], data[1], data[2], data[3],
                 data[4], data[5], data[6], data[7]];
    Ok(i64::from_be_bytes(bytes))
}
```

#### Duration
**Wire Format:** 3 **ZigZag-signed** VInts (months, days, nanoseconds).

> ### ⚠️ These are the ONLY signed VInts in `Data.db`
> Every other VInt in `Data.db` — all lengths, counts, and the timestamp/TTL/
> local-deletion-time deltas — is **unsigned**. A `duration`'s three components are the
> sole exception, and they apply **wherever a duration payload occurs**, including nested
> inside a collection, tuple, or UDT (`frozen<list<duration>>`,
> `map<text, frozen<tuple<duration,int>>>`, a UDT field of type `duration`, …).
> **Decoding them as unsigned VInts makes every negative duration wrong**, and a negative
> CQL duration makes every non-zero component negative.

**ZigZag decode**: `(zz >> 1) ^ -(zz & 1)` — i.e. positive `n` encodes as `2n`, negative
`n` as `2|n| - 1`.

```rust
/// ZigZag-decode an unsigned VInt payload into its signed value.
fn zigzag_decode(zz: u64) -> i64 {
    ((zz >> 1) as i64) ^ -((zz & 1) as i64)
}

fn deserialize_duration(data: &[u8]) -> Result<(i32, i32, i64)> {
    // parse_vuint reads the raw UNSIGNED VInt; the ZigZag step recovers the sign.
    let (rest, months_zz) = parse_vuint(data)?;
    let (rest, days_zz) = parse_vuint(rest)?;
    let (_, nanos_zz) = parse_vuint(rest)?;
    Ok((
        zigzag_decode(months_zz) as i32,
        zigzag_decode(days_zz) as i32,
        zigzag_decode(nanos_zz),
    ))
}
```

> **Citations**: `docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md:63-65`
> ("Every VInt in `Data.db` is unsigned **except** the three components of a serialized
> `DurationType` payload"), `:92-97` (the duration/ZigZag rule and its nesting scope),
> `:520-529` (structural VInts are unsigned; ZigZag appears only inside a `DurationType`
> payload). Cassandra: `DurationSerializer.java:34,49-51` (three `writeVInt` calls),
> `VIntCoding.java:449,522` (`writeVInt` → `writeUnsignedVInt(encodeZigZag64(v))`),
> `Duration.java:101-110` (a negative duration negates every component).

### Network Types

#### INet
**Wire Format:** 4 bytes (IPv4) or 16 bytes (IPv6)
```rust
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

fn deserialize_inet(data: &[u8]) -> Result<IpAddr> {
    match data.len() {
        4 => {
            let addr = Ipv4Addr::new(data[0], data[1], data[2], data[3]);
            Ok(IpAddr::V4(addr))
        }
        16 => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(data);
            Ok(IpAddr::V6(Ipv6Addr::from(bytes)))
        }
        _ => Err(Error::InvalidInetLength(data.len())),
    }
}
```

## Counter Type

**Wire Format:** 8 bytes big-endian signed
- Special semantics: accumulates writes
- Cannot be used in primary key
- Cannot mix with non-counter columns

```rust
fn deserialize_counter(data: &[u8]) -> Result<i64> {
    deserialize_bigint(data)  // Same wire format
}
```

## Null Handling

All CQL types can be null, but the *framing* that expresses null depends on whether you
are inside a **frozen** value or looking at a **non-frozen** collection's cells:
- **In a FROZEN collection / tuple / UDT:** a 4-byte big-endian `i32` length of `-1`
  (`0xFFFFFFFF`) (`row_decoder/frozen.rs:98,279,370`; `TupleType.java:341-364`).
- **In a NON-FROZEN collection cell:** there is no `-1` sentinel — an absent value is the
  cell flag `HAS_EMPTY_VALUE` (`0x04`), and lengths are **unsigned VInts** which cannot be
  negative (`row_decoder/complex_column.rs:1136`;
  `appendix-b-encodings-cheat-sheet.md:530-536`).
- **In cells generally:** a cell with `IS_DELETED` (`0x01`), or a column omitted from the
  row's column bitmap.

See `collections-and-udts.md` for the two collection encodings side by side.

```rust
fn deserialize_nullable<T>(
    data: &[u8],
    deserializer: impl Fn(&[u8]) -> Result<T>,
) -> Result<Option<T>> {
    if data.is_empty() {
        return Ok(None);
    }
    
    if data.len() >= 4 {
        let size = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if size == -1 {
            return Ok(None);
        }
    }
    
    deserializer(data).map(Some)
}
```

## Empty vs Null

**Important distinction:**
- **Null:** Field doesn't have a value (SQL NULL)
- **Empty:** Field has zero-length value (empty string, empty blob)

**Wire format** — inside a **frozen** value (4-byte BE `i32` lengths):
- Null: size = `-1`, no bytes follow
- Empty: size = `0`, no bytes follow
- Present: size = `N`, `N` bytes follow

**Wire format** — a **non-frozen** collection cell (unsigned-VInt lengths, so no `-1`):
- Empty/absent value: cell flag `HAS_EMPTY_VALUE` (`0x04`) set; no length, no bytes
- Deleted: cell flag `IS_DELETED` (`0x01`) set; no value bytes
- Present: unsigned VInt `value_len` = `N`, then `N` bytes

## Type Aliases

CQL has exactly one such alias pair:
- `text` = `varchar` (both are `UTF8Type`)

> **Citation**: cassandra-5.0.8 `src/java/org/apache/cassandra/cql3/CQL3Type.java:88-111`
> — the `Native` enum is the complete list of CQL native type names. `TEXT` (`:103`) and
> `VARCHAR` (`:111`) both map to `UTF8Type.instance`, which is the one true alias pair;
> `BLOB` (`:91`) maps to `BytesType.instance` and appears exactly once. CQL has **no**
> PostgreSQL-style alias for `blob` — do not invent one.

## Special Cases

### Empty Values
- Empty `text`: zero bytes, valid UTF-8
- Empty `blob`: zero bytes
- Empty collection: count = 0, no elements

### Edge Cases
- `varint` with value 0: single byte `0x00`
- `decimal` with value 0.0: `[0,0,0,0,0]` (scale=0, unscaled=0)
- `uuid` all zeros: nil UUID `00000000-0000-0000-0000-000000000000`

## Reference Implementation

- `cqlite-core/src/types/` — the CQL type model / `Value` enum.
- `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/cell_value_scalar.rs` —
  scalar on-disk decode (the authority for what CQLite does with `date`, `duration`, etc.).
- `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/frozen.rs` — frozen
  collections/tuples (4-byte BE `i32` framing).
- `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs` —
  non-frozen collections (unsigned-VInt framing).
- `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/udt.rs` — UDTs.

**Format authority** for a genuinely disputed on-disk question is Apache Cassandra 5.0.8
(`git show cassandra-5.0.8:src/java/org/apache/cassandra/...`) plus
`docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md`. A CQLite
`file:line` is authoritative for *what CQLite currently does*, not for what the format is.


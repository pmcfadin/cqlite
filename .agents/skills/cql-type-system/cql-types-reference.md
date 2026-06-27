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
**Wire Format:** 4 bytes unsigned (days since epoch: 1970-01-01)
```rust
fn deserialize_date(data: &[u8]) -> Result<u32> {
    Ok(u32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}
```

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
**Wire Format:** 3 VInts (months, days, nanoseconds)
```rust
fn deserialize_duration(data: &[u8]) -> Result<(i32, i32, i64)> {
    let (months_data, rest) = parse_vint(data)?;
    let (days_data, rest) = parse_vint(rest)?;
    let (nanos_data, _) = parse_vint(rest)?;
    Ok((months_data as i32, days_data as i32, nanos_data))
}
```

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

All CQL types can be null:
- **In collections:** 4-byte size prefix of `-1` (0xFFFFFFFF)
- **In UDT fields:** 4-byte size prefix of `-1`
- **In cells:** Cell with IS_DELETED flag or no value bytes

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

**Wire format:**
- Null: size = -1, no bytes follow
- Empty: size = 0, no bytes follow
- Present: size = N, N bytes follow

## Type Aliases

Some types have multiple names:
- `text` = `varchar`
- `blob` = `bytea` (PostgreSQL compatibility)

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

See `cqlite-core/src/types/` for complete Rust type system implementation.


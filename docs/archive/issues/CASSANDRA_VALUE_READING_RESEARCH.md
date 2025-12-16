# Cassandra 5.0 Value Reading Research

**Research Objective:** Document exact value reading logic to fix VInt length parsing in V5CompressedLegacy parser

**Research Date:** 2025-10-16

**Sources:**
- https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java
- https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/vint/VIntCoding.java
- Various type implementations (UTF8Type, Int32Type, BooleanType, etc.)

---

## Executive Summary

Cassandra 5.0 uses **unsigned VInt32** (`readUnsignedVInt32()`) for all variable-width value length prefixes. Fixed-width types bypass this and read exactly N bytes based on `valueLengthIfFixed()`. The Rust implementation must distinguish between:

1. **VInt-encoded lengths** for variable-width types (text, blob, decimal, etc.)
2. **Fixed-width direct reads** for primitive types (int, bigint, boolean, UUID, etc.)

**Critical Finding:** The current Rust parser uses `parse_vint()` which returns **signed i64**. Cassandra uses `readUnsignedVInt32()` which returns **unsigned int** capped at 32 bits. Negative values are rejected with `IOException`.

---

## 1. Variable-Width Value Reading Algorithm

### Java Implementation (AbstractType.read())

```java
// From AbstractType.java (lines 531-590)
public V read(DataInputPlus in, int length) throws IOException {
    // If fixed-width type, read exact number of bytes
    if (length >= 0)
        return accessor.read(in, length);

    // Variable-width: Read unsigned VInt length prefix
    int l = in.readUnsignedVInt32();
    if (l < 0)
        throw new IOException("Corrupt (negative) value length encountered");

    // Validate against maximum value size
    if (l > maxValueSize)
        throw new IOException(String.format(
            "Corrupt value length %d encountered, as it exceeds the maximum of %d",
            l, maxValueSize));

    // Read exactly l bytes
    return accessor.read(in, l);
}
```

### Algorithm Steps for Variable-Width Types

For types like `text`, `ascii`, `blob`, `inet`, `decimal`:

```
Step 1: Read unsigned VInt32 length prefix using readUnsignedVInt32()
Step 2: Validate length is non-negative (reject if negative)
Step 3: Validate length does not exceed maxValueSize
Step 4: Read exactly 'length' bytes from input stream
Step 5: Return wrapped bytes (no interpretation at AbstractType level)
```

**Key Point:** The length prefix is **always unsigned VInt32**, never signed VInt. Negative lengths are corruption errors.

---

## 2. VInt Length Specification

### Method Used: `readUnsignedVInt32()`

**Java Source (VIntCoding.java):**

```java
public static int readUnsignedVInt32(DataInput input) throws IOException {
    return checkedCast(readUnsignedVInt(input));
}
```

This delegates to `readUnsignedVInt()` and validates the result fits in 32 bits.

### Core VInt Reading: `readUnsignedVInt()`

```java
public static long readUnsignedVInt(DataInput input) throws IOException {
    int firstByte = input.readByte();
    if (firstByte >= 0) return firstByte;

    int size = numberOfExtraBytesToRead(firstByte);
    long retval = firstByte & firstByteValueMask(size);
    for (int ii = 0; ii < size; ii++) {
        byte b = input.readByte();
        retval <<= 8;
        retval |= b & 0xff;
    }
    return retval;
}
```

### VInt Encoding Format

**Unsigned VInt Format (MSB-first):**

```
First Byte Pattern: [leading 1-bits][0][value bits]

Examples:
0xxxxxxx    -> 1 byte  (7 bits)  [0-127]
10xxxxxx    -> 2 bytes (6+8 bits) [128-16,383]
110xxxxx    -> 3 bytes (5+16 bits) [16,384-2,097,151]
...
```

**Number of Extra Bytes:**
The number of leading 1-bits in the first byte indicates how many additional bytes follow.

```
firstByte >= 0       -> 0 extra bytes (single byte value)
firstByte < 0        -> count leading 1s for extra bytes
```

### Signed vs Unsigned VInt

**Unsigned VInt (`readUnsignedVInt()`):**
- Returns raw decoded value directly
- Used for: length prefixes, counts, sizes
- Range: 0 to (2^N - 1) where N is data bits

**Signed VInt (`readVInt()`):**
- Applies ZigZag decoding to unsigned value
- Used for: timestamps, TTLs, deletion times
- ZigZag formula: `(n >>> 1) ^ -(n & 1)`

```java
public static long readVInt(DataInput input) throws IOException {
    return decodeZigZag64(readUnsignedVInt(input));
}
```

**For Length Prefixes:** Cassandra **always** uses unsigned VInt. Never signed VInt or ZigZag encoding.

### Maximum Value Allowed

`readUnsignedVInt32()` enforces:
- Maximum: 2^31 - 1 (Integer.MAX_VALUE)
- Throws `VIntOutOfRangeException` if value exceeds 32 bits

### Negative Length Handling

```java
if (l < 0)
    throw new IOException("Corrupt (negative) value length encountered");
```

Negative lengths are **never valid**. This check exists because Java's `int` is signed, but semantically lengths must be non-negative.

---

## 3. Fixed-Width Types Table

| Type       | Width   | Encoding                      | Method                    |
|------------|---------|-------------------------------|---------------------------|
| Boolean    | 1 byte  | 0x00=false, non-zero=true     | getByte()                 |
| Int        | 4 bytes | Big-endian signed 32-bit      | 4 consecutive bytes       |
| BigInt     | 8 bytes | Big-endian signed 64-bit      | 8 consecutive bytes       |
| Float      | 4 bytes | IEEE 754 single precision     | 4 consecutive bytes       |
| Double     | 8 bytes | IEEE 754 double precision     | 8 consecutive bytes       |
| UUID       | 16 bytes| 128-bit UUID (MSB/LSB order) | 16 consecutive bytes      |
| TimeUUID   | 16 bytes| 128-bit time-based UUID       | 16 consecutive bytes      |

**Fixed-width types bypass VInt length prefixes entirely.**

### Type Identification

Each type implements:

```java
public int valueLengthIfFixed() {
    return -1; // Variable-width (use VInt length)
    // OR
    return N;  // Fixed N bytes
}
```

**Examples:**

```java
// BooleanType.java
@Override
public int valueLengthIfFixed() {
    return 1;
}

// Int32Type.java
@Override
public int valueLengthIfFixed() {
    return 4;
}

// LongType.java
@Override
public int valueLengthIfFixed() {
    return 8;
}

// UTF8Type.java (via StringType)
@Override
public int valueLengthIfFixed() {
    return -1; // Variable-width
}
```

---

## 4. Java Code Quotes

### AbstractType.read() - Core Value Reading Logic

```java
/**
 * Reads a value from the provided input.
 *
 * @param in the input to read from
 * @param accessor the accessor for the backing type
 * @param length the length of the value to read, or -1 for variable-length types
 * @return the value read
 */
public V read(DataInputPlus in, int length) throws IOException {
    if (length >= 0)
        return accessor.read(in, length);

    int l = in.readUnsignedVInt32();
    if (l < 0)
        throw new IOException("Corrupt (negative) value length encountered");

    if (l > maxValueSize)
        throw new IOException(String.format(
            "Corrupt value length %d encountered, as it exceeds the maximum of %d",
            l, maxValueSize));

    return accessor.read(in, l);
}
```

### VIntCoding.java - Unsigned VInt Reading

```java
/**
 * Reads an unsigned VInt from the input and returns it as a long.
 */
public static long readUnsignedVInt(DataInput input) throws IOException {
    int firstByte = input.readByte();
    if (firstByte >= 0) return firstByte;

    int size = numberOfExtraBytesToRead(firstByte);
    long retval = firstByte & firstByteValueMask(size);
    for (int ii = 0; ii < size; ii++) {
        byte b = input.readByte();
        retval <<= 8;
        retval |= b & 0xff;
    }
    return retval;
}

/**
 * Reads an unsigned VInt32 from the input.
 * Throws VIntOutOfRangeException if value exceeds 32 bits.
 */
public static int readUnsignedVInt32(DataInput input) throws IOException {
    return checkedCast(readUnsignedVInt(input));
}
```

### VInt vs ZigZag

```java
/**
 * Reads a signed VInt using ZigZag decoding.
 */
public static long readVInt(DataInput input) throws IOException {
    return decodeZigZag64(readUnsignedVInt(input));
}

/**
 * ZigZag decode: maps unsigned to signed efficiently.
 * Formula: (n >>> 1) ^ -(n & 1)
 */
private static long decodeZigZag64(long n) {
    return (n >>> 1) ^ -(n & 1);
}
```

---

## 5. Test Case Validation

### Given Test Case: `05 61 73 63 69 69`

**Hex Breakdown:**
```
0x05 = 00000101 (binary)
```

**VInt Decoding:**
- First byte: `0x05` (decimal 5)
- MSB = 0, so single-byte unsigned VInt
- Value: 5 (no extra bytes)

**UTF-8 Bytes:**
```
0x61 0x73 0x63 0x69 0x69
 'a'  's'  'c'  'i'  'i'
```

**Interpretation:**
- Length prefix: unsigned VInt = 5
- Value bytes: 5 bytes = "ascii"

**Why Current Rust Parser Might Read This Wrong:**

1. **Using signed VInt:** If Rust parser uses `parse_vint()` returning signed i64, it correctly decodes 5, but the type confusion may cause issues elsewhere.

2. **ZigZag decoding applied incorrectly:** If parser applies ZigZag to length prefixes, small values still work (zigzag_decode(5) = 2), which would be wrong but might not fail immediately.

3. **Offset calculation errors:** The Rust parser calculates offset using:
   ```rust
   offset = data.len() - remaining.len();
   ```
   This should work correctly if `parse_vint()` returns the right remaining slice.

**Correct Rust Implementation:**

```rust
// Read unsigned VInt32 length (NOT signed VInt)
let (remaining, len_unsigned) = parse_unsigned_vint32(&data[offset..])?;

// Validate non-negative (should always be true for unsigned)
if len_unsigned > i32::MAX as u64 {
    return Err(Error::corruption("Value length exceeds 2GB"));
}

let len = len_unsigned as usize;
offset = data.len() - remaining.len();

// Read exactly 'len' bytes
let value_bytes = &data[offset..offset + len];
```

---

## 6. Comparison with Current Rust Implementation

### Current V5CompressedLegacy Parser (v5_compressed_legacy.rs)

```rust
// Line 469-475 (Text/Varchar/Ascii parsing)
"text" | "varchar" | "ascii" => {
    // Text: VInt length + UTF-8 bytes
    let (remaining, len_signed) = crate::parser::vint::parse_vint(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse text length"))?;
    if len_signed < 0 {
        return Err(Error::corruption(format!("Negative text length: {}", len_signed)));
    }
    let len = len_signed as usize;
    offset = data.len() - remaining.len();

    if offset + len > data.len() {
        return Err(Error::corruption("Text value truncated"));
    }
    let text = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
    Ok((Value::Text(text), offset + len))
}
```

**Issues:**

1. **Wrong VInt method:** Uses `parse_vint()` which implements signed/ZigZag VInt, not unsigned VInt32
2. **Sign check after parsing:** The check `if len_signed < 0` is correct validation, but the underlying parser shouldn't use signed encoding
3. **Type confusion:** Cassandra's `readUnsignedVInt32()` is fundamentally different from `readVInt()`

### What Rust Parser Needs

A new function: `parse_unsigned_vint32()` that:

1. Implements `readUnsignedVInt()` logic (no ZigZag)
2. Returns unsigned value (u32 or u64)
3. Validates value fits in 32 bits
4. Never applies ZigZag decoding

**Proposed Implementation:**

```rust
/// Parse unsigned VInt32 exactly as Cassandra's readUnsignedVInt32()
pub fn parse_unsigned_vint32(input: &[u8]) -> IResult<&[u8], u32> {
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let first_byte = input[0] as i8; // Check sign bit

    // Single byte case: first byte >= 0 (MSB = 0)
    if first_byte >= 0 {
        return Ok((&input[1..], first_byte as u32));
    }

    // Multi-byte case: count leading 1s
    let leading_ones = (input[0] as u8).leading_ones() as usize;
    let total_bytes = leading_ones + 1;

    if total_bytes > 5 {
        // Max 5 bytes for 32-bit value
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if input.len() < total_bytes {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    // Extract value bits from first byte
    let first_data_bits = 8 - leading_ones - 1;
    let first_mask = (1u8 << first_data_bits) - 1;
    let mut value = (input[0] & first_mask) as u64;

    // Read remaining bytes
    for i in 1..total_bytes {
        value = (value << 8) | (input[i] as u64);
    }

    // Validate fits in u32
    if value > u32::MAX as u64 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    Ok((&input[total_bytes..], value as u32))
}
```

---

## 7. Recommendations for Rust Implementation

### Critical Fixes Required

1. **Create `parse_unsigned_vint32()` function** matching Cassandra's `readUnsignedVInt32()`
2. **Update V5CompressedLegacy parser** to use unsigned VInt for all length prefixes:
   - Text/Varchar/Ascii values
   - Blob values
   - Decimal values
   - Collection element counts
   - Any other variable-width value

3. **Keep signed VInt for metadata:**
   - Timestamps (VInt signed)
   - TTLs (VInt32 signed)
   - Deletion times (VInt32 signed)

### Implementation Checklist

- [ ] Add `parse_unsigned_vint32()` to `vint.rs`
- [ ] Add unit tests matching Java VIntCoding behavior
- [ ] Update `parse_value_bytes()` in V5CompressedLegacy parser
- [ ] Update blob parsing (line 522)
- [ ] Update decimal parsing (line 504)
- [ ] Verify test case `05 61 73 63 69 69` decodes correctly
- [ ] Add integration test with known Cassandra SSTable data

### Test Cases to Validate

```rust
#[test]
fn test_unsigned_vint32_single_byte() {
    // 0x05 -> 5
    assert_eq!(parse_unsigned_vint32(&[0x05]).unwrap(), (&[][..], 5));

    // 0x7F -> 127 (max single byte)
    assert_eq!(parse_unsigned_vint32(&[0x7F]).unwrap(), (&[][..], 127));
}

#[test]
fn test_unsigned_vint32_two_bytes() {
    // 0x80 0x80 -> 128
    let result = parse_unsigned_vint32(&[0x80, 0x80]).unwrap();
    assert_eq!(result.1, 128);
}

#[test]
fn test_text_value_ascii() {
    // Hex: 05 61 73 63 69 69 -> length=5, value="ascii"
    let data = vec![0x05, 0x61, 0x73, 0x63, 0x69, 0x69];
    let (remaining, len) = parse_unsigned_vint32(&data).unwrap();
    assert_eq!(len, 5);
    assert_eq!(remaining.len(), 5);
    let text = String::from_utf8(remaining.to_vec()).unwrap();
    assert_eq!(text, "ascii");
}
```

---

## 8. Summary

**Key Findings:**

1. **Unsigned VInt32 for Lengths:** Cassandra uses `readUnsignedVInt32()` for all variable-width value length prefixes
2. **No ZigZag for Lengths:** Length prefixes are pure unsigned VInt encoding, not ZigZag
3. **Fixed-Width Bypass:** Types like int, bigint, boolean read exact N bytes without length prefix
4. **Signed VInt for Metadata:** Timestamps, TTLs use signed VInt with ZigZag encoding
5. **32-bit Limit:** Length values capped at 2GB (Integer.MAX_VALUE)

**Critical Implementation Gap:**

The Rust parser currently uses `parse_vint()` (signed/ZigZag) for length prefixes where Cassandra uses `readUnsignedVInt32()` (unsigned, no ZigZag). This type mismatch may cause decoding errors for large values or edge cases.

**Action Required:**

Implement `parse_unsigned_vint32()` matching Cassandra's exact semantics and update all variable-width value parsers to use it for length prefixes.

---

## References

1. **Cassandra Source Files:**
   - `AbstractType.java` - Value reading interface
   - `VIntCoding.java` - VInt encoding/decoding implementations
   - Type implementations (UTF8Type, Int32Type, BooleanType, etc.)

2. **Cassandra VInt Documentation:**
   - VInt format: MSB-first with leading 1-bits indicating extra bytes
   - Unsigned vs Signed: Different use cases and encodings

3. **Relevant Issues:**
   - Issue #160: V5CompressedLegacy parser improvements
   - Issue #28: No-heuristics mandate (schema-aware decoding required)

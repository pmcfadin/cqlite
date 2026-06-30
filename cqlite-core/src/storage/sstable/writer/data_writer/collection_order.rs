//! Type-aware SET-element / MAP-key ordering for the SSTable writer (issue #1275).
//!
//! Cassandra `SetType`/`MapType` are SORTED collections: a frozen set serializes
//! its elements, and a frozen/non-frozen map its entries, in the order defined by
//! the element/key type's own `AbstractType.compare` — NOT raw unsigned
//! serialized-byte order. This module is the SINGLE shared comparator both the
//! non-frozen complex-cell path (`complex.rs`) and the frozen `serialize_value`
//! path (`encoding.rs`) call, so the two cannot drift.
//!
//! Part of the `data_writer` responsibility split (issue #1118). `use super::*`
//! provides the crate imports and sibling helpers re-exported from
//! `data_writer/mod.rs` (notably `serialize_value`). No emitted bytes change for
//! types whose comparator is unsigned-lexicographic.

use super::*;
use num_bigint::BigInt;
use std::cmp::Ordering;

/// Total-order comparator for two SET elements / MAP keys, matching the
/// element/key type's Cassandra `AbstractType.compare` (issue #1275).
///
/// For most types (text/ascii/blob/boolean/inet/date) that comparator IS
/// unsigned-lexicographic over the serialized bytes, so raw byte order is
/// correct (see the per-type audit on the fallback arm below). But several
/// types order differently from their raw serialized bytes:
///
///   * `uuid`/`timeuuid` (`UUIDType`) — NOT raw-byte-ordered: Cassandra compares
///     the version nibble first, then (for v1) the reordered timestamp, then the
///     remaining bytes. Implemented by [`compare_uuid_cassandra`].
///
/// And the SIGNED numeric types order differently from their big-endian
/// two's-complement bytes:
///
///   * `Int32Type`(int) / `LongType`(bigint) / `ShortType`(smallint) /
///     `ByteType`(tinyint) / counter — SIGNED integer order. `-1` (`0xFFFF_FFFF`)
///     sorts LAST under unsigned bytes but FIRST under the type comparator.
///   * `TimestampType` / `TimeType` — extend/share `LongType`'s signed long
///     compare.
///   * `FloatType` / `DoubleType` — `Float.compare` / `Double.compare`: a total
///     order where `-0.0 < +0.0` and EVERY `NaN` sorts last (greater than any
///     non-NaN); the sign bit makes raw big-endian byte order wrong for
///     negatives. Note Rust's `f32/f64::total_cmp` is NOT this order — it sorts
///     negative NaNs BEFORE numeric values — so we implement Java's comparator
///     directly (`compare_f32_java`/`compare_f64_java`).
///   * `IntegerType` (varint) — signed two's-complement big-integer compare over
///     variable-length bodies.
///   * `DecimalType` — numeric `BigDecimal` order (scale-aware), not byte order.
///
/// The ordering oracle is Cassandra's per-type `AbstractType.compare`
/// (`org.apache.cassandra.db.marshal`). The decision is driven ENTIRELY by the
/// element/key `Value` variant (authoritative CQL type metadata carried by the
/// value itself) — never by inspecting serialized byte patterns (no-heuristics,
/// issue #28). Any pair whose variants are not a known signed/numeric type (or
/// are mixed/unsupported) falls back to comparing their serialized bytes, the
/// historical unsigned-lexicographic behavior that is correct for those types.
pub(crate) fn compare_collection_elements(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        // Signed fixed-width integers — native signed order.
        (Value::TinyInt(x), Value::TinyInt(y)) => x.cmp(y),
        (Value::SmallInt(x), Value::SmallInt(y)) => x.cmp(y),
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),
        (Value::Counter(x), Value::Counter(y)) => x.cmp(y),
        // Temporal longs (TimestampType/TimeType extend/share LongType).
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        // Floating point — Float.compare / Double.compare total order.
        (Value::Float32(x), Value::Float32(y)) => compare_f32_java(*x, *y),
        (Value::Float(x), Value::Float(y)) => compare_f64_java(*x, *y),
        // Varint — signed big-integer compare of two's-complement bodies.
        (Value::Varint(x), Value::Varint(y)) => compare_signed_varint(x, y),
        // Decimal — scale-aware numeric compare.
        (
            Value::Decimal {
                scale: sa,
                unscaled: ua,
            },
            Value::Decimal {
                scale: sb,
                unscaled: ub,
            },
        ) => compare_decimal(*sa, ua, *sb, ub),
        // UUID/TimeUUID — version-first + v1-timestamp + tail compare (UUIDType).
        (Value::Uuid(x), Value::Uuid(y)) => compare_uuid_cassandra(x, y),
        // Frozen wrappers: compare the inner values by the same rule.
        (Value::Frozen(x), other) => compare_collection_elements(x, other),
        (x, Value::Frozen(y)) => compare_collection_elements(x, y),
        // Everything else falls back to unsigned-lexicographic order over the
        // serialized bytes. Per-type audit vs Cassandra `AbstractType.compare`
        // (issue #1275 convergence audit) — each of these IS unsigned-byte-
        // lexicographic, so raw serialized bytes are the correct order:
        //   * `text`/`varchar` (UTF8Type) and `ascii` (AsciiType): both use
        //     `ComparisonType.BYTE_ORDER` — UTF-8 / 7-bit ASCII bytes sort
        //     unsigned-lexicographically, which equals codepoint order.
        //   * `blob` (BytesType): BYTE_ORDER, raw bytes.
        //   * `boolean` (BooleanType): BYTE_ORDER over the single serialized byte
        //     (0x00 false < 0x01 true), which is the natural order.
        //   * `inet` (InetAddressType): BYTE_ORDER — IPv4 (4 bytes) sorts before
        //     any IPv6 (16 bytes) by length-then-byte, matching Cassandra.
        //   * `date` (SimpleDateType): stored as an UNSIGNED 32-bit day count
        //     offset by 2^31, compared `ByteBufferUtil.compareUnsigned`, i.e.
        //     unsigned big-endian bytes — exactly raw-byte order.
        //   * `duration` (DurationType): Cassandra makes duration NON-comparable
        //     (`isEmptyValueMeaningless`/no total order) and FORBIDS it as a set
        //     element or map key, so a `Value::Duration` cannot legitimately reach
        //     this comparator; the fallback keeps the sort total/panic-free if one
        //     somehow does, but it is not an ordering Cassandra would ever emit.
        //   * `tuple`/`UDT`/nested frozen collections: composite recursive order
        //     is intentionally OUT OF SCOPE here (tracked by issue #1296); they
        //     stay on this byte fallback.
        // A mixed/unsupported variant pair also lands here (byte fallback keeps
        // the sort total). `numeric/float/decimal/uuid` are handled above.
        _ => match (serialize_value(a), serialize_value(b)) {
            (Ok(ba), Ok(bb)) => ba.cmp(&bb),
            // Serialization should not fail for a non-null collection element; if
            // it does, treat as equal so the sort stays total and panic-free.
            _ => Ordering::Equal,
        },
    }
}

/// Signed two's-complement big-integer comparison of two `varint` bodies, matching
/// Cassandra `IntegerType.compare` (issue #1275). Empty bodies are treated as the
/// most-significant byte being 0 (non-negative). The bodies may differ in length;
/// sign is taken from the high bit of the most-significant byte.
fn compare_signed_varint(a: &[u8], b: &[u8]) -> Ordering {
    let neg_a = a.first().is_some_and(|&b0| b0 & 0x80 != 0);
    let neg_b = b.first().is_some_and(|&b0| b0 & 0x80 != 0);
    match (neg_a, neg_b) {
        (false, true) => return Ordering::Greater,
        (true, false) => return Ordering::Less,
        _ => {}
    }
    // Same sign: left-pad the shorter body with its sign-extension byte (0xFF for
    // negative, 0x00 for non-negative), then compare lexicographically — for
    // two's-complement of equal length this IS the signed order.
    let fill_a: u8 = if neg_a { 0xFF } else { 0x00 };
    let fill_b: u8 = if neg_b { 0xFF } else { 0x00 };
    let len = a.len().max(b.len());
    for i in 0..len {
        let ea = a
            .len()
            .checked_sub(len - i)
            .and_then(|idx| a.get(idx))
            .copied()
            .unwrap_or(fill_a);
        let eb = b
            .len()
            .checked_sub(len - i)
            .and_then(|idx| b.get(idx))
            .copied()
            .unwrap_or(fill_b);
        match ea.cmp(&eb) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    Ordering::Equal
}

/// Cassandra `UUIDType.compareCustom` for two 16-byte UUID values (issue #1275).
///
/// Matches `org.apache.cassandra.db.marshal.UUIDType.compareCustom`
/// (`~/projects/cassandra/.../db/marshal/UUIDType.java`) exactly:
///   1. Compare the 4-bit version nibble (top nibble of byte 6). If versions
///      differ, order by `version1 - version2`.
///   2. If both are version 1, compare the most-significant 64 bits AFTER
///      `reorderTimestampBytes` (the time-UUID timestamp reshuffle, copied from
///      `AbstractTimeUUIDType.reorderTimestampBytes`); the reordered value is
///      `>= 0` so `Long.compare` of the reordered longs is correct.
///      Otherwise compare the MSB 64 bits as an UNSIGNED long
///      (`UnsignedLongs.compare`).
///   3. Tie-break on the least-significant 64 bits as an UNSIGNED long.
///
/// WHY `UUIDType` and not `TimeUUIDType`: the writer's `Value::Uuid([u8; 16])`
/// carries NO uuid-vs-timeuuid discriminator, and this shared comparator only
/// receives `&Value` — the declared CQL element/key type (`uuid` vs `timeuuid`)
/// is NOT threaded to this layer (no-heuristics forbids inferring it from the
/// version nibble: a v1 UUID is legal in a `set<uuid>`). `UUIDType.compareCustom`
/// is the universal comparator that correctly orders UUIDs of EVERY version
/// (Cassandra "freely takes time UUIDs" in `UUIDType`), so it is the correct
/// order for a declared `uuid` element for all versions. For a declared
/// `timeuuid`, `AbstractTimeUUIDType.compareCustom` agrees on steps 1-2 and
/// differs ONLY in the v1 LSB tie-break, where `TimeUUIDType` compares the low
/// 64 bits with SIGNED-per-byte semantics rather than unsigned (the documented
/// CASSANDRA-8730 discrepancy). That tie-break only changes the relative order of
/// two UUIDs sharing an identical version-1 timestamp — vanishingly rare and not
/// representable without the missing type tag — so we apply the `uuid` order
/// uniformly and document the one-case divergence here rather than guess.
fn compare_uuid_cassandra(a: &[u8; 16], b: &[u8; 16]) -> Ordering {
    let msb1 = read_be_u64(&a[0..8]);
    let msb2 = read_be_u64(&b[0..8]);

    let version1 = ((msb1 >> 12) & 0xf) as i32;
    let version2 = ((msb2 >> 12) & 0xf) as i32;
    if version1 != version2 {
        return version1.cmp(&version2);
    }

    let msb_cmp = if version1 == 1 {
        // Reordered timestamp longs are non-negative; signed Long.compare ==
        // their natural numeric order.
        reorder_timestamp_bytes(msb1).cmp(&reorder_timestamp_bytes(msb2))
    } else {
        // UnsignedLongs.compare(msb1, msb2)
        msb1.cmp(&msb2)
    };
    if msb_cmp != Ordering::Equal {
        return msb_cmp;
    }

    // UnsignedLongs.compare of the least-significant 64 bits.
    read_be_u64(&a[8..16]).cmp(&read_be_u64(&b[8..16]))
}

/// Big-endian read of an 8-byte slice as a `u64` (mirrors Java `getLong`). The
/// slice is always exactly 8 bytes at the call sites; a short slice pads with
/// zeros to stay panic-free, but that never happens for a 16-byte UUID.
fn read_be_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(buf)
}

/// `AbstractTimeUUIDType.reorderTimestampBytes`, copied verbatim from
/// `~/projects/cassandra/.../db/marshal/AbstractTimeUUIDType.java`:
/// reshuffles the version-1 timestamp halves so a plain long compare orders by
/// time. All shifts are on `u64` to match Java's `>>>` (unsigned) and `<<`.
fn reorder_timestamp_bytes(input: u64) -> u64 {
    (input << 48) | ((input << 16) & 0xFFFF_0000_0000) | (input >> 32)
}

/// Java `Float.compare` total order for two `f32`, matching Cassandra
/// `FloatType.compare` (issue #1275). Unlike Rust's `f32::total_cmp`, Java treats
/// EVERY NaN (any payload/sign) as greater than every non-NaN value (NaN sorts
/// last) and does NOT distinguish NaN bit-patterns; it also orders `-0.0 < +0.0`.
fn compare_f32_java(x: f32, y: f32) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        // Neither is NaN: total_cmp gives the numeric order with -0.0 < +0.0.
        (false, false) => x.total_cmp(&y),
    }
}

/// Java `Double.compare` total order for two `f64`, matching Cassandra
/// `DoubleType.compare` (issue #1275). See [`compare_f32_java`] for the NaN /
/// signed-zero rules.
fn compare_f64_java(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => x.total_cmp(&y),
    }
}

/// Scale-aware numeric comparison of two `decimal` values, matching Cassandra
/// `DecimalType.compare` (`BigDecimal.compareTo`, arbitrary precision) (issue
/// #1275). The unscaled part is a two's-complement varint; the value is
/// `unscaled * 10^-scale`.
///
/// IMPORTANT (DoS-safe): we must NEVER materialize `10^(max_scale - own_scale)`,
/// because a valid `i32` scale difference (e.g. `0` vs `2_147_483_647`) would
/// build an astronomically large `BigInt` and hang/OOM the writer. Instead we
/// mirror `BigDecimal.compareTo`'s bounded strategy:
///
///   1. Sign first: negative < zero < positive. Differing signs decide with no
///      big-int multiply at all.
///   2. Same-sign non-zero: compare by ADJUSTED EXPONENT
///      `adj = (num_unscaled_digits - 1) - scale`, the base-10 exponent of the
///      most-significant digit (`value ≈ d.dddd × 10^adj`). A larger adjusted
///      exponent means a larger magnitude; apply the (shared) sign. Decided
///      WITHOUT any `10^p`.
///   3. Only when the adjusted exponents are EQUAL (same decimal magnitude band)
///      do we align and compare exactly. There the rescale multiplier exponent
///      is the scale delta, which — because the adjusted exponents match — is
///      bounded by the difference in digit counts (`|digits_a - digits_b|`), a
///      small number. That `10^delta` is safe to build.
///
/// Uses `num_bigint::BigInt` (already a cqlite-core dependency) so unscaled
/// magnitudes wider than 128 bits compare exactly, with no saturation.
fn compare_decimal(scale_a: i32, unscaled_a: &[u8], scale_b: i32, unscaled_b: &[u8]) -> Ordering {
    let big_a = varint_to_bigint(unscaled_a);
    let big_b = varint_to_bigint(unscaled_b);

    // 1. Sign comparison: negative < zero < positive. Differing signs decide
    //    immediately with no rescale.
    let sign_a = big_a.sign();
    let sign_b = big_b.sign();
    if sign_a != sign_b {
        return sign_a.cmp(&sign_b);
    }
    // Equal signs that are both Zero → both values are exactly 0 (scale is
    // irrelevant for a zero unscaled part), so the values are equal.
    if sign_a == num_bigint::Sign::NoSign {
        return Ordering::Equal;
    }

    // 2. Same-sign non-zero: compare by adjusted exponent (magnitude band).
    //    digits = number of base-10 digits of |unscaled| (>= 1 here).
    let digits_a = decimal_digit_count(&big_a);
    let digits_b = decimal_digit_count(&big_b);
    // adjusted exponent = (digits - 1) - scale, as i64 to avoid i32 overflow
    // for extreme scales near i32::MIN/MAX.
    let adj_a = (digits_a as i64 - 1) - scale_a as i64;
    let adj_b = (digits_b as i64 - 1) - scale_b as i64;
    let positive = sign_a == num_bigint::Sign::Plus;
    if adj_a != adj_b {
        let by_magnitude = adj_a.cmp(&adj_b);
        // For negatives the larger magnitude is the SMALLER value.
        return if positive {
            by_magnitude
        } else {
            by_magnitude.reverse()
        };
    }

    // 3. Adjusted exponents equal: same magnitude band, so an exact compare
    //    requires aligning scales. The scale delta is bounded by the digit-count
    //    difference (`adj_a == adj_b` ⇒ `scale_a - scale_b == digits_a -
    //    digits_b`), which is small — safe to build `10^delta`.
    let delta = scale_a as i64 - scale_b as i64; // == digits_a - digits_b
    let (mut lhs, mut rhs) = (big_a, big_b);
    match delta.cmp(&0) {
        // scale_a > scale_b: a is finer-grained; scale b UP by multiplying b.
        Ordering::Greater => rhs *= pow10(delta as u32),
        // scale_a < scale_b: scale a UP by multiplying a.
        Ordering::Less => lhs *= pow10((-delta) as u32),
        Ordering::Equal => {}
    }
    lhs.cmp(&rhs)
}

/// Number of base-10 digits in the magnitude of `n` (with `n != 0`), matching the
/// "precision" of `BigDecimal`. Derived from the decimal string of the magnitude
/// so it is exact; the BigInt here is bounded by the unscaled body length (an
/// actual serialized value), never by an unbounded power of ten.
fn decimal_digit_count(n: &BigInt) -> u64 {
    // magnitude() yields the BigUint; its base-10 string length is the digit
    // count. Cheap because the unscaled body is a real (short) serialized value.
    n.magnitude().to_str_radix(10).len() as u64
}

/// Decode a two's-complement big-endian `varint` body into an arbitrary-precision
/// signed big integer (matches the `from_signed_bytes_be` decode used elsewhere in
/// the crate). An empty body is `0`.
fn varint_to_bigint(bytes: &[u8]) -> BigInt {
    if bytes.is_empty() {
        return BigInt::ZERO;
    }
    BigInt::from_signed_bytes_be(bytes)
}

/// Arbitrary-precision `10^exp` for decimal scale alignment.
fn pow10(exp: u32) -> BigInt {
    BigInt::from(10).pow(exp)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Int32Type/SetType: signed order — -1 before 0 before 1, even though
    /// `0xFFFF_FFFF` (-1) sorts LAST under raw unsigned bytes.
    #[test]
    fn int_elements_sort_signed() {
        let mut v = vec![Value::Integer(1), Value::Integer(-1), Value::Integer(0)];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![Value::Integer(-1), Value::Integer(0), Value::Integer(1)]
        );
    }

    /// LongType (bigint), ShortType (smallint), ByteType (tinyint): all signed.
    #[test]
    fn other_signed_integers_sort_signed() {
        let mut big = vec![Value::BigInt(5), Value::BigInt(-9), Value::BigInt(0)];
        big.sort_by(compare_collection_elements);
        assert_eq!(
            big,
            vec![Value::BigInt(-9), Value::BigInt(0), Value::BigInt(5)]
        );

        let mut small = vec![Value::SmallInt(2), Value::SmallInt(-2)];
        small.sort_by(compare_collection_elements);
        assert_eq!(small, vec![Value::SmallInt(-2), Value::SmallInt(2)]);

        let mut tiny = vec![Value::TinyInt(7), Value::TinyInt(-7)];
        tiny.sort_by(compare_collection_elements);
        assert_eq!(tiny, vec![Value::TinyInt(-7), Value::TinyInt(7)]);
    }

    /// DoubleType/FloatType: Double.compare total order — -1.5 < -0.0 < +0.0 < 2.0,
    /// NaN sorts last; raw big-endian bytes would put negatives high.
    #[test]
    fn floats_sort_by_total_order() {
        let mut v = vec![
            Value::Float(2.0),
            Value::Float(f64::NAN),
            Value::Float(-1.5),
            Value::Float(0.0),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(v[0], Value::Float(-1.5));
        assert_eq!(v[1], Value::Float(0.0));
        assert_eq!(v[2], Value::Float(2.0));
        assert!(matches!(v[3], Value::Float(f) if f.is_nan()));
    }

    /// IntegerType (varint): signed two's-complement big-integer order across
    /// differing body lengths. `0xFF`(-1) < `0x00`(0) < `0x7F`(127) < `0x0100`(256).
    #[test]
    fn varints_sort_signed_across_lengths() {
        let mut v = vec![
            Value::Varint(vec![0x01, 0x00]), // 256
            Value::Varint(vec![0xFF]),       // -1
            Value::Varint(vec![0x00]),       // 0
            Value::Varint(vec![0x7F]),       // 127
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                Value::Varint(vec![0xFF]),
                Value::Varint(vec![0x00]),
                Value::Varint(vec![0x7F]),
                Value::Varint(vec![0x01, 0x00]),
            ]
        );
    }

    /// DecimalType: scale-aware numeric order. -1.0 (unscaled -10, scale 1) <
    /// 0.5 (unscaled 5, scale 1) < 2 (unscaled 2, scale 0).
    #[test]
    fn decimals_sort_numerically() {
        let mut v = vec![
            Value::Decimal {
                scale: 0,
                unscaled: vec![0x02],
            }, // 2
            Value::Decimal {
                scale: 1,
                unscaled: vec![0xF6],
            }, // -10 * 10^-1 = -1.0
            Value::Decimal {
                scale: 1,
                unscaled: vec![0x05],
            }, // 0.5
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                Value::Decimal {
                    scale: 1,
                    unscaled: vec![0xF6]
                },
                Value::Decimal {
                    scale: 1,
                    unscaled: vec![0x05]
                },
                Value::Decimal {
                    scale: 0,
                    unscaled: vec![0x02]
                },
            ]
        );
    }

    /// DoubleType: Java `Double.compare` rules that Rust's `total_cmp` gets wrong.
    /// A NEGATIVE NaN must sort LAST (greater than every numeric value), and
    /// `-0.0 < +0.0`. Under `total_cmp` a negative NaN would sort FIRST, so this
    /// case fails before the fix and passes after.
    #[test]
    fn double_nan_sorts_last_and_signed_zero() {
        let neg_nan = f64::from_bits(0xFFF8_0000_0000_0000); // a NEGATIVE quiet NaN
        assert!(neg_nan.is_nan() && neg_nan.is_sign_negative());
        let mut v = vec![
            Value::Float(neg_nan),
            Value::Float(2.0),
            Value::Float(0.0),  // +0.0
            Value::Float(-0.0), // -0.0
            Value::Float(-3.0),
        ];
        v.sort_by(compare_collection_elements);
        // -3.0 < -0.0 < +0.0 < 2.0 < NaN(last)
        assert_eq!(v[0], Value::Float(-3.0));
        assert!(matches!(v[1], Value::Float(f) if f == 0.0 && f.is_sign_negative()));
        assert!(matches!(v[2], Value::Float(f) if f == 0.0 && f.is_sign_positive()));
        assert_eq!(v[3], Value::Float(2.0));
        assert!(matches!(v[4], Value::Float(f) if f.is_nan()));
    }

    /// FloatType: same Java NaN-last / signed-zero rules for `f32`.
    #[test]
    fn float32_nan_sorts_last_and_signed_zero() {
        let neg_nan = f32::from_bits(0xFFC0_0000); // a NEGATIVE quiet NaN
        assert!(neg_nan.is_nan() && neg_nan.is_sign_negative());
        let mut v = vec![
            Value::Float32(neg_nan),
            Value::Float32(1.0),
            Value::Float32(0.0),
            Value::Float32(-0.0),
        ];
        v.sort_by(compare_collection_elements);
        assert!(matches!(v[0], Value::Float32(f) if f == 0.0 && f.is_sign_negative()));
        assert!(matches!(v[1], Value::Float32(f) if f == 0.0 && f.is_sign_positive()));
        assert_eq!(v[2], Value::Float32(1.0));
        assert!(matches!(v[3], Value::Float32(f) if f.is_nan()));
    }

    /// DecimalType: an unscaled value WIDER than 128 bits must compare exactly.
    /// `2^136` differs from `16` only in bits ABOVE 128, so the old i128 decode
    /// truncated `2^136` to `0` and reported `cmp(16, 2^136) = Greater` — the
    /// REVERSE of the true order. Arbitrary-precision `BigInt` orders it `Less`.
    /// Both share scale 0, so order is purely the unscaled big integers.
    #[test]
    fn decimals_with_unscaled_wider_than_128_bits() {
        let big = BigInt::from(2).pow(136).to_signed_bytes_be();
        // Sanity: body exceeds 16 bytes (128 bits), so i128 cannot hold it.
        assert!(big.len() > 16);

        let smaller = Value::Decimal {
            scale: 0,
            unscaled: BigInt::from(16).to_signed_bytes_be(),
        };
        let larger = Value::Decimal {
            scale: 0,
            unscaled: big,
        };
        // True order: 16 < 2^136 (old i128 truncation reported Greater here).
        assert_eq!(
            compare_collection_elements(&smaller, &larger),
            Ordering::Less
        );
        assert_eq!(
            compare_collection_elements(&larger, &smaller),
            Ordering::Greater
        );

        let mut v = vec![larger.clone(), smaller.clone()];
        v.sort_by(compare_collection_elements);
        assert_eq!(v, vec![smaller, larger]);
    }

    /// DecimalType: a huge scale difference (well beyond what 10^p fits in i128)
    /// must still align exactly. `1 * 10^0` vs `1 * 10^-40` → the first is far
    /// larger; i128 `mul_pow10` would saturate the alignment and could tie.
    #[test]
    fn decimals_with_large_scale_difference() {
        let whole = Value::Decimal {
            scale: 0,
            unscaled: vec![0x01], // 1
        };
        let tiny = Value::Decimal {
            scale: 40,
            unscaled: vec![0x01], // 1 * 10^-40
        };
        assert_eq!(compare_collection_elements(&tiny, &whole), Ordering::Less);
        assert_eq!(
            compare_collection_elements(&whole, &tiny),
            Ordering::Greater
        );
    }

    /// DecimalType DoS-safety: a scale near `i32::MAX` against a `scale 0` value
    /// must compare correctly AND instantly. Under the old approach this aligned
    /// to the max scale and built `10^2_000_000_000` as a `BigInt`, hanging/OOMing
    /// the writer. The bounded comparator decides by adjusted exponent
    /// (`(1-1) - 2_000_000_000 = -2e9` for the tiny value vs `(1-1) - 0 = 0` for
    /// the magnitude-1 value) without any `10^p`, so it returns immediately.
    #[test]
    fn decimal_extreme_scale_compares_fast_and_correct() {
        // tiny = 1 * 10^-2_000_000_000  (a positive value extremely close to 0)
        let tiny = Value::Decimal {
            scale: 2_000_000_000,
            unscaled: vec![0x01],
        };
        // one = 1 * 10^0
        let one = Value::Decimal {
            scale: 0,
            unscaled: vec![0x01],
        };
        // tiny is a vanishingly small positive number, far less than 1.
        assert_eq!(compare_collection_elements(&tiny, &one), Ordering::Less);
        assert_eq!(compare_collection_elements(&one, &tiny), Ordering::Greater);

        // The same against zero: tiny is positive, so > 0.
        let zero = Value::Decimal {
            scale: 0,
            unscaled: vec![0x00],
        };
        assert_eq!(compare_collection_elements(&tiny, &zero), Ordering::Greater);

        // And a negative extreme-scale value sorts below zero.
        let neg_tiny = Value::Decimal {
            scale: 2_000_000_000,
            unscaled: vec![0xFF], // -1 * 10^-2e9
        };
        assert_eq!(
            compare_collection_elements(&neg_tiny, &zero),
            Ordering::Less
        );
    }

    /// DecimalType: a huge scale difference where the SMALLER-scale number is the
    /// LARGER magnitude, and the reverse — decided by adjusted exponent without
    /// materializing `10^p`. `5 * 10^0 = 5` (scale 0) vs `999 * 10^-1_000_000_000`
    /// (a number with three digits but an enormous negative exponent, ≈ 0). The
    /// scale-0 value is far larger despite its smaller scale; reversing the roles
    /// (a huge whole number at scale 0 vs a finely-scaled value) flips the result.
    #[test]
    fn decimal_huge_scale_diff_magnitude_dominates() {
        let five = Value::Decimal {
            scale: 0,
            unscaled: vec![0x05], // 5
        };
        let near_zero = Value::Decimal {
            scale: 1_000_000_000,
            unscaled: vec![0x03, 0xE7], // 999 * 10^-1e9 ≈ 0
        };
        // Smaller scale (0) holds the larger magnitude.
        assert_eq!(
            compare_collection_elements(&five, &near_zero),
            Ordering::Greater
        );
        assert_eq!(
            compare_collection_elements(&near_zero, &five),
            Ordering::Less
        );

        // Reverse: a huge whole number (scale 0, adjusted exp huge) vs a finely
        // scaled small value — the larger-scale value is the smaller number.
        let huge_whole = Value::Decimal {
            scale: 0,
            unscaled: BigInt::from(10).pow(50).to_signed_bytes_be(), // 10^50
        };
        let small_fine = Value::Decimal {
            scale: 2_000_000_000,
            unscaled: vec![0x09], // 9 * 10^-2e9 ≈ 0
        };
        assert_eq!(
            compare_collection_elements(&huge_whole, &small_fine),
            Ordering::Greater
        );
    }

    /// DecimalType: adjusted exponents EQUAL → the bounded exact-rescale branch.
    /// `1.5` (unscaled 15, scale 1, adj = (2-1)-1 = 0) vs `2` (unscaled 2, scale
    /// 0, adj = (1-1)-0 = 0): same magnitude band, so the comparator rescales by
    /// the small `10^(digit-count delta)` and finds 1.5 < 2.
    #[test]
    fn decimal_equal_adjusted_exponent_exact_branch() {
        let one_point_five = Value::Decimal {
            scale: 1,
            unscaled: vec![0x0F], // 15 * 10^-1 = 1.5
        };
        let two = Value::Decimal {
            scale: 0,
            unscaled: vec![0x02], // 2
        };
        assert_eq!(
            compare_collection_elements(&one_point_five, &two),
            Ordering::Less
        );
        // Equal values across scales: 1.50 (150, scale 2) == 1.5 (15, scale 1).
        let one_point_fifty = Value::Decimal {
            scale: 2,
            unscaled: vec![0x00, 0x96], // 150 * 10^-2 = 1.50 (0x96 alone is negative)
        };
        assert_eq!(
            compare_collection_elements(&one_point_five, &one_point_fifty),
            Ordering::Equal
        );
    }

    /// Build a 16-byte UUID with the given version nibble (high nibble of byte 6),
    /// `msb_lo48` packed into the timestamp/version region, and `lsb` as the low
    /// 64 bits. `msb_hi32` is the time-low (bytes 0-3); `msb_mid16` is time-mid
    /// (bytes 4-5). Layout follows the standard big-endian UUID byte order so the
    /// comparator's `getLong(0)`/`getLong(8)` reads match Cassandra.
    fn make_uuid(time_low: u32, time_mid: u16, version: u8, time_hi: u16, lsb: u64) -> [u8; 16] {
        let mut u = [0u8; 16];
        u[0..4].copy_from_slice(&time_low.to_be_bytes());
        u[4..6].copy_from_slice(&time_mid.to_be_bytes());
        // bytes 6-7: top nibble = version, remaining 12 bits = time_hi (0..=0xFFF).
        let ver_and_hi: u16 = ((version as u16 & 0xf) << 12) | (time_hi & 0x0fff);
        u[6..8].copy_from_slice(&ver_and_hi.to_be_bytes());
        u[8..16].copy_from_slice(&lsb.to_be_bytes());
        u
    }

    /// UUIDType step 1: version nibble decides first. A version-1 UUID whose raw
    /// bytes are LARGER must still sort BEFORE a version-4 UUID, because
    /// `version1 - version2` (1 - 4 < 0) wins regardless of the rest. Under the
    /// old raw-serialized-byte fallback the all-0xFF v1 value would sort AFTER the
    /// small v4 value — the REVERSE — so this fails before the fix, passes after.
    #[test]
    fn uuid_version_decides_first() {
        // v1 with maximal bytes everywhere it can control.
        let v1 = make_uuid(0xFFFF_FFFF, 0xFFFF, 1, 0x0FFF, u64::MAX);
        // v4 with minimal bytes.
        let v4 = make_uuid(0x0000_0000, 0x0000, 4, 0x0000, 0);
        // Raw-byte fallback (the OLD behavior) would order v4 < v1 here:
        assert_eq!(v4.as_slice().cmp(v1.as_slice()), Ordering::Less);
        // Cassandra UUIDType orders by version first: v1 (1) < v4 (4), so
        // compare(v1, v4) is Less (and compare(v4, v1) is Greater).
        assert_eq!(
            compare_collection_elements(&Value::Uuid(v1), &Value::Uuid(v4)),
            Ordering::Less
        );
        assert_eq!(
            compare_collection_elements(&Value::Uuid(v4), &Value::Uuid(v1)),
            Ordering::Greater
        );
        let mut v = vec![Value::Uuid(v4), Value::Uuid(v1)];
        v.sort_by(compare_collection_elements);
        assert_eq!(v, vec![Value::Uuid(v1), Value::Uuid(v4)]);
    }

    /// UUIDType step 2 (version 1): two v1 UUIDs order by TIMESTAMP via
    /// `reorderTimestampBytes`, NOT by raw bytes. `earlier` has a smaller
    /// time_hi/time_mid but a LARGER time_low; the reorder puts time_hi/time_mid
    /// in the most-significant position, so `earlier` (smaller hi) sorts first
    /// even though its raw leading bytes (time_low) are larger. Grounded in
    /// `UUIDType.compareCustom` + `reorderTimestampBytes`.
    #[test]
    fn uuid_v1_orders_by_timestamp() {
        // earlier: time_hi=0x000, time_mid=0x0000, time_low=0xFFFF_FFFF
        let earlier = make_uuid(0xFFFF_FFFF, 0x0000, 1, 0x000, 0);
        // later: time_hi=0x001, everything else minimal
        let later = make_uuid(0x0000_0000, 0x0000, 1, 0x001, 0);
        // Raw bytes would call `earlier` (leading 0xFF..) GREATER than `later`:
        assert_eq!(earlier.as_slice().cmp(later.as_slice()), Ordering::Greater);
        // But the timestamp reorder makes the high-12-bits dominate: earlier < later.
        assert_eq!(
            compare_collection_elements(&Value::Uuid(earlier), &Value::Uuid(later)),
            Ordering::Less
        );
        let mut v = vec![Value::Uuid(later), Value::Uuid(earlier)];
        v.sort_by(compare_collection_elements);
        assert_eq!(v, vec![Value::Uuid(earlier), Value::Uuid(later)]);
    }

    /// UUIDType step 2 (non-v1) + step 3: two version-4 UUIDs compare MSB then LSB
    /// as UNSIGNED longs. Equal MSB, differing LSB → LSB unsigned order decides.
    /// Also a v4 whose MSB has the high bit set must sort ABOVE one without it
    /// (unsigned), which signed-long compare would get wrong.
    #[test]
    fn uuid_v4_orders_unsigned_msb_then_lsb() {
        // Same MSB region (version 4), LSB differs: 1 < 2.
        let a = make_uuid(0x0000_0001, 0x0000, 4, 0x000, 1);
        let b = make_uuid(0x0000_0001, 0x0000, 4, 0x000, 2);
        assert_eq!(
            compare_collection_elements(&Value::Uuid(a), &Value::Uuid(b)),
            Ordering::Less
        );

        // MSB high bit set (time_low top bit) must be treated as UNSIGNED-larger.
        let low_msb = make_uuid(0x0000_0001, 0x0000, 4, 0x000, 0);
        let high_msb = make_uuid(0x8000_0000, 0x0000, 4, 0x000, 0);
        assert_eq!(
            compare_collection_elements(&Value::Uuid(low_msb), &Value::Uuid(high_msb)),
            Ordering::Less
        );

        let mut v = vec![Value::Uuid(b), Value::Uuid(high_msb), Value::Uuid(a)];
        v.sort_by(compare_collection_elements);
        // a (msb small, lsb 1) < b (msb small, lsb 2) < high_msb (msb big)
        assert_eq!(
            v,
            vec![Value::Uuid(a), Value::Uuid(b), Value::Uuid(high_msb)]
        );
    }

    /// Unsigned-lexicographic types (text/blob/boolean) keep serialized-byte
    /// order — raw byte comparison is the correct Cassandra comparator there.
    #[test]
    fn text_keeps_byte_order() {
        let mut v = vec![
            Value::Text("c".to_string()),
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
                Value::Text("c".to_string()),
            ]
        );
    }
}

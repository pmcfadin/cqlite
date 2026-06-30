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
use std::cmp::Ordering;

/// Total-order comparator for two SET elements / MAP keys, matching the
/// element/key type's Cassandra `AbstractType.compare` (issue #1275).
///
/// For most types (text/ascii/blob/boolean/uuid/inet/date) that comparator IS
/// unsigned-lexicographic over the serialized bytes, so raw byte order is
/// correct. But the SIGNED numeric types order differently from their big-endian
/// two's-complement bytes:
///
///   * `Int32Type`(int) / `LongType`(bigint) / `ShortType`(smallint) /
///     `ByteType`(tinyint) / counter — SIGNED integer order. `-1` (`0xFFFF_FFFF`)
///     sorts LAST under unsigned bytes but FIRST under the type comparator.
///   * `TimestampType` / `TimeType` — extend/share `LongType`'s signed long
///     compare.
///   * `FloatType` / `DoubleType` — `Float.compare` / `Double.compare`: a total
///     order where `-0.0 < +0.0` and `NaN` sorts last; the sign bit makes raw
///     big-endian byte order wrong for negatives. Matched here with Rust's
///     `f32/f64::total_cmp`, which is exactly `Float/Double.compare`.
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
        (Value::Float32(x), Value::Float32(y)) => x.total_cmp(y),
        (Value::Float(x), Value::Float(y)) => x.total_cmp(y),
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
        // Frozen wrappers: compare the inner values by the same rule.
        (Value::Frozen(x), other) => compare_collection_elements(x, other),
        (x, Value::Frozen(y)) => compare_collection_elements(x, y),
        // Everything else (text/ascii/blob/boolean/uuid/inet/date/duration, or a
        // mixed/unsupported pair): the type comparator is unsigned-lexicographic
        // over the serialized bytes, so fall back to comparing serialized bytes.
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

/// Scale-aware numeric comparison of two `decimal` values, matching Cassandra
/// `DecimalType.compare` (BigDecimal numeric order) (issue #1275). The unscaled
/// part is a two's-complement varint; the value is `unscaled * 10^-scale`. We
/// align to the common (max) scale: multiply each unscaled integer by
/// `10^(max_scale - own_scale)` and compare the resulting signed big integers.
fn compare_decimal(scale_a: i32, unscaled_a: &[u8], scale_b: i32, unscaled_b: &[u8]) -> Ordering {
    let mut big_a = varint_to_bigint(unscaled_a);
    let mut big_b = varint_to_bigint(unscaled_b);
    let max_scale = scale_a.max(scale_b);
    // value = unscaled * 10^-scale; to align scales multiply by 10^(max-own).
    if let Some(p) = max_scale.checked_sub(scale_a).filter(|p| *p > 0) {
        big_a = mul_pow10(big_a, p as u32);
    }
    if let Some(p) = max_scale.checked_sub(scale_b).filter(|p| *p > 0) {
        big_b = mul_pow10(big_b, p as u32);
    }
    big_a.cmp(&big_b)
}

/// Decode a two's-complement big-endian `varint` body into a signed big integer.
/// Bodies in this codebase are bounded (collection elements), so an `i128` is
/// sufficient for realistic decimal/varint magnitudes; oversized bodies saturate
/// toward the correct sign so the comparison stays total and never panics.
fn varint_to_bigint(bytes: &[u8]) -> i128 {
    let Some(&first) = bytes.first() else {
        return 0;
    };
    let negative = first & 0x80 != 0;
    let mut acc: i128 = if negative { -1 } else { 0 };
    for &byte in bytes {
        match acc
            .checked_shl(8)
            .and_then(|shifted| shifted.checked_add(byte as i128))
        {
            Some(next) => acc = next,
            None => return if negative { i128::MIN } else { i128::MAX },
        }
    }
    acc
}

/// Multiply a signed big integer by `10^exp`, saturating toward the correct sign
/// on overflow (keeps the comparator total and panic-free for extreme inputs).
fn mul_pow10(value: i128, exp: u32) -> i128 {
    let mut acc = value;
    for _ in 0..exp {
        match acc.checked_mul(10) {
            Some(next) => acc = next,
            None => return if value < 0 { i128::MIN } else { i128::MAX },
        }
    }
    acc
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

    /// Unsigned-lexicographic types (text/blob/uuid/boolean) keep serialized-byte
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

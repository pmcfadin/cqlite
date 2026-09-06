//! GROUP BY key hashing + equality (issue #1587 hash index, #2074 Cassandra
//! float comparator).
//!
//! Split out of `aggregation.rs` (campsite rule, epic #1116) — these are pure,
//! self-contained functions with no `AggregationState`/executor dependencies.
//! [`hash_group_key`] and [`group_key_eq`] MUST stay mutually consistent (equal
//! keys hash equal) — see each doc comment.

use crate::types::Value;
use std::hash::{Hash, Hasher};

/// Hash a group key consistently with [`group_key_eq`]: if two keys are equal
/// under that predicate, they hash identically. Floats follow Cassandra's
/// total-order comparator (issue #2074), so NaN is canonicalized to a single bit
/// pattern (all NaN keys hash-collide into ONE group) and signed-zero bits are
/// kept DISTINCT (`-0.0` and `+0.0` hash apart into two groups) — at ALL nesting
/// depths, recursing into `List`/`Set`/`Map`/`Tuple`/`Frozen`. Exotic variants
/// that are not cheap to hash structurally fall back to a discriminant-only
/// contribution — still consistent (equal values agree on discriminant), merely
/// less selective, and only for key types that never appear in practice.
pub(super) fn hash_group_key(key: &[Value]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.len().hash(&mut hasher);
    for v in key {
        hash_one_value(v, &mut hasher);
    }
    hasher.finish()
}

fn hash_one_value<H: Hasher>(v: &Value, h: &mut H) {
    // Tag by discriminant so different variants never alias.
    std::mem::discriminant(v).hash(h);
    match v {
        Value::Null => {}
        // Hash the TYPE TAG, so `Empty(int)` and `Empty(bigint)` land in
        // different groups (issue #3805). The discriminant hashed above already
        // separates `Empty` from `Null` and from every typed zero.
        Value::Empty(ty) => ty.hash(h),
        Value::Boolean(b) => b.hash(h),
        Value::Integer(x) => x.hash(h),
        Value::BigInt(x) | Value::Counter(x) | Value::Timestamp(x) | Value::Time(x) => x.hash(h),
        Value::Date(x) => x.hash(h),
        Value::TinyInt(x) => x.hash(h),
        Value::SmallInt(x) => x.hash(h),
        // Issue #2074 (Cassandra comparator, consistent with `group_key_eq`):
        // canonicalize NaN to ONE bit pattern so all NaN keys hash-collide into one
        // group; keep signed-zero bits DISTINCT so `-0.0`/`+0.0` hash into two.
        Value::Float(f) => (if f.is_nan() { f64::NAN } else { *f }).to_bits().hash(h),
        Value::Float32(f) => (if f.is_nan() { f32::NAN } else { *f }).to_bits().hash(h),
        Value::Text(s) => s.hash(h),
        Value::Blob(b) | Value::Varint(b) | Value::Inet(b) => b.hash(h),
        Value::Uuid(u) => u.hash(h),
        Value::Decimal { scale, unscaled } => {
            scale.hash(h);
            unscaled.hash(h);
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            months.hash(h);
            days.hash(h);
            nanos.hash(h);
        }
        Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
            for item in items {
                hash_one_value(item, h);
            }
        }
        Value::Map(pairs) => {
            for (k, val) in pairs {
                hash_one_value(k, h);
                hash_one_value(val, h);
            }
        }
        Value::Frozen(inner) => hash_one_value(inner, h),
        // Exotic / rarely-grouped variants: discriminant-only (still consistent).
        Value::Json(_) | Value::Udt(_) | Value::Tombstone(_) => {}
    }
}

/// Group-key VALUE equality with Cassandra float-comparator semantics (issue
/// #2074), at ALL nesting depths: `float`/`double` — top-level OR nested inside a
/// `List`/`Set`/`Map`/`Tuple`/`Frozen` group key — route through `float_cmp` so
/// all NaN bit-patterns are EQUAL and `-0.0`/`+0.0` are DISTINCT (Cassandra's
/// comparator); every other leaf uses derived `==`. Recurses through the SAME
/// variants [`hash_one_value`] does, so it stays consistent with
/// [`hash_group_key`] at every depth. Cross-variant keys stay distinct (the
/// `_ => a == b` fallback) — never `compare_values_ordering`'s cross-coercion.
fn group_value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float(p), Value::Float(q)) => {
            crate::float_cmp::cassandra_double_cmp(*p, *q).is_eq()
        }
        (Value::Float32(p), Value::Float32(q)) => {
            crate::float_cmp::cassandra_float_cmp(*p, *q).is_eq()
        }
        (Value::List(x), Value::List(y))
        | (Value::Set(x), Value::Set(y))
        | (Value::Tuple(x), Value::Tuple(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(m, n)| group_value_eq(m, n))
        }
        (Value::Map(x), Value::Map(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((k1, v1), (k2, v2))| group_value_eq(k1, k2) && group_value_eq(v1, v2))
        }
        (Value::Frozen(x), Value::Frozen(y)) => group_value_eq(x, y),
        _ => a == b,
    }
}

/// Group-key equality: see [`group_value_eq`] for the per-element (recursive,
/// float-comparator-aware) semantics.
pub(super) fn group_key_eq(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| group_value_eq(x, y))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #2074: hash/eq consistency at every nesting depth — the specific
    /// property the review round flagged. For every pair below, `group_key_eq`
    /// agreeing (or disagreeing) MUST match `hash_group_key` agreeing (or
    /// disagreeing), or the hash index would silently split/merge groups the
    /// exact-match confirmation disagrees with.
    #[test]
    fn hash_and_eq_agree_for_nested_floats() {
        let other_nan = f64::from_bits(0xFFF8_0000_0000_0001);
        assert!(other_nan.is_nan());
        let cases: &[(Value, Value, bool)] = &[
            (
                Value::Tuple(vec![Value::Float(0.0)]),
                Value::Tuple(vec![Value::Float(-0.0)]),
                false, // -0.0/+0.0 distinct even nested
            ),
            (
                Value::Tuple(vec![Value::Float(f64::NAN)]),
                Value::Tuple(vec![Value::Float(other_nan)]),
                true, // differing NaN bit patterns merge even nested
            ),
            (
                Value::List(vec![Value::Float(0.0)]),
                Value::List(vec![Value::Float(-0.0)]),
                false,
            ),
            (
                Value::Map(vec![(Value::Integer(1), Value::Float(0.0))]),
                Value::Map(vec![(Value::Integer(1), Value::Float(-0.0))]),
                false,
            ),
            (
                Value::Frozen(Box::new(Value::Float(f64::NAN))),
                Value::Frozen(Box::new(Value::Float(other_nan))),
                true,
            ),
        ];
        for (a, b, expect_eq) in cases {
            let ka = [a.clone()];
            let kb = [b.clone()];
            let eq = group_key_eq(&ka, &kb);
            assert_eq!(
                eq, *expect_eq,
                "group_key_eq({a:?}, {b:?}) expected {expect_eq}"
            );
            let hash_eq = hash_group_key(&ka) == hash_group_key(&kb);
            assert!(
                hash_eq == eq || !eq,
                "hash/eq inconsistency for {a:?} vs {b:?}: eq={eq} but hashes {}",
                if hash_eq { "match" } else { "differ" }
            );
            // When eq() says equal, hash MUST also agree (the formal contract);
            // when eq() says unequal, colliding hashes are merely a (harmless)
            // collision, not a bug, so we only assert the equal-implies-equal-hash
            // direction strictly.
            if eq {
                assert!(
                    hash_eq,
                    "eq() reports equal but hashes differ for {a:?} vs {b:?} — \
                     violates the hash/eq contract"
                );
            }
        }
    }
}

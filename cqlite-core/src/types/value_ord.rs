//! Value ordering for [`Value`](super::Value) — the `PartialOrd` impl.
//!
//! Split out of `types.rs` under the campsite rule (epic #1116): that file is far
//! over the 800-line source target, and this comparator's contract needs a long
//! doc comment (two divergences from the derived `PartialEq`, and the
//! `time`/`timestamp` asymmetry) rather than a short one.

use crate::types::Value;

/// Ordering for `Value`.
///
/// NOTE (contract split, #1870/#2010/#2074): this `PartialOrd` INTENTIONALLY
/// diverges from the DERIVED `PartialEq`. For `float`/`double` it uses the
/// Cassandra/Java total order (`-0.0 < +0.0`; every `NaN` sorts last and compares
/// Equal to every other `NaN`), whereas the derived `PartialEq` keeps IEEE
/// semantics (`-0.0 == +0.0`, `NaN != NaN`). `partial_cmp` may thus report `Equal`
/// where `eq` reports `false` (two NaNs) and vice-versa (`-0.0`/`+0.0`). GROUP BY
/// grouping (issue #2074) does NOT use the derived `PartialEq` for floats: the
/// aggregation group-key path (`aggregation::group_key_eq` + `hash_group_key`)
/// routes them through this SAME total order (all NaN → ONE group; `-0.0`/`+0.0`
/// DISTINCT). Any future `impl Ord for Value` MUST reuse this comparator, never
/// derive from `PartialEq`.
///
/// NOTE (`time` vs `timestamp`, #3935): the `Time` arm compares the 8-byte
/// BIG-ENDIAN serialized form as UNSIGNED bytes, matching `TimeType`'s
/// `super(ComparisonType.BYTE_ORDER)` (pinned `cassandra-5.0.8`
/// `db/marshal/TimeType.java`); `ByteBufferUtil.compareUnsigned` is what
/// `BYTE_ORDER` resolves to. The `Timestamp` arm stays SIGNED numeric, because
/// `TimestampType` is `ComparisonType.CUSTOM` and its `compareCustom` delegates
/// to `LongType.compareLongs`. The two arms therefore diverge BY DESIGN for a
/// negative long, and unifying them would break one of the two types. This arm
/// now agrees with `types::comparator::custom::compare_time` and with the write
/// path's `write_engine::clustering_order::compare_values` for ALL inputs.
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use crate::float_cmp::{cassandra_double_cmp as dcmp, cassandra_float_cmp as fcmp};
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),

            // EMPTY-BUFFER SENTINEL (issue #3805). `Int32Type.compareCustom`
            // (`db/marshal/Int32Type.java:61-71` at `cassandra-5.0.8`) is
            //
            //     if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            //         return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));
            //
            // so with only the LEFT empty: `Boolean.compare(false, true) == -1`.
            // An empty buffer therefore sorts strictly BEFORE every non-empty
            // value of its type, INCLUDING `Integer.MIN_VALUE`, and two empties
            // are Equal (both branches empty ⇒ `Boolean.compare(true, true)`
            // == 0). Measured on real Cassandra-5.0.2 bytes for four independent
            // key types — the empty key is FIRST in every column
            // (`docs/round-artifacts/issue-3805-cassandra-oracle.md` §4b.4).
            //
            // These arms sit immediately after the `Null` arms so `Null` keeps
            // its existing first position (`Null` is not a key Cassandra admits
            // at all, so no Cassandra ordering is being contradicted) and
            // BEFORE the per-variant arms, so a sentinel can never fall through
            // to the `to_string()` fallback below.
            (Value::Empty(a), Value::Empty(b)) => {
                // Same type ⇒ the same (empty) bytes ⇒ Equal, exactly as
                // `compareCustom` reports for two empty buffers. Different
                // declared types is a cross-type comparison, which Cassandra
                // never performs (a map has ONE key type); ordering by the tag
                // keeps this total and deterministic rather than arbitrary.
                Some(a.cmp(b))
            }
            (Value::Empty(_), _) => Some(Ordering::Less),
            (_, Value::Empty(_)) => Some(Ordering::Greater),

            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::Counter(a), Value::Counter(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => Some(dcmp(*a, *b)),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            // `timestamp` (`TimestampType`) is ComparisonType.CUSTOM and its
            // `compareCustom` delegates to `LongType.compareLongs` — SIGNED.
            // `time` (`TimeType`) is ComparisonType.BYTE_ORDER — UNSIGNED bytes.
            // The asymmetry is Cassandra's; do NOT "unify" these two arms.
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.to_be_bytes().partial_cmp(&b.to_be_bytes()),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),
            (Value::TinyInt(a), Value::TinyInt(b)) => a.partial_cmp(b),
            (Value::SmallInt(a), Value::SmallInt(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float32(b)) => Some(fcmp(*a, *b)),
            (Value::Inet(a), Value::Inet(b)) => a.partial_cmp(b),

            // For complex types, compare by string representation
            (a, b) => a.to_string().partial_cmp(&b.to_string()),
        }
    }
}

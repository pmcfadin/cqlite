//! Value ordering for the CQL scalar types that map to
//! [`ComparatorType::Custom`](super::ComparatorType) (issue #3790).
//!
//! `ComparatorType::from_cql_type` represents `CqlType::Time` as
//! `Custom("time")` and `CqlType::Inet` as `Custom("inet")`. Both are legal
//! Cassandra clustering-column types, so both MUST order by value; the generic
//! `Custom(_)` fallback orders by the FORMATTED STRING, which misorders them
//! (e.g. `"10.0.0.2" < "9.0.0.1"` as text, while `[10,0,0,2] > [9,0,0,1]` as
//! address bytes).
//!
//! Dispatching on the `Custom(name)` string is schema-driven — the name is
//! derived authoritatively from the declared type, NOT inferred from byte
//! patterns (no-heuristics mandate, issue #28). Same idiom as
//! `storage::sstable::reader::parsing::custom_scalar`.
//!
//! Format authority — pinned Cassandra 5.0.8 (a CQLite `file:line` is never
//! format authority, #3041):
//! * `src/java/org/apache/cassandra/db/marshal/InetAddressType.java`:
//!   `InetAddressType() { super(ComparisonType.BYTE_ORDER); }`
//! * `src/java/org/apache/cassandra/db/marshal/TimeType.java`:
//!   `TimeType() { super(ComparisonType.BYTE_ORDER); }`
//!
//! `ComparisonType.BYTE_ORDER` resolves to `ByteBufferUtil.compareUnsigned`:
//! UNSIGNED lexicographic comparison of the serialized bytes, with the shorter
//! operand first where it is a prefix of the longer.
//!
//! `inet` implements that literally (`[u8]::cmp` IS `compareUnsigned`). **`time`
//! does NOT, deliberately**: it compares the nanos SIGNED, which is identical to
//! byte order for every value Cassandra can produce (the valid range is
//! non-negative) and which agrees with `Value`'s own `PartialOrd`, the ordering
//! the writer/memtable paths use. See `compare_time` for the measured reason and
//! issue #3920, which owns unifying validation and ordering. Stated here so the
//! authority citations above are not read as a claim about what this module does
//! for `time`.

use crate::types::Value;
use crate::{Error, Result};
use std::cmp::Ordering;

/// Compare two values under `ComparatorType::Custom(name)`.
///
/// `inet` and `time` order by value (see the module docs). Every OTHER name is
/// the RESIDUAL path — an unresolved UDT reference or a genuinely unknown type,
/// for which no value ordering is defined — and keeps the historical
/// formatted-string comparison. That string compare is ONLY that residual.
pub(super) fn compare(name: &str, left: &Value, right: &Value) -> Result<Ordering> {
    match name {
        "inet" => compare_inet(left, right),
        "time" => compare_time(left, right),
        _ => {
            let l_str = format!("{}", left);
            let r_str = format!("{}", right);
            Ok(l_str.cmp(&r_str))
        }
    }
}

/// True when `Custom(name)` denotes a scalar type that orders by value.
///
/// `supports_ordering()` is public API and must not lie: `inet` and `time` are
/// legal Cassandra clustering-column types. The residual unresolved-UDT /
/// unknown name does not order.
pub(super) fn supports_ordering(name: &str) -> bool {
    name == "inet" || name == "time"
}

/// `inet`: unsigned bytewise comparison of the RAW serialized address —
/// `Value::Inet` already holds exactly those bytes (4 for IPv4, 16 for IPv6),
/// so no deserialization is needed.
///
/// Rust's `<[u8] as Ord>::cmp` IS `ByteBufferUtil.compareUnsigned`: unsigned
/// lexicographic, and where one operand is a prefix of the other the SHORTER
/// sorts first. That is also what settles the IPv4-vs-IPv6 differing-length
/// case, so length needs no special handling here.
fn compare_inet(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Inet(l), Value::Inet(r)) => Ok(l.as_ref().cmp(r.as_ref())),
        _ => Err(Error::Schema(
            "Type mismatch: expected inet values".to_string(),
        )),
    }
}

/// `time`: SIGNED comparison of the nanos-since-midnight long.
///
/// Cassandra's `TimeType` is `super(ComparisonType.BYTE_ORDER)` over the 8-byte
/// big-endian long (pinned `cassandra-5.0.8`), and over `time`'s VALID range
/// (`0..=86_399_999_999_999`) every value is non-negative, so byte order,
/// unsigned order and signed order all COINCIDE. For every value Cassandra can
/// produce, this IS Cassandra's order.
///
/// WHY SIGNED AND NOT `to_be_bytes()` — this was byte-order-exact first, and the
/// change is deliberate (roborev jobs 45/46, issue #3920).
///
/// The two readings differ only for an out-of-range NEGATIVE nanos, whose sign
/// bit makes the leading byte `0xFF`: unsigned byte order sorts it ABOVE every
/// non-negative value, signed sorts it below. Byte-order-exactness looks like the
/// stricter choice, but it puts this comparator in conflict with the rest of
/// CQLite, and MEASURED, that conflict is REACHABLE:
///
/// * NOTHING validates the range. `86_399_999_999_999` appears in no check
///   anywhere in `cqlite-core/src` — decode is `map(be_i64, Value::Time)`
///   (`parser/types/primitives.rs`), which accepts ANY bit pattern, and encode
///   writes `nanos.to_be_bytes()` verbatim (`parser/types/mod.rs`). So a negative
///   `Value::Time` is constructible, WRITABLE, and READABLE.
/// * `Value`'s own `PartialOrd` compares SIGNED, and writer/memtable paths order
///   through it.
///
/// So an unsigned comparator would let CQLite write a negative `time` in one order
/// and read it back in another — a genuine misordering in CQLite-WRITTEN data,
/// traded for exactness on input Cassandra cannot generate. Agreeing with the
/// writer is the conservative direction until validation and ordering are changed
/// TOGETHER, which is #3920's job.
fn compare_time(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Time(l), Value::Time(r)) => Ok(l.cmp(r)),
        _ => Err(Error::Schema(
            "Type mismatch: expected time values".to_string(),
        )),
    }
}

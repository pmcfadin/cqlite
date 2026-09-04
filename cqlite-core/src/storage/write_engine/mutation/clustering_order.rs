//! Clustering-value ordering for the write engine — `compare_values`.
//!
//! Split out of `mutation.rs` under the campsite rule (epic #1116): that file is
//! far over the 800-line source target, and this comparator carries the long
//! per-type authority notes (float total order, and the `time`/`timestamp`
//! asymmetry) that #3935 required.
//!
//! A CHILD of `mutation` rather than a sibling under `write_engine`, on purpose:
//! `write_engine/mod.rs` is itself ~3560 lines, so adding a `mod` line there
//! would grow an over-threshold file and the `file-size` ratchet would (rightly)
//! FAIL. Declaring it from `mutation.rs`, which this split leaves ~70 lines
//! SMALLER than before, costs the ratchet nothing — and `ClusteringKey` is its
//! only caller anyway.
//!
//! This is the comparator BOTH routes to clustering order reach:
//! [`ClusteringKey::compare`](super::ClusteringKey::compare) (schema-aware,
//! used by `write_engine::merge` to sort merged rows "for output order") and
//! `ClusteringKey`'s `Ord` (the memtable `BTreeMap` key order). It therefore
//! REQUIRES a strict total order — see the per-arm notes.

use std::cmp::Ordering;

use crate::types::Value;
use crate::{Error, Result};

/// Compare two values for ordering
pub(super) fn compare_values(a: &Value, b: &Value) -> Result<Ordering> {
    use Value::*;

    match (a, b) {
        (Null, Null) => Ok(Ordering::Equal),
        (Null, _) => Ok(Ordering::Less),
        (_, Null) => Ok(Ordering::Greater),

        (Boolean(a), Boolean(b)) => Ok(a.cmp(b)),
        (TinyInt(a), TinyInt(b)) => Ok(a.cmp(b)),
        (SmallInt(a), SmallInt(b)) => Ok(a.cmp(b)),
        (Integer(a), Integer(b)) => Ok(a.cmp(b)),
        (BigInt(a), BigInt(b)) => Ok(a.cmp(b)),
        (Counter(a), Counter(b)) => Ok(a.cmp(b)),
        // Cassandra/Java total order (NaN last, -0.0 < +0.0) — NOT IEEE
        // partial_cmp. This feeds ClusteringKey's `Ord`/`compare` (memtable
        // BTreeMap key order + compaction merge), which requires a TOTAL order:
        // a non-total order would let NaN compare Equal to everything
        // (transitivity violation) and collapse -0.0/+0.0. See float_cmp.rs and
        // issues #1870/#2010. Must agree with the reader's Value::partial_cmp,
        // which since #3935 it does for EVERY arm, `Time` included, and with
        // `types::comparator::custom::compare_time`.
        (Float32(a), Float32(b)) => Ok(crate::float_cmp::cassandra_float_cmp(*a, *b)),
        (Float(a), Float(b)) => Ok(crate::float_cmp::cassandra_double_cmp(*a, *b)),
        (Text(a), Text(b)) => Ok(a.cmp(b)),
        (Blob(a), Blob(b)) => Ok(a.cmp(b)),
        // `timestamp` is `TimestampType` = ComparisonType.CUSTOM, whose
        // `compareCustom` delegates to `LongType.compareLongs` — SIGNED.
        (Timestamp(a), Timestamp(b)) => Ok(a.cmp(b)),
        (Date(a), Date(b)) => Ok(a.cmp(b)),
        // `time` is `TimeType` = ComparisonType.BYTE_ORDER (pinned
        // `cassandra-5.0.8` `db/marshal/TimeType.java`), i.e.
        // `ByteBufferUtil.compareUnsigned` over the 8-byte big-endian
        // nanos-of-day. NOT signed `i64::cmp`: the two agree over `time`'s valid
        // range (`0..=86_399_999_999_999`) and diverge for an out-of-range
        // NEGATIVE nanos, which Cassandra's own binary `validate` accepts
        // (see `types::comparator::custom::compare_time` for the canonical
        // statement, and #3935 for why validation is NOT the fix).
        //
        // TOTAL-ORDER SAFETY: this arm feeds `ClusteringKey`'s `Ord`/`compare`,
        // hence the memtable `BTreeMap` key order, the compaction merge order
        // and the physical `Data.db` row order — all of which REQUIRE a strict
        // total order. Unsigned lexicographic comparison of a FIXED 8-byte array
        // is trivially total (antisymmetric, transitive, no incomparable pair),
        // so this is safe; the total-order hazard lives in the float arms above.
        (Time(a), Time(b)) => Ok(a.to_be_bytes().cmp(&b.to_be_bytes())),
        (Uuid(a), Uuid(b)) => Ok(a.cmp(b)),
        (Inet(a), Inet(b)) => Ok(a.cmp(b)),

        // Collection types (element-wise lexicographic comparison)
        (List(a), List(b)) | (Set(a), Set(b)) => {
            for (elem_a, elem_b) in a.iter().zip(b.iter()) {
                let ord = compare_values(elem_a, elem_b)?;
                if ord != Ordering::Equal {
                    return Ok(ord);
                }
            }
            Ok(a.len().cmp(&b.len()))
        }
        (Map(a), Map(b)) => {
            for ((ka, va), (kb, vb)) in a.iter().zip(b.iter()) {
                let key_ord = compare_values(ka, kb)?;
                if key_ord != Ordering::Equal {
                    return Ok(key_ord);
                }
                let val_ord = compare_values(va, vb)?;
                if val_ord != Ordering::Equal {
                    return Ok(val_ord);
                }
            }
            Ok(a.len().cmp(&b.len()))
        }
        (Tuple(a), Tuple(b)) => {
            for (fa, fb) in a.iter().zip(b.iter()) {
                let ord = compare_values(fa, fb)?;
                if ord != Ordering::Equal {
                    return Ok(ord);
                }
            }
            Ok(a.len().cmp(&b.len()))
        }

        // Frozen wrapper: compare inner values
        (Frozen(a), Frozen(b)) => compare_values(a, b),

        _ => Err(Error::InvalidInput(format!(
            "Cannot compare values of different types: {:?} vs {:?}",
            a, b
        ))),
    }
}

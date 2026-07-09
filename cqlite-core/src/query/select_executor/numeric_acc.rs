//! Numeric accumulation domain for SUM/AVG (issue #2202).
//!
//! Split from `aggregation.rs` (campsite rule, epic #1116) so the accumulator's
//! type-preserving arithmetic lives in one small, self-contained place.

use crate::schema::CqlType;
use crate::types::Value;

/// Numeric accumulation domain for SUM/AVG (issue #2202). The domain is fixed at
/// group-init time from the aggregate's resolved RESULT CQL type (the SAME
/// `select_naming::sum_avg_result_cql_type` the result metadata is built from),
/// so the emitted value variant can never disagree with the metadata type.
#[derive(Debug, Clone)]
pub(super) enum NumericAcc {
    /// Integral SUM/AVG. `sum` accumulates in `i64` with Cassandra's two's-
    /// complement WRAPPING overflow (Java integer/long semantics) — never
    /// saturating or panicking. `wide` distinguishes the `bigint`/`counter`
    /// result (emitted as `Value::BigInt`, wraps at the i64 boundary) from the
    /// `int`/`smallint`/`tinyint` result (emitted as `Value::Integer`, truncated
    /// to i32 on finalize).
    ///
    /// Wrapping equivalence: modular (two's-complement) addition is associative,
    /// so accumulating narrow inputs in i64 with `wrapping_add` and truncating to
    /// i32 at the end yields exactly Java's step-wise i32-wrapped sum
    /// (`(x mod 2^64) mod 2^32 == x mod 2^32`). AVG uses i64 integer division
    /// (truncating toward zero, matching Java); an AVG over `bigint` whose i64
    /// sum overflows i64 can diverge from Cassandra's BigInteger AVG — an extreme
    /// edge (summing enough near-i64::MAX values) that real data never hits.
    Integral { sum: i64, wide: bool },
    /// Floating SUM/AVG (`float`/`double`/`decimal`/`varint`/unknown argument) —
    /// accumulated in `f64` and emitted as `Value::Float` (`double`), matching
    /// CQLite's pre-#2202 behaviour with no regression.
    Floating(f64),
}

impl NumericAcc {
    /// Initialize the accumulation domain from the aggregate's resolved result
    /// CQL type. `int`/`smallint`/`tinyint` → narrow integral, `bigint`/`counter`
    /// → wide integral, everything else (incl. an unknown/`None` type) → floating.
    /// Mirrors [`crate::query::select_naming::sum_avg_result_cql_type`].
    pub(super) fn init(result_type: Option<&CqlType>) -> Self {
        match result_type {
            Some(CqlType::TinyInt | CqlType::SmallInt | CqlType::Int) => NumericAcc::Integral {
                sum: 0,
                wide: false,
            },
            Some(CqlType::BigInt | CqlType::Counter) => NumericAcc::Integral { sum: 0, wide: true },
            _ => NumericAcc::Floating(0.0),
        }
    }

    /// Fold one non-null input value into the accumulator, returning whether the
    /// value was accepted (converted in this domain). Integral domains read
    /// `as_i64` and wrap; the floating domain reads `as_f64`. A value that does
    /// not convert (wrong variant for the domain) is ignored and returns `false`,
    /// exactly as the prior `as_f64`-only path ignored non-numeric inputs. AVG
    /// increments its count only on an accepted value, so a `0` still counts.
    pub(super) fn add(&mut self, value: &Value) -> bool {
        match self {
            NumericAcc::Integral { sum, .. } => match value.as_i64() {
                Some(v) => {
                    *sum = sum.wrapping_add(v);
                    true
                }
                None => false,
            },
            NumericAcc::Floating(sum) => match value.as_f64() {
                Some(v) => {
                    *sum += v;
                    true
                }
                None => false,
            },
        }
    }

    /// The finalized SUM value: integral domains emit `Value::Integer` (narrow,
    /// i32-truncated) or `Value::BigInt` (wide); the floating domain emits
    /// `Value::Float`.
    pub(super) fn finalize_sum(&self) -> Value {
        match self {
            NumericAcc::Integral { sum, wide: false } => Value::Integer(*sum as i32),
            NumericAcc::Integral { sum, wide: true } => Value::BigInt(*sum),
            NumericAcc::Floating(sum) => Value::Float(*sum),
        }
    }

    /// The finalized AVG value over `count` inputs (`count > 0`). Integral
    /// domains use i64 integer division (truncating toward zero, like Java) and
    /// emit the matching narrow/wide variant; the floating domain divides in f64.
    pub(super) fn finalize_avg(&self, count: u64) -> Value {
        match self {
            NumericAcc::Integral { sum, wide: false } => {
                Value::Integer((sum / count as i64) as i32)
            }
            NumericAcc::Integral { sum, wide: true } => Value::BigInt(sum / count as i64),
            NumericAcc::Floating(sum) => Value::Float(sum / count as f64),
        }
    }
}

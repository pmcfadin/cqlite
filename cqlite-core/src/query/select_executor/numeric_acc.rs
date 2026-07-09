//! Numeric accumulation domain for SUM/AVG (issue #2202).
//!
//! Split from `aggregation.rs` (campsite rule, epic #1116) so the accumulator's
//! type-preserving arithmetic lives in one small, self-contained place.

use crate::schema::CqlType;
use crate::types::Value;

/// The integral width SUM/AVG accumulates and narrows to at finalize (issue
/// #2202). Cassandra's `AggregateFcts.java` returns the SAME type as the
/// argument for every integral column — it does NOT promote `tinyint`/
/// `smallint` to `int` (its own docs warn of an overflow risk precisely
/// because the result stays the input's narrow width).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IntWidth {
    /// `tinyint` — narrows to `Value::TinyInt` (i8), wraps at the i8 boundary.
    Byte,
    /// `smallint` — narrows to `Value::SmallInt` (i16), wraps at the i16 boundary.
    Short,
    /// `int` — narrows to `Value::Integer` (i32), wraps at the i32 boundary.
    Int,
    /// `bigint`/`counter` — `Value::BigInt` (i64) directly, no narrowing.
    Long,
}

impl IntWidth {
    /// Narrow a wide (`i64`) accumulator to this width's `Value` variant. `as`
    /// truncation is Rust's two's-complement narrowing cast — the same
    /// semantics as Java's narrowing primitive conversion (`(byte) x`/
    /// `(short) x`/`(int) x`) that Cassandra's own aggregate functions use, so
    /// this is exact parity, not an approximation.
    fn narrow(self, sum: i64) -> Value {
        match self {
            IntWidth::Byte => Value::TinyInt(sum as i8),
            IntWidth::Short => Value::SmallInt(sum as i16),
            IntWidth::Int => Value::Integer(sum as i32),
            IntWidth::Long => Value::BigInt(sum),
        }
    }
}

/// Numeric accumulation domain for SUM/AVG (issue #2202). The domain is fixed at
/// group-init time from the aggregate's resolved RESULT CQL type (the SAME
/// `select_naming::sum_avg_result_cql_type` the result metadata is built from),
/// so the emitted value variant can never disagree with the metadata type.
#[derive(Debug, Clone)]
pub(super) enum NumericAcc {
    /// Integral SUM/AVG. `sum` accumulates in `i64` with Cassandra's two's-
    /// complement WRAPPING overflow (Java integer/long semantics) — never
    /// saturating or panicking — and narrows to `width` only at finalize.
    ///
    /// Wrapping equivalence: modular (two's-complement) addition is associative,
    /// so accumulating in i64 with `wrapping_add` and truncating to the narrow
    /// width at the end yields exactly the same result as wrapping at every
    /// step in that narrow width (`(x mod 2^64) mod 2^n == x mod 2^n`). This
    /// also matches Cassandra's OWN implementation: its AVG aggregates sum in a
    /// Java `long` and narrow only at `compute()`, exactly like this. AVG uses
    /// i64 integer division on the wide sum (truncating toward zero, matching
    /// Java) before narrowing; an AVG over `bigint` whose i64 sum overflows i64
    /// can diverge from Cassandra's BigInteger AVG — an extreme edge (summing
    /// enough near-i64::MAX values) that real data never hits.
    Integral { sum: i64, width: IntWidth },
    /// Floating SUM/AVG (`float`/`double`/`decimal`/`varint`/unknown argument) —
    /// accumulated in `f64` and emitted as `Value::Float` (`double`), matching
    /// CQLite's pre-#2202 behaviour with no regression.
    Floating(f64),
}

impl NumericAcc {
    /// Initialize the accumulation domain from the aggregate's resolved result
    /// CQL type. `tinyint`/`smallint`/`int` each get their OWN narrow integral
    /// width (Cassandra does not promote them); `bigint`/`counter` → wide
    /// integral; everything else (incl. an unknown/`None` type) → floating.
    /// Mirrors [`crate::query::select_naming::sum_avg_result_cql_type`].
    pub(super) fn init(result_type: Option<&CqlType>) -> Self {
        let width = match result_type {
            Some(CqlType::TinyInt) => IntWidth::Byte,
            Some(CqlType::SmallInt) => IntWidth::Short,
            Some(CqlType::Int) => IntWidth::Int,
            Some(CqlType::BigInt | CqlType::Counter) => IntWidth::Long,
            _ => return NumericAcc::Floating(0.0),
        };
        NumericAcc::Integral { sum: 0, width }
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

    /// The finalized SUM value: integral domains narrow the wide `i64` sum to
    /// their own width (`Value::TinyInt`/`SmallInt`/`Integer`/`BigInt`); the
    /// floating domain emits `Value::Float`.
    pub(super) fn finalize_sum(&self) -> Value {
        match self {
            NumericAcc::Integral { sum, width } => width.narrow(*sum),
            NumericAcc::Floating(sum) => Value::Float(*sum),
        }
    }

    /// The finalized AVG value over `count` accepted inputs. Integral domains
    /// divide the wide `i64` sum by `count` (integer division, truncating
    /// toward zero, like Java) and narrow to their own width; the floating
    /// domain divides in f64.
    ///
    /// Precondition: `count > 0` — every caller (`finalize_group`,
    /// `finalize_empty_global_aggregate`) checks this first, so an empty group
    /// never reaches here; asserted rather than guarded so a future caller
    /// cannot silently reintroduce a divide-by-zero panic.
    pub(super) fn finalize_avg(&self, count: u64) -> Value {
        debug_assert!(
            count > 0,
            "finalize_avg requires at least one accepted input"
        );
        match self {
            NumericAcc::Integral { sum, width } => width.narrow(sum / count as i64),
            NumericAcc::Floating(sum) => Value::Float(sum / count as f64),
        }
    }
}

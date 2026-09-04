//! Unit tests for the pushed-down aggregate accumulators (`Count`/`Min`/`Max`/
//! `Sum`/`Avg`) and their total order over `Value` — split out of `agg.rs` to
//! keep that file under the campsite file-size target (epic #1116 source /
//! #1135 tests), following this crate's existing `<module>_tests.rs` convention
//! (`filter_tests.rs`, `bypass_tests.rs`, `batch_bytes_tests.rs`, ...).
//!
//! This is a `--lib` unit module, deliberately NOT a target under
//! `cqlite-flight/tests/`: the gate's `flight-tests` component executes this
//! crate's `--lib --bins` and NAMES the 42 integration targets it does not run,
//! so a pin placed there would execute nowhere (#3522/#3384).

use super::*;
use cqlite_core::schema::{Column, KeyColumn};
use std::collections::HashMap;

/// Schema: pk id(int), regular v(bigint).
fn bigint_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".into(),
                data_type: "int".into(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".into(),
                data_type: "bigint".into(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn row_bigint(v: i64) -> QueryRow {
    let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
    values.insert("v".into(), Value::BigInt(v));
    QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

fn sum_v_plan() -> AggPlan {
    let agg = Aggregation {
        group_by: vec![],
        aggregates: vec![AggregateSpec {
            func: AggFunc::Sum,
            column: Some("v".into()),
            output: "agg0".into(),
        }],
    };
    AggPlan::build(&agg, &bigint_schema()).expect("plan")
}

#[test]
fn integer_sum_overflow_errors_not_wraps() {
    let plan = sum_v_plan();
    let rows = vec![row_bigint(i64::MAX), row_bigint(1)];
    let err = plan
        .aggregate(&rows)
        .expect_err("sum past i64::MAX must error");
    assert!(matches!(err, AggError::SumOverflow { column } if column == "v"));
}

#[test]
fn integer_sum_without_overflow_succeeds() {
    let plan = sum_v_plan();
    let rows = vec![row_bigint(10), row_bigint(32)];
    let out = plan.aggregate(&rows).expect("no overflow");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].values.get("agg0"), Some(&Value::BigInt(42)));
}

/// Schema with a double column `d` for float-aggregate tests.
fn double_schema() -> TableSchema {
    let mut schema = bigint_schema();
    schema.columns.push(Column {
        name: "d".into(),
        data_type: "double".into(),
        nullable: true,
        default: None,
        is_static: false,
    });
    schema
}

fn row_double(d: f64) -> QueryRow {
    let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
    values.insert("d".into(), Value::Float(d));
    QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

fn min_max_d_plan() -> AggPlan {
    let agg = Aggregation {
        group_by: vec![],
        aggregates: vec![
            AggregateSpec {
                func: AggFunc::Min,
                column: Some("d".into()),
                output: "agg_min".into(),
            },
            AggregateSpec {
                func: AggFunc::Max,
                column: Some("d".into()),
                output: "agg_max".into(),
            },
        ],
    };
    AggPlan::build(&agg, &double_schema()).expect("plan")
}

/// #896: float min/max now pushes; `NaN` orders greatest, so `max` returns
/// `NaN` and `min` ignores it — independent of input row order.
#[test]
fn float_min_max_nan_orders_greatest_and_is_order_independent() {
    let nan_first = vec![row_double(f64::NAN), row_double(1.0), row_double(3.0)];
    let nan_last = vec![row_double(1.0), row_double(3.0), row_double(f64::NAN)];
    for rows in [nan_first, nan_last] {
        let out = min_max_d_plan()
            .aggregate(&rows)
            .expect("float min/max pushes");
        assert_eq!(out.len(), 1);
        // min ignores NaN -> 1.0
        assert_eq!(out[0].values.get("agg_min"), Some(&Value::Float(1.0)));
        // max -> NaN (NaN is the largest value)
        match out[0].values.get("agg_max") {
            Some(Value::Float(v)) => assert!(v.is_nan(), "max over a NaN input must be NaN"),
            other => panic!("expected NaN max, got {other:?}"),
        }
    }
}

/// #896: when every input is `NaN`, both `min` and `max` are `NaN`.
#[test]
fn float_min_max_all_nan_is_nan() {
    let rows = vec![row_double(f64::NAN), row_double(f64::NAN)];
    let out = min_max_d_plan().aggregate(&rows).expect("plan");
    for col in ["agg_min", "agg_max"] {
        match out[0].values.get(col) {
            Some(Value::Float(v)) => assert!(v.is_nan(), "{col} over all-NaN must be NaN"),
            other => panic!("expected NaN for {col}, got {other:?}"),
        }
    }
}

/// #902: `SumDouble` (avg numerator) widens integers to f64 and never
/// overflows, where a checked-i64 `Sum` over the same data errors.
#[test]
fn sum_double_does_not_overflow_on_large_integer_total() {
    let agg = Aggregation {
        group_by: vec![],
        aggregates: vec![AggregateSpec {
            func: AggFunc::SumDouble,
            column: Some("v".into()),
            output: "agg0".into(),
        }],
    };
    let plan = AggPlan::build(&agg, &bigint_schema()).expect("plan");
    // A total exceeding i64 (i64::MAX + 1 + 1) would error under checked Sum.
    let rows = vec![row_bigint(i64::MAX), row_bigint(1), row_bigint(1)];
    let out = plan.aggregate(&rows).expect("SumDouble must not overflow");
    assert_eq!(out.len(), 1);
    let expected = i64::MAX as f64 + 1.0 + 1.0;
    assert_eq!(out[0].values.get("agg0"), Some(&Value::Float(expected)));
}

// ===========================================================================
// Issue #3935 — `time` Min/Max must order by BYTE_ORDER, `timestamp` by SIGNED.
//
// `compare_values` is the `Min`/`Max` accumulator's total order for pushed-down
// aggregates on the Arrow Flight / Trino surface, so its `Value::Time` arm is a
// `time` ordering site like every other one #3935 swept. It shared an arm with
// `BigInt`/`Counter`/`Timestamp` (signed `i64::cmp`) and was corrected to
// unsigned big-endian byte comparison.
//
// Format authority (pinned `cassandra-5.0.8`, never a CQLite file:line — #3041):
//
//   * `src/java/org/apache/cassandra/db/marshal/TimeType.java:48` —
//     `private TimeType() {super(ComparisonType.BYTE_ORDER);}`, and
//     `ComparisonType.BYTE_ORDER` is `ByteBufferUtil.compareUnsigned` over the
//     serialized 8-byte big-endian nanos-since-midnight long.
//   * `src/java/org/apache/cassandra/db/marshal/TimestampType.java:56` —
//     `super(ComparisonType.CUSTOM)`, whose `compareCustom` is
//     `LongType.compareLongs(...)`, i.e. SIGNED. The asymmetry is load-bearing:
//     `time` and `timestamp` are both 8-byte longs and do NOT share a
//     comparator, so `timestamp_min_max_keeps_signed_order` exists to stop a
//     later change re-unifying the arms.
//
// The trino-declared semantics in `agg.rs` are scoped to float/double `NaN`
// ordering (`cmp_f64_trino`, issue #896) and `bigint` sum overflow; nothing
// there declares Trino semantics for the temporal arms, and the rows being
// aggregated come from Cassandra SSTables — so `TimeType`'s rule governs.
//
// Range validation was considered and REFUSED (#3935, lead ruling): Cassandra's
// binary `validate` accepts an out-of-range `time`, so a range check would make
// CQLite reject data Cassandra created. The citations are written out ONCE, in
// `cqlite-core`'s `types::comparator::custom::compare_time`
// (`# CANONICAL STATEMENT`), and are deliberately not restated here.
// ===========================================================================

/// Schema with a `time` column `t` and a `timestamp` column `ts`.
fn temporal_schema() -> TableSchema {
    let mut schema = bigint_schema();
    schema.columns.push(Column {
        name: "t".into(),
        data_type: "time".into(),
        nullable: true,
        default: None,
        is_static: false,
    });
    schema.columns.push(Column {
        name: "ts".into(),
        data_type: "timestamp".into(),
        nullable: true,
        default: None,
        is_static: false,
    });
    schema
}

fn row_one(column: &str, value: Value) -> QueryRow {
    let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
    values.insert(column.into(), value);
    QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

fn extreme_plan(func: AggFunc, column: &str) -> AggPlan {
    let agg = Aggregation {
        group_by: vec![],
        aggregates: vec![AggregateSpec {
            func,
            column: Some(column.to_string()),
            output: "agg0".into(),
        }],
    };
    AggPlan::build(&agg, &temporal_schema()).expect("plan")
}

/// The comparator itself: an out-of-range NEGATIVE `time` sorts ABOVE every
/// in-range value, because its sign bit puts `0xFF` in the leading serialized
/// byte and `0xFF > 0x00` unsigned. Signed `i64::cmp` — the pre-#3935 behaviour
/// of this arm — sorts it FIRST.
///
/// Carries a NEGATIVE CONTROL: the same input re-sorted with the old signed
/// closure must produce a DIFFERENT sequence, so a green result provably
/// discriminates the two implementations rather than being satisfiable by any
/// total order.
#[test]
fn time_min_max_orders_by_byte_order_not_signed() {
    const MAX_VALID_NANOS: i64 = 86_399_999_999_999; // DAYS.toNanos(1) - 1

    // Anchor the rule on the serialized bytes it is stated over, not on a
    // remembered claim.
    assert_eq!((-1_i64).to_be_bytes()[0], 0xFF);
    assert_eq!(i64::MIN.to_be_bytes()[0], 0x80);
    assert_eq!(0_i64.to_be_bytes()[0], 0x00);
    assert_eq!(MAX_VALID_NANOS.to_be_bytes()[0], 0x00);

    let inputs = [0_i64, MAX_VALID_NANOS, i64::MIN, -1];

    let mut byte_order: Vec<i64> = inputs.to_vec();
    byte_order.sort_by(|a, b| {
        compare_values(&Value::Time(*a), &Value::Time(*b)).expect("time is comparable")
    });
    assert_eq!(
        byte_order,
        vec![
            // 0x00.. — the in-range values, ascending.
            0,
            MAX_VALID_NANOS,
            // 0x80.. — i64::MIN, the SMALLEST signed value, sorts here.
            i64::MIN,
            // 0xFF.. — the LARGEST unsigned leading byte sorts last.
            -1,
        ],
        "TimeType is ComparisonType.BYTE_ORDER: an out-of-range negative sorts \
         above every non-negative, and i64::MIN (0x80..) below -1 (0xFF..)"
    );

    // NEGATIVE CONTROL — the pre-#3935 signed order is a DIFFERENT sequence.
    let mut signed: Vec<i64> = inputs.to_vec();
    signed.sort();
    assert_ne!(
        signed, byte_order,
        "signed i64::cmp must NOT reproduce the BYTE_ORDER sequence, else this \
         case cannot distinguish the two implementations"
    );

    // WIRING EVIDENCE — the same rule through the public pushed-down aggregate
    // surface, which is what a Flight/Trino `min(t)`/`max(t)` actually executes.
    let rows: Vec<QueryRow> = inputs
        .iter()
        .map(|&n| row_one("t", Value::Time(n)))
        .collect();

    let max = extreme_plan(AggFunc::Max, "t")
        .aggregate(&rows)
        .expect("max over time");
    assert_eq!(max.len(), 1);
    assert_eq!(
        max[0].values.get("agg0"),
        Some(&Value::Time(-1)),
        "max(time) must be the 0xFF.. out-of-range negative under BYTE_ORDER \
         (pre-#3935 signed order would have answered {MAX_VALID_NANOS})"
    );

    let min = extreme_plan(AggFunc::Min, "t")
        .aggregate(&rows)
        .expect("min over time");
    assert_eq!(min.len(), 1);
    assert_eq!(
        min[0].values.get("agg0"),
        Some(&Value::Time(0)),
        "min(time) must be the smallest 0x00.. value under BYTE_ORDER \
         (pre-#3935 signed order would have answered i64::MIN)"
    );
}

/// The asymmetry: `timestamp` (TimestampType, ComparisonType.CUSTOM ->
/// `LongType.compareLongs`) stays SIGNED, so a negative millis-since-epoch —
/// a pre-1970 instant, entirely legal — must remain the MINIMUM. This is the
/// regression pin against "unifying" the `Time` arm back with `Timestamp`.
#[test]
fn timestamp_min_max_keeps_signed_order() {
    let inputs = [0_i64, 1_700_000_000_000, -1, i64::MIN];

    let mut ordered: Vec<i64> = inputs.to_vec();
    ordered.sort_by(|a, b| {
        compare_values(&Value::Timestamp(*a), &Value::Timestamp(*b))
            .expect("timestamp is comparable")
    });
    assert_eq!(
        ordered,
        vec![i64::MIN, -1, 0, 1_700_000_000_000],
        "TimestampType.compareCustom delegates to LongType.compareLongs: SIGNED"
    );

    // The two 8-byte-long types must NOT agree on the same input.
    let mut as_time: Vec<i64> = inputs.to_vec();
    as_time.sort_by(|a, b| {
        compare_values(&Value::Time(*a), &Value::Time(*b)).expect("time is comparable")
    });
    assert_ne!(
        as_time, ordered,
        "time (BYTE_ORDER) and timestamp (signed) must order this input \
         DIFFERENTLY — if they agree, one of the two arms has been unified away"
    );

    // WIRING EVIDENCE through the public surface.
    let rows: Vec<QueryRow> = inputs
        .iter()
        .map(|&n| row_one("ts", Value::Timestamp(n)))
        .collect();
    let min = extreme_plan(AggFunc::Min, "ts")
        .aggregate(&rows)
        .expect("min over timestamp");
    assert_eq!(
        min[0].values.get("agg0"),
        Some(&Value::Timestamp(i64::MIN)),
        "min(timestamp) must be the most negative value — signed order"
    );
    let max = extreme_plan(AggFunc::Max, "ts")
        .aggregate(&rows)
        .expect("max over timestamp");
    assert_eq!(
        max[0].values.get("agg0"),
        Some(&Value::Timestamp(1_700_000_000_000)),
        "max(timestamp) must be the largest positive value — signed order"
    );
}

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

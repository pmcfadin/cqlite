//! GROUP BY / aggregate accumulation for the SELECT executor.
//!
//! Holds the per-query aggregation state and the pure step functions
//! (`build_group_key`, `find_or_init_group`, `update_aggregate`,
//! `finalize_group`) that the executor's `execute_aggregation` drives row by
//! row.

use super::super::select_ast::AggregateType;
use super::super::select_optimizer::{AggregateComputation, AggregationPlan};
use super::value_ops::compare_values_ordering;
use crate::{
    query::result::QueryRow,
    types::{RowKey, Value},
};
use std::collections::HashMap;

/// Aggregation state for GROUP BY operations
#[derive(Debug)]
pub(super) struct AggregationState {
    /// Vector for grouping since Value doesn't implement Hash
    pub(super) groups: Vec<(Vec<Value>, Vec<AggregateValue>)>,
    /// Memory usage tracking
    pub(super) memory_usage_bytes: usize,
    /// Maximum memory limit
    pub(super) memory_limit_bytes: usize,
}

/// Aggregate value accumulator
#[derive(Debug, Clone)]
pub(super) enum AggregateValue {
    Count(u64),
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Value),
    Max(Value),
}

/// Build the GROUP BY key for `row`. With no GROUP BY, all rows hash into a
/// single `[Null]` bucket (global aggregation).
pub(super) fn build_group_key(row: &QueryRow, group_by_columns: &[String]) -> Vec<Value> {
    if group_by_columns.is_empty() {
        return vec![Value::Null];
    }
    group_by_columns
        .iter()
        .map(|col| row.values.get(col.as_str()).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Locate the group matching `key` in `groups`, or push a fresh entry with
/// initial aggregator state. Returns the index into `groups`.
///
/// `Value` doesn't implement `Hash`, so groups live in a `Vec` and lookup is
/// linear. This is unchanged from the legacy implementation; switching to a
/// hash map would change result-row ordering for callers that rely on
/// insertion order.
pub(super) fn find_or_init_group(
    groups: &mut Vec<(Vec<Value>, Vec<AggregateValue>)>,
    key: Vec<Value>,
    aggregates: &[AggregateComputation],
) -> usize {
    if let Some(idx) = groups.iter().position(|(k, _)| k == &key) {
        return idx;
    }
    let initial: Vec<_> = aggregates
        .iter()
        .map(|c| match c.function {
            AggregateType::Count => AggregateValue::Count(0),
            AggregateType::Sum => AggregateValue::Sum(0.0),
            AggregateType::Avg => AggregateValue::Avg { sum: 0.0, count: 0 },
            AggregateType::Min => AggregateValue::Min(Value::Null),
            AggregateType::Max => AggregateValue::Max(Value::Null),
        })
        .collect();
    groups.push((key, initial));
    groups.len() - 1
}

/// Apply one row's contribution to a single aggregate accumulator.
///
/// COUNT(*) always increments; COUNT(col) only increments on non-null. SUM and
/// AVG ignore non-numeric values. MIN/MAX clone the value only when it
/// becomes the new extremum, sparing per-row clones in the common case.
pub(super) fn update_aggregate(
    state: &mut AggregateValue,
    agg_comp: &AggregateComputation,
    row: &QueryRow,
) {
    let is_star = agg_comp.column == "*";
    // Look up the column once; for COUNT(*) we don't need it.
    let value: Option<&Value> = if is_star {
        None
    } else {
        row.values.get(agg_comp.column.as_str())
    };
    let is_null = !is_star && value.is_none_or(Value::is_null);

    match state {
        AggregateValue::Count(count) => {
            if is_star || !is_null {
                *count += 1;
            }
        }
        AggregateValue::Sum(sum) => {
            if let Some(v) = value.and_then(Value::as_f64) {
                *sum += v;
            }
        }
        AggregateValue::Avg { sum, count } => {
            if let Some(v) = value.and_then(Value::as_f64) {
                *sum += v;
                *count += 1;
            }
        }
        AggregateValue::Min(min_val) => {
            if let Some(v) = value {
                if !v.is_null()
                    && (min_val.is_null() || compare_values_ordering(v, min_val).is_lt())
                {
                    *min_val = v.clone();
                }
            }
        }
        AggregateValue::Max(max_val) => {
            if let Some(v) = value {
                if !v.is_null()
                    && (max_val.is_null() || compare_values_ordering(v, max_val).is_gt())
                {
                    *max_val = v.clone();
                }
            }
        }
    }
}

/// Materialize a single aggregation group into a `QueryRow`.
pub(super) fn finalize_group(
    group_key: Vec<Value>,
    group_aggregates: Vec<AggregateValue>,
    agg_plan: &AggregationPlan,
) -> QueryRow {
    let mut row_values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();

    for (i, col) in agg_plan.group_by_columns.iter().enumerate() {
        if let Some(v) = group_key.get(i) {
            row_values.insert(col.as_str().into(), v.clone());
        }
    }

    for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
        let result_value = match &group_aggregates[i] {
            AggregateValue::Count(count) => Value::BigInt(*count as i64),
            AggregateValue::Sum(sum) => Value::Float(*sum),
            AggregateValue::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float(sum / (*count as f64))
                } else {
                    Value::Null
                }
            }
            AggregateValue::Min(val) | AggregateValue::Max(val) => val.clone(),
        };
        row_values.insert(agg_comp.alias.as_str().into(), result_value);
    }

    QueryRow {
        values: row_values,
        key: RowKey::new(vec![]),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

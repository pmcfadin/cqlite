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
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// Test-only counter for GROUP-key exact (`==`) comparisons performed while
// locating a row's group (issue #1587, E5). The hash index makes this
// `O(rows)` (amortized) rather than the old linear scan's `O(rows × groups)`.
// Same thread-local rationale as the executor's other work counters.
#[cfg(test)]
thread_local! {
    pub(crate) static GROUP_KEY_COMPARISONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// Aggregation state for GROUP BY operations
#[derive(Debug)]
pub(super) struct AggregationState {
    /// Groups in first-seen (insertion) order. Result-row ordering is defined by
    /// this `Vec`, so it is preserved exactly; the hash index below is only a
    /// lookup accelerator.
    pub(super) groups: Vec<(Vec<Value>, Vec<AggregateValue>)>,
    /// Issue #1587 (E5): hash index over the group key → candidate `groups`
    /// indices sharing that hash. `Value` does not implement `Hash`/`Eq`
    /// (floats), so we hash a normalization that is *consistent* with `Vec<Value>`
    /// equality (equal keys hash equal; `-0.0`/`+0.0` normalized) and confirm the
    /// match with an exact `==` against each candidate. Distinct-but-colliding
    /// keys (and every NaN key, which is never `==` itself) simply fall through
    /// the exact check, so grouping semantics are byte-identical to the prior
    /// linear scan.
    ///
    /// Issue #1590 (E8): `FxHashMap` rather than the default `HashMap`. The key
    /// is already a well-mixed `u64` group-key hash, so SipHashing it again is
    /// wasted work; Fx hashes the integer key directly. Collisions remain safe —
    /// the `Vec<usize>` bucket + exact `==` confirmation above is unchanged.
    pub(super) group_index: FxHashMap<u64, Vec<usize>>,
    /// Memory usage tracking
    pub(super) memory_usage_bytes: usize,
    /// Maximum memory limit
    pub(super) memory_limit_bytes: usize,
}

/// Hash a group key consistently with `Vec<Value>` equality: if two keys are
/// `==`, they hash identically. Only floats need care (`-0.0 == +0.0`), which
/// are normalized; NaN keys are never `==` themselves, so their hash is
/// irrelevant to correctness (the exact `==` check separates them). Exotic /
/// nested variants that are not cheap to hash structurally fall back to a
/// discriminant-only contribution — still consistent (equal values agree on
/// discriminant), merely less selective, and only for key types that never
/// appear in practice.
fn hash_group_key(key: &[Value]) -> u64 {
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
        Value::Boolean(b) => b.hash(h),
        Value::Integer(x) => x.hash(h),
        Value::BigInt(x) | Value::Counter(x) | Value::Timestamp(x) | Value::Time(x) => x.hash(h),
        Value::Date(x) => x.hash(h),
        Value::TinyInt(x) => x.hash(h),
        Value::SmallInt(x) => x.hash(h),
        // `-0.0 == +0.0` must hash equal; NaN's hash is irrelevant (never `==`).
        Value::Float(f) => (if *f == 0.0 { 0.0f64 } else { *f }).to_bits().hash(h),
        Value::Float32(f) => (if *f == 0.0 { 0.0f32 } else { *f }).to_bits().hash(h),
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

/// Aggregate value accumulator
#[derive(Debug, Clone)]
pub(super) enum AggregateValue {
    Count(u64),
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Value),
    Max(Value),
}

/// Build the initial per-aggregate accumulators for one group. Shared by
/// [`find_or_init_group`] (the buffered GROUP BY path) and the streaming
/// GROUP-BY-free fold (issue #1578, D2) so the initial state — and thus the
/// per-row update + finalize semantics — are defined in exactly one place.
pub(super) fn init_aggregate_accumulators(
    aggregates: &[AggregateComputation],
) -> Vec<AggregateValue> {
    aggregates
        .iter()
        .map(|c| match c.function {
            AggregateType::Count => AggregateValue::Count(0),
            AggregateType::Sum => AggregateValue::Sum(0.0),
            AggregateType::Avg => AggregateValue::Avg { sum: 0.0, count: 0 },
            AggregateType::Min => AggregateValue::Min(Value::Null),
            AggregateType::Max => AggregateValue::Max(Value::Null),
        })
        .collect()
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

/// Locate the group matching `key`, or push a fresh entry with initial
/// aggregator state. Returns the index into `state.groups`.
///
/// Issue #1587 (E5): a hash index (`state.group_index`) makes this `O(1)`
/// amortized instead of the old `O(groups)` linear scan (which made a full
/// aggregation `O(rows × groups)`). Result-row ordering is still defined solely
/// by insertion order into `state.groups`; the index only accelerates lookup,
/// and the exact `==` confirmation preserves grouping semantics byte-for-byte
/// (colliding-but-unequal keys, and NaN keys, create distinct groups exactly as
/// the linear scan did).
pub(super) fn find_or_init_group(
    state: &mut AggregationState,
    key: Vec<Value>,
    aggregates: &[AggregateComputation],
) -> usize {
    let hash = hash_group_key(&key);
    if let Some(candidates) = state.group_index.get(&hash) {
        for &idx in candidates {
            #[cfg(test)]
            GROUP_KEY_COMPARISONS.with(|c| c.set(c.get() + 1));
            if state.groups[idx].0 == key {
                return idx;
            }
        }
    }
    let initial = init_aggregate_accumulators(aggregates);
    let new_index = state.groups.len();
    state.groups.push((key, initial));
    state.group_index.entry(hash).or_default().push(new_index);
    new_index
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
    // Issue #1584: one sized allocation per group — the row holds exactly the
    // GROUP BY columns plus the aggregate results, so pre-size to that count and
    // avoid rehash growth.
    let mut row_values: HashMap<std::sync::Arc<str>, Value> =
        HashMap::with_capacity(agg_plan.group_by_columns.len() + agg_plan.aggregates.len());

    // Issue #1763: emit each grouped dimension under its SELECT OUTPUT name
    // (alias when present, else the column name) — the SAME `select_naming` source
    // that names the result metadata column — so the grouped dimension's row value
    // key can never diverge from its metadata name. The group KEY was read by the
    // raw stored column (`build_group_key`); only the emitted key differs.
    for (i, out_name) in agg_plan.group_by_output_names.iter().enumerate() {
        if let Some(v) = group_key.get(i) {
            row_values.insert(out_name.as_str().into(), v.clone());
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

/// Issue #2069: materialize the single result row of a GLOBAL aggregate (no
/// GROUP BY) over ZERO input rows.
///
/// Cassandra's CQL semantics say a bare aggregate SELECT with no GROUP BY is a
/// "global aggregate" and ALWAYS produces exactly one row, even over an empty
/// table: `COUNT(*)`/`COUNT(col)` = `0`, and `MIN`/`MAX`/`SUM`/`AVG` = `NULL`
/// (notably SUM of empty input is NULL, not `0`). This is DISTINCT from a
/// GROUP BY query, which yields zero rows when there are no groups — the callers
/// only reach this helper on the GROUP-BY-free path.
///
/// We cannot reuse [`init_aggregate_accumulators`] + [`finalize_group`] for this
/// because a fresh `AggregateValue::Sum(0.0)` finalizes to `Float(0.0)`, whereas
/// the empty-input answer must be `NULL`. This builds the row directly instead.
pub(super) fn finalize_empty_global_aggregate(agg_plan: &AggregationPlan) -> QueryRow {
    let mut row_values: HashMap<std::sync::Arc<str>, Value> =
        HashMap::with_capacity(agg_plan.aggregates.len());

    for agg_comp in &agg_plan.aggregates {
        let result_value = match agg_comp.function {
            // COUNT of empty input is 0 (BigInt, matching the non-empty finalize).
            AggregateType::Count => Value::BigInt(0),
            // SUM/AVG/MIN/MAX of empty input are NULL in Cassandra.
            AggregateType::Sum | AggregateType::Avg | AggregateType::Min | AggregateType::Max => {
                Value::Null
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::select_ast::AggregateType;

    fn count_star_plan() -> AggregationPlan {
        AggregationPlan {
            group_by_columns: vec!["g".to_string()],
            group_by_output_names: vec!["g".to_string()],
            aggregates: vec![AggregateComputation {
                function: AggregateType::Count,
                column: "*".to_string(),
                alias: "c".to_string(),
                distinct: false,
            }],
        }
    }

    fn new_state() -> AggregationState {
        AggregationState {
            groups: Vec::new(),
            group_index: FxHashMap::default(),
            memory_usage_bytes: 0,
            memory_limit_bytes: usize::MAX,
        }
    }

    /// Issue #1587 (E5): locating a row's group is `O(1)` amortized via the hash
    /// index, so total group-key comparisons across a high-cardinality GROUP BY
    /// stay `O(rows)` — NOT the old linear scan's `O(rows × groups)`. Assert a
    /// tight linear bound the quadratic scan could never meet.
    #[test]
    fn find_or_init_group_is_subquadratic() {
        let plan = count_star_plan();
        let groups_count = 200usize;
        let repeats = 3usize; // each group is hit multiple times (exercises "found")
        let rows = groups_count * repeats;

        let mut state = new_state();
        GROUP_KEY_COMPARISONS.with(|c| c.set(0));

        for r in 0..repeats {
            for g in 0..groups_count {
                let key = vec![Value::Integer(g as i32)];
                let idx = find_or_init_group(&mut state, key, &plan.aggregates);
                // Groups are created on first pass in ascending order.
                assert_eq!(idx, g, "row (repeat {r}, group {g}) maps to a stable index");
            }
        }

        let comparisons = GROUP_KEY_COMPARISONS.with(|c| c.get());
        assert_eq!(
            state.groups.len(),
            groups_count,
            "exactly one group per key"
        );
        // Hash index: ~1 comparison per row on the "found" path, 0 on "new".
        // Linear scan would be ~rows × groups / 2 = {} here.
        assert!(
            comparisons <= rows * 2,
            "issue #1587: group lookup must be sub-quadratic — {comparisons} comparisons \
             for {rows} rows / {groups_count} groups (linear scan would be ~{})",
            rows * groups_count / 2
        );
    }

    /// Issue #2069: the empty global-aggregate synthesizer emits COUNT = 0
    /// (BigInt) and NULL for every other aggregate — SUM of empty is NULL, NOT
    /// the `Float(0.0)` a fresh `Sum(0.0)` accumulator would finalize to.
    #[test]
    fn finalize_empty_global_aggregate_count_zero_others_null() {
        let agg = |function, alias: &str| AggregateComputation {
            function,
            column: "x".to_string(),
            alias: alias.to_string(),
            distinct: false,
        };
        let plan = AggregationPlan {
            group_by_columns: Vec::new(),
            group_by_output_names: Vec::new(),
            aggregates: vec![
                agg(AggregateType::Count, "c"),
                agg(AggregateType::Sum, "s"),
                agg(AggregateType::Avg, "a"),
                agg(AggregateType::Min, "mn"),
                agg(AggregateType::Max, "mx"),
            ],
        };

        let row = finalize_empty_global_aggregate(&plan);

        assert_eq!(
            row.values.get("c"),
            Some(&Value::BigInt(0)),
            "COUNT of empty input is 0"
        );
        for (alias, kind) in [("s", "SUM"), ("a", "AVG"), ("mn", "MIN"), ("mx", "MAX")] {
            assert_eq!(
                row.values.get(alias),
                Some(&Value::Null),
                "{kind} of empty input is NULL (not 0)"
            );
        }
    }

    /// The hash index must never merge distinct keys: `-0.0`/`+0.0` group
    /// together (they are `==`), NaN never groups with itself, and different
    /// scalars stay separate — byte-identical to the linear-scan semantics.
    #[test]
    fn find_or_init_group_preserves_equality_semantics() {
        let plan = count_star_plan();
        let mut state = new_state();

        // -0.0 and +0.0 are `==` → same group.
        let i0 = find_or_init_group(&mut state, vec![Value::Float(0.0)], &plan.aggregates);
        let i1 = find_or_init_group(&mut state, vec![Value::Float(-0.0)], &plan.aggregates);
        assert_eq!(i0, i1, "-0.0 and +0.0 must land in the same group");

        // Two NaN keys are never `==` → two distinct groups.
        let n0 = find_or_init_group(&mut state, vec![Value::Float(f64::NAN)], &plan.aggregates);
        let n1 = find_or_init_group(&mut state, vec![Value::Float(f64::NAN)], &plan.aggregates);
        assert_ne!(n0, n1, "each NaN key forms its own group (NaN != NaN)");

        // Same-value different-variant keys stay separate.
        let a = find_or_init_group(&mut state, vec![Value::Integer(1)], &plan.aggregates);
        let b = find_or_init_group(&mut state, vec![Value::BigInt(1)], &plan.aggregates);
        assert_ne!(a, b, "Integer(1) and BigInt(1) are distinct groups");
    }
}

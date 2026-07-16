//! GROUP BY / aggregate accumulation for the SELECT executor.
//!
//! Holds the per-query aggregation state and the pure step functions
//! (`build_group_key`, `find_or_init_group`, `update_aggregate`,
//! `finalize_group`) that the executor's `execute_aggregation` drives row by
//! row.

mod group_key_cmp;

use super::super::select_ast::AggregateType;
use super::super::select_optimizer::{AggregateComputation, AggregationPlan};
use super::numeric_acc::NumericAcc;
use super::value_ops::compare_values_ordering;
use crate::schema::CqlType;
use crate::{
    query::result::QueryRow,
    types::{RowKey, Value},
};
use group_key_cmp::{group_key_eq, hash_group_key};
use rustc_hash::FxHashMap;
use std::collections::HashMap;

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
    /// (floats), so we hash a normalization *consistent* with the group-key
    /// equality used to confirm a match ([`group_key_eq`]), and confirm the match
    /// with that predicate against each candidate. Issue #2074: `float`/`double`
    /// group-key equality follows Cassandra's total-order comparator (`float_cmp`),
    /// so the hash canonicalizes NaN to one bit pattern (all NaN keys hash-collide
    /// into ONE group) and keeps signed-zero bits DISTINCT (`-0.0`/`+0.0` hash into
    /// two groups) — exactly consistent with [`group_key_eq`].
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

/// Aggregate value accumulator
#[derive(Debug, Clone)]
pub(super) enum AggregateValue {
    Count(u64),
    Sum(NumericAcc),
    Avg { acc: NumericAcc, count: u64 },
    Min(Value),
    Max(Value),
}

/// Build the initial per-aggregate accumulators for one group. Shared by
/// [`find_or_init_group`] (the buffered GROUP BY path) and the streaming
/// GROUP-BY-free fold (issue #1578, D2) so the initial state — and thus the
/// per-row update + finalize semantics — are defined in exactly one place.
///
/// Issue #2202: `result_types[i]` is the aggregate's resolved RESULT CQL type
/// (from `select_naming::aggregate_result_cql_type`, the SAME source the result
/// metadata uses), parallel to `aggregates`. It fixes each SUM/AVG accumulator's
/// numeric domain (integral vs floating) up front, so the emitted value variant
/// matches the metadata type even for an all-null group that folds no input.
pub(super) fn init_aggregate_accumulators(
    aggregates: &[AggregateComputation],
    result_types: &[Option<CqlType>],
) -> Vec<AggregateValue> {
    aggregates
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let result_type = result_types.get(i).and_then(|t| t.as_ref());
            match c.function {
                AggregateType::Count => AggregateValue::Count(0),
                AggregateType::Sum => AggregateValue::Sum(NumericAcc::init(result_type)),
                AggregateType::Avg => AggregateValue::Avg {
                    acc: NumericAcc::init(result_type),
                    count: 0,
                },
                AggregateType::Min => AggregateValue::Min(Value::Null),
                AggregateType::Max => AggregateValue::Max(Value::Null),
            }
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
/// and the [`group_key_eq`] confirmation defines the grouping semantics
/// (colliding-but-unequal keys create distinct groups). Issue #2074: that
/// confirmation is [`group_key_eq`], NOT derived `Vec<Value>` `==`, so
/// `float`/`double` grouping follows Cassandra's comparator (all NaN → ONE group;
/// `-0.0`/`+0.0` → DISTINCT groups).
pub(super) fn find_or_init_group(
    state: &mut AggregationState,
    key: Vec<Value>,
    aggregates: &[AggregateComputation],
    result_types: &[Option<CqlType>],
) -> usize {
    let hash = hash_group_key(&key);
    if let Some(candidates) = state.group_index.get(&hash) {
        for &idx in candidates {
            #[cfg(test)]
            GROUP_KEY_COMPARISONS.with(|c| c.set(c.get() + 1));
            if group_key_eq(&state.groups[idx].0, &key) {
                return idx;
            }
        }
    }
    let initial = init_aggregate_accumulators(aggregates, result_types);
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
        AggregateValue::Sum(acc) => {
            if let Some(v) = value {
                if !v.is_null() {
                    acc.add(v);
                }
            }
        }
        AggregateValue::Avg { acc, count } => {
            if let Some(v) = value {
                // Count only inputs the accumulator's numeric domain accepts, so
                // AVG's divisor matches SUM's contributor set — a `0` still
                // counts (issue #2202).
                if !v.is_null() && acc.add(v) {
                    *count += 1;
                }
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
            // Issue #2202: emit the value variant matching the accumulator's
            // numeric domain (int/bigint/double), which was fixed from the
            // resolved result type, so it agrees with the result metadata type.
            AggregateValue::Sum(acc) => acc.finalize_sum(),
            AggregateValue::Avg { acc, count } => {
                if *count > 0 {
                    acc.finalize_avg(*count)
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
/// because a fresh SUM accumulator finalizes to a typed zero (`0`/`0.0`), whereas
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
                let idx = find_or_init_group(&mut state, key, &plan.aggregates, &[]);
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

    /// Issue #2074: GROUP BY on a float/double key follows Cassandra's total-order
    /// comparator (`float_cmp`), NOT IEEE `==`. `-0.0` and `+0.0` are DISTINCT
    /// groups (Cassandra orders them apart); ALL NaN bit-patterns form ONE group
    /// (Java `doubleToLongBits` canonical); while cross-variant keys (`Integer(1)`
    /// vs `BigInt(1)`) stay distinct. Both the hash and the `group_key_eq`
    /// confirmation must agree, so the assertions below simultaneously prove
    /// hash/eq consistency.
    #[test]
    fn find_or_init_group_uses_cassandra_float_comparator() {
        let plan = count_star_plan();
        let mut state = new_state();
        let mut group =
            |f: f64| find_or_init_group(&mut state, vec![Value::Float(f)], &plan.aggregates, &[]);

        // -0.0 and +0.0 are ordered apart → DISTINCT groups.
        let i0 = group(0.0);
        let i1 = group(-0.0);
        assert_ne!(i0, i1, "-0.0 and +0.0 must land in DISTINCT groups (#2074)");

        // Multiple NaN keys — including DIFFERENT NaN bit patterns — all collapse
        // into ONE group (canonicalized, matching Cassandra).
        let other_nan = f64::from_bits(0xFFF8_0000_0000_0001);
        assert!(other_nan.is_nan());
        let n0 = group(f64::NAN);
        let n1 = group(f64::NAN);
        let n2 = group(other_nan);
        assert_eq!(n0, n1, "all NaN keys share ONE group (#2074)");
        assert_eq!(
            n0, n2,
            "a DIFFERENT NaN bit pattern also joins the single NaN group (#2074)"
        );

        // The same holds for f32 (`float`): -0.0/+0.0 distinct, NaN patterns merged.
        let f32key = |f: f32| vec![Value::Float32(f)];
        let f0 = find_or_init_group(&mut state, f32key(0.0), &plan.aggregates, &[]);
        let f1 = find_or_init_group(&mut state, f32key(-0.0), &plan.aggregates, &[]);
        assert_ne!(f0, f1, "f32 -0.0 and +0.0 must be DISTINCT groups (#2074)");
        let other_nan32 = f32::from_bits(0xFFC0_0001);
        assert!(other_nan32.is_nan());
        let g0 = find_or_init_group(&mut state, f32key(f32::NAN), &plan.aggregates, &[]);
        let g1 = find_or_init_group(&mut state, f32key(other_nan32), &plan.aggregates, &[]);
        assert_eq!(g0, g1, "f32 NaN bit patterns share ONE group (#2074)");

        // Same-value different-variant keys stay separate (no numeric coercion).
        let a = find_or_init_group(&mut state, vec![Value::Integer(1)], &plan.aggregates, &[]);
        let b = find_or_init_group(&mut state, vec![Value::BigInt(1)], &plan.aggregates, &[]);
        assert_ne!(a, b, "Integer(1) and BigInt(1) are distinct groups");
    }

    /// Issue #2074 (nested scope): the Cassandra float comparator applies at
    /// EVERY nesting depth, not just top-level scalar group columns — a float
    /// buried inside a `Tuple`/`List` group key must hash AND compare consistent
    /// with the same total order, or hash/eq would disagree (a correctness bug).
    #[test]
    fn find_or_init_group_nested_float_uses_cassandra_comparator() {
        let plan = count_star_plan();
        let mut state = new_state();
        let other_nan = f64::from_bits(0xFFF8_0000_0000_0001);
        assert!(other_nan.is_nan());
        let mut group = |k: Vec<Value>| find_or_init_group(&mut state, k, &plan.aggregates, &[]);

        // Nested -0.0 vs +0.0 inside a Tuple → DISTINCT groups (not merged by IEEE).
        let t0 = group(vec![Value::Tuple(vec![Value::Float(0.0)])]);
        let t1 = group(vec![Value::Tuple(vec![Value::Float(-0.0)])]);
        assert_ne!(t0, t1, "nested Tuple -0.0/+0.0 must be DISTINCT (#2074)");

        // Two DIFFERENT NaN bit patterns nested inside a Tuple → SAME group.
        let n0 = group(vec![Value::Tuple(vec![Value::Float(f64::NAN)])]);
        let n1 = group(vec![Value::Tuple(vec![Value::Float(other_nan)])]);
        assert_eq!(
            n0, n1,
            "nested Tuple differing-NaN must be ONE group (#2074)"
        );

        // Same nested behavior inside a List.
        let l0 = group(vec![Value::List(vec![Value::Float(0.0)])]);
        let l1 = group(vec![Value::List(vec![Value::Float(-0.0)])]);
        assert_ne!(l0, l1, "nested List -0.0/+0.0 must be DISTINCT (#2074)");
    }

    // ---- Issue #2202: SUM/AVG integral result-type preservation ----

    /// Build a single-group (global) plan with one SUM or AVG over column `col`.
    fn scalar_agg_plan(function: AggregateType, col: &str) -> AggregationPlan {
        AggregationPlan {
            group_by_columns: Vec::new(),
            group_by_output_names: Vec::new(),
            aggregates: vec![AggregateComputation {
                function,
                column: col.to_string(),
                alias: "r".to_string(),
                distinct: false,
            }],
        }
    }

    /// Drive `init → update per value → finalize_group` for one scalar aggregate
    /// with a resolved result type, returning the emitted result value.
    fn run_scalar_agg(
        function: AggregateType,
        result_type: Option<CqlType>,
        values: &[Value],
    ) -> Value {
        let plan = scalar_agg_plan(function, "v");
        let result_types = vec![result_type];
        let mut accs = init_aggregate_accumulators(&plan.aggregates, &result_types);
        for v in values {
            let row = QueryRow {
                values: [(std::sync::Arc::<str>::from("v"), v.clone())]
                    .into_iter()
                    .collect(),
                key: RowKey::new(vec![]),
                metadata: Default::default(),
                cell_metadata: None,
            };
            update_aggregate(&mut accs[0], &plan.aggregates[0], &row);
        }
        let row = finalize_group(vec![Value::Null], accs, &plan);
        row.values.get("r").cloned().expect("result present")
    }

    /// SUM over integral inputs preserves the ARGUMENT's own integral type and
    /// value (issue #2202, `AggregateFcts.java`): int → `Value::Integer`,
    /// bigint → `Value::BigInt`, and — Cassandra does NOT promote these —
    /// smallint → `Value::SmallInt`, tinyint → `Value::TinyInt`.
    #[test]
    fn sum_integral_inputs_preserve_integral_result() {
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::Int),
                &[Value::Integer(1), Value::Integer(2), Value::Integer(3)],
            ),
            Value::Integer(6),
            "SUM(int) emits Value::Integer, not a float"
        );
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::BigInt),
                &[Value::BigInt(10), Value::BigInt(20)],
            ),
            Value::BigInt(30),
            "SUM(bigint) emits Value::BigInt"
        );
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::SmallInt),
                &[Value::SmallInt(100), Value::SmallInt(50)],
            ),
            Value::SmallInt(150),
            "SUM(smallint) stays Value::SmallInt — Cassandra does not promote to int"
        );
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::TinyInt),
                &[Value::TinyInt(40), Value::TinyInt(2)],
            ),
            Value::TinyInt(42),
            "SUM(tinyint) stays Value::TinyInt — Cassandra does not promote to int"
        );
    }

    /// SUM over float/double inputs still yields `Value::Float` (double) — no
    /// regression from the pre-#2202 behaviour.
    #[test]
    fn sum_float_inputs_stay_double() {
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::Double),
                &[Value::Float(1.5), Value::Float(2.25)],
            ),
            Value::Float(3.75),
        );
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::Float),
                &[Value::Float32(1.0), Value::Float32(2.0)],
            ),
            Value::Float(3.0),
            "SUM(float) stays double (Value::Float)"
        );
    }

    /// AVG uses integer division for integral inputs and preserves the result
    /// type; bigint stays wide; float/double divide in f64.
    #[test]
    fn avg_integral_uses_integer_division_and_preserves_type() {
        // AVG(int) of [1, 2] = 1 (integer division truncates, like Cassandra).
        assert_eq!(
            run_scalar_agg(
                AggregateType::Avg,
                Some(CqlType::Int),
                &[Value::Integer(1), Value::Integer(2)],
            ),
            Value::Integer(1),
        );
        // A zero still counts toward AVG's divisor.
        assert_eq!(
            run_scalar_agg(
                AggregateType::Avg,
                Some(CqlType::Int),
                &[Value::Integer(0), Value::Integer(0), Value::Integer(3)],
            ),
            Value::Integer(1),
            "AVG counts zeros: 3/3 = 1"
        );
        assert_eq!(
            run_scalar_agg(
                AggregateType::Avg,
                Some(CqlType::BigInt),
                &[Value::BigInt(10), Value::BigInt(21)],
            ),
            Value::BigInt(15),
            "AVG(bigint) integer division stays bigint"
        );
        assert_eq!(
            run_scalar_agg(
                AggregateType::Avg,
                Some(CqlType::Double),
                &[Value::Float(1.0), Value::Float(2.0)],
            ),
            Value::Float(1.5),
            "AVG(double) divides in f64",
        );
        // Issue #2202: AVG(smallint)/AVG(tinyint) stay their own narrow width —
        // Cassandra does not promote them to int.
        assert_eq!(
            run_scalar_agg(
                AggregateType::Avg,
                Some(CqlType::SmallInt),
                &[Value::SmallInt(10), Value::SmallInt(21)],
            ),
            Value::SmallInt(15),
            "AVG(smallint) integer division stays smallint"
        );
        assert_eq!(
            run_scalar_agg(
                AggregateType::Avg,
                Some(CqlType::TinyInt),
                &[Value::TinyInt(10), Value::TinyInt(21)],
            ),
            Value::TinyInt(15),
            "AVG(tinyint) integer division stays tinyint"
        );
    }

    /// Integral SUM overflow WRAPS (Cassandra's two's-complement Java semantics),
    /// never saturating or panicking, at EACH type's OWN width: `tinyint` wraps
    /// at the i8 boundary, `smallint` at i16, `int` at i32, `bigint` at i64.
    #[test]
    fn sum_integral_overflow_wraps() {
        // i8::MAX + 1 wraps to i8::MIN (tinyint stays tinyint, no promotion).
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::TinyInt),
                &[Value::TinyInt(i8::MAX), Value::TinyInt(1)],
            ),
            Value::TinyInt(i8::MIN),
            "SUM(tinyint) wraps at the i8 boundary"
        );
        // i16::MAX + 1 wraps to i16::MIN (smallint stays smallint).
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::SmallInt),
                &[Value::SmallInt(i16::MAX), Value::SmallInt(1)],
            ),
            Value::SmallInt(i16::MIN),
            "SUM(smallint) wraps at the i16 boundary"
        );
        // i32::MAX + 1 wraps to i32::MIN.
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::Int),
                &[Value::Integer(i32::MAX), Value::Integer(1)],
            ),
            Value::Integer(i32::MIN),
            "SUM(int) wraps at the i32 boundary"
        );
        // i64::MAX + 1 wraps to i64::MIN.
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::BigInt),
                &[Value::BigInt(i64::MAX), Value::BigInt(1)],
            ),
            Value::BigInt(i64::MIN),
            "SUM(bigint) wraps at the i64 boundary"
        );
    }

    /// NULLs are ignored by SUM/AVG; an all-null group's SUM finalizes to the
    /// typed zero (matching its result-type metadata) and AVG (count 0) is NULL.
    #[test]
    fn sum_avg_ignore_nulls_typed_zero_and_null() {
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                Some(CqlType::Int),
                &[Value::Null, Value::Integer(5), Value::Null],
            ),
            Value::Integer(5),
            "SUM ignores NULLs"
        );
        assert_eq!(
            run_scalar_agg(AggregateType::Sum, Some(CqlType::Int), &[Value::Null]),
            Value::Integer(0),
            "all-null int SUM group is a typed integer zero (not a float)"
        );
        assert_eq!(
            run_scalar_agg(AggregateType::Avg, Some(CqlType::Int), &[Value::Null]),
            Value::Null,
            "AVG over no counted inputs is NULL"
        );
    }

    /// With an unknown argument type (no schema), SUM/AVG fall back to the
    /// floating domain → `Value::Float` (double), consistent with the `double`
    /// result metadata `aggregate_result_cql_type` returns for `None`.
    #[test]
    fn sum_avg_unknown_type_falls_back_to_double() {
        assert_eq!(
            run_scalar_agg(
                AggregateType::Sum,
                None,
                &[Value::Integer(1), Value::Integer(2)],
            ),
            Value::Float(3.0),
        );
    }
}

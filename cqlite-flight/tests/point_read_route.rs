//! Stage 0 (issue #2207): route-detection unit tests for the point-read analyzer.
//!
//! Exercises the PUBLIC `cqlite_flight::point_read::detect_route` surface against
//! typed `FilterExpr` trees + schema, proving the total, schema-driven decision:
//! full-PK equality (and `IN`/`Or` lists of it) → a point route; every other
//! shape → `Scan`. No byte-pattern inference (issue #28) — the analyzer only ever
//! reads column names and the schema's partition-key definition.

use cqlite_core::query::{SSTableFilterOp, SSTablePredicate};
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::types::Value;
use cqlite_flight::filter::FilterExpr;
use cqlite_flight::point_read::{detect_route, PointReadRoute};
use std::collections::HashMap;

fn key(name: &str, pos: usize) -> KeyColumn {
    KeyColumn {
        name: name.into(),
        data_type: "int".into(),
        position: pos,
    }
}

fn col(name: &str) -> Column {
    Column {
        name: name.into(),
        data_type: "int".into(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// Single-component PK `a`, plus clustering `ck` and regular `v`.
fn single_pk_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![key("a", 0)],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![col("a"), col("ck"), col("v")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Composite PK `(a, b)`, plus regular `v`.
fn composite_pk_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![key("a", 0), key("b", 1)],
        clustering_keys: vec![],
        columns: vec![col("a"), col("b"), col("v")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn eq(column: &str, n: i32) -> FilterExpr {
    FilterExpr::Leaf(SSTablePredicate {
        column: column.into(),
        operation: SSTableFilterOp::Equal,
        values: vec![Value::Integer(n)],
        token_columns: None,
    })
}

fn gt(column: &str, n: i32) -> FilterExpr {
    FilterExpr::Leaf(SSTablePredicate {
        column: column.into(),
        operation: SSTableFilterOp::Gt,
        values: vec![Value::Integer(n)],
        token_columns: None,
    })
}

fn in_list(column: &str, ns: &[i32]) -> FilterExpr {
    FilterExpr::Leaf(SSTablePredicate {
        column: column.into(),
        operation: SSTableFilterOp::In,
        values: ns.iter().map(|n| Value::Integer(*n)).collect(),
        token_columns: None,
    })
}

#[test]
fn full_single_pk_equality_is_point_route() {
    let schema = single_pk_schema();
    let route = detect_route(Some(&eq("a", 7)), &schema);
    assert_eq!(
        route,
        PointReadRoute::PartitionPointRead(vec![Value::Integer(7)])
    );
}

#[test]
fn composite_pk_fully_bound_is_point_route() {
    let schema = composite_pk_schema();
    let filter = FilterExpr::And(vec![eq("a", 1), eq("b", 2)]);
    let route = detect_route(Some(&filter), &schema);
    assert_eq!(
        route,
        PointReadRoute::PartitionPointRead(vec![Value::Integer(1), Value::Integer(2)]),
        "both components bound → point route, values in schema order"
    );
}

#[test]
fn composite_pk_order_follows_schema_not_predicate_order() {
    let schema = composite_pk_schema();
    // Predicates given b-first: the route must still be (a, b) schema order.
    let filter = FilterExpr::And(vec![eq("b", 2), eq("a", 1)]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::PartitionPointRead(vec![Value::Integer(1), Value::Integer(2)])
    );
}

#[test]
fn partial_composite_pk_keeps_scan() {
    let schema = composite_pk_schema();
    // Only `a` bound; `b` unconstrained → cannot point-read.
    assert_eq!(
        detect_route(Some(&eq("a", 1)), &schema),
        PointReadRoute::Scan
    );
}

#[test]
fn clustering_only_equality_keeps_scan() {
    let schema = single_pk_schema();
    assert_eq!(
        detect_route(Some(&eq("ck", 3)), &schema),
        PointReadRoute::Scan
    );
}

#[test]
fn range_on_pk_keeps_scan() {
    let schema = single_pk_schema();
    assert_eq!(
        detect_route(Some(&gt("a", 3)), &schema),
        PointReadRoute::Scan
    );
}

#[test]
fn no_predicate_keeps_scan() {
    let schema = single_pk_schema();
    assert_eq!(detect_route(None, &schema), PointReadRoute::Scan);
}

#[test]
fn is_null_on_pk_keeps_scan() {
    let schema = single_pk_schema();
    let filter = FilterExpr::IsNull("a".into());
    assert_eq!(detect_route(Some(&filter), &schema), PointReadRoute::Scan);
}

#[test]
fn not_on_pk_keeps_scan() {
    let schema = single_pk_schema();
    let filter = FilterExpr::Not(Box::new(eq("a", 1)));
    assert_eq!(detect_route(Some(&filter), &schema), PointReadRoute::Scan);
}

#[test]
fn full_pk_in_list_is_multi_point_route() {
    let schema = single_pk_schema();
    let route = detect_route(Some(&in_list("a", &[1, 2, 3])), &schema);
    assert_eq!(
        route,
        PointReadRoute::MultiPartitionPointRead(vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ])
    );
}

#[test]
fn composite_pk_in_list_keeps_scan() {
    // A composite-PK IN would be a cartesian expansion we deliberately do not take.
    let schema = composite_pk_schema();
    assert_eq!(
        detect_route(Some(&in_list("a", &[1, 2])), &schema),
        PointReadRoute::Scan
    );
}

#[test]
fn residual_non_pk_conjunct_does_not_block_point_route() {
    let schema = single_pk_schema();
    // `a = 5 AND v > 3` still routes on the PK equality; `v > 3` is a residual.
    let filter = FilterExpr::And(vec![eq("a", 5), gt("v", 3)]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::PartitionPointRead(vec![Value::Integer(5)])
    );
}

#[test]
fn residual_clustering_conjunct_does_not_block_point_route() {
    let schema = single_pk_schema();
    // `a = 5 AND ck = 9`: ck is a residual clustering filter, still a point route.
    let filter = FilterExpr::And(vec![eq("a", 5), eq("ck", 9)]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::PartitionPointRead(vec![Value::Integer(5)])
    );
}

#[test]
fn or_of_full_pk_equalities_is_multi_point_route() {
    let schema = composite_pk_schema();
    let filter = FilterExpr::Or(vec![
        FilterExpr::And(vec![eq("a", 1), eq("b", 2)]),
        FilterExpr::And(vec![eq("a", 3), eq("b", 4)]),
    ]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::MultiPartitionPointRead(vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(4)],
        ])
    );
}

#[test]
fn or_with_a_partial_disjunct_keeps_scan() {
    let schema = composite_pk_schema();
    // Second disjunct binds only `a` → not every disjunct is a full-PK equality.
    let filter = FilterExpr::Or(vec![
        FilterExpr::And(vec![eq("a", 1), eq("b", 2)]),
        eq("a", 3),
    ]);
    assert_eq!(detect_route(Some(&filter), &schema), PointReadRoute::Scan);
}

#[test]
fn conflicting_pk_equalities_keep_scan() {
    let schema = single_pk_schema();
    // `a = 1 AND a = 2` can match no partition; not a single-value point read.
    let filter = FilterExpr::And(vec![eq("a", 1), eq("a", 2)]);
    assert_eq!(detect_route(Some(&filter), &schema), PointReadRoute::Scan);
}

#[test]
fn duplicate_consistent_pk_equality_is_point_route() {
    let schema = single_pk_schema();
    // `a = 1 AND a = 1` is still a single-value binding.
    let filter = FilterExpr::And(vec![eq("a", 1), eq("a", 1)]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::PartitionPointRead(vec![Value::Integer(1)])
    );
}

/// The design's fixed named cap (`MAX_MULTI_PARTITION_POINT_READ_KEYS = 64` in
/// `point_read.rs`, design.md open question 2): an `IN` list AT the cap still
/// routes as N point reads.
#[test]
fn full_pk_in_list_at_cap_is_still_multi_point_route() {
    let schema = single_pk_schema();
    let values: Vec<i32> = (0..64).collect();
    let route = detect_route(Some(&in_list("a", &values)), &schema);
    match route {
        PointReadRoute::MultiPartitionPointRead(keys) => assert_eq!(keys.len(), 64),
        other => panic!("expected MultiPartitionPointRead at the cap, got {other:?}"),
    }
}

/// An `IN` list ONE OVER the cap falls back to `Scan` — never a wrong answer,
/// just the faster path for a very large list.
#[test]
fn full_pk_in_list_over_cap_falls_back_to_scan() {
    let schema = single_pk_schema();
    let values: Vec<i32> = (0..65).collect();
    assert_eq!(
        detect_route(Some(&in_list("a", &values)), &schema),
        PointReadRoute::Scan
    );
}

/// Finding 1 (roborev, issue #2207): a single-component full-PK `IN` nested
/// UNDER an `And` alongside a non-PK residual still routes as N point reads — the
/// `v = 3` conjunct stays a residual filter, exactly like the single-equality
/// residual path. Previously demoted to Scan because `IN` was only recognized at
/// the filter-tree root.
#[test]
fn pk_in_list_under_and_with_residual_is_multi_point_route() {
    let schema = single_pk_schema();
    // `a IN (1, 2) AND v = 3`
    let filter = FilterExpr::And(vec![in_list("a", &[1, 2]), eq("v", 3)]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::MultiPartitionPointRead(vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ]),
        "IN under And routes; the non-PK conjunct is a residual, not a blocker"
    );
}

/// Finding 1: an `Or` of full-PK equalities nested UNDER an `And` alongside a
/// residual routes as N point reads. Previously demoted to Scan because `Or` was
/// only recognized at the root.
#[test]
fn or_of_pk_equalities_under_and_with_residual_is_multi_point_route() {
    let schema = single_pk_schema();
    // `(a = 1 OR a = 2) AND v = 3`
    let filter = FilterExpr::And(vec![
        FilterExpr::Or(vec![eq("a", 1), eq("a", 2)]),
        eq("v", 3),
    ]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::MultiPartitionPointRead(vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ]),
        "Or-of-full-PK-equalities under And routes; the residual does not block it"
    );
}

/// Finding 1: a composite-PK `Or` disjunction under an `And` with a residual.
#[test]
fn composite_or_under_and_with_residual_is_multi_point_route() {
    let schema = composite_pk_schema();
    // `((a=1 AND b=2) OR (a=3 AND b=4)) AND v = 9`
    let filter = FilterExpr::And(vec![
        FilterExpr::Or(vec![
            FilterExpr::And(vec![eq("a", 1), eq("b", 2)]),
            FilterExpr::And(vec![eq("a", 3), eq("b", 4)]),
        ]),
        eq("v", 9),
    ]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::MultiPartitionPointRead(vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(4)],
        ])
    );
}

/// Finding 1 (conservative choice): two DIFFERENT full-PK key-groups under one
/// `And` (`a IN (1,2) AND a IN (3,4)`) fall back to Scan rather than intersect
/// them — always a correct answer via the scan path, never a wrong one.
#[test]
fn two_conflicting_key_groups_under_and_fall_back_to_scan() {
    let schema = single_pk_schema();
    let filter = FilterExpr::And(vec![in_list("a", &[1, 2]), in_list("a", &[3, 4])]);
    assert_eq!(
        detect_route(Some(&filter), &schema),
        PointReadRoute::Scan,
        "two distinct key-groups under one And → conservative Scan fallback"
    );
}

/// Finding 1 (conservative choice): a full-PK `IN` group AND a full-PK equality
/// binding under the same `And` are two key-groups → conservative Scan.
#[test]
fn in_group_plus_equality_binding_under_and_falls_back_to_scan() {
    let schema = single_pk_schema();
    // `a IN (1, 2) AND a = 1`
    let filter = FilterExpr::And(vec![in_list("a", &[1, 2]), eq("a", 1)]);
    assert_eq!(detect_route(Some(&filter), &schema), PointReadRoute::Scan);
}

/// Finding 2 (roborev, issue #2207): a duplicate-heavy `IN` list whose DISTINCT
/// key count is within the cap must route (not over-fall-back to Scan) — dedup
/// happens BEFORE the cap. 100 keys, all duplicates of 3 distinct values.
#[test]
fn duplicate_heavy_in_list_dedups_before_cap() {
    let schema = single_pk_schema();
    // 100 raw values cycling over {1, 2, 3} → 3 distinct, well under the 64 cap.
    let raw: Vec<i32> = (0..100).map(|i| (i % 3) + 1).collect();
    let route = detect_route(Some(&in_list("a", &raw)), &schema);
    assert_eq!(
        route,
        PointReadRoute::MultiPartitionPointRead(vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]),
        "100 duplicate keys deduping to 3 uniques must route, not fall back to Scan"
    );
}

/// Finding 2: dedup preserves first-seen order and the cap still enforces on the
/// DISTINCT count — 65 distinct values (even if repeated) fall back to Scan.
#[test]
fn over_cap_distinct_in_list_still_falls_back_to_scan() {
    let schema = single_pk_schema();
    // 65 distinct values, each duplicated once → 130 raw, 65 distinct > 64 cap.
    let mut raw: Vec<i32> = (0..65).collect();
    raw.extend(0..65);
    assert_eq!(
        detect_route(Some(&in_list("a", &raw)), &schema),
        PointReadRoute::Scan,
        "65 DISTINCT keys exceed the cap even after dedup"
    );
}

#[test]
fn token_predicate_on_pk_keeps_scan() {
    let schema = single_pk_schema();
    let filter = FilterExpr::Leaf(SSTablePredicate {
        column: "token(a)".into(),
        operation: SSTableFilterOp::Gte,
        values: vec![Value::BigInt(0)],
        token_columns: Some(vec!["a".into()]),
    });
    assert_eq!(detect_route(Some(&filter), &schema), PointReadRoute::Scan);
}

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
    assert_eq!(detect_route(Some(&eq("a", 1)), &schema), PointReadRoute::Scan);
}

#[test]
fn clustering_only_equality_keeps_scan() {
    let schema = single_pk_schema();
    assert_eq!(detect_route(Some(&eq("ck", 3)), &schema), PointReadRoute::Scan);
}

#[test]
fn range_on_pk_keeps_scan() {
    let schema = single_pk_schema();
    assert_eq!(detect_route(Some(&gt("a", 3)), &schema), PointReadRoute::Scan);
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

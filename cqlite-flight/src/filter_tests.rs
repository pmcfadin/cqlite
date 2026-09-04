//! Unit tests for the ticket -> `ScanSpec` lowering: token filters, the Kleene
//! predicate tree, operand coercion and the projection/limit fields — split out
//! of `filter.rs` to keep that file under the campsite file-size target (epic
//! #1116 source / #1135 tests), following this crate's existing
//! `<module>_tests.rs` convention (`bypass_tests.rs`, `statics_tests.rs`, ...).

use super::*;
use crate::testutil::{clustering_schema, simple_schema, uuid_schema};
// Issue #3742 admission tests: the aggregation half of the ticket, which
// `filter.rs` itself does not name.
use crate::ticket::{AggFunc, AggregateSpec, Aggregation};
use serde_json::json;

fn ticket_with(predicates: Vec<Predicate>) -> FlightTicket {
    FlightTicket {
        keyspace: "flight_ks".into(),
        table: "items".into(),
        predicates,
        ..Default::default()
    }
}

/// A v1 flat predicate list lowers to `And([Leaf, ...])`. Pull the resolved
/// `SSTablePredicate` for leaf `i` so the type/operand assertions below read
/// like the pre-#834 ones.
fn lowered_leaf(spec: &ScanSpec, i: usize) -> &SSTablePredicate {
    match spec.filter.as_ref().expect("filter present") {
        FilterExpr::And(exprs) => match &exprs[i] {
            FilterExpr::Leaf(p) => p,
            other => panic!("expected leaf, got {other:?}"),
        },
        other => panic!("expected And, got {other:?}"),
    }
}

/// Build a row with one named column value for evaluator tests.
fn row_with(column: &str, value: Value) -> QueryRow {
    let mut values: std::collections::HashMap<std::sync::Arc<str>, Value> =
        std::collections::HashMap::new();
    values.insert(column.into(), value);
    QueryRow {
        values,
        key: cqlite_core::RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

fn empty_row() -> QueryRow {
    QueryRow {
        values: std::collections::HashMap::new(),
        key: cqlite_core::RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

fn score_gt(n: i64) -> FilterExpr {
    FilterExpr::Leaf(SSTablePredicate {
        column: "score".into(),
        operation: SSTableFilterOp::Gt,
        values: vec![Value::Integer(n as i32)],
        token_columns: None,
    })
}

#[test]
fn token_filter_built_from_bounds() {
    let mut t = ticket_with(vec![]);
    t.token_start = Some(-10);
    t.token_end = Some(10);
    let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
    let tf = spec.token.expect("token filter present");
    assert!(!tf.contains(-10), "start exclusive");
    assert!(tf.contains(0));
    assert!(tf.contains(10), "end inclusive");
    assert!(!tf.contains(11));
}

/// Issue #2412 §C / #2413: the core-side `ScanTokenBound` the Summary-guided
/// walk uses for token pushdown must agree with this crate's `TokenFilter` on
/// EVERY token — the membership rule must live in one place, never diverge.
/// Grid across normal, wraparound, and equal-endpoint (full-ring) shapes.
#[test]
fn token_filter_lowering_agrees_with_core() {
    let shapes = [
        (0i64, 100i64, false),
        (100, -100, true),
        (42, 42, false),
        (42, 42, true),
        (i64::MIN, i64::MAX, false),
        (-5, 5, false),
    ];
    let probes = [
        i64::MIN,
        -1000,
        -101,
        -100,
        -99,
        -5,
        -1,
        0,
        1,
        5,
        41,
        42,
        43,
        99,
        100,
        101,
        1000,
        i64::MAX,
    ];
    for (start, end, wraparound) in shapes {
        let tf = TokenFilter {
            start,
            end,
            wraparound,
        };
        let bound = tf.to_scan_bound();
        for t in probes {
            assert_eq!(
                tf.contains(t),
                bound.contains(t),
                "TokenFilter and ScanTokenBound must agree for token {t} on \
                 ({start}, {end}] wrap={wraparound}"
            );
        }
    }
}

#[test]
fn overlaps_non_wraparound_boundaries() {
    // Split range (0, 100].
    let tf = TokenFilter {
        start: 0,
        end: 100,
        wraparound: false,
    };
    // Span entirely inside.
    assert!(tf.overlaps(10, 50));
    // Span straddling the start (max past start, min before start).
    assert!(tf.overlaps(-10, 10));
    // Span entirely below: max_token == start is NOT past the exclusive start.
    assert!(
        !tf.overlaps(-50, 0),
        "max_token==start excluded (half-open)"
    );
    assert!(!tf.overlaps(-50, -1));
    // Span entirely above: min_token == end is inclusive (overlaps).
    assert!(tf.overlaps(100, 200), "min_token==end included");
    assert!(!tf.overlaps(101, 200), "min_token>end excluded");
    // Span covering the whole range.
    assert!(tf.overlaps(i64::MIN, i64::MAX));
}

#[test]
fn overlaps_full_ring_equal_endpoints() {
    // #2228: an equal-endpoint range `(T, T]` is the full ring, so every
    // SSTable span overlaps it regardless of the `wraparound` flag.
    for wrap in [true, false] {
        let tf = TokenFilter {
            start: 42,
            end: 42,
            wraparound: wrap,
        };
        assert!(tf.overlaps(i64::MIN, i64::MAX), "whole ring (wrap={wrap})");
        assert!(tf.overlaps(100, 200), "span entirely above T (wrap={wrap})");
        assert!(
            tf.overlaps(-200, -100),
            "span entirely below T (wrap={wrap})"
        );
        assert!(tf.overlaps(42, 42), "single-token span at T (wrap={wrap})");
    }
}

#[test]
fn overlaps_wraparound_boundaries() {
    // Wraparound range (100, -100] = (100, MAX] ∪ [MIN, -100].
    let tf = TokenFilter {
        start: 100,
        end: -100,
        wraparound: true,
    };
    // Span in the high arm (past start).
    assert!(tf.overlaps(150, 200));
    // Span in the low arm (at or below end).
    assert!(tf.overlaps(-300, -200));
    assert!(tf.overlaps(-300, -100), "max_token reaching end inclusive");
    // Span entirely in the excluded middle gap (-99, 100].
    assert!(!tf.overlaps(-50, 50), "middle gap excluded");
    assert!(
        !tf.overlaps(0, 100),
        "max_token==start not past exclusive start"
    );
    // Span touching only the inclusive low end.
    assert!(tf.overlaps(-100, -100));
}

#[test]
fn no_bounds_means_no_token_filter() {
    let spec = ScanSpec::from_ticket(&ticket_with(vec![]), &simple_schema()).unwrap();
    assert!(spec.token.is_none());
}

#[test]
fn limit_is_carried_from_ticket() {
    // No limit on the ticket → None on the spec.
    let spec = ScanSpec::from_ticket(&ticket_with(vec![]), &simple_schema()).unwrap();
    assert_eq!(spec.limit, None);

    // A ticket limit flows straight through (including the 0 edge).
    let t = FlightTicket {
        limit: Some(5),
        ..ticket_with(vec![])
    };
    assert_eq!(
        ScanSpec::from_ticket(&t, &simple_schema()).unwrap().limit,
        Some(5)
    );
    let t = FlightTicket {
        limit: Some(0),
        ..ticket_with(vec![])
    };
    assert_eq!(
        ScanSpec::from_ticket(&t, &simple_schema()).unwrap().limit,
        Some(0)
    );
}

#[test]
fn int_predicate_translates_with_natural_width() {
    let t = ticket_with(vec![Predicate {
        column: "score".into(),
        op: PredicateOp::Gt,
        value: json!(10),
    }]);
    let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
    let leaf = lowered_leaf(&spec, 0);
    assert_eq!(leaf.column, "score");
    assert!(matches!(leaf.operation, SSTableFilterOp::Gt));
    assert_eq!(leaf.values, vec![Value::Integer(10)]);
}

#[test]
fn in_predicate_expands_json_array() {
    let t = ticket_with(vec![Predicate {
        column: "name".into(),
        op: PredicateOp::In,
        value: json!(["a", "b"]),
    }]);
    let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
    assert_eq!(
        lowered_leaf(&spec, 0).values,
        vec![Value::Text("a".into()), Value::Text("b".into())]
    );
}

#[test]
fn uuid_predicate_parses_to_bytes() {
    let t = FlightTicket {
        keyspace: "flight_ks".into(),
        table: "uu".into(),
        predicates: vec![Predicate {
            column: "id".into(),
            op: PredicateOp::Equal,
            value: json!("00000000-0000-0000-0000-000000000001"),
        }],
        ..Default::default()
    };
    let spec = ScanSpec::from_ticket(&t, &uuid_schema()).unwrap();
    let mut expected = [0u8; 16];
    expected[15] = 1;
    assert_eq!(lowered_leaf(&spec, 0).values, vec![Value::Uuid(expected)]);
}

#[test]
fn predicate_on_clustering_column_resolves_type() {
    let t = FlightTicket {
        keyspace: "flight_ks".into(),
        table: "wide".into(),
        predicates: vec![Predicate {
            column: "ck".into(),
            op: PredicateOp::Equal,
            value: json!("a"),
        }],
        ..Default::default()
    };
    let spec = ScanSpec::from_ticket(&t, &clustering_schema()).unwrap();
    assert_eq!(lowered_leaf(&spec, 0).values, vec![Value::Text("a".into())]);
}

#[test]
fn unknown_column_is_rejected() {
    let t = ticket_with(vec![Predicate {
        column: "nope".into(),
        op: PredicateOp::Equal,
        value: json!(1),
    }]);
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    assert!(matches!(err, FilterError::UnknownColumn(c) if c == "nope"));
}

#[test]
fn type_mismatch_is_rejected() {
    let t = ticket_with(vec![Predicate {
        column: "score".into(),
        op: PredicateOp::Equal,
        value: json!("not a number"),
    }]);
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    assert!(matches!(err, FilterError::BadOperand { .. }));
}

#[test]
fn empty_in_is_rejected() {
    let t = ticket_with(vec![Predicate {
        column: "score".into(),
        op: PredicateOp::In,
        value: json!([]),
    }]);
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    assert!(matches!(err, FilterError::BadOperand { .. }));
}

#[test]
fn v1_in_with_non_array_operand_is_rejected() {
    // A v1 flat IN predicate must carry a JSON array; a scalar operand is a
    // malformed legacy ticket and must error (not be folded into a singleton).
    let t = ticket_with(vec![Predicate {
        column: "score".into(),
        op: PredicateOp::In,
        value: json!(5),
    }]);
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    assert!(matches!(err, FilterError::BadOperand { .. }));
}

#[test]
fn null_operand_is_rejected() {
    let t = ticket_with(vec![Predicate {
        column: "score".into(),
        op: PredicateOp::Equal,
        value: serde_json::Value::Null,
    }]);
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    assert!(matches!(err, FilterError::BadOperand { .. }));
}

#[test]
fn out_of_range_int_is_rejected_not_truncated() {
    let t = ticket_with(vec![Predicate {
        column: "score".into(),
        op: PredicateOp::Equal,
        value: json!(i64::from(i32::MAX) + 1),
    }]);
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    assert!(
        matches!(err, FilterError::BadOperand { .. }),
        "must error, not wrap"
    );
}

// ---- Issue #834: Kleene three-valued evaluator ----

/// A present, comparable leaf yields True/False; a missing or Null column
/// yields Unknown (the SQL UNKNOWN that NOT/OR must propagate).
#[test]
fn leaf_null_or_missing_is_unknown() {
    let p = score_gt(10);
    assert_eq!(
        p.evaluate(&row_with("score", Value::Integer(20))),
        Kleene::True
    );
    assert_eq!(
        p.evaluate(&row_with("score", Value::Integer(5))),
        Kleene::False
    );
    assert_eq!(
        p.evaluate(&empty_row()),
        Kleene::Unknown,
        "missing → Unknown"
    );
    assert_eq!(
        p.evaluate(&row_with("score", Value::Null)),
        Kleene::Unknown,
        "Null cell → Unknown"
    );
}

/// `IS NULL` is always definite: True for absent/Null, False otherwise —
/// never Unknown.
#[test]
fn is_null_is_definite() {
    let expr = FilterExpr::IsNull("score".into());
    assert_eq!(expr.evaluate(&empty_row()), Kleene::True, "absent IS NULL");
    assert_eq!(
        expr.evaluate(&row_with("score", Value::Null)),
        Kleene::True,
        "Null IS NULL"
    );
    assert_eq!(
        expr.evaluate(&row_with("score", Value::Integer(1))),
        Kleene::False,
        "present is not null"
    );
}

/// `NOT` flips True/False and leaves Unknown unchanged.
#[test]
fn not_truth_table_propagates_unknown() {
    let not = |k_row: QueryRow| FilterExpr::Not(Box::new(score_gt(10))).evaluate(&k_row);
    assert_eq!(not(row_with("score", Value::Integer(20))), Kleene::False);
    assert_eq!(not(row_with("score", Value::Integer(5))), Kleene::True);
    assert_eq!(not(empty_row()), Kleene::Unknown, "NOT Unknown = Unknown");
}

/// `AND` truth table including Unknown propagation; empty AND is True.
#[test]
fn and_truth_table() {
    let t = || score_gt(10); // True for score=20
    let f = || {
        FilterExpr::Leaf(SSTablePredicate {
            column: "score".into(),
            operation: SSTableFilterOp::Lt,
            values: vec![Value::Integer(0)],
            token_columns: None,
        })
    }; // False for score=20
    let u = || FilterExpr::Leaf(score_pred_on_missing()); // Unknown
    let row = row_with("score", Value::Integer(20));

    assert_eq!(
        FilterExpr::And(vec![]).evaluate(&row),
        Kleene::True,
        "empty AND"
    );
    assert_eq!(FilterExpr::And(vec![t(), t()]).evaluate(&row), Kleene::True);
    assert_eq!(
        FilterExpr::And(vec![t(), f()]).evaluate(&row),
        Kleene::False
    );
    // Any False dominates, even with an Unknown present.
    assert_eq!(
        FilterExpr::And(vec![u(), f()]).evaluate(&row),
        Kleene::False
    );
    // Unknown with no False → Unknown.
    assert_eq!(
        FilterExpr::And(vec![t(), u()]).evaluate(&row),
        Kleene::Unknown
    );
}

/// `OR` truth table including Unknown propagation; empty OR is False.
#[test]
fn or_truth_table() {
    let t = || score_gt(10);
    let f = || {
        FilterExpr::Leaf(SSTablePredicate {
            column: "score".into(),
            operation: SSTableFilterOp::Lt,
            values: vec![Value::Integer(0)],
            token_columns: None,
        })
    };
    let u = || FilterExpr::Leaf(score_pred_on_missing());
    let row = row_with("score", Value::Integer(20));

    assert_eq!(
        FilterExpr::Or(vec![]).evaluate(&row),
        Kleene::False,
        "empty OR"
    );
    assert_eq!(FilterExpr::Or(vec![f(), f()]).evaluate(&row), Kleene::False);
    assert_eq!(FilterExpr::Or(vec![f(), t()]).evaluate(&row), Kleene::True);
    // Any True dominates, even with an Unknown present.
    assert_eq!(FilterExpr::Or(vec![u(), t()]).evaluate(&row), Kleene::True);
    // Unknown with no True → Unknown.
    assert_eq!(
        FilterExpr::Or(vec![f(), u()]).evaluate(&row),
        Kleene::Unknown
    );
}

/// `keeps` is True-only: Unknown and False both reject (WHERE semantics).
#[test]
fn keeps_only_when_true() {
    let p = score_gt(10);
    assert!(p.keeps(&row_with("score", Value::Integer(20))));
    assert!(
        !p.keeps(&row_with("score", Value::Integer(5))),
        "False rejects"
    );
    assert!(!p.keeps(&empty_row()), "Unknown rejects");
}

/// A leaf that always evaluates Unknown because it tests a column the row
/// never has — used to drive the Unknown cells of the truth tables.
fn score_pred_on_missing() -> SSTablePredicate {
    SSTablePredicate {
        column: "absent_column".into(),
        operation: SSTableFilterOp::Gt,
        values: vec![Value::Integer(0)],
        token_columns: None,
    }
}

/// #3634: `ScanSpec::from_ticket` must DERIVE `TokenFilter`'s wrapping from the
/// ticket's endpoints, never copy the ticket's `wraparound` wire flag.
///
/// `FlightTicket::wraparound` is `#[serde(default)]` and `validate()` never checks
/// it against `token_start`/`token_end`, so a client can present either
/// inconsistent shape — and Cassandra can express neither, because
/// `Range.isWrapAround(left, right)` IS `left.compareTo(right) >= 0`
/// (`cassandra-5.0.8:src/java/org/apache/cassandra/dht/Range.java`).
///
/// Copying the flag made the filter disagree with the core bound it lowers to:
/// `to_scan_bound` derives, while `contains` gates FOUR serving paths that take no
/// pushdown (producer_stream, producer_point, producer_drive, statics) and
/// `overlaps` gates warm pruning. The same ticket then returned different rows
/// depending on which path served it. Both shapes are pinned here BY OUTCOME, in
/// both directions, so a re-copy of the flag reds this test.
#[test]
fn from_ticket_derives_wraparound_and_ignores_an_inconsistent_wire_flag() {
    let schema = simple_schema();

    // Shape 1: start > end with the flag FALSE (the shape a client reaches by
    // merely OMITTING the optional flag). Cassandra wraps; so must we.
    for wire_flag in [false, true] {
        let mut t = ticket_with(vec![]);
        t.token_start = Some(100);
        t.token_end = Some(-100);
        t.wraparound = wire_flag;
        let spec = ScanSpec::from_ticket(&t, &schema).expect("spec");
        let tf = spec.token.expect("token filter present");
        for token in [i64::MIN, -1000, -100, 101, i64::MAX] {
            assert!(
                tf.contains(token),
                "(100, -100] wraps whatever the wire flag says (was {wire_flag}), \
                 so token {token} is in range"
            );
        }
        for token in [-99, 0, 99, 100] {
            assert!(
                !tf.contains(token),
                "(100, -100] excludes its interior gap; token {token} (wire flag \
                 {wire_flag})"
            );
        }
        // And the lowered core bound must agree with it, on every probe.
        let bound = tf.to_scan_bound();
        for token in [i64::MIN, -1000, -100, -99, 0, 99, 100, 101, i64::MAX] {
            assert_eq!(
                tf.contains(token),
                bound.contains(token),
                "filter and lowered bound must agree for token {token} (wire flag \
                 {wire_flag})"
            );
        }
    }

    // Shape 2: start < end with the flag TRUE — the direction roborev caught.
    // Cassandra does NOT wrap here, so the range is the narrow (10, 20], not the
    // outer-ring superset the flag would have produced.
    for wire_flag in [false, true] {
        let mut t = ticket_with(vec![]);
        t.token_start = Some(10);
        t.token_end = Some(20);
        t.wraparound = wire_flag;
        let spec = ScanSpec::from_ticket(&t, &schema).expect("spec");
        let tf = spec.token.expect("token filter present");
        assert!(
            !tf.contains(10),
            "start is exclusive (wire flag {wire_flag})"
        );
        assert!(tf.contains(11), "wire flag {wire_flag}");
        assert!(tf.contains(20), "end is inclusive (wire flag {wire_flag})");
        assert!(
            !tf.contains(21),
            "(10, 20] must NOT become the outer-ring superset the flag would give \
             (wire flag {wire_flag})"
        );
        assert!(
            !tf.contains(i64::MIN),
            "the outer ring is NOT in (10, 20] — this is the shape whose flag made \
             the filter admit every token (wire flag {wire_flag})"
        );
        // Pruning must follow the same derivation, or a table gets kept/dropped
        // on a rule the row filter disagrees with.
        assert!(
            !tf.overlaps(-1000, 0),
            "a span wholly below (10, 20] must not overlap (wire flag {wire_flag})"
        );
        assert!(
            tf.overlaps(15, 30),
            "a span meeting (10, 20] must overlap (wire flag {wire_flag})"
        );
        let bound = tf.to_scan_bound();
        for token in [i64::MIN, 0, 10, 11, 20, 21, 1000, i64::MAX] {
            assert_eq!(
                tf.contains(token),
                bound.contains(token),
                "filter and lowered bound must agree for token {token} (wire flag \
                 {wire_flag})"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Issue #3742 — output-column ADMISSION.
//
// `ScanSpec::from_ticket` is the one place every `do_get` route passes through
// before a producer, an Arrow schema or a response stream exists
// (`service.rs:481`), so a request that would emit ZERO output columns is
// refused here. These pin the predicate as **total output columns**, in both
// directions: the shapes that must be REFUSED, and — just as load-bearing — the
// live shapes that must still be ADMITTED.
// ---------------------------------------------------------------------------

/// Route 1: an explicitly empty `columns` list resolves to no output columns.
#[test]
fn an_empty_projection_list_is_refused_at_spec_admission() {
    let t = FlightTicket {
        columns: Some(vec![]),
        ..ticket_with(vec![])
    };
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    assert!(
        matches!(err, FilterError::EmptyProjection),
        "expected EmptyProjection, got {err:?}"
    );
}

/// Route 2: a projection naming ONLY columns the table does not have used to be
/// silently emptied by `MergeProducer::with_spec`'s `retain` (which cannot
/// fail). It is now refused, and the error NAMES the offending columns.
#[test]
fn a_projection_of_only_unknown_columns_is_refused_and_names_them() {
    let t = FlightTicket {
        columns: Some(vec!["no_such_col".into(), "also_missing".into()]),
        ..ticket_with(vec![])
    };
    let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
    match &err {
        FilterError::UnknownProjectionColumns(names) => {
            assert_eq!(names, &["no_such_col".to_string(), "also_missing".into()]);
        }
        other => panic!("expected UnknownProjectionColumns, got {other:?}"),
    }
    let msg = err.to_string();
    for name in ["no_such_col", "also_missing"] {
        assert!(
            msg.contains(name),
            "the message must NAME the offending column {name}: {msg}"
        );
    }
}

/// A projection that resolves to at least one real column is admitted — the
/// unknown name is dropped by `retain` exactly as before. Only a projection
/// resolving to NOTHING is a bad request; this pins that the admission did not
/// quietly widen into "reject any unknown projected column".
#[test]
fn a_projection_mixing_known_and_unknown_columns_is_still_admitted() {
    let t = FlightTicket {
        columns: Some(vec!["name".into(), "no_such_col".into()]),
        ..ticket_with(vec![])
    };
    let spec = ScanSpec::from_ticket(&t, &simple_schema()).expect("must be admitted");
    assert_eq!(
        spec.projection.as_deref(),
        Some(&["name".to_string(), "no_such_col".into()][..]),
        "the projection is carried verbatim; narrowing stays with `with_spec`"
    );
}

/// No projection at all (`columns: null`) means ALL columns — never zero.
#[test]
fn an_absent_projection_is_admitted() {
    let spec = ScanSpec::from_ticket(&ticket_with(vec![]), &simple_schema()).expect("admitted");
    assert!(spec.projection.is_none());
}

fn agg_ticket(group_by: Vec<&str>, aggregates: Vec<AggregateSpec>) -> FlightTicket {
    FlightTicket {
        aggregation: Some(Aggregation {
            group_by: group_by.into_iter().map(String::from).collect(),
            aggregates,
        }),
        ..ticket_with(vec![])
    }
}

/// Route 4: an aggregation with neither group-by keys nor aggregates has an
/// empty output column set (`agg.rs::partial_columns` builds it from
/// `group_by + aggregates`).
#[test]
fn an_aggregation_with_no_group_by_and_no_aggregates_is_refused() {
    let err = ScanSpec::from_ticket(&agg_ticket(vec![], vec![]), &simple_schema()).unwrap_err();
    assert!(
        matches!(err, FilterError::EmptyAggregation),
        "expected EmptyAggregation, got {err:?}"
    );
}

/// THE TRAP THIS TEST EXISTS TO PREVENT (#3742): the admission predicate is
/// "zero total OUTPUT columns", **never** "the `aggregates` list is empty".
///
/// `SELECT DISTINCT c` reaches Trino's `applyAggregation` with
/// `groupingKeys=[c]` and `aggregations={}`, and the connector emits
/// `{"group_by": ["c"], "aggregates": []}` verbatim
/// (`CqliteFlightMetadata.java:569`). That is a LIVE, LEGITIMATE wire shape with
/// ONE output column: a predicate keyed on an empty `aggregates` array would
/// reject working `SELECT DISTINCT` queries.
#[test]
fn a_distinct_shaped_aggregation_with_empty_aggregates_is_admitted() {
    ScanSpec::from_ticket(&agg_ticket(vec!["name"], vec![]), &simple_schema())
        .expect("group_by with an empty aggregates list has ONE output column and must be served");
}

/// The mirror shape: a global `count(*)` has no group_by and one aggregate.
#[test]
fn a_global_count_aggregation_with_no_group_by_is_admitted() {
    let count = AggregateSpec {
        func: AggFunc::Count,
        column: None,
        output: "c".into(),
    };
    ScanSpec::from_ticket(&agg_ticket(vec![], vec![count]), &simple_schema())
        .expect("a global count(*) has ONE output column and must be served");
}

/// With an aggregation present the OUTPUT columns are `group_by + aggregates`,
/// so the projection does not contribute and an empty one is NOT zero-output.
/// Refusing it here would reject a request whose output column set is
/// non-empty — the exact over-rejection the "total output columns" predicate
/// exists to avoid. (The shipped connector never sends this pairing:
/// `CqliteFlightAggregatePageSource.java:113` passes `Optional.empty()`.)
#[test]
fn an_empty_projection_alongside_an_aggregation_is_admitted() {
    let count = AggregateSpec {
        func: AggFunc::Count,
        column: None,
        output: "c".into(),
    };
    let t = FlightTicket {
        columns: Some(vec![]),
        ..agg_ticket(vec![], vec![count])
    };
    ScanSpec::from_ticket(&t, &simple_schema())
        .expect("the aggregation defines the output columns; the projection is not consulted");
}

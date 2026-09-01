//! Unit tests for [`FlightTicket`](super::FlightTicket) — wire parsing and
//! defaults, validation, the v1/v2 effective-filter reconciliation, and
//! `token_in_range` membership (including #3634's derived wrapping) — split out
//! of `ticket.rs` to keep that file under the campsite file-size target (epic
//! #1116 source / #1135 tests), following this crate's existing
//! `<module>_tests.rs` convention (`filter_tests.rs`, `bypass_tests.rs`,
//! `statics_tests.rs`, ...).

use super::*;

use serde_json::json;

fn minimal_json() -> Vec<u8> {
    json!({
        "keyspace": "test_basic",
        "table": "simple_table",
        "ddl": "CREATE TABLE test_basic.simple_table (id uuid PRIMARY KEY, name text)"
    })
    .to_string()
    .into_bytes()
}

#[test]
fn parses_minimal_ticket_with_defaults() {
    let t = FlightTicket::from_bytes(&minimal_json()).expect("parse");
    assert_eq!(t.keyspace, "test_basic");
    assert_eq!(t.table, "simple_table");
    assert!(t.ddl.starts_with("CREATE TABLE"));
    // Everything optional defaults sensibly.
    assert_eq!(t.snapshot, None);
    assert_eq!(t.token_start, None);
    assert_eq!(t.token_end, None);
    assert!(!t.wraparound);
    assert_eq!(t.columns, None);
    assert!(t.predicates.is_empty());
}

#[test]
fn round_trips_full_ticket() {
    let ticket = FlightTicket {
        version: TICKET_VERSION,
        keyspace: "ks".into(),
        table: "tbl".into(),
        ddl: "CREATE TABLE ks.tbl (pk int PRIMARY KEY, v int)".into(),
        snapshot: Some("cqlite-abc".into()),
        token_start: Some(-100),
        token_end: Some(100),
        wraparound: false,
        columns: Some(vec!["pk".into(), "v".into()]),
        predicates: vec![Predicate {
            column: "v".into(),
            op: PredicateOp::Gt,
            value: json!(10),
        }],
        filter: None,
        aggregation: None,
        limit: Some(5),
    };
    let bytes = ticket.to_bytes().expect("serialize");
    let back = FlightTicket::from_bytes(&bytes).expect("parse");
    assert_eq!(ticket, back);
}

// ---- Issue #2129: LIMIT pushdown wire format + compat ----

/// A ticket carrying `limit` parses it, and one omitting it defaults to
/// `None` (no bound → full scan). The field is plain JSON so the Java
/// connector emits `"limit": <n>`.
#[test]
fn limit_field_parses_and_defaults_to_none() {
    let with_limit = json!({
        "keyspace": "k", "table": "t",
        "ddl": "CREATE TABLE k.t (id int PRIMARY KEY)",
        "limit": 5
    })
    .to_string();
    let t = FlightTicket::from_bytes(with_limit.as_bytes()).expect("parse");
    assert_eq!(t.limit, Some(5));

    // Absent → None (an old connector's ticket on a new server).
    let t = FlightTicket::from_bytes(&minimal_json()).expect("parse");
    assert_eq!(t.limit, None);

    // limit: 0 is a distinct, valid value (SELECT ... LIMIT 0).
    let zero = json!({
        "keyspace": "k", "table": "t",
        "ddl": "CREATE TABLE k.t (id int PRIMARY KEY)",
        "limit": 0
    })
    .to_string();
    let t = FlightTicket::from_bytes(zero.as_bytes()).expect("parse");
    assert_eq!(t.limit, Some(0));
}

/// Compat posture (issue #2129): the ticket is NOT `deny_unknown_fields`, so
/// an UNKNOWN field (e.g. a future connector's addition seen by an older
/// server) is silently ignored rather than rejected. This proves the
/// additive-optional contract that makes `limit` safe to roll out in either
/// order (new connector ⇄ old server, old connector ⇄ new server).
#[test]
fn unknown_field_is_ignored_not_rejected() {
    let raw = json!({
        "keyspace": "k", "table": "t",
        "ddl": "CREATE TABLE k.t (id int PRIMARY KEY)",
        "limit": 7,
        "some_future_field": {"nested": [1, 2, 3]}
    })
    .to_string();
    let t =
        FlightTicket::from_bytes(raw.as_bytes()).expect("unknown field must not fail the parse");
    assert_eq!(t.limit, Some(7), "known fields still bind");
}

#[test]
fn rejects_malformed_json() {
    assert!(FlightTicket::from_bytes(b"not json").is_err());
    // Missing required field `ddl`.
    let missing = json!({"keyspace": "k", "table": "t"}).to_string();
    assert!(FlightTicket::from_bytes(missing.as_bytes()).is_err());
}

// ---- Issue #1430: path-traversal / absolute-path field rejection ----

#[test]
fn rejects_path_traversal_and_absolute_fields() {
    // Each malicious ticket must be rejected at parse time.
    let cases: Vec<serde_json::Value> = vec![
        json!({"keyspace": "a/../b", "table": "t", "ddl": "d"}),
        json!({"keyspace": "", "table": "t", "ddl": "d"}),
        json!({"keyspace": "/abs", "table": "t", "ddl": "d"}),
        json!({"keyspace": "k", "table": "../x", "ddl": "d"}),
        json!({"keyspace": "k", "table": "t", "ddl": "d", "snapshot": "../y"}),
        json!({"keyspace": "k", "table": "t", "ddl": "d", "snapshot": "/etc/passwd"}),
        json!({"keyspace": "k\u{0000}b", "table": "t", "ddl": "d"}),
        json!({"keyspace": "a.b", "table": "t", "ddl": "d"}),
    ];
    for c in cases {
        let raw = c.to_string();
        assert!(
            FlightTicket::from_bytes(raw.as_bytes()).is_err(),
            "malicious ticket must be rejected: {raw}"
        );
    }

    // A fully-valid ticket (identifier keyspace/table + hyphenated snapshot)
    // must still parse successfully.
    let ok = json!({
        "keyspace": "test_basic",
        "table": "simple_table",
        "ddl": "CREATE TABLE test_basic.simple_table (id uuid PRIMARY KEY, name text)",
        "snapshot": "cqlite-abc"
    })
    .to_string();
    assert!(
        FlightTicket::from_bytes(ok.as_bytes()).is_ok(),
        "valid ticket must still parse"
    );
}

#[test]
fn parses_in_predicate_with_array_value() {
    let raw = json!({
        "keyspace": "k", "table": "t", "ddl": "CREATE TABLE k.t (a int PRIMARY KEY)",
        "predicates": [{"column": "a", "op": "In", "value": [1, 2, 3]}]
    })
    .to_string();
    let t = FlightTicket::from_bytes(raw.as_bytes()).expect("parse");
    assert_eq!(t.predicates.len(), 1);
    assert_eq!(t.predicates[0].op, PredicateOp::In);
    assert_eq!(t.predicates[0].value, json!([1, 2, 3]));
}

// ---- Issue #834: PredicateExpr tree wire format ----

/// Each `PredicateExpr` variant serializes to its EXACT internally-tagged
/// JSON shape and round-trips back to the same value. The Java connector is
/// built to this contract, so the tags and field names must not drift.
#[test]
fn predicate_expr_json_shapes_round_trip() {
    let cases: Vec<(PredicateExpr, serde_json::Value)> = vec![
        (
            PredicateExpr::Compare {
                column: "c".into(),
                op: PredicateOp::Gt,
                value: json!(10),
            },
            json!({"type": "Compare", "column": "c", "op": "Gt", "value": 10}),
        ),
        (
            PredicateExpr::In {
                column: "c".into(),
                values: vec![json!(1), json!(2)],
            },
            json!({"type": "In", "column": "c", "values": [1, 2]}),
        ),
        (
            PredicateExpr::IsNull { column: "c".into() },
            json!({"type": "IsNull", "column": "c"}),
        ),
        (
            PredicateExpr::Not {
                expr: Box::new(PredicateExpr::IsNull { column: "c".into() }),
            },
            json!({"type": "Not", "expr": {"type": "IsNull", "column": "c"}}),
        ),
        (
            PredicateExpr::And {
                exprs: vec![PredicateExpr::IsNull { column: "a".into() }],
            },
            json!({"type": "And", "exprs": [{"type": "IsNull", "column": "a"}]}),
        ),
        (
            PredicateExpr::Or {
                exprs: vec![PredicateExpr::IsNull { column: "b".into() }],
            },
            json!({"type": "Or", "exprs": [{"type": "IsNull", "column": "b"}]}),
        ),
    ];
    for (expr, expected_json) in cases {
        let serialized = serde_json::to_value(&expr).expect("serialize");
        assert_eq!(serialized, expected_json, "JSON shape for {expr:?}");
        let back: PredicateExpr = serde_json::from_value(expected_json).expect("parse");
        assert_eq!(back, expr, "round-trip for {expr:?}");
    }
}

/// A nested tree mixing AND/OR/NOT round-trips through ticket JSON bytes.
#[test]
fn v2_ticket_with_filter_parses() {
    let raw = json!({
        "version": 2,
        "keyspace": "k", "table": "t",
        "ddl": "CREATE TABLE k.t (a int PRIMARY KEY, b text)",
        "filter": {
            "type": "Or",
            "exprs": [
                {"type": "And", "exprs": [
                    {"type": "Compare", "column": "a", "op": "Gt", "value": 10},
                    {"type": "Compare", "column": "b", "op": "Equal", "value": "x"}
                ]},
                {"type": "Not", "expr": {"type": "IsNull", "column": "b"}}
            ]
        }
    })
    .to_string();
    let t = FlightTicket::from_bytes(raw.as_bytes()).expect("parse");
    assert_eq!(t.version, 2);
    let filter = t.filter.as_ref().expect("filter present");
    assert!(matches!(filter, PredicateExpr::Or { exprs } if exprs.len() == 2));
    // `effective_filter` returns the v2 tree verbatim when present.
    assert_eq!(t.effective_filter().as_ref(), Some(filter));
}

/// Back-compat: a v1 flat list folds to an `And` of leaves, with `In`
/// becoming its own node and everything else a `Compare`.
#[test]
fn v1_predicates_fold_to_and_of_leaves() {
    let t = FlightTicket {
        keyspace: "k".into(),
        table: "t".into(),
        predicates: vec![
            Predicate {
                column: "score".into(),
                op: PredicateOp::Gt,
                value: json!(10),
            },
            Predicate {
                column: "name".into(),
                op: PredicateOp::In,
                value: json!(["a", "b"]),
            },
        ],
        ..Default::default()
    };
    let folded = t.effective_filter().expect("non-empty predicates → Some");
    assert_eq!(
        folded,
        PredicateExpr::And {
            exprs: vec![
                PredicateExpr::Compare {
                    column: "score".into(),
                    op: PredicateOp::Gt,
                    value: json!(10),
                },
                PredicateExpr::In {
                    column: "name".into(),
                    values: vec![json!("a"), json!("b")],
                },
            ],
        }
    );
}

/// With neither `filter` nor `predicates`, there is no effective filter; and
/// when both are set, `filter` wins (predicates ignored).
#[test]
fn effective_filter_precedence_and_empty() {
    let empty = FlightTicket {
        keyspace: "k".into(),
        table: "t".into(),
        ..Default::default()
    };
    assert_eq!(empty.effective_filter(), None);

    let both = FlightTicket {
        keyspace: "k".into(),
        table: "t".into(),
        predicates: vec![Predicate {
            column: "ignored".into(),
            op: PredicateOp::Equal,
            value: json!(1),
        }],
        filter: Some(PredicateExpr::IsNull { column: "c".into() }),
        ..Default::default()
    };
    assert_eq!(
        both.effective_filter(),
        Some(PredicateExpr::IsNull { column: "c".into() }),
        "filter is authoritative; predicates ignored"
    );
}

// ---- Issue #841: aggregation pushdown wire format ----

/// The aggregation JSON shape the Java connector emits round-trips exactly:
/// variant-name `func`, `column: null` for `count(*)`, and ordered outputs.
#[test]
fn aggregation_json_shape_round_trips() {
    let agg = Aggregation {
        group_by: vec!["c1".into()],
        aggregates: vec![
            AggregateSpec {
                func: AggFunc::Count,
                column: None,
                output: "agg0".into(),
            },
            AggregateSpec {
                func: AggFunc::Count,
                column: Some("x".into()),
                output: "agg1".into(),
            },
            AggregateSpec {
                func: AggFunc::Sum,
                column: Some("x".into()),
                output: "agg2".into(),
            },
            AggregateSpec {
                func: AggFunc::Min,
                column: Some("x".into()),
                output: "agg3".into(),
            },
            AggregateSpec {
                func: AggFunc::Max,
                column: Some("x".into()),
                output: "agg4".into(),
            },
        ],
    };
    let expected = json!({
        "group_by": ["c1"],
        "aggregates": [
            {"func": "Count", "column": null, "output": "agg0"},
            {"func": "Count", "column": "x", "output": "agg1"},
            {"func": "Sum", "column": "x", "output": "agg2"},
            {"func": "Min", "column": "x", "output": "agg3"},
            {"func": "Max", "column": "x", "output": "agg4"}
        ]
    });
    assert_eq!(serde_json::to_value(&agg).unwrap(), expected);
    let back: Aggregation = serde_json::from_value(expected).unwrap();
    assert_eq!(back, agg);
}

/// A full ticket carrying an aggregation parses from its JSON bytes.
#[test]
fn ticket_with_aggregation_parses() {
    let raw = json!({
        "keyspace": "k", "table": "t",
        "ddl": "CREATE TABLE k.t (id int PRIMARY KEY, x int)",
        "aggregation": {
            "group_by": [],
            "aggregates": [{"func": "Count", "column": null, "output": "agg0"}]
        }
    })
    .to_string();
    let t = FlightTicket::from_bytes(raw.as_bytes()).expect("parse");
    let agg = t.aggregation.as_ref().expect("aggregation present");
    assert!(agg.group_by.is_empty());
    assert_eq!(agg.aggregates.len(), 1);
    assert_eq!(agg.aggregates[0].func, AggFunc::Count);
    assert_eq!(agg.aggregates[0].column, None);
    assert_eq!(agg.aggregates[0].output, "agg0");
}

/// A ticket without an `aggregation` field defaults it to `None`.
#[test]
fn absent_aggregation_defaults_to_none() {
    let t = FlightTicket::from_bytes(&minimal_json()).expect("parse");
    assert_eq!(t.aggregation, None);
}

fn ticket_with_range(start: Option<i64>, end: Option<i64>, wrap: bool) -> FlightTicket {
    FlightTicket {
        keyspace: "k".into(),
        table: "t".into(),
        token_start: start,
        token_end: end,
        wraparound: wrap,
        ..Default::default()
    }
}

#[test]
fn no_bounds_accepts_every_token() {
    let t = ticket_with_range(None, None, false);
    assert!(t.token_in_range(i64::MIN));
    assert!(t.token_in_range(0));
    assert!(t.token_in_range(i64::MAX));
}

#[test]
fn normal_range_is_exclusive_start_inclusive_end() {
    let t = ticket_with_range(Some(-100), Some(100), false);
    assert!(!t.token_in_range(-100), "start is exclusive");
    assert!(t.token_in_range(-99));
    assert!(t.token_in_range(0));
    assert!(t.token_in_range(100), "end is inclusive");
    assert!(!t.token_in_range(101));
}

/// #3634: `token_in_range` must DERIVE wrapping from its own endpoints and
/// ignore the `wraparound` wire flag entirely.
///
/// The flag is `#[serde(default)]` and `validate()` never checks it against
/// `token_start`/`token_end`, so both inconsistent shapes are client-reachable
/// — `start > end` with the flag false (reached by merely OMITTING it) and
/// `start < end` with it true. Cassandra can express neither, because
/// `Range.isWrapAround(left, right)` IS `left.compareTo(right) >= 0`.
///
/// This is public API, and the server filters with a derived bound, so a
/// flag-trusting answer here would DISAGREE with the rows the scan returns.
/// Both shapes are pinned in BOTH flag directions, so the verdict is proved
/// independent of the flag rather than merely correct for one value of it.
#[test]
fn token_in_range_derives_wrapping_and_ignores_the_wire_flag() {
    for wrap in [true, false] {
        // start > end: wraps, whatever the flag says.
        let t = ticket_with_range(Some(100), Some(-100), wrap);
        for token in [i64::MIN, -200, -100, 200, i64::MAX] {
            assert!(
                t.token_in_range(token),
                "(100, -100] wraps (flag={wrap}), so {token} is in range"
            );
        }
        for token in [-99, 0, 99, 100] {
            assert!(
                !t.token_in_range(token),
                "(100, -100] excludes its gap (flag={wrap}); {token}"
            );
        }

        // start < end: does NOT wrap, whatever the flag says — so it must not
        // become the outer-ring superset a trusted `true` flag would produce.
        let t = ticket_with_range(Some(10), Some(20), wrap);
        assert!(!t.token_in_range(10), "start exclusive (flag={wrap})");
        assert!(t.token_in_range(11), "flag={wrap}");
        assert!(t.token_in_range(20), "end inclusive (flag={wrap})");
        assert!(
            !t.token_in_range(21),
            "(10, 20] must not admit 21 (flag={wrap})"
        );
        assert!(
            !t.token_in_range(i64::MIN),
            "(10, 20] must not admit the outer ring (flag={wrap})"
        );
    }
}

/// #3634: the whole point of deriving is that the THREE membership surfaces
/// agree. `token_in_range` (this public API), the producer's `TokenFilter`
/// and the core `ScanTokenBound` must give the SAME verdict for the same
/// ticket — including for a ticket whose flag contradicts its endpoints,
/// which is exactly where they used to diverge.
#[test]
fn token_in_range_agrees_with_the_producer_filter_on_inconsistent_flags() {
    let shapes = [
        (Some(100), Some(-100)),
        (Some(10), Some(20)),
        (Some(42), Some(42)),
    ];
    let probes = [
        i64::MIN,
        -1000,
        -100,
        -99,
        0,
        9,
        10,
        11,
        20,
        21,
        42,
        43,
        99,
        100,
        101,
        1000,
        i64::MAX,
    ];
    for (start, end) in shapes {
        for wrap in [true, false] {
            let t = ticket_with_range(start, end, wrap);
            let s = start.unwrap_or(i64::MIN);
            let e = end.unwrap_or(i64::MAX);
            for token in probes {
                // The derived rule, straight from `Range.contains` at the
                // pinned tag — not from this crate's prior behaviour.
                let expected = if s >= e {
                    token > s || token <= e
                } else {
                    token > s && token <= e
                };
                assert_eq!(
                    t.token_in_range(token),
                    expected,
                    "({s}, {e}] flag={wrap} token={token}: token_in_range must \
                     match Range.contains, so it cannot disagree with the \
                     producer filter or the core bound"
                );
            }
        }
    }
}

#[test]
fn wraparound_range_accepts_either_side() {
    // Segment crossing the ring boundary: tokens > 100 OR <= -100.
    let t = ticket_with_range(Some(100), Some(-100), true);
    assert!(t.token_in_range(200), "above start");
    assert!(t.token_in_range(-200), "at/below end");
    assert!(!t.token_in_range(0), "the gap is excluded");
    assert!(!t.token_in_range(100), "start still exclusive");
    assert!(t.token_in_range(-100), "end still inclusive");
}

#[test]
fn full_ring_range_with_equal_endpoints_accepts_all() {
    // #2228: a range whose endpoints are equal (`(T, T]`) denotes the FULL
    // ring, matching Cassandra's convention — NOT the empty set. The
    // Sidecar/split planner may emit this for single-token topologies. It
    // must accept every token regardless of the `wraparound` flag, so guard
    // both flag states here.
    for wrap in [true, false] {
        let t = ticket_with_range(Some(42), Some(42), wrap);
        assert!(t.token_in_range(i64::MIN), "MIN accepted (wrap={wrap})");
        assert!(t.token_in_range(i64::MAX), "MAX accepted (wrap={wrap})");
        assert!(t.token_in_range(42), "T itself accepted (wrap={wrap})");
        assert!(t.token_in_range(41), "T-1 accepted (wrap={wrap})");
        assert!(t.token_in_range(43), "T+1 accepted (wrap={wrap})");
        assert!(
            t.token_in_range(0),
            "arbitrary token accepted (wrap={wrap})"
        );
    }
}

#[test]
fn open_ended_bounds_default_to_min_max() {
    // A defaulted `start` is i64::MIN and stays exclusive, matching the
    // uniform (start, end] convention. Real Murmur3 tokens are never
    // i64::MIN (it is the ring's sentinel), so excluding it loses no data.
    let only_end = ticket_with_range(None, Some(0), false);
    assert!(
        !only_end.token_in_range(i64::MIN),
        "defaulted start is exclusive"
    );
    assert!(only_end.token_in_range(i64::MIN + 1));
    assert!(only_end.token_in_range(0));
    assert!(!only_end.token_in_range(1));

    let only_start = ticket_with_range(Some(0), None, false);
    assert!(!only_start.token_in_range(0));
    assert!(only_start.token_in_range(1));
    assert!(
        only_start.token_in_range(i64::MAX),
        "defaulted end is inclusive"
    );
}

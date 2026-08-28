//! The bench harness's own contract: what it parses, runs and reports.

use super::support::*;

// ---------------------------------------------------------------------------
// `rows_result` provenance — a projected VALUE is never a row COUNT
// ---------------------------------------------------------------------------

#[test]
fn only_the_count_scenarios_are_scalar_aggregates() {
    // The DataFusion arm decides whether to read `rows_result` out of a returned
    // scalar from THIS predicate, i.e. from the query it asked for. Deciding it
    // from the batch SHAPE instead is what let a 1-row/1-column projected batch
    // be reported as a row count.
    assert!(ScenarioKind::FullScanCount.is_scalar_aggregate());
    assert!(ScenarioKind::FilteredScan.is_scalar_aggregate());
    assert!(
        !ScenarioKind::ProjectedScan.is_scalar_aggregate(),
        "a projected scan returns ROWS; its result count is the number drained"
    );
}

#[test]
#[serial]
fn a_single_column_projected_scan_reports_rows_drained_not_a_cell_value() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        let runner = BenchRunner::new(BenchConfig {
            dir: table_dir.clone(),
            schema: schema.clone(),
            // One row per batch: the shape that made the superseded
            // batch-shape test misclassify a projected batch as a scalar.
            batch_size: 1,
            projection: vec!["val".to_string()],
            filter_column: "ck".to_string(),
            filter_op: RowOp::Lt,
            filter_value: RowLiteral::parse("c"),
            iterations: 1,
            df_target_partitions: Some(1),
        });
        let outcome = runner
            .run_one(
                Scenario {
                    kind: ScenarioKind::ProjectedScan,
                    arm: ArmKind::DataFusion,
                },
                1,
            )
            .expect("the projected DataFusion arm runs");
        // 7 rows survive reconciliation, so 7 rows are drained — never `1` (a
        // retained single-row batch) and never a `val` cell value (10/21/30/...).
        assert_eq!(
            outcome.rows_result, 7,
            "rows_result must be the rows the query returned, not a cell value"
        );
        assert_eq!(
            outcome.rows_scanned, 7,
            "the scan feeds all reconciled rows"
        );
    });
}

// ---------------------------------------------------------------------------
// The harness's CLI contract
//
// These are the surfaces the bench binary parses operator input through, and a
// mistake in any of them silently produces a plausible number for the WRONG
// query — an operator/value pair that means something other than what was typed.
// ---------------------------------------------------------------------------

#[test]
fn row_op_spellings_parse_to_the_operator_they_name() {
    use crate::df_spike::rowwise::RowOp;
    for (spellings, expected, sql) in [
        (["eq", "="].as_slice(), RowOp::Eq, "="),
        (["ne", "neq", "!=", "<>"].as_slice(), RowOp::NotEq, "<>"),
        (["lt", "<"].as_slice(), RowOp::Lt, "<"),
        (["lte", "le", "<="].as_slice(), RowOp::LtEq, "<="),
        (["gt", ">"].as_slice(), RowOp::Gt, ">"),
        (["gte", "ge", ">="].as_slice(), RowOp::GtEq, ">="),
    ] {
        for spelling in spellings {
            assert_eq!(
                RowOp::parse(spelling),
                Some(expected),
                "spelling '{spelling}' must parse to {expected:?}"
            );
        }
        // The SQL rendering must be the SAME operator, or the DataFusion arm
        // would run a different query than the row arm evaluates.
        assert_eq!(expected.sql(), sql);
    }
    // An unrecognized operator is REFUSED, never defaulted — a default would
    // silently run a different comparison than the operator asked for.
    assert_eq!(RowOp::parse("approximately"), None);
    assert_eq!(RowOp::parse(""), None);
}

#[test]
fn row_literal_parsing_prefers_the_narrowest_type_and_escapes_sql() {
    use crate::df_spike::rowwise::RowLiteral;
    assert!(matches!(RowLiteral::parse("true"), RowLiteral::Bool(true)));
    assert!(matches!(
        RowLiteral::parse("false"),
        RowLiteral::Bool(false)
    ));
    assert!(matches!(RowLiteral::parse("-7"), RowLiteral::Int(-7)));
    assert!(matches!(RowLiteral::parse("1.5"), RowLiteral::Float(_)));
    assert!(matches!(RowLiteral::parse("abc"), RowLiteral::Text(_)));
    // A non-finite float has no JSON number form and no useful comparison, so it
    // must fall through to text rather than become a `Float(NaN)` that compares
    // false against everything.
    assert!(matches!(RowLiteral::parse("NaN"), RowLiteral::Text(_)));
    assert!(matches!(RowLiteral::parse("inf"), RowLiteral::Text(_)));

    // SQL string literals: the quote must be doubled, or an operand containing
    // one would terminate the literal and change the query.
    assert_eq!(RowLiteral::parse("O'Hara").sql(), "'O''Hara'");
    assert_eq!(RowLiteral::parse("7").sql(), "7");
    assert_eq!(RowLiteral::parse("true").sql(), "true");

    // The JSON form is what production lowering coerces against the CQL type.
    assert_eq!(RowLiteral::parse("7").json(), serde_json::json!(7));
    assert_eq!(RowLiteral::parse("x").json(), serde_json::json!("x"));
    assert_eq!(RowLiteral::parse("true").json(), serde_json::json!(true));
}

#[test]
fn scenario_and_arm_identifiers_round_trip() {
    use crate::df_spike::{ArmKind, ScenarioKind};
    for scenario in ScenarioKind::all() {
        assert_eq!(ScenarioKind::parse(scenario.id()), Some(scenario));
    }
    for arm in ArmKind::all() {
        assert_eq!(ArmKind::parse(arm.id()), Some(arm));
    }
    // Unknown identifiers are refused so a typo cannot silently select a
    // different scenario/arm than the one named on the command line.
    assert_eq!(ScenarioKind::parse("full_scan"), None);
    assert_eq!(ArmKind::parse("data_fusion"), None);
    // The ids are distinct, or two cells would overwrite each other's results.
    let scenario_ids: std::collections::BTreeSet<_> =
        ScenarioKind::all().iter().map(|s| s.id()).collect();
    assert_eq!(scenario_ids.len(), ScenarioKind::all().len());
    let arm_ids: std::collections::BTreeSet<_> = ArmKind::all().iter().map(|a| a.id()).collect();
    assert_eq!(arm_ids.len(), ArmKind::all().len());
}

#[test]
fn the_datafusion_sql_matches_the_scenario_and_the_row_arms_filter() {
    use crate::df_spike::bench::{BenchConfig, BenchRunner};
    use crate::df_spike::rowwise::{RowLiteral, RowOp};
    let runner = BenchRunner::new(BenchConfig {
        dir: std::path::PathBuf::from("/nonexistent"),
        schema: testutil::clustering_schema(),
        batch_size: TEST_BATCH_SIZE,
        projection: vec!["ck".into(), "val".into()],
        filter_column: "val".into(),
        filter_op: RowOp::Gt,
        filter_value: RowLiteral::parse("25"),
        iterations: 1,
        df_target_partitions: Some(1),
    });
    assert_eq!(
        runner.sql_for(crate::df_spike::ScenarioKind::FullScanCount),
        "SELECT count(*) FROM t"
    );
    // Column names are quoted, so a column whose name collides with a SQL
    // keyword still resolves to the column.
    assert_eq!(
        runner.sql_for(crate::df_spike::ScenarioKind::ProjectedScan),
        "SELECT \"ck\", \"val\" FROM t"
    );
    // The predicate rendered for DataFusion must be the SAME comparison the
    // row-wise arm evaluates (operator AND operand), or the two arms would be
    // answering different questions and the delta would be meaningless.
    assert_eq!(
        runner.sql_for(crate::df_spike::ScenarioKind::FilteredScan),
        "SELECT count(*) FROM t WHERE \"val\" > 25"
    );
}

#[test]
fn the_recorded_datafusion_parallelism_is_resolved_not_left_as_default() {
    use crate::df_spike::bench::{BenchConfig, BenchRunner};
    use crate::df_spike::rowwise::{RowLiteral, RowOp};
    let config = |tp| BenchConfig {
        dir: std::path::PathBuf::from("/nonexistent"),
        schema: testutil::clustering_schema(),
        batch_size: TEST_BATCH_SIZE,
        projection: vec!["ck".into()],
        filter_column: "val".into(),
        filter_op: RowOp::Gt,
        filter_value: RowLiteral::parse("1"),
        iterations: 1,
        df_target_partitions: tp,
    };
    assert_eq!(
        BenchRunner::new(config(Some(3))).effective_df_partitions(),
        3
    );
    // With no pin the RESOLVED core count is recorded, not "default": a results
    // file read on another machine must still state the parallelism the number
    // was produced with, or the tp1-vs-default comparison is unreadable.
    let resolved = BenchRunner::new(config(None)).effective_df_partitions();
    assert_eq!(
        resolved,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );
    assert!(resolved >= 1);
}

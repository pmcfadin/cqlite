//! Issue #4201, spec R6.1 — `diff(A, B)` and `diff(B, A)` are label-swapped mirrors.
//!
//! The property is asserted GENERICALLY: every fixture the R1-R5 tests use is diffed both ways at
//! the same `now`, one report's `a`/`b` labels are swapped by a tree walk over its JSON (never by
//! knowledge of any one fixture), and the result must equal the other report. The mirror is built
//! from the report's own field names, so a label that is added to the report later but not to
//! [`relabel`] makes this test fail rather than pass vacuously.

#![cfg(feature = "write-support")]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/diff_staging.rs"]
mod diff_staging;

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::snapshot_diff::{diff_table, DiffOptions, DiffSide};
use diff_staging::*;
use serde_json::{Map, Value as Json};

fn swap_label(text: &str) -> String {
    match text {
        "a" => "b".into(),
        "b" => "a".into(),
        "only-a" => "only-b".into(),
        "only-b" => "only-a".into(),
        "newer-in-a" => "newer-in-b".into(),
        "newer-in-b" => "newer-in-a".into(),
        other => other.to_string(),
    }
}

/// Swap every `a`/`b` label in a report: object KEYS (`a`/`b`, `side_a`/`side_b`, `only_a`/
/// `only_b`, `newer_in_a`/`newer_in_b`, `tombstones_only_a`/`tombstones_only_b`) and the VALUES of
/// `status` and `side`. Nothing else changes.
fn relabel(value: &Json) -> Json {
    match value {
        Json::Object(map) => {
            let mut out = Map::new();
            for (key, inner) in map {
                let new_key = match key.as_str() {
                    "a" => "b",
                    "b" => "a",
                    "side_a" => "side_b",
                    "side_b" => "side_a",
                    "only_a" => "only_b",
                    "only_b" => "only_a",
                    "newer_in_a" => "newer_in_b",
                    "newer_in_b" => "newer_in_a",
                    "tombstones_only_a" => "tombstones_only_b",
                    "tombstones_only_b" => "tombstones_only_a",
                    other => other,
                };
                let new_value = match (key.as_str(), inner) {
                    ("status" | "side" | "resolves_to", Json::String(text)) => {
                        Json::String(swap_label(text))
                    }
                    _ => relabel(inner),
                };
                out.insert(new_key.to_string(), new_value);
            }
            Json::Object(out)
        }
        Json::Array(items) => Json::Array(items.iter().map(relabel).collect()),
        other => other.clone(),
    }
}

async fn report(a: &DiffSide, b: &DiffSide, table: &str, schema: &TableSchema, now: i64) -> Json {
    let options = DiffOptions {
        now_secs: now,
        max_result_bytes: 64 << 20,
    };
    serde_json::to_value(
        diff_table(a, b, table, schema, None, &options)
            .await
            .unwrap_or_else(|error| panic!("{table}: {error}")),
    )
    .unwrap()
}

/// Diff `(a, b)` and `(b, a)` and require the mirror property. Returns the A-vs-B report so the
/// caller can assert the fixture actually produced divergence (a mirror of two empty reports is
/// trivially equal).
async fn assert_mirror(
    a: &DiffSide,
    b: &DiffSide,
    table: &str,
    schema: &TableSchema,
    now: i64,
) -> Json {
    let forward = report(a, b, table, schema, now).await;
    let backward = report(b, a, table, schema, now).await;
    assert_eq!(
        relabel(&forward),
        backward,
        "{table} @ now={now}: diff(B, A) must be the label-swapped mirror of diff(A, B)"
    );
    // The mirror also holds for the phase: only the labels move.
    assert_eq!(
        forward["summary"]["partitions_compared"],
        backward["summary"]["partitions_compared"]
    );
    forward
}

#[tokio::test]
async fn mirror_holds_for_resurrection_gc_positive_either_side_of_the_boundary() {
    let Some(src) = fixture_dir("test_tomb", "resurrection_gc_positive") else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let a = side_of(&stage(root.path(), "a", &src, &["nb-1-big-", "nb-2-big-"]));
    let b = side_of(&stage(root.path(), "b", &src, &["nb-1-big-"]));
    let schema = load_schema(
        "tombstone-parity.cql",
        "test_tomb",
        "resurrection_gc_positive",
    );
    for now in [GC_POSITIVE_BOUNDARY, GC_POSITIVE_BOUNDARY + 1] {
        let forward =
            assert_mirror(&a, &b, "test_tomb.resurrection_gc_positive", &schema, now).await;
        assert_eq!(
            forward["summary"]["tombstones_only_a"], 3,
            "non-vacuous: real divergence"
        );
    }
}

#[tokio::test]
async fn mirror_holds_for_risk_cases_including_newer_in_either_side() {
    let src = risk_cases_dir();
    let root = tempfile::tempdir().unwrap();
    let a = side_of(&stage(root.path(), "a", &src, &["nb-2-big-"]));
    let b = side_of(&stage(root.path(), "b", &src, &["nb-1-big-"]));
    let schema = load_schema("issue-4201-diff-risk.cql", "test_diff", "risk_cases");
    let forward = assert_mirror(&a, &b, "test_diff.risk_cases", &schema, 1_796_167_685).await;
    assert_eq!(forward["summary"]["newer_in_a"], 1);
    assert_eq!(forward["summary"]["newer_in_b"], 1);
    // In the mirror the tombstones belong to side b.
    let backward = report(&b, &a, "test_diff.risk_cases", &schema, 1_796_167_685).await;
    assert_eq!(backward["summary"]["tombstones_only_b"], 7);
    assert_eq!(
        forward["summary"]["conflicts"], 5,
        "non-vacuous: conflicts mirror with resolves_to flipped"
    );
    assert_eq!(backward["summary"]["conflicts"], 5);
    assert_eq!(backward["summary"]["tombstones_only_a"], 0);
}

#[tokio::test]
async fn mirror_holds_for_every_tombstone_kind_on_the_decision_trail_fixture() {
    let src = trace_decisions_dir();
    let root = tempfile::tempdir().unwrap();
    let a = side_of(&stage(root.path(), "a", &src, &["nb-1-big-", "nb-2-big-"]));
    let b = side_of(&stage(root.path(), "b", &src, &["nb-1-big-"]));
    let schema = load_schema("explain-trace.cql", "test_explain", "trace_decisions");
    let forward = assert_mirror(
        &a,
        &b,
        "test_explain.trace_decisions",
        &schema,
        1_795_399_358,
    )
    .await;
    assert_eq!(
        forward["summary"]["tombstones_only_a"], 6,
        "non-vacuous: real divergence"
    );
}

#[tokio::test]
async fn mirror_holds_for_identical_snapshots_and_the_report_is_unchanged() {
    let src = trace_decisions_dir();
    let root = tempfile::tempdir().unwrap();
    let a = side_of(&stage(root.path(), "a", &src, &["nb-1-big-", "nb-2-big-"]));
    let b = side_of(&stage(root.path(), "b", &src, &["nb-1-big-", "nb-2-big-"]));
    let schema = load_schema("explain-trace.cql", "test_explain", "trace_decisions");
    let forward = assert_mirror(
        &a,
        &b,
        "test_explain.trace_decisions",
        &schema,
        1_795_399_358,
    )
    .await;
    assert_eq!(forward["summary"]["only_a"], 0);
    assert_eq!(forward["summary"]["only_b"], 0);
}

#[test]
fn relabel_is_an_involution_and_swaps_every_label_family() {
    let sample = serde_json::json!({
        "side_a": {"dir": "x"}, "side_b": {"dir": "y"},
        "partitions": [{"cells": [{"status": "conflict", "resolves_to": "a", "a": {"value": "1"}, "b": null}],
                        "tombstones": [{"side": "a", "resurrection_risk":
                            {"shadowed_cells": [{"side": "b"}]}}]}],
        "summary": {"only_a": 1, "only_b": 2, "newer_in_a": 3, "newer_in_b": 4,
                    "tombstones_only_a": 5, "tombstones_only_b": 6}
    });
    let swapped = relabel(&sample);
    assert_eq!(swapped["side_a"]["dir"], "y");
    assert_eq!(swapped["partitions"][0]["cells"][0]["resolves_to"], "b");
    assert_eq!(swapped["partitions"][0]["cells"][0]["b"]["value"], "1");
    assert_eq!(swapped["partitions"][0]["tombstones"][0]["side"], "b");
    assert_eq!(
        swapped["partitions"][0]["tombstones"][0]["resurrection_risk"]["shadowed_cells"][0]["side"],
        "a"
    );
    assert_eq!(swapped["summary"]["only_a"], 2);
    assert_eq!(swapped["summary"]["tombstones_only_b"], 5);
    assert_eq!(relabel(&swapped), sample);
}

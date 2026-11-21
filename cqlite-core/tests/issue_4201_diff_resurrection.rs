//! Issue #4201 — `cqlite diff`: replica divergence and resurrection risk (spec
//! `snapshot-diff-scan` R1-R5, R7; issue AC 1 and 2).
//!
//! Every expected value is derived from Cassandra, never from `diff_table`'s own output (#3041,
//! #3042):
//!
//!   * `test_tomb.resurrection_gc_positive` (datasets-v4, Cassandra 5.0.8; issue AC 1): side A =
//!     both generations, side B = generation 1 only. Expectations are read from the committed
//!     `sstabledump` goldens (`*-Data.db.jsonl`), pinned by [`goldens_anchor_the_literals`].
//!   * `test_diff.risk_cases` (committed, Cassandra 5.0.8; `test-data/fixtures/issue_4201/README.md`):
//!     pinned `USING TIMESTAMP` mutations whose outcomes follow from Cassandra's shadow rule.
//!   * `test_explain.trace_decisions` (committed, issue #4193): partition 3 holds a range tombstone
//!     `[1,2]` @2000 over a cell inside the span and one outside it (R5.1).
//!
//! Cassandra rules (`git show cassandra-5.0.8:<path>`):
//!   * `db/DeletionTime.java:173-175` — a cell is deleted iff `timestamp <= markedForDeleteAt`
//!     (a tie goes to the tombstone, `db/rows/Cells.java:92-98`);
//!   * `db/ColumnFamilyStore.java:1967-1970` + `db/partitions/PurgeFunction.java:41-42` —
//!     `gcBefore = now - gc_grace_seconds`; purgeable iff `localDeletionTime < gcBefore` (strict);
//!   * `db/Slice.java:181-184` + `db/ClusteringPrefix.java:70-79` — range containment.

#![cfg(feature = "write-support")]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/diff_staging.rs"]
mod diff_staging;

use std::collections::BTreeSet;

use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::merge::trace::{RecordingSink, Verdict};
use cqlite_core::storage::write_engine::merge::{
    build_traced_merger_with_registry, compute_gc_before, effective_compaction_schema, MergeStep,
};
use cqlite_core::storage::write_engine::snapshot_diff::{
    diff_partition, diff_partitions, diff_table, DiffError, DiffOptions, DiffSide, Side,
};
use cqlite_core::util::value_fmt::ValueFormatter;
use diff_staging::*;
use serde_json::Value as Json;

const BUDGET: u64 = 64 << 20;

fn options(now_secs: i64) -> DiffOptions {
    DiffOptions {
        now_secs,
        max_result_bytes: BUDGET,
    }
}

/// `(A, B)` for `resurrection_gc_positive`: A = both generations, B = generation 1 only (AC 1).
/// `None` (SKIP-clean, or a panic under `CQLITE_REQUIRE_FIXTURES=1`) when the fetched dataset is
/// absent.
fn gc_positive(root: &std::path::Path) -> Option<(DiffSide, DiffSide)> {
    let src = fixture_dir("test_tomb", "resurrection_gc_positive")?;
    let a = stage(root, "gc-a", &src, &["nb-1-big-", "nb-2-big-"]);
    let b = stage(root, "gc-b", &src, &["nb-1-big-"]);
    Some((side_of(&a), side_of(&b)))
}

fn gc_positive_schema() -> cqlite_core::schema::TableSchema {
    load_schema(
        "tombstone-parity.cql",
        "test_tomb",
        "resurrection_gc_positive",
    )
}

async fn gc_positive_report(sides: &(DiffSide, DiffSide), now: i64) -> Json {
    let report = diff_table(
        &sides.0,
        &sides.1,
        "test_tomb.resurrection_gc_positive",
        &gc_positive_schema(),
        None,
        &options(now),
    )
    .await
    .expect("diff_table over resurrection_gc_positive");
    serde_json::to_value(report).expect("report is JSON")
}

/// `(A, B)` for `risk_cases`: A = generation 2 (tombstones, "a" cells), B = generation 1.
fn risk_sides(root: &std::path::Path) -> (DiffSide, DiffSide) {
    let src = risk_cases_dir();
    (
        side_of(&stage(root, "risk-a", &src, &["nb-2-big-"])),
        side_of(&stage(root, "risk-b", &src, &["nb-1-big-"])),
    )
}

fn risk_schema() -> cqlite_core::schema::TableSchema {
    load_schema("issue-4201-diff-risk.cql", "test_diff", "risk_cases")
}

/// Tombstones' local deletion time, from `nb-2-big-Statistics.db.txt` ("SSTable min local
/// deletion time", written by Cassandra's own `sstablemetadata`).
const RISK_LDT: i64 = 1_795_305_334;
const RISK_BOUNDARY: i64 = RISK_LDT + 864_000;

async fn risk_report(sides: &(DiffSide, DiffSide), now: i64) -> Json {
    let report = diff_table(
        &sides.0,
        &sides.1,
        "test_diff.risk_cases",
        &risk_schema(),
        None,
        &options(now),
    )
    .await
    .expect("diff_table over risk_cases");
    serde_json::to_value(report).expect("report is JSON")
}

/// `(A, B)` for `trace_decisions`: A = both generations, B = generation 1 (R5.1, design §D6b).
fn trace_sides(root: &std::path::Path) -> (DiffSide, DiffSide) {
    let src = trace_decisions_dir();
    (
        side_of(&stage(root, "td-a", &src, &["nb-1-big-", "nb-2-big-"])),
        side_of(&stage(root, "td-b", &src, &["nb-1-big-"])),
    )
}

fn trace_schema() -> cqlite_core::schema::TableSchema {
    load_schema("explain-trace.cql", "test_explain", "trace_decisions")
}

/// README: B's tombstones have local deletion time 1794535357; gc_grace 864000.
const TD_BOUNDARY: i64 = 1_794_535_357 + 864_000;

async fn trace_report(sides: &(DiffSide, DiffSide), now: i64) -> Json {
    let report = diff_table(
        &sides.0,
        &sides.1,
        "test_explain.trace_decisions",
        &trace_schema(),
        None,
        &options(now),
    )
    .await
    .expect("diff_table over trace_decisions");
    serde_json::to_value(report).expect("report is JSON")
}

fn rfc3339_secs(text: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(text)
        .unwrap_or_else(|error| panic!("golden timestamp {text}: {error}"))
        .timestamp()
}

fn golden_partitions(dir: &std::path::Path, generation: &str) -> Vec<Json> {
    let path = dir.join(format!("{generation}-Data.db.jsonl"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()))
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("golden line is JSON"))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Anchors: the literals below are the goldens' own values.
// ---------------------------------------------------------------------------------------------

#[test]
fn goldens_anchor_the_literals() {
    // resurrection_gc_positive: gen 2's tombstones carry marked_deleted 2021-01-02T00:00:00Z and
    // local_delete_time 2026-11-11T00:53:44Z (= GC_POSITIVE_LDT); gen 1 is all live @2021-01-01.
    if let Some(src) = fixture_dir("test_tomb", "resurrection_gc_positive") {
        let g2 = golden_partitions(&src, "nb-2-big");
        let p1 = &g2[0];
        assert_eq!(p1["partition"]["key"][0], "1");
        let row = &p1["rows"][0];
        assert_eq!(row["clustering"][0], 2);
        assert_eq!(
            row["deletion_info"]["marked_deleted"],
            "2021-01-02T00:00:00Z"
        );
        assert_eq!(
            rfc3339_secs(row["deletion_info"]["local_delete_time"].as_str().unwrap()),
            GC_POSITIVE_LDT
        );
        let cell = &p1["rows"][1]["cells"][0];
        assert_eq!(cell["name"], "val");
        assert_eq!(
            rfc3339_secs(cell["deletion_info"]["local_delete_time"].as_str().unwrap()),
            GC_POSITIVE_LDT
        );
        let p2 = &g2[1];
        assert_eq!(p2["partition"]["key"][0], "2");
        assert_eq!(
            rfc3339_secs(
                p2["partition"]["deletion_info"]["local_delete_time"]
                    .as_str()
                    .unwrap()
            ),
            GC_POSITIVE_LDT
        );
        let stats = std::fs::read_to_string(src.join("nb-2-big-Statistics.db.txt")).unwrap();
        assert!(
            stats.contains(&format!("({GC_POSITIVE_LDT})")),
            "sstablemetadata must name local deletion time {GC_POSITIVE_LDT}"
        );
    }
    // risk_cases: gen 2's tombstones' local_delete_time is RISK_LDT.
    let src = risk_cases_dir();
    let stats = std::fs::read_to_string(src.join("nb-2-big-Statistics.db.txt")).unwrap();
    assert!(
        stats.contains(&format!("({RISK_LDT})")),
        "sstablemetadata local deletion time"
    );
    for partition in golden_partitions(&src, "nb-2-big") {
        let text = partition.to_string();
        if text.contains("local_delete_time") {
            for stamp in text.split("\"local_delete_time\":\"").skip(1) {
                let stamp = stamp.split('"').next().unwrap();
                assert_eq!(rfc3339_secs(stamp), RISK_LDT, "{text}");
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// R1.1 — each side is reconciled independently by the unmodified #4193 mechanism.
// ---------------------------------------------------------------------------------------------

/// Drive the `explain` sequence for ONE side and key, independently of `snapshot_diff`.
fn explain_trail(
    side: &DiffSide,
    key: &[u8],
    now: i64,
    schema: &cqlite_core::schema::TableSchema,
) -> RecordingSink {
    let effective = effective_compaction_schema(schema, &side.generations);
    let merger = build_traced_merger_with_registry(
        side.generations.clone(),
        &[key.to_vec()],
        &effective,
        None,
        ScanCancel::default(),
        RecordingSink::new(),
    )
    .expect("traced merger builds");
    let Some(merger) = merger else {
        return RecordingSink::new();
    };
    let mut merger = merger
        .with_now_secs(Some(now))
        .with_gc_before_secs(compute_gc_before(&effective, now))
        .with_purge_safe(true);
    while !matches!(merger.step().expect("step"), MergeStep::Complete) {}
    merger.into_trace_sink()
}

fn winner_set(sink: &RecordingSink) -> BTreeSet<(String, String, String)> {
    sink.cells()
        .iter()
        .filter(|cell| cell.verdict == Verdict::Winner)
        .map(|cell| {
            (
                cell.clustering
                    .as_ref()
                    .map(|ck| {
                        ck.columns
                            .iter()
                            .map(|(_, v)| ValueFormatter::format_value(v))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default(),
                cell.column.clone(),
                ValueFormatter::format_value(cell.value.as_ref().expect("winner has a value")),
            )
        })
        .collect()
}

fn reported_winners(report: &Json, key: &str, side: &str) -> BTreeSet<(String, String, String)> {
    partition(report, key)["cells"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| !c[side].is_null())
        .map(|c| {
            (
                c["clustering"].as_str().unwrap_or_default().to_string(),
                c["column"].as_str().unwrap().to_string(),
                c[side]["value"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn r1_1_both_sides_use_the_explain_mechanism_at_the_same_now() {
    let root = tempfile::tempdir().unwrap();
    let Some(sides) = gc_positive(root.path()) else {
        return;
    };
    let schema = gc_positive_schema();
    let now = GC_POSITIVE_BOUNDARY + 1;
    let report = gc_positive_report(&sides, now).await;
    for key in [1, 2] {
        let a = explain_trail(&sides.0, &int_key(key), now, &schema);
        let b = explain_trail(&sides.1, &int_key(key), now, &schema);
        let name = key.to_string();
        assert_eq!(
            reported_winners(&report, &name, "a"),
            winner_set(&a),
            "A winners, partition {key}"
        );
        assert_eq!(
            reported_winners(&report, &name, "b"),
            winner_set(&b),
            "B winners, partition {key}"
        );
        // B (generation 1 only) holds no tombstone at all; A's distinct tombstones are exactly the
        // report's one-sided tombstones (B holds none to cancel them).
        assert!(b.tombstones().is_empty(), "B has no tombstones");
        let distinct_a: BTreeSet<String> = a
            .tombstones()
            .iter()
            .map(|t| {
                format!(
                    "{:?}/{:?}/{}",
                    t.kind,
                    t.clustering,
                    t.column.clone().unwrap_or_default()
                )
            })
            .collect();
        let reported = partition(&report, &name)["tombstones"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(
            reported,
            distinct_a.len(),
            "partition {key}: one-sided tombstones == A's"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// R2 — cell statuses.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn r2_1_identical_snapshots_every_cell_equal() {
    let root = tempfile::tempdir().unwrap();
    let Some(src) = fixture_dir("test_tomb", "resurrection_gc_positive") else {
        return;
    };
    let a = side_of(&stage(
        root.path(),
        "same-a",
        &src,
        &["nb-1-big-", "nb-2-big-"],
    ));
    let b = side_of(&stage(
        root.path(),
        "same-b",
        &src,
        &["nb-1-big-", "nb-2-big-"],
    ));
    let report = diff_table(
        &a,
        &b,
        "test_tomb.resurrection_gc_positive",
        &gc_positive_schema(),
        None,
        &options(GC_POSITIVE_BOUNDARY + 1),
    )
    .await
    .unwrap();
    let json = serde_json::to_value(&report).unwrap();
    // Golden: partition 1 keeps clustering 1, 4, 5 (both columns) and `extra` at 3 = 7 live cells;
    // partition 2 is wholly deleted. Both partitions are compared.
    assert_eq!(report.summary.partitions_compared, 2);
    assert_eq!(report.summary.equal, 7);
    assert_eq!(report.summary.only_a + report.summary.only_b, 0);
    assert_eq!(report.summary.newer_in_a + report.summary.newer_in_b, 0);
    assert_eq!(
        report.summary.tombstones_only_a + report.summary.tombstones_only_b,
        0
    );
    for p in json["partitions"].as_array().unwrap() {
        for c in p["cells"].as_array().unwrap() {
            assert_eq!(c["status"], "equal", "{c}");
        }
        assert!(
            p["tombstones"].as_array().unwrap().is_empty(),
            "shared tombstones are omitted"
        );
    }
}

#[tokio::test]
async fn r2_1_zero_partitions_is_reported_explicitly_not_silently() {
    let root = tempfile::tempdir().unwrap();
    let Some(sides) = gc_positive(root.path()) else {
        return;
    };
    let report = diff_partitions(
        &sides.0,
        &sides.1,
        "test_tomb.resurrection_gc_positive",
        &gc_positive_schema(),
        None,
        &options(GC_POSITIVE_BOUNDARY + 1),
        &[],
    )
    .await
    .unwrap();
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["summary"]["partitions_compared"], 0);
    assert!(json["partitions"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn r2_2_classification_is_per_cell_not_per_row() {
    let root = tempfile::tempdir().unwrap();
    let Some(sides) = gc_positive(root.path()) else {
        return;
    };
    let report = gc_positive_report(&sides, GC_POSITIVE_BOUNDARY + 1).await;
    let p1 = partition(&report, "1");
    // Golden gen 2: partition 1 clustering 3 carries a CELL tombstone on `val` only.
    assert_eq!(cell(p1, "3", "val")["status"], "only-b");
    assert_eq!(cell(p1, "3", "val")["b"]["value"], "live_3");
    assert_eq!(cell(p1, "3", "extra")["status"], "equal");
    assert_eq!(cell(p1, "3", "extra")["a"]["value"], "extra_3");
    // Golden gen 2: clustering 2 holds a ROW tombstone, so both columns are only-b.
    assert_eq!(cell(p1, "2", "val")["status"], "only-b");
    assert_eq!(cell(p1, "2", "extra")["status"], "only-b");
    // Untouched rows are equal (the report is not vacuously "everything differs").
    for ck in ["1", "4", "5"] {
        for column in ["extra", "val"] {
            assert_eq!(cell(p1, ck, column)["status"], "equal", "ck {ck} {column}");
        }
    }
    // Golden gen 2: partition 2 is wholly deleted: all six cells exist only on B.
    let p2 = partition(&report, "2");
    assert_eq!(p2["cells"].as_array().unwrap().len(), 6);
    assert!(p2["cells"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["status"] == "only-b"));
    assert_eq!(report["summary"]["equal"], 7);
    assert_eq!(report["summary"]["only_b"], 9);
    assert_eq!(report["summary"]["only_a"], 0);
}

#[tokio::test]
async fn r2_newer_in_a_newer_in_b_only_a_only_b() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let report = risk_report(&sides, RISK_BOUNDARY + 1).await;
    // README (mutations): id4 A@2000 vs B@3000; id6 A@4000 vs B@3000; id7 only B; id8 only A.
    assert_eq!(
        cell(partition(&report, "4"), "1", "v")["status"],
        "newer-in-b"
    );
    assert_eq!(
        cell(partition(&report, "6"), "1", "v")["status"],
        "newer-in-a"
    );
    assert_eq!(cell(partition(&report, "7"), "1", "v")["status"], "only-b");
    assert_eq!(cell(partition(&report, "8"), "1", "v")["status"], "only-a");
    assert_eq!(
        cell(partition(&report, "4"), "1", "v")["a"]["writetime"],
        "1970-01-01T00:00:00.002Z"
    );
    assert_eq!(
        cell(partition(&report, "4"), "1", "v")["b"]["writetime"],
        "1970-01-01T00:00:00.003Z"
    );
    assert_eq!(report["summary"]["newer_in_a"], 1);
    assert_eq!(report["summary"]["newer_in_b"], 1);
    assert_eq!(report["summary"]["partitions_compared"], 18);
}

// ---------------------------------------------------------------------------------------------
// R3 — tombstone status; a tombstone both sides hold is omitted.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn r3_1_only_divergent_tombstones_are_reported() {
    let root = tempfile::tempdir().unwrap();
    let Some(sides) = gc_positive(root.path()) else {
        return;
    };
    let report = gc_positive_report(&sides, GC_POSITIVE_BOUNDARY + 1).await;
    let p1 = partition(&report, "1");
    let p2 = partition(&report, "2");
    // Golden gen 2: a ROW tombstone at ck 2, a CELL tombstone (val) at ck 3, a PARTITION tombstone
    // on partition 2 — each held by side A only (B is generation 1 only).
    let rows = tombstones_of(p1, "row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["clustering"], "2");
    assert_eq!(rows[0]["side"], "a");
    let cells = tombstones_of(p1, "cell");
    assert_eq!(cells.len(), 1);
    assert_eq!(
        (cells[0]["clustering"].as_str(), cells[0]["column"].as_str()),
        (Some("3"), Some("val"))
    );
    assert_eq!(cells[0]["side"], "a");
    assert_eq!(p1["tombstones"].as_array().unwrap().len(), 2);
    let partitions = tombstones_of(p2, "partition");
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0]["side"], "a");
    assert_eq!(report["summary"]["tombstones_only_a"], 3);
    assert_eq!(report["summary"]["tombstones_only_b"], 0);
    // Times are the goldens' (2021-01-02 / 2026-11-11T00:53:44Z).
    assert_eq!(rows[0]["deletion_time"], "2021-01-02T00:00:00Z");
    assert_eq!(rows[0]["local_deletion_time"], "2026-11-11T00:53:44Z");
}

// ---------------------------------------------------------------------------------------------
// R4 / R4A — resurrection risk.
// ---------------------------------------------------------------------------------------------

fn all_risks(report: &Json) -> Vec<&Json> {
    report["partitions"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|p| p["tombstones"].as_array().unwrap())
        .map(|t| &t["resurrection_risk"])
        .collect()
}

#[tokio::test]
async fn r4_1_past_gc_grace_names_every_shadowed_cell_for_all_three_kinds() {
    let root = tempfile::tempdir().unwrap();
    let Some(sides) = gc_positive(root.path()) else {
        return;
    };
    // One second past ldt + gc_grace: purge-eligible (strict `<`).
    let report = gc_positive_report(&sides, GC_POSITIVE_BOUNDARY + 1).await;
    let risks = all_risks(&report);
    assert_eq!(risks.len(), 3);
    for risk in &risks {
        assert_eq!(risk["status"], "yes");
        assert_eq!(risk["phase"], "past-gc-grace");
    }
    let p1 = partition(&report, "1");
    // Row tombstone @ck2 shadows the two cells of ck 2 that B still holds (ts 2021-01-01 <=
    // 2021-01-02, DeletionTime.java:173-175).
    assert_eq!(
        shadowed(tombstones_of(p1, "row")[0]),
        vec![
            ("2".to_string(), "extra".to_string()),
            ("2".to_string(), "val".to_string())
        ]
    );
    // Cell tombstone shadows exactly (ck 3, val) — NOT `extra`.
    assert_eq!(
        shadowed(tombstones_of(p1, "cell")[0]),
        vec![("3".to_string(), "val".to_string())]
    );
    // Partition tombstone names all six cells of partition 2.
    let expected: Vec<(String, String)> = ["1", "2", "3"]
        .iter()
        .flat_map(|ck| {
            [
                (ck.to_string(), "extra".to_string()),
                (ck.to_string(), "val".to_string()),
            ]
        })
        .collect();
    assert_eq!(
        shadowed(tombstones_of(partition(&report, "2"), "partition")[0]),
        expected
    );
    assert_eq!(
        report["summary"]["resurrection_risk_past_gc_grace"], 5,
        "+ the two id-18 element tombstones"
    );
    assert_eq!(report["summary"]["resurrection_risk_before_gc_grace"], 0);
    // Every shadowed cell is attributed to side B (the side that still holds the data).
    for tombstone in report["partitions"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|p| p["tombstones"].as_array().unwrap())
    {
        for cell in tombstone["resurrection_risk"]["shadowed_cells"]
            .as_array()
            .unwrap()
        {
            assert_eq!(cell["side"], "b");
        }
    }
}

#[tokio::test]
async fn r4_2_phase_flips_exactly_at_the_boundary_second() {
    let root = tempfile::tempdir().unwrap();
    let Some(sides) = gc_positive(root.path()) else {
        return;
    };
    // now == ldt + gc_grace: gcBefore == ldt, `ldt < gcBefore` is false => NOT purgeable yet.
    for now in [GC_POSITIVE_BOUNDARY - 1, GC_POSITIVE_BOUNDARY] {
        let report = gc_positive_report(&sides, now).await;
        for risk in all_risks(&report) {
            assert_eq!(
                risk["status"], "yes",
                "shadow relationship is unchanged at now={now}"
            );
            assert_eq!(risk["phase"], "before-gc-grace", "now={now}");
        }
        assert_eq!(report["summary"]["resurrection_risk_before_gc_grace"], 3);
    }
    let report = gc_positive_report(&sides, GC_POSITIVE_BOUNDARY + 1).await;
    for risk in all_risks(&report) {
        assert_eq!(risk["phase"], "past-gc-grace");
    }
    assert_eq!(report["gc_grace_seconds"], GC_POSITIVE_GC_GRACE);
}

#[tokio::test]
async fn r4_3_no_risk_when_nothing_is_shadowed_and_ties_go_to_the_tombstone() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let report = risk_report(&sides, RISK_BOUNDARY + 1).await;
    // README: id1 row tombstone @2000 vs B cell @3000 (newer) => no; id2 partition tombstone over a
    // key B never wrote => no; id5 cell tombstone @2000 vs B @3000 => no.
    for (key, kind) in [("1", "row"), ("2", "partition"), ("5", "cell")] {
        let tombstones = tombstones_of(partition(&report, key), kind);
        assert_eq!(tombstones.len(), 1, "id {key}");
        assert_eq!(tombstones[0]["side"], "a");
        let risk = &tombstones[0]["resurrection_risk"];
        assert_eq!(
            risk["status"], "no",
            "id {key}: field present and `no`, never omitted"
        );
        assert!(
            risk.get("phase").is_none() && risk.get("shadowed_cells").is_none(),
            "{risk}"
        );
    }
    // id3: row tombstone @2000 vs B cell @2000 — equal timestamps: the tombstone wins
    // (DeletionTime.deletes `<=`, Cells.resolveRegular tie) => yes.
    let tie = &tombstones_of(partition(&report, "3"), "row")[0]["resurrection_risk"];
    assert_eq!(tie["status"], "yes");
    assert_eq!(tie["phase"], "past-gc-grace");
    assert_eq!(
        shadowed(tombstones_of(partition(&report, "3"), "row")[0]),
        vec![("1".to_string(), "v".to_string())]
    );
    // ids 3 (tie), 11 (tie) and 16 (element tombstone over k1) are the three `yes` risks.
    assert_eq!(
        report["summary"]["resurrection_risk_past_gc_grace"], 5,
        "+ the two id-18 element tombstones"
    );
    assert_eq!(report["summary"]["tombstones_only_a"], 9);
    // The tie's phase flips exactly at ldt + gc_grace too (PurgeFunction.java:41-42).
    let at = risk_report(&sides, RISK_BOUNDARY).await;
    assert_eq!(
        tombstones_of(partition(&at, "3"), "row")[0]["resurrection_risk"]["phase"],
        "before-gc-grace"
    );
    // A `no` risk is never counted into a phase.
    assert_eq!(at["summary"]["resurrection_risk_before_gc_grace"], 5);
}

// ---------------------------------------------------------------------------------------------
// R5 — range tombstones and the other kinds, on the explain decision-trail fixture.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn r5_1_range_tombstone_shadows_only_cells_inside_its_span() {
    let root = tempfile::tempdir().unwrap();
    let sides = trace_sides(root.path());
    // README: partition 3: A ck=1 'range-shadowed' @1000 and ck=3 'outside-range' @1000; B's
    // inclusive range [1,2] @2000 (local deletion time 1794535357). Past the boundary:
    let report = trace_report(&sides, TD_BOUNDARY + 1).await;
    let p3 = partition(&report, "3");
    let ranges = tombstones_of(p3, "range");
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0]["side"], "a");
    assert_eq!(ranges[0]["range_start"], "inclusive:1");
    assert_eq!(ranges[0]["range_end"], "inclusive:2");
    assert_eq!(ranges[0]["resurrection_risk"]["status"], "yes");
    assert_eq!(ranges[0]["resurrection_risk"]["phase"], "past-gc-grace");
    // ck 1 is inside [1,2] (1000 <= 2000): named. ck 3 is outside: NOT named, and equal on both.
    assert_eq!(
        shadowed(ranges[0]),
        vec![("1".to_string(), "v".to_string())]
    );
    assert_eq!(cell(p3, "1", "v")["status"], "only-b");
    assert_eq!(cell(p3, "3", "v")["status"], "equal");
    // ...and before the boundary the phase flips, shadow unchanged.
    let at = trace_report(&sides, TD_BOUNDARY).await;
    let at_range = &tombstones_of(partition(&at, "3"), "range")[0]["resurrection_risk"];
    assert_eq!(at_range["status"], "yes");
    assert_eq!(at_range["phase"], "before-gc-grace");
}

#[tokio::test]
async fn every_tombstone_kind_is_classified_on_the_decision_trail_fixture() {
    let root = tempfile::tempdir().unwrap();
    let sides = trace_sides(root.path());
    let report = trace_report(&sides, TD_BOUNDARY + 1).await;
    // README rows (mutations @1000 in A, deletions @2000 in B), each against B = generation 1 only:
    for (key, kind) in [
        ("2", "row"),
        ("4", "cell"),
        ("5", "partition"),
        ("8", "cell"),
    ] {
        let tombstones = tombstones_of(partition(&report, key), kind);
        assert_eq!(tombstones.len(), 1, "partition {key} {kind}");
        assert_eq!(
            tombstones[0]["resurrection_risk"]["status"], "yes",
            "partition {key}"
        );
        assert_eq!(
            shadowed(tombstones[0]),
            vec![("1".to_string(), "v".to_string())],
            "partition {key}"
        );
    }
    // Partition 4: the cell tombstone is @1000 and B's cell is @1000 — an equal-timestamp tie that
    // the tombstone wins (Cells.java:92-98).
    // Partition 6: A's complex deletion @1999 shadows B's element @1000; the older complex deletion
    // @999 that both generations carry is held by BOTH sides and therefore omitted (R3).
    let collections = tombstones_of(partition(&report, "6"), "collection");
    assert_eq!(collections.len(), 1, "only the @1999 deletion diverges");
    assert_eq!(
        shadowed(collections[0]),
        vec![("1".to_string(), "m".to_string())]
    );
    // Partition 1: 'newer'@2000 vs 'older'@1000.
    assert_eq!(
        cell(partition(&report, "1"), "1", "v")["status"],
        "newer-in-a"
    );
    // Partition 7: the TTL cell (expires 2026-11-13) is expired on BOTH sides at this `now`; the
    // partition is compared and reports nothing.
    let p7 = partition(&report, "7");
    assert!(
        p7["cells"].as_array().unwrap().is_empty()
            && p7["tombstones"].as_array().unwrap().is_empty()
    );
    assert_eq!(report["summary"]["partitions_compared"], 8);
}

// ---------------------------------------------------------------------------------------------
// R11 (library half) — `--partition` restricts the scan to the requested keys.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn diff_partition_scopes_to_the_requested_key() {
    let root = tempfile::tempdir().unwrap();
    let Some(sides) = gc_positive(root.path()) else {
        return;
    };
    let one = diff_partition(
        &sides.0,
        &sides.1,
        &gc_positive_schema(),
        None,
        &int_key(2),
        &options(GC_POSITIVE_BOUNDARY + 1),
    )
    .await
    .unwrap();
    assert_eq!(one.partition_key, "2");
    assert_eq!(one.tombstones.len(), 1);
    assert_eq!(one.cells.len(), 6);
    // A key neither side holds is compared too: an explicit, empty entry — never silence.
    let absent = diff_partitions(
        &sides.0,
        &sides.1,
        "test_tomb.resurrection_gc_positive",
        &gc_positive_schema(),
        None,
        &options(GC_POSITIVE_BOUNDARY + 1),
        &[int_key(99)],
    )
    .await
    .unwrap();
    assert_eq!(absent.summary.partitions_compared, 1);
    assert!(absent.partitions[0].cells.is_empty() && absent.partitions[0].tombstones.is_empty());
}

// ---------------------------------------------------------------------------------------------
// R7 — fail closed on an unreadable generation, naming the side and the path.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn r7_1_truncated_statistics_on_side_b_names_side_and_path() {
    let root = tempfile::tempdir().unwrap();
    let Some(src) = fixture_dir("test_tomb", "resurrection_gc_positive") else {
        return;
    };
    let a = stage(root.path(), "r7-a", &src, &["nb-1-big-", "nb-2-big-"]);
    let b = stage(root.path(), "r7-b", &src, &["nb-1-big-"]);
    let stats = b.join("nb-1-big-Statistics.db");
    let truncated = std::fs::read(&stats).unwrap();
    std::fs::write(&stats, &truncated[..truncated.len() / 3]).unwrap();
    let outcome = diff_table(
        &side_of(&a),
        &side_of(&b),
        "test_tomb.resurrection_gc_positive",
        &gc_positive_schema(),
        None,
        &options(GC_POSITIVE_BOUNDARY + 1),
    )
    .await;
    match outcome {
        Err(DiffError::SideRead { side, path, .. }) => {
            assert_eq!(side, Side::B);
            assert!(
                path.ends_with("nb-1-big-Statistics.db"),
                "names the truncated component: {path}"
            );
            assert!(
                path.contains("r7-b"),
                "names side B's copy, not A's: {path}"
            );
        }
        other => panic!("expected SideRead on side b, got {other:?}"),
    }
    // The same corruption through the point path (no enumeration) fails closed the same way.
    let point = diff_partitions(
        &side_of(&a),
        &side_of(&b),
        "test_tomb.resurrection_gc_positive",
        &gc_positive_schema(),
        None,
        &options(GC_POSITIVE_BOUNDARY + 1),
        &[int_key(1)],
    )
    .await;
    assert!(
        matches!(&point, Err(DiffError::SideRead { side: Side::B, .. })),
        "point path: {point:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// R2A / R2B / R4B / R14 — conflicts, per-element cells, the declared liveness limit, the budget
// (owner Q21=b 2026-11-21). Fixture: test_diff.risk_cases ids 9-17 (README case table).
// ---------------------------------------------------------------------------------------------

/// "k1" / "k2" / "k3" / "x" / "y" as the hex of the element path bytes.
const K1: &str = "6b31";
const K2: &str = "6b32";
const K3: &str = "6b33";
const X: &str = "78";
const Y: &str = "79";

#[tokio::test]
async fn r2_3_scalar_conflict_resolves_to_the_larger_bytes_and_mirrors() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let report = risk_report(&sides, RISK_BOUNDARY + 1).await;
    // Same `USING TIMESTAMP 5000`. Cells.resolveRegular (Cells.java:79-128) at equal timestamps with
    // two live cells: the larger serialized value wins (`compareValues`, :128) — 'zebra' > 'apple'.
    let nine = cell(partition(&report, "9"), "1", "v");
    assert_eq!(nine["status"], "conflict");
    assert_eq!(nine["resolves_to"], "a");
    assert_eq!(nine["a"]["value"], "zebra");
    assert_eq!(nine["b"]["value"], "apple");
    assert_eq!(nine["a"]["writetime"], "1970-01-01T00:00:00.005Z");
    let ten = cell(partition(&report, "10"), "1", "v");
    assert_eq!(ten["status"], "conflict");
    assert_eq!(ten["resolves_to"], "b");
    assert_eq!(
        (ten["a"]["value"].as_str(), ten["b"]["value"].as_str()),
        (Some("apple"), Some("zebra"))
    );
    // The report is complete: the conflict did not abort the run, and is counted.
    assert_eq!(report["summary"]["partitions_compared"], 18);
    assert_eq!(report["summary"]["conflicts"], 5, "ids 9, 10, 11, 12, 15");
    // A non-conflict cell carries no `resolves_to`.
    assert!(cell(partition(&report, "4"), "1", "v")
        .get("resolves_to")
        .is_none());
}

#[tokio::test]
async fn r2_4_tombstone_and_expiring_beat_live_at_an_equal_timestamp() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let report = risk_report(&sides, RISK_BOUNDARY + 1).await;
    // id 11: A cell tombstone @5000 vs B live @5000: a tombstone beats a live cell (Cells.java:93-98).
    let tie = cell(partition(&report, "11"), "1", "v");
    assert_eq!(tie["status"], "conflict");
    assert_eq!(tie["resolves_to"], "a");
    assert_eq!(tie["a"]["value"], "<tombstone>");
    assert_eq!(tie["b"]["value"], "live-tie");
    // ...and the tombstone is still reported, with the tie's resurrection risk.
    let tombstones = tombstones_of(partition(&report, "11"), "cell");
    assert_eq!(tombstones.len(), 1);
    assert_eq!(tombstones[0]["resurrection_risk"]["status"], "yes");
    // id 12: A expiring (TTL) @5000 vs B live @5000: an expiring cell beats a live one (:93-98).
    let expiring = cell(partition(&report, "12"), "1", "v");
    assert_eq!(expiring["status"], "conflict");
    assert_eq!(expiring["resolves_to"], "a");
    assert_eq!(expiring["a"]["value"], "expiring-wins");
    assert_eq!(expiring["b"]["value"], "live-loses");
    // The mirror flips resolves_to (symmetry holds for conflicts).
    let swapped = diff_table(
        &sides.1,
        &sides.0,
        "test_diff.risk_cases",
        &risk_schema(),
        None,
        &options(RISK_BOUNDARY + 1),
    )
    .await
    .unwrap();
    let swapped = serde_json::to_value(swapped).unwrap();
    assert_eq!(
        cell(partition(&swapped, "11"), "1", "v")["resolves_to"],
        "b"
    );
    assert_eq!(cell(partition(&swapped, "9"), "1", "v")["resolves_to"], "b");
}

#[tokio::test]
async fn r2_5_collections_are_compared_per_element_on_typed_bytes() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let report = risk_report(&sides, RISK_BOUNDARY + 1).await;
    // id 13: A m['k2']=7, B m['k1']=7, same value and timestamp: DIFFERENT elements.
    let p13 = partition(&report, "13");
    assert_eq!(element(p13, "1", "m", K2)["status"], "only-a");
    assert_eq!(element(p13, "1", "m", K1)["status"], "only-b");
    assert_eq!(p13["cells"].as_array().unwrap().len(), 2);
    // id 14: set {y} vs {x} at one timestamp: per-element only-a / only-b, never a conflict.
    let p14 = partition(&report, "14");
    assert_eq!(element(p14, "1", "s", Y)["status"], "only-a");
    assert_eq!(element(p14, "1", "s", X)["status"], "only-b");
    // id 15: frozen ['a, b'] vs ['a','b']: NOT equal. Serialized collections lead with the element
    // count (2 > 1), so A's bytes are the larger and Cassandra keeps A.
    let frozen = cell(partition(&report, "15"), "1", "fl");
    assert_ne!(frozen["status"], "equal");
    assert_eq!(frozen["status"], "conflict");
    assert_eq!(frozen["resolves_to"], "a");
    assert!(
        frozen.get("cell_path").is_none(),
        "a frozen value has no element path"
    );
    // id 16: an element tombstone m['k1'] @6000 shadows ONLY k1 (5000 <= 6000), never k3.
    let p16 = partition(&report, "16");
    let tombstone = tombstones_of(p16, "cell");
    assert_eq!(tombstone.len(), 1);
    assert_eq!(tombstone[0]["cell_path"], K1);
    assert_eq!(
        shadowed_paths(tombstone[0]),
        vec![("1".to_string(), "m".to_string(), Some(K1.to_string()))],
        "only the deleted element is shadowed"
    );
    assert_eq!(tombstone[0]["resurrection_risk"]["status"], "yes");
    assert_eq!(element(p16, "1", "m", K1)["status"], "only-b");
    assert_eq!(element(p16, "1", "m", K3)["status"], "equal");
}

#[tokio::test]
async fn r4_4_liveness_only_row_reports_no_risk_a_declared_limit() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let report = risk_report(&sides, RISK_BOUNDARY + 1).await;
    // id 17 (#4601): B holds only a row liveness marker @5000, A a row tombstone @6000. The marker
    // is not a cell in the trail, so the tombstone is `no` — documented in --help, never an error.
    let p17 = partition(&report, "17");
    assert!(p17["cells"].as_array().unwrap().is_empty());
    let rows = tombstones_of(p17, "row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["resurrection_risk"]["status"], "no");
}

#[tokio::test]
async fn r14_1_the_report_never_exceeds_the_budget_metadata_included() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let schema = risk_schema();
    let run = |budget: u64| {
        let (a, b, schema) = (&sides.0, &sides.1, &schema);
        async move {
            diff_table(
                a,
                b,
                "test_diff.risk_cases",
                schema,
                None,
                &DiffOptions {
                    now_secs: RISK_BOUNDARY + 1,
                    max_result_bytes: budget,
                },
            )
            .await
        }
    };
    let full = run(BUDGET).await.expect("fits");
    let exact = serde_json::to_vec(&full).unwrap().len() as u64;
    // The partitions alone are smaller than the whole report: the header, generation lists and
    // summary count against the budget too.
    let partitions_only = serde_json::to_vec(&full.partitions).unwrap().len() as u64;
    assert!(partitions_only < exact);
    assert!(
        run(exact).await.is_ok(),
        "a report of exactly the budget is returned"
    );
    for budget in [exact - 1, partitions_only + 1] {
        match run(budget).await {
            Err(DiffError::Core(cqlite_core::Error::ResultTooLarge { .. })) => {}
            other => {
                panic!("budget {budget} < report {exact} must be ResultTooLarge, got {other:?}")
            }
        }
    }
}

#[tokio::test]
async fn r2_6_two_elements_deleted_by_one_statement_are_two_distinct_tombstones() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let report = risk_report(&sides, RISK_BOUNDARY + 1).await;
    // id 18: B holds m['k1'], m['k2'], m['k3'] @5000; A deleted k1 and k2 in ONE statement @6000, so
    // both element tombstones share row, column, timestamp and local deletion time and differ only
    // in their cell path. Each must be its own `only-a` tombstone shadowing only its own element.
    let p18 = partition(&report, "18");
    let tombstones = tombstones_of(p18, "cell");
    assert_eq!(tombstones.len(), 2, "{p18}");
    let mut paths: Vec<&str> = tombstones
        .iter()
        .map(|t| t["cell_path"].as_str().unwrap())
        .collect();
    paths.sort_unstable();
    assert_eq!(paths, [K1, K2]);
    for tombstone in &tombstones {
        assert_eq!(tombstone["side"], "a");
        assert_eq!(tombstone["resurrection_risk"]["status"], "yes");
        let own = tombstone["cell_path"].as_str().unwrap().to_string();
        assert_eq!(
            shadowed_paths(tombstone),
            vec![("1".to_string(), "m".to_string(), Some(own))]
        );
    }
    assert_eq!(element(p18, "1", "m", K3)["status"], "only-b");
}

#[tokio::test]
async fn r14_1_single_partition_report_at_exactly_the_budget_is_accepted() {
    let root = tempfile::tempdir().unwrap();
    let sides = risk_sides(root.path());
    let schema = risk_schema();
    let run = |budget: u64| {
        let (a, b, schema) = (&sides.0, &sides.1, &schema);
        async move {
            diff_partitions(
                a,
                b,
                "test_diff.risk_cases",
                schema,
                None,
                &DiffOptions {
                    now_secs: RISK_BOUNDARY + 1,
                    max_result_bytes: budget,
                },
                &[int_key(9)],
            )
            .await
        }
    };
    let exact = serde_json::to_vec(&run(BUDGET).await.unwrap())
        .unwrap()
        .len() as u64;
    assert!(
        run(exact).await.is_ok(),
        "exactly the budget (one partition, no separator)"
    );
    assert!(run(exact - 1).await.is_err(), "one byte under is refused");
}

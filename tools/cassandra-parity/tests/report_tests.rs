//! Report-rendering tests (issue #995): the generated parity report must make
//! the delta_scan evidence story honest — canonical-semantic JSONL scenarios are
//! surfaced as such with an explicit "needs byte-for-byte Data.db backing"
//! follow-up note, and the planned wide-partition corpus appears under gaps.

use std::path::PathBuf;

use cassandra_parity::model::Manifest;
use cassandra_parity::report;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn real_manifest() -> Manifest {
    let path = repo_root().join("test-data/cassandra-parity-manifest.yml");
    let text = std::fs::read_to_string(&path).expect("real manifest exists");
    Manifest::from_yaml(&text).expect("real manifest parses")
}

fn rendered() -> String {
    report::render(&real_manifest(), "test-data/cassandra-parity-manifest.yml")
}

#[test]
fn delta_scan_scenarios_render_under_canonical_semantic() {
    let r = rendered();
    let header = "## Canonical-semantic scenarios";
    let start = r.find(header).expect("canonical-semantic section present");
    let section = &r[start..];
    // The mirrored per-shape delta_scan scenarios are canonical-semantic.
    for id in [
        "cass.delta_scan.cell_tombstones",
        "cass.delta_scan.row_tombstones",
        "cass.delta_scan.range_tombstones",
        "cass.delta_scan.partition_tombstones",
        "cass.delta_scan.ttl_cells",
        "cass.delta_scan.static_with_rows",
        "cass.delta_scan.partial_updates",
        "cass.delta_scan.adjacent_ranges",
        "cass.delta_scan.collection_ops",
    ] {
        assert!(
            section.contains(id),
            "{id} should appear under the canonical-semantic section"
        );
    }
}

#[test]
fn delta_scan_canonical_semantic_marks_byte_for_byte_followup() {
    let r = rendered();
    // The byte-for-byte follow-up note must appear so the report never reads as
    // byte parity for delta_scan (AC3/AC7). Tie it to the Data.db epic.
    assert!(
        r.contains("Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969)."),
        "report must surface the delta_scan byte-for-byte follow-up note"
    );
}

#[test]
fn wide_partition_corpus_is_a_planned_gap() {
    let r = rendered();
    let gaps_start = r
        .find("## Gaps and next steps")
        .expect("gaps section present");
    let gaps = &r[gaps_start..];
    assert!(
        gaps.contains("cass.delta_scan.wide_partition_corpus"),
        "planned wide_partition_corpus must appear under gaps/next-steps"
    );
    assert!(
        gaps.contains("planned"),
        "wide_partition_corpus must be marked planned in the gaps section"
    );
}

#[test]
fn report_has_manifest_driven_claim_language_section() {
    // AC6 (issue #1023): the report summarizes release-safe claim language from
    // the manifest's claims section — safe wordings (with backing scenarios) and
    // blocked phrases (with the safe alternative to use instead).
    let r = rendered();
    let header = "## Release-safe claim language";
    let start = r.find(header).expect("claim-language section present");
    let section = &r[start..];
    assert!(section.contains("### Safe wordings"));
    assert!(section.contains("### Blocked phrases"));
    for id in [
        "claim.safe.selected_fixture_validation",
        "claim.safe.rust_byte_level_coverage",
        "claim.safe.traceable_cassandra_parity_suite",
        "claim.blocked.same_tests_as_cassandra",
        "claim.blocked.full_compaction_byte_parity",
        "claim.blocked.zero_diff_sstabledump_all_datasets",
    ] {
        assert!(section.contains(id), "claim section must list {id}");
    }
    // A blocked phrase must name its safe alternative.
    assert!(section.contains("Use instead: `claim.safe."));
}

#[test]
fn planned_scenario_in_evidence_group_is_marked_no_evidence_yet() {
    let r = rendered();
    let header = "## Canonical-semantic scenarios";
    let start = r.find(header).expect("canonical-semantic section present");
    let section = &r[start..];
    // The planned wide_partition_corpus is grouped by its declared evidence type
    // but must be flagged so it does not read as proven parity.
    let line = section
        .lines()
        .find(|l| l.contains("cass.delta_scan.wide_partition_corpus"))
        .expect("wide_partition_corpus listed in canonical-semantic section");
    assert!(
        line.contains("planned — no evidence yet"),
        "planned scenario must be marked, got: {line}"
    );
    // The byte-for-byte follow-up note is NOT emitted for a planned scenario
    // (it has no evidence to qualify).
    let next = section
        .lines()
        .skip_while(|l| !l.contains("cass.delta_scan.wide_partition_corpus"))
        .nth(1)
        .unwrap_or("");
    assert!(
        !next.contains("Byte-for-byte: not yet"),
        "planned scenario should not carry the byte-for-byte follow-up note"
    );
}

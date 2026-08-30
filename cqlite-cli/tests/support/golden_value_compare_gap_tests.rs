//! Unit coverage for the comparator's DECLARED-GAP machinery and its fixture
//! pairing (issue #1491) — the `SkipPaths` staleness rules (finding L1) and the
//! golden↔SSTable pairing (finding L3).
//!
//! Split out of `golden_value_compare_tests.rs` under the campsite rule
//! (CLAUDE.md, epic #1135), which had reached the ~1500-line test target. A child
//! of that module, so the shared `row`/`schema_of` helpers and its imports are
//! reached through `use super::*` and are stated once.

use super::*;

// =======================================================================
// L1: a declared gap retires itself once the divergence is gone
// =======================================================================

/// The property the whole `SkipPaths` mechanism exists for, in the direction
/// nothing used to test: once CQLite renders the excluded path CORRECTLY, the
/// exclusion is STALE and must FAIL, naming the path — otherwise the column stays
/// excluded forever and the recovered coverage never comes back.
///
/// A visit-keyed tally could never see this: the path is visited in both worlds,
/// so it registered a hit either way (issue #1491 review finding L1).
#[test]
fn a_skip_whose_divergence_is_gone_is_reported_as_stale() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": "Lovelace", "age": 36}),
        ),
    ])];
    let skip = ["p.last_name"];

    // STILL DIVERGING: the exclusion suppressed a real divergence, so it stands.
    let diverged = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "person", "first_name": "Ada",
                     "last_name": "0xdeadbeef", "age": 36}),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &diverged,
        &schema,
        &["id"],
        &[],
        &skip,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(
        report.stale_skips.is_empty(),
        "an exclusion suppressing a real divergence is not stale: {:?}",
        report.stale_skips
    );

    // FIXED: the same excluded path now agrees. The comparison must not fail (the
    // gap is declared, so the value is not compared), but the GAP must.
    let fixed = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "person", "first_name": "Ada",
                     "last_name": "Lovelace", "age": 36}),
        ),
    ])];
    let report = compare_rows(&golden, &fixed, &schema, &["id"], &[], &skip, Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(
        report.stale_skips.len(),
        1,
        "a fixed divergence must retire its gap: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("p.last_name") && report.stale_skips[0].contains("AGREE"),
        "the failure must name the path and why it is stale: {:?}",
        report.stale_skips
    );
}

/// One divergent row keeps the gap alive even when another row agrees: a gap is a
/// property of the output, and suppressing anywhere is suppressing. The opposite
/// rule (last row wins) would make staleness depend on row order.
#[test]
fn one_diverging_row_keeps_a_skip_applied() {
    let schema = schema_of(PERSON_DDL, "t");
    let person = |last: &str| json!({"first_name": "Ada", "last_name": last, "age": 36});
    let cli_person =
        |last: &str| json!({"_type": "person", "first_name": "Ada", "last_name": last, "age": 36});
    let golden = vec![
        row(&[("id", json!(1)), ("p", person("Lovelace"))]),
        row(&[("id", json!(2)), ("p", person("Byron"))]),
    ];
    let cli = vec![
        row(&[("id", json!(1)), ("p", cli_person("Lovelace"))]),
        row(&[("id", json!(2)), ("p", cli_person("0xdeadbeef"))]),
    ];
    let report = compare_rows(
        &golden,
        &cli,
        &schema,
        &["id"],
        &[],
        &["p.last_name"],
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(
        report.stale_skips.is_empty(),
        "one diverging row is enough to keep the gap: {:?}",
        report.stale_skips
    );
}

/// The CSV half of the same property, on a CONTAINER member — the shape where
/// the decode actually matters (a scalar member is raw text either way). An
/// excluded member's text is decoded when it CAN be: an un-invertible rendering
/// falls back to raw text, so one member cannot fail a whole cell nobody
/// compares, and still counts as suppressed; a member that now decodes and agrees
/// retires the gap. Returning raw text unconditionally made the excluded position
/// diverge forever — the finding's own shape, one level down.
///
/// The DDL is the real `udt_nested` shape (`test-data/schemas/*.cql`), whose
/// `e.home` gap is one of this lane's declared CSV exclusions.
#[test]
fn a_csv_skip_on_a_nested_container_retires_when_it_decodes_and_agrees() {
    let schema = schema_of(NESTED_UDT_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "e",
            json!({"name": "Ada", "home": {"street": "1 Navy Way", "city": "Arlington"}}),
        ),
    ])];
    let skip = ["e.home"];

    // Diverging exactly as CQLite does today: the inner frozen UDT arrives as
    // blob hex, which the `{…}` grammar cannot invert.
    let diverged = vec![row(&[
        ("id", json!("1")),
        ("e", json!("{name: Ada, home: 0x0000000a31204e617679}")),
    ])];
    let report = compare_rows(
        &golden,
        &diverged,
        &schema,
        &["id"],
        &[],
        &skip,
        Egress::Csv,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(
        report.stale_skips.is_empty(),
        "a member whose rendering does not invert keeps its gap: {:?}",
        report.stale_skips
    );

    let fixed = vec![row(&[
        ("id", json!("1")),
        (
            "e",
            json!("{name: Ada, home: {street: 1 Navy Way, city: Arlington}}"),
        ),
    ])];
    let report = compare_rows(&golden, &fixed, &schema, &["id"], &[], &skip, Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(
        report.stale_skips.len(),
        1,
        "a nested member that now decodes and agrees must retire its gap: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("e.home") && report.stale_skips[0].contains("AGREE"),
        "{:?}",
        report.stale_skips
    );
}

/// The third cause, kept distinct from the other two: when the cell the exclusion
/// names was REFUSED as CSV-unrepresentable there is no comparison to read an
/// answer from. "I could not tell" is not "the gap is still real", so it is
/// reported — with its own cause — rather than counted as applied.
#[test]
fn a_skip_whose_cell_was_refused_is_reported_as_unevaluable() {
    let schema = set_schema();
    // `, ` inside a member: `csv_container::node_refusal` refuses that container
    // from the GOLDEN alone, so the refusal is independent of what the CLI
    // rendered. The refused node here IS the excluded whole column; a refusal at
    // any DEPTH inside an excluded subtree is reported the same way.
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a, b"]))])];
    let cli = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &["s"], Egress::Csv);
    assert_eq!(report.ambiguous_container_cells, 1);
    assert_eq!(
        report.stale_skips.len(),
        1,
        "an unevaluable exclusion must be reported: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("could not be evaluated"),
        "the cause must be the unevaluable one, not `AGREE` or `matched no value`: {:?}",
        report.stale_skips
    );
}

/// P1: a declared exclusion suppresses a VALUE divergence, never the COLUMN's
/// PRESENCE. A skip used to record an omitted column as `Suppressed`, so each of
/// the five declared skips could hide a regression that dropped its column from
/// the egress row altogether — the one shape the comparator's own contract (every
/// DDL column is rendered) puts outside any gap's reach.
///
/// Asserted in BOTH lanes, because the omission is a property of the egress row
/// and not of one format's spelling: neither the presence of a skip nor the format
/// may excuse it.
#[test]
fn a_skip_cannot_hide_a_column_the_egress_omits() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
    for (egress, id) in [(Egress::Json, json!(1)), (Egress::Csv, json!("1"))] {
        // The egress row renders `id` and DROPS the declared `s` entirely.
        let cli = vec![row(&[("id", id)])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &["s"], egress);
        assert_eq!(
            report.diffs.len(),
            1,
            "{egress:?}: an omitted declared column must fail even under a whole-column \
             skip: {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains(".s:") && report.diffs[0].contains("absent from the"),
            "{egress:?}: the diff must name the omitted column: {:?}",
            report.diffs
        );
        // …and the skip itself is UNRESOLVED, not applied: with no value at that
        // path there is nothing to read an answer from. The two failures agree —
        // neither says the gap was measured.
        assert_eq!(
            report.stale_skips.len(),
            1,
            "{egress:?}: {:?}",
            report.stale_skips
        );
        assert!(
            report.stale_skips[0].contains("could not be evaluated")
                && report.stale_skips[0].contains("no `s` column"),
            "{egress:?}: the cause must be the unevaluable one: {:?}",
            report.stale_skips
        );
    }
}

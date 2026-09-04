//! Unit coverage for the AD2 comparator (issue #1491).
//!
//! Split out of `golden_value_compare.rs` under the campsite rule (CLAUDE.md,
//! epic #1135): the comparator and its cases were one 1.7k-line file.
//!
//! Every expectation here is derived from the committed DDL, from `sstabledump`
//! semantics, or from the rendering grammar the support modules document —
//! never from CQLite's current output. Each case is pinned from BOTH sides (the
//! shape that must pass and the shape that must fail), so narrowing a rule back
//! re-reds a case rather than quietly widening coverage.

use super::gap::Divergence;
use super::*;
use serde_json::json;

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// A schema for the unit cases, parsed from DDL exactly as the lane parses the
/// committed files — so these cases exercise the real authority, not a mock.
fn schema_of(ddl: &str, table: &str) -> TableSchema {
    match super::super::schema::from_ddl(ddl, table) {
        Ok(schema) => schema,
        Err(why) => panic!("{table}: {why}"),
    }
}

fn set_schema() -> TableSchema {
    schema_of("CREATE TABLE t (id int PRIMARY KEY, s set<text>);", "t")
}

// =======================================================================
// The column set is the DDL's, and only the golden may default to null
// =======================================================================

const ABSENT_DDL: &str =
    "CREATE TABLE t (pk int, ck int, anchor text, reg text, PRIMARY KEY (pk, ck));";

fn absent_schema() -> TableSchema {
    schema_of(ABSENT_DDL, "t")
}

fn absent_golden() -> Vec<Row> {
    // The `sstabledump` shape of `test_types.nb_absent_vs_null_regular` row 1:
    // `reg` was never written, so the physical dump simply has no such cell.
    vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("anchor_absent")),
    ])]
}

/// The property `nb_absent_vs_null_regular` exists for: an absent cell must be
/// RENDERED as null. Reading an omitted egress column as null (the first cut of
/// this file) made it unfalsifiable.
#[test]
fn a_column_the_egress_omits_is_a_named_failure() {
    let schema = absent_schema();
    let golden = absent_golden();

    let rendered = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("anchor_absent")),
        ("reg", Value::Null),
    ])];
    let report = compare_rows(
        &golden,
        &rendered,
        &schema,
        &["pk"],
        &["ck"],
        &[],
        Egress::Json,
    );
    assert!(
        report.diffs.is_empty(),
        "an absent golden cell rendered as null is the expected outcome: {:?}",
        report.diffs
    );
    assert_eq!(
        report.compared_cells, 4,
        "every declared column is compared"
    );

    // The regression: the egress drops the column entirely.
    let omitted = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("anchor_absent")),
    ])];
    let report = compare_rows(
        &golden,
        &omitted,
        &schema,
        &["pk"],
        &["ck"],
        &[],
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("reg") && report.diffs[0].contains("absent from the"),
        "the failure must name the omitted column: {:?}",
        report.diffs
    );
}

/// The mirror image: a column the DDL does not declare must not pass just
/// because it holds `null`.
#[test]
fn a_spurious_extra_column_is_a_named_failure() {
    let schema = absent_schema();
    let golden = absent_golden();
    let extra = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("anchor_absent")),
        ("reg", Value::Null),
        ("ghost", Value::Null),
    ])];
    let report = compare_rows(
        &golden,
        &extra,
        &schema,
        &["pk"],
        &["ck"],
        &[],
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("ghost") && report.diffs[0].contains("does not declare"),
        "the failure must name the undeclared column: {:?}",
        report.diffs
    );
}

/// A value where the golden has no cell at all is still a divergence — the
/// golden's absence is an expected NULL, not a wildcard.
#[test]
fn a_value_where_the_golden_has_no_cell_still_fails() {
    let schema = absent_schema();
    let golden = absent_golden();
    let invented = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("anchor_absent")),
        ("reg", json!("invented")),
    ])];
    let report = compare_rows(
        &golden,
        &invented,
        &schema,
        &["pk"],
        &["ck"],
        &[],
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains(".reg:") && report.diffs[0].contains("invented"),
        "{:?}",
        report.diffs
    );
}

/// A golden cell for a column the named schema does not declare means the
/// expectation itself is stale, so it is a failure rather than silent coverage.
#[test]
fn a_golden_cell_for_an_undeclared_column_is_a_named_failure() {
    let schema = absent_schema();
    let golden = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("a")),
        ("dropped", json!("x")),
    ])];
    let cli = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("a")),
        ("reg", Value::Null),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["pk"], &["ck"], &[], Egress::Json);
    assert!(
        report
            .diffs
            .iter()
            .any(|d| d.contains("dropped") && d.contains("does not declare")),
        "{:?}",
        report.diffs
    );
}

/// A shape divergence is reported ONCE per column, not once per row — the
/// tables in this lane run to 900 rows.
#[test]
fn a_column_shape_divergence_is_reported_once_per_column() {
    let schema = absent_schema();
    let golden: Vec<Row> = (1..=5)
        .map(|i| {
            row(&[
                ("pk", json!(1)),
                ("ck", json!(i)),
                ("anchor", json!("a")),
                ("reg", json!("v")),
            ])
        })
        .collect();
    let cli: Vec<Row> = (1..=5)
        .map(|i| row(&[("pk", json!(1)), ("ck", json!(i)), ("anchor", json!("a"))]))
        .collect();
    let report = compare_rows(&golden, &cli, &schema, &["pk"], &["ck"], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "5 rows missing the same column must report once: {:?}",
        report.diffs
    );
}

/// The `nb_empty_collections` shape, which is where this lane's empty-collection
/// gaps live: a NON-FROZEN `list<int>` whose golden row has no cell at all,
/// because Cassandra stores an empty multi-cell collection as a complex deletion
/// with no cells.
const ABSENT_MULTICELL_DDL: &str =
    "CREATE TABLE t (pk int, ck int, anchor text, ml list<int>, PRIMARY KEY (pk, ck));";

/// A declared skip path suppresses the VALUE at its path, so the
/// measured-divergence gaps keep working — and NOT the column's PRESENCE, which
/// no gap may excuse (issue #1491 review finding P1; the omission half is pinned
/// by `gaps::a_skip_cannot_hide_a_column_the_egress_omits`).
///
/// This case used to assert the opposite for an OMITTED column ("a declared skip
/// must stay declared", no diff), which is what made the five declared skips able
/// to hide a dropped column. The property it was really protecting is the one
/// asserted here: the column is RENDERED and DIVERGES, and the gap absorbs that.
///
/// The divergence is the one the gap DECLARES — golden absent, egress a present
/// empty container — because a gap now suppresses that divergence and no other
/// (review round 17).
#[test]
fn a_declared_skip_suppresses_its_columns_value_and_not_its_presence() {
    let schema = schema_of(ABSENT_MULTICELL_DDL, "t");
    let golden = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("anchor_absent")),
    ])];
    let diverging = vec![row(&[
        ("pk", json!(1)),
        ("ck", json!(1)),
        ("anchor", json!("anchor_absent")),
        // The golden has no `ml` cell, so the expected rendering is `null`; this
        // egress renders a present empty container instead, which is a different
        // value — and is exactly what the declared gap stands for.
        ("ml", json!([])),
    ])];
    let report = compare_rows(
        &golden,
        &diverging,
        &schema,
        &["pk"],
        &["ck"],
        &[("ml", Divergence::AbsentMulticellRendersEmpty)],
        Egress::Json,
    );
    assert!(
        report.diffs.is_empty(),
        "a declared skip must absorb the VALUE divergence at its path: {:?}",
        report.diffs
    );
    assert!(
        report.stale_skips.is_empty(),
        "the skip suppressed a real divergence, so it is not stale: {:?}",
        report.stale_skips
    );
    assert_eq!(
        report.compared_cells, 3,
        "the excluded column is not counted as compared coverage"
    );
}

// =======================================================================
// Types come from the DDL: a numeric-looking text is NOT a number
// =======================================================================

// One DDL per shape: the comparison now REQUIRES every declared column to be
// rendered, so a schema carrying columns a case does not exercise would fail
// for the right reason in the wrong test.
const TEXT_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, zip text);";
const NUM_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, n int);";
const SET_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, s set<int>, fs frozen<set<int>>);";
const UDT_MAP_DDL: &str = "CREATE TYPE address (street text, city text, zip text); \
     CREATE TABLE t (id int PRIMARY KEY, ma map<text, frozen<address>>);";
const INT_MAP_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, mi map<int, text>);";

/// BLOCKER 2: a CQL `text` value holding `\"22201\"` must NOT compare equal to
/// the JSON number `22201`, and `\"00000\"` must not equal `\"0\"`.
#[test]
fn a_numeric_looking_text_column_is_compared_exactly() {
    let schema = schema_of(TEXT_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("zip", json!("22201"))])];

    let same = vec![row(&[("id", json!(1)), ("zip", json!("22201"))])];
    let report = compare_rows(&golden, &same, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    for wrong in [json!(22201), json!("22201.0"), json!("022201")] {
        let cli = vec![row(&[("id", json!(1)), ("zip", wrong.clone())])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "text {wrong} must not equal the golden text \"22201\": {:?}",
            report.diffs
        );
        assert!(report.diffs[0].contains(".zip:"), "{:?}", report.diffs);
    }

    // Zero padding, the second half of the finding.
    let padded = vec![row(&[("id", json!(1)), ("zip", json!("00000"))])];
    let stripped = vec![row(&[("id", json!(1)), ("zip", json!("0"))])];
    let report = compare_rows(&padded, &stripped, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "\"00000\" must not equal \"0\": {:?}",
        report.diffs
    );
}

/// The same strictness one level in: a `text` UDT field nested inside a map
/// value. This is the shape the `udt_collections` fixture actually carries
/// (`ma frozen<map<text, frozen<address>>>`, zip `\"22201\"`/`\"00000\"`).
#[test]
fn a_numeric_looking_text_udt_field_is_compared_exactly() {
    let schema = schema_of(UDT_MAP_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "ma",
            json!({"home": {"street": "1 Navy Way", "city": "Arlington", "zip": "00000"}}),
        ),
    ])];
    let cli_ok = vec![row(&[
        ("id", json!(1)),
        (
            "ma",
            json!([{"key": "home", "value": {"street": "1 Navy Way",
                    "city": "Arlington", "zip": "00000"}}]),
        ),
    ])];
    let report = compare_rows(&golden, &cli_ok, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(report.container_cells, 1);

    for wrong in [json!(0), json!("0")] {
        let cli = vec![row(&[
            ("id", json!(1)),
            (
                "ma",
                json!([{"key": "home", "value": {
                        "street": "1 Navy Way", "city": "Arlington", "zip": wrong}}]),
            ),
        ])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a nested text zip {wrong} must not equal \"00000\": {:?}",
            report.diffs
        );
        assert!(report.diffs[0].contains("zip"), "{:?}", report.diffs);
    }
}

/// The normalization that must SURVIVE, and only where it is EARNED (review
/// finding R1). `sstabledump` writes a JSON string at exactly two kinds of
/// position — every partition-key component, and a non-frozen collection's cell
/// `path` — and a JSON number everywhere else, so the golden's own spelling here
/// is the expectation: `"1"` for the `int` partition key, `-5` for the ordinary
/// `int` cell.
#[test]
fn a_numeric_column_still_compares_across_spellings() {
    let schema = schema_of(NUM_DDL, "t");
    let golden = vec![row(&[("id", json!("1")), ("n", json!(-5))])];
    let cli = vec![row(&[("id", json!(1)), ("n", json!(-5))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "the dump's stringified partition key must still pair with the CLI's number: {:?}",
        report.diffs
    );

    // And a WRONG number still fails.
    let wrong = vec![row(&[("id", json!(1)), ("n", json!(-6))])];
    let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
}

/// The other half of finding R1: OUTSIDE a stringified position the JSON lane
/// compares by kind too, so an ordinary `int` cell rendered `"-5"` instead of `-5`
/// is a divergence. Before this the two canonicalized identically and the
/// regression passed.
#[test]
fn a_numeric_cell_rendered_as_a_string_is_a_json_divergence() {
    let schema = schema_of(NUM_DDL, "t");
    let golden = vec![row(&[("id", json!("1")), ("n", json!(-5))])];
    let stringified = vec![row(&[("id", json!(1)), ("n", json!("-5"))])];
    let report = compare_rows(
        &golden,
        &stringified,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "an int cell rendered as a JSON string must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains(".n:"), "{:?}", report.diffs);

    // CSV hands every cell over as text, so there the same pair is ONE value —
    // which is why the rule is scoped by egress as well as by position.
    let report = compare_rows(
        &golden,
        &stringified,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
}

/// Finding M1: the [`Kinding`] relaxation is the GOLDEN's, so it must not license
/// a CLI spelling. At a stringified position the dump's `"1"` still pairs with the
/// CLI's `1`, and the CLI rendering the same `int` partition key as `"1"` is a
/// DIVERGENCE — while the mechanism was symmetric it compared equal, so an egress
/// regression to `"id":"1"` passed at exactly the positions the relaxation covers.
#[test]
fn the_kinding_relaxation_applies_to_the_golden_side_only() {
    let schema = schema_of(NUM_DDL, "t");
    let golden = vec![row(&[("id", json!("1")), ("n", json!(-5))])];

    // Unchanged: the dump stringifies its partition key, the CLI numbers it.
    let good = vec![row(&[("id", json!(1)), ("n", json!(-5))])];
    let report = compare_rows(&golden, &good, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "the dump's stringified partition key must still pair with the CLI's number: {:?}",
        report.diffs
    );

    // The regression this closes: the CLI stringifies the numeric partition key.
    let stringified_pk = vec![row(&[("id", json!("1")), ("n", json!(-5))])];
    let report = compare_rows(
        &golden,
        &stringified_pk,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "an `int` partition key rendered as the JSON string \"1\" must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains("id"), "{:?}", report.diffs);

    // CSV carries no JSON kinds at all, so there the same pair is one value.
    let report = compare_rows(
        &golden,
        &stringified_pk,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
}

/// The same asymmetry at a multicell set's elements — the dump's other stringified
/// position. The golden's `["-2","-1"]` pairs with the CLI's `[-2,-1]`; the CLI
/// emitting the strings itself is a divergence.
#[test]
fn a_multicell_set_element_the_cli_stringifies_is_a_divergence() {
    let schema = schema_of(SET_DDL, "t");
    let golden = vec![row(&[
        ("id", json!("1")),
        ("s", json!(["-2", "-1"])),
        ("fs", json!([-2, -1])),
    ])];
    let stringified = vec![row(&[
        ("id", json!(1)),
        ("s", json!(["-2", "-1"])),
        ("fs", json!([-2, -1])),
    ])];
    let report = compare_rows(
        &golden,
        &stringified,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "a multicell `set<int>` element rendered as a JSON string must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains(".s"), "{:?}", report.diffs);
}

/// Finding M1's asymmetry AT A MAP KEY (issue #1491 review finding N1).
///
/// The relaxation belongs to the GOLDEN alone here too. A JSON object's key can
/// only be a string, so the golden's `{"-5": …}` is stringified BY THE FORMAT and
/// states nothing about kind; the CLI spells a map as an array of
/// `{"key":…,"value":…}` objects, whose `key` keeps the JSON kind its declared
/// type implies. So a `map<int,…>` key must be a JSON NUMBER on the CLI side, and
/// the string spelling is a divergence — which the two-sided rule accepted.
#[test]
fn a_map_key_relaxation_is_the_goldens_and_the_cli_keeps_its_declared_kind() {
    let schema = schema_of(INT_MAP_DDL, "t");
    let golden = vec![row(&[("id", json!("1")), ("mi", json!({"-5": "x"}))])];

    let natural = vec![row(&[
        ("id", json!(1)),
        ("mi", json!([{"key": -5, "value": "x"}])),
    ])];
    let report = compare_rows(&golden, &natural, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "the golden's stringified key pairs with the CLI's number: {:?}",
        report.diffs
    );

    let stringified = vec![row(&[
        ("id", json!(1)),
        ("mi", json!([{"key": "-5", "value": "x"}])),
    ])];
    let report = compare_rows(
        &golden,
        &stringified,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "a `map<int,…>` key the JSON egress spells as a string must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains(".mi"), "{:?}", report.diffs);

    // CSV carries no JSON kinds at all, so the same CLI spelling is correct there
    // and must NOT be read as this divergence.
    let csv = vec![row(&[("id", json!("1")), ("mi", json!("{-5: x}"))])];
    let report = compare_rows(&golden, &csv, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(
        report.diffs.is_empty(),
        "CSV has no kinds, so its textual key still pairs: {:?}",
        report.diffs
    );
}

/// Finding R1 at collection elements, where `frozen` decides the answer and the
/// committed DDL is what knows it: a MULTICELL `set<int>`'s elements are cell
/// paths (`writeString`, so the golden carries `["-2","-1"]`), while a
/// `frozen<set<int>>` is one value cell (`writeRawValue`, golden `[-2,-1]`).
#[test]
fn set_element_kinding_follows_frozen_from_the_ddl() {
    let schema = schema_of(SET_DDL, "t");
    // The shapes the dump actually produces for these two columns.
    let golden = vec![row(&[
        ("id", json!("1")),
        ("s", json!(["-2", "-1"])),
        ("fs", json!([-2, -1])),
    ])];
    let cli = vec![row(&[
        ("id", json!(1)),
        ("s", json!([-2, -1])),
        ("fs", json!([-2, -1])),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "a multicell set's stringified paths must pair with the CLI's numbers: {:?}",
        report.diffs
    );

    // The frozen set's elements are NOT stringified by the dump, so a string
    // there is a divergence — the permissive rule accepted it.
    let wrong = vec![row(&[
        ("id", json!(1)),
        ("s", json!([-2, -1])),
        ("fs", json!(["-2", "-1"])),
    ])];
    let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(report.diffs[0].contains(".fs:"), "{:?}", report.diffs);
}

/// The kinding of a set's elements is a TOP-LEVEL property, and a nested set is
/// refused loudly rather than compared with a borrowed relaxation.
///
/// A stringified set is a MULTICELL one, which can only be a whole column; a set
/// nested inside another container is frozen and its elements are ordinary cell
/// values. The dump writes a multicell `set<frozen<set<int>>>`'s cell path as ONE
/// string (`writeString(nameComparator().getString(...))`), so the golden's member
/// is not an array at all and the comparison says so.
#[test]
fn a_nested_set_does_not_inherit_the_columns_stringified_kinding() {
    let schema = schema_of(
        "CREATE TABLE t (id int PRIMARY KEY, ss set<frozen<set<int>>>);",
        "t",
    );
    // What the dump really produces for this column: the path IS the whole frozen
    // set, as one string.
    let golden = vec![row(&[("id", json!("1")), ("ss", json!(["{1, 2}"]))])];
    let cli = vec![row(&[("id", json!(1)), ("ss", json!([[1, 2]]))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "the shapes differ, and that is what must be reported: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains(".ss"), "{:?}", report.diffs);

    // And where a nested set IS an array on both sides — a frozen column, so the
    // column kinding is natural anyway — the inner elements are compared by kind:
    // a JSON string is not a JSON number.
    let frozen = schema_of(
        "CREATE TABLE t (id int PRIMARY KEY, fs frozen<set<frozen<set<int>>>>);",
        "t",
    );
    let golden = vec![row(&[("id", json!("1")), ("fs", json!([[1, 2]]))])];
    let ok = vec![row(&[("id", json!(1)), ("fs", json!([[1, 2]]))])];
    let report = compare_rows(&golden, &ok, &frozen, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    let stringified = vec![row(&[("id", json!(1)), ("fs", json!([["1", "2"]]))])];
    let report = compare_rows(
        &golden,
        &stringified,
        &frozen,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
}

/// Map KEYS are canonicalized under the declared KEY type: numeric for
/// `map<int,…>` (the dump renders every path as a string), exact for
/// `map<text,…>`.
#[test]
fn map_keys_are_canonicalized_under_the_declared_key_type() {
    let schema = schema_of(INT_MAP_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("mi", json!({"-5": "v"}))])];
    let cli = vec![row(&[
        ("id", json!(1)),
        ("mi", json!([{"key": -5, "value": "v"}])),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    let wrong = vec![row(&[
        ("id", json!(1)),
        ("mi", json!([{"key": -6, "value": "v"}])),
    ])];
    let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);

    // A text-keyed map compares its keys exactly.
    let schema = schema_of(UDT_MAP_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "ma",
            json!({"00000": {"street": "s", "city": "c", "zip": "z"}}),
        ),
    ])];
    let wrong = vec![row(&[
        ("id", json!(1)),
        (
            "ma",
            json!([{"key": 0, "value": {"street": "s", "city": "c", "zip": "z"}}]),
        ),
    ])];
    let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "a text map key \"00000\" must not equal the number 0: {:?}",
        report.diffs
    );
}

/// CSV keeps the same type rule: a `text` cell is exact even though every CSV
/// field arrives as text.
#[test]
fn the_csv_lane_uses_the_declared_types_too() {
    let schema = schema_of(TEXT_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("zip", json!("00000"))])];
    let ok = vec![row(&[("id", json!("1")), ("zip", json!("00000"))])];
    let report = compare_rows(&golden, &ok, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    let wrong = vec![row(&[("id", json!("1")), ("zip", json!("0"))])];
    let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Csv);
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(report.diffs[0].contains(".zip:"), "{:?}", report.diffs);
}

// Review findings F1/F2/F3/F5: four ways a comparison could be made to agree
// with itself. Each is pinned from BOTH sides — the shape that must pass and
// the shape that must fail — so narrowing the rule back re-reds one of these.

const PERSON_DDL: &str = "CREATE TYPE person (first_name text, last_name text, age int); \
     CREATE TABLE t (id int PRIMARY KEY, p frozen<person>);";

/// The `udt_nested` shape, transcribed from the committed
/// `test-data/schemas/compaction-parity-udt.cql`: a frozen UDT with a frozen UDT
/// field. It was the subject of this lane's `e.home` gap — the one declared gap
/// whose divergence was a UDT FIELD's rather than a whole column's — until #3631
/// made the nested frozen UDT decode and the gap retired itself. The DDL and the
/// fixtures below stay: they are how the FIELD-scoped machinery is covered, and
/// that machinery is what retired the gap.
const NESTED_UDT_DDL: &str = "CREATE TYPE address (street text, city text, zip text); \
     CREATE TYPE employee (name text, home frozen<address>, level int); \
     CREATE TABLE t (id int PRIMARY KEY, e frozen<employee>);";

/// F2: the key of a `map<text,…>` must be compared BY JSON KIND as well as by
/// text. The CLI's key used to be stringified before the declared key type
/// could be applied, so an emitted numeric key `0` satisfied the golden's
/// `"0"` — defeating the typed comparison in the one place a map most needs
/// it. The `map<int,…>` pairing across spellings must still work.
#[test]
fn a_text_map_key_is_compared_by_kind_and_a_numeric_one_still_pairs() {
    let schema = schema_of(UDT_MAP_DDL, "t");
    let address = json!({"street": "s", "city": "c", "zip": "z"});
    let golden = vec![row(&[("id", json!(1)), ("ma", json!({"0": address}))])];
    // Since #3629 the JSON egress renders a UDT as its declared fields and
    // nothing else, i.e. the same shape the golden carries.
    let cli_address = json!({"street": "s", "city": "c", "zip": "z"});

    let right = vec![row(&[
        ("id", json!(1)),
        ("ma", json!([{"key": "0", "value": cli_address}])),
    ])];
    let report = compare_rows(&golden, &right, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "the text key \"0\" must pair with the text key \"0\": {:?}",
        report.diffs
    );

    let numeric = vec![row(&[
        ("id", json!(1)),
        ("ma", json!([{"key": 0, "value": cli_address}])),
    ])];
    let report = compare_rows(&golden, &numeric, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "a text map key must not be satisfied by the JSON number 0: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains("map key"), "{:?}", report.diffs);

    // The int-keyed map still pairs the dump's string path with a number.
    let schema = schema_of(INT_MAP_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("mi", json!({"-5": "v"}))])];
    let cli = vec![row(&[
        ("id", json!(1)),
        ("mi", json!([{"key": -5, "value": "v"}])),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
}

/// R3, INVERTED BY #3629. The JSON egress used to inject a `_type` discriminator
/// naming the UDT's type, and this lane required it to be present, a string, and
/// the name the committed `CREATE TYPE` declares. `cassandra-5.0.8`'s
/// `UserType.toJSONString` writes `{"field": value, …}` and NO type key, so that
/// requirement demanded output the reference tool never produces — and it
/// collided with any UDT declaring a field of that name. The injection is gone,
/// and what this lane must now catch is its REINTRODUCTION: an object key the
/// `CREATE TYPE` does not declare is a divergence, whatever it is called.
#[test]
fn a_json_udt_key_the_ddl_does_not_declare_is_a_divergence() {
    let schema = schema_of(PERSON_DDL, "t");
    let fields = json!({"first_name": "A", "last_name": "B", "age": 30});
    let golden = vec![row(&[("id", json!(1)), ("p", fields.clone())])];

    // The shape that must PASS: the declared fields and nothing else, i.e. the
    // golden's own shape.
    let right = vec![row(&[("id", json!(1)), ("p", fields.clone())])];
    let report = compare_rows(&golden, &right, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    // Any undeclared key FAILS, naming the column and the type. `_type` is the
    // historical injection (issue #3629); `_keyspace` was its sibling; `zzz`
    // shows the rule is about DECLARATION, not about a marker denylist.
    for extra in ["_type", "_keyspace", "zzz"] {
        let mut o = fields.clone();
        o[extra] = json!("person");
        let cli = vec![row(&[("id", json!(1)), ("p", o)])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "an undeclared `{extra}` key must fail: {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains(".p:") && report.diffs[0].contains("person"),
            "the failure must name the column and the declared type: {:?}",
            report.diffs
        );
    }
}

/// The CSV lane has NO discriminator to check, and that is a property of the
/// format: `ValueFormatter` renders a UDT as `{field: value, …}` with no type
/// name, so the CSV decoder produces the `{key,value}` list and this rule
/// deliberately does not apply there.
#[test]
fn the_csv_lane_carries_no_udt_discriminator() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        ("p", json!({"first_name": "A", "last_name": "B", "age": 30})),
    ])];
    let cli = vec![row(&[
        ("id", json!("1")),
        ("p", json!("{first_name: A, last_name: B, age: 30}")),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(report.container_cells, 1, "the UDT cell must be compared");
}

/// J2: a duplicate CSV header used to OVERWRITE the earlier column of the same
/// name while the row map was built, so egress carrying a spurious duplicate column
/// compared equal to the golden whenever the LAST occurrence matched — and the
/// spurious column vanished from the shape check and the cell count too. A duplicate
/// header is malformed egress, so it is reported rather than reconciled.
#[test]
fn a_duplicate_csv_header_is_a_named_failure() {
    let why = cli_csv_rows("id,v,v\n1,x,x\n").expect_err("a duplicate header is malformed");
    assert!(
        why.contains("repeats the column `v`") && why.contains("fields 1 and 2"),
        "the failure must name the duplicated column and where it repeats: {why}"
    );
    // The distinct-header form is the ordinary one, so the rule is about the
    // DUPLICATE and not about the reader.
    let rows = cli_csv_rows("id,v,w\n1,x,y\n").expect("distinct headers are readable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3, "every column must survive into the row");
}

/// K2 — the JSON half of the same defect, at the reader that has to catch it, and
/// written in the order that makes last-wins pass: the SECOND `v` is the golden's
/// value, so under `serde_json::Value`'s own parse this document compared EQUAL to
/// a correct row.
#[test]
fn a_duplicate_json_egress_column_is_a_named_failure_even_when_the_last_one_matches() {
    let why = cli_json_rows(r#"[{"id":1,"v":"SPURIOUS","v":"x"}]"#)
        .expect_err("a duplicate JSON object key is malformed egress");
    assert!(
        why.contains("duplicate object key `v`") && why.contains("egress[0]"),
        "the failure must name the row and the duplicated key: {why}"
    );
    // The distinct-key form is the ordinary one, so the rule is about the
    // DUPLICATE and not about the reader.
    let rows = cli_json_rows(r#"[{"id":1,"v":"x","w":"y"}]"#).expect("distinct keys are readable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3, "every column must survive into the row");
}

/// K2, one level in: a duplicated UDT FIELD in the JSON egress. The comparison
/// never sees it — the parse refuses first — which is the point: with last-wins,
/// `compare_udt`'s own duplicate rule (the CSV path) had no JSON counterpart
/// because the duplicate was gone before it ran.
#[test]
fn a_duplicate_json_udt_field_is_refused_before_the_comparison_sees_it() {
    let why = cli_json_rows(r#"[{"id":1,"p":{"first_name":"SPURIOUS","first_name":"A"}}]"#)
        .expect_err("a duplicate UDT field is malformed egress");
    assert!(
        why.contains("duplicate object key `first_name`") && why.contains("egress[0].p"),
        "the failure must name the field path and the duplicated field: {why}"
    );
}

/// J2, UDT side: a repeated CSV field name used to overwrite, so egress carrying an
/// EXTRA spurious field passed whenever the last occurrence happened to match the
/// golden. The duplicate below is written in exactly that order — the second
/// `first_name` is the golden's value — so the case would pass under the old
/// last-wins insert and can only be caught by reporting the duplicate.
#[test]
fn a_duplicate_csv_udt_field_is_a_named_failure_even_when_the_last_one_matches() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        ("p", json!({"first_name": "A", "last_name": "B", "age": 30})),
    ])];
    let cli = vec![row(&[
        ("id", json!("1")),
        (
            "p",
            json!("{first_name: SPURIOUS, first_name: A, last_name: B, age: 30}"),
        ),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);
    assert_eq!(
        report.diffs.len(),
        1,
        "the duplicate field must be reported: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].contains("repeats the field `first_name`")
            && report.diffs[0].contains("person"),
        "the failure must name the UDT and the duplicated field: {:?}",
        report.diffs
    );
}

/// F3: each egress format renders a UDT exactly one way, so only that way is
/// accepted. Accepting both meant a JSON UDT that regressed to the map
/// `{key,value}` spelling still passed.
#[test]
fn the_accepted_udt_representation_is_scoped_to_the_egress_format() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": "Lovelace", "age": 36}),
        ),
    ])];

    // JSON renders a UDT as a field→value object: since #3629 the declared
    // fields and nothing else, i.e. the golden's own shape.
    let json_object = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada",
                     "last_name": "Lovelace", "age": 36}),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &json_object,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    // The same values in the MAP spelling are a representation regression.
    let json_pairs = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!([{"key": "first_name", "value": "Ada"},
                     {"key": "last_name", "value": "Lovelace"},
                     {"key": "age", "value": 36}]),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &json_pairs,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "a JSON UDT emitted as a {{key,value}} list must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains("person"), "{:?}", report.diffs);

    // CSV delivers one flat field, which this lane's decoder turns into the
    // pair spelling; that IS the CSV representation and it must pass.
    let csv = vec![row(&[
        ("id", json!("1")),
        (
            "p",
            json!("{first_name: Ada, last_name: Lovelace, age: 36}"),
        ),
    ])];
    let report = compare_rows(&golden, &csv, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    // A CSV cell that arrives already as an object never went through the
    // decoder, so it is not the CSV representation either.
    let report = compare_rows(
        &golden,
        &json_object,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
}

/// F1 at the comparison level: inside a CSV container an empty member and a
/// null member are distinguishable, so a null UDT field rendered as empty text
/// must FAIL — while a top-level empty CSV field still reads as null.
#[test]
fn an_empty_csv_member_does_not_satisfy_a_null_udt_field() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": null, "age": 36}),
        ),
    ])];

    let spelled_null = vec![row(&[
        ("id", json!("1")),
        ("p", json!("{first_name: Ada, last_name: null, age: 36}")),
    ])];
    let report = compare_rows(
        &golden,
        &spelled_null,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert!(
        report.diffs.is_empty(),
        "`last_name: null` is the rendering of a null member: {:?}",
        report.diffs
    );

    let spelled_empty = vec![row(&[
        ("id", json!("1")),
        ("p", json!("{first_name: Ada, last_name: , age: 36}")),
    ])];
    let report = compare_rows(
        &golden,
        &spelled_empty,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "`last_name: ` is NOT the rendering of a null member: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains("last_name"), "{:?}", report.diffs);

    // Unchanged at the top level, where the format genuinely cannot tell them
    // apart: an empty CSV field still satisfies a null column.
    let schema = schema_of(TEXT_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("zip", json!(null))])];
    let cli = vec![row(&[("id", json!("1")), ("zip", json!(""))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
}

/// The `udt_nested` golden's `e` value, with `home` decoded as `sstabledump`
/// decodes it. The CLI's spelling of the same value carries the same declared
/// fields (issue #3629 removed the injected type key); the blob-hex spelling for
/// `home` used below is the PRE-#3631 rendering, kept as a synthetic divergence so
/// the field-scoped machinery still has a subject now that the real `e.home` gap
/// has retired.
fn nested_udt_golden() -> Vec<Row> {
    vec![row(&[
        ("id", json!(1)),
        (
            "e",
            json!({"name": "Grace",
                     "home": {"street": "1 Navy Way", "city": "Arlington", "zip": "22201"},
                     "level": 9}),
        ),
    ])]
}

/// F5: an exclusion can name ONE UDT field, so the sibling fields stay
/// compared. Excluding the whole column instead left `udt_nested` comparing
/// nothing but its primary key while its comment claimed otherwise.
///
/// The `e.home` configuration as it stood before #3631, divergence included: the
/// excluded field arrives as blob hex, which is what that gap declared. Synthetic
/// since #3631 — no case declares this gap any more.
#[test]
fn a_field_scoped_skip_still_compares_the_sibling_fields() {
    let schema = schema_of(NESTED_UDT_DDL, "t");
    let golden = nested_udt_golden();
    let skip = [("e.home", Divergence::NestedFrozenUdtRendersAsBlobHex)];

    // The excluded field may diverge…
    let diverged_field = vec![row(&[
        ("id", json!(1)),
        (
            "e",
            json!({"name": "Grace",
                     "home": "0x0000000a31204e617679205761790000000941726c696e67746f6e",
                     "level": 9}),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &diverged_field,
        &schema,
        &["id"],
        &[],
        &skip,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(
        report.stale_skips.is_empty(),
        "the skip must suppress the divergence: {:?}",
        report.stale_skips
    );

    // …and its SIBLINGS must still be compared, which a whole-column skip
    // could never do.
    for wrong in ["name", "level"] {
        let mut fields = serde_json::Map::new();
        fields.insert("name".into(), json!("Grace"));
        fields.insert(
            "home".into(),
            json!("0x0000000a31204e617679205761790000000941726c696e67746f6e"),
        );
        fields.insert("level".into(), json!(9));
        fields.insert(wrong.to_string(), json!("WRONG"));
        let cli = vec![row(&[("id", json!(1)), ("e", Value::Object(fields))])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &skip, Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a wrong `{wrong}` must still fail under an `e.home` skip: {:?}",
            report.diffs
        );
        assert!(report.diffs[0].contains(wrong), "{:?}", report.diffs);
    }
}

/// F5, the other half: a declared exclusion that matches NOTHING is reported,
/// so a gap that has closed cannot stay standing and keep hiding coverage.
#[test]
fn a_skip_that_matches_nothing_is_reported() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": "Lovelace", "age": 36}),
        ),
    ])];
    let cli = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada",
                     "last_name": "Lovelace", "age": 36}),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &cli,
        &schema,
        &["id"],
        &[],
        &[("p.middle_name", Divergence::NestedFrozenUdtRendersAsBlobHex)],
        Egress::Json,
    );
    assert_eq!(
        report.stale_skips,
        vec!["`p.middle_name` (matched no value in the walk at all)".to_string()],
        "a skip path nothing matched must be reported, with the cause"
    );
}

// =======================================================================
// A boolean/blob PARTITION KEY: the comparator's own view of finding T1
// =======================================================================

/// The end-to-end shape of finding T1, at the position `column_kinding` decides:
/// a boolean PARTITION KEY. `serializePartitionKey` writes
/// `writeString(keyValidator.getString(v))`, so the golden carries `"true"`, while
/// the CLI renders the column at its declared type's JSON kind, i.e. `true`. Both
/// sides are CORRECT, so the comparator must report no diff — before finding T1 it
/// reported one, which is a lane that reds on correct input.
///
/// The golden spelling here is derived from `cassandra-5.0.8`
/// (`BooleanSerializer.toString` = `value.toString()`), not from any CQLite output.
#[test]
fn a_correct_boolean_partition_key_is_not_a_divergence() {
    let schema = schema_of("CREATE TABLE t (flag boolean PRIMARY KEY, v text);", "t");
    let golden = vec![row(&[("flag", json!("true")), ("v", json!("x"))])];
    let cli = vec![row(&[("flag", json!(true)), ("v", json!("x"))])];
    let report = compare_rows(&golden, &cli, &schema, &["flag"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "`sstabledump` stringifies a partition key and the CLI does not: {:?}",
        report.diffs
    );
    assert_eq!(
        report.compared_cells, 2,
        "both declared columns are compared"
    );
}

/// Pinned from the other side, so the relaxation above cannot drift into accepting
/// anything: the WRONG boolean still diverges, and so does a CLI that spells the
/// column as a string instead of a boolean.
///
/// A wrong PARTITION KEY value changes the emitted key SET rather than reordering
/// it, so no `row order:` line accompanies the value diff (only a reordering — the
/// same keys in a different sequence — is reported as one, finding V2). The value
/// diff is therefore asserted by NAME rather than by count, so it cannot be
/// satisfied by some other line.
#[test]
fn a_wrong_boolean_partition_key_still_diverges() {
    let schema = schema_of("CREATE TABLE t (flag boolean PRIMARY KEY, v text);", "t");
    let golden = vec![row(&[("flag", json!("true")), ("v", json!("x"))])];

    // Same column, opposite value.
    let flipped = vec![row(&[("flag", json!(false)), ("v", json!("x"))])];
    let report = compare_rows(
        &golden,
        &flipped,
        &schema,
        &["flag"],
        &[],
        &[],
        Egress::Json,
    );
    let value_diffs: Vec<&String> = report
        .diffs
        .iter()
        .filter(|d| d.contains(".flag: golden"))
        .collect();
    assert_eq!(
        value_diffs.len(),
        1,
        "the opposite boolean must be reported as a VALUE diff: {:?}",
        report.diffs
    );
    assert!(
        value_diffs[0].contains("bool:true") && value_diffs[0].contains("bool:false"),
        "the message must name both values: {value_diffs:?}"
    );

    // The ASYMMETRY: the CLI may not spell a boolean column as a JSON string, even
    // at a position where the golden is stringified. This one stands alone: the two
    // sides' key SETS differ rather than being a permutation of each other, so the
    // row-order check reports nothing (finding V2), and the untyped pairing
    // projection reads both spellings as the same boolean, so the rows still pair.
    let stringy = vec![row(&[("flag", json!("true")), ("v", json!("x"))])];
    let report = compare_rows(
        &golden,
        &stringy,
        &schema,
        &["flag"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "a CLI boolean rendered as a string must still fail: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].contains("golden bool:true vs cli text:true"),
        "the message must name both KINDS: {:?}",
        report.diffs
    );
}

/// The other stringified position `column_kinding` derives from the DDL: a
/// NON-FROZEN `set<boolean>`, whose golden elements are the cells' `path`
/// (`writeString(nameComparator().getString(...))`). The row key here is an `int`,
/// so a wrong element is reported alone and the value rule is pinned with no
/// row-order companion.
#[test]
fn a_multicell_set_of_booleans_compares_across_the_stringification() {
    let schema = schema_of("CREATE TABLE t (id int PRIMARY KEY, s set<boolean>);", "t");
    let golden = vec![row(&[("id", json!("1")), ("s", json!(["false", "true"]))])];

    let cli = vec![row(&[("id", json!(1)), ("s", json!([false, true]))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "a multicell set's elements ARE its cell paths, which the dump stringifies: {:?}",
        report.diffs
    );
    assert_eq!(report.container_cells, 1, "the set is a container cell");

    // A wrong element still diverges.
    let wrong = vec![row(&[("id", json!(1)), ("s", json!([true, true]))])];
    let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "a flipped set element must be reported: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].contains("bool:false") && report.diffs[0].contains("bool:true"),
        "the message must name both values: {:?}",
        report.diffs
    );
}

/// The blob half of the same finding: `BytesSerializer.toString` is the bare hex,
/// so a blob partition key's golden has no `0x` while every other position (and the
/// CLI) carries one. Pinned from both sides.
#[test]
fn a_blob_partition_key_compares_across_the_0x_prefix_only() {
    let schema = schema_of("CREATE TABLE t (k blob PRIMARY KEY, v text);", "t");
    let golden = vec![row(&[("k", json!("deadbeef")), ("v", json!("x"))])];

    let cli = vec![row(&[("k", json!("0xdeadbeef")), ("v", json!("x"))])];
    let report = compare_rows(&golden, &cli, &schema, &["k"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "`getString` drops the prefix `toJSONString` adds: {:?}",
        report.diffs
    );

    // Different bytes still diverge. As above, a changed PARTITION KEY also moves
    // the emitted key sequence, so the value diff is asserted by name.
    let wrong = vec![row(&[("k", json!("0xdeadbeee")), ("v", json!("x"))])];
    let report = compare_rows(&golden, &wrong, &schema, &["k"], &[], &[], Egress::Json);
    let value_diffs: Vec<&String> = report
        .diffs
        .iter()
        .filter(|d| d.contains(".k: golden"))
        .collect();
    assert_eq!(
        value_diffs.len(),
        1,
        "different bytes must still diverge: {:?}",
        report.diffs
    );
    assert!(
        value_diffs[0].contains("0xdeadbeef") && value_diffs[0].contains("0xdeadbeee"),
        "the message must name both spellings: {value_diffs:?}"
    );
}

#[path = "golden_value_compare_order_tests.rs"]
mod order;

#[path = "golden_value_compare_gap_tests.rs"]
mod gaps;

#[path = "golden_value_compare_gap_udt_tests.rs"]
mod gap_udt;

#[path = "golden_value_compare_refusal_tests.rs"]
mod refusals;

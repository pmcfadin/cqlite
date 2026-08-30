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

/// The refusal PATH, not just the predicate: no corpus fixture carries a
/// `, `-bearing collection member, so without this the wiring from
/// `csv_container::ambiguity` to the census counters never executes and the
/// lane's `0 REFUSED` line would be unfalsifiable.
#[test]
fn a_csv_unrepresentable_container_is_refused_and_named() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a, b"]))])];
    // The CLI text is IRRELEVANT to the refusal: it is decided from the
    // golden alone, so the defect under test can never cause it.
    let cli = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);

    assert!(
        report.diffs.is_empty(),
        "unexpected diffs: {:?}",
        report.diffs
    );
    assert_eq!(report.ambiguous_container_cells, 1);
    assert_eq!(
        report.container_cells, 0,
        "a refused cell is not a compared one"
    );
    assert_eq!(report.compared_cells, 1, "`id` is still compared");
    assert_eq!(report.ambiguity_reasons.len(), 1);
    assert!(
        report.ambiguity_reasons[0].starts_with("s ("),
        "the refusal must name its column: {:?}",
        report.ambiguity_reasons
    );
}

/// A representable container is compared, and a wrong member fails. Pins the
/// other side of the same branch so "refused" can never quietly become the
/// default.
#[test]
fn a_representable_container_is_compared_and_a_wrong_member_fails() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
    let good = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
    let report = compare_rows(&golden, &good, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(
        report.diffs.is_empty(),
        "unexpected diffs: {:?}",
        report.diffs
    );
    assert_eq!(report.container_cells, 1);
    assert_eq!(report.ambiguous_container_cells, 0);

    let bad = vec![row(&[("id", json!("1")), ("s", json!("{a, c}"))])];
    let report = compare_rows(&golden, &bad, &schema, &["id"], &[], &[], Egress::Csv);
    assert_eq!(
        report.diffs.len(),
        1,
        "a wrong member must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains(".s:"), "{:?}", report.diffs);
}

/// A CLI cell that is not the grammar at all is a DIVERGENCE, not a refusal.
#[test]
fn an_unparseable_container_is_reported_not_refused() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
    let cli = vec![row(&[("id", json!("1")), ("s", json!("a, b"))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);
    assert_eq!(report.ambiguous_container_cells, 0);
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("unparseable CSV container"),
        "{:?}",
        report.diffs
    );
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

/// A declared skip path still suppresses its column, so the
/// measured-divergence gaps keep working.
#[test]
fn a_declared_skip_column_is_not_required_to_be_rendered() {
    let schema = absent_schema();
    let golden = absent_golden();
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
        &["reg"],
        Egress::Json,
    );
    assert!(
        report.diffs.is_empty(),
        "a declared skip must stay declared: {:?}",
        report.diffs
    );
    assert_eq!(report.compared_cells, 3);
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
            json!([{"key": "home", "value": {"_type": "address", "street": "1 Navy Way",
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
                json!([{"key": "home", "value": {"_type": "address",
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
    // The JSON egress names a UDT's type in `_type`; the golden does not carry it.
    let cli_address = json!({"_type": "address", "street": "s", "city": "c", "zip": "z"});

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

/// R3: the JSON egress's `_type` discriminator is REQUIRED — present, a string,
/// and the name the committed `CREATE TYPE` declares. It used to be stripped
/// unconditionally, so all three of these regressions passed.
#[test]
fn the_json_udt_discriminator_must_name_the_declared_type() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        ("p", json!({"first_name": "A", "last_name": "B", "age": 30})),
    ])];
    let fields = json!({"first_name": "A", "last_name": "B", "age": 30});

    // The shape that must PASS: the declared name, in the field the egress uses.
    let right = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "person", "first_name": "A", "last_name": "B", "age": 30}),
        ),
    ])];
    let report = compare_rows(&golden, &right, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    // An unquoted CQL identifier is case-insensitive, so the case of the name is
    // not a divergence.
    let folded = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "PERSON", "first_name": "A", "last_name": "B", "age": 30}),
        ),
    ])];
    let report = compare_rows(&golden, &folded, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    // The three shapes that must FAIL, each naming the column and the type.
    for (why, cli_value) in [
        ("absent", fields.clone()),
        ("wrong name", {
            let mut o = fields.clone();
            o["_type"] = json!("address");
            o
        }),
        ("not a string", {
            let mut o = fields.clone();
            o["_type"] = json!(7);
            o
        }),
    ] {
        let cli = vec![row(&[("id", json!(1)), ("p", cli_value)])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a `_type` that is {why} must fail: {:?}",
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
    let why = cli_json_rows(
        r#"[{"id":1,"p":{"_type":"person","first_name":"SPURIOUS","first_name":"A"}}]"#,
    )
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

    // JSON renders a UDT as a field→value object (plus the `_type` the golden
    // does not carry).
    let json_object = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "person", "first_name": "Ada",
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

/// F5: an exclusion can name ONE UDT field, so the sibling fields stay
/// compared. Excluding the whole column instead left `udt_nested` comparing
/// nothing but its primary key while its comment claimed otherwise.
#[test]
fn a_field_scoped_skip_still_compares_the_sibling_fields() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": "Lovelace", "age": 36}),
        ),
    ])];
    let skip = ["p.last_name"];

    // The excluded field may diverge…
    let diverged_field = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "person", "first_name": "Ada",
                     "last_name": "0xdeadbeef", "age": 36}),
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
    assert!(report.skips_never_applied.is_empty(), "the skip must fire");

    // …and its SIBLINGS must still be compared, which a whole-column skip
    // could never do.
    for wrong in ["first_name", "age"] {
        let mut fields = serde_json::Map::new();
        fields.insert("_type".into(), json!("person"));
        fields.insert("first_name".into(), json!("Ada"));
        fields.insert("last_name".into(), json!("0xdeadbeef"));
        fields.insert("age".into(), json!(36));
        fields.insert(wrong.to_string(), json!("WRONG"));
        let cli = vec![row(&[("id", json!(1)), ("p", Value::Object(fields))])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &skip, Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a wrong `{wrong}` must still fail under a `p.last_name` skip: {:?}",
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
            json!({"_type": "person", "first_name": "Ada",
                     "last_name": "Lovelace", "age": 36}),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &cli,
        &schema,
        &["id"],
        &[],
        &["p.middle_name"],
        Egress::Json,
    );
    assert_eq!(
        report.skips_never_applied,
        vec!["p.middle_name".to_string()],
        "a skip path nothing matched must be reported"
    );
}

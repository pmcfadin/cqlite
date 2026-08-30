//! Unit coverage for the ORDER the two sides emit (issue #1491) — rows, a map's
//! entries and a UDT's fields.
//!
//! Split out of `golden_value_compare_tests.rs` under the campsite rule (CLAUDE.md,
//! epic #1135). One responsibility: every case here pins a sequence, and each
//! expectation comes from an external authority — `sstabledump` walking the
//! SSTable in on-disk order, and `cassandra-5.0.8 UserType.toJSONString` iterating
//! a UDT's fields in declaration order — never from CQLite's own output.
//!
//! A child of that module, so the shared `row`/`schema_of` helpers and the DDL
//! constants are reached through `use super::*` and are stated once.

use super::*;
/// Finding N2's shape ONE LEVEL UP: the emitted ROW order is compared, not sorted
/// away.
///
/// Both sides walk one SSTable — `sstabledump` emits partitions in on-disk order
/// and rows in clustering order — so a reordering is a divergence. The sort stays,
/// because pairing must be total whatever the order, but it no longer DISCARDS the
/// property: measured over the whole corpus, all 56 case x format runs agree.
#[test]
fn the_emitted_row_order_is_compared_and_the_pairing_still_works() {
    let schema = schema_of(NUM_DDL, "t");
    let golden = vec![
        row(&[("id", json!("1")), ("n", json!(10))]),
        row(&[("id", json!("2")), ("n", json!(20))]),
    ];
    let in_order = vec![
        row(&[("id", json!(1)), ("n", json!(10))]),
        row(&[("id", json!(2)), ("n", json!(20))]),
    ];
    let report = compare_rows(&golden, &in_order, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    let reversed = vec![
        row(&[("id", json!(2)), ("n", json!(20))]),
        row(&[("id", json!(1)), ("n", json!(10))]),
    ];
    let report = compare_rows(&golden, &reversed, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "a reordered result set must fail exactly once: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].starts_with("row order:"),
        "{:?}",
        report.diffs
    );
    assert_eq!(
        report.compared_cells, 4,
        "the pairing still runs, so the values are still compared"
    );

    // A reordered result set with a WRONG value reports both: the order line and
    // the value line, because the pairing is by key and not by position.
    let reordered_and_wrong = vec![
        row(&[("id", json!(2)), ("n", json!(20))]),
        row(&[("id", json!(1)), ("n", json!(99))]),
    ];
    let report = compare_rows(
        &golden,
        &reordered_and_wrong,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 2, "{:?}", report.diffs);
}

/// A UDT's fields are emitted in DECLARATION order, on both sides.
///
/// `cassandra-5.0.8 UserType.toJSONString` iterates `stringFieldNames` in order,
/// so the golden's object is in declaration order and a reader of the same value
/// has no licence to permute it. Expectation from the committed `CREATE TYPE`,
/// never from either side's current output.
#[test]
fn a_udt_renders_its_fields_in_the_declared_order() {
    let schema = schema_of(PERSON_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        ("p", json!({"first_name": "A", "last_name": "B", "age": 30})),
    ])];
    let ordered = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "person", "first_name": "A", "last_name": "B", "age": 30}),
        ),
    ])];
    let report = compare_rows(&golden, &ordered, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    let permuted = vec![row(&[
        ("id", json!(1)),
        (
            "p",
            json!({"_type": "person", "age": 30, "first_name": "A", "last_name": "B"}),
        ),
    ])];
    let report = compare_rows(&golden, &permuted, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "a permuted UDT must fail: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].contains("declaration order"),
        "{:?}",
        report.diffs
    );

    // The CSV lane decodes the flat `{k: v, …}` text in its emitted order, so the
    // same permutation is caught there too.
    let csv_golden = vec![row(&[
        ("id", json!("1")),
        ("p", json!({"first_name": "A", "last_name": "B", "age": 30})),
    ])];
    let csv_ok = vec![row(&[
        ("id", json!("1")),
        ("p", json!("{first_name: A, last_name: B, age: 30}")),
    ])];
    let report = compare_rows(
        &csv_golden,
        &csv_ok,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    let csv_permuted = vec![row(&[
        ("id", json!("1")),
        ("p", json!("{age: 30, first_name: A, last_name: B}")),
    ])];
    let report = compare_rows(
        &csv_golden,
        &csv_permuted,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "a permuted CSV UDT must fail: {:?}",
        report.diffs
    );

    // And the GOLDEN is held to the same rule: a transcription that permutes the
    // dump's own order is not the document this reader understands.
    let permuted_golden = vec![row(&[
        ("id", json!(1)),
        ("p", json!({"age": 30, "first_name": "A", "last_name": "B"})),
    ])];
    let report = compare_rows(
        &permuted_golden,
        &ordered,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("golden field order"),
        "{:?}",
        report.diffs
    );
}

/// Finding N2: a map's entries are compared IN EMITTED ORDER, so a reordering is
/// a divergence.
///
/// Cassandra stores a map's entries in key-comparator order and `sstabledump`
/// emits that on-disk order, so a reader of the same SSTable has no licence to
/// emit another one. Sorting both sides by canonicalized key first — the previous
/// rule — made this exact reversal compare EQUAL while the CSV decoder's own
/// documentation claimed member order was checked.
#[test]
fn a_reordered_map_is_a_divergence_in_both_egress_formats() {
    let schema = schema_of(INT_MAP_DDL, "t");
    let golden = vec![row(&[
        ("id", json!("1")),
        ("mi", json!({"-5": "a", "3": "b"})),
    ])];

    let in_order = vec![row(&[
        ("id", json!(1)),
        (
            "mi",
            json!([{"key": -5, "value": "a"}, {"key": 3, "value": "b"}]),
        ),
    ])];
    let report = compare_rows(&golden, &in_order, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "the emitted order agrees, so nothing diverges: {:?}",
        report.diffs
    );

    let reversed = vec![row(&[
        ("id", json!(1)),
        (
            "mi",
            json!([{"key": 3, "value": "b"}, {"key": -5, "value": "a"}]),
        ),
    ])];
    let report = compare_rows(&golden, &reversed, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(
        report.diffs.len(),
        1,
        "a reordered map must fail: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].contains("EMITTED order"),
        "the diagnostic must say what was compared: {:?}",
        report.diffs
    );

    // The CSV lane decodes the flat rendering into the same pair spelling, so the
    // same reordering is caught there too.
    let csv_golden = vec![row(&[
        ("id", json!("1")),
        ("mi", json!({"-5": "a", "3": "b"})),
    ])];
    let csv_in_order = vec![row(&[("id", json!("1")), ("mi", json!("{-5: a, 3: b}"))])];
    let report = compare_rows(
        &csv_golden,
        &csv_in_order,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    let csv_reversed = vec![row(&[("id", json!("1")), ("mi", json!("{3: b, -5: a}"))])];
    let report = compare_rows(
        &csv_golden,
        &csv_reversed,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "a reordered CSV map must fail too: {:?}",
        report.diffs
    );
}

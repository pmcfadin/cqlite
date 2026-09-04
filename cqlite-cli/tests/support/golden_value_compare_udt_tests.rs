//! W1: the `CREATE TYPE` is the authority for which UDT fields must EXIST.
//!
//! Companion to `golden_value_compare_udt.rs`, split out under the campsite rule
//! (CLAUDE.md, epic #1135). Every expectation here comes from the committed DDL or
//! from `cassandra-5.0.8` — never from CQLite's output (#3042).
//!
//! The property, from the pinned source: `UserType.toJSONString` writes
//! `for (int i = 0; i < types.size(); i++)` over the DECLARED type list and appends
//! `"null"` when the field's buffer is absent, so a dump of a frozen UDT carries
//! EVERY declared field. Measurable on the committed goldens as well —
//! `udt_frozen_person` renders `"last_name":null` and `udt_null_inner` renders a
//! nested `"city":null` rather than dropping either field. Therefore NEITHER SIDE
//! may default here, which is the one place a UDT field differs from a row's cell:
//! a never-written CELL is legitimately absent from a row's golden, a never-written
//! FIELD is not absent from a frozen UDT's rendering.
//!
//! Each case is pinned from BOTH sides — the complete shape that must pass and the
//! incomplete one that must fail — so narrowing the rule back re-reds a case.

// Reached through the parent comparison module, which is where `compare_rows` and
// the row/schema types live; the DDL parser is one level up again.
use super::super::super::schema::{from_ddl, TableSchema};
use super::super::gap::Divergence;
use super::super::{compare_rows, Egress, Row};
use serde_json::{json, Value};

/// `CREATE TYPE` declaring three fields, one of which every case below drops.
const PERSON_DDL: &str = "CREATE TYPE person (first_name text, last_name text, age int); \
     CREATE TABLE t (id int PRIMARY KEY, p frozen<person>);";

fn person_schema() -> TableSchema {
    match from_ddl(PERSON_DDL, "t") {
        Ok(schema) => schema,
        Err(why) => panic!("t: {why}"),
    }
}

fn row_of(pairs: &[(&str, Value)]) -> Row {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// The whole of W1: a field the `CREATE TYPE` declares and NEITHER side emits.
///
/// Side-vs-side agreement accepted it — the field was in neither the `missing` nor
/// the `extra` set, the field-order check filters by `contains_key`, and the
/// golden-field iteration never visited it — so an incomplete UDT value compared
/// equal even though the lane's stated invariant is that every declared field is
/// emitted. The same rule this comparator already applies to a row's COLUMNS, one
/// level down.
#[test]
fn a_declared_field_missing_from_both_sides_is_a_failure() {
    let schema = person_schema();

    // The complete shape, with the absent field rendered `null` exactly as
    // `UserType.toJSONString` writes it. This must PASS.
    let complete_golden = vec![row_of(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": null, "age": 36}),
        ),
    ])];
    let complete_cli = vec![row_of(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": null, "age": 36}),
        ),
    ])];
    let report = compare_rows(
        &complete_golden,
        &complete_cli,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert!(
        report.diffs.is_empty(),
        "a UDT carrying every declared field, `null` where the value is absent, is \
         what Cassandra emits: {:?}",
        report.diffs
    );

    // BOTH sides drop `last_name`. This is the shape that used to pass.
    let short_golden = vec![row_of(&[
        ("id", json!(1)),
        ("p", json!({"first_name": "Ada", "age": 36})),
    ])];
    let short_cli = vec![row_of(&[
        ("id", json!(1)),
        ("p", json!({"first_name": "Ada", "age": 36})),
    ])];
    let report = compare_rows(
        &short_golden,
        &short_cli,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "a field declared by CREATE TYPE and emitted by neither side is missing, \
         not agreed: {:?}",
        report.diffs
    );
    let why = &report.diffs[0];
    assert!(
        why.contains("last_name") && why.contains("person") && why.contains("at `p`"),
        "the failure must name the field, the UDT type and the column: {why}"
    );

    // Each side alone is reported too, and named as that side — the DDL check is
    // asked of each side separately, so a CLI that drops a field the golden carries
    // fails whichever way round the omission is.
    for (side, golden, cli) in [
        ("cli", &complete_golden, &short_cli),
        ("golden", &short_golden, &complete_cli),
    ] {
        let report = compare_rows(golden, cli, &schema, &["id"], &[], &[], Egress::Json);
        assert!(
            !report.diffs.is_empty(),
            "the {side} side dropping `last_name` is a divergence: {:?}",
            report.diffs
        );
    }
}

/// The rule holds on the CSV lane too, where the field set arrives from the flat
/// `{k: v, …}` decoding rather than from a JSON object — so the authority is the
/// DDL on both lanes and not the shape of whichever egress is being read.
#[test]
fn the_csv_lane_requires_every_declared_field_too() {
    let schema = person_schema();
    let golden = vec![row_of(&[
        ("id", json!(1)),
        (
            "p",
            json!({"first_name": "Ada", "last_name": "L", "age": 36}),
        ),
    ])];

    let complete = vec![row_of(&[
        ("id", json!("1")),
        ("p", json!("{first_name: Ada, last_name: L, age: 36}")),
    ])];
    let report = compare_rows(&golden, &complete, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);

    // Both sides short of `age`.
    let short_golden = vec![row_of(&[
        ("id", json!(1)),
        ("p", json!({"first_name": "Ada", "last_name": "L"})),
    ])];
    let short_csv = vec![row_of(&[
        ("id", json!("1")),
        ("p", json!("{first_name: Ada, last_name: L}")),
    ])];
    let report = compare_rows(
        &short_golden,
        &short_csv,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Csv,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(report.diffs[0].contains("age"), "{:?}", report.diffs);
}

/// The `udt_nested` shape from the committed
/// `test-data/schemas/compaction-parity-udt.cql`, which is where this lane's one
/// FIELD-scoped gap (`e.home`) lived until #3631 made the nested frozen UDT decode
/// and the gap retired itself. The pair below is synthetic from then on, and is
/// what keeps the FIELD-scoped rules covered.
const NESTED_EMPLOYEE_DDL: &str = "CREATE TYPE address (street text, city text, zip text); \
     CREATE TYPE employee (name text, home frozen<address>, level int); \
     CREATE TABLE t (id int PRIMARY KEY, e frozen<employee>);";

/// A declared exclusion excludes a VALUE, never a field's PRESENCE — the rule
/// `SkipPaths` already states for a row's columns, checked one level down.
///
/// `e.home` is excluded, so the two sides may disagree about its VALUE in the way
/// the gap declares; they may not agree by both DROPPING it. Both-sides-absent is
/// the discriminating shape here: side-vs-side agreement cannot see it, and the
/// exclusion must not suppress it either, or an egress that stopped rendering the
/// field entirely would pass while the declared gap reported itself as live.
///
/// The presence check runs at the `e` node, one level ABOVE the gap's root, so no
/// gap is even active where the absence is decided — which is why an absent field
/// cannot be absorbed however the gap's divergence is stated (review round 17).
#[test]
fn an_exclusion_cannot_excuse_an_absent_field() {
    let schema = match from_ddl(NESTED_EMPLOYEE_DDL, "t") {
        Ok(schema) => schema,
        Err(why) => panic!("t: {why}"),
    };
    let home = json!({"street": "1 Navy Way", "city": "Arlington", "zip": "22201"});
    let golden = vec![row_of(&[
        ("id", json!(1)),
        ("e", json!({"name": "Grace", "home": home, "level": 9})),
    ])];
    let skip = [("e.home", Divergence::NestedFrozenUdtRendersAsBlobHex)];

    // The exclusion doing its job: the field is PRESENT on both sides and diverges
    // exactly as the gap declares (blob hex where the golden decoded an object), so
    // the exclusion is applied and is not stale.
    let diverging = vec![row_of(&[
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
        &diverging,
        &schema,
        &["id"],
        &[],
        &skip,
        Egress::Json,
    );
    assert!(
        report.diffs.is_empty() && report.stale_skips.is_empty(),
        "an excluded VALUE divergence is suppressed: {:?} / {:?}",
        report.diffs,
        report.stale_skips
    );

    // The same exclusion where BOTH sides drop the field. Side-vs-side agreement
    // cannot see this, and the exclusion does not excuse it: the DDL declares
    // `home`, so its absence is a failure naming it.
    let dropped_golden = vec![row_of(&[
        ("id", json!(1)),
        ("e", json!({"name": "Grace", "level": 9})),
    ])];
    let dropped_cli = vec![row_of(&[
        ("id", json!(1)),
        ("e", json!({"name": "Grace", "level": 9})),
    ])];
    let report = compare_rows(
        &dropped_golden,
        &dropped_cli,
        &schema,
        &["id"],
        &[],
        &skip,
        Egress::Json,
    );
    assert_eq!(
        report.diffs.len(),
        1,
        "an exclusion excludes a value, never a field's presence: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].contains("home"),
        "the failure must name the absent declared field: {:?}",
        report.diffs
    );

    // And the ONE-SIDED absence stays a failure under the same exclusion, which is
    // what the side-vs-side check already caught.
    let report = compare_rows(
        &golden,
        &dropped_cli,
        &schema,
        &["id"],
        &[],
        &skip,
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
}

/// A `CREATE TYPE` declaring a field literally named `_type` USED TO BE REFUSED:
/// the JSON egress injected a `_type` discriminator into the same object, so the
/// declared field and the injected key were indistinguishable, and this lane
/// could not say which of the two a value was. Issue #3629 removed the
/// injection — `cassandra-5.0.8`'s `UserType.toJSONString` emits declared fields
/// and no type key, and the committed `sstabledump` golden for
/// `test-data/fixtures/issue_3504/` shows exactly that — so such a UDT is now an
/// ORDINARY value: comparable field by field, with no name reserved by us.
#[test]
fn a_udt_declaring_a_type_field_is_compared_like_any_other() {
    let schema = match from_ddl(
        "CREATE TYPE odd (_type text, v int); \
         CREATE TABLE t (id int PRIMARY KEY, o frozen<odd>);",
        "t",
    ) {
        Ok(schema) => schema,
        Err(why) => panic!("t: {why}"),
    };
    let golden = vec![row_of(&[
        ("id", json!(1)),
        ("o", json!({"_type": "x", "v": 2})),
    ])];

    let agreeing = vec![row_of(&[
        ("id", json!(1)),
        ("o", json!({"_type": "x", "v": 2})),
    ])];
    let report = compare_rows(&golden, &agreeing, &schema, &["id"], &[], &[], Egress::Json);
    assert!(
        report.diffs.is_empty(),
        "a UDT declaring `_type` is an ordinary value now: {:?}",
        report.diffs
    );

    // And its VALUE is really compared — the field is not silently dropped, which
    // is what the old discriminator strip did to it.
    let diverging = vec![row_of(&[
        ("id", json!(1)),
        ("o", json!({"_type": "WRONG", "v": 2})),
    ])];
    let report = compare_rows(
        &golden,
        &diverging,
        &schema,
        &["id"],
        &[],
        &[],
        Egress::Json,
    );
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("_type"),
        "the divergence must name the declared field: {:?}",
        report.diffs
    );
}

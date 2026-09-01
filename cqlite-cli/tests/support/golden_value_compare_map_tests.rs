//! Unit coverage for `compare::compare_map` with a CONTAINER-typed map key
//! (issue #3726), and for the three properties that must SURVIVE the change.
//!
//! Split into its own file under the campsite rule (CLAUDE.md, epic #1135):
//! `golden_value_compare_tests.rs` is at the ~1500-line test target.
//!
//! Driven through the PUBLIC surface (`compare_rows`) rather than the private walk,
//! so every case exercises the same entry point the lane does. Golden spellings are
//! quoted from the committed `test_nested_udt_keys.nested_udt_keys` golden
//! (`nb-1-big-Data.db.jsonl`); CLI spellings are MEASURED from
//! `cqlite … export --format json|csv` against that fixture. Nothing here is
//! derived from CQLite's current output as an EXPECTATION — the expectations are
//! the golden's, and the measured CLI spellings appear only as the input under
//! test.

use super::super::schema::TableSchema;
use super::super::{Egress, Row};
use super::compare_rows;
use serde_json::{json, Value};

/// The committed `CREATE TYPE`, and the two map types quoted verbatim from
/// `test-data/schemas/nested-udt-keys.cql`.
const UDT_DDL: &str = "CREATE TYPE key_part (label text, rank int);";
const MAP_TUPLE_UDT: &str = "frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>";
const MAP_SET_UDT: &str = "frozen<map<frozen<set<frozen<key_part>>>, int>>";

fn schema_for(cql_type: &str) -> TableSchema {
    let ddl = format!("{UDT_DDL} CREATE TABLE t (id int PRIMARY KEY, c {cql_type});");
    match super::super::schema::from_ddl(&ddl, "t") {
        Ok(schema) => schema,
        Err(why) => panic!("`{cql_type}` must parse: {why}"),
    }
}

fn row_of(value: Value) -> Row {
    [("id".to_string(), json!(1)), ("c".to_string(), value)]
        .into_iter()
        .collect()
}

/// The diffs `compare_rows` reports for one golden/CLI pair of a single-column
/// table.
fn diffs(cql_type: &str, golden: Value, cli: Value, egress: Egress) -> Vec<String> {
    let schema = schema_for(cql_type);
    let report = compare_rows(
        &[row_of(golden)],
        &[row_of(cli)],
        &schema,
        &["id"],
        &[],
        &[],
        egress,
    );
    report.diffs
}

/// The golden's `f_map_tuple_udt` cell in the fixture's row `id=1`, byte for byte.
fn golden_two_entries() -> Value {
    json!({
        "[{\"label\": \"mkey-a\", \"rank\": 21}, 1]": 210,
        "[{\"label\": \"mkey-b\", \"rank\": 22}, 2]": 220
    })
}

/// The CLI's JSON spelling of that same cell, measured.
fn cli_two_entries() -> Value {
    json!([
        {"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210},
        {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
    ])
}

// =======================================================================
// The property this issue adds
// =======================================================================

/// A container key pairs, in both formats. The CSV spelling is the flat rendering
/// the CSV egress actually emits for that cell — measured — so this also pins the
/// decoder half.
#[test]
fn a_container_keyed_map_compares_equal_in_both_formats() {
    assert!(
        diffs(
            MAP_TUPLE_UDT,
            golden_two_entries(),
            cli_two_entries(),
            Egress::Json
        )
        .is_empty(),
        "the two JSON spellings of one map must compare equal"
    );
    let csv = json!("{({label: mkey-a, rank: 21}, 1): 210, ({label: mkey-b, rank: 22}, 2): 220}");
    assert!(
        diffs(MAP_TUPLE_UDT, golden_two_entries(), csv, Egress::Csv).is_empty(),
        "and so must the CSV rendering of it"
    );
}

/// The SET-keyed shape too, whose key holds a UDT one level further in — and whose
/// CSV key therefore reaches the decoder's `{key,value}` UDT spelling.
#[test]
fn a_set_keyed_map_compares_equal_in_both_formats() {
    let golden = json!({"[{\"label\": \"solo\", \"rank\": 99}]": 7});
    let json_cli = json!([{"key": [{"label": "solo", "rank": 99}], "value": 7}]);
    assert!(diffs(MAP_SET_UDT, golden.clone(), json_cli, Egress::Json).is_empty());
    let csv = json!("{{{label: solo, rank: 99}}: 7}");
    assert!(diffs(MAP_SET_UDT, golden, csv, Egress::Csv).is_empty());
}

// =======================================================================
// The three properties that must SURVIVE it
// =======================================================================

/// EMITTED ORDER (issue #1491 finding N2). Cassandra stores a map's entries in
/// key-comparator order and both the dump and a reader of the same SSTable see that
/// order, so a reordering is a divergence. Sorting by canonicalized key — which
/// this lane used to do — made reversing the CLI's entries compare equal, and a
/// container key must not reintroduce that.
#[test]
fn a_reordered_container_keyed_map_is_a_divergence() {
    let reordered = json!([
        {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220},
        {"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210}
    ]);
    let diffs = diffs(MAP_TUPLE_UDT, golden_two_entries(), reordered, Egress::Json);
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    assert!(
        diffs[0].contains("emitted position 0") && diffs[0].contains("EMITTED order"),
        "the diagnostic must name the position and the rule: {diffs:?}"
    );
}

/// The CLI is NEVER given [`Kinding::Stringified`] (issue #1491 findings M1/N1).
/// The golden's object key is stringified BY THE FORMAT; the CLI's `{key,value}`
/// key is not — so a CLI that emitted the key as the golden's own TEXT, rather than
/// as the decoded container, must fail.
#[test]
fn a_cli_key_spelled_as_the_goldens_text_is_a_divergence() {
    let as_text = json!([
        {"key": "[{\"label\": \"mkey-a\", \"rank\": 21}, 1]", "value": 210},
        {"key": "[{\"label\": \"mkey-b\", \"rank\": 22}, 2]", "value": 220}
    ]);
    let diffs = diffs(MAP_TUPLE_UDT, golden_two_entries(), as_text, Egress::Json);
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    assert!(
        diffs[0].contains("JSON array"),
        "the message must say the declared type's spelling is an array: {diffs:?}"
    );
}

/// The same asymmetry one level down: a numeric member of the key keeps its
/// declared type's JSON kind on the CLI side.
#[test]
fn a_cli_key_member_spelled_as_a_string_is_a_divergence() {
    let stringy = json!([
        {"key": [{"label": "mkey-a", "rank": "21"}, 1], "value": 210},
        {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
    ]);
    let diffs = diffs(MAP_TUPLE_UDT, golden_two_entries(), stringy, Egress::Json);
    assert_eq!(diffs.len(), 1, "{diffs:?}");
}

// =======================================================================
// What the retired whole-column skip used to suppress
// =======================================================================

/// Every one of these was UNCHECKED under the retired
/// `ContainerMapKeyNotPairableByThisLane` skip, which is the coverage this issue
/// recovers. Each is asserted to produce exactly one named diff.
#[test]
fn the_four_shapes_the_retired_skip_suppressed_are_now_reported() {
    let cases: &[(&str, Value, &str)] = &[
        (
            "a null where the golden has a map",
            Value::Null,
            "not both that shape",
        ),
        (
            "a malformed {key,value} array",
            json!([{"key": [{"label": "mkey-a", "rank": 21}, 1]}, {"value": 220}]),
            "{key,value} pair",
        ),
        (
            "a wrong entry COUNT",
            json!([{"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210}]),
            "map size golden 2 vs cli 1",
        ),
        (
            "a wrong tuple ARITY in the key",
            json!([
                {"key": [{"label": "mkey-a", "rank": 21}, 1, 7], "value": 210},
                {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
            ]),
            "arity",
        ),
    ];
    for (name, cli, expected) in cases {
        let diffs = diffs(
            MAP_TUPLE_UDT,
            golden_two_entries(),
            cli.clone(),
            Egress::Json,
        );
        assert_eq!(diffs.len(), 1, "{name}: {diffs:?}");
        assert!(
            diffs[0].contains(expected),
            "{name}: the diff must name `{expected}`: {diffs:?}"
        );
    }
    assert_eq!(cases.len(), 4, "the case floor for this recovered coverage");
}

/// A map VALUE beside a container key is still compared — the key is not the only
/// thing the recovered coverage buys.
#[test]
fn a_map_value_beside_a_container_key_is_compared() {
    let wrong_value = json!([
        {"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 211},
        {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
    ]);
    let diffs = diffs(
        MAP_TUPLE_UDT,
        golden_two_entries(),
        wrong_value,
        Egress::Json,
    );
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    assert!(diffs[0].contains("num:211"), "{diffs:?}");
}

/// The MEASURED `m_tuple_udt` disagreement, at the level `compare_map` sees it: the
/// golden's key is `getString`'s colon-joined text (the MULTICELL cell path, written
/// `writeString(ct.nameComparator().getString(...))` by
/// `cassandra-5.0.8 JsonTransformer.serializeCell`), which is not a `toJSONString`
/// document at all — so the pairing REFUSES and names the oracle it contradicts,
/// rather than guessing. The whole-column claim for that one column therefore stays
/// a declared gap (see `gap::Divergence`), and it is a VALUE disagreement, not a
/// lane limitation.
#[test]
fn a_getstring_spelled_golden_key_is_refused_and_names_the_oracle() {
    let diffs = diffs(
        MAP_TUPLE_UDT,
        json!({"charlie\\:3:8": 80}),
        json!([{"key": "0x0000001300000007636861726c696500000004000000030000000400000008", "value": 80}]),
        Egress::Json,
    );
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    assert!(
        diffs[0].contains("toJSONString") && diffs[0].contains("does not parse as JSON"),
        "{diffs:?}"
    );
}

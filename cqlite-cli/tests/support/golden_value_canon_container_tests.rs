//! Unit coverage for CONTAINERS in the canonical value model (issue #3726).
//!
//! Every expectation here comes from the committed DDL, from `cassandra-5.0.8`
//! source read at the pin, or from a MEASUREMENT taken against the committed
//! `test_nested_udt_keys.nested_udt_keys` fixture — never from CQLite's current
//! output (CLAUDE.md: a CQLite `file:line` is never format authority). Where a case
//! quotes the fixture, the quoted text is the golden's own bytes.
//!
//! Each rule is pinned from BOTH sides — the shape that must canonicalize and the
//! shape that must be REFUSED — so narrowing a rule back reds a case rather than
//! quietly widening what this lane accepts.

use super::super::compare::compare_rows;
use super::super::schema::{from_ddl, CqlType, TableSchema};
use super::super::{canon_typed, Canon, Depth, Egress, Kinding, Row};
use super::{golden_map_key_value, is_container_type};
use serde_json::{json, Value};

// =======================================================================
// Fixtures: the DDL is parsed exactly as the lane parses a committed file
// =======================================================================

/// The committed `CREATE TYPE` from `test-data/schemas/nested-udt-keys.cql`.
const UDT_DDL: &str = "CREATE TYPE key_part (label text, rank int);";

// The four container-keyed map types, QUOTED VERBATIM from that file's
// `CREATE TABLE nested_udt_keys` — the authority for what these cases are about.
const MAP_TUPLE_UDT: &str = "frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>";
const MAP_SET_UDT: &str = "frozen<map<frozen<set<frozen<key_part>>>, int>>";
const MAP_TUPLE_LIST_UDT: &str =
    "frozen<map<frozen<tuple<frozen<list<frozen<key_part>>>, int>>, int>>";

/// A one-column table of the given declared type, parsed through the REAL schema
/// reader so every case exercises the same authority the lane does — and so a type
/// this lane cannot parse fails here rather than being mocked into existence.
///
/// One column, because `compare::compare_rows` requires the egress to render EVERY
/// declared column: a wider schema would make the differential below report the
/// unrendered columns instead of the property under test.
fn column_schema(cql_type: &str) -> TableSchema {
    let ddl = format!("{UDT_DDL} CREATE TABLE t (id int PRIMARY KEY, c {cql_type});");
    match from_ddl(&ddl, "t") {
        Ok(schema) => schema,
        Err(why) => panic!("`{cql_type}` must parse: {why}"),
    }
}

fn column_ty(cql_type: &str) -> CqlType {
    let schema = column_schema(cql_type);
    match schema.column("c") {
        Some(column) => column.ty.clone(),
        None => panic!("the one-column DDL must declare `c`"),
    }
}

/// The declared KEY type of a map column, for the cases that canonicalize a key on
/// its own.
fn key_ty_of(cql_type: &str) -> CqlType {
    match column_ty(cql_type) {
        CqlType::Map(key_ty, _) => *key_ty,
        other => panic!("`{cql_type}` is not a map but `{}`", other.describe()),
    }
}

/// Canonicalize as the comparator canonicalizes a whole COLUMN value: at the top
/// level, with the kinding the caller states.
fn canon(v: &Value, egress: Egress, ty: &CqlType, kinding: Kinding) -> Result<Canon, String> {
    canon_typed(v, egress, ty, Depth::TopLevel, kinding)
}

/// The GOLDEN side of a frozen container column: natural kind
/// (`cassandra-5.0.8 JsonTransformer.serializeCell` writes a cell VALUE with
/// `writeRawValue(type.toJSONString(...))`, and `compare::column_kinding` says the
/// same thing from the DDL).
fn canon_golden(v: &Value, ty: &CqlType) -> Result<Canon, String> {
    canon(v, Egress::Json, ty, Kinding::Natural)
}

/// The CLI side, held to [`Kinding::Natural`] at every position (issue #1491 review
/// finding M1).
fn canon_cli(v: &Value, ty: &CqlType) -> Result<Canon, String> {
    canon(v, Egress::Json, ty, Kinding::Natural)
}

/// The golden's spelling of `f_map_tuple_udt` in the committed fixture's row
/// `id=1`, byte for byte from `nb-1-big-Data.db.jsonl`.
fn golden_map_tuple_udt() -> Value {
    json!({
        "[{\"label\": \"mkey-a\", \"rank\": 21}, 1]": 210,
        "[{\"label\": \"mkey-b\", \"rank\": 22}, 2]": 220
    })
}

/// The CLI's JSON spelling of the same cell, MEASURED with
/// `cqlite … export --format json` against that fixture.
fn cli_map_tuple_udt() -> Value {
    json!([
        {"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210},
        {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
    ])
}

// =======================================================================
// The two map spellings, and the ORACLE for the golden's object key
// =======================================================================

/// THE property this issue exists for. `cassandra-5.0.8 MapType.toJSONString` writes
/// `keys.toJSONString(kv, protocolVersion)` and quotes it only when it does not
/// already start with `"`, so a container key's JSON object key is exactly that
/// key value's own `toJSONString` text — which means the two sides' spellings of the
/// SAME map denote the same value and must canonicalize identically.
#[test]
fn the_two_spellings_of_a_container_keyed_map_canonicalize_alike() {
    let ty = column_ty(MAP_TUPLE_UDT);
    let golden = canon_golden(&golden_map_tuple_udt(), &ty).expect("golden canonicalizes");
    let cli = canon_cli(&cli_map_tuple_udt(), &ty).expect("cli canonicalizes");
    assert_eq!(golden, cli, "golden {golden:?} vs cli {cli:?}");
    // And it really did descend: a flat scalar canon would prove nothing.
    assert!(
        matches!(&golden, Canon::Entries(entries) if entries.len() == 2
            && matches!(&entries[0].0, Canon::Seq(slots) if slots.len() == 2)),
        "the key must canonicalize as a 2-slot tuple: {golden:?}"
    );
}

/// The same for a SCALAR-keyed map, which is the rule that already existed: the
/// golden's object key is stringified BY THE FORMAT, so `"7"` denotes the number 7
/// while the CLI's `{"key":7}` carries the number itself.
#[test]
fn a_scalar_keyed_map_keeps_the_stringified_golden_key_rule() {
    let ty = column_ty("frozen<map<text, int>>");
    let golden = canon_golden(&json!({"a": 1}), &ty).expect("golden canonicalizes");
    let cli = canon_cli(&json!([{"key": "a", "value": 1}]), &ty).expect("cli canonicalizes");
    assert_eq!(golden, cli);
    // And the ASYMMETRY survives: a `text` key is compared exactly, so a CLI that
    // emitted the key as a NUMBER does not satisfy the golden's string (finding N1).
    let numeric_key = canon_cli(&json!([{"key": 0, "value": 1}]), &ty).expect("canonicalizes");
    let golden_zero = canon_golden(&json!({"0": 1}), &ty).expect("canonicalizes");
    assert_ne!(
        golden_zero, numeric_key,
        "a numeric CLI key must not satisfy a `map<text,int>` golden key"
    );
}

/// A container key nested INSIDE a container key — the deepest committed shape,
/// `map<frozen<tuple<frozen<list<frozen<key_part>>>, int>>, int>`. Golden text
/// quoted from the fixture's row `id=1`.
#[test]
fn a_container_inside_a_container_key_canonicalizes_recursively() {
    let ty = column_ty(MAP_TUPLE_LIST_UDT);
    let golden = json!({
        "[[{\"label\": \"la\", \"rank\": 1}, {\"label\": \"lb\", \"rank\": 2}], 1]": 120
    });
    let cli = json!([{
        "key": [[{"label": "la", "rank": 1}, {"label": "lb", "rank": 2}], 1],
        "value": 120
    }]);
    assert_eq!(
        canon_golden(&golden, &ty).expect("golden canonicalizes"),
        canon_cli(&cli, &ty).expect("cli canonicalizes")
    );
}

/// Entries are compared IN EMITTED ORDER (issue #1491 finding N2): sorting them
/// once discarded a real defect, so the canonical form must stay order-sensitive.
#[test]
fn a_reordered_map_is_a_different_canon() {
    let ty = column_ty(MAP_TUPLE_UDT);
    let golden = canon_golden(&golden_map_tuple_udt(), &ty).expect("canonicalizes");
    let reversed = json!([
        {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220},
        {"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210}
    ]);
    assert_ne!(
        golden,
        canon_cli(&reversed, &ty).expect("canonicalizes"),
        "a map's emitted order is not free"
    );
}

/// A size difference is an UNEQUAL canon, not a refusal — the comparison names it,
/// and nothing is suppressed.
#[test]
fn a_map_entry_count_difference_is_unequal_and_not_a_refusal() {
    let ty = column_ty(MAP_TUPLE_UDT);
    let short = json!([{"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210}]);
    let cli = canon_cli(&short, &ty).expect("a one-entry map canonicalizes fine");
    assert_ne!(
        canon_golden(&golden_map_tuple_udt(), &ty).expect("canonicalizes"),
        cli
    );
}

// =======================================================================
// Refusals — each one pinned, and each one NAMED in its message
// =======================================================================

fn refusal(result: Result<Canon, String>) -> String {
    match result {
        Ok(canon) => panic!("expected a refusal, got {canon:?}"),
        Err(why) => why,
    }
}

/// A tuple whose arity is not the DECLARED one is REFUSED, mirroring
/// `compare::compare_value_body`'s tuple rule: the DDL fixes the arity, so a value
/// of another arity is not a value of this type at all.
#[test]
fn a_tuple_arity_that_is_not_the_declared_one_is_refused() {
    let ty = column_ty("frozen<tuple<text, int>>");
    let why = refusal(canon_golden(&json!(["a", 1, "extra"]), &ty));
    assert!(
        why.contains("arity") && why.contains("2 field(s)"),
        "the refusal must name the declared arity: {why}"
    );
    assert!(canon_golden(&json!(["a", 1]), &ty).is_ok());
}

/// The three UDT rules, each from `cassandra-5.0.8 UserType.toJSONString`, which
/// iterates the DECLARED type list and emits every declared field.
#[test]
fn the_udt_ddl_rules_are_refusals() {
    let ty = column_ty("frozen<key_part>");
    assert!(canon_golden(&json!({"label": "x", "rank": 1}), &ty).is_ok());

    let absent = refusal(canon_golden(&json!({"label": "x"}), &ty));
    assert!(
        absent.contains("rank") && absent.contains("not emitted"),
        "a declared field that is not emitted must be named: {absent}"
    );

    let undeclared = refusal(canon_golden(
        &json!({"label": "x", "rank": 1, "spurious": 2}),
        &ty,
    ));
    assert!(
        undeclared.contains("spurious") && undeclared.contains("not declared"),
        "an undeclared field must be named: {undeclared}"
    );

    let misordered = refusal(canon_golden(&json!({"rank": 1, "label": "x"}), &ty));
    assert!(
        misordered.contains("declaration order"),
        "the order rule must cite the declaration order: {misordered}"
    );
}

/// A golden map key that is not the `toJSONString` spelling the oracle states is
/// REFUSED rather than guessed at. This is the MEASURED `m_tuple_udt` shape: for a
/// MULTICELL map `JsonTransformer.serializeCell` writes the cell path with
/// `writeString(ct.nameComparator().getString(...))`, so the golden carries
/// `getString`'s colon-joined text and no JSON document at all.
#[test]
fn a_golden_map_key_that_does_not_parse_as_json_is_refused() {
    let ty = column_ty(MAP_TUPLE_UDT);
    let why = refusal(canon_golden(&json!({"charlie\\:3:8": 80}), &ty));
    assert!(
        why.contains("toJSONString") && why.contains("does not parse as JSON"),
        "the refusal must name the oracle it contradicts: {why}"
    );
}

/// A golden map key that parses but is the WRONG JSON shape for its declared kind
/// is refused too — `toJSONString` spells a list/set/tuple as an array and a
/// map/UDT as an object, so an object where an array is legal is not this type's
/// spelling.
#[test]
fn a_golden_map_key_of_the_wrong_json_shape_is_refused() {
    let ty = column_ty(MAP_TUPLE_UDT);
    let why = refusal(canon_golden(&json!({"{\"label\": \"x\"}": 80}), &ty));
    assert!(
        why.contains("a JSON array"),
        "the refusal must name the shape the declared type has: {why}"
    );
}

/// A container type at a STRINGIFIED position is a NAMED refusal, not a relaxation:
/// `getString` spells a whole frozen container as ONE flat string, a different
/// shape, which is the case the [`Kinding`] doc comment names as not covered. A
/// permissive arm here would excuse exactly the regression it can never describe
/// (roborev job 305's ruling).
#[test]
fn a_container_at_a_stringified_position_is_refused() {
    let ty = column_ty("frozen<list<int>>");
    let why = refusal(canon(
        &json!([1, 2]),
        Egress::Json,
        &ty,
        Kinding::Stringified,
    ));
    assert!(
        why.contains("STRINGIFIED") && why.contains("getString"),
        "the refusal must state why the position is not merely a kind relaxation: {why}"
    );
}

/// The declared type decides the SHAPE: a scalar where a container is declared, and
/// a container where a scalar is declared, are both refused.
#[test]
fn the_declared_type_decides_the_shape_in_both_directions() {
    let list = column_ty("frozen<list<int>>");
    let why = refusal(canon_golden(&json!("[1, 2]"), &list));
    assert!(why.contains("JSON array"), "{why}");

    let scalar = CqlType::Numeric("int".to_string());
    let why = refusal(canon_golden(&json!([1]), &scalar));
    assert!(
        why.contains("scalar type"),
        "the pre-existing scalar refusal must survive: {why}"
    );
}

/// A malformed CLI map entry is refused through the SAME `{key,value}` reader the
/// comparator uses (`compare::pair`), so the two cannot disagree about what a
/// malformed entry is.
#[test]
fn a_malformed_cli_map_entry_is_refused() {
    let ty = column_ty(MAP_TUPLE_UDT);
    let why = refusal(canon_cli(&json!([{"key": [1]}]), &ty));
    assert!(why.contains("{key,value}"), "{why}");
}

// =======================================================================
// The UDT `{key,value}` spelling is CSV-ONLY (issue #1491 finding F3)
// =======================================================================

/// CSV delivers a UDT as one flat `{k: v, …}` field carrying nothing that could
/// distinguish it from a map, so `csv_container` decodes every brace-delimited body
/// into the `{key,value}` spelling — which is therefore a legal CLI spelling in the
/// CSV lane and in no other. Accepting it in the JSON lane would let a UDT that
/// regressed to the map representation pass.
#[test]
fn the_udt_pair_spelling_is_accepted_in_csv_and_refused_in_json() {
    let ty = column_ty("frozen<key_part>");
    let pairs = json!([{"key": "label", "value": "x"}, {"key": "rank", "value": 1}]);
    let csv = canon(&pairs, Egress::Csv, &ty, Kinding::Natural).expect("legal in the CSV lane");
    let object = canon(
        &json!({"label": "x", "rank": 1}),
        Egress::Csv,
        &ty,
        Kinding::Natural,
    )
    .expect("the golden's object spelling");
    assert_eq!(csv, object, "the two CSV-lane spellings denote one value");

    let why = refusal(canon(&pairs, Egress::Json, &ty, Kinding::Natural));
    assert!(
        why.contains("CSV lane"),
        "the JSON refusal must say where the pair spelling IS legal: {why}"
    );
}

/// A repeated field name in the CSV pair spelling is malformed output, not
/// something to reconcile: keeping the last occurrence hid the earlier one (issue
/// #1491 finding J2).
#[test]
fn a_repeated_field_in_the_csv_pair_spelling_is_refused() {
    let ty = column_ty("frozen<key_part>");
    let why = refusal(canon(
        &json!([
            {"key": "label", "value": "x"},
            {"key": "label", "value": "y"},
            {"key": "rank", "value": 1}
        ]),
        Egress::Csv,
        &ty,
        Kinding::Natural,
    ));
    assert!(why.contains("repeats the field"), "{why}");
}

// =======================================================================
// `describe()` must be INJECTIVE (issue #1491 finding DD1, one level down)
// =======================================================================

/// `compare::compare_map` builds a map entry's PATH from `Canon::describe()` and a
/// declared gap is matched against that path by exact string, so two DISTINCT keys
/// that describe alike would share one path and one gap would cover both.
#[test]
fn two_keys_differing_deep_inside_describe_differently() {
    let key_ty = key_ty_of(MAP_TUPLE_LIST_UDT);
    let one = json!([[{"label": "la", "rank": 1}, {"label": "lb", "rank": 2}], 1]);
    let two = json!([[{"label": "la", "rank": 1}, {"label": "lb", "rank": 3}], 1]);
    let a = canon_cli(&one, &key_ty).expect("canonicalizes");
    let b = canon_cli(&two, &key_ty).expect("canonicalizes");
    assert_ne!(a, b);
    assert_ne!(
        a.describe(),
        b.describe(),
        "a difference at depth 3 must survive into the path"
    );
}

/// The collision the escaping exists for, and it needs no exotic value: unescaped,
/// `seq[text:a, text:b]` is BOTH a two-member sequence and a one-member sequence
/// whose single member is the text `a, text:b`.
#[test]
fn a_separator_inside_a_member_cannot_forge_another_canon_rendering() {
    let two = Canon::Seq(vec![
        Canon::Text("a".to_string()),
        Canon::Text("b".to_string()),
    ]);
    let one = Canon::Seq(vec![Canon::Text("a, text:b".to_string())]);
    assert_ne!(two, one, "the two values are distinct");
    assert_ne!(
        two.describe(),
        one.describe(),
        "and so must their renderings be: {} vs {}",
        two.describe(),
        one.describe()
    );
}

/// The same for a map's `=>` and a UDT's `=`.
#[test]
fn an_entry_separator_inside_a_key_cannot_forge_another_rendering() {
    let entry = Canon::Entries(vec![(
        Canon::Text("a".to_string()),
        Canon::Text("b".to_string()),
    )]);
    let forged = Canon::Entries(vec![(
        Canon::Text("a => text:b".to_string()),
        Canon::Text("b".to_string()),
    )]);
    assert_ne!(entry.describe(), forged.describe());

    let fields = Canon::Fields(vec![("label".to_string(), Canon::Text("x".to_string()))]);
    let forged_fields = Canon::Fields(vec![("label=text:x".to_string(), Canon::Null)]);
    assert_ne!(fields.describe(), forged_fields.describe());
}

// =======================================================================
// `golden_map_key_value` and `is_container_type`
// =======================================================================

#[test]
fn a_scalar_map_key_denotes_its_own_text() {
    let ty = CqlType::Text("text".to_string());
    assert_eq!(
        golden_map_key_value("[1, 2]", &ty),
        Ok(Value::String("[1, 2]".to_string())),
        "a `text` key is its text, brackets and all — nothing is parsed"
    );
}

#[test]
fn a_container_map_key_denotes_its_parsed_tojsonstring_document() {
    let ty = column_ty("frozen<list<int>>");
    assert_eq!(golden_map_key_value("[1, 2]", &ty), Ok(json!([1, 2])));
}

/// A duplicate object key inside a key document would silently discard part of the
/// ORACLE, so the key is parsed by the same strict parse the golden LINE gets
/// (issue #1491 finding K2).
#[test]
fn a_duplicate_key_inside_a_golden_map_key_document_is_refused() {
    let ty = column_ty("frozen<key_part>");
    let why = match golden_map_key_value("{\"label\": \"x\", \"label\": \"y\", \"rank\": 1}", &ty) {
        Ok(value) => panic!("expected a refusal, got {value}"),
        Err(why) => why,
    };
    assert!(why.contains("does not parse as JSON"), "{why}");
}

#[test]
fn every_cql_type_is_classified_as_container_or_not() {
    for ty in [
        column_ty("frozen<list<int>>"),
        column_ty("frozen<key_part>"),
        column_ty("frozen<tuple<text, int>>"),
        column_ty("frozen<map<text, int>>"),
        CqlType::Set(Box::new(CqlType::Blob)),
    ] {
        assert!(is_container_type(&ty), "`{}`", ty.describe());
    }
    for ty in [
        CqlType::Numeric("int".to_string()),
        CqlType::Text("text".to_string()),
        CqlType::Boolean,
        CqlType::Blob,
        CqlType::Timestamp,
        CqlType::Opaque("uuid".to_string()),
    ] {
        assert!(!is_container_type(&ty), "`{}`", ty.describe());
    }
}

// =======================================================================
// THE DIFFERENTIAL: two recursive implementations must not drift apart
// =======================================================================

/// WHY THIS TEST EXISTS. This issue creates a SECOND recursive comparison beside
/// `compare::compare_value_at`: the comparator walks the two sides together, and
/// [`canon_typed`] reduces ONE side to a canonical value. Both now descend through
/// list/set/tuple/map/UDT, and both refuse a shape the DDL does not describe — so
/// they can DRIFT, and a drift is invisible in ordinary use because each is exercised
/// through a different entry point (a whole column vs a map key).
///
/// The property asserted is therefore the equivalence itself, over a table of pairs
/// that includes equality, inequality at each depth, and each refusal shape:
///
/// ```text
///   the comparator accepts (g, c)  ==  canon(g) and canon(c) both succeed and are equal
/// ```
///
/// SCOPE, stated because it is not the whole comparator:
///
///   * `Egress::Json`. The CSV lane's `node_refusal` layer sits ABOVE the canonical
///     model — it decides that a golden position is not RECOVERABLE from the flat
///     rendering and returns `Ok` for a node nobody could decide — so
///     `compare_value_at` legitimately accepts pairs no canonical equality can see.
///     That layer has no canonical counterpart by design (`csv_container`'s module
///     doc states it), so including CSV would assert a property neither side claims;
///   * no declared GAP is in scope: a gap suppresses a divergence, which is exactly
///     a licensed disagreement between the two;
///   * `Kinding::Natural` on both sides, which is what a frozen container column
///     gets (`compare::column_kinding`), and what a container may have at all.
#[test]
fn the_canonical_model_and_the_comparator_agree() {
    // (name, declared type, golden, cli)
    let cases: &[(&str, &str, Value, Value)] = &[
        (
            "container-keyed map, equal",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            cli_map_tuple_udt(),
        ),
        (
            "container-keyed map, key differs at depth 3",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            json!([
                {"key": [{"label": "mkey-a", "rank": 99}, 1], "value": 210},
                {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
            ]),
        ),
        (
            "container-keyed map, VALUE differs",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            json!([
                {"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 211},
                {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
            ]),
        ),
        (
            "container-keyed map, entries reordered",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            json!([
                {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220},
                {"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210}
            ]),
        ),
        (
            "container-keyed map, entry dropped",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            json!([{"key": [{"label": "mkey-a", "rank": 21}, 1], "value": 210}]),
        ),
        (
            "container-keyed map, malformed pair",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            json!([{"key": [{"label": "mkey-a", "rank": 21}, 1]}, {"value": 220}]),
        ),
        (
            "container-keyed map, wrong tuple arity in the key",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            json!([
                {"key": [{"label": "mkey-a", "rank": 21}, 1, 7], "value": 210},
                {"key": [{"label": "mkey-b", "rank": 22}, 2], "value": 220}
            ]),
        ),
        (
            "container-keyed map, the cli spelled the key the GOLDEN's way",
            MAP_TUPLE_UDT,
            golden_map_tuple_udt(),
            json!([
                {"key": "[{\"label\": \"mkey-a\", \"rank\": 21}, 1]", "value": 210},
                {"key": "[{\"label\": \"mkey-b\", \"rank\": 22}, 2]", "value": 220}
            ]),
        ),
        (
            "set-keyed map, equal (fixture row id=3)",
            MAP_SET_UDT,
            json!({"[{\"label\": \"solo\", \"rank\": 99}]": 7}),
            json!([{"key": [{"label": "solo", "rank": 99}], "value": 7}]),
        ),
        (
            "set-keyed map, null UDT fields (fixture row id=2)",
            MAP_SET_UDT,
            json!({"[{\"label\": null, \"rank\": null}]": 61}),
            json!([{"key": [{"label": null, "rank": null}], "value": 61}]),
        ),
        (
            "set-keyed map, a null field the cli filled in",
            MAP_SET_UDT,
            json!({"[{\"label\": null, \"rank\": null}]": 61}),
            json!([{"key": [{"label": "", "rank": null}], "value": 61}]),
        ),
        (
            "nested list in the key, equal (fixture row id=1)",
            MAP_TUPLE_LIST_UDT,
            json!({"[[{\"label\": \"la\", \"rank\": 1}], 1]": 120}),
            json!([{"key": [[{"label": "la", "rank": 1}], 1], "value": 120}]),
        ),
        (
            "nested list in the key, a member dropped",
            MAP_TUPLE_LIST_UDT,
            json!({"[[{\"label\": \"la\", \"rank\": 1}, {\"label\": \"lb\", \"rank\": 2}], 1]": 120}),
            json!([{"key": [[{"label": "la", "rank": 1}], 1], "value": 120}]),
        ),
        (
            "scalar-keyed map, equal",
            "frozen<map<text, int>>",
            json!({"a": 1, "b": 2}),
            json!([{"key": "a", "value": 1}, {"key": "b", "value": 2}]),
        ),
        (
            "scalar-keyed map, key kind regression",
            "frozen<map<text, int>>",
            json!({"0": 1}),
            json!([{"key": 0, "value": 1}]),
        ),
        (
            "tuple, equal",
            "frozen<tuple<text, int>>",
            json!(["a", 1]),
            json!(["a", 1]),
        ),
        (
            "tuple, arity beyond the declared one",
            "frozen<tuple<text, int>>",
            json!(["a", 1]),
            json!(["a", 1, "extra"]),
        ),
        (
            "udt, equal",
            "frozen<key_part>",
            json!({"label": "x", "rank": 1}),
            json!({"label": "x", "rank": 1}),
        ),
        (
            "udt, field misordered on the cli side",
            "frozen<key_part>",
            json!({"label": "x", "rank": 1}),
            json!({"rank": 1, "label": "x"}),
        ),
        (
            "udt, declared field absent from the cli side",
            "frozen<key_part>",
            json!({"label": "x", "rank": 1}),
            json!({"label": "x"}),
        ),
        (
            "udt, undeclared field on the cli side",
            "frozen<key_part>",
            json!({"label": "x", "rank": 1}),
            json!({"label": "x", "rank": 1, "spurious": 2}),
        ),
        (
            "list, equal",
            "frozen<list<int>>",
            json!([1, 2]),
            json!([1, 2]),
        ),
        (
            "list, member differs",
            "frozen<list<int>>",
            json!([1, 2]),
            json!([1, 3]),
        ),
        (
            "list, shorter",
            "frozen<list<int>>",
            json!([1, 2]),
            json!([1]),
        ),
        ("both null", "frozen<list<int>>", Value::Null, Value::Null),
        (
            "golden null, cli empty",
            "frozen<list<int>>",
            Value::Null,
            json!([]),
        ),
        (
            "nested udt inside a list, equal",
            "frozen<list<frozen<key_part>>>",
            json!([{"label": "x", "rank": 1}]),
            json!([{"label": "x", "rank": 1}]),
        ),
        (
            "nested udt inside a list, differs at depth 2",
            "frozen<list<frozen<key_part>>>",
            json!([{"label": "x", "rank": 1}]),
            json!([{"label": "x", "rank": 2}]),
        ),
    ];
    let mut disagreements: Vec<String> = Vec::new();
    for (name, cql_type, golden, cli) in cases {
        let schema = column_schema(cql_type);
        let ty = column_ty(cql_type);
        let comparator_accepts = {
            let g: Row = [
                ("id".to_string(), json!(1)),
                ("c".to_string(), golden.clone()),
            ]
            .into_iter()
            .collect();
            let c: Row = [("id".to_string(), json!(1)), ("c".to_string(), cli.clone())]
                .into_iter()
                .collect();
            let report = compare_rows(&[g], &[c], &schema, &["id"], &[], &[], Egress::Json);
            report.diffs.is_empty() && report.stale_skips.is_empty()
        };
        let canon_agrees = match (canon_golden(golden, &ty), canon_cli(cli, &ty)) {
            (Ok(g), Ok(c)) => g == c,
            _ => false,
        };
        if comparator_accepts != canon_agrees {
            disagreements.push(format!(
                "{name}: the comparator {} while the canonical model {}",
                if comparator_accepts {
                    "ACCEPTS"
                } else {
                    "REJECTS"
                },
                if canon_agrees { "AGREES" } else { "DISAGREES" }
            ));
        }
    }
    // A vacuous pass is the one outcome this test must not have: a shrunken table
    // reports no drift because it asks about nothing (the CASE-FLOOR idiom).
    assert!(cases.len() >= 27, "the differential table has shrunk");
    assert!(
        disagreements.is_empty(),
        "the two recursive implementations have drifted:\n  {}",
        disagreements.join("\n  ")
    );
}

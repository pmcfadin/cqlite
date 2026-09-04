//! WRITE-SIDE admission of the empty-buffer sentinel (issue #3805, roborev job
//! 449 finding D).
//!
//! # The defect
//! `serialize_value_into` accepted `Value::Empty(_)` unconditionally and wrote
//! zero bytes with no declared type in sight — so a caller could write a
//! sentinel whose tag disagreed with the column, or write one in a context where
//! zero bytes mean something else entirely. Concretely, as a regular CELL VALUE
//! zero bytes plus `HAS_EMPTY_VALUE_MASK` (`db/rows/Cell.java:264` at
//! `cassandra-5.0.8`) reads back as `Value::Null`, so the value SILENTLY CHANGED
//! TYPE across the round trip; and inside a length-prefixed collection element a
//! zero-length element is the empty value of the ELEMENT's declared type, which
//! for `text`/`blob` is a legal meaningful empty and for
//! `tinyint`/`smallint`/`date`/`time` is corruption Cassandra's own `validate`
//! throws on.
//!
//! # The rule these tests pin
//! Zero-byte sentinel serialization is legal on EXACTLY ONE write-path position —
//! a MULTICELL map's CELL PATH — because that is the only one where the length is
//! carried by the enclosing framing (an unsigned VInt,
//! `db/marshal/CollectionType.java:361-382`) AND the declared KEY type is
//! available to validate the tag against. Everywhere else it is REFUSED. Where
//! the declared type is unavailable the answer is also REFUSE, never a guess
//! (no-heuristics, issue #28): refusing beats writing bytes that read back as
//! something else.
//!
//! # ONE LEGAL WRITER, not two (roborev job 452)
//! An earlier revision of this file called `TypeSerializer::serialize_value` a
//! second legal writer, because it validates the tag against the declared type
//! via [`crate::types::EmptyValueType::check_admits`]. That was wrong: a
//! declared type is only the TYPE half of the admission, and
//! `TypeSerializer::serialize_value` is the general CELL-VALUE API, whose
//! positions supply no framing in which zero bytes mean an empty key — so it
//! refuses. `serialize_map_cell_path_key_into` is the only value-serializing
//! function in the crate that admits the sentinel. The last test here pins BOTH
//! halves of that: the type-aware writer refuses every family, and the map cell
//! path admits exactly the families the shared `check_admits` admits.

use super::super::*;
use super::support::*;
use crate::schema::Column;
use crate::types::{EmptyValueType, Value};

fn column(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

// ───────────────────────────────────────────────────────────────────────────
// THE ONE LEGAL POSITION — a multicell map's cell path, tag validated
// ───────────────────────────────────────────────────────────────────────────

/// The map cell path ACCEPTS a sentinel whose tag matches the declared KEY type,
/// and writes exactly zero bytes — for every admitted family that CQL permits as
/// a map key.
///
/// `counter` is excluded because CQL forbids a `counter` collection element
/// (`cql3/CQL3Type.java:827-828`, `:835-836`), so `map<counter,…>` is not a type
/// a schema can declare; the sentinel table admits it from the serializer source
/// alone (`serializers/CounterSerializer.java:20-23`).
#[test]
fn the_map_cell_path_accepts_a_matching_tag_and_writes_zero_bytes() {
    let cases: &[(EmptyValueType, &str)] = &[
        (EmptyValueType::Int, "int"),
        (EmptyValueType::BigInt, "bigint"),
        (EmptyValueType::Float, "float"),
        (EmptyValueType::Double, "double"),
        (EmptyValueType::Timestamp, "timestamp"),
        (EmptyValueType::Uuid, "uuid"),
        (EmptyValueType::TimeUuid, "timeuuid"),
        (EmptyValueType::Boolean, "boolean"),
        (EmptyValueType::Inet, "inet"),
        (EmptyValueType::Decimal, "decimal"),
        (EmptyValueType::Varint, "varint"),
    ];
    for (tag, key_type) in cases {
        let declared = format!("map<{key_type}, int>");
        let mut out = vec![0xAAu8; 3]; // pre-existing bytes must be untouched
        serialize_map_cell_path_key_into(&Value::Empty(*tag), &declared, &mut out)
            .unwrap_or_else(|e| panic!("{declared}: a matching sentinel must be accepted: {e}"));
        assert_eq!(
            out,
            vec![0xAAu8; 3],
            "{declared}: the sentinel's whole encoding is NOTHING — the length lives in \
             the caller's VInt"
        );
    }
}

/// A NON-sentinel key on that same path is untouched by the new gate: it goes
/// straight to the type-blind serializer and emits its ordinary bytes. So the
/// change costs nothing on any path that does not carry a sentinel — including
/// one whose declared type this gate could not have parsed.
#[test]
fn the_map_cell_path_leaves_every_non_sentinel_key_alone() {
    let mut out = Vec::new();
    serialize_map_cell_path_key_into(&Value::Integer(42), "map<int, int>", &mut out)
        .expect("an ordinary key still serializes");
    assert_eq!(out, 42i32.to_be_bytes().to_vec());

    // A declared type the gate cannot parse as `map<K,V>` is IRRELEVANT unless a
    // sentinel is actually being written.
    let mut out = Vec::new();
    serialize_map_cell_path_key_into(
        &Value::Integer(7),
        "org.apache.cassandra.db.marshal.MapType(Int32Type,Int32Type)",
        &mut out,
    )
    .expect("an ordinary key does not need the declared key type");
    assert_eq!(out, 7i32.to_be_bytes().to_vec());
}

// ───────────────────────────────────────────────────────────────────────────
// REFUSALS on that same path
// ───────────────────────────────────────────────────────────────────────────

/// A tag that DISAGREES with the declared key type is refused, naming both.
/// Writing it would put bytes on disk that read back as another type.
#[test]
fn the_map_cell_path_refuses_a_tag_that_disagrees_with_the_declared_key_type() {
    let mut out = Vec::new();
    let err = serialize_map_cell_path_key_into(
        &Value::Empty(EmptyValueType::Int),
        "map<bigint, int>",
        &mut out,
    )
    .expect_err("an Empty(int) key in a map<bigint,…> must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("`int`") && msg.contains("`bigint`") && msg.contains("#3805"),
        "the refusal must name BOTH types and the issue: {msg}"
    );
    assert!(out.is_empty(), "a refusal writes nothing: {out:?}");
}

/// A declared key type that does not ADMIT an empty buffer is refused — the four
/// strict families, for which an empty buffer is corruption on Cassandra's own
/// terms, and the text/blob families, for which it is a legal MEANINGFUL value
/// that must never be spelled as a sentinel.
#[test]
fn the_map_cell_path_refuses_a_key_type_that_does_not_admit_an_empty_buffer() {
    for key_type in [
        "tinyint", "smallint", "date", "time", "text", "ascii", "varchar", "blob",
    ] {
        let declared = format!("map<{key_type}, int>");
        let mut out = Vec::new();
        // The tag is deliberately a VALID one: what is refused is the DECLARED
        // type, not the tag, so this cannot pass for the wrong reason.
        let err = serialize_map_cell_path_key_into(
            &Value::Empty(EmptyValueType::Int),
            &declared,
            &mut out,
        )
        .expect_err("a non-admitting key type must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("does not admit an empty buffer") && msg.contains(&declared),
            "{declared}: the refusal must name the declared type and say why: {msg}"
        );
        assert!(out.is_empty(), "a refusal writes nothing: {out:?}");
    }
}

/// A declared type the gate cannot resolve to a CQL `map<K,V>` is a REFUSAL, not
/// a guess (#28). The Cassandra MARSHAL spelling is the realistic instance —
/// `CqlType::parse` does not model it and yields `Custom` — and a
/// non-map declaration is the degenerate one.
#[test]
fn the_map_cell_path_refuses_when_the_declared_key_type_is_unavailable() {
    for declared in [
        "org.apache.cassandra.db.marshal.MapType(Int32Type,Int32Type)",
        "int",
        "list<int>",
        "",
    ] {
        let mut out = Vec::new();
        let err = serialize_map_cell_path_key_into(
            &Value::Empty(EmptyValueType::Int),
            declared,
            &mut out,
        )
        .expect_err("an unresolvable declared key type must be refused, never guessed");
        let msg = err.to_string();
        assert!(
            msg.contains("#3805") && msg.contains("#28"),
            "{declared:?}: the refusal must cite the sentinel issue and the \
             no-heuristics mandate: {msg}"
        );
        assert!(out.is_empty(), "a refusal writes nothing: {out:?}");
    }
}

// ───────────────────────────────────────────────────────────────────────────
// EVERY GENERIC CONTEXT REFUSES — the type-blind serializer has no declared type
// ───────────────────────────────────────────────────────────────────────────

/// `serialize_value_into` is type-blind, so it refuses a sentinel outright: as a
/// bare value, and in every nested position it reaches (list/set element, map key
/// and map VALUE of a FROZEN collection, tuple field, frozen member).
///
/// The frozen MAP KEY case is worth its own mention: it looks like the legal
/// position and is not. A FROZEN map is ONE inline length-prefixed cell whose
/// keys are `i32`-length-prefixed elements inside it — there is no CellPath at
/// all — so its empty key is the inline-element case that
/// `require_fixed_width` owns (#3847/#4071), not this one.
#[test]
fn every_generic_context_refuses_the_sentinel() {
    let sentinel = Value::Empty(EmptyValueType::Int);
    let cases: Vec<(&str, Value)> = vec![
        ("bare value", sentinel.clone()),
        ("list element", Value::List(vec![sentinel.clone()])),
        ("set element", Value::Set(vec![sentinel.clone()])),
        (
            "frozen map key",
            Value::Map(vec![(sentinel.clone(), Value::Integer(1))]),
        ),
        (
            "frozen map value",
            Value::Map(vec![(Value::Integer(1), sentinel.clone())]),
        ),
        ("tuple field", Value::Tuple(vec![sentinel.clone()])),
        ("frozen member", Value::Frozen(Box::new(sentinel.clone()))),
    ];
    for (what, value) in cases {
        let mut out = Vec::new();
        let err = serialize_value_into(&value, &mut out)
            .expect_err("a type-blind context must refuse the sentinel");
        assert!(
            err.to_string().contains("#3805"),
            "{what}: the refusal must cite the issue: {}",
            err
        );
    }
    // The owned-`Vec` wrappers are the same rule (they delegate), and the
    // collection-element wrapper's own null guard must not shadow it.
    assert!(serialize_value(&sentinel).is_err(), "serialize_value");
    assert!(
        serialize_collection_element(&sentinel, "Collection").is_err(),
        "serialize_collection_element"
    );
}

/// A regular CELL VALUE refuses too — this is the case whose bytes would have
/// been indistinguishable from a NULL on the way back
/// (`db/rows/Cell.java:264`: `hasValue = !flag(HAS_EMPTY_VALUE_MASK)`).
#[test]
fn a_regular_cell_value_refuses_the_sentinel() {
    let mut buf = Vec::new();
    let err = write_cell_value_into(&mut buf, "c", &Value::Empty(EmptyValueType::Int))
        .expect_err("a sentinel is not a cell VALUE");
    assert!(err.to_string().contains("#3805"), "{err}");
}

// ───────────────────────────────────────────────────────────────────────────
// THE MAP CELL PATH THROUGH THE REAL WRITER, both directions
// ───────────────────────────────────────────────────────────────────────────

/// WIRING EVIDENCE: the schema-aware gate is reached through the REAL complex
/// column writer, not merely callable in isolation — a matching sentinel key is
/// written as a ZERO-LENGTH cell path, and a mismatched one fails the whole
/// column write.
///
/// The emitted cells are decoded with the suite's own
/// [`decode_complex_column`], so the assertion is on the CELL PATH of each
/// entry rather than on a byte window of the whole column (which would couple
/// this test to the deletion-time deltas and the cell headers, none of which is
/// its subject).
#[test]
fn the_real_complex_map_writer_reaches_the_schema_aware_gate() {
    let writer = DataWriter::new(create_test_stats());
    let col = column("m", "map<int, int>");
    let value = Value::Map(vec![
        (Value::Empty(EmptyValueType::Int), Value::Integer(7)),
        (Value::Integer(42), Value::Integer(1)),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &col, &value, 1_000, None, TEST_NOW_SECONDS)
        .expect("a map<int,int> with an Empty(int) key must write");
    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    let paths: Vec<Vec<u8>> = cells.iter().map(|c| c.cell_path.clone()).collect();
    assert_eq!(
        paths,
        vec![Vec::<u8>::new(), 42i32.to_be_bytes().to_vec()],
        "the empty key must be a ZERO-LENGTH cell path, sorted FIRST (an empty key \
         sorts strictly before every other, `db/marshal/Int32Type.java:61-71`), with \
         the golden sibling's 4-byte path after it"
    );
    assert_eq!(
        cells[0].value.as_deref(),
        Some(&7i32.to_be_bytes()[..]),
        "the empty key's ENTRY value is untouched by the cell-path gate"
    );

    // MISMATCH: the same value under a map<bigint,…> declaration fails the write
    // rather than emitting a path that reads back as another type.
    let bad = column("m", "map<bigint, int>");
    let mut buf = Vec::new();
    let err = writer
        .write_complex_column(&mut buf, &bad, &value, 1_000, None, TEST_NOW_SECONDS)
        .expect_err("an Empty(int) key under map<bigint,…> must fail the column write");
    assert!(
        err.to_string().contains("#3805"),
        "the refusal must reach the caller verbatim: {err}"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// EXACTLY ONE LEGAL WRITER — pinned in BOTH directions (roborev job 452)
// ───────────────────────────────────────────────────────────────────────────

/// The families a `map<K, int>` key type may legally carry the sentinel for,
/// derived from the shared admission rule rather than restated: a type is
/// admitted iff [`EmptyValueType::for_cql_type`] names a tag for it.
///
/// `counter` is absent because CQL forbids a `counter` collection element
/// (`cql3/CQL3Type.java:827-828`), so `map<counter,…>` is not declarable.
const CANDIDATE_KEY_TYPES: &[&str] = &[
    "int", "bigint", "float", "double", "timestamp", "uuid", "timeuuid", "boolean", "inet",
    "decimal", "varint", "tinyint", "smallint", "date", "time", "text", "ascii", "varchar", "blob",
    "duration",
];

/// DIRECTION 1 — the map cell path admits EXACTLY the families the shared
/// [`crate::types::EmptyValueType::check_admits`] admits, derived from
/// [`EmptyValueType::for_cql_type`] rather than from a second hand-written list
/// (a restated expectation is a second opinion able to drift from the tag
/// table).
#[test]
fn the_map_cell_path_admits_exactly_the_tag_tables_families() {
    let tag = EmptyValueType::Int;
    for key_type in CANDIDATE_KEY_TYPES {
        let declared = crate::schema::CqlType::parse(key_type)
            .unwrap_or_else(|e| panic!("{key_type} must parse as a CqlType: {e}"));
        let expected = EmptyValueType::for_cql_type(&declared) == Some(tag);

        let mut out = Vec::new();
        let admitted = serialize_map_cell_path_key_into(
            &Value::Empty(tag),
            &format!("map<{key_type}, int>"),
            &mut out,
        )
        .is_ok();

        assert_eq!(
            admitted, expected,
            "map<{key_type}, int>: the cell path's admission of Empty(int) disagrees \
             with the tag table it is supposed to be derived from"
        );
        if admitted {
            assert!(
                out.is_empty(),
                "map<{key_type}, int>: an admitted sentinel must write ZERO bytes, wrote {}",
                out.len()
            );
        }
    }
}

/// DIRECTION 2 — the type-aware writer refuses EVERY family, admitted or not.
///
/// This is the property roborev job 452 found missing. `TypeSerializer` knows the
/// declared type, so it can answer the TYPE half of the admission — and a
/// declared type is not sufficient, because a cell value supplies no framing in
/// which zero bytes mean an empty key (`db/rows/Cell.java:264`: they read back as
/// `null`). A refusal that held for `tinyint` and not for `int` would not be a
/// refusal, so every candidate key type is asserted, and the diagnostic must
/// name the issue AND the one legal route so a caller is not left guessing.
#[test]
fn the_type_aware_writer_refuses_every_family_as_a_cell_value() {
    let serializer = crate::storage::serialization::types::TypeSerializer::new();
    let tag = EmptyValueType::Int;
    for key_type in CANDIDATE_KEY_TYPES {
        let err = serializer
            .serialize_value(&Value::Empty(tag), key_type)
            .expect_err(&format!(
                "TypeSerializer::serialize_value must refuse Empty(int) for `{key_type}`: it is \
                 the general CELL-VALUE API and zero bytes there read back as null"
            ));
        let msg = err.to_string();
        for needle in ["#3805", "serialize_map_cell_path_key_into"] {
            assert!(
                msg.contains(needle),
                "the refusal for `{key_type}` must name {needle}; got: {msg}"
            );
        }
    }
}

/// DIRECTION 2, NESTED — the same refusal reaches a COLLECTION ELEMENT, a TUPLE
/// FIELD and a UDT FIELD, which are the type-aware writer's other positions.
///
/// Before job 452's fix these already failed, but with a per-type "Cannot
/// serialize Empty(int) as Int" mismatch that named neither the reason nor the
/// legal route; the assertion is on the NAMED refusal, so a future refactor that
/// re-admits the sentinel nested cannot pass by producing some other error.
#[test]
fn the_type_aware_writer_refuses_the_sentinel_nested_too() {
    let serializer = crate::storage::serialization::types::TypeSerializer::new();
    let sentinel = Value::Empty(EmptyValueType::Int);
    for (value, declared) in [
        (Value::List(vec![sentinel.clone()]), "list<int>"),
        (Value::Set(vec![sentinel.clone()]), "set<int>"),
        (
            Value::Map(vec![(sentinel.clone(), Value::Integer(1))]),
            "map<int, int>",
        ),
        (
            Value::Map(vec![(Value::Integer(1), sentinel.clone())]),
            "map<int, int>",
        ),
        (
            Value::Tuple(vec![Value::Integer(1), sentinel.clone()]),
            "tuple<int, int>",
        ),
    ] {
        let err = serializer
            .serialize_value(&value, declared)
            .expect_err(&format!("{declared} carrying a nested sentinel must be refused"));
        assert!(
            err.to_string().contains("#3805"),
            "the nested refusal for {declared} must name #3805; got: {err}"
        );
    }
}

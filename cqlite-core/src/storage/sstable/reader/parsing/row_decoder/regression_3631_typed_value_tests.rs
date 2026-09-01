//! Issue #3631 — the two blob-fallback arms, at the DECODER's own level.
//!
//! ## What this file covers and why it exists AT THIS LEVEL
//!
//! The fixture-backed parity oracle for #3631 is
//! `cqlite-core/tests/issue_3631_structured_values_not_blobs.rs`, against the
//! committed Cassandra-5.0.2-written `test-data/fixtures/issue_3504/` corpus. That
//! corpus declares exactly ONE collection-typed UDT field — `unhashable_fields.m`,
//! a `frozen<map<text,int>>` — and no `list`- or `set`-typed one anywhere. Its
//! schema is committed source and regenerating it needs Docker, which the gate does
//! not have.
//!
//! Acceptance criterion 4 asks for the `list` and `set` halves anyway, so that the
//! fix is demonstrably at the fallback ARM and not at the one type the fixture
//! happens to use. They are therefore covered here, with expected bytes DERIVED FROM
//! CASSANDRA SOURCE rather than captured from CQLite's own output (#3042):
//!
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/CollectionSerializer.java`
//!   — `pack` writes `writeCollectionSize` = `output.putInt(elements)` (a 4-byte BE
//!   i32 count, NOT a vint) and then `writeValue` per element = `putInt(size)` +
//!   bytes, with `putInt(-1)` for null. `list`, `set` and `map` share that packing;
//!   `MapSerializer` writes each entry as KEY then VALUE with the same per-item
//!   framing.
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java` (a
//!   `TupleType`) — per-field `[i32 size][bytes]`, `-1` for a null field.
//!
//! `MAP_A_1_GOLDEN_BYTES` below are the EXACT 20 bytes the Cassandra-written fixture
//! carries for `unhashable_fields.m`, quoted so this hand-built layer stays anchored
//! to the real corpus instead of drifting into a self-consistent fiction.

use super::*;
use crate::types::{UdtFieldDef, UdtTypeDef};

/// The bytes Cassandra wrote for the fixture's `frozen<map<text,int>>` field
/// `unhashable_fields.m` = `{"a": 1}`, whose sstabledump golden rendering is
/// `{"a": 1}` (`test-data/fixtures/issue_3504/.../nb-1-big-Data.db.jsonl`, row 3
/// `stn`).
const MAP_A_1_GOLDEN_BYTES: &[u8] = &[
    0, 0, 0, 1, // i32 element count = 1     (CollectionSerializer.writeCollectionSize)
    0, 0, 0, 1, b'a', // i32 key size = 1, "a"      (CollectionSerializer.writeValue)
    0, 0, 0, 4, 0, 0, 0, 1, // i32 value size = 4, int 1
];

/// `CollectionSerializer.pack`: `[i32 count]` then `[i32 size][bytes]` per element.
fn pack(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut out = (elements.len() as i32).to_be_bytes().to_vec();
    for e in elements {
        out.extend_from_slice(&(e.len() as i32).to_be_bytes());
        out.extend_from_slice(e);
    }
    out
}

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new(
        "test_udt_collision".to_string(),
        "t".to_string(),
        0,
        0,
        None,
    )
}

fn parser_with_udt(name: &str, fields: &[(&str, CqlType)]) -> V5CompressedLegacyParser {
    let mut registry = UdtRegistry::new();
    registry.register_udt(UdtTypeDef {
        keyspace: "test_udt_collision".to_string(),
        name: name.to_string(),
        fields: fields
            .iter()
            .map(|(n, t)| UdtFieldDef {
                name: n.to_string(),
                field_type: t.clone(),
                nullable: true,
            })
            .collect(),
    });
    parser().with_udt_registry(registry)
}

/// Peel CQLite's `Value::Frozen` marker — `frozen<X>` serializes exactly as `X`, so
/// the wrapper is a type-system artefact and never a value distinction.
fn unfrozen(v: &Value) -> &Value {
    match v {
        Value::Frozen(inner) => unfrozen(inner),
        other => other,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// INSTANCE B — `parse_simple_udt_field_value`'s fallback arm.
//
// Criterion 4: `map` (anchored on the fixture's own bytes), `list` and `set`, so the
// property proven is about the ARM and not about `map`.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn frozen_map_text_int_udt_field_decodes_from_the_fixtures_own_cassandra_bytes() {
    let field_type = CqlType::Frozen(Box::new(CqlType::Map(
        Box::new(CqlType::Text),
        Box::new(CqlType::Int),
    )));
    let decoded = parser()
        .parse_simple_udt_field_value(MAP_A_1_GOLDEN_BYTES, &field_type)
        .expect("the fixture's own 20 Cassandra-written bytes must decode");
    assert_eq!(
        unfrozen(&decoded),
        &Value::Map(vec![(Value::text("a"), Value::Integer(1))]),
        "golden (sstabledump, udt_hashable_shapes row 3 `stn`): {{\"a\": 1}}"
    );
}

#[test]
fn frozen_list_int_udt_field_decodes_to_its_elements() {
    let field_type = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int))));
    let bytes = pack(&[7i32.to_be_bytes().to_vec(), 8i32.to_be_bytes().to_vec()]);
    let decoded = parser()
        .parse_simple_udt_field_value(&bytes, &field_type)
        .expect("a frozen<list<int>> UDT field must decode");
    assert_eq!(
        unfrozen(&decoded),
        &Value::List(vec![Value::Integer(7), Value::Integer(8)]),
        "issue #3631 criterion 4: the fix is at the fallback ARM, so `list` must \
         decode as well as `map`"
    );
}

#[test]
fn frozen_set_text_udt_field_decodes_to_its_members() {
    let field_type = CqlType::Frozen(Box::new(CqlType::Set(Box::new(CqlType::Text))));
    let bytes = pack(&[b"alpha".to_vec(), b"beta".to_vec()]);
    let decoded = parser()
        .parse_simple_udt_field_value(&bytes, &field_type)
        .expect("a frozen<set<text>> UDT field must decode");
    assert_eq!(
        unfrozen(&decoded),
        &Value::Set(vec![Value::text("alpha"), Value::text("beta")]),
        "issue #3631 criterion 4: `set` too"
    );
}

/// A non-frozen (bare) collection field type reaches the same arm — the CQL parser
/// drops the mandatory `frozen<>` on a UDT collection field, which is exactly the
/// shape the fixture's `unhashable_fields.m` arrives as.
#[test]
fn bare_map_udt_field_type_decodes_too() {
    let field_type = CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int));
    let decoded = parser()
        .parse_simple_udt_field_value(MAP_A_1_GOLDEN_BYTES, &field_type)
        .expect("a bare map<text,int> UDT field must decode");
    assert_eq!(
        decoded,
        Value::Map(vec![(Value::text("a"), Value::Integer(1))])
    );
}

/// A `tuple`-typed UDT field: `TupleType.buildValue` framing, same `[i32 size]`
/// per component. Also on the far side of the old fallback arm.
#[test]
fn frozen_tuple_udt_field_decodes_to_its_components() {
    let field_type = CqlType::Frozen(Box::new(CqlType::Tuple(vec![CqlType::Text, CqlType::Int])));
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5i32.to_be_bytes());
    bytes.extend_from_slice(b"alpha");
    bytes.extend_from_slice(&4i32.to_be_bytes());
    bytes.extend_from_slice(&30i32.to_be_bytes());
    let decoded = parser()
        .parse_simple_udt_field_value(&bytes, &field_type)
        .expect("a frozen<tuple<text,int>> UDT field must decode");
    assert_eq!(
        unfrozen(&decoded),
        &Value::Tuple(vec![Value::text("alpha"), Value::Integer(30)])
    );
}

/// `TupleType.split` stops at the end of the buffer: a tuple written with fewer
/// components than its type declares leaves the trailing ones absent, i.e. null.
#[test]
fn frozen_tuple_udt_field_with_missing_trailing_components_is_null_padded() {
    let field_type = CqlType::Frozen(Box::new(CqlType::Tuple(vec![
        CqlType::Text,
        CqlType::Int,
        CqlType::Int,
    ])));
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5i32.to_be_bytes());
    bytes.extend_from_slice(b"alpha");
    let decoded = parser()
        .parse_simple_udt_field_value(&bytes, &field_type)
        .expect("a short tuple is legal per TupleType.split");
    assert_eq!(
        unfrozen(&decoded),
        &Value::Tuple(vec![Value::text("alpha"), Value::Null, Value::Null])
    );
}

/// A `-1` component length is `TupleType`'s / `CollectionSerializer.readValue`'s
/// NULL, not a length.
#[test]
fn frozen_tuple_udt_field_negative_component_length_is_null() {
    let field_type = CqlType::Frozen(Box::new(CqlType::Tuple(vec![CqlType::Text, CqlType::Int])));
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(-1i32).to_be_bytes());
    bytes.extend_from_slice(&4i32.to_be_bytes());
    bytes.extend_from_slice(&30i32.to_be_bytes());
    let decoded = parser()
        .parse_simple_udt_field_value(&bytes, &field_type)
        .expect("a null tuple component is legal");
    assert_eq!(
        unfrozen(&decoded),
        &Value::Tuple(vec![Value::Null, Value::Integer(30)])
    );
}

/// A UDT-typed field inside a collection resolves through the authoritative
/// `UdtRegistry` — the `list<frozen<udt>>` shape, which the old arm also lost.
#[test]
fn frozen_list_of_udt_udt_field_resolves_elements_through_the_registry() {
    let parser = parser_with_udt("plain", &[("label", CqlType::Text)]);
    let element = {
        let mut e = Vec::new();
        e.extend_from_slice(&5i32.to_be_bytes());
        e.extend_from_slice(b"inner");
        e
    };
    let field_type = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
        Box::new(CqlType::Custom("plain".to_string())),
    )))));
    let decoded = parser
        .parse_simple_udt_field_value(&pack(&[element]), &field_type)
        .expect("a frozen<list<frozen<plain>>> UDT field must decode via the registry");
    match unfrozen(&decoded) {
        Value::List(items) => {
            assert_eq!(items.len(), 1);
            match unfrozen(&items[0]) {
                Value::Udt(udt) => {
                    assert_eq!(udt.type_name, "plain");
                    assert_eq!(udt.keyspace, "test_udt_collision");
                    assert_eq!(udt.fields[0].value, Some(Value::text("inner")));
                }
                other => panic!("element must decode to a Udt, got {other:?}"),
            }
        }
        other => panic!("expected a List, got {other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// CRITERION 5 — no silent `Value::Blob` from EITHER arm.
// ════════════════════════════════════════════════════════════════════════════

/// A `blob`-declared field still decodes to `Value::Blob`: that is the CORRECT
/// answer for that declared type, and the no-silent-blob rule must not break it.
#[test]
fn a_declared_blob_field_still_decodes_to_a_blob() {
    let decoded = parser()
        .parse_simple_udt_field_value(&[0xDE, 0xAD], &CqlType::Blob)
        .expect("blob is a decodable declared type");
    assert_eq!(decoded, Value::blob(vec![0xDE, 0xAD]));
}

/// The types the old arm silently blobbed and that ARE expressible now decode.
/// `smallint`/`tinyint`/`varint`/`inet`/`time`/`date`/`counter` had no arm at all.
#[test]
fn scalars_the_old_arm_blobbed_now_decode_from_their_declared_type() {
    let p = parser();
    assert_eq!(
        p.parse_simple_udt_field_value(&7i16.to_be_bytes(), &CqlType::SmallInt)
            .expect("smallint"),
        Value::SmallInt(7)
    );
    assert_eq!(
        p.parse_simple_udt_field_value(&[0xFF], &CqlType::TinyInt)
            .expect("tinyint"),
        Value::TinyInt(-1)
    );
    assert_eq!(
        p.parse_simple_udt_field_value(&1_234i64.to_be_bytes(), &CqlType::Time)
            .expect("time"),
        Value::Time(1_234)
    );
    assert_eq!(
        p.parse_simple_udt_field_value(&9i64.to_be_bytes(), &CqlType::Counter)
            .expect("counter"),
        Value::BigInt(9)
    );
}

/// A UDT-typed field naming a type the registry does not hold and that carries no
/// inline field list cannot be decoded — and per #3631 criterion 5 that is an
/// explicit, caller-visible `Error` NAMING the type, never a silent `Value::Blob`
/// behind a `tracing::debug!`.
#[test]
fn an_unresolvable_nested_udt_field_is_an_explicit_error_naming_the_type() {
    let err = parser()
        .parse_simple_udt_field_value(&[0u8; 4], &CqlType::Custom("no_such_udt".to_string()))
        .expect_err("an unresolvable UDT field type must NOT degrade to a blob");
    let text = err.to_string();
    assert!(
        text.contains("no_such_udt"),
        "the error must NAME the undecodable type; got: {text}"
    );
}

// ════════════════════════════════════════════════════════════════════════════

/// The primitive fast arms are untouched by #3631 and must keep decoding.
/// A `blob`-typed map key is the one case where `Value::Blob` IS the right answer,
/// so the no-silent-blob guard must not turn it into an error. Both spellings of the
/// declared type — the CQL short form and Cassandra's `BytesType` marshal name.
/// A key type with no decoding rule is an explicit `Error` naming it — never the old
/// silent `Value::Blob` (#3631 criterion 5).
/// A frozen-UDT cell-path key resolves through the registry, and the `frozen<>`
/// wrapper is NOT surfaced as a `Value::Frozen`: a map key is implicitly frozen, and
/// Cassandra writes the marker in only one of the two map spellings
/// (`MapType(FrozenType(UserType(…)), …)` for the multicell column versus
/// `FrozenType(MapType(UserType(…), …))` for the frozen one — measured on the
/// fixture's own `Statistics.db`), while the key bytes are identical. Surfacing it
/// would make the two spellings disagree, which criterion 2 forbids.
/// primitive allowlist at all, so it reaches the delegating fallback and must
/// normalize through `primitive_marshal_to_cql_short` — which it cannot do
/// lowercased, where it would surface as a spurious `unsupported_format` error.
/// A marshal-form COLLECTION key, whose ELEMENT type is itself a marshal form: the
/// element case must survive the outer extraction, or the element decodes as a blob.
///
/// The element is `Int32Type` ON PURPOSE, and this is the whole discriminating power
/// of the case. `parse_value_from_raw_bytes` accepts a handful of TEXT marshal names
/// as literal LOWERCASE aliases (`...utf8type`, `...asciitype`, `...varchartype`), so
/// a `SetType(UTF8Type)` element decodes correctly even from a lowercased string and
/// this test would pass over the defect. `Int32Type` has no lowercase alias — it can
/// only reach an arm through the CASE-SENSITIVE `primitive_marshal_to_cql_short` — so
/// it reds. Verified by re-introducing the lowercasing: with `UTF8Type` this case
/// passed, with `Int32Type` it fails.
/// A marshal-form MAP key (`map<frozen<map<text,int>>, v>` is legal CQL). `MapType`
/// has no primitive-allowlist entry, so it reaches the delegating fallback, and its
/// VALUE component is `Int32Type` — no lowercase alias — so the case must survive.
/// DISCRIMINATING.
/// NON-DISCRIMINATING, and labelled so nobody reads it as casing evidence:
/// allowlist (matched lowercased on purpose) so they never reach the delegating
/// fallback at all. Kept as coverage that the marshal spelling of the two commonest
/// key types is in that allowlist — a real property, just not this one.
/// The canonical on-disk spelling of THIS ISSUE's subject: a non-frozen
/// `map<frozen<udt>, int>` key, as Cassandra writes it —
/// `MapType(FrozenType(UserType(...)), Int32Type)`, whose KEY component is
/// `FrozenType(UserType(...))`. Both the UDT itself and its nested FIELD types must
/// decode; lowercased, the fields come back as blobs.
/// A registry whose UDT `cyclic` has a field of type `frozen<list<frozen<cyclic>>>`,
/// i.e. it references ITSELF through a collection. This is the shape that used to
/// recurse without bound: each `list` layer went `collection -> UDT`, and the UDT hop
/// reset the counter, so no finite limit was ever reached.
fn cyclic_parser() -> V5CompressedLegacyParser {
    parser_with_udt(
        "cyclic",
        &[(
            "inner",
            CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
                Box::new(CqlType::Custom("cyclic".to_string())),
            ))))),
        )],
    )
}

/// `n` layers of `list<frozen<cyclic>>` nesting, innermost first: each layer is one
/// UDT body holding a one-element list holding the next layer's UDT body.
fn nested_cyclic_bytes(layers: usize) -> Vec<u8> {
    // Innermost: a `cyclic` whose single field is absent (i32 -1 = null).
    let mut udt = (-1i32).to_be_bytes().to_vec();
    for _ in 0..layers {
        let list = pack(&[udt]);
        // One UDT body: `[i32 size][list bytes]` for its single field.
        let mut next = (list.len() as i32).to_be_bytes().to_vec();
        next.extend_from_slice(&list);
        udt = next;
    }
    udt
}

/// TERMINATION, and it is a REFUSAL rather than a stack overflow. A depth well past
/// `MAX_TYPE_NESTING_DEPTH` must return an `Err` naming the depth — not recurse, not
/// abort the process.
#[test]
fn a_cyclic_udt_through_a_collection_is_refused_not_recursed() {
    let p = cyclic_parser();
    let deep = nested_cyclic_bytes(MAX_TYPE_NESTING_DEPTH * 4);
    let err = p
        .parse_simple_udt_field_value(
            &deep,
            &CqlType::Frozen(Box::new(CqlType::Custom("cyclic".to_string()))),
        )
        .expect_err(
            "a cyclic UDT reached through a collection must be REFUSED; before              #3631's BLOCKER 2 fix each UDT hop reset the depth counter and this              recursed until the stack was exhausted",
        );
    let text = err.to_string();
    assert!(
        text.contains("depth"),
        "the refusal must name the nesting depth; got: {text}"
    );
}

/// The limit is REACHED BY ALTERNATION, not only by one type of layer. A depth just
/// past the limit is refused while a shallow value of the SAME shape still decodes —
/// so the test distinguishes "the limit works" from "this shape never decodes".
#[test]
fn alternating_collection_and_udt_layers_share_one_nesting_limit() {
    let p = cyclic_parser();
    let ty = CqlType::Frozen(Box::new(CqlType::Custom("cyclic".to_string())));

    // POSITIVE CONTROL: two layers is well inside the limit and must decode.
    let shallow = nested_cyclic_bytes(2);
    let decoded = p
        .parse_simple_udt_field_value(&shallow, &ty)
        .expect("a SHALLOW cyclic-typed value must still decode — the limit must                  bound depth, not reject the shape");
    assert!(
        matches!(unfrozen(&decoded), Value::Udt(_)),
        "got {decoded:?}"
    );

    // Just past the limit: refused. Each alternating pair consumes levels, so
    // MAX_TYPE_NESTING_DEPTH layers of `list`+`UDT` is over budget.
    let deep = nested_cyclic_bytes(MAX_TYPE_NESTING_DEPTH);
    assert!(
        p.parse_simple_udt_field_value(&deep, &ty).is_err(),
        "{} alternating collection/UDT layers must exceed the shared limit of {}",
        MAX_TYPE_NESTING_DEPTH,
        MAX_TYPE_NESTING_DEPTH
    );
}

// ════════════════════════════════════════════════════════════════════════════
// BLOCKER 3 — no silently discarded bytes.
//
// The caller frames each slice as exactly one value, so leftover bytes mean the frame
// and the declared type disagree. Accepting them is the framing-error-MASKING class
// that let #3002's Rows.db root-base defect hide behind a compensating encoder
// defect: two errors that cancel are undetectable unless something insists the
// accounting balances.
// ════════════════════════════════════════════════════════════════════════════

/// THE SHARPEST CASE: a zero-count collection with a payload behind it. Nothing in
/// the loop runs, so before this fix it decoded as a cheerful EMPTY map and the
/// payload vanished.
#[test]
fn a_zero_count_collection_with_trailing_payload_is_refused_not_emptied() {
    let p = parser();
    let mut bytes = 0i32.to_be_bytes().to_vec();
    bytes.extend_from_slice(b"payload-that-must-not-vanish");
    let err = p
        .parse_simple_udt_field_value(
            &bytes,
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
        )
        .expect_err("a zero-count map with trailing bytes must be REFUSED, not emptied");
    let text = err.to_string();
    assert!(
        text.contains("trailing") && text.contains("28"),
        "the refusal must name the unaccounted byte count; got: {text}"
    );
}

/// Trailing bytes after a WELL-FORMED collection are refused too — the check is on
/// the accounting, not on the count being zero.
#[test]
fn trailing_bytes_after_a_well_formed_collection_are_refused() {
    let p = parser();
    let mut bytes = pack(&[7i32.to_be_bytes().to_vec()]);
    bytes.extend_from_slice(&[0xAA, 0xBB]);
    assert!(
        p.parse_simple_udt_field_value(&bytes, &CqlType::List(Box::new(CqlType::Int)))
            .is_err(),
        "2 bytes past the last declared element must be refused"
    );
    // POSITIVE CONTROL: the same value without the tail decodes, so the case
    // distinguishes "the check works" from "this shape never decodes".
    assert_eq!(
        p.parse_simple_udt_field_value(
            &pack(&[7i32.to_be_bytes().to_vec()]),
            &CqlType::List(Box::new(CqlType::Int))
        )
        .expect("the exactly-framed list must decode"),
        Value::List(vec![Value::Integer(7)])
    );
}

/// A tuple may be SHORT (`TupleType.split` stops at the end of the buffer, so
/// trailing components are null) but may not be LONG.
#[test]
fn trailing_bytes_after_the_last_tuple_component_are_refused() {
    let p = parser();
    let ty = CqlType::Tuple(vec![CqlType::Int]);
    let mut bytes = 4i32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&9i32.to_be_bytes());
    let exact = p
        .parse_simple_udt_field_value(&bytes, &ty)
        .expect("an exactly-framed 1-tuple must decode");
    assert_eq!(exact, Value::Tuple(vec![Value::Integer(9)]));

    bytes.extend_from_slice(&[0xFF]);
    assert!(
        p.parse_simple_udt_field_value(&bytes, &ty).is_err(),
        "a byte past the last declared component is trailing garbage"
    );
}

/// An OVERSIZED fixed-width scalar is refused. The type-string decoder bounds-checks
/// with `<`, so it would read the first 4 bytes of a 9-byte frame as an `int` and drop
/// the other 5.
#[test]
fn an_oversized_fixed_width_scalar_is_refused() {
    let p = parser();
    let ty = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int))));
    // Reached through a collection ELEMENT, which is where the delegation lives.
    let mut oversized = 9i32.to_be_bytes().to_vec();
    oversized.extend_from_slice(&[0, 0, 0, 0, 0]);
    let err = p
        .parse_simple_udt_field_value(&pack(&[oversized]), &ty)
        .expect_err("a 9-byte `int` element must be refused, not truncated to 4");
    assert!(
        err.to_string().contains("4 bytes wide"),
        "the refusal must name the declared width; got: {err}"
    );
}

/// The width rule accepts Cassandra's EMPTY buffer for a fixed-width type
/// (`Int32Type.validate` permits `remaining() == 0`), so the check may not be
/// "length == width" — that would reject legal data.
#[test]
fn an_empty_buffer_for_a_fixed_width_scalar_is_not_rejected_by_the_width_rule() {
    let p = parser();
    // An empty `map` field is the reachable empty-buffer case and must stay empty
    // rather than becoming a trailing-bytes refusal.
    assert_eq!(
        p.parse_simple_udt_field_value(
            &[],
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int))
        )
        .expect("an empty frozen map field must decode as the empty map"),
        Value::Map(vec![])
    );
}

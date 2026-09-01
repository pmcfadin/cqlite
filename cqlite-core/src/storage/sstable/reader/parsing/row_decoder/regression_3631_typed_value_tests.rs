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
//! `MAP_A_1_GOLDEN_BYTES` below are the EXACT 17 bytes the Cassandra-written fixture
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
        .parse_simple_udt_field_value_at(MAP_A_1_GOLDEN_BYTES, &field_type, 0)
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
        .parse_simple_udt_field_value_at(&bytes, &field_type, 0)
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
        .parse_simple_udt_field_value_at(&bytes, &field_type, 0)
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
        .parse_simple_udt_field_value_at(MAP_A_1_GOLDEN_BYTES, &field_type, 0)
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
        .parse_simple_udt_field_value_at(&bytes, &field_type, 0)
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
        .parse_simple_udt_field_value_at(&bytes, &field_type, 0)
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
        .parse_simple_udt_field_value_at(&bytes, &field_type, 0)
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
        .parse_simple_udt_field_value_at(&pack(&[element]), &field_type, 0)
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
        .parse_simple_udt_field_value_at(&[0xDE, 0xAD], &CqlType::Blob, 0)
        .expect("blob is a decodable declared type");
    assert_eq!(decoded, Value::blob(vec![0xDE, 0xAD]));
}

/// The types the old arm silently blobbed and that ARE expressible now decode.
/// `smallint`/`tinyint`/`varint`/`inet`/`time`/`date`/`counter` had no arm at all.
#[test]
fn scalars_the_old_arm_blobbed_now_decode_from_their_declared_type() {
    let p = parser();
    assert_eq!(
        p.parse_simple_udt_field_value_at(&7i16.to_be_bytes(), &CqlType::SmallInt, 0)
            .expect("smallint"),
        Value::SmallInt(7)
    );
    assert_eq!(
        p.parse_simple_udt_field_value_at(&[0xFF], &CqlType::TinyInt, 0)
            .expect("tinyint"),
        Value::TinyInt(-1)
    );
    assert_eq!(
        p.parse_simple_udt_field_value_at(&1_234i64.to_be_bytes(), &CqlType::Time, 0)
            .expect("time"),
        Value::Time(1_234)
    );
    assert_eq!(
        p.parse_simple_udt_field_value_at(&9i64.to_be_bytes(), &CqlType::Counter, 0)
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
        .parse_simple_udt_field_value_at(&[0u8; 4], &CqlType::Custom("no_such_udt".to_string()), 0)
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
        .parse_simple_udt_field_value_at(
            &deep,
            &CqlType::Frozen(Box::new(CqlType::Custom("cyclic".to_string()))), 0)
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
        .parse_simple_udt_field_value_at(&shallow, &ty, 0)
        .expect("a SHALLOW cyclic-typed value must still decode — the limit must                  bound depth, not reject the shape");
    assert!(
        matches!(unfrozen(&decoded), Value::Udt(_)),
        "got {decoded:?}"
    );

    // Just past the limit: refused. Each alternating pair consumes levels, so
    // MAX_TYPE_NESTING_DEPTH layers of `list`+`UDT` is over budget.
    let deep = nested_cyclic_bytes(MAX_TYPE_NESTING_DEPTH);
    assert!(
        p.parse_simple_udt_field_value_at(&deep, &ty, 0).is_err(),
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
        .parse_simple_udt_field_value_at(
            &bytes,
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
            0,
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
        p.parse_simple_udt_field_value_at(&bytes, &CqlType::List(Box::new(CqlType::Int)), 0)
            .is_err(),
        "2 bytes past the last declared element must be refused"
    );
    // POSITIVE CONTROL: the same value without the tail decodes, so the case
    // distinguishes "the check works" from "this shape never decodes".
    assert_eq!(
        p.parse_simple_udt_field_value_at(
            &pack(&[7i32.to_be_bytes().to_vec()]),
            &CqlType::List(Box::new(CqlType::Int)),
            0
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
        .parse_simple_udt_field_value_at(&bytes, &ty, 0)
        .expect("an exactly-framed 1-tuple must decode");
    assert_eq!(exact, Value::Tuple(vec![Value::Integer(9)]));

    bytes.extend_from_slice(&[0xFF]);
    assert!(
        p.parse_simple_udt_field_value_at(&bytes, &ty, 0).is_err(),
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
        .parse_simple_udt_field_value_at(&pack(&[oversized]), &ty, 0)
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
        p.parse_simple_udt_field_value_at(
            &[],
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
            0
        )
        .expect("an empty frozen map field must decode as the empty map"),
        Value::Map(vec![])
    );
}

// ════════════════════════════════════════════════════════════════════════════
// THE UDT BOUNDARY'S BYTE ACCOUNTING (roborev BLOCKER 6 on this issue).
//
// The UDT arms used to bypass the exhaustion rule: both delegated decoders returned a
// bare `Value` and dropped their cursor, so bytes after the last declared field — and
// an INCOMPLETE 1-3 byte field-length prefix, which the field loop silently treats as
// "trailing fields omitted" — vanished. Consumption is now part of the decode
// signature (`parse_typed_value_reporting`) and the assert is written ONCE, so these
// cases are refused for both UDT decoders at once.
//
// AUTHORITY, read first-hand at the pinned tag rather than from CQLite's behaviour:
// `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java` `split`,
// which `UserType extends TupleType` inherits and calls (`UserType.java:263`):
//
//     for (int i = 0; i < numberOfElements; i++) {
//         if (position == length) return Arrays.copyOfRange(components, 0, i);
//         if (position + 4 > length)
//             throw new MarshalException("Not enough bytes to read %dth component");
//         ...
//     }
//     if (position < length) throw new MarshalException("... but got more");
//
// So: omitted trailing components are legal ONLY at exact end-of-buffer; a partial
// length prefix is corruption; trailing bytes are corruption.
// ════════════════════════════════════════════════════════════════════════════

/// `UserType`'s per-field framing: `[i32 size][bytes]`, which is `TupleType`'s.
fn udt_field(bytes: &[u8]) -> Vec<u8> {
    let mut out = (bytes.len() as i32).to_be_bytes().to_vec();
    out.extend_from_slice(bytes);
    out
}

/// A one-field `inner(a int)` UDT carrying `a = 1`, exactly 8 bytes.
fn inner_a_1() -> Vec<u8> {
    udt_field(&1i32.to_be_bytes())
}

#[test]
fn a_registry_udt_field_consuming_every_byte_decodes() {
    let p = parser_with_udt("inner", &[("a", CqlType::Int)]);
    let value = p
        .parse_simple_udt_field_value_at(&inner_a_1(), &CqlType::Custom("inner".to_string()), 0)
        .expect("the exact serialization must decode");
    match unfrozen(&value) {
        Value::Udt(udt) => {
            assert_eq!(udt.type_name, "inner");
            assert_eq!(udt.fields[0].value, Some(Value::Integer(1)));
        }
        other => panic!("expected Udt, got {:?}", other),
    }
}

#[test]
fn trailing_bytes_after_the_last_registry_udt_field_are_refused() {
    let p = parser_with_udt("inner", &[("a", CqlType::Int)]);
    let mut bytes = inner_a_1();
    // A whole extra `[i32]` behind the last DECLARED field: `TupleType.split`'s
    // post-loop `if (position < length) throw ... "but got more"`.
    bytes.extend_from_slice(&0i32.to_be_bytes());
    let err = p
        .parse_simple_udt_field_value_at(&bytes, &CqlType::Custom("inner".to_string()), 0)
        .expect_err("trailing bytes after the last declared field are corruption");
    let msg = err.to_string();
    assert!(
        msg.contains("trailing byte"),
        "the error must name the unaccounted bytes, got: {msg}"
    );
}

#[test]
fn a_partial_trailing_registry_udt_field_length_prefix_is_refused() {
    // TWO declared fields, so the loop reaches the second and finds fewer than four
    // bytes left: Cassandra's `if (position + 4 > length) throw`. CQLite's field loop
    // treats that as "trailing fields omitted" and stops WITHOUT advancing, which is
    // precisely the state the reported offset makes visible.
    let p = parser_with_udt("inner2", &[("a", CqlType::Int), ("b", CqlType::Text)]);
    let mut bytes = inner_a_1();
    bytes.extend_from_slice(&[0x00, 0x00]); // 2 of the 4 prefix bytes
    let err = p
        .parse_simple_udt_field_value_at(&bytes, &CqlType::Custom("inner2".to_string()), 0)
        .expect_err("an incomplete field-length prefix is corruption, not an omitted field");
    assert!(err.to_string().contains("trailing byte"), "got: {}", err);
}

#[test]
fn omitted_trailing_registry_udt_fields_are_accepted_at_exact_end_of_buffer() {
    // The POSITIVE control for the two refusals above: the same two-field UDT, the
    // same one serialized field, and NO stray bytes — `if (position == length) return
    // Arrays.copyOfRange(components, 0, i)`. Without this case the assert could be
    // refusing every short encoding, which Cassandra accepts.
    let p = parser_with_udt("inner2", &[("a", CqlType::Int), ("b", CqlType::Text)]);
    let value = p
        .parse_simple_udt_field_value_at(&inner_a_1(), &CqlType::Custom("inner2".to_string()), 0)
        .expect("a UDT whose trailing fields are omitted at exact EOF is legal");
    match unfrozen(&value) {
        Value::Udt(udt) => {
            assert_eq!(udt.fields[0].value, Some(Value::Integer(1)));
            assert_eq!(udt.fields[1].value, None, "omitted trailing field is null");
        }
        other => panic!("expected Udt, got {:?}", other),
    }
}

#[test]
fn trailing_bytes_after_the_last_inline_udt_field_are_refused() {
    // The INLINE decoder (issue #239's fallback: a UDT with no registry entry but
    // inline field definitions) is a SECOND implementation of the same field loop, so
    // it needs its own case — a per-site checklist is what BLOCKER 6 rejected, and
    // this is the site the contract change had to reach as well.
    let p = parser();
    let ty = CqlType::Udt("inline1".to_string(), vec![("a".to_string(), CqlType::Int)]);
    let mut bytes = inner_a_1();
    bytes.extend_from_slice(&0i32.to_be_bytes());
    let err = p
        .parse_simple_udt_field_value_at(&bytes, &ty, 0)
        .expect_err("trailing bytes after the last inline field are corruption");
    assert!(err.to_string().contains("trailing byte"), "got: {}", err);
}

#[test]
fn a_partial_trailing_inline_udt_field_length_prefix_is_refused() {
    let p = parser();
    let ty = CqlType::Udt(
        "inline2".to_string(),
        vec![
            ("a".to_string(), CqlType::Int),
            ("b".to_string(), CqlType::Text),
        ],
    );
    let mut bytes = inner_a_1();
    bytes.extend_from_slice(&[0x00, 0x00, 0x00]); // 3 of the 4 prefix bytes
    let err = p
        .parse_simple_udt_field_value_at(&bytes, &ty, 0)
        .expect_err("an incomplete inline field-length prefix is corruption");
    assert!(err.to_string().contains("trailing byte"), "got: {}", err);
}

#[test]
fn omitted_trailing_inline_udt_fields_are_accepted_at_exact_end_of_buffer() {
    let p = parser();
    let ty = CqlType::Udt(
        "inline2".to_string(),
        vec![
            ("a".to_string(), CqlType::Int),
            ("b".to_string(), CqlType::Text),
        ],
    );
    let value = p
        .parse_simple_udt_field_value_at(&inner_a_1(), &ty, 0)
        .expect("an inline UDT whose trailing fields are omitted at exact EOF is legal");
    match unfrozen(&value) {
        Value::Udt(udt) => assert_eq!(udt.fields[1].value, None),
        other => panic!("expected Udt, got {:?}", other),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// MALFORMED FIELD LENGTHS ON BOTH UDT PATHS (roborev BLOCKER 4).
//
// A field length other than `-1` (null) or `0` (empty) used to be cast straight to
// `usize`, so `-2` became ~1.8e19 and the following bounds ADD overflowed — a panic
// on untrusted file bytes. #3612 / PR #3736 closed that upstream by routing all five
// field loops through `complex_column/component_len.rs::checked_component_len`, which
// rejects any negative before converting and uses `checked_add`. These two cases pin
// that the guard is REACHED FROM THIS ISSUE'S NEW ENTRY POINT — the `CqlType`-driven
// decoder — on both UDT implementations, because a guard upstream of a path nobody
// takes protects nothing.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn a_registry_udt_field_length_below_minus_one_is_refused_without_panicking() {
    let p = parser_with_udt("inner", &[("a", CqlType::Int)]);
    let err = p
        .parse_simple_udt_field_value_at(
            &(-2i32).to_be_bytes(),
            &CqlType::Custom("inner".to_string()),
            0,
        )
        .expect_err("-2 is not a legal component length");
    let msg = err.to_string();
    assert!(
        msg.contains("negative length") && msg.contains("-2"),
        "the error must name the illegal length, got: {msg}"
    );
}

#[test]
fn an_inline_udt_field_length_below_minus_one_is_refused_without_panicking() {
    let p = parser();
    let ty = CqlType::Udt("inline1".to_string(), vec![("a".to_string(), CqlType::Int)]);
    let err = p
        .parse_simple_udt_field_value_at(&(-2i32).to_be_bytes(), &ty, 0)
        .expect_err("-2 is not a legal component length");
    assert!(err.to_string().contains("negative length"), "got: {}", err);
}

#[test]
fn a_udt_field_length_of_i32_min_is_refused_without_overflowing() {
    // The extreme of the same class: `i32::MIN as usize` is 0xFFFF...80000000, and the
    // pre-#3736 `current_offset + field_len` would overflow in debug and wrap in
    // release. Kept as its own case because `-2` and `i32::MIN` exercise the same
    // guard but different arithmetic.
    let p = parser_with_udt("inner", &[("a", CqlType::Int)]);
    let err = p
        .parse_simple_udt_field_value_at(
            &i32::MIN.to_be_bytes(),
            &CqlType::Custom("inner".to_string()),
            0,
        )
        .expect_err("i32::MIN is not a legal component length");
    assert!(err.to_string().contains("negative length"), "got: {}", err);
}

// ════════════════════════════════════════════════════════════════════════════
// THE THIRD SITE — `parse_udt_field_value`, the field decoder a TOP-LEVEL frozen-UDT
// column takes (`parse_udt_value` -> here).
//
// The issue names two arms; this file's subject is one of them
// (`parse_simple_udt_field_value`). A THIRD arm in the same file had the identical
// shape — a closed set of primitives and `_ => Value::Blob` — and it is the one a
// DIRECT `frozen<unhashable_fields>` column would take. The committed corpus has no
// such column (which is why acceptance criterion 3's parenthetical "the direct
// `unhashable_fields` column" has no fixture to point at), so nothing else in the
// suite would have noticed it staying broken while the nested spelling was fixed.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn a_collection_field_of_a_top_level_frozen_udt_column_also_decodes_structurally() {
    let column = crate::schema::Column {
        name: "u".to_string(),
        data_type: "frozen<unhashable_fields>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };
    let udt_def = UdtTypeDef {
        keyspace: "test_udt_collision".to_string(),
        name: "unhashable_fields".to_string(),
        fields: vec![
            UdtFieldDef {
                name: "label".to_string(),
                field_type: CqlType::Text,
                nullable: true,
            },
            UdtFieldDef {
                name: "m".to_string(),
                field_type: CqlType::Frozen(Box::new(CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Int),
                ))),
                nullable: true,
            },
        ],
    };
    // `UserType`'s framing for `{label: "x", m: {"a": 1}}`: `[i32 size][bytes]` per
    // field, with `m`'s body being the fixture's own 17 golden bytes.
    let mut data = udt_field(b"x");
    data.extend_from_slice(&udt_field(MAP_A_1_GOLDEN_BYTES));

    let (value, consumed) = parser()
        .parse_udt_value(&data, 0, &udt_def, &column, 0)
        .expect("the top-level frozen-UDT column path must decode");
    assert_eq!(consumed, data.len(), "every byte must be accounted for");

    match unfrozen(&value) {
        Value::Udt(udt) => {
            assert_eq!(udt.fields[0].value, Some(Value::text("x")));
            let m = udt.fields[1]
                .value
                .as_ref()
                .expect("`m` must not decode NULL");
            assert_eq!(
                unfrozen(m),
                &Value::Map(vec![(Value::text("a"), Value::Integer(1))]),
                "the THIRD blob-fallback arm must decode from the declared type too, \
                 not hand back the field's 17 serialized bytes (issue #3631)"
            );
        }
        other => panic!("expected Udt, got {:?}", other),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// `duration` — the ONE scalar whose decode can stop SHORT.
//
// The scalar arm reports `data.len()` as consumed for every other type, and that is
// verified arm by arm; `duration` is the exception. Its decode reads three signed
// VInts (months, days, nanos — cassandra-5.0.8 DurationSerializer) and ignores what
// follows, so its consumption is MEASURED from the same framing. Without that, a
// duration-typed UDT field would accept trailing bytes silently — the exact
// silent-discard shape the exhaustion contract exists to refuse, surviving in one arm.
// ════════════════════════════════════════════════════════════════════════════

/// The three-VInt body Cassandra writes for `1mo2d3ns`. Each value is ZIGZAG-encoded
/// and then VInt-encoded; all three fit in one byte here, so the whole duration is
/// three bytes.
const DURATION_1MO_2D_3NS: &[u8] = &[0x02, 0x04, 0x06];

#[test]
fn a_duration_udt_field_decodes_and_consumes_exactly_its_three_vints() {
    let p = parser();
    let value = p
        .parse_simple_udt_field_value_at(DURATION_1MO_2D_3NS, &CqlType::Duration, 0)
        .expect("a well-formed duration field must decode");
    match unfrozen(&value) {
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            assert_eq!((*months, *days, *nanos), (1, 2, 3));
        }
        other => panic!("expected Duration, got {:?}", other),
    }
}

#[test]
fn trailing_bytes_after_a_duration_udt_field_are_refused() {
    let p = parser();
    let mut bytes = DURATION_1MO_2D_3NS.to_vec();
    bytes.push(0x7F); // a fourth VInt nobody declared
    let err = p
        .parse_simple_udt_field_value_at(&bytes, &CqlType::Duration, 0)
        .expect_err("bytes after the third duration VInt are unaccounted for");
    assert!(
        err.to_string().contains("trailing byte"),
        "the scalar arm must MEASURE duration's consumption, not assume it: {}",
        err
    );
}

/// The limit must count FRAMING layers, not call hops — the false-refusal direction,
/// which is the one a bound-tightening change breaks silently.
///
/// Unifying the five per-field dispatches (roborev round 3) routed every field through
/// one entry, and with `frozen` and the field entry each charging a level, a canonical
/// spelling from this repo's own corpus —
/// `frozen<set<frozen<tuple<frozen<udt>, int>>>>`, the fixture's `stn` — cost five
/// levels per LOGICAL layer and came within one of being refused. This case pins the
/// accounting from the other side: a legitimately nested value decodes, and only real
/// framing layers (collection elements, UDT boundaries) consume budget.
#[test]
fn a_legitimately_nested_frozen_spelling_is_not_refused_by_the_depth_limit() {
    // `set<frozen<tuple<frozen<inner>, int>>>` where `inner` itself declares a
    // `frozen<map<text,int>>` field: four `frozen` markers, three framing layers.
    let p = parser_with_udt(
        "inner",
        &[(
            "m",
            CqlType::Frozen(Box::new(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Int),
            ))),
        )],
    );
    let ty = CqlType::Frozen(Box::new(CqlType::Set(Box::new(CqlType::Frozen(Box::new(
        CqlType::Tuple(vec![
            CqlType::Frozen(Box::new(CqlType::Custom("inner".to_string()))),
            CqlType::Int,
        ]),
    ))))));

    let udt_body = udt_field(MAP_A_1_GOLDEN_BYTES);
    let mut tuple_body = udt_field(&udt_body);
    tuple_body.extend_from_slice(&udt_field(&30i32.to_be_bytes()));
    let bytes = pack(&[tuple_body]);

    let value = p
        .parse_simple_udt_field_value_at(&bytes, &ty, 0)
        .expect("a canonical nested frozen spelling must NOT hit the nesting limit");
    let members = match unfrozen(&value) {
        Value::Set(m) | Value::List(m) => m.clone(),
        other => panic!("expected a set, got {other:?}"),
    };
    let components = match unfrozen(&members[0]) {
        Value::Tuple(c) => c.clone(),
        other => panic!("expected a tuple, got {other:?}"),
    };
    match unfrozen(&components[0]) {
        Value::Udt(udt) => assert_eq!(
            unfrozen(udt.fields[0].value.as_ref().expect("`m` must decode")),
            &Value::Map(vec![(Value::text("a"), Value::Integer(1))])
        ),
        other => panic!("expected the inner UDT, got {other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// THE COLUMN-LEVEL ENTRY POINT (roborev round 4, findings A and B).
//
// `parse_udt_value` is the decoder a TOP-LEVEL frozen-UDT column takes. It reported a
// consumed offset that every production caller DISCARDED (`let (v, _) = …`), and its
// `Frozen`/`Udt` arms recursed through it again at depth ZERO — so on this path the
// byte accounting and the shared nesting limit were both unchecked, while the nested
// path had both. The enumeration that produced the five-dispatch collapse covered the
// PER-FIELD entries and missed these; the closure was larger than that table.
// ════════════════════════════════════════════════════════════════════════════

fn column(name: &str, data_type: &str) -> crate::schema::Column {
    crate::schema::Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// A one-field `inner(a int)` def for the column-level decoder.
fn inner_def() -> UdtTypeDef {
    UdtTypeDef {
        keyspace: "test_udt_collision".to_string(),
        name: "inner".to_string(),
        fields: vec![UdtFieldDef {
            name: "a".to_string(),
            field_type: CqlType::Int,
            nullable: true,
        }],
    }
}

#[test]
fn a_top_level_frozen_udt_column_consuming_every_byte_decodes() {
    let (value, consumed) = parser()
        .parse_udt_value(
            &inner_a_1(),
            0,
            &inner_def(),
            &column("u", "frozen<inner>"),
            0,
        )
        .expect("the exact serialization must decode");
    assert_eq!(consumed, inner_a_1().len());
    assert!(matches!(unfrozen(&value), Value::Udt(_)), "got {value:?}");
}

#[test]
fn trailing_bytes_in_a_top_level_frozen_udt_column_are_refused() {
    let mut bytes = inner_a_1();
    bytes.extend_from_slice(&0i32.to_be_bytes());
    let err = parser()
        .parse_udt_value(&bytes, 0, &inner_def(), &column("u", "frozen<inner>"), 0)
        .expect_err("trailing bytes after the last declared field are corruption");
    assert!(err.to_string().contains("trailing byte"), "got: {}", err);
}

#[test]
fn a_partial_trailing_field_length_prefix_in_a_top_level_udt_column_is_refused() {
    // Two declared fields so the loop reaches the second and finds fewer than four
    // bytes left — Cassandra's `if (position + 4 > length) throw`.
    let mut def = inner_def();
    def.fields.push(UdtFieldDef {
        name: "b".to_string(),
        field_type: CqlType::Text,
        nullable: true,
    });
    let mut bytes = inner_a_1();
    bytes.extend_from_slice(&[0x00, 0x00]);
    let err = parser()
        .parse_udt_value(&bytes, 0, &def, &column("u", "frozen<inner>"), 0)
        .expect_err("an incomplete field-length prefix is corruption");
    assert!(err.to_string().contains("trailing byte"), "got: {}", err);

    // POSITIVE CONTROL: the same two-field def with NO stray bytes is legal.
    parser()
        .parse_udt_value(&inner_a_1(), 0, &def, &column("u", "frozen<inner>"), 0)
        .expect("omitted trailing fields at exact EOF are legal");
}

#[test]
fn a_nested_inline_udt_in_a_top_level_column_is_byte_checked_and_depth_bounded() {
    // Finding B's shape: the field's declared type is an INLINE nested UDT, which used
    // to recurse through `parse_udt_value` at depth 0 with its offset discarded.
    let def = UdtTypeDef {
        keyspace: "test_udt_collision".to_string(),
        name: "outer".to_string(),
        fields: vec![UdtFieldDef {
            name: "n".to_string(),
            field_type: CqlType::Udt("inline".to_string(), vec![("a".to_string(), CqlType::Int)]),
            nullable: true,
        }],
    };
    // Well-formed: `[i32 len][inner UDT bytes]`.
    let good = udt_field(&inner_a_1());
    parser()
        .parse_udt_value(&good, 0, &def, &column("u", "frozen<outer>"), 0)
        .expect("a well-formed inline nested UDT must decode");

    // The nested UDT's own slice carries a trailing byte: invisible before finding B's
    // fix, because the nested decoder's consumed count was dropped.
    let mut inner = inner_a_1();
    inner.push(0x7F);
    let bad = udt_field(&inner);
    let err = parser()
        .parse_udt_value(&bad, 0, &def, &column("u", "frozen<outer>"), 0)
        .expect_err("a trailing byte INSIDE the nested UDT must be refused");
    assert!(err.to_string().contains("trailing byte"), "got: {}", err);
}

// ════════════════════════════════════════════════════════════════════════════
// NULL COLLECTION ENTRIES (roborev round 4, finding C).
//
// Authority, read first-hand at the pinned tag:
// `cassandra-5.0.8:.../serializers/MapSerializer.java` `deserialize` reads BOTH halves
// of every entry with `readNonNullValue` (lines 136 and 140), and
// `CollectionSerializer.readNonNullValue` throws
// `MarshalException("Null value read when not allowed")` for the null that `readValue`
// returns on any `size < 0`. So a negative length is corruption on EITHER side, and
// surfacing the value side as `Value::Null` invented a value no writer can produce.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn a_negative_map_value_length_is_refused_like_a_negative_key_length() {
    let ty = CqlType::Frozen(Box::new(CqlType::Map(
        Box::new(CqlType::Text),
        Box::new(CqlType::Int),
    )));
    // one entry: key "a" (well-formed), value length -1
    let mut bytes = 1i32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&1i32.to_be_bytes());
    bytes.push(b'a');
    bytes.extend_from_slice(&(-1i32).to_be_bytes());
    let err = parser()
        .parse_simple_udt_field_value_at(&bytes, &ty, 0)
        .expect_err("a null map VALUE is not a legal entry");
    let msg = err.to_string();
    assert!(
        msg.contains("null map value"),
        "the error must name the illegal entry half; got: {msg}"
    );

    // The KEY side was already refused; asserted here so the two stay symmetric.
    let mut key_bad = 1i32.to_be_bytes().to_vec();
    key_bad.extend_from_slice(&(-1i32).to_be_bytes());
    key_bad.extend_from_slice(&4i32.to_be_bytes());
    key_bad.extend_from_slice(&1i32.to_be_bytes());
    assert!(
        parser()
            .parse_simple_udt_field_value_at(&key_bad, &ty, 0)
            .is_err(),
        "a null map KEY must stay refused"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// EMPTY FIXED-WIDTH VALUES (roborev round 4, finding D).
//
// The width gate ALLOWED an empty buffer while every delegate rejected empty input, so
// the allowance was dead code and the real behaviour was a worse error. Worse, the test
// that claimed to cover the rule exercised an empty MAP — a variable-width type — so it
// could not have caught it. Authority (pinned tag):
// `Int32Serializer.validate` = `if (accessor.size(value) != 4 && !accessor.isEmpty(value))
// throw ... "Expected 4 or 0 byte int"`, and `deserialize` =
// `accessor.isEmpty(value) ? null : accessor.toInt(value)`. So empty is LEGAL and means
// NULL — which is also what the sibling column-level path already did for `[i32 0]`.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn an_empty_fixed_width_field_is_null_not_an_error() {
    let p = parser();
    for ty in [
        CqlType::Int,
        CqlType::BigInt,
        CqlType::Boolean,
        CqlType::Float,
        CqlType::Double,
        CqlType::Uuid,
        CqlType::Timestamp,
        CqlType::SmallInt,
        CqlType::TinyInt,
        CqlType::Date,
        CqlType::Time,
    ] {
        let value = p
            .parse_simple_udt_field_value_at(&[], &ty, 0)
            .unwrap_or_else(|e| {
                panic!("empty {ty:?} must be legal (Cassandra: \"4 or 0 byte int\"), got {e}")
            });
        assert_eq!(
            value,
            Value::Null,
            "an empty fixed-width {ty:?} deserializes to NULL, per Int32Serializer.deserialize"
        );
    }
}

#[test]
fn an_empty_variable_width_field_keeps_its_own_empty_semantics() {
    let p = parser();
    assert_eq!(
        p.parse_simple_udt_field_value_at(&[], &CqlType::Text, 0)
            .expect("empty text is the empty string, not null"),
        Value::text("")
    );
    match p
        .parse_simple_udt_field_value_at(&[], &CqlType::Blob, 0)
        .expect("empty blob is the empty blob")
    {
        Value::Blob(b) => assert!(b.is_empty()),
        other => panic!("expected an empty blob, got {other:?}"),
    }
}

#[test]
fn an_oversized_fixed_width_field_is_still_refused() {
    // The other side of the same gate, on an actual FIXED-WIDTH type — which is the
    // coverage finding D says was missing, the previous case having used a map.
    let err = parser()
        .parse_simple_udt_field_value_at(&[0, 0, 0, 1, 0xFF], &CqlType::Int, 0)
        .expect_err("5 bytes is neither 4 nor 0");
    assert!(
        err.to_string().contains("Int field requires 4 bytes")
            || err.to_string().contains("4 bytes"),
        "got: {}",
        err
    );
}

// ════════════════════════════════════════════════════════════════════════════
// ZERO-LENGTH FIELDS — the FOURTH arm of the blob-fallback class (roborev round 5).
//
// `field_len == 0` used to call `create_empty_value_for_type`, whose fallback arm was
// `Value::blob(Vec::new())`, so an empty `int` field surfaced as an empty BLOB and an
// empty nested structured field could too. That is acceptance criterion 5 verbatim. The
// helper is now DELETED rather than fixed — an arm that does not exist cannot be
// reached by a sixth caller.
//
// SEMANTICS, from `cassandra-5.0.8` and not from CQLite's prior behaviour:
// `AbstractType.isEmptyValueMeaningless()` is documented "Returns true for types where
// empty should be handled like null like Int32Type", defaults FALSE, and is overridden
// TRUE by `Int32Type:56`, `LongType`, `BooleanType`, `UUIDType`, `TimestampType`;
// `AbstractType`'s deserializer is `if (buffer == null || (!buffer.hasRemaining() &&
// type.isEmptyValueMeaningless())) return null; return type.compose(buffer);`. So an
// empty fixed-width value is NULL, an empty text/blob is the empty string / empty blob,
// and an empty UDT is every-component-null (`TupleType.split` returning
// `copyOfRange(components, 0, 0)`).
//
// These cases drive the TWO ACTUAL LOOPS the finding names — `parse_udt_value` (the
// top-level frozen-UDT column) and `parse_raw_type_value` (the marshal form) — not a
// per-field proxy.
// ════════════════════════════════════════════════════════════════════════════

/// A UDT def whose three fields cover the three empty-value rules.
fn empty_rules_def() -> UdtTypeDef {
    UdtTypeDef {
        keyspace: "test_udt_collision".to_string(),
        name: "empties".to_string(),
        fields: vec![
            UdtFieldDef {
                name: "i".to_string(),
                field_type: CqlType::Int,
                nullable: true,
            },
            UdtFieldDef {
                name: "t".to_string(),
                field_type: CqlType::Text,
                nullable: true,
            },
            UdtFieldDef {
                name: "m".to_string(),
                field_type: CqlType::Frozen(Box::new(CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Int),
                ))),
                nullable: true,
            },
        ],
    }
}

#[test]
fn zero_length_fields_of_a_top_level_udt_column_decode_from_their_declared_type() {
    // Three fields, each written `[i32 0]`.
    let mut data = Vec::new();
    for _ in 0..3 {
        data.extend_from_slice(&0i32.to_be_bytes());
    }
    let (value, consumed) = parser()
        .parse_udt_value(
            &data,
            0,
            &empty_rules_def(),
            &column("u", "frozen<empties>"),
            0,
        )
        .expect("three zero-length fields must decode");
    assert_eq!(consumed, data.len());

    let udt = match unfrozen(&value) {
        Value::Udt(u) => u.clone(),
        other => panic!("expected a Udt, got {other:?}"),
    };
    // The DEFECT: every one of these used to be an empty `Value::Blob`.
    assert_eq!(
        udt.fields[0].value,
        Some(Value::Null),
        "an empty fixed-width field is NULL (Int32Type.isEmptyValueMeaningless == true), \
         not an empty blob"
    );
    assert_eq!(
        udt.fields[1].value,
        Some(Value::text("")),
        "an empty text field is the empty string (compose of an empty buffer)"
    );
    assert_eq!(
        udt.fields[2].value.as_ref().map(unfrozen).cloned(),
        Some(Value::Map(Vec::new())),
        "an empty frozen<map> field is the empty map, NOT a blob"
    );
    for field in &udt.fields {
        assert!(
            !matches!(field.value.as_ref().map(unfrozen), Some(Value::Blob(_))),
            "no zero-length field may decode to a Blob (issue #3631 criterion 5): {:?}",
            field
        );
    }
}

#[test]
fn a_zero_length_nested_udt_field_is_all_null_fields_not_a_blob() {
    // `TupleType.split`: an empty buffer returns `copyOfRange(components, 0, 0)`, so
    // every declared component is absent — a UDT with all fields null.
    let def = UdtTypeDef {
        keyspace: "test_udt_collision".to_string(),
        name: "outer".to_string(),
        fields: vec![UdtFieldDef {
            name: "n".to_string(),
            field_type: CqlType::Udt("inline".to_string(), vec![("a".to_string(), CqlType::Int)]),
            nullable: true,
        }],
    };
    let data = 0i32.to_be_bytes().to_vec(); // one field, zero-length
    let (value, _) = parser()
        .parse_udt_value(&data, 0, &def, &column("u", "frozen<outer>"), 0)
        .expect("a zero-length nested UDT field must decode structurally");
    let outer = match unfrozen(&value) {
        Value::Udt(u) => u.clone(),
        other => panic!("expected the outer Udt, got {other:?}"),
    };
    match outer.fields[0].value.as_ref().map(unfrozen) {
        Some(Value::Udt(inner)) => assert_eq!(
            inner.fields[0].value, None,
            "the nested UDT's declared field is absent, i.e. null"
        ),
        other => panic!("a zero-length nested UDT must be a Udt with null fields, got {other:?}"),
    }
}

#[test]
fn zero_length_fields_of_a_marshal_form_udt_decode_from_their_declared_type() {
    // The SECOND loop the finding names: `parse_raw_type_value`'s marshal path, driven
    // by an on-disk `UserType(...)` string rather than by a registry def. Hex-encoded
    // field names, per Cassandra's marshal spelling.
    let type_str = concat!(
        "org.apache.cassandra.db.marshal.UserType(",
        "test_udt_collision,",
        "656d7074696573,",                               // "empties"
        "69:org.apache.cassandra.db.marshal.Int32Type,", // i
        "74:org.apache.cassandra.db.marshal.UTF8Type",   // t
        ")"
    );
    let mut data = Vec::new();
    for _ in 0..2 {
        data.extend_from_slice(&0i32.to_be_bytes());
    }
    let (value, _consumed) = parser()
        .parse_raw_type_value(&data, 0, type_str, "u", 0)
        .expect("the marshal-form loop must decode zero-length fields too");
    let udt = match unfrozen(&value) {
        Value::Udt(u) => u.clone(),
        other => panic!("expected a Udt, got {other:?}"),
    };
    assert_eq!(udt.fields.len(), 2, "got {:?}", udt.fields);
    assert_eq!(
        udt.fields[0].value,
        Some(Value::Null),
        "an empty Int32Type field is NULL on the marshal path as well"
    );
    assert_eq!(udt.fields[1].value, Some(Value::text("")));
}

//! Issue #4114, roborev job 109 — blocker 1: a `vector<float, n>` reached through
//! the REGISTRY-backed value parser must be DECODED, not returned as a blob.
//!
//! # The defect this file pins
//!
//! `CqlType::Vector` maps to `CqlTypeId::Custom`
//! (`parser::types::cql_type_to_type_id`), and `CqlTypeId::Custom` decodes as a
//! VINT-LENGTH-PREFIXED blob (`parse_cql_value` -> `parse_blob`). That mapping is
//! only safe because every caller is supposed to intercept `CqlType::Vector` BEFORE
//! reaching a type id. `parse_cql_value_for_type_with_registry` did not — so a
//! vector living inside a registry-backed UDT FIELD, a collection ELEMENT, or a map
//! KEY/VALUE was handed to `parse_blob`, which reads a length prefix a fixed-width
//! vector value never carries. That is the #4114 defect exactly (a wrong value or an
//! error depending on ONE byte of user data), still reachable by a second route.
//!
//! The same function's zero-length sibling branch, `create_empty_value_for_cql_type`,
//! fell through to `Value::Null` for a vector. Cassandra has NO empty vector: a
//! zero-length vector value THROWS `MarshalException("Invalid empty vector value")`
//! (`VectorType.java:365-368`, pinned `cassandra-5.0.8`), and `dimension <= 0` is
//! rejected at construction (`:89-90`). Turning that invalid value into a legal-
//! looking `null` is a silent misread; a genuinely NULL field is a different thing
//! and stays null.
//!
//! # Oracle and what is synthesized (and why)
//!
//! The 12 vector bytes are the CASSANDRA-WRITTEN bytes from the committed fixture
//! `test-data/fixtures/issue_4114/test_vector/vector_clustered-*/nb-1-big-Data.db`
//! — `[1.0, 2.5, -3.75]` == `3f800000 40200000 c0700000`, per that fixture's
//! `sstabledump` golden (`…-Data.db.jsonl`) and the byte-level verification in
//! `.drive-issue-4114/format-authority.md`. `cassandra_written_vector_bytes()`
//! ASSERTS that exact sequence is present verbatim in the fixture file, so the
//! constant cannot drift away from Cassandra's own output (#3042: an oracle must not
//! be CQLite's own behaviour).
//!
//! The framing AROUND those bytes — the UDT field / list element length prefixes —
//! is SYNTHESIZED, and that is unavoidable: no committed fixture contains a vector
//! inside a UDT field or a collection, so there are no Cassandra-written bytes for
//! that shape to read. The framing written here is whatever the function under test
//! already reads (`be_i32` field lengths, a zigzag `encode_vint` collection count);
//! the property under test is NOT that framing but that the vector's own 12 bytes
//! are decoded as three big-endian binary32 elements instead of being blobbed.

use cqlite_core::parser::types::parse_udt_with_schema_and_registry;
use cqlite_core::parser::vint::encode_vint;
use cqlite_core::schema::{CqlType, UdtRegistry};
use cqlite_core::types::{UdtFieldDef, UdtTypeDef, Value};

/// `[1.0, 2.5, -3.75]` as Cassandra wrote it: 3 x big-endian binary32, no length
/// prefix, no element count, no per-element framing.
const VECTOR_BYTES: [u8; 12] = [
    0x3f, 0x80, 0x00, 0x00, 0x40, 0x20, 0x00, 0x00, 0xc0, 0x70, 0x00, 0x00,
];

/// The expected decode of [`VECTOR_BYTES`], per the fixture's `sstabledump` golden
/// (`{"name":"v3","value":[1.0,2.5,-3.75]}`).
const EXPECTED: [f32; 3] = [1.0, 2.5, -3.75];

/// Return [`VECTOR_BYTES`] only after confirming that exact byte sequence appears
/// verbatim in the committed Cassandra-written fixture.
///
/// Fail-closed and unconditional (#3220): this fixture is git-committed, not a
/// fetched dataset, so a missing file is a FAILURE and never a skip — a skipping
/// oracle is indistinguishable from one that verified nothing.
fn cassandra_written_vector_bytes() -> [u8; 12] {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../test-data/fixtures/issue_4114/test_vector"
    );
    let mut matches = Vec::new();
    for entry in
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("committed fixture dir {dir}: {e}"))
    {
        let entry = entry.expect("readable fixture directory entry");
        let table = entry.path();
        let name = table
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        if !name.starts_with("vector_clustered-") {
            continue;
        }
        let data = table.join("nb-1-big-Data.db");
        let bytes = std::fs::read(&data)
            .unwrap_or_else(|e| panic!("committed fixture {}: {e}", data.display()));
        assert!(
            bytes.windows(VECTOR_BYTES.len()).any(|w| w == VECTOR_BYTES),
            "the oracle bytes must be present verbatim in the Cassandra-written \
             {} — if this fails, the constant in this test no longer matches \
             Cassandra's own output and every assertion below is meaningless",
            data.display()
        );
        matches.push(name);
    }
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one committed vector_clustered fixture, found {matches:?}"
    );
    VECTOR_BYTES
}

/// A UDT whose FIRST field is a `vector<float, 3>` and whose second is a `text`, so
/// a cursor desync in the vector field is also visible as a wrong `label`.
fn udt_def(vector_field_type: CqlType) -> UdtTypeDef {
    UdtTypeDef {
        keyspace: "test_vector".to_string(),
        name: "embedding_udt".to_string(),
        fields: vec![
            UdtFieldDef {
                name: "embedding".to_string(),
                field_type: vector_field_type,
                nullable: true,
            },
            UdtFieldDef {
                name: "label".to_string(),
                field_type: CqlType::Text,
                nullable: true,
            },
        ],
    }
}

/// `[be_i32 length][value]` — the field framing
/// `parse_udt_with_schema_and_registry` already reads. A length of `-1` is a NULL
/// field and `0` a zero-length value.
fn framed(value: &[u8]) -> Vec<u8> {
    let mut out = (value.len() as i32).to_be_bytes().to_vec();
    out.extend_from_slice(value);
    out
}

fn framed_length(length: i32) -> Vec<u8> {
    length.to_be_bytes().to_vec()
}

fn floats(value: &Value) -> Vec<f32> {
    match value {
        Value::List(items) => items
            .iter()
            .map(|item| match item {
                Value::Float32(f) => *f,
                other => panic!("a vector element must be Float32, got {other:?}"),
            })
            .collect(),
        other => panic!(
            "a vector<float, 3> must decode to a 3-element sequence, got {other:?} \
             — a Blob here IS the #4114 defect (the value was sent to parse_blob, \
             which read a phantom vint length)"
        ),
    }
}

fn fields(value: &Value) -> Vec<(String, Option<Value>)> {
    match value {
        Value::Udt(udt) => udt
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.value.clone()))
            .collect(),
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// BLOCKER 1, primary case: a vector as a registry-backed UDT FIELD.
///
/// Before the fix this reached `cql_type_to_type_id` -> `CqlTypeId::Custom` ->
/// `parse_blob`, which reads the leading `0x3f` as a vint length (63 bytes) and
/// fails the whole UDT parse — and with a different leading byte would have returned
/// a WRONG blob at exit 0.
#[test]
fn a_vector_udt_field_is_decoded_by_the_registry_path_not_blobbed() {
    let vector = cassandra_written_vector_bytes();
    let def = udt_def(CqlType::Vector(Box::new(CqlType::Float), 3));
    let registry = UdtRegistry::new();

    let mut input = framed(&vector);
    input.extend_from_slice(&framed(b"row-10"));

    let (remaining, value) = parse_udt_with_schema_and_registry(&input, &def, &registry)
        .expect("a UDT whose first field is a vector<float, 3> must parse");

    let got = fields(&value);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, "embedding");
    let embedding = got[0].1.clone().expect("a non-null vector field");
    assert_eq!(floats(&embedding), EXPECTED.to_vec());
    // The field AFTER the vector proves the cursor advanced by exactly 4*n: a
    // desync inside the vector field corrupts every later field (#3890).
    assert_eq!(got[1].0, "label");
    assert_eq!(
        got[1].1,
        Some(Value::text("row-10".to_string())),
        "the field after the vector must be intact"
    );
    assert!(remaining.is_empty(), "the whole UDT body must be consumed");
}

/// BLOCKER 1, second case: a vector as a COLLECTION ELEMENT inside that UDT field.
///
/// `parse_list_with_element_type` calls the same
/// `parse_cql_value_for_type_with_registry` per element, so a `list<vector<float,3>>`
/// had every element blobbed by the same missing arm.
#[test]
fn a_vector_collection_element_is_decoded_by_the_registry_path_not_blobbed() {
    let vector = cassandra_written_vector_bytes();
    let def = udt_def(CqlType::List(Box::new(CqlType::Vector(
        Box::new(CqlType::Float),
        3,
    ))));
    let registry = UdtRegistry::new();

    // The list body `parse_list_with_element_type` reads: a zigzag vint element
    // count (the crate's own `encode_vint`, so the count framing is not hand-rolled)
    // followed by `[be_i32 length][element]` per element.
    let mut list_body = encode_vint(2);
    list_body.extend_from_slice(&framed(&vector));
    list_body.extend_from_slice(&framed(&vector));

    let mut input = framed(&list_body);
    input.extend_from_slice(&framed(b"row-10"));

    let (_, value) = parse_udt_with_schema_and_registry(&input, &def, &registry)
        .expect("a UDT field of type list<vector<float, 3>> must parse");

    let got = fields(&value);
    let list = got[0].1.clone().expect("a non-null list field");
    let elements = match list {
        Value::List(items) => items,
        other => panic!("expected a list of vectors, got {other:?}"),
    };
    assert_eq!(elements.len(), 2, "both elements must survive");
    for element in &elements {
        assert_eq!(floats(element), EXPECTED.to_vec());
    }
    assert_eq!(got[1].1, Some(Value::text("row-10".to_string())));
}

/// BLOCKER 1, zero-length branch: an EMPTY vector value must be REFUSED, never
/// turned into `Value::Null`.
///
/// `VectorType.java:365-368` throws "Invalid empty vector value"; there is no
/// dimension-0 vector to construct (`:89-90`). Before the fix this field took the
/// `length == 0` branch into `create_empty_value_for_cql_type`, whose `_` arm
/// returned `Value::Null` — an invalid value presented as a legal one.
#[test]
fn a_zero_length_vector_udt_field_is_refused_not_reported_as_null() {
    let def = udt_def(CqlType::Vector(Box::new(CqlType::Float), 3));
    let registry = UdtRegistry::new();

    let mut input = framed_length(0);
    input.extend_from_slice(&framed(b"row-10"));

    let result = parse_udt_with_schema_and_registry(&input, &def, &registry);
    assert!(
        result.is_err(),
        "a zero-length vector<float, 3> value is an ERROR in Cassandra, not an \
         empty vector and not null — got {result:?}"
    );
}

/// The distinction the previous test depends on: a genuinely NULL field (`-1`) is
/// still null. `VectorType.java:409-414` — "we don't allow empty vectors, so we can
/// just check for null" — null is legal, zero-length is not.
#[test]
fn a_null_vector_udt_field_stays_null() {
    let def = udt_def(CqlType::Vector(Box::new(CqlType::Float), 3));
    let registry = UdtRegistry::new();

    let mut input = framed_length(-1);
    input.extend_from_slice(&framed(b"row-10"));

    let (_, value) = parse_udt_with_schema_and_registry(&input, &def, &registry)
        .expect("a NULL vector field is legal and must parse");
    let got = fields(&value);
    assert_eq!(got[0].1, None, "a null vector field must stay null");
    assert_eq!(got[1].1, Some(Value::text("row-10".to_string())));
}

/// A truncated vector field is an error, not a short decode: the width comes from
/// the DECLARED dimension and nothing is inferred from how many bytes arrived (#28).
#[test]
fn a_truncated_vector_udt_field_is_refused_rather_than_short_decoded() {
    let vector = cassandra_written_vector_bytes();
    let def = udt_def(CqlType::Vector(Box::new(CqlType::Float), 3));
    let registry = UdtRegistry::new();

    let mut input = framed(&vector[..8]);
    input.extend_from_slice(&framed(b"row-10"));

    assert!(
        parse_udt_with_schema_and_registry(&input, &def, &registry).is_err(),
        "8 bytes cannot hold a vector<float, 3>"
    );
}

/// AC4 through this same path: an element type CQLite does not implement is refused
/// by the registry path too, never decoded as something else.
#[test]
fn a_non_float_vector_element_is_refused_by_the_registry_path() {
    let def = udt_def(CqlType::Vector(Box::new(CqlType::Double), 3));
    let registry = UdtRegistry::new();

    let mut input = framed(&[0u8; 24]);
    input.extend_from_slice(&framed(b"row-10"));

    assert!(
        parse_udt_with_schema_and_registry(&input, &def, &registry).is_err(),
        "vector<double, 3> is not implemented (issue #4114 AC4) and must be refused, \
         not decoded as a blob or as floats"
    );
}

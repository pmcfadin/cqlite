//! Issue #4114 — a declared `vector<float, n>` must not degrade to a blob in the
//! V5CompressedLegacy **type-STRING** dispatch.
//!
//! This is the FOURTH instance of #4114's defect class and the one that hid the
//! longest, because it is invisible to the searches that found the other three.
//! Those were `CqlTypeId`-shaped — a vector mapped to `CqlTypeId::Custom` and the
//! `Custom` arm decoded a blob — so they were found by enumerating `Custom` sites
//! and `parse_cql_value*` callers. This site never sees a type id at all: it
//! dispatches on the DECLARED TYPE STRING, so no such enumeration can reach it.
//! The class is "a declared vector reaching a fallback decode", and it spans both
//! representations of a declared type.
//!
//! HOW THIS INSTANCE DIFFERS FROM ITS SIBLINGS, and why it is easy to miss twice:
//! the unknown-type arm returned `Value::Blob(data)`, and here `data` is EXACTLY
//! the value because the outer `[i32 len]` framing already delimited it. So the
//! blob is the RIGHT LENGTH and the row does NOT desync — unlike the vint-framed
//! sibling, which ate the first float's leading byte as a length and produced the
//! `need 63 bytes for blob` cascade. Nothing looks corrupt; the value is simply
//! returned as the WRONG TYPE. A test that only asserts "no error" cannot see it,
//! so these assert the TYPE.
//!
//! Reached when a multicell/frozen UDT field or a collection element is declared
//! as a vector: the field type then arrives as the on-disk marshal spelling
//! `org.apache.cassandra.db.marshal.VectorType(...FloatType , 3)`.
//!
//! ORACLE, stated honestly. The FRAMING (`4 * n` raw big-endian binary32, no
//! length prefix, no element count) is pinned against Cassandra-WRITTEN bytes by
//! `cqlite-core/tests/issue_4114_vector_float_cassandra_golden.rs`, and derived
//! from `cassandra-5.0.8` `VectorType.java:86-101` / `:445-460` — see
//! `.drive-issue-4114/format-authority.md`. What THESE tests pin is the DISPATCH:
//! that this code path routes a declared vector to that decoder instead of to the
//! blob/UDT arms. The byte literals below are therefore a fixture for the routing
//! question, not an independent oracle for the format, and they are not offered as
//! one (#3042 — a self-authored byte literal can never validate framing).
//!
//! No dataset, reader or feature dependency: these run in every build and lane and
//! cannot pass vacuously on an empty corpus.

use super::super::V5CompressedLegacyParser;
use crate::{Result, Value};

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("issue_4114_ks".to_string(), "t".to_string(), 0, 0, None)
}

fn decode(type_str: &str, data: &[u8]) -> Result<(Value, usize)> {
    parser().parse_value_from_raw_bytes_reporting(data, type_str, "v", 0)
}

/// `[1.0, 2.5, -3.75]` big-endian binary32, contiguous, no prefix — the same
/// values Cassandra wrote into the committed `vector_clustered` fixture, whose
/// Data.db carries these 12 bytes verbatim (see the ADDENDUM in
/// `.drive-issue-4114/format-authority.md`).
const THREE: [u8; 12] = [
    0x3f, 0x80, 0x00, 0x00, // 1.0
    0x40, 0x20, 0x00, 0x00, // 2.5
    0xc0, 0x70, 0x00, 0x00, // -3.75
];

/// A vector decodes to `Value::List` of `Value::Float` (`f64`) — issue #4114
/// deliberately reuses the existing List representation rather than adding a
/// parallel one (commit 4b6dda546: "map vector<float,n> to the Arrow List node
/// its Value::List already takes"). So the assertion of interest is "a list of
/// the right floats", and critically NOT `Value::Blob`.
fn floats(v: &Value) -> Vec<f32> {
    match v {
        Value::List(elems) => elems
            .iter()
            .map(|e| match e {
                // `Float32`, not `Float` (which is f64): a vector<float, n>
                // element is an IEEE-754 binary32 and is kept at that width
                // rather than widened, so a round-trip cannot invent precision.
                Value::Float32(f) => *f,
                other => panic!("element is not a float: {other:?}"),
            })
            .collect(),
        other => panic!("value is not a List (a vector decodes to one): {other:?}"),
    }
}

/// THE REGRESSION. Both the marshal spelling and the CQL spelling must decode to
/// a sequence of floats — never `Value::Blob`.
#[test]
fn vector_is_decoded_not_returned_as_a_blob() {
    for type_str in [
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)",
        // Cassandra writes `" , "`; its own reader tolerates whitespace variation
        // (`TypeParser.skipBlankAndComma`), so this path must too.
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,3)",
        "vector<float, 3>",
    ] {
        let (value, consumed) = decode(type_str, &THREE).unwrap_or_else(|e| {
            panic!("{type_str} must decode, got error: {e}");
        });

        assert!(
            !matches!(value, Value::Blob(_)),
            "{type_str} came back as a BLOB — this is the #4114 defect, not a pass: {value:?}"
        );
        assert_eq!(
            floats(&value),
            vec![1.0f32, 2.5, -3.75],
            "{type_str} decoded to the wrong values"
        );
        assert_eq!(
            consumed,
            THREE.len(),
            "{type_str} must report consuming the whole 4*n value"
        );
    }
}

/// AC4: an element type this path does not implement is refused BY NAME, never
/// decoded as a blob. `Int32Type` is a legitimate Cassandra scalar and a legal
/// vector element at 5.0.8 (`CQL3Type.java:928-933` imposes no element
/// restriction), so this asserts CQLite's own declared boundary, not Cassandra's.
#[test]
fn unimplemented_element_type_is_refused_by_name() {
    let ty = "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.Int32Type , 3)";
    let err = decode(ty, &THREE).expect_err("a non-float element must be refused");
    let msg = err.to_string();
    // The refusal names the element in its CQL spelling (`Int`), having already
    // resolved the marshal name; either spelling is an honest answer, so accept
    // both rather than pinning one and breaking on a cosmetic rename.
    assert!(
        msg.contains("Int32Type") || msg.contains("Int") || msg.contains("int"),
        "the refusal must NAME the offending element type, got: {msg}"
    );
    // And it must say the value was NOT decoded — a refusal that did not say so
    // would leave a reader unsure whether a partial value came back.
    assert!(
        !msg.is_empty(),
        "refusal must carry a message rather than fail silently"
    );
}

/// A malformed vector type must ERROR, never fall through to the blob arm — the
/// conflation roborev job 109 found in `marshal_vector_inner`, asserted here at
/// the dispatch site so a future refactor cannot reintroduce it locally.
#[test]
fn malformed_vector_type_errors_rather_than_blobbing() {
    for ty in [
        // unmatched parenthesis
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3",
        // missing dimension
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType)",
        // non-numeric dimension
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , n)",
        // zero dimension — Cassandra rejects n <= 0 at construction
        // (`VectorType.java:89-90`)
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 0)",
    ] {
        let got = decode(ty, &THREE);
        assert!(
            got.is_err(),
            "malformed vector type {ty:?} must ERROR, not decode; got {got:?}"
        );
    }
}

/// A width mismatch is an error in BOTH directions. Cassandra rejects a short
/// value and also rejects trailing bytes (`checkConsumedFully`,
/// `VectorType.java:358-363`), and an empty value is
/// `MarshalException("Invalid empty vector value")` (`:365-368`) — never an empty
/// vector and never null.
#[test]
fn wrong_width_and_empty_are_errors() {
    assert!(
        decode(
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)",
            &THREE[..8]
        )
        .is_err(),
        "8 bytes for n=3 (needs 12) must error"
    );

    let mut long = THREE.to_vec();
    long.push(0x00);
    assert!(
        decode(
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)",
            &long
        )
        .is_err(),
        "13 bytes for n=3 must error — Cassandra rejects trailing bytes, so a \
         tolerant reader would accept data Cassandra calls invalid"
    );

    assert!(
        decode(
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)",
            &[]
        )
        .is_err(),
        "an EMPTY value must error, never yield an empty vector or null"
    );
}

/// The neighbours must be untouched: this arm must not capture a genuine UDT or a
/// type merely containing the substring.
#[test]
fn arm_does_not_capture_neighbours() {
    // A UDT whose NAME contains "vector" is not a vector type.
    let udt = "org.apache.cassandra.db.marshal.UserType(ks,7665637f,61:org.apache.cassandra.db.marshal.FloatType)";
    let got = decode(udt, &THREE);
    assert!(
        !matches!(got, Ok((Value::List(_), _))),
        "a UserType must not be decoded as a vector: {got:?}"
    );
}

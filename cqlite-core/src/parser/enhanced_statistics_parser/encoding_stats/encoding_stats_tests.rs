//! Pinned tests for the SerializationHeader failure-kind split (issue #4104,
//! roborev job 116).
//!
//! # What is being pinned, and why the two buffers differ
//!
//! `parse_minimal_encoding_stats` reads the schema from the TOC-anchored
//! `full_input[offset..]`, and — when that read fails — retries with the
//! marker-search decoder over the SEPARATE `input` slice. Every test here
//! exploits that: `input` is handed a header the marker search parses HAPPILY,
//! so "did the fallback run?" is answered by whether that schema comes back.
//!
//! * A frozen-scalar header at the anchored offset must be REFUSED even though
//!   the fallback would have succeeded — proving the fallback is not reached for
//!   a semantic refusal (the fail-open hole this issue closes).
//! * A truncated header at the anchored offset must still reach the fallback and
//!   return its schema — proving the fix narrowed the fallback rather than
//!   disabling it.
//!
//! # Authority
//!
//! A `frozen<scalar>` column cannot exist in Cassandra: `CQL3Type.Raw::freeze()`
//! throws for every non-collection/tuple/UDT/vector (cassandra-5.0.8
//! `src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`), so no Cassandra
//! writer can record one in a SerializationHeader. Expectations below derive
//! from that, never from CQLite's own prior behaviour — the prior behaviour was
//! the defect.

use super::parse_minimal_encoding_stats;
use crate::parser::enhanced_statistics_parser::schema_refusal::HeaderSchemaError;
use crate::parser::enhanced_statistics_parser::serialization_header::{
    parse_serialization_header, parse_serialization_header_schema,
};
use crate::parser::vint::encode_vuint;

const MARSHAL: &str = "org.apache.cassandra.db.marshal.";

/// A fully-qualified marshal class name (the marker search requires the
/// `org.apache.cassandra.db.marshal` substring to accept a candidate header).
fn marshal(simple: &str) -> String {
    format!("{MARSHAL}{simple}")
}

/// `FrozenType(Int32Type)` — a spelling no Cassandra writer can emit.
fn frozen_scalar() -> String {
    format!("{MARSHAL}FrozenType({MARSHAL}Int32Type)")
}

/// Append a VInt-length-prefixed UTF-8 string, as SerializationHeader.java does.
fn push_str(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&encode_vuint(value.len() as u64));
    out.extend_from_slice(value.as_bytes());
}

/// Build a SerializationHeader schema section: keyType, clusteringTypes,
/// staticColumns, regularColumns (SerializationHeader.java field order).
fn schema_blob(pk_type: &str, clustering: &[&str], regular: &[(&str, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    push_str(&mut out, pk_type);
    out.extend_from_slice(&encode_vuint(clustering.len() as u64));
    for ck in clustering {
        push_str(&mut out, ck);
    }
    // staticColumns: none.
    out.extend_from_slice(&encode_vuint(0));
    out.extend_from_slice(&encode_vuint(regular.len() as u64));
    for (name, ty) in regular {
        push_str(&mut out, name);
        push_str(&mut out, ty);
    }
    out
}

/// The three epoch-relative EncodingStats VInts that precede the schema.
fn with_encoding_stats(schema: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8, 0u8, 0u8];
    out.extend_from_slice(schema);
    out
}

/// A header the marker-search fallback parses successfully, used as the
/// `input` slice so a reached fallback is observable.
fn fallback_parsable_header() -> Vec<u8> {
    schema_blob(
        &marshal("UTF8Type"),
        &[],
        &[("fallback_col", marshal("Int32Type"))],
    )
}

/// A well-formed header whose one regular column freezes a scalar.
fn frozen_scalar_header() -> Vec<u8> {
    schema_blob(&marshal("UTF8Type"), &[], &[("v", frozen_scalar())])
}

// ── The failure-kind split, at the decoder's own boundary ────────────────────

/// A frozen-scalar column type leaves the schema decoder as `Refused` (semantic),
/// NOT as `Structural` — the distinction the caller's fail-closed branch keys on.
/// Asserted on the VARIANT; no message text is inspected.
#[test]
fn frozen_scalar_column_leaves_schema_decoder_as_refused() {
    let header = frozen_scalar_header();
    match parse_serialization_header_schema(&header) {
        Err(HeaderSchemaError::Refused(_)) => {}
        Err(HeaderSchemaError::Structural(e)) => panic!(
            "frozen<scalar> must be a SEMANTIC refusal; got a structural failure \
             (which the caller is allowed to retry heuristically): {e:?}"
        ),
        Ok((_, (pk, ck, cols))) => panic!(
            "frozen<scalar> header was ACCEPTED: pk={pk:?} ck={ck:?} cols={}",
            cols.len()
        ),
    }
}

/// A truncated header leaves the decoder as `Structural` — the kind the
/// marker-search fallback may legitimately retry.
#[test]
fn truncated_header_leaves_schema_decoder_as_structural() {
    // Declares an 80-byte keyType and supplies none of it.
    let truncated = [0x50u8];
    match parse_serialization_header_schema(&truncated) {
        Err(HeaderSchemaError::Structural(_)) => {}
        Err(HeaderSchemaError::Refused(e)) => {
            panic!("a truncated header is a POSITIONING failure, not a semantic refusal: {e}")
        }
        Ok(_) => panic!("a keyType declaring 80 absent bytes must not parse"),
    }
}

/// The control: a well-formed header decodes through the anchored path.
#[test]
fn well_formed_header_decodes_through_the_anchored_path() {
    let header = schema_blob(
        &marshal("UTF8Type"),
        &[&marshal("TimestampType")],
        &[("v", marshal("Int32Type"))],
    );
    let (_, (pk, ck, cols)) = parse_serialization_header_schema(&header)
        .unwrap_or_else(|e| panic!("well-formed header must decode: {e:?}"));
    assert_eq!(pk, vec!["text".to_string()], "keyType");
    assert_eq!(ck.len(), 1, "one clustering type");
    assert_eq!(cols.len(), 1, "one regular column");
    assert_eq!(cols[0].name, "v");
    assert_eq!(cols[0].column_type, "int");
}

// ── The caller: fail-closed on semantic, fallback preserved on structural ────

/// THE REGRESSION THIS ISSUE CLOSES: a frozen-scalar SerializationHeader at the
/// TOC-anchored offset is refused, even though the marker-search fallback over
/// `input` would have returned a schema. So the fallback is NOT reached for a
/// semantic refusal, and the column-type gate now agrees with the key-type gate
/// one branch below it (which already failed closed).
#[test]
fn frozen_scalar_header_is_refused_and_never_marker_searched() {
    let fallback_input = fallback_parsable_header();
    let full_input = with_encoding_stats(&frozen_scalar_header());

    // Precondition: the fallback really would have succeeded over `input`, so a
    // refusal below cannot be explained by the fallback failing too.
    let (_, (fb_pk, _, fb_cols)) = parse_serialization_header(&fallback_input)
        .unwrap_or_else(|e| panic!("test fixture must be marker-search parsable: {e:?}"));
    assert!(
        !fb_pk.is_empty() && !fb_cols.is_empty(),
        "fixture precondition: the fallback yields a non-empty schema"
    );

    let err = match parse_minimal_encoding_stats(&fallback_input, &full_input, Some(0), None) {
        Err(e) => e,
        Ok((_, (_, _, _, _, _, cols))) => panic!(
            "a frozen<scalar> SerializationHeader was ACCEPTED via the marker-search \
             fallback ({} columns) — the refusal must be fail-CLOSED",
            cols.len()
        ),
    };
    match err {
        nom::Err::Error(e) => assert_eq!(
            e.code,
            nom::error::ErrorKind::Verify,
            "refusals surface as a Verify failure"
        ),
        other => panic!("expected a recoverable Verify error, got {other:?}"),
    }
}

/// The fallback is NARROWED, not removed: a genuinely truncated header at the
/// anchored offset still falls back to the marker search, and the schema that
/// comes back is the fallback's own.
#[test]
fn truncated_header_still_falls_back_to_marker_search() {
    let fallback_input = fallback_parsable_header();
    // Anchored bytes: valid EncodingStats, then a keyType declaring 80 absent bytes.
    let full_input = with_encoding_stats(&[0x50u8]);

    let (_, (min_ts, _, _, pk_cols, ck_cols, cols)) =
        parse_minimal_encoding_stats(&fallback_input, &full_input, Some(0), None)
            .unwrap_or_else(|e| panic!("structural failure must still reach the fallback: {e:?}"));

    assert_eq!(
        cols.len(),
        1,
        "the fallback's schema must come back (marker-search derived)"
    );
    assert_eq!(cols[0].name, "fallback_col");
    assert_eq!(pk_cols.len(), 1, "fallback partition key column");
    assert!(ck_cols.is_empty(), "fixture declares no clustering keys");
    // The EncodingStats themselves came from the anchored offset (all deltas 0).
    assert_eq!(min_ts, 1_442_880_000_000_000, "minTimestamp epoch baseline");
}

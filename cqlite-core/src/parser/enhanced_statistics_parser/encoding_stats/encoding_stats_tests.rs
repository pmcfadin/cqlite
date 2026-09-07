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
//! # Two more groups, added by the #4158 review round
//!
//! * THE NO-TOC PATH (roborev job 119): with `header_offset = None` there is no
//!   anchored read at all, so the marker search is the ONLY decoder. A refusal
//!   there used to be indistinguishable from a failed candidate, so the search
//!   continued and ended at `parse_serialization_header`'s empty-schema `Ok` —
//!   fail-open even though every per-candidate gate had refused. Settled by test,
//!   not by reading: `a_frozen_scalar_header_is_refused_on_the_no_toc_marker_search_path_too`
//!   carries its own legal-type control so the refusal cannot be a fixture artifact.
//! * THE MESSAGE, at the public boundary (blocker A): a refusal must reach the user
//!   as text naming the refused type and citing its oracle, not as
//!   `code: Verify` relabelled `Corruption`
//!   (`the_public_entry_point_surfaces_the_refusal_message`).
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
use crate::error::Error;
use crate::parser::enhanced_statistics_parser::parse_nb_format_statistics_data;
use crate::parser::enhanced_statistics_parser::schema_refusal::HeaderSchemaError;
use crate::parser::enhanced_statistics_parser::serialization_header::{
    parse_serialization_header, parse_serialization_header_schema,
};
use crate::parser::statistics::StatisticsHeader;
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
    // THE VARIANT is the fail-closed decision (#4104 job 116) …
    let HeaderSchemaError::Refused(refusal) = err else {
        panic!("a frozen<scalar> must leave as a SEMANTIC refusal, got {err:?}");
    };
    // … and THE MESSAGE is what the user sees (#4158 review, blocker A). Before it
    // was carried, this refusal reached the user as
    // `Corruption: … Error { input: [..], code: Verify }` — shape-identical to a
    // truncated file — while the citation-carrying text existed only in a
    // `tracing::error!` nobody had `RUST_LOG` on for.
    let msg = refusal.to_string();
    for expected in [
        // the refused type, verbatim
        "FrozenType(org.apache.cassandra.db.marshal.Int32Type)",
        // the writer rule that was applied
        "includeFrozenType",
        // and the CQL oracle
        "CQL3Type.java:647-651",
    ] {
        assert!(
            msg.contains(expected),
            "the surfaced refusal must name `{expected}`; got: {msg}"
        );
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

// ── The NO-TOC path: what does the marker search do with a semantic refusal? ──

/// The `parse_encoding_stats_fallback` preamble that precedes the schema when no
/// TOC HEADER offset is available: metadata_type (u32 BE), data_length,
/// partitioner length + string, then two skipped VInts, then the three
/// EncodingStats deltas.
fn fallback_preamble_then(schema: &[u8]) -> Vec<u8> {
    const PARTITIONER: &str = "org.apache.cassandra.dht.Murmur3Partitioner";
    let mut out = vec![0x00, 0x00, 0x00, 0x03]; // metadata_type
    out.extend_from_slice(&encode_vuint(0)); // data_length
    push_str(&mut out, PARTITIONER);
    out.extend_from_slice(&encode_vuint(0)); // skipped metadata 1
    out.extend_from_slice(&encode_vuint(0)); // skipped metadata 2
    out.extend_from_slice(&[0x00, 0x00, 0x00]); // EncodingStats deltas
    out.extend_from_slice(schema);
    out
}

/// EMPIRICAL ANSWER to the disagreement in roborev job 119 (#4104): with NO TOC
/// header offset, a frozen-scalar column reaches `parse_encoding_stats_fallback`
/// and therefore the MARKER-SEARCH decoder — and the refusal must be FAIL-CLOSED
/// there too.
///
/// The disagreement was about what the marker-search fallbacks do. Both reviewers
/// were half right, and reading alone could not settle it: the per-candidate gates
/// DO fire (`sequential.rs`'s four `convert_marshal_type_to_cql_checked` sites and
/// `mod.rs`'s `convert_marshal_type_to_cql_logged` ones each reject the candidate),
/// but a rejected candidate was only "this offset holds no readable header", so the
/// SEARCH continued and `parse_serialization_header` ended at its
/// `Ok((input, (Vec::new(), Vec::new(), Vec::new())))` empty-schema success. Net
/// effect: a header declaring `FrozenType(Int32Type)` was ACCEPTED with an empty
/// schema — fail-open, exactly as job 119 reported, even though every individual
/// gate had refused.
///
/// The fixture is a header the marker search parses HAPPILY except for its one
/// frozen-scalar column type, so an `Err` here can only come from the refusal.
#[test]
fn a_frozen_scalar_header_is_refused_on_the_no_toc_marker_search_path_too() {
    let no_toc = fallback_preamble_then(&frozen_scalar_header());

    // Control: the SAME buffer with a legal column type decodes, so the refusal
    // below is attributable to the type and not to the fixture's framing.
    let legal = fallback_preamble_then(&schema_blob(
        &marshal("UTF8Type"),
        &[],
        &[("v", marshal("Int32Type"))],
    ));
    let (_, (_, _, _, pk_cols, _, cols)) = parse_minimal_encoding_stats(&legal, &legal, None, None)
        .unwrap_or_else(|e| panic!("the no-TOC fixture must decode when legal: {e:?}"));
    assert_eq!(cols.len(), 1, "control: one regular column decoded");
    assert_eq!(cols[0].name, "v");
    assert_eq!(pk_cols.len(), 1, "control: one partition key column");

    let err = match parse_minimal_encoding_stats(&no_toc, &no_toc, None, None) {
        Err(e) => e,
        Ok((_, (_, _, _, _, _, cols))) => panic!(
            "FAIL-OPEN: a frozen<scalar> SerializationHeader was ACCEPTED on the \
             no-TOC marker-search path with {} column(s) — a semantic refusal must \
             fail closed there exactly as it does on the TOC-anchored path",
            cols.len()
        ),
    };
    let HeaderSchemaError::Refused(refusal) = err else {
        panic!(
            "the marker-search path must report a SEMANTIC refusal, not a structural \
             failure (which a caller is allowed to retry heuristically): {err:?}"
        );
    };
    assert!(
        refusal
            .to_string()
            .contains("FrozenType(org.apache.cassandra.db.marshal.Int32Type)"),
        "the refusal must name the refused type: {refusal}"
    );
}

/// The refusal at the PUBLIC boundary: `parse_nb_format_statistics_data` returns a
/// `Schema` error whose text names the refused type and cites its oracle (#4158
/// review, blocker A).
///
/// This is the assertion that matters for a user, and it is made on the public
/// entry point rather than on an internal channel: before the message was carried,
/// this same input produced
/// `UnsupportedFormat("… EncodingStats: Error { input: [..], code: Verify }")`,
/// which `StatisticsReader::open` then relabelled `Corruption` — a deliberate
/// refusal presented as a garbled file.
#[test]
fn the_public_entry_point_surfaces_the_refusal_message() {
    let header = StatisticsHeader {
        version: 4,
        statistics_kind: 0x2629_1b05,
        data_length: 44,
        metadata1: 1,
        metadata2: 101,
        metadata3: 2,
        checksum: 0x14d4,
        table_id: None,
    };
    let bytes = fallback_preamble_then(&frozen_scalar_header());

    let err = parse_nb_format_statistics_data(&bytes, &header, &bytes, None)
        .err()
        .unwrap_or_else(|| panic!("a frozen<scalar> header must not be accepted"));

    assert!(
        matches!(err, Error::Schema(_)),
        "a semantic refusal must keep the `Schema` kind rather than being \
         relabelled as unreadable/corrupt data: {err:?}"
    );
    let msg = err.to_string();
    for expected in [
        "FrozenType(org.apache.cassandra.db.marshal.Int32Type)",
        "includeFrozenType",
        "CQL3Type.java:647-651",
    ] {
        assert!(
            msg.contains(expected),
            "the user-visible error must name `{expected}`; got: {msg}"
        );
    }
    // And it must NOT read like a parse failure of unreadable bytes.
    assert!(
        !msg.contains("code: Verify"),
        "the refusal must not surface as a bare nom error: {msg}"
    );
}

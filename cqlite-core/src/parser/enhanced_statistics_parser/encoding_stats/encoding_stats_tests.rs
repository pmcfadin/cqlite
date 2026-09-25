//! Pinned tests for the SerializationHeader refusal contract — #4104's
//! `frozen<scalar>` gate, re-homed by #4159 onto the one decoder that survives.
//!
//! # Why this file was rewritten rather than merged
//!
//! #4104 built these cases around a THREE-way choice: the TOC-anchored decoder, a
//! marker-search fallback over a separate `input` slice, and a no-TOC path where
//! the marker search was the ONLY decoder. Every case exploited that choice —
//! "did the fallback run?" was answered by whether the fallback's schema came
//! back. #4159 deleted the marker search (it inferred structure from byte
//! patterns, #28, and made a legitimate refusal unobservable by substituting a
//! plausible-looking column list decoded at a guessed offset), so there is no
//! second decoder left to observe, and the questions those fixtures asked no
//! longer have referents.
//!
//! What DOES survive is the property #4104 actually bought, and it is the thing
//! this file exists to pin:
//!
//! * a header declaring a type Cassandra cannot write is REFUSED, and
//! * the refusal keeps the `Error::Schema` KIND and its own message from the
//!   decoder all the way to the public entry point.
//!
//! # The KIND replaces the VARIANT, and pins the same distinction
//!
//! #4104 expressed "semantic refusal vs structural failure" as
//! `HeaderSchemaError::{Refused, Structural}`, because a caller had to decide
//! whether retrying with the marker search was legitimate. With one decoder there
//! is no retry to authorise, so `Structural` lost its only consumer and the enum
//! collapsed. `Refused` did not: its `Error::Schema` kind is what stops a
//! deliberate refusal being presented to the user as a corrupt file, and it is
//! what `StatisticsReader::open` keys on. So every assertion below that #4104 made
//! on a VARIANT is made here on the KIND — the same two-way distinction, at the
//! same boundaries, one representation later.
//!
//! # Authority
//!
//! A `frozen<scalar>` column cannot exist in Cassandra: `CQL3Type.Raw::freeze()`
//! throws for every non-collection/tuple/UDT/vector (cassandra-5.0.8
//! `src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`), so no Cassandra
//! writer can record one in a SerializationHeader. Expectations below derive from
//! that, never from CQLite's own prior behaviour — the prior behaviour was the
//! defect.

use crate::error::Error;
use crate::parser::enhanced_statistics_parser::parse_nb_format_statistics_data;
use crate::parser::enhanced_statistics_parser::serialization_header::parse_serialization_header_schema;
use crate::parser::enhanced_statistics_parser::parse_nb_format_header;
use crate::parser::vint::encode_vuint;

const MARSHAL: &str = "org.apache.cassandra.db.marshal.";

/// The TOC entry type of the `SERIALIZATION_HEADER` component
/// (`MetadataType.HEADER.ordinal()` = 3, cassandra-5.0.8
/// `src/java/org/apache/cassandra/io/sstable/metadata/MetadataType.java`).
const METADATA_TYPE_HEADER: u32 = 3;

/// A fully-qualified marshal class name, as `SerializationHeader.java` writes it.
fn marshal(simple: &str) -> String {
    format!("{MARSHAL}{simple}")
}

/// `FrozenType(Int32Type)` — a spelling no Cassandra writer can emit.
fn frozen_scalar() -> String {
    format!("{MARSHAL}FrozenType({MARSHAL}Int32Type)")
}

/// Append a VInt-length-prefixed UTF-8 string, as `SerializationHeader.java` does.
fn push_str(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&encode_vuint(value.len() as u64));
    out.extend_from_slice(value.as_bytes());
}

/// Build a SerializationHeader schema section: keyType, clusteringTypes,
/// staticColumns, regularColumns (`SerializationHeader.java` field order).
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

/// A well-formed header whose one REGULAR column freezes a scalar.
fn frozen_scalar_header() -> Vec<u8> {
    schema_blob(&marshal("UTF8Type"), &[], &[("v", frozen_scalar())])
}

/// A well-formed header every gate accepts — the control for each refusal below,
/// so an `Err` can never be explained by the fixture's framing.
fn legal_header() -> Vec<u8> {
    schema_blob(
        &marshal("UTF8Type"),
        &[&marshal("TimestampType")],
        &[("v", marshal("Int32Type"))],
    )
}

/// A synthetic `Statistics.db` whose TOC declares ONE component — the
/// `SERIALIZATION_HEADER`, at byte 32 — holding the three EncodingStats deltas
/// then `schema`.
///
/// The first `u32` is read TWICE by design: `walk_statistics_toc` reads it as the
/// TOC component count and `parse_nb_format_header` reads the same bytes as the
/// outer header's `version` field, exactly as they overlap in a real file.
fn statistics_db_with_toc_header(schema: &[u8]) -> Vec<u8> {
    const HEADER_OFFSET: u32 = 32;
    let mut out = Vec::new();
    out.extend_from_slice(&1u32.to_be_bytes()); // TOC: one component
    out.extend_from_slice(&0u32.to_be_bytes()); // TOC bytes [4..8) are not entries
    out.extend_from_slice(&METADATA_TYPE_HEADER.to_be_bytes()); // entry 0: type
    out.extend_from_slice(&HEADER_OFFSET.to_be_bytes()); // entry 0: offset
    out.resize(HEADER_OFFSET as usize, 0); // rest of the 32-byte outer header
    out.extend_from_slice(&[0u8, 0u8, 0u8]); // three EncodingStats deltas
    out.extend_from_slice(schema);
    out
}

/// A synthetic `Statistics.db` whose TOC declares NO `HEADER` component. #4104's
/// fixtures reached the marker search this way; #4159 removed it, so this input
/// now has no route to a SerializationHeader at all.
fn statistics_db_without_toc_header(schema: &[u8]) -> Vec<u8> {
    const OTHER_COMPONENT: u32 = 0; // `MetadataType.VALIDATION`, not HEADER
    let mut out = Vec::new();
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&OTHER_COMPONENT.to_be_bytes());
    out.extend_from_slice(&32u32.to_be_bytes());
    out.resize(32, 0);
    out.extend_from_slice(&[0u8, 0u8, 0u8]);
    out.extend_from_slice(schema);
    out
}

/// Drive the public entry point over `bytes`, whose own outer header is parsed
/// from those same bytes so the two can never disagree.
fn public_entry_point(bytes: &[u8]) -> crate::error::Result<()> {
    let (_, header) = parse_nb_format_header(bytes)
        .unwrap_or_else(|e| panic!("the synthetic outer header must parse: {e:?}"));
    parse_nb_format_statistics_data(&header, bytes, None).map(|_| ())
}

// ── The refusal, at the decoder's own boundary ───────────────────────────────

/// PORTED FROM #4104 (`origin/main`
/// `encoding_stats/encoding_stats_tests.rs:118`). A frozen-scalar column type
/// leaves the schema decoder as a SEMANTIC refusal — `Error::Schema`, #4104's
/// `HeaderSchemaError::Refused` one representation later — and NOT as the
/// structural `Error::Corruption` that says "the bytes here are not a readable
/// header". That is the distinction the caller's fail-closed branch keys on.
/// Asserted on the KIND; no message text is inspected.
#[test]
fn frozen_scalar_column_leaves_schema_decoder_as_refused() {
    let header = frozen_scalar_header();
    match parse_serialization_header_schema(&header) {
        Err(Error::Schema(_)) => {}
        Err(e @ Error::Corruption(_)) => panic!(
            "frozen<scalar> must be a SEMANTIC refusal; got a structural failure \
             (the kind that says the header is unreadable rather than illegal): {e:?}"
        ),
        Err(e) => panic!("frozen<scalar> must be refused as `Error::Schema`, got: {e:?}"),
        Ok((pk, ck, cols)) => panic!(
            "frozen<scalar> header was ACCEPTED: pk={pk:?} ck={ck:?} cols={}",
            cols.len()
        ),
    }
}

/// The other half of the same distinction: a TRUNCATED header is a POSITIONING /
/// readability failure, so it must NOT borrow the semantic kind. Without this the
/// test above could be satisfied by a decoder that answered `Error::Schema` for
/// everything.
#[test]
fn a_truncated_header_leaves_schema_decoder_as_structural() {
    // Declares an 80-byte keyType and supplies none of it.
    let truncated = [0x50u8];
    match parse_serialization_header_schema(&truncated) {
        Err(Error::Corruption(_)) => {}
        Err(e @ Error::Schema(_)) => panic!(
            "a truncated header is a POSITIONING failure, not a semantic refusal: {e}"
        ),
        Err(e) => panic!("expected a structural `Error::Corruption`, got: {e:?}"),
        Ok(_) => panic!("a keyType declaring 80 absent bytes must not parse"),
    }
}

/// The control: a well-formed header decodes through the anchored path, so every
/// refusal above is attributable to the type and not to the fixture's framing.
#[test]
fn a_well_formed_header_decodes_through_the_anchored_path() {
    let (pk, ck, cols) = parse_serialization_header_schema(&legal_header())
        .unwrap_or_else(|e| panic!("well-formed header must decode: {e:?}"));
    assert_eq!(pk, vec!["text".to_string()], "keyType");
    assert_eq!(ck.len(), 1, "one clustering type");
    assert_eq!(cols.len(), 1, "one regular column");
    assert_eq!(cols[0].name, "v");
    assert_eq!(cols[0].column_type, "int");
}

// ── Every gate site #4104 installed, still installed ─────────────────────────
//
// #4104 spelled its column gate out at three call sites, because its decoder
// duplicated the column loop. #4159's decoder shares one loop between
// `staticColumns` and `regularColumns`, so the same three semantic sites are two
// literal ones — plus the KEY comparators, which are gated in
// `build_column_infos` one layer up. These cases pin all four sites by BEHAVIOUR,
// so a future edit cannot drop one and stay green.

/// The partition-key gate site (#4104's `schema.rs:61`).
#[test]
fn a_frozen_scalar_partition_key_type_is_refused() {
    let header = schema_blob(&frozen_scalar(), &[], &[("v", marshal("Int32Type"))]);
    let err = parse_serialization_header_schema(&header)
        .err()
        .unwrap_or_else(|| panic!("a frozen<scalar> keyType must not be accepted"));
    assert!(
        matches!(err, Error::Schema(_)),
        "the partition-key gate must refuse semantically: {err:?}"
    );
    assert!(
        err.to_string().contains("partition key type"),
        "the refusal must name which field it refused: {err}"
    );
}

/// The STATIC-column half of the shared column loop (#4104's `schema.rs:183`).
/// `schema_blob` writes no static columns, so this one is assembled by hand.
#[test]
fn a_frozen_scalar_static_column_type_is_refused() {
    let mut header = Vec::new();
    push_str(&mut header, &marshal("UTF8Type")); // keyType
    header.extend_from_slice(&encode_vuint(0)); // clusteringTypes: none
    header.extend_from_slice(&encode_vuint(1)); // staticColumns: one
    push_str(&mut header, "s");
    push_str(&mut header, &frozen_scalar());
    header.extend_from_slice(&encode_vuint(0)); // regularColumns: none

    let err = parse_serialization_header_schema(&header)
        .err()
        .unwrap_or_else(|| panic!("a frozen<scalar> static column must not be accepted"));
    assert!(
        matches!(err, Error::Schema(_)),
        "the static-column gate must refuse semantically: {err:?}"
    );
    assert!(
        err.to_string().contains("static column 0 ('s')"),
        "the refusal must name the refused column: {err}"
    );
}

/// The REGULAR-column half of the same loop (#4104's `schema.rs:267`).
#[test]
fn a_frozen_scalar_regular_column_type_is_refused() {
    let err = parse_serialization_header_schema(&frozen_scalar_header())
        .err()
        .unwrap_or_else(|| panic!("a frozen<scalar> regular column must not be accepted"));
    assert!(
        err.to_string().contains("regular column 0 ('v')"),
        "the refusal must name the refused column: {err}"
    );
}

/// The KEY-comparator gate, which lives one layer up in `build_column_infos`
/// (#4159 step 3 — `build_column_infos(..)?`). A clustering comparator is kept as
/// a RAW marshal string by the decoder so the DESC signal survives, so it is NOT
/// gated by the two sites above: it is refused here or nowhere.
#[test]
fn a_frozen_scalar_clustering_comparator_is_refused_by_build_column_infos() {
    let bytes = statistics_db_with_toc_header(&schema_blob(
        &marshal("UTF8Type"),
        &[&frozen_scalar()],
        &[("v", marshal("Int32Type"))],
    ));
    let err = public_entry_point(&bytes)
        .err()
        .unwrap_or_else(|| panic!("a frozen<scalar> clustering comparator must not be accepted"));
    assert!(
        matches!(err, Error::Schema(_)),
        "the key-comparator gate must refuse semantically: {err:?}"
    );
    assert!(
        err.to_string()
            .contains("FrozenType(org.apache.cassandra.db.marshal.Int32Type)"),
        "the refusal must name the refused type: {err}"
    );
}

// ── The refusal at the PUBLIC boundary: the KIND survives every layer ────────

/// PORTED FROM #4104 (`origin/main` `encoding_stats/encoding_stats_tests.rs:337`).
/// The refusal at the public boundary: `parse_nb_format_statistics_data` returns a
/// `Schema` error whose text names the refused type and cites its oracle.
///
/// This is the assertion that matters for a user, and it is made on the public
/// entry point rather than on an internal channel: before the message was carried,
/// this same input produced `UnsupportedFormat("… EncodingStats: Error { input:
/// [..], code: Verify }")`, which `StatisticsReader::open` then relabelled
/// `Corruption` — a deliberate refusal presented as a garbled file.
///
/// ADAPTED FROM THE ORIGINAL IN ONE RESPECT: #4104 fed this through a NO-TOC
/// buffer, because that was the route to its marker-search decoder. #4159 refuses
/// a no-TOC file outright (see the replacement case below), so the input here is
/// TOC-ANCHORED — which is the only route to a SerializationHeader that exists,
/// and therefore the only one on which "the message reaches the user" can still be
/// asked. The assertions are unchanged.
#[test]
fn the_public_entry_point_surfaces_the_refusal_message() {
    let bytes = statistics_db_with_toc_header(&frozen_scalar_header());

    // Control: the SAME buffer shape with a legal column type decodes, so the
    // refusal below is attributable to the type and not to the fixture.
    public_entry_point(&statistics_db_with_toc_header(&legal_header()))
        .unwrap_or_else(|e| panic!("the TOC-anchored fixture must decode when legal: {e}"));

    let err = public_entry_point(&bytes)
        .err()
        .unwrap_or_else(|| panic!("a frozen<scalar> header must not be accepted"));

    // ── THE KIND PIN (`origin/main` `encoding_stats_tests.rs:349`) ──
    // This is the assertion that fails if any layer between the decoder and here
    // coerces the error kind. It is why the `Err(e @ Error::Schema(_))` arm in
    // `enhanced_statistics_parser/mod.rs` exists, and why the one in
    // `storage/sstable/statistics_reader.rs` is not dead code.
    assert!(
        matches!(err, Error::Schema(_)),
        "a semantic refusal must keep the `Schema` kind rather than being \
         relabelled as unreadable/corrupt data: {err:?}"
    );
    let msg = err.to_string();
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
            "the user-visible error must name `{expected}`; got: {msg}"
        );
    }
    // And it must NOT read like a parse failure of unreadable bytes.
    assert!(
        !msg.contains("code: Verify"),
        "the refusal must not surface as a bare nom error: {msg}"
    );
}

// ── The replacement for #4104's two marker-search cases ──────────────────────

/// REPLACES `truncated_header_still_falls_back_to_marker_search` and
/// `a_frozen_scalar_header_is_refused_on_the_no_toc_marker_search_path_too`, both
/// of which asserted properties OF the marker search and died with it.
///
/// #4104 could only ask "does a refusal on the no-TOC path fail closed?" because
/// that path had a decoder — the marker search — that scanned for the literal
/// `org.apache.cassandra.db.marshal` and worked backwards through candidate
/// offsets. #4159 deleted it: the TOC's `HEADER` entry is the ONLY authoritative
/// route to the SerializationHeader, and its absence is a refusal, not an
/// invitation to guess (#28).
///
/// So the property to pin is stronger than the one it replaces, and it is pinned
/// on the input that used to reach the search: a `Statistics.db` with no `HEADER`
/// TOC entry is REFUSED — naming the missing component — rather than
/// marker-searched into a plausible-looking schema. The fixture's schema section
/// is deliberately WELL-FORMED and legal, so the refusal can only be attributable
/// to the missing TOC entry; the marker search would have found and accepted it.
#[test]
fn a_no_toc_header_entry_is_refused_rather_than_marker_searched() {
    let legal_but_unanchored = statistics_db_without_toc_header(&legal_header());

    // Precondition: those very bytes ARE a decodable header — so an `Err` below
    // cannot be explained by the schema section being unreadable. This is what
    // makes the case a replacement for the marker-search tests rather than a
    // weaker "no TOC fails" tautology: the marker search would have succeeded here.
    let (_, _, cols) = parse_serialization_header_schema(&legal_header())
        .unwrap_or_else(|e| panic!("fixture precondition: the schema section decodes: {e}"));
    assert_eq!(cols.len(), 1, "fixture precondition: one regular column");

    let err = public_entry_point(&legal_but_unanchored)
        .err()
        .unwrap_or_else(|| {
            panic!(
                "FAIL-OPEN: a Statistics.db declaring no SERIALIZATION_HEADER in its TOC was \
                 ACCEPTED — the only authoritative route to the header is absent, so this must \
                 refuse rather than locate the header some other way"
            )
        });
    let msg = err.to_string();
    assert!(
        msg.contains("SERIALIZATION_HEADER") && msg.contains("TOC"),
        "the refusal must name the component that is not locatable: {msg}"
    );
    // A missing component is a defect of the FILE, not an illegal declaration, so
    // it keeps the structural kind — the semantic kind is reserved for #4104's
    // "Cassandra cannot have written this" refusals.
    assert!(
        matches!(err, Error::Corruption(_)),
        "an absent TOC entry is a structural refusal: {err:?}"
    );
}

/// The other side of the replacement: a frozen-scalar header that is not
/// TOC-anchored is refused too — and for the ANCHORING reason, since there is no
/// longer any decoder that would reach its column types at all. #4104's version of
/// this case asserted the refusal came from the type; that answer is no longer
/// reachable, and asserting it would misdescribe the code.
#[test]
fn a_no_toc_frozen_scalar_header_is_refused_for_the_anchoring_reason() {
    let bytes = statistics_db_without_toc_header(&frozen_scalar_header());
    let err = public_entry_point(&bytes)
        .err()
        .unwrap_or_else(|| panic!("a Statistics.db with no HEADER TOC entry must be refused"));
    assert!(
        err.to_string().contains("SERIALIZATION_HEADER"),
        "the refusal must name the missing component: {err}"
    );
}

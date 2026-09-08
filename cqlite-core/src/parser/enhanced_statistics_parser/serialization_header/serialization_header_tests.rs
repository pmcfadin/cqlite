//! Unit tests for [`super`] — the marker-search SerializationHeader dispatcher,
//! its backtracking regular-column scanner and its ASCII fallback.
//!
//! Extracted from `serialization_header/mod.rs` under the campsite rule
//! (#1116/#1135): that file was already ~1060 lines, well over the 800-line
//! source target, so #4104's fail-closed change to the marker search could not
//! land there without tripping the `file-size` ratchet.

use super::*;

#[test]
fn test_serialization_header_with_no_clustering_keys() {
    // Test SerializationHeader with partition key and regular columns, no clustering keys
    // Format: [VInt partition_type_len] [0x00 0x00] [partition_type] [clustering_count=0] [0x00 0x00 column_count] [columns...]

    let mut test_data = vec![];

    // Partition key type: 41 bytes "(org.apache.cassandra.db.marshal.UUIDType"
    let partition_type = b"(org.apache.cassandra.db.marshal.UUIDType";
    test_data.extend_from_slice(&[0x00, 0x00]); // Marker
    test_data.push(partition_type.len() as u8);
    test_data.extend_from_slice(partition_type);

    // Clustering key count = 0
    test_data.push(0x00);

    // Regular columns section: separator (0x00) + count
    test_data.push(0x00); // section separator
    test_data.push(0x02); // column count

    // Column 1: "id" (UUID)
    test_data.push(0x02); // name length = 2
    test_data.extend_from_slice(b"id");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UUIDType");

    // Column 2: "name" (UTF8/text)
    test_data.push(0x04); // name length = 4
    test_data.extend_from_slice(b"name");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Add some garbage data before the SerializationHeader
    let mut full_data = vec![0xFF; 100];
    full_data.extend_from_slice(&test_data);

    let result = parse_serialization_header(&full_data);
    assert!(
        result.is_ok(),
        "Failed to parse SerializationHeader: {:?}",
        result.as_ref().err()
    );

    let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

    // Verify partition key
    assert_eq!(partition_types.len(), 1, "Expected 1 partition key");
    assert!(partition_types[0].contains("UUIDType"));

    // Verify clustering keys (should be none)
    assert_eq!(clustering_types.len(), 0, "Expected 0 clustering keys");

    // Verify regular columns
    assert_eq!(columns.len(), 2, "Expected 2 columns");
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[0].column_type, "uuid");
    assert_eq!(columns[1].name, "name");
    assert_eq!(columns[1].column_type, "text");
}

#[test]
fn test_serialization_header_with_clustering_keys() {
    // Test SerializationHeader with partition key, 2 clustering keys, and regular columns

    let mut test_data = vec![];

    // Partition key type: 41 bytes
    let partition_type = b"(org.apache.cassandra.db.marshal.UUIDType";
    test_data.extend_from_slice(&[0x00, 0x00]); // Marker
    test_data.push(partition_type.len() as u8);
    test_data.extend_from_slice(partition_type);

    // Clustering key count = 2
    test_data.push(0x02);

    // Clustering key 1: ReversedType(TimestampType)
    let ck1 =
        b"[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)";
    test_data.push(ck1.len() as u8);
    test_data.extend_from_slice(ck1);

    // Clustering key 2: UTF8Type
    let ck2 = b"(org.apache.cassandra.db.marshal.UTF8Type)";
    test_data.push(ck2.len() as u8);
    test_data.extend_from_slice(ck2);

    // Regular columns section
    test_data.push(0x00); // separator
    test_data.push(0x02); // count

    // Column 1: "data" (UTF8)
    test_data.push(0x04); // name length
    test_data.extend_from_slice(b"data");
    test_data.push(0x28); // type length
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Column 2: "value" (Int32)
    test_data.push(0x05); // name length
    test_data.extend_from_slice(b"value");
    test_data.push(0x29); // type length
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

    // Add garbage data before SerializationHeader
    let mut full_data = vec![0xFF; 100];
    full_data.extend_from_slice(&test_data);

    let result = parse_serialization_header(&full_data);
    assert!(
        result.is_ok(),
        "Failed to parse SerializationHeader with clustering keys: {:?}",
        result.err()
    );

    let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

    // Verify partition key
    assert_eq!(partition_types.len(), 1);
    assert!(partition_types[0].contains("UUIDType"));

    // Verify clustering keys
    assert_eq!(clustering_types.len(), 2, "Expected 2 clustering keys");
    assert!(clustering_types[0].contains("ReversedType"));
    assert!(clustering_types[0].contains("TimestampType"));
    assert!(clustering_types[1].contains("UTF8Type"));

    // Verify regular columns
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].name, "data");
    assert_eq!(columns[0].column_type, "text");
    assert_eq!(columns[1].name, "value");
    assert_eq!(columns[1].column_type, "int");
}

#[test]
fn test_serialization_header_with_static_columns() {
    // Test SerializationHeader with static columns (Issue #210)
    // Schema: partition key (uuid), clustering key (timestamp),
    //         static column (text), regular columns (text, int)

    let mut test_data = vec![];

    // Marker
    test_data.extend_from_slice(&[0x00, 0x00]);

    // Partition key type: UUIDType (40 bytes)
    let partition_type = b"org.apache.cassandra.db.marshal.UUIDType";
    test_data.push(partition_type.len() as u8);
    test_data.extend_from_slice(partition_type);

    // Clustering key count = 1
    test_data.push(0x01);

    // Clustering key 1: TimestampType (45 bytes)
    let ck1 = b"org.apache.cassandra.db.marshal.TimestampType";
    test_data.push(ck1.len() as u8);
    test_data.extend_from_slice(ck1);

    // Static column count = 1 (NOT a separator - this is the key fix!)
    test_data.push(0x01);

    // Static column 1: "static_data" (UTF8Type)
    test_data.push(0x0b); // name length = 11
    test_data.extend_from_slice(b"static_data");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Regular column count = 2
    test_data.push(0x02);

    // Regular column 1: "row_data" (UTF8)
    test_data.push(0x08); // name length
    test_data.extend_from_slice(b"row_data");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Regular column 2: "row_value" (Int32)
    test_data.push(0x09); // name length
    test_data.extend_from_slice(b"row_value");
    test_data.push(0x29); // type length = 41
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

    // Add garbage data before SerializationHeader
    let mut full_data = vec![0xFF; 100];
    full_data.extend_from_slice(&test_data);

    let result = parse_serialization_header(&full_data);
    assert!(
        result.is_ok(),
        "Failed to parse SerializationHeader with static columns: {:?}",
        result.err()
    );

    let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

    // Verify partition key
    assert_eq!(partition_types.len(), 1);
    assert!(partition_types[0].contains("UUIDType"));

    // Verify clustering keys
    assert_eq!(clustering_types.len(), 1);
    assert!(clustering_types[0].contains("TimestampType"));

    // Verify columns (static + regular = 3 total)
    assert_eq!(
        columns.len(),
        3,
        "Expected 3 columns (1 static + 2 regular)"
    );

    // Static column should be first and marked as static
    assert_eq!(columns[0].name, "static_data");
    assert_eq!(columns[0].column_type, "text");
    assert!(
        columns[0].is_static,
        "static_data should be marked as static"
    );

    // Regular columns should NOT be static
    assert_eq!(columns[1].name, "row_data");
    assert_eq!(columns[1].column_type, "text");
    assert!(
        !columns[1].is_static,
        "row_data should NOT be marked as static"
    );

    assert_eq!(columns[2].name, "row_value");
    assert_eq!(columns[2].column_type, "int");
    assert!(
        !columns[2].is_static,
        "row_value should NOT be marked as static"
    );
}

#[test]
fn test_partition_key_extraction_via_backtracking() {
    // Test the backtracking logic to extract partition key type before the column marker
    // This simulates the real ttl_test_table case where we have:
    // VInt(40) + "org.apache.cassandra.db.marshal.UUIDType" + 0x00 0x00 + [count]
    // Note: Real files use 2-byte VInt: 0x80 0x28 for length 40

    let mut test_data = vec![];

    // Add some garbage data before the partition key
    test_data.extend_from_slice(&[0xFF; 50]);

    // Partition key type: 40 bytes "org.apache.cassandra.db.marshal.UUIDType"
    test_data.extend_from_slice(&[0x80, 0x28]); // VInt: 40 (2-byte encoding)
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UUIDType");

    // Marker: 0x00 0x00 followed by column count
    // NOTE: In SerializationHeader, partition keys are NOT in the regular columns section
    // Only regular (non-key) columns are listed here
    test_data.push(0x00); // separator
    test_data.push(0x02); // 2 regular columns

    // Regular Column 1: "expiring_value" (Int32)
    test_data.push(0x0E); // name length = 14
    test_data.extend_from_slice(b"expiring_value");
    test_data.push(0x29); // type length = 41
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

    // Regular Column 2: "session_info" (UTF8)
    test_data.push(0x0C); // name length = 12
    test_data.extend_from_slice(b"session_info");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Parse the regular columns section which should extract partition key via backtracking
    let result = parse_regular_columns(&test_data);
    assert!(
        result.is_ok(),
        "Failed to parse columns with backtracking: {:?}",
        result.err()
    );

    let (_remaining, (partition_keys, columns)) = result.unwrap();

    // Verify partition key was extracted
    assert_eq!(
        partition_keys.len(),
        1,
        "Expected 1 partition key via backtracking"
    );
    assert_eq!(
        partition_keys[0],
        "org.apache.cassandra.db.marshal.UUIDType"
    );

    // Verify regular columns
    assert_eq!(columns.len(), 2, "Expected 2 regular columns");
    assert_eq!(columns[0].name, "expiring_value");
    assert_eq!(columns[0].column_type, "int");
    assert!(!columns[0].is_primary_key);
    assert_eq!(columns[1].name, "session_info");
    assert_eq!(columns[1].column_type, "text");
    assert!(!columns[1].is_primary_key);
}

#[test]
fn test_partition_key_extraction_with_longer_type() {
    // Test with a composite partition key type (longer type string)
    let mut test_data = vec![0xFF; 100]; // Garbage prefix

    // CompositeType with multiple components: 75 bytes
    let composite_type =
        "(org.apache.cassandra.db.marshal.CompositeType(UTF8Type,Int32Type,UUIDType)";
    let type_len = composite_type.len() as u8;

    // VInt encode the length (75 = 0x4B, fits in single byte)
    test_data.push(type_len);
    test_data.extend_from_slice(composite_type.as_bytes());

    // Marker + column count
    test_data.push(0x00); // separator
    test_data.push(0x01); // column count

    // Single column: "data" (UTF8)
    test_data.push(0x04);
    test_data.extend_from_slice(b"data");
    test_data.push(0x28);
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    let result = parse_regular_columns(&test_data);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (_remaining, (partition_keys, columns)) = result.unwrap();

    assert_eq!(partition_keys.len(), 1);
    assert_eq!(partition_keys[0], composite_type);

    // Expect 1 regular column
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "data");
    assert!(!columns[0].is_primary_key);
}

#[test]
fn test_backtracking_with_no_partition_key() {
    // Test case where there's no partition key before the marker
    // This should still parse columns successfully but return empty partition key list

    let mut test_data = vec![];

    // Just the marker and columns, no partition key type before
    test_data.push(0x00); // separator
    test_data.push(0x01); // count

    // Column: "name" (UTF8)
    test_data.push(0x04);
    test_data.extend_from_slice(b"name");
    test_data.push(0x28);
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    let result = parse_regular_columns(&test_data);
    assert!(result.is_ok());

    let (_remaining, (partition_keys, columns)) = result.unwrap();

    assert_eq!(partition_keys.len(), 0, "Should have no partition keys");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "name");
}

#[test]
fn test_backtracking_rejects_invalid_types() {
    // Test that backtracking rejects strings that don't match Cassandra type patterns
    let mut test_data = vec![0xFF; 50];

    // Invalid type: doesn't start with '(' and doesn't contain "org.apache.cassandra"
    test_data.push(0x15); // VInt: 21 bytes
    test_data.extend_from_slice(b"InvalidTypeDescriptor");

    // Marker + column count
    test_data.extend_from_slice(&[0x00, 0x00, 0x01]);

    // Column
    test_data.push(0x04);
    test_data.extend_from_slice(b"test");
    test_data.push(0x28);
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    let result = parse_regular_columns(&test_data);
    assert!(result.is_ok());

    let (_remaining, (partition_keys, _columns)) = result.unwrap();

    // Should not extract the invalid type
    assert_eq!(
        partition_keys.len(),
        0,
        "Should reject invalid type pattern"
    );
}

// ── The marker search's candidates are GUESSES (#4104, roborev job 120) ──────

const MARSHAL_PACKAGE: &str = "org.apache.cassandra.db.marshal.";

/// A fully-qualified marshal class spelling — what
/// `SerializationHeader.writeType` (`AbstractType::toString()`) writes for every
/// type field of the header.
fn marshal(simple_name: &str) -> String {
    format!("{MARSHAL_PACKAGE}{simple_name}")
}

/// `FrozenType(Int32Type)`: a spelling no Cassandra writer can emit, because
/// `CQL3Type.Raw::freeze()` throws for every non-collection/tuple/UDT/vector
/// (`cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`).
fn frozen_scalar() -> String {
    format!("{MARSHAL_PACKAGE}FrozenType({})", marshal("Int32Type"))
}

/// Append a `[VInt len][UTF-8 bytes]` field, as SerializationHeader.java does.
fn push_str(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&crate::parser::vint::encode_vuint(value.len() as u64));
    out.extend_from_slice(value.as_bytes());
}

/// A well-formed single-column SerializationHeader: `keyType`, clusteringTypes,
/// staticColumns, regularColumns (guide Ch.8, "SerializationHeader Component").
fn valid_header() -> Vec<u8> {
    let mut out = Vec::new();
    push_str(&mut out, &marshal("UTF8Type"));
    out.extend_from_slice(&[0x00]); // clusteringTypes: none
    out.extend_from_slice(&[0x00]); // staticColumns: none
    out.extend_from_slice(&[0x01]); // regularColumns: one
    push_str(&mut out, "v");
    push_str(&mut out, &marshal("Int32Type"));
    out
}

/// A byte run that is NOT a header but that the marker search enters, decodes
/// through, and refuses — the over-refusal fixture.
///
/// The search anchors on an `org.apache.cassandra.db.marshal` occurrence and then
/// tries the 1..=15 bytes BEFORE it as the key-type length VInt. Here the byte
/// that passes its plausibility test sits three bytes early, so the decoded "key
/// type" is `<two junk bytes>org.apache.cassandra.db.marshal.` — a string that
/// CONTAINS the package and is not a class spelling, which is precisely what
/// `writeType` can never have produced. Everything after it happens to read as
/// `0 clustering / 0 static / 1 regular column` whose type is a frozen scalar, so
/// the candidate reaches the semantic gate and refuses.
///
/// Any cell value, min/max clustering value or index byte quoting a Cassandra type
/// name can produce this shape; nothing about it is a header.
fn false_candidate_ending_in_a_frozen_scalar() -> Vec<u8> {
    let mut out = Vec::new();
    // Read as keyType length: 2 junk bytes + the 32-byte package prefix.
    out.push(0x22);
    out.extend_from_slice(&[0x00, 0x01]);
    out.extend_from_slice(MARSHAL_PACKAGE.as_bytes());
    out.extend_from_slice(&[0x00, 0x00, 0x01]); // "0 clustering, 0 static, 1 regular"
    push_str(&mut out, "v");
    push_str(&mut out, &frozen_scalar());
    out
}

/// A FALSE candidate's semantic refusal must NOT abort the search: the valid
/// header that follows it is found and returned.
///
/// Before the confirm-then-refuse rule, `parse_serialization_header` propagated
/// `Refused` from the first candidate that hit the frozen-scalar gate, whoever it
/// was. This exact buffer therefore came back as
/// `Err(Refused("… FrozenType(org.apache.cassandra.db.marshal.Int32Type) …"))`,
/// i.e. a perfectly good SSTable was unopenable because a byte run earlier in the
/// file quoted a type name. The refusal is authoritative only once the candidate
/// is confirmed to BE a SerializationHeader, which this one is not.
#[test]
fn a_false_candidate_carrying_a_frozen_scalar_run_must_not_refuse_a_valid_header() {
    let mut buffer = false_candidate_ending_in_a_frozen_scalar();
    let header_offset = buffer.len();
    buffer.extend_from_slice(&valid_header());

    let (_, (pk_types, ck_types, columns)) = parse_serialization_header(&buffer)
        .unwrap_or_else(|e| {
            panic!(
                "OVER-REFUSAL: a valid header at offset {header_offset} was rejected because an \
                 EARLIER false candidate quoted a frozen scalar; a marker-search candidate is a \
                 guess, so its refusal is authoritative only once confirmed: {e:?}"
            )
        });

    // The schema returned must be the REAL header's, not the false candidate's:
    // continuing the search must not degrade into accepting the guess either.
    assert_eq!(pk_types, vec![marshal("UTF8Type")], "real header's keyType");
    assert!(ck_types.is_empty(), "real header declares no clustering keys");
    assert_eq!(columns.len(), 1, "real header declares one regular column");
    assert_eq!(columns[0].name, "v");
    assert_eq!(columns[0].column_type, "int");
}

/// Anti-vacuity control for the test above: the fixture really does reach the
/// frozen-scalar gate, and the confirmation really does call it unconfirmed.
///
/// Without this, a fixture the search happened to SKIP would make the test above
/// pass for the wrong reason. There is deliberately no "same run with a legal
/// type" twin instead: the search's ACCEPTANCE rule is a weaker, pre-existing
/// `contains("org.apache.cassandra.db.marshal")` test that admits this
/// junk-prefixed key type, so such a twin would characterise that separate
/// heuristic rather than this fix.
#[test]
fn the_false_candidate_reaches_the_semantic_gate_and_is_unconfirmed() {
    let run = false_candidate_ending_in_a_frozen_scalar();

    // Under `Enforce` the run DOES hit the frozen-scalar gate — which is why,
    // before the confirm-then-refuse rule, it aborted the whole search.
    match parse_serialization_header_sequential(&run, SemanticGate::Enforce) {
        Err(HeaderSchemaError::Refused(_)) => {}
        Err(HeaderSchemaError::Structural(e)) => {
            panic!("fixture never reached the semantic gate (structural): {e:?}")
        }
        Ok(_) => panic!("fixture never reached the semantic gate (it decoded cleanly)"),
    }

    // Under `Survey` it decodes to the end of its own declared column list, and
    // its key type is `<junk>org.apache.cassandra.db.marshal.` — a string that
    // contains the package and is not the class spelling `writeType` produces. So
    // these bytes are not a SerializationHeader and the refusal above is not
    // authoritative.
    let survey = parse_serialization_header_sequential(&run, SemanticGate::Survey);
    let Ok((_, (ref key_types, _, ref columns))) = survey else {
        panic!("the survey decode must run past the deferred refusal to the end")
    };
    assert_eq!(key_types.len(), 1, "one keyType field was decoded");
    assert!(
        !key_types[0].starts_with(MARSHAL_PACKAGE),
        "the fixture's keyType is junk-prefixed: {:?}",
        key_types[0]
    );
    assert_eq!(columns.len(), 1, "the deferred refusal did not cut the decode");
    assert!(
        !candidate_confirmed_as_header(survey),
        "a run whose keyType is not a marshal class spelling must stay UNCONFIRMED"
    );
}

/// A GENUINE header that the search CONFIRMS must still fail closed — pinned on
/// a fixture where the confirmed candidate is the ONLY route to the refusal.
///
/// Why the fixture looks like this: the existing higher-level guard
/// (`encoding_stats_tests::a_frozen_scalar_header_is_refused_on_the_no_toc_marker_search_path_too`)
/// does not pin THIS branch, because its bytes carry a `0x00` that the
/// last-resort `parse_regular_columns` scanner anchors on and refuses at as well
/// — so dropping the confirmed refusal here leaves that test green. This header
/// declares a clustering key, a static column and a regular column, so every
/// count and length is non-zero and it contains no `0x00` at all: the scanner
/// finds no anchor, the ASCII fallback finds no `CompositeType(`, and the
/// dispatcher's own empty-schema `Ok` is what a fall-through would produce. That
/// `Ok` is job 119's fail-open, and this test is what keeps it closed.
#[test]
fn a_confirmed_frozen_scalar_header_still_fails_closed_on_the_marker_search() {
    let refusal = match parse_serialization_header(&zero_free_header(&frozen_scalar())) {
        Err(HeaderSchemaError::Refused(refusal)) => refusal,
        Err(HeaderSchemaError::Structural(e)) => panic!(
            "a frozen-scalar header must be a SEMANTIC refusal, not a structural \
             failure a caller may retry heuristically: {e:?}"
        ),
        Ok((_, (pk_types, ck_types, columns))) => panic!(
            "FAIL-OPEN: a CONFIRMED frozen<scalar> header was accepted by the marker \
             search — pk={pk_types:?} ck={ck_types:?} {} column(s). A confirmed \
             candidate's refusal must not fall through to another candidate or to \
             the no-header success",
            columns.len()
        ),
    };
    assert!(
        refusal
            .to_string()
            .contains("FrozenType(org.apache.cassandra.db.marshal.Int32Type)"),
        "the refusal must name the refused type: {refusal}"
    );
}

/// The control for the fixture above: with a LEGAL column type the same bytes are
/// a header the marker search parses happily, so the refusal is attributable to
/// the type and not to the fixture's framing.
#[test]
fn the_zero_free_header_fixture_decodes_when_its_column_type_is_legal() {
    let (_, (pk_types, ck_types, columns)) =
        parse_serialization_header(&zero_free_header(&marshal("Int32Type")))
            .unwrap_or_else(|e| panic!("the control fixture must decode: {e:?}"));
    assert_eq!(pk_types, vec![marshal("UTF8Type")], "keyType");
    assert_eq!(ck_types, vec![marshal("TimestampType")], "clusteringTypes");
    assert_eq!(columns.len(), 2, "one static plus one regular column");
    assert_eq!(columns[0].name, "s");
    assert!(columns[0].is_static);
    assert_eq!(columns[1].name, "v");
    assert_eq!(columns[1].column_type, "int");
}

/// A well-formed SerializationHeader containing no `0x00` byte: one clustering
/// type, one static column and one regular column of `column_type`, so every
/// count and every length VInt is non-zero (guide Ch.8 field order).
fn zero_free_header(column_type: &str) -> Vec<u8> {
    let mut out = Vec::new();
    push_str(&mut out, &marshal("UTF8Type"));
    out.extend_from_slice(&[0x01]); // clusteringTypes: one
    push_str(&mut out, &marshal("TimestampType"));
    out.extend_from_slice(&[0x01]); // staticColumns: one
    push_str(&mut out, "s");
    push_str(&mut out, &marshal("UTF8Type"));
    out.extend_from_slice(&[0x01]); // regularColumns: one
    push_str(&mut out, "v");
    push_str(&mut out, column_type);
    assert!(
        !out.contains(&0x00),
        "the fixture's premise is that it carries no 0x00 anchor"
    );
    out
}

/// The same confirm-then-refuse rule on the search's OTHER candidate flavour —
/// the legacy `0x00 0x00` marker, which decodes counts and name/type lengths as
/// single bytes rather than VInts.
///
/// Reaching it needs a key-type length byte the SEQUENTIAL flavour disagrees
/// about, since an equivalent sequential candidate sits two bytes later and is
/// therefore tried first: `0x80` is a 128-byte key type as a single byte (a
/// `CompositeType(..)` spelling reaches that size easily) and a different, small
/// value as a VInt, so the sequential candidate fails structurally and the legacy
/// one is what reaches the frozen-scalar gate. Its key type is junk-prefixed, so
/// it is unconfirmed and the valid header that follows must still be found.
#[test]
fn a_false_legacy_marker_candidate_must_not_refuse_a_valid_header() {
    let mut buffer = vec![0x00, 0x00, 0x80, 0x01];
    buffer.extend_from_slice(MARSHAL_PACKAGE.as_bytes());
    // Pad the declared 128-byte key type out to its full length.
    buffer.resize(3 + 128, b'x');
    buffer.extend_from_slice(&[0x00, 0x00, 0x01]); // 0 clustering, 0 static, 1 regular
    push_str(&mut buffer, "v");
    push_str(&mut buffer, &frozen_scalar());
    let header_offset = buffer.len();
    buffer.extend_from_slice(&valid_header());

    let (_, (pk_types, _, columns)) = parse_serialization_header(&buffer).unwrap_or_else(|e| {
        panic!(
            "OVER-REFUSAL on the legacy-marker flavour: a valid header at offset \
             {header_offset} was rejected because an earlier false candidate quoted a \
             frozen scalar: {e:?}"
        )
    });
    assert_eq!(pk_types, vec![marshal("UTF8Type")], "real header's keyType");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "v");
    assert_eq!(columns[0].column_type, "int");
}


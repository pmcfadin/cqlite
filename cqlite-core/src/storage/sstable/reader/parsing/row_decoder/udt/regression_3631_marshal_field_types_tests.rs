//! Issue #3631 / roborev job 68 finding 1 — a marshal-form UDT field whose type is a
//! valid Cassandra native type that `parse_cassandra_type_with_depth` did not map.
//!
//! ## The defect these cases pin
//!
//! Every unmapped marshal name became [`CqlType::Custom`], and the `CqlType` decoder
//! routes `Custom` to the nested-UDT decoder — so a UDT with a `duration`,
//! `smallint`, `tinyint`, `counter` or `tuple<…>` field failed with *"nested
//! user-defined type … is declared but its field list is not available"* instead of
//! decoding. Before #3631 those fields fell to the blob fallback (silently wrong);
//! after it they hard-ERRORED. A fix that closes a silent-blob gap must not open a
//! hard-failure gap, so each case here drives the WHOLE path a real header takes:
//! marshal type STRING -> [`UdtTypeDef`] -> field decode.
//!
//! ## Oracles — pinned Cassandra source, never CQLite's own output (#3041/#3042)
//!
//! * The name -> type binding is `cassandra-5.0.8:src/java/org/apache/cassandra/
//!   cql3/CQL3Type.java`'s `Native` enum, plus the three `asCQL3Type()` overrides
//!   quoted in `type_string.rs`'s header.
//! * `duration` bytes: `serializers/DurationSerializer.java` `serialize` writes
//!   `output.writeVInt(months); writeVInt(days); writeVInt(nanoseconds)`, and
//!   `utils/vint/VIntCoding.java:447` `writeVInt(long) = writeUnsignedVInt(
//!   encodeZigZag64(value))` — a one-byte encoding for a small value. So
//!   `1mo 2d 3ns` is `02 04 06` (zigzag of 1, 2, 3).
//! * `TupleType` component framing: `db/marshal/TupleType.java` `buildValue` —
//!   `[i32 size][bytes]` per component; its marshal STRING form is
//!   `getClass().getName() + stringifyTypeParameters(types, true)` (line 557), i.e.
//!   `…TupleType(<t1>,<t2>)`.
//! * `ReversedType`: `db/marshal/ReversedType.java:138,144` — `asCQL3Type()` AND
//!   `getSerializer()` both delegate to `baseType`, so reversal changes the
//!   comparison order and never the value layout.

use super::*;

const PKG: &str = "org.apache.cassandra.db.marshal.";

/// `UserType(keyspace,hex(name),hex(field):type,…)` — the shape a real
/// `SerializationHeader` carries (`UserType.toString()` at the pinned tag).
fn marshal_udt_with_field(field_marshal_type: &str) -> String {
    format!(
        "{PKG}UserType(ks,{},{}:{})",
        hex::encode("f_type"),
        hex::encode("f"),
        field_marshal_type
    )
}

/// The `CqlType` a marshal-form UDT's single field parses to — the exact route a
/// header takes (`parse_udt_type_definition` -> `parse_cassandra_type_with_depth`).
fn field_type_of(field_marshal_type: &str) -> CqlType {
    let def = V5CompressedLegacyParser::parse_udt_type_definition(&marshal_udt_with_field(
        field_marshal_type,
    ))
    .expect("a UserType(...) marshal string with one field must parse");
    assert_eq!(def.fields.len(), 1, "one declared field");
    def.fields[0].field_type.clone()
}

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("ks".to_string(), "t".to_string(), 0, 0, None)
}

/// Decode `bytes` as the field type the marshal string declares.
fn decode_field(field_marshal_type: &str, bytes: &[u8]) -> Result<Value> {
    parser().parse_simple_udt_field_value_at(bytes, &field_type_of(field_marshal_type), 0)
}

// ════════════════════════════════════════════════════════════════════════════
// The five types roborev job 68 finding 1 names.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn duration_marshal_udt_field_decodes() {
    assert_eq!(field_type_of("DurationType"), CqlType::Duration);
    assert_eq!(
        decode_field("DurationType", &[0x02, 0x04, 0x06]).expect("a duration field must decode"),
        Value::Duration {
            months: 1,
            days: 2,
            nanos: 3
        },
        "DurationSerializer: three zigzag vints (months, days, nanos)"
    );
}

#[test]
fn smallint_marshal_udt_field_decodes() {
    assert_eq!(field_type_of("ShortType"), CqlType::SmallInt);
    assert_eq!(
        decode_field("ShortType", &(-5i16).to_be_bytes()).expect("a smallint field must decode"),
        Value::SmallInt(-5),
        "CQL3Type.Native.SMALLINT is ShortType"
    );
}

#[test]
fn tinyint_marshal_udt_field_decodes() {
    assert_eq!(field_type_of("ByteType"), CqlType::TinyInt);
    assert_eq!(
        decode_field("ByteType", &7i8.to_be_bytes()).expect("a tinyint field must decode"),
        Value::TinyInt(7),
        "CQL3Type.Native.TINYINT is ByteType"
    );
}

#[test]
fn counter_marshal_udt_field_decodes() {
    assert_eq!(field_type_of("CounterColumnType"), CqlType::Counter);
    assert_eq!(
        decode_field("CounterColumnType", &42i64.to_be_bytes())
            .expect("a counter field must decode"),
        Value::BigInt(42),
        "CQL3Type.Native.COUNTER is CounterColumnType; the value is an 8-byte long"
    );
}

#[test]
fn tuple_marshal_udt_field_decodes() {
    let tuple = format!("{PKG}TupleType({PKG}UTF8Type,{PKG}Int32Type)");
    assert_eq!(
        field_type_of(&tuple),
        CqlType::Tuple(vec![CqlType::Text, CqlType::Int]),
        "TupleType is STRUCTURAL — a parenthesised parameter list, not a name match"
    );
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5i32.to_be_bytes());
    bytes.extend_from_slice(b"alpha");
    bytes.extend_from_slice(&4i32.to_be_bytes());
    bytes.extend_from_slice(&30i32.to_be_bytes());
    assert_eq!(
        decode_field(&tuple, &bytes).expect("a tuple<text,int> field must decode"),
        Value::Tuple(vec![Value::text("alpha"), Value::Integer(30)]),
        "TupleType.buildValue: [i32 size][bytes] per component"
    );
}

/// A `TupleType` nested inside a `ListType` recurses through the SAME depth counter
/// (`depth + 1`, never a reset), and still decodes.
#[test]
fn tuple_inside_a_collection_marshal_udt_field_decodes() {
    let ty = format!("{PKG}ListType({PKG}TupleType({PKG}Int32Type,{PKG}Int32Type))");
    assert_eq!(
        field_type_of(&ty),
        CqlType::List(Box::new(CqlType::Tuple(vec![CqlType::Int, CqlType::Int])))
    );
}

// ════════════════════════════════════════════════════════════════════════════
// The rest of the marshal surface: the holes found while enumerating CQL3Type.Native.
// ════════════════════════════════════════════════════════════════════════════

/// `DateType.asCQL3Type()` is `TIMESTAMP` (the legacy 8-byte millis type), NOT CQL
/// `date`. A `ends_with("DateType")` arm also matches `SimpleDateType`, which is how
/// the two got the same mapping.
#[test]
fn legacy_datetype_marshal_udt_field_is_a_timestamp_not_a_date() {
    assert_eq!(field_type_of("DateType"), CqlType::Timestamp);
    assert_eq!(
        decode_field("DateType", &1_700_000_000_000i64.to_be_bytes())
            .expect("a legacy DateType field is 8 bytes of millis"),
        Value::Timestamp(1_700_000_000_000),
    );
}

/// `SimpleDateType` IS CQL `date` — a 4-byte unsigned days-since-epoch value.
#[test]
fn simpledatetype_marshal_udt_field_is_a_date() {
    assert_eq!(field_type_of("SimpleDateType"), CqlType::Date);
    assert_eq!(
        decode_field("SimpleDateType", &(1u32 << 31).to_be_bytes()).expect("a date field decodes"),
        Value::Date(0),
    );
}

#[test]
fn varchartype_marshal_udt_field_is_text() {
    assert_eq!(field_type_of("VarcharType"), CqlType::Text);
    assert_eq!(
        decode_field("VarcharType", b"hi").expect("a VarcharType field must decode"),
        Value::text("hi"),
        "TypeParser resolves the VarcharType alias to UTF8Type"
    );
}

#[test]
fn timeuuidtype_marshal_udt_field_is_a_timeuuid_not_a_plain_uuid() {
    assert_eq!(
        field_type_of("TimeUUIDType"),
        CqlType::TimeUuid,
        "CQL3Type.Native.TIMEUUID is TimeUUIDType; the previous suffix arm collapsed \
         it onto `uuid`"
    );
    assert_eq!(field_type_of("UUIDType"), CqlType::Uuid);
}

/// `LexicalUUIDType` has no `asCQL3Type()` override, but its `Serializer extends
/// UUIDSerializer` and `valueLengthIfFixed() == 16`: the VALUE is a UUID's.
#[test]
fn lexicaluuidtype_marshal_udt_field_is_a_uuid() {
    assert_eq!(field_type_of("LexicalUUIDType"), CqlType::Uuid);
}

#[test]
fn reversedtype_marshal_udt_field_takes_its_base_types_layout() {
    assert_eq!(
        field_type_of(&format!("{PKG}ReversedType({PKG}Int32Type)")),
        CqlType::Int,
        "ReversedType.asCQL3Type()/getSerializer() both delegate to baseType"
    );
}

/// A third-party marshal class is NOT a native type and NOT a UDT name — exact
/// simple-name matching keeps it out of `blob`, which an `ends_with("BytesType")`
/// suffix match would have handed it.
///
/// Since roborev job 76 it is REFUSED rather than carried as `CqlType::Custom`:
/// the type-string parser is the only place that still knows a dotted name here is
/// a JAVA CLASS, so it is where the package rule can be applied without guessing
/// (downstream, a `Custom` payload cannot be told from a keyspace-qualified UDT
/// name). The package rule's own cases live in
/// `regression_3631_marshal_package_rule_tests.rs`.
#[test]
fn a_third_party_marshal_class_is_not_silently_a_blob() {
    let err = V5CompressedLegacyParser::parse_udt_type_definition(&marshal_udt_with_field(
        "com.acme.MyBytesType",
    ))
    .expect_err("a foreign marshal package must be refused, never decoded as blob");
    let msg = err.to_string();
    assert!(
        msg.contains("com.acme.MyBytesType") && msg.contains("com.acme."),
        "the refusal must name the type and the package it was rejected on: {msg}"
    );
    assert!(
        !msg.contains("nested user-defined type"),
        "not a UDT: {msg}"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// A genuinely unknown marshal name: an explicit, ACCURATE refusal.
// ════════════════════════════════════════════════════════════════════════════

/// `EmptyType` is a real Cassandra type with no `CqlType` to express it. It must be
/// refused BY NAME — never a silent blob (#28), and never the misleading
/// "nested user-defined type" message, which said the field list was missing when
/// the type was not a UDT at all.
#[test]
fn an_unmappable_marshal_type_is_refused_as_an_undecodable_type_not_as_a_missing_udt() {
    let err =
        decode_field(&format!("{PKG}EmptyType"), &[]).expect_err("EmptyType has no decoding rule");
    let msg = err.to_string();
    assert!(
        msg.contains("EmptyType") && msg.contains("no decoding rule"),
        "the refusal must NAME the type it cannot decode: {msg}"
    );
    assert!(
        !msg.contains("nested user-defined type"),
        "EmptyType is not a UDT — the old message misattributed the cause: {msg}"
    );
}

/// `VectorType(FloatType , 3)` is Cassandra 5.0's `vector<float, 3>`, and since
/// issue #4114 it DECODES rather than being refused.
///
/// This case previously required the refusal, on the grounds that `CqlType` had no
/// variant for it. That is no longer true — `CqlType::Vector(Box<CqlType>, usize)`
/// exists, `parse_cassandra_type_with_depth` parses the constructor structurally,
/// and the value is `4 * n` raw big-endian binary32 bytes with NO prefix of any
/// kind (`cassandra-5.0.8` VectorType.java:94-101 picks width AND serializer from
/// the element type; FixedLengthSerializer.split at :445-460 reads no prefix;
/// FloatType.valueLengthIfFixed() == 4 at FloatType.java:148-152). So the old
/// expectation encoded a MISSING FEATURE as a requirement, and keeping it would
/// have pinned the defect in place.
///
/// The spacing is Cassandra's own: `TypeParser.stringifyVectorParameters`
/// (TypeParser.java:239-242) writes `" , "`.
#[test]
fn a_marshal_vector_field_decodes_from_its_declared_dimension() {
    let ty = format!("{PKG}VectorType({PKG}FloatType , 3)");
    assert_eq!(
        field_type_of(&ty),
        CqlType::Vector(Box::new(CqlType::Float), 3),
        "the dimension must survive the type parse — it is the only thing that \
         makes a prefix-free fixed-width value parseable"
    );
    // 3f800000 40200000 c0700000 == [1.0, 2.5, -3.75], the byte sequence verified
    // against the committed Cassandra-written fixture in
    // `.drive-issue-4114/format-authority.md`.
    let bytes = [
        0x3f, 0x80, 0x00, 0x00, 0x40, 0x20, 0x00, 0x00, 0xc0, 0x70, 0x00, 0x00,
    ];
    assert_eq!(
        decode_field(&ty, &bytes).expect("a 12-byte vector<float, 3> field must decode"),
        Value::List(vec![
            Value::Float32(1.0),
            Value::Float32(2.5),
            Value::Float32(-3.75)
        ]),
    );
    // The refusal that DID survive: 12 bytes is the ONLY legal width, in both
    // directions (`checkConsumedFully`, VectorType.java:358-363).
    for wrong in [&bytes[..11], &bytes[..4]] {
        assert!(
            decode_field(&ty, wrong).is_err(),
            "a {}-byte value is not a vector<float, 3>",
            wrong.len()
        );
    }
}

/// The refusal #4114 KEEPS: a non-`float` element type. The shape generalises to
/// any fixed-width element, but "the shape generalises" is not evidence the decode
/// is right and there is no Cassandra-written fixture for any other element, so it
/// is refused BY NAME rather than decoded (AC4).
#[test]
fn a_marshal_vector_of_a_non_float_element_is_refused_by_name() {
    let ty = format!("{PKG}VectorType({PKG}DoubleType , 2)");
    assert_eq!(
        field_type_of(&ty),
        CqlType::Vector(Box::new(CqlType::Double), 2),
        "the TYPE parses; only the DECODE is unimplemented"
    );
    let err = decode_field(&ty, &[0u8; 16]).expect_err("only vector<float, n> decodes");
    let msg = err.to_string();
    assert!(
        msg.contains("Double") && msg.contains("not implemented"),
        "the refusal must name the element type it cannot decode: {msg}"
    );
    assert!(
        !msg.contains("nested user-defined type"),
        "a vector is not a UDT: {msg}"
    );
}

/// A dimension Cassandra itself rejects (`VectorType.java:89-90`, `dimension <= 0`)
/// must be refused at the TYPE, by name — never read as an empty vector, which
/// Cassandra does not have (`:409-414`).
#[test]
fn a_zero_dimension_marshal_vector_is_refused_at_the_type() {
    let err = V5CompressedLegacyParser::parse_udt_type_definition(&marshal_udt_with_field(
        &format!("{PKG}VectorType({PKG}FloatType , 0)"),
    ))
    .expect_err("dimension 0 is not a legal vector");
    let msg = err.to_string();
    assert!(msg.contains("dimension 0"), "{msg}");
}

/// The SAME type spelled BARE. `TypeParser.getAbstractType` (TypeParser.java:450)
/// resolves an unqualified name against the marshal package, so CQLite records the
/// resolved class name and the refusal is identical — a bare spelling must not be
/// mistaken for a UDT name.
#[test]
fn a_bare_unmappable_marshal_name_is_resolved_against_the_marshal_package() {
    assert_eq!(
        field_type_of("EmptyType"),
        CqlType::Custom(format!("{PKG}EmptyType")),
    );
    let err = decode_field("EmptyType", &[]).expect_err("still no decoding rule");
    assert!(
        err.to_string().contains("no decoding rule")
            && !err.to_string().contains("nested user-defined type"),
        "{err}"
    );
}

/// The counterpart that must KEEP the UDT message: a real, unresolvable UDT
/// reference. This is what makes the case above a discrimination and not a
/// blanket rewording.
#[test]
fn an_unresolvable_udt_name_still_reports_a_missing_field_list() {
    let err = parser()
        .parse_simple_udt_field_value_at(&[0, 0, 0, 0], &CqlType::Custom("address".to_string()), 0)
        .expect_err("no registry, no inline fields");
    assert!(
        err.to_string().contains("nested user-defined type"),
        "a UDT NAME keeps the UDT diagnostic: {err}"
    );
}

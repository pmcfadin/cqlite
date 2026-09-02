//! Issue #3631 / roborev job 76 — **the PACKAGE of a marshal class name is part of
//! its identity**, so a third-party class that merely shares a simple name must be
//! refused rather than decoded as the Cassandra native type it resembles.
//!
//! ## The defect these cases pin
//!
//! `native_marshal_to_cql_type` took the text after the LAST `.` and matched it
//! against `CQL3Type.Native`, ignoring the package — so `com.acme.Int32Type`
//! decoded as CQL `int` and `notorg.apache.cassandra.db.marshal.LongType` as
//! `bigint`. CQLite knows nothing about those classes' byte layouts; picking one
//! from a name RESEMBLANCE is exactly the guessing #28 forbids, and it can produce
//! wrong values or spurious width errors.
//!
//! ## Oracle — the pinned tag, never CQLite's own tables (#3041)
//!
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TypeParser.java:450`
//! (identical at line 466) is the single place Cassandra turns a name inside a
//! marshal type string into a class:
//!
//! ```java
//! String className = compareWith.contains(".") ? compareWith
//!                  : "org.apache.cassandra.db.marshal." + compareWith;
//! ```
//!
//! So there are EXACTLY TWO legal spellings of a marshal name, and both must decode
//! identically (the positive controls below). Every `AbstractType` this reader maps
//! declares `package org.apache.cassandra.db.marshal;` at that tag — verified over
//! every `.java` under `src/java/org/apache/cassandra/db/marshal/` — so no third
//! package is legitimate.
//!
//! ## Why the near-miss packages are pinned explicitly
//!
//! A prefix/suffix/substring test is the obvious wrong implementation, and each
//! spelling below defeats exactly one of them: `…db.marshalX.` a `starts_with`,
//! `notorg.apache…` an `ends_with`, and `my.org.apache…` a `contains` (which is
//! what the `UserType(` marker locator used).

use super::*;

const PKG: &str = "org.apache.cassandra.db.marshal.";

/// `UserType(keyspace,hex(name),hex(field):type)` — the shape a real
/// `SerializationHeader` carries.
fn marshal_udt_with_field(field_marshal_type: &str) -> String {
    format!(
        "{PKG}UserType(ks,{},{}:{})",
        hex::encode("f_type"),
        hex::encode("f"),
        field_marshal_type
    )
}

/// The whole route a header takes: marshal type STRING -> `UdtTypeDef` -> field
/// `CqlType`.
fn field_type_of(field_marshal_type: &str) -> Result<CqlType> {
    let def = V5CompressedLegacyParser::parse_udt_type_definition(&marshal_udt_with_field(
        field_marshal_type,
    ))?;
    assert_eq!(def.fields.len(), 1, "one declared field");
    Ok(def.fields[0].field_type.clone())
}

/// Assert that `field_marshal_type` is REFUSED, and that the refusal is the
/// accurate one: it names the type, names the package it was rejected on, and is
/// NOT the misattributed "nested user-defined type" message (roborev job 68
/// finding 1 fixed that misattribution; it must not regress).
fn assert_refused_as_foreign_package(field_marshal_type: &str, foreign_package: &str) {
    let err = field_type_of(field_marshal_type)
        .expect_err("a marshal name outside Cassandra's marshal package must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains(field_marshal_type),
        "the refusal must NAME the declared type: {msg}"
    );
    assert!(
        msg.contains(foreign_package),
        "the refusal must NAME the package it was rejected on ('{foreign_package}'): {msg}"
    );
    assert!(
        msg.contains(PKG),
        "the refusal must NAME the package it EXPECTED: {msg}"
    );
    assert!(
        !msg.contains("nested user-defined type"),
        "a foreign marshal class is not a UDT — that message misattributes the cause: {msg}"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// The finding: a third-party class with the SAME SIMPLE NAME as a native type.
// ════════════════════════════════════════════════════════════════════════════

/// The exact case roborev job 76 names. Pre-fix this returned `CqlType::Int`.
#[test]
fn a_foreign_package_with_a_native_simple_name_is_refused_not_decoded_as_int() {
    assert_refused_as_foreign_package("com.acme.Int32Type", "com.acme.");
}

/// Not one arm of the table — every one of them. A single arm left
/// package-insensitive would be the same defect for one type.
#[test]
fn every_native_simple_name_is_refused_under_a_foreign_package() {
    // The full `CQL3Type.Native` set the table maps, plus the four names mapped
    // from their own `asCQL3Type()`/serializer authority.
    for simple in [
        "AsciiType",
        "LongType",
        "BytesType",
        "BooleanType",
        "CounterColumnType",
        "SimpleDateType",
        "DecimalType",
        "DoubleType",
        "DurationType",
        "FloatType",
        "InetAddressType",
        "Int32Type",
        "ShortType",
        "UTF8Type",
        "TimeType",
        "TimestampType",
        "TimeUUIDType",
        "ByteType",
        "UUIDType",
        "IntegerType",
        "DateType",
        "VarcharType",
        "LexicalUUIDType",
        "LegacyTimeUUIDType",
    ] {
        assert_refused_as_foreign_package(&format!("com.acme.{simple}"), "com.acme.");
    }
}

// ════════════════════════════════════════════════════════════════════════════
// STRUCTURAL forms take the same rule.
// ════════════════════════════════════════════════════════════════════════════

/// `com.acme.TupleType(...)` must not be parsed as a Cassandra tuple. The
/// structural arms require the QUALIFIED marshal spelling, so this matches none of
/// them and is refused by the package rule at the fall-through — with the package
/// named, rather than as a generic undecodable `Custom`.
#[test]
fn a_foreign_package_structural_form_is_refused() {
    for class in [
        "TupleType",
        "ListType",
        "SetType",
        "MapType",
        "FrozenType",
        "ReversedType",
        "UserType",
    ] {
        assert_refused_as_foreign_package(
            &format!("com.acme.{class}({PKG}Int32Type)"),
            "com.acme.",
        );
    }
}

/// The package rule is applied to the class-name HEAD, never to the whole string:
/// the last `.` of `com.acme.VectorType(org.apache.cassandra.db.marshal.Int32Type)`
/// is inside the ARGUMENTS, so a rule applied to the whole string would read this
/// foreign class as living in the marshal package.
#[test]
fn a_marshal_package_inside_the_arguments_does_not_qualify_a_foreign_head() {
    assert_refused_as_foreign_package(&format!("com.acme.VectorType({PKG}Int32Type)"), "com.acme.");
}

// ════════════════════════════════════════════════════════════════════════════
// NEAR-MISS packages: one per wrong implementation.
// ════════════════════════════════════════════════════════════════════════════

/// Defeats a `starts_with(PKG_WITHOUT_DOT)` — and note the trailing `.` is what
/// makes the equality reject it, since `…db.marshal` IS a prefix of `…db.marshalX`.
#[test]
fn a_package_that_extends_the_marshal_package_is_refused() {
    assert_refused_as_foreign_package(
        "org.apache.cassandra.db.marshalX.Int32Type",
        "org.apache.cassandra.db.marshalX.",
    );
    assert_refused_as_foreign_package(
        "org.apache.cassandra.db.marshal.sub.Int32Type",
        "org.apache.cassandra.db.marshal.sub.",
    );
}

/// Defeats an `ends_with(PKG)`.
#[test]
fn a_package_that_ends_with_the_marshal_package_is_refused() {
    assert_refused_as_foreign_package(
        "notorg.apache.cassandra.db.marshal.Int32Type",
        "notorg.apache.cassandra.db.marshal.",
    );
}

/// Defeats a `contains(PKG)` — the shape the `UserType(` marker locator used, so
/// this is asserted on the MARKER path (a top-level type string) as well as on a
/// field type.
#[test]
fn a_package_that_contains_the_marshal_package_is_refused() {
    assert_refused_as_foreign_package(
        "my.org.apache.cassandra.db.marshal.Int32Type",
        "my.org.apache.cassandra.db.marshal.",
    );
}

/// The marker locator itself: `my.org.apache.cassandra.db.marshal.UserType(...)` is
/// a package SUFFIX, and a substring `find` of the qualified literal accepted it as
/// the marshal `UserType`. Asserted on BOTH `UserType(` consumers, which used to
/// carry independent copies of that `find`.
#[test]
fn the_user_type_marker_locator_rejects_a_package_suffix() {
    let forged = format!(
        "my.org.apache.cassandra.db.marshal.UserType(ks,{},{}:{PKG}Int32Type)",
        hex::encode("f_type"),
        hex::encode("f")
    );
    let err = V5CompressedLegacyParser::parse_udt_type_definition(&forged)
        .expect_err("a package suffix is not the marshal package");
    assert!(
        err.to_string().contains("Not a UserType"),
        "the locator must not recognise it at all: {err}"
    );
    let err = V5CompressedLegacyParser::udt_field_marshal_types(&forged)
        .expect_err("the raw-marshal consumer shares the locator");
    assert!(err.to_string().contains("Not a UserType"), "{err}");
}

// ════════════════════════════════════════════════════════════════════════════
// POSITIVE CONTROLS: both legal spellings, unchanged.
// ════════════════════════════════════════════════════════════════════════════

/// `TypeParser.getAbstractType` resolves a bare name against the marshal package,
/// so the two legal spellings must be the SAME type. Without this the fix could
/// have been "refuse everything qualified", which would break every real
/// `SerializationHeader`.
#[test]
fn both_legal_spellings_of_a_native_type_decode_identically() {
    for (simple, expected) in [
        ("Int32Type", CqlType::Int),
        ("LongType", CqlType::BigInt),
        ("UTF8Type", CqlType::Text),
        ("InetAddressType", CqlType::Inet),
        ("SimpleDateType", CqlType::Date),
        ("DurationType", CqlType::Duration),
    ] {
        let bare = field_type_of(simple).expect("the bare spelling is legal");
        let qualified = field_type_of(&format!("{PKG}{simple}")).expect("so is the qualified one");
        assert_eq!(bare, expected, "bare {simple}");
        assert_eq!(qualified, expected, "qualified {simple}");
        assert_eq!(bare, qualified, "the two legal spellings must not diverge");
    }
}

/// The PACKAGE compare is ASCII-case-insensitive (the tolerance the `UserType(`
/// marker already had); a case variant cannot be a DIFFERENT package, so this
/// cannot admit a foreign class. The SIMPLE NAME stays case-SENSITIVE, which
/// several cell-path width cases depend on.
#[test]
fn the_package_is_case_insensitive_and_the_simple_name_is_not() {
    assert_eq!(
        field_type_of("ORG.APACHE.CASSANDRA.DB.MARSHAL.Int32Type").expect("package case tolerated"),
        CqlType::Int,
    );
    assert_eq!(
        field_type_of(&format!("{PKG}int32type")).expect("resolves, but to no native type"),
        CqlType::Custom(format!("{PKG}int32type")),
        "classForName is case-sensitive: `int32type` is not `Int32Type`",
    );
}

/// Structural forms still parse in their qualified spelling — the arms were
/// rewritten onto the shared dispatcher, so this is the control proving the
/// rewrite did not narrow them.
#[test]
fn qualified_structural_forms_still_parse() {
    assert_eq!(
        field_type_of(&format!("{PKG}ListType({PKG}Int32Type)")).expect("list"),
        CqlType::List(Box::new(CqlType::Int)),
    );
    assert_eq!(
        field_type_of(&format!("{PKG}SetType({PKG}UTF8Type)")).expect("set"),
        CqlType::Set(Box::new(CqlType::Text)),
    );
    assert_eq!(
        field_type_of(&format!("{PKG}MapType({PKG}UTF8Type,{PKG}Int32Type)")).expect("map"),
        CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
    );
    assert_eq!(
        field_type_of(&format!("{PKG}TupleType({PKG}Int32Type,{PKG}UTF8Type)")).expect("tuple"),
        CqlType::Tuple(vec![CqlType::Int, CqlType::Text]),
    );
    assert_eq!(
        field_type_of(&format!("{PKG}FrozenType({PKG}Int32Type)")).expect("frozen"),
        CqlType::Frozen(Box::new(CqlType::Int)),
    );
    assert_eq!(
        field_type_of(&format!("{PKG}ReversedType({PKG}Int32Type)")).expect("reversed"),
        CqlType::Int,
    );
    let nested = field_type_of(&format!(
        "{PKG}UserType(ks,{},{}:{PKG}Int32Type)",
        hex::encode("inner"),
        hex::encode("g")
    ))
    .expect("nested UserType");
    assert_eq!(
        nested,
        CqlType::Udt("inner".to_string(), vec![("g".to_string(), CqlType::Int)]),
    );
}

// ════════════════════════════════════════════════════════════════════════════
// The OTHER lookup fed by the same table: the CQL-short-form projection.
// ════════════════════════════════════════════════════════════════════════════

/// `primitive_marshal_to_cql_short` is a projection of the same table, and its two
/// call sites gate on `type_str.contains(PKG)` — a SUBSTRING test, which
/// `notorg.apache.cassandra.db.marshal.Int32Type` passes. So the package rule has
/// to hold inside the lookup itself, not only at the guard.
#[test]
fn the_cql_short_form_projection_inherits_the_package_rule() {
    assert_eq!(
        V5CompressedLegacyParser::primitive_marshal_to_cql_short(&format!("{PKG}Int32Type")),
        Some("int"),
    );
    assert_eq!(
        V5CompressedLegacyParser::primitive_marshal_to_cql_short("Int32Type"),
        Some("int"),
        "the bare spelling is legal",
    );
    for foreign in [
        "com.acme.Int32Type",
        "notorg.apache.cassandra.db.marshal.Int32Type",
        "my.org.apache.cassandra.db.marshal.Int32Type",
        "org.apache.cassandra.db.marshalX.Int32Type",
    ] {
        assert_eq!(
            V5CompressedLegacyParser::primitive_marshal_to_cql_short(foreign),
            None,
            "{foreign} is not a Cassandra native type",
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Boundary-safe indexing: a type string is attacker-influenced input.
// ════════════════════════════════════════════════════════════════════════════

/// The package rule and the `UserType(` locator both step through the string by
/// BYTE offset (`char_indices`), and so do the paren walks they replaced. Both
/// walks used to index with a CHARACTER index — `extract_inner_parens("é)")`
/// sliced `s[..1]` inside a 2-byte `é` and PANICKED — so a multi-byte character
/// anywhere in a `SerializationHeader` type string was a panic in the read path.
///
/// A type string is attacker-influenced, so this must be a clean refusal (or a
/// clean parse), never a panic.
#[test]
fn a_multibyte_character_in_a_type_string_does_not_panic() {
    for ty in [
        "com.acmé.Int32Type",
        &format!("{PKG}ListType(é)"),
        &format!("{PKG}FrozenType(çà)"),
        "é)",
        &format!("{PKG}UserType(é)"),
    ] {
        // Either outcome is fine; only a panic is not.
        let _ = field_type_of(ty);
        let _ = V5CompressedLegacyParser::parse_udt_type_definition(ty);
        let _ = V5CompressedLegacyParser::udt_field_marshal_types(ty);
        let _ = V5CompressedLegacyParser::extract_inner_parens(ty);
    }
}

/// The one case above whose OUTCOME is determinate: an unbalanced parameter list
/// carrying a multi-byte character is an unbalanced-parens refusal, not a panic
/// and not a silent empty parse.
#[test]
fn extract_inner_parens_refuses_an_unbalanced_multibyte_argument_list() {
    let err =
        V5CompressedLegacyParser::extract_inner_parens("é").expect_err("no closing paren at all");
    assert!(err.to_string().contains("Unbalanced parentheses"), "{err}");
    assert_eq!(
        V5CompressedLegacyParser::extract_inner_parens("é)")
            .expect("a multi-byte argument then a close paren is well-formed"),
        "é",
        "the inner text must come back whole, not sliced mid-character",
    );
}

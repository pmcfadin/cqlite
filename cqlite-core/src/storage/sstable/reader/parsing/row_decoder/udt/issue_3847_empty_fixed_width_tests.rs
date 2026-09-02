//! Issue #3847 — a ZERO-LENGTH UDT field of a fixed-width type is a LEGAL value
//! meaning `null`, at all three items of `field_value.rs` and at every UDT
//! FRAMING site that feeds them.
//!
//! # Oracle
//!
//! `docs/round-artifacts/issue-3847-cassandra-oracle.md` (pinned
//! `cassandra-5.0.8`), restated in `raw_value/fixed_width.rs`:
//! `TypeSerializer.deserialize` maps an empty buffer to `null` for all twelve
//! fixed-width scalars, uniformly. The stricter `validate()` (which rejects empty
//! for `smallint`, `tinyint`, `date` and `time`) gates writes, not reads.
//!
//! # Why the FRAMING cases are the ones that matter
//!
//! Five sites decide what a `[i32 BE len] == 0` field becomes, and before #3847
//! they disagreed with each other as well as with Cassandra: `parse_udt_value`
//! and `raw_type_value.rs`'s UDT arm routed to `create_empty_value_for_type`,
//! which presented a fixed-width field as an EMPTY BLOB; the other three
//! (`parse_nested_udt_from_registry`, `parse_inline_udt_value` and
//! `raw_type_value.rs`'s registry arm) call a field decoder with an explicit
//! `&[]`, which returned `Err`. An `Err` from a field decode is worse than it
//! looks — `row_data.rs` `break`s its column loop on one, so the failing column
//! AND every later column silently become null. Both halves are pinned below.
//!
//! # DECLARED REACHABILITY GAP
//!
//! `parse_udt_field_value`'s EMPTY arm is not reachable from `parse_udt_value`:
//! that framing site intercepts `field_len == 0` at
//! `create_empty_value_for_type` and never calls the decoder with an empty slice.
//! The arm is still widened, and pinned here at FUNCTION level, so the two field
//! decoders cannot drift into two opinions about a width — but these unit cases
//! must not be cited as end-to-end evidence for that one decoder. The end-to-end
//! evidence is the framing cases, which go through `create_empty_value_for_type`
//! (`parse_udt_value`) and `parse_simple_udt_field_value`
//! (`parse_nested_udt_from_registry`, `parse_inline_udt_value`).
//!
//! No dataset, reader or feature dependency: these run in every build and lane.

use super::*;
use crate::schema::{CqlType, UdtRegistry};
use crate::types::{UdtTypeDef, Value};

const KEYSPACE: &str = "issue_3847_ks";

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new(KEYSPACE.to_string(), "t".to_string(), 0, 0, None)
}

fn column() -> crate::schema::Column {
    crate::schema::Column {
        name: "u".to_string(),
        data_type: "udt".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// Every fixed-width `CqlType` the two field decoders enumerate, with its width.
/// `Counter`, `SmallInt`, `TinyInt` and `Time` are absent DELIBERATELY: neither
/// decoder has an arm for them (they fall to the blob default), which is a
/// pre-existing gap this change does not widen or close — see
/// `a_fixed_width_type_with_no_arm_still_falls_to_the_blob_default`.
fn decoder_arms() -> Vec<(CqlType, usize)> {
    vec![
        (CqlType::Int, 4),
        (CqlType::BigInt, 8),
        (CqlType::Float, 4),
        (CqlType::Double, 8),
        (CqlType::Boolean, 1),
        (CqlType::Uuid, 16),
        (CqlType::Timestamp, 8),
    ]
}

/// Case floor: a span-replacing edit that silently drops rows must red rather
/// than report a green tally over a shrunken table (#3544's lesson).
#[test]
fn the_arm_table_covers_both_decoders() {
    let arms = decoder_arms();
    assert_eq!(arms.len(), 7, "the table must carry all 7 shared arms");
    for w in [1usize, 4, 8, 16] {
        assert!(
            arms.iter().any(|(_, n)| *n == w),
            "no {w}-byte arm in the table"
        );
    }
}

/// `require_udt_field_width` — the shared rule, at its own level: `{n, 0}` and
/// nothing else.
#[test]
fn the_shared_width_rule_admits_exactly_n_and_zero() {
    for (_, n) in decoder_arms() {
        assert_eq!(
            V5CompressedLegacyParser::require_udt_field_width(&[], n, "T").unwrap(),
            FixedWidthCell::Null,
            "{n}: the empty buffer is the null spelling"
        );
        assert_eq!(
            V5CompressedLegacyParser::require_udt_field_width(&vec![1u8; n], n, "T").unwrap(),
            FixedWidthCell::Bytes,
            "{n}: exactly n bytes is the value"
        );
        for bad in (1..n).chain(std::iter::once(n + 1)) {
            let err = V5CompressedLegacyParser::require_udt_field_width(&vec![1u8; bad], n, "T")
                .expect_err("neither n nor 0");
            assert!(
                err.to_string().contains(&format!("got {bad}")),
                "{n}: {bad} bytes must be refused naming the width, got: {err}"
            );
        }
    }
}

/// The message wording is UNCHANGED by #3847, singular `byte` included, so a
/// caller or test matching on the text is unaffected.
#[test]
fn the_refusal_wording_is_unchanged() {
    let err = V5CompressedLegacyParser::require_udt_field_width(&[1, 2], 1, "Boolean")
        .expect_err("2 bytes is not a boolean");
    assert_eq!(
        err.to_string(),
        "Data corruption: Boolean field requires 1 byte, got 2",
        "the singular `byte` and the whole phrasing must be byte-identical to pre-#3847"
    );
    let err = V5CompressedLegacyParser::require_udt_field_width(&[1, 2], 4, "Int")
        .expect_err("2 bytes is not an int");
    assert_eq!(
        err.to_string(),
        "Data corruption: Int field requires 4 bytes, got 2"
    );
}

/// FUNCTION LEVEL (see the DECLARED REACHABILITY GAP in the header):
/// `parse_udt_field_value` decodes an empty field to null and still refuses every
/// other non-`n` width.
#[test]
fn parse_udt_field_value_decodes_an_empty_field_to_null() {
    let p = parser();
    for (ty, n) in decoder_arms() {
        assert_eq!(
            p.parse_udt_field_value(&[], &ty)
                .unwrap_or_else(|e| panic!("{ty:?}: empty is legal, got Err: {e}")),
            Value::Null,
            "{ty:?}: an empty field must decode to null"
        );
        assert_ne!(
            p.parse_udt_field_value(&vec![1u8; n], &ty)
                .unwrap_or_else(|e| panic!("{ty:?}: {n} bytes must decode, got Err: {e}")),
            Value::Null,
            "{ty:?}: a full-width field must not decode to null"
        );
        for bad in (1..n).chain(std::iter::once(n + 1)) {
            assert!(
                p.parse_udt_field_value(&vec![1u8; bad], &ty).is_err(),
                "{ty:?}: {bad} of {n} bytes must still be refused"
            );
        }
    }
}

/// `parse_simple_udt_field_value` — the decoder three framing sites call with an
/// explicit `&[]`, so this one IS the wired path for those sites.
#[test]
fn parse_simple_udt_field_value_decodes_an_empty_field_to_null() {
    for (ty, n) in decoder_arms() {
        assert_eq!(
            V5CompressedLegacyParser::parse_simple_udt_field_value(&[], &ty)
                .unwrap_or_else(|e| panic!("{ty:?}: empty is legal, got Err: {e}")),
            Value::Null,
            "{ty:?}: an empty field must decode to null"
        );
        for bad in (1..n).chain(std::iter::once(n + 1)) {
            assert!(
                V5CompressedLegacyParser::parse_simple_udt_field_value(&vec![1u8; bad], &ty)
                    .is_err(),
                "{ty:?}: {bad} of {n} bytes must still be refused"
            );
        }
    }
}

/// `TimeUuid` shares family B's `Uuid | TimeUuid` arm but has NO arm of its own
/// in family A. Both spellings are pinned, because an arm widened in one and not
/// the other is exactly the drift the shared rule exists to prevent.
#[test]
fn timeuuid_shares_the_uuid_arm_where_it_has_one() {
    assert_eq!(
        V5CompressedLegacyParser::parse_simple_udt_field_value(&[], &CqlType::TimeUuid).unwrap(),
        Value::Null
    );
    assert!(
        V5CompressedLegacyParser::parse_simple_udt_field_value(&[0u8; 8], &CqlType::TimeUuid)
            .is_err(),
        "8 bytes is not a timeuuid"
    );
}

/// The ZERO-LENGTH ROUTER: `create_empty_value_for_type` is what two of the five
/// framing sites consult, so its fixed-width rows must say `null` and its
/// variable-width rows must NOT (an empty `text` is `''`, not null — Cassandra
/// distinguishes them).
#[test]
fn the_zero_length_router_says_null_for_scalars_and_keeps_the_others() {
    for ty in [
        CqlType::Boolean,
        CqlType::TinyInt,
        CqlType::SmallInt,
        CqlType::Int,
        CqlType::BigInt,
        CqlType::Counter,
        CqlType::Float,
        CqlType::Double,
        CqlType::Timestamp,
        CqlType::Date,
        CqlType::Time,
        CqlType::Uuid,
        CqlType::TimeUuid,
    ] {
        assert_eq!(
            V5CompressedLegacyParser::create_empty_value_for_type(&ty),
            Value::Null,
            "{ty:?}: a zero-length fixed-width field is null"
        );
    }
    assert_eq!(
        V5CompressedLegacyParser::create_empty_value_for_type(&CqlType::Text),
        Value::text(String::new()),
        "an empty text field is the EMPTY STRING, not null"
    );
    assert_eq!(
        V5CompressedLegacyParser::create_empty_value_for_type(&CqlType::Blob),
        Value::blob(Vec::new())
    );
    assert_eq!(
        V5CompressedLegacyParser::create_empty_value_for_type(&CqlType::List(Box::new(
            CqlType::Int
        ))),
        Value::List(Vec::new())
    );
}

/// DECLARED, PRE-EXISTING GAP, pinned so it is measured rather than assumed:
/// `smallint`, `tinyint`, `time` and `counter` have no arm in either field
/// decoder and fall to the blob default, so their widths are unchecked THERE.
/// #3847 neither widens nor closes that — the zero-length ROUTER above does now
/// answer `null` for them, which is the half this change reaches.
#[test]
fn a_fixed_width_type_with_no_arm_still_falls_to_the_blob_default() {
    for ty in [
        CqlType::SmallInt,
        CqlType::TinyInt,
        CqlType::Time,
        CqlType::Counter,
    ] {
        let decoded = V5CompressedLegacyParser::parse_simple_udt_field_value(&[9, 9, 9], &ty)
            .expect("the blob default accepts any width");
        assert!(
            matches!(decoded, Value::Blob(_)),
            "{ty:?}: pinning the CURRENT behaviour — no arm, so blob. If this \
             starts failing, an arm was added and this case should become a real \
             width case rather than being deleted"
        );
    }
}

/// FRAMING, end to end: `parse_udt_value` over a real `[i32 BE len]` layout whose
/// second field is declared with length 0. Before #3847 that field surfaced as an
/// empty BLOB; it is now null.
#[test]
fn a_zero_length_field_of_a_framed_udt_decodes_to_null() {
    let def = UdtTypeDef::new(KEYSPACE.to_string(), "pair".to_string())
        .with_field("a".to_string(), CqlType::Int, true)
        .with_field("b".to_string(), CqlType::Int, true);
    // [len 4][7] [len 0]
    let mut data = 4i32.to_be_bytes().to_vec();
    data.extend_from_slice(&7i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes());

    let (value, consumed) = parser()
        .parse_udt_value(&data, 0, &def, &column())
        .expect("a zero-length field is legal");
    assert_eq!(consumed, data.len(), "the whole layout must be consumed");
    match value {
        Value::Udt(udt) => {
            assert_eq!(udt.fields[0].value, Some(Value::Integer(7)));
            assert_eq!(
                udt.fields[1].value,
                Some(Value::Null),
                "a zero-length int field is null, not an empty blob"
            );
        }
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// FRAMING, end to end, the other half: `parse_nested_udt_from_registry` calls
/// the field decoder with an explicit `&[]`, so before #3847 this returned `Err`
/// — and an `Err` here makes `row_data.rs` drop the column AND every later one.
#[test]
fn a_zero_length_field_of_a_registry_udt_decodes_to_null() {
    let def = UdtTypeDef::new(KEYSPACE.to_string(), "pair".to_string())
        .with_field("a".to_string(), CqlType::BigInt, true)
        .with_field("b".to_string(), CqlType::Int, true);
    let mut registry = UdtRegistry::new();
    registry.register_udt(def.clone());
    // [len 0] [len 4][7]
    let mut data = 0i32.to_be_bytes().to_vec();
    data.extend_from_slice(&4i32.to_be_bytes());
    data.extend_from_slice(&7i32.to_be_bytes());

    let value = parser()
        .parse_nested_udt_from_registry(&data, &def, &registry)
        .expect("a zero-length field is legal, not corruption");
    match value {
        Value::Udt(udt) => {
            assert_eq!(udt.fields[0].value, Some(Value::Null));
            assert_eq!(udt.fields[1].value, Some(Value::Integer(7)));
        }
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// FRAMING, end to end, third site: the INLINE-field decoder, which also passes
/// an explicit `&[]` and also returned `Err` before #3847.
#[test]
fn a_zero_length_field_of_an_inline_udt_decodes_to_null() {
    let fields = vec![
        ("a".to_string(), CqlType::Timestamp),
        ("b".to_string(), CqlType::Int),
    ];
    // [len 0] [len 4][7]
    let mut data = 0i32.to_be_bytes().to_vec();
    data.extend_from_slice(&4i32.to_be_bytes());
    data.extend_from_slice(&7i32.to_be_bytes());

    let value = parser()
        .parse_inline_udt_value(&data, "pair", &fields, 0)
        .expect("a zero-length field is legal, not corruption");
    match value {
        Value::Udt(udt) => {
            assert_eq!(udt.fields[0].value, Some(Value::Null));
            assert_eq!(udt.fields[1].value, Some(Value::Integer(7)));
        }
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// A `-1` length is still a DISTINCT thing from a `0` length at the framing
/// level: Cassandra writes `-1` for an absent field and `0` for a present-but-
/// empty one. Both render as null to a caller, but the framing must not collapse
/// them, or a future writer round-trip would lose the difference.
#[test]
fn a_negative_one_length_stays_an_absent_field_not_an_empty_one() {
    let def = UdtTypeDef::new(KEYSPACE.to_string(), "pair".to_string())
        .with_field("a".to_string(), CqlType::Int, true)
        .with_field("b".to_string(), CqlType::Int, true);
    let mut data = (-1i32).to_be_bytes().to_vec();
    data.extend_from_slice(&0i32.to_be_bytes());

    let (value, _) = parser()
        .parse_udt_value(&data, 0, &def, &column())
        .expect("both spellings are legal");
    match value {
        Value::Udt(udt) => {
            assert_eq!(
                udt.fields[0].value, None,
                "-1 means ABSENT: no value at all"
            );
            assert_eq!(
                udt.fields[1].value,
                Some(Value::Null),
                "0 means PRESENT-and-empty: a value, which is null"
            );
        }
        other => panic!("expected a UDT, got {other:?}"),
    }
}

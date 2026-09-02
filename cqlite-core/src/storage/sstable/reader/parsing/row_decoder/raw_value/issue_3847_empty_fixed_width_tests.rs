//! Issue #3847 — the EMPTY buffer is a LEGAL value for a fixed-width CQL scalar
//! on the bounded read path, and it means `null`.
//!
//! # Oracle
//!
//! `docs/round-artifacts/issue-3847-cassandra-oracle.md`, read at the pinned
//! `cassandra-5.0.8` tag: `TypeSerializer.deserialize` maps an empty buffer to
//! `null` for all twelve fixed-width scalars, with no per-type exceptions, and
//! `BooleanSerializer.serialize(null)` emits `EMPTY_BYTE_BUFFER`, so empty is the
//! on-the-wire spelling of null. `validate()` — which DOES reject empty for
//! `smallint`, `tinyint`, `date` and `time` — gates writes, not reads, and is not
//! this path's oracle. See `raw_value/fixed_width.rs` for the full statement.
//!
//! # What these cases pin, and why the THREE widths are one test each
//!
//! Before #3847 the composed accepted set of this path was exactly `{n}`: the
//! width guard refused `len < n` and the arm's reported consumption `n` made the
//! caller's fully-consumed assert refuse `len > n`. It is now `{n, 0}`, and the
//! `0` half only works because the arm reports `0` CONSUMED — report `n` there
//! and the fully-consumed assert refuses the value the width guard just admitted.
//! So each type is driven at all four widths that matter: `0` (⇒ `Null`), the
//! under-widths `1..n` (still refused), exactly `n` (⇒ the value), and `n + 1`
//! (still refused, by consumption). Widening one half without the other is a
//! defect that a `0`-only case cannot see.
//!
//! Driven through `parse_value_from_raw_bytes` — the bounded entry point that
//! CARRIES the fully-consumed assert — never through the reporting twin, because
//! the composition is the subject.
//!
//! No dataset, reader or feature dependency: these run in every build and lane
//! and cannot pass vacuously on an empty corpus.

use super::super::V5CompressedLegacyParser;
use crate::schema::CqlType;
use crate::{Result, Value};

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("issue_3847_ks".to_string(), "t".to_string(), 0, 0, None)
}

fn decode(type_str: &str, data: &[u8]) -> Result<Value> {
    parser().parse_value_from_raw_bytes(data, type_str, "col", 0)
}

/// Every fixed-width scalar spelling this path accepts, with its width. Both
/// aliases of each family are listed (`counter`/`bigint`, `short`/`smallint`,
/// `byte`/`tinyint`, `timeuuid`/`uuid`) because they are separate match arms and
/// an arm widened in one spelling and not the other is exactly the drift this
/// table exists to catch.
const FIXED_WIDTH: &[(&str, usize)] = &[
    ("int", 4),
    ("bigint", 8),
    ("counter", 8),
    ("boolean", 1),
    ("uuid", 16),
    ("timeuuid", 16),
    ("float", 4),
    ("double", 8),
    ("smallint", 2),
    ("short", 2),
    ("tinyint", 1),
    ("byte", 1),
    ("timestamp", 8),
    ("date", 4),
    ("time", 8),
];

/// The table above must cover all ELEVEN `require_fixed_width` call sites named
/// in #3847 — a case floor, so a span-replacing edit that silently drops rows
/// reds instead of reporting a green tally over a shrunken table (#3544's
/// lesson). 15 spellings over 11 arms.
#[test]
fn the_fixed_width_table_covers_every_arm() {
    assert_eq!(
        FIXED_WIDTH.len(),
        15,
        "the table must carry all 15 spellings of the 11 fixed-width arms"
    );
    let widths: Vec<usize> = FIXED_WIDTH.iter().map(|(_, n)| *n).collect();
    for n in [1usize, 2, 4, 8, 16] {
        assert!(
            widths.contains(&n),
            "no {n}-byte type in the table — an arm's width is unrepresented"
        );
    }
}

/// THE `0` HALF: an empty buffer decodes to `Value::Null` for every fixed-width
/// scalar, with NO per-type exceptions — including the four (`smallint`,
/// `tinyint`, `date`, `time`) whose `validate()` rejects empty. `validate()`
/// gates writes; `deserialize()` defines reads.
#[test]
fn an_empty_buffer_decodes_to_null_for_every_fixed_width_scalar() {
    for (type_str, n) in FIXED_WIDTH {
        assert_eq!(
            decode(type_str, &[]).unwrap_or_else(|e| panic!(
                "{type_str}: an empty buffer is a LEGAL {n}-byte-type value \
                 meaning null (Cassandra deserialize), got Err: {e}"
            )),
            Value::Null,
            "{type_str}: an empty buffer must decode to null"
        );
    }
}

/// THE `n` HALF, unchanged: an exactly-`n`-byte buffer still decodes to a real
/// value and NOT to null. Without this the `0` case above could be satisfied by
/// an arm that returned `Null` for everything.
#[test]
fn an_exactly_n_byte_buffer_still_decodes_to_a_value() {
    for (type_str, n) in FIXED_WIDTH {
        let value = decode(type_str, &vec![1u8; *n])
            .unwrap_or_else(|e| panic!("{type_str}: {n} bytes must decode, got Err: {e}"));
        assert_ne!(
            value,
            Value::Null,
            "{type_str}: a full-width buffer must not decode to null"
        );
    }
}

/// THE UNDER-WIDTH CASES ARE STILL REFUSED. `1..n` is a TRUNCATED value, not an
/// absent one — Cassandra's serializers admit `{n, 0}` and nothing between — so
/// widening to accept `0` must not have widened to accept `n - 1`.
#[test]
fn a_partial_buffer_is_still_refused() {
    let mut checked = 0usize;
    for (type_str, n) in FIXED_WIDTH {
        for short in 1..*n {
            assert!(
                decode(type_str, &vec![1u8; short]).is_err(),
                "{type_str}: {short} of {n} bytes is a TRUNCATED value and must be refused"
            );
            checked += 1;
        }
    }
    // Per-case floor: the 1-byte families contribute no under-width case, so a
    // table reduced to those alone would make this test vacuous.
    assert!(
        checked >= 30,
        "only {checked} under-width cases exercised — the table lost its wide types"
    );
}

/// THE OVER-WIDTH CASE IS STILL REFUSED, and it is refused by the CONSUMPTION
/// assert, which is what proves the empty arm reports `0` rather than `n`: an arm
/// that reported `n` for an empty buffer would be refused by this same assert.
#[test]
fn an_over_width_buffer_is_still_refused_by_the_consumption_assert() {
    for (type_str, n) in FIXED_WIDTH {
        let err = decode(type_str, &vec![1u8; n + 1])
            .expect_err("an over-width fixed-width value must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("decoded only") && msg.contains(&format!("{} of {}", n, n + 1)),
            "{type_str}: {} of {} bytes must be refused by the CONSUMPTION assert, got: {msg}",
            n,
            n + 1
        );
    }
}

/// WIRING: an empty ELEMENT inside a bounded frozen `list<int>` — the shape a
/// real frozen collection carries, with the element length coming from the
/// on-disk `[i32 BE len]` component header rather than from a hand-passed empty
/// slice. `[count = 1][len = 0]` ⇒ a one-element list holding null.
#[test]
fn an_empty_element_of_a_frozen_list_decodes_to_null() {
    let mut data = 1i32.to_be_bytes().to_vec();
    data.extend_from_slice(&0i32.to_be_bytes());
    assert_eq!(
        decode("list<int>", &data).expect("a zero-length element is a legal null element"),
        Value::List(vec![Value::Null]),
        "a zero-length frozen list element must decode to null"
    );
}

/// WIRING: an empty COMPONENT of a bounded `tuple<int, int>`, where the empty is
/// again framed by the on-disk component length. `[len = 0][len = 4][7]`.
#[test]
fn an_empty_component_of_a_tuple_decodes_to_null() {
    let mut data = 0i32.to_be_bytes().to_vec();
    data.extend_from_slice(&4i32.to_be_bytes());
    data.extend_from_slice(&7i32.to_be_bytes());
    assert_eq!(
        decode("tuple<int, int>", &data).expect("a zero-length component is a legal null"),
        Value::Tuple(vec![Value::Null, Value::Integer(7)]),
        "a zero-length tuple component must decode to null"
    );
}

/// DECLARED RESIDUAL — `duration` is NOT in #3847's scope and is NOT widened
/// here, and this case pins that so the gap is measured rather than assumed.
///
/// `duration` is variable-width (three signed VInts), so it is not one of the 11
/// fixed-width arms. Cassandra splits on it: `DurationSerializer.validate` is
/// `< 3` and REJECTS empty, while `DurationSerializer.deserialize:61-63` returns
/// `null` for empty like every other type. By this path's own oracle
/// (`deserialize`, not `validate`) an empty `duration` should therefore decode to
/// null too — CQLite instead refuses it, from `parse_vint`'s EOF on a zero-length
/// slice. Widening it needs its own follow-up: it is a different framing (a
/// VInt-triple, whose arm reports a measured consumption rather than a constant
/// `n`), so it does not ride on the fixed-width helper this change adds.
#[test]
fn an_empty_duration_is_still_refused_declared_residual_of_3847() {
    let err = decode("duration", &[]).expect_err(
        "pinning the CURRENT behaviour: an empty duration is refused. If this \
         starts passing, the residual was closed — update this case and the \
         note in fixed_width.rs rather than deleting it",
    );
    assert!(
        err.to_string().contains("duration months"),
        "the refusal must still come from the duration VInt decode, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// roborev job 96 (Low): the FOURTH and FIFTH framing sites.
//
// The three tests in `udt/issue_3847_empty_fixed_width_tests.rs` drive
// `parse_udt_value`, `parse_nested_udt_from_registry` and
// `parse_inline_udt_value` DIRECTLY. `raw_type_value.rs` has two more
// zero-length branches, reachable only THROUGH `parse_value_from_raw_bytes`,
// and they use DIFFERENT routing helpers — the marshal/inline arm calls
// `create_empty_value_for_type` while the registry arm calls
// `parse_simple_udt_field_value(&[], …)`. Two helpers, so two ways to diverge,
// which is exactly how rounds 1 and 2 of this review found real defects. Kept
// end-to-end rather than helper-level on the wiring-evidence rule: green
// helper-only unit tests are not sufficient.
// ---------------------------------------------------------------------------

/// `pair(a int, b int)` in MARSHAL form. Field names are hex: `70616972` =
/// "pair", `61` = "a", `62` = "b". Reaches `raw_type_value.rs`'s marshal/inline
/// UDT arm, whose zero-length branch routes through
/// `create_empty_value_for_type`.
const MARSHAL_PAIR: &str = "org.apache.cassandra.db.marshal.UserType(issue_3847_ks,70616972,\
61:org.apache.cassandra.db.marshal.Int32Type,\
62:org.apache.cassandra.db.marshal.Int32Type)";

/// A parser whose registry resolves the BARE name `pair` — the route to
/// `raw_type_value.rs`'s registry arm, whose zero-length branch calls
/// `parse_simple_udt_field_value` with an explicit `&[]`.
fn parser_with_pair_registry() -> V5CompressedLegacyParser {
    let mut reg = crate::schema::UdtRegistry::new();
    reg.register_udt(
        crate::types::UdtTypeDef::new("issue_3847_ks".to_string(), "pair".to_string())
            .with_field("a".to_string(), crate::schema::CqlType::Int, true)
            .with_field("b".to_string(), crate::schema::CqlType::Int, true),
    );
    V5CompressedLegacyParser::new("issue_3847_ks".to_string(), "t".to_string(), 0, 0, None)
        .with_udt_registry(reg)
}

/// `[i32 BE len][bytes]` per field: a present `a = 7`, then a zero-length `b`.
fn pair_with_empty_second_field() -> Vec<u8> {
    let mut data = 4i32.to_be_bytes().to_vec();
    data.extend_from_slice(&7i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes());
    data
}

fn assert_pair_is_seven_then_null(value: Value, site: &str) {
    match value {
        Value::Udt(udt) => {
            assert_eq!(
                udt.fields[0].value,
                Some(Value::Integer(7)),
                "{site}: the present field must still decode"
            );
            assert_eq!(
                udt.fields[1].value,
                Some(Value::Null),
                "{site}: a zero-length int field is NULL, not an empty blob and not an Err"
            );
        }
        other => panic!("{site}: expected a UDT, got {other:?}"),
    }
}

/// FOURTH site: the marshal/inline UDT arm, end to end.
#[test]
fn a_zero_length_field_of_a_marshal_form_udt_decodes_to_null() {
    let value = decode(MARSHAL_PAIR, &pair_with_empty_second_field())
        .expect("a zero-length field is legal, not corruption");
    assert_pair_is_seven_then_null(value, "marshal-form UDT via parse_value_from_raw_bytes");
}

/// FIFTH site: the registry-resolved arm, end to end. Distinct from the fourth
/// because it routes through a DIFFERENT helper.
#[test]
fn a_zero_length_field_of_a_registry_resolved_udt_decodes_to_null() {
    let value = parser_with_pair_registry()
        .parse_value_from_raw_bytes(&pair_with_empty_second_field(), "pair", "col", 0)
        .expect("a zero-length field is legal, not corruption");
    assert_pair_is_seven_then_null(
        value,
        "registry-resolved UDT via parse_value_from_raw_bytes",
    );
}

// ---------------------------------------------------------------------------
// roborev job 97 (Medium): the tests above were INVARIANT TO THE REAL DEFECT.
//
// They build `CqlType::SmallInt` / `CqlType::TinyInt` by hand. Production does
// not: a marshal-form UDT field arrives as a STRING and
// `udt.rs::parse_cassandra_type_with_depth` resolves it, and that resolver had no
// `ShortType` / `ByteType` / `CounterColumnType` arm — so those fields became
// `CqlType::Custom("…ShortType")`, `fixed_width::width_of` answered `None`, the
// #3847 empty rule never fired, and a zero-length `smallint` field still decoded
// to an EMPTY BLOB on the only path a real SSTable takes.
//
// This is #3042's blind-spot class inside this issue's own test suite: a test that
// cannot fail for the defect it exists to pin. The cases below therefore drive the
// MARSHAL STRING, never a hand-built `CqlType`, and one of them asserts the
// resolver directly so the two halves cannot drift apart again.
// ---------------------------------------------------------------------------

/// `nums(s smallint, t tinyint)` in MARSHAL form — the production spelling.
/// `6e756d73` = "nums", `73` = "s", `74` = "t".
const MARSHAL_NUMS: &str = "org.apache.cassandra.db.marshal.UserType(issue_3847_ks,6e756d73,\
73:org.apache.cassandra.db.marshal.ShortType,\
74:org.apache.cassandra.db.marshal.ByteType)";

/// The RESOLVER, asserted directly: every marshal spelling of a fixed-width scalar
/// must resolve to a variant `width_of` recognises. A `Custom(_)` here is the job-97
/// defect, and it is silent — the decode simply returns a blob.
#[test]
fn every_fixed_width_marshal_name_resolves_to_a_width_bearing_variant() {
    use super::super::V5CompressedLegacyParser as P;
    use super::fixed_width;
    for (marshal, expect) in [
        (
            "org.apache.cassandra.db.marshal.ShortType",
            CqlType::SmallInt,
        ),
        ("org.apache.cassandra.db.marshal.ByteType", CqlType::TinyInt),
        (
            "org.apache.cassandra.db.marshal.CounterColumnType",
            CqlType::Counter,
        ),
        ("org.apache.cassandra.db.marshal.Int32Type", CqlType::Int),
        ("org.apache.cassandra.db.marshal.LongType", CqlType::BigInt),
        ("org.apache.cassandra.db.marshal.TimeType", CqlType::Time),
        (
            "org.apache.cassandra.db.marshal.SimpleDateType",
            CqlType::Date,
        ),
        (
            "org.apache.cassandra.db.marshal.BooleanType",
            CqlType::Boolean,
        ),
    ] {
        let resolved = P::parse_cassandra_type(marshal)
            .unwrap_or_else(|e| panic!("{marshal}: resolver failed: {e:?}"));
        assert_eq!(
            resolved, expect,
            "{marshal} must resolve to {expect:?}, not a Custom(_) the width rule cannot see"
        );
        assert!(
            fixed_width::width_of(&resolved).is_some(),
            "{marshal} resolved to {resolved:?}, which width_of does not recognise — \
             the #3847 empty rule would not fire for it"
        );
    }

    // BytesType must NOT be captured by the ByteType arm (suffix collision check,
    // asserted rather than reasoned about).
    assert_eq!(
        P::parse_cassandra_type("org.apache.cassandra.db.marshal.BytesType")
            .expect("resolver failed"),
        CqlType::Blob,
        "BytesType is blob; the ByteType arm must not shadow it"
    );
}

/// END TO END on the production path: a zero-length `smallint` and a zero-length
/// `tinyint` field, reached through a MARSHAL type string.
#[test]
fn zero_length_smallint_and_tinyint_fields_are_null_through_the_marshal_path() {
    // [len 0] [len 0] — both fields present and empty.
    let mut data = 0i32.to_be_bytes().to_vec();
    data.extend_from_slice(&0i32.to_be_bytes());

    let value = decode(MARSHAL_NUMS, &data).expect("zero-length fields are legal");
    match value {
        Value::Udt(udt) => {
            for (i, name) in ["smallint", "tinyint"].iter().enumerate() {
                assert_eq!(
                    udt.fields[i].value,
                    Some(Value::Null),
                    "a zero-length {name} field must be NULL through the marshal path, \
                     not an empty blob (roborev job 97)"
                );
            }
        }
        other => panic!("expected a UDT, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// roborev job 98 (Medium) — and the reason this test exists rather than a sixth
// one-line arm.
//
// Jobs 97 and 98 are the SAME defect twice: this repository has TWO marshal-name
// resolvers, and they were out of sync. Job 97 was `ShortType`/`ByteType`; job 98
// was `VarcharType`. Enumerating both tables by hand at that point showed SIX
// divergences, so rounds 6, 7 and 8 would have found the rest one at a time.
//
// A per-instance fix cannot close that: the two tables are independent `ends_with`
// chains and nothing compares them. This case DOES compare them, over the whole
// marshal-name set, so a future omission on either side fails HERE instead of in
// someone's review round. Every remaining divergence is DECLARED below with its
// reason — a declared exception is a decision; an undeclared one is a bug waiting.
// ---------------------------------------------------------------------------

/// The two resolvers must AGREE for every marshal name, except where a divergence
/// is DECLARED — and agreement is checked by comparing what each one ANSWERS, not
/// by checking that both merely answered something.
///
/// roborev job 99 corrected this test: its first version asserted only
/// `primitive_marshal_to_cql_short(..).is_some()`, i.e. that the sibling KNEW the
/// name. That is presence, not agreement, so an undeclared mapping disagreement
/// passed — a false PASS in the very guard written to close the job-97/98 class,
/// and the repository's own rule is that such a guard is worse than none because
/// it invites reliance it cannot support. The expected value is now DERIVED
/// (`CqlType::parse` of the sibling's short form) rather than curated, and each
/// declared divergence must actually BE divergent, so an exception that becomes
/// obsolete fails here instead of lingering.
#[test]
fn the_two_marshal_resolvers_agree_or_declare_their_divergence() {
    use super::super::V5CompressedLegacyParser as P;
    const PREFIX: &str = "org.apache.cassandra.db.marshal.";

    // (marshal name, sibling's canonical short form, declared-divergence reason)
    let cases: &[(&str, &str, Option<&str>)] = &[
        ("UTF8Type", "text", None),
        ("VarcharType", "text", None),
        ("AsciiType", "ascii", None),
        ("Int32Type", "int", None),
        ("LongType", "bigint", None),
        ("ShortType", "smallint", None),
        ("ByteType", "tinyint", None),
        ("FloatType", "float", None),
        ("DoubleType", "double", None),
        ("BooleanType", "boolean", None),
        ("UUIDType", "uuid", None),
        ("LexicalUUIDType", "uuid", None),
        ("TimestampType", "timestamp", None),
        ("SimpleDateType", "date", None),
        ("TimeType", "time", None),
        ("DecimalType", "decimal", None),
        ("IntegerType", "varint", None),
        ("DurationType", "duration", None),
        ("InetAddressType", "inet", None),
        ("BytesType", "blob", None),
        // ---- DECLARED DIVERGENCES: the two resolvers answer DIFFERENTLY on purpose ----
        (
            "TimeUUIDType",
            "timeuuid",
            Some(
                "sibling says `timeuuid`, the UDT resolver says Uuid. Both are 16 bytes so the \
                 #3847 width rule is unaffected; the only loss is that a timeuuid field reports \
                 as uuid. Type fidelity, not an empty-buffer question.",
            ),
        ),
        (
            "CounterColumnType",
            "bigint",
            Some(
                "sibling normalises to `bigint`, the UDT resolver answers Counter. Same 8-byte \
                 width and the reporting path shares one arm for bigint/counter, so they are \
                 behaviourally equivalent. NOTE: commit 50fc7c22 (this branch) INTRODUCED this \
                 divergence; it is kept because Counter is the more faithful variant.",
            ),
        ),
        (
            "DateType",
            "timestamp",
            Some(
                "REAL LATENT DEFECT, out of #3847's scope and reported for follow-up: legacy \
                 DateType is an 8-byte millis-since-epoch value (the sibling maps it to \
                 timestamp and documents why) while the UDT resolver answers Date, whose width \
                 is 4 — so a NON-EMPTY legacy DateType UDT field fails the exact-width check \
                 instead of decoding. Fixing it changes non-empty behaviour and needs its own \
                 corpus measurement.",
            ),
        ),
    ];

    for (name, short, divergence) in cases {
        let full = format!("{PREFIX}{name}");

        // The sibling must answer, and answer exactly the short form recorded here.
        let sibling = P::primitive_marshal_to_cql_short(&full).unwrap_or_else(|| {
            panic!("{full}: the sibling resolver returned None — the tables have drifted")
        });
        assert_eq!(
            sibling, *short,
            "{full}: sibling resolver answered {sibling:?}, this table says {short:?}"
        );

        // DERIVED expectation: what the sibling's short form means as a CqlType.
        let via_sibling = CqlType::parse(short)
            .unwrap_or_else(|e| panic!("{full}: short form {short:?} does not parse: {e:?}"));
        let via_udt = P::parse_cassandra_type(&full)
            .unwrap_or_else(|e| panic!("{full}: UDT resolver failed: {e:?}"));

        assert!(
            !matches!(via_udt, CqlType::Custom(_)),
            "{full}: UDT resolver answered Custom(_). A marshal name the sibling knows must \
             never reach Custom here — that is the job-97/98 defect, and it is SILENT: the \
             decode quietly returns a blob."
        );

        match divergence {
            None => assert_eq!(
                via_udt, via_sibling,
                "{full}: the two resolvers DISAGREE and the divergence is not declared. \
                 Either make them agree, or add a declared reason to this table."
            ),
            Some(reason) => assert_ne!(
                via_udt, via_sibling,
                "{full}: a divergence is DECLARED but the resolvers now AGREE — the \
                 declaration is obsolete and must be removed. Recorded reason: {reason}"
            ),
        }
    }
}

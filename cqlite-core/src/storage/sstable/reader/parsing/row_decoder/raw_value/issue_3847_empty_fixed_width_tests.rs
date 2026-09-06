//! Issue #3847 — the EMPTY buffer is a LEGAL value for a fixed-width CQL scalar
//! on the bounded read path, and it means `null`.
//!
//! # Oracle
//!
//! `docs/round-artifacts/issue-3847-cassandra-oracle.md`, read at the pinned
//! `cassandra-5.0.8` tag: `TypeSerializer.deserialize` maps an empty buffer to
//! `null` for every fixed-width scalar, with no per-type exceptions, and
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

/// The table above must cover EVERY `require_fixed_width` call site in
/// `raw_value/reporting.rs`, the arm set #3847 names — a case floor, so a
/// span-replacing edit that silently drops rows reds instead of reporting a green
/// tally over a shrunken table (#3544's lesson). The arm count is deliberately
/// not restated in prose: a hand-maintained census decays (it did, twice, in the
/// sibling `nested_fixed_width_length_tests.rs`). The one number below is
/// admissible because the assertion PINS it — it cannot drift silently.
#[test]
fn the_fixed_width_table_covers_every_arm() {
    assert_eq!(
        FIXED_WIDTH.len(),
        15,
        "the table must carry all 15 spellings the fixed-width arms accept"
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
// NOTE (roborev job 149): this block once also cited three sibling tests in
// `udt/issue_3847_empty_fixed_width_tests.rs`. That file is GONE — #3631/PR#3820
// superseded this branch's UDT field decoders with `row_decoder/typed_value.rs`,
// and the tests went with the code they pinned. Left as a pointer rather than
// deleted, because the REASON these two cases exist is unchanged.
//
// `raw_type_value.rs` has two zero-length branches reachable only THROUGH
// `parse_value_from_raw_bytes`, and they use DIFFERENT routing helpers — the
// marshal/inline arm calls `create_empty_value_for_type` while the registry arm
// calls a field decoder with an explicit `&[]`. Two helpers, so two ways to
// diverge, which is exactly how rounds 1 and 2 of this review found real defects.
// Kept end-to-end rather than helper-level on the wiring-evidence rule: green
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
// They built `CqlType::SmallInt` / `CqlType::TinyInt` by hand. Production does
// not: a marshal-form UDT field arrives as a STRING and a resolver maps it, and
// that resolver had no `ShortType` / `ByteType` / `CounterColumnType` arm — so
// those fields became `CqlType::Custom("…ShortType")`, this issue's empty rule
// never fired, and a zero-length `smallint` field still decoded to an EMPTY BLOB
// on the only path a real SSTable takes.
//
// This is #3042's blind-spot class inside this issue's own test suite: a test that
// cannot fail for the defect it exists to pin. The cases below therefore drive the
// MARSHAL STRING, never a hand-built `CqlType`.
//
// UPDATE (roborev job 149): the RESOLVER half of that fix is now MAIN'S. #3631
// rewrote the marshal-name table in `udt/type_string.rs` as an EXACT match on the
// simple name — superseding the six `ends_with` arms this branch had added, and
// additionally fixing `TimeUUIDType` -> TimeUuid and `DateType` -> Timestamp. The
// companion assertion that compared the two resolvers is removed with the arms it
// guarded; `parse_cassandra_type` is private on main. What remains below is the
// end-to-end case, which is the part that pins THIS path.
// ---------------------------------------------------------------------------

/// `nums(s smallint, t tinyint)` in MARSHAL form — the production spelling.
/// `6e756d73` = "nums", `73` = "s", `74` = "t".
const MARSHAL_NUMS: &str = "org.apache.cassandra.db.marshal.UserType(issue_3847_ks,6e756d73,\
73:org.apache.cassandra.db.marshal.ShortType,\
74:org.apache.cassandra.db.marshal.ByteType)";

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

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

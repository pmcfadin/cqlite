//! Issue #3723: the multicell-`set` member decode, at the COLLECTION level.
//!
//! These are the hermetic siblings of `tests/issue_3723_read_path_refusal.rs`
//! (which proves the same properties through the public reader over a real
//! written+patched `Data.db`). They exercise `parse_complex_column_inner`
//! directly, so a regression is localized to the set branch rather than
//! diagnosed from a whole-file read.
//!
//! Oracle — pinned `cassandra-5.0.8`:
//! * `serializers/Int32Serializer.java` `validate(...)` refuses a length that is
//!   neither 4 nor 0 by THROWING (`"Expected 4 or 0 byte int (%d)"`).
//! * `serializers/SetSerializer.java` `validate(...)` does not catch that
//!   exception and additionally throws on extraneous bytes — Cassandra rejects
//!   the whole value rather than dropping the element, which is why the error is
//!   fatal here.
//! * The zero-length case is where this decoder is deliberately STRICTER than
//!   `Int32Serializer.validate` — rationale in `fixed_width.rs`.

use super::super::test_support::helpers::build_set_cell_bytes;
use super::*;
use crate::schema::Column;

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None)
}

fn column(data_type: &str) -> Column {
    Column {
        name: "my_set".to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// `[cell_count = 1][one HAS_EMPTY_VALUE cell whose PATH is `path`]`.
fn one_element_blob(path: &[u8]) -> Vec<u8> {
    let mut blob = vec![0x01u8];
    blob.extend(build_set_cell_bytes(path));
    blob
}

fn decode(column_type: &str, path: &[u8]) -> Result<Value> {
    let p = parser();
    let col = column(column_type);
    let blob = one_element_blob(path);
    p.parse_complex_column_inner(&blob, 0, &col, &col.data_type, false, 0, None, None)
        .map(|(value, _consumed, _meta)| value)
}

#[test]
fn well_formed_int_member_decodes_as_control() {
    assert_eq!(
        decode("set<int>", &7i32.to_be_bytes()).expect("a 4-byte int member must decode"),
        Value::Set(vec![Value::Integer(7)]),
        "control: without this a refusal below could pass for the wrong reason"
    );
}

/// A WRONG WIDTH is refused with the NAMED error AND is FATAL to the read: the
/// collection is not returned minus the member.
///
/// The ZERO-length case is deliberately NOT in this loop — it is refused too,
/// but stays TOLERATED (see `zero_length_member_is_refused_but_tolerated`
/// below), because unlike a wrong width it is not a disposition this branch
/// introduced.
#[test]
fn wrong_width_int_members_are_refused_not_omitted() {
    for path in [
        vec![0x00, 0x00, 0x07],             // 3 bytes
        vec![0x00, 0x00, 0x00, 0x07, 0x00], // 5 bytes
    ] {
        let declared = path.len();
        match decode("set<int>", &path) {
            Err(Error::FixedWidthLengthMismatch {
                expected, actual, ..
            }) => {
                assert_eq!(expected, 4, "`int` admits exactly 4 bytes here");
                assert_eq!(
                    actual, declared,
                    "the error must report the declared length"
                );
            }
            Err(other) => panic!(
                "a {declared}-byte `int` set member must be refused with the NAMED \
                 FixedWidthLengthMismatch, got {other:?}"
            ),
            Ok(value) => panic!(
                "a {declared}-byte `int` set member must be REFUSED, not silently omitted \
                 from the collection — got {value:?}"
            ),
        }
    }
}

/// A ZERO-length fixed-width member is refused with the SAME named error, but
/// the refusal is TOLERATED: the member is omitted and the collection is
/// returned — the EXACT disposition this input had before issue #3723.
///
/// Pre-#3723 the set branch's `else if !cell.path_bytes.is_empty()` guard meant
/// an empty path never reached the decoder at all, so the member was silently
/// dropped and the read returned `Set([])` (verified against `origin/main`'s
/// `complex_column.rs`). #3723 routes it through the width guard so the refusal
/// is NAMED and observable — but escalating it to a fatal read failure would
/// change the behaviour of a path that already errored before this branch, which
/// is precisely what #3723 promised not to do. Only a WRONG width is fatal; see
/// `fatal_decode_error.rs`.
#[test]
fn zero_length_member_is_refused_but_tolerated() {
    assert_eq!(
        decode("set<int>", &[]).expect(
            "a zero-length fixed-width member must keep its pre-#3723 TOLERATED \
             disposition — refused, named, but not fatal to the read"
        ),
        Value::Set(vec![]),
        "the member is omitted and the collection survives, exactly as on origin/main"
    );
}

/// Anti-blanket-propagation: a TOLERATED decode failure still omits the member
/// and returns the collection. Invalid UTF-8 is `Error::corruption` from the text
/// arm, which is NOT in the fatal set.
#[test]
fn tolerated_member_failure_still_returns_the_collection() {
    assert_eq!(
        decode("set<text>", &[0x80]).expect(
            "a tolerated (non-width) member failure must NOT fail the whole collection — \
             #3723 changed exactly one error class"
        ),
        Value::Set(vec![]),
        "the undecodable member is omitted, the collection survives"
    );
}

/// An EMPTY path for a VARIABLE-width element type is a legal empty value and
/// must decode — the #3723 empty-path routing must not turn every zero-length
/// member into a refusal, only the fixed-width ones.
#[test]
fn empty_path_for_a_variable_width_element_decodes_as_empty() {
    assert_eq!(
        decode("set<text>", &[]).expect("an empty `text` member is a legal empty value"),
        Value::Set(vec![Value::text(String::new())]),
        "routing the empty path through the decoder must yield the empty text value, \
         not a refusal and not a silently dropped member"
    );
}

// ---------------------------------------------------------------------------
// Issue #3723 (review round 2): a set cell that UNUSUALLY carries a cell value
// must still have its PATH decoded.
//
// Extra oracle for these cases, pinned `cassandra-5.0.8`:
// * `schema/ColumnMetadata.java` `validateCell(...)` validates a live non-UDT
//   cell's value and THEN, with no condition on that value,
//   `validateCellPath(path)` → `nameComparator().validate(path.get(0))`, i.e.
//   the element type's own `validate`. So the path check is not gated on the
//   value's absence, which is exactly what an early `return` on the cell value
//   broke.
// * `db/marshal/CollectionType.java` `serializeForNativeProtocol(...)` →
//   `SetType.serializedValues(...)` reads `cells.next().path().get(0)` and never
//   the cell value, so the path is never dead bytes for a set.
// ---------------------------------------------------------------------------

/// One complex cell WITHOUT `HAS_EMPTY_VALUE` (flags `0x00`), i.e. carrying both
/// a path and a value, in the layout `parse_complex_cell_value` reads:
/// `[flags:u8][timestamp:VUInt][path_len:VUInt][path][value_len:VUInt][value]`.
fn one_valued_element_blob(path: &[u8], value: &[u8]) -> Vec<u8> {
    assert!(
        path.len() < 0x80 && value.len() < 0x80,
        "single-byte VUInt only"
    );
    let mut blob = vec![0x01u8]; // cell_count = 1
    blob.extend_from_slice(&[0x00, 0x00, path.len() as u8]);
    blob.extend_from_slice(path);
    blob.push(value.len() as u8);
    blob.extend_from_slice(value);
    blob
}

fn decode_valued(column_type: &str, path: &[u8], value: &[u8]) -> Result<Value> {
    let p = parser();
    let col = column(column_type);
    let blob = one_valued_element_blob(path, value);
    p.parse_complex_column_inner(&blob, 0, &col, &col.data_type, false, 0, None, None)
        .map(|(value, _consumed, _meta)| value)
}

/// THE hole: a VALID 4-byte cell value used to short-circuit the member decode,
/// so a wrong-width member PATH never reached the width guard at all.
#[test]
fn wrong_width_path_is_refused_even_when_the_cell_carries_a_valid_value() {
    let good_value = 9i32.to_be_bytes();
    for path in [
        vec![0x00, 0x00, 0x07],             // 3 bytes
        vec![0x00, 0x00, 0x00, 0x07, 0x00], // 5 bytes
    ] {
        let declared = path.len();
        match decode_valued("set<int>", &path, &good_value) {
            Err(Error::FixedWidthLengthMismatch {
                expected, actual, ..
            }) => {
                assert_eq!(expected, 4, "`int` admits exactly 4 bytes here");
                assert_eq!(
                    actual, declared,
                    "the error must report the declared length"
                );
            }
            Err(other) => panic!(
                "a {declared}-byte `int` member path must be refused with the NAMED \
                 FixedWidthLengthMismatch even when a valid cell value is present, \
                 got {other:?}"
            ),
            Ok(value) => panic!(
                "a {declared}-byte `int` member path must be REFUSED even when the cell \
                 carries a valid value — a correctly sized value must not buy the path a \
                 bypass of the width guard; got {value:?}"
            ),
        }
    }
}

/// The zero-length half of the case above: refused, but TOLERATED, so the cell
/// value still wins and the collection is returned. Pre-#3723 `if let Some(val)
/// = cell.value` short-circuited before the path was looked at, yielding exactly
/// this `Set([9])` — the disposition preserved here.
#[test]
fn a_zero_length_path_on_a_valued_cell_still_yields_the_cell_value() {
    assert_eq!(
        decode_valued("set<int>", &[], &9i32.to_be_bytes()).expect(
            "a zero-length path is refused but TOLERATED, so the valued cell still decodes"
        ),
        Value::Set(vec![Value::Integer(9)]),
        "the pre-#3723 result for an empty path on a valued set cell"
    );
}

/// Negative control 1: a well-formed path plus a cell value still decodes, and
/// the RETURNED member stays the cell value — pre-#3723 precedence is
/// deliberately unchanged (rationale at the ordering in `set_member.rs`).
#[test]
fn a_valued_cell_with_a_well_formed_path_still_decodes() {
    assert_eq!(
        decode_valued("set<int>", &7i32.to_be_bytes(), &9i32.to_be_bytes())
            .expect("a 4-byte path plus a 4-byte value must decode"),
        Value::Set(vec![Value::Integer(9)]),
        "the path is now always decoded, but the cell value still wins as the \
         returned member exactly as before issue #3723"
    );
}

/// Negative control 2: a TOLERATED path failure on a valued cell behaves exactly
/// as before — the member survives via the cell value, nothing is refused.
/// Invalid UTF-8 is `Error::corruption`, which is NOT in the one-variant fatal set.
#[test]
fn a_tolerated_path_failure_on_a_valued_cell_still_yields_the_cell_value() {
    assert_eq!(
        decode_valued("set<text>", &[0x80], b"hi").expect(
            "a tolerated (non-width) path failure must not fail the collection — \
             #3723 made exactly one error class fatal"
        ),
        Value::Set(vec![Value::text("hi".to_string())]),
        "the undecodable path is tolerated and the cell value is returned"
    );
}

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

/// Overlong, short AND zero-length are all refused with the NAMED error.
///
/// The zero-length case is the one issue #3723 review found bypassing the guard
/// entirely: `else if !cell.path_bytes.is_empty()` meant an empty path never
/// reached the decoder and the member was silently OMITTED.
#[test]
fn wrong_width_int_members_are_refused_not_omitted() {
    for path in [
        vec![],                             // 0 bytes
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

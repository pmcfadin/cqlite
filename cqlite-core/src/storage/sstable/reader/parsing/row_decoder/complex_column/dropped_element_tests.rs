//! Issue #3723 (review round 5): a multicell MAP entry's KEY and a multicell
//! UDT FIELD's value are width-validated even when the #1741 per-element
//! shadow/TTL filter is about to DROP them.
//!
//! Round 3 moved the width guard ahead of the `dropped` early return for SETS
//! only (`raw_value/set_member_dropped_tests.rs`). These two sibling branches
//! still `continue`d BEFORE their own decode, so malformed fixed-width bytes
//! returned success whenever the affected entry/field happened to be shadowed
//! by a covering deletion or expired at the read clock — the very asymmetry
//! round 3 removed, one branch over.
//!
//! Oracle — pinned `cassandra-5.0.8`, never CQLite's prior output:
//! * `schema/ColumnMetadata.java` `validateCell(...)` validates the cell VALUE
//!   and then, unconditionally, `validateCellPath(cell.path())` for every live
//!   cell it is handed. Nothing in that validation consults a covering deletion
//!   or the read clock — reconciliation (`db/rows/Cells.java`) is a separate,
//!   LATER concern from whether the bytes are well formed.
//! * `serializers/Int32Serializer.java` `validate(...)` throws
//!   `"Expected 4 or 0 byte int (%d)"`; `serializers/ListSerializer.java`
//!   `validate(...)` lets that element-level `MarshalException` escape for the
//!   whole value. Cassandra REFUSES; it does not drop the element and continue.
//!
//! ## Which declared length reaches the width guard (the calibration trap)
//!
//! The map KEY is decoded by `parse_cell_path_key_reporting`, which for a
//! TOP-LEVEL fixed-width key type first applies its own allowed-widths table and
//! reports `Error::Corruption` — a TOLERATED class. The named
//! [`Error::FixedWidthLengthMismatch`] the fatal set is built on is reachable on
//! that path only for a NESTED fixed-width element, so the map cases below use
//! `map<frozen<list<int>>, int>` and a key whose single `int` element declares
//! length **3** (`[count=1][len=3][3B]`). 3 rather than 5 deliberately: an
//! over-long declaration can trip an outer framing bound first and surface as
//! `Corruption` instead of the named variant.
//!
//! The UDT field value is decoded by `parse_value_from_raw_bytes` with the
//! DECLARED field type, so a top-level 3-byte `int` reaches the guard directly.

use super::super::test_support::helpers::encode_unsigned;
use super::super::ElementShadow;
use super::*;
use crate::schema::Column;

/// Read clock used by every case here (epoch seconds); only the TTL cases care.
const NOW: i64 = 1_000_000;

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None)
}

fn column(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// A filter that SHADOWS every element whose write ts is `<= 100`. The cells
/// below encode timestamp delta 0 against `min_timestamp = 0`, so their
/// effective write ts is `0` and they are all shadowed.
fn shadow_everything() -> ElementShadow {
    ElementShadow {
        cover: Some(100),
        now: NOW,
        row_ts: None,
        row_expires_at: None,
        row_ttl_seconds: None,
    }
}

/// A filter with NO covering deletion: only an element carrying its own expiry
/// is dropped (the TTL cases).
fn expiry_only_filter() -> ElementShadow {
    ElementShadow {
        cover: None,
        now: NOW,
        row_ts: None,
        row_expires_at: None,
        row_ttl_seconds: None,
    }
}

/// One live complex cell carrying BOTH a path and a value, the shape a multicell
/// map entry and a multicell UDT field both have.
///
/// Wire order read by `parse_complex_cell_value`:
/// `[flags:u8][timestamp:VUInt][path_len:VUInt][path][value_len:VUInt][value]`
/// with `min_timestamp = 0` in the test parser, so the delta IS the value.
fn cell(path: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = vec![0x00u8, 0x00];
    encode_unsigned(path.len() as u64, &mut buf);
    buf.extend_from_slice(path);
    encode_unsigned(value.len() as u64, &mut buf);
    buf.extend_from_slice(value);
    buf
}

/// The same cell, EXPIRING (`IS_EXPIRING` 0x02) with an explicit
/// `localDeletionTime`/`ttl` of one second — i.e. long expired at [`NOW`].
///
/// `[flags][timestamp][localDeletionTime][ttl][path_len][path][value_len][value]`
fn expiring_cell(path: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = vec![0x02u8, 0x00, 0x01, 0x01];
    encode_unsigned(path.len() as u64, &mut buf);
    buf.extend_from_slice(path);
    encode_unsigned(value.len() as u64, &mut buf);
    buf.extend_from_slice(value);
    buf
}

/// `[cell_count][cells...]`, one byte of count (VUInt) for a small collection.
fn blob(cells: &[Vec<u8>]) -> Vec<u8> {
    assert!(cells.len() < 0x80, "single-byte VUInt only");
    let mut out = vec![cells.len() as u8];
    for c in cells {
        out.extend_from_slice(c);
    }
    out
}

/// A `frozen<list<int>>` cell-path key whose single element DECLARES `len`
/// bytes and carries exactly that many. `len == 4` is well formed.
fn frozen_list_int_key(len: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1i32.to_be_bytes());
    buf.extend_from_slice(&(len as i32).to_be_bytes());
    buf.extend_from_slice(&7i32.to_be_bytes()[..len.min(4)]);
    buf.resize(8 + len, 0);
    buf
}

/// Decode a whole complex column, returning the collapsed value and the number
/// of elements the #1741 filter dropped.
fn decode(
    column_name: &str,
    schema_type: &str,
    complex_type: &str,
    cells: &[Vec<u8>],
    filter: Option<ElementShadow>,
) -> Result<(Value, usize)> {
    let col = column(column_name, schema_type);
    parser()
        .parse_complex_column_inner(&blob(cells), 0, &col, complex_type, false, 0, None, filter)
        .map(|(value, _consumed, meta)| (value, meta.shadow_filtered_element_count))
}

const MAP_TYPE: &str = "map<frozen<list<int>>, int>";

fn decode_map(cells: &[Vec<u8>], filter: Option<ElementShadow>) -> Result<(Value, usize)> {
    decode("my_map", MAP_TYPE, MAP_TYPE, cells, filter)
}

/// `udt<f0 int>` in the AUTHORITATIVE marshal spelling the UDT branch requires.
fn udt_marshal_type() -> String {
    format!(
        "org.apache.cassandra.db.marshal.UserType(test,{},{}:org.apache.cassandra.db.marshal.Int32Type)",
        hex::encode("my_udt"),
        hex::encode("f0")
    )
}

fn decode_udt(cells: &[Vec<u8>], filter: Option<ElementShadow>) -> Result<(Value, usize)> {
    decode("my_udt_col", "my_udt", &udt_marshal_type(), cells, filter)
}

/// Field index 0 as the 2-byte signed-short cell path the UDT branch reads.
fn field_path(index: i16) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// THE hole: a dropped entry/field skipped the width guard entirely.
// ---------------------------------------------------------------------------

/// A shadowed MAP entry whose key declares a wrong nested width is REFUSED.
#[test]
fn wrong_width_map_key_is_refused_even_when_shadowed() {
    match decode_map(
        &[cell(&frozen_list_int_key(3), &7i32.to_be_bytes())],
        Some(shadow_everything()),
    ) {
        Err(Error::FixedWidthLengthMismatch {
            expected, actual, ..
        }) => assert_eq!((expected, actual), (4, 3)),
        other => panic!(
            "a shadowed map entry whose `int` key element declares 3 bytes must be refused \
             with the NAMED FixedWidthLengthMismatch, got {other:?}"
        ),
    }
}

/// The same for the OTHER drop reason: TTL expiry at the read clock.
#[test]
fn wrong_width_map_key_is_refused_even_when_ttl_expired() {
    match decode_map(
        &[expiring_cell(&frozen_list_int_key(3), &7i32.to_be_bytes())],
        Some(expiry_only_filter()),
    ) {
        Err(Error::FixedWidthLengthMismatch {
            expected, actual, ..
        }) => assert_eq!((expected, actual), (4, 3)),
        other => panic!(
            "a TTL-expired map entry whose `int` key element declares 3 bytes must be \
             refused with the NAMED FixedWidthLengthMismatch, got {other:?}"
        ),
    }
}

/// A shadowed UDT FIELD whose declared `int` value is 3 bytes is REFUSED.
#[test]
fn wrong_width_udt_field_is_refused_even_when_shadowed() {
    match decode_udt(
        &[cell(&field_path(0), &[0x00, 0x00, 0x07])],
        Some(shadow_everything()),
    ) {
        Err(Error::FixedWidthLengthMismatch {
            expected, actual, ..
        }) => assert_eq!((expected, actual), (4, 3)),
        other => panic!(
            "a shadowed UDT field whose declared `int` value is 3 bytes must be refused \
             with the NAMED FixedWidthLengthMismatch, got {other:?}"
        ),
    }
}

/// The same for the OTHER drop reason: TTL expiry at the read clock.
#[test]
fn wrong_width_udt_field_is_refused_even_when_ttl_expired() {
    match decode_udt(
        &[expiring_cell(&field_path(0), &[0x00, 0x00, 0x07])],
        Some(expiry_only_filter()),
    ) {
        Err(Error::FixedWidthLengthMismatch {
            expected, actual, ..
        }) => assert_eq!((expected, actual), (4, 3)),
        other => panic!(
            "a TTL-expired UDT field whose declared `int` value is 3 bytes must be refused \
             with the NAMED FixedWidthLengthMismatch, got {other:?}"
        ),
    }
}

// ---------------------------------------------------------------------------
// Negative controls: #1741 filtering is UNCHANGED.
// ---------------------------------------------------------------------------

/// A WELL-FORMED shadowed map entry still filters, with the same accounting.
/// The unfiltered decode is the control proving the entry was there to drop.
#[test]
fn a_well_formed_shadowed_map_entry_still_filters_with_the_same_accounting() {
    let cells = [cell(&frozen_list_int_key(4), &7i32.to_be_bytes())];

    let (value, filtered) = decode_map(&cells, None).expect("physical consumers filter nothing");
    assert_eq!(filtered, 0, "control: nothing is counted as dropped");
    match value {
        Value::Map(entries) => assert_eq!(entries.len(), 1, "control: the entry is present"),
        other => panic!("expected a map, got {other:?}"),
    }

    assert_eq!(
        decode_map(&cells, Some(shadow_everything()))
            .expect("a well-formed shadowed entry must still filter silently"),
        (Value::Map(vec![]), 1),
        "the shadowed entry is dropped and counted exactly as before #3723 round 5"
    );
}

/// A WELL-FORMED shadowed UDT field still filters, with the same accounting.
#[test]
fn a_well_formed_shadowed_udt_field_still_filters_with_the_same_accounting() {
    let cells = [cell(&field_path(0), &7i32.to_be_bytes())];

    let (value, filtered) = decode_udt(&cells, None).expect("physical consumers filter nothing");
    assert_eq!(filtered, 0, "control: nothing is counted as dropped");
    match &value {
        Value::Udt(u) => assert_eq!(
            u.fields[0].value,
            Some(Value::Integer(7)),
            "control: the field is present"
        ),
        other => panic!("expected a UDT, got {other:?}"),
    }

    let (value, filtered) = decode_udt(&cells, Some(shadow_everything()))
        .expect("a well-formed shadowed field must still filter silently");
    assert_eq!(filtered, 1, "the field is counted as dropped, as before");
    match &value {
        Value::Udt(u) => assert_eq!(
            u.fields[0].value, None,
            "the shadowed field is left absent, exactly as before #3723 round 5"
        ),
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// A dropped MAP entry whose key fails with a TOLERATED (non-fatal) class is
/// still filtered SILENTLY. Validating dropped entries must not promote a
/// tolerated error into a read failure: a top-level fixed-width key of the wrong
/// width is `Error::Corruption` (the allowed-widths table in `cell_path_key`),
/// which is NOT in the one-variant fatal set (`raw_value/fatal_decode_error.rs`).
#[test]
fn a_tolerated_map_key_failure_on_a_shadowed_entry_still_filters_silently() {
    assert_eq!(
        decode(
            "my_map",
            "map<int, int>",
            "map<int, int>",
            &[cell(&[0x00, 0x00, 0x07], &7i32.to_be_bytes())],
            Some(shadow_everything()),
        )
        .expect("a tolerated key failure on a dropped entry must not fail the read"),
        (Value::Map(vec![]), 1),
        "the entry is filtered and counted, and no error escapes"
    );
}

/// The UDT counterpart: a ZERO-length fixed-width field value is refused by the
/// width guard but stays TOLERATED, so a dropped field carrying one is still
/// filtered silently (`fatal_decode_error.rs`, "the zero-length half").
#[test]
fn a_zero_length_shadowed_udt_field_still_filters_silently() {
    let (value, filtered) = decode_udt(&[cell(&field_path(0), &[])], Some(shadow_everything()))
        .expect("a zero-length dropped field must not fail the read");
    assert_eq!(filtered, 1, "the field is counted as dropped, as before");
    match &value {
        Value::Udt(u) => assert_eq!(u.fields[0].value, None, "the field is left absent"),
        other => panic!("expected a UDT, got {other:?}"),
    }
}

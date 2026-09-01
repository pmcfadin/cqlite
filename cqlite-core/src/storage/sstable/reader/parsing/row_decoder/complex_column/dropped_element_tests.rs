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
//! ## Which declared length reaches the width guard — and the GAP that shapes it
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
//! State that the other way round, because the first way is how a real gap got
//! written down as a calibration note: a DIRECT fixed-width key (`map<int, …>`)
//! is NOT covered by the four refusal cases below, and the cases avoid it
//! BECAUSE it is not covered. That is the census's SIXTH tolerant site
//! (`raw_value/fatal_decode_error.rs`), open under **#3778**, and it is declared
//! and characterised — not endorsed — by
//! `wrong_width_direct_map_key_is_tolerated_today_known_gap_3778` below. So the
//! guard these cases pin is the NESTED one; the map branch's dropped-path
//! validation is complete only for key types that reach the fixed-width arm.
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

/// A DIRECT (top-level) fixed-width map key type, whose wrong-width refusal is
/// `Error::Corruption` and therefore never reaches the fatal set.
const DIRECT_MAP: &str = "map<int, int>";

fn decode_direct_map(key: &[u8], filter: Option<ElementShadow>) -> Result<(Value, usize)> {
    decode(
        "my_map",
        DIRECT_MAP,
        DIRECT_MAP,
        &[cell(key, &7i32.to_be_bytes())],
        filter,
    )
}

/// CHARACTERISATION of a KNOWN-TOLERATED GAP — the census's **SIXTH** site
/// (`raw_value/fatal_decode_error.rs`), open under **#3778**. This is **not
/// desired behaviour** and the assertions below are NOT a guard.
///
/// A map key whose declared type is fixed-width DIRECTLY (`map<int, …>`, not
/// `map<frozen<list<int>>, …>`) never reaches the fixed-width arm at all:
/// `parse_cell_path_key_reporting` applies its OWN allowed-widths table first
/// and reports [`Error::Corruption`], the general pre-existing TOLERATED class.
/// So a 3-byte `int` key is refused as a class every tolerant site absorbs —
/// including [`is_fatal_decode_error`]'s dropped-entry arm here, which lets the
/// entry be filtered and the read SUCCEED.
///
/// **Said plainly, because a previous round did not:** round 6's four refusal
/// cases above use `map<frozen<list<int>>, int>` precisely BECAUSE a direct
/// fixed-width key yields `Corruption` rather than the named variant. That was
/// recorded as a calibration fact and routed around; it is a GAP the tests were
/// steered past (roborev round 6, job 33), and this test is the declaration of
/// it rather than an endorsement.
///
/// It is not fixed here, and the reason is specific rather than a scope excuse:
/// the cause is the error VARIANT, not a missing guard, so closing it means
/// making a top-level cell-path width failure fatal — i.e. promoting a
/// `Corruption`. Assertion (a) below is why that is unsafe: the SAME bytes on
/// the LIVE path also refuse as `Corruption`, which census site 1
/// (`row_data.rs:614`) tolerates with a partial-row `break`. Renaming the
/// variant would turn that live read into a hard failure — the exact defect
/// already fixed once on this branch (the zero-length case), which had to be
/// reverted to restore the zero-regression property.
///
/// It fails in BOTH directions:
///
/// * if either decode starts returning the fatal variant (someone made this
///   fatal — a real behaviour change, which must be a deliberate #3778 commit
///   updating the census and this test together, not a side effect);
/// * if the dropped entry stops being filtered-and-counted exactly as it is
///   today (someone made the tolerance silently worse — e.g. surfacing the
///   entry under a salvaged key, which would additionally be a no-heuristics
///   violation, or changing the drop accounting).
#[test]
fn wrong_width_direct_map_key_is_tolerated_today_known_gap_3778() {
    // Anti-empty-pass control: a WELL-FORMED 4-byte key of the same declared
    // type decodes and surfaces. Without it every assertion below could hold
    // because a `map<int, int>` cell path never decodes at all.
    let (value, filtered) =
        decode_direct_map(&7i32.to_be_bytes(), None).expect("control: a well-formed direct key");
    assert_eq!(filtered, 0, "control: nothing is counted as dropped");
    assert_eq!(
        value,
        Value::Map(vec![(Value::Integer(7), Value::Integer(7))]),
        "control: the well-formed entry is present"
    );

    // (a) The wrong-width bytes ARE refused — but as the TOLERATED class, which
    // is the whole of the gap. Pinned on the LIVE path, where the refusal is
    // visible as an `Err`, because that is what makes promotion unsafe: site 1
    // absorbs this `Corruption` into a partial-row `break` today.
    match decode_direct_map(&[0x00, 0x00, 0x07], None) {
        Err(Error::Corruption(msg)) => assert!(
            msg.contains("of type 'int' requires exactly 0 or 4 bytes, got 3"),
            "the refusal must come from cell_path_key's allowed-widths table: {msg}"
        ),
        other => panic!(
            "KNOWN-TOLERATED GAP (#3778): a 3-byte direct `int` map key refuses as \
             Error::Corruption today, NOT as the fatal FixedWidthLengthMismatch. If \
             this changed, the live path at row_data.rs:614 now FAILS reads it used \
             to serve — update the census in raw_value/fatal_decode_error.rs and this \
             test together. Got {other:?}"
        ),
    }

    // (b) And on the DROPPED path that tolerated class is absorbed, so the read
    // SUCCEEDS with the malformed entry silently filtered.
    assert_eq!(
        decode_direct_map(&[0x00, 0x00, 0x07], Some(shadow_everything())).expect(
            "KNOWN-TOLERATED GAP (#3778): a dropped entry whose DIRECT fixed-width key \
             has the wrong width is silently accepted today, because the refusal is \
             Error::Corruption rather than the fatal variant"
        ),
        (Value::Map(vec![]), 1),
        "KNOWN-TOLERATED GAP (#3778): the malformed entry is filtered and counted and \
         no error escapes. Not desired behaviour — characterised so a change of \
         disposition in EITHER direction is visible"
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

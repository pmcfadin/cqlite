//! CHARACTERISATION of the #1741 per-element shadow/TTL filter in the
//! multicell complex-column loop — set members, map entries and UDT fields —
//! and of the ONE width gap that filter still leaves open (issue #3723).
//!
//! ## What these cases are, and are not
//!
//! Issue #3723 set out to make a wrong-width fixed-width element FATAL and to
//! validate it even when the filter is about to drop it. Issue **#3811** landed
//! first and enforced the width property one layer down, inside
//! `raw_value::parse_value_from_raw_bytes`, as the pre-existing TOLERATED
//! `Error::Corruption` class rather than a new fatal variant — so #3723's own
//! mechanism was superseded and removed. What survives here is the
//! CHARACTERISATION half: these cases pin TODAY's filtering behaviour exactly,
//! including the part that is still wrong, so any change of disposition is
//! visible in a diff rather than silent.
//!
//! Nothing below is a guard for desired behaviour except the negative controls.
//! The gap is tracked as **#3778**; the width property itself is pinned by
//! `raw_value/nested_fixed_width_length_tests.rs`.
//!
//! ## The gap, stated plainly
//!
//! An element the filter DROPS is `continue`d before any decode runs, so its
//! bytes are never width-validated: a malformed fixed-width element is silently
//! filtered rather than refused, purely because some OTHER cell shadows it or
//! its own TTL has expired.
//!
//! And even on the LIVE path the refusal does not reach a read: it is
//! `Error::Corruption`, which the complex-column caller in `row_data.rs`
//! absorbs into a partial-row `break`. So the disposition is TOLERATED in both
//! directions today.
//!
//! Oracle for why that is wrong — pinned `cassandra-5.0.8`, never CQLite's own
//! prior output:
//! * `schema/ColumnMetadata.java` `validateCell(...)` validates the cell VALUE
//!   and then, unconditionally, `validateCellPath(cell.path())` for every live
//!   cell it is handed. Nothing in that validation consults a covering deletion
//!   or the read clock — reconciliation (`db/rows/Cells.java`) is a separate,
//!   LATER concern from whether the bytes are well formed.
//! * `serializers/Int32Serializer.java` `validate(...)` throws
//!   `"Expected 4 or 0 byte int (%d)"`; `serializers/ListSerializer.java` and
//!   `SetSerializer.java` let that element-level `MarshalException` escape for
//!   the whole value. Cassandra REFUSES; it does not drop the element and
//!   continue.

use super::super::test_support::helpers::{build_set_cell_bytes, encode_unsigned};
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

/// One EXPIRING live SET cell (`HAS_EMPTY_VALUE | IS_EXPIRING`) whose explicit
/// `localDeletionTime`/`ttl` is one second — i.e. long expired at [`NOW`].
///
/// `[flags][timestamp][localDeletionTime][ttl][path_len][path]`
fn expiring_set_cell(path: &[u8]) -> Vec<u8> {
    assert!(path.len() < 0x80, "single-byte VUInt only");
    let mut buf = vec![0x04u8 | 0x02u8, 0x00, 0x01, 0x01, path.len() as u8];
    buf.extend_from_slice(path);
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

fn decode_set(cells: &[Vec<u8>], filter: Option<ElementShadow>) -> Result<(Value, usize)> {
    decode("my_set", "set<int>", "set<int>", cells, filter)
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
// Negative controls: the filter drops exactly what it should, and counts it.
// ---------------------------------------------------------------------------

/// A WELL-FORMED shadowed map entry filters, with the drop counted. The
/// unfiltered decode is the control proving the entry was there to drop.
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
        "the shadowed entry is dropped and counted"
    );
}

/// A WELL-FORMED shadowed UDT field filters, with the drop counted.
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
    assert_eq!(filtered, 1, "the field is counted as dropped");
    match &value {
        Value::Udt(u) => assert_eq!(u.fields[0].value, None, "the shadowed field is left absent"),
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// A WELL-FORMED shadowed SET member filters, with the drop counted.
#[test]
fn a_well_formed_shadowed_set_member_still_filters_with_the_same_accounting() {
    let cells = [build_set_cell_bytes(&7i32.to_be_bytes())];

    assert_eq!(
        decode_set(&cells, None).expect("physical consumers filter nothing"),
        (Value::Set(vec![Value::Integer(7)]), 0),
        "control: with no filter the member is present and nothing is counted as dropped"
    );

    assert_eq!(
        decode_set(&cells, Some(shadow_everything()))
            .expect("a well-formed shadowed member must still filter silently"),
        (Value::Set(vec![]), 1),
        "the shadowed member is dropped and counted"
    );
}

/// A shadowed set member alongside a LIVE one: only the shadowed member goes,
/// the live member survives, and the count is 1. This is the case that breaks
/// if the element loop's filtering is ever restructured.
#[test]
fn only_the_shadowed_member_is_filtered_from_a_mixed_set() {
    // The live member carries USE_ROW_TIMESTAMP (0x08) with `row_ts = None` in
    // the filter, so it has no authoritative write ts and is NEVER shadowed
    // (no-heuristics, #1741) — while the shadowed member below carries its own
    // ts of 0, which the cover at 100 shadows.
    let live = {
        let mut c = vec![0x04u8 | 0x08u8, 0x04];
        c.extend_from_slice(&9i32.to_be_bytes());
        c
    };
    let shadowed = build_set_cell_bytes(&7i32.to_be_bytes());

    assert_eq!(
        decode_set(&[shadowed, live], Some(shadow_everything())).expect("a mixed set must decode"),
        (Value::Set(vec![Value::Integer(9)]), 1),
        "exactly the shadowed member is dropped; the live member is untouched"
    );
}

// ---------------------------------------------------------------------------
// The GAP: a dropped element's bytes are never width-validated (#3778).
// ---------------------------------------------------------------------------

/// CHARACTERISATION of a KNOWN GAP, open under **#3778** — **not desired
/// behaviour**, and the assertions here are NOT a guard for it.
///
/// A wrong-width fixed-width element that the #1741 filter drops is accepted
/// silently, in all three branches and for both drop reasons, because the
/// `continue` runs before any decode. Each case carries its own live-path
/// control: the SAME bytes unfiltered, showing the width violation is real and
/// that #3811 does refuse it once something looks.
///
/// It fails in BOTH directions:
///
/// * if a dropped element starts being refused (someone closed #3778 — a real
///   behaviour change that must update this test in the same commit);
/// * if the LIVE path stops refusing the same bytes (someone loosened #3811's
///   composed width rule).
#[test]
fn a_wrong_width_dropped_element_is_not_validated_today_known_gap_3778() {
    // (set, shadowed) — a 3-byte `int` member.
    let short_member = build_set_cell_bytes(&[0x00, 0x00, 0x07]);
    assert!(
        decode_set(std::slice::from_ref(&short_member), None).is_err(),
        "live-path control: a 3-byte `int` set member IS refused by #3811's width rule"
    );
    assert_eq!(
        decode_set(&[short_member], Some(shadow_everything()))
            .expect("KNOWN GAP (#3778): a shadowed member's path is never width-validated today"),
        (Value::Set(vec![]), 1),
        "KNOWN GAP (#3778): the malformed member is filtered and counted, no error escapes"
    );

    // (set, TTL-expired) — the other drop reason, same outcome.
    let expired_member = expiring_set_cell(&[0x00, 0x00, 0x07]);
    assert_eq!(
        decode_set(&[expired_member], Some(expiry_only_filter()))
            .expect("KNOWN GAP (#3778): a TTL-expired member is not width-validated either"),
        (Value::Set(vec![]), 1),
        "KNOWN GAP (#3778): expiry is not a licence to skip the width rule"
    );

    // (map, shadowed) — a key whose single nested `int` element declares 3 bytes.
    let short_key_entry = cell(&frozen_list_int_key(3), &7i32.to_be_bytes());
    assert!(
        decode_map(std::slice::from_ref(&short_key_entry), None).is_err(),
        "live-path control: a 3-byte nested `int` map key IS refused"
    );
    assert_eq!(
        decode_map(&[short_key_entry], Some(shadow_everything()))
            .expect("KNOWN GAP (#3778): a shadowed entry's key is never width-validated today"),
        (Value::Map(vec![]), 1),
        "KNOWN GAP (#3778): the malformed entry is filtered and counted"
    );

    // (UDT, shadowed) — a 3-byte `int` field value.
    let short_field = cell(&field_path(0), &[0x00, 0x00, 0x07]);
    assert!(
        decode_udt(std::slice::from_ref(&short_field), None).is_err(),
        "live-path control: a 3-byte `int` UDT field value IS refused"
    );
    let (value, filtered) = decode_udt(&[short_field], Some(shadow_everything()))
        .expect("KNOWN GAP (#3778): a shadowed field's value is never width-validated today");
    assert_eq!(
        filtered, 1,
        "KNOWN GAP (#3778): the field is counted as dropped"
    );
    match &value {
        Value::Udt(u) => assert_eq!(u.fields[0].value, None, "the field is left absent"),
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// A DIRECT (top-level) fixed-width map key type, whose wrong-width refusal
/// comes from `cell_path_key`'s own allowed-widths table.
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

/// CHARACTERISATION of the same gap for a DIRECT fixed-width map key, kept
/// separate because the refusal comes from a DIFFERENT layer — **not desired
/// behaviour**, open under **#3778**.
///
/// A `map<int, …>` key never reaches `raw_value`'s fixed-width arm at all:
/// `parse_cell_path_key_reporting` applies its OWN allowed-widths table first
/// and reports [`Error::Corruption`] with its own wording. Assertion (a) pins
/// that wording so a change of layer is visible; (b) pins that the dropped path
/// still accepts the same bytes silently.
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

    // (a) The wrong-width bytes ARE refused on the LIVE path, by cell_path_key's
    // table rather than by #3811's composed rule.
    match decode_direct_map(&[0x00, 0x00, 0x07], None) {
        Err(Error::Corruption(msg)) => assert!(
            msg.contains("of type 'int' requires exactly 0 or 4 bytes, got 3"),
            "the refusal must come from cell_path_key's allowed-widths table: {msg}"
        ),
        other => panic!(
            "a 3-byte direct `int` map key refuses through cell_path_key's own table \
             today. If this changed, update this test and the module header \
             together. Got {other:?}"
        ),
    }

    // (b) And on the DROPPED path nothing looks at all, so the read SUCCEEDS
    // with the malformed entry silently filtered.
    assert_eq!(
        decode_direct_map(&[0x00, 0x00, 0x07], Some(shadow_everything())).expect(
            "KNOWN GAP (#3778): a dropped entry whose DIRECT fixed-width key has the \
             wrong width is silently accepted today"
        ),
        (Value::Map(vec![]), 1),
        "KNOWN GAP (#3778): the malformed entry is filtered and counted and no error \
         escapes. Not desired behaviour — characterised so a change of disposition in \
         EITHER direction is visible"
    );
}

// ---------------------------------------------------------------------------
// Zero-length and tolerated-class elements on the dropped path.
// ---------------------------------------------------------------------------

/// A dropped UDT field whose value is ZERO-length is filtered silently. #3811's
/// composed rule refuses a zero-length `int` on the live path (`#3847` records
/// that this is narrower than Cassandra), but the dropped path never asks.
#[test]
fn a_zero_length_shadowed_udt_field_still_filters_silently() {
    let (value, filtered) = decode_udt(&[cell(&field_path(0), &[])], Some(shadow_everything()))
        .expect("a zero-length dropped field must not fail the read");
    assert_eq!(filtered, 1, "the field is counted as dropped");
    match &value {
        Value::Udt(u) => assert_eq!(u.fields[0].value, None, "the field is left absent"),
        other => panic!("expected a UDT, got {other:?}"),
    }
}

/// A dropped SET member with a ZERO-length path is filtered silently.
#[test]
fn a_zero_length_shadowed_set_member_still_filters_silently() {
    assert_eq!(
        decode_set(&[build_set_cell_bytes(&[])], Some(shadow_everything()))
            .expect("a zero-length path on a dropped member must not fail the read"),
        (Value::Set(vec![]), 1),
        "the member is filtered and counted"
    );
}

/// A dropped SET member whose path fails for a NON-width reason (invalid UTF-8
/// for a `set<text>`) is also filtered silently — the tolerance is not specific
/// to widths.
#[test]
fn a_tolerated_path_failure_on_a_shadowed_member_still_filters_silently() {
    assert_eq!(
        decode(
            "my_set",
            "set<text>",
            "set<text>",
            &[build_set_cell_bytes(&[0x80])],
            Some(shadow_everything())
        )
        .expect("a non-width path failure on a dropped member must not fail the read"),
        (Value::Set(vec![]), 1),
        "the member is filtered and counted, and no error escapes"
    );
}

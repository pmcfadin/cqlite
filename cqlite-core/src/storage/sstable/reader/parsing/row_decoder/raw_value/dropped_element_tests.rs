//! CHARACTERISATION of the #1741 per-element shadow/TTL filter in the
//! multicell complex-column loop — its LIST, SET, MAP and UDT branches — and of
//! the ONE width gap that filter still leaves open (issue #3723; the LIST
//! branch was added by issue #4034, which closed this header's earlier
//! declaration that it was uncovered).
//!
//! ## What these cases are, and are not
//!
//! Issue #3723 set out to make a wrong-width fixed-width element FATAL and to
//! validate it even when the filter is about to drop it. Issue **#3811** landed
//! first and enforced the width property one layer down, inside
//! `raw_value::parse_value_from_raw_bytes`, reporting the pre-existing
//! `Error::Corruption` rather than adding a new fatal variant — so #3723's own
//! mechanism was superseded and removed. What survives here is the
//! CHARACTERISATION half: these cases pin TODAY's filtering behaviour exactly,
//! including the part that is still wrong, so any change of disposition is
//! visible in a diff rather than silent.
//!
//! Nothing below is a guard for desired behaviour except the negative controls.
//! The gap is tracked as **#3778**; the width property itself is pinned by
//! `raw_value/nested_fixed_width_length_tests.rs`.
//!
//! ## The gap, stated plainly — and the branch it does NOT reach
//!
//! An element the filter DROPS is `continue`d before its branch's OWN element
//! decode runs, so its bytes are never width-validated: a malformed
//! fixed-width element is silently filtered rather than refused, purely because
//! some OTHER cell shadows it or its own TTL has expired.
//!
//! That is a claim about the elements whose decode is DEFERRED past the
//! `continue`, and only those: the SET member and the MAP key (both live in the
//! cell PATH, decoded in the branch body) and the UDT field (whose value the
//! loop reads as `BytesType` and re-decodes per field afterwards). It does NOT
//! reach the LIST element, which lives in the cell VALUE and which
//! `parse_complex_cell_value` decodes with the declared element type BEFORE
//! `element_dropped` is ever consulted — so a wrong-width dropped list element
//! is REFUSED. That divergence is pinned by
//! `a_wrong_width_dropped_list_element_is_still_refused_unlike_set_map_and_udt`,
//! which carries the mechanism (issue #4034). A malformed multicell map VALUE
//! is refused on both paths for the same reason and is not covered here — the
//! map case below characterises its KEY.
//!
//! The LIVE path is NOT part of that gap, and the tolerance is therefore
//! ONE-DIRECTIONAL — the DROPPED path only. A wrong-width element that is not
//! dropped surfaces `Error::Corruption`, and `row_data.rs`'s complex-column
//! caller PROPAGATES it: the `Err(e)` arm of its `parse_complex_column_inner`
//! match returns `column_decode_failure(...)` under #3721's
//! `column_decode_error` policy. The partial-row `break` that used to absorb it
//! there was removed by #3721.
//!
//! That paragraph is a claim about a DIFFERENT file, and nothing mechanically
//! couples it to that file: re-read the `Err(e)` arm before trusting it. It has
//! already decayed once — it asserted the absorbing `break` for as long as it
//! took #3721 to land — which is the drifted-mechanism class this module exists
//! to make visible in a diff.
//!
//! What the cases below assert about either direction is deliberately narrower
//! than that: every assertion is taken at `parse_complex_column_inner`, so they
//! pin `Err` on the live path and `Ok` on the dropped one and say nothing about
//! what the row-level caller ultimately reports.
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
// LIST-branch fixtures (issue #4034).
//
// A multicell LIST does NOT key its elements by value the way a SET does: the
// cell PATH is a generated 16-byte TimeUUID carrying only insertion order, and
// the element itself lives in the cell VALUE, unsigned-VInt length-prefixed
// even for a fixed-width element type. Authority is the on-disk framing table
// and the verbatim `test_collections/collection_table` bytes in
// `docs/sstables-definitive-guide/chapters/05-data-db-format.md` ("Cell Path
// and Value by Collection Type" / "List Element Ordering") — the path below is
// literally cell 1's TimeUUID from that dump. So `build_set_cell_bytes` cannot
// build a list element (it sets `HAS_EMPTY_VALUE` and writes no value at all)
// and there is no list analogue of it; the generic [`cell`] helper, which
// writes `[flags][ts][path_len][path][value_len][value]`, is the right shape.
// ---------------------------------------------------------------------------

/// The 16-byte TimeUUID cell path of a multicell list element. Its CONTENT is
/// inert to every case here — the list arm never decodes a list cell path (it
/// only records it for the compaction contract) — so one fixed literal is
/// enough, and it is a real Cassandra-written TimeUUID rather than a made-up
/// one so the fixture's framing is the framing on disk.
fn list_element_path() -> Vec<u8> {
    vec![
        0x79, 0xf2, 0xa0, 0x80, 0xa2, 0x51, 0x11, 0xf0, 0xa3, 0xfe, 0xf1, 0xa5, 0x51, 0x38, 0x3f,
        0xb9,
    ]
}

/// One live multicell list element carrying `value` in the cell VALUE, with an
/// EXPLICIT timestamp delta of 0 (so [`shadow_everything`] shadows it — an
/// element that instead carried `USE_ROW_TIMESTAMP` would have no
/// authoritative write ts and could never be shadowed, per #1741).
fn list_cell(value: &[u8]) -> Vec<u8> {
    cell(&list_element_path(), value)
}

/// One EXPIRING live list element (`IS_EXPIRING`, and deliberately NOT
/// `HAS_EMPTY_VALUE`, so a value still follows) whose explicit
/// `localDeletionTime`/`ttl` is one second — i.e. long expired at [`NOW`].
///
/// `[flags][timestamp][localDeletionTime][ttl][path_len][path][value_len][value]`
fn expiring_list_cell(value: &[u8]) -> Vec<u8> {
    let path = list_element_path();
    assert!(
        path.len() < 0x80 && value.len() < 0x80,
        "single-byte VUInt only"
    );
    let mut buf = vec![0x02u8, 0x00, 0x01, 0x01, path.len() as u8];
    buf.extend_from_slice(&path);
    buf.push(value.len() as u8);
    buf.extend_from_slice(value);
    buf
}

/// One live list element flagged `HAS_EMPTY_VALUE` (0x04): no value length and
/// no value bytes follow at all, which is how Cassandra writes a zero-length
/// element. Contrast an explicit `value_len = 0`, which [`list_cell`] with an
/// empty slice produces and which is a DIFFERENT encoding — see
/// `a_zero_length_shadowed_list_element_filters_only_under_has_empty_value`.
fn empty_value_list_cell() -> Vec<u8> {
    let path = list_element_path();
    assert!(path.len() < 0x80, "single-byte VUInt only");
    let mut buf = vec![0x04u8, 0x00, path.len() as u8];
    buf.extend_from_slice(&path);
    buf
}

fn decode_list(cells: &[Vec<u8>], filter: Option<ElementShadow>) -> Result<(Value, usize)> {
    decode("my_list", "list<int>", "list<int>", cells, filter)
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

/// A WELL-FORMED shadowed LIST element filters, with the drop counted
/// (issue #4034). This is the LIST counterpart of the three cases above; it is
/// the case that establishes the filter reaches the list arm AT ALL, so it is
/// also the control the divergence case below leans on.
#[test]
fn a_well_formed_shadowed_list_element_still_filters_with_the_same_accounting() {
    let cells = [list_cell(&7i32.to_be_bytes())];

    assert_eq!(
        decode_list(&cells, None).expect("physical consumers filter nothing"),
        (Value::List(vec![Value::Integer(7)]), 0),
        "control: with no filter the element is present and nothing is counted as dropped"
    );

    assert_eq!(
        decode_list(&cells, Some(shadow_everything()))
            .expect("a well-formed shadowed element must still filter silently"),
        (Value::List(vec![]), 1),
        "the shadowed element is dropped and counted"
    );
}

/// A WELL-FORMED TTL-EXPIRED LIST element filters, with the drop counted
/// (issue #4034) — the OTHER drop reason, reached with no covering deletion at
/// all. Its own no-filter control proves the element was there to drop, so the
/// case cannot pass because the cell simply failed to parse.
#[test]
fn a_well_formed_ttl_expired_list_element_still_filters_with_the_same_accounting() {
    let cells = [expiring_list_cell(&7i32.to_be_bytes())];

    assert_eq!(
        decode_list(&cells, None).expect("physical consumers filter nothing"),
        (Value::List(vec![Value::Integer(7)]), 0),
        "control: with no filter the expiring element is present and nothing is dropped"
    );

    assert_eq!(
        decode_list(&cells, Some(expiry_only_filter()))
            .expect("a well-formed TTL-expired element must still filter silently"),
        (Value::List(vec![]), 1),
        "the expired element is dropped and counted, with no covering deletion involved"
    );
}

/// A shadowed list element alongside a LIVE one: only the shadowed element
/// goes, the live element survives, and the count is 1 (issue #4034) — the LIST
/// counterpart of [`only_the_shadowed_member_is_filtered_from_a_mixed_set`],
/// and what distinguishes PER-ELEMENT filtering in the list arm from dropping
/// the whole column.
#[test]
fn only_the_shadowed_element_is_filtered_from_a_mixed_list() {
    // The live element carries USE_ROW_TIMESTAMP (0x08) with `row_ts = None` in
    // the filter, so it has no authoritative write ts and is NEVER shadowed
    // (no-heuristics, #1741). Unlike the set fixture its value still follows
    // the path, because a list element lives in the cell VALUE.
    let live = {
        let path = list_element_path();
        assert!(path.len() < 0x80, "single-byte VUInt only");
        let mut c = vec![0x08u8, path.len() as u8];
        c.extend_from_slice(&path);
        c.push(0x04);
        c.extend_from_slice(&9i32.to_be_bytes());
        c
    };
    let shadowed = list_cell(&7i32.to_be_bytes());

    assert_eq!(
        decode_list(&[shadowed, live], Some(shadow_everything()))
            .expect("a mixed list must decode"),
        (Value::List(vec![Value::Integer(9)]), 1),
        "exactly the shadowed element is dropped; the live element is untouched"
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
/// silently, because the `continue` runs before these branches' own element
/// decode. Coverage HERE is the SET, MAP and UDT branches for the SHADOWED
/// reason, plus the SET branch for the TTL-EXPIRED reason. The LIST branch is
/// NOT one of them and is not an omission: it REFUSES instead, under
/// `a_wrong_width_dropped_list_element_is_still_refused_unlike_set_map_and_udt`
/// (issue #4034), which covers both drop reasons. So of the 8 branch x reason
/// combinations, 6 are characterised across the two cases and the two still
/// uncovered are (MAP, TTL-expired) and (UDT, TTL-expired) — neither case is a
/// cross product.
///
/// Each case carries a live-path control: the same bytes decoded with NO filter
/// are refused. The controls assert `is_err()` only — they show that SOMETHING
/// refuses those bytes once a decode looks at them, and deliberately do not pin
/// WHICH layer refused (contrast the map-key case below, which matches the
/// message and so does pin its layer). Note the TTL control cannot use bytes
/// identical to the shadowed cases': `expiring_set_cell` is a different
/// encoding, so it re-asserts the refusal on its OWN bytes.
///
/// It fails in BOTH directions:
///
/// * if a dropped element starts being refused (someone closed #3778 — a real
///   behaviour change that must update this test in the same commit);
/// * if the LIVE path stops refusing those bytes (someone loosened the composed
///   width rule).
#[test]
fn a_wrong_width_dropped_element_is_not_validated_today_known_gap_3778() {
    // (set, shadowed) — a 3-byte `int` member.
    let short_member = build_set_cell_bytes(&[0x00, 0x00, 0x07]);
    assert!(
        decode_set(std::slice::from_ref(&short_member), None).is_err(),
        "live-path control: a 3-byte `int` set member IS refused — by the `int` arm's own under-width `require_fixed_width`, which #3811 made PROPAGATE out of the set-member path"
    );
    assert_eq!(
        decode_set(&[short_member], Some(shadow_everything()))
            .expect("KNOWN GAP (#3778): a shadowed member's path is never width-validated today"),
        (Value::Set(vec![]), 1),
        "KNOWN GAP (#3778): the malformed member is filtered and counted, no error escapes"
    );

    // (set, TTL-expired) — the other drop reason, same outcome. Carries its own
    // live-path control (roborev r13): without it this case shows only that an
    // expired member is dropped, not that the SAME bytes would otherwise be
    // REFUSED — which is the whole claim.
    let expired_member = expiring_set_cell(&[0x00, 0x00, 0x07]);
    assert!(
        decode_set(std::slice::from_ref(&expired_member), None).is_err(),
        "live-path control: the same TTL-expired member's 3-byte `int` IS refused when nothing drops it"
    );
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

/// CHARACTERISATION of the LIST branch, which **DIVERGES** from SET, MAP and
/// UDT on exactly this axis: a wrong-width dropped list element is **REFUSED**,
/// so the #3778 gap above does NOT reach it (issue #4034).
///
/// Why, mechanically — established by reading `complex_column.rs`, not inferred
/// from this outcome: every branch calls `parse_complex_cell_value` FIRST, and
/// that function decodes the cell VALUE (its step 6) with the `element_type` it
/// was handed, BEFORE the loop body ever consults `element_dropped`. What the
/// branch passes there decides whether the gap can exist:
///
/// * LIST passes the DECLARED element type (`list<int>` ⇒ `int`), and the list
///   element lives in the cell VALUE — so it is width-validated eagerly, on
///   both paths, and no `continue` can skip it.
/// * SET passes the element type too, but a live set member sets
///   `HAS_EMPTY_VALUE`, so `parse_complex_cell_value` decodes nothing; the
///   member is decoded from the cell PATH in the branch body, AFTER the
///   `continue`.
/// * MAP passes the VALUE type, and the case above characterises its KEY, which
///   is the cell PATH and is likewise decoded after the `continue`. (A
///   malformed multicell map VALUE is therefore refused on both paths for the
///   same reason LIST is — not covered here.)
/// * UDT passes `BytesType` (identity) and re-decodes each field with its
///   declared type afterwards, again past the `continue`.
///
/// This case is NOT a guard for desired behaviour either, but it is the
/// direction Cassandra takes: `schema/ColumnMetadata.java` `validateCell(...)`
/// validates a live cell's bytes without consulting a covering deletion or the
/// read clock. So the LIST branch happens to be the CORRECT one on this axis
/// and #3778's remedy for the other three is to make them behave like it.
///
/// It fails in BOTH directions:
///
/// * if a dropped list element starts being FILTERED silently (someone deferred
///   the list value decode past the `continue`, extending #3778 to a fourth
///   branch — a real behaviour change that must update this test and the module
///   header in the same commit);
/// * if the well-formed controls stop filtering (the #1741 filter no longer
///   reaches the list arm, which would make the refusals below vacuous).
///
/// # Falsification, measured rather than asserted
///
/// A test that pins an absence has to be shown to red when the absence ends.
/// MEASURED on this commit: replace `&element_type` with `"blob"` in the list
/// arm's `parse_complex_cell_value` call in `complex_column.rs` — which is the
/// #3778 shape, a list element decode deferred past the `continue` — and all
/// FIVE list cases in this module fail (`9 passed; 5 failed`), this one on its
/// `live-path control` line, because the element type is what carries the width
/// rule on BOTH paths: deferring the decode removes the live refusal too. So
/// the mutation is a check on vacuity, not a per-arm isolation; the dropped-path
/// `panic!` arm's own wording is what a genuine one-sided change would print.
#[test]
fn a_wrong_width_dropped_list_element_is_still_refused_unlike_set_map_and_udt() {
    // Anti-vacuity control: the SAME cell shape with a WELL-FORMED 4-byte value
    // IS dropped by each filter below. Without it every refusal here could hold
    // because the element was never a drop candidate in the first place, which
    // is the whole claim (compare the live-path controls in the gap case above,
    // which make the mirror-image argument for the other three branches).
    assert_eq!(
        decode_list(&[list_cell(&7i32.to_be_bytes())], Some(shadow_everything()))
            .expect("control: a well-formed shadowed element filters"),
        (Value::List(vec![]), 1),
        "control: the covering deletion DOES drop an element of this shape"
    );
    assert_eq!(
        decode_list(
            &[expiring_list_cell(&7i32.to_be_bytes())],
            Some(expiry_only_filter())
        )
        .expect("control: a well-formed TTL-expired element filters"),
        (Value::List(vec![]), 1),
        "control: expiry DOES drop an element of this shape"
    );

    // (list, shadowed) — a 3-byte `int` element. Refused, not filtered. The
    // message pins the LAYER (#3811's composed width rule reached through
    // `parse_complex_cell_value`'s eager value decode), the same way the
    // direct-map-key case below pins `cell_path_key`'s table, so a change of
    // layer is visible in a diff rather than silent.
    let short = list_cell(&[0x00, 0x00, 0x07]);
    assert!(
        decode_list(std::slice::from_ref(&short), None).is_err(),
        "live-path control: a 3-byte `int` list element IS refused"
    );
    match decode_list(std::slice::from_ref(&short), Some(shadow_everything())) {
        Err(Error::Corruption(msg)) => assert!(
            msg.contains("need 4 byte(s) for int, got 3"),
            "the shadowed element must still be refused by the composed width rule: {msg}"
        ),
        other => panic!(
            "a 3-byte `int` list element is refused on the DROPPED path too today, \
             because the list value decode runs before the filter. If this changed, \
             #3778 now reaches the list branch — update this test and the module \
             header together. Got {other:?}"
        ),
    }

    // (list, TTL-expired) — the other drop reason, same refusal.
    let short_expiring = expiring_list_cell(&[0x00, 0x00, 0x07]);
    assert!(
        decode_list(std::slice::from_ref(&short_expiring), None).is_err(),
        "live-path control: the same TTL-expiring element's 3-byte `int` IS refused"
    );
    match decode_list(
        std::slice::from_ref(&short_expiring),
        Some(expiry_only_filter()),
    ) {
        Err(Error::Corruption(msg)) => assert!(
            msg.contains("need 4 byte(s) for int, got 3"),
            "the TTL-expired element must still be refused by the composed width rule: {msg}"
        ),
        other => panic!(
            "a 3-byte `int` list element is refused on the TTL-expired path too today. \
             If this changed, update this test and the module header together. \
             Got {other:?}"
        ),
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

/// A dropped LIST element that is ZERO-LENGTH filters silently ONLY when the
/// cell carries `HAS_EMPTY_VALUE`; an explicit `value_len = 0` is refused on
/// both paths (issue #4034).
///
/// The two are DIFFERENT encodings of "empty" and the flag is what decides,
/// because `HAS_EMPTY_VALUE` makes `parse_complex_cell_value` skip its eager
/// value decode entirely (`value = None`), so nothing looks at the bytes and
/// the `continue` is reached — while an explicit zero length still reaches
/// #3811's composed rule, whose accepted set for `int` is exactly `{4}`
/// (`#3847` records that Cassandra's is `{4, 0}`, i.e. the LIVE-path refusal
/// here is narrower than Cassandra's — that is a separate, pre-existing gap and
/// not this module's subject).
///
/// So the LIST counterpart of
/// [`a_zero_length_shadowed_udt_field_still_filters_silently`] holds for one
/// framing and not the other, which is worth pinning precisely rather than
/// forcing into the other branches' shape.
#[test]
fn a_zero_length_shadowed_list_element_filters_only_under_has_empty_value() {
    // (a) HAS_EMPTY_VALUE: no value bytes exist, so the drop is silent.
    assert_eq!(
        decode_list(&[empty_value_list_cell()], None)
            .expect("control: an empty-value element parses with no filter"),
        (Value::List(vec![]), 0),
        "control: an empty-value element contributes no member and is not counted as dropped"
    );
    assert_eq!(
        decode_list(&[empty_value_list_cell()], Some(shadow_everything()))
            .expect("a HAS_EMPTY_VALUE dropped element must not fail the read"),
        (Value::List(vec![]), 1),
        "the element is filtered and counted, and no error escapes"
    );

    // (b) An EXPLICIT `value_len = 0` for a declared `int` is refused, dropped
    // path included — the same divergence as the wrong-width case above, for
    // the same reason.
    let explicit_zero = list_cell(&[]);
    assert!(
        decode_list(std::slice::from_ref(&explicit_zero), None).is_err(),
        "live-path control: an explicitly zero-length `int` element IS refused"
    );
    match decode_list(
        std::slice::from_ref(&explicit_zero),
        Some(shadow_everything()),
    ) {
        Err(Error::Corruption(msg)) => assert!(
            msg.contains("need 4 byte(s) for int, got 0"),
            "the shadowed zero-length element must still be refused: {msg}"
        ),
        other => panic!(
            "an explicitly zero-length `int` list element is refused on the DROPPED \
             path too today. If this changed, update this test and the module header \
             together. Got {other:?}"
        ),
    }
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

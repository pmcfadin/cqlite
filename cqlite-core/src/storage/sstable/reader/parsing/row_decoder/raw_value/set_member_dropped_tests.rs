//! Issue #3723 (review round 3): a multicell-`set` member that the #1741
//! per-element shadow/TTL filter DROPS is still width-validated.
//!
//! Before this round a shadowed or TTL-expired live member `continue`d ahead of
//! `decode_set_member`, so its cell path never reached the fixed-width guard: a
//! malformed member was silently filtered instead of refused, purely because
//! some OTHER cell shadowed it. These cases pin the refusal AND pin that the
//! #1741 filtering itself did not change.
//!
//! Oracle — pinned `cassandra-5.0.8`, never CQLite's prior output:
//! * `serializers/Int32Serializer.java` `validate(...)` throws
//!   `"Expected 4 or 0 byte int (%d)"` for any other length, and
//!   `serializers/SetSerializer.java` `validate(...)` lets that `MarshalException`
//!   escape for the whole value — Cassandra refuses, it does not drop the element.
//! * `schema/ColumnMetadata.java` `validateCell(...)` runs
//!   `validateCellPath(...)` → the element type's own `validate` for EVERY live
//!   cell it is given. Nothing in that validation consults a covering deletion or
//!   the read clock: reconciliation (`db/rows/Cells.java` / `RowFilter`) is a
//!   separate, LATER concern from whether the bytes are well formed. So "this
//!   member is shadowed" is not a licence to skip validating it.
//! * The zero-length case is where this decoder is deliberately STRICTER than
//!   `Int32Serializer.validate` — rationale in `fixed_width.rs`.

use super::super::test_support::helpers::build_set_cell_bytes;
use super::super::ElementShadow;
use super::*;
use crate::schema::Column;

/// Read clock used by every case here (epoch seconds); only the TTL cases care.
const NOW: i64 = 1_000_000;

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

/// A filter that SHADOWS every element whose write ts is `<= 100`. The helper
/// cells below encode timestamp delta 0 against `min_timestamp = 0`, so their
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

/// A filter with NO covering deletion: nothing is shadowed, and only an element
/// carrying its own expiry can be dropped (used by the TTL-expiry cases).
fn expiry_only_filter() -> ElementShadow {
    ElementShadow {
        cover: None,
        now: NOW,
        row_ts: None,
        row_expires_at: None,
        row_ttl_seconds: None,
    }
}

/// One EXPIRING live set cell (`HAS_EMPTY_VALUE | IS_EXPIRING`) whose explicit
/// `localDeletionTime` is `ldt` seconds — i.e. already expired at [`NOW`].
///
/// Wire order read by `parse_complex_cell_value`:
/// `[flags:u8][timestamp:VUInt][localDeletionTime:VUInt][ttl:VUInt][path_len:VUInt][path]`
/// with `min_timestamp = min_local_deletion_time = 0` in the test parser, so the
/// deltas below ARE the absolute values.
fn build_expiring_set_cell_bytes(path: &[u8], ldt: u8, ttl: u8) -> Vec<u8> {
    assert!(path.len() < 0x80, "single-byte VUInt only");
    let mut buf = vec![0x04u8 | 0x02u8, 0x00, ldt, ttl, path.len() as u8];
    buf.extend_from_slice(path);
    buf
}

/// `[cell_count][cells...]`, one byte of count (VUInt) for a small set.
fn blob(cells: &[Vec<u8>]) -> Vec<u8> {
    assert!(cells.len() < 0x80, "single-byte VUInt only");
    let mut out = vec![cells.len() as u8];
    for c in cells {
        out.extend_from_slice(c);
    }
    out
}

/// Decode a whole set column, returning the collapsed value and the number of
/// elements the #1741 filter dropped.
fn decode(
    column_type: &str,
    cells: &[Vec<u8>],
    filter: Option<ElementShadow>,
) -> Result<(Value, usize)> {
    let p = parser();
    let col = column(column_type);
    p.parse_complex_column_inner(
        &blob(cells),
        0,
        &col,
        &col.data_type,
        false,
        0,
        None,
        filter,
    )
    .map(|(value, _consumed, meta)| (value, meta.shadow_filtered_element_count))
}

// ---------------------------------------------------------------------------
// THE hole: a dropped element's path skipped the width guard.
// ---------------------------------------------------------------------------

/// A wrong-width member is REFUSED even though the covering deletion would have
/// filtered it away. RED before this fix: the element `continue`d first and the
/// read returned `Set([])` with `shadow_filtered_element_count == 1`.
#[test]
fn wrong_width_member_is_refused_even_when_shadowed() {
    for path in [
        vec![],                             // 0 bytes
        vec![0x00, 0x00, 0x07],             // 3 bytes
        vec![0x00, 0x00, 0x00, 0x07, 0x00], // 5 bytes
    ] {
        let declared = path.len();
        match decode(
            "set<int>",
            &[build_set_cell_bytes(&path)],
            Some(shadow_everything()),
        ) {
            Err(Error::FixedWidthLengthMismatch {
                expected, actual, ..
            }) => {
                assert_eq!(expected, 4, "`int` admits exactly 4 bytes here");
                assert_eq!(actual, declared, "the error reports the declared length");
            }
            Err(other) => panic!(
                "a {declared}-byte shadowed `int` member must be refused with the NAMED \
                 FixedWidthLengthMismatch, got {other:?}"
            ),
            Ok((value, filtered)) => panic!(
                "a {declared}-byte `int` member must be REFUSED even when the covering \
                 deletion would drop it — being shadowed is not a licence to skip the \
                 width guard; got {value:?} (filtered {filtered})"
            ),
        }
    }
}

/// The same for the OTHER drop reason: TTL expiry at the read clock.
#[test]
fn wrong_width_member_is_refused_even_when_ttl_expired() {
    // localDeletionTime 1s, TTL 1s ⇒ expired long before NOW.
    let cell = build_expiring_set_cell_bytes(&[0x00, 0x00, 0x07], 1, 1);
    match decode("set<int>", &[cell], Some(expiry_only_filter())) {
        Err(Error::FixedWidthLengthMismatch {
            expected, actual, ..
        }) => {
            assert_eq!((expected, actual), (4, 3));
        }
        other => panic!(
            "a 3-byte TTL-expired `int` member must be refused with the NAMED \
             FixedWidthLengthMismatch, got {other:?}"
        ),
    }
}

// ---------------------------------------------------------------------------
// Negative controls: #1741 filtering is UNCHANGED.
// ---------------------------------------------------------------------------

/// A WELL-FORMED shadowed member still filters exactly as before, and the drop
/// accounting is unchanged. The unfiltered decode of the same bytes is the
/// control that proves the member was really there to be dropped.
#[test]
fn a_well_formed_shadowed_member_still_filters_with_the_same_accounting() {
    let cells = [build_set_cell_bytes(&7i32.to_be_bytes())];

    assert_eq!(
        decode("set<int>", &cells, None).expect("physical consumers filter nothing"),
        (Value::Set(vec![Value::Integer(7)]), 0),
        "control: with no filter the member is present and nothing is counted as dropped"
    );

    assert_eq!(
        decode("set<int>", &cells, Some(shadow_everything()))
            .expect("a well-formed shadowed member must still filter silently"),
        (Value::Set(vec![]), 1),
        "the shadowed member is dropped and counted exactly as before #3723 round 3"
    );
}

/// A shadowed member alongside a LIVE one: only the shadowed member goes, the
/// live member survives, and the count is 1. This is the case that would break
/// if the element loop's filtering had been restructured.
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
        decode("set<int>", &[shadowed, live], Some(shadow_everything()))
            .expect("a mixed set must decode"),
        (Value::Set(vec![Value::Integer(9)]), 1),
        "exactly the shadowed member is dropped; the live member is untouched"
    );
}

/// A dropped member whose path fails with a TOLERATED (non-fatal) class is still
/// filtered SILENTLY — validating dropped members must not promote a tolerated
/// error into a read failure. Invalid UTF-8 is `Error::corruption`, which is not
/// in the one-variant fatal set (`fatal_decode_error.rs`).
#[test]
fn a_tolerated_path_failure_on_a_shadowed_member_still_filters_silently() {
    assert_eq!(
        decode(
            "set<text>",
            &[build_set_cell_bytes(&[0x80])],
            Some(shadow_everything())
        )
        .expect("a tolerated (non-width) path failure on a dropped member must not fail the read"),
        (Value::Set(vec![]), 1),
        "the member is filtered and counted, and no error escapes"
    );
}

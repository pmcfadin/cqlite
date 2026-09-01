//! One guarded conversion for a COMPOSITE COMPONENT LENGTH read from untrusted
//! on-disk bytes (issue #3612 review round 3, finding R3-F1).
//!
//! # The defect this closes
//! A tuple/UDT component is framed `[i32 BE len][bytes]`, where `-1` means null.
//! Three of the UDT field loops handled `-1` and `0` and then cast the remainder
//! with a bare `as usize`, so a length BELOW `-1` — e.g. `-2`, which is
//! representable in any hostile or corrupt cell path — became a ~1.8e19 `usize`.
//! The following `current_offset + field_len > data.len()` bounds test then
//! OVERFLOWED (a debug-build panic; a wrap in release, after which the test can
//! pass and the slice index panics instead). CLAUDE.md forbids a reachable panic
//! in a parser on untrusted bytes, so this is a hard failure, not a nit.
//!
//! The UDT field loops, ALL FIVE now routed through here:
//! * `udt.rs` `parse_udt_value` — the one that already rejected `< 0`
//!   explicitly, i.e. the control that this was an inconsistency and not a
//!   design; routed anyway so its duplicated arm could go away.
//! * `udt.rs` `parse_nested_udt_from_registry`
//! * `udt.rs` `parse_inline_udt_value`
//! * `raw_type_value.rs`'s two INLINE loops inside `parse_raw_type_value` — the
//!   MARSHAL one (the branch the committed `cm` fixture key actually takes) and
//!   the REGISTRY-BARE one. Neither is a call to `parse_udt_value`; an earlier
//!   revision of this header and of the enumeration table said it was.
//!
//! # Why the guard lives HERE, of all places
//! Not for elegance. Those three branches became reachable from a MULTICELL map's
//! cell-path key only because #3612 delegated that key to the structural decoder
//! (`decode_reporting_consumption` routes a registry-resolved bare UDT name and a
//! marshal `UserType(..)` through `parse_raw_type_value`), so this module is the
//! change that exposed them. Their own files — `udt.rs` (1777 lines) and
//! `raw_type_value.rs` (1233) — are both far over the 800-line campsite threshold
//! and cannot grow without tripping the gate's file-size ratchet, whereas routing
//! each site through this helper REMOVES ~5 lines from each of them. Splitting
//! either file properly is epic #1116's job, not a bounds fix's.
//!
//! # Why it also owns the bounds test
//! Folding the conversion and the `offset + len <= end` test into one call is what
//! makes the change line-negative at every site, and it means the two can never
//! again be applied in the wrong order or one without the other — which is exactly
//! how R3-F1 arose.

use super::*;

impl V5CompressedLegacyParser {
    /// Convert a component/field length read from on-disk bytes into a `usize`
    /// and prove it fits inside `end`, or return a corruption error.
    ///
    /// Callers handle the two SPECIAL lengths first — `-1` (null component) and
    /// `0` (empty component) — so anything arriving here must be positive.
    /// A negative value is corruption and is rejected BEFORE any conversion; the
    /// bounds test then uses `checked_add`, so no arithmetic here can wrap or
    /// panic for any `i32` input.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn checked_component_len(
        raw: i32,
        component: &str,
        offset: usize,
        end: usize,
    ) -> Result<usize> {
        if raw < 0 {
            return Err(Error::corruption(format!(
                "UDT/tuple component '{}': invalid negative length {} (only -1, meaning \
                 null, is a legal negative)",
                component, raw
            )));
        }
        let len = raw as usize;
        let past = offset.checked_add(len).ok_or_else(|| {
            Error::corruption(format!(
                "UDT/tuple component '{}': length {} overflows the offset {}",
                component, len, offset
            ))
        })?;
        if past > end {
            return Err(Error::corruption(format!(
                "UDT/tuple component '{}' extends beyond data (need {}, have {})",
                component,
                len,
                end.saturating_sub(offset)
            )));
        }
        Ok(len)
    }
}

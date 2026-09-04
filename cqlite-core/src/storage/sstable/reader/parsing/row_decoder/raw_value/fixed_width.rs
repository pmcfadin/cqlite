//! Issue #3847 — the ONE place that says which byte widths a fixed-width CQL
//! scalar admits on the READ path, and what an EMPTY buffer means there.
//!
//! # The oracle, and why it is `deserialize()` and not `validate()`
//!
//! `docs/round-artifacts/issue-3847-cassandra-oracle.md`, read at the pinned
//! `cassandra-5.0.8` tag. A bounded value decoder is a READ path, so its
//! Cassandra analogue is `TypeSerializer.deserialize`, and on that side the rule
//! is **uniform, 12 of 12, with no per-type exceptions**: an EMPTY buffer
//! deserializes to `null` for `int`, `bigint`/`counter`, `boolean`, `uuid`,
//! `timeuuid`, `float`, `double`, `smallint`, `tinyint`, `timestamp`, `date` and
//! `time` alike.
//!
//! `validate()` is NOT this path's oracle. It is not uniform — `smallint`,
//! `tinyint`, `date` and `time` reject the empty buffer there — but it gates what
//! may be WRITTEN, not what must be READ, and all four of those still
//! *deserialize* empty to `null`. Reading is the permissive side of Cassandra's
//! contract, and CQLite's reader must match Cassandra's reader.
//!
//! Corroboration from the other direction: `BooleanSerializer.serialize(null)`
//! returns `ByteBufferUtil.EMPTY_BYTE_BUFFER`. Empty **is** the on-the-wire
//! spelling of `null` for a fixed-width scalar, so a reader that refuses it
//! cannot read data Cassandra legitimately writes.
//!
//! # Scope: BOUNDED decoders only, and that is not a shortcut
//!
//! Only a decoder handed an EXACTLY-bounded slice can observe an empty buffer at
//! all: the length that says "zero bytes" has to come from somewhere. That is the
//! case for a frozen collection element, a tuple/UDT component and a cell-path
//! key, all of which carry an explicit component length. It is NOT the case for
//! the offset-advancing cell/clustering decoders (`cell_value_scalar.rs`,
//! `raw_type_value.rs`, `row_framing.rs`), where a fixed-width value is written
//! with NO length prefix at all (`AbstractType.valueLengthIfFixed`) — an empty
//! fixed-width value is not expressible there, for Cassandra either, so those
//! families are out of scope rather than merely unvisited.
//!
//! # The five UDT framing sites, which had three different answers
//!
//! A UDT field's length header decides whether a field is absent (`-1`), present
//! and empty (`0`) or present with bytes. Five sites route the `0`:
//! `parse_udt_value` and `raw_type_value.rs`'s UDT arm through
//! `create_empty_value_for_type`, and `parse_nested_udt_from_registry`,
//! `parse_inline_udt_value` and `raw_type_value.rs`'s registry arm by calling a
//! field decoder with an explicit `&[]`. Before #3847 the first two answered
//! "empty BLOB" and the other three answered `Err`; all five now answer `null`.
//! An `Err` there was the worse of the two, because `row_data.rs` `break`s its
//! column loop on a failing column, so the failing column AND every later one
//! silently became null.
//!
//! # DECLARED DIVERGENCE, opened by #3847 and NOT closed by it
//!
//! `complex_column/cell_path_key.rs`'s `cql_short_allowed_widths` is a separate
//! WIDTH-ADMISSIBILITY table in front of this decoder, and its oracle is
//! `validate()`: it admits `{n, 0}` for the eight permissive families but only
//! `{n}` for `smallint`, `tinyint`, `date` and `time`. Those four now disagree
//! with this rule, and the stricter table wins — an empty MAP KEY of those four
//! types is refused before the decoder sees it. Left alone deliberately: #3847's
//! subject is the value decoder, and relaxing what a map KEY may be is its own
//! behaviour change needing its own corpus measurement. Recorded here, and
//! pointed at from `cell_path_key_tests.rs`, rather than left to be rediscovered.
//! (Reachability caveat, from #3612 R6-F2(a): no READ reaches an empty cell path
//! at all — the multicell map caller filters `path_bytes.is_empty()` — so the
//! divergence is unit-observable only, which is why it is a residual and not a
//! defect.)
//!
//! # Why this rule lives under `raw_value`
//!
//! It is shared by `raw_value/reporting.rs` (the `parse_value_from_raw_bytes`
//! path, #3847's named subject) and by `udt.rs`'s two scalar field decoders,
//! which this change reconciles TO it — one rule, one place, so the two families
//! cannot drift into two opinions about a width again. It is a child of
//! `raw_value` rather than a sibling registered in `mod.rs` because `mod.rs` is
//! over the campsite file-size ratchet ceiling and cannot take a new line
//! (epic #1116).

/// An ADMISSIBLE fixed-width slice, and which of the two admissible shapes it is.
///
/// Inadmissibility is deliberately NOT a variant: each caller family formats its
/// own corruption message (they differ, and their wording is asserted by existing
/// tests), so the classifiers below report it as `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::sstable::reader::parsing::row_decoder) enum FixedWidthCell {
    /// The EMPTY buffer — Cassandra's on-the-wire spelling of `null`.
    ///
    /// The caller MUST decode it to [`crate::types::Value::Null`] and, where it
    /// reports consumption, report **`0`** — never the type's width `n`. Reporting
    /// `n` here would make the caller's fully-consumed assert refuse the very
    /// value this classification admits (that composition is the whole substance
    /// of #3847: relaxing the width guard alone is a defect, not the fix).
    Null,
    /// A non-empty slice wide enough for the type: `data.len() >= n` from
    /// [`admissible_at_least`], `data.len() == n` from [`admissible_exactly`].
    /// The caller may read the type's first `n` bytes.
    Bytes,
}

/// Accepted widths `{0} ∪ {w : w >= n}`.
///
/// For a caller that reports its consumption and inherits a fully-consumed
/// assert: an over-width slice is admitted here and refused THERE, because the
/// arm reports `n` and leaves the tail unconsumed. The composed accepted set is
/// `{n, 0}`, which is Cassandra's.
pub(in crate::storage::sstable::reader::parsing::row_decoder) fn admissible_at_least(
    data: &[u8],
    n: usize,
) -> Option<FixedWidthCell> {
    if data.is_empty() {
        Some(FixedWidthCell::Null)
    } else if data.len() >= n {
        Some(FixedWidthCell::Bytes)
    } else {
        None
    }
}

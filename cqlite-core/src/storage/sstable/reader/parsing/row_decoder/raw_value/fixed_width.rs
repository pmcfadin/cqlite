//! Issue #3847 — which byte widths a fixed-width CQL scalar admits on the
//! `raw_value` READ path, and what an EMPTY buffer means there.
//!
//! # SCOPE: this is ONE OF TWO independent tables. It is NOT the only one.
//!
//! An earlier revision of this header called it "the ONE place" and said the UDT
//! field decoders were reconciled TO it. **That was true of this branch before it
//! merged `origin/main`, and it is false now** (roborev job 149). #3631/PR#3820
//! moved the UDT field decoders to `row_decoder/typed_value.rs`, which answers the
//! same question from its OWN table:
//! `typed_value/scalar_rules.rs::empty_is_a_value`. So the repository currently has
//! **two independent answers** to "what does an empty buffer mean for this type":
//!
//! | path | authority |
//! |---|---|
//! | `parse_value_from_raw_bytes` (`raw_value/reporting.rs`) | THIS module |
//! | typed / UDT field decode (`typed_value.rs`) | `typed_value/scalar_rules.rs::empty_is_a_value` |
//!
//! **The drift risk is therefore REAL and NOT prevented by this module** — stating
//! that plainly, because the previous wording advertised a guarantee that no longer
//! exists, and a false claim of drift-prevention is worse than an acknowledged gap:
//! it tells the next reader to stop looking. The two agree TODAY on the shape that
//! matters (empty ⇒ null at consumed `0` for the fixed-width family); nothing
//! enforces that they keep agreeing.
//!
//! Unifying them is deliberately NOT done here. It would mean editing
//! `typed_value/` — code #3631 landed hours ago — under an issue whose subject is
//! the `raw_value` path, and #3847's corpus census measured only this path. It is
//! proposed as a follow-up on the issue thread (`REQ-3847-04`).
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
//! # THE CELL-PATH KEY: this rule REACHES it by delegation, and the key path
//! # answers for itself
//!
//! `complex_column/cell_path_key.rs` has a separate WIDTH-ADMISSIBILITY table whose
//! oracle is `validate()`: it admits `{n, 0}` for the eight permissive families but
//! only `{n}` for `smallint`, `tinyint`, `date` and `time`, and the stricter table
//! wins for those four. That much is unchanged.
//!
//! **TWO CLAIMS THAT USED TO STAND HERE WERE FALSE, and each cost something:**
//!
//! 1. *"No READ reaches an empty cell path — the multicell map caller filters
//!    `path_bytes.is_empty()` — so the divergence is unit-observable only, a
//!    residual and not a defect."* **#3747 REMOVED that caller guard on purpose**
//!    (and #4106 removed the SET branch's identical one, for the identical
//!    reason: a zero-length cell path is the EMPTY component, not its absence);
//!    its tests now carry `!! REACHABILITY: THE EMPTY-KEY CASES BELOW ARE NOW
//!    REACHED BY A REAL READ`. A stale reachability argument is the worst kind of
//!    comment: it reads as a licence to stop looking.
//! 2. *"This rule is about VALUES and does not reach the key path."* **It reaches it
//!    by DELEGATION** — `cell_path_key.rs` decodes through
//!    `parse_value_from_raw_bytes`. Widening this rule therefore changed KEY
//!    decoding, and it was not a residual: #3747's opaque policy sat on the
//!    decoder's `Err` branch and stopped firing once the decode began SUCCEEDING
//!    with `Null`, so `an_empty_key_of_an_n_or_zero_type_is_preserved_opaquely`
//!    FAILED in the gate of record. Grepping `cell_path_key.rs` for this module's
//!    symbols found nothing and looked like proof of no coupling; the coupling is
//!    through the shared decoder, not through a name.
//!
//! **What is true now:** `{n, 0}` -> `null` is the rule for a VALUE. A KEY cannot
//! take that answer — Cassandra has no null map key — so `cell_path_key.rs` applies
//! #3747's empty -> OPAQUE answer (empty blob plus `opaque_out`) for itself, KEYED ON
//! THIS DECODER'S ANSWER: it delegates, and when an empty buffer comes back `Null` it
//! substitutes the opaque key. It does NOT inherit this rule, and it no longer depends
//! on the decode FAILING (the door #3847 shut).
//!
//! **A third correction, from the fix round itself:** the first attempt put that check
//! BEFORE the delegation, gated on the key table admitting width 0. That is too broad
//! and broke `inet`, which admits `[0, 4, 16]` and whose empty buffer decodes to a real
//! `Value::Inet(empty)` (`InetAddressSerializer.validate` returns early on empty). The
//! width table says what is ADMISSIBLE; only the decode says whether a value could be
//! PRODUCED — so the policy has to read the answer, not the table. Three false
//! statements have now stood in this one header; each was a claim about code that had
//! moved underneath it.
//!
//! # Why this rule lives under `raw_value`
//!
//! Its sole caller is `raw_value/reporting.rs` (the `parse_value_from_raw_bytes`
//! path, #3847's named subject). It was ALSO used by this branch's own UDT field
//! decoders until the `origin/main` merge; #3631 superseded those with
//! `typed_value.rs`, so the UDT half is no longer this module's business — see the
//! SCOPE note at the top. It is a child of
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
    /// A non-empty slice wide enough for the type: `data.len() >= n`, from
    /// [`admissible_at_least`] — the sole remaining classifier. The caller may read
    /// the type's first `n` bytes.
    ///
    /// An `admissible_exactly` sibling existed while this branch carried its own UDT
    /// field decoders; #3631 superseded those with `row_decoder/typed_value.rs`, so it
    /// was removed with them rather than left as an uncalled function. Referencing it
    /// here would be a broken intra-doc link (roborev job 147).
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

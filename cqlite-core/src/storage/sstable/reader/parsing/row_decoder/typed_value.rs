//! Decode a bounded byte slice whose type is a **declared `CqlType`** (issue #3631),
//! and own the ONE implementation of the bounded-decode exhaustion rule.
//!
//! ## Why this module exists
//!
//! Two decode paths used to match a CLOSED SET of primitive types and fall back to
//! `Value::Blob` for everything else, while the schema naming the real type was in
//! hand and unread:
//!
//! * `parse_simple_udt_field_value` (`udt.rs`) — a COLLECTION-typed field of a
//!   frozen UDT (`frozen<map<text,int>>`, `frozen<list<…>>`, `frozen<set<…>>`)
//!   surfaced to callers as bytes. Instance B of #3631.
//! * `parse_cell_path_key` (`cell_path_key.rs`) — a non-frozen map's cell-path key.
//!   Instance A, fixed THERE by #3612 / PR #3736, not here.
//!
//! Issue #28 (no-heuristics) forbids that silent degradation: authoritative
//! metadata only, and where the metadata is present it must be USED. A
//! `tracing::debug!` is not a diagnostic a caller can see, so a type this decoder
//! genuinely cannot express is an explicit `Error` naming it.
//!
//! ## Format authority (never CQLite's own prior output)
//!
//! Every structured shape below is the **frozen / "multi-cell-free"** serialization
//! Cassandra writes for a value nested inside another value, read at the pinned tag:
//!
//! * Collections — `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/CollectionSerializer.java`.
//!   `writeCollectionSize` is `output.putInt(elements)` (a 4-byte BE i32 count, NOT a
//!   vint), and `writeValue` writes `putInt(size)` then the bytes, with `putInt(-1)`
//!   for a null element; `readValue` treats any negative size as null. A `list`/`set`
//!   is `[i32 count]` then that many values; a `map` is `[i32 count]` then that many
//!   KEY, VALUE pairs, each independently length-prefixed.
//! * Tuples — `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java`
//!   (`buildValue` / `split`): each component is `[i32 size][bytes]`, `-1` for null,
//!   and a tuple may be written with FEWER components than the type declares
//!   (trailing components are then absent, i.e. null).
//! * UDTs — `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java`,
//!   which extends `TupleType`: identical per-field `[i32 size][bytes]` framing. That
//!   is already implemented by `parse_nested_udt_from_registry` /
//!   `parse_inline_udt_value`, which this module delegates to so there is ONE UDT
//!   field-framing implementation.
//!
//! Cross-checked against the committed Cassandra-5.0.2-written fixture
//! `test-data/fixtures/issue_3504/` and its `sstabledump` golden, where
//! `udt_hashable_shapes` row 3's `stn` renders the nested `frozen<map<text,int>>`
//! field as `{"a": 1}` from the on-disk bytes
//! `00000001 00000001 61 00000004 00000001`.

use super::*;

impl V5CompressedLegacyParser {
    /// Assert a bounded decode consumed its entire slice.
    ///
    /// # THE one implementation of this rule (issues #3811 + #3820/#3631)
    /// #3811 landed `require_fully_consumed_raw` beside the type-STRING decoder and
    /// recorded, in its own doc comment, that #3820 was adding a second copy over
    /// `&CqlType` and that *"there must be ONE implementation of this rule, not two"*.
    /// This is that one implementation: `require_fully_consumed_raw` is GONE and all of
    /// its call sites — `raw_value.rs`'s bounded wrapper, `cell_value_complex.rs`'s two
    /// frozen-UDT columns, `udt.rs`'s nested-UDT arms and `udt/inline.rs` — name this
    /// function, with the SAME arguments and the SAME message. The type-string and
    /// `CqlType` sides therefore share one error class, which is what the #3811 note
    /// asked for: a caller matching on the message must not have to know which layer
    /// refused.
    ///
    /// `consumed` is what the decoder reports it read; `len` is the exact extent the
    /// caller handed it. Anything short is `cassandra-5.0.8` `TupleType.split` rule 2
    /// or rule 4 — a partial component-length prefix, or trailing bytes after the last
    /// declared component — and Cassandra throws `MarshalException` for both. A
    /// genuinely SHORT encoding (rule 1, omitted trailing components) leaves
    /// `consumed == len` and stays ACCEPTED.
    ///
    /// Discarding the leftover bytes silently is the framing-error-MASKING class that
    /// let #3002's `Rows.db` root-base defect hide behind a compensating encoder
    /// defect: two errors that cancel are undetectable unless something insists the
    /// accounting balances.
    ///
    /// `subject` names the thing being decoded (a column, a field, a nested type) and
    /// `type_desc` its declared type, both for the message only.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn require_fully_consumed(
        consumed: usize,
        len: usize,
        subject: &str,
        type_desc: &str,
    ) -> Result<()> {
        if consumed == len {
            return Ok(());
        }
        if consumed < len {
            // Wording deliberately SHARED with `cell_path_key.rs`'s consumption
            // refusal ("decoded only N of M byte(s)"): it is the same rule, and a
            // caller matching on the message must not have to know which of the
            // layers refused.
            return Err(Error::corruption(format!(
                "Bounded value '{}' of type '{}' decoded only {} of {} byte(s); the whole \
             slice must be the value (trailing bytes, or a partial trailing component \
             header, are corruption — Cassandra TupleType.split rules 2 and 4)",
                subject, type_desc, consumed, len
            )));
        }
        Err(Error::corruption(format!(
        "Bounded value '{}' (type '{}'): decoder reported {} bytes consumed but only {} were available",
        subject, type_desc, consumed, len
    )))
    }
}

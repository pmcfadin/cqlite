//! The per-type SPELLING question of the CSV container seam: what TEXT does a value
//! of this declared type carry, and can that text be EMPTY? (issue #3815 round 3
//! split it out of `golden_csv_container.rs` under the campsite rule, CLAUDE.md epic
//! #1135, which the key-scoped refusal work pushed past the ~1500-line test-file
//! target.)
//!
//! THE SEAM IS ONE THE REPOSITORY HAD ALREADY DRAWN: the TESTS for exactly this
//! content were split out first, as `golden_csv_container_spelling_tests.rs`, whose
//! own doc states the responsibility — "the per-type SPELLING differential", the
//! GOLDEN's `writeString(type.getString(v))` against `ValueFormatter`'s rendering.
//! This file is the source half of that same boundary, so the differential and its
//! subject now sit at the same granularity.
//!
//! Its two authorities are DIFFERENT and the distinction is the whole reason the
//! seam is here, rather than being about size:
//!
//!   * what a value of a type SHOULD be spelled as is the `sstabledump` golden's
//!     answer, read from the pin `cassandra-5.0.8`. Taking it from CQLite would be
//!     circular (CLAUDE.md, #3042);
//!   * what SHAPES this egress's own output can take — specifically, whether it can
//!     be zero-length — is a question only `ValueFormatter` can answer, and
//!     [`member_can_render_empty`] states at length why asking it is legitimate.
//!
//! Everything the parent module does with these answers — deciding whether a node's
//! rendering can be read back — is the REFUSAL question and stays there. No surface
//! change: the parent re-exports all five, so every call site is unchanged.

use super::*;

/// The text a scalar carries inside the golden's own rendering, translated to the
/// CSV spelling where the golden's own spelling at THIS position is not it.
///
/// An untyped position is left verbatim: no translation can be derived without a
/// declared type, and the shape disagreement belongs to the comparison.
pub(super) fn scalar_csv_text(scalar: &Value, ty: Option<&CqlType>, kinding: Kinding) -> String {
    let text = scalar_text(scalar);
    match (kinding, ty) {
        (Kinding::Stringified, Some(ty)) => stringified_csv_text(text, ty),
        _ => text,
    }
}

/// The CSV text a golden scalar at a [`Kinding::Stringified`] position denotes.
///
/// # The two sides, and which authority each comes from
///
/// A stringified golden is `writeString(type.getString(v))` — the GOLDEN side, read
/// from the pin `cassandra-5.0.8` (the per-type census is in [`Kinding`]'s doc).
/// The CSV side is a question about CQLite's OWN output shape, so it is read from
/// `cqlite_core::util::value_fmt::ValueFormatter::format_value`, which is
/// legitimate for the same reason [`member_can_render_empty`] states at length.
///
/// # Why only `blob` is TRANSLATED
///
/// Walked over every type that can occupy a stringified position (a partition-key
/// component, a multicell set's element, a map key). `getString` is
/// `serializer.toString(deserialize(v))`:
///
///   * **`blob` — DIFFERS, and MATERIALLY.** `BytesSerializer.toString` is
///     `ByteBufferUtil.bytesToHex`, the BARE lowercase hex, so the empty blob is
///     `""`; `ValueFormatter` renders `format!("0x{hex}")`, so the empty blob is
///     `0x`. Left untranslated, a sole empty blob member synthesized an EMPTY body
///     and the node was refused as unrecoverable — and a refused one-member node
///     accepts any framed body at all, so the member went uncompared. Translated
///     by [`super::stringified_blob_spelling`], the one place this repository
///     states the rule;
///   * **`timestamp` — differs, IMMATERIALLY.** `FORMATTER_UTC`'s
///     `yyyy-MM-dd'T'HH:mm:ss.SSSX` against `ValueFormatter::format_timestamp`'s
///     `YYYY-MM-DD HH:MM:SS.fff+0000`. That is the lane's DECLARED timestamp
///     narrowing and this function does not close it; it cannot move a `, `, a
///     `: ` (the pattern's colons are digit-flanked) or a bracket, and neither
///     spelling is ever empty, so the structural question is unaffected;
///   * **`duration` — DIFFERS, and MATERIALLY.** MEASURED, not reasoned:
///     Cassandra's `Duration.toString()` decomposes into `y/mo/w/d/h/m/s/ms/us/ns`
///     — the committed `test_basic.simple_table` golden carries `"12h58m22s"` and
///     `"1h20m44s"` — while `ValueFormatter::format_duration` prints
///     months/days/NANOS only, i.e. `46702000000000ns` for that same value. Same
///     value, materially different text. This function does NOT translate it, so a
///     `duration` at a stringified position is compared untranslated and will
///     diverge. The sibling #1490 lane records the same divergence
///     (`tests/support/parquet_parity/spelling.rs`, module doc). Correcting
///     `format_duration` is a follow-up, not this lane's business; what belongs
///     here is that the census says so rather than claiming a match it does not
///     have;
///   * **`counter` — CANNOT OCCUPY THIS POSITION, and is therefore not part of
///     the walk above.** Stated because it is a `CqlType::Numeric` and would
///     otherwise be read as covered by "the integer family": Cassandra's
///     `CounterColumnType.getString` is `accessor.toHex(value)`
///     (`cassandra-5.0.8:.../marshal/CounterColumnType.java:74-77`), i.e. BARE
///     HEX like a blob's, which this function does NOT translate. That would be a
///     material divergence if a golden could carry it, and none can: every
///     stringified position is barred to a counter by Cassandra itself — a
///     PRIMARY KEY column (`CreateTableStatement.java:231-232`, "counter type is
///     not supported for PRIMARY KEY column"), a multicell set element and a map
///     key (`CQL3Type.java:825-836`, "Counters are not allowed inside
///     collections"). The spelling differential records it as the one DECLARED
///     UNREACHABLE position rather than pinning a spelling no golden has;
///   * **every other type — IDENTICAL text.** `boolean` is `Boolean.toString()` on
///     both sides; the integer family is `String.valueOf` / `BigInteger.toString(10)`
///     against `to_string()`; `float`/`double`/`decimal` differ only in the
///     narrowings this lane already declares (trailing zeros, exponent form), which
///     like the timestamp cannot carry a separator, a bracket or an empty spelling;
///     `text`/`varchar`/`ascii`, `uuid`/`timeuuid`, `date`, `time` and `inet` are
///     spelled by the same function on both sides.
///
/// A CONTAINER type cannot be reached: this is called for a scalar golden only,
/// and a frozen container at a stringified position is the case [`Kinding`] names
/// as NOT COVERED (`getString` spells the whole value as one string, which the
/// comparison reports as a shape divergence).
///
/// TOTAL over `CqlType` with no wildcard, for the reason
/// [`member_can_render_empty`] gives: a new variant must have its answer
/// established here rather than inherited from whichever side a wildcard sat on.
pub(super) fn stringified_csv_text(text: String, ty: &CqlType) -> String {
    match ty {
        CqlType::Blob => stringified_blob_spelling(&text).unwrap_or(text),
        CqlType::Numeric(_)
        | CqlType::Text(_)
        | CqlType::Boolean
        | CqlType::Timestamp
        | CqlType::Opaque(_)
        | CqlType::List(_)
        | CqlType::Set(_)
        | CqlType::Map(..)
        | CqlType::Tuple(_)
        | CqlType::Udt(_) => text,
    }
}

/// The text a scalar carries inside the golden's own rendering
/// ([`golden_rendering`]).
/// `Value::Null` renders as the `null` token (NULL-TOKEN in the module doc), which
/// is a text a `text` member can also produce — resolved by the parent's
/// `decode_half::decode_shape` from the golden's own type (private to that module, so
/// not linkable from here), and deliberately not a refusal.
pub(super) fn scalar_text(scalar: &Value) -> String {
    match scalar {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

/// Is an EMPTY golden array of this declared type genuinely unrecoverable?
///
/// Only for a `list`/`set` whose ELEMENT can render as the empty string. A
/// `tuple` is exempt because its member count comes from the DDL, so the
/// comparison's arity check sees a dropped member; every other type does not
/// describe an array at all, and refusing there would hide the shape divergence
/// the comparison exists to report.
pub(super) fn empty_container_is_ambiguous(ty: Option<&CqlType>) -> bool {
    match ty {
        Some(CqlType::List(element)) | Some(CqlType::Set(element)) => {
            member_can_render_empty(element)
        }
        _ => false,
    }
}

/// Can a value of this declared type render as the EMPTY string?
///
/// # Why asking CQLite's own formatter is legitimate here
///
/// This is the one question in this lane whose answer comes from
/// `cqlite_core::util::value_fmt::ValueFormatter` rather than from an external
/// oracle, and the distinction is worth stating: it does not ask what a value of
/// this type SHOULD render as — that is the `sstabledump` golden's answer, and
/// taking it from CQLite would be circular (CLAUDE.md, #3042) — it asks what
/// SHAPE this egress's own output can take, i.e. whether the CSV rendering of SOME
/// value of this type can be zero-length. Nothing outside the formatter can answer
/// that, and the answer only ever decides whether an EMPTY container is refused;
/// every value the comparison then makes is still the golden's.
///
/// # The answer, per type, from that formatter's branches
///
/// `ValueFormatter::format_value` has exactly one branch that passes its payload
/// straight through — `Value::Text(s)` renders `String::from_utf8_lossy(s)` — so
/// `text`/`varchar`/`ascii` are the ONLY types with an empty rendering. Every other
/// branch emits at least one character on every path it can take, including its
/// emptiest and its invalid inputs:
///
///   * integers/floats render through `to_string()`, a `{:e}`/`{}` format, or the
///     tokens `NaN`/`Infinity`/`-Infinity`; a zero-length `varint` renders `0`, and
///     a zero-length or over-ceiling `decimal` renders `0` or
///     `<corrupt-decimal:…>`;
///   * `boolean` is `true`/`false`;
///   * `blob` is `format!("0x{hex}")`, so an EMPTY blob is `0x` — 2 characters, not
///     none. This is the type the earlier deny-list got wrong;
///   * `timestamp`/`date`/`time` render a fixed-width `chrono` pattern, or an
///     `<invalid-…:{value}>` marker;
///   * `uuid`/`timeuuid` render 36 characters; `inet` renders an
///     `Ipv4Addr`/`Ipv6Addr` display or `<invalid-inet:N-bytes>`; a `duration`
///     whose every component is zero renders `0ns`;
///   * a container renders its bracket pair, so an empty one is `[]`/`{}`/`()`.
///
/// A NULL member does not widen any of these: `Value::Null` renders as the `null`
/// token (the module doc's NULL-TOKEN), which is 4 characters.
///
/// `super::tests::an_empty_rendering_is_possible_only_for_text` — in the PARENT's test
/// module, which this file's subject was split out of — runs that formatter over each
/// type's emptiest value and requires this function to agree with it, so the claim
/// above is measured rather than asserted in prose.
///
/// # Exhaustive on purpose, and why the DEFAULT matters
///
/// Written as a total match with no `_` arm. The earlier form was a deny-list —
/// "answer `false` for these variants, `true` for everything else" — which
/// answered `true` for `blob`, `timestamp` and every opaque scalar, so an empty
/// collection of any of them was refused and dropped from the coverage counts
/// (review round 19, finding Y2). Over-refusal is a BLIND SPOT and not
/// conservatism: a refused node keeps only [`body_emptiness_bound`], so refusing a
/// recoverable position makes it unchecked. A wildcard would also decide a FUTURE
/// `CqlType` variant's answer silently, in whichever direction the wildcard
/// happens to sit; with the match total, a new variant is a compile error whose
/// fix is to establish that type's answer from the formatter, here.
pub(super) fn member_can_render_empty(ty: &CqlType) -> bool {
    match ty {
        // The one pass-the-payload-through branch: an empty string renders as
        // nothing at all, which is what makes `{}` ambiguous for a `set<text>`.
        CqlType::Text(_) => true,
        CqlType::Numeric(_)
        | CqlType::Boolean
        | CqlType::Blob
        | CqlType::Timestamp
        | CqlType::Opaque(_)
        | CqlType::List(_)
        | CqlType::Set(_)
        | CqlType::Map(..)
        | CqlType::Tuple(_)
        | CqlType::Udt(_) => false,
    }
}

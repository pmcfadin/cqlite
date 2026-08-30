//! What a DECLARED GAP says the divergence IS, in a form the walk can CHECK
//! (issue #1491 review finding, round 17).
//!
//! A declared gap is this lane's honest exception: the whole argument for shipping
//! one is that it names a specific, MEASURED divergence. That argument only holds
//! if the gap suppresses THAT divergence and nothing else. Before this module a
//! gap suppressed whatever happened at its path — so each of the five was really a
//! permanent blind spot for its whole column, and two named regressions would have
//! passed as the documented gap: a NON-EMPTY collection emitting wrong members
//! under an empty-collection gap, and `e.home` changing from blob hex to arbitrary
//! text.
//!
//! # Each divergence is stated from the ORACLE side, plus the SHAPE of the egress
//!
//! Every variant below is a conjunction of two things:
//!
//!   * what the GOLDEN (or the committed DDL) has at that position — the oracle
//!     side, which is where the expectation may come from at all (#3042); and
//!   * the SHAPE the egress renders — a bracket frame the DDL fixes, a blob-hex
//!     literal, a JSON null, a JSON string.
//!
//! What no variant does is pin CQLite's exact current bytes: that would make the
//! gap self-fulfilling — it would "match" precisely as long as nothing changed,
//! which is a tautology, not a measurement. A SHAPE is falsifiable by a regression
//! that renders something else at that position, which is the property the finding
//! asked for.
//!
//! # What a variant does NOT cover is the point of it
//!
//! Each doc comment ends with the divergences the variant deliberately does NOT
//! absorb. Those are the regressions that used to pass as the documented gap and
//! now produce an ordinary diff naming the column, the declared gap and what was
//! actually seen (see `super::compare_value_at`).

use super::super::schema::CqlType;
use super::{canon_typed, csv_container, Depth, Egress, Kinding};
use serde_json::Value;

/// The measured divergence ONE declared gap stands for.
///
/// A gap declares exactly one of these, so "is the mismatch at this position the
/// declared divergence?" is a question with an answer. A variant declared for a
/// position it cannot describe — the wrong CQL type, the wrong egress format —
/// never matches, so the gap suppresses nothing and this lane reports it as stale:
/// a mis-declaration fails, it does not silently widen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Divergence {
    /// An EMPTY multi-cell collection renders as a PRESENT empty container where
    /// Cassandra has no value at all.
    ///
    /// ORACLE: Cassandra stores an empty multi-cell collection as a complex
    /// deletion with NO cells, so `sstabledump` emits no cell and the golden row
    /// has no value at that path — the committed `nb_empty_collections` golden's
    /// `ck=1` row carries `deletion_info` for `ml`/`ms`/`mm` and nothing else —
    /// and `SELECT` reads `null` (`test-data/schemas/cql-type-parity.cql` states
    /// this in the DDL comment for that table).
    ///
    /// EGRESS SHAPE: a PRESENT EMPTY container at a whole collection column. In
    /// JSON that is an empty ARRAY, because the CLI spells a list, a set AND a map
    /// as a JSON array (a map as an array of `{"key","value"}` pairs — see
    /// `super::compare_map`). In CSV it is exactly the declared type's own empty
    /// bracket frame, taken from the DDL by
    /// [`csv_container::empty_rendering`] — so a `list` rendered `{}` or a `set`
    /// rendered `[]` is NOT this gap.
    ///
    /// NOT COVERED: a NON-EMPTY rendering of any kind, and any position where the
    /// golden HAS a value — a non-empty multi-cell collection is compared member
    /// by member, in both formats, exactly as an undeclared column is.
    AbsentMulticellRendersEmpty,
    /// A frozen UDT nested inside another frozen UDT renders as its RAW BYTES in
    /// CQL blob-hex spelling instead of a decoded object.
    ///
    /// ORACLE: `sstabledump` decodes the nested value —
    /// `cassandra-5.0.8 UserType.toJSONString` walks the declared field list and
    /// writes each field — so the golden carries a JSON OBJECT there
    /// (`{"street":"1 Navy Way","city":"Arlington","zip":"22201"}` in the
    /// committed `udt_nested` golden).
    ///
    /// EGRESS SHAPE: a blob literal and nothing else — `0x` followed by an EVEN
    /// number of hex digits, which is CQL's spelling of a byte string.
    ///
    /// NOT COVERED: arbitrary text at that position, a decoded object whose
    /// content differs, a null, a number. DECLARED RESIDUAL: what the bytes behind
    /// the hex DECODE to is not compared — those bytes are the nested UDT's
    /// serialization, so recovering the content would mean re-implementing
    /// Cassandra's UDT value serializer here, which this gap does not do. The gap
    /// therefore still costs the nested field's CONTENT; what it no longer costs
    /// is the shape.
    NestedFrozenUdtRendersAsBlobHex,
    /// A non-finite float renders as JSON `null`, because JSON has no literal for
    /// it.
    ///
    /// ORACLE: the golden carries the token by name. The committed
    /// `signed_special_collections` golden spells `sf`'s cell paths
    /// `"-Infinity"`, `"Infinity"` and `"NaN"` — Java's `Double.toString`
    /// spelling, which is what `sstabledump` writes — and JSON itself has no
    /// literal for a non-finite number (RFC 8259 §6 admits only finite decimal
    /// numbers). So the VALUE is lost with no legal JSON to put in its place.
    ///
    /// EGRESS SHAPE: JSON `null`, in the JSON lane only. The CSV lane renders
    /// every cell as text and carries the three tokens verbatim, which is why the
    /// gap is format-scoped (review finding K1) and why this variant refuses to
    /// match under [`Egress::Csv`] at all.
    ///
    /// NOT COVERED: a FINITE value rendering as null. `-1.5`, `-0.0`, `0.0` and
    /// `2.5` sit beside the three tokens in that same set and are compared as
    /// ordinary members: JSON can spell them, so losing one is data loss with no
    /// format excuse.
    NonFiniteFloatRendersAsJsonNull,
    /// A `decimal` renders as a JSON STRING where the oracle emits an unquoted
    /// number.
    ///
    /// ORACLE: `cassandra-5.0.8 DecimalType.toJSONString` returns
    /// `BigDecimal.toString()` with no quotes, i.e. a JSON NUMBER.
    ///
    /// EGRESS SHAPE: a JSON string, in the JSON lane only, whose NUMERIC VALUE is
    /// the golden's. That last clause is what makes this variant narrow: the two
    /// sides are canonicalized under the declared type and must come out EQUAL, so
    /// the only thing suppressed is the JSON KIND. A `decimal` whose digits differ
    /// from the golden's — the 30-digit exactness this lane exists to check — is
    /// NOT this gap and is reported.
    ///
    /// NOT COVERED: a different number, a null, a non-numeric string, and the CSV
    /// lane (where every cell is text and the 30-digit values match exactly).
    DecimalRendersAsJsonString,
}

impl Divergence {
    /// A one-line statement of the declared divergence, for the census and for the
    /// diff that reports a mismatch which is NOT this one.
    pub fn declared(self) -> &'static str {
        match self {
            Divergence::AbsentMulticellRendersEmpty => {
                "the golden has NO value at that path (an empty multi-cell collection is \
                 stored as a complex deletion with no cells, and SELECT reads null) while \
                 the egress renders a present EMPTY container in the declared type's own \
                 bracket frame"
            }
            Divergence::NestedFrozenUdtRendersAsBlobHex => {
                "the golden decodes the nested frozen UDT into an object while the egress \
                 renders its raw bytes as a CQL blob literal (`0x` + hex digits)"
            }
            Divergence::NonFiniteFloatRendersAsJsonNull => {
                "the golden carries a non-finite float token (`NaN`/`Infinity`/`-Infinity`) \
                 which JSON has no literal for, and the JSON egress renders null"
            }
            Divergence::DecimalRendersAsJsonString => {
                "the golden's decimal is an unquoted JSON number (DecimalType.toJSONString \
                 returns BigDecimal.toString()) while the JSON egress quotes the SAME \
                 number as a JSON string"
            }
        }
    }

    /// Is the pair at THIS position exactly the declared divergence?
    ///
    /// `ty`, `depth` and `kinding` are the position's own — the declared CQL type,
    /// CSV's empty-field depth and how the GOLDEN spells its JSON kind here — so
    /// every rule below is stated against the committed DDL rather than against a
    /// value's appearance.
    pub fn matched(
        self,
        golden: &Value,
        cli: &Value,
        ty: &CqlType,
        egress: Egress,
        depth: Depth,
        kinding: Kinding,
    ) -> bool {
        match self {
            Divergence::AbsentMulticellRendersEmpty => {
                // The golden side: NO value at all. A multi-cell collection is
                // always a whole column, so this is asked at the top level only —
                // and a FROZEN empty collection does persist as a present empty
                // value, which is why a golden `[]`/`{}` is not this gap.
                if !matches!(golden, Value::Null) || depth != Depth::TopLevel {
                    return false;
                }
                if !matches!(ty, CqlType::List(_) | CqlType::Set(_) | CqlType::Map(..)) {
                    return false;
                }
                match egress {
                    // The CLI spells every collection as a JSON array.
                    Egress::Json => matches!(cli, Value::Array(items) if items.is_empty()),
                    // Exactly the declared type's own empty bracket frame.
                    Egress::Csv => match (cli, csv_container::empty_rendering(ty)) {
                        (Value::String(text), Some(empty)) => *text == empty,
                        _ => false,
                    },
                }
            }
            Divergence::NestedFrozenUdtRendersAsBlobHex => {
                // The golden decoded an object at a position the DDL declares a
                // UDT, and the egress rendered a blob literal there.
                matches!(golden, Value::Object(_))
                    && matches!(ty, CqlType::Udt(_))
                    && matches!(cli, Value::String(text) if is_blob_hex(text))
            }
            Divergence::NonFiniteFloatRendersAsJsonNull => {
                egress == Egress::Json
                    && is_float_type(ty)
                    && matches!(golden, Value::String(token) if is_non_finite(token))
                    && matches!(cli, Value::Null)
            }
            Divergence::DecimalRendersAsJsonString => {
                if egress != Egress::Json || !is_decimal_type(ty) {
                    return false;
                }
                let Value::String(_) = cli else {
                    return false;
                };
                // The ONLY difference may be the JSON kind: read the CLI's string
                // with the relaxation the golden gets at a stringified position and
                // require the two canonical values to be EQUAL. A decimal whose
                // digits differ fails here and is reported as an ordinary diff.
                match (
                    canon_typed(golden, egress, ty, depth, kinding),
                    canon_typed(cli, egress, ty, depth, Kinding::Stringified),
                ) {
                    (Ok(g), Ok(c)) => g == c,
                    _ => false,
                }
            }
        }
    }
}

/// CQL's blob literal: `0x` and an EVEN number of hex digits (a byte string), and
/// nothing else. `0x` alone is a legal empty blob and is accepted; the point of the
/// check is that arbitrary text at that position is NOT this gap.
fn is_blob_hex(text: &str) -> bool {
    let Some(digits) = text.strip_prefix("0x") else {
        return false;
    };
    digits.len() % 2 == 0 && digits.chars().all(|c| c.is_ascii_hexdigit())
}

/// The three tokens a non-finite IEEE-754 float is spelled with in the golden —
/// Java's `Double.toString`/`Float.toString` spelling, as the committed
/// `signed_special_collections` golden carries them. Case-sensitive: these are
/// exact spellings, not a family of them.
fn is_non_finite(token: &str) -> bool {
    matches!(token, "NaN" | "Infinity" | "-Infinity")
}

/// Does the DDL declare a float/double at this position? The gap is about a
/// vocabulary JSON lacks for those two types, so it may not fire on any other.
fn is_float_type(ty: &CqlType) -> bool {
    matches!(ty, CqlType::Numeric(name) if name == "float" || name == "double")
}

fn is_decimal_type(ty: &CqlType) -> bool {
    matches!(ty, CqlType::Numeric(name) if name == "decimal")
}

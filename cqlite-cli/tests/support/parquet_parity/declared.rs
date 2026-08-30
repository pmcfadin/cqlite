//! THE declared-type-guided canonicalization entry point (issue #1490).
//!
//! # The invariant, and the three review rounds that produced it
//!
//! Both sides of this harness turn a raw value — a JSON value out of the
//! sstabledump golden, or an Arrow cell out of the exported Parquet file — into
//! a [`CanonicalValue`]. Neither raw form carries a CQL type: sstabledump
//! renders a `boolean` key as `"true"`, a `set<int>` element as `"-2"` and a
//! `timestamp` as a quoted string, while Arrow renders both a `varint` and a
//! whole-valued `decimal` as `Decimal128(38, 0)`. Every one of those is
//! ambiguous WITHOUT the column's DECLARED CQL type, and inferring the type from
//! the value's bytes is the no-heuristics violation of issue #28.
//!
//! **So: every position where a raw value becomes a `CanonicalValue` MUST route
//! through this module, carrying the [`CqlTypeSpec`] declared for THAT
//! position.** That is the invariant, and it is written here because it was
//! established the expensive way — three consecutive review rounds each found a
//! DIFFERENT position that canonicalized without consulting the declared type,
//! and each was patched where it was found:
//!
//!   * round 5 — top-level scalars, collection values and map keys typed a
//!     STRING by its spelling (a `text` cell spelling a timestamp became a
//!     `Timestamp`);
//!   * round 6 — primary-KEY components (`boolean`/`float`/`decimal` keys stayed
//!     `Text`, a false primary-key difference on every row);
//!   * round 7 — `Decimal128` decoding (scale-zero `decimal` compared as an
//!     integer) and multicell collection PATH components (only integral
//!     elements were converted, so `set<float>`/`set<decimal>` and
//!     boolean-keyed maps compared stringified text against typed values).
//!
//! The pattern is one defect, not three: a canonicalization site that does not
//! take a declared type. Hence ONE recursion per side, and the SEVEN positions
//! all of it flows through:
//!
//!   1. a top-level scalar cell — [`Declared::cell`];
//!   2. a collection ELEMENT (list/set) — internal, from the parent's `elem`;
//!   3. a map VALUE — internal, from the parent's `value`;
//!   4. a map KEY — internal, from the parent's `key`;
//!   5. a multicell collection PATH component — [`Declared::collection_path`];
//!   6. a primary-key component — [`Declared::primary_key`];
//!   7. a tuple / UDT field — internal; a UDT's FIELD types are genuinely not
//!      declared to the harness, so that position carries an explicitly NAMED
//!      absence ([`DeclaredType::Unavailable`]) rather than a silent guess.
//!
//! # What makes bypassing it structurally hard
//!
//! * The two canonicalizers ([`canonicalize_golden`], [`canonicalize_arrow`] and
//!   the `Decimal128` door [`canonicalize_arrow_decimal`]) take a `&Declared`.
//!   There is no spec-free overload, so a new call site cannot compile without
//!   deciding which declared type applies.
//! * `Declared`'s public constructors all REQUIRE a `&CqlTypeSpec`. The
//!   spec-less constructor is private to this module and is reachable only from
//!   the recursion, which must name a REASON the declared type does not exist.
//! * The child position of every container is built INSIDE the recursion, from
//!   the parent's spec — so adding a nested position means adding an arm that
//!   already has the spec in hand.
//! * Every ambiguous decision REFUSES (`Err`) when the declared type is
//!   unavailable, instead of picking a default: an accidental bypass fails
//!   closed and loudly rather than comparing something it guessed.
//! * The per-pass helpers this module absorbed (string typing, number
//!   canonicalization, frozen-map reshaping, stringified-scalar conversion) are
//!   private to it. Nothing outside can canonicalize a value one pass at a time,
//!   which is how a position came to run two of the three passes and not the
//!   third.

#![allow(dead_code)]

use arrow::array::Array;

use super::canonical_jsonl::{CanonicalValue, NormalizedFloat};
use super::cql_type::{ColumnType, CqlTypeSpec, SeqKind};
use super::decimal::{
    exact_from_decimal128, exact_from_text, is_canonical_text, ExactDecimal, EXPORT_DECIMAL_SCALE,
};
use super::golden_lexeme::Lex;
use super::golden_rows::fold_null;

/// WHERE a value sits. Two things depend on it: the diagnostic text, and
/// whether Cassandra renders the value at that position as a STRING (see
/// [`Position::is_stringified`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Position {
    /// A top-level cell of a declared column.
    Cell,
    /// An element of a list/set.
    Element,
    /// A map entry's key, in a value that is ALREADY a canonical map.
    MapKey,
    /// A map entry's value.
    MapValue,
    /// A multicell collection PATH component — a set element, or a map key,
    /// as sstabledump writes it: STRINGIFIED.
    CollectionPath,
    /// A partition-key or clustering component — STRINGIFIED.
    PrimaryKey,
    /// A frozen map's JSON-OBJECT key — STRINGIFIED (a JSON object key always
    /// is).
    FrozenMapObjectKey,
    /// A positional CQL tuple member.
    TupleField(usize),
    /// A named UDT field.
    UdtField(String),
}

impl Position {
    /// Does Cassandra render this position's value as a STRING that has to be
    /// converted back through the declared type?
    ///
    /// `true` for the three positions where it does: every primary-key
    /// component and every collection PATH component goes through
    /// `AbstractType.getString` (so an `int` is `"1"`, a `boolean` `"true"`, a
    /// `float` `Float.toString`, a `decimal` `BigDecimal.toString`), and a JSON
    /// object key is a string by JSON's own rules. Everywhere else sstabledump
    /// writes a TYPED JSON value, which must NOT be re-parsed out of its text.
    pub fn is_stringified(&self) -> bool {
        matches!(
            self,
            Position::CollectionPath | Position::PrimaryKey | Position::FrozenMapObjectKey
        )
    }

    fn describe(&self) -> String {
        match self {
            Position::Cell => "cell".to_string(),
            Position::Element => "collection element".to_string(),
            Position::MapKey => "map key".to_string(),
            Position::MapValue => "map value".to_string(),
            Position::CollectionPath => "collection path component".to_string(),
            Position::PrimaryKey => "primary-key component".to_string(),
            Position::FrozenMapObjectKey => "frozen map object key".to_string(),
            Position::TupleField(i) => format!("tuple field {i}"),
            Position::UdtField(name) => format!("UDT field '{name}'"),
        }
    }
}

/// The declared type at a position — or a NAMED absence.
///
/// Three-valued in spirit and two-valued in code: there is no "unknown, carry
/// on" state. `Unavailable` carries WHY, and every ambiguous decision refuses
/// on it (a positive verdict requires an affirmative measurement).
#[derive(Debug, Clone)]
pub enum DeclaredType<'a> {
    Known(&'a CqlTypeSpec),
    /// The declared type genuinely does not exist for this position — today only
    /// a UDT's field types, which the harness models as a NAME only.
    Unavailable(&'static str),
}

/// A position plus the declared type that governs it plus the diagnostic
/// context. The single argument both canonicalizers take.
#[derive(Debug, Clone)]
pub struct Declared<'a> {
    ty: DeclaredType<'a>,
    position: Position,
    ctx: String,
}

impl<'a> Declared<'a> {
    /// Position 1: a top-level cell of a declared column.
    pub fn cell(spec: &'a CqlTypeSpec, ctx: impl Into<String>) -> Self {
        Self::known(spec, Position::Cell, ctx)
    }

    /// Position 6: a partition-key or clustering component, which sstabledump
    /// renders as a quoted string.
    pub fn primary_key(spec: &'a CqlTypeSpec, ctx: impl Into<String>) -> Self {
        Self::known(spec, Position::PrimaryKey, ctx)
    }

    /// Position 5: a multicell collection path component (a set element or a map
    /// key), which sstabledump also renders as a quoted string.
    pub fn collection_path(spec: &'a CqlTypeSpec, ctx: impl Into<String>) -> Self {
        Self::known(spec, Position::CollectionPath, ctx)
    }

    /// Position 2/3: an element of a non-frozen list, or a map entry's value,
    /// assembled from a per-element cell's VALUE (already typed JSON).
    pub(super) fn element(spec: &'a CqlTypeSpec, ctx: impl Into<String>) -> Self {
        Self::known(spec, Position::Element, ctx)
    }

    pub(super) fn map_value(spec: &'a CqlTypeSpec, ctx: impl Into<String>) -> Self {
        Self::known(spec, Position::MapValue, ctx)
    }

    fn known(spec: &'a CqlTypeSpec, position: Position, ctx: impl Into<String>) -> Self {
        Declared {
            ty: DeclaredType::Known(spec),
            position,
            ctx: ctx.into(),
        }
    }

    /// PRIVATE on purpose: a spec-less position must be created by the recursion
    /// that knows WHY the declared type does not exist. No outside caller can
    /// manufacture one, so "I had no spec here" is never a way past this module.
    fn unavailable(why: &'static str, position: Position, ctx: impl Into<String>) -> Self {
        Declared {
            ty: DeclaredType::Unavailable(why),
            position,
            ctx: ctx.into(),
        }
    }

    /// A child position, with the child's declared type derived from this one's.
    fn child(&self, spec: Option<&'a CqlTypeSpec>, position: Position, ctx: String) -> Self {
        match spec {
            Some(spec) => Declared {
                ty: DeclaredType::Known(spec),
                position,
                ctx,
            },
            None => Declared {
                ty: DeclaredType::Unavailable(
                    "the parent's declared type does not describe this position",
                ),
                position,
                ctx,
            },
        }
    }

    pub fn ctx(&self) -> &str {
        &self.ctx
    }

    pub fn position(&self) -> &Position {
        &self.position
    }

    fn spec(&self) -> Option<&'a CqlTypeSpec> {
        match self.ty {
            DeclaredType::Known(spec) => Some(spec),
            DeclaredType::Unavailable(_) => None,
        }
    }

    /// The declared SCALAR type name at this position, if the declared type is a
    /// scalar at all.
    fn scalar(&self) -> Option<&'a str> {
        match self.spec() {
            Some(CqlTypeSpec::Scalar(name)) => Some(name.as_str()),
            _ => None,
        }
    }

    /// Does a value at THIS position have to keep its literal TEXT, because
    /// `serde_json`'s parse would destroy it?
    ///
    /// True for exactly two declared scalars, and the decision is taken HERE —
    /// at a position, from that position's declared type — rather than per
    /// COLUMN, so a `map<decimal,int>` preserves its declared-`decimal`
    /// positions and leaves its `int` values alone (round 11).
    ///
    /// * `decimal` — `sstabledump` writes a decimal CELL as a bare JSON number,
    ///   which the shared parser turns into an `f64`, and an `f64` cannot
    ///   identify the decimal it came from (`0.100000000000000001` and `0.1`
    ///   are ONE double).
    /// * `varint` — a `varint` above `u64::MAX` also becomes an `f64`
    ///   (`Number::as_i64` and `as_u64` both fail), while the exported side
    ///   reads that column back as an exact `Decimal128(38, 0)` `Int`. So the
    ///   comparison was `Float` vs `Int` — a false mismatch — and the digits
    ///   that would have shown a real corruption were already gone.
    ///
    /// Nothing else, deliberately: a `float`/`double` literal MUST reach
    /// `serde_json`'s (exact, `float_roundtrip`) number parser unchanged, which
    /// is what keeps the exact-bit float comparison exact.
    fn preserves_exact_lexeme(&self) -> bool {
        matches!(self.scalar(), Some("decimal") | Some("varint"))
    }

    /// A refusal naming the position, the declared type (or its absence) and the
    /// context — used wherever a decision NEEDS the declared type.
    fn refuse(&self, what: &str) -> String {
        let ty = match &self.ty {
            DeclaredType::Known(spec) => format!("declared type {spec:?}"),
            DeclaredType::Unavailable(why) => {
                format!("NO declared type is available here ({why})")
            }
        };
        format!(
            "{}: at the {} — {ty} — {what}",
            self.ctx,
            self.position.describe()
        )
    }
}

// ---------------------------------------------------------------------------
// The GOLDEN side
// ---------------------------------------------------------------------------

/// Canonicalize ONE raw golden value at ONE declared position, recursively.
///
/// Five things happen here, in this order, and the order is load-bearing:
///
///   1. an explicit JSON `null` folds into `Absent` (Arrow has one null, and CQL
///      does not distinguish the two);
///   2. at a STRINGIFIED position ([`Position::is_stringified`]) the value's
///      TEXT is converted through the declared scalar type — this is the round-6
///      key-component conversion and the round-7 path conversion, now one
///      function;
///   3. a shape JSON cannot carry is reshaped: a frozen map arrives as a JSON
///      OBJECT, which the shared parser necessarily reads as a `Tuple`, while
///      the Arrow side reads the same column back as a `Map`;
///   4. containers RECURSE, each child at its own position with its own child
///      spec;
///   5. scalars are typed from the declared type — a `Timestamp` only where
///      `timestamp` is declared, a `float` re-narrowed to 32 bits, a `decimal`
///      read from its PRESERVED LITERAL into an EXACT unscaled/scale pair, and
///      REFUSED if it arrives as an `f64`.
///
/// # Idempotent by construction
///
/// A multicell collection's PATH components are canonicalized where they are
/// assembled (only there is it known that they are stringified), and the
/// assembled column value then goes through this function again. That is safe
/// because every rule here is idempotent: `Int` stays `Int`, an exact decimal is
/// already a tagged `Text`, narrowing an `f32`-widened double again is the
/// identity, and a non-stringified position never re-parses text.
pub fn canonicalize_golden(
    raw: CanonicalValue,
    at: &Declared<'_>,
) -> Result<CanonicalValue, String> {
    let value = fold_null(raw);
    let value = if at.position.is_stringified() {
        type_stringified_scalar(value, at)?
    } else {
        value
    };
    match (value, at.spec()) {
        // A frozen map's JSON object (read as a `Tuple` of string-keyed fields)
        // becomes a canonical `Map`. The keys are STRINGS by JSON's own rules,
        // so each goes back through the declared key type at the
        // `FrozenMapObjectKey` position — never blindly, so a `map<text,int>`
        // key "5" stays `Text` and can never equal an integer key 5.
        //
        // ORDER is preserved, and the comparison depends on it: the workspace
        // pins `serde_json`'s `preserve_order`, so the golden's object order IS
        // the order sstabledump wrote (Cassandra's key-comparator order), which
        // is the order the Arrow map carries.
        (CanonicalValue::Tuple(fields), Some(CqlTypeSpec::Map { key, value })) => {
            let mut out = Vec::with_capacity(fields.len());
            for (k, v) in fields {
                let kc = canonicalize_golden(
                    CanonicalValue::Text(k.clone()),
                    &at.child(
                        Some(key),
                        Position::FrozenMapObjectKey,
                        format!("{}.key({k})", at.ctx),
                    ),
                )?;
                let vc = canonicalize_golden(
                    v,
                    &at.child(Some(value), Position::MapValue, format!("{}.{k}", at.ctx)),
                )?;
                out.push((kc, vc));
            }
            Ok(CanonicalValue::Map(out))
        }
        (CanonicalValue::Map(kvs), Some(CqlTypeSpec::Map { key, value })) => {
            let mut out = Vec::with_capacity(kvs.len());
            for (i, (k, v)) in kvs.into_iter().enumerate() {
                let kc = canonicalize_golden(
                    k,
                    &at.child(Some(key), Position::MapKey, format!("{}.key[{i}]", at.ctx)),
                )?;
                let vc = canonicalize_golden(
                    v,
                    &at.child(
                        Some(value),
                        Position::MapValue,
                        format!("{}.value[{i}]", at.ctx),
                    ),
                )?;
                out.push((kc, vc));
            }
            Ok(CanonicalValue::Map(out))
        }
        (CanonicalValue::List(xs), Some(CqlTypeSpec::Seq { elem, .. })) => {
            Ok(CanonicalValue::List(recurse_seq(xs, elem, at)?))
        }
        (CanonicalValue::Set(xs), Some(CqlTypeSpec::Seq { elem, .. })) => {
            Ok(CanonicalValue::Set(recurse_seq(xs, elem, at)?))
        }
        // A CQL tuple arrives as a POSITIONAL JSON array, matched member-wise.
        // (Its outer shape still differs from the Arrow `Struct` the export
        // produces, which is why such a column's VALUES are refused by name in
        // `unsupported.rs` rather than compared — but the members are still
        // canonicalized from their declared types, so the refusal is the only
        // thing standing between the two, not a missing conversion.)
        (CanonicalValue::List(xs), Some(CqlTypeSpec::Tuple(specs))) if xs.len() == specs.len() => {
            let mut out = Vec::with_capacity(xs.len());
            for (i, (x, s)) in xs.into_iter().zip(specs.iter()).enumerate() {
                out.push(canonicalize_golden(
                    x,
                    &at.child(Some(s), Position::TupleField(i), format!("{}.{i}", at.ctx)),
                )?);
            }
            Ok(CanonicalValue::List(out))
        }
        // A UDT arrives as ONE JSON object whose field values sstabledump has
        // already typed, and the harness models a UDT as a NAME only — so each
        // field's declared type is genuinely absent. Recorded as an explicit,
        // NAMED absence rather than a guess: the fields keep the representation
        // the shared parser produced, and if that is ever wrong it shows up as a
        // loud value difference. (Nothing in the corpus reaches here today: the
        // UDT columns sit behind the #3556 whole-case gap.)
        (CanonicalValue::Tuple(fields), Some(CqlTypeSpec::Udt(_))) => {
            let mut out = Vec::with_capacity(fields.len());
            for (name, v) in fields {
                let child = Declared::unavailable(
                    "a UDT's FIELD types are not declared to the harness — a frozen UDT \
                     arrives as one JSON object whose values sstabledump has already typed",
                    Position::UdtField(name.clone()),
                    format!("{}.{name}", at.ctx),
                );
                out.push((name, canonicalize_golden(v, &child)?));
            }
            Ok(CanonicalValue::Tuple(out))
        }
        (value, _) => type_scalar_golden(value, at),
    }
}

fn recurse_seq(
    xs: Vec<CanonicalValue>,
    elem: &CqlTypeSpec,
    at: &Declared<'_>,
) -> Result<Vec<CanonicalValue>, String> {
    let mut out = Vec::with_capacity(xs.len());
    for (i, x) in xs.into_iter().enumerate() {
        out.push(canonicalize_golden(
            x,
            &at.child(Some(elem), Position::Element, format!("{}[{i}]", at.ctx)),
        )?);
    }
    Ok(out)
}

/// Convert a STRINGIFIED position's text into the variant its declared scalar
/// type denotes (positions 5, 6 and the frozen-map object key).
///
/// It converts, it never guesses: the conversion is driven entirely by the
/// DECLARED type, and a value that does not denote that type is a REFUSAL, never
/// a fallback to the string — a declared `boolean` holding `"maybe"` is a broken
/// fixture or a broken declaration, and comparing it as text would hide both.
/// A NON-scalar declared type (a `frozen<list<text>>` clustering key) is left
/// untouched: that is a refusal to guess, and the case declaring one is
/// enumerated as uncovered rather than silently mishandled.
fn type_stringified_scalar(v: CanonicalValue, at: &Declared<'_>) -> Result<CanonicalValue, String> {
    let (CanonicalValue::Text(s), Some(name)) = (&v, at.scalar()) else {
        return Ok(v);
    };
    let refuse = |what: &str| {
        Err(at.refuse(&format!(
            "the golden renders the value as {s:?}, which is not {what}; the harness refuses \
             to compare it as text rather than hide the disagreement"
        )))
    };
    match name {
        // Cassandra's `BooleanType.getString` is Java's `Boolean.toString`.
        "boolean" => match s.as_str() {
            "true" => Ok(CanonicalValue::Bool(true)),
            "false" => Ok(CanonicalValue::Bool(false)),
            _ => refuse("'true' or 'false'"),
        },
        // 32-bit narrowing for `float` is applied by `type_scalar_golden`, which
        // runs next in the same descent — the same rule a `float` CELL goes
        // through, so a key/path and a cell cannot diverge.
        "float" | "double" => match s.parse::<f64>() {
            Ok(f) if f.is_finite() => Ok(CanonicalValue::Float(NormalizedFloat(f))),
            _ => refuse("a finite decimal number"),
        },
        // Exact, with nothing to refuse for ambiguity: the literal TEXT is
        // present at every golden decimal position — written by Cassandra's
        // `BigDecimal.toString` here, and preserved by `golden_lexeme.rs` for a
        // decimal CELL, which `sstabledump` writes as a bare JSON number.
        "decimal" => Ok(exact_from_text(s, EXPORT_DECIMAL_SCALE, &at.ctx)?.canonical()),
        "int" | "bigint" | "smallint" | "tinyint" | "varint" | "counter" => {
            match s.parse::<i128>() {
                Ok(i) => Ok(CanonicalValue::Int(i)),
                Err(_) => refuse("an integer"),
            }
        }
        // text/varchar/ascii/blob/uuid/timeuuid/date/time/inet/duration all
        // compare as `Text` on the Arrow side, and `timestamp` is settled by
        // `type_scalar_golden` — nothing to convert.
        _ => Ok(v),
    }
}

/// Type a golden SCALAR from its declared type: the string rule (round 5) and
/// the number rules, in one place.
///
/// * A golden value is a `Timestamp` **iff** its declared type IS `timestamp`.
///   The shared JSON parser has to guess from the SPELLING (its other lanes have
///   no schema), and a `text` value that legally spells a timestamp became a
///   `Timestamp` on the golden side while the Arrow `Utf8` side stayed `Text` —
///   a false failure produced by inferring a type from a value's bytes (#28).
///   Nothing is weakened: the restored value is the string the golden literally
///   carried, and a declared `timestamp` still compares as an INSTANT.
/// * A `float` re-narrows to 32 bits. sstabledump prints Java's
///   `Float.toString`, which round-trips through 32 bits; parsed as JSON it is
///   the nearest DOUBLE to that text, which is not the double a widened `f32`
///   gives (1.84 vs 1.8399999141693115). Narrowing makes both sides hold the
///   same double WITHOUT a tolerance — the comparison stays exact-bit.
/// * A `decimal` becomes an EXACT decimal, and NOTHING about it goes through an
///   `f64` (see `decimal.rs` and `golden_lexeme.rs`): the literal's TEXT is
///   preserved before the shared parser sees it, so a decimal arrives here as
///   that text and is read exactly by `exact_from_text`. An integer-shaped
///   literal (`Int`) is already exact. A decimal that arrives as a DOUBLE is
///   REFUSED, because a double cannot identify the decimal it was parsed from —
///   `0.100000000000000001` and `0.1` are the same double, so the recovery this
///   replaced (round 4→10) canonicalized the first as the second and would have
///   passed a lossy export.
/// * A `varint` stays an INTEGER domain on both sides — but a `varint` above
///   `u64::MAX` cannot survive `serde_json`'s number parse either, so its
///   literal is preserved by the same mechanism and read back here with an
///   exact `i128` parse. A `varint` that arrives as a DOUBLE is REFUSED for the
///   same reason a decimal is: the digits are already gone.
fn type_scalar_golden(v: CanonicalValue, at: &Declared<'_>) -> Result<CanonicalValue, String> {
    let Some(name) = at.scalar() else {
        // No declared SCALAR here: either a container value under a container
        // declaration whose shapes disagree (which the value comparison must
        // report as the difference it is), or a position whose declared type is
        // genuinely unavailable. Either way, EXACTLY as it was — this function
        // never invents a type.
        return Ok(v);
    };
    // A `Timestamp` is only a timestamp where `timestamp` is DECLARED; anywhere
    // else it is the text the golden literally carried, which the rules below
    // then apply to. Done first and unconditionally so no later rule can be
    // skipped by the shared parser's spelling-based timestamp recognition.
    let v = match v {
        CanonicalValue::Timestamp { micros, raw } => {
            if name == "timestamp" {
                return Ok(CanonicalValue::Timestamp { micros, raw });
            }
            CanonicalValue::Text(raw)
        }
        other => other,
    };
    Ok(match (v, name) {
        (CanonicalValue::Float(NormalizedFloat(f)), "float") => {
            CanonicalValue::Float(NormalizedFloat(f as f32 as f64))
        }
        (CanonicalValue::Float(NormalizedFloat(f)), "decimal") => {
            return Err(at.refuse(&format!(
                "the golden decimal arrived as the double {f:?}, i.e. its LITERAL TEXT was \
                 lost before the harness saw it. A double cannot identify the decimal it was \
                 parsed from (0.100000000000000001 and 0.1 are the same double), so the \
                 harness REFUSES rather than recover one — recovery is what let a lossy \
                 export compare equal. The literal is preserved by \
                 golden_lexeme::preserve_decimal_lexemes, which every golden goes through; \
                 reaching here means this value bypassed it"
            )));
        }
        // The PRESERVED LITERAL (or, at a stringified position, the text
        // Cassandra's `BigDecimal.toString` wrote) — read exactly.
        // `is_canonical_text` keeps the descent idempotent: a stringified
        // position is converted where it is assembled and passes through here
        // again already canonical.
        (CanonicalValue::Text(s), "decimal") => {
            if is_canonical_text(&s) {
                CanonicalValue::Text(s)
            } else {
                exact_from_text(&s, EXPORT_DECIMAL_SCALE, &at.ctx)?.canonical()
            }
        }
        (CanonicalValue::Int(i), "decimal") => ExactDecimal::from_i128(i).canonical(),
        // A `varint`'s PRESERVED LITERAL. The exported side reads a
        // `Decimal128(38, 0)` back as an `Int`, so the golden lands on the same
        // exact `i128` — no `f64` in either path. A literal too large for an
        // `i128` is REFUSED: `Decimal128` could not have carried it either, so
        // there is nothing to compare it against.
        (CanonicalValue::Text(s), "varint") => match s.parse::<i128>() {
            Ok(i) => CanonicalValue::Int(i),
            Err(_) => {
                return Err(at.refuse(&format!(
                    "the golden varint literal '{s}' does not fit an i128, which is \
                     the unscaled range of the Decimal128(38, 0) the export writes a \
                     `varint` to; the harness refuses to compare rather than truncate"
                )))
            }
        },
        (CanonicalValue::Float(NormalizedFloat(f)), "varint") => {
            return Err(at.refuse(&format!(
                "the golden varint arrived as the double {f:?}, i.e. its \
                 LITERAL TEXT was lost before the harness saw it — a `varint` above \
                 u64::MAX is parsed by serde_json as an f64, while the export writes it \
                 as an exact Decimal128(38, 0). The literal is preserved by \
                 golden_lexeme::preserve_exact_lexemes, which every golden goes through; \
                 reaching here means this value bypassed it"
            )));
        }
        (other, _) => other,
    })
}

// ---------------------------------------------------------------------------
// LEXEME PRESERVATION — the SAME descent, one stage earlier
// ---------------------------------------------------------------------------
//
// # Why this lives here and not in `golden_lexeme.rs`
//
// `golden_lexeme.rs` owns the LEXICAL machinery (a JSON reader that retains
// every scalar's original text, and an emitter that puts it back verbatim). It
// does NOT own the question "must this number keep its literal?", because that
// question is answered by a POSITION's declared type — and this module is the
// single place that maps positions to declared types.
//
// The first version of the lexeme pass got this wrong in exactly the way rounds
// 5–7 got the canonicalization wrong: it asked "does this COLUMN mention a
// decimal anywhere?" and then quoted every number in the column's value. That
// is coarse where the declared type is precise, and it produced a real false
// failure — a `map<decimal,int>` had its `int` VALUES turned into strings. One
// declared-type recursion, one set of positions, one answer; a second walker
// beside it is the defect, not a detail of it.

/// Preserve the literal text of every number sitting at a position whose
/// declared type is `decimal` or `varint` — and NO other number.
///
/// Recurses in lockstep with [`canonicalize_golden`], over the same declared
/// type, deriving each child position with the same private [`Declared::child`]:
/// a sequence's `Element`, a frozen map's `MapValue`, a tuple's `TupleField`,
/// and a UDT field's explicitly NAMED absence. So the two descents cannot
/// disagree about which position carries which declared type.
pub(super) fn preserve_lexemes(value: &mut Lex, at: &Declared<'_>) {
    if at.preserves_exact_lexeme() {
        // A declared SCALAR: this position IS the value, and a scalar has no
        // children to descend into.
        value.quote_number_lexeme();
        return;
    }
    match (value, at.spec()) {
        // A frozen list/set arrives as a JSON ARRAY of typed values.
        (Lex::Arr(items), Some(CqlTypeSpec::Seq { elem, .. })) => {
            for (i, item) in items.iter_mut().enumerate() {
                let child = at.child(Some(elem), Position::Element, format!("{}[{i}]", at.ctx));
                preserve_lexemes(item, &child);
            }
        }
        // A CQL tuple arrives as a POSITIONAL JSON array; a length disagreement
        // is a difference for the value comparison to report, never something to
        // guess past here.
        (Lex::Arr(items), Some(CqlTypeSpec::Tuple(specs))) if items.len() == specs.len() => {
            for (i, (item, spec)) in items.iter_mut().zip(specs.iter()).enumerate() {
                let child = at.child(
                    Some(spec),
                    Position::TupleField(i),
                    format!("{}.{i}", at.ctx),
                );
                preserve_lexemes(item, &child);
            }
        }
        // A frozen map arrives as a JSON OBJECT. Its KEYS are strings by JSON's
        // own rules — a `map<decimal,int>` key's literal is therefore ALREADY
        // preserved, and read exactly at the `FrozenMapObjectKey` position — so
        // only the VALUES can be bare numbers here. A non-frozen map's key is
        // preserved the same way, as Cassandra's stringified `path` component.
        (Lex::Obj(fields), Some(CqlTypeSpec::Map { value, .. })) => {
            for (_, key, field) in fields.iter_mut() {
                let child = at.child(Some(value), Position::MapValue, format!("{}.{key}", at.ctx));
                preserve_lexemes(field, &child);
            }
        }
        // A UDT's FIELD types are not declared to the harness, so no position
        // inside one can be known to be a `decimal`/`varint`. Descended anyway,
        // with the same NAMED absence `canonicalize_golden` uses, so the two
        // recursions stay structurally identical — nothing is quoted, and the
        // matching absence in `canonicalize_golden` means nothing needs to be.
        (Lex::Obj(fields), Some(CqlTypeSpec::Udt(_))) => {
            for (_, name, field) in fields.iter_mut() {
                let child = Declared::unavailable(
                    "a UDT's FIELD types are not declared to the harness — a frozen UDT \
                     arrives as one JSON object whose values sstabledump has already typed",
                    Position::UdtField(name.clone()),
                    format!("{}.{name}", at.ctx),
                );
                preserve_lexemes(field, &child);
            }
        }
        _ => {}
    }
}

/// The declared POSITION of the `value` field of ONE `sstabledump` cell of
/// `col` — or `None` when that cell's `value` carries nothing typed.
///
/// This is the same split `golden_rows::project_column` makes when it assembles
/// a column, kept here because it is a statement about declared types: a
/// non-frozen collection is dumped as one cell PER ELEMENT, so such a cell's
/// `value` is an ELEMENT or a MAP VALUE, not the whole column. Getting it from
/// the column's own spec (as the first version of the lexeme pass did) would
/// quote a `list<decimal>` element's literal only by accident and a
/// `map<decimal,int>` value's wrongly.
///
/// `None` for a non-frozen SET, whose elements live entirely in the stringified
/// `path` (the cell's `value` is the empty string), and for a multicell column
/// whose parsed type is not a collection at all — a disagreement
/// `project_column` reports as the error it is.
pub(super) fn cell_value_declared<'a>(col: &'a ColumnType, ctx: String) -> Option<Declared<'a>> {
    if !col.is_multicell_collection() {
        return Some(Declared::cell(&col.spec, ctx));
    }
    match &col.spec {
        CqlTypeSpec::Seq {
            kind: SeqKind::Set, ..
        } => None,
        CqlTypeSpec::Seq {
            kind: SeqKind::List,
            elem,
        } => Some(Declared::element(elem, ctx)),
        CqlTypeSpec::Map { value, .. } => Some(Declared::map_value(value, ctx)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// The ARROW side
// ---------------------------------------------------------------------------

/// Canonicalize ONE exported Arrow cell at ONE declared position.
///
/// The Arrow decode is mostly STRUCTURAL (an `Int32Array` can only be an
/// integer), so the declared type is threaded through for the decisions that are
/// genuinely ambiguous — today `Decimal128`, whose scale-zero form is both a
/// `varint` and a whole-valued `decimal`. Threading it everywhere anyway is the
/// point of this module: the next ambiguous representation gets the declared
/// type for free instead of becoming review round 8.
pub fn canonicalize_arrow(
    array: &dyn Array,
    row: usize,
    at: &Declared<'_>,
) -> Result<CanonicalValue, String> {
    super::arrow_rows::decode_declared(array, row, at)
}

/// The `Decimal128` door, reachable with an unscaled value and a scale so the
/// decimal rules can be exercised without building an Arrow array — still only
/// with a `&Declared`, so it is part of the entry point rather than a way round
/// it.
///
/// # The defect this closes (issue #1490 round 7)
///
/// Every scale-zero `Decimal128` used to canonicalize as an `Int`, on the
/// grounds that scale zero is `varint`'s mapping. But `arrow_expect` accepts
/// `Decimal128(p, s)` for ANY `s >= 0` for a declared `decimal` — deliberately,
/// since Arrow carries one scale per COLUMN — so a perfectly valid `decimal`
/// column exported at scale 0 passed the TYPE check and then compared `Int(n)`
/// against the golden's exact `decimal(n)`: a false VALUE failure on real data.
/// The declared type settles it, and an unavailable declared type REFUSES rather
/// than picking one of the two.
pub fn canonicalize_arrow_decimal(
    unscaled: i128,
    scale: i8,
    at: &Declared<'_>,
) -> Result<CanonicalValue, String> {
    match at.scalar() {
        // An integer domain on both sides: the golden's integer literal stays an
        // `Int`, so converting it to a decimal would be the type confusion the
        // canonical space exists to prevent.
        Some("varint") => {
            if scale != 0 {
                return Err(at.refuse(&format!(
                    "the export wrote Decimal128 scale {scale}, but a `varint` is an INTEGER \
                     domain and the harness pins it to scale 0 (arrow_expect); refusing to \
                     rescale rather than compare two different numbers"
                )));
            }
            Ok(CanonicalValue::Int(unscaled))
        }
        // EXACT unscaled/scale pair, at ANY scale INCLUDING ZERO — no `f64`
        // anywhere. `ExactDecimal` normalizes, so scale 0 lands on exactly the
        // value the golden's integer-shaped literal recovers to.
        Some("decimal") => Ok(exact_from_decimal128(unscaled, scale, &at.ctx)?.canonical()),
        _ => Err(at.refuse(
            "an exported Decimal128 cell is ambiguous without the declared type — \
             scale-zero is both a `varint` and a whole-valued `decimal` — so the harness \
             refuses to decode it rather than guess which one it is",
        )),
    }
}

/// Build the child position for an Arrow container member, from the parent's
/// declared type. Used by `arrow_rows`' structural decode, so the two sides
/// derive their child positions the SAME way.
pub(super) fn arrow_child<'a>(
    at: &Declared<'a>,
    spec: Option<&'a CqlTypeSpec>,
    position: Position,
    ctx: String,
) -> Declared<'a> {
    at.child(spec, position, ctx)
}

/// The declared ELEMENT type of a sequence, or `None` when the declared type is
/// not a sequence at all (an Arrow shape that disagrees with the declaration —
/// reported by the TYPE stage, not silently fixed here).
pub(super) fn seq_elem<'a>(at: &Declared<'a>) -> Option<&'a CqlTypeSpec> {
    match at.spec() {
        Some(CqlTypeSpec::Seq { elem, .. }) => Some(elem),
        _ => None,
    }
}

/// The declared KEY and VALUE types of a map, or `None`s when the declared type
/// is not a map.
pub(super) fn map_kv<'a>(at: &Declared<'a>) -> (Option<&'a CqlTypeSpec>, Option<&'a CqlTypeSpec>) {
    match at.spec() {
        Some(CqlTypeSpec::Map { key, value }) => (Some(key), Some(value)),
        _ => (None, None),
    }
}

/// The declared type of a struct member at position `i` with field name `name`:
/// a CQL tuple's member by POSITION, and a UDT field's as an explicitly NAMED
/// absence (a UDT's field types are not declared to the harness).
pub(super) fn struct_field<'a>(
    at: &Declared<'a>,
    i: usize,
    name: &str,
    field_count: usize,
) -> Declared<'a> {
    let ctx = format!("{}.{name}", at.ctx());
    match at.spec() {
        Some(CqlTypeSpec::Tuple(specs)) if specs.len() == field_count => {
            at.child(Some(&specs[i]), Position::TupleField(i), ctx)
        }
        Some(CqlTypeSpec::Udt(_)) => Declared::unavailable(
            "a UDT's FIELD types are not declared to the harness — only its NAME is",
            Position::UdtField(name.to_string()),
            ctx,
        ),
        _ => at.child(None, Position::UdtField(name.to_string()), ctx),
    }
}

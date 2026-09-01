//! DECLARED GAPS: what each one says the divergence IS, in a form the walk can
//! CHECK, and what the walk observed it to do (issue #1491, review round 17).
//!
//! Two halves of one responsibility, which is why they share a file: the
//! [`Divergence`] a gap declares, and the bookkeeping ([`SkipPaths`],
//! [`Observed`], [`Suppressions`]) that decides from the walk's own outcome
//! whether the gap is APPLIED or STALE. The comparator asks; nothing here decides
//! how to compare.
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

use super::super::container::{golden_map_key_value, is_container_type, MapKeySpelling};
use super::super::schema::CqlType;
use super::{canon_typed, csv_container, Depth, Egress, Kinding, Side};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::BTreeMap;

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
    /// This is the ONE place the CLI side is read with [`Kinding::Stringified`],
    /// the relaxation reserved for the golden (`super::compare_value_body` holds
    /// the CLI to [`Kinding::Natural`] at every position — finding M1). It is
    /// scoped to deciding THIS gap's own question, "is the kind the only
    /// difference?", and it decides nothing else: a non-match here becomes an
    /// ordinary diff, compared under the normal asymmetry.
    ///
    /// NOT COVERED: a different number, a null, a non-numeric string, and the CSV
    /// lane (where every cell is text and the 30-digit values match exactly).
    DecimalRendersAsJsonString,
    /// A frozen value nested inside a multi-cell collection is left UNDECODED by
    /// the golden as a flat scalar, while the egress decodes it into a structure.
    ///
    /// ORACLE: `sstabledump` does not descend into a frozen value that sits inside
    /// a multi-cell collection. The element lives in the cell PATH, where Cassandra
    /// wrote its raw serialized bytes, so the golden carries a flat STRING at a
    /// position the DDL declares a container — hex digits for a nested frozen
    /// collection (`set<frozen<set<key_part>>>` gives `000000020000...`) and
    /// sstabledump's colon-joined tuple spelling for a nested frozen tuple
    /// (`set<frozen<tuple<frozen<key_part>, int>>>` gives `alpha\:1:1`).
    ///
    /// EGRESS SHAPE: a DECODED JSON **array** at that same position. All four
    /// declared types this variant admits (list/set/map/tuple) are rendered by the
    /// CLI as arrays; an object is not one of their spellings, so accepting one
    /// would excuse a regression rather than describe a divergence.
    ///
    /// NOTE THE DIRECTION. It is the OPPOSITE of
    /// [`Divergence::NestedFrozenUdtRendersAsBlobHex`], where the GOLDEN decodes and
    /// the egress emits hex. Here the golden leaves the value undecoded and the
    /// egress decodes it. The two are not interchangeable, and declaring the wrong
    /// one matches nothing and is reported stale (issue #3500).
    ///
    /// NOT COVERED: a null, a number, a golden that DID decode (the two sides are
    /// then compared normally), and an egress that also emitted a flat scalar (then
    /// there is no shape difference to excuse). DECLARED RESIDUAL: the CONTENT
    /// behind the golden's flat scalar is not compared — recovering it would mean
    /// re-implementing Cassandra's collection and tuple serializers here. This gap
    /// costs the nested value's content; what it does not cost is the shape.
    NestedFrozenValueLeftUndecodedByGolden,
    /// A MULTICELL map's container-typed KEY is left UNDECODED by the golden as
    /// `getString`'s flat text, while the egress renders the key's RAW BYTES as a
    /// CQL blob literal. NEITHER SIDE DECODES IT.
    ///
    /// **A VALUE disagreement, not a lane limitation.** It replaces the retired
    /// `ContainerMapKeyNotPairableByThisLane`, which said this lane had no rule for
    /// pairing a container map key at all — true until issue #3726, and false now:
    /// `super::compare_map` pairs one through `container::golden_map_key_value`, and
    /// the three FROZEN container-keyed columns of
    /// `test_nested_udt_keys.nested_udt_keys` are compared in full, in both formats.
    /// This one column is different, and the difference is measured rather than
    /// structural.
    ///
    /// ORACLE, both halves from the pin. A non-frozen map's entries are separate
    /// cells whose KEY is the cell PATH, and
    /// `cassandra-5.0.8 JsonTransformer.serializeCell` writes a cell path with
    /// `writeString(ct.nameComparator().getString(...))` — `getString`, not
    /// `toJSONString` — so the golden carries `TupleType.getString`'s colon-joined,
    /// escaped spelling (`"charlie\:3:8"` in the committed golden) and no JSON
    /// document at all. That is why the golden side of this variant is stated as
    /// "every object key is NOT the declared key type's `toJSONString` spelling",
    /// asked through the same `container::golden_map_key_value` the comparison pairs
    /// with: a golden that DID decode (i.e. a future `sstabledump` writing the
    /// toJSONString form, or a frozen column mis-declared here) is not this gap and
    /// is compared normally.
    ///
    /// EGRESS SHAPE: the `{key,value}` array both formats produce — the JSON egress
    /// directly, the CSV lane through `csv_container`'s decode — every entry of
    /// which carries a `key` that is a CQL blob literal (`0x` + an EVEN number of
    /// hex digits), i.e. the key's raw serialized bytes rather than a decoded
    /// container. MEASURED: `0x0000001300000007636861726c696500000004000000030000000400000008`
    /// against that same golden entry. The ENTRY COUNTS must agree too, which is
    /// cheap and removes one item from the list below.
    ///
    /// NOT COVERED: a null on either side, a golden whose keys DO parse, an egress
    /// that decoded the key (or rendered anything else — text, a number, an object)
    /// at it, a malformed `{key,value}` entry, and a differing entry COUNT. Each of
    /// those is reported as an ordinary diff naming this gap.
    ///
    /// DECLARED RESIDUAL, and it is the whole of what this gap costs: the KEY's
    /// CONTENT is not compared, because neither side's key is a value this lane can
    /// read. The entry VALUES are — `super::map::compare_map` pairs the entries
    /// POSITIONALLY (emitted order is a map's order here, and both sides preserve
    /// it), reports each unpairable key at its own node where this gap suppresses
    /// it, and compares the value beside it like any other. Suppressing those too
    /// was roborev job 28: measured, a value changed 90 -> 999 produced ZERO diffs.
    /// Nor is what the blob's bytes DECODE TO recovered — that would mean re-implementing
    /// Cassandra's tuple and UDT value serializers here, exactly as
    /// [`Divergence::NestedFrozenUdtRendersAsBlobHex`] declares one level down. The
    /// egress-side defect (a real `SELECT` returns the decoded tuple) is a
    /// read-fidelity bug in CQLite and separable from this lane.
    ///
    /// # SUPERSEDED BY #3612, AND NO COMMITTED CASE DECLARES IT ANY MORE
    ///
    /// `8c503f7cf` (#3612 / PR #3736) taught `cqlite-core` to decode a multicell composite
    /// cell-path map key STRUCTURALLY, so the egress half of this divergence — raw key bytes
    /// as a `0x` blob literal — is no longer something CQLite produces. Measured on the
    /// committed fixture after rebasing onto it: the CLI now emits
    /// `[{label:charlie,rank:3},8]` where it previously emitted
    /// `0x0000001300000007636861726c69650…`.
    ///
    /// `m_tuple_udt` has therefore moved to [`Divergence::NestedFrozenValueLeftUndecodedByGolden`],
    /// which its five sibling columns already declare and which describes what is left exactly:
    /// the GOLDEN still leaves the key as `getString`'s colon-joined text while the egress
    /// decodes it. That is the self-retirement this gap was built to undergo, and it happened
    /// for the reason predicted rather than by accident.
    ///
    /// KEPT, NOT DELETED, and the reason is scope rather than sentiment. Removing it is right
    /// and is NOT free: the guard that pins the DDL-over-parse rule
    /// (`gaps::a_frozen_column_with_an_unparseable_golden_key_is_not_this_gap`) tests that rule
    /// THROUGH this variant, so deleting it means retargeting that guard onto
    /// `container::golden_map_key_value` directly — better placed, but a change of its own with
    /// its own review and gate. The variant is INERT in the meantime: a `Divergence` suppresses
    /// nothing unless a `Skip` names it, and none does. Removal is proposed as a follow-up.
    MulticellMapKeyUndecodedByGoldenRendersAsBlobHex,
}

/// EVERYTHING A DIVERGENCE RULE MAY ASK ABOUT A POSITION, and nothing else.
///
/// One parameter rather than five, because every rule below states itself against
/// the same handful of facts and passing them singly had reached clippy's arity
/// limit — but the grouping is not merely cosmetic: it names the closed set a
/// divergence is allowed to reason from. Each field is either the committed DDL or a
/// property of the format, never a property of a VALUE, which is what keeps a
/// matcher from drifting into the shape ladder #3500 abandoned (roborev jobs
/// 302/305/306).
#[derive(Clone, Copy)]
pub struct Position<'t> {
    /// The declared CQL type at this position.
    pub ty: &'t CqlType,
    pub egress: Egress,
    /// What CSV's empty-field rule keys on.
    pub depth: Depth,
    /// How the GOLDEN spells this value's JSON kind.
    pub kinding: Kinding,
    /// WHICH CASSANDRA WRITER spelled this position's map keys, from the DDL.
    pub map_key_spelling: MapKeySpelling,
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
            Divergence::NestedFrozenValueLeftUndecodedByGolden => {
                "the golden leaves a frozen value nested in a multi-cell collection \
                 UNDECODED as a flat scalar (raw bytes as hex for a collection, \
                 colon-joined text for a tuple) while the egress decodes it into a \
                 structure"
            }
            Divergence::MulticellMapKeyUndecodedByGoldenRendersAsBlobHex => {
                "the golden leaves a MULTICELL map's container-typed key UNDECODED as \
                 getString's flat cell-path text (writeString, not toJSONString) while \
                 the egress renders the key's raw bytes as a CQL blob literal (`0x` + \
                 hex digits) — neither side decodes it, so the KEY's content cannot be \
                 compared; the entries are still paired in emitted order and their \
                 VALUES compared"
            }
        }
    }

    /// Is the pair at THIS position exactly the declared divergence?
    ///
    /// `ty`, `depth` and `kinding` are the position's own — the declared CQL type,
    /// CSV's empty-field depth and how the GOLDEN spells its JSON kind here — so
    /// every rule below is stated against the committed DDL rather than against a
    /// value's appearance.
    pub fn matched(self, golden: &Value, cli: &Value, at: Position<'_>) -> bool {
        let Position {
            ty,
            egress,
            depth,
            kinding,
            map_key_spelling,
        } = at;
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
                    canon_typed(golden, egress, ty, depth, kinding, Side::Golden),
                    canon_typed(cli, egress, ty, depth, Kinding::Stringified, Side::Cli),
                ) {
                    (Ok(g), Ok(c)) => g == c,
                    _ => false,
                }
            }
            Divergence::NestedFrozenValueLeftUndecodedByGolden => {
                // The GOLDEN side: a flat scalar STRING at a position the committed
                // DDL declares a container. Stated against the DDL, not against the
                // string's appearance, so this does not depend on recognising hex
                // versus colon-joined text — either spelling is `sstabledump`
                // declining to descend, and the DDL is what says a structure was
                // expected there.
                if !matches!(golden, Value::String(_)) {
                    return false;
                }
                if !matches!(
                    ty,
                    CqlType::List(_) | CqlType::Set(_) | CqlType::Map(..) | CqlType::Tuple(_)
                ) {
                    return false;
                }
                // The EGRESS side: a DECODED ARRAY, and nothing else.
                //
                // An earlier version also accepted `Value::Object`, on the reasoning
                // that the CLI spells a UDT as an object. That arm was unreachable by
                // design — the type guard above admits only list/set/map/tuple, never
                // `Udt` — but it was still PERMISSIVE: it would have suppressed an
                // object rendered where only an array is legal (roborev job 305,
                // Medium). An unreachable-but-permissive arm is worse than no arm,
                // because it excuses exactly the regression it can never legitimately
                // describe. The CLI renders every one of these four types as a JSON
                // array (`super::compare_map` reads a map as an array of
                // `{key,value}` objects), so an object, a scalar, a null or a number
                // here is NOT this gap and is reported as an ordinary diff.
                matches!(cli, Value::Array(_))
            }
            Divergence::MulticellMapKeyUndecodedByGoldenRendersAsBlobHex => {
                // ASKED AT THE KEY NODE, NOT AT THE COLUMN (roborev job 28).
                //
                // This used to match the whole map — golden object vs CLI `{key,value}`
                // array — and suppress it ENTIRE. That threw away the entry VALUES, which
                // ARE comparable: both sides preserve emitted order and the values are
                // ordinary cells. Measured before fixing: a value changed 90 -> 999 produced
                // ZERO diffs. `compare::map::compare_map` now reports an unpairable key at
                // its OWN position (`At::map_key`) and compares the values beside it, so
                // this matcher describes exactly the key and nothing more.
                let _ = (depth, kinding, egress);
                // DDL ONLY for the two structural facts: this position's declared type is a
                // CONTAINER, on a MULTICELL column. Neither is read from a value — see the
                // note on `MapKeySpelling` for why inferring multicellness from the key text
                // is unsound in the permissive direction.
                if !is_container_type(ty) {
                    return false;
                }
                if map_key_spelling != MapKeySpelling::GetString {
                    return false;
                }
                // GOLDEN: `getString`'s flat text — a plain string that is NOT the declared
                // container's `toJSONString` document. Asked through the one function the
                // comparison pairs with, so the gap and the pairing cannot disagree about
                // what the golden key is.
                let Value::String(golden_key) = golden else {
                    return false;
                };
                if golden_map_key_value(golden_key, ty, MapKeySpelling::ToJsonString).is_ok() {
                    return false;
                }
                // EGRESS: a CQL blob literal — the raw key bytes, undecoded. Anything else
                // here (a decoded container, a null, a number) is NOT this gap and is
                // reported as an ordinary diff, which is how the egress learning to decode
                // the cell path retires this declaration.
                matches!(cli, Value::String(text) if is_blob_hex(text))
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

// ===========================================================================
// The declared-gap BOOKKEEPING: the set, and what each was observed to do
// ===========================================================================
//
// Split from the comparator under the campsite rule (CLAUDE.md, epic #1135), which
// the comparator had outgrown. What belongs here is the whole of ONE
// responsibility: a declared gap — what it declares, what the walk observed it to
// do, and whether that makes it applied or stale. The comparator asks; it does not
// decide.

/// What a declared exclusion was OBSERVED to do, over a whole table's walk.
///
/// Ordered by strength: an exclusion that suppressed a real divergence anywhere is
/// applied, whatever happened on the other rows.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Observed {
    /// The path was reached and the comparison there COULD NOT BE DECIDED (the
    /// column was absent from the egress row, or the CSV cell was refused). "I
    /// could not tell" is not "the gap is still real", so it is reported.
    Unresolved(String),
    /// The path was reached and the two sides AGREE — the divergence the exclusion
    /// was declared for is gone, so the exclusion is stale.
    Agreed,
    /// The path was reached and the two sides DIVERGED, but NOT in the way the
    /// exclusion declares. The exclusion did not suppress that divergence — it is
    /// reported as an ordinary diff — so the exclusion suppressed nothing, and the
    /// unexplained divergence is named here too.
    ///
    /// Stronger than [`Self::Agreed`] because it is the more actionable answer: a
    /// table where one row agrees and another diverges in an undeclared way should
    /// say so rather than report the gap as merely retired.
    Undeclared(String),
    /// The path was reached and the two sides diverged EXACTLY as the exclusion
    /// declares (see [`Divergence`]): the exclusion suppressed the divergence
    /// it names, which is the only thing that makes it applied.
    Suppressed,
}

/// Value paths excluded from the comparison, with what each was observed to do.
///
/// A path is fully qualified from the row: `sf` excludes a whole column, `e.home`
/// excludes ONE field of the `frozen<employee>` in column `e` while `e.name` and
/// `e.level` keep being compared. Whole-column granularity alone was too coarse
/// and cost real coverage — skipping `e` for its one divergent inner field left
/// `udt_nested` comparing nothing but its primary key (issue #1491 review finding
/// F5).
///
/// # An exclusion excludes a VALUE, never a column's PRESENCE
///
/// Every entry names a position whose rendered VALUE the two sides disagree
/// about. None of them says the position may be absent: the comparator's contract
/// is that the egress renders every column the committed `CREATE TABLE` declares,
/// and an omitted column is a divergence of the egress SHAPE that no gap covers.
/// So a whole-column entry still leaves "the column is rendered at all" asserted,
/// and when the column IS missing the entry is `Unresolved` — there is no value at
/// that path to read an answer from (issue #1491 review finding P1). Recording it
/// as [`Observed::Suppressed`] instead is what let the declared skips hide a
/// dropped column.
///
/// # An exclusion is applied only when it SUPPRESSES ITS DECLARED divergence
///
/// Being VISITED is not enough, and treating it as enough is a guard weaker than
/// the property it guards (issue #1491 review finding L1): once CQLite renders the
/// path correctly, a visit-keyed tally still registers a hit, so the column stays
/// excluded forever and the stale-gap check reports the dead exclusion as live —
/// silently preventing the coverage from coming back. So the comparison at an
/// excluded path is still RUN; its result is recorded here and only then
/// discarded. [`Self::stale`] then fails on an exclusion that agreed, was never
/// reached, could not be evaluated, or met a divergence it does not declare — four
/// distinct causes, each named, and no two of them can be reported for the same
/// path.
pub struct SkipPaths<'a> {
    gaps: &'a [Gap<'a>],
    observed: RefCell<BTreeMap<String, Observed>>,
}

/// One declared gap as the comparator receives it: the path, and the divergence
/// the caller declares is there.
///
/// A path alone was the whole declaration until review round 17, and that is what
/// made every gap a permanent blind spot for its column: with nothing to check the
/// mismatch AGAINST, any mismatch at the path counted as the declared one. See
/// [`Divergence`].
pub type Gap<'a> = (&'a str, Divergence);

impl<'a> SkipPaths<'a> {
    pub fn new(gaps: &'a [Gap<'a>]) -> Self {
        Self {
            gaps,
            observed: RefCell::new(BTreeMap::new()),
        }
    }

    /// The divergence declared at this EXACT path, or `None`. Records NOTHING —
    /// what the exclusion did is recorded by [`Self::observe`], from the
    /// comparison's own outcome.
    pub(super) fn declared(&self, path: &str) -> Option<Divergence> {
        self.gaps
            .iter()
            .find(|(p, _)| *p == path)
            .map(|(_, divergence)| *divergence)
    }

    /// Is this exact path excluded? A thin reading of [`Self::declared`], kept
    /// because the CSV decoder is handed a path predicate (see `super::csv_decoded`).
    pub(super) fn excludes(&self, path: &str) -> bool {
        self.declared(path).is_some()
    }

    /// Record what the exclusion at `path` was observed to do. The strongest
    /// observation over the table's rows wins, so one divergent row is enough to
    /// keep the exclusion applied and no later agreeing row can retire it.
    pub(super) fn observe(&self, path: &str, what: Observed) {
        let mut observed = self.observed.borrow_mut();
        match observed.get(path) {
            Some(prev) if *prev >= what => {}
            _ => {
                observed.insert(path.to_string(), what);
            }
        }
    }

    /// Every declared exclusion that did not suppress a divergence, with the cause.
    pub(super) fn stale(&self) -> Vec<String> {
        let observed = self.observed.borrow();
        self.gaps
            .iter()
            .map(|(p, _)| p)
            .filter_map(|p| match observed.get(*p) {
                Some(Observed::Suppressed) => None,
                Some(Observed::Agreed) => Some(format!(
                    "`{p}` (the two sides AGREE at that path now, so the exclusion \
                     suppresses nothing and is holding back recovered coverage)"
                )),
                Some(Observed::Undeclared(what)) => Some(format!(
                    "`{p}` (a divergence was seen there, but NOT the one this gap declares, \
                     so the gap suppressed nothing and the divergence is reported as an \
                     ordinary diff: {what})"
                )),
                Some(Observed::Unresolved(why)) => Some(format!(
                    "`{p}` (the comparison there could not be evaluated: {why} — an \
                     exclusion whose subject cannot be measured is not a measured gap)"
                )),
                None => Some(format!("`{p}` (matched no value in the walk at all)")),
            })
            .collect()
    }
}

/// Every position where a DECLARED GAP actually suppressed its declared
/// divergence, recorded as the gap ROOT it belongs to.
///
/// Two readers, and both need the root rather than a bare count:
///
///   * a gap's root asks whether the divergence was found DEEPER in its own
///     subtree, because then the suppression is already recorded and this node has
///     nothing to add (a `set<double>`'s three non-finite members are suppressed
///     at `sf[i]`, not at `sf`);
///   * `super::count_cell` asks whether a gap fired anywhere inside THIS cell,
///     because a cell part of whose value was discarded is not compared coverage —
///     the same rule a `super::Refusals` entry already imposes.
///
/// A separate channel from the value tree for the same reason `super::Refusals` is:
/// it is CONTROL information about a position, and a sentinel inside the decoded
/// `Value` would be indistinguishable from data the egress could produce.
#[derive(Default)]
pub struct Suppressions {
    recorded: RefCell<Vec<String>>,
}

impl Suppressions {
    /// How many suppressions have been recorded so far, taken as a MARK before a
    /// subtree and compared after it (see `super::Refusals::mark`).
    pub(super) fn mark(&self) -> usize {
        self.recorded.borrow().len()
    }

    pub(super) fn record(&self, root: &str) {
        self.recorded.borrow_mut().push(root.to_string());
    }

    /// Did any gap suppress a divergence since `mark`?
    pub(super) fn any_since(&self, mark: usize) -> bool {
        self.recorded.borrow().len() > mark
    }

    /// Did the gap rooted at `root` suppress a divergence since `mark`?
    pub(super) fn root_since(&self, mark: usize, root: &str) -> bool {
        self.recorded
            .borrow()
            .iter()
            .skip(mark)
            .any(|recorded| recorded == root)
    }
}

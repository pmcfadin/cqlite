//! CONTAINERS in the canonical value model: the recursive half of
//! [`super::canon_typed`], and the ONE rule for what a golden map key DENOTES
//! (issue #3726).
//!
//! Split out of `golden_value_parity.rs` under the campsite rule (CLAUDE.md, epic
//! #1135), which that file had nearly reached. What lives here is the whole of one
//! responsibility: reducing a CONTAINER value to a [`Canon`], so that a `map` whose
//! declared KEY type is a container can be paired at all. Before this, [`Canon`] was
//! scalar-only and [`super::canon_typed`] refused a container outright, so
//! `compare::compare_map` had no rule for a container key and four columns of the
//! committed `test_nested_udt_keys.nested_udt_keys` fixture were INEXPRESSIBLE to
//! the lane rather than merely excluded (issue #3726, refs #3500).
//!
//! # THE ORACLE: what the golden's map-key TEXT is
//!
//! Every rule below about a golden map key comes from ONE piece of Cassandra
//! source, read at the pin — `cassandra-5.0.8`
//! `src/java/org/apache/cassandra/db/marshal/MapType.java`, `toJSONString`:
//!
//! ```java
//! String key = keys.toJSONString(kv, protocolVersion);
//! if (key.startsWith("\"")) sb.append(key);
//! else sb.append('"').append(JsonUtils.quoteAsJsonString(key)).append('"');
//! ```
//!
//! A container's `toJSONString` is `[…]`/`{…}` and NEVER starts with `"`, so the
//! golden's JSON object key is EXACTLY the key value's own `toJSONString` text,
//! JSON-escaped. Two consequences, and the second is easy to get wrong:
//!
//!   * the pairing rule is a PARSE, not a shape heuristic: read the golden's object
//!     key as JSON and compare it as an ordinary value of the declared key type
//!     ([`golden_map_key_value`]);
//!   * that text is `toJSONString` output, NOT `getString` output, so every scalar
//!     nested inside a container key keeps its natural JSON kind —
//!     [`Kinding::Natural`], never [`Kinding::Stringified`]. Measured on the
//!     committed golden, which carries `[{"label": "mkey-a", "rank": 21}, 1]` for a
//!     `map<frozen<tuple<frozen<key_part>, int>>, int>` key: `rank` and the tuple's
//!     second slot are JSON NUMBERS, not strings.
//!
//! # Why every MEMBER of a container is [`Kinding::Natural`]
//!
//! A container reachable here is FROZEN — CQL requires a map key, and every
//! collection nested inside another, to be frozen — so its members live inside ONE
//! value cell, which `cassandra-5.0.8 JsonTransformer.serializeCell` writes with
//! `writeRawValue(type.toJSONString(...))`. That is the same argument
//! `csv_container::member_kinding` already makes for a nested set, stated once more
//! here because this module is where the recursion happens.
//!
//! Conversely a container type arriving at [`Kinding::Stringified`] is a NAMED
//! REFUSAL and not a relaxation: `getString` of a frozen container is one flat
//! colon-joined or hex string, a different SHAPE entirely, and that case is the one
//! [`Kinding`]'s own doc comment names as NOT COVERED. A permissive arm for a path
//! believed unreachable is worse than no arm, because it excuses exactly the
//! regression it can never legitimately describe (roborev job 305's ruling on the
//! gap module).
//!
//! # EACH SIDE IS HELD TO ITS OWN MAP SPELLING
//!
//! The dump writes a map as a JSON object and the egress as an array of
//! `{key,value}` entries, so [`canon_map`] takes a [`Side`] and accepts ONLY that
//! side's spelling. It was first written SIDE-FREE — accepting either shape from
//! either side, on the reasoning that canonicalizing IS the act of mapping two
//! spellings onto one representation — and that left a false pass: a map nested
//! inside a container map KEY is walked by [`super::canon_typed`] alone, so a JSON
//! egress that emitted the GOLDEN's object spelling there canonicalized EQUAL and
//! nothing downstream caught it (roborev finding, issue #3726). At a whole map COLUMN
//! `compare::compare_value_body`'s `(Value::Object, Value::Array)` match still pins
//! each side; inside a key there is nothing else left.
//!
//! It is the same correction as [`MapKeySpelling`] one level over: a STRUCTURAL fact
//! about the caller, carried explicitly, because inferring it from the shape in front
//! of you is what lets the regression through. The mirror case was already bounded
//! this way — a UDT's `{key,value}` spelling is accepted only under [`Egress::Csv`]
//! (see [`canon_udt`]), and now only on the CLI side, which preserves issue #1491
//! review finding F3 in the JSON lane exactly.

use super::compare::pair;
use super::schema::{CqlType, UdtType};
use super::{canon_typed, scalar_spelling, strict_json, Canon, Depth, Egress, Kinding, Side};
use serde_json::Value;

/// The side's name, for a refusal message.
fn side_name(side: Side) -> &'static str {
    match side {
        Side::Golden => "golden",
        Side::Cli => "cli",
    }
}

/// Is this a type whose value is a CONTAINER — a list, set, map, tuple or UDT?
///
/// The one predicate for that question in this lane. It replaces
/// It replaces the removed `compare::is_scalar_type`, whose only two callers were
/// the precondition `compare_map` used to refuse a container key with and the gap
/// that stood in for this module; keeping both would be two notions of what a
/// container is.
pub fn is_container_type(ty: &CqlType) -> bool {
    match ty {
        CqlType::List(_)
        | CqlType::Set(_)
        | CqlType::Map(..)
        | CqlType::Tuple(_)
        | CqlType::Udt(_) => true,
        CqlType::Numeric(_)
        | CqlType::Text(_)
        | CqlType::Boolean
        | CqlType::Blob
        | CqlType::Timestamp
        | CqlType::Opaque(_) => false,
    }
}

/// WHICH CASSANDRA WRITER spelled a map's keys in the golden.
///
/// This is a STRUCTURAL fact about the COLUMN, decided by the committed DDL, and it
/// is deliberately an INPUT here rather than something inferred from the key text.
/// `sstabledump` has two writers and they meet at this one position:
///
///   * [`MapKeySpelling::ToJsonString`] — a FROZEN map is one value cell, written
///     `writeRawValue(type.toJSONString(...))`, and `cassandra-5.0.8`
///     `MapType.toJSONString` renders each key with
///     `keys.toJSONString(kv, protocolVersion)`, quoting it only when it does not
///     already start with `"`. So the golden's JSON object key is exactly the key
///     value's own toJSONString document.
///   * [`MapKeySpelling::GetString`] — a MULTICELL map is one cell PER ENTRY, whose
///     key is the cell PATH, written
///     `writeString(ct.nameComparator().getString(...))` (`JsonTransformer`). For a
///     container key that is `getString`'s flat colon-joined, escaped text —
///     `charlie\:3:8` on the committed `test_nested_udt_keys` fixture — which is not
///     a JSON document at all.
///
/// # Why this is a parameter and NOT derived from the key text
///
/// Deciding it by "does the key parse as JSON?" would decide a STRUCTURAL fact by
/// INSPECTING A VALUE, which is the move this lane forbids most explicitly — the
/// gap module states the rule in as many words ("DDL ONLY. No value is read —
/// deliberately") and gives the reason: a shape assertion there invites the next
/// depth, and the list of depths is `compare_map`'s own feature list, which does not
/// close (issue #3500, roborev jobs 302/305/306). It is also unsound in the
/// PERMISSIVE direction: "the key did not parse" is not "this column is multicell",
/// so a FROZEN map whose golden key is genuinely malformed — an ORACLE fault, which
/// [`golden_map_key_value`] must REPORT — would be silently reclassified as the
/// multicell case. And its soundness would rest on an unproven negative, that no
/// `getString` rendering of any container key type ever parses as JSON of the
/// declared kind. The DDL answers the question outright and cannot be wrong.
///
/// Source: `schema::Column::is_multicell()` (`!frozen && ty.is_complex()`) at the
/// COLUMN ROOT, and [`MapKeySpelling::ToJsonString`] for any map reached by
/// RECURSION — CQL requires a map key, and every collection nested inside another,
/// to be frozen, so a nested map always lives in one value cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapKeySpelling {
    /// A frozen map: the key is `keys.toJSONString(...)` text.
    ToJsonString,
    /// A multicell map: the key is the cell path, `nameComparator().getString(...)`.
    GetString,
}

/// The [`Kinding`] the GOLDEN's map key is read under.
///
/// The single statement of that rule, called by both [`canon_map`] and
/// `compare::compare_map` — two spellings of it would be two notions of what the
/// golden's key text is, which is the drift this lane's review history is made of.
///
///   * a CONTAINER key under [`MapKeySpelling::ToJsonString`] is
///     `keys.toJSONString(...)` text (the module doc's oracle), whose scalars keep
///     their natural JSON kind — [`Kinding::Natural`];
///   * a SCALAR key is stringified BY THE FORMAT (a JSON object key can only be a
///     string), which is exactly the rule `compare_map` has always applied. Note
///     `MapType.toJSONString` spells a scalar key through `keys.toJSONString` too,
///     not `getString`; [`Kinding::Stringified`] covers BOTH spellings for every
///     type that can occupy this position — identical text for the integer family
///     and `boolean`, and guarded relaxations for `blob` (`0x`-prefixed text is not
///     a spelling `BytesSerializer.toString` can produce, so it is left exact) and
///     `timestamp` (both spellings are accepted) — so one rule is kept rather than
///     two. That is why the scalar answer does not depend on the spelling.
///
/// A CONTAINER key under [`MapKeySpelling::GetString`] never reaches a kinding:
/// [`golden_map_key_value`] refuses it first, so this answers [`Kinding::Natural`]
/// for the shape of the rule rather than as a licence.
pub fn golden_map_key_kinding(key_ty: &CqlType, spelling: MapKeySpelling) -> Kinding {
    let _ = spelling;
    if is_container_type(key_ty) {
        Kinding::Natural
    } else {
        Kinding::Stringified
    }
}

/// The VALUE a golden map key denotes: the parsed `toJSONString` document for a
/// CONTAINER key of a FROZEN map, the key text itself for a scalar one.
///
/// THE one function that answers that question — `compare::compare_map`,
/// [`canon_map`] and `csv_container` all call it, so the comparison, the
/// canonicalization and the CSV rendering cannot drift apart on what the golden's
/// key is.
///
/// # The two refusals, and what each one claims
///
/// A container key under [`MapKeySpelling::GetString`] is refused IMMEDIATELY, with
/// no parse attempted: the dump wrote that position with
/// `nameComparator().getString(...)`, so there is no toJSONString document there to
/// read, and the refusal says so. That sentence is the one
/// `compare::gap::Divergence::MulticellMapKeyUndecodedByGoldenRendersAsBlobHex`
/// mirrors — the CAUSE, not the symptom of a failed parse.
///
/// The PARSE failure that remains is therefore scoped to what it can honestly
/// claim: a FROZEN map whose key text is not the spelling the oracle says it is.
/// That is a fact about the ORACLE, not about the egress, so it is reported and
/// never guessed at. Neither refusal is a shape ladder: the parse must succeed, and
/// the parsed document must be the ONE JSON shape the declared kind has
/// (`toJSONString` spells a list/set/tuple as an array and a map/UDT as an object).
///
/// Parsed through [`strict_json`] — the same strict parse the golden LINE and the
/// CLI's own egress get (issue #1491 review finding K2): a duplicate object key
/// inside a key document would silently discard part of the oracle.
pub fn golden_map_key_value(
    key: &str,
    key_ty: &CqlType,
    spelling: MapKeySpelling,
) -> Result<Value, String> {
    if !is_container_type(key_ty) {
        return Ok(Value::String(key.to_string()));
    }
    if spelling == MapKeySpelling::GetString {
        return Err(format!(
            "the schema declares the map key type `{}`, a container, on a MULTICELL map \
             — so sstabledump wrote this key as the cell PATH, \
             `writeString(ct.nameComparator().getString(...))`, which is getString's \
             flat colon-joined text and not a toJSONString document; there is no \
             container here to pair (cassandra-5.0.8)",
            key_ty.describe()
        ));
    }
    let parsed = strict_json::parse(key, "golden map key").map_err(|why| {
        format!(
            "the schema declares the map key type `{}`, a container, on a FROZEN map \
             — so cassandra-5.0.8 MapType.toJSONString spells this golden object key \
             as the key value's own toJSONString text — and {} does not parse as \
             JSON: {why}",
            key_ty.describe(),
            brief(key)
        )
    })?;
    // The ONE JSON shape the declared kind's `toJSONString` has: `[…]` for a
    // list/set/tuple, `{…}` for a map/UDT (cassandra-5.0.8).
    let shaped = matches!(
        (&parsed, key_ty),
        (
            Value::Array(_),
            CqlType::List(_) | CqlType::Set(_) | CqlType::Tuple(_)
        ) | (Value::Object(_), CqlType::Map(..) | CqlType::Udt(_))
    );
    if !shaped {
        return Err(format!(
            "the golden's map key {} parses as {}, but the schema declares the key \
             type `{}`, whose toJSONString spelling is {} (cassandra-5.0.8)",
            brief(key),
            shape_of(&parsed),
            key_ty.describe(),
            expected_shape(key_ty)
        ));
    }
    Ok(parsed)
}

/// Does this canonical value's SHAPE AND SCALAR KINDS match what the DDL declares?
///
/// Exists because `canon_typed(...).is_ok()` is not a validity test (roborev job 38). The
/// canonicalizer accepts a string where an `int` is declared ON PURPOSE — it canonicalizes
/// both sides and lets the COMPARISON report the inequality — so a gap that reads its `Ok`
/// as "the egress produced a well-formed value of this type" suppresses `"not-an-int"`
/// inside a declared `set<int>`.
///
/// NOT a second opinion about the type system, which is the shape #3500 abandoned — and NOT
/// the general strict validator #3846 rules out either. It reads `canon_typed`'s OWN OUTPUT
/// back against the declared type: the canonicalizer already decided what each leaf became,
/// and this asks whether that variant is the one the DDL implies. No arity logic and no field
/// sets — `canon_container` already enforces both — so this adds exactly the leaf question
/// and nothing else.
///
/// For four declared types the variant alone is not the whole question, because they share
/// `Canon::Text`: `blob`, `timestamp` and the six opaque scalars each get ONE BOUNDED
/// SPELLING CHECK, quoting the `cassandra-5.0.8` `toJSONString` it is read from, in
/// `scalar_spelling`. `text`/`varchar`/`ascii` get none, because every string is a
/// well-formed value of those. That is the per-type read-back the CSV `Boolean` arm already
/// does, not a spelling GRAMMAR for the type system at large: an opaque type this module has
/// read no authority for is REFUSED rather than given a guessed rule.
///
/// `Canon::Null` is accepted at EVERY position: a null member is legal in every container
/// this lane compares, and `cassandra-5.0.8 UserType.toJSONString` emits `null` for a field
/// whose buffer is absent.
///
/// Egress-aware, because the projection differs: `Canon::for_csv` renders a boolean as its
/// TEXT spelling, so `Boolean` legitimately canonicalizes to [`Canon::Text`] under
/// [`Egress::Csv`] and to [`Canon::Bool`] under [`Egress::Json`].
pub fn canon_matches_declared_kinds(canon: &Canon, ty: &CqlType, egress: Egress) -> bool {
    // A whole CELL may legitimately be null, so the entry point allows it; the recursion
    // then decides per POSITION.
    kinds_match(canon, ty, egress, NullHere::Allowed)
}

/// May a NULL stand at this position? A positional fact, from CQL rather than from taste.
///
/// CQL does not permit a null inside a COLLECTION: a `set<int>` cannot hold one, and writing a
/// null map value DELETES the entry rather than storing one. It does permit a null TUPLE SLOT
/// and a null UDT FIELD — `cassandra-5.0.8 UserType.toJSONString` emits `null` for a field
/// whose buffer is absent, which is why a dump of a frozen UDT always carries every declared
/// field.
///
/// Corpus-consistent, measured on the committed golden rather than assumed: **zero** null
/// members appear directly in a collection array, while `nested-udt-keys.cql` deliberately
/// carries null UDT FIELDS (`rank:null`, `label:null`).
///
/// It matters because accepting null everywhere made the undecoded-golden gap SUPPRESS
/// malformed collection output — a `set<int>` containing `null` is not a decode of that type,
/// and was being excused as one (roborev job 60).
#[derive(Clone, Copy, PartialEq, Eq)]
enum NullHere {
    /// A cell, a tuple slot, or a UDT field.
    Allowed,
    /// A list/set ELEMENT, or a map KEY or VALUE.
    Forbidden,
}

fn kinds_match(canon: &Canon, ty: &CqlType, egress: Egress, null_here: NullHere) -> bool {
    if matches!(canon, Canon::Null) {
        return null_here == NullHere::Allowed;
    }
    match ty {
        CqlType::Numeric(_) => matches!(canon, Canon::Num(_)),
        CqlType::Boolean => match egress {
            Egress::Json => matches!(canon, Canon::Bool(_)),
            // CSV: `Canon::for_csv` renders a boolean as `Canon::Text(b.to_string())`, so the
            // text form is legal here — but ONLY the two spellings it can produce. Accepting
            // every `Canon::Text` let `"not-a-bool"` qualify as a decoded boolean and be
            // suppressed (roborev job 72). Authority for the pair: `cassandra-5.0.8`
            // `BooleanSerializer.toString` returns `value.toString()`, i.e. `true`/`false`,
            // and `for_csv` uses Rust's `bool::to_string`, which is the same two words.
            Egress::Csv => match canon {
                Canon::Bool(_) => true,
                Canon::Text(text) => text == "true" || text == "false",
                _ => false,
            },
        },
        // `text`/`varchar`/`ascii`: EVERY string is a well-formed value, so
        // `Canon::Text` is the whole question here and there is nothing to narrow.
        CqlType::Text(_) => matches!(canon, Canon::Text(_)),
        // THE OTHER THREE `Canon::Text` FAMILIES ARE NOT (roborev job 105, Medium).
        // These four types used to share one arm accepting every `Canon::Text`, so a
        // non-hex blob, a malformed uuid and arbitrary text at an `inet` position all
        // qualified as decodes of those types — and the undecoded-golden gap then
        // SUPPRESSED them, at a position where the golden is undecoded and there is no
        // other side left to catch it. Same shape as the CSV `Boolean` arm above (job
        // 72) and `NullHere` (job 60), and answered the same way: read `canon_typed`'s
        // OWN OUTPUT back with a BOUNDED, authority-quoting spelling check, per type.
        //
        // NOT the strict type validator #3846 rules out: each predicate is one
        // Cassandra `toJSONString` spelling, quoted at its definition in
        // `scalar_spelling`, with no calendar, range or arity logic anywhere.
        CqlType::Blob => matches!(canon, Canon::Text(text) if scalar_spelling::is_blob_hex(text)),
        CqlType::Timestamp => {
            matches!(canon, Canon::Text(text) if scalar_spelling::is_canonical_timestamp(text))
        }
        // `Opaque` covers SIX types with distinct syntaxes (`schema::NATIVE_OPAQUE`),
        // so the rule is per NAME — and a name no authority has been read for answers
        // `None`, which REFUSES here rather than accepting any text. That is the
        // fail-closed direction: the gap stops suppressing at that position and reports
        // an ordinary diff. `== Some(true)` rather than `!= Some(false)` for the
        // standing reason a permissive branch is keyed on the affirmative value.
        CqlType::Opaque(name) => match canon {
            Canon::Text(text) => scalar_spelling::opaque_spelling_matches(name, text) == Some(true),
            _ => false,
        },
        CqlType::List(element) | CqlType::Set(element) => match canon {
            Canon::Seq(items) => items
                .iter()
                .all(|item| kinds_match(item, element, egress, NullHere::Forbidden)),
            _ => false,
        },
        CqlType::Tuple(items) => match canon {
            // Arity is already enforced by `canon_container`; this asks only about kinds.
            Canon::Seq(slots) => slots
                .iter()
                .zip(items.iter())
                .all(|(slot, slot_ty)| kinds_match(slot, slot_ty, egress, NullHere::Allowed)),
            _ => false,
        },
        CqlType::Map(key_ty, value_ty) => match canon {
            Canon::Entries(entries) => entries.iter().all(|(k, v)| {
                kinds_match(k, key_ty, egress, NullHere::Forbidden)
                    && kinds_match(v, value_ty, egress, NullHere::Forbidden)
            }),
            _ => false,
        },
        CqlType::Udt(udt) => match canon {
            // The field SET and ORDER are already enforced by `canon_udt`, so an undeclared
            // name cannot reach here; a missing declared type is a fault, not a tolerated
            // shape, and answers false.
            Canon::Fields(fields) => fields.iter().all(|(name, value)| {
                match udt.fields.iter().find(|(n, _)| n == name) {
                    Some((_, field_ty)) => kinds_match(value, field_ty, egress, NullHere::Allowed),
                    None => false,
                }
            }),
            _ => false,
        },
    }
}

/// Canonicalize a CONTAINER value under its declared type — the recursive arms of
/// [`super::canon_typed`], which dispatches here for every container type and keeps
/// the scalar arms itself.
///
/// `kinding` is the position's own, as [`super::canon_typed`] received it; see the
/// module doc for why anything but [`Kinding::Natural`] is a refusal here.
pub fn canon_container(
    v: &Value,
    egress: Egress,
    ty: &CqlType,
    kinding: Kinding,
    side: Side,
) -> Result<Canon, String> {
    // A NULL has no container spelling for the two sides to disagree about, so it
    // canonicalizes exactly as a scalar null does — BEFORE the kinding rule below,
    // which is a statement about how a PRESENT value is spelled. Reachable at every
    // position the DDL declares a container: a whole column the row never wrote, a
    // map value, a tuple slot, and a UDT field (`cassandra-5.0.8
    // UserType.toJSONString` writes `null` for a field whose buffer is absent).
    if matches!(v, Value::Null) {
        return Ok(Canon::Null);
    }
    if kinding == Kinding::Stringified {
        return Err(format!(
            "the schema declares the container type `{}` at a STRINGIFIED position, \
             where cassandra-5.0.8 writes `writeString(type.getString(v))` and getString \
             spells a whole frozen container as ONE flat string — a different shape, not a \
             kind relaxation (see the Kinding doc comment, which names this case as not \
             covered)",
            ty.describe()
        ));
    }
    match ty {
        // A list, a set and a tuple are all JSON ARRAYS on both sides:
        // `ListType`/`SetType`/`TupleType.toJSONString` each emit `[…]`
        // (cassandra-5.0.8), and the CLI spells all three as arrays too.
        CqlType::List(element) | CqlType::Set(element) => {
            let items = array(v, ty)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(canon_member(item, egress, element, side)?);
            }
            Ok(Canon::Seq(out))
        }
        // A TUPLE's arity is the DDL's, and an arity that is not the declared one is
        // a REFUSAL rather than an inequality — the same rule
        // `compare::compare_value_body`'s tuple arm states, so the two
        // implementations cannot mean different things by "a tuple of this type".
        CqlType::Tuple(items) => {
            let slots = array(v, ty)?;
            if slots.len() != items.len() {
                return Err(format!(
                    "tuple arity {} but the schema declares {} field(s) (`{}`)",
                    slots.len(),
                    items.len(),
                    ty.describe()
                ));
            }
            let mut out = Vec::with_capacity(slots.len());
            for (slot, slot_ty) in slots.iter().zip(items.iter()) {
                out.push(canon_member(slot, egress, slot_ty, side)?);
            }
            Ok(Canon::Seq(out))
        }
        CqlType::Map(key_ty, value_ty) => canon_map(v, egress, key_ty, value_ty, ty, side),
        CqlType::Udt(udt) => canon_udt(v, egress, udt, side),
        // Unreachable: [`super::canon_typed`] dispatches here only for a container
        // type. Stated as a REFUSAL rather than a permissive fall-through, so a
        // future caller that got the dispatch wrong fails loudly instead of having
        // its scalar canonicalized by whichever arm a wildcard sat on.
        CqlType::Numeric(_)
        | CqlType::Text(_)
        | CqlType::Boolean
        | CqlType::Blob
        | CqlType::Timestamp
        | CqlType::Opaque(_) => Err(format!(
            "canon_container was asked for the SCALAR type `{}`; canon_typed owns the \
             scalar arms",
            ty.describe()
        )),
    }
}

/// A MAP, with EACH SIDE HELD TO ITS OWN SPELLING — the shape is required to match
/// the [`Side`] the caller states, never used to infer it:
///
///   * [`Side::Golden`] requires `Value::Object`. A JSON object key can only be a
///     string, so each key is read through [`golden_map_key_value`] at
///     [`golden_map_key_kinding`]'s kinding (the module doc's oracle);
///   * [`Side::Cli`] requires `Value::Array`, the `{"key":…,"value":…}` entries (and
///     what `csv_container` decodes a CSV map into). BOTH halves keep their natural JSON
///     kind: the egress is under no stringification constraint, which is issue #1491
///     review findings M1/N1 and is why the golden-side relaxation above can never
///     license a CLI spelling.
///
/// Entries stay in EMITTED ORDER — the order Cassandra stores a map's entries in
/// (key-comparator order) and the order `sstabledump` and a reader of the same
/// SSTable both see, which `serde_json`'s workspace-wide `preserve_order` keeps. A
/// canonical form that sorted them would make a reordering compare equal, the
/// defect issue #1491 finding N2 records.
///
/// A SIZE difference is simply an unequal [`Canon`], never a refusal: the two
/// `Canon::Entries` differ in length and the comparison names it.
fn canon_map(
    v: &Value,
    egress: Egress,
    key_ty: &CqlType,
    value_ty: &CqlType,
    ty: &CqlType,
    side: Side,
) -> Result<Canon, String> {
    match (side, v) {
        (Side::Golden, Value::Object(entries)) => {
            let mut out = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                // [`MapKeySpelling::ToJsonString`] UNCONDITIONALLY. This is an
                // INVARIANT, not a default, and it is the ONE place this module's rules
                // could disagree with `compare::compare_map`'s — that function takes the
                // spelling from the DDL and can be `GetString`, so the two would differ
                // on a TOP-LEVEL MULTICELL container-keyed map.
                //
                // EVERY PATH INTO `canon_typed` IS ENUMERATED, because an invariant
                // backed by one of its arguments is an invariant nobody can re-check:
                //
                //   1. `compare::compare_value_body` routes a top-level map's
                //      `(Object, Array)` pair to `compare::compare_map`, never here;
                //   2. the row-order key path (`compare::row_order_divergence`)
                //      canonicalizes PK and CK columns only, and CQL forbids a
                //      NON-FROZEN collection as a key column;
                //   3. recursion from [`canon_container`] is already inside a container,
                //      every level of which CQL requires to be frozen;
                //   4. `compare::compare_map` canonicalizes a KEY, and a map key is
                //      frozen by CQL.
                //
                // AND IT FAILS CLOSED IF IT IS EVER WRONG, which is why this is an
                // assertion and not a threaded parameter: a `getString` cell path does
                // not parse as the declared container's `toJSONString` document, so
                // [`golden_map_key_value`] REFUSES rather than mis-pairing. The cost of
                // a broken invariant here is a loud refusal, not a false pass.
                let key_value = golden_map_key_value(key, key_ty, MapKeySpelling::ToJsonString)?;
                let canon_key = canon_typed(
                    &key_value,
                    egress,
                    key_ty,
                    Depth::Inside,
                    golden_map_key_kinding(key_ty, MapKeySpelling::ToJsonString),
                    Side::Golden,
                )?;
                out.push((
                    canon_key,
                    canon_member(value, egress, value_ty, Side::Golden)?,
                ));
            }
            Ok(Canon::Entries(out))
        }
        (Side::Cli, Value::Array(entries)) => {
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                let (key, value) = pair(entry, egress)?;
                out.push((
                    canon_member(key, egress, key_ty, Side::Cli)?,
                    canon_member(value, egress, value_ty, Side::Cli)?,
                ));
            }
            Ok(Canon::Entries(out))
        }
        (side, other) => Err(format!(
            "the schema declares `{}`, but the {} side spelled it {} — the dump writes a \
             map as a JSON object and the egress as an array of {{key,value}} entries, and \
             each side is held to ITS OWN spelling so a regression that emitted the other \
             one cannot canonicalize equal",
            ty.describe(),
            side_name(side),
            shape_of(other)
        )),
    }
}

/// A UDT, with the SAME three DDL rules `compare::udt::compare_udt` enforces,
/// stated here as REFUSALS so the two implementations cannot mean different things
/// by "a value of this UDT". Authority for all three: `cassandra-5.0.8`
/// `UserType.toJSONString`, which writes `for (int i = 0; i < types.size(); i++)`
/// over the DECLARED field list and emits every declared field (`null` when its
/// buffer is absent), so a rendering of a frozen UDT carries exactly the declared
/// fields, in declaration order:
///
///   1. every declared field is present;
///   2. no field name the `CREATE TYPE` does not declare (an undeclared name has no
///      declared type, and a value with no declared type is never compared
///      permissively);
///   3. the emitted order IS the declared order.
///
/// The members are then canonicalized in DECLARED order — the same sequence rule 3
/// requires of the emitted one, walked from the DDL because the DDL is the
/// authority.
///
/// # Accepted spellings, and why the `{key,value}` one is CSV-ONLY
///
/// A field→value OBJECT is the dump's spelling and the JSON egress's. CSV carries
/// the whole cell as one flat `{k: v, …}` text with nothing that could distinguish
/// a map from a UDT, so `csv_container` decodes EVERY brace-delimited body into the
/// `{key,value}` pair spelling — which is therefore accepted here under
/// [`Egress::Csv`] and only there. Accepting it in the JSON lane would let a UDT
/// that regressed to the map representation canonicalize equal, which is issue
/// #1491 review finding F3.
fn canon_udt(v: &Value, egress: Egress, udt: &UdtType, side: Side) -> Result<Canon, String> {
    let emitted: Vec<(&str, &Value)> = match (v, egress, side) {
        (Value::Object(fields), _, _) => fields.iter().map(|(k, v)| (k.as_str(), v)).collect(),
        // CSV's pair spelling is the DECODED EGRESS's, never the golden's: the golden
        // is the JSONL document in every format, so accepting it on that side would
        // let a malformed golden pose as a decoded CLI cell.
        (Value::Array(entries), Egress::Csv, Side::Cli) => {
            let mut out: Vec<(&str, &Value)> = Vec::with_capacity(entries.len());
            for entry in entries {
                let (key, value) = pair(entry, egress)?;
                let Value::String(name) = key else {
                    return Err(format!(
                        "udt `{}`: decoded field name {} is not a string",
                        udt.name,
                        shape_of(key)
                    ));
                };
                // A repeated field name is malformed output, not something to
                // reconcile: keeping the last occurrence would hide the earlier one
                // (issue #1491 review finding J2, the same refusal `compare_udt`
                // makes).
                if out.iter().any(|(seen, _)| *seen == name.as_str()) {
                    return Err(format!(
                        "udt `{}`: the {egress:?} spelling repeats the field `{name}`",
                        udt.name
                    ));
                }
                out.push((name.as_str(), value));
            }
            out
        }
        (other, _, _) => {
            return Err(format!(
                "the schema declares the UDT `{}`, but {} is not a field→value object \
                 (and the `{{key,value}}` spelling is legal only in the CSV lane, where \
                 the decoder produces it)",
                udt.name,
                shape_of(other)
            ))
        }
    };
    let declared: Vec<&str> = udt.fields.iter().map(|(name, _)| name.as_str()).collect();
    let undeclared: Vec<&str> = emitted
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !declared.contains(name))
        .collect();
    if !undeclared.is_empty() {
        return Err(format!(
            "udt `{}`: the field(s) {undeclared:?} are not declared by the committed \
             CREATE TYPE, which declares {declared:?}",
            udt.name
        ));
    }
    let absent: Vec<&str> = declared
        .iter()
        .copied()
        .filter(|name| !emitted.iter().any(|(emitted, _)| emitted == name))
        .collect();
    if !absent.is_empty() {
        return Err(format!(
            "udt `{}`: the declared field(s) {absent:?} are not emitted — cassandra-5.0.8 \
             UserType.toJSONString emits every declared field (`null` when its value is \
             absent), so an absent field is a missing field and not an agreement",
            udt.name
        ));
    }
    let order: Vec<&str> = emitted.iter().map(|(name, _)| *name).collect();
    if order != declared {
        return Err(format!(
            "udt `{}`: the field order is {order:?}, but the committed CREATE TYPE \
             declares {declared:?} — cassandra-5.0.8 UserType.toJSONString emits a UDT's \
             fields in declaration order",
            udt.name
        ));
    }
    let mut out = Vec::with_capacity(declared.len());
    for (name, field_ty) in &udt.fields {
        // The three rules above leave the emitted set EQUAL to the declared one, so
        // this cannot be absent. Stated as an error rather than defaulted to `Null`:
        // a default would silently canonicalize an absent field as a null one if
        // that agreement ever moved.
        let value = emitted
            .iter()
            .find(|(emitted, _)| emitted == name)
            .map(|(_, value)| *value)
            .ok_or_else(|| {
                format!(
                    "udt `{}`: field `{name}` vanished between the field-set check and the \
                     canonicalization",
                    udt.name
                )
            })?;
        out.push((name.clone(), canon_member(value, egress, field_ty, side)?));
    }
    Ok(Canon::Fields(out))
}

/// One MEMBER of a container: at [`Depth::Inside`] and [`Kinding::Natural`], for
/// the reasons the module doc states (a frozen container's members are one value
/// cell, written `writeRawValue`; and a member has a distinct `null` spelling, so
/// CSV's empty-field collapse must not apply to it — issue #1491 review finding F1).
fn canon_member(v: &Value, egress: Egress, ty: &CqlType, side: Side) -> Result<Canon, String> {
    canon_typed(v, egress, ty, Depth::Inside, Kinding::Natural, side)
}

/// This value as a JSON array, or an error naming the declared type.
fn array<'v>(v: &'v Value, ty: &CqlType) -> Result<&'v Vec<Value>, String> {
    v.as_array().ok_or_else(|| {
        format!(
            "the schema declares `{}`, whose toJSONString spelling and egress spelling are \
             both a JSON array, but {} is not one",
            ty.describe(),
            shape_of(v)
        )
    })
}

// ===========================================================================
// `Canon::describe` for the container variants
// ===========================================================================

/// The rendering of a [`Canon::Seq`]. Recursive, and INJECTIVE — see [`escape`].
pub fn describe_seq(items: &[Canon]) -> String {
    format!("seq[{}]", join(items.iter().map(escape)))
}

/// The rendering of a [`Canon::Entries`], in emitted order.
pub fn describe_entries(entries: &[(Canon, Canon)]) -> String {
    format!(
        "map[{}]",
        join(
            entries
                .iter()
                .map(|(key, value)| format!("{} => {}", escape(key), escape(value)))
        )
    )
}

/// The rendering of a [`Canon::Fields`], in declared order.
pub fn describe_fields(fields: &[(String, Canon)]) -> String {
    format!(
        "udt[{}]",
        join(
            fields
                .iter()
                .map(|(name, value)| format!("{}={}", escape_text(name), escape(value)))
        )
    )
}

fn join(parts: impl Iterator<Item = String>) -> String {
    parts.collect::<Vec<_>>().join(", ")
}

/// One child's rendering, with this level's delimiters escaped so that DISTINCT
/// values can never describe alike.
///
/// This is issue #1491 finding DD1 one level down, and it is load-bearing rather
/// than cosmetic: `compare::compare_map` builds a map entry's PATH from
/// `Canon::describe()` (`at.index(&gk.describe(), …)`) and a declared gap is matched
/// against that path by EXACT STRING, so two distinct keys that described alike
/// would share one path — and one gap would silently cover both.
///
/// Unescaped, the collision is trivial and needs no exotic value: `Seq[Text("a"),
/// Text("b")]` renders `seq[text:a, text:b]`, and so would the ONE-member
/// `Seq[Text("a, text:b")]`. Escaping `,` and `=` (and `\` itself, so the escape is
/// reversible) makes every unescaped delimiter at this level ours, so the children
/// split uniquely and the mapping is injective by induction.
fn escape(canon: &Canon) -> String {
    escape_text(&canon.describe())
}

fn escape_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            ',' => out.push_str("\\,"),
            '=' => out.push_str("\\="),
            other => out.push(other),
        }
    }
    out
}

/// The JSON shape of a value, for a diagnostic that must name what arrived without
/// printing a 4 KiB blob.
fn shape_of(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "a JSON boolean".to_string(),
        Value::Number(_) => "a JSON number".to_string(),
        Value::String(s) => format!("the string {}", brief(s)),
        Value::Array(items) => format!("a JSON array of {} element(s)", items.len()),
        Value::Object(fields) => format!("a JSON object of {} key(s)", fields.len()),
    }
}

/// The ONE JSON shape a container type's `toJSONString` has (cassandra-5.0.8).
fn expected_shape(ty: &CqlType) -> &'static str {
    match ty {
        CqlType::List(_) | CqlType::Set(_) | CqlType::Tuple(_) => "a JSON array",
        CqlType::Map(..) | CqlType::Udt(_) => "a JSON object",
        _ => "a JSON scalar",
    }
}

/// Truncate a rendering for a diagnostic (the corpus carries 4 KiB blobs).
fn brief(s: &str) -> String {
    const LIMIT: usize = 80;
    if s.chars().count() <= LIMIT {
        return format!("`{s}`");
    }
    let head: String = s.chars().take(LIMIT).collect();
    format!("`{head}…`({} chars)", s.chars().count())
}

#[cfg(test)]
#[path = "golden_value_canon_container_tests.rs"]
mod tests;

// The LEAF question `canon_matches_declared_kinds` answers, in a suite of its own:
// it had no direct coverage at all and is now a SECOND caller's rule (issue #3846).
#[cfg(test)]
#[path = "golden_value_canon_kinds_tests.rs"]
mod kinds_tests;

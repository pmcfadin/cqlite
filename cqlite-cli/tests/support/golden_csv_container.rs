//! Decoding a CSV container cell back into a comparable value (issue #1491).
//!
//! # Why a decoder and not a skip
//!
//! Acceptance criterion 2 of #1491 is "parse the CSV output back into rows and
//! compare cells to the golden", and it carves out no container exception. CSV
//! carries no types at all, so decoding ANY cell needs an external statement of
//! its shape — that is a property of the format, not a weakness of this lane.
//! Here the shape comes from TWO committed authorities: the GOLDEN
//! (`sstabledump` renders a list/set/frozen collection as a JSON array and a
//! map/UDT as a JSON object), and the committed `CREATE TABLE`/`CREATE TYPE` DDL,
//! which is what distinguishes `list<>` from `set<>` from `tuple<>` — a
//! distinction the golden's JSON array cannot express. Nothing here is derived
//! from CQLite's own output.
//!
//! # Why this is not redundant with the JSON lane — MEASURED, not argued
//!
//! The standing objection to this module is that the JSON lane already compares
//! container VALUES at full strictness, so all CSV adds is a rendering no oracle
//! governs (see the grammar section below) — on which reading the whole decoder
//! could become a declared gap and this file could be deleted. The objection is
//! sound in outline and FALSE in fact, so the measurement is recorded here rather
//! than left to be retaken:
//!
//! `test_signed_coll.signed_special_collections` declares `sd SET<DECIMAL>` and
//! `sf SET<DOUBLE>`, and BOTH are format-scoped declared gaps of the JSON lane —
//! `sf` because JSON has no literal for `Infinity`/`NaN`, `sd` because the egress
//! renders a `decimal` as a JSON string where `cassandra-5.0.8`
//! `DecimalType.toJSONString` emits an unquoted number. The table has no other
//! non-key column, so in JSON that case compares its `id` and nothing else. The
//! CSV lane is therefore the ONLY place any oracle checks how CQLite renders a
//! `set<double>` carrying `-Infinity`/`NaN` or a `set<decimal>` carrying exact
//! 30-digit unscaled text — against the `sstabledump` golden, member by member.
//!
//! Measured across the whole corpus when this was written: 46 container cells
//! value-compared under CSV against 45 under JSON, and the two lanes' sets are
//! NOT nested either way — CSV compares those 2 cells JSON declares away, JSON
//! compares 1 (`nb_empty_collections`'s `fs`) that CSV refuses. So neither lane
//! subsumes the other, and deleting this one would drop two container columns to
//! zero coverage in any lane.
//!
//! The counts will drift as the corpus grows; the STRUCTURAL fact is the durable
//! one — a format-scoped gap on a container column moves that column's only
//! coverage into the other format's lane.
//!
//! # The grammar, and what pinning it is (and is not) worth
//!
//! `cqlite_core::util::value_fmt::ValueFormatter` renders a container as
//! `[a, b]` (list), `{a, b}` (set), `{k: v}` (map and UDT) and `(a, b)` (tuple):
//! `, ` and `: ` separators, and NO quoting or escaping of members.
//!
//! That syntax is CQLite's own product decision — Cassandra has no CSV egress
//! counterpart for it — so it is deliberately NOT treated as authority for
//! anything, and this module asserts no claim that it is correct. What IS
//! asserted against an external oracle is every VALUE the syntax carries:
//! member count, member order, nesting depth, map keys, and each scalar's
//! rendering (blob hex, exact decimal digits, timestamp spelling), all compared
//! against the `sstabledump` golden by the same rules the JSON lane uses.
//!
//! The decoder is STRICT precisely so that pinning the grammar is worth
//! something. A tolerant decoder would absorb a writer regression symmetrically,
//! which is the round-trip blind spot CLAUDE.md names ("a CQLite-written +
//! CQLite-read round-trip is invariant to a uniform framing error"). So the
//! separators must be exactly `, ` and `: `, the brackets must balance, and
//! nothing is trimmed: a separator change, a bracket change, a dropped member or
//! a re-ordered one all surface as a failure rather than being normalized away.
//!
//! # The bracket is required, per DECLARED type
//!
//! The expected bracket pair comes from the column's declared CQL type, so
//! `list<>` requires `[…]`, `set<>` requires `{…}`, `tuple<>` requires `(…)` and
//! `map<>`/UDT require `{…}` (the grammar above). A set or tuple rendered with
//! list brackets is therefore a FAILURE, not an accepted spelling.
//!
//! This replaces an earlier concession that accepted `[`, `{` and `(`
//! interchangeably for any golden ARRAY, on the grounds that `sstabledump`
//! renders a list and a set alike so the golden cannot say which is which (issue
//! #1491 review finding R2). The premise was true of the golden and false of the
//! lane: the DDL is committed beside the fixtures and says exactly which kind the
//! column is.
//!
//! It leaves the JSON lane strictly less discriminating here, which is a property
//! of that format rather than a gap in this one: CQLite's JSON egress renders a
//! set as an array (measured on `test_da.collection_table`: `tags SET<TEXT>` →
//! `["alpha","beta"]`) exactly as the golden does, so in JSON there is no bracket
//! to check.
//!
//! # Refusals, declared rather than papered over — each attributed to the
//! NARROWEST node it destroys
//!
//! Some golden content cannot be recovered from the flat rendering at all. Such a
//! position is REFUSED: never guessed, always counted and named in the run census.
//!
//! What matters as much as the refusal is its BLAST RADIUS, because the
//! comparison walk is per MEMBER, per DEPTH. A refusal decided at any coarser
//! granularity suppresses positions that are perfectly decidable — the same
//! defect three times over in this lane's review history: first per LANE (CSV
//! skipped every container), then per CELL (an ambiguous golden refused the whole
//! cell, so `null` or unrelated text passed for it), then per OUTER CONTAINER (an
//! ambiguous NESTED member suppressed its unambiguous siblings and the outer
//! structure, so a golden `[[]]` of `list<frozen<list<text>>>` accepted a CLI `[]`
//! — the unambiguous outer member silently dropped, review finding P2).
//!
//! So each cause is attributed to the narrowest node whose decode it destroys,
//! and the refusal RIDES THE SAME RECURSION as the comparison: [`node_refusal`] is
//! asked at every node, by the decoder ([`decode`]) and by the comparator alike,
//! on the same golden value and the same declared type — so what one leaves the
//! other expects, and the two cannot drift. Everything above and beside a refused
//! node keeps being compared: every enclosing bracket frame, every enclosing
//! member count, and every unambiguous sibling.
//!
//! The BRACKET DEPTH is what makes that attribution sound rather than convenient:
//! [`scan`] splits a body only at DEPTH ZERO, so a `, ` (or a `: `) inside a
//! member can corrupt the split of the container that DIRECTLY holds it and of no
//! other level — every enclosing level sees that member's own brackets and never
//! looks inside them. Exactly one cause breaks that argument, and it is therefore
//! the one whole-CELL refusal.
//!
//! ## Node-local causes ([`node_refusal`]) — this node's member split, nothing else
//!
//! * **EMPTY-CONTAINER: an EMPTY container vs a container of one EMPTY member.**
//!   Members are unquoted and unseparated at count 1, so a `set<text>` holding
//!   exactly the empty string renders `{}` — byte for byte what an EMPTY set
//!   renders as. The two are different values, so neither reading is trustworthy
//!   and the node is REFUSED. The rule is bounded by the DECLARED element type: a
//!   `list<int>` member always carries a digit, so `[]` there can only mean zero
//!   members and IS compared. It is the mirror of EMPTY-MEMBER below, which
//!   refuses the case where the GOLDEN carries the empty member; without it the
//!   golden-side scan saw nothing to refuse in an empty golden container while the
//!   CLI could perfectly well have held one empty member.
//! * **EMPTY-MEMBER: a scalar member that renders as the empty string.** One empty
//!   member and zero members produce the same empty body, so THIS node's member
//!   count is unrecoverable.
//! * **SEPARATOR: a `, ` inside a direct scalar member.** The body splits at every
//!   depth-zero `, `, so this node's member count and contents are unrecoverable.
//! * **KEY-SEPARATOR: a `: ` or a `, ` inside a map/UDT KEY.** Entries split at
//!   their FIRST top-level `: `, so a `: ` in a KEY moves the key/value cut (a
//!   colon inside a VALUE is already correct and is NOT refused), and a `, ` in a
//!   key splits one entry into two. Both destroy THIS object's entries.
//! * **VALUE-SEPARATOR: a `, ` inside a map/UDT scalar VALUE.** Entries split at
//!   every top-level `, ` before the key/value cut is made, so one in a VALUE
//!   splits one entry into two just as one in a key does — THIS object's entries
//!   again. Scanning only the KEYS is what made CORRECT output for a golden
//!   `{"k": "a, b"}` — the rendering `{k: a, b}` — be reported `unparseable`
//!   rather than refused (review round 10, finding Q2). A lane that reds on
//!   correct input is the lane agents learn to waive (CLAUDE.md), so this
//!   direction matters more than the permissive one.
//!
//! ## The whole-cell cause ([`cell_refusal`]) — a STRUCTURAL character
//!
//! A `[`, `]`, `{`, `}`, `(` or `)` inside a member's text unbalances the depth
//! counter for every enclosing level at once, so no level of the rendering can be
//! split reliably and the CELL is refused before the decode is attempted. Scanned
//! recursively for that reason: a structural character at any depth is a
//! whole-cell property.
//!
//! ## Not a refusal: NULL-TOKEN, `null` vs the text `"null"`
//!
//! A container has no empty-field mechanism, so `ValueFormatter` spells a null
//! member `null` — the same text a `text` member holding `"null"` produces (issue
//! #1499's ambiguity, one level in). The token is resolved from the GOLDEN's own
//! type: null there decodes to null here, anything else stays text. That keeps the
//! distinction wherever the oracle knows it, and loses it only where CSV genuinely
//! cannot express it. A CLI that emits the wrong member still fails — only the
//! exact null/`"null"` swap is invisible.
//!
//! ## What survives a refusal, stated exactly — and the residual it leaves
//!
//! EXACTLY TWO things are still required at a refused position, and it is worth
//! saying so precisely, because an earlier wording here ("never a blind spot the
//! size of the node"; "the member counts no confusable reading can explain are
//! still compared") implied a stronger guarantee than the code has ever had —
//! review round 10, finding Q1:
//!
//!   1. the FRAME: the position carries a text rendering, framed with the bracket
//!      pair the DECLARED type requires;
//!   2. BODY EMPTINESS, in the two directions no confusable reading can reach: an
//!      EMPTY golden container must render as an empty body, and a golden of TWO
//!      OR MORE members must render as a non-empty one.
//!
//! See [`decidable_despite_node_refusal`] and [`decidable_despite_cell_refusal`]
//! (finding N3), which apply the two together, and [`body_emptiness_bound`] for
//! the second.
//!
//! THE RESIDUAL, which is real and is therefore declared rather than implied
//! away: WHICH members the body holds is NOT compared at a refused node. So a
//! golden of two or more members accepts ANY non-empty body there, and a golden
//! of EXACTLY ONE member accepts ANY body at all (an empty body is the legal
//! rendering of one empty member, so not even emptiness is decidable there). At
//! such a node the blind spot IS the size of the node's CONTENT — bounded by the
//! frame and by emptiness, and by nothing else. The unit cases below assert that
//! residual directly, so a future strengthening has to come and change them.
//!
//! What keeps it from being a SILENT gap is the census: every refused position is
//! counted and NAMED by path and cause, and a refused cell is deliberately not
//! counted as compared container coverage. Every refusal is decided from the
//! GOLDEN and the committed DDL alone, so it can never be caused by the very
//! defect under test.

use super::schema::CqlType;
use serde_json::{Map, Value};

/// Characters that carry structure in the rendering and therefore cannot appear
/// inside an unquoted member without making it unparseable.
const STRUCTURAL: [char; 6] = ['[', ']', '{', '}', '(', ')'];

/// The ONE bracket pair a container of this declared type may be rendered with
/// (the grammar in the module doc), or `None` for a scalar type.
///
/// Taken from the DDL, so each kind is required to use its own bracket: a `set`
/// rendered `[a, b]` or a `tuple` rendered `[a, b]` is a failure (review finding
/// R2), where the earlier golden-shape-only rule accepted any of the three.
fn brackets(ty: &CqlType) -> Option<(char, char)> {
    match ty {
        CqlType::List(_) => Some(('[', ']')),
        CqlType::Set(_) => Some(('{', '}')),
        CqlType::Tuple(_) => Some(('(', ')')),
        CqlType::Map(..) | CqlType::Udt(_) => Some(('{', '}')),
        _ => None,
    }
}

/// Does the golden's own content make the WHOLE cell's rendering unsplittable?
/// `Some(reason)` means it does, and the cell is refused before the decode is
/// attempted.
///
/// The one cause with that blast radius is a STRUCTURAL character in a member's
/// (or a key's) text: [`scan`] tracks bracket depth, so a stray bracket anywhere
/// in the rendering corrupts every enclosing level's split at once — and would
/// otherwise surface as an "unbalanced bracket" DIVERGENCE caused by the golden
/// rather than by the CLI. Scanned recursively for exactly that reason.
///
/// Decided from the GOLDEN — never from the CLI's output — so a refusal can never
/// be produced by the defect the lane is looking for. The declared type is
/// deliberately NOT a parameter: a bracket is structural whatever the DDL says,
/// so there is no type-dependent narrowing to state here.
pub fn cell_refusal(golden: &Value) -> Option<String> {
    match golden {
        Value::Array(items) => items.iter().find_map(cell_refusal),
        Value::Object(fields) => fields.iter().find_map(|(key, value)| {
            structural_char(key)
                .map(|why| format!("map/UDT key: {why}"))
                .or_else(|| cell_refusal(value))
        }),
        scalar => structural_char(&scalar_text(scalar)),
    }
}

/// Is the golden AT THIS NODE unrecoverable from the flat rendering, for a reason
/// whose blast radius is THIS node's member split? `Some(reason)` means the node's
/// contents and count are refused — and nothing else is (review finding P2).
///
/// NON-RECURSIVE by construction: every cause it reports is a property of this
/// node's own body, so a nested position's refusal is reported when the walk
/// reaches THAT node and cannot suppress this one's siblings, count or frame. Both
/// the decoder ([`decode`]) and the comparator ask this at every node, which is
/// what keeps "what the decoder left" and "what the comparator expects" the same
/// question.
///
/// `ty` is the declared type of THIS position; `None` means the declared type does
/// not describe this shape (the comparison reports that), and no type-dependent
/// rule may then fire — refusing there would suppress a real divergence rather
/// than declare a format limit.
///
/// The DDL is consulted for ONE question only: whether a member of the declared
/// element type can render as the empty string, which is what decides
/// EMPTY-CONTAINER. Everything else is read from the golden's own content.
pub fn node_refusal(golden: &Value, ty: Option<&CqlType>) -> Option<String> {
    match golden {
        Value::Array(items) => {
            // EMPTY-CONTAINER: zero members and one EMPTY member render
            // identically. Keyed on the AFFIRMATIVE answer — this element type
            // really can render empty — so an unknown or non-collection type does
            // not refuse.
            if items.is_empty() && empty_container_is_ambiguous(ty) {
                return Some(
                    "an empty container is indistinguishable from a container of one empty \
                     member of this element type"
                        .into(),
                );
            }
            items
                .iter()
                .filter(|item| is_scalar(item))
                .find_map(|item| {
                    let text = scalar_text(item);
                    // EMPTY-MEMBER: a member rendering to the empty string makes the
                    // member count unrecoverable — one empty member and zero members
                    // both render as an empty body.
                    if text.is_empty() {
                        return Some(
                            "an empty scalar member is indistinguishable from no member".into(),
                        );
                    }
                    // SEPARATOR: this node's body splits at every depth-zero `, `.
                    separator_in_member(&text)
                })
        }
        Value::Object(fields) => fields.iter().find_map(|(key, value)| {
            // KEY-SEPARATOR. Only a KEY is harmed by `: `: entries are split at
            // their FIRST top-level `: `, so a colon inside a VALUE is already
            // correct.
            if key.contains(": ") {
                return Some(format!(
                    "map/UDT key {} contains the `: ` separator",
                    brief(key)
                ));
            }
            if let Some(why) = separator_in_member(key) {
                return Some(format!("map/UDT key: {why}"));
            }
            // VALUE-SEPARATOR: a `, ` in a SCALAR value splits one entry into two,
            // destroying this object's entries exactly as one in a key does. Only
            // a scalar's, because a nested container's members are split inside
            // its own brackets and reported at ITS node.
            if is_scalar(value) {
                if let Some(why) = separator_in_member(&scalar_text(value)) {
                    return Some(format!("map/UDT value at key {}: {why}", brief(key)));
                }
            }
            None
        }),
        // A scalar is never refused for itself: the causes above are all about the
        // BODY that holds it, so the container one level up is the node that
        // reports them.
        _ => None,
    }
}

/// SEPARATOR, for one member's text.
fn separator_in_member(text: &str) -> Option<String> {
    if text.contains(", ") {
        return Some(format!(
            "member {} contains the `, ` separator",
            brief(text)
        ));
    }
    None
}

/// STRUCTURAL, for one member's text.
fn structural_char(text: &str) -> Option<String> {
    STRUCTURAL.iter().find(|c| text.contains(**c)).map(|found| {
        format!(
            "member {} contains the structural character `{found}`",
            brief(text)
        )
    })
}

/// The text `ValueFormatter` renders a scalar as, for the ambiguity scan only.
/// `Value::Null` renders as the `null` token (NULL-TOKEN in the module doc), which
/// is a text a `text` member can also produce — resolved by [`decode_shape`] from
/// the golden's own type, and deliberately not a refusal.
fn scalar_text(scalar: &Value) -> String {
    match scalar {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

fn is_scalar(v: &Value) -> bool {
    !matches!(v, Value::Array(_) | Value::Object(_))
}

/// Is an EMPTY golden array of this declared type genuinely unrecoverable?
///
/// Only for a `list`/`set` whose ELEMENT can render as the empty string. A
/// `tuple` is exempt because its member count comes from the DDL, so the
/// comparison's arity check sees a dropped member; every other type does not
/// describe an array at all, and refusing there would hide the shape divergence
/// the comparison exists to report.
fn empty_container_is_ambiguous(ty: Option<&CqlType>) -> bool {
    match ty {
        Some(CqlType::List(element)) | Some(CqlType::Set(element)) => {
            member_can_render_empty(element)
        }
        _ => false,
    }
}

/// Can a value of this declared type render as the EMPTY string?
///
/// Deliberately CONSERVATIVE — it answers `false` only for the types whose every
/// rendering provably carries at least one character: a number always carries a
/// digit, a boolean is `true`/`false`, and a container carries its brackets (an
/// empty one renders as the bracket pair, not as nothing). Everything else —
/// `text`/`varchar`/`ascii`, which hold the empty string, and every type whose
/// empty-value spelling this lane has not established — answers `true`, because
/// the cost of over-refusing is a counted, NAMED gap in the census while the cost
/// of under-refusing is a false pass.
fn member_can_render_empty(ty: &CqlType) -> bool {
    !matches!(
        ty,
        CqlType::Numeric(_)
            | CqlType::Boolean
            | CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(..)
            | CqlType::Tuple(_)
            | CqlType::Udt(_)
    )
}

/// The declared type of member `i` of an array position, or `None` when the
/// declared type does not describe an array.
fn member_type(ty: Option<&CqlType>, i: usize) -> Option<&CqlType> {
    match ty {
        Some(CqlType::List(element)) | Some(CqlType::Set(element)) => Some(element),
        Some(CqlType::Tuple(items)) => items.get(i),
        _ => None,
    }
}

/// The declared type of entry `key` of an object position: a map's VALUE type, or
/// the named UDT field's type. `None` for an undeclared field or a type that does
/// not describe an object.
fn field_type<'t>(ty: Option<&'t CqlType>, key: &str) -> Option<&'t CqlType> {
    match ty {
        Some(CqlType::Map(_, value)) => Some(value),
        Some(CqlType::Udt(udt)) => udt
            .fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, t)| t),
        _ => None,
    }
}

/// What is STILL decidable at a position whose golden was refused — so a refusal
/// suppresses only the genuinely indistinguishable readings and not the whole
/// position (issue #1491 review finding N3).
///
/// Every refusal cause is about the CONTENT of a body: which members it holds, and
/// how many. None is about its FRAME. So three properties survive every cause,
/// each decided from the GOLDEN and the committed DDL exactly as the refusal
/// itself is — and these three are ALL that survive, which the module doc states
/// together with the residual they leave (finding Q1):
///
///   1. the position carries a rendering AT ALL. The golden is a container, and
///      the shortest rendering of any container is its bracket PAIR, so an empty
///      CSV field (which [`super::compare::cli_csv_rows`] reads as `null`) or a
///      non-text cell is a divergence, not an ambiguity;
///   2. it is framed with the bracket pair the DECLARED type requires — the same
///      rule [`strip`] applies on the decodable path, where a `set` rendered
///      `[a, b]` is a failure (review finding R2);
///   3. BODY EMPTINESS, i.e. the member count in the only two directions no
///      confusable reading can reach — never WHICH members the body holds. A
///      golden container with NO members can only render as the empty bracket
///      pair: both readings EMPTY-CONTAINER confuses — zero members, and one
///      member that renders empty — are `{}` byte for byte, so ANY other body is a
///      third thing and diverges. Symmetrically a golden with TWO OR MORE members
///      cannot render as an empty body, because even all-empty members are
///      separated by `, `. (At exactly ONE member the empty body IS a legal
///      rendering — of a single empty member — so nothing is asserted there.)
///
/// What stays suppressed is exactly the indistinguishable set: WHICH members the
/// body holds, and how many when the count is 1.
///
/// # Two entry points, one rule
///
/// [`decidable_despite_cell_refusal`] is the CELL-level one: the cell was refused
/// before any decode, so it holds the raw rendering and strips the frame itself.
/// [`decidable_despite_node_refusal`] is the per-NODE one used inside the walk,
/// where the decoder has already required and stripped this node's frame (that IS
/// property 2, applied at every depth) and left the un-split BODY. Both then apply
/// the identical emptiness bound, stated once in [`body_emptiness_bound`].
pub fn decidable_despite_cell_refusal(
    golden: &Value,
    cli: &Value,
    ty: &CqlType,
) -> Result<(), String> {
    let Some(members) = member_count(golden) else {
        // Not a container, so nothing was refused for it; `cell_refusal` only
        // refuses a cell whose golden is one.
        return Ok(());
    };
    let text = cli_text(cli)?;
    body_emptiness_bound(members, strip(text, ty)?, text)
}

/// [`decidable_despite_cell_refusal`] for a node the DECODER refused: `cli` is the
/// body it left after requiring and stripping this node's declared bracket pair,
/// so only the count bounds remain to be applied.
pub fn decidable_despite_node_refusal(golden: &Value, cli: &Value) -> Result<(), String> {
    let Some(members) = member_count(golden) else {
        return Ok(());
    };
    let body = cli_text(cli)?;
    body_emptiness_bound(members, body, body)
}

/// The member count of a golden container, or `None` for a scalar.
fn member_count(golden: &Value) -> Option<usize> {
    match golden {
        Value::Array(items) => Some(items.len()),
        Value::Object(fields) => Some(fields.len()),
        _ => None,
    }
}

/// The CLI side of a refused position must be TEXT: property 1 above.
fn cli_text(cli: &Value) -> Result<&str, String> {
    match cli {
        Value::String(text) => Ok(text),
        other => Err(format!(
            "the golden carries a container the CSV rendering cannot express \
             unambiguously, but the csv egress cell is {} — a container always \
             renders as at least its bracket pair, so an empty or non-text field \
             is a divergence the ambiguity does not cover",
            match other {
                Value::Null => "absent/empty".to_string(),
                other => brief(&other.to_string()),
            }
        )),
    }
}

/// Property 3, and NOTHING more: is the body's EMPTINESS consistent with the
/// golden's member count, in the only two directions no confusable reading can
/// reach?
///
/// Named for what it checks, because it is NOT a bound on the count itself: at
/// exactly ONE member it accepts any body, and at two or more it accepts any
/// NON-EMPTY body, so WHICH members the body holds is never compared here (the
/// module doc's residual, finding Q1).
///
/// `rendering` is what the diagnostic quotes (the whole cell, or the body).
fn body_emptiness_bound(members: usize, body: &str, rendering: &str) -> Result<(), String> {
    if members == 0 && !body.is_empty() {
        return Err(format!(
            "the golden container is EMPTY, so the only renderings the CSV \
             ambiguity confuses are the empty bracket pair — but the csv egress \
             cell {} carries a body",
            brief(rendering)
        ));
    }
    if members >= 2 && body.is_empty() {
        return Err(format!(
            "the golden container holds {members} members, which cannot render as \
             an empty body (members are `, `-separated even when each is empty), \
             but the csv egress cell is {}",
            brief(rendering)
        ));
    }
    Ok(())
}

/// A predicate over a value path: `true` means the comparison excludes that path,
/// so the decoder must not require the CLI's text there to invert the grammar.
///
/// Kept as a bare closure type rather than a dependency on the comparator's
/// `SkipPaths`, so this module stays independent of it.
pub type Excluded<'a> = dyn Fn(&str) -> bool + 'a;

/// Decode `text` (one CSV field, or one member of one) into the shape `golden`
/// declares, with `ty` — the column's DECLARED CQL type — supplying the bracket
/// each container kind must be rendered with. A map/UDT decodes to the
/// `[{"key":…,"value":…}, …]` spelling the JSON egress uses, so the existing map
/// comparison applies unchanged.
pub fn decode(golden: &Value, text: &str, ty: &CqlType) -> Result<Value, String> {
    decode_at(golden, text, ty, "", &|_| false)
}

/// [`decode`], but aware of the paths the comparison excludes.
///
/// `path` is the fully-qualified position of `text` in the row, spelled the same
/// way the comparator spells it (`col.field` for a named field, `col[i]` for a
/// positional member). At an EXCLUDED path the decode is ATTEMPTED and its result
/// used when it succeeds, falling back to the raw, UNDECODED text only when the
/// grammar does not invert there. That fallback is what keeps one un-invertible
/// member from failing a whole cell nobody compares — which is what forced the
/// `udt_nested` exclusion to be whole-column (issue #1491 review finding F5).
///
/// Attempting it rather than short-circuiting matters for the STALENESS side
/// (finding L1): an exclusion is applied only while it suppresses a real
/// divergence, and an unconditional raw-text answer here would keep the excluded
/// position diverging (an object against a string) even after CQLite renders it
/// correctly — so the gap could never retire itself.
///
/// The refusal scan is deliberately NOT exclusion-aware: it is decided from the
/// golden alone, and refusing a node is a conservative, counted, NAMED outcome in
/// the census, never a silent pass.
pub fn decode_at(
    golden: &Value,
    text: &str,
    ty: &CqlType,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    if excluded(path) {
        return Ok(decode_shape(golden, text, ty, path, excluded)
            .unwrap_or_else(|_| Value::String(text.to_string())));
    }
    decode_shape(golden, text, ty, path, excluded)
}

/// The decode itself, with no exclusion check of its own at this level — that
/// belongs to [`decode_at`], so this can be run for an excluded path too. Nested
/// members still go through [`decode_at`], so a deeper exclusion applies normally.
fn decode_shape(
    golden: &Value,
    text: &str,
    ty: &CqlType,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    // A node the GOLDEN's own content makes unsplittable is not split: its FRAME
    // is still required (that is `strip`, i.e. property 2 of
    // `decidable_despite_node_refusal`, applied at every depth) and its un-split
    // BODY is handed on for the count bounds. Interpreting the body here is what
    // would produce a member count no reading of the rendering supports — and
    // deciding the refusal for the whole CELL instead is what let an ambiguous
    // NESTED member suppress its unambiguous siblings and the outer structure
    // (review finding P2).
    //
    // The comparator asks `node_refusal` at the same node, on the same golden and
    // the same declared type, so it expects exactly what is left here.
    if node_refusal(golden, Some(ty)).is_some() {
        return Ok(Value::String(strip(text, ty)?.to_string()));
    }
    // The declared TYPE decides the structure — including which bracket is
    // required — and the golden decides the member shapes underneath it. When the
    // two disagree the child is decoded against `null`, and the comparison is what
    // reports the shape divergence.
    match ty {
        CqlType::List(element) | CqlType::Set(element) => {
            decode_sequence(golden, text, ty, &|_| Some(element), path, excluded)
        }
        CqlType::Tuple(items) => {
            decode_sequence(golden, text, ty, &|i| items.get(i), path, excluded)
        }
        CqlType::Map(_, value_ty) => {
            decode_object(golden, text, ty, &|_| Some(value_ty), path, excluded)
        }
        CqlType::Udt(udt) => decode_object(
            golden,
            text,
            ty,
            &|field| {
                udt.fields
                    .iter()
                    .find(|(name, _)| name == field)
                    .map(|(_, t)| t)
            },
            path,
            excluded,
        ),
        // NULL-TOKEN: the golden's own type resolves the `null` token.
        _ => match golden {
            Value::Null if text == "null" => Ok(Value::Null),
            _ => Ok(Value::String(text.to_string())),
        },
    }
}

/// A list / set / tuple: one bracket pair fixed by `ty`, `, `-separated members,
/// each decoded under the element type `element_ty` gives for its position.
///
/// `element_ty` answers `None` only for a member BEYOND a tuple's declared arity,
/// which has no declared type; such a member is kept as raw text so the
/// comparator reports the arity divergence rather than the decoder swallowing it.
fn decode_sequence<'t>(
    golden: &Value,
    text: &str,
    ty: &'t CqlType,
    element_ty: &dyn Fn(usize) -> Option<&'t CqlType>,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    let parts = members(text, ty)?;
    let items = golden.as_array();
    let mut out = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        // A member the golden does not have is decoded against `null`; the
        // length mismatch is what the comparison then reports.
        let child_golden = items.and_then(|g| g.get(i)).unwrap_or(&Value::Null);
        out.push(match element_ty(i) {
            Some(et) => decode_at(child_golden, part, et, &format!("{path}[{i}]"), excluded)?,
            None => Value::String((*part).to_string()),
        });
    }
    Ok(Value::Array(out))
}

/// A map or UDT: `{…}`, `, `-separated `key: value` entries. `value_ty` answers
/// with the declared type of the value under a given key — `None` for a UDT field
/// the `CREATE TYPE` does not declare, whose value is therefore left as raw text
/// for the comparator to reject by name.
fn decode_object<'t>(
    golden: &Value,
    text: &str,
    ty: &'t CqlType,
    value_ty: &dyn Fn(&str) -> Option<&'t CqlType>,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    let parts = members(text, ty)?;
    let fields = golden.as_object();
    let mut out = Vec::with_capacity(parts.len());
    for part in parts {
        let cut = *scan(part, ": ")?.first().ok_or_else(|| {
            format!(
                "map/UDT entry {} has no top-level `: ` separator",
                brief(part)
            )
        })?;
        let (key, value) = (&part[..cut], &part[cut + 2..]);
        let mut entry = Map::new();
        entry.insert("key".to_string(), Value::String(key.to_string()));
        // A UDT field step, spelled `parent.field` as the comparator spells it. A
        // MAP key reaches the same branch (CSV cannot tell the two apart), but a
        // dotted skip path through a map is rejected when the case is validated
        // against the DDL, so no exclusion can ever name one.
        let child = if path.is_empty() {
            key.to_string()
        } else {
            format!("{path}.{key}")
        };
        let child_golden = fields.and_then(|g| g.get(key)).unwrap_or(&Value::Null);
        let decoded = match value_ty(key) {
            Some(vt) => decode_at(child_golden, value, vt, &child, excluded)?,
            None => Value::String(value.to_string()),
        };
        entry.insert("value".to_string(), decoded);
        out.push(Value::Object(entry));
    }
    Ok(Value::Array(out))
}

/// Strip the bracket pair `ty` requires and split the body at every depth-zero
/// `, `. An empty body is zero members.
fn members<'a>(text: &'a str, ty: &CqlType) -> Result<Vec<&'a str>, String> {
    let inner = strip(text, ty)?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    split_top_level(inner, ", ")
}

/// Remove the opening/closing bracket pair the DECLARED type requires. Strict in
/// both directions: a body that does not open with THAT bracket, or does not close
/// with its mate, is an error rather than a best-effort parse — so a `set`
/// rendered `[a, b]` fails instead of being read as a list.
fn strip<'a>(text: &'a str, ty: &CqlType) -> Result<&'a str, String> {
    let (open, close) = brackets(ty).ok_or_else(|| {
        format!(
            "{} was decoded as a container but the schema declares the scalar type \
             `{}`",
            brief(text),
            ty.describe()
        )
    })?;
    let rest = text.strip_prefix(open).ok_or_else(|| {
        format!(
            "{} is not a `{}` rendering: the declared type requires an opening \
             `{open}` (`{open}…{close}`)",
            brief(text),
            ty.describe()
        )
    })?;
    rest.strip_suffix(close).ok_or_else(|| {
        format!(
            "{} opens with `{open}` but does not close with `{close}`",
            brief(text)
        )
    })
}

/// Split `body` at every depth-zero `sep`.
fn split_top_level<'a>(body: &'a str, sep: &str) -> Result<Vec<&'a str>, String> {
    let cuts = scan(body, sep)?;
    let mut parts = Vec::with_capacity(cuts.len() + 1);
    let mut start = 0usize;
    for cut in cuts {
        parts.push(&body[start..cut]);
        start = cut + sep.len();
    }
    parts.push(&body[start..]);
    Ok(parts)
}

/// Byte offsets of every depth-zero, non-overlapping `sep` in `body`.
///
/// Iterates by `char_indices` so slicing stays on UTF-8 boundaries (member text
/// is arbitrary CQL `text`). Unbalanced brackets are an error — the rendering is
/// then not the grammar this decoder inverts, and silently tolerating it is how
/// a decoder starts absorbing writer defects.
fn scan(body: &str, sep: &str) -> Result<Vec<usize>, String> {
    let mut cuts = Vec::new();
    let mut depth: i32 = 0;
    let mut consumed = 0usize;
    for (idx, ch) in body.char_indices() {
        match ch {
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(format!(
                        "{} closes a bracket that never opened",
                        brief(body)
                    ));
                }
            }
            _ => {}
        }
        if depth == 0 && idx >= consumed && body[idx..].starts_with(sep) {
            cuts.push(idx);
            consumed = idx + sep.len();
        }
    }
    if depth != 0 {
        return Err(format!(
            "{} leaves {depth} bracket(s) unclosed",
            brief(body)
        ));
    }
    Ok(cuts)
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

// ===========================================================================
// Unit coverage for the branches the corpus does not reach
// ===========================================================================
//
// The corpus reaches ONE of the refusal causes and none of the strictness rules.
// Measured on the census: with the fetched corpus present the CSV lane reports
// `1 REFUSED` — `test_types.nb_empty_collections`'s `fs`, a `frozen<set<text>>`
// the golden carries EMPTY (ambiguity 0). The committed tier alone reports
// `0 REFUSED`, and no container member anywhere in the committed or fetched
// corpus carries a `, `, a bracket, a `: ` in a map key, or an empty scalar
// member. So the other causes, and every strictness rule, are exercised only
// here — which is what makes a census `0` mean "the scan ran and found none"
// rather than "the scan may not work".
//
// Inputs are renderings in the grammar `ValueFormatter` documents; expected
// outputs are the GOLDEN-side shapes `sstabledump` produces. Nothing here is
// derived from CQLite's current output.
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The declared type of a column, parsed by the lane's OWN DDL parser from a
    /// `CREATE TABLE` — so these cases exercise the real authority (the committed
    /// schema) rather than a hand-built type tree.
    fn ty_of(decl: &str) -> CqlType {
        let ddl = format!(
            "CREATE TYPE address (street text, city text, zip text); \
             CREATE TYPE person (first_name text, last_name text, age int); \
             CREATE TABLE t (id int PRIMARY KEY, c {decl});"
        );
        let schema = match super::super::schema::from_ddl(&ddl, "t") {
            Ok(schema) => schema,
            Err(why) => panic!("{decl}: {why}"),
        };
        match schema.column("c") {
            Some(column) => column.ty.clone(),
            None => panic!("{decl}: no column `c`"),
        }
    }

    // --- the refusal valve, per NODE and per CELL --------------------------
    //
    // Each case pins BOTH which function refuses and which does NOT, because the
    // whole content of finding P2 is the BLAST RADIUS: a node-local cause that
    // reported a whole-cell refusal would suppress positions that are decidable,
    // and a whole-cell cause reported per node would let an unbalanced rendering
    // be split anyway.

    /// The element type's own bracket pair, for a node-local refusal query on a
    /// nested position (`node_refusal` takes the declared type OF THAT NODE).
    fn element_of(decl: &str) -> CqlType {
        match ty_of(decl) {
            CqlType::List(element) | CqlType::Set(element) => (*element).clone(),
            other => panic!("{decl} is not a list/set: {}", other.describe()),
        }
    }

    #[test]
    fn member_containing_the_element_separator_is_refused_at_its_container() {
        // `{"a, b"}` and `{"a", "b"}` render identically, so no reading of THIS
        // body is trustworthy…
        let ty = ty_of("set<text>");
        let why = node_refusal(&json!(["a, b"]), Some(&ty))
            .expect("a `, `-bearing member must refuse its container");
        assert!(why.contains("`, ` separator"), "unexpected reason: {why}");
        // …and the rendering is still splittable at every OTHER depth, so the cell
        // is not refused: a `, ` inside a member sits at bracket depth ≥ 1 for
        // every enclosing level.
        assert_eq!(
            cell_refusal(&json!(["a, b"])),
            None,
            "a `, ` corrupts one body's split, not the whole cell's"
        );
    }

    #[test]
    fn member_containing_a_bracket_refuses_the_whole_cell() {
        // A stray bracket unbalances the depth counter every level is split on, so
        // no level can be split reliably.
        let why = cell_refusal(&json!(["x}y"])).expect("a bracket-bearing member must be refused");
        assert!(
            why.contains("structural character"),
            "unexpected reason: {why}"
        );
        // At depth, too: the cause is a property of the whole rendering.
        assert!(cell_refusal(&json!([["x}y"]])).is_some());
        assert!(cell_refusal(&json!({"k": "x}y"})).is_some());
        assert!(cell_refusal(&json!({"x}y": 1})).is_some());
    }

    #[test]
    fn map_key_containing_a_separator_is_refused_at_its_object() {
        let ty = ty_of("map<text, int>");
        let why = node_refusal(&json!({"a: b": 1}), Some(&ty))
            .expect("a `: `-bearing KEY must refuse its object");
        assert!(why.contains("key"), "unexpected reason: {why}");
        // A `, ` in a key splits one entry into two, which is the same loss.
        let why = node_refusal(&json!({"a, b": 1}), Some(&ty))
            .expect("a `, `-bearing KEY must refuse its object");
        assert!(why.contains("key"), "unexpected reason: {why}");
        assert_eq!(cell_refusal(&json!({"a: b": 1})), None);
    }

    #[test]
    fn map_value_containing_the_pair_separator_is_not_refused() {
        // Entries split at their FIRST top-level `: `, which is the real
        // separator, so a colon inside the VALUE is already decoded correctly.
        // Refusing it would narrow the lane for no reason.
        let ty = ty_of("map<text, text>");
        assert_eq!(node_refusal(&json!({"k": "a: b"}), Some(&ty)), None);
        assert_eq!(cell_refusal(&json!({"k": "a: b"})), None);
        let decoded = decode(&json!({"k": "a: b"}), "{k: a: b}", &ty).expect("decodes");
        assert_eq!(decoded, json!([{"key": "k", "value": "a: b"}]));
    }

    #[test]
    fn an_empty_member_of_a_non_empty_collection_is_refused() {
        // `{}` is both "no members" and "one empty member".
        let ty = ty_of("set<text>");
        let why = node_refusal(&json!([""]), Some(&ty)).expect("an empty member must be refused");
        assert!(
            why.contains("empty scalar member"),
            "unexpected reason: {why}"
        );
    }

    /// EMPTY-CONTAINER, the MIRROR of the case above: the golden container is
    /// EMPTY and the CLI could perfectly well have held one member that renders
    /// empty. The golden-side scan saw nothing to refuse there, so `{}` accepted
    /// both readings — and the two are different values.
    ///
    /// Bounded by the DECLARED element type, which is what makes this a
    /// measurement and not blanket strictness: a `set<text>` member can BE the
    /// empty string, a `list<int>` member always carries a digit, and a `tuple`'s
    /// member count comes from the DDL (so the comparison's arity check sees a
    /// dropped member).
    #[test]
    fn an_empty_container_is_refused_only_where_its_element_can_render_empty() {
        for decl in ["set<text>", "list<ascii>", "set<blob>", "list<timestamp>"] {
            let ty = ty_of(decl);
            let why = node_refusal(&json!([]), Some(&ty))
                .unwrap_or_else(|| panic!("{decl}: an empty container must be refused"));
            assert!(
                why.contains("empty container is indistinguishable"),
                "{decl}: unexpected reason: {why}"
            );
        }
        for decl in [
            "set<int>",
            "list<double>",
            "set<boolean>",
            "list<frozen<set<int>>>",
        ] {
            assert_eq!(
                node_refusal(&json!([]), Some(&ty_of(decl))),
                None,
                "{decl}: no member of this element type can render empty, so `[]` can \
                 only mean zero members and must stay compared"
            );
        }
        // A tuple's arity is the DDL's, so `()` cannot hide a member.
        assert_eq!(
            node_refusal(&json!([]), Some(&ty_of("tuple<text, text>"))),
            None
        );
        // An empty map/UDT body is unambiguous too: every entry carries a `: `, so
        // a one-entry rendering can never be `{}`.
        assert_eq!(
            node_refusal(&json!({}), Some(&ty_of("map<text, text>"))),
            None
        );
        // And an UNDECLARED type refuses nothing: the comparison reports the shape.
        assert_eq!(node_refusal(&json!([]), None), None);
    }

    /// Finding P2: an ambiguous NESTED member is refused AT ITSELF, so the outer
    /// container stays decidable. Deciding it one level up made a golden `[[]]`
    /// accept a CLI `[]`, the unambiguous outer member silently dropped.
    ///
    /// The end-to-end half of this (the comparator reporting that dropped member)
    /// is `compare::tests::an_ambiguous_nested_member_does_not_suppress_its_container`.
    #[test]
    fn an_ambiguous_nested_member_is_refused_at_itself_and_not_at_its_container() {
        let outer = ty_of("list<frozen<list<text>>>");
        let inner = element_of("list<frozen<list<text>>>");
        // The inner empty `list<text>` is the indistinguishable position…
        let why = node_refusal(&json!([]), Some(&inner))
            .expect("the inner empty list<text> is the ambiguous position");
        assert!(
            why.contains("empty container is indistinguishable"),
            "{why}"
        );
        // …and the OUTER container, which holds exactly one member, is decidable:
        // its bracket kind and its member count are recoverable whatever the
        // member's own body turns out to mean.
        assert_eq!(
            node_refusal(&json!([[]]), Some(&outer)),
            None,
            "the outer container's own body is splittable, so it is not refused"
        );
        assert_eq!(cell_refusal(&json!([[]])), None);
        // The decode reflects that: the outer level is SPLIT (one member), and only
        // the refused member's body is left un-split for the count bounds.
        assert_eq!(
            decode(&json!([[]]), "[[]]", &outer).expect("decodes"),
            json!([""]),
            "one outer member, whose refused body is empty"
        );
        // A dropped outer member decodes to zero members, which is what lets the
        // comparator report it.
        assert_eq!(
            decode(&json!([[]]), "[]", &outer).expect("decodes"),
            json!([])
        );
        // And the refused member's own FRAME is still required at its depth.
        assert!(
            decode(&json!([[]]), "[{}]", &outer).is_err(),
            "the inner list's `[…]` frame is required even though its body is refused"
        );
    }

    /// Finding N3: a refusal suppresses the INDISTINGUISHABLE readings only.
    ///
    /// The subject is the ambiguity the corpus actually reaches — an empty
    /// `frozen<set<text>>`, which renders `{}` whether it holds nothing or one
    /// empty member. Everything else about the cell is still decided, so a
    /// `null`, an unrelated word, the wrong bracket or a non-empty body is a
    /// divergence. Before this the whole cell was discarded before the CLI value
    /// was looked at, so all four passed.
    #[test]
    fn a_refused_cell_still_has_its_frame_and_body_emptiness_compared() {
        let ty = ty_of("frozen<set<text>>");
        let empty = json!([]);
        // The one reading pair the format genuinely cannot tell apart.
        assert_eq!(
            decidable_despite_cell_refusal(&empty, &json!("{}"), &ty),
            Ok(()),
            "the empty bracket pair is exactly the indistinguishable case"
        );
        for (cli, expect) in [
            (Value::Null, "empty or non-text field"),
            (json!("null"), "opening"),
            (json!("unrelated text"), "opening"),
            (json!("[]"), "opening"),
            (json!("{a}"), "carries a body"),
        ] {
            let why = decidable_despite_cell_refusal(&empty, &cli, &ty)
                .expect_err(&format!("{cli} must diverge from an empty golden set"));
            assert!(why.contains(expect), "unexpected reason for {cli}: {why}");
        }

        // The other decidable count: two or more members cannot render empty,
        // whatever the refusal cause — here a `, `-bearing member.
        let two = json!(["a, b", "c"]);
        assert!(
            node_refusal(&two, Some(&ty)).is_some(),
            "the `, ` in a member is what refuses this node"
        );
        let why = decidable_despite_cell_refusal(&two, &json!("{}"), &ty)
            .expect_err("two members cannot render as an empty body");
        assert!(why.contains("cannot render as an empty body"), "{why}");
        // …and WHICH members the body holds stays suppressed, because that is
        // what the ambiguity destroys.
        assert_eq!(
            decidable_despite_cell_refusal(&two, &json!("{something, else}"), &ty),
            Ok(())
        );
        // At exactly ONE member the empty body is a legal rendering (of one empty
        // member), so nothing is asserted about the count there.
        assert_eq!(
            decidable_despite_cell_refusal(&json!([""]), &json!("{}"), &ty),
            Ok(())
        );
    }

    /// The per-NODE half of the same rule, on the BODY the decoder leaves: the
    /// same emptiness bound, and the same suppression of which members the body
    /// holds.
    /// The frame is not re-checked here because the decoder already required it at
    /// that depth — which is what makes it checked at EVERY depth rather than only
    /// at the cell's outer level.
    #[test]
    fn a_refused_node_still_has_its_body_emptiness_compared() {
        for (golden, body, expect) in [
            (json!([]), "", None),
            (json!([]), "a", Some("carries a body")),
            (json!([""]), "", None),
            (json!([""]), "anything", None),
            (
                json!(["a, b", "c"]),
                "",
                Some("cannot render as an empty body"),
            ),
            (json!(["a, b", "c"]), "something, else", None),
        ] {
            let outcome = decidable_despite_node_refusal(&golden, &Value::String(body.into()));
            match expect {
                None => assert_eq!(outcome, Ok(()), "golden {golden} vs body `{body}`"),
                Some(needle) => {
                    let why = outcome.expect_err(&format!("golden {golden} vs body `{body}`"));
                    assert!(why.contains(needle), "unexpected reason: {why}");
                }
            }
        }
        // A refused node whose CLI side is not text at all: the decoder always
        // leaves a body, so this is a divergence rather than an ambiguity.
        assert!(decidable_despite_node_refusal(&json!([]), &Value::Null).is_err());
    }

    #[test]
    fn ordinary_corpus_content_is_not_refused() {
        // Spaces, hyphens, `0x` hex, exact decimals and nesting are all fine —
        // only the separators and brackets are structural. (`1 Navy Way` is real
        // content from test_compactionparityudt.udt_collections.)
        let map_ty = ty_of("map<text, frozen<address>>");
        let nested = json!({"home": {"street": "1 Navy Way", "zip": "22201"}});
        assert_eq!(cell_refusal(&nested), None);
        assert_eq!(node_refusal(&nested, Some(&map_ty)), None);
        // The nested UDT node too, under its own declared type.
        let address = match &map_ty {
            CqlType::Map(_, value) => (**value).clone(),
            other => panic!("not a map: {}", other.describe()),
        };
        assert_eq!(
            node_refusal(
                &json!({"street": "1 Navy Way", "zip": "22201"}),
                Some(&address)
            ),
            None
        );
        let list_ty = ty_of("list<text>");
        let scalars = json!(["0xdeadbeef", "-1.5", "neg-five", null]);
        assert_eq!(cell_refusal(&scalars), None);
        assert_eq!(node_refusal(&scalars, Some(&list_ty)), None);
    }

    // --- strictness: the decoder must not repair a malformed rendering ------

    #[test]
    fn the_element_separator_must_be_exactly_comma_space() {
        // A writer that dropped the space must NOT decode as two members; that
        // tolerance is what would let a framing regression through.
        let decoded =
            decode(&json!([1, 2]), "[1,2]", &ty_of("frozen<list<int>>")).expect("one member");
        assert_eq!(decoded, json!(["1,2"]), "`,` was wrongly treated as `, `");
    }

    #[test]
    fn a_mismatched_or_unbalanced_bracket_is_an_error() {
        let list = ty_of("frozen<list<int>>");
        assert!(
            decode(&json!([1]), "[1}", &list).is_err(),
            "mismatched bracket must fail"
        );
        assert!(
            decode(&json!([1]), "[[1]", &list).is_err(),
            "unclosed bracket must fail"
        );
        assert!(
            decode(&json!([1]), "1, 2", &list).is_err(),
            "a bare body must fail"
        );
        assert!(
            decode(&json!({"k": 1}), "[k: 1]", &ty_of("map<text, int>")).is_err(),
            "a map needs braces"
        );
    }

    #[test]
    fn a_map_entry_without_the_pair_separator_is_an_error() {
        assert!(decode(&json!({"k": 1}), "{k=1}", &ty_of("map<text, int>")).is_err());
    }

    // --- the bracket comes from the DECLARED type (review finding R2) -------

    #[test]
    fn each_collection_kind_requires_its_own_bracket() {
        // The grammar `ValueFormatter` documents, one kind per bracket. The
        // golden is a JSON array for all three, which is exactly why the DDL — not
        // the golden — has to answer the question.
        assert_eq!(
            decode(&json!([1, 2]), "[1, 2]", &ty_of("frozen<list<int>>")).unwrap(),
            json!(["1", "2"])
        );
        assert_eq!(
            decode(&json!([1, 2]), "{1, 2}", &ty_of("set<int>")).unwrap(),
            json!(["1", "2"])
        );
        assert_eq!(
            decode(&json!([1, 2]), "(1, 2)", &ty_of("tuple<int, int>")).unwrap(),
            json!(["1", "2"])
        );
    }

    /// The other side of R2: a set or tuple rendered with LIST brackets is a
    /// failure. The earlier rule accepted `[`, `{` and `(` for any golden array,
    /// so this regression passed.
    #[test]
    fn a_collection_rendered_with_another_kinds_bracket_is_an_error() {
        for (decl, wrong) in [
            ("set<int>", "[1, 2]"),
            ("set<int>", "(1, 2)"),
            ("tuple<int, int>", "[1, 2]"),
            ("tuple<int, int>", "{1, 2}"),
            ("frozen<list<int>>", "{1, 2}"),
            ("frozen<list<int>>", "(1, 2)"),
        ] {
            let ty = ty_of(decl);
            let why = decode(&json!([1, 2]), wrong, &ty)
                .expect_err("the declared kind's bracket is required: {decl} vs {wrong}");
            assert!(
                why.contains(&ty.describe()),
                "the failure must name the declared type: {why}"
            );
        }
        // A map/UDT rendered with list brackets likewise.
        assert!(decode(&json!({"k": 1}), "(k: 1)", &ty_of("map<text, int>")).is_err());
        assert!(decode(&json!({"zip": "1"}), "[zip: 1]", &ty_of("frozen<address>")).is_err());
    }

    /// A NESTED collection's bracket is required too — the type is threaded all
    /// the way down, so an inner set rendered `[…]` fails at depth.
    #[test]
    fn a_nested_collections_bracket_is_required_at_depth() {
        let ty = ty_of("frozen<map<text, frozen<set<int>>>>");
        assert_eq!(
            decode(&json!({"a": [1]}), "{a: {1}}", &ty).unwrap(),
            json!([{"key": "a", "value": ["1"]}])
        );
        assert!(
            decode(&json!({"a": [1]}), "{a: [1]}", &ty).is_err(),
            "an inner set rendered with list brackets must fail"
        );
    }

    // --- decoding ----------------------------------------------------------

    #[test]
    fn an_empty_body_decodes_to_zero_members() {
        assert_eq!(
            decode(&json!([]), "[]", &ty_of("frozen<list<int>>")).unwrap(),
            json!([])
        );
        assert_eq!(
            decode(&json!([]), "{}", &ty_of("set<int>")).unwrap(),
            json!([])
        );
        assert_eq!(
            decode(&json!({}), "{}", &ty_of("map<text, int>")).unwrap(),
            json!([])
        );
    }

    #[test]
    fn nesting_is_decoded_at_depth() {
        // A map<text, frozen<udt>>, as in test_compactionparityudt.udt_collections:
        // the inner `, ` and `: ` must not be mistaken for outer separators.
        let golden = json!({"home": {"street": "1 Navy Way", "city": "Arlington"}});
        let decoded = decode(
            &golden,
            "{home: {street: 1 Navy Way, city: Arlington}}",
            &ty_of("map<text, frozen<address>>"),
        )
        .unwrap();
        assert_eq!(
            decoded,
            json!([{
                "key": "home",
                "value": [
                    {"key": "street", "value": "1 Navy Way"},
                    {"key": "city", "value": "Arlington"},
                ],
            }])
        );
    }

    #[test]
    fn the_null_token_is_resolved_from_the_goldens_type() {
        // Ambiguity 1, in both directions: a null member decodes to null, and a
        // `text` member holding "null" stays text.
        let person = ty_of("frozen<person>");
        assert_eq!(
            decode(&json!({"last_name": null}), "{last_name: null}", &person).unwrap(),
            json!([{"key": "last_name", "value": null}])
        );
        assert_eq!(
            decode(&json!({"last_name": "null"}), "{last_name: null}", &person).unwrap(),
            json!([{"key": "last_name", "value": "null"}])
        );
    }

    #[test]
    fn a_surplus_member_is_kept_so_the_length_mismatch_is_reported() {
        // The decoder must not silently truncate to the golden's length — the
        // comparison is what reports the divergence.
        let decoded = decode(&json!([1]), "[1, 2]", &ty_of("frozen<list<int>>")).unwrap();
        assert_eq!(decoded, json!(["1", "2"]));
    }

    /// A member beyond a TUPLE's declared arity has no declared type, so it is
    /// kept as raw text rather than guessed at — the comparator's arity check is
    /// what reports it.
    #[test]
    fn a_member_beyond_a_tuples_arity_is_kept_as_text() {
        let decoded = decode(&json!([1]), "(1, 2)", &ty_of("tuple<int>")).unwrap();
        assert_eq!(decoded, json!(["1", "2"]));
    }
}

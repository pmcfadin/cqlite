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
//! # Two ambiguities, declared rather than papered over
//!
//! 0. **An EMPTY container vs a container of one EMPTY member.** Members are
//!    unquoted and unseparated at count 1, so a `set<text>` holding exactly the
//!    empty string renders `{}` — byte for byte what an EMPTY set renders as. The
//!    two are different values, so neither reading is trustworthy and such a cell
//!    is REFUSED. The rule is bounded by the DECLARED element type: a `list<int>`
//!    member always carries a digit, so `[]` there can only mean zero members and
//!    IS compared. It is the mirror of ambiguity 3 below, which refuses the case
//!    where the GOLDEN carries the empty member; without it the golden-side scan
//!    saw nothing to refuse in an empty golden container while the CLI could
//!    perfectly well have held one empty member.
//! 1. **`null` vs the text `"null"`.** A container has no empty-field mechanism,
//!    so `ValueFormatter` spells a null member `null` — the same text a `text`
//!    member holding `"null"` produces (issue #1499's ambiguity, one level in).
//!    The token is resolved from the GOLDEN's own type: null there decodes to
//!    null here, anything else stays text. That keeps the distinction wherever
//!    the oracle knows it, and loses it only where CSV genuinely cannot express
//!    it. A CLI that emits the wrong member still fails — only the exact
//!    null/`"null"` swap is invisible.
//! 3. **Separator collisions.** Members are unquoted, so a scalar whose text
//!    contains `, ` (or, for a map/UDT KEY, `: `) or a bracket makes the
//!    rendering genuinely unparseable. Such a cell is REFUSED, never guessed —
//!    and the refusal is decided from the GOLDEN alone, so it can never be
//!    caused by the very defect under test. Refusals are counted and named in
//!    the run census.

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

/// Is this golden container unambiguously recoverable from the flat CSV
/// rendering? `Some(reason)` means it is not, and the cell must be refused.
///
/// Decided from the GOLDEN and the committed DDL — never from the CLI's output —
/// so a refusal can never be produced by the defect the lane is looking for.
///
/// The DDL is consulted for ONE question only: whether a member of the declared
/// element type can render as the empty string, which is what decides ambiguity 0.
/// Everything else is read from the golden's own content.
pub fn ambiguity(golden: &Value, ty: &CqlType) -> Option<String> {
    ambiguity_at(golden, Some(ty))
}

/// [`ambiguity`], carrying the declared type of THIS position. `None` means the
/// declared type does not describe this shape (the comparison reports that), and
/// no type-dependent rule may then fire — refusing there would suppress a real
/// divergence rather than declare a format limit.
fn ambiguity_at(golden: &Value, ty: Option<&CqlType>) -> Option<String> {
    match golden {
        Value::Array(items) => {
            // Ambiguity 0: zero members and one EMPTY member render identically.
            // Keyed on the AFFIRMATIVE answer — this element type really can
            // render empty — so an unknown or non-collection type does not refuse.
            if items.is_empty() && empty_container_is_ambiguous(ty) {
                return Some(
                    "an empty container is indistinguishable from a container of one empty \
                     member of this element type"
                        .into(),
                );
            }
            for (i, item) in items.iter().enumerate() {
                // A member rendering to the empty string makes the member count
                // unrecoverable: one empty member and zero members both render
                // as an empty body.
                if is_scalar(item) && scalar_text(item).is_empty() {
                    return Some(
                        "an empty scalar member is indistinguishable from no member".into(),
                    );
                }
                if let Some(why) = ambiguity_at(item, member_type(ty, i)) {
                    return Some(why);
                }
            }
            None
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                // Only a KEY is harmed by `: `: entries are split at their FIRST
                // top-level `: `, so a colon inside a VALUE is already correct.
                if key.contains(": ") {
                    return Some(format!(
                        "map/UDT key {} contains the `: ` separator",
                        brief(key)
                    ));
                }
                if let Some(why) = scalar_ambiguity_of(&Value::String(key.clone())) {
                    return Some(format!("map/UDT key: {why}"));
                }
                if let Some(why) = ambiguity_at(value, field_type(ty, key)) {
                    return Some(why);
                }
            }
            None
        }
        scalar => scalar_ambiguity_of(scalar),
    }
}

fn scalar_ambiguity_of(scalar: &Value) -> Option<String> {
    let text = scalar_text(scalar);
    if text.contains(", ") {
        return Some(format!(
            "member {} contains the `, ` separator",
            brief(&text)
        ));
    }
    if let Some(found) = STRUCTURAL.iter().find(|c| text.contains(**c)) {
        return Some(format!(
            "member {} contains the structural character `{found}`",
            brief(&text)
        ));
    }
    None
}

/// The text `ValueFormatter` renders a scalar as, for the ambiguity scan only.
/// `Value::Null` is excluded: its `null` spelling is handled by ambiguity 2.
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
/// positional member). A member at an EXCLUDED path is returned as its raw,
/// UNDECODED text: the comparison will not look at it, and requiring the grammar
/// to invert there would fail the whole cell for a member nobody compares — which
/// is what forced the `udt_nested` exclusion to be whole-column (issue #1491
/// review finding F5).
///
/// The ambiguity scan is deliberately NOT exclusion-aware: it is decided from the
/// golden alone and refusing a whole cell is a conservative, counted, NAMED
/// outcome in the census, never a silent pass.
pub fn decode_at(
    golden: &Value,
    text: &str,
    ty: &CqlType,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    if excluded(path) {
        return Ok(Value::String(text.to_string()));
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
        // Ambiguity 1: the golden's own type resolves the `null` token.
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

    // --- the refusal valve -------------------------------------------------

    #[test]
    fn member_containing_the_element_separator_is_refused() {
        // `{"a, b"}` and `{"a", "b"}` render identically, so no reading of the
        // CLI's text is trustworthy.
        let why = ambiguity(&json!(["a, b"]), &ty_of("set<text>"))
            .expect("a `, `-bearing member must be refused");
        assert!(why.contains("`, ` separator"), "unexpected reason: {why}");
    }

    #[test]
    fn member_containing_a_bracket_is_refused() {
        let why = ambiguity(&json!(["x}y"]), &ty_of("set<text>"))
            .expect("a bracket-bearing member must be refused");
        assert!(
            why.contains("structural character"),
            "unexpected reason: {why}"
        );
    }

    #[test]
    fn map_key_containing_the_pair_separator_is_refused() {
        let why = ambiguity(&json!({"a: b": 1}), &ty_of("map<text, int>"))
            .expect("a `: `-bearing KEY must be refused");
        assert!(why.contains("key"), "unexpected reason: {why}");
    }

    #[test]
    fn map_value_containing_the_pair_separator_is_not_refused() {
        // Entries split at their FIRST top-level `: `, which is the real
        // separator, so a colon inside the VALUE is already decoded correctly.
        // Refusing it would narrow the lane for no reason.
        assert_eq!(
            ambiguity(&json!({"k": "a: b"}), &ty_of("map<text, text>")),
            None
        );
        let decoded = decode(
            &json!({"k": "a: b"}),
            "{k: a: b}",
            &ty_of("map<text, text>"),
        )
        .expect("decodes");
        assert_eq!(decoded, json!([{"key": "k", "value": "a: b"}]));
    }

    #[test]
    fn an_empty_member_of_a_non_empty_collection_is_refused() {
        // `{}` is both "no members" and "one empty member".
        let why =
            ambiguity(&json!([""]), &ty_of("set<text>")).expect("an empty member must be refused");
        assert!(
            why.contains("empty scalar member"),
            "unexpected reason: {why}"
        );
    }

    /// Ambiguity 0, the MIRROR of the case above: the golden container is EMPTY
    /// and the CLI could perfectly well have held one member that renders empty.
    /// The golden-side scan saw nothing to refuse there, so `{}` accepted both
    /// readings — and the two are different values.
    ///
    /// Bounded by the DECLARED element type, which is what makes this a
    /// measurement and not blanket strictness: a `set<text>` member can BE the
    /// empty string, a `list<int>` member always carries a digit, and a `tuple`'s
    /// member count comes from the DDL (so the comparison's arity check sees a
    /// dropped member).
    #[test]
    fn an_empty_container_is_refused_only_where_its_element_can_render_empty() {
        for decl in ["set<text>", "list<ascii>", "set<blob>", "list<timestamp>"] {
            let why = ambiguity(&json!([]), &ty_of(decl))
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
                ambiguity(&json!([]), &ty_of(decl)),
                None,
                "{decl}: no member of this element type can render empty, so `[]` can \
                 only mean zero members and must stay compared"
            );
        }
        // A tuple's arity is the DDL's, so `()` cannot hide a member.
        assert_eq!(ambiguity(&json!([]), &ty_of("tuple<text, text>")), None);
        // An empty map/UDT body is unambiguous too: every entry carries a `: `, so
        // a one-entry rendering can never be `{}`.
        assert_eq!(ambiguity(&json!({}), &ty_of("map<text, text>")), None);
        // And the rule reaches a NESTED empty container, under its own element type.
        let why = ambiguity(&json!([[]]), &ty_of("list<frozen<list<text>>>"))
            .expect("a nested empty list<text> must be refused");
        assert!(
            why.contains("empty container is indistinguishable"),
            "{why}"
        );
    }

    #[test]
    fn ordinary_corpus_content_is_not_refused() {
        // Spaces, hyphens, `0x` hex, exact decimals and nesting are all fine —
        // only the separators and brackets are structural. (`1 Navy Way` is real
        // content from test_compactionparityudt.udt_collections.)
        assert_eq!(
            ambiguity(
                &json!({"home": {"street": "1 Navy Way", "zip": "22201"}}),
                &ty_of("map<text, frozen<address>>")
            ),
            None
        );
        assert_eq!(
            ambiguity(
                &json!(["0xdeadbeef", "-1.5", "neg-five", null]),
                &ty_of("list<text>")
            ),
            None
        );
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

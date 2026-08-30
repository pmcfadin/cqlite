//! Decoding a CSV container cell back into a comparable value (issue #1491).
//!
//! # Why a decoder and not a skip
//!
//! Acceptance criterion 2 of #1491 is "parse the CSV output back into rows and
//! compare cells to the golden", and it carves out no container exception. CSV
//! carries no types at all, so decoding ANY cell needs an external statement of
//! its shape — that is a property of the format, not a weakness of this lane.
//! Here the shape comes from the GOLDEN (`sstabledump` renders a list/set/frozen
//! collection as a JSON array and a map/UDT as a JSON object), i.e. from the
//! oracle. Nothing here is derived from CQLite's own output.
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
//! # Three ambiguities, declared rather than papered over
//!
//! 1. **list vs set.** `sstabledump` renders BOTH as a JSON array, so the golden
//!    cannot say which one it is, and `[a, b]` and `{a, b}` are therefore both
//!    accepted for a golden array. This is NOT a CSV-specific loss: CQLite's
//!    JSON egress also renders a set as an array (measured on
//!    `test_da.collection_table`: `tags SET<TEXT>` → `["alpha","beta"]`), so the
//!    JSON lane has the identical blind spot.
//! 2. **`null` vs the text `"null"`.** A container has no empty-field mechanism,
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

use serde_json::{Map, Value};

/// Characters that carry structure in the rendering and therefore cannot appear
/// inside an unquoted member without making it unparseable.
const STRUCTURAL: [char; 6] = ['[', ']', '{', '}', '(', ')'];

/// Openers accepted for a golden ARRAY: `[` (list), `{` (set — see ambiguity 1)
/// and `(` (tuple).
const ARRAY_OPENERS: [(char, char); 3] = [('[', ']'), ('{', '}'), ('(', ')')];

/// Is this golden container unambiguously recoverable from the flat CSV
/// rendering? `Some(reason)` means it is not, and the cell must be refused.
///
/// Decided from the GOLDEN alone — never from the CLI's output — so a refusal
/// can never be produced by the defect the lane is looking for.
pub fn ambiguity(golden: &Value) -> Option<String> {
    match golden {
        Value::Array(items) => {
            for item in items {
                // A member rendering to the empty string makes the member count
                // unrecoverable: one empty member and zero members both render
                // as an empty body.
                if is_scalar(item) && scalar_text(item).is_empty() {
                    return Some(
                        "an empty scalar member is indistinguishable from no member".into(),
                    );
                }
                if let Some(why) = ambiguity(item) {
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
                if let Some(why) = ambiguity(value) {
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

/// A predicate over a value path: `true` means the comparison excludes that path,
/// so the decoder must not require the CLI's text there to invert the grammar.
///
/// Kept as a bare closure type rather than a dependency on the comparator's
/// `SkipPaths`, so this module stays independent of it.
pub type Excluded<'a> = dyn Fn(&str) -> bool + 'a;

/// Decode `text` (one CSV field, or one member of one) into the shape `golden`
/// declares. A map/UDT decodes to the `[{"key":…,"value":…}, …]` spelling the
/// JSON egress uses, so the existing map comparison applies unchanged.
pub fn decode(golden: &Value, text: &str) -> Result<Value, String> {
    decode_at(golden, text, "", &|_| false)
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
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    if excluded(path) {
        return Ok(Value::String(text.to_string()));
    }
    match golden {
        Value::Array(items) => decode_array(items, text, path, excluded),
        Value::Object(fields) => decode_object(fields, text, path, excluded),
        // Ambiguity 2: the golden's own type resolves the `null` token.
        Value::Null if text == "null" => Ok(Value::Null),
        _ => Ok(Value::String(text.to_string())),
    }
}

fn decode_array(
    golden: &[Value],
    text: &str,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    let inner = strip(text, &ARRAY_OPENERS)?;
    let parts = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ", ")?
    };
    let mut out = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        // A member the golden does not have is decoded against `null`; the
        // length mismatch is what the comparison then reports.
        out.push(decode_at(
            golden.get(i).unwrap_or(&Value::Null),
            part,
            &format!("{path}[{i}]"),
            excluded,
        )?);
    }
    Ok(Value::Array(out))
}

fn decode_object(
    golden: &Map<String, Value>,
    text: &str,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    let inner = strip(text, &[('{', '}')])?;
    let parts = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ", ")?
    };
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
        entry.insert(
            "value".to_string(),
            decode_at(
                golden.get(key).unwrap_or(&Value::Null),
                value,
                &child,
                excluded,
            )?,
        );
        out.push(Value::Object(entry));
    }
    Ok(Value::Array(out))
}

/// Remove the exact opening/closing bracket pair. Strict: a body that does not
/// open with an accepted bracket, or does not close with that bracket's mate, is
/// an error rather than a best-effort parse.
fn strip<'a>(text: &'a str, pairs: &[(char, char)]) -> Result<&'a str, String> {
    for (open, close) in pairs {
        if let Some(rest) = text.strip_prefix(*open) {
            return rest.strip_suffix(*close).ok_or_else(|| {
                format!(
                    "{} opens with `{open}` but does not close with `{close}`",
                    brief(text)
                )
            });
        }
    }
    let accepted: String = pairs.iter().map(|(o, _)| *o).collect();
    Err(format!(
        "{} is not a container rendering (expected an opening `{accepted}`)",
        brief(text)
    ))
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
// The committed + fetched corpus contains no container member carrying a `, `,
// a bracket or a `: ` in a map key, so the run census reports `0 REFUSED` — a
// true measurement, but one that leaves the refusal valve and the strictness
// rules unexecuted. These cases exercise them directly, so "0 refusals" means
// "the scan ran and found none" rather than "the scan may not work".
//
// Inputs are renderings in the grammar `ValueFormatter` documents; expected
// outputs are the GOLDEN-side shapes `sstabledump` produces. Nothing here is
// derived from CQLite's current output.
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- the refusal valve -------------------------------------------------

    #[test]
    fn member_containing_the_element_separator_is_refused() {
        // `{"a, b"}` and `{"a", "b"}` render identically, so no reading of the
        // CLI's text is trustworthy.
        let why = ambiguity(&json!(["a, b"])).expect("a `, `-bearing member must be refused");
        assert!(why.contains("`, ` separator"), "unexpected reason: {why}");
    }

    #[test]
    fn member_containing_a_bracket_is_refused() {
        let why = ambiguity(&json!(["x}y"])).expect("a bracket-bearing member must be refused");
        assert!(
            why.contains("structural character"),
            "unexpected reason: {why}"
        );
    }

    #[test]
    fn map_key_containing_the_pair_separator_is_refused() {
        let why = ambiguity(&json!({"a: b": 1})).expect("a `: `-bearing KEY must be refused");
        assert!(why.contains("key"), "unexpected reason: {why}");
    }

    #[test]
    fn map_value_containing_the_pair_separator_is_not_refused() {
        // Entries split at their FIRST top-level `: `, which is the real
        // separator, so a colon inside the VALUE is already decoded correctly.
        // Refusing it would narrow the lane for no reason.
        assert_eq!(ambiguity(&json!({"k": "a: b"})), None);
        let decoded = decode(&json!({"k": "a: b"}), "{k: a: b}").expect("decodes");
        assert_eq!(decoded, json!([{"key": "k", "value": "a: b"}]));
    }

    #[test]
    fn an_empty_member_of_a_non_empty_collection_is_refused() {
        // `{}` is both "no members" and "one empty member".
        let why = ambiguity(&json!([""])).expect("an empty member must be refused");
        assert!(
            why.contains("empty scalar member"),
            "unexpected reason: {why}"
        );
    }

    #[test]
    fn ordinary_corpus_content_is_not_refused() {
        // Spaces, hyphens, `0x` hex, exact decimals and nesting are all fine —
        // only the separators and brackets are structural. (`1 Navy Way` is real
        // content from test_compactionparityudt.udt_collections.)
        assert_eq!(
            ambiguity(&json!({"home": {"street": "1 Navy Way", "zip": "22201"}})),
            None
        );
        assert_eq!(
            ambiguity(&json!(["0xdeadbeef", "-1.5", "neg-five", null])),
            None
        );
    }

    // --- strictness: the decoder must not repair a malformed rendering ------

    #[test]
    fn the_element_separator_must_be_exactly_comma_space() {
        // A writer that dropped the space must NOT decode as two members; that
        // tolerance is what would let a framing regression through.
        let decoded = decode(&json!([1, 2]), "[1,2]").expect("decodes as one member");
        assert_eq!(decoded, json!(["1,2"]), "`,` was wrongly treated as `, `");
    }

    #[test]
    fn a_mismatched_or_unbalanced_bracket_is_an_error() {
        assert!(
            decode(&json!([1]), "[1}").is_err(),
            "mismatched bracket must fail"
        );
        assert!(
            decode(&json!([1]), "[[1]").is_err(),
            "unclosed bracket must fail"
        );
        assert!(
            decode(&json!([1]), "1, 2").is_err(),
            "a bare body must fail"
        );
        assert!(
            decode(&json!({"k": 1}), "[k: 1]").is_err(),
            "a map needs braces"
        );
    }

    #[test]
    fn a_map_entry_without_the_pair_separator_is_an_error() {
        assert!(decode(&json!({"k": 1}), "{k=1}").is_err());
    }

    // --- decoding ----------------------------------------------------------

    #[test]
    fn a_golden_array_accepts_both_bracket_spellings() {
        // Ambiguity 1: `sstabledump` renders a list AND a set as a JSON array,
        // so the golden cannot say which bracket to expect.
        assert_eq!(decode(&json!([1, 2]), "[1, 2]").unwrap(), json!(["1", "2"]));
        assert_eq!(decode(&json!([1, 2]), "{1, 2}").unwrap(), json!(["1", "2"]));
    }

    #[test]
    fn an_empty_body_decodes_to_zero_members() {
        assert_eq!(decode(&json!([]), "[]").unwrap(), json!([]));
        assert_eq!(decode(&json!([]), "{}").unwrap(), json!([]));
        assert_eq!(decode(&json!({}), "{}").unwrap(), json!([]));
    }

    #[test]
    fn nesting_is_decoded_at_depth() {
        // A map<text, frozen<udt>>, as in test_compactionparityudt.udt_collections:
        // the inner `, ` and `: ` must not be mistaken for outer separators.
        let golden = json!({"home": {"street": "1 Navy Way", "city": "Arlington"}});
        let decoded = decode(&golden, "{home: {street: 1 Navy Way, city: Arlington}}").unwrap();
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
        // Ambiguity 2, in both directions: a null member decodes to null, and a
        // `text` member holding "null" stays text.
        assert_eq!(
            decode(&json!({"last_name": null}), "{last_name: null}").unwrap(),
            json!([{"key": "last_name", "value": null}])
        );
        assert_eq!(
            decode(&json!({"last_name": "null"}), "{last_name: null}").unwrap(),
            json!([{"key": "last_name", "value": "null"}])
        );
    }

    #[test]
    fn a_surplus_member_is_kept_so_the_length_mismatch_is_reported() {
        // The decoder must not silently truncate to the golden's length — the
        // comparison is what reports the divergence.
        let decoded = decode(&json!([1]), "[1, 2]").unwrap();
        assert_eq!(decoded, json!(["1", "2"]));
    }
}

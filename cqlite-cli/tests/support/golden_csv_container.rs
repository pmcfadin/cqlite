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

/// Decode `text` (one CSV field, or one member of one) into the shape `golden`
/// declares. A map/UDT decodes to the `[{"key":…,"value":…}, …]` spelling the
/// JSON egress uses, so the existing map comparison applies unchanged.
pub fn decode(golden: &Value, text: &str) -> Result<Value, String> {
    match golden {
        Value::Array(items) => decode_array(items, text),
        Value::Object(fields) => decode_object(fields, text),
        // Ambiguity 2: the golden's own type resolves the `null` token.
        Value::Null if text == "null" => Ok(Value::Null),
        _ => Ok(Value::String(text.to_string())),
    }
}

fn decode_array(golden: &[Value], text: &str) -> Result<Value, String> {
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
        out.push(decode(golden.get(i).unwrap_or(&Value::Null), part)?);
    }
    Ok(Value::Array(out))
}

fn decode_object(golden: &Map<String, Value>, text: &str) -> Result<Value, String> {
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
        entry.insert(
            "value".to_string(),
            decode(golden.get(key).unwrap_or(&Value::Null), value)?,
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

//! LEXEME-PRESERVING golden text for declared-`decimal`/`varint` positions
//! (#1490 rounds 10–11).
//!
//! # The defect this closes: an `f64` cannot identify the value it came from
//!
//! `sstabledump` writes a `decimal` CELL as a bare JSON number, and the shared
//! comparator (`canonical_jsonl::CanonicalValue::from_json`) parses a bare JSON
//! number into an `f64`. The harness used to RECOVER the decimal from that
//! double — render it at the export scale, then check that neither one-unit
//! neighbour parses to the same double — and treat a unique answer as exact.
//!
//! That is unsound in principle, not merely imprecise. `0.100000000000000001`
//! and `0.1` parse to the SAME `f64`; the recovered value is `0.1`, both
//! neighbours round elsewhere, so the recovery reported itself EXACT and a
//! golden literal carrying eighteen fractional digits was canonicalized as
//! `0.1`. A lossy export would then have compared EQUAL — a false PASS in the
//! one place this harness exists to be trusted. No amount of neighbour probing
//! fixes it: by the time the value is an `f64` the distinguishing digits are
//! already gone.
//!
//! `varint` has the same disease one exponent further out: a valid `varint`
//! above `u64::MAX` fails both `Number::as_i64` and `as_u64` and lands on an
//! `f64` too, while the exported side reads that column back as an exact
//! `Decimal128(38, 0)` integer — so the comparison was `Float` vs `Int`, a
//! false mismatch, with the digits that would have shown a real corruption
//! already lost.
//!
//! So the literal TEXT has to survive the parse. This module preserves it, and
//! `declared.rs` REFUSES a declared-`decimal`/`varint` position that arrives as
//! a double — so a lexeme this module fails to preserve produces a loud
//! refusal, never a comparison.
//!
//! # How: quote those lexemes BEFORE the shared parser sees them
//!
//! The number lexemes at declared-`decimal`/`varint` POSITIONS are QUOTED —
//! turned into JSON strings, verbatim, digit for digit. The shared parser then
//! hands the harness the literal itself, and the declared-type door routes it
//! through [`super::decimal::exact_from_text`] (a `decimal`) or an exact `i128`
//! parse (a `varint`): the same exact, `f64`-free reads a `decimal` PRIMARY-KEY
//! component already used (Cassandra stringifies those, so their text was never
//! lost).
//!
//! Everything else is re-emitted verbatim — every string keeps its original
//! escaping, every other number keeps its original digits — so a `double`
//! column's literal still reaches `serde_json`'s (`float_roundtrip`, exact)
//! parser unchanged and the exact-bit `float`/`double` comparison is untouched.
//!
//! # The split with `declared.rs`, and why it is where it is
//!
//! This module owns the LEXICAL half only: a JSON reader that retains every
//! scalar's original text, an emitter that puts it back verbatim, and the walk
//! that finds the `sstabledump` CELLS in a line. The decision **"must this
//! number keep its literal?"** is NOT here — it is taken per POSITION, from
//! that position's declared type, by `declared::preserve_lexemes`, which
//! recurses in lockstep with `declared::canonicalize_golden` over the same
//! `CqlTypeSpec` and derives its child positions with the same private
//! `Declared::child`.
//!
//! That split is the round-11 fix and it is deliberate. The first version of
//! this module asked "does this COLUMN mention a `decimal` anywhere?" and then
//! quoted every number in the column's value — reproducing, beside the
//! declared-type door, exactly the coarse-instead-of-positional defect that
//! door was built to end (rounds 5–7). It turned a `map<decimal,int>`'s `int`
//! VALUES into strings: a false parity failure. Two recursive walkers over one
//! value, disagreeing about positions, IS the defect.
//!
//! # Why this option and not the other two
//!
//! * `serde_json::value::RawValue` (which would keep the number's text through
//!   the parse) needs serde_json's `raw_value` feature, which no crate in the
//!   workspace enables.
//! * `arbitrary_precision` would change `serde_json::Number` for every
//!   serde_json user in the `cqlite-cli` test targets — and it would NOT fix
//!   this: `canonical_jsonl::from_json` asks `Number::as_i64()` and then
//!   `Number::as_f64()`, and `as_f64()` SUCCEEDS under `arbitrary_precision`
//!   (it parses the retained text), so the value still reaches the harness as a
//!   double and the retained lexeme is never consulted.
//! * `canonical_jsonl.rs` itself is `cqlite-core`-owned and shared by several
//!   lanes, so teaching it to carry a decimal is out of this issue's scope.
//!
//! That leaves capturing the lexeme where the JSONL text is READ, which is what
//! this module does.
//!
//! # Fail-closed
//!
//! The scanner is a complete JSON reader that REFUSES rather than guesses: any
//! byte it does not recognise, an unterminated string, a malformed number or
//! trailing content after the value is an `Err` that aborts the case. It never
//! falls back to the untransformed text, because that would silently restore the
//! `f64` path this module removes.

#![allow(dead_code)]

use super::cql_type::ColumnType;
use super::declared;

/// Placeholder markers a golden must never contain.
///
/// MIRRORS `canonical_jsonl`'s private `PLACEHOLDER_MARKERS`, re-applied here
/// because the harness now reads the golden TEXT itself (to preserve decimal
/// lexemes) and so no longer goes through `load_golden_document_with_keys`,
/// which owns that refusal. `canonical_jsonl.rs` is `cqlite-core`-owned, so the
/// const cannot be shared from here; `golden_placeholder_marker_is_refused`
/// pins this copy.
const PLACEHOLDER_MARKERS: &[&str] = &[
    "\"PLACEHOLDER\"",
    "\"__PLACEHOLDER__\"",
    "\"TODO\"",
    "\"GENERATED_PLACEHOLDER\"",
    "PLACEHOLDER_REFERENCE",
];

/// The first placeholder marker `content` carries, if any.
pub fn placeholder_marker(content: &str) -> Option<&'static str> {
    PLACEHOLDER_MARKERS
        .iter()
        .copied()
        .find(|m| content.contains(m))
}

/// Rewrite a whole JSONL document, quoting the number lexemes that sit at a
/// declared-`decimal`/`varint` POSITION — and no others.
///
/// Blank lines are preserved as-is; every other line is parsed, transformed and
/// re-emitted. A line the scanner cannot read is an `Err` naming the line.
pub fn preserve_exact_lexemes(content: &str, columns: &[ColumnType]) -> Result<String, String> {
    let mut out = String::with_capacity(content.len());
    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let mut value = parse_line(line.trim()).map_err(|e| format!("line {}: {e}", idx + 1))?;
        visit_cells(&mut value, columns);
        value.emit(&mut out);
        out.push('\n');
    }
    Ok(out)
}

/// Find every `sstabledump` CELL in the line and hand its `value` to the
/// declared-type descent at the position that cell's `value` occupies.
///
/// The whole document is walked (a cell sits under `partition`/`rows`, and a
/// nested structure could hold one too), and the column is identified from the
/// cell's OWN `name` field — never from the value's shape, which would be the
/// type-guessing issue #28 forbids. A cell naming a column the case does not
/// declare is left untouched here; `golden_rows::reject_undeclared_cells`
/// FAILS the case on it, which is a louder answer than anything this pass could
/// give.
fn visit_cells(value: &mut Lex, columns: &[ColumnType]) {
    let named = match value.field("name") {
        Some(Lex::Str { decoded, .. }) => Some(decoded.clone()),
        _ => None,
    };
    if let Some(name) = named {
        if let Some(col) = columns.iter().find(|c| c.name == name) {
            // WHICH position the cell's `value` sits at is a declared-type
            // question, so it is answered by the declared-type door, not here.
            if let Some(at) = declared::cell_value_declared(
                col,
                format!("cell '{}' ({})", col.name, col.declared),
            ) {
                if let Some(cell_value) = value.field_mut("value") {
                    declared::preserve_lexemes(cell_value, &at);
                }
            }
        }
    }
    match value {
        Lex::Arr(items) => {
            for item in items {
                visit_cells(item, columns);
            }
        }
        Lex::Obj(fields) => {
            for (_, _, v) in fields {
                visit_cells(v, columns);
            }
        }
        _ => {}
    }
}

/// A JSON value with every scalar's ORIGINAL TEXT retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lex {
    /// `null`, `true`, `false` — verbatim.
    Lit(String),
    /// A number, verbatim: the whole point of this module.
    Num(String),
    /// A string: `raw` is the original literal INCLUDING its quotes and
    /// escaping, `decoded` its value (needed to read a cell's `name`).
    Str {
        raw: String,
        decoded: String,
    },
    Arr(Vec<Lex>),
    /// Object fields in order, each as (raw key literal, decoded key, value).
    Obj(Vec<(String, String, Lex)>),
}

impl Lex {
    /// Re-emit compact JSON. Every retained lexeme goes out verbatim, so the
    /// only textual change this module can make is the intended quoting.
    pub fn emit(&self, out: &mut String) {
        match self {
            Lex::Lit(raw) | Lex::Num(raw) => out.push_str(raw),
            Lex::Str { raw, .. } => out.push_str(raw),
            Lex::Arr(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.emit(out);
                }
                out.push(']');
            }
            Lex::Obj(fields) => {
                out.push('{');
                for (i, (raw_key, _, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(raw_key);
                    out.push(':');
                    value.emit(out);
                }
                out.push('}');
            }
        }
    }

    /// Turn a NUMBER into the JSON STRING of its own lexeme; anything else is
    /// left exactly as it is.
    ///
    /// The primitive `declared::preserve_lexemes` calls once it has decided,
    /// FROM THE DECLARED TYPE AT THIS POSITION, that the literal must survive.
    /// It is deliberately not recursive and not type-aware: recursion belongs to
    /// the one declared-type descent.
    pub(super) fn quote_number_lexeme(&mut self) {
        if let Lex::Num(raw) = self {
            // A JSON number lexeme contains only `-+.eE0123456789`, none of
            // which JSON escapes, so quoting it is exact.
            let quoted = format!("\"{raw}\"");
            *self = Lex::Str {
                raw: quoted,
                decoded: std::mem::take(raw),
            };
        }
    }

    pub fn to_json_text(&self) -> String {
        let mut out = String::new();
        self.emit(&mut out);
        out
    }

    fn field(&self, name: &str) -> Option<&Lex> {
        match self {
            Lex::Obj(fields) => fields.iter().find(|(_, k, _)| k == name).map(|(_, _, v)| v),
            _ => None,
        }
    }

    fn field_mut(&mut self, name: &str) -> Option<&mut Lex> {
        match self {
            Lex::Obj(fields) => fields
                .iter_mut()
                .find(|(_, k, _)| k == name)
                .map(|(_, _, v)| v),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// The scanner: complete JSON, refusing whatever it does not recognise
// ---------------------------------------------------------------------------

/// Parse ONE JSON value that must span the whole of `line`.
pub fn parse_line(line: &str) -> Result<Lex, String> {
    let bytes = line.as_bytes();
    let mut at = 0usize;
    let value = parse_value(bytes, &mut at)?;
    skip_ws(bytes, &mut at);
    if at != bytes.len() {
        return Err(format!(
            "trailing content after the JSON value at byte {at}: {:?}",
            &line[at..line.len().min(at + 40)]
        ));
    }
    Ok(value)
}

fn skip_ws(bytes: &[u8], at: &mut usize) {
    while *at < bytes.len() && matches!(bytes[*at], b' ' | b'\t' | b'\n' | b'\r') {
        *at += 1;
    }
}

fn parse_value(bytes: &[u8], at: &mut usize) -> Result<Lex, String> {
    skip_ws(bytes, at);
    let Some(&b) = bytes.get(*at) else {
        return Err("unexpected end of input where a JSON value was expected".to_string());
    };
    match b {
        b'{' => parse_object(bytes, at),
        b'[' => parse_array(bytes, at),
        b'"' => {
            let (raw, decoded) = parse_string(bytes, at)?;
            Ok(Lex::Str { raw, decoded })
        }
        b't' => parse_literal(bytes, at, "true"),
        b'f' => parse_literal(bytes, at, "false"),
        b'n' => parse_literal(bytes, at, "null"),
        b'-' | b'0'..=b'9' => parse_number(bytes, at),
        other => Err(format!(
            "byte {at}: {:?} cannot start a JSON value",
            other as char
        )),
    }
}

fn parse_literal(bytes: &[u8], at: &mut usize, word: &str) -> Result<Lex, String> {
    if bytes[*at..].starts_with(word.as_bytes()) {
        *at += word.len();
        Ok(Lex::Lit(word.to_string()))
    } else {
        Err(format!("byte {at}: expected `{word}`"))
    }
}

fn parse_number(bytes: &[u8], at: &mut usize) -> Result<Lex, String> {
    let start = *at;
    if bytes.get(*at) == Some(&b'-') {
        *at += 1;
    }
    let int_digits = take_digits(bytes, at);
    if int_digits == 0 {
        return Err(format!("byte {start}: a JSON number needs an integer part"));
    }
    if bytes.get(*at) == Some(&b'.') {
        *at += 1;
        if take_digits(bytes, at) == 0 {
            return Err(format!(
                "byte {start}: a JSON number's fraction needs at least one digit"
            ));
        }
    }
    if matches!(bytes.get(*at), Some(b'e') | Some(b'E')) {
        *at += 1;
        if matches!(bytes.get(*at), Some(b'+') | Some(b'-')) {
            *at += 1;
        }
        if take_digits(bytes, at) == 0 {
            return Err(format!(
                "byte {start}: a JSON number's exponent needs at least one digit"
            ));
        }
    }
    // The lexeme is ASCII by JSON's grammar, so this slice is always valid UTF-8.
    let raw = std::str::from_utf8(&bytes[start..*at])
        .map_err(|e| format!("byte {start}: number lexeme is not UTF-8: {e}"))?;
    Ok(Lex::Num(raw.to_string()))
}

fn take_digits(bytes: &[u8], at: &mut usize) -> usize {
    let start = *at;
    while matches!(bytes.get(*at), Some(b'0'..=b'9')) {
        *at += 1;
    }
    *at - start
}

/// Read a string literal, returning (raw literal including quotes, decoded
/// value). The decode is `serde_json`'s own, so every escape form is handled
/// exactly rather than re-implemented here.
fn parse_string(bytes: &[u8], at: &mut usize) -> Result<(String, String), String> {
    let start = *at;
    debug_assert_eq!(bytes[*at], b'"');
    *at += 1;
    loop {
        let Some(&b) = bytes.get(*at) else {
            return Err(format!("byte {start}: unterminated string literal"));
        };
        match b {
            b'\\' => {
                // Skip the escape marker and whatever it escapes; a `\uXXXX`'s
                // four hex digits are ordinary bytes and need no special case,
                // since none of them can be an unescaped `"`.
                *at += 2;
                if *at > bytes.len() {
                    return Err(format!("byte {start}: string ends inside an escape"));
                }
            }
            b'"' => {
                *at += 1;
                break;
            }
            _ => *at += 1,
        }
    }
    let raw = std::str::from_utf8(&bytes[start..*at])
        .map_err(|e| format!("byte {start}: string literal is not UTF-8: {e}"))?
        .to_string();
    let decoded: String = serde_json::from_str(&raw)
        .map_err(|e| format!("byte {start}: {raw} is not a valid JSON string: {e}"))?;
    Ok((raw, decoded))
}

fn parse_array(bytes: &[u8], at: &mut usize) -> Result<Lex, String> {
    *at += 1; // '['
    let mut items = Vec::new();
    skip_ws(bytes, at);
    if bytes.get(*at) == Some(&b']') {
        *at += 1;
        return Ok(Lex::Arr(items));
    }
    loop {
        items.push(parse_value(bytes, at)?);
        skip_ws(bytes, at);
        match bytes.get(*at) {
            Some(b',') => *at += 1,
            Some(b']') => {
                *at += 1;
                return Ok(Lex::Arr(items));
            }
            Some(other) => {
                return Err(format!(
                    "byte {at}: expected `,` or `]` in an array, found {:?}",
                    *other as char
                ))
            }
            None => return Err("unterminated array".to_string()),
        }
    }
}

fn parse_object(bytes: &[u8], at: &mut usize) -> Result<Lex, String> {
    *at += 1; // '{'
    let mut fields = Vec::new();
    skip_ws(bytes, at);
    if bytes.get(*at) == Some(&b'}') {
        *at += 1;
        return Ok(Lex::Obj(fields));
    }
    loop {
        skip_ws(bytes, at);
        if bytes.get(*at) != Some(&b'"') {
            return Err(format!("byte {at}: an object key must be a string"));
        }
        let (raw_key, decoded_key) = parse_string(bytes, at)?;
        skip_ws(bytes, at);
        if bytes.get(*at) != Some(&b':') {
            return Err(format!("byte {at}: expected `:` after an object key"));
        }
        *at += 1;
        let value = parse_value(bytes, at)?;
        fields.push((raw_key, decoded_key, value));
        skip_ws(bytes, at);
        match bytes.get(*at) {
            Some(b',') => *at += 1,
            Some(b'}') => {
                *at += 1;
                return Ok(Lex::Obj(fields));
            }
            Some(other) => {
                return Err(format!(
                    "byte {at}: expected `,` or `}}` in an object, found {:?}",
                    *other as char
                ))
            }
            None => return Err("unterminated object".to_string()),
        }
    }
}

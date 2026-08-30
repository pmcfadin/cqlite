//! LEXEME-PRESERVING golden text for declared-`decimal` columns (#1490 round 10).
//!
//! # The defect this closes: an `f64` cannot identify the decimal it came from
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
//! So the literal TEXT has to survive the parse. This module preserves it, and
//! `declared.rs` REFUSES a declared-`decimal` position that arrives as a
//! double — so a lexeme this module fails to preserve produces a loud refusal,
//! never a comparison.
//!
//! # How: quote the decimal lexemes before the shared parser sees them
//!
//! For every `sstabledump` cell naming a column whose DECLARED type carries a
//! `decimal`, the number lexemes inside that cell's `value` are QUOTED — turned
//! into JSON strings, verbatim, digit for digit. The shared parser then hands
//! the harness the literal itself, and the declared-type door routes it through
//! [`super::decimal::exact_from_text`]: the same exact, `f64`-free parse a
//! `decimal` PRIMARY-KEY component already used (Cassandra stringifies those, so
//! their text was never lost).
//!
//! Everything else is re-emitted verbatim — every string keeps its original
//! escaping, every other number keeps its original digits — so a `double`
//! column's literal still reaches `serde_json`'s (`float_roundtrip`, exact)
//! parser unchanged and the exact-bit `float`/`double` comparison is untouched.
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

use std::collections::BTreeSet;

use super::cql_type::{ColumnType, CqlTypeSpec};

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

/// The names of the columns whose DECLARED type carries a `decimal` anywhere —
/// a `decimal` cell, a `set<decimal>` element, a `map<text,decimal>` value, a
/// `tuple<int,decimal>` member.
///
/// A UDT is named but not structurally known to the harness, so a `decimal`
/// FIELD of a UDT is not reachable here. That is a declared absence, not an
/// oversight: such a field arrives at a position whose declared type is
/// explicitly `Unavailable`, where nothing converts it to a decimal on either
/// side (see `declared.rs`), so it cannot reach the decimal comparison at all.
pub fn decimal_columns(columns: &[ColumnType]) -> BTreeSet<String> {
    columns
        .iter()
        .filter(|c| spec_carries_decimal(&c.spec))
        .map(|c| c.name.clone())
        .collect()
}

fn spec_carries_decimal(spec: &CqlTypeSpec) -> bool {
    match spec {
        CqlTypeSpec::Scalar(name) => name == "decimal",
        CqlTypeSpec::Seq { elem, .. } => spec_carries_decimal(elem),
        CqlTypeSpec::Map { key, value } => spec_carries_decimal(key) || spec_carries_decimal(value),
        CqlTypeSpec::Tuple(specs) => specs.iter().any(spec_carries_decimal),
        CqlTypeSpec::Udt(_) => false,
    }
}

/// Rewrite a whole JSONL document, quoting the number lexemes of every
/// declared-`decimal` column's cell value.
///
/// Blank lines are preserved as-is; every other line is parsed, transformed and
/// re-emitted. A line the scanner cannot read is an `Err` naming the line.
pub fn preserve_decimal_lexemes(
    content: &str,
    decimals: &BTreeSet<String>,
) -> Result<String, String> {
    let mut out = String::with_capacity(content.len());
    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let mut value = parse_line(line.trim()).map_err(|e| format!("line {}: {e}", idx + 1))?;
        quote_decimal_cells(&mut value, decimals);
        value.emit(&mut out);
        out.push('\n');
    }
    Ok(out)
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

/// Quote every number lexeme inside the `value` of every cell naming a
/// declared-`decimal` column.
///
/// The whole document is walked (a cell sits under `partition`/`rows`, and a
/// static block or a nested structure could hold one too), and the decision is
/// made from the cell's OWN `name` field — never from the value's shape, which
/// would be the type-guessing issue #28 forbids.
fn quote_decimal_cells(value: &mut Lex, decimals: &BTreeSet<String>) {
    let names_a_decimal_column = match value.field("name") {
        Some(Lex::Str { decoded, .. }) => decimals.contains(decoded),
        _ => false,
    };
    if names_a_decimal_column {
        if let Some(cell_value) = value.field_mut("value") {
            quote_numbers(cell_value);
        }
    }
    match value {
        Lex::Arr(items) => {
            for item in items {
                quote_decimal_cells(item, decimals);
            }
        }
        Lex::Obj(fields) => {
            for (_, _, v) in fields {
                quote_decimal_cells(v, decimals);
            }
        }
        _ => {}
    }
}

/// Turn every number in this subtree into the JSON STRING of its own lexeme.
///
/// Recursive so a collection of decimals (`frozen<set<decimal>>` arrives as a
/// JSON array, `frozen<map<text,decimal>>` as a JSON object) is covered too.
fn quote_numbers(value: &mut Lex) {
    match value {
        Lex::Num(raw) => {
            // A JSON number lexeme contains only `-+.eE0123456789`, none of
            // which JSON escapes, so quoting it is exact.
            let quoted = format!("\"{raw}\"");
            *value = Lex::Str {
                raw: quoted,
                decoded: std::mem::take(raw),
            };
        }
        Lex::Arr(items) => items.iter_mut().for_each(quote_numbers),
        Lex::Obj(fields) => fields.iter_mut().for_each(|(_, _, v)| quote_numbers(v)),
        _ => {}
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
